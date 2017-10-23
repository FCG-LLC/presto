/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package co.llective.presto.ip.appname

import co.llective.presto.ip.util.*
import org.slf4j.LoggerFactory
import java.net.Inet4Address
import java.net.Inet6Address
import java.net.InetAddress
import java.net.UnknownHostException
import java.util.*

class ApplicationNameResolver {
    private val UNKNOWN_NAME = "" // empty string is marking ip in cache as not named
    private val FILE_NAME = "application_name.txt"
    private val PORTS_FILE_NAME = "application_ports.csv"
    private val ipv4Subnets = LinkedHashMap<SubnetV4, String>()
    private val ipv6Subnets = LinkedHashMap<SubnetV6, String>()
    private val portNames = arrayOfNulls<String>(49151) // largest not ephemeral port number
    private val LOGGER = LoggerFactory.getLogger(ApplicationNameResolver::class.java)
    /**
     * Contains counted before application names for given IP pairs.
     */
    private val cache = SoftIpCache<String>()

    fun init() {
        populateSubnets(FileReader(FILE_NAME, "\t"))
        populatePortNames(FileReader(PORTS_FILE_NAME, ","))
    }

    private fun processSubnetLine(line: Array<String>) {
        if (line.size != 2) {
            LOGGER.warn("Line doesn't have 2 expected columns")
            return
        }

        val subnet = line[0]
        val applicationName = line[1]

        val index = subnet.indexOf("/")

        val address = subnet.substring(0, index)
        val maskLength = Integer.parseInt(subnet.substring(index + 1))

        try {
            val inetAddress = InetAddress.getByName(address)

            when (inetAddress) {
                is Inet4Address -> {
                    val subnetV4 = SubnetV4(address, maskLength)
                    ipv4Subnets.put(subnetV4, applicationName)
                }
                is Inet6Address -> {
                    val subnetV6 = SubnetV6(address, maskLength)
                    ipv6Subnets.put(subnetV6, applicationName)
                }
                else -> throw UnknownHostException()
            }
        } catch (e: UnknownHostException) {
            throw IllegalArgumentException("Wrong IP address " + address)
        }
    }

    private fun processPortLine(line: Array<String>) {
        if (line.size < 2) {
            LOGGER.warn("Line doesn't have at least 2 expected columns")
            return
        }

        val port = line[0]
        val applicationName = line[1]
        portNames[Integer.parseInt(port)] = applicationName
    }

    private fun populateSubnets(fileReader: FileReader) {
        fileReader.processFile(this::processSubnetLine)
    }

    private fun populatePortNames(fileReader: FileReader) {
        fileReader.processFile(this::processPortLine)
    }

    /**
     * Returns application name for given ip and port.
     * First thing taken into account while resolving is IP.
     * If no information is found then port is defining what is the application name.
     * @param ip1 high bits of IP (WKP when ipv4)
     * @param ip2 low bits of IP
     * @param port (optional) port of used application
     * @return String with application name or null if not recognized
     */
    @JvmOverloads
    fun getApplicationName(ip1: Long, ip2: Long, port: Int = -1): String? {
        val cacheValue = cache[ip1, ip2]
        cacheValue?.let { return if (cacheValue == UNKNOWN_NAME) getPortName(port) else cacheValue }

        if (ip1 == IpUtil.WKP) {
            for ((subnet, value) in ipv4Subnets) {
                if (subnet.mask and ip2 == subnet.address) {
                    cache.put(ip1, ip2, value)
                    return value
                }
            }
        } else {
            for ((subnet, value) in ipv6Subnets) {
                if (subnet.maskHighBits and ip1 == subnet.addressHighBits
                        && subnet.maskLowBits and ip2 == subnet.addressLowBits) {
                    cache.put(ip1, ip2, value)
                    return value
                }
            }
        }

        cache.put(ip1, ip2, UNKNOWN_NAME)
        return getPortName(port)
    }

    private fun getPortName(port: Int): String? {
        return if (port >= 0 && port < portNames.size) portNames[port] else null
    }
}
