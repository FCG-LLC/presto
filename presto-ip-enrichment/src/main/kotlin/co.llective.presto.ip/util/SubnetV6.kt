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
package co.llective.presto.ip.util

class SubnetV6(address: String, maskLength: Int) : Subnet {

    var addressHighBits: Long = 0
        private set
    var addressLowBits: Long = 0
        private set
    var maskHighBits: Long = 0
        private set
    var maskLowBits: Long = 0
        private set

    init {
        processIp(address)
        processMask(maskLength)
    }

    private fun processMask(maskLength: Int) {
        if (maskLength > java.lang.Long.SIZE) {
            maskHighBits = get64Mask(java.lang.Long.SIZE)
            maskLowBits = get64Mask(maskLength - java.lang.Long.SIZE)
        } else {
            maskHighBits = get64Mask(maskLength)
            maskLowBits = 0L
        }
    }

    private fun processIp(ip: String) {
        val ipPair = IpUtil.getLongsIpV6Address(ip)
        addressHighBits = ipPair.highBits
        addressLowBits = ipPair.lowBits
    }
}
