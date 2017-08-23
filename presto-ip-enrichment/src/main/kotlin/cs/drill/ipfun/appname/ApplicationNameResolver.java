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
package cs.drill.ipfun.appname;

import cs.drill.util.FileReader;
import cs.drill.util.IpUtil;
import cs.drill.util.SoftIpCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.LinkedHashMap;
import java.util.Map;

public class ApplicationNameResolver {
  private static final String UNKNOWN_NAME = ""; // empty string is marking ip in cache as not named
  private static final String FILE_NAME = "application_name.txt";
  private static final String PORTS_FILE_NAME = "application_ports.csv";
  private static Map<SubnetV4, String> ipv4Subnets = new LinkedHashMap<>();
  private static Map<SubnetV6, String> ipv6Subnets = new LinkedHashMap<>();
  private static String[] portNames = new String[49151]; // largest not ephemeral port number
  private static final Logger LOGGER = LoggerFactory.getLogger(ApplicationNameResolver.class);
  /**
   * Contains counted before application names for given IP pairs.
   */
  private static final SoftIpCache<String> cache = new SoftIpCache<>();

  static {
    populateNames();
    populatePortNames();
  }

  private static void populateNames() {
    FileReader fileReader = new FileReader(FILE_NAME, "\t");
    fileReader.processFile((line) -> {
      if (line.length != 2) {
        LOGGER.warn("Line doesn't have 2 expected columns");
        return;
      }

      String subnet = line[0];
      String applicationName = line[1];

      int index = subnet.indexOf("/");

      String address = subnet.substring(0, index);
      int maskLength = Integer.parseInt(subnet.substring(index + 1));

      try {
        InetAddress inetAddress = InetAddress.getByName(address);

        if (inetAddress instanceof Inet4Address) {
          SubnetV4 subnetV4 = new SubnetV4(address, maskLength);
          ipv4Subnets.put(subnetV4, applicationName);
        } else if (inetAddress instanceof Inet6Address) {
          SubnetV6 subnetV6 = new SubnetV6(address, maskLength);
          ipv6Subnets.put(subnetV6, applicationName);
        } else {
          throw new UnknownHostException();
        }
      } catch (UnknownHostException e) {
        throw new IllegalArgumentException("Wrong IP address " + address);
      }
    });
  }

  private static void populatePortNames() {
    FileReader fileReader = new FileReader(PORTS_FILE_NAME, ",");
    fileReader.processFile((line) -> {
      if (line.length < 2) {
        LOGGER.warn("Line doesn't have at least 2 expected columns");
        return;
      }

      String port = line[0];
      String applicationName = line[1];
      portNames[Integer.parseInt(port)] = applicationName;
    });
  }

  public static String getApplicationName(long ip1, long ip2, int port) {
    String cacheValue = cache.get(ip1, ip2);
    if (cacheValue != null) {
      return cacheValue.equals(UNKNOWN_NAME) ? getPortName(port) : cacheValue;
    }

    if (ip1 == IpUtil.WKP) {
      for (Map.Entry<SubnetV4, String> entry : ipv4Subnets.entrySet()) {
        SubnetV4 subnet = entry.getKey();
        if ((subnet.getMask() & ip2) == subnet.getAddress()) {
          cache.put(ip1, ip2, entry.getValue());
          return entry.getValue();
        }
      }
    } else {
      for (Map.Entry<SubnetV6, String> entry : ipv6Subnets.entrySet()) {
        SubnetV6 subnet = entry.getKey();
        if ((subnet.getMaskHighBits() & ip1) == subnet.getAddressHighBits()
          && (subnet.getMaskLowBits() & ip2) == subnet.getAddressLowBits()) {
          cache.put(ip1, ip2, entry.getValue());
          return entry.getValue();
        }
      }
    }

    cache.put(ip1, ip2, UNKNOWN_NAME);
    return getPortName(port);
  }

  public static String getApplicationName(long ip1, long ip2) {
    return getApplicationName(ip1, ip2, -1);
  }

  public static String getPortName(int port) {
    return port > 0 && port < portNames.length ? portNames[port] : null;
  }
}
