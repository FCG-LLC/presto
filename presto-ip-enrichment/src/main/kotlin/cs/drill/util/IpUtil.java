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
package cs.drill.util;

import org.apache.commons.lang3.StringUtils;

public final class IpUtil {
  public static final long WKP = 0x64ff9b0000000000L;
  private static final String IPV6_SPLIT = ":";

  public static long getLongIpV4Address(String address) {
    String[] parts = address.split("\\.");
    long lowBits = (Long.parseLong(parts[0]) << 24)
      + (Long.parseLong(parts[1]) << 16)
      + (Long.parseLong(parts[2]) << 8)
      + Long.parseLong(parts[3]);
    return lowBits;
  }

  public static IpPair getLongsIpV6Address(String ip) {
    long[] numbers = getNumbers(ip);

    long highBits = numbers[0];
    for (int i = 1; i < 4; i++) {
      highBits = (highBits << 16) + numbers[i];
    }

    long lowBits = numbers[4];
    for (int i = 5; i < 8; i++) {
      lowBits = (lowBits << 16) + numbers[i];
    }

    return new IpPair(highBits, lowBits);
  }

  private static long[] getNumbers(String ip) {
    long[] numbers = new long[8];
    int semicolonsCount = StringUtils.countMatches(ip, IPV6_SPLIT);
    if (semicolonsCount < 7) {
      int doubleSemicolonIndex = StringUtils.indexOf(ip, IPV6_SPLIT + IPV6_SPLIT);
      for (int i = 0; i < 7 - semicolonsCount; i++) {
        ip = insertStringAtPosition(ip, IPV6_SPLIT, doubleSemicolonIndex);
      }
    }
    String[] parts = ip.split(IPV6_SPLIT);
    for (int i = 0; i < parts.length; i++) {
      if (parts[i].isEmpty()) {
        continue;
      }
      numbers[i] = Long.parseLong(parts[i], 16);
    }
    return numbers;
  }

  private static String insertStringAtPosition(String string, String insertedString, int index) {
    StringBuilder sb = new StringBuilder(string);
    sb.insert(index, insertedString);
    return sb.toString();
  }

  public static class IpPair {
    private long highBits;
    private long lowBits;

    public IpPair(long highBits, long lowBits) {
      this.highBits = highBits;
      this.lowBits = lowBits;
    }

    public long getHighBits() {
      return highBits;
    }

    public long getLowBits() {
      return lowBits;
    }
  }

  private IpUtil() {}
}
