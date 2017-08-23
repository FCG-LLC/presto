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

import cs.drill.util.IpUtil;

public class SubnetV6 implements Subnet{

  private long addressHighBits;
  private long addressLowBits;
  private long maskHighBits;
  private long maskLowBits;

  public long getAddressHighBits() {
    return addressHighBits;
  }

  public long getAddressLowBits() {
    return addressLowBits;
  }

  public long getMaskHighBits() {
    return maskHighBits;
  }

  public long getMaskLowBits() {
    return maskLowBits;
  }

  public SubnetV6(String address, int maskLength) {
    processIp(address);
    processMask(maskLength);
  }

  private void processMask(int maskLength) {
    if (maskLength > Long.SIZE) {
      maskHighBits = get64Mask(Long.SIZE);
      maskLowBits = get64Mask(maskLength - Long.SIZE);
    } else {
      maskHighBits = get64Mask(maskLength);
      maskLowBits = 0L;
    }
  }

  private void processIp(String ip) {
    IpUtil.IpPair ipPair = IpUtil.getLongsIpV6Address(ip);
    addressHighBits = ipPair.getHighBits();
    addressLowBits = ipPair.getLowBits();
  }
}
