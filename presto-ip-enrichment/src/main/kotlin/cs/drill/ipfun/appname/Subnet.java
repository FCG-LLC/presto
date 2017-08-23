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

public interface Subnet {

  /**
   * Gets mask representation for given length and size
   * @param maskLength length of the mask, cannot surpass maskSize
   * @param maskSize maximum of 64
   * @return mask representation in long value
   */
  default long getMask(int maskLength, int maskSize) {
    long mask = 0L;
    for (int i = maskSize - 1; i >= maskSize - maskLength; i--) {
      mask |= 1L << i;
    }
    return mask;
  }

  default long get64Mask(int maskLength) {
    return getMask(maskLength, Long.SIZE);
  }

  default long get32Mask(int maskLength) {
    return getMask(maskLength, Integer.SIZE);
  }
}
