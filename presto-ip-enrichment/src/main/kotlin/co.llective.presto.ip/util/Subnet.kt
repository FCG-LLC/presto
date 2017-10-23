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

interface Subnet {

    /**
     * Gets mask representation for given length and size
     * @param maskLength length of the mask, cannot surpass maskSize
     * @param maskSize maximum of 64
     * @return mask representation in long value
     */
    fun getMask(maskLength: Int, maskSize: Int): Long {
        var mask = 0L
        for (i in maskSize - 1 downTo maskSize - maskLength) {
            mask = mask or (1L shl i)
        }
        return mask
    }

    fun get64Mask(maskLength: Int): Long {
        return getMask(maskLength, java.lang.Long.SIZE)
    }

    fun get32Mask(maskLength: Int): Long {
        return getMask(maskLength, Integer.SIZE)
    }
}
