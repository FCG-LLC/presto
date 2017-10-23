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

import org.apache.commons.lang3.StringUtils

object IpUtil {
    val WKP = 0x64ff9b0000000000L
    private val IPV6_SPLIT = ":"

    fun getLongIpV4Address(address: String): Long {
        val parts = address.split(".").map { it.toLong() }
        val lowBits = (parts[0] shl 24)
            + (parts[1] shl 16)
            + (parts[2] shl 8)
            + parts[3]
        return lowBits
    }

    fun getLongsIpV6Address(ip: String): IpPair {
        val numbers = getNumbers(ip)

        var highBits = numbers[0]
        for (i in 1..3) {
            highBits = (highBits shl 16) + numbers[i]
        }

        var lowBits = numbers[4]
        for (i in 5..7) {
            lowBits = (lowBits shl 16) + numbers[i]
        }

        return IpPair(highBits, lowBits)
    }

    private fun getNumbers(ip: String): LongArray {
        var ip = ip
        val numbers = LongArray(8)
        val semicolonsCount = StringUtils.countMatches(ip, IPV6_SPLIT)
        if (semicolonsCount < 7) {
            val doubleSemicolonIndex = StringUtils.indexOf(ip, IPV6_SPLIT + IPV6_SPLIT)
            for (i in 0..7 - semicolonsCount - 1) {
                ip = insertStringAtPosition(ip, IPV6_SPLIT, doubleSemicolonIndex)
            }
        }
        val parts = ip.split(IPV6_SPLIT.toRegex()).dropLastWhile { it.isEmpty() }.toTypedArray()
        for (i in parts.indices) {
            if (parts[i].isEmpty()) {
                continue
            }
            numbers[i] = java.lang.Long.parseLong(parts[i], 16)
        }
        return numbers
    }

    private fun insertStringAtPosition(string: String, insertedString: String, index: Int): String {
        val sb = StringBuilder(string)
        sb.insert(index, insertedString)
        return sb.toString()
    }

    class IpPair(val highBits: Long, val lowBits: Long)
}
