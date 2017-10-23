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

import java.util.HashMap

class HardIpCache<T> : IpCache<T> {
    private val map = HashMap<Long, MutableMap<Long, T?>>()

    override fun get(ip1: Long, ip2: Long): T? {
        val inner = map[ip1]
        return inner?.get(ip1)
    }

    override fun put(ip1: Long, ip2: Long, value: T) {
        val inner = (map as MutableMap<Long, MutableMap<Long, T?>>)
                .computeIfAbsent(ip1) { HashMap() } as HashMap<Long, T?>
        inner.put(ip2, value)
    }
}
