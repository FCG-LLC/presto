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

import java.lang.ref.SoftReference
import java.util.HashMap

class SoftIpCache<T> : IpCache<T> {
    // TODO: after longer validation on larger amounts of data and different server configurations,
    //       this approach can be changed into keeping SoftReferences for inner map keys instead of
    //       one SoftReference for the whole map
    private var mapRef = SoftReference<MutableMap<Long, MutableMap<Long, T?>>>(null)

    private fun getInnerMap(ip1: Long): MutableMap<Long, T?> {
        var map: MutableMap<Long, MutableMap<Long, T?>>? = mapRef.get()
        if (map == null) {
            map = HashMap()
            mapRef = SoftReference(map)
        }

        var inner: MutableMap<Long, T?>? = map[ip1]
        if (inner == null) {
            inner = HashMap()
            map.put(ip1, inner)
        }
        return inner
    }

    override fun get(ip1: Long, ip2: Long): T? {
        val inner = getInnerMap(ip1)
        return inner[ip2]
    }

    override fun put(ip1: Long, ip2: Long, value: T?) {
        val inner = getInnerMap(ip1)
        inner.put(ip2, value)
    }
}
