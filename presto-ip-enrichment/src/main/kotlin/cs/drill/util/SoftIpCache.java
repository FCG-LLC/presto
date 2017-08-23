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

import java.lang.ref.SoftReference;
import java.util.HashMap;
import java.util.Map;

public class SoftIpCache<T> implements IpCache<T> {
    // TODO: after longer validation on larger amounts of data and different server configurations,
    //       this approach can be changed into keeping SoftReferences for inner map keys instead of
    //       one SoftReference for the whole map
    private SoftReference<Map<Long, Map<Long, T>>> mapRef = new SoftReference<>(null);

    private Map<Long, T> getInnerMap(long ip1) {
      Map<Long, Map<Long, T>> map = mapRef.get();
      if (map == null) {
        map = new HashMap<>();
        mapRef = new SoftReference<>(map);
      }

      Map<Long, T> inner = map.get(ip1);
      if (inner == null) {
        inner = new HashMap<>();
        map.put(ip1, inner);
      }
      return inner;
    }

    public T get(long ip1, long ip2) {
      Map<Long, T> inner = getInnerMap(ip1);
      return inner.get(ip2);
    }

    public void put(long ip1, long ip2, T value) {
      Map<Long, T> inner = getInnerMap(ip1);
      inner.put(ip2, value);
    }
}
