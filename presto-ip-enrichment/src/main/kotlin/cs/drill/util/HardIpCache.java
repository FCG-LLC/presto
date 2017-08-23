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

import java.util.HashMap;
import java.util.Map;

public class HardIpCache<T> implements IpCache<T> {
  private Map<Long, Map<Long, T>> map = new HashMap<>();

  public T get(long ip1, long ip2) {
    Map<Long, T> inner = map.get(ip1);
    return inner == null ? null : inner.get(ip2);
  }

  public void put(long ip1, long ip2, T value) {
    Map<Long, T> inner = map.computeIfAbsent(ip1, k -> new HashMap<>());
    inner.put(ip2, value);
  }
}
