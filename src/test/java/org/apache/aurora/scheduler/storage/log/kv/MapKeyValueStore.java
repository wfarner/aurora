/**
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
package org.apache.aurora.scheduler.storage.log.kv;

import java.util.Map;
import java.util.Optional;
import java.util.Set;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import org.apache.aurora.scheduler.storage.log.kv.KeyValueStore.ListableWithDelete;

import static org.apache.aurora.scheduler.storage.log.kv.TransactionalKeyValueStore.LAST_TXN_KEY;

public class MapKeyValueStore implements ListableWithDelete<String, byte[]> {

  private final Map<String, byte[]> contents = Maps.newHashMap();

  @VisibleForTesting
  Map<String, byte[]> asMap() {
    return contents;
  }

  @Override
  public Set<String> keys() throws StoreException {
    return contents.keySet();
  }

  @Override
  public Optional<byte[]> get(String key) {
    return Optional.ofNullable(contents.get(key));
  }

  @Override
  public void save(Map<String, Record<byte[]>> records) {
    records.forEach((key, record) -> {
      if (contents.put(key, record.getValue()) != null && !key.equals(LAST_TXN_KEY)) {
        throw new IllegalArgumentException("Entities may not be overwritten");
      }
    });
  }

  @Override
  public void delete(Set<String> keys) {
    if (!contents.keySet().containsAll(keys)) {
      throw new IllegalArgumentException(
          "Attempted to delete non-existent keys: " + Sets.difference(keys, contents.keySet()));
    }
    contents.keySet().removeAll(keys);
  }
}
