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

import java.util.AbstractMap;
import java.util.Map;
import java.util.stream.Stream;

import org.apache.aurora.scheduler.storage.log.kv.KeyValueStore.Record;
import org.apache.aurora.scheduler.storage.log.kv.KeyValueStore.StoreException;
import org.apache.commons.codec.digest.DigestUtils;

import static java.util.Objects.requireNonNull;

class HashedKeyValueStore<V> implements AppendOnlyStore<Map.Entry<String, Record<V>>, V> {

  private final AppendOnlyStore<Map.Entry<String, Record<V>>, Map.Entry<String, V>> wrapped;

  HashedKeyValueStore(AppendOnlyStore<Map.Entry<String, Record<V>>, Map.Entry<String, V>> wrapped) {
    this.wrapped = requireNonNull(wrapped);
  }

  @Override
  public Stream<V> stream() throws StoreException {
    return wrapped.stream().map(Map.Entry::getValue);
  }

  @Override
  public void save(Stream<Map.Entry<String, Record<V>>> values) throws StoreException {
    wrapped.save(values
        .map(e -> new AbstractMap.SimpleImmutableEntry<>(
            hashAndNamespace(e.getKey()),
            e.getValue())));
  }

  private static String hashAndNamespace(String key) {
    String hash = DigestUtils.shaHex(key);

    // Add two characters of the hash as a namespace to reduce the number of children in a
    // given namespace.
    return "/" + hash.substring(0, 2) + "/" + hash.substring(3);
  }
}
