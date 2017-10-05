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
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

import javax.annotation.Nullable;

import com.google.common.base.Preconditions;

import static java.util.Objects.requireNonNull;

/**
 * A key-value oriented store.
 *
 * @param <K> Key type.
 * @param <V> Value type.
 */
public interface KeyValueStore<K, V> {

  /**
   * Fetches a single record from the store.
   *
   * @param key key of the record to fetch.
   * @return The stored record, if one exists.
   * @throws StoreException If the operation failed.
   */
  Optional<V> get(K key) throws StoreException;

  /**
   * Saves a batch of records to the store.
   *
   * @param records Records to save.
   * @throws StoreException If the operation failed.
   */
  void save(Map<K, Record<V>> records) throws StoreException;

  /**
   * Thrown when a storage operation fails.
   */
  class StoreException extends RuntimeException {
    public StoreException(String msg) {
      super(msg);
    }
    public StoreException(String msg, Throwable cause) {
      super(msg, cause);
    }
  }

  /**
   * A key-value store that supports streaming to retrieve all saved records.
   *
   * @param <K> Key type.
   * @param <V> Value type.
   */
  interface Streamable<K, V> extends KeyValueStore<K, V> {

    /**
     * Retrieves all records from the store.
     *
     * @return A stream containing all records in the store at the time of invocation.
     * @throws StoreException If the operation failed.
     */
    Stream<Entry<K, V>> stream() throws StoreException;
  }

  /**
   * A key-value store that supports listing all available keys and record deletion.  Intended to
   * be the lowest-level key-value interface.
   *
   * @param <K> Key type.
   * @param <V> Value type.
   */
  interface ListableWithDelete<K, V> extends KeyValueStore<K, V> {

    /**
     * Retrieves all keys available in the store.
     *
     * @return All stored record keys.
     * @throws StoreException If the operation failed.
     */
    Set<String> keys() throws StoreException;

    /**
     * Deletes a batch of records.
     *
     * @param keys Keys of the recoreds to delete.
     * @throws StoreException If the operation failed.
     */
    void delete(Set<K> keys) throws StoreException;
  }

  /**
   * A stored record, possibly containing a value.  A record may be a tombstone, in which case
   * it does not require an underlying value.  All non-tombstone records must contain a value.
   *
   * @param <V> Value type.
   */
  class Record<V> {
    private final boolean tombstone;
    private final Optional<V> value;

    private Record(boolean tombstone, Optional<V> value) {
      Preconditions.checkState(tombstone || value.isPresent());

      this.tombstone = tombstone;
      this.value = requireNonNull(value);
    }

    public boolean isTombstone() {
      return tombstone;
    }

    @Nullable
    public V getValue() {
      return value.orElse(null);
    }

    public static <V> Record<V> value(V value) {
      return new Record<>(false, Optional.of(value));
    }

    public static <V> Record<V> tombstone(V value) {
      return new Record<>(true, Optional.of(value));
    }

    public static <V> Record<V> tombstone() {
      return new Record<>(true, Optional.empty());
    }
  }
}
