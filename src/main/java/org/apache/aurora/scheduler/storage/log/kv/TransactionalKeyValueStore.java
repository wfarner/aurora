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
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Ordering;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.Objects.requireNonNull;

/**
 * A key-value store that provides transactional behavior for the <b>last</b> call to
 * {@link #save(Map)}.  This is particularly useful when the underlying key-value store does not
 * support transactions across multiple keys.
 */
class TransactionalKeyValueStore implements KeyValueStore.Streamable<String, byte[]> {
  private static final Logger LOG = LoggerFactory.getLogger(TransactionalKeyValueStore.class);

  @VisibleForTesting
  static final String LAST_TXN_KEY = "/last_transaction";
  private static final String TOMBSTONE_MARKER = ".tombstone";

  private final KeyValueStore.ListableWithDelete<String, byte[]> wrapped;

  private final AtomicLong nextTxnId = new AtomicLong(-1);

  // Associates logical keys to the transaction IDs they were written in.
  private final Map<String, Long> logicalKeys = Maps.newHashMap();

  TransactionalKeyValueStore(KeyValueStore.ListableWithDelete<String, byte[]> wrapped) {
    this.wrapped = requireNonNull(wrapped);
  }

  @Override
  public Stream<Entry<String, byte[]>> stream() throws StoreException {
    long lastTxnId = wrapped.get(LAST_TXN_KEY)
        .map(v -> Long.valueOf(new String(v, Charsets.US_ASCII)))
        .orElse(-1L);
    Preconditions.checkState(lastTxnId >= -1);

    if (lastTxnId == -1) {
      LOG.info("No last_transaction recovered, store will be treated as empty");
    } else {
      LOG.info("Recovered last_transaction " + lastTxnId);
    }

    nextTxnId.set(lastTxnId + 1);

    return recoverOrderedKeys(lastTxnId)
        .map(key -> new AbstractMap.SimpleImmutableEntry<>(
              key.logical,
              wrapped.get(key.physical()).get()));
  }

  private Stream<Key> recoverOrderedKeys(long lastTxnId) {
    // Sort the keys.  This simplifies read-repair by avoiding the need to handle
    // non-sequential playback order.
    List<Key> orderedKeys = wrapped.keys().stream()
        .filter(key -> !LAST_TXN_KEY.equals(key))
        .map(Key::parse)
        .sorted(Ordering.natural().onResultOf(key -> key.txnId))
        .collect(Collectors.toList());

    Map<String, String> tombstones = Maps.newHashMap();

    // Walk the keys in reverse order and populate logicalKeyToTxn.  Walking in reverse
    // simplifies the process of discarding outdated records.
    ListIterator<Key> reversedKeys = orderedKeys.listIterator(orderedKeys.size());
    while (reversedKeys.hasPrevious()) {
      Key key = reversedKeys.previous();

      boolean discard = false;

      if (key.txnId > lastTxnId) {
        LOG.info("Cleaning up key from uncommitted transaction: " + key);
        discard = true;
      } else if (tombstones.containsKey(key.logical)) {
        LOG.info("Cleaning up incomplete delete for " + key);
        discard = true;
      } else {
        Long newerRecordTxnId = logicalKeys.get(key.logical);
        if (newerRecordTxnId != null) {
          Preconditions.checkState(newerRecordTxnId > key.txnId);
          LOG.info("Cleaning up stale record {}, replaced in txn {}", key, newerRecordTxnId);
          discard = true;
        }
      }

      if (discard) {
        wrapped.delete(ImmutableSet.of(key.physical()));
        reversedKeys.remove();
      } else {
        if (key.tombstone) {
          tombstones.put(key.logical, key.physical());
          reversedKeys.remove();
        } else {
          logicalKeys.put(key.logical, key.txnId);
        }
      }
    }

    // Delete any orphaned tombstones.  This is done outside the read-repair loop above to ensure
    // any referenced records are deleted first.  Otherwise, it would be possible to delete the
    // tombstone and crash before deleting the tombstoned record; resurrecting the value.
    if (!tombstones.isEmpty()) {
      wrapped.delete(ImmutableSet.copyOf(tombstones.values()));
    }

    return orderedKeys.stream();
  }

  @Override
  public Optional<byte[]> get(String key) throws StoreException {
    Preconditions.checkArgument(!LAST_TXN_KEY.equals(key));
    checkTxnId();

    return Optional.ofNullable(logicalKeys.get(key))
        .map(txnId -> getPhysicalKey(key, txnId))
        .flatMap(wrapped::get);
  }

  @Override
  public void save(Map<String, Record<byte[]>> records) throws StoreException {
    Preconditions.checkArgument(!records.containsKey(LAST_TXN_KEY));
    Preconditions.checkArgument(
        records.keySet().stream().noneMatch(k -> k.endsWith(TOMBSTONE_MARKER)));
    checkTxnId();

    long txnId = nextTxnId.getAndIncrement();

    // Builders ensure iteration order, as the store ensures persistence in iteration order.
    ImmutableSet.Builder<String> staleKeys = ImmutableSet.builder();
    ImmutableMap.Builder<String, Record<byte[]>> save =
        ImmutableMap.builderWithExpectedSize(records.size());

    records.forEach((logicalKey, record) -> {
      // Record the txn ID for this key, and queue the replaced key for deletion.
      Optional.ofNullable(logicalKeys.put(logicalKey, txnId))
          .ifPresent(prevTxnId -> staleKeys.add(getPhysicalKey(logicalKey, prevTxnId)));
      Key key = new Key(logicalKey, record.isTombstone(), txnId);
      if (key.tombstone) {
        // This is a tombstone record; queue the tombstone itself for deletion.  This prevents
        // tombstone records from lingering in storage (similar to a database vacuum process).
        staleKeys.add(key.physical());
      }

      save.put(key.physical(), record);
    });

    // Based on the contract that map entries are persisted in (key) iteration order, add the
    // LAST_TXN_KEY to the to-be-saved values.  This allows the underlying store to save the entire
    // batch in one operation, if possible.
    save.put(
        LAST_TXN_KEY,
        Record.value(String.valueOf(txnId).getBytes(Charsets.US_ASCII)));

    wrapped.save(save.build());

    // TODO(wfarner): This can be done asynchronously once save() completes.
    Set<String> delete = staleKeys.build();
    if (!delete.isEmpty()) {
      LOG.info("Deleting stale keys: " + delete);
      wrapped.delete(delete);
    }
  }

  private static String getPhysicalKey(String key, long txnId) {
    return key + "." + txnId;
  }

  private static class Key {
    final String logical;
    final boolean tombstone;
    final long txnId;

    Key(String logical, boolean tombstone, long txnId) {
      this.logical = logical;
      this.tombstone = tombstone;
      this.txnId = txnId;
    }

    static Key parse(String physicalKey) {
      int lastDotPos = physicalKey.lastIndexOf('.');
      long txnId = Long.parseLong(physicalKey.substring(lastDotPos + 1));
      String remainder = physicalKey.substring(0, lastDotPos);
      boolean tombstone = remainder.endsWith(TOMBSTONE_MARKER);
      String logical = tombstone ? remainder.substring(0, remainder.lastIndexOf('.')) : remainder;
      return new Key(logical, tombstone, txnId);
    }

    String physical() {
      return getPhysicalKey(logical + (tombstone ? TOMBSTONE_MARKER : ""), txnId);
    }

    @Override
    public String toString() {
      return physical();
    }
  }

  private void checkTxnId() throws StoreException {
    Preconditions.checkState(
        nextTxnId.get() != -1,
        "Invalid transaction ID, likely due to not calling stream() first.");
  }
}
