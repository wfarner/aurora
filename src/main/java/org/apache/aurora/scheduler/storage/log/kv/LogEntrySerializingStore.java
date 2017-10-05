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
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;

import org.apache.aurora.common.quantity.Amount;
import org.apache.aurora.common.quantity.Data;
import org.apache.aurora.gen.storage.LogEntry;
import org.apache.aurora.scheduler.storage.log.Framing;
import org.apache.aurora.scheduler.storage.log.kv.KeyValueStore.Record;
import org.apache.aurora.scheduler.storage.log.kv.KeyValueStore.StoreException;

import static java.util.Objects.requireNonNull;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * A key-value store that serializes {@link LogEntry log entries}.  Entries stored in the underlying
 * key-value store are bounded in size.  Serialized entries larger than the limit will be split into
 * multiple underlying entries, but are accessible to the caller as a single logical entry.
 */
class LogEntrySerializingStore implements
    AppendOnlyStore<Map.Entry<String, Record<LogEntry>>, Map.Entry<String, LogEntry>> {

  private static final HashFunction CHECKSUM = Hashing.crc32c();

  private final KeyValueStore.Streamable<String, byte[]> wrapped;
  private final Amount<Integer, Data> entrySizeLimit;

  // Maps from key to number of chunks for that key, if the key was chunked.
  private final Map<String, Integer> chunkedKeys = Maps.newHashMap();

  LogEntrySerializingStore(
      KeyValueStore.Streamable<String, byte[]> wrapped,
      Amount<Integer, Data> entrySizeLimit) {

    this.wrapped = requireNonNull(wrapped);
    checkArgument(entrySizeLimit.getValue() > 0);
    this.entrySizeLimit = entrySizeLimit;
  }

  @Override
  public Stream<Map.Entry<String, LogEntry>> stream() throws StoreException {
    return wrapped.stream()
        .filter(e -> visibleKey(e.getKey()))
        .map(e -> new AbstractMap.SimpleImmutableEntry<>(e.getKey(), getInternal(e.getKey())));
  }

  private LogEntry getInternal(String key) throws StoreException {
    checkArgument(visibleKey(key));

    AtomicInteger chunkIndex = new AtomicInteger(-1);
    Iterator<byte[]> chunks = new AbstractIterator<byte[]>() {
      @Override
      protected byte[] computeNext() {
        int pos = chunkIndex.getAndIncrement();
        String underlyingKey;
        if (pos == -1) {
          // Read the externally-visible representative key first.  If the underlying value is
          // non-framed, the frame reader will not read any chunk keys.
          underlyingKey = key;
        } else {
          // The initial entry was a frame header, we are now reading chunks.
          underlyingKey = chunkKey(key, pos);
        }

        Optional<byte[]> result = wrapped.get(underlyingKey);
        if (!result.isPresent()) {
          endOfData();
        }

        return result.orElse(null);
      }
    };

    LogEntry entry = requireNonNull(
        Framing.readAndUnframeSerialized(chunks, CHECKSUM.newHasher()),
        "Failed to read entry at key " + key);
    if (chunkIndex.get() > 0) {
      chunkedKeys.put(key, chunkIndex.get());
    }
    return entry;
  }

  @Override
  public void save(Stream<Map.Entry<String, Record<LogEntry>>> values) throws StoreException {
    ImmutableMap.Builder<String, Record<byte[]>> save = ImmutableMap.builder();

    values.forEach(entry -> {
      String key = entry.getKey();
      checkArgument(visibleKey(key));
      Record<LogEntry> record = entry.getValue();

      LogEntry logEntry = record.getValue();
      checkArgument(!logEntry.isSetFrame());

      Iterator<byte[]> encoded =
          Framing.serializeWithFrames(logEntry, entrySizeLimit.as(Data.BYTES), CHECKSUM);

      // The first entry is either the header or the full entity if framing was not necessary.
      save.put(
          key,
          record.isTombstone() ? Record.tombstone(encoded.next()) : Record.value(encoded.next()));

      AtomicInteger chunkIndex = new AtomicInteger(0);
      while (encoded.hasNext()) {
        save.put(
            chunkKey(key, chunkIndex.getAndIncrement()),
            record.isTombstone() ? Record.tombstone(encoded.next()) : Record.value(encoded.next()));
      }

      Integer previousChunks = chunkedKeys.remove(key);
      if (chunkIndex.get() > 0) {
        chunkedKeys.put(key, chunkIndex.get());
      }

      // Any chunks not overwritten by this operation must be tombstoned to ensure they are
      // eventually deleted.  Replaced chunk keys will be automatically deleted by the
      // underlying transacitonal store.
      if (previousChunks != null && previousChunks > chunkIndex.get()) {
        for (int i = chunkIndex.get(); i < previousChunks; i++) {
          save.put(chunkKey(key, i), Record.tombstone());
        }
      }
    });

    wrapped.save(save.build());
  }

  private static String chunkKey(String key, int chunk) {
    return key + "." + chunk + ".part";
  }

  private static boolean visibleKey(String key) {
    return !key.endsWith(".part");
  }
}
