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

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.stream.Stream;

import javax.inject.Inject;

import com.google.common.collect.ImmutableList;

import org.apache.aurora.codec.ThriftBinaryCodec.CodingException;
import org.apache.aurora.gen.storage.LogEntry;
import org.apache.aurora.gen.storage.Op;
import org.apache.aurora.gen.storage.Snapshot;
import org.apache.aurora.gen.storage.Transaction;
import org.apache.aurora.scheduler.log.Log.Position;
import org.apache.aurora.scheduler.log.Log.Stream.StreamAccessException;
import org.apache.aurora.scheduler.storage.log.AbstractStreamTransaction;
import org.apache.aurora.scheduler.storage.log.StreamManager;
import org.apache.aurora.scheduler.storage.log.StreamTransaction;
import org.apache.aurora.scheduler.storage.log.kv.KeyValueStore.StoreException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.Objects.requireNonNull;

import static com.google.common.base.Preconditions.checkState;

/**
 * A log-structured stream in which operations are idempotent and can be uniquely associated
 * with the entity modified.  This property is used to persist in a {@link AppendOnlyStore}
 * and eliminate the need for snapshots.
 */
public class KeyedStreamManager implements StreamManager {

  private static final Logger LOG = LoggerFactory.getLogger(KeyedStreamManager.class);

  private final AppendOnlyStore<Op, Op> store;
  private final AtomicBoolean recovered = new AtomicBoolean(false);

  @Inject
  KeyedStreamManager(AppendOnlyStore<Op, Op> store) {
    this.store = requireNonNull(store);
  }

  @Override
  public void readFromBeginning(Consumer<LogEntry> reader)
      throws CodingException, StreamAccessException {

    Stream<Op> values;
    try {
      // Replicate behavior of snapshot recovery - stream entity types in a specific order.
      values = store.stream();
    } catch (StoreException e) {
      throw new StreamAccessException("Failed to list keys", e);
    }

    // TODO(wfarner): If any struct types must be played in a specific order (for example,
    // job udpate events _after_ job updates), they may be filtered here for ordering.
    values.forEach(entry -> {
      reader.accept(LogEntry.transaction(new Transaction().setOps(ImmutableList.of(entry))));
    });

    recovered.set(true);
  }

  @Override
  public void snapshot(Snapshot snapshot) {
    LOG.warn("A snapshot was requested, but this storage system does not support snapshots.");
  }

  @Override
  public StreamTransaction startTransaction() {
    return new AbstractStreamTransaction() {
      @Override
      protected void doCommit(List<Op> ops) throws CodingException {
        KeyedStreamManager.this.commit(ops);
      }
    };
  }

  private void commit(List<Op> transaction) throws CodingException, StreamAccessException {
    checkState(recovered.get(), "A commit cannot occur before recovery");

    try {
      store.save(transaction.stream());
    } catch (StoreException e) {
      throw new StreamAccessException("Failed to save to KV store", e);
    }
  }

  @Override
  public void truncateBefore(Position position) throws StreamAccessException {
    throw new UnsupportedOperationException();
  }
}
