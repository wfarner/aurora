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
package org.apache.aurora.scheduler.storage.log;

import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import javax.annotation.Nullable;
import javax.inject.Inject;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hasher;
import com.google.common.primitives.Bytes;
import com.google.inject.assistedinject.Assisted;

import org.apache.aurora.common.stats.Stats;
import org.apache.aurora.gen.ScheduledTask;
import org.apache.aurora.gen.storage.Frame;
import org.apache.aurora.gen.storage.FrameHeader;
import org.apache.aurora.gen.storage.LogEntry;
import org.apache.aurora.gen.storage.Op;
import org.apache.aurora.gen.storage.RemoveTasks;
import org.apache.aurora.gen.storage.SaveHostAttributes;
import org.apache.aurora.gen.storage.SaveTasks;
import org.apache.aurora.gen.storage.Snapshot;
import org.apache.aurora.gen.storage.Storage_Constants;
import org.apache.aurora.gen.storage.Transaction;
import org.apache.aurora.scheduler.log.Log;
import org.apache.aurora.scheduler.log.Log.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.Objects.requireNonNull;

import static org.apache.aurora.codec.ThriftBinaryCodec.CodingException;
import static org.apache.aurora.common.inject.TimedInterceptor.Timed;
import static org.apache.aurora.scheduler.log.Log.Stream.InvalidPositionException;
import static org.apache.aurora.scheduler.log.Log.Stream.StreamAccessException;
import static org.apache.aurora.scheduler.storage.log.LogManager.LogEntryHashFunction;

class StreamManagerImpl implements StreamManager {
  private static final Logger LOG = LoggerFactory.getLogger(StreamManagerImpl.class);

  private static class Vars {
    private final AtomicInteger unSnapshottedTransactions =
        Stats.exportInt("scheduler_log_un_snapshotted_transactions");
    private final AtomicLong bytesWritten = Stats.exportLong("scheduler_log_bytes_written");
    private final AtomicLong entriesWritten = Stats.exportLong("scheduler_log_entries_written");
    private final AtomicLong badFramesRead = Stats.exportLong("scheduler_log_bad_frames_read");
    private final AtomicLong bytesRead = Stats.exportLong("scheduler_log_bytes_read");
    private final AtomicLong entriesRead = Stats.exportLong("scheduler_log_entries_read");
    private final AtomicLong deflatedEntriesRead =
        Stats.exportLong("scheduler_log_deflated_entries_read");
    private final AtomicLong snapshots = Stats.exportLong("scheduler_log_snapshots");
  }
  private final Vars vars = new Vars();

  private final Object writeMutex = new Object();
  private final Log.Stream stream;
  private final EntrySerializer entrySerializer;
  private final HashFunction hashFunction;
  private final SnapshotDeduplicator snapshotDeduplicator;

  @Inject
  StreamManagerImpl(
      @Assisted Stream stream,
      EntrySerializer entrySerializer,
      @LogEntryHashFunction HashFunction hashFunction,
      SnapshotDeduplicator snapshotDeduplicator) {

    this.stream = requireNonNull(stream);
    this.entrySerializer = requireNonNull(entrySerializer);
    this.hashFunction = requireNonNull(hashFunction);
    this.snapshotDeduplicator = requireNonNull(snapshotDeduplicator);
  }

  @Override
  public void readFromBeginning(Consumer<LogEntry> reader)
      throws CodingException, InvalidPositionException, StreamAccessException {

    Iterator<Log.Entry> entries = stream.readAll();

    while (entries.hasNext()) {
      LogEntry logEntry = decodeLogEntry(entries.next());
      while (logEntry != null && isFrame(logEntry)) {
        logEntry = tryDecodeFrame(logEntry.getFrame(), entries);
      }
      if (logEntry != null) {
        if (logEntry.hasDeflatedEntry()) {
          logEntry = Entries.inflate(logEntry);
          vars.deflatedEntriesRead.incrementAndGet();
        }

        if (logEntry.hasDeduplicatedSnapshot()) {
          logEntry = LogEntry.withSnapshot(
              snapshotDeduplicator.reduplicate(logEntry.getDeduplicatedSnapshot()));
        }

        reader.accept(logEntry);
        vars.entriesRead.incrementAndGet();
      }
    }
  }

  @Nullable
  private LogEntry tryDecodeFrame(Frame frame, Iterator<Log.Entry> entries) throws CodingException {
    if (!isHeader(frame)) {
      LOG.warn("Found a frame with no preceding header, skipping.");
      return null;
    }
    FrameHeader header = frame.getHeader();
    byte[][] chunks = new byte[header.getChunkCount()][];

    Hasher hasher = hashFunction.newHasher();
    for (int i = 0; i < header.getChunkCount(); i++) {
      if (!entries.hasNext()) {
        logBadFrame(header, i);
        return null;
      }
      LogEntry logEntry = decodeLogEntry(entries.next());
      if (!isFrame(logEntry)) {
        logBadFrame(header, i);
        return logEntry;
      }
      Frame chunkFrame = logEntry.getFrame();
      if (!isChunk(chunkFrame)) {
        logBadFrame(header, i);
        return logEntry;
      }
      byte[] chunkData = chunkFrame.getChunk().getData().get();
      hasher.putBytes(chunkData);
      chunks[i] = chunkData;
    }
    if (!Arrays.equals(header.getChecksum().get(), hasher.hash().asBytes())) {
      throw new CodingException("Read back a framed log entry that failed its checksum");
    }
    return Entries.thriftBinaryDecode(Bytes.concat(chunks));
  }

  private static boolean isFrame(LogEntry logEntry) {
    return logEntry.hasFrame();
  }

  private static boolean isChunk(Frame frame) {
    return frame.hasChunk();
  }

  private static boolean isHeader(Frame frame) {
    return frame.hasHeader();
  }

  private void logBadFrame(FrameHeader header, int chunkIndex) {
    LOG.info(String.format("Found an aborted transaction, required %d frames and found %d",
        header.getChunkCount(), chunkIndex));
    vars.badFramesRead.incrementAndGet();
  }

  private LogEntry decodeLogEntry(Log.Entry entry) throws CodingException {
    byte[] contents = entry.contents();
    vars.bytesRead.addAndGet(contents.length);
    return Entries.thriftBinaryDecode(contents);
  }

  @Override
  public void truncateBefore(Log.Position position) {
    stream.truncateBefore(position);
  }

  @Override
  public StreamTransactionImpl startTransaction() {
    return new StreamTransactionImpl();
  }

  @Override
  @Timed("log_manager_snapshot")
  public void snapshot(Snapshot snapshot)
      throws CodingException, InvalidPositionException, StreamAccessException {

    LogEntry entry =
        deflate(LogEntry.withDeduplicatedSnapshot(snapshotDeduplicator.deduplicate(snapshot)));
    Log.Position position = appendAndGetPosition(entry);
    vars.snapshots.incrementAndGet();
    vars.unSnapshottedTransactions.set(0);
    stream.truncateBefore(position);
  }

  // Not meant to be subclassed, but timed methods must be non-private.
  // See https://github.com/google/guice/wiki/AOP#limitations
  @Timed("log_manager_deflate")
  protected LogEntry deflate(LogEntry entry) throws CodingException {
    return Entries.deflate(entry);
  }

  // Not meant to be subclassed, but timed methods must be non-private.
  // See https://github.com/google/guice/wiki/AOP#limitations
  @Timed("log_manager_append")
  protected Log.Position appendAndGetPosition(LogEntry logEntry) throws CodingException {
    Log.Position firstPosition = null;
    Iterable<byte[]> entries = entrySerializer.serialize(logEntry);
    synchronized (writeMutex) { // ensure all sub-entries are written as a unit
      for (byte[] entry : entries) {
        Log.Position position = stream.append(entry);
        if (firstPosition == null) {
          firstPosition = position;
        }
        vars.bytesWritten.addAndGet(entry.length);
      }
    }
    vars.entriesWritten.incrementAndGet();
    return firstPosition;
  }

  final class StreamTransactionImpl implements StreamTransaction {
    private final Transaction._Builder transaction =
        Transaction.builder().setSchemaVersion(Storage_Constants.CURRENT_SCHEMA_VERSION);
    private final AtomicBoolean committed = new AtomicBoolean(false);

    StreamTransactionImpl() {
      // supplied by factory method
    }

    @Override
    public Log.Position commit() throws CodingException {
      Preconditions.checkState(!committed.getAndSet(true),
          "Can only call commit once per transaction.");

      if (transaction.mutableOps().isEmpty()) {
        return null;
      }

      Log.Position position = appendAndGetPosition(LogEntry.withTransaction(transaction));
      vars.unSnapshottedTransactions.incrementAndGet();
      return position;
    }

    @Override
    public void add(Op op) {
      Preconditions.checkState(!committed.get());

      Op prior = !transaction.mutableOps().isEmpty()
          ? Iterables.getLast(transaction.mutableOps(), null) : null;
      if (prior == null) {
        transaction.addToOps(op);
      } else {
        Optional<Op> coalesced = coalesce(prior, op);
        coalesced.ifPresent(replacement -> {
          transaction.mutableOps().set(transaction.mutableOps().size() - 1, replacement);
        });
      }
    }

    /**
     * Tries to coalesce a new op into the prior to compact the binary representation and increase
     * batching.
     *
     * @param prior The previous op.
     * @param next The next op to be added.
     * @return {@code true} if the next op was coalesced into the prior, {@code false} otherwise.
     */
    private Optional<Op> coalesce(Op prior, Op next) {
      if (!prior.unionFieldIsSet() && !next.unionFieldIsSet()) {
        return Optional.empty();
      }

      Op._Field priorType = prior.unionField();
      if (!priorType.equals(next.unionField())) {
        return Optional.empty();
      }

      switch (priorType) {
        case SAVE_FRAMEWORK_ID:
          return Optional.of(Op.withSaveFrameworkId(next.getSaveFrameworkId()));
        case SAVE_TASKS:
          return Optional.of(Op.withSaveTasks(coalesce(prior.getSaveTasks(), next.getSaveTasks())));
        case REMOVE_TASKS:
          return Optional.of(
              Op.withRemoveTasks(coalesce(prior.getRemoveTasks(), next.getRemoveTasks())));
        case SAVE_HOST_ATTRIBUTES:
          return coalesce(prior.getSaveHostAttributes(), next.getSaveHostAttributes())
              .map(Op::withSaveHostAttributes);
        default:
          return Optional.empty();
      }
    }

    private SaveTasks coalesce(SaveTasks prior, SaveTasks next) {
      // It is an expected invariant that an operation may reference a task (identified by
      // task ID) no more than one time.  Therefore, to coalesce two SaveTasks operations,
      // the most recent task definition overrides the prior operation.
      Map<String, ScheduledTask> coalesced = Maps.newHashMap();
      prior.getTasks().forEach(task -> coalesced.put(task.getAssignedTask().getTaskId(), task));
      next.getTasks().forEach(task -> coalesced.put(task.getAssignedTask().getTaskId(), task));
      return SaveTasks.builder().setTasks(coalesced.values()).build();
    }

    private RemoveTasks coalesce(RemoveTasks prior, RemoveTasks next) {
      return RemoveTasks.builder().setTaskIds(
          ImmutableSet.<String>builder()
              .addAll(prior.getTaskIds())
              .addAll(next.getTaskIds())
              .build())
          .build();
    }

    private Optional<SaveHostAttributes> coalesce(SaveHostAttributes prior, SaveHostAttributes next) {
      if (prior.getHostAttributes().getHost().equals(next.getHostAttributes().getHost())) {
        SaveHostAttributes._Builder builder = prior.mutate();
        builder.mutableHostAttributes().setAttributes(next.getHostAttributes().getAttributes());
        return Optional.of(builder.build());
      }
      return Optional.empty();
    }
  }
}
