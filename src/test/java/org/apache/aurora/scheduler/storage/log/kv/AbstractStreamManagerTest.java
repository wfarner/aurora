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
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import com.google.common.base.Charsets;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.google.common.util.concurrent.Atomics;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.TypeLiteral;

import org.apache.aurora.common.quantity.Data;
import org.apache.aurora.gen.ResourceAggregate;
import org.apache.aurora.gen.ScheduleStatus;
import org.apache.aurora.gen.ScheduledTask;
import org.apache.aurora.gen.storage.LogEntry;
import org.apache.aurora.gen.storage.Op;
import org.apache.aurora.gen.storage.RemoveQuota;
import org.apache.aurora.gen.storage.RemoveTasks;
import org.apache.aurora.gen.storage.SaveFrameworkId;
import org.apache.aurora.gen.storage.SaveQuota;
import org.apache.aurora.gen.storage.SaveTasks;
import org.apache.aurora.gen.storage.Transaction;
import org.apache.aurora.scheduler.base.TaskTestUtil;
import org.apache.aurora.scheduler.config.types.DataAmount;
import org.apache.aurora.scheduler.log.Log.Stream.StreamAccessException;
import org.apache.aurora.scheduler.storage.entities.IScheduledTask;
import org.apache.aurora.scheduler.storage.log.StreamManager;
import org.apache.aurora.scheduler.storage.log.StreamManagerFactory;
import org.apache.aurora.scheduler.storage.log.StreamTransaction;
import org.apache.aurora.scheduler.storage.log.kv.KeyValueStore.ListableWithDelete;
import org.apache.aurora.scheduler.storage.log.kv.KeyValueStore.Record;
import org.junit.Before;
import org.junit.Test;

import static org.apache.aurora.scheduler.storage.log.kv.TransactionalKeyValueStore.LAST_TXN_KEY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public abstract class AbstractStreamManagerTest {

  private static final DataAmount SIZE_LIMIT = new DataAmount(1, Data.KB);

  protected ListableWithDelete<String, byte[]> store;
  private FailureInjectingStore faultInjection;
  private StreamManager stream;

  @Before
  public void setUp() throws Exception {
    store = createStorage();
    resetStream();
  }

  protected abstract ListableWithDelete<String, byte[]> createStorage() throws Exception;

  protected abstract Map<String, byte[]> storeContents();

  @Test
  public void testTransactionOrdering() {
    // Ops must be streamed in transaction order.  However, Ops saved in the same transaction may
    // be streamed in a different order (while still maintaining transaction order).
    stream.readFromBeginning(e -> { });

    Op one = Op.saveQuota(new SaveQuota().setRole("role1").setQuota(resource(1)));
    Op twoA = Op.saveQuota(new SaveQuota().setRole("role2a").setQuota(resource(1)));
    Op twoB = Op.saveQuota(new SaveQuota().setRole("role2b").setQuota(resource(1)));
    Op three = Op.saveQuota(new SaveQuota().setRole("role3").setQuota(resource(1)));
    Op fourA = Op.saveQuota(new SaveQuota().setRole("role4a").setQuota(resource(1)));
    Op fourB = Op.saveQuota(new SaveQuota().setRole("role4b").setQuota(resource(1)));
    Op fourC = Op.saveQuota(new SaveQuota().setRole("role4c").setQuota(resource(1)));
    Op five = Op.saveQuota(new SaveQuota().setRole("role5").setQuota(resource(1)));

    transaction(one);
    transaction(twoA, twoB);
    transaction(three);
    transaction(fourA, fourB, fourC);
    transaction(five);

    resetStream();
    List<Op> actual = Lists.newArrayList();
    stream.readFromBeginning(
        entry -> actual.add(Iterables.getOnlyElement(entry.getTransaction().getOps())));

    Consumer<Set<Op>> verifyNext = expected -> {
      Set<Op> next = ImmutableSet.copyOf(
          Iterables.limit(Iterables.consumingIterable(actual), expected.size()));
      assertEquals(expected, next);
    };

    verifyNext.accept(ImmutableSet.of(one));
    verifyNext.accept(ImmutableSet.of(twoA, twoB));
    verifyNext.accept(ImmutableSet.of(three));
    verifyNext.accept(ImmutableSet.of(fourA, fourB, fourC));
    verifyNext.accept(ImmutableSet.of(five));
    assertEquals(ImmutableList.of(), actual);
  }

  @Test
  public void testMultiKeyOp() {
    // An Op may affect many keys, which should all be treated as individual records as part of
    // the transaction.  Some key saves may involve overwriting prior records.
    stream.readFromBeginning(e -> { });

    ScheduledTask overwritten = TaskTestUtil.makeTask("overwritten", TaskTestUtil.JOB).newBuilder();
    overwritten.setStatus(ScheduleStatus.PENDING);
    transaction(Op.saveTasks(new SaveTasks().setTasks(ImmutableSet.of(overwritten))));

    SaveTasks save = new SaveTasks();
    save.addToTasks(TaskTestUtil.makeTask("task1", TaskTestUtil.JOB).newBuilder());
    save.addToTasks(TaskTestUtil.makeTask("task2", TaskTestUtil.JOB).newBuilder());
    overwritten.setStatus(ScheduleStatus.ASSIGNED);
    save.addToTasks(overwritten);
    transaction(Op.saveTasks(save));

    assertStreamContents(save.getTasks().stream()
        .map(task -> Op.saveTasks(new SaveTasks().setTasks(ImmutableSet.of(task))))
        .collect(Collectors.toSet())
    );
  }

  @Test
  public void testSizeLimit() {
    // An entry larger than the size limit should be transparently broken apart into separate
    // records.
    String largeValue = Strings.repeat("a", SIZE_LIMIT.as(Data.BYTES) + 1024);

    // Verify that the fault injection layer will deny large writes.
    try {
      faultInjection.save(
          ImmutableMap.of("key", Record.value(largeValue.getBytes(Charsets.UTF_8))));
      fail("Value should have been denied by fault injection layer");
    } catch (IllegalArgumentException e) {
      // Expected.
    }

    stream.readFromBeginning(e -> { });

    ScheduledTask task = TaskTestUtil.makeTask("task", TaskTestUtil.JOB).newBuilder();
    task.getAssignedTask().getTask().getExecutorConfig().setData(largeValue);

    // Chunking should be transparent for the caller.
    Op bigOp = Op.saveTasks(new SaveTasks().setTasks(ImmutableSet.of(task)));
    transaction(bigOp);
    assertStreamContents(bigOp);

    // Overwriting a chunked entry should remove all chunks from the replaced entry.
    resetStream();
    stream.readFromBeginning(e -> { });
    task.getAssignedTask().getTask().getExecutorConfig().setData("smaller");
    Op replacement = Op.saveTasks(new SaveTasks().setTasks(ImmutableSet.of(task)));
    transaction(replacement);
    assertStreamContents(replacement);
    assertStorageEntries(1);

    // Deleting a chunked entry should also remove all chunks from the replaced entry.
    resetStream();
    stream.readFromBeginning(e -> { });
    Op delete = Op.removeTasks(new RemoveTasks().setTaskIds(ImmutableSet.of("task")));
    transaction(delete);
    assertStreamContents();
    assertEmptyStorage();
  }

  @Test
  public void testPartialSave() {
    // A partially-written transaction should be discarded when restoring.
    stream.readFromBeginning(e -> { });
    Set<IScheduledTask> tasks = IntStream.range(0, 10)
        .boxed()
        .map(i -> TaskTestUtil.makeTask("task" + i, TaskTestUtil.JOB))
        .collect(Collectors.toSet());

    AtomicLong count = new AtomicLong(0);
    injectSaveFailure(
        s -> count.incrementAndGet() > 5,
        Op.saveTasks(new SaveTasks().setTasks(IScheduledTask.toBuildersSet(tasks))));
    assertStorageEntries(5);

    assertStreamContents();
  }

  private static ResourceAggregate resource(int multiplier) {
    return new ResourceAggregate()
        .setNumCpus(multiplier)
        .setRamMb(1024L * multiplier)
        .setDiskMb(1024L * multiplier);
  }

  @Test
  public void testLastWriteWins() {
    // When a record is overwritten, only the latest value should be retained.
    stream.readFromBeginning(e -> { });

    transaction(Op.saveQuota(new SaveQuota().setRole("role")));

    Op replacement = Op.saveQuota(new SaveQuota().setRole("role").setQuota(resource(1)));
    transaction(replacement);

    assertStorageEntries(1);
    assertStreamContents(replacement);
  }

  @Test
  public void testLastWriteWinsWithinTransaction() {
    // When a record is overwritten within a transaction, only the latest value should be retained.
    stream.readFromBeginning(e -> { });
    SaveQuota base = new SaveQuota().setRole("role");
    Op one = Op.saveQuota(base.deepCopy().setQuota(resource(1)));
    Op two = Op.saveQuota(base.deepCopy().setQuota(resource(2)));
    Op three = Op.saveQuota(base.deepCopy().setQuota(resource(3)));

    transaction(one, two);
    assertStreamContents(two);
    assertStorageEntries(1);

    transaction(two, one);
    assertStreamContents(one);
    assertStorageEntries(1);

    transaction(two, three);
    assertStreamContents(three);
    assertStorageEntries(1);

    transaction(three, two, one);
    assertStreamContents(one);
    assertStorageEntries(1);
  }

  @Test
  public void testLastWriteWinsDeleteWithinTransaction() {
    // A record may be created and deleted within a transaction having correct semantics and leaving
    // zero impact on storage.
    stream.readFromBeginning(e -> { });
    Op save = Op.saveQuota(new SaveQuota().setRole("role"));
    Op remove = Op.removeQuota(new RemoveQuota().setRole("role"));
    transaction(save, remove);
    assertEmptyStorage();
    assertStreamContents();

    // An odd scenario, but reversing the Op order should result in the entry being retained.
    stream.readFromBeginning(e -> { });
    transaction(remove, save);
    assertStreamContents(save);
    assertStorageEntries(1);
  }

  @Test
  public void testPartialOverwrite() {
    // If the original value is not successfully deleted after a successful transaction, the
    // operation should preserve the new value.
    stream.readFromBeginning(e -> { });

    transaction(Op.saveQuota(new SaveQuota().setRole("role")));

    Op replacement = Op.saveQuota(new SaveQuota().setRole("role").setQuota(resource(1)));
    injectSaveFailure(k -> k.endsWith(".0"), replacement);

    assertStreamContents(replacement);
    assertStorageEntries(1);
  }

  @Test
  public void testOverwriteTransactionFails() {
    // If a transaction fails when overwriting a value, the original value must remain.
    stream.readFromBeginning(e -> { });

    Op original = Op.saveQuota(new SaveQuota().setRole("role"));
    transaction(original);

    Op replacement = Op.saveQuota(new SaveQuota().setRole("role").setQuota(resource(1)));
    injectSaveFailure(LAST_TXN_KEY::equals, replacement);

    assertStreamContents(original);
    assertStorageEntries(1);
  }

  @Test
  public void testDeletion() {
    // When a delete record is written, neither record should be read during recovery.

    stream.readFromBeginning(e -> { });

    transaction(Op.saveQuota(new SaveQuota().setRole("role")));
    transaction(Op.removeQuota(new RemoveQuota().setRole("role")));

    assertStreamContents();
    assertEmptyStorage();
  }

  @Test
  public void testTransactionalDeleteOnTransactionFailure() {
    // All writes succeed but the last_transaction update fails (failed transaction)

    stream.readFromBeginning(e -> { });

    Op save = Op.saveQuota(new SaveQuota().setRole("role"));
    transaction(save);
    injectSaveFailure(
        Predicate.isEqual(LAST_TXN_KEY),
        Op.removeQuota(new RemoveQuota().setRole("role")));

    assertStreamContents(save);
    assertStorageEntries(1);
  }

  @Test
  public void testTransactionalDeleteOnPostDeleteFailure() {
    // A delete transaction is successful, but deleting the original record fails.

    stream.readFromBeginning(e -> { });

    transaction(Op.saveQuota(new SaveQuota().setRole("role")));
    injectSaveFailure(
        k -> k.endsWith(".0"),
        Op.removeQuota(new RemoveQuota().setRole("role")));

    // Restore fault injection during recovery.
    faultInjection.failOnKey.set(s -> false);

    assertStreamContents();
    assertEmptyStorage();
  }

  @Test
  public void testRecoverWithMultipleValuesForOneKey() {
    // If a value is overwritten but not successfully deleted, it should be cleaned up during
    // recovery.

    stream.readFromBeginning(e -> { });

    transaction(Op.saveQuota(new SaveQuota().setRole("role")));
    // There will be a (transactional) save followed by a delete for the original record.  We want
    // to fail the delete, which will be the second operation on a key for txn 0.
    AtomicInteger count = new AtomicInteger(0);
    Op replacement =
        Op.saveQuota(new SaveQuota().setRole("role").setQuota(new ResourceAggregate()));
    injectSaveFailure(
        k -> count.incrementAndGet() > 1 && k.endsWith(".0"),
        replacement);

    // Restore fault injection during recovery.
    faultInjection.failOnKey.set(s -> false);
    assertStreamContents(replacement);
    assertStorageEntries(1);
  }

  @Test
  public void testResistsHighNamespaceFanout() {
    // The store is exposed as a flat namespace, but is internally bucketed to limit fanout.

    int totalValues = 3000;
    Set<Op> manyQuotas = IntStream.range(0, totalValues)
        .boxed()
        .map(i -> Op.saveQuota(new SaveQuota().setRole("role" + i)))
        .collect(Collectors.toSet());

    stream.readFromBeginning(e -> { });
    transaction(manyQuotas.stream());

    assertStorageEntries(totalValues);
    Multimap<String, String> tree = HashMultimap.create();
    storeContents().keySet().forEach(key -> {
      List<String> parts = ImmutableList.copyOf(Splitter.on('/').split(key));
      for (int i = 1; i < parts.size(); i++) {
        tree.put(Joiner.on('/').join(parts.subList(0, i)), parts.get(i));
      }
    });

    tree.asMap().forEach((namespace, contents) -> {
      assertTrue(
          String.format("High fanout in namespace %s (%s entries): %s",
              namespace, contents.size(), contents),
          contents.size() < (totalValues / 10));
    });
  }

  @Test(expected = IllegalStateException.class)
  public void testMustReadFirst() {
    transaction(Op.saveFrameworkId(new SaveFrameworkId()));
  }

  private void transaction(Op... ops) {
    transaction(Stream.of(ops));
  }

  private void transaction(Stream<Op> ops) {
    StreamTransaction transaction = stream.startTransaction();
    ops.forEach(transaction::add);
    transaction.commit();
  }

  private void injectSaveFailure(Predicate<String> failToSave, Op... ops) {
    faultInjection.failOnKey.set(failToSave);
    try {
      transaction(ops);
      fail("Expected StreamAccessException");
    } catch (StreamAccessException e) {
      // Expected.
    }
  }

  private void resetStream() {
    faultInjection = new FailureInjectingStore(store);

    KeyValueStreamModule.Options options = new KeyValueStreamModule.Options();
    options.maxEntrySize = SIZE_LIMIT;

    Injector injector = Guice.createInjector(
        new KeyValueStreamModule(options),
        new AbstractModule() {
          @Override
          protected void configure() {
            bind(new TypeLiteral<ListableWithDelete<String, byte[]>>() { })
                .toInstance(faultInjection);
          }
        });
    stream = injector.getInstance(StreamManagerFactory.class).create(null);
  }

  private void assertStreamContents(Op... expected) {
    assertStreamContents(ImmutableSet.copyOf(expected));
  }

  private void assertStreamContents(Set<Op> expected) {
    resetStream();

    Set<LogEntry> expectedEntries = expected.stream()
        .map(op -> LogEntry.transaction(new Transaction().setOps(ImmutableList.of(op))))
        .collect(Collectors.toSet());

    // Store in a list at first.  Specific ordering of entries is not guaranteed, but we do this
    // to ensure no duplicate entries are read (which would be hidden by storing directly into a
    // set.
    List<LogEntry> actual = Lists.newArrayList();
    stream.readFromBeginning(actual::add);
    assertEquals(expectedEntries, ImmutableSet.copyOf(actual));
    assertEquals(expected.size(), actual.size());
  }

  private void assertEmptyStorage() {
    assertEquals(ImmutableSet.of(LAST_TXN_KEY), storeContents().keySet());
  }

  private void assertStorageEntries(int count) {
    assertEquals(
        count,
        storeContents().keySet().stream()
            .filter(k -> !LAST_TXN_KEY.equals(k))
            .count());
  }

  private static class FailureInjectingStore implements ListableWithDelete<String, byte[]> {
    // Allow slightly larger recoreds than configured, as the framing code does not currently
    // account for the overhead of the frame record itself, and only limits the data array
    // _within_ the record.
    private static final int MAX_ALLOWED_SIZE = SIZE_LIMIT.as(Data.BYTES) + 100;

    private final ListableWithDelete<String, byte[]> wrapped;

    private final AtomicReference<Predicate<String>> failOnKey = Atomics.newReference(s -> false);

    FailureInjectingStore(ListableWithDelete<String, byte[]> wrapped) {
      this.wrapped = wrapped;
    }

    @Override
    public Set<String> keys() throws StoreException {
      return wrapped.keys();
    }

    @Override
    public Optional<byte[]> get(String key) throws StoreException {
      return wrapped.get(String.valueOf(key));
    }

    @Override
    public void save(Map<String, Record<byte[]>> records) throws StoreException {
      Preconditions.checkArgument(records.values().stream()
          .allMatch(v -> v.isTombstone() || v.getValue().length <= MAX_ALLOWED_SIZE));

      // For any save, last_transaction should always be last in the iteration order.
      assertEquals(LAST_TXN_KEY, Iterables.getLast(records.keySet()));

      // Collect values and retain iteration order.
      ImmutableMap.Builder<String, Record<byte[]>> save = ImmutableMap.builder();

      Predicate<String> fail = failOnKey.get();
      boolean failed = false;
      for (Map.Entry<String, Record<byte[]>> entry : records.entrySet()) {
        if (fail.test(entry.getKey())) {
          failed = true;
          break;
        }
        save.put(entry.getKey(), entry.getValue());
      }

      wrapped.save(save.build());
      if (failed) {
        throw new StoreException("Something broke");
      }
    }

    @Override
    public void delete(Set<String> keys) throws StoreException {
      ImmutableSet.Builder<String> successfulDeletes = ImmutableSet.builder();

      Predicate<String> fail = failOnKey.get();
      boolean failed = false;
      for (String key : keys) {
        if (fail.test(key)) {
          failed = true;
          break;
        } else {
          successfulDeletes.add(key);
        }
      }

      Set<String> deletes = successfulDeletes.build();
      if (!deletes.isEmpty()) {
        wrapped.delete(deletes);
      }
      if (failed) {
        throw new StoreException("Something broke");
      }
    }
  }
}
