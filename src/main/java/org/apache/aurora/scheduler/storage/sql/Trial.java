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
package org.apache.aurora.scheduler.storage.sql;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import javax.sql.DataSource;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import org.apache.aurora.gen.AssignedTask;
import org.apache.aurora.gen.ExecutorConfig;
import org.apache.aurora.gen.InstanceTaskConfig;
import org.apache.aurora.gen.JobKey;
import org.apache.aurora.gen.JobUpdate;
import org.apache.aurora.gen.JobUpdateEvent;
import org.apache.aurora.gen.JobUpdateInstructions;
import org.apache.aurora.gen.JobUpdateKey;
import org.apache.aurora.gen.JobUpdateSummary;
import org.apache.aurora.gen.ScheduledTask;
import org.apache.aurora.gen.TaskConfig;
import org.apache.aurora.gen.storage.Op;
import org.apache.aurora.gen.storage.SaveJobUpdate;
import org.apache.aurora.gen.storage.SaveJobUpdateEvent;
import org.apache.aurora.gen.storage.SaveQuota;
import org.apache.aurora.gen.storage.SaveTasks;
import org.apache.aurora.scheduler.storage.durability.Persistence;
import org.apache.aurora.scheduler.storage.sql.SqlPersistence.Mode;
import org.apache.commons.lang3.RandomStringUtils;

public class Trial {

  private static List<List<Op>> makeTiny(String prefix, int count) {
    return IntStream.range(0, count)
        .mapToObj(i -> ImmutableList.of(Op.saveQuota(new SaveQuota()
            .setRole(prefix + i))))
        .collect(Collectors.toList());
  }

  private static TaskConfig makeConfig(int blobSizeKb) {
    return new TaskConfig()
        .setExecutorConfig(new ExecutorConfig()
            .setName("fake")
            .setData(RandomStringUtils.randomAscii(blobSizeKb * 1024)));
  }

  private static ScheduledTask makeTask(String id, TaskConfig config) {
    return new ScheduledTask()
        .setAssignedTask(new AssignedTask()
            .setTaskId(id)
            .setTask(config));
  }

  private static List<List<Op>> makeTaskSaves(String prefix, int count, TaskConfig config) {
    return IntStream.range(0, count)
        .mapToObj(i -> {
          Op op = Op.saveTasks(new SaveTasks()
              .setTasks(ImmutableSet.of(makeTask(prefix + i, config))));
          return ImmutableList.of(op);
        })
        .collect(Collectors.toList());
  }

  private static void loadSimple(Persistence persistence, int count) {
    load(persistence, makeTiny("user", count));
  }

  private static void loadSized(Persistence persistence, int count, TaskConfig config) {
    load(persistence, makeTaskSaves("task", count, config));
  }

  private static void loadUpdates(Persistence persistence, int count) {
    TaskConfig config = makeConfig(10);
    List<List<Op>> batches = IntStream.range(0, count)
        .mapToObj(i -> {
          Op op = Op.saveTasks(new SaveTasks()
              .setTasks(ImmutableSet.of(makeTask("task", config))));
          return ImmutableList.of(op);
        })
        .collect(Collectors.toList());

    load(persistence, batches);
  }

  private static List<List<Op>> makeBatched(String prefix, int count) {
    TaskConfig config = makeConfig(1);
    return IntStream.range(0, count)
        .mapToObj(i -> IntStream.range(0, 100)
            .mapToObj(j -> Op.saveTasks(
                new SaveTasks().setTasks(ImmutableSet.of(makeTask(prefix + i + "-" + j, config)))))
            .collect(Collectors.toList()))
        .collect(Collectors.toList());
  }

  private static void loadBatched(Persistence persistence, int count) {
    load(persistence, makeBatched("task", count));
  }

  private static List<List<Op>> makeParents(String prefix, int count) {
    TaskConfig config = makeConfig(1);
    return IntStream.range(0, count)
        .mapToObj(i -> ImmutableList.of(Op.saveJobUpdate(new SaveJobUpdate()
            .setJobUpdate(new JobUpdate()
                .setInstructions(new JobUpdateInstructions()
                .setDesiredState(new InstanceTaskConfig()
                .setTask(config)))
                .setSummary(new JobUpdateSummary()
                    .setKey(new JobUpdateKey()
                        .setJob(new JobKey("role", "env", "job"))
                        .setId(prefix + i)))))))
        .collect(Collectors.toList());
  }

  private static void loadMixed(Persistence persistence, int count) {
    int numTinyRecords = count * 69 / 100;
    int numHugeRecords = count / 100;
    int numBigRecords = count * 9 / 100;
    int numBatchedRecords = count / 100;
    int numParentRecords = count * 5 / 100;
    int numChildRecords = count * 15 / 100;

    List<List<Op>> batches = Lists.newArrayListWithExpectedSize(count);
    batches.addAll(makeTiny("tiny", numTinyRecords));
    batches.addAll(makeTaskSaves("huge", numHugeRecords, makeConfig(200)));
    batches.addAll(makeTaskSaves("big", numBigRecords, makeConfig(50)));
    batches.addAll(makeBatched("batched", numBatchedRecords));

    List<List<Op>> parents = makeParents("parents", numParentRecords);
    batches.addAll(parents);

    Collections.shuffle(batches);

    Iterator<List<Op>> parentIterator = Iterators.cycle(parents);
    List<List<Op>> children = IntStream.range(0, numChildRecords)
        .mapToObj(i -> ImmutableList.of(Op.saveJobUpdateEvent(
            new SaveJobUpdateEvent()
                .setKey(parentIterator.next().get(0).getSaveJobUpdate()
                    .getJobUpdate().getSummary().getKey())
                .setEvent(new JobUpdateEvent()
                    .setMessage("something happened")
                    .setTimestampMs(System.currentTimeMillis())))))
        .collect(Collectors.toList());

    Collections.shuffle(children);
    batches.addAll(children);

    load(persistence, batches);
  }

  private static void load(Persistence persistence, List<List<Op>> persists) {
    System.out.println("Loading " + persists.size() + " persists");
    long[] latencies = new long[persists.size()];
    AtomicInteger i = new AtomicInteger(0);
    long start = System.nanoTime();
    persists.forEach(batch -> {
      long saveStart = System.nanoTime();
      persistence.persist(batch.stream());
      long saveEnd = System.nanoTime();
      latencies[i.getAndIncrement()] = saveEnd - saveStart;
    });
    long end = System.nanoTime();
    long durationMs = (end - start) / 1_000_000;
    System.out.print("Persisted " + persists.size() + " times in " + durationMs + " ms");
    System.out.println(" (" + ((persists.size() * 1000) / durationMs) + " persists/sec)");
    Arrays.sort(latencies);
    System.out.println("  avg " + formatNanosAsMillis((end - start) / persists.size()));
    System.out.println("  p50 " + formatNanosAsMillis(latencies[latencies.length / 2]));
    System.out.println("  p90 " + formatNanosAsMillis(latencies[(latencies.length * 90) / 100]));
    System.out.println("  p99 " + formatNanosAsMillis(latencies[(latencies.length * 99) / 100]));
  }

  private static String formatNanosAsMillis(long nanos) {
    return String.format("%.2f ms", ((float) nanos) / 1_000_000);
  }

  private static void recover(Persistence persistence) {
    AtomicLong recordCount = new AtomicLong();
    long start = System.nanoTime();
    try (Stream<Op> ops = persistence.recover()) {
      ops.forEach(op -> {
        recordCount.incrementAndGet();
      });
    }
    long end = System.nanoTime();
    long durationMs = (end - start) / 1_000_000;
    System.out.print("Recovered " + recordCount.get() + " records in " + durationMs + " ms");
    System.out.println(" ("
        + String.format("%,d", ((recordCount.get() * 1000) / durationMs)) + " records/sec)");
  }

  private static void dropTable(DataSource dataSource) throws SQLException {
    try (
        Connection connection = dataSource.getConnection();
        Statement statement = connection.createStatement()) {

      statement.execute("DROP TABLE IF EXISTS records");
    }
  }

  private static void reset(DataSource dataSource, Persistence persistence) throws SQLException {
    dropTable(dataSource);
    // No-op, only creating the schema.
    persistence.recover().close();
  }

  public static void main(String[] args) throws SQLException {
    HikariConfig config = new HikariConfig();
    // http://assets.en.oreilly.com/1/event/21/Connector_J%20Performance%20Gems%20Presentation.pdf
    config.setJdbcUrl("jdbc:mysql://192.168.33.7/aurora?useConfigs=maxPerformance&rewriteBatchedStatements=true");

    config.setDriverClassName("com.mysql.jdbc.Driver");
    config.setUsername("aurora");

    HikariDataSource dataSource = new HikariDataSource(config);

    Persistence persistence = new SqlPersistence(dataSource, Mode.MYSQL);

    System.out.println("-- Mixed records");
    reset(dataSource, persistence);
    loadMixed(persistence, 200_000);
    recover(persistence);

    System.out.println();
    System.out.println("-- Simple records (minimal SaveQuota records)");
    reset(dataSource, persistence);
    loadSimple(persistence, 50_000);
    recover(persistence);

    System.out.println();
    System.out.println("-- Repeated updates to the same record (ScheduledTask with 10 KB blob)");
    reset(dataSource, persistence);
    loadUpdates(persistence, 10_000);
    recover(persistence);

    System.out.println();
    System.out.println("-- Medium records (ScheduledTasks with 10 KB blobs)");
    reset(dataSource, persistence);
    loadSized(persistence, 10_000, makeConfig(10));
    recover(persistence);

    System.out.println();
    System.out.println("-- Large records (ScheduledTasks with 100 KB blobs)");
    reset(dataSource, persistence);
    loadSized(persistence, 10_000, makeConfig(100));
    recover(persistence);

    System.out.println();
    System.out.println("-- Record batches (each persist is 100 simple records)");
    reset(dataSource, persistence);
    loadBatched(persistence, 5_000);
    recover(persistence);
  }

  /*
Latest results, with innodb_buffer_pool_size=2G on the server

-- Mixed records
Loading 200000 persists
Persisted 200000 times in 638946 ms (313 persists/sec)
  avg 3.19 ms
  p50 1.51 ms
  p90 7.10 ms
  p99 28.33 ms
Recovered 398000 records in 18881 ms (21,079 records/sec)
-- Simple records (minimal SaveQuota records)
Loading 50000 persists
Persisted 50000 times in 88938 ms (562 persists/sec)
  avg 1.78 ms
  p50 1.26 ms
  p90 2.23 ms
  p99 11.13 ms
Recovered 50000 records in 527 ms (94,876 records/sec)

-- Repeated updates to the same record (ScheduledTask with 10 KB blob)
Loading 10000 persists
Persisted 10000 times in 52583 ms (190 persists/sec)
  avg 5.26 ms
  p50 2.76 ms
  p90 12.71 ms
  p99 27.00 ms
Recovered 1 records in 2 ms (500 records/sec)

-- Medium records (ScheduledTasks with 10 KB blobs)
Loading 10000 persists
Persisted 10000 times in 65408 ms (152 persists/sec)
  avg 6.54 ms
  p50 3.16 ms
  p90 14.98 ms
  p99 36.42 ms
Recovered 10000 records in 618 ms (16,181 records/sec)

-- Large records (ScheduledTasks with 100 KB blobs)
Loading 10000 persists
Persisted 10000 times in 102436 ms (97 persists/sec)
  avg 10.24 ms
  p50 6.19 ms
  p90 22.41 ms
  p99 37.97 ms
Recovered 10000 records in 6175 ms (1,619 records/sec)

-- Record batches (each persist is 100 simple records)
Loading 5000 persists
Persisted 5000 times in 70084 ms (71 persists/sec)
  avg 14.02 ms
  p50 9.60 ms
  p90 27.90 ms
  p99 42.84 ms
Recovered 500000 records in 14097 ms (35,468 records/sec)
   */
}
