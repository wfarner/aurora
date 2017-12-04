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
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import javax.sql.DataSource;

import com.google.common.collect.ImmutableSet;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import org.apache.aurora.gen.AssignedTask;
import org.apache.aurora.gen.ExecutorConfig;
import org.apache.aurora.gen.ScheduledTask;
import org.apache.aurora.gen.TaskConfig;
import org.apache.aurora.gen.storage.Op;
import org.apache.aurora.gen.storage.SaveQuota;
import org.apache.aurora.gen.storage.SaveTasks;
import org.apache.aurora.scheduler.storage.durability.Persistence;
import org.apache.aurora.scheduler.storage.sql.SqlPersistence.Mode;
import org.apache.commons.lang3.RandomStringUtils;

public class Trial {

  private static void loadSimple(Persistence persistence, int count) {
    List<Stream<Op>> batches = IntStream.range(0, count)
        .mapToObj(i -> Stream.of(Op.saveQuota(new SaveQuota()
            .setRole("user" + i))))
        .collect(Collectors.toList());

    load(persistence, batches);
  }

  private static void loadLarge(Persistence persistence, int count) {
    List<Stream<Op>> batches = IntStream.range(0, count)
        .mapToObj(i -> {
          Op op = Op.saveTasks(new SaveTasks()
              .setTasks(ImmutableSet.of(
                  new ScheduledTask()
                      .setAssignedTask(new AssignedTask()
                          .setTaskId("task" + i)
                          .setTask(new TaskConfig()
                          .setExecutorConfig(new ExecutorConfig()
                          .setName("fake")
                          .setData(RandomStringUtils.randomAscii(100*1024)))))
              )));
          return Stream.of(op);
        })
        .collect(Collectors.toList());

    load(persistence, batches);
  }

  private static void loadBatched(Persistence persistence, int count) {
    List<Stream<Op>> batches = IntStream.range(0, count)
        .mapToObj(i -> IntStream.range(0, 100)
            .mapToObj(j -> {
              ScheduledTask task = new ScheduledTask()
                  .setAssignedTask(new AssignedTask()
                      .setTaskId("task" + i + "-" + j)
                      .setTask(new TaskConfig()
                          .setExecutorConfig(new ExecutorConfig()
                              .setName("fake"))));
              return Op.saveTasks(new SaveTasks().setTasks(ImmutableSet.of(task)));
            }))
        .collect(Collectors.toList());

    load(persistence, batches);
  }

  private static void load(Persistence persistence, List<Stream<Op>> persists) {
    long[] latencies = new long[persists.size()];
    AtomicInteger i = new AtomicInteger(0);
    long start = System.nanoTime();
    persists.forEach(batch -> {
      long saveStart = System.nanoTime();
      persistence.persist(batch);
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

      statement.execute("DROP TABLE records");
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

    System.out.println("-- Simple records (minimal SaveQuota records)");
    reset(dataSource, persistence);
    loadSimple(persistence, 1_000_000);
    recover(persistence);

    System.out.println();
    System.out.println("-- Large records (ScheduledTasks with 100 KB blobs)");
    reset(dataSource, persistence);
    loadLarge(persistence, 10_000);
    recover(persistence);

    System.out.println();
    System.out.println("-- Record batches (each persist is 100 simple records)");
    reset(dataSource, persistence);
    loadBatched(persistence, 10_000);
    recover(persistence);
  }
}
