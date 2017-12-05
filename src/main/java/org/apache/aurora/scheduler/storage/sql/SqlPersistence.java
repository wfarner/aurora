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

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import javax.annotation.Nullable;
import javax.inject.Inject;

import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.io.Resources;
import com.zaxxer.hikari.HikariDataSource;

import org.apache.aurora.codec.ThriftBinaryCodec;
import org.apache.aurora.gen.JobKey;
import org.apache.aurora.gen.JobUpdateKey;
import org.apache.aurora.gen.storage.Op;
import org.apache.aurora.gen.storage.SaveTasks;
import org.apache.aurora.scheduler.base.JobKeys;
import org.apache.aurora.scheduler.storage.durability.Persistence;
import org.apache.aurora.scheduler.storage.entities.IJobKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.Objects.requireNonNull;

import static org.apache.aurora.scheduler.storage.sql.SqlPersistence.RecordType.cron;
import static org.apache.aurora.scheduler.storage.sql.SqlPersistence.RecordType.host;
import static org.apache.aurora.scheduler.storage.sql.SqlPersistence.RecordType.metadata;
import static org.apache.aurora.scheduler.storage.sql.SqlPersistence.RecordType.quota;
import static org.apache.aurora.scheduler.storage.sql.SqlPersistence.RecordType.task;
import static org.apache.aurora.scheduler.storage.sql.SqlPersistence.RecordType.update;
import static org.apache.aurora.scheduler.storage.sql.SqlPersistence.RecordType.update_event;
import static org.apache.aurora.scheduler.storage.sql.SqlPersistence.RecordType.update_instance_event;

/**
 * A persistence implementation that uses a SQL database.
 */
class SqlPersistence implements Persistence {

  private static final Logger LOG = LoggerFactory.getLogger(SqlPersistence.class);

  private final HikariDataSource dataSource;
  private final Mode mode;

  @Inject
  SqlPersistence(HikariDataSource dataSource, Mode mode) {
    this.dataSource = requireNonNull(dataSource);
    this.mode = requireNonNull(mode);
  }

  public enum Mode {
    H2,
    MYSQL,
  }

  @Override
  public void prepare() {
    // no-op.
  }

  @Override
  public void close() {
    dataSource.close();
  }

  @Override
  public Stream<Op> recover() throws PersistenceException {
    try {
      return doRecover();
    } catch (SQLException e) {
      throw new PersistenceException(e);
    }
  }

  @Override
  public void persist(Stream<Op> records) throws PersistenceException {
    try {
      doPersist(records);
    } catch (SQLException e) {
      throw new PersistenceException(e);
    }
  }

  private void createTable() throws SQLException {
    try (
        Connection connection = dataSource.getConnection();
        Statement statement = connection.createStatement()) {
      String schema;
      try {
        schema = Resources.toString(
            Resources.getResource(getClass(), "schema.sql"),
            Charsets.UTF_8);
      } catch (IOException e) {
        throw new PersistenceException("Failed to create schema", e);
      }
      statement.execute(schema);
    }
  }

  private Stream<Op> stream(ResultSet resultSet) {
    return StreamSupport.stream(new Spliterators.AbstractSpliterator<Op>(
        Long.MAX_VALUE,
        Spliterator.ORDERED | Spliterator.IMMUTABLE) {

      @Override
      public boolean tryAdvance(Consumer<? super Op> action) {
        try {
          if (resultSet.next()) {
            action.accept(ThriftBinaryCodec.decodeNonNull(Op.class, resultSet.getBytes("value")));
            return true;
          }
        } catch (SQLException e) {
          throw new PersistenceException("Recovery failed", e);
        }

        return false;
      }
    }, false);
  }

  private Stream<Op> doRecover() throws SQLException {
    createTable();

    // This routine requires some gymnastics to prevent resource leaks due to exceptions, while
    // using java.util.stream APIs to convert the ResultSet to a stream (which prevent us from using
    // try-with-resources.  As a result, we use several NOPMD markers to suppress lint warnings.

    Connection connection = dataSource.getConnection(); //NOPMD
    try {
      connection.setReadOnly(true);
      // Nulls first to ensure child records are replayed last.
      PreparedStatement statement = connection.prepareStatement(
          "SELECT parent,value FROM records ORDER BY parent IS NULL DESC",
          ResultSet.TYPE_FORWARD_ONLY,
          ResultSet.CONCUR_READ_ONLY);

      try {
        if (mode == Mode.MYSQL) {
          // To handle high-volume storage, we must avoid loading the entire resultset into memory.
          // From
          // dev.mysql.com/doc/connector-j/5.1/en/connector-j-reference-implementation-notes.html
          // "a forward-only, read-only result set, with a fetch size of Integer.MIN_VALUE serves as
          // a signal to the driver to stream result sets row-by-row."
          statement.setFetchSize(Integer.MIN_VALUE);
        }

        ResultSet resultSet = statement.executeQuery(); //NOPMD
        return stream(resultSet).onClose(() -> {
          try {
            resultSet.close();
            statement.close();
            connection.close();
          } catch (SQLException e) {
            throw new PersistenceException(e);
          }
        });
      } catch (SQLException | RuntimeException e) {
        statement.close();
        throw e;
      }
    } catch (SQLException | RuntimeException e) {
      connection.close();
      throw e;
    }
  }

  private static class Key {
    final String value;

    Key(RecordType type, String value) {
      this.value = type.name() + "/" + value;
    }

    Key(RecordType type, JobKey job) {
      this(type, JobKeys.canonicalString(IJobKey.build(job)));
    }

    Key(JobUpdateKey key) {
      this(update, JobKeys.canonicalString(IJobKey.build(key.getJob())) + "/" + key.getId());
    }
  }

  enum RecordType {
    cron,
    host,
    metadata,
    quota,
    task,
    update,
    update_event,
    update_instance_event,
  }

  private interface Mutation {
    PreparedStatement prepare(Connection connection) throws SQLException;

    void applyTo(PreparedStatement statement) throws SQLException;
  }

  private static class Insert implements Mutation {
    final Key key;
    final @Nullable Key parent;
    final Op op;

    Insert(Key key, @Nullable Key parent, Op op) {
      this.key = key;
      this.parent = parent;
      this.op = op;
    }

    Insert(Key key, Op op) {
      this(key, null, op);
    }

    @Override
    public PreparedStatement prepare(Connection connection) throws SQLException {
      // An 'upsert' is deliberately used for insertion.
      return connection.prepareStatement(
          "INSERT INTO records (id, parent, value) VALUES(?, ?, ?)"
              + " ON DUPLICATE KEY UPDATE value=VALUES(value)");
    }

    @Override
    public void applyTo(PreparedStatement statement) throws SQLException {
      statement.setString(1, key.value);
      statement.setString(2, parent == null ? null : parent.value);
      statement.setBytes(3, ThriftBinaryCodec.encodeNonNull(op));
      statement.addBatch();
    }
  }

  private static class Delete implements Mutation {
    final Key key;

    Delete(Key key) {
      this.key = key;
    }

    @Override
    public PreparedStatement prepare(Connection connection) throws SQLException {
      return connection.prepareStatement("DELETE FROM records WHERE id=?");
    }

    @Override
    public void applyTo(PreparedStatement statement) throws SQLException {
      statement.setString(1, key.value);
      statement.addBatch();
    }
  }

  private static Stream<Mutation> asRecords(Op op) {
    switch (op.getSetField()) {
      case SAVE_FRAMEWORK_ID:
        return Stream.of(new Insert(new Key(metadata, "framework-id"), op));

      case SAVE_CRON_JOB:
        return Stream.of(
            new Insert(new Key(cron, op.getSaveCronJob().getJobConfig().getKey()), op));

      case REMOVE_JOB:
        return Stream.of(new Delete(new Key(cron, op.getRemoveJob().getJobKey())));

      case SAVE_TASKS:
        return op.getSaveTasks().getTasks().stream()
            .map(t -> new Insert(
                new Key(task, t.getAssignedTask().getTaskId()),
                Op.saveTasks(new SaveTasks().setTasks(ImmutableSet.of(t)))));

      case REMOVE_TASKS:
        return op.getRemoveTasks().getTaskIds().stream()
            .map(id -> new Delete(new Key(task, id)));

      case SAVE_QUOTA:
        return Stream.of(new Insert(new Key(quota, op.getSaveQuota().getRole()), op));

      case REMOVE_QUOTA:
        return Stream.of(new Delete(new Key(quota, op.getRemoveQuota().getRole())));

      case SAVE_HOST_ATTRIBUTES:
        return Stream.of(new Insert(
            new Key(host, op.getSaveHostAttributes().getHostAttributes().getSlaveId()),
            op));

      case SAVE_JOB_UPDATE:
        return Stream.of(
            new Insert(new Key(op.getSaveJobUpdate().getJobUpdate().getSummary().getKey()), op));

      case SAVE_JOB_UPDATE_EVENT:
        // Associate with the update for cascading deletes.
        return Stream.of(new Insert(
            new Key(update_event, UUID.randomUUID().toString()),
            new Key(op.getSaveJobUpdateEvent().getKey()),
            op));

      case SAVE_JOB_INSTANCE_UPDATE_EVENT:
        // Associate with the update for cascading deletes.
        return Stream.of(new Insert(
            new Key(update_instance_event, UUID.randomUUID().toString()),
            new Key(op.getSaveJobInstanceUpdateEvent().getKey()),
            op));

      case PRUNE_JOB_UPDATE_HISTORY:
        // No-op.
        return Stream.empty();

      case REMOVE_JOB_UPDATE:
        return op.getRemoveJobUpdate().getKeys().stream()
            .map(u -> new Delete(new Key(u)));

      default:
        throw new IllegalArgumentException("Unhandled operation type " + op.getSetField());
    }
  }

  private void doPersist(Stream<Op> records) throws SQLException {
    try (Connection connection = dataSource.getConnection()) {
      connection.setAutoCommit(false);

      // This looks and acts like a statement cache, but the goal is to handle batches in an attempt
      // to minimize database round-trips.
      Map<Class<?>, PreparedStatement> activeStatements = Maps.newHashMap();

      try {
        records.flatMap(SqlPersistence::asRecords)
            .forEach(mutation -> {
              PreparedStatement statement = activeStatements.computeIfAbsent(
                  mutation.getClass(),
                  unused -> {
                    try {
                      return mutation.prepare(connection);
                    } catch (SQLException e) {
                      throw new PersistenceException(e);
                    }
                  });
              try {
                mutation.applyTo(statement);
              } catch (SQLException e) {
                throw new PersistenceException(e);
              }
            });

        activeStatements.values().forEach(statement -> {
          try {
            statement.executeBatch();
          } catch (SQLException e) {
            throw new PersistenceException(e);
          }
        });

        connection.commit();
      } finally {
        activeStatements.values().forEach(statement -> {
          try {
            statement.close();
          } catch (SQLException e) {
            LOG.warn("Failed to close statement", e);
          }
        });
      }
    }
  }
}
