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
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;

import org.apache.aurora.gen.JobKey;
import org.apache.aurora.gen.storage.LogEntry;
import org.apache.aurora.gen.storage.Op;
import org.apache.aurora.gen.storage.RemoveTasks;
import org.apache.aurora.gen.storage.SaveTasks;
import org.apache.aurora.gen.storage.Transaction;
import org.apache.aurora.scheduler.storage.log.kv.KeyValueStore.Record;
import org.apache.aurora.scheduler.storage.log.kv.KeyValueStore.StoreException;

import static java.util.Objects.requireNonNull;

class LogOpStore implements AppendOnlyStore<Op, Op> {

  private final AppendOnlyStore<Map.Entry<String, Record<LogEntry>>, LogEntry> wrapped;

  enum Namespace {
    ATTRIBUTE,
    TASK,
    CRON,
    METADATA,
    QUOTA,
    UPDATE,
  }

  LogOpStore(AppendOnlyStore<Map.Entry<String, Record<LogEntry>>, LogEntry> wrapped) {
    this.wrapped = requireNonNull(wrapped);
  }

  @Override
  public Stream<Op> stream() throws StoreException {
    return wrapped.stream()
        .map(record -> Iterables.getOnlyElement(record.getTransaction().getOps()));
  }

  @Override
  public void save(Stream<Op> values) throws StoreException {
    wrapped.save(index(values).entrySet().stream()
        .map(entry -> new AbstractMap.SimpleImmutableEntry<>(
            "/" + entry.getKey(),
            entry.getValue())));
  }

  private static Map<String, Record<LogEntry>> index(Stream<Op> ops) {
    Map<String, Record<LogEntry>> singletonOps = Maps.newHashMap();
    ops.forEach(op -> {
      // Note - this will silently overwrite same-key ops within the scope of the transaction.
      // For example, if a task is created and deleted in a single transaction, it is expected to
      // appear as a single 'delete' entry (with no corresponding 'save').  Since ops must be
      // idempotent, this is fine.
      singletonOps.putAll(split(op));
    });

    return singletonOps;
  }

  private static String key(Namespace namespace, String item) {
    return namespace.name() + "/" + item;
  }

  private static String key(Namespace namespace) {
    return key(namespace, "singleton");
  }

  private static String key(Namespace namespace, JobKey job) {
    return key(namespace, Joiner.on(".").join(job.getRole(), job.getEnvironment(), job.getName()));
  }

  private static final Set<Op._Fields> TOMBSTONE_OPS = ImmutableSet.of(
      Op._Fields.REMOVE_JOB,
      Op._Fields.REMOVE_JOB_UPDATE,
      Op._Fields.REMOVE_TASKS,
      Op._Fields.REMOVE_QUOTA);

  private static boolean isTombstone(Op op) {
    return TOMBSTONE_OPS.contains(op.getSetField());
  }

  /**
   * Splits an op into equivalent individual ops that each modify exactly one entity.
   *
   * @param op Op to split.
   * @return Multiple single-entity ops equivalent to {@code op}.
   */
  private static Map<String, Record<LogEntry>> split(Op op) {
    Op._Fields type = op.getSetField();
    switch (type) {
      case SAVE_FRAMEWORK_ID:
        return ImmutableMap.of(key(Namespace.METADATA), value(op));

      case SAVE_CRON_JOB:
        return ImmutableMap.of(
            key(Namespace.CRON, op.getSaveCronJob().getJobConfig().getKey()),
            value(op));

      case REMOVE_JOB:
        return ImmutableMap.of(key(Namespace.CRON, op.getRemoveJob().getJobKey()), tombstone(op));

      case SAVE_TASKS:
        return op.getSaveTasks().getTasks()
            .stream()
            .collect(Collectors.toMap(
                task -> key(Namespace.TASK, task.getAssignedTask().getTaskId()),
                task -> value(Op.saveTasks(new SaveTasks().setTasks(ImmutableSet.of(task))))
            ));

      case REMOVE_TASKS:
        return op.getRemoveTasks().getTaskIds()
            .stream()
            .collect(Collectors.toMap(
                taskId -> key(Namespace.TASK, taskId),
                taskId -> tombstone(Op.removeTasks(
                    new RemoveTasks().setTaskIds(ImmutableSet.of(taskId))))
            ));

      case SAVE_QUOTA:
        return ImmutableMap.of(key(Namespace.QUOTA, op.getSaveQuota().getRole()), value(op));

      case REMOVE_QUOTA:
        return ImmutableMap.of(key(Namespace.QUOTA, op.getRemoveQuota().getRole()), tombstone(op));

      case SAVE_HOST_ATTRIBUTES:
        return ImmutableMap.of(
            key(Namespace.ATTRIBUTE, op.getSaveHostAttributes().getHostAttributes().getSlaveId()),
            value(op));

      case SAVE_JOB_UPDATE:
        return ImmutableMap.of(
            key(Namespace.UPDATE, op.getSaveJobUpdate().getJobUpdate().getSummary().getKey().getId()),
            value(op));

      case SAVE_JOB_UPDATE_EVENT:
        // TODO(wfarner): How will update events be deleted?  We have a record type for deleting
        // a JobUpdate, but no way to handle one-to-many relationships (one update, many events).
        // May need to allow multi-key tombstones; however this layer does not have the context
        // to look up events given a JobUpdateKey.
        // Alternatively, may want to introduce DeleteJobUpdateEvents, a multi record for deleting
        // all events associated with an update.  This would be broken into singletons here, similar
        //
        return ImmutableMap.of(
            key(Namespace.UPDATE, op.getSaveJobUpdateEvent().getKey().getId()),
            value(op));

      case SAVE_JOB_INSTANCE_UPDATE_EVENT:
        throw new UnsupportedOperationException("Non-idempotent operation");

      case PRUNE_JOB_UPDATE_HISTORY:
        // TODO(wfarner): Need to remove this and replace with DeleteJobUpdate Op.
        throw new UnsupportedOperationException("Non-idempotent operation");

      default:
        throw new IllegalArgumentException("Unhandled op type " + type);
    }
  }

  private static LogEntry toLogEntry(Op op) {
    return LogEntry.transaction(new Transaction().setOps(ImmutableList.of(op)));
  }

  private static Record<LogEntry> value(Op op) {
    return Record.value(toLogEntry(op));
  }

  private static Record<LogEntry> tombstone(Op op) {
    return Record.tombstone(toLogEntry(op));
  }
}
