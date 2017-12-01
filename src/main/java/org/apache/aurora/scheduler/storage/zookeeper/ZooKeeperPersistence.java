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
package org.apache.aurora.scheduler.storage.zookeeper;

import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.inject.Inject;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;

import org.apache.aurora.codec.ThriftBinaryCodec;
import org.apache.aurora.gen.JobKey;
import org.apache.aurora.gen.storage.Op;
import org.apache.aurora.gen.storage.SaveJobInstanceUpdateEvent;
import org.apache.aurora.gen.storage.SaveJobUpdateEvent;
import org.apache.aurora.gen.storage.SaveTasks;
import org.apache.aurora.scheduler.storage.durability.Persistence;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.transaction.CuratorTransactionFinal;

import static java.util.Objects.requireNonNull;

/**
 * A persistence layer that uses ZooKeeper.
 */
class ZooKeeperPersistence implements Persistence {

  private final CuratorFramework zookeeper;

  @Inject
  ZooKeeperPersistence(CuratorFramework zookeeper) {
    // TODO(wfarner): Ensure CuratorFramework.usingNamespace is used.
    this.zookeeper = requireNonNull(zookeeper);
  }

  @Override
  public void prepare() {
    // No-op.
  }

  @Override
  public Stream<Op> recover() throws PersistenceException {
    return null;
  }

  @Override
  public void persist(Stream<Op> records) throws PersistenceException {
    Map<String, Optional<Op>> combined = Maps.newHashMap();
    records.forEach(op -> combined.putAll(split(op)));

    if (combined.isEmpty()) {
      return;
    }

    // The need to cast is unfortunate, but this version of curator has an API that makes for
    // batch-style collection awkward otherwise.
    CuratorTransactionFinal transaction = (CuratorTransactionFinal) zookeeper.inTransaction();
    combined.forEach((path, value) -> {
      try {
        if (value.isPresent()) {
          transaction.create().forPath(path, ThriftBinaryCodec.encodeNonNull(value.get()));
        } else {
          transaction.delete().forPath(path);
        }
      } catch (Exception e) {
        throw new PersistenceException("Failed to save " + path, e);
      }
    });
    try {
      transaction.commit();
    } catch (Exception e) {
      throw new PersistenceException("Failed to persist records " + combined.keySet(), e);
    }
  }

  enum Namespace {
    ATTRIBUTE,
    TASK,
    CRON,
    METADATA,
    QUOTA,
    UPDATE,
  }

  private static String key(Namespace namespace, String item, String... items) {
    StringBuilder builder = new StringBuilder()
        .append(namespace.name())
        .append('/')
        .append(item);
    for (String part : items) {
      builder.append(part);
    }
    return builder.toString();
  }

  private static String key(Namespace namespace, JobKey job) {
    return key(namespace, Joiner.on(".").join(job.getRole(), job.getEnvironment(), job.getName()));
  }

  private static Map<String, Optional<Op>> split(Op op) {
    Op._Fields type = op.getSetField();
    switch (type) {
      case SAVE_FRAMEWORK_ID:
        return ImmutableMap.of(key(Namespace.METADATA, "framework_id"), Optional.of(op));

      case SAVE_CRON_JOB:
        return ImmutableMap.of(
            key(Namespace.CRON, op.getSaveCronJob().getJobConfig().getKey()),
            Optional.of(op));

      case REMOVE_JOB:
        return ImmutableMap.of(
            key(Namespace.CRON, op.getRemoveJob().getJobKey()),
            Optional.empty());

      case SAVE_TASKS:
        return op.getSaveTasks().getTasks()
            .stream()
            .collect(Collectors.toMap(
                task -> key(Namespace.TASK, task.getAssignedTask().getTaskId()),
                task -> Optional.of(Op.saveTasks(new SaveTasks().setTasks(ImmutableSet.of(task))))
            ));

      case REMOVE_TASKS:
        return op.getRemoveTasks().getTaskIds()
            .stream()
            .collect(Collectors.toMap(
                taskId -> key(Namespace.TASK, taskId),
                taskId -> Optional.empty()
            ));

      case SAVE_QUOTA:
        return ImmutableMap.of(key(Namespace.QUOTA, op.getSaveQuota().getRole()), Optional.of(op));

      case REMOVE_QUOTA:
        return ImmutableMap.of(
            key(Namespace.QUOTA, op.getRemoveQuota().getRole()),
            Optional.empty());

      case SAVE_HOST_ATTRIBUTES:
        return ImmutableMap.of(
            key(Namespace.ATTRIBUTE, op.getSaveHostAttributes().getHostAttributes().getSlaveId()),
            Optional.of(op));

      case SAVE_JOB_UPDATE:
        return ImmutableMap.of(
            key(
                Namespace.UPDATE,
                op.getSaveJobUpdate().getJobUpdate().getSummary().getKey().getId()),
            Optional.of(op));

      case SAVE_JOB_UPDATE_EVENT:
        // Update events are deleted automatically by association - an update's data is stored
        // in the node itself, and events are children of the update node.
        SaveJobUpdateEvent jobEvent = op.getSaveJobUpdateEvent();
        return ImmutableMap.of(
            key(
                Namespace.UPDATE,
                "event",
                jobEvent.getKey().getId(),
                jobEvent.getEvent().getTimestampMs() + "-" + jobEvent.getEvent().getStatus()),
            Optional.of(op));

      case SAVE_JOB_INSTANCE_UPDATE_EVENT:
        SaveJobInstanceUpdateEvent event = op.getSaveJobInstanceUpdateEvent();
        return ImmutableMap.of(
            key(Namespace.UPDATE,
                event.getKey().getId(),
                "instance-event",
                event.getEvent().getTimestampMs() + "-" + event.getEvent().getInstanceId()),
            Optional.of(op));

      case PRUNE_JOB_UPDATE_HISTORY:
        // TODO(wfarner): Need to remove this and replace with DeleteJobUpdate Op.
        throw new UnsupportedOperationException("Non-idempotent operation");

      default:
        throw new IllegalArgumentException("Unhandled op type " + type);
    }
  }
}
