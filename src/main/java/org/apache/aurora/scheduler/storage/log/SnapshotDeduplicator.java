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

import java.util.List;
import java.util.Map.Entry;

import com.google.common.base.Function;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Multimaps;

import org.apache.aurora.codec.ThriftBinaryCodec.CodingException;
import org.apache.aurora.common.inject.TimedInterceptor.Timed;
import org.apache.aurora.gen.ScheduledTask;
import org.apache.aurora.gen.TaskConfig;
import org.apache.aurora.gen.storage.DeduplicatedScheduledTask;
import org.apache.aurora.gen.storage.DeduplicatedSnapshot;
import org.apache.aurora.gen.storage.Snapshot;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Converter between denormalized storage Snapshots and de-duplicated snapshots.
 *
 * <p>
 * For information on the difference in the two formats see the documentation in storage.thrift.
 */
public interface SnapshotDeduplicator {
  /**
   * Convert a Snapshot to the deduplicated format.
   *
   * @param snapshot Snapshot to convert.
   * @return deduplicated snapshot.
   */
  DeduplicatedSnapshot deduplicate(Snapshot snapshot);

  /**
   * Restore a deduplicated snapshot to its original denormalized form.
   *
   * @param snapshot Deduplicated snapshot to restore.
   * @return A full snapshot.
   * @throws CodingException when the input data is corrupt.
   */
  Snapshot reduplicate(DeduplicatedSnapshot snapshot) throws CodingException;

  class SnapshotDeduplicatorImpl implements SnapshotDeduplicator {
    private static final Logger LOG = LoggerFactory.getLogger(SnapshotDeduplicatorImpl.class);

    private static final Function<ScheduledTask, TaskConfig> SCHEDULED_TO_CONFIG =
        task -> task.getAssignedTask().getTask();

    private static ScheduledTask deepCopyWithoutTaskConfig(ScheduledTask scheduledTask) {
      ScheduledTask._Builder scheduledTaskCopy = scheduledTask.mutate();
      scheduledTaskCopy.mutableAssignedTask().clearTask();
      return scheduledTaskCopy.build();
    }

    private static Snapshot deepCopyWithoutTasks(Snapshot snapshot) {
      return snapshot.mutate()
          .clearTasks()
          .build();
    }

    @Override
    @Timed("snapshot_deduplicate")
    public DeduplicatedSnapshot deduplicate(Snapshot snapshot) {
      int numInputTasks = snapshot.getTasks().size();
      LOG.info("Starting deduplication of a snapshot with {} tasks.", numInputTasks);

      DeduplicatedSnapshot._Builder deduplicatedSnapshot = DeduplicatedSnapshot.builder()
          .setPartialSnapshot(deepCopyWithoutTasks(snapshot));

      // Nothing to do if we don't have any input tasks.
      if (!snapshot.hasTasks()) {
        LOG.warn("Got snapshot with unset tasks field.");
        return deduplicatedSnapshot.build();
      }

      // Match each unique TaskConfig to its hopefully-multiple ScheduledTask owners.
      ListMultimap<TaskConfig, ScheduledTask> index = Multimaps.index(
          snapshot.getTasks(),
          SCHEDULED_TO_CONFIG);

      for (Entry<TaskConfig, List<ScheduledTask>> entry : Multimaps.asMap(index).entrySet()) {
        deduplicatedSnapshot.addToTaskConfigs(entry.getKey());
        for (ScheduledTask scheduledTask : entry.getValue()) {
          deduplicatedSnapshot.addToPartialTasks(DeduplicatedScheduledTask.builder()
              .setPartialScheduledTask(deepCopyWithoutTaskConfig(scheduledTask))
              .setTaskConfigId(deduplicatedSnapshot.mutableTaskConfigs().size() - 1)
              .build());
        }
      }

      int numOutputTasks = deduplicatedSnapshot.mutableTaskConfigs().size();

      LOG.info(String.format(
          "Finished deduplicating snapshot. Deduplication ratio: %d/%d = %.2f%%.",
          numInputTasks,
          numOutputTasks,
          100.0 * numInputTasks / numOutputTasks));

      return deduplicatedSnapshot.build();
    }

    @Override
    @Timed("snapshot_reduplicate")
    public Snapshot reduplicate(DeduplicatedSnapshot deduplicatedSnapshot) throws CodingException {
      LOG.info("Starting reduplication.");
      Snapshot._Builder snapshot = deduplicatedSnapshot.getPartialSnapshot().mutate();
      if (!deduplicatedSnapshot.hasTaskConfigs()) {
        LOG.warn("Got deduplicated snapshot with unset task configs.");
        return snapshot.build();
      }

      for (DeduplicatedScheduledTask partialTask : deduplicatedSnapshot.getPartialTasks()) {
        ScheduledTask._Builder scheduledTask = partialTask.getPartialScheduledTask().mutate();
        int taskConfigId = partialTask.getTaskConfigId();
        TaskConfig config;
        try {
          config = deduplicatedSnapshot.getTaskConfigs().get(taskConfigId);
        } catch (IndexOutOfBoundsException e) {
          throw new CodingException(
              "DeduplicatedScheduledTask referenced invalid task index " + taskConfigId, e);
        }
        scheduledTask.mutableAssignedTask().setTask(config);
        snapshot.addToTasks(scheduledTask.build());
      }

      int numInputTasks = deduplicatedSnapshot.getTaskConfigs().size();
      int numOutputTasks = snapshot.mutableTasks().size();
      LOG.info(String.format(
          "Finished reduplicating snapshot. Compression ratio: %d/%d = %.2f%%.",
          numInputTasks,
          numOutputTasks,
          100.0 * numInputTasks / numOutputTasks));

      return snapshot.build();
    }
  }
}
