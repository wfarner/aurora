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
package org.apache.aurora.scheduler.updater;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Range;

import org.apache.aurora.common.quantity.Amount;
import org.apache.aurora.common.quantity.Time;
import org.apache.aurora.gen.JobUpdateStatus;
import org.apache.aurora.gen.ScheduleStatus;
import org.apache.aurora.scheduler.base.Query;
import org.apache.aurora.scheduler.base.Tasks;
import org.apache.aurora.scheduler.state.StateManager;
import org.apache.aurora.gen.InstanceKey;
import org.apache.aurora.gen.InstanceTaskConfig;
import org.apache.aurora.gen.JobUpdateInstructions;
import org.apache.aurora.gen.JobUpdateKey;
import org.apache.aurora.gen.Range;
import org.apache.aurora.gen.ScheduledTask;
import org.apache.aurora.gen.TaskConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.aurora.gen.JobUpdateStatus.ROLLING_FORWARD;
import static org.apache.aurora.scheduler.storage.Storage.MutableStoreProvider;

interface InstanceActionHandler {

  Optional<Amount<Long, Time>> getReevaluationDelay(
      InstanceKey instance,
      JobUpdateInstructions instructions,
      MutableStoreProvider storeProvider,
      StateManager stateManager,
      UpdateAgentReserver reserver,
      JobUpdateStatus status,
      JobUpdateKey key);

  Logger LOG = LoggerFactory.getLogger(InstanceActionHandler.class);

  static Optional<ScheduledTask> getExistingTask(
      MutableStoreProvider storeProvider,
      InstanceKey instance) {

    return Optional.fromNullable(Iterables.getOnlyElement(
        storeProvider.getTaskStore().fetchTasks(Query.instanceScoped(instance).active()), null));
  }

  class AddTask implements InstanceActionHandler {
    private static TaskConfig getTargetConfig(
        JobUpdateInstructions instructions,
        boolean rollingForward,
        int instanceId) {

      if (rollingForward) {
        // Desired state is assumed to be non-null when AddTask is used.
        return instructions.getDesiredState().getTask();
      } else {
        for (IInstanceTaskConfig config : instructions.getInitialState()) {
          for (Range range : config.getInstances()) {
            if (Range.closed(range.getFirst(), range.getLast()).contains(instanceId)) {
              return config.getTask();
            }
          }
        }

        throw new IllegalStateException("Failed to find instance " + instanceId);
      }
    }

    @Override
    public Optional<Amount<Long, Time>> getReevaluationDelay(
        InstanceKey instance,
        JobUpdateInstructions instructions,
        MutableStoreProvider storeProvider,
        StateManager stateManager,
        UpdateAgentReserver reserver,
        JobUpdateStatus status,
        JobUpdateKey key) {

      Optional<ScheduledTask> task = getExistingTask(storeProvider, instance);
      if (task.isPresent()) {
        // Due to async event processing it's possible to have a race between task event
        // and instance addition. This is a perfectly valid case.
        LOG.info("Instance " + instance + " already exists while " + status);
      } else {
        LOG.info("Adding instance " + instance + " while " + status);
        TaskConfig replacement = getTargetConfig(
            instructions,
            status == ROLLING_FORWARD,
            instance.getInstanceId());
        stateManager.insertPendingTasks(
            storeProvider,
            replacement,
            ImmutableSet.of(instance.getInstanceId()));
      }
      // A task state transition will trigger re-evaluation in this case, rather than a timer.
      return Optional.absent();
    }
  }

  class KillTask implements InstanceActionHandler {
    private final boolean reserveForReplacement;

    KillTask(boolean reserveForReplacement) {
      this.reserveForReplacement = reserveForReplacement;
    }

    @Override
    public Optional<Amount<Long, Time>> getReevaluationDelay(
        InstanceKey instance,
        JobUpdateInstructions instructions,
        MutableStoreProvider storeProvider,
        StateManager stateManager,
        UpdateAgentReserver reserver,
        JobUpdateStatus status,
        JobUpdateKey key) {

      Optional<ScheduledTask> task = getExistingTask(storeProvider, instance);
      if (task.isPresent()) {
        LOG.info("Killing " + instance + " while " + status);
        stateManager.changeState(
            storeProvider,
            Tasks.id(task.get()),
            Optional.absent(),
            ScheduleStatus.KILLING,
            Optional.of("Killed for job update " + key.getId()));
        if (reserveForReplacement && task.get().getAssignedTask().hasSlaveId()) {
          reserver.reserve(task.get().getAssignedTask().getSlaveId(), instance);
        }
      } else {
        // Due to async event processing it's possible to have a race between task event
        // and it's deletion from the store. This is a perfectly valid case.
        LOG.info("No active instance " + instance + " to kill while " + status);
      }
      // A task state transition will trigger re-evaluation in this case, rather than a timer.
      return Optional.absent();
    }
  }

  class WatchRunningTask implements InstanceActionHandler {
    @Override
    public Optional<Amount<Long, Time>> getReevaluationDelay(
        InstanceKey instance,
        JobUpdateInstructions instructions,
        MutableStoreProvider storeProvider,
        StateManager stateManager,
        UpdateAgentReserver reserver,
        JobUpdateStatus status,
        JobUpdateKey key) {

      return Optional.of(Amount.of(
          (long) instructions.getSettings().getMinWaitInInstanceRunningMs(),
          Time.MILLISECONDS));
    }
  }
}
