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
package org.apache.aurora.scheduler.base;

import java.util.List;
import java.util.Set;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import com.google.common.collect.Ordering;

import org.apache.aurora.gen.Api_Constants;
import org.apache.aurora.gen.AssignedTask;
import org.apache.aurora.gen.JobKey;
import org.apache.aurora.gen.ScheduleStatus;
import org.apache.aurora.gen.ScheduledTask;
import org.apache.aurora.gen.TaskConfig;
import org.apache.aurora.gen.TaskEvent;

/**
 * Utility class providing convenience functions relating to tasks.
 */
public final class Tasks {

  @VisibleForTesting
  static final List<ScheduleStatus> ORDERED_TASK_STATUSES = ImmutableList.<ScheduleStatus>builder()
          .addAll(Api_Constants.TERMINAL_STATES)
          .addAll(Api_Constants.ACTIVE_STATES)
          .build();

  public static TaskConfig getConfig(ScheduledTask scheduledTask) {
    return scheduledTask.getAssignedTask().getTask();
  }

  public static int getInstanceId(ScheduledTask scheduledTask) {
    return scheduledTask.getAssignedTask().getInstanceId();
  }

  public static JobKey getJob(AssignedTask assignedTask) {
    return assignedTask.getTask().getJob();
  }

  public static JobKey getJob(ScheduledTask scheduledTask) {
    return getJob(scheduledTask.getAssignedTask());
  }

  public static String scheduledToSlaveHost(ScheduledTask scheduledTask) {
    return scheduledTask.getAssignedTask().getSlaveHost();
  }

  private Tasks() {
    // Utility class.
  }

  /**
   * A utility method that returns a multi-map of tasks keyed by JobKey.
   *
   * @param tasks A list of tasks to be keyed by map
   * @return A multi-map of tasks keyed by job key.
   */
  public static Multimap<JobKey, ScheduledTask> byJobKey(Iterable<ScheduledTask> tasks) {
    return Multimaps.index(tasks, Tasks::getJob);
  }

  public static boolean isActive(ScheduleStatus status) {
    return Api_Constants.ACTIVE_STATES.contains(status);
  }

  public static boolean isTerminated(ScheduleStatus status) {
    return Api_Constants.TERMINAL_STATES.contains(status);
  }

  public static String id(ScheduledTask task) {
    return task.getAssignedTask().getTaskId();
  }

  public static Set<String> ids(Iterable<ScheduledTask> tasks) {
    return ImmutableSet.copyOf(Iterables.transform(tasks, Tasks::id));
  }

  public static Set<String> ids(ScheduledTask... tasks) {
    return ids(ImmutableList.copyOf(tasks));
  }

  /**
   * Get the latest active task or the latest inactive task if no active task exists.
   *
   * @param tasks a collection of tasks
   * @return the task that transitioned most recently.
   */
  public static ScheduledTask getLatestActiveTask(Iterable<ScheduledTask> tasks) {
    Preconditions.checkArgument(Iterables.size(tasks) != 0);

    return Ordering.explicit(ORDERED_TASK_STATUSES)
        .onResultOf(ScheduledTask::getStatus)
        .compound(LATEST_ACTIVITY)
        .max(tasks);
  }

  public static TaskEvent getLatestEvent(ScheduledTask task) {
    return Iterables.getLast(task.getTaskEvents());
  }

  public static final Ordering<ScheduledTask> LATEST_ACTIVITY = Ordering.natural()
      .onResultOf(new Function<ScheduledTask, Long>() {
        @Override
        public Long apply(ScheduledTask task) {
          return getLatestEvent(task).getTimestamp();
        }
      });
}
