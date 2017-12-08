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
package org.apache.aurora.scheduler.scheduling;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import javax.inject.Inject;
import javax.inject.Qualifier;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.google.common.eventbus.Subscribe;

import org.apache.aurora.common.inject.TimedInterceptor.Timed;
import org.apache.aurora.common.stats.Stats;
import org.apache.aurora.scheduler.base.Query;
import org.apache.aurora.scheduler.base.TaskGroupKey;
import org.apache.aurora.scheduler.configuration.executor.ExecutorSettings;
import org.apache.aurora.scheduler.events.PubsubEvent;
import org.apache.aurora.scheduler.filter.AttributeAggregate;
import org.apache.aurora.scheduler.filter.SchedulingFilter;
import org.apache.aurora.scheduler.preemptor.BiCache;
import org.apache.aurora.scheduler.preemptor.Preemptor;
import org.apache.aurora.scheduler.resources.ResourceBag;
import org.apache.aurora.scheduler.storage.Storage.MutableStoreProvider;
import org.apache.aurora.scheduler.storage.Storage.StoreProvider;
import org.apache.aurora.scheduler.storage.entities.IAssignedTask;
import org.apache.aurora.scheduler.storage.entities.IScheduledTask;
import org.apache.aurora.scheduler.storage.entities.ITaskConfig;
import org.apache.mesos.Protos.OfferID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.ElementType.PARAMETER;
import static java.lang.annotation.RetentionPolicy.RUNTIME;
import static java.util.Objects.requireNonNull;

import static org.apache.aurora.gen.ScheduleStatus.PENDING;
import static org.apache.aurora.scheduler.resources.ResourceManager.bagFromResources;

/**
 * An asynchronous task scheduler.  Scheduling of tasks is performed on a delay, where each task
 * backs off after a failed scheduling attempt.
 * <p>
 * Pending tasks are advertised to the scheduler via internal pubsub notifications.
 */
public class TaskSchedulerImpl implements TaskScheduler {
  /**
   * Binding annotation for the time duration of reservations.
   */
  @VisibleForTesting
  @Qualifier
  @Target({ FIELD, PARAMETER, METHOD }) @Retention(RUNTIME)
  public @interface ReservationDuration { }

  private static final Logger LOG =
      LoggerFactory.getLogger(org.apache.aurora.scheduler.scheduling.TaskSchedulerImpl.class);

  private final TaskAssigner assigner;
  private final Preemptor preemptor;
  private final ExecutorSettings executorSettings;
  private final BiCache<String, TaskGroupKey> reservations;

  private final AtomicLong attemptsFired = Stats.exportLong("schedule_attempts_fired");
  private final AtomicLong attemptsFailed = Stats.exportLong("schedule_attempts_failed");
  private final AtomicLong attemptsNoMatch = Stats.exportLong("schedule_attempts_no_match");

  @Inject
  TaskSchedulerImpl(
      TaskAssigner assigner,
      Preemptor preemptor,
      ExecutorSettings executorSettings,
      BiCache<String, TaskGroupKey> reservations) {

    this.assigner = requireNonNull(assigner);
    this.preemptor = requireNonNull(preemptor);
    this.executorSettings = requireNonNull(executorSettings);
    this.reservations = requireNonNull(reservations);
  }

  @Timed("task_schedule_attempt")
  @Override
  public Map<String, MatchResult> findMatches(StoreProvider store, Set<String> taskIds) {
    try {
      // TODO(wfarner): Consider removing the RTE catch from this method, and instead move to the
      // assignment function.
      return doFindMatches(store, taskIds);
    } catch (RuntimeException e) {
      // We catch the generic unchecked exception here to ensure tasks are not abandoned
      // if there is a transient issue resulting in an unchecked exception.
      LOG.warn("Task scheduling unexpectedly failed, will be retried", e);
      attemptsFailed.incrementAndGet();
      // Return empty map for all task IDs to be retried later.
      // It's ok if some tasks were already assigned, those will be ignored in the next round.
      return ImmutableMap.of();
    }
  }

  private Map<String, MatchResult> doFindMatches(StoreProvider store, Set<String> tasks) {

    ImmutableSet<String> taskIds = ImmutableSet.copyOf(tasks);
    String taskIdValues = Joiner.on(",").join(taskIds);
    LOG.debug("Attempting to schedule tasks {}", taskIdValues);
    Set<IAssignedTask> assignedTasks =
        ImmutableSet.copyOf(Iterables.transform(
            store.getTaskStore().fetchTasks(Query.taskScoped(taskIds).byStatus(PENDING)),
            IScheduledTask::getAssignedTask));
    Map<String, IAssignedTask> assignableTasks = assignedTasks.stream()
        .collect(Collectors.toMap(IAssignedTask::getTaskId, t -> t));

    ImmutableMap.Builder<String, MatchResult> result = ImmutableMap.builder();
    Sets.difference(tasks, assignableTasks.keySet())
        .forEach(invalidTask -> result.put(invalidTask, MatchResult.invalidTask()));

    // Ensure these tasks are identical with respect to schedulability.
    Preconditions.checkState(
        assignedTasks.stream()
            .collect(Collectors.groupingBy(t -> t.getTask()))
            .entrySet()
            .size() == 1,
        "Found multiple task groups for %s",
        taskIdValues);
    ITaskConfig task = assignedTasks.stream().findFirst().get().getTask();

    AttributeAggregate aggregate = AttributeAggregate.getJobActiveState(store, task.getJob());

    // Valid Docker tasks can have a container but no executor config
    ResourceBag overhead = ResourceBag.EMPTY;
    if (task.isSetExecutorConfig()) {
      overhead = executorSettings.getExecutorOverhead(task.getExecutorConfig().getName())
          .orElseThrow(
              () -> new IllegalArgumentException("Cannot find executor configuration"));
    }

    Set<String> launched = assigner.maybeAssign(
        store,
        new SchedulingFilter.ResourceRequest(
            task,
            bagFromResources(task.getResources()).add(overhead), aggregate),
        TaskGroupKey.from(task),
        assignedTasks,
        reservations.asMap());

    attemptsFired.addAndGet(assignableTasks.size());
    Set<String> failedToLaunch = Sets.difference(assignableTasks.keySet(), launched);

    failedToLaunch.forEach(taskId -> {
      // Task could not be scheduled.
      // TODO(maxim): Now that preemption slots are searched asynchronously, consider
      // retrying a launch attempt within the current scheduling round IFF a reservation is
      // available.
      maybePreemptFor(assignableTasks.get(taskId), aggregate, store);
    });
    attemptsNoMatch.addAndGet(failedToLaunch.size());

    return result.build();
  }

  private void maybePreemptFor(
      IAssignedTask task,
      AttributeAggregate jobState,
      MutableStoreProvider storeProvider) {

    if (!reservations.getByValue(TaskGroupKey.from(task.getTask())).isEmpty()) {
      return;
    }
    Optional<String> slaveId = preemptor.attemptPreemptionFor(task, jobState, storeProvider);
    if (slaveId.isPresent()) {
      reservations.put(slaveId.get(), TaskGroupKey.from(task.getTask()));
    }
  }

  @Subscribe
  public void taskChanged(final PubsubEvent.TaskStateChange stateChangeEvent) {
    if (Optional.of(PENDING).equals(stateChangeEvent.getOldState())) {
      IAssignedTask assigned = stateChangeEvent.getTask().getAssignedTask();
      if (assigned.getSlaveId() != null) {
        reservations.remove(assigned.getSlaveId(), TaskGroupKey.from(assigned.getTask()));
      }
    }
  }
}
