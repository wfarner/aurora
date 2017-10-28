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
package org.apache.aurora.scheduler.state;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.inject.Inject;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;

import org.apache.aurora.common.util.Clock;
import org.apache.aurora.gen.AssignedTask;
import org.apache.aurora.gen.ScheduleStatus;
import org.apache.aurora.gen.ScheduledTask;
import org.apache.aurora.gen.TaskConfig;
import org.apache.aurora.gen.TaskEvent;
import org.apache.aurora.scheduler.TaskIdGenerator;
import org.apache.aurora.scheduler.base.Query;
import org.apache.aurora.scheduler.base.Tasks;
import org.apache.aurora.scheduler.events.EventSink;
import org.apache.aurora.scheduler.events.PubsubEvent;
import org.apache.aurora.scheduler.events.PubsubEvent.TaskStateChange;
import org.apache.aurora.scheduler.mesos.Driver;
import org.apache.aurora.scheduler.scheduling.RescheduleCalculator;
import org.apache.aurora.scheduler.state.SideEffect.Action;
import org.apache.aurora.scheduler.storage.Storage.MutableStoreProvider;
import org.apache.aurora.scheduler.storage.TaskStore;
import org.apache.mesos.v1.Protos.AgentID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.Objects.requireNonNull;

import static org.apache.aurora.common.base.MorePreconditions.checkNotBlank;
import static org.apache.aurora.gen.ScheduleStatus.ASSIGNED;
import static org.apache.aurora.gen.ScheduleStatus.INIT;
import static org.apache.aurora.gen.ScheduleStatus.PENDING;
import static org.apache.aurora.gen.ScheduleStatus.THROTTLED;
import static org.apache.aurora.scheduler.state.StateChangeResult.INVALID_CAS_STATE;
import static org.apache.aurora.scheduler.state.StateChangeResult.SUCCESS;

/**
 * Manager of all persistence-related operations for the scheduler.  Acts as a controller for
 * persisted state machine transitions, and their side-effects.
 */
public class StateManagerImpl implements StateManager {
  private static final Logger LOG = LoggerFactory.getLogger(StateManagerImpl.class);

  private final Clock clock;
  private final Driver driver;
  private final TaskIdGenerator taskIdGenerator;
  private final EventSink eventSink;
  private final RescheduleCalculator rescheduleCalculator;

  @Inject
  StateManagerImpl(
      final Clock clock,
      Driver driver,
      TaskIdGenerator taskIdGenerator,
      EventSink eventSink,
      RescheduleCalculator rescheduleCalculator) {

    this.clock = requireNonNull(clock);
    this.driver = requireNonNull(driver);
    this.taskIdGenerator = requireNonNull(taskIdGenerator);
    this.eventSink = requireNonNull(eventSink);
    this.rescheduleCalculator = requireNonNull(rescheduleCalculator);
  }

  private ScheduledTask createTask(int instanceId, TaskConfig template) {
    AssignedTask assigned = AssignedTask.builder()
        .setTaskId(taskIdGenerator.generate(template, instanceId))
        .setInstanceId(instanceId)
        .setTask(template)
        .build();
    return ScheduledTask.builder()
        .setStatus(INIT)
        .setAssignedTask(assigned)
        .build();
  }

  @Override
  public void insertPendingTasks(
      MutableStoreProvider storeProvider,
      final TaskConfig task,
      Set<Integer> instanceIds) {

    requireNonNull(storeProvider);
    requireNonNull(task);
    checkNotBlank(instanceIds);

    // Done outside the write transaction to minimize the work done inside a transaction.
    Set<ScheduledTask> scheduledTasks = FluentIterable.from(instanceIds)
        .transform(instanceId -> createTask(instanceId, task)).toSet();

    Iterable<ScheduledTask> collision = storeProvider.getTaskStore().fetchTasks(
        Query.instanceScoped(task.getJob(), instanceIds).active());

    if (!Iterables.isEmpty(collision)) {
      throw new IllegalArgumentException("Instance ID collision detected.");
    }

    storeProvider.getUnsafeTaskStore().saveTasks(scheduledTasks);

    for (ScheduledTask scheduledTask : scheduledTasks) {
      updateTaskAndExternalState(
          storeProvider.getUnsafeTaskStore(),
          Tasks.id(scheduledTask),
          Optional.of(scheduledTask),
          Optional.of(PENDING),
          Optional.absent());
    }
  }

  @Override
  public StateChangeResult changeState(
      MutableStoreProvider storeProvider,
      String taskId,
      Optional<ScheduleStatus> casState,
      final ScheduleStatus newState,
      final Optional<String> auditMessage) {

    return updateTaskAndExternalState(
        storeProvider.getUnsafeTaskStore(),
        casState,
        taskId,
        newState,
        auditMessage);
  }

  @Override
  public AssignedTask assignTask(
      MutableStoreProvider storeProvider,
      String taskId,
      String slaveHost,
      AgentID slaveId,
      Function<AssignedTask, AssignedTask> resourceAssigner) {

    checkNotBlank(taskId);
    checkNotBlank(slaveHost);
    requireNonNull(slaveId);
    requireNonNull(resourceAssigner);

    ScheduledTask mutated = storeProvider.getUnsafeTaskStore().mutateTask(taskId,
        task -> {
          ScheduledTask._Builder builder = task.mutate();
          builder.setAssignedTask(resourceAssigner.apply(task.getAssignedTask()));
          builder.mutableAssignedTask()
              .setSlaveHost(slaveHost)
              .setSlaveId(slaveId.getValue());
          return builder.build();
        }).get();

    StateChangeResult changeResult = updateTaskAndExternalState(
        storeProvider.getUnsafeTaskStore(),
        Optional.absent(),
        taskId,
        ASSIGNED,
        Optional.absent());

    Preconditions.checkState(
        changeResult == SUCCESS,
        "Attempt to assign task %s to %s failed",
        taskId,
        slaveHost);

    return mutated.getAssignedTask();
  }

  @VisibleForTesting
  static final Supplier<String> LOCAL_HOST_SUPPLIER = Suppliers.memoize(
      () -> {
        try {
          return InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
          LOG.error("Failed to get self hostname.");
          throw new RuntimeException(e);
        }
      });

  private StateChangeResult updateTaskAndExternalState(
      TaskStore.Mutable taskStore,
      Optional<ScheduleStatus> casState,
      String taskId,
      ScheduleStatus targetState,
      Optional<String> transitionMessage) {

    Optional<ScheduledTask> task = taskStore.fetchTask(taskId);

    // CAS operation fails if the task does not exist, or the states don't match.
    if (casState.isPresent()
        && (!task.isPresent() || casState.get() != task.get().getStatus())) {

      return INVALID_CAS_STATE;
    }

    return updateTaskAndExternalState(
        taskStore,
        taskId,
        task,
        Optional.of(targetState),
        transitionMessage);
  }

  private static final List<Action> ACTIONS_IN_ORDER = ImmutableList.of(
      Action.INCREMENT_FAILURES,
      Action.SAVE_STATE,
      Action.RESCHEDULE,
      Action.KILL,
      Action.DELETE);

  static {
    // Sanity check to ensure no actions are missing.
    Preconditions.checkState(
        ImmutableSet.copyOf(ACTIONS_IN_ORDER).equals(ImmutableSet.copyOf(Action.values())),
        "Not all actions are included in ordering.");
  }

  // Actions are deliberately ordered to prevent things like deleting a task before rescheduling it
  // (thus losing the object to copy), or rescheduling a task before incrementing the failure count
  // (thus not carrying forward the failure increment).
  private static final Ordering<SideEffect> ACTION_ORDER =
      Ordering.explicit(ACTIONS_IN_ORDER).onResultOf(SideEffect::getAction);

  private StateChangeResult updateTaskAndExternalState(
      TaskStore.Mutable taskStore,
      String taskId,
      // Note: This argument should be used with caution.
      // This is because using the captured value within the storage operation below is
      // highly-risky, since it doesn't necessarily represent the value in storage.
      // As a result, it would be easy to accidentally clobber mutations.
      Optional<ScheduledTask> task,
      Optional<ScheduleStatus> targetState,
      Optional<String> transitionMessage) {

    if (task.isPresent()) {
      Preconditions.checkArgument(taskId.equals(task.get().getAssignedTask().getTaskId()));
    }

    List<PubsubEvent> events = Lists.newArrayList();

    TaskStateMachine stateMachine = task.isPresent()
        ? new TaskStateMachine(task.get())
        : new TaskStateMachine(taskId);

    TransitionResult result = stateMachine.updateState(targetState);

    for (SideEffect sideEffect : ACTION_ORDER.sortedCopy(result.getSideEffects())) {
      Optional<ScheduledTask> upToDateTask = taskStore.fetchTask(taskId);

      switch (sideEffect.getAction()) {
        case INCREMENT_FAILURES:
          taskStore.mutateTask(
              taskId,
              task1 -> task1.mutate().setFailureCount(task1.getFailureCount() + 1).build());
          break;

        case SAVE_STATE:
          Preconditions.checkState(
              upToDateTask.isPresent(),
              "Operation expected task %s to be present.",
              taskId);

          Optional<ScheduledTask> mutated = taskStore.mutateTask(taskId, task1 -> {
            ScheduledTask._Builder mutableTask = task1.mutate();
            mutableTask.setStatus(targetState.get());
            mutableTask.addToTaskEvents(TaskEvent.builder()
                .setTimestamp(clock.nowMillis())
                .setStatus(targetState.get())
                .setMessage(transitionMessage.orNull())
                .setScheduler(LOCAL_HOST_SUPPLIER.get())
                .build());
            return mutableTask.build();
          });
          events.add(TaskStateChange.transition(mutated.get(), stateMachine.getPreviousState()));
          break;

        case RESCHEDULE:
          Preconditions.checkState(
              upToDateTask.isPresent(),
              "Operation expected task %s to be present.",
              taskId);
          LOG.info("Task being rescheduled: " + taskId);

          ScheduleStatus newState;
          String auditMessage;
          long flapPenaltyMs = rescheduleCalculator.getFlappingPenaltyMs(upToDateTask.get());
          if (flapPenaltyMs > 0) {
            newState = THROTTLED;
            auditMessage =
                String.format("Rescheduled, penalized for %s ms for flapping", flapPenaltyMs);
          } else {
            newState = PENDING;
            auditMessage = "Rescheduled";
          }

          ScheduledTask newTask = createTask(
              upToDateTask.get().getAssignedTask().getInstanceId(),
              upToDateTask.get().getAssignedTask().getTask())
              .mutate()
              .setFailureCount(upToDateTask.get().getFailureCount())
              .setAncestorId(taskId)
              .build();
          taskStore.saveTasks(ImmutableSet.of(newTask));
          updateTaskAndExternalState(
              taskStore,
              Tasks.id(newTask),
              Optional.of(newTask),
              Optional.of(newState),
              Optional.of(auditMessage));
          break;

        case KILL:
          driver.killTask(taskId);
          break;

        case DELETE:
          Preconditions.checkState(
              upToDateTask.isPresent(),
              "Operation expected task %s to be present.",
              taskId);

          PubsubEvent.TasksDeleted event = createDeleteEvent(taskStore, ImmutableSet.of(taskId));
          taskStore.deleteTasks(
              event.getTasks().stream().map(Tasks::id).collect(Collectors.toSet()));
          events.add(event);
          break;

        default:
          throw new IllegalStateException("Unrecognized side-effect " + sideEffect.getAction());
      }
    }

    // Note (AURORA-138): Delaying events until after the write operation is somewhat futile, since
    // the state may actually not be written to durable store
    // (e.g. if this is a nested transaction). Ideally, Storage would add a facility to attach
    // side-effects that are performed after the outer-most transaction completes (meaning state
    // has been durably persisted).
    for (PubsubEvent event : events) {
      eventSink.post(event);
    }

    return result.getResult();
  }

  @Override
  public void deleteTasks(MutableStoreProvider storeProvider, final Set<String> taskIds) {
    TaskStore.Mutable taskStore = storeProvider.getUnsafeTaskStore();
    // Create a single event for all task deletions.
    PubsubEvent.TasksDeleted event = createDeleteEvent(taskStore, taskIds);

    taskStore.deleteTasks(event.getTasks().stream().map(Tasks::id).collect(Collectors.toSet()));

    eventSink.post(event);
  }

  private static PubsubEvent.TasksDeleted createDeleteEvent(
      TaskStore.Mutable taskStore,
      Set<String> taskIds) {

    return new PubsubEvent.TasksDeleted(
        ImmutableSet.copyOf(taskStore.fetchTasks(Query.taskScoped(taskIds))));
  }
}
