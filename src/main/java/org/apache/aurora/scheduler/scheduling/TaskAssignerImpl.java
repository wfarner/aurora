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

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import javax.annotation.Nullable;
import javax.inject.Inject;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;

import org.apache.aurora.common.stats.StatsProvider;
import org.apache.aurora.scheduler.TierManager;
import org.apache.aurora.scheduler.base.InstanceKeys;
import org.apache.aurora.scheduler.base.TaskGroupKey;
import org.apache.aurora.scheduler.filter.SchedulingFilter.ResourceRequest;
import org.apache.aurora.scheduler.mesos.MesosTaskFactory;
import org.apache.aurora.scheduler.offers.HostOffer;
import org.apache.aurora.scheduler.offers.OfferManager;
import org.apache.aurora.scheduler.offers.OfferManager.LaunchException;
import org.apache.aurora.scheduler.resources.ResourceManager;
import org.apache.aurora.scheduler.resources.ResourceType;
import org.apache.aurora.scheduler.state.StateManager;
import org.apache.aurora.scheduler.storage.Storage;
import org.apache.aurora.scheduler.storage.entities.IAssignedTask;
import org.apache.aurora.scheduler.storage.entities.IInstanceKey;
import org.apache.aurora.scheduler.updater.UpdateAgentReserver;
import org.apache.mesos.v1.Protos;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.Objects.requireNonNull;

import static org.apache.aurora.common.inject.TimedInterceptor.Timed;
import static org.apache.aurora.gen.ScheduleStatus.LOST;
import static org.apache.aurora.gen.ScheduleStatus.PENDING;

public class TaskAssignerImpl implements TaskAssigner {
  private static final Logger LOG = LoggerFactory.getLogger(TaskAssignerImpl.class);

  @VisibleForTesting
  static final Optional<String> LAUNCH_FAILED_MSG =
      Optional.of("Unknown exception attempting to schedule task.");
  @VisibleForTesting
  static final String ASSIGNER_LAUNCH_FAILURES = "assigner_launch_failures";

  private final AtomicLong launchFailures;

  private final StateManager stateManager;
  private final MesosTaskFactory taskFactory;
  private final OfferManager offerManager;
  private final TierManager tierManager;
  private final UpdateAgentReserver updateAgentReserver;
  private final OfferSelector offerSelector;

  @Inject
  public TaskAssignerImpl(
      StateManager stateManager,
      MesosTaskFactory taskFactory,
      OfferManager offerManager,
      TierManager tierManager,
      UpdateAgentReserver updateAgentReserver,
      StatsProvider statsProvider,
      OfferSelector offerSelector) {

    this.stateManager = requireNonNull(stateManager);
    this.taskFactory = requireNonNull(taskFactory);
    this.offerManager = requireNonNull(offerManager);
    this.tierManager = requireNonNull(tierManager);
    this.launchFailures = statsProvider.makeCounter(ASSIGNER_LAUNCH_FAILURES);
    this.updateAgentReserver = requireNonNull(updateAgentReserver);
    this.offerSelector = requireNonNull(offerSelector);
  }

  @VisibleForTesting
  IAssignedTask mapAndAssignResources(Protos.Offer offer, IAssignedTask task) {
    IAssignedTask assigned = task;
    for (ResourceType type : ResourceManager.getTaskResourceTypes(assigned)) {
      if (type.getMapper().isPresent()) {
        assigned = type.getMapper().get().mapAndAssign(offer, assigned);
      }
    }
    return assigned;
  }

  private Protos.TaskInfo assign(
      Storage.MutableStoreProvider storeProvider,
      Protos.Offer offer,
      String taskId,
      boolean revocable) {

    String host = offer.getHostname();
    IAssignedTask assigned = stateManager.assignTask(
        storeProvider,
        taskId,
        host,
        offer.getAgentId(),
        task -> mapAndAssignResources(offer, task));
    LOG.info(
        "Offer on agent {} (id {}) is being assigned task for {}.",
        host, offer.getAgentId().getValue(), taskId);
    return taskFactory.createFrom(assigned, offer, revocable);
  }

  private void launchUsingOffer(
      Storage.MutableStoreProvider storeProvider,
      boolean revocable,
      ResourceRequest resourceRequest,
      IAssignedTask task,
      HostOffer offer) throws LaunchException {

    String taskId = task.getTaskId();
    Protos.TaskInfo taskInfo = assign(storeProvider, offer.getOffer(), taskId, revocable);
    resourceRequest.getJobState().updateAttributeAggregate(offer.getAttributes());
    try {
      offerManager.launchTask(offer.getOffer().getId(), taskInfo);
    } catch (LaunchException e) {
      LOG.warn("Failed to launch task.", e);
      launchFailures.incrementAndGet();

      // The attempt to schedule the task failed, so we need to backpedal on the assignment.
      // It is in the LOST state and a new task will move to PENDING to replace it.
      // Should the state change fail due to storage issues, that's okay.  The task will
      // time out in the ASSIGNED state and be moved to LOST.
      stateManager.changeState(
          storeProvider,
          taskId,
          Optional.of(PENDING),
          LOST,
          LAUNCH_FAILED_MSG);
      throw e;
    }
  }

  private static final class ReservationStatus {
    final boolean taskReserving;

    @Nullable
    final HostOffer offer;

    private ReservationStatus(boolean taskReserving, @Nullable HostOffer offer) {
      this.taskReserving = taskReserving;
      this.offer = offer;
    }

    static final ReservationStatus NOT_RESERVING = new ReservationStatus(false, null);
    static final ReservationStatus NOT_READY = new ReservationStatus(true, null);

    static ReservationStatus ready(HostOffer offer) {
      return new ReservationStatus(true, requireNonNull(offer));
    }

    boolean isTaskReserving() {
      return taskReserving;
    }

    boolean isReady() {
      return offer != null;
    }

    HostOffer getOffer() {
      return requireNonNull(offer);
    }
  }

  private ReservationStatus getReservation(
      IAssignedTask task,
      ResourceRequest resourceRequest,
      boolean revocable) {

    IInstanceKey key = InstanceKeys.from(task.getTask().getJob(), task.getInstanceId());
    Optional<String> agentId = updateAgentReserver.getAgent(key);
    if (!agentId.isPresent()) {
      return ReservationStatus.NOT_RESERVING;
    }
    Optional<HostOffer> offer = offerManager.getMatching(
        Protos.AgentID.newBuilder().setValue(agentId.get()).build(),
        resourceRequest,
        revocable);
    if (offer.isPresent()) {
      LOG.info("Used update reservation for {} on {}", key, agentId.get());
      updateAgentReserver.release(agentId.get(), key);
      return ReservationStatus.ready(offer.get());
    } else {
      LOG.info(
          "Tried to reuse offer on {} for {}, but was not ready yet.",
          agentId.get(),
          key);
      return ReservationStatus.NOT_READY;
    }
  }

  /**
   * Determine whether or not the offer is reserved for a different task via preemption or
   * update affinity.
   */
  private boolean isAgentReserved(
      HostOffer offer,
      TaskGroupKey groupKey,
      Map<String, TaskGroupKey> preemptionReservations) {

    String agentId = offer.getOffer().getAgentId().getValue();
    boolean reservedForPreemption = Optional.ofNullable(preemptionReservations.get(agentId))
        .map(group -> !group.equals(groupKey))
        .orElse(false);

    return reservedForPreemption || updateAgentReserver.isReserved(agentId);
  }

  private static class SchedulingMatch {
    final IAssignedTask task;
    final HostOffer offer;

    SchedulingMatch(IAssignedTask task, HostOffer offer) {
      this.task = requireNonNull(task);
      this.offer = requireNonNull(offer);
    }
  }

  private List<SchedulingMatch> findMatches(
      ResourceRequest resourceRequest,
      TaskGroupKey groupKey,
      Iterable<IAssignedTask> tasks,
      boolean revocable,
      Map<String, TaskGroupKey> preemptionReservations) {

    ImmutableList.Builder<SchedulingMatch> matches = ImmutableList.builder();

    if (Iterables.isEmpty(tasks)) {
      return matches.build();
    }

    for (IAssignedTask task : tasks) {
      ReservationStatus reservation = getReservation(task, resourceRequest, revocable);
      Optional<HostOffer> chosenOffer;
      if (reservation.isReady()) {
        chosenOffer = Optional.of(reservation.getOffer());
      } else if (reservation.isTaskReserving()) {
        continue;
      } else {
        // Get all offers that will satisfy the given ResourceRequest and that are not reserved
        // for updates or preemption
        Iterable<HostOffer> matchingOffers = Iterables.filter(
            offerManager.getAllMatching(groupKey, resourceRequest, revocable),
            o -> !isAgentReserved(o, groupKey, preemptionReservations));

        // Determine which is the optimal offer to select for the given request
        chosenOffer = offerSelector.select(matchingOffers, resourceRequest);
      }

      // If no offer is chosen, continue to the next task
      if (!chosenOffer.isPresent()) {
        continue;
      }

      matches.add(new SchedulingMatch(task, chosenOffer.get()));
    }

    return matches.build();
  }

  @Timed("assigner_maybe_assign")
  @Override
  public Set<String> maybeAssign(
      Storage.MutableStoreProvider storeProvider,
      ResourceRequest resourceRequest,
      TaskGroupKey groupKey,
      Iterable<IAssignedTask> tasks,
      Map<String, TaskGroupKey> preemptionReservations) {

    if (Iterables.isEmpty(tasks)) {
      return ImmutableSet.of();
    }

    boolean revocable = tierManager.getTier(groupKey.getTask()).isRevocable();
    ImmutableSet.Builder<String> assigned = ImmutableSet.builder();

    for (SchedulingMatch match
        : findMatches(resourceRequest, groupKey, tasks, revocable, preemptionReservations)) {

      try {
        launchUsingOffer(storeProvider, revocable, resourceRequest, match.task, match.offer);
        assigned.add(match.task.getTaskId());
      } catch (LaunchException e) {
        // Any launch exception causes the scheduling round to terminate for this TaskGroup.
        break;
      }
    }

    return assigned.build();
  }
}
