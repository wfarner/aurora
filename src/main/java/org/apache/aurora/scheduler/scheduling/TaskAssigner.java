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

import java.util.Map;

import org.apache.aurora.scheduler.base.TaskGroupKey;
import org.apache.aurora.scheduler.filter.SchedulingFilter.ResourceRequest;
import org.apache.aurora.scheduler.storage.entities.IAssignedTask;
import org.apache.mesos.v1.Protos;
import org.apache.mesos.v1.Protos.OfferID;

/**
 * Responsible for matching a task against an offer and launching it.
 */
public interface TaskAssigner {

  /**
   * Tries match a task against available offers.  If a match is found, the assigner makes the
   * appropriate changes to the task and requests task launch.
   *
   * @param resourceRequest The request for resources being scheduled.
   * @param groupKey Task group key.
   * @param tasks Tasks to assign.
   * @param preemptionReservations Slave reservations.
   * @return Successfully assigned task IDs.
   */
  Map<String, Protos.OfferID> findMatches(
      ResourceRequest resourceRequest,
      TaskGroupKey groupKey,
      Iterable<IAssignedTask> tasks,
      Map<String, TaskGroupKey> preemptionReservations);

  class ProposedAssignment {
    private final IAssignedTask task;
    private final Protos.OfferID offer;

    public ProposedAssignment(IAssignedTask task, OfferID offer) {
      this.task = task;
      this.offer = offer;
    }
  }
}