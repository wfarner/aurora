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
package org.apache.aurora.scheduler.storage;

import java.util.EnumSet;
import java.util.List;
import java.util.Set;

import com.google.common.base.Optional;

import org.apache.aurora.gen.JobUpdateStatus;
import org.apache.aurora.scheduler.storage.entities.IJobInstanceUpdateEvent;
import org.apache.aurora.scheduler.storage.entities.IJobUpdateDetails;
import org.apache.aurora.scheduler.storage.entities.IJobUpdateInstructions;
import org.apache.aurora.scheduler.storage.entities.IJobUpdateKey;
import org.apache.aurora.scheduler.storage.entities.IJobUpdateQuery;
import org.apache.aurora.scheduler.storage.entities.IJobUpdateSummary;

import static org.apache.aurora.gen.JobUpdateStatus.ABORTED;
import static org.apache.aurora.gen.JobUpdateStatus.ERROR;
import static org.apache.aurora.gen.JobUpdateStatus.FAILED;
import static org.apache.aurora.gen.JobUpdateStatus.ROLLED_BACK;
import static org.apache.aurora.gen.JobUpdateStatus.ROLLED_FORWARD;

/**
 * Stores all job updates and defines methods for saving, updating and fetching job updates.
 */
public interface JobUpdateStore {

  EnumSet<JobUpdateStatus> TERMINAL_STATES = EnumSet.of(
      ROLLED_FORWARD,
      ROLLED_BACK,
      ABORTED,
      FAILED,
      ERROR
  );

  /**
   * Fetches a read-only view of job update summaries.
   *
   * @param query Query to identify job update summaries with.
   * @return A read-only view of job update summaries.
   */
  List<IJobUpdateSummary> fetchJobUpdateSummaries(IJobUpdateQuery query);

  /**
   * Fetches a read-only view of job update details matching the {@code query}.
   *
   * @param query Query to identify job update details with.
   * @return A read-only list view of job update details matching the query.
   */
  List<IJobUpdateDetails> fetchJobUpdateDetails(IJobUpdateQuery query);

  /**
   * Fetches a read-only view of job update details.
   *
   * @param key Update identifier.
   * @return A read-only view of job update details.
   */
  Optional<IJobUpdateDetails> fetchJobUpdateDetails(IJobUpdateKey key);

  /**
   * Fetches a read-only view of the instructions for a job update.
   *
   * @param key Update identifier.
   * @return A read-only view of job update instructions.
   */
  Optional<IJobUpdateInstructions> fetchJobUpdateInstructions(IJobUpdateKey key);

  /**
   * Fetches a read-only view of all job update details available in the store.
   * TODO(wfarner): Generate immutable wrappers for storage.thrift structs, use an immutable object
   *                here.
   *
   * @return A read-only view of all job update details.
   */
  Set<IJobUpdateDetails> fetchAllJobUpdateDetails();
  /**
   * Fetches the events that have affected an instance within a job update.
   *
   * @param key Update identifier.
   * @param instanceId Instance to fetch events for.
   * @return Instance events in {@code key} that affected {@code instanceId}.
   */
  List<IJobInstanceUpdateEvent> fetchInstanceEvents(IJobUpdateKey key, int instanceId);

  interface Mutable extends JobUpdateStore {

    /**
     * Saves a job update.
     *
     * @param update Update to save.
     */
    void saveJobUpdate(IJobUpdateDetails update);

    /**
     * Deletes job updates.
     *
     * @param keys Keys of the updates to delete.
     */
    void removeJobUpdates(Set<IJobUpdateKey> keys);

    /**
     * Deletes all updates and update events from the store.
     */
    void deleteAllUpdatesAndEvents();
  }
}
