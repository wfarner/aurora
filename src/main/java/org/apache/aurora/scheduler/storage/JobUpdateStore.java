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
import org.apache.aurora.gen.JobInstanceUpdateEvent;
import org.apache.aurora.gen.JobUpdate;
import org.apache.aurora.gen.JobUpdateDetails;
import org.apache.aurora.gen.JobUpdateEvent;
import org.apache.aurora.gen.JobUpdateInstructions;
import org.apache.aurora.gen.JobUpdateKey;
import org.apache.aurora.gen.JobUpdateQuery;
import org.apache.aurora.gen.JobUpdateSummary;

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
  List<JobUpdateSummary> fetchJobUpdateSummaries(JobUpdateQuery query);

  /**
   * Fetches a read-only view of job update details matching the {@code query}.
   *
   * @param query Query to identify job update details with.
   * @return A read-only list view of job update details matching the query.
   */
  List<JobUpdateDetails> fetchJobUpdateDetails(JobUpdateQuery query);

  /**
   * Fetches a read-only view of job update details.
   *
   * @param key Update identifier.
   * @return A read-only view of job update details.
   */
  Optional<JobUpdateDetails> fetchJobUpdateDetails(JobUpdateKey key);

  /**
   * Fetches a read-only view of a job update.
   *
   * @param key Update identifier.
   * @return A read-only view of job update.
   */
  Optional<JobUpdate> fetchJobUpdate(JobUpdateKey key);

  /**
   * Fetches a read-only view of the instructions for a job update.
   *
   * @param key Update identifier.
   * @return A read-only view of job update instructions.
   */
  Optional<JobUpdateInstructions> fetchJobUpdateInstructions(JobUpdateKey key);

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
  List<JobInstanceUpdateEvent> fetchInstanceEvents(JobUpdateKey key, int instanceId);

  interface Mutable extends JobUpdateStore {

    /**
     * Saves a new job update.
     *
     * <p>
     * Note: This call must be followed by the
     * {@link #saveJobUpdateEvent(JobUpdateKey, JobUpdateEvent)} before fetching a saved update as
     * it does not save the following required fields:
     * <ul>
     *   <li>{@link org.apache.aurora.gen.JobUpdateState#getStatus()}</li>
     *   <li>{@link org.apache.aurora.gen.JobUpdateState#getCreatedTimestampMs()}</li>
     *   <li>{@link org.apache.aurora.gen.JobUpdateState#getLastModifiedTimestampMs()}</li>
     * </ul>
     * The above fields are auto-populated from the update events and any attempt to fetch an update
     * without having at least one {@link JobUpdateEvent} present in the store will return empty.
     *
     * @param update Update to save.
     */
    void saveJobUpdate(JobUpdate update);

    /**
     * Saves a new job update event.
     *
     * @param key Update identifier.
     * @param event Job update event to save.
     */
    void saveJobUpdateEvent(JobUpdateKey key, JobUpdateEvent event);

    /**
     * Saves a new job instance update event.
     *
     * @param key Update identifier.
     * @param event Job instance update event.
     */
    void saveJobInstanceUpdateEvent(JobUpdateKey key, JobInstanceUpdateEvent event);

    /**
     * Deletes all updates and update events from the store.
     */
    void deleteAllUpdatesAndEvents();

    /**
     * Prunes (deletes) old completed updates and events from the store.
     * <p>
     * At least {@code perJobRetainCount} last completed updates that completed less than
     * {@code historyPruneThreshold} ago will be kept for every job.
     *
     * @param perJobRetainCount Number of completed updates to retain per job.
     * @param historyPruneThresholdMs Earliest timestamp in the past to retain history.
     *                                Any completed updates created before this timestamp
     *                                will be pruned.
     * @return Set of pruned update keys.
     */
    Set<JobUpdateKey> pruneHistory(int perJobRetainCount, long historyPruneThresholdMs);
  }
}
