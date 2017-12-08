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
import java.util.Set;

import javax.annotation.Nullable;

import org.apache.aurora.scheduler.events.PubsubEvent.EventSubscriber;
import org.apache.aurora.scheduler.storage.Storage.MutableStoreProvider;
import org.apache.aurora.scheduler.storage.Storage.StoreProvider;
import org.apache.mesos.v1.Protos.OfferID;

/**
 * Enables scheduling and preemption of tasks.
 */
public interface TaskScheduler extends EventSubscriber {

  /**
   * Attempts to schedule a task, possibly performing irreversible actions.
   *
   * @param storeProvider data store.
   * @param taskIds The tasks to attempt to schedule.
   * @return Successfully scheduled task IDs. The caller should call schedule again if a given
   *         task ID was not present in the result.
   */
  Map<String, MatchResult> findMatches(StoreProvider storeProvider, Set<String> taskIds);

  Set<String> schedule(MutableStoreProvider storeProvider, Map<String, MatchResult> matches);

  // TODO(wfarner): Capture a return value with the several states that a task may be in:
  //   - exists, and match found
  //   - exists and no match found
  //   - does not exist

  class MatchResult {
    private boolean valid;
    private final @Nullable OfferID offer;

    private MatchResult(boolean valid, @Nullable OfferID offer) {
      this.valid = valid;
      this.offer = offer;
    }

    public static MatchResult invalidTask() {
      return new MatchResult(false, null);
    }

    public static MatchResult matched(OfferID offer) {
      return new MatchResult(true, offer);
    }

    public static MatchResult noMatch() {
      return new MatchResult(true, null);
    }

    public boolean isValid() {
      return valid;
    }

    public boolean hasOffer() {
      return offer != null;
    }
  }
}
