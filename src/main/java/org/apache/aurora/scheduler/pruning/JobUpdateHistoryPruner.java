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
package org.apache.aurora.scheduler.pruning;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.inject.Inject;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import com.google.common.collect.Ordering;
import com.google.common.util.concurrent.AbstractIdleService;

import org.apache.aurora.common.quantity.Amount;
import org.apache.aurora.common.quantity.Time;
import org.apache.aurora.common.stats.StatsProvider;
import org.apache.aurora.common.util.Clock;
import org.apache.aurora.gen.JobUpdateQuery;
import org.apache.aurora.scheduler.storage.Storage;
import org.apache.aurora.scheduler.storage.Storage.MutateWork.NoResult;
import org.apache.aurora.scheduler.storage.entities.IJobKey;
import org.apache.aurora.scheduler.storage.entities.IJobUpdateDetails;
import org.apache.aurora.scheduler.storage.entities.IJobUpdateKey;
import org.apache.aurora.scheduler.storage.entities.IJobUpdateQuery;
import org.apache.aurora.scheduler.storage.entities.IJobUpdateSummary;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.Objects.requireNonNull;

import static org.apache.aurora.scheduler.storage.JobUpdateStore.TERMINAL_STATES;

/**
 * Prunes per-job update history on a periodic basis.
 */
class JobUpdateHistoryPruner extends AbstractIdleService {
  private static final Logger LOG = LoggerFactory.getLogger(JobUpdateHistoryPruner.class);
  @VisibleForTesting
  static final String JOB_UPDATES_PRUNED = "job_updates_pruned";

  private final Clock clock;
  private final ScheduledExecutorService executor;
  private final Storage storage;
  private final HistoryPrunerSettings settings;
  private final AtomicLong prunedUpdatesCount;

  static class HistoryPrunerSettings {
    private final Amount<Long, Time> pruneInterval;
    private final Amount<Long, Time> maxHistorySize;
    private final int maxUpdatesPerJob;

    HistoryPrunerSettings(
        Amount<Long, Time> pruneInterval,
        Amount<Long, Time> maxHistorySize,
        int maxUpdatesPerJob) {

      this.pruneInterval = requireNonNull(pruneInterval);
      this.maxHistorySize = requireNonNull(maxHistorySize);
      this.maxUpdatesPerJob = maxUpdatesPerJob;
    }
  }

  @Inject
  JobUpdateHistoryPruner(
      Clock clock,
      ScheduledExecutorService executor,
      Storage storage,
      HistoryPrunerSettings settings,
      StatsProvider statsProvider) {

    this.clock = requireNonNull(clock);
    this.executor = requireNonNull(executor);
    this.storage = requireNonNull(storage);
    this.settings = requireNonNull(settings);
    this.prunedUpdatesCount = statsProvider.makeCounter(JOB_UPDATES_PRUNED);
  }

  @Override
  protected void startUp() {
    executor.scheduleAtFixedRate(
        () -> storage.write((NoResult.Quiet) storeProvider -> {

          List<IJobUpdateSummary> completedUpdates = storeProvider.getJobUpdateStore()
              .fetchJobUpdateDetails(IJobUpdateQuery.build(
                  new JobUpdateQuery().setUpdateStatuses(TERMINAL_STATES)))
              .stream()
              .map(u -> u.getUpdate().getSummary())
              .collect(Collectors.toList());

          Predicate<IJobUpdateSummary> expiredFilter =
              s -> s.getState().getCreatedTimestampMs() < historyPruneThresholdMs;

          ImmutableSet.Builder<IJobUpdateKey> pruneBuilder = ImmutableSet.builder();

          // Gather updates based on time threshold.
          pruneBuilder.addAll(completedUpdates
              .stream()
              .filter(expiredFilter)
              .map(IJobUpdateSummary::getKey)
              .collect(Collectors.toList()));

          Multimap<IJobKey, IJobUpdateSummary> updatesByJob = Multimaps.index(
              // Avoid counting to-be-removed expired updates.
              completedUpdates.stream().filter(expiredFilter.negate()).iterator(),
              s -> s.getKey().getJob());

          for (Map.Entry<IJobKey, Collection<IJobUpdateSummary>> entry
              : updatesByJob.asMap().entrySet()) {

            if (entry.getValue().size() > perJobRetainCount) {
              Ordering<IJobUpdateSummary> creationOrder = Ordering.natural()
                  .onResultOf(s -> s.getState().getCreatedTimestampMs());
              pruneBuilder.addAll(creationOrder
                  .leastOf(entry.getValue(), entry.getValue().size() - perJobRetainCount)
                  .stream()
                  .map(IJobUpdateSummary::getKey)
                  .iterator());
            }
          }

          Set<IJobUpdateKey> pruned = pruneBuilder.build();
          updates.keySet().removeAll(pruned);



          Set<IJobUpdateKey> prunedUpdates = storeProvider.getJobUpdateStore().pruneHistory(
              settings.maxUpdatesPerJob,
              clock.nowMillis() - settings.maxHistorySize.as(Time.MILLISECONDS));

          prunedUpdatesCount.addAndGet(prunedUpdates.size());
          LOG.info(prunedUpdates.isEmpty()
              ? "No job update history to prune."
              : "Pruned job update history: " + Joiner.on(",").join(prunedUpdates));
        }),
        settings.pruneInterval.as(Time.MILLISECONDS),
        settings.pruneInterval.as(Time.MILLISECONDS),
        TimeUnit.MILLISECONDS);
  }

  @Override
  protected void shutDown() {
    // Nothing to do - await VM shutdown.
  }
}
