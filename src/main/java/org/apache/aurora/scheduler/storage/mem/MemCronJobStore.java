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
package org.apache.aurora.scheduler.storage.mem;

import java.util.Map;

import javax.inject.Inject;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;

import org.apache.aurora.common.inject.TimedInterceptor.Timed;
import org.apache.aurora.common.stats.StatsProvider;
import org.apache.aurora.scheduler.base.JobKeys;
import org.apache.aurora.scheduler.storage.CronJobStore;
import org.apache.aurora.gen.JobConfiguration;
import org.apache.aurora.gen.JobKey;

/**
 * An in-memory cron job store.
 */
class MemCronJobStore implements CronJobStore.Mutable {
  @VisibleForTesting
  static final String CRON_JOBS_SIZE = "mem_storage_cron_size";

  private final Map<JobKey, JobConfiguration> jobs = Maps.newConcurrentMap();

  @Inject
  MemCronJobStore(StatsProvider statsProvider) {
    statsProvider.makeGauge(CRON_JOBS_SIZE, () -> jobs.size());
  }

  @Timed("mem_storage_cron_save_accepted_job")
  @Override
  public void saveAcceptedJob(JobConfiguration jobConfig) {
    JobKey key = JobKeys.assertValid(jobConfig.getKey());
    jobs.put(key, jobConfig);
  }

  @Timed("mem_storage_cron_remove_job")
  @Override
  public void removeJob(JobKey jobKey) {
    jobs.remove(jobKey);
  }

  @Timed("mem_storage_cron_delete_jobs")
  @Override
  public void deleteJobs() {
    jobs.clear();
  }

  @Timed("mem_storage_cron_fetch_jobs")
  @Override
  public Iterable<JobConfiguration> fetchJobs() {
    return ImmutableSet.copyOf(jobs.values());
  }

  @Timed("mem_storage_cron_fetch_job")
  @Override
  public Optional<JobConfiguration> fetchJob(JobKey jobKey) {
    return Optional.fromNullable(jobs.get(jobKey));
  }
}
