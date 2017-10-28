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
package org.apache.aurora.scheduler.storage.log;

import java.util.EnumSet;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.inject.Inject;

import org.apache.aurora.GuavaUtils;
import org.apache.aurora.gen.JobConfiguration;
import org.apache.aurora.gen.JobUpdate;
import org.apache.aurora.gen.JobUpdateInstructions;
import org.apache.aurora.gen.Resource;
import org.apache.aurora.gen.ResourceAggregate;
import org.apache.aurora.gen.ScheduledTask;
import org.apache.aurora.gen.TaskConfig;
import org.apache.aurora.scheduler.TierInfo;
import org.apache.aurora.scheduler.TierManager;
import org.apache.aurora.scheduler.quota.QuotaManager;
import org.apache.aurora.scheduler.resources.ResourceType;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

import static org.apache.aurora.scheduler.resources.ResourceType.CPUS;
import static org.apache.aurora.scheduler.resources.ResourceType.DISK_MB;
import static org.apache.aurora.scheduler.resources.ResourceType.RAM_MB;

/**
 * Helps migrating thrift schema by populating deprecated and/or replacement fields.
 */
public final class ThriftBackfill {

  private final TierManager tierManager;

  @Inject
  public ThriftBackfill(TierManager tierManager) {
    this.tierManager = requireNonNull(tierManager);
  }

  private static Resource getResource(Set<Resource> resources, ResourceType type) {
    return resources.stream()
            .filter(e -> ResourceType.fromResource(e).equals(type))
            .findFirst()
            .orElseThrow(() ->
                    new IllegalArgumentException("Missing resource definition for " + type));
  }

  /**
   * Ensures TaskConfig.resources and correspondent task-level fields are all populated.
   *
   * @param input TaskConfig to backfill.
   * @return Backfilled TaskConfig.
   */
  public TaskConfig backfillTask(TaskConfig input) {
    TaskConfig._Builder builder = input.mutate();
    if (input.hasTier()) {
      TierInfo tier = tierManager.getTier(input);
      builder.setProduction(!tier.isPreemptible() && !tier.isRevocable());
    } else {
      builder.setTier(tierManager.getTiers()
          .entrySet()
          .stream()
          .filter(e -> e.getValue().isPreemptible() == !input.isProduction()
              && !e.getValue().isRevocable())
          .findFirst()
          .orElseThrow(() -> new IllegalStateException(
              format("No matching implicit tier for task of job %s", input.getJob())))
          .getKey());
    }

    return builder.build();
  }

  /**
   * Backfills JobConfiguration. See {@link #backfillTask(TaskConfig)}.
   *
   * @param jobConfig JobConfiguration to backfill.
   * @return Backfilled JobConfiguration.
   */
  JobConfiguration backfillJobConfiguration(JobConfiguration jobConfig) {
    return jobConfig.mutate()
        .setTaskConfig(backfillTask(jobConfig.getTaskConfig()))
        .build();
  }

  /**
   * Backfills set of tasks. See {@link #backfillTask(TaskConfig)}.
   *
   * @param tasks Set of tasks to backfill.
   * @return Backfilled set of tasks.
   */
  Set<ScheduledTask> backfillTasks(Set<ScheduledTask> tasks) {
    return tasks.stream()
        .map(this::backfillScheduledTask)
        .collect(GuavaUtils.toImmutableSet());
  }

  /**
   * Ensures ResourceAggregate.resources and correspondent deprecated fields are all populated.
   *
   * @param input ResourceAggregate to backfill.
   * @return Backfilled ResourceAggregate.
   */
  public static ResourceAggregate backfillResourceAggregate(ResourceAggregate input) {
    ResourceAggregate._Builder aggregate = input.mutate();
    if (!input.hasResources() || input.getResources().isEmpty()) {
      aggregate.addToResources(Resource.withNumCpus(aggregate.getNumCpus()));
      aggregate.addToResources(Resource.withRamMb(aggregate.getRamMb()));
      aggregate.addToResources(Resource.withDiskMb(aggregate.getDiskMb()));
    } else {
      EnumSet<ResourceType> quotaResources = QuotaManager.QUOTA_RESOURCE_TYPES;
      if (input.getResources().size() > quotaResources.size()) {
        throw new IllegalArgumentException("Too many resource values in quota.");
      }

      if (!quotaResources.equals(input.getResources().stream()
              .map(e -> ResourceType.fromResource(e))
              .collect(Collectors.toSet()))) {

        throw new IllegalArgumentException("Quota resources must be exactly: " + quotaResources);
      }
      aggregate.setNumCpus(getResource(input.getResources(), CPUS).getNumCpus());
      aggregate.setRamMb(getResource(input.getResources(), RAM_MB).getRamMb());
      aggregate.setDiskMb(getResource(input.getResources(), DISK_MB).getDiskMb());
    }
    return aggregate.build();
  }

  private ScheduledTask backfillScheduledTask(ScheduledTask task) {
    backfillTask(task.getAssignedTask().getTask());
    return task;
  }

  /**
   * Backfills JobUpdate. See {@link #backfillTask(TaskConfig)}.
   *
   * @param input JobUpdate to backfill.
   * @return Backfilled job update.
   */
  JobUpdate backFillJobUpdate(JobUpdate input) {
    JobUpdate._Builder update = input.mutate();
    JobUpdateInstructions instructions = update.getInstructions();
    if (instructions.hasDesiredState()) {
      update.mutableInstructions().mutableDesiredState()
          .setTask(instructions.getDesiredState().getTask());
    }

    update.mutableInstructions().setInitialState(
        instructions.getInitialState().stream()
            .map(e -> e.mutate().setTask(backfillTask(e.getTask())).build())
            .collect(Collectors.toList()));
    return update.build();
  }
}
