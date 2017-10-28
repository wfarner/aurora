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
package org.apache.aurora.scheduler.thrift;

import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;

import org.apache.aurora.gen.AssignedTask;
import org.apache.aurora.gen.Container;
import org.apache.aurora.gen.ExecutorConfig;
import org.apache.aurora.gen.Identity;
import org.apache.aurora.gen.InstanceKey;
import org.apache.aurora.gen.JobConfiguration;
import org.apache.aurora.gen.JobKey;
import org.apache.aurora.gen.JobSummary;
import org.apache.aurora.gen.JobSummaryResult;
import org.apache.aurora.gen.JobUpdateKey;
import org.apache.aurora.gen.LockKey;
import org.apache.aurora.gen.MesosContainer;
import org.apache.aurora.gen.Resource;
import org.apache.aurora.gen.Response;
import org.apache.aurora.gen.ResponseCode;
import org.apache.aurora.gen.ResponseDetail;
import org.apache.aurora.gen.Result;
import org.apache.aurora.gen.ScheduledTask;
import org.apache.aurora.gen.TaskConfig;
import org.apache.aurora.scheduler.base.JobKeys;
import org.apache.aurora.scheduler.base.TaskTestUtil;
import org.apache.aurora.scheduler.quota.QuotaCheckResult;
import org.apache.aurora.scheduler.resources.ResourceBag;
import org.apache.aurora.scheduler.resources.ResourceTestUtil;

import static org.apache.aurora.gen.Api_Constants.AURORA_EXECUTOR_NAME;
import static org.apache.aurora.gen.ResponseCode.OK;
import static org.apache.aurora.scheduler.quota.QuotaCheckResult.Result.INSUFFICIENT_QUOTA;
import static org.apache.aurora.scheduler.quota.QuotaCheckResult.Result.SUFFICIENT_QUOTA;
import static org.junit.Assert.assertEquals;

final class Fixtures {
  static final String ROLE = "bar_role";
  static final String USER = "foo_user";
  static final Identity IDENTITY = Identity.builder().setUser(USER).build();
  static final String JOB_NAME = "job_foo";
  static final JobKey JOB_KEY = JobKeys.from(ROLE, "devel", JOB_NAME);
  static final LockKey LOCK_KEY = LockKey.withJob(JOB_KEY);
  static final JobConfiguration CRON_JOB = makeJob().mutate().setCronSchedule("* * * * *").build();
  static final String TASK_ID = "task_id";
  static final String UPDATE_ID = "82d6d790-3212-11e3-aa6e-0800200c9a74";
  static final JobUpdateKey UPDATE_KEY =
      JobUpdateKey.builder().setJob(JOB_KEY).setId(UPDATE_ID).build();
  static final UUID UU_ID = UUID.fromString(UPDATE_ID);
  private static final Function<String, ResponseDetail> MESSAGE_TO_DETAIL =
      message -> ResponseDetail.builder().setMessage(message).build();
  static final String CRON_SCHEDULE = "0 * * * *";
  static final ResourceBag QUOTA = ResourceTestUtil.bag(10.0, 1024, 2048);
  static final QuotaCheckResult ENOUGH_QUOTA = new QuotaCheckResult(SUFFICIENT_QUOTA);
  static final QuotaCheckResult NOT_ENOUGH_QUOTA = new QuotaCheckResult(INSUFFICIENT_QUOTA);
  static final InstanceKey INSTANCE_KEY =
      InstanceKey.builder().setJobKey(JOB_KEY).setInstanceId(0).build();
  static final TaskConfig INVALID_TASK_CONFIG = defaultTask(true).mutate().setTier(",").build();

  private Fixtures() {
    // Utility class.
  }

  static JobConfiguration makeJob() {
    return makeJob(nonProductionTask(), 1);
  }

  static JobConfiguration makeJob(TaskConfig task, int shardCount) {
    return JobConfiguration.builder()
        .setOwner(IDENTITY)
        .setInstanceCount(shardCount)
        .setTaskConfig(task)
        .setKey(JOB_KEY)
        .build();
  }

  static TaskConfig defaultTask(boolean production) {
    return TaskConfig.builder()
        .setJob(JOB_KEY)
        .setOwner(IDENTITY)
        .setContactEmail("testing@twitter.com")
        .setExecutorConfig(ExecutorConfig.builder().setName(AURORA_EXECUTOR_NAME).setData("data"))
        .setNumCpus(1)
        .setRamMb(1024)
        .setDiskMb(1024)
        .setProduction(production)
        .setTier(production ? TaskTestUtil.PROD_TIER_NAME : TaskTestUtil.DEV_TIER_NAME)
        .setRequestedPorts(ImmutableSet.of())
        .setTaskLinks(ImmutableMap.of())
        .setMaxTaskFailures(1)
        .setConstraints(ImmutableSet.of())
        .setMetadata(ImmutableSet.of())
        .setMesosFetcherUris(ImmutableSet.of())
        .setContainer(Container.withMesos(MesosContainer.builder().setVolumes(ImmutableList.of())))
        .setResources(ImmutableSet.of(
            Resource.withNumCpus(1),
            Resource.withRamMb(1024),
            Resource.withDiskMb(1024)))
        .build();
  }

  static TaskConfig nonProductionTask() {
    return defaultTask(false);
  }

  static Response jobSummaryResponse(Set<JobSummary> jobSummaries) {
    return okResponse(Result.withJobSummaryResult(
        JobSummaryResult.builder().setSummaries(jobSummaries)));
  }

  static Response response(ResponseCode code, Optional<Result> result, String... messages) {
    Response._Builder response = Responses.empty()
        .setResponseCode(code)
        .setResult(result.orNull());
    if (messages.length > 0) {
      response.setDetails(FluentIterable.from(Arrays.asList(messages)).transform(MESSAGE_TO_DETAIL)
          .toList());
    }

    return response.build();
  }

  static Response okResponse(Result result) {
    return response(OK, Optional.of(result));
  }

  static JobConfiguration makeProdJob() {
    return makeJob(productionTask(), 1);
  }

  static TaskConfig productionTask() {
    return defaultTask(true);
  }

  static JobConfiguration makeJob(TaskConfig._Builder task) {
    return makeJob(task.build());
  }

  static JobConfiguration makeJob(TaskConfig task) {
    return makeJob(task, 1);
  }

  static Iterable<ScheduledTask> makeDefaultScheduledTasks(int n) {
    return makeDefaultScheduledTasks(n, defaultTask(true));
  }

  static Iterable<ScheduledTask> makeDefaultScheduledTasks(int n, TaskConfig config) {
    List<ScheduledTask> tasks = Lists.newArrayList();
    for (int i = 0; i < n; i++) {
      tasks.add(ScheduledTask.builder()
          .setAssignedTask(AssignedTask.builder().setTask(config).setInstanceId(i))
          .build());
    }

    return tasks;
  }

  static Response assertOkResponse(Response response) {
    return assertResponse(OK, response);
  }

  static Response assertResponse(ResponseCode expected, Response response) {
    assertEquals(expected, response.getResponseCode());
    return response;
  }
}
