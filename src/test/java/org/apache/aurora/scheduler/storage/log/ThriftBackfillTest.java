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

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;

import org.apache.aurora.gen.AssignedTask;
import org.apache.aurora.gen.Identity;
import org.apache.aurora.gen.InstanceTaskConfig;
import org.apache.aurora.gen.JobConfiguration;
import org.apache.aurora.gen.JobUpdate;
import org.apache.aurora.gen.JobUpdateInstructions;
import org.apache.aurora.gen.ScheduledTask;
import org.apache.aurora.gen.TaskConfig;
import org.apache.aurora.scheduler.base.JobKeys;
import org.apache.aurora.scheduler.storage.entities.IJobConfiguration;
import org.apache.aurora.scheduler.storage.entities.IJobKey;
import org.apache.aurora.scheduler.storage.entities.IJobUpdate;
import org.apache.aurora.scheduler.storage.entities.IScheduledTask;
import org.apache.aurora.scheduler.storage.entities.ITaskConfig;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class ThriftBackfillTest {
  private static final IJobKey JOB_KEY = JobKeys.from("role", "env", "name");
  private static final ITaskConfig TASK = ITaskConfig.build(new TaskConfig()
      .setJob(JOB_KEY.newBuilder())
      .setOwner(new Identity(null, "user")));

  @Test
  public void testUpdateBackfill() {
    JobUpdate update = update();
    JobUpdate expected = update();
    populateTask(expected.getInstructions().getDesiredState().getTask());
    populateTask(Iterables.getOnlyElement(expected.getInstructions().getInitialState()).getTask());
    assertEquals(IJobUpdate.build(expected), ThriftBackfill.backFillJobUpdate(update));
  }

  @Test
  public void testTaskBackfill() {
    ScheduledTask task =
        new ScheduledTask().setAssignedTask(new AssignedTask().setTask(TASK.newBuilder()));
    ScheduledTask expected = new ScheduledTask(task);
    expected.getAssignedTask().setTask(populateTask(TASK.newBuilder()));

    assertEquals(
        ImmutableSet.of(IScheduledTask.build(expected)),
        ThriftBackfill.backFillScheduledTasks(ImmutableSet.of(task)));
  }

  @Test
  public void testJobConfigurationBackfill() {
    JobConfiguration configuration = new JobConfiguration()
        .setKey(JOB_KEY.newBuilder())
        .setTaskConfig(TASK.newBuilder())
        .setOwner(new Identity().setUser("user"));
    JobConfiguration expected = new JobConfiguration(configuration);
    expected.getOwner().setRole(JOB_KEY.getRole());
    expected.setTaskConfig(populateTask(TASK.newBuilder()));

    assertEquals(
        IJobConfiguration.build(expected),
        ThriftBackfill.backFillJobConfiguration(configuration));
  }

  @Test
  public void testUpdateBackfillNoDesiredState() {
    JobUpdate update = update();
    update.getInstructions().setDesiredState(null);
    JobUpdate expected = update();
    expected.getInstructions().setDesiredState(null);
    populateTask(Iterables.getOnlyElement(expected.getInstructions().getInitialState()).getTask());
    assertEquals(IJobUpdate.build(expected), ThriftBackfill.backFillJobUpdate(update));
  }

  private static JobUpdate update() {
    return new JobUpdate().setInstructions(new JobUpdateInstructions()
        .setDesiredState(new InstanceTaskConfig().setTask(TASK.newBuilder()))
        .setInitialState(ImmutableSet.of(new InstanceTaskConfig().setTask(TASK.newBuilder()))));
  }

  private static TaskConfig populateTask(TaskConfig task) {
    task.setJobName("name").setEnvironment("env").getOwner().setRole("role");
    return task;
  }
}