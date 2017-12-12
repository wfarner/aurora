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
package org.apache.aurora.scheduler.storage.sql;

import java.util.List;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nullable;

import com.google.common.collect.ImmutableSet;
import com.google.inject.Guice;

import org.apache.aurora.gen.AssignedTask;
import org.apache.aurora.gen.HostAttributes;
import org.apache.aurora.gen.JobConfiguration;
import org.apache.aurora.gen.JobInstanceUpdateEvent;
import org.apache.aurora.gen.JobKey;
import org.apache.aurora.gen.JobUpdate;
import org.apache.aurora.gen.JobUpdateAction;
import org.apache.aurora.gen.JobUpdateEvent;
import org.apache.aurora.gen.JobUpdateKey;
import org.apache.aurora.gen.JobUpdateStatus;
import org.apache.aurora.gen.JobUpdateSummary;
import org.apache.aurora.gen.MaintenanceMode;
import org.apache.aurora.gen.ResourceAggregate;
import org.apache.aurora.gen.ScheduleStatus;
import org.apache.aurora.gen.ScheduledTask;
import org.apache.aurora.gen.storage.Op;
import org.apache.aurora.gen.storage.RemoveJob;
import org.apache.aurora.gen.storage.RemoveJobUpdates;
import org.apache.aurora.gen.storage.RemoveQuota;
import org.apache.aurora.gen.storage.RemoveTasks;
import org.apache.aurora.gen.storage.SaveCronJob;
import org.apache.aurora.gen.storage.SaveFrameworkId;
import org.apache.aurora.gen.storage.SaveHostAttributes;
import org.apache.aurora.gen.storage.SaveJobInstanceUpdateEvent;
import org.apache.aurora.gen.storage.SaveJobUpdate;
import org.apache.aurora.gen.storage.SaveJobUpdateEvent;
import org.apache.aurora.gen.storage.SaveQuota;
import org.apache.aurora.gen.storage.SaveTasks;
import org.apache.aurora.scheduler.storage.durability.Persistence;
import org.apache.aurora.scheduler.storage.durability.Persistence.Edit;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.apache.aurora.gen.Resource.diskMb;
import static org.apache.aurora.gen.Resource.numCpus;
import static org.apache.aurora.gen.Resource.ramMb;
import static org.junit.Assert.assertEquals;

public class SqlPersistenceTest {

  private static final JobKey JOB_KEY =
      new JobKey().setRole("role").setEnvironment("env").setName("name");

  private static final JobUpdateKey UPDATE_KEY =
      new JobUpdateKey().setJob(JOB_KEY).setId("update-id");

  private Persistence persistence;

  @Before
  public void setUp() {
    persistence = Guice.createInjector(SqlPersistenceModule.inMemory())
        .getInstance(Persistence.class);
  }

  @After
  public void tearDown() {
    persistence.close();
  }

  @Test
  public void testEmpty() {
    assertEquals(ImmutableSet.of(), closingRecover(Collectors.toSet()));
  }

  @Test
  public void testCrud() {
    Stream<CrudSequence> cases = Stream.of(
        new CrudSequence(
            Op.saveCronJob(new SaveCronJob().setJobConfig(new JobConfiguration().setKey(JOB_KEY))),
            op -> op.getSaveCronJob().getJobConfig().setCronSchedule("* * * * *"),
            Op.removeJob(new RemoveJob().setJobKey(JOB_KEY))),

        new CrudSequence(
            Op.saveHostAttributes(new SaveHostAttributes().setHostAttributes(
                new HostAttributes().setSlaveId("slave-id"))),
            op -> op.getSaveHostAttributes().getHostAttributes().setMode(MaintenanceMode.DRAINING)),

        new CrudSequence(
            Op.saveFrameworkId(new SaveFrameworkId().setId("framework")),
            op -> op.getSaveFrameworkId().setId("framework-new")),

        new CrudSequence(
            Op.saveQuota(new SaveQuota().setRole("role")),
            op -> op.getSaveQuota().setQuota(new ResourceAggregate().setResources(
                ImmutableSet.of(numCpus(1), ramMb(1024), diskMb(1024)))),
            Op.removeQuota(new RemoveQuota().setRole("role"))),

        new CrudSequence(
            Op.saveJobUpdate(new SaveJobUpdate().setJobUpdate(new JobUpdate().setSummary(
                new JobUpdateSummary().setKey(UPDATE_KEY)))),
            op -> op.getSaveJobUpdate().getJobUpdate().getSummary().setUser("alice"),
            Op.removeJobUpdate(new RemoveJobUpdates().setKeys(ImmutableSet.of(UPDATE_KEY)))),

        new CrudSequence(
            saveTasks("a"),
            op -> {
              ScheduledTask task = op.getSaveTasks().getTasksIterator().next();
              task.setStatus(ScheduleStatus.KILLING);
              op.getSaveTasks().setTasks(ImmutableSet.of(task));
            },
            Op.removeTasks(new RemoveTasks().setTaskIds(ImmutableSet.of("a"))))
    );

    cases.forEach(crud -> {
      assertNoRecords();

      persistAndCheck(Stream.of(crud.record), ImmutableSet.of(crud.record));
      Op copy = crud.record.deepCopy();
      crud.mutation.accept(copy);
      persistAndCheck(Stream.of(copy), ImmutableSet.of(copy));

      if (crud.delete != null) {
        persistAndCheck(Stream.of(crud.delete), ImmutableSet.of());
      }

      tearDown();
      setUp();
    });
  }

  /**
   * Performs a dummy read of the persistence, expecting that it is empty.
   */
  private void assertNoRecords() {
    assertEquals(ImmutableSet.of(), closingRecover(Collectors.toSet()));
  }

  private static class CrudSequence {
    final Op record;
    final Consumer<Op> mutation;
    final @Nullable Op delete;

    CrudSequence(Op record, Consumer<Op> mutation, Op delete) {
      this.record = record;
      this.mutation = mutation;
      this.delete = delete;
    }

    CrudSequence(Op record, Consumer<Op> mutation) {
      this(record, mutation, null);
    }
  }

  @Test
  public void testPersistTasks() {
    assertNoRecords();

    // Create
    Op create = saveTasks("a", "b", "c");
    persistAndCheck(
        Stream.of(create),
        ImmutableSet.of(saveTasks("a"), saveTasks("b"), saveTasks("c")));

    // Update
    ScheduledTask updated = new ScheduledTask()
        .setStatus(ScheduleStatus.RUNNING)
        .setFailureCount(5)
        .setAssignedTask(new AssignedTask().setTaskId("b"));
    Op update = Op.saveTasks(new SaveTasks().setTasks(ImmutableSet.of(updated)));
    persistAndCheck(Stream.of(update), ImmutableSet.of(saveTasks("a"), update, saveTasks("c")));

    // Delete
    persistAndCheck(
        Stream.of(Op.removeTasks(new RemoveTasks().setTaskIds(ImmutableSet.of("c")))),
        ImmutableSet.of(saveTasks("a"), update));
  }

  @Test
  public void testJobUpdateRelations() {
    assertNoRecords();

    // Create
    Op create = Op.saveJobUpdate(new SaveJobUpdate()
        .setJobUpdate(new JobUpdate()
            .setSummary(new JobUpdateSummary()
            .setKey(UPDATE_KEY))));
    persistAndCheck(Stream.of(create), ImmutableSet.of(create));

    // Add events
    Op jobEvent = Op.saveJobUpdateEvent(new SaveJobUpdateEvent()
        .setKey(UPDATE_KEY)
        .setEvent(new JobUpdateEvent()
            .setMessage("message")
            .setStatus(JobUpdateStatus.ROLLING_FORWARD)));
    Op instanceEvent = Op.saveJobInstanceUpdateEvent(new SaveJobInstanceUpdateEvent()
        .setKey(UPDATE_KEY)
        .setEvent(new JobInstanceUpdateEvent()
            .setAction(JobUpdateAction.INSTANCE_UPDATING)));
    persistence.persist(Stream.of(jobEvent, instanceEvent));
    // Records with relations are guaranteed to be replayed _after_ records without.
    List<Op> recovered = closingRecover(Collectors.toList());
    assertEquals(create, recovered.get(0));
    assertEquals(
        ImmutableSet.of(jobEvent, instanceEvent),
        ImmutableSet.copyOf(recovered.subList(1, recovered.size())));

    // Mutate the update to ensure the relations are preserved.
    Op mutate = create.deepCopy();
    mutate.getSaveJobUpdate().getJobUpdate().getSummary().setUser("alice");
    persistence.persist(Stream.of(mutate));
    recovered = closingRecover(Collectors.toList());
    assertEquals(mutate, recovered.get(0));
    assertEquals(
        ImmutableSet.of(jobEvent, instanceEvent),
        ImmutableSet.copyOf(recovered.subList(1, recovered.size())));

    // Delete
    persistAndCheck(
        Stream.of(Op.removeJobUpdate(new RemoveJobUpdates().setKeys(ImmutableSet.of(UPDATE_KEY)))),
        ImmutableSet.of());

    assertNoRecords();
  }

  private void persistAndCheck(Stream<Op> persist, Set<Op> expectContents) {
    persistence.persist(persist);
    assertEquals(expectContents, closingRecover(Collectors.toSet()));
  }

  private <R> R closingRecover(Collector<Op, ?, R> collector) {
    try (Stream<Op> recovered = persistence.recover().map(Edit::getOp)) {
      return recovered.collect(collector);
    }
  }

  private static Op saveTasks(String... ids) {
    return Op.saveTasks(new SaveTasks().setTasks(
        Stream.of(ids)
            .map(id -> new ScheduledTask().setAssignedTask(new AssignedTask().setTaskId(id)))
            .collect(Collectors.toSet())));
  }
}
