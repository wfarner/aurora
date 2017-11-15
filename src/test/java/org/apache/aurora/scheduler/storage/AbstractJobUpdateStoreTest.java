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

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Injector;

import org.apache.aurora.gen.InstanceTaskConfig;
import org.apache.aurora.gen.JobInstanceUpdateEvent;
import org.apache.aurora.gen.JobUpdate;
import org.apache.aurora.gen.JobUpdateAction;
import org.apache.aurora.gen.JobUpdateDetails;
import org.apache.aurora.gen.JobUpdateEvent;
import org.apache.aurora.gen.JobUpdateInstructions;
import org.apache.aurora.gen.JobUpdateKey;
import org.apache.aurora.gen.JobUpdateQuery;
import org.apache.aurora.gen.JobUpdateSettings;
import org.apache.aurora.gen.JobUpdateState;
import org.apache.aurora.gen.JobUpdateStatus;
import org.apache.aurora.gen.JobUpdateSummary;
import org.apache.aurora.gen.Metadata;
import org.apache.aurora.gen.Range;
import org.apache.aurora.gen.TaskConfig;
import org.apache.aurora.scheduler.base.JobKeys;
import org.apache.aurora.scheduler.base.TaskTestUtil;
import org.apache.aurora.scheduler.storage.Storage.MutateWork.NoResult;
import org.apache.aurora.scheduler.storage.Storage.StorageException;
import org.apache.aurora.scheduler.storage.entities.IJobInstanceUpdateEvent;
import org.apache.aurora.scheduler.storage.entities.IJobKey;
import org.apache.aurora.scheduler.storage.entities.IJobUpdate;
import org.apache.aurora.scheduler.storage.entities.IJobUpdateDetails;
import org.apache.aurora.scheduler.storage.entities.IJobUpdateEvent;
import org.apache.aurora.scheduler.storage.entities.IJobUpdateInstructions;
import org.apache.aurora.scheduler.storage.entities.IJobUpdateKey;
import org.apache.aurora.scheduler.storage.entities.IJobUpdateQuery;
import org.apache.aurora.scheduler.storage.entities.IJobUpdateState;
import org.apache.aurora.scheduler.storage.entities.IJobUpdateSummary;
import org.apache.aurora.scheduler.storage.testing.StorageEntityUtil;
import org.apache.aurora.scheduler.testing.FakeStatsProvider;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.apache.aurora.gen.JobUpdateAction.INSTANCE_ROLLBACK_FAILED;
import static org.apache.aurora.gen.JobUpdateAction.INSTANCE_ROLLED_BACK;
import static org.apache.aurora.gen.JobUpdateAction.INSTANCE_ROLLING_BACK;
import static org.apache.aurora.gen.JobUpdateAction.INSTANCE_UPDATED;
import static org.apache.aurora.gen.JobUpdateAction.INSTANCE_UPDATING;
import static org.apache.aurora.gen.JobUpdateStatus.ABORTED;
import static org.apache.aurora.gen.JobUpdateStatus.ERROR;
import static org.apache.aurora.gen.JobUpdateStatus.ROLLED_BACK;
import static org.apache.aurora.gen.JobUpdateStatus.ROLLING_BACK;
import static org.apache.aurora.gen.JobUpdateStatus.ROLLING_FORWARD;
import static org.apache.aurora.gen.JobUpdateStatus.ROLL_BACK_PAUSED;
import static org.apache.aurora.gen.JobUpdateStatus.ROLL_FORWARD_PAUSED;
import static org.apache.aurora.gen.Resource.diskMb;
import static org.apache.aurora.gen.Resource.numCpus;
import static org.apache.aurora.gen.Resource.ramMb;
import static org.apache.aurora.scheduler.storage.Util.jobUpdateActionStatName;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public abstract class AbstractJobUpdateStoreTest {

  private static final IJobKey JOB = JobKeys.from("testRole", "testEnv", "job");
  private static final IJobUpdateKey UPDATE1 =
      IJobUpdateKey.build(new JobUpdateKey(JOB.newBuilder(), "update1"));
  private static final long CREATED_MS = 111L;
  private static final IJobUpdateEvent FIRST_EVENT =
      makeJobUpdateEvent(ROLLING_FORWARD, CREATED_MS);
  private static final ImmutableSet<Metadata> METADATA =
      ImmutableSet.of(new Metadata("k1", "v1"), new Metadata("k2", "v2"), new Metadata("k3", "v3"));

  private Storage storage;
  private FakeStatsProvider stats;

  @Before
  public void setUp() throws Exception {
    Injector injector = createStorageInjector();
    storage = injector.getInstance(Storage.class);
    stats = injector.getInstance(FakeStatsProvider.class);
  }

  protected abstract Injector createStorageInjector();

  @After
  public void tearDown() throws Exception {
    truncateUpdates();
  }

  private static IJobUpdate makeFullyPopulatedUpdate(IJobUpdateKey key) {
    JobUpdate builder = makeJobUpdate(key).newBuilder();
    JobUpdateInstructions instructions = builder.getInstructions();
    Stream.of(
        instructions.getInitialState().stream()
            .map(InstanceTaskConfig::getInstances)
            .flatMap(Set::stream)
            .collect(Collectors.toSet()),
        instructions.getDesiredState().getInstances(),
        instructions.getSettings().getUpdateOnlyTheseInstances())
        .flatMap(Set::stream)
        .forEach(range -> {
          if (range.getFirst() == 0) {
            range.setFirst(1);
          }
          if (range.getLast() == 0) {
            range.setLast(1);
          }
        });
    return IJobUpdate.build(builder);
  }

  @Test
  public void testSaveJobUpdates() {
    IJobUpdateKey updateId1 = makeKey(JobKeys.from("role", "env", "name1"), "u1");
    IJobUpdateKey updateId2 = makeKey(JobKeys.from("role", "env", "name2"), "u2");

    IJobUpdate update1 = makeFullyPopulatedUpdate(updateId1);
    IJobUpdate update2 = makeJobUpdate(updateId2);

    assertEquals(Optional.absent(), getUpdate(updateId1));
    assertEquals(Optional.absent(), getUpdate(updateId2));

    StorageEntityUtil.assertFullyPopulated(
        update1,
        StorageEntityUtil.getField(JobUpdateSummary.class, "state"),
        StorageEntityUtil.getField(IJobUpdateSummary.class, "state"),
        StorageEntityUtil.getField(Range.class, "first"),
        StorageEntityUtil.getField(Range.class, "last"));
    saveUpdate(update1);
    assertUpdate(update1);

    saveUpdate(update2);
    assertUpdate(update2);

    // Colliding update keys should be forbidden.
    IJobUpdate update3 = makeJobUpdate(updateId2);
    try {
      saveUpdate(update3);
      fail("Update ID collision should not be allowed");
    } catch (StorageException e) {
      // Expected.
    }
  }

  @Test
  public void testSaveJobUpdateWithLargeTaskConfigValues() {
    // AURORA-1494 regression test validating max resources values are allowed.
    IJobUpdateKey updateId = makeKey(JobKeys.from("role", "env", "name1"), "u1");

    JobUpdate builder = makeFullyPopulatedUpdate(updateId).newBuilder();
    builder.getInstructions().getDesiredState().getTask().setResources(
            ImmutableSet.of(
                    numCpus(Double.MAX_VALUE),
                    ramMb(Long.MAX_VALUE),
                    diskMb(Long.MAX_VALUE)));

    IJobUpdate update = IJobUpdate.build(builder);

    assertEquals(Optional.absent(), getUpdate(updateId));

    StorageEntityUtil.assertFullyPopulated(
        update,
        StorageEntityUtil.getField(JobUpdateSummary.class, "state"),
        StorageEntityUtil.getField(IJobUpdateSummary.class, "state"),
        StorageEntityUtil.getField(Range.class, "first"),
        StorageEntityUtil.getField(Range.class, "last"));
    saveUpdate(update);
    assertUpdate(update);
  }

  @Test
  public void testSaveNullInitialState() {
    JobUpdate builder = makeJobUpdate(makeKey("u1")).newBuilder();
    builder.getInstructions().unsetInitialState();

    // Save with null initial state instances.
    saveUpdate(IJobUpdate.build(builder));

    builder.getInstructions().setInitialState(ImmutableSet.of());
    assertUpdate(IJobUpdate.build(builder));
  }

  @Test
  public void testSaveNullDesiredState() {
    JobUpdate builder = makeJobUpdate(makeKey("u1")).newBuilder();
    builder.getInstructions().unsetDesiredState();

    // Save with null desired state instances.
    saveUpdate(IJobUpdate.build(builder));

    assertUpdate(IJobUpdate.build(builder));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testSaveBothInitialAndDesiredMissingThrows() {
    JobUpdate builder = makeJobUpdate(makeKey("u1")).newBuilder();
    builder.getInstructions().unsetInitialState();
    builder.getInstructions().unsetDesiredState();

    saveUpdate(IJobUpdate.build(builder));
  }

  @Test(expected = NullPointerException.class)
  public void testSaveNullInitialStateTaskThrows() {
    JobUpdate builder = makeJobUpdate(makeKey("u1")).newBuilder();
    builder.getInstructions().getInitialState().add(
        new InstanceTaskConfig(null, ImmutableSet.of()));

    saveUpdate(IJobUpdate.build(builder));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testSaveEmptyInitialStateRangesThrows() {
    JobUpdate builder = makeJobUpdate(makeKey("u1")).newBuilder();
    builder.getInstructions().getInitialState().add(
        new InstanceTaskConfig(
            TaskTestUtil.makeConfig(TaskTestUtil.JOB).newBuilder(),
            ImmutableSet.of()));

    saveUpdate(IJobUpdate.build(builder));
  }

  @Test(expected = NullPointerException.class)
  public void testSaveNullDesiredStateTaskThrows() {
    JobUpdate builder = makeJobUpdate(makeKey("u1")).newBuilder();
    builder.getInstructions().getDesiredState().setTask(null);

    saveUpdate(IJobUpdate.build(builder));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testSaveEmptyDesiredStateRangesThrows() {
    JobUpdate builder = makeJobUpdate(makeKey("u1")).newBuilder();
    builder.getInstructions().getDesiredState().setInstances(ImmutableSet.of());

    saveUpdate(IJobUpdate.build(builder));
  }

  @Test
  public void testSaveJobUpdateEmptyInstanceOverrides() {
    IJobUpdateKey updateId = makeKey("u1");

    IJobUpdate update = makeJobUpdate(updateId);
    JobUpdate builder = update.newBuilder();
    builder.getInstructions().getSettings().setUpdateOnlyTheseInstances(ImmutableSet.of());

    IJobUpdate expected = IJobUpdate.build(builder);

    // Save with empty overrides.
    saveUpdate(expected);
    assertUpdate(expected);
  }

  @Test
  public void testSaveJobUpdateNullInstanceOverrides() {
    IJobUpdateKey updateId = makeKey("u1");

    IJobUpdate update = makeJobUpdate(updateId);
    JobUpdate builder = update.newBuilder();
    builder.getInstructions().getSettings().setUpdateOnlyTheseInstances(ImmutableSet.of());

    IJobUpdate expected = IJobUpdate.build(builder);

    // Save with null overrides.
    builder.getInstructions().getSettings().setUpdateOnlyTheseInstances(null);
    saveUpdate(IJobUpdate.build(builder));
    assertUpdate(expected);
  }

  @Test(expected = StorageException.class)
  public void testSaveJobUpdateTwiceThrows() {
    IJobUpdateKey updateId = makeKey("u1");
    IJobUpdate update = makeJobUpdate(updateId);

    saveUpdate(update);
    saveUpdate(update);
  }

  private <T extends Number> void assertStats(Map<JobUpdateStatus, T> expected) {
    for (Map.Entry<JobUpdateStatus, T> entry : expected.entrySet()) {
      assertEquals(
          entry.getValue().longValue(),
          stats.getLongValue(Util.jobUpdateStatusStatName(entry.getKey())));
    }
  }

  @Test(expected = StorageException.class)
  public void testSaveJobEventWithoutUpdateFails() {
    saveJobEvent(makeJobUpdateEvent(ROLLING_FORWARD, 123L), makeKey("u2"));
  }

  @Test(expected = StorageException.class)
  public void testSaveInstanceEventWithoutUpdateFails() {
    saveJobInstanceEvent(makeJobInstanceEvent(0, 125L, INSTANCE_UPDATED), makeKey("u1"));
  }

  @Test
  public void testSaveJobUpdateStateIgnored() {
    IJobUpdateKey updateId = makeKey("u1");
    IJobUpdate update = populateExpected(makeJobUpdate(updateId), ABORTED, 567L, 567L);
    saveUpdate(update);

    // Assert state fields were ignored.
    assertUpdate(update);
  }

  @Test
  public void testMultipleJobDetails() {
    IJobUpdateKey updateId1 = makeKey(JobKeys.from("role", "env", "name1"), "u1");
    IJobUpdateKey updateId2 = makeKey(JobKeys.from("role", "env", "name2"), "u2");
    IJobUpdateDetails details1 = makeJobDetails(makeJobUpdate(updateId1));
    IJobUpdateDetails details2 = makeJobDetails(makeJobUpdate(updateId2));

    assertEquals(ImmutableList.of(), getInstanceEvents(updateId2, 3));

    saveUpdate(details1.getUpdate());
    saveUpdate(details2.getUpdate());

    details1 = updateJobDetails(populateExpected(details1.getUpdate()), FIRST_EVENT);
    details2 = updateJobDetails(populateExpected(details2.getUpdate()), FIRST_EVENT);
    assertEquals(Optional.of(details1), getUpdateDetails(updateId1));
    assertEquals(Optional.of(details2), getUpdateDetails(updateId2));

    IJobUpdateEvent jEvent11 = makeJobUpdateEvent(ROLLING_FORWARD, 456L);
    IJobUpdateEvent jEvent12 = makeJobUpdateEvent(ERROR, 457L);
    IJobInstanceUpdateEvent iEvent11 = makeJobInstanceEvent(1, 451L, INSTANCE_UPDATED);
    IJobInstanceUpdateEvent iEvent12 = makeJobInstanceEvent(2, 452L, INSTANCE_UPDATING);

    IJobUpdateEvent jEvent21 = makeJobUpdateEvent(ROLL_FORWARD_PAUSED, 567L);
    IJobUpdateEvent jEvent22 = makeJobUpdateEvent(ABORTED, 568L);
    IJobInstanceUpdateEvent iEvent21 = makeJobInstanceEvent(3, 561L, INSTANCE_UPDATING);
    IJobInstanceUpdateEvent iEvent22 = makeJobInstanceEvent(3, 562L, INSTANCE_UPDATED);

    saveJobEvent(jEvent11, updateId1);
    saveJobEvent(jEvent12, updateId1);
    saveJobInstanceEvent(iEvent11, updateId1);
    assertEquals(1L, stats.getLongValue(jobUpdateActionStatName(INSTANCE_UPDATED)));
    saveJobInstanceEvent(iEvent12, updateId1);
    assertEquals(1L, stats.getLongValue(jobUpdateActionStatName(INSTANCE_UPDATING)));

    saveJobEvent(jEvent21, updateId2);
    saveJobEvent(jEvent22, updateId2);
    assertEquals(ImmutableList.of(), getInstanceEvents(updateId2, 3));
    saveJobInstanceEvent(iEvent21, updateId2);
    assertEquals(2L, stats.getLongValue(jobUpdateActionStatName(INSTANCE_UPDATING)));

    assertEquals(ImmutableList.of(iEvent21), getInstanceEvents(updateId2, 3));
    saveJobInstanceEvent(iEvent22, updateId2);
    assertEquals(ImmutableList.of(iEvent21, iEvent22), getInstanceEvents(updateId2, 3));
    assertEquals(2L, stats.getLongValue(jobUpdateActionStatName(INSTANCE_UPDATING)));

    details1 = updateJobDetails(
        populateExpected(details1.getUpdate(), ERROR, CREATED_MS, 457L),
        ImmutableList.of(FIRST_EVENT, jEvent11, jEvent12), ImmutableList.of(iEvent11, iEvent12));

    details2 = updateJobDetails(
        populateExpected(details2.getUpdate(), ABORTED, CREATED_MS, 568L),
        ImmutableList.of(FIRST_EVENT, jEvent21, jEvent22), ImmutableList.of(iEvent21, iEvent22));

    assertEquals(Optional.of(details1), getUpdateDetails(updateId1));
    assertEquals(Optional.of(details2), getUpdateDetails(updateId2));

    assertEquals(ImmutableSet.of(details1, details2), getAllUpdateDetails());

    assertEquals(
        ImmutableList.of(getUpdateDetails(updateId2).get(), getUpdateDetails(updateId1).get()),
        queryDetails(new JobUpdateQuery().setRole("role")));
  }

  @Test
  public void testTruncateJobUpdates() {
    IJobUpdateKey updateId = makeKey("u5");
    IJobUpdate update = makeJobUpdate(updateId);
    IJobInstanceUpdateEvent instanceEvent = IJobInstanceUpdateEvent.build(
        new JobInstanceUpdateEvent(0, 125L, INSTANCE_ROLLBACK_FAILED));

    saveUpdate(update);
    saveJobEvent(makeJobUpdateEvent(ROLLING_FORWARD, 123L), updateId);
    saveJobInstanceEvent(instanceEvent, updateId);
    assertEquals(1L, stats.getLongValue(jobUpdateActionStatName(INSTANCE_ROLLBACK_FAILED)));
    assertEquals(
        populateExpected(update, ROLLING_FORWARD, CREATED_MS, 125L),
        getUpdate(updateId).get());
    assertEquals(2, getUpdateDetails(updateId).get().getUpdateEvents().size());
    assertEquals(1, getUpdateDetails(updateId).get().getInstanceEvents().size());

    truncateUpdates();
    assertEquals(Optional.absent(), getUpdateDetails(updateId));
  }

  @Test
  public void testDeleteUpdates() {
    fail();
  }

  @Test(expected = StorageException.class)
  public void testSaveTwoUpdatesForOneJob() {
    IJobUpdate update = makeJobUpdate(makeKey("updateId"));
    saveUpdate(update);
    saveUpdate(update);
  }

  @Test
  public void testSaveJobUpdateWithDuplicateMetadataKeys() {
    IJobUpdateKey updateId = makeKey(JobKeys.from("role", "env", "name1"), "u1");

    ImmutableSet<Metadata> duplicatedMetadata =
        ImmutableSet.of(new Metadata("k1", "v1"), new Metadata("k1", "v2"));
    JobUpdate builder = makeJobUpdate(updateId).newBuilder();
    builder.getSummary().setMetadata(duplicatedMetadata);
    IJobUpdate update = IJobUpdate.build(builder);

    assertEquals(Optional.absent(), getUpdate(updateId));

    saveUpdate(update);
    assertUpdate(update);
  }

  @Test
  public void testGetSummaries() {
    String role1 = "role1";
    IJobKey job1 = JobKeys.from(role1, "env", "name1");
    IJobKey job2 = JobKeys.from(role1, "env", "name2");
    IJobKey job3 = JobKeys.from(role1, "env", "name3");
    IJobKey job4 = JobKeys.from(role1, "env", "name4");
    IJobKey job5 = JobKeys.from("role", "env", "name5");
    IJobUpdateSummary s1 = saveSummary(makeKey(job1, "u1"), 1230L, ROLLED_BACK, "user");
    IJobUpdateSummary s2 = saveSummary(makeKey(job2, "u2"), 1231L, ABORTED, "user");
    IJobUpdateSummary s3 = saveSummary(makeKey(job3, "u3"), 1239L, ERROR, "user2");
    IJobUpdateSummary s4 = saveSummary(makeKey(job4, "u4"), 1234L, ROLL_BACK_PAUSED, "user3");
    IJobUpdateSummary s5 = saveSummary(makeKey(job5, "u5"), 1235L, ROLLING_FORWARD, "user4");

    // Test empty query returns all.
    assertEquals(ImmutableList.of(s3, s5, s4, s2, s1), getSummaries(new JobUpdateQuery()));

    // Test query by updateId.
    assertEquals(
        ImmutableList.of(s1),
        getSummaries(new JobUpdateQuery().setKey(new JobUpdateKey(job1.newBuilder(), "u1"))));

    // Test query by role.
    assertEquals(
        ImmutableList.of(s3, s4, s2, s1),
        getSummaries(new JobUpdateQuery().setRole(role1)));

    // Test query by job key.
    assertEquals(
        ImmutableList.of(s5),
        getSummaries(new JobUpdateQuery().setJobKey(job5.newBuilder())));

    // Test querying by update key.
    assertEquals(
        ImmutableList.of(s5),
        getSummaries(
            new JobUpdateQuery().setKey(new JobUpdateKey(job5.newBuilder(), s5.getKey().getId()))));

    // Test querying by incorrect update keys.
    assertEquals(
        ImmutableList.of(),
        getSummaries(
            new JobUpdateQuery().setKey(new JobUpdateKey(job5.newBuilder(), s4.getKey().getId()))));
    assertEquals(
        ImmutableList.of(),
        getSummaries(
            new JobUpdateQuery().setKey(new JobUpdateKey(job4.newBuilder(), s5.getKey().getId()))));

    // Test query by user.
    assertEquals(ImmutableList.of(s2, s1), getSummaries(new JobUpdateQuery().setUser("user")));

    // Test query by one status.
    assertEquals(ImmutableList.of(s3), getSummaries(new JobUpdateQuery().setUpdateStatuses(
        ImmutableSet.of(ERROR))));

    // Test query by multiple statuses.
    assertEquals(ImmutableList.of(s3, s2, s1), getSummaries(new JobUpdateQuery().setUpdateStatuses(
        ImmutableSet.of(ERROR, ABORTED, ROLLED_BACK))));

    // Test query by empty statuses.
    assertEquals(
        ImmutableList.of(s3, s5, s4, s2, s1),
        getSummaries(new JobUpdateQuery().setUpdateStatuses(ImmutableSet.of())));

    // Test paging.
    assertEquals(
        ImmutableList.of(s3, s5),
        getSummaries(new JobUpdateQuery().setLimit(2).setOffset(0)));
    assertEquals(
        ImmutableList.of(s4, s2),
        getSummaries(new JobUpdateQuery().setLimit(2).setOffset(2)));
    assertEquals(
        ImmutableList.of(s1),
        getSummaries(new JobUpdateQuery().setLimit(2).setOffset(4)));

    // Test no match.
    assertEquals(
        ImmutableList.of(),
        getSummaries(new JobUpdateQuery().setRole("no_match")));
  }

  @Test
  public void testQueryDetails() {
    IJobKey jobKey1 = JobKeys.from("role1", "env", "name1");
    IJobUpdateKey updateId1 = makeKey(jobKey1, "u1");
    IJobKey jobKey2 = JobKeys.from("role2", "env", "name2");
    IJobUpdateKey updateId2 = makeKey(jobKey2, "u2");

    IJobUpdate update1 = makeJobUpdate(updateId1);
    IJobUpdate update2 = makeJobUpdate(updateId2);

    assertEquals(ImmutableList.of(), getInstanceEvents(updateId2, 3));

    saveUpdate(update1);
    saveUpdate(update2);

    updateJobDetails(populateExpected(update1), FIRST_EVENT);
    updateJobDetails(populateExpected(update2), FIRST_EVENT);

    IJobUpdateEvent jEvent11 = makeJobUpdateEvent(ROLLING_BACK, 450L);
    IJobUpdateEvent jEvent12 = makeJobUpdateEvent(ROLLED_BACK, 500L);
    IJobInstanceUpdateEvent iEvent11 = makeJobInstanceEvent(1, 451L, INSTANCE_ROLLING_BACK);
    IJobInstanceUpdateEvent iEvent12 = makeJobInstanceEvent(2, 458L, INSTANCE_ROLLED_BACK);

    IJobUpdateEvent jEvent21 = makeJobUpdateEvent(ROLL_FORWARD_PAUSED, 550L);
    IJobUpdateEvent jEvent22 = makeJobUpdateEvent(ABORTED, 600L);
    IJobInstanceUpdateEvent iEvent21 = makeJobInstanceEvent(3, 561L, INSTANCE_UPDATING);
    IJobInstanceUpdateEvent iEvent22 = makeJobInstanceEvent(3, 570L, INSTANCE_UPDATED);

    saveJobEvent(jEvent11, updateId1);
    saveJobEvent(jEvent12, updateId1);
    saveJobInstanceEvent(iEvent11, updateId1);
    saveJobInstanceEvent(iEvent12, updateId1);

    saveJobEvent(jEvent21, updateId2);
    saveJobEvent(jEvent22, updateId2);

    saveJobInstanceEvent(iEvent21, updateId2);
    saveJobInstanceEvent(iEvent22, updateId2);

    IJobUpdateDetails details1 = getUpdateDetails(updateId1).get();
    IJobUpdateDetails details2 = getUpdateDetails(updateId2).get();

    // Test empty query returns all.
    assertEquals(ImmutableList.of(details2, details1), queryDetails(new JobUpdateQuery()));

    // Test query by update ID.
    assertEquals(
        ImmutableList.of(details1),
        queryDetails(new JobUpdateQuery().setKey(updateId1.newBuilder())));

    // Test query by role.
    assertEquals(
        ImmutableList.of(details2),
        queryDetails(new JobUpdateQuery().setRole(jobKey2.getRole())));

    // Test query by job key.
    assertEquals(
        ImmutableList.of(details2),
        queryDetails(new JobUpdateQuery().setJobKey(jobKey2.newBuilder())));

    // Test query by status.
    assertEquals(
        ImmutableList.of(details2),
        queryDetails(new JobUpdateQuery().setUpdateStatuses(ImmutableSet.of(ABORTED))));

    // Test no match.
    assertEquals(
        ImmutableList.of(),
        queryDetails(new JobUpdateQuery().setRole("no match")));
  }

  @Test
  public void testSaveEventsOutOfChronologicalOrder() {
    IJobUpdate update1 = makeJobUpdate(UPDATE1);
    saveUpdate(update1);

    IJobUpdateEvent event2 = makeJobUpdateEvent(ROLLING_FORWARD, 124);
    IJobUpdateEvent event1 = makeJobUpdateEvent(ROLL_FORWARD_PAUSED, 122);
    saveJobEvent(event2, UPDATE1);
    saveJobEvent(event1, UPDATE1);

    IJobInstanceUpdateEvent instanceEvent2 = makeJobInstanceEvent(0, 125, INSTANCE_UPDATED);
    IJobInstanceUpdateEvent instanceEvent1 = makeJobInstanceEvent(0, 124, INSTANCE_UPDATING);

    saveJobInstanceEvent(instanceEvent2, UPDATE1);
    saveJobInstanceEvent(instanceEvent1, UPDATE1);

    IJobUpdateState state = getUpdateDetails(UPDATE1).get().getUpdate().getSummary().getState();
    assertEquals(ROLLING_FORWARD, state.getStatus());
    assertEquals(125, state.getLastModifiedTimestampMs());
  }

  private static IJobUpdateKey makeKey(String id) {
    return makeKey(JOB, id);
  }

  private static IJobUpdateKey makeKey(IJobKey job, String id) {
    return IJobUpdateKey.build(new JobUpdateKey(job.newBuilder(), id));
  }

  private void assertUpdate(IJobUpdate expected) {
    IJobUpdateKey key = expected.getSummary().getKey();
    assertEquals(populateExpected(expected), getUpdate(key).get());
    assertEquals(getUpdate(key).get(), getUpdateDetails(key).get().getUpdate());
    assertEquals(getUpdateInstructions(key).get(), expected.getInstructions());
  }

  private Optional<IJobUpdate> getUpdate(IJobUpdateKey key) {
    return storage.read(storeProvider -> storeProvider.getJobUpdateStore().fetchJobUpdate(key));
  }

  private List<IJobInstanceUpdateEvent> getInstanceEvents(IJobUpdateKey key, int id) {
    return storage.read(
        storeProvider -> storeProvider.getJobUpdateStore().fetchInstanceEvents(key, id));
  }

  private Optional<IJobUpdateInstructions> getUpdateInstructions(IJobUpdateKey key) {
    return storage.read(
        storeProvider -> storeProvider.getJobUpdateStore().fetchJobUpdateInstructions(key));
  }

  private Optional<IJobUpdateDetails> getUpdateDetails(IJobUpdateKey key) {
    return storage.read(
        storeProvider -> storeProvider.getJobUpdateStore().fetchJobUpdates(key));
  }

  private Set<IJobUpdateDetails> getAllUpdateDetails() {
    return storage.read(
        storeProvider -> storeProvider.getJobUpdateStore().fetchAllJobUpdateDetails());
  }

  private List<IJobUpdateDetails> queryDetails(JobUpdateQuery query) {
    return storage.read(storeProvider -> storeProvider.getJobUpdateStore().fetchJobUpdates(
        IJobUpdateQuery.build(query)));
  }

  private List<IJobUpdateSummary> getSummaries(JobUpdateQuery query) {
    return storage.read(storeProvider -> storeProvider.getJobUpdateStore().fetchJobUpdateSummaries(
        IJobUpdateQuery.build(query)));
  }

  private void saveUpdate(IJobUpdate update) {
    storage.write((NoResult.Quiet) storeProvider -> {
      storeProvider.getJobUpdateStore().saveJobUpdate(update);
      storeProvider.getJobUpdateStore().saveJobUpdateEvent(
          update.getSummary().getKey(),
          FIRST_EVENT);
    });
  }

  private void saveJobEvent(IJobUpdateEvent event, IJobUpdateKey key) {
    storage.write((NoResult.Quiet)
        storeProvider -> storeProvider.getJobUpdateStore().saveJobUpdateEvent(key, event));
  }

  private void saveJobInstanceEvent(IJobInstanceUpdateEvent event, IJobUpdateKey key) {
    storage.write((NoResult.Quiet)
        storeProvider -> storeProvider.getJobUpdateStore().saveJobInstanceUpdateEvent(key, event));
  }

  private void truncateUpdates() {
    storage.write((NoResult.Quiet)
        storeProvider -> storeProvider.getJobUpdateStore().deleteAllUpdatesAndEvents());
  }

  private IJobUpdate populateExpected(IJobUpdate update) {
    return populateExpected(update, ROLLING_FORWARD, CREATED_MS, CREATED_MS);
  }

  private IJobUpdate populateExpected(
      IJobUpdate update,
      JobUpdateStatus status,
      long createdMs,
      long lastMs) {

    JobUpdateState state = new JobUpdateState()
        .setCreatedTimestampMs(createdMs)
        .setLastModifiedTimestampMs(lastMs)
        .setStatus(status);
    JobUpdate builder = update.newBuilder();
    builder.getSummary().setState(state);
    return IJobUpdate.build(builder);
  }

  private static IJobUpdateEvent makeJobUpdateEvent(JobUpdateStatus status, long timestampMs) {
    return IJobUpdateEvent.build(
        new JobUpdateEvent(status, timestampMs)
            .setUser("user")
            .setMessage("message"));
  }

  private IJobInstanceUpdateEvent makeJobInstanceEvent(
      int instanceId,
      long timestampMs,
      JobUpdateAction action) {

    return IJobInstanceUpdateEvent.build(
        new JobInstanceUpdateEvent(instanceId, timestampMs, action));
  }

  private IJobUpdateDetails makeJobDetails(IJobUpdate update) {
    return updateJobDetails(
        update,
        ImmutableList.of(FIRST_EVENT),
        ImmutableList.of());
  }

  private IJobUpdateDetails updateJobDetails(IJobUpdate update, IJobUpdateEvent event) {
    return updateJobDetails(
        update,
        ImmutableList.of(event),
        ImmutableList.of());
  }

  private IJobUpdateDetails updateJobDetails(
      IJobUpdate update,
      List<IJobUpdateEvent> jobEvents,
      List<IJobInstanceUpdateEvent> instanceEvents) {

    return IJobUpdateDetails.build(new JobUpdateDetails()
        .setUpdate(update.newBuilder())
        .setUpdateEvents(IJobUpdateEvent.toBuildersList(jobEvents))
        .setInstanceEvents(IJobInstanceUpdateEvent.toBuildersList(instanceEvents)));
  }

  private static IJobUpdateSummary makeSummary(IJobUpdateKey key, String user) {
    return IJobUpdateSummary.build(new JobUpdateSummary()
        .setKey(key.newBuilder())
        .setUser(user)
        .setMetadata(METADATA));
  }

  private IJobUpdateSummary saveSummary(
      IJobUpdateKey key,
      Long modifiedTimestampMs,
      JobUpdateStatus status,
      String user) {

    IJobUpdateSummary summary = IJobUpdateSummary.build(new JobUpdateSummary()
        .setKey(key.newBuilder())
        .setUser(user)
        .setMetadata(METADATA));

    IJobUpdate update = makeJobUpdate(summary);
    saveUpdate(update);
    saveJobEvent(makeJobUpdateEvent(status, modifiedTimestampMs), key);
    return populateExpected(update, status, CREATED_MS, modifiedTimestampMs).getSummary();
  }

  private IJobUpdate makeJobUpdate(IJobUpdateSummary summary) {
    return IJobUpdate.build(makeJobUpdate().newBuilder().setSummary(summary.newBuilder()));
  }

  private static IJobUpdate makeJobUpdate(IJobUpdateKey key) {
    return IJobUpdate.build(makeJobUpdate().newBuilder()
        .setSummary(makeSummary(key, "user").newBuilder()));
  }

  private static IJobUpdate makeJobUpdate() {
    return IJobUpdate.build(new JobUpdate()
        .setInstructions(makeJobUpdateInstructions().newBuilder()));
  }

  private static IJobUpdateInstructions makeJobUpdateInstructions() {
    TaskConfig config = TaskTestUtil.makeConfig(JOB).newBuilder();
    return IJobUpdateInstructions.build(new JobUpdateInstructions()
        .setDesiredState(new InstanceTaskConfig()
            .setTask(config)
            .setInstances(ImmutableSet.of(new Range(0, 7), new Range(8, 9))))
        .setInitialState(ImmutableSet.of(
            new InstanceTaskConfig()
                .setInstances(ImmutableSet.of(new Range(0, 1), new Range(2, 3)))
                .setTask(config),
            new InstanceTaskConfig()
                .setInstances(ImmutableSet.of(new Range(4, 5), new Range(6, 7)))
                .setTask(config)))
        .setSettings(new JobUpdateSettings()
            .setBlockIfNoPulsesAfterMs(500)
            .setUpdateGroupSize(1)
            .setMaxPerInstanceFailures(1)
            .setMaxFailedInstances(1)
            .setMinWaitInInstanceRunningMs(200)
            .setRollbackOnFailure(true)
            .setWaitForBatchCompletion(true)
            .setUpdateOnlyTheseInstances(ImmutableSet.of(new Range(0, 0), new Range(3, 5)))));
  }
}
