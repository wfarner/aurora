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
import org.apache.aurora.scheduler.storage.entities.IJobUpdateDetails;
import org.apache.aurora.scheduler.storage.entities.IJobUpdateEvent;
import org.apache.aurora.scheduler.storage.entities.IJobUpdateInstructions;
import org.apache.aurora.scheduler.storage.entities.IJobUpdateKey;
import org.apache.aurora.scheduler.storage.entities.IJobUpdateQuery;
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

  private static IJobUpdateDetails makeFullyPopulatedUpdate(IJobUpdateKey key) {
    JobUpdateDetails builder = makeJobUpdate(key).newBuilder()
        .setUpdateEvents(ImmutableList.of(new JobUpdateEvent()
            .setStatus(JobUpdateStatus.ROLLING_FORWARD)
            .setTimestampMs(1)
            .setUser("user")
            .setMessage("message")))
        .setInstanceEvents(ImmutableList.of(new JobInstanceUpdateEvent()
            .setTimestampMs(1)
            .setInstanceId(1)
            .setAction(JobUpdateAction.INSTANCE_UPDATING)));
    JobUpdateInstructions instructions = builder.getUpdate().getInstructions();
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
    return IJobUpdateDetails.build(builder);
  }

  @Test
  public void testSaveJobUpdates() {
    IJobUpdateKey updateId1 = makeKey(JobKeys.from("role", "env", "name1"), "u1");
    IJobUpdateKey updateId2 = makeKey(JobKeys.from("role", "env", "name2"), "u2");

    IJobUpdateDetails update1 = makeFullyPopulatedUpdate(updateId1);
    IJobUpdateDetails update2 = makeJobUpdate(updateId2);

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
    IJobUpdateDetails update3 = makeJobUpdate(updateId2);
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

    JobUpdateDetails builder = makeFullyPopulatedUpdate(updateId).newBuilder();
    builder.getUpdate().getInstructions().getDesiredState().getTask().setResources(
            ImmutableSet.of(
                    numCpus(Double.MAX_VALUE),
                    ramMb(Long.MAX_VALUE),
                    diskMb(Long.MAX_VALUE)));

    assertEquals(Optional.absent(), getUpdate(updateId));

    IJobUpdateDetails update = makeFullyPopulatedUpdate(updateId);
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
    JobUpdateDetails builder = makeJobUpdate(makeKey("u1")).newBuilder();
    builder.getUpdate().getInstructions().unsetInitialState();

    // Save with null initial state instances.
    saveUpdate(IJobUpdateDetails.build(builder));

    builder.getUpdate().getInstructions().setInitialState(ImmutableSet.of());
    assertUpdate(IJobUpdateDetails.build(builder));
  }

  @Test
  public void testSaveNullDesiredState() {
    JobUpdateDetails builder = makeJobUpdate(makeKey("u1")).newBuilder();
    builder.getUpdate().getInstructions().unsetDesiredState();

    // Save with null desired state instances.
    saveUpdate(IJobUpdateDetails.build(builder));

    assertUpdate(IJobUpdateDetails.build(builder));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testSaveBothInitialAndDesiredMissingThrows() {
    JobUpdateDetails builder = makeJobUpdate(makeKey("u1")).newBuilder();
    builder.getUpdate().getInstructions().unsetInitialState();
    builder.getUpdate().getInstructions().unsetDesiredState();

    saveUpdate(IJobUpdateDetails.build(builder));
  }

  @Test(expected = NullPointerException.class)
  public void testSaveNullInitialStateTaskThrows() {
    JobUpdateDetails builder = makeJobUpdate(makeKey("u1")).newBuilder();
    builder.getUpdate().getInstructions().getInitialState().add(
        new InstanceTaskConfig(null, ImmutableSet.of()));

    saveUpdate(IJobUpdateDetails.build(builder));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testSaveEmptyInitialStateRangesThrows() {
    JobUpdateDetails builder = makeJobUpdate(makeKey("u1")).newBuilder();
    builder.getUpdate().getInstructions().getInitialState().add(
        new InstanceTaskConfig(
            TaskTestUtil.makeConfig(TaskTestUtil.JOB).newBuilder(),
            ImmutableSet.of()));

    saveUpdate(IJobUpdateDetails.build(builder));
  }

  @Test(expected = NullPointerException.class)
  public void testSaveNullDesiredStateTaskThrows() {
    JobUpdateDetails builder = makeJobUpdate(makeKey("u1")).newBuilder();
    builder.getUpdate().getInstructions().getDesiredState().setTask(null);

    saveUpdate(IJobUpdateDetails.build(builder));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testSaveEmptyDesiredStateRangesThrows() {
    JobUpdateDetails builder = makeJobUpdate(makeKey("u1")).newBuilder();
    builder.getUpdate().getInstructions().getDesiredState().setInstances(ImmutableSet.of());

    saveUpdate(IJobUpdateDetails.build(builder));
  }

  @Test
  public void testSaveJobUpdateEmptyInstanceOverrides() {
    IJobUpdateKey updateId = makeKey("u1");

    JobUpdateDetails builder = makeJobUpdate(updateId).newBuilder();
    builder.getUpdate().getInstructions().getSettings()
        .setUpdateOnlyTheseInstances(ImmutableSet.of());

    IJobUpdateDetails expected = IJobUpdateDetails.build(builder);

    // Save with empty overrides.
    saveUpdate(expected);
    assertUpdate(expected);
  }

  @Test
  public void testSaveJobUpdateNullInstanceOverrides() {
    IJobUpdateKey updateId = makeKey("u1");

    JobUpdateDetails builder = makeJobUpdate(updateId).newBuilder();
    builder.getUpdate().getInstructions().getSettings()
        .setUpdateOnlyTheseInstances(ImmutableSet.of());

    IJobUpdateDetails expected = IJobUpdateDetails.build(builder);

    // Save with null overrides.
    builder.getUpdate().getInstructions().getSettings().setUpdateOnlyTheseInstances(null);
    saveUpdate(IJobUpdateDetails.build(builder));
    assertUpdate(expected);
  }

  @Test(expected = StorageException.class)
  public void testSaveJobUpdateTwiceThrows() {
    IJobUpdateKey updateId = makeKey("u1");
    IJobUpdateDetails update = makeJobUpdate(updateId);

    saveUpdate(update);
    saveUpdate(update);
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
    IJobUpdateDetails update = populateExpected(makeJobUpdate(updateId), ABORTED, 567L, 567L);
    saveUpdate(update);

    // Assert state fields were ignored.
    assertUpdate(update);
  }

  @Test
  public void testMultipleJobDetails() {
    IJobUpdateKey updateId1 = makeKey(JobKeys.from("role", "env", "name1"), "u1");
    IJobUpdateKey updateId2 = makeKey(JobKeys.from("role", "env", "name2"), "u2");
    IJobUpdateDetails update1 = makeJobUpdate(updateId1);
    IJobUpdateDetails update2 = makeJobUpdate(updateId2);

    assertEquals(ImmutableList.of(), getInstanceEvents(updateId2, 3));

    saveUpdate(update1);
    saveUpdate(update2);

    update1 = updateJobDetails(populateExpected(update1), FIRST_EVENT);
    update2 = updateJobDetails(populateExpected(update2), FIRST_EVENT);
    assertEquals(Optional.of(update1), getUpdateDetails(updateId1));
    assertEquals(Optional.of(update2), getUpdateDetails(updateId2));

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

    update1 = updateJobDetails(
        populateExpected(update1, ERROR, CREATED_MS, 457L),
        ImmutableList.of(FIRST_EVENT, jEvent11, jEvent12), ImmutableList.of(iEvent11, iEvent12));

    update2 = updateJobDetails(
        populateExpected(update2, ABORTED, CREATED_MS, 568L),
        ImmutableList.of(FIRST_EVENT, jEvent21, jEvent22), ImmutableList.of(iEvent21, iEvent22));

    assertEquals(
        ImmutableSet.of(update1, update2),
        fetchUpdates(JobUpdateStore.MATCH_ALL.newBuilder()));

    assertEquals(
        ImmutableList.of(getUpdateDetails(updateId2).get(), getUpdateDetails(updateId1).get()),
        fetchUpdates(new JobUpdateQuery().setRole("role")));
  }

  @Test
  public void testTruncateJobUpdates() {
    IJobUpdateKey updateId = makeKey("u5");
    IJobUpdateDetails update = makeJobUpdate(updateId);
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
    IJobUpdateDetails update = makeJobUpdate(makeKey("updateId"));
    saveUpdate(update);
    saveUpdate(update);
  }

  @Test
  public void testSaveJobUpdateWithDuplicateMetadataKeys() {
    IJobUpdateKey updateId = makeKey(JobKeys.from("role", "env", "name1"), "u1");

    ImmutableSet<Metadata> duplicatedMetadata =
        ImmutableSet.of(new Metadata("k1", "v1"), new Metadata("k1", "v2"));
    JobUpdateDetails builder = makeJobUpdate(updateId).newBuilder();
    builder.getUpdate().getSummary().setMetadata(duplicatedMetadata);

    assertEquals(Optional.absent(), getUpdate(updateId));

    IJobUpdateDetails update = IJobUpdateDetails.build(builder);
    saveUpdate(update);
    assertUpdate(update);
  }

  @Test
  public void testQueryDetails() {
    IJobKey jobKey1 = JobKeys.from("role1", "env", "name1");
    IJobUpdateKey updateId1 = makeKey(jobKey1, "u1");
    IJobKey jobKey2 = JobKeys.from("role2", "env", "name2");
    IJobUpdateKey updateId2 = makeKey(jobKey2, "u2");

    IJobUpdateDetails update1 = makeJobUpdate(updateId1);
    IJobUpdateDetails update2 = makeJobUpdate(updateId2);

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

    // Empty query returns all.
    assertQueryMatches(new JobUpdateQuery(), details1, details2);

    // Query by update ID.
    assertQueryMatches(new JobUpdateQuery().setKey(updateId1.newBuilder()), details1);

    // Query by role.
    assertQueryMatches(new JobUpdateQuery().setRole(jobKey2.getRole()), details2);

    // Query by job key.
    assertQueryMatches(new JobUpdateQuery().setJobKey(jobKey2.newBuilder()), details2);

    // Query by status.
    assertQueryMatches(new JobUpdateQuery().setUpdateStatuses(ImmutableSet.of(ABORTED)), details2);

    // No match.
    assertEquals(
        ImmutableList.of(),
        fetchUpdates(new JobUpdateQuery().setRole("no match")));
/*
TODO(wfarner): Finish.
    // Querying by incorrect update keys.
    assertEquals(
        ImmutableList.of(),
        getSummaries(
            new JobUpdateQuery().setKey(new JobUpdateKey(job5.newBuilder(), s4.getKey().getId()))));
    assertEquals(
        ImmutableList.of(),
        getSummaries(
            new JobUpdateQuery().setKey(new JobUpdateKey(job4.newBuilder(), s5.getKey().getId()))));

    // Query by multiple statuses.
    assertEquals(
        ImmutableList.of(s3, s2, s1),
        getSummaries(new JobUpdateQuery().setUpdateStatuses(
            ImmutableSet.of(ERROR, ABORTED, ROLLED_BACK))));

    // Query by empty statuses.
    assertEquals(
        ImmutableList.of(s3, s5, s4, s2, s1),
        getSummaries(new JobUpdateQuery().setUpdateStatuses(ImmutableSet.of())));

    // Query by user.
    assertEquals(ImmutableList.of(s2, s1), getSummaries(new JobUpdateQuery().setUser("user")));

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
*/
  }

  private static IJobUpdateKey key(IJobUpdateDetails update) {
    return update.getUpdate().getSummary().getKey();
  }

  private void assertQueryMatches(JobUpdateQuery query, IJobUpdateDetails... matches) {
    assertEquals(
        ImmutableSet.of(matches),
        storage.read(store ->
            store.getJobUpdateStore().fetchJobUpdates(IJobUpdateQuery.build(query))));
  }

  private static IJobUpdateKey makeKey(String id) {
    return makeKey(JOB, id);
  }

  private static IJobUpdateKey makeKey(IJobKey job, String id) {
    return IJobUpdateKey.build(new JobUpdateKey(job.newBuilder(), id));
  }

  private void assertUpdate(IJobUpdateDetails expected) {
    assertEquals(expected, getUpdate(key(expected)).get());
  }

  private Optional<IJobUpdateDetails> getUpdate(IJobUpdateKey key) {
    return storage.read(storeProvider -> storeProvider.getJobUpdateStore().fetchJobUpdate(key));
  }

  private List<IJobInstanceUpdateEvent> getInstanceEvents(IJobUpdateKey key, int id) {
    IJobUpdateDetails update =
        storage.read(store -> store.getJobUpdateStore().fetchJobUpdate(key).get());
    return update.getInstanceEvents().stream()
        .filter(e -> e.getInstanceId() == id)
        .collect(Collectors.toList());
  }

  private Optional<IJobUpdateDetails> getUpdateDetails(IJobUpdateKey key) {
    return storage.read(storeProvider -> storeProvider.getJobUpdateStore().fetchJobUpdate(key));
  }

  private Set<IJobUpdateDetails> fetchUpdates(JobUpdateQuery query) {
    return storage.read(storeProvider ->
        storeProvider.getJobUpdateStore().fetchJobUpdates(IJobUpdateQuery.build(query)));
  }

  private void saveUpdate(IJobUpdateDetails update) {
    storage.write((NoResult.Quiet) storeProvider ->
      storeProvider.getJobUpdateStore().saveJobUpdate(update)
    );
  }

  private void saveJobEvent(IJobUpdateEvent event, IJobUpdateKey key) {
    storage.write((NoResult.Quiet) store -> {
      JobUpdateDetails mutable = store.getJobUpdateStore().fetchJobUpdate(key).get().newBuilder();
      mutable.addToUpdateEvents(event.newBuilder());
      store.getJobUpdateStore().saveJobUpdate(IJobUpdateDetails.build(mutable));
    });
  }

  private void saveJobInstanceEvent(IJobInstanceUpdateEvent event, IJobUpdateKey key) {
    storage.write((NoResult.Quiet) store -> {
      JobUpdateDetails mutable = store.getJobUpdateStore().fetchJobUpdate(key).get().newBuilder();
      mutable.addToInstanceEvents(event.newBuilder());
      store.getJobUpdateStore().saveJobUpdate(IJobUpdateDetails.build(mutable));
    });
  }

  private void truncateUpdates() {
    storage.write((NoResult.Quiet)
        storeProvider -> storeProvider.getJobUpdateStore().deleteAllUpdates());
  }

  private IJobUpdateDetails populateExpected(IJobUpdateDetails update) {
    return populateExpected(update, ROLLING_FORWARD, CREATED_MS, CREATED_MS);
  }

  private IJobUpdateDetails populateExpected(
      IJobUpdateDetails update,
      JobUpdateStatus status,
      long createdMs,
      long lastMs) {

    JobUpdateState state = new JobUpdateState()
        .setCreatedTimestampMs(createdMs)
        .setLastModifiedTimestampMs(lastMs)
        .setStatus(status);
    JobUpdateDetails builder = update.newBuilder();
    builder.getUpdate().getSummary().setState(state);
    return IJobUpdateDetails.build(builder);
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

  private IJobUpdateDetails updateJobDetails(IJobUpdateDetails update, IJobUpdateEvent event) {
    return updateJobDetails(
        update,
        ImmutableList.of(event),
        ImmutableList.of());
  }

  private IJobUpdateDetails updateJobDetails(
      IJobUpdateDetails update,
      List<IJobUpdateEvent> jobEvents,
      List<IJobInstanceUpdateEvent> instanceEvents) {

    return IJobUpdateDetails.build(update.newBuilder()
        .setUpdateEvents(IJobUpdateEvent.toBuildersList(jobEvents))
        .setInstanceEvents(IJobInstanceUpdateEvent.toBuildersList(instanceEvents)));
  }

  private static IJobUpdateSummary makeSummary(IJobUpdateKey key, String user) {
    return IJobUpdateSummary.build(new JobUpdateSummary()
        .setKey(key.newBuilder())
        .setUser(user)
        .setMetadata(METADATA));
  }

  private static IJobUpdateDetails makeJobUpdate(IJobUpdateKey key) {
    return IJobUpdateDetails.build(new JobUpdateDetails()
        .setUpdateEvents(ImmutableList.of(FIRST_EVENT.newBuilder()))
        .setUpdate(new JobUpdate()
            .setInstructions(makeJobUpdateInstructions().newBuilder())
            .setSummary(makeSummary(key, "user").newBuilder())));
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
