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

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;

import javax.sql.DataSource;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;

import org.apache.aurora.common.stats.Stats;
import org.apache.aurora.common.stats.StatsProvider;
import org.apache.aurora.common.util.testing.FakeBuildInfo;
import org.apache.aurora.common.util.testing.FakeClock;
import org.apache.aurora.gen.Attribute;
import org.apache.aurora.gen.CronCollisionPolicy;
import org.apache.aurora.gen.HostAttributes;
import org.apache.aurora.gen.Identity;
import org.apache.aurora.gen.InstanceTaskConfig;
import org.apache.aurora.gen.JobConfiguration;
import org.apache.aurora.gen.JobInstanceUpdateEvent;
import org.apache.aurora.gen.JobKey;
import org.apache.aurora.gen.JobUpdate;
import org.apache.aurora.gen.JobUpdateAction;
import org.apache.aurora.gen.JobUpdateDetails;
import org.apache.aurora.gen.JobUpdateEvent;
import org.apache.aurora.gen.JobUpdateInstructions;
import org.apache.aurora.gen.JobUpdateKey;
import org.apache.aurora.gen.JobUpdateSettings;
import org.apache.aurora.gen.JobUpdateState;
import org.apache.aurora.gen.JobUpdateStatus;
import org.apache.aurora.gen.JobUpdateSummary;
import org.apache.aurora.gen.Lock;
import org.apache.aurora.gen.LockKey;
import org.apache.aurora.gen.MaintenanceMode;
import org.apache.aurora.gen.Range;
import org.apache.aurora.gen.storage.QuotaConfiguration;
import org.apache.aurora.gen.storage.SchedulerMetadata;
import org.apache.aurora.gen.storage.Snapshot;
import org.apache.aurora.gen.storage.StoredCronJob;
import org.apache.aurora.gen.storage.StoredJobUpdateDetails;
import org.apache.aurora.scheduler.base.JobKeys;
import org.apache.aurora.scheduler.base.TaskTestUtil;
import org.apache.aurora.scheduler.resources.ResourceBag;
import org.apache.aurora.scheduler.storage.Storage;
import org.apache.aurora.scheduler.storage.db.DbModule;
import org.apache.aurora.scheduler.storage.db.DbStorage;
import org.apache.aurora.scheduler.storage.db.DbUtil;
import org.apache.aurora.gen.HostAttributes;
import org.apache.aurora.gen.JobConfiguration;
import org.apache.aurora.gen.JobKey;
import org.apache.aurora.gen.JobUpdateDetails;
import org.apache.aurora.gen.JobUpdateKey;
import org.apache.aurora.gen.Lock;
import org.apache.aurora.gen.ResourceAggregate;
import org.apache.aurora.gen.ScheduledTask;
import org.apache.aurora.gen.TaskConfig;
import org.apache.aurora.scheduler.storage.mem.MemStorageModule;
import org.apache.aurora.scheduler.testing.FakeStatsProvider;
import org.junit.Test;

import static org.apache.aurora.common.util.testing.FakeBuildInfo.generateBuildInfo;
import static org.apache.aurora.scheduler.resources.ResourceManager.aggregateFromBag;
import static org.apache.aurora.scheduler.storage.Storage.MutateWork.NoResult;
import static org.apache.aurora.scheduler.storage.log.SnapshotStoreImpl.SNAPSHOT_RESTORE;
import static org.apache.aurora.scheduler.storage.log.SnapshotStoreImpl.SNAPSHOT_SAVE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class SnapshotStoreImplIT {

  private static final long NOW = 10335463456L;
  private static final JobKey JOB_KEY = JobKeys.from("role", "env", "job");

  private Storage storage;
  private SnapshotStoreImpl snapshotStore;

  private void setUpStore() {
    Injector injector = Guice.createInjector(
        new MemStorageModule(),
        new AbstractModule() {
          @Override
          protected void configure() {
            bind(StatsProvider.class).toInstance(new FakeStatsProvider());
          }
        });
    storage = injector.getInstance(Storage.class);

    FakeClock clock = new FakeClock();
    clock.setNowMillis(NOW);
    snapshotStore = new SnapshotStoreImpl(
        generateBuildInfo(),
        clock,
        storage,
        TaskTestUtil.THRIFT_BACKFILL);
    Stats.flush();
  }

  private static Snapshot makeComparable(Snapshot snapshot) {
    Snapshot copy = snapshot.deepCopy();
    // Ignore DB snapshot. It will be tested by asserting the DB data.
    copy.unsetDbScript();
    return copy;
  }

  @Test
  public void testNoDBTaskStore() {
    setUpStore();
    populateStore(storage);

    Snapshot snapshot1 = snapshotStore.createSnapshot();
    assertEquals(expected(), makeComparable(snapshot1));
    assertSnapshotSaveStats(1L);

    snapshotStore.applySnapshot(snapshot1);
    Snapshot snapshot2 = snapshotStore.createSnapshot();
    assertEquals(expected(), makeComparable(snapshot2));
    assertEquals(makeComparable(snapshot1), makeComparable(snapshot2));
    assertSnapshotRestoreStats(1L);
    assertSnapshotSaveStats(2L);
  }

  @Test
  public void testMigrateFromDBStores() {
    // Produce a snapshot from DbStorage, populating the dbScript field.
    Injector injector = DbUtil.createStorageInjector(DbModule.testModuleWithWorkQueue());
    DbStorage dbStorage = injector.getInstance(DbStorage.class);
    populateStore(dbStorage);

    Snapshot dbScriptSnapshot = new Snapshot();
    try (Connection c = ((DataSource) dbStorage.getUnsafeStoreAccess()).getConnection()) {
      try (PreparedStatement ps = c.prepareStatement("SCRIPT")) {
        try (ResultSet rs = ps.executeQuery()) {
          ImmutableList.Builder<String> builder = ImmutableList.builder();
          while (rs.next()) {
            String columnValue = rs.getString("SCRIPT");
            builder.add(columnValue + "\n");
          }
          dbScriptSnapshot.setDbScript(builder.build());
        }
      }
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }

    // Verify that the dbScript snapshot can be loaded into a storage, and the resulting snapshot
    // fills thrift fields.
    setUpStore();
    snapshotStore.applySnapshot(dbScriptSnapshot);
    Snapshot snapshot2 = snapshotStore.createSnapshot();
    assertEquals(expected(), makeComparable(snapshot2));
    assertSnapshotRestoreStats(2L);
    assertSnapshotSaveStats(2L);
  }

  @Test
  public void testBackfill() {
    setUpStore();
    snapshotStore.applySnapshot(makeNonBackfilled());

    Snapshot backfilled = snapshotStore.createSnapshot();
    assertEquals(expected(), makeComparable(backfilled));
    assertSnapshotRestoreStats(1L);
    assertSnapshotSaveStats(1L);
  }

  private static final ScheduledTask TASK = TaskTestUtil.makeTask("id", JOB_KEY);
  private static final TaskConfig TASK_CONFIG = TaskTestUtil.makeConfig(JOB_KEY);
  private static final JobConfiguration CRON_JOB = JobConfiguration.build(new JobConfiguration()
      .setKey(new JobKey("owner", "env", "name"))
      .setOwner(new Identity("user"))
      .setCronSchedule("* * * * *")
      .setCronCollisionPolicy(CronCollisionPolicy.KILL_EXISTING)
      .setInstanceCount(1)
      .setTaskConfig(TASK_CONFIG.newBuilder()));
  private static final String ROLE = "role";
  private static final ResourceAggregate QUOTA =
      ThriftBackfill.backfillResourceAggregate(aggregateFromBag(ResourceBag.LARGE).newBuilder());
  private static final IHostAttributes ATTRIBUTES = IHostAttributes.build(
      new HostAttributes("host", ImmutableSet.of(new Attribute("attr", ImmutableSet.of("value"))))
          .setMode(MaintenanceMode.NONE)
          .setSlaveId("slave id"));
  private static final String FRAMEWORK_ID = "framework_id";
  private static final Map<String, String> METADATA = ImmutableMap.of(
          FakeBuildInfo.DATE, FakeBuildInfo.DATE,
          FakeBuildInfo.GIT_REVISION, FakeBuildInfo.GIT_REVISION,
          FakeBuildInfo.GIT_TAG, FakeBuildInfo.GIT_TAG);
  private static final ILock LOCK = ILock.build(new Lock()
      .setKey(LockKey.job(JobKeys.from("role", "env", "job").newBuilder()))
      .setToken("lockId")
      .setUser("testUser")
      .setTimestampMs(12345L));
  private static final JobUpdateKey UPDATE_ID =
      JobUpdateKey.build(new JobUpdateKey(JOB_KEY.newBuilder(), "updateId1"));
  private static final JobUpdateDetails UPDATE = IJobUpdateDetails.build(new JobUpdateDetails()
      .setUpdate(new JobUpdate()
          .setInstructions(new JobUpdateInstructions()
              .setDesiredState(new InstanceTaskConfig()
                  .setTask(TASK_CONFIG.newBuilder())
                  .setInstances(ImmutableSet.of(new Range(0, 7))))
              .setInitialState(ImmutableSet.of(
                  new InstanceTaskConfig()
                      .setInstances(ImmutableSet.of(new Range(0, 1)))
                      .setTask(TASK_CONFIG.newBuilder())))
              .setSettings(new JobUpdateSettings()
                  .setBlockIfNoPulsesAfterMs(500)
                  .setUpdateGroupSize(1)
                  .setMaxPerInstanceFailures(1)
                  .setMaxFailedInstances(1)
                  .setMinWaitInInstanceRunningMs(200)
                  .setRollbackOnFailure(true)
                  .setWaitForBatchCompletion(true)
                  .setUpdateOnlyTheseInstances(ImmutableSet.of(new Range(0, 0)))))
          .setSummary(new JobUpdateSummary()
              .setState(new JobUpdateState().setStatus(JobUpdateStatus.ERROR))
              .setUser("user")
              .setKey(UPDATE_ID.newBuilder())))
      .setUpdateEvents(ImmutableList.of(new JobUpdateEvent()
          .setUser("user")
          .setMessage("message")
          .setStatus(JobUpdateStatus.ERROR)))
      .setInstanceEvents(ImmutableList.of(new JobInstanceUpdateEvent()
          .setAction(JobUpdateAction.INSTANCE_UPDATED))));

  private Snapshot expected() {
    return new Snapshot()
        .setTimestamp(NOW)
        .setTasks(ImmutableSet.of(TASK.newBuilder()))
        .setQuotaConfigurations(ImmutableSet.of(new QuotaConfiguration(ROLE, QUOTA.newBuilder())))
        .setHostAttributes(ImmutableSet.of(ATTRIBUTES.newBuilder()))
        .setCronJobs(ImmutableSet.of(new StoredCronJob(CRON_JOB.newBuilder())))
        .setSchedulerMetadata(new SchedulerMetadata(FRAMEWORK_ID, METADATA))
        .setLocks(ImmutableSet.of(LOCK.newBuilder()))
        .setJobUpdateDetails(ImmutableSet.of(
            new StoredJobUpdateDetails(UPDATE.newBuilder(), LOCK.getToken())));
  }

  private Snapshot makeNonBackfilled() {
    return expected();
  }

  private void populateStore(Storage toPopulate) {
    toPopulate.write((NoResult.Quiet) store -> {
      store.getUnsafeTaskStore().saveTasks(ImmutableSet.of(TASK));
      store.getCronJobStore().saveAcceptedJob(CRON_JOB);
      store.getQuotaStore().saveQuota(ROLE, QUOTA);
      store.getAttributeStore().saveHostAttributes(ATTRIBUTES);
      store.getSchedulerStore().saveFrameworkId(FRAMEWORK_ID);
      store.getLockStore().saveLock(LOCK);
      store.getJobUpdateStore().saveJobUpdate(UPDATE.getUpdate(), Optional.of(LOCK.getToken()));
      store.getJobUpdateStore().saveJobUpdateEvent(
          UPDATE.getUpdate().getSummary().getKey(),
          UPDATE.getUpdateEvents().get(0));
      store.getJobUpdateStore().saveJobInstanceUpdateEvent(
          UPDATE.getUpdate().getSummary().getKey(),
          UPDATE.getInstanceEvents().get(0)
      );
    });
  }

  private void assertSnapshotSaveStats(long count) {
    for (String stat : snapshotStore.snapshotFieldNames()) {
      assertEquals(count, Stats.getVariable(SNAPSHOT_SAVE + stat + "_events").read());
      assertNotNull(Stats.getVariable(SNAPSHOT_SAVE + stat + "_nanos_total"));
    }
  }

  private void assertSnapshotRestoreStats(long count) {
    for (String stat : snapshotStore.snapshotFieldNames()) {
      assertEquals(count, Stats.getVariable(SNAPSHOT_RESTORE + stat + "_events").read());
      assertNotNull(Stats.getVariable(SNAPSHOT_RESTORE + stat + "_nanos_total"));
    }
  }
}
