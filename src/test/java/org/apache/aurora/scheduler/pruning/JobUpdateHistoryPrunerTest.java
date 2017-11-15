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

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import org.apache.aurora.common.quantity.Amount;
import org.apache.aurora.common.quantity.Time;
import org.apache.aurora.common.util.testing.FakeClock;
import org.apache.aurora.gen.JobUpdate;
import org.apache.aurora.gen.JobUpdateDetails;
import org.apache.aurora.gen.JobUpdateEvent;
import org.apache.aurora.gen.JobUpdateKey;
import org.apache.aurora.gen.JobUpdateState;
import org.apache.aurora.gen.JobUpdateStatus;
import org.apache.aurora.gen.JobUpdateSummary;
import org.apache.aurora.scheduler.base.JobKeys;
import org.apache.aurora.scheduler.pruning.JobUpdateHistoryPruner.HistoryPrunerSettings;
import org.apache.aurora.scheduler.storage.Storage;
import org.apache.aurora.scheduler.storage.Storage.MutateWork.NoResult;
import org.apache.aurora.scheduler.storage.entities.IJobKey;
import org.apache.aurora.scheduler.storage.entities.IJobUpdateDetails;
import org.apache.aurora.scheduler.storage.entities.IJobUpdateKey;
import org.apache.aurora.scheduler.storage.mem.MemStorageModule;
import org.apache.aurora.scheduler.testing.FakeStatsProvider;
import org.junit.Before;
import org.junit.Test;

import static org.apache.aurora.gen.JobUpdateStatus.ABORTED;
import static org.apache.aurora.gen.JobUpdateStatus.ERROR;
import static org.apache.aurora.gen.JobUpdateStatus.FAILED;
import static org.apache.aurora.gen.JobUpdateStatus.ROLLED_BACK;
import static org.apache.aurora.gen.JobUpdateStatus.ROLLING_BACK;
import static org.apache.aurora.gen.JobUpdateStatus.ROLLING_FORWARD;
import static org.apache.aurora.scheduler.base.TaskTestUtil.JOB;
import static org.junit.Assert.assertEquals;

public class JobUpdateHistoryPrunerTest {

  private Storage storage;

  @Before
  public void setUp() {
    storage = MemStorageModule.newEmptyStorage();
  }

  @Test
  public void testPruneHistory() {
    IJobUpdateKey updateId1 = makeKey("u11");
    IJobUpdateKey updateId2 = makeKey("u12");
    IJobUpdateKey updateId3 = makeKey("u13");
    IJobUpdateKey updateId4 = makeKey("u14");
    IJobKey job2 = JobKeys.from("testRole2", "testEnv2", "job2");
    IJobUpdateKey updateId5 = makeKey(job2, "u15");
    IJobUpdateKey updateId6 = makeKey(job2, "u16");
    IJobUpdateKey updateId7 = makeKey(job2, "u17");

    IJobUpdateDetails update1 = makeAndSave(updateId1, ROLLING_BACK, 123L, 123L);
    IJobUpdateDetails update2 = makeAndSave(updateId2, ABORTED, 124L, 124L);
    IJobUpdateDetails update3 = makeAndSave(updateId3, ROLLED_BACK, 125L, 125L);
    IJobUpdateDetails update4 = makeAndSave(updateId4, FAILED, 126L, 126L);
    IJobUpdateDetails update5 = makeAndSave(updateId5, ERROR, 123L, 123L);
    IJobUpdateDetails update6 = makeAndSave(updateId6, FAILED, 125L, 125L);
    IJobUpdateDetails update7 = makeAndSave(updateId7, ROLLING_FORWARD, 126L, 126L);

    long pruningThreshold = 120L;

    // No updates pruned.
    pruneHistory(3, pruningThreshold);
    assertRetainedUpdates(update1, update2, update3, update4, update5, update6, update7);

    // 1 update pruned.
    pruneHistory(2, pruningThreshold);
    assertRetainedUpdates(update1, update3, update4, update5, update6, update7);

    // 2 update pruned.
    pruneHistory(1, pruningThreshold);
    assertRetainedUpdates(update1, update4, update6, update7);

    // The oldest update is pruned.
    pruneHistory(1, 126L);
    assertRetainedUpdates(update1, update4, update7);

    // Nothing survives the 0 per job count.
    pruneHistory(0, pruningThreshold);
    assertRetainedUpdates(update1, update7);

    /*
    assertEquals(0L, statsProvider.getValue(JOB_UPDATES_PRUNED));
    executorClock.advance(Amount.of(1L, Time.MILLISECONDS));
    assertEquals(1L, statsProvider.getValue(JOB_UPDATES_PRUNED));
    executorClock.advance(Amount.of(1L, Time.MILLISECONDS));
    assertEquals(1L, statsProvider.getValue(JOB_UPDATES_PRUNED));
    */
  }

  private Optional<IJobUpdateDetails> getUpdate(IJobUpdateKey key) {
    return storage.read(store -> store.getJobUpdateStore().fetchJobUpdates(key));
  }

  private void pruneHistory(int retainCount, long pruningThresholdMs) {
    JobUpdateHistoryPruner pruner = new JobUpdateHistoryPruner(
        new FakeClock(),
        storage,
        new HistoryPrunerSettings(
            Amount.of(1L, Time.DAYS),
            Amount.of(pruningThresholdMs, Time.MILLISECONDS),
            retainCount),
        new FakeStatsProvider());
    pruner.runForTest();
  }

  private void assertRetainedUpdates(IJobUpdateDetails... updates) {
    storage.read(store -> {
      assertEquals(
          ImmutableSet.of(updates),
          store.getJobUpdateStore().fetchAllJobUpdateDetails());
      return null;
    });
  }

  private static IJobUpdateKey makeKey(String id) {
    return makeKey(JOB, id);
  }

  private static IJobUpdateKey makeKey(IJobKey job, String id) {
    return IJobUpdateKey.build(new JobUpdateKey().setJob(job.newBuilder()).setId(id));
  }

  private IJobUpdateDetails makeAndSave(
      IJobUpdateKey key,
      JobUpdateStatus status,
      long createdMs,
      long lastMs) {

    IJobUpdateDetails update = IJobUpdateDetails.build(new JobUpdateDetails()
        .setUpdateEvents(ImmutableList.of(
            new JobUpdateEvent(status, lastMs)
                .setUser("user")
                .setMessage("message")
        ))
        .setInstanceEvents(ImmutableList.of())
        .setUpdate(new JobUpdate()
            .setSummary(new JobUpdateSummary()
                .setKey(key.newBuilder())
                .setState(new JobUpdateState()
                    .setCreatedTimestampMs(createdMs)
                    .setLastModifiedTimestampMs(lastMs)
                    .setStatus(status)))));

    storage.write((NoResult.Quiet) storeProvider -> {
      storeProvider.getJobUpdateStore().saveJobUpdate(update);
    });
    return update;
  }
}
