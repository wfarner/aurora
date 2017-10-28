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
package org.apache.aurora.scheduler.base;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import org.apache.aurora.gen.ScheduleStatus;
import org.apache.aurora.gen.ScheduledTask;
import org.junit.Test;

import static org.apache.aurora.gen.ScheduleStatus.FINISHED;
import static org.apache.aurora.gen.ScheduleStatus.RUNNING;
import static org.apache.aurora.scheduler.base.Tasks.getLatestActiveTask;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class TasksTest {

  @Test
  public void testOrderedStatusesForCompleteness() {
    // OrderedTaskStatuses should contain all ScheduleStatus values except INIT.
    assertEquals(
        ImmutableSet.copyOf(ScheduleStatus.values()),
        ImmutableSet.builder()
            .addAll(Tasks.ORDERED_TASK_STATUSES)
            .add(ScheduleStatus.INIT)
            .build());
  }

  @Test
  public void testLatestTransitionedTasks() {
    ScheduledTask f1 = makeTask(FINISHED, 100);
    ScheduledTask f2 = makeTask(FINISHED, 200);
    ScheduledTask f3 = makeTask(FINISHED, 300);
    ScheduledTask r1 = makeTask(RUNNING, 400);
    ScheduledTask r2 = makeTask(RUNNING, 500);
    ScheduledTask r3 = makeTask(RUNNING, 600);

    try {
      getLatestActiveTask(ImmutableList.of());
      fail("Should have thrown IllegalArgumentException.");
    } catch (IllegalArgumentException e) {
      // Expected when called with an empty task list.
    }

    assertLatestTask(r1, r1);
    assertLatestTask(r2, r1, r2);
    assertLatestTask(r2, r2, r1);
    assertLatestTask(r3, r2, r1, r3);
    assertLatestTask(r3, r3, r2, r1);

    assertLatestTask(f1, f1);
    assertLatestTask(f2, f1, f2);
    assertLatestTask(f2, f2, f1);
    assertLatestTask(f3, f2, f1, f3);
    assertLatestTask(f3, f3, f2, f1);

    assertLatestTask(r1, f2, f1, r1);
    assertLatestTask(r2, f2, f1, r1, r2);
    assertLatestTask(r3, f2, f1, f3, r1, r2, r3);
    assertLatestTask(r3, r1, r3, r2, f3, f1, f2);
  }

  private void assertLatestTask(ScheduledTask expectedLatest, ScheduledTask... tasks) {
    assertEquals(expectedLatest, getLatestActiveTask(ImmutableList.copyOf(tasks)));
  }

  private ScheduledTask makeTask(ScheduleStatus status, long timestamp) {
    return TaskTestUtil.addStateTransition(
        TaskTestUtil.makeTask("id-" + timestamp, TaskTestUtil.JOB),
        status,
        timestamp);
  }
}
