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

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import org.apache.aurora.codec.ThriftBinaryCodec.CodingException;
import org.apache.aurora.gen.ScheduledTask;
import org.apache.aurora.gen.storage.Op;
import org.apache.aurora.gen.storage.RemoveTasks;
import org.apache.aurora.gen.storage.SaveHostAttributes;
import org.apache.aurora.gen.storage.SaveTasks;

/**
 * Partial implementation of {@link StreamTransaction} that handles accumulation of appended
 * operations and ensures call ordering.
 */
public abstract class AbstractStreamTransaction implements StreamTransaction {
  private final List<Op> ops = Lists.newArrayList();
  private final AtomicBoolean committed = new AtomicBoolean(false);

  @Override
  public final void commit() throws CodingException {
    Preconditions.checkState(!committed.getAndSet(true),
        "Can only call commit once per transaction.");

    if (ops.isEmpty()) {
      return;
    }

    doCommit(ops);
  }

  protected abstract void doCommit(List<Op> commit) throws CodingException;

  @Override
  public final StreamTransaction add(Op op) {
    Preconditions.checkState(!committed.get());

    Op prior = Iterables.getLast(ops, null);
    if (prior == null || !coalesce(prior, op)) {
      ops.add(op);
    }
    return this;
  }

  /**
   * Tries to coalesce a new op into the prior to compact the binary representation and increase
   * batching.
   *
   * @param prior The previous op.
   * @param next The next op to be added.
   * @return {@code true} if the next op was coalesced into the prior, {@code false} otherwise.
   */
  private static boolean coalesce(Op prior, Op next) {
    if (!prior.isSet() && !next.isSet()) {
      return false;
    }

    Op._Fields priorType = prior.getSetField();
    if (!priorType.equals(next.getSetField())) {
      return false;
    }

    switch (priorType) {
      case SAVE_FRAMEWORK_ID:
        prior.setSaveFrameworkId(next.getSaveFrameworkId());
        return true;
      case SAVE_TASKS:
        coalesce(prior.getSaveTasks(), next.getSaveTasks());
        return true;
      case REMOVE_TASKS:
        coalesce(prior.getRemoveTasks(), next.getRemoveTasks());
        return true;
      case SAVE_HOST_ATTRIBUTES:
        return coalesce(prior.getSaveHostAttributes(), next.getSaveHostAttributes());
      default:
        return false;
    }
  }

  private static void coalesce(SaveTasks prior, SaveTasks next) {
    if (next.isSetTasks()) {
      if (prior.isSetTasks()) {
        // It is an expected invariant that an operation may reference a task (identified by
        // task ID) no more than one time.  Therefore, to coalesce two SaveTasks operations,
        // the most recent task definition overrides the prior operation.
        Map<String, ScheduledTask> coalesced = Maps.newHashMap();
        for (ScheduledTask task : prior.getTasks()) {
          coalesced.put(task.getAssignedTask().getTaskId(), task);
        }
        for (ScheduledTask task : next.getTasks()) {
          coalesced.put(task.getAssignedTask().getTaskId(), task);
        }
        prior.setTasks(ImmutableSet.copyOf(coalesced.values()));
      } else {
        prior.setTasks(next.getTasks());
      }
    }
  }

  private static void coalesce(RemoveTasks prior, RemoveTasks next) {
    if (next.isSetTaskIds()) {
      if (prior.isSetTaskIds()) {
        prior.setTaskIds(ImmutableSet.<String>builder()
            .addAll(prior.getTaskIds())
            .addAll(next.getTaskIds())
            .build());
      } else {
        prior.setTaskIds(next.getTaskIds());
      }
    }
  }

  private static boolean coalesce(SaveHostAttributes prior, SaveHostAttributes next) {
    if (prior.getHostAttributes().getHost().equals(next.getHostAttributes().getHost())) {
      prior.getHostAttributes().setAttributes(next.getHostAttributes().getAttributes());
      return true;
    }
    return false;
  }
}
