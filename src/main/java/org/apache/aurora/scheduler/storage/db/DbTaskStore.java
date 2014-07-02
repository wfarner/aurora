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
package org.apache.aurora.scheduler.storage.db;

import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import com.twitter.common.args.Arg;
import com.twitter.common.args.CmdLine;
import com.twitter.common.inject.TimedInterceptor.Timed;
import com.twitter.common.quantity.Amount;
import com.twitter.common.quantity.Time;

import org.apache.aurora.scheduler.base.Query;
import org.apache.aurora.scheduler.base.Query.Builder;
import org.apache.aurora.scheduler.base.Tasks;
import org.apache.aurora.scheduler.storage.TaskStore;
import org.apache.aurora.scheduler.storage.entities.IScheduledTask;
import org.apache.aurora.scheduler.storage.entities.ITaskConfig;

import static java.util.Objects.requireNonNull;

/**
 *
 */
class DbTaskStore implements TaskStore.Mutable {

  private static final Logger LOG = Logger.getLogger(DbTaskStore.class.getName());

  @CmdLine(name = "slow_query_log_threshold",
      help = "Log all queries that take at least this long to execute.")
  private static final Arg<Amount<Long, Time>> SLOW_QUERY_LOG_THRESHOLD =
      Arg.create(Amount.of(25L, Time.MILLISECONDS));

  private final long slowQueryThresholdNanos = SLOW_QUERY_LOG_THRESHOLD.get().as(Time.NANOSECONDS);

  private final TaskMapper taskMapper;

  @Inject
  DbTaskStore(TaskMapper taskMapper) {
    this.taskMapper = requireNonNull(taskMapper);
  }

  @Timed("db_storage_fetch_tasks")
  @Override
  public ImmutableSet<IScheduledTask> fetchTasks(Builder query) {
    requireNonNull(query);

    long start = System.nanoTime();
    ImmutableSet<IScheduledTask> result = matches(query).toSet();
    long durationNanos = System.nanoTime() - start;
    Level level = durationNanos >= slowQueryThresholdNanos ? Level.INFO : Level.FINE;
    if (LOG.isLoggable(level)) {
      Long time = Amount.of(durationNanos, Time.NANOSECONDS).as(Time.MILLISECONDS);
      LOG.log(level, "Query took " + time + " ms: " + query.get());
    }

    return result;
  }

  @Timed("db_storage_fetch_tasks")
  @Override
  public void saveTasks(Set<IScheduledTask> tasks) {
    for (IScheduledTask task : tasks) {
      // TODO(wfarner): Need to merge nested objects.
      taskMapper.merge(task);
    }
  }

  @Timed("db_storage_delete_all_tasks")
  @Override
  public void deleteAllTasks() {
    taskMapper.truncate();
  }

  @Timed("db_storage_delete_tasks")
  @Override
  public void deleteTasks(Set<String> taskIds) {
    if (!taskIds.isEmpty()) {
      taskMapper.deleteTasks(taskIds);
    }
  }

  @Timed("db_storage_mutate_tasks")
  @Override
  public ImmutableSet<IScheduledTask> mutateTasks(
      Builder query,
      Function<IScheduledTask, IScheduledTask> mutator) {

    requireNonNull(query);
    requireNonNull(mutator);

    ImmutableSet.Builder<IScheduledTask> mutated = ImmutableSet.builder();
    for (IScheduledTask original : fetchTasks(query)) {
      IScheduledTask maybeMutated = mutator.apply(original);
      if (!original.equals(maybeMutated)) {
        Preconditions.checkState(
            Tasks.id(original).equals(Tasks.id(maybeMutated)),
            "A task's ID may not be mutated.");
        saveTasks(ImmutableSet.of(maybeMutated));
        mutated.add(maybeMutated);
      }
    }

    return mutated.build();
  }

  @Timed("db_storage_unsafe_modify_in_place")
  @Override
  public boolean unsafeModifyInPlace(String taskId, ITaskConfig taskConfiguration) {
    return false;
  }

  private FluentIterable<IScheduledTask> matches(Query.Builder query) {
    return null;
  }
}
