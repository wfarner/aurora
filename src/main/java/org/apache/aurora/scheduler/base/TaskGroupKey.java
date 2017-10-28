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

import java.util.Objects;

import org.apache.aurora.gen.TaskConfig;

import static java.util.Objects.requireNonNull;

/**
 * Identifier for a group of identical {@link TaskConfig} instances. Serves as a separation layer
 * between a task configuration and its scheduling purpose representation.
 */
public final class TaskGroupKey {
  private final TaskConfig canonicalTask;

  private TaskGroupKey(TaskConfig task) {
    this.canonicalTask = requireNonNull(task);
  }

  /**
   * Creates a {@code TaskGroupKey} from {@link TaskConfig}.
   *
   * @param task Task to create a {@code TaskGroupKey} from.
   * @return An instance of {@code TaskGroupKey}.
   */
  public static TaskGroupKey from(TaskConfig task) {
    return new TaskGroupKey(task);
  }

  /**
   * Gets {@link TaskConfig} the key created from.
   *
   * @return A task config.
   */
  public TaskConfig getTask() {
    return canonicalTask;
  }

  @Override
  public int hashCode() {
    return Objects.hash(canonicalTask);
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof TaskGroupKey)) {
      return false;
    }
    TaskGroupKey other = (TaskGroupKey) o;
    return Objects.equals(canonicalTask, other.canonicalTask);
  }

  @Override
  public String toString() {
    return JobKeys.canonicalString(canonicalTask.getJob());
  }
}
