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
package org.apache.aurora.scheduler.updater;

import java.util.Set;

import com.google.common.collect.ImmutableRangeSet;

import org.apache.aurora.gen.InstanceTaskConfig;
import org.apache.aurora.gen.Range;

/**
 * Utility functions for job updates.
 */
public final class Updates {
  private Updates() {
    // Utility class.
  }

  /**
   * Creates a range set representing all instance IDs represented by a set of instance
   * configurations included in a job update.
   *
   * @param configs Job update components.
   * @return A range set representing the instance IDs mentioned in instance groupings.
   */
  public static ImmutableRangeSet<Integer> getInstanceIds(Set<InstanceTaskConfig> configs) {
    ImmutableRangeSet.Builder<Integer> builder = ImmutableRangeSet.builder();
    for (InstanceTaskConfig config : configs) {
      for (Range range : config.getInstances()) {
        builder.add(com.google.common.collect.Range.closed(range.getFirst(), range.getLast()));
      }
    }

    return builder.build();
  }
}
