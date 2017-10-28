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
package org.apache.aurora.scheduler.storage.db.views;

import java.util.List;
import java.util.Set;

import org.apache.aurora.GuavaUtils;
import org.apache.aurora.common.collections.Pair;
import org.apache.aurora.gen.ResourceAggregate;
import org.apache.aurora.scheduler.resources.ResourceType;
import org.apache.aurora.gen.Resource;
import org.apache.aurora.gen.ResourceAggregate;

import static org.apache.aurora.GuavaUtils.toImmutableSet;

public final class DBResourceAggregate {
  private double numCpus;
  private long ramMb;
  private long diskMb;
  private List<DBResource> resources;

  private DBResourceAggregate() {
  }

  public ResourceAggregate toThrift() {
    return new ResourceAggregate()
        .setNumCpus(numCpus)
        .setRamMb(ramMb)
        .setDiskMb(diskMb)
        .setResources(resources.stream().map(DBResource::toThrift).collect(toImmutableSet()));
  }

  public static List<Pair<Integer, String>> pairsFromResources(Set<Resource> resources) {
    return resources.stream()
        .map(e -> Pair.of(
            ResourceType.fromResource(e).getValue(),
            ResourceType.fromResource(e).getAuroraResourceConverter().stringify(e.getRawValue())))
        .collect(GuavaUtils.toImmutableList());
  }

  public ResourceAggregate toImmutable() {
    return ResourceAggregate.build(toThrift());
  }
}
