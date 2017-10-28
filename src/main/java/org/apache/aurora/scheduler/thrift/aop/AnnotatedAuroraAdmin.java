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
package org.apache.aurora.scheduler.thrift.aop;

import java.util.Set;

import javax.annotation.Nullable;

import org.apache.aurora.gen.AuroraAdmin;
import org.apache.aurora.gen.InstanceKey;
import org.apache.aurora.gen.JobConfiguration;
import org.apache.aurora.gen.JobKey;
import org.apache.aurora.gen.JobUpdateKey;
import org.apache.aurora.gen.JobUpdateRequest;
import org.apache.aurora.gen.Response;
import org.apache.aurora.scheduler.http.api.security.AuthorizingParam;

/**
 * Extension of the generated thrift interface with Java annotations, for example {@link Nullable}
 * and {@link AuthorizingParam}.
 *
 * When adding new methods to api.thrift you should add, at the very least, {@link Nullable}
 * annotations for them here as well.
 *
 * TODO(ksweeney): Investigate adding other (JSR303) validation annotations here as well.
 *
 * TODO(ksweeney): Consider disallowing throwing subclasses by removing {@code throws TException}.
 */
public interface AnnotatedAuroraAdmin extends AuroraAdmin.Iface {
  @Override
  Response createJob(@AuthorizingParam @Nullable JobConfiguration description);

  @Override
  Response scheduleCronJob(
      @AuthorizingParam @Nullable JobConfiguration description);

  @Override
  Response descheduleCronJob(@AuthorizingParam @Nullable JobKey job);

  @Override
  Response startCronJob(
      @AuthorizingParam @Nullable JobKey job);

  @Override
  Response restartShards(
      @AuthorizingParam @Nullable JobKey job,
      @Nullable Set<Integer> shardIds);

  @Override
  Response killTasks(
      @AuthorizingParam @Nullable JobKey job,
      @Nullable Set<Integer> instances,
      @Nullable String message);

  @Override
  Response addInstances(
      @AuthorizingParam @Nullable InstanceKey key,
      int count);

  @Override
  Response replaceCronTemplate(
      @AuthorizingParam @Nullable JobConfiguration config);

  @Override
  Response startJobUpdate(
      @AuthorizingParam @Nullable JobUpdateRequest request,
      @Nullable String message);

  @Override
  Response pauseJobUpdate(
      @AuthorizingParam @Nullable JobUpdateKey key,
      @Nullable String message);

  @Override
  Response resumeJobUpdate(
      @AuthorizingParam @Nullable JobUpdateKey key,
      @Nullable String message);

  @Override
  Response abortJobUpdate(
      @AuthorizingParam @Nullable JobUpdateKey key,
      @Nullable String message);

  @Override
  Response pulseJobUpdate(
      @AuthorizingParam @Nullable JobUpdateKey key);

  @Override
  Response rollbackJobUpdate(
      @AuthorizingParam @Nullable JobUpdateKey key,
      @Nullable String message);
}
