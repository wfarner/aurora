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
package org.apache.aurora.common.util.concurrent;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;

/**
 * An executor service that delegates to another executor service, invoking an uncaught
 * exception handler if any exceptions are thrown in submitted work.
 *
 * @see MoreExecutors
 */
class ExceptionHandlingExecutorService extends ForwardingExecutorService<ExecutorService> {
  private final Supplier<Thread.UncaughtExceptionHandler> handler;

  ExceptionHandlingExecutorService(
      ExecutorService delegate,
      Supplier<Thread.UncaughtExceptionHandler> handler) {

    super(delegate);
    this.handler = Preconditions.checkNotNull(handler);
  }

  @Override
  public <T> Future<T> submit(Callable<T> task) {
    return super.submit(TaskConverter.alertingCallable(task, handler));
  }

  @Override
  public <T> Future<T> submit(Runnable task, T result) {
    return super.submit(TaskConverter.alertingRunnable(task, handler), result);
  }

  @Override
  public Future<?> submit(Runnable task) {
    return super.submit(TaskConverter.alertingRunnable(task, handler));
  }

  @Override
  public <T> List<Future<T>> invokeAll(
      Collection<? extends Callable<T>> tasks) throws InterruptedException {

    return super.invokeAll(TaskConverter.alertingCallables(tasks, handler));
  }

  @Override
  public <T> List<Future<T>> invokeAll(
      Collection<? extends Callable<T>> tasks,
      long timeout,
      TimeUnit unit) throws InterruptedException {

    return super.invokeAll(TaskConverter.alertingCallables(tasks, handler), timeout, unit);
  }

  @Override
  public <T> T invokeAny(
      Collection<? extends Callable<T>> tasks) throws InterruptedException, ExecutionException {

    return super.invokeAny(TaskConverter.alertingCallables(tasks, handler));
  }

  @Override
  public <T> T invokeAny(
      Collection<? extends Callable<T>> tasks,
      long timeout,
      TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {

    return super.invokeAny(TaskConverter.alertingCallables(tasks, handler), timeout, unit);
  }
}
