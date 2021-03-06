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

import java.lang.Exception;
import java.lang.NullPointerException;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.junit.Before;
import org.junit.Test;

import org.apache.aurora.common.quantity.Amount;
import org.apache.aurora.common.quantity.Time;
import org.apache.aurora.common.testing.easymock.EasyMockTest;

import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class ExceptionHandlingScheduledExecutorServiceTest extends EasyMockTest {
  private static final RuntimeException EXCEPTION = new RuntimeException();

  private ScheduledExecutorService executorService;
  private Thread.UncaughtExceptionHandler signallingHandler;

  @Before
  public void setUp() throws Exception {
    signallingHandler = createMock(Thread.UncaughtExceptionHandler.class);
    executorService = MoreExecutors.exceptionHandlingExecutor(
        Executors.newSingleThreadScheduledExecutor(), signallingHandler);

    ExecutorServiceShutdown executorServiceShutdown = new ExecutorServiceShutdown(
        executorService, Amount.of(3L, Time.SECONDS));
    addTearDown(executorServiceShutdown::execute);
  }

  @Test
  public void testSubmitRunnable() throws Exception {
    signallingHandler.uncaughtException(anyObject(Thread.class), eq(EXCEPTION));
    Runnable runnable = createMock(Runnable.class);
    runnable.run();
    expectLastCall().andThrow(EXCEPTION);

    control.replay();

    try {
      executorService.submit(runnable).get();
      fail(EXCEPTION.getClass().getSimpleName() + " should be thrown.");
    } catch (ExecutionException e) {
      assertEquals(EXCEPTION, e.getCause());
    }
  }

  @Test
  public void testSubmitCallable() throws Exception {
    signallingHandler.uncaughtException(anyObject(Thread.class), eq(EXCEPTION));
    Callable<Void> callable = createMock(new Clazz<Callable<Void>>() {});
    expect(callable.call()).andThrow(EXCEPTION);

    control.replay();

    try {
      executorService.submit(callable).get();
      fail(EXCEPTION.getClass().getSimpleName() + " should be thrown.");
    } catch (ExecutionException e) {
      assertEquals(EXCEPTION, e.getCause());
    }
  }

  @Test
  public void testScheduleAtFixedRate() throws Exception {
    signallingHandler.uncaughtException(anyObject(Thread.class), eq(EXCEPTION));
    Runnable runnable = createMock(Runnable.class);
    runnable.run();
    expectLastCall().andThrow(EXCEPTION);

    control.replay();

    try {
      executorService.scheduleAtFixedRate(runnable, 0, 10, TimeUnit.MILLISECONDS).get();
      fail(EXCEPTION.getClass().getSimpleName() + " should be thrown.");
    } catch (ExecutionException e) {
      assertEquals(EXCEPTION, e.getCause());
    }
  }

  @Test
  public void testScheduleWithFixedDelay() throws Exception {
    signallingHandler.uncaughtException(anyObject(Thread.class), eq(EXCEPTION));
    Runnable runnable = createMock(Runnable.class);
    runnable.run();
    expectLastCall().andThrow(EXCEPTION);

    control.replay();

    try {
      executorService.scheduleWithFixedDelay(runnable, 0, 10, TimeUnit.MILLISECONDS).get();
      fail(EXCEPTION.getClass().getSimpleName() + " should be thrown.");
    } catch (ExecutionException e) {
      assertEquals(EXCEPTION, e.getCause());
    }
  }

  @Test(expected = NullPointerException.class)
  public void testNullHandler() throws Exception {
    control.replay();
    MoreExecutors.exceptionHandlingExecutor(executorService, null);
  }
}
