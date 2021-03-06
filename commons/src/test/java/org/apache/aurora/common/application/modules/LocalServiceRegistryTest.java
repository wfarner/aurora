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
package org.apache.aurora.common.application.modules;

import java.net.InetSocketAddress;
import java.util.Map;
import java.util.Set;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.inject.Provider;

import org.junit.Before;
import org.junit.Test;

import org.apache.aurora.common.application.ShutdownRegistry;
import org.apache.aurora.common.application.modules.LifecycleModule.LaunchException;
import org.apache.aurora.common.application.modules.LifecycleModule.ServiceRunner;
import org.apache.aurora.common.application.modules.LocalServiceRegistry.LocalService;
import org.apache.aurora.common.base.Commands;
import org.apache.aurora.common.testing.easymock.EasyMockTest;

import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

/**
 * @author William Farner
 */
public class LocalServiceRegistryTest extends EasyMockTest {

  private static final Function<InetSocketAddress, Integer> INET_TO_PORT =
      new Function<InetSocketAddress, Integer>() {
        @Override public Integer apply(InetSocketAddress address) {
          return address.getPort();
        }
      };

  private static final String A = "a";
  private static final String B = "b";
  private static final String C = "c";

  private ServiceRunner runner1;
  private ServiceRunner runner2;
  private Provider<Set<ServiceRunner>> serviceProvider;
  private ShutdownRegistry shutdownRegistry;
  private LocalServiceRegistry registry;

  @Before
  public void setUp() {
    runner1 = createMock(ServiceRunner.class);
    runner2 = createMock(ServiceRunner.class);
    serviceProvider = createMock(new Clazz<Provider<Set<ServiceRunner>>>() { });
    shutdownRegistry = createMock(ShutdownRegistry.class);
    registry = new LocalServiceRegistry(serviceProvider, shutdownRegistry);
  }

  @Test
  public void testCreate() throws LaunchException {
    expect(serviceProvider.get()).andReturn(ImmutableSet.of(runner1));
    expect(runner1.launch()).andReturn(auxiliary(A, 2));
    shutdownRegistry.addAction(Commands.NOOP);

    control.replay();

    checkPorts(ImmutableMap.of(A, 2));
  }

  private LocalService auxiliary(String name, int port) {
    return LocalService.auxiliaryService(name, port, Commands.NOOP);
  }

  private LocalService auxiliary(Set<String> names, int port) {
    return LocalService.auxiliaryService(names, port, Commands.NOOP);
  }

  @Test
  public void testNoPrimary() throws LaunchException {
    expect(serviceProvider.get()).andReturn(ImmutableSet.of(runner1));
    expect(runner1.launch()).andReturn(auxiliary(A, 2));
    shutdownRegistry.addAction(Commands.NOOP);
    expectLastCall().times(1);

    control.replay();

    assertFalse(registry.getPrimarySocket().isPresent());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testDuplicateName() throws LaunchException {
    expect(serviceProvider.get()).andReturn(ImmutableSet.of(runner1, runner2));
    expect(runner1.launch()).andReturn(auxiliary(A, 1));
    expect(runner2.launch()).andReturn(auxiliary(A, 2));
    shutdownRegistry.addAction(Commands.NOOP);
    expectLastCall().times(2);

    control.replay();

    registry.getPrimarySocket();
  }

  @Test
  public void testAllowsPortReuse() throws LaunchException {
    expect(serviceProvider.get()).andReturn(ImmutableSet.of(runner1, runner2));
    expect(runner1.launch()).andReturn(auxiliary(A, 2));
    expect(runner2.launch()).andReturn(auxiliary(B, 2));
    shutdownRegistry.addAction(Commands.NOOP);
    expectLastCall().times(2);

    control.replay();

    checkPorts(ImmutableMap.of(A, 2, B, 2));
  }

  @Test
  public void testMultiNameBreakout() throws LaunchException {
    expect(serviceProvider.get()).andReturn(ImmutableSet.of(runner1, runner2));
    expect(runner1.launch()).andReturn(auxiliary(A, 2));
    expect(runner2.launch()).andReturn(auxiliary(ImmutableSet.of(B, C), 6));
    shutdownRegistry.addAction(Commands.NOOP);
    expectLastCall().times(2);

    control.replay();

    checkPorts(ImmutableMap.of(A, 2, B, 6, C, 6));
  }

  private void checkPorts(Map<String, Integer> expected) {
    assertEquals(Optional.<InetSocketAddress>absent(), registry.getPrimarySocket());
    assertEquals(expected, Maps.transformValues(registry.getAuxiliarySockets(), INET_TO_PORT));
  }
}
