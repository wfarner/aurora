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
package org.apache.aurora.scheduler.offers;

import java.util.List;
import java.util.concurrent.ScheduledExecutorService;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;

import org.apache.aurora.common.quantity.Amount;
import org.apache.aurora.common.quantity.Time;
import org.apache.aurora.common.testing.easymock.EasyMockTest;
import org.apache.aurora.common.util.testing.FakeTicker;
import org.apache.aurora.gen.HostAttributes;
import org.apache.aurora.gen.MaintenanceMode;
import org.apache.aurora.gen.ScheduledTask;
import org.apache.aurora.scheduler.HostOffer;
import org.apache.aurora.scheduler.base.TaskGroupKey;
import org.apache.aurora.scheduler.base.Tasks;
import org.apache.aurora.scheduler.events.PubsubEvent.DriverDisconnected;
import org.apache.aurora.scheduler.events.PubsubEvent.HostAttributesChanged;
import org.apache.aurora.scheduler.mesos.Driver;
import org.apache.aurora.scheduler.offers.Deferment.Noop;
import org.apache.aurora.scheduler.offers.OfferManager.OfferManagerImpl;
import org.apache.aurora.scheduler.resources.ResourceType;
import org.apache.aurora.scheduler.testing.FakeScheduledExecutor;
import org.apache.aurora.scheduler.testing.FakeStatsProvider;
import org.apache.mesos.v1.Protos;
import org.apache.mesos.v1.Protos.Filters;
import org.apache.mesos.v1.Protos.Offer.Operation;
import org.apache.mesos.v1.Protos.OfferID;
import org.apache.mesos.v1.Protos.TaskInfo;
import org.apache.mesos.v1.Protos.TimeInfo;
import org.apache.mesos.v1.Protos.Unavailability;
import org.junit.Before;
import org.junit.Test;

import static org.apache.aurora.gen.MaintenanceMode.DRAINING;
import static org.apache.aurora.gen.MaintenanceMode.NONE;
import static org.apache.aurora.scheduler.base.TaskTestUtil.JOB;
import static org.apache.aurora.scheduler.base.TaskTestUtil.makeTask;
import static org.apache.aurora.scheduler.offers.OfferManager.OfferManagerImpl.GLOBALLY_BANNED_OFFERS;
import static org.apache.aurora.scheduler.offers.OfferManager.OfferManagerImpl.OFFER_ACCEPT_RACES;
import static org.apache.aurora.scheduler.offers.OfferManager.OfferManagerImpl.OFFER_CANCEL_FAILURES;
import static org.apache.aurora.scheduler.offers.OfferManager.OfferManagerImpl.OUTSTANDING_OFFERS;
import static org.apache.aurora.scheduler.offers.OfferManager.OfferManagerImpl.STATICALLY_BANNED_OFFERS;
import static org.apache.aurora.scheduler.resources.ResourceTestUtil.mesosRange;
import static org.apache.aurora.scheduler.resources.ResourceTestUtil.mesosScalar;
import static org.apache.aurora.scheduler.resources.ResourceTestUtil.offer;
import static org.apache.aurora.scheduler.resources.ResourceType.CPUS;
import static org.apache.aurora.scheduler.resources.ResourceType.DISK_MB;
import static org.apache.aurora.scheduler.resources.ResourceType.PORTS;
import static org.apache.aurora.scheduler.resources.ResourceType.RAM_MB;
import static org.easymock.EasyMock.expectLastCall;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class OfferManagerImplTest extends EasyMockTest {

  private static final Amount<Long, Time> RETURN_DELAY = Amount.of(1L, Time.DAYS);
  private static final Amount<Long, Time> ONE_HOUR = Amount.of(1L, Time.HOURS);
  private static final String HOST_A = "HOST_A";
  private static final HostAttributes HOST_ATTRIBUTES_A =
      HostAttributes.builder().setMode(NONE).setHost(HOST_A).build();
  private static final HostOffer OFFER_A = new HostOffer(
      Offers.makeOffer("OFFER_A", HOST_A),
      HOST_ATTRIBUTES_A);
  private static final Protos.OfferID OFFER_A_ID = OFFER_A.getOffer().getId();
  private static final String HOST_B = "HOST_B";
  private static final HostOffer OFFER_B = new HostOffer(
      Offers.makeOffer("OFFER_B", HOST_B),
      HostAttributes.builder().setMode(NONE).build());
  private static final String HOST_C = "HOST_C";
  private static final HostOffer OFFER_C = new HostOffer(
      Offers.makeOffer("OFFER_C", HOST_C),
      HostAttributes.builder().setMode(NONE).build());
  private static final int PORT = 1000;
  private static final Protos.Offer MESOS_OFFER = offer(mesosRange(PORTS, PORT));
  private static final ScheduledTask TASK = makeTask("id", JOB);
  private static final TaskGroupKey GROUP_KEY = TaskGroupKey.from(TASK.getAssignedTask().getTask());
  private static final TaskInfo TASK_INFO = TaskInfo.newBuilder()
      .setName("taskName")
      .setTaskId(Protos.TaskID.newBuilder().setValue(Tasks.id(TASK)))
      .setAgentId(MESOS_OFFER.getAgentId())
      .build();
  private static Operation launch = Operation.newBuilder()
      .setType(Operation.Type.LAUNCH)
      .setLaunch(Operation.Launch.newBuilder().addTaskInfos(TASK_INFO))
      .build();
  private static final List<Operation> OPERATIONS = ImmutableList.of(launch);
  private static final long OFFER_FILTER_SECONDS = 0;
  private static final Filters OFFER_FILTER = Filters.newBuilder()
      .setRefuseSeconds(OFFER_FILTER_SECONDS)
      .build();
  private static final FakeTicker FAKE_TICKER = new FakeTicker();

  private Driver driver;
  private OfferManagerImpl offerManager;
  private FakeStatsProvider statsProvider;

  @Before
  public void setUp() {
    driver = createMock(Driver.class);
    OfferSettings offerSettings = new OfferSettings(
        Amount.of(OFFER_FILTER_SECONDS, Time.SECONDS),
        ImmutableList.of(OfferOrder.RANDOM),
        RETURN_DELAY,
        Long.MAX_VALUE,
        FAKE_TICKER
    );
    statsProvider = new FakeStatsProvider();
    offerManager = new OfferManagerImpl(driver, offerSettings, statsProvider, new Noop());
  }

  @Test
  public void testOffersSortedByUnavailability() {
    HostOffer hostOfferB = setUnavailability(OFFER_B, 1);
    long offerCStartTime = ONE_HOUR.as(Time.MILLISECONDS);
    HostOffer hostOfferC = setUnavailability(OFFER_C, offerCStartTime);

    control.replay();

    offerManager.addOffer(hostOfferB);
    offerManager.addOffer(OFFER_A);
    offerManager.addOffer(hostOfferC);

    List<HostOffer> actual = ImmutableList.copyOf(offerManager.getOffers());

    assertEquals(
        // hostOfferC has a further away start time, so it should be preferred.
        ImmutableList.of(OFFER_A, hostOfferC, hostOfferB),
        actual);
  }

  @Test
  public void testOffersSortedByMaintenance() throws Exception {
    // Ensures that non-DRAINING offers are preferred - the DRAINING offer would be tried last.

    HostOffer offerA = setMode(OFFER_A, DRAINING);
    HostOffer offerC = setMode(OFFER_C, DRAINING);

    driver.acceptOffers(OFFER_B.getOffer().getId(), OPERATIONS, OFFER_FILTER);
    expectLastCall();

    control.replay();

    offerManager.addOffer(offerA);
    assertEquals(1, statsProvider.getLongValue(OUTSTANDING_OFFERS));
    offerManager.addOffer(OFFER_B);
    assertEquals(2, statsProvider.getLongValue(OUTSTANDING_OFFERS));
    offerManager.addOffer(offerC);
    assertEquals(3, statsProvider.getLongValue(OUTSTANDING_OFFERS));
    assertEquals(
        ImmutableSet.of(OFFER_B, offerA, offerC),
        ImmutableSet.copyOf(offerManager.getOffers()));
    offerManager.launchTask(OFFER_B.getOffer().getId(), TASK_INFO);
    assertEquals(2, statsProvider.getLongValue(OUTSTANDING_OFFERS));
  }

  @Test
  public void hostAttributeChangeUpdatesOfferSorting() {
    control.replay();

    offerManager.hostAttributesChanged(new HostAttributesChanged(HOST_ATTRIBUTES_A));

    offerManager.addOffer(OFFER_A);
    offerManager.addOffer(OFFER_B);
    assertEquals(ImmutableSet.of(OFFER_A, OFFER_B), ImmutableSet.copyOf(offerManager.getOffers()));

    HostOffer offerA = setMode(OFFER_A, DRAINING);
    offerManager.hostAttributesChanged(new HostAttributesChanged(offerA.getAttributes()));
    assertEquals(ImmutableSet.of(OFFER_B, offerA), ImmutableSet.copyOf(offerManager.getOffers()));

    offerA = setMode(OFFER_A, NONE);
    HostOffer offerB = setMode(OFFER_B, DRAINING);
    offerManager.hostAttributesChanged(new HostAttributesChanged(offerA.getAttributes()));
    offerManager.hostAttributesChanged(new HostAttributesChanged(offerB.getAttributes()));
    assertEquals(ImmutableSet.of(OFFER_A, OFFER_B), ImmutableSet.copyOf(offerManager.getOffers()));
  }

  @Test
  public void testAddSameSlaveOffer() {
    driver.declineOffer(OFFER_A_ID, OFFER_FILTER);
    expectLastCall().times(2);

    control.replay();

    offerManager.addOffer(OFFER_A);
    assertEquals(1, statsProvider.getLongValue(OUTSTANDING_OFFERS));
    offerManager.addOffer(OFFER_A);
    assertEquals(0, statsProvider.getLongValue(OUTSTANDING_OFFERS));
  }

  @Test
  public void testGetOffersReturnsAllOffers() {
    control.replay();

    offerManager.addOffer(OFFER_A);
    assertEquals(OFFER_A, Iterables.getOnlyElement(offerManager.getOffers()));
    assertEquals(1, statsProvider.getLongValue(OUTSTANDING_OFFERS));

    offerManager.cancelOffer(OFFER_A_ID);
    assertEquals(0, statsProvider.getLongValue(OFFER_CANCEL_FAILURES));
    assertTrue(Iterables.isEmpty(offerManager.getOffers()));
    assertEquals(0, statsProvider.getLongValue(OUTSTANDING_OFFERS));
  }

  @Test
  public void testOfferFilteringDueToStaticBan() {
    control.replay();

    // Static ban ignored when now offers.
    offerManager.banOfferForTaskGroup(OFFER_A_ID, GROUP_KEY);
    assertEquals(0, statsProvider.getLongValue(STATICALLY_BANNED_OFFERS));
    offerManager.addOffer(OFFER_A);
    assertEquals(OFFER_A, Iterables.getOnlyElement(offerManager.getOffers(GROUP_KEY)));
    assertEquals(OFFER_A, Iterables.getOnlyElement(offerManager.getOffers()));

    // Add static ban.
    offerManager.banOfferForTaskGroup(OFFER_A_ID, GROUP_KEY);
    assertEquals(1, statsProvider.getLongValue(STATICALLY_BANNED_OFFERS));
    assertEquals(OFFER_A, Iterables.getOnlyElement(offerManager.getOffers()));
    assertTrue(Iterables.isEmpty(offerManager.getOffers(GROUP_KEY)));
  }

  @Test
  public void testStaticBanExpiresAfterMaxHoldTime() throws InterruptedException {
    control.replay();

    offerManager.addOffer(OFFER_A);
    offerManager.banOfferForTaskGroup(OFFER_A_ID, GROUP_KEY);
    assertEquals(OFFER_A, Iterables.getOnlyElement(offerManager.getOffers()));
    assertTrue(Iterables.isEmpty(offerManager.getOffers(GROUP_KEY)));
    assertEquals(1, statsProvider.getLongValue(STATICALLY_BANNED_OFFERS));

    // Make sure the static ban expires after maximum amount of time an offer is held.
    FAKE_TICKER.advance(RETURN_DELAY);
    offerManager.cleanupStaticBans();
    assertEquals(OFFER_A, Iterables.getOnlyElement(offerManager.getOffers(GROUP_KEY)));
    assertEquals(0, statsProvider.getLongValue(STATICALLY_BANNED_OFFERS));
  }

  @Test
  public void testStaticBanIsClearedOnDriverDisconnect() {
    control.replay();

    offerManager.addOffer(OFFER_A);
    offerManager.banOfferForTaskGroup(OFFER_A_ID, GROUP_KEY);
    assertEquals(OFFER_A, Iterables.getOnlyElement(offerManager.getOffers()));
    assertTrue(Iterables.isEmpty(offerManager.getOffers(GROUP_KEY)));
    assertEquals(1, statsProvider.getLongValue(STATICALLY_BANNED_OFFERS));

    // Make sure the static ban is cleared when driver is disconnected.
    offerManager.driverDisconnected(new DriverDisconnected());
    assertEquals(0, statsProvider.getLongValue(STATICALLY_BANNED_OFFERS));
    offerManager.addOffer(OFFER_A);
    assertEquals(OFFER_A, Iterables.getOnlyElement(offerManager.getOffers(GROUP_KEY)));
  }

  @Test
  public void testGetOffer() {
    control.replay();

    offerManager.addOffer(OFFER_A);
    assertEquals(Optional.of(OFFER_A), offerManager.getOffer(OFFER_A.getOffer().getAgentId()));
    assertEquals(1, statsProvider.getLongValue(OUTSTANDING_OFFERS));
  }

  @Test(expected = OfferManager.LaunchException.class)
  public void testAcceptOffersDriverThrows() throws OfferManager.LaunchException {
    driver.acceptOffers(OFFER_A_ID, OPERATIONS, OFFER_FILTER);
    expectLastCall().andThrow(new IllegalStateException());

    control.replay();

    offerManager.addOffer(OFFER_A);
    offerManager.launchTask(OFFER_A_ID, TASK_INFO);
  }

  @Test
  public void testLaunchTaskOfferRaceThrows() {
    control.replay();
    try {
      offerManager.launchTask(OFFER_A_ID, TASK_INFO);
      fail("Method invocation is expected to throw exception.");
    } catch (OfferManager.LaunchException e) {
      assertEquals(1, statsProvider.getLongValue(OFFER_ACCEPT_RACES));
    }
  }

  @Test
  public void testFlushOffers() {
    control.replay();

    offerManager.addOffer(OFFER_A);
    offerManager.addOffer(OFFER_B);
    assertEquals(2, statsProvider.getLongValue(OUTSTANDING_OFFERS));
    offerManager.driverDisconnected(new DriverDisconnected());
    assertEquals(0, statsProvider.getLongValue(OUTSTANDING_OFFERS));
  }

  @Test
  public void testCancelFailure() {
    control.replay();

    offerManager.cancelOffer(OFFER_A.getOffer().getId());
    assertEquals(1, statsProvider.getLongValue(OFFER_CANCEL_FAILURES));
  }

  @Test
  public void testBanAndUnbanOffer() {
    control.replay();

    // After adding a banned offer, user can see it is in OUTSTANDING_OFFERS but cannot retrieve it.
    offerManager.banOffer(OFFER_A_ID);
    offerManager.addOffer(OFFER_A);
    assertEquals(1, statsProvider.getLongValue(OUTSTANDING_OFFERS));
    assertEquals(1, statsProvider.getLongValue(GLOBALLY_BANNED_OFFERS));
    assertTrue(Iterables.isEmpty(offerManager.getOffers(GROUP_KEY)));

    offerManager.cancelOffer(OFFER_A_ID);
    offerManager.addOffer(OFFER_A);
    assertEquals(1, statsProvider.getLongValue(OUTSTANDING_OFFERS));
    assertEquals(0, statsProvider.getLongValue(GLOBALLY_BANNED_OFFERS));
    assertEquals(OFFER_A, Iterables.getOnlyElement(offerManager.getOffers(GROUP_KEY)));
  }

  private static HostOffer setUnavailability(HostOffer offer, long startMs) {
    Unavailability unavailability = Unavailability.newBuilder()
        .setStart(TimeInfo.newBuilder().setNanoseconds(startMs * 1000L)).build();
    return new HostOffer(
        offer.getOffer().toBuilder().setUnavailability(unavailability).build(),
        offer.getAttributes());
  }

  private static HostOffer setMode(HostOffer offer, MaintenanceMode mode) {
    return new HostOffer(
        offer.getOffer(),
        offer.getAttributes().mutate().setMode(mode).build());
  }

  private OfferManager createOrderedManager(List<OfferOrder> order) {
    OfferSettings settings =
        new OfferSettings(
            Amount.of(OFFER_FILTER_SECONDS, Time.SECONDS),
            order,
            RETURN_DELAY,
            Long.MAX_VALUE,
            FAKE_TICKER);
    return new OfferManagerImpl(driver, settings, statsProvider, new Noop());
  }

  @Test
  public void testCPUOrdering() {
    OfferManager cpuManager = createOrderedManager(ImmutableList.of(OfferOrder.CPU));

    HostOffer small = setMode(new HostOffer(
        offer(
            "host1",
            mesosScalar(CPUS, 1.0),
            mesosScalar(CPUS, 24.0, true),
            mesosScalar(RAM_MB, 1024)),
        HOST_ATTRIBUTES_A), DRAINING);

    HostOffer medium = setMode(new HostOffer(
        offer("host2", mesosScalar(CPUS, 5.0), mesosScalar(RAM_MB, 1024)),
        HOST_ATTRIBUTES_A), DRAINING);

    HostOffer large = setMode(new HostOffer(
        offer("host3", mesosScalar(CPUS, 10.0), mesosScalar(RAM_MB, 1024)),
        HOST_ATTRIBUTES_A), DRAINING);

    control.replay();

    cpuManager.addOffer(medium);
    cpuManager.addOffer(large);
    cpuManager.addOffer(small);

    assertEquals(ImmutableList.of(small, medium, large),
        ImmutableList.copyOf(cpuManager.getOffers(GROUP_KEY)));
    assertEquals(ImmutableList.of(small, medium, large),
        ImmutableList.copyOf(cpuManager.getOffers()));
  }

  @Test
  public void testRevocableCPUOrdering() {
    ResourceType.initializeEmptyCliArgsForTest();
    OfferManager cpuManager = createOrderedManager(ImmutableList.of(OfferOrder.REVOCABLE_CPU));

    HostOffer small = setMode(new HostOffer(
        offer(
            "host2",
            mesosScalar(CPUS, 5.0),
            mesosScalar(CPUS, 23.0, true),
            mesosScalar(RAM_MB, 1024)),
        HOST_ATTRIBUTES_A), DRAINING);

    HostOffer medium = setMode(new HostOffer(
        offer(
            "host1",
            mesosScalar(CPUS, 3.0),
            mesosScalar(CPUS, 24.0, true),
            mesosScalar(RAM_MB, 1024)),
        HOST_ATTRIBUTES_A), DRAINING);

    HostOffer large = setMode(new HostOffer(
        offer("host3", mesosScalar(CPUS, 1.0), mesosScalar(RAM_MB, 1024)),
        HOST_ATTRIBUTES_A), DRAINING);

    control.replay();

    cpuManager.addOffer(medium);
    cpuManager.addOffer(large);
    cpuManager.addOffer(small);

    assertEquals(ImmutableList.of(small, medium, large),
        ImmutableList.copyOf(cpuManager.getOffers(GROUP_KEY)));
    assertEquals(ImmutableList.of(small, medium, large),
        ImmutableList.copyOf(cpuManager.getOffers()));
  }

  @Test
  public void testDiskOrdering() {
    OfferManager cpuManager = createOrderedManager(ImmutableList.of(OfferOrder.DISK));

    HostOffer small = setMode(new HostOffer(
        offer("host1", mesosScalar(CPUS, 1), mesosScalar(RAM_MB, 1), mesosScalar(DISK_MB, 1.0)),
        HOST_ATTRIBUTES_A), DRAINING);

    HostOffer medium = setMode(new HostOffer(
        offer("host2", mesosScalar(CPUS, 1), mesosScalar(RAM_MB, 1), mesosScalar(DISK_MB, 5.0)),
        HOST_ATTRIBUTES_A), DRAINING);

    HostOffer large = setMode(new HostOffer(
        offer("host3", mesosScalar(CPUS, 1), mesosScalar(RAM_MB, 1), mesosScalar(DISK_MB, 10.0)),
        HOST_ATTRIBUTES_A), DRAINING);

    control.replay();

    cpuManager.addOffer(medium);
    cpuManager.addOffer(large);
    cpuManager.addOffer(small);

    assertEquals(ImmutableList.of(small, medium, large),
        ImmutableList.copyOf(cpuManager.getOffers(GROUP_KEY)));
    assertEquals(ImmutableList.of(small, medium, large),
        ImmutableList.copyOf(cpuManager.getOffers()));
  }

  @Test
  public void testMemoryOrdering() {
    OfferManager cpuManager = createOrderedManager(ImmutableList.of(OfferOrder.MEMORY));

    HostOffer small = setMode(new HostOffer(
        offer("host1", mesosScalar(CPUS, 10), mesosScalar(RAM_MB, 1.0)),
        HOST_ATTRIBUTES_A), DRAINING);

    HostOffer medium = setMode(new HostOffer(
        offer("host2", mesosScalar(CPUS, 10), mesosScalar(RAM_MB, 5.0)),
        HOST_ATTRIBUTES_A), DRAINING);

    HostOffer large = setMode(new HostOffer(
        offer("host3", mesosScalar(CPUS, 10), mesosScalar(RAM_MB, 10.0)),
        HOST_ATTRIBUTES_A), DRAINING);

    control.replay();

    cpuManager.addOffer(medium);
    cpuManager.addOffer(large);
    cpuManager.addOffer(small);

    assertEquals(ImmutableList.of(small, medium, large),
        ImmutableList.copyOf(cpuManager.getOffers(GROUP_KEY)));
    assertEquals(ImmutableList.of(small, medium, large),
        ImmutableList.copyOf(cpuManager.getOffers()));
  }

  @Test
  public void testCPUMemoryOrdering() {
    OfferManager cpuManager = createOrderedManager(
        ImmutableList.of(OfferOrder.CPU, OfferOrder.MEMORY));

    HostOffer small = setMode(new HostOffer(
        offer("host1",
            mesosScalar(CPUS, 1.0),
            mesosScalar(RAM_MB, 2.0),
            mesosScalar(DISK_MB, 3.0)),
        HOST_ATTRIBUTES_A), DRAINING);

    HostOffer medium = setMode(new HostOffer(
        offer("host2",
            mesosScalar(CPUS, 1.0),
            mesosScalar(RAM_MB, 3.0),
            mesosScalar(DISK_MB, 2.0)),
        HOST_ATTRIBUTES_A), DRAINING);

    HostOffer large = setMode(new HostOffer(
        offer("host3",
            mesosScalar(CPUS, 10.0),
            mesosScalar(CPUS, 1.0),
            mesosScalar(RAM_MB, 1024),
            mesosScalar(DISK_MB, 1.0)),
        HOST_ATTRIBUTES_A), DRAINING);

    control.replay();

    cpuManager.addOffer(large);
    cpuManager.addOffer(medium);
    cpuManager.addOffer(small);

    assertEquals(ImmutableList.of(small, medium, large),
        ImmutableList.copyOf(cpuManager.getOffers(GROUP_KEY)));
    assertEquals(ImmutableList.of(small, medium, large),
        ImmutableList.copyOf(cpuManager.getOffers()));
  }

  @Test
  public void testDelayedOfferReturn() {
    OfferSettings settings = new OfferSettings(
        Amount.of(OFFER_FILTER_SECONDS, Time.SECONDS),
        ImmutableList.of(OfferOrder.RANDOM),
        RETURN_DELAY,
        Long.MAX_VALUE,
        FAKE_TICKER);
    ScheduledExecutorService executorMock = createMock(ScheduledExecutorService.class);
    FakeScheduledExecutor clock = FakeScheduledExecutor.fromScheduledExecutorService(executorMock);
    addTearDown(clock::assertEmpty);
    offerManager = new OfferManagerImpl(
        driver,
        settings,
        statsProvider,
        new Deferment.DelayedDeferment(() -> RETURN_DELAY, executorMock));

    driver.declineOffer(OFFER_A_ID, OFFER_FILTER);

    control.replay();

    offerManager.addOffer(OFFER_A);
    assertEquals(1, statsProvider.getLongValue(OUTSTANDING_OFFERS));

    clock.advance(RETURN_DELAY);
    assertEquals(0, statsProvider.getLongValue(OUTSTANDING_OFFERS));
  }

  @Test
  public void testTwoOffersPerHost() {
    // Test for regression of AURORA-1952, where a specific call order could cause OfferManager
    // to violate its one-offer-per-host invariant.

    HostOffer sameAgent = new HostOffer(
        OFFER_A.getOffer().toBuilder().setId(OfferID.newBuilder().setValue("sameAgent")).build(),
        HOST_ATTRIBUTES_A);
    HostOffer sameAgent2 = new HostOffer(
        OFFER_A.getOffer().toBuilder().setId(OfferID.newBuilder().setValue("sameAgent2")).build(),
        HOST_ATTRIBUTES_A);

    driver.declineOffer(OFFER_A_ID, OFFER_FILTER);
    driver.declineOffer(sameAgent.getOffer().getId(), OFFER_FILTER);

    control.replay();

    offerManager.banOffer(OFFER_A_ID);
    offerManager.addOffer(OFFER_A);
    offerManager.addOffer(sameAgent);
    offerManager.cancelOffer(OFFER_A_ID);
    offerManager.addOffer(sameAgent2);
    assertEquals(ImmutableSet.of(sameAgent2), offerManager.getOffers());
  }
}
