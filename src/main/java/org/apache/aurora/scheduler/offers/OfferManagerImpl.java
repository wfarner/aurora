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

import java.util.concurrent.atomic.AtomicLong;

import javax.inject.Inject;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.eventbus.Subscribe;

import org.apache.aurora.common.inject.TimedInterceptor;
import org.apache.aurora.common.quantity.Time;
import org.apache.aurora.common.stats.StatsProvider;
import org.apache.aurora.scheduler.HostOffer;
import org.apache.aurora.scheduler.TierInfo;
import org.apache.aurora.scheduler.base.TaskGroupKey;
import org.apache.aurora.scheduler.events.PubsubEvent;
import org.apache.aurora.scheduler.mesos.Driver;
import org.apache.mesos.v1.Protos;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.Objects.requireNonNull;

/**
 *
 */
public class OfferManagerImpl implements OfferManager {
  private static final Logger LOG = LoggerFactory.getLogger(OfferManagerImpl.class);

  @VisibleForTesting
  static final String OFFER_ACCEPT_RACES = "offer_accept_races";
  @VisibleForTesting
  static final String OUTSTANDING_OFFERS = "outstanding_offers";
  @VisibleForTesting
  static final String OFFER_CANCEL_FAILURES = "offer_cancel_failures";

  private final HostOffers hostOffers;
  private final AtomicLong offerRaces;
  private final AtomicLong offerCancelFailures;

  private final Driver driver;
  private final OfferSettings offerSettings;
  private final Deferment offerDecline;

  @Inject
  @VisibleForTesting
  public OfferManagerImpl(
      Driver driver,
      OfferSettings offerSettings,
      StatsProvider statsProvider,
      Deferment offerDecline) {

    this.driver = requireNonNull(driver);
    this.offerSettings = requireNonNull(offerSettings);
    this.hostOffers = new HostOffers(statsProvider, offerSettings.getOfferOrder());
    this.offerRaces = statsProvider.makeCounter(OFFER_ACCEPT_RACES);
    this.offerCancelFailures = statsProvider.makeCounter(OFFER_CANCEL_FAILURES);
    this.offerDecline = requireNonNull(offerDecline);
  }

  @Override
  public void addOffer(HostOffer offer) {
    Optional<HostOffer> sameAgent = hostOffers.addAndPreventAgentCollision(offer);
    if (sameAgent.isPresent()) {
      // We have an existing offer for the same agent.  We choose to return both offers so that
      // they may be combined into a single offer.
      LOG.info("Returning offers for " + offer.getOffer().getAgentId().getValue()
          + " for compaction.");
      decline(offer.getOffer().getId());
      decline(sameAgent.get().getOffer().getId());
    } else {
      offerDecline.defer(() -> removeAndDecline(offer.getOffer().getId()));
    }
  }

  private void removeAndDecline(Protos.OfferID id) {
    if (removeFromHostOffers(id)) {
      decline(id);
    }
  }

  private void decline(Protos.OfferID id) {
    LOG.debug("Declining offer {}", id);
    driver.declineOffer(id, getOfferFilter());
  }

  private Protos.Filters getOfferFilter() {
    return Protos.Filters.newBuilder()
        .setRefuseSeconds(offerSettings.getOfferFilterDuration().as(Time.SECONDS))
        .build();
  }

  @Override
  public boolean cancelOffer(final Protos.OfferID offerId) {
    boolean success = removeFromHostOffers(offerId);
    if (!success) {
      // This will happen rarely when we race to process this rescind against accepting the offer
      // to launch a task.
      // If it happens frequently, we are likely processing rescinds before the offer itself.
      LOG.warn("Failed to cancel offer: {}.", offerId.getValue());
      this.offerCancelFailures.incrementAndGet();
    }
    return success;
  }

  @Override
  public void banOffer(Protos.OfferID offerId) {
    hostOffers.addGlobalBan(offerId);
  }

  private boolean removeFromHostOffers(final Protos.OfferID offerId) {
    requireNonNull(offerId);

    // The small risk of inconsistency is acceptable here - if we have an accept/remove race
    // on an offer, the master will mark the task as LOST and it will be retried.
    return hostOffers.remove(offerId);
  }

  @Override
  public Iterable<HostOffer> getOffers() {
    return hostOffers.getOffers();
  }

  @Override
  public Iterable<HostOffer> getOffers(TaskGroupKey groupKey, boolean revocable) {
    return hostOffers.getWeaklyConsistentOffers(groupKey, revocable);
  }

  @Override
  public Optional<HostOffer> getOffer(Protos.AgentID slaveId) {
    return hostOffers.get(slaveId);
  }

  /**
   * Updates the preference of a host's offers.
   *
   * @param change Host change notification.
   */
  @Subscribe
  public void hostAttributesChanged(PubsubEvent.HostAttributesChanged change) {
    hostOffers.updateHostAttributes(change.getAttributes());
  }

  /**
   * Notifies the queue that the driver is disconnected, and all the stored offers are now
   * invalid.
   * <p>
   * The queue takes this as a signal to flush its queue.
   *
   * @param event Disconnected event.
   */
  @Subscribe
  public void driverDisconnected(PubsubEvent.DriverDisconnected event) {
    LOG.info("Clearing stale offers since the driver is disconnected.");
    hostOffers.clear();
  }

  @Override
  public void banOfferForTaskGroup(Protos.OfferID offerId, TaskGroupKey groupKey) {
    hostOffers.addStaticGroupBan(offerId, groupKey);
  }

  @TimedInterceptor.Timed("offer_manager_launch_task")
  @Override
  public void launchTask(Protos.OfferID offerId, Protos.TaskInfo task) throws LaunchException {
    // Guard against an offer being removed after we grabbed it from the iterator.
    // If that happens, the offer will not exist in hostOffers, and we can immediately
    // send it back to LOST for quick reschedule.
    // Removing while iterating counts on the use of a weakly-consistent iterator being used,
    // which is a feature of ConcurrentSkipListSet.
    if (hostOffers.remove(offerId)) {
      try {
        Protos.Offer.Operation launch = Protos.Offer.Operation.newBuilder()
            .setType(Protos.Offer.Operation.Type.LAUNCH)
            .setLaunch(Protos.Offer.Operation.Launch.newBuilder().addTaskInfos(task))
            .build();
        driver.acceptOffers(offerId, ImmutableList.of(launch), getOfferFilter());
      } catch (IllegalStateException e) {
        // TODO(William Farner): Catch only the checked exception produced by Driver
        // once it changes from throwing IllegalStateException when the driver is not yet
        // registered.
        throw new LaunchException("Failed to launch task.", e);
      }
    } else {
      offerRaces.incrementAndGet();
      throw new LaunchException("Offer no longer exists in offer queue, likely data race.");
    }
  }
}
