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

import java.util.Map;
import java.util.Set;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Ordering;
import com.google.common.collect.Sets;
import com.googlecode.cqengine.ConcurrentIndexedCollection;
import com.googlecode.cqengine.IndexedCollection;
import com.googlecode.cqengine.attribute.Attribute;
import com.googlecode.cqengine.index.Index;
import com.googlecode.cqengine.index.hash.HashIndex;
import com.googlecode.cqengine.index.navigable.NavigableIndex;
import com.googlecode.cqengine.index.unique.UniqueIndex;
import com.googlecode.cqengine.quantizer.Quantizer;
import com.googlecode.cqengine.query.Query;
import com.googlecode.cqengine.query.QueryFactory;
import com.googlecode.cqengine.resultset.ResultSet;
import com.googlecode.cqengine.resultset.common.NoSuchObjectException;

import org.apache.aurora.common.stats.StatsProvider;
import org.apache.aurora.scheduler.HostOffer;
import org.apache.aurora.scheduler.base.TaskGroupKey;
import org.apache.aurora.scheduler.configuration.ConfigurationManager;
import org.apache.aurora.scheduler.resources.ResourceManager;
import org.apache.aurora.scheduler.resources.ResourceType;
import org.apache.aurora.scheduler.storage.entities.IHostAttributes;
import org.apache.mesos.v1.Protos;

import static com.googlecode.cqengine.query.QueryFactory.greaterThanOrEqualTo;

import static org.apache.aurora.scheduler.offers.Indexing.OFFER_AGENT;
import static org.apache.aurora.scheduler.offers.Indexing.OFFER_DEDICATED_HOST;
import static org.apache.aurora.scheduler.offers.Indexing.OFFER_HOST;
import static org.apache.aurora.scheduler.offers.Indexing.OFFER_ID;
import static org.apache.aurora.scheduler.offers.Indexing.OFFER_NON_REVOCABLE_CPU;
import static org.apache.aurora.scheduler.offers.Indexing.OFFER_NON_REVOCABLE_RAM;
import static org.apache.aurora.scheduler.offers.Indexing.OFFER_REVOCABLE_CPU;
import static org.apache.aurora.scheduler.offers.Indexing.OFFER_REVOCABLE_RAM;

/**
 * A container for the data structures used by this class, to make it easier to reason about
 * the different indices used and their consistency.
 */
class HostOffers {

  @VisibleForTesting
  static final String STATICALLY_BANNED_OFFERS = "statically_banned_offers_size";

  @VisibleForTesting
  static final String GLOBALLY_BANNED_OFFERS = "globally_banned_offers_size";

  private final IndexedCollection<HostOffer> offers;

  private final Map<Attribute<HostOffer, ?>, Index<HostOffer>> offerQueryIndices = ImmutableMap.of(
      OFFER_REVOCABLE_CPU, quantizedDoubleIndex(OFFER_REVOCABLE_CPU),
      OFFER_REVOCABLE_RAM, quantizedDoubleIndex(OFFER_REVOCABLE_RAM),
      OFFER_NON_REVOCABLE_CPU, quantizedDoubleIndex(OFFER_NON_REVOCABLE_CPU),
      OFFER_NON_REVOCABLE_RAM, quantizedDoubleIndex(OFFER_NON_REVOCABLE_RAM),
      OFFER_DEDICATED_HOST, HashIndex.onAttribute(OFFER_DEDICATED_HOST));

  // Keep track of globally banned offers that will never be matched to anything.
  private final Set<Protos.OfferID> globallyBannedOffers = Sets.newConcurrentHashSet();

  private static class OneDecimalPlaceQuantizer implements Quantizer<Double> {
    @Override
    public Double getQuantizedValue(Double attributeValue) {
      double scaled = attributeValue * 10.0;
      long floored = (long) scaled;
      double flooredDouble = (double) floored;
      return flooredDouble / 10.0;
    }
  }

  private static Index<HostOffer> quantizedDoubleIndex(Attribute<HostOffer, Double> attribute) {
    return NavigableIndex.withQuantizerOnAttribute(new OneDecimalPlaceQuantizer(), attribute);
  }

  HostOffers(StatsProvider statsProvider, Ordering<HostOffer> offerOrder) {
    offers = new ConcurrentIndexedCollection<>();
    offers.addIndex(UniqueIndex.onAttribute(OFFER_ID));
    // A hash index is used here because this layer imposes no restriction on offer hostname
    // uniqueness.
    offers.addIndex(HashIndex.onSemiUniqueAttribute(OFFER_HOST));
    offers.addIndex(UniqueIndex.onAttribute(OFFER_AGENT));
    offerQueryIndices.values().forEach(offers::addIndex);

    statsProvider.exportSize(OfferManagerImpl.OUTSTANDING_OFFERS, offers);
    statsProvider.makeGauge(GLOBALLY_BANNED_OFFERS, globallyBannedOffers::size);
  }

  synchronized Optional<HostOffer> get(Protos.AgentID slaveId) {
    try {
      HostOffer offer =
          offers.retrieve(QueryFactory.equal(OFFER_AGENT, slaveId.getValue())).uniqueResult();
      if (globallyBannedOffers.contains(offer.getOffer().getId())) {
        return Optional.absent();
      } else {
        return Optional.of(offer);
      }
    } catch (NoSuchObjectException e) {
      return Optional.absent();
    }
  }

  /**
   * Adds an offer while maintaining a guarantee that no two offers may exist with the same
   * agent ID.  If an offer exists with the same agent ID, the existing offer is removed
   * and returned, and {@code offer} is not added.
   *
   * @param offer Offer to add.
   * @return The pre-existing offer with the same agent ID as {@code offer}, if one exists,
   *         which will also be removed prior to returning.
   */
  synchronized Optional<HostOffer> addAndPreventAgentCollision(HostOffer offer) {
    ResultSet<HostOffer> sameAgent =
        offers.retrieve(QueryFactory.equal(OFFER_AGENT, offer.getOffer().getAgentId().getValue()));
    if (sameAgent.isNotEmpty()) {
      HostOffer redundant = sameAgent.uniqueResult();
      remove(redundant.getOffer().getId());
      return Optional.of(redundant);
    }

    addInternal(offer);
    return Optional.absent();
  }

  private void addInternal(HostOffer offer) {
    offers.add(offer);
  }

  synchronized boolean remove(Protos.OfferID id) {
    ResultSet<HostOffer> match = offers.retrieve(QueryFactory.equal(OFFER_ID, id.getValue()));
    // The result of isNotEmpty() is affected by remove() on the underlying collection, making
    // it necessary to get the value before removing.
    boolean hasMatch = match.isNotEmpty();
    if (hasMatch) {
      offers.remove(match.uniqueResult());
    }

    globallyBannedOffers.remove(id);
    return hasMatch;
  }

  synchronized void updateHostAttributes(IHostAttributes attributes) {
    ResultSet<HostOffer> match =
        offers.retrieve(QueryFactory.equal(OFFER_HOST, attributes.getHost()));
    if (match.isNotEmpty()) {
      for (HostOffer offer : match) {
        offers.remove(offer);
        addInternal(new HostOffer(offer.getOffer(), attributes));
      }
    }
  }

  /**
   * Returns an iterable giving the state of the offers at the time the method is called. Unlike
   * {@code getWeaklyConsistentOffers}, the underlying collection is a copy of the original and
   * will not be modified outside of the returned iterable.
   *
   * @return The offers currently known by the scheduler.
   */
  synchronized Iterable<HostOffer> getOffers() {
    return FluentIterable.from(offers).filter(
        e -> !globallyBannedOffers.contains(e.getOffer().getId())
    ).toSet();
  }

  /**
   * Returns a weakly-consistent iterable giving the available offers to a given
   * {@code groupKey}. This iterable can handle concurrent operations on its underlying
   * collection, and may reflect changes that happen after the construction of the iterable.
   * This property is mainly used in {@code launchTask}.
   *
   * @param groupKey The task group to get offers for.
   * @param revocable Whether the task uses revocable resources.
   * @return The offers a given task group can use.
   */
  synchronized Iterable<HostOffer> getWeaklyConsistentOffers(
      TaskGroupKey groupKey,
      boolean revocable) {

    // TODO(wfarner): Add support for selecting based on offer matching dedicated constraint.

    Attribute<HostOffer, Double> cpuAttribute =
        revocable ? OFFER_REVOCABLE_CPU : OFFER_NON_REVOCABLE_CPU;
    Attribute<HostOffer, Double> ramAttribute =
        revocable ? OFFER_REVOCABLE_RAM : OFFER_NON_REVOCABLE_RAM;

    Query<HostOffer> cpuFilter = greaterThanOrEqualTo(
        cpuAttribute,
        ResourceManager.quantityOf(groupKey.getTask().getResources(), ResourceType.CPUS));
    Query<HostOffer> ramFilter = greaterThanOrEqualTo(
        ramAttribute,
        ResourceManager.quantityOf(groupKey.getTask().getResources(), ResourceType.RAM_MB));

    Optional<String> dedicatedValue =
        Optional.fromNullable(ConfigurationManager.getDedicatedConstraint(groupKey.getTask()))
        .transform(c -> Iterables.getOnlyElement(c.getConstraint().getValue().getValues()));
    Query<HostOffer> dedicatedFilter = QueryFactory.equal(OFFER_DEDICATED_HOST, dedicatedValue);

    ResultSet<HostOffer> matches =
        offers.retrieve(QueryFactory.and(cpuFilter, ramFilter, dedicatedFilter));

    // TODO(wfarner): Surface this information so that it may be plumbed back to NearestFit.
    /*
    if (matches.isEmpty()) {
      // Deterine which vector did not match.
      if (offerQueryIndices.get(cpuAttribute).retrieve(cpuFilter, new QueryOptions()).isEmpty()) {
        System.out.println("No CPU matches");
      }
      if (offerQueryIndices.get(cpuAttribute).retrieve(ramFilter, new QueryOptions()).isEmpty()) {
        System.out.println("No RAM matches");
      }
    }
    */

    return FluentIterable.from(matches)
        .filter(e -> !globallyBannedOffers.contains(e.getOffer().getId()));
  }

  synchronized void addGlobalBan(Protos.OfferID offerId) {
    globallyBannedOffers.add(offerId);
  }

  synchronized void addStaticGroupBan(Protos.OfferID offerId, TaskGroupKey groupKey) {
    // no-op.
  }

  synchronized void clear() {
    offers.clear();
    globallyBannedOffers.clear();
  }
}
