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


import java.util.stream.Collectors;

import com.google.common.base.Optional;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.googlecode.cqengine.attribute.Attribute;
import com.googlecode.cqengine.attribute.MultiValueAttribute;
import com.googlecode.cqengine.attribute.SimpleAttribute;
import com.googlecode.cqengine.query.option.QueryOptions;

import org.apache.aurora.scheduler.HostOffer;
import org.apache.aurora.scheduler.TierInfo;
import org.apache.aurora.scheduler.resources.ResourceType;
import org.apache.aurora.scheduler.storage.entities.IAttribute;

import static org.apache.aurora.scheduler.configuration.ConfigurationManager.DEDICATED_ATTRIBUTE;

final class Indexing {
  private Indexing() {
    // Utility class.
  }

  static final Attribute<HostOffer, String> OFFER_ID =
      new SimpleAttribute<HostOffer, String>("offerId") {
        @Override
        public String getValue(HostOffer offer, QueryOptions queryOptions) {
          return offer.getOffer().getId().getValue();
        }
      };

  static final Attribute<HostOffer, String> OFFER_HOST =
      new SimpleAttribute<HostOffer, String>("hostname") {
        @Override
        public String getValue(HostOffer offer, QueryOptions queryOptions) {
          return offer.getOffer().getHostname();
        }
      };

  static final Attribute<HostOffer, String> OFFER_AGENT =
      new SimpleAttribute<HostOffer, String>("agentId") {
        @Override
        public String getValue(HostOffer offer, QueryOptions queryOptions) {
          return offer.getOffer().getAgentId().getValue();
        }
      };

  static final Attribute<HostOffer, Double> OFFER_REVOCABLE_CPU =
      new SimpleAttribute<HostOffer, Double>("revocableCpu") {
        final TierInfo revocable = new TierInfo(false, true);
        @Override
        public Double getValue(HostOffer offer, QueryOptions queryOptions) {
          return offer.getResourceBag(revocable).valueOf(ResourceType.CPUS);
        }
      };

  static final Attribute<HostOffer, Double> OFFER_NON_REVOCABLE_CPU =
      new SimpleAttribute<HostOffer, Double>("nonRevocableCpu") {
        final TierInfo nonRevocable = new TierInfo(false, false);
        @Override
        public Double getValue(HostOffer offer, QueryOptions queryOptions) {
          return offer.getResourceBag(nonRevocable).valueOf(ResourceType.CPUS);
        }
      };

  static final Attribute<HostOffer, Double> OFFER_REVOCABLE_RAM =
      new SimpleAttribute<HostOffer, Double>("revocableRam") {
        final TierInfo revocable = new TierInfo(false, true);
        @Override
        public Double getValue(HostOffer offer, QueryOptions queryOptions) {
          return offer.getResourceBag(revocable).valueOf(ResourceType.RAM_MB);
        }
      };

  static final Attribute<HostOffer, Double> OFFER_NON_REVOCABLE_RAM =
      new SimpleAttribute<HostOffer, Double>("nonRevocableRam") {
        final TierInfo nonRevocable = new TierInfo(false, false);
        @Override
        public Double getValue(HostOffer offer, QueryOptions queryOptions) {
          return offer.getResourceBag(nonRevocable).valueOf(ResourceType.RAM_MB);
        }
      };

  static final Attribute<HostOffer, Optional<String>> OFFER_DEDICATED_HOST =
      new MultiValueAttribute<HostOffer, Optional<String>>("dedicated") {
        @Override
        public Iterable<Optional<String>> getValues(HostOffer offer, QueryOptions queryOptions) {
          Optional<IAttribute> attribute =
              FluentIterable.from(offer.getAttributes().getAttributes())
                  .filter(a -> a.getName().equals(DEDICATED_ATTRIBUTE))
                  .first();
          if (attribute.isPresent()) {
            return attribute.get().getValues().stream()
                .map(Optional::of)
                .collect(Collectors.toList());
          } else {
            return ImmutableList.of(Optional.absent());
          }
        }
      };
}
