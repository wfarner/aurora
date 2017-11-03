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

import java.lang.annotation.Retention;
import java.lang.annotation.Target;
import java.util.List;

import javax.inject.Qualifier;
import javax.inject.Singleton;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Ordering;
import com.google.inject.AbstractModule;
import com.google.inject.Module;
import com.google.inject.PrivateModule;
import com.google.inject.Provides;
import com.google.inject.TypeLiteral;

import org.apache.aurora.common.quantity.Amount;
import org.apache.aurora.common.quantity.Time;
import org.apache.aurora.common.util.Random;
import org.apache.aurora.scheduler.HostOffer;
import org.apache.aurora.scheduler.app.MoreModules;
import org.apache.aurora.scheduler.config.CliOptions;
import org.apache.aurora.scheduler.config.types.TimeAmount;
import org.apache.aurora.scheduler.config.validators.NotNegativeAmount;
import org.apache.aurora.scheduler.events.PubsubEventModule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.ElementType.PARAMETER;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

/**
 * Binding module for resource offer management.
 */
public class OffersModule extends AbstractModule {
  private static final Logger LOG = LoggerFactory.getLogger(OffersModule.class);

  @Parameters(separators = "=")
  public static class Options {
    @Parameter(names = "-hold_offers_forever",
        description =
            "Hold resource offers indefinitely, disabling automatic offer decline settings.",
        arity = 1)
    public boolean holdOffersForever = false;

    @Parameter(names = "-min_offer_hold_time",
        validateValueWith = NotNegativeAmount.class,
        description = "Minimum amount of time to hold a resource offer before declining.")
    public TimeAmount minOfferHoldTime = new TimeAmount(5, Time.MINUTES);

    @Parameter(names = "-offer_hold_jitter_window",
        validateValueWith = NotNegativeAmount.class,
        description = "Maximum amount of random jitter to add to the offer hold time window.")
    public TimeAmount offerHoldJitterWindow = new TimeAmount(1, Time.MINUTES);

    @Parameter(names = "-offer_filter_duration",
        description =
            "Duration after which we expect Mesos to re-offer unused resources. A short duration "
                + "improves scheduling performance in smaller clusters, but might lead to resource "
                + "starvation for other frameworks if you run many frameworks in your cluster.")
    public TimeAmount offerFilterDuration = new TimeAmount(5, Time.SECONDS);

    @Parameter(names = "-unavailability_threshold",
        description =
            "Threshold time, when running tasks should be drained from a host, before a host "
                + "becomes unavailable. Should be greater than min_offer_hold_time + "
                + "offer_hold_jitter_window.")
    public TimeAmount unavailabilityThreshold = new TimeAmount(6, Time.MINUTES);

    @Parameter(names = "-offer_order",
        description =
            "Iteration order for offers, to influence task scheduling. Multiple orderings will be "
                + "compounded together. E.g. CPU,MEMORY,RANDOM would sort first by cpus offered,"
                + " then memory and finally would randomize any equal offers.")
    public List<OfferOrder> offerOrder = ImmutableList.of(OfferOrder.RANDOM);

    @Parameter(names = "-offer_order_modules",
        description = "Custom Guice module to provide an offer ordering.")
    @SuppressWarnings("rawtypes")
    public List<Class> offerOrderModules = ImmutableList.of(OfferOrderModule.class);
  }

  public static class OfferOrderModule extends AbstractModule {
    private final CliOptions options;

    public OfferOrderModule(CliOptions options) {
      this.options = options;
    }

    @Override
    protected void configure() {
      bind(new TypeLiteral<Ordering<HostOffer>>() { })
          .toInstance(OfferOrderBuilder.create(options.offer.offerOrder));
    }
  }

  /**
   * Binding annotation for the threshold to veto tasks with unavailability.
   */
  @Qualifier
  @Target({ FIELD, PARAMETER, METHOD }) @Retention(RUNTIME)
  public @interface UnavailabilityThreshold { }

  private final CliOptions cliOptions;

  public OffersModule(CliOptions cliOptions) {
    this.cliOptions = cliOptions;
  }

  @Override
  protected void configure() {
    Options options = cliOptions.offer;
    if (!options.holdOffersForever) {
      long offerHoldTime = options.offerHoldJitterWindow.as(Time.SECONDS)
          + options.minOfferHoldTime.as(Time.SECONDS);
      if (options.unavailabilityThreshold.as(Time.SECONDS) < offerHoldTime) {
        LOG.warn("unavailability_threshold ({}) is less than the sum of min_offer_hold_time ({})"
                + " and offer_hold_jitter_window ({}). This creates risks of races between "
                + "launching and draining",
            options.unavailabilityThreshold,
            options.minOfferHoldTime,
            options.offerHoldJitterWindow);
      }
    }

    for (Module module: MoreModules.instantiateAll(options.offerOrderModules, cliOptions)) {
      install(module);
    }

    bind(new TypeLiteral<Amount<Long, Time>>() { })
        .annotatedWith(UnavailabilityThreshold.class)
        .toInstance(options.unavailabilityThreshold);

    install(new PrivateModule() {
      @Override
      protected void configure() {
        if (options.holdOffersForever) {
          bind(Deferment.class).to(Deferment.Noop.class);
        } else {
          bind(new TypeLiteral<Supplier<Amount<Long, Time>>>() { }).toInstance(
              new RandomJitterReturnDelay(
                  options.minOfferHoldTime.as(Time.MILLISECONDS),
                  options.offerHoldJitterWindow.as(Time.MILLISECONDS),
                  Random.Util.newDefaultRandom()));
          bind(Deferment.class).to(Deferment.DelayedDeferment.class);
        }

        bind(OfferManager.class).to(OfferManagerImpl.class);
        bind(OfferManagerImpl.class).in(Singleton.class);
        expose(OfferManager.class);
      }
    });
    PubsubEventModule.bindSubscriber(binder(), OfferManager.class);
  }

  @Provides
  @Singleton
  OfferSettings provideOfferSettings(Ordering<HostOffer> offerOrdering) {
    return new OfferSettings(cliOptions.offer.offerFilterDuration, offerOrdering);
  }
}
