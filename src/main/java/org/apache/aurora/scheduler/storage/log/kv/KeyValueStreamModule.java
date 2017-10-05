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
package org.apache.aurora.scheduler.storage.log.kv;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;

import org.apache.aurora.common.quantity.Data;
import org.apache.aurora.gen.storage.Op;
import org.apache.aurora.scheduler.config.types.DataAmount;
import org.apache.aurora.scheduler.log.Log;
import org.apache.aurora.scheduler.storage.log.StreamManagerFactory;
import org.apache.aurora.scheduler.storage.log.kv.KeyValueStore.ListableWithDelete;

import static java.util.Objects.requireNonNull;

public class KeyValueStreamModule extends AbstractModule {

  @Parameters(separators = "=")
  public static class Options {
    @Parameter(names = "-kv_max_entry_size")
    public DataAmount maxEntrySize = new DataAmount(500, Data.KB);
  }

  private final Options options;

  public KeyValueStreamModule(Options options) {
    this.options = requireNonNull(options);
  }

  @Override
  protected void configure() {
    // TODO(wfarner): Clean up bindings to make this less clumbsy.
    bind(Log.class).toInstance(() -> null);
  }

  @Provides
  @Singleton
  AppendOnlyStore<Op, Op> provideKeyValueStore(ListableWithDelete<String, byte[]> kvStore) {
    return new LogOpStore(
        new HashedKeyValueStore<>(
            new LogEntrySerializingStore(
                new TransactionalKeyValueStore(kvStore),
                options.maxEntrySize)));
  }

  @Provides
  @Singleton
  StreamManagerFactory provideStreamFactory(KeyedStreamManager streamManager) {
    return logStream -> streamManager;
  }
}
