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
package org.apache.aurora.scheduler.storage.sql;

import com.google.inject.AbstractModule;
import com.google.inject.Singleton;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import org.apache.aurora.scheduler.storage.durability.Persistence;
import org.apache.aurora.scheduler.storage.sql.SqlPersistence.Mode;

import static java.util.Objects.requireNonNull;

public final class SqlPersistenceModule extends AbstractModule {

  private final HikariConfig serverConfig;
  private final Mode mode;

  private SqlPersistenceModule(HikariConfig serverConfig, Mode mode) {
    this.serverConfig = requireNonNull(serverConfig);
    this.mode = requireNonNull(mode);
  }

  @Override
  protected void configure() {
    bind(HikariDataSource.class).toInstance(new HikariDataSource(serverConfig));
    bind(Mode.class).toInstance(mode);
    bind(Persistence.class).to(SqlPersistence.class);
    bind(SqlPersistence.class).in(Singleton.class);
  }

  /**
   * Creates a fully in-memory SQL persistence module, suitable for use in testing.
   *
   * @return Module that will create an in-memory SQL persistence.
   */
  public static SqlPersistenceModule inMemory() {
    HikariConfig config = new HikariConfig();
    config.setJdbcUrl("jdbc:h2:mem:test;MODE=MySQL");
    return withConfig(config, Mode.H2);
  }

  /**
   * Creates a custom SQL persistence module.
   *
   * @param config Module configuration.
   * @param mode Database mode to use.
   * @return SQL persistence module.
   */
  public static SqlPersistenceModule withConfig(HikariConfig config, Mode mode) {
    return new SqlPersistenceModule(config, mode);
  }
}
