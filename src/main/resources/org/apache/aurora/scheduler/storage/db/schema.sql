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

-- schema for h2 engine.

CREATE TABLE job_keys(
  id IDENTITY,
  -- 255 is not completely arbitrary here, see MAX_IDENTIFIER_LENGTH in ConfigurationManager.java
  role VARCHAR(255) NOT NULL,
  environment VARCHAR(255) NOT NULL,
  name VARCHAR(255) NOT NULL

  UNIQUE(role, environment, name)
);

CREATE TABLE locks(
  id IDENTITY,
  job_key_id INTEGER NOT NULL REFERENCES job_keys(id),
  token VARCHAR NOT NULL,
  user VARCHAR NOT NULL,
  timestamp_ms BIGINT NOT NULL,
  message VARCHAR,

  UNIQUE(job_key_id)
);

CREATE TABLE quotas(
  id IDENTITY,
  role VARCHAR NOT NULL,
  num_cpus FLOAT NOT NULL,
  ram_mb INTEGER NOT NULL,
  disk_mb INTEGER NOT NULL,

  UNIQUE(role)
);

/**
 * NOTE: This table is truncated by TaskMapper, which will cause a conflict when the table is shared
 * with the forthcoming jobs table.  See note in TaskMapper about this before migrating MemJobStore.
 */
CREATE TABLE task_configs(
  id IDENTITY,
  job_key_id INTEGER NOT NULL REFERENCES job_keys(id),
  creator_user VARCHAR NOT NULL,
  service BOOLEAN NOT NULL,
  num_cpus DOUBLE PRECISION NOT NULL,
  ram_mb BIGINT NOT NULL,
  disk_mb BIGINT NOT NULL,
  priority INTEGER NOT NULL,
  max_task_failures INTEGER NOT NULL,
  production BOOLEAN NOT NULL,
  contact_email VARCHAR(255) NOT NULL,
  executor_name VARCHAR(255) NOT NULL,
  executor_data TEXT NOT NULL,

  UNIQUE(
    job_key_id,
    creator_user,
    service,
    num_cpus,
    ram_mb,
    disk_mb,
    priority,
    max_task_failures,
    production,
    contact_email,
    executor_name,
    executor_data)
);

CREATE TABLE task_constraints(
  id IDENTITY,
  task_config_id INTEGER NOT NULL REFERENCES task_configs(id) ON DELETE CASCADE,
  name VARCHAR NOT NULL,

  UNIQUE(task_config_id, name)
);

CREATE TABLE value_constraints(
  id IDENTITY,
  constraint_id INTEGER NOT NULL REFERENCES task_constraints(id) ON DELETE CASCADE,
  negated BOOLEAN NOT NULL,

  UNIQUE(constraint_id)
);

CREATE TABLE value_constraint_values(
  id IDENTITY,
  value_constraint_id INTEGER NOT NULL REFERENCES value_constraints(id) ON DELETE CASCADE,
  value VARCHAR NOT NULL,

  UNIQUE(value_constraint_id, value)
);

CREATE TABLE limit_constraints(
  id IDENTITY,
  constraint_id INTEGER NOT NULL REFERENCES task_constraints(id) ON DELETE CASCADE,
  value INTEGER NOT NULL,

  UNIQUE(constraint_id)
);

CREATE TABLE task_config_requested_ports(
  id IDENTITY,
  task_config_id INTEGER NOT NULL REFERENCES task_configs(id) ON DELETE CASCADE,
  port_name VARCHAR NOT NULL,

  UNIQUE(task_config_id, port_name)
);

CREATE TABLE task_config_task_links(
  id IDENTITY,
  task_config_id INTEGER NOT NULL REFERENCES task_configs(id) ON DELETE CASCADE,
  name VARCHAR NOT NULL,
  value VARCHAR NOT NULL,

  UNIQUE(task_config_id, name)
);

CREATE TABLE task_config_metadata(
  id IDENTITY,
  task_config_id INTEGER NOT NULL REFERENCES task_configs(id) ON DELETE CASCADE,
  name VARCHAR NOT NULL,
  value VARCHAR NOT NULL,

  UNIQUE(task_config_id)
);

CREATE TABLE task_states(
  id INTEGER PRIMARY KEY,
  name VARCHAR NOT NULL,

  UNIQUE(name)
);

CREATE TABLE tasks(
  id IDENTITY,
  task_id VARCHAR NOT NULL,
  slave_id VARCHAR NOT NULL,
  slave_host VARCHAR NOT NULL,
  instance_id INTEGER NOT NULL,
  status INTEGER NOT NULL REFERENCES task_states(id),
  failure_count INTEGER NOT NULL,
  -- TODO(ksweeney): Consider a self-reference here instead of internally using the Mesos task ID.
  ancestor_task_id VARCHAR NULL,
  task_config_id INTEGER NOT NULL REFERENCES task_configs(id) ON DELETE CASCADE,

  UNIQUE(task_id)
);

CREATE TABLE task_events(
  id IDENTITY,
  task_id INTEGER NOT NULL REFERENCES tasks(id) ON DELETE CASCADE,
  timestamp_ms BIGINT NOT NULL,
  status INTEGER NOT NULL REFERENCES task_states(id),
  message VARCHAR NULL,
  scheduler_host VARCHAR NULL,
);
