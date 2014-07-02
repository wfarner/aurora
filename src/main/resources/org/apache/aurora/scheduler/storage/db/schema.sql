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
  id INT IDENTITY,
  role VARCHAR NOT NULL,
  environment VARCHAR NOT NULL,
  name VARCHAR NOT NULL,

  UNIQUE(role, environment, name)
);

CREATE TABLE locks(
  id INT IDENTITY,
  job_key_id INT NOT NULL REFERENCES job_keys(id),
  token VARCHAR NOT NULL,
  user VARCHAR NOT NULL,
  timestampMs BIGINT NOT NULL,
  message VARCHAR,

  UNIQUE(job_key_id)
);

CREATE TABLE quotas(
  id INT IDENTITY,
  role VARCHAR NOT NULL,
  num_cpus FLOAT NOT NULL,
  ram_mb INT NOT NULL,
  disk_mb INT NOT NULL,

  UNIQUE(role)
);

CREATE TABLE task_configs(
  id INT IDENTITY,
  job_key_id INT NOT NULL REFERENCES job_keys(id),
  creator_user VARCHAR NOT NULL,
  service BOOLEAN NOT NULL,
  num_cpus FLOAT8 NOT NULL,
  ram_mb INTEGER NOT NULL,
  disk_mb INTEGER NOT NULL,
  priority INTEGER NOT NULL,
  max_task_failures INTEGER NOT NULL,
  production BOOLEAN NOT NULL,
  contact_email VARCHAR NOT NULL
  executor_name VARCHAR NOT NULL,
  executor_data VARCHAR NOT NULL
);

CREATE TABLE task_constraints(
  id INT IDENTITY,
  task_config_id INT NOT NULL REFERENCES task_configs(id),
  name VARCHAR NOT NULL,

  UNIQUE(task_config_id, name)
);

CREATE TABLE value_constraints(
  id INT IDENTITY,
  constraint_id INT NOT NULL REFERENCES task_constraints(id),
  negated BOOLEAN NOT NULL,

  UNIQUE(constraint_id)
);

CREATE TABLE value_constraint_values(
  id INT IDENTITY,
  value_constraint_id INT NOT NULL REFERENCES value_constraints(id),
  value VARCHAR NOT NULL,

  UNIQUE(value_constraint_id, value)
);

CREATE TABLE limit_constraints(
  id INT IDENTITY,
  constraint_id INT NOT NULL REFERENCES task_constraints(id),
  value INTEGER NOT NULL,

  UNIQUE(constraint_id)
);

CREATE TABLE task_config_requested_ports(
  id INT IDENTITY,
  task_config_id INT NOT NULL REFERENCES task_configs(id),
  port_name VARCHAR NOT NULL,

  UNIQUE(task_config_id, port_name)
);

CREATE TABLE task_config_task_links(
  id INT IDENTITY,
  task_config_id INT NOT NULL REFERENCES task_configs(id),
  name VARCHAR NOT NULL,
  value VARCHAR NOT NULL,

  UNIQUE(task_config_id, name)
);

CREATE TABLE task_config_metadata(
  id INT IDENTITY,
  task_config_id INT NOT NULL REFERENCES task_configs(id),
  name VARCHAR NOT NULL,
  value VARCHAR NOT NULL,

  UNIQUE(task_config_id)
);

CREATE TABLE task_states(
  id INT PRIMARY KEY,
  name VARCHAR NOT NULL,

  UNIQUE(name)
);

CREATE TABLE tasks(
  id INT IDENTITY,
  task_id VARCHAR NOT NULL,
  slave_id VARCHAR NOT NULL,
  slave_host VARCHAR NOT NULL,
  instance_id INTEGER NOT NULL,
  status INT NOT NULL REFERENCES task_states(id),
  failure_count INTEGER NOT NULL,
  ancestor_task_id VARCHAR NULL,

  UNIQUE(task_id)
);

CREATE TAABLE task_events(
  id INT IDENTITY,
  task_id INT NOT NULL REFERENCES tasks(id),
  timestamp_ms BIGINT NOT NULL,
  status INT NOT NULL REFERENCES task_states(id),
  message VARCHAR NULL,
  scheduler_host VARCHAR NULL
);
