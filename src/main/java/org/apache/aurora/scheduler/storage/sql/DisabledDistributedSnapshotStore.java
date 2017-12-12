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

import org.apache.aurora.codec.ThriftBinaryCodec;
import org.apache.aurora.gen.storage.Snapshot;
import org.apache.aurora.scheduler.storage.SnapshotStore;
import org.apache.aurora.scheduler.storage.Storage;

/**
 * A snapshot store that throws for any snapshot attempt.
 */
class DisabledDistributedSnapshotStore implements SnapshotStore {
  @Override
  public void snapshot() throws Storage.StorageException {
    throw new UnsupportedOperationException("This storage system does not support snapshotting");
  }

  @Override
  public void snapshotWith(Snapshot snapshot) throws ThriftBinaryCodec.CodingException {
    throw new UnsupportedOperationException("This storage system does not support snapshotting");
  }
}
