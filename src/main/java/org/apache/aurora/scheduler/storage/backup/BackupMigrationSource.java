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
package org.apache.aurora.scheduler.storage.backup;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.stream.Stream;

import javax.inject.Inject;

import org.apache.aurora.gen.storage.Op;
import org.apache.aurora.gen.storage.Snapshot;
import org.apache.aurora.scheduler.storage.durability.Persistence;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TIOStreamTransport;

import static java.util.Objects.requireNonNull;

/**
 * A persistence implementation to be used as a migration source.
 */
class BackupMigrationSource implements Persistence {

  private final File backupFile;

  @Inject
  BackupMigrationSource(File backupFile) {
    this.backupFile = requireNonNull(backupFile);
  }

  @Override
  public void prepare() {
    // no-op
  }

  @Override
  public Stream<Op> recover() throws PersistenceException {
    if (!backupFile.exists()) {
      throw new PersistenceException("Backup " + backupFile + " does not exist.");
    }

    Snapshot snapshot = new Snapshot();
    try {
      TBinaryProtocol prot = new TBinaryProtocol(
          new TIOStreamTransport(new BufferedInputStream(new FileInputStream(backupFile))));
      snapshot.read(prot);
    } catch (TException e) {
      throw new PersistenceException("Failed to decode backup " + e, e);
    } catch (IOException e) {
      throw new PersistenceException("Failed to read backup " + e, e);
    }

    // TODO(wfarner): Waiting on https://reviews.apache.org/r/64286/ to get
    // Snapshotter#asStream
  }

  @Override
  public void persist(Stream<Op> records) {
    throw new UnsupportedOperationException();
  }
}
