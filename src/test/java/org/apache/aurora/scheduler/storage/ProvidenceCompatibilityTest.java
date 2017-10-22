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
package org.apache.aurora.scheduler.storage;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

import net.morimekta.providence.serializer.BinarySerializer;

import org.apache.aurora.codec.ThriftBinaryCodec;
import org.apache.aurora.providence.storage.Snapshot;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class ProvidenceCompatibilityTest {

  @Test
  public void testProvidenceThriftRoundtrip() throws Exception {
    // thrift -> thrift binary
    org.apache.aurora.gen.storage.Snapshot tSnapshot = new org.apache.aurora.gen.storage.Snapshot();
    byte[] tSnapshotData = ThriftBinaryCodec.encode(tSnapshot);

    // thrift binary -> providence -> providence binary
    Snapshot pSnapshot = decodeProvidence(tSnapshotData);
    byte[] pSnapshotData = encodeProvidence(pSnapshot);

    // providence binary -> thrift -> thrift binary
    org.apache.aurora.gen.storage.Snapshot tSnapshot2 =
        ThriftBinaryCodec.decode(org.apache.aurora.gen.storage.Snapshot.class, pSnapshotData);
    // TODO(wfarner): This is failing because providence defaults to empty collections while
    // thrift defaults to nulls.  Patch providence with a compatibility mode for this.
    assertEquals(tSnapshot, tSnapshot2);
    byte[] tSnapshotData2 = ThriftBinaryCodec.encode(tSnapshot2);

    // thrift binary -> providence
    assertEquals(pSnapshot, decodeProvidence(tSnapshotData2));
  }

  private byte[] encodeProvidence(Snapshot snapshot) throws IOException {
    ByteArrayOutputStream data = new ByteArrayOutputStream();
    new BinarySerializer().serialize(data, snapshot);
    return data.toByteArray();
  }

  private Snapshot decodeProvidence(byte[] data) throws IOException {
    return new BinarySerializer().deserialize(new ByteArrayInputStream(data), Snapshot.kDescriptor);
  }
}
