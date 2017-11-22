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
package org.apache.aurora.codec;

import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.zip.Deflater;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.InflaterInputStream;

import javax.annotation.Nullable;

import net.morimekta.providence.PMessage;
import net.morimekta.providence.descriptor.PField;
import net.morimekta.providence.descriptor.PStructDescriptor;
import net.morimekta.providence.serializer.BinarySerializer;
import net.morimekta.providence.serializer.Serializer;
import net.morimekta.providence.serializer.binary.BinaryReader;
import net.morimekta.providence.serializer.binary.BinaryWriter;
import net.morimekta.providence.thrift.TBinaryProtocolSerializer;

import org.apache.aurora.common.quantity.Amount;
import org.apache.aurora.common.quantity.Data;
import org.apache.aurora.gen.ScheduledTask;
import org.apache.aurora.gen.storage.Snapshot;
import org.apache.thrift.TBase;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TIOStreamTransport;
import org.apache.thrift.transport.TTransport;

import static java.util.Objects.requireNonNull;

/**
 * Codec that works for thrift objects.
 */
public final class ThriftBinaryCodec {

  private static final Serializer SERIALIZER = new BinarySerializer();

  private ThriftBinaryCodec() {
    // Utility class.
  }

  /**
   * Identical to {@link #decodeNonNull(Class, byte[])}, but allows for a null buffer.
   *
   * @param readInto Object to populate from binary data.
   * @param buffer Buffer to decode.
   * @param <T> Target type.
   * @return A populated message, or {@code null} if the buffer was {@code null}.
   * @throws CodingException If the message could not be decoded.
   */
  @Nullable
  public static <T extends PMessage<T, F>, F extends PField> T decode(
      PStructDescriptor<T, F> descriptor,
      @Nullable byte[] buffer) throws CodingException {

    if (buffer == null) {
      return null;
    }

    return decodeNonNull(descriptor, buffer);
  }

  /**
   * Decodes a binary-encoded byte array into a target type.
   *
   * @param readInto Object to populate from binary data.
   * @param buffer Buffer to decode.
   * @param <T> Target type.
   * @return A populated message.
   * @throws CodingException If the message could not be decoded.
   */
  public static <T extends PMessage<T, F>, F extends PField> T decodeNonNull(
      PStructDescriptor<T, F> descriptor,
      byte[] buffer) throws CodingException {

    requireNonNull(descriptor);
    requireNonNull(buffer);

    try {
      return new TBinaryProtocolSerializer()
          .deserialize(new ByteArrayInputStream(buffer), descriptor);
    } catch (IOException e) {
      throw new CodingException("Failed to decode", e);
    }
  }

  /**
   * Identical to {@link #encodeNonNull(BinaryWriter)}, but allows for a null input.
   *
   * @param object Object to encode.
   * @return Encoded object, or {@code null} if the argument was {@code null}.
   * @throws CodingException If the object could not be encoded.
   */
  @Nullable
  public static <Message extends PMessage<Message, Field>, Field extends PField> byte[]
  encode(Message object) throws CodingException {
    if (object== null) {
      return null;
    }
    return encodeNonNull(object);
  }

  /**
   * Encodes a thrift object into a binary array.
   *
   * @param object Object to encode.
   * @return Encoded object.
   * @throws CodingException If the object could not be encoded.
   */
  public static <Message extends PMessage<Message, Field>, Field extends PField> byte[]
  encodeNonNull(Message object) throws CodingException {

    requireNonNull(object);

    ByteArrayOutputStream out = new ByteArrayOutputStream();
    ScheduledTask.builder().build();

    try {
      new TBinaryProtocolSerializer().serialize(out, object);
      return out.toByteArray();
    } catch (IOException e) {
      throw new CodingException("Failed to serialize: " + object, e);
    }
  }

  // See http://www.zlib.net/zlib_how.html
  // "If the memory is available, buffers sizes on the order of 128K or 256K bytes should be used."
  private static final int DEFLATER_BUFFER_SIZE = Amount.of(256, Data.KB).as(Data.BYTES);

  // Empirical from microbenchmarks (assuming 20MiB/s writes to the replicated log and a large
  // de-duplicated Snapshot from a production environment).
  // TODO(ksweeney): Consider making this configurable.
  private static final int DEFLATE_LEVEL = 3;

  /**
   * Encodes a thrift object into a DEFLATE-compressed binary array.
   *
   * @param tBase Object to encode.
   * @return Deflated, encoded object.
   * @throws CodingException If the object could not be encoded.
   */
  public static <Message extends PMessage<Message, Field>, Field extends PField> byte[]
  deflateNonNull(Message object) throws CodingException {
    requireNonNull(object);

    // NOTE: Buffering is needed here for performance.
    // There are actually 2 buffers in play here - the BufferedOutputStream prevents thrift from
    // causing a call to deflate() on every encoded primitive. The DeflaterOutputStream buffer
    // allows the underlying Deflater to operate on a larger chunk at a time without stopping to
    // copy the intermediate compressed output to outBytes.
    // See http://bugs.java.com/bugdatabase/view_bug.do?bug_id=4986239
    ByteArrayOutputStream outBytes = new ByteArrayOutputStream();
    BufferedOutputStream buffer = new BufferedOutputStream(
        new DeflaterOutputStream(outBytes, new Deflater(DEFLATE_LEVEL), DEFLATER_BUFFER_SIZE),
        DEFLATER_BUFFER_SIZE);
    try {
      new TBinaryProtocolSerializer().serialize(buffer, object);
      buffer.close(); // calls finish() on the underlying stream, completing the compression
      return outBytes.toByteArray();
    } catch (IOException e) {
      throw new CodingException("Failed to serialize: " + object, e);
    }
  }

  /**
   * Decodes a thrift object from a DEFLATE-compressed byte array into a target type.
   *
   * @param clazz Class to instantiate and deserialize to.
   * @param buffer Compressed buffer to decode.
   * @return A populated message.
   * @throws CodingException If the message could not be decoded.
   */
  public static <T extends PMessage<T, F>, F extends PField> T inflateNonNull(
      PStructDescriptor<T, F> descriptor,
      byte[] buffer) throws CodingException {

    requireNonNull(descriptor);
    requireNonNull(buffer);

    try {
      return new TBinaryProtocolSerializer()
          .deserialize(new InflaterInputStream(new ByteArrayInputStream(buffer)), descriptor);
    } catch (IOException e) {
      throw new CodingException("Failed to deserialize: " + e, e);
    }
  }

  /**
   * Thrown when serialization or deserialization failed.
   */
  public static class CodingException extends Exception {
    public CodingException(String message) {
      super(message);
    }
    public CodingException(String msg, Throwable cause) {
      super(msg, cause);
    }
  }
}
