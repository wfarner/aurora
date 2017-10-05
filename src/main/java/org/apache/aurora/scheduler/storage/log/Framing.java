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
package org.apache.aurora.scheduler.storage.log;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Iterator;

import javax.annotation.Nullable;

import com.google.common.collect.AbstractIterator;
import com.google.common.collect.Iterators;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hasher;
import com.google.common.primitives.Bytes;

import org.apache.aurora.codec.ThriftBinaryCodec.CodingException;
import org.apache.aurora.gen.storage.Frame;
import org.apache.aurora.gen.storage.FrameChunk;
import org.apache.aurora.gen.storage.FrameHeader;
import org.apache.aurora.gen.storage.LogEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class Framing {

  private static final Logger LOG = LoggerFactory.getLogger(Framing.class);

  private static final int SERIALIZE_CHUNK_OVERHEAD = computeSerializedChunkOverhead();

  private Framing() {
    // Utility class.
  }

  private static int computeSerializedChunkOverhead() {
    return encode(Frame.chunk(new FrameChunk(ByteBuffer.wrap(new byte[] {})))).length;
  }

  /**
   * Serializes a log entry, splitting it into a series of chunked frames if necessary.
   *
   * @param logEntry Entry to serialize.
   * @param maxEntrySizeBytes Maximum size for any chunk of the serialized entry.
   * @param hasher Hash function to checksum framed data.
   * @return An iterator over the serialized entry.  If framing was not needed, the iterator will
   *         contain exactly one element.
   */
  public static Iterator<byte[]> serializeWithFrames(
      LogEntry logEntry,
      int maxEntrySizeBytes,
      HashFunction hasher) {

    final byte[] entry = Entries.thriftBinaryEncode(logEntry);

    if (entry.length <= maxEntrySizeBytes) {
      return Iterators.forArray(entry);
    }

    // Subtract overhead of the frame chunk structure itself.
    int maxDataArraySize = maxEntrySizeBytes - SERIALIZE_CHUNK_OVERHEAD;

    int chunks = (int) Math.ceil(entry.length / (double) maxDataArraySize);

    byte[] header = Entries.thriftBinaryEncode(LogEntry.frame(
        Frame.header(new FrameHeader(chunks, ByteBuffer.wrap(hasher.hashBytes(entry).asBytes())))));
    return new AbstractIterator<byte[]>() {
      private int i = -1;

      @Override
      protected byte[] computeNext() {
        byte[] result;
        if (i == -1) {
          result = header;
        } else if (i < chunks) {
          int offset = i * maxDataArraySize;
          ByteBuffer chunk =
              ByteBuffer.wrap(entry, offset, Math.min(maxDataArraySize, entry.length - offset));
          try {
            result = encode(Frame.chunk(new FrameChunk(chunk)));
          } catch (CodingException e) {
            throw new RuntimeException(e);
          }
        } else {
          return endOfData();
        }

        i++;
        return result;
      };
    };
  }

  private static byte[] encode(Frame frame) throws CodingException {
    return Entries.thriftBinaryEncode(LogEntry.frame(frame));
  }

  @Nullable
  public static LogEntry readAndUnframeSerialized(Iterator<byte[]> serialized, Hasher hasher)
      throws CodingException {

    return readAndUnframe(Iterators.transform(serialized, Entries::thriftBinaryDecode), hasher);
  }

  @Nullable
  public static LogEntry readAndUnframe(Iterator<LogEntry> entries, Hasher hasher)
      throws CodingException {

    if (!entries.hasNext()) {
      return null;
    }

    LogEntry logEntry = entries.next();
    while (logEntry != null && isFrame(logEntry)) {
      logEntry = tryDecodeFrame(logEntry.getFrame(), entries, hasher);
    }

    return logEntry;
  }

  private static LogEntry tryDecodeFrame(Frame frame, Iterator<LogEntry> entries, Hasher hasher) {
    if (!isHeader(frame)) {
      LOG.warn("Found a frame with no preceding header, skipping.");
      return null;
    }
    FrameHeader header = frame.getHeader();
    byte[][] chunks = new byte[header.getChunkCount()][];

    for (int i = 0; i < header.getChunkCount(); i++) {
      if (!entries.hasNext()) {
        // Stream ended before end of frame.
        logBadFrame(header, i);
        return null;
      }
      LogEntry logEntry = entries.next();
      if (!isFrame(logEntry)) {
        // Non-frame entry before expected number of chunks read.
        logBadFrame(header, i);
        return logEntry;
      }
      Frame chunkFrame = logEntry.getFrame();
      if (!isChunk(chunkFrame)) {
        // Non-chunk entry before expected number of chunks read.
        logBadFrame(header, i);
        return logEntry;
      }
      byte[] chunkData = chunkFrame.getChunk().getData();
      hasher.putBytes(chunkData);
      chunks[i] = chunkData;
    }
    if (!Arrays.equals(header.getChecksum(), hasher.hash().asBytes())) {
      throw new CodingException("Read back a framed log entry that failed its checksum");
    }
    return Entries.thriftBinaryDecode(Bytes.concat(chunks));
  }

  private static void logBadFrame(FrameHeader header, int chunkIndex) {
    LOG.info(String.format("Found an aborted transaction, required %d frames and found %d",
        header.getChunkCount(), chunkIndex));
    //vars.badFramesRead.incrementAndGet();
  }

  private static boolean isFrame(LogEntry logEntry) {
    return logEntry.getSetField() == LogEntry._Fields.FRAME;
  }

  private static boolean isChunk(Frame frame) {
    return frame.getSetField() == Frame._Fields.CHUNK;
  }

  private static boolean isHeader(Frame frame) {
    return frame.getSetField() == Frame._Fields.HEADER;
  }
}
