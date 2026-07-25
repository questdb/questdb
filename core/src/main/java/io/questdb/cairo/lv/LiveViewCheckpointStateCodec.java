/*+*****************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2026 QuestDB
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 ******************************************************************************/

package io.questdb.cairo.lv;

import io.questdb.cairo.CairoException;
import io.questdb.cairo.vm.api.MemoryA;
import io.questdb.std.DirectLongList;
import io.questdb.std.MemoryTag;
import io.questdb.std.MemoryTracker;
import io.questdb.std.Unsafe;
import io.questdb.std.Vect;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.Closeable;

/**
 * Bounded semantic codecs for immutable live-view checkpoint state pages.
 * Codec tags are global within {@link LiveViewCheckpointStatePageRef}; callers
 * additionally validate the function-specific page kind before decoding.
 *
 * <p>The timestamp encoding stores the first timestamp raw, the first checked
 * non-negative delta as unsigned LEB128, and later checked delta-of-delta values
 * as ZigZag LEB128. The double encoding is a bit-exact Gorilla-style XOR stream
 * over raw IEEE-754 bits. Both adaptive selectors retain raw 64-bit storage
 * unless encoding saves at least 6.25% and at least 16 bytes.</p>
 */
public final class LiveViewCheckpointStateCodec {

    public static final int CHUNK_ROWS = 4096;
    public static final int DOUBLE_RAW_64 = 2;
    public static final int DOUBLE_XOR = 3;
    public static final int LONG_RAW_64 = 4;
    public static final int MIN_SAVING_BYTES = 16;
    public static final int TIMESTAMP_DELTA_OF_DELTA_VARINT = 1;
    public static final int TIMESTAMP_RAW_64 = 0;
    private static final int BITS_PER_BYTE = 8;
    private static final int MAX_VARINT_BYTES = 10;

    private LiveViewCheckpointStateCodec() {
    }

    /**
     * Decodes a complete double stream into {@code targetAddress}. The target
     * capacity and every source read are validated before native memory access.
     *
     * @return bytes consumed, always {@code storedLength} on success
     */
    public static int decodeDoubles(
            long sourceAddress,
            int storedLength,
            int codec,
            int rowCount,
            long targetAddress,
            int targetCapacity
    ) {
        validateDecodeArguments(sourceAddress, storedLength, rowCount, targetAddress, targetCapacity);
        if (codec == DOUBLE_RAW_64) {
            final int rawLength = rawLength(rowCount);
            if (storedLength != rawLength) {
                throw invalid("raw double page length mismatch")
                        .put(" [storedLength=").put(storedLength)
                        .put(", expected=").put(rawLength).put(']');
            }
            if (rawLength > 0) {
                Vect.memcpy(targetAddress, sourceAddress, rawLength);
            }
            return rawLength;
        }
        if (codec != DOUBLE_XOR) {
            throw invalid("unknown double codec tag [codec=").put(codec).put(']');
        }
        if (rowCount == 0) {
            if (storedLength != 0) {
                throw invalid("empty double page has trailing bytes [storedLength=").put(storedLength).put(']');
            }
            return 0;
        }
        if (storedLength < Long.BYTES) {
            throw invalid("truncated double XOR first value");
        }

        long previous = Unsafe.getLong(sourceAddress);
        Unsafe.putLong(targetAddress, previous);
        final BitReader bits = new BitReader(sourceAddress + Long.BYTES, storedLength - Long.BYTES);
        int previousLeading = -1;
        int previousTrailing = -1;
        for (int i = 1; i < rowCount; i++) {
            final long xor;
            if (bits.readBit() == 0) {
                xor = 0;
            } else if (bits.readBit() == 0) {
                if (previousLeading < 0) {
                    throw invalid("double XOR stream reuses a missing window");
                }
                final int significantBits = Long.SIZE - previousLeading - previousTrailing;
                final long significant = bits.readBits(significantBits);
                if (significant == 0) {
                    throw invalid("double XOR reuse encodes a zero XOR");
                }
                xor = significant << previousTrailing;
            } else {
                final int leading = (int) bits.readBits(6);
                final int storedSignificantBits = (int) bits.readBits(6);
                final int significantBits = storedSignificantBits == 0 ? Long.SIZE : storedSignificantBits;
                final int trailing = Long.SIZE - leading - significantBits;
                if (trailing < 0) {
                    throw invalid("double XOR window out of bounds")
                            .put(" [leading=").put(leading)
                            .put(", significantBits=").put(significantBits).put(']');
                }
                final long significant = bits.readBits(significantBits);
                if (significant == 0) {
                    throw invalid("double XOR window encodes a zero XOR");
                }
                xor = significant << trailing;
                if (Long.numberOfLeadingZeros(xor) != leading || Long.numberOfTrailingZeros(xor) != trailing) {
                    throw invalid("double XOR window is non-canonical");
                }
                previousLeading = leading;
                previousTrailing = trailing;
            }
            previous ^= xor;
            Unsafe.putLong(targetAddress + (long) i * Long.BYTES, previous);
        }
        bits.assertFullyConsumed();
        return storedLength;
    }

    /**
     * Decodes a complete raw 64-bit value stream into {@code targetAddress}. Long,
     * DATE and TIMESTAMP value rings store their payload verbatim rather than through
     * the double XOR stream: an arbitrary 64-bit value has no floating-point structure
     * to exploit, and reinterpreting it as a double could canonicalize a NaN bit
     * pattern and corrupt the stored value.
     *
     * @return bytes consumed, always {@code storedLength} on success
     */
    public static int decodeLongs(
            long sourceAddress,
            int storedLength,
            int codec,
            int rowCount,
            long targetAddress,
            int targetCapacity
    ) {
        validateDecodeArguments(sourceAddress, storedLength, rowCount, targetAddress, targetCapacity);
        if (codec != LONG_RAW_64) {
            throw invalid("unknown long codec tag [codec=").put(codec).put(']');
        }
        final int rawLength = rawLength(rowCount);
        if (storedLength != rawLength) {
            throw invalid("raw long page length mismatch")
                    .put(" [storedLength=").put(storedLength)
                    .put(", expected=").put(rawLength).put(']');
        }
        if (rawLength > 0) {
            Vect.memcpy(targetAddress, sourceAddress, rawLength);
        }
        return rawLength;
    }

    /**
     * Decodes a complete timestamp stream and rejects non-canonical varints,
     * checked-arithmetic overflow, decreasing output, truncation, and trailing
     * bytes before returning decoded state.
     *
     * @return bytes consumed, always {@code storedLength} on success
     */
    public static int decodeTimestamps(
            long sourceAddress,
            int storedLength,
            int codec,
            int rowCount,
            long targetAddress,
            int targetCapacity
    ) {
        validateDecodeArguments(sourceAddress, storedLength, rowCount, targetAddress, targetCapacity);
        if (codec == TIMESTAMP_RAW_64) {
            final int rawLength = rawLength(rowCount);
            if (storedLength != rawLength) {
                throw invalid("raw timestamp page length mismatch")
                        .put(" [storedLength=").put(storedLength)
                        .put(", expected=").put(rawLength).put(']');
            }
            if (rawLength > 0) {
                Vect.memcpy(targetAddress, sourceAddress, rawLength);
            }
            return rawLength;
        }
        if (codec != TIMESTAMP_DELTA_OF_DELTA_VARINT) {
            throw invalid("unknown timestamp codec tag [codec=").put(codec).put(']');
        }
        if (rowCount == 0) {
            if (storedLength != 0) {
                throw invalid("empty timestamp page has trailing bytes [storedLength=").put(storedLength).put(']');
            }
            return 0;
        }
        if (storedLength < Long.BYTES) {
            throw invalid("truncated timestamp first value");
        }

        int offset = Long.BYTES;
        long previousTimestamp = Unsafe.getLong(sourceAddress);
        Unsafe.putLong(targetAddress, previousTimestamp);
        long previousDelta = 0;
        for (int i = 1; i < rowCount; i++) {
            // The varint scan runs inline and hands the value back in a register.
            // Reading it out of the target buffer instead - which is where the
            // decoded timestamp lands a few instructions later - costs a native
            // store and the load that reads it straight back, once per row.
            long encoded = 0;
            int shift = 0;
            int bytes = 0;
            while (true) {
                if (offset >= storedLength) {
                    throw invalid("truncated LEB128 value");
                }
                final int b = Unsafe.getByte(sourceAddress + offset) & 0xff;
                offset++;
                bytes++;
                // The last byte a 64-bit value may spend carries bit 63 and nothing
                // else, so anything above it - a payload bit or another continuation
                // marker - overflows. Rejecting the continuation here is also what
                // bounds the loop.
                if (bytes == MAX_VARINT_BYTES && (b & 0xfe) != 0) {
                    throw invalid("LEB128 value overflows 64 bits");
                }
                encoded |= (long) (b & 0x7f) << shift;
                if ((b & 0x80) == 0) {
                    // A canonical encoding spends its last byte on at least one set
                    // bit, so a zero terminator means the writer padded a shorter
                    // value out.
                    if (bytes > 1 && b == 0) {
                        throw invalid("non-canonical LEB128 value");
                    }
                    break;
                }
                shift += 7;
            }
            final long delta;
            if (i == 1) {
                delta = encoded;
                if (delta < 0) {
                    throw invalid("timestamp delta exceeds signed range");
                }
            } else {
                try {
                    delta = Math.addExact(previousDelta, zigZagDecode(encoded));
                } catch (ArithmeticException e) {
                    throw invalid("timestamp delta arithmetic overflow");
                }
                if (delta < 0) {
                    throw invalid("decoded timestamp sequence decreases");
                }
            }
            previousTimestamp = checkedTimestampAdd(previousTimestamp, delta);
            Unsafe.putLong(targetAddress + (long) i * Long.BYTES, previousTimestamp);
            previousDelta = delta;
        }
        if (offset != storedLength) {
            throw invalid("timestamp stream has trailing bytes")
                    .put(" [consumed=").put(offset).put(", storedLength=").put(storedLength).put(']');
        }
        return offset;
    }

    /**
     * Encodes raw or exact-XOR double bits. Passing the adaptive selector's
     * result ensures the stream never expands for incompressible input.
     *
     * @return bytes appended
     */
    public static int encodeDoubles(
            @NotNull MemoryA sink,
            long sourceAddress,
            int rowCount,
            int codec
    ) {
        validateEncodeArguments(sourceAddress, rowCount);
        final long start = sink.getAppendOffset();
        if (codec == DOUBLE_RAW_64) {
            final int rawLength = rawLength(rowCount);
            if (rawLength > 0) {
                sink.putBlockOfBytes(sourceAddress, rawLength);
            }
        } else if (codec == DOUBLE_XOR) {
            encodeDoubleXor(sink, sourceAddress, rowCount);
        } else {
            throw CairoException.critical(0).put("unknown live view checkpoint double codec tag [codec=").put(codec).put(']');
        }
        return checkedWrittenLength(sink, start);
    }

    /**
     * Encodes a raw 64-bit value stream. Long, DATE and TIMESTAMP value rings store
     * their payload verbatim; {@link #LONG_RAW_64} is the only codec they use.
     *
     * @return bytes appended
     */
    public static int encodeLongs(
            @NotNull MemoryA sink,
            long sourceAddress,
            int rowCount,
            int codec
    ) {
        validateEncodeArguments(sourceAddress, rowCount);
        if (codec != LONG_RAW_64) {
            throw CairoException.critical(0).put("unknown live view checkpoint long codec tag [codec=").put(codec).put(']');
        }
        final long start = sink.getAppendOffset();
        final int rawLength = rawLength(rowCount);
        if (rawLength > 0) {
            sink.putBlockOfBytes(sourceAddress, rawLength);
        }
        return checkedWrittenLength(sink, start);
    }

    /**
     * Encodes raw or checked delta/delta-of-delta timestamps.
     *
     * @return bytes appended
     */
    public static int encodeTimestamps(
            @NotNull MemoryA sink,
            long sourceAddress,
            int rowCount,
            int codec
    ) {
        validateEncodeArguments(sourceAddress, rowCount);
        final long start = sink.getAppendOffset();
        if (codec == TIMESTAMP_RAW_64) {
            final int rawLength = rawLength(rowCount);
            if (rawLength > 0) {
                sink.putBlockOfBytes(sourceAddress, rawLength);
            }
        } else if (codec == TIMESTAMP_DELTA_OF_DELTA_VARINT) {
            encodeTimestampDeltaOfDelta(sink, sourceAddress, rowCount);
        } else {
            throw CairoException.critical(0).put("unknown live view checkpoint timestamp codec tag [codec=").put(codec).put(']');
        }
        return checkedWrittenLength(sink, start);
    }

    /**
     * Selects exact XOR only when it meets the fixed adaptive-saving rule.
     */
    public static int selectDoubleCodec(long sourceAddress, int rowCount) {
        validateEncodeArguments(sourceAddress, rowCount);
        final int rawLength = rawLength(rowCount);
        final int encodedLength = doubleXorLength(sourceAddress, rowCount);
        return savesEnough(rawLength, encodedLength) ? DOUBLE_XOR : DOUBLE_RAW_64;
    }

    /**
     * Selects checked delta-of-delta only when supported and sufficiently smaller.
     */
    public static int selectTimestampCodec(long sourceAddress, int rowCount) {
        validateEncodeArguments(sourceAddress, rowCount);
        final int rawLength = rawLength(rowCount);
        final int encodedLength = timestampDeltaOfDeltaLength(sourceAddress, rowCount);
        return encodedLength >= 0 && savesEnough(rawLength, encodedLength)
                ? TIMESTAMP_DELTA_OF_DELTA_VARINT
                : TIMESTAMP_RAW_64;
    }

    static int doubleXorLength(long sourceAddress, int rowCount) {
        if (rowCount == 0) {
            return 0;
        }
        long bitCount = 0;
        long previous = Unsafe.getLong(sourceAddress);
        int previousLeading = -1;
        int previousTrailing = -1;
        for (int i = 1; i < rowCount; i++) {
            final long value = Unsafe.getLong(sourceAddress + (long) i * Long.BYTES);
            final long xor = previous ^ value;
            if (xor == 0) {
                bitCount++;
            } else {
                final int leading = Long.numberOfLeadingZeros(xor);
                final int trailing = Long.numberOfTrailingZeros(xor);
                bitCount += 2;
                if (previousLeading >= 0 && leading >= previousLeading && trailing >= previousTrailing) {
                    bitCount += Long.SIZE - previousLeading - previousTrailing;
                } else {
                    bitCount += 12L + Long.SIZE - leading - trailing;
                    previousLeading = leading;
                    previousTrailing = trailing;
                }
            }
            previous = value;
        }
        return Long.BYTES + (int) ((bitCount + 7) >>> 3);
    }

    static int timestampDeltaOfDeltaLength(long sourceAddress, int rowCount) {
        if (rowCount == 0) {
            return 0;
        }
        int size = Long.BYTES;
        if (rowCount == 1) {
            return size;
        }
        long previousTimestamp = Unsafe.getLong(sourceAddress);
        long timestamp = Unsafe.getLong(sourceAddress + Long.BYTES);
        if (timestamp < previousTimestamp) {
            return -1;
        }
        final long firstDelta;
        try {
            firstDelta = Math.subtractExact(timestamp, previousTimestamp);
        } catch (ArithmeticException e) {
            return -1;
        }
        size += unsignedLeb128Length(firstDelta);
        long previousDelta = firstDelta;
        previousTimestamp = timestamp;
        for (int i = 2; i < rowCount; i++) {
            timestamp = Unsafe.getLong(sourceAddress + (long) i * Long.BYTES);
            if (timestamp < previousTimestamp) {
                return -1;
            }
            final long delta;
            final long deltaOfDelta;
            try {
                delta = Math.subtractExact(timestamp, previousTimestamp);
                deltaOfDelta = Math.subtractExact(delta, previousDelta);
            } catch (ArithmeticException e) {
                return -1;
            }
            size += unsignedLeb128Length(zigZagEncode(deltaOfDelta));
            previousTimestamp = timestamp;
            previousDelta = delta;
        }
        return size;
    }

    private static long checkedTimestampAdd(long timestamp, long delta) {
        final long next;
        try {
            next = Math.addExact(timestamp, delta);
        } catch (ArithmeticException e) {
            throw invalid("timestamp arithmetic overflow");
        }
        if (next < timestamp) {
            throw invalid("decoded timestamp sequence decreases");
        }
        return next;
    }

    private static int checkedWrittenLength(MemoryA sink, long start) {
        final long written = sink.getAppendOffset() - start;
        if (written < 0 || written > Integer.MAX_VALUE) {
            throw CairoException.critical(0).put("live view checkpoint encoded page size out of range [bytes=").put(written).put(']');
        }
        return (int) written;
    }

    private static void encodeDoubleXor(MemoryA sink, long sourceAddress, int rowCount) {
        if (rowCount == 0) {
            return;
        }
        long previous = Unsafe.getLong(sourceAddress);
        sink.putLong(previous);
        final BitWriter bits = new BitWriter(sink);
        int previousLeading = -1;
        int previousTrailing = -1;
        for (int i = 1; i < rowCount; i++) {
            final long value = Unsafe.getLong(sourceAddress + (long) i * Long.BYTES);
            final long xor = previous ^ value;
            if (xor == 0) {
                bits.writeBit(0);
            } else {
                bits.writeBit(1);
                final int leading = Long.numberOfLeadingZeros(xor);
                final int trailing = Long.numberOfTrailingZeros(xor);
                if (previousLeading >= 0 && leading >= previousLeading && trailing >= previousTrailing) {
                    bits.writeBit(0);
                    bits.writeBits(xor >>> previousTrailing, Long.SIZE - previousLeading - previousTrailing);
                } else {
                    bits.writeBit(1);
                    final int significantBits = Long.SIZE - leading - trailing;
                    bits.writeBits(leading, 6);
                    bits.writeBits(significantBits == Long.SIZE ? 0 : significantBits, 6);
                    bits.writeBits(xor >>> trailing, significantBits);
                    previousLeading = leading;
                    previousTrailing = trailing;
                }
            }
            previous = value;
        }
        bits.finish();
    }

    private static void encodeTimestampDeltaOfDelta(MemoryA sink, long sourceAddress, int rowCount) {
        final int expectedLength = timestampDeltaOfDeltaLength(sourceAddress, rowCount);
        if (expectedLength < 0) {
            throw CairoException.critical(0).put("live view checkpoint timestamp stream is decreasing or overflows delta arithmetic");
        }
        if (rowCount == 0) {
            return;
        }
        long previousTimestamp = Unsafe.getLong(sourceAddress);
        sink.putLong(previousTimestamp);
        if (rowCount == 1) {
            return;
        }
        long timestamp = Unsafe.getLong(sourceAddress + Long.BYTES);
        long previousDelta = timestamp - previousTimestamp;
        writeUnsignedLeb128(sink, previousDelta);
        previousTimestamp = timestamp;
        for (int i = 2; i < rowCount; i++) {
            timestamp = Unsafe.getLong(sourceAddress + (long) i * Long.BYTES);
            final long delta = timestamp - previousTimestamp;
            writeUnsignedLeb128(sink, zigZagEncode(delta - previousDelta));
            previousTimestamp = timestamp;
            previousDelta = delta;
        }
    }

    private static CairoException invalid(CharSequence reason) {
        return CairoException.critical(CairoException.LV_CHECKPOINT_TIMELINE_INVALID)
                .put("live view checkpoint state codec ").put(reason);
    }

    private static int rawLength(int rowCount) {
        return rowCount * Long.BYTES;
    }

    private static boolean savesEnough(int rawLength, int encodedLength) {
        final int minimumSaving = Math.max(MIN_SAVING_BYTES, (rawLength + 15) >>> 4);
        return rawLength - encodedLength >= minimumSaving;
    }

    private static int unsignedLeb128Length(long value) {
        int bytes = 1;
        while ((value & ~0x7fL) != 0) {
            value >>>= 7;
            bytes++;
        }
        return bytes;
    }

    private static void validateDecodeArguments(
            long sourceAddress,
            int storedLength,
            int rowCount,
            long targetAddress,
            int targetCapacity
    ) {
        if (rowCount < 0 || rowCount > CHUNK_ROWS) {
            throw invalid("row count out of bounds [rowCount=").put(rowCount).put(", max=").put(CHUNK_ROWS).put(']');
        }
        if (storedLength < 0) {
            throw invalid("stored length is negative [storedLength=").put(storedLength).put(']');
        }
        if (rowCount > targetCapacity || targetCapacity < 0) {
            throw invalid("decode target capacity too small")
                    .put(" [rowCount=").put(rowCount).put(", capacity=").put(targetCapacity).put(']');
        }
        if (storedLength > 0 && sourceAddress == 0) {
            throw invalid("null encoded source address");
        }
        if (rowCount > 0 && targetAddress == 0) {
            throw invalid("null decode target address");
        }
    }

    private static void validateEncodeArguments(long sourceAddress, int rowCount) {
        if (rowCount < 0 || rowCount > CHUNK_ROWS) {
            throw CairoException.critical(0)
                    .put("live view checkpoint codec row count out of bounds [rowCount=")
                    .put(rowCount).put(", max=").put(CHUNK_ROWS).put(']');
        }
        if (rowCount > 0 && sourceAddress == 0) {
            throw CairoException.critical(0).put("null live view checkpoint codec source address");
        }
    }

    private static void writeUnsignedLeb128(MemoryA sink, long value) {
        while ((value & ~0x7fL) != 0) {
            sink.putByte((byte) ((value & 0x7f) | 0x80));
            value >>>= 7;
        }
        sink.putByte((byte) value);
    }

    private static long zigZagDecode(long value) {
        return (value >>> 1) ^ -(value & 1);
    }

    private static long zigZagEncode(long value) {
        return (value << 1) ^ (value >> 63);
    }

    /**
     * Reusable, lazily allocated native input/output scratch for one logical
     * chunk. Allocation is fixed at two {@link #CHUNK_ROWS}-long arrays and is
     * charged to the owning live-view refresh tracker. The scratch must be
     * closed before its tracker is released.
     */
    public static final class Scratch implements Closeable {
        private final DirectLongList timestamps = new DirectLongList(CHUNK_ROWS, MemoryTag.NATIVE_LIVE_VIEW_IN_MEM, true);
        private final DirectLongList values = new DirectLongList(CHUNK_ROWS, MemoryTag.NATIVE_LIVE_VIEW_IN_MEM, true);
        private long timestampsAddress;
        private long valuesAddress;

        public Scratch(@Nullable MemoryTracker memoryTracker) {
            values.setMemoryTracker(memoryTracker);
            timestamps.setMemoryTracker(memoryTracker);
        }

        @Override
        public void close() {
            values.close();
            timestamps.close();
            timestampsAddress = 0;
            valuesAddress = 0;
        }

        /**
         * @return the timestamp buffer's address, allocating it on first use. Both
         * the ring walk and the ring builder address the buffer a row at a time, so
         * the accessor caches what {@link DirectLongList#reopen()} resolved rather
         * than re-entering the list on every row
         */
        public long timestampsAddress() {
            if (timestampsAddress == 0) {
                timestamps.reopen();
                timestampsAddress = timestamps.getAddress();
            }
            return timestampsAddress;
        }

        /**
         * @return the value buffer's address, allocating it on first use. See
         * {@link #timestampsAddress()} for why the address is cached
         */
        public long valuesAddress() {
            if (valuesAddress == 0) {
                values.reopen();
                valuesAddress = values.getAddress();
            }
            return valuesAddress;
        }
    }

    private static final class BitReader {
        private final long address;
        private final long bitLimit;
        private final int byteLength;
        private long bitPosition;

        private BitReader(long address, int byteLength) {
            this.address = address;
            this.byteLength = byteLength;
            this.bitLimit = (long) byteLength * BITS_PER_BYTE;
        }

        private void assertFullyConsumed() {
            final int bytesConsumed = (int) ((bitPosition + 7) >>> 3);
            if (bytesConsumed != byteLength) {
                throw invalid("double XOR stream has trailing bytes")
                        .put(" [consumed=").put(bytesConsumed).put(", stored=").put(byteLength).put(']');
            }
            final int usedBitsInLastByte = (int) (bitPosition & 7);
            if (usedBitsInLastByte != 0) {
                final int last = Unsafe.getByte(address + byteLength - 1L) & 0xff;
                final int paddingMask = ~((1 << usedBitsInLastByte) - 1) & 0xff;
                if ((last & paddingMask) != 0) {
                    throw invalid("double XOR stream has non-zero padding bits");
                }
            }
        }

        private int readBit() {
            return (int) readBits(1);
        }

        private long readBits(int count) {
            if (count < 0 || count > Long.SIZE || bitPosition > bitLimit - count) {
                throw invalid("truncated double XOR bitstream");
            }
            long value = 0;
            int consumed = 0;
            while (consumed < count) {
                final int bitInByte = (int) (bitPosition & 7);
                final int take = Math.min(count - consumed, BITS_PER_BYTE - bitInByte);
                final int b = Unsafe.getByte(address + (bitPosition >>> 3)) & 0xff;
                final long mask = (1L << take) - 1;
                value |= ((b >>> bitInByte) & mask) << consumed;
                consumed += take;
                bitPosition += take;
            }
            return value;
        }
    }

    private static final class BitWriter {
        private final MemoryA sink;
        private int bitCount;
        private int currentByte;

        private BitWriter(MemoryA sink) {
            this.sink = sink;
        }

        private void finish() {
            if (bitCount > 0) {
                sink.putByte((byte) currentByte);
                bitCount = 0;
                currentByte = 0;
            }
        }

        private void writeBit(int bit) {
            writeBits(bit, 1);
        }

        private void writeBits(long value, int count) {
            int consumed = 0;
            while (consumed < count) {
                final int take = Math.min(count - consumed, BITS_PER_BYTE - bitCount);
                final long mask = (1L << take) - 1;
                currentByte |= (int) (((value >>> consumed) & mask) << bitCount);
                consumed += take;
                bitCount += take;
                if (bitCount == BITS_PER_BYTE) {
                    sink.putByte((byte) currentByte);
                    bitCount = 0;
                    currentByte = 0;
                }
            }
        }
    }

}
