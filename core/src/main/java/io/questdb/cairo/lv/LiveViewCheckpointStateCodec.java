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
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.idx.CoveringCompressor;
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
 * Format-1 adapter between immutable live-view checkpoint state pages and the
 * covering-index encodings in {@link CoveringCompressor}. The adapter implements
 * no compression of its own: it trials the covering candidates a page kind
 * allows, copies the shortest representation - raw storage included - and
 * dispatches a stored page back through the bounded checked decoders.
 *
 * <p>Codec tags are global within {@link LiveViewCheckpointStatePageRef} and the
 * page kind supplies the semantic type:</p>
 *
 * <table>
 *     <caption>format-1 codec tags</caption>
 *     <tr><td>{@link #RAW_64}</td><td>raw 64-bit words</td></tr>
 *     <tr><td>{@link #COVERING_LONG}</td><td>covering long block, plain FoR or
 *     linear-prediction FoR, distinguished by the block's own flag byte</td></tr>
 *     <tr><td>{@link #COVERING_DOUBLE}</td><td>covering ALP/FoR double block with
 *     exception positions and values</td></tr>
 * </table>
 *
 * <p>A timestamp page and an integer-oriented value page accept raw or covering
 * long; a double value page accepts raw or covering double. Selection is by exact
 * stored byte count with raw winning ties, so a page is never larger than the
 * payload it decodes to.</p>
 *
 * <p>What the decoders validate is FRAMING, not content. The checked covering
 * decoders reject every header field, extent and exception position that could
 * walk a read past the mapped page or a write past the target, and the raw paths
 * check the stored length and then {@code memcpy} - so a flipped bit inside a
 * stored accumulator decodes as a perfectly legal value. Neither is an integrity
 * check: like every other data payload in the engine, these pages carry no
 * checksum (the CRC32s live on the metadata pages and superblock slots - see
 * {@link LiveViewCheckpointLayout}). Read a successful decode as "well-formed",
 * never as "uncorrupted".</p>
 */
public final class LiveViewCheckpointStateCodec {

    public static final int CHUNK_ROWS = 4096;
    public static final int COVERING_DOUBLE = 2;
    public static final int COVERING_LONG = 1;
    public static final int RAW_64 = 0;
    /**
     * The {@code valueShift} {@link CoveringCompressor#compressDoubles} reads its
     * source stride by: a checkpoint value word is always a full 64-bit double,
     * never a promoted FLOAT.
     */
    private static final int DOUBLE_VALUE_SHIFT = 3;
    /**
     * The double ALP block is the widest thing an encoder may write: its worst
     * case stores every value as an exception, which costs more than the raw
     * payload. The region also holds the plain-FoR long candidate, which is
     * smaller.
     */
    private static final int ENCODE_DESTINATION_BYTES =
            align8(CoveringCompressor.maxCompressedSize(CHUNK_ROWS, ColumnType.DOUBLE));
    private static final int ENCODE_FLAGS_BYTES = CHUNK_ROWS;
    private static final int ENCODE_LINEAR_DESTINATION_BYTES =
            align8(CoveringCompressor.maxCompressedSize(CHUNK_ROWS, ColumnType.TIMESTAMP));
    private static final int ENCODE_LONG_WORKSPACE_BYTES = CHUNK_ROWS * Long.BYTES;
    private static final int ENCODE_SCRATCH_WORDS = (ENCODE_DESTINATION_BYTES + ENCODE_FLAGS_BYTES
            + ENCODE_LINEAR_DESTINATION_BYTES + ENCODE_LONG_WORKSPACE_BYTES) / Long.BYTES;

    private LiveViewCheckpointStateCodec() {
    }

    /**
     * Decodes a complete double value page into {@code targetAddress}. The page
     * carries either raw IEEE-754 bits or a covering ALP/FoR block; both paths
     * validate the stored length, the row count and the target capacity before
     * any native access, and the covering path additionally runs
     * {@link CoveringCompressor#decompressDoublesToAddrChecked} so a corrupt
     * block cannot read past the page or write past the target.
     *
     * @return bytes consumed, always {@code storedLength} on success
     */
    public static int decodeDoubles(
            long sourceAddress,
            int storedLength,
            int codec,
            int rowCount,
            long targetAddress,
            int targetCapacity,
            @NotNull Scratch scratch
    ) {
        validateDecodeArguments(sourceAddress, storedLength, rowCount, targetAddress, targetCapacity);
        if (codec == RAW_64) {
            return decodeRaw(sourceAddress, storedLength, rowCount, targetAddress, "double");
        }
        if (codec != COVERING_DOUBLE) {
            throw invalid("unknown double codec tag [codec=").put(codec).put(']');
        }
        final int status = CoveringCompressor.decompressDoublesToAddrChecked(
                sourceAddress,
                storedLength,
                rowCount,
                targetAddress,
                targetCapacity,
                scratch.decodeWorkspaceAddress(),
                CHUNK_ROWS
        );
        if (status != CoveringCompressor.DECODE_OK) {
            throw invalid("covering double block rejected [reason=")
                    .put(CoveringCompressor.decodeStatusName(status)).put(']');
        }
        return storedLength;
    }

    /**
     * Decodes a complete timestamp or integer-oriented value page into
     * {@code targetAddress}. The page carries either raw 64-bit words or a
     * covering long block in either layout - plain FoR or linear-prediction FoR -
     * which the checked decoder tells apart from the block's own flag byte.
     * <p>
     * Integer-oriented words never travel through the double codec: an arbitrary
     * 64-bit value has no floating-point structure for ALP to exploit, and
     * reinterpreting it as a double could canonicalize a NaN bit pattern and
     * corrupt the stored value.
     *
     * @return bytes consumed, always {@code storedLength} on success
     */
    public static int decodeLongs(
            long sourceAddress,
            int storedLength,
            int codec,
            int rowCount,
            long targetAddress,
            int targetCapacity,
            @NotNull Scratch scratch
    ) {
        validateDecodeArguments(sourceAddress, storedLength, rowCount, targetAddress, targetCapacity);
        if (codec == RAW_64) {
            return decodeRaw(sourceAddress, storedLength, rowCount, targetAddress, "long");
        }
        if (codec != COVERING_LONG) {
            throw invalid("unknown long codec tag [codec=").put(codec).put(']');
        }
        final int status = CoveringCompressor.decompressLongsToAddrChecked(
                sourceAddress,
                storedLength,
                rowCount,
                targetAddress,
                targetCapacity,
                scratch.decodeWorkspaceAddress(),
                CHUNK_ROWS
        );
        if (status != CoveringCompressor.DECODE_OK) {
            throw invalid("covering long block rejected [reason=")
                    .put(CoveringCompressor.decodeStatusName(status)).put(']');
        }
        return storedLength;
    }

    /**
     * Appends a double value page, choosing the shorter of the covering ALP/FoR
     * block and the raw payload. Raw wins ties, so the page never exceeds
     * {@code rowCount * 8} bytes.
     * <p>
     * The ALP transform is bit-exact: NaNs, infinities, signed zero and every
     * value that does not round-trip through the selected decimal transform go
     * into the block's exception list rather than being approximated.
     *
     * @return the format-1 codec tag the caller must record for the page
     */
    public static int encodeDoubles(
            @NotNull MemoryA sink,
            @NotNull Scratch scratch,
            long sourceAddress,
            int rowCount
    ) {
        validateEncodeArguments(sourceAddress, rowCount);
        final int rawLength = rawLength(rowCount);
        if (rowCount > 0) {
            final long destination = scratch.encodeDestinationAddress();
            final int encodedLength = CoveringCompressor.compressDoubles(
                    sourceAddress,
                    rowCount,
                    DOUBLE_VALUE_SHIFT,
                    destination,
                    scratch.encodeLongWorkspaceAddress(),
                    scratch.encodeFlagsAddress()
            );
            if (encodedLength < rawLength) {
                sink.putBlockOfBytes(destination, encodedLength);
                return COVERING_DOUBLE;
            }
        }
        putRaw(sink, sourceAddress, rawLength);
        return RAW_64;
    }

    /**
     * Appends an integer-oriented value page, choosing the shorter of the
     * covering plain-FoR long block and the raw payload. Raw wins ties.
     * <p>
     * Wide decimals arrive flattened into their 64-bit words, most significant
     * first, and are trialled as one word stream: repeated or sign-extended high
     * words compress, arbitrary low words force a 64-bit width and lose to raw.
     *
     * @return the format-1 codec tag the caller must record for the page
     */
    public static int encodeLongs(
            @NotNull MemoryA sink,
            @NotNull Scratch scratch,
            long sourceAddress,
            int rowCount
    ) {
        validateEncodeArguments(sourceAddress, rowCount);
        final int rawLength = rawLength(rowCount);
        if (rowCount > 0) {
            final long destination = scratch.encodeDestinationAddress();
            final int encodedLength = CoveringCompressor.compressLongs(sourceAddress, rowCount, destination);
            if (encodedLength < rawLength) {
                sink.putBlockOfBytes(destination, encodedLength);
                return COVERING_LONG;
            }
        }
        putRaw(sink, sourceAddress, rawLength);
        return RAW_64;
    }

    /**
     * Appends a timestamp page, choosing the shortest of the raw payload, the
     * covering plain-FoR long block and the covering linear-prediction long
     * block. Raw wins ties.
     * <p>
     * Both covering layouts share {@link #COVERING_LONG}: the block's own flag
     * byte says which one it is, so the page reference does not have to. The
     * linear candidate is trialled only for a non-descending stream, which is
     * what its stride is fitted to; a small page can still be shorter under the
     * 13-byte plain header even when the linear residuals are all zero.
     *
     * @return the format-1 codec tag the caller must record for the page
     */
    public static int encodeTimestamps(
            @NotNull MemoryA sink,
            @NotNull Scratch scratch,
            long sourceAddress,
            int rowCount
    ) {
        validateEncodeArguments(sourceAddress, rowCount);
        final int rawLength = rawLength(rowCount);
        // A zero winner is the raw candidate: the scratch regions are allocated, so
        // neither covering candidate can sit at address zero.
        long winnerAddress = 0;
        int winnerLength = rawLength;
        if (rowCount > 0) {
            final long plainDestination = scratch.encodeDestinationAddress();
            final int plainLength = CoveringCompressor.compressLongs(sourceAddress, rowCount, plainDestination);
            if (plainLength < winnerLength) {
                winnerAddress = plainDestination;
                winnerLength = plainLength;
            }
            if (hasNonDescendingBounds(sourceAddress, rowCount)) {
                final long linearDestination = scratch.encodeLinearDestinationAddress();
                final int linearLength = CoveringCompressor.compressLongsLinearPred(
                        sourceAddress, rowCount, linearDestination, scratch.encodeLongWorkspaceAddress()
                );
                if (linearLength < winnerLength) {
                    winnerAddress = linearDestination;
                    winnerLength = linearLength;
                }
            }
        }
        if (winnerAddress == 0) {
            putRaw(sink, sourceAddress, rawLength);
            return RAW_64;
        }
        sink.putBlockOfBytes(winnerAddress, winnerLength);
        return COVERING_LONG;
    }

    private static int align8(int bytes) {
        return (bytes + 7) & ~7;
    }

    private static int decodeRaw(
            long sourceAddress,
            int storedLength,
            int rowCount,
            long targetAddress,
            CharSequence what
    ) {
        final int rawLength = rawLength(rowCount);
        if (storedLength != rawLength) {
            throw invalid("raw ").put(what).put(" page length mismatch")
                    .put(" [storedLength=").put(storedLength)
                    .put(", expected=").put(rawLength).put(']');
        }
        if (rawLength > 0) {
            Vect.memcpy(targetAddress, sourceAddress, rawLength);
        }
        return rawLength;
    }

    /**
     * @return whether the stream's last word is at or above its first, which is
     * the precondition {@link CoveringCompressor#compressLongsLinearPred} fits a
     * stride to. A ring's timestamps are non-decreasing by construction, so a
     * stream that fails this came from a caller handing the codec something else
     */
    private static boolean hasNonDescendingBounds(long sourceAddress, int rowCount) {
        return Unsafe.getLong(sourceAddress + (long) (rowCount - 1) * Long.BYTES) >= Unsafe.getLong(sourceAddress);
    }

    private static CairoException invalid(CharSequence reason) {
        return CairoException.critical(CairoException.LV_CHECKPOINT_TIMELINE_INVALID)
                .put("live view checkpoint state codec ").put(reason);
    }

    private static void putRaw(MemoryA sink, long sourceAddress, int rawLength) {
        if (rawLength > 0) {
            sink.putBlockOfBytes(sourceAddress, rawLength);
        }
    }

    private static int rawLength(int rowCount) {
        return rowCount * Long.BYTES;
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
        // The encoder scratch regions are sized for CHUNK_ROWS words, so this is
        // what bounds every covering encoder's writes into them.
        if (rowCount < 0 || rowCount > CHUNK_ROWS) {
            throw CairoException.critical(0)
                    .put("live view checkpoint codec row count out of bounds [rowCount=")
                    .put(rowCount).put(", max=").put(CHUNK_ROWS).put(']');
        }
        if (rowCount > 0 && sourceAddress == 0) {
            throw CairoException.critical(0).put("null live view checkpoint codec source address");
        }
    }

    /**
     * Reusable, lazily allocated native scratch for one logical chunk, charged to
     * the owning live-view refresh tracker and closed before that tracker is
     * released.
     * <p>
     * The timestamp and value buffers hold one chunk's decoded words. The decode
     * workspace holds the residuals or ALP words a checked covering decode needs,
     * and the encoder region holds the covering candidates a seal trials plus the
     * workspaces the encoders write them through. Each region is a single
     * contiguous allocation, because every one of them reaches a covering
     * encoder or decoder as a bare native address with a declared capacity.
     * <p>
     * A restore-only reader never touches the encoder region, which is the
     * largest of them, so it is opened by the first encode rather than with the
     * scratch.
     */
    public static final class Scratch implements Closeable {
        private final DirectLongList encodeRegion =
                new DirectLongList(ENCODE_SCRATCH_WORDS, MemoryTag.NATIVE_LIVE_VIEW_IN_MEM, true);
        private final DirectLongList timestamps =
                new DirectLongList(CHUNK_ROWS, MemoryTag.NATIVE_LIVE_VIEW_IN_MEM, true);
        private final DirectLongList values =
                new DirectLongList(CHUNK_ROWS, MemoryTag.NATIVE_LIVE_VIEW_IN_MEM, true);
        private final DirectLongList workspace =
                new DirectLongList(CHUNK_ROWS, MemoryTag.NATIVE_LIVE_VIEW_IN_MEM, true);
        private long encodeRegionAddress;
        private long timestampsAddress;
        private long valuesAddress;
        private long workspaceAddress;

        public Scratch(@Nullable MemoryTracker memoryTracker) {
            encodeRegion.setMemoryTracker(memoryTracker);
            timestamps.setMemoryTracker(memoryTracker);
            values.setMemoryTracker(memoryTracker);
            workspace.setMemoryTracker(memoryTracker);
        }

        @Override
        public void close() {
            encodeRegion.close();
            timestamps.close();
            values.close();
            workspace.close();
            encodeRegionAddress = 0;
            timestampsAddress = 0;
            valuesAddress = 0;
            workspaceAddress = 0;
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

        private long decodeWorkspaceAddress() {
            if (workspaceAddress == 0) {
                workspace.reopen();
                workspaceAddress = workspace.getAddress();
            }
            return workspaceAddress;
        }

        private long encodeDestinationAddress() {
            return encodeRegionAddress();
        }

        private long encodeFlagsAddress() {
            return encodeRegionAddress()
                    + ENCODE_DESTINATION_BYTES + ENCODE_LINEAR_DESTINATION_BYTES + ENCODE_LONG_WORKSPACE_BYTES;
        }

        private long encodeLinearDestinationAddress() {
            return encodeRegionAddress() + ENCODE_DESTINATION_BYTES;
        }

        private long encodeLongWorkspaceAddress() {
            return encodeRegionAddress() + ENCODE_DESTINATION_BYTES + ENCODE_LINEAR_DESTINATION_BYTES;
        }

        private long encodeRegionAddress() {
            if (encodeRegionAddress == 0) {
                encodeRegion.reopen();
                encodeRegionAddress = encodeRegion.getAddress();
            }
            return encodeRegionAddress;
        }
    }
}
