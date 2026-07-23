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

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoException;
import io.questdb.std.MemoryTracker;
import io.questdb.std.Misc;
import io.questdb.std.Unsafe;
import io.questdb.std.Vect;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.Closeable;
import java.util.Arrays;

/**
 * Copy-on-write chunk builder for the partitioned bounded-RANGE value ring.
 * Sealed chunks are reused by reference, expiration advances a logical offset
 * into the shared head, and only the rows this boundary appended are encoded.
 * The value column holds a 64-bit word per row: a DOUBLE ring stores exact
 * IEEE-754 bits (raw or XOR-compressed), a LONG/DATE/TIMESTAMP ring stores the
 * raw payload; {@link #of} configures which kind the seal writes. A {@code max}/
 * {@code min} frame ring uses the same payload but tags its value pages with the
 * deque page kinds so a deque-family root stays distinct from a value-ring root.
 * <p>
 * The tail a boundary appends is always a fresh chunk, so a chunk boundary sits
 * at every checkpoint boundary and a sealed chunk is never rewritten. That is
 * what keeps a seal proportional to the rows the batch added instead of the
 * live frame: reopening the previous tail to top it up would re-encode every
 * row already in it, which for a dense cadence is the whole frame, every time.
 * The price is one chunk per boundary spanned by the frame; the caller bounds
 * that by rebuilding from empty once a partition's chunk count reaches its cap
 * ({@link #getChunkCount()}).
 * <p>
 * The builder never reads a data page. Everything it carries forward - the
 * chunk references, row count, head offset and last timestamp - comes out of
 * the previous root's checksummed partition entry, which is what lets a repair
 * chain onto a boundary whose chunks are still in an unpublished segment.
 */
public class LiveViewCheckpointRangeRingStateBuilder implements Closeable {

    private int headOffset;
    private boolean initialized;
    private long lastTimestamp;
    private final LiveViewCheckpointRangeRingStateReader previousReader;
    private int refCount;
    private LiveViewCheckpointStatePageRef[] refs = new LiveViewCheckpointStatePageRef[8];
    private long rowCount;
    private final LiveViewCheckpointStateCodec.Scratch scratch;
    private int tailCount;
    private int valueKind = LiveViewCheckpointRangeRingStateReader.VALUE_KIND_DOUBLE;

    public LiveViewCheckpointRangeRingStateBuilder(@NotNull CairoConfiguration configuration) {
        this(configuration, null);
    }

    public LiveViewCheckpointRangeRingStateBuilder(
            @NotNull CairoConfiguration configuration,
            @Nullable MemoryTracker memoryTracker
    ) {
        previousReader = new LiveViewCheckpointRangeRingStateReader(configuration, memoryTracker);
        scratch = new LiveViewCheckpointStateCodec.Scratch(memoryTracker);
    }

    /**
     * Appends one row in designated-timestamp order. The value arrives as raw
     * 64-bit bits: a DOUBLE ring passes IEEE-754 bits (a NULL first/last/nth row
     * is a legitimate NaN), a LONG/DATE/TIMESTAMP ring passes the raw payload. The
     * value codec round-trips whatever bits it is handed, so no value is rejected.
     */
    public void append(
            @NotNull LiveViewCheckpointDataSegmentWriter writer,
            long timestamp,
            long valueBits
    ) {
        ensureInitialized();
        if (rowCount > 0 && timestamp < lastTimestamp) {
            throw CairoException.critical(0)
                    .put("live view checkpoint RANGE ring timestamps must be non-decreasing")
                    .put(" [previous=").put(lastTimestamp).put(", timestamp=").put(timestamp).put(']');
        }
        if (tailCount == LiveViewCheckpointStateCodec.CHUNK_ROWS) {
            sealTail(writer);
        }
        Unsafe.putLong(scratch.timestampsAddress() + (long) tailCount * Long.BYTES, timestamp);
        Unsafe.putLong(scratch.valuesAddress() + (long) tailCount * Long.BYTES, valueBits);
        tailCount++;
        rowCount++;
        lastTimestamp = timestamp;
    }

    @Override
    public void close() {
        Misc.free(previousReader);
        Misc.free(scratch);
        initialized = false;
        refs = new LiveViewCheckpointStatePageRef[0];
    }

    /**
     * Drops rows from the logical head without copying a shared page. Whole
     * chunks are unreferenced and a partial head is represented by headOffset.
     */
    public void dropHeadRows(long count) {
        ensureInitialized();
        if (count < 0 || count > rowCount) {
            throw CairoException.critical(0)
                    .put("live view checkpoint RANGE ring head drop out of bounds")
                    .put(" [count=").put(count).put(", rowCount=").put(rowCount).put(']');
        }
        long remaining = count;
        while (remaining > 0 && refCount > 0) {
            final int rows = refs[0].getRowCount();
            final int available = rows - headOffset;
            if (remaining < available) {
                headOffset += (int) remaining;
                rowCount -= remaining;
                return;
            }
            remaining -= available;
            rowCount -= available;
            removeFirstChunk();
            headOffset = 0;
        }
        if (remaining > 0) {
            // Only the rows this boundary appended are left to drop, and they sit
            // in scratch rather than in a page, so the head advances by moving them
            // down instead of by an offset.
            if (refCount != 0 || headOffset != 0 || remaining > tailCount) {
                throw CairoException.critical(0).put("live view checkpoint RANGE ring mutable head state inconsistent");
            }
            final int kept = tailCount - (int) remaining;
            if (kept > 0) {
                final long bytes = (long) kept * Long.BYTES;
                final long dropped = remaining * Long.BYTES;
                Vect.memmove(scratch.timestampsAddress(), scratch.timestampsAddress() + dropped, bytes);
                Vect.memmove(scratch.valuesAddress(), scratch.valuesAddress() + dropped, bytes);
            }
            rowCount -= remaining;
            tailCount = kept;
            remaining = 0;
        }
        if (remaining != 0 || rowCount < 0) {
            throw CairoException.critical(0).put("live view checkpoint RANGE ring head drop state inconsistent");
        }
        if (rowCount == 0) {
            lastTimestamp = 0;
            refCount = 0;
            tailCount = 0;
            headOffset = 0;
        }
    }

    /**
     * Freezes the candidate descriptor. The exact scalar continuation state (the
     * running aggregate for avg/sum, the emitted frame value for first_value/
     * last_value/nth_value) is stored by raw bits, and the frame size is stored
     * rather than recomputed, to preserve the exact continuation state.
     */
    public void freeze(
            @NotNull LiveViewCheckpointDataSegmentWriter writer,
            @NotNull byte[] key,
            long scalarBits,
            long frameSize,
            @NotNull LiveViewCheckpointPartitionMapEntry out
    ) {
        ensureInitialized();
        // A frame with an unbounded low bound counts rows into its aggregate and
        // then expires them from the ring, so frameSize may exceed the live rows.
        if (frameSize < 0) {
            throw CairoException.critical(0)
                    .put("live view checkpoint RANGE ring frame size out of bounds")
                    .put(" [frameSize=").put(frameSize).put(", rowCount=").put(rowCount).put(']');
        }
        if (tailCount > 0) {
            sealTail(writer);
        }
        if (rowCount == 0) {
            refCount = 0;
            headOffset = 0;
            lastTimestamp = 0;
        }
        validateLogicalBounds();
        final LiveViewCheckpointStatePageRef[] resultRefs = new LiveViewCheckpointStatePageRef[refCount];
        for (int i = 0; i < refCount; i++) {
            resultRefs[i] = LiveViewCheckpointPartitionMapEntry.copyRef(refs[i]);
        }
        out.of(
                key,
                LiveViewCheckpointRangeRingStateReader.encodeScalar(
                        headOffset,
                        rowCount,
                        scalarBits,
                        frameSize,
                        lastTimestamp
                ),
                resultRefs
        );
        initialized = false;
    }

    /**
     * @return the chunks this state currently holds, sealed plus the one the
     * appended tail will become
     */
    public int getChunkCount() {
        ensureInitialized();
        return refCount / 2 + (tailCount > 0 ? 1 : 0);
    }

    /**
     * @return the designated timestamp of the newest live row, or 0 for an empty
     * ring
     */
    public long getLastTimestamp() {
        ensureInitialized();
        return lastTimestamp;
    }

    public long getRowCount() {
        ensureInitialized();
        return rowCount;
    }

    /**
     * Carries the previous root's chunk references forward under the given value
     * kind. Reads no data page: {@code previous} is the checksummed partition
     * entry, and everything the builder needs is in it. The kind must match the
     * pages the previous root wrote - a function seals a partition under one value
     * kind across every boundary - so a mismatch means the caller wired the ring to
     * the wrong function.
     *
     * @param valueKind one of {@link LiveViewCheckpointRangeRingStateReader#VALUE_KIND_DOUBLE},
     *                  {@link LiveViewCheckpointRangeRingStateReader#VALUE_KIND_LONG},
     *                  {@link LiveViewCheckpointRangeRingStateReader#VALUE_KIND_DEQUE_DOUBLE}
     *                  or {@link LiveViewCheckpointRangeRingStateReader#VALUE_KIND_DEQUE_LONG}
     */
    public void of(@NotNull LiveViewCheckpointPartitionMapEntry previous, int valueKind) {
        previousReader.ofMetadata(previous);
        final LiveViewCheckpointStatePageRef[] previousRefs = previousReader.copyStatePageRefs();
        if (previousRefs.length > 0 && previousReader.getValueKind() != valueKind) {
            throw CairoException.critical(0)
                    .put("live view checkpoint RANGE ring value kind mismatch")
                    .put(" [configured=").put(valueKind).put(", previous=").put(previousReader.getValueKind()).put(']');
        }
        ensureRefCapacity(previousRefs.length);
        System.arraycopy(previousRefs, 0, refs, 0, previousRefs.length);
        refCount = previousRefs.length;
        rowCount = previousReader.getRowCount();
        headOffset = previousReader.getHeadOffset();
        lastTimestamp = previousReader.getLastTimestamp();
        tailCount = 0;
        this.valueKind = valueKind;
        initialized = true;
    }

    public void ofEmpty(int valueKind) {
        refCount = 0;
        rowCount = 0;
        headOffset = 0;
        lastTimestamp = 0;
        tailCount = 0;
        this.valueKind = valueKind;
        initialized = true;
    }

    private void ensureInitialized() {
        if (!initialized) {
            throw CairoException.critical(0).put("live view checkpoint RANGE ring state builder is not initialized");
        }
    }

    private void ensureRefCapacity(int capacity) {
        if (capacity > refs.length) {
            refs = Arrays.copyOf(refs, Math.max(capacity, refs.length * 2));
        }
    }

    private void removeFirstChunk() {
        if (refCount > 2) {
            System.arraycopy(refs, 2, refs, 0, refCount - 2);
        }
        // The shift leaves the vacated slots aliasing references that are still
        // live further down. Clear them so nothing can reach a chunk twice.
        refs[refCount - 1] = null;
        refs[refCount - 2] = null;
        refCount -= 2;
    }

    private void sealTail(@NotNull LiveViewCheckpointDataSegmentWriter writer) {
        if (tailCount <= 0) {
            return;
        }
        ensureRefCapacity(refCount + 2);
        if (refCount > LiveViewCheckpointMetadata.MAX_STATE_PAGE_REFS - 2) {
            throw CairoException.critical(0)
                    .put("live view checkpoint RANGE ring state page reference count exceeds format limit");
        }
        refs[refCount] = new LiveViewCheckpointStatePageRef();
        refs[refCount + 1] = new LiveViewCheckpointStatePageRef();
        final int timestampCodec = LiveViewCheckpointStateCodec.selectTimestampCodec(scratch.timestampsAddress(), tailCount);
        LiveViewCheckpointStateCodec.encodeTimestamps(
                writer.beginPage(), scratch.timestampsAddress(), tailCount, timestampCodec
        );
        writer.endPage(
                refs[refCount], tailCount * Long.BYTES,
                LiveViewCheckpointRangeRingStateReader.TIMESTAMP_PAGE_KIND,
                timestampCodec, tailCount, 0
        );
        final int valuePageKind = LiveViewCheckpointRangeRingStateReader.valuePageKind(valueKind);
        if (LiveViewCheckpointRangeRingStateReader.isLongColumn(valueKind)) {
            LiveViewCheckpointStateCodec.encodeLongs(
                    writer.beginPage(), scratch.valuesAddress(), tailCount, LiveViewCheckpointStateCodec.LONG_RAW_64
            );
            writer.endPage(
                    refs[refCount + 1], tailCount * Long.BYTES,
                    valuePageKind, LiveViewCheckpointStateCodec.LONG_RAW_64, tailCount, 0
            );
        } else {
            final int doubleCodec = LiveViewCheckpointStateCodec.selectDoubleCodec(scratch.valuesAddress(), tailCount);
            LiveViewCheckpointStateCodec.encodeDoubles(
                    writer.beginPage(), scratch.valuesAddress(), tailCount, doubleCodec
            );
            writer.endPage(
                    refs[refCount + 1], tailCount * Long.BYTES,
                    valuePageKind, doubleCodec, tailCount, 0
            );
        }
        refCount += 2;
        tailCount = 0;
    }

    private void validateLogicalBounds() {
        long physicalRows = 0;
        for (int i = 0; i < refCount; i += 2) {
            physicalRows += refs[i].getRowCount();
        }
        if ((rowCount == 0 && (physicalRows != 0 || headOffset != 0))
                || (rowCount > 0 && (refCount == 0 || headOffset < 0
                || headOffset >= refs[0].getRowCount() || physicalRows - headOffset != rowCount))) {
            throw CairoException.critical(0)
                    .put("live view checkpoint RANGE ring logical chunk bounds inconsistent")
                    .put(" [physicalRows=").put(physicalRows)
                    .put(", headOffset=").put(headOffset)
                    .put(", rowCount=").put(rowCount).put(']');
        }
    }
}
