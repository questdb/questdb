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
import io.questdb.std.Numbers;
import io.questdb.std.Transient;
import io.questdb.std.Unsafe;
import io.questdb.std.Vect;
import io.questdb.std.str.Path;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.Closeable;
import java.util.Arrays;

/**
 * Copy-on-write chunk builder for the partitioned bounded-RANGE double-average
 * ring. Sealed chunks are reused by reference, expiration advances a logical
 * offset into the shared head, and only a tail that receives new rows is copied
 * and frozen into the candidate data segment.
 */
public class LiveViewCheckpointAvgDoubleRangeStateBuilder implements Closeable {

    private int headOffset;
    private boolean initialized;
    private long lastTimestamp;
    private final LiveViewCheckpointAvgDoubleRangeStateReader previousReader;
    private int refCount;
    private LiveViewCheckpointStatePageRef[] refs = new LiveViewCheckpointStatePageRef[8];
    private long rowCount;
    private final LiveViewCheckpointStateCodec.Scratch scratch;
    private int tailCount;
    private boolean tailMutable;

    public LiveViewCheckpointAvgDoubleRangeStateBuilder(@NotNull CairoConfiguration configuration) {
        this(configuration, null);
    }

    public LiveViewCheckpointAvgDoubleRangeStateBuilder(
            @NotNull CairoConfiguration configuration,
            @Nullable MemoryTracker memoryTracker
    ) {
        previousReader = new LiveViewCheckpointAvgDoubleRangeStateReader(configuration, memoryTracker);
        scratch = new LiveViewCheckpointStateCodec.Scratch(memoryTracker);
    }

    /** Appends one finite row in designated-timestamp order. */
    public void append(
            @NotNull LiveViewCheckpointDataSegmentWriter writer,
            long timestamp,
            double value
    ) {
        ensureInitialized();
        if (!Numbers.isFinite(value)) {
            throw CairoException.critical(0).put("live view checkpoint avg RANGE ring accepts finite values only");
        }
        if (rowCount > 0 && timestamp < lastTimestamp) {
            throw CairoException.critical(0)
                    .put("live view checkpoint avg RANGE timestamps must be non-decreasing")
                    .put(" [previous=").put(lastTimestamp).put(", timestamp=").put(timestamp).put(']');
        }
        makeTailMutable();
        if (tailCount == LiveViewCheckpointStateCodec.CHUNK_ROWS && refCount == 0 && headOffset > 0) {
            compactMutableHead();
        }
        if (tailCount == LiveViewCheckpointStateCodec.CHUNK_ROWS) {
            sealMutableTail(writer);
        }
        Unsafe.putLong(scratch.timestampsAddress() + (long) tailCount * Long.BYTES, timestamp);
        Unsafe.putLong(
                scratch.doublesAddress() + (long) tailCount * Long.BYTES,
                Double.doubleToRawLongBits(value)
        );
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
                    .put("live view checkpoint avg RANGE head drop out of bounds")
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
            if (!tailMutable || refCount != 0 || remaining > tailCount - headOffset) {
                throw CairoException.critical(0).put("live view checkpoint avg RANGE mutable head state inconsistent");
            }
            final int available = tailCount - headOffset;
            if (remaining < available) {
                headOffset += (int) remaining;
                rowCount -= remaining;
                return;
            }
            rowCount -= available;
            remaining -= available;
            tailCount = 0;
            headOffset = 0;
        }
        if (remaining != 0 || rowCount < 0) {
            throw CairoException.critical(0).put("live view checkpoint avg RANGE head drop state inconsistent");
        }
        if (rowCount == 0) {
            lastTimestamp = 0;
            refCount = 0;
            tailCount = 0;
            headOffset = 0;
        }
    }

    /**
     * Freezes the candidate descriptor. The exact aggregate sum and frame size
     * are stored, rather than recomputed, to preserve floating-point state bits.
     */
    public void freeze(
            @NotNull LiveViewCheckpointDataSegmentWriter writer,
            @NotNull byte[] key,
            double sum,
            long frameSize,
            @NotNull LiveViewCheckpointPartitionMapEntry out
    ) {
        ensureInitialized();
        if (frameSize < 0 || frameSize > rowCount) {
            throw CairoException.critical(0)
                    .put("live view checkpoint avg RANGE frame size out of bounds")
                    .put(" [frameSize=").put(frameSize).put(", rowCount=").put(rowCount).put(']');
        }
        if (tailMutable && tailCount > 0) {
            sealMutableTail(writer);
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
                LiveViewCheckpointAvgDoubleRangeStateReader.encodeScalar(
                        headOffset,
                        rowCount,
                        Double.doubleToRawLongBits(sum),
                        frameSize,
                        lastTimestamp
                ),
                resultRefs
        );
        initialized = false;
    }

    public void of(
            @Transient @NotNull Path checkpointsDir,
            @NotNull LiveViewCheckpointSegmentDirectory segmentDirectory,
            @NotNull LiveViewCheckpointPartitionMapEntry previous
    ) {
        previousReader.of(checkpointsDir, segmentDirectory, previous);
        final LiveViewCheckpointStatePageRef[] previousRefs = previousReader.copyStatePageRefs();
        ensureRefCapacity(previousRefs.length);
        for (int i = 0; i < previousRefs.length; i++) {
            refs[i] = previousRefs[i];
        }
        refCount = previousRefs.length;
        rowCount = previousReader.getRowCount();
        headOffset = previousReader.getHeadOffset();
        lastTimestamp = previousReader.getLastTimestamp();
        tailCount = 0;
        tailMutable = false;
        initialized = true;
    }

    public void ofEmpty() {
        refCount = 0;
        rowCount = 0;
        headOffset = 0;
        lastTimestamp = 0;
        tailCount = 0;
        tailMutable = false;
        initialized = true;
    }

    private void compactMutableHead() {
        final int live = tailCount - headOffset;
        if (live > 0) {
            final long bytes = (long) live * Long.BYTES;
            Vect.memmove(scratch.timestampsAddress(), scratch.timestampsAddress() + (long) headOffset * Long.BYTES, bytes);
            Vect.memmove(scratch.doublesAddress(), scratch.doublesAddress() + (long) headOffset * Long.BYTES, bytes);
        }
        tailCount = live;
        headOffset = 0;
    }

    private void ensureInitialized() {
        if (!initialized) {
            throw CairoException.critical(0).put("live view checkpoint avg RANGE state builder is not initialized");
        }
    }

    private void ensureRefCapacity(int capacity) {
        if (capacity > refs.length) {
            refs = Arrays.copyOf(refs, Math.max(capacity, refs.length * 2));
        }
    }

    private void makeTailMutable() {
        if (tailMutable) {
            return;
        }
        tailMutable = true;
        tailCount = 0;
        if (refCount == 0) {
            return;
        }
        final int lastRows = refs[refCount - 2].getRowCount();
        if (lastRows < LiveViewCheckpointStateCodec.CHUNK_ROWS) {
            tailCount = previousReader.decodeChunk(
                    refs[refCount - 2], refs[refCount - 1],
                    scratch.timestampsAddress(), scratch.doublesAddress()
            );
            refCount -= 2;
        }
    }

    private void removeFirstChunk() {
        if (refCount > 2) {
            System.arraycopy(refs, 2, refs, 0, refCount - 2);
        }
        refCount -= 2;
    }

    private void sealMutableTail(@NotNull LiveViewCheckpointDataSegmentWriter writer) {
        if (tailCount <= 0) {
            return;
        }
        ensureRefCapacity(refCount + 2);
        if (refCount > LiveViewCheckpointMetadata.MAX_STATE_PAGE_REFS - 2) {
            throw CairoException.critical(0)
                    .put("live view checkpoint avg RANGE state page reference count exceeds format limit");
        }
        if (refs[refCount] == null) {
            refs[refCount] = new LiveViewCheckpointStatePageRef();
        }
        if (refs[refCount + 1] == null) {
            refs[refCount + 1] = new LiveViewCheckpointStatePageRef();
        }
        final int timestampCodec = LiveViewCheckpointStateCodec.selectTimestampCodec(scratch.timestampsAddress(), tailCount);
        LiveViewCheckpointStateCodec.encodeTimestamps(
                writer.beginPage(), scratch.timestampsAddress(), tailCount, timestampCodec
        );
        writer.endPage(
                refs[refCount], tailCount * Long.BYTES,
                LiveViewCheckpointAvgDoubleRangeStateReader.TIMESTAMP_PAGE_KIND,
                timestampCodec, tailCount, 0
        );
        final int doubleCodec = LiveViewCheckpointStateCodec.selectDoubleCodec(scratch.doublesAddress(), tailCount);
        LiveViewCheckpointStateCodec.encodeDoubles(
                writer.beginPage(), scratch.doublesAddress(), tailCount, doubleCodec
        );
        writer.endPage(
                refs[refCount + 1], tailCount * Long.BYTES,
                LiveViewCheckpointAvgDoubleRangeStateReader.VALUE_PAGE_KIND,
                doubleCodec, tailCount, 0
        );
        refCount += 2;
        tailCount = 0;
        tailMutable = true;
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
                    .put("live view checkpoint avg RANGE logical chunk bounds inconsistent")
                    .put(" [physicalRows=").put(physicalRows)
                    .put(", headOffset=").put(headOffset)
                    .put(", rowCount=").put(rowCount).put(']');
        }
    }
}
