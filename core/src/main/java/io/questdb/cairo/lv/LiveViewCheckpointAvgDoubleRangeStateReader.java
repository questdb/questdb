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
import io.questdb.std.str.Path;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.Closeable;

/**
 * Restores the persistent chunked ring used by partitioned {@code avg(double)}
 * and {@code sum(double)} over a bounded RANGE frame. Each logical chunk is a
 * timestamp page followed by an exact-double page. The partition entry's
 * checksummed scalar payload owns the logical head offset and exact aggregate
 * continuation state.
 * <p>
 * Chunks carry whatever row count the seal that wrote them appended, capped at
 * {@link LiveViewCheckpointStateCodec#CHUNK_ROWS}: a cadence seal closes its
 * tail so the next root can reference it rather than copy it, which puts a
 * chunk boundary at every checkpoint boundary. The scalar row count and head
 * offset, not the chunk sizes, say which rows are live.
 */
public class LiveViewCheckpointAvgDoubleRangeStateReader implements Closeable, LiveViewCheckpointRingStateSource {

    public static final int FORMAT_VERSION = 1;
    public static final int SCALAR_STATE_BYTES = 5 * Long.BYTES;
    public static final int TIMESTAMP_PAGE_KIND = 0x21;
    public static final int VALUE_PAGE_KIND = 0x22;
    private static final int FLAGS = 0;
    private final Path checkpointsDir = new Path();
    private final LiveViewCheckpointDataSegmentReader dataReader;
    private long frameSize;
    private int headOffset;
    private boolean initialized;
    private long lastTimestamp;
    private long openSegmentId = -1;
    private LiveViewCheckpointSegmentDirectory segmentDirectory;
    private final LiveViewCheckpointStateCodec.Scratch scratch;
    private LiveViewCheckpointStatePageRef[] statePageRefs = new LiveViewCheckpointStatePageRef[0];
    private long sumBits;
    private long rowCount;

    public LiveViewCheckpointAvgDoubleRangeStateReader(@NotNull CairoConfiguration configuration) {
        this(configuration, null);
    }

    public LiveViewCheckpointAvgDoubleRangeStateReader(
            @NotNull CairoConfiguration configuration,
            @Nullable MemoryTracker memoryTracker
    ) {
        dataReader = new LiveViewCheckpointDataSegmentReader(configuration);
        scratch = new LiveViewCheckpointStateCodec.Scratch(memoryTracker);
    }

    @Override
    public void close() {
        Misc.free(dataReader);
        Misc.free(scratch);
        Misc.free(checkpointsDir);
        initialized = false;
        openSegmentId = -1;
        segmentDirectory = null;
        statePageRefs = new LiveViewCheckpointStatePageRef[0];
    }

    @Override
    public long getFrameSize() {
        ensureInitialized();
        return frameSize;
    }

    public int getHeadOffset() {
        ensureInitialized();
        return headOffset;
    }

    public long getLastTimestamp() {
        ensureInitialized();
        return lastTimestamp;
    }

    @Override
    public long getRowCount() {
        ensureInitialized();
        return rowCount;
    }

    public int getStatePageCount() {
        ensureInitialized();
        return statePageRefs.length;
    }

    public void getStatePageRef(int index, @NotNull LiveViewCheckpointStatePageRef out) {
        ensureInitialized();
        copyRef(statePageRefs[index], out);
    }

    @Override
    public double getSum() {
        ensureInitialized();
        return Double.longBitsToDouble(sumBits);
    }

    /**
     * Decodes every live row in canonical ring order. Payload validation is
     * deliberately lazy: opening the root validates bounded metadata only,
     * while a malformed referenced data page invalidates the root when read.
     */
    @Override
    public void forEachRow(@NotNull RowConsumer consumer) {
        ensureInitialized();
        ensureBound();
        long rowsRead = 0;
        long previousTimestamp = 0;
        boolean hasPrevious = false;
        for (int chunk = 0, n = statePageRefs.length / 2; chunk < n; chunk++) {
            final int physicalRows = decodeChunk(chunk, scratch.timestampsAddress(), scratch.doublesAddress());
            final int lo = chunk == 0 ? headOffset : 0;
            for (int i = 0; i < physicalRows; i++) {
                final long timestamp = Unsafe.getLong(scratch.timestampsAddress() + (long) i * Long.BYTES);
                final double value = Double.longBitsToDouble(
                        Unsafe.getLong(scratch.doublesAddress() + (long) i * Long.BYTES)
                );
                if ((hasPrevious && timestamp < previousTimestamp) || !Numbers.isFinite(value)) {
                    throw invalid("avg RANGE chunk rows are not canonical")
                            .put(" [chunk=").put(chunk).put(", row=").put(i).put(']');
                }
                previousTimestamp = timestamp;
                hasPrevious = true;
                if (i >= lo) {
                    consumer.accept(timestamp, value);
                    rowsRead++;
                }
            }
        }
        if (rowsRead != rowCount || (rowCount > 0 && (!hasPrevious || previousTimestamp != lastTimestamp))) {
            throw invalid("avg RANGE scalar/page bounds mismatch")
                    .put(" [decodedRows=").put(rowsRead)
                    .put(", expectedRows=").put(rowCount)
                    .put(", decodedLastTimestamp=").put(previousTimestamp)
                    .put(", expectedLastTimestamp=").put(lastTimestamp).put(']');
        }
    }

    /**
     * Opens {@code entry} for both metadata and payload access.
     */
    public void of(
            @Transient @NotNull Path checkpointsDir,
            @NotNull LiveViewCheckpointSegmentDirectory segmentDirectory,
            @NotNull LiveViewCheckpointPartitionMapEntry entry
    ) {
        ofMetadata(entry);
        this.checkpointsDir.of(checkpointsDir);
        this.segmentDirectory = segmentDirectory;
    }

    /**
     * Decodes and validates {@code entry}'s scalar payload and chunk references
     * without binding a data segment to read them from.
     * <p>
     * A cadence seal starts from the previous root this way: it needs the row
     * count, head offset, last timestamp and chunk references to carry the
     * shared prefix forward, and none of those live in a data page. It also
     * means a repair can chain one captured boundary onto the one before it,
     * whose chunks are still sitting in an unpublished temporary segment that no
     * reader could open.
     */
    public void ofMetadata(@NotNull LiveViewCheckpointPartitionMapEntry entry) {
        initialized = false;
        openSegmentId = -1;
        this.segmentDirectory = null;
        final byte[] scalar = entry.getScalarState();
        if (scalar.length != SCALAR_STATE_BYTES) {
            throw invalid("avg RANGE scalar state size mismatch")
                    .put(" [expected=").put(SCALAR_STATE_BYTES).put(", actual=").put(scalar.length).put(']');
        }
        final long header = getLong(scalar, 0);
        final int version = (int) header;
        headOffset = (int) (header >>> 32);
        if (version != FORMAT_VERSION) {
            throw invalid("avg RANGE state format version mismatch")
                    .put(" [expected=").put(FORMAT_VERSION).put(", actual=").put(version).put(']');
        }
        rowCount = getLong(scalar, Long.BYTES);
        sumBits = getLong(scalar, 2 * Long.BYTES);
        frameSize = getLong(scalar, 3 * Long.BYTES);
        lastTimestamp = getLong(scalar, 4 * Long.BYTES);

        final int refCount = entry.getStatePageCount();
        if ((refCount & 1) != 0 || refCount > LiveViewCheckpointMetadata.MAX_STATE_PAGE_REFS) {
            throw invalid("avg RANGE state page reference count invalid, count=").put(refCount);
        }
        statePageRefs = new LiveViewCheckpointStatePageRef[refCount];
        long physicalRows = 0;
        for (int i = 0; i < refCount; i += 2) {
            final LiveViewCheckpointStatePageRef timestampRef = entry.getStatePageRef(i);
            final LiveViewCheckpointStatePageRef valueRef = entry.getStatePageRef(i + 1);
            validateTimestampRef(timestampRef);
            validateValueRef(valueRef);
            if (timestampRef.getRowCount() != valueRef.getRowCount()) {
                throw invalid("avg RANGE chunk stream row counts differ")
                        .put(" [timestamps=").put(timestampRef.getRowCount())
                        .put(", values=").put(valueRef.getRowCount()).put(']');
            }
            if (physicalRows > Long.MAX_VALUE - timestampRef.getRowCount()) {
                throw invalid("avg RANGE physical row count overflow");
            }
            physicalRows += timestampRef.getRowCount();
            statePageRefs[i] = LiveViewCheckpointPartitionMapEntry.copyRef(timestampRef);
            statePageRefs[i + 1] = LiveViewCheckpointPartitionMapEntry.copyRef(valueRef);
        }
        // frameSize is the function's own aggregate cardinality, not a ring index:
        // a frame whose low bound is unbounded folds rows into the aggregate and
        // then drops them from the ring, so it counts rows the ring no longer
        // holds. Only its sign is structural here.
        if (rowCount < 0 || frameSize < 0) {
            throw invalid("avg RANGE scalar row counts invalid")
                    .put(" [rowCount=").put(rowCount).put(", frameSize=").put(frameSize).put(']');
        }
        if (rowCount == 0) {
            if (refCount != 0 || headOffset != 0 || lastTimestamp != 0) {
                throw invalid("avg RANGE empty state is not canonical");
            }
        } else if (refCount == 0 || headOffset < 0
                || headOffset >= statePageRefs[0].getRowCount()
                || physicalRows - headOffset != rowCount) {
            throw invalid("avg RANGE logical chunk bounds invalid")
                    .put(" [physicalRows=").put(physicalRows)
                    .put(", headOffset=").put(headOffset)
                    .put(", rowCount=").put(rowCount).put(']');
        }
        initialized = true;
    }

    int decodeChunk(int chunkIndex, long timestampAddress, long valueAddress) {
        final LiveViewCheckpointStatePageRef timestampRef = statePageRefs[chunkIndex * 2];
        final LiveViewCheckpointStatePageRef valueRef = statePageRefs[chunkIndex * 2 + 1];
        decodeTimestamps(timestampRef, timestampAddress);
        decodeValues(valueRef, valueAddress);
        return timestampRef.getRowCount();
    }

    LiveViewCheckpointStatePageRef[] copyStatePageRefs() {
        final LiveViewCheckpointStatePageRef[] copy = new LiveViewCheckpointStatePageRef[statePageRefs.length];
        for (int i = 0; i < statePageRefs.length; i++) {
            copy[i] = LiveViewCheckpointPartitionMapEntry.copyRef(statePageRefs[i]);
        }
        return copy;
    }

    long getSumBits() {
        return sumBits;
    }

    static byte[] encodeScalar(int headOffset, long rowCount, long sumBits, long frameSize, long lastTimestamp) {
        final byte[] scalar = new byte[SCALAR_STATE_BYTES];
        putLong(scalar, 0, ((long) headOffset << 32) | (FORMAT_VERSION & 0xffff_ffffL));
        putLong(scalar, Long.BYTES, rowCount);
        putLong(scalar, 2 * Long.BYTES, sumBits);
        putLong(scalar, 3 * Long.BYTES, frameSize);
        putLong(scalar, 4 * Long.BYTES, lastTimestamp);
        return scalar;
    }

    static void copyRef(LiveViewCheckpointStatePageRef from, LiveViewCheckpointStatePageRef to) {
        to.of(from.getSegmentId(), from.getOffset(), from.getStoredLength(), from.getDecodedLength(),
                from.getPageKind(), from.getCodec(), from.getRowCount(), from.getFlags());
    }

    private static long getLong(byte[] bytes, int offset) {
        long value = 0;
        for (int i = 0; i < Long.BYTES; i++) {
            value |= (long) (bytes[offset + i] & 0xff) << (i * 8);
        }
        return value;
    }

    private static void putLong(byte[] bytes, int offset, long value) {
        for (int i = 0; i < Long.BYTES; i++) {
            bytes[offset + i] = (byte) (value >>> (i * 8));
        }
    }

    private static void validateTimestampRef(LiveViewCheckpointStatePageRef ref) {
        LiveViewCheckpointMetadata.validateStateRef(ref, false, "avg RANGE timestamp chunk");
        if (ref.getPageKind() != TIMESTAMP_PAGE_KIND
                || (ref.getCodec() != LiveViewCheckpointStateCodec.TIMESTAMP_RAW_64
                && ref.getCodec() != LiveViewCheckpointStateCodec.TIMESTAMP_DELTA_OF_DELTA_VARINT)) {
            throw invalid("avg RANGE timestamp page kind or codec invalid")
                    .put(" [kind=").put(ref.getPageKind()).put(", codec=").put(ref.getCodec()).put(']');
        }
        validateCommonRef(ref);
    }

    private static void validateValueRef(LiveViewCheckpointStatePageRef ref) {
        LiveViewCheckpointMetadata.validateStateRef(ref, false, "avg RANGE value chunk");
        if (ref.getPageKind() != VALUE_PAGE_KIND
                || (ref.getCodec() != LiveViewCheckpointStateCodec.DOUBLE_RAW_64
                && ref.getCodec() != LiveViewCheckpointStateCodec.DOUBLE_XOR)) {
            throw invalid("avg RANGE value page kind or codec invalid")
                    .put(" [kind=").put(ref.getPageKind()).put(", codec=").put(ref.getCodec()).put(']');
        }
        validateCommonRef(ref);
    }

    private static void validateCommonRef(LiveViewCheckpointStatePageRef ref) {
        final int rows = ref.getRowCount();
        if (rows <= 0 || rows > LiveViewCheckpointStateCodec.CHUNK_ROWS
                || ref.getDecodedLength() != rows * Long.BYTES || ref.getFlags() != FLAGS) {
            throw invalid("avg RANGE state page bounds invalid")
                    .put(" [rows=").put(rows)
                    .put(", decodedLength=").put(ref.getDecodedLength())
                    .put(", flags=").put(ref.getFlags()).put(']');
        }
    }

    private void decodeTimestamps(LiveViewCheckpointStatePageRef ref, long targetAddress) {
        openPage(ref, TIMESTAMP_PAGE_KIND);
        final int consumed = LiveViewCheckpointStateCodec.decodeTimestamps(
                dataReader.getPageAddress(), dataReader.getPageStoredLength(), ref.getCodec(), ref.getRowCount(),
                targetAddress, LiveViewCheckpointStateCodec.CHUNK_ROWS
        );
        dataReader.assertFullyConsumed(consumed, ref.getDecodedLength(), ref.getRowCount());
    }

    private void decodeValues(LiveViewCheckpointStatePageRef ref, long targetAddress) {
        openPage(ref, VALUE_PAGE_KIND);
        final int consumed = LiveViewCheckpointStateCodec.decodeDoubles(
                dataReader.getPageAddress(), dataReader.getPageStoredLength(), ref.getCodec(), ref.getRowCount(),
                targetAddress, LiveViewCheckpointStateCodec.CHUNK_ROWS
        );
        dataReader.assertFullyConsumed(consumed, ref.getDecodedLength(), ref.getRowCount());
    }

    private void ensureBound() {
        if (segmentDirectory == null) {
            throw CairoException.critical(0)
                    .put("live view checkpoint avg RANGE state reader is not bound to a data segment directory");
        }
    }

    private void ensureInitialized() {
        if (!initialized) {
            throw CairoException.critical(0).put("live view checkpoint avg RANGE state reader is not initialized");
        }
    }

    private static CairoException invalid(CharSequence reason) {
        return LiveViewCheckpointMetadata.invalid(reason);
    }

    private void openPage(LiveViewCheckpointStatePageRef ref, int pageKind) {
        final long fileLength;
        try {
            fileLength = segmentDirectory.getFileLength(ref.getSegmentId());
        } catch (CairoException e) {
            throw invalid("avg RANGE page references unknown data segment, segmentId=").put(ref.getSegmentId());
        }
        if (openSegmentId != ref.getSegmentId()) {
            dataReader.of(checkpointsDir, ref.getSegmentId(), fileLength);
            openSegmentId = ref.getSegmentId();
        }
        dataReader.openPage(
                ref,
                pageKind,
                ref.getCodec(),
                FLAGS,
                LiveViewCheckpointStateCodec.CHUNK_ROWS,
                LiveViewCheckpointStateCodec.CHUNK_ROWS * Long.BYTES
        );
    }
}
