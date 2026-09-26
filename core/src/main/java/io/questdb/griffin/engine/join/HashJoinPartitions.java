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

package io.questdb.griffin.engine.join;

import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.std.MemoryTag;
import io.questdb.std.MemoryTracker;
import io.questdb.std.Numbers;
import io.questdb.std.QuietCloseable;
import io.questdb.std.Unsafe;
import org.jetbrains.annotations.Nullable;

/**
 * The bookkeeping of a parallel hash join build, which {@link IntHashJoinBuild} and
 * {@link MapHashJoinBuild} share; see {@link IntHashJoinBuild} for the scheme. Each frame task
 * sorts the rows its frame keeps into a chunk of its own, grouped by bucket, the top bits of the
 * key's hash, and leaves the row start of every bucket in the frame's row of the bucket table.
 * The owner then groups the buckets into partitions by the exact count of kept rows, which gives
 * each partition one contiguous region of the heap, and each partition task records where each
 * frame's rows of its partition start in it.
 * <p>
 * What a chunk holds is the build's business: this class allocates, tracks and frees it. Each
 * build keeps its own key tables.
 */
final class HashJoinPartitions implements QuietCloseable {
    static final int MAX_PARTITIONS = 1 << 8;
    // Each frame's chunk: its address and its size.
    private static final int CHUNK_ENTRY_SIZE = 2 * Long.BYTES;
    private static final long MAX_BUFFER_SIZE = 1L << 48;
    private static final int MAX_PARTITION_BITS = Integer.numberOfTrailingZeros(MAX_PARTITIONS);
    // Each frame's bucket starts within its chunk, in rows, plus the chunk's row count: bucket count + 1 ints.
    private final HashJoinBuffer bucketStarts = new HashJoinBuffer(MAX_BUFFER_SIZE);
    private final HashJoinBuffer chunks = new HashJoinBuffer(MAX_BUFFER_SIZE);
    private final long[] partitionKeyHints = new long[MAX_PARTITIONS];
    private final long[] partitionStarts = new long[MAX_PARTITIONS];
    // Heap ordinal of the first row that frame f keeps in partition p, at f * partitions + p.
    private final HashJoinBuffer segmentStarts = new HashJoinBuffer(MAX_BUFFER_SIZE);
    private int bucketBits;
    // The frames of a parallel build; zero for a serial one.
    private int frameCount;
    @Nullable
    private MemoryTracker memoryTracker;
    private int partitionBits;
    // The rows the frames kept, once the partitions are planned.
    private long rowCount;

    /**
     * The bucket, or with fewer bits the partition, of a key's 64-bit hash: its top bits, for a
     * shift of 32 minus the bits. A key table takes its slot from the low bits, so the keys of one
     * partition spread over its table.
     */
    static int bucketOf(long hash, int shift) {
        return (int) ((hash >>> 32) >>> shift);
    }

    /** Allocates the chunk of this frame and returns its address; {@link #freeChunks()} frees it. */
    long allocateChunk(int frameIndex, long size) {
        final long chunk = Unsafe.malloc(size, MemoryTag.NATIVE_JOIN_MAP, memoryTracker);
        final long chunkEntry = chunks.address + (long) frameIndex * CHUNK_ENTRY_SIZE;
        Unsafe.putLong(chunkEntry, chunk);
        Unsafe.putLong(chunkEntry + Long.BYTES, size);
        return chunk;
    }

    /**
     * Starts a parallel build over this many frames. The frames' row count before any filter and the
     * rows the caller wants per partition set how many buckets a frame sorts its rows into; the bucket
     * table costs four bytes per bucket per frame, so the frames' average row count bounds the bucket
     * count too.
     */
    void begin(int frameCount, long rowCountBound, long rowsPerPartition) {
        if (frameCount < 1 || rowsPerPartition < 1 || this.frameCount != 0) {
            throw new IllegalStateException("hash join build cannot start partitioning");
        }
        this.frameCount = frameCount;
        bucketBits = getPartitionBits(Math.min(Numbers.ceilDiv(rowCountBound, rowsPerPartition), rowCountBound / frameCount));
        chunks.allocate((long) frameCount * CHUNK_ENTRY_SIZE, true);
        bucketStarts.allocate((long) frameCount * getBucketStride(), true);
    }

    @Override
    public void close() {
        freeChunks();
        chunks.close();
        bucketStarts.close();
        segmentStarts.close();
        frameCount = bucketBits = partitionBits = 0;
        rowCount = 0;
        memoryTracker = null;
    }

    void freeChunks() {
        for (long entry = chunks.address, limit = entry + chunks.capacity; entry < limit; entry += CHUNK_ENTRY_SIZE) {
            final long chunk = Unsafe.getLong(entry);
            if (chunk != 0) {
                Unsafe.free(chunk, Unsafe.getLong(entry + Long.BYTES), MemoryTag.NATIVE_JOIN_MAP, memoryTracker);
                Unsafe.putLong(entry, 0);
            }
        }
    }

    int getBucketCount() {
        return 1 << bucketBits;
    }

    /** The shift that {@link #bucketOf} takes for a bucket. */
    int getBucketShift() {
        return 32 - bucketBits;
    }

    /** Bytes of one frame's row of the bucket table. */
    long getBucketStride() {
        return (long) Integer.BYTES * ((1 << bucketBits) + 1);
    }

    /**
     * Address of this frame's row of the bucket table: the row start of each bucket within the
     * frame's chunk, then the frame's kept row count. A frame task fills its own row.
     */
    long getBucketStarts(int frameIndex) {
        return bucketStarts.address + frameIndex * getBucketStride();
    }

    long getChunk(int frameIndex) {
        return Unsafe.getLong(chunks.address + (long) frameIndex * CHUNK_ENTRY_SIZE);
    }

    int getFrameCount() {
        return frameCount;
    }

    /** Byte offset, within a frame's row of the bucket table, of the first bucket of this partition. */
    long getPartitionBucketLo(int partition) {
        return (long) Integer.BYTES * (partition << (bucketBits - partitionBits));
    }

    int getPartitionCount() {
        return 1 << partitionBits;
    }

    /** The shift that {@link #bucketOf} takes for a partition, once the partitions are planned. */
    int getPartitionShift() {
        return 32 - partitionBits;
    }

    long getPartitionKeyHint(int partition) {
        return partitionKeyHints[partition];
    }

    /** Rows of this partition, once the partitions are planned. */
    long getPartitionRowCount(int partition) {
        assert frameCount > 0 && partition >= 0 && partition < 1 << partitionBits;
        final long end = partition + 1 < 1 << partitionBits ? partitionStarts[partition + 1] : rowCount;
        return end - partitionStarts[partition];
    }

    /** Heap ordinal of this partition's first row, once the partitions are planned. */
    long getPartitionStart(int partition) {
        return partitionStarts[partition];
    }

    /** Rows the frames kept, once every frame is partitioned. */
    long getPartitionedRowCount() {
        final long stride = getBucketStride();
        final long total = (long) Integer.BYTES * (1 << bucketBits);
        long keptCount = 0;
        for (int frame = 0; frame < frameCount; frame++) {
            keptCount += Unsafe.getInt(bucketStarts.address + frame * stride + total);
        }
        return keptCount;
    }

    /** Rows that this frame keeps in this partition. */
    long getSegmentRowCount(int frameIndex, int partition) {
        assert frameIndex >= 0 && frameIndex < frameCount && partition >= 0 && partition < 1 << partitionBits;
        final long counts = getBucketStarts(frameIndex);
        return Unsafe.getInt(counts + getPartitionBucketLo(partition + 1)) - Unsafe.getInt(counts + getPartitionBucketLo(partition));
    }

    /** Heap ordinal of the first row that this frame keeps in this partition, once the partition is built. */
    long getSegmentStart(int frameIndex, int partition) {
        assert frameIndex >= 0 && frameIndex < frameCount && partition >= 0 && partition < 1 << partitionBits;
        return Unsafe.getLong(segmentStarts.address + ((long) frameIndex * (1 << partitionBits) + partition) * Long.BYTES);
    }

    /** Native bytes of the tables, not counting the chunks they point at. */
    long getSizeInBytes() {
        return bucketStarts.capacity + chunks.capacity + segmentStarts.capacity;
    }

    /** True between {@link #begin} and {@link #close()}. */
    boolean isPartitioning() {
        return frameCount != 0;
    }

    /** Binds the execution that charges and cancels this build's allocations. */
    void of(@Nullable MemoryTracker memoryTracker, SqlExecutionCircuitBreaker circuitBreaker) {
        this.memoryTracker = memoryTracker;
        chunks.of(memoryTracker, circuitBreaker);
        bucketStarts.of(memoryTracker, circuitBreaker);
        segmentStarts.of(memoryTracker, circuitBreaker);
    }

    /**
     * Groups the buckets into partitions of about {@code rowsPerPartition} rows each, a power of two
     * of them, from the rows the frames kept, and gives each partition its region of the heap, in
     * partition order. A positive key hint bounds the distinct keys; each partition's key table then
     * takes its share, see {@link #getPartitionKeyHint(int)}; -1 lets the tables grow. Returns the
     * partition count.
     */
    int plan(long rowsPerPartition, long keyCountHint) {
        if (frameCount == 0 || rowsPerPartition < 1) {
            throw new IllegalStateException("hash join build is not partitioned");
        }
        rowCount = getPartitionedRowCount();
        partitionBits = Math.min(bucketBits, getPartitionBits(Numbers.ceilDiv(rowCount, rowsPerPartition)));
        final int partitionCount = 1 << partitionBits;
        final long bucketStride = getBucketStride();
        long start = 0;
        for (int p = 0; p < partitionCount; p++) {
            final long bucketLo = getPartitionBucketLo(p);
            final long bucketHi = getPartitionBucketLo(p + 1);
            long rows = 0;
            for (int frame = 0; frame < frameCount; frame++) {
                final long counts = bucketStarts.address + frame * bucketStride;
                rows += Unsafe.getInt(counts + bucketHi) - Unsafe.getInt(counts + bucketLo);
            }
            partitionStarts[p] = start;
            start += rows;
            // A key bound is shared by every partition; the hash spreads the keys evenly over
            // them, so each takes twice its share and grows past it if it must.
            partitionKeyHints[p] = keyCountHint < 1 ? -1
                    : partitionCount == 1 ? keyCountHint
                    : Math.min(rows, Numbers.ceilDiv(2 * keyCountHint, partitionCount));
        }
        assert start == rowCount;
        segmentStarts.allocate((long) frameCount * partitionCount * Long.BYTES, false);
        return partitionCount;
    }

    /** Records where this frame's rows of this partition start in the heap; the partition's task does. */
    void setSegmentStart(int frameIndex, int partition, long ordinal) {
        Unsafe.putLong(segmentStarts.address + ((long) frameIndex * (1 << partitionBits) + partition) * Long.BYTES, ordinal);
    }

    // Bits of the smallest power of two at least this many partitions, at most MAX_PARTITIONS.
    private static int getPartitionBits(long partitions) {
        return partitions <= 1 ? 0 : Math.min(MAX_PARTITION_BITS, 64 - Long.numberOfLeadingZeros(partitions - 1));
    }
}
