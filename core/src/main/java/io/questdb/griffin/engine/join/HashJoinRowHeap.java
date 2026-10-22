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

import io.questdb.cairo.CairoException;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.griffin.engine.CompressedOffsets;
import io.questdb.std.MemoryTracker;
import io.questdb.std.Unsafe;
import org.jetbrains.annotations.Nullable;

import java.io.Closeable;

/**
 * Rows of a hash join build: the lookup structure holds keys, this holds the rows those keys
 * point at. {@link IntHashJoinBuild} keys it with an open-addressed INT table and
 * {@link MapHashJoinBuild} with a {@link io.questdb.cairo.map.Map}; both store the same
 * compressed offset of a chain head and share the row layout below.
 * <p>
 * A row is an eight-byte previous-match link (a byte offset plus eight, zero for a chain end)
 * followed by the build row's id, at which a probe's {@link HashJoinPayloadSource.Reader} reads the
 * payload columns where they live, as the light hash join does. A reader over a copy of those
 * columns reads the copy's row instead, which the row's ordinal, its offset over the row size,
 * names. A build that needs no payload column stores the link alone. The heap is bounded by
 * {@link CompressedOffsets#MAX_ALIGNED8_HEAP_SIZE} before allocation or encoding, so every row
 * offset round trips through {@link CompressedOffsets#compressBiased8(long)}. Duplicate iteration
 * follows the links in reverse input order, as the light join's LongChain does.
 * <p>
 * The heap is owner-built, or, in a parallel build, sized by the owner and written by the
 * partition tasks, one partition's rows each; either way it is frozen for the execution. A
 * parallel build whose frames keep every row has its frame tasks write each row's id, and lend the
 * row's link its key of at most eight bytes, which the partition task that links the row reads
 * back; see {@link #getLinkAddress(long)}.
 * {@link #freeze()} publishes it and hands out the generation that every probe asserts against,
 * so that a probe of an expired execution faults instead of reading a freed or re-filled row.
 * <p>
 * A query that only counts the joined pairs needs each key's row count and none of its rows, so
 * its owner turns the frozen links into chain lengths before any probe runs; see
 * {@link #countChains()}. A parallel build of more than one partition may count ranges of its
 * partitions apart, on the workers; see {@link #getChainCountTaskCount(HashJoinPartitions)}.
 */
final class HashJoinRowHeap implements Closeable {
    // A parallel count over frame regions takes no more tasks than leave each one runs of this many
    // rows on average; see getChainCountTaskCount().
    private static final int FRAME_REGION_MIN_RUN_ROWS = 256;
    // A parallel count over frame regions keeps at least this many tasks; see getChainCountTaskCount().
    private static final int FRAME_REGION_MIN_TASKS = 4;
    private static final int LINK_SIZE = Long.BYTES;
    private static final int ROW_ID_ROW_SIZE = LINK_SIZE + Long.BYTES;
    private static final int ROW_ID_ROW_SHIFT = Integer.numberOfTrailingZeros(ROW_ID_ROW_SIZE);
    // A task of a parallel count checks the breaker once per this many rows, as a partition task of
    // the build does; a power of two.
    private static final int ROWS_PER_CHECK = 64 * 1024;
    private final boolean hasRowId;
    private final long initialCapacity;
    private final HashJoinBuffer rows = new HashJoinBuffer(CompressedOffsets.MAX_ALIGNED8_HEAP_SIZE);
    private final int rowSize;
    private SqlExecutionCircuitBreaker circuitBreaker;
    private long generation;
    // True once countChains() replaced this execution's links with chain lengths.
    private boolean hasChainLengths;
    private long nextHandleBase;
    private long rowBytes;

    /**
     * A heap that stores row ids when the build has payload columns, and links alone otherwise.
     */
    HashJoinRowHeap(boolean hasPayload, long initialCapacity) {
        if (initialCapacity < 1 || initialCapacity > CompressedOffsets.MAX_ALIGNED8_HEAP_SIZE) {
            throw new IllegalArgumentException("invalid hash join build capacity");
        }
        this.initialCapacity = initialCapacity;
        this.hasRowId = hasPayload;
        this.rowSize = getRowSize(hasPayload);
    }

    /**
     * Bytes of one row: a link, plus the build row's id when the build has payload columns. The
     * payload's width does not matter: a row stores its id, not its columns.
     */
    static int getRowSize(boolean hasPayload) {
        return hasPayload ? ROW_ID_ROW_SIZE : LINK_SIZE;
    }

    /**
     * Appends one row and returns its byte offset, which the caller compresses into the key's
     * chain head. {@code link} is the row link of the match this row displaces: zero for a new
     * key, otherwise the previous head widened through
     * {@link CompressedOffsets#uncompressAligned8(int)}. A heap without row ids ignores the id.
     */
    long append(long rowId, long link) {
        final long offset = rowBytes;
        final long required = offset + rowSize;
        rows.ensure(required, initialCapacity);
        final long address = rows.address + offset;
        Unsafe.putLong(address, link);
        if (hasRowId) {
            Unsafe.putLong(address + LINK_SIZE, rowId);
        }
        rowBytes = required;
        return offset;
    }

    /**
     * Sizes an empty heap for exactly this many rows and counts them as appended, so that a parallel
     * build can write each row at an offset it computed through {@link #put(long, long, long)}.
     */
    void allocateRows(long rowCount) {
        assert rowBytes == 0;
        reserve(rowCount);
        rowBytes = rowCount * rowSize;
    }

    /**
     * Releases the rows and expires every probe of this execution.
     */
    @Override
    public void close() {
        rows.close();
        rowBytes = 0;
        hasChainLengths = false;
        generation++;
        circuitBreaker = null;
    }

    /**
     * Replaces every row's link with the length of the chain that the row heads: the row itself
     * plus every row that its link reached. Every build links a row to the row of its key that it
     * wrote last, which lies at a lower offset, so one pass in heap order finds each link's target
     * already counted. The head of a key's chain then holds the key's row count, which a probe reads
     * through {@link AbstractHashJoinProbe#getKeyRowCount(int)} in place of walking the chain. The
     * pass runs in place and allocates nothing, and it leaves no link behind: no probe of this
     * execution may walk a chain afterwards. The owner runs it once on the frozen heap, before any
     * probe reads it, and checks the breaker once per MiB of rows.
     */
    void countChains() {
        if (hasChainLengths) {
            throw new IllegalStateException("hash join build rows are already counted");
        }
        final long limit = rowBytes;
        for (long chunkLo = 0; chunkLo < limit; chunkLo += HashJoinBuffer.COPY_CHUNK_SIZE) {
            circuitBreaker.statefulThrowExceptionIfTrippedTimeThrottled();
            countChains(chunkLo, Math.min(limit, chunkLo + HashJoinBuffer.COPY_CHUNK_SIZE));
        }
        hasChainLengths = true;
    }

    /**
     * Counts the chains of the partitions from {@code partitionLo} to {@code partitionHi}, exclusive,
     * of a frozen parallel build: their share of {@link #countChains()}, so that tasks over disjoint
     * ranges of partitions count the heap concurrently. A row links only to a row of its own key,
     * which lies in the same partition, and each partition's build task linked the partition's rows
     * frame by frame, and in heap order within a frame; see {@link IntHashJoinBuild#buildPartition}.
     * <p>
     * When each partition fills one region of the heap, consecutive partitions fill one region, in
     * partition order, and each partition's rows lie there in the order its task linked them; the
     * pass walks that region in heap order. When the frames have regions, each frame's region holds
     * the frame's rows sorted by bucket, so the range's rows in a frame form one run; the pass walks
     * the runs frame by frame, each in heap order. Either way it reaches each partition's rows in the
     * order they were linked, so it finds every link's target counted already, by itself, and it
     * reads and writes the range's rows alone, which no other task touches.
     * <p>
     * The pass allocates nothing, and checks the calling thread's breaker at its first row and once
     * per {@value #ROWS_PER_CHECK} rows after it, as the build's partition tasks do. Once every
     * range's pass returned, the owner ends the count through {@link #endPartitionedChainCount()}; a
     * pass that throws leaves its partitions half counted, so the owner then closes the heap.
     */
    void countChains(HashJoinPartitions partitions, int partitionLo, int partitionHi, SqlExecutionCircuitBreaker circuitBreaker) {
        assert !hasChainLengths && partitionLo >= 0 && partitionLo < partitionHi && partitionHi <= partitions.getPartitionCount();
        if (partitions.hasFrameRegions()) {
            final long bucketLo = partitions.getPartitionBucketLo(partitionLo);
            final long bucketHi = partitions.getPartitionBucketLo(partitionHi);
            long counted = 0;
            for (int frame = 0, frameCount = partitions.getFrameCount(); frame < frameCount; frame++) {
                final long bucketStarts = partitions.getBucketStarts(frame);
                final int lo = Unsafe.getInt(bucketStarts + bucketLo);
                final int hi = Unsafe.getInt(bucketStarts + bucketHi);
                counted = countRun(partitions.getFrameRegionStart(frame) + lo, hi - lo, counted, circuitBreaker);
            }
        } else {
            long rowCount = 0;
            for (int partition = partitionLo; partition < partitionHi; partition++) {
                rowCount += partitions.getPartitionRowCount(partition);
            }
            countRun(partitions.getPartitionStart(partitionLo), rowCount, 0, circuitBreaker);
        }
    }

    /**
     * Ends a count that ran {@link #countChains(HashJoinPartitions, int, int, SqlExecutionCircuitBreaker)}
     * once per range of partitions, the ranges covering every partition once: the rows hold chain
     * lengths from here on, as after {@link #countChains()}. The owner calls it once every range's
     * pass returned and no task of theirs still runs, which also makes their writes visible to the
     * owner, and to the probes it publishes the heap to next.
     */
    void endPartitionedChainCount() {
        if (hasChainLengths) {
            throw new IllegalStateException("hash join build rows are already counted");
        }
        hasChainLengths = true;
    }

    /**
     * Ends mutation and returns the generation that this execution's probes assert against.
     */
    long freeze() {
        return ++generation;
    }

    long getAddress() {
        return rows.address;
    }

    /**
     * How many tasks count the chains of this frozen parallel build, each over as many consecutive
     * partitions, through {@link #countChains(HashJoinPartitions, int, int, SqlExecutionCircuitBreaker)},
     * or one when the owner's single pass, {@link #countChains()}, costs less. The result divides the
     * partition count.
     * <p>
     * When each partition fills one region of the heap, a task's rows form one run, which it walks
     * as the owner's pass walks the heap, so each partition takes a task of its own. When the frames
     * have regions, a task's rows form one run per frame. Each run starts a new stream of reads, which
     * costs as much as a few hundred rows of the walk, and neighbouring tasks share a cache line where
     * their runs meet; with frames of a few hundred rows or fewer, a task per partition would spend
     * more time starting runs than counting rows. Fewer tasks over more partitions each walk longer
     * runs, so the tasks halve until their runs average at least {@value #FRAME_REGION_MIN_RUN_ROWS}
     * rows. Fewer than {@value #FRAME_REGION_MIN_TASKS} such tasks cannot win back the cost of their
     * runs, so the owner's pass counts instead.
     */
    int getChainCountTaskCount(HashJoinPartitions partitions) {
        int taskCount = partitions.getPartitionCount();
        if (partitions.hasFrameRegions()) {
            final long maxTaskCount = getRowCount() / ((long) partitions.getFrameCount() * FRAME_REGION_MIN_RUN_ROWS);
            while (taskCount > maxTaskCount && taskCount >= FRAME_REGION_MIN_TASKS) {
                taskCount >>= 1;
            }
            if (taskCount < FRAME_REGION_MIN_TASKS) {
                return 1;
            }
        }
        return taskCount;
    }

    long getGeneration() {
        return generation;
    }

    /**
     * Where the row at this byte offset keeps its link, eight bytes. Until a parallel build links a
     * row that its frame task wrote, they hold the row's key instead.
     */
    long getLinkAddress(long offset) {
        return rows.address + offset;
    }

    long getRowCount() {
        return rowBytes / rowSize;
    }

    int getRowSize() {
        return rowSize;
    }

    /**
     * Allocated native bytes, including unused capacity.
     */
    long getSizeInBytes() {
        return rows.capacity;
    }

    /**
     * True once {@link #countChains()} replaced this execution's links with chain lengths.
     */
    boolean hasChainLengths() {
        return hasChainLengths;
    }

    /**
     * True when rows carry the id of their build row, which a payload reader positions at.
     */
    boolean hasRowId() {
        return hasRowId;
    }

    /**
     * Takes the handle base of one execution, so that handles never repeat across executions.
     */
    long nextHandleBase() {
        if (nextHandleBase > Long.MAX_VALUE - rowBytes - 1) {
            throw CairoException.nonCritical().put("hash join handle capacity overflow");
        }
        final long handleBase = nextHandleBase;
        nextHandleBase += rowBytes + 1;
        return handleBase;
    }

    /**
     * Binds the execution that charges and cancels this heap's allocations.
     */
    void of(@Nullable MemoryTracker memoryTracker, SqlExecutionCircuitBreaker circuitBreaker) {
        this.circuitBreaker = circuitBreaker;
        rows.of(memoryTracker, circuitBreaker);
    }

    /**
     * Writes the row at this byte offset of a heap that {@link #allocateRows(long)} sized: its link,
     * as {@link #append(long, long)} takes it, and its id, which a heap without row ids ignores.
     */
    void put(long offset, long link, long rowId) {
        final long address = rows.address + offset;
        Unsafe.putLong(address, link);
        if (hasRowId) {
            Unsafe.putLong(address + LINK_SIZE, rowId);
        }
    }

    /**
     * Writes the id of the row at this byte offset of a heap that {@link #allocateRows(long)} sized;
     * a heap without row ids ignores it.
     */
    void putRowId(long offset, long rowId) {
        if (hasRowId) {
            Unsafe.putLong(rows.address + offset + LINK_SIZE, rowId);
        }
    }

    /**
     * Presizes the heap for a known row count, so that appends of that many rows do not grow it.
     */
    void reserve(long rowCount) {
        circuitBreaker.statefulThrowExceptionIfTrippedNoThrottle();
        if (rowCount > (CompressedOffsets.MAX_ALIGNED8_HEAP_SIZE - rowBytes) / rowSize) {
            throw CairoException.nonCritical().put("hash join build buffer overflow");
        }
        rows.ensure(rowBytes + rowCount * rowSize, initialCapacity);
    }

    /**
     * How many rows the heap appended before the row at this byte offset. Only a heap with row
     * ids counts rows this way, which every heap a payload reader positions over is.
     */
    static long getOrdinal(long offset) {
        return offset >>> ROW_ID_ROW_SHIFT;
    }

    /**
     * The build row id of the heap's ordinal-th row, the rows starting at {@code rowsAddress}; only a
     * heap with row ids stores one.
     */
    static long getRowIdAt(long rowsAddress, long ordinal) {
        return getRowId(rowsAddress + (ordinal << ROW_ID_ROW_SHIFT));
    }

    /**
     * The build row id of the row at this address; only a heap with row ids stores one.
     */
    static long getRowId(long rowAddress) {
        return Unsafe.getLong(getRowIdAddress(rowAddress));
    }

    /**
     * Where the row at this address stores its build row id; only a heap with row ids stores one.
     */
    static long getRowIdAddress(long rowAddress) {
        return rowAddress + LINK_SIZE;
    }

    // Replaces the link of every row between these byte offsets, in heap order, with the length of
    // its chain. The caller has counted every row that a link here reaches, which lies lower.
    private void countChains(long lo, long hi) {
        final long base = rows.address;
        final int rowSize = this.rowSize;
        for (long offset = lo; offset < hi; offset += rowSize) {
            final long link = Unsafe.getLong(base + offset);
            assert link == 0 || link - LINK_SIZE < offset;
            Unsafe.putLong(base + offset, link == 0 ? 1 : Unsafe.getLong(base + link - LINK_SIZE) + 1);
        }
    }

    // Counts the chains of a run of this many rows from this ordinal on, in heap order, checking the
    // breaker wherever the rows that the task counted so far reach a multiple of ROWS_PER_CHECK.
    // Returns the task's rows counted after the run.
    private long countRun(long ordinal, long rowCount, long counted, SqlExecutionCircuitBreaker circuitBreaker) {
        while (rowCount > 0) {
            final long sinceCheck = counted & (ROWS_PER_CHECK - 1);
            if (sinceCheck == 0) {
                circuitBreaker.statefulThrowExceptionIfTrippedTimeThrottled();
            }
            final long runRows = Math.min(rowCount, ROWS_PER_CHECK - sinceCheck);
            countChains(ordinal * rowSize, (ordinal + runRows) * rowSize);
            ordinal += runRows;
            rowCount -= runRows;
            counted += runRows;
        }
        return counted;
    }
}
