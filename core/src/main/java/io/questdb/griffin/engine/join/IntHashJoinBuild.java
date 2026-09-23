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
import io.questdb.cairo.sql.PageFrameMemoryRecord;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.griffin.engine.CompressedOffsets;
import io.questdb.std.DirectLongList;
import io.questdb.std.Hash;
import io.questdb.std.MemoryTag;
import io.questdb.std.MemoryTracker;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.Rows;
import io.questdb.std.Unsafe;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

import java.io.Closeable;

import static io.questdb.griffin.engine.join.IntHashJoinKeyTable.SLOT_SIZE;

/**
 * INT lookup for fused hash join aggregation, built by the owner, or, for a large build, by
 * the workers one hash partition at a time (see the last paragraph). No native memory is
 * allocated by construction. {@link #open} binds the execution's tracker before
 * allocation; any append/build failure closes the entire partial build.
 * <p>
 * Linear probing at a maximum load of 1/2. An eight-byte slot holds an INT key
 * and an unsigned compressed payload offset. References encode an eight-byte
 * aligned row offset divided by eight plus one; zero marks an unused slot.
 * Zero, negative and INT_NULL keys need no special representation and null keys
 * match each other. Widening a slot head only scales its unsigned value; duplicate
 * advances need no offset decoding.
 * <p>
 * The rows themselves, their links and the ids of their build rows live in
 * {@link HashJoinRowHeap}, which {@link MapHashJoinBuild} shares; see there for the
 * row layout and the heap bound. Probes read payload columns through the build's
 * {@link HashJoinPayloadSource}. A SYMBOL join key is stored as the build's own symbol key;
 * the probe translates its key into that domain through {@link SymbolKeyTranslator}.
 * <p>
 * Hash tables and rows use tracked native buffers. Growth accounts for both old and
 * new allocations and is cancellable. Frozen views borrow these buffers until close;
 * see {@link FrozenHashJoinBuild}.
 * <p>
 * The key table doubles from its initial size, rehashing every key it holds, unless the
 * caller bounds the distinct keys up front; a key count hint buys the whole table before
 * the first row. Only the caller can bound them: a row count bounds them too, but a table
 * sized by the rows of a key that repeats holds mostly empty slots, costs memory and scatters
 * every lookup over them.
 * <p>
 * A parallel build splits the rows by the top bits of their key's hash into up to
 * {@link #MAX_PARTITIONS} partitions, each with a key table of its own, so that each worker
 * fills one table alone and a table small enough fits the caches. It runs in two rounds over
 * the build input, which the caller dispatches. First, one task per page frame sorts the rows
 * the frame keeps into a chunk of its own, grouped by bucket: the key and the row within the
 * frame of each. Buckets are the partitions at their finest, chosen from the frames' row count
 * before any filter; the owner then groups them into partitions by the exact count of kept
 * rows, and sizes the row heap for exactly those rows. Second, one task per partition copies
 * the partition's bucket runs from every chunk, frame by frame, into the partition's
 * contiguous region of the heap, and inserts each row into the partition's table. A key's
 * rows land in one partition in input order, so its chain is the one a serial build makes.
 * The owner frees the chunks once the partitions are built. A probe of such a build takes
 * the partition from the key's hash, then the partition's table; see {@link #freezePartitioned}.
 */
public final class IntHashJoinBuild implements Closeable {
    public static final int MAX_PARTITIONS = 1 << 8;
    // Each frame's chunk: its address and its size.
    private static final int CHUNK_ENTRY_SIZE = 2 * Long.BYTES;
    // Each partition's table address and slot mask, which a partitioned probe reads per lookup.
    private static final int DIRECTORY_ENTRY_SIZE = 2 * Long.BYTES;
    private static final long MAX_BUFFER_SIZE = 1L << 48;
    private static final int MAX_PARTITION_BITS = Integer.numberOfTrailingZeros(MAX_PARTITIONS);
    // A partition task checks the breaker once per this many rows; a power of two.
    private static final int ROWS_PER_CHECK = 64 * 1024;
    // Each frame's bucket starts within its chunk, plus the chunk's row count: bucket count + 1 ints.
    private final HashJoinBuffer bucketStarts = new HashJoinBuffer(MAX_BUFFER_SIZE);
    private final HashJoinBuffer chunks = new HashJoinBuffer(MAX_BUFFER_SIZE);
    private final HashJoinBuffer directory = new HashJoinBuffer(MAX_BUFFER_SIZE);
    private final HashJoinRowHeap heap;
    private final int initialSlots;
    // The serial build's table, and a parallel build's first partition.
    private final IntHashJoinKeyTable keys = new IntHashJoinKeyTable();
    private final long[] partitionKeyHints = new long[MAX_PARTITIONS];
    private final long[] partitionStarts = new long[MAX_PARTITIONS];
    private final Frozen reusableFrozen;
    private final PartitionedFrozen reusablePartitionedFrozen;
    // Heap ordinal of the first row that frame f keeps in partition p, at f * partitions + p.
    private final HashJoinBuffer segmentStarts = new HashJoinBuffer(MAX_BUFFER_SIZE);
    // One key table per partition of a parallel build; the first is the serial build's.
    private final ObjList<IntHashJoinKeyTable> tables = new ObjList<>();
    private int bucketBits;
    private SqlExecutionCircuitBreaker circuitBreaker;
    // The frames of a parallel build; zero for a serial one.
    private int frameCount;
    private AbstractFrozen frozen;
    @Nullable
    private MemoryTracker memoryTracker;
    private boolean open;
    private int partitionBits;

    /** A build with payload columns stores row ids for probes to read them through; see the class docs. */
    @TestOnly
    public IntHashJoinBuild(boolean hasPayload, int initialSlots, long initialRowCapacity) {
        this(hasPayload, initialSlots, initialRowCapacity, false);
    }

    /**
     * Reusable mode is for a factory that drains all consumers before reopening.
     * Its snapshot is a flyweight; retained probes must explicitly reopen for each
     * execution. The ordinary constructor keeps execution-specific snapshots.
     */
    public IntHashJoinBuild(boolean hasPayload, int initialSlots, long initialRowCapacity, boolean reusable) {
        if (initialSlots < 2 || initialSlots > IntHashJoinKeyTable.MAX_SLOTS || Integer.bitCount(initialSlots) != 1) {
            throw new IllegalArgumentException("invalid hash join build capacity");
        }
        this.initialSlots = initialSlots;
        heap = new HashJoinRowHeap(hasPayload, initialRowCapacity);
        reusableFrozen = reusable ? new Frozen() : null;
        reusablePartitionedFrozen = reusable ? new PartitionedFrozen() : null;
        tables.add(keys);
    }

    /** Appends one row. On failure all execution allocations are released. */
    public void append(int key, long rowId) {
        requireBuilding();
        try {
            appendRow(key, rowId);
        } catch (Throwable th) {
            close();
            throw th;
        }
    }

    /**
     * Appends every row of the page frame the record is bound to, keyed by the INT key column.
     * The caller checks cancellation at frame boundaries. On failure all execution allocations
     * are released.
     */
    public void appendFrame(PageFrameMemoryRecord record, int keyColumn, long rowCount) {
        requireBuilding();
        try {
            for (long r = 0; r < rowCount; r++) {
                record.setRowIndex(r);
                appendRow(record.getInt(keyColumn), record.getRowId());
            }
        } catch (Throwable th) {
            close();
            throw th;
        }
    }

    /** The filtered twin of {@link #appendFrame(PageFrameMemoryRecord, int, long)}: appends the listed rows only. */
    public void appendFrame(PageFrameMemoryRecord record, int keyColumn, DirectLongList rows) {
        requireBuilding();
        try {
            for (long p = 0, n = rows.size(); p < n; p++) {
                record.setRowIndex(rows.get(p));
                appendRow(record.getInt(keyColumn), record.getRowId());
            }
        } catch (Throwable th) {
            close();
            throw th;
        }
    }

    /**
     * Starts a parallel build over the build input's page frames; see the class docs. Call it after
     * {@link #open} instead of appending. Every frame then goes once to a {@code partitionFrame()}
     * method, on any thread, followed by {@link #planPartitions}, {@link #buildPartition} once per
     * partition, on any thread, and {@link #freezePartitioned}. The frames' row count before any
     * filter and the rows the caller wants per partition set how many buckets a frame sorts its
     * rows into; the bucket table costs four bytes per bucket per frame, so the frames' average row
     * count bounds the bucket count too. On failure all execution allocations are released.
     */
    public void beginPartitioning(int frameCount, long rowCountBound, long rowsPerPartition) {
        requireBuilding();
        try {
            if (frameCount < 1 || rowsPerPartition < 1 || heap.getRowCount() != 0 || keys.keyCount != 0 || this.frameCount != 0) {
                throw new IllegalStateException("hash join build cannot start partitioning");
            }
            // Each partition's table opens on the thread that builds it, the first partition's too.
            keys.close();
            this.frameCount = frameCount;
            bucketBits = getPartitionBits(Math.min(Numbers.ceilDiv(rowCountBound, rowsPerPartition), rowCountBound / frameCount));
            chunks.allocate((long) frameCount * CHUNK_ENTRY_SIZE, true);
            bucketStarts.allocate((long) frameCount * getBucketStride(), true);
        } catch (Throwable th) {
            close();
            throw th;
        }
    }

    /**
     * Consumes a borrowed cursor once, keeping the id of each row, and freezes. Probes read payload
     * columns through the source, so the caller keeps whatever the source reads open until close.
     * The hints are those of {@link #reserve(long, long)}. A SYMBOL key column keeps its own key,
     * which is the domain the probe translates into.
     */
    public FrozenHashJoinBuild.IntKeyed build(
            RecordCursor cursor,
            int keyColumn,
            long rowCountHint,
            long keyCountHint,
            @Nullable HashJoinPayloadSource payloads
    ) {
        reserve(rowCountHint, keyCountHint);
        try {
            final Record record = cursor.getRecord();
            final boolean hasRowId = heap.hasRowId();
            // The source cursor checks the breaker at its frame boundaries.
            while (cursor.hasNext()) {
                appendRow(record.getInt(keyColumn), hasRowId ? record.getRowId() : 0);
            }
        } catch (Throwable th) {
            close();
            throw th;
        }
        return freeze(payloads);
    }

    /**
     * Builds one partition of a parallel build: copies the partition's rows out of every frame's
     * chunk into the partition's region of the heap and inserts them into the partition's table,
     * which opens here with the calling thread's circuit breaker. Runs on any thread, once per
     * partition, concurrently with the other partitions. On failure the caller closes the build
     * once every partition task has stopped.
     */
    public void buildPartition(int partition, SqlExecutionCircuitBreaker circuitBreaker) {
        assert open && frozen == null && frameCount > 0 && partition >= 0 && partition < 1 << partitionBits;
        final IntHashJoinKeyTable table = tables.getQuick(partition);
        table.open(memoryTracker, circuitBreaker, initialSlots);
        final long keyCountHint = partitionKeyHints[partition];
        if (keyCountHint > 0) {
            table.reserve(keyCountHint);
        }
        final int bucketShift = bucketBits - partitionBits;
        final long bucketLo = (long) Integer.BYTES * (partition << bucketShift);
        final long bucketHi = (long) Integer.BYTES * ((partition + 1) << bucketShift);
        final long bucketStride = getBucketStride();
        final int partitionCount = 1 << partitionBits;
        final int entrySize = getChunkEntrySize();
        final boolean hasRowId = heap.hasRowId();
        final int rowSize = heap.getRowSize();
        long ordinal = partitionStarts[partition];
        for (int frame = 0; frame < frameCount; frame++) {
            Unsafe.putLong(segmentStarts.address + ((long) frame * partitionCount + partition) * Long.BYTES, ordinal);
            final long counts = bucketStarts.address + frame * bucketStride;
            final int lo = Unsafe.getInt(counts + bucketLo);
            final int hi = Unsafe.getInt(counts + bucketHi);
            final long chunk = Unsafe.getLong(chunks.address + (long) frame * CHUNK_ENTRY_SIZE);
            for (int e = lo; e < hi; e++, ordinal++) {
                if ((ordinal & (ROWS_PER_CHECK - 1)) == 0) {
                    circuitBreaker.statefulThrowExceptionIfTrippedTimeThrottled();
                }
                final long entry = chunk + (long) e * entrySize;
                final int key = Unsafe.getInt(entry);
                final long slot = table.claim(key);
                final int previous = Unsafe.getInt(slot + 4);
                final long offset = ordinal * rowSize;
                heap.put(offset, toRowLink(previous), hasRowId ? Rows.toRowID(frame, Unsafe.getInt(entry + Integer.BYTES)) : 0);
                Unsafe.putInt(slot, key);
                Unsafe.putInt(slot + 4, CompressedOffsets.compressBiased8(offset));
                if (previous == 0) {
                    table.keyCount++;
                }
            }
        }
    }

    /** Only call after every probe is drained and aggregate output is finished. */
    @Override
    public void close() {
        if (frozen != null) {
            // Do not retain the borrowed source past its execution.
            frozen.payloads = null;
            frozen = null;
        }
        open = false;
        freeChunks();
        chunks.close();
        bucketStarts.close();
        segmentStarts.close();
        directory.close();
        for (int i = 0, n = tables.size(); i < n; i++) {
            tables.getQuick(i).close();
        }
        heap.close();
        frameCount = bucketBits = partitionBits = 0;
        circuitBreaker = null;
        memoryTracker = null;
    }

    /** Ends mutation of a build without payload columns. */
    public FrozenHashJoinBuild.IntKeyed freeze() {
        return freeze(null);
    }

    /**
     * Ends mutation. Probes read payload columns through the borrowed source until close.
     * Publication to probe workers is the caller's responsibility.
     */
    public FrozenHashJoinBuild.IntKeyed freeze(@Nullable HashJoinPayloadSource payloads) {
        requireBuilding();
        try {
            if (frameCount != 0) {
                throw new IllegalStateException("partitioned hash join build freezes through freezePartitioned()");
            }
            return freezeSerial(payloads);
        } catch (Throwable th) {
            close();
            throw th;
        }
    }

    /**
     * Ends a parallel build once every partition is built, and frees the frames' chunks. A build
     * of one partition freezes as a serial build does, since its one table is the serial build's;
     * probes of any other build take their partition from the key's hash first. Probes read
     * payload columns through the borrowed source until close. On failure all execution
     * allocations are released.
     */
    public FrozenHashJoinBuild.IntKeyed freezePartitioned(@Nullable HashJoinPayloadSource payloads) {
        requireBuilding();
        try {
            if (frameCount == 0) {
                throw new IllegalStateException("hash join build is not partitioned");
            }
            // The rows hold everything the chunks held.
            freeChunks();
            final int partitionCount = 1 << partitionBits;
            if (partitionCount == 1) {
                return freezeSerial(payloads);
            }
            circuitBreaker.statefulThrowExceptionIfTrippedTimeThrottled();
            checkPayloadSource(payloads);
            directory.allocate((long) partitionCount * DIRECTORY_ENTRY_SIZE, false);
            long keyCount = 0;
            for (int p = 0; p < partitionCount; p++) {
                final IntHashJoinKeyTable table = tables.getQuick(p);
                final long entry = directory.address + (long) p * DIRECTORY_ENTRY_SIZE;
                Unsafe.putLong(entry, table.slots.address);
                Unsafe.putLong(entry + Long.BYTES, table.slotCount - 1);
                keyCount += table.keyCount;
            }
            final PartitionedFrozen partitioned = reusablePartitionedFrozen != null ? reusablePartitionedFrozen : new PartitionedFrozen();
            partitioned.of(payloads, keyCount);
            frozen = partitioned;
            return partitioned;
        } catch (Throwable th) {
            close();
            throw th;
        }
    }

    /**
     * Partitions of the last parallel build, or one for a serial build. The heap holds each
     * partition's rows in one region, frame after frame; see {@link #getSegmentStart(int, int)}.
     */
    public int getPartitionCount() {
        return 1 << partitionBits;
    }

    /** Rows the frames of a parallel build kept, once every frame is partitioned. */
    public long getPartitionedRowCount() {
        final long stride = getBucketStride();
        final long total = (long) Integer.BYTES * (1 << bucketBits);
        long rowCount = 0;
        for (int frame = 0; frame < frameCount; frame++) {
            rowCount += Unsafe.getInt(bucketStarts.address + frame * stride + total);
        }
        return rowCount;
    }

    /** Rows that this frame of a parallel build keeps in this partition. */
    public long getSegmentRowCount(int frameIndex, int partition) {
        assert frameIndex >= 0 && frameIndex < frameCount && partition >= 0 && partition < 1 << partitionBits;
        final int bucketShift = bucketBits - partitionBits;
        final long counts = bucketStarts.address + frameIndex * getBucketStride();
        return Unsafe.getInt(counts + (long) Integer.BYTES * ((partition + 1) << bucketShift))
                - Unsafe.getInt(counts + (long) Integer.BYTES * (partition << bucketShift));
    }

    /**
     * Heap ordinal of the first row that this frame of a parallel build keeps in this partition.
     * The frame's rows of the partition follow it in the frame's order; see
     * {@link #getSegmentRowCount(int, int)}. Valid from the partition's build until close.
     */
    public long getSegmentStart(int frameIndex, int partition) {
        assert frameIndex >= 0 && frameIndex < frameCount && partition >= 0 && partition < 1 << partitionBits;
        return Unsafe.getLong(segmentStarts.address + ((long) frameIndex * (1 << partitionBits) + partition) * Long.BYTES);
    }

    public long getSizeInBytes() {
        long size = heap.getSizeInBytes() + bucketStarts.capacity + chunks.capacity + directory.capacity + segmentStarts.capacity;
        for (int i = 0, n = tables.size(); i < n; i++) {
            size += tables.getQuick(i).getSizeInBytes();
        }
        return size;
    }

    /** Reopens a closed skeleton for a fresh execution. */
    public void open(@Nullable MemoryTracker memoryTracker, SqlExecutionCircuitBreaker circuitBreaker) {
        if (open) {
            throw new IllegalStateException("hash join build is already open");
        }
        this.circuitBreaker = circuitBreaker;
        this.memoryTracker = memoryTracker;
        heap.of(memoryTracker, circuitBreaker);
        chunks.of(memoryTracker, circuitBreaker);
        bucketStarts.of(memoryTracker, circuitBreaker);
        segmentStarts.of(memoryTracker, circuitBreaker);
        directory.of(memoryTracker, circuitBreaker);
        try {
            keys.open(memoryTracker, circuitBreaker, initialSlots);
            open = true;
        } catch (Throwable th) {
            close();
            throw th;
        }
    }

    /**
     * Sorts the rows of one page frame of a parallel build into the frame's chunk, grouped by
     * bucket: every row of the frame, keyed by the INT key column. Runs on any thread, once per
     * frame, concurrently with the other frames. On failure the caller closes the build once
     * every frame task has stopped.
     */
    public void partitionFrame(int frameIndex, PageFrameMemoryRecord record, int keyColumn, long rowCount) {
        partitionFrame(frameIndex, record, keyColumn, null, rowCount);
    }

    /** The filtered twin of {@link #partitionFrame(int, PageFrameMemoryRecord, int, long)}: sorts the listed rows only. */
    public void partitionFrame(int frameIndex, PageFrameMemoryRecord record, int keyColumn, DirectLongList rows) {
        partitionFrame(frameIndex, record, keyColumn, rows, rows.size());
    }

    /**
     * Sizes the row heap for exactly the rows the frames kept and groups the buckets into
     * partitions of about {@code rowsPerPartition} rows each, a power of two of them. A positive
     * key hint bounds the distinct keys and presizes each partition's table by its share, as
     * {@link #reserve(long, long)} presizes the serial one; -1 lets the tables grow. Returns the
     * partition count. On failure all execution allocations are released.
     */
    public int planPartitions(long rowsPerPartition, long keyCountHint) {
        requireBuilding();
        try {
            if (frameCount == 0 || rowsPerPartition < 1) {
                throw new IllegalStateException("hash join build is not partitioned");
            }
            final long rowCount = getPartitionedRowCount();
            partitionBits = Math.min(bucketBits, getPartitionBits(Numbers.ceilDiv(rowCount, rowsPerPartition)));
            final int partitionCount = 1 << partitionBits;
            heap.allocateRows(rowCount);
            final int bucketShift = bucketBits - partitionBits;
            final long bucketStride = getBucketStride();
            long start = 0;
            for (int p = 0; p < partitionCount; p++) {
                final long bucketLo = (long) Integer.BYTES * (p << bucketShift);
                final long bucketHi = (long) Integer.BYTES * ((p + 1) << bucketShift);
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
            while (tables.size() < partitionCount) {
                tables.add(new IntHashJoinKeyTable());
            }
            return partitionCount;
        } catch (Throwable th) {
            close();
            throw th;
        }
    }

    /**
     * Presizes the execution's storage. A positive row hint is the number of rows the build will
     * append and presizes the row heap. A positive key hint bounds the distinct keys among them and
     * presizes the key table for that many, so that no rehash runs below it; -1 leaves the table to
     * grow. On failure all execution allocations are released.
     */
    public void reserve(long rowCountHint, long keyCountHint) {
        requireBuilding();
        try {
            if (rowCountHint > 0) {
                heap.reserve(rowCountHint);
            }
            if (keyCountHint > 0) {
                keys.reserve(keyCountHint);
            }
        } catch (Throwable th) {
            close();
            throw th;
        }
    }

    // The bucket, or with fewer bits the partition, of a key: the top bits of its hash. The key
    // table's slot index takes the low bits, so the keys of one partition spread over its table.
    private static int bucketOf(int key, int shift) {
        return (int) ((Hash.hashInt64(key) >>> 32) >>> shift);
    }

    // Bits of the smallest power of two at least this many partitions, at most MAX_PARTITIONS.
    private static int getPartitionBits(long partitions) {
        return partitions <= 1 ? 0 : Math.min(MAX_PARTITION_BITS, 64 - Long.numberOfLeadingZeros(partitions - 1));
    }

    private static long toRowLink(int head) {
        return CompressedOffsets.uncompressAligned8(head);
    }

    // The caller owns failure cleanup. Growth checks the breaker per MiB of rehashed or copied memory.
    private void appendRow(int key, long rowId) {
        final long slot = keys.claim(key);
        final int previous = Unsafe.getInt(slot + 4);
        final long offset = heap.append(rowId, toRowLink(previous));
        Unsafe.putInt(slot, key);
        Unsafe.putInt(slot + 4, CompressedOffsets.compressBiased8(offset));
        if (previous == 0) {
            keys.keyCount++;
        }
    }

    private void checkPayloadSource(@Nullable HashJoinPayloadSource payloads) {
        if (payloads == null && heap.hasRowId()) {
            throw new IllegalArgumentException("hash join build with payload columns requires a payload source");
        }
    }

    private void freeChunks() {
        for (long entry = chunks.address, limit = entry + chunks.capacity; entry < limit; entry += CHUNK_ENTRY_SIZE) {
            final long chunk = Unsafe.getLong(entry);
            if (chunk != 0) {
                Unsafe.free(chunk, Unsafe.getLong(entry + Long.BYTES), MemoryTag.NATIVE_JOIN_MAP, memoryTracker);
                Unsafe.putLong(entry, 0);
            }
        }
    }

    private FrozenHashJoinBuild.IntKeyed freezeSerial(@Nullable HashJoinPayloadSource payloads) {
        circuitBreaker.statefulThrowExceptionIfTrippedTimeThrottled();
        checkPayloadSource(payloads);
        final Frozen serial = reusableFrozen != null ? reusableFrozen : new Frozen();
        serial.of(payloads);
        frozen = serial;
        return serial;
    }

    private long getBucketStride() {
        return (long) Integer.BYTES * ((1 << bucketBits) + 1);
    }

    // Bytes of a chunk entry: the key, and the row within its frame when the heap keeps row ids.
    private int getChunkEntrySize() {
        return heap.hasRowId() ? 2 * Integer.BYTES : Integer.BYTES;
    }

    private void partitionFrame(int frameIndex, PageFrameMemoryRecord record, int keyColumn, @Nullable DirectLongList rows, long rowCount) {
        assert open && frozen == null && frameIndex >= 0 && frameIndex < frameCount;
        final long counts = bucketStarts.address + frameIndex * getBucketStride();
        final int bucketCount = 1 << bucketBits;
        final int shift = 32 - bucketBits;
        // Each bucket counts one int to the right of its own, so that the running sums leave each
        // bucket's start in its own int and the frame's row count in the last one.
        for (long i = 0; i < rowCount; i++) {
            record.setRowIndex(rows != null ? rows.get(i) : i);
            final long counter = counts + (long) Integer.BYTES * (bucketOf(record.getInt(keyColumn), shift) + 1);
            Unsafe.putInt(counter, Unsafe.getInt(counter) + 1);
        }
        for (int b = 1; b <= bucketCount; b++) {
            final long counter = counts + (long) Integer.BYTES * b;
            Unsafe.putInt(counter, Unsafe.getInt(counter) + Unsafe.getInt(counter - Integer.BYTES));
        }
        final int keptCount = Unsafe.getInt(counts + (long) Integer.BYTES * bucketCount);
        if (keptCount == 0) {
            return;
        }
        final int entrySize = getChunkEntrySize();
        final long size = (long) keptCount * entrySize;
        final long chunk = Unsafe.malloc(size, MemoryTag.NATIVE_JOIN_MAP, memoryTracker);
        final long chunkEntry = chunks.address + (long) frameIndex * CHUNK_ENTRY_SIZE;
        Unsafe.putLong(chunkEntry, chunk);
        Unsafe.putLong(chunkEntry + Long.BYTES, size);
        final boolean hasRowId = heap.hasRowId();
        // Each bucket's start serves as its write cursor, which leaves it at the next bucket's start.
        for (long i = 0; i < rowCount; i++) {
            final long row = rows != null ? rows.get(i) : i;
            assert row <= Integer.MAX_VALUE;
            record.setRowIndex(row);
            final int key = record.getInt(keyColumn);
            final long cursor = counts + (long) Integer.BYTES * bucketOf(key, shift);
            final int position = Unsafe.getInt(cursor);
            Unsafe.putInt(cursor, position + 1);
            final long entry = chunk + (long) position * entrySize;
            Unsafe.putInt(entry, key);
            if (hasRowId) {
                Unsafe.putInt(entry + Integer.BYTES, (int) row);
            }
        }
        // Shift the cursors back to the starts.
        for (int b = bucketCount - 1; b > 0; b--) {
            final long counter = counts + (long) Integer.BYTES * b;
            Unsafe.putInt(counter, Unsafe.getInt(counter - Integer.BYTES));
        }
        Unsafe.putInt(counts, 0);
    }

    private void requireBuilding() {
        if (!open || frozen != null) {
            throw new IllegalStateException("hash join build is not mutable");
        }
    }

    /** What both kinds of snapshot share: the frozen heap and the source its probes read payloads through. */
    private abstract class AbstractFrozen implements FrozenHashJoinBuild.IntKeyed {
        long generation;
        long handleBase;
        long keysCount;
        HashJoinPayloadSource payloads;
        long rowsAddress;
        long rowsCount;
        long size;

        @Override
        public long getKeyCount() {
            return keysCount;
        }

        @Override
        public long getRowCount() {
            return rowsCount;
        }

        @Override
        public long getRowId(long ordinal) {
            assert heap.hasRowId() && ordinal >= 0 && ordinal < rowsCount && generation == heap.getGeneration();
            return HashJoinRowHeap.getRowIdAt(rowsAddress, ordinal);
        }

        @Override
        public long getSizeInBytes() {
            return size;
        }

        void ofHeap(HashJoinPayloadSource payloads, long keysCount) {
            handleBase = heap.nextHandleBase();
            this.keysCount = keysCount;
            rowsAddress = heap.getAddress();
            rowsCount = heap.getRowCount();
            size = IntHashJoinBuild.this.getSizeInBytes();
            this.payloads = payloads;
            generation = heap.freeze();
        }
    }

    private class Frozen extends AbstractFrozen {
        private long keysAddress;
        private int slots;

        @Override
        public FrozenHashJoinBuild.IntProbe newProbe() {
            if (frozen != this) {
                throw new IllegalStateException("hash join build has expired");
            }
            return new View();
        }

        private void of(HashJoinPayloadSource payloads) {
            keysAddress = keys.slots.address;
            slots = keys.slotCount;
            ofHeap(payloads, keys.keyCount);
        }

        private class View extends AbstractHashJoinProbe implements FrozenHashJoinBuild.IntProbe {
            private long lookupKeysAddress;
            private int lookupMask;

            private View() {
                super(heap);
                try {
                    reopen();
                } catch (Throwable th) {
                    close();
                    throw th;
                }
            }

            @Override
            public void find(int key) {
                assert isCurrent();
                long slot = IntHashJoinKeyTable.findSlot(keysAddress, slots, key);
                next = toRowLink(Unsafe.getInt(slot + 4));
            }

            @Override
            public boolean findSingleUnchecked(int key) {
                assert keysCount == rowsCount;
                if (rowsCount == 0) {
                    assert isCurrent();
                    next = 0;
                    return false;
                }
                final int head = findHead(key);
                next = 0;
                if (head == 0) {
                    return false;
                }
                positionAt(CompressedOffsets.uncompressBiased8(head));
                return true;
            }

            @Override
            public void findUnchecked(int key) {
                next = toRowLink(findHead(key));
            }

            @Override
            public void reopen() {
                if (frozen != Frozen.this) {
                    throw new IllegalStateException("hash join build has expired");
                }
                // This view is rebound after the previous execution drains. Cache the
                // immutable native lookup metadata for its entire acquired lifetime.
                lookupMask = slots - 1;
                lookupKeysAddress = keysAddress;
                ofSnapshot(payloads, handleBase, rowsAddress, rowsCount, generation);
            }

            private int findCollision(int key, long address) {
                final long base = lookupKeysAddress;
                final long limit = base + ((long) lookupMask + 1) * SLOT_SIZE;
                int head;
                do {
                    address += SLOT_SIZE;
                    if (address == limit) {
                        address = base;
                    }
                } while ((head = Unsafe.getInt(address + 4)) != 0 && Unsafe.getInt(address) != key);
                return head;
            }

            private int findHead(int key) {
                assert isCurrent();
                final int mask = lookupMask;
                final long base = lookupKeysAddress;
                long address = base + ((long) ((int) Hash.hashInt64(key) & mask)) * SLOT_SIZE;
                int head = Unsafe.getInt(address + 4);
                if (head != 0 && Unsafe.getInt(address) != key) {
                    head = findCollision(key, address);
                }
                return head;
            }
        }
    }

    /**
     * The snapshot of a parallel build of more than one partition. Its probes are a class of their
     * own, so that a reducer loop over a serial build's probes keeps one receiver class at each call
     * site, and one over these keeps another.
     */
    private class PartitionedFrozen extends AbstractFrozen {
        private long directoryAddress;
        private int partitionShift;

        @Override
        public FrozenHashJoinBuild.IntProbe newProbe() {
            if (frozen != this) {
                throw new IllegalStateException("hash join build has expired");
            }
            return new View();
        }

        private void of(HashJoinPayloadSource payloads, long keyCount) {
            directoryAddress = directory.address;
            partitionShift = 32 - partitionBits;
            ofHeap(payloads, keyCount);
        }

        private class View extends AbstractHashJoinProbe implements FrozenHashJoinBuild.IntProbe {
            private long lookupDirectory;
            private int lookupShift;

            private View() {
                super(heap);
                try {
                    reopen();
                } catch (Throwable th) {
                    close();
                    throw th;
                }
            }

            @Override
            public void find(int key) {
                next = toRowLink(findHead(key));
            }

            @Override
            public boolean findSingleUnchecked(int key) {
                assert keysCount == rowsCount;
                if (rowsCount == 0) {
                    assert isCurrent();
                    next = 0;
                    return false;
                }
                final int head = findHead(key);
                next = 0;
                if (head == 0) {
                    return false;
                }
                positionAt(CompressedOffsets.uncompressBiased8(head));
                return true;
            }

            @Override
            public void findUnchecked(int key) {
                next = toRowLink(findHead(key));
            }

            @Override
            public void reopen() {
                if (frozen != PartitionedFrozen.this) {
                    throw new IllegalStateException("hash join build has expired");
                }
                lookupDirectory = directoryAddress;
                lookupShift = partitionShift;
                ofSnapshot(payloads, handleBase, rowsAddress, rowsCount, generation);
            }

            private int findHead(int key) {
                assert isCurrent();
                final long hash = Hash.hashInt64(key);
                final long entry = lookupDirectory + ((hash >>> 32) >>> lookupShift) * DIRECTORY_ENTRY_SIZE;
                final long base = Unsafe.getLong(entry);
                final int mask = (int) Unsafe.getLong(entry + Long.BYTES);
                long address = base + ((long) ((int) hash & mask)) * SLOT_SIZE;
                int head = Unsafe.getInt(address + 4);
                if (head != 0 && Unsafe.getInt(address) != key) {
                    final long limit = base + ((long) mask + 1) * SLOT_SIZE;
                    do {
                        address += SLOT_SIZE;
                        if (address == limit) {
                            address = base;
                        }
                    } while ((head = Unsafe.getInt(address + 4)) != 0 && Unsafe.getInt(address) != key);
                }
                return head;
            }
        }
    }
}
