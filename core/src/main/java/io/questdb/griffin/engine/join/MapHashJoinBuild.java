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

import io.questdb.cairo.ArrayColumnTypes;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.ColumnTypes;
import io.questdb.cairo.RecordSink;
import io.questdb.cairo.SingleColumnType;
import io.questdb.cairo.map.Map;
import io.questdb.cairo.map.MapKey;
import io.questdb.cairo.map.MapProbeView;
import io.questdb.cairo.map.MapValue;
import io.questdb.cairo.map.OrderedMap;
import io.questdb.cairo.map.Unordered8Map;
import io.questdb.cairo.sql.PageFrameMemoryRecord;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.griffin.engine.CompressedOffsets;
import io.questdb.std.DirectLongList;
import io.questdb.std.MemoryTracker;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.Rows;
import io.questdb.std.Transient;
import io.questdb.std.Unsafe;
import io.questdb.std.bytes.Bytes;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

import java.io.Closeable;

/**
 * General-key lookup for fused hash join aggregation, built by the owner, or, for a large build, by
 * the workers one hash partition at a time (see the last paragraph): a {@link Map} from the join
 * key to the head of a chain, plus the {@link HashJoinRowHeap} that {@link IntHashJoinBuild}
 * also uses. Rows keep the ids of their build rows, and probes read payload columns through the
 * build's {@link HashJoinPayloadSource}. A key of any width and column count that a {@link RecordSink} can stage takes
 * this route; a single INT key, and a SYMBOL key translated into the build's domain, stay on
 * the narrower INT layout.
 * <p>
 * The map is chosen by the key alone. A single eight-byte key that
 * {@link Unordered8Map#isSupportedKeyType(int)} admits - LONG, TIMESTAMP micros and DATE - gets
 * an {@link Unordered8Map}, whose entry is inline in the table, so a lookup takes the same hops
 * as the INT layout. Everything else, every multi-column key and every variable-size key
 * included, gets an {@link OrderedMap}, which costs one more random access per match: slot,
 * entry, row. The maps are constructed directly rather than through
 * {@code MapFactory.createUnorderedMap()}, whose dispatch would also pick {@code Unordered4Map}
 * or {@code UnorderedVarcharMap}, for which no probe view exists.
 * <p>
 * The map value is one INT column holding the same compressed chain head the INT layout stores
 * in its slot. NULL keys match each other, as they do in the ordinary hash join: the shared
 * {@link RecordSink} encoding gives that by construction.
 * <p>
 * The maps keep their default memory tags, so the key table lands under
 * {@code NATIVE_FAST_MAP}/{@code NATIVE_FAST_MAP_INT_LIST} or {@code NATIVE_UNORDERED_MAP} while
 * the row heap stays under {@code NATIVE_JOIN_MAP}: an assertion about one build's bytes has to
 * sum the tags, and so does {@link #getSizeInBytes()}.
 * <p>
 * Cancellation: the build loops check the circuit breaker once per
 * {@value #ROWS_PER_BREAKER_CHECK} rows, and the row heap checks once per MiB it copies. The
 * maps never check it, so with a key count hint the build presizes the map and no rehash runs
 * below it; without one, a single rehash and a single {@code OrderedMap.resize()} stay
 * uninterruptible. A row count bounds the keys too, but the caller passes it as a key count hint
 * only when the keys are unlikely to repeat: a map sized by the rows of a repeating key holds
 * mostly empty slots.
 * <p>
 * A parallel build runs the two rounds of {@link IntHashJoinBuild}'s, over a map per partition. A
 * frame task stages each kept row's key through the caller's key sink into a probe view of the
 * build's layout, which hashes it as the map would, and sorts the row into its bucket by the top
 * bits of that hash; the frame's chunk holds each row's key as the map encodes it, so a partition
 * task inserts the key without reading the frame again. The frame task stages each key twice, once
 * to count its bucket and once to write it there, rather than keep a second copy of the frame's
 * keys. A probe of such a build stages and hashes its key once, then looks it up in the one map
 * that the hash's top bits select; see {@link #freezePartitioned}.
 */
public final class MapHashJoinBuild implements Closeable {
    public static final int MAX_PARTITIONS = HashJoinPartitions.MAX_PARTITIONS;
    // The map value: the compressed offset of the chain head, as the INT layout's slot holds it.
    private static final SingleColumnType CHAIN_HEAD_TYPE = new SingleColumnType(ColumnType.INT);
    private static final long MAX_BUFFER_SIZE = 1L << 48;
    private static final int ROWS_PER_BREAKER_CHECK = 64 * 1024;
    // Each frame's bucket starts within its chunk, in bytes, plus the chunk's size: bucket count + 1
    // longs. Keys may vary in size, so a bucket's rows do not tell where it starts.
    private final HashJoinBuffer bucketOffsets = new HashJoinBuffer(MAX_BUFFER_SIZE);
    private final HashJoinRowHeap heap;
    private final int initialKeyCapacity;
    private final long initialMapHeapSize;
    // Bytes of a raw key, or -1 for a var-size key, whose chunk entry leads with its length.
    private final long keySize;
    // A copy of the constructor's transient key types, from which each later partition's map is made.
    private final ArrayColumnTypes keyTypes = new ArrayColumnTypes();
    private final double loadFactor;
    // The serial build's map, and a parallel build's first partition's.
    private final Map map;
    private final int mapMaxResizes;
    // One map per partition of a parallel build; the first is the serial build's.
    private final ObjList<Map> maps = new ObjList<>();
    // The most keys the map can be presized to; beyond it setKeyCapacity() rejects the request.
    private final int maxPresizedKeys;
    // Exactly one of these is set; it is the concrete type of every map, which a probe view binds to.
    @Nullable
    private final OrderedMap orderedMap;
    private final HashJoinPartitions partitions = new HashJoinPartitions();
    private final Frozen reusableFrozen;
    private final PartitionedFrozen reusablePartitionedFrozen;
    @Nullable
    private final Unordered8Map unordered8Map;
    private SqlExecutionCircuitBreaker circuitBreaker;
    private AbstractFrozen frozen;
    @Nullable
    private MemoryTracker memoryTracker;
    private boolean open;

    /** A build with payload columns stores row ids for probes to read them through; see the class docs. */
    @TestOnly
    public MapHashJoinBuild(
            CairoConfiguration configuration,
            @Transient @NotNull ColumnTypes keyTypes,
            boolean hasPayload,
            int initialKeyCapacity,
            long initialMapHeapSize,
            long initialRowCapacity
    ) {
        this(configuration, keyTypes, hasPayload, initialKeyCapacity, initialMapHeapSize, initialRowCapacity, false);
    }

    /**
     * Reusable mode is for a factory that drains all consumers before reopening.
     * Its snapshot is a flyweight; retained probes must explicitly reopen for each
     * execution. The ordinary constructor keeps execution-specific snapshots.
     */
    public MapHashJoinBuild(
            CairoConfiguration configuration,
            @Transient @NotNull ColumnTypes keyTypes,
            boolean hasPayload,
            int initialKeyCapacity,
            long initialMapHeapSize,
            long initialRowCapacity,
            boolean reusable
    ) {
        final int keyColumnCount = keyTypes.getColumnCount();
        if (keyColumnCount < 1) {
            throw new IllegalArgumentException("hash join build needs at least one key column");
        }
        heap = new HashJoinRowHeap(hasPayload, initialRowCapacity);
        loadFactor = configuration.getSqlFastMapLoadFactor();
        mapMaxResizes = configuration.getSqlMapMaxResizes();
        this.initialKeyCapacity = initialKeyCapacity;
        this.initialMapHeapSize = initialMapHeapSize;
        maxPresizedKeys = (int) Math.min(Integer.MAX_VALUE, (long) (Numbers.MAX_SAFE_INT_POW_2 * loadFactor));
        long rawKeySize = 0;
        for (int i = 0; i < keyColumnCount; i++) {
            final int columnType = keyTypes.getColumnType(i);
            this.keyTypes.add(columnType);
            // The map's own rule: any column without a fixed size makes the key var-size.
            final int size = ColumnType.sizeOf(columnType);
            rawKeySize = rawKeySize != -1 && size > 0 ? rawKeySize + size : -1;
        }
        keySize = rawKeySize;
        try {
            // Lazy construction throughout: open() binds the execution's tracker before the
            // first byte is charged, as the ordinary hash joins do.
            map = newMap();
            maps.add(map);
            if (map instanceof OrderedMap ordered) {
                orderedMap = ordered;
                unordered8Map = null;
            } else {
                orderedMap = null;
                unordered8Map = (Unordered8Map) map;
            }
        } catch (Throwable th) {
            close();
            throw th;
        }
        reusableFrozen = reusable ? new Frozen() : null;
        reusablePartitionedFrozen = reusable ? new PartitionedFrozen() : null;
    }

    /**
     * Appends one row, keyed by what the sink stages and identified by the record's row id. On
     * failure all execution allocations are released.
     */
    public void append(Record record, RecordSink keySink) {
        requireBuilding();
        try {
            appendRow(record, keySink);
        } catch (Throwable th) {
            close();
            throw th;
        }
    }

    /**
     * Appends every row of the page frame the record is bound to. The sink stages the build key
     * from each row; it is the owner's own, since sinks must not be shared across workers. The
     * caller checks cancellation at frame boundaries, and the loop checks too, because the map's
     * own growth does not. On failure all execution allocations are released.
     */
    public void appendFrame(PageFrameMemoryRecord record, RecordSink keySink, long rowCount) {
        requireBuilding();
        try {
            for (long r = 0; r < rowCount; r++) {
                if (((r + 1) & (ROWS_PER_BREAKER_CHECK - 1)) == 0) {
                    circuitBreaker.statefulThrowExceptionIfTrippedTimeThrottled();
                }
                record.setRowIndex(r);
                appendRow(record, keySink);
            }
        } catch (Throwable th) {
            close();
            throw th;
        }
    }

    /** The filtered twin of {@link #appendFrame(PageFrameMemoryRecord, RecordSink, long)}: appends the listed rows only. */
    public void appendFrame(PageFrameMemoryRecord record, RecordSink keySink, DirectLongList rows) {
        requireBuilding();
        try {
            for (long p = 0, n = rows.size(); p < n; p++) {
                if (((p + 1) & (ROWS_PER_BREAKER_CHECK - 1)) == 0) {
                    circuitBreaker.statefulThrowExceptionIfTrippedTimeThrottled();
                }
                record.setRowIndex(rows.get(p));
                appendRow(record, keySink);
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
     * partition, on any thread, and {@link #freezePartitioned}. The arguments are those of
     * {@link IntHashJoinBuild#beginPartitioning}. On failure all execution allocations are released.
     */
    public void beginPartitioning(int frameCount, long rowCountBound, long rowsPerPartition) {
        requireBuilding();
        try {
            if (heap.getRowCount() != 0 || map.size() != 0) {
                throw new IllegalStateException("hash join build cannot start partitioning");
            }
            // Each partition's map opens on the thread that builds it, the first partition's too.
            map.close();
            partitions.begin(frameCount, rowCountBound, rowsPerPartition);
            bucketOffsets.allocate((long) frameCount * getOffsetStride(), true);
        } catch (Throwable th) {
            close();
            throw th;
        }
    }

    /**
     * Consumes a borrowed cursor once, keeping the id of each row, and freezes. Probes read payload
     * columns through the source, so the caller keeps whatever the source reads open until close.
     * The sink stages the build key from each row; it is the owner's own, since sinks must not be
     * shared across workers. The hints are those of {@link #reserve(long, long)}.
     */
    public FrozenHashJoinBuild.RecordKeyed build(
            RecordCursor cursor,
            RecordSink keySink,
            long rowCountHint,
            long keyCountHint,
            @Nullable HashJoinPayloadSource payloads
    ) {
        reserve(rowCountHint, keyCountHint);
        try {
            final Record record = cursor.getRecord();
            // The source cursor checks the breaker at its frame boundaries; the map's own
            // growth does not, so the loop checks too.
            long rows = 0;
            while (cursor.hasNext()) {
                if ((rows++ & (ROWS_PER_BREAKER_CHECK - 1)) == 0) {
                    circuitBreaker.statefulThrowExceptionIfTrippedTimeThrottled();
                }
                appendRow(record, keySink);
            }
        } catch (Throwable th) {
            close();
            throw th;
        }
        return freeze(payloads);
    }

    /**
     * Builds one partition of a parallel build: inserts the partition's keys out of every frame's
     * chunk into the partition's map, which opens here, and writes their rows into the partition's
     * region of the heap. Runs on any thread, once per partition, concurrently with the other
     * partitions; the calling thread's circuit breaker cancels it. On failure the caller closes the
     * build once every partition task has stopped.
     */
    public void buildPartition(int partition, SqlExecutionCircuitBreaker circuitBreaker) {
        assert open && frozen == null && partitions.isPartitioning() && partition >= 0 && partition < partitions.getPartitionCount();
        circuitBreaker.statefulThrowExceptionIfTrippedTimeThrottled();
        final Map partitionMap = maps.getQuick(partition);
        partitionMap.setMemoryTracker(memoryTracker);
        partitionMap.reopen();
        final long keyCountHint = partitions.getPartitionKeyHint(partition);
        if (keyCountHint > 0) {
            partitionMap.setKeyCapacity((int) Math.min(keyCountHint, maxPresizedKeys));
        }
        final long bucketLo = partitions.getPartitionBucketLo(partition);
        final long bucketHi = partitions.getPartitionBucketLo(partition + 1);
        final long offsetStride = getOffsetStride();
        final boolean hasRowId = heap.hasRowId();
        final int rowSize = heap.getRowSize();
        long ordinal = partitions.getPartitionStart(partition);
        for (int frame = 0, frameCount = partitions.getFrameCount(); frame < frameCount; frame++) {
            partitions.setSegmentStart(frame, partition, ordinal);
            final long counts = partitions.getBucketStarts(frame);
            final int rowCount = Unsafe.getInt(counts + bucketHi) - Unsafe.getInt(counts + bucketLo);
            if (rowCount == 0) {
                continue;
            }
            // The bucket table holds an int per bucket, the offset table a long.
            long entry = partitions.getChunk(frame) + Unsafe.getLong(bucketOffsets.address + frame * offsetStride + 2 * bucketLo);
            for (int r = 0; r < rowCount; r++, ordinal++) {
                if ((ordinal & (ROWS_PER_BREAKER_CHECK - 1)) == 0) {
                    circuitBreaker.statefulThrowExceptionIfTrippedTimeThrottled();
                }
                final long rawKeySize;
                if (keySize != -1) {
                    rawKeySize = keySize;
                } else {
                    rawKeySize = Unsafe.getInt(entry);
                    entry += Integer.BYTES;
                }
                final MapKey key = withRawKey(partitionMap, entry, rawKeySize);
                entry += rawKeySize;
                final long rowId;
                if (hasRowId) {
                    rowId = Rows.toRowID(frame, Unsafe.getInt(entry));
                    entry += Integer.BYTES;
                } else {
                    rowId = 0;
                }
                final MapValue value = key.createValue();
                // A new key ends its chain here; an existing one links to the row it displaces.
                final int previous = value.isNew() ? 0 : value.getInt(0);
                final long offset = ordinal * rowSize;
                heap.put(offset, toRowLink(previous), rowId);
                value.putInt(0, CompressedOffsets.compressBiased8(offset));
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
        partitions.close();
        bucketOffsets.close();
        Misc.freeObjListAndKeepObjects(maps);
        heap.close();
        memoryTracker = null;
        circuitBreaker = null;
    }

    /** Ends mutation of a build without payload columns. */
    public FrozenHashJoinBuild.RecordKeyed freeze() {
        return freeze(null);
    }

    /**
     * Ends mutation. Probes read payload columns through the borrowed source until close.
     * Publication to probe workers is the caller's responsibility.
     */
    public FrozenHashJoinBuild.RecordKeyed freeze(@Nullable HashJoinPayloadSource payloads) {
        requireBuilding();
        try {
            if (partitions.isPartitioning()) {
                throw new IllegalStateException("partitioned hash join build freezes through freezePartitioned()");
            }
            return freezeSerial(payloads);
        } catch (Throwable th) {
            close();
            throw th;
        }
    }

    /**
     * Ends a parallel build once every partition is built, and frees the frames' chunks. A build of
     * one partition freezes as a serial build does, since its one map is the serial build's; probes
     * of any other build take their map from the key's hash first, a probe class of their own.
     * Probes read payload columns through the borrowed source until close. On failure all execution
     * allocations are released.
     */
    public FrozenHashJoinBuild.RecordKeyed freezePartitioned(@Nullable HashJoinPayloadSource payloads) {
        requireBuilding();
        try {
            if (!partitions.isPartitioning()) {
                throw new IllegalStateException("hash join build is not partitioned");
            }
            // The maps and the rows hold everything the chunks held.
            partitions.freeChunks();
            final int partitionCount = partitions.getPartitionCount();
            if (partitionCount == 1) {
                return freezeSerial(payloads);
            }
            circuitBreaker.statefulThrowExceptionIfTrippedTimeThrottled();
            checkPayloadSource(payloads);
            long keyCount = 0;
            for (int p = 0; p < partitionCount; p++) {
                keyCount += maps.getQuick(p).size();
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
        return partitions.getPartitionCount();
    }

    /** Rows of this partition of a parallel build, once the partitions are planned. */
    public long getPartitionRowCount(int partition) {
        return partitions.getPartitionRowCount(partition);
    }

    /** Rows the frames of a parallel build kept, once every frame is partitioned. */
    public long getPartitionedRowCount() {
        return partitions.getPartitionedRowCount();
    }

    /** Rows that this frame of a parallel build keeps in this partition. */
    public long getSegmentRowCount(int frameIndex, int partition) {
        return partitions.getSegmentRowCount(frameIndex, partition);
    }

    /**
     * Heap ordinal of the first row that this frame of a parallel build keeps in this partition.
     * The frame's rows of the partition follow it in the frame's order; see
     * {@link #getSegmentRowCount(int, int)}. Valid from the partition's build until close.
     */
    public long getSegmentStart(int frameIndex, int partition) {
        return partitions.getSegmentStart(frameIndex, partition);
    }

    /** Allocated native bytes of the row heap, the key tables and a parallel build's tables, unused capacity included. */
    public long getSizeInBytes() {
        long size = heap.getSizeInBytes() + partitions.getSizeInBytes() + bucketOffsets.capacity;
        for (int i = 0, n = maps.size(); i < n; i++) {
            size += mapSizeInBytes(maps.getQuick(i));
        }
        return size;
    }

    /**
     * A view that stages the build's keys on one thread for {@code partitionFrame()}: a probe view
     * of the build's own map class, which the frame task binds to the build's key layout. The caller
     * keeps one per thread across executions, and closes it at the end of each execution, since it
     * holds the staged key in memory that the execution charged.
     */
    public MapProbeView newKeyStager() {
        return orderedMap != null ? new OrderedMap.ProbeView() : new Unordered8Map.ProbeView();
    }

    /** Reopens a closed skeleton for a fresh execution. */
    public void open(@Nullable MemoryTracker memoryTracker, SqlExecutionCircuitBreaker circuitBreaker) {
        if (open) {
            throw new IllegalStateException("hash join build is already open");
        }
        this.memoryTracker = memoryTracker;
        this.circuitBreaker = circuitBreaker;
        heap.of(memoryTracker, circuitBreaker);
        partitions.of(memoryTracker, circuitBreaker);
        bucketOffsets.of(memoryTracker, circuitBreaker);
        try {
            circuitBreaker.statefulThrowExceptionIfTrippedTimeThrottled();
            // The tracker has to be bound before the map's first allocation, so that malloc and
            // free are charged symmetrically.
            map.setMemoryTracker(memoryTracker);
            map.reopen();
            open = true;
        } catch (Throwable th) {
            close();
            throw th;
        }
    }

    /**
     * Sorts the rows of one page frame of a parallel build into the frame's chunk, grouped by
     * bucket: every row of the frame, keyed by what the sink stages through the stager. The sink and
     * the stager, see {@link #newKeyStager()}, belong to the calling thread. Runs on any thread, once
     * per frame, concurrently with the other frames. On failure the caller closes the build once
     * every frame task has stopped.
     */
    public void partitionFrame(int frameIndex, PageFrameMemoryRecord record, RecordSink keySink, MapProbeView stager, long rowCount) {
        partitionFrame(frameIndex, record, keySink, stager, null, rowCount);
    }

    /** The filtered twin of {@link #partitionFrame(int, PageFrameMemoryRecord, RecordSink, MapProbeView, long)}: sorts the listed rows only. */
    public void partitionFrame(int frameIndex, PageFrameMemoryRecord record, RecordSink keySink, MapProbeView stager, DirectLongList rows) {
        partitionFrame(frameIndex, record, keySink, stager, rows, rows.size());
    }

    /**
     * Sizes the row heap for exactly the rows the frames kept and groups the buckets into
     * partitions of about {@code rowsPerPartition} rows each, a power of two of them. A positive
     * key hint bounds the distinct keys and presizes each partition's map by its share, as
     * {@link #reserve(long, long)} presizes the serial one; -1 lets the maps grow. Returns the
     * partition count. On failure all execution allocations are released.
     */
    public int planPartitions(long rowsPerPartition, long keyCountHint) {
        requireBuilding();
        try {
            final int partitionCount = partitions.plan(rowsPerPartition, keyCountHint);
            heap.allocateRows(partitions.getPartitionedRowCount());
            while (maps.size() < partitionCount) {
                maps.add(newMap());
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
     * presizes the map for that many; -1 leaves the map to grow. On failure all execution
     * allocations are released.
     */
    public void reserve(long rowCountHint, long keyCountHint) {
        requireBuilding();
        try {
            if (rowCountHint > 0) {
                heap.reserve(rowCountHint);
            }
            if (keyCountHint > 0) {
                // A rehash is uninterruptible, so buy the whole table up front. A hint past what
                // the map can hold is not an error here: the build may still fit, since the hint
                // only bounds the distinct keys, so leave those rehashes to the map.
                map.setKeyCapacity((int) Math.min(keyCountHint, maxPresizedKeys));
            }
        } catch (Throwable th) {
            close();
            throw th;
        }
    }

    private static long toRowLink(int head) {
        return CompressedOffsets.uncompressAligned8(head);
    }

    // The caller owns failure cleanup.
    private void appendRow(Record record, RecordSink keySink) {
        final MapKey key = map.withKey();
        keySink.copy(record, key);
        final MapValue value = key.createValue();
        // A new key ends its chain here; an existing one links to the row it displaces.
        final int previous = value.isNew() ? 0 : value.getInt(0);
        final long offset = heap.append(heap.hasRowId() ? record.getRowId() : 0, toRowLink(previous));
        value.putInt(0, CompressedOffsets.compressBiased8(offset));
    }

    // Binds a probe view to one of the build's maps, which checks the map's layout against the view's.
    private void bindView(MapProbeView view, Map target) {
        if (orderedMap != null) {
            ((OrderedMap.ProbeView) view).of((OrderedMap) target);
        } else {
            ((Unordered8Map.ProbeView) view).of((Unordered8Map) target);
        }
    }

    private void checkPayloadSource(@Nullable HashJoinPayloadSource payloads) {
        if (payloads == null && heap.hasRowId()) {
            throw new IllegalArgumentException("hash join build with payload columns requires a payload source");
        }
    }

    private FrozenHashJoinBuild.RecordKeyed freezeSerial(@Nullable HashJoinPayloadSource payloads) {
        circuitBreaker.statefulThrowExceptionIfTrippedTimeThrottled();
        checkPayloadSource(payloads);
        final Frozen serial = reusableFrozen != null ? reusableFrozen : new Frozen();
        serial.of(payloads);
        frozen = serial;
        return serial;
    }

    private long getOffsetStride() {
        return (long) Long.BYTES * (partitions.getBucketCount() + 1);
    }

    private long mapSizeInBytes(Map target) {
        if (!target.isOpen()) {
            // A closed map keeps its last capacity, so ask it nothing until it reopens.
            return 0;
        }
        if (unordered8Map != null) {
            // Entries are inline and eight-byte aligned, with one extra entry for the zero key.
            final long entrySize = Bytes.align8b(Long.BYTES + Integer.BYTES);
            return entrySize * ((long) target.getKeyCapacity() + 1);
        }
        // The key heap, plus the offset table: one eight-byte {offset, hash} slot per capacity.
        return ((OrderedMap) target).getHeapSize() + ((long) target.getKeyCapacity() << 3);
    }

    // A map of the build's key layout that allocates nothing until it reopens.
    private Map newMap() {
        if (keyTypes.getColumnCount() == 1 && Unordered8Map.isSupportedKeyType(keyTypes.getColumnType(0))) {
            return new Unordered8Map(keyTypes.getColumnType(0), CHAIN_HEAD_TYPE, initialKeyCapacity, loadFactor, mapMaxResizes, false);
        }
        return new OrderedMap(initialMapHeapSize, keyTypes, CHAIN_HEAD_TYPE, initialKeyCapacity, loadFactor, mapMaxResizes, false);
    }

    private void partitionFrame(
            int frameIndex,
            PageFrameMemoryRecord record,
            RecordSink keySink,
            MapProbeView stager,
            @Nullable DirectLongList rows,
            long rowCount
    ) {
        assert open && frozen == null && frameIndex >= 0 && frameIndex < partitions.getFrameCount();
        // The stager's staged key lives in memory that this execution charges.
        stager.setMemoryTracker(memoryTracker);
        if (orderedMap != null) {
            ((OrderedMap.ProbeView) stager).ofLayout(orderedMap);
        } else {
            ((Unordered8Map.ProbeView) stager).ofLayout(unordered8Map);
        }
        final long counts = partitions.getBucketStarts(frameIndex);
        final long offsets = bucketOffsets.address + frameIndex * getOffsetStride();
        final int bucketCount = partitions.getBucketCount();
        final int shift = partitions.getBucketShift();
        final long lengthSize = keySize == -1 ? Integer.BYTES : 0;
        final long rowIdSize = heap.hasRowId() ? Integer.BYTES : 0;
        // Each bucket counts its rows and bytes one slot to the right of its own, so that the running
        // sums leave each bucket's starts in its own slots and the frame's totals in the last ones.
        for (long i = 0; i < rowCount; i++) {
            record.setRowIndex(rows != null ? rows.get(i) : i);
            keySink.copy(record, stager.withKey());
            final int bucket = HashJoinPartitions.bucketOf(stager.hash(), shift) + 1;
            final long counter = counts + (long) Integer.BYTES * bucket;
            Unsafe.putInt(counter, Unsafe.getInt(counter) + 1);
            final long byteCounter = offsets + (long) Long.BYTES * bucket;
            Unsafe.putLong(byteCounter, Unsafe.getLong(byteCounter) + lengthSize + stager.getStagedKeySize() + rowIdSize);
        }
        for (int b = 1; b <= bucketCount; b++) {
            final long counter = counts + (long) Integer.BYTES * b;
            Unsafe.putInt(counter, Unsafe.getInt(counter) + Unsafe.getInt(counter - Integer.BYTES));
            final long byteCounter = offsets + (long) Long.BYTES * b;
            Unsafe.putLong(byteCounter, Unsafe.getLong(byteCounter) + Unsafe.getLong(byteCounter - Long.BYTES));
        }
        if (Unsafe.getInt(counts + (long) Integer.BYTES * bucketCount) == 0) {
            return;
        }
        final long chunk = partitions.allocateChunk(frameIndex, Unsafe.getLong(offsets + (long) Long.BYTES * bucketCount));
        // Each bucket's byte start serves as its write cursor, which leaves it at the next bucket's start.
        for (long i = 0; i < rowCount; i++) {
            final long row = rows != null ? rows.get(i) : i;
            assert row <= Integer.MAX_VALUE;
            record.setRowIndex(row);
            keySink.copy(record, stager.withKey());
            final long cursor = offsets + (long) Long.BYTES * HashJoinPartitions.bucketOf(stager.hash(), shift);
            long entry = chunk + Unsafe.getLong(cursor);
            if (lengthSize != 0) {
                Unsafe.putInt(entry, (int) stager.getStagedKeySize());
                entry += Integer.BYTES;
            }
            entry += stager.copyStagedKey(entry);
            if (rowIdSize != 0) {
                Unsafe.putInt(entry, (int) row);
                entry += Integer.BYTES;
            }
            Unsafe.putLong(cursor, entry - chunk);
        }
        // Shift the cursors back to the starts.
        for (int b = bucketCount - 1; b > 0; b--) {
            final long byteCounter = offsets + (long) Long.BYTES * b;
            Unsafe.putLong(byteCounter, Unsafe.getLong(byteCounter - Long.BYTES));
        }
        Unsafe.putLong(offsets, 0);
    }

    private void requireBuilding() {
        if (!open || frozen != null) {
            throw new IllegalStateException("hash join build is not mutable");
        }
    }

    // Starts a key of one of the build's maps from the raw key a frame task staged.
    private MapKey withRawKey(Map target, long address, long size) {
        return orderedMap != null
                ? ((OrderedMap) target).withRawKey(address, size)
                : ((Unordered8Map) target).withRawKey(address, size);
    }

    /** What both kinds of snapshot share: the frozen heap and the source its probes read payloads through. */
    private abstract class AbstractFrozen implements FrozenHashJoinBuild.RecordKeyed {
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
            size = MapHashJoinBuild.this.getSizeInBytes();
            this.payloads = payloads;
            generation = heap.freeze();
        }
    }

    private class Frozen extends AbstractFrozen {

        @Override
        public FrozenHashJoinBuild.RecordProbe newProbe(RecordSink probeKeySink) {
            if (frozen != this) {
                throw new IllegalStateException("hash join build has expired");
            }
            return new View(probeKeySink);
        }

        private void of(HashJoinPayloadSource payloads) {
            ofHeap(payloads, map.size());
        }

        private class View extends AbstractHashJoinProbe implements FrozenHashJoinBuild.RecordProbe {
            private final RecordSink keySink;
            private final MapProbeView view;

            private View(RecordSink keySink) {
                super(heap);
                this.keySink = keySink;
                view = newKeyStager();
                try {
                    reopen();
                } catch (Throwable th) {
                    close();
                    throw th;
                }
            }

            @Override
            public void close() {
                Misc.free(view);
                super.close();
            }

            @Override
            public void find(Record probeRecord) {
                assert isCurrent();
                next = toRowLink(findHead(probeRecord));
            }

            @Override
            public boolean findSingleUnchecked(Record probeRecord) {
                assert keysCount == rowsCount;
                if (rowsCount == 0) {
                    assert isCurrent();
                    next = 0;
                    return false;
                }
                final int head = findHead(probeRecord);
                next = 0;
                if (head == 0) {
                    return false;
                }
                positionAt(CompressedOffsets.uncompressBiased8(head));
                return true;
            }

            @Override
            public void findUnchecked(Record probeRecord) {
                next = toRowLink(findHead(probeRecord));
            }

            @Override
            public void reopen() {
                if (frozen != Frozen.this) {
                    throw new IllegalStateException("hash join build has expired");
                }
                // The staging buffer belongs to the execution that charged it, so rebind the
                // tracker before the view primes it against this execution's map.
                view.setMemoryTracker(memoryTracker);
                bindView(view, map);
                ofSnapshot(payloads, handleBase, rowsAddress, rowsCount, generation);
            }

            private int findHead(Record probeRecord) {
                assert isCurrent();
                keySink.copy(probeRecord, view.withKey());
                final MapValue value = view.findValue();
                return value != null ? value.getInt(0) : 0;
            }
        }
    }

    /**
     * The snapshot of a parallel build of more than one partition. Its probes are a class of their
     * own, so that a reducer loop over a serial build's probes keeps one receiver class at each call
     * site, and one over these keeps another.
     */
    private class PartitionedFrozen extends AbstractFrozen {
        private int partitionCount;
        private int partitionShift;

        @Override
        public FrozenHashJoinBuild.RecordProbe newProbe(RecordSink probeKeySink) {
            if (frozen != this) {
                throw new IllegalStateException("hash join build has expired");
            }
            return new View(probeKeySink);
        }

        private void of(HashJoinPayloadSource payloads, long keyCount) {
            partitionCount = partitions.getPartitionCount();
            partitionShift = partitions.getPartitionShift();
            ofHeap(payloads, keyCount);
        }

        private class View extends AbstractHashJoinProbe implements FrozenHashJoinBuild.RecordProbe {
            private final RecordSink keySink;
            private final MapProbeView view;
            private int lookupShift;

            private View(RecordSink keySink) {
                super(heap);
                this.keySink = keySink;
                view = newKeyStager();
                try {
                    reopen();
                } catch (Throwable th) {
                    close();
                    throw th;
                }
            }

            @Override
            public void close() {
                Misc.free(view);
                super.close();
            }

            @Override
            public void find(Record probeRecord) {
                next = toRowLink(findHead(probeRecord));
            }

            @Override
            public boolean findSingleUnchecked(Record probeRecord) {
                assert keysCount == rowsCount;
                if (rowsCount == 0) {
                    assert isCurrent();
                    next = 0;
                    return false;
                }
                final int head = findHead(probeRecord);
                next = 0;
                if (head == 0) {
                    return false;
                }
                positionAt(CompressedOffsets.uncompressBiased8(head));
                return true;
            }

            @Override
            public void findUnchecked(Record probeRecord) {
                next = toRowLink(findHead(probeRecord));
            }

            @Override
            public void reopen() {
                if (frozen != PartitionedFrozen.this) {
                    throw new IllegalStateException("hash join build has expired");
                }
                view.setMemoryTracker(memoryTracker);
                // Binding to every partition's map checks its layout once, so that the lookups
                // can take whichever map a key selects without binding to it.
                for (int p = 0; p < partitionCount; p++) {
                    bindView(view, maps.getQuick(p));
                }
                lookupShift = partitionShift;
                ofSnapshot(payloads, handleBase, rowsAddress, rowsCount, generation);
            }

            private int findHead(Record probeRecord) {
                assert isCurrent();
                keySink.copy(probeRecord, view.withKey());
                final MapValue value = view.findValueIn(maps.getQuick(HashJoinPartitions.bucketOf(view.hash(), lookupShift)));
                return value != null ? value.getInt(0) : 0;
            }
        }
    }
}
