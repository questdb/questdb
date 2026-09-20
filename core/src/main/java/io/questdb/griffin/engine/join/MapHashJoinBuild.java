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
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.griffin.engine.CompressedOffsets;
import io.questdb.std.IntList;
import io.questdb.std.MemoryTracker;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.Transient;
import io.questdb.std.bytes.Bytes;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

import java.io.Closeable;

/**
 * Owner-built general-key lookup for fused hash join aggregation: a {@link Map} from the join
 * key to the head of a chain, plus the {@link HashJoinRowHeap} that {@link IntHashJoinBuild}
 * also uses. A key of any width and column count that a {@link RecordSink} can stage takes
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
 * Cancellation: the build loop checks the circuit breaker once per
 * {@value #ROWS_PER_BREAKER_CHECK} rows, and the row heap checks once per MiB it copies. The
 * maps never check it, so with a known row count the build presizes the map and no rehash runs;
 * with an unknown one, a single rehash and a single {@code OrderedMap.resize()} stay
 * uninterruptible.
 */
public final class MapHashJoinBuild implements Closeable {
    // The map value: the compressed offset of the chain head, as the INT layout's slot holds it.
    private static final SingleColumnType CHAIN_HEAD_TYPE = new SingleColumnType(ColumnType.INT);
    private static final int ROWS_PER_BREAKER_CHECK = 64 * 1024;
    private final HashJoinRowHeap heap;
    private final Map map;
    // The most keys the map can be presized to; beyond it setKeyCapacity() rejects the request.
    private final int maxPresizedKeys;
    // Exactly one of these is set; it is the concrete type a probe view binds to.
    @Nullable
    private final OrderedMap orderedMap;
    private final Frozen reusableFrozen;
    @Nullable
    private final Unordered8Map unordered8Map;
    private SqlExecutionCircuitBreaker circuitBreaker;
    private Frozen frozen;
    @Nullable
    private MemoryTracker memoryTracker;
    private boolean open;

    /** Payload types/indexes are in the same order; indexes address the source record. */
    @TestOnly
    public MapHashJoinBuild(
            CairoConfiguration configuration,
            @Transient @NotNull ColumnTypes keyTypes,
            ColumnTypes payloadTypes,
            IntList payloadColumns,
            int initialKeyCapacity,
            long initialMapHeapSize,
            long initialRowCapacity
    ) {
        this(configuration, keyTypes, payloadTypes, payloadColumns, initialKeyCapacity, initialMapHeapSize, initialRowCapacity, false);
    }

    /**
     * Reusable mode is for a factory that drains all consumers before reopening.
     * Its snapshot is a flyweight; retained probes must explicitly reopen for each
     * execution. The ordinary constructor keeps execution-specific snapshots.
     */
    public MapHashJoinBuild(
            CairoConfiguration configuration,
            @Transient @NotNull ColumnTypes keyTypes,
            ColumnTypes payloadTypes,
            IntList payloadColumns,
            int initialKeyCapacity,
            long initialMapHeapSize,
            long initialRowCapacity,
            boolean reusable
    ) {
        if (keyTypes.getColumnCount() < 1) {
            throw new IllegalArgumentException("hash join build needs at least one key column");
        }
        heap = new HashJoinRowHeap(payloadTypes, payloadColumns, initialRowCapacity);
        final double loadFactor = configuration.getSqlFastMapLoadFactor();
        maxPresizedKeys = (int) Math.min(Integer.MAX_VALUE, (long) (Numbers.MAX_SAFE_INT_POW_2 * loadFactor));
        try {
            // Lazy construction throughout: open() binds the execution's tracker before the
            // first byte is charged, as the ordinary hash joins do.
            if (keyTypes.getColumnCount() == 1 && Unordered8Map.isSupportedKeyType(keyTypes.getColumnType(0))) {
                orderedMap = null;
                map = unordered8Map = new Unordered8Map(
                        keyTypes.getColumnType(0),
                        CHAIN_HEAD_TYPE,
                        initialKeyCapacity,
                        loadFactor,
                        configuration.getSqlMapMaxResizes(),
                        false
                );
            } else {
                unordered8Map = null;
                map = orderedMap = new OrderedMap(
                        initialMapHeapSize,
                        keyTypes,
                        CHAIN_HEAD_TYPE,
                        initialKeyCapacity,
                        loadFactor,
                        configuration.getSqlMapMaxResizes(),
                        false
                );
            }
        } catch (Throwable th) {
            close();
            throw th;
        }
        reusableFrozen = reusable ? new Frozen() : null;
    }

    /** Copies one row, keyed by what the sink stages. On failure all execution allocations are released. */
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
     * Consumes a borrowed cursor once and resolves SYMBOL payloads through it until close, so
     * the caller keeps the cursor open until then. The sink stages the build key from each row;
     * it is the owner's own, since sinks must not be shared across workers. A nonnegative hint
     * is the remaining row count of a freshly acquired cursor, and presizes both the row heap
     * and the map.
     */
    public FrozenHashJoinBuild.RecordKeyed build(RecordCursor cursor, RecordSink keySink, long rowCountHint) {
        requireBuilding();
        try {
            if (rowCountHint > 0) {
                heap.reserve(rowCountHint);
                // A rehash is uninterruptible, so buy the whole table up front. The build has
                // no distinct-key estimate, so a duplicate-heavy build over-allocates the table.
                // A hint past what the map can hold is not an error here: the build may still
                // fit, since rows are not distinct keys, so leave those rehashes to the map.
                map.setKeyCapacity((int) Math.min(rowCountHint, maxPresizedKeys));
            }
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
            return freeze(cursor);
        } catch (Throwable th) {
            close();
            throw th;
        }
    }

    /** Only call after every probe is drained and aggregate output is finished. */
    @Override
    public void close() {
        if (frozen != null) {
            // Do not retain the borrowed source past its execution.
            frozen.symbols = null;
            frozen = null;
        }
        open = false;
        Misc.free(map);
        heap.close();
        memoryTracker = null;
        circuitBreaker = null;
    }

    /** Ends mutation of a build without SYMBOL payloads. */
    public FrozenHashJoinBuild.RecordKeyed freeze() {
        return freeze(null);
    }

    /**
     * Ends mutation. Views resolve SYMBOL payloads through the borrowed source until close.
     * Publication to probe workers is the caller's responsibility.
     */
    public FrozenHashJoinBuild.RecordKeyed freeze(@Nullable SymbolTableSource symbolSource) {
        requireBuilding();
        try {
            circuitBreaker.statefulThrowExceptionIfTrippedTimeThrottled();
            if (symbolSource == null && heap.hasSymbolPayload()) {
                throw new IllegalArgumentException("hash join build with SYMBOL payload requires a symbol source");
            }
            frozen = reusableFrozen != null ? reusableFrozen : new Frozen();
            frozen.of(symbolSource);
            return frozen;
        } catch (Throwable th) {
            close();
            throw th;
        }
    }

    /** Allocated native bytes of the row heap and the key table together, unused capacity included. */
    public long getSizeInBytes() {
        return heap.getSizeInBytes() + mapSizeInBytes();
    }

    /** Reopens a closed skeleton for a fresh execution. */
    public void open(@Nullable MemoryTracker memoryTracker, SqlExecutionCircuitBreaker circuitBreaker) {
        if (open) {
            throw new IllegalStateException("hash join build is already open");
        }
        this.memoryTracker = memoryTracker;
        this.circuitBreaker = circuitBreaker;
        heap.of(memoryTracker, circuitBreaker);
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
        final long offset = heap.append(record, toRowLink(previous));
        value.putInt(0, CompressedOffsets.compressBiased8(offset));
    }

    private long mapSizeInBytes() {
        if (!map.isOpen()) {
            // A closed map keeps its last capacity, so ask it nothing until it reopens.
            return 0;
        }
        if (unordered8Map != null) {
            // Entries are inline and eight-byte aligned, with one extra entry for the zero key.
            final long entrySize = Bytes.align8b(Long.BYTES + Integer.BYTES);
            return entrySize * ((long) unordered8Map.getKeyCapacity() + 1);
        }
        assert orderedMap != null;
        // The key heap, plus the offset table: one eight-byte {offset, hash} slot per capacity.
        return orderedMap.getHeapSize() + ((long) orderedMap.getKeyCapacity() << 3);
    }

    private void requireBuilding() {
        if (!open || frozen != null) {
            throw new IllegalStateException("hash join build is not mutable");
        }
    }

    private class Frozen implements FrozenHashJoinBuild.RecordKeyed {
        private long generation;
        private long handleBase;
        private long keysCount;
        private long rowsAddress;
        private long rowsCount;
        private long size;
        private SymbolTableSource symbols;

        @Override
        public long getKeyCount() {
            return keysCount;
        }

        @Override
        public long getRowCount() {
            return rowsCount;
        }

        @Override
        public long getSizeInBytes() {
            return size;
        }

        @Override
        public FrozenHashJoinBuild.RecordProbe newProbe(RecordSink probeKeySink) {
            if (frozen != this) {
                throw new IllegalStateException("hash join build has expired");
            }
            return new View(probeKeySink);
        }

        private void of(SymbolTableSource symbolSource) {
            handleBase = heap.nextHandleBase();
            keysCount = map.size();
            rowsAddress = heap.getAddress();
            rowsCount = heap.getRowCount();
            size = MapHashJoinBuild.this.getSizeInBytes();
            symbols = symbolSource;
            generation = heap.freeze();
        }

        private class View extends AbstractHashJoinProbe implements FrozenHashJoinBuild.RecordProbe {
            private final RecordSink keySink;
            private final MapProbeView view;

            private View(RecordSink keySink) {
                super(heap);
                this.keySink = keySink;
                view = orderedMap != null ? new OrderedMap.ProbeView() : new Unordered8Map.ProbeView();
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
                record.address = 0;
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
                record.address = payloadRowsAddress + CompressedOffsets.uncompressBiased8(head);
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
                if (orderedMap != null) {
                    ((OrderedMap.ProbeView) view).of(orderedMap);
                } else {
                    ((Unordered8Map.ProbeView) view).of(unordered8Map);
                }
                ofSnapshot(symbols, handleBase, rowsAddress, rowsCount, generation);
            }

            private int findHead(Record probeRecord) {
                assert isCurrent();
                keySink.copy(probeRecord, view.withKey());
                final MapValue value = view.findValue();
                return value != null ? value.getInt(0) : 0;
            }
        }
    }
}
