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

package io.questdb.griffin.engine.window;

import io.questdb.cairo.AbstractRecordCursorFactory;
import io.questdb.cairo.ArrayColumnTypes;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.GenericRecordMetadata;
import io.questdb.cairo.RecordSink;
import io.questdb.cairo.Reopenable;
import io.questdb.cairo.map.Map;
import io.questdb.cairo.map.MapKey;
import io.questdb.cairo.map.MapRecord;
import io.questdb.cairo.map.MapRecordCursor;
import io.questdb.cairo.map.MapValue;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.VirtualRecord;
import io.questdb.cairo.sql.WindowSPI;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryARW;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;

/**
 * Streaming window factory that supports deferred-emit window functions (e.g. LEAD).
 * <p>
 * Phase 2/3/4/5 restrictions enforced at construction time:
 * <ul>
 *   <li>Exactly one window function with positive {@link WindowFunction#getLookahead() lookahead}.</li>
 *   <li>No additional window functions in the output (no mixed LAG + LEAD yet).</li>
 *   <li>Lookahead {@code <=} 63 (per-partition filled-bit mask is a single long).</li>
 *   <li>Base cursor must support random access. Pending entries store the base row id and look up
 *       base column values via {@link RecordCursor#recordAt(Record, long)} at emission time.</li>
 * </ul>
 * Optionally supports {@code PARTITION BY} via the {@link Map}/{@link VirtualRecord}/{@link RecordSink}
 * trio supplied at construction. When no PARTITION BY is present (partitionByRecord {@code == null}),
 * the cursor uses a single global ring buffer; otherwise it lazily allocates one ring per partition
 * inside a shared {@link MemoryARW} and tracks per-partition state in the supplied {@link Map}.
 * <p>
 * The cursor enforces {@code cairo.sql.window.streaming.max.partitions} at runtime; an overflowing
 * partition map throws {@link CairoException} mid-cursor rather than allowing unbounded memory
 * growth.
 * <p>
 * Cross-thread safety: the cursor is single-threaded. The partition map, pending memory, and base
 * cursor are private to this cursor instance and never shared across threads.
 */
public class DeferredEmitWindowRecordCursorFactory extends AbstractRecordCursorFactory {

    public static final ArrayColumnTypes PARTITION_VALUE_TYPES;
    private static final long SLOT_BYTES = 16L;          // 8 (rowid) + 8 (lead value)
    private static final long SLOT_LEAD_OFFSET = 8L;     // byte offset of lead value within a slot
    private static final long SLOT_ROWID_OFFSET = 0L;    // byte offset of base rowid within a slot
    private final RecordCursorFactory base;
    private final DeferredEmitWindowRecordCursor cursor;
    private final ObjList<Function> functions;
    private final WindowFunction leadFunction;
    private final int leadFunctionColumnIndex;
    private final int lookahead;
    private final int maxPartitions;
    private final Map partitionMap;
    private final RecordSink partitionBySink;
    private final VirtualRecord partitionByRecord;
    private boolean closed;

    public DeferredEmitWindowRecordCursorFactory(
            RecordCursorFactory base,
            GenericRecordMetadata metadata,
            ObjList<Function> functions,
            VirtualRecord partitionByRecord,
            RecordSink partitionBySink,
            Map partitionMap,
            int maxPartitions
    ) {
        super(metadata);

        if (!base.recordCursorSupportsRandomAccess()) {
            throw CairoException.critical(0)
                    .put("DeferredEmitWindowRecordCursorFactory requires a base cursor that supports random access");
        }

        WindowFunction lead = null;
        int leadIdx = -1;
        int la = 0;
        for (int i = 0, n = functions.size(); i < n; i++) {
            Function f = functions.getQuick(i);
            if (f instanceof WindowFunction wf) {
                if (wf.getLookahead() > 0) {
                    if (lead != null) {
                        throw CairoException.critical(0)
                                .put("DeferredEmitWindowRecordCursorFactory supports exactly one lookahead function");
                    }
                    lead = wf;
                    leadIdx = i;
                    la = wf.getLookahead();
                } else {
                    throw CairoException.critical(0)
                            .put("DeferredEmitWindowRecordCursorFactory does not yet support mixing immediate-emit and deferred window functions");
                }
            }
        }
        if (lead == null) {
            throw CairoException.critical(0)
                    .put("DeferredEmitWindowRecordCursorFactory requires at least one lookahead function");
        }
        // ringCapacity == lookahead + 1; with a single-long filled-bit mask we cap at 63.
        if (la >= 63) {
            throw CairoException.critical(0)
                    .put("DeferredEmitWindowRecordCursorFactory lookahead must be less than 63 (filled-bit mask is one long)");
        }
        if ((partitionByRecord == null) != (partitionBySink == null) || (partitionByRecord == null) != (partitionMap == null)) {
            throw CairoException.critical(0)
                    .put("DeferredEmitWindowRecordCursorFactory: partitionByRecord, partitionBySink and partitionMap must all be null or all non-null");
        }

        this.base = base;
        this.functions = functions;
        this.leadFunction = lead;
        this.leadFunctionColumnIndex = leadIdx;
        this.lookahead = la;
        this.partitionByRecord = partitionByRecord;
        this.partitionBySink = partitionBySink;
        this.partitionMap = partitionMap;
        this.maxPartitions = maxPartitions;
        this.cursor = new DeferredEmitWindowRecordCursor();
    }

    @Override
    public boolean followedOrderByAdvice() {
        return base.followedOrderByAdvice();
    }

    @Override
    public RecordCursorFactory getBaseFactory() {
        return base;
    }

    @Override
    public RecordCursor getCursor(SqlExecutionContext executionContext) throws SqlException {
        final RecordCursor baseCursor = base.getCursor(executionContext);
        try {
            cursor.of(baseCursor, executionContext);
        } catch (Throwable t) {
            Misc.free(baseCursor);
            throw t;
        }
        return cursor;
    }

    @Override
    public int getScanDirection() {
        return base.getScanDirection();
    }

    @Override
    public boolean recordCursorSupportsRandomAccess() {
        // Deferred-emit output positions do not correspond 1:1 to base positions (rows are emitted later
        // than they are read), so downstream operators cannot seek to arbitrary output rowids.
        return false;
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.type("DeferredEmitWindow");
        sink.attr("functions").val('[').val(leadFunction).val(']');
        sink.attr("maxLookahead").val(lookahead);
        sink.child(base);
    }

    @Override
    public boolean usesCompiledFilter() {
        return base.usesCompiledFilter();
    }

    @Override
    public boolean usesIndex() {
        return base.usesIndex();
    }

    @Override
    protected void _close() {
        if (closed) {
            return;
        }
        Misc.free(base);
        Misc.free(cursor);
        Misc.free(partitionMap);
        Misc.freeObjList(functions);
        closed = true;
    }

    /**
     * Cursor implementing the deferred-emit state machine. Owns the per-cursor pending memory and the
     * partition map (when in PARTITION BY mode). Implements {@link WindowSPI} so the LEAD function can
     * address its slot via {@link #getAddress(long, int)} during back-fill and flush.
     */
    final class DeferredEmitWindowRecordCursor implements RecordCursor, WindowSPI, Reopenable {

        private final OutputRecord outputRecord = new OutputRecord();
        private final OutputRecord outputRecordB = new OutputRecord();
        // Capacity of a per-partition ring (in slots). Equals lookahead + 1; one slot beyond what the
        // function needs so we always have room to enqueue the current row immediately after emitting
        // the head.
        private final int ringCapacity;
        // For no-partition mode: deferred-emit slots for the single global partition.
        // For partition mode: drained on demand from the partition map's record cursor.
        private final long[] singlePartitionState = new long[5];
        private RecordCursor baseCursor;
        private Record baseRecordForEmit;
        private SqlExecutionCircuitBreaker circuitBreaker;
        // Drives partition iteration during flush.
        private MapRecordCursor flushMapCursor;
        // Tracks position within flushMapCursor's "current partition" between hasNext calls.
        private boolean flushPartitionOpen;
        private long flushPartitionRingCount;
        private long flushPartitionRingHead;
        private long flushPartitionSlotsOff;
        // True once base cursor exhausts; we are draining pending entries with defaultValue back-fill.
        private boolean flushPhase;
        private boolean isOpen;
        private long nextFreeSlotOffset;
        // Slot address staged by processBaseRow for emission on the next hasNext iteration. -1 = none.
        private long pendingEmitSlotOffset = -1L;
        // Native memory holding ring slots. Layout per slot: [rowid:long][leadValue:long].
        // Sized at construction for the single-partition case; grown lazily for partition mode.
        private MemoryARW pendingMem;

        DeferredEmitWindowRecordCursor() {
            this.ringCapacity = lookahead + 1;
            this.isOpen = true;
        }

        @Override
        public void calculateSize(SqlExecutionCircuitBreaker circuitBreaker, Counter counter) {
            // Output row count equals base row count: every base row produces exactly one output row,
            // whether emitted in-stream or via end-of-cursor flush.
            baseCursor.calculateSize(circuitBreaker, counter);
        }

        @Override
        public void close() {
            if (!isOpen) {
                return;
            }
            isOpen = false;
            baseCursor = Misc.free(baseCursor);
            baseRecordForEmit = null;
            pendingMem = Misc.free(pendingMem);
            flushMapCursor = null;
            // Reset functions and clear partition map so a subsequent reopen starts from a clean state.
            resetFunctions();
            if (partitionMap != null) {
                partitionMap.clear();
            }
            clearState();
        }

        @Override
        public long getAddress(long pendingSlot, int columnIndex) {
            // For Phase 5: there is exactly one LEAD function, stored at the LEAD offset within each slot.
            // columnIndex is the output column index; the cursor maps it implicitly to slot+LEAD_OFFSET.
            return pendingMem.getPageAddress(0) + pendingSlot + SLOT_LEAD_OFFSET;
        }

        @Override
        public Record getRecord() {
            return outputRecord;
        }

        @Override
        public Record getRecordAt(long recordOffset) {
            throw new UnsupportedOperationException("DeferredEmitWindowRecordCursor does not back two-pass window functions");
        }

        @Override
        public Record getRecordB() {
            return outputRecordB;
        }

        @Override
        public boolean hasNext() {
            circuitBreaker.statefulThrowExceptionIfTripped();
            while (true) {
                if (pendingEmitSlotOffset != -1L) {
                    bindOutputToSlot(outputRecord, pendingEmitSlotOffset);
                    pendingEmitSlotOffset = -1L;
                    return true;
                }
                if (!flushPhase) {
                    if (baseCursor.hasNext()) {
                        processBaseRow(baseCursor.getRecord());
                        continue;
                    }
                    flushPhase = true;
                    beginFlush();
                    continue;
                }
                // flush phase: drain remaining pending entries
                long flushSlot = nextFlushSlot();
                if (flushSlot != -1L) {
                    bindOutputToSlot(outputRecord, flushSlot);
                    return true;
                }
                return false;
            }
        }

        public void of(RecordCursor baseCursor, SqlExecutionContext executionContext) throws SqlException {
            this.baseCursor = baseCursor;
            this.baseRecordForEmit = baseCursor.getRecordB();
            this.circuitBreaker = executionContext.getCircuitBreaker();
            if (!isOpen) {
                isOpen = true;
                try {
                    reopenFunctions();
                } catch (Throwable t) {
                    close();
                    throw t;
                }
            }
            // Allocate pendingMem if not yet allocated. Use a generous initial size for partition mode
            // (it'll grow); for no-partition mode size it exactly for one ring.
            if (pendingMem == null) {
                pendingMem = Vm.getCARWInstance(
                        Math.max(16L, SLOT_BYTES * ringCapacity),
                        Integer.MAX_VALUE,
                        MemoryTag.NATIVE_WINDOW_PENDING
                );
            }
            // Reset state on every (re)open.
            if (partitionMap != null) {
                partitionMap.clear();
            }
            clearState();
            // No-partition: pre-reserve the single ring slice and seed singlePartitionState.
            if (partitionByRecord == null) {
                nextFreeSlotOffset = SLOT_BYTES * ringCapacity;
                pendingMem.jumpTo(nextFreeSlotOffset);
                singlePartitionState[0] = 0L;             // slotsByteOffset
                singlePartitionState[1] = 0L;             // ringHead
                singlePartitionState[2] = 0L;             // ringTail
                singlePartitionState[3] = 0L;             // ringCount
                singlePartitionState[4] = 0L;             // pendingFilled
            }
            flushPhase = false;
            pendingEmitSlotOffset = -1L;
            flushMapCursor = null;
            flushPartitionOpen = false;
            Function.init(functions, baseCursor, executionContext, null);
        }

        @Override
        public long preComputedStateSize() {
            return 0;
        }

        @Override
        public void recordAt(Record record, long atRowId) {
            throw new UnsupportedOperationException("DeferredEmitWindowRecordCursor does not support random access");
        }

        @Override
        public void reopen() {
            if (!isOpen) {
                isOpen = true;
            }
            if (partitionMap != null) {
                partitionMap.clear();
            }
            clearState();
            flushPhase = false;
            pendingEmitSlotOffset = -1L;
            flushMapCursor = null;
            flushPartitionOpen = false;
        }

        @Override
        public long size() {
            return baseCursor != null ? baseCursor.size() : -1;
        }

        @Override
        public void skipRows(Counter rowCount) {
            RecordCursor.skipRows(this, rowCount);
        }

        @Override
        public void toTop() {
            baseCursor.toTop();
            if (partitionMap != null) {
                partitionMap.clear();
            }
            clearState();
            if (partitionByRecord == null) {
                nextFreeSlotOffset = SLOT_BYTES * ringCapacity;
                pendingMem.jumpTo(nextFreeSlotOffset);
                singlePartitionState[0] = 0L;
                singlePartitionState[1] = 0L;
                singlePartitionState[2] = 0L;
                singlePartitionState[3] = 0L;
                singlePartitionState[4] = 0L;
            } else {
                nextFreeSlotOffset = 0L;
            }
            flushPhase = false;
            pendingEmitSlotOffset = -1L;
            flushMapCursor = null;
            flushPartitionOpen = false;
            for (int i = 0, n = functions.size(); i < n; i++) {
                functions.getQuick(i).toTop();
            }
        }

        // ------------------------------------------------------------------------------------------
        // Flush phase: iterate all partitions and drain remaining pending slots.
        // ------------------------------------------------------------------------------------------
        private void beginFlush() {
            if (partitionByRecord == null) {
                // Single global partition. nextFlushSlot will drain singlePartitionState.
                flushPartitionOpen = true;
                flushPartitionSlotsOff = singlePartitionState[0];
                flushPartitionRingHead = singlePartitionState[1];
                flushPartitionRingCount = singlePartitionState[3];
                return;
            }
            // Partition mode: iterate map records.
            flushMapCursor = partitionMap.getCursor();
            flushPartitionOpen = false;
        }

        private void bindOutputToSlot(OutputRecord rec, long slotOff) {
            final long rowid = pendingMem.getLong(slotOff + SLOT_ROWID_OFFSET);
            final long leadValue = pendingMem.getLong(slotOff + SLOT_LEAD_OFFSET);
            baseCursor.recordAt(baseRecordForEmit, rowid);
            rec.of(baseRecordForEmit, leadValue);
        }

        private void clearState() {
            nextFreeSlotOffset = 0L;
        }

        private long nextFlushSlot() {
            while (true) {
                if (!flushPartitionOpen) {
                    if (flushMapCursor == null) {
                        // No-partition single ring was already drained.
                        return -1L;
                    }
                    if (!flushMapCursor.hasNext()) {
                        return -1L;
                    }
                    MapRecord rec = flushMapCursor.getRecord();
                    flushPartitionSlotsOff = rec.getLong(0);
                    flushPartitionRingHead = rec.getLong(1);
                    flushPartitionRingCount = rec.getLong(3);
                    flushPartitionOpen = true;
                }
                if (flushPartitionRingCount == 0) {
                    flushPartitionOpen = false;
                    if (flushMapCursor == null) {
                        return -1L;
                    }
                    continue;
                }
                final long headSlot = flushPartitionSlotsOff + flushPartitionRingHead * SLOT_BYTES;
                // Fill with defaultValue (or NULL) before emitting; the function writes into the slot.
                leadFunction.streamingFlushDefault(headSlot, this);
                flushPartitionRingHead = (flushPartitionRingHead + 1) % ringCapacity;
                flushPartitionRingCount--;
                return headSlot;
            }
        }

        private void processBaseRow(Record baseRow) {
            final long[] state;
            if (partitionByRecord == null) {
                state = singlePartitionState;
            } else {
                partitionByRecord.of(baseRow);
                MapKey key = partitionMap.withKey();
                key.put(partitionByRecord, partitionBySink);
                MapValue mapValue = key.createValue();
                if (mapValue.isNew()) {
                    if (partitionMap.size() > maxPartitions) {
                        throw CairoException.critical(0)
                                .put("DeferredEmitWindowRecordCursor partition cap exceeded: maxPartitions=").put(maxPartitions);
                    }
                    final long slotsOff = allocatePartitionSlice();
                    mapValue.putLong(0, slotsOff);
                    mapValue.putLong(1, 0L); // ringHead
                    mapValue.putLong(2, 0L); // ringTail
                    mapValue.putLong(3, 0L); // ringCount
                    mapValue.putLong(4, 0L); // pendingFilled
                }
                singlePartitionState[0] = mapValue.getLong(0);
                singlePartitionState[1] = mapValue.getLong(1);
                singlePartitionState[2] = mapValue.getLong(2);
                singlePartitionState[3] = mapValue.getLong(3);
                singlePartitionState[4] = mapValue.getLong(4);
                state = singlePartitionState;
            }

            final long slotsOff = state[0];
            long ringHead = state[1];
            long ringTail = state[2];
            long ringCount = state[3];
            long pendingFilled = state[4];

            // 1) Back-fill if head's lookahead has been reached.
            if (ringCount >= lookahead && (pendingFilled & (1L << ringHead)) == 0L) {
                final long headSlot = slotsOff + ringHead * SLOT_BYTES;
                leadFunction.streamingBackfill(baseRow, headSlot, this);
                pendingFilled |= (1L << ringHead);
                // Head is now fully resolved; stage it for emission and immediately advance head so the
                // post-state in the map reflects post-emit (eliminates a second map lookup at emit time).
                pendingEmitSlotOffset = headSlot;
                pendingFilled &= ~(1L << ringHead);
                ringHead = (ringHead + 1) % ringCapacity;
                ringCount--;
            }

            // 2) Enqueue the current row at the tail.
            final long tailSlot = slotsOff + ringTail * SLOT_BYTES;
            pendingMem.putLong(tailSlot + SLOT_ROWID_OFFSET, baseRow.getRowId());
            // Defensive clear of the lead slot and the filled bit (streamingBackfill / flush will write).
            pendingMem.putLong(tailSlot + SLOT_LEAD_OFFSET, 0L);
            pendingFilled &= ~(1L << ringTail);
            ringTail = (ringTail + 1) % ringCapacity;
            ringCount++;

            // 3) Write back state.
            if (partitionByRecord == null) {
                state[1] = ringHead;
                state[2] = ringTail;
                state[3] = ringCount;
                state[4] = pendingFilled;
            } else {
                // Re-acquire the map value (createValue above invalidates references after subsequent ops).
                partitionByRecord.of(baseRow);
                MapKey key2 = partitionMap.withKey();
                key2.put(partitionByRecord, partitionBySink);
                MapValue mapValue2 = key2.findValue();
                if (mapValue2 == null) {
                    throw CairoException.critical(0)
                            .put("partition state lost between row arrivals; this is a bug");
                }
                mapValue2.putLong(0, slotsOff);
                mapValue2.putLong(1, ringHead);
                mapValue2.putLong(2, ringTail);
                mapValue2.putLong(3, ringCount);
                mapValue2.putLong(4, pendingFilled);
            }
        }

        private long allocatePartitionSlice() {
            final long sliceBytes = SLOT_BYTES * ringCapacity;
            final long off = nextFreeSlotOffset;
            nextFreeSlotOffset += sliceBytes;
            // Grow pendingMem to cover the new slice.
            pendingMem.jumpTo(nextFreeSlotOffset);
            return off;
        }

        private void reopenFunctions() {
            for (int i = 0, n = functions.size(); i < n; i++) {
                Function f = functions.getQuick(i);
                if (f instanceof Reopenable) {
                    ((Reopenable) f).reopen();
                }
            }
        }

        private void resetFunctions() {
            for (int i = 0, n = functions.size(); i < n; i++) {
                Function f = functions.getQuick(i);
                if (f instanceof WindowFunction) {
                    ((WindowFunction) f).reset();
                }
            }
        }

        /**
         * Output record that combines base columns (read via baseCursor.recordAt) with the pre-computed
         * LEAD value held as the raw 8-byte pendingMem slot. The accessor methods for the LEAD column
         * decode {@link #leadValue} according to the supported scalar types (long, double, int, date,
         * timestamp); all other column accesses delegate to the base record.
         */
        final class OutputRecord implements Record {
            private Record baseRec;
            private long leadValue;

            @Override
            public boolean getBool(int col) {
                return baseRec.getBool(col);
            }

            @Override
            public byte getByte(int col) {
                return baseRec.getByte(col);
            }

            @Override
            public char getChar(int col) {
                return baseRec.getChar(col);
            }

            @Override
            public long getDate(int col) {
                if (col == leadFunctionColumnIndex) {
                    return leadValue;
                }
                return baseRec.getDate(col);
            }

            @Override
            public double getDouble(int col) {
                if (col == leadFunctionColumnIndex) {
                    return Double.longBitsToDouble(leadValue);
                }
                return baseRec.getDouble(col);
            }

            @Override
            public float getFloat(int col) {
                return baseRec.getFloat(col);
            }

            @Override
            public int getInt(int col) {
                if (col == leadFunctionColumnIndex) {
                    return (int) leadValue;
                }
                return baseRec.getInt(col);
            }

            @Override
            public long getLong(int col) {
                if (col == leadFunctionColumnIndex) {
                    return leadValue;
                }
                return baseRec.getLong(col);
            }

            @Override
            public short getShort(int col) {
                return baseRec.getShort(col);
            }

            @Override
            public CharSequence getStrA(int col) {
                return baseRec.getStrA(col);
            }

            @Override
            public CharSequence getStrB(int col) {
                return baseRec.getStrB(col);
            }

            @Override
            public int getStrLen(int col) {
                return baseRec.getStrLen(col);
            }

            @Override
            public CharSequence getSymA(int col) {
                return baseRec.getSymA(col);
            }

            @Override
            public CharSequence getSymB(int col) {
                return baseRec.getSymB(col);
            }

            @Override
            public long getTimestamp(int col) {
                if (col == leadFunctionColumnIndex) {
                    return leadValue;
                }
                return baseRec.getTimestamp(col);
            }

            void of(Record baseRec, long leadValue) {
                this.baseRec = baseRec;
                this.leadValue = leadValue;
            }
        }
    }

    static {
        PARTITION_VALUE_TYPES = new ArrayColumnTypes();
        PARTITION_VALUE_TYPES.add(ColumnType.LONG); // [0] slotsByteOffset
        PARTITION_VALUE_TYPES.add(ColumnType.LONG); // [1] ringHead
        PARTITION_VALUE_TYPES.add(ColumnType.LONG); // [2] ringTail
        PARTITION_VALUE_TYPES.add(ColumnType.LONG); // [3] ringCount
        PARTITION_VALUE_TYPES.add(ColumnType.LONG); // [4] pendingFilled
    }
}
