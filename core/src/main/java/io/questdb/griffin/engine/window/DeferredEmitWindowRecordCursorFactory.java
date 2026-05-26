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
import io.questdb.std.IntList;
import io.questdb.std.LongList;
import io.questdb.std.ObjList;

/**
 * Streaming window factory that supports mixed deferred-emit and immediate-emit window functions
 * within a single query — Phase 6 generalisation of the original Phase 2 single-LEAD cursor.
 * <p>
 * Each pending row gets a fixed-width native slot laid out as
 * <pre>
 *   [rowid:8][value_0:8][value_1:8]...[value_n-1:8]
 * </pre>
 * where {@code value_i} corresponds to the i-th window function in column order. LAG-style
 * (lookahead = 0) functions write their value at processBaseRow time via {@link Function#pass1}
 * which delegates to {@link WindowSPI#getAddress} on this cursor; LEAD-style (lookahead &gt; 0)
 * functions defer via {@link WindowFunction#streamingBackfill} and have a one-bit-per-slot pending
 * marker. The slot is emittable when every LEAD function's bit is filled.
 * <p>
 * Phase 6 constraints enforced at construction:
 * <ul>
 *   <li>Every window function's value must fit in 8 bytes (Long, Double, Date, Timestamp, Int).</li>
 *   <li>At least one window function must have positive lookahead (otherwise the planner uses
 *       {@link WindowRecordCursorFactory} directly).</li>
 *   <li>{@code ringCapacity * leadCount <= 64} so the per-slot LEAD-pending bits fit in a single
 *       {@code long}. Equivalently, {@code (maxLookahead + 1) * leadCount <= 64}.</li>
 *   <li>Base cursor must support random access (rowids are stored per pending entry; base columns
 *       are looked up via {@link RecordCursor#recordAt(Record, long)} at emission time).</li>
 * </ul>
 * Optionally supports {@code PARTITION BY} via the {@link Map}/{@link VirtualRecord}/{@link RecordSink}
 * trio. Per-partition state is stored in {@link #PARTITION_VALUE_TYPES} (5 longs:
 * slotsByteOffset, ringHead, ringTail, ringCount, pendingFilled). The cursor enforces the runtime
 * partition cardinality cap from {@code cairo.sql.window.streaming.max.partitions}.
 */
public class DeferredEmitWindowRecordCursorFactory extends AbstractRecordCursorFactory {

    public static final ArrayColumnTypes PARTITION_VALUE_TYPES;
    private static final int FUNC_VALUE_BYTES = 8;       // each window function's value is 8 bytes
    private static final int ROWID_OFFSET = 0;
    private static final int ROWID_BYTES = 8;
    private final RecordCursorFactory base;
    // For each output column index: the slot byte offset of that column's window value, or -1 if
    // the column is not a window function.
    private final int[] columnToSlotOffset;
    private final DeferredEmitWindowRecordCursor cursor;
    private final ObjList<Function> functions;
    private final int[] lagColumnIndices;
    // LAG (immediate-emit) functions in column order.
    private final ObjList<WindowFunction> lagFunctions;
    private final int leadCount;
    private final int[] leadColumnIndices;
    // LEAD (deferred-emit) functions in column order. leadOffsets[i] is the lookahead of leadFunctions[i].
    private final ObjList<WindowFunction> leadFunctions;
    private final long[] leadOffsets;
    private final int maxLookahead;
    private final int maxPartitions;
    private final Map partitionMap;
    private final RecordSink partitionBySink;
    private final VirtualRecord partitionByRecord;
    // Mask of leadCount bits, representing one slot's LEAD pending bits.
    private final long perSlotLeadMask;
    private final int ringCapacity;
    // Per-slot bytes = ROWID_BYTES + FUNC_VALUE_BYTES * windowFunctions.size().
    private final int slotBytes;
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

        // Inventory all window functions; bucket into LAG (lookahead=0) and LEAD (lookahead>0).
        // Each function's value occupies 8 bytes in the slot. The function's column index is its
        // position i in the functions list (consistent with how SqlCodeGenerator wires
        // setColumnIndex).
        final int columnCount = metadata.getColumnCount();
        final ObjList<WindowFunction> lagFns = new ObjList<>();
        final ObjList<WindowFunction> leadFns = new ObjList<>();
        final int[] colToSlot = new int[columnCount];
        for (int i = 0; i < columnCount; i++) {
            colToSlot[i] = -1;
        }
        // We need leadColumnIndices in the same order as leadFns; build them in lock-step.
        // Same for lagColumnIndices. Use primitive list types so we don't autobox into Integer/Long.
        final IntList lagColIdxTmp = new IntList();
        final IntList leadColIdxTmp = new IntList();
        final LongList leadOffsetsTmp = new LongList();
        int windowFnIndex = 0;
        int maxLA = 0;
        for (int i = 0, n = functions.size(); i < n; i++) {
            Function f = functions.getQuick(i);
            if (f instanceof WindowFunction wf) {
                int t = wf.getType();
                if (!isFixed8ByteType(t)) {
                    throw CairoException.critical(0)
                            .put("DeferredEmitWindowRecordCursorFactory cannot stream window function of type ")
                            .put(ColumnType.nameOf(t));
                }
                colToSlot[i] = ROWID_BYTES + windowFnIndex * FUNC_VALUE_BYTES;
                windowFnIndex++;
                int la = wf.getLookahead();
                if (la > 0) {
                    leadFns.add(wf);
                    leadColIdxTmp.add(i);
                    leadOffsetsTmp.add(la);
                    if (la > maxLA) {
                        maxLA = la;
                    }
                } else {
                    lagFns.add(wf);
                    lagColIdxTmp.add(i);
                }
            }
        }
        if (leadFns.size() == 0) {
            throw CairoException.critical(0)
                    .put("DeferredEmitWindowRecordCursorFactory requires at least one positive-lookahead window function");
        }

        final int ringCap = maxLA + 1;
        final int lCount = leadFns.size();
        if ((long) ringCap * lCount > 64) {
            throw CairoException.critical(0)
                    .put("DeferredEmitWindowRecordCursorFactory: (lookahead+1)*leadCount must be <= 64 (got ")
                    .put(ringCap).put('x').put(lCount).put(')');
        }
        if ((partitionByRecord == null) != (partitionBySink == null) || (partitionByRecord == null) != (partitionMap == null)) {
            throw CairoException.critical(0)
                    .put("DeferredEmitWindowRecordCursorFactory: partitionByRecord, partitionBySink and partitionMap must all be null or all non-null");
        }

        this.base = base;
        this.functions = functions;
        this.lagFunctions = lagFns;
        this.leadFunctions = leadFns;
        this.maxLookahead = maxLA;
        this.ringCapacity = ringCap;
        this.leadCount = lCount;
        this.perSlotLeadMask = (1L << lCount) - 1;
        this.slotBytes = ROWID_BYTES + (lagFns.size() + leadFns.size()) * FUNC_VALUE_BYTES;
        this.columnToSlotOffset = colToSlot;

        this.leadOffsets = new long[lCount];
        this.leadColumnIndices = new int[lCount];
        for (int i = 0; i < lCount; i++) {
            this.leadOffsets[i] = leadOffsetsTmp.getQuick(i);
            this.leadColumnIndices[i] = leadColIdxTmp.getQuick(i);
        }
        this.lagColumnIndices = new int[lagFns.size()];
        for (int i = 0, n = lagFns.size(); i < n; i++) {
            this.lagColumnIndices[i] = lagColIdxTmp.getQuick(i);
        }
        // Release the temp lists to GC; we've copied into primitive arrays.

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
        return false;
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.type("DeferredEmitWindow");
        // Emit LAG functions first, then LEAD functions, in column order within each group.
        sink.attr("functions").val('[');
        boolean first = true;
        for (int i = 0, n = lagFunctions.size(); i < n; i++) {
            if (!first) {
                sink.val(',');
            }
            sink.val(lagFunctions.getQuick(i));
            first = false;
        }
        for (int i = 0, n = leadFunctions.size(); i < n; i++) {
            if (!first) {
                sink.val(',');
            }
            sink.val(leadFunctions.getQuick(i));
            first = false;
        }
        sink.val(']');
        sink.attr("maxLookahead").val(maxLookahead);
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

    private static boolean isFixed8ByteType(int type) {
        // Defensive type check on the cursor. The actual dispatch in SqlCodeGenerator is more
        // restrictive (it also checks the function's getPassCount() and getLookahead() values).
        // Only types whose LEAD factory has a Streaming variant can reach the cursor; the others
        // would have failed the isFastPath gate. Keeping the list in sync with the dispatch site.
        final int tag = ColumnType.tagOf(type);
        return tag == ColumnType.LONG
                || tag == ColumnType.DOUBLE
                || tag == ColumnType.DATE
                || tag == ColumnType.TIMESTAMP
                || tag == ColumnType.INT
                || tag == ColumnType.FLOAT;
    }

    /**
     * Cursor implementing the deferred-emit state machine. Owns the per-cursor pending memory and
     * the partition map (when in PARTITION BY mode). Implements {@link WindowSPI} so window
     * functions can address their slot via {@link #getAddress(long, int)}.
     */
    final class DeferredEmitWindowRecordCursor implements RecordCursor, WindowSPI, Reopenable {

        private final OutputRecord outputRecord = new OutputRecord();
        private final OutputRecord outputRecordB = new OutputRecord();
        // For no-partition mode this is the only partition state; for partition mode it's a scratch
        // copy holding the looked-up Map value during processBaseRow.
        private final long[] singlePartitionState = new long[5];
        private RecordCursor baseCursor;
        private Record baseRecordForEmit;
        private SqlExecutionCircuitBreaker circuitBreaker;
        private MapRecordCursor flushMapCursor;
        private long flushPartitionFilled;
        private boolean flushPartitionOpen;
        private long flushPartitionRingCount;
        private long flushPartitionRingHead;
        private long flushPartitionSlotsOff;
        private boolean flushPhase;
        private boolean isOpen;
        private long nextFreeSlotOffset;
        private long pendingEmitSlotOffset = -1L;
        private MemoryARW pendingMem;

        DeferredEmitWindowRecordCursor() {
            this.isOpen = true;
        }

        @Override
        public void calculateSize(SqlExecutionCircuitBreaker circuitBreaker, Counter counter) {
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
            resetFunctions();
            if (partitionMap != null) {
                partitionMap.clear();
            }
            clearState();
        }

        @Override
        public long getAddress(long pendingSlot, int columnIndex) {
            // Map output column index to slot offset. Called by window functions via pass1 (LAG)
            // and streamingBackfill (LEAD). columnIndex is the function's setColumnIndex value.
            return pendingMem.getPageAddress(0) + pendingSlot + columnToSlotOffset[columnIndex];
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
            if (pendingMem == null) {
                pendingMem = Vm.getCARWInstance(
                        Math.max(16L, (long) slotBytes * ringCapacity),
                        Integer.MAX_VALUE,
                        MemoryTag.NATIVE_WINDOW_PENDING
                );
            }
            if (partitionMap != null) {
                partitionMap.clear();
            }
            clearState();
            if (partitionByRecord == null) {
                nextFreeSlotOffset = (long) slotBytes * ringCapacity;
                pendingMem.jumpTo(nextFreeSlotOffset);
                singlePartitionState[0] = 0L;
                singlePartitionState[1] = 0L;
                singlePartitionState[2] = 0L;
                singlePartitionState[3] = 0L;
                singlePartitionState[4] = 0L;
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
                nextFreeSlotOffset = (long) slotBytes * ringCapacity;
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

        private long allocatePartitionSlice() {
            final long sliceBytes = (long) slotBytes * ringCapacity;
            final long off = nextFreeSlotOffset;
            nextFreeSlotOffset += sliceBytes;
            pendingMem.jumpTo(nextFreeSlotOffset);
            return off;
        }

        private void beginFlush() {
            if (partitionByRecord == null) {
                flushPartitionOpen = true;
                flushPartitionSlotsOff = singlePartitionState[0];
                flushPartitionRingHead = singlePartitionState[1];
                flushPartitionRingCount = singlePartitionState[3];
                flushPartitionFilled = singlePartitionState[4];
                return;
            }
            flushMapCursor = partitionMap.getCursor();
            flushPartitionOpen = false;
        }

        private void bindOutputToSlot(OutputRecord rec, long slotOff) {
            final long rowid = pendingMem.getLong(slotOff + ROWID_OFFSET);
            baseCursor.recordAt(baseRecordForEmit, rowid);
            rec.of(baseRecordForEmit, slotOff);
        }

        private void clearState() {
            nextFreeSlotOffset = 0L;
        }

        private long nextFlushSlot() {
            while (true) {
                if (!flushPartitionOpen) {
                    if (flushMapCursor == null) {
                        return -1L;
                    }
                    if (!flushMapCursor.hasNext()) {
                        return -1L;
                    }
                    MapRecord rec = flushMapCursor.getRecord();
                    flushPartitionSlotsOff = rec.getLong(0);
                    flushPartitionRingHead = rec.getLong(1);
                    flushPartitionRingCount = rec.getLong(3);
                    flushPartitionFilled = rec.getLong(4);
                    flushPartitionOpen = true;
                }
                if (flushPartitionRingCount == 0) {
                    flushPartitionOpen = false;
                    if (flushMapCursor == null) {
                        return -1L;
                    }
                    continue;
                }
                final long headSlot = flushPartitionSlotsOff + flushPartitionRingHead * slotBytes;
                // Fill UNFILLED LEAD slots with defaultValue. The pendingFilled bitmask tracks
                // which (slot, lead) pairs were already backfilled during processBaseRow — those
                // must not be overwritten. LAG slots were already filled at enqueue and have no
                // pending bits.
                final long headSlotPendingShift = flushPartitionRingHead * leadCount;
                for (int i = 0; i < leadCount; i++) {
                    long bit = 1L << (headSlotPendingShift + i);
                    if ((flushPartitionFilled & bit) == 0L) {
                        leadFunctions.getQuick(i).streamingFlushDefault(headSlot, this);
                    }
                }
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
                    mapValue.putLong(1, 0L);
                    mapValue.putLong(2, 0L);
                    mapValue.putLong(3, 0L);
                    mapValue.putLong(4, 0L);
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

            // 1) Back-fill: for each LEAD function with offset k_i, find the entry at age k_i
            //    (i.e., enqueued k_i partition-local rows ago) and write LEAD's value into its slot.
            //    The target ring index = (ringHead + (ringCount - k_i)) % ringCapacity, valid only
            //    when ringCount >= k_i.
            for (int i = 0; i < leadCount; i++) {
                long k = leadOffsets[i];
                if (ringCount >= k) {
                    long targetRingIdx = (ringHead + (ringCount - k)) % ringCapacity;
                    long bit = 1L << (targetRingIdx * leadCount + i);
                    if ((pendingFilled & bit) == 0L) {
                        long targetSlotOff = slotsOff + targetRingIdx * slotBytes;
                        leadFunctions.getQuick(i).streamingBackfill(baseRow, targetSlotOff, this);
                        pendingFilled |= bit;
                    }
                }
            }

            // 2) If head is fully resolved (all leadCount bits set in its slot mask), stage it for
            //    emission and advance head. Only one head emit per processBaseRow; subsequent
            //    backfills wait for the next row arrival.
            long headSlotMask = perSlotLeadMask << (ringHead * leadCount);
            if (ringCount > 0 && (pendingFilled & headSlotMask) == headSlotMask) {
                pendingEmitSlotOffset = slotsOff + ringHead * slotBytes;
                pendingFilled &= ~headSlotMask;
                ringHead = (ringHead + 1) % ringCapacity;
                ringCount--;
            }

            // 3) Enqueue R at ringTail. Write rowid first, then call LAG functions' pass1 to write
            //    their values into R's slot. LEAD functions are not invoked here; their values are
            //    deferred to back-fill / flush.
            final long newSlot = slotsOff + ringTail * slotBytes;
            pendingMem.putLong(newSlot + ROWID_OFFSET, baseRow.getRowId());
            // Clear LEAD pending bits for the new slot (defensive — should already be 0 from prior
            // emit or initial state).
            long newSlotMask = perSlotLeadMask << (ringTail * leadCount);
            pendingFilled &= ~newSlotMask;
            // LAG functions write their values directly to the new slot.
            for (int i = 0, n = lagFunctions.size(); i < n; i++) {
                lagFunctions.getQuick(i).pass1(baseRow, newSlot, this);
            }
            ringTail = (ringTail + 1) % ringCapacity;
            ringCount++;

            // 4) Persist state.
            if (partitionByRecord == null) {
                state[1] = ringHead;
                state[2] = ringTail;
                state[3] = ringCount;
                state[4] = pendingFilled;
            } else {
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
         * Output record dispatching column accesses to either the slot (for window-function columns)
         * or the base record (for everything else). The slot holds 8-byte raw values; type-specific
         * decode happens in the accessor based on which getX method the caller invoked.
         */
        final class OutputRecord implements Record {
            private Record baseRec;
            private long slotOff;

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
                int off = columnToSlotOffset[col];
                if (off != -1) {
                    return pendingMem.getLong(slotOff + off);
                }
                return baseRec.getDate(col);
            }

            @Override
            public double getDouble(int col) {
                int off = columnToSlotOffset[col];
                if (off != -1) {
                    return Double.longBitsToDouble(pendingMem.getLong(slotOff + off));
                }
                return baseRec.getDouble(col);
            }

            @Override
            public float getFloat(int col) {
                int off = columnToSlotOffset[col];
                if (off != -1) {
                    return Float.intBitsToFloat((int) pendingMem.getLong(slotOff + off));
                }
                return baseRec.getFloat(col);
            }

            @Override
            public int getInt(int col) {
                int off = columnToSlotOffset[col];
                if (off != -1) {
                    return (int) pendingMem.getLong(slotOff + off);
                }
                return baseRec.getInt(col);
            }

            @Override
            public long getLong(int col) {
                int off = columnToSlotOffset[col];
                if (off != -1) {
                    return pendingMem.getLong(slotOff + off);
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
                int off = columnToSlotOffset[col];
                if (off != -1) {
                    return pendingMem.getLong(slotOff + off);
                }
                return baseRec.getTimestamp(col);
            }

            void of(Record baseRec, long slotOff) {
                this.baseRec = baseRec;
                this.slotOff = slotOff;
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
