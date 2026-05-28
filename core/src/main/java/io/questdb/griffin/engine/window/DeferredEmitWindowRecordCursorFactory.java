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
import io.questdb.cairo.arr.ArrayView;
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
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.cairo.sql.VirtualRecord;
import io.questdb.cairo.sql.WindowSPI;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryARW;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.BinarySequence;
import io.questdb.std.Decimal128;
import io.questdb.std.Decimal256;
import io.questdb.std.Interval;
import io.questdb.std.Long256;
import io.questdb.std.LongList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.Unsafe;
import io.questdb.std.str.CharSink;
import io.questdb.std.str.Utf8Sequence;

import java.util.Arrays;

/**
 * Streaming window factory that supports mixed deferred-emit and immediate-emit window functions
 * within a single query — Phase 6 generalisation of the original Phase 2 single-LEAD cursor.
 * <p>
 * Each pending row gets a fixed-width native slot laid out as
 * <pre>
 *   [rowid:8][value_0:8][value_1:8]...[value_n-1:8]
 * </pre>
 * where {@code value_i} corresponds to the i-th window function in column order. LAG-style
 * (lookahead = 0) functions write their value at processBaseRow time via {@link WindowFunction#pass1}
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
    // True when leadCount == 1. The hot paths branch on this to skip the per-slot bit-mask
    // machinery (perSlotLeadMask, targetRingIdx * leadCount, etc.) since each slot's mask is a
    // single bit at position equal to the slot index.
    private final boolean isSingleLead;
    // LAG (immediate-emit) functions in column order.
    private final ObjList<WindowFunction> lagFunctions;
    private final int leadCount;
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
    // Set when isSingleLead. Cached references for the single-LEAD fast path to avoid the
    // ObjList.getQuick(0) and array load on every backfill / flush.
    private final WindowFunction soleLeadFunction;
    private final long soleLeadOffset;
    private boolean isClosed;

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

        // The caller (SqlCodeGenerator.generateSelectWindow) is responsible for releasing base,
        // functions, and partitionMap if any validation below throws. partitionByRecord is shared
        // with the lookahead window function and must not be freed on construction failure.
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
        Arrays.fill(colToSlot, -1);
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
                    leadOffsetsTmp.add(la);
                    if (la > maxLA) {
                        maxLA = la;
                    }
                } else {
                    lagFns.add(wf);
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
        this.isSingleLead = lCount == 1;
        this.perSlotLeadMask = (1L << lCount) - 1;
        this.slotBytes = ROWID_BYTES + (lagFns.size() + leadFns.size()) * FUNC_VALUE_BYTES;
        this.columnToSlotOffset = colToSlot;

        this.leadOffsets = new long[lCount];
        for (int i = 0; i < lCount; i++) {
            this.leadOffsets[i] = leadOffsetsTmp.getQuick(i);
        }
        this.soleLeadFunction = isSingleLead ? leadFns.getQuick(0) : null;
        this.soleLeadOffset = isSingleLead ? leadOffsetsTmp.getQuick(0) : 0L;

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
        // cursor.of() assigns baseCursor before doing any work that can throw, so any failure
        // mid-init leaves the cursor owning the reference; _close() -> cursor.close() handles
        // cleanup via the normal path.
        cursor.of(base.getCursor(executionContext), executionContext);
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
        if (isClosed) {
            return;
        }
        Misc.free(base);
        Misc.free(cursor);
        Misc.free(partitionMap);
        // partitionByRecord shares its underlying Function list with the lookahead window function's
        // own partitionByRecord. VirtualRecord.close() nulls each list entry; the lookahead function's
        // close() runs next (via freeObjList(functions)) and iterates the now-null list as a no-op.
        Misc.free(partitionByRecord);
        Misc.freeObjList(functions);
        isClosed = true;
    }

    private static boolean isFixed8ByteType(int type) {
        // Defensive type check on the cursor. The actual dispatch in SqlCodeGenerator is more
        // restrictive (it also checks the function's getPassCount() and getLookahead() values).
        // Only types whose LEAD factory has a Streaming variant can reach the cursor.
        // INT and FLOAT widen to LONG and DOUBLE at parse time (no LeadInt / LeadFloat factories
        // exist), so the function's getType() never reports those tags — the LONG and DOUBLE
        // entries cover them.
        final int tag = ColumnType.tagOf(type);
        return tag == ColumnType.LONG
                || tag == ColumnType.DOUBLE
                || tag == ColumnType.DATE
                || tag == ColumnType.TIMESTAMP;
    }

    /**
     * Cursor implementing the deferred-emit state machine. Owns the per-cursor pending memory and
     * the partition map (when in PARTITION BY mode). Implements {@link WindowSPI} so window
     * functions can address their slot via {@link #getAddress(long, int)}.
     */
    final class DeferredEmitWindowRecordCursor implements RecordCursor, WindowSPI, Reopenable {

        private final OutputRecord outputRecord = new OutputRecord();
        // For no-partition mode this is the only partition state; for partition mode it's a scratch
        // copy holding the looked-up Map value during processBaseRow.
        private final long[] singlePartitionState = new long[5];
        private RecordCursor baseCursor;
        private Record baseRecordForEmit;
        private SqlExecutionCircuitBreaker circuitBreaker;
        private MapRecordCursor flushMapCursor;
        private long flushPartitionFilled;
        private long flushPartitionRingCount;
        private long flushPartitionRingHead;
        private long flushPartitionSlotsOff;
        private boolean isFlushPartitionOpen;
        private boolean isFlushPhase;
        private boolean isOpen;
        private long nextFreeSlotOffset;
        // Cached pendingMem.getPageAddress(0) so per-row getAddress() avoids an interface dispatch.
        // Refreshed wherever pendingMem may have been re-allocated or extended (of(), allocatePartitionSlice()).
        private long pendingBaseAddr;
        private long pendingEmitSlotOffset = -1L;
        private MemoryARW pendingMem;

        DeferredEmitWindowRecordCursor() {
            this.isOpen = true;
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
            pendingBaseAddr = 0;
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
            return pendingBaseAddr + pendingSlot + columnToSlotOffset[columnIndex];
        }

        @Override
        public Record getRecord() {
            return outputRecord;
        }

        @Override
        public SymbolTable getSymbolTable(int columnIndex) {
            return baseCursor.getSymbolTable(columnIndex);
        }

        @Override
        public Record getRecordAt(long recordOffset) {
            throw new UnsupportedOperationException("DeferredEmitWindowRecordCursor does not back two-pass window functions");
        }

        @Override
        public Record getRecordB() {
            throw new UnsupportedOperationException();
        }

        @Override
        public SymbolTable newSymbolTable(int columnIndex) {
            return baseCursor.newSymbolTable(columnIndex);
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
                if (!isFlushPhase) {
                    if (baseCursor.hasNext()) {
                        processBaseRow(baseCursor.getRecord());
                        continue;
                    }
                    isFlushPhase = true;
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
            // Take ownership of baseCursor immediately so that close() handles cleanup if any
            // initialization step below throws. The caller (factory.getCursor) does not free
            // baseCursor on its own.
            this.baseCursor = baseCursor;
            this.baseRecordForEmit = baseCursor.getRecordB();
            this.circuitBreaker = executionContext.getCircuitBreaker();
            if (!isOpen) {
                isOpen = true;
                reopenFunctions();
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
            resetSinglePartitionStateIfNonPartitioned();
            isFlushPhase = false;
            pendingEmitSlotOffset = -1L;
            flushMapCursor = null;
            isFlushPartitionOpen = false;
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
            // Match the partition-state reset done by of() and toTop(). pendingMem may be null at
            // this point (close freed it), so cannot jumpTo() here; the of() call that follows
            // re-allocates pendingMem and jumps to the correct offset before any reads.
            if (partitionByRecord == null) {
                nextFreeSlotOffset = (long) slotBytes * ringCapacity;
                Arrays.fill(singlePartitionState, 0L);
            }
            isFlushPhase = false;
            pendingEmitSlotOffset = -1L;
            flushMapCursor = null;
            isFlushPartitionOpen = false;
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
            resetSinglePartitionStateIfNonPartitioned();
            isFlushPhase = false;
            pendingEmitSlotOffset = -1L;
            flushMapCursor = null;
            isFlushPartitionOpen = false;
            for (int i = 0, n = functions.size(); i < n; i++) {
                functions.getQuick(i).toTop();
            }
        }

        private long allocatePartitionSlice() {
            final long sliceBytes = (long) slotBytes * ringCapacity;
            final long off = nextFreeSlotOffset;
            nextFreeSlotOffset += sliceBytes;
            pendingMem.jumpTo(nextFreeSlotOffset);
            // jumpTo may have triggered a realloc moving the contiguous region; refresh the cache.
            pendingBaseAddr = pendingMem.getPageAddress(0);
            return off;
        }

        private void beginFlush() {
            if (partitionByRecord == null) {
                isFlushPartitionOpen = true;
                flushPartitionSlotsOff = singlePartitionState[0];
                flushPartitionRingHead = singlePartitionState[1];
                flushPartitionRingCount = singlePartitionState[3];
                flushPartitionFilled = singlePartitionState[4];
                return;
            }
            flushMapCursor = partitionMap.getCursor();
            isFlushPartitionOpen = false;
        }

        private void bindOutputToSlot(OutputRecord rec, long slotOff) {
            final long rowid = Unsafe.getUnsafe().getLong(pendingBaseAddr + slotOff + ROWID_OFFSET);
            baseCursor.recordAt(baseRecordForEmit, rowid);
            rec.of(baseRecordForEmit, slotOff);
        }

        private void clearState() {
            nextFreeSlotOffset = 0L;
        }

        private long nextFlushSlot() {
            while (true) {
                if (!isFlushPartitionOpen) {
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
                    isFlushPartitionOpen = true;
                }
                if (flushPartitionRingCount == 0) {
                    isFlushPartitionOpen = false;
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
                if (isSingleLead) {
                    long bit = 1L << flushPartitionRingHead;
                    if ((flushPartitionFilled & bit) == 0L) {
                        soleLeadFunction.streamingFlushDefault(headSlot, this);
                    }
                } else {
                    final long headSlotPendingShift = flushPartitionRingHead * leadCount;
                    for (int i = 0; i < leadCount; i++) {
                        long bit = 1L << (headSlotPendingShift + i);
                        if ((flushPartitionFilled & bit) == 0L) {
                            leadFunctions.getQuick(i).streamingFlushDefault(headSlot, this);
                        }
                    }
                }
                flushPartitionRingHead++;
                if (flushPartitionRingHead == ringCapacity) {
                    flushPartitionRingHead = 0;
                }
                flushPartitionRingCount--;
                return headSlot;
            }
        }

        private void processBaseRow(Record baseRow) {
            final MapValue mapValue;
            final long slotsOff;
            long ringHead;
            long ringTail;
            long ringCount;
            long pendingFilled;

            if (partitionByRecord == null) {
                mapValue = null;
                slotsOff = singlePartitionState[0];
                ringHead = singlePartitionState[1];
                ringTail = singlePartitionState[2];
                ringCount = singlePartitionState[3];
                pendingFilled = singlePartitionState[4];
            } else {
                partitionByRecord.of(baseRow);
                MapKey key = partitionMap.withKey();
                key.put(partitionByRecord, partitionBySink);
                mapValue = key.createValue();
                if (mapValue.isNew()) {
                    if (partitionMap.size() > maxPartitions) {
                        throw CairoException.critical(0)
                                .put("DeferredEmitWindowRecordCursor partition cap exceeded: maxPartitions=").put(maxPartitions);
                    }
                    slotsOff = allocatePartitionSlice();
                    mapValue.putLong(0, slotsOff);
                    mapValue.putLong(1, 0L);
                    mapValue.putLong(2, 0L);
                    mapValue.putLong(3, 0L);
                    mapValue.putLong(4, 0L);
                    ringHead = 0L;
                    ringTail = 0L;
                    ringCount = 0L;
                    pendingFilled = 0L;
                } else {
                    slotsOff = mapValue.getLong(0);
                    ringHead = mapValue.getLong(1);
                    ringTail = mapValue.getLong(2);
                    ringCount = mapValue.getLong(3);
                    pendingFilled = mapValue.getLong(4);
                }
            }

            final long mapValueAddr = mapValue != null ? mapValue.getAddress(0) : 0;

            // 1) Back-fill: for each LEAD function with offset k_i, find the entry at age k_i
            //    (i.e., enqueued k_i partition-local rows ago) and write LEAD's value into its slot.
            //    The target ring index = (ringHead + (ringCount - k_i)) % ringCapacity, valid only
            //    when ringCount >= k_i. The sum is bounded by 2*ringCapacity-2, so a single
            //    conditional subtract is enough.
            if (isSingleLead) {
                long k = soleLeadOffset;
                if (ringCount >= k) {
                    long targetRingIdx = ringHead + (ringCount - k);
                    if (targetRingIdx >= ringCapacity) {
                        targetRingIdx -= ringCapacity;
                    }
                    long bit = 1L << targetRingIdx;
                    if ((pendingFilled & bit) == 0L) {
                        long targetSlotOff = slotsOff + targetRingIdx * slotBytes;
                        soleLeadFunction.streamingBackfill(baseRow, targetSlotOff, this);
                        pendingFilled |= bit;
                    }
                }
            } else {
                for (int i = 0; i < leadCount; i++) {
                    long k = leadOffsets[i];
                    if (ringCount >= k) {
                        long targetRingIdx = ringHead + (ringCount - k);
                        if (targetRingIdx >= ringCapacity) {
                            targetRingIdx -= ringCapacity;
                        }
                        long bit = 1L << (targetRingIdx * leadCount + i);
                        if ((pendingFilled & bit) == 0L) {
                            long targetSlotOff = slotsOff + targetRingIdx * slotBytes;
                            leadFunctions.getQuick(i).streamingBackfill(baseRow, targetSlotOff, this);
                            pendingFilled |= bit;
                        }
                    }
                }
            }

            // 2) If head is fully resolved (all leadCount bits set in its slot mask), stage it for
            //    emission and advance head. Only one head emit per processBaseRow; subsequent
            //    backfills wait for the next row arrival.
            long headSlotMask = isSingleLead ? 1L << ringHead : perSlotLeadMask << (ringHead * leadCount);
            if (ringCount > 0 && (pendingFilled & headSlotMask) == headSlotMask) {
                pendingEmitSlotOffset = slotsOff + ringHead * slotBytes;
                pendingFilled &= ~headSlotMask;
                ringHead++;
                if (ringHead == ringCapacity) {
                    ringHead = 0;
                }
                ringCount--;
            }

            // 3) Enqueue R at ringTail. Write rowid first, then call LAG functions' pass1 to write
            //    their values into R's slot. LEAD functions are not invoked here; their values are
            //    deferred to back-fill / flush.
            final long newSlot = slotsOff + ringTail * slotBytes;
            Unsafe.getUnsafe().putLong(pendingBaseAddr + newSlot + ROWID_OFFSET, baseRow.getRowId());
            // Clear LEAD pending bits for the new slot (defensive — should already be 0 from prior
            // emit or initial state).
            long newSlotMask = isSingleLead ? 1L << ringTail : perSlotLeadMask << (ringTail * leadCount);
            pendingFilled &= ~newSlotMask;
            // LAG functions write their values directly to the new slot.
            for (int i = 0, n = lagFunctions.size(); i < n; i++) {
                lagFunctions.getQuick(i).pass1(baseRow, newSlot, this);
            }
            ringTail++;
            if (ringTail == ringCapacity) {
                ringTail = 0;
            }
            ringCount++;

            // 4) Persist state. The flyweight assertion proves mapValue still points at the same value
            //    tuple captured at mapValueAddr; PARTITION_VALUE_TYPES is five LONG columns laid out
            //    8 bytes apart, so we can write directly via Unsafe without going through the
            //    flyweight's per-call valueOffsets lookup.
            if (partitionByRecord == null) {
                singlePartitionState[1] = ringHead;
                singlePartitionState[2] = ringTail;
                singlePartitionState[3] = ringCount;
                singlePartitionState[4] = pendingFilled;
            } else {
                assert mapValue.getAddress(0) == mapValueAddr : "partitionMap flyweight invalidated between read and write-back";
                Unsafe.getUnsafe().putLong(mapValueAddr + Long.BYTES, ringHead);
                Unsafe.getUnsafe().putLong(mapValueAddr + 2 * Long.BYTES, ringTail);
                Unsafe.getUnsafe().putLong(mapValueAddr + 3 * Long.BYTES, ringCount);
                Unsafe.getUnsafe().putLong(mapValueAddr + 4 * Long.BYTES, pendingFilled);
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

        private void resetSinglePartitionStateIfNonPartitioned() {
            if (partitionByRecord == null) {
                nextFreeSlotOffset = (long) slotBytes * ringCapacity;
                pendingMem.jumpTo(nextFreeSlotOffset);
                // jumpTo may have triggered the first allocation; refresh the cached base address.
                pendingBaseAddr = pendingMem.getPageAddress(0);
                singlePartitionState[0] = 0L;
                singlePartitionState[1] = 0L;
                singlePartitionState[2] = 0L;
                singlePartitionState[3] = 0L;
                singlePartitionState[4] = 0L;
            } else {
                nextFreeSlotOffset = 0L;
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
         * <p>
         * The window-function type allowlist enforced by the planner restricts slot-dispatched
         * columns to LONG / DOUBLE / DATE / TIMESTAMP (with INT / FLOAT widening to LONG / DOUBLE at
         * parse time). All other Record getters can therefore delegate straight to the base record
         * for non-window columns; the {@code columnToSlotOffset[col] == -1} check is unnecessary
         * for those types.
         */
        final class OutputRecord implements Record {
            private Record baseRec;
            private long slotOff;

            @Override
            public ArrayView getArray(int col, int columnType) {
                return baseRec.getArray(col, columnType);
            }

            @Override
            public BinarySequence getBin(int col) {
                return baseRec.getBin(col);
            }

            @Override
            public long getBinLen(int col) {
                return baseRec.getBinLen(col);
            }

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
                    return Unsafe.getUnsafe().getLong(pendingBaseAddr + slotOff + off);
                }
                return baseRec.getDate(col);
            }

            @Override
            public void getDecimal128(int col, Decimal128 sink) {
                baseRec.getDecimal128(col, sink);
            }

            @Override
            public short getDecimal16(int col) {
                return baseRec.getDecimal16(col);
            }

            @Override
            public void getDecimal256(int col, Decimal256 sink) {
                baseRec.getDecimal256(col, sink);
            }

            @Override
            public int getDecimal32(int col) {
                return baseRec.getDecimal32(col);
            }

            @Override
            public long getDecimal64(int col) {
                return baseRec.getDecimal64(col);
            }

            @Override
            public byte getDecimal8(int col) {
                return baseRec.getDecimal8(col);
            }

            @Override
            public double getDouble(int col) {
                int off = columnToSlotOffset[col];
                if (off != -1) {
                    return Double.longBitsToDouble(Unsafe.getUnsafe().getLong(pendingBaseAddr + slotOff + off));
                }
                return baseRec.getDouble(col);
            }

            @Override
            public float getFloat(int col) {
                int off = columnToSlotOffset[col];
                if (off != -1) {
                    return Float.intBitsToFloat((int) Unsafe.getUnsafe().getLong(pendingBaseAddr + slotOff + off));
                }
                return baseRec.getFloat(col);
            }

            @Override
            public byte getGeoByte(int col) {
                return baseRec.getGeoByte(col);
            }

            @Override
            public int getGeoInt(int col) {
                return baseRec.getGeoInt(col);
            }

            @Override
            public long getGeoLong(int col) {
                return baseRec.getGeoLong(col);
            }

            @Override
            public short getGeoShort(int col) {
                return baseRec.getGeoShort(col);
            }

            @Override
            public int getIPv4(int col) {
                return baseRec.getIPv4(col);
            }

            @Override
            public int getInt(int col) {
                int off = columnToSlotOffset[col];
                if (off != -1) {
                    return (int) Unsafe.getUnsafe().getLong(pendingBaseAddr + slotOff + off);
                }
                return baseRec.getInt(col);
            }

            @Override
            public Interval getInterval(int col) {
                return baseRec.getInterval(col);
            }

            @Override
            public long getLong(int col) {
                int off = columnToSlotOffset[col];
                if (off != -1) {
                    return Unsafe.getUnsafe().getLong(pendingBaseAddr + slotOff + off);
                }
                return baseRec.getLong(col);
            }

            @Override
            public long getLong128Hi(int col) {
                return baseRec.getLong128Hi(col);
            }

            @Override
            public long getLong128Lo(int col) {
                return baseRec.getLong128Lo(col);
            }

            @Override
            public void getLong256(int col, CharSink<?> sink) {
                baseRec.getLong256(col, sink);
            }

            @Override
            public Long256 getLong256A(int col) {
                return baseRec.getLong256A(col);
            }

            @Override
            public Long256 getLong256B(int col) {
                return baseRec.getLong256B(col);
            }

            @Override
            public Record getRecord(int col) {
                return baseRec.getRecord(col);
            }

            @Override
            public long getRowId() {
                return baseRec.getRowId();
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
                    return Unsafe.getUnsafe().getLong(pendingBaseAddr + slotOff + off);
                }
                return baseRec.getTimestamp(col);
            }

            @Override
            public long getUpdateRowId() {
                return baseRec.getUpdateRowId();
            }

            @Override
            public Utf8Sequence getVarcharA(int col) {
                return baseRec.getVarcharA(col);
            }

            @Override
            public Utf8Sequence getVarcharB(int col) {
                return baseRec.getVarcharB(col);
            }

            @Override
            public int getVarcharSize(int col) {
                return baseRec.getVarcharSize(col);
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
