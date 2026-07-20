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
import io.questdb.griffin.engine.functions.SymbolFunction;
import io.questdb.std.BinarySequence;
import io.questdb.std.Decimal128;
import io.questdb.std.Decimal256;
import io.questdb.std.Interval;
import io.questdb.std.Long256;
import io.questdb.std.LongList;
import io.questdb.std.MemoryTag;
import io.questdb.std.MemoryTracker;
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
 *   <li>Every window function's value must fit in 8 bytes (Long, Double, Date, Timestamp).</li>
 *   <li>At least one window function must have positive lookahead (otherwise the planner uses
 *       {@link WindowRecordCursorFactory} directly).</li>
 *   <li>{@code ringCapacity * leadCount <= 64} so the per-slot LEAD-pending bits fit in a single
 *       {@code long}. Equivalently, {@code (maxLookahead + 1) * leadCount <= 64}.</li>
 *   <li>Base cursor must support random access (rowids are stored per pending entry; base columns
 *       are looked up via {@link RecordCursor#recordAt(Record, long)} at emission time).</li>
 * </ul>
 * Optionally supports {@code PARTITION BY} via the {@link Map}/{@link VirtualRecord}/{@link RecordSink}
 * trio. Per-partition state is stored as the value layout built by {@link #buildPartitionValueTypes(int)}
 * (5 base longs: slotsByteOffset, ringHead, ringTail, ringCount, pendingFilled, plus 3 longs per LAG
 * function). The cursor enforces the runtime partition cardinality cap from
 * {@code cairo.sql.window.streaming.max.partitions}. Partitioned mode emits in partition-major
 * resolution order and does not preserve base scan order, so any query without an outer
 * {@code ORDER BY} may observe a different row order when streaming is enabled.
 * <p>
 * Memory bound: pending native memory is {@code O(partitions × (maxLookahead + Σ lag_offsets))}, not
 * {@code O(partitions × maxLookahead)} — each streaming LAG eagerly reserves an {@code offset}-sized
 * ring per partition on top of the LEAD lookahead slots. The two caps
 * {@code cairo.sql.window.streaming.max.partitions} and the per-function {@code offset} limit multiply,
 * so the worst-case reservation is their product times 8 bytes per LAG. This is not a regression over
 * the cached LAG (which caps neither offset nor partition count), but the interaction is multiplicative
 * and should be sized accordingly when raising either cap.
 */
public class DeferredEmitWindowRecordCursorFactory extends AbstractRecordCursorFactory {

    private static final int FUNC_VALUE_BYTES = 8;       // each window function's value is 8 bytes
    // Base layout for the cursor's per-partition state: 5 LONGs. Streaming LAG variants extend
    // this layout with 3 LONGs each via {@link #buildPartitionValueTypes(int)} so they can store
    // (startOffset, firstIdx, count) inline in the cursor's MapValue and skip a second hash probe.
    private static final int PARTITION_VALUE_BASE_LONGS = 5;
    private static final int PARTITION_VALUE_LONGS_PER_LAG = 3;
    // Upfront partition slices to pre-allocate when a partitioned cursor first opens. Trades a small
    // amount of overcommit (most queries materialise under this many partitions) for elimination of
    // doubling reallocs in the typical case. Capped by maxPartitions inside of() so a deliberately
    // low cap is honoured.
    private static final long PENDING_MEM_PREALLOC_PARTITIONS = 256L;
    private static final int ROWID_BYTES = 8;
    private static final int ROWID_OFFSET = 0;
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
    // Mirror of lagFunctions as a final array. Same JIT-range-analysis motivation as
    // leadFunctionsArr; the LAG dispatch loop in processBaseRow runs once per base row.
    private final WindowFunction[] lagFunctionsArr;
    private final int leadCount;
    // LEAD (deferred-emit) functions in column order. leadOffsets[i] is the lookahead of leadFunctions[i].
    private final ObjList<WindowFunction> leadFunctions;
    // Mirrors leadFunctions as a final array so HotSpot can apply array-bound range analysis to the
    // per-row backfill and flush loops below. ObjList.getQuick hides the array load from the JIT's
    // range analyzer because the size field is read indirectly.
    private final WindowFunction[] leadFunctionsArr;
    private final long[] leadOffsets;
    private final int maxLookahead;
    private final int maxPartitions;
    private final VirtualRecord partitionByRecord;
    private final RecordSink partitionBySink;
    private final Map partitionMap;
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
        // Partitioned streaming emits rows in partition-major resolution order, not in base scan
        // order, so the cursor cannot honour a designated timestamp. Strip the timestamp index from
        // the metadata before AbstractRecordCursorFactory captures it. The Sort that
        // generateOrderBy inserts when followedOrderByAdvice returns false still finds ts by column
        // name; nothing downstream that relies on a timestamp-indexed cursor would be correct here.
        super(partitionByRecord != null && metadata.getTimestampIndex() != -1
                ? GenericRecordMetadata.copyOfSansTimestamp(metadata)
                : metadata);

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
        this.lagFunctionsArr = new WindowFunction[lagFns.size()];
        for (int i = 0, n = lagFns.size(); i < n; i++) {
            this.lagFunctionsArr[i] = lagFns.getQuick(i);
        }
        this.maxLookahead = maxLA;
        this.ringCapacity = ringCap;
        this.leadCount = lCount;
        this.isSingleLead = lCount == 1;
        this.perSlotLeadMask = (1L << lCount) - 1;
        this.slotBytes = ROWID_BYTES + (lagFns.size() + leadFns.size()) * FUNC_VALUE_BYTES;
        this.columnToSlotOffset = colToSlot;

        this.leadOffsets = new long[lCount];
        this.leadFunctionsArr = new WindowFunction[lCount];
        for (int i = 0; i < lCount; i++) {
            this.leadOffsets[i] = leadOffsetsTmp.getQuick(i);
            this.leadFunctionsArr[i] = leadFns.getQuick(i);
        }
        this.soleLeadFunction = isSingleLead ? leadFns.getQuick(0) : null;
        this.soleLeadOffset = isSingleLead ? leadOffsetsTmp.getQuick(0) : 0L;

        this.partitionByRecord = partitionByRecord;
        this.partitionBySink = partitionBySink;
        this.partitionMap = partitionMap;
        // Start the map closed so its backing is allocated only after of() binds the per-query
        // MemoryTracker, then reopen()ed under it and close()d at cursor close — symmetric on the
        // per-query counter. Mirrors the lazy-open pattern in BasePartitionedWindowFunction.
        if (partitionMap != null) {
            partitionMap.close();
        }
        this.maxPartitions = maxPartitions;
        this.cursor = new DeferredEmitWindowRecordCursor();
    }

    @Override
    public boolean followedOrderByAdvice() {
        // Partitioned streaming emits rows in partition-major resolution order: backfill-driven
        // in-stream emits and the end-of-cursor flush both iterate partitions independently of
        // base scan order. Outer ORDER BY on the base timestamp would otherwise be skipped here
        // and the user would see the tail rows out of order.
        return partitionByRecord == null && base.followedOrderByAdvice();
    }

    @Override
    public RecordCursorFactory getBaseFactory() {
        return base;
    }

    @Override
    public RecordCursor getCursor(SqlExecutionContext executionContext) throws SqlException {
        // cursor.of() assigns baseCursor before doing any work that can throw, so a failure
        // mid-init leaves the cursor owning the reference. Close it here, while the per-query
        // MemoryTracker is still bound, so partial native allocations (pending ring, partition
        // map, streaming-function state) are released and no base reader stays busy. Mirrors the
        // three sibling window factories.
        final RecordCursor baseCursor = base.getCursor(executionContext);
        try {
            cursor.of(baseCursor, executionContext);
            return cursor;
        } catch (Throwable th) {
            cursor.close();
            throw th;
        }
    }

    @Override
    public int getScanDirection() {
        return partitionByRecord == null ? base.getScanDirection() : SCAN_DIRECTION_OTHER;
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
        boolean isFirst = true;
        for (int i = 0, n = lagFunctions.size(); i < n; i++) {
            if (!isFirst) {
                sink.val(',');
            }
            sink.val(lagFunctions.getQuick(i));
            isFirst = false;
        }
        for (int i = 0, n = leadFunctions.size(); i < n; i++) {
            if (!isFirst) {
                sink.val(',');
            }
            sink.val(leadFunctions.getQuick(i));
            isFirst = false;
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
        // Commit to the closed state before touching owners: close is one-shot, so a throwing
        // owner must not leave _close re-enterable. Close every owner best-effort and rethrow the
        // first failure with the rest suppressed, so one throwing owner never strands the others.
        isClosed = true;
        Throwable failure = Misc.freeBestEffort(null, base);
        failure = Misc.freeBestEffort(failure, cursor);
        failure = Misc.freeBestEffort(failure, partitionMap);
        // partitionByRecord shares its underlying Function list with the lookahead window function's
        // own partitionByRecord. VirtualRecord.close() nulls each list entry; the lookahead function's
        // close() runs next (via freeObjList(functions)) and iterates the now-null list as a no-op.
        failure = Misc.freeBestEffort(failure, partitionByRecord);
        failure = Misc.freeObjListBestEffort(failure, functions);
        CairoException.rethrowCleanupFailure(failure);
    }

    /**
     * Builds the {@link ArrayColumnTypes} layout for a streaming-dispatch partition map: the base
     * 5 LONGs followed by {@code lagCount * 3} LONGs reserved for per-LAG state tuples.
     */
    public static ArrayColumnTypes buildPartitionValueTypes(int lagCount) {
        ArrayColumnTypes types = new ArrayColumnTypes();
        final int total = PARTITION_VALUE_BASE_LONGS + lagCount * PARTITION_VALUE_LONGS_PER_LAG;
        for (int i = 0; i < total; i++) {
            types.add(ColumnType.LONG);
        }
        return types;
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
        private final long[] singlePartitionState = new long[PARTITION_VALUE_BASE_LONGS];
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
            // Start closed so the first of() binds the per-query MemoryTracker on the window
            // functions and the partition map before reopening their native backing, charging
            // every byte to the per-query counter symmetrically. Mirrors the lazy-open pattern in
            // CachedWindowRecordCursor. Without this, the first of() would skip the reopen of the
            // window functions' partition maps (closed at construction) and the first row would
            // dereference a zero-backed map.
            this.isOpen = false;
        }

        @Override
        public void close() {
            if (!isOpen) {
                return;
            }
            // Set isOpen=false up front: AbstractRecordCursorFactory makes close one-shot, so if
            // any owner's close throws we must not re-enter and must have already committed to the
            // closed state. Close every owner best-effort and rethrow the first failure with the
            // rest attached as suppressed, so one throwing owner never strands the others.
            isOpen = false;
            Throwable failure = Misc.freeBestEffort(null, baseCursor);
            baseCursor = null;
            baseRecordForEmit = null;
            failure = Misc.freeBestEffort(failure, pendingMem);
            pendingMem = null;
            pendingBaseAddr = 0;
            flushMapCursor = null;
            failure = resetFunctionsBestEffort(failure);
            if (partitionMap != null) {
                // Free (not just clear) so the per-query MemoryTracker bound in of() is decremented
                // symmetrically; of() reopen()s the backing on the next execution.
                failure = Misc.freeBestEffort(failure, partitionMap);
            }
            clearState();
            CairoException.rethrowCleanupFailure(failure);
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
        public Record getRecordAt(long recordOffset) {
            throw new UnsupportedOperationException("DeferredEmitWindowRecordCursor does not back two-pass window functions");
        }

        @Override
        public Record getRecordB() {
            throw new UnsupportedOperationException();
        }

        @Override
        public SymbolTable getSymbolTable(int columnIndex) {
            // Output metadata is in SELECT-list order with window columns interspersed; a base
            // column sitting after a window column has output index != base index. Resolve via the
            // function list (a passthrough SYMBOL column is a SymbolColumn carrying the base index),
            // matching AbstractVirtualFunctionRecordCursor. Delegating the output index to baseCursor
            // would return the wrong symbol table.
            return (SymbolTable) functions.getQuick(columnIndex);
        }

        @Override
        public boolean hasNext() {
            while (true) {
                if (pendingEmitSlotOffset != -1L) {
                    bindOutputToSlot(outputRecord, pendingEmitSlotOffset);
                    pendingEmitSlotOffset = -1L;
                    return true;
                }
                if (!isFlushPhase) {
                    // Check the breaker on every drained base row, not once per outer call: a large
                    // ingest drain (many base rows before a row becomes emittable) would otherwise
                    // delay cancellation until the next emit.
                    circuitBreaker.statefulThrowExceptionIfTripped();
                    if (baseCursor.hasNext()) {
                        processBaseRow(baseCursor.getRecord());
                        continue;
                    }
                    isFlushPhase = true;
                    beginFlush();
                    continue;
                }
                // Check the breaker on every flushed tail too, not just during ingest: after base
                // EOF the flush can emit up to (partitions * maxLookahead) rows and downstream
                // consumers (HTTP, PGWire, embedded) do not universally add per-row checks. The
                // breaker is time-throttled internally, so a per-flush-row check is cheap.
                circuitBreaker.statefulThrowExceptionIfTripped();
                long flushSlot = nextFlushSlot();
                if (flushSlot != -1L) {
                    bindOutputToSlot(outputRecord, flushSlot);
                    return true;
                }
                return false;
            }
        }

        @Override
        public SymbolTable newSymbolTable(int columnIndex) {
            // See getSymbolTable: resolve via the function list, not the base cursor's output index.
            return ((SymbolFunction) functions.getQuick(columnIndex)).newSymbolTable();
        }

        public void of(RecordCursor baseCursor, SqlExecutionContext executionContext) throws SqlException {
            // Take ownership of baseCursor immediately so that close() handles cleanup if any
            // initialization step below throws. The caller (factory.getCursor) does not free
            // baseCursor on its own. Free any previous baseCursor first, in case of() is invoked
            // twice without an intervening close().
            this.baseCursor = Misc.free(this.baseCursor);
            this.baseCursor = baseCursor;
            // Set isOpen ahead of any throw-prone calls below so close() runs the full cleanup
            // path on failure. The cursor starts closed (constructor) and close() sets it back to
            // false, so of() always reopens the state below; without isOpen=true here, close()'s
            // early-exit would leak the newly assigned baseCursor and anything reopened below.
            isOpen = true;
            // baseCursor.getRecordB() typically returns a cached B record; calling it on every
            // of() is cheap. The reference is reused across toTop() because toTop() resets ringHead
            // but does not re-call of(), so the B record we hold remains valid against the same
            // baseCursor.
            this.baseRecordForEmit = baseCursor.getRecordB();
            this.circuitBreaker = executionContext.getCircuitBreaker();
            // Bind the per-query memory tracker on the two native structures that grow with the
            // input (the partition-state map and the pending-slot ring) so their allocations count
            // against the per-query limit, not just global RSS. Mirrors WindowRecordCursorFactory
            // wiring setMemoryTracker on each cached window function's map. Both must be bound BEFORE
            // any allocation so the per-query counter is symmetric: pendingMem's prealloc jumpTo and
            // the map's reopen() both charge the bound tracker, and close() frees against it.
            // partitionMap is null in non-partitioned mode.
            final MemoryTracker memoryTracker = executionContext.getMemoryTracker();
            // Bind the per-query tracker on every window function's tracker-aware state (per-
            // partition maps and streaming rings) and reopen the state that resetFunctions() and
            // construction tore down, BEFORE the first row can reach it. This runs on every
            // execution, including the first: a window function's partition map is closed at
            // construction (BasePartitionedWindowFunction) and its streaming ring is unallocated,
            // so binding+reopening here is what makes the first processBaseRow safe and keeps
            // every function-side allocation on the per-query counter rather than global RSS.
            bindAndReopenFunctions(memoryTracker);
            if (pendingMem == null) {
                // Page size = one partition's slice. In non-partitioned mode there is exactly one
                // page; in partitioned mode each new partition extends pendingMem by one page via
                // jumpTo(), which triggers Unsafe.realloc and memcpys the existing region. To bound
                // the cumulative memcpy traffic on high-cardinality partition workloads, pre-extend
                // the region up to a budget of PENDING_MEM_PREALLOC_PARTITIONS partitions on first
                // allocation. The budget is capped at maxPartitions so a query with maxPartitions=1
                // does not allocate beyond what the cap permits.
                final long pageBytes = Math.max(16L, (long) slotBytes * ringCapacity);
                pendingMem = Vm.getCARWInstance(pageBytes, Integer.MAX_VALUE, MemoryTag.NATIVE_WINDOW_PENDING);
                pendingMem.setMemoryTracker(memoryTracker);
                if (partitionByRecord != null && maxPartitions > 1) {
                    final long prealloc = Math.min(PENDING_MEM_PREALLOC_PARTITIONS, maxPartitions) * pageBytes;
                    pendingMem.jumpTo(prealloc);
                    pendingMem.jumpTo(0);
                    // Refresh pendingBaseAddr defensively. allocatePartitionSlice refreshes it on
                    // first partition touch today, but any caller (e.g. Function.init below) that
                    // routed through getAddress() before then would dereference 0.
                    pendingBaseAddr = pendingMem.getPageAddress(0);
                }
            } else {
                pendingMem.setMemoryTracker(memoryTracker);
            }
            if (partitionMap != null) {
                // Bind first, then reopen so the backing allocates under the tracker. reopen() is a
                // no-op if the map is already open (idempotent), and close() at cursor close frees it.
                partitionMap.setMemoryTracker(memoryTracker);
                partitionMap.reopen();
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
            // Per-function state and pendingMem are (re)bound and reopened by the of() that always
            // follows reopen(); of() binds the per-query MemoryTracker first so their backing
            // allocates under it. Reopening the functions here (without a tracker) would leave that
            // allocation unaccounted, so reopen() only resets the cursor's own scalar state.
            isOpen = true;
            // partitionMap is intentionally left closed here: close() freed it and the of() that
            // always follows reopen() binds the per-query tracker before reopen()ing the backing.
            // Touching a closed map here (clear() memsets a null base) would crash.
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
                            leadFunctionsArr[i].streamingFlushDefault(headSlot, this);
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
                // Below the cap, insert freely. At the cap, refuse to insert a new key so the map
                // never exceeds maxPartitions entries (existing keys still resolve normally).
                if (partitionMap.size() < maxPartitions) {
                    mapValue = key.createValue();
                } else {
                    mapValue = key.findValue();
                    if (mapValue == null) {
                        throw CairoException.critical(0)
                                .put("DeferredEmitWindowRecordCursor partition cap exceeded: maxPartitions=").put(maxPartitions);
                    }
                }
                if (mapValue.isNew()) {
                    slotsOff = allocatePartitionSlice();
                    mapValue.putLong(0, slotsOff);
                    mapValue.putLong(1, 0L);
                    mapValue.putLong(2, 0L);
                    mapValue.putLong(3, 0L);
                    mapValue.putLong(4, 0L);
                    // Zero the per-LAG slots so streaming LAG variants see a clean (0,0,0) tuple
                    // on first touch. Map.createValue's memset covers the value region but writing
                    // explicitly makes the contract local to the LAG dispatch.
                    for (int j = 0, m = lagFunctionsArr.length * PARTITION_VALUE_LONGS_PER_LAG; j < m; j++) {
                        mapValue.putLong(PARTITION_VALUE_BASE_LONGS + j, 0L);
                    }
                    ringHead = 0L;
                    ringTail = 0L;
                    ringCount = 0L;
                    pendingFilled = 0L;
                } else {
                    // Read the partition's state tuple through direct Unsafe to skip 5 interface
                    // dispatches and 5 column-offset array indirections per existing-partition row.
                    // The flyweight invariant is the same one the write-back at step 4 relies on:
                    // nothing between here and the end of processBaseRow mutates partitionMap.
                    final long addr = mapValue.getAddress(0);
                    slotsOff = Unsafe.getUnsafe().getLong(addr);
                    ringHead = Unsafe.getUnsafe().getLong(addr + Long.BYTES);
                    ringTail = Unsafe.getUnsafe().getLong(addr + 2L * Long.BYTES);
                    ringCount = Unsafe.getUnsafe().getLong(addr + 3L * Long.BYTES);
                    pendingFilled = Unsafe.getUnsafe().getLong(addr + 4L * Long.BYTES);
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
                            leadFunctionsArr[i].streamingBackfill(baseRow, targetSlotOff, this);
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
            // LAG functions write to the new slot via streamingPass1. In partitioned mode the
            // cursor has already resolved the partition; passing each LAG its co-located per-
            // partition state address (3 LONGs at offset PARTITION_VALUE_BASE_LONGS + lagIdx*3)
            // lets the LAG skip a redundant hash probe per row. In non-partitioned mode the
            // address is 0 and streamingPass1's default falls back to plain pass1. The branch on
            // mapValueAddr is loop-invariant so the two arms are split out to avoid a compare per
            // LAG per row.
            if (mapValueAddr == 0L) {
                for (int i = 0, n = lagFunctionsArr.length; i < n; i++) {
                    lagFunctionsArr[i].streamingPass1(baseRow, newSlot, this, 0L);
                }
            } else {
                for (int i = 0, n = lagFunctionsArr.length; i < n; i++) {
                    long lagStateAddr = mapValueAddr + (long) (PARTITION_VALUE_BASE_LONGS + i * PARTITION_VALUE_LONGS_PER_LAG) * Long.BYTES;
                    lagFunctionsArr[i].streamingPass1(baseRow, newSlot, this, lagStateAddr);
                }
            }
            ringTail++;
            if (ringTail == ringCapacity) {
                ringTail = 0;
            }
            ringCount++;

            // 4) Persist state. The flyweight assertion proves mapValue still points at the same value
            //    tuple captured at mapValueAddr; the partition value layout is five LONG columns laid
            //    out 8 bytes apart, so we can write directly via Unsafe without going through the
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

        private void bindAndReopenFunctions(MemoryTracker memoryTracker) {
            for (int i = 0, n = functions.size(); i < n; i++) {
                Function f = functions.getQuick(i);
                if (f instanceof WindowFunction wf) {
                    // Bind the tracker before reopen so the function's tracker-aware state (its
                    // partition map and, for streaming variants, the lazily allocated ring)
                    // charges the per-query counter. BasePartitionedWindowFunction retains the
                    // tracker even while its map is null, so a ring created later in
                    // streamingPass1 inherits it.
                    wf.setMemoryTracker(memoryTracker);
                }
                if (f instanceof Reopenable r) {
                    r.reopen();
                }
            }
        }

        private Throwable resetFunctionsBestEffort(Throwable failure) {
            for (int i = 0, n = functions.size(); i < n; i++) {
                Function f = functions.getQuick(i);
                if (f instanceof WindowFunction wf) {
                    try {
                        wf.reset();
                    } catch (Throwable th) {
                        if (failure == null) {
                            failure = th;
                        } else if (th != failure) {
                            failure.addSuppressed(th);
                        }
                    }
                }
            }
            return failure;
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
                return functions.getQuick(col).getArray(baseRec);
            }

            @Override
            public BinarySequence getBin(int col) {
                return functions.getQuick(col).getBin(baseRec);
            }

            @Override
            public long getBinLen(int col) {
                return functions.getQuick(col).getBinLen(baseRec);
            }

            @Override
            public boolean getBool(int col) {
                return functions.getQuick(col).getBool(baseRec);
            }

            @Override
            public byte getByte(int col) {
                return functions.getQuick(col).getByte(baseRec);
            }

            @Override
            public char getChar(int col) {
                return functions.getQuick(col).getChar(baseRec);
            }

            @Override
            public long getDate(int col) {
                int off = columnToSlotOffset[col];
                if (off != -1) {
                    return Unsafe.getUnsafe().getLong(pendingBaseAddr + slotOff + off);
                }
                return functions.getQuick(col).getDate(baseRec);
            }

            @Override
            public void getDecimal128(int col, Decimal128 sink) {
                functions.getQuick(col).getDecimal128(baseRec, sink);
            }

            @Override
            public short getDecimal16(int col) {
                return functions.getQuick(col).getDecimal16(baseRec);
            }

            @Override
            public void getDecimal256(int col, Decimal256 sink) {
                functions.getQuick(col).getDecimal256(baseRec, sink);
            }

            @Override
            public int getDecimal32(int col) {
                return functions.getQuick(col).getDecimal32(baseRec);
            }

            @Override
            public long getDecimal64(int col) {
                return functions.getQuick(col).getDecimal64(baseRec);
            }

            @Override
            public byte getDecimal8(int col) {
                return functions.getQuick(col).getDecimal8(baseRec);
            }

            @Override
            public double getDouble(int col) {
                int off = columnToSlotOffset[col];
                if (off != -1) {
                    return Double.longBitsToDouble(Unsafe.getUnsafe().getLong(pendingBaseAddr + slotOff + off));
                }
                return functions.getQuick(col).getDouble(baseRec);
            }

            @Override
            public float getFloat(int col) {
                assert columnToSlotOffset[col] == -1 : "streaming window columns cannot have FLOAT type";
                return functions.getQuick(col).getFloat(baseRec);
            }

            @Override
            public byte getGeoByte(int col) {
                return functions.getQuick(col).getGeoByte(baseRec);
            }

            @Override
            public int getGeoInt(int col) {
                return functions.getQuick(col).getGeoInt(baseRec);
            }

            @Override
            public long getGeoLong(int col) {
                return functions.getQuick(col).getGeoLong(baseRec);
            }

            @Override
            public short getGeoShort(int col) {
                return functions.getQuick(col).getGeoShort(baseRec);
            }

            @Override
            public int getIPv4(int col) {
                return functions.getQuick(col).getIPv4(baseRec);
            }

            @Override
            public int getInt(int col) {
                assert columnToSlotOffset[col] == -1 : "streaming window columns cannot have INT type";
                return functions.getQuick(col).getInt(baseRec);
            }

            @Override
            public Interval getInterval(int col) {
                return functions.getQuick(col).getInterval(baseRec);
            }

            @Override
            public long getLong(int col) {
                int off = columnToSlotOffset[col];
                if (off != -1) {
                    return Unsafe.getUnsafe().getLong(pendingBaseAddr + slotOff + off);
                }
                return functions.getQuick(col).getLong(baseRec);
            }

            @Override
            public long getLong128Hi(int col) {
                return functions.getQuick(col).getLong128Hi(baseRec);
            }

            @Override
            public long getLong128Lo(int col) {
                return functions.getQuick(col).getLong128Lo(baseRec);
            }

            @Override
            public void getLong256(int col, CharSink<?> sink) {
                functions.getQuick(col).getLong256(baseRec, sink);
            }

            @Override
            public Long256 getLong256A(int col) {
                return functions.getQuick(col).getLong256A(baseRec);
            }

            @Override
            public Long256 getLong256B(int col) {
                return functions.getQuick(col).getLong256B(baseRec);
            }

            @Override
            public Record getRecord(int col) {
                // Function does not expose getRecord; nested-record projection is not produced by
                // the streaming dispatch (no SELECT-list expression yields a record-typed value
                // through any of the supported function factories), so delegating to baseRec via
                // the cursor's own column index is fine here.
                return baseRec.getRecord(col);
            }

            @Override
            public long getRowId() {
                return baseRec.getRowId();
            }

            @Override
            public short getShort(int col) {
                return functions.getQuick(col).getShort(baseRec);
            }

            @Override
            public CharSequence getStrA(int col) {
                return functions.getQuick(col).getStrA(baseRec);
            }

            @Override
            public CharSequence getStrB(int col) {
                return functions.getQuick(col).getStrB(baseRec);
            }

            @Override
            public int getStrLen(int col) {
                return functions.getQuick(col).getStrLen(baseRec);
            }

            @Override
            public CharSequence getSymA(int col) {
                return functions.getQuick(col).getSymbol(baseRec);
            }

            @Override
            public CharSequence getSymB(int col) {
                return functions.getQuick(col).getSymbolB(baseRec);
            }

            @Override
            public long getTimestamp(int col) {
                int off = columnToSlotOffset[col];
                if (off != -1) {
                    return Unsafe.getUnsafe().getLong(pendingBaseAddr + slotOff + off);
                }
                return functions.getQuick(col).getTimestamp(baseRec);
            }

            @Override
            public long getUpdateRowId() {
                return baseRec.getUpdateRowId();
            }

            @Override
            public Utf8Sequence getVarcharA(int col) {
                return functions.getQuick(col).getVarcharA(baseRec);
            }

            @Override
            public Utf8Sequence getVarcharB(int col) {
                return functions.getQuick(col).getVarcharB(baseRec);
            }

            @Override
            public int getVarcharSize(int col) {
                return functions.getQuick(col).getVarcharSize(baseRec);
            }

            void of(Record baseRec, long slotOff) {
                this.baseRec = baseRec;
                this.slotOff = slotOff;
            }
        }
    }
}
