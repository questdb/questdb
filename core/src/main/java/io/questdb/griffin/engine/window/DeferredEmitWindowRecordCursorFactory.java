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
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.GenericRecordMetadata;
import io.questdb.cairo.Reopenable;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.WindowSPI;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryARW;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.Unsafe;

/**
 * Streaming window factory that supports deferred-emit window functions (e.g. LEAD).
 * <p>
 * Phase 2 restrictions enforced at construction time:
 * <ul>
 *   <li>Exactly one window function with positive {@link WindowFunction#getLookahead() lookahead}.</li>
 *   <li>No additional window functions in the output (no mixed LAG + LEAD yet).</li>
 *   <li>No {@code PARTITION BY} — single global partition only.</li>
 *   <li>Base cursor must support random access ({@link RecordCursorFactory#recordCursorSupportsRandomAccess()}).
 *       Pending entries store the base row id and look up base column values via
 *       {@link RecordCursor#recordAt(Record, long)} at emission time.</li>
 * </ul>
 * The factory throws {@link CairoException} at construction if any restriction is violated.
 * Callers are expected to gate selection via {@link CairoConfiguration#getSqlWindowStreamingLeadEnabled()}
 * and to fall back to {@link CachedWindowRecordCursorFactory} when the restrictions don't hold.
 * <p>
 * The cursor allocates a fixed-size ring buffer of {@code lookahead + 1} pending slots in native memory,
 * tagged {@link MemoryTag#NATIVE_WINDOW_PENDING}. Per pending slot:
 * <ul>
 *   <li>8 bytes hold the base row id.</li>
 *   <li>8 bytes hold the deferred LEAD value, back-filled when the lookahead position is reached
 *       or written with the function's default at end-of-cursor flush.</li>
 *   <li>One bit in {@code filled} indicates whether the LEAD slot has been written.</li>
 * </ul>
 * Total native memory: {@code 16 * (lookahead + 1)} bytes, constant per cursor lifetime.
 * <p>
 * Cross-thread safety: the cursor is single-threaded. The base cursor and pending memory are private
 * to this cursor instance and are never shared across threads.
 */
public class DeferredEmitWindowRecordCursorFactory extends AbstractRecordCursorFactory {

    private static final long SLOT_BYTES = 16L;          // 8 (rowid) + 8 (lead value)
    private static final long SLOT_LEAD_OFFSET = 8L;     // byte offset of lead value within a slot
    private static final long SLOT_ROWID_OFFSET = 0L;    // byte offset of base rowid within a slot

    private final RecordCursorFactory base;
    private final DeferredEmitWindowRecordCursor cursor;
    private final ObjList<Function> functions;
    private final WindowFunction leadFunction;
    private final int leadFunctionColumnIndex;
    private final int lookahead;
    private boolean closed;

    public DeferredEmitWindowRecordCursorFactory(
            RecordCursorFactory base,
            GenericRecordMetadata metadata,
            ObjList<Function> functions
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
                                .put("DeferredEmitWindowRecordCursorFactory supports exactly one lookahead function (Phase 2 restriction)");
                    }
                    lead = wf;
                    leadIdx = i;
                    la = wf.getLookahead();
                } else {
                    throw CairoException.critical(0)
                            .put("DeferredEmitWindowRecordCursorFactory does not yet support mixing immediate-emit and deferred window functions (Phase 2 restriction)");
                }
            }
        }
        if (lead == null) {
            throw CairoException.critical(0)
                    .put("DeferredEmitWindowRecordCursorFactory requires at least one lookahead function");
        }

        this.base = base;
        this.functions = functions;
        this.leadFunction = lead;
        this.leadFunctionColumnIndex = leadIdx;
        this.lookahead = la;
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
        Misc.freeObjList(functions);
        closed = true;
    }

    /**
     * Cursor implementing the deferred-emit state machine. Owns the per-cursor pending ring buffer.
     * Implements {@link WindowSPI} so the LEAD function can address its slot via
     * {@link #getAddress(long, int)} during back-fill and flush.
     */
    final class DeferredEmitWindowRecordCursor implements RecordCursor, WindowSPI, Reopenable {

        private final OutputRecord outputRecord = new OutputRecord();
        private final OutputRecord outputRecordB = new OutputRecord();
        private final int ringCapacity;
        private RecordCursor baseCursor;
        private Record baseRecordForEmit;
        private SqlExecutionCircuitBreaker circuitBreaker;
        // True once base cursor exhausts; we are draining pending entries with defaultValue back-fill.
        private boolean flushPhase;
        private boolean isOpen;
        // Bit per pending slot: 1 = LEAD slot has been written; 0 = still pending.
        // For Phase 2's single LEAD, fullMask == 1 (one bit). long bitmap supports up to 64 future functions.
        private long pendingFilled;
        // Native memory holding the pending ring: ringCapacity slots of SLOT_BYTES each.
        // Layout per slot: [rowid:long][leadValue:long].
        private MemoryARW pendingMem;
        private int ringCount;
        private int ringHead;
        private int ringTail;

        DeferredEmitWindowRecordCursor() {
            this.ringCapacity = lookahead + 1;
            // 16 bytes per slot * (lookahead + 1) slots. Allocate as one page.
            this.pendingMem = Vm.getCARWInstance(
                    Math.max(16L, SLOT_BYTES * ringCapacity),
                    1,
                    MemoryTag.NATIVE_WINDOW_PENDING
            );
            // Reserve the full memory range up front so getAddress is valid for every slot.
            this.pendingMem.jumpTo(SLOT_BYTES * ringCapacity);
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
            // Reset functions so a subsequent reopen starts from a clean state.
            resetFunctions();
            clearRing();
        }

        @Override
        public long getAddress(long pendingSlot, int columnIndex) {
            // For Phase 2: there is exactly one LEAD function, stored at the LEAD offset within each slot.
            // columnIndex is the output column index; the cursor maps it implicitly to slot+LEAD_OFFSET.
            return pendingMem.getPageAddress(0) + pendingSlot + SLOT_LEAD_OFFSET;
        }

        @Override
        public Record getRecord() {
            return outputRecord;
        }

        @Override
        public Record getRecordAt(long recordOffset) {
            // Used by the cached executor's two-pass path; the deferred-emit cursor never invokes it on
            // itself and never passes itself to a two-pass function. Throw loudly if reached.
            throw new UnsupportedOperationException("DeferredEmitWindowRecordCursor does not back two-pass window functions");
        }

        @Override
        public Record getRecordB() {
            return outputRecordB;
        }

        @Override
        public boolean hasNext() {
            circuitBreaker.statefulThrowExceptionIfTripped();
            // 1) Drain any ready head emissions first.
            // 2) Otherwise pull from base; processing may make the head emittable.
            // 3) Otherwise enter flush phase and emit remaining pending entries with defaultValue.
            while (true) {
                if (ringCount > 0 && isFilled(ringHead)) {
                    bindOutputToHead(outputRecord);
                    advanceHead();
                    return true;
                }
                if (!flushPhase) {
                    if (baseCursor.hasNext()) {
                        processBaseRow(baseCursor.getRecord());
                        continue;
                    }
                    flushPhase = true;
                    flushRemainingPending();
                    continue;
                }
                // flushPhase true and no ring entries left.
                return false;
            }
        }

        public void of(RecordCursor baseCursor, SqlExecutionContext executionContext) throws SqlException {
            // Note: this method takes ownership of baseCursor only on success. On exception the caller
            // (factory.getCursor) must free it. We assign to the field early so close() reclaims on
            // partial init failure.
            this.baseCursor = baseCursor;
            this.baseRecordForEmit = baseCursor.getRecordB();
            this.circuitBreaker = executionContext.getCircuitBreaker();
            if (!isOpen) {
                isOpen = true;
                try {
                    pendingMem = Vm.getCARWInstance(
                            Math.max(16L, SLOT_BYTES * ringCapacity),
                            1,
                            MemoryTag.NATIVE_WINDOW_PENDING
                    );
                    pendingMem.jumpTo(SLOT_BYTES * ringCapacity);
                    reopenFunctions();
                } catch (Throwable t) {
                    close();
                    throw t;
                }
            }
            // Always reset ring state on (re)open, even when isOpen was already true.
            clearRing();
            flushPhase = false;
            Function.init(functions, baseCursor, executionContext, null);
        }

        @Override
        public long preComputedStateSize() {
            return 0;
        }

        @Override
        public void recordAt(Record record, long atRowId) {
            // Deferred-emit cursor does not expose stable output row ids — emissions are not 1:1 to base
            // positions. Random access at the output is therefore unsupported; this matches
            // recordCursorSupportsRandomAccess() returning false on the factory.
            throw new UnsupportedOperationException("DeferredEmitWindowRecordCursor does not support random access");
        }

        @Override
        public void reopen() {
            // Called by the factory when reusing the cursor across multiple getCursor() invocations.
            // Native memory is freed in close() and re-allocated in of(); nothing to do here unless
            // we keep the cursor open across invocations.
            if (!isOpen) {
                pendingMem = Vm.getCARWInstance(
                        Math.max(16L, SLOT_BYTES * ringCapacity),
                        1,
                        MemoryTag.NATIVE_WINDOW_PENDING
                );
                pendingMem.jumpTo(SLOT_BYTES * ringCapacity);
                isOpen = true;
            }
            clearRing();
            flushPhase = false;
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
            clearRing();
            flushPhase = false;
            for (int i = 0, n = functions.size(); i < n; i++) {
                functions.getQuick(i).toTop();
            }
        }

        private void advanceHead() {
            // Clear the head's filled bit before advancing; defensive zeroing.
            pendingFilled &= ~(1L << ringHead);
            ringHead = (ringHead + 1) % ringCapacity;
            ringCount--;
        }

        private void bindOutputToHead(OutputRecord rec) {
            final long slotAddr = slotByteOffset(ringHead);
            final long rowid = pendingMem.getLong(slotAddr + SLOT_ROWID_OFFSET);
            final long leadValue = pendingMem.getLong(slotAddr + SLOT_LEAD_OFFSET);
            // Position base record to the right rowid so the consumer can read base columns through us.
            baseCursor.recordAt(baseRecordForEmit, rowid);
            rec.of(baseRecordForEmit, leadValue);
        }

        private void clearRing() {
            ringHead = 0;
            ringTail = 0;
            ringCount = 0;
            pendingFilled = 0L;
        }

        private void flushRemainingPending() {
            // All remaining pending entries get the LEAD function's defaultValue written to their slot.
            for (int i = 0; i < ringCount; i++) {
                int idx = (ringHead + i) % ringCapacity;
                if (!isFilled(idx)) {
                    leadFunction.streamingFlushDefault(slotByteOffset(idx), this);
                    pendingFilled |= 1L << idx;
                }
            }
        }

        private boolean isFilled(int ringIdx) {
            return (pendingFilled & (1L << ringIdx)) != 0L;
        }

        private void processBaseRow(Record baseRow) {
            // 1) Back-fill: if ringCount has reached lookahead, the head's lookahead position is met by R.
            //    For Phase 2 single LEAD, the entry to back-fill is always exactly the head.
            //    We back-fill BEFORE enqueueing R so the head's age matches lookahead at this moment.
            if (ringCount >= lookahead && !isFilled(ringHead)) {
                leadFunction.streamingBackfill(baseRow, slotByteOffset(ringHead), this);
                pendingFilled |= 1L << ringHead;
            }

            // 2) Enqueue R as a new pending entry.
            //    Ring is sized at lookahead + 1, so we always have room because head was just made
            //    emittable in step 1 (and hasNext will pop it before pulling the next base row).
            //    Defensive guard: refuse to enqueue if ring is somehow full to avoid silent corruption.
            if (ringCount == ringCapacity) {
                throw CairoException.critical(0)
                        .put("DeferredEmitWindowRecordCursor ring buffer full; this is a bug");
            }
            final long slotAddr = slotByteOffset(ringTail);
            pendingMem.putLong(slotAddr + SLOT_ROWID_OFFSET, baseRow.getRowId());
            // Clear the LEAD slot pre-emptively; streamingBackfill / streamingFlushDefault will write it.
            pendingMem.putLong(slotAddr + SLOT_LEAD_OFFSET, 0L);
            // Mark unfilled (bit clear). Defensive clear in case a prior round left state.
            pendingFilled &= ~(1L << ringTail);
            ringTail = (ringTail + 1) % ringCapacity;
            ringCount++;
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

        private long slotByteOffset(int ringIdx) {
            return (long) ringIdx * SLOT_BYTES;
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

    // Force linker to keep Unsafe reference if we ever need it for native writes outside WindowSPI.
    static {
        //noinspection ResultOfMethodCallIgnored
        Unsafe.getUnsafe();
    }
}
