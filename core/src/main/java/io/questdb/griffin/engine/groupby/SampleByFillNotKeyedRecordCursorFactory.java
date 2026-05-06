/*+******************************************************************************
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

package io.questdb.griffin.engine.groupby;

import io.questdb.cairo.AbstractRecordCursorFactory;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.GeoHashes;
import io.questdb.cairo.TimestampDriver;
import io.questdb.cairo.arr.ArrayView;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.NoRandomAccessRecordCursor;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.StaticSymbolTable;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.SymbolFunction;
import io.questdb.griffin.engine.functions.TimestampFunction;
import io.questdb.griffin.engine.functions.constants.NullConstant;
import io.questdb.std.BinarySequence;
import io.questdb.std.Decimal128;
import io.questdb.std.Decimal256;
import io.questdb.std.Decimals;
import io.questdb.std.IntList;
import io.questdb.std.Interval;
import io.questdb.std.Long256;
import io.questdb.std.Long256Impl;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.datetime.DateLocaleFactory;
import io.questdb.std.datetime.TimeZoneRules;
import io.questdb.std.datetime.millitime.Dates;
import io.questdb.std.str.CharSink;
import io.questdb.std.str.Utf8Sequence;
import org.jetbrains.annotations.NotNull;

import java.util.Arrays;

/**
 * Non-keyed variant of the SAMPLE BY FILL fast-path cursor, peeled off from
 * {@link SampleByFillRecordCursorFactory} so the non-keyed flow stops paying for
 * keyed scaffolding (no Map allocation, no per-key value rebind, no key-skip
 * scan in the gap-emit loop).
 * <p>
 * The previous-value cache lives in a single {@link SimpleMapValue} (one row of
 * 32-byte slots, off-heap). Slots:
 * <ul>
 *   <li>{@link #LAST_KNOWN_TS_SLOT} -- bucket timestamp where the last data row
 *       arrived, or {@link Numbers#LONG_NULL} when no data has been seen yet.
 *       Drives {@code hasPrevForCurrentGap}.</li>
 *   <li>{@link #PREV_ROWID_SLOT} -- rowId of the most recent data row, used by
 *       {@code baseCursor.recordAt} to materialize variable-width FILL_PREV
 *       columns.</li>
 *   <li>{@link #PREV_CACHE_OFFSET}..{@code PREV_CACHE_OFFSET + N - 1} -- one
 *       slot per fixed-size FILL_PREV source column. Read directly via
 *       {@link #DISPATCH_PREV_CACHE_SLOT} so the gap-emit hot path does not
 *       have to issue {@code recordAt}.</li>
 * </ul>
 * Variable-width FILL_PREV sources (VARCHAR/BIN/STRING/ARRAY) still go through
 * {@link #DISPATCH_PREV_SLOT} and {@code recordAt}; {@code isPrevPositioningNeeded}
 * is true exactly when at least one such column exists.
 */
public class SampleByFillNotKeyedRecordCursorFactory extends AbstractRecordCursorFactory {
    // The mode tags read off fillModes are part of the shared SAMPLE BY FILL
    // protocol; SqlCodeGenerator populates fillModes using the unified factory's
    // public constants. Re-aliased here purely to shorten switch arms below.
    private static final int FILL_CONSTANT = SampleByFillRecordCursorFactory.FILL_CONSTANT;
    private static final int FILL_PREV_SELF = SampleByFillRecordCursorFactory.FILL_PREV_SELF;
    // Slot indices into the SimpleMapValue cache. PREV_CACHE_OFFSET == 2 keeps
    // the per-source-col cache slots packed right after the two header slots,
    // matching the keyed cursor's MapValue layout for easy mental reuse.
    private static final int LAST_KNOWN_TS_SLOT = 0;
    private static final int PREV_CACHE_OFFSET = 2;
    private static final int PREV_ROWID_SLOT = 1;

    private final RecordCursorFactory base;
    private final ObjList<Function> constantFills;
    private final SampleByFillNotKeyedCursor cursor;
    private final IntList fillModes;
    private final Function fromFunc;
    private final boolean hasPrevFill;
    private final Function offsetFunc;
    // Owned by the factory so the cache survives cursor close()/of() cycles
    // (each test/query reuse closes and re-opens the cursor). Freed in _close().
    private final SimpleMapValue prevValue;
    private final long samplingInterval;
    private final char samplingIntervalUnit;
    private final int timestampIndex;
    private final int timestampType;
    private final Function toFunc;
    // Non-null only for day-or-larger SAMPLE BY + non-trivial FILL + TIME ZONE
    // (set by SqlOptimiser.rewriteSampleBy). Cursor re-evaluates per of() so a
    // bind-variable TZ picks up its current value. Null means no TZ wrap.
    private final Function tzFunc;

    public SampleByFillNotKeyedRecordCursorFactory(
            RecordMetadata metadata,
            RecordCursorFactory base,
            Function fromFunc,
            Function toFunc,
            int toFuncPos,
            long samplingInterval,
            char samplingIntervalUnit,
            TimestampSampler timestampSampler,
            IntList fillModes,
            ObjList<Function> constantFills,
            int timestampIndex,
            int timestampType,
            IntList symbolTableColIndices,
            Function offsetFunc,
            int offsetFuncPos,
            Function tzFunc,
            int tzFuncPos,
            IntList fixedPrevSrcCols,
            IntList fixedPrevTypeTags,
            IntList prevValueSlot,
            boolean isPrevPositioningNeeded
    ) {
        super(metadata);
        boolean localHasPrevFill = false;
        for (int i = 0, n = fillModes.size(); i < n; i++) {
            int mode = fillModes.getQuick(i);
            if (mode == FILL_PREV_SELF || mode >= 0) {
                localHasPrevFill = true;
                break;
            }
        }
        // 2 header slots + one slot per fixed-size FILL_PREV source.
        final int slotCount = PREV_CACHE_OFFSET + fixedPrevSrcCols.size();
        SimpleMapValue prevValueLocal = null;
        SampleByFillNotKeyedCursor cursorLocal;
        try {
            prevValueLocal = new SimpleMapValue(slotCount);
            cursorLocal = new SampleByFillNotKeyedCursor(
                    metadata, timestampSampler,
                    fromFunc, toFunc, toFuncPos, fillModes, constantFills,
                    timestampIndex, timestampType, localHasPrevFill,
                    prevValueLocal, symbolTableColIndices,
                    offsetFunc, offsetFuncPos,
                    tzFunc, tzFuncPos, samplingIntervalUnit,
                    fixedPrevSrcCols, fixedPrevTypeTags, prevValueSlot, isPrevPositioningNeeded
            );
        } catch (Throwable th) {
            // Free what this constructor allocated. Caller still owns its inputs
            // (base, fromFunc, toFunc, constantFills, offsetFunc, tzFunc).
            Misc.free(prevValueLocal);
            throw th;
        }
        this.base = base;
        this.fromFunc = fromFunc;
        this.toFunc = toFunc;
        this.offsetFunc = offsetFunc;
        this.tzFunc = tzFunc;
        this.samplingInterval = samplingInterval;
        this.samplingIntervalUnit = samplingIntervalUnit;
        this.timestampIndex = timestampIndex;
        this.timestampType = timestampType;
        this.constantFills = constantFills;
        this.fillModes = fillModes;
        this.hasPrevFill = localHasPrevFill;
        this.prevValue = prevValueLocal;
        this.cursor = cursorLocal;
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
            return cursor;
        } catch (Throwable th) {
            cursor.close();
            throw th;
        }
    }

    @Override
    public boolean recordCursorSupportsRandomAccess() {
        // Fill rows are synthesized per hasNext() and have no row id.
        return false;
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.type("Sample By Fill");
        TimestampDriver driver = ColumnType.getTimestampDriver(timestampType);
        if (fromFunc != driver.getTimestampConstantNull() || toFunc != driver.getTimestampConstantNull()) {
            sink.attr("range").val('(').val(fromFunc).val(',').val(toFunc).val(')');
        }
        sink.attr("stride").val('\'').val(samplingInterval).val(samplingIntervalUnit).val('\'');
        if (hasPrevFill && hasAnyConstantOrNullFill()) {
            // PREV mixed with a constant fill column -- "prev" alone would mislead.
            sink.attr("fill").val("mixed");
        } else if (hasPrevFill) {
            sink.attr("fill").val("prev");
        } else if (hasAnyNonNullConstantFill()) {
            sink.attr("fill").val("value");
        } else {
            sink.attr("fill").val("null");
        }
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
        Misc.free(cursor);
        Misc.free(prevValue);
        Misc.free(base);
        Misc.free(fromFunc);
        Misc.free(toFunc);
        Misc.free(offsetFunc);
        Misc.free(tzFunc);
        Misc.freeObjList(constantFills);
    }

    private boolean hasAnyConstantOrNullFill() {
        for (int i = 0, n = fillModes.size(); i < n; i++) {
            if (i == timestampIndex) {
                continue;
            }
            if (fillModes.getQuick(i) == FILL_CONSTANT) {
                return true;
            }
        }
        return false;
    }

    private boolean hasAnyNonNullConstantFill() {
        // The !(f instanceof NullConstant) filter excludes both NULL fills
        // and the timestamp slot (always FILL_CONSTANT/NullConstant.NULL).
        for (int i = 0, n = fillModes.size(); i < n; i++) {
            if (fillModes.getQuick(i) == FILL_CONSTANT) {
                Function f = constantFills.getQuick(i);
                if (f != null && !(f instanceof NullConstant)) {
                    return true;
                }
            }
        }
        return false;
    }

    private static class SampleByFillNotKeyedCursor implements NoRandomAccessRecordCursor {
        // Per-column dispatch codes compiled by compileDispatchPlan(). Two parallel
        // tables: fillDispatchCode for fill rows, dataDispatchCode (all DISPATCH_BASE)
        // for data rows. currentDispatchCode swaps between them at row boundaries.
        private static final int DISPATCH_BASE = 5;
        private static final int DISPATCH_CONSTANT = 0;
        private static final int DISPATCH_NULL = 1;
        // Cached FILL_PREV: read directly off prevValue (the cursor-local
        // SimpleMapValue) without re-binding. SYMBOL slots hold the 4-byte id;
        // getSymA/B resolve via the cached SymbolTable.
        private static final int DISPATCH_PREV_CACHE_SLOT = 4;
        private static final int DISPATCH_PREV_SLOT = 2;
        private static final int DISPATCH_TIMESTAMP_FILL = 3;

        private RecordCursor baseCursor;
        private Record baseRecord;
        // Unwrapped uniform-UTC sampler. timestampSampler may point here or at
        // tzWrap; held separately so the wrap can be rebuilt per of().
        private final TimestampSampler baseSampler;
        private long calendarOffset;
        private SqlExecutionCircuitBreaker circuitBreaker;
        private final ObjList<Function> constantFills;
        private long currentBucketTimestamp;
        private int[] currentDispatchCode;
        private int[] dataDispatchCode;
        private final ObjList<Function> dispatchConstant = new ObjList<>();
        private int[] dispatchSlot;
        private int[] fillDispatchCode;
        private final IntList fillModes;
        private final FillRecord fillRecord = new FillRecord();
        private final FillTimestampHolder fillTimestampFunc;
        private final IntList fixedPrevSrcCols;
        private final IntList fixedPrevTypeTags;
        private final Function fromFunc;
        private boolean hasExplicitTo;
        private boolean hasPendingRow;
        private final boolean hasPrevFill;
        private boolean hasPrevForCurrentGap;
        private boolean isBaseCursorExhausted;
        private boolean isInitialized;
        private boolean isOpen = true;
        // True when the recordAt-based PREV path is reachable: any FILL_PREV
        // output column reads a variable-width source (VARCHAR/BIN/STRING/ARRAY).
        // False lets the gap-emit branch skip baseCursor.recordAt entirely.
        private final boolean isPrevPositioningNeeded;
        private long maxTimestamp;
        private final Function offsetFunc;
        private final int offsetFuncPos;
        private long pendingTs;
        private Record prevRecord;
        // The previous-value cache (one row, off-heap, slot-indexed). Owned by
        // the factory; the cursor only borrows it so close()/of() cycles
        // (test harnesses re-getCursor against the same factory) preserve the
        // off-heap allocation.
        private final SimpleMapValue prevValue;
        // Per output column: cache slot for the cached fixed-size PREV value,
        // or -1 if not slot-eligible (variable-width sources fall back to PREV_SLOT).
        private final IntList prevValueSlot;
        // FILL stride unit ('d','w','M','y'), forwarded to the TZ wrap so the
        // local-grid floor uses the right calendar resolution.
        private final char samplingIntervalUnit;
        // Per output column SymbolTable cache, populated in of(); used by
        // getSymA/getSymB to skip the MapRecord setSymbolTableResolver chain.
        // For non-keyed only SYMBOL-typed FILL_PREV sources populate entries.
        private final ObjList<SymbolTable> symbolCache = new ObjList<>();
        private final IntList symbolTableColIndices;
        private final TimestampDriver timestampDriver;
        private final int timestampIndex;
        // Active sampler. Points at baseSampler or tzWrap; re-bound per of()
        // so a runtime-constant TIME ZONE picks up its current value.
        private TimestampSampler timestampSampler;
        private final Function toFunc;
        private final int toFuncPos;
        // Runtime-constant TIME ZONE Function (null when no TZ clause). Re-read
        // per of() so a bind variable picks up its current value -- pre-resolving
        // at compile time would silently bake the first-execute value.
        private final Function tzFunc;
        private final int tzFuncPos;
        // Lazily-allocated TZ wrap around baseSampler. Reused across of() calls
        // via setTzRules; held even after a fixed-offset of() for the next bind.
        private TimezoneFloorTimestampSampler tzWrap;

        private SampleByFillNotKeyedCursor(
                RecordMetadata metadata,
                TimestampSampler timestampSampler,
                @NotNull Function fromFunc,
                @NotNull Function toFunc,
                int toFuncPos,
                IntList fillModes,
                ObjList<Function> constantFills,
                int timestampIndex,
                int timestampType,
                boolean hasPrevFill,
                SimpleMapValue prevValue,
                IntList symbolTableColIndices,
                Function offsetFunc,
                int offsetFuncPos,
                Function tzFunc,
                int tzFuncPos,
                char samplingIntervalUnit,
                IntList fixedPrevSrcCols,
                IntList fixedPrevTypeTags,
                IntList prevValueSlot,
                boolean isPrevPositioningNeeded
        ) {
            this.offsetFunc = offsetFunc;
            this.offsetFuncPos = offsetFuncPos;
            this.tzFunc = tzFunc;
            this.tzFuncPos = tzFuncPos;
            this.samplingIntervalUnit = samplingIntervalUnit;
            // Factory passes the unwrapped sampler; of() lazily binds tzWrap.
            this.baseSampler = timestampSampler;
            this.timestampSampler = timestampSampler;
            this.fromFunc = fromFunc;
            this.toFunc = toFunc;
            this.toFuncPos = toFuncPos;
            this.fillModes = fillModes;
            this.constantFills = constantFills;
            this.timestampIndex = timestampIndex;
            this.timestampDriver = ColumnType.getTimestampDriver(timestampType);
            this.fillTimestampFunc = new FillTimestampHolder(timestampType);
            this.hasPrevFill = hasPrevFill;
            this.symbolTableColIndices = symbolTableColIndices;
            this.fixedPrevSrcCols = fixedPrevSrcCols;
            this.fixedPrevTypeTags = fixedPrevTypeTags;
            this.prevValueSlot = prevValueSlot;
            this.isPrevPositioningNeeded = isPrevPositioningNeeded;
            this.prevValue = prevValue;

            compileDispatchPlan(metadata.getColumnCount());
        }

        @Override
        public void close() {
            // Factory owns prevValue; never free it here. The cursor is
            // closed-and-reopened across query reuses (try-with-resources in
            // tests, repeated getCursor calls) and prevValue must outlive
            // those cycles.
            baseCursor = Misc.free(baseCursor);
            isOpen = false;
        }

        @Override
        public Record getRecord() {
            return fillRecord;
        }

        @Override
        public SymbolTable getSymbolTable(int columnIndex) {
            return baseCursor.getSymbolTable(columnIndex);
        }

        @Override
        public boolean hasNext() {
            circuitBreaker.statefulThrowExceptionIfTripped();
            if (!isInitialized) {
                initialize();
                isInitialized = true;
            }

            while (currentBucketTimestamp < maxTimestamp) {
                long dataTs;
                if (hasPendingRow) {
                    dataTs = pendingTs;
                } else if (!isBaseCursorExhausted && baseCursor.hasNext()) {
                    dataTs = baseRecord.getTimestamp(timestampIndex);
                    hasPendingRow = true;
                    pendingTs = dataTs;
                } else {
                    isBaseCursorExhausted = true;
                    dataTs = Long.MAX_VALUE;
                }

                if (isBaseCursorExhausted && !hasExplicitTo) {
                    return false;
                }

                if (dataTs == currentBucketTimestamp) {
                    hasPendingRow = false;
                    currentDispatchCode = dataDispatchCode;
                    // Stamp the bucket as carrying data so subsequent gap
                    // buckets recognise that prev exists; mirrors the keyed
                    // cursor's LAST_KNOWN_TS_SLOT discipline.
                    prevValue.putLong(LAST_KNOWN_TS_SLOT, currentBucketTimestamp);
                    if (hasPrevFill) {
                        updatePrevState(baseRecord);
                    }
                    currentBucketTimestamp = timestampSampler.nextTimestamp(currentBucketTimestamp);
                    return true;
                }

                if (dataTs > currentBucketTimestamp) {
                    // Gap -- emit one fill row before advancing the bucket.
                    currentDispatchCode = fillDispatchCode;
                    fillTimestampFunc.value = currentBucketTimestamp;
                    final long lastKnownTs = prevValue.getLong(LAST_KNOWN_TS_SLOT);
                    final boolean hasPrev = lastKnownTs != Numbers.LONG_NULL;
                    hasPrevForCurrentGap = hasPrev;
                    if (hasPrev && isPrevPositioningNeeded) {
                        // Position prevRecord once; FillRecord getters read from it
                        // for variable-width FILL_PREV columns.
                        baseCursor.recordAt(prevRecord, prevValue.getLong(PREV_ROWID_SLOT));
                    }
                    currentBucketTimestamp = timestampSampler.nextTimestamp(currentBucketTimestamp);
                    return true;
                }

                // Data row before the current bucket boundary -- upstream contract
                // violation or bucket-grid drift (DST, FROM/offset misalignment).
                // Fail visibly rather than silently corrupting output.
                throw CairoException.critical(0)
                        .put("sample by fill: data row timestamp ")
                        .put(dataTs)
                        .put(" precedes next bucket ")
                        .put(currentBucketTimestamp);
            }
            return false;
        }

        @Override
        public SymbolTable newSymbolTable(int columnIndex) {
            return baseCursor.newSymbolTable(columnIndex);
        }

        @Override
        public long preComputedStateSize() {
            return 0;
        }

        @Override
        public long size() {
            return -1;
        }

        @Override
        public void toTop() {
            if (baseCursor != null) {
                baseCursor.toTop();
            }
            // Reset the prev cache: clear all slots, then write LONG_NULL into
            // LAST_KNOWN_TS_SLOT so hasPrev evaluates false until the first
            // data row arrives. clear() zeroes everything; the per-type null
            // pre-fill below fixes up the prev-cache slots so unconditional
            // PREV_CACHE_SLOT reads return the right nulls before any data.
            prevValue.clear();
            prevValue.putLong(LAST_KNOWN_TS_SLOT, Numbers.LONG_NULL);
            preFillNullSlots();
            isInitialized = false;
            hasPendingRow = false;
            isBaseCursorExhausted = false;
            hasExplicitTo = false;
            hasPrevForCurrentGap = false;
            // Drop the previous baseCursor's recordB so a stale-pointer read
            // can't survive cursor reuse. initialize() reassigns it on the
            // next run when the new base has rows.
            prevRecord = null;
        }

        private void compileDispatchPlan(int columnCount) {
            // Precompute per-column dispatch tables once per cursor. Two parallel
            // arrays let hasNext swap a single pointer at row boundaries instead
            // of branching on an isGapFilling flag inside every getter.
            if (fillDispatchCode == null || fillDispatchCode.length < columnCount) {
                fillDispatchCode = new int[columnCount];
                dataDispatchCode = new int[columnCount];
                dispatchSlot = new int[columnCount];
            }
            dispatchConstant.setAll(columnCount, null);
            // Data rows always pass through to baseRecord, including the
            // timestamp column.
            Arrays.fill(dataDispatchCode, 0, columnCount, DISPATCH_BASE);
            for (int col = 0; col < columnCount; col++) {
                if (col == timestampIndex) {
                    fillDispatchCode[col] = DISPATCH_TIMESTAMP_FILL;
                    continue;
                }
                int mode = fillModes.getQuick(col);
                if (mode == FILL_PREV_SELF || mode >= 0) {
                    int slot = prevValueSlot.getQuick(col);
                    if (slot >= 0) {
                        // Fixed-size scalar (or SYMBOL) -- read from the cached prevValue slot.
                        fillDispatchCode[col] = DISPATCH_PREV_CACHE_SLOT;
                        dispatchSlot[col] = slot;
                    } else {
                        // Variable-width source -- materialize via baseCursor.recordAt.
                        fillDispatchCode[col] = DISPATCH_PREV_SLOT;
                        dispatchSlot[col] = mode >= 0 ? mode : col;
                    }
                } else if (mode == FILL_CONSTANT) {
                    fillDispatchCode[col] = DISPATCH_CONSTANT;
                    dispatchConstant.setQuick(col, constantFills.getQuick(col));
                } else {
                    fillDispatchCode[col] = DISPATCH_NULL;
                }
            }
            // hasNext rebinds this before returning the first row; defaulting
            // to fill mode keeps pre-first-row reads well-defined.
            currentDispatchCode = fillDispatchCode;
        }

        private void initialize() {
            TimestampDriver driver = timestampDriver;
            long fromTs = fromFunc == driver.getTimestampConstantNull() ? Numbers.LONG_NULL
                    : driver.from(fromFunc.getTimestamp(null), ColumnType.getTimestampType(fromFunc.getType()));
            hasExplicitTo = toFunc != driver.getTimestampConstantNull();
            maxTimestamp = hasExplicitTo
                    ? driver.from(toFunc.getTimestamp(null), ColumnType.getTimestampType(toFunc.getType()))
                    : Numbers.LONG_NULL;
            // Demote hasExplicitTo when TO evaluates to LONG_NULL at runtime
            // (bind variable, null::timestamp, or function returning null).
            // The toFunc identity check above only catches the constant-null
            // singleton. Long.MIN_VALUE folds into the same path: LONG_NULL ==
            // Long.MIN_VALUE is QuestDB's universal timestamp null sentinel.
            if (maxTimestamp == Numbers.LONG_NULL) hasExplicitTo = false;

            // Peek first row to determine range. prevRecord MUST be captured
            // AFTER the first hasNext on the base cursor -- earlier capture
            // would let SortedRecordCursor reposition recordB underneath us.
            if (baseCursor.hasNext()) {
                prevRecord = baseCursor.getRecordB();
                long firstTs = baseRecord.getTimestamp(timestampIndex);
                final boolean currentBucketIsFirstTs = (fromTs == Numbers.LONG_NULL || firstTs < fromTs);
                currentBucketTimestamp = currentBucketIsFirstTs ? firstTs : fromTs;
                if (calendarOffset != 0 && fromTs == Numbers.LONG_NULL) {
                    // No FROM but offset exists: align grid to offset so round()
                    // matches timestamp_floor_utc buckets.
                    timestampSampler.setOffset(calendarOffset);
                    currentBucketTimestamp = timestampSampler.round(currentBucketTimestamp);
                } else if (calendarOffset != 0 && currentBucketIsFirstTs) {
                    // firstTs already sits on the floor grid (anchored at
                    // fromTs+calendarOffset). setLocalAnchor forwards untranslated
                    // because fromTs+calendarOffset is local-grid space (matches
                    // timestamp_floor_utc's raw-modulus treatment).
                    timestampSampler.setLocalAnchor(fromTs + calendarOffset);
                } else {
                    // firstTs path (calendarOffset == 0) OR fromTs path (any offset).
                    // Anchor at effectiveOffset = currentBucketTimestamp + calendarOffset
                    // to match timestamp_floor_utc's grid.
                    final long effectiveOffset = currentBucketTimestamp + calendarOffset;
                    final long anchorUtc;
                    if (currentBucketIsFirstTs) {
                        timestampSampler.setStart(effectiveOffset);
                        anchorUtc = effectiveOffset;
                    } else {
                        timestampSampler.setLocalAnchor(effectiveOffset);
                        anchorUtc = timestampSampler.localAnchorAsUtc(effectiveOffset);
                    }
                    currentBucketTimestamp = Math.max(anchorUtc, timestampSampler.round(currentBucketTimestamp));
                }
                hasPendingRow = true;
                pendingTs = firstTs;
                if (maxTimestamp == Numbers.LONG_NULL) {
                    maxTimestamp = Long.MAX_VALUE;
                }
            } else {
                if (fromTs != Numbers.LONG_NULL && maxTimestamp != Numbers.LONG_NULL) {
                    final long effectiveOffset = fromTs + calendarOffset;
                    timestampSampler.setLocalAnchor(effectiveOffset);
                    final long anchorUtc = timestampSampler.localAnchorAsUtc(effectiveOffset);
                    currentBucketTimestamp = Math.max(anchorUtc, timestampSampler.round(fromTs));
                } else {
                    maxTimestamp = Long.MIN_VALUE;
                    currentBucketTimestamp = Long.MAX_VALUE;
                }
                isBaseCursorExhausted = true;
            }
        }

        private void of(RecordCursor baseCursor, SqlExecutionContext executionContext) throws SqlException {
            this.baseCursor = baseCursor;
            this.baseRecord = baseCursor.getRecord();
            // Factory owns prevValue, so there is nothing to reopen between
            // cursor close()/of() cycles -- just flip the open flag back on.
            isOpen = true;
            this.circuitBreaker = executionContext.getCircuitBreaker();
            Function.init(constantFills, baseCursor, executionContext, null);
            fromFunc.init(baseCursor, executionContext);
            toFunc.init(baseCursor, executionContext);
            // Reject FROM > TO at the same point as HORIZON JOIN RANGE
            // (SqlCodeGenerator.java) and MAT VIEW REFRESH RANGE
            // (SqlCompilerImpl.java). FROM == TO is allowed (a single-point
            // range) -- only strict inversion is rejected.
            final TimestampDriver driver = timestampDriver;
            if (fromFunc != driver.getTimestampConstantNull() && toFunc != driver.getTimestampConstantNull()) {
                final long fromTs = driver.from(fromFunc.getTimestamp(null), ColumnType.getTimestampType(fromFunc.getType()));
                final long toTs = driver.from(toFunc.getTimestamp(null), ColumnType.getTimestampType(toFunc.getType()));
                if (toTs != Numbers.LONG_NULL && fromTs != Numbers.LONG_NULL && fromTs > toTs) {
                    throw SqlException.$(toFuncPos, "TO timestamp must not be earlier than FROM timestamp");
                }
            }
            offsetFunc.init(baseCursor, executionContext);
            // Evaluate runtime-constant OFFSET into native units. Mirrors
            // AbstractSampleByCursor.parseParams. Null/absent leaves
            // calendarOffset == 0, which the initialize() branches no-op.
            final CharSequence offsetStr = offsetFunc.getStrA(null);
            if (offsetStr != null) {
                final long parsed = Dates.parseOffset(offsetStr);
                if (parsed == Numbers.LONG_NULL) {
                    throw SqlException.$(offsetFuncPos, "invalid offset: ").put(offsetStr);
                }
                calendarOffset = timestampDriver.fromMinutes(Numbers.decodeLowInt(parsed));
            } else {
                calendarOffset = 0;
            }
            // Re-resolve TIME ZONE per of() so bind variables pick up their
            // current value. tzFunc != null already implies a TZ wrap is
            // required (SqlOptimiser only sets it for day-or-larger SAMPLE BY
            // + non-trivial FILL).
            if (tzFunc != null) {
                tzFunc.init(baseCursor, executionContext);
                final CharSequence tz = tzFunc.getStrA(null);
                if (tz != null) {
                    final TimeZoneRules tzRules;
                    try {
                        tzRules = timestampDriver.getTimezoneRules(DateLocaleFactory.EN_LOCALE, tz);
                    } catch (CairoException e) {
                        throw SqlException.$(tzFuncPos, "invalid timezone: ").put(tz);
                    }
                    if (tzWrap == null) {
                        tzWrap = new TimezoneFloorTimestampSampler(baseSampler, tzRules, samplingIntervalUnit);
                    } else {
                        tzWrap.setTzRules(tzRules);
                    }
                    timestampSampler = tzWrap;
                } else {
                    timestampSampler = baseSampler;
                }
            }
            // Cache one SymbolTable per slot-dispatched output column. For
            // non-keyed only PREV_CACHE_SLOT entries with SYMBOL source need
            // resolving; KEY_SLOT does not exist on this path.
            int columnCount = fillDispatchCode.length;
            symbolCache.setAll(columnCount, null);
            int symTableSize = symbolTableColIndices.size();
            for (int col = 0; col < columnCount; col++) {
                int code = fillDispatchCode[col];
                if (code != DISPATCH_PREV_CACHE_SLOT) {
                    continue;
                }
                int slot = dispatchSlot[col];
                if (slot < 0 || slot >= symTableSize) {
                    continue;
                }
                int srcCol = symbolTableColIndices.getQuick(slot);
                if (srcCol >= 0) {
                    // Unwrap MapSymbolColumn-style wrappers to drop the
                    // per-cell wrapper hop on the hot read path.
                    SymbolTable st = baseCursor.getSymbolTable(srcCol);
                    if (st instanceof SymbolFunction sf) {
                        StaticSymbolTable inner = sf.getStaticSymbolTable();
                        if (inner != null) {
                            st = inner;
                        }
                    }
                    symbolCache.setQuick(col, st);
                }
            }
            toTop();
        }

        // Pre-fill cached PREV slots with per-type null sentinels so
        // PREV_CACHE_SLOT getters can read unconditionally -- no hasPrev branch
        // needed in the hot path. Called from toTop() so each new query starts
        // with the correct null defaults regardless of prior state.
        private void preFillNullSlots() {
            int prevSlotCount = fixedPrevSrcCols.size();
            for (int i = 0; i < prevSlotCount; i++) {
                int slot = PREV_CACHE_OFFSET + i;
                switch (fixedPrevTypeTags.getQuick(i)) {
                    case ColumnType.DOUBLE -> prevValue.putDouble(slot, Double.NaN);
                    case ColumnType.FLOAT -> prevValue.putFloat(slot, Float.NaN);
                    case ColumnType.LONG, ColumnType.DATE, ColumnType.TIMESTAMP ->
                            prevValue.putLong(slot, Numbers.LONG_NULL);
                    case ColumnType.GEOLONG -> prevValue.putLong(slot, GeoHashes.NULL);
                    case ColumnType.DECIMAL64 -> prevValue.putLong(slot, Decimals.DECIMAL64_NULL);
                    case ColumnType.INT, ColumnType.SYMBOL -> prevValue.putInt(slot, Numbers.INT_NULL);
                    case ColumnType.IPv4 -> prevValue.putInt(slot, Numbers.IPv4_NULL);
                    case ColumnType.GEOINT -> prevValue.putInt(slot, GeoHashes.INT_NULL);
                    case ColumnType.DECIMAL32 -> prevValue.putInt(slot, Decimals.DECIMAL32_NULL);
                    case ColumnType.SHORT -> prevValue.putShort(slot, (short) 0);
                    case ColumnType.GEOSHORT -> prevValue.putShort(slot, GeoHashes.SHORT_NULL);
                    case ColumnType.DECIMAL16 -> prevValue.putShort(slot, Decimals.DECIMAL16_NULL);
                    case ColumnType.BYTE -> prevValue.putByte(slot, (byte) 0);
                    case ColumnType.GEOBYTE -> prevValue.putByte(slot, GeoHashes.BYTE_NULL);
                    case ColumnType.DECIMAL8 -> prevValue.putByte(slot, Decimals.DECIMAL8_NULL);
                    case ColumnType.BOOLEAN -> prevValue.putBool(slot, false);
                    case ColumnType.CHAR -> prevValue.putChar(slot, (char) 0);
                    case ColumnType.LONG128 -> prevValue.putLong128(slot, Numbers.LONG_NULL, Numbers.LONG_NULL);
                    case ColumnType.LONG256 -> prevValue.putLong256(slot, Long256Impl.NULL_LONG256);
                    case ColumnType.DECIMAL128 -> prevValue.putDecimal128Null(slot);
                    case ColumnType.DECIMAL256 -> prevValue.putDecimal256Null(slot);
                    default -> {
                        assert false : "unsupported fixed-size FILL(PREV) source type: "
                                + ColumnType.nameOf(fixedPrevTypeTags.getQuick(i));
                    }
                }
            }
        }

        private void updatePrevState(Record record) {
            // The data-row arrival path already wrote LAST_KNOWN_TS_SLOT to the
            // current bucket timestamp; that doubles as the "has prev" marker
            // for subsequent gap buckets, so no separate flag write is needed.
            prevValue.putLong(PREV_ROWID_SLOT, record.getRowId());
            // Copy fixed-size FILL_PREV values into cached prevValue slots --
            // amortises a recordAt+RecordChain per read into N small writes.
            for (int i = 0, n = fixedPrevSrcCols.size(); i < n; i++) {
                int slot = PREV_CACHE_OFFSET + i;
                int srcCol = fixedPrevSrcCols.getQuick(i);
                int tag = fixedPrevTypeTags.getQuick(i);
                switch (tag) {
                    case ColumnType.DOUBLE -> prevValue.putDouble(slot, record.getDouble(srcCol));
                    case ColumnType.FLOAT -> prevValue.putFloat(slot, record.getFloat(srcCol));
                    case ColumnType.LONG -> prevValue.putLong(slot, record.getLong(srcCol));
                    case ColumnType.DATE -> prevValue.putLong(slot, record.getDate(srcCol));
                    case ColumnType.TIMESTAMP -> prevValue.putLong(slot, record.getTimestamp(srcCol));
                    case ColumnType.GEOLONG -> prevValue.putLong(slot, record.getGeoLong(srcCol));
                    case ColumnType.INT -> prevValue.putInt(slot, record.getInt(srcCol));
                    case ColumnType.IPv4 -> prevValue.putInt(slot, record.getIPv4(srcCol));
                    case ColumnType.GEOINT -> prevValue.putInt(slot, record.getGeoInt(srcCol));
                    // SYMBOL stores the 4-byte id; getSymA/B resolves via symbolCache.
                    case ColumnType.SYMBOL -> prevValue.putInt(slot, record.getInt(srcCol));
                    case ColumnType.SHORT -> prevValue.putShort(slot, record.getShort(srcCol));
                    case ColumnType.GEOSHORT -> prevValue.putShort(slot, record.getGeoShort(srcCol));
                    case ColumnType.BYTE -> prevValue.putByte(slot, record.getByte(srcCol));
                    case ColumnType.GEOBYTE -> prevValue.putByte(slot, record.getGeoByte(srcCol));
                    case ColumnType.CHAR -> prevValue.putChar(slot, record.getChar(srcCol));
                    case ColumnType.BOOLEAN -> prevValue.putBool(slot, record.getBool(srcCol));
                    case ColumnType.DECIMAL8 -> prevValue.putByte(slot, record.getDecimal8(srcCol));
                    case ColumnType.DECIMAL16 -> prevValue.putShort(slot, record.getDecimal16(srcCol));
                    case ColumnType.DECIMAL32 -> prevValue.putInt(slot, record.getDecimal32(srcCol));
                    case ColumnType.DECIMAL64 -> prevValue.putLong(slot, record.getDecimal64(srcCol));
                    case ColumnType.LONG128 ->
                            prevValue.putLong128(slot, record.getLong128Lo(srcCol), record.getLong128Hi(srcCol));
                    case ColumnType.LONG256 -> prevValue.putLong256(slot, record.getLong256A(srcCol));
                    case ColumnType.DECIMAL128 -> prevValue.putDecimal128(slot, record, srcCol);
                    case ColumnType.DECIMAL256 -> prevValue.putDecimal256(slot, record, srcCol);
                    default -> {
                        assert false : "unsupported fixed-size FILL(PREV) source type: "
                                + ColumnType.nameOf(tag);
                    }
                }
            }
        }

        /**
         * Per-cell dispatch consumes the flat arrays compiled by
         * {@link #compileDispatchPlan}. The default null/0/NaN tail in each getter
         * is defensive -- compileDispatchPlan always assigns a known code.
         * <p>
         * For PREV fills, the emit path positions prevRecord once via
         * {@code baseCursor.recordAt}; getters read typed values uniformly.
         * <p>
         * {@code getRecord(int)}, {@code getRowId()}, {@code getUpdateRowId()} are
         * deliberately not overridden -- they are not valid output columns for
         * SAMPLE BY FILL, and the inherited UOE flags any upstream regression.
         */
        private class FillRecord implements Record {

            @Override
            public ArrayView getArray(int col, int columnType) {
                return switch (currentDispatchCode[col]) {
                    case DISPATCH_BASE -> baseRecord.getArray(col, columnType);
                    case DISPATCH_PREV_SLOT ->
                            hasPrevForCurrentGap ? prevRecord.getArray(dispatchSlot[col], columnType) : null;
                    case DISPATCH_CONSTANT -> dispatchConstant.getQuick(col).getArray(null);
                    default -> {
                        assert false : "unexpected dispatch code: " + currentDispatchCode[col];
                        yield null;
                    }
                };
            }

            @Override
            public BinarySequence getBin(int col) {
                return switch (currentDispatchCode[col]) {
                    case DISPATCH_BASE -> baseRecord.getBin(col);
                    case DISPATCH_PREV_SLOT -> hasPrevForCurrentGap ? prevRecord.getBin(dispatchSlot[col]) : null;
                    case DISPATCH_CONSTANT -> dispatchConstant.getQuick(col).getBin(null);
                    default -> {
                        assert false : "unexpected dispatch code: " + currentDispatchCode[col];
                        yield null;
                    }
                };
            }

            @Override
            public long getBinLen(int col) {
                return switch (currentDispatchCode[col]) {
                    case DISPATCH_BASE -> baseRecord.getBinLen(col);
                    case DISPATCH_PREV_SLOT -> hasPrevForCurrentGap ? prevRecord.getBinLen(dispatchSlot[col]) : -1;
                    case DISPATCH_CONSTANT -> dispatchConstant.getQuick(col).getBinLen(null);
                    default -> {
                        assert false : "unexpected dispatch code: " + currentDispatchCode[col];
                        yield -1;
                    }
                };
            }

            @Override
            public boolean getBool(int col) {
                return switch (currentDispatchCode[col]) {
                    case DISPATCH_BASE -> baseRecord.getBool(col);
                    case DISPATCH_PREV_CACHE_SLOT -> prevValue.getBool(dispatchSlot[col]);
                    case DISPATCH_PREV_SLOT -> hasPrevForCurrentGap && prevRecord.getBool(dispatchSlot[col]);
                    case DISPATCH_CONSTANT -> dispatchConstant.getQuick(col).getBool(null);
                    default -> {
                        assert false : "unexpected dispatch code: " + currentDispatchCode[col];
                        yield false;
                    }
                };
            }

            @Override
            public byte getByte(int col) {
                return switch (currentDispatchCode[col]) {
                    case DISPATCH_BASE -> baseRecord.getByte(col);
                    case DISPATCH_PREV_CACHE_SLOT -> prevValue.getByte(dispatchSlot[col]);
                    case DISPATCH_PREV_SLOT -> hasPrevForCurrentGap ? prevRecord.getByte(dispatchSlot[col]) : 0;
                    // Narrow-integer Function convention: byte fills come through getInt().
                    case DISPATCH_CONSTANT -> (byte) dispatchConstant.getQuick(col).getInt(null);
                    default -> {
                        assert false : "unexpected dispatch code: " + currentDispatchCode[col];
                        yield 0;
                    }
                };
            }

            @Override
            public char getChar(int col) {
                return switch (currentDispatchCode[col]) {
                    case DISPATCH_BASE -> baseRecord.getChar(col);
                    case DISPATCH_PREV_CACHE_SLOT -> prevValue.getChar(dispatchSlot[col]);
                    case DISPATCH_PREV_SLOT -> hasPrevForCurrentGap ? prevRecord.getChar(dispatchSlot[col]) : 0;
                    case DISPATCH_CONSTANT -> dispatchConstant.getQuick(col).getChar(null);
                    default -> {
                        assert false : "unexpected dispatch code: " + currentDispatchCode[col];
                        yield 0;
                    }
                };
            }

            @Override
            public void getDecimal128(int col, Decimal128 sink) {
                switch (currentDispatchCode[col]) {
                    case DISPATCH_BASE -> baseRecord.getDecimal128(col, sink);
                    case DISPATCH_PREV_CACHE_SLOT -> prevValue.getDecimal128(dispatchSlot[col], sink);
                    case DISPATCH_PREV_SLOT -> {
                        if (hasPrevForCurrentGap) prevRecord.getDecimal128(dispatchSlot[col], sink);
                        else sink.ofRawNull();
                    }
                    case DISPATCH_CONSTANT -> dispatchConstant.getQuick(col).getDecimal128(null, sink);
                    default -> {
                        assert false : "unexpected dispatch code: " + currentDispatchCode[col];
                        sink.ofRawNull();
                    }
                }
            }

            @Override
            public short getDecimal16(int col) {
                return switch (currentDispatchCode[col]) {
                    case DISPATCH_BASE -> baseRecord.getDecimal16(col);
                    case DISPATCH_PREV_CACHE_SLOT -> prevValue.getShort(dispatchSlot[col]);
                    case DISPATCH_PREV_SLOT ->
                            hasPrevForCurrentGap ? prevRecord.getDecimal16(dispatchSlot[col]) : Decimals.DECIMAL16_NULL;
                    case DISPATCH_CONSTANT -> dispatchConstant.getQuick(col).getDecimal16(null);
                    default -> {
                        assert false : "unexpected dispatch code: " + currentDispatchCode[col];
                        yield Decimals.DECIMAL16_NULL;
                    }
                };
            }

            @Override
            public void getDecimal256(int col, Decimal256 sink) {
                switch (currentDispatchCode[col]) {
                    case DISPATCH_BASE -> baseRecord.getDecimal256(col, sink);
                    case DISPATCH_PREV_CACHE_SLOT -> prevValue.getDecimal256(dispatchSlot[col], sink);
                    case DISPATCH_PREV_SLOT -> {
                        if (hasPrevForCurrentGap) prevRecord.getDecimal256(dispatchSlot[col], sink);
                        else sink.ofRawNull();
                    }
                    case DISPATCH_CONSTANT -> dispatchConstant.getQuick(col).getDecimal256(null, sink);
                    default -> {
                        assert false : "unexpected dispatch code: " + currentDispatchCode[col];
                        sink.ofRawNull();
                    }
                }
            }

            @Override
            public int getDecimal32(int col) {
                return switch (currentDispatchCode[col]) {
                    case DISPATCH_BASE -> baseRecord.getDecimal32(col);
                    case DISPATCH_PREV_CACHE_SLOT -> prevValue.getInt(dispatchSlot[col]);
                    case DISPATCH_PREV_SLOT ->
                            hasPrevForCurrentGap ? prevRecord.getDecimal32(dispatchSlot[col]) : Decimals.DECIMAL32_NULL;
                    case DISPATCH_CONSTANT -> dispatchConstant.getQuick(col).getDecimal32(null);
                    default -> {
                        assert false : "unexpected dispatch code: " + currentDispatchCode[col];
                        yield Decimals.DECIMAL32_NULL;
                    }
                };
            }

            @Override
            public long getDecimal64(int col) {
                return switch (currentDispatchCode[col]) {
                    case DISPATCH_BASE -> baseRecord.getDecimal64(col);
                    case DISPATCH_PREV_CACHE_SLOT -> prevValue.getLong(dispatchSlot[col]);
                    case DISPATCH_PREV_SLOT ->
                            hasPrevForCurrentGap ? prevRecord.getDecimal64(dispatchSlot[col]) : Decimals.DECIMAL64_NULL;
                    case DISPATCH_CONSTANT -> dispatchConstant.getQuick(col).getDecimal64(null);
                    default -> {
                        assert false : "unexpected dispatch code: " + currentDispatchCode[col];
                        yield Decimals.DECIMAL64_NULL;
                    }
                };
            }

            @Override
            public byte getDecimal8(int col) {
                return switch (currentDispatchCode[col]) {
                    case DISPATCH_BASE -> baseRecord.getDecimal8(col);
                    case DISPATCH_PREV_CACHE_SLOT -> prevValue.getByte(dispatchSlot[col]);
                    case DISPATCH_PREV_SLOT ->
                            hasPrevForCurrentGap ? prevRecord.getDecimal8(dispatchSlot[col]) : Decimals.DECIMAL8_NULL;
                    case DISPATCH_CONSTANT -> dispatchConstant.getQuick(col).getDecimal8(null);
                    default -> {
                        assert false : "unexpected dispatch code: " + currentDispatchCode[col];
                        yield Decimals.DECIMAL8_NULL;
                    }
                };
            }

            @Override
            public double getDouble(int col) {
                return switch (currentDispatchCode[col]) {
                    case DISPATCH_BASE -> baseRecord.getDouble(col);
                    case DISPATCH_PREV_CACHE_SLOT -> prevValue.getDouble(dispatchSlot[col]);
                    case DISPATCH_PREV_SLOT ->
                            hasPrevForCurrentGap ? prevRecord.getDouble(dispatchSlot[col]) : Double.NaN;
                    case DISPATCH_CONSTANT -> dispatchConstant.getQuick(col).getDouble(null);
                    default -> {
                        assert false : "unexpected dispatch code: " + currentDispatchCode[col];
                        yield Double.NaN;
                    }
                };
            }

            @Override
            public float getFloat(int col) {
                return switch (currentDispatchCode[col]) {
                    case DISPATCH_BASE -> baseRecord.getFloat(col);
                    case DISPATCH_PREV_CACHE_SLOT -> prevValue.getFloat(dispatchSlot[col]);
                    case DISPATCH_PREV_SLOT ->
                            hasPrevForCurrentGap ? prevRecord.getFloat(dispatchSlot[col]) : Float.NaN;
                    case DISPATCH_CONSTANT -> dispatchConstant.getQuick(col).getFloat(null);
                    default -> {
                        assert false : "unexpected dispatch code: " + currentDispatchCode[col];
                        yield Float.NaN;
                    }
                };
            }

            @Override
            public byte getGeoByte(int col) {
                return switch (currentDispatchCode[col]) {
                    case DISPATCH_BASE -> baseRecord.getGeoByte(col);
                    case DISPATCH_PREV_CACHE_SLOT -> prevValue.getByte(dispatchSlot[col]);
                    case DISPATCH_PREV_SLOT ->
                            hasPrevForCurrentGap ? prevRecord.getGeoByte(dispatchSlot[col]) : GeoHashes.BYTE_NULL;
                    case DISPATCH_CONSTANT -> dispatchConstant.getQuick(col).getGeoByte(null);
                    default -> {
                        assert false : "unexpected dispatch code: " + currentDispatchCode[col];
                        yield GeoHashes.BYTE_NULL;
                    }
                };
            }

            @Override
            public int getGeoInt(int col) {
                return switch (currentDispatchCode[col]) {
                    case DISPATCH_BASE -> baseRecord.getGeoInt(col);
                    case DISPATCH_PREV_CACHE_SLOT -> prevValue.getInt(dispatchSlot[col]);
                    case DISPATCH_PREV_SLOT ->
                            hasPrevForCurrentGap ? prevRecord.getGeoInt(dispatchSlot[col]) : GeoHashes.INT_NULL;
                    case DISPATCH_CONSTANT -> dispatchConstant.getQuick(col).getGeoInt(null);
                    default -> {
                        assert false : "unexpected dispatch code: " + currentDispatchCode[col];
                        yield GeoHashes.INT_NULL;
                    }
                };
            }

            @Override
            public long getGeoLong(int col) {
                return switch (currentDispatchCode[col]) {
                    case DISPATCH_BASE -> baseRecord.getGeoLong(col);
                    case DISPATCH_PREV_CACHE_SLOT -> prevValue.getLong(dispatchSlot[col]);
                    case DISPATCH_PREV_SLOT ->
                            hasPrevForCurrentGap ? prevRecord.getGeoLong(dispatchSlot[col]) : GeoHashes.NULL;
                    case DISPATCH_CONSTANT -> dispatchConstant.getQuick(col).getGeoLong(null);
                    default -> {
                        assert false : "unexpected dispatch code: " + currentDispatchCode[col];
                        yield GeoHashes.NULL;
                    }
                };
            }

            @Override
            public short getGeoShort(int col) {
                return switch (currentDispatchCode[col]) {
                    case DISPATCH_BASE -> baseRecord.getGeoShort(col);
                    case DISPATCH_PREV_CACHE_SLOT -> prevValue.getShort(dispatchSlot[col]);
                    case DISPATCH_PREV_SLOT ->
                            hasPrevForCurrentGap ? prevRecord.getGeoShort(dispatchSlot[col]) : GeoHashes.SHORT_NULL;
                    case DISPATCH_CONSTANT -> dispatchConstant.getQuick(col).getGeoShort(null);
                    default -> {
                        assert false : "unexpected dispatch code: " + currentDispatchCode[col];
                        yield GeoHashes.SHORT_NULL;
                    }
                };
            }

            @Override
            public int getIPv4(int col) {
                return switch (currentDispatchCode[col]) {
                    case DISPATCH_BASE -> baseRecord.getIPv4(col);
                    case DISPATCH_PREV_CACHE_SLOT -> prevValue.getIPv4(dispatchSlot[col]);
                    case DISPATCH_PREV_SLOT ->
                            hasPrevForCurrentGap ? prevRecord.getIPv4(dispatchSlot[col]) : Numbers.IPv4_NULL;
                    case DISPATCH_CONSTANT -> dispatchConstant.getQuick(col).getIPv4(null);
                    default -> {
                        assert false : "unexpected dispatch code: " + currentDispatchCode[col];
                        yield Numbers.IPv4_NULL;
                    }
                };
            }

            @Override
            public int getInt(int col) {
                return switch (currentDispatchCode[col]) {
                    case DISPATCH_BASE -> baseRecord.getInt(col);
                    case DISPATCH_PREV_CACHE_SLOT -> prevValue.getInt(dispatchSlot[col]);
                    case DISPATCH_PREV_SLOT ->
                            hasPrevForCurrentGap ? prevRecord.getInt(dispatchSlot[col]) : Numbers.INT_NULL;
                    case DISPATCH_CONSTANT -> dispatchConstant.getQuick(col).getInt(null);
                    default -> {
                        assert false : "unexpected dispatch code: " + currentDispatchCode[col];
                        yield Numbers.INT_NULL;
                    }
                };
            }

            @Override
            public Interval getInterval(int col) {
                return switch (currentDispatchCode[col]) {
                    case DISPATCH_BASE -> baseRecord.getInterval(col);
                    case DISPATCH_PREV_SLOT ->
                            hasPrevForCurrentGap ? prevRecord.getInterval(dispatchSlot[col]) : Interval.NULL;
                    case DISPATCH_CONSTANT -> dispatchConstant.getQuick(col).getInterval(null);
                    default -> {
                        assert false : "unexpected dispatch code: " + currentDispatchCode[col];
                        yield Interval.NULL;
                    }
                };
            }

            @Override
            public long getLong(int col) {
                // Timestamp is a 64-bit long internally; Record.getLong(timestampIndex)
                // is a valid call. Without DISPATCH_TIMESTAMP_FILL here, fill rows
                // would silently return LONG_NULL for the bucket timestamp.
                // getDate() defaults to getLong(), so this arm covers both.
                return switch (currentDispatchCode[col]) {
                    case DISPATCH_BASE -> baseRecord.getLong(col);
                    case DISPATCH_TIMESTAMP_FILL -> fillTimestampFunc.value;
                    case DISPATCH_PREV_CACHE_SLOT -> prevValue.getLong(dispatchSlot[col]);
                    case DISPATCH_PREV_SLOT ->
                            hasPrevForCurrentGap ? prevRecord.getLong(dispatchSlot[col]) : Numbers.LONG_NULL;
                    case DISPATCH_CONSTANT -> dispatchConstant.getQuick(col).getLong(null);
                    default -> {
                        assert false : "unexpected dispatch code: " + currentDispatchCode[col];
                        yield Numbers.LONG_NULL;
                    }
                };
            }

            @Override
            public long getLong128Hi(int col) {
                return switch (currentDispatchCode[col]) {
                    case DISPATCH_BASE -> baseRecord.getLong128Hi(col);
                    case DISPATCH_PREV_CACHE_SLOT -> prevValue.getLong128Hi(dispatchSlot[col]);
                    case DISPATCH_PREV_SLOT ->
                            hasPrevForCurrentGap ? prevRecord.getLong128Hi(dispatchSlot[col]) : Numbers.LONG_NULL;
                    case DISPATCH_CONSTANT -> dispatchConstant.getQuick(col).getLong128Hi(null);
                    default -> {
                        assert false : "unexpected dispatch code: " + currentDispatchCode[col];
                        yield Numbers.LONG_NULL;
                    }
                };
            }

            @Override
            public long getLong128Lo(int col) {
                return switch (currentDispatchCode[col]) {
                    case DISPATCH_BASE -> baseRecord.getLong128Lo(col);
                    case DISPATCH_PREV_CACHE_SLOT -> prevValue.getLong128Lo(dispatchSlot[col]);
                    case DISPATCH_PREV_SLOT ->
                            hasPrevForCurrentGap ? prevRecord.getLong128Lo(dispatchSlot[col]) : Numbers.LONG_NULL;
                    case DISPATCH_CONSTANT -> dispatchConstant.getQuick(col).getLong128Lo(null);
                    default -> {
                        assert false : "unexpected dispatch code: " + currentDispatchCode[col];
                        yield Numbers.LONG_NULL;
                    }
                };
            }

            @Override
            public void getLong256(int col, CharSink<?> sink) {
                // Per the Record.getLong256 contract, null appends nothing.
                // Do NOT call sink.clear() -- it would erase the caller's row prefix.
                switch (currentDispatchCode[col]) {
                    case DISPATCH_BASE -> baseRecord.getLong256(col, sink);
                    case DISPATCH_PREV_CACHE_SLOT -> Numbers.appendLong256(prevValue.getLong256A(dispatchSlot[col]), sink);
                    case DISPATCH_PREV_SLOT -> {
                        if (hasPrevForCurrentGap) prevRecord.getLong256(dispatchSlot[col], sink);
                    }
                    case DISPATCH_CONSTANT -> dispatchConstant.getQuick(col).getLong256(null, sink);
                    default -> {
                        assert false : "unexpected dispatch code: " + currentDispatchCode[col];
                    }
                }
            }

            @Override
            public Long256 getLong256A(int col) {
                return switch (currentDispatchCode[col]) {
                    case DISPATCH_BASE -> baseRecord.getLong256A(col);
                    case DISPATCH_PREV_CACHE_SLOT -> prevValue.getLong256A(dispatchSlot[col]);
                    case DISPATCH_PREV_SLOT ->
                            hasPrevForCurrentGap ? prevRecord.getLong256A(dispatchSlot[col]) : Long256Impl.NULL_LONG256;
                    case DISPATCH_CONSTANT -> dispatchConstant.getQuick(col).getLong256A(null);
                    default -> {
                        assert false : "unexpected dispatch code: " + currentDispatchCode[col];
                        yield Long256Impl.NULL_LONG256;
                    }
                };
            }

            @Override
            public Long256 getLong256B(int col) {
                return switch (currentDispatchCode[col]) {
                    case DISPATCH_BASE -> baseRecord.getLong256B(col);
                    case DISPATCH_PREV_CACHE_SLOT -> prevValue.getLong256B(dispatchSlot[col]);
                    case DISPATCH_PREV_SLOT ->
                            hasPrevForCurrentGap ? prevRecord.getLong256B(dispatchSlot[col]) : Long256Impl.NULL_LONG256;
                    case DISPATCH_CONSTANT -> dispatchConstant.getQuick(col).getLong256B(null);
                    default -> {
                        assert false : "unexpected dispatch code: " + currentDispatchCode[col];
                        yield Long256Impl.NULL_LONG256;
                    }
                };
            }

            @Override
            public short getShort(int col) {
                return switch (currentDispatchCode[col]) {
                    case DISPATCH_BASE -> baseRecord.getShort(col);
                    case DISPATCH_PREV_CACHE_SLOT -> prevValue.getShort(dispatchSlot[col]);
                    case DISPATCH_PREV_SLOT ->
                            hasPrevForCurrentGap ? prevRecord.getShort(dispatchSlot[col]) : (short) 0;
                    // Narrow-integer Function convention: short fills come through getInt().
                    case DISPATCH_CONSTANT -> (short) dispatchConstant.getQuick(col).getInt(null);
                    default -> {
                        assert false : "unexpected dispatch code: " + currentDispatchCode[col];
                        yield (short) 0;
                    }
                };
            }

            @Override
            public CharSequence getStrA(int col) {
                return switch (currentDispatchCode[col]) {
                    case DISPATCH_BASE -> baseRecord.getStrA(col);
                    case DISPATCH_PREV_SLOT -> hasPrevForCurrentGap ? prevRecord.getStrA(dispatchSlot[col]) : null;
                    case DISPATCH_CONSTANT -> dispatchConstant.getQuick(col).getStrA(null);
                    default -> {
                        assert false : "unexpected dispatch code: " + currentDispatchCode[col];
                        yield null;
                    }
                };
            }

            @Override
            public CharSequence getStrB(int col) {
                return switch (currentDispatchCode[col]) {
                    case DISPATCH_BASE -> baseRecord.getStrB(col);
                    case DISPATCH_PREV_SLOT -> hasPrevForCurrentGap ? prevRecord.getStrB(dispatchSlot[col]) : null;
                    case DISPATCH_CONSTANT -> dispatchConstant.getQuick(col).getStrB(null);
                    default -> {
                        assert false : "unexpected dispatch code: " + currentDispatchCode[col];
                        yield null;
                    }
                };
            }

            @Override
            public int getStrLen(int col) {
                return switch (currentDispatchCode[col]) {
                    case DISPATCH_BASE -> baseRecord.getStrLen(col);
                    case DISPATCH_PREV_SLOT -> hasPrevForCurrentGap ? prevRecord.getStrLen(dispatchSlot[col]) : -1;
                    case DISPATCH_CONSTANT -> dispatchConstant.getQuick(col).getStrLen(null);
                    default -> {
                        assert false : "unexpected dispatch code: " + currentDispatchCode[col];
                        yield -1;
                    }
                };
            }

            @Override
            public CharSequence getSymA(int col) {
                // PREV_CACHE_SLOT routes through the cached symbolCache for a
                // direct slot read. Pre-filled with INT_NULL == VALUE_IS_NULL
                // by toTop(), so valueOf returns null on first read.
                return switch (currentDispatchCode[col]) {
                    case DISPATCH_BASE -> baseRecord.getSymA(col);
                    case DISPATCH_PREV_CACHE_SLOT -> symbolCache.getQuick(col).valueOf(prevValue.getInt(dispatchSlot[col]));
                    case DISPATCH_PREV_SLOT -> hasPrevForCurrentGap ? prevRecord.getSymA(dispatchSlot[col]) : null;
                    case DISPATCH_CONSTANT -> dispatchConstant.getQuick(col).getSymbol(null);
                    default -> {
                        assert false : "unexpected dispatch code: " + currentDispatchCode[col];
                        yield null;
                    }
                };
            }

            @Override
            public CharSequence getSymB(int col) {
                return switch (currentDispatchCode[col]) {
                    case DISPATCH_BASE -> baseRecord.getSymB(col);
                    case DISPATCH_PREV_CACHE_SLOT -> symbolCache.getQuick(col).valueBOf(prevValue.getInt(dispatchSlot[col]));
                    case DISPATCH_PREV_SLOT -> hasPrevForCurrentGap ? prevRecord.getSymB(dispatchSlot[col]) : null;
                    case DISPATCH_CONSTANT -> dispatchConstant.getQuick(col).getSymbolB(null);
                    default -> {
                        assert false : "unexpected dispatch code: " + currentDispatchCode[col];
                        yield null;
                    }
                };
            }

            @Override
            public long getTimestamp(int col) {
                return switch (currentDispatchCode[col]) {
                    case DISPATCH_BASE -> baseRecord.getTimestamp(col);
                    case DISPATCH_TIMESTAMP_FILL -> fillTimestampFunc.value;
                    case DISPATCH_PREV_CACHE_SLOT -> prevValue.getLong(dispatchSlot[col]);
                    case DISPATCH_PREV_SLOT ->
                            hasPrevForCurrentGap ? prevRecord.getTimestamp(dispatchSlot[col]) : Numbers.LONG_NULL;
                    case DISPATCH_CONSTANT -> dispatchConstant.getQuick(col).getTimestamp(null);
                    default -> {
                        assert false : "unexpected dispatch code: " + currentDispatchCode[col];
                        yield Numbers.LONG_NULL;
                    }
                };
            }

            @Override
            public Utf8Sequence getVarcharA(int col) {
                return switch (currentDispatchCode[col]) {
                    case DISPATCH_BASE -> baseRecord.getVarcharA(col);
                    case DISPATCH_PREV_SLOT -> hasPrevForCurrentGap ? prevRecord.getVarcharA(dispatchSlot[col]) : null;
                    case DISPATCH_CONSTANT -> dispatchConstant.getQuick(col).getVarcharA(null);
                    default -> {
                        assert false : "unexpected dispatch code: " + currentDispatchCode[col];
                        yield null;
                    }
                };
            }

            @Override
            public Utf8Sequence getVarcharB(int col) {
                return switch (currentDispatchCode[col]) {
                    case DISPATCH_BASE -> baseRecord.getVarcharB(col);
                    case DISPATCH_PREV_SLOT -> hasPrevForCurrentGap ? prevRecord.getVarcharB(dispatchSlot[col]) : null;
                    case DISPATCH_CONSTANT -> dispatchConstant.getQuick(col).getVarcharB(null);
                    default -> {
                        assert false : "unexpected dispatch code: " + currentDispatchCode[col];
                        yield null;
                    }
                };
            }

            @Override
            public int getVarcharSize(int col) {
                return switch (currentDispatchCode[col]) {
                    case DISPATCH_BASE -> baseRecord.getVarcharSize(col);
                    case DISPATCH_PREV_SLOT -> hasPrevForCurrentGap ? prevRecord.getVarcharSize(dispatchSlot[col]) : -1;
                    case DISPATCH_CONSTANT -> dispatchConstant.getQuick(col).getVarcharSize(null);
                    default -> {
                        assert false : "unexpected dispatch code: " + currentDispatchCode[col];
                        yield -1;
                    }
                };
            }
        }

        private static class FillTimestampHolder extends TimestampFunction {
            // The framework never sees this instance, so the inherited
            // Function defaults (isConstant=false) match actual semantics.
            long value;

            FillTimestampHolder(int timestampType) {
                super(timestampType);
            }

            @Override
            public long getTimestamp(Record rec) {
                return value;
            }
        }
    }
}
