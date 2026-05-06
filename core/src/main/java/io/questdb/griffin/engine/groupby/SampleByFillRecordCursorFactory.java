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
import io.questdb.cairo.ArrayColumnTypes;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.GeoHashes;
import io.questdb.cairo.RecordSink;
import io.questdb.cairo.TimestampDriver;
import io.questdb.cairo.arr.ArrayView;
import io.questdb.cairo.map.Map;
import io.questdb.cairo.map.MapFactory;
import io.questdb.cairo.map.MapKey;
import io.questdb.cairo.map.MapRecord;
import io.questdb.cairo.map.MapRecordCursor;
import io.questdb.cairo.map.MapValue;
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
 * Unified fill cursor for SAMPLE BY on the GROUP BY fast path. Two-pass
 * streaming design that handles keyed and non-keyed queries.
 * <p>
 * Pass 1: iterate sorted base cursor, discover all unique key combinations.
 * Pass 2: iterate again, emit data rows + fill rows for missing keys per bucket.
 * <p>
 * Expects sorted input (ORDER BY ts). Reports followedOrderByAdvice=false — the outer sort handles ordering.
 */
public class SampleByFillRecordCursorFactory extends AbstractRecordCursorFactory {
    public static final int FILL_CONSTANT = -1;
    public static final int FILL_KEY = -3;
    public static final int FILL_PREV_SELF = -2;
    private static final int HAS_PREV_SLOT = 1;
    // Per-key generational stamp: holds the bucket timestamp at which the key
    // last received a data row, or LONG_NULL before any data arrives. Presence
    // in the current bucket is `lastKnownTs == nextBucketTimestamp` -- bucket
    // transitions flip every key's flag in O(1) by advancing nextBucketTimestamp.
    private static final int LAST_KNOWN_TS_SLOT = 0;
    // Key columns in MapRecord start at KEY_POS_OFFSET, after the fixed-width
    // value header (LAST_KNOWN_TS, HAS_PREV, PREV_ROWID).
    private static final int KEY_POS_OFFSET = 3;
    private static final int PREV_ROWID_SLOT = 2;

    private final RecordCursorFactory base;
    private final ObjList<Function> constantFills;
    private final SampleByFillCursor cursor;
    private final IntList fillModes;
    private final Function fromFunc;
    private final boolean hasPrevFill;
    private final Function offsetFunc;
    private final long samplingInterval;
    private final char samplingIntervalUnit;
    private final int timestampIndex;
    private final int timestampType;
    private final Function toFunc;
    // Non-null only for day-or-larger SAMPLE BY + non-trivial FILL + TIME ZONE
    // (set by SqlOptimiser.rewriteSampleBy). Cursor re-evaluates per of() so a
    // bind-variable TZ picks up its current value. Null means no TZ wrap.
    private final Function tzFunc;

    /**
     * Appends the fixed-width value header (LAST_KNOWN_TS_SLOT, HAS_PREV_SLOT,
     * PREV_ROWID_SLOT - three LONGs) the cursor expects on every key entry.
     * External map builders must call this so slot indices stay authoritative.
     */
    public static void populateMapValueTypes(ArrayColumnTypes mapValueTypes) {
        mapValueTypes.add(ColumnType.LONG);
        mapValueTypes.add(ColumnType.LONG);
        mapValueTypes.add(ColumnType.LONG);
    }

    public SampleByFillRecordCursorFactory(
            CairoConfiguration configuration,
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
            RecordSink keySink,
            ArrayColumnTypes mapKeyTypes,
            ArrayColumnTypes mapValueTypes,
            IntList keyColIndices,
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
        // True if any column uses self-prev or cross-column prev fill.
        boolean localHasPrevFill = false;
        for (int i = 0, n = fillModes.size(); i < n; i++) {
            int mode = fillModes.getQuick(i);
            if (mode == FILL_PREV_SELF || mode >= 0) {
                localHasPrevFill = true;
                break;
            }
        }
        Map keysMap = null;
        SampleByFillCursor cursorLocal;
        try {
            if (keyColIndices.size() > 0) {
                keysMap = MapFactory.createOrderedMap(configuration, mapKeyTypes, mapValueTypes);
            }
            cursorLocal = new SampleByFillCursor(
                    metadata, timestampSampler,
                    fromFunc, toFunc, toFuncPos, fillModes, constantFills,
                    timestampIndex, timestampType, localHasPrevFill,
                    keySink, keysMap, keyColIndices, symbolTableColIndices,
                    offsetFunc, offsetFuncPos,
                    tzFunc, tzFuncPos, samplingIntervalUnit,
                    fixedPrevSrcCols, fixedPrevTypeTags, prevValueSlot, isPrevPositioningNeeded
            );
        } catch (Throwable th) {
            // Free what this constructor allocated. Caller still owns its inputs
            // (base, fromFunc, toFunc, constantFills, offsetFunc, tzFunc).
            Misc.free(keysMap);
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
        if (hasPrevFill && hasAnyConstantSlot()) {
            // PREV mixed with a constant fill column — "prev" alone would mislead.
            sink.attr("fill").val("mixed");
        } else if (hasPrevFill) {
            sink.attr("fill").val("prev");
        } else if (hasAnyConstantFill()) {
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
        Misc.free(base);
        Misc.free(fromFunc);
        Misc.free(toFunc);
        Misc.free(offsetFunc);
        Misc.free(tzFunc);
        Misc.freeObjList(constantFills);
    }

    private boolean hasAnyConstantFill() {
        // Returns true if any column has a non-null constant fill. The
        // !(f instanceof NullConstant) filter excludes both NULL fills and the
        // timestamp slot (always FILL_CONSTANT/NullConstant.NULL).
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

    private boolean hasAnyConstantSlot() {
        // Returns true when any non-timestamp column is FILL_CONSTANT
        // (NULL or value). Used by toPlan to label "mixed" when PREV coexists.
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

    private static class SampleByFillCursor implements NoRandomAccessRecordCursor {
        // Per-column dispatch codes compiled by compileDispatchPlan(). Two parallel
        // tables: fillDispatchCode for fill rows, dataDispatchCode (all DISPATCH_BASE)
        // for data rows. currentDispatchCode swaps between them at row boundaries.
        private static final int DISPATCH_BASE = 6;
        private static final int DISPATCH_CONSTANT = 0;
        private static final int DISPATCH_KEY_SLOT = 1;
        private static final int DISPATCH_NULL = 2;
        // Cached FILL_PREV: read directly off keysMapRecord without rebinding a
        // MapValue (OrderedMap value slots share the MapValue offsets). SYMBOL
        // slots hold the 4-byte id; getSymA/B resolve via the cached SymbolTable.
        private static final int DISPATCH_PREV_CACHE_SLOT = 5;
        private static final int DISPATCH_PREV_SLOT = 3;
        private static final int DISPATCH_TIMESTAMP_FILL = 4;

        private RecordCursor baseCursor;
        private Record baseRecord;
        // Unwrapped uniform-UTC sampler. timestampSampler may point here or at
        // tzWrap; held separately so the wrap can be rebuilt per of().
        private final TimestampSampler baseSampler;
        private long calendarOffset;
        private SqlExecutionCircuitBreaker circuitBreaker;
        private final ObjList<Function> constantFills;
        private int[] currentDispatchCode;
        private int[] dataDispatchCode;
        private final ObjList<Function> dispatchConstant = new ObjList<>();
        private int[] dispatchSlot;
        private int[] fillDispatchCode;
        private final IntList fillModes;
        private final FillRecord fillRecord = new FillRecord();
        private final FillTimestampConstant fillTimestampFunc;
        private final IntList fixedPrevSrcCols;
        private final IntList fixedPrevTypeTags;
        private final Function fromFunc;
        private boolean hasDataForCurrentBucket;
        private boolean hasExplicitTo;
        private boolean hasPendingRow;
        private final boolean hasPrevFill;
        private boolean hasPrevForCurrentGap;
        private boolean hasSimplePrev;
        private boolean isBaseCursorExhausted;
        private boolean isEmittingFills;
        private boolean isInitialized;
        private final boolean isKeyed;
        private boolean isOpen = true;
        // True when the recordAt-based PREV path is reachable: any FILL_PREV
        // output column reads a variable-width source (VARCHAR/BIN/STRING/ARRAY),
        // or non-keyed FILL_PREV is in use (no MapValue cache available).
        // False lets emitNextFillRow skip baseCursor.recordAt entirely.
        private final boolean isPrevPositioningNeeded;
        private int keyCount;
        private final RecordSink keySink;
        private final Map keysMap;
        private MapRecordCursor keysMapCursor;
        private MapRecord keysMapRecord;
        private long maxTimestamp;
        private long nextBucketTimestamp;
        private final Function offsetFunc;
        private final int offsetFuncPos;
        private final IntList outputColToKeyPos = new IntList();
        private long pendingTs;
        private Record prevRecord;
        // Per output column: MapValue slot for the cached fixed-size PREV value,
        // or -1 if not slot-eligible (variable-width sources fall back to PREV_SLOT).
        private final IntList prevValueSlot;
        // FILL stride unit ('d','w','M','y'), forwarded to the TZ wrap so the
        // local-grid floor uses the right calendar resolution.
        private final char samplingIntervalUnit;
        private long simplePrevRowId = -1L;
        // Per output column SymbolTable cache, populated in of(); used by
        // getSymA/getSymB to skip the MapRecord setSymbolTableResolver chain.
        private final ObjList<SymbolTable> symbolCache = new ObjList<>();
        private final IntList symbolTableColIndices;
        private final TimestampDriver timestampDriver;
        private final int timestampIndex;
        // Active sampler. Points at baseSampler or tzWrap; re-bound per of()
        // so a runtime-constant TIME ZONE picks up its current value.
        private TimestampSampler timestampSampler;
        // Keys still pending a fill emission for the current bucket. Reset to
        // keyCount at every boundary; decremented when a data row marks a key
        // present. toEmitCnt == 0 means the bucket is dense -- skip the scan.
        private int toEmitCnt;
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

        private SampleByFillCursor(
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
                RecordSink keySink,
                Map keysMap,
                IntList keyColIndices,
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
            this.fillTimestampFunc = new FillTimestampConstant(timestampType);
            this.hasPrevFill = hasPrevFill;
            this.keySink = keySink;
            this.keysMap = keysMap;
            this.symbolTableColIndices = symbolTableColIndices;
            this.fixedPrevSrcCols = fixedPrevSrcCols;
            this.fixedPrevTypeTags = fixedPrevTypeTags;
            this.prevValueSlot = prevValueSlot;
            this.isPrevPositioningNeeded = isPrevPositioningNeeded;
            assert (keysMap == null) == (keyColIndices.size() == 0);
            this.isKeyed = keysMap != null;

            // Key columns sit after the fixed-width value header plus any
            // FILL_PREV cache slots; dispatchSlot[col] for KEY_SLOT entries
            // resolves through this offset.
            final int keyPosOffset = KEY_POS_OFFSET + fixedPrevSrcCols.size();
            outputColToKeyPos.setAll(metadata.getColumnCount(), -1);
            for (int i = 0, n = keyColIndices.size(); i < n; i++) {
                outputColToKeyPos.setQuick(keyColIndices.getQuick(i), keyPosOffset + i);
            }

            compileDispatchPlan(metadata.getColumnCount());
        }

        @Override
        public void close() {
            baseCursor = Misc.free(baseCursor);
            if (isOpen) {
                isOpen = false;
                Misc.free(keysMap);
            }
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

            if (isEmittingFills) {
                if (emitNextFillRow()) {
                    return true;
                }
                // Gap buckets exhausted — fall through to main loop.
            }

            while (nextBucketTimestamp < maxTimestamp) {
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
                    if (hasDataForCurrentBucket && keysMap != null) {
                        isEmittingFills = true;
                        keysMapCursor.toTop();
                        return emitNextFillRow();
                    }
                    return false;
                }

                if (dataTs == nextBucketTimestamp) {
                    hasPendingRow = false;
                    currentDispatchCode = dataDispatchCode;
                    if (keysMap != null) {
                        hasDataForCurrentBucket = true;
                        MapKey mapKey = keysMap.withKey();
                        keySink.copy(baseRecord, mapKey);
                        MapValue value = mapKey.findValue();
                        // Pass 2 sees the same cursor as pass 1, so every key must be in the map.
                        // A null hit would mean internal corruption of a direct dependency.
                        assert value != null : "key discovered in pass 1 must exist in keysMap";
                        // Stamp this key as present in the current bucket. Stale stamps
                        // from prior buckets are auto-invalidated by the advance of
                        // nextBucketTimestamp, so no per-bucket reset is needed.
                        value.putLong(LAST_KNOWN_TS_SLOT, nextBucketTimestamp);
                        toEmitCnt--;
                        if (hasPrevFill) {
                            updateKeyPrevRowId(value, baseRecord);
                        }
                    } else {
                        // Non-keyed: only one row per bucket, advance immediately
                        if (hasPrevFill) {
                            saveSimplePrevRowId(baseRecord);
                        }
                        nextBucketTimestamp = timestampSampler.nextTimestamp(nextBucketTimestamp);
                    }
                    return true;
                }

                if (dataTs > nextBucketTimestamp) {
                    // Gap -- emit fill rows before advancing bucket.
                    if (hasDataForCurrentBucket && keysMap != null) {
                        // Dense bucket fast-path: skip the inner key-scan if every key already had data.
                        if (toEmitCnt == 0) {
                            toEmitCnt = keyCount;
                            nextBucketTimestamp = timestampSampler.nextTimestamp(nextBucketTimestamp);
                            hasDataForCurrentBucket = false;
                            continue;
                        }
                        isEmittingFills = true;
                        keysMapCursor.toTop();
                        if (emitNextFillRow()) {
                            return true;
                        }
                        continue; // gap fills exhausted, continue main loop
                    }

                    if (keysMap != null && keyCount > 0) {
                        // This bucket has NO data at all -- emit fills for all keys
                        isEmittingFills = true;
                        keysMapCursor.toTop();
                        toEmitCnt = keyCount;
                        if (emitNextFillRow()) {
                            return true;
                        }
                        continue; // gap fills exhausted, continue main loop
                    }

                    // Non-keyed gap
                    currentDispatchCode = fillDispatchCode;
                    fillTimestampFunc.value = nextBucketTimestamp;
                    hasPrevForCurrentGap = hasSimplePrev;
                    if (hasPrevForCurrentGap && isPrevPositioningNeeded) {
                        // Position prevRecord once; FillRecord getters read from it directly.
                        // Non-keyed FILL_PREV always lands here (no MapValue cache available).
                        baseCursor.recordAt(prevRecord, simplePrevRowId);
                    }
                    nextBucketTimestamp = timestampSampler.nextTimestamp(nextBucketTimestamp);
                    hasDataForCurrentBucket = false;
                    return true;
                }

                // Data row before the current bucket boundary -- upstream contract
                // violation or bucket-grid drift (DST, FROM/offset misalignment).
                // Fail visibly rather than silently corrupting output.
                throw CairoException.critical(0)
                        .put("sample by fill: data row timestamp ")
                        .put(dataTs)
                        .put(" precedes next bucket ")
                        .put(nextBucketTimestamp);
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
            if (isKeyed) {
                keysMap.clear();
            }
            isInitialized = false;
            hasSimplePrev = false;
            simplePrevRowId = -1L;
            hasPendingRow = false;
            isBaseCursorExhausted = false;
            hasExplicitTo = false;
            hasDataForCurrentBucket = false;
            isEmittingFills = false;
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
                if (mode == FILL_KEY) {
                    fillDispatchCode[col] = DISPATCH_KEY_SLOT;
                    dispatchSlot[col] = outputColToKeyPos.getQuick(col);
                } else if (mode >= 0 && outputColToKeyPos.getQuick(mode) >= 0) {
                    fillDispatchCode[col] = DISPATCH_KEY_SLOT;
                    dispatchSlot[col] = outputColToKeyPos.getQuick(mode);
                } else if (mode == FILL_PREV_SELF || mode >= 0) {
                    int slot = prevValueSlot.getQuick(col);
                    if (slot >= 0) {
                        // Fixed-size scalar (or SYMBOL) -- read from the cached MapValue slot.
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

        private boolean emitNextFillRow() {
            int skipCount = 0;
            while (true) {
                circuitBreaker.statefulThrowExceptionIfTripped();
                // Scan remaining keys in current bucket. Reads go through
                // keysMapRecord directly; OrderedMap value slots share the
                // MapValue offsets, so no per-row getValue() rebind is needed.
                while (keysMapCursor.hasNext()) {
                    // High-cardinality buckets where most keys had data force
                    // this loop to skip thousands of present keys before
                    // finding an absent one to fill. Poll the breaker on a
                    // 1024-iteration stride so cancellation does not stall.
                    if ((++skipCount & 0x3FF) == 0) {
                        circuitBreaker.statefulThrowExceptionIfTrippedNoThrottle();
                    }
                    long lastKnownTs = keysMapRecord.getLong(LAST_KNOWN_TS_SLOT);
                    if (lastKnownTs != nextBucketTimestamp) {
                        currentDispatchCode = fillDispatchCode;
                        fillTimestampFunc.value = nextBucketTimestamp;
                        // PREV_CACHE_SLOT slots are pre-filled with null sentinels in
                        // initialize(), so HAS_PREV / hasPrevForCurrentGap matter only
                        // for the variable-width PREV_SLOT path.
                        if (isPrevPositioningNeeded) {
                            boolean hasPrev = keysMapRecord.getLong(HAS_PREV_SLOT) != 0;
                            hasPrevForCurrentGap = hasPrev;
                            if (hasPrev) {
                                baseCursor.recordAt(prevRecord, keysMapRecord.getLong(PREV_ROWID_SLOT));
                            }
                        }
                        return true;
                    }
                }
                // Bucket exhausted -- advance. No per-key reset needed: the
                // next bucket's timestamp differs from any LAST_KNOWN_TS_SLOT
                // values still carrying the just-emitted bucket's stamp.
                toEmitCnt = keyCount;
                nextBucketTimestamp = timestampSampler.nextTimestamp(nextBucketTimestamp);
                hasDataForCurrentBucket = false;
                isEmittingFills = false;

                // Check if next bucket also needs fills (iterative, no recursion)
                if (nextBucketTimestamp >= maxTimestamp) {
                    return false;
                }
                if (hasPendingRow && pendingTs == nextBucketTimestamp) {
                    return false; // next bucket has data — let hasNext() handle it
                }
                if (isBaseCursorExhausted && !hasExplicitTo) {
                    return false;
                }
                // Reaching here means the next bucket is a confirmed gap:
                // either a pending row sits at a later timestamp, or the base
                // is exhausted with an explicit TO still driving fills.
                assert (hasPendingRow && pendingTs > nextBucketTimestamp) || isBaseCursorExhausted
                        : "next bucket must be a confirmed gap before re-entering inner emit";
                isEmittingFills = true;
                keysMapCursor.toTop();
            }
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

            // Pass 1: key discovery (keyed queries only)
            if (keysMap != null) {
                keysMap.clear();
                int keyIdx = 0;
                int prevSlotCount = fixedPrevSrcCols.size();
                while (baseCursor.hasNext()) {
                    circuitBreaker.statefulThrowExceptionIfTripped();
                    MapKey key = keysMap.withKey();
                    keySink.copy(baseRecord, key);
                    MapValue value = key.createValue();
                    if (value.isNew()) {
                        keyIdx++;
                        // LONG_NULL is the absence sentinel: it can never equal
                        // any bucket timestamp produced by TimestampSampler.
                        value.putLong(LAST_KNOWN_TS_SLOT, Numbers.LONG_NULL);
                        value.putLong(HAS_PREV_SLOT, 0L);
                        // Pre-fill cached PREV slots with per-type null sentinels
                        // so PREV_CACHE_SLOT getters can read unconditionally
                        // -- no hasPrev branch needed in the hot path.
                        for (int i = 0; i < prevSlotCount; i++) {
                            int slot = KEY_POS_OFFSET + i;
                            switch (fixedPrevTypeTags.getQuick(i)) {
                                case ColumnType.DOUBLE -> value.putDouble(slot, Double.NaN);
                                case ColumnType.FLOAT -> value.putFloat(slot, Float.NaN);
                                case ColumnType.LONG, ColumnType.DATE, ColumnType.TIMESTAMP ->
                                        value.putLong(slot, Numbers.LONG_NULL);
                                case ColumnType.GEOLONG -> value.putLong(slot, GeoHashes.NULL);
                                case ColumnType.DECIMAL64 -> value.putLong(slot, Decimals.DECIMAL64_NULL);
                                case ColumnType.INT, ColumnType.SYMBOL -> value.putInt(slot, Numbers.INT_NULL);
                                case ColumnType.IPv4 -> value.putInt(slot, Numbers.IPv4_NULL);
                                case ColumnType.GEOINT -> value.putInt(slot, GeoHashes.INT_NULL);
                                case ColumnType.DECIMAL32 -> value.putInt(slot, Decimals.DECIMAL32_NULL);
                                case ColumnType.SHORT -> value.putShort(slot, (short) 0);
                                case ColumnType.GEOSHORT -> value.putShort(slot, GeoHashes.SHORT_NULL);
                                case ColumnType.DECIMAL16 -> value.putShort(slot, Decimals.DECIMAL16_NULL);
                                case ColumnType.BYTE -> value.putByte(slot, (byte) 0);
                                case ColumnType.GEOBYTE -> value.putByte(slot, GeoHashes.BYTE_NULL);
                                case ColumnType.DECIMAL8 -> value.putByte(slot, Decimals.DECIMAL8_NULL);
                                case ColumnType.BOOLEAN -> value.putBool(slot, false);
                                case ColumnType.CHAR -> value.putChar(slot, (char) 0);
                                case ColumnType.LONG128 -> value.putLong128(slot, Numbers.LONG_NULL, Numbers.LONG_NULL);
                                case ColumnType.LONG256 -> value.putLong256(slot, Long256Impl.NULL_LONG256);
                                case ColumnType.DECIMAL128 -> value.putDecimal128Null(slot);
                                case ColumnType.DECIMAL256 -> value.putDecimal256Null(slot);
                                default -> {
                                    assert false : "unsupported fixed-size FILL(PREV) source type: "
                                            + ColumnType.nameOf(fixedPrevTypeTags.getQuick(i));
                                }
                            }
                        }
                    }
                }
                keyCount = keyIdx;
                if (keyCount == 0) {
                    // Empty GROUP BY output -- no keys to fill, emit zero rows.
                    isBaseCursorExhausted = true;
                    maxTimestamp = Long.MIN_VALUE;
                    nextBucketTimestamp = Long.MAX_VALUE;
                    return;
                }
                toEmitCnt = keyCount;
                baseCursor.toTop();
                keysMapRecord = keysMap.getRecord();
                keysMapRecord.setSymbolTableResolver(baseCursor, symbolTableColIndices);
                keysMapCursor = keysMap.getCursor();
            } else {
                // Non-keyed: degenerate case with 1 "empty" key
                keyCount = 1;
                toEmitCnt = 1;
            }

            // Peek first row to determine range. prevRecord MUST be captured
            // AFTER buildChain (i.e. after the first hasNext on the non-keyed
            // path) -- earlier capture would let SortedRecordCursor reposition
            // recordB underneath us.
            if (baseCursor.hasNext()) {
                prevRecord = baseCursor.getRecordB();
                long firstTs = baseRecord.getTimestamp(timestampIndex);
                final boolean nextBucketIsFirstTs = (fromTs == Numbers.LONG_NULL || firstTs < fromTs);
                nextBucketTimestamp = nextBucketIsFirstTs ? firstTs : fromTs;
                if (calendarOffset != 0 && fromTs == Numbers.LONG_NULL) {
                    // No FROM but offset exists: align grid to offset so round()
                    // matches timestamp_floor_utc buckets.
                    timestampSampler.setOffset(calendarOffset);
                    nextBucketTimestamp = timestampSampler.round(nextBucketTimestamp);
                } else if (calendarOffset != 0 && nextBucketIsFirstTs) {
                    // firstTs already sits on the floor grid (anchored at
                    // fromTs+calendarOffset). setLocalAnchor forwards untranslated
                    // because fromTs+calendarOffset is local-grid space (matches
                    // timestamp_floor_utc's raw-modulus treatment).
                    timestampSampler.setLocalAnchor(fromTs + calendarOffset);
                } else {
                    // firstTs path (calendarOffset == 0) OR fromTs path (any offset).
                    // Anchor at effectiveOffset = nextBucketTimestamp + calendarOffset
                    // to match timestamp_floor_utc's grid.
                    //
                    // Math.max clamps a positive-offset case where effectiveOffset
                    // > seed: GROUP BY's Micros.floor* clamps up; round() doesn't.
                    //
                    // setStart vs setLocalAnchor tracks the origin of nextBucketTimestamp:
                    //  - firstTs path: a GROUP BY bucket label on the local grid;
                    //    setStart applies UTC->local conversion. (Here calendarOffset
                    //    is 0, so effectiveOffset == firstTs.)
                    //  - fromTs path: a raw user FROM in local-grid space;
                    //    setLocalAnchor forwards untranslated, and localAnchorAsUtc
                    //    lifts back to UTC for the Math.max comparison.
                    //  Using setStart on the fromTs path would shift the grid by
                    //  tzOffset and trip the grid-drift guard on super-day strides.
                    final long effectiveOffset = nextBucketTimestamp + calendarOffset;
                    final long anchorUtc;
                    if (nextBucketIsFirstTs) {
                        timestampSampler.setStart(effectiveOffset);
                        anchorUtc = effectiveOffset;
                    } else {
                        timestampSampler.setLocalAnchor(effectiveOffset);
                        anchorUtc = timestampSampler.localAnchorAsUtc(effectiveOffset);
                    }
                    nextBucketTimestamp = Math.max(anchorUtc, timestampSampler.round(nextBucketTimestamp));
                }
                hasPendingRow = true;
                pendingTs = firstTs;
                if (maxTimestamp == Numbers.LONG_NULL) {
                    maxTimestamp = Long.MAX_VALUE;
                }
            } else {
                if (fromTs != Numbers.LONG_NULL && maxTimestamp != Numbers.LONG_NULL) {
                    // Same anchor rule as the non-empty-base branch, fromTs path
                    // only (no firstTs). effectiveOffset is local-grid space;
                    // setLocalAnchor forwards untranslated and localAnchorAsUtc
                    // lifts back so Math.max clamps in UTC.
                    final long effectiveOffset = fromTs + calendarOffset;
                    timestampSampler.setLocalAnchor(effectiveOffset);
                    final long anchorUtc = timestampSampler.localAnchorAsUtc(effectiveOffset);
                    nextBucketTimestamp = Math.max(anchorUtc, timestampSampler.round(fromTs));
                } else {
                    maxTimestamp = Long.MIN_VALUE;
                    nextBucketTimestamp = Long.MAX_VALUE;
                }
                isBaseCursorExhausted = true;
            }
        }

        private void of(RecordCursor baseCursor, SqlExecutionContext executionContext) throws SqlException {
            this.baseCursor = baseCursor;
            this.baseRecord = baseCursor.getRecord();
            if (!isOpen) {
                isOpen = true;
                if (isKeyed) {
                    keysMap.reopen();
                }
            }
            this.circuitBreaker = executionContext.getCircuitBreaker();
            Function.init(constantFills, baseCursor, executionContext, null);
            fromFunc.init(baseCursor, executionContext);
            toFunc.init(baseCursor, executionContext);
            // Reject FROM > TO at the same point as HORIZON JOIN RANGE
            // (SqlCodeGenerator.java) and MAT VIEW REFRESH RANGE
            // (SqlCompilerImpl.java). Both surface a clear SQL-level error
            // pointing at the offending TO expression. Without this guard
            // SAMPLE BY silently returns zero rows, masking what is almost
            // always a query-construction bug. FROM == TO is allowed (a
            // single-point range) -- only strict inversion is rejected.
            // LONG_NULL on either side means the clause is absent / null /
            // unbound and the bound is not in effect; only check when both
            // are concrete timestamps.
            final TimestampDriver driver = timestampDriver;
            if (fromFunc != driver.getTimestampConstantNull() && toFunc != driver.getTimestampConstantNull()) {
                final long fromTs = driver.from(fromFunc.getTimestamp(null), ColumnType.getTimestampType(fromFunc.getType()));
                final long toTs = driver.from(toFunc.getTimestamp(null), ColumnType.getTimestampType(toFunc.getType()));
                if (toTs != Numbers.LONG_NULL && fromTs > toTs) {
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
            // current value. The wrap is needed whenever a TZ resolves
            // (named zone or offset literal): only setLocalAnchor /
            // localAnchorAsUtc can fold tzOffset into the anchor for
            // super-day strides. tzFunc != null already implies the wrap
            // is required (SqlOptimiser only sets it for day-or-larger
            // SAMPLE BY + non-trivial FILL). getTimezoneRules unifies
            // offset literals (FixedTimeZoneRule) and DST zones uniformly.
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
            // Cache one SymbolTable per slot-dispatched output column. Cuts the
            // per-cell setSymbolTableResolver chain to a single valueOf call --
            // the dominant cost on sparse keyed SYMBOL fills.
            int columnCount = fillDispatchCode.length;
            symbolCache.setAll(columnCount, null);
            int symTableSize = symbolTableColIndices.size();
            for (int col = 0; col < columnCount; col++) {
                int code = fillDispatchCode[col];
                if (code != DISPATCH_KEY_SLOT && code != DISPATCH_PREV_CACHE_SLOT) {
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

        private void saveSimplePrevRowId(Record record) {
            simplePrevRowId = record.getRowId();
            hasSimplePrev = true;
        }

        private void updateKeyPrevRowId(MapValue value, Record record) {
            value.putLong(HAS_PREV_SLOT, 1L);
            value.putLong(PREV_ROWID_SLOT, record.getRowId());
            // Copy fixed-size FILL_PREV values into cached MapValue slots --
            // amortises a recordAt+RecordChain per read into N small writes.
            for (int i = 0, n = fixedPrevSrcCols.size(); i < n; i++) {
                int slot = KEY_POS_OFFSET + i;
                int srcCol = fixedPrevSrcCols.getQuick(i);
                int tag = fixedPrevTypeTags.getQuick(i);
                switch (tag) {
                    case ColumnType.DOUBLE -> value.putDouble(slot, record.getDouble(srcCol));
                    case ColumnType.FLOAT -> value.putFloat(slot, record.getFloat(srcCol));
                    case ColumnType.LONG -> value.putLong(slot, record.getLong(srcCol));
                    case ColumnType.DATE -> value.putLong(slot, record.getDate(srcCol));
                    case ColumnType.TIMESTAMP -> value.putLong(slot, record.getTimestamp(srcCol));
                    case ColumnType.GEOLONG -> value.putLong(slot, record.getGeoLong(srcCol));
                    case ColumnType.INT -> value.putInt(slot, record.getInt(srcCol));
                    case ColumnType.IPv4 -> value.putInt(slot, record.getIPv4(srcCol));
                    case ColumnType.GEOINT -> value.putInt(slot, record.getGeoInt(srcCol));
                    // SYMBOL stores the 4-byte id; getSymA/B resolves via symbolCache.
                    case ColumnType.SYMBOL -> value.putInt(slot, record.getInt(srcCol));
                    case ColumnType.SHORT -> value.putShort(slot, record.getShort(srcCol));
                    case ColumnType.GEOSHORT -> value.putShort(slot, record.getGeoShort(srcCol));
                    case ColumnType.BYTE -> value.putByte(slot, record.getByte(srcCol));
                    case ColumnType.GEOBYTE -> value.putByte(slot, record.getGeoByte(srcCol));
                    case ColumnType.CHAR -> value.putChar(slot, record.getChar(srcCol));
                    case ColumnType.BOOLEAN -> value.putBool(slot, record.getBool(srcCol));
                    case ColumnType.DECIMAL8 -> value.putByte(slot, record.getDecimal8(srcCol));
                    case ColumnType.DECIMAL16 -> value.putShort(slot, record.getDecimal16(srcCol));
                    case ColumnType.DECIMAL32 -> value.putInt(slot, record.getDecimal32(srcCol));
                    case ColumnType.DECIMAL64 -> value.putLong(slot, record.getDecimal64(srcCol));
                    case ColumnType.LONG128 ->
                            value.putLong128(slot, record.getLong128Lo(srcCol), record.getLong128Hi(srcCol));
                    case ColumnType.LONG256 -> value.putLong256(slot, record.getLong256A(srcCol));
                    case ColumnType.DECIMAL128 -> value.putDecimal128(slot, record, srcCol);
                    case ColumnType.DECIMAL256 -> value.putDecimal256(slot, record, srcCol);
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
                    case DISPATCH_KEY_SLOT -> keysMapRecord.getArray(dispatchSlot[col], columnType);
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
                    case DISPATCH_KEY_SLOT -> keysMapRecord.getBin(dispatchSlot[col]);
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
                    case DISPATCH_KEY_SLOT -> keysMapRecord.getBinLen(dispatchSlot[col]);
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
                    case DISPATCH_KEY_SLOT, DISPATCH_PREV_CACHE_SLOT -> keysMapRecord.getBool(dispatchSlot[col]);
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
                    case DISPATCH_KEY_SLOT, DISPATCH_PREV_CACHE_SLOT -> keysMapRecord.getByte(dispatchSlot[col]);
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
                    case DISPATCH_KEY_SLOT, DISPATCH_PREV_CACHE_SLOT -> keysMapRecord.getChar(dispatchSlot[col]);
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
                    case DISPATCH_KEY_SLOT, DISPATCH_PREV_CACHE_SLOT ->
                            keysMapRecord.getDecimal128(dispatchSlot[col], sink);
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
                    case DISPATCH_KEY_SLOT -> keysMapRecord.getDecimal16(dispatchSlot[col]);
                    case DISPATCH_PREV_CACHE_SLOT -> keysMapRecord.getShort(dispatchSlot[col]);
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
                    case DISPATCH_KEY_SLOT, DISPATCH_PREV_CACHE_SLOT ->
                            keysMapRecord.getDecimal256(dispatchSlot[col], sink);
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
                    case DISPATCH_KEY_SLOT -> keysMapRecord.getDecimal32(dispatchSlot[col]);
                    case DISPATCH_PREV_CACHE_SLOT -> keysMapRecord.getInt(dispatchSlot[col]);
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
                    case DISPATCH_KEY_SLOT -> keysMapRecord.getDecimal64(dispatchSlot[col]);
                    case DISPATCH_PREV_CACHE_SLOT -> keysMapRecord.getLong(dispatchSlot[col]);
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
                    case DISPATCH_KEY_SLOT -> keysMapRecord.getDecimal8(dispatchSlot[col]);
                    case DISPATCH_PREV_CACHE_SLOT -> keysMapRecord.getByte(dispatchSlot[col]);
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
                    case DISPATCH_KEY_SLOT, DISPATCH_PREV_CACHE_SLOT -> keysMapRecord.getDouble(dispatchSlot[col]);
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
                    case DISPATCH_KEY_SLOT, DISPATCH_PREV_CACHE_SLOT -> keysMapRecord.getFloat(dispatchSlot[col]);
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
                    case DISPATCH_KEY_SLOT -> keysMapRecord.getGeoByte(dispatchSlot[col]);
                    case DISPATCH_PREV_CACHE_SLOT -> keysMapRecord.getByte(dispatchSlot[col]);
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
                    case DISPATCH_KEY_SLOT -> keysMapRecord.getGeoInt(dispatchSlot[col]);
                    case DISPATCH_PREV_CACHE_SLOT -> keysMapRecord.getInt(dispatchSlot[col]);
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
                    case DISPATCH_KEY_SLOT -> keysMapRecord.getGeoLong(dispatchSlot[col]);
                    case DISPATCH_PREV_CACHE_SLOT -> keysMapRecord.getLong(dispatchSlot[col]);
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
                    case DISPATCH_KEY_SLOT -> keysMapRecord.getGeoShort(dispatchSlot[col]);
                    case DISPATCH_PREV_CACHE_SLOT -> keysMapRecord.getShort(dispatchSlot[col]);
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
                    case DISPATCH_KEY_SLOT, DISPATCH_PREV_CACHE_SLOT -> keysMapRecord.getIPv4(dispatchSlot[col]);
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
                    case DISPATCH_KEY_SLOT, DISPATCH_PREV_CACHE_SLOT -> keysMapRecord.getInt(dispatchSlot[col]);
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
                    case DISPATCH_KEY_SLOT -> keysMapRecord.getInterval(dispatchSlot[col]);
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
                    case DISPATCH_KEY_SLOT, DISPATCH_PREV_CACHE_SLOT -> keysMapRecord.getLong(dispatchSlot[col]);
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
                    case DISPATCH_KEY_SLOT, DISPATCH_PREV_CACHE_SLOT -> keysMapRecord.getLong128Hi(dispatchSlot[col]);
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
                    case DISPATCH_KEY_SLOT, DISPATCH_PREV_CACHE_SLOT -> keysMapRecord.getLong128Lo(dispatchSlot[col]);
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
                    case DISPATCH_KEY_SLOT, DISPATCH_PREV_CACHE_SLOT ->
                            keysMapRecord.getLong256(dispatchSlot[col], sink);
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
                    case DISPATCH_KEY_SLOT, DISPATCH_PREV_CACHE_SLOT -> keysMapRecord.getLong256A(dispatchSlot[col]);
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
                    case DISPATCH_KEY_SLOT, DISPATCH_PREV_CACHE_SLOT -> keysMapRecord.getLong256B(dispatchSlot[col]);
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
                    case DISPATCH_KEY_SLOT, DISPATCH_PREV_CACHE_SLOT -> keysMapRecord.getShort(dispatchSlot[col]);
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
                    case DISPATCH_KEY_SLOT -> keysMapRecord.getStrA(dispatchSlot[col]);
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
                    case DISPATCH_KEY_SLOT -> keysMapRecord.getStrB(dispatchSlot[col]);
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
                    case DISPATCH_KEY_SLOT -> keysMapRecord.getStrLen(dispatchSlot[col]);
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
                // KEY_SLOT and PREV_CACHE_SLOT route through the cached symbolCache
                // for a direct slot read. PREV_CACHE_SLOT is pre-filled with
                // INT_NULL == VALUE_IS_NULL, so valueOf returns null on first read.
                // Constant fills go through Function.getSymbol() by historical convention.
                return switch (currentDispatchCode[col]) {
                    case DISPATCH_BASE -> baseRecord.getSymA(col);
                    case DISPATCH_KEY_SLOT, DISPATCH_PREV_CACHE_SLOT ->
                            symbolCache.getQuick(col).valueOf(keysMapRecord.getInt(dispatchSlot[col]));
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
                    case DISPATCH_KEY_SLOT, DISPATCH_PREV_CACHE_SLOT ->
                            symbolCache.getQuick(col).valueBOf(keysMapRecord.getInt(dispatchSlot[col]));
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
                    case DISPATCH_KEY_SLOT, DISPATCH_PREV_CACHE_SLOT -> keysMapRecord.getTimestamp(dispatchSlot[col]);
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
                    case DISPATCH_KEY_SLOT -> keysMapRecord.getVarcharA(dispatchSlot[col]);
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
                    case DISPATCH_KEY_SLOT -> keysMapRecord.getVarcharB(dispatchSlot[col]);
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
                    case DISPATCH_KEY_SLOT -> keysMapRecord.getVarcharSize(dispatchSlot[col]);
                    case DISPATCH_PREV_SLOT -> hasPrevForCurrentGap ? prevRecord.getVarcharSize(dispatchSlot[col]) : -1;
                    case DISPATCH_CONSTANT -> dispatchConstant.getQuick(col).getVarcharSize(null);
                    default -> {
                        assert false : "unexpected dispatch code: " + currentDispatchCode[col];
                        yield -1;
                    }
                };
            }
        }

        private static class FillTimestampConstant extends TimestampFunction {
            // value is mutated per gap bucket and read by FillRecord. Not a
            // ConstantFunction -- the framework never sees this instance, and
            // the Function defaults (isConstant=false) match actual semantics.
            long value;

            FillTimestampConstant(int timestampType) {
                super(timestampType);
            }

            @Override
            public long getTimestamp(Record rec) {
                return value;
            }
        }
    }
}
