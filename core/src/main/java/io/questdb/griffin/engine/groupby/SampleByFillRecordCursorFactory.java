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
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.TimestampFunction;
import io.questdb.griffin.engine.functions.constants.ConstantFunction;
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
    private static final int KEY_INDEX_SLOT = 0;
    // Key columns in the MapRecord start at KEY_POS_OFFSET (= PREV_ROWID_SLOT + 1);
    // slots below it are the fixed-width value header (KEY_INDEX, HAS_PREV, PREV_ROWID).
    private static final int KEY_POS_OFFSET = 3;
    private static final int PREV_ROWID_SLOT = 2;

    private final RecordCursorFactory base;
    private final ObjList<Function> constantFills;
    private final SampleByFillCursor cursor;
    private final IntList fillModes;
    private final Function fromFunc;
    private final boolean hasPrevFill;
    private final Map keysMap;
    private final long samplingInterval;
    private final char samplingIntervalUnit;
    private final int timestampIndex;
    private final int timestampType;
    private final Function toFunc;

    /**
     * Populates {@code mapValueTypes} with the fixed-width value header that the
     * cursor expects on every key entry: three LONG slots in KEY_INDEX_SLOT,
     * HAS_PREV_SLOT, PREV_ROWID_SLOT order. Callers that build the keys map
     * externally (e.g., {@code generateFill}) must invoke this helper so the
     * cursor's slot indices remain authoritative.
     */
    public static void populateMapValueTypes(ArrayColumnTypes mapValueTypes) {
        mapValueTypes.add(ColumnType.LONG); // slot KEY_INDEX_SLOT
        mapValueTypes.add(ColumnType.LONG); // slot HAS_PREV_SLOT
        mapValueTypes.add(ColumnType.LONG); // slot PREV_ROWID_SLOT
    }

    public SampleByFillRecordCursorFactory(
            CairoConfiguration configuration,
            RecordMetadata metadata,
            RecordCursorFactory base,
            Function fromFunc,
            Function toFunc,
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
            long calendarOffset,
            IntList fixedPrevSrcCols,
            IntList fixedPrevTypeTags,
            IntList prevValueSlot,
            boolean needsPrevPositioning
    ) {
        super(metadata);
        // hasPrevFill mirrors the "any PREV mode" signal consumed by toPlan and
        // the cursor's emit branches. Self-prev (FILL_PREV_SELF) and cross-column
        // prev (mode >= 0) both count.
        boolean localHasPrevFill = false;
        for (int i = 0, n = fillModes.size(); i < n; i++) {
            int mode = fillModes.getQuick(i);
            if (mode == FILL_PREV_SELF || mode >= 0) {
                localHasPrevFill = true;
                break;
            }
        }
        Map keysMapLocal = null;
        SampleByFillCursor cursorLocal;
        try {
            if (keyColIndices.size() > 0) {
                keysMapLocal = MapFactory.createOrderedMap(configuration, mapKeyTypes, mapValueTypes);
            }
            cursorLocal = new SampleByFillCursor(
                    metadata, timestampSampler,
                    fromFunc, toFunc, fillModes, constantFills,
                    timestampIndex, timestampType, localHasPrevFill,
                    keySink, keysMapLocal, keyColIndices, symbolTableColIndices,
                    calendarOffset,
                    fixedPrevSrcCols, fixedPrevTypeTags, prevValueSlot, needsPrevPositioning
            );
        } catch (Throwable th) {
            // Free resources allocated inside the constructor. Inputs (base, fromFunc,
            // toFunc, constantFills) remain owned by the caller's catch block, which
            // frees them on its own — this avoids a double-free.
            Misc.free(keysMapLocal);
            throw th;
        }
        this.base = base;
        this.fromFunc = fromFunc;
        this.toFunc = toFunc;
        this.samplingInterval = samplingInterval;
        this.samplingIntervalUnit = samplingIntervalUnit;
        this.timestampIndex = timestampIndex;
        this.timestampType = timestampType;
        this.constantFills = constantFills;
        this.fillModes = fillModes;
        this.hasPrevFill = localHasPrevFill;
        this.keysMap = keysMapLocal;
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
            // Function.init() can throw SqlException; close the cursor so its
            // internal resources don't leak before rethrowing.
            cursor.close();
            throw th;
        }
    }

    /**
     * Fill rows are synthesized per {@code hasNext()} from PREV snapshots, the
     * keys map, and bucket state; they have no row id, so {@code recordAt()}
     * has no meaningful slot to land on. The cursor-path equivalent only
     * advertised random access through an outer Sort wrapper that materialized
     * the entire result — the fill cursor itself never has.
     */
    @Override
    public boolean recordCursorSupportsRandomAccess() {
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
            // At least one aggregate is filled via PREV and at least one is
            // filled via a constant (NULL or non-null). "prev" alone would
            // misrepresent the per-column behavior.
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
        Misc.freeObjList(constantFills);
        Misc.free(keysMap);
    }

    private boolean hasAnyConstantFill() {
        // Intentional asymmetry with hasAnyConstantSlot: this method scans ALL
        // fillModes entries (including timestampIndex, which is always classified
        // FILL_CONSTANT with NullConstant.NULL), and the `!(f instanceof NullConstant)`
        // filter below naturally excludes the timestamp's sentinel. The sibling
        // method skips timestampIndex explicitly because it does NOT apply a
        // NullConstant filter.
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
        // Returns true when any non-timestamp, non-key column is classified as
        // FILL_CONSTANT (NULL or a concrete value). Used by toPlan to detect
        // heterogeneous fill modes: a "value"/"null" slot alongside a PREV
        // slot warrants the "mixed" label instead of "prev".
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
        // Dispatch codes compiled once per query from fillModes + outputColToKeyPos +
        // constantFills + timestampIndex into flat per-column arrays consumed by
        // FillRecord.getXxx on every fill-row read. See compileDispatchPlan().
        private static final int DISPATCH_CONSTANT = 0;
        private static final int DISPATCH_KEY_SLOT = 1;
        private static final int DISPATCH_NULL = 2;
        // Cached FILL_PREV: read currentMapValue.getXxx(dispatchSlot[col]) for
        // fixed-size scalar types, or keysMapRecord.getSymA/B(dispatchSlot[col])
        // for SYMBOL slots (the resolver wired up by setSymbolTableResolver()
        // turns the cached symbol id back into a CharSequence). The source
        // value was copied into the MapValue slot at update time, so no
        // recordAt/RecordChain traversal happens on emit.
        private static final int DISPATCH_PREV_CACHE_SLOT = 5;
        private static final int DISPATCH_PREV_SLOT = 3;
        private static final int DISPATCH_TIMESTAMP_FILL = 4;

        private RecordCursor baseCursor;
        private Record baseRecord;
        private final long calendarOffset;
        private SqlExecutionCircuitBreaker circuitBreaker;
        private final ObjList<Function> constantFills;
        // MapValue currently being emitted in emitNextFillRow; cached so the
        // FillRecord getters can read PREV cache slots without re-positioning
        // the keysMap cursor on every per-column read.
        private MapValue currentMapValue;
        private int[] dispatchCode;
        private Function[] dispatchConstant;
        private int[] dispatchSlot;
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
        private int keyCount;
        // Position in MapRecord where user key columns start. KEY_POS_OFFSET +
        // numFixedPrevSlots, baked into outputColToKeyPos so dispatchSlot[col]
        // values for KEY_SLOT entries are absolute MapRecord positions.
        private final int keyPosOffset;
        private boolean[] keyPresent;
        private final RecordSink keySink;
        private final Map keysMap;
        private MapRecordCursor keysMapCursor;
        private MapRecord keysMapRecord;
        private long maxTimestamp;
        // True when at least one FILL_PREV output column reads a variable-width
        // source (VARCHAR/BIN/STRING/ARRAY) or non-keyed FILL_PREV is in use --
        // i.e. the recordAt-based PREV path is reachable. False allows
        // emitNextFillRow to skip baseCursor.recordAt entirely.
        private final boolean needsPrevPositioning;
        private long nextBucketTimestamp;
        private final IntList outputColToKeyPos = new IntList();
        private long pendingTs;
        private Record prevRecord;
        // Per output column: index into the MapValue value section where the
        // cached fixed-size PREV value lives, or -1 if not slot-eligible.
        // SYMBOL slots store the 4-byte symbol id and are read via
        // keysMapRecord.getSymA/B(slot), which uses the symbolTableResolver
        // wired up in initialize() to turn the id back into a CharSequence.
        private final IntList prevValueSlot;
        private long simplePrevRowId = -1L;
        // Per output column SymbolTable cache for SYMBOL output cols whose
        // dispatch reads from a slot (DISPATCH_KEY_SLOT or
        // DISPATCH_PREV_CACHE_SLOT). Populated in of() and used by
        // getSymA/getSymB to short-circuit the MapRecord
        // setSymbolTableResolver chain (one IntList.getQuick + one virtual
        // getSymbolTable call per per-cell read).
        private SymbolTable[] symbolCache;
        private final IntList symbolTableColIndices;
        private final TimestampDriver timestampDriver;
        private final int timestampIndex;
        private final TimestampSampler timestampSampler;
        private final Function toFunc;

        private SampleByFillCursor(
                RecordMetadata metadata,
                TimestampSampler timestampSampler,
                @NotNull Function fromFunc,
                @NotNull Function toFunc,
                IntList fillModes,
                ObjList<Function> constantFills,
                int timestampIndex,
                int timestampType,
                boolean hasPrevFill,
                RecordSink keySink,
                Map keysMap,
                IntList keyColIndices,
                IntList symbolTableColIndices,
                long calendarOffset,
                IntList fixedPrevSrcCols,
                IntList fixedPrevTypeTags,
                IntList prevValueSlot,
                boolean needsPrevPositioning
        ) {
            this.calendarOffset = calendarOffset;
            this.timestampSampler = timestampSampler;
            this.fromFunc = fromFunc;
            this.toFunc = toFunc;
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
            this.needsPrevPositioning = needsPrevPositioning;
            this.keyPosOffset = KEY_POS_OFFSET + fixedPrevSrcCols.size();

            // Key columns in the MapRecord follow the fixed-width value header
            // (KEY_INDEX_SLOT, HAS_PREV_SLOT, PREV_ROWID_SLOT) plus any appended
            // fixed-size FILL_PREV cache slots. keyPosOffset bakes both
            // contributions in so dispatchSlot[col] values for KEY_SLOT entries
            // resolve to the right MapRecord position.
            outputColToKeyPos.setAll(metadata.getColumnCount(), -1);
            for (int i = 0, n = keyColIndices.size(); i < n; i++) {
                outputColToKeyPos.setQuick(keyColIndices.getQuick(i), keyPosOffset + i);
            }

            compileDispatchPlan(metadata.getColumnCount());
        }

        @Override
        public void close() {
            baseCursor = Misc.free(baseCursor);
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
                    fillRecord.isGapFilling = false;
                    if (keysMap != null) {
                        hasDataForCurrentBucket = true;
                        MapKey mapKey = keysMap.withKey();
                        keySink.copy(baseRecord, mapKey);
                        MapValue value = mapKey.findValue();
                        // Pass 2 iterates the same sorted cursor as pass 1, so every key must already be in
                        // the map. A null hit implies a bug in SortedRecordCursor, OrderedMap, or RecordSink
                        // -- internal corruption of a direct dependency, which matches CLAUDE.md's assert
                        // pattern. The explicit throw below (on the "dataTs precedes nextBucketTimestamp"
                        // branch) guards cross-component grid drift (sampler vs timestamp_floor_utc), which
                        // was empirically triggered during this PR's development and must fail visibly
                        // regardless of -ea.
                        assert value != null : "key discovered in pass 1 must exist in keysMap";
                        int keyIdx = (int) value.getLong(KEY_INDEX_SLOT);
                        keyPresent[keyIdx] = true;
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
                    // Gap — emit fill rows before advancing bucket.
                    if (hasDataForCurrentBucket && keysMap != null) {
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
                        Arrays.fill(keyPresent, 0, keyCount, false);
                        if (emitNextFillRow()) {
                            return true;
                        }
                        continue; // gap fills exhausted, continue main loop
                    }

                    // Non-keyed gap
                    fillRecord.isGapFilling = true;
                    fillTimestampFunc.value = nextBucketTimestamp;
                    hasPrevForCurrentGap = hasSimplePrev;
                    if (hasPrevForCurrentGap && needsPrevPositioning) {
                        // Position prevRecord once; FillRecord getters read from it directly.
                        // Non-keyed FILL_PREV always needs positioning because there is no
                        // MapValue to cache slots in -- SqlCodeGenerator forces
                        // needsPrevPositioning on for any non-keyed FILL_PREV column.
                        baseCursor.recordAt(prevRecord, simplePrevRowId);
                    }
                    nextBucketTimestamp = timestampSampler.nextTimestamp(nextBucketTimestamp);
                    hasDataForCurrentBucket = false;
                    return true;
                }

                // A pending data row landed at a timestamp strictly before the current
                // bucket boundary. The async GROUP BY upstream emits one row per
                // (bucket, key) and the bucket sampler moves forward in lockstep with
                // observed data, so this case implies an upstream contract violation or
                // a bucket grid drift (e.g., DST fall-back interacting with the sampler
                // or a FROM/offset misalignment between the sampler and floor_utc).
                // Either way the row is already being emitted against a bucket grid
                // that disagrees with the data, so silently passing it through would
                // corrupt query output. Fail the query so the problem is visible
                // instead of being absorbed into results.
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
            if (keysMap != null) {
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
        }

        private void compileDispatchPlan(int columnCount) {
            // Precompute per-output-column dispatch for FillRecord.getXxx once
            // per cursor. Inputs (fillModes, outputColToKeyPos, constantFills,
            // timestampIndex) are query-constants; the per-row FillRecord code
            // consumes the flat dispatchCode/dispatchSlot/dispatchConstant arrays
            // directly and avoids the 2-3 IntList.getQuick lookups + 4-branch
            // conditional chain that dominated the 805-sample IntList.getQuick
            // leaf in the JFR baseline.
            if (dispatchCode == null || dispatchCode.length < columnCount) {
                dispatchCode = new int[columnCount];
                dispatchSlot = new int[columnCount];
                dispatchConstant = new Function[columnCount];
            }
            for (int col = 0; col < columnCount; col++) {
                dispatchConstant[col] = null;
                if (col == timestampIndex) {
                    dispatchCode[col] = DISPATCH_TIMESTAMP_FILL;
                    continue;
                }
                int mode = fillModes.getQuick(col);
                if (mode == FILL_KEY) {
                    dispatchCode[col] = DISPATCH_KEY_SLOT;
                    dispatchSlot[col] = outputColToKeyPos.getQuick(col);
                } else if (mode >= 0 && outputColToKeyPos.getQuick(mode) >= 0) {
                    dispatchCode[col] = DISPATCH_KEY_SLOT;
                    dispatchSlot[col] = outputColToKeyPos.getQuick(mode);
                } else if (mode == FILL_PREV_SELF || mode >= 0) {
                    int slot = prevValueSlot.getQuick(col);
                    if (slot >= 0) {
                        // Source value is cached in the keysMap MapValue at the
                        // assigned slot. The same dispatch handles fixed-size
                        // scalars (read via currentMapValue.getXxx) and SYMBOL
                        // (read via keysMapRecord.getSymA/B which uses the
                        // existing setSymbolTableResolver wiring to turn the
                        // cached id into a CharSequence).
                        dispatchCode[col] = DISPATCH_PREV_CACHE_SLOT;
                        dispatchSlot[col] = slot;
                    } else {
                        // Variable-width or otherwise non-cacheable source
                        // (VARCHAR, STRING, BIN, ARRAY) -- fall back to the
                        // recordAt-based PREV path that materialises the row
                        // through prevRecord.
                        dispatchCode[col] = DISPATCH_PREV_SLOT;
                        dispatchSlot[col] = mode >= 0 ? mode : col;
                    }
                } else if (mode == FILL_CONSTANT) {
                    dispatchCode[col] = DISPATCH_CONSTANT;
                    dispatchConstant[col] = constantFills.getQuick(col);
                } else {
                    dispatchCode[col] = DISPATCH_NULL;
                }
            }
        }

        private boolean emitNextFillRow() {
            while (true) {
                circuitBreaker.statefulThrowExceptionIfTripped();
                // Inner loop: scan remaining keys in current bucket
                while (keysMapCursor.hasNext()) {
                    MapValue value = keysMapRecord.getValue();
                    int keyIdx = (int) value.getLong(KEY_INDEX_SLOT);
                    if (!keyPresent[keyIdx]) {
                        fillRecord.isGapFilling = true;
                        fillTimestampFunc.value = nextBucketTimestamp;
                        // Cache the HAS_PREV flag once per fill row so the 30+
                        // FillRecord getters avoid re-materialising the MapValue
                        // via keysMapRecord.getValue() on every per-column read.
                        hasPrevForCurrentGap = value.getLong(HAS_PREV_SLOT) != 0;
                        // Cache the MapValue so DISPATCH_PREV_CACHE_SLOT and
                        // DISPATCH_PREV_CACHE_SYMBOL getters can read directly
                        // from it without re-materialising the keysMap cursor
                        // position on every per-column read.
                        currentMapValue = value;
                        if (hasPrevForCurrentGap && needsPrevPositioning) {
                            // Position prevRecord only when at least one PREV col still
                            // needs the recordAt-based path (variable-width source). When
                            // all PREV cols are slot-eligible, dispatch is served from
                            // currentMapValue and recordAt drops out of the hot path.
                            baseCursor.recordAt(prevRecord, value.getLong(PREV_ROWID_SLOT));
                        }
                        return true;
                    }
                }
                // Bucket exhausted -- advance
                Arrays.fill(keyPresent, 0, keyCount, false);
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
                // Next bucket is a gap — emit fills for all keys
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
            // Demote hasExplicitTo when the runtime-evaluated TO expression returns
            // LONG_NULL. Object-identity check on toFunc above only catches the
            // TimestampConstantNull singleton; TO null::timestamp, bind variables,
            // and functions returning null at runtime reach here with maxTimestamp ==
            // LONG_NULL but hasExplicitTo == true, which would otherwise promote
            // maxTimestamp to Long.MAX_VALUE and skip the isBaseCursorExhausted
            // short-circuit at line 362.
            if (maxTimestamp == Numbers.LONG_NULL) hasExplicitTo = false;

            // Pass 1: key discovery (keyed queries only)
            if (keysMap != null) {
                keysMap.clear();
                int keyIdx = 0;
                while (baseCursor.hasNext()) {
                    circuitBreaker.statefulThrowExceptionIfTripped();
                    MapKey key = keysMap.withKey();
                    keySink.copy(baseRecord, key);
                    MapValue value = key.createValue();
                    if (value.isNew()) {
                        value.putLong(KEY_INDEX_SLOT, keyIdx++);
                        value.putLong(HAS_PREV_SLOT, 0L);
                    }
                }
                keyCount = keyIdx;
                if (keyCount == 0) {
                    // No keys discovered — empty GROUP BY output.
                    // With FROM/TO, emit zero rows (no keys to fill).
                    // Without FROM/TO, also emit zero rows (no data).
                    isBaseCursorExhausted = true;
                    maxTimestamp = Long.MIN_VALUE;
                    nextBucketTimestamp = Long.MAX_VALUE;
                    return;
                }
                if (keyPresent == null || keyPresent.length < keyCount) {
                    keyPresent = new boolean[Math.max(keyCount, 1)];
                } else {
                    Arrays.fill(keyPresent, 0, keyCount, false);
                }
                baseCursor.toTop();
                keysMapRecord = keysMap.getRecord();
                keysMapRecord.setSymbolTableResolver(baseCursor, symbolTableColIndices);
                keysMapCursor = keysMap.getCursor();
            } else {
                // Non-keyed: degenerate case with 1 "empty" key
                keyCount = 1;
                if (keyPresent == null || keyPresent.length < 1) {
                    keyPresent = new boolean[1];
                }
            }

            // Peek first row to determine range. The previous step either ran
            // pass 1 (keyed path) or will run buildChain() on this first
            // baseCursor.hasNext() call (non-keyed path). Either way, by the time
            // this branch body executes, SortedRecordCursor has completed
            // buildChain() and recordB is stable for the remainder of the cursor
            // lifetime. prevRecord MUST be initialized here, not in of(), because
            // buildChain would otherwise reposition recordB underneath us.
            if (baseCursor.hasNext()) {
                prevRecord = baseCursor.getRecordB();
                long firstTs = baseRecord.getTimestamp(timestampIndex);
                if (fromTs == Numbers.LONG_NULL || firstTs < fromTs) {
                    nextBucketTimestamp = firstTs;
                } else {
                    nextBucketTimestamp = fromTs;
                }
                if (calendarOffset != 0 && fromTs == Numbers.LONG_NULL) {
                    // No explicit FROM but offset exists: align sampler grid
                    // to the offset so round() matches timestamp_floor_utc buckets.
                    timestampSampler.setOffset(calendarOffset);
                    nextBucketTimestamp = timestampSampler.round(nextBucketTimestamp);
                } else if (calendarOffset != 0 && firstTs < fromTs) {
                    // firstTs is already a bucket on the floor grid (grid is
                    // anchored at effectiveOffset = fromTs + calendarOffset).
                    // With a non-zero offset the grid can place buckets strictly
                    // below fromTs -- e.g. FROM='05:00' WITH OFFSET '-00:30' and
                    // data at 05:00 floors to 04:30. Anchor the sampler at
                    // effectiveOffset; keep nextBucketTimestamp at firstTs so the
                    // first emitted bucket matches the data row.
                    timestampSampler.setStart(fromTs + calendarOffset);
                    // nextBucketTimestamp intentionally unchanged (== firstTs).
                } else {
                    // FROM exists or no offset: anchor sampler at fromTs + offset.
                    // timestamp_floor_utc uses effectiveOffset = from + offset,
                    // so the sampler grid must start at the same shifted point.
                    timestampSampler.setStart(nextBucketTimestamp + calendarOffset);
                    nextBucketTimestamp = nextBucketTimestamp + calendarOffset;
                }
                hasPendingRow = true;
                pendingTs = firstTs;
                if (maxTimestamp == Numbers.LONG_NULL) {
                    maxTimestamp = Long.MAX_VALUE;
                }
            } else {
                if (fromTs != Numbers.LONG_NULL && maxTimestamp != Numbers.LONG_NULL) {
                    nextBucketTimestamp = fromTs + calendarOffset;
                    timestampSampler.setStart(nextBucketTimestamp);
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
            this.circuitBreaker = executionContext.getCircuitBreaker();
            Function.init(constantFills, baseCursor, executionContext, null);
            fromFunc.init(baseCursor, executionContext);
            toFunc.init(baseCursor, executionContext);
            // Cache one SymbolTable per output column whose getSymA/getSymB
            // dispatch reads from a MapRecord/MapValue slot. JFR shows that on
            // sparse keyed fills the SYMBOL key column dominates per-cell cost
            // via the keysMapRecord.getSymA -> setSymbolTableResolver chain.
            // Caching the resolver per output col cuts that chain to one
            // valueOf call.
            int columnCount = dispatchCode.length;
            if (symbolCache == null || symbolCache.length < columnCount) {
                symbolCache = new SymbolTable[columnCount];
            } else {
                Arrays.fill(symbolCache, 0, symbolCache.length, null);
            }
            int symTableSize = symbolTableColIndices.size();
            for (int col = 0; col < columnCount; col++) {
                int code = dispatchCode[col];
                if (code != DISPATCH_KEY_SLOT && code != DISPATCH_PREV_CACHE_SLOT) {
                    continue;
                }
                int slot = dispatchSlot[col];
                if (slot < 0 || slot >= symTableSize) {
                    continue;
                }
                int srcCol = symbolTableColIndices.getQuick(slot);
                if (srcCol >= 0) {
                    symbolCache[col] = baseCursor.getSymbolTable(srcCol);
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
            // Copy each fixed-size FILL_PREV source value into its cached
            // MapValue slot. Per-row cost is N small writes onto the same
            // cache-hot MapValue page already touched for HAS_PREV/PREV_ROWID;
            // amortised over many fill rows that would otherwise pay
            // recordAt + RecordChain on every read.
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
                    // SYMBOL stores the 4-byte symbol id; getSymA/getSymB read
                    // it back via keysMapRecord which routes the slot through
                    // the symbolTableResolver wired up in initialize().
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
                    default -> {
                        // SqlCodeGenerator's isFixedSizePrevSlotEligible filter
                        // gates which tags reach this loop; an unknown tag here
                        // means the gate and this switch fell out of sync.
                    }
                }
            }
        }

        /**
         * The default null/0/NaN returns at the tail of each getter are defensive:
         * the GROUP BY pipeline always supplies a fill mode (FILL_KEY, FILL_PREV_SELF,
         * cross-column, or FILL_CONSTANT) for every output column, so those branches
         * are unreachable in practice.
         * <p>
         * Every output column is compiled once by compileDispatchPlan() into a
         * single dispatch code (DISPATCH_KEY_SLOT, DISPATCH_PREV_SLOT,
         * DISPATCH_CONSTANT, DISPATCH_TIMESTAMP_FILL, DISPATCH_NULL) plus an
         * optional slot index (for KEY_SLOT / PREV_SLOT) or Function reference
         * (for CONSTANT). The per-row dispatch below consumes those flat arrays
         * directly; no IntList.getQuick lookups, no fill-mode conditional chain.
         * <p>
         * PREV fill uses a single rowId per key (keyed) or one simplePrevRowId
         * (non-keyed); the fill emit path calls baseCursor.recordAt(prevRecord,
         * rowId) once per row, and the getters below read typed values uniformly
         * from prevRecord for every supported type.
         * <p>
         * Record methods intentionally NOT overridden: {@code getRecord(int)},
         * {@code getRowId()}, {@code getUpdateRowId()}. These are plumbing for
         * nested records, UPDATE row targeting, and filesystem row identifiers,
         * and are never valid output columns of a SAMPLE BY FILL query. Calling
         * them during gap-fill produces the default
         * {@link UnsupportedOperationException}, which is the desired signal if
         * an upstream change makes one of them reachable.
         */
        private class FillRecord implements Record {
            boolean isGapFilling;

            @Override
            public ArrayView getArray(int col, int columnType) {
                if (!isGapFilling) return baseRecord.getArray(col, columnType);
                int code = dispatchCode[col];
                if (code == DISPATCH_KEY_SLOT) return keysMapRecord.getArray(dispatchSlot[col], columnType);
                if (code == DISPATCH_PREV_SLOT) {
                    if (hasPrevForCurrentGap) return prevRecord.getArray(dispatchSlot[col], columnType);
                    return null;
                }
                if (code == DISPATCH_CONSTANT) return dispatchConstant[col].getArray(null);
                return null;
            }

            @Override
            public BinarySequence getBin(int col) {
                if (!isGapFilling) return baseRecord.getBin(col);
                int code = dispatchCode[col];
                if (code == DISPATCH_KEY_SLOT) return keysMapRecord.getBin(dispatchSlot[col]);
                if (code == DISPATCH_PREV_SLOT) {
                    if (hasPrevForCurrentGap) return prevRecord.getBin(dispatchSlot[col]);
                    return null;
                }
                if (code == DISPATCH_CONSTANT) return dispatchConstant[col].getBin(null);
                return null;
            }

            @Override
            public long getBinLen(int col) {
                if (!isGapFilling) return baseRecord.getBinLen(col);
                int code = dispatchCode[col];
                if (code == DISPATCH_KEY_SLOT) return keysMapRecord.getBinLen(dispatchSlot[col]);
                if (code == DISPATCH_PREV_SLOT) {
                    if (hasPrevForCurrentGap) return prevRecord.getBinLen(dispatchSlot[col]);
                    return -1;
                }
                if (code == DISPATCH_CONSTANT) return dispatchConstant[col].getBinLen(null);
                return -1;
            }

            @Override
            public boolean getBool(int col) {
                if (!isGapFilling) return baseRecord.getBool(col);
                int code = dispatchCode[col];
                if (code == DISPATCH_KEY_SLOT) return keysMapRecord.getBool(dispatchSlot[col]);
                if (code == DISPATCH_PREV_CACHE_SLOT) {
                    if (hasPrevForCurrentGap) return currentMapValue.getBool(dispatchSlot[col]);
                    return false;
                }
                if (code == DISPATCH_PREV_SLOT) {
                    if (hasPrevForCurrentGap) return prevRecord.getBool(dispatchSlot[col]);
                    return false;
                }
                if (code == DISPATCH_CONSTANT) return dispatchConstant[col].getBool(null);
                return false;
            }

            @Override
            public byte getByte(int col) {
                if (!isGapFilling) return baseRecord.getByte(col);
                int code = dispatchCode[col];
                if (code == DISPATCH_KEY_SLOT) return keysMapRecord.getByte(dispatchSlot[col]);
                if (code == DISPATCH_PREV_CACHE_SLOT) {
                    if (hasPrevForCurrentGap) return currentMapValue.getByte(dispatchSlot[col]);
                    return 0;
                }
                if (code == DISPATCH_PREV_SLOT) {
                    if (hasPrevForCurrentGap) return prevRecord.getByte(dispatchSlot[col]);
                    return 0;
                }
                // Constant byte fills go through getInt() to match the
                // narrow-integer convention used by Function implementations.
                if (code == DISPATCH_CONSTANT) return (byte) dispatchConstant[col].getInt(null);
                return 0;
            }

            @Override
            public char getChar(int col) {
                if (!isGapFilling) return baseRecord.getChar(col);
                int code = dispatchCode[col];
                if (code == DISPATCH_KEY_SLOT) return keysMapRecord.getChar(dispatchSlot[col]);
                if (code == DISPATCH_PREV_CACHE_SLOT) {
                    if (hasPrevForCurrentGap) return currentMapValue.getChar(dispatchSlot[col]);
                    return 0;
                }
                if (code == DISPATCH_PREV_SLOT) {
                    if (hasPrevForCurrentGap) return prevRecord.getChar(dispatchSlot[col]);
                    return 0;
                }
                if (code == DISPATCH_CONSTANT) return dispatchConstant[col].getChar(null);
                return 0;
            }

            @Override
            public void getDecimal128(int col, Decimal128 sink) {
                if (!isGapFilling) {
                    baseRecord.getDecimal128(col, sink);
                    return;
                }
                int code = dispatchCode[col];
                if (code == DISPATCH_KEY_SLOT) {
                    keysMapRecord.getDecimal128(dispatchSlot[col], sink);
                    return;
                }
                if (code == DISPATCH_PREV_SLOT) {
                    if (hasPrevForCurrentGap) prevRecord.getDecimal128(dispatchSlot[col], sink);
                    else sink.ofRawNull();
                    return;
                }
                if (code == DISPATCH_CONSTANT) {
                    dispatchConstant[col].getDecimal128(null, sink);
                    return;
                }
                sink.ofRawNull();
            }

            @Override
            public short getDecimal16(int col) {
                if (!isGapFilling) return baseRecord.getDecimal16(col);
                int code = dispatchCode[col];
                if (code == DISPATCH_KEY_SLOT) return keysMapRecord.getDecimal16(dispatchSlot[col]);
                if (code == DISPATCH_PREV_CACHE_SLOT) {
                    if (hasPrevForCurrentGap) return currentMapValue.getShort(dispatchSlot[col]);
                    return Decimals.DECIMAL16_NULL;
                }
                if (code == DISPATCH_PREV_SLOT) {
                    if (hasPrevForCurrentGap) return prevRecord.getDecimal16(dispatchSlot[col]);
                    return Decimals.DECIMAL16_NULL;
                }
                if (code == DISPATCH_CONSTANT) return dispatchConstant[col].getDecimal16(null);
                return Decimals.DECIMAL16_NULL;
            }

            @Override
            public void getDecimal256(int col, Decimal256 sink) {
                if (!isGapFilling) {
                    baseRecord.getDecimal256(col, sink);
                    return;
                }
                int code = dispatchCode[col];
                if (code == DISPATCH_KEY_SLOT) {
                    keysMapRecord.getDecimal256(dispatchSlot[col], sink);
                    return;
                }
                if (code == DISPATCH_PREV_SLOT) {
                    if (hasPrevForCurrentGap) prevRecord.getDecimal256(dispatchSlot[col], sink);
                    else sink.ofRawNull();
                    return;
                }
                if (code == DISPATCH_CONSTANT) {
                    dispatchConstant[col].getDecimal256(null, sink);
                    return;
                }
                sink.ofRawNull();
            }

            @Override
            public int getDecimal32(int col) {
                if (!isGapFilling) return baseRecord.getDecimal32(col);
                int code = dispatchCode[col];
                if (code == DISPATCH_KEY_SLOT) return keysMapRecord.getDecimal32(dispatchSlot[col]);
                if (code == DISPATCH_PREV_CACHE_SLOT) {
                    if (hasPrevForCurrentGap) return currentMapValue.getInt(dispatchSlot[col]);
                    return Decimals.DECIMAL32_NULL;
                }
                if (code == DISPATCH_PREV_SLOT) {
                    if (hasPrevForCurrentGap) return prevRecord.getDecimal32(dispatchSlot[col]);
                    return Decimals.DECIMAL32_NULL;
                }
                if (code == DISPATCH_CONSTANT) return dispatchConstant[col].getDecimal32(null);
                return Decimals.DECIMAL32_NULL;
            }

            @Override
            public long getDecimal64(int col) {
                if (!isGapFilling) return baseRecord.getDecimal64(col);
                int code = dispatchCode[col];
                if (code == DISPATCH_KEY_SLOT) return keysMapRecord.getDecimal64(dispatchSlot[col]);
                if (code == DISPATCH_PREV_CACHE_SLOT) {
                    if (hasPrevForCurrentGap) return currentMapValue.getLong(dispatchSlot[col]);
                    return Decimals.DECIMAL64_NULL;
                }
                if (code == DISPATCH_PREV_SLOT) {
                    if (hasPrevForCurrentGap) return prevRecord.getDecimal64(dispatchSlot[col]);
                    return Decimals.DECIMAL64_NULL;
                }
                if (code == DISPATCH_CONSTANT) return dispatchConstant[col].getDecimal64(null);
                return Decimals.DECIMAL64_NULL;
            }

            @Override
            public byte getDecimal8(int col) {
                if (!isGapFilling) return baseRecord.getDecimal8(col);
                int code = dispatchCode[col];
                if (code == DISPATCH_KEY_SLOT) return keysMapRecord.getDecimal8(dispatchSlot[col]);
                if (code == DISPATCH_PREV_CACHE_SLOT) {
                    if (hasPrevForCurrentGap) return currentMapValue.getByte(dispatchSlot[col]);
                    return Decimals.DECIMAL8_NULL;
                }
                if (code == DISPATCH_PREV_SLOT) {
                    if (hasPrevForCurrentGap) return prevRecord.getDecimal8(dispatchSlot[col]);
                    return Decimals.DECIMAL8_NULL;
                }
                if (code == DISPATCH_CONSTANT) return dispatchConstant[col].getDecimal8(null);
                return Decimals.DECIMAL8_NULL;
            }

            @Override
            public double getDouble(int col) {
                if (!isGapFilling) return baseRecord.getDouble(col);
                int code = dispatchCode[col];
                if (code == DISPATCH_KEY_SLOT) return keysMapRecord.getDouble(dispatchSlot[col]);
                if (code == DISPATCH_PREV_CACHE_SLOT) {
                    if (hasPrevForCurrentGap) return currentMapValue.getDouble(dispatchSlot[col]);
                    return Double.NaN;
                }
                if (code == DISPATCH_PREV_SLOT) {
                    if (hasPrevForCurrentGap) return prevRecord.getDouble(dispatchSlot[col]);
                    return Double.NaN;
                }
                if (code == DISPATCH_CONSTANT) return dispatchConstant[col].getDouble(null);
                return Double.NaN;
            }

            @Override
            public float getFloat(int col) {
                if (!isGapFilling) return baseRecord.getFloat(col);
                int code = dispatchCode[col];
                if (code == DISPATCH_KEY_SLOT) return keysMapRecord.getFloat(dispatchSlot[col]);
                if (code == DISPATCH_PREV_CACHE_SLOT) {
                    if (hasPrevForCurrentGap) return currentMapValue.getFloat(dispatchSlot[col]);
                    return Float.NaN;
                }
                if (code == DISPATCH_PREV_SLOT) {
                    if (hasPrevForCurrentGap) return prevRecord.getFloat(dispatchSlot[col]);
                    return Float.NaN;
                }
                if (code == DISPATCH_CONSTANT) return dispatchConstant[col].getFloat(null);
                return Float.NaN;
            }

            @Override
            public byte getGeoByte(int col) {
                if (!isGapFilling) return baseRecord.getGeoByte(col);
                int code = dispatchCode[col];
                if (code == DISPATCH_KEY_SLOT) return keysMapRecord.getGeoByte(dispatchSlot[col]);
                if (code == DISPATCH_PREV_CACHE_SLOT) {
                    if (hasPrevForCurrentGap) return currentMapValue.getByte(dispatchSlot[col]);
                    return GeoHashes.BYTE_NULL;
                }
                if (code == DISPATCH_PREV_SLOT) {
                    if (hasPrevForCurrentGap) return prevRecord.getGeoByte(dispatchSlot[col]);
                    return GeoHashes.BYTE_NULL;
                }
                if (code == DISPATCH_CONSTANT) return dispatchConstant[col].getGeoByte(null);
                return GeoHashes.BYTE_NULL;
            }

            @Override
            public int getGeoInt(int col) {
                if (!isGapFilling) return baseRecord.getGeoInt(col);
                int code = dispatchCode[col];
                if (code == DISPATCH_KEY_SLOT) return keysMapRecord.getGeoInt(dispatchSlot[col]);
                if (code == DISPATCH_PREV_CACHE_SLOT) {
                    if (hasPrevForCurrentGap) return currentMapValue.getInt(dispatchSlot[col]);
                    return GeoHashes.INT_NULL;
                }
                if (code == DISPATCH_PREV_SLOT) {
                    if (hasPrevForCurrentGap) return prevRecord.getGeoInt(dispatchSlot[col]);
                    return GeoHashes.INT_NULL;
                }
                if (code == DISPATCH_CONSTANT) return dispatchConstant[col].getGeoInt(null);
                return GeoHashes.INT_NULL;
            }

            @Override
            public long getGeoLong(int col) {
                if (!isGapFilling) return baseRecord.getGeoLong(col);
                int code = dispatchCode[col];
                if (code == DISPATCH_KEY_SLOT) return keysMapRecord.getGeoLong(dispatchSlot[col]);
                if (code == DISPATCH_PREV_CACHE_SLOT) {
                    if (hasPrevForCurrentGap) return currentMapValue.getLong(dispatchSlot[col]);
                    return GeoHashes.NULL;
                }
                if (code == DISPATCH_PREV_SLOT) {
                    if (hasPrevForCurrentGap) return prevRecord.getGeoLong(dispatchSlot[col]);
                    return GeoHashes.NULL;
                }
                if (code == DISPATCH_CONSTANT) return dispatchConstant[col].getGeoLong(null);
                return GeoHashes.NULL;
            }

            @Override
            public short getGeoShort(int col) {
                if (!isGapFilling) return baseRecord.getGeoShort(col);
                int code = dispatchCode[col];
                if (code == DISPATCH_KEY_SLOT) return keysMapRecord.getGeoShort(dispatchSlot[col]);
                if (code == DISPATCH_PREV_CACHE_SLOT) {
                    if (hasPrevForCurrentGap) return currentMapValue.getShort(dispatchSlot[col]);
                    return GeoHashes.SHORT_NULL;
                }
                if (code == DISPATCH_PREV_SLOT) {
                    if (hasPrevForCurrentGap) return prevRecord.getGeoShort(dispatchSlot[col]);
                    return GeoHashes.SHORT_NULL;
                }
                if (code == DISPATCH_CONSTANT) return dispatchConstant[col].getGeoShort(null);
                return GeoHashes.SHORT_NULL;
            }

            @Override
            public int getIPv4(int col) {
                if (!isGapFilling) return baseRecord.getIPv4(col);
                int code = dispatchCode[col];
                if (code == DISPATCH_KEY_SLOT) return keysMapRecord.getIPv4(dispatchSlot[col]);
                if (code == DISPATCH_PREV_CACHE_SLOT) {
                    if (hasPrevForCurrentGap) return currentMapValue.getIPv4(dispatchSlot[col]);
                    return Numbers.IPv4_NULL;
                }
                if (code == DISPATCH_PREV_SLOT) {
                    if (hasPrevForCurrentGap) return prevRecord.getIPv4(dispatchSlot[col]);
                    return Numbers.IPv4_NULL;
                }
                if (code == DISPATCH_CONSTANT) return dispatchConstant[col].getIPv4(null);
                return Numbers.IPv4_NULL;
            }

            @Override
            public int getInt(int col) {
                if (!isGapFilling) return baseRecord.getInt(col);
                int code = dispatchCode[col];
                if (code == DISPATCH_KEY_SLOT) return keysMapRecord.getInt(dispatchSlot[col]);
                if (code == DISPATCH_PREV_CACHE_SLOT) {
                    if (hasPrevForCurrentGap) return currentMapValue.getInt(dispatchSlot[col]);
                    return Numbers.INT_NULL;
                }
                if (code == DISPATCH_PREV_SLOT) {
                    if (hasPrevForCurrentGap) return prevRecord.getInt(dispatchSlot[col]);
                    return Numbers.INT_NULL;
                }
                if (code == DISPATCH_CONSTANT) return dispatchConstant[col].getInt(null);
                return Numbers.INT_NULL;
            }

            @Override
            public Interval getInterval(int col) {
                if (!isGapFilling) return baseRecord.getInterval(col);
                int code = dispatchCode[col];
                if (code == DISPATCH_KEY_SLOT) return keysMapRecord.getInterval(dispatchSlot[col]);
                if (code == DISPATCH_PREV_SLOT) {
                    if (hasPrevForCurrentGap) return prevRecord.getInterval(dispatchSlot[col]);
                    return Interval.NULL;
                }
                if (code == DISPATCH_CONSTANT) return dispatchConstant[col].getInterval(null);
                return Interval.NULL;
            }

            @Override
            public long getLong(int col) {
                if (!isGapFilling) return baseRecord.getLong(col);
                int code = dispatchCode[col];
                if (code == DISPATCH_KEY_SLOT) return keysMapRecord.getLong(dispatchSlot[col]);
                if (code == DISPATCH_PREV_CACHE_SLOT) {
                    if (hasPrevForCurrentGap) return currentMapValue.getLong(dispatchSlot[col]);
                    return Numbers.LONG_NULL;
                }
                if (code == DISPATCH_PREV_SLOT) {
                    if (hasPrevForCurrentGap) return prevRecord.getLong(dispatchSlot[col]);
                    return Numbers.LONG_NULL;
                }
                if (code == DISPATCH_CONSTANT) return dispatchConstant[col].getLong(null);
                return Numbers.LONG_NULL;
            }

            @Override
            public long getLong128Hi(int col) {
                if (!isGapFilling) return baseRecord.getLong128Hi(col);
                int code = dispatchCode[col];
                if (code == DISPATCH_KEY_SLOT) return keysMapRecord.getLong128Hi(dispatchSlot[col]);
                if (code == DISPATCH_PREV_SLOT) {
                    if (hasPrevForCurrentGap) return prevRecord.getLong128Hi(dispatchSlot[col]);
                    return Numbers.LONG_NULL;
                }
                if (code == DISPATCH_CONSTANT) return dispatchConstant[col].getLong128Hi(null);
                return Numbers.LONG_NULL;
            }

            @Override
            public long getLong128Lo(int col) {
                if (!isGapFilling) return baseRecord.getLong128Lo(col);
                int code = dispatchCode[col];
                if (code == DISPATCH_KEY_SLOT) return keysMapRecord.getLong128Lo(dispatchSlot[col]);
                if (code == DISPATCH_PREV_SLOT) {
                    if (hasPrevForCurrentGap) return prevRecord.getLong128Lo(dispatchSlot[col]);
                    return Numbers.LONG_NULL;
                }
                if (code == DISPATCH_CONSTANT) return dispatchConstant[col].getLong128Lo(null);
                return Numbers.LONG_NULL;
            }

            @Override
            public void getLong256(int col, CharSink<?> sink) {
                if (!isGapFilling) {
                    baseRecord.getLong256(col, sink);
                    return;
                }
                int code = dispatchCode[col];
                if (code == DISPATCH_KEY_SLOT) {
                    keysMapRecord.getLong256(dispatchSlot[col], sink);
                    return;
                }
                if (code == DISPATCH_PREV_SLOT) {
                    if (hasPrevForCurrentGap) prevRecord.getLong256(dispatchSlot[col], sink);
                    return;
                }
                if (code == DISPATCH_CONSTANT) {
                    dispatchConstant[col].getLong256(null, sink);
                }
                // Per the Record.getLong256(int, CharSink) contract: null Long256 appends nothing to
                // the sink. The caller owns the delimiters on both sides, and an empty segment reads
                // as an empty text value -- this is how QuestDB renders null Long256 in text output.
                // Do NOT call sink.clear() here: it would erase row-prefix content written by the
                // caller (e.g., CursorPrinter before the cell is rendered).
            }

            @Override
            public Long256 getLong256A(int col) {
                if (!isGapFilling) return baseRecord.getLong256A(col);
                int code = dispatchCode[col];
                if (code == DISPATCH_KEY_SLOT) return keysMapRecord.getLong256A(dispatchSlot[col]);
                if (code == DISPATCH_PREV_SLOT) {
                    if (hasPrevForCurrentGap) return prevRecord.getLong256A(dispatchSlot[col]);
                    return Long256Impl.NULL_LONG256;
                }
                if (code == DISPATCH_CONSTANT) return dispatchConstant[col].getLong256A(null);
                return Long256Impl.NULL_LONG256;
            }

            @Override
            public Long256 getLong256B(int col) {
                if (!isGapFilling) return baseRecord.getLong256B(col);
                int code = dispatchCode[col];
                if (code == DISPATCH_KEY_SLOT) return keysMapRecord.getLong256B(dispatchSlot[col]);
                if (code == DISPATCH_PREV_SLOT) {
                    if (hasPrevForCurrentGap) return prevRecord.getLong256B(dispatchSlot[col]);
                    return Long256Impl.NULL_LONG256;
                }
                if (code == DISPATCH_CONSTANT) return dispatchConstant[col].getLong256B(null);
                return Long256Impl.NULL_LONG256;
            }

            @Override
            public short getShort(int col) {
                if (!isGapFilling) return baseRecord.getShort(col);
                int code = dispatchCode[col];
                if (code == DISPATCH_KEY_SLOT) return keysMapRecord.getShort(dispatchSlot[col]);
                if (code == DISPATCH_PREV_CACHE_SLOT) {
                    if (hasPrevForCurrentGap) return currentMapValue.getShort(dispatchSlot[col]);
                    return 0;
                }
                if (code == DISPATCH_PREV_SLOT) {
                    if (hasPrevForCurrentGap) return prevRecord.getShort(dispatchSlot[col]);
                    return 0;
                }
                // Constant short fills go through getInt() to match the
                // narrow-integer convention used by Function implementations.
                if (code == DISPATCH_CONSTANT) return (short) dispatchConstant[col].getInt(null);
                return 0;
            }

            @Override
            public CharSequence getStrA(int col) {
                if (!isGapFilling) return baseRecord.getStrA(col);
                int code = dispatchCode[col];
                if (code == DISPATCH_KEY_SLOT) return keysMapRecord.getStrA(dispatchSlot[col]);
                if (code == DISPATCH_PREV_SLOT) {
                    if (hasPrevForCurrentGap) return prevRecord.getStrA(dispatchSlot[col]);
                    return null;
                }
                if (code == DISPATCH_CONSTANT) return dispatchConstant[col].getStrA(null);
                return null;
            }

            @Override
            public CharSequence getStrB(int col) {
                if (!isGapFilling) return baseRecord.getStrB(col);
                int code = dispatchCode[col];
                if (code == DISPATCH_KEY_SLOT) return keysMapRecord.getStrB(dispatchSlot[col]);
                if (code == DISPATCH_PREV_SLOT) {
                    if (hasPrevForCurrentGap) return prevRecord.getStrB(dispatchSlot[col]);
                    return null;
                }
                if (code == DISPATCH_CONSTANT) return dispatchConstant[col].getStrB(null);
                return null;
            }

            @Override
            public int getStrLen(int col) {
                if (!isGapFilling) return baseRecord.getStrLen(col);
                int code = dispatchCode[col];
                if (code == DISPATCH_KEY_SLOT) return keysMapRecord.getStrLen(dispatchSlot[col]);
                if (code == DISPATCH_PREV_SLOT) {
                    if (hasPrevForCurrentGap) return prevRecord.getStrLen(dispatchSlot[col]);
                    return -1;
                }
                if (code == DISPATCH_CONSTANT) return dispatchConstant[col].getStrLen(null);
                return -1;
            }

            @Override
            public CharSequence getSymA(int col) {
                if (!isGapFilling) return baseRecord.getSymA(col);
                int code = dispatchCode[col];
                if (code == DISPATCH_KEY_SLOT) {
                    // Cached SymbolTable + direct slot read shortcuts the
                    // MapRecord setSymbolTableResolver chain.
                    return symbolCache[col].valueOf(keysMapRecord.getInt(dispatchSlot[col]));
                }
                if (code == DISPATCH_PREV_CACHE_SLOT) {
                    if (hasPrevForCurrentGap) {
                        return symbolCache[col].valueOf(currentMapValue.getInt(dispatchSlot[col]));
                    }
                    return null;
                }
                if (code == DISPATCH_PREV_SLOT) {
                    if (hasPrevForCurrentGap) return prevRecord.getSymA(dispatchSlot[col]);
                    return null;
                }
                // Constant symbol fills dispatch through Function.getSymbol()
                // rather than Function.getSymA() by historical convention.
                if (code == DISPATCH_CONSTANT) return dispatchConstant[col].getSymbol(null);
                return null;
            }

            @Override
            public CharSequence getSymB(int col) {
                if (!isGapFilling) return baseRecord.getSymB(col);
                int code = dispatchCode[col];
                if (code == DISPATCH_KEY_SLOT) {
                    return symbolCache[col].valueBOf(keysMapRecord.getInt(dispatchSlot[col]));
                }
                if (code == DISPATCH_PREV_CACHE_SLOT) {
                    if (hasPrevForCurrentGap) {
                        return symbolCache[col].valueBOf(currentMapValue.getInt(dispatchSlot[col]));
                    }
                    return null;
                }
                if (code == DISPATCH_PREV_SLOT) {
                    if (hasPrevForCurrentGap) return prevRecord.getSymB(dispatchSlot[col]);
                    return null;
                }
                if (code == DISPATCH_CONSTANT) return dispatchConstant[col].getSymbolB(null);
                return null;
            }

            @Override
            public long getTimestamp(int col) {
                if (!isGapFilling) return baseRecord.getTimestamp(col);
                int code = dispatchCode[col];
                if (code == DISPATCH_TIMESTAMP_FILL) return fillTimestampFunc.value;
                if (code == DISPATCH_KEY_SLOT) return keysMapRecord.getTimestamp(dispatchSlot[col]);
                if (code == DISPATCH_PREV_CACHE_SLOT) {
                    if (hasPrevForCurrentGap) return currentMapValue.getTimestamp(dispatchSlot[col]);
                    return Numbers.LONG_NULL;
                }
                if (code == DISPATCH_PREV_SLOT) {
                    if (hasPrevForCurrentGap) return prevRecord.getTimestamp(dispatchSlot[col]);
                    return Numbers.LONG_NULL;
                }
                if (code == DISPATCH_CONSTANT) return dispatchConstant[col].getTimestamp(null);
                return Numbers.LONG_NULL;
            }

            @Override
            public Utf8Sequence getVarcharA(int col) {
                if (!isGapFilling) return baseRecord.getVarcharA(col);
                int code = dispatchCode[col];
                if (code == DISPATCH_KEY_SLOT) return keysMapRecord.getVarcharA(dispatchSlot[col]);
                if (code == DISPATCH_PREV_SLOT) {
                    if (hasPrevForCurrentGap) return prevRecord.getVarcharA(dispatchSlot[col]);
                    return null;
                }
                if (code == DISPATCH_CONSTANT) return dispatchConstant[col].getVarcharA(null);
                return null;
            }

            @Override
            public Utf8Sequence getVarcharB(int col) {
                if (!isGapFilling) return baseRecord.getVarcharB(col);
                int code = dispatchCode[col];
                if (code == DISPATCH_KEY_SLOT) return keysMapRecord.getVarcharB(dispatchSlot[col]);
                if (code == DISPATCH_PREV_SLOT) {
                    if (hasPrevForCurrentGap) return prevRecord.getVarcharB(dispatchSlot[col]);
                    return null;
                }
                if (code == DISPATCH_CONSTANT) return dispatchConstant[col].getVarcharB(null);
                return null;
            }

            @Override
            public int getVarcharSize(int col) {
                if (!isGapFilling) return baseRecord.getVarcharSize(col);
                int code = dispatchCode[col];
                if (code == DISPATCH_KEY_SLOT) return keysMapRecord.getVarcharSize(dispatchSlot[col]);
                if (code == DISPATCH_PREV_SLOT) {
                    if (hasPrevForCurrentGap) return prevRecord.getVarcharSize(dispatchSlot[col]);
                    return -1;
                }
                if (code == DISPATCH_CONSTANT) return dispatchConstant[col].getVarcharSize(null);
                return -1;
            }
        }

        private static class FillTimestampConstant extends TimestampFunction implements ConstantFunction {
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
