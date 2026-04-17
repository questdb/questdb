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
import io.questdb.cairo.GeoHashes;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.RecordSink;
import io.questdb.cairo.TimestampDriver;
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
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.TimestampFunction;
import io.questdb.griffin.engine.functions.constants.ConstantFunction;
import io.questdb.griffin.engine.functions.constants.NullConstant;
import io.questdb.std.IntList;
import io.questdb.std.Decimals;
import io.questdb.std.Long256Impl;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
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
            IntList prevSourceCols,
            RecordSink keySink,
            ArrayColumnTypes mapKeyTypes,
            ArrayColumnTypes mapValueTypes,
            IntList keyColIndices,
            IntList symbolTableColIndices,
            long calendarOffset
    ) {
        super(metadata);
        Map keysMapLocal = null;
        SampleByFillCursor cursorLocal;
        try {
            if (keyColIndices.size() > 0) {
                keysMapLocal = MapFactory.createOrderedMap(configuration, mapKeyTypes, mapValueTypes);
            }
            cursorLocal = new SampleByFillCursor(
                    configuration, metadata, timestampSampler,
                    fromFunc, toFunc, fillModes, constantFills,
                    timestampIndex, timestampType, prevSourceCols,
                    keySink, keysMapLocal, keyColIndices, symbolTableColIndices,
                    calendarOffset
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
        this.hasPrevFill = prevSourceCols != null && prevSourceCols.size() > 0;
        this.keysMap = keysMapLocal;
        this.cursor = cursorLocal;
    }

    @Override
    public boolean followedOrderByAdvice() {
        return false;
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
            // Defensive: cursor.of() currently only calls Function.init() and toTop(),
            // which do not throw on the happy path; kept to maintain the cursor-contract.
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
        if (hasPrevFill) {
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
        base.close();
        Misc.free(fromFunc);
        Misc.free(toFunc);
        Misc.freeObjList(constantFills);
        Misc.free(keysMap);
    }

    private boolean hasAnyConstantFill() {
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

    private static class SampleByFillCursor implements NoRandomAccessRecordCursor {
        private static final int HAS_PREV_SLOT = 1;
        private static final int KEY_INDEX_SLOT = 0;
        private static final int PREV_START_SLOT = 2;

        private final long calendarOffset;
        private final int columnCount;
        private final short[] columnTypes;
        private final ObjList<Function> constantFills;
        private final IntList fillModes;
        private final FillRecord fillRecord = new FillRecord();
        private final FillTimestampConstant fillTimestampFunc;
        private final Function fromFunc;
        private final boolean hasPrevFill;
        private final IntList keyColIndices;
        private final int keyPosOffset;
        private final RecordSink keySink;
        private final Map keysMap;
        private final int[] outputColToAggSlot;
        private final int[] outputColToKeyPos;
        private final IntList prevSourceCols;
        private final IntList symbolTableColIndices;
        private final TimestampDriver timestampDriver;
        private final int timestampIndex;
        private final TimestampSampler timestampSampler;
        private final Function toFunc;
        private RecordCursor baseCursor;
        private Record baseRecord;
        private boolean hasDataForCurrentBucket;
        private boolean hasExplicitTo;
        private boolean hasPendingRow;
        private boolean hasSimplePrev;
        private boolean isBaseCursorExhausted;
        private boolean isEmittingFills;
        private boolean isInitialized;
        private int keyCount;
        private boolean[] keyPresent;
        private MapRecordCursor keysMapCursor;
        private MapRecord keysMapRecord;
        private long maxTimestamp;
        private long nextBucketTimestamp;
        private long pendingTs;
        private long[] simplePrev;

        private SampleByFillCursor(
                CairoConfiguration configuration,
                RecordMetadata metadata,
                TimestampSampler timestampSampler,
                @NotNull Function fromFunc,
                @NotNull Function toFunc,
                IntList fillModes,
                ObjList<Function> constantFills,
                int timestampIndex,
                int timestampType,
                IntList prevSourceCols,
                RecordSink keySink,
                Map keysMap,
                IntList keyColIndices,
                IntList symbolTableColIndices,
                long calendarOffset
        ) {
            this.calendarOffset = calendarOffset;
            this.timestampSampler = timestampSampler;
            this.fromFunc = fromFunc;
            this.toFunc = toFunc;
            this.fillModes = fillModes;
            this.constantFills = constantFills;
            this.timestampIndex = timestampIndex;
            this.timestampDriver = ColumnType.getTimestampDriver(timestampType);
            this.columnCount = metadata.getColumnCount();
            this.fillTimestampFunc = new FillTimestampConstant(timestampType);
            this.prevSourceCols = prevSourceCols;
            this.hasPrevFill = prevSourceCols != null && prevSourceCols.size() > 0;
            this.columnTypes = new short[columnCount];
            for (int i = 0; i < columnCount; i++) {
                columnTypes[i] = ColumnType.tagOf(metadata.getColumnType(i));
            }
            this.keySink = keySink;
            this.keysMap = keysMap;
            this.keyColIndices = keyColIndices;
            this.symbolTableColIndices = symbolTableColIndices;
            this.simplePrev = hasPrevFill ? new long[columnCount] : null;

            // Build outputColToKeyPos and outputColToAggSlot mappings.
            // keyPosOffset compensates for value columns preceding key columns
            // in the MapRecord column index space: value cols are at indices
            // 0..valueColCount-1, key cols at valueColCount..valueColCount+keyCount-1.
            this.outputColToKeyPos = new int[columnCount];
            this.outputColToAggSlot = new int[columnCount];
            Arrays.fill(outputColToKeyPos, -1);
            Arrays.fill(outputColToAggSlot, -1);
            int aggSlot = 0;
            for (int col = 0; col < columnCount; col++) {
                if (fillModes.getQuick(col) != FILL_KEY && col != timestampIndex) {
                    outputColToAggSlot[col] = aggSlot++;
                }
            }
            // keyPosOffset = 2 (keyIndex + hasPrev) + aggColumnCount
            this.keyPosOffset = 2 + aggSlot;
            for (int i = 0, n = keyColIndices.size(); i < n; i++) {
                outputColToKeyPos[keyColIndices.getQuick(i)] = keyPosOffset + i;
            }
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
            if (!isInitialized) {
                initialize();
                isInitialized = true;
            }

            // If we're in the middle of emitting fill rows for absent keys
            if (isEmittingFills) {
                if (emitNextFillRow()) {
                    return true;
                }
                // emitNextFillRow exhausted gap buckets — fall through to main loop
            }

            while (nextBucketTimestamp < maxTimestamp) {
                // Try to get the next data row's timestamp
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

                // Base cursor exhausted and no explicit TO bound
                if (isBaseCursorExhausted && !hasExplicitTo) {
                    // Emit fills for remaining absent keys in current bucket
                    if (hasDataForCurrentBucket && keysMap != null) {
                        isEmittingFills = true;
                        keysMapCursor.toTop();
                        return emitNextFillRow();
                    }
                    return false;
                }

                if (dataTs == nextBucketTimestamp) {
                    // Data row at expected bucket
                    hasPendingRow = false;
                    fillRecord.isGapFilling = false;
                    if (keysMap != null) {
                        // Find this key in keysMap, mark present, update prev
                        hasDataForCurrentBucket = true;
                        MapKey mapKey = keysMap.withKey();
                        keySink.copy(baseRecord, mapKey);
                        MapValue value = mapKey.findValue();
                        assert value != null : "key discovered in pass 1 must exist in keysMap";
                        int keyIdx = (int) value.getLong(KEY_INDEX_SLOT);
                        keyPresent[keyIdx] = true;
                        if (hasPrevFill) {
                            updatePerKeyPrev(value, baseRecord);
                        }
                    } else {
                        // Non-keyed: only one row per bucket, advance immediately
                        if (hasPrevFill) {
                            savePrevValues(baseRecord);
                        }
                        nextBucketTimestamp = timestampSampler.nextTimestamp(nextBucketTimestamp);
                    }
                    return true;
                }

                if (dataTs > nextBucketTimestamp) {
                    // Gap — need to emit fill rows before advancing bucket
                    if (hasDataForCurrentBucket && keysMap != null) {
                        // Emit fills for absent keys in the current bucket
                        isEmittingFills = true;
                        keysMapCursor.toTop();
                        if (emitNextFillRow()) {
                            return true;
                        }
                        continue; // gap fills exhausted, continue main loop
                    }

                    if (keysMap != null && keyCount > 0) {
                        // This bucket has NO data at all — emit fills for all keys
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
                    nextBucketTimestamp = timestampSampler.nextTimestamp(nextBucketTimestamp);
                    hasDataForCurrentBucket = false;
                    return true;
                }

                if (dataTs < nextBucketTimestamp && hasPendingRow) {
                    // Defensive: the async GROUP BY upstream emits exactly one row per
                    // (bucket, key), so this branch is unreachable in practice. Kept to
                    // preserve the cursor-contract semantics around DST fall-back.
                    hasPendingRow = false;
                    fillRecord.isGapFilling = false;
                    if (hasPrevFill) {
                        if (keysMap != null) {
                            MapKey mapKey = keysMap.withKey();
                            keySink.copy(baseRecord, mapKey);
                            MapValue value = mapKey.findValue();
                            if (value != null) {
                                updatePerKeyPrev(value, baseRecord);
                            }
                        } else {
                            savePrevValues(baseRecord);
                        }
                    }
                    return true;
                }
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
            hasPendingRow = false;
            isBaseCursorExhausted = false;
            hasExplicitTo = false;
            hasDataForCurrentBucket = false;
            isEmittingFills = false;
        }

        private boolean emitNextFillRow() {
            while (true) {
                // Inner loop: scan remaining keys in current bucket
                while (keysMapCursor.hasNext()) {
                    MapValue value = keysMapRecord.getValue();
                    int keyIdx = (int) value.getLong(KEY_INDEX_SLOT);
                    if (!keyPresent[keyIdx]) {
                        fillRecord.isGapFilling = true;
                        fillTimestampFunc.value = nextBucketTimestamp;
                        return true;
                    }
                }
                // Bucket exhausted — advance
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
                keysMapRecord = (MapRecord) keysMap.getRecord();
                keysMapRecord.setSymbolTableResolver(baseCursor, symbolTableColIndices);
                keysMapCursor = keysMap.getCursor();
            } else {
                // Non-keyed: degenerate case with 1 "empty" key
                keyCount = 1;
                if (keyPresent == null || keyPresent.length < 1) {
                    keyPresent = new boolean[1];
                }
            }

            // Peek first row to determine range
            if (baseCursor.hasNext()) {
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
            Function.init(constantFills, baseCursor, executionContext, null);
            fromFunc.init(baseCursor, executionContext);
            toFunc.init(baseCursor, executionContext);
            toTop();
        }

        private long prevValue(int col) {
            if (keysMap != null) {
                MapValue value = keysMapRecord.getValue();
                boolean hasPrev = value.getLong(HAS_PREV_SLOT) != 0;
                if (!hasPrev) {
                    return Numbers.LONG_NULL;
                }
                int mode = fillMode(col);
                int sourceCol = mode >= 0 ? mode : col;
                int aggSlot = outputColToAggSlot[sourceCol];
                if (aggSlot >= 0) {
                    return value.getLong(PREV_START_SLOT + aggSlot);
                }
                return Numbers.LONG_NULL;
            }
            // Non-keyed fallback
            if (hasSimplePrev) {
                int mode = fillMode(col);
                if (mode == FILL_PREV_SELF) {
                    return simplePrev[col];
                }
                if (mode >= 0) {
                    return simplePrev[mode];
                }
            }
            return Numbers.LONG_NULL;
        }

        private void savePrevValues(Record record) {
            hasSimplePrev = true;
            for (int i = 0, n = prevSourceCols.size(); i < n; i++) {
                int col = prevSourceCols.getQuick(i);
                simplePrev[col] = readColumnAsLongBits(record, col, columnTypes[col]);
            }
        }

        private static long readColumnAsLongBits(Record record, int col, short type) {
            return switch (type) {
                case ColumnType.DOUBLE -> Double.doubleToRawLongBits(record.getDouble(col));
                case ColumnType.FLOAT -> Float.floatToRawIntBits(record.getFloat(col));
                case ColumnType.INT, ColumnType.IPv4, ColumnType.GEOINT -> record.getInt(col);
                case ColumnType.SHORT, ColumnType.GEOSHORT -> record.getShort(col);
                case ColumnType.BYTE, ColumnType.GEOBYTE -> record.getByte(col);
                case ColumnType.BOOLEAN -> record.getBool(col) ? 1 : 0;
                case ColumnType.CHAR -> record.getChar(col);
                case ColumnType.DATE, ColumnType.LONG, ColumnType.TIMESTAMP -> record.getLong(col);
                case ColumnType.GEOLONG -> record.getGeoLong(col);
                case ColumnType.DECIMAL8 -> record.getDecimal8(col);
                case ColumnType.DECIMAL16 -> record.getDecimal16(col);
                case ColumnType.DECIMAL32 -> record.getDecimal32(col);
                case ColumnType.DECIMAL64 -> record.getDecimal64(col);
                // Defensive: the optimizer gate rejects unsupported PREV source types
                // before reaching here and routes those queries to the legacy path.
                default ->
                        throw new UnsupportedOperationException("unsupported column type for PREV fill: " + ColumnType.nameOf(type));
            };
        }

        private void updatePerKeyPrev(MapValue value, Record record) {
            value.putLong(HAS_PREV_SLOT, 1L);
            for (int i = 0, n = prevSourceCols.size(); i < n; i++) {
                int col = prevSourceCols.getQuick(i);
                int aggSlot = outputColToAggSlot[col];
                if (aggSlot >= 0) {
                    value.putLong(PREV_START_SLOT + aggSlot,
                            readColumnAsLongBits(record, col, columnTypes[col]));
                }
            }
        }

        private boolean hasKeyPrev() {
            if (keysMap != null) {
                return keysMapRecord.getValue().getLong(HAS_PREV_SLOT) != 0;
            }
            return hasSimplePrev;
        }

        private int fillMode(int col) {
            return fillModes.getQuick(col);
        }

        /**
         * The default null/0/NaN returns at the tail of each getter are defensive:
         * the GROUP BY pipeline always supplies a fill mode (FILL_KEY, FILL_PREV_SELF,
         * cross-column, or FILL_CONSTANT) for every output column, so those branches
         * are unreachable in practice.
         */
        private class FillRecord implements Record {
            boolean isGapFilling;

            @Override
            public double getDouble(int col) {
                if (!isGapFilling) return baseRecord.getDouble(col);
                int mode = fillMode(col);
                if (mode == FILL_KEY) return keysMapRecord.getDouble(outputColToKeyPos[col]);
                if (mode >= 0 && outputColToKeyPos[mode] >= 0)
                    return keysMapRecord.getDouble(outputColToKeyPos[mode]);
                if ((mode == FILL_PREV_SELF || mode >= 0) && hasKeyPrev())
                    return Double.longBitsToDouble(prevValue(col));
                if (mode == FILL_CONSTANT) return constantFills.getQuick(col).getDouble(null);
                return Double.NaN;
            }

            @Override
            public float getFloat(int col) {
                if (!isGapFilling) return baseRecord.getFloat(col);
                int mode = fillMode(col);
                if (mode == FILL_KEY) return keysMapRecord.getFloat(outputColToKeyPos[col]);
                if (mode >= 0 && outputColToKeyPos[mode] >= 0)
                    return keysMapRecord.getFloat(outputColToKeyPos[mode]);
                if ((mode == FILL_PREV_SELF || mode >= 0) && hasKeyPrev())
                    return Float.intBitsToFloat((int) prevValue(col));
                if (mode == FILL_CONSTANT) return constantFills.getQuick(col).getFloat(null);
                return Float.NaN;
            }

            @Override
            public int getInt(int col) {
                if (!isGapFilling) return baseRecord.getInt(col);
                int mode = fillMode(col);
                if (mode == FILL_KEY) return keysMapRecord.getInt(outputColToKeyPos[col]);
                if (mode >= 0 && outputColToKeyPos[mode] >= 0)
                    return keysMapRecord.getInt(outputColToKeyPos[mode]);
                if ((mode == FILL_PREV_SELF || mode >= 0) && hasKeyPrev()) return (int) prevValue(col);
                if (mode == FILL_CONSTANT) return constantFills.getQuick(col).getInt(null);
                return Numbers.INT_NULL;
            }

            @Override
            public long getLong(int col) {
                if (!isGapFilling) return baseRecord.getLong(col);
                int mode = fillMode(col);
                if (mode == FILL_KEY) return keysMapRecord.getLong(outputColToKeyPos[col]);
                if (mode >= 0 && outputColToKeyPos[mode] >= 0)
                    return keysMapRecord.getLong(outputColToKeyPos[mode]);
                if ((mode == FILL_PREV_SELF || mode >= 0) && hasKeyPrev()) return prevValue(col);
                if (mode == FILL_CONSTANT) return constantFills.getQuick(col).getLong(null);
                return Numbers.LONG_NULL;
            }

            @Override
            public short getShort(int col) {
                if (!isGapFilling) return baseRecord.getShort(col);
                int mode = fillMode(col);
                if (mode == FILL_KEY) return keysMapRecord.getShort(outputColToKeyPos[col]);
                if (mode >= 0 && outputColToKeyPos[mode] >= 0)
                    return keysMapRecord.getShort(outputColToKeyPos[mode]);
                if ((mode == FILL_PREV_SELF || mode >= 0) && hasKeyPrev()) return (short) prevValue(col);
                if (mode == FILL_CONSTANT) return (short) constantFills.getQuick(col).getInt(null);
                return 0;
            }

            @Override
            public byte getByte(int col) {
                if (!isGapFilling) return baseRecord.getByte(col);
                int mode = fillMode(col);
                if (mode == FILL_KEY) return keysMapRecord.getByte(outputColToKeyPos[col]);
                if (mode >= 0 && outputColToKeyPos[mode] >= 0)
                    return keysMapRecord.getByte(outputColToKeyPos[mode]);
                if ((mode == FILL_PREV_SELF || mode >= 0) && hasKeyPrev()) return (byte) prevValue(col);
                if (mode == FILL_CONSTANT) return (byte) constantFills.getQuick(col).getInt(null);
                return 0;
            }

            @Override
            public boolean getBool(int col) {
                if (!isGapFilling) return baseRecord.getBool(col);
                int mode = fillMode(col);
                if (mode == FILL_KEY) return keysMapRecord.getBool(outputColToKeyPos[col]);
                if (mode >= 0 && outputColToKeyPos[mode] >= 0)
                    return keysMapRecord.getBool(outputColToKeyPos[mode]);
                if ((mode == FILL_PREV_SELF || mode >= 0) && hasKeyPrev()) return prevValue(col) != 0;
                if (mode == FILL_CONSTANT) return constantFills.getQuick(col).getBool(null);
                return false;
            }

            @Override
            public char getChar(int col) {
                if (!isGapFilling) return baseRecord.getChar(col);
                int mode = fillMode(col);
                if (mode == FILL_KEY) return keysMapRecord.getChar(outputColToKeyPos[col]);
                if (mode >= 0 && outputColToKeyPos[mode] >= 0)
                    return keysMapRecord.getChar(outputColToKeyPos[mode]);
                if ((mode == FILL_PREV_SELF || mode >= 0) && hasKeyPrev()) return (char) prevValue(col);
                if (mode == FILL_CONSTANT) return constantFills.getQuick(col).getChar(null);
                return 0;
            }

            @Override
            public long getTimestamp(int col) {
                if (!isGapFilling) return baseRecord.getTimestamp(col);
                if (col == timestampIndex) return fillTimestampFunc.value;
                int mode = fillMode(col);
                if (mode == FILL_KEY) return keysMapRecord.getTimestamp(outputColToKeyPos[col]);
                if (mode >= 0 && outputColToKeyPos[mode] >= 0)
                    return keysMapRecord.getTimestamp(outputColToKeyPos[mode]);
                if ((mode == FILL_PREV_SELF || mode >= 0) && hasKeyPrev()) return prevValue(col);
                if (mode == FILL_CONSTANT) return constantFills.getQuick(col).getTimestamp(null);
                return Numbers.LONG_NULL;
            }

            @Override
            public io.questdb.cairo.arr.ArrayView getArray(int col, int columnType) {
                if (!isGapFilling) return baseRecord.getArray(col, columnType);
                if (fillMode(col) == FILL_CONSTANT) return constantFills.getQuick(col).getArray(null);
                return null;
            }

            @Override
            public io.questdb.std.BinarySequence getBin(int col) {
                if (!isGapFilling) return baseRecord.getBin(col);
                if (fillMode(col) == FILL_CONSTANT) return constantFills.getQuick(col).getBin(null);
                return null;
            }

            @Override
            public long getBinLen(int col) {
                if (!isGapFilling) return baseRecord.getBinLen(col);
                if (fillMode(col) == FILL_CONSTANT) return constantFills.getQuick(col).getBinLen(null);
                return -1;
            }

            @Override
            public void getDecimal128(int col, io.questdb.std.Decimal128 sink) {
                if (!isGapFilling) {
                    baseRecord.getDecimal128(col, sink);
                    return;
                }
                int mode = fillMode(col);
                if (mode == FILL_KEY) {
                    keysMapRecord.getDecimal128(outputColToKeyPos[col], sink);
                    return;
                }
                if (mode >= 0 && outputColToKeyPos[mode] >= 0) {
                    keysMapRecord.getDecimal128(outputColToKeyPos[mode], sink);
                    return;
                }
                if (mode == FILL_CONSTANT) {
                    constantFills.getQuick(col).getDecimal128(null, sink);
                }
            }

            @Override
            public short getDecimal16(int col) {
                if (!isGapFilling) return baseRecord.getDecimal16(col);
                int mode = fillMode(col);
                if (mode == FILL_KEY) return keysMapRecord.getDecimal16(outputColToKeyPos[col]);
                if (mode >= 0 && outputColToKeyPos[mode] >= 0)
                    return keysMapRecord.getDecimal16(outputColToKeyPos[mode]);
                if ((mode == FILL_PREV_SELF || mode >= 0) && hasKeyPrev()) return (short) prevValue(col);
                if (mode == FILL_CONSTANT) return constantFills.getQuick(col).getDecimal16(null);
                return Decimals.DECIMAL16_NULL;
            }

            @Override
            public void getDecimal256(int col, io.questdb.std.Decimal256 sink) {
                if (!isGapFilling) {
                    baseRecord.getDecimal256(col, sink);
                    return;
                }
                int mode = fillMode(col);
                if (mode == FILL_KEY) {
                    keysMapRecord.getDecimal256(outputColToKeyPos[col], sink);
                    return;
                }
                if (mode >= 0 && outputColToKeyPos[mode] >= 0) {
                    keysMapRecord.getDecimal256(outputColToKeyPos[mode], sink);
                    return;
                }
                if (mode == FILL_CONSTANT) {
                    constantFills.getQuick(col).getDecimal256(null, sink);
                }
            }

            @Override
            public int getDecimal32(int col) {
                if (!isGapFilling) return baseRecord.getDecimal32(col);
                int mode = fillMode(col);
                if (mode == FILL_KEY) return keysMapRecord.getDecimal32(outputColToKeyPos[col]);
                if (mode >= 0 && outputColToKeyPos[mode] >= 0)
                    return keysMapRecord.getDecimal32(outputColToKeyPos[mode]);
                if ((mode == FILL_PREV_SELF || mode >= 0) && hasKeyPrev()) return (int) prevValue(col);
                if (mode == FILL_CONSTANT) return constantFills.getQuick(col).getDecimal32(null);
                return Decimals.DECIMAL32_NULL;
            }

            @Override
            public long getDecimal64(int col) {
                if (!isGapFilling) return baseRecord.getDecimal64(col);
                int mode = fillMode(col);
                if (mode == FILL_KEY) return keysMapRecord.getDecimal64(outputColToKeyPos[col]);
                if (mode >= 0 && outputColToKeyPos[mode] >= 0)
                    return keysMapRecord.getDecimal64(outputColToKeyPos[mode]);
                if ((mode == FILL_PREV_SELF || mode >= 0) && hasKeyPrev()) return prevValue(col);
                if (mode == FILL_CONSTANT) return constantFills.getQuick(col).getDecimal64(null);
                return Decimals.DECIMAL64_NULL;
            }

            @Override
            public byte getDecimal8(int col) {
                if (!isGapFilling) return baseRecord.getDecimal8(col);
                int mode = fillMode(col);
                if (mode == FILL_KEY) return keysMapRecord.getDecimal8(outputColToKeyPos[col]);
                if (mode >= 0 && outputColToKeyPos[mode] >= 0)
                    return keysMapRecord.getDecimal8(outputColToKeyPos[mode]);
                if ((mode == FILL_PREV_SELF || mode >= 0) && hasKeyPrev()) return (byte) prevValue(col);
                if (mode == FILL_CONSTANT) return constantFills.getQuick(col).getDecimal8(null);
                return Decimals.DECIMAL8_NULL;
            }

            @Override
            public byte getGeoByte(int col) {
                if (!isGapFilling) return baseRecord.getGeoByte(col);
                int mode = fillMode(col);
                if (mode == FILL_KEY) return keysMapRecord.getGeoByte(outputColToKeyPos[col]);
                if (mode >= 0 && outputColToKeyPos[mode] >= 0)
                    return keysMapRecord.getGeoByte(outputColToKeyPos[mode]);
                if ((mode == FILL_PREV_SELF || mode >= 0) && hasKeyPrev()) return (byte) prevValue(col);
                if (mode == FILL_CONSTANT) return constantFills.getQuick(col).getGeoByte(null);
                return GeoHashes.BYTE_NULL;
            }

            @Override
            public int getGeoInt(int col) {
                if (!isGapFilling) return baseRecord.getGeoInt(col);
                int mode = fillMode(col);
                if (mode == FILL_KEY) return keysMapRecord.getGeoInt(outputColToKeyPos[col]);
                if (mode >= 0 && outputColToKeyPos[mode] >= 0)
                    return keysMapRecord.getGeoInt(outputColToKeyPos[mode]);
                if ((mode == FILL_PREV_SELF || mode >= 0) && hasKeyPrev()) return (int) prevValue(col);
                if (mode == FILL_CONSTANT) return constantFills.getQuick(col).getGeoInt(null);
                return GeoHashes.INT_NULL;
            }

            @Override
            public long getGeoLong(int col) {
                if (!isGapFilling) return baseRecord.getGeoLong(col);
                int mode = fillMode(col);
                if (mode == FILL_KEY) return keysMapRecord.getGeoLong(outputColToKeyPos[col]);
                if (mode >= 0 && outputColToKeyPos[mode] >= 0)
                    return keysMapRecord.getGeoLong(outputColToKeyPos[mode]);
                if ((mode == FILL_PREV_SELF || mode >= 0) && hasKeyPrev()) return prevValue(col);
                if (mode == FILL_CONSTANT) return constantFills.getQuick(col).getGeoLong(null);
                return GeoHashes.NULL;
            }

            @Override
            public short getGeoShort(int col) {
                if (!isGapFilling) return baseRecord.getGeoShort(col);
                int mode = fillMode(col);
                if (mode == FILL_KEY) return keysMapRecord.getGeoShort(outputColToKeyPos[col]);
                if (mode >= 0 && outputColToKeyPos[mode] >= 0)
                    return keysMapRecord.getGeoShort(outputColToKeyPos[mode]);
                if ((mode == FILL_PREV_SELF || mode >= 0) && hasKeyPrev()) return (short) prevValue(col);
                if (mode == FILL_CONSTANT) return constantFills.getQuick(col).getGeoShort(null);
                return GeoHashes.SHORT_NULL;
            }

            @Override
            public int getIPv4(int col) {
                if (!isGapFilling) return baseRecord.getIPv4(col);
                int mode = fillMode(col);
                if (mode == FILL_KEY) return keysMapRecord.getIPv4(outputColToKeyPos[col]);
                if (mode >= 0 && outputColToKeyPos[mode] >= 0)
                    return keysMapRecord.getIPv4(outputColToKeyPos[mode]);
                if ((mode == FILL_PREV_SELF || mode >= 0) && hasKeyPrev()) return (int) prevValue(col);
                if (mode == FILL_CONSTANT) return constantFills.getQuick(col).getIPv4(null);
                return Numbers.IPv4_NULL;
            }

            @Override
            public long getLong128Hi(int col) {
                if (!isGapFilling) return baseRecord.getLong128Hi(col);
                int mode = fillMode(col);
                if (mode == FILL_KEY) return keysMapRecord.getLong128Hi(outputColToKeyPos[col]);
                if (mode >= 0 && outputColToKeyPos[mode] >= 0)
                    return keysMapRecord.getLong128Hi(outputColToKeyPos[mode]);
                if (mode == FILL_CONSTANT) return constantFills.getQuick(col).getLong128Hi(null);
                return Numbers.LONG_NULL;
            }

            @Override
            public long getLong128Lo(int col) {
                if (!isGapFilling) return baseRecord.getLong128Lo(col);
                int mode = fillMode(col);
                if (mode == FILL_KEY) return keysMapRecord.getLong128Lo(outputColToKeyPos[col]);
                if (mode >= 0 && outputColToKeyPos[mode] >= 0)
                    return keysMapRecord.getLong128Lo(outputColToKeyPos[mode]);
                if (mode == FILL_CONSTANT) return constantFills.getQuick(col).getLong128Lo(null);
                return Numbers.LONG_NULL;
            }

            @Override
            public void getLong256(int col, io.questdb.std.str.CharSink<?> sink) {
                if (!isGapFilling) {
                    baseRecord.getLong256(col, sink);
                    return;
                }
                int mode = fillMode(col);
                if (mode == FILL_KEY) {
                    keysMapRecord.getLong256(outputColToKeyPos[col], sink);
                    return;
                }
                if (mode >= 0 && outputColToKeyPos[mode] >= 0) {
                    keysMapRecord.getLong256(outputColToKeyPos[mode], sink);
                    return;
                }
                if (mode == FILL_CONSTANT) {
                    constantFills.getQuick(col).getLong256(null, sink);
                }
            }

            @Override
            public io.questdb.std.Long256 getLong256A(int col) {
                if (!isGapFilling) return baseRecord.getLong256A(col);
                int mode = fillMode(col);
                if (mode == FILL_KEY) return keysMapRecord.getLong256A(outputColToKeyPos[col]);
                if (mode >= 0 && outputColToKeyPos[mode] >= 0)
                    return keysMapRecord.getLong256A(outputColToKeyPos[mode]);
                if (mode == FILL_CONSTANT) return constantFills.getQuick(col).getLong256A(null);
                return Long256Impl.NULL_LONG256;
            }

            @Override
            public io.questdb.std.Long256 getLong256B(int col) {
                if (!isGapFilling) return baseRecord.getLong256B(col);
                int mode = fillMode(col);
                if (mode == FILL_KEY) return keysMapRecord.getLong256B(outputColToKeyPos[col]);
                if (mode >= 0 && outputColToKeyPos[mode] >= 0)
                    return keysMapRecord.getLong256B(outputColToKeyPos[mode]);
                if (mode == FILL_CONSTANT) return constantFills.getQuick(col).getLong256B(null);
                return Long256Impl.NULL_LONG256;
            }

            @Override
            public CharSequence getStrA(int col) {
                if (!isGapFilling) return baseRecord.getStrA(col);
                int mode = fillMode(col);
                if (mode == FILL_KEY) return keysMapRecord.getStrA(outputColToKeyPos[col]);
                if (mode >= 0 && outputColToKeyPos[mode] >= 0)
                    return keysMapRecord.getStrA(outputColToKeyPos[mode]);
                if (mode == FILL_CONSTANT) return constantFills.getQuick(col).getStrA(null);
                return null;
            }

            @Override
            public CharSequence getStrB(int col) {
                if (!isGapFilling) return baseRecord.getStrB(col);
                int mode = fillMode(col);
                if (mode == FILL_KEY) return keysMapRecord.getStrB(outputColToKeyPos[col]);
                if (mode >= 0 && outputColToKeyPos[mode] >= 0)
                    return keysMapRecord.getStrB(outputColToKeyPos[mode]);
                if (mode == FILL_CONSTANT) return constantFills.getQuick(col).getStrB(null);
                return null;
            }

            @Override
            public int getStrLen(int col) {
                if (!isGapFilling) return baseRecord.getStrLen(col);
                int mode = fillMode(col);
                if (mode == FILL_KEY) return keysMapRecord.getStrLen(outputColToKeyPos[col]);
                if (mode >= 0 && outputColToKeyPos[mode] >= 0)
                    return keysMapRecord.getStrLen(outputColToKeyPos[mode]);
                if (mode == FILL_CONSTANT) return constantFills.getQuick(col).getStrLen(null);
                return -1;
            }

            @Override
            public CharSequence getSymA(int col) {
                if (!isGapFilling) return baseRecord.getSymA(col);
                int mode = fillMode(col);
                if (mode == FILL_KEY) return keysMapRecord.getSymA(outputColToKeyPos[col]);
                if (mode >= 0 && outputColToKeyPos[mode] >= 0)
                    return keysMapRecord.getSymA(outputColToKeyPos[mode]);
                if (mode == FILL_CONSTANT) return constantFills.getQuick(col).getSymbol(null);
                return null;
            }

            @Override
            public CharSequence getSymB(int col) {
                if (!isGapFilling) return baseRecord.getSymB(col);
                int mode = fillMode(col);
                if (mode == FILL_KEY) return keysMapRecord.getSymB(outputColToKeyPos[col]);
                if (mode >= 0 && outputColToKeyPos[mode] >= 0)
                    return keysMapRecord.getSymB(outputColToKeyPos[mode]);
                if (mode == FILL_CONSTANT) return constantFills.getQuick(col).getSymbolB(null);
                return null;
            }

            @Override
            public io.questdb.std.str.Utf8Sequence getVarcharA(int col) {
                if (!isGapFilling) return baseRecord.getVarcharA(col);
                int mode = fillMode(col);
                if (mode == FILL_KEY) return keysMapRecord.getVarcharA(outputColToKeyPos[col]);
                if (mode >= 0 && outputColToKeyPos[mode] >= 0)
                    return keysMapRecord.getVarcharA(outputColToKeyPos[mode]);
                if (mode == FILL_CONSTANT) return constantFills.getQuick(col).getVarcharA(null);
                return null;
            }

            @Override
            public io.questdb.std.str.Utf8Sequence getVarcharB(int col) {
                if (!isGapFilling) return baseRecord.getVarcharB(col);
                int mode = fillMode(col);
                if (mode == FILL_KEY) return keysMapRecord.getVarcharB(outputColToKeyPos[col]);
                if (mode >= 0 && outputColToKeyPos[mode] >= 0)
                    return keysMapRecord.getVarcharB(outputColToKeyPos[mode]);
                if (mode == FILL_CONSTANT) return constantFills.getQuick(col).getVarcharB(null);
                return null;
            }

            @Override
            public int getVarcharSize(int col) {
                if (!isGapFilling) return baseRecord.getVarcharSize(col);
                int mode = fillMode(col);
                if (mode == FILL_KEY) return keysMapRecord.getVarcharSize(outputColToKeyPos[col]);
                if (mode >= 0 && outputColToKeyPos[mode] >= 0)
                    return keysMapRecord.getVarcharSize(outputColToKeyPos[mode]);
                if (mode == FILL_CONSTANT) return constantFills.getQuick(col).getVarcharSize(null);
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
