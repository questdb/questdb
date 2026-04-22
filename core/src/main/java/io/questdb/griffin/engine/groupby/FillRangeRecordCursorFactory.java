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

package io.questdb.griffin.engine.groupby;

import io.questdb.cairo.AbstractRecordCursorFactory;
import io.questdb.cairo.ArrayColumnTypes;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.TimestampDriver;
import io.questdb.cairo.arr.ArrayView;
import io.questdb.cairo.map.Map;
import io.questdb.cairo.map.MapFactory;
import io.questdb.cairo.map.MapKey;
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
import io.questdb.std.BinarySequence;
import io.questdb.std.Decimal128;
import io.questdb.std.Decimal256;
import io.questdb.std.DirectLongList;
import io.questdb.std.IntList;
import io.questdb.std.Long256;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.str.CharSink;
import io.questdb.std.str.Utf8Sequence;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;


/**
 * Fills missing rows in a result set range based on histogram buckets calculated using time intervals.
 * Currently only generated as a parent node to a group by, to support parallel SAMPLE BY with fills.
 * Intended to support FILL(VALUE), FILL(NULL).
 * Cannot be generated standalone (there's no syntax just to generate this, it is only generated
 * via the above optimisation).
 *
 * <p>When key columns are present (GROUPING SETS), the factory tracks all distinct
 * key combinations seen during the base scan. For each missing timestamp bucket,
 * it emits one fill row per key combination, with key columns set to their stored
 * values and aggregate columns set to the fill value.</p>
 */
public class FillRangeRecordCursorFactory extends AbstractRecordCursorFactory {
    private final RecordCursorFactory base;
    private final FillRangeRecordCursor cursor;
    private final ObjList<Function> fillValues;
    private final Function fromFunc;
    private final long samplingInterval;
    private final char samplingIntervalUnit;
    private final int timestampIndex;
    private final int timestampType;
    private final Function toFunc;

    public FillRangeRecordCursorFactory(
            RecordMetadata metadata,
            RecordCursorFactory base,
            Function fromFunc,
            Function toFunc,
            long samplingInterval,
            char samplingIntervalUnit,
            TimestampSampler timestampSampler,
            ObjList<Function> fillValues,
            int timestampIndex,
            int timestampType
    ) throws SqlException {
        this(metadata, base, fromFunc, toFunc, samplingInterval, samplingIntervalUnit,
                timestampSampler, fillValues, timestampIndex, timestampType, null, null, 0);
    }

    public FillRangeRecordCursorFactory(
            RecordMetadata metadata,
            RecordCursorFactory base,
            Function fromFunc,
            Function toFunc,
            long samplingInterval,
            char samplingIntervalUnit,
            TimestampSampler timestampSampler,
            ObjList<Function> fillValues,
            int timestampIndex,
            int timestampType,
            @Nullable CairoConfiguration configuration,
            @Nullable IntList keyColumnPositions,
            int position
    ) throws SqlException {
        super(metadata);
        this.base = base;
        this.fromFunc = fromFunc;
        this.toFunc = toFunc;

        // needed for the EXPLAIN plan
        this.samplingInterval = samplingInterval;
        this.samplingIntervalUnit = samplingIntervalUnit;
        this.timestampIndex = timestampIndex;
        this.fillValues = fillValues;
        this.timestampType = timestampType;
        this.cursor = new FillRangeRecordCursor(
                timestampSampler, fromFunc, toFunc, fillValues, timestampIndex, timestampType,
                configuration, metadata, keyColumnPositions, position
        );
    }

    @Override
    public RecordCursorFactory getBaseFactory() {
        return base;
    }

    @Override
    public RecordCursor getCursor(SqlExecutionContext executionContext) throws SqlException {
        if (getMetadata().getColumnCount() > fillValues.size() + 1) {
            if (fillValues.size() == 1 && cursor.hasKeyColumns) {
                // Grouping sets path: auto-expand a single fill value to cover
                // all non-timestamp columns. SYMBOL columns are GROUP BY keys
                // (never aggregates), so they get NULL in fill rows. Other
                // columns get the user-specified fill value.
                //
                // Note: fill values assigned to key columns (including GROUPING()
                // / GROUPING_ID() columns) are never read at runtime because
                // isKeyColumn() intercepts all reads and returns the actual key
                // value from the key map instead.
                final Function singleFill = fillValues.getQuick(0);
                final Function fillFunc = singleFill.isNullConstant() ? NullConstant.NULL : singleFill;
                final RecordMetadata metadata = getMetadata();
                final int columnCount = metadata.getColumnCount();
                fillValues.clear();
                for (int col = 0; col < columnCount; col++) {
                    if (col == timestampIndex) {
                        continue;
                    }
                    if (ColumnType.isSymbol(metadata.getColumnType(col))) {
                        fillValues.add(NullConstant.NULL);
                    } else {
                        fillValues.add(fillFunc);
                    }
                }
            } else if (fillValues.size() == 1 && fillValues.getQuick(0).isNullConstant()) {
                // Non-grouping-sets path: only auto-expand FILL(NULL)
                final int diff = getMetadata().getColumnCount() - 1;
                for (int i = 1; i < diff; i++) {
                    fillValues.add(NullConstant.NULL);
                }
            } else {
                throw SqlException.$(-1, "not enough fill values");
            }
        }

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
        return false;
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.type("Fill Range");
        TimestampDriver driver = ColumnType.getTimestampDriver(timestampType);
        if (fromFunc != driver.getTimestampConstantNull() || toFunc != driver.getTimestampConstantNull()) {
            sink.attr("range").val('(').val(fromFunc).val(',').val(toFunc).val(')');
        }
        sink.attr("stride").val('\'').val(samplingInterval).val(samplingIntervalUnit).val('\'');
        // print values omitting the timestamp column
        // since we added an extra artificial null
        sink.attr("values").val('[');
        final int commaStartIndex = timestampIndex == 0 ? 2 : 1;
        for (int i = 0; i < fillValues.size(); i++) {
            if (i == timestampIndex) {
                continue;
            }
            if (i >= commaStartIndex) {
                sink.val(',');
            }
            sink.val(fillValues.getQuick(i));
        }
        sink.val(']');
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
        base.close();
        Misc.free(cursor);
        Misc.free(fromFunc);
        Misc.free(toFunc);
        Misc.freeObjList(fillValues);
    }

    private static class FillRangeRecordCursor implements NoRandomAccessRecordCursor {
        private final ObjList<Function> fillValues;
        private final FillRangeRecord fillingRecord = new FillRangeRecord();
        private final FillRangeTimestampConstant fillingTimestampFunc;
        private final Function fromFunc;
        private final boolean hasKeyColumns;
        // Maps output column position to key map column index (-1 = not a key column).
        // Only allocated when hasKeyColumns is true.
        private final int[] outputColToKeyMapCol;
        // Column types for each key column in the key map (indexed by map column index).
        // SYMBOL columns are stored as INT (symbol table index). Only allocated
        // when hasKeyColumns is true.
        private final int[] keyMapColTypes;
        private final TimestampDriver timestampDriver;
        private final int timestampIndex;
        private final TimestampSampler timestampSampler;
        private final Function toFunc;
        private RecordCursor baseCursor;
        private Record baseRecord;
        private boolean gapFilling;
        private boolean hasNegative;
        // Map storing distinct key combinations seen during the base scan.
        // Key columns are stored with their actual types (SYMBOL columns
        // are stored as INT via their symbol table index).
        // Only allocated when hasKeyColumns is true.
        private Map keyMap;
        private RecordCursor keyMapCursor;
        private Record keyMapRecord;
        private boolean isValueFuncsInitialized;
        private long lastTimestamp = Long.MIN_VALUE;
        private long maxTimestamp;
        private long minTimestamp;
        private boolean needsSorting = false;
        private long nextBucketTimestamp;
        private DirectLongList presentTimestamps;
        private long presentTimestampsIndex;
        private long presentTimestampsSize;

        private final int columnCount;

        private FillRangeRecordCursor(
                TimestampSampler timestampSampler,
                @NotNull Function fromFunc,
                @NotNull Function toFunc,
                ObjList<Function> fillValues,
                int timestampIndex,
                int timestampType,
                @Nullable CairoConfiguration configuration,
                RecordMetadata metadata,
                @Nullable IntList keyColumnPositions,
                int position
        ) throws SqlException {
            this.timestampSampler = timestampSampler;
            this.fromFunc = fromFunc;
            this.toFunc = toFunc;
            this.fillValues = fillValues;
            this.timestampIndex = timestampIndex;
            this.fillingTimestampFunc = new FillRangeTimestampConstant(timestampType);
            this.timestampDriver = ColumnType.getTimestampDriver(timestampType);
            this.columnCount = metadata.getColumnCount();
            this.hasKeyColumns = keyColumnPositions != null && keyColumnPositions.size() > 0;

            if (hasKeyColumns) {
                this.outputColToKeyMapCol = new int[metadata.getColumnCount()];
                for (int j = 0, m = outputColToKeyMapCol.length; j < m; j++) {
                    outputColToKeyMapCol[j] = -1;
                }
                this.keyMapColTypes = new int[keyColumnPositions.size()];
                ArrayColumnTypes keyMapTypes = new ArrayColumnTypes();
                for (int i = 0, n = keyColumnPositions.size(); i < n; i++) {
                    int colPos = keyColumnPositions.getQuick(i);
                    outputColToKeyMapCol[colPos] = i;
                    int colType = metadata.getColumnType(colPos);
                    // SYMBOL columns are stored as INT (symbol table index)
                    int mapType = ColumnType.isSymbol(colType) ? ColumnType.INT : colType;
                    validateKeyColumnType(mapType, position);
                    keyMapColTypes[i] = mapType;
                    keyMapTypes.add(mapType);
                }
                ArrayColumnTypes emptyValueTypes = new ArrayColumnTypes();
                this.keyMap = MapFactory.createUnorderedMap(configuration, keyMapTypes, emptyValueTypes);
            } else {
                this.outputColToKeyMapCol = null;
                this.keyMapColTypes = null;
            }
        }

        @Override
        public void close() {
            baseCursor = Misc.free(baseCursor);
            presentTimestamps = Misc.free(presentTimestamps);
            keyMap = Misc.free(keyMap);
        }

        @Override
        public Record getRecord() {
            return fillingRecord;
        }

        @Override
        public SymbolTable getSymbolTable(int columnIndex) {
            return baseCursor.getSymbolTable(columnIndex);
        }

        @Override
        public boolean hasNext() {
            if (gapFilling) {
                if (hasKeyColumns) {
                    // Try next key combination for the current gap timestamp.
                    if (keyMapCursor.hasNext()) {
                        return true;
                    }
                    // All key combinations emitted. Advance to next gap.
                    nextBucketTimestamp = timestampSampler.nextTimestamp(nextBucketTimestamp);
                    if (foundGapToFill()) {
                        keyMapCursor.toTop();
                        return keyMapCursor.hasNext();
                    }
                    return false;
                }
                // Non-keyed: one fill row per gap.
                nextBucketTimestamp = timestampSampler.nextTimestamp(nextBucketTimestamp);
                return foundGapToFill();
            } else if (baseCursor.hasNext()) {
                // Scan base cursor and return all the records.
                // Also save all the timestamps already returned by the base cursor
                // to determine the gaps later.
                long timestamp = baseRecord.getTimestamp(timestampIndex);
                needsSorting |= lastTimestamp > timestamp;
                hasNegative = hasNegative || timestamp < 0;
                if (hasNegative && needsSorting) {
                    throw CairoException.nonCritical().put("cannot FILL for the timestamps before 1970");
                }
                // Start saving timestamps to determine the gaps
                presentTimestamps.add(lastTimestamp = timestamp);
                // Record key combination for per-key gap filling
                if (hasKeyColumns) {
                    recordKeyValues();
                }
                return true;
            }
            gapFilling = true;
            prepareGapFilling();
            if (foundGapToFill()) {
                if (hasKeyColumns) {
                    keyMapCursor = keyMap.getCursor();
                    keyMapRecord = keyMapCursor.getRecord();
                    return keyMapCursor.hasNext();
                }
                return true;
            }
            return false;
        }

        @Override
        public long preComputedStateSize() {
            return 0;
        }

        @Override
        public long size() {
            // Can be improved, potentially we know the size if both TO and FROM are set
            return -1;
        }

        @Override
        public void toTop() {
            baseCursor.toTop();
            presentTimestamps.clear();
            if (hasKeyColumns) {
                keyMap.clear();
            }
            gapFilling = false;
            needsSorting = false;
            lastTimestamp = Long.MIN_VALUE;
        }

        private void deduplicateTimestamps() {
            // GROUPING SETS produces multiple rows per timestamp (one per
            // grouping set × key combination). The gap-finding logic expects
            // unique timestamps, so we deduplicate after sorting.
            if (presentTimestampsSize > 1) {
                long writeIdx = 1;
                for (long readIdx = 1; readIdx < presentTimestampsSize; readIdx++) {
                    if (presentTimestamps.get(readIdx) != presentTimestamps.get(readIdx - 1)) {
                        presentTimestamps.set(writeIdx++, presentTimestamps.get(readIdx));
                    }
                }
                presentTimestampsSize = writeIdx;
            }
        }

        private boolean foundGapToFill() {
            // Scroll presentTimestampsIndex and nextBucketTimestamp until we find a gap
            // or until we reach the end of the presentTimestamps.
            for (; presentTimestampsIndex < presentTimestampsSize; presentTimestampsIndex++) {
                if (presentTimestamps.get(presentTimestampsIndex) == nextBucketTimestamp) {
                    nextBucketTimestamp = timestampSampler.nextTimestamp(nextBucketTimestamp);
                } else {
                    // A potential gap found.
                    break;
                }
            }

            return nextBucketTimestamp < maxTimestamp;
            // Do not return true if nextBucketTimestamp == maxTimestamp
            // If maxTimestamp is coming from TO clause, it's exclusive,
            // and if it's coming from the base cursor, it already returned.
        }

        private Function getFillFunction(int col) {
            if (col == timestampIndex) {
                return fillingTimestampFunc;
            }
            return fillValues.getQuick(col);
        }

        private void initTimestamps(Function fromFunc, Function toFunc) {
            minTimestamp = fromFunc == timestampDriver.getTimestampConstantNull() ? Long.MAX_VALUE : timestampDriver.from(fromFunc.getTimestamp(null),
                    ColumnType.getTimestampType(fromFunc.getType()));
            maxTimestamp = toFunc == timestampDriver.getTimestampConstantNull() ? Long.MIN_VALUE : timestampDriver.from(toFunc.getTimestamp(null),
                    ColumnType.getTimestampType(toFunc.getType()));
        }

        private void initValueFuncs(ObjList<Function> valueFuncs) {
            if (isValueFuncsInitialized) {
                return;
            }
            isValueFuncsInitialized = true;

            // can't just check null, as we use this as the placeholder value
            if (valueFuncs.size() - 1 < timestampIndex) {
                // timestamp is the last column, so we add it
                valueFuncs.extendAndSet(timestampIndex, NullConstant.NULL);
                return;
            }

            // else we grab the value in the corresponding slot
            final Function func = valueFuncs.getQuick(timestampIndex);

            // if it is a real function, i.e we've not added our placeholder null
            if (func != null) {
                // then we insert at this position
                valueFuncs.insert(timestampIndex, 1, NullConstant.NULL);
            }
        }

        private boolean isKeyColumn(int col) {
            return hasKeyColumns && outputColToKeyMapCol[col] >= 0;
        }

        private void of(
                RecordCursor baseCursor,
                SqlExecutionContext executionContext
        ) throws SqlException {
            this.baseCursor = baseCursor;
            Function.init(fillValues, baseCursor, executionContext, null);
            fromFunc.init(baseCursor, executionContext);
            toFunc.init(baseCursor, executionContext);
            initTimestamps(fromFunc, toFunc);
            if (presentTimestamps == null) {
                long capacity = baseCursor.size();
                if (capacity < 0) {
                    // use the default capacity
                    capacity = 8;
                }
                presentTimestamps = new DirectLongList(capacity, MemoryTag.NATIVE_GROUP_BY_FUNCTION);
            }
            initValueFuncs(fillValues);
            assert fillValues.size() <= columnCount : "fillValues.size()=" + fillValues.size() + " exceeds columnCount=" + columnCount;
            if (hasKeyColumns) {
                keyMap.reopen();
            }
            baseRecord = baseCursor.getRecord();
            toTop();
        }

        private void prepareGapFilling() {
            // Cache the size of the present timestamps for loop optimization
            presentTimestampsSize = presentTimestamps.size();

            if (needsSorting && presentTimestampsSize > 1) {
                presentTimestamps.sortAsUnsigned();
            }

            if (hasKeyColumns) {
                deduplicateTimestamps();
            }

            if (presentTimestampsSize > 0) {
                minTimestamp = Math.min(minTimestamp, presentTimestamps.get(0));
                maxTimestamp = Math.max(maxTimestamp, presentTimestamps.get(presentTimestampsSize - 1));
            }
            timestampSampler.setStart(minTimestamp);
            nextBucketTimestamp = minTimestamp;
            presentTimestampsIndex = 0;
        }

        private void recordKeyValues() {
            MapKey key = keyMap.withKey();
            for (int i = 0, n = outputColToKeyMapCol.length; i < n; i++) {
                int mapCol = outputColToKeyMapCol[i];
                if (mapCol >= 0) {
                    switch (ColumnType.tagOf(keyMapColTypes[mapCol])) {
                        case ColumnType.BOOLEAN -> key.putBool(baseRecord.getBool(i));
                        case ColumnType.BYTE -> key.putByte(baseRecord.getByte(i));
                        case ColumnType.CHAR -> key.putChar(baseRecord.getChar(i));
                        case ColumnType.DATE -> key.putDate(baseRecord.getDate(i));
                        case ColumnType.DOUBLE -> key.putDouble(baseRecord.getDouble(i));
                        case ColumnType.FLOAT -> key.putFloat(baseRecord.getFloat(i));
                        case ColumnType.INT -> key.putInt(baseRecord.getInt(i));
                        case ColumnType.IPv4 -> key.putIPv4(baseRecord.getIPv4(i));
                        case ColumnType.LONG -> key.putLong(baseRecord.getLong(i));
                        case ColumnType.SHORT -> key.putShort(baseRecord.getShort(i));
                        case ColumnType.STRING -> key.putStr(baseRecord.getStrA(i));
                        case ColumnType.TIMESTAMP -> key.putTimestamp(baseRecord.getTimestamp(i));
                        case ColumnType.VARCHAR -> key.putVarchar(baseRecord.getVarcharA(i));
                        default -> throw CairoException.nonCritical()
                                .put("unsupported key column type for FILL: ")
                                .put(ColumnType.nameOf(keyMapColTypes[mapCol]));
                    }
                }
            }
            key.createValue();
        }

        private static void validateKeyColumnType(int mapType, int position) throws SqlException {
            switch (ColumnType.tagOf(mapType)) {
                case ColumnType.BOOLEAN, ColumnType.BYTE, ColumnType.CHAR,
                     ColumnType.DATE, ColumnType.DOUBLE, ColumnType.FLOAT,
                     ColumnType.INT, ColumnType.IPv4, ColumnType.LONG,
                     ColumnType.SHORT, ColumnType.STRING, ColumnType.TIMESTAMP,
                     ColumnType.VARCHAR -> {
                }
                default -> throw SqlException.$(position, "unsupported key column type for FILL: ")
                        .put(ColumnType.nameOf(mapType));
            }
        }

        private class FillRangeRecord implements Record {

            @Override
            public ArrayView getArray(int col, int columnType) {
                if (gapFilling) {
                    return getFillFunction(col).getArray(null);
                } else {
                    return baseRecord.getArray(col, columnType);
                }
            }

            @Override
            public BinarySequence getBin(int col) {
                if (gapFilling) {
                    return getFillFunction(col).getBin(null);
                } else {
                    return baseRecord.getBin(col);
                }
            }

            @Override
            public long getBinLen(int col) {
                if (gapFilling) {
                    return getFillFunction(col).getBinLen(null);
                } else {
                    return baseRecord.getBinLen(col);
                }
            }

            @Override
            public boolean getBool(int col) {
                if (gapFilling) {
                    if (isKeyColumn(col)) {
                        return keyMapRecord.getBool(outputColToKeyMapCol[col]);
                    }
                    return getFillFunction(col).getBool(null);
                } else {
                    return baseRecord.getBool(col);
                }
            }

            @Override
            public byte getByte(int col) {
                if (gapFilling) {
                    if (isKeyColumn(col)) {
                        return keyMapRecord.getByte(outputColToKeyMapCol[col]);
                    }
                    return getFillFunction(col).getByte(null);
                } else {
                    return baseRecord.getByte(col);
                }
            }

            @Override
            public char getChar(int col) {
                if (gapFilling) {
                    if (isKeyColumn(col)) {
                        return keyMapRecord.getChar(outputColToKeyMapCol[col]);
                    }
                    return getFillFunction(col).getChar(null);
                } else {
                    return baseRecord.getChar(col);
                }
            }

            @Override
            public long getDate(int col) {
                if (gapFilling) {
                    if (isKeyColumn(col)) {
                        return keyMapRecord.getDate(outputColToKeyMapCol[col]);
                    }
                    return getFillFunction(col).getDate(null);
                } else {
                    return baseRecord.getDate(col);
                }
            }

            @Override
            public void getDecimal128(int col, Decimal128 sink) {
                if (gapFilling) {
                    getFillFunction(col).getDecimal128(null, sink);
                } else {
                    baseRecord.getDecimal128(col, sink);
                }
            }

            @Override
            public short getDecimal16(int col) {
                if (gapFilling) {
                    return getFillFunction(col).getDecimal16(null);
                } else {
                    return baseRecord.getDecimal16(col);
                }
            }

            @Override
            public void getDecimal256(int col, Decimal256 sink) {
                if (gapFilling) {
                    getFillFunction(col).getDecimal256(null, sink);
                } else {
                    baseRecord.getDecimal256(col, sink);
                }
            }

            @Override
            public int getDecimal32(int col) {
                if (gapFilling) {
                    return getFillFunction(col).getDecimal32(null);
                } else {
                    return baseRecord.getDecimal32(col);
                }
            }

            @Override
            public long getDecimal64(int col) {
                if (gapFilling) {
                    return getFillFunction(col).getDecimal64(null);
                } else {
                    return baseRecord.getDecimal64(col);
                }
            }

            @Override
            public byte getDecimal8(int col) {
                if (gapFilling) {
                    return getFillFunction(col).getDecimal8(null);
                } else {
                    return baseRecord.getDecimal8(col);
                }
            }

            @Override
            public double getDouble(int col) {
                if (gapFilling) {
                    if (isKeyColumn(col)) {
                        return keyMapRecord.getDouble(outputColToKeyMapCol[col]);
                    }
                    return getFillFunction(col).getDouble(null);
                } else {
                    return baseRecord.getDouble(col);
                }
            }

            @Override
            public float getFloat(int col) {
                if (gapFilling) {
                    if (isKeyColumn(col)) {
                        return keyMapRecord.getFloat(outputColToKeyMapCol[col]);
                    }
                    return getFillFunction(col).getFloat(null);
                } else {
                    return baseRecord.getFloat(col);
                }
            }

            @Override
            public byte getGeoByte(int col) {
                if (gapFilling) {
                    return getFillFunction(col).getGeoByte(null);
                } else {
                    return baseRecord.getGeoByte(col);
                }
            }

            @Override
            public int getGeoInt(int col) {
                if (gapFilling) {
                    return getFillFunction(col).getGeoInt(null);
                } else {
                    return baseRecord.getGeoInt(col);
                }
            }

            @Override
            public long getGeoLong(int col) {
                if (gapFilling) {
                    return getFillFunction(col).getGeoLong(null);
                } else {
                    return baseRecord.getGeoLong(col);
                }
            }

            @Override
            public short getGeoShort(int col) {
                if (gapFilling) {
                    return getFillFunction(col).getGeoShort(null);
                } else {
                    return baseRecord.getGeoShort(col);
                }
            }

            @Override
            public int getIPv4(int col) {
                if (gapFilling) {
                    if (isKeyColumn(col)) {
                        return keyMapRecord.getIPv4(outputColToKeyMapCol[col]);
                    }
                    return getFillFunction(col).getIPv4(null);
                } else {
                    return baseRecord.getIPv4(col);
                }
            }

            @Override
            public int getInt(int col) {
                if (gapFilling) {
                    if (isKeyColumn(col)) {
                        return keyMapRecord.getInt(outputColToKeyMapCol[col]);
                    }
                    return getFillFunction(col).getInt(null);
                } else {
                    return baseRecord.getInt(col);
                }
            }

            @Override
            public long getLong(int col) {
                if (gapFilling) {
                    if (isKeyColumn(col)) {
                        return keyMapRecord.getLong(outputColToKeyMapCol[col]);
                    }
                    return getFillFunction(col).getLong(null);
                } else {
                    return baseRecord.getLong(col);
                }
            }

            @Override
            public long getLong128Hi(int col) {
                if (gapFilling) {
                    return getFillFunction(col).getLong128Hi(null);
                } else {
                    return baseRecord.getLong128Hi(col);
                }
            }

            @Override
            public long getLong128Lo(int col) {
                if (gapFilling) {
                    return getFillFunction(col).getLong128Lo(null);
                } else {
                    return baseRecord.getLong128Lo(col);
                }
            }

            @Override
            public void getLong256(int col, CharSink<?> sink) {
                if (gapFilling) {
                    getFillFunction(col).getLong256(null, sink);
                } else {
                    baseRecord.getLong256(col, sink);
                }
            }

            @Override
            public Long256 getLong256A(int col) {
                if (gapFilling) {
                    return getFillFunction(col).getLong256A(null);
                } else {
                    return baseRecord.getLong256A(col);
                }
            }

            @Override
            public Long256 getLong256B(int col) {
                if (gapFilling) {
                    return getFillFunction(col).getLong256B(null);
                } else {
                    return baseRecord.getLong256B(col);
                }
            }

            @Override
            public short getShort(int col) {
                if (gapFilling) {
                    if (isKeyColumn(col)) {
                        return keyMapRecord.getShort(outputColToKeyMapCol[col]);
                    }
                    return getFillFunction(col).getShort(null);
                } else {
                    return baseRecord.getShort(col);
                }
            }


            @Override
            public @Nullable CharSequence getStrA(int col) {
                if (gapFilling) {
                    if (isKeyColumn(col)) {
                        return keyMapRecord.getStrA(outputColToKeyMapCol[col]);
                    }
                    return getFillFunction(col).getStrA(null);
                } else {
                    return baseRecord.getStrA(col);
                }
            }

            @Override
            public CharSequence getStrB(int col) {
                if (gapFilling) {
                    if (isKeyColumn(col)) {
                        return keyMapRecord.getStrB(outputColToKeyMapCol[col]);
                    }
                    return getFillFunction(col).getStrB(null);
                } else {
                    return baseRecord.getStrB(col);
                }
            }

            @Override
            public int getStrLen(int col) {
                if (gapFilling) {
                    if (isKeyColumn(col)) {
                        return keyMapRecord.getStrLen(outputColToKeyMapCol[col]);
                    }
                    return getFillFunction(col).getStrLen(null);
                } else {
                    return baseRecord.getStrLen(col);
                }
            }

            @Override
            public CharSequence getSymA(int col) {
                if (gapFilling) {
                    if (isKeyColumn(col)) {
                        int symIdx = keyMapRecord.getInt(outputColToKeyMapCol[col]);
                        if (symIdx == SymbolTable.VALUE_IS_NULL) {
                            return null;
                        }
                        return baseCursor.getSymbolTable(col).valueOf(symIdx);
                    }
                    return getFillFunction(col).getSymbol(null);
                } else {
                    return baseRecord.getSymA(col);
                }
            }

            @Override
            public CharSequence getSymB(int col) {
                if (gapFilling) {
                    if (isKeyColumn(col)) {
                        int symIdx = keyMapRecord.getInt(outputColToKeyMapCol[col]);
                        if (symIdx == SymbolTable.VALUE_IS_NULL) {
                            return null;
                        }
                        return baseCursor.getSymbolTable(col).valueBOf(symIdx);
                    }
                    return getFillFunction(col).getSymbolB(null);
                } else {
                    return baseRecord.getSymB(col);
                }
            }

            @Override
            public long getTimestamp(int col) {
                if (gapFilling) {
                    if (col == timestampIndex) {
                        return nextBucketTimestamp;
                    }
                    if (isKeyColumn(col)) {
                        return keyMapRecord.getTimestamp(outputColToKeyMapCol[col]);
                    }
                    return getFillFunction(col).getLong(null);
                } else {
                    return baseRecord.getTimestamp(col);
                }
            }

            @Override
            public @Nullable Utf8Sequence getVarcharA(int col) {
                if (gapFilling) {
                    if (isKeyColumn(col)) {
                        return keyMapRecord.getVarcharA(outputColToKeyMapCol[col]);
                    }
                    return getFillFunction(col).getVarcharA(null);
                } else {
                    return baseRecord.getVarcharA(col);
                }
            }

            @Override
            public @Nullable Utf8Sequence getVarcharB(int col) {
                if (gapFilling) {
                    if (isKeyColumn(col)) {
                        return keyMapRecord.getVarcharB(outputColToKeyMapCol[col]);
                    }
                    return getFillFunction(col).getVarcharB(null);
                } else {
                    return baseRecord.getVarcharB(col);
                }
            }

            @Override
            public int getVarcharSize(int col) {
                if (gapFilling) {
                    if (isKeyColumn(col)) {
                        return keyMapRecord.getVarcharSize(outputColToKeyMapCol[col]);
                    }
                    return getFillFunction(col).getVarcharSize(null);
                } else {
                    return baseRecord.getVarcharSize(col);
                }
            }
        }

        private class FillRangeTimestampConstant extends TimestampFunction implements ConstantFunction {
            public FillRangeTimestampConstant(int timestampType) {
                super(timestampType);
            }

            @Override
            public long getTimestamp(Record rec) {
                return nextBucketTimestamp;
            }
        }
    }
}
