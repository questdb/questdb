/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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
import io.questdb.cairo.DataUnavailableException;
import io.questdb.cairo.TimestampDriver;
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
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.BinarySequence;
import io.questdb.std.DirectLongList;
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
 */
public class FillRangeRecordCursorFactory extends AbstractRecordCursorFactory {
    public static final Log LOG = LogFactory.getLog(FillRangeRecordCursorFactory.class);
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
    ) {
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
        this.cursor = new FillRangeRecordCursor(timestampSampler, fromFunc, toFunc, fillValues, timestampIndex, timestampType);
    }

    @Override
    public RecordCursorFactory getBaseFactory() {
        return base;
    }

    @Override
    public RecordCursor getCursor(SqlExecutionContext executionContext) throws SqlException {
        if (getMetadata().getColumnCount() > fillValues.size() + 1) {
            if (fillValues.size() == 1 && fillValues.getQuick(0).isNullConstant()) {
                final int diff = (getMetadata().getColumnCount() - 1);
                // skip one entry as it should be the designated timestamp
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
        Misc.free(fromFunc);
        Misc.free(toFunc);
        Misc.freeObjList(fillValues);
    }

    private static class FillRangeRecordCursor implements NoRandomAccessRecordCursor {
        private final ObjList<Function> fillValues;
        private final FillRangeRecord fillingRecord = new FillRangeRecord();
        private final FillRangeTimestampConstant fillingTimestampFunc;
        private final Function fromFunc;
        private final TimestampDriver timestampDriver;
        private final int timestampIndex;
        private final TimestampSampler timestampSampler;
        private final Function toFunc;
        private RecordCursor baseCursor;
        private Record baseRecord;
        private boolean gapFilling;
        private boolean hasNegative;
        private long lastTimestamp = Long.MIN_VALUE;
        private long maxTimestamp;
        private long minTimestamp;
        private boolean needsSorting = false;
        private long nextBucketTimestamp;
        private DirectLongList presentTimestamps;
        private long presentTimestampsIndex;
        private long presentTimestampsSize;

        private FillRangeRecordCursor(
                TimestampSampler timestampSampler,
                @NotNull Function fromFunc,
                @NotNull Function toFunc,
                ObjList<Function> fillValues,
                int timestampIndex,
                int timestampType
        ) {
            this.timestampSampler = timestampSampler;
            this.fromFunc = fromFunc;
            this.toFunc = toFunc;
            this.fillValues = fillValues;
            this.timestampIndex = timestampIndex;
            this.fillingTimestampFunc = new FillRangeTimestampConstant(timestampType);
            this.timestampDriver = ColumnType.getTimestampDriver(timestampType);
        }

        @Override
        public void close() {
            baseCursor = Misc.free(baseCursor);
            presentTimestamps = Misc.free(presentTimestamps);
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
            // We rely on the fact this cursor is allowed to return result set
            // that is not ordered on time. For that reason all the filling is done at the end
            if (gapFilling) {
                nextBucketTimestamp = timestampSampler.nextTimestamp(nextBucketTimestamp);
                return foundGapToFill();
            } else if (baseCursor.hasNext()) {
                // Scan base cursor and return all the records
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
                return true;
            }
            gapFilling = true;
            prepareGapFilling();
            return foundGapToFill();
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
            gapFilling = false;
            needsSorting = false;
            lastTimestamp = Long.MIN_VALUE;
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
                long capacity = 8;
                try {
                    capacity = baseCursor.size();
                    if (capacity < 0) {
                        capacity = 8;
                    }
                } catch (DataUnavailableException ex) {
                    // That's ok, we'll just use the default capacity
                }
                presentTimestamps = new DirectLongList(capacity, MemoryTag.NATIVE_GROUP_BY_FUNCTION);
            }
            initValueFuncs(fillValues);
            baseRecord = baseCursor.getRecord();
            toTop();
        }

        private void prepareGapFilling() {
            // Cache the size of the present timestamps for loop optimization
            presentTimestampsSize = presentTimestamps.size();

            if (needsSorting && presentTimestampsSize > 1) {
                presentTimestamps.sortAsUnsigned();
            }

            if (presentTimestampsSize > 0) {
                minTimestamp = Math.min(minTimestamp, presentTimestamps.get(0));
                maxTimestamp = Math.max(maxTimestamp, presentTimestamps.get(presentTimestampsSize - 1));
            }
            timestampSampler.setStart(minTimestamp);
            nextBucketTimestamp = minTimestamp;
            presentTimestampsIndex = 0;
        }

        private class FillRangeRecord implements Record {

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
                    return getFillFunction(col).getBool(null);
                } else {
                    return baseRecord.getBool(col);
                }
            }

            @Override
            public byte getByte(int col) {
                if (gapFilling) {
                    return getFillFunction(col).getByte(null);
                } else {
                    return baseRecord.getByte(col);
                }
            }

            @Override
            public char getChar(int col) {
                if (gapFilling) {
                    return getFillFunction(col).getChar(null);
                } else {
                    return baseRecord.getChar(col);
                }
            }

            @Override
            public double getDouble(int col) {
                if (gapFilling) {
                    return getFillFunction(col).getDouble(null);
                } else {
                    return baseRecord.getDouble(col);
                }
            }

            @Override
            public float getFloat(int col) {
                if (gapFilling) {
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
                    return getFillFunction(col).getIPv4(null);
                } else {
                    return baseRecord.getIPv4(col);
                }
            }

            @Override
            public int getInt(int col) {
                if (gapFilling) {
                    return getFillFunction(col).getInt(null);
                } else {
                    return baseRecord.getInt(col);
                }
            }

            @Override
            public long getLong(int col) {
                if (gapFilling) {
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
                    return getFillFunction(col).getShort(null);
                } else {
                    return baseRecord.getShort(col);
                }
            }


            @Override
            public @Nullable CharSequence getStrA(int col) {
                if (gapFilling) {
                    return getFillFunction(col).getStrA(null);
                } else {
                    return baseRecord.getStrA(col);
                }
            }

            @Override
            public CharSequence getStrB(int col) {
                if (gapFilling) {
                    return getFillFunction(col).getStrB(null);
                } else {
                    return baseRecord.getStrB(col);
                }
            }

            @Override
            public int getStrLen(int col) {
                if (gapFilling) {
                    return getFillFunction(col).getStrLen(null);
                } else {
                    return baseRecord.getStrLen(col);
                }
            }

            @Override
            public CharSequence getSymA(int col) {
                if (gapFilling) {
                    return getFillFunction(col).getSymbol(null);
                } else {
                    return baseRecord.getSymA(col);
                }
            }

            @Override
            public CharSequence getSymB(int col) {
                if (gapFilling) {
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
                    } else {
                        return getFillFunction(col).getLong(null);
                    }
                } else {
                    return baseRecord.getTimestamp(col);
                }
            }

            @Override
            public @Nullable Utf8Sequence getVarcharA(int col) {
                if (gapFilling) {
                    return getFillFunction(col).getVarcharA(null);
                } else {
                    return baseRecord.getVarcharA(col);
                }
            }

            @Override
            public @Nullable Utf8Sequence getVarcharB(int col) {
                if (gapFilling) {
                    return getFillFunction(col).getVarcharB(null);
                } else {
                    return baseRecord.getVarcharB(col);
                }
            }

            @Override
            public int getVarcharSize(int col) {
                if (gapFilling) {
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
