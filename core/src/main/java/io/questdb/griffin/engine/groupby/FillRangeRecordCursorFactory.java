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
import io.questdb.cairo.ColumnType;
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
import io.questdb.griffin.engine.functions.constants.TimestampConstant;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.BinarySequence;
import io.questdb.std.IntList;
import io.questdb.std.Long256;
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
    private final Function fromFunc;
    private final long samplingInterval;
    private final char samplingIntervalUnit;
    private final int timestampIndex;
    private final Function toFunc;
    private final ObjList<Function> valueFuncs;
    private final IntList valueFuncsPos;

    public FillRangeRecordCursorFactory(
            RecordMetadata metadata,
            RecordCursorFactory base,
            Function fromFunc,
            Function toFunc,
            long samplingInterval,
            char samplingIntervalUnit,
            TimestampSampler timestampSampler,
            ObjList<Function> fillValues,
            IntList fillValuesPos,
            int timestampIndex
    ) throws SqlException {
        super(metadata);
        this.base = base;
        this.fromFunc = fromFunc;
        this.toFunc = toFunc;

        // needed for the EXPLAIN plan
        this.samplingInterval = samplingInterval;
        this.samplingIntervalUnit = samplingIntervalUnit;
        this.timestampIndex = timestampIndex;
        this.valueFuncs = fillValues;
        this.valueFuncsPos = fillValuesPos;

        // only do this for value fill
        if (!(valueFuncs.size() == 1 && valueFuncs.get(0).isNullConstant())) {
            if (metadata.getColumnCount() - 1 > valueFuncs.size()) {
                throw SqlException.$(fillValuesPos.getLast(), "not enough fill values");
            }

            /*
            This is used to offset our lookup into the columns.
            We don't expect the timestamp column to be included in the args list.
            Therefore, once we pass the column, we need to offset back one entry to find
            the corresponding fill value.
             */
            int passedTimestamp = 0;

            // validate metadata
            for (int i = 0, n = metadata.getColumnCount(); i < n; i++) {
                int columnType = metadata.getColumnType(i);
                if (i == metadata.getTimestampIndex()) {
                    passedTimestamp = 1;
                    continue;
                }

                // see earlier comment regarding `passedTimestamp`
                int fillColumnType = valueFuncs.get(i - passedTimestamp).getType();

                // check if the value can appropriately cast to the corresponding column type
                // in the metadata
                if (fillColumnType != columnType && !ColumnType.isBuiltInWideningCast(fillColumnType, columnType)) {
                    throw SqlException.$(fillValuesPos.getQuick(i - passedTimestamp), "invalid fill value, cannot cast ")
                            .put(ColumnType.nameOf(fillColumnType)).put(" to ")
                            .put(ColumnType.nameOf(columnType));
                }
            }
        }

        this.cursor = new FillRangeRecordCursor(timestampSampler, fromFunc, toFunc, fillValues, timestampIndex);
    }

    @Override
    public RecordCursorFactory getBaseFactory() {
        return base;
    }

    @Override
    public RecordCursor getCursor(SqlExecutionContext executionContext) throws SqlException {
        if (getMetadata().getColumnCount() > valueFuncs.size() + 1) {
            if (valueFuncs.size() == 1 && valueFuncs.getQuick(0).isNullConstant()) {
                final int diff = (getMetadata().getColumnCount() - 1);
                // skip one entry as it should be the designated timestamp
                for (int i = 1; i < diff; i++) {
                    valueFuncs.add(NullConstant.NULL);
                }
            } else {
                throw SqlException.$(valueFuncsPos.getQuick(valueFuncsPos.getLast()), "not enough fill values");
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
    public boolean implementsFill() {
        return true;
    }

    @Override
    public boolean recordCursorSupportsRandomAccess() {
        return false;
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.type("Fill Range");
        if (fromFunc != TimestampConstant.NULL || toFunc != TimestampConstant.NULL) {
            sink.attr("range").val('(').val(fromFunc).val(',').val(toFunc).val(')');
        }
        sink.attr("stride").val('\'').val(samplingInterval).val(samplingIntervalUnit).val('\'');

        // print values omitting the timestamp column
        // since we added an extra artificial null
        sink.attr("values").val('[');
        for (int i = 0; i < valueFuncs.size(); i++) {
            if (i == timestampIndex) {
                continue;
            }

            sink.val(valueFuncs.getQuick(i));

            if (i != 0 && i != valueFuncs.size() - 1) {
                sink.val(',');
            }
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
        Misc.freeObjList(valueFuncs);
    }

    private static class FillRangeRecordCursor implements NoRandomAccessRecordCursor {
        private final FillRangeRecord fillingRecord = new FillRangeRecord();
        private final FillRangeTimestampConstant fillingTimestampFunc = new FillRangeTimestampConstant();
        private final Function fromFunc;
        private final int timestampIndex;
        private final TimestampSampler timestampSampler;
        private final Function toFunc;
        private final ObjList<Function> valueFuncs;
        private RecordCursor baseCursor;
        private Record baseRecord;
        private long currTimestamp;
        private long fillTimestamp;
        private boolean gapFilling;
        private boolean initialised = false;
        private long maxTimestamp;
        private long minTimestamp;

        private FillRangeRecordCursor(
                TimestampSampler timestampSampler,
                @NotNull Function fromFunc,
                @NotNull Function toFunc,
                ObjList<Function> valueFuncs,
                int timestampIndex
        ) {
            this.timestampSampler = timestampSampler;
            this.fromFunc = fromFunc;
            this.toFunc = toFunc;
            this.valueFuncs = valueFuncs;
            this.timestampIndex = timestampIndex;
        }

        @Override
        public void close() {
            baseCursor = Misc.free(baseCursor);
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

            if (!initialised) {
                return initLoop();
            }

            fillTimestamp = timestampSampler.nextTimestamp(fillTimestamp);

            // bounds check
            if (fillTimestamp >= maxTimestamp) {
                return false;
            }


            if (gapFilling) {

                // we cannot overshoot
                assert fillTimestamp <= currTimestamp;

                // we must have had fill < curr

                if (fillTimestamp == currTimestamp) {
                    // no more to fill
                    gapFilling = false;
                }

                // either we fill, or use the existing record
                return true;
            }

            // if not filling, we get the next record
            if (baseCursor.hasNext()) {

                // get next record timestamp
                currTimestamp = baseRecord.getTimestamp(timestampIndex);

                // we cannot overshoot
                assert fillTimestamp <= currTimestamp;

                if (fillTimestamp < currTimestamp) {
                    gapFilling = true;
                }

                return true;
            }

            // no more records
            if (maxTimestamp != Long.MAX_VALUE && fillTimestamp < maxTimestamp) {
                currTimestamp = maxTimestamp;
                gapFilling = true;
                return true;
            }

            return false;
        }

        @Override
        public long size() {
            // Can be improved, potentially we know the size if both TO and FROM are set
            if (minTimestamp != Long.MIN_VALUE && maxTimestamp != Long.MAX_VALUE) {
                timestampSampler.setStart(minTimestamp);
                long ts = minTimestamp, n = 0;
                while (ts <= maxTimestamp) {
                    timestampSampler.nextTimestamp(ts);
                    n++;
                }
                return n;
            }
            return -1;
        }

        @Override
        public void toTop() {
            baseCursor.toTop();
            gapFilling = false;
            initialised = false;
            fillTimestamp = minTimestamp;
            currTimestamp = Long.MIN_VALUE;
        }

        private Function getFillFunction(int col) {
            if (col == timestampIndex) {
                return fillingTimestampFunc;
            }
            return valueFuncs.getQuick(col);
        }

        private boolean initLoop() {

            initialised = true;

            // if we have a min timestamp, we should start the fill from there
            if (minTimestamp != Long.MIN_VALUE) {
                timestampSampler.setStart(minTimestamp);
                fillTimestamp = minTimestamp;
            }

            // if there is real data
            if (baseCursor.hasNext()) {
                // set that as our current timestamp
                currTimestamp = baseRecord.getTimestamp(timestampIndex);

                // if we didn't have a min timestamp, then we set min to be the first entry
                if (minTimestamp == Long.MIN_VALUE) {
                    fillTimestamp = currTimestamp;
                    minTimestamp = currTimestamp;
                    timestampSampler.setStart(fillTimestamp);
                }

                if (fillTimestamp < currTimestamp) {
                    gapFilling = true;
                }

                return true;
            } else {
                // if we have a suitable range to fill
                if (minTimestamp != Long.MIN_VALUE && maxTimestamp != Long.MIN_VALUE) {
                    assert minTimestamp <= maxTimestamp;

                    // we fill from min up to max
                    fillTimestamp = minTimestamp;
                    currTimestamp = maxTimestamp;
                    gapFilling = true;
                    return true;
                }

                // no real data, no valid fill, so nothing to return
                return false;
            }
        }

        private void initTimestamps(Function fromFunc, Function toFunc) {
            minTimestamp = fromFunc == TimestampConstant.NULL ? Long.MIN_VALUE : fromFunc.getTimestamp(null);
            maxTimestamp = toFunc == TimestampConstant.NULL ? Long.MAX_VALUE : toFunc.getTimestamp(null);
        }

        private void initValueFuncs(ObjList<Function> valueFuncs) {
            // can't just check null, as we use this as the placeholder value
            if (valueFuncs.size() - 1 < timestampIndex) {
                // timestamp is the last column, so we add it
                valueFuncs.insert(timestampIndex, 1, null);
                return;
            }

            // else we grab the value in the corresponding slot
            final Function func = valueFuncs.getQuick(timestampIndex);

            // if it is a real function, i.e we've not added our placeholder null
            if (func != null) {
                // then we insert at this position
                valueFuncs.insert(timestampIndex, 1, null);
            }
        }

        private void of(
                RecordCursor baseCursor,
                SqlExecutionContext executionContext
        ) throws SqlException {
            this.baseCursor = baseCursor;
            Function.initNcFunctions(valueFuncs, baseCursor, executionContext);
            fromFunc.init(baseCursor, executionContext);
            toFunc.init(baseCursor, executionContext);
            initTimestamps(fromFunc, toFunc);
            initValueFuncs(valueFuncs);
            baseRecord = baseCursor.getRecord();
            toTop();
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
            public long getLongIPv4(int col) {
                return getLong(col);
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
                        return fillTimestamp;
                    } else {
                        return getFillFunction(col).getTimestamp(null);
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
            @Override
            public long getTimestamp(Record rec) {
                return fillTimestamp;
            }
        }
    }
}
