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
import io.questdb.std.BitSet;
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
    private final RecordMetadata metadata;
    private final long samplingInterval;
    private final char samplingIntervalUnit;
    private final int timestampIndex;
    private final Function toFunc;
    private final ObjList<Function> valueFuncs;

    public FillRangeRecordCursorFactory(
            RecordMetadata metadata,
            RecordCursorFactory base,
            Function fromFunc,
            Function toFunc,
            long samplingInterval,
            char samplingIntervalUnit,
            TimestampSampler timestampSampler,
            ObjList<Function> fillValues,
            int timestampIndex
    ) {
        super(metadata);
        this.base = base;
        this.fromFunc = fromFunc;
        this.toFunc = toFunc;

        // needed for the EXPLAIN plan
        this.samplingInterval = samplingInterval;
        this.samplingIntervalUnit = samplingIntervalUnit;
        this.timestampIndex = timestampIndex;
        this.valueFuncs = fillValues;
        this.metadata = metadata;
        this.cursor = new FillRangeRecordCursor(timestampSampler, fromFunc, toFunc, fillValues, timestampIndex);
    }

    @Override
    public RecordCursorFactory getBaseFactory() {
        return base;
    }

    @Override
    public RecordCursor getCursor(SqlExecutionContext executionContext) throws SqlException {
        if (metadata.getColumnCount() > valueFuncs.size() + 1) {
            if (valueFuncs.size() == 1 && valueFuncs.getQuick(0).isNullConstant()) {
                final int diff = (metadata.getColumnCount() - 1);
                // skip one entry as it should be the designated timestamp
                for (int i = 1; i < diff; i++) {
                    valueFuncs.add(NullConstant.NULL);
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
    public RecordMetadata getMetadata() {
        return metadata;
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
        // Cache line size on Xeon, EPYC processors
        private static final int DEFAULT_BITSET_SIZE = 64 * 8;
        private static final int RANGE_FULLY_BOUND = 1;
        private static final int RANGE_LOWER_BOUND = 2;
        private static final int RANGE_UNBOUNDED = 0;
        private static final int RANGE_UPPER_BOUND = 3;

        private final FillRangeRecord fillingRecord = new FillRangeRecord();
        private final FillRangeTimestampConstant fillingTimestampFunc = new FillRangeTimestampConstant();
        private final Function fromFunc;
        private final int timestampIndex;
        private final TimestampSampler timestampSampler;
        private final Function toFunc;
        private final ObjList<Function> valueFuncs;
        private RecordCursor baseCursor;
        private Record baseRecord;
        private int bucketIndex;
        private int finalBucketIndex;
        private long fromTimestamp;
        private boolean gapFilling;
        private long maxTimestamp;
        private long minTimestamp;
        private long nextBucketTimestamp;
        private BitSet presentRecords;
        private int rangeBound;
        private long toTimestamp;

        private FillRangeRecordCursor(TimestampSampler timestampSampler,
                                      @NotNull Function fromFunc,
                                      @NotNull Function toFunc,
                                      ObjList<Function> valueFuncs,
                                      int timestampIndex) {
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
            if (baseRecord == null) {
                baseRecord = baseCursor.getRecord();
            }

            if (baseCursor.hasNext()) {
                final long timestamp = baseRecord.getTimestamp(timestampIndex);
                final int bucketIndex = timestampSampler.bucketIndex(timestamp);
                presentRecords.set(bucketIndex);

                if (timestamp < minTimestamp) {
                    minTimestamp = timestamp;
                }

                if (timestamp > maxTimestamp) {
                    maxTimestamp = timestamp;
                }

                assert presentRecords.capacity() > bucketIndex;
                return true;
            } else {
                // otherwise, we need to use bitset to fill
                if (!gapFilling) {
                    gapFilling = true;
                    finalBucketIndex = timestampSampler.bucketIndex(maxTimestamp);

                    // we need to get up to date, since the sampler started at unix epoch, not min timestamp
                    if (rangeBound == RANGE_UPPER_BOUND || rangeBound == RANGE_UNBOUNDED) {
                        bucketIndex = timestampSampler.bucketIndex(minTimestamp);
                        nextBucketTimestamp = minTimestamp;
                    }

                    // if there are no records, then timestamps won't be set correctly i.e
                    // therefore bucket index is garbage
                    // check for this and fall out, since we can't fill
                    if (bucketIndex < 0) {
                        return false;
                    }

                    while (recordWasPresent()) {
                        moveToNextBucket();
                    }

                    return notAtEndOfBitset();
                }

                do {
                    moveToNextBucket();
                } while (recordWasPresent());

                return notAtEndOfBitset();
            }
        }

        @Override
        public long size() {
            return -1;
        }

        @Override
        public void toTop() {
            baseCursor.toTop();
            presentRecords.clear();
            gapFilling = false;
            baseRecord = null;
            bucketIndex = 0;
            finalBucketIndex = -1;
        }

        private Function getFillFunction(int col) {
            if (col == timestampIndex) {
                return fillingTimestampFunc;
            }
            return valueFuncs.getQuick(col);
        }

        private void initBounds(Function fromFunc, Function toFunc) {
            if (fromFunc != TimestampConstant.NULL && toFunc != TimestampConstant.NULL) {
                rangeBound = RANGE_FULLY_BOUND;
                return;
            }

            if (fromFunc != TimestampConstant.NULL) {
                rangeBound = RANGE_LOWER_BOUND;
                return;
            }

            if (toFunc != TimestampConstant.NULL) {
                rangeBound = RANGE_UPPER_BOUND;
                return;
            }

            rangeBound = RANGE_UNBOUNDED;
        }

        private void initTimestamps(Function fromFunc, Function toFunc) {
            if (fromFunc != TimestampConstant.NULL) {
                fromTimestamp = fromFunc.getTimestamp(null);
            }

            if (toFunc != TimestampConstant.NULL) {
                toTimestamp = toFunc.getTimestamp(null);
            }

            timestampSampler.setStart(fromTimestamp);

            nextBucketTimestamp = fromTimestamp;

            minTimestamp = fromFunc == TimestampConstant.NULL ? Long.MAX_VALUE : fromTimestamp;
            maxTimestamp = toFunc == TimestampConstant.NULL ? Long.MIN_VALUE : toTimestamp;
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

        private void moveToNextBucket() {
            bucketIndex++;
            nextBucketTimestamp = timestampSampler.nextTimestamp(nextBucketTimestamp);
        }

        private boolean notAtEndOfBitset() {
            assert finalBucketIndex != -1;
            if (rangeBound == RANGE_LOWER_BOUND || rangeBound == RANGE_UNBOUNDED || nextBucketTimestamp == maxTimestamp) {
                return bucketIndex < finalBucketIndex;
            } else {
                return bucketIndex <= finalBucketIndex;
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
            if (presentRecords == null) {
                presentRecords = new BitSet(toFunc != TimestampConstant.NULL ? timestampSampler.bucketIndex(toTimestamp) : DEFAULT_BITSET_SIZE);
            }
            initValueFuncs(valueFuncs);
            initBounds(fromFunc, toFunc);
            toTop();
        }

        private boolean recordWasPresent() {
            assert finalBucketIndex != -1;
            return presentRecords.get(bucketIndex) && bucketIndex <= finalBucketIndex;
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
            @Override
            public long getTimestamp(Record rec) {
                return nextBucketTimestamp;
            }
        }
    }
}
