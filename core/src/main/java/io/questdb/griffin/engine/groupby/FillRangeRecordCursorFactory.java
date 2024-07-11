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
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.*;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.constants.NullConstant;
import io.questdb.griffin.engine.functions.constants.TimestampConstant;
import io.questdb.griffin.engine.functions.date.TimestampFloorOffsetFunctionFactory;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.*;
import io.questdb.std.str.CharSink;
import io.questdb.std.str.Utf16Sink;
import io.questdb.std.str.Utf8Sequence;
import io.questdb.std.str.Utf8Sink;
import org.jetbrains.annotations.Nullable;


public class FillRangeRecordCursorFactory extends AbstractRecordCursorFactory {
    public static final Log LOG = LogFactory.getLog(FillRangeRecordCursorFactory.class);
    private final RecordCursorFactory base;
    private final FillRangeRecordCursor cursor = new FillRangeRecordCursor();
    private final Function from;
    private final CharSequence stride;
    private final int timestampIndex;
    private final Function to;
    private final ObjList<Function> values;
    RecordMetadata metadata;

    public FillRangeRecordCursorFactory(RecordMetadata metadata, RecordCursorFactory base, Function from, Function to, CharSequence stride, ObjList<Function> fillValues, int timestampIndex) {
        super(metadata);
        this.base = base;
        this.from = from;
        this.to = to;
        this.stride = stride;
        this.timestampIndex = timestampIndex;
        this.values = fillValues;
        this.metadata = metadata;
    }

    @Override
    public RecordCursorFactory getBaseFactory() {
        return base;
    }

    @Override
    public RecordCursor getCursor(SqlExecutionContext executionContext) throws SqlException {
        if (metadata.getColumnCount() > this.values.size() + 1) {

            if (this.values.size() == 1 && this.values.getQuick(0).isNullConstant()) {
                final int diff = (metadata.getColumnCount() - 1);
                for (int i = 0; i < diff; i++) {
                    values.add(NullConstant.NULL);
                }
            }
            throw SqlException.$(-1, "not enough fill values");
        }


        final RecordCursor baseCursor = base.getCursor(executionContext);
        try {
            cursor.of(baseCursor, executionContext.getCircuitBreaker(), from, to, stride, values, timestampIndex);
            return cursor;
        } catch (Throwable th) {
            baseCursor.close();
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
        sink.attr("range").val('(').val(from).val(',').val(to).val(')');
        sink.attr("stride").val('\'').val(stride).val('\'');
        sink.attr("values").val(values);
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
    }

    private static class FillRangeRecordCursor implements NoRandomAccessRecordCursor {
        private static final int RANGE_FULLY_BOUND = 0;
        private static final int RANGE_LOWER_BOUND = 1;
        private static final int RANGE_UPPER_BOUND = 2;
        protected final FillRangeRecord fillingRecord = new FillRangeRecord();
        protected final TimestampFloorOffsetFunctionFactory flooringFactory = new TimestampFloorOffsetFunctionFactory();
        protected RecordCursor baseCursor;
        protected Record baseRecord;
        protected SqlExecutionCircuitBreaker circuitBreaker;
        protected int fillOffset;
        protected long fromTimestamp;
        protected boolean gapFilling;
        protected long maxTimestamp;
        protected long minTimestamp;
        protected long nextBucket;
        protected BitSet presentRecords;
        protected int timestampIndex;
        protected TimestampSampler timestampSampler;
        protected long toTimestamp;
        protected ObjList<Function> values;
        private int rangeBound;

        @Override
        public void close() {
            baseCursor = Misc.free(baseCursor);
        }

        @Override
        public Record getRecord() {
            return fillingRecord;
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
                    presentRecords.checkCapacity(bucketIndex);
                }

                return true;

            } else {
                // otherwise, we need to use bitset to fill
                if (!gapFilling) {
                    gapFilling = true;

                    if (rangeBound == RANGE_UPPER_BOUND) {
                        while (nextBucket < minTimestamp) {
                            moveToNextBucket();
                        }

                        while (recordWasPresent()) {
                            moveToNextBucket();
                        }
                    }
                    return true;
                }

                do {
                    moveToNextBucket();
                }
                while (recordWasPresent());

                if (notAtEndOfBitset()) {
                    return true;
                }

                return false;
            }
        }

        @Override
        public long size() {
            return -1;
        }

        @Override
        public void toTop() {
            baseCursor.toTop();
            gapFilling = false;
            baseRecord = null;
            fillOffset = 0;
        }

        private Function getFillFunction(int col) {
            if (col == timestampIndex) {
                return TimestampConstant.newInstance(nextBucket);
            }

            return values.getQuick(col < timestampIndex ? col : col - 1);
        }

        private void initBounds(Function from, Function to) {
            if (from != TimestampConstant.NULL
                    && to != TimestampConstant.NULL) {
                rangeBound = RANGE_FULLY_BOUND;
                return;
            }

            if (from != TimestampConstant.NULL) {
                rangeBound = RANGE_LOWER_BOUND;
                return;
            }

            if (to != TimestampConstant.NULL) {
                rangeBound = RANGE_UPPER_BOUND;
                return;
            }
        }


        private void initTimestamps(Function from, Function to, CharSequence stride) throws SqlException {
            if (from != TimestampConstant.NULL) {
                fromTimestamp = from.getTimestamp(null);
            }

            if (to != TimestampConstant.NULL) {
                toTimestamp = to.getTimestamp(null);
            }

            timestampSampler = TimestampSamplerFactory.getInstance(stride, 0);
            timestampSampler.setStart(fromTimestamp);

            nextBucket = fromTimestamp;

            minTimestamp = from == TimestampConstant.NULL ? Long.MAX_VALUE : fromTimestamp;
            maxTimestamp = to == TimestampConstant.NULL ? Long.MIN_VALUE : toTimestamp;
        }

        private void moveToNextBucket() {
            fillOffset++;
            nextBucket = timestampSampler.nextTimestamp(nextBucket);
        }

        private boolean notAtEndOfBitset() {
            return fillOffset <= timestampSampler.bucketIndex(maxTimestamp);
        }

        private void of(RecordCursor baseCursor, SqlExecutionCircuitBreaker circuitBreaker, Function from, Function to, CharSequence stride, ObjList<Function> values, int timestampIndex) throws SqlException {
            assert from != null;
            assert to != null;
            this.baseCursor = baseCursor;
            this.circuitBreaker = circuitBreaker;
            this.timestampIndex = timestampIndex;
            this.values = values;
            initTimestamps(from, to, stride);
            presentRecords = new BitSet(to != TimestampConstant.NULL ? timestampSampler.bucketIndex(toTimestamp) : 64 * 8);
            initBounds(from, to);
            toTop();
        }

        private boolean recordWasPresent() {
            return presentRecords.get(fillOffset) && fillOffset <= timestampSampler.bucketIndex(maxTimestamp);
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
            @SuppressWarnings("unused")
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
            public void getStr(int col, Utf16Sink utf16Sink) {
                if (gapFilling) {
                    getFillFunction(col).getStr(null, utf16Sink);
                } else {
                    baseRecord.getStr(col, utf16Sink);
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
                        return nextBucket;
                    } else {
                        return getFillFunction(col).getLong(null);
                    }
                } else {
                    return baseRecord.getTimestamp(col);
                }
            }

            @Override
            public void getVarchar(int col, Utf8Sink utf8Sink) {
                if (gapFilling) {
                    getFillFunction(col).getVarchar(null, utf8Sink);
                } else {
                    baseRecord.getVarchar(col, utf8Sink);
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
    }

}
