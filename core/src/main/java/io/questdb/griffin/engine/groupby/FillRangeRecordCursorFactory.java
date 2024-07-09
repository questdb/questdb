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
import io.questdb.cairo.GenericRecordMetadata;
import io.questdb.cairo.TableColumnMetadata;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.*;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.constants.TimestampConstant;
import io.questdb.griffin.engine.functions.date.TimestampFloorOffsetFunctionFactory;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.BitSet;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;


public class FillRangeRecordCursorFactory extends AbstractRecordCursorFactory {
    public static final GenericRecordMetadata DEFAULT_COUNT_METADATA = new GenericRecordMetadata();
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
                            fillOffset++;
                            nextBucket = timestampSampler.nextTimestamp(nextBucket);
                        }

                        while (presentRecords.get(fillOffset) && fillOffset < timestampSampler.bucketIndex(maxTimestamp)) {
                            fillOffset++;
                            nextBucket = timestampSampler.nextTimestamp(nextBucket);
                        }
                    }
                    return true;
                }

                do {
                    fillOffset++;
                    nextBucket = timestampSampler.nextTimestamp(nextBucket);
                }
                while (presentRecords.get(fillOffset) &&
                        fillOffset < timestampSampler.bucketIndex(maxTimestamp));

                if (fillOffset <= timestampSampler.bucketIndex(maxTimestamp)) {
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

        private class FillRangeRecord implements Record {

            @Override
            public double getDouble(int col) {
                if (!gapFilling) {
                    final double d = baseRecord.getDouble(col);
                    if (!Double.isNaN(d)) {
                        return d;
                    }
                }
                return getFillFunction(col).getDouble(null);
            }

            @Override
            public long getLong(int col) {
                if (!gapFilling) {
                    final long l = baseRecord.getLong(col);
                    if (l != Long.MIN_VALUE) {
                        return l;
                    }
                }
                return getFillFunction(col).getLong(null);
            }

            @Override
            public long getTimestamp(int col) {
                if (!gapFilling) {
                    final long l = baseRecord.getLong(col);
                    if (l != Long.MIN_VALUE) {
                        return l;
                    }
                }

                if (col == timestampIndex) {
                    return nextBucket;
                } else {
                    return getFillFunction(col).getLong(null);
                }
            }
        }
    }

    static {
        DEFAULT_COUNT_METADATA.add(new TableColumnMetadata("fill", ColumnType.DOUBLE));
    }
}
