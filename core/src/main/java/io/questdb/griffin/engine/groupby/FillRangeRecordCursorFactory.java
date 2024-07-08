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
import io.questdb.griffin.engine.functions.constants.ConstantFunction;
import io.questdb.griffin.engine.functions.constants.TimestampConstant;
import io.questdb.griffin.engine.functions.date.TimestampFloorOffsetFunctionFactory;
import io.questdb.std.Misc;
import io.questdb.std.datetime.microtime.Timestamps;


// initialise the class
// get the next record.
// compare timestamps
// if we need to fill, we cache that record and return the
// the fill record instead
// etc.
// fill based on how sample by works
// then its initialised by looking at from/to within
// the sample by rewrite

public class FillRangeRecordCursorFactory extends AbstractRecordCursorFactory {
    public static final GenericRecordMetadata DEFAULT_COUNT_METADATA = new GenericRecordMetadata();
    private final RecordCursorFactory base;
    private final FillRangeRecordCursor cursor = new FillRangeRecordCursor();
    private long currentBucket;
    private long expectedBucket;
    private Function from;
    private long nextBucket;
    private CharSequence stride;
    private int timestampIndex;
    private Function to;


    public FillRangeRecordCursorFactory(RecordMetadata metadata, RecordCursorFactory base, Function from, Function to, CharSequence stride, int timestampIndex) {
        super(metadata);
        this.base = base;
        this.from = from;
        this.to = to;
        this.stride = stride;
        this.timestampIndex = timestampIndex;
    }

    @Override
    public RecordCursorFactory getBaseFactory() {
        return base;
    }

    @Override
    public RecordCursor getCursor(SqlExecutionContext executionContext) throws SqlException {
        final RecordCursor baseCursor = base.getCursor(executionContext);
        try {
            cursor.of(baseCursor, executionContext.getCircuitBreaker(), from, to, stride, timestampIndex);
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
        sink.optAttr("from", from);
        sink.optAttr("to", to);
        sink.optAttr("stride", stride);
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
        private RecordCursor baseCursor;
        private Record baseRecord;
        private SqlExecutionCircuitBreaker circuitBreaker;
        private ConstantFunction fillFunction;
        private FillRangeRecord fillingRecord = new FillRangeRecord();
        private TimestampFloorOffsetFunctionFactory flooringFactory = new TimestampFloorOffsetFunctionFactory();
        private long fromTimestamp;
        private boolean gapFilling;
        private boolean hasNext = true;
        private long nextBucket;
        private int strideMultiple;
        private char strideUnit;
        private int timestampIndex;
        private TimestampSampler timestampSampler;
        private long toTimestamp;

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

            // if we no longer need to fill, return to the real record
            if (gapFilling) {
                nextBucket = timestampSampler.nextTimestamp(nextBucket);
                if (nextBucket >= baseRecord.getTimestamp(timestampIndex)) {
                    gapFilling = false;
                }
                return true;
            }

            if (baseRecord == null) {
                baseRecord = baseCursor.getRecord();
            }

            // if we have no further records of either kind
            if (!baseCursor.hasNext() && !gapFilling && nextBucket >= toTimestamp) {
                return false;
            }


            final long timestamp = baseRecord.getTimestamp(timestampIndex);

            if (timestamp <= nextBucket) {
                nextBucket = timestampSampler.nextTimestamp(nextBucket);
            } else {
                // fill
                gapFilling = true;
            }

            return true;
        }

        @Override
        public long size() {
            return -1;
        }

        @Override
        public void toTop() {
            baseCursor.toTop();
            hasNext = true;
            timestampSampler.setStart(fromTimestamp);
            gapFilling = false;
            baseRecord = null;
        }

        private void initTimestamps(Function from, Function to, CharSequence stride) throws SqlException {
            if (from != null && from != TimestampConstant.NULL) {
                fromTimestamp = from.getTimestamp(null);
            }

            if (to != null && to != TimestampConstant.NULL) {
                toTimestamp = to.getTimestamp(null);
            }

            timestampSampler = TimestampSamplerFactory.getInstance(stride, 0);
            timestampSampler.setStart(fromTimestamp);

            nextBucket = fromTimestamp;

            strideMultiple = Timestamps.getStrideMultiple(stride);
            strideUnit = Timestamps.getStrideUnit(stride);
        }

        private void of(RecordCursor baseCursor, SqlExecutionCircuitBreaker circuitBreaker, Function from, Function to, CharSequence stride, int timestampIndex) throws SqlException {
            this.baseCursor = baseCursor;
            this.circuitBreaker = circuitBreaker;
            this.timestampIndex = timestampIndex;
            initTimestamps(from, to, stride);
            toTop();
        }


        private class FillRangeRecord implements Record {

            @Override
            public double getDouble(int col) {
                if (gapFilling) {
                    return Double.NaN;
                } else {
                    return baseRecord.getDouble(col);
                }
            }

            @Override
            public long getTimestamp(int col) {
                if (gapFilling) {
                    return nextBucket;
                } else {
                    return baseRecord.getTimestamp(col);
                }
            }
        }
    }


    static {
        DEFAULT_COUNT_METADATA.add(new TableColumnMetadata("fill", ColumnType.DOUBLE));
    }
}
