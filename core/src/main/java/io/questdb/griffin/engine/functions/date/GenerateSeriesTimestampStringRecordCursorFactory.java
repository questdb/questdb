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

package io.questdb.griffin.engine.functions.date;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.GenericRecordMetadata;
import io.questdb.cairo.TableColumnMetadata;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.IntList;
import io.questdb.std.Numbers;
import io.questdb.std.NumericException;
import io.questdb.std.ThreadLocal;
import io.questdb.std.datetime.microtime.Timestamps;
import org.jetbrains.annotations.Nullable;


public class GenerateSeriesTimestampStringRecordCursorFactory extends AbstractGenerateSeriesRecordCursorFactory {
    public static final TimestampAddFunctionFactory.LongAddIntFunction ADD_DAYS_FUNCTION = Timestamps::addDays;
    public static final TimestampAddFunctionFactory.LongAddIntFunction ADD_HOURS_FUNCTION = Timestamps::addHours;
    public static final TimestampAddFunctionFactory.LongAddIntFunction ADD_MICROS_FUNCTION = Timestamps::addMicros;
    public static final TimestampAddFunctionFactory.LongAddIntFunction ADD_MILLIS_FUNCTION = Timestamps::addMillis;
    public static final TimestampAddFunctionFactory.LongAddIntFunction ADD_MINUTES_FUNCTION = Timestamps::addMinutes;
    public static final TimestampAddFunctionFactory.LongAddIntFunction ADD_MONTHS_FUNCTION = Timestamps::addMonths;
    public static final TimestampAddFunctionFactory.LongAddIntFunction ADD_SECONDS_FUNCTION = Timestamps::addSeconds;
    public static final TimestampAddFunctionFactory.LongAddIntFunction ADD_WEEKS_FUNCTION = Timestamps::addWeeks;
    public static final TimestampAddFunctionFactory.LongAddIntFunction ADD_YEARS_FUNCTION = Timestamps::addYears;

    private static final RecordMetadata METADATA;
    private static final io.questdb.std.ThreadLocal<GenerateSeriesTimestampStringRecordCursor.GenerateSeriesPeriod> tlSampleByUnit = new ThreadLocal<>(GenerateSeriesTimestampStringRecordCursor.GenerateSeriesPeriod::new);
    private GenerateSeriesTimestampStringRecordCursor cursor;

    public GenerateSeriesTimestampStringRecordCursorFactory(Function startFunc, Function endFunc, Function stepFunc, IntList argPositions) throws SqlException {
        super(METADATA, startFunc, endFunc, stepFunc, argPositions);
    }

    public static @Nullable TimestampAddFunctionFactory.LongAddIntFunction lookupAddFunction(char period) {
        switch (period) {
            case 'u': // compatibility with dateadd syntax
            case 'U':
                return ADD_MICROS_FUNCTION;
            case 'T':
                return ADD_MILLIS_FUNCTION;
            case 's':
                return ADD_SECONDS_FUNCTION;
            case 'm':
                return ADD_MINUTES_FUNCTION;
            case 'H':
            case 'h': // compatibility with sample by syntax
                return ADD_HOURS_FUNCTION;
            case 'd':
                return ADD_DAYS_FUNCTION;
            case 'w':
                return ADD_WEEKS_FUNCTION;
            case 'M':
                return ADD_MONTHS_FUNCTION;
            case 'y':
                return ADD_YEARS_FUNCTION;
            default:
                return null;
        }
    }

    @Override
    public RecordCursor getCursor(SqlExecutionContext executionContext) throws SqlException {
        if (cursor == null) {
            cursor = new GenerateSeriesTimestampStringRecordCursor(startFunc, endFunc, stepFunc);
        }
        cursor.of(executionContext, stepPosition);
        return cursor;
    }

    @Override
    public int getScanDirection() {
        if (cursor != null && cursor.stride != 0) {
            return cursor.stride > 0 ? SCAN_DIRECTION_FORWARD : SCAN_DIRECTION_BACKWARD;
        }
        return SCAN_DIRECTION_FORWARD;
    }

    @Override
    public boolean recordCursorSupportsRandomAccess() {
        if (cursor == null) {
            return false;
        } else {
            return cursor.supportsRandomAccess();
        }
    }

    private static class GenerateSeriesTimestampStringRecordCursor extends AbstractGenerateSeriesRecordCursor {
        private final GenerateSeriesTimestampStringRecord recordA = new GenerateSeriesTimestampStringRecord();
        private final GenerateSeriesTimestampStringRecord recordB = new GenerateSeriesTimestampStringRecord();
        public int stride;
        private TimestampAddFunctionFactory.LongAddIntFunction adder;
        private long end;
        private long start;
        private char unit;

        public GenerateSeriesTimestampStringRecordCursor(Function startFunc, Function endFunc, Function stepFunc) {
            super(startFunc, endFunc, stepFunc);
        }

        public static void throwInvalidPeriod(CharSequence stepStr, int stepPosition) throws SqlException {
            throw SqlException.$(stepPosition, "invalid period [period=")
                    .put(stepStr)
                    .put(']');
        }

        @Override
        public void calculateSize(SqlExecutionCircuitBreaker circuitBreaker, Counter counter) {
            while (hasNext()) {
                counter.inc();
            }
        }

        @Override
        public Record getRecord() {
            return recordA;
        }

        @Override
        public Record getRecordB() {
            if (supportsRandomAccess()) {
                return recordB;
            }
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean hasNext() {
            recordA.of(adder.add(recordA.curr, stride));
            if (stride >= 0) {
                return recordA.curr <= end;
            } else {
                return recordA.curr >= end;
            }
        }

        public void of(SqlExecutionContext executionContext, int stepPosition) throws SqlException {
            super.of(executionContext);
            this.start = startFunc.getTimestamp(null);
            this.end = endFunc.getTimestamp(null);

            final CharSequence stepStr = stepFunc.getStrA(null);

            GenerateSeriesPeriod sbu = tlSampleByUnit.get();

            sbu.parse(stepStr, stepPosition);

            this.adder = lookupAddFunction(sbu.unit);

            if (this.adder == null) {
                throwInvalidPeriod(stepStr, stepPosition);
            }

            unit = sbu.unit;

            if (sbu.stride == 0) {
                throw SqlException.$(stepPosition, "stride cannot be zero");
            }

            stride = sbu.stride;

            // swap args round transparently if needed
            // so from/to are really a range
            if (start <= end && this.stride < 0
                    || start >= end && this.stride > 0) {
                final long temp = start;
                start = end;
                end = temp;
            }
            toTop();
        }

        @Override
        public void recordAt(Record record, long atRowId) {
            if (supportsRandomAccess()) {
                long micros = adjustStride();
                ((GenerateSeriesTimestampStringRecord) record).of(start + micros * (atRowId - 1));
                return;
            }
            throw new UnsupportedOperationException();
        }

        @Override
        public long size() {
            long micros = stride;
            switch (unit) {
                case 'w':
                    micros *= Timestamps.WEEK_MICROS;
                    break;
                case 'd':
                    micros *= Timestamps.DAY_MICROS;
                    break;
                case 'h':
                    micros *= Timestamps.HOUR_MICROS;
                    break;
                case 'm':
                    micros *= Timestamps.MINUTE_MICROS;
                    break;
                case 's':
                    micros *= Timestamps.SECOND_MICROS;
                    break;
                case 'T':
                    micros *= Timestamps.MILLI_MICROS;
                    break;
                case 'U':
                case 'u':
                    // todo: get rid of 'u' case whe nanosecond refactor happens
                    break;
                default:
                    return -1;
            }
            return (Math.abs(end - start) / Math.abs(micros)) + 1;
        }

        @Override
        public void skipRows(Counter rowCount) {
            if (supportsRandomAccess()) {
                long newRowId = recordA.getRowId() + rowCount.get()
                        - 1 // one-indexed
                        - 1 // we increment at the start of hasNext()
                        ;
                recordAt(recordA, newRowId);
            } else {
                super.skipRows(rowCount);
            }
        }

        public boolean supportsRandomAccess() {
            switch (unit) {
                case 'M':
                case 'y':
                    return false;
                default:
                    return true;
            }
        }

        @Override
        public void toTop() {
            recordA.of(adder.add(start, -stride));
        }

        private long adjustStride() {
            switch (unit) {
                case 'w':
                    return stride * Timestamps.WEEK_MICROS;
                case 'd':
                    return stride * Timestamps.DAY_MICROS;
                case 'h':
                    return stride * Timestamps.HOUR_MICROS;
                case 'm':
                    return stride * Timestamps.MINUTE_MICROS;
                case 's':
                    return stride * Timestamps.SECOND_MICROS;
                case 'T':
                    return stride * Timestamps.MILLI_MICROS;
                case 'U':
                case 'u':
                    // todo: get rid of 'u' case whe nanosecond refactor happens
                    return stride;
                default:
                    throw new UnsupportedOperationException();
            }
        }

        public static class GenerateSeriesPeriod {
            public int stride = 0;
            public char unit = (char) 0;

            public static boolean isPotentiallyValidUnit(char c) {
                switch (c) {
                    case 'u':  // compatibility
                    case 'U':
                        // micros
                    case 'T':
                        // millis
                    case 's':
                        // seconds
                    case 'm':
                        // minutes
                    case 'h':
                    case 'H': // compatibility
                        // hours
                    case 'd':
                        // days
                    case 'w':
                        // weeks
                    case 'M':
                        // months
                    case 'y':
                        return true;
                    default:
                        return false;
                }
            }

            public void clear() {
                of(0, (char) 0);
            }

            public void of(int stride, char unit) {
                this.stride = stride;
                this.unit = unit;
            }

            public boolean parse(CharSequence str, int position) throws SqlException {
                if (str == null) {
                    throw SqlException.$(position, "null step");
                }

                int len = str.length();
                switch (len) {
                    case 0:
                        throw SqlException.$(position, "empty step");
                    case 1:
                        unit = str.charAt(0);
                        stride = 1;
                        break;
                    case 2:
                        // rule out edge case: -y, -h etc.
                        if (str.charAt(0) == '-') {
                            unit = str.charAt(1);
                            stride = -1;
                            break;
                        }
                    default:
                        unit = str.charAt(str.length() - 1);
                        try {
                            stride = Numbers.parseInt(str, 0, str.length() - 1);
                        } catch (NumericException ignored) {
                            throwInvalidPeriod(str, position);
                        }
                }

                if (!isPotentiallyValidUnit(unit)) {
                    throwInvalidPeriod(str, position);
                }

                return true;
            }


            // todo: when nanosecond PR is fleshed out, either fold this into it, or move validation etc. to this class
        }

        private class GenerateSeriesTimestampStringRecord implements Record {

            private long curr;

            @Override
            public long getLong(int col) {
                return curr;
            }

            @Override
            public long getRowId() {
                if (supportsRandomAccess()) {
                    return Math.abs(start - curr) / Math.abs(adjustStride()) + 1;
                }
                throw new UnsupportedOperationException();
            }

            @Override
            public long getTimestamp(int col) {
                return curr;
            }

            public void of(long value) {
                curr = value;
            }
        }

    }

    static {
        final GenericRecordMetadata metadata = new GenericRecordMetadata();
        metadata.add(0, new TableColumnMetadata("generate_series", ColumnType.TIMESTAMP));
        metadata.setTimestampIndex(0);
        METADATA = metadata;
    }
}
