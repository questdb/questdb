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
import io.questdb.std.Numbers;
import io.questdb.std.NumericException;
import io.questdb.std.datetime.microtime.Timestamps;

public class GenerateSeriesTimestampStringRecordCursorFactory extends AbstractGenerateSeriesRecordCursorFactory {
    private static final RecordMetadata METADATA;
    private GenerateSeriesTimestampStringRecordCursor cursor;

    public GenerateSeriesTimestampStringRecordCursorFactory(Function startFunc, Function endFunc, Function stepFunc, int position) throws SqlException {
        super(METADATA, startFunc, endFunc, stepFunc, position);
    }

    @Override
    public RecordCursor getCursor(SqlExecutionContext executionContext) throws SqlException {
        if (cursor == null) {
            cursor = new GenerateSeriesTimestampStringRecordCursor(startFunc, endFunc, stepFunc);
        }
        cursor.of(executionContext);
        return cursor;
    }

    @Override
    public boolean recordCursorSupportsRandomAccess() {
        if (cursor == null) {
            return false;
        } else {
            switch (cursor.unit) {
                case 'M':
                case 'y':
                    return false;
                default:
                    return true;
            }
        }
    }

    private static class GenerateSeriesTimestampStringRecordCursor extends AbstractGenerateSeriesRecordCursor {
        private final GenerateSeriesTimestampStringRecord record = new GenerateSeriesTimestampStringRecord();
        public int stride;
        private TimestampAddFunctionFactory.LongAddIntFunction adder;
        private long curr;
        private long end;
        private long start;
        private char unit;

        public GenerateSeriesTimestampStringRecordCursor(Function startFunc, Function endFunc, Function stepFunc) {
            super(startFunc, endFunc, stepFunc);
        }

        @Override
        public void calculateSize(SqlExecutionCircuitBreaker circuitBreaker, Counter counter) {
            toTop();
            while (hasNext()) {
                counter.inc();
            }
        }

        @Override
        public Record getRecord() {
            return record;
        }

        @Override
        public boolean hasNext() {
            curr = adder.add(curr, stride);
            if (curr == Long.MIN_VALUE) {
                return false;
            }
            if (stride >= 0) {
                return curr <= end;
            } else {
                return curr >= end;
            }
        }

        public void of(SqlExecutionContext executionContext) throws SqlException {
            super.of(executionContext);
            this.start = startFunc.getTimestamp(null);
            this.end = endFunc.getTimestamp(null);

            final CharSequence str = stepFunc.getStrA(null);

            if (str != null) {
                if (str.length() == 1) {
                    unit = str.charAt(0);
                } else if (str.length() > 1) {
                    unit = str.charAt(str.length() - 1);
                    try {
                        stride = Numbers.parseInt(str, 0, str.length() - 1);
                        if (stride <= 0) {
                            unit = 1;
                        }
                    } catch (NumericException ignored) {
                        unit = 1;
                    }
                } else {
                    unit = 1; // report it as an empty unit rather than null
                }
            } else {
                unit = 1;
            }
            this.adder = TimestampAddFunctionFactory.lookupAddFunction(unit);

            if (this.adder == null) {
                throw SqlException.$(-1, "invalid unit [unit=").put(str).put(']');
            }

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
                case 'u':
                    break;
                default:
                    return -1;
            }
            return (Math.abs(end - start) / Math.abs(micros)) + 1;
        }

        @Override
        public void toTop() {
            curr = adder.add(start, -stride);
        }

        private class GenerateSeriesTimestampStringRecord implements Record {
            @Override
            public long getRowId() {
                return Math.abs(start - curr) / Math.abs(stride);
            }

            @Override
            public long getTimestamp(int col) {
                return curr;
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
