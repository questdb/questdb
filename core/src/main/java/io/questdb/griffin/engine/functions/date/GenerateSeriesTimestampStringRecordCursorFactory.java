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

        @Override
        public void calculateSize(SqlExecutionCircuitBreaker circuitBreaker, Counter counter) {
            toTop();
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
                case 'u':
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
                case 'u':
                    return stride;
                default:
                    throw new UnsupportedOperationException();
            }
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
