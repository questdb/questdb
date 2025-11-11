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
import io.questdb.cairo.TimestampDriver;
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


public class GenerateSeriesTimestampStringRecordCursorFactory extends AbstractGenerateSeriesRecordCursorFactory {
    private static final RecordMetadata METADATA_MICROS;
    private static final RecordMetadata METADATA_NANOS;
    private static final ThreadLocal<GenerateSeriesTimestampStringRecordCursor.GenerateSeriesPeriod> tlSampleByUnit = new ThreadLocal<>(GenerateSeriesTimestampStringRecordCursor.GenerateSeriesPeriod::new);
    private final TimestampDriver timestampDriver;
    private GenerateSeriesTimestampStringRecordCursor cursor;

    public GenerateSeriesTimestampStringRecordCursorFactory(int timestampType, Function startFunc, Function endFunc, Function stepFunc, IntList argPositions) throws SqlException {
        super(getMetadata(timestampType), startFunc, endFunc, stepFunc, argPositions);
        this.timestampDriver = ColumnType.getTimestampDriver(timestampType);
    }

    @Override
    public RecordCursor getCursor(SqlExecutionContext executionContext) throws SqlException {
        if (cursor == null) {
            cursor = new GenerateSeriesTimestampStringRecordCursor(timestampDriver, startFunc, endFunc, stepFunc);
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

    static RecordMetadata getMetadata(int timestampType) {
        return switch (timestampType) {
            case ColumnType.TIMESTAMP_MICRO -> METADATA_MICROS;
            case ColumnType.TIMESTAMP_NANO -> METADATA_NANOS;
            default -> null;
        };
    }

    private static class GenerateSeriesTimestampStringRecordCursor extends AbstractGenerateSeriesRecordCursor {
        private final GenerateSeriesTimestampStringRecord recordA = new GenerateSeriesTimestampStringRecord();
        private final GenerateSeriesTimestampStringRecord recordB = new GenerateSeriesTimestampStringRecord();
        private final TimestampDriver timestampDriver;
        public int stride;
        private TimestampDriver.TimestampAddMethod adder;
        private long end;
        private long start;
        private char unit;

        public GenerateSeriesTimestampStringRecordCursor(TimestampDriver driver, Function startFunc, Function endFunc, Function stepFunc) {
            super(startFunc, endFunc, stepFunc);
            this.timestampDriver = driver;
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
            this.start = timestampDriver.from(startFunc.getTimestamp(null), ColumnType.getTimestampType(startFunc.getType()));
            this.end = timestampDriver.from(endFunc.getTimestamp(null), ColumnType.getTimestampType(endFunc.getType()));
            final CharSequence stepStr = stepFunc.getStrA(null);
            GenerateSeriesPeriod sbu = tlSampleByUnit.get();
            sbu.parse(stepStr, stepPosition);
            this.adder = timestampDriver.getAddMethod(sbu.unit);
            if (this.adder == null) {
                throwInvalidPeriod(stepStr, stepPosition);
            }

            unit = sbu.unit;
            if (adder.add(0, sbu.stride) == 0) {
                throw SqlException.$(stepPosition, "step cannot be zero");
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
        public long preComputedStateSize() {
            return 0;
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
            return Math.abs(end - start) / adder.add(0, Math.abs(stride)) + 1;
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
            return switch (unit) {
                case 'M', 'y' -> false;
                default -> true;
            };
        }

        @Override
        public void toTop() {
            recordA.of(adder.add(start, -stride));
        }

        private long adjustStride() {
            return switch (unit) {
                case 'w' -> timestampDriver.fromWeeks(stride);
                case 'd' -> timestampDriver.fromDays(stride);
                case 'h' -> timestampDriver.fromHours(stride);
                case 'm' -> timestampDriver.fromMinutes(stride);
                case 's' -> timestampDriver.fromSeconds(stride);
                case 'T' -> timestampDriver.fromMillis(stride);
                case 'U', 'u' -> timestampDriver.fromMicros(stride);
                case 'n' -> timestampDriver.fromNanos(stride);
                default -> throw new UnsupportedOperationException();
            };
        }

        public static class GenerateSeriesPeriod {
            public int stride = 0;
            public char unit = (char) 0;

            public static boolean isPotentiallyValidUnit(char c) {
                return switch (c) {
                    // n:nanos U:micros u:micros-compatibility T:millis s:seconds m:minutes
                    // h:hours H:hours-compatibility d:days w:weeks M:months y:years
                    case 'n', 'u', 'U', 'T', 's', 'm', 'h', 'H', 'd', 'w', 'M', 'y' -> true;
                    default -> false;
                };
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
        metadata.add(0, new TableColumnMetadata("generate_series", ColumnType.TIMESTAMP_MICRO));
        metadata.setTimestampIndex(0);
        METADATA_MICROS = metadata;

        final GenericRecordMetadata metadata1 = new GenericRecordMetadata();
        metadata1.add(0, new TableColumnMetadata("generate_series", ColumnType.TIMESTAMP_NANO));
        metadata1.setTimestampIndex(0);
        METADATA_NANOS = metadata1;
    }
}
