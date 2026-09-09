/*+*****************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2026 QuestDB
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
import io.questdb.std.CarrierLocal;


public class GenerateSeriesTimestampStringRecordCursorFactory extends AbstractGenerateSeriesRecordCursorFactory {
    private static final RecordMetadata METADATA_MICROS;
    private static final RecordMetadata METADATA_NANOS;
    private static final CarrierLocal<GenerateSeriesTimestampStringRecordCursor.GenerateSeriesPeriod> tlSampleByUnit = new CarrierLocal<>(GenerateSeriesTimestampStringRecordCursor.GenerateSeriesPeriod::new);
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
        // The cursor has not been opened yet (getScanDirection() is plan-time metadata).
        // Determine the order from a constant step: a leading '-' means the series counts
        // down, so the output descends. A bind-variable step is only known at runtime, so
        // the order cannot be guaranteed at plan time.
        if (stepFunc.isConstant()) {
            final CharSequence step = stepFunc.getStrA(null);
            if (step != null && !step.isEmpty()) {
                return step.charAt(0) == '-' ? SCAN_DIRECTION_BACKWARD : SCAN_DIRECTION_FORWARD;
            }
            return SCAN_DIRECTION_FORWARD;
        }
        return SCAN_DIRECTION_OTHER;
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
        // Whether the step has a constant tick width. A calendar step does not, so the
        // series it names is not an arithmetic progression: it has no closed form for a
        // row's value, and the walk below keeps the comparison this one replaces.
        private boolean isRandomAccess;
        // Rows already handed out, on the arithmetic arm. That walk counts rather than
        // comparing curr against end: neither end of the series is bounds-checked, so
        // every arithmetic position - toTop()'s start - stepTicks included - can run off
        // the end of the long range, and a comparison cannot tell a wrapped value from an
        // in-range one.
        private long rowIndex;
        private long size;
        private long start;
        // The step's width in the driver's ticks, on the arithmetic arm. Read once in of()
        // rather than re-derived per row from the unit.
        private long stepTicks;
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
            circuitBreaker.statefulThrowExceptionIfTripped();
            if (isRandomAccess) {
                if (rowIndex >= size) {
                    return false;
                }
                rowIndex++;
                // start + stepTicks * (rowIndex - 1) is the row's definition, and every row
                // of the series fits in a long even when the span between the ends does not.
                // The multiply may wrap on the way there; two's complement carries it back,
                // so the sum is exact for every row the count admits.
                recordA.of(start + stepTicks * (rowIndex - 1));
                return true;
            }
            final long next = adder.add(recordA.curr, stride);
            if (rowIndex > 0 && (stride >= 0 ? next <= recordA.curr : next >= recordA.curr)) {
                // A calendar step moves strictly in its own direction whenever it lands
                // inside the long range, and the calendar addition does not clamp - so a
                // step that failed to move ran off the end and wrapped. Every position
                // after that is inside the series by comparison and outside it in fact,
                // which is what used to carry this walk through the whole far half of the
                // range. Skipped on the first advance because toTop()'s sentinel is itself
                // one step outside the series and may have wrapped to get there.
                return false;
            }
            recordA.of(next);
            if (stride >= 0 ? recordA.curr > end : recordA.curr < end) {
                return false;
            }
            rowIndex++;
            return true;
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
            isRandomAccess = switch (unit) {
                case 'M', 'y' -> false;
                default -> true;
            };
            stepTicks = isRandomAccess ? adjustStride() : 0;

            // swap args round transparently if needed
            // so from/to are really a range
            if (start <= end && this.stride < 0
                    || start >= end && this.stride > 0) {
                final long temp = start;
                start = end;
                end = temp;
            }
            if (isRandomAccess) {
                // The span between the ends can need the 64th bit, so subtract in the
                // direction that cannot go negative and divide the result as unsigned.
                // Math.abs(stepTicks) is likewise the step's magnitude read unsigned.
                // Saturate on the +1 rather than wrapping: a count no long can hold must
                // not come back as a negative row count.
                final long steps = Long.divideUnsigned(
                        end >= start ? end - start : start - end,
                        Math.abs(stepTicks)
                );
                size = steps < 0 || steps == Long.MAX_VALUE ? Long.MAX_VALUE : steps + 1;
            } else {
                // A calendar step has no constant tick width, so nothing here can turn the
                // span into an exact count: repeated addition clamps the day of month, which
                // makes the walk's length a property of the walk rather than of the span.
                // This stays the estimate it has always been, and the walk stays the
                // comparison it has always been. What it must not do is come back negative,
                // so read the span unsigned exactly as the arm above does - Math.abs is
                // negative for a span that needs the 64th bit, and so is the step's own
                // width for a stride that overflowed the range - and saturate on the +1.
                final long stepEstimate = adder.add(0, Math.abs(stride));
                final long steps = Long.divideUnsigned(
                        end >= start ? end - start : start - end,
                        stepEstimate < 0 ? -stepEstimate : stepEstimate
                );
                size = steps < 0 || steps == Long.MAX_VALUE ? Long.MAX_VALUE : steps + 1;
            }
            toTop();
        }

        @Override
        public long preComputedStateSize() {
            return 0;
        }

        @Override
        public void recordAt(Record record, long atRowId) {
            if (isRandomAccess) {
                ((GenerateSeriesTimestampStringRecord) record).of(start + stepTicks * (atRowId - 1));
                return;
            }
            throw new UnsupportedOperationException();
        }

        @Override
        public long size() {
            return size;
        }

        @Override
        public void skipRows(Counter rowCount, long maxRowsAfterSkip) {
            if (isRandomAccess) {
                final long rowsToSkip = Math.max(0, Math.min(rowCount.get(), size - rowIndex));
                rowIndex += rowsToSkip;
                // Leaves the record on the last row skipped over, or on toTop()'s sentinel
                // when nothing was skipped, so a skip of 0 stays a positional no-op.
                recordAt(recordA, rowIndex);
                rowCount.dec(rowsToSkip);
            } else {
                super.skipRows(rowCount, maxRowsAfterSkip);
            }
        }

        public boolean supportsRandomAccess() {
            return isRandomAccess;
        }

        @Override
        public void toTop() {
            rowIndex = 0;
            recordA.of(isRandomAccess ? start - stepTicks : adder.add(start, -stride));
        }

        private long adjustStride() {
            return switch (unit) {
                case 'w' -> timestampDriver.fromWeeks(stride);
                case 'd' -> timestampDriver.fromDays(stride);
                // 'H' alongside 'h' for the same reason getAddMethod takes both: SAMPLE BY
                // spells hours with a capital, and a step this switch does not name is one
                // every random-access path - a sorted read, a LIMIT with an offset - raises on.
                case 'h', 'H' -> timestampDriver.fromHours(stride);
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
                if (isRandomAccess) {
                    // Derived rather than read off the cursor, because recordB is positioned
                    // by recordAt() and carries no index of its own. curr is on-grid at
                    // start + stepTicks * (rowId - 1), so measuring the offset as an unsigned
                    // magnitude keeps a span that overflows a signed long dividing correctly.
                    // toTop()'s sentinel is the one position that is not a row, and it can
                    // wrap to the far side of start - so recognise it by its exact value
                    // rather than by which side of start it appears to fall on.
                    if (curr == start - stepTicks) {
                        return 0;
                    }
                    final long offset = curr - start;
                    return Long.divideUnsigned(stepTicks > 0 ? offset : -offset, Math.abs(stepTicks)) + 1;
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
