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

import io.questdb.cairo.TimestampDriver;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.engine.functions.MonotonicTimestampFunction;
import io.questdb.griffin.engine.functions.TimestampFunction;
import io.questdb.griffin.engine.functions.UnaryFunction;
import io.questdb.std.Chars;
import io.questdb.std.Interval;
import io.questdb.std.Numbers;

/**
 * Functions used by both data_trunc() and timestamp_floor() as these functions have overlapping behaviour.
 */
final class TimestampFloorFunctions {

    private TimestampFloorFunctions() {
    }

    static class TimestampFloorFunction extends TimestampFunction implements UnaryFunction, MonotonicTimestampFunction {
        private final Function arg;
        private final TimestampDriver.TimestampCeilMethod ceil;
        private final TimestampDriver.TimestampFloorMethod floor;
        private final CharSequence unit;

        public TimestampFloorFunction(Function arg, String unit, int timestampType) {
            super(timestampType);
            this.arg = arg;
            this.unit = unit;
            this.floor = timestampDriver.getTimestampFloorMethod(unit);
            final char ceilUnit = ceilUnitChar(unit);
            this.ceil = ceilUnit == 0 ? null : timestampDriver.getTimestampCeilMethod(ceilUnit);
        }

        @Override
        public Function getArg() {
            return arg;
        }

        @Override
        public final long getTimestamp(Record rec) {
            long ts = arg.getTimestamp(rec);
            return ts == Numbers.LONG_NULL ? Numbers.LONG_NULL : floor.floor(ts);
        }

        @Override
        public Function getTimestampArg() {
            return arg;
        }

        @Override
        public int invertTimestampInterval(Interval io) {
            if (ceil == null) {
                return NONE;
            }
            long lo = io.getLo();
            long hi = io.getHi();
            if (lo != Numbers.LONG_NULL && floor.floor(lo) != lo) {
                final long c = ceil.ceil(lo);
                if (c < lo) {
                    return NONE;
                }
                lo = c;
            }
            if (hi != Long.MAX_VALUE) {
                // ceil is the identity for the smallest unit (us/ns), where the bucket is one tick
                final long c = ceil.ceil(hi);
                if (c < hi) {
                    return NONE;
                }
                hi = c == hi ? hi : c - 1;
            }
            io.of(lo, hi);
            return EXACT;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(TimestampFloorFunctionFactory.NAME).val("('").val(unit).val("',").val(getArg()).val(')');
        }

        private static char ceilUnitChar(CharSequence unit) {
            if (Chars.equals(unit, "second")) {
                return 's';
            }
            if (Chars.equals(unit, "minute")) {
                return 'm';
            }
            if (Chars.equals(unit, "hour")) {
                return 'h';
            }
            if (Chars.equals(unit, "day")) {
                return 'd';
            }
            if (Chars.equals(unit, "month")) {
                return 'M';
            }
            if (Chars.equals(unit, "year")) {
                return 'y';
            }
            if (Chars.equals(unit, "millisecond")) {
                return 'T';
            }
            if (Chars.equals(unit, "microsecond")) {
                return 'U';
            }
            if (Chars.equals(unit, "nanosecond")) {
                return 'n';
            }
            return 0;
        }
    }

    static class TimestampFloorWithStrideFunction extends TimestampFunction implements UnaryFunction, MonotonicTimestampFunction {
        private final char addUnit;
        private final Function arg;
        private final TimestampDriver.TimestampFloorWithStrideMethod floor;
        private final boolean isExactlyInvertible;
        private final int stride;
        private final CharSequence unit;

        public TimestampFloorWithStrideFunction(Function arg, String unit, int stride, int timestampType) {
            super(timestampType);
            this.arg = arg;
            this.unit = unit;
            this.stride = stride;
            this.floor = timestampDriver.getTimestampFloorWithStrideMethod(unit);
            this.addUnit = fixedStrideUnitChar(unit);
            this.isExactlyInvertible = isExactlyInvertible();
        }

        @Override
        public Function getArg() {
            return arg;
        }

        @Override
        public final long getTimestamp(Record rec) {
            long ts = arg.getTimestamp(rec);
            return ts == Numbers.LONG_NULL ? Numbers.LONG_NULL : floor.floor(ts, stride);
        }

        @Override
        public Function getTimestampArg() {
            return arg;
        }

        @Override
        public int invertTimestampInterval(Interval io) {
            if (!isExactlyInvertible) {
                return NONE;
            }
            long lo = io.getLo();
            long hi = io.getHi();
            if (lo != Numbers.LONG_NULL) {
                final long b = floor.floor(lo, stride);
                if (b != lo) {
                    final long c = timestampDriver.add(b, addUnit, stride);
                    if (c < lo) {
                        return NONE;
                    }
                    lo = c;
                }
            }
            if (hi != Long.MAX_VALUE) {
                final long c = timestampDriver.add(floor.floor(hi, stride), addUnit, stride);
                if (c <= hi) {
                    return NONE;
                }
                hi = c - 1;
            }
            io.of(lo, hi);
            return EXACT;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(TimestampFloorFunctionFactory.NAME).val("('").val(unit).val("',").val(getArg()).val(')');
        }

        private static char fixedStrideUnitChar(CharSequence unit) {
            if (Chars.equals(unit, "second")) {
                return 's';
            }
            if (Chars.equals(unit, "minute")) {
                return 'm';
            }
            if (Chars.equals(unit, "hour")) {
                return 'h';
            }
            if (Chars.equals(unit, "day")) {
                return 'd';
            }
            if (Chars.equals(unit, "millisecond")) {
                return 'T';
            }
            if (Chars.equals(unit, "microsecond")) {
                // 'u', not 'U': add()/dateadd use the lowercase microsecond unit, ceil/floor the upper
                return 'u';
            }
            if (Chars.equals(unit, "nanosecond")) {
                return 'n';
            }
            return 0;
        }

        // EXACT only when add() reproduces the floor's bucket boundaries; a sub-resolution
        // stride (e.g. nanoseconds on a micro column) does not, and must stay a row filter.
        private boolean isExactlyInvertible() {
            if (addUnit == 0) {
                return false;
            }
            final long b0 = floor.floor(0, stride);
            final long next = timestampDriver.add(b0, addUnit, stride);
            return next > b0 && floor.floor(next, stride) == next && floor.floor(next - 1, stride) == b0;
        }
    }
}
