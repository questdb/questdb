/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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

import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.engine.functions.TimestampFunction;
import io.questdb.griffin.engine.functions.UnaryFunction;
import io.questdb.std.Numbers;
import io.questdb.std.datetime.microtime.Timestamps;

/**
 * Functions used by timestamp_ceil().
 */
final class TimestampCeilFunctions {
    private TimestampCeilFunctions() {

    }

    abstract static class AbstractTimestampCeilFunction extends TimestampFunction implements UnaryFunction {
        private final Function arg;

        public AbstractTimestampCeilFunction(Function arg) {
            this.arg = arg;
        }

        @Override
        public Function getArg() {
            return arg;
        }

        @Override
        public final long getTimestamp(Record rec) {
            long micros = arg.getTimestamp(rec);
            return micros == Numbers.LONG_NaN ? Numbers.LONG_NaN : ceil(micros);
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val("timestamp_ceil('").val(getUnit()).val("',").val(arg).val(')');
        }

        abstract long ceil(long timestamp);

        abstract CharSequence getUnit();
    }

    static class TimestampCeilCenturyFunction extends AbstractTimestampCeilFunction {
        public TimestampCeilCenturyFunction(Function arg) {
            super(arg);
        }

        @Override
        public long ceil(long timestamp) {
            return Timestamps.ceilCentury(timestamp);
        }

        @Override
        CharSequence getUnit() {
            return "century";
        }
    }

    static class TimestampCeilDDFunction extends AbstractTimestampCeilFunction {
        private final int stride;

        public TimestampCeilDDFunction(Function arg) {
            this(arg, 1);
        }

        public TimestampCeilDDFunction(Function arg, int stride) {
            super(arg);
            this.stride = stride;
        }

        @Override
        public long ceil(long timestamp) {
            return Timestamps.ceilDD(timestamp, stride);
        }

        @Override
        CharSequence getUnit() {
            return "day";
        }
    }

    static class TimestampCeilDayOfWeekFunction extends AbstractTimestampCeilFunction {
        public TimestampCeilDayOfWeekFunction(Function arg) {
            super(arg);
        }

        @Override
        public long ceil(long timestamp) {
            return Timestamps.ceilDOW(timestamp);
        }

        @Override
        CharSequence getUnit() {
            return "week";
        }
    }

    static class TimestampCeilDecadeFunction extends AbstractTimestampCeilFunction {
        public TimestampCeilDecadeFunction(Function arg) {
            super(arg);
        }

        @Override
        public long ceil(long timestamp) {
            return Timestamps.ceilDecade(timestamp);
        }

        @Override
        CharSequence getUnit() {
            return "decade";
        }
    }

    static class TimestampCeilHHFunction extends AbstractTimestampCeilFunction {
        private final int stride;

        public TimestampCeilHHFunction(Function arg) {
            this(arg, 1);
        }

        public TimestampCeilHHFunction(Function arg, int stride) {
            super(arg);
            this.stride = stride;
        }

        @Override
        public long ceil(long timestamp) {
            return Timestamps.ceilHH(timestamp, stride);
        }

        @Override
        CharSequence getUnit() {
            return "hour";
        }
    }

    static class TimestampCeilMIFunction extends AbstractTimestampCeilFunction {

        private final int stride;
        public TimestampCeilMIFunction(Function arg) {
            this(arg, 1);
        }

        public TimestampCeilMIFunction(Function arg, int stride) {
            super(arg);
            this.stride = stride;
        }

        @Override
        public long ceil(long timestamp) {
            return Timestamps.ceilMI(timestamp, stride);
        }

        @Override
        CharSequence getUnit() {
            return "minute";
        }
    }

    static class TimestampCeilMMFunction extends AbstractTimestampCeilFunction {
        private final int stride;
        public TimestampCeilMMFunction(Function arg) {
            this(arg, 1);
        }

        public TimestampCeilMMFunction(Function arg, int stride) {
            super(arg);
            this.stride = stride;
        }

        @Override
        public long ceil(long timestamp) {
            return stride > 1 ? Timestamps.ceilMM(timestamp, stride) : Timestamps.ceilMM(timestamp);
        }

        @Override
        CharSequence getUnit() {
            return "month";
        }
    }

    static class TimestampCeilMSFunction extends AbstractTimestampCeilFunction {
        private final int stride;
        public TimestampCeilMSFunction(Function arg) {
            this(arg, 1);
        }

        public TimestampCeilMSFunction(Function arg, int stride) {
            super(arg);
            this.stride = stride;
        }

        @Override
        public long ceil(long timestamp) {
            return Timestamps.ceilMS(timestamp, stride);
        }

        @Override
        CharSequence getUnit() {
            return "millisecond";
        }
    }

    static class TimestampCeilMillenniumFunction extends AbstractTimestampCeilFunction {
        private final int stride;
        public TimestampCeilMillenniumFunction(Function arg) {
            this(arg, 1);
        }

        public TimestampCeilMillenniumFunction(Function arg, int stride) {
            super(arg);
            this.stride = stride;
        }

        @Override
        public long ceil(long timestamp) {
            return Timestamps.ceilMillennium(timestamp);
        }

        @Override
        CharSequence getUnit() {
            return "millennium";
        }
    }

    static class TimestampCeilQuarterFunction extends AbstractTimestampCeilFunction {
       public TimestampCeilQuarterFunction(Function arg) {
           super(arg);
        }

        @Override
        public long ceil(long timestamp) {
            return Timestamps.ceilQuarter(timestamp);
        }

        @Override
        CharSequence getUnit() {
            return "quarter";
        }
    }

    static class TimestampCeilSSFunction extends AbstractTimestampCeilFunction {
        private final int stride;

        public TimestampCeilSSFunction(Function arg) {
            this(arg, 1);
        }

        public TimestampCeilSSFunction(Function arg, int stride) {
            super(arg);
            this.stride = stride;
        }

        @Override
        public long ceil(long timestamp) {
            return Timestamps.ceilSS(timestamp, stride);
        }

        @Override
        CharSequence getUnit() {
            return "second";
        }
    }

    static class TimestampCeilWWFunction extends AbstractTimestampCeilFunction {
        private final int stride;
        public TimestampCeilWWFunction(Function arg) {
            this(arg, 1);
        }

        public TimestampCeilWWFunction(Function arg, int stride) {
            super(arg);
            this.stride = stride;
        }

        @Override
        public long ceil(long timestamp) {
            return Timestamps.ceilWW(timestamp, stride);
        }

        @Override
        CharSequence getUnit() {
            return "week";
        }
    }

    static class TimestampCeilYYYYFunction extends AbstractTimestampCeilFunction {
        private final int stride;
        public TimestampCeilYYYYFunction(Function arg) {
            this(arg, 1);
        }

        public TimestampCeilYYYYFunction(Function arg, int stride) {
            super(arg);
            this.stride = stride;
        }

        @Override
        public long ceil(long timestamp) {
            return stride > 1 ? Timestamps.ceilYYYY(timestamp, stride) : Timestamps.ceilYYYY(timestamp);
        }

        @Override
        CharSequence getUnit() {
            return "year";
        }
    }
}
