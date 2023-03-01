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
 * Functions used by both data_trunc() and timestamp_floor() as these functions have overlapping behaviour.
 */
final class TimestampFloorFunctions {
    private TimestampFloorFunctions() {

    }

    abstract static class AbstractTimestampFloorFunction extends TimestampFunction implements UnaryFunction {
        private final Function arg;

        public AbstractTimestampFloorFunction(Function arg) {
            this.arg = arg;
        }

        @Override
        public Function getArg() {
            return arg;
        }

        @Override
        public final long getTimestamp(Record rec) {
            long micros = arg.getTimestamp(rec);
            return micros == Numbers.LONG_NaN ? Numbers.LONG_NaN : floor(micros);
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val("timestamp_floor('").val(getUnit()).val("',").val(getArg()).val(')');
        }

        abstract protected long floor(long timestamp);

        abstract CharSequence getUnit();
    }

    static class TimestampFloorCenturyFunction extends TimestampFloorFunctions.AbstractTimestampFloorFunction {
        public TimestampFloorCenturyFunction(Function arg) {
            super(arg);
        }

        @Override
        public long floor(long timestamp) {
            return Timestamps.floorCentury(timestamp);
        }

        @Override
        CharSequence getUnit() {
            return "century";
        }
    }

    static class TimestampFloorDDFunction extends AbstractTimestampFloorFunction {
        private final int stride;

        public TimestampFloorDDFunction(Function arg) {
            this(arg, 1);
        }

        public TimestampFloorDDFunction(Function arg, int stride) {
            super(arg);
            this.stride = stride;
        }

        @Override
        public long floor(long timestamp) {
            return Timestamps.floorDD(timestamp, stride);
        }

        @Override
        CharSequence getUnit() {
            return "day";
        }
    }

    static class TimestampFloorDayOfWeekFunction extends TimestampFloorFunctions.AbstractTimestampFloorFunction {
        public TimestampFloorDayOfWeekFunction(Function arg) {
            super(arg);
        }

        @Override
        public long floor(long timestamp) {
            return Timestamps.floorDOW(timestamp);
        }

        @Override
        CharSequence getUnit() {
            return "week";
        }
    }

    static class TimestampFloorDecadeFunction extends TimestampFloorFunctions.AbstractTimestampFloorFunction {
        public TimestampFloorDecadeFunction(Function arg) {
            super(arg);
        }

        @Override
        public long floor(long timestamp) {
            return Timestamps.floorDecade(timestamp);
        }

        @Override
        CharSequence getUnit() {
            return "decade";
        }
    }

    static class TimestampFloorHHFunction extends AbstractTimestampFloorFunction {
        private final int stride;

        public TimestampFloorHHFunction(Function arg) {
            this(arg, 1);
        }

        public TimestampFloorHHFunction(Function arg, int stride) {
            super(arg);
            this.stride = stride;
        }

        @Override
        public long floor(long timestamp) {
            return Timestamps.floorHH(timestamp, stride);
        }

        @Override
        CharSequence getUnit() {
            return "hour";
        }
    }

    static class TimestampFloorMIFunction extends AbstractTimestampFloorFunction {
        private final int stride;

        public TimestampFloorMIFunction(Function arg) {
            this(arg, 1);
        }

        public TimestampFloorMIFunction(Function arg, int stride) {
            super(arg);
            this.stride = stride;
        }

        @Override
        public long floor(long timestamp) {
            return Timestamps.floorMI(timestamp, stride);
        }

        @Override
        CharSequence getUnit() {
            return "minute";
        }
    }

    static class TimestampFloorMMFunction extends AbstractTimestampFloorFunction {
        private final int stride;

        public TimestampFloorMMFunction(Function arg) {
            this(arg, 1);
        }

        public TimestampFloorMMFunction(Function arg, int stride) {
            super(arg);
            this.stride = stride;
        }

        @Override
        public long floor(long timestamp) {
            return stride > 1 ? Timestamps.floorMM(timestamp, stride) : Timestamps.floorMM(timestamp);
        }

        @Override
        CharSequence getUnit() {
            return "month";
        }
    }

    static class TimestampFloorMSFunction extends AbstractTimestampFloorFunction {
        private final int stride;

        public TimestampFloorMSFunction(Function arg) {
            this(arg, 1);
        }

        public TimestampFloorMSFunction(Function arg, int stride) {
            super(arg);
            this.stride = stride;
        }

        @Override
        public long floor(long timestamp) {
            return Timestamps.floorMS(timestamp, stride);
        }

        @Override
        CharSequence getUnit() {
            return "millisecond";
        }
    }

    static class TimestampFloorMillenniumFunction extends TimestampFloorFunctions.AbstractTimestampFloorFunction {
        public TimestampFloorMillenniumFunction(Function arg) {
            super(arg);
        }

        @Override
        public long floor(long timestamp) {
            return Timestamps.floorMillennium(timestamp);
        }

        @Override
        CharSequence getUnit() {
            return "millennium";
        }
    }

    static class TimestampFloorQuarterFunction extends TimestampFloorFunctions.AbstractTimestampFloorFunction {
        public TimestampFloorQuarterFunction(Function arg) {
            super(arg);
        }

        @Override
        public long floor(long timestamp) {
            return Timestamps.floorQuarter(timestamp);
        }

        @Override
        CharSequence getUnit() {
            return "quarter";
        }
    }

    static class TimestampFloorSSFunction extends AbstractTimestampFloorFunction {
        private final int stride;

        public TimestampFloorSSFunction(Function arg) {
            this(arg, 1);
        }

        public TimestampFloorSSFunction(Function arg, int stride) {
            super(arg);
            this.stride = stride;
        }

        @Override
        public long floor(long timestamp) {
            return Timestamps.floorSS(timestamp, stride);
        }

        @Override
        CharSequence getUnit() {
            return "second";
        }
    }

    static class TimestampFloorWWFunction extends AbstractTimestampFloorFunction {
        private final int stride;

        public TimestampFloorWWFunction(Function arg) {
            this(arg, 1);
        }

        public TimestampFloorWWFunction(Function arg, int stride) {
            super(arg);
            this.stride = stride;
        }

        @Override
        public long floor(long timestamp) {
            return Timestamps.floorWW(timestamp, stride);
        }

        @Override
        CharSequence getUnit() {
            return "week";
        }
    }

    static class TimestampFloorYYYYFunction extends AbstractTimestampFloorFunction {
        private final int stride;

        public TimestampFloorYYYYFunction(Function arg) {
            this(arg, 1);
        }

        public TimestampFloorYYYYFunction(Function arg, int stride) {
            super(arg);
            this.stride = stride;
        }

        @Override
        public long floor(long timestamp) {
            return stride > 1 ? Timestamps.floorYYYY(timestamp, stride) : Timestamps.floorYYYY(timestamp);
        }

        @Override
        CharSequence getUnit() {
            return "year";
        }
    }
}
