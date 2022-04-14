/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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
import io.questdb.griffin.engine.functions.TimestampFunction;
import io.questdb.griffin.engine.functions.UnaryFunction;
import io.questdb.std.Numbers;
import io.questdb.std.datetime.microtime.Timestamps;

/**
 * Functions used by both data_trunc() and timestamp_floor() as these functions have overlapping behaviour.
 *
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
        final public long getTimestamp(Record rec) {
            long micros = arg.getTimestamp(rec);
            return micros == Numbers.LONG_NaN ? Numbers.LONG_NaN : floor(micros);
        }

        abstract protected long floor(long timestamp);
    }

    static class TimestampFloorDDFunction extends AbstractTimestampFloorFunction {
        public TimestampFloorDDFunction(Function arg) {
            super(arg);
        }

        @Override
        public long floor(long timestamp) {
            return Timestamps.floorDD(timestamp);
        }
    }

    static class TimestampFloorMMFunction extends AbstractTimestampFloorFunction {
        public TimestampFloorMMFunction(Function arg) {
            super(arg);
        }

        @Override
        public long floor(long timestamp) {
            return Timestamps.floorMM(timestamp);
        }
    }

    static class TimestampFloorYYYYFunction extends AbstractTimestampFloorFunction {
        public TimestampFloorYYYYFunction(Function arg) {
            super(arg);
        }

        @Override
        public long floor(long timestamp) {
            return Timestamps.floorYYYY(timestamp);
        }
    }

    static class TimestampFloorHHFunction extends AbstractTimestampFloorFunction {
        public TimestampFloorHHFunction(Function arg) {
            super(arg);
        }

        @Override
        public long floor(long timestamp) {
            return Timestamps.floorHH(timestamp);
        }
    }

    static class TimestampFloorMIFunction extends AbstractTimestampFloorFunction {
        public TimestampFloorMIFunction(Function arg) {
            super(arg);
        }

        @Override
        public long floor(long timestamp) {
            return Timestamps.floorMI(timestamp);
        }
    }

    static class TimestampFloorSSFunction extends AbstractTimestampFloorFunction {
        public TimestampFloorSSFunction(Function arg) {
            super(arg);
        }

        @Override
        public long floor(long timestamp) {
            return Timestamps.floorSS(timestamp);
        }
    }

    static class TimestampFloorMSFunction extends AbstractTimestampFloorFunction {
        public TimestampFloorMSFunction(Function arg) {
            super(arg);
        }

        @Override
        public long floor(long timestamp) {
            return Timestamps.floorMS(timestamp);
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
    }

    static class TimestampFloorQuarterFunction extends TimestampFloorFunctions.AbstractTimestampFloorFunction {
        public TimestampFloorQuarterFunction(Function arg) {
            super(arg);
        }

        @Override
        public long floor(long timestamp) {
            return Timestamps.floorQuarter(timestamp);
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
    }

    static class TimestampFloorCenturyFunction extends TimestampFloorFunctions.AbstractTimestampFloorFunction {
        public TimestampFloorCenturyFunction(Function arg) {
            super(arg);
        }

        @Override
        public long floor(long timestamp) {
            return Timestamps.floorCentury(timestamp);
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
    }
}
