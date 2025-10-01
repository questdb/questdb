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

import io.questdb.cairo.TimestampDriver;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.engine.functions.TimestampFunction;
import io.questdb.griffin.engine.functions.UnaryFunction;
import io.questdb.std.Numbers;

/**
 * Functions used by both data_trunc() and timestamp_floor() as these functions have overlapping behaviour.
 */
final class TimestampFloorFunctions {

    private TimestampFloorFunctions() {
    }

    static class TimestampFloorFunction extends TimestampFunction implements UnaryFunction {
        private final Function arg;
        private final TimestampDriver.TimestampFloorMethod floor;
        private final CharSequence unit;

        public TimestampFloorFunction(Function arg, String unit, int timestampType) {
            super(timestampType);
            this.arg = arg;
            this.unit = unit;
            this.floor = timestampDriver.getTimestampFloorMethod(unit);
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
        public void toPlan(PlanSink sink) {
            sink.val(TimestampFloorFunctionFactory.NAME).val("('").val(unit).val("',").val(getArg()).val(')');
        }
    }

    static class TimestampFloorWithStrideFunction extends TimestampFunction implements UnaryFunction {
        private final Function arg;
        private final TimestampDriver.TimestampFloorWithStrideMethod floor;
        private final int stride;
        private final CharSequence unit;

        public TimestampFloorWithStrideFunction(Function arg, String unit, int stride, int timestampType) {
            super(timestampType);
            this.arg = arg;
            this.unit = unit;
            this.stride = stride;
            this.floor = timestampDriver.getTimestampFloorWithStrideMethod(unit);
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
        public void toPlan(PlanSink sink) {
            sink.val(TimestampFloorFunctionFactory.NAME).val("('").val(unit).val("',").val(getArg()).val(')');
        }
    }
}
