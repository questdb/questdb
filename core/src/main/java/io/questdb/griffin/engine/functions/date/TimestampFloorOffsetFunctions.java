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

import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.engine.functions.TimestampFunction;
import io.questdb.griffin.engine.functions.UnaryFunction;
import io.questdb.std.Numbers;
import io.questdb.std.datetime.microtime.Timestamps;


final class TimestampFloorOffsetFunctions {
    private TimestampFloorOffsetFunctions() {

    }

    abstract static class AbstractTimestampFloorOffsetFunction extends TimestampFunction implements UnaryFunction {
        protected final long offset;
        protected final int stride;
        private final Function arg;


        public AbstractTimestampFloorOffsetFunction(Function arg, int stride, long offset) {
            this.arg = arg;
            this.stride = stride;
            this.offset = offset;
        }

        @Override
        public Function getArg() {
            return arg;
        }

        public long getOffset() {
            return offset;
        }

        public CharSequence getOffsetAsString() {
            return Timestamps.toString(offset);
        }

        public int getStride() {
            return stride;
        }

        @Override
        public final long getTimestamp(Record rec) {
            final long micros = arg.getTimestamp(rec);
            return micros == Numbers.LONG_NULL ? Numbers.LONG_NULL : floor(micros);
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val("timestamp_floor('").val(getUnit()).val("',").val(getArg()).val(getOffsetAsString()).val(')');
        }

        abstract protected long floor(long timestamp);

        abstract CharSequence getUnit();
    }

    static class TimestampFloorOffsetDDFunction extends AbstractTimestampFloorOffsetFunction {

        public TimestampFloorOffsetDDFunction(Function arg, int stride, long offset) {
            super(arg, stride, offset);
        }

        @Override
        public long floor(long timestamp) {
            return Timestamps.floorDD(timestamp, stride, offset);
        }

        @Override
        CharSequence getUnit() {
            return "day";
        }
    }

    static class TimestampFloorOffsetHHFunction extends AbstractTimestampFloorOffsetFunction {

        public TimestampFloorOffsetHHFunction(Function arg, int stride, long offset) {
            super(arg, stride, offset);
        }

        @Override
        public long floor(long timestamp) {
            return Timestamps.floorHH(timestamp, stride, offset);
        }

        @Override
        CharSequence getUnit() {
            return "hour";
        }
    }

    static class TimestampFloorOffsetMCFunction extends AbstractTimestampFloorOffsetFunction {

        public TimestampFloorOffsetMCFunction(Function arg, int stride, long offset) {
            super(arg, stride, offset);
        }

        @Override
        public long floor(long timestamp) {
            return Timestamps.floorMC(timestamp, stride, offset);
        }

        @Override
        CharSequence getUnit() {
            return "microsecond";
        }
    }

    static class TimestampFloorOffsetMIFunction extends AbstractTimestampFloorOffsetFunction {

        public TimestampFloorOffsetMIFunction(Function arg, int stride, long offset) {
            super(arg, stride, offset);
        }

        @Override
        public long floor(long timestamp) {
            return Timestamps.floorMI(timestamp, stride, offset);
        }

        @Override
        CharSequence getUnit() {
            return "minute";
        }
    }

    static class TimestampFloorOffsetMMFunction extends AbstractTimestampFloorOffsetFunction {

        public TimestampFloorOffsetMMFunction(Function arg, int stride, long offset) {
            super(arg, stride, offset);
        }

        @Override
        public long floor(long timestamp) {
            return Timestamps.floorMM(timestamp, stride, offset);
        }

        @Override
        CharSequence getUnit() {
            return "month";
        }
    }

    static class TimestampFloorOffsetMSFunction extends AbstractTimestampFloorOffsetFunction {

        public TimestampFloorOffsetMSFunction(Function arg, int stride, long offset) {
            super(arg, stride, offset);
        }

        @Override
        public long floor(long timestamp) {
            return Timestamps.floorMS(timestamp, stride, offset);
        }

        @Override
        CharSequence getUnit() {
            return "millisecond";
        }
    }

    static class TimestampFloorOffsetSSFunction extends AbstractTimestampFloorOffsetFunction {

        public TimestampFloorOffsetSSFunction(Function arg, int stride, long offset) {
            super(arg, stride, offset);
        }

        @Override
        public long floor(long timestamp) {
            return Timestamps.floorSS(timestamp, stride, offset);
        }

        @Override
        CharSequence getUnit() {
            return "second";
        }
    }

    static class TimestampFloorOffsetWWFunction extends AbstractTimestampFloorOffsetFunction {

        public TimestampFloorOffsetWWFunction(Function arg, int stride, long offset) {
            super(arg, stride, offset);
        }

        @Override
        public long floor(long timestamp) {
            return Timestamps.floorWW(timestamp, stride, offset);
        }

        @Override
        CharSequence getUnit() {
            return "week";
        }
    }

    static class TimestampFloorOffsetYYYYFunction extends AbstractTimestampFloorOffsetFunction {

        public TimestampFloorOffsetYYYYFunction(Function arg, int stride, long offset) {
            super(arg, stride, offset);
        }

        @Override
        public long floor(long timestamp) {
            return Timestamps.floorYYYY(timestamp, stride, offset);
        }

        @Override
        CharSequence getUnit() {
            return "year";
        }
    }

}
