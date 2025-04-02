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
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.BinaryFunction;
import io.questdb.griffin.engine.functions.TimestampFunction;
import io.questdb.griffin.engine.functions.UnaryFunction;
import io.questdb.std.Numbers;
import io.questdb.std.datetime.microtime.Timestamps;


final class TimestampFloorOffsetFunctions {

    private TimestampFloorOffsetFunctions() {
    }

    abstract static class AbstractTimestampFloorConstOffsetFunction extends TimestampFunction implements UnaryFunction {
        protected final long offset;
        protected final int stride;
        private final Function arg;

        public AbstractTimestampFloorConstOffsetFunction(Function arg, int stride, long offset) {
            this.arg = arg;
            this.stride = stride;
            this.offset = offset;
        }

        @Override
        public Function getArg() {
            return arg;
        }

        @Override
        public final long getTimestamp(Record rec) {
            final long micros = arg.getTimestamp(rec);
            return micros == Numbers.LONG_NULL ? Numbers.LONG_NULL : floor(micros);
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val("timestamp_floor('")
                    .val(getUnit()).val("',")
                    .val(getArg()).val(',')
                    .val(Timestamps.toString(offset))
                    .val(')');
        }

        abstract protected long floor(long timestamp);

        abstract CharSequence getUnit();
    }

    abstract static class AbstractTimestampFloorOffsetFunction extends TimestampFunction implements BinaryFunction {
        protected final int stride;
        private final long from;
        private final Function offsetFunc;
        private final int offsetPos;
        private final Function tsFunc;
        private long offset;

        public AbstractTimestampFloorOffsetFunction(Function tsFunc, int stride, long from, Function offsetFunc, int offsetPos) {
            this.tsFunc = tsFunc;
            this.stride = stride;
            this.from = from;
            this.offsetFunc = offsetFunc;
            this.offsetPos = offsetPos;
        }

        public long effectiveOffset() {
            return from + offset;
        }

        @Override
        public Function getLeft() {
            return tsFunc;
        }

        @Override
        public Function getRight() {
            return offsetFunc;
        }

        @Override
        public final long getTimestamp(Record rec) {
            final long micros = tsFunc.getTimestamp(rec);
            return micros == Numbers.LONG_NULL ? Numbers.LONG_NULL : floor(micros);
        }

        @Override
        public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
            BinaryFunction.super.init(symbolTableSource, executionContext);

            final CharSequence offsetStr = offsetFunc.getStrA(null);
            if (offsetStr != null) {
                final long val = Timestamps.parseOffset(offsetStr);
                if (val == Numbers.LONG_NULL) {
                    // bad value for offset
                    throw SqlException.$(offsetPos, "invalid offset: ").put(offsetStr);
                }
                offset = Numbers.decodeLowInt(val) * Timestamps.MINUTE_MICROS;
            } else {
                offset = 0;
            }
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val("timestamp_floor('")
                    .val(getUnit()).val("',")
                    .val(tsFunc).val(',')
                    .val(Timestamps.toString(from)).val(',')
                    .val(offsetFunc)
                    .val(')');
        }

        abstract protected long floor(long timestamp);

        abstract CharSequence getUnit();
    }

    static class TimestampFloorConstOffsetDDFunction extends AbstractTimestampFloorConstOffsetFunction {

        public TimestampFloorConstOffsetDDFunction(Function arg, int stride, long offset) {
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

    static class TimestampFloorConstOffsetHHFunction extends AbstractTimestampFloorConstOffsetFunction {

        public TimestampFloorConstOffsetHHFunction(Function arg, int stride, long offset) {
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

    static class TimestampFloorConstOffsetMCFunction extends AbstractTimestampFloorConstOffsetFunction {

        public TimestampFloorConstOffsetMCFunction(Function arg, int stride, long offset) {
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

    static class TimestampFloorConstOffsetMIFunction extends AbstractTimestampFloorConstOffsetFunction {

        public TimestampFloorConstOffsetMIFunction(Function arg, int stride, long offset) {
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

    static class TimestampFloorConstOffsetMMFunction extends AbstractTimestampFloorConstOffsetFunction {

        public TimestampFloorConstOffsetMMFunction(Function arg, int stride, long offset) {
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

    static class TimestampFloorConstOffsetMSFunction extends AbstractTimestampFloorConstOffsetFunction {

        public TimestampFloorConstOffsetMSFunction(Function arg, int stride, long offset) {
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

    static class TimestampFloorConstOffsetSSFunction extends AbstractTimestampFloorConstOffsetFunction {

        public TimestampFloorConstOffsetSSFunction(Function arg, int stride, long offset) {
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

    static class TimestampFloorConstOffsetWWFunction extends AbstractTimestampFloorConstOffsetFunction {

        public TimestampFloorConstOffsetWWFunction(Function arg, int stride, long offset) {
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

    static class TimestampFloorConstOffsetYYYYFunction extends AbstractTimestampFloorConstOffsetFunction {

        public TimestampFloorConstOffsetYYYYFunction(Function arg, int stride, long offset) {
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

    static class TimestampFloorOffsetDDFunction extends AbstractTimestampFloorOffsetFunction {

        public TimestampFloorOffsetDDFunction(Function tsFunc, int stride, long offset, Function offsetFunc, int offsetPos) {
            super(tsFunc, stride, offset, offsetFunc, offsetPos);
        }

        @Override
        public long floor(long timestamp) {
            return Timestamps.floorDD(timestamp, stride, effectiveOffset());
        }

        @Override
        CharSequence getUnit() {
            return "day";
        }
    }

    static class TimestampFloorOffsetHHFunction extends AbstractTimestampFloorOffsetFunction {

        public TimestampFloorOffsetHHFunction(Function tsFunc, int stride, long offset, Function offsetFunc, int offsetPos) {
            super(tsFunc, stride, offset, offsetFunc, offsetPos);
        }

        @Override
        public long floor(long timestamp) {
            return Timestamps.floorHH(timestamp, stride, effectiveOffset());
        }

        @Override
        CharSequence getUnit() {
            return "hour";
        }
    }

    static class TimestampFloorOffsetMCFunction extends AbstractTimestampFloorOffsetFunction {

        public TimestampFloorOffsetMCFunction(Function tsFunc, int stride, long offset, Function offsetFunc, int offsetPos) {
            super(tsFunc, stride, offset, offsetFunc, offsetPos);
        }

        @Override
        public long floor(long timestamp) {
            return Timestamps.floorMC(timestamp, stride, effectiveOffset());
        }

        @Override
        CharSequence getUnit() {
            return "microsecond";
        }
    }

    static class TimestampFloorOffsetMIFunction extends AbstractTimestampFloorOffsetFunction {

        public TimestampFloorOffsetMIFunction(Function tsFunc, int stride, long offset, Function offsetFunc, int offsetPos) {
            super(tsFunc, stride, offset, offsetFunc, offsetPos);
        }

        @Override
        public long floor(long timestamp) {
            return Timestamps.floorMI(timestamp, stride, effectiveOffset());
        }

        @Override
        CharSequence getUnit() {
            return "minute";
        }
    }

    static class TimestampFloorOffsetMMFunction extends AbstractTimestampFloorOffsetFunction {

        public TimestampFloorOffsetMMFunction(Function tsFunc, int stride, long offset, Function offsetFunc, int offsetPos) {
            super(tsFunc, stride, offset, offsetFunc, offsetPos);
        }

        @Override
        public long floor(long timestamp) {
            return Timestamps.floorMM(timestamp, stride, effectiveOffset());
        }

        @Override
        CharSequence getUnit() {
            return "month";
        }
    }

    static class TimestampFloorOffsetMSFunction extends AbstractTimestampFloorOffsetFunction {

        public TimestampFloorOffsetMSFunction(Function tsFunc, int stride, long offset, Function offsetFunc, int offsetPos) {
            super(tsFunc, stride, offset, offsetFunc, offsetPos);
        }

        @Override
        public long floor(long timestamp) {
            return Timestamps.floorMS(timestamp, stride, effectiveOffset());
        }

        @Override
        CharSequence getUnit() {
            return "millisecond";
        }
    }

    static class TimestampFloorOffsetSSFunction extends AbstractTimestampFloorOffsetFunction {

        public TimestampFloorOffsetSSFunction(Function tsFunc, int stride, long offset, Function offsetFunc, int offsetPos) {
            super(tsFunc, stride, offset, offsetFunc, offsetPos);
        }

        @Override
        public long floor(long timestamp) {
            return Timestamps.floorSS(timestamp, stride, effectiveOffset());
        }

        @Override
        CharSequence getUnit() {
            return "second";
        }
    }

    static class TimestampFloorOffsetWWFunction extends AbstractTimestampFloorOffsetFunction {

        public TimestampFloorOffsetWWFunction(Function tsFunc, int stride, long offset, Function offsetFunc, int offsetPos) {
            super(tsFunc, stride, offset, offsetFunc, offsetPos);
        }

        @Override
        public long floor(long timestamp) {
            return Timestamps.floorWW(timestamp, stride, effectiveOffset());
        }

        @Override
        CharSequence getUnit() {
            return "week";
        }
    }

    static class TimestampFloorOffsetYYYYFunction extends AbstractTimestampFloorOffsetFunction {

        public TimestampFloorOffsetYYYYFunction(Function tsFunc, int stride, long offset, Function offsetFunc, int offsetPos) {
            super(tsFunc, stride, offset, offsetFunc, offsetPos);
        }

        @Override
        public long floor(long timestamp) {
            return Timestamps.floorYYYY(timestamp, stride, effectiveOffset());
        }

        @Override
        CharSequence getUnit() {
            return "year";
        }
    }
}
