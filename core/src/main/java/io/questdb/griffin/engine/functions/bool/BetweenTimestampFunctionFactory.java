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

package io.questdb.griffin.engine.functions.bool;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.TimestampDriver;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.BooleanFunction;
import io.questdb.griffin.engine.functions.TernaryFunction;
import io.questdb.griffin.engine.functions.TimestampBounds;
import io.questdb.griffin.engine.functions.UnaryFunction;
import io.questdb.griffin.engine.functions.constants.BooleanConstant;
import io.questdb.std.IntList;
import io.questdb.std.LongList;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;

public class BetweenTimestampFunctionFactory implements FunctionFactory {
    @Override
    public String getSignature() {
        return "between(NNN)";
    }

    @Override
    public Function newInstance(
            int position,
            ObjList<Function> args,
            IntList argPositions,
            CairoConfiguration configuration,
            SqlExecutionContext sqlExecutionContext
    ) {
        Function arg = args.getQuick(0);
        Function fromFn = args.getQuick(1);
        Function toFn = args.getQuick(2);
        int argType = ColumnType.getTimestampType(arg.getType());
        int fromType = ColumnType.getTimestampType(fromFn.getType());
        int toType = ColumnType.getTimestampType(toFn.getType());
        if (!ColumnType.isTimestamp(argType)) {
            if (fromFn.isConstant() && toFn.isConstant()) {
                long fromFnTimestamp = fromFn.getTimestamp(null);
                long toFnTimestamp = toFn.getTimestamp(null);
                if (fromFnTimestamp == Numbers.LONG_NULL || toFnTimestamp == Numbers.LONG_NULL) {
                    return BooleanConstant.FALSE;
                }
                return new ConstFunc(arg, fromFnTimestamp, toFnTimestamp);
            }
            return new VarBetweenFunction(arg, fromFn, toFn, null, fromType, toType);
        }


        TimestampDriver driver = ColumnType.getTimestampDriver(argType);

        if (fromFn.isConstant() && toFn.isConstant()) {
            final LongList range = new LongList(2);
            if (!getRange(driver, fromFn, toFn, range)) {
                return BooleanConstant.FALSE;
            }
            final long lo = range.getQuick(0);
            final long hi = range.getQuick(1);
            if (lo == Numbers.LONG_NULL || hi == Numbers.LONG_NULL || lo > hi) {
                return BooleanConstant.FALSE;
            }
            return new ConstFunc(arg, lo, hi);
        }
        if (isBound(fromFn) && isBound(toFn)) {
            return new RuntimeConstFunc(arg, fromFn, toFn, driver);
        }
        boolean leftNeedConvert = fromType != argType;
        boolean rightNeedConvert = toType != argType;

        if (!leftNeedConvert && !rightNeedConvert) {
            return new VarBetweenFunction(arg, fromFn, toFn, driver, fromType, toType);
        } else if (leftNeedConvert && rightNeedConvert) {
            return new BothConvertFunction(arg, fromFn, toFn, driver, fromType, toType);
        } else if (leftNeedConvert) {
            return new LeftConvertFunction(arg, fromFn, toFn, driver, fromType, toType);
        } else {
            return new RightConvertFunction(arg, fromFn, toFn, driver, fromType, toType);
        }
    }

    // BETWEEN takes its bounds in either order, and a bound grows with its constant, so the values between them
    // start at the smaller of the two lower bounds and end at the larger of the two upper bounds. A bound with no
    // value of the type on its side (LONG_NULL) gives way to the other bound. Returns false for a NULL bound.
    private static boolean getRange(TimestampDriver driver, Function fromFn, Function toFn, LongList out) {
        if (!TimestampBounds.bounds(driver, fromFn, out)) {
            return false;
        }
        final long fromLo = out.getQuick(0);
        final long fromHi = out.getQuick(1);
        if (!TimestampBounds.bounds(driver, toFn, out)) {
            return false;
        }
        final long toLo = out.getQuick(0);
        final long toHi = out.getQuick(1);
        out.clear();
        out.add(
                fromLo == Numbers.LONG_NULL ? toLo : toLo == Numbers.LONG_NULL ? fromLo : Math.min(fromLo, toLo),
                fromHi == Numbers.LONG_NULL ? toHi : toHi == Numbers.LONG_NULL ? fromHi : Math.max(fromHi, toHi)
        );
        return true;
    }

    private static boolean isBound(Function func) {
        return func.isConstant() || func.isRuntimeConstant();
    }

    private static class BothConvertFunction extends VarBetweenFunction {
        public BothConvertFunction(Function left, Function from, Function to, TimestampDriver driver, int fromType, int toType) {
            super(left, from, to, driver, fromType, toType);
        }

        @Override
        public boolean getBool(Record rec) {
            long value = arg.getTimestamp(rec);
            if (value == Numbers.LONG_NULL) {
                return false;
            }
            long fromTs = from.getTimestamp(rec);
            if (fromTs == Numbers.LONG_NULL) {
                return false;
            }

            long toTs = to.getTimestamp(rec);
            if (toTs == Numbers.LONG_NULL) {
                return false;
            }

            // the value lies at or between the bounds, in either order; nothing converts
            return driver.compareFrom(fromTs, fromType, value) * driver.compareFrom(toTs, toType, value) <= 0;
        }
    }

    private static class ConstFunc extends BooleanFunction implements UnaryFunction {
        private final long from;
        private final Function left;
        private final long to;

        public ConstFunc(Function left, long from, long to) {
            this.left = left;
            this.from = Math.min(from, to);
            this.to = Math.max(from, to);
        }

        @Override
        public Function getArg() {
            return left;
        }

        @Override
        public boolean getBool(Record rec) {
            long timestamp = left.getTimestamp(rec);
            if (timestamp == Numbers.LONG_NULL) return false;

            return from <= timestamp && timestamp <= to;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(left).val(" between ").val(from).val(" and ").val(to);
        }
    }

    private static class LeftConvertFunction extends VarBetweenFunction {
        public LeftConvertFunction(Function left, Function from, Function to, TimestampDriver driver, int fromType, int toType) {
            super(left, from, to, driver, fromType, toType);
        }

        @Override
        public boolean getBool(Record rec) {
            long value = arg.getTimestamp(rec);
            if (value == Numbers.LONG_NULL) {
                return false;
            }
            long fromTs = from.getTimestamp(rec);
            if (fromTs == Numbers.LONG_NULL) {
                return false;
            }

            long toTs = to.getTimestamp(rec);
            if (toTs == Numbers.LONG_NULL) {
                return false;
            }

            // the value lies at or between the bounds, in either order; nothing converts
            return driver.compareFrom(fromTs, fromType, value) * Long.compare(toTs, value) <= 0;
        }
    }

    // the bounds are runtime constants, such as bind variables: the range comes from them on each execution
    private static class RuntimeConstFunc extends BooleanFunction implements TernaryFunction {
        private final Function arg;
        private final TimestampDriver driver;
        private final Function from;
        private final LongList range = new LongList(2);
        private final Function to;
        private long hi;
        private long lo;

        public RuntimeConstFunc(Function arg, Function from, Function to, TimestampDriver driver) {
            this.arg = arg;
            this.from = from;
            this.to = to;
            this.driver = driver;
        }

        @Override
        public boolean getBool(Record rec) {
            final long value = arg.getTimestamp(rec);
            if (value == Numbers.LONG_NULL) {
                return false;
            }
            return lo <= value && value <= hi;
        }

        @Override
        public Function getCenter() {
            return arg;
        }

        @Override
        public Function getLeft() {
            return from;
        }

        @Override
        public Function getRight() {
            return to;
        }

        @Override
        public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
            TernaryFunction.super.init(symbolTableSource, executionContext);
            lo = TimestampBounds.EMPTY_LO;
            hi = TimestampBounds.EMPTY_HI;
            if (getRange(driver, from, to, range)) {
                final long rangeLo = range.getQuick(0);
                final long rangeHi = range.getQuick(1);
                if (rangeLo != Numbers.LONG_NULL && rangeHi != Numbers.LONG_NULL) {
                    lo = rangeLo;
                    hi = rangeHi;
                }
            }
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(arg).val(" between ").val(from).val(" and ").val(to);
        }
    }

    private static class RightConvertFunction extends VarBetweenFunction {
        public RightConvertFunction(Function left, Function from, Function to, TimestampDriver driver, int fromType, int toType) {
            super(left, from, to, driver, fromType, toType);
        }

        @Override
        public boolean getBool(Record rec) {
            long value = arg.getTimestamp(rec);
            if (value == Numbers.LONG_NULL) {
                return false;
            }
            long fromTs = from.getTimestamp(rec);
            if (fromTs == Numbers.LONG_NULL) {
                return false;
            }

            long toTs = to.getTimestamp(rec);
            if (toTs == Numbers.LONG_NULL) {
                return false;
            }

            // the value lies at or between the bounds, in either order; nothing converts
            return Long.compare(fromTs, value) * driver.compareFrom(toTs, toType, value) <= 0;
        }
    }

    private static class VarBetweenFunction extends BooleanFunction implements TernaryFunction {
        protected final Function arg;
        protected final TimestampDriver driver;
        protected final Function from;
        protected final int fromType;
        protected final Function to;
        protected final int toType;

        public VarBetweenFunction(Function left, Function from, Function to, TimestampDriver driver, int fromType, int toType) {
            this.arg = left;
            this.from = from;
            this.to = to;
            this.driver = driver;
            this.fromType = fromType;
            this.toType = toType;
        }

        @Override
        public boolean getBool(Record rec) {
            long value = arg.getTimestamp(rec);
            if (value == Numbers.LONG_NULL) {
                return false;
            }

            long fromTs = from.getTimestamp(rec);
            if (fromTs == Numbers.LONG_NULL) {
                return false;
            }

            long toTs = to.getTimestamp(rec);
            if (toTs == Numbers.LONG_NULL) {
                return false;
            }

            return Math.min(fromTs, toTs) <= value && value <= Math.max(fromTs, toTs);
        }

        @Override
        public Function getCenter() {
            return arg;
        }

        @Override
        public Function getLeft() {
            return from;
        }

        @Override
        public Function getRight() {
            return to;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(arg).val(" between ").val(from).val(" and ").val(to);
        }
    }
}
