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
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.TimestampDriver;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.BooleanFunction;
import io.questdb.griffin.engine.functions.TernaryFunction;
import io.questdb.griffin.engine.functions.UnaryFunction;
import io.questdb.griffin.engine.functions.constants.BooleanConstant;
import io.questdb.std.IntList;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;

public class BetweenTimestampFunctionFactory implements FunctionFactory {
    @Override
    public int getResultType(IntList argTypes) {
        return ColumnType.BOOLEAN;
    }

    @Override
    public String getSignature() {
        return "between(NNN)";
    }

    @Override
    public boolean isConstructionDeferrable(int position, ObjList<Function> args, IntList argPositions, CairoConfiguration configuration) {
        final Function fromFn = args.getQuick(1);
        final Function toFn = args.getQuick(2);
        return !fromFn.isConstant() || !toFn.isConstant()
                || fromFn.getTimestamp(null) != Numbers.LONG_NULL && toFn.getTimestamp(null) != Numbers.LONG_NULL;
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
                    CairoException.rethrowCleanupFailure(Misc.freeObjListBestEffort(null, args));
                    return BooleanConstant.FALSE;
                }
                return new ConstFunc(arg, Math.min(fromFnTimestamp, toFnTimestamp), Math.max(fromFnTimestamp, toFnTimestamp));
            }
            return new VarBetweenFunction(arg, fromFn, toFn, null, fromType, toType);
        }


        TimestampDriver driver = ColumnType.getTimestampDriver(argType);

        if (fromFn.isConstant() && toFn.isConstant()) {
            final long fromTimestamp = fromFn.getTimestamp(null);
            final long toTimestamp = toFn.getTimestamp(null);
            if (fromTimestamp == Numbers.LONG_NULL || toTimestamp == Numbers.LONG_NULL) {
                CairoException.rethrowCleanupFailure(Misc.freeObjListBestEffort(null, args));
                return BooleanConstant.FALSE;
            }
            return new ConstFunc(
                    arg,
                    Math.min(driver.ceilFrom(fromTimestamp, fromType), driver.ceilFrom(toTimestamp, toType)),
                    Math.max(driver.floorFrom(fromTimestamp, fromType), driver.floorFrom(toTimestamp, toType))
            );
        }
        if (fromType == argType && toType == argType) {
            return new VarBetweenFunction(arg, fromFn, toFn, driver, fromType, toType);
        }
        return new ConvertFunction(arg, fromFn, toFn, driver, fromType, toType);
    }

    private static class ConstFunc extends BooleanFunction implements UnaryFunction {
        private final long from;
        private final Function left;
        private final long to;

        public ConstFunc(Function left, long from, long to) {
            this.left = left;
            this.from = from;
            this.to = to;
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

    /**
     * Compares at the finer of the argument's and each bound's precision: the bounds round inward to the
     * argument's precision.
     */
    private static class ConvertFunction extends VarBetweenFunction {
        public ConvertFunction(Function left, Function from, Function to, TimestampDriver driver, int fromType, int toType) {
            super(left, from, to, driver, fromType, toType);
        }

        @Override
        public boolean getBool(Record rec) {
            final long value = arg.getTimestamp(rec);
            if (value == Numbers.LONG_NULL) {
                return false;
            }
            final long fromTs = from.getTimestamp(rec);
            if (fromTs == Numbers.LONG_NULL) {
                return false;
            }
            final long toTs = to.getTimestamp(rec);
            if (toTs == Numbers.LONG_NULL) {
                return false;
            }
            return Math.min(driver.ceilFrom(fromTs, fromType), driver.ceilFrom(toTs, toType)) <= value
                    && value <= Math.max(driver.floorFrom(fromTs, fromType), driver.floorFrom(toTs, toType));
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
