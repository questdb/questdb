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

package io.questdb.griffin.engine.functions.lt;

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
import io.questdb.griffin.engine.functions.BinaryFunction;
import io.questdb.griffin.engine.functions.NegatableBooleanFunction;
import io.questdb.griffin.engine.functions.TimestampBounds;
import io.questdb.std.IntList;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;

public class LtTimestampFunctionFactory implements FunctionFactory {

    @Override
    public String getSignature() {
        return "<(NN)";
    }

    @Override
    public boolean isBoolean() {
        return true;
    }

    @Override
    public Function newInstance(
            int position,
            ObjList<Function> args,
            IntList argPositions,
            CairoConfiguration configuration,
            SqlExecutionContext sqlExecutionContext
    ) {
        final Function left = args.getQuick(0);
        final Function right = args.getQuick(1);
        final int leftType = ColumnType.getTimestampType(left.getType());
        final int rightType = ColumnType.getTimestampType(right.getType());
        final int timestampType = ColumnType.getHigherPrecisionTimestampType(leftType, rightType);
        assert ColumnType.isTimestamp(timestampType);
        // A constant or runtime-constant side turns into bounds in the type of the other side's values, so that
        // no value converts to another precision.
        if (isBound(right)) {
            return new BoundFunc(left, right, left, right, getDriver(leftType, timestampType), false);
        }
        if (isBound(left)) {
            return new BoundFunc(left, right, right, left, getDriver(rightType, timestampType), true);
        }
        if (leftType == rightType) {
            return new Func(left, right);
        }
        final TimestampDriver driver = ColumnType.getTimestampDriver(timestampType);
        if (leftType != timestampType) {
            return new LeftConvertFunc(left, right, driver, leftType);
        }
        return new RightConvertFunc(left, right, driver, rightType);
    }

    private static TimestampDriver getDriver(int valueType, int timestampType) {
        // values of a type other than a timestamp, such as LONG, count in the unit of the comparison's type
        return ColumnType.getTimestampDriver(ColumnType.isTimestamp(valueType) ? valueType : timestampType);
    }

    private static boolean isBound(Function func) {
        return func.isConstant() || func.isRuntimeConstant();
    }

    // column < bound, or column > bound when the bound is on the left, and their negations >= and <=. The
    // non-NULL values in [lo, hi] satisfy it: lo and hi come from the bound once for a constant and on each
    // execution for a runtime constant.
    private static class BoundFunc extends NegatableBooleanFunction implements BinaryFunction {
        private final Function bound;
        private final Function column;
        private final TimestampDriver driver;
        private final boolean isBoundOnLeft;
        private final Function leftFunc;
        private final Function rightFunc;
        private long hi;
        private long lo;

        public BoundFunc(
                Function leftFunc,
                Function rightFunc,
                Function column,
                Function bound,
                TimestampDriver driver,
                boolean isBoundOnLeft
        ) {
            this.leftFunc = leftFunc;
            this.rightFunc = rightFunc;
            this.column = column;
            this.bound = bound;
            this.driver = driver;
            this.isBoundOnLeft = isBoundOnLeft;
            if (bound.isConstant()) {
                setRange();
            }
        }

        @Override
        public boolean getBool(Record rec) {
            final long value = column.getTimestamp(rec);
            return lo <= value & value <= hi;
        }

        @Override
        public Function getLeft() {
            return leftFunc;
        }

        @Override
        public Function getRight() {
            return rightFunc;
        }

        @Override
        public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
            BinaryFunction.super.init(symbolTableSource, executionContext);
            if (!bound.isConstant()) {
                setRange();
            }
        }

        @Override
        public void setNegated() {
            super.setNegated();
            if (bound.isConstant()) {
                setRange();
            }
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(leftFunc);
            if (negated) {
                sink.val(">=");
            } else {
                sink.val('<');
            }
            sink.val(rightFunc);
        }

        private void setRange() {
            // column < c and column > c are strict, their negations column >= c and column <= c are not
            final boolean isStrict = !negated;
            if (TimestampBounds.isNull(bound)) {
                // as Numbers.lessThan(): NULL >= NULL and NULL <= NULL hold, NULL < NULL does not
                if (isStrict) {
                    lo = TimestampBounds.EMPTY_LO;
                    hi = TimestampBounds.EMPTY_HI;
                } else {
                    lo = hi = Numbers.LONG_NULL;
                }
                return;
            }
            if (isBoundOnLeft == negated) {
                // column < c or column <= c
                lo = Long.MIN_VALUE + 1;
                hi = TimestampBounds.upper(driver, bound, isStrict);
            } else {
                lo = TimestampBounds.lower(driver, bound, isStrict);
                hi = Long.MAX_VALUE;
            }
            if (lo == Numbers.LONG_NULL || hi == Numbers.LONG_NULL) {
                lo = TimestampBounds.EMPTY_LO;
                hi = TimestampBounds.EMPTY_HI;
            }
        }
    }

    private static class Func extends NegatableBooleanFunction implements BinaryFunction {
        private final Function leftFunc;
        private final Function rightFunc;

        public Func(Function leftFunc, Function rightFunc) {
            this.leftFunc = leftFunc;
            this.rightFunc = rightFunc;
        }

        @Override
        public boolean getBool(Record rec) {
            return Numbers.lessThan(
                    leftFunc.getTimestamp(rec),
                    rightFunc.getTimestamp(rec),
                    negated
            );
        }

        @Override
        public Function getLeft() {
            return leftFunc;
        }

        @Override
        public Function getRight() {
            return rightFunc;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(leftFunc);
            if (negated) {
                sink.val(">=");
            } else {
                sink.val('<');
            }
            sink.val(rightFunc);
        }
    }

    // the left side has the lower precision; compareFrom() never converts a value past the range of the right side
    private static class LeftConvertFunc extends NegatableBooleanFunction implements BinaryFunction {
        private final TimestampDriver driver;
        private final int fromTimestampType;
        private final Function leftFunc;
        private final Function rightFunc;

        public LeftConvertFunc(Function leftFunc, Function rightFunc, TimestampDriver driver, int fromTimestampType) {
            this.leftFunc = leftFunc;
            this.rightFunc = rightFunc;
            this.driver = driver;
            this.fromTimestampType = fromTimestampType;
        }

        @Override
        public boolean getBool(Record rec) {
            final long left = leftFunc.getTimestamp(rec);
            final long right = rightFunc.getTimestamp(rec);
            if (left == Numbers.LONG_NULL || right == Numbers.LONG_NULL) {
                // as Numbers.lessThan(): NULL >= NULL holds, no other comparison with NULL does
                return negated && left == right;
            }
            final int cmp = driver.compareFrom(left, fromTimestampType, right);
            return negated ? cmp >= 0 : cmp < 0;
        }

        @Override
        public Function getLeft() {
            return leftFunc;
        }

        @Override
        public Function getRight() {
            return rightFunc;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(leftFunc);
            if (negated) {
                sink.val(">=");
            } else {
                sink.val('<');
            }
            sink.val(rightFunc);
        }
    }

    // the right side has the lower precision; compareFrom() never converts a value past the range of the left side
    private static class RightConvertFunc extends NegatableBooleanFunction implements BinaryFunction {
        private final TimestampDriver driver;
        private final int fromTimestampType;
        private final Function leftFunc;
        private final Function rightFunc;

        public RightConvertFunc(Function leftFunc, Function rightFunc, TimestampDriver driver, int fromTimestampType) {
            this.leftFunc = leftFunc;
            this.rightFunc = rightFunc;
            this.driver = driver;
            this.fromTimestampType = fromTimestampType;
        }

        @Override
        public boolean getBool(Record rec) {
            final long left = leftFunc.getTimestamp(rec);
            final long right = rightFunc.getTimestamp(rec);
            if (left == Numbers.LONG_NULL || right == Numbers.LONG_NULL) {
                // as Numbers.lessThan(): NULL >= NULL holds, no other comparison with NULL does
                return negated && left == right;
            }
            // right against left
            final int cmp = driver.compareFrom(right, fromTimestampType, left);
            return negated ? cmp <= 0 : cmp > 0;
        }

        @Override
        public Function getLeft() {
            return leftFunc;
        }

        @Override
        public Function getRight() {
            return rightFunc;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(leftFunc);
            if (negated) {
                sink.val(">=");
            } else {
                sink.val('<');
            }
            sink.val(rightFunc);
        }
    }
}
