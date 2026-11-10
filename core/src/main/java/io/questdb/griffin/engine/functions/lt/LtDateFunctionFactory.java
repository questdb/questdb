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
import io.questdb.cairo.MillisTimestampDriver;
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

public class LtDateFunctionFactory implements FunctionFactory {
    @Override
    public String getSignature() {
        return "<(MM)";
    }

    @Override
    public boolean isBoolean() {
        return true;
    }

    @Override
    public Function newInstance(int position, ObjList<Function> args, IntList argPositions, CairoConfiguration configuration, SqlExecutionContext sqlExecutionContext) {
        final Function left = args.getQuick(0);
        final Function right = args.getQuick(1);
        // A constant or runtime-constant side, text with digits finer than a millisecond included, turns into a
        // threshold of DATE values
        if (isBound(right)) {
            return new RightBoundFunc(left, right);
        }
        if (isBound(left)) {
            return new LeftBoundFunc(left, right);
        }
        return new LtDateFunctionFactory.LtDateFunction(left, right);
    }

    private static boolean isBound(Function func) {
        return func.isConstant() || func.isRuntimeConstant();
    }

    // A comparison of values with a constant or runtime-constant bound c, as LtTimestampFunctionFactory does it
    private abstract static class BoundFunc extends LtDateFunction {
        protected final Function bound;
        protected final Function column;
        // negated, or the opposite when the threshold stands for a bound past the range of the type
        protected boolean isNegated;
        protected long threshold;

        protected BoundFunc(Function left, Function right, Function column, Function bound) {
            super(left, right);
            this.column = column;
            this.bound = bound;
        }

        @Override
        public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
            super.init(symbolTableSource, executionContext);
            if (!bound.isConstant()) {
                setThreshold();
            }
        }

        @Override
        public void setNegated() {
            super.setNegated();
            if (bound.isConstant()) {
                setThreshold();
            }
        }

        protected abstract void setThreshold();
    }

    // c < value: holds exactly when the last value at or before c is less than the value, and so does its negation
    private static class LeftBoundFunc extends BoundFunc {

        public LeftBoundFunc(Function left, Function right) {
            super(left, right, right, left);
            if (bound.isConstant()) {
                setThreshold();
            }
        }

        @Override
        public boolean getBool(Record rec) {
            return Numbers.lessThan(threshold, column.getDate(rec), isNegated);
        }

        @Override
        protected void setThreshold() {
            isNegated = negated;
            if (TimestampBounds.isNull(bound)) {
                threshold = Numbers.LONG_NULL;
                return;
            }
            threshold = TimestampBounds.upper(MillisTimestampDriver.INSTANCE, bound, false);
            if (threshold == Numbers.LONG_NULL) {
                threshold = Long.MAX_VALUE;
                isNegated = !negated;
            }
        }
    }

    private static class LtDateFunction extends NegatableBooleanFunction implements BinaryFunction {
        private final Function left;
        private final Function right;

        public LtDateFunction(Function left, Function right) {
            this.left = left;
            this.right = right;
        }

        @Override
        public boolean getBool(Record rec) {
            return Numbers.lessThan(
                    this.left.getDate(rec),
                    this.right.getDate(rec),
                    negated
            );
        }

        @Override
        public Function getLeft() {
            return left;
        }

        @Override
        public Function getRight() {
            return right;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(left);
            if (negated) {
                sink.val(">=");
            } else {
                sink.val('<');
            }
            sink.val(right);
        }
    }

    // value < c: holds exactly when the value is less than the first value at or after c, and so does its negation
    private static class RightBoundFunc extends BoundFunc {

        public RightBoundFunc(Function left, Function right) {
            super(left, right, left, right);
            if (bound.isConstant()) {
                setThreshold();
            }
        }

        @Override
        public boolean getBool(Record rec) {
            return Numbers.lessThan(column.getDate(rec), threshold, isNegated);
        }

        @Override
        protected void setThreshold() {
            isNegated = negated;
            if (TimestampBounds.isNull(bound)) {
                threshold = Numbers.LONG_NULL;
                return;
            }
            threshold = TimestampBounds.lower(MillisTimestampDriver.INSTANCE, bound, false);
            if (threshold == Numbers.LONG_NULL) {
                threshold = Long.MIN_VALUE + 1;
                isNegated = !negated;
            }
        }
    }
}
