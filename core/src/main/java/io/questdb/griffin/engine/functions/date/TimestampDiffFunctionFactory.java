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

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.TimestampDriver;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.BinaryFunction;
import io.questdb.griffin.engine.functions.LongFunction;
import io.questdb.griffin.engine.functions.TernaryFunction;
import io.questdb.griffin.engine.functions.UnaryFunction;
import io.questdb.std.IntList;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;

public class TimestampDiffFunctionFactory implements FunctionFactory {
    @Override
    public String getSignature() {
        return "datediff(ANN)";
    }

    @Override
    public Function newInstance(
            int position,
            ObjList<Function> args,
            IntList argPositions,
            CairoConfiguration configuration,
            SqlExecutionContext sqlExecutionContext
    ) {
        final Function periodFunction = args.getQuick(0);
        final Function start = args.getQuick(1);
        final Function end = args.getQuick(2);
        final int startType = start.getType();
        final int endType = end.getType();
        int timestampType = ColumnType.getTimestampType(startType, endType, configuration);
        TimestampDriver driver = ColumnType.getTimestampDriver(timestampType);

        if (periodFunction.isConstant()) {
            final char period = periodFunction.getChar(null);
            TimestampDriver.TimestampDiffMethod diffMethod = driver.getTimestampDiffMethod(period);

            if (diffMethod != null) {
                if (start.isConstant() && start.getTimestamp(null) != Numbers.LONG_NULL) {
                    return new DiffVarConstFunction(args.getQuick(2), start.getLong(null), diffMethod, driver, period);
                }
                if (end.isConstant() && end.getTimestamp(null) != Numbers.LONG_NULL) {
                    return new DiffVarConstFunction(args.getQuick(1), end.getLong(null), diffMethod, driver, period);
                }
                return new DiffVarVarFunction(args.getQuick(1), args.getQuick(2), driver, diffMethod, period);
            }
            return driver.getTimestampConstantNull();
        }
        return new DateDiffFunc(args.getQuick(0), args.getQuick(1), args.getQuick(2), driver);
    }

    private static class DateDiffFunc extends LongFunction implements TernaryFunction {
        final Function center;
        final TimestampDriver driver;
        final Function left;
        final int leftType;
        final Function right;
        final int rightType;

        public DateDiffFunc(Function left, Function center, Function right, TimestampDriver driver) {
            this.left = left;
            this.center = center;
            this.right = right;
            this.driver = driver;
            this.leftType = center.getType();
            this.rightType = right.getType();
        }

        @Override
        public Function getCenter() {
            return center;
        }

        @Override
        public Function getLeft() {
            return left;
        }

        @Override
        public long getLong(Record rec) {
            final char l = left.getChar(rec);
            final long c = center.getTimestamp(rec);
            final long r = right.getTimestamp(rec);
            return driver.getPeriodBetween(l, c, r, leftType, rightType);
        }

        @Override
        public Function getRight() {
            return right;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val("datediff('").val(left).val("',").val(center).val(',').val(right).val(')');
        }
    }

    private static class DiffVarConstFunction extends LongFunction implements UnaryFunction {
        private final Function arg;
        private final long constantTime;
        private final TimestampDriver driver;
        private final TimestampDriver.TimestampDiffMethod func;
        private final char symbol;
        private final int timestampType;

        public DiffVarConstFunction(Function left, long right, TimestampDriver.TimestampDiffMethod func, TimestampDriver driver, char symbol) {
            this.arg = left;
            this.constantTime = right;
            this.func = func;
            this.symbol = symbol;
            this.driver = driver;
            this.timestampType = arg.getType();
        }

        @Override
        public Function getArg() {
            return arg;
        }

        @Override
        public long getLong(Record rec) {
            final long l = driver.from(arg.getTimestamp(rec), timestampType);
            if (l == Numbers.LONG_NULL) {
                return Numbers.LONG_NULL;
            }
            return func.diff(l, constantTime);
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val("datediff('").val(symbol).val("',").val(arg).val(',').val(constantTime).val(')');
        }
    }

    private static class DiffVarVarFunction extends LongFunction implements BinaryFunction {
        private final TimestampDriver driver;
        private final TimestampDriver.TimestampDiffMethod func;
        private final Function left;
        private final int leftType;
        private final Function right;
        private final int rightType;
        private final char symbol;


        public DiffVarVarFunction(Function left, Function right, TimestampDriver driver, TimestampDriver.TimestampDiffMethod func, char symbol) {
            this.left = left;
            this.right = right;
            this.func = func;
            this.symbol = symbol;
            this.driver = driver;
            this.leftType = left.getType();
            this.rightType = right.getType();
        }

        @Override
        public Function getLeft() {
            return left;
        }

        @Override
        public long getLong(Record rec) {
            final long l = driver.from(left.getTimestamp(rec), leftType);
            final long r = driver.from(right.getTimestamp(rec), rightType);
            if (l == Numbers.LONG_NULL || r == Numbers.LONG_NULL) {
                return Numbers.LONG_NULL;
            }
            return func.diff(l, r);
        }

        @Override
        public Function getRight() {
            return right;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val("datediff('").val(symbol).val("',").val(left).val(',').val(right).val(')');
        }
    }
}
