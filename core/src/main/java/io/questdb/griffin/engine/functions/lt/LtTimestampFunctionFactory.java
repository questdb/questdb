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
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.BinaryFunction;
import io.questdb.griffin.engine.functions.NegatableBooleanFunction;
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
        int leftType = ColumnType.getTimestampType(left.getType());
        int rightType = ColumnType.getTimestampType(right.getType());
        int timestampType = ColumnType.getHigherPrecisionTimestampType(leftType, rightType);
        assert ColumnType.isTimestamp(timestampType);
        // Nullability is static (table-level) information, and a constant's value is
        // known here, so the null handling is resolved ONCE at bind time: a never-null
        // operand's side compiles to a plain value comparison that reads the sentinel
        // bit pattern as data, and only a nullable operand's side keeps its NULL
        // exclusion. No per-row isNotNull() calls, no per-row nullability flags.
        // Cross-precision pairings keep the symmetric NULL exclusion unless both sides
        // are provably never-null; the sentinel identity-maps through the driver, so
        // the never-null case stays a plain value comparison there as well.
        final boolean leftNeverNull = isNeverNull(left);
        final boolean rightNeverNull = isNeverNull(right);
        if (leftType == rightType) {
            if (leftNeverNull && rightNeverNull) {
                return new DataFunc(left, right);
            }
            if (leftNeverNull) {
                return new RightNullableFunc(left, right);
            }
            if (rightNeverNull) {
                return new LeftNullableFunc(left, right);
            }
            return new Func(left, right);
        } else if (leftType != timestampType) {
            TimestampDriver driver = ColumnType.getTimestampDriver(timestampType);
            if (leftNeverNull && rightNeverNull) {
                return new LeftConvertDataFunc(left, right, driver, leftType);
            }
            return new LeftConvertFunc(left, right, driver, leftType);
        } else {
            TimestampDriver driver = ColumnType.getTimestampDriver(timestampType);
            if (leftNeverNull && rightNeverNull) {
                return new RightConvertDataFunc(left, right, driver, rightType);
            }
            return new RightConvertFunc(left, right, driver, rightType);
        }
    }

    private static boolean isNeverNull(Function f) {
        return f.isNotNull() || (f.isConstant() && f.getTimestamp(null) != Numbers.LONG_NULL);
    }

    private abstract static class AbstractLtTimestampFunction extends NegatableBooleanFunction implements BinaryFunction {
        protected final Function leftFunc;
        protected final Function rightFunc;

        public AbstractLtTimestampFunction(Function leftFunc, Function rightFunc) {
            this.leftFunc = leftFunc;
            this.rightFunc = rightFunc;
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

    private static class DataFunc extends AbstractLtTimestampFunction {
        public DataFunc(Function leftFunc, Function rightFunc) {
            super(leftFunc, rightFunc);
        }

        @Override
        public boolean getBool(Record rec) {
            final long l = leftFunc.getTimestamp(rec);
            final long r = rightFunc.getTimestamp(rec);
            return negated ? l >= r : l < r;
        }
    }

    private static class Func extends AbstractLtTimestampFunction {
        public Func(Function leftFunc, Function rightFunc) {
            super(leftFunc, rightFunc);
        }

        @Override
        public boolean getBool(Record rec) {
            return Numbers.lessThan(
                    leftFunc.getTimestamp(rec),
                    rightFunc.getTimestamp(rec),
                    negated
            );
        }
    }

    private static class LeftConvertDataFunc extends AbstractLtTimestampFunction {
        private final TimestampDriver driver;
        private final int toTimestampType;

        public LeftConvertDataFunc(Function leftFunc, Function rightFunc, TimestampDriver driver, int toTimestampType) {
            super(leftFunc, rightFunc);
            this.driver = driver;
            this.toTimestampType = toTimestampType;
        }

        @Override
        public boolean getBool(Record rec) {
            final long l = driver.from(leftFunc.getTimestamp(rec), toTimestampType);
            final long r = rightFunc.getTimestamp(rec);
            return negated ? l >= r : l < r;
        }
    }

    private static class LeftConvertFunc extends AbstractLtTimestampFunction {
        private final TimestampDriver driver;
        private final int toTimestampType;

        public LeftConvertFunc(Function leftFunc, Function rightFunc, TimestampDriver driver, int toTimestampType) {
            super(leftFunc, rightFunc);
            this.driver = driver;
            this.toTimestampType = toTimestampType;
        }

        @Override
        public boolean getBool(Record rec) {
            return Numbers.lessThan(
                    driver.from(leftFunc.getTimestamp(rec), toTimestampType),
                    rightFunc.getTimestamp(rec),
                    negated
            );
        }
    }

    private static class LeftNullableFunc extends AbstractLtTimestampFunction {
        public LeftNullableFunc(Function leftFunc, Function rightFunc) {
            super(leftFunc, rightFunc);
        }

        @Override
        public boolean getBool(Record rec) {
            final long l = leftFunc.getTimestamp(rec);
            final long r = rightFunc.getTimestamp(rec);
            if (l == Numbers.LONG_NULL) {
                return false;
            }
            return negated ? l >= r : l < r;
        }
    }

    private static class RightConvertDataFunc extends AbstractLtTimestampFunction {
        private final TimestampDriver driver;
        private final int toTimestampType;

        public RightConvertDataFunc(Function leftFunc, Function rightFunc, TimestampDriver driver, int toTimestampType) {
            super(leftFunc, rightFunc);
            this.driver = driver;
            this.toTimestampType = toTimestampType;
        }

        @Override
        public boolean getBool(Record rec) {
            final long l = leftFunc.getTimestamp(rec);
            final long r = driver.from(rightFunc.getTimestamp(rec), toTimestampType);
            return negated ? l >= r : l < r;
        }
    }

    private static class RightConvertFunc extends AbstractLtTimestampFunction {
        private final TimestampDriver driver;
        private final int toTimestampType;

        public RightConvertFunc(Function leftFunc, Function rightFunc, TimestampDriver driver, int toTimestampType) {
            super(leftFunc, rightFunc);
            this.driver = driver;
            this.toTimestampType = toTimestampType;
        }

        @Override
        public boolean getBool(Record rec) {
            return Numbers.lessThan(
                    leftFunc.getTimestamp(rec),
                    driver.from(rightFunc.getTimestamp(rec), toTimestampType),
                    negated
            );
        }
    }

    private static class RightNullableFunc extends AbstractLtTimestampFunction {
        public RightNullableFunc(Function leftFunc, Function rightFunc) {
            super(leftFunc, rightFunc);
        }

        @Override
        public boolean getBool(Record rec) {
            final long l = leftFunc.getTimestamp(rec);
            final long r = rightFunc.getTimestamp(rec);
            if (r == Numbers.LONG_NULL) {
                return false;
            }
            return negated ? l >= r : l < r;
        }
    }
}
