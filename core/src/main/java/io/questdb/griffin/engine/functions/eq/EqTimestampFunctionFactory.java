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

package io.questdb.griffin.engine.functions.eq;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.TimestampDriver;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.TimestampBounds;
import io.questdb.griffin.engine.functions.constants.BooleanConstant;
import io.questdb.std.IntList;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;

public class EqTimestampFunctionFactory implements FunctionFactory {

    @Override
    public String getSignature() {
        return "=(NN)";
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
        Function left = args.getQuick(0);
        Function right = args.getQuick(1);
        int leftType = ColumnType.getTimestampType(left.getType());
        int rightType = ColumnType.getTimestampType(right.getType());
        int timestampType = ColumnType.getHigherPrecisionTimestampType(leftType, rightType);
        assert ColumnType.isTimestamp(timestampType);

        // Equality is symmetric: normalize so the lower-precision (converting) side is
        // always on the left. That removes the need for mirrored Right* variants.
        if (leftType != rightType && timestampType == leftType) {
            Function tmpFn = left;
            left = right;
            right = tmpFn;
            int tmpType = leftType;
            leftType = rightType;
            rightType = tmpType;
        }

        // A constant compared with values that are not constant turns into the one value of their type that
        // equals it, so that no value converts to another precision. When no value equals it, as for a constant
        // past the range of the type or between two of its values, nothing matches.
        if (left.isConstant() != right.isConstant()) {
            if (leftType == rightType && right.isConstant()) {
                // the constant comes first, as the plan shows it
                final Function tmpFn = left;
                left = right;
                right = tmpFn;
            }
            final Function column = left.isConstant() ? right : left;
            final Function constant = left.isConstant() ? left : right;
            final int columnType = ColumnType.getTimestampType(column.getType());
            final TimestampDriver driver = ColumnType.getTimestampDriver(ColumnType.isTimestamp(columnType) ? columnType : timestampType);
            // NULL equals NULL
            final long value = TimestampBounds.point(driver, constant);
            if (value == Numbers.LONG_NULL && !TimestampBounds.isNull(constant)) {
                Misc.free(left);
                Misc.free(right);
                return BooleanConstant.FALSE;
            }
            if (column.isRuntimeConstant()) {
                return new ConstRunTimeConstFunc(constant, column, value);
            }
            return new ConstValueFunc(left, right, column, value);
        }

        // Fold to a BooleanConstant when both operands are fully known at factory time.
        // That shortcuts every future evaluation and also skips the per-row virtual
        // call into a constant function. The args are no longer referenced by the
        // returned function, so close them here per the factory contract (same as
        // FunctionParser.functionToConstant does when folding).
        if (left.isConstant() && right.isConstant()) {
            long leftVal = left.getTimestamp(null);
            long rightVal = right.getTimestamp(null);
            final boolean isEqual = isEqual(ColumnType.getTimestampDriver(timestampType), leftVal, leftType, rightVal);
            Misc.free(left);
            Misc.free(right);
            return BooleanConstant.of(isEqual);
        }

        if (leftType == rightType) {
            // No precision conversion needed. Normalize so the cached side is on the
            // left, then cache runtime-const sides to avoid repeating any implicit
            // STRING / SYMBOL to TIMESTAMP cast on every row.
            if (!left.isRuntimeConstant() && right.isRuntimeConstant()) {
                Function tmpFn = left;
                left = right;
                right = tmpFn;
            }
            if (left.isRuntimeConstant()) {
                if (right.isRuntimeConstant()) {
                    return new MatchedBothRunTimeConstFunc(left, right);
                }
                return new MatchedLeftRunTimeConstFunc(left, right);
            }
            return new Func(left, right);
        }

        // Left has lower precision and needs conversion per row. Cache whichever side
        // is known ahead of time so the cast / precision conversion is not repeated.
        TimestampDriver targetDriver = ColumnType.getTimestampDriver(timestampType);
        if (left.isRuntimeConstant()) {
            if (right.isRuntimeConstant()) {
                return new LeftConvertBothRunTimeConstFunc(left, right, targetDriver, leftType);
            }
            return new LeftRunTimeConstFunc(left, right, targetDriver, leftType);
        }
        if (right.isRuntimeConstant()) {
            return new LeftConvertRightRunTimeConstFunc(left, right, targetDriver, leftType);
        }
        return new LeftConvertFunc(left, right, targetDriver, leftType);
    }

    // from(value, fromTimestampType) == other, without converting a value past the range of the other's type
    private static boolean isEqual(TimestampDriver driver, long value, int fromTimestampType, long other) {
        if (value == Numbers.LONG_NULL || other == Numbers.LONG_NULL) {
            return value == other;
        }
        return driver.compareFrom(value, fromTimestampType, other) == 0;
    }

    // the constant on the left, the runtime constant, read once per execution, on the right
    private static class ConstRunTimeConstFunc extends AbstractEqBinaryFunction {
        private final long leftValue;
        private long rightValue;

        public ConstRunTimeConstFunc(Function left, Function right, long leftValue) {
            super(left, right);
            this.leftValue = leftValue;
        }

        @Override
        public boolean getBool(Record rec) {
            return negated != (leftValue == rightValue);
        }

        @Override
        public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
            super.init(symbolTableSource, executionContext);
            rightValue = right.getTimestamp(null);
        }
    }

    private static class ConstValueFunc extends AbstractEqBinaryFunction {
        private final Function column;
        private final long value;

        public ConstValueFunc(Function left, Function right, Function column, long value) {
            super(left, right);
            this.column = column;
            this.value = value;
        }

        @Override
        public boolean getBool(Record rec) {
            return negated != (value == column.getTimestamp(rec));
        }
    }

    private static class Func extends AbstractEqBinaryFunction {
        public Func(Function left, Function right) {
            super(left, right);
        }

        @Override
        public boolean getBool(Record rec) {
            return negated != (left.getTimestamp(rec) == right.getTimestamp(rec));
        }
    }

    private static class LeftConvertBothRunTimeConstFunc extends AbstractEqBinaryFunction {
        private final TimestampDriver driver;
        private final int fromTimestampType;
        private long leftValue;
        private long rightValue;

        public LeftConvertBothRunTimeConstFunc(Function left, Function right, TimestampDriver driver, int fromTimestampType) {
            super(left, right);
            this.driver = driver;
            this.fromTimestampType = fromTimestampType;
        }

        @Override
        public boolean getBool(Record rec) {
            return negated != (leftValue == rightValue);
        }

        @Override
        public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
            super.init(symbolTableSource, executionContext);
            leftValue = driver.from(left.getTimestamp(null), fromTimestampType);
            rightValue = right.getTimestamp(null);
        }
    }

    private static class LeftConvertFunc extends AbstractEqBinaryFunction {
        private final TimestampDriver driver;
        private final int fromTimestampType;

        public LeftConvertFunc(Function left, Function right, TimestampDriver driver, int fromTimestampType) {
            super(left, right);
            this.driver = driver;
            this.fromTimestampType = fromTimestampType;
        }

        @Override
        public boolean getBool(Record rec) {
            return negated != isEqual(driver, left.getTimestamp(rec), fromTimestampType, right.getTimestamp(rec));
        }
    }

    private static class LeftConvertRightRunTimeConstFunc extends AbstractEqBinaryFunction {
        private final TimestampDriver driver;
        private final int fromTimestampType;
        private long rightValue;

        public LeftConvertRightRunTimeConstFunc(Function left, Function right, TimestampDriver driver, int fromTimestampType) {
            super(left, right);
            this.driver = driver;
            this.fromTimestampType = fromTimestampType;
        }

        @Override
        public boolean getBool(Record rec) {
            return negated != isEqual(driver, left.getTimestamp(rec), fromTimestampType, rightValue);
        }

        @Override
        public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
            super.init(symbolTableSource, executionContext);
            rightValue = right.getTimestamp(null);
        }
    }

    private static class LeftRunTimeConstFunc extends AbstractEqBinaryFunction {
        private final TimestampDriver driver;
        private final int fromTimestampType;
        private long value;

        public LeftRunTimeConstFunc(Function left, Function right, TimestampDriver driver, int fromTimestampType) {
            super(left, right);
            this.driver = driver;
            this.fromTimestampType = fromTimestampType;
        }

        @Override
        public boolean getBool(Record rec) {
            return negated != (value == right.getTimestamp(rec));
        }

        @Override
        public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
            super.init(symbolTableSource, executionContext);
            value = driver.from(left.getTimestamp(null), fromTimestampType);
        }
    }

    private static class MatchedBothRunTimeConstFunc extends AbstractEqBinaryFunction {
        private long leftValue;
        private long rightValue;

        public MatchedBothRunTimeConstFunc(Function left, Function right) {
            super(left, right);
        }

        @Override
        public boolean getBool(Record rec) {
            return negated != (leftValue == rightValue);
        }

        @Override
        public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
            super.init(symbolTableSource, executionContext);
            leftValue = left.getTimestamp(null);
            rightValue = right.getTimestamp(null);
        }
    }

    private static class MatchedLeftRunTimeConstFunc extends AbstractEqBinaryFunction {
        private long value;

        public MatchedLeftRunTimeConstFunc(Function left, Function right) {
            super(left, right);
        }

        @Override
        public boolean getBool(Record rec) {
            return negated != (value == right.getTimestamp(rec));
        }

        @Override
        public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
            super.init(symbolTableSource, executionContext);
            value = left.getTimestamp(null);
        }
    }
}
