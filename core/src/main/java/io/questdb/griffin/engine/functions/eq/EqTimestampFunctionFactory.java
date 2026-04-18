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
import io.questdb.std.IntList;
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

        if (leftType == rightType) {
            // No precision conversion needed. Normalize so the cached side is on the
            // left, then cache const and runtime-const sides to avoid repeating any
            // implicit STRING / SYMBOL to TIMESTAMP cast on every row.
            if (!left.isConstant() && right.isConstant()) {
                Function tmpFn = left;
                left = right;
                right = tmpFn;
            } else if (!left.isConstant() && !left.isRuntimeConstant() && right.isRuntimeConstant()) {
                Function tmpFn = left;
                left = right;
                right = tmpFn;
            }
            if (left.isConstant()) {
                return new LeftConstFunc(left, right, left.getTimestamp(null));
            }
            if (left.isRuntimeConstant()) {
                return new MatchedLeftRunTimeConstFunc(left, right);
            }
            return new Func(left, right);
        }

        // Left has lower precision and needs conversion per row. Cache whichever side
        // is known ahead of time so the cast / precision conversion is not repeated.
        if (left.isConstant()) {
            return new LeftConstFunc(left, right, ColumnType.getTimestampDriver(timestampType).from(left.getTimestamp(null), leftType));
        }
        if (left.isRuntimeConstant()) {
            return new LeftRunTimeConstFunc(left, right, ColumnType.getTimestampDriver(timestampType), leftType);
        }
        if (right.isConstant()) {
            return new LeftConvertRightConstFunc(left, right, ColumnType.getTimestampDriver(rightType), leftType, right.getTimestamp(null));
        }
        if (right.isRuntimeConstant()) {
            return new LeftConvertRightRunTimeConstFunc(left, right, ColumnType.getTimestampDriver(rightType), leftType);
        }
        return new LeftConvertFunc(left, right, ColumnType.getTimestampDriver(rightType), leftType);
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

    private static class LeftConstFunc extends AbstractEqBinaryFunction {
        private final long value;

        public LeftConstFunc(Function left, Function right, long value) {
            super(left, right);
            this.value = value;
        }

        @Override
        public boolean getBool(Record rec) {
            return negated != (value == right.getTimestamp(rec));
        }
    }

    private static class LeftConvertFunc extends AbstractEqBinaryFunction {
        protected TimestampDriver driver;
        protected int fromTimestampType;

        public LeftConvertFunc(Function left, Function right, TimestampDriver driver, int fromTimestampType) {
            super(left, right);
            this.driver = driver;
            this.fromTimestampType = fromTimestampType;
        }

        @Override
        public boolean getBool(Record rec) {
            return negated != (driver.from(left.getTimestamp(rec), fromTimestampType) == right.getTimestamp(rec));
        }
    }

    private static class LeftConvertRightConstFunc extends AbstractEqBinaryFunction {
        private final TimestampDriver driver;
        private final int fromTimestampType;
        private final long rightValue;

        public LeftConvertRightConstFunc(Function left, Function right, TimestampDriver driver, int fromTimestampType, long rightValue) {
            super(left, right);
            this.driver = driver;
            this.fromTimestampType = fromTimestampType;
            this.rightValue = rightValue;
        }

        @Override
        public boolean getBool(Record rec) {
            return negated != (driver.from(left.getTimestamp(rec), fromTimestampType) == rightValue);
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
            return negated != (driver.from(left.getTimestamp(rec), fromTimestampType) == rightValue);
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
