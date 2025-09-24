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
        if (leftType == rightType) {
            return new Func(left, right);
        } else if (timestampType == rightType) {
            if (left.isConstant()) {
                return new LeftConstFunc(left, right, ColumnType.getTimestampDriver(timestampType).from(left.getTimestamp(null), leftType));
            } else if (left.isRuntimeConstant()) {
                return new LeftRunTimeConstFunc(left, right, ColumnType.getTimestampDriver(timestampType), leftType);
            }
            return new LeftConvertFunc(left, right, ColumnType.getTimestampDriver(rightType), leftType);
        } else {
            if (right.isConstant()) {
                return new RightConstFunc(left, right, ColumnType.getTimestampDriver(timestampType).from(right.getTimestamp(null), rightType));
            } else if (right.isRuntimeConstant()) {
                return new RightRunTimeConstFunc(left, right, ColumnType.getTimestampDriver(timestampType), rightType);
            }
            return new RightConvertFunc(left, right, ColumnType.getTimestampDriver(leftType), rightType);
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

    private static class RightConstFunc extends AbstractEqBinaryFunction {
        private final long value;

        public RightConstFunc(Function left, Function right, long value) {
            super(left, right);
            this.value = value;
        }

        @Override
        public boolean getBool(Record rec) {
            return negated != (left.getTimestamp(rec) == value);
        }
    }

    private static class RightConvertFunc extends AbstractEqBinaryFunction {
        protected TimestampDriver driver;
        protected int fromTimestampType;

        public RightConvertFunc(Function left, Function right, TimestampDriver driver, int fromTimestampType) {
            super(left, right);
            this.driver = driver;
            this.fromTimestampType = fromTimestampType;
        }

        @Override
        public boolean getBool(Record rec) {
            return negated != (left.getTimestamp(rec) == driver.from(right.getTimestamp(rec), fromTimestampType));
        }
    }

    private static class RightRunTimeConstFunc extends AbstractEqBinaryFunction {
        private final TimestampDriver driver;
        private final int fromTimestampType;
        private long value;

        public RightRunTimeConstFunc(Function left, Function right, TimestampDriver driver, int fromTimestampType) {
            super(left, right);
            this.driver = driver;
            this.fromTimestampType = fromTimestampType;
        }

        @Override
        public boolean getBool(Record rec) {
            return negated != (value == left.getTimestamp(rec));
        }

        @Override
        public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
            super.init(symbolTableSource, executionContext);
            value = driver.from(right.getTimestamp(null), fromTimestampType);
        }
    }
}
