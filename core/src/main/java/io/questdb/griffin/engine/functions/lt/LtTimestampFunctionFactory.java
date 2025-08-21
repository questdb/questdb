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
        int leftType = ColumnType.getTimestampType(args.getQuick(0).getType());
        int rightType = ColumnType.getTimestampType(args.getQuick(1).getType());
        int timestampType = ColumnType.getHigherPrecisionTimestampType(leftType, rightType);
        assert ColumnType.isTimestamp(timestampType);
        if (leftType == rightType) {
            return new Func(args.getQuick(0), args.getQuick(1));
        } else if (leftType != timestampType) {
            return new LeftConvertFunc(args.getQuick(0), args.getQuick(1), ColumnType.getTimestampDriver(timestampType), leftType);
        } else {
            return new RightConvertFunc(args.getQuick(0), args.getQuick(1), ColumnType.getTimestampDriver(timestampType), rightType);
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

    private static class LeftConvertFunc extends NegatableBooleanFunction implements BinaryFunction {
        private final Function leftFunc;
        private final Function rightFunc;
        protected TimestampDriver driver;
        protected int toTimestampType;

        public LeftConvertFunc(Function leftFunc, Function rightFunc, TimestampDriver driver, int toTimestampType) {
            this.leftFunc = leftFunc;
            this.rightFunc = rightFunc;
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

    private static class RightConvertFunc extends NegatableBooleanFunction implements BinaryFunction {
        private final Function leftFunc;
        private final Function rightFunc;
        protected TimestampDriver driver;
        protected int toTimestampType;

        public RightConvertFunc(Function leftFunc, Function rightFunc, TimestampDriver driver, int toTimestampType) {
            this.leftFunc = leftFunc;
            this.rightFunc = rightFunc;
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
