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
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.TimestampDriver;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.FunctionExtension;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.BinaryFunction;
import io.questdb.griffin.engine.functions.IntervalFunction;
import io.questdb.griffin.engine.functions.constants.IntervalConstant;
import io.questdb.std.IntList;
import io.questdb.std.Interval;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import org.jetbrains.annotations.NotNull;


public class IntervalFunctionFactory implements FunctionFactory {

    @Override
    public String getSignature() {
        return "interval(NN)";
    }

    @Override
    public Function newInstance(
            int position,
            ObjList<Function> args,
            IntList argPositions,
            CairoConfiguration configuration,
            SqlExecutionContext sqlExecutionContext
    ) throws SqlException {
        final Function loFunc = args.getQuick(0);
        final Function hiFunc = args.getQuick(1);
        int leftTimestampType = loFunc.getType();
        int rightTimestampType = hiFunc.getType();
        int timestampType = ColumnType.getTimestampType(leftTimestampType, rightTimestampType, configuration);
        TimestampDriver driver = ColumnType.getTimestampDriver(timestampType);
        int intervalType = ColumnType.getIntervalType(timestampType);
        if (loFunc.isConstant() && hiFunc.isConstant()) {
            long lo = driver.from(loFunc.getTimestamp(null), leftTimestampType);
            long hi = driver.from(hiFunc.getTimestamp(null), rightTimestampType);
            if (lo == Numbers.LONG_NULL || hi == Numbers.LONG_NULL) {
                return driver.getIntervalConstantNull();
            }
            if (lo > hi) {
                throw SqlException.position(position).put("invalid interval boundaries");
            }
            return IntervalConstant.newInstance(lo, hi, intervalType);
        }
        if ((loFunc.isConstant() || loFunc.isRuntimeConstant())
                || (hiFunc.isConstant() || hiFunc.isRuntimeConstant())) {
            return new RuntimeConstFunc(position, loFunc, hiFunc, intervalType, driver);
        }
        if (leftTimestampType == rightTimestampType || !ColumnType.isTimestamp(leftTimestampType) || !ColumnType.isTimestamp(rightTimestampType)) {
            return new Func(loFunc, hiFunc, intervalType, driver);
        } else if (leftTimestampType < rightTimestampType) {
            return new LeftConvert(loFunc, hiFunc, intervalType, driver);
        } else {
            return new RightConvert(loFunc, hiFunc, intervalType, driver);
        }
    }

    private static class Func extends IntervalFunction implements BinaryFunction, FunctionExtension {
        protected final Function hiFunc;
        protected final Interval interval = new Interval();
        protected final Function loFunc;
        protected final TimestampDriver timestampDriver;

        public Func(Function loFunc, Function hiFunc, int timestampType, TimestampDriver timestampDriver) {
            super(timestampType);
            this.loFunc = loFunc;
            this.hiFunc = hiFunc;
            this.timestampDriver = timestampDriver;
        }

        @Override
        public FunctionExtension extendedOps() {
            return this;
        }

        @Override
        public int getArrayLength() {
            throw new UnsupportedOperationException();
        }

        @Override
        public @NotNull Interval getInterval(Record rec) {
            long l = loFunc.getTimestamp(rec);
            long r = hiFunc.getTimestamp(rec);
            if (l == Numbers.LONG_NULL || r == Numbers.LONG_NULL) {
                return Interval.NULL;
            }
            if (l > r) {
                throw CairoException.nonCritical().put("invalid interval boundaries");
            }
            return interval.of(l, r);
        }

        @Override
        public Function getLeft() {
            return loFunc;
        }

        @Override
        public String getName() {
            return "interval";
        }

        @Override
        public Record getRecord(Record rec) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Function getRight() {
            return hiFunc;
        }

        @Override
        public CharSequence getStrA(Record rec, int arrayIndex) {
            throw new UnsupportedOperationException();
        }

        @Override
        public CharSequence getStrB(Record rec, int arrayIndex) {
            throw new UnsupportedOperationException();
        }

        @Override
        public int getStrLen(Record rec, int arrayIndex) {
            throw new UnsupportedOperationException();
        }
    }

    private static class LeftConvert extends Func {
        private final int leftFunctionType;

        public LeftConvert(Function loFunc, Function hiFunc, int timestampType, TimestampDriver timestampDriver) {
            super(loFunc, hiFunc, timestampType, timestampDriver);
            this.leftFunctionType = loFunc.getType();
        }

        @Override
        public @NotNull Interval getInterval(Record rec) {
            long l = timestampDriver.from(loFunc.getTimestamp(rec), leftFunctionType);
            long r = hiFunc.getTimestamp(rec);
            if (l == Numbers.LONG_NULL || r == Numbers.LONG_NULL) {
                return Interval.NULL;
            }
            if (l > r) {
                throw CairoException.nonCritical().put("invalid interval boundaries");
            }
            return interval.of(l, r);
        }
    }

    private static class RightConvert extends Func {
        private final int rightFunctionType;

        public RightConvert(Function loFunc, Function hiFunc, int timestampType, TimestampDriver timestampDriver) {
            super(loFunc, hiFunc, timestampType, timestampDriver);
            this.rightFunctionType = hiFunc.getType();
        }

        @Override
        public @NotNull Interval getInterval(Record rec) {
            long l = loFunc.getTimestamp(rec);
            long r = timestampDriver.from(hiFunc.getTimestamp(rec), rightFunctionType);
            if (l == Numbers.LONG_NULL || r == Numbers.LONG_NULL) {
                return Interval.NULL;
            }
            if (l > r) {
                throw CairoException.nonCritical().put("invalid interval boundaries");
            }
            return interval.of(l, r);
        }
    }

    private static class RuntimeConstFunc extends IntervalFunction implements BinaryFunction, FunctionExtension {
        private final TimestampDriver driver;
        private final Function hiFunc;
        private final Interval interval = new Interval();
        private final Function loFunc;
        private final int position;

        public RuntimeConstFunc(int position, Function loFunc, Function hiFunc, int intervalType, TimestampDriver driver) {
            super(intervalType);
            this.position = position;
            this.loFunc = loFunc;
            this.hiFunc = hiFunc;
            this.driver = driver;
        }

        @Override
        public FunctionExtension extendedOps() {
            return this;
        }

        @Override
        public int getArrayLength() {
            throw new UnsupportedOperationException();
        }

        @Override
        public @NotNull Interval getInterval(Record rec) {
            return interval;
        }

        @Override
        public Function getLeft() {
            return loFunc;
        }

        @Override
        public String getName() {
            return "interval";
        }

        @Override
        public Record getRecord(Record rec) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Function getRight() {
            return hiFunc;
        }

        @Override
        public CharSequence getStrA(Record rec, int arrayIndex) {
            throw new UnsupportedOperationException();
        }

        @Override
        public CharSequence getStrB(Record rec, int arrayIndex) {
            throw new UnsupportedOperationException();
        }

        @Override
        public int getStrLen(Record rec, int arrayIndex) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
            BinaryFunction.super.init(symbolTableSource, executionContext);
            long lo = driver.from(loFunc.getTimestamp(null), loFunc.getType());
            long hi = driver.from(hiFunc.getTimestamp(null), hiFunc.getType());
            if (lo == Numbers.LONG_NULL || hi == Numbers.LONG_NULL) {
                interval.of(Interval.NULL.getLo(), Interval.NULL.getHi());
            }
            if (lo > hi) {
                throw SqlException.position(position).put("invalid interval boundaries");
            }
            interval.of(lo, hi);
        }

        @Override
        public boolean isConstant() {
            return false;
        }

        @Override
        public boolean isRuntimeConstant() {
            return true;
        }
    }
}
