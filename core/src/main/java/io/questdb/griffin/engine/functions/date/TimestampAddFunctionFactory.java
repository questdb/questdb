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
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.BinaryFunction;
import io.questdb.griffin.engine.functions.TernaryFunction;
import io.questdb.griffin.engine.functions.TimestampFunction;
import io.questdb.griffin.engine.functions.UnaryFunction;
import io.questdb.std.IntList;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;

public class TimestampAddFunctionFactory implements FunctionFactory {

    @Override
    public String getSignature() {
        return "dateadd(AIN)";
    }

    @Override
    public Function newInstance(int position, ObjList<Function> args, IntList argPositions, CairoConfiguration configuration, SqlExecutionContext sqlExecutionContext) throws SqlException {
        Function periodFunc = args.getQuick(0);
        Function strideFunc = args.getQuick(1);
        Function timestampFunc = args.getQuick(2);
        int stride;
        int timestampType = ColumnType.getHigherPrecisionTimestampType(ColumnType.getTimestampType(timestampFunc.getType()), ColumnType.TIMESTAMP_MICRO);

        if (periodFunc.isConstant()) {
            char period = periodFunc.getChar(null);
            TimestampDriver.TimestampAddMethod periodAddFunc = ColumnType.getTimestampDriver(timestampType).getAddMethod(period);
            if (periodAddFunc == null) {
                throw SqlException.$(argPositions.getQuick(0), "invalid time period [unit=").put(period).put(']');
            }

            if (strideFunc.isConstant()) {
                if ((stride = strideFunc.getInt(null)) != Numbers.INT_NULL) {
                    return new TimestampAddConstConstVar(period, periodAddFunc, stride, timestampFunc, timestampType);
                } else {
                    throw SqlException.$(argPositions.getQuick(1), "`null` is not a valid stride");
                }
            }
            return new TimestampAddConstVarVar(period, periodAddFunc, strideFunc, timestampFunc, timestampType);
        }
        return new TimestampAddFunc(periodFunc, strideFunc, argPositions.getQuick(1), timestampFunc, timestampType);
    }

    private static class TimestampAddConstConstVar extends TimestampFunction implements UnaryFunction {
        private final char period;
        private final TimestampDriver.TimestampAddMethod periodAddFunction;
        private final int stride;
        private final Function timestampFunc;

        public TimestampAddConstConstVar(char period, TimestampDriver.TimestampAddMethod periodAddFunction, int stride, Function timestampFunc, int timestampType) {
            super(timestampType);
            this.period = period;
            this.periodAddFunction = periodAddFunction;
            this.stride = stride;
            this.timestampFunc = timestampFunc;
        }

        @Override
        public Function getArg() {
            return timestampFunc;
        }

        @Override
        public long getTimestamp(Record rec) {
            final long timestamp = timestampFunc.getTimestamp(rec);
            if (timestamp == Numbers.LONG_NULL) {
                return Numbers.LONG_NULL;
            }
            return periodAddFunction.add(timestamp, stride);
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val("dateadd('").val(period).val("',").val(stride).val(',').val(timestampFunc).val(')');
        }
    }

    private static class TimestampAddConstVarVar extends TimestampFunction implements BinaryFunction {
        private final char period;
        private final TimestampDriver.TimestampAddMethod periodAddFunc;
        private final Function strideFunc;
        private final Function timestampFunc;

        public TimestampAddConstVarVar(char period, TimestampDriver.TimestampAddMethod periodAddFunc, Function strideFunc, Function timestampFunc, int timestampType) {
            super(timestampType);
            this.period = period;
            this.periodAddFunc = periodAddFunc;
            this.strideFunc = strideFunc;
            this.timestampFunc = timestampFunc;
        }

        @Override
        public Function getLeft() {
            return timestampFunc;
        }

        @Override
        public Function getRight() {
            return strideFunc;
        }

        @Override
        public long getTimestamp(Record rec) {
            final int stride = strideFunc.getInt(rec);
            final long timestamp = timestampFunc.getTimestamp(rec);
            if (timestamp == Numbers.LONG_NULL || stride == Numbers.INT_NULL) {
                return Numbers.LONG_NULL;
            }
            return periodAddFunc.add(timestamp, stride);
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val("dateadd('").val(period).val("',").val(strideFunc).val(',').val(timestampFunc).val(')');
        }
    }

    private static class TimestampAddFunc extends TimestampFunction implements TernaryFunction {
        private final Function periodFunc;
        private final Function strideFunc;
        private final int stridePosition;
        private final Function timestampFunc;

        public TimestampAddFunc(Function periodFunc, Function strideFunc, int stridePosition, Function timestampFunc, int timestampType) {
            super(timestampType);
            this.periodFunc = periodFunc;
            this.strideFunc = strideFunc;
            this.stridePosition = stridePosition;
            this.timestampFunc = timestampFunc;
        }

        @Override
        public Function getCenter() {
            return strideFunc;
        }

        @Override
        public Function getLeft() {
            return periodFunc;
        }

        @Override
        public String getName() {
            return "dateadd";
        }

        @Override
        public Function getRight() {
            return timestampFunc;
        }

        @Override
        public long getTimestamp(Record rec) {
            final char period = periodFunc.getChar(rec);
            final int stride = strideFunc.getInt(rec);
            final long timestamp = timestampFunc.getTimestamp(rec);

            if (stride == Numbers.INT_NULL) {
                throw CairoException.nonCritical().position(stridePosition).put("`null` is not a valid stride");
            }

            if (timestamp == Numbers.LONG_NULL) {
                return Numbers.LONG_NULL;
            }
            return timestampDriver.add(timestamp, period, stride);
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val("dateadd('").val(periodFunc).val("',").val(strideFunc).val(',').val(timestampFunc).val(')');
        }
    }
}
