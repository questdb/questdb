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

package io.questdb.griffin.engine.functions.bool;

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
import io.questdb.griffin.model.IntervalUtils;
import io.questdb.std.IntList;
import io.questdb.std.Interval;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;

public class InTimestampIntervalFunctionFactory implements FunctionFactory {

    @Override
    public String getSignature() {
        return "in(NΔ)";
    }

    @Override
    public Function newInstance(
            int position,
            ObjList<Function> args,
            IntList argPositions,
            CairoConfiguration configuration,
            SqlExecutionContext sqlExecutionContext
    ) {
        return new Func(args.getQuick(0), args.getQuick(1));
    }

    public static class Func extends NegatableBooleanFunction implements BinaryFunction {
        private final Function left;
        private final int leftTimestampType;
        private final Function right;
        private final TimestampDriver timestampDriver;

        public Func(Function left, Function right) {
            this.left = left;
            this.right = right;
            leftTimestampType = ColumnType.getHigherPrecisionTimestampType(ColumnType.getTimestampType(left.getType()), ColumnType.TIMESTAMP_MICRO);
            timestampDriver = ColumnType.getTimestampDriver(leftTimestampType);
        }

        @Override
        public boolean getBool(Record rec) {
            final long ts = left.getTimestamp(rec);
            if (ts == Numbers.LONG_NULL) {
                return negated;
            }
            final Interval interval = right.getInterval(rec);
            if (Interval.NULL.equals(interval)) {
                return negated;
            }

            return negated != timestampDriver.inInterval(ts, right.getType(), interval);
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
        public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
            getLeft().init(symbolTableSource, executionContext);
            int oldIntervalType = executionContext.getIntervalFunctionType();
            try {
                executionContext.setIntervalFunctionType(IntervalUtils.getIntervalType(leftTimestampType));
                getRight().init(symbolTableSource, executionContext);
            } finally {
                executionContext.setIntervalFunctionType(oldIntervalType);
            }
        }

        @Override
        public boolean isThreadSafe() {
            return false;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(left);
            if (negated) {
                sink.val(" not");
            }
            sink.val(" in ").val(right);
        }
    }
}
