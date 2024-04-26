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
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.BooleanFunction;
import io.questdb.griffin.engine.functions.TernaryFunction;
import io.questdb.griffin.engine.functions.UnaryFunction;
import io.questdb.griffin.engine.functions.constants.BooleanConstant;
import io.questdb.std.IntList;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;

public class BetweenTimestampFunctionFactory implements FunctionFactory {
    @Override
    public String getSignature() {
        return "between(NNN)";
    }

    @Override
    public Function newInstance(
            int position,
            ObjList<Function> args,
            IntList argPositions,
            CairoConfiguration configuration,
            SqlExecutionContext sqlExecutionContext
    ) {
        Function arg = args.getQuick(0);
        Function fromFn = args.getQuick(1);
        Function toFn = args.getQuick(2);

        if (fromFn.isConstant() && toFn.isConstant()) {
            long fromFnTimestamp = fromFn.getTimestamp(null);
            long toFnTimestamp = toFn.getTimestamp(null);

            if (fromFnTimestamp == Numbers.LONG_NULL || toFnTimestamp == Numbers.LONG_NULL) {
                return BooleanConstant.FALSE;
            }
            return new ConstFunc(arg, fromFnTimestamp, toFnTimestamp);
        }
        return new VarBetweenFunction(arg, fromFn, toFn);
    }

    private static class ConstFunc extends BooleanFunction implements UnaryFunction {
        private final long from;
        private final Function left;
        private final long to;

        public ConstFunc(Function left, long from, long to) {
            this.left = left;
            this.from = Math.min(from, to);
            this.to = Math.max(from, to);
        }

        @Override
        public Function getArg() {
            return left;
        }

        @Override
        public boolean getBool(Record rec) {
            long timestamp = left.getTimestamp(rec);
            if (timestamp == Numbers.LONG_NULL) return false;

            return from <= timestamp && timestamp <= to;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(left).val(" between ").val(from).val(" and ").val(to);
        }
    }

    private static class VarBetweenFunction extends BooleanFunction implements TernaryFunction {
        private final Function arg;
        private final Function from;
        private final Function to;

        public VarBetweenFunction(Function left, Function from, Function to) {
            this.arg = left;
            this.from = from;
            this.to = to;
        }

        @Override
        public boolean getBool(Record rec) {
            long value = arg.getTimestamp(rec);
            if (value == Numbers.LONG_NULL) {
                return false;
            }

            long fromTs = from.getTimestamp(rec);
            if (fromTs == Numbers.LONG_NULL) {
                return false;
            }

            long toTs = to.getTimestamp(rec);
            if (toTs == Numbers.LONG_NULL) {
                return false;
            }

            return Math.min(fromTs, toTs) <= value && value <= Math.max(fromTs, toTs);
        }

        @Override
        public Function getCenter() {
            return arg;
        }

        @Override
        public Function getLeft() {
            return from;
        }

        @Override
        public Function getRight() {
            return to;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(arg).val(" between ").val(from).val(" and ").val(to);
        }
    }
}
