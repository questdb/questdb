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

import io.questdb.cairo.BinarySearch;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.BinaryFunction;
import io.questdb.griffin.engine.functions.MultiArgFunction;
import io.questdb.griffin.engine.functions.NegatableBooleanFunction;
import io.questdb.griffin.engine.functions.UnaryFunction;
import io.questdb.griffin.model.IntervalUtils;
import io.questdb.std.*;

import static io.questdb.griffin.model.IntervalUtils.isInIntervals;
import static io.questdb.griffin.model.IntervalUtils.parseAndApplyIntervalEx;

public class InTimestampTimestampFunctionFactory implements FunctionFactory {
    @Override
    public String getSignature() {
        return "in(NV)";
    }

    @Override
    public Function newInstance(int position, ObjList<Function> args, IntList argPositions, CairoConfiguration configuration, SqlExecutionContext sqlExecutionContext) throws SqlException {
        boolean allConst = true;
        boolean allRuntimeConst = true;
        for (int i = 1, n = args.size(); i < n && (allConst || allRuntimeConst); i++) {
            Function func = args.getQuick(i);
            switch (ColumnType.tagOf(func.getType())) {
                case ColumnType.NULL:
                case ColumnType.DATE:
                case ColumnType.TIMESTAMP:
                case ColumnType.LONG:
                case ColumnType.INT:
                case ColumnType.STRING:
                case ColumnType.SYMBOL:
                case ColumnType.VARCHAR:
                case ColumnType.UNDEFINED:
                    break;
                default:
                    throw SqlException.position(argPositions.getQuick(i)).put("cannot compare TIMESTAMP with type ").put(ColumnType.nameOf(func.getType()));
            }
            if (!func.isConstant()) {
                allConst = false;

                // allRuntimeConst can mean a mix of constants and runtime constants
                if (!func.isRuntimeConstant()) {
                    allRuntimeConst = false;
                }
            }
        }

        if (allConst) {
            return new InTimestampConstFunction(args.getQuick(0), parseDiscreteTimestampValues(args, argPositions));
        }

        if (args.size() == 2 && (ColumnType.isString(args.get(1).getType()) || ColumnType.isVarchar(args.get(1).getType()))) {
            // special case - one argument and it a string
            return new InTimestampStrFunctionFactory.EqTimestampStrFunction(args.get(0), args.get(1));
        }

        if (allRuntimeConst) {
            if (args.size() == 2 && args.get(1).getType() == ColumnType.UNDEFINED) {
                // this is an odd case, we have something like this
                //
                // where ts in ?
                //
                // Type of the runtime constant may not be known upfront.
                // When user passes string as the value we perform the interval lookup,
                // otherwise it is discrete value
                return new InTimestampRuntimeConstIntervalFunction(args.getQuick(0), args.getQuick(1), argPositions.getQuick(1));

            }
            return new InTimestampManyRuntimeConstantsFunction(new ObjList<>(args));
        }
        // have to copy, args is mutable
        return new InTimestampVarFunction(new ObjList<>(args));
    }

    private static LongList parseDiscreteTimestampValues(ObjList<Function> args, IntList argPositions) throws SqlException {
        LongList res = new LongList(args.size() - 1);
        res.extendAndSet(args.size() - 2, 0);

        for (int i = 1, n = args.size(); i < n; i++) {
            Function func = args.getQuick(i);
            long val;
            switch (ColumnType.tagOf(func.getType())) {
                case ColumnType.DATE:
                    val = func.getDate(null);
                    val = (val == Numbers.LONG_NULL) ? val : val * 1000L;
                    break;
                case ColumnType.TIMESTAMP:
                case ColumnType.LONG:
                case ColumnType.INT:
                    val = func.getTimestamp(null);
                    break;
                case ColumnType.STRING:
                case ColumnType.SYMBOL:
                case ColumnType.VARCHAR:
                case ColumnType.NULL:
                    CharSequence tsValue = func.getStrA(null);
                    val = (tsValue != null) ? tryParseTimestamp(tsValue, argPositions.getQuick(i)) : Numbers.LONG_NULL;
                    break;
                default:
                    throw SqlException.inconvertibleTypes(argPositions.getQuick(i), func.getType(), ColumnType.nameOf(func.getType()), ColumnType.TIMESTAMP, ColumnType.nameOf(ColumnType.TIMESTAMP));
            }
            res.setQuick(i - 1, val);
        }

        res.sort();
        return res;
    }

    private static long tryParseTimestamp(CharSequence seq, int position) throws SqlException {
        try {
            return IntervalUtils.parseFloorPartialTimestamp(seq);
        } catch (NumericException e) {
            throw SqlException.invalidDate(seq, position);
        }
    }

    private static class InTimestampConstFunction extends NegatableBooleanFunction implements UnaryFunction {
        private final LongList inList;
        private final Function tsFunc;

        public InTimestampConstFunction(Function tsFunc, LongList longList) {
            this.tsFunc = tsFunc;
            this.inList = longList;
        }

        @Override
        public Function getArg() {
            return tsFunc;
        }

        @Override
        public boolean getBool(Record rec) {
            long ts = tsFunc.getTimestamp(rec);
            return negated != inList.binarySearch(ts, BinarySearch.SCAN_UP) >= 0;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(tsFunc);
            if (negated) {
                sink.val(" not");
            }
            sink.val(" in ").val(inList);
        }
    }

    private static class InTimestampManyRuntimeConstantsFunction extends NegatableBooleanFunction implements MultiArgFunction {
        private final ObjList<Function> args;
        private final LongList timestampValues;

        public InTimestampManyRuntimeConstantsFunction(ObjList<Function> args) {
            this.args = args;
            this.timestampValues = new LongList(args.size());
        }

        @Override
        public ObjList<Function> getArgs() {
            return args;
        }

        @Override
        public boolean getBool(Record rec) {
            long ts = args.getQuick(0).getTimestamp(rec);
            for (int i = 0, n = timestampValues.size(); i < n; i++) {
                long val = timestampValues.getQuick(i);
                if (val == ts) {
                    return !negated;
                }
            }
            return negated;
        }

        @Override
        public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
            MultiArgFunction.super.init(symbolTableSource, executionContext);
            timestampValues.clear();
            for (int i = 1, n = args.size(); i < n; i++) {
                Function func = args.getQuick(i);
                long val = Numbers.LONG_NULL;
                switch (ColumnType.tagOf(func.getType())) {
                    case ColumnType.TIMESTAMP:
                    case ColumnType.LONG:
                    case ColumnType.INT:
                        val = func.getTimestamp(null);
                        break;
                    case ColumnType.STRING:
                    case ColumnType.SYMBOL:
                    case ColumnType.VARCHAR:
                        CharSequence str = func.getStrA(null);
                        val = str != null ? IntervalUtils.tryParseTimestamp(str) : Numbers.LONG_NULL;
                        break;
                }
                timestampValues.add(val);
            }
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(args.getQuick(0));
            if (negated) {
                sink.val(" not");
            }
            sink.val(" in ");
            sink.val(args, 1);
        }
    }

    public static class InTimestampRuntimeConstIntervalFunction extends NegatableBooleanFunction implements BinaryFunction {
        private final Function intervalFunc;
        private final int intervalFuncPos;
        private final LongList intervals = new LongList();
        private final Function left;

        public InTimestampRuntimeConstIntervalFunction(Function left, Function intervalFunc, int intervalFuncPos) {
            this.left = left;
            this.intervalFunc = intervalFunc;
            this.intervalFuncPos = intervalFuncPos;
        }

        @Override
        public boolean getBool(Record rec) {
            final long ts = left.getTimestamp(rec);
            return ts == Numbers.LONG_NULL ? negated : negated != isInIntervals(intervals, ts);
        }

        @Override
        public Function getLeft() {
            return left;
        }

        @Override
        public Function getRight() {
            return intervalFunc;
        }

        @Override
        public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
            BinaryFunction.super.init(symbolTableSource, executionContext);
            intervals.clear();
            // This is a specific function, which accepts "in interval" as bind variable.
            // For this reason only STRING and VARCHAR bind variables are supported. Other types,
            // such as INT, LONG etc will require two or move values to represent the interval
            switch (intervalFunc.getType()) {
                case ColumnType.STRING:
                case ColumnType.VARCHAR:
                    parseAndApplyIntervalEx(intervalFunc.getStrA(null), intervals, 0);
                    break;
                default:
                    throw SqlException
                            .$(intervalFuncPos, "unsupported bind variable type [").put(ColumnType.nameOf(intervalFunc.getType()))
                            .put("] expected one of [STRING or VARCHAR]");
            }
        }

        @Override
        public boolean isReadThreadSafe() {
            return false;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(left);
            if (negated) {
                sink.val(" not");
            }
            sink.val(" in ").val(intervalFunc);
        }
    }

    private static class InTimestampVarFunction extends NegatableBooleanFunction implements MultiArgFunction {
        private final ObjList<Function> args;

        public InTimestampVarFunction(ObjList<Function> args) {
            this.args = args;
        }

        @Override
        public ObjList<Function> getArgs() {
            return args;
        }

        @Override
        public boolean getBool(Record rec) {
            long ts = args.getQuick(0).getTimestamp(rec);

            for (int i = 1, n = args.size(); i < n; i++) {
                Function func = args.getQuick(i);
                long val = Numbers.LONG_NULL;
                switch (ColumnType.tagOf(func.getType())) {
                    case ColumnType.TIMESTAMP:
                    case ColumnType.LONG:
                    case ColumnType.INT:
                        val = func.getTimestamp(rec);
                        break;
                    case ColumnType.STRING:
                    case ColumnType.SYMBOL:
                    case ColumnType.VARCHAR:
                        CharSequence str = func.getStrA(rec);
                        val = str != null ? IntervalUtils.tryParseTimestamp(str) : Numbers.LONG_NULL;
                        break;
                }
                if (val == ts) {
                    return !negated;
                }
            }
            return negated;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(args.getQuick(0));
            if (negated) {
                sink.val(" not");
            }
            sink.val(" in ");
            sink.val(args, 1);
        }
    }
}
