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
import io.questdb.cairo.CairoException;
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
import io.questdb.griffin.engine.functions.MultiArgFunction;
import io.questdb.griffin.engine.functions.NegatableBooleanFunction;
import io.questdb.griffin.engine.functions.UnaryFunction;
import io.questdb.std.IntList;
import io.questdb.std.LongList;
import io.questdb.std.Numbers;
import io.questdb.std.NumericException;
import io.questdb.std.ObjList;
import io.questdb.std.Vect;
import io.questdb.std.str.Utf8Sequence;

import static io.questdb.griffin.model.IntervalUtils.isInIntervals;
import static io.questdb.griffin.model.IntervalUtils.parseAndApplyInterval;

public class InTimestampTimestampFunctionFactory implements FunctionFactory {

    @Override
    public String getSignature() {
        return "in(NV)";
    }

    @Override
    public Function newInstance(
            int position,
            ObjList<Function> args,
            IntList argPositions,
            CairoConfiguration configuration,
            SqlExecutionContext sqlExecutionContext) throws SqlException {
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
                case ColumnType.INTERVAL:
                    return new InTimestampIntervalFunctionFactory.Func(args.getQuick(0), args.getQuick(1));
                default:
                    throw SqlException.position(argPositions.getQuick(i))
                            .put("cannot compare TIMESTAMP with type ")
                            .put(ColumnType.nameOf(func.getType()));
            }
            if (!func.isConstant()) {
                allConst = false;

                // allRuntimeConst can mean a mix of constants and runtime constants
                if (!func.isRuntimeConstant()) {
                    allRuntimeConst = false;
                }
            }
        }

        boolean intervalSearch = isIntervalSearch(args);
        int timestampType = ColumnType.getTimestampType(args.getQuick(0).getType());
        assert ColumnType.isTimestamp(timestampType);
        if (allConst) {
            if (intervalSearch) {
                Function rightFn = args.getQuick(1);
                CharSequence right = rightFn.getStrA(null);
                return new EqTimestampStrConstantFunction(args.getQuick(0), timestampType, right, argPositions.getQuick(1));
            }
            return new InTimestampConstFunction(args.getQuick(0), parseDiscreteTimestampValues(timestampType, args, argPositions));
        }

        if (allRuntimeConst) {
            if (intervalSearch) {
                return new InTimestampRuntimeConstIntervalFunction(
                        args.getQuick(0),
                        args.getQuick(1),
                        timestampType,
                        argPositions.getQuick(1)
                );

            }
            return new InTimestampManyRuntimeConstantsFunction(new ObjList<>(args), timestampType);
        }

        if (intervalSearch) {
            return new EqTimestampStrFunction(args.get(0), args.get(1), timestampType);
        }

        // have to copy, args is mutable
        return new InTimestampVarFunction(new ObjList<>(args), timestampType);
    }

    private static boolean isIntervalSearch(ObjList<Function> args) {
        if (args.size() != 2) {
            return false;
        }
        Function rightFn = args.getQuick(1);
        return ColumnType.isVarcharOrString(rightFn.getType());
    }

    private static LongList parseDiscreteTimestampValues(int timestampType, ObjList<Function> args, IntList argPositions)
            throws SqlException {
        TimestampDriver driver = ColumnType.getTimestampDriver(timestampType);
        LongList res = new LongList(args.size() - 1);
        res.extendAndSet(args.size() - 2, 0);

        for (int i = 1, n = args.size(); i < n; i++) {
            Function func = args.getQuick(i);
            long val;
            int funcType = func.getType();
            switch (ColumnType.tagOf(funcType)) {
                case ColumnType.DATE:
                    val = driver.fromDate(func.getDate(null));
                    break;
                case ColumnType.TIMESTAMP:
                    val = driver.from(func.getTimestamp(null), funcType);
                    break;
                case ColumnType.LONG:
                case ColumnType.INT:
                    val = func.getTimestamp(null);
                    break;
                case ColumnType.STRING:
                case ColumnType.SYMBOL:
                case ColumnType.NULL:
                    val = parseFloorOrDie(driver, func.getStrA(null), argPositions.getQuick(i));
                    break;
                case ColumnType.VARCHAR:
                    val = parseFloorOrDie(driver, func.getVarcharA(null), argPositions.getQuick(i));
                    break;
                default:
                    throw SqlException.inconvertibleTypes(argPositions.getQuick(i), func.getType(),
                            ColumnType.nameOf(func.getType()), timestampType,
                            ColumnType.nameOf(timestampType));
            }
            res.setQuick(i - 1, val);
        }

        res.sort();
        return res;
    }

    private static long parseFloorOrDie(TimestampDriver driver, CharSequence value) {
        try {
            return driver.parseFloorLiteral(value);
        } catch (NumericException e) {
            throw CairoException.nonCritical().put("Invalid timestamp: ").put(value);
        }
    }

    private static long parseFloorOrDie(TimestampDriver driver, Utf8Sequence value) {
        try {
            return driver.parseFloorLiteral(value);
        } catch (NumericException e) {
            throw CairoException.nonCritical().put("Invalid timestamp: ").put(value);
        }
    }

    private static long parseFloorOrDie(TimestampDriver driver, CharSequence seq, int position) throws SqlException {
        try {
            return driver.parseFloorLiteral(seq);
        } catch (NumericException e) {
            throw SqlException.invalidDate(seq, position);
        }
    }

    private static long parseFloorOrDie(TimestampDriver driver, Utf8Sequence seq, int position) throws SqlException {
        try {
            return driver.parseFloorLiteral(seq);
        } catch (NumericException e) {
            throw SqlException.invalidDate(seq, position);
        }
    }

    private static class EqTimestampStrConstantFunction extends NegatableBooleanFunction implements UnaryFunction {
        private final LongList intervals = new LongList();
        private final Function left;

        public EqTimestampStrConstantFunction(
                Function left,
                int leftTimestampType,
                CharSequence right,
                int rightPosition
        ) throws SqlException {
            this.left = left;
            parseAndApplyInterval(ColumnType.getTimestampDriver(leftTimestampType), right, intervals, rightPosition);
        }

        @Override
        public Function getArg() {
            return left;
        }

        @Override
        public boolean getBool(Record rec) {
            return negated != isInIntervals(intervals, left.getTimestamp(rec));
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(left);
            if (negated) {
                sink.val(" not");
            }
            sink.val(" in ").val(intervals);
        }
    }

    private static class EqTimestampStrFunction extends NegatableBooleanFunction implements BinaryFunction {
        private final LongList intervals = new LongList();
        private final Function left;
        private final Function right;
        private final TimestampDriver timestampDriver;

        public EqTimestampStrFunction(Function left, Function right, int timestampType) {
            this.left = left;
            this.right = right;
            this.timestampDriver = ColumnType.getTimestampDriver(timestampType);
        }

        @Override
        public boolean getBool(Record rec) {
            long ts = left.getTimestamp(rec);
            if (ts == Numbers.LONG_NULL) {
                return negated;
            }
            CharSequence timestampAsString = right.getStrA(rec);
            if (timestampAsString == null) {
                return negated;
            }
            intervals.clear();
            try {
                // we are ignoring exception contents here, so we do not need the exact position
                parseAndApplyInterval(timestampDriver, timestampAsString, intervals, 0);
            } catch (SqlException e) {
                return negated;
            }
            return negated != isInIntervals(intervals, ts);
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
            return negated != inList.binarySearch(ts, Vect.BIN_SEARCH_SCAN_UP) >= 0;
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

    private static class InTimestampManyRuntimeConstantsFunction extends NegatableBooleanFunction
            implements MultiArgFunction {
        private final ObjList<Function> args;
        private final TimestampDriver driver;
        private final LongList timestampValues;

        public InTimestampManyRuntimeConstantsFunction(ObjList<Function> args, int timestampType) {
            this.args = args;
            this.timestampValues = new LongList(args.size());
            this.driver = ColumnType.getTimestampDriver(timestampType);
        }

        @Override
        public ObjList<Function> args() {
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
        public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext)
                throws SqlException {
            MultiArgFunction.super.init(symbolTableSource, executionContext);
            timestampValues.clear();
            for (int i = 1, n = args.size(); i < n; i++) {
                Function func = args.getQuick(i);
                long val = Numbers.LONG_NULL;
                int funcType = func.getType();
                switch (ColumnType.tagOf(funcType)) {
                    case ColumnType.DATE:
                        val = driver.fromDate(func.getDate(null));
                        break;
                    case ColumnType.TIMESTAMP:
                        val = driver.from(func.getTimestamp(null), funcType);
                        break;
                    case ColumnType.LONG:
                    case ColumnType.INT:
                        val = func.getTimestamp(null);
                        break;
                    case ColumnType.STRING:
                    case ColumnType.SYMBOL:
                        val = parseFloorOrDie(driver, func.getStrA(null));
                        break;
                    case ColumnType.VARCHAR:
                        val = parseFloorOrDie(driver, func.getVarcharA(null));
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

    private static class InTimestampRuntimeConstIntervalFunction extends NegatableBooleanFunction
            implements BinaryFunction {
        private final TimestampDriver driver;
        private final Function intervalFunc;
        private final int intervalFuncPos;
        private final LongList intervals = new LongList();
        private final Function left;

        public InTimestampRuntimeConstIntervalFunction(Function left, Function intervalFunc, int timestampType, int intervalFuncPos) {
            this.left = left;
            this.intervalFunc = intervalFunc;
            this.intervalFuncPos = intervalFuncPos;
            this.driver = ColumnType.getTimestampDriver(timestampType);
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
        public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext)
                throws SqlException {
            BinaryFunction.super.init(symbolTableSource, executionContext);
            intervals.clear();
            // This is a specific function, which accepts "in interval" as bind variable.
            // For this reason, only STRING and VARCHAR bind variables are supported. Other
            // types,
            // such as INT, LONG etc. will require two or move values to represent the
            // interval
            switch (intervalFunc.getType()) {
                case ColumnType.STRING:
                case ColumnType.VARCHAR:
                    parseAndApplyInterval(driver, intervalFunc.getStrA(null), intervals, 0);
                    break;
                default:
                    throw SqlException
                            .$(intervalFuncPos, "unsupported bind variable type [")
                            .put(ColumnType.nameOf(intervalFunc.getType()))
                            .put("] expected one of [STRING or VARCHAR]");
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
            sink.val(" in ").val(intervalFunc);
        }
    }

    private static class InTimestampVarFunction extends NegatableBooleanFunction implements MultiArgFunction {
        private final ObjList<Function> args;
        private final TimestampDriver driver;

        public InTimestampVarFunction(ObjList<Function> args, int timestampType) {
            this.args = args;
            this.driver = ColumnType.getTimestampDriver(timestampType);
        }

        @Override
        public ObjList<Function> args() {
            return args;
        }

        @Override
        public boolean getBool(Record rec) {
            long ts = args.getQuick(0).getTimestamp(rec);

            for (int i = 1, n = args.size(); i < n; i++) {
                Function func = args.getQuick(i);
                long val = Numbers.LONG_NULL;
                int funcType = func.getType();
                switch (ColumnType.tagOf(funcType)) {
                    case ColumnType.DATE:
                        val = driver.fromDate(func.getDate(rec));
                        break;
                    case ColumnType.TIMESTAMP:
                        val = driver.from(func.getTimestamp(rec), funcType);
                        break;
                    case ColumnType.LONG:
                    case ColumnType.INT:
                        val = func.getTimestamp(rec);
                        break;
                    case ColumnType.STRING:
                    case ColumnType.SYMBOL:
                        val = parseFloorOrDie(driver, func.getStrA(rec));
                        break;
                    case ColumnType.VARCHAR:
                        val = parseFloorOrDie(driver, func.getVarcharA(rec));
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
