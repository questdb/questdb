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
import io.questdb.griffin.model.CompiledTickExpression;
import io.questdb.griffin.model.DateExpressionEvaluator;
import io.questdb.griffin.model.IntervalUtils;
import io.questdb.std.FiberLocal;
import io.questdb.std.IntList;
import io.questdb.std.LongList;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.NumericException;
import io.questdb.std.ObjList;
import io.questdb.std.Vect;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf8Sequence;

import static io.questdb.griffin.model.IntervalUtils.isInIntervals;

public class InTimestampTimestampFunctionFactory implements FunctionFactory {
    private static final int LIST_CONSTANT = 0;
    private static final int LIST_INTERVAL = 1;
    private static final int LIST_RUNTIME_CONSTANT = 2;
    private static final int LIST_VARIABLE = 3;
    private static final FiberLocal<StringSink> VETTED_SINK = new FiberLocal<>(StringSink::new);
    private static final FiberLocal<LongList> VETTED_VALUES = new FiberLocal<>(LongList::new);

    @Override
    public int getResultType(IntList argTypes) {
        return ColumnType.BOOLEAN;
    }

    @Override
    public String getSignature() {
        return "in(NV)";
    }

    /**
     * Keeps a tick expression with date variables built: vetting it would compile it once more.
     */
    @Override
    public boolean isConstructionDeferrable(int position, ObjList<Function> args, IntList argPositions, CairoConfiguration configuration) throws SqlException {
        final int listKind = listKind(args, argPositions);
        if (listKind != LIST_CONSTANT) {
            return listKind != LIST_INTERVAL;
        }
        final int timestampType = ColumnType.getTimestampType(args.getQuick(0).getType());
        final LongList values = VETTED_VALUES.get();
        values.clear();
        if (!isIntervalSearch(args)) {
            parseDiscreteTimestampValues(timestampType, args, argPositions, values);
            return true;
        }
        final CharSequence right = args.getQuick(1).getStrA(null);
        if (right != null && containsDateVariable(right)) {
            return false;
        }
        final StringSink sink = VETTED_SINK.get();
        sink.clear();
        IntervalUtils.parseTickExprAndIntersect(ColumnType.getTimestampDriver(timestampType), configuration, right, values, argPositions.getQuick(1), sink, true);
        return true;
    }

    @Override
    public Function newInstance(
            int position,
            ObjList<Function> args,
            IntList argPositions,
            CairoConfiguration configuration,
            SqlExecutionContext sqlExecutionContext) throws SqlException {
        final int listKind = listKind(args, argPositions);
        if (listKind == LIST_INTERVAL) {
            for (int i = 2, n = args.size(); i < n; i++) {
                args.setQuick(i, Misc.free(args.getQuick(i)));
            }
            return new InTimestampIntervalFunctionFactory.Func(args.getQuick(0), args.getQuick(1));
        }

        boolean intervalSearch = isIntervalSearch(args);
        int timestampType = ColumnType.getTimestampType(args.getQuick(0).getType());
        assert ColumnType.isTimestamp(timestampType);
        if (listKind == LIST_CONSTANT) {
            if (intervalSearch) {
                Function rightFn = args.getQuick(1);
                CharSequence right = rightFn.getStrA(null);
                if (right != null && containsDateVariable(right)) {
                    TimestampDriver driver = ColumnType.getTimestampDriver(timestampType);
                    CompiledTickExpression compiled = IntervalUtils.compileTickExpr(
                            driver, configuration, right, 0, right.length(), argPositions.getQuick(1));
                    return new EqTimestampCompiledTickExprFunction(args.getQuick(0), compiled);
                }
                return new EqTimestampStrConstantFunction(args.getQuick(0), timestampType, right, argPositions.getQuick(1), configuration);
            }
            final LongList values = new LongList(args.size() - 1);
            parseDiscreteTimestampValues(timestampType, args, argPositions, values);
            return new InTimestampConstFunction(args.getQuick(0), values);
        }

        if (listKind == LIST_RUNTIME_CONSTANT) {
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
            return new EqTimestampStrFunction(args.get(0), args.get(1), timestampType, configuration);
        }

        // have to copy, args is mutable
        return new InTimestampVarFunction(new ObjList<>(args), timestampType);
    }

    /**
     * Adds the element's value at the column's precision; an element no column value equals adds nothing.
     */
    private static void addElement(LongList values, Function func, Record rec, TimestampDriver driver) throws NumericException {
        final int type = elementType(func, rec, driver);
        final long value = elementValue(func, rec, type, driver);
        final long ceil = driver.ceilFrom(value, type);
        if (ceil == driver.floorFrom(value, type)) {
            values.add(ceil);
        }
    }

    private static boolean containsDateVariable(CharSequence seq) {
        int lim = seq.length();
        for (int i = 0; i < lim - 1; i++) {
            if (seq.charAt(i) == '$' && DateExpressionEvaluator.isDateVariable(seq, i, lim)) {
                return true;
            }
        }
        return false;
    }

    /**
     * The timestamp type at which an IN element is exact: its own timestamp type, the finer of a
     * literal's precision and the column's, or the column's for any other value.
     */
    private static int elementType(Function func, Record rec, TimestampDriver driver) {
        return switch (ColumnType.tagOf(func.getType())) {
            case ColumnType.TIMESTAMP -> func.getType();
            case ColumnType.STRING, ColumnType.SYMBOL -> {
                final CharSequence value = func.getStrA(rec);
                yield value == null ? driver.getTimestampType() : IntervalUtils.literalTimestampType(driver, value);
            }
            case ColumnType.VARCHAR -> {
                final Utf8Sequence value = func.getVarcharA(rec);
                yield value == null ? driver.getTimestampType() : IntervalUtils.literalTimestampType(driver, value.asAsciiCharSequence());
            }
            default -> driver.getTimestampType();
        };
    }

    private static long elementValue(Function func, Record rec, int type, TimestampDriver driver) throws NumericException {
        return switch (ColumnType.tagOf(func.getType())) {
            case ColumnType.DATE -> driver.fromDate(func.getDate(rec));
            case ColumnType.TIMESTAMP, ColumnType.LONG, ColumnType.INT -> func.getTimestamp(rec);
            case ColumnType.STRING, ColumnType.SYMBOL ->
                    ColumnType.getTimestampDriver(type).parseFloorLiteral(func.getStrA(rec));
            case ColumnType.VARCHAR -> ColumnType.getTimestampDriver(type).parseFloorLiteral(func.getVarcharA(rec));
            default -> Numbers.LONG_NULL;
        };
    }

    private static boolean isIntervalSearch(ObjList<Function> args) {
        if (args.size() != 2) {
            return false;
        }
        Function rightFn = args.getQuick(1);
        return ColumnType.isVarcharOrString(rightFn.getType());
    }

    /**
     * The kind of IN list the call builds over: an INTERVAL element, constants, constants mixed with runtime
     * constants, or anything else. Raises the error for an element that does not compare with TIMESTAMP, up to the
     * first element that decides the kind.
     */
    private static int listKind(ObjList<Function> args, IntList argPositions) throws SqlException {
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
                    return LIST_INTERVAL;
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
        return allConst ? LIST_CONSTANT : allRuntimeConst ? LIST_RUNTIME_CONSTANT : LIST_VARIABLE;
    }

    private static void parseDiscreteTimestampValues(int timestampType, ObjList<Function> args, IntList argPositions, LongList res)
            throws SqlException {
        final TimestampDriver driver = ColumnType.getTimestampDriver(timestampType);
        for (int i = 1, n = args.size(); i < n; i++) {
            final Function func = args.getQuick(i);
            switch (ColumnType.tagOf(func.getType())) {
                case ColumnType.DATE, ColumnType.TIMESTAMP, ColumnType.LONG, ColumnType.INT, ColumnType.STRING,
                     ColumnType.SYMBOL, ColumnType.NULL, ColumnType.VARCHAR -> {
                }
                default -> throw SqlException.inconvertibleTypes(argPositions.getQuick(i), func.getType(),
                        ColumnType.nameOf(func.getType()), timestampType,
                        ColumnType.nameOf(timestampType));
            }
            try {
                addElement(res, func, null, driver);
            } catch (NumericException e) {
                throw SqlException.invalidDate(func.getStrA(null), argPositions.getQuick(i));
            }
        }
        res.sort();
    }

    private static class EqTimestampCompiledTickExprFunction extends NegatableBooleanFunction implements UnaryFunction {
        private final CompiledTickExpression compiledExpr;
        private final LongList intervals = new LongList();
        private final Function left;

        public EqTimestampCompiledTickExprFunction(Function left, CompiledTickExpression compiledExpr) {
            this.left = left;
            this.compiledExpr = compiledExpr;
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
        public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
            UnaryFunction.super.init(symbolTableSource, executionContext);
            intervals.clear();
            compiledExpr.init(symbolTableSource, executionContext);
            compiledExpr.evaluate(intervals);
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
            sink.val(" in ").val(intervals);
        }
    }

    private static class EqTimestampStrConstantFunction extends NegatableBooleanFunction implements UnaryFunction {
        private final LongList intervals = new LongList();
        private final Function left;

        public EqTimestampStrConstantFunction(
                Function left,
                int leftTimestampType,
                CharSequence right,
                int rightPosition,
                CairoConfiguration configuration
        ) throws SqlException {
            this.left = left;
            final StringSink sink = new StringSink();
            IntervalUtils.parseTickExprAndIntersect(ColumnType.getTimestampDriver(leftTimestampType), configuration, right, intervals, rightPosition, sink, true);
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
        private final CairoConfiguration configuration;
        private final LongList intervals = new LongList();
        private final Function left;
        private final Function right;
        private final StringSink sink = new StringSink();
        private final TimestampDriver timestampDriver;

        public EqTimestampStrFunction(Function left, Function right, int timestampType, CairoConfiguration configuration) {
            this.left = left;
            this.right = right;
            this.timestampDriver = ColumnType.getTimestampDriver(timestampType);
            this.configuration = configuration;
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
                IntervalUtils.parseTickExprAndIntersect(timestampDriver, configuration, timestampAsString, intervals, 0, sink, true);
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
                final Function func = args.getQuick(i);
                try {
                    addElement(timestampValues, func, null, driver);
                } catch (NumericException e) {
                    throw CairoException.nonCritical().put("Invalid timestamp: ").put(func.getStrA(null));
                }
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
        private final StringSink sink = new StringSink();

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
                    IntervalUtils.parseTickExprAndIntersect(driver, executionContext.getCairoEngine().getConfiguration(), intervalFunc.getStrA(null), intervals, 0, sink, true);
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
            final long ts = args.getQuick(0).getTimestamp(rec);
            for (int i = 1, n = args.size(); i < n; i++) {
                final Function func = args.getQuick(i);
                final int type = elementType(func, rec, driver);
                final long value;
                try {
                    value = elementValue(func, rec, type, driver);
                } catch (NumericException e) {
                    throw CairoException.nonCritical().put("Invalid timestamp: ").put(func.getStrA(rec));
                }
                if (driver.ceilFrom(value, type) == ts && driver.floorFrom(value, type) == ts) {
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
