/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.MultiArgFunction;
import io.questdb.griffin.engine.functions.NegatableBooleanFunction;
import io.questdb.griffin.engine.functions.UnaryFunction;
import io.questdb.std.*;

public class InLongFunctionFactory implements FunctionFactory {

    public static long tryParseLong(CharSequence seq, int position) throws SqlException {
        try {
            return Numbers.parseLong(seq, 0, seq.length());
        } catch (NumericException e) {
            throw SqlException.position(position).put("invalid LONG value");
        }
    }

    @Override
    public String getSignature() {
        return "in(LV)";
    }

    @Override
    public Function newInstance(int position, ObjList<Function> args, IntList argPositions, CairoConfiguration configuration, SqlExecutionContext sqlExecutionContext) throws SqlException {
        boolean allConst = true;
        for (int i = 1, n = args.size(); i < n; i++) {
            Function func = args.getQuick(i);
            switch (ColumnType.tagOf(func.getType())) {
                case ColumnType.NULL:
                case ColumnType.TIMESTAMP:
                case ColumnType.LONG:
                case ColumnType.INT:
                case ColumnType.SHORT:
                case ColumnType.BYTE:
                case ColumnType.STRING:
                case ColumnType.SYMBOL:
                    break;
                default:
                    throw SqlException.position(argPositions.get(i)).put("cannot compare LONG with type ").put(ColumnType.nameOf(func.getType()));
            }
            if (!func.isConstant()) {
                allConst = false;
                break;
            }
        }

        if (allConst) {
            return new InLongConstFunction(args.getQuick(0), parseToLong(args, argPositions));
        }

        // have to copy, args is mutable
        return new InLongVarFunction(new ObjList<>(args));
    }

    private LongList parseToLong(ObjList<Function> args, IntList argPositions) throws SqlException {
        LongList res = new LongList(args.size() - 1);
        res.extendAndSet(args.size() - 2, 0);

        for (int i = 1, n = args.size(); i < n; i++) {
            Function func = args.getQuick(i);
            long val = Numbers.LONG_NaN;
            switch (ColumnType.tagOf(func.getType())) {
                case ColumnType.TIMESTAMP:
                case ColumnType.LONG:
                case ColumnType.INT:
                case ColumnType.SHORT:
                case ColumnType.BYTE:
                    val = func.getLong(null);
                    break;
                case ColumnType.STRING:
                case ColumnType.SYMBOL:
                    CharSequence tsValue = func.getStr(null);
                    val = (tsValue != null) ? tryParseLong(tsValue, argPositions.getQuick(i)) : Numbers.LONG_NaN;
                    break;
            }
            res.setQuick(i - 1, val);
        }

        res.sort();
        return res;
    }

    private static class InLongConstFunction extends NegatableBooleanFunction implements UnaryFunction {
        private final LongList inList;
        private final Function tsFunc;

        public InLongConstFunction(Function tsFunc, LongList longList) {
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

    private static class InLongVarFunction extends NegatableBooleanFunction implements MultiArgFunction {
        private final ObjList<Function> args;

        public InLongVarFunction(ObjList<Function> args) {
            this.args = args;
        }

        @Override
        public ObjList<Function> getArgs() {
            return args;
        }

        @Override
        public boolean getBool(Record rec) {
            long ts = args.getQuick(0).getLong(rec);

            for (int i = 1, n = args.size(); i < n; i++) {
                Function func = args.getQuick(i);
                long val = Numbers.LONG_NaN;
                switch (ColumnType.tagOf(func.getType())) {
                    case ColumnType.BYTE:
                    case ColumnType.SHORT:
                    case ColumnType.INT:
                    case ColumnType.LONG:
                    case ColumnType.TIMESTAMP:
                        val = func.getLong(rec);
                        break;
                    case ColumnType.STRING:
                    case ColumnType.SYMBOL:
                        CharSequence str = func.getStr(rec);
                        val = Numbers.parseLongQuiet(str);
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

