/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
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
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.BooleanFunction;
import io.questdb.griffin.engine.functions.MultiArgFunction;
import io.questdb.griffin.model.IntervalUtils;
import io.questdb.std.LongList;
import io.questdb.std.Numbers;
import io.questdb.std.NumericException;
import io.questdb.std.ObjList;

public class InTimestampTimestampFunctionFactory implements FunctionFactory {
    @Override
    public String getSignature() {
        return "in(NV)";
    }

    @Override
    public Function newInstance(ObjList<Function> args, int position, CairoConfiguration configuration, SqlExecutionContext sqlExecutionContext) throws SqlException {
        boolean allConst = true;
        for (int i = 1, n = args.size(); i < n; i++) {
            Function func = args.getQuick(i);
            switch (func.getType()) {
                case ColumnType.TIMESTAMP:
                case ColumnType.LONG:
                case ColumnType.INT:
                case ColumnType.STRING:
                    break;
                default:
                    throw SqlException.position(0).put("cannot compare TIMESTAMP with type ").put(ColumnType.nameOf(func.getType()));
            }
            if (!func.isConstant()) {
                allConst = false;
                break;
            }
        }

        if (allConst) {
            return new InTimestampConstFunction(position, args.getQuick(0), parseToTs(args));
        }
        // have to copy, args is mutable
        return new InTimestampVarFunction(position, new ObjList<>(args));
    }

    private LongList parseToTs(ObjList<Function> args) throws SqlException {
        LongList res = new LongList(args.size() - 1);
        res.extendAndSet(args.size() - 2, 0);

        for (int i = 1, n = args.size(); i < n; i++) {
            Function func = args.getQuick(i);
            long val = Numbers.LONG_NaN;
            switch (func.getType()) {
                case ColumnType.TIMESTAMP:
                case ColumnType.LONG:
                case ColumnType.INT:
                    val = func.getTimestamp(null);
                    break;
                case ColumnType.STRING:
                    CharSequence tsValue = func.getStr(null);
                    val = (tsValue != null) ? tryParseTimestamp(tsValue, func.getPosition()) : Numbers.LONG_NaN;
                    break;
            }
            res.setQuick(i - 1, val);
        }

        res.sort();
        return res;
    }

    public static long tryParseTimestamp(CharSequence seq, int position) throws SqlException {
        try {
            return IntervalUtils.parseFloorPartialDate(seq, 0, seq.length());
        } catch (NumericException e) {
            throw SqlException.invalidDate(position);
        }
    }

    private static class InTimestampVarFunction extends BooleanFunction implements MultiArgFunction {
        private final ObjList<Function> args;

        public InTimestampVarFunction(int position, ObjList<Function> args) {
            super(position);
            this.args = args;
        }

        @Override
        public ObjList<Function> getArgs() {
            return args;
        }

        @Override
        public boolean getBool(Record rec) {
            long ts = args.getQuick(0).getTimestamp(rec);
            if (ts == Numbers.LONG_NaN) {
                return false;
            }

            for (int i = 1, n = args.size(); i < n; i++) {
                Function func = args.getQuick(i);
                long val = Numbers.LONG_NaN;
                switch (func.getType()) {
                    case ColumnType.TIMESTAMP:
                    case ColumnType.LONG:
                    case ColumnType.INT:
                        val = func.getTimestamp(rec);
                        break;
                    case ColumnType.STRING:
                        CharSequence str = func.getStr(rec);
                        val = str != null ? IntervalUtils.tryParseTimestamp(str) : Numbers.LONG_NaN;
                        break;
                }
                if (val == ts) {
                    return true;
                }
            }
            return false;
        }
    }

    private static class InTimestampConstFunction extends BooleanFunction {
        private final Function tsFunc;
        private final LongList inList;

        public InTimestampConstFunction(int position, Function tsFunc, LongList longList) {
            super(position);
            this.tsFunc = tsFunc;
            this.inList = longList;
        }

        @Override
        public boolean getBool(Record rec) {
            long ts = tsFunc.getTimestamp(rec);
            if (ts == Numbers.LONG_NaN) {
                return false;
            }

            return inList.binarySearch(ts) >= 0;
        }
    }
}
