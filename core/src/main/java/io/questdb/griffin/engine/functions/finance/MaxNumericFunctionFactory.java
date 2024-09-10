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

package io.questdb.griffin.engine.functions.finance;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.DoubleFunction;
import io.questdb.griffin.engine.functions.LongFunction;
import io.questdb.griffin.engine.functions.MultiArgFunction;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;

public class MaxNumericFunctionFactory implements FunctionFactory {
    @Override
    public String getSignature() {
        return "max(V)";
    }

    @Override
    public Function newInstance(
            int position,
            ObjList<Function> args,
            IntList argPositions,
            CairoConfiguration configuration,
            SqlExecutionContext sqlExecutionContext
    ) throws SqlException {
        boolean anyFloats = false;

        for (int i = 0; i < args.size(); i++) {
            final Function arg = args.getQuick(i);
            final int type = arg.getType();
            switch (type) {
                case ColumnType.FLOAT:
                case ColumnType.DOUBLE:
                    anyFloats = true;
                case ColumnType.LONG:
                case ColumnType.INT:
                case ColumnType.SHORT:
                case ColumnType.BYTE:
                    continue;
                default:
                    throw SqlException.position(argPositions.getQuick(i)).put("unsupported type");
            }
        }

        if (anyFloats) {
            return new MaxDoubleRecordFunction(new ObjList<>(args), argPositions);
        } else {
            return new MaxLongRecordFunction(new ObjList<>(args), argPositions);
        }
    }

    private static class MaxDoubleRecordFunction extends DoubleFunction implements MultiArgFunction {
        final IntList argPositions;
        final ObjList<Function> args;

        public MaxDoubleRecordFunction(ObjList<Function> args, IntList argPositions) {
            this.args = args;
            this.argPositions = argPositions;
        }

        @Override
        public ObjList<Function> getArgs() {
            return args;
        }

        @Override
        public double getDouble(Record rec) {
            double value = Double.MIN_VALUE;
            for (int i = 0, n = args.size(); i < n; i++) {
                final double v = args.getQuick(i).getDouble(rec);
                value = Math.max(value, v);
            }
            return value;
        }

        @Override
        public String getName() {
            return "max[DOUBLE]";
        }

        @Override
        public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
            MultiArgFunction.super.init(symbolTableSource, executionContext);
        }
    }

    private static class MaxLongRecordFunction extends LongFunction implements MultiArgFunction {
        final IntList argPositions;
        final ObjList<Function> args;

        public MaxLongRecordFunction(ObjList<Function> args, IntList argPositions) {
            this.args = args;
            this.argPositions = argPositions;
        }

        @Override
        public ObjList<Function> getArgs() {
            return args;
        }

        @Override
        public long getLong(Record rec) {
            long value = Long.MIN_VALUE;
            for (int i = 0, n = args.size(); i < n; i++) {
                final long v = args.getQuick(i).getLong(rec);
                value = Math.max(value, v);
            }
            return value;
        }

        @Override
        public String getName() {
            return "max[LONG]";
        }

        @Override
        public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
            MultiArgFunction.super.init(symbolTableSource, executionContext);
        }
    }
}


