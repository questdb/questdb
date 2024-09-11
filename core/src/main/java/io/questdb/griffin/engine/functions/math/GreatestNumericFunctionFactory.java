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

package io.questdb.griffin.engine.functions.math;

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
import io.questdb.griffin.engine.functions.cast.*;
import io.questdb.griffin.engine.functions.constants.NullConstant;
import io.questdb.std.IntList;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;

public class GreatestNumericFunctionFactory implements FunctionFactory {

    @Override
    public String getSignature() {
        return "greatest(V)";
    }

    @Override
    public Function newInstance(
            int position,
            ObjList<Function> args,
            IntList argPositions,
            CairoConfiguration configuration,
            SqlExecutionContext sqlExecutionContext
    ) throws SqlException {
        final int[] counters = new int[11];

        for (int i = 0; i < args.size(); i++) {
            final Function arg = args.getQuick(i);
            final int type = arg.getType();

            switch (type) {
                case ColumnType.FLOAT:
                case ColumnType.DOUBLE:
                case ColumnType.LONG:
                case ColumnType.INT:
                case ColumnType.SHORT:
                case ColumnType.BYTE:
                case ColumnType.TIMESTAMP:
                    counters[type]++;
                    continue;
                default:
                    if (arg.isNullConstant()) {
                        return NullConstant.NULL;
                    }
                    throw SqlException.position(argPositions.getQuick(i)).put("unsupported type");
            }
        }

        if (counters[ColumnType.DOUBLE] > 0) {
            return new GreatestDoubleRecordFunction(new ObjList<>(args), argPositions);
        }

        if (counters[ColumnType.FLOAT] > 0) {
            return new CastDoubleToFloatFunctionFactory()
                    .newInstance(position,
                            new ObjList<>(new GreatestDoubleRecordFunction(new ObjList<>(args), argPositions)),
                            null, configuration, sqlExecutionContext);
        }


        if (counters[ColumnType.LONG] > 0) {
            return new GreatestLongRecordFunction(new ObjList<>(args), argPositions);
        }

        if (counters[ColumnType.TIMESTAMP] > 0) {
            return new CastLongToTimestampFunctionFactory()
                    .newInstance(position,
                            new ObjList<>(new GreatestLongRecordFunction(new ObjList<>(args), argPositions)),
                            null, configuration, sqlExecutionContext);
        }

        if (counters[ColumnType.INT] > 0) {
            return new CastLongToIntFunctionFactory()
                    .newInstance(position,
                            new ObjList<>(new GreatestLongRecordFunction(new ObjList<>(args), argPositions)),
                            null, configuration, sqlExecutionContext);
        }

        if (counters[ColumnType.SHORT] > 0) {
            return new CastLongToShortFunctionFactory()
                    .newInstance(position,
                            new ObjList<>(new GreatestLongRecordFunction(new ObjList<>(args), argPositions)),
                            null, configuration, sqlExecutionContext);
        }

        if (counters[ColumnType.BYTE] > 0) {
            return new CastLongToByteFunctionFactory()
                    .newInstance(position,
                            new ObjList<>(new GreatestLongRecordFunction(new ObjList<>(args), argPositions)),
                            null, configuration, sqlExecutionContext);
        }

        assert false;

        // unreachable
        return null;
    }


    private static class GreatestDoubleRecordFunction extends DoubleFunction implements MultiArgFunction {
        final IntList argPositions;
        final ObjList<Function> args;

        public GreatestDoubleRecordFunction(ObjList<Function> args, IntList argPositions) {
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
                if (Numbers.isNull(v)) {
                    return Double.NaN;
                }
                value = Math.max(value, v);
            }
            return value;
        }

        @Override
        public String getName() {
            return "greatest[DOUBLE]";
        }

        @Override
        public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
            MultiArgFunction.super.init(symbolTableSource, executionContext);
        }
    }

    private static class GreatestLongRecordFunction extends LongFunction implements MultiArgFunction {
        final IntList argPositions;
        final ObjList<Function> args;

        public GreatestLongRecordFunction(ObjList<Function> args, IntList argPositions) {
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
                if (v == Long.MIN_VALUE) {
                    return Long.MIN_VALUE;
                }
                value = Math.max(value, v);
            }
            return value;
        }

        @Override
        public String getName() {
            return "greatest[LONG]";
        }

        @Override
        public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
            MultiArgFunction.super.init(symbolTableSource, executionContext);
        }
    }
}


