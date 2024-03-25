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

package io.questdb.griffin.engine.functions.str;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlUtil;
import io.questdb.griffin.engine.functions.MultiArgFunction;
import io.questdb.griffin.engine.functions.StrFunction;
import io.questdb.std.IntList;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.Transient;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf16Sink;

public class ConcatFunctionFactory implements FunctionFactory {
    private static final ObjList<TypeAdapter> adapterReferences = new ObjList<>();

    @Override
    public String getSignature() {
        return "concat(V)";
    }

    @Override
    public Function newInstance(
            int position,
            @Transient ObjList<Function> args,
            IntList argPositions,
            CairoConfiguration configuration,
            SqlExecutionContext sqlExecutionContext
    ) {
        final ObjList<Function> functions = new ObjList<>(args.size());
        functions.addAll(args);
        return new ConcatFunction(functions);
    }

    private static void sinkBin(Utf16Sink sink, Function function, Record record) {
        sink.put('[');
        sink.put(']');
    }

    private static void sinkBool(Utf16Sink sink, Function function, Record record) {
        sink.put(function.getBool(record));
    }

    private static void sinkByte(Utf16Sink sink, Function function, Record record) {
        sink.put(function.getByte(record));
    }

    private static void sinkChar(Utf16Sink sink, Function function, Record record) {
        sink.put(function.getChar(record));
    }

    private static void sinkDate(Utf16Sink sink, Function function, Record record) {
        sink.put(function.getDate(record));
    }

    private static void sinkDouble(Utf16Sink sink, Function function, Record record) {
        sink.put(function.getDouble(record));
    }

    private static void sinkFloat(Utf16Sink sink, Function function, Record record) {
        sink.put(function.getFloat(record), 3);
    }

    private static void sinkIPv4(Utf16Sink utf16Sink, Function function, Record record) {
        Numbers.intToIPv4Sink(utf16Sink, function.getIPv4(record));
    }

    private static void sinkInt(Utf16Sink sink, Function function, Record record) {
        sink.put(function.getInt(record));
    }

    private static void sinkLong(Utf16Sink sink, Function function, Record record) {
        sink.put(function.getLong(record));
    }

    private static void sinkLong256(Utf16Sink sink, Function function, Record record) {
        function.getLong256(record, sink);
    }

    private static void sinkNull(Utf16Sink sink, Function function, Record record) {
        // ignore nulls
    }

    private static void sinkShort(Utf16Sink sink, Function function, Record record) {
        sink.put(function.getShort(record));
    }

    private static void sinkStr(Utf16Sink sink, Function function, Record record) {
        function.getStr(record, sink);
    }

    private static void sinkSymbol(Utf16Sink sink, Function function, Record record) {
        sink.put(function.getSymbol(record));
    }

    private static void sinkTimestamp(Utf16Sink sink, Function function, Record record) {
        sink.put(function.getTimestamp(record));
    }

    private static void sinkUuid(Utf16Sink sink, Function function, Record record) {
        long lo = function.getLong128Lo(record);
        long hi = function.getLong128Hi(record);
        SqlUtil.implicitCastUuidAsStr(lo, hi, sink);
    }

    private static void sinkVarchar(Utf16Sink utf16Sink, Function function, Record record) {
        function.getStr(record, utf16Sink);
    }

    @FunctionalInterface
    private interface TypeAdapter {
        void sink(Utf16Sink sink, Function function, Record record);
    }

    private static class ConcatFunction extends StrFunction implements MultiArgFunction {
        private final ObjList<TypeAdapter> adapters;
        private final int functionCount;
        private final ObjList<Function> functions;
        private final StringSink sinkA = new StringSink();
        private final StringSink sinkB = new StringSink();

        public ConcatFunction(ObjList<Function> functions) {
            this.functions = functions;
            this.functionCount = functions.size();
            this.adapters = new ObjList<>(functionCount);
            for (int i = 0; i < functionCount; i++) {
                adapters.add(adapterReferences.getQuick(functions.getQuick(i).getType()));
            }
        }

        @Override
        public ObjList<Function> getArgs() {
            return functions;
        }

        @Override
        public void getStr(Record rec, Utf16Sink utf16Sink) {
            for (int i = 0; i < functionCount; i++) {
                adapters.getQuick(i).sink(utf16Sink, functions.getQuick(i), rec);
            }
        }

        @Override
        public CharSequence getStrA(Record rec) {
            sinkA.clear();
            getStr(rec, sinkA);
            return sinkA;
        }

        @Override
        public CharSequence getStrB(Record rec) {
            sinkB.clear();
            getStr(rec, sinkB);
            return sinkB;
        }

        @Override
        public boolean isReadThreadSafe() {
            return false;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val("concat(").val(functions).val(')');
        }
    }

    static {
        adapterReferences.extendAndSet(ColumnType.LONG256, ConcatFunctionFactory::sinkLong256);
        adapterReferences.extendAndSet(ColumnType.BOOLEAN, ConcatFunctionFactory::sinkBool);
        adapterReferences.extendAndSet(ColumnType.BYTE, ConcatFunctionFactory::sinkByte);
        adapterReferences.extendAndSet(ColumnType.SHORT, ConcatFunctionFactory::sinkShort);
        adapterReferences.extendAndSet(ColumnType.CHAR, ConcatFunctionFactory::sinkChar);
        adapterReferences.extendAndSet(ColumnType.INT, ConcatFunctionFactory::sinkInt);
        adapterReferences.extendAndSet(ColumnType.IPv4, ConcatFunctionFactory::sinkIPv4);
        adapterReferences.extendAndSet(ColumnType.LONG, ConcatFunctionFactory::sinkLong);
        adapterReferences.extendAndSet(ColumnType.FLOAT, ConcatFunctionFactory::sinkFloat);
        adapterReferences.extendAndSet(ColumnType.DOUBLE, ConcatFunctionFactory::sinkDouble);
        adapterReferences.extendAndSet(ColumnType.STRING, ConcatFunctionFactory::sinkStr);
        adapterReferences.extendAndSet(ColumnType.VARCHAR, ConcatFunctionFactory::sinkVarchar);
        adapterReferences.extendAndSet(ColumnType.SYMBOL, ConcatFunctionFactory::sinkSymbol);
        adapterReferences.extendAndSet(ColumnType.BINARY, ConcatFunctionFactory::sinkBin);
        adapterReferences.extendAndSet(ColumnType.DATE, ConcatFunctionFactory::sinkDate);
        adapterReferences.extendAndSet(ColumnType.TIMESTAMP, ConcatFunctionFactory::sinkTimestamp);
        adapterReferences.extendAndSet(ColumnType.UUID, ConcatFunctionFactory::sinkUuid);
        adapterReferences.extendAndSet(ColumnType.NULL, ConcatFunctionFactory::sinkNull);
    }
}
