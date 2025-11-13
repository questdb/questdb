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

package io.questdb.griffin.engine.functions.str;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlUtil;
import io.questdb.griffin.engine.functions.MultiArgFunction;
import io.questdb.griffin.engine.functions.StrFunction;
import io.questdb.griffin.engine.functions.constants.ConstantFunction;
import io.questdb.std.Decimal128;
import io.questdb.std.Decimal256;
import io.questdb.std.Decimals;
import io.questdb.std.IntList;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.Transient;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf16Sink;

import static io.questdb.cairo.ColumnType.*;

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
            @Transient IntList argPositions,
            CairoConfiguration configuration,
            SqlExecutionContext sqlExecutionContext
    ) throws SqlException {
        if (args == null || args.size() == 0) {
            throw SqlException.$(position, "no arguments provided");
        }

        final int n = args.size();

        boolean allConst = true;
        for (int i = 0; i < n; i++) {
            Function func = args.getQuick(i);
            if (!func.isConstant()) {
                allConst = false;
            }
        }

        if (allConst) {
            return new ConstConcatFunction(new ObjList<>(args), argPositions);
        }
        final IntList positions = new IntList();
        positions.addAll(argPositions);
        return new ConcatFunction(new ObjList<>(args), positions);
    }

    private static void populateAdapters(ObjList<TypeAdapter> adapters, ObjList<Function> functions, IntList argPositions) throws SqlException {
        final int functionCount = functions.size();
        for (int i = 0; i < functionCount; i++) {
            final int type = functions.getQuick(i).getType();
            int tag = ColumnType.tagOf(type);
            final TypeAdapter adapter = adapterReferences.getQuick(tag);
            if (adapter == null) {
                throw SqlException.position(argPositions.getQuick(i)).put("unsupported type: ").put(nameOf(type));
            }
            adapters.add(adapter);
        }
    }

    private static void sinkBin(Decimal256 decimal256, Decimal128 decimal128, Utf16Sink sink, Function function, Record record) {
        sink.put('[');
        sink.put(']');
    }

    private static void sinkBool(Decimal256 decimal256, Decimal128 decimal128, Utf16Sink sink, Function function, Record record) {
        sink.put(function.getBool(record));
    }

    private static void sinkByte(Decimal256 decimal256, Decimal128 decimal128, Utf16Sink sink, Function function, Record record) {
        sink.put(function.getByte(record));
    }

    private static void sinkChar(Decimal256 decimal256, Decimal128 decimal128, Utf16Sink sink, Function function, Record record) {
        sink.put(function.getChar(record));
    }

    private static void sinkDate(Decimal256 decimal256, Decimal128 decimal128, Utf16Sink sink, Function function, Record record) {
        sink.put(function.getDate(record));
    }

    private static void sinkDecimal128(Decimal256 decimal256, Decimal128 decimal128, Utf16Sink sink, Function function, Record record) {
        function.getDecimal128(record, decimal128);
        if (decimal128.isNull()) {
            sink.putAscii("null");
        } else {
            final int type = function.getType();
            Decimals.appendNonNull(decimal128, ColumnType.getDecimalPrecision(type), ColumnType.getDecimalScale(type), sink);
        }
    }

    private static void sinkDecimal16(Decimal256 decimal256, Decimal128 decimal128, Utf16Sink sink, Function function, Record record) {
        short value = function.getDecimal16(record);
        if (value == Decimals.DECIMAL16_NULL) {
            sink.putAscii("null");
        } else {
            sinkDecimalLong(sink, value, function.getType());
        }
    }

    private static void sinkDecimal256(Decimal256 decimal256, Decimal128 decimal128, Utf16Sink sink, Function function, Record record) {
        function.getDecimal256(record, decimal256);
        if (decimal256.isNull()) {
            sink.putAscii("null");
        } else {
            final int type = function.getType();
            Decimals.append(decimal256, ColumnType.getDecimalPrecision(type), ColumnType.getDecimalScale(type), sink);
        }
    }

    private static void sinkDecimal32(Decimal256 decimal256, Decimal128 decimal128, Utf16Sink sink, Function function, Record record) {
        int value = function.getDecimal32(record);
        if (value == Decimals.DECIMAL32_NULL) {
            sink.putAscii("null");
        } else {
            sinkDecimalLong(sink, value, function.getType());
        }
    }

    private static void sinkDecimal64(Decimal256 decimal256, Decimal128 decimal128, Utf16Sink sink, Function function, Record record) {
        long value = function.getDecimal64(record);
        if (value == Decimals.DECIMAL64_NULL) {
            sink.putAscii("null");
        } else {
            sinkDecimalLong(sink, value, function.getType());
        }
    }

    private static void sinkDecimal8(Decimal256 decimal256, Decimal128 decimal128, Utf16Sink sink, Function function, Record record) {
        byte value = function.getDecimal8(record);
        if (value == Decimals.DECIMAL8_NULL) {
            sink.putAscii("null");
        } else {
            sinkDecimalLong(sink, value, function.getType());
        }
    }

    private static void sinkDecimalLong(Utf16Sink sink, long value, int type) {
        Decimals.append(value, ColumnType.getDecimalPrecision(type), ColumnType.getDecimalScale(type), sink);
    }

    private static void sinkDouble(Decimal256 decimal256, Decimal128 decimal128, Utf16Sink sink, Function function, Record record) {
        sink.put(function.getDouble(record));
    }

    private static void sinkFloat(Decimal256 decimal256, Decimal128 decimal128, Utf16Sink sink, Function function, Record record) {
        sink.put(function.getFloat(record));
    }

    private static void sinkIPv4(Decimal256 decimal256, Decimal128 decimal128, Utf16Sink utf16Sink, Function function, Record record) {
        Numbers.intToIPv4Sink(utf16Sink, function.getIPv4(record));
    }

    private static void sinkInt(Decimal256 decimal256, Decimal128 decimal128, Utf16Sink sink, Function function, Record record) {
        sink.put(function.getInt(record));
    }

    private static void sinkLong(Decimal256 decimal256, Decimal128 decimal128, Utf16Sink sink, Function function, Record record) {
        sink.put(function.getLong(record));
    }

    private static void sinkLong256(Decimal256 decimal256, Decimal128 decimal128, Utf16Sink sink, Function function, Record record) {
        function.getLong256(record, sink);
    }

    private static void sinkNull(Decimal256 decimal256, Decimal128 decimal128, Utf16Sink sink, Function function, Record record) {
        // ignore nulls
    }

    private static void sinkShort(Decimal256 decimal256, Decimal128 decimal128, Utf16Sink sink, Function function, Record record) {
        sink.put(function.getShort(record));
    }

    private static void sinkStr(Decimal256 decimal256, Decimal128 decimal128, Utf16Sink sink, Function function, Record record) {
        sink.put(function.getStrA(record));
    }

    private static void sinkSymbol(Decimal256 decimal256, Decimal128 decimal128, Utf16Sink sink, Function function, Record record) {
        sink.put(function.getSymbol(record));
    }

    private static void sinkTimestamp(Decimal256 decimal256, Decimal128 decimal128, Utf16Sink sink, Function function, Record record) {
        ColumnType.getTimestampDriver(function.getType()).append(sink, function.getTimestamp(record));
    }

    private static void sinkUuid(Decimal256 decimal256, Decimal128 decimal128, Utf16Sink sink, Function function, Record record) {
        long lo = function.getLong128Lo(record);
        long hi = function.getLong128Hi(record);
        SqlUtil.implicitCastUuidAsStr(lo, hi, sink);
    }

    private static void sinkVarchar(Decimal256 decimal256, Decimal128 decimal128, Utf16Sink utf16Sink, Function function, Record record) {
        utf16Sink.put(function.getStrA(record));
    }

    @FunctionalInterface
    private interface TypeAdapter {
        void sink(Decimal256 decimal256, Decimal128 decimal128, Utf16Sink sink, Function function, Record record);
    }

    private static class ConcatFunction extends StrFunction implements MultiArgFunction {
        private final ObjList<TypeAdapter> adapters;
        private final IntList argPositions;
        private final Decimal128 decimal128 = new Decimal128();
        private final Decimal256 decimal256 = new Decimal256();
        private final int functionCount;
        private final ObjList<Function> functions;
        private final StringSink sinkA = new StringSink();
        private final StringSink sinkB = new StringSink();

        public ConcatFunction(ObjList<Function> functions, IntList argPositions) {
            this.functions = functions;
            this.functionCount = functions.size();
            this.argPositions = argPositions;
            this.adapters = new ObjList<>(functionCount);
        }

        @Override
        public ObjList<Function> args() {
            return functions;
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
        public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
            MultiArgFunction.super.init(symbolTableSource, executionContext);
            populateAdapters(adapters, functions, argPositions);
        }

        @Override
        public boolean isThreadSafe() {
            return false;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val("concat(").val(functions).val(')');
        }

        private void getStr(Record rec, Utf16Sink utf16Sink) {
            for (int i = 0; i < functionCount; i++) {
                adapters.getQuick(i).sink(decimal256, decimal128, utf16Sink, functions.getQuick(i), rec);
            }
        }
    }

    private static class ConstConcatFunction extends StrFunction implements ConstantFunction {
        private final ObjList<Function> functions;
        private final StringSink sink = new StringSink();

        public ConstConcatFunction(ObjList<Function> functions, IntList argPositions) throws SqlException {
            this.functions = functions;
            Decimal128 decimal128 = Misc.getThreadLocalDecimal128();
            Decimal256 decimal256 = Misc.getThreadLocalDecimal256();

            final int functionCount = functions.size();
            final ObjList<TypeAdapter> adapters = new ObjList<>(functionCount);
            populateAdapters(adapters, functions, argPositions);
            for (int i = 0; i < functionCount; i++) {
                adapters.getQuick(i).sink(decimal256, decimal128, sink, functions.getQuick(i), null);
            }
        }

        @Override
        public CharSequence getStrA(Record rec) {
            return sink;
        }

        @Override
        public CharSequence getStrB(Record rec) {
            return sink;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val("concat(").val(functions).val(')');
        }
    }

    static {
        adapterReferences.extendAndSet(LONG256, ConcatFunctionFactory::sinkLong256);
        adapterReferences.extendAndSet(BOOLEAN, ConcatFunctionFactory::sinkBool);
        adapterReferences.extendAndSet(BYTE, ConcatFunctionFactory::sinkByte);
        adapterReferences.extendAndSet(SHORT, ConcatFunctionFactory::sinkShort);
        adapterReferences.extendAndSet(CHAR, ConcatFunctionFactory::sinkChar);
        adapterReferences.extendAndSet(INT, ConcatFunctionFactory::sinkInt);
        adapterReferences.extendAndSet(IPv4, ConcatFunctionFactory::sinkIPv4);
        adapterReferences.extendAndSet(LONG, ConcatFunctionFactory::sinkLong);
        adapterReferences.extendAndSet(FLOAT, ConcatFunctionFactory::sinkFloat);
        adapterReferences.extendAndSet(DOUBLE, ConcatFunctionFactory::sinkDouble);
        adapterReferences.extendAndSet(STRING, ConcatFunctionFactory::sinkStr);
        adapterReferences.extendAndSet(VARCHAR, ConcatFunctionFactory::sinkVarchar);
        adapterReferences.extendAndSet(SYMBOL, ConcatFunctionFactory::sinkSymbol);
        adapterReferences.extendAndSet(BINARY, ConcatFunctionFactory::sinkBin);
        adapterReferences.extendAndSet(DATE, ConcatFunctionFactory::sinkDate);
        adapterReferences.extendAndSet(TIMESTAMP, ConcatFunctionFactory::sinkTimestamp);
        adapterReferences.extendAndSet(UUID, ConcatFunctionFactory::sinkUuid);
        adapterReferences.extendAndSet(DECIMAL8, ConcatFunctionFactory::sinkDecimal8);
        adapterReferences.extendAndSet(DECIMAL16, ConcatFunctionFactory::sinkDecimal16);
        adapterReferences.extendAndSet(DECIMAL32, ConcatFunctionFactory::sinkDecimal32);
        adapterReferences.extendAndSet(DECIMAL64, ConcatFunctionFactory::sinkDecimal64);
        adapterReferences.extendAndSet(DECIMAL128, ConcatFunctionFactory::sinkDecimal128);
        adapterReferences.extendAndSet(DECIMAL256, ConcatFunctionFactory::sinkDecimal256);
        adapterReferences.extendAndSet(NULL, ConcatFunctionFactory::sinkNull);
    }
}
