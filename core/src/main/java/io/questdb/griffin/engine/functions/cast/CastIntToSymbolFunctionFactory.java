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

package io.questdb.griffin.engine.functions.cast;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.engine.functions.SymbolFunction;
import io.questdb.griffin.engine.functions.UnaryFunction;
import io.questdb.griffin.engine.functions.constants.SymbolConstant;
import io.questdb.std.*;
import io.questdb.std.str.StringSink;
import org.jetbrains.annotations.NotNull;

public class CastIntToSymbolFunctionFactory implements FunctionFactory {
    @Override
    public String getSignature() {
        return "cast(Ik)";
    }

    @Override
    public Function newInstance(ObjList<Function> args, int position, CairoConfiguration configuration) {
        final Function arg = args.getQuick(0);
        if (arg.isConstant()) {
            final StringSink sink = Misc.getThreadLocalBuilder();
            Numbers.append(sink, arg.getInt(null));
            return new SymbolConstant(position, Chars.toString(sink), 0);
        }
        return new Func(position, arg);
    }

    private static class Func extends SymbolFunction implements UnaryFunction {
        private final Function arg;
        private final StringSink sink = new StringSink();
        private final IntIntHashMap symbolTableShortcut = new IntIntHashMap();
        private final ObjList<String> symbolTable = new ObjList<>();

        public Func(int position, Function arg) {
            super(position);
            this.arg = arg;
        }

        @Override
        public Function getArg() {
            return arg;
        }


        @Override
        public CharSequence getSymbol(Record rec) {
            final int value = arg.getInt(rec);
            if (value == Numbers.INT_NaN) {
                return null;
            }
            final int keyIndex = symbolTableShortcut.keyIndex(value);
            if (keyIndex < 0) {
                return symbolTable.getQuick(symbolTableShortcut.valueAt(keyIndex));
            }
            return cacheAndReturn(value, keyIndex);
        }

        @Override
        public int getInt(Record rec) {
            final int keyIndex = symbolTableShortcut.keyIndex(arg.getInt(rec));
            return keyIndex < 0 ? symbolTableShortcut.valueAt(keyIndex) : SymbolTable.VALUE_NOT_FOUND;
        }

        @NotNull
        private CharSequence cacheAndReturn(int value, int keyIndex) {
            symbolTableShortcut.putAt(keyIndex, value, symbolTable.size());
            sink.clear();
            Numbers.append(sink, value);
            symbolTable.add(Chars.toString(sink));
            return sink;
        }
    }
}
