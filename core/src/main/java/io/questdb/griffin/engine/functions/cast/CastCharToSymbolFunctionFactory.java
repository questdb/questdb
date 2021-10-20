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
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.constants.SymbolConstant;
import io.questdb.std.Chars;
import io.questdb.std.IntList;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.str.StringSink;

public class CastCharToSymbolFunctionFactory implements FunctionFactory {
    @Override
    public String getSignature() {
        return "cast(Ak)";
    }

    @Override
    public Function newInstance(int position, ObjList<Function> args, IntList argPositions, CairoConfiguration configuration, SqlExecutionContext sqlExecutionContext) {
        final Function arg = args.getQuick(0);
        if (arg.isConstant()) {
            final char value = arg.getChar(null);
            if (value == 0) {
                return SymbolConstant.NULL;
            }
            final StringSink sink = Misc.getThreadLocalBuilder();
            sink.put(value);
            return SymbolConstant.newInstance(Chars.toString(sink));
        }
        return new Func(arg);
    }

    private static class Func extends AbstractToSymbolCastFunction {

        public Func(Function arg) {
            super(arg);
        }

        @Override
        public int getInt(Record rec) {
            final char value = arg.getChar(rec);
            if (value == 0) {
                return SymbolTable.VALUE_IS_NULL;
            }
            return getInt0(value);
        }

        @Override
        public CharSequence getSymbol(Record rec) {
            final char value = arg.getChar(rec);
            if (value == 0) {
                return null;
            }

            final int keyIndex = symbolTableShortcut.keyIndex(value);
            if (keyIndex < 0) {
                return symbols.getQuick(symbolTableShortcut.valueAt(keyIndex));
            }

            symbolTableShortcut.putAt(keyIndex, value, next++);
            sink.clear();
            sink.put(value);
            final String str = Chars.toString(sink);
            symbols.add(Chars.toString(sink));
            return str;
        }
    }
}
