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
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.SymbolFunction;
import io.questdb.griffin.engine.functions.UnaryFunction;
import io.questdb.griffin.engine.functions.constants.SymbolConstant;
import io.questdb.std.CharSequenceIntHashMap;
import io.questdb.std.Chars;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;

public class CastStrToSymbolFunctionFactory implements FunctionFactory {
    @Override
    public String getSignature() {
        return "cast(Sk)";
    }

    @Override
    public Function newInstance(int position, ObjList<Function> args, IntList argPositions, CairoConfiguration configuration, SqlExecutionContext sqlExecutionContext) {
        final Function arg = args.getQuick(0);
        if (arg.isConstant()) {
            return SymbolConstant.newInstance(arg.getStr(null));
        }
        return new Func(arg);
    }

    private static class Func extends SymbolFunction implements UnaryFunction {
        private final Function arg;
        private final CharSequenceIntHashMap lookupMap = new CharSequenceIntHashMap();
        private final ObjList<CharSequence> symbols = new ObjList<>();
        private int next = 1;

        public Func(Function arg) {
            this.arg = arg;
            symbols.add(null);
        }

        @Override
        public Function getArg() {
            return arg;
        }

        @Override
        public CharSequence getSymbol(Record rec) {
            final CharSequence value = arg.getStr(rec);
            return getSymbol(value);
        }

        @Override
        public CharSequence getSymbolB(Record rec) {
            final CharSequence value = arg.getStrB(rec);
            return getSymbol(value);
        }

        private CharSequence getSymbol(CharSequence value) {
            final int keyIndex;
            if (value != null && (keyIndex = lookupMap.keyIndex(value)) > -1) {
                final String str = Chars.toString(value);
                lookupMap.putAt(keyIndex, str, next++);
                symbols.add(str);
            }
            return value;
        }

        @Override
        public CharSequence valueOf(int symbolKey) {
            return symbols.getQuick(TableUtils.toIndexKey(symbolKey));
        }

        @Override
        public CharSequence valueBOf(int key) {
            return valueOf(key);
        }

        @Override
        public int getInt(Record rec) {
            final CharSequence value = arg.getStr(rec);
            final int keyIndex;
            if (value == null) {
                return SymbolTable.VALUE_IS_NULL;
            }
            if ((keyIndex = lookupMap.keyIndex(value)) > -1) {
                final String str = Chars.toString(value);
                lookupMap.putAt(keyIndex, str, next);
                symbols.add(str);
                return next++ - 1;
            }
            return lookupMap.valueAt(keyIndex) - 1;
        }

        @Override
        public boolean isSymbolTableStatic() {
            return false;
        }

        @Override
        public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
            arg.init(symbolTableSource, executionContext);
            lookupMap.clear();
            symbols.clear();
            symbols.add(null);
            next = 1;
        }
    }
}
