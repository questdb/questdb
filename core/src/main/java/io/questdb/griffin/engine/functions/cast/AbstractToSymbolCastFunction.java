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

import io.questdb.cairo.TableUtils;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.SymbolFunction;
import io.questdb.griffin.engine.functions.UnaryFunction;
import io.questdb.std.Chars;
import io.questdb.std.IntIntHashMap;
import io.questdb.std.ObjList;
import io.questdb.std.str.StringSink;
import org.jetbrains.annotations.Nullable;

public abstract class AbstractToSymbolCastFunction extends SymbolFunction implements UnaryFunction {
    protected final Function arg;
    protected final StringSink sink = new StringSink();
    protected final IntIntHashMap symbolTableShortcut = new IntIntHashMap();
    protected final ObjList<String> symbols = new ObjList<>();
    protected int next = 1;

    public AbstractToSymbolCastFunction(Function arg) {
        this.arg = arg;
        symbols.add(null);
    }

    @Override
    public Function getArg() {
        return arg;
    }

    protected int getInt0(int value) {
        final int keyIndex = symbolTableShortcut.keyIndex(value);
        if (keyIndex < 0) {
            return symbolTableShortcut.valueAt(keyIndex) - 1;
        }

        symbolTableShortcut.putAt(keyIndex, value, next);
        sink.clear();
        sink.put(value);
        symbols.add(Chars.toString(sink));
        return next++ - 1;
    }

    @Nullable
    protected String getSymbol0(int value) {
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

    @Override
    public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
        arg.init(symbolTableSource, executionContext);
        symbolTableShortcut.clear();
        symbols.clear();
        symbols.add(null);
        next = 1;
    }

    @Override
    public boolean isSymbolTableStatic() {
        return false;
    }

    @Override
    public CharSequence getSymbolB(Record rec) {
        return getSymbol(rec);
    }


    @Override
    public CharSequence valueOf(int symbolKey) {
        return symbols.getQuick(TableUtils.toIndexKey(symbolKey));
    }

    @Override
    public CharSequence valueBOf(int key) {
        return valueOf(key);
    }
}
