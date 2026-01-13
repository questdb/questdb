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

package io.questdb.griffin.engine.functions.memoization;

import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.StaticSymbolTable;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.SymbolFunction;
import io.questdb.std.str.StringSink;

public final class SymbolFunctionMemoizer extends SymbolFunction implements MemoizerFunction {
    private final Function fn;
    private final StringSink sinkA = new StringSink();
    private final StringSink sinkB = new StringSink();
    private int cachedInt;
    // Cached results: either null or pointing to the corresponding sink
    private StringSink cachedSymbolA;
    private StringSink cachedSymbolB;
    private boolean validAValue;
    private boolean validBValue;
    private boolean validIntValue;

    public SymbolFunctionMemoizer(Function fn) {
        this.fn = fn;
    }

    @Override
    public Function getArg() {
        return fn;
    }

    @Override
    public int getInt(Record rec) {
        if (!validIntValue) {
            cachedInt = fn.getInt(rec);
            validIntValue = true;
        }
        return cachedInt;
    }

    @Override
    public String getName() {
        return "memoize";
    }

    @Override
    public StaticSymbolTable getStaticSymbolTable() {
        if (fn instanceof SymbolFunction symbolFunction) {
            return symbolFunction.getStaticSymbolTable();
        }
        return null;
    }

    @Override
    public CharSequence getSymbol(Record rec) {
        if (!validAValue) {
            CharSequence symbol;
            if (validBValue) {
                symbol = cachedSymbolB;
            } else {
                if (!validIntValue) {
                    cachedInt = fn.getInt(rec);
                    validIntValue = true;
                }
                symbol = valueOf(cachedInt);
            }
            if (symbol == null) {
                cachedSymbolA = null;
            } else {
                sinkA.clear();
                sinkA.put(symbol);
                cachedSymbolA = sinkA;
            }
            validAValue = true;
        }
        return cachedSymbolA;
    }

    @Override
    public CharSequence getSymbolB(Record rec) {
        if (!validBValue) {
            CharSequence symbol;
            if (validAValue) {
                symbol = cachedSymbolA;
            } else {
                if (!validIntValue) {
                    cachedInt = fn.getInt(rec);
                    validIntValue = true;
                }
                symbol = valueBOf(cachedInt);
            }
            if (symbol == null) {
                cachedSymbolB = null;
            } else {
                sinkB.clear();
                sinkB.put(symbol);
                cachedSymbolB = sinkB;
            }
            validBValue = true;
        }
        return cachedSymbolB;
    }

    @Override
    public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
        MemoizerFunction.super.init(symbolTableSource, executionContext);
    }

    @Override
    public boolean isSymbolTableStatic() {
        if (fn instanceof SymbolFunction symbolFunction) {
            return symbolFunction.isSymbolTableStatic();
        }
        return false;
    }

    @Override
    public boolean isThreadSafe() {
        return false;
    }

    @Override
    public void memoize(Record record) {
        validIntValue = false;
        validAValue = false;
        validBValue = false;
    }

    @Override
    public SymbolTable newSymbolTable() {
        if (fn instanceof SymbolFunction symbolFunction) {
            return symbolFunction.newSymbolTable();
        }
        return null;
    }

    @Override
    public boolean supportsRandomAccess() {
        return fn.supportsRandomAccess();
    }

    @Override
    public CharSequence valueBOf(int key) {
        if (fn instanceof SymbolTable symbolTable) {
            return symbolTable.valueBOf(key);
        }
        throw new UnsupportedOperationException();
    }

    @Override
    public CharSequence valueOf(int key) {
        if (fn instanceof SymbolTable symbolTable) {
            return symbolTable.valueOf(key);
        }
        throw new UnsupportedOperationException();
    }
}
