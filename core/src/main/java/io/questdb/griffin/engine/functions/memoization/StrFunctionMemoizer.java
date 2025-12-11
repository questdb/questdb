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
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.StrFunction;
import io.questdb.std.str.StringSink;

public final class StrFunctionMemoizer extends StrFunction implements MemoizerFunction {
    private final Function fn;
    private final StringSink sinkA = new StringSink();
    private final StringSink sinkB = new StringSink();
    // Either null or pointing to the corresponding sink
    private CharSequence cachedStrA;
    private CharSequence cachedStrB;
    private boolean validAValue;
    private boolean validBValue;

    public StrFunctionMemoizer(Function fn) {
        this.fn = fn;
    }

    @Override
    public Function getArg() {
        return fn;
    }

    @Override
    public String getName() {
        return "memoize";
    }

    @Override
    public CharSequence getStrA(final Record rec) {
        if (!validAValue) {
            CharSequence strA;
            if (validBValue) {
                strA = cachedStrB;
            } else {
                strA = fn.getStrA(rec);
            }
            if (strA == null) {
                cachedStrA = null;
            } else {
                sinkA.clear();
                sinkA.put(strA);
                cachedStrA = sinkA;
            }
            validAValue = true;
        }
        return cachedStrA;
    }

    @Override
    public CharSequence getStrB(final Record rec) {
        if (!validBValue) {
            CharSequence strB;
            if (validAValue) {
                strB = cachedStrA;
            } else {
                strB = fn.getStrB(rec);
            }
            if (strB == null) {
                cachedStrB = null;
            } else {
                sinkB.clear();
                sinkB.put(strB);
                cachedStrB = sinkB;
            }
            validBValue = true;
        }
        return cachedStrB;
    }

    @Override
    public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
        MemoizerFunction.super.init(symbolTableSource, executionContext);
    }

    @Override
    public boolean isThreadSafe() {
        return false;
    }

    @Override
    public void memoize(Record record) {
        validAValue = false;
        validBValue = false;
    }

    @Override
    public boolean supportsRandomAccess() {
        return fn.supportsRandomAccess();
    }
}
