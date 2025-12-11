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
import io.questdb.griffin.engine.functions.VarcharFunction;
import io.questdb.std.str.Utf8Sequence;
import io.questdb.std.str.Utf8StringSink;

public final class VarcharFunctionMemoizer extends VarcharFunction implements MemoizerFunction {
    private final Function fn;
    private final Utf8StringSink sinkA = new Utf8StringSink();
    private final Utf8StringSink sinkB = new Utf8StringSink();
    // Either null or pointing to the corresponding sink
    private Utf8Sequence cachedVarcharA;
    private Utf8Sequence cachedVarcharB;
    private boolean validAValue;
    private boolean validBValue;

    public VarcharFunctionMemoizer(Function fn) {
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
    public Utf8Sequence getVarcharA(Record rec) {
        if (!validAValue) {
            Utf8Sequence strA;
            if (validBValue) {
                strA = cachedVarcharB;
            } else {
                strA = fn.getVarcharA(rec);
            }
            if (strA == null) {
                cachedVarcharA = null;
            } else {
                sinkA.clear();
                sinkA.put(strA);
                cachedVarcharA = sinkA;
            }
            validAValue = true;
        }
        return cachedVarcharA;
    }

    @Override
    public Utf8Sequence getVarcharB(Record rec) {
        if (!validBValue) {
            Utf8Sequence strB;
            if (validAValue) {
                strB = cachedVarcharA;
            } else {
                strB = fn.getVarcharB(rec);
            }
            if (strB == null) {
                cachedVarcharB = null;
            } else {
                sinkB.clear();
                sinkB.put(strB);
                cachedVarcharB = sinkB;
            }
            validBValue = true;
        }
        return cachedVarcharB;
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
