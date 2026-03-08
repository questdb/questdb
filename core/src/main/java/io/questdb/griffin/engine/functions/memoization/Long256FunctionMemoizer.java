/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2026 QuestDB
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
import io.questdb.griffin.engine.functions.Long256Function;
import io.questdb.std.Long256;
import io.questdb.std.Long256Impl;
import io.questdb.std.str.CharSink;

public final class Long256FunctionMemoizer extends Long256Function implements MemoizerFunction {
    private final Function fn;
    private final Long256Impl valueA = new Long256Impl();
    private final Long256Impl valueB = new Long256Impl();
    private boolean validAValue;
    private boolean validBValue;

    public Long256FunctionMemoizer(Function fn) {
        this.fn = fn;
    }

    @Override
    public Function getArg() {
        return fn;
    }

    @Override
    public void getLong256(Record rec, CharSink<?> sink) {
        if (!validAValue) {
            Long256 long256 = fn.getLong256A(rec);
            valueA.copyFrom(long256);
            validAValue = true;
        }
        valueA.toSink(sink);
    }

    @Override
    public Long256 getLong256A(Record rec) {
        if (!validAValue) {
            if (validBValue) {
                valueA.copyFrom(valueB);
            } else {
                valueA.copyFrom(fn.getLong256A(rec));
            }
            validAValue = true;
        }
        return valueA;
    }

    @Override
    public Long256 getLong256B(Record rec) {
        if (!validBValue) {
            if (validAValue) {
                valueB.copyFrom(valueA);
            } else {
                valueB.copyFrom(fn.getLong256B(rec));
            }
            validBValue = true;
        }
        return valueB;
    }

    @Override
    public String getName() {
        return "memoize";
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
