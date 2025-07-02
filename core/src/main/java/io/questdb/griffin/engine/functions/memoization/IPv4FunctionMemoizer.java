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
import io.questdb.griffin.engine.functions.IPv4Function;
import io.questdb.griffin.engine.functions.UnaryFunction;

public final class IPv4FunctionMemoizer extends IPv4Function implements UnaryFunction {
    private final Function fn;
    private boolean memoized;
    private int value;

    public IPv4FunctionMemoizer(Function fn) {
        assert fn.shouldMemoize();
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
    public int getIPv4(Record rec) {
        return memoized ? value : fn.getIPv4(rec);
    }

    @Override
    public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
        memoized = false;
        UnaryFunction.super.init(symbolTableSource, executionContext);
    }

    @Override
    public boolean isThreadSafe() {
        return false;
    }

    @Override
    public void memoize(Record record) {
        value = fn.getIPv4(record);
        memoized = true;
    }

    @Override
    public boolean shouldMemoize() {
        return true;
    }

    @Override
    public boolean supportsRandomAccess() {
        return fn.supportsRandomAccess();
    }
}
