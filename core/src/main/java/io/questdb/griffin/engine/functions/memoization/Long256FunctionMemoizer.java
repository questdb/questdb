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

import io.questdb.cairo.CairoException;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.Long256Function;
import io.questdb.griffin.engine.functions.UnaryFunction;
import io.questdb.std.Long256;
import io.questdb.std.Long256Impl;
import io.questdb.std.str.CharSink;

public final class Long256FunctionMemoizer extends Long256Function implements UnaryFunction {
    private final Function fn;
    private final Long256Impl valueLeft = new Long256Impl();
    private final Long256Impl valueRight = new Long256Impl();
    private Record recordLeft;
    private Record recordRight;

    public Long256FunctionMemoizer(Function fn) {
        assert fn.shouldMemoize();
        this.fn = fn;
    }

    @Override
    public Function getArg() {
        return fn;
    }

    @Override
    public void getLong256(Record rec, CharSink<?> sink) {
        if (recordLeft == rec) {
            valueLeft.toSink(sink);
            return;
        }
        if (recordRight == rec) {
            valueRight.toSink(sink);
            return;
        }
        fn.getLong256(rec, sink);
    }

    @Override
    public Long256 getLong256A(Record rec) {
        if (recordLeft == rec) {
            return valueLeft;
        }
        if (recordRight == rec) {
            return valueRight;
        }
        return fn.getLong256A(rec);
    }

    @Override
    public Long256 getLong256B(Record rec) {
        // B value is not memoized
        return fn.getLong256B(rec);
    }

    @Override
    public String getName() {
        return "memoize";
    }

    @Override
    public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
        recordLeft = null;
        recordRight = null;
        UnaryFunction.super.init(symbolTableSource, executionContext);
    }

    @Override
    public boolean isThreadSafe() {
        return false;
    }

    @Override
    public void memoize(Record record) {
        Long256 long256 = fn.getLong256A(record);
        if (recordLeft == record) {
            valueLeft.setAll(long256.getLong0(),
                    long256.getLong1(),
                    long256.getLong2(),
                    long256.getLong3());
        } else if (recordRight == record) {
            valueRight.setAll(long256.getLong0(),
                    long256.getLong1(),
                    long256.getLong2(),
                    long256.getLong3());
        } else if (recordLeft == null) {
            recordLeft = record;
            valueLeft.setAll(long256.getLong0(),
                    long256.getLong1(),
                    long256.getLong2(),
                    long256.getLong3());
        } else if (recordRight == null) {
            assert supportsRandomAccess();
            recordRight = record;
            valueRight.setAll(long256.getLong0(),
                    long256.getLong1(),
                    long256.getLong2(),
                    long256.getLong3());
        } else {
            throw CairoException.nonCritical().
                    put("Long256FunctionMemoizer can only memoize two records, but got more than two: [recordLeft=")
                    .put(recordLeft.toString())
                    .put(", recordRight=")
                    .put(recordRight.toString())
                    .put(", newRecord=")
                    .put(record.toString())
                    .put(']');
        }
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
