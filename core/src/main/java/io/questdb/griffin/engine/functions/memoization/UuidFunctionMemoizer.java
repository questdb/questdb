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
import io.questdb.cairo.sql.NullRecord;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.UnaryFunction;
import io.questdb.griffin.engine.functions.UuidFunction;

public final class UuidFunctionMemoizer extends UuidFunction implements UnaryFunction {
    private final Function fn;
    private long hiLeft;
    private long hiRight;
    private long loLeft;
    private long loRight;
    private Record recordLeft;
    private Record recordRight;

    public UuidFunctionMemoizer(Function fn) {
        assert fn.shouldMemoize();
        this.fn = fn;
    }

    @Override
    public Function getArg() {
        return fn;
    }

    @Override
    public long getLong128Hi(Record rec) {
        if (recordLeft == rec) {
            return hiLeft;
        }
        if (recordRight == rec) {
            return hiRight;
        }
        return fn.getLong128Hi(rec);
    }

    @Override
    public long getLong128Lo(Record rec) {
        if (recordLeft == rec) {
            return loLeft;
        }
        if (recordRight == rec) {
            return loRight;
        }
        return fn.getLong128Lo(rec);
    }

    @Override
    public String getName() {
        return "memoize";
    }

    @Override
    public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
        recordLeft = NullRecord.INSTANCE;
        recordRight = NullRecord.INSTANCE;
        UnaryFunction.super.init(symbolTableSource, executionContext);
    }

    @Override
    public boolean isThreadSafe() {
        return false;
    }

    @Override
    public void memoize(Record record) {
        if (recordLeft == record) {
            loLeft = fn.getLong128Lo(record);
            hiLeft = fn.getLong128Hi(record);
        } else if (recordRight == record) {
            loRight = fn.getLong128Lo(record);
            hiRight = fn.getLong128Hi(record);
        } else if (recordLeft == NullRecord.INSTANCE) {
            recordLeft = record;
            loLeft = fn.getLong128Lo(record);
            hiLeft = fn.getLong128Hi(record);
        } else if (recordRight == NullRecord.INSTANCE) {
            assert supportsRandomAccess();
            recordRight = record;
            loRight = fn.getLong128Lo(record);
            hiRight = fn.getLong128Hi(record);
        } else {
            throw CairoException.nonCritical().
                    put("UuidFunctionMemoizer can only memoize two records, but got more than two: [recordLeft=")
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
