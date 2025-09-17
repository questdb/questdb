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
import io.questdb.griffin.engine.functions.TimestampFunction;
import io.questdb.griffin.engine.functions.UnaryFunction;

public final class TimestampFunctionMemoizer extends TimestampFunction implements UnaryFunction {
    private final Function fn;
    private Record recordLeft;
    private Record recordRight;
    private long valueLeft;
    private long valueRight;

    public TimestampFunctionMemoizer(Function fn) {
        super(fn.getType());
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
    public long getTimestamp(Record rec) {
        if (recordLeft == rec) {
            return valueLeft;
        }
        if (recordRight == rec) {
            return valueRight;
        }
        return fn.getTimestamp(rec);
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
            valueLeft = fn.getTimestamp(record);
        } else if (recordRight == record) {
            valueRight = fn.getTimestamp(record);
        } else if (recordLeft == NullRecord.INSTANCE) {
            recordLeft = record;
            valueLeft = fn.getTimestamp(record);
        } else if (recordRight == NullRecord.INSTANCE) {
            assert supportsRandomAccess();
            recordRight = record;
            valueRight = fn.getTimestamp(record);
        } else {
            throw CairoException.nonCritical().
                    put("TimestampFunctionMemoizer can only memoize two records, but got more than two: [recordLeft=")
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
