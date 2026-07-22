/*+*****************************************************************************
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
import io.questdb.griffin.engine.functions.IntFunction;
import io.questdb.std.Numbers;

public final class IntFunctionMemoizer extends IntFunction implements MemoizerFunction {
    private final Function fn;
    private final boolean isIntWidthStable;
    private boolean isValidLongValue;
    private boolean isValidValue;
    private long longValue;
    private int value;

    public IntFunctionMemoizer(Function fn) {
        this.fn = fn;
        this.isIntWidthStable = fn.isIntWidthStable();
    }

    @Override
    public Function getArg() {
        return fn;
    }

    @Override
    public int getInt(Record rec) {
        if (!isValidValue) {
            value = fn.getInt(rec);
            isValidValue = true;
        }
        return value;
    }

    /**
     * Memoizes the long width separately. IntFunction.getLong() would widen the memoized INT,
     * truncating a delegate whose value only exists at long width - which is exactly the
     * disagreement between a stored value and an explicit cast that {@link Function#isIntWidthStable}
     * exists to prevent.
     */
    @Override
    public long getLong(Record rec) {
        if (isIntWidthStable) {
            return Numbers.intToLong(getInt(rec));
        }
        if (!isValidLongValue) {
            longValue = fn.getLong(rec);
            isValidLongValue = true;
        }
        return longValue;
    }

    @Override
    public boolean isIntWidthStable() {
        return isIntWidthStable;
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
        isValidValue = false;
        isValidLongValue = false;
    }

    @Override
    public boolean supportsRandomAccess() {
        return fn.supportsRandomAccess();
    }
}
