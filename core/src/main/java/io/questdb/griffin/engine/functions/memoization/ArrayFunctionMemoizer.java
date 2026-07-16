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

import io.questdb.cairo.arr.ArrayView;
import io.questdb.cairo.arr.DerivedArrayView;
import io.questdb.cairo.sql.ArrayFunction;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.Misc;

public final class ArrayFunctionMemoizer extends ArrayFunction implements MemoizerFunction {
    private final DerivedArrayView derivedArray = new DerivedArrayView();
    private final Function fn;
    private boolean validValue;

    public ArrayFunctionMemoizer(Function fn) {
        this.fn = fn;
    }

    @Override
    public void close() {
        MemoizerFunction.super.close();
        Misc.free(derivedArray);
    }

    @Override
    public Function getArg() {
        return fn;
    }

    @Override
    public ArrayView getArray(Record rec) {
        if (!validValue) {
            // getArray() never hands back a Java null - a producer with no array returns a NULL
            // ArrayView. Absorbing a null here would launder a broken producer past the asserts
            // Record.getArrayDimLen()/getArrayDouble1d2d() rely on to catch exactly that.
            ArrayView view = fn.getArray(rec);
            assert view != null : "getArray() returned a Java null, expected a NULL ArrayView";
            derivedArray.of(view);
            validValue = true;
        }
        return derivedArray;
    }

    @Override
    public String getName() {
        return "memoize";
    }

    /**
     * Delegates rather than reporting {@link ArrayFunction}'s own {@code type} field, which nothing
     * here ever sets and which would therefore read {@code UNDEFINED}. Dimensionality is encoded in
     * the type, so a caller that decoded it off the wrapper would see zero dimensions and reject
     * every index.
     * <p>
     * This is hardening, not a fix for a known query: the code generator only ever swaps a memoizer
     * in for a top-level projection function, whose arguments were bound before the swap, so no
     * argument tree can hold one - and the callers that do decode dimensionality reach the column
     * through {@code ColumnFunction.unwrap()}, which peels memoizers off first.
     * <p>
     * Delegating rather than snapshotting in the constructor also keeps the answer right for a
     * wrapped array bind variable, whose type is weak-dimensioned until a value is bound.
     */
    @Override
    public int getType() {
        return fn.getType();
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
        validValue = false;
    }

    @Override
    public boolean supportsRandomAccess() {
        return fn.supportsRandomAccess();
    }
}
