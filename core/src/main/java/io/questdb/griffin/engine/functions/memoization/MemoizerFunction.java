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

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.UnaryFunction;
import org.jetbrains.annotations.Nullable;

/**
 * A function that computes its argument once per row and answers every later getter of that
 * row from the cached result. The cache is a value, not a position: nothing in it identifies
 * the row it came from, so a reader that never clears it cannot tell a stale value from a
 * fresh one.
 * <p>
 * Clearing is therefore the owning cursor's job on every row, and this interface's job on
 * every event that moves the cursor without producing a row - {@link #init} on a rebind and
 * {@link #toTop} on a rewind. Both are defaults here rather than a convention each cursor
 * has to keep, because the failure they prevent is silent: a projection read after a rebind
 * that skipped the clear serves the previous traversal's last value, with no fault and no
 * type error to notice.
 */
public interface MemoizerFunction extends UnaryFunction {

    /**
     * The memoizer of the function's type over the function, or null when its type has none.
     */
    static @Nullable MemoizerFunction newInstance(Function function) {
        return switch (ColumnType.tagOf(function.getType())) {
            case ColumnType.LONG -> new LongFunctionMemoizer(function);
            case ColumnType.INT -> new IntFunctionMemoizer(function);
            case ColumnType.TIMESTAMP -> new TimestampFunctionMemoizer(function);
            case ColumnType.DOUBLE -> new DoubleFunctionMemoizer(function);
            case ColumnType.SHORT -> new ShortFunctionMemoizer(function);
            case ColumnType.BOOLEAN -> new BooleanFunctionMemoizer(function);
            case ColumnType.BYTE -> new ByteFunctionMemoizer(function);
            case ColumnType.CHAR -> new CharFunctionMemoizer(function);
            case ColumnType.DATE -> new DateFunctionMemoizer(function);
            case ColumnType.FLOAT -> new FloatFunctionMemoizer(function);
            case ColumnType.IPv4 -> new IPv4FunctionMemoizer(function);
            case ColumnType.UUID -> new UuidFunctionMemoizer(function);
            case ColumnType.LONG256 -> new Long256FunctionMemoizer(function);
            case ColumnType.DECIMAL8, ColumnType.DECIMAL16, ColumnType.DECIMAL32,
                 ColumnType.DECIMAL64, ColumnType.DECIMAL128, ColumnType.DECIMAL256 ->
                    new DecimalFunctionMemoizer(function);
            case ColumnType.ARRAY -> new ArrayFunctionMemoizer(function);
            case ColumnType.STRING -> new StrFunctionMemoizer(function);
            case ColumnType.VARCHAR, ColumnType.VARCHAR_SLICE -> new VarcharFunctionMemoizer(function);
            case ColumnType.SYMBOL -> new SymbolFunctionMemoizer(function);
            case ColumnType.GEOBYTE, ColumnType.GEOSHORT, ColumnType.GEOINT, ColumnType.GEOLONG ->
                    new GeoHashFunctionMemoizer(function);
            case ColumnType.BINARY -> new BinFunctionMemoizer(function);
            case ColumnType.LONG128 -> new Long128FunctionMemoizer(function);
            case ColumnType.INTERVAL -> new IntervalFunctionMemoizer(function);
            default -> null;
        };
    }

    /**
     * Drops the cached value, so the next getter recomputes it from the record it is handed.
     */
    void clearMemo();

    @Override
    default void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
        clearMemo();
        UnaryFunction.super.init(symbolTableSource, executionContext);
    }

    @Override
    default void toTop() {
        clearMemo();
        UnaryFunction.super.toTop();
    }
}
