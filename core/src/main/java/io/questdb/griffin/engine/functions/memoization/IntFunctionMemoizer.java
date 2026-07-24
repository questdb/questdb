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
    private final boolean isRowStable;
    private boolean isValidLongValue;
    private boolean isValidValue;
    private long longValue;
    private int value;

    public IntFunctionMemoizer(Function fn) {
        this.fn = fn;
        this.isIntWidthStable = fn.isIntWidthStable();
        this.isRowStable = fn.isRowStable();
    }

    @Override
    public Function getArg() {
        return fn;
    }

    /**
     * A width-unstable delegate carries two values per row, so the widths memoize separately -
     * except when the delegate also redraws per read. There the two reads would be two different
     * draws, which is exactly what a memoizer exists to prevent, so the delegate is evaluated once
     * at long width and the INT width is narrowed from it.
     * <p>
     * That narrowing is a CHOICE of authoritative width, not a lossless derivation. It is exact for
     * the operators whose two widths agree on their low 32 bits - {@code + - *} and the bitwise
     * ones, a modular ring homomorphism - but {@code /} and {@code %} break even that, and so does
     * the branch a conditional picks: {@code COALESCE} tests nullness at INT width in
     * {@code getInt()} and at long width in {@code getLong()}, {@code NULLIF} moves its equality
     * comparison the same way, and {@code CASE} re-runs its picker. See the
     * worked counterexample in {@code RuntimeConstFunction}'s int arm:
     * {@code (1000000 * 1000000) / 7} wraps to {@code -103911424} under {@code getInt()} while its
     * wide value {@code 142857142857} narrows to {@code 1123222089}.
     * <p>
     * Taking the long width as authoritative is nonetheless the right resolution, and the same one
     * {@link io.questdb.griffin.engine.functions.bool.InLongFunctionFactory} and
     * {@code CoalesceFunctionFactory} reach: a row-unstable INT expression has no single stable
     * value to wrap anyway, so there is no int-width answer to preserve - only a second draw. The
     * reverse choice, the pre-memoization {@code getLong() = Numbers.intToLong(getInt())}, would
     * throw the wide half away and re-break {@code alias::LONG}.
     * <p>
     * A delegate whose two widths are independent rather than a wrap of one another
     * ({@code json_extract}) is deterministic, so it takes the row-stable branch and keeps both.
     */
    @Override
    public int getInt(Record rec) {
        if (!isIntWidthStable && !isRowStable) {
            final long l = getLong(rec);
            return l != Numbers.LONG_NULL ? (int) l : Numbers.INT_NULL;
        }
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
    public String getName() {
        return "memoize";
    }

    @Override
    public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
        MemoizerFunction.super.init(symbolTableSource, executionContext);
    }

    @Override
    public boolean isIntWidthStable() {
        return isIntWidthStable;
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
