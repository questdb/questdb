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

import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.UnaryFunction;

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
