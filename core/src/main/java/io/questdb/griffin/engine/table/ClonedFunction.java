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

package io.questdb.griffin.engine.table;

import io.questdb.cairo.CairoException;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.groupby.GroupByUtils;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.QuietCloseable;
import org.jetbrains.annotations.Nullable;

/**
 * A function of a HORIZON JOIN that the owner thread evaluates, and its per-worker clones, which are null when the
 * function is thread-safe and every worker evaluates it. Owns both: initializes the clones over clones of the symbol
 * tables, and rewinds, notifies and frees them together with the function.
 */
final class ClonedFunction implements QuietCloseable {
    private @Nullable Function function;
    private @Nullable ObjList<Function> perWorkerFunctions;

    @Override
    public void close() {
        final Function function = this.function;
        this.function = null;
        final ObjList<Function> perWorkerFunctions = this.perWorkerFunctions;
        this.perWorkerFunctions = null;
        final Throwable cleanupFailure = Misc.freeBestEffort(null, function);
        CairoException.rethrowCleanupFailure(Misc.freeObjListBestEffort(cleanupFailure, perWorkerFunctions));
    }

    /**
     * Notifies the function and its clones that the cursor has closed, folding each failure into
     * {@code cleanupFailure}, which it returns.
     */
    Throwable cursorClosed(@Nullable Throwable cleanupFailure) {
        cleanupFailure = Function.cursorClosed(cleanupFailure, function);
        return Function.cursorClosed(cleanupFailure, perWorkerFunctions);
    }

    /**
     * The function the worker in {@code slotId} evaluates: its clone, or the function itself for the owner thread, slot
     * -1, and when the function has no clones.
     */
    @Nullable Function get(int slotId) {
        return slotId == -1 || perWorkerFunctions == null ? function : perWorkerFunctions.getQuick(slotId);
    }

    /**
     * The function the owner thread evaluates.
     */
    @Nullable Function getFunction() {
        return function;
    }

    /**
     * Initializes the function over {@code symbolTableSource}, and its clones over clones of its symbol tables. The
     * caller notifies them through {@link #cursorClosed}.
     */
    void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
        if (function != null) {
            function.init(symbolTableSource, executionContext);
        }
        if (perWorkerFunctions != null) {
            final boolean isCloneSymbolTables = executionContext.getCloneSymbolTables();
            executionContext.setCloneSymbolTables(true);
            try {
                Function.init(perWorkerFunctions, symbolTableSource, executionContext, function);
            } finally {
                executionContext.setCloneSymbolTables(isCloneSymbolTables);
            }
        }
    }

    void setFunction(@Nullable Function function) {
        this.function = function;
    }

    void setPerWorkerFunctions(@Nullable ObjList<Function> perWorkerFunctions) {
        this.perWorkerFunctions = perWorkerFunctions;
    }

    /**
     * Rewinds the function and its clones.
     */
    void toTop() {
        if (function != null) {
            function.toTop();
        }
        if (perWorkerFunctions != null) {
            GroupByUtils.toTop(perWorkerFunctions);
        }
    }
}
