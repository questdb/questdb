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

import io.questdb.cairo.AbstractRecordCursorFactory;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.EmptyTableRandomRecordCursor;
import io.questdb.std.Misc;

/**
 * Gates an outer scan behind a whole-predicate boolean filter that is a runtime constant, i.e.
 * one that evaluates to a single value per query execution rather than per row (for example
 * {@code where (select b from x limit 1)} or {@code where not (select b from x limit 1)}).
 * <p>
 * The filter is re-evaluated at every cursor open - never baked at compile time - because a
 * runtime constant may differ between executions (bind variables, {@code now()}, changed
 * sub-query data). When the filter is:
 * <ul>
 *     <li>{@code true}: the gate delegates straight to the base cursor. Every base row matches,
 *     so no per-row predicate branch is needed;</li>
 *     <li>{@code false} (including NULL, which QuestDB's two-valued BOOLEAN renders as false):
 *     the gate returns an empty cursor without opening the base, so a false predicate costs no
 *     outer I/O.</li>
 * </ul>
 * The gate owns both the base factory and the filter and frees each exactly once on close,
 * including on the false path where the base cursor was never opened.
 */
public class RuntimeConstGateRecordCursorFactory extends AbstractRecordCursorFactory {
    private RecordCursorFactory base;
    private Function filter;

    public RuntimeConstGateRecordCursorFactory(RecordCursorFactory base, Function filter) {
        super(base.getMetadata());
        this.base = base;
        this.filter = filter;
    }

    @Override
    public RecordCursorFactory getBaseFactory() {
        return base;
    }

    @Override
    public RecordCursor getCursor(SqlExecutionContext executionContext) throws SqlException {
        // Re-evaluate the runtime constant for THIS execution; the sub-query is self-contained,
        // so it inits against a null symbol-table source (matching the join const-filter path).
        filter.init(null, executionContext);
        if (filter.getBool(null)) {
            return base.getCursor(executionContext);
        }
        // The empty result never iterates and never consults the circuit breaker on its own.
        // Honor cancellation once at open, so a gated-out query still observes a tripped breaker.
        executionContext.getCircuitBreaker().statefulThrowExceptionIfTrippedTimeThrottled();
        return EmptyTableRandomRecordCursor.INSTANCE;
    }

    @Override
    public Function getFilter() {
        return filter;
    }

    @Override
    public int getScanDirection() {
        return base.getScanDirection();
    }

    @Override
    public TableToken getTableToken() {
        return base.getTableToken();
    }

    @Override
    public boolean implementsLimit() {
        return base.implementsLimit();
    }

    // Stable iff the retained filter and the base are stable.
    @Override
    public boolean isNonDeterministic() {
        return filter.isNonDeterministic() || base.isNonDeterministic();
    }

    @Override
    public boolean isStableWithinExecution() {
        return filter.isStableWithinExecution() && base.isStableWithinExecution();
    }

    @Override
    public boolean recordCursorSupportsRandomAccess() {
        return base.recordCursorSupportsRandomAccess();
    }

    @Override
    public boolean supportsUpdateRowId(TableToken tableToken) {
        return base.supportsUpdateRowId(tableToken);
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.type("Filter");
        sink.meta("filter").val(filter);
        sink.child(base);
    }

    @Override
    public boolean usesCompiledFilter() {
        return base.usesCompiledFilter();
    }

    @Override
    public boolean usesIndex() {
        return base.usesIndex();
    }

    @Override
    protected void _close() {
        final RecordCursorFactory base = this.base;
        this.base = null;
        final Function filter = this.filter;
        this.filter = null;

        Throwable failure = Misc.freeBestEffort(null, base);
        failure = Misc.freeBestEffort(failure, filter);
        CairoException.rethrowCleanupFailure(failure);
    }
}
