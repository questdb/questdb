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
import io.questdb.cairo.ReaderScanProfile;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.sql.ColumnMapping;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.PageFrame;
import io.questdb.cairo.sql.PageFrameCursor;
import io.questdb.cairo.sql.PartitionFrameCursor;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.StaticSymbolTable;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.EmptyTableRandomRecordCursor;
import io.questdb.std.Misc;
import org.jetbrains.annotations.Nullable;

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
 * including on the record-cursor false path where the base cursor was never opened.
 * <p>
 * The gate also preserves the base's page-frame capability: {@link #supportsPageFrameCursor()}
 * reports the base's value, so a parent parallel/vectorized operator (count/sum aggregation,
 * parallel GROUP BY, horizon join, TopK) keeps its page-frame path instead of falling back to
 * serial. On {@link #getPageFrameCursor} the true path delegates straight to the base cursor
 * (full parallel scan), while the false path returns a wrapper that yields zero frames. That
 * false page-frame wrapper opens a real base cursor so the metadata accessors
 * ({@code getColumnMapping()}, {@code getSymbolTable()}) honor their contracts during a parallel
 * consumer's setup - acquiring the base reader but lifting no column data. Only the rare false
 * page-frame path pays that reader acquisition; the common false record-cursor path stays fully
 * zero-I/O.
 * <p>
 * The false wrapper claims only the capability the base actually provides: a table base gets a
 * {@link TablePageFrameCursor} wrapper (parents such as a projection downcast to that surface),
 * while a non-table base such as {@code read_parquet()} - whose page-frame cursor is a plain
 * {@link PageFrameCursor} - gets a plain wrapper, so the gate never advertises a table contract
 * it cannot honor.
 */
public class RuntimeConstGateRecordCursorFactory extends AbstractRecordCursorFactory {
    private RecordCursorFactory base;
    private EmptyPageFrameCursor emptyPageFrameCursor;
    private EmptyTablePageFrameCursor emptyTablePageFrameCursor;
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
    public PageFrameCursor getPageFrameCursor(SqlExecutionContext executionContext, int order) throws SqlException {
        // Re-evaluate the runtime constant for THIS execution, same contract as getCursor. A
        // consumer calls exactly one of getCursor()/getPageFrameCursor() per execution.
        filter.init(null, executionContext);
        if (filter.getBool(null)) {
            // TRUE: delegate straight to the base page-frame cursor so the base's full parallel /
            // vectorized scan is preserved with zero wrapper overhead.
            return base.getPageFrameCursor(executionContext, order);
        }
        // The empty result never iterates and never consults the circuit breaker on its own.
        // Honor cancellation once at open, so a gated-out query still observes a tripped breaker.
        executionContext.getCircuitBreaker().statefulThrowExceptionIfTrippedTimeThrottled();
        // FALSE: open a real base page-frame cursor so getColumnMapping()/getSymbolTable()/
        // newSymbolTable() honor their contracts during a parallel consumer's setup, then wrap it
        // to yield ZERO frames so no column data is ever lifted. This acquires the base reader on
        // the rare false page-frame path; the common false record-cursor path (getCursor) stays
        // fully zero-I/O.
        final PageFrameCursor baseCursor = base.getPageFrameCursor(executionContext, order);
        try {
            // Claim only what the base provides: a table base keeps the TablePageFrameCursor
            // surface (parents such as a projection downcast to it), while a non-table base
            // (e.g. read_parquet) gets a plain PageFrameCursor wrapper instead of a
            // getTableReader()/hasIntervalFilter()/toPartition() contract it cannot honor.
            if (baseCursor instanceof TablePageFrameCursor tableBaseCursor) {
                if (emptyTablePageFrameCursor == null) {
                    emptyTablePageFrameCursor = new EmptyTablePageFrameCursor();
                }
                return emptyTablePageFrameCursor.of(tableBaseCursor);
            }
            if (emptyPageFrameCursor == null) {
                emptyPageFrameCursor = new EmptyPageFrameCursor();
            }
            return emptyPageFrameCursor.of(baseCursor);
        } catch (Throwable th) {
            Misc.free(baseCursor);
            throw th;
        }
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

    // The gate keeps the base's page-frame capability: TRUE delegates to the base cursor (full
    // parallel scan), FALSE returns an empty page-frame cursor. Reporting the base's value lets a
    // parent parallel/vectorized operator keep its page-frame path instead of falling back to
    // serial.
    @Override
    public boolean supportsPageFrameCursor() {
        return base.supportsPageFrameCursor();
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
        final EmptyPageFrameCursor emptyPageFrameCursor = this.emptyPageFrameCursor;
        this.emptyPageFrameCursor = null;
        final EmptyTablePageFrameCursor emptyTablePageFrameCursor = this.emptyTablePageFrameCursor;
        this.emptyTablePageFrameCursor = null;
        final RecordCursorFactory base = this.base;
        this.base = null;
        final Function filter = this.filter;
        this.filter = null;

        Throwable failure = Misc.freeBestEffort(null, emptyPageFrameCursor);
        failure = Misc.freeBestEffort(failure, emptyTablePageFrameCursor);
        failure = Misc.freeBestEffort(failure, base);
        failure = Misc.freeBestEffort(failure, filter);
        CairoException.rethrowCleanupFailure(failure);
    }

    /**
     * Wraps a real base page-frame cursor to expose an EMPTY scan for the gate's false path.
     * Every frame-producing method reports empty so no column data is ever lifted, while the
     * metadata accessors delegate to the base cursor so a parallel consumer's setup contract is
     * honored. Opening the base cursor acquires its reader; the wrapper releases it on close.
     * <p>
     * This wrapper claims only the plain {@link PageFrameCursor} surface, so it can sit over any
     * base (e.g. read_parquet()). When the base cursor is a {@link TablePageFrameCursor}, the
     * gate hands out {@link EmptyTablePageFrameCursor} instead so parents that downcast to the
     * table surface keep working.
     */
    private static class EmptyPageFrameCursor implements PageFrameCursor {
        private PageFrameCursor baseCursor;

        @Override
        public void calculateSize(RecordCursor.Counter counter) {
            // Empty scan: no rows to add to the counter.
        }

        @Override
        public void close() {
            baseCursor = Misc.free(baseCursor);
        }

        @Override
        public ColumnMapping getColumnMapping() {
            return baseCursor.getColumnMapping();
        }

        @Override
        public long getRemainingRowsInInterval() {
            return 0;
        }

        @Override
        public StaticSymbolTable getSymbolTable(int columnIndex) {
            return baseCursor.getSymbolTable(columnIndex);
        }

        @Override
        public boolean isExternal() {
            return baseCursor.isExternal();
        }

        @Override
        public SymbolTable newSymbolTable(int columnIndex) {
            return baseCursor.newSymbolTable(columnIndex);
        }

        @Override
        public @Nullable PageFrame next(long skipTarget) {
            return null;
        }

        @Override
        public void releaseOpenPartitions() {
            baseCursor.releaseOpenPartitions();
        }

        @Override
        public void setScanProfile(ReaderScanProfile profile) {
            baseCursor.setScanProfile(profile);
        }

        @Override
        public long size() {
            return 0;
        }

        @Override
        public boolean supportsSizeCalculation() {
            return true;
        }

        @Override
        public void toTop() {
            // Empty scan: nothing to rewind.
        }

        private EmptyPageFrameCursor of(PageFrameCursor baseCursor) {
            this.baseCursor = baseCursor;
            return this;
        }
    }

    /**
     * The gate's false-path wrapper over a table base: extends the empty scan with the
     * {@link TablePageFrameCursor} surface, delegating the table-specific methods to the typed
     * base cursor - the same reader the TRUE path would expose. The gate hands this wrapper out
     * only when the base cursor is a {@link TablePageFrameCursor}, so no cast can fail.
     */
    private static final class EmptyTablePageFrameCursor extends EmptyPageFrameCursor implements TablePageFrameCursor {
        private TablePageFrameCursor tableBaseCursor;

        @Override
        public void close() {
            tableBaseCursor = null;
            super.close();
        }

        @Override
        public TableReader getTableReader() {
            return tableBaseCursor.getTableReader();
        }

        @Override
        public boolean hasIntervalFilter() {
            return tableBaseCursor.hasIntervalFilter();
        }

        // This wrapper is initialized via of(TablePageFrameCursor), not via
        // of(SqlExecutionContext, PartitionFrameCursor). The base factory's getPageFrameCursor()
        // already handles partition-level initialization; we only wrap its result.
        @Override
        public TablePageFrameCursor of(SqlExecutionContext executionContext, PartitionFrameCursor partitionFrameCursor) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void toPartition(int partitionIndex) {
            tableBaseCursor.toPartition(partitionIndex);
        }

        private EmptyTablePageFrameCursor of(TablePageFrameCursor baseCursor) {
            this.tableBaseCursor = baseCursor;
            super.of(baseCursor);
            return this;
        }
    }
}
