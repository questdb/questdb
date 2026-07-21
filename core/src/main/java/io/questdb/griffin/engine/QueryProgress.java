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

package io.questdb.griffin.engine;

import io.questdb.cairo.AbstractRecordCursorFactory;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ReaderScanProfile;
import io.questdb.cairo.SqlJitMode;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.pool.ResourcePoolSupervisor;
import io.questdb.cairo.sql.ColumnMapping;
import io.questdb.cairo.sql.PageFrame;
import io.questdb.cairo.sql.PageFrameCursor;
import io.questdb.cairo.sql.ParquetDecodeHint;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.StaticSymbolTable;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.cairo.sql.TimeFrameCursor;
import io.questdb.griffin.engine.table.ConcurrentTimeFrameCursor;
import io.questdb.cairo.sql.async.PageFrameSequence;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.QueryRegistry;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.log.LogRecord;
import io.questdb.metrics.QueryTrace;
import io.questdb.mp.SCSequence;
import io.questdb.std.Chars;
import io.questdb.std.FlyweightMessageContainer;
import io.questdb.std.IntHashSet;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

// Factory that adds query to registry on getCursor() and removes on cursor close().
public class QueryProgress extends AbstractRecordCursorFactory implements ResourcePoolSupervisor<TableReader> {
    // this field is modified via reflection from tests, via LogFactory.enableGuaranteedLogging
    @SuppressWarnings("FieldMayBeFinal")
    private static Log LOG = LogFactory.getLog(QueryProgress.class);
    private RecordCursorFactory base;
    private RegisteredRecordCursor cursor;
    private final boolean jit;
    private RegisteredPageFrameCursor pageFrameCursor;
    private final QueryTrace queryTrace = new QueryTrace();
    private final ObjList<TableReader> readers = new ObjList<>();
    private final QueryRegistry registry;
    private long beginNanos;
    private SqlExecutionContext executionContext;
    private long sqlId;

    public QueryProgress(QueryRegistry registry, CharSequence sqlText, RecordCursorFactory base) {
        super(base.getMetadata());
        this.base = base;
        this.registry = registry;
        this.cursor = new RegisteredRecordCursor();
        this.pageFrameCursor = new RegisteredPageFrameCursor();
        this.jit = base.usesCompiledFilter();
        queryTrace.queryText = Chars.toString(sqlText);
    }

    public static void logEnd(
            long sqlId,
            @NotNull CharSequence sqlText,
            @NotNull SqlExecutionContext executionContext,
            long beginNanos
    ) {
        logEnd(sqlId, sqlText, executionContext, beginNanos, null, null);
    }

    public static void logEnd(
            long sqlId,
            CharSequence sqlText,
            @NotNull SqlExecutionContext executionContext,
            long beginNanos,
            @Nullable ObjList<TableReader> leakedReaders,
            @Nullable QueryTrace queryTrace
    ) {
        final int leakedReadersCount = leakedReaders != null ? leakedReaders.size() : 0;
        // Validation only compiles the SQL to check it; suppress the normal query-progress
        // line so the validation endpoint does not pollute the log. Still report reader leaks,
        // as those indicate a real bug regardless of validation mode.
        if ((!executionContext.shouldLogSql() && leakedReadersCount == 0) || sqlText == null) {
            return;
        }

        CairoEngine engine = executionContext.getCairoEngine();
        CairoConfiguration config = engine.getConfiguration();
        long durationNanos = config.getNanosecondClock().getTicks() - beginNanos;
        boolean isJit = executionContext.getJitMode() != SqlJitMode.JIT_MODE_DISABLED;

        CharSequence principal = executionContext.getSecurityContext().getPrincipal();
        LogRecord log = null;
        try {
            if (leakedReadersCount > 0) {
                log = LOG.errorW();
                executionContext.getCairoEngine().getMetrics().healthMetrics()
                        .incrementReaderLeakCounter(leakedReadersCount);
                log.$("brk");
            } else {
                log = LOG.info();
                log.$("fin");
            }
            log.$(" [id=").$(sqlId)
                    .$(", sql=`").$safe(sqlText)
                    .$("`, ").$(executionContext)
                    .$(", jit=").$(isJit)
                    .$(", time=").$(durationNanos);

            appendLeakedReaderNames(leakedReaders, leakedReadersCount, log);
        } catch (Throwable e) {
            // Game over, we can't log anything
            System.err.print("could not log exception message");
            e.printStackTrace(System.err);
        } finally {
            if (log != null) {
                log.I$();
            }
        }
        // When queryTrace is not null, queryTrace.queryText is already set and equal to sqlText,
        // as well as already converted to an immutable String, as needed to queue it up for handling
        // at a later time. For this reason, do not assign queryTrace.queryText = sqlText here.
        if (queryTrace != null && executionContext.shouldLogSql() && engine.getConfiguration().isQueryTracingEnabled()) {
            queryTrace.executionNanos = durationNanos;
            queryTrace.isJit = isJit;
            queryTrace.timestamp = config.getMicrosecondClock().getTicks();
            queryTrace.principal = principal.toString();
            engine.getMessageBus().getQueryTraceQueue().enqueue(queryTrace);
        }
    }

    public static void logError(
            @Nullable Throwable e,
            long sqlId,
            @NotNull CharSequence sqlText,
            @NotNull SqlExecutionContext executionContext,
            long beginNanos
    ) {
        logError(e, sqlId, sqlText, executionContext, beginNanos, null);
    }

    public static void logError(
            @Nullable Throwable e,
            long sqlId,
            CharSequence sqlText,
            @NotNull SqlExecutionContext executionContext,
            long beginNanos,
            @Nullable ObjList<TableReader> leakedReaders
    ) {
        int leakedReadersCount = leakedReaders != null ? leakedReaders.size() : 0;
        // Validation only compiles the SQL to check it; a validation failure is reported to the
        // client, so do not log it as a server-side query error or inflate the error metrics.
        // Still report reader leaks, as those indicate a real bug regardless of validation mode.
        if (executionContext.isValidationOnly() && leakedReadersCount == 0) {
            return;
        }
        LogRecord log = null;
        try {
            // A validation failure is reported to the client, not as a server-side query error,
            // so do not inflate the query-error metric for it. A reader leak is still counted
            // below regardless of validation mode, as it indicates a real bug.
            if (!executionContext.isValidationOnly()) {
                executionContext.getCairoEngine().getMetrics().healthMetrics().incrementQueryErrorCounter();
            }
            // Extract all the variables before the call to call LOG.errorW() to avoid exception
            // causing log sequence leaks.
            long durationNanos =
                    executionContext.getCairoEngine().getConfiguration().getNanosecondClock().getTicks() - beginNanos;
            log = LOG.errorW();
            if (leakedReadersCount > 0) {
                log.$("brk");
                executionContext.getCairoEngine().getMetrics().healthMetrics().incrementReaderLeakCounter(leakedReadersCount);
            } else {
                log.$("err");
            }
            if (e instanceof FlyweightMessageContainer) {
                final int pos = ((FlyweightMessageContainer) e).getPosition();
                final int errno = e instanceof CairoException ? ((CairoException) e).getErrno() : 0;
                final CharSequence message = ((FlyweightMessageContainer) e).getFlyweightMessage();
                // We need guaranteed logging for errors, hence errorW() call.

                log.$(" [id=").$(sqlId)
                        .$(", sql=`").$safe(sqlText == null ? "**subquery**" : sqlText)
                        .$("`, ").$(executionContext)
                        .$(", jit=").$(executionContext.getJitMode() != SqlJitMode.JIT_MODE_DISABLED)
                        .$(", time=").$(durationNanos)
                        .$(", msg=").$safe(message)
                        .$(", errno=").$(errno)
                        .$(", pos=").$(pos);
            } else {
                // This is unknown exception, can be OOM that can cause exception in logging.
                log.$(" [id=").$(sqlId)
                        .$(", sql=`").$safe(sqlText == null ? "**subquery**" : sqlText)
                        .$("`, ").$(executionContext)
                        .$(", jit=").$(executionContext.getJitMode() != SqlJitMode.JIT_MODE_DISABLED)
                        .$(", time=").$(durationNanos)
                        .$(", exception=").$(e);
            }
            appendLeakedReaderNames(leakedReaders, leakedReadersCount, log);
        } catch (Throwable th) {
            // Game over, we can't log anything
            System.err.print("Could not log exception message! ");
            th.printStackTrace(System.err);
        } finally {
            // Make sure logging sequence is always released.
            if (log != null) {
                log.I$();
            }
        }
    }

    public static void logStart(
            long sqlId,
            CharSequence sqlText,
            @NotNull SqlExecutionContext executionContext,
            boolean jit
    ) {
        if (executionContext.shouldLogSql() && executionContext.getCairoEngine().getConfiguration().getLogSqlQueryProgressExe() && sqlText != null) {
            LOG.info()
                    .$("exe")
                    .$(" [id=").$(sqlId)
                    .$(", sql=`").$safe(sqlText)
                    .$("`, ").$(executionContext)
                    .$(", jit=").$(jit)
                    .I$();
        }
    }

    @Override
    public PageFrameSequence<?> execute(SqlExecutionContext executionContext, SCSequence collectSubSeq, int order) throws SqlException {
        return base.execute(executionContext, collectSubSeq, order);
    }

    @Override
    public boolean followedOrderByAdvice() {
        return base.followedOrderByAdvice();
    }

    @Override
    public boolean fragmentedSymbolTables() {
        return base.fragmentedSymbolTables();
    }

    @Override
    public String getBaseColumnName(int idx) {
        return base.getBaseColumnName(idx);
    }

    @Override
    public RecordCursorFactory getBaseFactory() {
        return base;
    }

    @Override
    public RecordCursor getCursor(SqlExecutionContext executionContext) throws SqlException {
        if (!cursor.isOpen) {
            this.executionContext = executionContext;
            CharSequence sqlText = queryTrace.queryText;
            sqlId = registry.register(sqlText, executionContext);
            beginNanos = executionContext.getCairoEngine().getConfiguration().getNanosecondClock().getTicks();
            logStart(sqlId, sqlText, executionContext, jit);
            // Install this factory as the reader-pool supervisor for the duration of cursor
            // open, so every table reader the query borrows while building the cursor is
            // attributed to it for leak detection. The supervisor lives on the
            // SqlExecutionContext (not a carrier/thread local) so it survives a continuation
            // that parks inside base.getCursor() and resumes on a different worker -- the
            // displaced value is restored relative to the same context object regardless of
            // which carrier finishes the open. Open runs synchronously per connection (a
            // parked open does not let another statement open a cursor on the same context),
            // so this set/restore is strictly nested and is safe on the shared per-connection
            // context. Readers opened later, during fetch, are not supervised here -- the
            // same limitation as before this change -- because fetch can interleave across
            // PGWire portals, which a single context supervisor slot cannot model.
            final ResourcePoolSupervisor<TableReader> prevSupervisor = executionContext.getReaderPoolSupervisor();
            executionContext.setReaderPoolSupervisor(this);
            try {
                final RecordCursor baseCursor = base.getCursor(executionContext);
                cursor.of(baseCursor); // this should not fail, it is just variable assignment
            } catch (Throwable th) {
                cursor.close0(th);
                throw th;
            } finally {
                executionContext.setReaderPoolSupervisor(prevSupervisor);
            }
        }
        return cursor;
    }

    @Override
    public PageFrameCursor getPageFrameCursor(SqlExecutionContext executionContext, int order) throws SqlException {
        if (!base.supportsPageFrameCursor()) {
            return null;
        }
        // IMPORTANT: getPageFrameCursor() and getCursor() are mutually exclusive in QueryProgress because it is TOP RecordCursorFactory.
        // For streaming parquet exports, the caller may directly call getPageFrameCursor()
        // instead of getCursor() to obtain PageFrame-based access to the data.
        // Since these two methods are never called together in the same query execution,
        // we must ensure that query registration (registry.register) and logging (logStart)
        // are performed here as well, not just in getCursor().
        // This ensures proper query tracking, cancellation support, and resource leak detection
        // for both cursor-based and PageFrame-based data access paths.
        if (!pageFrameCursor.isOpen) {
            this.executionContext = executionContext;
            CharSequence sqlText = queryTrace.queryText;
            sqlId = registry.register(sqlText, executionContext);
            beginNanos = executionContext.getCairoEngine().getConfiguration().getNanosecondClock().getTicks();
            logStart(sqlId, sqlText, executionContext, jit);
            // See getCursor: supervise only the synchronous cursor-open window, on the
            // context so it survives a cont park/resume, and restore on return.
            final ResourcePoolSupervisor<TableReader> prevSupervisor = executionContext.getReaderPoolSupervisor();
            executionContext.setReaderPoolSupervisor(this);
            try {
                final PageFrameCursor baseCursor = base.getPageFrameCursor(executionContext, order);
                pageFrameCursor.of(baseCursor);
            } catch (Throwable th) {
                pageFrameCursor.close0(th);
                throw th;
            } finally {
                executionContext.setReaderPoolSupervisor(prevSupervisor);
            }
        }
        return pageFrameCursor;
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
    public TimeFrameCursor getTimeFrameCursor(SqlExecutionContext executionContext) throws SqlException {
        return base.getTimeFrameCursor(executionContext);
    }

    @Override
    public boolean implementsLimit() {
        return base.implementsLimit();
    }

    @Override
    public ConcurrentTimeFrameCursor newTimeFrameCursor() {
        return base.newTimeFrameCursor();
    }

    @Override
    public void onResourceBorrowed(TableReader resource) {
        readers.add(resource);
    }

    @Override
    public void onResourceReturned(TableReader resource) {
        int index = readers.remove(resource);
        // After our cursor closes, unregisterAndCleanup() has already cleared the list, so a
        // late return naturally finds nothing -- expected, do not log it. _close() nulls the
        // cursor fields before it closes the base factory chain, and that chain returns any
        // reader still borrowed after a failed cursor open re-entrantly through this method,
        // so a nulled field also means the query is shutting down and the return is expected.
        final RegisteredRecordCursor cursor = this.cursor;
        final RegisteredPageFrameCursor pageFrameCursor = this.pageFrameCursor;
        final boolean isCursorOpen = (cursor != null && cursor.isOpen)
                || (pageFrameCursor != null && pageFrameCursor.isOpen);
        if (index < 0 && isCursorOpen) {
            // Still open but the reader is untracked: our leak bookkeeping is inconsistent.
            // This is NOT pool-entry reuse (a previous hypothesis): R.close() detaches the
            // supervisor (sets it to null) before returnToPool, so a recycled entry cannot
            // deliver a stale return into this query. In practice it means a reader was
            // attributed to this query and then removed out of band -- e.g. unsupported
            // concurrent use of one SqlExecutionContext across statements, which routes
            // another query's borrows through this supervisor slot. Log it as a diagnostic.
            LOG.critical().$("returned reader is not in supervisor's list [tableName=")
                    .$(resource.getTableToken()).I$();
        }
    }

    @Override
    public boolean recordCursorSupportsRandomAccess() {
        return base.recordCursorSupportsRandomAccess();
    }

    @Override
    public boolean supportsPageFrameCursor() {
        return base.supportsPageFrameCursor();
    }

    @Override
    public boolean supportsTimeFrameCursor() {
        return base.supportsTimeFrameCursor();
    }

    @Override
    public boolean supportsUpdateRowId(TableToken tableName) {
        return base.supportsUpdateRowId(tableName);
    }

    @Override
    public void toPlan(PlanSink sink) {
        base.toPlan(sink);
    }

    @Override
    public boolean usesCompiledFilter() {
        return base.usesCompiledFilter();
    }

    private static void appendLeakedReaderNames(ObjList<TableReader> leakedReaders, int leakedReadersCount, LogRecord log) {
        for (int i = 0; i < leakedReadersCount; i++) {
            log.$(", leaked=").$(leakedReaders.getQuick(i).getTableToken());
        }
    }

    private void unregisterAndCleanup(@Nullable Throwable th) {
        // When execution context is null, the cursor has never been opened.
        // Otherwise, cursor open attempt has been made, but may not have fully succeeded.
        // In this case we must be certain that we still track the reader leak
        if (executionContext != null) {
            try {
                String sqlText = queryTrace.queryText;
                if (th == null) {
                    logEnd(sqlId, sqlText, executionContext, beginNanos, readers, queryTrace);
                } else {
                    logError(th, sqlId, sqlText, executionContext, beginNanos, readers);
                }
            } finally {
                // Unregister must follow the base cursor close call to avoid concurrent access
                // to cleaned up circuit breaker.
                registry.unregister(sqlId, executionContext);
                if (executionContext.getCairoEngine().getConfiguration().freeLeakedReaders()) {
                    // Each leaked reader still references this QueryProgress as its pool supervisor,
                    // so closing it re-enters onResourceReturned() -> readers.remove(). A fixed-index
                    // walk (Misc.freeObjListAndClear) would resize the list mid-iteration and skip
                    // every other reader. Detach each entry from the list BEFORE freeing it, so the
                    // re-entrant remove() is a harmless no-op and no leaked reader is missed.
                    while (readers.size() > 0) {
                        int last = readers.size() - 1;
                        TableReader reader = readers.getQuick(last);
                        readers.remove(last);
                        Misc.free(reader);
                    }
                } else {
                    // just clearing readers should fail leak test
                    readers.clear();
                }
                // make sure we never double-unregister queries
                executionContext = null;
            }
        }
    }

    @Override
    protected void _close() {
        final RecordCursorFactory base = this.base;
        this.base = null;
        final RegisteredRecordCursor cursor = this.cursor;
        this.cursor = null;
        final RegisteredPageFrameCursor pageFrameCursor = this.pageFrameCursor;
        this.pageFrameCursor = null;

        Throwable failure = Misc.freeBestEffort(null, cursor);
        failure = Misc.freeBestEffort(failure, base);
        failure = Misc.freeBestEffort(failure, pageFrameCursor);
        CairoException.rethrowCleanupFailure(failure);
    }

    class RegisteredPageFrameCursor implements PageFrameCursor {
        private PageFrameCursor baseCursor;
        private boolean isOpen = false;

        private RegisteredPageFrameCursor() {
        }

        @Override
        public void calculateSize(RecordCursor.Counter counter) {
            baseCursor.calculateSize(counter);
        }

        @Override
        public void close() {
            close0(null);
        }

        @Override
        public ColumnMapping getColumnMapping() {
            return baseCursor.getColumnMapping();
        }

        @Override
        public long getRemainingRowsInInterval() {
            return baseCursor.getRemainingRowsInInterval();
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
            return baseCursor.next(skipTarget);
        }

        public void of(PageFrameCursor baseCursor) {
            this.baseCursor = baseCursor;
            this.isOpen = true;
        }

        // Qodana false positive
        @SuppressWarnings("unused")
        @Override
        public void releaseOpenPartitions() {
            baseCursor.releaseOpenPartitions();
        }

        // Qodana false positive
        @SuppressWarnings("unused")
        @Override
        public void setScanProfile(ReaderScanProfile profile) {
            baseCursor.setScanProfile(profile);
        }

        @Override
        public long size() {
            return baseCursor.size();
        }

        @Override
        public boolean supportsSizeCalculation() {
            return baseCursor.supportsSizeCalculation();
        }

        @Override
        public void toTop() {
            baseCursor.toTop();
        }

        private void close0(@Nullable Throwable th) {
            if (!isOpen && th == null) {
                return;
            }
            try {
                isOpen = false;
                baseCursor = Misc.free(baseCursor);
            } catch (Throwable th0) {
                LOG.critical()
                        .$("could not close pageFrame cursor")
                        .$(" [id=").$(sqlId)
                        .$(", sql=`").$safe(queryTrace.queryText)
                        .$(", error=").$(th0)
                        .I$();
            } finally {
                unregisterAndCleanup(th);
            }
        }
    }

    class RegisteredRecordCursor implements RecordCursor {
        private RecordCursor base;
        private boolean isOpen = false;

        @Override
        public void calculateSize(SqlExecutionCircuitBreaker circuitBreaker, Counter counter) {
            base.calculateSize(circuitBreaker, counter);
        }

        @Override
        public void close() {
            close0(null);
        }

        @Override
        public Record getRecord() {
            return base.getRecord();
        }

        @Override
        public Record getRecordB() {
            return base.getRecordB();
        }

        @Override
        public SymbolTable getSymbolTable(int columnIndex) {
            return base.getSymbolTable(columnIndex);
        }

        @Override
        public boolean hasNext() {
            try {
                return base.hasNext();
            } catch (Throwable th) {
                close0(th);
                throw th;
            }
        }

        @Override
        public boolean isUsingIndex() {
            return base.isUsingIndex();
        }

        @Override
        public SymbolTable newSymbolTable(int columnIndex) {
            return base.newSymbolTable(columnIndex);
        }

        public void of(RecordCursor cursor) {
            this.base = cursor;
            this.isOpen = true;
        }

        @Override
        public long preComputedStateSize() {
            return base.preComputedStateSize();
        }

        @Override
        public void recordAt(Record record, long atRowId) {
            base.recordAt(record, atRowId);
        }

        @Override
        public void setParentUsedColumns(@Nullable IntHashSet columnIndexes) {
            base.setParentUsedColumns(columnIndexes);
        }

        @Override
        public void setParquetDecodeHint(ParquetDecodeHint hint) {
            base.setParquetDecodeHint(hint);
        }

        @Override
        public void setRecordAtRows(@Nullable RowIdSource source) {
            base.setRecordAtRows(source);
        }

        @Override
        public long size() {
            return base.size();
        }

        @Override
        public void skipRows(Counter rowCount, long maxRowsAfterSkip) {
            base.skipRows(rowCount, maxRowsAfterSkip);
        }

        @Override
        public void toTop() {
            base.toTop();
        }

        private void close0(@Nullable Throwable th) {
            if (!isOpen && th == null) {
                return;
            }
            try {
                if (isOpen) {
                    isOpen = false;
                    base = Misc.free(base);
                }
            } catch (Throwable th0) {
                LOG.critical()
                        .$("could not close record cursor")
                        .$(" [id=").$(sqlId)
                        .$(", sql=`").$safe(queryTrace.queryText)
                        .$(", error=").$(th0)
                        .I$();
            } finally {
                unregisterAndCleanup(th);
            }
        }
    }
}
