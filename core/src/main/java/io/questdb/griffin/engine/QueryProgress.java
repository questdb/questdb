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

package io.questdb.griffin.engine;

import io.questdb.cairo.AbstractRecordCursorFactory;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.SqlJitMode;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.pool.ReaderPool;
import io.questdb.cairo.pool.ResourcePoolSupervisor;
import io.questdb.cairo.sql.PageFrame;
import io.questdb.cairo.sql.PageFrameCursor;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.StaticSymbolTable;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.cairo.sql.TimeFrameCursor;
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
import io.questdb.std.IntList;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

// Factory that adds query to registry on getCursor() and removes on cursor close().
public class QueryProgress extends AbstractRecordCursorFactory implements ResourcePoolSupervisor<ReaderPool.R> {
    // this field is modified via reflection from tests, via LogFactory.enableGuaranteedLogging
    @SuppressWarnings("FieldMayBeFinal")
    private static Log LOG = LogFactory.getLog(QueryProgress.class);
    private final RecordCursorFactory base;
    private final RegisteredRecordCursor cursor;
    private final boolean jit;
    private final RegisteredPageFrameCursor pageFrameCursor;
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
        if (!executionContext.shouldLogSql() || sqlText == null) {
            return;
        }

        CairoEngine engine = executionContext.getCairoEngine();
        CairoConfiguration config = engine.getConfiguration();
        long durationNanos = config.getNanosecondClock().getTicks() - beginNanos;
        boolean isJit = executionContext.getJitMode() != SqlJitMode.JIT_MODE_DISABLED;

        CharSequence principal = executionContext.getSecurityContext().getPrincipal();
        LogRecord log = null;
        try {
            final int leakedReadersCount = leakedReaders != null ? leakedReaders.size() : 0;
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
        if (queryTrace != null && engine.getConfiguration().isQueryTracingEnabled()) {
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
        LogRecord log = null;
        try {
            executionContext.getCairoEngine().getMetrics().healthMetrics().incrementQueryErrorCounter();
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
            try {
                // Configure this factory to be the supervisor for all open table readers.
                // We are assuming that all readers will be open on the same thread, which is
                // typically before cursor is fetched. Readers open after fetch has begun can go
                // unreported and may still leak.
                //
                // As a context, when cursor is being fetched it starts being dependent on the IO to
                // the network client that is receiving the data. As this client slows down, the server
                // may start throwing this cursor to another thread. This happens by virtue of parking the
                // unresponsive client and resuming it on a random thread when this client wishes to
                // continue receiving the data.
                executionContext.getCairoEngine().configureThreadLocalReaderPoolSupervisor(this);
                final RecordCursor baseCursor = base.getCursor(executionContext);
                executionContext.getCairoEngine().removeThreadLocalReaderPoolSupervisor();
                cursor.of(baseCursor); // this should not fail, it is just variable assignment
            } catch (Throwable th) {
                executionContext.getCairoEngine().removeThreadLocalReaderPoolSupervisor();
                cursor.close0(th);
                throw th;
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
            try {
                executionContext.getCairoEngine().configureThreadLocalReaderPoolSupervisor(this);
                final PageFrameCursor baseCursor = base.getPageFrameCursor(executionContext, order);
                executionContext.getCairoEngine().removeThreadLocalReaderPoolSupervisor();
                pageFrameCursor.of(baseCursor);
            } catch (Throwable th) {
                executionContext.getCairoEngine().removeThreadLocalReaderPoolSupervisor();
                pageFrameCursor.close0(th);
                throw th;
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
    public void onResourceBorrowed(ReaderPool.R resource) {
        assert resource.getSupervisor() != null;
        readers.add(resource);
    }

    @Override
    public void onResourceReturned(ReaderPool.R resource) {
        int index = readers.remove(resource);
        // do not freak out if reader is not in the list after our cursor has been closed
        if (index < 0 && (cursor.isOpen || pageFrameCursor.isOpen)) {
            // when this happens, it could be down to a race condition
            // where readers list is cleared before borrowed resources are returned.
            // Last time, this occurred when pool entry was released before readers were cleared.
            // In this scenario, the returned pool entry got used by another query and
            // readers.clear() came in tangentially to this query.
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
                    Misc.freeObjListAndClear(readers);
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
        cursor.close();
        base.close();
        pageFrameCursor.close();
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
        public IntList getColumnIndexes() {
            return baseCursor.getColumnIndexes();
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
        public long size() {
            return base.size();
        }

        @Override
        public void skipRows(Counter rowCount) {
            base.skipRows(rowCount);
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
