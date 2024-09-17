/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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
import io.questdb.cairo.CairoException;
import io.questdb.cairo.DataUnavailableException;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.*;
import io.questdb.cairo.sql.async.PageFrameSequence;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.QueryRegistry;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.SCSequence;
import io.questdb.std.Chars;
import io.questdb.std.FlyweightMessageContainer;

// Factory that adds query to registry on getCursor() and removes on cursor close().
public class QueryProgress extends AbstractRecordCursorFactory {
    private static final Log LOG = LogFactory.getLog(QueryProgress.class);
    private final RecordCursorFactory base;
    private final RegisteredRecordCursor cursor;
    private final QueryRegistry registry;
    private final String sqlText;
    private long beginNanos;
    private SqlExecutionContext executionContext;
    private boolean failed = false;
    private long sqlId;
    private final boolean jit;

    public QueryProgress(QueryRegistry registry, CharSequence sqlText, RecordCursorFactory base) {
        super(base.getMetadata());
        this.base = base;
        this.registry = registry;
        this.sqlText = Chars.toString(sqlText);
        this.cursor = new RegisteredRecordCursor();
        this.jit = base.usesCompiledFilter();
    }

    public static void logEnd(long sqlId, CharSequence sqlText, SqlExecutionContext executionContext, long beginNanos, boolean jit) {
        LOG.infoW()
                .$("fin [id=").$(sqlId)
                .$(", sql=`").utf8(sqlText).$('`')
                .$(", principal=").$(executionContext.getSecurityContext().getPrincipal())
                .$(", cache=").$(executionContext.isCacheHit())
                .$(", jit=").$(jit)
                .$(", time=").$(executionContext.getCairoEngine().getConfiguration().getNanosecondClock().getTicks() - beginNanos)
                .I$();
    }

    public static void logError(
            Throwable e,
            long sqlId,
            CharSequence sqlText,
            SqlExecutionContext executionContext,
            long beginNanos,
            boolean jit
    ) {
        final int errno = e instanceof CairoException ? ((CairoException) e).getErrno() : 0;
        final int pos = e instanceof FlyweightMessageContainer ? ((FlyweightMessageContainer) e).getPosition() : 0;
        LOG.errorW()
                .$("err")
                .$(" [id=").$(sqlId)
                .$(", sql=`").utf8(sqlText).$('`')
                .$(", principal=").$(executionContext.getSecurityContext().getPrincipal())
                .$(", cache=").$(executionContext.isCacheHit())
                .$(", jit=").$(jit)
                .$(", time=").$(executionContext.getCairoEngine().getConfiguration().getNanosecondClock().getTicks() - beginNanos)
                .$(", msg=").$(e.getMessage())
                .$(", errno=").$(errno)
                .$(", pos=").$(pos)
                .I$();
    }

    public static void logStart(
            long sqlId,
            CharSequence sqlText,
            SqlExecutionContext executionContext,
            boolean jit
    ) {
        LOG.infoW()
                .$("exe")
                .$(" [id=").$(sqlId)
                .$(", sql=`").utf8(sqlText).$('`')
                .$(", principal=").$(executionContext.getSecurityContext().getPrincipal())
                .$(", cache=").$(executionContext.isCacheHit())
                .$(", jit=").$(jit)
                .I$();
    }

    @Override
    public PageFrameSequence<?> execute(SqlExecutionContext executionContext, SCSequence collectSubSeq, int order) throws SqlException {
        return base.execute(executionContext, collectSubSeq, order);
    }

    @Override
    public boolean followedLimitAdvice() {
        return base.followedLimitAdvice();
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
    public String getBaseColumnNameNoRemap(int idx) {
        return base.getBaseColumnNameNoRemap(idx);
    }

    @Override
    public RecordCursorFactory getBaseFactory() {
        return base;
    }

    @Override
    public RecordCursor getCursor(SqlExecutionContext executionContext) throws SqlException {
        if (!cursor.isOpen) {
            this.executionContext = executionContext;
            sqlId = registry.register(sqlText, executionContext);
            beginNanos = executionContext.getCairoEngine().getConfiguration().getNanosecondClock().getTicks();
            logStart(sqlId, sqlText, executionContext, jit);
            try {
                final RecordCursor baseCursor = base.getCursor(executionContext);
                cursor.of(baseCursor); // this should not fail, it is just variable assigment
            } catch (Throwable e) {
                registry.unregister(sqlId, executionContext);
                logError(e);
                throw e;
            }
        }
        return cursor;
    }

    @Override
    public PageFrameCursor getPageFrameCursor(SqlExecutionContext executionContext, int order) throws SqlException {
        return base.getPageFrameCursor(executionContext, order);
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
    public TimeFrameRecordCursor getTimeFrameCursor(SqlExecutionContext executionContext) throws SqlException {
        return base.getTimeFrameCursor(executionContext);
    }

    @Override
    public boolean implementsLimit() {
        return base.implementsLimit();
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

    private void logError(Throwable e) {
        logError(
                e,
                sqlId,
                sqlText,
                executionContext,
                beginNanos,
                jit
        );
    }

    @Override
    protected void _close() {
        cursor.close();
        base.close();
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
            if (isOpen) {
                registry.unregister(sqlId, executionContext);
                isOpen = false;
                base.close();
                if (!failed) {
                    logEnd(sqlId, sqlText, executionContext, beginNanos, jit);
                }
            }
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
        public boolean hasNext() throws DataUnavailableException {
            try {
                return base.hasNext();
            } catch (Throwable e) {
                failed = true;
                logError(e);
                throw e;
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
        public void recordAt(Record record, long atRowId) {
            base.recordAt(record, atRowId);
        }

        @Override
        public long size() throws DataUnavailableException {
            return base.size();
        }

        @Override
        public void skipRows(Counter rowCount) throws DataUnavailableException {
            base.skipRows(rowCount);
        }

        @Override
        public void toTop() {
            base.toTop();
        }
    }
}
