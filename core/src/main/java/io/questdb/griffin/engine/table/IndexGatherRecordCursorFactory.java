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

package io.questdb.griffin.engine.table;

import io.questdb.cairo.AbstractRecordCursorFactory;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ListColumnFilter;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.PageFrameCursor;
import io.questdb.cairo.sql.PartitionFrameCursor;
import io.questdb.cairo.sql.PartitionFrameCursorFactory;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.model.ExpressionNode;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

/**
 * The base of a GROUP BY whose rows a symbol {@code IN}/{@code =} filter selects through a
 * POSTING index. As a record cursor factory it is the serial index scan it wraps, a
 * {@link FilterOnValuesRecordCursorFactory} that also applies the rest of the filter. It also
 * gives an index-driven parallel GROUP BY its page frames and gather tasks
 * ({@link IndexGatherPageFrameCursor}).
 * <p>
 * It does not advertise page frames ({@link #supportsPageFrameCursor()} is false): its frames
 * hold every row of the scan, and only a consumer that reads them through the tasks may take
 * them, see {@link #supportsIndexGatherFrames()}. That consumer applies the rest of the filter
 * itself, compiled from {@link #getStealFilterExpr()}.
 */
public class IndexGatherRecordCursorFactory extends AbstractRecordCursorFactory {
    private final int frameRows;
    private final ObjList<Function> keyFunctions = new ObjList<>();
    private final long taskRows;
    private final int workerCount;
    private FilterOnValuesRecordCursorFactory delegate;
    // the rest of the filter, only valid while the query compiles
    private ExpressionNode filterExpr;
    private IndexGatherPageFrameCursor pageFrameCursor;

    public IndexGatherRecordCursorFactory(
            @NotNull FilterOnValuesRecordCursorFactory delegate,
            @Nullable ExpressionNode filterExpr,
            int frameRows,
            long taskRows,
            int workerCount
    ) {
        super(delegate.getMetadata());
        this.delegate = delegate;
        this.filterExpr = filterExpr;
        this.frameRows = frameRows;
        this.taskRows = taskRows;
        this.workerCount = workerCount;
        delegate.getKeyFunctions(keyFunctions);
    }

    /**
     * Whether the index-driven GROUP BY may read its rows key by key: always, unless an aggregate
     * depends on row order and the groups can draw from more than one key. With a single value in
     * the list, or the key column among the grouping columns, every group draws from one key,
     * whose rows the tasks return in table order.
     *
     * @param hasOrderSensitiveAggregates whether an aggregate depends on row order
     * @param groupByKeyColumns           the consumer's plain grouping columns, null for none
     */
    public boolean acceptsKeyMajorRows(boolean hasOrderSensitiveAggregates, @Nullable ListColumnFilter groupByKeyColumns) {
        if (!hasOrderSensitiveAggregates || keyFunctions.size() == 1) {
            // a single key's rows come in table order
            return true;
        }
        if (groupByKeyColumns == null) {
            return false;
        }
        final int keyColumnIndex = getKeyColumnIndex();
        for (int i = 0, n = groupByKeyColumns.getColumnCount(); i < n; i++) {
            if (groupByKeyColumns.getColumnIndexFactored(i) == keyColumnIndex) {
                return true;
            }
        }
        return false;
    }

    @Override
    public long estimateRowCountUpperBound(SqlExecutionContext executionContext) {
        return delegate.estimateRowCountUpperBound(executionContext);
    }

    @Override
    public boolean followedOrderByAdvice() {
        return delegate.followedOrderByAdvice();
    }

    @Override
    public String getBaseColumnName(int idx) {
        return delegate.getBaseColumnName(idx);
    }

    @Override
    public RecordCursor getCursor(SqlExecutionContext executionContext) throws SqlException {
        return delegate.getCursor(executionContext);
    }

    public FilterOnValuesRecordCursorFactory getDelegate() {
        return delegate;
    }

    public int getKeyColumnIndex() {
        return delegate.getKeyColumnIndex();
    }

    @TestOnly
    public IndexGatherPageFrameCursor getPageFrameCursorForTesting() {
        return pageFrameCursor;
    }

    @Override
    public PageFrameCursor getPageFrameCursor(SqlExecutionContext executionContext, int order) throws SqlException {
        if (pageFrameCursor == null) {
            pageFrameCursor = new IndexGatherPageFrameCursor(
                    delegate.columnIndexes,
                    delegate.columnSizeShifts,
                    getKeyColumnIndex(),
                    frameRows,
                    taskRows,
                    workerCount
            );
        }
        final PartitionFrameCursor partitionFrameCursor = delegate.partitionFrameCursorFactory.getCursor(
                executionContext,
                delegate.columnIndexes,
                PartitionFrameCursorFactory.ORDER_ASC
        );
        try {
            return pageFrameCursor.of(executionContext, partitionFrameCursor, keyFunctions);
        } catch (Throwable th) {
            Misc.free(pageFrameCursor);
            throw th;
        }
    }

    @Override
    public int getScanDirection() {
        return delegate.getScanDirection();
    }

    /**
     * The rest of the filter, besides the symbol list, or null. Only valid while the query
     * compiles; {@link #releaseStealFilterExpr()} drops it.
     */
    @Override
    public ExpressionNode getStealFilterExpr() {
        return filterExpr;
    }

    @Override
    public TableToken getTableToken() {
        return delegate.getTableToken();
    }

    @Override
    public boolean hasParquetConvertedColumns(SqlExecutionContext executionContext) {
        return delegate.hasParquetConvertedColumns(executionContext);
    }

    @Override
    public boolean isNonDeterministic() {
        return delegate.isNonDeterministic();
    }

    @Override
    public boolean isStableWithinExecution() {
        return delegate.isStableWithinExecution();
    }

    @Override
    public boolean mayHaveParquetPartitions(SqlExecutionContext executionContext) {
        return delegate.mayHaveParquetPartitions(executionContext);
    }

    @Override
    public boolean recordCursorSupportsRandomAccess() {
        return delegate.recordCursorSupportsRandomAccess();
    }

    /**
     * Drops the reference to the filter expression, whose nodes the compiler reuses.
     */
    public void releaseStealFilterExpr() {
        filterExpr = null;
    }

    @Override
    public boolean supportsIndexGatherFrames() {
        return true;
    }

    @Override
    public boolean supportsUpdateRowId(TableToken tableToken) {
        return delegate.supportsUpdateRowId(tableToken);
    }

    // transparent: the plan is the serial index scan's, the consumer says when it gathers
    @Override
    public void toPlan(PlanSink sink) {
        delegate.toPlan(sink);
    }

    @Override
    public boolean usesIndex() {
        return true;
    }

    @Override
    protected void _close() {
        final FilterOnValuesRecordCursorFactory delegate = this.delegate;
        this.delegate = null;
        final IndexGatherPageFrameCursor pageFrameCursor = this.pageFrameCursor;
        this.pageFrameCursor = null;
        filterExpr = null;
        keyFunctions.clear();
        Throwable failure = Misc.freeBestEffort(null, pageFrameCursor);
        failure = Misc.freeBestEffort(failure, delegate);
        CairoException.rethrowCleanupFailure(failure);
    }
}
