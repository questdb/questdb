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
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.idx.AbstractPostingIndexReader;
import io.questdb.cairo.idx.IndexFwdNullReader;
import io.questdb.cairo.idx.IndexReader;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.PageFrameCursor;
import io.questdb.cairo.sql.PartitionFormat;
import io.questdb.cairo.sql.PartitionFrame;
import io.questdb.cairo.sql.PartitionFrameCursor;
import io.questdb.cairo.sql.PartitionFrameCursorFactory;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.model.ExpressionNode;
import io.questdb.std.IntList;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

/**
 * The base of a GROUP BY whose rows a symbol {@code IN}/{@code =} filter selects through a
 * POSTING index. As a record cursor factory it is the serial index scan it wraps, a
 * {@link FilterOnValuesRecordCursorFactory} that also applies the rest of the filter. It also
 * gives an index-driven parallel GROUP BY its page frames and gather tasks
 * ({@link IndexGatherPageFrameCursor}), and estimates, from the index's per-key row counts, the
 * share of the scanned rows the list selects, for the choice between this plan and a scan.
 * <p>
 * It does not advertise page frames ({@link #supportsPageFrameCursor()} is false): its frames
 * hold every row of the scan, and only a consumer that reads them through the tasks may take
 * them, see {@link #supportsIndexGatherFrames()}. That consumer applies the rest of the filter
 * itself, compiled from {@link #getStealFilterExpr()}.
 */
public class IndexGatherRecordCursorFactory extends AbstractRecordCursorFactory {
    // budget of (partition, key) count probes of one estimate, see estimateSelection()
    private static final int ESTIMATE_MAX_PROBES = 65_536;
    private final IntList estimateKeys = new IntList();
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

    /**
     * Estimates, from the posting index's per-key row counts, the rows the list selects and the
     * rows the scan would read. Up to {@value #ESTIMATE_MAX_PROBES} (partition, key) counts are
     * read; past that, partitions are sampled by stride and the selection extrapolated. A
     * partition without a posting reader counts every row as selected, which leans towards the
     * scan, since the index plan scans such partitions too.
     *
     * @param sink receives {selected, scanned}
     */
    public void estimateSelection(SqlExecutionContext executionContext, long[] sink) throws SqlException {
        long scanned = 0;
        long sampledScanned = 0;
        long sampledSelected = 0;
        try (PartitionFrameCursor frameCursor = delegate.partitionFrameCursorFactory.getCursor(
                executionContext,
                delegate.columnIndexes,
                PartitionFrameCursorFactory.ORDER_ASC
        )) {
            final TableReader reader = frameCursor.getTableReader();
            final int keyColumnIndex = getKeyColumnIndex();
            final int readerKeyColumnIndex = delegate.columnIndexes.getQuick(keyColumnIndex);
            for (int i = 0, n = keyFunctions.size(); i < n; i++) {
                keyFunctions.getQuick(i).init(frameCursor, executionContext);
            }
            IndexGatherPageFrameCursor.resolveKeys(keyFunctions, reader.getSymbolTable(readerKeyColumnIndex), estimateKeys);
            final int keyCount = estimateKeys.size();
            final long frameCountBound = frameCursor.getFrameCountUpperBound();
            final long frameCount = frameCountBound > 0 ? frameCountBound : reader.getPartitionCount();
            final long stride = keyCount == 0
                    ? Long.MAX_VALUE
                    : Math.max(1, (frameCount * keyCount + ESTIMATE_MAX_PROBES - 1) / ESTIMATE_MAX_PROBES);
            long frameIndex = 0;
            while (true) {
                final boolean probe = keyCount > 0 && frameIndex % stride == 0;
                // a skipped frame is resolved from partition metadata, without opening the partition
                final PartitionFrame frame = frameCursor.next(probe ? 0 : Long.MAX_VALUE);
                if (frame == null) {
                    break;
                }
                frameIndex++;
                final long rowLo = frame.getRowLo();
                final long rowHi = frame.getRowHi();
                final long rows = rowHi - rowLo;
                scanned += rows;
                if (!probe || rows <= 0) {
                    continue;
                }
                sampledScanned += rows;
                if (frame.getPartitionFormat() != PartitionFormat.NATIVE) {
                    sampledSelected += rows;
                    continue;
                }
                final IndexReader indexReader = reader.getIndexReader(frame.getPartitionIndex(), readerKeyColumnIndex, IndexReader.DIR_FORWARD);
                if (indexReader instanceof AbstractPostingIndexReader posting) {
                    final long callerHiInclusive = rowHi - 1;
                    final long entryMax = posting.getEntryMaxValue();
                    final long clampedMax = entryMax >= 0 ? Math.min(callerHiInclusive, entryMax) : callerHiInclusive;
                    long selected = 0;
                    for (int k = 0; k < keyCount; k++) {
                        long count = posting.estimateMatchesClamped(estimateKeys.getQuick(k), rowLo, callerHiInclusive, clampedMax);
                        if (count == AbstractPostingIndexReader.ESTIMATE_REJECT || count == Numbers.LONG_NULL) {
                            count = rows;
                        }
                        selected += count;
                    }
                    sampledSelected += Math.min(selected, rows);
                } else if (indexReader instanceof IndexFwdNullReader) {
                    // the column does not exist in this partition: every row is NULL
                    if (estimateKeys.binarySearchUniqueList(0) > -1) {
                        sampledSelected += rows;
                    }
                } else {
                    sampledSelected += rows;
                }
            }
        }
        sink[0] = sampledScanned > 0 ? (long) ((double) sampledSelected * scanned / sampledScanned) : 0;
        sink[1] = scanned;
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
