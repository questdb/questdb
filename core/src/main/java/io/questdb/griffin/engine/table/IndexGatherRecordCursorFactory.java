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
    // the (partition, key) counts one estimate reads at most, see estimateSelection()
    static final int ESTIMATE_MAX_COUNTS = PostingEstimateSample.MAX_COUNTS;
    // the distinct partitions one estimate probes at most, see estimateSelection()
    static final int ESTIMATE_SAMPLE_PARTITIONS = PostingEstimateSample.SAMPLE_PARTITIONS;
    private final IntList estimateKeys = new IntList();
    private final PostingEstimateSample estimateSample = new PostingEstimateSample();
    private final int frameRows;
    private final ObjList<Function> keyFunctions = new ObjList<>();
    private final long taskRows;
    private final int workerCount;
    private FilterOnValuesRecordCursorFactory delegate;
    // the rest of the filter, only valid while the query compiles
    private ExpressionNode filterExpr;
    private IndexGatherPageFrameCursor pageFrameCursor;
    // see setSplitKeysByRowRange()
    private boolean splitKeysByRowRange = true;

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
     * whose rows the tasks return in table order, provided that one slot reads them: such a GROUP
     * BY must also forbid row-range splits, see {@link #setSplitKeysByRowRange(boolean)}.
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
     * rows the scan would read. The rows scanned are exact: a first walk reads them from partition
     * metadata, without opening a partition. The selection is sampled, so that the cost of an open
     * does not grow with the partitions or the keys:
     * <ul>
     *   <li>at most {@value #ESTIMATE_SAMPLE_PARTITIONS} partitions are probed, evenly placed over
     *   the G partitions the walk yields rows from: the j-th is the floor(j * G / m)-th of them,
     *   m = min(G, {@value #ESTIMATE_SAMPLE_PARTITIONS}). A second walk stops after the last one,
     *   and opens no other partition;</li>
     *   <li>at most {@value #ESTIMATE_MAX_COUNTS} counts are read: each probed frame counts kS =
     *   min(K, {@value #ESTIMATE_MAX_COUNTS} / m) of the list's K keys, and a probed partition
     *   counts up to {@value #ESTIMATE_MAX_COUNTS} / (m * kS) of its frames. NULL (key 0), when in
     *   the list, is always counted as it is; the other keys are evenly spread in key order and
     *   their sum scaled by the share of the non-NULL keys they stand for. With every key counted,
     *   nothing is scaled.</li>
     * </ul>
     * The selection is the probed partitions' share extrapolated to the rows scanned. A parquet
     * partition counts every row as selected, which leans towards the scan, since the index plan
     * scans such partitions too; a partition that predates the column counts its rows as NULL.
     * Clustered keys can be misestimated; that only changes the plan, never the rows.
     *
     * @param sink receives {selected, scanned}
     */
    public void estimateSelection(SqlExecutionContext executionContext, long[] sink) throws SqlException {
        final PostingEstimateSample sample = estimateSample;
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

            // first walk, from partition metadata: the rows scanned, and the partitions with rows
            sample.walkMetadata(frameCursor);
            if (sample.plan(estimateKeys)) {
                // resolveKeys() sorts the keys: NULL, key 0, comes first
                final boolean hasNull = sample.hasNull();
                final int sampledKeyCount = sample.getSampledKeyCount();
                final int countsPerFrame = sampledKeyCount + (hasNull ? 1 : 0);
                // second walk, over the target partitions' frames
                frameCursor.toTop();
                PartitionFrame frame;
                while ((frame = sample.nextProbeFrame(frameCursor)) != null) {
                    final long rowLo = frame.getRowLo();
                    final long rowHi = frame.getRowHi();
                    final long rows = rowHi - rowLo;
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
                        sample.addCounts(countsPerFrame);
                        final long nullSelected = hasNull ? count(posting, 0, rows, rowLo, callerHiInclusive, clampedMax) : 0;
                        long selected = 0;
                        for (int k = 0; k < sampledKeyCount; k++) {
                            selected += count(posting, sample.getSampledKey(k), rows, rowLo, callerHiInclusive, clampedMax);
                        }
                        sampledSelected += Math.min(nullSelected + sample.scaleNonNull(selected), rows);
                    } else {
                        // the gather only serves a POSTING column, whose reader is a posting
                        // reader, or the stand-in of a partition that predates the column, where
                        // every row is NULL
                        assert indexReader instanceof IndexFwdNullReader;
                        if (hasNull) {
                            sampledSelected += rows;
                        }
                    }
                }
            }
            sample.finish();
        }
        final long scanned = sample.getScanned();
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

    /**
     * Sets the estimates' budget, see {@link #estimateSelection}: the partitions they probe and the
     * counts they read, at most. It also bounds the covering index's estimates
     * ({@link CoveringIndexRecordCursorFactory#estimateSelection}). Tests that change it restore
     * {@value #ESTIMATE_SAMPLE_PARTITIONS} and {@value #ESTIMATE_MAX_COUNTS}.
     */
    @TestOnly
    public static void setEstimateBudgetForTesting(int samplePartitions, int maxCounts) {
        PostingEstimateSample.setBudgetForTesting(samplePartitions, maxCounts);
    }

    @TestOnly
    public int getLastEstimateOpenedPartitions() {
        return estimateSample.getOpenedPartitions();
    }

    @TestOnly
    public IntList getLastEstimatePartitions() {
        return estimateSample.getProbedPartitions();
    }

    @TestOnly
    public long getLastEstimateProbes() {
        return estimateSample.getCounts();
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
                    splitKeysByRowRange,
                    workerCount,
                    executionContext.getCairoEngine().getConfiguration().isSqlParallelFrameDispatchEnabled()
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

    /**
     * Whether a key with more rows in a partition than a task holds may be split by row range
     * into several tasks, which different slots then read. The GROUP BY forbids it when an
     * aggregate depends on the order rows arrive in
     * ({@link io.questdb.griffin.engine.functions.GroupByFunction#isOrderSensitive()}): see
     * {@link IndexGatherPageFrameCursor}. Must be set before the first cursor opens.
     */
    public void setSplitKeysByRowRange(boolean splitKeysByRowRange) {
        assert pageFrameCursor == null;
        this.splitKeysByRowRange = splitKeysByRowRange;
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

    // a key's rows in the frame; a count the index cannot give (a clipped legacy EF block)
    // counts every row
    private static long count(AbstractPostingIndexReader posting, int key, long rows, long rowLo, long callerHiInclusive, long clampedMax) {
        final long count = posting.estimateMatchesClamped(key, rowLo, callerHiInclusive, clampedMax);
        // LONG_NULL answers a negative key only, and resolveKeys() gives keys >= 0
        assert count != Numbers.LONG_NULL;
        return count == AbstractPostingIndexReader.ESTIMATE_REJECT ? rows : count;
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
