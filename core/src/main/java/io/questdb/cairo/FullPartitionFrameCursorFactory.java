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

package io.questdb.cairo;

import io.questdb.cairo.sql.PartitionFrameCursor;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.model.RuntimeIntervalModel;
import io.questdb.std.IntList;
import io.questdb.std.LongList;
import io.questdb.std.Misc;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.TestOnly;

public class FullPartitionFrameCursorFactory extends AbstractPartitionFrameCursorFactory {
    private static final ThreadLocal<CloseObserver> TEST_CLOSE_OBSERVER = new ThreadLocal<>();
    private final int baseOrder;
    private final CloseObserver closeObserver;
    private FullBwdPartitionFrameCursor bwdCursor;
    private IntervalBwdPartitionFrameCursor bwdRangeCursor;
    private RuntimeIntervalModel bwdRangeIntervalModel;
    private LongList bwdRangeIntervals;
    private FullFwdPartitionFrameCursor fwdCursor;
    private IntervalFwdPartitionFrameCursor fwdRangeCursor;
    private RuntimeIntervalModel fwdRangeIntervalModel;
    private LongList fwdRangeIntervals;

    public FullPartitionFrameCursorFactory(
            TableToken tableToken,
            long metadataVersion,
            RecordMetadata metadata,
            int order,
            String viewName,
            int viewPosition,
            boolean updateQuery
    ) {
        super(tableToken, metadataVersion, metadata, viewName, viewPosition, updateQuery);
        this.baseOrder = order;
        this.closeObserver = TEST_CLOSE_OBSERVER.get();
    }

    @TestOnly
    public static void clearCloseObserverForTesting() {
        TEST_CLOSE_OBSERVER.remove();
    }

    @TestOnly
    public static void setCloseObserverForTesting(CloseObserver observer) {
        TEST_CLOSE_OBSERVER.set(observer);
    }

    @Override
    public void close() {
        super.close();
        fwdCursor = Misc.free(fwdCursor);
        bwdCursor = Misc.free(bwdCursor);
        fwdRangeCursor = Misc.free(fwdRangeCursor);
        fwdRangeIntervalModel = Misc.free(fwdRangeIntervalModel);
        bwdRangeCursor = Misc.free(bwdRangeCursor);
        bwdRangeIntervalModel = Misc.free(bwdRangeIntervalModel);
        // Notify last, so an observer sees a fully released factory.
        if (closeObserver != null) {
            closeObserver.onClose(this);
        }
    }

    @Override
    public PartitionFrameCursor getCursor(SqlExecutionContext executionContext, @NotNull IntList columnIndexes, int order) throws SqlException {
        authorizeSelect(executionContext, columnIndexes);
        final TableReader reader = getReader(executionContext);
        try {
            reader.setActiveColumns(columnIndexes);
            if (order == ORDER_ASC || ((order == ORDER_ANY || order < 0) && baseOrder != ORDER_DESC)) {
                if (fwdCursor == null) {
                    fwdCursor = new FullFwdPartitionFrameCursor();
                }
                return fwdCursor.of(reader);
            }

            // Create backward scanning cursor when needed. Factory requesting backward cursor must
            // still return records in ascending timestamp order.
            if (bwdCursor == null) {
                bwdCursor = new FullBwdPartitionFrameCursor();
            }
            return bwdCursor.of(reader);
        } catch (Throwable th) {
            Misc.free(reader);
            throw th;
        }
    }

    /**
     * Returns a forward cursor whose first frame starts at {@code timestampLo},
     * inclusive. The interval cursor culls historical partitions and binary
     * searches the boundary partition instead of scanning the row prefix.
     */
    public PartitionFrameCursor getCursor(
            SqlExecutionContext executionContext,
            @NotNull IntList columnIndexes,
            long timestampLo
    ) throws SqlException {
        return getCursor(executionContext, columnIndexes, timestampLo, Long.MAX_VALUE);
    }

    /**
     * Returns a forward cursor over the timestamp range
     * {@code [timestampLo, timestampHi]}, <b>inclusive of both edges</b> - the
     * convention every QuestDB interval model uses. The interval cursor culls the
     * partitions below {@code timestampLo} and above {@code timestampHi}, then
     * binary searches each boundary partition, so neither end costs a row scan.
     * <p>
     * A caller holding an <i>exclusive</i> high bound converts it before calling.
     * The conversion cannot be folded in here, because it is not total: an
     * exclusive bound one past {@code Long.MAX_VALUE} has no {@code long} to
     * express it, so a caller reaching the end of the frame must carry that case as
     * a tag rather than as a timestamp (see {@code LiveViewCheckpointRepairPlan}).
     * <p>
     * {@code timestampLo > timestampHi} is an empty range, not an error: the cursor
     * yields no frame at all.
     */
    public PartitionFrameCursor getCursor(
            SqlExecutionContext executionContext,
            @NotNull IntList columnIndexes,
            long timestampLo,
            long timestampHi
    ) throws SqlException {
        authorizeSelect(executionContext, columnIndexes);
        final TableReader reader = getReader(executionContext);
        try {
            reader.setActiveColumns(columnIndexes);
            if (fwdRangeCursor == null) {
                fwdRangeIntervals = new LongList(2);
                fwdRangeIntervalModel = new RuntimeIntervalModel(
                        ColumnType.getTimestampDriver(getMetadata().getTimestampType()),
                        PartitionBy.NONE,
                        fwdRangeIntervals
                );
                fwdRangeCursor = new IntervalFwdPartitionFrameCursor(
                        reader.getConfiguration(),
                        fwdRangeIntervalModel,
                        getMetadata().getTimestampIndex()
                );
            }
            stampRange(fwdRangeIntervals, timestampLo, timestampHi);
            return fwdRangeCursor.of(reader, executionContext);
        } catch (Throwable th) {
            Misc.free(reader);
            throw th;
        }
    }

    /**
     * Returns a cursor over the timestamp range {@code [timestampLo, timestampHi]},
     * <b>inclusive of both edges</b>, walking partitions from the top of the range
     * down. It is the descending mirror of
     * {@link #getCursor(SqlExecutionContext, IntList, long, long)} and obeys the same
     * inclusive-edge and empty-on-inverted-bounds rules.
     * <p>
     * Descent is what makes predecessor discovery bounded. A caller looking for the
     * {@code N} qualifying rows that precede a repair floor does not know how far back
     * they sit, so it walks down and stops on the row that satisfies the last key it is
     * still waiting for. Closing the cursor there leaves every partition below that row
     * unopened - the cost the discovery bound exists to avoid, and one an ascending scan
     * cannot avoid at all, because it would have to start from the bottom of the range
     * to arrive in that order.
     * <p>
     * The backward cursor keeps its own interval list rather than sharing the forward
     * one: both directions are open at once while a repair collects its output keys
     * above the floor and its dependency floor below it, and a shared list would let the
     * second call silently re-point the first cursor's range.
     */
    public PartitionFrameCursor getCursorBackward(
            SqlExecutionContext executionContext,
            @NotNull IntList columnIndexes,
            long timestampLo,
            long timestampHi
    ) throws SqlException {
        authorizeSelect(executionContext, columnIndexes);
        final TableReader reader = getReader(executionContext);
        try {
            reader.setActiveColumns(columnIndexes);
            if (bwdRangeCursor == null) {
                bwdRangeIntervals = new LongList(2);
                bwdRangeIntervalModel = new RuntimeIntervalModel(
                        ColumnType.getTimestampDriver(getMetadata().getTimestampType()),
                        PartitionBy.NONE,
                        bwdRangeIntervals
                );
                bwdRangeCursor = new IntervalBwdPartitionFrameCursor(
                        reader.getConfiguration(),
                        bwdRangeIntervalModel,
                        getMetadata().getTimestampIndex()
                );
            }
            stampRange(bwdRangeIntervals, timestampLo, timestampHi);
            return bwdRangeCursor.of(reader, executionContext);
        } catch (Throwable th) {
            Misc.free(reader);
            throw th;
        }
    }

    @Override
    public int getOrder() {
        return baseOrder;
    }

    // A full scan of the table snapshot has no value sources of its own.
    @Override
    public boolean isNonDeterministic() {
        return false;
    }

    @Override
    public void toPlan(PlanSink sink) {
        int order = sink.getOrder();
        if (order == ORDER_ANY || order < 0) {
            order = baseOrder;
        }
        if (order == ORDER_DESC) {
            sink.type("Frame backward scan");
        } else {
            sink.type("Frame forward scan");
        }
        super.toPlan(sink);
    }

    /**
     * Re-stamps <b>both</b> bounds of a cached interval list, so a later call cannot
     * inherit an earlier one's edge. An empty list is how the interval cursor spells
     * "matches nothing"; an inverted pair would instead drive its binary searches off a
     * range that cannot exist.
     */
    private static void stampRange(LongList intervals, long timestampLo, long timestampHi) {
        intervals.clear();
        if (timestampLo <= timestampHi) {
            intervals.add(timestampLo);
            intervals.add(timestampHi);
        }
    }

    @TestOnly
    public interface CloseObserver {
        void onClose(FullPartitionFrameCursorFactory factory);
    }
}
