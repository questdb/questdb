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

import io.questdb.std.DirectLongList;
import io.questdb.std.IntList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.QuietCloseable;
import io.questdb.std.Vect;

/**
 * This is a helper class that stores information about segments and transactions
 * that are processed as single transaction block.
 * It is used by {@link TableWriter} and {@link io.questdb.cairo.wal.WalTxnDetails}
 * <p>
 * Apart from the segments and transactions it builds the sort plan of the block, see {@link #buildSortPlan()}.
 */
public class TableWriterSegmentCopyInfo implements QuietCloseable {
    // Item layout matches sort_plan_item in ooo.h
    public static final int SORT_PLAN_ITEM_COPY = 0;
    public static final int SORT_PLAN_ITEM_LONGS = 5;
    public static final int SORT_PLAN_ITEM_SORT = 1;
    // A cluster of fewer rows is sorted with its neighbours rather than copied,
    // this bounds the number of plan items and the per-item overhead
    public static final long SORT_PLAN_MIN_COPY_ROWS = 64;
    // The plan is not built when runs are shorter than this on average, sorting all rows is cheaper then
    public static final long SORT_PLAN_MIN_ROWS_PER_RUN = 32;
    private static final int RUN_LONGS = 7;
    private static final int RUN_MAX_TS = 1;
    private static final int RUN_MIN_TS = 0;
    private static final int RUN_ORDERED = 5;
    private static final int RUN_ROWS = 4;
    private static final int RUN_SEGMENT = 6;
    private static final int RUN_TXN_HI = 3;
    private static final int RUN_TXN_LO = 2;
    private static final int SORT_PLAN_ITEM_MAX_TS = 4;
    private static final int SORT_PLAN_ITEM_MIN_TS = 3;
    private static final int SORT_PLAN_ITEM_TXN_HI = 2;
    private static final int SORT_PLAN_ITEM_TYPE = 0;
    private static final int TXN_META_LONGS = 3;
    private static final int TXN_META_MAX_TS = 1;
    private static final int TXN_META_MIN_TS = 0;
    private static final int TXN_META_ORDERED = 2;
    private final IntList seqTxnOrder = new IntList();
    private boolean allDataInOrder;
    private boolean hasSegmentGap;
    private long maxTimestamp = Long.MIN_VALUE;
    private long maxTxnRowCount;
    private long minTimestamp = Long.MAX_VALUE;
    // Runs of transactions consecutive in the txns list, built by buildSortPlan()
    private DirectLongList runs = new DirectLongList(RUN_LONGS, MemoryTag.NATIVE_TABLE_WRITER);
    private DirectLongList segments = new DirectLongList(4, MemoryTag.NATIVE_TABLE_WRITER);
    private long sortPlanCopyRows;
    private DirectLongList sortPlanItems = new DirectLongList(SORT_PLAN_ITEM_LONGS, MemoryTag.NATIVE_TABLE_WRITER);
    // (min timestamp with flipped sign bit, run index) pairs, sorted to order the runs by time
    private DirectLongList sortPlanRunOrder = new DirectLongList(2, MemoryTag.NATIVE_TABLE_WRITER);
    // indexes of the transactions in the txns list, in the plan order
    private DirectLongList sortPlanTxns = new DirectLongList(4, MemoryTag.NATIVE_TABLE_WRITER);
    private long startSeqTxn;
    private long totalRows;
    // min timestamp, max timestamp and the in order flag of every transaction
    private DirectLongList txnMeta = new DirectLongList(TXN_META_LONGS, MemoryTag.NATIVE_TABLE_WRITER);
    private DirectLongList txns = new DirectLongList(4, MemoryTag.NATIVE_TABLE_WRITER);

    public void addSegment(int walId, int segmentId, long segmentLo, long segmentHi, boolean isLastSegmentUse) {
        segments.add(walId);
        segments.add(segmentId);
        segments.add(segmentLo);
        segments.add(isLastSegmentUse ? segmentHi : -segmentHi);
    }

    /**
     * Adds a transaction, transactions must be added grouped by segment and in seqTxn order within a segment.
     */
    public void addTxn(
            long segmentRowOffset,
            int relativeSeqTxn,
            long committedRowsCount,
            int segmentIndex,
            long minTimestamp,
            long maxTimestamp,
            boolean txnDataInOrder
    ) {
        txnMeta.add(minTimestamp);
        txnMeta.add(maxTimestamp);
        txnMeta.add(txnDataInOrder ? 1 : 0);
        txns.add(segmentRowOffset);
        txns.add(relativeSeqTxn);
        txns.add(committedRowsCount);
        txns.add(segmentIndex);

        if (seqTxnOrder.size() > 0) {
            seqTxnOrder.set(relativeSeqTxn, (int) (txns.size() / 4 - 1));
        }
        maxTxnRowCount = Math.max(maxTxnRowCount, committedRowsCount);
        totalRows += committedRowsCount;
        this.minTimestamp = Math.min(this.minTimestamp, minTimestamp);
        this.maxTimestamp = Math.max(this.maxTimestamp, maxTimestamp);
    }

    /**
     * Plans how to build the sort index of the block. Runs of transactions are grouped into clusters of
     * overlapping runs. The clusters do not overlap, so the position of every cluster in the sorted output
     * is known upfront. A cluster of one sorted run is copied, the rows of other clusters are sorted.
     * Consecutive clusters of the same kind are merged into one plan item.
     * <p>
     * Every transaction is a run of its own when transactions are big enough. A run of transactions coalesced
     * in a segment spans the time other segments may have written in between, so it is used only when
     * transactions are small, e.g. many small commits of a single writer. There is no plan when runs are
     * small, sorting all the rows is cheaper then.
     *
     * @return true when the plan copies enough rows to be faster than sorting all the rows
     */
    public boolean buildSortPlan() {
        sortPlanItems.clear();
        sortPlanTxns.clear();
        sortPlanRunOrder.clear();
        sortPlanCopyRows = 0;

        final long maxRunCount = totalRows / SORT_PLAN_MIN_ROWS_PER_RUN;
        if (!buildSortPlanRuns(getTxnCount() > maxRunCount, maxRunCount)) {
            return false;
        }
        final long runCount = runs.size() / RUN_LONGS;

        for (long r = 0; r < runCount; r++) {
            if (getRunValue(r, RUN_ROWS) > 0) {
                sortPlanRunOrder.add(getRunValue(r, RUN_MIN_TS) ^ Long.MIN_VALUE);
                sortPlanRunOrder.add(r);
            } else {
                // Empty runs have no position in the output, copy them first
                addSortPlanRun(SORT_PLAN_ITEM_COPY, r, Long.MIN_VALUE, Long.MIN_VALUE);
            }
        }

        final long orderedRunCount = sortPlanRunOrder.size() / 2;
        if (orderedRunCount == 0) {
            return false;
        }
        Vect.sortLongIndexAscInPlace(sortPlanRunOrder.getAddress(), orderedRunCount);

        long clusterLo = 0;
        long firstRun = sortPlanRunOrder.get(1);
        long clusterMin = getRunValue(firstRun, RUN_MIN_TS);
        long clusterMax = getRunValue(firstRun, RUN_MAX_TS);
        for (long i = 1; i < orderedRunCount; i++) {
            final long run = sortPlanRunOrder.get(2 * i + 1);
            final long runMin = getRunValue(run, RUN_MIN_TS);
            // Strictly greater, rows with equal timestamps are ordered by seqTxn, leave it to the sort
            if (runMin > clusterMax) {
                addSortPlanCluster(clusterLo, i, clusterMin, clusterMax);
                clusterLo = i;
                clusterMin = runMin;
                clusterMax = getRunValue(run, RUN_MAX_TS);
            } else {
                clusterMax = Math.max(clusterMax, getRunValue(run, RUN_MAX_TS));
            }
        }
        addSortPlanCluster(clusterLo, orderedRunCount, clusterMin, clusterMax);

        return sortPlanCopyRows > 0 && sortPlanCopyRows >= totalRows / 4;
    }

    public void clear() {
        segments.clear();
        txns.clear();
        txnMeta.clear();
        runs.clear();
        sortPlanItems.clear();
        sortPlanTxns.clear();
        sortPlanRunOrder.clear();
        sortPlanCopyRows = 0;
        seqTxnOrder.clear();
        totalRows = 0;
        maxTxnRowCount = 0;
        startSeqTxn = 0;
        minTimestamp = Long.MAX_VALUE;
        maxTimestamp = Long.MIN_VALUE;
        hasSegmentGap = false;
        allDataInOrder = false;
    }

    @Override
    public void close() {
        segments = Misc.free(segments);
        txns = Misc.free(txns);
        txnMeta = Misc.free(txnMeta);
        runs = Misc.free(runs);
        sortPlanItems = Misc.free(sortPlanItems);
        sortPlanTxns = Misc.free(sortPlanTxns);
        sortPlanRunOrder = Misc.free(sortPlanRunOrder);
    }

    public boolean getAllTxnDataInOrder() {
        return allDataInOrder;
    }

    public int getMappingOrder(long absoluteSeqTxn) {
        return seqTxnOrder.get((int) (absoluteSeqTxn - startSeqTxn));
    }

    public long getMaxTimestamp() {
        return maxTimestamp;
    }

    public long getMaxTxRowCount() {
        return maxTxnRowCount;
    }

    public long getMinTimestamp() {
        return minTimestamp;
    }

    public long getRowHi(int segmentIndex) {
        return Math.abs(segments.get(segmentIndex * 4L + 3));
    }

    public long getRowLo(int segmentIndex) {
        return Math.abs(segments.get(segmentIndex * 4L + 2));
    }

    public int getSegmentCount() {
        return (int) (segments.size() / 4);
    }

    public int getSegmentId(int segmentIndex) {
        return (int) segments.get(segmentIndex * 4L + 1);
    }

    public long getSegmentsAddress() {
        return segments.getAddress();
    }

    public long getSortPlanCopyRows() {
        return sortPlanCopyRows;
    }

    public long getSortPlanItemCount() {
        return sortPlanItems.size() / SORT_PLAN_ITEM_LONGS;
    }

    public long getSortPlanItemsAddress() {
        return sortPlanItems.getAddress();
    }

    public long getSortPlanTxnCount() {
        return sortPlanTxns.size();
    }

    public long getSortPlanTxnsAddress() {
        return sortPlanTxns.getAddress();
    }

    public long getStartTxn() {
        return startSeqTxn;
    }

    public long getTotalRows() {
        return totalRows;
    }

    public long getTxnCount() {
        return txns.size() / 4;
    }

    public long getTxnInfoAddress() {
        return txns.getAddress();
    }

    public int getWalId(int segmentIndex) {
        return (int) segments.get(segmentIndex * 4L);
    }

    public boolean hasSegmentGaps() {
        return hasSegmentGap;
    }

    public void initBlock(long startSeqTxn, int txnCount, boolean hasSymbols) {
        this.startSeqTxn = startSeqTxn;
        if (hasSymbols) {
            seqTxnOrder.setPos(txnCount);
        }
    }

    public boolean isLastSegmentUse(int segmentIndex) {
        return segments.get(segmentIndex * 4L + 3) > 0;
    }

    public void setAllTxnDataInOrder(boolean allInOrder) {
        this.allDataInOrder = allInOrder;
    }

    public void setSegmentGap(boolean value) {
        hasSegmentGap = value;
    }

    private void addSortPlanCluster(long orderLo, long orderHi, long clusterMin, long clusterMax) {
        final long firstRun = sortPlanRunOrder.get(2 * orderLo + 1);
        final boolean copy = orderHi - orderLo == 1
                && getRunValue(firstRun, RUN_ORDERED) == 1
                && getRunValue(firstRun, RUN_ROWS) >= SORT_PLAN_MIN_COPY_ROWS;
        final int itemType = copy ? SORT_PLAN_ITEM_COPY : SORT_PLAN_ITEM_SORT;
        for (long i = orderLo; i < orderHi; i++) {
            addSortPlanRun(itemType, sortPlanRunOrder.get(2 * i + 1), clusterMin, clusterMax);
        }
        if (copy) {
            sortPlanCopyRows += getRunValue(firstRun, RUN_ROWS);
        }
    }

    private void addSortPlanRun(int itemType, long run, long minTs, long maxTs) {
        final long itemCount = sortPlanItems.size() / SORT_PLAN_ITEM_LONGS;
        long item = (itemCount - 1) * SORT_PLAN_ITEM_LONGS;
        if (itemCount == 0 || sortPlanItems.get(item + SORT_PLAN_ITEM_TYPE) != itemType) {
            item = sortPlanItems.size();
            sortPlanItems.add(itemType);
            sortPlanItems.add(sortPlanTxns.size());
            sortPlanItems.add(sortPlanTxns.size());
            sortPlanItems.add(minTs);
            sortPlanItems.add(maxTs);
        } else {
            // Runs are added in time order, the item min timestamp stays
            sortPlanItems.set(item + SORT_PLAN_ITEM_MAX_TS, Math.max(maxTs, sortPlanItems.get(item + SORT_PLAN_ITEM_MAX_TS)));
            if (sortPlanItems.get(item + SORT_PLAN_ITEM_MIN_TS) == Long.MIN_VALUE) {
                // the item started with empty runs only
                sortPlanItems.set(item + SORT_PLAN_ITEM_MIN_TS, minTs);
            }
        }
        for (long t = getRunValue(run, RUN_TXN_LO), hi = getRunValue(run, RUN_TXN_HI); t < hi; t++) {
            sortPlanTxns.add(t);
        }
        sortPlanItems.set(item + SORT_PLAN_ITEM_TXN_HI, sortPlanTxns.size());
    }

    // Builds the runs to plan with. Without coalescing every transaction is a run of its own. Otherwise consecutive
    // transactions of a segment form a run while they are sorted and do not overlap, transactions of a segment
    // are in seqTxn order, so the rows of equal timestamps are in (timestamp, seqTxn) order in the run.
    // Returns false when there are not 2 to maxRunCount runs.
    private boolean buildSortPlanRuns(boolean coalesce, long maxRunCount) {
        runs.clear();
        long runCount = 0;
        for (long t = 0, n = getTxnCount(); t < n; t++) {
            final long rowCount = txns.get(t * 4 + 2);
            final long segmentIndex = txns.get(t * 4 + 3);
            final long minTs = txnMeta.get(t * TXN_META_LONGS + TXN_META_MIN_TS);
            final long maxTs = txnMeta.get(t * TXN_META_LONGS + TXN_META_MAX_TS);
            final long ordered = txnMeta.get(t * TXN_META_LONGS + TXN_META_ORDERED);

            if (coalesce && runCount > 0) {
                final long last = (runCount - 1) * RUN_LONGS;
                // empty transactions do not change the order
                if (rowCount == 0 || (ordered == 1
                        && runs.get(last + RUN_ORDERED) == 1
                        && runs.get(last + RUN_SEGMENT) == segmentIndex
                        && minTs >= runs.get(last + RUN_MAX_TS))) {
                    if (rowCount > 0) {
                        runs.set(last + RUN_MIN_TS, Math.min(minTs, runs.get(last + RUN_MIN_TS)));
                        runs.set(last + RUN_MAX_TS, maxTs);
                        runs.set(last + RUN_ROWS, runs.get(last + RUN_ROWS) + rowCount);
                    }
                    runs.set(last + RUN_TXN_HI, t + 1);
                    continue;
                }
            }

            if (runCount == maxRunCount) {
                return false;
            }
            runs.add(rowCount > 0 ? minTs : Long.MAX_VALUE);
            runs.add(rowCount > 0 ? maxTs : Long.MIN_VALUE);
            runs.add(t);
            runs.add(t + 1);
            runs.add(rowCount);
            runs.add(ordered);
            runs.add(segmentIndex);
            runCount++;
        }
        return runCount > 1;
    }

    private long getRunValue(long run, int offset) {
        return runs.get(run * RUN_LONGS + offset);
    }
}
