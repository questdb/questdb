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

package io.questdb.cairo.lv;

import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.idx.IndexReader;
import io.questdb.cairo.sql.RowCursor;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.table.FwdTableReaderPageFrameCursor;
import io.questdb.std.IntList;
import io.questdb.std.Numbers;
import org.jetbrains.annotations.NotNull;

/**
 * Prices a <b>keyed</b> repair scan - one that follows the affected keys' rows through the
 * base's posting index - against the whole-range scan that reads every row, so a repair
 * takes the cheaper of the two on evidence rather than on the assumption that fewer keys
 * is always less work.
 * <p>
 * The estimate reads the pinned reader's own posting lists, never
 * {@code affectedKeys * averageRowsPerKey}: one hot key can hold most of a segment's rows,
 * and an average would price that segment as though it held a few thousand.
 *
 * <h2>The three terms</h2>
 * A keyed scan does not cost one unit per row it yields. {@code HeapRowCursorFactory}
 * rebuilds one row cursor per key for every page frame, and merges those cursors through a
 * priority queue, so the price is:
 * <ul>
 *     <li><b>posting rows</b> - what the index says those keys hold over the interval, and
 *     what the scan actually pulls off the columns;</li>
 *     <li><b>the merge</b> - {@code O(rows * log |Q|)} rather than {@code O(rows)},
 *     because every row leaves the heap through a sift;</li>
 *     <li><b>the setup</b> - one index open per key per page frame, and a partition carries
 *     as many frames as {@code FwdTableReaderPageFrameCursor} splits it into. A base
 *     partitioned by hour against a daily anchor segment is {@code 24 * F * |Q|} index opens
 *     before a row is read, where {@code F} is that split; this is the term that sinks a
 *     keyed scan over a sparse key domain.</li>
 * </ul>
 * The setup term is expressed in row-equivalents through
 * {@link io.questdb.cairo.CairoConfiguration#getLiveViewCheckpointRepairKeyedScanIndexOpenRows()},
 * because the two halves have to be comparable and only one of them is rows.
 *
 * <h2>What it rounds, and in which direction</h2>
 * A key's postings are counted per <b>partition</b>, whole, for every partition the
 * interval touches at all - the index names row positions rather than timestamps, so the
 * two partitions the interval ends inside cannot be narrowed without walking them. That
 * overestimates the keyed side and never the whole-range one, so the comparison errs
 * towards the whole-range scan, which is the safe direction: it is correct for every shape
 * and merely reads more.
 * <p>
 * The frame split behind the setup term rounds the same way, and for the same reason: it
 * derives from each partition's whole row count rather than from the rows the interval
 * leaves inside the two it ends in. It ignores the extra split a column top forces, which
 * rounds the other way - a partition written before one of the scan's columns existed
 * carries one more frame than the row limit alone accounts for.
 * <p>
 * The estimate opens every partition the interval touches, which is what the scan it prices
 * would map anyway. It has to: {@code TableReader} hands out a no-op index reader for a
 * partition whose columns it has not mapped, and caches it, so pricing through an unopened
 * partition would both report the keyed scan as free and leave a cached reader behind that a
 * later index-driven query on the same {@code TableReader} would miss rows through.
 * <p>
 * A key whose postings the index cannot count in constant time - a bitmap index reports no
 * size - is walked, and the walk stops at {@link #estimateKeyedScanRows}'s budget. The
 * budget is not an approximation of the answer: once the keyed side has passed the
 * whole-range scan's row count there is no verdict left to change, so stopping there costs
 * nothing and bounds the pricing by the cost of the option it prices.
 * <p>
 * One instance per refresh job, bound to the repair's pinned reader by {@link #of} and
 * reused across repairs.
 */
public final class LiveViewCheckpointKeyedScanCost {
    /**
     * The keys could not be priced at all: the interval's partitions carry no index for
     * the column, or the reader refused one. The caller reads the whole range.
     */
    public static final long UNPRICEABLE = Numbers.LONG_NULL;
    private long indexOpens;
    private int pageFrameMaxRows;
    private int pageFrameMinRows;
    private long postingRows;
    private TableReader reader;
    private int sharedQueryWorkerCount;

    /**
     * Estimates how many base rows a keyed scan of {@code symbolKeys} over the inclusive
     * interval pulls, and records the setup term beside it.
     *
     * @param columnIndex the key column's index in the <b>reader's</b> metadata, which a
     *                    caller holding a scan position resolves through
     *                    {@code PageFrameRecordCursorFactory.getBaseColumnIndex}
     * @param budgetRows  the point above which the answer cannot change the verdict, so
     *                    the count saturates rather than continuing. Pass
     *                    {@link Long#MAX_VALUE} to count exactly
     * @return the posting rows, or {@link #UNPRICEABLE}
     */
    public long estimateKeyedScanRows(
            long lowTs,
            long highTsInclusive,
            int columnIndex,
            @NotNull IntList symbolKeys,
            long budgetRows
    ) {
        indexOpens = 0;
        postingRows = 0;
        if (highTsInclusive < lowTs || symbolKeys.size() == 0 || reader.size() == 0) {
            return 0;
        }
        final int partitionCount = reader.getPartitionCount();
        final long tableMaxTs = reader.getMaxTimestamp();
        // PARTITION BY NONE maintains no table minimum - it reads back as Long.MAX_VALUE,
        // above the maximum - so the single partition's own floor is the only lower bound
        // there is. The same clamp LiveViewCheckpointScanCost applies, for the same reason.
        final long tableMinTs = reader.getMinTimestamp() <= tableMaxTs
                ? reader.getMinTimestamp()
                : reader.getPartitionMinTimestampFromMetadata(0);
        long rows = 0;
        // An interval starting below the table's own minimum searches to -1, which is the
        // first partition.
        for (int i = Math.max(0, reader.getPartitionIndexByTimestamp(lowTs)); i < partitionCount; i++) {
            final long partitionLowTs = i == 0 ? tableMinTs : reader.getPartitionMinTimestampFromMetadata(i);
            if (partitionLowTs > highTsInclusive) {
                break;
            }
            final long partitionHighTs = i + 1 < partitionCount
                    ? reader.getPartitionMaxTimestampFromMetadata(i)
                    : tableMaxTs;
            if (partitionHighTs < lowTs) {
                continue;
            }
            final long partitionRows;
            final IndexReader indexReader;
            try {
                // Opened, not just counted. TableReader hands out an index reader per
                // partition, and for a partition whose columns it has not mapped yet it
                // hands out - and caches - a null reader that yields no row at all. Pricing
                // through that would report the keyed scan as free AND leave the cached null
                // reader behind for the next index-driven query on this reader, which would
                // then miss rows. The open is not extra work either: it maps exactly the
                // partitions the scan being priced would read.
                partitionRows = reader.openPartition(i);
                if (partitionRows <= 0) {
                    continue;
                }
                indexReader = reader.getIndexReader(i, columnIndex, IndexReader.DIR_FORWARD);
            } catch (Throwable ignore) {
                // A partition written before the column was indexed, a Parquet partition, or
                // an index this reader cannot open. None is an error here: the repair reads
                // the whole range, exactly as it did before this estimate existed.
                indexOpens = 0;
                postingRows = 0;
                return UNPRICEABLE;
            }
            // The scan rebuilds every key's row cursor once per page frame, not once per
            // partition, so the setup term is charged per frame the partition splits into.
            final long partitionFrames = countPartitionFrames(partitionRows);
            for (int k = 0, n = symbolKeys.size(); k < n; k++) {
                indexOpens += partitionFrames;
                rows += countPostings(indexReader, symbolKeys.getQuick(k), partitionRows, budgetRows - rows);
                if (rows >= budgetRows) {
                    // The verdict is settled. Report what is known and stop paying for an
                    // answer nothing reads.
                    postingRows = rows;
                    return rows;
                }
            }
        }
        postingRows = rows;
        return rows;
    }

    /**
     * @return how many index opens {@link #estimateKeyedScanRows} counted - one per key per
     * <b>page frame</b> of every partition it visited, which is what
     * {@code HeapRowCursorFactory} rebuilds: {@code PageFrameRecordCursorImpl.hasNext} asks
     * it for a cursor once per frame, and it builds one per key each time
     */
    public long getIndexOpens() {
        return indexOpens;
    }

    /**
     * @return the posting rows the last estimate reached, saturating at its budget
     */
    public long getPostingRows() {
        return postingRows;
    }

    /**
     * Whether a keyed scan of {@code keyCount} keys yielding {@code postingRows} rows,
     * behind {@code indexOpens} index opens, reads less than a whole-range scan of
     * {@code wholeRangeRows}.
     * <p>
     * A tie keeps the whole-range scan: it needs no key domain, no index and no merge, and
     * this estimate rounds the keyed side down in exactly one place - the budget - so a tie
     * is as likely to be a saturated count as a real draw.
     */
    public static boolean isKeyedScanCheaper(
            long postingRows,
            long indexOpens,
            int keyCount,
            long wholeRangeRows,
            long indexOpenRows
    ) {
        if (postingRows == UNPRICEABLE || keyCount < 1) {
            return false;
        }
        return keyedScanCostRows(postingRows, indexOpens, keyCount, indexOpenRows) < wholeRangeRows;
    }

    /**
     * The keyed scan's price in whole-range row equivalents: its posting rows, each carrying
     * the heap sift the merge costs, plus the per-key-per-frame setup.
     * <p>
     * Saturating rather than overflowing: an unpriceably wide key domain has to read as
     * expensive, and a wrapped long would read as free.
     */
    public static long keyedScanCostRows(long postingRows, long indexOpens, int keyCount, long indexOpenRows) {
        // One sift per level of the heap the merge builds, and a single-key scan builds
        // none: ceil(log2(keyCount)) on top of the row itself.
        final long sift = 1L + (keyCount <= 1 ? 0 : 32 - Integer.numberOfLeadingZeros(keyCount - 1));
        final long merged = postingRows > Long.MAX_VALUE / sift ? Long.MAX_VALUE : postingRows * sift;
        final long setup = indexOpens > Long.MAX_VALUE / Math.max(1, indexOpenRows)
                ? Long.MAX_VALUE
                : indexOpens * indexOpenRows;
        return Long.MAX_VALUE - merged < setup ? Long.MAX_VALUE : merged + setup;
    }

    /**
     * Binds the estimate to the snapshot one repair plans against, which is the same reader
     * {@link LiveViewCheckpointScanCost} prices the whole-range side against: two readers
     * at two {@code seqTxn}s would describe two different scans.
     * <p>
     * The context comes with it because the setup term is per page frame, and how many
     * frames a partition splits into is the context's own: it carries the frame row bounds
     * and the shared query worker count {@code FwdTableReaderPageFrameCursor} divides by.
     * The estimate has to read them off the context the priced cursor will open under, not
     * off the configuration, because a caller may narrow the pair for one query.
     */
    public void of(@NotNull TableReader reader, @NotNull SqlExecutionContext executionContext) {
        this.reader = reader;
        this.pageFrameMinRows = executionContext.getPageFrameMinRows();
        this.pageFrameMaxRows = executionContext.getPageFrameMaxRows();
        this.sharedQueryWorkerCount = executionContext.getSharedQueryWorkerCount();
    }

    /**
     * Counts the page frames one whole partition splits into, which is how many times the
     * scan rebuilds each key's index-backed row cursor inside it.
     */
    private long countPartitionFrames(long partitionRows) {
        // Clamped the way calculatePageFrameRowLimit clamps its own worker count. A row
        // bound of zero reaches it as a divisor, and PropServerConfiguration rejects one -
        // but a hand-built CairoConfiguration is under no such rule.
        final long rowsPerFrame = FwdTableReaderPageFrameCursor.calculatePageFrameRowLimit(
                0,
                partitionRows,
                Math.max(1, pageFrameMinRows),
                Math.max(1, pageFrameMaxRows),
                sharedQueryWorkerCount
        );
        return (partitionRows + rowsPerFrame - 1) / rowsPerFrame;
    }

    /**
     * Counts one key's postings inside one partition, in constant time where the index can
     * and by walking where it cannot.
     */
    private static long countPostings(IndexReader indexReader, int symbolKey, long partitionRows, long budgetRows) {
        // The index keys its postings by symbolKey + 1, with 0 reserved for the null value,
        // which is what SymbolIndexRowCursorFactory converts on the way in. Passing the
        // table-local key straight through would count the neighbouring key's rows.
        try (RowCursor cursor = indexReader.getCursor(TableUtils.toIndexKey(symbolKey), 0, partitionRows - 1)) {
            final long size = cursor.size();
            if (size > -1) {
                return size;
            }
            long rows = 0;
            while (rows < budgetRows && cursor.hasNext()) {
                cursor.next();
                rows++;
            }
            return rows;
        }
    }
}
