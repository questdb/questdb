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
 *     <li><b>the merge</b> - {@code O(rows * |Q|)} rather than {@code O(rows)}, because
 *     the priority queue is a sorted array rather than a heap: every row that leaves it
 *     shifts the elements above the slot its replacement takes. The row pays the larger of
 *     the two charges that can dominate: {@code ceil(log2 |Q|)}, which is what this route's
 *     own measurement was taken under, and its shifts at {@link #MERGE_SHIFTS_PER_ROW} to
 *     the base row - the first up to 320 keys, the second from 321 up;</li>
 *     <li><b>the setup</b> - two counts, not one, because the scan does two different
 *     things. It <b>opens</b> the index once per <b>partition</b>: {@code TableReader}
 *     caches an index reader per (partition, column, direction) and hands the same one to
 *     every key and every frame, so the count is neither {@code |Q|} nor {@code F} times
 *     that - a base partitioned by hour against a daily anchor segment is 24 opens. Inside
 *     a partition it then <b>seeks</b> that already-open reader once per {@code (key,
 *     frame)} pair, which is what {@code HeapRowCursorFactory} rebuilds, for
 *     {@code 24 * F * |Q|} seeks; a seek is a pooled cursor and a block-chain walk rather
 *     than two file opens and two mmaps, and is charged a sixth of the configured open
 *     price - a policy divisor, not the ratio the two measure at. Together they are the
 *     term that sinks a keyed scan over a sparse key domain.</li>
 * </ul>
 * The setup term is expressed in row-equivalents through
 * {@link io.questdb.cairo.CairoConfiguration#getLiveViewCheckpointRepairKeyedScanIndexOpenRows()},
 * because the two halves have to be comparable and only one of them is rows. The seek price
 * derives from that one knob through {@link #INDEX_SEEKS_PER_INDEX_OPEN} rather than
 * carrying a knob of its own, so the knob moves both halves together - it prices the setup
 * term, and is not a number to calibrate against a measured index open.
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
 * nothing and bounds the pricing by the cost of the option it prices. It costs nothing for
 * <b>that</b> verdict only - a stopped walk leaves every key below it and every partition
 * above it out of all three figures - so {@link #isSaturated()} reports the stop for any
 * consumer that prices the keyed side against something the budget does not bound.
 * <p>
 * One instance per refresh job, bound to the repair's pinned reader by {@link #of} and
 * reused across repairs.
 */
public final class LiveViewCheckpointKeyedScanCost {
    /**
     * The divisor that derives the per-{@code (key, frame)} seek price from the configured
     * index-open price, so the seek needs no knob of its own. A policy number, not the ratio
     * an open and a seek measure at.
     * <p>
     * An open is two file opens, two mmaps and a header verification; a seek pops a pooled
     * cursor off the already-open reader and positions it. Measured against a bare
     * sequential scan of the same fixture, an open is about 4,000 base rows and a warm seek
     * 33 to 44 - nearer 90:1 than 6:1, and on an already-pooled reader the open is free
     * while the seek is not. Six is picked against the seek rather than the open: at the
     * shipped 256-row open price it derives a seek of 42, the top of that band, and the seek
     * is the half that carries {@code |Q| * F}. So the configured price is what six seeks
     * are worth, and setting it to a machine's real open cost would over-charge every seek
     * by the same factor.
     */
    public static final int INDEX_SEEKS_PER_INDEX_OPEN = 6;
    /**
     * The keys could not be priced at all: the interval's partitions carry no index for
     * the column, or the reader refused one. The caller reads the whole range.
     */
    public static final long UNPRICEABLE = Numbers.LONG_NULL;
    /**
     * How many of the merge's element shifts one base row pays for, so the merge term
     * carries no knob of its own.
     * <p>
     * {@code HeapRowCursor} merges through {@code IntLongSortedList}, which is a sorted
     * array and not a heap: {@code pollAndReplace} searches for the replacement's slot and
     * then {@code arrayCopy}s the elements above it down by one, across two arrays.
     * Round-robin key interleaving - the ordinary shape for a time-ordered base - lands
     * every replacement at the top of the list, so the shift is the whole of it and the
     * per-row cost is linear in {@code |Q|} rather than logarithmic in it.
     * <p>
     * Thirty-two is what the route's one recorded measurement solves to. That measurement -
     * an 800_000-row daily partition over eight shared query workers at 256 keys, priced
     * 446_272 against a measured cost near 450_000 - leaves {@code (450_000 - 86_272) /
     * 40_000 - 1}, about eight row-equivalents per posting row, for the merge at 256 keys.
     * A round-robin merge over 256 keys shifts 255 elements per yielded row, so a base row
     * buys {@code 255 / 8} of them, which rounds to thirty-two.
     * <p>
     * That solves {@code 1 + shifts / 32} to the measurement, which is why
     * {@link #keyedScanCostRows} charges the larger of the shift term and the logarithmic
     * one rather than their sum: the shift term is calibrated to <b>be</b> the whole merge
     * charge wherever it applies, not to sit on top of another one. The logarithmic term
     * keeps the floor under it, so no key domain prices cheaper than it did before this
     * term existed; the two agree at 320 keys and the shift term leads from 321 up.
     * <p>
     * The number is a policy choice all the same. A shift element is one step of a
     * primitive {@code System.arraycopy}; a base row is a column read plus the view's
     * window evaluation, which {@link LiveViewCheckpointOpenSegmentCost} carries a 100ns
     * prior for, and that pair would put the ratio in the hundreds rather than at
     * thirty-two. Where the two disagree this follows the measurement, which over-charges
     * the merge - the safe direction for a route that has to earn its place against a plain
     * sequential read.
     */
    private static final int MERGE_SHIFTS_PER_ROW = 32;
    private long indexOpens;
    private long indexSeeks;
    private int pageFrameMaxRows;
    private int pageFrameMinRows;
    private long postingRows;
    private TableReader reader;
    private boolean saturated;
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
     *                    {@link Long#MAX_VALUE} to count exactly. A count that stopped
     *                    there is an understatement and not a total, which
     *                    {@link #isSaturated()} reports
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
        indexSeeks = 0;
        postingRows = 0;
        saturated = false;
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
                indexSeeks = 0;
                postingRows = 0;
                return UNPRICEABLE;
            }
            // One open for the whole partition, whatever |Q| and F are: TableReader caches
            // the index reader per (partition, column, direction) and hands the same one to
            // every key of every frame. What repeats per (key, frame) is the seek that
            // HeapRowCursorFactory rebuilds, and it is charged below at its own, far lower
            // price.
            indexOpens++;
            final long partitionSeeks = countPartitionFrames(partitionRows);
            for (int k = 0, n = symbolKeys.size(); k < n; k++) {
                indexSeeks += partitionSeeks;
                rows += countPostings(indexReader, symbolKeys.getQuick(k), partitionRows, budgetRows - rows);
                if (rows >= budgetRows) {
                    // The verdict is settled. Report what is known and stop paying for an
                    // answer nothing reads - but say that it is what is known and not what
                    // there is, because every key below k and every partition above i is
                    // now uncounted in all three figures.
                    postingRows = rows;
                    final boolean hasPartitionAbove = i + 1 < partitionCount
                            && reader.getPartitionMinTimestampFromMetadata(i + 1) <= highTsInclusive;
                    saturated |= k + 1 < n || hasPartitionAbove;
                    return rows;
                }
            }
        }
        postingRows = rows;
        return rows;
    }

    /**
     * @return how many index opens {@link #estimateKeyedScanRows} counted - one per
     * <b>partition</b> it visited, independent of the key count and of the frame split,
     * because {@code TableReader.getIndexReader} caches its reader per (partition, column,
     * direction) and every key of every frame is handed the same one
     */
    public long getIndexOpens() {
        return indexOpens;
    }

    /**
     * @return how many index seeks {@link #estimateKeyedScanRows} counted - one per key per
     * <b>page frame</b> of every partition it visited, which is what
     * {@code HeapRowCursorFactory} rebuilds: {@code PageFrameRecordCursorImpl.hasNext} asks
     * it for a cursor once per frame, and it builds one per key each time, off the index
     * reader the partition already opened
     */
    public long getIndexSeeks() {
        return indexSeeks;
    }

    /**
     * @return the posting rows the last estimate reached, saturating at its budget.
     * {@link #isSaturated()} says whether that is a total or a floor
     */
    public long getPostingRows() {
        return postingRows;
    }

    /**
     * What one per-{@code (key, frame)} index seek is charged, in base rows: the configured
     * open price divided by {@link #INDEX_SEEKS_PER_INDEX_OPEN}.
     * <p>
     * Never zero for a non-zero open price - a seek that costs nothing would let an
     * arbitrarily wide key domain price as free - and zero for a zero one, because a
     * configured zero disables the setup term outright and the seek must not smuggle it
     * back in.
     */
    public static long indexSeekRows(long indexOpenRows) {
        return indexOpenRows <= 0 ? 0 : Math.max(1, indexOpenRows / INDEX_SEEKS_PER_INDEX_OPEN);
    }

    /**
     * Whether a keyed scan of {@code keyCount} keys yielding {@code postingRows} rows,
     * behind {@code indexOpens} index opens and {@code indexSeeks} index seeks, reads less
     * than a whole-range scan of {@code wholeRangeRows}.
     * <p>
     * A tie keeps the whole-range scan: it needs no key domain, no index and no merge, and
     * this estimate rounds the keyed side down in exactly one place - the budget - so a tie
     * is as likely to be a saturated count as a real draw.
     */
    public static boolean isKeyedScanCheaper(
            long postingRows,
            long indexOpens,
            long indexSeeks,
            int keyCount,
            long wholeRangeRows,
            long indexOpenRows,
            long indexSeekRows
    ) {
        if (postingRows == UNPRICEABLE || keyCount < 1) {
            return false;
        }
        return keyedScanCostRows(postingRows, indexOpens, indexSeeks, keyCount, indexOpenRows, indexSeekRows)
                < wholeRangeRows;
    }

    /**
     * Whether the last {@link #estimateKeyedScanRows} stopped on its budget with postings
     * still uncounted, so that {@link #getPostingRows()}, {@link #getIndexOpens()} and
     * {@link #getIndexSeeks()} are floors rather than totals.
     * <p>
     * {@link #isKeyedScanCheaper} needs no such warning - a count that reached the budget
     * already answers "not cheaper", because the merge charge is at least one row per
     * posting row. Any consumer that prices the keyed side against something the budget
     * does <b>not</b> bound - an elapsed-time model whose other term is a state restore,
     * say - has to read this before it trusts the figure, because the estimate is least
     * accurate exactly where the keyed route is most expensive: the more postings there
     * really are, the earlier the walk stops and the closer the reported count sits to the
     * budget it was given.
     */
    public boolean isSaturated() {
        return saturated;
    }

    /**
     * The keyed scan's price in whole-range row equivalents: its posting rows, each carrying
     * what the merge costs to yield it, plus the per-partition index opens and the
     * per-key-per-frame index seeks the setup costs.
     * <p>
     * Saturating rather than overflowing: an unpriceably wide key domain has to read as
     * expensive, and a wrapped long would read as free.
     */
    public static long keyedScanCostRows(
            long postingRows,
            long indexOpens,
            long indexSeeks,
            int keyCount,
            long indexOpenRows,
            long indexSeekRows
    ) {
        // The merge runs on a sorted array and not on a heap: yielding a row searches for
        // its replacement's slot and then shifts every element above that slot down by one
        // across two arrays - up to keyCount - 1 of them, which round-robin key
        // interleaving makes the ordinary case rather than the worst one. A single-key
        // scan merges nothing and pays neither.
        //
        // The larger of the two charges, not their sum. The logarithmic one is what the one
        // shape this route was measured on priced at, and that shape leaves nothing beside
        // it - see MERGE_SHIFTS_PER_ROW - so a shift charge stacked on top would re-price
        // the measured shape at 1.6x what it measured. The shift charge takes over where it
        // is the bigger of the two, which is where the array's linear cost is what actually
        // dominates.
        final long merge;
        if (keyCount <= 1) {
            merge = 1L;
        } else {
            final int shifts = keyCount - 1;
            // The bit width of keyCount - 1, which is ceil(log2(keyCount)). Not literally
            // the search's step count - IntLongSortedList.binSearch halves only while more
            // than 65 entries are left and then walks - but the term the 256-key
            // measurement was taken under, and the floor that keeps a narrow key domain
            // priced exactly as it was before the shift charge existed.
            final int keyBits = Integer.SIZE - Integer.numberOfLeadingZeros(shifts);
            merge = 1L + Math.max(keyBits, shifts / MERGE_SHIFTS_PER_ROW);
        }
        final long merged = postingRows > Long.MAX_VALUE / merge ? Long.MAX_VALUE : postingRows * merge;
        final long setup = saturatingSum(
                saturatingProduct(indexOpens, indexOpenRows),
                saturatingProduct(indexSeeks, indexSeekRows)
        );
        return saturatingSum(merged, setup);
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
    private long countPostings(IndexReader indexReader, int symbolKey, long partitionRows, long budgetRows) {
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
            // A walk that stopped on the budget leaves this one key's own postings
            // uncounted, which the caller's own budget check cannot see: the count it
            // reads back is the budget either way. The extra hasNext is the first call
            // for that position - the loop short-circuits before making it - so it costs
            // one step and misreports neither an exhausted cursor nor a truncated one.
            saturated |= rows >= budgetRows && cursor.hasNext();
            return rows;
        }
    }

    /**
     * {@code count * rows}, saturating instead of wrapping, and zero where either side is
     * non-positive - a configured price of zero disables its term rather than pinning it.
     */
    private static long saturatingProduct(long count, long rows) {
        if (count <= 0 || rows <= 0) {
            return 0;
        }
        return count > Long.MAX_VALUE / rows ? Long.MAX_VALUE : count * rows;
    }

    /**
     * {@code a + b}, saturating instead of wrapping, for two non-negative terms.
     */
    private static long saturatingSum(long a, long b) {
        return Long.MAX_VALUE - a < b ? Long.MAX_VALUE : a + b;
    }
}
