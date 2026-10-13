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

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.idx.AbstractPostingIndexReader;
import io.questdb.cairo.idx.IndexReader;
import io.questdb.cairo.idx.PostingIndexFwdReader;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.PageFrame;
import io.questdb.cairo.sql.PageFrameAddressCache;
import io.questdb.cairo.sql.PageFrameCursor;
import io.questdb.cairo.sql.PageFrameMemory;
import io.questdb.cairo.sql.ParquetDecodeHint;
import io.questdb.cairo.sql.PartitionFormat;
import io.questdb.cairo.sql.PartitionFrameCursorFactory;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.RowCursor;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.DirectLongList;
import io.questdb.std.IntList;
import io.questdb.std.LongList;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.Transient;
import io.questdb.std.Unsafe;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

/**
 * Emits the rows of an index scan key by key across the whole scan: all rows of the first key
 * from every page frame, then all rows of the second key, and so on. Within a key, rows come in
 * the index direction. This is the order {@code ORDER BY <indexed symbol>[, ts [DESC]]} wants,
 * so a factory that uses this cursor can report {@code followedOrderByAdvice()} without
 * assuming that the scan is a single page frame.
 * <p>
 * {@link PageFrameRecordCursorImpl} with a {@link SequentialRowCursorFactory} walks the keys
 * within each page frame instead, and a partition is split into frames of at most
 * {@code cairo.sql.page.frame.max.rows} rows, so its output is key-major per frame only.
 * <p>
 * The first {@link #hasNext()} drains the page frame cursor into the frame address cache, so
 * every frame can be revisited once per key. Each key then opens one index cursor per frame.
 * Native frames are revisited for free. A Parquet frame is skipped when the index shows the key
 * has no rows in it, and is otherwise decoded again for each key that misses the decoded-frame
 * cache, which is why the planner keeps the sort over Parquet partitions.
 * <p>
 * A key's rows are spread across each frame, so reading them in key order misses the cache and
 * the TLB on nearly every column of every row. The cursor therefore takes row ids from the index
 * in batches and loads the batch's column cache lines in one tight loop before it emits the rows,
 * which lets those misses overlap instead of being paid one row at a time by the consumer.
 * A consumer that stops early (LIMIT) leaves up to one batch of rows per (key, frame) read
 * from the index, residual filter included, and touched for nothing.
 */
public class KeyMajorPageFrameRecordCursor extends AbstractPageFrameRecordCursor {
    /**
     * {@link #collectKeyRows} found no key left.
     */
    public static final int COLLECT_EXHAUSTED = 2;
    /**
     * {@link #collectKeyRows} stopped at the end of a key; the next call starts the next key.
     */
    public static final int COLLECT_KEY_END = 0;
    /**
     * {@link #collectKeyRows} took its row limit before the key ended; the next call continues it.
     */
    public static final int COLLECT_ROW_LIMIT = 1;
    /**
     * Longs per segment of a walk plan, see {@link #planKeyRows}: the key and frame, the rank of
     * the segment's first row among the key's rows in the frame, and its row count.
     */
    public static final int PLAN_SEGMENT_LONGS = 3;
    // set in a segment's key/frame word when the segment ends at the key's last row in the frame: the
    // read then checks that the index holds no row after it, see readKeyRows()
    private static final long SEGMENT_ENDS_FRAME = 1L << 31;
    private static final long SEGMENT_FRAME_MASK = SEGMENT_ENDS_FRAME - 1;
    private static final int[] NO_KEYS = new int[0];
    // tests: false makes a planned walk count and seek by reading the index, as it does for a
    // legacy unranked encoding, whose counts and ranks the index cannot tell
    private static volatile boolean indexRanksEnabled = true;
    // tests: added to the count of one (key, frame), the first with 2+ rows that is counted, so that the
    // plan disagrees with the index; see setCountSkew()
    private static volatile long countSkew;
    private static volatile long countSkewKeyFrame = -1;
    // Row ids drained from the row cursor ahead of emission. Their column cache lines are loaded
    // in one tight loop (see PageFrameRowToucher), which must fit L1 with room to spare: measured
    // best at 32-64 on a 50M-row table, and worse from 128 up.
    private static final int PREFETCH_ROWS = 32;
    // the rank up to which readKeyRows() checks a rank seek by reading the index, see there
    private static final long RANK_CHECK_MAX_ROWS = 4096;
    private final Function filter;
    // posting readers frozen for a planned walk, see enablePlannedWalk()
    private final ObjList<AbstractPostingIndexReader> frozenReaders = new ObjList<>();
    private final LongList frameHis = new LongList();
    private final ObjList<IndexReader> frameIndexReaders = new ObjList<>();
    private final LongList frameLos = new LongList();
    // the segments of a planned collect that this thread reads itself
    private final LongList planScratch = new LongList();
    private final IntList framePartitionIndexes = new IntList();
    private final FrameSnapshot frameSnapshot = new FrameSnapshot();
    // row ids of the current (key, frame) taken from rowCursor ahead of emission, see fillRowBuf()
    private final long[] rowBuf = new long[PREFETCH_ROWS];
    private final KeyedRowCursorFactory rowCursorFactory;
    private final PageFrameRowToucher toucher = new PageFrameRowToucher();
    private final boolean walkFramesBackward;
    private boolean areCursorsPrepared;
    private SqlExecutionCircuitBreaker circuitBreaker;
    // frame of the current row cursor, as an index into the frame address cache
    private int currentFrameIndex;
    // position of the current frame in the walk, 0..frameCount
    private int framePos;
    private boolean isExhausted;
    private boolean isFramesCollected;
    // the walk counts its rows from the index and reads them through readPlan(), see enablePlannedWalk()
    private boolean isPlanned;
    private int keyCount;
    private int keyIndex;
    // planned walk: the rows of the (key, frame) it stands in, the rows taken from it, and its frame
    private long planCount;
    private int planFrameIndex;
    private boolean planOpen;
    private long planRank;
    private int rowBufLen;
    private int rowBufPos;
    private RowCursor rowCursor;
    // planned walk: every (key, frame)'s row count, key-major, that other threads counted ahead
    // of the walk, see preparePlanCounts(); valid once plannedCountsReady
    private long[] plannedCounts;
    private boolean plannedCountsReady;

    /**
     * @param frameOrder order of the page frames the partition frame cursor yields,
     *                   {@link PartitionFrameCursorFactory#ORDER_ASC} or {@code ORDER_DESC}
     */
    public KeyMajorPageFrameRecordCursor(
            CairoConfiguration configuration,
            @Transient RecordMetadata metadata,
            KeyedRowCursorFactory rowCursorFactory,
            int frameOrder,
            // this cursor owns "toTop()" lifecycle of filter
            @Nullable Function filter
    ) {
        super(configuration, metadata);
        assert rowCursorFactory.getIndexColumnIndex() > -1;
        this.rowCursorFactory = rowCursorFactory;
        this.filter = filter;
        // A row cursor walks its frame in the index direction. To keep that direction across
        // frames, walk the frames forward when they come in the same direction, else backward.
        final boolean indexForward = rowCursorFactory.getIndexDirection() == IndexReader.DIR_FORWARD;
        // ORDER_ANY gets the forward frame cursor, see AbstractPageFrameRecordCursorFactory
        final boolean framesForward = frameOrder != PartitionFrameCursorFactory.ORDER_DESC;
        this.walkFramesBackward = indexForward != framesForward;
    }

    /**
     * The frame index of a row id from {@link #collectKeyRows}, an index into
     * {@link #getFrameAddressCache()}.
     */
    public static int toFrameIndex(long frameRowId) {
        return (int) (frameRowId >>> 32);
    }

    /**
     * Encodes a row of a frame the way {@link #collectKeyRows} reports it.
     */
    public static long toFrameRowId(int frameIndex, long frameRowIndex) {
        return ((long) frameIndex << 32) | frameRowIndex;
    }

    /**
     * The row index within its frame of a row id from {@link #collectKeyRows}.
     */
    public static long toFrameRowIndex(long frameRowId) {
        return frameRowId & 0xFFFF_FFFFL;
    }

    /**
     * Counts the rows the walk has left without loading any column: no one reads them, so the
     * touch-ahead {@link #hasNext()} does for its consumer would be wasted.
     */
    @Override
    public void calculateSize(SqlExecutionCircuitBreaker circuitBreaker, Counter counter) {
        assert !isPlanned : "calculateSize() on a planned walk";
        if (isExhausted) {
            return;
        }
        // rows hasNext() has taken from the index but not yet returned
        counter.add(rowBufLen - rowBufPos);
        rowBufPos = rowBufLen;
        try {
            prepareFrames();
            while (true) {
                final RowCursor cursor = rowCursor;
                if (cursor != null) {
                    long n = 0;
                    while (cursor.hasNext()) {
                        cursor.next();
                        n++;
                    }
                    counter.add(n);
                }
                if (!nextKeyFrame(false)) {
                    break;
                }
            }
        } catch (NoMoreFramesException ignore) {
            // fall through
        }
        rowCursor = Misc.free(rowCursor);
        isExhausted = true;
    }

    @Override
    public void close() {
        rowCursor = Misc.free(rowCursor);
        unfreezeReaders();
        isPlanned = false;
        plannedCountsReady = false;
        planOpen = false;
        frameIndexReaders.clear();
        super.close();
    }

    /**
     * Walks the current key the way {@link #hasNext()} would emit it, but appends its rows to
     * {@code sink} as row ids (see {@link #toFrameRowId}) instead of positioning the record, and
     * loads no column. Stops at the end of the key, returning {@link #COLLECT_KEY_END}, or once
     * this call has appended {@code rowLimit} rows, returning {@link #COLLECT_ROW_LIMIT}; the next
     * call then continues the same key. Returns {@link #COLLECT_EXHAUSTED} when no key is left.
     * A key that has no rows still ends with {@link #COLLECT_KEY_END}.
     * <p>
     * A walk is either collected or emitted: this cursor must not mix the two between
     * {@link #toTop()} calls.
     */
    public int collectKeyRows(DirectLongList sink, long rowLimit) {
        assert rowBufPos == rowBufLen : "collectKeyRows() mixed with hasNext()";
        if (isPlanned) {
            planScratch.clear();
            final int status = planKeyRows(planScratch, rowLimit);
            readPlan(planScratch, sink);
            return status;
        }
        if (isExhausted) {
            return COLLECT_EXHAUSTED;
        }
        try {
            prepareFrames();
            if (keyIndex >= keyCount) {
                rowCursor = Misc.free(rowCursor);
                isExhausted = true;
                return COLLECT_EXHAUSTED;
            }
            long collected = 0;
            while (true) {
                final RowCursor cursor = rowCursor;
                if (cursor != null) {
                    // the frame's rows of the key, in bulk where the index can
                    collected += cursor.drainTo(sink, toFrameRowId(currentFrameIndex, 0), rowLimit - collected);
                    if (collected >= rowLimit) {
                        return COLLECT_ROW_LIMIT;
                    }
                }
                if (!openNextFrameOfKey(false)) {
                    keyIndex++;
                    framePos = 0;
                    return COLLECT_KEY_END;
                }
            }
        } catch (NoMoreFramesException e) {
            rowCursor = Misc.free(rowCursor);
            isExhausted = true;
            return COLLECT_EXHAUSTED;
        }
    }

    /**
     * Walks one key of the scan apart from the walk itself, whole frames at a time: appends the
     * key's rows (see {@link #toFrameRowId}) from its frame at walk position {@code framePos} on,
     * until at least {@code rowLimit} rows have been appended. Returns the walk position to
     * continue from, or -1 once the key's last frame has been read. It leaves no row cursor open,
     * so the walk may continue between two calls for a key it has passed with
     * {@link #skipKey()}, as long as the walk has no row cursor open itself, i.e. it stands at a
     * key boundary. Loads no column.
     */
    public int collectKeyFrames(int keyIndex, int framePos, DirectLongList sink, long rowLimit) {
        assert rowCursor == null : "collectKeyFrames() with the walk inside a key";
        prepareFrames();
        long collected = 0;
        int pos = framePos;
        while (pos < frameCount) {
            final int frameIndex = walkFramesBackward ? frameCount - 1 - pos : pos;
            pos++;
            circuitBreaker.statefulThrowExceptionIfTrippedTimeThrottledOrYield();
            if (isPlanned) {
                final long count = countKeyRows(keyIndex, frameIndex);
                readKeyRows(keyIndex, frameIndex, 0, count, true, sink);
                collected += count;
                if (collected >= rowLimit && pos < frameCount) {
                    return pos;
                }
                continue;
            }
            if (frameAddressCache.getFrameFormat(frameIndex) == PartitionFormat.PARQUET && hasNoRows(keyIndex, frameIndex)) {
                continue;
            }
            final PageFrameMemory frameMemory = frameMemoryPool.navigateTo(frameIndex);
            frameSnapshot.of(frameIndex);
            final RowCursor cursor = rowCursorFactory.getCursor(keyIndex, frameSnapshot, frameMemory);
            try {
                final long rowIdBase = toFrameRowId(frameIndex, 0);
                while (cursor.hasNext()) {
                    sink.add(rowIdBase | cursor.next());
                    collected++;
                }
            } finally {
                Misc.free(cursor);
            }
            if (collected >= rowLimit && pos < frameCount) {
                return pos;
            }
        }
        return -1;
    }

    /**
     * Switches the walk, at its start, to planned mode: {@link #planKeyRows} then advances it by
     * the posting index's per-(key, frame) row counts, without reading a row, and any thread may
     * read the planned rows later through {@link #readPlan}. {@link #collectKeyRows} and
     * {@link #collectKeyFrames} read their rows the same way on the calling thread, so a planned
     * walk can be collected and planned in turns. Rows and their order are those the unplanned
     * walk collects.
     * <p>
     * Planning needs a forward POSTING index on every frame, native frames that no covering
     * index serves, and keys that read the index as it is, without a filter (see
     * {@link KeyedRowCursorFactory#isPlainKeyScan}). Returns false, and leaves the walk as it is,
     * when the scan has anything else. The readers are frozen for the readers' concurrent
     * cursors until the walk rewinds or closes, which the consumer does only once no thread
     * reads a plan of it any more. {@link #hasNext()} must not be used on a planned walk.
     */
    public boolean enablePlannedWalk() {
        prepareFrames();
        if (isPlanned) {
            return true;
        }
        assert rowCursor == null && keyIndex == 0 && framePos == 0 && rowBufPos == rowBufLen : "enablePlannedWalk() past the walk's start";
        if (!canPlanWalk()) {
            return false;
        }
        for (int i = 0; i < frameCount; i++) {
            final AbstractPostingIndexReader reader = (AbstractPostingIndexReader) frameIndexReaders.getQuick(i);
            if (!reader.isFrozen()) {
                // positioned at this query's index snapshot, with its value memory mapped in full, so
                // that detached cursors on other threads read it without changing it, see
                // IndexGatherPageFrameCursor
                reader.warmForKeys(NO_KEYS, null);
                reader.setFrozen(true);
                frozenReaders.add(reader);
            }
        }
        isPlanned = true;
        return true;
    }

    public boolean isPlannedWalk() {
        return isPlanned;
    }

    /**
     * Planned walks only: makes room for every (key, frame)'s row count, so that other threads can
     * count them ahead of the walk ({@link #countPlanKeys}) and the walk then reads the counts
     * rather than counting on its own thread. Returns false, and the walk counts as it goes, when
     * the keys times the frames exceed {@code maxCounts}.
     */
    public boolean preparePlanCounts(long maxCounts) {
        assert isPlanned;
        plannedCountsReady = false;
        final long n = (long) keyCount * frameCount;
        if (n == 0 || n > maxCounts) {
            return false;
        }
        if (plannedCounts == null || plannedCounts.length < n) {
            plannedCounts = new long[(int) n];
        }
        return true;
    }

    /**
     * Counts the rows of keys {@code [keyLo, keyHi)} in every frame, for {@link #preparePlanCounts}.
     * Any thread may call it, for a key range of its own, between preparePlanCounts() and
     * {@link #usePlanCounts()}: it only reads the frozen readers.
     */
    public void countPlanKeys(int keyLo, int keyHi) {
        final long[] counts = plannedCounts;
        for (int k = keyLo; k < keyHi; k++) {
            final int base = k * frameCount;
            for (int f = 0; f < frameCount; f++) {
                counts[base + f] = countKeyRowsUnplanned(k, f);
            }
        }
    }

    /**
     * Every key's counts are in: the walk reads them from now on. The caller has waited for every
     * {@link #countPlanKeys} call, which orders their writes before this thread's reads.
     */
    public void usePlanCounts() {
        assert isPlanned && plannedCounts != null;
        plannedCountsReady = true;
    }

    /**
     * Plans the current key the way {@link #collectKeyRows} collects it, with the same status
     * and the same rows, but appends segments to {@code plan} (see {@link #PLAN_SEGMENT_LONGS})
     * instead of row ids. Planned walks only, see {@link #enablePlannedWalk()}.
     *
     * @return the status, as {@link #collectKeyRows} returns it
     */
    public int planKeyRows(LongList plan, long rowLimit) {
        assert isPlanned;
        if (isExhausted) {
            return COLLECT_EXHAUSTED;
        }
        if (keyIndex >= keyCount) {
            isExhausted = true;
            return COLLECT_EXHAUSTED;
        }
        // mirrors collectKeyRows(): a frame's rows are drained up to the limit, and the limit is
        // checked after every frame, before the key's end
        long collected = 0;
        while (true) {
            if (planOpen) {
                final long take = Math.min(planCount - planRank, rowLimit - collected);
                if (take > 0) {
                    addSegment(plan, keyIndex, planFrameIndex, planRank, take, planRank + take == planCount);
                    planRank += take;
                    collected += take;
                }
                if (collected >= rowLimit) {
                    return COLLECT_ROW_LIMIT;
                }
            }
            if (!openNextPlannedFrame()) {
                keyIndex++;
                framePos = 0;
                return COLLECT_KEY_END;
            }
        }
    }

    /**
     * Plans the rows of the current key that come right before the walk's position, at most
     * {@code maxRows} of them, in walk order: the rows a task that continues the key starts
     * early with. Planned walks only.
     *
     * @return the rows planned
     */
    public long planPrecedingRows(LongList plan, long maxRows) {
        assert isPlanned;
        final int lo = plan.size();
        long need = maxRows;
        if (planOpen && need > 0) {
            final long take = Math.min(planRank, need);
            if (take > 0) {
                addSegment(plan, keyIndex, planFrameIndex, planRank - take, take, planRank == planCount);
                need -= take;
            }
        }
        // the frames before the one the walk stands in, from the nearest back
        for (int pos = (planOpen ? framePos - 1 : framePos) - 1; pos >= 0 && need > 0 && keyIndex < keyCount; pos--) {
            final int frameIndex = walkFramesBackward ? frameCount - 1 - pos : pos;
            final long count = countKeyRows(keyIndex, frameIndex);
            final long take = Math.min(count, need);
            if (take > 0) {
                addSegment(plan, keyIndex, frameIndex, count - take, take, true);
                need -= take;
            }
        }
        // into walk order
        for (int i = lo, j = plan.size() - PLAN_SEGMENT_LONGS; i < j; i += PLAN_SEGMENT_LONGS, j -= PLAN_SEGMENT_LONGS) {
            for (int k = 0; k < PLAN_SEGMENT_LONGS; k++) {
                final long t = plan.getQuick(i + k);
                plan.setQuick(i + k, plan.getQuick(j + k));
                plan.setQuick(j + k, t);
            }
        }
        return maxRows - need;
    }

    @TestOnly
    public static void setIndexRanksEnabled(boolean enabled) {
        indexRanksEnabled = enabled;
    }

    /**
     * The number of rows in a plan's segments from position {@code lo} of the list on.
     */
    public static long planRowCount(LongList plan, int lo) {
        long rows = 0;
        for (int i = lo, n = plan.size(); i < n; i += PLAN_SEGMENT_LONGS) {
            rows += plan.getQuick(i + 2);
        }
        return rows;
    }

    /**
     * Appends the rows of a plan to {@code sink} as row ids (see {@link #toFrameRowId}), in plan
     * order: the rows {@link #collectKeyRows} would have appended in its place. Any thread may
     * call it, concurrently with the walk and other reads, until the walk rewinds or closes.
     * Throws when the index holds other rows than it counted.
     */
    public void readPlan(LongList plan, DirectLongList sink) {
        for (int i = 0, n = plan.size(); i < n; i += PLAN_SEGMENT_LONGS) {
            final long keyFrame = plan.getQuick(i);
            readKeyRows(
                    (int) (keyFrame >>> 32),
                    (int) (keyFrame & SEGMENT_FRAME_MASK),
                    plan.getQuick(i + 1),
                    plan.getQuick(i + 2),
                    (keyFrame & SEGMENT_ENDS_FRAME) != 0,
                    sink
            );
        }
    }

    /**
     * The number of keys the walk goes over, valid once {@link #prepareFrames()} has run.
     */
    public int getKeyCount() {
        return keyCount;
    }

    /**
     * The index of the key the walk is in, or will start next when it stands at a key boundary.
     */
    public int getKeyIndex() {
        return keyIndex;
    }

    /**
     * The address cache of every page frame of the scan, which row ids from
     * {@link #collectKeyRows} index into. Complete once {@link #prepareFrames()} has run.
     */
    public PageFrameAddressCache getFrameAddressCache() {
        return frameAddressCache;
    }

    public KeyedRowCursorFactory getRowCursorFactory() {
        return rowCursorFactory;
    }

    @Override
    public boolean hasNext() {
        assert !isPlanned : "hasNext() on a planned walk";
        if (isExhausted) {
            return false;
        }
        try {
            prepareFrames();
            while (true) {
                if (rowBufPos < rowBufLen) {
                    final long rowIndex = rowBuf[rowBufPos++];
                    frameMemoryPool.navigateTo(currentFrameIndex, recordA);
                    recordA.setRowIndex(rowIndex);
                    return true;
                }
                if (rowCursor != null && fillRowBuf()) {
                    continue;
                }
                if (!nextKeyFrame(true)) {
                    break;
                }
            }
        } catch (NoMoreFramesException ignore) {
            // fall through
        }
        rowCursor = Misc.free(rowCursor);
        isExhausted = true;
        return false;
    }

    /**
     * True when every frame of the scan is a native one that no covering index serves, so that
     * any thread can read a collected row through its own {@link io.questdb.cairo.sql.PageFrameMemoryPool}
     * at a stable address. Valid once {@link #prepareFrames()} has run.
     */
    public boolean hasOnlyPlainNativeFrames() {
        for (int i = 0; i < frameCount; i++) {
            if (frameAddressCache.getFrameFormat(i) != PartitionFormat.NATIVE || frameAddressCache.isFrameCovered(i)) {
                return false;
            }
        }
        return true;
    }

    @Override
    public boolean isUsingIndex() {
        return true;
    }

    /**
     * True once {@link #collectKeyRows} has reported {@link #COLLECT_EXHAUSTED}, or the walk has
     * otherwise ended, until {@link #toTop()}.
     */
    public boolean isWalkExhausted() {
        return isExhausted;
    }

    @Override
    public void of(PageFrameCursor frameCursor, SqlExecutionContext sqlExecutionContext) throws SqlException {
        if (this.frameCursor != frameCursor) {
            close();
            this.frameCursor = frameCursor;
        }
        recordA.of(frameCursor);
        recordB.of(frameCursor);
        rowCursorFactory.init(frameCursor, sqlExecutionContext);
        circuitBreaker = sqlExecutionContext.getCircuitBreaker();
        // see PageFrameRecordCursorImpl.of(): observe cancellation even when the scan has no frames
        circuitBreaker.statefulThrowExceptionIfTrippedTimeThrottledOrYield();
        areCursorsPrepared = false;
        resetWalk();
        super.init(sqlExecutionContext.getMemoryTracker());
        // every key comes back to every frame, so give decoded Parquet frames the whole cache budget
        frameMemoryPool.setParquetDecodeHint(ParquetDecodeHint.SCATTERED);
    }

    @Override
    public long preComputedStateSize() {
        return RecordCursor.fromBool(areCursorsPrepared);
    }

    /**
     * Drains the page frame cursor into the frame address cache and resolves the keys, which the
     * first {@link #hasNext()} or {@link #collectKeyRows} would otherwise do. Idempotent until
     * {@link #toTop()}.
     */
    public void prepareFrames() {
        prepareRowCursorFactory();
        if (!isFramesCollected) {
            collectFrames();
            keyCount = rowCursorFactory.getKeyCount();
        }
    }

    /**
     * Abandons the rest of the current key: the next {@link #collectKeyRows} starts the next key,
     * and no row cursor stays open.
     */
    public void skipKey() {
        assert rowBufPos == rowBufLen : "skipKey() mixed with hasNext()";
        rowCursor = Misc.free(rowCursor);
        planOpen = false;
        keyIndex++;
        framePos = 0;
    }

    @Override
    public long size() {
        return -1;
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.type("Key-major page frame scan");
    }

    @Override
    public void toTop() {
        if (filter != null) {
            filter.toTop();
        }
        resetWalk();
        super.toTop();
    }

    private static void addSegment(LongList plan, int keyIndex, int frameIndex, long rankLo, long count, boolean endsFrame) {
        assert frameIndex >= 0;
        plan.add(((long) keyIndex << 32) | (frameIndex & SEGMENT_FRAME_MASK) | (endsFrame ? SEGMENT_ENDS_FRAME : 0));
        plan.add(rankLo);
        plan.add(count);
    }

    private boolean canPlanWalk() {
        if (rowCursorFactory.getIndexDirection() != IndexReader.DIR_FORWARD) {
            // detached cursors read forward
            return false;
        }
        for (int k = 0; k < keyCount; k++) {
            if (!rowCursorFactory.isPlainKeyScan(k)) {
                return false;
            }
        }
        for (int i = 0; i < frameCount; i++) {
            if (frameAddressCache.getFrameFormat(i) != PartitionFormat.NATIVE
                    || frameAddressCache.isFrameCovered(i)
                    || !(frameIndexReaders.getQuick(i) instanceof PostingIndexFwdReader)) {
                return false;
            }
        }
        return true;
    }

    // The rows of a key in a frame, as its row cursor returns them. The index counts them where it
    // can; a legacy unranked encoding is counted by reading it.
    private long countKeyRows(int keyIndex, int frameIndex) {
        if (plannedCountsReady) {
            return skewed(keyIndex, frameIndex, plannedCounts[keyIndex * frameCount + frameIndex]);
        }
        return skewed(keyIndex, frameIndex, countKeyRowsUnplanned(keyIndex, frameIndex));
    }

    // The rows of a key in a frame, as countKeyRows() counts them, before any test skew.
    private long countKeyRowsUnplanned(int keyIndex, int frameIndex) {
        final int indexKey = rowCursorFactory.getIndexKey(keyIndex);
        if (indexKey < 0) {
            // a symbol the table does not have: its row cursor is empty
            return 0;
        }
        final AbstractPostingIndexReader reader = (AbstractPostingIndexReader) frameIndexReaders.getQuick(frameIndex);
        final long lo = frameLos.getQuick(frameIndex);
        final long hi = frameHis.getQuick(frameIndex) - 1;
        final long entryMax = reader.getEntryMaxValue();
        final long count = indexRanksEnabled
                ? reader.countMatchesClamped(indexKey, lo, hi, entryMax >= 0 ? Math.min(hi, entryMax) : hi)
                : Numbers.LONG_NULL;
        if (count != Numbers.LONG_NULL) {
            return count;
        }
        final RowCursor cursor = reader.getDetachedCursor(indexKey, lo, hi, null);
        try {
            long n = 0;
            while (cursor.hasNext()) {
                cursor.next();
                n++;
            }
            return n;
        } finally {
            Misc.free(cursor);
        }
    }

    // tests: see setCountSkew()
    private static long skewed(int keyIndex, int frameIndex, long count) {
        final long skew = countSkew;
        if (skew == 0 || count < 2) {
            return count;
        }
        final long keyFrame = ((long) keyIndex << 32) | frameIndex;
        synchronized (KeyMajorPageFrameRecordCursor.class) {
            if (countSkewKeyFrame == -1) {
                countSkewKeyFrame = keyFrame;
            }
        }
        return countSkewKeyFrame == keyFrame ? Math.max(0, count + skew) : count;
    }

    /**
     * Tests: makes the planned walk count {@code skew} rows more (or fewer) than the index holds
     * for one (key, frame), the first with two rows or more that is counted after the call; 0
     * switches it off.
     * The walk's reads must then fail the query rather than return other rows.
     */
    @TestOnly
    public static void setCountSkew(long skew) {
        synchronized (KeyMajorPageFrameRecordCursor.class) {
            countSkew = skew;
            countSkewKeyFrame = -1;
        }
    }

    // Opens the planned walk's next frame of the current key. Returns false once the key has been
    // walked across all frames.
    private boolean openNextPlannedFrame() {
        planOpen = false;
        if (framePos < frameCount) {
            final int frameIndex = walkFramesBackward ? frameCount - 1 - framePos : framePos;
            framePos++;
            circuitBreaker.statefulThrowExceptionIfTrippedTimeThrottledOrYield();
            planFrameIndex = frameIndex;
            planRank = 0;
            planCount = countKeyRows(keyIndex, frameIndex);
            planOpen = true;
            return true;
        }
        return false;
    }

    // Appends count rows of the key in the frame, from its rankLo-th on, with a cursor of this thread's own.
    // endsFrame: the rows are the key's last in the frame, so the index must hold none after them.
    private void readKeyRows(int keyIndex, int frameIndex, long rankLo, long count, boolean endsFrame, DirectLongList sink) {
        if (count <= 0) {
            return;
        }
        final int indexKey = rowCursorFactory.getIndexKey(keyIndex);
        final AbstractPostingIndexReader reader = (AbstractPostingIndexReader) frameIndexReaders.getQuick(frameIndex);
        final long lo = frameLos.getQuick(frameIndex);
        final long hi = frameHis.getQuick(frameIndex) - 1;
        long rowLo = lo;
        long skip = rankLo;
        if (rankLo > 0) {
            final long entryMax = reader.getEntryMaxValue();
            final long row = indexRanksEnabled
                    ? reader.selectKthMatch(indexKey, lo, hi, entryMax >= 0 ? Math.min(hi, entryMax) : hi, rankLo)
                    : Numbers.LONG_NULL;
            if (row != Numbers.LONG_NULL) {
                // Reading the index up to the rank costs rankLo reads per task, quadratic in the
                // key's rows over the walk, and servers run with -ea (TAQ idx 72: 22% of the
                // workers' window CPU). So the rank is checked in full only up to
                // RANK_CHECK_MAX_ROWS, which every test reaches, and past it only that the row is
                // one of the key's.
                assert rankLo > RANK_CHECK_MAX_ROWS
                        ? isKeyRow(reader, indexKey, row, hi)
                        : row == rankRowByReading(reader, indexKey, lo, hi, rankLo) : "rank seek found another row [keyIndex=" + keyIndex + ", frameIndex=" + frameIndex + ", rankLo=" + rankLo + ']';
                rowLo = row;
                skip = 0;
            }
        }
        final RowCursor cursor = reader.getDetachedCursor(indexKey, rowLo, hi, null);
        try {
            for (long i = 0; i < skip; i++) {
                if (!cursor.hasNext()) {
                    throw planMismatch(keyIndex, frameIndex, rankLo, count, i);
                }
                cursor.next();
            }
            final long pos = sink.size();
            final long n = cursor.drainTo(sink, toFrameRowId(frameIndex, 0), count);
            if (n != count) {
                throw planMismatch(keyIndex, frameIndex, rankLo, count, n);
            }
            if (endsFrame && cursor.hasNext()) {
                // more rows than counted: they would be dropped without a word
                throw planMismatch(keyIndex, frameIndex, rankLo, count, count + 1);
            }
            final long shift = rowLo - lo;
            if (shift > 0) {
                // the cursor's rows are relative to its first row
                final long addr = sink.getAddress() + pos * Long.BYTES;
                for (long i = 0; i < n; i++) {
                    final long a = addr + i * Long.BYTES;
                    Unsafe.putLong(a, Unsafe.getLong(a) + shift);
                }
            }
        } finally {
            Misc.free(cursor);
        }
    }

    // Whether the index holds the key at the row.
    private static boolean isKeyRow(AbstractPostingIndexReader reader, int indexKey, long row, long hi) {
        final RowCursor cursor = reader.getDetachedCursor(indexKey, row, hi, null);
        try {
            return cursor.hasNext() && cursor.next() == 0;
        } finally {
            Misc.free(cursor);
        }
    }

    // The rankLo-th row of the key in [lo, hi], by reading the index from lo: what selectKthMatch() must return.
    private static long rankRowByReading(AbstractPostingIndexReader reader, int indexKey, long lo, long hi, long rankLo) {
        final RowCursor cursor = reader.getDetachedCursor(indexKey, lo, hi, null);
        try {
            for (long i = 0; i < rankLo; i++) {
                if (!cursor.hasNext()) {
                    return Numbers.LONG_NULL;
                }
                cursor.next();
            }
            return cursor.hasNext() ? lo + cursor.next() : Numbers.LONG_NULL;
        } finally {
            Misc.free(cursor);
        }
    }

    private CairoException planMismatch(int keyIndex, int frameIndex, long rankLo, long count, long found) {
        return CairoException.critical(0)
                .put("index walk found other rows than it counted [keyIndex=").put(keyIndex)
                .put(", frameIndex=").put(frameIndex)
                .put(", rankLo=").put(rankLo)
                .put(", count=").put(count)
                .put(", found=").put(found)
                .put(']');
    }

    private void unfreezeReaders() {
        for (int i = 0, n = frozenReaders.size(); i < n; i++) {
            frozenReaders.getQuick(i).setFrozen(false);
        }
        frozenReaders.clear();
    }

    private void collectFrames() {
        final int indexColumnIndex = rowCursorFactory.getIndexColumnIndex();
        final int indexDirection = rowCursorFactory.getIndexDirection();
        PageFrame frame;
        while ((frame = frameCursor.next()) != null) {
            // see PageFrameRecordCursorImpl.hasNext() on the time-throttled breaker check
            circuitBreaker.statefulThrowExceptionIfTrippedTimeThrottledOrYield();
            frameAddressCache.add(frameCount++, frame);
            frameLos.add(frame.getPartitionLo());
            frameHis.add(frame.getPartitionHi());
            framePartitionIndexes.add(frame.getPartitionIndex());
            // The frame object is reused by the next call to next(), so take the reader now. The
            // table reader caches it per partition, and it stays open until the cursor closes.
            frameIndexReaders.add(frame.getIndexReader(indexColumnIndex, indexDirection));
        }
        isFramesCollected = true;
    }

    // Moves to the next (key, frame) pair and opens its row cursor. Returns false once all keys
    // have been walked across all frames.
    private boolean nextKeyFrame(boolean touch) {
        while (keyIndex < keyCount) {
            if (openNextFrameOfKey(touch)) {
                return true;
            }
            keyIndex++;
            framePos = 0;
        }
        return false;
    }

    // Opens the row cursor of the current key in its next frame, skipping a Parquet frame the key
    // has no rows in. Returns false once the key has been walked across all frames.
    private boolean openNextFrameOfKey(boolean touch) {
        rowCursor = Misc.free(rowCursor);
        while (framePos < frameCount) {
            final int frameIndex = walkFramesBackward ? frameCount - 1 - framePos : framePos;
            framePos++;
            circuitBreaker.statefulThrowExceptionIfTrippedTimeThrottledOrYield();
            if (frameAddressCache.getFrameFormat(frameIndex) == PartitionFormat.PARQUET && hasNoRows(keyIndex, frameIndex)) {
                // a Parquet frame is decoded on navigation: skip it when the key has no rows in it
                continue;
            }
            final PageFrameMemory frameMemory = frameMemoryPool.navigateTo(frameIndex);
            frameSnapshot.of(frameIndex);
            rowCursor = rowCursorFactory.getCursor(keyIndex, frameSnapshot, frameMemory);
            currentFrameIndex = frameIndex;
            recordA.init(frameMemory);
            if (touch) {
                toucher.of(frameAddressCache, frameIndex, frameMemory);
            } else {
                toucher.clear();
            }
            return true;
        }
        return false;
    }

    // Takes up to PREFETCH_ROWS row ids of the current (key, frame) and loads their column cache
    // lines. Returns false when the row cursor is exhausted.
    private boolean fillRowBuf() {
        final long[] buf = rowBuf;
        final RowCursor cursor = rowCursor;
        int n = 0;
        while (n < buf.length && cursor.hasNext()) {
            buf[n++] = cursor.next();
        }
        rowBufPos = 0;
        rowBufLen = n;
        if (n > 0 && toucher.isEnabled()) {
            toucher.touch(buf, n);
        }
        return n > 0;
    }

    private boolean hasNoRows(int keyIndex, int frameIndex) {
        final int indexKey = rowCursorFactory.getIndexKey(keyIndex);
        if (indexKey < 0) {
            return false;
        }
        final RowCursor probe = frameIndexReaders.getQuick(frameIndex)
                .getCursor(indexKey, frameLos.getQuick(frameIndex), frameHis.getQuick(frameIndex) - 1);
        try {
            return !probe.hasNext();
        } finally {
            Misc.free(probe);
        }
    }

    private void prepareRowCursorFactory() {
        if (!areCursorsPrepared) {
            rowCursorFactory.prepareCursor(frameCursor);
            areCursorsPrepared = true;
        }
    }

    private void resetWalk() {
        rowCursor = Misc.free(rowCursor);
        // a planned walk's readers are read by no task any more: the consumer waits for its
        // tasks before it rewinds or closes the walk
        unfreezeReaders();
        isPlanned = false;
        plannedCountsReady = false;
        planOpen = false;
        rowBufPos = 0;
        rowBufLen = 0;
        toucher.clear();
        isExhausted = false;
        isFramesCollected = false;
        frameLos.clear();
        frameHis.clear();
        framePartitionIndexes.clear();
        frameIndexReaders.clear();
        keyIndex = 0;
        framePos = 0;
        currentFrameIndex = -1;
        keyCount = 0;
    }

    /**
     * Stands in for a page frame the frame cursor has already moved past. The per-key row
     * cursors read only the index reader and the row range of the frame.
     */
    private class FrameSnapshot implements PageFrame {
        private int frameIndex;

        @Override
        public long getAuxPageAddress(int columnIndex) {
            throw new UnsupportedOperationException();
        }

        @Override
        public long getAuxPageSize(int columnIndex) {
            throw new UnsupportedOperationException();
        }

        @Override
        public int getColumnCount() {
            throw new UnsupportedOperationException();
        }

        @Override
        public byte getFormat() {
            return frameAddressCache.getFrameFormat(frameIndex);
        }

        @Override
        public IndexReader getIndexReader(int columnIndex, int direction) {
            assert columnIndex == rowCursorFactory.getIndexColumnIndex() && direction == rowCursorFactory.getIndexDirection();
            return frameIndexReaders.getQuick(frameIndex);
        }

        @Override
        public long getPageAddress(int columnIndex) {
            throw new UnsupportedOperationException();
        }

        @Override
        public long getPageSize(int columnIndex) {
            throw new UnsupportedOperationException();
        }

        @Override
        public int getParquetRowGroup() {
            return frameAddressCache.getParquetRowGroup(frameIndex);
        }

        @Override
        public int getParquetRowGroupHi() {
            return frameAddressCache.getParquetRowGroupHi(frameIndex);
        }

        @Override
        public int getParquetRowGroupLo() {
            return frameAddressCache.getParquetRowGroupLo(frameIndex);
        }

        @Override
        public long getPartitionHi() {
            return frameHis.getQuick(frameIndex);
        }

        @Override
        public int getPartitionIndex() {
            return framePartitionIndexes.getQuick(frameIndex);
        }

        @Override
        public long getPartitionLo() {
            return frameLos.getQuick(frameIndex);
        }

        private void of(int frameIndex) {
            this.frameIndex = frameIndex;
        }
    }
}
