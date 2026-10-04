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
import io.questdb.cairo.idx.IndexReader;
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
import io.questdb.std.ObjList;
import io.questdb.std.Transient;
import org.jetbrains.annotations.Nullable;

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
    // Row ids drained from the row cursor ahead of emission. Their column cache lines are loaded
    // in one tight loop (see PageFrameRowToucher), which must fit L1 with room to spare: measured
    // best at 32-64 on a 50M-row table, and worse from 128 up.
    private static final int PREFETCH_ROWS = 32;
    private final Function filter;
    private final LongList frameHis = new LongList();
    private final ObjList<IndexReader> frameIndexReaders = new ObjList<>();
    private final LongList frameLos = new LongList();
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
    private int keyCount;
    private int keyIndex;
    private int rowBufLen;
    private int rowBufPos;
    private RowCursor rowCursor;

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

    @Override
    public void close() {
        rowCursor = Misc.free(rowCursor);
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
                    final long rowIdBase = toFrameRowId(currentFrameIndex, 0);
                    while (collected < rowLimit && cursor.hasNext()) {
                        sink.add(rowIdBase | cursor.next());
                        collected++;
                    }
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
                if (!nextKeyFrame()) {
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
    private boolean nextKeyFrame() {
        while (keyIndex < keyCount) {
            if (openNextFrameOfKey(true)) {
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
