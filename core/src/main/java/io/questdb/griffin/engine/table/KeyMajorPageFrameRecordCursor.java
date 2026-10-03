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
 */
public class KeyMajorPageFrameRecordCursor extends AbstractPageFrameRecordCursor {
    private final Function filter;
    private final LongList frameHis = new LongList();
    private final ObjList<IndexReader> frameIndexReaders = new ObjList<>();
    private final LongList frameLos = new LongList();
    private final IntList framePartitionIndexes = new IntList();
    private final FrameSnapshot frameSnapshot = new FrameSnapshot();
    private final KeyedRowCursorFactory rowCursorFactory;
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

    @Override
    public void close() {
        rowCursor = Misc.free(rowCursor);
        frameIndexReaders.clear();
        super.close();
    }

    public KeyedRowCursorFactory getRowCursorFactory() {
        return rowCursorFactory;
    }

    @Override
    public boolean hasNext() {
        if (isExhausted) {
            return false;
        }
        prepareRowCursorFactory();
        try {
            if (!isFramesCollected) {
                collectFrames();
                keyCount = rowCursorFactory.getKeyCount();
            }
            while (true) {
                if (rowCursor != null && rowCursor.hasNext()) {
                    final long rowIndex = rowCursor.next();
                    frameMemoryPool.navigateTo(currentFrameIndex, recordA);
                    recordA.setRowIndex(rowIndex);
                    return true;
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

    @Override
    public boolean isUsingIndex() {
        return true;
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
        rowCursor = Misc.free(rowCursor);
        while (keyIndex < keyCount) {
            if (framePos < frameCount) {
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
                return true;
            }
            keyIndex++;
            framePos = 0;
        }
        return false;
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
