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

import io.questdb.PropertyKey;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.ColumnTypeDriver;
import io.questdb.cairo.EntityColumnFilter;
import io.questdb.cairo.LoopingRecordSink;
import io.questdb.cairo.RecordChain;
import io.questdb.cairo.idx.IndexReader;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.PageFrame;
import io.questdb.cairo.sql.PageFrameCursor;
import io.questdb.cairo.sql.PageFrameMemory;
import io.questdb.cairo.sql.ParquetDecodeHint;
import io.questdb.cairo.sql.PartitionFormat;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.DirectLongList;
import io.questdb.std.MemoryTag;
import io.questdb.std.MemoryTracker;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.QuietCloseable;
import io.questdb.std.Rows;
import io.questdb.std.Transient;
import org.jetbrains.annotations.Nullable;

/**
 * Reads the rows of a {@link KeyOrderedRowCursorFactory} key by key over all page frames, or gathers them in one pass
 * over the page frames when its Parquet page frames do not stay decoded across the passes of the keys.
 */
final class KeyOrderedPageFrameRecordCursor extends PageFrameRecordCursorImpl {
    private final RecordCursor.Counter countCounter = new RecordCursor.Counter();
    private final int decodedRowSize;
    // The rows of a read ordered by keys that one pass over the page frames gathers, when its Parquet page frames
    // do not stay decoded across the passes of the keys, see isGatherRequired().
    private final KeyOrderedRowChain gatheredRows;
    private final KeyOrderedRowCursorFactory keyOrderedFactory;
    // The cumulative row counts of the keys the gathered read counted, in key order.
    private final DirectLongList keyRowCounts;
    private int countedKeyCount;
    // The window of rows, by position in key order, that the next read of a gathered read starts at and ends before.
    private long gatherHi = Long.MAX_VALUE;
    private long gatherLo;
    // The window of rows the gathered rows hold, gatheredHi at Long.MAX_VALUE when it runs to the last row, and
    // gatheredLo at -1 before the read gathers any.
    private long gatheredHi;
    private long gatheredLo = -1;
    private boolean isGatherPositioned;
    private boolean isGathering;
    private boolean isKeyRowCountUnknown;
    private int keyCount;
    private int keyHi;
    private int keyLo;
    // The number of page frames of the scan, known once a pass of a read ordered by keys reads them all, -1 before.
    private int scanFrameCount = -1;

    KeyOrderedPageFrameRecordCursor(
            CairoConfiguration configuration,
            @Transient RecordMetadata metadata,
            KeyOrderedRowCursorFactory rowCursorFactory,
            boolean entityCursor,
            @Nullable Function filter
    ) {
        super(configuration, metadata, rowCursorFactory, entityCursor, filter);
        this.keyOrderedFactory = rowCursorFactory;
        this.decodedRowSize = getDecodedRowSize(metadata);
        try {
            gatheredRows = new KeyOrderedRowChain(configuration, metadata, this);
            keyRowCounts = new DirectLongList(16, MemoryTag.NATIVE_DEFAULT, true);
        } catch (Throwable th) {
            close();
            throw th;
        }
    }

    @Override
    public void calculateSize(SqlExecutionCircuitBreaker circuitBreaker, RecordCursor.Counter counter) {
        prepareRowCursorFactory();
        if (isGathering) {
            calculateGatheredSize(counter);
            return;
        }
        if (isKeyOrderedByIndex() && isSizeWalked()) {
            // the skip counts the rows by the index, without decoding a page frame
            if (!isExhausted) {
                countCounter.set(Long.MAX_VALUE);
                skipKeyOrderedRows(countCounter);
                counter.add(Long.MAX_VALUE - countCounter.get());
                isExhausted = true;
            }
            return;
        }
        super.calculateSize(circuitBreaker, counter);
    }

    @Override
    public void close() {
        super.close();
        Misc.free(gatheredRows);
        Misc.free(keyRowCounts);
    }

    @Override
    public Record getRecord() {
        return isGathering ? gatheredRows.getRecord() : recordA;
    }

    @Override
    public Record getRecordB() {
        return isGathering ? gatheredRows.getRecordB() : recordB;
    }

    @Override
    public boolean hasNext() {
        if (isExhausted) {
            return false;
        }
        prepareRowCursorFactory();
        if (isGathering) {
            if (!isGatherPositioned) {
                positionGatheredRows();
            }
            return gatheredRows.hasNext();
        }
        try {
            // frames are only decoded up to the cap; rows past it are undecoded memory
            if (rowsProducedSinceSkip >= maxRowsAfterSkip) {
                isExhausted = true;
                return false;
            }
            if (rowCursor != null && rowCursor.hasNext()) {
                final int frameIndex = frameCount - 1;
                final long rowIndex = rowCursor.next();
                frameMemoryPool.navigateTo(frameIndex, recordA);
                recordA.setRowIndex(rowIndex);
                rowsProducedSinceSkip++;
                return true;
            }

            dropSkipWalkFramesAtTop();
            PageFrame frame = nextFrame();
            while (frame != null) {
                circuitBreaker.statefulThrowExceptionIfTrippedTimeThrottledOrYield();
                frameAddressCache.add(frameCount, frame);
                final long remaining = maxRowsAfterSkip - rowsProducedSinceSkip;
                final long frameSize = frame.getPartitionHi() - frame.getPartitionLo();
                final int inFrameHi = (int) Math.min(Math.min(frameSize, remaining), Integer.MAX_VALUE);
                PageFrameMemory frameMemory = frame.getFormat() == PartitionFormat.PARQUET && isKeyOrderedByIndex()
                        ? null : frameMemoryPool.navigateTo(frameCount, inFrameHi);
                frameCount++;
                rowCursor = Misc.free(rowCursor);
                rowCursor = keyCount > 1 ? keyOrderedFactory.getCursor(frame, frameMemory, keyLo, keyHi)
                        : keyOrderedFactory.getCursor(frame, frameMemory);
                countReadFrame();
                if (rowCursor.hasNext()) {
                    if (frameMemory == null) {
                        frameMemory = frameMemoryPool.navigateTo(frameCount - 1, inFrameHi);
                    }
                    recordA.init(frameMemory);
                    recordA.setRowIndex(rowCursor.next());
                    rowsProducedSinceSkip++;
                    return true;
                }
                frame = (frameCount == 1 || frame.getFormat() == PartitionFormat.PARQUET)
                        && keyHi - keyLo == 1 && isKeyOrderedByIndex() ? nextKeyFrame(frame) : nextFrame();
            }
        } catch (NoMoreFramesException ignore) {
            isExhausted = true;
            return false;
        }

        isExhausted = true;
        return false;
    }

    @Override
    public void of(PageFrameCursor frameCursor, SqlExecutionContext sqlExecutionContext) throws SqlException {
        super.of(frameCursor, sqlExecutionContext);
        scanFrameCount = -1;
        isGathering = false;
        gatheredRows.of(sqlExecutionContext.getMemoryTracker());
        keyRowCounts.close();
        keyRowCounts.setMemoryTracker(sqlExecutionContext.getMemoryTracker());
        countedKeyCount = 0;
        isKeyRowCountUnknown = false;
        gatherLo = 0;
        gatherHi = Long.MAX_VALUE;
        gatheredLo = -1;
        isGatherPositioned = false;
        isGathering = isGatherRequired();
        if (isGathering) {
            frameMemoryPool.setParquetDecodeHint(ParquetDecodeHint.MONOTONIC);
        }
    }

    @Override
    public long preComputedStateSize() {
        return super.preComputedStateSize() + RecordCursor.fromBool(gatheredLo >= 0);
    }

    @Override
    public void recordAt(Record record, long rowId) {
        if (isGathering) {
            gatheredRows.recordAt(record, rowId);
        } else {
            super.recordAt(record, rowId);
        }
    }

    @Override
    public void setParquetDecodeHint(ParquetDecodeHint hint) {
        // a consumer of gathered rows revisits them in the chain, never in the page frames
        if (!isGathering) {
            super.setParquetDecodeHint(hint);
        }
    }

    @Override
    public void setRecordAtRows(@Nullable RowIdSource source) {
        if (!isGathering) {
            super.setRecordAtRows(source);
        }
    }

    @Override
    public void skipRows(Counter rowCount, long requestedMaxRowsAfterSkip) {
        prepareRowCursorFactory();
        if (isGathering) {
            skipGatheredRows(rowCount, requestedMaxRowsAfterSkip);
            return;
        }
        if (isKeyOrderedByIndex()) {
            rowsProducedSinceSkip = 0;
            maxRowsAfterSkip = isSkipClampable() ? requestedMaxRowsAfterSkip : RecordCursor.UNBOUNDED_ROW_COUNT;
            skipKeyOrderedRows(rowCount);
            return;
        }
        super.skipRows(rowCount, requestedMaxRowsAfterSkip);
    }

    @Override
    public void toTop() {
        super.toTop();
        keyLo = 0;
        keyHi = Math.min(keyCount, 1);
        scanFrameCount = -1;
        gatherLo = 0;
        gatherHi = Long.MAX_VALUE;
        isGatherPositioned = false;
    }

    @Override
    protected boolean isSizeWalked() {
        return keyCount > 1 && (keyLo > 0 || frameCount > 0) || super.isSizeWalked();
    }

    @Override
    protected void prepareRowCursors() {
        super.prepareRowCursors();
        keyCount = keyOrderedFactory.getKeyCount();
        if (keyCount > 1) {
            // a consumer that revisits rows in any order keeps its SCATTERED hint
            if (frameMemoryPool.getDecodeHint() == ParquetDecodeHint.MONOTONIC) {
                frameMemoryPool.setParquetDecodeHint(ParquetDecodeHint.CYCLIC);
            }
            // a read ordered by keys opens an index cursor per key and page frame, and lands its skips by index
            // counts, so a page frame per partition frame costs it least
            frameCursor.setPageFrameMaxRows(Integer.MAX_VALUE);
        }
        keyLo = 0;
        keyHi = Math.min(keyCount, 1);
    }

    private static void countReadFrame() {
        if (isWalkCounterEnabled) {
            testReadFrames.incrementAndGet();
        }
    }

    /**
     * The bytes a row of these columns decodes to from Parquet, counting only the fixed part of a var-size column.
     */
    private static int getDecodedRowSize(RecordMetadata metadata) {
        int size = 0;
        for (int i = 0, n = metadata.getColumnCount(); i < n; i++) {
            final int columnType = metadata.getColumnType(i);
            if (ColumnType.isVarSize(columnType)) {
                final ColumnTypeDriver driver = ColumnType.getDriver(columnType);
                size += (int) (driver.auxRowsToBytes(1) + driver.getDataVectorMinEntrySize());
            } else {
                size += Math.max(ColumnType.sizeOf(columnType), 0);
            }
        }
        return size;
    }

    private void calculateGatheredSize(Counter counter) {
        if (isExhausted) {
            return;
        }
        if (!isGatherPositioned) {
            long rowCount = size();
            if (rowCount < 0) {
                rowCount = countRowsTo(Long.MAX_VALUE);
            }
            if (rowCount >= 0) {
                counter.add(Math.max(0, Math.min(rowCount, gatherHi) - gatherLo));
                isExhausted = true;
                return;
            }
            positionGatheredRows();
        }
        while (gatheredRows.hasNext()) {
            counter.inc();
        }
        isExhausted = true;
    }

    /**
     * The rows of the key over the whole scan, which the index counts partition by partition, or -1 when it cannot.
     */
    private long countKeyRows(int key) {
        toTopFrames();
        long count = 0;
        PageFrame frame;
        while ((frame = frameCursor.next(Long.MAX_VALUE)) != null) {
            circuitBreaker.statefulThrowExceptionIfTrippedTimeThrottledOrYield();
            final long frameRowCount = isCompositePartition(frame) ? -1
                    : keyOrderedFactory.getRowCount(frame, key, key + 1, frame.getPartitionLo(), frame.getPartitionHi() - 1, Long.MAX_VALUE);
            if (frameRowCount < 0) {
                return -1;
            }
            count += frameRowCount;
        }
        return count;
    }

    /**
     * The rows of the keys up to the first key whose rows bring the count to the target, or of all keys when no key
     * does, or -1 when the index cannot count them.
     */
    private long countRowsTo(long target) {
        long count = 0;
        for (int key = 0; key < keyCount; key++) {
            count = getCumulativeRowCount(key);
            if (count < 0 || count >= target) {
                return count;
            }
        }
        return count;
    }

    private int findCachedFrame(int partitionIndex, long row) {
        final long rowId = Rows.toRowID(partitionIndex, row);
        final boolean isBackward = isBackward();
        int lo = frameCount;
        int hi = frameAddressCache.getFrameCount() - 1;
        while (lo <= hi) {
            final int mid = (lo + hi) >>> 1;
            final long frameRowId = frameAddressCache.getRowIdOffset(mid);
            if (isBackward ? frameRowId + frameAddressCache.getFrameSize(mid) - 1 >= rowId : frameRowId <= rowId) {
                lo = mid + 1;
            } else {
                hi = mid - 1;
            }
        }
        return hi;
    }

    /**
     * Gathers the rows at positions {@code gatherLo} to {@code gatherHi} of the key order with one pass over the page
     * frames in frame order, which decodes a page frame once, and only when it holds a row the window takes. The row
     * counts of the keys place the window among the keys and their rows; without them the pass gathers every row.
     */
    private void gather() {
        long lo = gatherLo;
        long hi = gatherHi;
        int gatherKeyLo = 0;
        int gatherKeyHi = keyCount;
        long rankLo = 0;
        long rankHi = Long.MAX_VALUE;
        if (lo > 0 || hi < Long.MAX_VALUE) {
            long count = 0;
            long previousCount = 0;
            int key = 0;
            while (key < keyCount && (count = getCumulativeRowCount(key)) >= 0 && count <= lo) {
                previousCount = count;
                key++;
            }
            gatherKeyLo = key;
            rankLo = lo - previousCount;
            while (count >= 0 && key < keyCount && (count = getCumulativeRowCount(key)) >= 0 && count < hi) {
                previousCount = count;
                key++;
            }
            if (count < 0) {
                lo = 0;
                hi = Long.MAX_VALUE;
                gatherKeyLo = 0;
                rankLo = 0;
            } else if (key < keyCount) {
                gatherKeyHi = key + 1;
                rankHi = hi - previousCount;
            }
        }

        gatheredRows.reset(gatherKeyLo, gatherKeyHi);
        final boolean isByIndex = isKeyOrderedByIndex();
        long remaining = hi == Long.MAX_VALUE ? Long.MAX_VALUE : hi - lo;
        toTopFrames();
        PageFrame frame;
        while (remaining > 0 && gatherKeyLo < gatherKeyHi && (frame = frameCursor.next()) != null) {
            circuitBreaker.statefulThrowExceptionIfTrippedTimeThrottledOrYield();
            frameAddressCache.add(frameCount, frame);
            final int frameIndex = frameCount++;
            PageFrameMemory frameMemory = null;
            if (!isByIndex) {
                frameMemory = frameMemoryPool.navigateTo(frameIndex);
                recordA.init(frameMemory);
            }
            for (int key = gatherKeyLo; key < gatherKeyHi && remaining > 0; key++) {
                final long keyRankLo = key == gatherKeyLo ? rankLo : 0;
                final long keyRankHi = key == gatherKeyHi - 1 ? rankHi : Long.MAX_VALUE;
                long rank = gatheredRows.getRank(key);
                if (rank >= keyRankHi) {
                    continue;
                }
                if (rank < keyRankLo) {
                    final long count = keyOrderedFactory.getRowCount(frame, key, key + 1, frame.getPartitionLo(), frame.getPartitionHi() - 1, keyRankLo - rank);
                    if (count >= 0 && rank + count <= keyRankLo) {
                        gatheredRows.setRank(key, rank + count);
                        continue;
                    }
                }
                rowCursor = keyOrderedFactory.getCursor(frame, frameMemory, key, key + 1);
                while (rowCursor.hasNext()) {
                    final long row = rowCursor.next();
                    if (rank++ < keyRankLo) {
                        continue;
                    }
                    if (frameMemory == null) {
                        frameMemory = frameMemoryPool.navigateTo(frameIndex);
                        recordA.init(frameMemory);
                    }
                    recordA.setRowIndex(row);
                    gatheredRows.put(key, recordA);
                    if (--remaining == 0 || rank == keyRankHi) {
                        break;
                    }
                }
                gatheredRows.setRank(key, rank);
                rowCursor = Misc.free(rowCursor);
            }
        }
        gatheredLo = lo;
        gatheredHi = gatherKeyHi == keyCount && rankHi == Long.MAX_VALUE ? Long.MAX_VALUE : hi;
    }

    /**
     * The rows of the keys up to and including the key, which the index counts once per read, or -1 when it cannot.
     */
    private long getCumulativeRowCount(int key) {
        while (countedKeyCount <= key && !isKeyRowCountUnknown) {
            final long count = isKeyOrderedByIndex() ? countKeyRows(countedKeyCount) : -1;
            if (count < 0) {
                isKeyRowCountUnknown = true;
            } else {
                keyRowCounts.ensureCapacity(1);
                keyRowCounts.add((countedKeyCount > 0 ? keyRowCounts.get(countedKeyCount - 1) : 0) + count);
                countedKeyCount++;
            }
        }
        return isKeyRowCountUnknown ? -1 : keyRowCounts.get(key);
    }

    /**
     * The row after a skip of {@code rowIndex} rows of the key among the rows {@code rowLo} to {@code rowHi}, where it
     * has {@code count} rows, more than the skip: the index finds it, unless the key holds every row there, which places
     * it by arithmetic.
     */
    private long getLandingRow(PageFrame frame, int key, long rowLo, long rowHi, long count, long rowIndex) {
        if (count == rowHi - rowLo + 1) {
            return isBackward() ? rowHi - rowIndex : rowLo + rowIndex;
        }
        return keyOrderedFactory.getRow(frame, key, rowLo, rowHi, rowIndex);
    }

    private boolean isBackward() {
        return keyOrderedFactory.getIndexDirection() == IndexReader.DIR_BACKWARD;
    }

    private boolean isCompositePartition(PageFrame frame) {
        return frameCursor instanceof TablePageFrameCursor tableCursor
                && tableCursor.getTableReader().getTxFile().isPartitionComposite(frame.getPartitionIndex());
    }

    /**
     * True when a read ordered by keys gathers its rows: the Parquet page frames it scans do not stay decoded across
     * the passes of its keys, being more than the decode cache keeps or decoding, by the sizes of the column types, to
     * more than its budget, so that every pass would decode them again.
     */
    private boolean isGatherRequired() {
        if (!(frameCursor instanceof TablePageFrameCursor tableCursor) || !tableCursor.getTableReader().hasParquetPartitions()) {
            return false;
        }
        prepareRowCursorFactory();
        if (keyCount < 2) {
            return false;
        }
        try {
            int parquetFrameCount = 0;
            long parquetRowCount = 0;
            PageFrame frame;
            while ((frame = frameCursor.next()) != null) {
                if (frame.getFormat() == PartitionFormat.PARQUET) {
                    parquetFrameCount++;
                    parquetRowCount += frame.getPartitionHi() - frame.getPartitionLo();
                    if (!frameMemoryPool.isCachingFrames(parquetFrameCount, parquetRowCount * decodedRowSize)) {
                        return true;
                    }
                }
            }
            return false;
        } finally {
            frameCursor.toTop();
        }
    }

    // The rows of a key-ordered read are the rows the index holds for its keys only when no filter drops some of them
    // and no pushdown filter prunes the row groups that hold them.
    private boolean isKeyOrderedByIndex() {
        return keyCount > 1 && filter == null && !frameCursor.hasActivePushdownFilter();
    }

    /**
     * The next page frame. A read ordered by keys passes over the page frames once for each key, or, when the scan
     * reads a single page frame, once for the first key and once for all the others.
     */
    private @Nullable PageFrame nextFrame() {
        PageFrame frame = frameCursor.next();
        while (frame == null) {
            scanFrameCount = frameCount;
            if (!nextKeys()) {
                return null;
            }
            frame = frameCursor.next();
        }
        return frame;
    }

    /**
     * The next page frame after an empty frame of a single key pass. Within the current interval the index finds the
     * next row of the key, or the last frame when no key rows remain. Only the first frame can rewind to probe later
     * intervals as whole partition frames, so an absent key reads one page frame and probes once per interval.
     */
    private @Nullable PageFrame nextKeyFrame(PageFrame frame) {
        if (isCompositePartition(frame)) {
            return nextFrame();
        }
        final long restRowCount = frameCursor.getRemainingRowsInInterval();
        if (restRowCount > 0) {
            final long restLo = isBackward() ? frame.getPartitionLo() - restRowCount : frame.getPartitionHi();
            final long restHi = restLo + restRowCount - 1;
            final long count = keyOrderedFactory.getRowCount(frame, keyLo, keyHi, restLo, restHi, 0);
            if (count < 0) {
                return nextFrame();
            }
            if (count > 0) {
                return seekFrame(frame.getPartitionIndex(), keyOrderedFactory.getRow(frame, keyLo, restLo, restHi, 0));
            }
            if (frameCount > 1) {
                return seekFrame(frame.getPartitionIndex(), isBackward() ? restLo : restHi);
            }
        }
        if (frameCount > 1) {
            return nextFrame();
        }
        toTopFrames();
        frameCursor.next(Long.MAX_VALUE);
        PageFrame partitionFrame = frameCursor.next(Long.MAX_VALUE);
        if (partitionFrame == null && restRowCount == 0) {
            scanFrameCount = 1;
        }
        while (partitionFrame != null) {
            circuitBreaker.statefulThrowExceptionIfTrippedTimeThrottledOrYield();
            final long rowLo = partitionFrame.getPartitionLo();
            final long rowHi = partitionFrame.getPartitionHi() - 1;
            final long count = isCompositePartition(partitionFrame) ? -1
                    : keyOrderedFactory.getRowCount(partitionFrame, keyLo, keyHi, rowLo, rowHi, 0);
            if (count != 0) {
                final int partitionIndex = partitionFrame.getPartitionIndex();
                final long row = count > 0 ? keyOrderedFactory.getRow(partitionFrame, keyLo, rowLo, rowHi, 0) : isBackward() ? rowHi : rowLo;
                toTopFrames();
                return seekFrame(partitionIndex, row);
            }
            partitionFrame = frameCursor.next(Long.MAX_VALUE);
        }
        return nextKeys() ? nextFrame() : null;
    }

    /**
     * Moves a read ordered by keys on to the keys after the pass that ended: all the remaining keys when the scan reads
     * a single page frame, the next key otherwise or while the number of page frames is unknown.
     */
    private boolean nextKeys() {
        if (keyHi >= keyCount || scanFrameCount == 0) {
            return false;
        }
        keyLo = keyHi;
        keyHi = scanFrameCount == 1 ? keyCount : keyHi + 1;
        toTopFrames();
        return true;
    }

    /**
     * Positions the gathered rows at the row {@code gatherLo}, gathering them first unless the window the read
     * gathered holds the window from {@code gatherLo} to {@code gatherHi}.
     */
    private void positionGatheredRows() {
        if (gatheredLo < 0 || gatherLo < gatheredLo || gatherHi > gatheredHi) {
            gather();
        }
        gatheredRows.toTop();
        for (long i = gatheredLo; i < gatherLo && gatheredRows.hasNext(); i++) {
            circuitBreaker.statefulThrowExceptionIfTrippedOrYield();
        }
        isGatherPositioned = true;
    }

    /**
     * Opens the cursor of the keys of the pass over the page frame and skips its rows up to the row count.
     */
    private void readFrameRows(PageFrame frame, Counter rowCount) {
        circuitBreaker.statefulThrowExceptionIfTrippedTimeThrottledOrYield();
        frameAddressCache.add(frameCount, frame);
        final int frameIndex = frameCount++;
        final PageFrameMemory frameMemory = frame.getFormat() == PartitionFormat.PARQUET && isKeyOrderedByIndex()
                ? null : frameMemoryPool.navigateTo(frameIndex);
        rowCursor = keyOrderedFactory.getCursor(frame, frameMemory, keyLo, keyHi);
        countReadFrame();
        skipRowCursorRows(rowCount);
    }

    /**
     * Opens the cursor of the keys of the pass from {@code key} on over the page frame, starting the rows of that key at
     * {@code firstRow}.
     */
    private void readFrameRowsFrom(PageFrame frame, int key, long firstRow) {
        circuitBreaker.statefulThrowExceptionIfTrippedTimeThrottledOrYield();
        frameAddressCache.add(frameCount, frame);
        rowCursor = keyOrderedFactory.getCursor(frame, frameMemoryPool.navigateTo(frameCount++), key, keyHi, firstRow);
        countReadFrame();
    }

    /**
     * Moves the frame cursor on to the page frame of the partition that holds the row, caching the addresses of the
     * page frames it passes, and returns that page frame unread.
     */
    private @Nullable PageFrame seekFrame(int partitionIndex, long row) {
        final int cachedFrameIndex = findCachedFrame(partitionIndex, row);
        PageFrame frame;
        while ((frame = frameCursor.next()) != null
                && (frame.getPartitionIndex() != partitionIndex || row < frame.getPartitionLo() || row >= frame.getPartitionHi())) {
            circuitBreaker.statefulThrowExceptionIfTrippedTimeThrottledOrYield();
            frameAddressCache.add(frameCount++, frame);
            if (cachedFrameIndex > frameCount
                    && frameCursor instanceof TablePageFrameCursor tableCursor
                    && tableCursor.skipToCachedFrame(frameAddressCache, cachedFrameIndex)) {
                frameCount = cachedFrameIndex;
            }
        }
        return frame;
    }

    /**
     * Skips the rows of a gathered read. Before the read gathers, the skip moves the window the read gathers, by the
     * row counts of the keys, and the requested limit bounds it.
     */
    private void skipGatheredRows(Counter rowCount, long requestedMaxRowsAfterSkip) {
        if (isExhausted) {
            return;
        }
        if (!isGatherPositioned) {
            final long skipCount = rowCount.get();
            final long count = skipCount > 0 ? countRowsTo(skipCount > Long.MAX_VALUE - gatherLo ? Long.MAX_VALUE : gatherLo + skipCount) : 0;
            if (count >= 0) {
                final long skippedCount = skipCount > 0 ? Math.max(0, Math.min(skipCount, count - gatherLo)) : 0;
                gatherLo += skippedCount;
                rowCount.dec(skippedCount);
                gatherHi = requestedMaxRowsAfterSkip > Long.MAX_VALUE - gatherLo ? Long.MAX_VALUE : gatherLo + requestedMaxRowsAfterSkip;
                return;
            }
            positionGatheredRows();
        }
        while (rowCount.get() > 0 && gatheredRows.hasNext()) {
            rowCount.dec();
        }
    }

    /**
     * Skips the rows of a read ordered by keys pass by pass.
     */
    private void skipKeyOrderedRows(Counter rowCount) {
        if (rowCount.get() == 0) {
            return;
        }
        if (rowCursor != null) {
            skipRowCursorRows(rowCount);
            if (rowCount.get() == 0) {
                return;
            }
            rowCursor = Misc.free(rowCursor);
        }
        boolean isInPass = frameCount > 0 ? skipPassFrames(frameCursor.next(), rowCount) : skipPass(rowCount);
        while (!isInPass) {
            if (!nextKeys()) {
                isExhausted = true;
                return;
            }
            isInPass = skipPass(rowCount);
        }
    }

    /**
     * Skips the rows of the pass that starts at the next page frame, and reports whether the skip ends inside the pass.
     * A skip that ends in the first page frame lands there, since the read after the skip opens it anyway. Otherwise
     * the skip counts the rows of the pass partition frame by partition frame, and goes on page frame by page frame only
     * in the partition frame where it ends. There the index finds the row after the skip, unless the pass has a single
     * key that holds every row of the partition frame, which places that row by arithmetic.
     */
    private boolean skipPass(Counter rowCount) {
        PageFrame frame = frameCursor.next();
        if (frame == null) {
            scanFrameCount = 0;
            return false;
        }
        if (isCompositePartition(frame)) {
            return skipPassFrames(frame, rowCount);
        }
        final long restRowCount = frameCursor.getRemainingRowsInInterval();
        final long skipCount = rowCount.get();
        final long firstFrameRowCount = skipCount < frame.getPartitionHi() - frame.getPartitionLo()
                ? keyOrderedFactory.getRowCount(frame, keyLo, keyHi, frame.getPartitionLo(), frame.getPartitionHi() - 1, skipCount)
                : 0;
        if (firstFrameRowCount > skipCount) {
            skipFrameKeys(frame, rowCount);
            return true;
        }
        if (firstFrameRowCount < 0) {
            readFrameRows(frame, rowCount);
            if (rowCount.get() == 0) {
                return true;
            }
            rowCount.add(skipCount - rowCount.get());
            rowCursor = Misc.free(rowCursor);
        }
        toTopFrames();
        int partitionFrameCount = 0;
        while ((frame = frameCursor.next(Long.MAX_VALUE)) != null) {
            circuitBreaker.statefulThrowExceptionIfTrippedTimeThrottledOrYield();
            partitionFrameCount++;
            final long rowLo = frame.getPartitionLo();
            final long rowHi = frame.getPartitionHi() - 1;
            final long count = isCompositePartition(frame) ? -1
                    : keyOrderedFactory.getRowCount(frame, keyLo, keyHi, rowLo, rowHi, rowCount.get());
            if (count >= 0 && count <= rowCount.get()) {
                rowCount.dec(count);
                continue;
            }
            final int partitionIndex = frame.getPartitionIndex();
            if (count < 0 || keyHi - keyLo > 1) {
                toTopFrames();
                return skipPassFrames(seekFrame(partitionIndex, isBackward() ? rowHi : rowLo), rowCount);
            }
            final long row = getLandingRow(frame, keyLo, rowLo, rowHi, count, rowCount.get());
            toTopFrames();
            readFrameRowsFrom(seekFrame(partitionIndex, row), keyLo, row);
            rowCount.dec(rowCount.get());
            return true;
        }
        if (partitionFrameCount == 1 && restRowCount == 0) {
            scanFrameCount = 1;
        }
        return false;
    }

    /**
     * Skips the rows of the pass page frame by page frame from the given page frame on, a whole page frame at a time
     * when the factory counts its rows, and reports whether the skip ends inside the pass.
     */
    private boolean skipPassFrames(@Nullable PageFrame frame, Counter rowCount) {
        while (frame != null) {
            final long count = keyOrderedFactory.getRowCount(frame, keyLo, keyHi, frame.getPartitionLo(), frame.getPartitionHi() - 1, rowCount.get());
            if (count >= 0 && count <= rowCount.get()) {
                circuitBreaker.statefulThrowExceptionIfTrippedTimeThrottledOrYield();
                frameAddressCache.add(frameCount++, frame);
                rowCount.dec(count);
                if (rowCount.get() == 0) {
                    return true;
                }
            } else if (count >= 0) {
                skipFrameKeys(frame, rowCount);
                return true;
            } else {
                readFrameRows(frame, rowCount);
                if (rowCount.get() == 0) {
                    return true;
                }
                rowCursor = Misc.free(rowCursor);
            }
            frame = frameCursor.next();
        }
        scanFrameCount = frameCount;
        return false;
    }

    /**
     * Skips rows of the pass inside the page frame, which holds more rows of the pass than the skip: the rows of whole
     * keys by their counts, then those of the key where the skip ends, whose cursor starts at the row the index finds.
     */
    private void skipFrameKeys(PageFrame frame, Counter rowCount) {
        final long rowLo = frame.getPartitionLo();
        final long rowHi = frame.getPartitionHi() - 1;
        int key = keyLo;
        long count;
        while ((count = keyOrderedFactory.getRowCount(frame, key, key + 1, rowLo, rowHi, rowCount.get())) <= rowCount.get()) {
            rowCount.dec(count);
            key++;
        }
        readFrameRowsFrom(frame, key, getLandingRow(frame, key, rowLo, rowHi, count, rowCount.get()));
        rowCount.dec(rowCount.get());
    }

    private void skipRowCursorRows(Counter rowCount) {
        final long skipCount = rowCount.get();
        while (rowCount.get() > 0 && rowCursor.hasNext()) {
            rowCursor.next();
            rowCount.dec();
        }
        countWalkedRows(skipCount - rowCount.get());
    }

    /**
     * The rows of a range of keys of a read ordered by keys, which a pass over the page frames in frame order appends
     * to a record chain, linking the rows of each key in the order the pass reads them, so that the read returns them
     * key by key.
     */
    private static class KeyOrderedRowChain implements QuietCloseable {
        // the first and the last record of a key, and the rows of the key the pass went over
        private static final int KEY_ENTRY_SIZE = 3;
        private final RecordChain chain;
        private final DirectLongList keyEntries;
        private int key;
        private int keyHi;
        private int keyLo;

        private KeyOrderedRowChain(CairoConfiguration configuration, RecordMetadata metadata, SymbolTableSource symbolTableSource) {
            final EntityColumnFilter columnFilter = new EntityColumnFilter();
            columnFilter.of(metadata.getColumnCount());
            final long pageSize = configuration.getSqlSortValuePageSize();
            final long maxPages = Math.max(1L, configuration.getSqlSortValueMaxBytes() / Numbers.ceilPow2(pageSize));
            chain = new RecordChain(
                    metadata,
                    new LoopingRecordSink(metadata, columnFilter, null, null, null, null),
                    pageSize,
                    (int) Math.min(maxPages, Integer.MAX_VALUE),
                    PropertyKey.CAIRO_SQL_SORT_VALUE_MAX_BYTES.getPropertyPath()
            );
            chain.setSymbolTableResolver(symbolTableSource);
            keyEntries = new DirectLongList(KEY_ENTRY_SIZE, MemoryTag.NATIVE_DEFAULT, true);
        }

        @Override
        public void close() {
            chain.clear();
            keyEntries.close();
            keyLo = 0;
            keyHi = 0;
            key = 0;
        }

        private long entryOf(int key) {
            return (long) (key - keyLo) * KEY_ENTRY_SIZE;
        }

        private long getRank(int key) {
            return keyEntries.get(entryOf(key) + 2);
        }

        private Record getRecord() {
            return chain.getRecord();
        }

        private Record getRecordB() {
            return chain.getRecordB();
        }

        private boolean hasNext() {
            while (!chain.hasNext()) {
                if (++key >= keyHi) {
                    key = keyHi;
                    return false;
                }
                chain.of(keyEntries.get(entryOf(key)));
            }
            return true;
        }

        private void of(MemoryTracker memoryTracker) {
            close();
            chain.setMemoryTracker(memoryTracker);
            keyEntries.setMemoryTracker(memoryTracker);
        }

        private void put(int key, Record record) {
            final long entry = entryOf(key);
            final long lastRecord = keyEntries.get(entry + 1);
            final long offset = chain.put(record, lastRecord);
            if (lastRecord == -1) {
                keyEntries.set(entry, offset);
            }
            keyEntries.set(entry + 1, offset);
        }

        private void recordAt(Record record, long rowId) {
            chain.recordAt(record, rowId);
        }

        /**
         * Drops the gathered rows and starts gathering the rows of the keys at {@code keyLo}, inclusive, to
         * {@code keyHi}, exclusive.
         */
        private void reset(int keyLo, int keyHi) {
            chain.clear();
            keyEntries.clear();
            this.keyLo = keyLo;
            this.keyHi = keyHi;
            key = keyLo - 1;
            if (keyHi > keyLo) {
                keyEntries.ensureCapacity((long) (keyHi - keyLo) * KEY_ENTRY_SIZE);
                for (int i = keyLo; i < keyHi; i++) {
                    keyEntries.add(-1);
                    keyEntries.add(-1);
                    keyEntries.add(0);
                }
            }
        }

        private void setRank(int key, long rank) {
            keyEntries.set(entryOf(key) + 2, rank);
        }

        private void toTop() {
            key = keyLo - 1;
            chain.of(-1);
        }
    }
}
