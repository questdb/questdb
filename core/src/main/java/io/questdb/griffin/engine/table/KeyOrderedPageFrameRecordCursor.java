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
import io.questdb.cairo.ArrayColumnTypes;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.ColumnTypeDriver;
import io.questdb.cairo.EntityColumnFilter;
import io.questdb.cairo.LoopingRecordSink;
import io.questdb.cairo.RecordChain;
import io.questdb.cairo.RecordSink;
import io.questdb.cairo.RecordSinkSPI;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.arr.ArrayTypeDriver;
import io.questdb.cairo.idx.IndexReader;
import io.questdb.cairo.sql.DelegatingRecord;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.PageFrame;
import io.questdb.cairo.sql.PageFrameCursor;
import io.questdb.cairo.sql.PageFrameMemory;
import io.questdb.cairo.sql.PageFrameMemoryRecord;
import io.questdb.cairo.sql.ParquetDecodeHint;
import io.questdb.cairo.sql.PartitionFormat;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.RowCursor;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.cairo.vm.Vm;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.DirectIntList;
import io.questdb.std.DirectLongList;
import io.questdb.std.Files;
import io.questdb.std.IntList;
import io.questdb.std.MemoryTag;
import io.questdb.std.MemoryTracker;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.QuietCloseable;
import io.questdb.std.Rows;
import io.questdb.std.Transient;
import org.jetbrains.annotations.Nullable;

/**
 * Reads the rows of a {@link KeyOrderedRowCursorFactory} key by key over all page frames, or, when its Parquet page
 * frames do not stay decoded across the passes of the keys, gathers them window by window, each window in one pass over
 * the page frames that decodes each of them once, and returns the rows of a window key by key. A window holds what the
 * budget of the gather allows, see {@link #getGatherBudget(long)}, so a read whose rows fit it gathers them in one pass.
 * The windows rest on estimates of the bytes of the rows, which the gather measures row by row: a window ends where its
 * rows outgrow the memory of the record chain, see {@link #gatherWindow(long, int, long)}.
 */
final class KeyOrderedPageFrameRecordCursor extends PageFrameRecordCursorImpl {
    // The bytes of a row in the record chain of a gather, past the values of its var-size columns.
    private final int chainRowSize;
    private final RecordCursor.Counter countCounter = new RecordCursor.Counter();
    private final int decodedRowSize;
    // The rows of a window of a read ordered by keys that one pass over the page frames gathers, when its Parquet page
    // frames do not stay decoded across the passes of the keys, see isGatherRequired().
    private final KeyOrderedRowChain gatheredRows;
    private final KeyOrderedRowCursorFactory keyOrderedFactory;
    // The cumulative counts of the index for the keys the gathered read counted, in key order: the rows of the keys of
    // a read that the index orders, and a bound on them otherwise.
    private final DirectLongList keyRowCounts;
    // The key that a dense scan of a gather finds for each index key of the column, by index key, or -1 for an index
    // key whose rows the gather does not take.
    private final DirectIntList symbolKeyIndexes;
    // The index and the type of each var-size column, in pairs.
    private final IntList varSizeColumns = new IntList();
    // The records of a gather whose rows the read holds in more than one way, see isDelegatingRecords.
    private final WindowRecord windowRecord;
    private final WindowRecord windowRecordB;
    // The window starts of a filtered gather that its passes went over from its first row on, a position in key order,
    // a key and a row of the key each, in triples, so that a skip lands in the window that holds its row.
    private final DirectLongList windowStarts;
    private int countedKeyCount;
    // The bytes a window of the gathered read holds by the plan of its windows, see planWindows().
    private long gatherBudget;
    // The window of rows, by position in key order, that the next read of a gathered read starts at and ends before.
    private long gatherHi = Long.MAX_VALUE;
    private long gatherLo;
    // The estimated bytes a gathered row takes in the record chain, and the rows the gathered read returns.
    private double gatherRowBytes;
    private long gatherRowCount;
    // The window of rows the gathered rows hold, gatheredHi at Long.MAX_VALUE when it runs to the last row, and
    // gatheredLo at -1 before the read gathers any.
    private long gatheredHi;
    private long gatheredLo = -1;
    // True when the records of the read delegate to the record of the rows that hold the row: a gather in windows, or a
    // filtered gather, whose rows its end may split into windows or the passes of the keys may read instead.
    private boolean isDelegatingRecords;
    private boolean isGatherPositioned;
    // True when the Parquet page frames of the scan stay decoded across the passes of the keys: no more than the decode
    // cache keeps under their hint, decoding, by the sizes of the column types, to no more than its budget.
    private boolean isParquetCached;
    // True when gatherRowCount is the rows the read returns, which the index counts, rather than an estimate.
    private boolean isGatherRowCountExact;
    private boolean isGathering;
    private boolean isKeyRowCountUnknown;
    // True until the first window of a filtered gather that fits its budget decides whether its rows carry their row
    // ids, which they do when the end of the read may split the read into windows.
    private boolean isLayoutPending;
    private int keyCount;
    private int keyHi;
    private int keyLo;
    private @Nullable MemoryTracker memoryTracker;
    // The key and the row of the key that the window after the window the gathered rows hold starts at.
    private int nextWindowKey;
    private long nextWindowRank;
    // The Parquet page frames of the scan and their rows.
    private int parquetFrameCount;
    private long parquetRowCount;
    // The number of page frames of the scan, known once a pass of a read ordered by keys reads them all, -1 before.
    private int scanFrameCount = -1;
    // The rows the index counts for the keys of a filtered read per row its filter is estimated to keep, which a window
    // takes as many of as it may hold rows.
    private double windowBoundScale = 1;
    // The decode hint of the passes of the keys, which a filtered read returns to when it reads them instead.
    private ParquetDecodeHint walkHint;
    // The rows a window of a gather holds at most, Long.MAX_VALUE when one window holds every row of the read.
    private long windowRowCount = Long.MAX_VALUE;
    // The position the window after the last of windowStarts starts at, and the rows of the read once a pass of the
    // windows reached its last row, -1 before.
    private long windowStartsHi;
    private long windowStartsRowCount = -1;

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
        this.chainRowSize = getChainRowSize(metadata);
        this.windowRecord = new WindowRecord(recordA, metadata.getColumnCount());
        this.windowRecordB = new WindowRecord(recordB, metadata.getColumnCount());
        for (int i = 0, n = metadata.getColumnCount(); i < n; i++) {
            final int columnType = metadata.getColumnType(i);
            if (ColumnType.isVarSize(columnType)) {
                varSizeColumns.add(i);
                varSizeColumns.add(columnType);
            }
        }
        try {
            gatheredRows = new KeyOrderedRowChain(configuration, metadata, this);
            keyRowCounts = new DirectLongList(16, MemoryTag.NATIVE_DEFAULT, true);
            symbolKeyIndexes = new DirectIntList(0, MemoryTag.NATIVE_DEFAULT, true);
            windowStarts = new DirectLongList(3, MemoryTag.NATIVE_DEFAULT, true);
        } catch (Throwable th) {
            close();
            throw th;
        }
    }

    @Override
    public void calculateSize(SqlExecutionCircuitBreaker circuitBreaker, RecordCursor.Counter counter) {
        prepareKeyOrderedRowCursors();
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
        Misc.free(symbolKeyIndexes);
        Misc.free(windowStarts);
    }

    @Override
    public Record getRecord() {
        if (isDelegatingRecords) {
            return windowRecord;
        }
        return isGathering ? gatheredRows.getRecord() : recordA;
    }

    @Override
    public Record getRecordB() {
        if (isDelegatingRecords) {
            return windowRecordB;
        }
        return isGathering ? gatheredRows.getRecordB() : recordB;
    }

    @Override
    public boolean hasNext() {
        if (isExhausted) {
            return false;
        }
        prepareKeyOrderedRowCursors();
        if (isGathering) {
            return hasNextGatheredRow();
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

            if (hasSkipWalkFramesAtTop()) {
                dropSkipWalkFrames();
            }
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
        memoryTracker = sqlExecutionContext.getMemoryTracker();
        gatheredRows.of(memoryTracker);
        keyRowCounts.close();
        keyRowCounts.setMemoryTracker(memoryTracker);
        symbolKeyIndexes.close();
        symbolKeyIndexes.setMemoryTracker(memoryTracker);
        windowStarts.close();
        windowStarts.setMemoryTracker(memoryTracker);
        windowStartsHi = 0;
        windowStartsRowCount = -1;
        countedKeyCount = 0;
        isKeyRowCountUnknown = false;
        gatherLo = 0;
        gatherHi = Long.MAX_VALUE;
        gatheredLo = -1;
        isGatherPositioned = false;
        isDelegatingRecords = false;
        isLayoutPending = false;
        try {
            if (isGatherRequired()) {
                planWindows();
                // the passes of the keys decode page frames that stay decoded once, and each window would decode them
                isGathering = windowRowCount == Long.MAX_VALUE || !isParquetCached;
                if (isGathering) {
                    // the bytes of var-size values may outgrow their sample, which splits the read into windows
                    isDelegatingRecords = gatheredRows.isWindowed || isLayoutPending || varSizeColumns.size() > 0;
                    windowRecord.of(recordA);
                    windowRecordB.of(recordB);
                    walkHint = frameMemoryPool.getDecodeHint();
                    // each pass of a gather decodes each page frame at most once
                    frameMemoryPool.setParquetDecodeHint(ParquetDecodeHint.MONOTONIC);
                } else {
                    isLayoutPending = false;
                }
            }
        } catch (Throwable th) {
            // a cursor that fails to open is not closed before the query that charged this memory ends
            recordA.clear();
            recordB.clear();
            frameMemoryPool.releaseParquetBuffers();
            keyRowCounts.close();
            throw th;
        }
    }

    @Override
    public long preComputedStateSize() {
        return super.preComputedStateSize() + RecordCursor.fromBool(gatheredLo >= 0);
    }

    @Override
    public void recordAt(Record record, long rowId) {
        final boolean isChained = isGathering && !gatheredRows.isWindowed;
        if (isDelegatingRecords && (record == windowRecord || record == windowRecordB)) {
            final WindowRecord delegatingRecord = (WindowRecord) record;
            if (isChained) {
                final Record chainRecord = record == windowRecord ? gatheredRows.getRecord() : gatheredRows.getRecordB();
                gatheredRows.recordAt(chainRecord, rowId);
                delegatingRecord.of(chainRecord);
            } else {
                super.recordAt(delegatingRecord.frameRecord, rowId);
                delegatingRecord.of(delegatingRecord.frameRecord);
            }
        } else if (isChained) {
            gatheredRows.recordAt(record, rowId);
        } else {
            super.recordAt(record, rowId);
        }
    }

    @Override
    public void setParquetDecodeHint(ParquetDecodeHint hint) {
        // the passes of a gather decode each page frame once, which the hint they set suits
        if (!isGathering) {
            super.setParquetDecodeHint(hint);
        }
    }

    @Override
    public void setRecordAtRows(@Nullable RowIdSource source) {
        // the passes of a gather decode whole page frames
        if (!isGathering) {
            super.setRecordAtRows(source);
        }
    }

    @Override
    public void skipRows(Counter rowCount, long requestedMaxRowsAfterSkip) {
        prepareKeyOrderedRowCursors();
        if (isLayoutPending && requestedMaxRowsAfterSkip != RecordCursor.UNBOUNDED_ROW_COUNT && isParquetCached) {
            // the passes of the keys over page frames that stay decoded end at the end of the read, which a window of a
            // filtered gather only bounds
            isGathering = false;
            isLayoutPending = false;
            frameMemoryPool.setParquetDecodeHint(walkHint);
            windowRecord.of(recordA);
            windowRecordB.of(recordB);
            toTopFrames();
        }
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

    private static void countDenseScanFrame() {
        if (isWalkCounterEnabled) {
            testDenseScanFrames.incrementAndGet();
        }
    }

    private static void countReadFrame() {
        if (isWalkCounterEnabled) {
            testReadFrames.incrementAndGet();
        }
    }

    /**
     * The bytes a row of these columns takes in a record chain, past the values of its var-size columns: the link to
     * the next row, the offset of each var-size value and the fixed-size values.
     */
    private static int getChainRowSize(RecordMetadata metadata) {
        int size = Long.BYTES;
        for (int i = 0, n = metadata.getColumnCount(); i < n; i++) {
            final int columnType = metadata.getColumnType(i);
            size += ColumnType.isVarSize(columnType) ? Long.BYTES : Math.max(ColumnType.sizeOf(columnType), 0);
        }
        return size;
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

    /**
     * The cost of a dense scan of a page frame of {@code rowCount} rows, in probes of the index for a key, which the
     * gather pays there instead of a probe per key. Measured over Parquet row groups: the scan decodes no more than the
     * probes of a page frame that holds a row of the window, but it appends the rows to the record chain in row order
     * rather than key by key, and reading them back key by key costs about a third of a probe more per row.
     */
    private static double getDenseScanCost(long rowCount) {
        return rowCount / 3.0;
    }

    /**
     * The bytes the var-size value takes in a record chain.
     */
    private static long getVarSizeBytes(Record record, int columnIndex, int columnType) {
        return switch (ColumnType.tagOf(columnType)) {
            case ColumnType.VARCHAR -> Integer.BYTES + Math.max(record.getVarcharSize(columnIndex), 0);
            case ColumnType.STRING -> Vm.getStorageLength(Math.max(record.getStrLen(columnIndex), 0));
            case ColumnType.BINARY -> Long.BYTES + Math.max(record.getBinLen(columnIndex), 0);
            default -> ArrayTypeDriver.getPlainValueSize(record.getArray(columnIndex, columnType));
        };
    }

    private void calculateGatheredSize(Counter counter) {
        if (isExhausted) {
            return;
        }
        if (!isGatherPositioned) {
            long rowCount = isGatherRowCountExact ? gatherRowCount : size();
            if (rowCount < 0) {
                rowCount = countRowsTo(Long.MAX_VALUE);
            }
            if (rowCount < 0) {
                rowCount = windowStartsRowCount;
            }
            if (rowCount >= 0) {
                counter.add(Math.max(0, Math.min(rowCount, gatherHi) - gatherLo));
                isExhausted = true;
                return;
            }
        }
        while (hasNextGatheredRow()) {
            counter.inc();
        }
        isExhausted = true;
    }

    /**
     * The rows the index holds for the key over the whole scan, which it counts partition by partition, or -1 when it
     * cannot.
     */
    private long countKeyRows(int key) {
        toTopFrames();
        long count = 0;
        PageFrame frame;
        while ((frame = frameCursor.next(Long.MAX_VALUE)) != null) {
            circuitBreaker.statefulThrowExceptionIfTrippedTimeThrottledOrYield();
            final long frameRowCount = isCompositePartition(frame) ? -1
                    : keyOrderedFactory.getIndexRowCount(frame, key, key + 1, frame.getPartitionLo(), frame.getPartitionHi() - 1, Long.MAX_VALUE);
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

    /**
     * The rows of a read that the index orders: those of every row of the scan for a read of every key of the column,
     * and those the index counts for the keys otherwise, partition by partition, or -1 when it cannot count them.
     */
    private long countGatheredRows() {
        if (!isKeyOrderedByIndex()) {
            return -1;
        }
        if (keyOrderedFactory.isReadingEveryKey()) {
            return countScanRows();
        }
        toTopFrames();
        long count = 0;
        PageFrame frame;
        while ((frame = frameCursor.next(Long.MAX_VALUE)) != null) {
            circuitBreaker.statefulThrowExceptionIfTrippedTimeThrottledOrYield();
            final long frameRowCount = isCompositePartition(frame) ? -1
                    : keyOrderedFactory.getRowCount(frame, 0, keyCount, frame.getPartitionLo(), frame.getPartitionHi() - 1, Long.MAX_VALUE);
            if (frameRowCount < 0) {
                return -1;
            }
            count += frameRowCount;
        }
        return count;
    }

    /**
     * The rows of the scan, which its partition frames hold.
     */
    private long countScanRows() {
        toTopFrames();
        long count = 0;
        PageFrame frame;
        while ((frame = frameCursor.next(Long.MAX_VALUE)) != null) {
            count += frame.getPartitionHi() - frame.getPartitionLo();
        }
        return count;
    }

    /**
     * Ends the window that filled the memory of the record chain before it held its rows, at the first row in key order
     * that it does not hold: the window holds every row of each key that the index counts no more rows of than its pass
     * went over, and the rows of the key after those keys up to the last its pass reached, which the pass gathers in
     * their order. The index does not count the rows a filter keeps, so the window of a filtered read ends within its
     * first key. Returns -1, or, when the window holds no row up to there, the key it would end within, and leaves the
     * window open.
     */
    private int endFilledWindow(long position, int keyLo, long rankLo) {
        int key = keyLo;
        long keyRankLo = rankLo;
        long rank = gatheredRows.getRank(key);
        long rowCount = Math.max(rank - keyRankLo, 0);
        while (key < gatheredRows.keyHi - 1 && isGatherRowCountExact && getCumulativeIndexRowCount(key) >= 0 && rank >= getKeyRowBound(key)) {
            key++;
            keyRankLo = 0;
            rank = gatheredRows.getRank(key);
            rowCount += rank;
        }
        if (rowCount == 0) {
            return key;
        }
        gatheredRows.endAt(rank > keyRankLo ? key + 1 : key);
        gatheredLo = position;
        gatheredHi = position + rowCount;
        nextWindowKey = key;
        nextWindowRank = Math.max(rank, keyRankLo);
        if (!isGatherRowCountExact) {
            recordWindowStart(position, keyLo, rankLo, rowCount);
        }
        return -1;
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
     * Gathers the rows of the page frame that the keys {@code keyLo} to {@code keyHi} hold, scanning the rows in the
     * index direction and finding the key of each by its symbol key, and returns the rows the window of the gather takes
     * after the page frame, none once the record chain does not take a row.
     */
    private long gatherFrame(int frameIndex, long rowCount, int keyLo, int keyHi, long rankLo, long rankHi, long remaining) {
        recordA.init(frameMemoryPool.navigateTo(frameIndex));
        final int columnIndex = keyOrderedFactory.getColumnIndex();
        final long indexKeyCount = symbolKeyIndexes.size();
        final long step = isBackward() ? -1 : 1;
        for (long row = step < 0 ? rowCount - 1 : 0; row >= 0 && row < rowCount && remaining > 0; row += step) {
            recordA.setRowIndex(row);
            final int indexKey = TableUtils.toIndexKey(recordA.getInt(columnIndex));
            final int key = indexKey >= 0 && indexKey < indexKeyCount ? symbolKeyIndexes.get(indexKey) : -1;
            if (key < 0 || filter != null && !filter.getBool(recordA)) {
                continue;
            }
            final long rank = gatheredRows.getRank(key);
            if (key == keyHi - 1 && rank >= rankHi) {
                continue;
            }
            if (key != keyLo || rank >= rankLo) {
                if (!gatheredRows.put(key, recordA, getRowBytes(recordA))) {
                    return 0;
                }
                remaining--;
            }
            gatheredRows.setRank(key, rank + 1);
        }
        return remaining;
    }

    /**
     * Gathers the window of rows that starts at the row {@code rankLo} of the key {@code keyLo}, at {@code position}
     * in key order, see {@link #gatherWindowRows(long, int, long, int)}. A window whose rows outgrow the memory of the
     * record chain before its pass ends plans the windows anew by the bytes its rows took, see {@link #replanWindows()},
     * and ends at the rows it holds without a gap in key order, see {@link #endFilledWindow(long, int, long)}. It
     * gathers again when its rows lack their row ids, which the windows after it need, or when it holds no row up to
     * the key it ends within, then taking no key after that one, so that the first row it takes is a row of that key,
     * which it holds whatever its bytes.
     */
    private void gatherWindow(long position, int keyLo, long rankLo) {
        int keyLimit = keyCount;
        while (!gatherWindowRows(position, keyLo, rankLo, keyLimit)) {
            final boolean hasRowIds = gatheredRows.isWindowed;
            replanWindows();
            if (hasRowIds) {
                final int gapKey = endFilledWindow(position, keyLo, rankLo);
                if (gapKey < 0) {
                    return;
                }
                keyLimit = gapKey + 1;
            }
        }
    }

    /**
     * Gathers the window of rows that starts at the row {@code rankLo} of the key {@code keyLo}, at {@code position}
     * in key order, with one pass over the page frames in frame order, which decodes a page frame once, and only when
     * it holds a row the window takes and the read gathers rows or filters them. The window takes the rows from there
     * up to the row {@code gatherHi}, as many as it may hold: the counts of the index place its end among the keys and
     * their rows. They count the rows of a read that the index orders, and a filtered read takes as many of them as hold
     * the rows the window may hold by the share of them it is estimated to keep. The window takes no keys from
     * {@code keyLimit} on. Reports false, which leaves the window open, when its rows outgrow the memory of the record
     * chain before the pass ends: twice the budget of the gather when the index counts the rows of the read, and no
     * more than {@link #getMemoryCap()} when the window starts.
     */
    private boolean gatherWindowRows(long position, int keyLo, long rankLo, int keyLimit) {
        int keyHi = keyLimit;
        long rankHi = Long.MAX_VALUE;
        long remaining = Long.MAX_VALUE;
        long windowRowEstimate = Math.max(gatherRowCount - position, 0);
        // the end of the read places the end of a window where the counts are the rows the read returns, or where the
        // rows carry their row ids, which stay valid after the window is gone
        final long rowCount = isGatherRowCountExact || gatheredRows.isWindowed ? Math.min(windowRowCount, gatherHi - position) : windowRowCount;
        final boolean isPartOfRead = rowCount < Long.MAX_VALUE;
        if (isPartOfRead) {
            final long boundRowCount = (long) Math.min(rowCount * windowBoundScale, Long.MAX_VALUE / 2.0);
            long room = boundRowCount;
            int key = keyLo;
            long keyRowCount = key < keyLimit ? Math.max(getKeyRowBound(key) - rankLo, 0) : 0;
            while (key < keyLimit && keyRowCount <= room) {
                room -= keyRowCount;
                if (++key < keyLimit) {
                    keyRowCount = getKeyRowBound(key);
                }
            }
            if (key < keyLimit && room > 0) {
                keyHi = key + 1;
                rankHi = (key == keyLo ? rankLo : 0) + (long) Math.ceil(room / windowBoundScale);
                room = 0;
            } else {
                keyHi = key;
            }
            windowRowEstimate = (long) Math.ceil((boundRowCount - room) / windowBoundScale);
            if (isGatherRowCountExact) {
                remaining = windowRowEstimate;
            }
        }

        gatheredRows.reset(keyLo, keyHi);
        final long memoryCap = getMemoryCap();
        gatheredRows.maxBytes = isGatherRowCountExact ? Math.min(2 * gatherBudget, memoryCap) : memoryCap;
        gatheredRows.reserve((long) (windowRowEstimate * (gatherRowBytes + (gatheredRows.isWindowed ? Long.BYTES : 0))));
        final boolean isByIndex = isKeyOrderedByIndex();
        final boolean isDenseScanAllowed = isDenseScanAllowed(keyLo, keyHi);
        final long remainingLo = remaining;
        boolean isKeyMapPrepared = false;
        toTopFrames();
        PageFrame frame;
        while (remaining > 0 && keyLo < keyHi && (frame = frameCursor.next()) != null) {
            circuitBreaker.statefulThrowExceptionIfTrippedTimeThrottledOrYield();
            frameAddressCache.add(frameCount, frame);
            final int frameIndex = frameCount++;
            final long frameRowCount = frame.getPartitionHi() - frame.getPartitionLo();
            if (isDenseScanAllowed && keyHi - keyLo > getDenseScanCost(frameRowCount)) {
                final long frameKeyRowCount = isByIndex && (varSizeColumns.size() > 0 || keyHi - keyLo <= getDecodeCost(frameRowCount))
                        ? keyOrderedFactory.getRowCount(frame, keyLo, keyHi, frame.getPartitionLo(), frame.getPartitionHi() - 1, 0)
                        : 1;
                if (frameKeyRowCount == 0) {
                    continue;
                }
                if (frameKeyRowCount > 0) {
                    if (!isKeyMapPrepared) {
                        prepareSymbolKeyIndexes(keyLo, keyHi);
                        isKeyMapPrepared = true;
                    }
                    countDenseScanFrame();
                    remaining = gatherFrame(frameIndex, frameRowCount, keyLo, keyHi, rankLo, rankHi, remaining);
                    continue;
                }
            }
            PageFrameMemory frameMemory = null;
            if (!isByIndex) {
                // a filtered read decodes the page frame to filter its rows, unless the index finds no row of the keys of
                // a window that takes part of the read there, where asking it costs less than the decode
                if (isPartOfRead && keyHi - keyLo <= getDecodeCost(frameRowCount)
                        && keyOrderedFactory.getIndexRowCount(frame, keyLo, keyHi, frame.getPartitionLo(), frame.getPartitionHi() - 1, 0) == 0) {
                    continue;
                }
                frameMemory = frameMemoryPool.navigateTo(frameIndex);
                recordA.init(frameMemory);
            }
            for (int key = keyLo; key < keyHi && remaining > 0; key++) {
                final long keyRankLo = key == keyLo ? rankLo : 0;
                final long keyRankHi = key == keyHi - 1 ? rankHi : Long.MAX_VALUE;
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
                    if (rank < keyRankLo) {
                        rank++;
                        continue;
                    }
                    if (frameMemory == null) {
                        frameMemory = frameMemoryPool.navigateTo(frameIndex);
                        recordA.init(frameMemory);
                    }
                    recordA.setRowIndex(row);
                    if (!gatheredRows.put(key, recordA, getRowBytes(recordA))) {
                        remaining = 0;
                        break;
                    }
                    rank++;
                    if (--remaining == 0 || rank == keyRankHi) {
                        break;
                    }
                }
                gatheredRows.setRank(key, rank);
                rowCursor = Misc.free(rowCursor);
            }
        }
        if (gatheredRows.isFull()) {
            return false;
        }
        gatheredLo = position;
        gatheredHi = keyHi == keyCount && rankHi == Long.MAX_VALUE ? Long.MAX_VALUE : position + remainingLo - remaining;
        nextWindowKey = rankHi == Long.MAX_VALUE ? keyHi : keyHi - 1;
        nextWindowRank = rankHi == Long.MAX_VALUE ? 0 : rankHi;
        if (!isGatherRowCountExact) {
            recordWindowStart(position, keyLo, rankLo, remainingLo - remaining);
        }
        return true;
    }

    /**
     * The rows the index holds for the keys up to and including the key, which it counts once per read, or -1 when it
     * cannot.
     */
    private long getCumulativeIndexRowCount(int key) {
        while (countedKeyCount <= key && !isKeyRowCountUnknown) {
            final long count = countKeyRows(countedKeyCount);
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
     * The rows of the keys up to and including the key, which the index counts once per read, or -1 when it cannot.
     */
    private long getCumulativeRowCount(int key) {
        return isKeyOrderedByIndex() ? getCumulativeIndexRowCount(key) : -1;
    }

    /**
     * The cost of the decode of a page frame of {@code rowCount} rows of fixed-size columns, in probes of the index for
     * a key: measured over Parquet row groups, about 64 probes plus a probe per 128 bytes of its rows.
     */
    private double getDecodeCost(long rowCount) {
        return 64 + rowCount * (double) decodedRowSize / 128;
    }

    /**
     * The bytes the record chain of a gathered read may take: the memory that a sort of its rows would take instead,
     * an entry of a row id and a key for each row and the Parquet row groups of the scan that the decode cache keeps
     * while the sort reads the rows back, decoded with their var-size values, less those that the gather keeps decoded
     * itself. Under a query memory limit the chain takes at most {@code memoryCap}, see {@link #getMemoryCap()}, but no
     * less than the entries of the sort, which keeps the windows few: a read that the limit leaves less fails at the
     * limit, as the sort would.
     */
    private long getGatherBudget(long memoryCap) {
        final long decodedBytes = (long) (parquetRowCount * (decodedRowSize + gatherRowBytes - chainRowSize));
        final long cacheBytes = frameMemoryPool.getCacheableBytes(ParquetDecodeHint.SCATTERED, parquetFrameCount, decodedBytes)
                - frameMemoryPool.getCacheableBytes(ParquetDecodeHint.MONOTONIC, parquetFrameCount, decodedBytes);
        final long sortEntryBytes = gatherRowCount * 2 * Long.BYTES;
        return Math.max(Math.min(sortEntryBytes + Math.max(cacheBytes, 0), memoryCap), sortEntryBytes);
    }

    /**
     * The index keys of the column: one per symbol of its symbol table, and one for NULL.
     */
    private int getIndexKeyCount() {
        return frameCursor.getSymbolTable(keyOrderedFactory.getColumnIndex()).getSymbolCount() + 1;
    }

    /**
     * The rows the index holds for the key over the scan, which are the rows the read returns of the key when the
     * index orders the read and bound them otherwise, or an even share of the rows the read is estimated to return when
     * the index cannot count them.
     */
    private long getKeyRowBound(int key) {
        final long count = getCumulativeIndexRowCount(key);
        if (count < 0) {
            // The index cannot count a composite partition's keys, so with skewed keys this even share can put more rows
            // in a window than its budget allows; counting those keys page frame by page frame would bound it exactly.
            return (gatherRowCount + keyCount - 1) / keyCount;
        }
        return key > 0 ? count - getCumulativeIndexRowCount(key - 1) : count;
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

    /**
     * The memory the record chain of a window may take under a query memory limit: half the memory the limit leaves the
     * query when the window starts, so that the other half holds the Parquet row groups its pass decodes, or
     * Long.MAX_VALUE without a limit. Each window measures it anew, after the window before it released its chain, so
     * that it counts the row groups that the records of the read hold for a consumer that moves to rows by their row
     * ids.
     */
    private long getMemoryCap() {
        final long limit = memoryTracker != null ? memoryTracker.getLimit() : 0;
        return limit > 0 ? Math.max(limit - memoryTracker.getUsed(), 0) / 2 : Long.MAX_VALUE;
    }

    /**
     * The bytes the row takes in the record chain of a gather, past its row id: the link to the next row, the offset of
     * each var-size value, the fixed-size values and the var-size values.
     */
    private long getRowBytes(Record record) {
        long bytes = chainRowSize;
        for (int i = 0, n = varSizeColumns.size(); i < n; i += 2) {
            bytes += getVarSizeBytes(record, varSizeColumns.getQuick(i), varSizeColumns.getQuick(i + 1));
        }
        return bytes;
    }

    /**
     * Moves the gathered read on to its next row, gathering the windows after the window it holds until one holds a
     * row, and reports whether there is one.
     */
    private boolean hasNextGatheredRow() {
        if (!isGatherPositioned) {
            positionGatheredRows();
        }
        while (!gatheredRows.hasNext()) {
            if (gatheredHi >= gatherHi) {
                return false;
            }
            gatherWindow(gatheredHi, nextWindowKey, nextWindowRank);
            gatheredRows.toTop();
        }
        if (isDelegatingRecords) {
            windowRecord.of(gatheredRows.getRecord());
        }
        return true;
    }

    private boolean isBackward() {
        return keyOrderedFactory.getIndexDirection() == IndexReader.DIR_BACKWARD;
    }

    private boolean isCompositePartition(PageFrame frame) {
        return frameCursor instanceof TablePageFrameCursor tableCursor
                && tableCursor.getTableReader().getTxFile().isPartitionComposite(frame.getPartitionIndex());
    }

    /**
     * True when the gather of the keys {@code keyLo} to {@code keyHi} may scan a page frame row by row rather than
     * probe the index once per key there.
     * <ul>
     * <li>The scan maps every index key of the column to its key, once per window. The map may take no more memory
     * than the entries the window keeps for its keys, so that a few keys of a column with many symbols probe the index
     * rather than map every symbol.</li>
     * <li>The scan decodes the page frame in full, where the probes decode it only when it holds a row of the window.
     * The index tells which page frames hold none, which the gather skips, where asking it costs less than a decode
     * of fixed-size columns, see {@link #getDecodeCost(long)}. The values of var-size columns have no such bound, so
     * over them the scan reads only a page frame that the probes decode as well: any page frame of a filtered read,
     * which decodes it to filter its rows, and otherwise one where the index finds a row of the window.</li>
     * </ul>
     */
    private boolean isDenseScanAllowed(int keyLo, int keyHi) {
        return (long) getIndexKeyCount() * Integer.BYTES <= (long) (keyHi - keyLo) * KeyOrderedRowChain.KEY_ENTRY_SIZE * Long.BYTES;
    }

    /**
     * True when a read ordered by keys may gather its rows: the Parquet page frames it scans do not stay decoded across
     * the passes of its keys, see isParquetCached, so that every pass would decode them again, or its keys outnumber
     * the rows of a Parquet page frame enough that a dense scan of each costs less than the passes of the keys over it.
     */
    private boolean isGatherRequired() {
        parquetFrameCount = 0;
        parquetRowCount = 0;
        isParquetCached = true;
        if (!(frameCursor instanceof TablePageFrameCursor tableCursor) || !tableCursor.getTableReader().hasParquetPartitions()) {
            return false;
        }
        prepareKeyOrderedRowCursors();
        if (keyCount < 2) {
            return false;
        }
        try {
            PageFrame frame;
            while ((frame = frameCursor.next()) != null) {
                if (frame.getFormat() == PartitionFormat.PARQUET) {
                    parquetFrameCount++;
                    parquetRowCount += frame.getPartitionHi() - frame.getPartitionLo();
                }
            }
            isParquetCached = frameMemoryPool.isCachingFrames(parquetFrameCount, parquetRowCount * decodedRowSize);
            return !isParquetCached
                    || parquetFrameCount > 0 && keyCount > getDenseScanCost(parquetRowCount / parquetFrameCount) && isDenseScanAllowed(0, keyCount);
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
     * Estimates the rows the gathered read returns and the bytes each of them takes in the record chain, and sizes the
     * windows of the gather: one window holds every row when they fit the budget of the gather, and otherwise the
     * fewest windows that each fit it share the rows evenly. The rows of a read of more than one window carry their row
     * ids. A read that fits whose rows the index does not count leaves that to its first window, see isLayoutPending.
     * The record chain doubles its memory as it grows, so that the chain of a window whose rows outgrow the estimate of
     * their bytes takes up to twice the budget, and never more than {@link #getMemoryCap()}. The budget of a filtered
     * read rests on an estimate of its rows, so that only the limit bounds its chain.
     */
    private void planWindows() {
        sampleGatheredRows();
        if (!isGatherRowCountExact) {
            // an estimate of the rows keeps an eighth of headroom, so that the record chain of rows it falls short of
            // by less does not double its memory
            gatherRowBytes += gatherRowBytes / 8;
        }
        final long memoryCap = getMemoryCap();
        final long budget = getGatherBudget(memoryCap);
        gatherBudget = isGatherRowCountExact ? budget : Math.min(budget, memoryCap);
        final boolean isOverBudget = gatherRowCount * gatherRowBytes > gatherBudget;
        setWindowed(isOverBudget);
        isLayoutPending = !isOverBudget && !isGatherRowCountExact;
        if (isOverBudget) {
            final long windowCount = (long) Math.ceil(gatherRowCount * (gatherRowBytes + Long.BYTES) / Math.max(gatherBudget, 1));
            windowRowCount = Math.max(1, (gatherRowCount + windowCount - 1) / windowCount);
        } else {
            windowRowCount = Long.MAX_VALUE;
        }
    }

    /**
     * Positions the gathered rows at the row {@code gatherLo}, gathering the window that starts there first unless the
     * window the read holds holds that row. The row of a read that the index orders lands by the row counts of the
     * keys, and any other read moves its window only from its first row on.
     */
    private void positionGatheredRows() {
        if (isLayoutPending) {
            isLayoutPending = false;
            setWindowed(gatherHi < Long.MAX_VALUE);
        }
        if (gatheredLo < 0 || gatherLo < gatheredLo || gatherLo >= gatheredHi) {
            if (isGatherRowCountExact) {
                int key = 0;
                long previousCount = 0;
                long count;
                while (gatherLo > 0 && key < keyCount && (count = getCumulativeRowCount(key)) >= 0 && count <= gatherLo) {
                    previousCount = count;
                    key++;
                }
                gatherWindow(gatherLo, key, gatherLo - previousCount);
            } else {
                // a filtered read moves its window only from its first row or from a window its passes went over
                long entry = -1;
                for (long lo = 0, hi = windowStarts.size() / 3 - 1; lo <= hi; ) {
                    final long mid = (lo + hi) >>> 1;
                    if (windowStarts.get(mid * 3) <= gatherLo) {
                        entry = mid * 3;
                        lo = mid + 1;
                    } else {
                        hi = mid - 1;
                    }
                }
                if (entry < 0) {
                    gatherWindow(0, 0, 0);
                } else {
                    gatherWindow(windowStarts.get(entry), (int) windowStarts.get(entry + 1), windowStarts.get(entry + 2));
                }
            }
        }
        gatheredRows.toTop();
        isGatherPositioned = true;
        // a window that the passes went over may end before the row once the windows are planned anew
        for (long i = gatheredLo; i < gatherLo && hasNextGatheredRow(); i++) {
            circuitBreaker.statefulThrowExceptionIfTrippedOrYield();
        }
    }

    private void prepareKeyOrderedRowCursors() {
        if (!prepareRowCursorFactory()) {
            return;
        }
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

    /**
     * Maps each index key of the column to the key among the keys {@code keyLo} to {@code keyHi} that holds it, and the
     * other index keys to -1.
     */
    private void prepareSymbolKeyIndexes(int keyLo, int keyHi) {
        final int indexKeyCount = getIndexKeyCount();
        if (symbolKeyIndexes.getCapacity() < indexKeyCount) {
            symbolKeyIndexes.setCapacity(indexKeyCount);
        }
        symbolKeyIndexes.setPos(indexKeyCount);
        symbolKeyIndexes.zero(-1);
        for (int key = keyLo; key < keyHi; key++) {
            final int indexKey = TableUtils.toIndexKey(keyOrderedFactory.getSymbolKey(key));
            if (indexKey >= 0 && indexKey < indexKeyCount) {
                symbolKeyIndexes.set(indexKey, key);
            }
        }
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
     * Records the start of the window that the gather of a filtered read took {@code rowCount} rows from, when it
     * follows the windows the read recorded, and the rows of the read once the window reaches its last row.
     */
    private void recordWindowStart(long position, int keyLo, long rankLo, long rowCount) {
        final long size = windowStarts.size();
        if (windowStartsRowCount >= 0 || position != windowStartsHi
                || size > 0 && windowStarts.get(size - 2) == keyLo && windowStarts.get(size - 1) == rankLo) {
            return;
        }
        windowStarts.ensureCapacity(3);
        windowStarts.add(position);
        windowStarts.add(keyLo);
        windowStarts.add(rankLo);
        windowStartsHi = position + rowCount;
        if (gatheredHi == Long.MAX_VALUE) {
            windowStartsRowCount = windowStartsHi;
        }
    }

    /**
     * Plans the windows of the read anew after a window filled the memory of the record chain before it held its rows,
     * by the bytes its rows took: each window holds as many rows as fit the budget at those bytes, with their row ids,
     * within the memory of the chain, which a window of a filtered read fills, since its budget rests on an estimate of
     * its rows. A window of a filtered read takes no more rows of the index than the rows it may hold.
     */
    private void replanWindows() {
        gatherRowBytes = Math.max(gatherRowBytes, gatheredRows.getMeasuredRowBytes());
        windowBoundScale = 1;
        final long windowBytes = isGatherRowCountExact ? Math.min(gatherBudget, gatheredRows.maxBytes) : gatheredRows.maxBytes;
        windowRowCount = Math.max(1, (long) (windowBytes / (gatherRowBytes + Long.BYTES)));
        setWindowed(true);
    }

    /**
     * Estimates the rows a gathered read returns and the bytes each of them takes in its record chain. The index counts
     * the rows of a read that it orders, and the share of the rows of the first Parquet page frame of the scan that the
     * read keeps scales the rows of the scan otherwise. That page frame also samples the bytes of var-size values and
     * the share of its rows the filter keeps.
     */
    private void sampleGatheredRows() {
        gatherRowBytes = chainRowSize;
        windowBoundScale = 1;
        gatherRowCount = countGatheredRows();
        isGatherRowCountExact = gatherRowCount >= 0;
        if (isGatherRowCountExact && varSizeColumns.size() == 0) {
            toTopFrames();
            return;
        }
        if (!isGatherRowCountExact) {
            gatherRowCount = countScanRows();
        }
        toTopFrames();
        try {
            PageFrame frame;
            while ((frame = frameCursor.next()) != null) {
                frameAddressCache.add(frameCount, frame);
                final int frameIndex = frameCount++;
                final long frameRowCount = frame.getPartitionHi() - frame.getPartitionLo();
                if (frame.getFormat() == PartitionFormat.PARQUET && frameRowCount > 0) {
                    final PageFrameMemory frameMemory = frameMemoryPool.navigateTo(frameIndex);
                    if (!isGatherRowCountExact) {
                        long keptRowCount = 0;
                        try (RowCursor keptRows = keyOrderedFactory.getCursor(frame, frameMemory, 0, keyCount)) {
                            while (keptRows.hasNext()) {
                                keptRows.next();
                                keptRowCount++;
                            }
                        }
                        gatherRowCount = (long) Math.ceil((double) gatherRowCount * keptRowCount / frameRowCount);
                    }
                    recordA.init(frameMemory);
                    long rowBytes = 0;
                    long filteredRowCount = 0;
                    for (long row = 0; row < frameRowCount; row++) {
                        recordA.setRowIndex(row);
                        rowBytes += getRowBytes(recordA);
                        if (filter != null && filter.getBool(recordA)) {
                            filteredRowCount++;
                        }
                    }
                    gatherRowBytes = (double) rowBytes / frameRowCount;
                    if (filter != null) {
                        windowBoundScale = (double) frameRowCount / Math.max(filteredRowCount, 1);
                    }
                    return;
                }
            }
        } finally {
            toTopFrames();
        }
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

    private void setWindowed(boolean isWindowed) {
        gatheredRows.isWindowed = isWindowed;
        windowRecord.isRowIdStored = isWindowed;
        windowRecordB.isRowIdStored = isWindowed;
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
            long count = skipCount > 0 ? countRowsTo(skipCount > Long.MAX_VALUE - gatherLo ? Long.MAX_VALUE : gatherLo + skipCount) : 0;
            if (count < 0) {
                count = windowStartsRowCount;
            }
            if (count >= 0) {
                final long skippedCount = skipCount > 0 ? Math.max(0, Math.min(skipCount, count - gatherLo)) : 0;
                gatherLo += skippedCount;
                rowCount.dec(skippedCount);
                gatherHi = requestedMaxRowsAfterSkip > Long.MAX_VALUE - gatherLo ? Long.MAX_VALUE : gatherLo + requestedMaxRowsAfterSkip;
                return;
            }
        }
        while (rowCount.get() > 0 && hasNextGatheredRow()) {
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
     * key by key. The chain takes a row only while its memory, which doubles as it grows, stays within maxBytes, see
     * {@link #put(int, Record, long)}.
     */
    private static class KeyOrderedRowChain implements QuietCloseable {
        // the first and the last record of a key, and the rows of the key the pass went over
        private static final int KEY_ENTRY_SIZE = 3;
        private final DirectLongList keyEntries;
        // The rows of a read that one window holds whole, whose row ids are their offsets in the chain.
        private final RecordChain rows;
        // The rows of a window of a read that other windows follow, each with its row id in the page frames, which
        // stays valid after the window is gone.
        private final RecordChain windowRows;
        // The bytes the rows of the chain take, and the memory the chain holds for them.
        private long bytes;
        private long capacity;
        private RecordChain chain;
        private boolean isWindowed;
        private int key;
        private int keyHi;
        private int keyLo;
        // The memory the chain may take.
        private long maxBytes = Long.MAX_VALUE;
        // The bytes of the row the chain did not take for want of memory, 0 while it takes every row.
        private long rejectedRowBytes;
        private long rowCount;

        private KeyOrderedRowChain(CairoConfiguration configuration, RecordMetadata metadata, SymbolTableSource symbolTableSource) {
            final int columnCount = metadata.getColumnCount();
            final EntityColumnFilter columnFilter = new EntityColumnFilter();
            columnFilter.of(columnCount);
            final LoopingRecordSink rowSink = new LoopingRecordSink(metadata, columnFilter, null, null, null, null);
            final ArrayColumnTypes windowColumnTypes = new ArrayColumnTypes();
            for (int i = 0; i < columnCount; i++) {
                windowColumnTypes.add(metadata.getColumnType(i));
            }
            windowColumnTypes.add(ColumnType.LONG);
            // the chain reserves the memory of a window up front, which pages of the OS page size round the least
            final long pageSize = Numbers.ceilPow2(Math.min(configuration.getSqlSortValuePageSize(), Files.PAGE_SIZE));
            final int maxPages = (int) Math.min(Math.max(1L, configuration.getSqlSortValueMaxBytes() / pageSize), Integer.MAX_VALUE);
            final String maxBytesKey = PropertyKey.CAIRO_SQL_SORT_VALUE_MAX_BYTES.getPropertyPath();
            rows = new RecordChain(metadata, rowSink, pageSize, maxPages, maxBytesKey);
            windowRows = new RecordChain(windowColumnTypes, new RowIdRecordSink(rowSink), pageSize, maxPages, maxBytesKey);
            rows.setSymbolTableResolver(symbolTableSource);
            windowRows.setSymbolTableResolver(symbolTableSource);
            keyEntries = new DirectLongList(KEY_ENTRY_SIZE, MemoryTag.NATIVE_DEFAULT, true);
            chain = rows;
        }

        @Override
        public void close() {
            rows.clear();
            windowRows.clear();
            keyEntries.close();
            isWindowed = false;
            chain = rows;
            keyLo = 0;
            keyHi = 0;
            key = 0;
            maxBytes = Long.MAX_VALUE;
            clearBytes();
        }

        private void clearBytes() {
            bytes = 0;
            capacity = 0;
            rejectedRowBytes = 0;
            rowCount = 0;
        }

        /**
         * Ends the rows the chain returns before the key {@code keyHi}.
         */
        private void endAt(int keyHi) {
            this.keyHi = keyHi;
        }

        private long entryOf(int key) {
            return (long) (key - keyLo) * KEY_ENTRY_SIZE;
        }

        /**
         * The bytes a row took in the chain, past its row id, on average over its rows and the row it did not take.
         */
        private double getMeasuredRowBytes() {
            return (double) (bytes + rejectedRowBytes) / (rowCount + 1) - (isWindowed ? Long.BYTES : 0);
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

        /**
         * True when the chain did not take a row for want of memory.
         */
        private boolean isFull() {
            return rejectedRowBytes > 0;
        }

        private void of(MemoryTracker memoryTracker) {
            close();
            rows.setMemoryTracker(memoryTracker);
            windowRows.setMemoryTracker(memoryTracker);
            keyEntries.setMemoryTracker(memoryTracker);
        }

        /**
         * Appends the row, which takes {@code rowBytes} in the chain past its row id, to the rows of the key, and
         * reports whether it did: a chain that holds rows does not take one that would grow its memory past maxBytes.
         */
        private boolean put(int key, Record record, long rowBytes) {
            final long storedBytes = rowBytes + (isWindowed ? Long.BYTES : 0);
            if (bytes + storedBytes > capacity) {
                final long grownCapacity = Math.max(capacity * 2, bytes + storedBytes);
                if (grownCapacity > maxBytes && rowCount > 0) {
                    rejectedRowBytes = storedBytes;
                    return false;
                }
                capacity = grownCapacity;
            }
            final long entry = entryOf(key);
            final long lastRecord = keyEntries.get(entry + 1);
            final long offset = chain.put(record, lastRecord);
            if (lastRecord == -1) {
                keyEntries.set(entry, offset);
            }
            keyEntries.set(entry + 1, offset);
            bytes += storedBytes;
            rowCount++;
            return true;
        }

        private void recordAt(Record record, long rowId) {
            chain.recordAt(record, rowId);
        }

        /**
         * Reserves the memory of rows of {@code bytes} bytes in all, and no more than maxBytes.
         */
        private void reserve(long bytes) {
            capacity = Math.min(bytes, maxBytes);
            chain.reserve(capacity);
        }

        /**
         * Drops the gathered rows and starts gathering the rows of the keys at {@code keyLo}, inclusive, to
         * {@code keyHi}, exclusive.
         */
        private void reset(int keyLo, int keyHi) {
            chain.clear();
            chain = isWindowed ? windowRows : rows;
            chain.clear();
            clearBytes();
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

    /**
     * Copies the columns of a record and then its row id.
     */
    private static class RowIdRecordSink implements RecordSink {
        private final RecordSink sink;

        private RowIdRecordSink(RecordSink sink) {
            this.sink = sink;
        }

        @Override
        public void copy(Record r, RecordSinkSPI w) {
            sink.copy(r, w);
            w.putLong(r.getRowId());
        }

        @Override
        public void setFunctions(ObjList<Function> keyFunctions) {
            sink.setFunctions(keyFunctions);
        }
    }

    /**
     * A record of a gather that delegates to the record of the rows that hold the row: the record chain of a window, or
     * the page frames, which the decode cache serves, for a row that a consumer moves to by a row id that the window
     * that held the row stored, or for the passes of the keys that a filtered read takes instead of the gather.
     */
    private static class WindowRecord extends DelegatingRecord {
        private final PageFrameMemoryRecord frameRecord;
        private final int rowIdColumnIndex;
        // True when the rows of the record chain carry their row ids in the page frames, see KeyOrderedRowChain.
        private boolean isRowIdStored;

        private WindowRecord(PageFrameMemoryRecord frameRecord, int rowIdColumnIndex) {
            this.frameRecord = frameRecord;
            this.rowIdColumnIndex = rowIdColumnIndex;
        }

        @Override
        public long getRowId() {
            return isRowIdStored && base != frameRecord ? base.getLong(rowIdColumnIndex) : base.getRowId();
        }
    }
}
