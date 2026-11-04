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
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.ColumnTypeDriver;
import io.questdb.cairo.EntityColumnFilter;
import io.questdb.cairo.LoopingRecordSink;
import io.questdb.cairo.RecordChain;
import io.questdb.cairo.RecordSink;
import io.questdb.cairo.RecordSinkSPI;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.arr.ArrayTypeDriver;
import io.questdb.cairo.idx.BitmapIndexFwdReader;
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

import java.util.concurrent.atomic.AtomicLong;

/**
 * Reads the rows of a {@link KeyOrderedRowCursorFactory} key by key over all page frames, or, when its Parquet page
 * frames do not stay decoded across the passes of the keys, either returns the rows of each key by row id from the
 * posting lists of the index, which the decode cache serves, see {@link #isDirectReadCheaper()}, or gathers them window
 * by window, each window in one pass over the page frames that decodes each of them once, and returns the rows of a
 * window key by key. A window holds what the budget of the gather allows, see {@link #getGatherBudget(long)}, so a read
 * whose rows fit it gathers them in one pass.
 * The windows rest on estimates of the bytes of the rows, which the gather measures row by row: a window ends where its
 * rows outgrow the memory of the record chain, see {@link #gatherWindow(long, int, long)}, and the passes of the keys
 * take over where they decode fewer page frames than the windows, see {@link #isKeyPassCheaper(int, long)}, or where the
 * windows reach a memory limit of the query that a sort fits. A filtered read over page frames that stay decoded
 * collects the row indexes of its keys in one pass over the page frames instead of a pass per key, see
 * {@link #collectNextRows()}, and returns the rows of its first key while the pass goes on. The collection takes no
 * more memory than its budget, see {@link #getCollectionBudget()}, and each key reads its rows past the page frames the
 * collection went over in a pass of its own. A filtered read whose outer cursor reads only its last rows, see {@link #expectLastRows(long)}, finds
 * them key by key from its last key back instead, see {@link #collectTailRows(long)}.
 */
final class KeyOrderedPageFrameRecordCursor extends PageFrameRecordCursorImpl {
    // The bytes of a row in the record chain of a gather, past the values of its var-size columns.
    private final int chainRowSize;
    // The bytes of the headers of the VARCHAR values of a row in the record chain, which their decoded page frames lack.
    private final int chainVarcharHeaderSize;
    // The rows of the keys a filtered read collects in one pass over the page frames, see collectNextRows(): the first
    // and the last run of each key, in pairs; the row indexes of the rows in their page frames, in runs of the rows of a
    // key in a page frame, which page frames of at most Integer.MAX_VALUE rows keep within an int; and each run as its
    // page frame, its end among the rows and the next run of its key, in triples.
    private final DirectLongList collectedKeyRuns;
    private final DirectIntList collectedRows;
    private final DirectLongList collectedRuns;
    private final RecordCursor.Counter countCounter = new RecordCursor.Counter();
    private final int decodedRowSize;
    // The model of the decode cache and of the windows of a gather that the choice of a direct read runs, see
    // isDirectModelCheaper(int, long, long), in triples for each page frame: the page frame used before and the page
    // frame used after it in the cache, -1 for none, so that a page frame is in the cache when it is its head or has one
    // before it, and the window of the gather that decoded it last, -1 before the first. The choice of the passes of the
    // keys holds here the page frames each key shares with the key before it, see isKeyPassCheaper(int, long).
    private final DirectIntList frameModel;
    // The rows of a window of a read ordered by keys that one pass over the page frames gathers, when its Parquet page
    // frames do not stay decoded across the passes of the keys, see isGatherRequired().
    private final KeyOrderedRowChain gatheredRows;
    private final KeyOrderedRowCursorFactory keyOrderedFactory;
    // The cumulative counts of the index for the keys the gathered read counted, in key order: the rows of the keys of
    // a read that the index orders, and a bound on them otherwise.
    private final DirectLongList keyRowCounts;
    // The page frames of a read that the index orders, in runs of page frames of a partition whose rows follow each other
    // in the index direction: the partition, the first row, the end row, the shift of the rows of the index and the
    // first page frame of each run, in quintuples, see prepareScanRuns(), and the reader of the index of each run, in
    // the index direction and forward, which counts and locates rows.
    private final ObjList<BitmapIndexFwdReader> scanRunCounters = new ObjList<>();
    private final ObjList<IndexReader> scanRunReaders = new ObjList<>();
    private final DirectLongList scanRuns;
    // The key that a dense scan of a gather finds for each index key of the column, by index key, or -1 for an index
    // key whose rows the gather does not take.
    private final DirectIntList symbolKeyIndexes;
    // The last rows of a filtered read whose outer cursor reads only its last rows, see expectLastRows(long): the page
    // frame and the row of each, in reverse order of the read.
    private final DirectLongList tailRows;
    // The index and the type of each var-size column, in pairs.
    private final IntList varSizeColumns = new IntList();
    // 1 for each page frame that holds a row of the window a gather gathers, see markWindowFrames(int, int).
    private final DirectIntList windowFrameFlags;
    // The records of a gather whose rows the read holds in more than one way, see isDelegatingRecords.
    private final WindowRecord windowRecord;
    private final WindowRecord windowRecordB;
    // The window starts of a filtered gather that its passes went over from its first row on, a position in key order,
    // a key and a row of the key each, in triples, so that a skip lands in the window that holds its row.
    private final DirectLongList windowStarts;
    // The last page frame the collection went over, whose keys after the first it collects after the read returned
    // the rows of the first key there, and the page frames it went over.
    private @Nullable PageFrame collectedFrame;
    private int collectedFrameCount;
    // The key of the collected rows the read returns, the run of the key it returns, -1 before the first, and its rows.
    private int collectedKey;
    // The collected rows the read returned or skipped.
    private long collectedReadCount;
    private long collectedRow;
    private long collectedRowHi;
    private long collectedRun;
    // The memory the lists of the collection may take by the memory limit of the query, see getCollectionBudget().
    private long collectionMemoryCap;
    private int countedKeyCount;
    // The position in key order of the next row of a direct read; the run whose posting list of the key keyLo it reads,
    // and the partition row of the first value of that posting list.
    private long directPosition;
    private int directRun;
    private long directRowLo;
    // The Parquet page frames that the passes of the windows of this read decoded before their rows outgrew the memory
    // of the record chain, see gatherWindow(long, int, long), which count against its windows, see
    // isKeyPassCheaper(int, long).
    private long discardedFrameCount;
    // The bytes a window of the gathered read holds by the plan of its windows, see planWindows().
    private long gatherBudget;
    // The position in key order that the next read of a gathered read starts at.
    private long gatherLo;
    // The estimated bytes a gathered row takes in the record chain.
    private double gatherRowBytes;
    // The window of rows the gathered rows hold, gatheredHi at Long.MAX_VALUE when it runs to the last row, and
    // gatheredLo at -1 before the read gathers any.
    private long gatheredHi;
    private long gatheredLo = -1;
    // True when the collection went over every page frame or stopped at its budget, see passFrameLo.
    private boolean isCollectionComplete;
    // True when the records of the read delegate to the record of the rows that hold the row, see delegateRecords().
    private boolean isDelegatingRecords;
    // True when the collection went over every key of the last page frame it went over.
    private boolean isFrameCollected;
    private boolean isGatherPositioned;
    private boolean isKeyRowCountUnknown;
    // True when the Parquet page frames of the scan stay decoded across the passes of the keys: no more than the decode
    // cache keeps under their hint, decoding to no more than its budget, by the sizes of the column types until the read
    // knows the bytes of its var-size values, see isParquetCachedAt(double).
    private boolean isParquetCached;
    // True when readRowCount is the rows the read returns, which the index counts, rather than an estimate.
    private boolean isReadRowCountExact;
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
    // The first page frame that the collection did not go over for every key once it stopped at its budget, -1 while it
    // did not, and the key whose pass over the page frames past those the collection went over the read takes, -1
    // before the first.
    private int passFrameLo = -1;
    private int passKey = -1;
    // The position in key order that the read returns no row at or past: the position a skip lands on plus the rows the
    // read returns at most after it, see addRows(long, long), or Long.MAX_VALUE while no skip bounds the read.
    private long readHi = Long.MAX_VALUE;
    private ReadMode readMode = ReadMode.PASSES;
    // The rows the read returns, which the index counts for a read that it orders, and an estimate otherwise, see
    // sampleGatheredRows().
    private long readRowCount;
    // The page frame that frameOf(int, long) found last, its run and its rows.
    private int rowFrameIndex;
    private long rowFrameHi;
    private long rowFrameLo;
    private int rowFrameRun = -1;
    // The number of page frames of the scan, known once a pass of a read ordered by keys reads them all, -1 before.
    private int scanFrameCount = -1;
    // The page frame recordA is at, and the position of the read among the tail rows, which counts down to -1.
    private long tailRow = -1;
    // The rows the index counts for the keys of a filtered read per row its filter is estimated to keep, which a window
    // takes as many of as it may hold rows.
    private double windowBoundScale = 1;
    // The decode hint of the page frames of a gathered read outside the passes of its windows: the hint the read had
    // before it gathered, or the one its consumer set since. A filtered read returns to it when it reads the passes of
    // the keys instead, and a read in windows moves to rows by their row ids under it once it returned its last row.
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
        int varcharHeaderSize = 0;
        for (int i = 0, n = metadata.getColumnCount(); i < n; i++) {
            final int columnType = metadata.getColumnType(i);
            if (ColumnType.isVarSize(columnType)) {
                varSizeColumns.add(i);
                varSizeColumns.add(columnType);
                if (ColumnType.tagOf(columnType) == ColumnType.VARCHAR) {
                    varcharHeaderSize += Integer.BYTES;
                }
            }
        }
        this.chainVarcharHeaderSize = varcharHeaderSize;
        try {
            collectedKeyRuns = new DirectLongList(16, MemoryTag.NATIVE_DEFAULT, true);
            collectedRows = new DirectIntList(16, MemoryTag.NATIVE_DEFAULT, true);
            collectedRuns = new DirectLongList(16, MemoryTag.NATIVE_DEFAULT, true);
            frameModel = new DirectIntList(0, MemoryTag.NATIVE_DEFAULT, true);
            gatheredRows = new KeyOrderedRowChain(configuration, metadata, this);
            keyRowCounts = new DirectLongList(16, MemoryTag.NATIVE_DEFAULT, true);
            scanRuns = new DirectLongList(16, MemoryTag.NATIVE_DEFAULT, true);
            symbolKeyIndexes = new DirectIntList(0, MemoryTag.NATIVE_DEFAULT, true);
            tailRows = new DirectLongList(16, MemoryTag.NATIVE_DEFAULT, true);
            windowFrameFlags = new DirectIntList(0, MemoryTag.NATIVE_DEFAULT, true);
            windowStarts = new DirectLongList(3, MemoryTag.NATIVE_DEFAULT, true);
        } catch (Throwable th) {
            close();
            throw th;
        }
    }

    @Override
    public void calculateSize(SqlExecutionCircuitBreaker circuitBreaker, RecordCursor.Counter counter) {
        prepareKeyOrderedRowCursors();
        resolveReadMode(0, RecordCursor.UNBOUNDED_ROW_COUNT);
        switch (readMode) {
            case DIRECT -> {
                if (!isExhausted) {
                    counter.add(Math.max(Math.min(readRowCount, readHi) - directPosition, 0));
                    isExhausted = true;
                }
            }
            case GATHER -> calculateGatheredSize(counter);
            case COLLECT -> calculateCollectedSize(counter);
            case TAIL -> {
                if (!isExhausted) {
                    counter.add(tailRow + 1);
                    tailRow = -1;
                    isExhausted = true;
                }
            }
            case PASSES -> {
                if (keyCount > 1 && isSizeWalked()) {
                    // the skip counts the rows by the index, without decoding a page frame
                    if (!isExhausted) {
                        countCounter.set(Long.MAX_VALUE);
                        skipKeyOrderedRows(countCounter);
                        counter.add(Long.MAX_VALUE - countCounter.get());
                        isExhausted = true;
                    }
                } else {
                    super.calculateSize(circuitBreaker, counter);
                }
            }
        }
    }

    @Override
    public void close() {
        super.close();
        closeReadLists();
        Misc.free(gatheredRows);
    }

    /**
     * A filtered read collects its last {@code rowCount} rows key by key from its last key back, see
     * {@link #collectTailRows(long)}, and returns only those from then on, unless it started to read its rows. A read
     * whose mode waits chooses it for that many rows after a skip that its outer cursor counts first, see
     * {@link #resolveReadMode(long, long)}.
     */
    @Override
    public void expectLastRows(long rowCount) {
        prepareKeyOrderedRowCursors();
        resolveReadMode(RecordCursor.UNBOUNDED_ROW_COUNT, rowCount);
        if (readMode == ReadMode.COLLECT && collectedFrameCount == 0 && collectTailRows(rowCount)) {
            readMode = ReadMode.TAIL;
            tailRow = tailRows.size() - 1;
        }
    }

    /**
     * A read whose mode waits, see {@link ReadMode#PENDING}, chooses it once its outer cursor bounds the rows it reads,
     * see {@link #resolveReadMode(long, long)}, and returns records that delegate to the rows of either mode until
     * then, see {@link #delegateRecords()}.
     */
    @Override
    public void expectLimitedIteration() {
        if (readMode == ReadMode.PENDING) {
            delegateRecords();
        }
    }

    @Override
    public Record getRecord() {
        if (!isDelegatingRecords) {
            resolveReadMode(0, RecordCursor.UNBOUNDED_ROW_COUNT);
        }
        if (isDelegatingRecords) {
            return windowRecord;
        }
        return readMode == ReadMode.GATHER ? gatheredRows.getRecord() : recordA;
    }

    @Override
    public Record getRecordB() {
        if (!isDelegatingRecords) {
            resolveReadMode(0, RecordCursor.UNBOUNDED_ROW_COUNT);
        }
        if (isDelegatingRecords) {
            return windowRecordB;
        }
        return readMode == ReadMode.GATHER ? gatheredRows.getRecordB() : recordB;
    }

    @Override
    public boolean hasNext() {
        if (isExhausted) {
            return false;
        }
        prepareKeyOrderedRowCursors();
        resolveReadMode(0, RecordCursor.UNBOUNDED_ROW_COUNT);
        if (readMode == ReadMode.DIRECT) {
            return hasNextDirectRow();
        }
        if (readMode == ReadMode.GATHER) {
            return hasNextGatheredRow();
        }
        if (readMode == ReadMode.COLLECT) {
            return hasNextCollectedRow();
        }
        if (readMode == ReadMode.TAIL) {
            return hasNextTailRow();
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
                final int frameIndex = addFrame(frame);
                final long remaining = maxRowsAfterSkip - rowsProducedSinceSkip;
                final long frameSize = frame.getPartitionHi() - frame.getPartitionLo();
                final int inFrameHi = (int) Math.min(Math.min(frameSize, remaining), Integer.MAX_VALUE);
                PageFrameMemory frameMemory = frame.getFormat() == PartitionFormat.PARQUET && keyCount > 1
                        ? null : frameMemoryPool.navigateTo(frameIndex, inFrameHi);
                rowCursor = Misc.free(rowCursor);
                rowCursor = keyCount > 1 ? keyOrderedFactory.getCursor(frame, frameMemory, keyLo, keyHi)
                        : keyOrderedFactory.getCursor(frame, frameMemory);
                countWalk(testReadFrames);
                if (rowCursor.hasNext()) {
                    if (frameMemory == null) {
                        frameMemory = frameMemoryPool.navigateTo(frameIndex, inFrameHi);
                    }
                    recordA.init(frameMemory);
                    recordA.setRowIndex(rowCursor.next());
                    rowsProducedSinceSkip++;
                    return true;
                }
                frame = (frameCount == 1 || frame.getFormat() == PartitionFormat.PARQUET)
                        && keyHi - keyLo == 1 && keyCount > 1 ? nextKeyFrame(frame) : nextFrame();
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
        readMode = ReadMode.PASSES;
        memoryTracker = sqlExecutionContext.getMemoryTracker();
        gatheredRows.of(memoryTracker);
        closeReadLists();
        keyRowCounts.setMemoryTracker(memoryTracker);
        collectedKeyRuns.setMemoryTracker(memoryTracker);
        collectedRows.setMemoryTracker(memoryTracker);
        collectedRuns.setMemoryTracker(memoryTracker);
        collectedFrameCount = 0;
        collectedFrame = null;
        isCollectionComplete = false;
        isFrameCollected = true;
        passFrameLo = -1;
        toTopCollectedRows();
        symbolKeyIndexes.setMemoryTracker(memoryTracker);
        windowStarts.setMemoryTracker(memoryTracker);
        frameModel.setMemoryTracker(memoryTracker);
        windowFrameFlags.setMemoryTracker(memoryTracker);
        scanRuns.setMemoryTracker(memoryTracker);
        rowFrameRun = -1;
        toTopDirectRows();
        tailRows.setMemoryTracker(memoryTracker);
        tailRow = -1;
        windowStartsHi = 0;
        windowStartsRowCount = -1;
        countedKeyCount = 0;
        isKeyRowCountUnknown = false;
        readHi = Long.MAX_VALUE;
        toTopGatheredRows();
        gatheredLo = -1;
        isDelegatingRecords = false;
        try {
            if (isGatherRequired()) {
                walkHint = frameMemoryPool.getDecodeHint();
                planWindows();
                if (isReadRowCountExact && prepareScanRuns() && isDirectReadAllowed()) {
                    if (getScatteredFrameCapacity() >= parquetFrameCount) {
                        startDirectRead();
                    } else {
                        readMode = ReadMode.PENDING;
                    }
                } else {
                    startGatherOrPasses();
                }
            }
        } catch (Throwable th) {
            recordA.clear();
            recordB.clear();
            frameMemoryPool.releaseParquetBuffers();
            closeReadLists();
            throw th;
        }
    }

    @Override
    public long preComputedStateSize() {
        return super.preComputedStateSize() + RecordCursor.fromBool(gatheredLo >= 0) + RecordCursor.fromBool(collectedFrameCount > 0)
                + RecordCursor.fromBool(readMode == ReadMode.TAIL);
    }

    @Override
    public void recordAt(Record record, long rowId) {
        resolveReadMode(0, RecordCursor.UNBOUNDED_ROW_COUNT);
        final boolean isChained = readMode == ReadMode.GATHER && !gatheredRows.isWindowed;
        if (isDelegatingRecords && (record == windowRecord || record == windowRecordB)) {
            final WindowRecord delegatingRecord = (WindowRecord) record;
            if (isChained) {
                final Record chainRecord = record == windowRecord ? gatheredRows.getRecord() : gatheredRows.getRecordB();
                gatheredRows.recordAt(chainRecord, rowId);
                delegatingRecord.of(chainRecord);
            } else {
                frameRecordAt(delegatingRecord.frameRecord, rowId);
                delegatingRecord.of(delegatingRecord.frameRecord);
            }
        } else if (isChained) {
            gatheredRows.recordAt(record, rowId);
        } else {
            super.recordAt(record, rowId);
        }
    }

    /**
     * A gathered read keeps the hint for the page frames it reads outside the passes of its windows, see walkHint,
     * since each pass of a window decodes each page frame at most once, under MONOTONIC. A direct read keeps
     * SCATTERED, since it moves to the rows of each key across the page frames itself.
     */
    @Override
    public void setParquetDecodeHint(ParquetDecodeHint hint) {
        switch (readMode) {
            case GATHER, PENDING -> walkHint = hint;
            case DIRECT -> {
            }
            default -> super.setParquetDecodeHint(hint);
        }
    }

    @Override
    public void setRecordAtRows(@Nullable RowIdSource source) {
        resolveReadMode(0, RecordCursor.UNBOUNDED_ROW_COUNT);
        // the passes of a gather decode whole page frames
        if (readMode != ReadMode.GATHER) {
            super.setRecordAtRows(source);
        }
    }

    @Override
    public void skipRows(Counter rowCount, long requestedMaxRowsAfterSkip) {
        prepareKeyOrderedRowCursors();
        resolveReadMode(rowCount.get(), requestedMaxRowsAfterSkip);
        switch (readMode) {
            case DIRECT -> skipDirectRows(rowCount, requestedMaxRowsAfterSkip);
            case GATHER, PENDING -> skipGatheredRows(rowCount, requestedMaxRowsAfterSkip);
            case TAIL -> {
                final long skipCount = Math.min(rowCount.get(), tailRow + 1);
                tailRow -= skipCount;
                rowCount.dec(skipCount);
            }
            case COLLECT -> {
                if (!isExhausted) {
                    readHi = addRows(rowCount.get(), requestedMaxRowsAfterSkip);
                    skipCollectedRows(rowCount, keyCount);
                }
            }
            case PASSES -> {
                if (keyCount > 1) {
                    rowsProducedSinceSkip = 0;
                    maxRowsAfterSkip = isSkipClampable() ? requestedMaxRowsAfterSkip : RecordCursor.UNBOUNDED_ROW_COUNT;
                    skipKeyOrderedRows(rowCount);
                } else {
                    super.skipRows(rowCount, requestedMaxRowsAfterSkip);
                }
            }
        }
    }

    @Override
    public void toTop() {
        super.toTop();
        keyLo = 0;
        keyHi = Math.min(keyCount, 1);
        scanFrameCount = -1;
        readHi = Long.MAX_VALUE;
        toTopCollectedRows();
        tailRow = tailRows.size() - 1;
        toTopGatheredRows();
        toTopDirectRows();
        if (readMode == ReadMode.DIRECT) {
            setDirectHint(Long.MAX_VALUE);
        }
    }

    @Override
    protected boolean isSizeWalked() {
        return keyCount > 1 && (keyLo > 0 || frameCount > 0) || super.isSizeWalked();
    }

    /**
     * The position {@code rowCount} rows past {@code position} in key order, Long.MAX_VALUE when it would pass it.
     */
    private static long addRows(long position, long rowCount) {
        return rowCount > Long.MAX_VALUE - position ? Long.MAX_VALUE : position + rowCount;
    }

    private static void countWalk(AtomicLong counter) {
        if (isWalkCounterEnabled) {
            counter.incrementAndGet();
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

    /**
     * True when the passes of the keys that collect the last {@code rowCount} rows of the read, which went over
     * {@code passRowCount} rows of the index and found {@code foundRowCount} rows, would go over more than
     * {@code maxPassRowCount} rows of the index to find the rest at that rate.
     */
    private static boolean isTailPassTooLong(long foundRowCount, long rowCount, long passRowCount, double maxPassRowCount) {
        if (foundRowCount >= rowCount) {
            return false;
        }
        return (foundRowCount == 0 ? passRowCount : passRowCount + (double) (rowCount - foundRowCount) * passRowCount / foundRowCount) > maxPassRowCount;
    }

    /**
     * Appends the row index to the collected rows, and reports whether it did: it does not when the rows would grow past
     * the budget of the collection.
     */
    private boolean addCollectedRow(long row) {
        final long rowCapacity = collectedRows.getCapacity();
        if (collectedRows.size() == rowCapacity && isOverCollectionBudget(2 * rowCapacity, collectedRuns.getCapacity())) {
            return false;
        }
        collectedRows.add((int) row);
        return true;
    }

    /**
     * Appends a run of the rows of the key that end at {@code rowHi} among the collected rows to the runs of the page
     * frame, and reports whether it did: it does not when the runs would grow past the budget of the collection. The
     * run holds its key in place of its next run until {@link #linkCollectedRuns(long)} links it.
     */
    private boolean addCollectedRun(int frameIndex, int key, long rowHi) {
        final long runCapacity = collectedRuns.getCapacity();
        if (collectedRuns.size() + 3 > runCapacity && isOverCollectionBudget(collectedRows.getCapacity(), 2 * runCapacity)) {
            return false;
        }
        collectedRuns.add(frameIndex);
        collectedRuns.add(rowHi);
        collectedRuns.add(key);
        return true;
    }

    /**
     * Counts the rows the collected read returns from its position on: the collected rows it did not return, once the
     * collection went over every page frame, and otherwise the rows the passes of the keys past the page frames the
     * collection went over walk as well.
     */
    private void calculateCollectedSize(Counter counter) {
        if (isExhausted) {
            return;
        }
        while (!isCollectionComplete) {
            collectNextRows();
        }
        if (passFrameLo < 0) {
            counter.add(collectedRows.size() - collectedReadCount);
        } else {
            countCounter.set(Long.MAX_VALUE);
            skipCollectedRows(countCounter, keyCount);
            counter.add(Long.MAX_VALUE - countCounter.get());
        }
        isExhausted = true;
    }

    private void calculateGatheredSize(Counter counter) {
        if (isExhausted) {
            return;
        }
        if (!isGatherPositioned) {
            long rowCount = isReadRowCountExact ? readRowCount : size();
            if (rowCount < 0) {
                rowCount = countRowsTo(Long.MAX_VALUE);
            }
            if (rowCount < 0) {
                rowCount = windowStartsRowCount;
            }
            if (rowCount >= 0) {
                counter.add(Math.max(0, Math.min(rowCount, readHi) - gatherLo));
                isExhausted = true;
                return;
            }
        }
        while (hasNextGatheredRow()) {
            counter.inc();
        }
        isExhausted = true;
    }

    private void closeReadLists() {
        Misc.free(collectedKeyRuns);
        Misc.free(collectedRows);
        Misc.free(collectedRuns);
        Misc.free(frameModel);
        Misc.free(keyRowCounts);
        Misc.free(scanRuns);
        scanRunCounters.clear();
        scanRunReaders.clear();
        Misc.free(symbolKeyIndexes);
        Misc.free(tailRows);
        Misc.free(windowFrameFlags);
        Misc.free(windowStarts);
    }

    /**
     * Collects the rows of the row cursor of the page frame as a run per key while the lists of the collection grow
     * within its budget, and reports whether it collected all of them.
     */
    private boolean collectCursorRows(int frameIndex) {
        int runKey = -1;
        while (rowCursor.hasNext()) {
            final long row = rowCursor.next();
            final int key = keyOrderedFactory.getCursorKey();
            if (key != runKey) {
                if (runKey >= 0 && !addCollectedRun(frameIndex, runKey, collectedRows.size())) {
                    return false;
                }
                runKey = key;
            }
            if (!addCollectedRow(row)) {
                return false;
            }
        }
        return runKey < 0 || addCollectedRun(frameIndex, runKey, collectedRows.size());
    }

    /**
     * Collects the rows that the filter keeps of the keys {@code keyLo} to {@code keyHi} in the page frame as a run per
     * key and links each run to the run of its key before it. When they would grow the memory of the collection past
     * its budget, see {@link #getCollectionBudget()}, it collects none of them and ends the collection at the page frame.
     * One cursor of the
     * factory over those keys returns the rows, so that a sequential factory opens the cursors of all of them, which seeks
     * their index cursors, before it reads the rows of the first, as a scan in frame order does.
     */
    private void collectFrameRows(PageFrame frame, int frameIndex, int keyLo, int keyHi) {
        final long rowLo = collectedRows.size();
        final long runLo = collectedRuns.size();
        rowCursor = keyOrderedFactory.getCursor(frame, frameMemoryPool.navigateTo(frameIndex), keyLo, keyHi);
        final boolean isCollected = collectCursorRows(frameIndex);
        rowCursor = Misc.free(rowCursor);
        if (isCollected) {
            linkCollectedRuns(runLo);
        } else {
            collectedRows.setPos(rowLo);
            collectedRuns.setPos(runLo);
            endCollection(frameIndex);
        }
    }

    /**
     * Takes the next step of the one pass over the page frames that collects the rows of the keys: the rows of the
     * keys after the first in the last page frame it went over, while that page frame stays in the CPU caches, or the
     * rows of the first key in the next page frame, which the read returns before the pass goes on. A pass of each key
     * over all page frames would bring each page frame back once per key. A step whose rows would grow the collection
     * past its budget ends the collection at its page frame, and each key reads its rows from there on in a pass of its
     * own, see {@link #nextPassFrame()}.
     */
    private void collectNextRows() {
        while (frameCount < collectedFrameCount) {
            collectedFrame = frameCursor.next();
            frameCount++;
        }
        if (!isFrameCollected) {
            isFrameCollected = true;
            collectFrameRows(collectedFrame, collectedFrameCount - 1, 1, keyCount);
            return;
        }
        final PageFrame frame = frameCursor.next();
        if (frame == null) {
            isCollectionComplete = true;
            return;
        }
        addFrame(frame);
        collectedFrame = frame;
        collectedFrameCount = frameCount;
        countWalk(testReadFrames);
        isFrameCollected = false;
        collectFrameRows(frame, collectedFrameCount - 1, 0, 1);
    }

    /**
     * Collects the last {@code rowCount} rows of the read, in reverse order of the read: those of the last key, then of
     * the key before it, and so on, each found by a pass of the key over the page frames that keeps its last rows, and
     * reports whether it did. It does not when the index cannot count the rows of the keys, when they hold no more
     * rows than that, so that the read returns all of its rows, or when the passes would go over more than a third of
     * the rows of the keys by the share of their rows the filter keeps so far: a pass of each key brings back the page
     * frames that the one pass of the collection over them reads once for all keys.
     */
    private boolean collectTailRows(long rowCount) {
        final long indexRowCount = getCumulativeIndexRowCount(keyCount - 1);
        if (indexRowCount < 0 || rowCount >= indexRowCount) {
            return false;
        }
        final double maxPassRowCount = indexRowCount / 3.0;
        long passRowCount = 0;
        tailRows.reopen();
        tailRows.setCapacity(rowCount);
        tailRows.clear();
        for (int key = keyCount - 1; key >= 0 && tailRows.size() < rowCount; key--) {
            final long rowLo = tailRows.size();
            final long ringSize = rowCount - rowLo;
            long keyRowCount = 0;
            toTopFrames();
            PageFrame frame;
            while ((frame = frameCursor.next()) != null) {
                final int frameIndex = addFrame(frame);
                final long frameRowCount = keyOrderedFactory.getIndexRowCount(frame, key, key + 1, frame.getPartitionLo(), frame.getPartitionHi() - 1, Long.MAX_VALUE);
                if (frameRowCount == 0) {
                    continue;
                }
                rowCursor = keyOrderedFactory.getCursor(frame, frameMemoryPool.navigateTo(frameIndex), key, key + 1);
                while (rowCursor.hasNext()) {
                    final long row = rowCursor.next();
                    tailRows.set(rowLo + keyRowCount % ringSize, Numbers.encodeLowHighInts((int) row, frameIndex));
                    if ((++keyRowCount & 255) == 0 && frameRowCount > 0
                            && isTailPassTooLong(rowLo + keyRowCount, rowCount, passRowCount + getPassRowCount(frame, key, row), maxPassRowCount)) {
                        endTailRows();
                        return false;
                    }
                }
                rowCursor = Misc.free(rowCursor);
                passRowCount += frameRowCount > 0 ? frameRowCount : frame.getPartitionHi() - frame.getPartitionLo();
                if (isTailPassTooLong(rowLo + keyRowCount, rowCount, passRowCount, maxPassRowCount)) {
                    endTailRows();
                    return false;
                }
            }
            final long keptRowCount = Math.min(keyRowCount, ringSize);
            final long ringLo = keyRowCount > ringSize ? keyRowCount % ringSize : keptRowCount;
            reverseTailRows(rowLo, rowLo + ringLo);
            reverseTailRows(rowLo + ringLo, rowLo + keptRowCount);
            tailRows.setPos(rowLo + keptRowCount);
        }
        toTopFrames();
        return true;
    }

    /**
     * The rows the index holds for the keys {@code keyLo}, inclusive, to {@code keyHi}, exclusive, over the whole scan,
     * which it counts partition by partition, or -1 when it cannot.
     */
    private long countIndexRows(int keyLo, int keyHi) {
        toTopFrames();
        long count = 0;
        PageFrame frame;
        while ((frame = frameCursor.next(Long.MAX_VALUE)) != null) {
            circuitBreaker.statefulThrowExceptionIfTrippedTimeThrottledOrYield();
            final long frameRowCount = isCompositePartition(frame) ? -1
                    : keyOrderedFactory.getIndexRowCount(frame, keyLo, keyHi, frame.getPartitionLo(), frame.getPartitionHi() - 1, Long.MAX_VALUE);
            if (frameRowCount < 0) {
                return -1;
            }
            count += frameRowCount;
        }
        return count;
    }

    /**
     * The rows the index holds for the key over the whole scan, which it counts run by run once the read prepared its
     * runs, see prepareScanRuns(), and partition by partition otherwise, or -1 when it cannot.
     */
    private long countKeyRows(int key) {
        if (scanRunReaders.size() == 0) {
            return countIndexRows(key, key + 1);
        }
        final int indexKey = keyOrderedFactory.getIndexKey(key);
        if (indexKey < 0) {
            return 0;
        }
        long count = 0;
        for (int run = 0, n = scanRunCounters.size(); run < n; run++) {
            count += scanRunCounters.getQuick(run).countRows(indexKey, getRunIndexLo(run), getRunIndexHi(run), Long.MAX_VALUE);
        }
        return count;
    }

    /**
     * The rows of a read that the index orders: those of every row of the scan for a read of every key of the column,
     * and those the index counts for the keys otherwise, see {@link #countIndexRows(int, int)}, or -1 when it cannot
     * count them.
     */
    private long countReadRows() {
        if (!isKeyOrderedByIndex()) {
            return -1;
        }
        return keyOrderedFactory.isReadingEveryKey() ? countScanRows() : countIndexRows(0, keyCount);
    }

    /**
     * The rows of the keys up to the key that holds the row {@code target - 1} in key order, see
     * {@link #findKeyOfPosition(long)}, or of all keys when no key does, or -1 when the index cannot count them or does
     * not order the read.
     */
    private long countRowsTo(long target) {
        if (!isKeyOrderedByIndex()) {
            return -1;
        }
        final int key = findKeyOfPosition(target - 1);
        return key < 0 ? -1 : getCumulativeIndexRowCount(Math.min(key, keyCount - 1));
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
     * Makes the records of the read delegate to the record of the rows that hold the row, see {@link WindowRecord},
     * starting at the records of the page frames: a gather whose rows may split into windows moves them between its
     * record chain and the page frames, and a read whose mode waits may choose a mode with other records.
     */
    private void delegateRecords() {
        isDelegatingRecords = true;
        windowRecord.of(recordA);
        windowRecordB.of(recordB);
    }

    /**
     * Ends the collection, which stopped at its budget, at the page frame {@code frameLo}: each key reads its rows from
     * the page frame after its last run on, and from {@code frameLo} on at the earliest, in a pass of its own.
     */
    private void endCollection(int frameLo) {
        passFrameLo = frameLo;
        isCollectionComplete = true;
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
        while (key < gatheredRows.keyHi - 1 && isReadRowCountExact && getCumulativeIndexRowCount(key) >= 0 && rank >= getKeyRowBound(key)) {
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
        if (!isReadRowCountExact) {
            recordWindowStart(position, keyLo, rankLo, rowCount);
        }
        return -1;
    }

    /**
     * Drops the tail rows that a collection of the last rows of the read gave up on, and moves the frame cursor back to
     * its first page frame.
     */
    private void endTailRows() {
        rowCursor = Misc.free(rowCursor);
        tailRows.close();
        toTopFrames();
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
     * The key that holds the row at {@code position} in key order: the first key whose rows and the rows of the keys
     * before it outnumber the position, keyCount when no key does, or -1 when the index cannot count the rows of the
     * keys, see {@link #getCumulativeIndexRowCount(int)}. A position among the keys the read counted lands by a binary
     * search of their counts, and any other counts the keys after them up to the key that holds it.
     */
    private int findKeyOfPosition(long position) {
        if (isKeyRowCountUnknown) {
            return -1;
        }
        if (countedKeyCount > 0 && keyRowCounts.get(countedKeyCount - 1) > position) {
            int lo = 0;
            int hi = countedKeyCount - 1;
            while (lo < hi) {
                final int mid = (lo + hi) >>> 1;
                if (keyRowCounts.get(mid) > position) {
                    hi = mid;
                } else {
                    lo = mid + 1;
                }
            }
            return lo;
        }
        for (int key = countedKeyCount; key < keyCount; key++) {
            final long count = getCumulativeIndexRowCount(key);
            if (count < 0) {
                return -1;
            }
            if (count > position) {
                return key;
            }
        }
        return keyCount;
    }

    /**
     * The page frame of the run that holds the row of its partition, see prepareScanRuns(). The page frames of a run
     * hold rows in nearly even counts, so the share of the rows of the run before the row places the page frame, and a
     * step at a time moves on to it from there.
     */
    private int frameOf(int run, long row) {
        if (run != rowFrameRun || row < rowFrameLo || row >= rowFrameHi) {
            final boolean isBackward = isBackward();
            final int frameLo = getRunFrameLo(run);
            final int frameCount = (run + 1 < scanRunReaders.size() ? getRunFrameLo(run + 1) : frameAddressCache.getFrameCount()) - frameLo;
            final long runRowLo = getRunRowLo(run);
            final long runRowHi = getRunRowHi(run);
            final long rowOffset = isBackward ? runRowHi - 1 - row : row - runRowLo;
            int frame = frameLo + (int) Math.min((double) rowOffset * frameCount / (runRowHi - runRowLo), frameCount - 1);
            while (true) {
                final long frameRowLo = Rows.toLocalRowID(frameAddressCache.getRowIdOffset(frame));
                final long frameRowHi = frameRowLo + frameAddressCache.getFrameSize(frame);
                if (row < frameRowLo) {
                    frame += isBackward ? 1 : -1;
                } else if (row >= frameRowHi) {
                    frame += isBackward ? -1 : 1;
                } else {
                    rowFrameIndex = frame;
                    rowFrameLo = frameRowLo;
                    rowFrameHi = frameRowHi;
                    rowFrameRun = run;
                    break;
                }
            }
        }
        return rowFrameIndex;
    }

    /**
     * Moves the record to the row by its row id over the page frames. Once a read in windows returned its last row, it
     * drops its last window, and a consumer moves to its rows under walkHint from then on, so that the row groups the
     * consumer keeps decoded take the memory of the window, within {@link #getMemoryCap()}.
     */
    private void frameRecordAt(PageFrameMemoryRecord record, long rowId) {
        if (readMode == ReadMode.GATHER) {
            if (gatheredHi >= readHi && gatheredRows.isExhausted()) {
                gatheredRows.reset(keyCount, keyCount);
                gatheredLo = gatheredHi;
            }
            if (gatheredLo == gatheredHi && frameMemoryPool.getDecodeHint() != walkHint) {
                frameMemoryPool.setParquetDecodeHint(walkHint, getMemoryCap());
            }
        }
        super.recordAt(record, rowId);
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
     * which it holds whatever its bytes. Its passes decode under MONOTONIC, since each decodes a page frame at most
     * once. A read whose rows from the window on, in windows of the bytes its rows took, would decode more page frames
     * than the passes of its keys from there, see {@link #isKeyPassCheaper(int, long)}, or whose Parquet page frames stay
     * decoded across those passes at those bytes, see {@link #isParquetCachedAt(double)}, which then decode each of them
     * once, takes those passes instead, see {@link #startKeyPasses(long, int, long)}, and reports false. So does a read
     * whose window reaches a memory limit of the query that a sort of its rows fits, see
     * {@link #isSortWithinMemoryLimit()}, since those passes hold no record chain.
     */
    private boolean gatherWindow(long position, int keyLo, long rankLo) {
        frameMemoryPool.setParquetDecodeHint(ParquetDecodeHint.MONOTONIC);
        int keyLimit = keyCount;
        long decodeCountLo = frameMemoryPool.getDecodeCount();
        try {
            while (!gatherWindowRows(position, keyLo, rankLo, keyLimit)) {
                final long decodeCount = frameMemoryPool.getDecodeCount();
                discardedFrameCount += decodeCount - decodeCountLo;
                decodeCountLo = decodeCount;
                final boolean hasRowIds = gatheredRows.isWindowed;
                isParquetCached = isParquetCached && isParquetCachedAt(getPassRowBytes());
                replanWindows();
                if (isParquetCached || isKeyPassCheaper(keyLo, rankLo)) {
                    startKeyPasses(position, keyLo, rankLo);
                    return false;
                }
                frameMemoryPool.setParquetDecodeHint(ParquetDecodeHint.MONOTONIC);
                if (hasRowIds) {
                    final int gapKey = endFilledWindow(position, keyLo, rankLo);
                    if (gapKey < 0) {
                        return true;
                    }
                    keyLimit = gapKey + 1;
                }
            }
        } catch (CairoException e) {
            if (!e.isOutOfMemory() || !isSortWithinMemoryLimit()) {
                throw e;
            }
            startKeyPasses(position, keyLo, rankLo);
            return false;
        }
        return true;
    }

    /**
     * Gathers the window of rows that starts at the row {@code rankLo} of the key {@code keyLo}, at {@code position}
     * in key order, with one pass over the page frames in frame order, which decodes a page frame once, and only when
     * it holds a row the window takes and the read gathers rows or filters them. The window takes the rows from there
     * up to the row readHi, as many as it may hold: the counts of the index place its end among the keys and
     * their rows. They count the rows of a read that the index orders, and a filtered read takes as many of them as hold
     * the rows the window may hold by the share of them it is estimated to keep. The window takes no keys from
     * {@code keyLimit} on. Reports false, which leaves the window open, when its rows outgrow the memory of the record
     * chain before the pass ends: the budget of the gather, by the bytes the rows take rather than by their estimate,
     * and no more than {@link #getMemoryCap()} when the window starts.
     */
    private boolean gatherWindowRows(long position, int keyLo, long rankLo, int keyLimit) {
        int keyHi = keyLimit;
        long rankHi = Long.MAX_VALUE;
        long remaining = Long.MAX_VALUE;
        long windowRowEstimate = Math.max(readRowCount - position, 0);
        // the end of the read places the end of a window where the counts are the rows the read returns, or where the
        // rows carry their row ids, which stay valid after the window is gone
        final long rowCount = isReadRowCountExact || gatheredRows.isWindowed ? Math.min(windowRowCount, readHi - position) : windowRowCount;
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
            if (isReadRowCountExact) {
                remaining = windowRowEstimate;
            }
        }

        gatheredRows.reset(keyLo, keyHi);
        gatheredRows.maxBytes = Math.min(gatherBudget, getMemoryCap());
        gatheredRows.reserve((long) (windowRowEstimate * (gatherRowBytes + (gatheredRows.isWindowed ? Long.BYTES : 0))), !isReadRowCountExact || varSizeColumns.size() > 0);
        final boolean isByIndex = isKeyOrderedByIndex();
        final boolean isDenseScanAllowed = isDenseScanAllowed(keyLo, keyHi);
        final long remainingLo = remaining;
        final boolean isFrameMarkable = isByIndex && scanRunReaders.size() > 0;
        boolean isFrameMarked = false;
        boolean isKeyMapPrepared = false;
        toTopFrames();
        PageFrame frame;
        while (remaining > 0 && keyLo < keyHi && (frame = frameCursor.next()) != null) {
            final int frameIndex = addFrame(frame);
            gatheredRows.startFrame();
            retainFramesOfWiderRows();
            final long frameRowCount = frame.getPartitionHi() - frame.getPartitionLo();
            final boolean isDenseScan = isDenseScanAllowed && keyHi - keyLo > getDenseScanCost(frameRowCount);
            final boolean isFrameChecked = isByIndex && (!isDenseScan || varSizeColumns.size() > 0 || keyHi - keyLo <= getDecodeCost(frameRowCount));
            if (isFrameMarkable && isFrameChecked) {
                if (!isFrameMarked) {
                    markWindowFrames(keyLo, keyHi);
                    isFrameMarked = true;
                }
                if (windowFrameFlags.get(frameIndex) == 0) {
                    continue;
                }
            }
            if (isDenseScan) {
                long frameKeyRowCount = 1;
                if (isFrameChecked && !isFrameMarkable) {
                    countWalk(testFrameKeyCounts);
                    frameKeyRowCount = keyOrderedFactory.getRowCount(frame, keyLo, keyHi, frame.getPartitionLo(), frame.getPartitionHi() - 1, 0);
                }
                if (frameKeyRowCount == 0) {
                    continue;
                }
                if (frameKeyRowCount > 0) {
                    if (!isKeyMapPrepared) {
                        prepareSymbolKeyIndexes(keyLo, keyHi);
                        isKeyMapPrepared = true;
                    }
                    countWalk(testDenseScanFrames);
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
        if (!isReadRowCountExact) {
            recordWindowStart(position, keyLo, rankLo, remainingLo - remaining);
        }
        return true;
    }

    /**
     * The memory the lists of the collection may take, as the budget of the gather, see {@link #getGatherBudget(long)}:
     * the entries of a sort of the rows the read returns at most, which a skip bounds, see readHi, and no more than
     * {@link #getMemoryCap()} when the collection starts. Unlike the gather, the collection does not fail at the memory
     * limit of the query, since the passes of the keys that read the rows past it take no memory.
     */
    private long getCollectionBudget() {
        final long sortEntryBytes = readHi > Long.MAX_VALUE / (2 * Long.BYTES) ? Long.MAX_VALUE : readHi * 2 * Long.BYTES;
        return Math.min(sortEntryBytes, collectionMemoryCap);
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
     * The cost of the decode of a page frame of {@code rowCount} rows of fixed-size columns, in probes of the index for
     * a key: measured over Parquet row groups, about 64 probes plus a probe per 128 bytes of its rows.
     */
    private double getDecodeCost(long rowCount) {
        return 64 + rowCount * (double) decodedRowSize / 128;
    }

    /**
     * The bytes the Parquet page frames of the scan decode to, by the sizes of the column types and the bytes of the
     * var-size values of the rows the read sampled, see {@link #sampleGatheredRows()}.
     */
    private long getDecodedBytes() {
        return (long) (parquetRowCount * (decodedRowSize + gatherRowBytes - chainRowSize));
    }

    /**
     * The bytes the Parquet page frames of the scan decode to if their rows take {@code rowBytes} in the record chain.
     */
    private long getDecodedBytes(double rowBytes) {
        return (long) (parquetRowCount * (decodedRowSize + rowBytes - chainRowSize - chainVarcharHeaderSize));
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
        final long decodedBytes = getDecodedBytes();
        final long cacheBytes = frameMemoryPool.getCacheableBytes(ParquetDecodeHint.SCATTERED, parquetFrameCount, decodedBytes)
                - frameMemoryPool.getCacheableBytes(ParquetDecodeHint.MONOTONIC, parquetFrameCount, decodedBytes);
        final long sortEntryBytes = readRowCount * 2 * Long.BYTES;
        final long cappedBytes = Math.min(sortEntryBytes + Math.max(cacheBytes, 0), memoryCap);
        return Math.max(cappedBytes, sortEntryBytes);
    }

    /**
     * The index keys of the column: one per symbol of its symbol table, and one for NULL.
     */
    private int getIndexKeyCount() {
        return frameCursor.getSymbolTable(keyOrderedFactory.getColumnIndex()).getSymbolCount() + 1;
    }

    /**
     * The position in key order of the first row of the key: the rows the index holds for the keys before it, which it
     * counted, see {@link #getCumulativeIndexRowCount(int)}.
     */
    private long getKeyPosition(int key) {
        return key > 0 ? getCumulativeIndexRowCount(key - 1) : 0;
    }

    /**
     * The mode of a read of more than one key that does not gather its rows: the passes of the keys over the page frames
     * when the index orders its rows, and otherwise the collection of the rows its filter keeps.
     */
    private ReadMode getKeyReadMode() {
        return isKeyOrderedByIndex() ? ReadMode.PASSES : ReadMode.COLLECT;
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
            return (readRowCount + keyCount - 1) / keyCount;
        }
        return count - getKeyPosition(key);
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
     * ids. The row groups that such a consumer keeps decoded once the read dropped its last window take no more either,
     * nor do those that a direct read keeps decoded.
     */
    private long getMemoryCap() {
        final long limit = memoryTracker != null ? memoryTracker.getLimit() : 0;
        return limit > 0 ? Math.max(limit - memoryTracker.getUsed(), 0) / 2 : Long.MAX_VALUE;
    }

    /**
     * The bytes a row of the page frames of the scan takes in the record chain, as the pass of a window knows them: those
     * it took over the page frames it went over, and over those after them the most it measured or expected.
     */
    private double getPassRowBytes() {
        final double seenFrameShare = Math.min((double) frameCount / parquetFrameCount, 1);
        return seenFrameShare * gatheredRows.getAverageRowBytes() + (1 - seenFrameShare) * Math.max(gatherRowBytes, gatheredRows.getMeasuredRowBytes());
    }

    /**
     * The rows of the key that the pass of the key over the page frame went over up to the row, which the index counts
     * in the direction of the pass.
     */
    private long getPassRowCount(PageFrame frame, int key, long row) {
        final long partitionRow = frame.getPartitionLo() + row;
        return isBackward()
                ? keyOrderedFactory.getIndexRowCount(frame, key, key + 1, partitionRow, frame.getPartitionHi() - 1, Long.MAX_VALUE)
                : keyOrderedFactory.getIndexRowCount(frame, key, key + 1, frame.getPartitionLo(), partitionRow, Long.MAX_VALUE);
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

    private int getRunFrameLo(int run) {
        return (int) scanRuns.get(5L * run + 4);
    }

    /**
     * The last value of the index of the partition rows of the run, inclusive.
     */
    private long getRunIndexHi(int run) {
        return getRunRowHi(run) - 1 + getRunIndexShift(run);
    }

    /**
     * The first value of the index of the partition rows of the run.
     */
    private long getRunIndexLo(int run) {
        return getRunRowLo(run) + getRunIndexShift(run);
    }

    private long getRunIndexShift(int run) {
        return scanRuns.get(5L * run + 3);
    }

    private int getRunPartition(int run) {
        return (int) scanRuns.get(5L * run);
    }

    private long getRunRowHi(int run) {
        return scanRuns.get(5L * run + 2);
    }

    private long getRunRowLo(int run) {
        return scanRuns.get(5L * run + 1);
    }

    /**
     * The Parquet page frames the decode cache keeps under SCATTERED within {@link #getMemoryCap()}, at least one.
     */
    private int getScatteredFrameCapacity() {
        final long decodedBytes = getDecodedBytes();
        final long frameBytes = Math.max(decodedBytes / parquetFrameCount, 1);
        final long cacheBytes = Math.min(frameMemoryPool.getCacheableBytes(ParquetDecodeHint.SCATTERED, parquetFrameCount, decodedBytes), getMemoryCap());
        return Math.clamp(cacheBytes / frameBytes, 1, parquetFrameCount);
    }

    /**
     * Moves the read of the collected rows on to its next row, a collected row or a row of the pass of its key past the
     * page frames the collection went over, and reports whether there is one.
     */
    private boolean hasNextCollectedRow() {
        while (collectedRow == collectedRowHi) {
            if (rowCursor != null && rowCursor.hasNext()) {
                recordA.setRowIndex(rowCursor.next());
                return true;
            }
            if (!nextCollectedRun()) {
                return false;
            }
        }
        collectedReadCount++;
        recordA.setRowIndex(collectedRows.get(collectedRow++));
        return true;
    }

    /**
     * Moves the direct read on to its next row, the next row of the posting list it reads or the first row of the
     * posting lists after it, and reports whether there is one.
     */
    private boolean hasNextDirectRow() {
        if (directPosition < readHi) {
            while (rowCursor == null || !rowCursor.hasNext()) {
                if (!nextDirectRun()) {
                    isExhausted = true;
                    return false;
                }
            }
            final long row = directRowLo + rowCursor.next();
            frameMemoryPool.navigateTo(frameOf(directRun, row), recordA);
            recordA.setRowIndex(row - rowFrameLo);
            directPosition++;
            return true;
        }
        isExhausted = true;
        return false;
    }

    /**
     * Moves the gathered read on to its next row, gathering the windows after the window it holds until one holds a
     * row, and reports whether there is one. A read that moved on to the passes of its keys, see
     * {@link #startKeyPasses(long, int, long)}, reads those.
     */
    private boolean hasNextGatheredRow() {
        if (!isGatherPositioned) {
            positionGatheredRows();
        }
        if (readMode != ReadMode.GATHER) {
            return hasNext();
        }
        while (!gatheredRows.hasNext()) {
            if (gatheredHi >= readHi) {
                return false;
            }
            if (!gatherWindow(gatheredHi, nextWindowKey, nextWindowRank)) {
                return hasNext();
            }
            gatheredRows.toTop();
        }
        if (isDelegatingRecords) {
            windowRecord.of(gatheredRows.getRecord());
        }
        return true;
    }

    /**
     * Moves the read of the tail rows on to its next row, and reports whether there is one.
     */
    private boolean hasNextTailRow() {
        if (tailRow < 0) {
            isExhausted = true;
            return false;
        }
        final long tailRowId = tailRows.get(tailRow--);
        frameMemoryPool.navigateTo(Numbers.decodeHighInt(tailRowId), recordA);
        recordA.setRowIndex(Numbers.decodeLowInt(tailRowId));
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
     * True when a model of a direct read, see {@link ReadMode#DIRECT}, decodes no more row groups than a model of the
     * gather in windows of {@code windowRows} rows. Both read the rows of the posting lists of the keys in key order
     * over the runs: the direct read moves to their Parquet page frames through a decode cache that keeps the
     * {@code capacity} page frames it moved to last, and the gather decodes each page frame that holds a row of a
     * window once for the window. The model gives up on the direct read once it decodes more than {@code maxDecodes},
     * all page frames for every window.
     */
    private boolean isDirectModelCheaper(int capacity, long windowRows, long maxDecodes) {
        final int frameCount = frameAddressCache.getFrameCount();
        frameModel.reopen();
        frameModel.setCapacity(3L * frameCount);
        frameModel.setPos(3L * frameCount);
        frameModel.zero(-1);
        int head = -1;
        int tail = -1;
        int cachedCount = 0;
        long directDecodes = 0;
        long gatherDecodes = 0;
        long position = 0;
        for (int key = 0; key < keyCount; key++) {
            final int indexKey = keyOrderedFactory.getIndexKey(key);
            if (indexKey < 0) {
                continue;
            }
            for (int run = 0, n = scanRunReaders.size(); run < n; run++) {
                circuitBreaker.statefulThrowExceptionIfTrippedTimeThrottledOrYield();
                final long rowLo = getRunRowLo(run);
                final RowCursor rows = scanRunReaders.getQuick(run).getCursor(indexKey, getRunIndexLo(run), getRunIndexHi(run));
                try {
                    int lastFrame = -1;
                    int lastWindow = -1;
                    while (rows.hasNext()) {
                        final int frame = frameOf(run, rowLo + rows.next());
                        final int window = windowRows == Long.MAX_VALUE ? 0 : (int) (position / windowRows);
                        position++;
                        if (frame == lastFrame && window == lastWindow || frameAddressCache.getFrameFormat(frame) != PartitionFormat.PARQUET) {
                            continue;
                        }
                        final long model = 3L * frame;
                        if (frameModel.get(model + 2) != window) {
                            frameModel.set(model + 2, window);
                            gatherDecodes++;
                        }
                        lastWindow = window;
                        if (frame == lastFrame) {
                            continue;
                        }
                        lastFrame = frame;
                        final int prev = frameModel.get(model);
                        if (frame == head || prev >= 0) {
                            if (frame == tail) {
                                continue;
                            }
                            final int next = frameModel.get(model + 1);
                            if (frame == head) {
                                head = next;
                            } else {
                                frameModel.set(3L * prev + 1, next);
                            }
                            frameModel.set(3L * next, prev);
                        } else {
                            if (++directDecodes > maxDecodes) {
                                return false;
                            }
                            if (cachedCount == capacity) {
                                final int evicted = head;
                                head = frameModel.get(3L * evicted + 1);
                                frameModel.set(3L * evicted + 1, -1);
                                if (head < 0) {
                                    tail = -1;
                                } else {
                                    frameModel.set(3L * head, -1);
                                }
                            } else {
                                cachedCount++;
                            }
                        }
                        frameModel.set(model, tail);
                        frameModel.set(model + 1, -1);
                        if (tail < 0) {
                            head = frame;
                        } else {
                            frameModel.set(3L * tail + 1, frame);
                        }
                        tail = frame;
                    }
                } finally {
                    Misc.free(rows);
                }
            }
        }
        return directDecodes <= gatherDecodes;
    }

    /**
     * True when a read that the index orders may return its rows by row id, see {@link ReadMode#DIRECT}: the row groups
     * its decode cache keeps under SCATTERED, as the cache of a sort reading its rows back keeps them, fit
     * {@link #getMemoryCap()}, so that a tight query memory limit leaves the read to the windows of the gather.
     */
    private boolean isDirectReadAllowed() {
        return frameMemoryPool.getCacheableBytes(ParquetDecodeHint.SCATTERED, parquetFrameCount, getDecodedBytes()) <= getMemoryCap();
    }

    /**
     * True when a model of the direct read and of the gather in windows or the passes of the keys finds that the direct
     * read decodes no more row groups, see {@link #isDirectModelCheaper(int, long, long)}.
     */
    private boolean isDirectReadCheaper() {
        countWalk(testDirectModelRuns);
        // the passes of the keys over page frames that stay decoded decode each of them once, as a single window does
        final long windowRows = windowRowCount == Long.MAX_VALUE || isParquetCached ? Long.MAX_VALUE : windowRowCount;
        final long windowCount = windowRows == Long.MAX_VALUE ? 1 : (readRowCount + windowRows - 1) / windowRows;
        try {
            return isDirectModelCheaper(getScatteredFrameCapacity(), windowRows, windowCount * parquetFrameCount);
        } finally {
            frameModel.close();
        }
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
     * True when the passes of the keys from the key {@code key} on, past the first {@code rank} rows of that key, decode
     * no more Parquet page frames than the windows of the gather. A pass of a key decodes the page frames where the
     * index finds a row of the key, see {@link #nextPassFrame()}. A pass of a window decodes those where it finds a row
     * of a key of the window, which holds for a filtered read, since the index does not count the rows the filter keeps,
     * and overcounts the windows of a read that the index orders, which skip the page frames before their rows by those
     * counts. Such a read therefore asks only once a window outgrew its record chain, see
     * {@link #startGatherOrPasses()}. The windows take the rows of the index their rows may hold, see windowBoundScale,
     * so the counts of the index place the keys of each. The model stops once the page frames it did not count cannot
     * change its answer.
     * <p>
     * The page frames that windows already discarded count against the windows, although they are sunk, as a hedge
     * against further windows outgrowing their record chains. So the read keeps its windows only while those page frames
     * and the decodes of the windows stay below the passes of the keys, and what it wastes can reach what the passes cost
     * over the windows, plus the pass of the window that crossed it. A read whose keys the index cannot count has no such
     * model and would keep its windows forever, so it takes the passes once its windows discarded a scan of page frames.
     * <p>
     * The pass of a key does not decode the page frames it shares with the pass of the key before it while they fit the
     * decode cache under SCATTERED, which then still holds them: it comes back to each past as many other page frames as
     * the two share. Over more shared page frames, as over interleaved keys, it holds none and the pass counts each. A
     * full cache under a cyclic walk gives up the page frame it decoded last first, so it holds only that one for the next
     * key. Passes of keys that share more than one take SCATTERED, and the others keep the walk, which on a read of the
     * rows again still holds the page frames it decoded first.
     * <p>
     * The model probes the index for each key in each page frame, and the passes probe it as much again. Measured over
     * Parquet row groups, the two cost more than the record chain saves past about 0.6 probes per row of the scan, so over
     * more than half a probe per row, or than a page frame of few rows costs to decode, see {@link #getDecodeCost(long)},
     * the read keeps its windows without the model, until they discarded as many page frames as the passes can decode at
     * most: one for each row.
     */
    private boolean isKeyPassCheaper(int key, long rank) {
        if (windowRowCount == Long.MAX_VALUE) {
            return false;
        }
        final long rowHi = getCumulativeIndexRowCount(keyCount - 1);
        if (rowHi < 0) {
            return discardedFrameCount >= parquetFrameCount;
        }
        if ((double) parquetFrameCount * (keyCount - key) > Math.max(parquetRowCount / 2.0, parquetFrameCount * getDecodeCost(0))) {
            return discardedFrameCount >= parquetRowCount;
        }
        final double windowIndexRowCount = windowRowCount * windowBoundScale;
        final long rowLo = getKeyPosition(key) + rank;
        final long windowCount = (long) (Math.max(rowHi - rowLo, 0) / windowIndexRowCount) + 1;
        final int frameCapacity = getScatteredFrameCapacity();
        long keyDecodes = 0;
        boolean hasSharedRuns = false;
        long cachedDecodes = 0;
        long windowDecodes = discardedFrameCount;
        long uncountedFrameCount = parquetFrameCount;
        toTopFrames();
        try {
            frameModel.reopen();
            frameModel.setCapacity(keyCount - key);
            frameModel.setPos(keyCount - key);
            frameModel.zero(0);
            while (uncountedFrameCount > 0) {
                if (windowDecodes + uncountedFrameCount * windowCount < keyDecodes - cachedDecodes) {
                    return false;
                }
                if (keyDecodes + uncountedFrameCount * (keyCount - key) <= windowDecodes) {
                    return true;
                }
                final PageFrame frame = frameCursor.next();
                assert frame != null : "a read ordered by keys scans one partition, whose page frames isGatherRequired() counted";
                circuitBreaker.statefulThrowExceptionIfTrippedTimeThrottledOrYield();
                uncountedFrameCount--;
                long lastWindow = -1;
                int lastKey = -1;
                for (int k = key; k < keyCount; k++) {
                    if (keyOrderedFactory.getIndexRowCount(frame, k, k + 1, frame.getPartitionLo(), frame.getPartitionHi() - 1, 0) == 0) {
                        continue;
                    }
                    keyDecodes++;
                    if (lastKey >= 0 && getCumulativeIndexRowCount(lastKey) == getKeyPosition(k)) {
                        final int sharedFrameCount = frameModel.get(k - key) + 1;
                        frameModel.set(k - key, sharedFrameCount);
                        if (sharedFrameCount <= frameCapacity) {
                            cachedDecodes++;
                        } else if (sharedFrameCount == frameCapacity + 1) {
                            cachedDecodes -= frameCapacity;
                        }
                        hasSharedRuns |= sharedFrameCount > 1;
                    }
                    lastKey = k;
                    final long keyRowLo = Math.max(getKeyPosition(k), rowLo);
                    final long keyRowHi = Math.max(getCumulativeIndexRowCount(k), keyRowLo + 1);
                    final long windowLo = Math.max((long) ((keyRowLo - rowLo) / windowIndexRowCount), lastWindow + 1);
                    final long windowHi = (long) ((keyRowHi - 1 - rowLo) / windowIndexRowCount);
                    if (windowHi >= windowLo) {
                        windowDecodes += windowHi - windowLo + 1;
                        lastWindow = windowHi;
                    }
                }
            }
            final boolean isCheaper = keyDecodes - cachedDecodes <= windowDecodes;
            if (isCheaper && hasSharedRuns && cachedDecodes > 0) {
                walkHint = ParquetDecodeHint.SCATTERED;
            }
            return isCheaper;
        } finally {
            frameModel.close();
            toTopFrames();
        }
    }

    /**
     * True when the Parquet page frames of the scan stay decoded across the passes of the keys under the hint of those
     * passes, within {@link #getMemoryCap()}, if their rows take {@code rowBytes} in the record chain.
     */
    private boolean isParquetCachedAt(double rowBytes) {
        final long decodedBytes = getDecodedBytes(rowBytes);
        return frameMemoryPool.isCachingFrames(walkHint, parquetFrameCount, decodedBytes) && decodedBytes <= getMemoryCap();
    }

    /**
     * True when lists of the collection of {@code rowCapacity} rows and {@code runCapacity} longs of runs would pass
     * the budget of the collection, see {@link #getCollectionBudget()}, or the capacity of an int list.
     */
    private boolean isOverCollectionBudget(long rowCapacity, long runCapacity) {
        return rowCapacity > Numbers.MAX_SAFE_INT_POW_2
                || rowCapacity * Integer.BYTES + (runCapacity + collectedKeyRuns.getCapacity()) * Long.BYTES > getCollectionBudget();
    }

    /**
     * Whether the memory limit of the query holds the entries of a sort of the rows the read returns, a row id and a key
     * for each. A read under no limit of the query, which only the memory of the process bounds, fails at that memory.
     */
    private boolean isSortWithinMemoryLimit() {
        final long limit = memoryTracker != null ? memoryTracker.getLimit() : 0;
        return limit > 0 && limit >= Math.min(readRowCount, readHi) * 2 * Long.BYTES;
    }

    /**
     * Links each run from the run {@code runLo / 3} on, which holds its key in place of its next run, to the run of
     * its key before it.
     */
    private void linkCollectedRuns(long runLo) {
        for (long run = runLo / 3, runHi = collectedRuns.size() / 3; run < runHi; run++) {
            final long next = 3 * run + 2;
            final long keyRuns = 2 * collectedRuns.get(next);
            collectedRuns.set(next, -1);
            final long lastRun = collectedKeyRuns.get(keyRuns + 1);
            if (lastRun < 0) {
                collectedKeyRuns.set(keyRuns, run);
            } else {
                collectedRuns.set(3 * lastRun + 2, run);
            }
            collectedKeyRuns.set(keyRuns + 1, run);
        }
    }

    /**
     * Marks the page frames that hold a row of the keys {@code keyLo} to {@code keyHi}, see windowFrameFlags, from the
     * posting list of each key over the runs of the scan, see {@link #prepareScanRuns()}.
     */
    private void markWindowFrames(int keyLo, int keyHi) {
        final int frameCount = frameAddressCache.getFrameCount();
        windowFrameFlags.reopen();
        windowFrameFlags.setCapacity(Math.max(frameCount, 1));
        windowFrameFlags.setPos(frameCount);
        windowFrameFlags.zero(0);
        for (int key = keyLo; key < keyHi; key++) {
            final int indexKey = keyOrderedFactory.getIndexKey(key);
            if (indexKey < 0) {
                continue;
            }
            for (int run = 0, n = scanRunCounters.size(); run < n; run++) {
                circuitBreaker.statefulThrowExceptionIfTrippedTimeThrottledOrYield();
                final long rowLo = getRunRowLo(run);
                final RowCursor rows = scanRunCounters.getQuick(run).getCursor(indexKey, getRunIndexLo(run), getRunIndexHi(run));
                try {
                    while (rows.hasNext()) {
                        windowFrameFlags.set(frameOf(run, rowLo + rows.next()), 1);
                    }
                } finally {
                    Misc.free(rows);
                }
            }
        }
    }

    /**
     * Moves the read of the collected rows on to their next run, key by key and, within a key, run by run, collecting
     * rows until the key has another run or the collection ends, and then to the page frames of the pass of the key
     * past those the collection went over, and reports whether there is one.
     */
    private boolean nextCollectedRun() {
        while (true) {
            final long run = collectedRun < 0 ? collectedKeyRuns.get(2L * collectedKey) : collectedRuns.get(3 * collectedRun + 2);
            if (run >= 0) {
                final long entry = 3 * run;
                collectedRun = run;
                collectedRow = run > 0 ? collectedRuns.get(entry - 2) : 0;
                collectedRowHi = collectedRuns.get(entry + 1);
                frameMemoryPool.navigateTo((int) collectedRuns.get(entry), recordA);
                return true;
            }
            if (!isCollectionComplete) {
                collectNextRows();
            } else if (passFrameLo >= 0 && nextPassFrame()) {
                return true;
            } else if (collectedKey + 1 < keyCount) {
                collectedKey++;
                collectedRun = -1;
            } else {
                isExhausted = true;
                return false;
            }
        }
    }

    /**
     * Opens the posting list of the key keyLo over the run after the one the direct read reads, or of the next key that
     * the column holds over the first run once the key has no run left, and reports whether there is one.
     */
    private boolean nextDirectRun() {
        rowCursor = Misc.free(rowCursor);
        final int runCount = scanRunReaders.size();
        while (true) {
            if (++directRun >= runCount) {
                if (++keyLo >= keyCount || runCount == 0) {
                    keyLo = keyCount;
                    return false;
                }
                directRun = 0;
            }
            circuitBreaker.statefulThrowExceptionIfTrippedTimeThrottledOrYield();
            final int indexKey = keyOrderedFactory.getIndexKey(keyLo);
            if (indexKey < 0) {
                directRun = runCount;
                continue;
            }
            openDirectRun(indexKey, 0);
            return true;
        }
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
     * Moves the pass of the key of the collected read over the page frames on to its next page frame, whose rows of the
     * key its row cursor returns, and reports whether there is one. The pass of a key starts at the page frame after
     * the last run the collection holds of the key, and at {@link #passFrameLo} at the earliest, and decodes no page
     * frame where the index finds no row of the key.
     */
    private boolean nextPassFrame() {
        rowCursor = Misc.free(rowCursor);
        if (passKey != collectedKey) {
            passKey = collectedKey;
            final long lastRun = collectedKeyRuns.get(2L * collectedKey + 1);
            final int frameLo = lastRun < 0 ? passFrameLo : Math.max(passFrameLo, (int) collectedRuns.get(3 * lastRun) + 1);
            toTopFrames();
            while (frameCount < frameLo && frameCursor.next() != null) {
                frameCount++;
            }
        }
        PageFrame frame;
        while ((frame = frameCursor.next()) != null) {
            final int frameIndex = addFrame(frame);
            if (isCompositePartition(frame)
                    || keyOrderedFactory.getIndexRowCount(frame, collectedKey, collectedKey + 1, frame.getPartitionLo(), frame.getPartitionHi() - 1, 0) != 0) {
                final PageFrameMemory frameMemory = frameMemoryPool.navigateTo(frameIndex);
                recordA.init(frameMemory);
                rowCursor = keyOrderedFactory.getCursor(frame, frameMemory, collectedKey, collectedKey + 1);
                countWalk(testReadFrames);
                return true;
            }
        }
        return false;
    }

    /**
     * Opens the posting list of the index key over the run directRun, from the row of the key the posting list returns
     * after it skips {@code rank} of them in the index direction on.
     */
    private void openDirectRun(int indexKey, long rank) {
        long minValue = getRunIndexLo(directRun);
        long maxValue = getRunIndexHi(directRun);
        if (rank > 0) {
            final long row = scanRunCounters.getQuick(directRun).getRow(indexKey, minValue, maxValue, keyOrderedFactory.getIndexDirection(), rank);
            if (isBackward()) {
                maxValue = row;
            } else {
                minValue = row;
            }
        }
        directRowLo = minValue - getRunIndexShift(directRun);
        rowCursor = scanRunReaders.getQuick(directRun).getCursor(indexKey, minValue, maxValue);
    }

    /**
     * Estimates the rows the gathered read returns and the bytes each of them takes in the record chain, and sizes the
     * windows of the gather: one window holds every row when they fit the budget of the gather, and otherwise the
     * fewest windows that each fit it share the rows evenly. The rows of a read of more than one window carry their row
     * ids. A read that fits whose rows the index does not count leaves that to its first window, see
     * {@link ReadMode#PENDING}.
     * The record chain of a window takes no more memory than the budget, and than {@link #getMemoryCap()}, by the bytes
     * its rows take: a window whose rows outgrow it plans the windows anew, see {@link #gatherWindow(long, int, long)}.
     */
    private void planWindows() {
        sampleGatheredRows();
        isParquetCached = isParquetCached && isParquetCachedAt(gatherRowBytes);
        if (!isReadRowCountExact) {
            // an estimate of the rows keeps an eighth of headroom, so that the record chain of rows it falls short of
            // by less does not double its memory
            gatherRowBytes += gatherRowBytes / 8;
        }
        final long memoryCap = getMemoryCap();
        final long budget = getGatherBudget(memoryCap);
        gatherBudget = isReadRowCountExact ? budget : Math.min(budget, memoryCap);
        final boolean isOverBudget = readRowCount * gatherRowBytes > gatherBudget;
        setWindowed(isOverBudget);
        if (isOverBudget) {
            final long windowCount = (long) Math.ceil(readRowCount * (gatherRowBytes + Long.BYTES) / Math.max(gatherBudget, 1));
            windowRowCount = Math.max(1, (readRowCount + windowCount - 1) / windowCount);
        } else {
            windowRowCount = Long.MAX_VALUE;
        }
    }

    /**
     * Moves the direct read on to the row {@code position} in key order, at or past its next row: the counts of the
     * index land it on the key and the run that hold the row, and the posting list of the key there on the row, without
     * reading the rows before.
     */
    private void positionDirectRows(long position) {
        rowCursor = Misc.free(rowCursor);
        final int key = findKeyOfPosition(position);
        assert key >= 0 : "the runs of a direct read count the rows of every key";
        keyLo = key;
        directPosition = position;
        directRun = scanRunReaders.size();
        if (key < keyCount) {
            final int indexKey = keyOrderedFactory.getIndexKey(key);
            long rank = position - getKeyPosition(key);
            for (int run = 0, n = scanRunCounters.size(); run < n; run++) {
                final long runRowCount = scanRunCounters.getQuick(run).countRows(indexKey, getRunIndexLo(run), getRunIndexHi(run), Long.MAX_VALUE);
                if (rank < runRowCount) {
                    directRun = run;
                    openDirectRun(indexKey, rank);
                    return;
                }
                rank -= runRowCount;
            }
        }
    }

    /**
     * Positions the gathered rows at the row {@code gatherLo}, gathering the window that starts there first unless the
     * window the read holds holds that row. The row of a read that the index orders lands by the row counts of the
     * keys, and any other read moves its window only from its first row on. A read that moves on to the passes of its
     * keys from the window on, see {@link #startKeyPasses(long, int, long)}, skips their rows up to the row there.
     */
    private void positionGatheredRows() {
        resolveReadMode(0, RecordCursor.UNBOUNDED_ROW_COUNT);
        if (gatheredLo < 0 || gatherLo < gatheredLo || gatherLo >= gatheredHi) {
            if (isReadRowCountExact) {
                final int key = gatherLo > 0 ? findKeyOfPosition(gatherLo) : 0;
                assert key >= 0 : "a skip moves a read that the index counts by the counts of its keys";
                if (!gatherWindow(gatherLo, key, gatherLo - getKeyPosition(key))) {
                    isGatherPositioned = true;
                    return;
                }
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
                final long windowLo = entry < 0 ? 0 : windowStarts.get(entry);
                final boolean isGathered = entry < 0
                        ? gatherWindow(0, 0, 0)
                        : gatherWindow(windowLo, (int) windowStarts.get(entry + 1), windowStarts.get(entry + 2));
                if (!isGathered) {
                    isGatherPositioned = true;
                    countCounter.set(gatherLo - windowLo);
                    if (readMode == ReadMode.PASSES) {
                        skipKeyOrderedRows(countCounter);
                    } else {
                        skipCollectedRows(countCounter, keyCount);
                    }
                    return;
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
            // counts, so a page frame per partition frame costs it least; a filtered read filters page frames that
            // stay in the CPU caches across its keys instead
            readMode = getKeyReadMode();
            if (readMode == ReadMode.PASSES) {
                frameCursor.setPageFrameMaxRows(Integer.MAX_VALUE);
            } else {
                collectionMemoryCap = getMemoryCap();
                final long keyRunsSize = 2L * keyCount;
                collectedKeyRuns.reopen();
                collectedKeyRuns.clear();
                collectedKeyRuns.ensureCapacity(keyRunsSize);
                collectedKeyRuns.fill(-1);
                collectedKeyRuns.setPos(keyRunsSize);
                collectedRows.reopen();
                collectedRows.clear();
                collectedRuns.reopen();
                collectedRuns.clear();
            }
        }
        keyLo = 0;
        keyHi = Math.min(keyCount, 1);
    }

    /**
     * Caches every page frame of the scan and splits them into runs of page frames of a partition whose rows follow each
     * other in the index direction, each with the readers of the index of its partition, see scanRuns. Reports whether
     * the index reads the runs: it does not when a partition is composite or its index cannot count rows, which leaves
     * no run.
     */
    private boolean prepareScanRuns() {
        final int columnIndex = keyOrderedFactory.getColumnIndex();
        final int indexDirection = keyOrderedFactory.getIndexDirection();
        final boolean isBackward = isBackward();
        scanRuns.reopen();
        scanRuns.clear();
        scanRunCounters.clear();
        scanRunReaders.clear();
        rowFrameRun = -1;
        toTopFrames();
        try {
            PageFrame frame;
            while ((frame = frameCursor.next()) != null) {
                addFrame(frame);
                if (isCompositePartition(frame) || !(frame.getIndexReader(columnIndex, IndexReader.DIR_FORWARD) instanceof BitmapIndexFwdReader counter)) {
                    scanRuns.clear();
                    scanRunCounters.clear();
                    scanRunReaders.clear();
                    return false;
                }
                final int partitionIndex = frame.getPartitionIndex();
                final long rowLo = frame.getPartitionLo();
                final long rowHi = frame.getPartitionHi();
                final int run = scanRunReaders.size() - 1;
                if (run >= 0 && getRunPartition(run) == partitionIndex && (isBackward ? getRunRowLo(run) == rowHi : getRunRowHi(run) == rowLo)) {
                    scanRuns.set(5L * run + (isBackward ? 1 : 2), isBackward ? rowLo : rowHi);
                } else {
                    scanRuns.add(partitionIndex);
                    scanRuns.add(rowLo);
                    scanRuns.add(rowHi);
                    scanRuns.add(frame.getIndexRowLo() - rowLo);
                    scanRuns.add(frameCount - 1);
                    scanRunCounters.add(counter);
                    scanRunReaders.add(frame.getIndexReader(columnIndex, indexDirection));
                }
            }
            return true;
        } finally {
            toTopFrames();
        }
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
            final int indexKey = keyOrderedFactory.getIndexKey(key);
            if (indexKey >= 0 && indexKey < indexKeyCount) {
                symbolKeyIndexes.set(indexKey, key);
            }
        }
    }

    /**
     * Opens the cursor of the keys of the pass over the page frame and skips its rows up to the row count.
     */
    private void readFrameRows(PageFrame frame, Counter rowCount) {
        final int frameIndex = addFrame(frame);
        final PageFrameMemory frameMemory = frame.getFormat() == PartitionFormat.PARQUET ? null : frameMemoryPool.navigateTo(frameIndex);
        rowCursor = keyOrderedFactory.getCursor(frame, frameMemory, keyLo, keyHi);
        countWalk(testReadFrames);
        skipRowCursorRows(rowCount);
    }

    /**
     * Opens the cursor of the keys of the pass from {@code key} on over the page frame, starting the rows of that key at
     * {@code firstRow}.
     */
    private void readFrameRowsFrom(PageFrame frame, int key, long firstRow) {
        rowCursor = keyOrderedFactory.getCursor(frame, frameMemoryPool.navigateTo(addFrame(frame)), key, keyHi, firstRow);
        countWalk(testReadFrames);
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
     * by the bytes its rows took, see {@link KeyOrderedRowChain#getMeasuredRowBytes()}: each window holds as many rows
     * as fit the memory of the chain at those bytes, with their row ids. A window of a filtered read takes no more rows
     * of the index than the rows it may hold.
     */
    private void replanWindows() {
        gatherRowBytes = Math.max(gatherRowBytes, gatheredRows.getMeasuredRowBytes());
        windowBoundScale = 1;
        windowRowCount = Math.max(1, (long) (Math.max(gatheredRows.maxBytes, gatheredRows.pageSize) / (gatherRowBytes + Long.BYTES)));
        setWindowed(true);
    }

    /**
     * Chooses the mode of a read whose mode waits, see {@link ReadMode#PENDING}, for a read that skips
     * {@code skipCount} rows, {@link RecordCursor#UNBOUNDED_ROW_COUNT} when its outer cursor counts its rows to skip all
     * but its last ones, and returns no more than {@code rowCount} rows after them.
     * <ul>
     * <li>A read whose rows the index counts takes DIRECT when those rows are fewer than the Parquet page frames of the
     * scan, whose row groups it decodes no more than once a row, see {@link #setDirectHint(long)}, and otherwise the
     * mode that a model of both reads finds to decode fewer row groups, see {@link #isDirectReadCheaper()}.</li>
     * <li>A filtered gather that fits one window takes the passes of the keys over Parquet page frames that stay decoded
     * when the skip and the rows after it are known, since the passes end at the end of the read, which a window of the
     * gather only bounds. Otherwise it gathers once the read returns its rows without a bound, in a window that ends at
     * the end of the read, see readHi, where its rows carry their row ids, and it waits for a bounded read, whose skip
     * of the gather sets that end, see {@link #skipGatheredRows(Counter, long)}.</li>
     * </ul>
     */
    private void resolveReadMode(long skipCount, long rowCount) {
        if (readMode != ReadMode.PENDING) {
            return;
        }
        if (isReadRowCountExact) {
            if (rowCount < parquetFrameCount || isDirectReadCheaper()) {
                startDirectRead();
            } else {
                startGatherOrPasses();
            }
        } else if (isParquetCached && skipCount != RecordCursor.UNBOUNDED_ROW_COUNT && rowCount != RecordCursor.UNBOUNDED_ROW_COUNT) {
            startKeyReads();
        } else if (rowCount == RecordCursor.UNBOUNDED_ROW_COUNT) {
            readMode = ReadMode.GATHER;
            setWindowed(readHi < Long.MAX_VALUE);
        }
    }

    /**
     * Keeps the page frames that the pass of a window decoded once its rows measure wider than expected over page frames
     * that stay decoded at those bytes, so that the passes of the keys do not decode them again should the window outgrow
     * its record chain. Its budget gives the decode cache the share it expected the frames to need, see
     * {@link #getGatherBudget(long)}, and the frames kept and the memory of the chain together stay within a sort of the
     * rows at the bytes measured.
     */
    private void retainFramesOfWiderRows() {
        if (isParquetCached && gatheredRows.rowCount > 0 && gatheredRows.getMeasuredRowBytes() > gatherRowBytes) {
            final double rowBytes = getPassRowBytes();
            final long retainedBytes = readRowCount * 2 * Long.BYTES + getDecodedBytes(rowBytes) - gatheredRows.capacity;
            if (retainedBytes > 0 && isParquetCachedAt(rowBytes)) {
                frameMemoryPool.setParquetDecodeHint(walkHint, Math.min(retainedBytes, getMemoryCap()));
            }
        }
    }

    /**
     * Reverses the order of the tail rows from {@code lo}, inclusive, to {@code hi}, exclusive.
     */
    private void reverseTailRows(long lo, long hi) {
        for (long i = lo, j = hi - 1; i < j; i++, j--) {
            final long row = tailRows.get(i);
            tailRows.set(i, tailRows.get(j));
            tailRows.set(j, row);
        }
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
        readRowCount = countReadRows();
        isReadRowCountExact = readRowCount >= 0;
        if (isReadRowCountExact && varSizeColumns.size() == 0) {
            toTopFrames();
            return;
        }
        if (!isReadRowCountExact) {
            readRowCount = countScanRows();
        }
        toTopFrames();
        try {
            PageFrame frame;
            while ((frame = frameCursor.next()) != null) {
                final int frameIndex = addFrame(frame);
                final long frameRowCount = frame.getPartitionHi() - frame.getPartitionLo();
                if (frame.getFormat() == PartitionFormat.PARQUET && frameRowCount > 0) {
                    final PageFrameMemory frameMemory = frameMemoryPool.navigateTo(frameIndex);
                    if (!isReadRowCountExact) {
                        long keptRowCount = 0;
                        try (RowCursor keptRows = keyOrderedFactory.getCursor(frame, frameMemory, 0, keyCount)) {
                            while (keptRows.hasNext()) {
                                keptRows.next();
                                keptRowCount++;
                            }
                        }
                        readRowCount = (long) Math.ceil((double) readRowCount * keptRowCount / frameRowCount);
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
            addFrame(frame);
            if (cachedFrameIndex > frameCount
                    && frameCursor instanceof TablePageFrameCursor tableCursor
                    && tableCursor.skipToCachedFrame(frameAddressCache, cachedFrameIndex)) {
                frameCount = cachedFrameIndex;
            }
        }
        return frame;
    }

    /**
     * Sets the decode hint of a direct read that returns no more than {@code rowCount} rows: SCATTERED, so that the
     * decode cache keeps the row groups that the keys revisit, unless the read returns fewer rows than the scan has
     * Parquet page frames, whose row groups it then decodes no more than once a row under MONOTONIC. Either keeps the
     * decoded row groups within {@link #getMemoryCap()}.
     */
    private void setDirectHint(long rowCount) {
        frameMemoryPool.setParquetDecodeHint(rowCount < parquetFrameCount ? ParquetDecodeHint.MONOTONIC : ParquetDecodeHint.SCATTERED, getMemoryCap());
    }

    private void setWindowed(boolean isWindowed) {
        gatheredRows.isWindowed = isWindowed;
        windowRecord.isRowIdStored = isWindowed;
        windowRecordB.isRowIdStored = isWindowed;
    }

    /**
     * Skips the collected rows run by run, and the rows of the passes of the keys past the page frames the collection
     * went over row by row, up to the first row of the key {@code keyHi}.
     */
    private void skipCollectedRows(Counter rowCount, int keyHi) {
        while (rowCount.get() > 0 && collectedKey < keyHi) {
            if (collectedRow < collectedRowHi) {
                final long skipCount = Math.min(rowCount.get(), collectedRowHi - collectedRow);
                collectedRow += skipCount;
                collectedReadCount += skipCount;
                rowCount.dec(skipCount);
            } else if (rowCursor != null && rowCursor.hasNext()) {
                rowCursor.next();
                rowCount.dec();
            } else if (!nextCollectedRun()) {
                return;
            }
        }
    }

    /**
     * Skips the rows of a direct read, which lands on the row after them by the counts of the index, see
     * {@link #positionDirectRows(long)}, and returns no more than {@code requestedMaxRowsAfterSkip} rows after them,
     * under the decode hint of that many rows, see {@link #setDirectHint(long)}.
     */
    private void skipDirectRows(Counter rowCount, long requestedMaxRowsAfterSkip) {
        if (isExhausted) {
            return;
        }
        final long position = directPosition + Math.clamp(readRowCount - directPosition, 0, rowCount.get());
        rowCount.dec(position - directPosition);
        if (position > directPosition) {
            positionDirectRows(position);
        }
        readHi = addRows(position, requestedMaxRowsAfterSkip);
        setDirectHint(requestedMaxRowsAfterSkip);
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
            long count = skipCount > 0 ? countRowsTo(addRows(gatherLo, skipCount)) : 0;
            if (count < 0) {
                count = windowStartsRowCount;
            }
            if (count >= 0) {
                final long skippedCount = skipCount > 0 ? Math.clamp(count - gatherLo, 0, skipCount) : 0;
                gatherLo += skippedCount;
                rowCount.dec(skippedCount);
                readHi = addRows(gatherLo, requestedMaxRowsAfterSkip);
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
                addFrame(frame);
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

    private void skipRowCursorRows(Counter rowCount) {
        final long skipCount = rowCount.get();
        while (rowCount.get() > 0 && rowCursor.hasNext()) {
            rowCursor.next();
            rowCount.dec();
        }
        countWalkedRows(skipCount - rowCount.get());
    }

    private void startDirectRead() {
        readMode = ReadMode.DIRECT;
        frameMemoryPool.releaseFrameMemory();
        setDirectHint(Long.MAX_VALUE);
    }

    /**
     * Drops the window of the gather, whose rows outgrew the memory of its record chain, and reads the keys from the
     * window on in a pass of each over the page frames: the row {@code rank} of the key {@code key}, at the row
     * {@code position} in key order. A read that the index orders lands on that row by its counts. A filtered read, whose
     * rank may lie past the rows the filter keeps of the key, collects its rows over page frames that stay decoded, which
     * opens a cursor of a key over a page frame only once, see {@link #collectNextRows()}. Over any others a collection of
     * the rows of all keys would decode every page frame once more, so it takes the passes of its collection, see
     * {@link #nextPassFrame()}, which decode no page frame without a row of their key. The passes hold only the decode
     * cache, under the memory that the limit of the query leaves, see {@link #getMemoryCap()}. The records of the read
     * delegate to the page frames.
     */
    private void startKeyPasses(long position, int key, long rank) {
        assert isDelegatingRecords : "a window fills its record chain only under records that delegate, see startGatherOrPasses()";
        gatheredRows.reset(keyCount, keyCount);
        rowCursor = Misc.free(rowCursor);
        startKeyReads();
        if (readMode == ReadMode.PASSES) {
            keyLo = 0;
            keyHi = Math.min(keyCount, 1);
            scanFrameCount = -1;
            countCounter.set(position);
            skipKeyOrderedRows(countCounter);
        } else {
            if (!isParquetCached) {
                endCollection(0);
            }
            toTopCollectedRows();
            collectedKey = key;
            countCounter.set(rank);
            skipCollectedRows(countCounter, key + 1);
            collectedReadCount = position;
        }
    }

    /**
     * Leaves the gather to the reads of the keys: their records read the page frames, from the first, which the decode
     * cache holds under walkHint within {@link #getMemoryCap()}.
     */
    private void startKeyReads() {
        readMode = getKeyReadMode();
        frameMemoryPool.setParquetDecodeHint(walkHint, getMemoryCap());
        windowRecord.of(recordA);
        windowRecordB.of(recordB);
        toTopFrames();
    }

    /**
     * Starts the gather of a read ordered by keys, or leaves a read in windows to the passes of its keys over Parquet
     * page frames that stay decoded, which decode them once where each window would decode them, or a filtered read to
     * a pass of each key over page frames that its windows would decode more often, see
     * {@link #isKeyPassCheaper(int, long)}. A gather whose rows the index does not count and that fits one window waits
     * for the layout of its rows, see {@link ReadMode#PENDING}. The records of a gather delegate to the rows that hold
     * the row, see {@link #delegateRecords()}, when its rows may split into windows: a read in windows, a read whose
     * rows are an estimate, a read of var-size values, whose bytes may outgrow their sample, and a read under a query
     * memory limit, which may leave its record chain less than the budget it planned by.
     */
    private void startGatherOrPasses() {
        if (windowRowCount < Long.MAX_VALUE && isParquetCached) {
            startKeyReads();
            return;
        }
        if (!isKeyOrderedByIndex() && isKeyPassCheaper(0, 0)) {
            endCollection(0);
            startKeyReads();
            return;
        }
        readMode = isReadRowCountExact || windowRowCount < Long.MAX_VALUE ? ReadMode.GATHER : ReadMode.PENDING;
        if (gatheredRows.isWindowed || !isReadRowCountExact || varSizeColumns.size() > 0 || getMemoryCap() < Long.MAX_VALUE) {
            delegateRecords();
        }
    }

    /**
     * Moves the read of the collected rows back to the first run of the first key, keeping the rows collected so far.
     */
    private void toTopCollectedRows() {
        collectedKey = 0;
        collectedRun = -1;
        collectedRow = 0;
        collectedRowHi = 0;
        collectedReadCount = 0;
        passKey = -1;
    }

    private void toTopDirectRows() {
        directPosition = 0;
        directRun = -1;
    }

    private void toTopGatheredRows() {
        discardedFrameCount = 0;
        gatherLo = 0;
        isGatherPositioned = false;
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
        // The memory of the chain grows by pages of this size, a power of two, so that it holds at least one.
        private final long pageSize;
        // The rows of a read that one window holds whole, whose row ids are their offsets in the chain.
        private final RecordChain rows;
        // The rows of a window of a read that other windows follow, each with its row id in the page frames, which
        // stays valid after the window is gone.
        private final RecordChain windowRows;
        // The bytes the rows of the chain take, and the memory the chain holds for them.
        private long bytes;
        private long capacity;
        private RecordChain chain;
        // The bytes and the rows of the chain when the pass reached the last page frame that it took rows from, and the
        // page frame before that one, see startFrame().
        private long frameBytesLo;
        private long frameRowCountLo;
        private boolean isWindowed;
        private int key;
        private int keyHi;
        private int keyLo;
        // The memory the chain may take.
        private long maxBytes = Long.MAX_VALUE;
        private long recentBytesLo;
        private long recentRowCountLo;
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
            pageSize = Numbers.ceilPow2(Math.min(configuration.getSqlSortValuePageSize(), Files.PAGE_SIZE));
            final int maxPages = Math.clamp(configuration.getSqlSortValueMaxBytes() / pageSize, 1, Integer.MAX_VALUE);
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

        private long ceilPage(long bytes) {
            return (bytes + pageSize - 1) & -pageSize;
        }

        private void clearBytes() {
            bytes = 0;
            capacity = 0;
            frameBytesLo = 0;
            frameRowCountLo = 0;
            recentBytesLo = 0;
            recentRowCountLo = 0;
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
        private double getAverageRowBytes() {
            return (double) (bytes + rejectedRowBytes) / (rowCount + 1) - (isWindowed ? Long.BYTES : 0);
        }

        /**
         * The bytes a row took in the chain, past its row id, on average over its rows and the row it did not take, or
         * over those of the last two page frames it took rows from, when more: rows that grow wider along the pass
         * take more than the rows before them.
         */
        private double getMeasuredRowBytes() {
            final double rowBytes = (double) (bytes + rejectedRowBytes) / (rowCount + 1);
            final double recentRowBytes = (double) (bytes - recentBytesLo + rejectedRowBytes) / (rowCount - recentRowCountLo + 1);
            return Math.max(rowBytes, recentRowBytes) - (isWindowed ? Long.BYTES : 0);
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
         * True when the read of the chain went past its last row.
         */
        private boolean isExhausted() {
            return key >= keyHi;
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
                final long grownCapacity = ceilPage(Math.max(capacity * 2, bytes + storedBytes));
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
         * Reserves the memory of rows of {@code bytes} bytes in all, and no more than maxBytes. Rows that may outgrow
         * their estimate take maxBytes up front once a doubling of their memory would pass it, which leaves them all of it.
         */
        private void reserve(long bytes, boolean isEstimate) {
            final long reservedBytes = isEstimate && bytes > maxBytes / 2 ? maxBytes : Math.min(bytes, maxBytes);
            chain.reserve(reservedBytes);
            capacity = ceilPage(reservedBytes);
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

        /**
         * Notes the start of the next page frame of the pass, once the chain took rows from the page frame before.
         */
        private void startFrame() {
            if (rowCount > frameRowCountLo) {
                recentBytesLo = frameBytesLo;
                recentRowCountLo = frameRowCountLo;
                frameBytesLo = bytes;
                frameRowCountLo = rowCount;
            }
        }

        private void toTop() {
            key = keyLo - 1;
            chain.of(-1);
        }
    }

    /**
     * How a read of more than one key reads its rows key by key.
     */
    private enum ReadMode {
        /**
         * Collects the rows the filter keeps of its keys in one pass over the page frames, see
         * {@link KeyOrderedPageFrameRecordCursor#collectNextRows()}.
         */
        COLLECT,
        /**
         * Returns the rows of each key by row id from the posting lists of the index over the runs of the page frames, see
         * {@link KeyOrderedPageFrameRecordCursor#isDirectReadCheaper()}, which the decode cache serves under SCATTERED.
         */
        DIRECT,
        /**
         * Gathers its rows window by window, see {@link KeyOrderedPageFrameRecordCursor#gatherWindow(long, int, long)}.
         */
        GATHER,
        /**
         * Reads the rows of each key in a pass over the page frames, or, for a read of no more than one key, the rows of
         * the page frames in one pass.
         */
        PASSES,
        /**
         * Waits until its outer cursor reads it for the choice between DIRECT and the gather of a read whose rows the
         * index counts, or between the passes of the keys and the gather of a filtered read that fits one window, and
         * then for the layout of the rows of that gather, see
         * {@link KeyOrderedPageFrameRecordCursor#resolveReadMode(long, long)}.
         */
        PENDING,
        /**
         * Returns only the last rows of a filtered read, see {@link KeyOrderedPageFrameRecordCursor#collectTailRows(long)}.
         */
        TAIL
    }

    /**
     * Copies the columns of a record and then its row id.
     */
    private record RowIdRecordSink(RecordSink sink) implements RecordSink {

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
