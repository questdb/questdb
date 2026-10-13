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

import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.idx.AbstractPostingIndexReader;
import io.questdb.cairo.idx.IndexFwdNullReader;
import io.questdb.cairo.idx.IndexReader;
import io.questdb.cairo.idx.PostingIndexFwdReader;
import io.questdb.cairo.sql.ColumnMapping;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.PageFrame;
import io.questdb.cairo.sql.PageFrameCursor;
import io.questdb.cairo.sql.PageFrameMemoryRecord;
import io.questdb.cairo.sql.PartitionFormat;
import io.questdb.cairo.sql.PartitionFrameCursor;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RowCursor;
import io.questdb.cairo.sql.StaticSymbolTable;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.ByteList;
import io.questdb.std.DirectLongList;
import io.questdb.std.IntList;
import io.questdb.std.LongList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.QuietCloseable;
import io.questdb.std.Unsafe;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicLong;

/**
 * The page frames and the tasks of an index-driven parallel GROUP BY over a symbol
 * {@code IN}/{@code =} filter on a POSTING index.
 * <p>
 * <b>Frames</b> are the table's native page frames, as a forward table scan produces them, except
 * for their size: up to {@code frameRows} rows each (they still split at column tops). The
 * sequence caches their column addresses as for any scan, and the tasks read rows through them.
 * They are never handed to a consumer that reads every row of a frame: the factory that produces
 * this cursor does not advertise page frames, see
 * {@link IndexGatherRecordCursorFactory#supportsIndexGatherFrames()}.
 * <p>
 * <b>Tasks</b> are built once the frames are, from the posting index's per-key row counts
 * ({@link AbstractPostingIndexReader#estimateMatchesClamped}). For each run of a partition's
 * native frames, the keys of the list that have rows there are grouped, in ascending key order,
 * into tasks of about {@code taskRows} rows; a key with more rows is split by row range into
 * tasks of its own, at the row of its k-th posting where the index can tell
 * ({@link AbstractPostingIndexReader#selectKthMatch}), unless the consumer forbids such splits
 * (see below). A task's rows are the postings of its keys
 * in its row range, which a worker reads with a detached cursor
 * ({@link PostingIndexFwdReader#getDetachedCursor}). Partitions without a posting reader (a
 * Parquet partition, or one written before the column existed) are read by a scan task per frame
 * that keeps the rows whose key is in the list.
 * <p>
 * <b>Order.</b> Tasks follow the frames: partition by partition, and within a partition key by
 * key, then by row range. So for any one key, a later task reads later rows, and within a task a
 * key's rows ascend. Row ids are those of the frames, so an aggregate that depends on row order
 * (first, last) sees, within a group, rows in table order, provided that no group spans two keys:
 * the factory accepts such aggregates only when the grouping includes the key column.
 * <p>
 * <b>No splits for order-sensitive aggregates.</b> Pieces of one key run on different slots. An
 * aggregate that compares row ids on every update and merge (first, last) is exact whatever slot
 * reads which piece, but not one whose state depends on the order rows arrive in on a slot or on
 * which of equal rows comes first: {@code array_agg}, {@code twap} and {@code sparkline} cut a
 * group's buffer into batches only where the frame changes, and merge those batches by their
 * first row, which assumes that no two slots read rows of one frame for one group; {@code mode}
 * and {@code arg_max}/{@code arg_min} keep whichever tied row a slot met first and merge without
 * row ids. With {@code splitKeysByRowRange} false, which the GROUP BY asks for whenever an
 * aggregate declares {@link io.questdb.griffin.engine.functions.GroupByFunction#isOrderSensitive()},
 * a key's rows of a partition run form one task, so one slot reads them, in order.
 * <p>
 * <b>Readers.</b> The posting readers the tasks read are prepared and frozen
 * ({@link AbstractPostingIndexReader#warmForKeys}, {@link IndexReader#setFrozen}) on the query's
 * thread before any task is dispatched, and unfrozen when this cursor closes or rewinds, after
 * the sequence has waited for every task.
 */
public class IndexGatherPageFrameCursor implements PageFrameCursor {
    public static final byte TASK_INDEX = 0;
    public static final byte TASK_SCAN = 1;
    // a run's slice of the list's keys, counted and read by the worker, see buildUnits()
    public static final byte TASK_UNIT = 2;
    // (run, key) count probes from which the tasks are built as units, see buildTasks()
    static final long UNIT_MODE_MIN_PROBES = 4096;
    // units per slot (the owner's and each worker's) that unit mode aims for
    private static final int UNITS_PER_SLOT = 8;
    private static long unitModeMinProbes = UNIT_MODE_MIN_PROBES;
    private static final int[] NO_KEYS = new int[0];
    private final ByteList frameFormats = new ByteList();
    private final IntList framePartitions = new IntList();
    private final ObjList<IndexReader> frameReaders = new ObjList<>();
    private final LongList frameRowHis = new LongList();
    private final LongList frameRowLos = new LongList();
    private final int frameRows;
    // index runs of native frames: first frame, frame end, posting reader; see buildTasks()
    private final IntList runFrameLos = new IntList();
    private final IntList runFrameHis = new IntList();
    private final ObjList<AbstractPostingIndexReader> runReaders = new ObjList<>();
    // whether the tasks may be built as units the workers count, see buildTasks()
    private final boolean unitsEnabled;
    private final int workerCount;
    private final ObjList<AbstractPostingIndexReader> frozenReaders = new ObjList<>();
    private final FwdTableReaderPageFrameCursor inner;
    private final int keyColumnIndex;
    // whether a key with more than taskRows rows in a partition run is split by row range
    private final boolean splitKeysByRowRange;
    // the index keys of the list (TableUtils.toIndexKey), distinct and ascending
    private final IntList keys = new IntList();
    private final IntList taskFrameHis = new IntList();
    private final IntList taskFrameLos = new IntList();
    private final IntList taskKeyHis = new IntList();
    private final IntList taskKeyLos = new IntList();
    private final ByteList taskKinds = new ByteList();
    private final ObjList<IndexReader> taskReaders = new ObjList<>();
    private final ObjList<TaskReader> taskReaderSlots;
    private final LongList taskRowCounts = new LongList();
    private final LongList taskRowHis = new LongList();
    private final LongList taskRowLos = new LongList();
    // the keys of the index tasks, task by task, see taskKeyLos and taskKeyHis
    private final IntList taskKeys = new IntList();
    private final long taskRows;
    private final AtomicLong testIndexTasks = new AtomicLong();
    private final AtomicLong testScanTasks = new AtomicLong();
    private final AtomicLong testWorkerTasks = new AtomicLong();
    private final AtomicLong testUnitTasks = new AtomicLong();
    private boolean areTasksBuilt;
    // the last build made units, see buildTasks()
    private boolean isUnitMode;
    private long estimatedRows;
    private boolean isExhausted;

    public IndexGatherPageFrameCursor(
            @NotNull IntList columnIndexes,
            @NotNull IntList columnSizeShifts,
            int keyColumnIndex,
            int frameRows,
            long taskRows,
            boolean splitKeysByRowRange,
            int workerCount,
            boolean unitsEnabled
    ) {
        this.inner = new FwdTableReaderPageFrameCursor(columnIndexes, columnSizeShifts, null, 1);
        this.inner.setPageFrameRows(frameRows);
        this.keyColumnIndex = keyColumnIndex;
        this.frameRows = frameRows;
        this.taskRows = Math.max(1, taskRows);
        this.splitKeysByRowRange = splitKeysByRowRange;
        this.workerCount = workerCount;
        this.unitsEnabled = unitsEnabled;
        // one reader per slot: the owner's (-1) and the workers'
        this.taskReaderSlots = new ObjList<>(workerCount + 1);
        for (int i = 0; i <= workerCount; i++) {
            taskReaderSlots.add(new TaskReader(i - 1));
        }
    }

    /**
     * Resolves the symbols to the index keys of the reader's symbol table: distinct, ascending,
     * symbols the table does not have dropped, NULL as key 0.
     */
    public static void resolveKeys(
            @NotNull ObjList<Function> keyFunctions,
            @NotNull StaticSymbolTable symbolTable,
            @NotNull IntList sink
    ) {
        sink.clear();
        for (int i = 0, n = keyFunctions.size(); i < n; i++) {
            final int symbolKey = symbolTable.keyOf(keyFunctions.getQuick(i).getSymbol(null));
            if (symbolKey != SymbolTable.VALUE_NOT_FOUND) {
                sink.add(TableUtils.toIndexKey(symbolKey));
            }
        }
        sink.sortGroups(1);
        // drop duplicates, which repeated literals or bind variable values make
        int distinct = 0;
        for (int i = 0, n = sink.size(); i < n; i++) {
            final int key = sink.getQuick(i);
            if (distinct == 0 || sink.getQuick(distinct - 1) != key) {
                sink.setQuick(distinct++, key);
            }
        }
        sink.setPos(distinct);
    }

    /**
     * Keeps the rows, frame row indexes of the record's frame, that pass the filter, in order.
     */
    public static void filterRows(Function filter, DirectLongList rows, PageFrameMemoryRecord record) {
        long kept = 0;
        for (long i = 0, n = rows.size(); i < n; i++) {
            final long r = rows.get(i);
            record.setRowIndex(r);
            if (filter.getBool(record)) {
                rows.set(kept++, r);
            }
        }
        rows.setPos(kept);
    }

    @Override
    public void calculateSize(RecordCursor.Counter counter) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void close() {
        unfreeze();
        Misc.free(inner);
        Misc.freeObjListAndKeepObjects(taskReaderSlots);
    }

    @Override
    public ColumnMapping getColumnMapping() {
        return inner.getColumnMapping();
    }

    /**
     * Rows the tasks are estimated to read, from the posting index's counts; the rows of scan
     * tasks count in full. Valid once the frames are built.
     */
    public long getEstimatedRows() {
        return estimatedRows;
    }

    /**
     * Below this many (run, key) count probes, the query's thread builds the tasks from the
     * index's counts; from it on, it builds units the workers count. Tests force either.
     */
    @TestOnly
    public static void setUnitModeMinProbes(long probes) {
        unitModeMinProbes = probes;
    }

    @TestOnly
    public int getFrozenReaderCount() {
        return frozenReaders.size();
    }

    /**
     * Tasks of the last execution: index tasks, scan tasks, and tasks a worker slot read.
     */
    @TestOnly
    public long[] getTaskCounts() {
        return new long[]{testIndexTasks.get(), testScanTasks.get(), testWorkerTasks.get(), testUnitTasks.get()};
    }

    /**
     * Whether the last execution's tasks were built as units, see {@link #buildTasks()}.
     */
    @TestOnly
    public boolean isUnitMode() {
        return isUnitMode;
    }

    public int getKeyColumnIndex() {
        return keyColumnIndex;
    }

    @TestOnly
    public IntList getKeys() {
        return keys;
    }

    public @Nullable LongList getIntervals() {
        return inner.getIntervals();
    }

    @Override
    public long getRemainingRowsInInterval() {
        return 0;
    }

    @Override
    public StaticSymbolTable getSymbolTable(int columnIndex) {
        return inner.getSymbolTable(columnIndex);
    }

    public TableReader getTableReader() {
        return inner.getTableReader();
    }

    public int getTaskCount() {
        return taskKinds.size();
    }

    public byte getTaskKind(int taskIndex) {
        return taskKinds.getQuick(taskIndex);
    }

    /**
     * The task reader of a slot, -1 being the owner's. Only the slot's holder may use it.
     */
    public TaskReader getTaskReader(int slotId) {
        return taskReaderSlots.getQuick(slotId + 1);
    }

    /**
     * The estimated rows of each task, which the sequence dispatches the tasks by. Valid once the
     * frames are built.
     */
    public LongList getTaskRowCounts() {
        return taskRowCounts;
    }

    public boolean hasIntervalFilter() {
        return inner.hasIntervalFilter();
    }

    @Override
    public boolean isExternal() {
        return false;
    }

    @Override
    public SymbolTable newSymbolTable(int columnIndex) {
        return inner.newSymbolTable(columnIndex);
    }

    /**
     * Returns the next native or Parquet frame of the scan. After the last frame the tasks are
     * built, and the posting readers they read frozen. The skip target is ignored: the consumer
     * aggregates every task.
     */
    @Override
    public @Nullable PageFrame next(long skipTarget) {
        if (isExhausted) {
            return null;
        }
        final PageFrame frame = inner.next(0);
        if (frame != null) {
            final byte format = frame.getFormat();
            framePartitions.add(frame.getPartitionIndex());
            frameRowLos.add(frame.getPartitionLo());
            frameRowHis.add(frame.getPartitionHi());
            frameFormats.add(format);
            frameReaders.add(format == PartitionFormat.NATIVE ? frame.getIndexReader(keyColumnIndex, IndexReader.DIR_FORWARD) : null);
            return frame;
        }
        isExhausted = true;
        buildTasks();
        return null;
    }

    /**
     * Opens the scan over the given partitions and resolves the list's keys. The key functions
     * are initialized against this cursor.
     */
    public IndexGatherPageFrameCursor of(
            SqlExecutionContext executionContext,
            PartitionFrameCursor partitionFrameCursor,
            ObjList<Function> keyFunctions
    ) throws SqlException {
        clearState();
        testIndexTasks.set(0);
        testScanTasks.set(0);
        testWorkerTasks.set(0);
        testUnitTasks.set(0);
        inner.of(executionContext, partitionFrameCursor);
        for (int i = 0, n = keyFunctions.size(); i < n; i++) {
            keyFunctions.getQuick(i).init(this, executionContext);
        }
        resolveKeys(keyFunctions, inner.getSymbolTable(keyColumnIndex), keys);
        return this;
    }

    @Override
    public long size() {
        return -1;
    }

    @Override
    public boolean supportsSizeCalculation() {
        return false;
    }

    @Override
    public void toTop() {
        unfreeze();
        clearState();
        inner.toTop();
    }

    private static int findFrame(LongList frameRowHis, int frameLo, int frameHi, long row) {
        // the first frame of [frameLo, frameHi) whose rows end above the row
        int lo = frameLo;
        int hi = frameHi - 1;
        while (lo < hi) {
            final int mid = (lo + hi) >>> 1;
            if (frameRowHis.getQuick(mid) <= row) {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        return lo;
    }

    private void addIndexTask(int frameLo, int frameHi, long rowLo, long rowHi, int keyLo, int keyHi, long rows, IndexReader reader) {
        taskKinds.add(TASK_INDEX);
        taskFrameLos.add(findFrame(frameRowHis, frameLo, frameHi, rowLo));
        taskFrameHis.add(findFrame(frameRowHis, frameLo, frameHi, rowHi - 1) + 1);
        taskRowLos.add(rowLo);
        taskRowHis.add(rowHi);
        taskKeyLos.add(keyLo);
        taskKeyHis.add(keyHi);
        taskReaders.add(reader);
        taskRowCounts.add(Math.max(1, rows));
        estimatedRows += rows;
    }

    private void addScanTask(int frameIndex) {
        final long rows = frameRowHis.getQuick(frameIndex) - frameRowLos.getQuick(frameIndex);
        taskKinds.add(TASK_SCAN);
        taskFrameLos.add(frameIndex);
        taskFrameHis.add(frameIndex + 1);
        taskRowLos.add(frameRowLos.getQuick(frameIndex));
        taskRowHis.add(frameRowHis.getQuick(frameIndex));
        taskKeyLos.add(0);
        taskKeyHis.add(0);
        taskReaders.add(null);
        taskRowCounts.add(Math.max(1, rows));
        estimatedRows += rows;
    }

    private void buildIndexTasks(int frameLo, int frameHi, AbstractPostingIndexReader reader) {
        final long rowLo = frameRowLos.getQuick(frameLo);
        final long rowHi = frameRowHis.getQuick(frameHi - 1);
        final long callerHiInclusive = rowHi - 1;
        final long entryMax = reader.getEntryMaxValue();
        final long clampedMax = entryMax >= 0 ? Math.min(callerHiInclusive, entryMax) : callerHiInclusive;
        int sliceLo = taskKeys.size();
        long sliceRows = 0;
        boolean used = false;
        for (int i = 0, n = keys.size(); i < n; i++) {
            final int key = keys.getQuick(i);
            long count = reader.estimateMatchesClamped(key, rowLo, callerHiInclusive, clampedMax);
            if (count == AbstractPostingIndexReader.ESTIMATE_REJECT || count == Numbers.LONG_NULL) {
                // no bounded count: read the key on its own (by row range, when allowed), as if it
                // held every row
                count = rowHi - rowLo;
            }
            if (count <= 0) {
                continue;
            }
            used = true;
            if (count > taskRows && splitKeysByRowRange) {
                if (taskKeys.size() > sliceLo) {
                    addIndexTask(frameLo, frameHi, rowLo, rowHi, sliceLo, taskKeys.size(), sliceRows, reader);
                }
                final int keyPos = taskKeys.size();
                taskKeys.add(key);
                final long pieces = Math.min((count + taskRows - 1) / taskRows, rowHi - rowLo);
                long pieceLo = rowLo;
                for (long j = 1; j <= pieces; j++) {
                    long pieceHi;
                    if (j == pieces) {
                        pieceHi = rowHi;
                    } else {
                        // the row of the key's posting that starts the next piece, when the index can tell
                        pieceHi = reader.selectKthMatch(key, rowLo, callerHiInclusive, clampedMax, count / pieces * j);
                        if (pieceHi == Numbers.LONG_NULL || pieceHi <= pieceLo || pieceHi >= rowHi) {
                            pieceHi = rowLo + (long) ((double) (rowHi - rowLo) * j / pieces);
                        }
                        if (pieceHi <= pieceLo) {
                            continue;
                        }
                    }
                    addIndexTask(frameLo, frameHi, pieceLo, pieceHi, keyPos, keyPos + 1, count / pieces, reader);
                    pieceLo = pieceHi;
                }
                sliceLo = taskKeys.size();
                sliceRows = 0;
                continue;
            }
            if (sliceRows + count > taskRows && taskKeys.size() > sliceLo) {
                addIndexTask(frameLo, frameHi, rowLo, rowHi, sliceLo, taskKeys.size(), sliceRows, reader);
                sliceLo = taskKeys.size();
                sliceRows = 0;
            }
            taskKeys.add(key);
            sliceRows += count;
        }
        if (taskKeys.size() > sliceLo) {
            addIndexTask(frameLo, frameHi, rowLo, rowHi, sliceLo, taskKeys.size(), sliceRows, reader);
        }
        if (used) {
            freeze(reader);
        }
    }

    /**
     * Builds the tasks once the frames are. Runs of a partition's contiguous native frames, read
     * through its posting reader, make index tasks; other frames scan tasks.
     * <p>
     * With few (run, key) pairs, the query's thread builds index tasks from the index's per-key
     * counts, which balance the tasks by the rows they read and split a frequent key by row range.
     * With many, which a long list over many partitions makes, those counts are serial work on the
     * query's thread before any worker starts. The tasks are then built as units instead
     * ({@link #TASK_UNIT}): a run and a slice of the list's keys, which the slot that takes the
     * unit counts and reads. Units never split a key within a run, so a key's rows of a run come,
     * in order, from one slot, as with {@code splitKeysByRowRange} false. The choice is made at
     * every open, from the frames and the keys of that open.
     */
    private void buildTasks() {
        assert !areTasksBuilt;
        areTasksBuilt = true;
        runFrameLos.clear();
        runFrameHis.clear();
        runReaders.clear();
        for (int f = 0, n = framePartitions.size(); f < n; ) {
            final IndexReader reader = frameReaders.getQuick(f);
            if (frameFormats.getQuick(f) != PartitionFormat.NATIVE || !(reader instanceof PostingIndexFwdReader posting)) {
                f++;
                continue;
            }
            // a run of a partition's contiguous native frames, all read through its posting reader
            int g = f + 1;
            while (g < n
                    && framePartitions.getQuick(g) == framePartitions.getQuick(f)
                    && frameFormats.getQuick(g) == PartitionFormat.NATIVE
                    && frameReaders.getQuick(g) == reader
                    && frameRowLos.getQuick(g) == frameRowHis.getQuick(g - 1)) {
                g++;
            }
            runFrameLos.add(f);
            runFrameHis.add(g);
            runReaders.add(posting);
            f = g;
        }
        final int runCount = runReaders.size();
        isUnitMode = unitsEnabled && keys.size() > 0 && (long) runCount * keys.size() >= unitModeMinProbes;
        // tasks follow the frames: a run's tasks where its first frame is, scan tasks in between
        int run = 0;
        for (int f = 0, n = framePartitions.size(); f < n; ) {
            if (run < runCount && runFrameLos.getQuick(run) == f) {
                final int g = runFrameHis.getQuick(run);
                if (isUnitMode) {
                    buildUnits(f, g, runReaders.getQuick(run), runCount);
                } else {
                    buildIndexTasks(f, g, runReaders.getQuick(run));
                }
                run++;
                f = g;
                continue;
            }
            if (isScanNeeded(frameReaders.getQuick(f))) {
                addScanTask(f);
            }
            f++;
        }
    }

    // Unit mode: the run's units, the list's keys in contiguous slices, as many slices as make
    // the units of all runs about UNITS_PER_SLOT per slot. Their rows are estimated from the
    // run's rows and the share of the table's symbols the slice holds; the slot reads them.
    private void buildUnits(int frameLo, int frameHi, AbstractPostingIndexReader reader, int runCount) {
        final long rowLo = frameRowLos.getQuick(frameLo);
        final long rowHi = frameRowHis.getQuick(frameHi - 1);
        final int keyCount = keys.size();
        final long targetUnits = (long) UNITS_PER_SLOT * (workerCount + 1);
        final int slices = (int) Math.max(1, Math.min(keyCount, (targetUnits + runCount - 1) / runCount));
        final int symbolCount = Math.max(1, inner.getSymbolTable(keyColumnIndex).getSymbolCount() + 1);
        for (int i = 0; i < slices; i++) {
            final int keyLo = (int) ((long) keyCount * i / slices);
            final int keyHi = (int) ((long) keyCount * (i + 1) / slices);
            if (keyHi <= keyLo) {
                continue;
            }
            taskKinds.add(TASK_UNIT);
            taskFrameLos.add(frameLo);
            taskFrameHis.add(frameHi);
            taskRowLos.add(rowLo);
            taskRowHis.add(rowHi);
            taskKeyLos.add(keyLo);
            taskKeyHis.add(keyHi);
            taskReaders.add(reader);
            final long rows = Math.max(1, (rowHi - rowLo) * Math.min(keyHi - keyLo, symbolCount) / symbolCount);
            taskRowCounts.add(rows);
            estimatedRows += rows;
        }
        freeze(reader);
    }

    private void freeze(AbstractPostingIndexReader reader) {
        if (!reader.isFrozen()) {
            // positioned at this query's index snapshot, with its value memory mapped in full, so
            // the workers' detached cursors read it without changing it; an interval scan meets a
            // partition's reader once per interval
            reader.warmForKeys(NO_KEYS, null);
            reader.setFrozen(true);
            frozenReaders.add(reader);
        }
    }

    private void clearState() {
        framePartitions.clear();
        frameRowLos.clear();
        frameRowHis.clear();
        frameFormats.clear();
        frameReaders.clear();
        taskKinds.clear();
        taskFrameLos.clear();
        taskFrameHis.clear();
        taskRowLos.clear();
        taskRowHis.clear();
        taskKeyLos.clear();
        taskKeyHis.clear();
        taskReaders.clear();
        taskRowCounts.clear();
        taskKeys.clear();
        runFrameLos.clear();
        runFrameHis.clear();
        runReaders.clear();
        estimatedRows = 0;
        isExhausted = false;
        areTasksBuilt = false;
    }

    private boolean isKeyInList(int indexKey) {
        return keys.binarySearchUniqueList(indexKey) > -1;
    }

    // A partition without a posting reader has no rows of the list when the column does not exist
    // there (every row is NULL) and NULL is not in the list. Every other one is scanned.
    private boolean isScanNeeded(IndexReader reader) {
        if (keys.size() == 0) {
            return false;
        }
        if (reader instanceof IndexFwdNullReader) {
            return isKeyInList(0);
        }
        return true;
    }

    private void unfreeze() {
        for (int i = 0, n = frozenReaders.size(); i < n; i++) {
            frozenReaders.getQuick(i).setFrozen(false);
        }
        frozenReaders.clear();
    }

    /**
     * Reads one task's rows, a frame at a time, as the row indexes of that frame: the postings of
     * the task's keys within its row range, or, for a scan task, the frame's rows whose key is in
     * the list. A slot owns one; see {@link #getTaskReader(int)}.
     */
    public class TaskReader implements QuietCloseable {
        // a frame's rows of the buffer, the frames ascending: (frame index, end position in the buffer)
        private final IntList bucketFrames = new IntList();
        private final LongList bucketEnds = new LongList();
        // posting rows of the task not yet returned, absolute partition rows, bucketed by frame
        private DirectLongList buffer = new DirectLongList(4096, MemoryTag.NATIVE_OFFLOAD, true);
        // the drained rows before bucketing, key after key
        private DirectLongList drained = new DirectLongList(4096, MemoryTag.NATIVE_OFFLOAD, true);
        private final LongList segmentStarts = new LongList();
        // where a key's rows start in the buffer, ascending; see getKeyRuns()
        private final LongList runStarts = new LongList();
        // the key runs of the rows next() returned last, as positions in those rows
        private final LongList currentRuns = new LongList();
        private int[] lastSegments = new int[16];
        private int runPos;
        private final int slotId;
        private int bucket;
        private long bufferPos;
        private int[] counts = new int[16];
        private RowCursor cursor;
        private int frameIndex;
        private int keyPos;
        private long rowHi;
        private long rowLo;
        private int task;

        TaskReader(int slotId) {
            this.slotId = slotId;
        }

        @Override
        public void close() {
            cursor = Misc.free(cursor);
            Misc.free(buffer);
            Misc.free(drained);
        }

        /**
         * The frame the rows {@link #next} returned last belong to.
         */
        public int getFrameIndex() {
            return frameIndex;
        }

        /**
         * Where, in the rows {@link #next} returned last, a run of one key's rows starts,
         * ascending, the first at 0; or an empty list when the reader cannot tell (a scan task).
         */
        public LongList getKeyRuns() {
            return currentRuns;
        }

        /**
         * Fills {@code rows}, after clearing it, with at most {@code maxRows} rows of the task, all
         * of one frame, as that frame's row indexes; {@link #getFrameIndex()} tells which frame.
         * A key's rows ascend; the rows of the task's keys follow each other key by key. For a
         * scan task the record must be on the frame, see {@link #peekFrameIndex()}. Returns false
         * when the task has no rows left.
         */
        public boolean next(DirectLongList rows, int maxRows, PageFrameMemoryRecord record) {
            rows.clear();
            currentRuns.clear();
            if (taskKinds.getQuick(task) == TASK_SCAN) {
                return nextScan(rows, maxRows, record);
            }
            if (bucket >= bucketFrames.size() && !refill(maxRows)) {
                return false;
            }
            frameIndex = bucketFrames.getQuick(bucket);
            final long end = bucketEnds.getQuick(bucket++);
            final long frameRowLo = frameRowLos.getQuick(frameIndex);
            final long count = end - bufferPos;
            rows.ensureCapacity(count);
            final long src = buffer.getAddress() + bufferPos * Long.BYTES;
            final long dst = rows.getAppendAddress();
            for (long i = 0; i < count; i++) {
                Unsafe.putLong(dst + i * Long.BYTES, Unsafe.getLong(src + i * Long.BYTES) - frameRowLo);
            }
            rows.skip(count);
            for (int n = runStarts.size(); runPos < n && runStarts.getQuick(runPos) < end; runPos++) {
                currentRuns.add(runStarts.getQuick(runPos) - bufferPos);
            }
            assert currentRuns.size() > 0 && currentRuns.getQuick(0) == 0;
            bufferPos = end;
            return true;
        }

        /**
         * Positions the reader at the start of a task.
         */
        public void of(int task) {
            this.task = task;
            this.frameIndex = taskFrameLos.getQuick(task);
            this.rowLo = taskRowLos.getQuick(task);
            this.rowHi = taskRowHis.getQuick(task);
            this.keyPos = taskKeyLos.getQuick(task);
            this.cursor = Misc.free(cursor);
            // the buffers are freed with the cursor, see close()
            buffer.reopen();
            drained.reopen();
            buffer.clear();
            bufferPos = 0;
            bucketFrames.clear();
            bucketEnds.clear();
            bucket = 0;
            runStarts.clear();
            currentRuns.clear();
            runPos = 0;
            final byte kind = taskKinds.getQuick(task);
            (kind == TASK_SCAN ? testScanTasks : kind == TASK_UNIT ? testUnitTasks : testIndexTasks).incrementAndGet();
            if (slotId > -1) {
                testWorkerTasks.incrementAndGet();
            }
        }

        /**
         * The frame of a scan task, which the record must be on before {@link #next} is called.
         */
        public int peekFrameIndex() {
            return frameIndex;
        }

        // Buckets the drained rows by frame: rows of one frame together, frames ascending, and
        // within a frame key after key, each key's rows ascending.
        private void bucket(int frameLo, int frameHi) {
            final long size = drained.size();
            final long addr = drained.getAddress();
            final int frameCount = frameHi - frameLo;
            if (frameCount == 1) {
                swapBuffers();
                bucketFrames.add(frameLo);
                bucketEnds.add(size);
                runStarts.addAll(segmentStarts);
                return;
            }
            if (segmentStarts.size() == 1) {
                // one key: its rows ascend, so the frames do
                swapBuffers();
                int f = frameLo;
                for (long i = 0; i < size; ) {
                    final long row = Unsafe.getLong(addr + i * Long.BYTES);
                    while (frameRowHis.getQuick(f) <= row) {
                        f++;
                    }
                    final long frameRowHi = frameRowHis.getQuick(f);
                    long j = i + 1;
                    while (j < size && Unsafe.getLong(addr + j * Long.BYTES) < frameRowHi) {
                        j++;
                    }
                    bucketFrames.add(f);
                    bucketEnds.add(j);
                    runStarts.add(i);
                    i = j;
                }
                return;
            }
            // a counting sort by frame, stable; each key's rows ascend, so its frame walks forward
            if (counts.length < frameCount + 1) {
                counts = new int[Math.max(frameCount + 1, counts.length * 2)];
            }
            Arrays.fill(counts, 0, frameCount + 1, 0);
            for (int s = 0, n = segmentStarts.size(); s < n; s++) {
                final long lo = segmentStarts.getQuick(s);
                final long hi = s + 1 < n ? segmentStarts.getQuick(s + 1) : size;
                int f = frameLo;
                for (long i = lo; i < hi; i++) {
                    final long row = Unsafe.getLong(addr + i * Long.BYTES);
                    while (frameRowHis.getQuick(f) <= row) {
                        f++;
                    }
                    counts[f - frameLo + 1]++;
                }
            }
            for (int k = 0; k < frameCount; k++) {
                final int c = counts[k + 1];
                counts[k + 1] = counts[k] + c;
                if (c > 0) {
                    bucketFrames.add(frameLo + k);
                    bucketEnds.add(counts[k + 1]);
                }
            }
            buffer.clear();
            buffer.ensureCapacity(size);
            final long dst = buffer.getAddress();
            if (lastSegments.length < frameCount) {
                lastSegments = new int[Math.max(frameCount, lastSegments.length * 2)];
            }
            Arrays.fill(lastSegments, 0, frameCount, -1);
            for (int s = 0, n = segmentStarts.size(); s < n; s++) {
                final long lo = segmentStarts.getQuick(s);
                final long hi = s + 1 < n ? segmentStarts.getQuick(s + 1) : size;
                int f = frameLo;
                for (long i = lo; i < hi; i++) {
                    final long row = Unsafe.getLong(addr + i * Long.BYTES);
                    while (frameRowHis.getQuick(f) <= row) {
                        f++;
                    }
                    final int pos = counts[f - frameLo]++;
                    if (lastSegments[f - frameLo] != s) {
                        // the key's first row in this frame
                        lastSegments[f - frameLo] = s;
                        runStarts.add(pos);
                    }
                    Unsafe.putLong(dst + (long) pos * Long.BYTES, row);
                }
            }
            buffer.skip(size);
            runStarts.sort();
        }

        private boolean nextScan(DirectLongList rows, int maxRows, PageFrameMemoryRecord record) {
            // rowLo walks the frame's rows; the frame holds the whole task
            final long frameRowLo = frameRowLos.getQuick(frameIndex);
            long r = rowLo - frameRowLo;
            final long hi = rowHi - frameRowLo;
            if (r >= hi) {
                return false;
            }
            final long batchHi = Math.min(hi, r + maxRows);
            rows.ensureCapacity(batchHi - r);
            for (; r < batchHi; r++) {
                record.setRowIndex(r);
                if (isKeyInList(TableUtils.toIndexKey(record.getInt(keyColumnIndex)))) {
                    rows.add(r);
                }
            }
            rowLo = frameRowLo + r;
            return true;
        }

        // Drains up to maxRows rows of the task's keys, in key order, then buckets them by frame.
        private boolean refill(int maxRows) {
            drained.clear();
            segmentStarts.clear();
            runStarts.clear();
            runPos = 0;
            bucketFrames.clear();
            bucketEnds.clear();
            bucket = 0;
            bufferPos = 0;
            final int keyHi = taskKeyHis.getQuick(task);
            final boolean unit = taskKinds.getQuick(task) == TASK_UNIT;
            final IndexReader reader = taskReaders.getQuick(task);
            while (keyPos < keyHi && drained.size() < maxRows) {
                if (cursor == null) {
                    final int key = unit ? keys.getQuick(keyPos) : taskKeys.getQuick(keyPos);
                    if (unit && !hasRows((AbstractPostingIndexReader) reader, key)) {
                        // the unit's count, which the query's thread no longer makes
                        keyPos++;
                        continue;
                    }
                    cursor = reader.getDetachedCursor(key, rowLo, rowHi - 1, null);
                }
                final long start = drained.size();
                final long wanted = maxRows - start;
                final long n = cursor.drainTo(drained, 0, wanted);
                if (n > 0) {
                    // the cursor's rows are relative to the range's low row
                    final long addr = drained.getAddress();
                    for (long i = start, m = start + n; i < m; i++) {
                        Unsafe.putLong(addr + i * Long.BYTES, Unsafe.getLong(addr + i * Long.BYTES) + rowLo);
                    }
                    segmentStarts.add(start);
                }
                if (n < wanted) {
                    cursor = Misc.free(cursor);
                    keyPos++;
                }
            }
            if (drained.size() == 0) {
                buffer.clear();
                return false;
            }
            bucket(taskFrameLos.getQuick(task), taskFrameHis.getQuick(task));
            return true;
        }

        // Whether the key may have rows in the unit's range: a count of zero says it has none.
        private boolean hasRows(AbstractPostingIndexReader reader, int key) {
            final long callerHiInclusive = rowHi - 1;
            final long entryMax = reader.getEntryMaxValue();
            final long clampedMax = entryMax >= 0 ? Math.min(callerHiInclusive, entryMax) : callerHiInclusive;
            return reader.estimateMatchesClamped(key, rowLo, callerHiInclusive, clampedMax) != 0;
        }

        private void swapBuffers() {
            final DirectLongList t = buffer;
            buffer = drained;
            drained = t;
        }
    }
}
