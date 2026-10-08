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

package io.questdb.griffin.engine.join;

import io.questdb.MessageBus;
import io.questdb.cairo.AbstractRecordCursorFactory;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.sql.PartitionFormat;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.PageFrameMemory;
import io.questdb.cairo.sql.PageFrameMemoryRecord;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.StaticSymbolTable;
import io.questdb.cairo.sql.async.PageFrameReduceTask;
import io.questdb.cairo.sql.async.PageFrameReduceTaskFactory;
import io.questdb.cairo.sql.async.PageFrameReducer;
import io.questdb.cairo.sql.async.PageFrameSequence;
import io.questdb.cairo.vm.api.MemoryCARW;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.table.AsyncFilterUtils;
import io.questdb.jit.CompiledFilter;
import io.questdb.mp.SCSequence;
import io.questdb.std.DirectIntIntHashMap;
import io.questdb.std.DirectLongList;
import io.questdb.std.IntHashSet;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.Rows;
import io.questdb.std.Unsafe;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

import static io.questdb.cairo.sql.PartitionFrameCursorFactory.ORDER_ASC;
import static io.questdb.cairo.sql.PartitionFrameCursorFactory.ORDER_DESC;
import static io.questdb.griffin.engine.join.AbstractAsOfJoinFastRecordCursor.scaleTimestamp;
import static io.questdb.griffin.engine.table.AsyncFilterUtils.applyCompiledFilter;

/**
 * Keyed ASOF JOIN on one SYMBOL column, the master's page frames joined in parallel.
 * <p>
 * Per master page frame, a worker finds for each master row the last slave row of the same key at or
 * before the row's timestamp (ties: the last in storage order), or none. It scans the frame's slave
 * span - the slave rows from the frame's first master timestamp to its last - once, forward, merged
 * with the master rows, keeping per key the last slave row id met. A key that has no slave row in the
 * span up to a master row takes the prevailing row before the span, from the backward scan that the
 * page frames share through {@link WindowJoinPrevailingSummaries}. The output of a frame is one slave
 * row id per master row, -1 for none, and the values of the slave's fixed-size columns gathered by
 * those row ids, so that a column-wise consumer reads both sides as arrays.
 * <p>
 * Results are those of the serial ASOF JOIN, TOLERANCE included.
 */
public class AsyncAsOfJoinRecordCursorFactory extends AbstractRecordCursorFactory {
    static final long NO_ROW = -1;
    @TestOnly
    public static final int WALK_ALWAYS = 1;
    @TestOnly
    public static final int WALK_AUTO = 0;
    @TestOnly
    public static final int WALK_NEVER = 2;
    // walked rows the walk may spend over half the span rows it passed before it gives up
    private static final long WALK_SLACK_ROWS = 4096;
    // tests force one mode: the walk always (never giving up), or the span scan always
    @TestOnly
    public static volatile int WALK_MODE = WALK_AUTO;
    private static final int CIRCUIT_BREAKER_CHECK_ROWS = 1024;
    private static final PageFrameReducer FILTER_AND_JOIN = AsyncAsOfJoinRecordCursorFactory::filterAndJoin;
    private static final PageFrameReducer JOIN = AsyncAsOfJoinRecordCursorFactory::join;
    // a master row whose key has no slave row in the span: resolved from the prevailing scan
    private static final long PREVAILING = -2;
    private final SCSequence collectSubSeq = new SCSequence();
    private final @Nullable CharSequence selectReason;
    private final int workerCount;
    private AsyncAsOfJoinRecordCursor cursor;
    private PageFrameSequence<AsyncAsOfJoinAtom> frameSequence;
    private JoinRecordMetadata joinMetadata;
    private RecordCursorFactory masterFactory;
    private RecordCursorFactory slaveFactory;

    public AsyncAsOfJoinRecordCursorFactory(
            @NotNull CairoEngine engine,
            @NotNull CairoConfiguration configuration,
            @NotNull MessageBus messageBus,
            @NotNull JoinRecordMetadata joinMetadata,
            @NotNull RecordCursorFactory masterFactory,
            @NotNull RecordCursorFactory slaveFactory,
            int masterSymbolIndex,
            int slaveSymbolIndex,
            long toleranceInterval,
            boolean isMasterFiltered,
            @NotNull PageFrameReduceTaskFactory reduceTaskFactory,
            int workerCount,
            @Nullable CharSequence selectReason
    ) {
        super(joinMetadata);
        assert masterFactory.supportsPageFrameCursor();
        assert slaveFactory.supportsTimeFrameCursor();
        this.joinMetadata = joinMetadata;
        this.masterFactory = masterFactory;
        this.slaveFactory = slaveFactory;
        this.workerCount = workerCount;
        this.selectReason = selectReason;
        final int columnSplit = masterFactory.getMetadata().getColumnCount();

        final int masterTsType = masterFactory.getMetadata().getTimestampType();
        final int slaveTsType = slaveFactory.getMetadata().getTimestampType();
        long masterTsScale = 1;
        long slaveTsScale = 1;
        if (masterTsType != slaveTsType) {
            masterTsScale = ColumnType.getTimestampDriver(masterTsType).toNanosScale();
            slaveTsScale = ColumnType.getTimestampDriver(slaveTsType).toNanosScale();
        }

        PageFrameSequence<AsyncAsOfJoinAtom> frameSequence0 = null;
        try {
            final AsyncAsOfJoinAtom atom = new AsyncAsOfJoinAtom(
                    configuration,
                    slaveFactory,
                    masterSymbolIndex,
                    slaveSymbolIndex,
                    masterFactory.getMetadata().getTimestampIndex(),
                    toleranceInterval,
                    masterTsScale,
                    slaveTsScale,
                    workerCount
            );
            frameSequence0 = new PageFrameSequence<>(
                    engine,
                    configuration,
                    messageBus,
                    atom,
                    isMasterFiltered ? FILTER_AND_JOIN : JOIN,
                    reduceTaskFactory,
                    workerCount,
                    PageFrameReduceTask.TYPE_WINDOW_JOIN
            );
            this.cursor = new AsyncAsOfJoinRecordCursor(
                    configuration,
                    masterFactory.getMetadata(),
                    slaveFactory,
                    columnSplit,
                    isMasterFiltered
            );
        } catch (Throwable th) {
            Misc.free(frameSequence0, th);
            throw th;
        }
        this.frameSequence = frameSequence0;
    }

    /**
     * Hands the filters the code generator stole from the master and the slave to the atom, once
     * the factory is built. Cannot fail.
     */
    public void adoptFilters(
            @Nullable CompiledFilter compiledMasterFilter,
            @Nullable MemoryCARW bindVarMemory,
            @Nullable ObjList<Function> bindVarFunctions,
            @Nullable Function masterFilter,
            @Nullable ObjList<Function> perWorkerMasterFilters,
            @Nullable IntHashSet filterUsedColumnIndexes,
            boolean isMasterKeyFilter,
            @Nullable Function slaveKeyFilter,
            int slaveKeyFilterColumnIndex
    ) {
        frameSequence.getAtom().adoptFilters(
                compiledMasterFilter,
                bindVarMemory,
                bindVarFunctions,
                masterFilter,
                perWorkerMasterFilters,
                filterUsedColumnIndexes,
                isMasterKeyFilter,
                slaveKeyFilter,
                slaveKeyFilterColumnIndex
        );
    }

    @Override
    public PageFrameSequence<AsyncAsOfJoinAtom> execute(SqlExecutionContext executionContext, SCSequence collectSubSeq, int order) throws SqlException {
        final CairoConfiguration config = executionContext.getCairoEngine().getConfiguration();
        executionContext.changePageFrameSizes(config.getSqlSmallPageFrameMinRows(), config.getSqlSmallPageFrameMaxRows());
        try {
            return frameSequence.of(masterFactory, executionContext, collectSubSeq, order);
        } finally {
            executionContext.restoreToDefaultPageFrameSizes();
        }
    }

    @Override
    @TestOnly
    public AsyncAsOfJoinAtom getAtom() {
        return frameSequence.getAtom();
    }

    @Override
    public RecordCursorFactory getBaseFactory() {
        return masterFactory;
    }

    @Override
    public RecordCursor getCursor(SqlExecutionContext executionContext) throws SqlException {
        final int masterOrder = masterFactory.getScanDirection() == SCAN_DIRECTION_BACKWARD ? ORDER_DESC : ORDER_ASC;
        final int slaveOrder = slaveFactory.getScanDirection() == SCAN_DIRECTION_BACKWARD ? ORDER_DESC : ORDER_ASC;
        final PageFrameSequence<AsyncAsOfJoinAtom> masterFrameSequence = execute(executionContext, collectSubSeq, masterOrder);
        try {
            cursor.of(masterFrameSequence, slaveOrder, executionContext);
            return cursor;
        } catch (Throwable th) {
            cursor.close();
            throw th;
        }
    }

    @Override
    public int getScanDirection() {
        return masterFactory.getScanDirection();
    }

    @Override
    public TableToken getTableToken() {
        return masterFactory.getTableToken();
    }

    @Override
    public boolean recordCursorSupportsRandomAccess() {
        return false;
    }

    @Override
    public boolean supportsUpdateRowId(TableToken tableToken) {
        return masterFactory.supportsUpdateRowId(tableToken);
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.type("Async AsOf Join");
        sink.meta("workers").val(workerCount);
        final AsyncAsOfJoinAtom atom = frameSequence.getAtom();
        sink.attr("symbol")
                .val(masterFactory.getMetadata().getColumnName(atom.getMasterSymbolIndex()))
                .val("=")
                .val(slaveFactory.getMetadata().getColumnName(atom.getSlaveSymbolIndex()));
        if (selectReason != null) {
            sink.attr("select").val(selectReason);
        }
        sink.val(atom);
        if (atom.getMasterFilter(0) != null) {
            sink.attr("master filter").val(atom.getMasterFilter(0), masterFactory);
        }
        if (atom.getSlaveKeyFilter() != null) {
            sink.attr("slave key filter").val(atom.getSlaveKeyFilter(), slaveFactory);
        }
        sink.child(masterFactory);
        sink.child(slaveFactory);
    }

    @Override
    public boolean usesExternalDataSource() {
        final RecordCursorFactory masterFactory = this.masterFactory;
        if (masterFactory != null && masterFactory.usesExternalDataSource()) {
            return true;
        }
        final RecordCursorFactory slaveFactory = this.slaveFactory;
        return slaveFactory != null && slaveFactory.usesExternalDataSource();
    }

    private static long applyFilter(
            @NotNull PageFrameMemoryRecord record,
            @NotNull PageFrameReduceTask task,
            @NotNull AsyncAsOfJoinAtom atom,
            int slotId,
            long frameRowCount
    ) {
        final boolean isParquetFrame = task.isParquetFrame();
        final boolean useLateMaterialization = atom.shouldUseLateMaterialization(slotId, isParquetFrame);
        final PageFrameMemory frameMemory;
        if (useLateMaterialization) {
            frameMemory = task.populateFrameMemory(atom.getFilterUsedColumnIndexes());
        } else {
            frameMemory = task.populateFrameMemory();
        }
        record.init(frameMemory);
        final DirectLongList rows = task.getFilteredRows();
        rows.clear();

        final Function filter = atom.getMasterFilter(slotId);
        final CompiledFilter compiledFilter = atom.getCompiledMasterFilter();
        if (compiledFilter == null || frameMemory.hasColumnTops() || frameMemory.hasColumnTypeCasts()) {
            AsyncFilterUtils.applyFilter(filter, rows, record, frameRowCount);
        } else {
            applyCompiledFilter(compiledFilter, atom.getBindVarMemory(), atom.getBindVarFunctions(), task);
        }

        final long filteredRowCount = rows.size();
        task.setFilteredRowCount(filteredRowCount);
        if (isParquetFrame) {
            atom.getSelectivityStats(slotId).update(rows.size(), frameRowCount);
        }
        if (useLateMaterialization && task.populateRemainingColumns(atom.getFilterUsedColumnIndexes(), rows, true)) {
            record.init(frameMemory);
        }
        return filteredRowCount;
    }

    private static void filterAndJoin(
            int workerId,
            @NotNull PageFrameMemoryRecord record,
            @NotNull PageFrameReduceTask task,
            @NotNull SqlExecutionCircuitBreaker circuitBreaker,
            @Nullable PageFrameSequence<?> stealingFrameSequence
    ) {
        final long frameRowCount = task.getFrameRowCount();
        assert frameRowCount > 0;
        final AsyncAsOfJoinAtom atom = task.getFrameSequence(AsyncAsOfJoinAtom.class).getAtom();
        final boolean owner = stealingFrameSequence != null && stealingFrameSequence == task.getFrameSequence();
        final int slotId = atom.maybeAcquire(workerId, owner, circuitBreaker);
        try {
            final long rowCount = applyFilter(record, task, atom, slotId, frameRowCount);
            if (rowCount > 0 && !atom.isSkipJoin()) {
                joinFrame(atom, slotId, record, task.getFilteredRows(), true, rowCount, circuitBreaker);
            }
        } finally {
            atom.release(slotId);
        }
    }

    // Gathers the slave's fixed-size columns by the frame's slave row ids, after a TOLERANCE check
    // that turns a row too old into no row.
    private static void gather(
            AsyncAsOfJoinAtom atom,
            WindowJoinTimeFrameHelper helper,
            long outAddress,
            long rowCount,
            PageFrameMemoryRecord record,
            DirectLongList rows,
            boolean isMasterFiltered,
            SqlExecutionCircuitBreaker circuitBreaker
    ) {
        final Record slaveRecord = helper.getRecord();
        final int gatherCount = atom.getGatherCount();
        final long gatherAddress = outAddress + rowCount * Long.BYTES;
        final long tolerance = atom.getToleranceInterval();
        final boolean hasTolerance = tolerance != Numbers.LONG_NULL;
        final int slaveTimestampIndex = helper.getTimestampIndex();
        final int masterTimestampIndex = atom.getMasterTimestampIndex();
        final long masterTsScale = atom.getMasterTsScale();
        final long slaveTsScale = atom.getSlaveTsScale();
        for (long i = 0; i < rowCount; i++) {
            if ((i & (CIRCUIT_BREAKER_CHECK_ROWS - 1)) == 0) {
                circuitBreaker.statefulThrowExceptionIfTripped();
            }
            long rowId = Unsafe.getLong(outAddress + (i << 3));
            if (rowId >= 0) {
                helper.recordAt(rowId);
                if (hasTolerance) {
                    record.setRowIndex(isMasterFiltered ? rows.get(i) : i);
                    final long masterTs = scaleTimestamp(record.getTimestamp(masterTimestampIndex), masterTsScale);
                    final long slaveTs = scaleTimestamp(slaveRecord.getTimestamp(slaveTimestampIndex), slaveTsScale);
                    if (slaveTs < masterTs - tolerance) {
                        rowId = NO_ROW;
                        Unsafe.putLong(outAddress + (i << 3), NO_ROW);
                    }
                }
            }
            if (rowId >= 0) {
                for (int g = 0; g < gatherCount; g++) {
                    final int col = atom.getGatherColumn(g);
                    Unsafe.putLong(
                            gatherAddress + ((g * rowCount + i) << 3),
                            AsyncAsOfJoinAtom.readBits(slaveRecord, col, atom.getGatherKind(col))
                    );
                }
            } else {
                for (int g = 0; g < gatherCount; g++) {
                    Unsafe.putLong(gatherAddress + ((g * rowCount + i) << 3), atom.getGatherNullBits(g));
                }
            }
        }
    }

    private static void join(
            int workerId,
            @NotNull PageFrameMemoryRecord record,
            @NotNull PageFrameReduceTask task,
            @NotNull SqlExecutionCircuitBreaker circuitBreaker,
            @Nullable PageFrameSequence<?> stealingFrameSequence
    ) {
        final long frameRowCount = task.getFrameRowCount();
        assert frameRowCount > 0;
        final AsyncAsOfJoinAtom atom = task.getFrameSequence(AsyncAsOfJoinAtom.class).getAtom();
        final boolean owner = stealingFrameSequence != null && stealingFrameSequence == task.getFrameSequence();
        final int slotId = atom.maybeAcquire(workerId, owner, circuitBreaker);
        try {
            record.init(task.populateFrameMemory());
            final DirectLongList rows = task.getFilteredRows();
            rows.clear();
            task.setFilteredRowCount(frameRowCount);
            if (!atom.isSkipJoin()) {
                joinFrame(atom, slotId, record, rows, false, frameRowCount, circuitBreaker);
            }
        } finally {
            atom.release(slotId);
        }
    }

    // Joins one master page frame: writes the slave row ids, then the gathered slave columns, after
    // the master row indexes (when filtered) in the task's row list.
    private static void joinFrame(
            AsyncAsOfJoinAtom atom,
            int slotId,
            PageFrameMemoryRecord record,
            DirectLongList rows,
            boolean isMasterFiltered,
            long rowCount,
            SqlExecutionCircuitBreaker circuitBreaker
    ) {
        final long outOffset = isMasterFiltered ? rowCount : 0;
        final long totalLongs = outOffset + rowCount * (1 + atom.getGatherCount());
        rows.ensureCapacity(totalLongs - rows.size());
        rows.setPos(totalLongs);
        final long outAddress = rows.getAddress() + (outOffset << 3);
        final WindowJoinTimeFrameHelper helper = atom.getSlaveTimeFrameHelper(slotId);
        boolean done = false;
        if (!atom.isLeanOnly()) {
            try {
                final int walkMode = WALK_MODE;
                if (walkMode != WALK_NEVER && (walkMode == WALK_ALWAYS || atom.isWalkWorthTrying())) {
                    if (joinWalk(atom, slotId, helper, record, rows, isMasterFiltered, rowCount, outAddress, walkMode == WALK_ALWAYS, circuitBreaker)) {
                        atom.recordFrameWalk();
                        done = true;
                    } else {
                        atom.recordWalkAbort();
                    }
                }
                if (!done) {
                    joinSpan(atom, slotId, helper, record, rows, isMasterFiltered, rowCount, outAddress, circuitBreaker);
                    atom.recordFrameSpan();
                    done = true;
                }
            } catch (CairoException e) {
                if (!e.isOutOfMemory()) {
                    throw e;
                }
                // the prevailing cache hit the query's memory limit: this frame joins without it
            }
        }
        if (!done) {
            joinLean(atom, helper, record, rows, isMasterFiltered, rowCount, outAddress, circuitBreaker);
            atom.recordFrameLean();
        }
        gather(atom, helper, outAddress, rowCount, record, rows, isMasterFiltered, circuitBreaker);
    }

    // Lean mode: no per-key state. Per master row, a backward scan from the last slave row at or before
    // its timestamp to the first row of its key, bounded by TOLERANCE. Allocates nothing.
    private static void joinLean(
            AsyncAsOfJoinAtom atom,
            WindowJoinTimeFrameHelper helper,
            PageFrameMemoryRecord record,
            DirectLongList rows,
            boolean isMasterFiltered,
            long rowCount,
            long outAddress,
            SqlExecutionCircuitBreaker circuitBreaker
    ) {
        final Record slaveRecord = helper.getRecord();
        final DirectIntIntHashMap lookupMap = atom.getSlaveSymbolLookupMap();
        final int masterTimestampIndex = atom.getMasterTimestampIndex();
        final int masterSymbolIndex = atom.getMasterSymbolIndex();
        final int slaveSymbolIndex = atom.getSlaveSymbolIndex();
        final int slaveTimestampIndex = helper.getTimestampIndex();
        final long masterTsScale = atom.getMasterTsScale();
        final long slaveTsScale = atom.getSlaveTsScale();
        final long tolerance = atom.getToleranceInterval();
        int scanned = 0;
        for (long i = 0; i < rowCount; i++) {
            record.setRowIndex(isMasterFiltered ? rows.get(i) : i);
            final long masterTs = scaleTimestamp(record.getTimestamp(masterTimestampIndex), masterTsScale);
            final int masterKey = record.getInt(masterSymbolIndex);
            final long minSlaveTs = tolerance != Numbers.LONG_NULL ? masterTs - tolerance : Long.MIN_VALUE;
            long found = NO_ROW;
            // the last slave row at or before masterTs: the row before the first one past it
            helper.findRowLo(masterTs == Long.MAX_VALUE ? masterTs : masterTs + 1, Long.MAX_VALUE, true);
            int frameIndex = helper.getPrevailingFrameIndex();
            long rowIndex = helper.getPrevailingRowIndex();
            if (frameIndex >= 0) {
                helper.openFrame(frameIndex);
                outer:
                do {
                    frameIndex = helper.getTimeFrameIndex();
                    helper.recordAt(frameIndex, 0);
                    final long rowLo = helper.getTimeFrameRowLo();
                    for (long r = Math.min(rowIndex, helper.getTimeFrameRowHi() - 1); r >= rowLo; r--) {
                        if ((scanned++ & (CIRCUIT_BREAKER_CHECK_ROWS - 1)) == 0) {
                            circuitBreaker.statefulThrowExceptionIfTripped();
                        }
                        helper.recordAtRowIndex(r);
                        if (scaleTimestamp(slaveRecord.getTimestamp(slaveTimestampIndex), slaveTsScale) < minSlaveTs) {
                            break outer;
                        }
                        final int slaveKey = slaveRecord.getInt(slaveSymbolIndex);
                        final int matchingMasterKey = lookupMap.get(AsyncWindowJoinFastAtom.toSymbolMapKey(slaveKey));
                        if (matchingMasterKey != StaticSymbolTable.VALUE_NOT_FOUND && matchingMasterKey == masterKey) {
                            found = Rows.toRowID(frameIndex, r);
                            break outer;
                        }
                    }
                    rowIndex = Long.MAX_VALUE;
                } while (helper.previousFrame());
            }
            Unsafe.putLong(outAddress + (i << 3), found);
        }
        // findRowLo() bookmarks positions per master row; the next frame's lookups start afresh
        helper.toTop();
    }

    // Span mode, see the class comment.
    private static void joinSpan(
            AsyncAsOfJoinAtom atom,
            int slotId,
            WindowJoinTimeFrameHelper helper,
            PageFrameMemoryRecord record,
            DirectLongList rows,
            boolean isMasterFiltered,
            long rowCount,
            long outAddress,
            SqlExecutionCircuitBreaker circuitBreaker
    ) {
        final Record slaveRecord = helper.getRecord();
        final PageFrameMemoryRecord slaveFrameRecord = slaveRecord instanceof PageFrameMemoryRecord pfmr ? pfmr : null;
        final int masterTimestampIndex = atom.getMasterTimestampIndex();
        final int masterSymbolIndex = atom.getMasterSymbolIndex();
        final int slaveSymbolIndex = atom.getSlaveSymbolIndex();
        final int slaveTimestampIndex = helper.getTimestampIndex();
        final long masterTsScale = atom.getMasterTsScale();
        final long slaveTsScale = atom.getSlaveTsScale();

        record.setRowIndex(isMasterFiltered ? rows.get(0) : 0);
        final long masterTsLo = scaleTimestamp(record.getTimestamp(masterTimestampIndex), masterTsScale);
        record.setRowIndex(isMasterFiltered ? rows.get(rowCount - 1) : rowCount - 1);
        final long masterTsHi = scaleTimestamp(record.getTimestamp(masterTimestampIndex), masterTsScale);

        long r = helper.findRowLo(masterTsLo, masterTsHi, true);
        final WindowJoinPrevailingCache prevailingCache = atom.getPrevailingCache(slotId);
        prevailingCache.of(helper.getPrevailingFrameIndex(), helper.getPrevailingRowIndex(), circuitBreaker);

        final int epoch = atom.nextEpoch(slotId);
        final long lastRowsAddress = atom.getSpanStateAddress(slotId);
        final int spareSlot = atom.getJoinableCount();
        final long epochsAddress = lastRowsAddress + 16L * atom.getSpanSlotCount();
        final long slaveSlotsAddress = atom.getSlaveSlotsAddress();
        final int slaveSlotCount = atom.getSlaveSlotCount();

        boolean exhausted = r == Long.MIN_VALUE;
        int frameIndex = -1;
        long frameRowHi = 0;
        long tsAddress = 0;
        long keyAddress = 0;
        if (!exhausted) {
            frameIndex = helper.getTimeFrameIndex();
            frameRowHi = helper.getTimeFrameRowHi();
            helper.recordAt(frameIndex, r);
            if (slaveFrameRecord != null) {
                tsAddress = slaveFrameRecord.getPageAddress(slaveTimestampIndex);
                keyAddress = slaveFrameRecord.getPageAddress(slaveSymbolIndex);
            }
        }

        for (long i = 0; i < rowCount; i++) {
            if ((i & (CIRCUIT_BREAKER_CHECK_ROWS - 1)) == 0) {
                circuitBreaker.statefulThrowExceptionIfTripped();
            }
            record.setRowIndex(isMasterFiltered ? rows.get(i) : i);
            final long masterTs = scaleTimestamp(record.getTimestamp(masterTimestampIndex), masterTsScale);
            while (!exhausted) {
                final long slaveTs;
                if (tsAddress != 0) {
                    slaveTs = Unsafe.getLong(tsAddress + (r << 3));
                } else {
                    helper.recordAtRowIndex(r);
                    slaveTs = slaveRecord.getTimestamp(slaveTimestampIndex);
                }
                if (scaleTimestamp(slaveTs, slaveTsScale) > masterTs) {
                    break;
                }
                final int slaveKey;
                if (keyAddress != 0) {
                    slaveKey = Unsafe.getInt(keyAddress + (r << 2));
                } else {
                    helper.recordAtRowIndex(r);
                    slaveKey = slaveRecord.getInt(slaveSymbolIndex);
                }
                // NULL (Integer.MIN_VALUE) at index 0, key k at k + 1; a key that cannot join stores
                // into the spare slot, so that there is no branch on the key
                final int index = Math.max(slaveKey + 1, 0);
                final int slot = index < slaveSlotCount ? Unsafe.getInt(slaveSlotsAddress + ((long) index << 2)) : spareSlot;
                Unsafe.putLong(lastRowsAddress + ((long) slot << 3), Rows.toRowID(frameIndex, r));
                Unsafe.putInt(epochsAddress + ((long) slot << 2), epoch);
                if (++r >= frameRowHi) {
                    if (!helper.nextFrame(masterTsHi)) {
                        exhausted = true;
                    } else {
                        frameIndex = helper.getTimeFrameIndex();
                        frameRowHi = helper.getTimeFrameRowHi();
                        r = helper.getTimeFrameRowLo();
                        helper.recordAt(frameIndex, r);
                        if (slaveFrameRecord != null) {
                            tsAddress = slaveFrameRecord.getPageAddress(slaveTimestampIndex);
                            keyAddress = slaveFrameRecord.getPageAddress(slaveSymbolIndex);
                        }
                    }
                }
            }
            final int slot = atom.masterSlotOf(record.getInt(masterSymbolIndex));
            final long rowId;
            if (slot < 0) {
                rowId = NO_ROW;
            } else if (Unsafe.getInt(epochsAddress + ((long) slot << 2)) == epoch) {
                rowId = Unsafe.getLong(lastRowsAddress + ((long) slot << 3));
            } else {
                rowId = PREVAILING;
            }
            Unsafe.putLong(outAddress + (i << 3), rowId);
        }

        resolvePrevailing(atom, prevailingCache, helper, record, rows, isMasterFiltered, rowCount, outAddress);
    }

    // Walk mode, for a frame with few master rows against a long slave span. Per master row: R, the
    // last slave row at or before it, by a galloping search forward from the previous row's R; then
    // a walk back from R to the key's previous row, no further than an earlier row of the same key
    // already walked back from. Native slave frames only. Gives up (returns false) when it has walked
    // more than half the span rows R has passed: the span scan is then the cheaper of the two.
    private static boolean joinWalk(
            AsyncAsOfJoinAtom atom,
            int slotId,
            WindowJoinTimeFrameHelper helper,
            PageFrameMemoryRecord record,
            DirectLongList rows,
            boolean isMasterFiltered,
            long rowCount,
            long outAddress,
            boolean neverGiveUp,
            SqlExecutionCircuitBreaker circuitBreaker
    ) {
        if (!(helper.getRecord() instanceof PageFrameMemoryRecord slaveFrameRecord)) {
            return false;
        }
        final int masterTimestampIndex = atom.getMasterTimestampIndex();
        final int masterSymbolIndex = atom.getMasterSymbolIndex();
        final int slaveSymbolIndex = atom.getSlaveSymbolIndex();
        final int slaveTimestampIndex = helper.getTimestampIndex();
        final long masterTsScale = atom.getMasterTsScale();
        final long slaveTsScale = atom.getSlaveTsScale();

        record.setRowIndex(isMasterFiltered ? rows.get(0) : 0);
        final long masterTsLo = scaleTimestamp(record.getTimestamp(masterTimestampIndex), masterTsScale);
        record.setRowIndex(isMasterFiltered ? rows.get(rowCount - 1) : rowCount - 1);
        final long masterTsHi = scaleTimestamp(record.getTimestamp(masterTimestampIndex), masterTsScale);

        final long spanRow = helper.findRowLo(masterTsLo, masterTsHi, true);
        if (spanRow == Long.MIN_VALUE) {
            // no slave row in the span: the span scan is just the prevailing lookups
            return false;
        }
        final int spanFrame = helper.getTimeFrameIndex();
        final int prevailingFrameIndex = helper.getPrevailingFrameIndex();
        final long prevailingRowIndex = helper.getPrevailingRowIndex();
        final long[] frames = atom.getWalkFrameCache(slotId);
        final int frameCount = atom.getSlaveFrameCount();
        if (!walkFrame(atom, helper, slaveFrameRecord, frames, spanFrame, slaveTimestampIndex, slaveSymbolIndex)) {
            return false;
        }
        final WindowJoinPrevailingCache prevailingCache = atom.getPrevailingCache(slotId);
        prevailingCache.of(prevailingFrameIndex, prevailingRowIndex, circuitBreaker);

        final int epoch = atom.nextEpoch(slotId);
        final int spanSlotCount = atom.getSpanSlotCount();
        final long lastRowsAddress = atom.getSpanStateAddress(slotId);
        final long walkedToAddress = lastRowsAddress + 8L * spanSlotCount;
        final long epochsAddress = lastRowsAddress + 16L * spanSlotCount;
        final long slaveSlotsAddress = atom.getSlaveSlotsAddress();
        final int slaveSlotCount = atom.getSlaveSlotCount();

        // R: frame and row; one row before the span's first row to start with
        int rFrame = spanFrame;
        long rRow = spanRow - 1;
        long passed = 0;
        long walked = 0;
        for (long i = 0; i < rowCount; i++) {
            if ((i & (CIRCUIT_BREAKER_CHECK_ROWS - 1)) == 0) {
                circuitBreaker.statefulThrowExceptionIfTripped();
            }
            record.setRowIndex(isMasterFiltered ? rows.get(i) : i);
            final long masterTs = scaleTimestamp(record.getTimestamp(masterTimestampIndex), masterTsScale);

            // move R forward to the last slave row at or before masterTs
            for (; ; ) {
                final long frameRows = frames[4 * rFrame] - 1;
                final long tsAddress = frames[4 * rFrame + 1];
                if (rRow + 1 < frameRows) {
                    if (scaleTimestamp(Unsafe.getLong(tsAddress + ((rRow + 1) << 3)), slaveTsScale) > masterTs) {
                        break;
                    }
                    // gallop, then bisect: lo is at or before masterTs, hi past it or the frame's end
                    long lo = rRow + 1;
                    long step = 1;
                    while (lo + step < frameRows && scaleTimestamp(Unsafe.getLong(tsAddress + ((lo + step) << 3)), slaveTsScale) <= masterTs) {
                        lo += step;
                        step <<= 1;
                    }
                    long hi = Math.min(lo + step, frameRows);
                    while (hi - lo > 1) {
                        final long mid = (lo + hi) >>> 1;
                        if (scaleTimestamp(Unsafe.getLong(tsAddress + (mid << 3)), slaveTsScale) <= masterTs) {
                            lo = mid;
                        } else {
                            hi = mid;
                        }
                    }
                    passed += lo - rRow;
                    rRow = lo;
                    if (lo + 1 < frameRows) {
                        break;
                    }
                }
                // the frame ends at or before masterTs: the next non-empty frame, if it starts there too
                int nextFrame = rFrame + 1;
                while (nextFrame < frameCount) {
                    if (!walkFrame(atom, helper, slaveFrameRecord, frames, nextFrame, slaveTimestampIndex, slaveSymbolIndex)) {
                        return false;
                    }
                    if (frames[4 * nextFrame] > 1) {
                        break;
                    }
                    nextFrame++;
                }
                if (nextFrame >= frameCount
                        || scaleTimestamp(Unsafe.getLong(frames[4 * nextFrame + 1]), slaveTsScale) > masterTs) {
                    break;
                }
                rFrame = nextFrame;
                rRow = -1;
            }

            final int slot = atom.masterSlotOf(record.getInt(masterSymbolIndex));
            if (slot < 0) {
                Unsafe.putLong(outAddress + (i << 3), NO_ROW);
                continue;
            }
            final long slotOffset8 = (long) slot << 3;
            if (Unsafe.getInt(epochsAddress + ((long) slot << 2)) != epoch) {
                Unsafe.putInt(epochsAddress + ((long) slot << 2), epoch);
                Unsafe.putLong(lastRowsAddress + slotOffset8, NO_ROW);
                Unsafe.putLong(walkedToAddress + slotOffset8, NO_ROW);
            }
            // R before the span's first row: no span row at or before the master row
            if (rRow >= 0 && (rFrame != spanFrame || rRow >= spanRow)) {
                final long rRowId = Rows.toRowID(rFrame, rRow);
                final long walkedTo = Unsafe.getLong(walkedToAddress + slotOffset8);
                if (rRowId > walkedTo) {
                    // walk back from R over the rows not yet walked for the key
                    long found = NO_ROW;
                    walk:
                    for (int g = rFrame; g >= spanFrame; g--) {
                        final long frameRows = frames[4 * g] - 1;
                        if (frameRows <= 0) {
                            continue;
                        }
                        final long keyAddress = frames[4 * g + 2];
                        final long from = g == rFrame ? rRow : frameRows - 1;
                        final long to = g == spanFrame ? spanRow : 0;
                        for (long x = from; x >= to; x--) {
                            if (Rows.toRowID(g, x) <= walkedTo) {
                                break walk;
                            }
                            walked++;
                            final int index = Math.max(Unsafe.getInt(keyAddress + (x << 2)) + 1, 0);
                            if (index < slaveSlotCount && Unsafe.getInt(slaveSlotsAddress + ((long) index << 2)) == slot) {
                                found = Rows.toRowID(g, x);
                                break walk;
                            }
                        }
                    }
                    if (found != NO_ROW) {
                        Unsafe.putLong(lastRowsAddress + slotOffset8, found);
                    }
                    Unsafe.putLong(walkedToAddress + slotOffset8, rRowId);
                    if (!neverGiveUp && walked > (passed >> 1) + WALK_SLACK_ROWS) {
                        return false;
                    }
                }
            }
            final long last = Unsafe.getLong(lastRowsAddress + slotOffset8);
            Unsafe.putLong(outAddress + (i << 3), last != NO_ROW ? last : PREVAILING);
        }
        resolvePrevailing(atom, prevailingCache, helper, record, rows, isMasterFiltered, rowCount, outAddress);
        return true;
    }

    // keys with no slave row in the span up to their master row: the prevailing row before the span
    private static void resolvePrevailing(
            AsyncAsOfJoinAtom atom,
            WindowJoinPrevailingCache prevailingCache,
            WindowJoinTimeFrameHelper helper,
            PageFrameMemoryRecord record,
            DirectLongList rows,
            boolean isMasterFiltered,
            long rowCount,
            long outAddress
    ) {
        final DirectIntIntHashMap lookupMap = atom.getSlaveSymbolLookupMap();
        final Record slaveRecord = helper.getRecord();
        final int masterSymbolIndex = atom.getMasterSymbolIndex();
        final int slaveSymbolIndex = atom.getSlaveSymbolIndex();
        for (long i = 0; i < rowCount; i++) {
            if (Unsafe.getLong(outAddress + (i << 3)) == PREVAILING) {
                record.setRowIndex(isMasterFiltered ? rows.get(i) : i);
                final long rowId = prevailingCache.findPrevailingSlaveRowId(
                        helper,
                        slaveRecord,
                        slaveSymbolIndex,
                        lookupMap,
                        record.getInt(masterSymbolIndex)
                );
                Unsafe.putLong(outAddress + (i << 3), rowId == Long.MIN_VALUE ? NO_ROW : rowId);
            }
        }
    }

    // Caches a slave time frame's row count (+1, 0 = unknown, -1 = refused) and column addresses for
    // the walk; false when the walk cannot read the frame from memory (Parquet, a column top, a cast).
    private static boolean walkFrame(
            AsyncAsOfJoinAtom atom,
            WindowJoinTimeFrameHelper helper,
            PageFrameMemoryRecord slaveFrameRecord,
            long[] frames,
            int frameIndex,
            int timestampIndex,
            int keyIndex
    ) {
        final long state = frames[4 * frameIndex];
        if (state != 0) {
            return state > 0;
        }
        if (atom.getSlaveFrameFormat(frameIndex) != PartitionFormat.NATIVE) {
            frames[4 * frameIndex] = -1;
            return false;
        }
        final long frameRows = helper.openFrame(frameIndex);
        if (frameRows <= 0) {
            frames[4 * frameIndex] = 1;
            return true;
        }
        helper.recordAt(frameIndex, 0);
        final long tsAddress = slaveFrameRecord.getPageAddress(timestampIndex);
        final long keyAddress = slaveFrameRecord.getPageAddress(keyIndex);
        if (tsAddress == 0 || keyAddress == 0) {
            frames[4 * frameIndex] = -1;
            return false;
        }
        frames[4 * frameIndex + 1] = tsAddress;
        frames[4 * frameIndex + 2] = keyAddress;
        frames[4 * frameIndex] = frameRows + 1;
        return true;
    }

    @Override
    protected void _close() {
        final AsyncAsOfJoinRecordCursor cursor = this.cursor;
        this.cursor = null;
        final PageFrameSequence<AsyncAsOfJoinAtom> frameSequence = this.frameSequence;
        this.frameSequence = null;
        final JoinRecordMetadata joinMetadata = this.joinMetadata;
        this.joinMetadata = null;
        final RecordCursorFactory masterFactory = this.masterFactory;
        this.masterFactory = null;
        final RecordCursorFactory slaveFactory = this.slaveFactory;
        this.slaveFactory = null;

        Throwable failure = Misc.freeBestEffort(null, masterFactory);
        if (slaveFactory != masterFactory) {
            failure = Misc.freeBestEffort(failure, slaveFactory);
        }
        failure = Misc.freeBestEffort(failure, frameSequence);
        failure = Misc.freeBestEffort(failure, cursor);
        failure = Misc.freeBestEffort(failure, joinMetadata);
        CairoException.rethrowCleanupFailure(failure);
    }
}
