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

package io.questdb.griffin.engine.table;

import io.questdb.cairo.CairoException;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.NoRandomAccessRecordCursor;
import io.questdb.cairo.sql.PageFrame;
import io.questdb.cairo.sql.PageFrameAddressCache;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.cairo.sql.VirtualRecord;
import io.questdb.cairo.sql.async.PageFrameReduceTask;
import io.questdb.cairo.sql.async.PageFrameSequence;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.GroupByFunction;
import io.questdb.griffin.engine.functions.SymbolFunction;
import io.questdb.griffin.engine.groupby.GroupByFunctionsUpdater;
import io.questdb.griffin.engine.groupby.GroupByUtils;
import io.questdb.griffin.engine.groupby.SimpleMapValue;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.IntList;
import io.questdb.std.LongList;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.Os;

import static io.questdb.cairo.sql.PartitionFrameCursorFactory.ORDER_ASC;

/**
 * Cursor for parallel non-keyed markout GROUP BY using PageFrameSequence.
 * <p>
 * Produces a single output row by collecting and merging SimpleMapValue instances from all workers.
 */
class AsyncHorizonJoinNotKeyedRecordCursor implements NoRandomAccessRecordCursor {
    private static final Log LOG = LogFactory.getLog(AsyncHorizonJoinNotKeyedRecordCursor.class);

    private final ObjList<GroupByFunction> groupByFunctions;
    private final VirtualRecord recordA;
    private final RecordCursorFactory slaveFactory;
    private final LongList slavePartitionTimestamps = new LongList();
    // Slave time frame cache data
    private final PageFrameAddressCache slaveTimeFrameAddressCache;
    private final IntList slaveTimeFramePartitionIndexes = new IntList();
    private final LongList slaveTimeFrameRowCounts = new LongList();
    private SqlExecutionContext executionContext;
    private int frameLimit;
    private PageFrameSequence<AsyncHorizonJoinNotKeyedAtom> frameSequence;
    private boolean isOpen;
    private boolean isSlaveTimeFrameCacheBuilt;
    private boolean isValueBuilt;
    private int recordsRemaining = 1;
    private TablePageFrameCursor slavePageFrameCursor;

    public AsyncHorizonJoinNotKeyedRecordCursor(
            ObjList<GroupByFunction> groupByFunctions,
            RecordCursorFactory slaveFactory
    ) {
        this.groupByFunctions = groupByFunctions;
        this.slaveFactory = slaveFactory;
        this.recordA = new VirtualRecord(groupByFunctions);
        this.slaveTimeFrameAddressCache = new PageFrameAddressCache();
        this.isOpen = true;
    }

    @Override
    public void calculateSize(SqlExecutionCircuitBreaker circuitBreaker, Counter counter) {
        if (recordsRemaining > 0) {
            counter.add(recordsRemaining);
            recordsRemaining = 0;
        }
    }

    @Override
    public void close() {
        if (isOpen) {
            try {
                if (frameSequence != null) {
                    LOG.debug()
                            .$("closing [shard=").$(frameSequence.getShard())
                            .$(", frameCount=").$(frameLimit)
                            .I$();

                    if (frameLimit > -1) {
                        frameSequence.await();
                    }
                    frameSequence.reset();
                }
            } finally {
                // Free shared resources only after workers have finished
                Misc.clearObjList(groupByFunctions);
                slavePageFrameCursor = Misc.free(slavePageFrameCursor);
                Misc.free(slaveTimeFrameAddressCache);
                isOpen = false;
            }
        }
    }

    @Override
    public Record getRecord() {
        return recordA;
    }

    @Override
    public SymbolTable getSymbolTable(int columnIndex) {
        return (SymbolTable) groupByFunctions.getQuick(columnIndex);
    }

    @Override
    public boolean hasNext() {
        buildSlaveTimeFrameCacheConditionally();
        if (!isValueBuilt) {
            buildValue();
        }
        return recordsRemaining-- > 0;
    }

    @Override
    public SymbolTable newSymbolTable(int columnIndex) {
        return ((SymbolFunction) groupByFunctions.getQuick(columnIndex)).newSymbolTable();
    }

    @Override
    public long preComputedStateSize() {
        return RecordCursor.fromBool(isValueBuilt);
    }

    @Override
    public long size() {
        return 1;
    }

    @Override
    public void toTop() {
        recordsRemaining = 1;
        GroupByUtils.toTop(groupByFunctions);
        if (frameSequence != null) {
            frameSequence.getAtom().toTop();
        }
    }

    private void buildSlaveTimeFrameCacheConditionally() {
        if (!isSlaveTimeFrameCacheBuilt) {
            final int frameCount = initializeSlaveTimeFrameCache();
            populateSlavePartitionTimestamps();
            initializeTimeFrameCursors(frameCount);
            isSlaveTimeFrameCacheBuilt = true;
        }
    }

    private void buildValue() {
        if (frameLimit == -1) {
            frameSequence.prepareForDispatch();
            frameLimit = frameSequence.getFrameCount() - 1;
        }

        int frameIndex = -1;
        boolean allFramesActive = true;
        try {
            do {
                final long cursor = frameSequence.next();
                if (cursor > -1) {
                    PageFrameReduceTask task = frameSequence.getTask(cursor);
                    LOG.debug()
                            .$("collected [shard=").$(frameSequence.getShard())
                            .$(", frameIndex=").$(task.getFrameIndex())
                            .$(", frameCount=").$(frameSequence.getFrameCount())
                            .$(", active=").$(frameSequence.isActive())
                            .$(", cursor=").$(cursor)
                            .I$();
                    if (task.hasError()) {
                        throw CairoException.nonCritical()
                                .position(task.getErrorMessagePosition())
                                .put(task.getErrorMsg())
                                .setCancellation(task.isCancelled())
                                .setInterruption(task.isCancelled())
                                .setOutOfMemory(task.isOutOfMemory());
                    }

                    allFramesActive &= frameSequence.isActive();
                    frameIndex = task.getFrameIndex();

                    frameSequence.collect(cursor, false);
                } else if (cursor == -2) {
                    break; // No frames to process
                } else {
                    Os.pause();
                }
            } while (frameIndex < frameLimit);
        } catch (Throwable e) {
            LOG.error().$("horizon join error [ex=").$(e).I$();
            if (e instanceof CairoException) {
                CairoException ce = (CairoException) e;
                if (ce.isInterruption()) {
                    throwTimeoutException();
                } else {
                    throw ce;
                }
            }
            throw CairoException.nonCritical().put(e.getMessage());
        }

        if (!allFramesActive) {
            throwTimeoutException();
        }

        // Merge all per-worker values into the owner value
        final AsyncHorizonJoinNotKeyedAtom atom = frameSequence.getAtom();
        final GroupByFunctionsUpdater functionUpdater = atom.getFunctionUpdater(-1);
        final SimpleMapValue destValue = atom.getOwnerMapValue();
        for (int i = 0, n = atom.getPerWorkerMapValues().size(); i < n; i++) {
            final SimpleMapValue srcValue = atom.getPerWorkerMapValues().getQuick(i);
            if (srcValue.isNew()) {
                continue;
            }

            if (destValue.isNew()) {
                destValue.copy(srcValue);
            } else {
                functionUpdater.merge(destValue, srcValue);
            }
            destValue.setNew(false);
        }

        isValueBuilt = true;
    }

    private int initializeSlaveTimeFrameCache() {
        RecordMetadata slaveMetadata = slaveFactory.getMetadata();
        slaveTimeFrameAddressCache.of(slaveMetadata, slavePageFrameCursor.getColumnIndexes(), slavePageFrameCursor.isExternal());
        slaveTimeFramePartitionIndexes.clear();
        slaveTimeFrameRowCounts.clear();

        int frameCount = 0;
        PageFrame frame;
        while ((frame = slavePageFrameCursor.next()) != null) {
            slaveTimeFramePartitionIndexes.add(frame.getPartitionIndex());
            slaveTimeFrameRowCounts.add(frame.getPartitionHi() - frame.getPartitionLo());
            slaveTimeFrameAddressCache.add(frameCount++, frame);
        }
        return frameCount;
    }

    private void initializeTimeFrameCursors(int frameCount) {
        try {
            frameSequence.getAtom().initTimeFrameCursors(
                    executionContext,
                    frameSequence.getSymbolTableSource(),
                    slavePageFrameCursor,
                    slaveTimeFrameAddressCache,
                    slaveTimeFramePartitionIndexes,
                    slaveTimeFrameRowCounts,
                    slavePartitionTimestamps,
                    frameCount
            );
        } catch (SqlException e) {
            throw CairoException.nonCritical().put(e.getFlyweightMessage());
        }
    }

    private void populateSlavePartitionTimestamps() {
        slavePartitionTimestamps.clear();
        final TableReader reader = slavePageFrameCursor.getTableReader();
        for (int i = 0, n = reader.getPartitionCount(); i < n; i++) {
            slavePartitionTimestamps.add(reader.getPartitionTimestampByIndex(i));
        }
    }

    private void throwTimeoutException() {
        if (frameSequence.getCancelReason() == SqlExecutionCircuitBreaker.STATE_CANCELLED) {
            throw CairoException.queryCancelled();
        } else {
            throw CairoException.queryTimedOut();
        }
    }

    void of(PageFrameSequence<AsyncHorizonJoinNotKeyedAtom> frameSequence, SqlExecutionContext executionContext) throws SqlException {
        final AsyncHorizonJoinNotKeyedAtom atom = frameSequence.getAtom();
        if (!isOpen) {
            isOpen = true;
            atom.reopen();
        }
        this.frameSequence = frameSequence;
        this.executionContext = executionContext;

        // Get slave page frame cursor for time frame initialization
        this.slavePageFrameCursor = (TablePageFrameCursor) slaveFactory.getPageFrameCursor(executionContext, ORDER_ASC);

        // Initialize record functions with a symbol table source that routes lookups
        // to the correct source (master or slave) based on column mappings
        final MarkoutSymbolTableSource symbolTableSource = atom.getMarkoutSymbolTableSource();
        symbolTableSource.of(frameSequence.getSymbolTableSource(), slavePageFrameCursor);

        // Initialize record with the owner's map value
        recordA.of(atom.getOwnerMapValue());
        Function.init(groupByFunctions, symbolTableSource, executionContext, null);

        isValueBuilt = false;
        isSlaveTimeFrameCacheBuilt = false;
        frameLimit = -1;
        toTop();
    }
}
