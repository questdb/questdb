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
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.cairo.sql.VirtualRecord;
import io.questdb.cairo.sql.async.UnorderedPageFrameSequence;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.GroupByFunction;
import io.questdb.griffin.engine.functions.SymbolFunction;
import io.questdb.griffin.engine.groupby.GroupByFunctionsUpdater;
import io.questdb.griffin.engine.groupby.GroupByUtils;
import io.questdb.griffin.engine.groupby.SimpleMapValue;
import io.questdb.std.DirectIntList;
import io.questdb.std.LongList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;

import static io.questdb.cairo.sql.PartitionFrameCursorFactory.ORDER_ASC;
import static io.questdb.griffin.engine.table.ConcurrentTimeFrameCursor.populatePartitionTimestamps;

/**
 * Async cursor for not-keyed multi-slave HORIZON JOIN (single output row).
 */
class AsyncMultiHorizonJoinNotKeyedRecordCursor implements NoRandomAccessRecordCursor {
    private final ObjList<GroupByFunction> groupByFunctions;
    private final VirtualRecord recordA;
    private final int slaveCount;
    private final RecordCursorFactory[] slaveFactories;
    private final PageFrameAddressCache[] slaveTimeFrameAddressCaches;
    private final DirectIntList[] slaveTimeFramePartitionIndexes;
    private final LongList[] slaveTimeFrameRowCounts;
    private final LongList[] slavePartitionTimestamps;
    private final LongList[] slavePartitionCeilings;
    private SqlExecutionContext executionContext;
    private UnorderedPageFrameSequence<AsyncMultiHorizonJoinNotKeyedAtom> frameSequence;
    private boolean isExhausted;
    private boolean isOpen;
    private boolean isSlaveTimeFrameCacheBuilt;
    private boolean isValueBuilt;
    private TablePageFrameCursor[] slaveFrameCursors;

    public AsyncMultiHorizonJoinNotKeyedRecordCursor(
            ObjList<GroupByFunction> groupByFunctions,
            RecordCursorFactory[] slaveFactories
    ) {
        try {
            this.isOpen = true;
            this.groupByFunctions = groupByFunctions;
            this.slaveFactories = slaveFactories;
            this.slaveCount = slaveFactories.length;
            this.recordA = new VirtualRecord(groupByFunctions);

            this.slaveTimeFrameAddressCaches = new PageFrameAddressCache[slaveCount];
            this.slaveTimeFramePartitionIndexes = new DirectIntList[slaveCount];
            this.slaveTimeFrameRowCounts = new LongList[slaveCount];
            this.slavePartitionTimestamps = new LongList[slaveCount];
            this.slavePartitionCeilings = new LongList[slaveCount];
            for (int s = 0; s < slaveCount; s++) {
                slaveTimeFrameAddressCaches[s] = new PageFrameAddressCache();
                slaveTimeFramePartitionIndexes[s] = new DirectIntList(64, MemoryTag.NATIVE_DEFAULT, true);
                slaveTimeFrameRowCounts[s] = new LongList();
                slavePartitionTimestamps[s] = new LongList();
                slavePartitionCeilings[s] = new LongList();
            }
        } catch (Throwable th) {
            close();
            throw th;
        }
    }

    @Override
    public void calculateSize(SqlExecutionCircuitBreaker circuitBreaker, Counter counter) {
        if (!isExhausted) {
            counter.inc();
            isExhausted = true;
        }
    }

    @Override
    public void close() {
        if (isOpen) {
            try {
                if (frameSequence != null) {
                    frameSequence.await();
                    frameSequence.reset();
                }
            } finally {
                Misc.clearObjList(groupByFunctions);
                if (slaveFrameCursors != null) {
                    for (int s = 0; s < slaveCount; s++) {
                        slaveFrameCursors[s] = Misc.free(slaveFrameCursors[s]);
                    }
                }
                for (int s = 0; s < slaveCount; s++) {
                    Misc.free(slaveTimeFrameAddressCaches[s]);
                    Misc.free(slaveTimeFramePartitionIndexes[s]);
                }
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
        if (isExhausted) {
            return false;
        }
        if (!isValueBuilt) {
            buildSlaveTimeFrameCacheConditionally();
            buildValue();
        }
        isExhausted = true;
        return true;
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
        isExhausted = false;
        GroupByUtils.toTop(groupByFunctions);
        if (frameSequence != null) {
            frameSequence.getAtom().toTop();
        }
    }

    private void buildSlaveTimeFrameCacheConditionally() {
        if (!isSlaveTimeFrameCacheBuilt) {
            final AsyncMultiHorizonJoinNotKeyedAtom atom = frameSequence.getAtom();
            final SymbolTableSource masterSource = frameSequence.getSymbolTableSource();
            final SymbolTableSource[] slaveSources = new SymbolTableSource[slaveCount];

            for (int s = 0; s < slaveCount; s++) {
                int frameCount = initializeSlaveTimeFrameCache(s);
                populatePartitionTimestamps(slaveFrameCursors[s], slavePartitionTimestamps[s], slavePartitionCeilings[s]);
                try {
                    atom.initSlaveTimeFrameCursors(
                            s, executionContext, masterSource,
                            slaveFrameCursors[s],
                            slaveTimeFrameAddressCaches[s],
                            slaveTimeFramePartitionIndexes[s],
                            slaveTimeFrameRowCounts[s],
                            slavePartitionTimestamps[s],
                            slavePartitionCeilings[s],
                            frameCount
                    );
                } catch (SqlException e) {
                    throw CairoException.nonCritical().put(e.getFlyweightMessage());
                }
                slaveSources[s] = slaveFrameCursors[s];
            }

            try {
                atom.initGroupByFunctions(executionContext, masterSource, slaveSources);
            } catch (SqlException e) {
                throw CairoException.nonCritical().put(e.getFlyweightMessage());
            }

            isSlaveTimeFrameCacheBuilt = true;
        }
    }

    private void buildValue() {
        frameSequence.prepareForDispatch();
        frameSequence.getAtom().getFilterContext().initMemoryPools(frameSequence.getPageFrameAddressCache());
        frameSequence.dispatchAndAwait();

        final AsyncMultiHorizonJoinNotKeyedAtom atom = frameSequence.getAtom();
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

    private int initializeSlaveTimeFrameCache(int slaveIndex) {
        RecordMetadata slaveMetadata = slaveFactories[slaveIndex].getMetadata();
        TablePageFrameCursor cursor = slaveFrameCursors[slaveIndex];
        PageFrameAddressCache cache = slaveTimeFrameAddressCaches[slaveIndex];
        DirectIntList partIndexes = slaveTimeFramePartitionIndexes[slaveIndex];
        LongList rowCounts = slaveTimeFrameRowCounts[slaveIndex];

        cache.of(slaveMetadata, cursor.getColumnIndexes(), cursor.isExternal());
        partIndexes.reopen();
        partIndexes.clear();
        rowCounts.clear();

        int frameCount = 0;
        PageFrame frame;
        while ((frame = cursor.next()) != null) {
            partIndexes.add(frame.getPartitionIndex());
            rowCounts.add(frame.getPartitionHi() - frame.getPartitionLo());
            cache.add(frameCount++, frame);
        }
        return frameCount;
    }

    void of(UnorderedPageFrameSequence<AsyncMultiHorizonJoinNotKeyedAtom> frameSequence, SqlExecutionContext executionContext) throws SqlException {
        final AsyncMultiHorizonJoinNotKeyedAtom atom = frameSequence.getAtom();
        if (!isOpen) {
            isOpen = true;
            atom.reopen();
        }
        this.frameSequence = frameSequence;
        this.executionContext = executionContext;

        this.slaveFrameCursors = new TablePageFrameCursor[slaveCount];
        for (int s = 0; s < slaveCount; s++) {
            slaveFrameCursors[s] = (TablePageFrameCursor) slaveFactories[s].getPageFrameCursor(executionContext, ORDER_ASC);
        }

        recordA.of(atom.getOwnerMapValue());
        Function.init(groupByFunctions, atom.getSymbolTableSource(), executionContext, null);

        isValueBuilt = false;
        isSlaveTimeFrameCacheBuilt = false;
        toTop();
    }
}
