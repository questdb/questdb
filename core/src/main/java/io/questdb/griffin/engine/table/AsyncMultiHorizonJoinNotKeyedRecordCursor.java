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
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
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
import io.questdb.std.Misc;
import io.questdb.std.ObjList;

import static io.questdb.cairo.sql.PartitionFrameCursorFactory.ORDER_ASC;

/**
 * Async cursor for not-keyed multi-slave HORIZON JOIN (single output row).
 */
class AsyncMultiHorizonJoinNotKeyedRecordCursor implements NoRandomAccessRecordCursor {
    private final ObjList<GroupByFunction> groupByFunctions;
    private final VirtualRecord recordA;
    private final int slaveCount;
    private final ObjList<RecordCursorFactory> slaveFactories;
    private final ObjList<TablePageFrameCursor> slaveFrameCursors;
    private final ObjList<SymbolTableSource> slaveSources;
    private final ObjList<ConcurrentTimeFrameState> slaveTimeFrameStates;
    private SqlExecutionContext executionContext;
    private UnorderedPageFrameSequence<AsyncMultiHorizonJoinNotKeyedAtom> frameSequence;
    private boolean isExhausted;
    private boolean isOpen;
    private boolean isSlaveTimeFrameCacheBuilt;
    private boolean isValueBuilt;

    public AsyncMultiHorizonJoinNotKeyedRecordCursor(
            ObjList<GroupByFunction> groupByFunctions,
            ObjList<RecordCursorFactory> slaveFactories
    ) {
        try {
            this.isOpen = true;
            this.groupByFunctions = groupByFunctions;
            this.slaveFactories = slaveFactories;
            this.slaveCount = slaveFactories.size();
            this.slaveFrameCursors = new ObjList<>(slaveCount);
            this.slaveFrameCursors.setPos(slaveCount);
            this.slaveSources = new ObjList<>(slaveCount);
            this.slaveSources.setPos(slaveCount);
            this.recordA = new VirtualRecord(groupByFunctions);

            this.slaveTimeFrameStates = new ObjList<>(slaveCount);
            for (int s = 0; s < slaveCount; s++) {
                slaveTimeFrameStates.add(new ConcurrentTimeFrameState());
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
                Misc.freeObjListAndKeepObjects(slaveFrameCursors);
                Misc.freeObjListAndKeepObjects(slaveTimeFrameStates);
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

            for (int s = 0; s < slaveCount; s++) {
                TablePageFrameCursor cursor = slaveFrameCursors.getQuick(s);
                slaveTimeFrameStates.getQuick(s).of(
                        cursor,
                        slaveFactories.getQuick(s).getMetadata(),
                        cursor.getColumnMapping(),
                        cursor.isExternal(),
                        executionContext.getPageFrameMinRows(),
                        executionContext.getPageFrameMaxRows(),
                        executionContext.getSharedQueryWorkerCount()
                );
                try {
                    atom.initSlaveTimeFrameCursors(
                            s,
                            masterSource,
                            cursor,
                            slaveTimeFrameStates.getQuick(s)
                    );
                } catch (SqlException e) {
                    throw CairoException.nonCritical().put(e.getFlyweightMessage());
                }
                slaveSources.setQuick(s, cursor);
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

    void of(UnorderedPageFrameSequence<AsyncMultiHorizonJoinNotKeyedAtom> frameSequence, SqlExecutionContext executionContext) throws SqlException {
        final AsyncMultiHorizonJoinNotKeyedAtom atom = frameSequence.getAtom();
        if (!isOpen) {
            isOpen = true;
            atom.reopen();
        }
        this.frameSequence = frameSequence;
        this.executionContext = executionContext;

        try {
            for (int s = 0; s < slaveCount; s++) {
                slaveFrameCursors.setQuick(s, (TablePageFrameCursor) slaveFactories.getQuick(s).getPageFrameCursor(executionContext, ORDER_ASC));
            }

            // Initialize symbol table source with master and all slave sources
            final MultiHorizonJoinSymbolTableSource symbolTableSource = atom.getSymbolTableSource();
            for (int s = 0; s < slaveCount; s++) {
                slaveSources.setQuick(s, slaveFrameCursors.getQuick(s));
            }
            symbolTableSource.of(frameSequence.getSymbolTableSource(), slaveSources);

            recordA.of(atom.getOwnerMapValue());
            Function.init(groupByFunctions, symbolTableSource, executionContext, null);
        } catch (Throwable th) {
            Misc.freeObjList(slaveFrameCursors);
            throw th;
        }

        isValueBuilt = false;
        isSlaveTimeFrameCacheBuilt = false;
        toTop();
    }
}
