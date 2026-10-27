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

import io.questdb.cairo.ArrayColumnTypes;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ListColumnFilter;
import io.questdb.cairo.RecordChain;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.StatefulAtom;
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.cairo.sql.async.PageFrameReduceTask;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.PerWorkerLockOwner;
import io.questdb.griffin.engine.PerWorkerLocks;
import io.questdb.griffin.engine.functions.GroupByFunction;
import io.questdb.griffin.engine.functions.PerWorkerFunctionList;
import io.questdb.griffin.engine.groupby.FlyweightPackedMapValue;
import io.questdb.griffin.engine.groupby.GroupByFunctionsUpdater;
import io.questdb.griffin.engine.groupby.GroupByFunctionsUpdaterFactory;
import io.questdb.griffin.engine.groupby.GroupByUtils;
import io.questdb.griffin.engine.groupby.StreamGroupByMap;
import io.questdb.std.BytecodeAssembler;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;

public class AsyncStreamGroupByAtom implements StatefulAtom, PerWorkerLockOwner {
    private final int[] batchColumnIndexes;
    private final StreamGroupByMap collectorMap;
    private final CairoConfiguration configuration;
    private final ObjList<GroupByFunction> groupByFunctions;
    private final Function keyFunction;
    private final int keyFunctionIndex;
    private final ObjList<Function> keyFunctions;
    private final PerWorkerLocks locks;
    private final StreamGroupByMap ownerMap;
    private final ObjList<ObjList<GroupByFunction>> perWorkerFunctions;
    private final ObjList<ObjList<Function>> perWorkerKeys;
    private final ObjList<StreamGroupByMap> perWorkerMaps;
    private final ObjList<GroupByFunctionsUpdater> perWorkerUpdaters;
    private final ObjList<FlyweightPackedMapValue> perWorkerValues;
    private final ArrayColumnTypes stateTypes;
    private final GroupByFunctionsUpdater updater;
    private final FlyweightPackedMapValue value;

    public AsyncStreamGroupByAtom(
            CairoConfiguration configuration,
            BytecodeAssembler asm,
            RecordMetadata baseMetadata,
            ListColumnFilter columnFilter,
            ArrayColumnTypes keyTypes,
            ArrayColumnTypes valueTypes,
            int streamKeyIndex,
            Function keyFunction,
            ObjList<Function> keyFunctions,
            ObjList<ObjList<Function>> perWorkerKeys,
            ObjList<GroupByFunction> groupByFunctions,
            ObjList<ObjList<GroupByFunction>> perWorkerFunctions,
            int workerCount
    ) {
        this.configuration = configuration;
        this.keyFunction = keyFunction;
        this.keyFunctionIndex = streamKeyIndex - columnFilter.getColumnCount();
        this.keyFunctions = keyFunctions;
        this.perWorkerKeys = perWorkerKeys;
        this.groupByFunctions = groupByFunctions;
        this.perWorkerFunctions = perWorkerFunctions;
        try {
            stateTypes = new ArrayColumnTypes().addAll(valueTypes);
            stateTypes.add(keyFunction.getType());
            batchColumnIndexes = new int[groupByFunctions.size()];
            for (int i = 0; i < batchColumnIndexes.length; i++) {
                batchColumnIndexes[i] = AsyncGroupByNotKeyedRecordCursorFactory.computeBatchColumnIndex(groupByFunctions.getQuick(i), baseMetadata);
            }
            locks = new PerWorkerLocks(configuration, workerCount);
            final Class<? extends GroupByFunctionsUpdater> updaterClass = GroupByFunctionsUpdaterFactory.getInstanceClass(asm, groupByFunctions.size());
            updater = GroupByFunctionsUpdaterFactory.getInstance(updaterClass, groupByFunctions);
            value = new FlyweightPackedMapValue(stateTypes);
            perWorkerValues = new ObjList<>(workerCount);
            perWorkerUpdaters = new ObjList<>(workerCount);
            for (int i = 0; i < workerCount; i++) {
                perWorkerValues.add(new FlyweightPackedMapValue(stateTypes));
                perWorkerUpdaters.add(perWorkerFunctions == null ? updater
                        : GroupByFunctionsUpdaterFactory.getInstance(updaterClass, perWorkerFunctions.getQuick(i)));
            }
            if (keyTypes.getColumnCount() > 1) {
                ownerMap = new StreamGroupByMap(configuration, asm, baseMetadata, columnFilter, keyTypes, valueTypes,
                        streamKeyIndex, keyFunctions, groupByFunctions, updater);
                collectorMap = new StreamGroupByMap(configuration, asm, baseMetadata, columnFilter, keyTypes, valueTypes,
                        streamKeyIndex, keyFunctions, groupByFunctions, updater);
                perWorkerMaps = new ObjList<>(workerCount);
                for (int i = 0; i < workerCount; i++) {
                    perWorkerMaps.add(new StreamGroupByMap(configuration, asm, baseMetadata, columnFilter, keyTypes, valueTypes,
                            streamKeyIndex, perWorkerKeys == null ? keyFunctions : perWorkerKeys.getQuick(i),
                            getFunctions(i), getUpdater(i)));
                }
            } else {
                ownerMap = collectorMap = null;
                perWorkerMaps = null;
            }
        } catch (Throwable th) {
            Misc.free(this, th);
            throw th;
        }
    }

    @Override
    public void clear() {
        CairoException.rethrowCleanupFailure(closeMaps(null));
        cursorClosed(groupByFunctions);
        cursorClosed(keyFunctions);
        Misc.clearObjList(groupByFunctions);
        if (perWorkerFunctions != null) {
            for (int i = 0, n = perWorkerFunctions.size(); i < n; i++) {
                cursorClosed(perWorkerFunctions.getQuick(i));
                PerWorkerFunctionList.clear(perWorkerFunctions.getQuick(i));
            }
        }
        if (perWorkerKeys != null) {
            for (int i = 0, n = perWorkerKeys.size(); i < n; i++) {
                cursorClosed(perWorkerKeys.getQuick(i));
            }
        }
    }

    @Override
    public void close() {
        Throwable th = Misc.freeObjListBestEffort(null, keyFunctions);
        th = closeMaps(th);
        th = AsyncGroupByAtom.closePerWorkerFunctions(th, perWorkerKeys);
        th = AsyncGroupByAtom.closePerWorkerFunctions(th, perWorkerFunctions);
        CairoException.rethrowCleanupFailure(th);
    }

    public int[] getBatchColumnIndexes() {
        return batchColumnIndexes;
    }

    public StreamGroupByMap getCollectorMap() {
        return collectorMap;
    }

    public ObjList<GroupByFunction> getFunctions(int slot) {
        return slot < 0 || perWorkerFunctions == null ? groupByFunctions : perWorkerFunctions.getQuick(slot);
    }

    public Function getKey(int slot) {
        return slot < 0 || perWorkerKeys == null || keyFunctionIndex < 0
                ? keyFunction : perWorkerKeys.getQuick(slot).getQuick(keyFunctionIndex);
    }

    public ObjList<Function> getKeyFunctions() {
        return keyFunctions;
    }

    public StreamGroupByMap getMap(int slot) {
        return slot < 0 ? ownerMap : perWorkerMaps.getQuick(slot);
    }

    @Override
    public PerWorkerLocks getPerWorkerLocks() {
        return locks;
    }

    public RecordChain getResultRecords(PageFrameReduceTask task) {
        return task.getGroupByRecords(ownerMap.getStateTypes(), configuration.getSqlSmallMapPageSize());
    }

    public ArrayColumnTypes getStateTypes() {
        return stateTypes;
    }

    public GroupByFunctionsUpdater getUpdater(int slot) {
        return slot < 0 ? updater : perWorkerUpdaters.getQuick(slot);
    }

    public FlyweightPackedMapValue getValue(int slot) {
        return slot < 0 ? value : perWorkerValues.getQuick(slot);
    }

    @Override
    public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
        Function.init(groupByFunctions, symbolTableSource, executionContext, null);
        Function.init(keyFunctions, symbolTableSource, executionContext, null);
        final boolean cloneSymbolTables = executionContext.getCloneSymbolTables();
        executionContext.setCloneSymbolTables(true);
        try {
            if (perWorkerFunctions != null) {
                for (int i = 0, n = perWorkerFunctions.size(); i < n; i++) {
                    PerWorkerFunctionList.init(perWorkerFunctions.getQuick(i), groupByFunctions, symbolTableSource, executionContext);
                }
            }
            if (perWorkerKeys != null) {
                for (int i = 0, n = perWorkerKeys.size(); i < n; i++) {
                    PerWorkerFunctionList.init(perWorkerKeys.getQuick(i), keyFunctions, symbolTableSource, executionContext);
                }
            }
        } finally {
            executionContext.setCloneSymbolTables(cloneSymbolTables);
        }
        if (ownerMap != null) {
            try {
                ownerMap.reopen(executionContext.getMemoryTracker());
                collectorMap.reopen(executionContext.getMemoryTracker());
                for (int i = 0, n = perWorkerMaps.size(); i < n; i++) {
                    perWorkerMaps.getQuick(i).reopen(executionContext.getMemoryTracker());
                }
            } catch (Throwable th) {
                closeMaps(th);
                throw th;
            }
        }
    }

    public int maybeAcquire(int workerId, boolean owner, SqlExecutionCircuitBreaker circuitBreaker) {
        return workerId == -1 && owner ? -1 : locks.acquireSlot(workerId, circuitBreaker);
    }

    public void release(int slot) {
        locks.releaseSlot(slot);
    }

    public void toTop() {
        GroupByUtils.toTop(groupByFunctions);
        GroupByUtils.toTop(keyFunctions);
        if (perWorkerFunctions != null) {
            for (int i = 0, n = perWorkerFunctions.size(); i < n; i++) {
                GroupByUtils.toTop(perWorkerFunctions.getQuick(i));
            }
        }
        if (perWorkerKeys != null) {
            for (int i = 0, n = perWorkerKeys.size(); i < n; i++) {
                GroupByUtils.toTop(perWorkerKeys.getQuick(i));
            }
        }
    }

    private Throwable closeMaps(Throwable th) {
        th = Misc.freeBestEffort(th, ownerMap);
        th = Misc.freeBestEffort(th, collectorMap);
        if (perWorkerMaps != null) {
            for (int i = 0, n = perWorkerMaps.size(); i < n; i++) {
                th = Misc.freeBestEffort(th, perWorkerMaps.getQuick(i));
            }
        }
        return th;
    }

    private static void cursorClosed(ObjList<? extends Function> functions) {
        for (int i = 0, n = functions.size(); i < n; i++) {
            if (PerWorkerFunctionList.isOwned(functions, i)) {
                functions.getQuick(i).cursorClosed();
            }
        }
    }
}
