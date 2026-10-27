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
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.StatefulAtom;
import io.questdb.cairo.sql.SymbolTableSource;
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
import io.questdb.std.BytecodeAssembler;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;

public class AsyncStreamGroupByAtom implements StatefulAtom, PerWorkerLockOwner {
    private final int[] batchColumnIndexes;
    private final ObjList<GroupByFunction> groupByFunctions;
    private final Function keyFunction;
    private final ObjList<Function> keyFunctions;
    private final PerWorkerLocks locks;
    private final ObjList<ObjList<GroupByFunction>> perWorkerFunctions;
    private final ObjList<ObjList<Function>> perWorkerKeys;
    private final ObjList<GroupByFunctionsUpdater> perWorkerUpdaters;
    private final ObjList<FlyweightPackedMapValue> perWorkerValues;
    private final ArrayColumnTypes stateTypes;
    private final GroupByFunctionsUpdater updater;
    private final FlyweightPackedMapValue value;

    public AsyncStreamGroupByAtom(
            CairoConfiguration configuration,
            BytecodeAssembler asm,
            RecordMetadata baseMetadata,
            ArrayColumnTypes valueTypes,
            Function keyFunction,
            ObjList<Function> keyFunctions,
            ObjList<ObjList<Function>> perWorkerKeys,
            ObjList<GroupByFunction> groupByFunctions,
            ObjList<ObjList<GroupByFunction>> perWorkerFunctions,
            int workerCount
    ) {
        this.keyFunction = keyFunction;
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
        } catch (Throwable th) {
            Misc.free(this, th);
            throw th;
        }
    }

    @Override
    public void clear() {
        Misc.clearObjList(groupByFunctions);
        if (perWorkerFunctions != null) {
            for (int i = 0, n = perWorkerFunctions.size(); i < n; i++) {
                PerWorkerFunctionList.clear(perWorkerFunctions.getQuick(i));
            }
        }
    }

    @Override
    public void close() {
        Throwable th = Misc.freeObjListBestEffort(null, keyFunctions);
        th = AsyncGroupByAtom.closePerWorkerFunctions(th, perWorkerKeys);
        th = AsyncGroupByAtom.closePerWorkerFunctions(th, perWorkerFunctions);
        CairoException.rethrowCleanupFailure(th);
    }

    public int[] getBatchColumnIndexes() {
        return batchColumnIndexes;
    }

    public ObjList<GroupByFunction> getFunctions(int slot) {
        return slot < 0 || perWorkerFunctions == null ? groupByFunctions : perWorkerFunctions.getQuick(slot);
    }

    public Function getKey(int slot) {
        return slot < 0 || perWorkerKeys == null || keyFunctions.size() == 0
                ? keyFunction : perWorkerKeys.getQuick(slot).getQuick(0);
    }

    public ObjList<Function> getKeyFunctions() {
        return keyFunctions;
    }

    @Override
    public PerWorkerLocks getPerWorkerLocks() {
        return locks;
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
    }

    public int maybeAcquire(int workerId, boolean owner, SqlExecutionCircuitBreaker circuitBreaker) {
        return workerId == -1 && owner ? -1 : locks.acquireSlot(workerId, circuitBreaker);
    }

    public void release(int slot) {
        locks.releaseSlot(slot);
    }

    public void toTop() {
        GroupByUtils.toTop(groupByFunctions);
        keyFunction.toTop();
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
}
