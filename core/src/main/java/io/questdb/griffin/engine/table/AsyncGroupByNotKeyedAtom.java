/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.StatefulAtom;
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.Plannable;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.PerWorkerLocks;
import io.questdb.griffin.engine.functions.GroupByFunction;
import io.questdb.griffin.engine.groupby.GroupByFunctionsUpdater;
import io.questdb.griffin.engine.groupby.GroupByFunctionsUpdaterFactory;
import io.questdb.griffin.engine.groupby.SimpleMapValue;
import io.questdb.std.BytecodeAssembler;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.Transient;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.Closeable;

public class AsyncGroupByNotKeyedAtom implements StatefulAtom, Closeable, Plannable {

    private final Function filter;
    private final GroupByFunctionsUpdater functionUpdater;
    private final SimpleMapValue mapValue;
    private final ObjList<Function> perWorkerFilters;
    private final PerWorkerLocks perWorkerLocks;
    private final ObjList<SimpleMapValue> perWorkerMapValues;

    public AsyncGroupByNotKeyedAtom(
            @Transient @NotNull BytecodeAssembler asm,
            @NotNull CairoConfiguration configuration,
            @NotNull ObjList<GroupByFunction> groupByFunctions,
            int valueCount,
            @Nullable Function filter,
            @Nullable ObjList<Function> perWorkerFilters,
            int workerCount
    ) {
        assert perWorkerFilters == null || perWorkerFilters.size() == workerCount;
        try {
            this.filter = filter;
            this.perWorkerFilters = perWorkerFilters;
            functionUpdater = GroupByFunctionsUpdaterFactory.getInstance(asm, groupByFunctions);
            perWorkerLocks = new PerWorkerLocks(configuration, workerCount);
            mapValue = new SimpleMapValue(valueCount);
            perWorkerMapValues = new ObjList<>(workerCount);
            for (int i = 0; i < workerCount; i++) {
                perWorkerMapValues.extendAndSet(i, new SimpleMapValue(valueCount));
            }
            clear();
        } catch (Throwable e) {
            close();
            throw e;
        }
    }

    public int acquire(int workerId, boolean owner, SqlExecutionCircuitBreaker circuitBreaker) {
        if (workerId == -1 && owner) {
            // Owner thread is free to use the original filter anytime.
            return -1;
        }
        return perWorkerLocks.acquireSlot(workerId, circuitBreaker);
    }

    @Override
    public void clear() {
        functionUpdater.updateEmpty(mapValue);
        mapValue.setNew(true);
        for (int i = 0, n = perWorkerMapValues.size(); i < n; i++) {
            SimpleMapValue value = perWorkerMapValues.getQuick(i);
            functionUpdater.updateEmpty(value);
            value.setNew(true);
        }
    }

    @Override
    public void close() {
        Misc.free(filter);
        Misc.freeObjList(perWorkerFilters);
    }

    public Function getFilter(int slotId) {
        if (slotId == -1 || perWorkerFilters == null) {
            return filter;
        }
        return perWorkerFilters.getQuick(slotId);
    }

    public GroupByFunctionsUpdater getFunctionUpdater() {
        return functionUpdater;
    }

    public SimpleMapValue getMapValue(int slotId) {
        if (slotId == -1) {
            return mapValue;
        }
        return perWorkerMapValues.getQuick(slotId);
    }

    // Thread-unsafe, should be used by query owner thread only.
    public SimpleMapValue getOwnerMapValue() {
        return mapValue;
    }

    // Thread-unsafe, should be used by query owner thread only.
    public ObjList<SimpleMapValue> getPerWorkerMapValues() {
        return perWorkerMapValues;
    }

    @Override
    public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
        if (filter != null) {
            filter.init(symbolTableSource, executionContext);
        }
        if (perWorkerFilters != null) {
            final boolean current = executionContext.getCloneSymbolTables();
            executionContext.setCloneSymbolTables(true);
            try {
                Function.init(perWorkerFilters, symbolTableSource, executionContext);
            } finally {
                executionContext.setCloneSymbolTables(current);
            }
        }
    }

    @Override
    public void initCursor() {
        if (filter != null) {
            filter.initCursor();
        }
        if (perWorkerFilters != null) {
            // Initialize all per-worker filters on the query owner thread to avoid
            // DataUnavailableException thrown on worker threads when filtering.
            Function.initCursor(perWorkerFilters);
        }
    }

    public void release(int filterId) {
        perWorkerLocks.releaseSlot(filterId);
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.val(filter);
    }
}
