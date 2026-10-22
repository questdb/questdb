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

package io.questdb.griffin;

import io.questdb.cairo.ArrayColumnTypes;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.GenericRecordMetadata;
import io.questdb.cairo.IndexType;
import io.questdb.cairo.ListColumnFilter;
import io.questdb.cairo.RecordSink;
import io.questdb.cairo.RecordSinkFactory;
import io.questdb.cairo.TableColumnMetadata;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.griffin.engine.functions.GroupByFunction;
import io.questdb.griffin.engine.functions.PerWorkerFunctionList;
import io.questdb.griffin.engine.functions.SymbolFunction;
import io.questdb.griffin.engine.functions.cast.CastStrToSymbolFunctionFactory;
import io.questdb.griffin.engine.groupby.GroupByFunctionsUpdater;
import io.questdb.griffin.engine.groupby.GroupByFunctionsUpdaterFactory;
import io.questdb.griffin.engine.groupby.GroupByUtils;
import io.questdb.griffin.plan.logical.AggregatePlan;
import io.questdb.griffin.plan.logical.BoundExpression;
import io.questdb.griffin.plan.logical.ColumnExpression;
import io.questdb.griffin.plan.logical.OutputSchema;
import io.questdb.std.BytecodeAssembler;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;

import java.io.Closeable;

/**
 * Compiled grouping, aggregate and post-join functions, with one mutable instance
 * per execution slot where required. Owns functions only; it borrows joined
 * metadata and all execution symbol sources. Initialize once per execution, when the
 * cursor opens: before the build, dispatch or parent initialization. Initialization
 * needs both inputs' symbol tables and no frozen build. Drain workers and finish output
 * before releasing symbol backing. No model or expression nodes survive compilation.
 */
public final class HashJoinGroupByFunctions implements Closeable, SymbolTableSource {
    private final ObjList<GroupByFunction> groupByFunctions = new ObjList<>();
    private final ObjList<Function> keyFunctions = new ObjList<>();
    private final ArrayColumnTypes keyTypes = new ArrayColumnTypes();
    private final ObjList<RecordSink> mapSinks = new ObjList<>();
    private final ObjList<Function> outputFunctions = new ObjList<>();
    private final GenericRecordMetadata outputMetadata = new GenericRecordMetadata();
    private final ObjList<GroupByFunctionsUpdater> updaters = new ObjList<>();
    private final ArrayColumnTypes valueTypes = new ArrayColumnTypes();
    private final int workerCount;
    private Function filter;
    private ObjList<Function> outputOnlyFunctions;
    private ObjList<Function> workerFilters;
    private ObjList<ObjList<GroupByFunction>> workerGroupByFunctions;
    private ObjList<ObjList<Function>> workerKeyFunctions;

    public HashJoinGroupByFunctions(
            CairoConfiguration configuration,
            BytecodeAssembler asm,
            FunctionInstantiator instantiator,
            HashJoinGroupByCandidate candidate,
            HashJoinGroupByMetadata metadata,
            int workerCount,
            SqlExecutionContext executionContext
    ) throws SqlException {
        if (workerCount < 0) {
            throw new IllegalArgumentException("negative hash join worker count");
        }
        this.workerCount = workerCount;
        try {
            final AggregatePlan plan = candidate.getAggregate();
            final OutputSchema input = metadata.getInput();
            final RecordMetadata joined = metadata.getJoinedMetadata();
            final ObjList<BoundExpression> keys = plan.getGroupingExpressions();
            final ObjList<BoundExpression> computedKeys = new ObjList<>();
            final ListColumnFilter columnFilter = new ListColumnFilter();
            outputFunctions.setPos(plan.getOutput().getColumnCount());
            for (int i = 0; i < plan.getAggregates().size(); i++) {
                GroupByFunction function = (GroupByFunction) instantiator.instantiateAggregate(plan.getAggregates().getQuick(i), input, joined, executionContext);
                outputFunctions.setQuick(keys.size() + i, function);
                if (!HashJoinGroupByCandidate.isSupportedAggregate(function)) {
                    throw SqlException.$(plan.getPosition(), "unsupported fused hash join aggregate");
                }
                groupByFunctions.add(function);
                function.initValueTypes(valueTypes);
            }
            int lastIndex = -1;
            for (int i = 0; i < keys.size(); i++) {
                if (keys.getQuick(i) instanceof ColumnExpression column) {
                    int index = input.getColumnIndexById(column.getColumnId());
                    if (index != lastIndex) {
                        columnFilter.add(index + 1);
                        keyTypes.add(column.getDataType());
                        lastIndex = index;
                    }
                    outputFunctions.setQuick(i, GroupByUtils.createColumnFunction(joined,
                            valueTypes.getColumnCount() + keyTypes.getColumnCount(), column.getDataType(), index));
                }
            }
            for (int i = 0; i < keys.size(); i++) {
                BoundExpression key = keys.getQuick(i);
                if (!(key instanceof ColumnExpression)) {
                    Function function = instantiator.instantiate(key, input, joined, executionContext);
                    keyFunctions.add(function);
                    computedKeys.add(key);
                    Function column = GroupByUtils.createColumnFunction(null,
                            valueTypes.getColumnCount() + keyTypes.getColumnCount() + 1, function.getType(), -1);
                    keyTypes.add(column.getType());
                    if (function.getType() == ColumnType.SYMBOL && column.getType() == ColumnType.STRING) {
                        column = new CastStrToSymbolFunctionFactory.Func(column);
                    }
                    outputFunctions.setQuick(i, column);
                }
            }
            for (int i = 0; i < outputFunctions.size(); i++) {
                Function function = outputFunctions.getQuick(i);
                outputMetadata.add(new TableColumnMetadata(plan.getOutput().getColumnName(i).toString(), function.getType(),
                        IndexType.NONE, 0, function instanceof SymbolFunction symbol && symbol.isSymbolTableStatic(), function.getMetadata()));
            }
            outputOnlyFunctions = GroupByUtils.extractNonGroupByFunctions(outputFunctions);
            workerGroupByFunctions = instantiator.instantiateWorkerAggregates(plan.getAggregates(), input, joined,
                    groupByFunctions, workerCount, executionContext);
            workerKeyFunctions = instantiator.instantiateWorkerFunctions(computedKeys, input, joined,
                    keyFunctions, workerCount, executionContext);
            BoundExpression postFilter = candidate.getPostJoinFilter();
            if (postFilter != null) {
                filter = instantiator.instantiate(postFilter, input, joined, executionContext);
                workerFilters = instantiator.instantiateWorkers(postFilter, input, joined, filter, workerCount, executionContext);
            }
            final boolean isKeyed = isKeyed();
            // A null class requests LoopingRecordSink, which getInstance() builds, so branch on isKeyed only.
            Class<RecordSink> sinkClass = isKeyed ? RecordSinkFactory.getInstanceClass(configuration, asm,
                    joined, columnFilter, keyFunctions, null, null, null, null) : null;
            Class<? extends GroupByFunctionsUpdater> updaterClass = GroupByFunctionsUpdaterFactory.getInstanceClass(asm, groupByFunctions.size());
            // Index zero is the owner (slot -1); other entries are acquired worker slots.
            for (int slot = -1; slot < workerCount; slot++) {
                mapSinks.add(isKeyed ? RecordSinkFactory.getInstance(sinkClass, joined, columnFilter,
                        getKeyFunctions(slot), null, null, null, null) : null);
                updaters.add(GroupByFunctionsUpdaterFactory.getInstance(updaterClass, getGroupByFunctions(slot)));
            }
        } catch (Throwable th) {
            Misc.free(this, th);
            throw th;
        }
    }

    @Override
    public void close() {
        Throwable failure = closeWorkers(null, workerKeyFunctions);
        failure = closeWorkers(failure, workerGroupByFunctions);
        failure = Misc.freeObjListBestEffort(failure, workerFilters);
        if (workerFilters != null) {
            workerFilters.clear();
        }
        failure = Misc.freeBestEffort(failure, filter);
        filter = null;
        failure = Misc.freeObjListBestEffort(failure, outputFunctions);
        outputFunctions.clear();
        failure = Misc.freeObjListBestEffort(failure, keyFunctions);
        keyFunctions.clear();
        CairoException.rethrowCleanupFailure(failure);
    }

    /**
     * Release execution state after draining slots and finishing output, also after failed init.
     */
    public void cursorClosed() {
        Throwable failure = cursorClosed(null, keyFunctions);
        failure = cursorClosed(failure, groupByFunctions);
        failure = cursorClosed(failure, outputOnlyFunctions);
        for (int i = 0; i < workerCount; i++) {
            if (workerKeyFunctions != null) {
                failure = cursorClosed(failure, workerKeyFunctions.getQuick(i));
            }
            if (workerGroupByFunctions != null) {
                failure = cursorClosed(failure, workerGroupByFunctions.getQuick(i));
            }
        }
        failure = cursorClosed(failure, workerFilters);
        if (filter != null) {
            try {
                filter.cursorClosed();
            } catch (Throwable th) {
                failure = addFailure(failure, th);
            }
        }
        CairoException.rethrowCleanupFailure(failure);
    }

    public Function getFilter(int slot) {
        return slot < 0 || workerFilters == null ? filter : workerFilters.getQuick(slot);
    }

    public ObjList<GroupByFunction> getGroupByFunctions(int slot) {
        return slot < 0 || workerGroupByFunctions == null ? groupByFunctions : workerGroupByFunctions.getQuick(slot);
    }

    public ObjList<Function> getKeyFunctions(int slot) {
        return slot < 0 || workerKeyFunctions == null ? keyFunctions : workerKeyFunctions.getQuick(slot);
    }

    public ArrayColumnTypes getKeyTypes() {
        return keyTypes;
    }

    public RecordSink getMapSink(int slot) {
        return mapSinks.getQuick(slot + 1);
    }

    public ObjList<Function> getOutputFunctions() {
        return outputFunctions;
    }

    public RecordMetadata getOutputMetadata() {
        return outputMetadata;
    }

    @Override
    public SymbolTable getSymbolTable(int columnIndex) {
        return (SymbolFunction) outputFunctions.getQuick(columnIndex);
    }

    public GroupByFunctionsUpdater getUpdater(int slot) {
        return updaters.getQuick(slot + 1);
    }

    public ArrayColumnTypes getValueTypes() {
        return valueTypes;
    }

    public int getWorkerCount() {
        return workerCount;
    }

    public boolean isKeyed() {
        return keyTypes.getColumnCount() > 0;
    }

    /**
     * Sources are owner followed by worker slots. Each must already serve the symbol
     * tables of the joined record's columns for its execution, through symbol table
     * flyweights that no other slot shares. The build need not be frozen: the fused
     * operator calls this when its cursor opens, before it builds.
     * The caller cleans up partial initialization before retrying an execution.
     */
    public void init(ObjList<? extends SymbolTableSource> sources, SqlExecutionContext executionContext) throws SqlException {
        assert sources.size() == workerCount + 1;
        SymbolTableSource ownerSource = sources.getQuick(0);
        Function.init(keyFunctions, ownerSource, executionContext, null);
        Function.init(groupByFunctions, ownerSource, executionContext, null);
        if (filter != null) {
            filter.init(ownerSource, executionContext);
        }
        boolean isCloneSymbolTables = executionContext.getCloneSymbolTables();
        executionContext.setCloneSymbolTables(true);
        try {
            for (int i = 0; i < workerCount; i++) {
                SymbolTableSource source = sources.getQuick(i + 1);
                if (workerKeyFunctions != null) {
                    PerWorkerFunctionList.init(workerKeyFunctions.getQuick(i), keyFunctions, source, executionContext);
                }
                if (workerGroupByFunctions != null) {
                    PerWorkerFunctionList.init(workerGroupByFunctions.getQuick(i), groupByFunctions, source, executionContext);
                }
                if (workerFilters != null) {
                    Function workerFilter = workerFilters.getQuick(i);
                    filter.offerStateTo(workerFilter);
                    workerFilter.init(source, executionContext);
                }
            }
        } finally {
            executionContext.setCloneSymbolTables(isCloneSymbolTables);
        }
        // Aggregate owners were initialized above. Initialize only the rewritten
        // output keys here, so parent projections/sorts can immediately resolve symbols.
        Function.init(outputOnlyFunctions, ownerSource, executionContext, null);
    }

    @Override
    public SymbolTable newSymbolTable(int columnIndex) {
        return ((SymbolFunction) outputFunctions.getQuick(columnIndex)).newSymbolTable();
    }

    private static Throwable addFailure(Throwable failure, Throwable th) {
        if (failure == null) {
            return th;
        }
        if (failure != th) {
            failure.addSuppressed(th);
        }
        return failure;
    }

    private static Throwable closeWorkers(Throwable failure, ObjList<? extends ObjList<? extends Function>> workers) {
        if (workers != null) {
            for (int i = 0; i < workers.size(); i++) {
                try {
                    PerWorkerFunctionList.close(workers.getQuick(i));
                } catch (Throwable th) {
                    failure = addFailure(failure, th);
                }
            }
            workers.clear();
        }
        return failure;
    }

    private static Throwable cursorClosed(Throwable failure, ObjList<? extends Function> functions) {
        if (functions != null) {
            for (int i = 0; i < functions.size(); i++) {
                if (PerWorkerFunctionList.isOwned(functions, i)) {
                    try {
                        functions.getQuick(i).cursorClosed();
                    } catch (Throwable th) {
                        failure = addFailure(failure, th);
                    }
                }
            }
        }
        return failure;
    }

}
