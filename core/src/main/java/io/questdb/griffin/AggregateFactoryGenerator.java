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
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.ColumnTypes;
import io.questdb.cairo.EntityColumnFilter;
import io.questdb.cairo.FullPartitionFrameCursorFactory;
import io.questdb.cairo.GenericRecordMetadata;
import io.questdb.cairo.IndexType;
import io.questdb.cairo.IntervalPartitionFrameCursorFactory;
import io.questdb.cairo.ListColumnFilter;
import io.questdb.cairo.RecordSink;
import io.questdb.cairo.RecordSinkFactory;
import io.questdb.cairo.TableColumnMetadata;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableReaderMetadata;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.PartitionFrameCursorFactory;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.vm.api.MemoryCARW;
import io.questdb.griffin.engine.functions.GroupByFunction;
import io.questdb.griffin.engine.functions.PerWorkerFunctionList;
import io.questdb.griffin.engine.functions.SymbolFunction;
import io.questdb.griffin.engine.functions.cast.CastStrToSymbolFunctionFactory;
import io.questdb.griffin.engine.groupby.CountRecordCursorFactory;
import io.questdb.griffin.engine.groupby.DistinctRecordCursorFactory;
import io.questdb.griffin.engine.groupby.DistinctTimeSeriesRecordCursorFactory;
import io.questdb.griffin.engine.groupby.GroupByNotKeyedRecordCursorFactory;
import io.questdb.griffin.engine.groupby.GroupByUtils;
import io.questdb.griffin.engine.groupby.vect.AvgDoubleVectorAggregateFunction;
import io.questdb.griffin.engine.groupby.vect.AvgIntVectorAggregateFunction;
import io.questdb.griffin.engine.groupby.vect.AvgLongVectorAggregateFunction;
import io.questdb.griffin.engine.groupby.vect.AvgShortVectorAggregateFunction;
import io.questdb.griffin.engine.groupby.vect.CountDoubleVectorAggregateFunction;
import io.questdb.griffin.engine.groupby.vect.CountIntVectorAggregateFunction;
import io.questdb.griffin.engine.groupby.vect.CountLongVectorAggregateFunction;
import io.questdb.griffin.engine.groupby.vect.CountVectorAggregateFunction;
import io.questdb.griffin.engine.groupby.vect.GroupByRecordCursorFactory;
import io.questdb.griffin.engine.groupby.vect.KSumDoubleVectorAggregateFunction;
import io.questdb.griffin.engine.groupby.vect.MaxDateVectorAggregateFunction;
import io.questdb.griffin.engine.groupby.vect.MaxDoubleVectorAggregateFunction;
import io.questdb.griffin.engine.groupby.vect.MaxIntVectorAggregateFunction;
import io.questdb.griffin.engine.groupby.vect.MaxLongVectorAggregateFunction;
import io.questdb.griffin.engine.groupby.vect.MaxShortVectorAggregateFunction;
import io.questdb.griffin.engine.groupby.vect.MaxTimestampVectorAggregateFunction;
import io.questdb.griffin.engine.groupby.vect.MinDateVectorAggregateFunction;
import io.questdb.griffin.engine.groupby.vect.MinDoubleVectorAggregateFunction;
import io.questdb.griffin.engine.groupby.vect.MinIntVectorAggregateFunction;
import io.questdb.griffin.engine.groupby.vect.MinLongVectorAggregateFunction;
import io.questdb.griffin.engine.groupby.vect.MinShortVectorAggregateFunction;
import io.questdb.griffin.engine.groupby.vect.MinTimestampVectorAggregateFunction;
import io.questdb.griffin.engine.groupby.vect.NSumDoubleVectorAggregateFunction;
import io.questdb.griffin.engine.groupby.vect.SumDoubleVectorAggregateFunction;
import io.questdb.griffin.engine.groupby.vect.SumIntVectorAggregateFunction;
import io.questdb.griffin.engine.groupby.vect.SumLong256VectorAggregateFunction;
import io.questdb.griffin.engine.groupby.vect.SumLongVectorAggregateFunction;
import io.questdb.griffin.engine.groupby.vect.SumShortVectorAggregateFunction;
import io.questdb.griffin.engine.groupby.vect.VectorAggregateFunction;
import io.questdb.griffin.engine.groupby.vect.VectorAggregateFunctionConstructor;
import io.questdb.griffin.engine.join.JoinRecordMetadata;
import io.questdb.griffin.engine.join.SharedRecordCursorFactory;
import io.questdb.griffin.engine.table.AsyncGroupByNotKeyedRecordCursorFactory;
import io.questdb.griffin.engine.table.AsyncGroupByRecordCursorFactory;
import io.questdb.griffin.engine.table.AsyncHorizonJoinNotKeyedRecordCursorFactory;
import io.questdb.griffin.engine.table.AsyncHorizonJoinRecordCursorFactory;
import io.questdb.griffin.engine.table.AsyncHorizonJoinResources;
import io.questdb.griffin.engine.table.AsyncMultiHorizonJoinNotKeyedRecordCursorFactory;
import io.questdb.griffin.engine.table.AsyncMultiHorizonJoinRecordCursorFactory;
import io.questdb.griffin.engine.table.FilteredRecordCursorFactory;
import io.questdb.griffin.engine.table.HorizonJoinNotKeyedRecordCursorFactory;
import io.questdb.griffin.engine.table.HorizonJoinRecordCursorFactory;
import io.questdb.griffin.engine.table.HorizonJoinSlaveState;
import io.questdb.griffin.engine.table.MultiHorizonJoinNotKeyedRecordCursorFactory;
import io.questdb.griffin.engine.table.MultiHorizonJoinRecord;
import io.questdb.griffin.engine.table.MultiHorizonJoinRecordCursorFactory;
import io.questdb.griffin.engine.table.PostingIndexDistinctRecordCursorFactory;
import io.questdb.griffin.engine.table.SelectedRecordCursorFactory;
import io.questdb.griffin.model.RuntimeIntrinsicIntervalModel;
import io.questdb.griffin.plan.logical.AggregatePlan;
import io.questdb.griffin.plan.logical.BoundExpression;
import io.questdb.griffin.plan.logical.ColumnExpression;
import io.questdb.griffin.plan.logical.DistinctPlan;
import io.questdb.griffin.plan.logical.FilterPlan;
import io.questdb.griffin.plan.logical.FunctionExpression;
import io.questdb.griffin.plan.logical.HorizonJoinPlan;
import io.questdb.griffin.plan.logical.HorizonJoinSlave;
import io.questdb.griffin.plan.logical.JoinInput;
import io.questdb.griffin.plan.logical.LimitPlan;
import io.questdb.griffin.plan.logical.LogicalPlan;
import io.questdb.griffin.plan.logical.OutputSchema;
import io.questdb.griffin.plan.logical.ProjectPlan;
import io.questdb.griffin.plan.logical.ScanPlan;
import io.questdb.griffin.plan.logical.SetOperationPlan;
import io.questdb.griffin.plan.logical.SortPlan;
import io.questdb.jit.CompiledFilter;
import io.questdb.std.BitSet;
import io.questdb.std.BytecodeAssembler;
import io.questdb.std.Chars;
import io.questdb.std.IntHashSet;
import io.questdb.std.IntList;
import io.questdb.std.IntObjHashMap;
import io.questdb.std.LongList;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.ObjectPool;

import static io.questdb.cairo.ColumnType.*;
import static io.questdb.cairo.sql.PartitionFrameCursorFactory.ORDER_ASC;
import static io.questdb.griffin.SqlKeywords.isCountKeyword;
import static io.questdb.griffin.SqlKeywords.isSumKeyword;

final class AggregateFactoryGenerator {
    private static final VectorAggregateFunctionConstructor COUNT_CONSTRUCTOR = (keyKind, _, _, _) -> new CountVectorAggregateFunction(keyKind);
    private static final IntObjHashMap<VectorAggregateFunctionConstructor> avgConstructors = new IntObjHashMap<>();
    private static final IntObjHashMap<VectorAggregateFunctionConstructor> countConstructors = new IntObjHashMap<>();
    private static final IntObjHashMap<VectorAggregateFunctionConstructor> ksumConstructors = new IntObjHashMap<>();
    private static final IntObjHashMap<VectorAggregateFunctionConstructor> maxConstructors = new IntObjHashMap<>();
    private static final IntObjHashMap<VectorAggregateFunctionConstructor> minConstructors = new IntObjHashMap<>();
    private static final IntObjHashMap<VectorAggregateFunctionConstructor> nsumConstructors = new IntObjHashMap<>();
    private static final IntObjHashMap<VectorAggregateFunctionConstructor> sumConstructors = new IntObjHashMap<>();
    private final BytecodeAssembler asm;
    private final SqlCodeGenerator codeGenerator;
    private final CairoConfiguration configuration;
    private final OutputSchema emptySchema;
    private final EntityColumnFilter entityColumnFilter;
    private final ObjList<HorizonJoinKeys> horizonKeys = new ObjList<>();
    private final IntList horizonMasterSymbols;
    private final IntList horizonSlaveSymbols;
    private final ObjectPool<SortPlan> sorts;

    AggregateFactoryGenerator(
            CairoConfiguration configuration,
            SqlCodeGenerator codeGenerator,
            BytecodeAssembler asm,
            OutputSchema emptySchema,
            EntityColumnFilter entityColumnFilter,
            IntList horizonMasterSymbols,
            IntList horizonSlaveSymbols,
            ObjectPool<SortPlan> sorts
    ) {
        this.codeGenerator = codeGenerator;
        this.configuration = configuration;
        this.asm = asm;
        this.emptySchema = emptySchema;
        this.entityColumnFilter = entityColumnFilter;
        this.horizonMasterSymbols = horizonMasterSymbols;
        this.horizonSlaveSymbols = horizonSlaveSymbols;
        this.sorts = sorts;
    }

    private static void assemble(
            AggregatePlan plan,
            RecordMetadata inputMetadata,
            int timestampIndex,
            boolean isBaseTimestampAscending,
            FunctionInstantiator instantiator,
            SqlExecutionContext executionContext,
            ObjList<GroupByFunction> aggregates,
            ObjList<Function> keyFunctions,
            ObjList<Function> recordFunctions,
            ArrayColumnTypes keyTypes,
            ArrayColumnTypes valueTypes,
            ListColumnFilter columnFilter
    ) throws SqlException {
        final OutputSchema input = plan.getInput().getOutput();
        final ObjList<BoundExpression> keys = plan.getGroupingExpressions();
        recordFunctions.setPos(plan.getOutput().getColumnCount());
        for (int i = 0, n = plan.getAggregates().size(); i < n; i++) {
            final FunctionExpression call = plan.getAggregates().getQuick(i);
            final GroupByFunction function = (GroupByFunction) instantiator.instantiateAggregate(call, input, inputMetadata, executionContext);
            recordFunctions.setQuick(keys.size() + i, function);
            aggregates.add(function);
            GroupByUtils.validateTimestampOrder(function, timestampIndex, isBaseTimestampAscending, call.getPosition());
            function.initValueTypes(valueTypes);
        }
        // RecordSink writes direct columns before computed keys. Output functions
        // map those physical key slots back to the logical key order.
        int lastIndex = -1;
        for (int i = 0, n = keys.size(); i < n; i++) {
            if (keys.getQuick(i) instanceof ColumnExpression column) {
                final int index = input.getColumnIndexById(column.getColumnId());
                // The map stores a column repeated consecutively as one key.
                if (index != lastIndex) {
                    columnFilter.add(index + 1);
                    keyTypes.add(column.getDataType());
                    lastIndex = index;
                }
                recordFunctions.setQuick(i, GroupByUtils.createColumnFunction(inputMetadata,
                        valueTypes.getColumnCount() + keyTypes.getColumnCount(), column.getDataType(), index));
            }
        }
        for (int i = 0, n = keys.size(); i < n; i++) {
            final BoundExpression key = keys.getQuick(i);
            if (!(key instanceof ColumnExpression)) {
                final Function function = instantiator.instantiate(key, input, inputMetadata, executionContext);
                keyFunctions.add(function);
                Function keyColumn = GroupByUtils.createColumnFunction(null,
                        valueTypes.getColumnCount() + keyTypes.getColumnCount() + 1, function.getType(), -1);
                keyTypes.add(keyColumn.getType());
                if (function.getType() == ColumnType.SYMBOL && keyColumn.getType() == ColumnType.STRING) {
                    keyColumn = new CastStrToSymbolFunctionFactory.Func(keyColumn);
                }
                recordFunctions.setQuick(i, keyColumn);
            }
        }
    }

    private static boolean isThreadSafe(ObjList<? extends Function> functions) {
        for (int i = 0, n = functions.size(); i < n; i++) {
            if (!functions.getQuick(i).isThreadSafe()) {
                return false;
            }
        }
        return true;
    }

    private static GenericRecordMetadata metadata(AggregatePlan plan, RecordMetadata input, ObjList<Function> functions) {
        final GenericRecordMetadata metadata = new GenericRecordMetadata();
        final OutputSchema output = plan.getOutput();
        final ObjList<BoundExpression> keys = plan.getGroupingExpressions();
        for (int i = 0, n = output.getColumnCount(); i < n; i++) {
            final String name = Chars.toString(output.getColumnName(i));
            if (i < keys.size() && keys.getQuick(i) instanceof ColumnExpression column) {
                final int index = plan.getInput().getOutput().getColumnIndexById(column.getColumnId());
                metadata.add(Chars.equals(name, input.getColumnName(index)) ? input.getColumnMetadata(index)
                        : new TableColumnMetadata(name, output.getColumnType(i), input.getColumnIndexType(index),
                        input.getIndexValueBlockCapacity(index), input.isSymbolTableStatic(index), input.getMetadata(index)));
            } else {
                final Function function = functions.getQuick(i);
                metadata.add(new TableColumnMetadata(name, output.getColumnType(i), IndexType.NONE, 0,
                        function instanceof SymbolFunction symbol && symbol.isSymbolTableStatic(), function.getMetadata()));
            }
        }
        metadata.setTimestampIndex(output.getTimestampIndex());
        return metadata;
    }

    private static void sharedRecordFunctions(
            AggregatePlan plan,
            RecordMetadata inputMetadata,
            FunctionInstantiator instantiator,
            SqlExecutionContext executionContext,
            ObjList<Function> recordFunctions,
            ArrayColumnTypes valueTypes,
            ObjList<Function> functions
    ) throws SqlException {
        final OutputSchema input = plan.getInput().getOutput();
        final ObjList<BoundExpression> keys = plan.getGroupingExpressions();
        functions.setPos(recordFunctions.size());
        for (int i = 0, n = plan.getAggregates().size(); i < n; i++) {
            final int index = keys.size() + i;
            final GroupByFunction function = (GroupByFunction) instantiator.instantiateAggregate(plan.getAggregates().getQuick(i), input, inputMetadata, executionContext);
            functions.setQuick(index, function);
            function.initSharedFrom((GroupByFunction) recordFunctions.getQuick(index));
        }
        int keySlot = valueTypes.getColumnCount();
        int lastIndex = -1;
        for (int i = 0, n = keys.size(); i < n; i++) {
            if (keys.getQuick(i) instanceof ColumnExpression column) {
                final int index = input.getColumnIndexById(column.getColumnId());
                if (index != lastIndex) {
                    keySlot++;
                    lastIndex = index;
                }
                functions.setQuick(i, GroupByUtils.createColumnFunction(inputMetadata,
                        keySlot, column.getDataType(), index));
            }
        }
        for (int i = 0, n = keys.size(); i < n; i++) {
            if (!(keys.getQuick(i) instanceof ColumnExpression)) {
                final Function owner = recordFunctions.getQuick(i);
                final int type = owner instanceof CastStrToSymbolFunctionFactory.Func ? ColumnType.STRING : owner.getType();
                Function keyColumn = GroupByUtils.createColumnFunction(null, ++keySlot, type, -1);
                if (owner instanceof CastStrToSymbolFunctionFactory.Func) {
                    keyColumn = new CastStrToSymbolFunctionFactory.Func(keyColumn);
                }
                functions.setQuick(i, keyColumn);
            }
        }
    }

    /**
     * Skips column projections that keep every input column at its position and type: the aggregate
     * binds its input by position, so only the names differ.
     */
    private static LogicalPlan skipRenames(LogicalPlan plan) {
        while (plan instanceof ProjectPlan project && !project.hasTimestampDeclaration() && !project.hasUpdateConversions()) {
            final OutputSchema input = project.getInput().getOutput();
            final OutputSchema output = project.getOutput();
            if (output.getColumnCount() != input.getColumnCount() || output.getTimestampIndex() != input.getTimestampIndex()) {
                return plan;
            }
            for (int i = 0, n = output.getColumnCount(); i < n; i++) {
                if (!(project.getExpressions().getQuick(i) instanceof ColumnExpression column) || !column.isDirectReference() || column.isCast()
                        || column.getColumnId() != input.getColumnId(i) || output.getColumnType(i) != input.getColumnType(i)) {
                    return plan;
                }
            }
            plan = project.getInput();
        }
        return plan;
    }

    private static BoundExpression stolenPredicate(GenerationFrame frame, FilterPlan filterPlan, RecordCursorFactory filter) {
        final BoundExpression predicate = FilterFactoryGenerator.getParallelPredicate(frame, filter);
        return predicate != null ? predicate : filterPlan.getPredicate();
    }

    private static VectorAggregateFunctionConstructor vectorConstructor(FunctionExpression call) {
        if (!call.isAggregate()) {
            return null;
        }
        final int count = call.getArgumentCount();
        if (count == 0) {
            return getVectorAggregateConstructor(call.getName(), ColumnType.UNDEFINED, true);
        }
        if (count == 1 && call.argumentAt(0) instanceof ColumnExpression column) {
            return getVectorAggregateConstructor(call.getName(), column.getDataType(), false);
        }
        return null;
    }

    private static ColumnExpression vectorKey(BoundExpression key) {
        if (key instanceof ColumnExpression column) {
            return column.getDataType() == ColumnType.INT || column.getDataType() == ColumnType.SYMBOL ? column : null;
        }
        if (key instanceof FunctionExpression call && SqlKeywords.isHourKeyword(call.getName())
                && call.getArgumentCount() == 1 && call.argumentAt(0) instanceof ColumnExpression column
                && ColumnType.isTimestamp(column.getDataType())) {
            return column;
        }
        return null;
    }

    private static ObjList<ObjList<GroupByFunction>> workerAggregates(
            AggregatePlan plan,
            ObjList<GroupByFunction> aggregates,
            RecordMetadata inputMetadata,
            FunctionInstantiator instantiator,
            SqlExecutionContext executionContext
    ) throws SqlException {
        if (isThreadSafe(aggregates)) {
            return null;
        }
        final int workerCount = executionContext.getSharedQueryWorkerCount();
        final ObjList<ObjList<GroupByFunction>> workers = new ObjList<>(workerCount);
        instantiator.beginWorkerClones();
        try {
            for (int w = 0; w < workerCount; w++) {
                final PerWorkerFunctionList<GroupByFunction> functions = new PerWorkerFunctionList<>(aggregates.size());
                workers.add(functions);
                for (int i = 0, n = aggregates.size(); i < n; i++) {
                    final GroupByFunction owner = aggregates.getQuick(i);
                    if (owner.isThreadSafe()) {
                        functions.add(owner, false);
                    } else {
                        final GroupByFunction function = (GroupByFunction) instantiator.instantiateAggregate(plan.getAggregates().getQuick(i),
                                plan.getInput().getOutput(), inputMetadata, executionContext);
                        functions.add(function, true);
                        function.initValueIndex(owner.getValueIndex());
                    }
                }
            }
            return workers;
        } catch (Throwable th) {
            closeWorkers(workers, th);
            throw th;
        } finally {
            instantiator.endWorkerClones();
        }
    }

    private static ObjList<ObjList<Function>> workerKeys(
            AggregatePlan plan,
            ObjList<Function> keyFunctions,
            RecordMetadata inputMetadata,
            FunctionInstantiator instantiator,
            SqlExecutionContext executionContext
    ) throws SqlException {
        if (isThreadSafe(keyFunctions)) {
            return null;
        }
        final ObjList<BoundExpression> keys = plan.getGroupingExpressions();
        final int workerCount = executionContext.getSharedQueryWorkerCount();
        final ObjList<ObjList<Function>> workers = new ObjList<>(workerCount);
        instantiator.beginWorkerClones();
        try {
            for (int w = 0; w < workerCount; w++) {
                final PerWorkerFunctionList<Function> functions = new PerWorkerFunctionList<>(keyFunctions.size());
                workers.add(functions);
                for (int i = 0, k = 0, n = keys.size(); i < n; i++) {
                    if (!(keys.getQuick(i) instanceof ColumnExpression)) {
                        final Function owner = keyFunctions.getQuick(k++);
                        functions.add(owner.isThreadSafe() ? owner : instantiator.instantiate(keys.getQuick(i), plan.getInput().getOutput(),
                                inputMetadata, executionContext), !owner.isThreadSafe());
                    }
                }
            }
            return workers;
        } catch (Throwable th) {
            closeWorkers(workers, th);
            throw th;
        } finally {
            instantiator.endWorkerClones();
        }
    }

    private RecordCursorFactory generateFunctions(
            GenerationFrame frame,
            AggregatePlan plan,
            RecordCursorFactory base,
            int timestampIndex,
            FunctionInstantiator instantiator,
            int sharedConsumerCount,
            SqlExecutionContext executionContext
    ) throws SqlException {
        ObjList<Function> keyFunctions = null;
        ObjList<Function> recordFunctions = null;
        ObjList<ObjList<GroupByFunction>> workerAggregates = null;
        ObjList<ObjList<Function>> workerKeys = null;
        ObjList<Function> workerFilters = null;
        Function ownedFilter = null;
        CompiledFilter compiledFilter = null;
        MemoryCARW bindVariableMemory = null;
        ObjList<Function> bindVariables = null;
        ObjList<ObjList<Function>> sharedRecordFunctions = null;
        boolean isAdopted = false;
        try {
            final ObjList<GroupByFunction> aggregates = new ObjList<>(plan.getAggregates().size());
            keyFunctions = new ObjList<>(plan.getGroupingExpressions().size());
            recordFunctions = new ObjList<>(plan.getOutput().getColumnCount());
            final ArrayColumnTypes keyTypes = frame.keyTypes;
            final ArrayColumnTypes valueTypes = frame.valueTypes;
            final ListColumnFilter columnFilter = frame.listColumnFilterA;
            keyTypes.clear();
            valueTypes.clear();
            columnFilter.clear();
            assemble(plan, base.getMetadata(), timestampIndex, SqlCodeGenerator.isBaseTimestampAscending(base, timestampIndex),
                    instantiator, executionContext, aggregates, keyFunctions, recordFunctions, keyTypes, valueTypes, columnFilter);
            final GenericRecordMetadata metadata = metadata(plan, base.getMetadata(), recordFunctions);
            if (sharedConsumerCount > 0) {
                sharedRecordFunctions = new ObjList<>(sharedConsumerCount);
                for (int i = 0; i < sharedConsumerCount; i++) {
                    final ObjList<Function> functions = new ObjList<>(recordFunctions.size());
                    sharedRecordFunctions.add(functions);
                    sharedRecordFunctions(plan, base.getMetadata(), instantiator, executionContext, recordFunctions, valueTypes, functions);
                }
            }
            // Identity projections can disappear during generation. The physical
            // wrapper check confirms that its filter still has this input layout.
            final FilterPlan filterPlan = findFilterPlan(plan.getInput());
            final boolean isFilterStealable = filterPlan != null && (FilterFactoryGenerator.isParallelFilter(base)
                    || !base.implementsLimit() && base instanceof FilteredRecordCursorFactory && filterPlan.getInput() instanceof ScanPlan)
                    && base.getBaseFactory().supportsPageFrameCursor();
            final boolean isParallel = canParallelizeGroupBy(
                    base, keyTypes.getColumnCount(), keyFunctions, aggregates, isFilterStealable, executionContext
            );
            IntHashSet filterIndexes = null;
            if (isParallel) {
                workerAggregates = workerAggregates(plan, aggregates, base.getMetadata(), instantiator, executionContext);
                workerKeys = workerKeys(plan, keyFunctions, base.getMetadata(), instantiator, executionContext);
                if (isFilterStealable) {
                    final Function filter = FilterFactoryGenerator.stolenFilter(base);
                    final BoundExpression predicate = stolenPredicate(frame, filterPlan, base);
                    workerFilters = FilterFactoryGenerator.compileWorkers(predicate, filterPlan.getInput().getOutput(), base.getMetadata(), filter, instantiator, executionContext);
                    filterIndexes = new IntHashSet();
                    FilterFactoryGenerator.collectColumnIndexes(predicate, filterPlan.getInput().getOutput(), filterIndexes);
                    // The unopened wrapper owns these until halfClose succeeds. That
                    // releases its worker clones and count-only JIT, retaining the
                    // filter JIT and parameter storage for the aggregate consumer.
                    base.halfClose();
                    ownedFilter = filter;
                    compiledFilter = base.getCompiledFilter();
                    bindVariableMemory = base.getBindVarMemory();
                    bindVariables = base.getBindVarFunctions();
                    base = base.getBaseFactory();
                }
            }
            isAdopted = true;
            return generateGroupBy(
                    base, metadata, columnFilter, keyTypes, valueTypes, aggregates, workerAggregates,
                    keyFunctions, workerKeys, recordFunctions, compiledFilter, bindVariableMemory,
                    bindVariables, ownedFilter, filterIndexes, workerFilters, sharedRecordFunctions, isParallel, executionContext
            );
        } catch (Throwable th) {
            if (!isAdopted) {
                if (sharedRecordFunctions != null) {
                    for (int i = 0, n = sharedRecordFunctions.size(); i < n; i++) {
                        Misc.freeObjList(sharedRecordFunctions.getQuick(i), th);
                    }
                }
                closeWorkers(workerAggregates, th);
                closeWorkers(workerKeys, th);
                Misc.freeObjList(workerFilters, th);
                Misc.freeObjList(recordFunctions, th);
                Misc.freeObjList(keyFunctions, th);
                Misc.free(ownedFilter, th);
                Misc.free(compiledFilter, th);
                Misc.free(bindVariableMemory, th);
                Misc.freeObjList(bindVariables, th);
                Misc.free(base, th);
            }
            throw th;
        }
    }

    private RecordCursorFactory generateSharedInput(GenerationFrame frame, AggregatePlan aggregate) {
        final JoinInput source = aggregate.getSharedSource();
        final int entry = source == null ? -1 : frame.sharedSources.indexOf(source);
        if (entry < 0 || !frame.sharedFactories.getQuick(entry).supportsSharedCursors()) {
            return null;
        }
        final RecordCursorFactory primary = frame.sharedFactories.getQuick(entry);
        final RecordMetadata primaryMetadata = primary.getMetadata();
        final OutputSchema sourceOutput = source.getSourceOutput();
        final OutputSchema input = aggregate.getInput().getOutput();
        if (sourceOutput.getColumnCount() != primaryMetadata.getColumnCount()) {
            return null;
        }
        final GenericRecordMetadata metadata = new GenericRecordMetadata();
        final IntList mapping = new IntList(input.getColumnCount());
        for (int i = 0, n = input.getColumnCount(); i < n; i++) {
            final int shared = aggregate.getSharedInputIds().indexOf(input.getColumnId(i), 0, aggregate.getSharedInputIds().size());
            final int index = shared < 0 ? sourceOutput.getColumnIndexQuiet(input.getColumnName(i))
                    : sourceOutput.getColumnIndexById(aggregate.getSharedSourceIds().getQuick(shared));
            if (index < 0 || primaryMetadata.getColumnType(index) != input.getColumnType(i)) {
                return null;
            }
            mapping.add(index);
            metadata.add(SqlCodeGenerator.copyColumn(primaryMetadata, index, SqlUtil.toColumnName(input.getColumnName(i))));
        }
        final int sharedId = frame.sharedConsumerCounts.getQuick(entry);
        frame.sharedConsumerCounts.setQuick(entry, sharedId + 1);
        return new SelectedRecordCursorFactory(metadata, mapping, new SharedRecordCursorFactory(primary, sharedId));
    }

    private RecordCursorFactory generateVector(AggregatePlan plan, RecordCursorFactory base, SqlExecutionContext executionContext) {
        ObjList<VectorAggregateFunction> functions = null;
        boolean isAdopted = false;
        try {
            functions = new ObjList<>(plan.getAggregates().size() + 1);
            final BoundExpression keyExpression = plan.getGroupingExpressions().getQuick(0);
            final ColumnExpression key = vectorKey(keyExpression);
            assert key != null;
            final int keyType = keyExpression.getDataType();
            final int keyKind = keyExpression instanceof FunctionExpression
                    ? ColumnType.getTimestampDriver(key.getDataType()).getGKKHourInt() : SqlCodeGenerator.GKK_VANILLA_INT;
            final OutputSchema input = plan.getInput().getOutput();
            final int keyIndex = input.getColumnIndexById(key.getColumnId());
            final ArrayColumnTypes types = new ArrayColumnTypes();
            types.add(keyType);
            final GenericRecordMetadata metadata = new GenericRecordMetadata();
            final OutputSchema output = plan.getOutput();
            for (int i = 0, n = output.getColumnCount(); i < n; i++) {
                metadata.add(i == 0
                        ? new TableColumnMetadata(Chars.toString(output.getColumnName(i)), keyType,
                        IndexType.NONE, 0, base.getMetadata().isSymbolTableStatic(keyIndex), null)
                        : new TableColumnMetadata(Chars.toString(output.getColumnName(i)), output.getColumnType(i)));
            }
            final IntList symbolIndexes = new IntList();
            if (keyType == ColumnType.SYMBOL) {
                symbolIndexes.setAll(output.getColumnCount(), -1);
                symbolIndexes.setQuick(0, keyIndex);
            }
            for (int i = 1, n = output.getColumnCount(); i < n; i++) {
                final FunctionExpression call = plan.getAggregates().getQuick(i - 1);
                final int index = call.getArgumentCount() == 0 ? -1
                        : input.getColumnIndexById(((ColumnExpression) call.argumentAt(0)).getColumnId());
                final VectorAggregateFunctionConstructor constructor = vectorConstructor(call);
                assert constructor != null;
                final VectorAggregateFunction function = constructor.create(keyKind,
                        index, base.getMetadata().getTimestampIndex(), executionContext.getSharedQueryWorkerCount());
                functions.add(function);
            }
            isAdopted = true;
            return generateVectorGroupBy(
                    base, metadata, types, functions, keyKind, keyIndex, symbolIndexes, executionContext
            );
        } catch (Throwable th) {
            if (!isAdopted) {
                Misc.freeObjList(functions, th);
                Misc.free(base, th);
            }
            throw th;
        }
    }

    private HorizonJoinKeys horizonJoinKeys(HorizonJoinSlave step, OutputSchema masterOutput, RecordMetadata masterMetadata,
                                            RecordMetadata slaveMetadata) throws SqlException {
        final IntList masterIds = step.getMasterKeyColumnIds();
        if (masterIds.size() == 0) {
            return null;
        }
        final HorizonJoinKeys keys = new HorizonJoinKeys();
        final OutputSchema slaveOutput = step.getInput().getOutput();
        final IntList masterSymbols = horizonMasterSymbols;
        final IntList slaveSymbols = horizonSlaveSymbols;
        masterSymbols.clear();
        slaveSymbols.clear();
        for (int i = 0, n = masterIds.size(); i < n; i++) {
            final int masterIndex = masterOutput.getColumnIndexById(masterIds.getQuick(i));
            final int slaveIndex = slaveOutput.getColumnIndexById(step.getSlaveKeyColumnIds().getQuick(i));
            keys.masterColumns.add(masterIndex + 1);
            keys.slaveColumns.add(slaveIndex + 1);
            final int masterType = masterMetadata.getColumnType(masterIndex);
            final int slaveType = slaveMetadata.getColumnType(slaveIndex);
            assert JoinBinder.isJoinKeyTypeCompatible(masterType, slaveType);
            if (ColumnType.isVarchar(slaveType) || ColumnType.isVarchar(masterType)) {
                keys.types.add(ColumnType.VARCHAR);
                if (ColumnType.isVarchar(slaveType)) {
                    keys.masterStringAsVarchar.set(masterIndex);
                } else {
                    keys.slaveStringAsVarchar.set(slaveIndex);
                }
                keys.slaveSymbolAsString.set(slaveIndex);
                keys.masterSymbolAsString.set(masterIndex);
            } else if (slaveType == ColumnType.SYMBOL && masterType == ColumnType.SYMBOL) {
                keys.types.add(ColumnType.SYMBOL);
                masterSymbols.add(masterIndex);
                slaveSymbols.add(slaveIndex);
            } else if (masterType == ColumnType.SYMBOL || slaveType == ColumnType.SYMBOL) {
                keys.types.add(ColumnType.STRING);
                keys.slaveSymbolAsString.set(slaveIndex);
                keys.masterSymbolAsString.set(masterIndex);
            } else if (ColumnType.isString(slaveType) || ColumnType.isString(masterType)) {
                keys.types.add(masterType);
                keys.slaveSymbolAsString.set(slaveIndex);
                keys.masterSymbolAsString.set(masterIndex);
            } else if (slaveType != masterType) {
                keys.types.add(TIMESTAMP_NANO);
                if (!isTimestampNano(slaveType)) {
                    keys.slaveTimestampAsNanos.set(slaveIndex);
                }
                if (!isTimestampNano(masterType)) {
                    keys.masterTimestampAsNanos.set(masterIndex);
                }
            } else {
                keys.types.add(slaveType);
            }
        }
        if (masterSymbols.size() > 0) {
            keys.masterSymbolIndexes = masterSymbols.toArray();
            keys.slaveSymbolIndexes = slaveSymbols.toArray();
        }
        keys.masterSinkClass = RecordSinkFactory.getInstanceClass(configuration, asm, masterMetadata, keys.masterColumns, null, null,
                keys.masterSymbolAsString, keys.masterStringAsVarchar, keys.masterTimestampAsNanos);
        keys.slaveSinkClass = RecordSinkFactory.getInstanceClass(configuration, asm, slaveMetadata, keys.slaveColumns, null, null,
                keys.slaveSymbolAsString, keys.slaveStringAsVarchar, keys.slaveTimestampAsNanos);
        return keys;
    }

    private boolean isVectorizable(AggregatePlan plan, RecordCursorFactory base, SqlExecutionContext executionContext) {
        final ObjList<BoundExpression> keys = plan.getGroupingExpressions();
        final ColumnExpression key = keys.size() == 1 ? vectorKey(keys.getQuick(0)) : null;
        if (key == null || !canVectorizeGroupBy(base, executionContext)) {
            return false;
        }
        for (int i = 0, n = plan.getAggregates().size(); i < n; i++) {
            if (vectorConstructor(plan.getAggregates().getQuick(i)) == null) {
                return false;
            }
        }
        return true;
    }

    /**
     * A set operation cannot share its cursor, so a domain over a shared set operation re-reads the
     * operation's leading branch instead.
     */
    private boolean prepareSharedHead(GenerationFrame frame, AggregatePlan aggregate) {
        final JoinInput source = aggregate.getSharedSource();
        final int entry = source == null ? -1 : frame.sharedSources.indexOf(source);
        if (entry < 0) {
            return false;
        }
        final int index = frame.setOperationPlans.indexOf(SqlCodeGenerator.unwrapColumnProjections(source.getInput()));
        if (index < 0 || !frame.setOperationHeads.getQuick(index).supportsSharedCursors()) {
            return false;
        }
        LogicalPlan leaf = aggregate.getInput();
        if (!(SqlCodeGenerator.unwrapColumnProjections(leaf) instanceof SetOperationPlan)) {
            return false;
        }
        while (SqlCodeGenerator.unwrapColumnProjections(leaf) instanceof SetOperationPlan operation) {
            leaf = operation.getLeft();
        }
        final RecordCursorFactory head = frame.setOperationHeads.getQuick(index);
        final RecordMetadata metadata = head.getMetadata();
        final OutputSchema output = leaf.getOutput();
        if (output.getColumnCount() != metadata.getColumnCount()) {
            return false;
        }
        for (int i = 0, n = output.getColumnCount(); i < n; i++) {
            if (output.getColumnType(i) != metadata.getColumnType(i)) {
                return false;
            }
        }
        frame.sharedHeadTarget = leaf;
        frame.sharedHeadFactory = head;
        frame.sharedHeadId = frame.sharedConsumerCounts.getQuick(entry);
        frame.sharedConsumerCounts.setQuick(entry, frame.sharedHeadId + 1);
        return true;
    }

    // Advises a GROUP BY input of the order of its column keys.
    private SortPlan remapKeyOrderAdvice(AggregatePlan aggregate, SortPlan advice) {
        if (advice == null) {
            return null;
        }
        final SortPlan mapped = sorts.next().of(aggregate.getInput(), advice.getPosition());
        for (int i = 0, n = advice.getColumnIds().size(); i < n; i++) {
            final int index = aggregate.getOutput().getColumnIndexById(advice.getColumnIds().getQuick(i));
            if (index < 0 || index >= aggregate.getGroupingExpressions().size()
                    || !(aggregate.getGroupingExpressions().getQuick(index) instanceof ColumnExpression column) || !column.isDirectReference()) {
                return null;
            }
            mapped.getColumnIds().add(column.getColumnId());
            mapped.getDirections().add(advice.getDirections().getQuick(i));
        }
        return mapped;
    }

    private int sharedConsumerCount(GenerationFrame frame, LogicalPlan plan) {
        for (int i = 0, n = frame.sharedConsumerSources.size(); i < n; i++) {
            LogicalPlan source = frame.sharedConsumerSources.getQuick(i).getInput();
            while (source != plan) {
                if (source instanceof ProjectPlan project && SqlCodeGenerator.isColumnOnlyProjection(project)) {
                    source = project.getInput();
                } else if (source instanceof SetOperationPlan operation) {
                    source = operation.getLeft();
                } else {
                    break;
                }
            }
            if (source == plan) {
                return frame.sharedConsumerTotals.getQuick(i);
            }
        }
        return 0;
    }

    private RecordCursorFactory tryPostingIndex(GenerationFrame frame, AggregatePlan plan, FunctionInstantiator instantiator, SqlExecutionContext executionContext) throws SqlException {
        if (plan.getAggregates().size() != 0 || plan.getGroupingExpressions().size() != 1
                || !(plan.getGroupingExpressions().getQuick(0) instanceof ColumnExpression key)
                || key.getDataType() != ColumnType.SYMBOL || !executionContext.isCoveringIndexEnabled()) {
            return null;
        }
        LogicalPlan source = plan.getInput();
        final BoundExpression predicate;
        if (source instanceof FilterPlan filter) {
            predicate = filter.getPredicate();
            source = filter.getInput();
        } else {
            predicate = null;
        }
        if (!(source instanceof ScanPlan scan)) {
            return null;
        }
        if (scan.hasHint(ScanPlan.HINT_NO_COVERING) || scan.hasHint(ScanPlan.HINT_NO_INDEX)) {
            return null;
        }
        final RecordCursorFactory result;
        try {
            result = generatePostingIndex(frame, plan, key, scan, predicate, instantiator, executionContext);
        } catch (Throwable th) {
            Misc.clear(frame.intervals, th);
            throw th;
        }
        return SqlCodeGenerator.clearAfter(frame.intervals, result);
    }

    private RecordCursorFactory generatePostingIndex(GenerationFrame frame, AggregatePlan plan, ColumnExpression key, ScanPlan scan,
                                                     BoundExpression predicate, FunctionInstantiator instantiator,
                                                     SqlExecutionContext executionContext) throws SqlException {
        final OutputSchema schema = scan.getOutput();
        final int nativeTimestampIndex = schema.getColumnIndexById(scan.getNativeTimestampColumnId());
        if (predicate != null && (nativeTimestampIndex < 0
                || frame.intervals.extract(predicate, scan.getNativeTimestampColumnId(), schema, instantiator, frame.expressionRewriter, executionContext) != null)) {
            return null;
        }
        final TableReader reader = ScanFactoryGenerator.getBoundReader(scan, executionContext);
        final RecordCursorFactory result;
        try {
            result = generatePostingIndexScan(plan, key, scan, predicate, reader, frame, executionContext);
        } catch (Throwable th) {
            Misc.free(reader, th);
            throw th;
        }
        return SqlCodeGenerator.closeAfter(reader, result);
    }

    private RecordCursorFactory generatePostingIndexScan(AggregatePlan plan, ColumnExpression key, ScanPlan scan, BoundExpression predicate,
                                                         TableReader reader, GenerationFrame frame, SqlExecutionContext executionContext) throws SqlException {
        final OutputSchema schema = scan.getOutput();
        final TableReaderMetadata tableMetadata = reader.getMetadata();
        final int index = scan.getSourceColumnIndexes().getQuick(schema.getColumnIndexById(key.getColumnId()));
        if (!IndexType.isPosting(tableMetadata.getColumnIndexType(index))) {
            return null;
        }
        RuntimeIntrinsicIntervalModel intervalModel = null;
        PartitionFrameCursorFactory frames = null;
        try {
            if (predicate != null) {
                intervalModel = frame.intervals.build(reader.getPartitionedBy());
                if (intervalModel == null || intervalModel.calculateIntervals(executionContext).size() == 0) {
                    final RuntimeIntrinsicIntervalModel discarded = intervalModel;
                    intervalModel = null;
                    Misc.free(discarded);
                    return null;
                }
            }
            final TableColumnMetadata column = tableMetadata.getColumnMetadata(index);
            final GenericRecordMetadata output = new GenericRecordMetadata().add(new TableColumnMetadata(
                    Chars.toString(plan.getOutput().getColumnName(0)), key.getDataType(), column.getIndexType(),
                    column.getIndexValueBlockCapacity(), column.isSymbolTableStatic(), null, column.getWriterIndex(),
                    false, 0, column.isSymbolCacheFlag(), column.getSymbolCapacity()));
            final GenericRecordMetadata frameMetadata = GenericRecordMetadata.copyOfNew(tableMetadata);
            final IntList indexes = new IntList();
            indexes.add(index);
            if (intervalModel != null) {
                final int timestampIndex = tableMetadata.getTimestampIndex();
                if (timestampIndex != index) {
                    indexes.add(timestampIndex);
                }
                final RuntimeIntrinsicIntervalModel ownedIntervalModel = intervalModel;
                intervalModel = null;
                frames = new IntervalPartitionFrameCursorFactory(scan.getTableToken(), scan.getMetadataVersion(), ownedIntervalModel,
                        timestampIndex, frameMetadata, ORDER_ASC, scan.getViewName(), scan.getViewPosition(), scan.isUpdate());
            } else {
                frames = new FullPartitionFrameCursorFactory(scan.getTableToken(), scan.getMetadataVersion(), frameMetadata,
                        ORDER_ASC, scan.getViewName(), scan.getViewPosition(), scan.isUpdate());
            }
            frames.setAuthorizedColumnIndexes(scan.getAuthorizedColumnIndexes());
            final PartitionFrameCursorFactory ownedFrames = frames;
            frames = null;
            return new PostingIndexDistinctRecordCursorFactory(output, ownedFrames, index, 0, indexes);
        } catch (Throwable th) {
            Misc.free(frames, th);
            Misc.free(intervalModel, th);
            throw th;
        }
    }

    static boolean canParallelizeGroupBy(
            RecordCursorFactory base,
            int keyCount,
            ObjList<Function> keyFunctions,
            ObjList<GroupByFunction> groupByFunctions,
            boolean canStealFilter,
            SqlExecutionContext executionContext
    ) {
        return executionContext.isParallelGroupByEnabled()
                && !(keyCount == 0 && GroupByUtils.isEarlyExitSupported(groupByFunctions) && base.getFilter() == null)
                && SqlUtil.isParallelismSupported(keyFunctions)
                && GroupByUtils.isParallelismSupported(groupByFunctions)
                && (base.supportsPageFrameCursor() || canStealFilter);
    }

    static boolean canVectorizeGroupBy(RecordCursorFactory base, SqlExecutionContext executionContext) {
        // Rosti reads raw page addresses without the row-wise type conversion used by Async group-by.
        return executionContext.isParallelGroupByEnabled()
                && base.supportsPageFrameCursor()
                && !base.hasParquetConvertedColumns(executionContext);
    }

    static void closeWorkers(ObjList<? extends ObjList<? extends Function>> workers, Throwable primary) {
        if (workers != null) {
            for (int i = 0, n = workers.size(); i < n; i++) {
                PerWorkerFunctionList.close(workers.getQuick(i), primary);
            }
        }
    }

    static FilterPlan findFilterPlan(LogicalPlan input) {
        while (input instanceof ProjectPlan project) {
            final OutputSchema source = project.getInput().getOutput();
            if (project.getExpressions().size() != source.getColumnCount()) {
                return null;
            }
            for (int i = 0, n = source.getColumnCount(); i < n; i++) {
                if (!(project.getExpressions().getQuick(i) instanceof ColumnExpression column)
                        || column.getColumnId() != source.getColumnId(i)
                        || project.getOutput().getColumnType(i) != source.getColumnType(i)) {
                    return null;
                }
            }
            input = project.getInput();
        }
        return input instanceof FilterPlan filter ? filter : null;
    }

    // Callers establish the selected aggregate and direct-column shape before this lookup.
    static VectorAggregateFunctionConstructor getVectorAggregateConstructor(
            CharSequence name, int argumentType, boolean isCountAll
    ) {
        if (isSumKeyword(name)) {
            return sumConstructors.get(argumentType);
        }
        if (isCountKeyword(name)) {
            return isCountAll ? COUNT_CONSTRUCTOR : countConstructors.get(argumentType);
        }
        if (Chars.equalsIgnoreCase(name, "ksum")) {
            return ksumConstructors.get(argumentType);
        }
        if (Chars.equalsIgnoreCase(name, "nsum")) {
            return nsumConstructors.get(argumentType);
        }
        if (Chars.equalsIgnoreCase(name, "avg")) {
            return avgConstructors.get(argumentType);
        }
        if (Chars.equalsIgnoreCase(name, "min")) {
            return minConstructors.get(argumentType);
        }
        if (Chars.equalsIgnoreCase(name, "max")) {
            return maxConstructors.get(argumentType);
        }
        return null;
    }

    static boolean isTimeSeriesDistinct(RecordCursorFactory base) {
        return base.recordCursorSupportsRandomAccess() && base.getMetadata().getTimestampIndex() >= 0;
    }

    void countSharedConsumers(GenerationFrame frame, LogicalPlan plan) {
        if (plan instanceof AggregatePlan aggregate && aggregate.getSharedSource() != null) {
            final int index = frame.sharedConsumerSources.indexOf(aggregate.getSharedSource());
            if (index < 0) {
                frame.sharedConsumerSources.add(aggregate.getSharedSource());
                frame.sharedConsumerTotals.add(1);
            } else {
                frame.sharedConsumerTotals.increment(index);
            }
        }
        for (int i = 0, n = plan.inputCount(); i < n; i++) {
            countSharedConsumers(frame, plan.inputAt(i));
        }
    }

    /**
     * Consumes the input on entry, including on failure.
     */
    RecordCursorFactory generate(
            GenerationFrame frame,
            AggregatePlan plan,
            RecordCursorFactory base,
            int timestampIndex,
            FunctionInstantiator instantiator,
            int sharedConsumerCount,
            SqlExecutionContext executionContext
    ) throws SqlException {
        boolean isAdopted = false;
        try {
            final ObjList<FunctionExpression> aggregates = plan.getAggregates();
            if (plan.getGroupingExpressions().size() == 0 && aggregates.size() == 0) {
                // DISTINCT of constants uses global aggregation even over an empty
                // input. Its outer projection owns the values; COUNT supplies the row.
                final GenericRecordMetadata metadata = new GenericRecordMetadata();
                isAdopted = true;
                return new CountRecordCursorFactory(metadata, base);
            }
            if (plan.getGroupingExpressions().size() == 0 && aggregates.size() == 1) {
                final FunctionExpression call = aggregates.getQuick(0);
                if (call.getArgumentCount() == 0 && call.isAggregate()
                        && SqlKeywords.isCountKeyword(call.getName())) {
                    final CharSequence name = plan.getOutput().getColumnName(0);
                    final RecordMetadata metadata = SqlKeywords.isCountKeyword(name)
                            ? CountRecordCursorFactory.DEFAULT_COUNT_METADATA
                            : new GenericRecordMetadata().add(new TableColumnMetadata(Chars.toString(name), ColumnType.LONG));
                    if (base instanceof SelectedRecordCursorFactory selected && selected.getMetadata().getColumnCount() == 0) {
                        base = selected.getBaseFactory();
                    }
                    isAdopted = true;
                    return new CountRecordCursorFactory(metadata, base);
                }
            }
            final RecordCursorFactory posting = tryPostingIndex(frame, plan, instantiator, executionContext);
            if (posting != null) {
                // This source is a native scan, so replacing its unopened cursor has no
                // table-function or scalar-construction effects to replay.
                final RecordCursorFactory discarded = base;
                base = null;
                try {
                    discarded.close();
                } catch (Throwable th) {
                    Misc.free(posting, th);
                    throw th;
                }
                return posting;
            }
            if (isVectorizable(plan, base, executionContext)) {
                isAdopted = true;
                return generateVector(plan, base, executionContext);
            }
            isAdopted = true;
            return generateFunctions(frame, plan, base, timestampIndex, instantiator, sharedConsumerCount, executionContext);
        } catch (Throwable th) {
            if (!isAdopted) {
                Misc.free(base, th);
            }
            throw th;
        }
    }

    RecordCursorFactory generateAggregate(GenerationFrame frame, AggregatePlan aggregate, SortPlan orderAdvice, SqlExecutionContext executionContext,
                                          int requiredOrderColumnId, int requiredScanDirection) throws SqlException {
        if (aggregate.getInput() instanceof HorizonJoinPlan horizon) {
            return generateHorizonJoin(frame, aggregate, horizon, executionContext);
        }
        int inputMnemonic = OrderByMnemonic.ORDER_BY_INVARIANT;
        for (int i = 0, n = aggregate.getAggregates().size(); i < n; i++) {
            if (aggregate.getAggregates().getQuick(i).getOverload().isOrderSensitiveAggregate()) {
                inputMnemonic = OrderByMnemonic.ORDER_BY_REQUIRED;
                break;
            }
        }
        RecordCursorFactory shared = generateSharedInput(frame, aggregate);
        if (shared == null && prepareSharedHead(frame, aggregate)) {
            try {
                shared = codeGenerator.generate(frame, aggregate.getInput(), executionContext, -1, RecordCursorFactory.SCAN_DIRECTION_OTHER, null, null, inputMnemonic);
            } finally {
                frame.sharedHeadTarget = null;
            }
        }
        int inputOrderId = -1;
        final int orderIndex = aggregate.getAggregates().size() == 0 ? aggregate.getOutput().getColumnIndexById(requiredOrderColumnId) : -1;
        if (orderIndex >= 0 && orderIndex < aggregate.getGroupingExpressions().size()
                && aggregate.getGroupingExpressions().getQuick(orderIndex) instanceof ColumnExpression key && key.isDirectReference()) {
            inputOrderId = key.getColumnId();
        }
        final LogicalPlan input = skipRenames(aggregate.getInput());
        final boolean isTimestampDeclared = shared == null && SqlCodeGenerator.isTimestampDeclarationOnly(input);
        final RecordCursorFactory base = shared != null ? shared : codeGenerator.generate(frame, isTimestampDeclared ? input.inputAt(0) : input,
                executionContext, inputOrderId, inputOrderId < 0 ? RecordCursorFactory.SCAN_DIRECTION_OTHER : requiredScanDirection,
                remapKeyOrderAdvice(aggregate, orderAdvice), null, inputMnemonic);
        final int timestampIndex = isTimestampDeclared ? input.getOutput().getTimestampIndex() : base.getMetadata().getTimestampIndex();
        return generate(frame, aggregate, base, timestampIndex, frame.functionInstantiator, sharedConsumerCount(frame, aggregate), executionContext);
    }

    RecordCursorFactory generateDistinct(GenerationFrame frame, DistinctPlan distinct, LimitPlan limitAdvice, SqlExecutionContext executionContext) throws SqlException {
        // DISTINCT keeps its input order. A downstream sort must not turn the
        // timestamp-specialized factory's forward input into a backward scan.
        final RecordCursorFactory base = codeGenerator.generate(frame, distinct.getInput(), executionContext);
        Function lo = null;
        Function hi = null;
        if (limitAdvice != null && !isTimeSeriesDistinct(base)) {
            try {
                lo = frame.functionInstantiator.instantiate(limitAdvice.getLo(), emptySchema, executionContext);
                if (limitAdvice.getHi() != null) {
                    hi = frame.functionInstantiator.instantiate(limitAdvice.getHi(), emptySchema, executionContext);
                }
            } catch (Throwable th) {
                Misc.free(lo, th);
                Misc.free(base, th);
                throw th;
            }
        }
        return generateDistinct(base, lo, hi);
    }

    /**
     * Consumes the input and optional LIMIT advice on entry, including on failure.
     */
    RecordCursorFactory generateDistinct(RecordCursorFactory base, Function loAdvice, Function hiAdvice) {
        boolean isAdopted = false;
        try {
            if (isTimeSeriesDistinct(base)) {
                assert loAdvice == null && hiAdvice == null;
                isAdopted = true;
                return new DistinctTimeSeriesRecordCursorFactory(configuration, base, entityColumnFilter, asm);
            }
            // Both constructors consume their inputs even when initialization fails.
            isAdopted = true;
            return new DistinctRecordCursorFactory(configuration, base, entityColumnFilter, asm, loAdvice, hiAdvice);
        } catch (Throwable th) {
            if (!isAdopted) {
                Misc.free(base, th);
                Misc.free(loAdvice, th);
                if (hiAdvice != loAdvice) {
                    Misc.free(hiAdvice, th);
                }
            }
            throw th;
        }
    }

    /**
     * Consumes the prepared functions, filter resources and input on entry.
     */
    RecordCursorFactory generateGroupBy(
            RecordCursorFactory base,
            RecordMetadata metadata,
            ListColumnFilter columnFilter,
            ArrayColumnTypes preparedKeyTypes,
            ArrayColumnTypes preparedValueTypes,
            ObjList<GroupByFunction> groupByFunctions,
            ObjList<ObjList<GroupByFunction>> workerGroupByFunctions,
            ObjList<Function> keyFunctions,
            ObjList<ObjList<Function>> workerKeyFunctions,
            ObjList<Function> recordFunctions,
            CompiledFilter compiledFilter,
            MemoryCARW bindVariableMemory,
            ObjList<Function> bindVariables,
            Function filter,
            IntHashSet filterColumnIndexes,
            ObjList<Function> workerFilters,
            ObjList<ObjList<Function>> sharedRecordFunctions,
            boolean isParallel,
            SqlExecutionContext executionContext
    ) {
        if (preparedKeyTypes.getColumnCount() == 0) {
            assert keyFunctions.size() == 0;
            assert recordFunctions.size() == groupByFunctions.size();
            if (isParallel) {
                return new AsyncGroupByNotKeyedRecordCursorFactory(
                        executionContext.getCairoEngine(), asm, configuration, executionContext.getMessageBus(), base,
                        metadata, groupByFunctions, workerGroupByFunctions, preparedValueTypes.getColumnCount(),
                        compiledFilter, bindVariableMemory, bindVariables, filter, filterColumnIndexes,
                        workerFilters, executionContext.getSharedQueryWorkerCount(), sharedRecordFunctions
                );
            }
            return new GroupByNotKeyedRecordCursorFactory(
                    asm, configuration, base, metadata, groupByFunctions, preparedValueTypes.getColumnCount(), sharedRecordFunctions
            );
        }
        if (isParallel) {
            return new AsyncGroupByRecordCursorFactory(
                    executionContext.getCairoEngine(), asm, configuration, executionContext.getMessageBus(), base,
                    metadata, columnFilter, preparedKeyTypes, preparedValueTypes, groupByFunctions, workerGroupByFunctions,
                    keyFunctions, workerKeyFunctions, recordFunctions, compiledFilter, bindVariableMemory, bindVariables,
                    filter, filterColumnIndexes, workerFilters, executionContext.getSharedQueryWorkerCount(), sharedRecordFunctions
            );
        }
        return new io.questdb.griffin.engine.groupby.GroupByRecordCursorFactory(
                asm, configuration, base, columnFilter, preparedKeyTypes, preparedValueTypes, metadata,
                groupByFunctions, keyFunctions, recordFunctions, sharedRecordFunctions
        );
    }

    /**
     * Consumes the inputs on entry, including on failure.
     */
    RecordCursorFactory generateHorizonJoin(
            GenerationFrame frame,
            AggregatePlan plan,
            HorizonJoinPlan horizon,
            RecordCursorFactory master,
            ObjList<RecordCursorFactory> slaves,
            FunctionInstantiator instantiator,
            SqlExecutionContext executionContext
    ) throws SqlException {
        JoinRecordMetadata innerMetadata = null;
        ObjList<Function> keyFunctions = null;
        ObjList<Function> recordFunctions = null;
        ObjList<ObjList<GroupByFunction>> workerAggregates = null;
        ObjList<ObjList<Function>> workerKeys = null;
        ObjList<Function> workerFilters = null;
        Function ownedFilter = null;
        CompiledFilter compiledFilter = null;
        MemoryCARW bindVariableMemory = null;
        ObjList<Function> bindVariables = null;
        boolean isAdopted = false;
        try {
            final OutputSchema output = horizon.getOutput();
            innerMetadata = new JoinRecordMetadata(configuration, output.getColumnCount());
            final RecordMetadata masterMetadata = master.getMetadata();
            for (int i = 0, n = masterMetadata.getColumnCount(); i < n; i++) {
                innerMetadata.add(horizon.getMasterAlias(), masterMetadata.getColumnMetadata(i));
            }
            final int offsetIndex = masterMetadata.getColumnCount();
            innerMetadata.add(horizon.getHorizonAlias(), new TableColumnMetadata(Chars.toString(output.getColumnName(offsetIndex)), ColumnType.LONG));
            innerMetadata.add(horizon.getHorizonAlias(), new TableColumnMetadata(Chars.toString(output.getColumnName(offsetIndex + 1)),
                    output.getColumnType(offsetIndex + 1)));
            for (int s = 0, m = slaves.size(); s < m; s++) {
                final RecordMetadata slaveMetadata = slaves.getQuick(s).getMetadata();
                for (int i = 0, n = slaveMetadata.getColumnCount(); i < n; i++) {
                    innerMetadata.add(horizon.getSlaves().getQuick(s).getAlias(), slaveMetadata.getColumnMetadata(i));
                }
            }
            innerMetadata.setTimestampIndex(masterMetadata.getTimestampIndex());
            final ObjList<GroupByFunction> aggregates = new ObjList<>(plan.getAggregates().size());
            keyFunctions = new ObjList<>(plan.getGroupingExpressions().size());
            recordFunctions = new ObjList<>(plan.getOutput().getColumnCount());
            final ArrayColumnTypes keyTypes = frame.keyTypes;
            final ArrayColumnTypes valueTypes = frame.valueTypes;
            final ListColumnFilter columnFilter = frame.listColumnFilterA;
            keyTypes.clear();
            valueTypes.clear();
            columnFilter.clear();
            assemble(plan, innerMetadata, masterMetadata.getTimestampIndex(), false, instantiator, executionContext,
                    aggregates, keyFunctions, recordFunctions, keyTypes, valueTypes, columnFilter);
            final GenericRecordMetadata metadata = metadata(plan, innerMetadata, recordFunctions);
            final boolean isEnabled = executionContext.isParallelHorizonJoinEnabled();
            final FilterPlan filterPlan = findFilterPlan(horizon.getMaster());
            final boolean isFilterStealable = isEnabled && filterPlan != null && !master.supportsPageFrameCursor()
                    && (FilterFactoryGenerator.isParallelFilter(master) || !master.implementsLimit() && master instanceof FilteredRecordCursorFactory
                    && filterPlan.getInput() instanceof ScanPlan) && master.getBaseFactory().supportsPageFrameCursor();
            final boolean isParallel = isEnabled && (master.supportsPageFrameCursor() || isFilterStealable)
                    && SqlUtil.isParallelismSupported(keyFunctions) && GroupByUtils.isParallelismSupported(aggregates);
            IntHashSet filterIndexes = null;
            if (isParallel) {
                workerAggregates = workerAggregates(plan, aggregates, innerMetadata, instantiator, executionContext);
                workerKeys = workerKeys(plan, keyFunctions, innerMetadata, instantiator, executionContext);
                if (isFilterStealable) {
                    final Function filter = FilterFactoryGenerator.stolenFilter(master);
                    final BoundExpression predicate = stolenPredicate(frame, filterPlan, master);
                    workerFilters = FilterFactoryGenerator.compileWorkers(predicate, filterPlan.getInput().getOutput(), masterMetadata, filter, instantiator, executionContext);
                    filterIndexes = new IntHashSet();
                    FilterFactoryGenerator.collectColumnIndexes(predicate, filterPlan.getInput().getOutput(), filterIndexes);
                    filterIndexes.add(masterMetadata.getTimestampIndex());
                    master.halfClose();
                    ownedFilter = filter;
                    compiledFilter = master.getCompiledFilter();
                    bindVariableMemory = master.getBindVarMemory();
                    bindVariables = master.getBindVarFunctions();
                    master = master.getBaseFactory();
                }
            }
            isAdopted = true;
            return generateHorizonJoin(horizon, master, slaves, innerMetadata, metadata, columnFilter, keyTypes,
                    valueTypes, aggregates, workerAggregates, keyFunctions, workerKeys, recordFunctions, compiledFilter,
                    bindVariableMemory, bindVariables, ownedFilter, filterIndexes, workerFilters, isParallel, executionContext);
        } catch (Throwable th) {
            if (!isAdopted) {
                closeWorkers(workerAggregates, th);
                closeWorkers(workerKeys, th);
                Misc.freeObjList(workerFilters, th);
                Misc.freeObjList(recordFunctions, th);
                Misc.freeObjList(keyFunctions, th);
                Misc.free(ownedFilter, th);
                Misc.free(compiledFilter, th);
                Misc.free(bindVariableMemory, th);
                Misc.freeObjList(bindVariables, th);
                Misc.free(innerMetadata, th);
                Misc.freeObjList(slaves, th);
                Misc.free(master, th);
            }
            throw th;
        }
    }

    RecordCursorFactory generateHorizonJoin(GenerationFrame frame, AggregatePlan aggregate, HorizonJoinPlan horizon, SqlExecutionContext executionContext)
            throws SqlException {
        final int slaveCount = horizon.getSlaves().size();
        final ObjList<RecordCursorFactory> slaves = new ObjList<>(slaveCount);
        final RecordCursorFactory master = codeGenerator.generateJoinInput(frame, horizon.getMaster(), executionContext, true, OrderByMnemonic.ORDER_BY_REQUIRED);
        try {
            for (int i = 0; i < slaveCount; i++) {
                slaves.add(codeGenerator.generateJoinInput(frame, horizon.getSlaves().getQuick(i).getInput(), executionContext, true, OrderByMnemonic.ORDER_BY_REQUIRED));
            }
        } catch (Throwable th) {
            Misc.freeObjList(slaves, th);
            Misc.free(master, th);
            throw th;
        }
        return generateHorizonJoin(frame, aggregate, horizon, master, slaves, frame.functionInstantiator, executionContext);
    }

    /**
     * Consumes the inputs, the inner metadata and every function and filter resource on entry, including on failure.
     */
    RecordCursorFactory generateHorizonJoin(
            HorizonJoinPlan plan,
            RecordCursorFactory master,
            ObjList<RecordCursorFactory> slaves,
            JoinRecordMetadata innerMetadata,
            RecordMetadata metadata,
            ListColumnFilter columnFilter,
            ArrayColumnTypes keyTypes,
            ArrayColumnTypes valueTypes,
            ObjList<GroupByFunction> groupByFunctions,
            ObjList<ObjList<GroupByFunction>> workerGroupByFunctions,
            ObjList<Function> keyFunctions,
            ObjList<ObjList<Function>> workerKeyFunctions,
            ObjList<Function> recordFunctions,
            CompiledFilter compiledFilter,
            MemoryCARW bindVariableMemory,
            ObjList<Function> bindVariables,
            Function filter,
            IntHashSet filterColumnIndexes,
            ObjList<Function> workerFilters,
            boolean isParallel,
            SqlExecutionContext executionContext
    ) throws SqlException {
        ObjList<HorizonJoinSlaveState> slaveStates = null;
        boolean isAdopted = false;
        try {
            final RecordMetadata masterMetadata = master.getMetadata();
            final ObjList<HorizonJoinSlave> steps = plan.getSlaves();
            final int slaveCount = slaves.size();
            for (int s = 0; s < slaveCount; s++) {
                final RecordCursorFactory slave = slaves.getQuick(s);
                final int position = steps.getQuick(s).getPosition();
                assert masterMetadata.getTimestampIndex() >= 0 && slave.getMetadata().getTimestampIndex() >= 0;
                JoinFactoryGenerator.validateBothTimestampOrders(master, slave, position);
                if (!slave.supportsTimeFrameCursor()) {
                    throw SqlException.position(position).put("right-hand side of HORIZON JOIN can only be a table with an optional filter");
                }
            }
            if (!isParallel && !master.recordCursorSupportsRandomAccess()) {
                throw SqlException.position(steps.getQuick(0).getPosition()).put("left-hand side of HORIZON JOIN can only be a table with an optional filter");
            }
            final LongList offsetValues = plan.getOffsetValues();
            final long[] offsets = new long[offsetValues.size()];
            for (int i = 0, n = offsets.length; i < n; i++) {
                offsets[i] = offsetValues.getQuick(i);
            }
            final int masterTimestampIndex = masterMetadata.getTimestampIndex();
            final int masterColumnCount = masterMetadata.getColumnCount();
            final int[] columnSources = new int[innerMetadata.getColumnCount()];
            final int[] columnIndices = new int[columnSources.length];
            int column = 0;
            for (int i = 0; i < masterColumnCount; i++, column++) {
                columnSources[column] = MultiHorizonJoinRecord.SOURCE_MASTER;
                columnIndices[column] = i;
            }
            for (int i = 0; i < 2; i++, column++) {
                columnSources[column] = MultiHorizonJoinRecord.SOURCE_SEQUENCE;
                columnIndices[column] = i;
            }
            final ObjList<HorizonJoinKeys> keys = horizonKeys;
            keys.clear();
            for (int s = 0; s < slaveCount; s++) {
                final RecordMetadata slaveMetadata = slaves.getQuick(s).getMetadata();
                for (int i = 0, n = slaveMetadata.getColumnCount(); i < n; i++, column++) {
                    columnSources[column] = MultiHorizonJoinRecord.SOURCE_SLAVE_BASE + s;
                    columnIndices[column] = i;
                }
                keys.add(horizonJoinKeys(steps.getQuick(s), plan.getMaster().getOutput(), masterMetadata, slaveMetadata));
            }
            final boolean isKeyed = keyTypes.getColumnCount() > 0;
            final int workerCount = executionContext.getSharedQueryWorkerCount();
            if (isParallel) {
                master.changePageFrameSizes(configuration.getSqlSmallPageFrameMinRows(), configuration.getSqlSmallPageFrameMaxRows());
            }
            if (slaveCount == 1) {
                final HorizonJoinKeys key = keys.getQuick(0);
                final RecordCursorFactory slave = slaves.getQuick(0);
                final RecordMetadata slaveMetadata = slave.getMetadata();
                final ArrayColumnTypes joinKeyTypes = key == null ? null : key.types;
                final int[] masterSymbols = key == null ? null : key.masterSymbolIndexes;
                final int[] slaveSymbols = key == null ? null : key.slaveSymbolIndexes;
                if (!isParallel) {
                    final RecordSink masterSink = key == null ? null : RecordSinkFactory.getInstance(key.masterSinkClass, masterMetadata,
                            key.masterColumns, null, null, key.masterSymbolAsString, key.masterStringAsVarchar, key.masterTimestampAsNanos);
                    final RecordSink slaveSink = key == null ? null : RecordSinkFactory.getInstance(key.slaveSinkClass, slaveMetadata,
                            key.slaveColumns, null, null, key.slaveSymbolAsString, key.slaveStringAsVarchar, key.slaveTimestampAsNanos);
                    isAdopted = true;
                    return isKeyed
                            ? new HorizonJoinRecordCursorFactory(configuration, asm, metadata, innerMetadata, master, slave, offsets,
                            masterTimestampIndex, groupByFunctions, recordFunctions, keyFunctions, keyTypes, valueTypes, joinKeyTypes,
                            masterSink, slaveSink, masterColumnCount, masterSymbols, slaveSymbols, columnFilter, columnSources, columnIndices)
                            : new HorizonJoinNotKeyedRecordCursorFactory(configuration, asm, metadata, innerMetadata, master, slave, offsets,
                            masterTimestampIndex, groupByFunctions, valueTypes.getColumnCount(), joinKeyTypes, masterSink, slaveSink,
                            masterColumnCount, masterSymbols, slaveSymbols, columnSources, columnIndices);
                }
                final AsyncHorizonJoinResources resources = new AsyncHorizonJoinResources(workerGroupByFunctions, workerKeyFunctions,
                        compiledFilter, bindVariableMemory, bindVariables, filter, filterColumnIndexes, workerFilters);
                final Class<RecordSink> masterSinkClass = key == null ? null : key.masterSinkClass;
                final Class<RecordSink> slaveSinkClass = key == null ? null : key.slaveSinkClass;
                isAdopted = true;
                return isKeyed
                        ? new AsyncHorizonJoinRecordCursorFactory(configuration, asm, executionContext.getCairoEngine(),
                        executionContext.getMessageBus(), metadata, innerMetadata, master, slave, offsets, masterTimestampIndex,
                        groupByFunctions, recordFunctions, keyFunctions, keyTypes, valueTypes, joinKeyTypes, masterSinkClass,
                        slaveSinkClass, masterColumnCount, masterSymbols, slaveSymbols, columnFilter, columnSources, columnIndices,
                        resources, workerCount)
                        : new AsyncHorizonJoinNotKeyedRecordCursorFactory(configuration, asm, executionContext.getCairoEngine(),
                        executionContext.getMessageBus(), metadata, innerMetadata, master, slave, offsets, masterTimestampIndex,
                        groupByFunctions, valueTypes.getColumnCount(), joinKeyTypes, masterSinkClass, slaveSinkClass,
                        masterColumnCount, masterSymbols, slaveSymbols, columnSources, columnIndices, resources, workerCount);
            }
            final ColumnTypes[] joinKeyTypes = new ColumnTypes[slaveCount];
            @SuppressWarnings("unchecked") final Class<RecordSink>[] masterSinkClasses = new Class[slaveCount];
            @SuppressWarnings("unchecked") final Class<RecordSink>[] slaveSinkClasses = new Class[slaveCount];
            final int masterTimestampType = masterMetadata.getTimestampType();
            slaveStates = new ObjList<>(slaveCount);
            for (int s = 0; s < slaveCount; s++) {
                final HorizonJoinKeys key = keys.getQuick(s);
                final RecordCursorFactory slave = slaves.getQuick(s);
                final int slaveTimestampType = slave.getMetadata().getTimestampType();
                final boolean isScaled = masterTimestampType != slaveTimestampType;
                if (key != null) {
                    joinKeyTypes[s] = key.types;
                    masterSinkClasses[s] = key.masterSinkClass;
                    slaveSinkClasses[s] = key.slaveSinkClass;
                }
                slaveStates.add(new HorizonJoinSlaveState(slave,
                        isScaled ? ColumnType.getTimestampDriver(masterTimestampType).toNanosScale() : 1,
                        isScaled ? ColumnType.getTimestampDriver(slaveTimestampType).toNanosScale() : 1,
                        joinKeyTypes[s], masterColumnCount,
                        key == null ? null : key.masterSymbolIndexes, key == null ? null : key.slaveSymbolIndexes));
                slaves.setQuick(s, null);
            }
            if (!isParallel) {
                isAdopted = true;
                return isKeyed
                        ? new MultiHorizonJoinRecordCursorFactory(configuration, asm, metadata, innerMetadata, master, slaveStates,
                        masterSinkClasses, slaveSinkClasses, offsets, masterTimestampIndex, groupByFunctions, recordFunctions,
                        keyFunctions, keyTypes, valueTypes, columnFilter, columnSources, columnIndices)
                        : new MultiHorizonJoinNotKeyedRecordCursorFactory(configuration, asm, metadata, innerMetadata, master, slaveStates,
                        masterSinkClasses, slaveSinkClasses, offsets, masterTimestampIndex, groupByFunctions,
                        valueTypes.getColumnCount(), columnSources, columnIndices);
            }
            final AsyncHorizonJoinResources resources = new AsyncHorizonJoinResources(workerGroupByFunctions, workerKeyFunctions,
                    compiledFilter, bindVariableMemory, bindVariables, filter, filterColumnIndexes, workerFilters);
            isAdopted = true;
            return isKeyed
                    ? new AsyncMultiHorizonJoinRecordCursorFactory(configuration, asm, executionContext.getCairoEngine(),
                    executionContext.getMessageBus(), metadata, innerMetadata, master, slaveStates, joinKeyTypes, masterSinkClasses,
                    slaveSinkClasses, offsets, masterTimestampIndex, groupByFunctions, recordFunctions, keyFunctions, keyTypes,
                    valueTypes, columnFilter, columnSources, columnIndices, resources, workerCount)
                    : new AsyncMultiHorizonJoinNotKeyedRecordCursorFactory(configuration, asm, executionContext.getCairoEngine(),
                    executionContext.getMessageBus(), metadata, innerMetadata, master, slaveStates, joinKeyTypes, masterSinkClasses,
                    slaveSinkClasses, offsets, masterTimestampIndex, groupByFunctions, valueTypes.getColumnCount(), columnSources,
                    columnIndices, resources, workerCount);
        } catch (Throwable th) {
            if (!isAdopted) {
                Misc.free(innerMetadata, th);
                AggregateFactoryGenerator.closeWorkers(workerGroupByFunctions, th);
                AggregateFactoryGenerator.closeWorkers(workerKeyFunctions, th);
                Misc.freeObjList(workerFilters, th);
                Misc.freeObjList(recordFunctions, th);
                Misc.freeObjList(keyFunctions, th);
                Misc.free(filter, th);
                Misc.free(compiledFilter, th);
                Misc.free(bindVariableMemory, th);
                Misc.freeObjList(bindVariables, th);
                Misc.freeObjList(slaveStates, th);
                Misc.freeObjList(slaves, th);
                Misc.free(master, th);
            }
            throw th;
        } finally {
            horizonKeys.clear();
        }
    }

    /**
     * Consumes the aggregate functions and input on entry.
     */
    RecordCursorFactory generateVectorGroupBy(
            RecordCursorFactory base,
            RecordMetadata metadata,
            ArrayColumnTypes columnTypes,
            ObjList<VectorAggregateFunction> functions,
            int keyKind,
            int inputKeyIndex,
            IntList symbolIndexes,
            SqlExecutionContext executionContext
    ) {
        boolean isAdopted = false;
        try {
            if (functions.size() == 0) {
                functions.add(new CountVectorAggregateFunction(keyKind));
            }
            for (int i = 0, n = functions.size(); i < n; i++) {
                functions.getQuick(i).pushValueTypes(columnTypes);
            }
            isAdopted = true;
            return new GroupByRecordCursorFactory(
                    executionContext.getCairoEngine(), configuration, base, metadata, columnTypes,
                    executionContext.getSharedQueryWorkerCount(), functions, inputKeyIndex, 0, symbolIndexes
            );
        } catch (Throwable th) {
            if (!isAdopted) {
                Misc.freeObjList(functions, th);
                Misc.free(base, th);
            }
            throw th;
        }
    }

    /**
     * Container class to hold the detected parameters of a markout horizon pattern.
     */
    private static final class HorizonJoinKeys {
        private final ListColumnFilter masterColumns = new ListColumnFilter();
        private final BitSet masterStringAsVarchar = new BitSet();
        private final BitSet masterSymbolAsString = new BitSet();
        private final BitSet masterTimestampAsNanos = new BitSet();
        private final ListColumnFilter slaveColumns = new ListColumnFilter();
        private final BitSet slaveStringAsVarchar = new BitSet();
        private final BitSet slaveSymbolAsString = new BitSet();
        private final BitSet slaveTimestampAsNanos = new BitSet();
        private final ArrayColumnTypes types = new ArrayColumnTypes();
        private Class<RecordSink> masterSinkClass;
        private int[] masterSymbolIndexes;
        private Class<RecordSink> slaveSinkClass;
        private int[] slaveSymbolIndexes;
    }

    static {
        countConstructors.put(DOUBLE, CountDoubleVectorAggregateFunction::new);
        countConstructors.put(INT, CountIntVectorAggregateFunction::new);
        countConstructors.put(LONG, CountLongVectorAggregateFunction::new);
        countConstructors.put(DATE, CountLongVectorAggregateFunction::new);
        countConstructors.put(TIMESTAMP_MICRO, CountLongVectorAggregateFunction::new);
        countConstructors.put(TIMESTAMP_NANO, CountLongVectorAggregateFunction::new);

        sumConstructors.put(DOUBLE, SumDoubleVectorAggregateFunction::new);
        sumConstructors.put(INT, SumIntVectorAggregateFunction::new);
        sumConstructors.put(LONG, SumLongVectorAggregateFunction::new);
        sumConstructors.put(LONG256, SumLong256VectorAggregateFunction::new);
        sumConstructors.put(SHORT, SumShortVectorAggregateFunction::new);

        ksumConstructors.put(DOUBLE, KSumDoubleVectorAggregateFunction::new);
        nsumConstructors.put(DOUBLE, NSumDoubleVectorAggregateFunction::new);

        avgConstructors.put(DOUBLE, AvgDoubleVectorAggregateFunction::new);
        avgConstructors.put(LONG, AvgLongVectorAggregateFunction::new);
        avgConstructors.put(INT, AvgIntVectorAggregateFunction::new);
        avgConstructors.put(SHORT, AvgShortVectorAggregateFunction::new);

        minConstructors.put(DOUBLE, MinDoubleVectorAggregateFunction::new);
        minConstructors.put(LONG, MinLongVectorAggregateFunction::new);
        minConstructors.put(DATE, MinDateVectorAggregateFunction::new);
        minConstructors.put(TIMESTAMP_MICRO, (int keyKind, int columnIndex, int timestampIndex, int _) -> new MinTimestampVectorAggregateFunction(keyKind, columnIndex, TIMESTAMP_MICRO, timestampIndex));
        minConstructors.put(TIMESTAMP_NANO, (int keyKind, int columnIndex, int timestampIndex, int _) -> new MinTimestampVectorAggregateFunction(keyKind, columnIndex, TIMESTAMP_NANO, timestampIndex));
        minConstructors.put(INT, MinIntVectorAggregateFunction::new);
        minConstructors.put(SHORT, MinShortVectorAggregateFunction::new);

        maxConstructors.put(DOUBLE, MaxDoubleVectorAggregateFunction::new);
        maxConstructors.put(LONG, MaxLongVectorAggregateFunction::new);
        maxConstructors.put(DATE, MaxDateVectorAggregateFunction::new);
        maxConstructors.put(TIMESTAMP_MICRO, (int keyKind, int columnIndex, int timestampIndex, int _) -> new MaxTimestampVectorAggregateFunction(keyKind, columnIndex, TIMESTAMP_MICRO, timestampIndex));
        maxConstructors.put(TIMESTAMP_NANO, (int keyKind, int columnIndex, int timestampIndex, int _) -> new MaxTimestampVectorAggregateFunction(keyKind, columnIndex, TIMESTAMP_NANO, timestampIndex));
        maxConstructors.put(INT, MaxIntVectorAggregateFunction::new);
        maxConstructors.put(SHORT, MaxShortVectorAggregateFunction::new);
    }
}
