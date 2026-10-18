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

package io.questdb.griffin.codegen;

import io.questdb.ParanoiaState;
import io.questdb.cairo.ArrayColumnTypes;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.ColumnTypes;
import io.questdb.cairo.EntityColumnFilter;
import io.questdb.cairo.GenericRecordMetadata;
import io.questdb.cairo.IndexType;
import io.questdb.cairo.ListColumnFilter;
import io.questdb.cairo.RecordSink;
import io.questdb.cairo.RecordSinkTemplate;
import io.questdb.cairo.TableColumnMetadata;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.vm.api.MemoryCARW;
import io.questdb.griffin.FunctionInstantiator;
import io.questdb.griffin.LogicalPlans;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlKeywords;
import io.questdb.griffin.SqlUtil;
import io.questdb.griffin.engine.functions.GroupByFunction;
import io.questdb.griffin.engine.functions.PerWorkerFunctionList;
import io.questdb.griffin.engine.functions.SymbolFunction;
import io.questdb.griffin.engine.functions.cast.CastStrToSymbolFunctionFactory;
import io.questdb.griffin.engine.groupby.CountRecordCursorFactory;
import io.questdb.griffin.engine.groupby.DistinctRecordCursorFactory;
import io.questdb.griffin.engine.groupby.DistinctTimeSeriesRecordCursorFactory;
import io.questdb.griffin.engine.groupby.GroupByNotKeyedRecordCursorFactory;
import io.questdb.griffin.engine.groupby.GroupByUtils;
import io.questdb.griffin.engine.groupby.TimestampEndpointRecordCursorFactory;
import io.questdb.griffin.engine.groupby.vect.CountVectorAggregateFunction;
import io.questdb.griffin.engine.groupby.vect.GroupByRecordCursorFactory;
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
import io.questdb.griffin.engine.table.HorizonJoinNotKeyedRecordCursorFactory;
import io.questdb.griffin.engine.table.HorizonJoinRecordCursorFactory;
import io.questdb.griffin.engine.table.HorizonJoinSlaveState;
import io.questdb.griffin.engine.table.MultiHorizonJoinNotKeyedRecordCursorFactory;
import io.questdb.griffin.engine.table.MultiHorizonJoinRecord;
import io.questdb.griffin.engine.table.MultiHorizonJoinRecordCursorFactory;
import io.questdb.griffin.engine.table.SelectedRecordCursorFactory;
import io.questdb.griffin.plan.logical.AggregatePlan;
import io.questdb.griffin.plan.logical.BoundExpression;
import io.questdb.griffin.plan.logical.ColumnExpression;
import io.questdb.griffin.plan.logical.DistinctPlan;
import io.questdb.griffin.plan.logical.FilterPlan;
import io.questdb.griffin.plan.logical.FunctionExpression;
import io.questdb.griffin.plan.logical.GroupingPlan;
import io.questdb.griffin.plan.logical.HorizonJoinPlan;
import io.questdb.griffin.plan.logical.HorizonJoinSlave;
import io.questdb.griffin.plan.logical.JoinInput;
import io.questdb.griffin.plan.logical.LimitPlan;
import io.questdb.griffin.plan.logical.LogicalPlan;
import io.questdb.griffin.plan.logical.OutputSchema;
import io.questdb.griffin.plan.logical.PhysicalProperties;
import io.questdb.griffin.plan.logical.PlanToFactoryRules;
import io.questdb.griffin.plan.logical.ProjectPlan;
import io.questdb.griffin.plan.logical.SampleByPlan;
import io.questdb.griffin.plan.logical.SetOperationPlan;
import io.questdb.jit.CompiledFilter;
import io.questdb.std.BytecodeAssembler;
import io.questdb.std.Chars;
import io.questdb.std.IntHashSet;
import io.questdb.std.IntList;
import io.questdb.std.LongList;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import org.jetbrains.annotations.Nullable;

final class AggregateFactoryGenerator {
    private final BytecodeAssembler asm;
    private final SqlCodeGenerator codeGenerator;
    private final CairoConfiguration configuration;
    private final EntityColumnFilter entityColumnFilter;
    private final FilterFactoryGenerator filterGenerator;
    private final FunctionInstantiator functionInstantiator;
    private final ObjList<JoinKeyLayout> horizonKeys = new ObjList<>();
    private final IntList masterSymbolColumns;
    private final IntList slaveSymbolColumns;
    private final IntList symbolKeyFlags;

    AggregateFactoryGenerator(
            CairoConfiguration configuration,
            SqlCodeGenerator codeGenerator,
            FilterFactoryGenerator filterGenerator,
            FunctionInstantiator functionInstantiator,
            BytecodeAssembler asm,
            EntityColumnFilter entityColumnFilter,
            IntList symbolKeyFlags,
            IntList masterSymbolColumns,
            IntList slaveSymbolColumns
    ) {
        this.codeGenerator = codeGenerator;
        this.filterGenerator = filterGenerator;
        this.functionInstantiator = functionInstantiator;
        this.configuration = configuration;
        this.asm = asm;
        this.entityColumnFilter = entityColumnFilter;
        this.symbolKeyFlags = symbolKeyFlags;
        this.masterSymbolColumns = masterSymbolColumns;
        this.slaveSymbolColumns = slaveSymbolColumns;
    }

    /**
     * Takes the filter function from the holder, which hands every resource it holds to the caller's consumer.
     */
    private static Function adoptFilter(@Nullable PreparedFilter filter) {
        if (filter == null) {
            return null;
        }
        final Function function = filter.getFilter();
        filter.adopt();
        return function;
    }

    /**
     * Lays out the GROUP BY map and the output functions of the aggregate: the aggregates' value columns, the column
     * keys, then the computed keys, recording each key's map slot in {@code keySlots}.
     */
    private static void assemble(
            AggregatePlan plan,
            RecordMetadata inputMetadata,
            FunctionInstantiator instantiator,
            SqlExecutionContext executionContext,
            ObjList<GroupByFunction> aggregates,
            ObjList<Function> keyFunctions,
            ObjList<Function> recordFunctions,
            ArrayColumnTypes keyTypes,
            ArrayColumnTypes valueTypes,
            ListColumnFilter columnFilter,
            IntList keySlots
    ) throws SqlException {
        instantiateAggregates(plan, inputMetadata, instantiator, executionContext, aggregates, recordFunctions, valueTypes);
        layOutColumnKeys(plan, inputMetadata, recordFunctions, keyTypes, valueTypes, columnFilter, keySlots);
        final OutputSchema input = plan.getChild().getOutput();
        final ObjList<BoundExpression> keys = plan.getGroupingExpressions();
        for (int i = 0, n = keys.size(); i < n; i++) {
            final BoundExpression key = keys.getQuick(i);
            if (!(key instanceof ColumnExpression)) {
                final Function function = instantiator.instantiate(key, input, inputMetadata, executionContext);
                keyFunctions.add(function);
                final int slot = valueTypes.getColumnCount() + keyTypes.getColumnCount() + 1;
                Function keyColumn = GroupByUtils.createColumnFunction(null, slot, function.getType(), -1);
                keyTypes.add(keyColumn.getType());
                keySlots.setQuick(i, slot);
                if (function.getType() == ColumnType.SYMBOL && keyColumn.getType() == ColumnType.STRING) {
                    keyColumn = new CastStrToSymbolFunctionFactory.Func(keyColumn);
                }
                recordFunctions.setQuick(i, keyColumn);
            }
        }
    }

    private static GenericRecordMetadata metadata(AggregatePlan plan, RecordMetadata input, ObjList<Function> functions) {
        final GenericRecordMetadata metadata = new GenericRecordMetadata();
        final OutputSchema output = plan.getOutput();
        final ObjList<BoundExpression> keys = plan.getGroupingExpressions();
        for (int i = 0, n = output.getColumnCount(); i < n; i++) {
            final String name = Chars.toString(output.getColumnName(i));
            if (i < keys.size() && keys.getQuick(i) instanceof ColumnExpression column) {
                final int index = plan.getChild().getOutput().getColumnIndexById(column.getColumnId());
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
            IntList keySlots,
            ObjList<Function> functions
    ) throws SqlException {
        final OutputSchema input = plan.getChild().getOutput();
        final ObjList<BoundExpression> keys = plan.getGroupingExpressions();
        functions.setPos(recordFunctions.size());
        for (int i = 0, n = plan.getAggregates().size(); i < n; i++) {
            final int index = keys.size() + i;
            final GroupByFunction function = (GroupByFunction) instantiator.instantiateAggregate(plan.getAggregates().getQuick(i), input, inputMetadata, executionContext);
            functions.setQuick(index, function);
            function.initSharedFrom((GroupByFunction) recordFunctions.getQuick(index));
        }
        for (int i = 0, n = keys.size(); i < n; i++) {
            if (keys.getQuick(i) instanceof ColumnExpression column) {
                functions.setQuick(i, GroupByUtils.createColumnFunction(inputMetadata, keySlots.getQuick(i), column.getDataType(),
                        input.getColumnIndexById(column.getColumnId())));
            } else {
                final Function owner = recordFunctions.getQuick(i);
                final int type = owner instanceof CastStrToSymbolFunctionFactory.Func ? ColumnType.STRING : owner.getType();
                Function keyColumn = GroupByUtils.createColumnFunction(null, keySlots.getQuick(i), type, -1);
                if (owner instanceof CastStrToSymbolFunctionFactory.Func) {
                    keyColumn = new CastStrToSymbolFunctionFactory.Func(keyColumn);
                }
                functions.setQuick(i, keyColumn);
            }
        }
    }

    private static ObjList<ObjList<Function>> workerKeys(
            GenerationFrame frame,
            AggregatePlan plan,
            ObjList<Function> keyFunctions,
            RecordMetadata inputMetadata,
            FunctionInstantiator instantiator,
            SqlExecutionContext executionContext
    ) throws SqlException {
        final ObjList<BoundExpression> keys = plan.getGroupingExpressions();
        final ObjList<BoundExpression> functionKeys = frame.expressions;
        functionKeys.clear();
        for (int i = 0, n = keys.size(); i < n; i++) {
            if (!(keys.getQuick(i) instanceof ColumnExpression)) {
                functionKeys.add(keys.getQuick(i));
            }
        }
        return instantiator.instantiateWorkerFunctions(functionKeys, plan.getChild().getOutput(), inputMetadata, keyFunctions,
                executionContext.getSharedQueryWorkerCount(), executionContext);
    }

    private RecordCursorFactory generateFunctions(
            GenerationFrame frame,
            AggregatePlan plan,
            RecordCursorFactory base,
            @Nullable PreparedFilter stolenFilter,
            SqlExecutionContext executionContext
    ) throws SqlException {
        ObjList<Function> keyFunctions = null;
        ObjList<Function> recordFunctions = null;
        ObjList<ObjList<GroupByFunction>> workerAggregates = null;
        ObjList<ObjList<Function>> workerKeys = null;
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
            assemble(plan, base.getMetadata(), functionInstantiator, executionContext, aggregates, keyFunctions, recordFunctions, keyTypes, valueTypes,
                    columnFilter, frame.intListA);
            final GenericRecordMetadata metadata = metadata(plan, base.getMetadata(), recordFunctions);
            final int sharedConsumerCount = plan.getSharedConsumerCount();
            if (sharedConsumerCount > 0) {
                sharedRecordFunctions = new ObjList<>(sharedConsumerCount);
                for (int i = 0; i < sharedConsumerCount; i++) {
                    final ObjList<Function> functions = new ObjList<>(recordFunctions.size());
                    sharedRecordFunctions.add(functions);
                    sharedRecordFunctions(plan, base.getMetadata(), functionInstantiator, executionContext, recordFunctions, frame.intListA, functions);
                }
            }
            final boolean isParallel = plan.getAlgorithm() != null && plan.getAlgorithm() != AggregatePlan.Algorithm.SERIAL;
            if (ParanoiaState.PLAN_PARANOIA_MODE && isParallel != canParallelizeGroupBy(
                    base, keyTypes.getColumnCount(), keyFunctions, aggregates, stolenFilter != null, executionContext)) {
                throw new AssertionError("recorded aggregate algorithm differs from the generator's parallel choice");
            }
            final PreparedFilter filter = isParallel ? stolenFilter : null;
            if (isParallel) {
                workerAggregates = functionInstantiator.instantiateWorkerAggregates(plan.getAggregates(), plan.getChild().getOutput(), base.getMetadata(),
                        aggregates, executionContext.getSharedQueryWorkerCount(), executionContext);
                workerKeys = workerKeys(frame, plan, keyFunctions, base.getMetadata(), functionInstantiator, executionContext);
                if (filter != null) {
                    filterGenerator.prepareParallel(frame, filter, base, executionContext);
                }
            }
            final CompiledFilter compiledFilter = filter != null ? filter.getCompiledFilter() : null;
            final MemoryCARW bindVariableMemory = filter != null ? filter.getBindVarMemory() : null;
            final ObjList<Function> bindVariables = filter != null ? filter.getBindVarFunctions() : null;
            final Function filterFunction = filter != null ? filter.getFilter() : null;
            final IntHashSet filterIndexes = filter != null ? filter.getColumns() : null;
            final ObjList<Function> workerFilters = filter != null ? filter.getWorkers() : null;
            if (filter != null) {
                filter.adopt();
            }
            isAdopted = true;
            return generateGroupBy(
                    base, metadata, columnFilter, keyTypes, valueTypes, aggregates, workerAggregates,
                    keyFunctions, workerKeys, recordFunctions, compiledFilter, bindVariableMemory,
                    bindVariables, filterFunction, filterIndexes, workerFilters, sharedRecordFunctions, isParallel, executionContext
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
                Misc.freeObjList(recordFunctions, th);
                Misc.freeObjList(keyFunctions, th);
                Misc.free(base, th);
            }
            throw th;
        }
    }

    /**
     * Builds the factory a HORIZON JOIN reads the time frames of its slave from, under the filter it steals, see
     * {@link PlanToFactoryRules#horizonStolenFilter}, whose holder it pushes onto the frame, and the metadata the join
     * reads the slave through, with the slave column each of its columns reads when a selection maps them.
     */
    private void generateHorizonSlave(
            GenerationFrame frame,
            LogicalPlan slave,
            ObjList<RecordCursorFactory> slaves,
            ObjList<PreparedFilter> slaveFilters,
            ObjList<RecordMetadata> slaveMetadatas,
            ObjList<IntList> slaveCrossIndexes,
            SqlExecutionContext executionContext
    ) throws SqlException {
        final FilterPlan filter = PlanToFactoryRules.horizonStolenFilter(slave);
        if (filter == null) {
            final RecordCursorFactory factory = codeGenerator.generate(frame, slave, executionContext);
            slaves.add(factory);
            slaveFilters.add(null);
            slaveMetadatas.add(factory.getMetadata());
            slaveCrossIndexes.add(null);
            return;
        }
        final PreparedFilter prepared = frame.pushPreparedFilter();
        slaveFilters.add(prepared);
        final RecordCursorFactory leaf = codeGenerator.generateStolenFilter(frame, filter, prepared, executionContext);
        slaves.add(leaf);
        final ProjectPlan projection = PlanToFactoryRules.horizonSlaveProjection(slave);
        if (projection == null) {
            slaveMetadatas.add(leaf.getMetadata());
            slaveCrossIndexes.add(null);
            return;
        }
        final GenericRecordMetadata metadata = new GenericRecordMetadata();
        final IntList crossIndex = new IntList(projection.getExpressions().size());
        ProjectionFactoryGenerator.selectColumns(projection, leaf.getMetadata(), metadata, crossIndex);
        slaveMetadatas.add(metadata);
        slaveCrossIndexes.add(crossIndex);
    }

    private RecordCursorFactory generateSharedInput(GenerationFrame frame, AggregatePlan aggregate) {
        final JoinInput source = aggregate.getSharedSource();
        final int entry = source == null ? -1 : frame.sharedSources.indexOf(source);
        if (entry < 0 || PhysicalProperties.supportsSharedCursors(source.getInput()) != PhysicalProperties.Capability.YES) {
            return null;
        }
        final RecordCursorFactory primary = frame.sharedFactories.getQuick(entry);
        final RecordMetadata primaryMetadata = primary.getMetadata();
        final OutputSchema sourceOutput = source.getSourceOutput();
        final OutputSchema input = aggregate.getChild().getOutput();
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
            metadata.add(TableColumnMetadata.copyOf(primaryMetadata, index, SqlUtil.toColumnName(input.getColumnName(i))));
        }
        final int sharedId = frame.sharedConsumerCounts.getQuick(entry);
        frame.sharedConsumerCounts.setQuick(entry, sharedId + 1);
        return new SelectedRecordCursorFactory(metadata, mapping, new SharedRecordCursorFactory(primary, sharedId));
    }

    /**
     * Builds the parallel GROUP BY that steals the filter of its input, over the factory under that filter.
     */
    private RecordCursorFactory generateStolenFilterGroupBy(GenerationFrame frame, AggregatePlan aggregate, SqlExecutionContext executionContext)
            throws SqlException {
        final PreparedFilter prepared = frame.pushPreparedFilter();
        final RecordCursorFactory factory;
        try {
            final RecordCursorFactory leaf = codeGenerator.generateSource(frame, PlanToFactoryRules.aggregateBase(aggregate), prepared, executionContext);
            factory = generateFunctions(frame, aggregate, leaf, prepared, executionContext);
        } catch (Throwable th) {
            frame.popPreparedFilter(th);
            throw th;
        }
        frame.popPreparedFilter();
        return factory;
    }

    private RecordCursorFactory generateTimestampEndpoint(GenerationFrame frame, AggregatePlan aggregate, SqlExecutionContext executionContext)
            throws SqlException {
        final LogicalPlan input = aggregate.getChild();
        final OutputSchema output = aggregate.getOutput();
        final GenericRecordMetadata metadata = new GenericRecordMetadata()
                .add(new TableColumnMetadata(Chars.toString(output.getColumnName(0)), output.getColumnType(0)));
        final int columnIndex = input.getOutput().getColumnIndexById(LogicalPlans.endpointColumn(aggregate).getColumnId());
        return new TimestampEndpointRecordCursorFactory(metadata, codeGenerator.generate(frame, input, executionContext),
                aggregate.getAggregates().getQuick(0).getName(), columnIndex);
    }

    private RecordCursorFactory generateVector(GenerationFrame frame, AggregatePlan plan, RecordCursorFactory base, SqlExecutionContext executionContext) {
        ObjList<VectorAggregateFunction> functions = null;
        boolean isAdopted = false;
        try {
            functions = new ObjList<>(plan.getAggregates().size() + 1);
            final BoundExpression keyExpression = plan.getGroupingExpressions().getQuick(0);
            final ColumnExpression key = PlanToFactoryRules.vectorKey(keyExpression);
            assert key != null;
            final int keyType = keyExpression.getDataType();
            final int keyKind = keyExpression instanceof FunctionExpression
                    ? ColumnType.getTimestampDriver(key.getDataType()).getGKKHourInt() : VectorAggregateFunction.GKK_VANILLA_INT;
            final OutputSchema input = plan.getChild().getOutput();
            final int keyIndex = input.getColumnIndexById(key.getColumnId());
            final ArrayColumnTypes types = frame.keyTypes;
            types.clear();
            types.add(keyType);
            final GenericRecordMetadata metadata = new GenericRecordMetadata();
            final OutputSchema output = plan.getOutput();
            for (int i = 0, n = output.getColumnCount(); i < n; i++) {
                metadata.add(i == 0
                        ? new TableColumnMetadata(Chars.toString(output.getColumnName(i)), keyType,
                        IndexType.NONE, 0, base.getMetadata().isSymbolTableStatic(keyIndex), null)
                        : new TableColumnMetadata(Chars.toString(output.getColumnName(i)), output.getColumnType(i)));
            }
            final IntList symbolIndexes = frame.intListA;
            symbolIndexes.clear();
            if (keyType == ColumnType.SYMBOL) {
                symbolIndexes.setAll(output.getColumnCount(), -1);
                symbolIndexes.setQuick(0, keyIndex);
            }
            for (int i = 1, n = output.getColumnCount(); i < n; i++) {
                final FunctionExpression call = plan.getAggregates().getQuick(i - 1);
                final int index = call.getArgumentCount() == 0 ? -1
                        : input.getColumnIndexById(((ColumnExpression) call.argumentAt(0)).getColumnId());
                final VectorAggregateFunctionConstructor constructor = PlanToFactoryRules.vectorConstructor(call);
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

    private JoinKeyLayout horizonJoinKeys(HorizonJoinSlave step, OutputSchema masterOutput, RecordMetadata masterMetadata,
                                          RecordMetadata slaveMetadata, @Nullable IntList slaveCrossIndex) {
        if (step.getMasterKeyColumnIds().size() == 0) {
            return null;
        }
        final JoinKeyLayout keys = new JoinKeyLayout(symbolKeyFlags, masterSymbolColumns, slaveSymbolColumns);
        JoinFactoryGenerator.resolveKeys(masterOutput, step.getMasterKeyColumnIds(), keys.masterIndexes);
        JoinFactoryGenerator.resolveKeys(step.getInput().getOutput(), step.getSlaveKeyColumnIds(), keys.slaveIndexes);
        if (slaveCrossIndex != null) {
            for (int i = 0, n = keys.slaveIndexes.size(); i < n; i++) {
                keys.slaveIndexes.setQuick(i, slaveCrossIndex.getQuick(keys.slaveIndexes.getQuick(i)));
            }
        }
        keys.of(masterMetadata, slaveMetadata, keys.masterIndexes, keys.slaveIndexes, false);
        keys.convertSymbolKeysToInt(masterMetadata, slaveMetadata);
        return keys;
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
        final LogicalPlan sharedOperation = SqlCodeGenerator.unwrapColumnProjections(source.getInput());
        final int index = frame.setOperationPlans.indexOf(sharedOperation);
        if (index < 0) {
            return false;
        }
        LogicalPlan headPlan = sharedOperation;
        while (SqlCodeGenerator.unwrapColumnProjections(headPlan) instanceof SetOperationPlan branch) {
            headPlan = branch.getLeft();
        }
        if (PhysicalProperties.supportsSharedCursors(headPlan) != PhysicalProperties.Capability.YES) {
            return false;
        }
        LogicalPlan leaf = aggregate.getChild();
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

    static boolean canParallelizeGroupBy(
            RecordCursorFactory base,
            int keyCount,
            ObjList<Function> keyFunctions,
            ObjList<GroupByFunction> groupByFunctions,
            boolean isFilterStolen,
            SqlExecutionContext executionContext
    ) {
        return executionContext.isParallelGroupByEnabled()
                && !(keyCount == 0 && GroupByUtils.isEarlyExitSupported(groupByFunctions) && !isFilterStolen && base.getFilter() == null)
                && SqlUtil.isParallelismSupported(keyFunctions)
                && GroupByUtils.isParallelismSupported(groupByFunctions)
                && (base.supportsPageFrameCursor() || isFilterStolen);
    }

    static void closeWorkers(ObjList<? extends ObjList<? extends Function>> workers, Throwable primary) {
        if (workers != null) {
            for (int i = 0, n = workers.size(); i < n; i++) {
                PerWorkerFunctionList.close(workers.getQuick(i), primary);
            }
        }
    }

    /**
     * Instantiates the aggregates of the plan into their output slots of {@code recordFunctions}, after the keys, and
     * appends their map value types.
     */
    static void instantiateAggregates(
            GroupingPlan plan,
            RecordMetadata inputMetadata,
            FunctionInstantiator instantiator,
            SqlExecutionContext executionContext,
            ObjList<GroupByFunction> aggregates,
            ObjList<Function> recordFunctions,
            ArrayColumnTypes valueTypes
    ) throws SqlException {
        final OutputSchema input = plan.getChild().getOutput();
        final int keyCount = plan.getGroupingExpressions().size();
        recordFunctions.setPos(plan.getOutput().getColumnCount());
        for (int i = 0, n = plan.getAggregates().size(); i < n; i++) {
            final FunctionExpression call = plan.getAggregates().getQuick(i);
            final GroupByFunction function = (GroupByFunction) instantiator.instantiateAggregate(call, input, inputMetadata, executionContext);
            recordFunctions.setQuick(keyCount + i, function);
            aggregates.add(function);
            function.initValueTypes(valueTypes);
        }
    }

    static boolean isTimeSeriesDistinct(RecordCursorFactory base) {
        return base.recordCursorSupportsRandomAccess() && base.getMetadata().getTimestampIndex() >= 0;
    }

    /**
     * Lays out the column keys of the plan in the GROUP BY map after its value columns, and builds the output function
     * of each over its map slot, which {@code keySlots} records by key position: a column repeated consecutively takes
     * one slot, and the bucket timestamp of a SAMPLE BY none, -1. The computed keys follow in the map.
     */
    static void layOutColumnKeys(
            GroupingPlan plan,
            RecordMetadata inputMetadata,
            ObjList<Function> recordFunctions,
            ArrayColumnTypes keyTypes,
            ArrayColumnTypes valueTypes,
            ListColumnFilter columnFilter,
            IntList keySlots
    ) {
        final OutputSchema input = plan.getChild().getOutput();
        final ObjList<BoundExpression> keys = plan.getGroupingExpressions();
        final int bucketColumnId = plan instanceof SampleByPlan sample ? sample.getTimestampColumnId() : -1;
        keySlots.setAll(keys.size(), -1);
        // RecordSink writes direct columns before computed keys. Output functions
        // map those physical key slots back to the logical key order.
        int lastIndex = -1;
        for (int i = 0, n = keys.size(); i < n; i++) {
            if (keys.getQuick(i) instanceof ColumnExpression column && column.getColumnId() != bucketColumnId) {
                final int index = input.getColumnIndexById(column.getColumnId());
                // The map stores a column repeated consecutively as one key.
                if (index != lastIndex) {
                    columnFilter.add(index + 1);
                    keyTypes.add(column.getDataType());
                    lastIndex = index;
                }
                final int slot = valueTypes.getColumnCount() + keyTypes.getColumnCount();
                keySlots.setQuick(i, slot);
                recordFunctions.setQuick(i, GroupByUtils.createColumnFunction(inputMetadata, slot, column.getDataType(), index));
            }
        }
    }

    /**
     * Consumes the input on entry, including on failure.
     */
    RecordCursorFactory generate(
            GenerationFrame frame,
            AggregatePlan plan,
            RecordCursorFactory base,
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
                    final CharSequence name = PlanToFactoryRules.factoryColumnName(plan, 0);
                    final RecordMetadata metadata = Chars.equals(name, "count")
                            ? CountRecordCursorFactory.DEFAULT_COUNT_METADATA
                            : new GenericRecordMetadata().add(new TableColumnMetadata(Chars.toString(name), ColumnType.LONG));
                    if (base instanceof SelectedRecordCursorFactory selected && selected.getMetadata().getColumnCount() == 0) {
                        base = selected.getBaseFactory();
                    }
                    isAdopted = true;
                    return new CountRecordCursorFactory(metadata, base);
                }
            }
            if (plan.getAlgorithm() == AggregatePlan.Algorithm.VECTORISED) {
                isAdopted = true;
                return generateVector(frame, plan, base, executionContext);
            }
            isAdopted = true;
            return generateFunctions(frame, plan, base, null, executionContext);
        } catch (Throwable th) {
            if (!isAdopted) {
                Misc.free(base, th);
            }
            throw th;
        }
    }

    RecordCursorFactory generateAggregate(GenerationFrame frame, AggregatePlan aggregate, SqlExecutionContext executionContext) throws SqlException {
        final PlanToFactoryRules.AggregateFactory factory = PlanToFactoryRules.aggregateFactory(aggregate);
        if (factory == PlanToFactoryRules.AggregateFactory.HORIZON) {
            return generateHorizonJoin(frame, aggregate, (HorizonJoinPlan) aggregate.getChild(), executionContext);
        }
        if (factory == PlanToFactoryRules.AggregateFactory.TIMESTAMP_ENDPOINT) {
            return generateTimestampEndpoint(frame, aggregate, executionContext);
        }
        if (aggregate.getAlgorithm() == AggregatePlan.Algorithm.PARALLEL_STOLEN_FILTER) {
            return generateStolenFilterGroupBy(frame, aggregate, executionContext);
        }
        RecordCursorFactory shared = generateSharedInput(frame, aggregate);
        if (shared == null && prepareSharedHead(frame, aggregate)) {
            try {
                shared = codeGenerator.generate(frame, aggregate.getChild(), executionContext);
            } finally {
                frame.sharedHeadTarget = null;
            }
        }
        final RecordCursorFactory base = shared != null ? shared : codeGenerator.generate(frame, PlanToFactoryRules.aggregateBase(aggregate), executionContext);
        return generate(frame, aggregate, base, executionContext);
    }

    RecordCursorFactory generateDistinct(GenerationFrame frame, DistinctPlan distinct, LimitPlan limitAdvice, SqlExecutionContext executionContext) throws SqlException {
        // DISTINCT keeps its input order. A downstream sort must not turn the
        // timestamp-specialized factory's forward input into a backward scan.
        final RecordCursorFactory base = codeGenerator.generate(frame, distinct.getChild(), executionContext);
        if (limitAdvice == null || isTimeSeriesDistinct(base)) {
            return generateDistinct(base, null, null);
        }
        codeGenerator.instantiateLimit(frame, limitAdvice, base, executionContext);
        return generateDistinct(base, frame.limitLo, frame.limitHi);
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
            ObjList<PreparedFilter> slaveFilters,
            ObjList<RecordMetadata> slaveMetadatas,
            ObjList<IntList> slaveCrossIndexes,
            @Nullable PreparedFilter stolenFilter,
            SqlExecutionContext executionContext
    ) throws SqlException {
        JoinRecordMetadata innerMetadata = null;
        ObjList<Function> keyFunctions = null;
        ObjList<Function> recordFunctions = null;
        ObjList<ObjList<GroupByFunction>> workerAggregates = null;
        ObjList<ObjList<Function>> workerKeys = null;
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
                final RecordMetadata slaveMetadata = slaveMetadatas.getQuick(s);
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
            assemble(plan, innerMetadata, functionInstantiator, executionContext,
                    aggregates, keyFunctions, recordFunctions, keyTypes, valueTypes, columnFilter, frame.intListA);
            final GenericRecordMetadata metadata = metadata(plan, innerMetadata, recordFunctions);
            final boolean isParallel = plan.getAlgorithm() != null && plan.getAlgorithm() != AggregatePlan.Algorithm.SERIAL;
            if (ParanoiaState.PLAN_PARANOIA_MODE && isParallel != (executionContext.isParallelHorizonJoinEnabled()
                    && (master.supportsPageFrameCursor() || stolenFilter != null)
                    && SqlUtil.isParallelismSupported(keyFunctions) && GroupByUtils.isParallelismSupported(aggregates))) {
                throw new AssertionError("recorded horizon join algorithm differs from the generator's parallel choice");
            }
            final PreparedFilter filter = isParallel ? stolenFilter : null;
            if (isParallel) {
                workerAggregates = functionInstantiator.instantiateWorkerAggregates(plan.getAggregates(), plan.getChild().getOutput(), innerMetadata,
                        aggregates, executionContext.getSharedQueryWorkerCount(), executionContext);
                workerKeys = workerKeys(frame, plan, keyFunctions, innerMetadata, functionInstantiator, executionContext);
                if (filter != null) {
                    filterGenerator.prepareParallel(frame, filter, master, executionContext);
                    filter.getColumns().add(masterMetadata.getTimestampIndex());
                }
            }
            final CompiledFilter compiledFilter = filter != null ? filter.getCompiledFilter() : null;
            final MemoryCARW bindVariableMemory = filter != null ? filter.getBindVarMemory() : null;
            final ObjList<Function> bindVariables = filter != null ? filter.getBindVarFunctions() : null;
            final Function filterFunction = filter != null ? filter.getFilter() : null;
            final IntHashSet filterIndexes = filter != null ? filter.getColumns() : null;
            final ObjList<Function> workerFilters = filter != null ? filter.getWorkers() : null;
            if (filter != null) {
                filter.adopt();
            }
            isAdopted = true;
            return generateHorizonJoin(horizon, master, slaves, slaveFilters, slaveMetadatas, slaveCrossIndexes, innerMetadata,
                    metadata, columnFilter, keyTypes, valueTypes, aggregates, workerAggregates, keyFunctions, workerKeys,
                    recordFunctions, compiledFilter, bindVariableMemory, bindVariables, filterFunction, filterIndexes,
                    workerFilters, isParallel, executionContext);
        } catch (Throwable th) {
            if (!isAdopted) {
                closeWorkers(workerAggregates, th);
                closeWorkers(workerKeys, th);
                Misc.freeObjList(recordFunctions, th);
                Misc.freeObjList(keyFunctions, th);
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
        final ObjList<PreparedFilter> slaveFilters = new ObjList<>(slaveCount);
        final ObjList<RecordMetadata> slaveMetadatas = new ObjList<>(slaveCount);
        final ObjList<IntList> slaveCrossIndexes = new ObjList<>(slaveCount);
        final PreparedFilter prepared = frame.pushPreparedFilter(aggregate.getAlgorithm() == AggregatePlan.Algorithm.PARALLEL_STOLEN_FILTER);
        final RecordCursorFactory factory;
        try {
            final RecordCursorFactory master = codeGenerator.generateSource(frame, horizon.getMaster(), prepared, executionContext);
            try {
                for (int i = 0; i < slaveCount; i++) {
                    generateHorizonSlave(frame, horizon.getSlaves().getQuick(i).getInput(), slaves, slaveFilters, slaveMetadatas,
                            slaveCrossIndexes, executionContext);
                }
            } catch (Throwable th) {
                Misc.freeObjList(slaves, th);
                Misc.free(master, th);
                throw th;
            }
            factory = generateHorizonJoin(frame, aggregate, horizon, master, slaves, slaveFilters, slaveMetadatas, slaveCrossIndexes,
                    prepared, executionContext);
        } catch (Throwable th) {
            for (int i = slaveFilters.size() - 1; i >= 0; i--) {
                frame.popPreparedFilter(slaveFilters.getQuick(i), th);
            }
            frame.popPreparedFilter(prepared, th);
            throw th;
        }
        for (int i = slaveFilters.size() - 1; i >= 0; i--) {
            frame.popPreparedFilter(slaveFilters.getQuick(i));
        }
        frame.popPreparedFilter(prepared);
        return factory;
    }

    /**
     * Consumes the inputs, the inner metadata and every function and filter resource on entry, including on failure.
     */
    RecordCursorFactory generateHorizonJoin(
            HorizonJoinPlan plan,
            RecordCursorFactory master,
            ObjList<RecordCursorFactory> slaves,
            ObjList<PreparedFilter> slaveFilters,
            ObjList<RecordMetadata> slaveMetadatas,
            ObjList<IntList> slaveCrossIndexes,
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
                if (!slave.supportsTimeFrameCursor()) {
                    throw SqlException.position(position).put("right-hand side of HORIZON JOIN can only be a table with an optional filter");
                }
            }
            if (!isParallel && !master.recordCursorSupportsRandomAccess()) {
                throw SqlException.position(steps.getQuick(0).getPosition()).put("left-hand side of HORIZON JOIN can only be a table with an optional filter");
            }
            final int workerCount = executionContext.getSharedQueryWorkerCount();
            if (isParallel) {
                for (int s = 0; s < slaveCount; s++) {
                    final PreparedFilter slaveFilter = slaveFilters.getQuick(s);
                    if (slaveFilter != null) {
                        slaveFilter.setWorkers(functionInstantiator.instantiateWorkers(slaveFilter.getPredicate(), slaveFilter.getInput(),
                                slaves.getQuick(s).getMetadata(), slaveFilter.getFilter(), workerCount, executionContext));
                    }
                }
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
            final ObjList<JoinKeyLayout> keys = horizonKeys;
            keys.clear();
            for (int s = 0; s < slaveCount; s++) {
                final IntList crossIndex = slaveCrossIndexes.getQuick(s);
                for (int i = 0, n = slaveMetadatas.getQuick(s).getColumnCount(); i < n; i++, column++) {
                    columnSources[column] = MultiHorizonJoinRecord.SOURCE_SLAVE_BASE + s;
                    columnIndices[column] = crossIndex != null ? crossIndex.getQuick(i) : i;
                }
                keys.add(horizonJoinKeys(steps.getQuick(s), plan.getMaster().getOutput(), masterMetadata, slaves.getQuick(s).getMetadata(), crossIndex));
            }
            final boolean isKeyed = keyTypes.getColumnCount() > 0;
            if (isParallel) {
                master.changePageFrameSizes(configuration.getSqlSmallPageFrameMinRows(), configuration.getSqlSmallPageFrameMaxRows());
            }
            if (slaveCount == 1) {
                final JoinKeyLayout key = keys.getQuick(0);
                final RecordCursorFactory slave = slaves.getQuick(0);
                final ArrayColumnTypes joinKeyTypes = key == null ? null : key.keyTypes;
                final int[] masterSymbols = key == null ? null : key.masterSymbolKeys;
                final int[] slaveSymbols = key == null ? null : key.slaveSymbolKeys;
                final PreparedFilter slaveFilter = slaveFilters.getQuick(0);
                if (!isParallel) {
                    final RecordSink masterSink = key == null ? null : key.createMasterSink(configuration, asm, masterMetadata);
                    final RecordSink slaveSink = key == null ? null : key.createSlaveSink(configuration, asm, slave.getMetadata());
                    final Function slaveFilterFunction = adoptFilter(slaveFilter);
                    isAdopted = true;
                    return isKeyed
                            ? new HorizonJoinRecordCursorFactory(configuration, asm, metadata, innerMetadata, master, slave, slaveFilterFunction, offsets,
                            masterTimestampIndex, groupByFunctions, recordFunctions, keyFunctions, keyTypes, valueTypes, joinKeyTypes,
                            masterSink, slaveSink, masterColumnCount, masterSymbols, slaveSymbols, columnFilter, columnSources, columnIndices)
                            : new HorizonJoinNotKeyedRecordCursorFactory(configuration, asm, metadata, innerMetadata, master, slave, slaveFilterFunction, offsets,
                            masterTimestampIndex, groupByFunctions, valueTypes.getColumnCount(), joinKeyTypes, masterSink, slaveSink,
                            masterColumnCount, masterSymbols, slaveSymbols, columnSources, columnIndices);
                }
                final AsyncHorizonJoinResources resources = new AsyncHorizonJoinResources(workerGroupByFunctions, workerKeyFunctions,
                        compiledFilter, bindVariableMemory, bindVariables, filter, filterColumnIndexes, workerFilters);
                final RecordSinkTemplate masterSinkTemplate = key == null ? null : key.masterSinkTemplate(configuration, asm, masterMetadata);
                final RecordSinkTemplate slaveSinkTemplate = key == null ? null : key.slaveSinkTemplate(configuration, asm, slave.getMetadata());
                if (slaveFilter != null) {
                    final ObjList<Function> slaveWorkerFilters = slaveFilter.getWorkers();
                    resources.setSlaveFilters(adoptFilter(slaveFilter), slaveWorkerFilters);
                }
                isAdopted = true;
                return isKeyed
                        ? new AsyncHorizonJoinRecordCursorFactory(configuration, asm, executionContext.getCairoEngine(),
                        executionContext.getMessageBus(), metadata, innerMetadata, master, slave, offsets, masterTimestampIndex,
                        groupByFunctions, recordFunctions, keyFunctions, keyTypes, valueTypes, joinKeyTypes, masterSinkTemplate,
                        slaveSinkTemplate, masterColumnCount, masterSymbols, slaveSymbols, columnFilter, columnSources, columnIndices,
                        resources, workerCount)
                        : new AsyncHorizonJoinNotKeyedRecordCursorFactory(configuration, asm, executionContext.getCairoEngine(),
                        executionContext.getMessageBus(), metadata, innerMetadata, master, slave, offsets, masterTimestampIndex,
                        groupByFunctions, valueTypes.getColumnCount(), joinKeyTypes, masterSinkTemplate, slaveSinkTemplate,
                        masterColumnCount, masterSymbols, slaveSymbols, columnSources, columnIndices, resources, workerCount);
            }
            final ColumnTypes[] joinKeyTypes = new ColumnTypes[slaveCount];
            final int masterTimestampType = masterMetadata.getTimestampType();
            slaveStates = new ObjList<>(slaveCount);
            for (int s = 0; s < slaveCount; s++) {
                final JoinKeyLayout key = keys.getQuick(s);
                final RecordCursorFactory slave = slaves.getQuick(s);
                final int slaveTimestampType = slave.getMetadata().getTimestampType();
                final boolean isScaled = masterTimestampType != slaveTimestampType;
                if (key != null) {
                    joinKeyTypes[s] = key.keyTypes;
                }
                final HorizonJoinSlaveState state = new HorizonJoinSlaveState(slave,
                        isScaled ? ColumnType.getTimestampDriver(masterTimestampType).toNanosScale() : 1,
                        isScaled ? ColumnType.getTimestampDriver(slaveTimestampType).toNanosScale() : 1,
                        joinKeyTypes[s],
                        key == null ? null : key.masterSinkTemplate(configuration, asm, masterMetadata),
                        key == null ? null : key.slaveSinkTemplate(configuration, asm, slave.getMetadata()),
                        masterColumnCount,
                        key == null ? null : key.masterSymbolKeys, key == null ? null : key.slaveSymbolKeys);
                slaveStates.add(state);
                slaves.setQuick(s, null);
                final PreparedFilter slaveFilter = slaveFilters.getQuick(s);
                if (slaveFilter != null) {
                    state.setPerWorkerFilters(slaveFilter.getWorkers());
                    state.setFilter(adoptFilter(slaveFilter));
                }
            }
            if (!isParallel) {
                isAdopted = true;
                return isKeyed
                        ? new MultiHorizonJoinRecordCursorFactory(configuration, asm, metadata, innerMetadata, master, slaveStates,
                        offsets, masterTimestampIndex, groupByFunctions, recordFunctions,
                        keyFunctions, keyTypes, valueTypes, columnFilter, columnSources, columnIndices)
                        : new MultiHorizonJoinNotKeyedRecordCursorFactory(configuration, asm, metadata, innerMetadata, master, slaveStates,
                        offsets, masterTimestampIndex, groupByFunctions, valueTypes.getColumnCount(), columnSources, columnIndices);
            }
            final AsyncHorizonJoinResources resources = new AsyncHorizonJoinResources(workerGroupByFunctions, workerKeyFunctions,
                    compiledFilter, bindVariableMemory, bindVariables, filter, filterColumnIndexes, workerFilters);
            isAdopted = true;
            return isKeyed
                    ? new AsyncMultiHorizonJoinRecordCursorFactory(configuration, asm, executionContext.getCairoEngine(),
                    executionContext.getMessageBus(), metadata, innerMetadata, master, slaveStates, joinKeyTypes,
                    offsets, masterTimestampIndex, groupByFunctions, recordFunctions, keyFunctions, keyTypes,
                    valueTypes, columnFilter, columnSources, columnIndices, resources, workerCount)
                    : new AsyncMultiHorizonJoinNotKeyedRecordCursorFactory(configuration, asm, executionContext.getCairoEngine(),
                    executionContext.getMessageBus(), metadata, innerMetadata, master, slaveStates, joinKeyTypes,
                    offsets, masterTimestampIndex, groupByFunctions, valueTypes.getColumnCount(), columnSources,
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
}
