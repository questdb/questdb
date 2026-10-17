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
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.GenericRecordMetadata;
import io.questdb.cairo.IndexType;
import io.questdb.cairo.TableColumnMetadata;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.griffin.FunctionInstantiator;
import io.questdb.griffin.FunctionResolver;
import io.questdb.griffin.LogicalPlans;
import io.questdb.griffin.PriorityMetadata;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.SymbolFunction;
import io.questdb.griffin.engine.functions.memoization.MemoizerFunction;
import io.questdb.griffin.engine.table.SelectedRecordCursorFactory;
import io.questdb.griffin.engine.table.VirtualRecordCursorFactory;
import io.questdb.griffin.plan.logical.BoundExpression;
import io.questdb.griffin.plan.logical.ColumnExpression;
import io.questdb.griffin.plan.logical.DistinctPlan;
import io.questdb.griffin.plan.logical.FillPlan;
import io.questdb.griffin.plan.logical.FilterPlan;
import io.questdb.griffin.plan.logical.ForwardingPlan;
import io.questdb.griffin.plan.logical.FunctionExpression;
import io.questdb.griffin.plan.logical.GroupingPlan;
import io.questdb.griffin.plan.logical.HorizonJoinPlan;
import io.questdb.griffin.plan.logical.JoinInput;
import io.questdb.griffin.plan.logical.JoinPlan;
import io.questdb.griffin.plan.logical.LatestByPlan;
import io.questdb.griffin.plan.logical.LogicalPlan;
import io.questdb.griffin.plan.logical.OutputSchema;
import io.questdb.griffin.plan.logical.PlanToFactoryRules;
import io.questdb.griffin.plan.logical.ProjectPlan;
import io.questdb.griffin.plan.logical.SampleByPlan;
import io.questdb.griffin.plan.logical.SetOperationPlan;
import io.questdb.griffin.plan.logical.SortPlan;
import io.questdb.griffin.plan.logical.WindowJoinPlan;
import io.questdb.griffin.plan.logical.WindowPlan;
import io.questdb.griffin.plan.logical.WindowSpec;
import io.questdb.std.Chars;
import io.questdb.std.IntList;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;

final class ProjectionFactoryGenerator {
    private final FunctionInstantiator functionInstantiator;

    ProjectionFactoryGenerator(FunctionInstantiator functionInstantiator) {
        this.functionInstantiator = functionInstantiator;
    }

    private static boolean hasProjectionReferences(ProjectPlan project) {
        for (int i = 0, n = project.getExpressions().size(); i < n; i++) {
            if (referencesOutput(project.getExpressions().getQuick(i), project.getOutput(), project.getChild().getOutput())) {
                return true;
            }
        }
        return false;
    }

    /**
     * Whether the plan's answer to the identity of the column projection must match the generator's: everywhere but
     * over a join of several inputs, directly or through nodes that forward its columns, whose factories name its
     * columns qualifier.name, which the plan does not model; no planner decision reads that answer differently.
     */
    private static boolean isPlannedIdentityChecked(ProjectPlan project) {
        LogicalPlan input = project.getChild();
        while (input instanceof ForwardingPlan forwarding) {
            input = forwarding.getChild();
        }
        return !LogicalPlans.isComputedProjection(project) && !(input instanceof JoinPlan join && join.getOrderedInputs().size() > 1);
    }

    private static RecordCursorFactory newSelected(GenericRecordMetadata metadata, IntList mapping, RecordCursorFactory base, LogicalPlan input) {
        if (input instanceof JoinPlan && base instanceof SelectedRecordCursorFactory selected) {
            final IntList inner = selected.getColumnCrossIndex();
            for (int i = 0, n = mapping.size(); i < n; i++) {
                mapping.setQuick(i, inner.getQuick(mapping.getQuick(i)));
            }
            return new SelectedRecordCursorFactory(metadata, mapping, selected.getBaseFactory());
        }
        return new SelectedRecordCursorFactory(metadata, mapping, base);
    }

    private static boolean referencesOutput(BoundExpression expression, OutputSchema output, OutputSchema input) {
        if (expression instanceof ColumnExpression column) {
            return output.getColumnIndexById(column.getColumnId()) >= 0 && input.getColumnIndexById(column.getColumnId()) < 0;
        }
        if (expression instanceof FunctionExpression call) {
            for (int i = 0, n = call.getArgumentCount(); i < n; i++) {
                if (referencesOutput(call.argumentAt(i), output, input)) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * These operators consume their inputs independently of reads of their output records. A join passes
     * source columns through, so the projection above still counts their reads.
     */
    private static void setInputReferenceCounts(GenerationFrame frame, LogicalPlan plan) {
        final boolean isJoin = plan instanceof JoinPlan;
        for (int i = 0, n = plan.inputCount(); i < n; i++) {
            final boolean isPassThrough = isJoin && !(LogicalPlans.skipFilters(plan.inputAt(i)) instanceof ProjectPlan);
            final OutputSchema input = plan.inputAt(i).getOutput();
            for (int j = 0, m = input.getColumnCount(); j < m; j++) {
                final int columnId = input.getColumnId(j);
                frame.setReferenceCount(columnId, isPassThrough ? Math.max(1, frame.getReferenceCount(columnId)) : 1);
            }
        }
    }

    private static String uniqueColumnName(RecordMetadata metadata, CharSequence name) {
        if (metadata.getColumnIndexQuiet(name) < 0) {
            return Chars.toString(name);
        }
        for (int i = 1; ; i++) {
            final String candidate = name + Integer.toString(i);
            if (metadata.getColumnIndexQuiet(candidate) < 0) {
                return candidate;
            }
        }
    }

    private void addColumnReferences(GenerationFrame frame, BoundExpression expression, int count) {
        if (expression instanceof ColumnExpression column) {
            final int columnId = column.getColumnId();
            frame.setReferenceCount(columnId, Math.min(2, frame.getReferenceCount(columnId) + count));
        } else if (expression instanceof FunctionExpression function) {
            for (int i = 0, n = function.getArgumentCount(); i < n; i++) {
                addColumnReferences(frame, function.argumentAt(i), count);
            }
        }
    }

    private RecordCursorFactory generateVirtualProjection(GenerationFrame frame, ProjectPlan project, RecordCursorFactory base, int timestampIndex,
                                                          SqlExecutionContext executionContext) throws SqlException {
        final int count = project.getExpressions().size();
        ObjList<Function> functions = null;
        final GenericRecordMetadata metadata;
        final PriorityMetadata priorityMetadata;
        final int reservedSlots;
        try {
            final OutputSchema input = project.getChild().getOutput();
            functions = new ObjList<>(count);
            metadata = new GenericRecordMetadata();
            // A column that reads an earlier column by alias reads that column's slot of this record,
            // so every column addresses the base past the reserved prefix.
            reservedSlots = !project.hasUpdateConversions() && hasProjectionReferences(project) ? count : 0;
            priorityMetadata = new PriorityMetadata(reservedSlots, base.getMetadata());
            final OutputSchema scope;
            final GenericRecordMetadata scopeMetadata;
            if (reservedSlots > 0) {
                scope = frame.schema;
                scope.clear();
                scopeMetadata = frame.projectionScopeMetadata;
                scopeMetadata.clear();
                for (int i = 0; i < count; i++) {
                    scope.add(project.getOutput().getColumnId(i), project.getOutput().getColumnName(i), project.getOutput().getColumnType(i), true);
                    scopeMetadata.add(frame.projectionSlotColumn(i, project.getOutput().getColumnType(i)));
                }
                for (int i = 0, n = input.getColumnCount(); i < n; i++) {
                    scope.add(input.getColumnId(i), input.getColumnName(i), input.getColumnType(i), input.getMetadata(i), true);
                    scopeMetadata.add(base.getMetadata().getColumnMetadata(i));
                }
            } else {
                scope = input;
                scopeMetadata = null;
            }
            for (int i = 0; i < count; i++) {
                final BoundExpression expression = project.getExpressions().getQuick(i);
                Function function;
                final int columnIndex;
                if (project.hasUpdateConversions()) {
                    columnIndex = expression instanceof ColumnExpression column ? input.getColumnIndexById(column.getColumnId()) : -1;
                    function = functionInstantiator.instantiateUpdateAssignment(expression, project.getUpdateTargetTypes().getQuick(i),
                            input, base.getMetadata(), executionContext);
                } else if (expression instanceof ColumnExpression column) {
                    columnIndex = input.getColumnIndexById(column.getColumnId());
                    function = reservedSlots == 0 ? FunctionResolver.createColumn(expression.getPosition(), columnIndex, base.getMetadata())
                            : FunctionResolver.createColumn(expression.getPosition(),
                            columnIndex >= 0 ? reservedSlots + columnIndex : scope.getColumnIndexById(column.getColumnId()), scopeMetadata);
                } else {
                    columnIndex = -1;
                    function = reservedSlots == 0 ? functionInstantiator.instantiate(expression, input, base.getMetadata(), executionContext)
                            : functionInstantiator.instantiate(expression, scope, scopeMetadata, executionContext);
                }
                try {
                    if (project.hasUpdateConversions() && LogicalPlans.updateColumnType(function.getType(), project.getUpdateTargetTypes().getQuick(i))
                            != project.getOutput().getColumnType(i)) {
                        throw new IllegalStateException("UPDATE assignment output type has changed");
                    }
                    function = memoizeProjectionFunction(function, frame.getReferenceCount(project.getOutput().getColumnId(i)));
                } catch (Throwable th) {
                    Misc.free(function, th);
                    throw th;
                }
                functions.add(function);
                final TableColumnMetadata columnMetadata = new TableColumnMetadata(
                        uniqueColumnName(metadata, project.getOutput().getColumnName(i)), project.getOutput().getColumnType(i),
                        IndexType.NONE, 0, project.hasUpdateConversions()
                        ? function instanceof SymbolFunction symbol && symbol.isSymbolTableStatic()
                        : columnIndex >= 0 && base.getMetadata().isSymbolTableStatic(columnIndex),
                        function.getMetadata()
                );
                if (columnIndex >= 0) {
                    columnMetadata.setParquetEncodingConfig(base.getMetadata().getColumnMetadata(columnIndex).getParquetEncodingConfig());
                }
                metadata.add(columnMetadata);
                if (reservedSlots > 0) {
                    priorityMetadata.add(columnMetadata);
                }
            }
            if (timestampIndex >= 0 && !ColumnType.isTimestamp(metadata.getColumnType(timestampIndex))) {
                throw SqlException.$(project.getExpressions().getQuick(timestampIndex).getPosition(), "TIMESTAMP column is required but not provided");
            }
            metadata.setTimestampIndex(timestampIndex);
        } catch (Throwable th) {
            Misc.freeObjList(functions, th);
            Misc.free(base, th);
            throw th;
        }
        return new VirtualRecordCursorFactory(metadata, priorityMetadata, functions, base, reservedSlots);
    }

    static Function memoizeProjectionFunction(Function function, int referenceCount) {
        if (function == null || function.isConstant()) {
            return function;
        }
        final boolean isVolatileReadTwice = referenceCount > 1 && function.isNonDeterministic() && !function.isStableWithinExecution();
        if (!isVolatileReadTwice && (!SqlCodeGenerator.ALLOW_FUNCTION_MEMOIZATION || referenceCount <= 1 && !function.shouldMemoize())) {
            return function;
        }
        if (ColumnType.tagOf(function.getType()) == ColumnType.BINARY && !isVolatileReadTwice) {
            return function;
        }
        final MemoizerFunction memoizer = MemoizerFunction.newInstance(function);
        return memoizer != null ? memoizer : function;
    }

    /**
     * Fills the metadata and the input column indexes of the selection the generator builds for a projection of
     * plain columns over a factory with {@code baseMetadata}.
     */
    static void selectColumns(ProjectPlan project, RecordMetadata baseMetadata, GenericRecordMetadata metadata, IntList mapping) {
        final OutputSchema input = project.getChild().getOutput();
        for (int i = 0, n = project.getExpressions().size(); i < n; i++) {
            final int index = input.getColumnIndexById(((ColumnExpression) project.getExpressions().getQuick(i)).getColumnId());
            mapping.add(index);
            final CharSequence name = project.getOutput().getColumnName(i);
            metadata.add(Chars.equals(baseMetadata.getColumnName(index), name)
                    ? baseMetadata.getColumnMetadata(index)
                    : SqlCodeGenerator.copyColumn(baseMetadata, index, Chars.toString(name)));
        }
        metadata.setTimestampIndex(PlanToFactoryRules.projectedTimestampIndex(project, baseMetadata.getTimestampIndex()));
    }

    void collectColumnReferenceCounts(GenerationFrame frame, LogicalPlan plan) {
        switch (plan) {
            case ProjectPlan project -> {
                // Later columns first: a column read by alias counts those reads before its own inputs.
                for (int i = project.getExpressions().size() - 1; i >= 0; i--) {
                    final int columnId = project.getOutput().getColumnId(i);
                    final BoundExpression expression = project.getExpressions().getQuick(i);
                    if (!(expression instanceof ColumnExpression column) || column.getColumnId() != columnId) {
                        addColumnReferences(frame, expression, frame.getReferenceCount(columnId));
                    }
                }
            }
            case FilterPlan filter -> addColumnReferences(frame, filter.getPredicate(), 1);
            case SortPlan sort -> {
                // A sort key that a projection passes through reads the projection's input column.
                final LogicalPlan input = sort.getChild();
                final IntList columns = sort.getColumnIds();
                for (int i = 0, n = columns.size(); i < n; i++) {
                    int columnId = columns.getQuick(i);
                    if (input instanceof ProjectPlan project) {
                        final int index = project.getOutput().getColumnIndexById(columnId);
                        if (index >= 0 && project.getExpressions().getQuick(index) instanceof ColumnExpression column) {
                            columnId = column.getColumnId();
                        }
                    }
                    frame.setReferenceCount(columnId, Math.min(2, frame.getReferenceCount(columnId) + 1));
                }
            }
            case GroupingPlan aggregate -> {
                setReferenceCounts(frame, aggregate.getChild().getOutput(), 0);
                final int timestampId = aggregate instanceof SampleByPlan sample ? sample.getTimestampColumnId() : -1;
                if (timestampId >= 0) {
                    frame.setReferenceCount(timestampId, 1);
                }
                for (int i = 0, n = aggregate.getGroupingExpressions().size(); i < n; i++) {
                    final BoundExpression key = aggregate.getGroupingExpressions().getQuick(i);
                    if (!(key instanceof ColumnExpression column) || column.getColumnId() != timestampId) {
                        addColumnReferences(frame, key, 1);
                    }
                }
                for (int i = 0, n = aggregate.getAggregates().size(); i < n; i++) {
                    addColumnReferences(frame, aggregate.getAggregates().getQuick(i), 1);
                }
            }
            case WindowPlan window -> {
                for (int i = 0, n = window.getFunctions().size(); i < n; i++) {
                    addColumnReferences(frame, window.getFunctions().getQuick(i), 1);
                    final WindowSpec spec = window.getSpecs().getQuick(i);
                    for (int j = 0, m = spec.getPartitionBy().size(); j < m; j++) {
                        addColumnReferences(frame, spec.getPartitionBy().getQuick(j), 1);
                    }
                    final IntList columns = spec.getOrderByColumnIds();
                    for (int j = 0, m = columns.size(); j < m; j++) {
                        final int columnId = columns.getQuick(j);
                        frame.setReferenceCount(columnId, Math.min(2, frame.getReferenceCount(columnId) + 1));
                    }
                }
            }
            case SetOperationPlan _ -> {
                for (int i = 0, n = plan.inputCount(); i < n; i++) {
                    final OutputSchema input = plan.inputAt(i).getOutput();
                    for (int j = 0, m = input.getColumnCount(); j < m; j++) {
                        frame.setReferenceCount(input.getColumnId(j), frame.getReferenceCount(plan.getOutput().getColumnId(j)));
                    }
                }
            }
            case JoinPlan join -> {
                setInputReferenceCounts(frame, join);
                final ObjList<JoinInput> inputs = join.getInputs();
                for (int i = 0, n = inputs.size(); i < n; i++) {
                    final JoinInput step = inputs.getQuick(i);
                    if (step.getUnnest() != null) {
                        final ObjList<BoundExpression> expressions = step.getUnnest().getExpressions();
                        for (int j = 0, m = expressions.size(); j < m; j++) {
                            addColumnReferences(frame, expressions.getQuick(j), 1);
                        }
                    }
                }
            }
            case WindowJoinPlan _, HorizonJoinPlan _, DistinctPlan _, LatestByPlan _, FillPlan _ ->
                    setInputReferenceCounts(frame, plan);
            default -> {
            }
        }
        for (int i = 0, n = plan.inputCount(); i < n; i++) {
            collectColumnReferenceCounts(frame, plan.inputAt(i));
        }
    }

    RecordCursorFactory generateProjection(GenerationFrame frame, ProjectPlan project, RecordCursorFactory base, SqlExecutionContext executionContext)
            throws SqlException {
        final int timestampIndex;
        try {
            timestampIndex = PlanToFactoryRules.projectedTimestampIndex(project, base.getMetadata().getTimestampIndex());
            if (ParanoiaState.PLAN_PARANOIA_MODE && isPlannedIdentityChecked(project)
                    && PlanToFactoryRules.isIdentityProjection(project) != PlanToFactoryRules.isIdentityProjection(project, base.getMetadata(), timestampIndex, base.getMetadata().getTimestampIndex())) {
                throw new AssertionError("planned identity projection differs from the generator's");
            }
        } catch (Throwable th) {
            Misc.free(base, th);
            throw th;
        }
        return generateProjection(frame, project, base, timestampIndex, executionContext);
    }

    /**
     * Builds the projection over the factory, designating the timestamp at {@code timestampIndex}, -1 for none.
     */
    RecordCursorFactory generateProjection(
            GenerationFrame frame,
            ProjectPlan project,
            RecordCursorFactory base,
            int timestampIndex,
            SqlExecutionContext executionContext
    ) throws SqlException {
        final LogicalPlan input = project.getChild();
        if (LogicalPlans.isComputedProjection(project)) {
            return generateVirtualProjection(frame, project, base, timestampIndex, executionContext);
        }
        final IntList mapping;
        final GenericRecordMetadata metadata;
        try {
            if (PlanToFactoryRules.isIdentityProjection(project, base.getMetadata(), timestampIndex, base.getMetadata().getTimestampIndex())) {
                return base;
            }
            mapping = new IntList(project.getExpressions().size());
            metadata = new GenericRecordMetadata();
            selectColumns(project, base.getMetadata(), metadata, mapping);
            metadata.setTimestampIndex(timestampIndex);
        } catch (Throwable th) {
            Misc.free(base, th);
            throw th;
        }
        return newSelected(metadata, mapping, base, input);
    }

    void setReferenceCounts(GenerationFrame frame, OutputSchema output, int count) {
        for (int i = 0, n = output.getColumnCount(); i < n; i++) {
            frame.setReferenceCount(output.getColumnId(i), count);
        }
    }
}
