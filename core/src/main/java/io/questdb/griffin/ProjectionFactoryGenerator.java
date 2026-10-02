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

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.GenericRecordMetadata;
import io.questdb.cairo.IndexType;
import io.questdb.cairo.TableColumnMetadata;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.griffin.engine.functions.SymbolFunction;
import io.questdb.griffin.engine.functions.memoization.ArrayFunctionMemoizer;
import io.questdb.griffin.engine.functions.memoization.BinFunctionMemoizer;
import io.questdb.griffin.engine.functions.memoization.BooleanFunctionMemoizer;
import io.questdb.griffin.engine.functions.memoization.ByteFunctionMemoizer;
import io.questdb.griffin.engine.functions.memoization.CharFunctionMemoizer;
import io.questdb.griffin.engine.functions.memoization.DateFunctionMemoizer;
import io.questdb.griffin.engine.functions.memoization.DecimalFunctionMemoizer;
import io.questdb.griffin.engine.functions.memoization.DoubleFunctionMemoizer;
import io.questdb.griffin.engine.functions.memoization.FloatFunctionMemoizer;
import io.questdb.griffin.engine.functions.memoization.GeoHashFunctionMemoizer;
import io.questdb.griffin.engine.functions.memoization.IPv4FunctionMemoizer;
import io.questdb.griffin.engine.functions.memoization.IntFunctionMemoizer;
import io.questdb.griffin.engine.functions.memoization.IntervalFunctionMemoizer;
import io.questdb.griffin.engine.functions.memoization.Long128FunctionMemoizer;
import io.questdb.griffin.engine.functions.memoization.Long256FunctionMemoizer;
import io.questdb.griffin.engine.functions.memoization.LongFunctionMemoizer;
import io.questdb.griffin.engine.functions.memoization.ShortFunctionMemoizer;
import io.questdb.griffin.engine.functions.memoization.StrFunctionMemoizer;
import io.questdb.griffin.engine.functions.memoization.SymbolFunctionMemoizer;
import io.questdb.griffin.engine.functions.memoization.TimestampFunctionMemoizer;
import io.questdb.griffin.engine.functions.memoization.UuidFunctionMemoizer;
import io.questdb.griffin.engine.functions.memoization.VarcharFunctionMemoizer;
import io.questdb.griffin.engine.table.SelectedRecordCursorFactory;
import io.questdb.griffin.engine.table.VirtualRecordCursorFactory;
import io.questdb.griffin.plan.logical.AggregatePlan;
import io.questdb.griffin.plan.logical.BoundExpression;
import io.questdb.griffin.plan.logical.ColumnExpression;
import io.questdb.griffin.plan.logical.DistinctPlan;
import io.questdb.griffin.plan.logical.FillPlan;
import io.questdb.griffin.plan.logical.FilterPlan;
import io.questdb.griffin.plan.logical.FunctionExpression;
import io.questdb.griffin.plan.logical.HorizonJoinPlan;
import io.questdb.griffin.plan.logical.JoinInput;
import io.questdb.griffin.plan.logical.JoinPlan;
import io.questdb.griffin.plan.logical.LatestByPlan;
import io.questdb.griffin.plan.logical.LogicalPlan;
import io.questdb.griffin.plan.logical.OutputSchema;
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
    private final IntList projectionInputIds;
    private final IntList projectionOrder;

    ProjectionFactoryGenerator(IntList projectionOrder, IntList projectionInputIds) {
        this.projectionOrder = projectionOrder;
        this.projectionInputIds = projectionInputIds;
    }

    private static boolean hasExplicitJoinTimestamp(LogicalPlan plan) {
        plan = LogicalPlans.skipFilters(plan);
        return plan instanceof JoinPlan join && join.hasExplicitTimestamp();
    }

    private static boolean hasProjectionReferences(ProjectPlan project) {
        for (int i = 0, n = project.getExpressions().size(); i < n; i++) {
            if (referencesOutput(project.getExpressions().getQuick(i), project.getOutput(), project.getInput().getOutput())) {
                return true;
            }
        }
        return false;
    }

    private static RecordCursorFactory newSelected(GenericRecordMetadata metadata, IntList mapping, RecordCursorFactory base, LogicalPlan input) {
        if (input.getType() == LogicalPlan.Type.JOIN && base instanceof SelectedRecordCursorFactory selected) {
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
        final boolean isJoin = plan.getType() == LogicalPlan.Type.JOIN;
        for (int i = 0, n = plan.inputCount(); i < n; i++) {
            final boolean isPassThrough = isJoin && LogicalPlans.skipFilters(plan.inputAt(i)).getType() != LogicalPlan.Type.PROJECT;
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

    private void collectProjectionInputIds(BoundExpression expression, OutputSchema input) {
        if (expression instanceof ColumnExpression column) {
            final int columnId = column.getColumnId();
            if (input.getColumnIndexById(columnId) >= 0 && projectionInputIds.indexOf(columnId, 0, projectionInputIds.size()) < 0) {
                projectionInputIds.add(columnId);
            }
        } else if (expression instanceof FunctionExpression call) {
            final int count = call.getArgumentCount();
            // Immediate column arguments come before nested ones, the last argument of three or more first.
            for (int i = count < 3 ? count - 1 : count - 2; i >= 0; i--) {
                if (call.argumentAt(i) instanceof ColumnExpression) {
                    collectProjectionInputIds(call.argumentAt(i), input);
                }
            }
            if (count >= 3) {
                collectProjectionInputIds(call.argumentAt(count - 1), input);
            }
            for (int i = 0, n = count < 3 ? count : count - 1; i < n; i++) {
                if (!(call.argumentAt(i) instanceof ColumnExpression)) {
                    collectProjectionInputIds(call.argumentAt(i), input);
                }
            }
        }
    }

    private int generateVirtualProjection(GenerationFrame frame, ProjectPlan project, int inputSlot, int slot, int timestampIndex, SqlExecutionContext executionContext) throws SqlException {
        final OutputSchema input = project.getInput().getOutput();
        final RecordCursorFactory base = (RecordCursorFactory) frame.resources.resources.getQuick(inputSlot);
        final int count = project.getExpressions().size();
        final ObjList<Function> functions = new ObjList<>(count);
        final GenericRecordMetadata metadata = new GenericRecordMetadata();
        // A column that reads an earlier column by alias reads that column's slot of this record,
        // so every column addresses the base past the reserved prefix.
        final int reservedSlots = !project.hasUpdateConversions() && hasProjectionReferences(project) ? count : 0;
        final PriorityMetadata priorityMetadata = new PriorityMetadata(reservedSlots, base.getMetadata());
        final OutputSchema scope;
        final GenericRecordMetadata scopeMetadata;
        if (reservedSlots > 0) {
            scope = frame.projectionScope;
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
        final int firstFunctionSlot = frame.resources.resources.size();
        for (int i = 0; i < count; i++) {
            final int functionSlot = frame.resources.reserve();
            final BoundExpression expression = project.getExpressions().getQuick(i);
            Function function;
            final int columnIndex;
            if (project.hasUpdateConversions()) {
                columnIndex = expression instanceof ColumnExpression column ? input.getColumnIndexById(column.getColumnId()) : -1;
                function = frame.functionBinder.instantiateUpdateAssignment(expression, project.getUpdateTargetTypes().getQuick(i),
                        input, base.getMetadata(), executionContext);
            } else if (expression instanceof ColumnExpression column) {
                columnIndex = input.getColumnIndexById(column.getColumnId());
                function = reservedSlots == 0 ? FunctionParser.createColumn(expression.getPosition(), columnIndex, base.getMetadata())
                        : FunctionParser.createColumn(expression.getPosition(),
                        columnIndex >= 0 ? reservedSlots + columnIndex : scope.getColumnIndexById(column.getColumnId()), scopeMetadata);
            } else {
                columnIndex = -1;
                function = reservedSlots == 0 ? frame.functionBinder.instantiate(expression, input, base.getMetadata(), executionContext)
                        : frame.functionBinder.instantiate(expression, scope, scopeMetadata, executionContext);
            }
            try {
                if (project.hasUpdateConversions() && FunctionBinder.updateColumnType(function, project.getUpdateTargetTypes().getQuick(i))
                        != project.getOutput().getColumnType(i)) {
                    throw new IllegalStateException("UPDATE assignment output type has changed");
                }
                function = memoizeProjectionFunction(function, frame.getReferenceCount(project.getOutput().getColumnId(i)));
            } catch (Throwable th) {
                Misc.free(function, th);
                throw th;
            }
            frame.resources.own(functionSlot, function);
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
        // The constructor consumes the source and all functions, including on failure.
        frame.resources.detach(inputSlot);
        for (int i = 0; i < count; i++) {
            frame.resources.detach(firstFunctionSlot + i);
        }
        frame.resources.own(slot, new VirtualRecordCursorFactory(metadata, priorityMetadata, functions, base, reservedSlots));
        return slot;
    }

    /**
     * A computing projection over a window join keeps the designated timestamp only when the master
     * timestamp holds its master position among the input columns the projection reads, taken in
     * SELECT-list order.
     */
    private boolean isWindowJoinTimestampKept(ProjectPlan project, WindowJoinPlan windowJoin) {
        final OutputSchema input = project.getInput().getOutput();
        final ObjList<BoundExpression> expressions = project.getExpressions();
        projectionOrder.clear();
        for (int i = 0, n = expressions.size(); i < n; i++) {
            final int position = expressions.getQuick(i).getPosition();
            int index = projectionOrder.size();
            while (index > 0 && position < expressions.getQuick(projectionOrder.getQuick(index - 1)).getPosition()) {
                index--;
            }
            projectionOrder.insert(index, i);
        }
        projectionInputIds.clear();
        for (int i = 0, n = projectionOrder.size(); i < n; i++) {
            collectProjectionInputIds(expressions.getQuick(projectionOrder.getQuick(i)), input);
        }
        final OutputSchema master = windowJoin.getMaster().getOutput();
        final int index = master.getTimestampIndex();
        return index >= 0 && index < projectionInputIds.size() && projectionInputIds.getQuick(index) == master.getColumnId(index);
    }

    static Function memoizeProjectionFunction(Function function, int referenceCount) {
        if (function == null || function.isConstant()) {
            return function;
        }
        final boolean isVolatileReadTwice = referenceCount > 1 && function.isNonDeterministic() && !function.isStableWithinExecution();
        if (!isVolatileReadTwice && (!SqlCodeGenerator.ALLOW_FUNCTION_MEMOIZATION || referenceCount <= 1 && !function.shouldMemoize())) {
            return function;
        }
        return switch (ColumnType.tagOf(function.getType())) {
            case ColumnType.LONG -> new LongFunctionMemoizer(function);
            case ColumnType.INT -> new IntFunctionMemoizer(function);
            case ColumnType.TIMESTAMP -> new TimestampFunctionMemoizer(function);
            case ColumnType.DOUBLE -> new DoubleFunctionMemoizer(function);
            case ColumnType.SHORT -> new ShortFunctionMemoizer(function);
            case ColumnType.BOOLEAN -> new BooleanFunctionMemoizer(function);
            case ColumnType.BYTE -> new ByteFunctionMemoizer(function);
            case ColumnType.CHAR -> new CharFunctionMemoizer(function);
            case ColumnType.DATE -> new DateFunctionMemoizer(function);
            case ColumnType.FLOAT -> new FloatFunctionMemoizer(function);
            case ColumnType.IPv4 -> new IPv4FunctionMemoizer(function);
            case ColumnType.UUID -> new UuidFunctionMemoizer(function);
            case ColumnType.LONG256 -> new Long256FunctionMemoizer(function);
            case ColumnType.DECIMAL8, ColumnType.DECIMAL16, ColumnType.DECIMAL32,
                 ColumnType.DECIMAL64, ColumnType.DECIMAL128, ColumnType.DECIMAL256 ->
                    new DecimalFunctionMemoizer(function);
            case ColumnType.ARRAY -> new ArrayFunctionMemoizer(function);
            case ColumnType.STRING -> new StrFunctionMemoizer(function);
            case ColumnType.VARCHAR, ColumnType.VARCHAR_SLICE -> new VarcharFunctionMemoizer(function);
            case ColumnType.SYMBOL -> new SymbolFunctionMemoizer(function);
            case ColumnType.GEOBYTE, ColumnType.GEOSHORT, ColumnType.GEOINT, ColumnType.GEOLONG -> new GeoHashFunctionMemoizer(function);
            case ColumnType.BINARY -> new BinFunctionMemoizer(function);
            case ColumnType.LONG128 -> new Long128FunctionMemoizer(function);
            case ColumnType.INTERVAL -> new IntervalFunctionMemoizer(function);
            default -> function;
        };
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
                final LogicalPlan input = sort.getInput();
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
            case AggregatePlan aggregate -> {
                setReferenceCounts(frame, aggregate.getInput().getOutput(), 0);
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
            case WindowJoinPlan _, HorizonJoinPlan _, DistinctPlan _, LatestByPlan _, FillPlan _ -> setInputReferenceCounts(frame, plan);
            default -> {
            }
        }
        for (int i = 0, n = plan.inputCount(); i < n; i++) {
            collectColumnReferenceCounts(frame, plan.inputAt(i));
        }
    }

    int generateProjection(GenerationFrame frame, ProjectPlan project, int inputSlot, int slot, int timestampIndex, SqlExecutionContext executionContext) throws SqlException {
        final LogicalPlan input = project.getInput();
        final RecordCursorFactory base = (RecordCursorFactory) frame.resources.resources.getQuick(inputSlot);
        if (!project.hasTimestampDeclaration() && timestampIndex >= 0 && base.getMetadata().getTimestampIndex() < 0
                && !hasExplicitJoinTimestamp(input)
                && project.getExpressions().getQuick(timestampIndex) instanceof ColumnExpression timestamp
                && (input.getOutput().getTimestampIndex() < 0 || timestamp.getColumnId() == input.getOutput().getTimestampColumnId())) {
            timestampIndex = -1;
        }
        if (timestampIndex >= 0 && input instanceof WindowJoinPlan windowJoin && !SqlCodeGenerator.isColumnOnlyProjection(project)
                && !isWindowJoinTimestampKept(project, windowJoin)) {
            timestampIndex = -1;
        }
        if (project.hasUpdateConversions() || project.hasPrunedComputedColumns()) {
            return generateVirtualProjection(frame, project, inputSlot, slot, timestampIndex, executionContext);
        }
        for (int i = 0, n = project.getExpressions().size(); i < n; i++) {
            if (!(project.getExpressions().getQuick(i) instanceof ColumnExpression column) || column.isCast()) {
                return generateVirtualProjection(frame, project, inputSlot, slot, timestampIndex, executionContext);
            }
        }
        // A SELECT list over GROUP BY keeps the key spelling when it only changes the name case.
        final LogicalPlan source = LogicalPlans.skipFilters(input);
        final boolean isKeySpellingKept = source instanceof AggregatePlan aggregate && source.getType() == LogicalPlan.Type.AGGREGATE
                && aggregate.hasKeySpellingKept() && aggregate.getInput().getType() != LogicalPlan.Type.HORIZON_JOIN;
        boolean isIdentity = project.getOutput().getColumnCount() == base.getMetadata().getColumnCount()
                && timestampIndex == base.getMetadata().getTimestampIndex();
        for (int i = 0, n = project.getExpressions().size(); isIdentity && i < n; i++) {
            final ColumnExpression column = (ColumnExpression) project.getExpressions().getQuick(i);
            final CharSequence name = project.getOutput().getColumnName(i);
            isIdentity = input.getOutput().getColumnIndexById(column.getColumnId()) == i
                    && project.getOutput().getColumnType(i) == base.getMetadata().getColumnType(i)
                    && (isKeySpellingKept ? Chars.equalsIgnoreCase(name, base.getMetadata().getColumnName(i)) : Chars.equals(name, base.getMetadata().getColumnName(i)));
        }
        if (isIdentity) {
            return inputSlot;
        }
        final IntList mapping = new IntList(project.getExpressions().size());
        final GenericRecordMetadata metadata = new GenericRecordMetadata();
        for (int i = 0, n = project.getExpressions().size(); i < n; i++) {
            final int index = input.getOutput().getColumnIndexById(((ColumnExpression) project.getExpressions().getQuick(i)).getColumnId());
            mapping.add(index);
            metadata.add(SqlCodeGenerator.copyColumn(base.getMetadata(), index, Chars.toString(project.getOutput().getColumnName(i))));
        }
        metadata.setTimestampIndex(timestampIndex);
        final RecordCursorFactory factory = newSelected(metadata, mapping, base, input);
        frame.resources.detach(inputSlot);
        frame.resources.own(slot, factory);
        return slot;
    }

    void setReferenceCounts(GenerationFrame frame, OutputSchema output, int count) {
        for (int i = 0, n = output.getColumnCount(); i < n; i++) {
            frame.setReferenceCount(output.getColumnId(i), count);
        }
    }
}
