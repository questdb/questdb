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

package io.questdb.griffin.optimiser;

import io.questdb.griffin.LogicalPlans;
import io.questdb.griffin.SetOperationCasts;
import io.questdb.griffin.plan.logical.AggregatePlan;
import io.questdb.griffin.plan.logical.BoundExpression;
import io.questdb.griffin.plan.logical.ColumnExpression;
import io.questdb.griffin.plan.logical.DistinctPlan;
import io.questdb.griffin.plan.logical.DomainReadPlan;
import io.questdb.griffin.plan.logical.FillMode;
import io.questdb.griffin.plan.logical.FillPlan;
import io.questdb.griffin.plan.logical.FilterPlan;
import io.questdb.griffin.plan.logical.ForwardingPlan;
import io.questdb.griffin.plan.logical.FunctionExpression;
import io.questdb.griffin.plan.logical.FunctionSourcePlan;
import io.questdb.griffin.plan.logical.GroupingPlan;
import io.questdb.griffin.plan.logical.HorizonJoinPlan;
import io.questdb.griffin.plan.logical.HorizonJoinSlave;
import io.questdb.griffin.plan.logical.JoinInput;
import io.questdb.griffin.plan.logical.JoinPlan;
import io.questdb.griffin.plan.logical.LatestByPlan;
import io.questdb.griffin.plan.logical.LimitPlan;
import io.questdb.griffin.plan.logical.LogicalPlan;
import io.questdb.griffin.plan.logical.OutputSchema;
import io.questdb.griffin.plan.logical.ProjectPlan;
import io.questdb.griffin.plan.logical.SampleByPlan;
import io.questdb.griffin.plan.logical.ScanPlan;
import io.questdb.griffin.plan.logical.SetOperationPlan;
import io.questdb.griffin.plan.logical.SortPlan;
import io.questdb.griffin.plan.logical.UnaryPlan;
import io.questdb.griffin.plan.logical.UnnestSpec;
import io.questdb.griffin.plan.logical.WindowJoinPlan;
import io.questdb.griffin.plan.logical.WindowJoinStep;
import io.questdb.griffin.plan.logical.WindowPlan;
import io.questdb.griffin.plan.logical.WindowSpec;
import io.questdb.std.BitSet;
import io.questdb.std.Chars;
import io.questdb.std.IntHashSet;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;
import io.questdb.std.ObjectPool;

/**
 * Removes the columns no consumer reads, starting from the root's output, with the window calls that
 * compute them, and lays out the outputs of scans, joins and windows over the surviving columns.
 */
final class ColumnPruning implements OptimiserPass {
    private final AggregateInputOrder aggregateInputOrder;
    private final ObjList<LogicalPlan> alignedPath;
    private final IntHashSet childInputIds = new IntHashSet();
    private final ColumnReads columnReads;
    private final ObjectPool<ColumnExpression> narrowingColumns;
    private final ObjectPool<ProjectPlan> narrowingProjects;
    private final ProjectionMerge projectionMerge;
    private final ObjList<LogicalPlan> pruneAncestors;
    private final IntHashSet requiredColumnIds;
    private final IntList remappedSymbolColumns;
    private final IntList retainedColumnIndexes;
    private final BitSet retainedColumns = new BitSet();
    private final IntHashSet retainedProjectOutputIds = new IntHashSet();
    private final ObjList<BoundExpression> tmpExpressions;
    private final OutputSchema tmpSchema;
    private LogicalPlan joinInputPlan;

    ColumnPruning(
            AggregateInputOrder aggregateInputOrder,
            ProjectionMerge projectionMerge,
            ColumnReads columnReads,
            ObjList<BoundExpression> tmpExpressions,
            ObjectPool<ColumnExpression> narrowingColumns,
            ObjectPool<ProjectPlan> narrowingProjects,
            IntList retainedColumnIndexes,
            IntList remappedSymbolColumns,
            IntHashSet requiredColumnIds,
            ObjList<LogicalPlan> pruneAncestors,
            ObjList<LogicalPlan> alignedPath,
            OutputSchema tmpSchema
    ) {
        this.aggregateInputOrder = aggregateInputOrder;
        this.projectionMerge = projectionMerge;
        this.columnReads = columnReads;
        this.tmpExpressions = tmpExpressions;
        this.narrowingColumns = narrowingColumns;
        this.narrowingProjects = narrowingProjects;
        this.retainedColumnIndexes = retainedColumnIndexes;
        this.remappedSymbolColumns = remappedSymbolColumns;
        this.requiredColumnIds = requiredColumnIds;
        this.pruneAncestors = pruneAncestors;
        this.alignedPath = alignedPath;
        this.tmpSchema = tmpSchema;
    }

    @Override
    public LogicalPlan apply(LogicalPlan plan) {
        prune(plan);
        return plan;
    }

    @Override
    public void clear() {
        joinInputPlan = null;
        columnReads.clear();
        childInputIds.clear();
        retainedProjectOutputIds.clear();
    }

    @Override
    public String getName() {
        return "column pruning";
    }

    private void alignScanColumns(ProjectPlan project) {
        final WindowJoinPlan windowJoin = project.getChild() instanceof WindowJoinPlan join ? join : null;
        final LogicalPlan top = windowJoin != null ? windowJoin.getMaster() : project.getChild();
        LogicalPlan input = top;
        boolean hasWindow = false;
        while (input instanceof FilterPlan || input instanceof SortPlan || input instanceof LimitPlan
                || input instanceof LatestByPlan || input instanceof WindowPlan) {
            hasWindow |= input instanceof WindowPlan;
            input = input.inputAt(0);
        }
        final IntList sourceIndexes;
        if (input instanceof ScanPlan scanPlan) {
            sourceIndexes = scanPlan.getSourceColumnIndexes();
        } else if (input instanceof FunctionSourcePlan source && source.isProjectable()) {
            sourceIndexes = source.getSourceColumnIndexes();
        } else {
            return;
        }
        final LogicalPlan scan = input;
        final OutputSchema output = scan.getOutput();
        retainedColumnIndexes.clear();
        retainedColumns.clear();
        for (int i = 0, n = project.getExpressions().size(); i < n; i++) {
            final BoundExpression expression = project.getExpressions().getQuick(i);
            if (expression instanceof FunctionExpression) {
                retainReferencedColumns(expression, output);
                continue;
            }
            if (!(expression instanceof ColumnExpression column) || !column.isDirectReference()) {
                return;
            }
            final int index = output.getColumnIndexById(column.getColumnId());
            if (index < 0 && (windowJoin != null || hasWindow || project.getOutput().getColumnIndexById(column.getColumnId()) >= 0)) {
                continue;
            }
            assert index >= 0;
            retainColumn(index);
        }
        for (int i = 0, n = output.getColumnCount(); i < n; i++) {
            retainColumn(i);
        }
        boolean isReordered = false;
        for (int i = 0, n = retainedColumnIndexes.size(); i < n; i++) {
            isReordered |= retainedColumnIndexes.getQuick(i) != i;
        }
        if (!isReordered) {
            return;
        }
        output.retain(retainedColumnIndexes);
        for (int i = 0, n = retainedColumnIndexes.size(); i < n; i++) {
            retainedColumnIndexes.setQuick(i, sourceIndexes.getQuick(retainedColumnIndexes.getQuick(i)));
        }
        sourceIndexes.clear();
        sourceIndexes.addAll(retainedColumnIndexes);
        alignedPath.clear();
        for (input = top; input != scan; input = input.inputAt(0)) {
            alignedPath.add(input);
        }
        for (int k = alignedPath.size() - 1; k >= 0; k--) {
            final LogicalPlan node = alignedPath.getQuick(k);
            if (node instanceof WindowPlan window) {
                window.deriveOutput(tmpSchema);
            } else {
                ((ForwardingPlan) node).deriveOutput();
            }
        }
        alignedPath.clear();
        if (windowJoin != null) {
            rebuildWindowJoinSchemas(windowJoin);
        }
    }

    private boolean hasRequiredColumn(OutputSchema output) {
        for (int i = 0, n = output.getColumnCount(); i < n; i++) {
            if (requiredColumnIds.contains(output.getColumnId(i))) {
                return true;
            }
        }
        return false;
    }

    private boolean isJoinSource() {
        for (int i = pruneAncestors.size() - 2; i >= 0; i--) {
            final LogicalPlan ancestor = pruneAncestors.getQuick(i);
            if (!(ancestor instanceof FilterPlan)) {
                return ancestor instanceof JoinPlan;
            }
        }
        return false;
    }

    // A filter or join reads a non-projectable table function's full record; narrowing it would only add a mapping.
    private boolean isUnnarrowedFunctionSource(FunctionSourcePlan source) {
        if (source.isProjectable()) {
            return false;
        }
        final int depth = pruneAncestors.size();
        return source == joinInputPlan || depth > 1 && pruneAncestors.getQuick(depth - 2) instanceof FilterPlan;
    }

    private void permuteProjection(ProjectPlan project) {
        final ObjList<BoundExpression> expressions = project.getExpressions();
        final OutputSchema output = project.getOutput();
        tmpExpressions.clear();
        tmpExpressions.addAll(expressions);
        expressions.clear();
        for (int i = 0, n = retainedColumnIndexes.size(); i < n; i++) {
            expressions.add(tmpExpressions.getQuick(retainedColumnIndexes.getQuick(i)));
        }
        output.retain(retainedColumnIndexes);
        tmpExpressions.clear();
    }

    private void prune(LogicalPlan root) {
        pruneAncestors.clear();
        columnReads.of(root, requiredColumnIds);
        pruneColumns(root);
    }

    private void pruneColumns(LogicalPlan plan) {
        pruneAncestors.add(plan);
        try {
            pruneColumns0(plan);
        } finally {
            pruneAncestors.setPos(pruneAncestors.size() - 1);
        }
    }

    private void pruneColumns0(LogicalPlan plan) {
        final int timestampId = plan.getOutput().getTimestampColumnId();
        switch (plan) {
            case ScanPlan scan -> {
                pruneOutput(scan);
                return;
            }
            case FunctionSourcePlan source -> {
                if (!isUnnarrowedFunctionSource(source) && (source.isProjectable()
                        || !isJoinSource() && (source != joinInputPlan || hasRequiredColumn(source.getOutput())))) {
                    pruneOutput(source);
                }
                return;
            }
            case DomainReadPlan _ -> {
                return;
            }
            case SampleByPlan sample -> {
                // FILL values follow the aggregates by position.
                if (sample.getFillModes().size() == 0) {
                    columnReads.retainOutputs(sample);
                    pruneOutput(sample);
                }
                columnReads.collectInputReads(sample);
                pruneColumns(sample.getChild());
                return;
            }
            case AggregatePlan aggregate -> {
                columnReads.retainOutputs(aggregate);
                final int previousCount = aggregate.getAggregates().size();
                pruneOutput(aggregate);
                if (aggregate.getAggregates().size() < previousCount) {
                    aggregateInputOrder.removeInputOrder(aggregate);
                }
                removeOrderedAggregateInputProjection(aggregate);
                if (columnReads.isUnreadCountInput(aggregate)) {
                    removeCountInputProjections(aggregate);
                }
                columnReads.collectInputReads(aggregate);
                pruneColumns(aggregate.getChild());
                return;
            }
            case WindowPlan window -> {
                final LogicalPlan input = window.getChild();
                final IntList functionColumnIds = window.getFunctionColumnIds();
                final ObjList<FunctionExpression> functions = window.getFunctions();
                final ObjList<WindowSpec> specs = window.getSpecs();
                columnReads.collectInputReads(window);
                retainedColumnIndexes.clear();
                for (int i = functionColumnIds.size() - 1; i >= 0; i--) {
                    if (requiredColumnIds.contains(functionColumnIds.getQuick(i))) {
                        retainedColumnIndexes.add(i);
                    }
                }
                final int keptCount = retainedColumnIndexes.size();
                for (int k = 0; k < keptCount; k++) {
                    final int i = retainedColumnIndexes.getQuick(keptCount - 1 - k);
                    if (i != k) {
                        functions.setQuick(k, functions.getQuick(i));
                        specs.setQuick(k, specs.getQuick(i));
                        functionColumnIds.setQuick(k, functionColumnIds.getQuick(i));
                    }
                }
                functions.set(keptCount, functions.size(), null);
                specs.set(keptCount, specs.size(), null);
                functions.setPos(keptCount);
                specs.setPos(keptCount);
                functionColumnIds.setPos(keptCount);
                if (keptCount == 0) {
                    removeWindow(window);
                    return;
                }
                pruneColumns(input);
                window.deriveOutput(tmpSchema);
                return;
            }
            case JoinPlan join -> {
                final ObjList<JoinInput> ordered = join.getOrderedInputs();
                columnReads.collectInputReads(join);
                plan.getOutput().clear();
                for (int i = 0, n = ordered.size(); i < n; i++) {
                    final JoinInput step = ordered.getQuick(i);
                    if (step.getInput() != null) {
                        joinInputPlan = step.getInput();
                        pruneColumns(step.getInput());
                    }
                    final OutputSchema input = step.getSourceOutput();
                    final UnnestSpec unnest = step.getUnnest();
                    if (unnest != null && unnest.isStandalone()) {
                        plan.getOutput().clear();
                    }
                    final OutputSchema prefix = step.getOutput();
                    prefix.copyFrom(plan.getOutput());
                    prefix.addColumnsFrom(input, step.getBindingAlias());
                    prefix.setTimestampColumnId(LogicalPlans.joinTimestampColumnId(join, i + 1));
                    plan.getOutput().copyFrom(prefix);
                }
                return;
            }
            case WindowJoinPlan windowJoin -> {
                columnReads.collectInputReads(windowJoin);
                for (int i = 0, n = plan.inputCount(); i < n; i++) {
                    pruneColumns(plan.inputAt(i));
                }
                rebuildWindowJoinSchemas(windowJoin);
                return;
            }
            case HorizonJoinPlan horizon -> {
                columnReads.collectInputReads(horizon);
                final int masterCount = horizon.getMaster().getOutput().getColumnCount();
                for (int i = 0, n = plan.inputCount(); i < n; i++) {
                    pruneColumns(plan.inputAt(i));
                }
                rebuildHorizonJoinSchema(horizon, masterCount);
                return;
            }
            case SetOperationPlan operation -> {
                if (columnReads.isNarrowedUnionAll(operation)) {
                    pruneUnionAll(operation);
                }
                // Set equality observes the complete tuple. Keep branch layouts
                // aligned, narrowing only UNION ALL. Narrowing projections on its
                // edges keep any child's equality keys and hidden ordering intact.
                columnReads.collectInputReads(operation);
                for (int i = 0; i < plan.inputCount(); i++) {
                    pruneColumns(plan.inputAt(i));
                }
                return;
            }
            case ProjectPlan project -> {
                final int inputTimestampId = project.getChild().getOutput().getTimestampColumnId();
                columnReads.retainOutputs(project);
                retainGroupingKeys(project);
                selectProjectOutputs(project);
                final boolean isComputed = !LogicalPlans.isColumnProjection(project);
                pruneOutput(project, retainedProjectOutputIds);
                project.setPrunedComputedColumns(isComputed && LogicalPlans.isColumnProjection(project));
                final LogicalPlan projectInput = project.getChild();
                // A computing projection reads a table function in place; narrowing
                // a source that produces every column anyway only adds a mapping.
                if (!(projectInput instanceof FunctionSourcePlan source) || source.isProjectable()
                        || LogicalPlans.isColumnProjection(project)) {
                    pruneColumns(projectInput);
                }
                alignScanColumns(project);
                final OutputSchema input = project.getChild().getOutput();
                if (!project.hasTimestampDeclaration() && project.getOutput().getTimestampIndex() < 0 && input.getTimestampIndex() >= 0
                        && (timestampId >= 0 || inputTimestampId != input.getTimestampColumnId())) {
                    // Pruning one timestamp alias can make another alias the designated
                    // output. Refresh its consumers without undoing an explicit loss of
                    // designation at an unchanged ORDER BY/projection boundary.
                    for (int i = 0, n = project.getExpressions().size(); i < n; i++) {
                        if (project.getExpressions().getQuick(i) instanceof ColumnExpression column
                                && column.isDirectReference() && column.getColumnId() == input.getTimestampColumnId()) {
                            if (project.getOutput().getTimestampIndex() < 0 || Chars.equalsIgnoreCase(
                                    project.getOutput().getColumnName(i), input.getColumnName(input.getTimestampIndex()))) {
                                project.getOutput().setTimestampIndex(i);
                            }
                        }
                    }
                }
                return;
            }
            case FilterPlan _, LatestByPlan _ -> columnReads.collectInputReads(plan);
            case FillPlan fill -> {
                columnReads.collectInputReads(fill);
                pruneFillEntries(fill);
            }
            case DistinctPlan _ -> columnReads.collectInputReads(plan);
            case SortPlan sort -> {
                columnReads.collectInputReads(sort);
                if (sort.getChild() instanceof LatestByPlan || sort.getChild() instanceof FilterPlan) {
                    final LogicalPlan input = sort.getChild();
                    final OutputSchema output = input.getOutput();
                    final ProjectPlan projection = narrowingProjects.next().of(input, plan.getPosition());
                    // Predicate and partition keys need not occupy the sorted record.
                    for (int i = 0, n = output.getColumnCount(); i < n; i++) {
                        final int columnId = output.getColumnId(i);
                        if (requiredColumnIds.contains(columnId)) {
                            projection.getExpressions().add(narrowingColumns.next().of(columnId, output.getColumnType(i), plan.getPosition()));
                            projection.getOutput().addColumnFrom(output, i);
                        }
                    }
                    projection.getOutput().setTimestampColumnId(output.getTimestampColumnId());
                    pruneColumns(input);
                    if (projection.getOutput().getColumnCount() < input.getOutput().getColumnCount()) {
                        sort.replaceInput(0, projection);
                    } else {
                        sort.deriveOutput();
                    }
                    return;
                }
            }
            case LimitPlan _ -> {
            }
        }
        final ForwardingPlan forwarding = (ForwardingPlan) plan;
        pruneColumns(forwarding.getChild());
        forwarding.deriveOutput();
    }

    private void pruneFillEntries(FillPlan fill) {
        final IntList targets = fill.getTargetColumnIds();
        final IntList sources = fill.getSourceColumnIds();
        final ObjList<FillMode> modes = fill.getModes();
        int retained = 0;
        for (int i = 0, n = targets.size(); i < n; i++) {
            if (requiredColumnIds.contains(targets.getQuick(i))) {
                targets.setQuick(retained, targets.getQuick(i));
                sources.setQuick(retained, sources.getQuick(i));
                modes.setQuick(retained, modes.getQuick(i));
                fill.getTokens().setQuick(retained, fill.getTokens().getQuick(i));
                fill.getValues().setQuick(retained++, fill.getValues().getQuick(i));
            }
        }
        targets.setPos(retained);
        sources.setPos(retained);
        modes.setPos(retained);
        while (fill.getTokens().size() > retained) {
            fill.getTokens().popLast();
            fill.getValues().popLast();
        }
    }

    private void pruneOutput(LogicalPlan plan) {
        pruneOutput(plan, requiredColumnIds);
    }

    private void pruneOutput(LogicalPlan plan, IntHashSet selectedIds) {
        final OutputSchema output = plan.getOutput();
        retainedColumnIndexes.clear();
        for (int i = 0, n = output.getColumnCount(); i < n; i++) {
            if (selectedIds.contains(output.getColumnId(i))) {
                retainedColumnIndexes.add(i);
            }
        }
        final IntList indexes = switch (plan) {
            case ScanPlan scan -> scan.getSourceColumnIndexes();
            case FunctionSourcePlan source -> source.getSourceColumnIndexes();
            default -> null;
        };
        final ProjectPlan project = plan instanceof ProjectPlan projectPlan ? projectPlan : null;
        final ObjList<BoundExpression> expressions = project != null ? project.getExpressions() : null;
        final IntList updateTypes = project != null && project.hasUpdateConversions() ? project.getUpdateTargetTypes() : null;
        final GroupingPlan grouping = plan instanceof GroupingPlan groupingPlan ? groupingPlan : null;
        final ObjList<BoundExpression> keys = grouping == null ? null : grouping.getGroupingExpressions();
        final int keyCount = keys == null ? 0 : keys.size();
        int keptKeyCount = 0;
        final ObjList<FunctionExpression> aggregates = grouping == null ? null : grouping.getAggregates();
        for (int index = 0, n = retainedColumnIndexes.size(); index < n; index++) {
            final int i = retainedColumnIndexes.getQuick(index);
            if (indexes != null) {
                indexes.setQuick(index, indexes.getQuick(i));
            }
            if (expressions != null) {
                expressions.setQuick(index, expressions.getQuick(i));
            }
            if (updateTypes != null) {
                updateTypes.setQuick(index, updateTypes.getQuick(i));
            }
            if (keys != null && i < keyCount) {
                keys.setQuick(keptKeyCount++, keys.getQuick(i));
            }
            if (aggregates != null && i >= keyCount) {
                aggregates.setQuick(index - keptKeyCount, aggregates.getQuick(i - keyCount));
            }
        }
        output.retain(retainedColumnIndexes);
        if (indexes != null) {
            indexes.setPos(output.getColumnCount());
        }
        if (updateTypes != null) {
            updateTypes.setPos(output.getColumnCount());
        }
        if (expressions != null) {
            for (int i = output.getColumnCount(), n = expressions.size(); i < n; i++) {
                expressions.setQuick(i, null);
            }
            expressions.setPos(output.getColumnCount());
        }
        if (keys != null) {
            keys.setPos(keptKeyCount);
        }
        if (aggregates != null) {
            while (aggregates.size() > output.getColumnCount() - keptKeyCount) {
                aggregates.popLast();
            }
        }
    }

    private void selectProjectOutputs(ProjectPlan project) {
        childInputIds.clear();
        retainedProjectOutputIds.clear();
        final OutputSchema input = project.getChild().getOutput();
        for (int i = 0, n = input.getColumnCount(); i < n; i++) {
            childInputIds.add(input.getColumnId(i));
        }
        final OutputSchema output = project.getOutput();
        for (int i = 0, n = output.getColumnCount(); i < n; i++) {
            final int columnId = output.getColumnId(i);
            if (requiredColumnIds.contains(columnId)) {
                retainedProjectOutputIds.add(columnId);
            }
        }
        columnReads.collectInputReads(project);
        // Closure adds both input reads and projection aliases. Only aliases need extra output slots;
        // keep raw inputs in requiredColumnIds for the child, without forwarding unused copies.
        for (int i = 0, n = output.getColumnCount(); i < n; i++) {
            final int columnId = output.getColumnId(i);
            if (requiredColumnIds.contains(columnId) && !childInputIds.contains(columnId)) {
                retainedProjectOutputIds.add(columnId);
            }
        }
        // pruneOutput consumes this selection before recursion reuses either scratch set.
    }

    private void pruneUnionAll(SetOperationPlan plan) {
        retainedColumnIndexes.clear();
        retainedColumns.clear();
        final OutputSchema output = plan.getOutput();
        for (int i = 0, n = output.getColumnCount(); i < n; i++) {
            if (requiredColumnIds.contains(output.getColumnId(i))) {
                retainColumn(i);
            }
        }
        if (retainedColumnIndexes.size() == output.getColumnCount()) {
            return;
        }
        // Build both edges before recursion reuses the retained-ordinal list.
        for (int i = 0; i < plan.inputCount(); i++) {
            final LogicalPlan input = plan.inputAt(i);
            final OutputSchema inputSchema = input.getOutput();
            if (input instanceof ProjectPlan branch && !branch.hasUpdateConversions()
                    && inputSchema.getColumnCount() == output.getColumnCount() && !readsDroppedColumn(branch)) {
                permuteProjection(branch);
                continue;
            }
            final ProjectPlan projection = narrowingProjects.next().of(input, input.getPosition());
            for (int k = 0, n = retainedColumnIndexes.size(); k < n; k++) {
                final int index = retainedColumnIndexes.getQuick(k);
                final int columnId = inputSchema.getColumnId(index);
                final int type = inputSchema.getColumnType(index);
                final int position = input instanceof ProjectPlan projected
                        ? projected.getExpressions().getQuick(index).getPosition() : input.getPosition();
                projection.getExpressions().add(narrowingColumns.next().of(columnId, type, position));
                // This edge only narrows an existing relation: pass through its
                // column identities, without defining new expression occurrences.
                projection.getOutput().addColumnFrom(inputSchema, index);
            }
            projection.getOutput().setTimestampColumnId(inputSchema.getTimestampColumnId());
            plan.replaceInput(i, projection);
        }
        plan.retainColumns(retainedColumnIndexes, remappedSymbolColumns);
        output.retain(retainedColumnIndexes);
        plan.setCastRequired(SetOperationCasts.isCastRequired(plan));
    }

    /**
     * A kept column can read an earlier column of the same projection by alias.
     */
    private boolean readsDroppedColumn(ProjectPlan project) {
        for (int i = 0, n = retainedColumnIndexes.size(); i < n; i++) {
            if (readsDroppedColumn(project.getExpressions().getQuick(retainedColumnIndexes.getQuick(i)), project.getOutput())) {
                return true;
            }
        }
        return false;
    }

    private boolean readsDroppedColumn(BoundExpression expression, OutputSchema output) {
        if (expression instanceof ColumnExpression column) {
            final int index = output.getColumnIndexById(column.getColumnId());
            return index >= 0 && !retainedColumns.get(index);
        }
        if (expression instanceof FunctionExpression call) {
            for (int i = 0, n = call.getArgumentCount(); i < n; i++) {
                if (readsDroppedColumn(call.argumentAt(i), output)) {
                    return true;
                }
            }
        }
        return false;
    }

    private void rebuildHorizonJoinSchema(HorizonJoinPlan plan, int previousMasterCount) {
        final OutputSchema output = plan.getOutput();
        tmpSchema.copyFrom(output);
        output.clear();
        final OutputSchema master = plan.getMaster().getOutput();
        output.addColumnsFrom(master, plan.getMasterAlias());
        output.setTimestampIndex(master.getTimestampIndex());
        for (int i = previousMasterCount; i < previousMasterCount + 2; i++) {
            output.addColumnFrom(tmpSchema, i);
        }
        for (int s = 0, n = plan.getSlaves().size(); s < n; s++) {
            final HorizonJoinSlave slave = plan.getSlaves().getQuick(s);
            output.addColumnsFrom(slave.getInput().getOutput(), slave.getAlias());
        }
        tmpSchema.clear();
    }

    private void rebuildWindowJoinSchemas(WindowJoinPlan plan) {
        final OutputSchema output = plan.getOutput();
        tmpSchema.copyFrom(output);
        output.clear();
        final OutputSchema master = plan.getMaster().getOutput();
        output.addColumnsFrom(master, plan.getSteps().getQuick(0).getMasterAlias());
        output.setTimestampIndex(master.getTimestampIndex());
        for (int s = 0, m = plan.getSteps().size(); s < m; s++) {
            final WindowJoinStep step = plan.getSteps().getQuick(s);
            step.getMasterScope().copyFrom(output);
            final OutputSchema scope = step.getScope();
            scope.copyFrom(output);
            scope.addColumnsFrom(step.getSlave().getOutput(), step.getSlaveAlias());
            for (int i = 0, n = step.getAggregateColumnIds().size(); i < n; i++) {
                output.addColumnFrom(tmpSchema, tmpSchema.getColumnIndexById(step.getAggregateColumnIds().getQuick(i)));
            }
        }
        tmpSchema.clear();
    }

    private void removeCountInputProjections(AggregatePlan aggregate) {
        UnaryPlan boundary = null;
        LogicalPlan input = aggregate.getChild();
        while (true) {
            // Only the fused consumer of a HORIZON JOIN reads the join directly.
            if (input instanceof ProjectPlan project && LogicalPlans.isColumnProjection(project)
                    && !(project.getChild() instanceof HorizonJoinPlan horizon && horizon.getConsumer() == HorizonJoinPlan.Consumer.PROJECTION)) {
                projectionMerge.absorbColumnProject(aggregate, project, boundary);
            } else if (input instanceof LimitPlan limit) {
                boundary = limit;
            } else {
                return;
            }
            input = boundary == null ? aggregate.getChild() : boundary.getChild();
        }
    }

    private void removeOrderedAggregateInputProjection(AggregatePlan aggregate) {
        while (true) {
            LogicalPlan candidate = aggregate.getChild();
            UnaryPlan boundary = null;
            while (candidate instanceof LimitPlan limit) {
                boundary = limit;
                candidate = limit.getChild();
            }
            if (!(candidate instanceof ProjectPlan project)) {
                return;
            }
            final LogicalPlan input = project.getChild();
            if (project.hasTimestampDeclaration() || !LogicalPlans.isColumnProjection(project)
                    || project.getExpressions().size() != input.getOutput().getColumnCount()
                    || !(input instanceof SortPlan) && !(input instanceof LimitPlan)) {
                return;
            }
            for (int i = 0, n = project.getExpressions().size(); i < n; i++) {
                final ColumnExpression column = (ColumnExpression) project.getExpressions().getQuick(i);
                final int index = input.getOutput().getColumnIndexById(column.getColumnId());
                if (index != i || !Chars.equals(project.getOutput().getColumnName(i), input.getOutput().getColumnName(index))) {
                    return;
                }
            }
            projectionMerge.absorbColumnProject(aggregate, project, boundary);
        }
    }

    /**
     * Replaces a window that computes no column a consumer reads with its input, which prunes in the window's place.
     */
    private void removeWindow(WindowPlan window) {
        final LogicalPlan input = window.getChild();
        final int depth = pruneAncestors.size();
        final LogicalPlan parent = pruneAncestors.getQuick(depth - 2);
        for (int i = 0, n = parent.inputCount(); i < n; i++) {
            if (parent.inputAt(i) == window) {
                parent.replaceInput(i, input);
            }
        }
        pruneAncestors.setQuick(depth - 1, input);
        pruneColumns0(input);
    }

    private void retainColumn(int index) {
        if (!retainedColumns.getAndSet(index)) {
            retainedColumnIndexes.add(index);
        }
    }

    /**
     * A grouping key stays in the aggregate output anyway; dropping its selection only adds a remapping.
     */
    private void retainGroupingKeys(ProjectPlan project) {
        final LogicalPlan input = LogicalPlans.skipFilters(project.getChild());
        if (project != joinInputPlan || !(input instanceof AggregatePlan aggregate) || !LogicalPlans.isColumnProjection(project)) {
            return;
        }
        final int keyCount = aggregate.getGroupingExpressions().size();
        for (int i = 0, n = project.getExpressions().size(); i < n; i++) {
            final int index = aggregate.getOutput().getColumnIndexById(((ColumnExpression) project.getExpressions().getQuick(i)).getColumnId());
            if (index >= 0 && index < keyCount) {
                requiredColumnIds.add(project.getOutput().getColumnId(i));
            }
        }
    }

    private void retainReferencedColumns(BoundExpression expression, OutputSchema output) {
        if (expression instanceof ColumnExpression column) {
            final int index = output.getColumnIndexById(column.getColumnId());
            if (index >= 0) {
                retainColumn(index);
            }
        } else if (expression instanceof FunctionExpression function) {
            for (int i = 0, n = function.getArgumentCount(); i < n; i++) {
                retainReferencedColumns(function.argumentAt(i), output);
            }
        }
    }
}
