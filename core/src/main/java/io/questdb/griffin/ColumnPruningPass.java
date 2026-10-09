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

import io.questdb.griffin.plan.logical.AggregatePlan;
import io.questdb.griffin.plan.logical.BoundExpression;
import io.questdb.griffin.plan.logical.ColumnExpression;
import io.questdb.griffin.plan.logical.DistinctPlan;
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
import io.questdb.griffin.plan.logical.SetOperationKind;
import io.questdb.griffin.plan.logical.SetOperationPlan;
import io.questdb.griffin.plan.logical.SortPlan;
import io.questdb.griffin.plan.logical.UnaryPlan;
import io.questdb.griffin.plan.logical.UnnestSpec;
import io.questdb.griffin.plan.logical.WindowJoinPlan;
import io.questdb.griffin.plan.logical.WindowJoinStep;
import io.questdb.griffin.plan.logical.WindowPlan;
import io.questdb.griffin.plan.logical.WindowSpec;
import io.questdb.std.Chars;
import io.questdb.std.IntHashSet;
import io.questdb.std.IntList;
import io.questdb.std.Mutable;
import io.questdb.std.ObjList;
import io.questdb.std.ObjectPool;

/**
 * Removes the columns no consumer reads, starting from the root's output, and lays out the
 * outputs of scans, joins and windows over the surviving columns.
 */
final class ColumnPruningPass implements Mutable {
    private final AggregateInputOrderPass aggregateInputOrder;
    private final ObjList<LogicalPlan> alignedPath = new ObjList<>();
    private final OptimiserContext context;
    private final ObjList<BoundExpression> tmpExpressions;
    private final ObjectPool<ColumnExpression> narrowingColumns;
    private final ObjectPool<ProjectPlan> narrowingProjects;
    private final ObjList<LogicalPlan> pruneAncestors;
    private final IntHashSet requiredColumnIds;
    private final IntList retainedColumnIndexes;
    private final OutputSchema tmpSchema;
    private final ObjList<LogicalPlan> sharedSetOperations = new ObjList<>();
    private final IntList tmpUpdateTypes;
    private LogicalPlan joinInputPlan;

    ColumnPruningPass(
            OptimiserContext context,
            AggregateInputOrderPass aggregateInputOrder,
            ObjList<BoundExpression> tmpExpressions,
            ObjectPool<ColumnExpression> narrowingColumns,
            ObjectPool<ProjectPlan> narrowingProjects,
            IntList retainedColumnIndexes,
            IntList tmpUpdateTypes,
            IntHashSet requiredColumnIds,
            ObjList<LogicalPlan> pruneAncestors,
            OutputSchema tmpSchema
    ) {
        this.context = context;
        this.aggregateInputOrder = aggregateInputOrder;
        this.tmpExpressions = tmpExpressions;
        this.narrowingColumns = narrowingColumns;
        this.narrowingProjects = narrowingProjects;
        this.retainedColumnIndexes = retainedColumnIndexes;
        this.tmpUpdateTypes = tmpUpdateTypes;
        this.requiredColumnIds = requiredColumnIds;
        this.pruneAncestors = pruneAncestors;
        this.tmpSchema = tmpSchema;
    }

    @Override
    public void clear() {
        joinInputPlan = null;
        alignedPath.clear();
        sharedSetOperations.clear();
    }

    private static boolean isCountInputMaterialized(LogicalPlan input) {
        while (true) {
            switch (input) {
                case SetOperationPlan _ -> {
                    return true;
                }
                case ProjectPlan project -> {
                    if (!LogicalPlans.isColumnProjection(project)) {
                        return true;
                    }
                }
                case LimitPlan _, SortPlan _, FilterPlan _ -> {
                }
                default -> {
                    return false;
                }
            }
            input = input.inputAt(0);
        }
    }

    private static boolean isUnionAllChain(LogicalPlan plan) {
        for (int i = 0, n = plan.inputCount(); i < n; i++) {
            final LogicalPlan input = plan.inputAt(i);
            if (input instanceof SetOperationPlan operation
                    && (operation.getOperation() != SetOperationKind.UNION_ALL || !isUnionAllChain(operation))) {
                return false;
            }
        }
        return true;
    }

    private void alignScanColumns(ProjectPlan project) {
        final WindowJoinPlan windowJoin = project.getInput() instanceof WindowJoinPlan join ? join : null;
        final LogicalPlan top = windowJoin != null ? windowJoin.getMaster() : project.getInput();
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
            if (!retainedColumnIndexes.contains(index)) {
                retainedColumnIndexes.add(index);
            }
        }
        for (int i = 0, n = output.getColumnCount(); i < n; i++) {
            if (!retainedColumnIndexes.contains(i)) {
                retainedColumnIndexes.add(i);
            }
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
                // Window outputs follow its input columns, then its function columns.
                final OutputSchema nodeOutput = window.getOutput();
                final OutputSchema child = window.getInput().getOutput();
                final int timestampId = nodeOutput.getTimestampColumnId();
                tmpSchema.copyFrom(nodeOutput);
                nodeOutput.copyFrom(child);
                for (int i = 0, n = tmpSchema.getColumnCount(); i < n; i++) {
                    if (child.getColumnIndexById(tmpSchema.getColumnId(i)) < 0) {
                        nodeOutput.addColumnFrom(tmpSchema, i);
                    }
                }
                nodeOutput.setTimestampColumnId(timestampId);
                tmpSchema.clear();
            } else {
                ((ForwardingPlan) node).deriveOutput();
            }
        }
        alignedPath.clear();
        if (windowJoin != null) {
            rebuildWindowJoinSchemas(windowJoin);
        }
    }

    private void collectRequiredColumns(BoundExpression expression) {
        if (expression instanceof ColumnExpression column) {
            requiredColumnIds.add(column.getColumnId());
        } else if (expression instanceof FunctionExpression function) {
            for (int i = 0, n = function.getArgumentCount(); i < n; i++) {
                collectRequiredColumns(function.argumentAt(i));
            }
        }
    }

    private void collectWindowRequiredColumns(WindowPlan window, int index) {
        collectRequiredColumns(window.getFunctions().getQuick(index));
        final WindowSpec spec = window.getSpecs().getQuick(index);
        for (int i = 0, n = spec.getPartitionBy().size(); i < n; i++) {
            collectRequiredColumns(spec.getPartitionBy().getQuick(i));
        }
        for (int i = 0, n = spec.getOrderByColumnIds().size(); i < n; i++) {
            requiredColumnIds.add(spec.getOrderByColumnIds().getQuick(i));
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
            case SampleByPlan sample -> {
                requiredColumnIds.add(sample.getTimestampColumnId());
                pruneGroupingInput(sample);
                return;
            }
            case AggregatePlan aggregate -> {
                if (aggregate.getSharedSource() != null) {
                    LogicalPlan input = aggregate.getInput();
                    while (input instanceof ProjectPlan project && LogicalPlans.isColumnProjection(project)) {
                        input = input.inputAt(0);
                    }
                    if (input instanceof SetOperationPlan) {
                        sharedSetOperations.add(input);
                    }
                }
                requireGroupingKeys(aggregate);
                final int previousCount = aggregate.getAggregates().size();
                pruneOutput(aggregate);
                if (aggregate.getAggregates().size() < previousCount) {
                    aggregateInputOrder.removeInputOrder(aggregate);
                }
                removeOrderedAggregateInputProjection(aggregate);
                if (aggregate.getGroupingExpressions().size() == 0 && aggregate.getAggregates().size() == 1) {
                    final FunctionExpression call = aggregate.getAggregates().getQuick(0);
                    if (call.getArgumentCount() == 0 && SqlKeywords.isCountKeyword(call.getName())) {
                        if (isCountInputMaterialized(aggregate.getInput())) {
                            final OutputSchema input = aggregate.getInput().getOutput();
                            for (int i = 0, n = input.getColumnCount(); i < n; i++) {
                                requiredColumnIds.add(input.getColumnId(i));
                            }
                        } else {
                            aggregate.replaceInput(0, removeCountInputProjections(aggregate.getInput()));
                        }
                    }
                }
                pruneGroupingInput(aggregate);
                return;
            }
            case WindowPlan window -> {
                final LogicalPlan input = window.getInput();
                for (int i = 0, n = window.getFunctionColumnIds().size(); i < n; i++) {
                    collectWindowRequiredColumns(window, i);
                }
                pruneColumns(input);
                final OutputSchema output = window.getOutput();
                tmpSchema.copyFrom(output);
                output.copyFrom(input.getOutput());
                for (int i = 0, n = window.getFunctionColumnIds().size(); i < n; i++) {
                    final int index = tmpSchema.getColumnIndexById(window.getFunctionColumnIds().getQuick(i));
                    removeReplacedWindowColumn(output, tmpSchema.getColumnName(index));
                    output.addColumnFrom(tmpSchema, index);
                }
                output.setTimestampColumnId(timestampId);
                tmpSchema.clear();
                return;
            }
            case JoinPlan join -> {
                final ObjList<JoinInput> ordered = join.getOrderedInputs();
                for (int i = 0, n = ordered.size(); i < n; i++) {
                    final JoinInput step = ordered.getQuick(i);
                    if (step.getJoinType().isTemporal()) {
                        requiredColumnIds.add(ordered.getQuick(0).getInput().getOutput().getTimestampColumnId());
                        requiredColumnIds.add(step.getInput().getOutput().getTimestampColumnId());
                    }
                    for (int k = 0, count = step.getMasterKeyColumnIds().size(); k < count; k++) {
                        requiredColumnIds.add(step.getMasterKeyColumnIds().getQuick(k));
                        requiredColumnIds.add(step.getSlaveKeyColumnIds().getQuick(k));
                    }
                    collectRequiredColumns(step.getOnResidual());
                    collectRequiredColumns(step.getPostJoinFilter());
                    if (step.getUnnest() != null) {
                        final ObjList<BoundExpression> expressions = step.getUnnest().getExpressions();
                        for (int k = 0, count = expressions.size(); k < count; k++) {
                            collectRequiredColumns(expressions.getQuick(k));
                        }
                    }
                }
                plan.getOutput().clear();
                int joinTimestampId = -1;
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
                        joinTimestampId = -1;
                    }
                    if (i == 0) {
                        joinTimestampId = input.getTimestampColumnId();
                    } else if (step.getJoinType().isMasterNulling() && !join.hasExplicitTimestamp()) {
                        joinTimestampId = -1;
                    }
                    final OutputSchema prefix = step.getOutput();
                    prefix.copyFrom(plan.getOutput());
                    prefix.addColumnsFrom(input, step.getBindingAlias());
                    prefix.setTimestampColumnId(joinTimestampId);
                    plan.getOutput().copyFrom(prefix);
                }
                return;
            }
            case WindowJoinPlan windowJoin -> {
                requiredColumnIds.add(windowJoin.getMaster().getOutput().getTimestampColumnId());
                for (int i = 0, n = windowJoin.getSteps().size(); i < n; i++) {
                    final WindowJoinStep step = windowJoin.getSteps().getQuick(i);
                    requiredColumnIds.add(step.getSlave().getOutput().getTimestampColumnId());
                    collectRequiredColumns(step.getFilter());
                    collectRequiredColumns(step.getLoExpression());
                    collectRequiredColumns(step.getHiExpression());
                    for (int k = 0, count = step.getAggregates().size(); k < count; k++) {
                        collectRequiredColumns(step.getAggregates().getQuick(k));
                    }
                }
                for (int i = 0, n = plan.inputCount(); i < n; i++) {
                    pruneColumns(plan.inputAt(i));
                }
                rebuildWindowJoinSchemas(windowJoin);
                return;
            }
            case HorizonJoinPlan horizon -> {
                requiredColumnIds.add(horizon.getMaster().getOutput().getTimestampColumnId());
                for (int i = 0, n = horizon.getSlaves().size(); i < n; i++) {
                    final HorizonJoinSlave slave = horizon.getSlaves().getQuick(i);
                    requiredColumnIds.add(slave.getInput().getOutput().getTimestampColumnId());
                    for (int k = 0, count = slave.getMasterKeyColumnIds().size(); k < count; k++) {
                        requiredColumnIds.add(slave.getMasterKeyColumnIds().getQuick(k));
                        requiredColumnIds.add(slave.getSlaveKeyColumnIds().getQuick(k));
                    }
                }
                final int masterCount = horizon.getMaster().getOutput().getColumnCount();
                for (int i = 0, n = plan.inputCount(); i < n; i++) {
                    pruneColumns(plan.inputAt(i));
                }
                rebuildHorizonJoinSchema(horizon, masterCount);
                return;
            }
            case SetOperationPlan operation -> {
                if (operation.getOperation() == SetOperationKind.UNION_ALL && sharedSetOperations.indexOf(operation) < 0
                        && isUnionAllChain(operation)) {
                    pruneUnionAll(operation);
                }
                // Set equality observes the complete tuple. Keep branch layouts
                // aligned, narrowing only UNION ALL. Narrowing projections on its
                // edges keep any child's equality keys and hidden ordering intact.
                for (int i = 0; i < plan.inputCount(); i++) {
                    final LogicalPlan input = plan.inputAt(i);
                    for (int k = 0, n = input.getOutput().getColumnCount(); k < n; k++) {
                        requiredColumnIds.add(input.getOutput().getColumnId(k));
                    }
                    pruneColumns(input);
                }
                return;
            }
            case ProjectPlan project -> {
                final int inputTimestampId = project.getInput().getOutput().getTimestampColumnId();
                if (project.hasTimestampDeclaration()) {
                    requiredColumnIds.add(timestampId);
                }
                retainProjectionReferences(project);
                retainGroupingKeys(project);
                final boolean isComputed = !LogicalPlans.isColumnProjection(project);
                pruneOutput(project);
                project.setPrunedComputedColumns(isComputed && LogicalPlans.isColumnProjection(project));
                for (int i = 0, n = project.getExpressions().size(); i < n; i++) {
                    collectRequiredColumns(project.getExpressions().getQuick(i));
                }
                final LogicalPlan projectInput = project.getInput();
                // A computing projection reads a table function in place; narrowing
                // a source that produces every column anyway only adds a mapping.
                if (!(projectInput instanceof FunctionSourcePlan source) || source.isProjectable()
                        || LogicalPlans.isColumnProjection(project)) {
                    pruneColumns(projectInput);
                }
                alignScanColumns(project);
                final OutputSchema input = project.getInput().getOutput();
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
            case FilterPlan filter -> collectRequiredColumns(filter.getPredicate());
            case LatestByPlan latest -> {
                final LogicalPlan input = latest.getInput();
                final boolean isNativeScan = input instanceof ScanPlan
                        || input instanceof FilterPlan filter && filter.getInput() instanceof ScanPlan;
                if (latest.getTimestampColumnId() >= 0 && !isNativeScan) {
                    requiredColumnIds.add(latest.getTimestampColumnId());
                }
                final IntList keys = latest.getKeyColumnIds();
                for (int i = 0, n = keys.size(); i < n; i++) {
                    requiredColumnIds.add(keys.getQuick(i));
                }
            }
            case FillPlan fill -> pruneFillEntries(fill);
            case DistinctPlan _ -> {
                // Every value contributes to tuple equality, including hidden columns.
                final OutputSchema input = plan.inputAt(0).getOutput();
                for (int i = 0, n = input.getColumnCount(); i < n; i++) {
                    requiredColumnIds.add(input.getColumnId(i));
                }
            }
            case SortPlan sort -> {
                final IntList keys = sort.getColumnIds();
                for (int i = 0, n = keys.size(); i < n; i++) {
                    requiredColumnIds.add(keys.getQuick(i));
                }
                if (sort.getInput() instanceof LatestByPlan || sort.getInput() instanceof FilterPlan) {
                    final LogicalPlan input = sort.getInput();
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
        pruneColumns(forwarding.getInput());
        forwarding.deriveOutput();
    }

    private void pruneFillEntries(FillPlan fill) {
        requiredColumnIds.add(fill.getTimestampColumnId());
        final IntList targets = fill.getTargetColumnIds();
        final IntList sources = fill.getSourceColumnIds();
        final IntList modes = fill.getModes();
        for (int i = 0, n = targets.size(); i < n; i++) {
            if (requiredColumnIds.contains(targets.getQuick(i))) {
                int entry = i;
                while (entry >= 0 && modes.getQuick(entry) == FillPlan.FILL_PREV_COLUMN) {
                    final int source = sources.getQuick(entry);
                    if (!requiredColumnIds.add(source)) {
                        break;
                    }
                    entry = targets.indexOf(source, 0, targets.size());
                }
            }
        }
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

    private void pruneGroupingInput(GroupingPlan grouping) {
        for (int i = 0, n = grouping.getGroupingExpressions().size(); i < n; i++) {
            collectRequiredColumns(grouping.getGroupingExpressions().getQuick(i));
        }
        for (int i = 0, n = grouping.getAggregates().size(); i < n; i++) {
            collectRequiredColumns(grouping.getAggregates().getQuick(i));
        }
        // Hidden keys still define groups; a keyless aggregate still returns
        // one row when every aggregate value has been pruned.
        pruneColumns(grouping.getInput());
    }

    private void pruneOutput(LogicalPlan plan) {
        final OutputSchema output = plan.getOutput();
        retainedColumnIndexes.clear();
        for (int i = 0, n = output.getColumnCount(); i < n; i++) {
            if (requiredColumnIds.contains(output.getColumnId(i))) {
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
        tmpUpdateTypes.clear();
        if (updateTypes != null) {
            tmpUpdateTypes.addAll(updateTypes);
            updateTypes.clear();
        }
        final AggregatePlan aggregate = plan instanceof AggregatePlan aggregatePlan ? aggregatePlan : null;
        final ObjList<BoundExpression> keys = aggregate == null ? null : aggregate.getGroupingExpressions();
        final int keyCount = keys == null ? 0 : keys.size();
        int keptKeyCount = 0;
        final ObjList<FunctionExpression> aggregates = aggregate == null ? null : aggregate.getAggregates();
        for (int index = 0, n = retainedColumnIndexes.size(); index < n; index++) {
            final int i = retainedColumnIndexes.getQuick(index);
            if (indexes != null) {
                indexes.setQuick(index, indexes.getQuick(i));
            }
            if (expressions != null) {
                expressions.setQuick(index, expressions.getQuick(i));
            }
            if (updateTypes != null) {
                updateTypes.add(tmpUpdateTypes.getQuick(i));
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

    private void pruneUnionAll(SetOperationPlan plan) {
        retainedColumnIndexes.clear();
        final OutputSchema output = plan.getOutput();
        for (int i = 0, n = output.getColumnCount(); i < n; i++) {
            if (requiredColumnIds.contains(output.getColumnId(i))) {
                retainedColumnIndexes.add(i);
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
                    && inputSchema.getColumnCount() == output.getColumnCount()) {
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
        plan.remapSymbolColumns(retainedColumnIndexes);
        output.retain(retainedColumnIndexes);
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

    private LogicalPlan removeCountInputProjections(LogicalPlan input) {
        while (input instanceof ProjectPlan project && LogicalPlans.isColumnProjection(project)) {
            input = input.inputAt(0);
        }
        if (input instanceof LimitPlan limit) {
            limit.replaceInput(0, removeCountInputProjections(limit.getInput()));
        }
        return input;
    }

    private void removeOrderedAggregateInputProjection(AggregatePlan aggregate) {
        while (true) {
            LogicalPlan candidate = aggregate.getInput();
            UnaryPlan boundary = null;
            while (candidate instanceof LimitPlan limit) {
                boundary = limit;
                candidate = limit.getInput();
            }
            if (!(candidate instanceof ProjectPlan project)) {
                return;
            }
            final LogicalPlan input = project.getInput();
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
            for (int i = 0, n = aggregate.getGroupingExpressions().size(); i < n; i++) {
                aggregate.getGroupingExpressions().setQuick(i,
                        context.getRewriter().remapColumns(aggregate.getGroupingExpressions().getQuick(i), project));
            }
            for (int i = 0, n = aggregate.getAggregates().size(); i < n; i++) {
                aggregate.getAggregates().setQuick(i,
                        (FunctionExpression) context.getRewriter().remapColumns(aggregate.getAggregates().getQuick(i), project));
            }
            if (boundary == null) {
                aggregate.replaceInput(0, input);
            } else {
                boundary.replaceInput(0, input);
                LogicalPlans.deriveLimits(aggregate.getInput());
            }
        }
    }

    /**
     * Keeps out the nested window column a top-level window function has replaced in the window's output.
     */
    private void removeReplacedWindowColumn(OutputSchema output, CharSequence name) {
        for (int i = 0, n = output.getColumnCount(); i < n; i++) {
            if (Chars.equalsIgnoreCase(output.getColumnName(i), name) && tmpSchema.getColumnIndexById(output.getColumnId(i)) < 0) {
                output.remove(i);
                return;
            }
        }
    }

    /**
     * Keys define the groups, so they stay.
     */
    private void requireGroupingKeys(AggregatePlan aggregate) {
        final OutputSchema output = aggregate.getOutput();
        for (int i = 0, n = aggregate.getGroupingExpressions().size(); i < n; i++) {
            requiredColumnIds.add(output.getColumnId(i));
        }
    }

    /**
     * A grouping key stays in the aggregate output anyway; dropping its selection only adds a remapping.
     */
    private void retainGroupingKeys(ProjectPlan project) {
        final LogicalPlan input = LogicalPlans.skipFilters(project.getInput());
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

    /**
     * Keeps the earlier columns a kept column reads by alias.
     */
    private void retainProjectionReferences(ProjectPlan project) {
        final OutputSchema output = project.getOutput();
        boolean isChanged = true;
        while (isChanged) {
            isChanged = false;
            for (int i = output.getColumnCount() - 1; i >= 0; i--) {
                if (requiredColumnIds.contains(output.getColumnId(i))) {
                    isChanged |= retainReferences(project.getExpressions().getQuick(i), output);
                }
            }
        }
    }

    private void retainReferencedColumns(BoundExpression expression, OutputSchema output) {
        if (expression instanceof ColumnExpression column) {
            final int index = output.getColumnIndexById(column.getColumnId());
            if (index >= 0 && !retainedColumnIndexes.contains(index)) {
                retainedColumnIndexes.add(index);
            }
        } else if (expression instanceof FunctionExpression function) {
            for (int i = 0, n = function.getArgumentCount(); i < n; i++) {
                retainReferencedColumns(function.argumentAt(i), output);
            }
        }
    }

    private boolean retainReferences(BoundExpression expression, OutputSchema output) {
        if (expression instanceof ColumnExpression column) {
            return output.getColumnIndexById(column.getColumnId()) >= 0 && requiredColumnIds.add(column.getColumnId());
        }
        boolean isChanged = false;
        if (expression instanceof FunctionExpression call) {
            for (int i = 0, n = call.getArgumentCount(); i < n; i++) {
                isChanged |= retainReferences(call.argumentAt(i), output);
            }
        }
        return isChanged;
    }

    void prune(LogicalPlan root) {
        pruneAncestors.clear();
        requiredColumnIds.clear();
        for (int i = 0, n = root.getOutput().getColumnCount(); i < n; i++) {
            requiredColumnIds.add(root.getOutput().getColumnId(i));
        }
        pruneColumns(root);
    }
}
