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

import io.questdb.griffin.FunctionFactoryCache;
import io.questdb.griffin.LogicalPlans;
import io.questdb.griffin.plan.logical.AggregatePlan;
import io.questdb.griffin.plan.logical.BoundExpression;
import io.questdb.griffin.plan.logical.ColumnExpression;
import io.questdb.griffin.plan.logical.ConstantExpression;
import io.questdb.griffin.plan.logical.DistinctPlan;
import io.questdb.griffin.plan.logical.FillPlan;
import io.questdb.griffin.plan.logical.FilterPlan;
import io.questdb.griffin.plan.logical.FunctionExpression;
import io.questdb.griffin.plan.logical.FunctionSourcePlan;
import io.questdb.griffin.plan.logical.HorizonJoinPlan;
import io.questdb.griffin.plan.logical.JoinInput;
import io.questdb.griffin.plan.logical.JoinKind;
import io.questdb.griffin.plan.logical.JoinPlan;
import io.questdb.griffin.plan.logical.LatestByPlan;
import io.questdb.griffin.plan.logical.LimitPlan;
import io.questdb.griffin.plan.logical.LogicalPlan;
import io.questdb.griffin.plan.logical.OutputSchema;
import io.questdb.griffin.plan.logical.PlanVisitor;
import io.questdb.griffin.plan.logical.ProjectPlan;
import io.questdb.griffin.plan.logical.SampleByPlan;
import io.questdb.griffin.plan.logical.ScanPlan;
import io.questdb.griffin.plan.logical.SetOperationKind;
import io.questdb.griffin.plan.logical.SetOperationPlan;
import io.questdb.griffin.plan.logical.SortDirection;
import io.questdb.griffin.plan.logical.SortPlan;
import io.questdb.griffin.plan.logical.TreeWalk;
import io.questdb.griffin.plan.logical.WindowJoinPlan;
import io.questdb.griffin.plan.logical.WindowPlan;
import io.questdb.griffin.plan.logical.WindowSpec;
import io.questdb.std.Chars;
import io.questdb.std.ObjList;
import io.questdb.std.ObjectPool;

/**
 * Pushes the order each consumer requires down the plan. As an optimiser pass it marks markout-horizon sorts, drops
 * the sorts whose order a consumer re-sorts or discards and the sorts of a single row that keep its designated
 * timestamp, then records the order each consumer requires where the generator picks a physical operator by it: the
 * scan direction, the requested key order, the LIMIT a filtered scan may stop at and whether row order matters on a
 * {@link ScanPlan}, the requested order of a {@link ProjectPlan} and a {@link SetOperationPlan}, and the query order a
 * {@link WindowPlan} serves. {@link AccessPathPlanning} then picks each scan's access path by them, and
 * {@link OperatorPlanning} the operators over the scans.
 */
final class OrderPlanning implements OptimiserPass {
    private static final PlanVisitor MARKOUT_HORIZONS = plan -> {
        if (plan instanceof SortPlan sort) {
            markMarkoutHorizon(sort);
        }
        return TreeWalk.CONTINUE;
    };
    private final FunctionFactoryCache functionFactoryCache;
    private final ObjectPool<SortPlan> sorts;
    private boolean isJoinSlaveInput;

    OrderPlanning(FunctionFactoryCache functionFactoryCache, ObjectPool<SortPlan> sorts) {
        this.functionFactoryCache = functionFactoryCache;
        this.sorts = sorts;
    }

    @Override
    public LogicalPlan apply(LogicalPlan plan) {
        markMarkoutHorizons(plan);
        final LogicalPlan root = removeReorderedSorts(plan);
        isJoinSlaveInput = false;
        requireNothing(root);
        return root;
    }

    @Override
    public String getName() {
        return "order planning";
    }

    private static boolean hasColumns(OutputSchema schema, SortPlan order) {
        if (order == null) {
            return false;
        }
        for (int i = 0, n = order.getColumnIds().size(); i < n; i++) {
            if (schema.getColumnIndexById(order.getColumnIds().getQuick(i)) < 0) {
                return false;
            }
        }
        return true;
    }

    private static boolean hasNestedUnionAll(LogicalPlan plan) {
        return LogicalPlans.skipProjectsAndFilters(plan) instanceof SetOperationPlan operation
                && operation.getOperation() == SetOperationKind.UNION_ALL;
    }

    private static boolean isHiddenSortKeyPrefix(ProjectPlan project, ProjectPlan inner) {
        final int n = project.getExpressions().size();
        if (!LogicalPlans.isColumnProjection(project) || project.hasTimestampDeclaration() || inner.hasTimestampDeclaration()
                || inner.hasUpdateConversions() || n >= inner.getExpressions().size()) {
            return false;
        }
        for (int i = 0; i < n; i++) {
            final ColumnExpression column = (ColumnExpression) project.getExpressions().getQuick(i);
            if (!column.isDirectReference() || column.isCast() || column.getColumnId() != inner.getOutput().getColumnId(i)
                    || !Chars.equals(project.getOutput().getColumnName(i), inner.getOutput().getColumnName(i))) {
                return false;
            }
        }
        return true;
    }

    private static boolean isSingleRow(LogicalPlan plan) {
        plan = LogicalPlans.skipProjectsAndFilters(plan);
        return plan instanceof AggregatePlan aggregate && aggregate.getGroupingExpressions().size() == 0;
    }

    private static void markMarkoutHorizon(SortPlan sort) {
        if (sort.getColumnIds().size() != 1 || sort.getDirections().getQuick(0) != SortDirection.ASCENDING) {
            return;
        }
        int columnId = sort.getColumnIds().getQuick(0);
        LogicalPlan plan = sort.getInput();
        while (plan instanceof ProjectPlan project) {
            final int index = project.getOutput().getColumnIndexById(columnId);
            if (index < 0) {
                return;
            }
            final BoundExpression expression = project.getExpressions().getQuick(index);
            plan = project.getInput();
            if (expression instanceof ColumnExpression column) {
                columnId = column.getColumnId();
                continue;
            }
            if (expression instanceof FunctionExpression call && call.getArgumentCount() == 2
                    && Chars.equals(call.getName(), '+')
                    && call.argumentAt(0) instanceof ColumnExpression left
                    && call.argumentAt(1) instanceof ColumnExpression right) {
                markMarkoutHorizon(sort, plan, left.getColumnId(), right.getColumnId());
            }
            return;
        }
    }

    private static void markMarkoutHorizon(SortPlan sort, LogicalPlan plan, int leftId, int rightId) {
        while (plan instanceof ProjectPlan project) {
            final int leftIndex = project.getOutput().getColumnIndexById(leftId);
            final int rightIndex = project.getOutput().getColumnIndexById(rightId);
            if (leftIndex < 0 || rightIndex < 0
                    || !(project.getExpressions().getQuick(leftIndex) instanceof ColumnExpression left)
                    || !(project.getExpressions().getQuick(rightIndex) instanceof ColumnExpression right)) {
                return;
            }
            leftId = left.getColumnId();
            rightId = right.getColumnId();
            plan = project.getInput();
        }
        if (!(plan instanceof JoinPlan join)) {
            return;
        }
        final ObjList<JoinInput> inputs = join.getOrderedInputs();
        if (inputs.size() != 2) {
            return;
        }
        final JoinInput slave = inputs.getQuick(1);
        if (slave.getJoinType() != JoinKind.CROSS || (slave.getHints() & JoinInput.HINT_MARKOUT_HORIZON) == 0
                || inputs.getQuick(0).getInput() == null || slave.getInput() == null) {
            return;
        }
        final int timestampId = inputs.getQuick(0).getInput().getOutput().getTimestampColumnId();
        final OutputSchema slaveOutput = slave.getInput().getOutput();
        if (timestampId < 0) {
            return;
        }
        if (leftId == timestampId && slaveOutput.getColumnIndexById(rightId) >= 0) {
            slave.setMarkout(timestampId, rightId);
        } else if (rightId == timestampId && slaveOutput.getColumnIndexById(leftId) >= 0) {
            slave.setMarkout(timestampId, leftId);
        } else {
            return;
        }
        sort.markMarkoutHorizon();
    }

    /**
     * Marks a sort over a {@code markout_horizon}-hinted CROSS JOIN whose key is {@code master.ts + slave.offset},
     * so the generator can use the markout factory.
     */
    private static void markMarkoutHorizons(LogicalPlan plan) {
        plan.walkBottomUp(MARKOUT_HORIZONS);
    }

    /**
     * Drops the sorts whose order a consumer re-sorts or discards, and the sorts of a single row that keep its
     * designated timestamp; a window's own ORDER BY and a markout sort stay.
     */
    private static LogicalPlan removeReorderedSorts(LogicalPlan root) {
        return removeReorderedSorts0(root, false, false);
    }

    /**
     * {@code isReordered}: a consumer above re-sorts or discards the order of {@code plan};
     * {@code isSetBranchReordered}: that consumer is a set operation, whose branch order never survives.
     */
    private static LogicalPlan removeReorderedSorts0(LogicalPlan plan, boolean isReordered, boolean isSetBranchReordered) {
        switch (plan) {
            case SortPlan sort -> {
                final LogicalPlan source = LogicalPlans.skipProjects(sort.getInput());
                final LogicalPlan input = removeReorderedSorts0(sort.getInput(), true, false);
                if (source instanceof WindowPlan) {
                    sort.replaceInput(0, input);
                    return sort;
                }
                if (isReordered && !sort.isMarkoutHorizon() && (isSetBranchReordered && !(source instanceof FillPlan) || sort.getOutput().getTimestampIndex() < 0
                        || sort.getOutput().getTimestampIndex() == input.getOutput().getTimestampIndex())) {
                    return input;
                }
                if (isSingleRow(input) && sort.getOutput().getTimestampIndex() == input.getOutput().getTimestampIndex()) {
                    return input;
                }
                sort.replaceInput(0, input);
                return sort;
            }
            case ProjectPlan project -> {
                final boolean isSorted = project.getInput() instanceof SortPlan;
                replaceReorderedInput(project, isReordered && !project.hasTimestampDeclaration(), isSetBranchReordered);
                if (isSorted && project.getInput() instanceof ProjectPlan inner && isHiddenSortKeyPrefix(project, inner)) {
                    inner.getExpressions().setPos(project.getExpressions().size());
                    inner.getOutput().copyFrom(project.getOutput());
                    return inner;
                }
            }
            case FilterPlan filter -> replaceReorderedInput(filter, isReordered, isSetBranchReordered);
            case WindowPlan window -> {
                boolean isInputReordered = isReordered;
                for (int i = 0, n = window.getSpecs().size(); i < n; i++) {
                    isInputReordered &= window.getSpecs().getQuick(i).getOrderByColumnIds().size() > 0
                            && !window.getSpecs().getQuick(i).isSubsampleKeepFlag();
                }
                replaceReorderedInput(window, isInputReordered, false);
            }
            case JoinPlan join -> {
                final ObjList<JoinInput> ordered = join.getOrderedInputs();
                boolean isMasterReordered = isReordered;
                for (int i = 1, n = ordered.size(); i < n; i++) {
                    switch (ordered.getQuick(i).getJoinType()) {
                        case INNER, CROSS, LEFT_OUTER -> {
                        }
                        default -> isMasterReordered = false;
                    }
                }
                for (int i = 0, n = ordered.size(); i < n; i++) {
                    final JoinInput input = ordered.getQuick(i);
                    if (input.getInput() != null) {
                        input.setInput(removeReorderedSorts0(input.getInput(), i == 0 && isMasterReordered, false));
                    }
                }
            }
            case SetOperationPlan _ -> {
                for (int i = 0, n = plan.inputCount(); i < n; i++) {
                    plan.replaceInput(i, removeReorderedSorts0(plan.inputAt(i), isReordered, isReordered));
                }
            }
            default -> {
                for (int i = 0, n = plan.inputCount(); i < n; i++) {
                    plan.replaceInput(i, removeReorderedSorts0(plan.inputAt(i), false, false));
                }
            }
        }
        return plan;
    }

    private static void replaceReorderedInput(LogicalPlan plan, boolean isReordered, boolean isSetBranchReordered) {
        final int previousTimestampId = plan.inputAt(0).getOutput().getTimestampColumnId();
        final LogicalPlan replacement = removeReorderedSorts0(plan.inputAt(0), isReordered, isSetBranchReordered);
        plan.replaceInput(0, replacement);
        final int timestampId = replacement.getOutput().getTimestampColumnId();
        if (timestampId == previousTimestampId || plan.getOutput().getTimestampIndex() >= 0) {
            return;
        }
        if (plan instanceof ProjectPlan project) {
            for (int i = 0, n = project.getExpressions().size(); i < n; i++) {
                if (project.getExpressions().getQuick(i) instanceof ColumnExpression column && column.getColumnId() == timestampId) {
                    project.getOutput().setTimestampIndex(i);
                    return;
                }
            }
        } else if (plan instanceof WindowPlan) {
            plan.getOutput().setTimestampColumnId(timestampId);
        }
    }

    private static void requireScan(ScanPlan scan, SortDirection direction, SortPlan order, LimitPlan limit, boolean isRowOrderRequired) {
        scan.setScanDirection(direction);
        scan.getRequestedOrder().of(order);
        scan.setLimit(limit == null ? null : limit.getLo(), limit == null ? null : limit.getHi());
        scan.setRowOrderRequired(isRowOrderRequired);
    }

    private static SortDirection scanDirection(ScanPlan scan, int orderColumnId, SortDirection direction) {
        return orderColumnId == scan.getOutput().getTimestampColumnId() && direction == SortDirection.DESCENDING
                ? SortDirection.DESCENDING : SortDirection.ASCENDING;
    }

    private boolean hasGroupByWindowFunction(WindowPlan window) {
        for (int i = 0, n = window.getFunctions().size(); i < n; i++) {
            if (functionFactoryCache.isGroupBy(window.getFunctions().getQuick(i).getName())) {
                return true;
            }
        }
        return false;
    }

    /**
     * Re-expresses an order over the aggregate's grouping keys as an order over its input, or returns null when a key
     * is not a grouping column.
     */
    private SortPlan keyOrder(AggregatePlan aggregate, SortPlan order) {
        if (order == null) {
            return null;
        }
        final SortPlan mapped = sorts.next().of(aggregate.getInput(), order.getPosition());
        for (int i = 0, n = order.getColumnIds().size(); i < n; i++) {
            final int index = aggregate.getOutput().getColumnIndexById(order.getColumnIds().getQuick(i));
            if (index < 0 || index >= aggregate.getGroupingExpressions().size()
                    || !(aggregate.getGroupingExpressions().getQuick(index) instanceof ColumnExpression column) || !column.isDirectReference()) {
                return null;
            }
            mapped.getColumnIds().add(column.getColumnId());
            mapped.getDirections().add(order.getDirections().getQuick(i));
        }
        return mapped;
    }

    /**
     * Re-expresses an order over the projection's output as an order over its input, or returns null when a key is
     * computed or aliased.
     */
    private SortPlan projectedOrder(ProjectPlan project, SortPlan order) {
        if (order == null || order.hasAliasedKey()) {
            return null;
        }
        final SortPlan mapped = sorts.next().of(project.getInput(), order.getPosition());
        for (int i = 0, n = order.getColumnIds().size(); i < n; i++) {
            final int index = project.getOutput().getColumnIndexById(order.getColumnIds().getQuick(i));
            if (index < 0 || !(project.getExpressions().getQuick(index) instanceof ColumnExpression column)) {
                return null;
            }
            mapped.getColumnIds().add(column.getColumnId());
            mapped.getDirections().add(order.getDirections().getQuick(i));
        }
        return mapped;
    }

    /**
     * Records what the consumer of {@code plan} requires and pushes it on to the inputs: rows ordered by
     * {@code orderColumnId} in {@code direction}, an {@code order} an access path may deliver, a {@code limit} on the
     * rows read, and whether the row order matters at all.
     */
    private void require(LogicalPlan plan, int orderColumnId, SortDirection direction, SortPlan order, LimitPlan limit, boolean isRowOrderRequired) {
        switch (plan) {
            case WindowPlan window -> requireWindow(window, orderColumnId, direction, order,
                    window.isSelectOrdered() && order != null && order.getInput() == plan, isRowOrderRequired);
            case ProjectPlan project when project.getInput() instanceof WindowPlan window && LogicalPlans.isWindowOutputProjection(project, window) ->
                    requireWindow(window, LogicalPlans.projectedSourceColumnId(project, orderColumnId), direction, projectedOrder(project, order),
                            window.isSelectOrdered() && order != null && order.getInput() == plan, isRowOrderRequired);
            case ProjectPlan project when project.getInput() instanceof WindowJoinPlan windowJoin && LogicalPlans.isColumnOnlyProjection(project) ->
                    requireWindowJoin(windowJoin);
            case LatestByPlan latest -> {
                final ScanPlan scan = LogicalPlans.latestByScan(latest);
                if (scan != null) {
                    requireScan(scan, SortDirection.DESCENDING, null, null, true);
                } else {
                    requireNothing(LogicalPlans.latestByBase(latest));
                }
            }
            case SampleByPlan sample -> requireNothing(LogicalPlans.sampleByBase(sample));
            case FillPlan fill -> requireNothing(fill.getInput());
            case ScanPlan scan ->
                    requireScan(scan, orderColumnId >= 0 ? scanDirection(scan, orderColumnId, direction) : SortDirection.ASCENDING,
                            null, null, true);
            case FunctionSourcePlan _ -> {
            }
            case DistinctPlan distinct -> requireNothing(distinct.getInput());
            case LimitPlan sortedLimit when LogicalPlans.hasSortUnderStableProjects(sortedLimit.getInput()) ->
                    requireSortedLimit(sortedLimit.getInput(), sortedLimit);
            case AggregatePlan aggregate -> requireAggregate(aggregate, orderColumnId, direction, order);
            case JoinPlan join -> requireJoin(join, orderColumnId, direction, order, isRowOrderRequired);
            case WindowJoinPlan windowJoin -> requireWindowJoin(windowJoin);
            case SetOperationPlan operation ->
                    requireSetOperation(operation, orderColumnId, direction, isRowOrderRequired);
            default -> requireUnary(plan, orderColumnId, direction, order, limit, isRowOrderRequired);
        }
    }

    private void requireAggregate(AggregatePlan aggregate, int orderColumnId, SortDirection direction, SortPlan order) {
        if (aggregate.getInput() instanceof HorizonJoinPlan horizon) {
            requireNothing(horizon.getMaster());
            for (int i = 0, n = horizon.getSlaves().size(); i < n; i++) {
                requireNothing(horizon.getSlaves().getQuick(i).getInput());
            }
            return;
        }
        boolean isInputRowOrderRequired = false;
        for (int i = 0, n = aggregate.getAggregates().size(); i < n; i++) {
            if (aggregate.getAggregates().getQuick(i).getOverload().isOrderSensitiveAggregate()) {
                isInputRowOrderRequired = true;
                break;
            }
        }
        int inputOrderColumnId = -1;
        final int orderIndex = aggregate.getAggregates().size() == 0 ? aggregate.getOutput().getColumnIndexById(orderColumnId) : -1;
        if (orderIndex >= 0 && orderIndex < aggregate.getGroupingExpressions().size()
                && aggregate.getGroupingExpressions().getQuick(orderIndex) instanceof ColumnExpression key && key.isDirectReference()) {
            inputOrderColumnId = key.getColumnId();
        }
        final LogicalPlan input = LogicalPlans.skipRenames(aggregate.getInput());
        SortPlan inputOrder = keyOrder(aggregate, order);
        for (LogicalPlan plan = aggregate.getInput(); plan != input; plan = plan.inputAt(0)) {
            final ProjectPlan rename = (ProjectPlan) plan;
            if (inputOrderColumnId >= 0) {
                inputOrderColumnId = LogicalPlans.projectedSourceColumnId(rename, inputOrderColumnId);
            }
            inputOrder = projectedOrder(rename, inputOrder);
        }
        require(LogicalPlans.aggregateBase(aggregate), inputOrderColumnId,
                inputOrderColumnId < 0 ? null : direction, inputOrder, null, isInputRowOrderRequired);
    }

    private void requireJoin(JoinPlan join, int orderColumnId, SortDirection direction, SortPlan order, boolean isRowOrderRequired) {
        final ObjList<JoinInput> ordered = join.getOrderedInputs();
        final LogicalPlan master = ordered.getQuick(0).getInput();
        boolean isMasterTimestampRequired = false;
        for (int i = 1, n = ordered.size(); i < n; i++) {
            isMasterTimestampRequired |= ordered.getQuick(i).getJoinType().isTemporal();
        }
        final boolean isMasterOrderPreserved = LogicalPlans.isMasterOrderPreserved(join);
        final OutputSchema masterOutput = master.getOutput();
        if (isMasterOrderPreserved && orderColumnId >= 0 && masterOutput.getColumnIndexById(orderColumnId) >= 0) {
            require(master, orderColumnId, direction, hasColumns(masterOutput, order) ? order : null, null, isRowOrderRequired);
        } else if (isMasterOrderPreserved && hasColumns(masterOutput, order)) {
            require(master, -1, null, order, null, isRowOrderRequired);
        } else {
            require(master, -1, null, null, null, isMasterTimestampRequired || isRowOrderRequired);
        }
        final boolean wasJoinSlaveInput = isJoinSlaveInput;
        isJoinSlaveInput = true;
        try {
            for (int i = 1, n = ordered.size(); i < n; i++) {
                final JoinInput step = ordered.getQuick(i);
                if (step.getJoinType() != JoinKind.UNNEST) {
                    require(step.getInput(), -1, null, null, null, step.getJoinType().isTemporal() || isRowOrderRequired);
                }
            }
        } finally {
            isJoinSlaveInput = wasJoinSlaveInput;
        }
    }

    private void requireNothing(LogicalPlan plan) {
        require(plan, -1, null, null, null, true);
    }

    private void requireSetOperation(SetOperationPlan operation, int orderColumnId, SortDirection direction, boolean isRowOrderRequired) {
        operation.setRequestedOrder(orderColumnId, direction);
        final int orderIndex = operation.getOperation() == SetOperationKind.UNION_ALL ? operation.getOutput().getColumnIndexById(orderColumnId) : -1;
        require(operation.getLeft(), orderIndex < 0 ? -1 : operation.getLeft().getOutput().getColumnId(orderIndex), direction, null, null, isRowOrderRequired);
        require(operation.getRight(), orderIndex < 0 ? -1 : operation.getRight().getOutput().getColumnId(orderIndex), direction, null, null, isRowOrderRequired);
    }

    /**
     * A join slave never scans backward for a sort that it re-sorts anyway, unless the sort reverses a negative LIMIT.
     */
    private void requireSortInput(SortPlan sort, int orderColumnId, SortDirection direction, LimitPlan limit) {
        if (isJoinSlaveInput && direction == SortDirection.DESCENDING && !sort.isReversal()) {
            require(sort.getInput(), -1, null, null, null, false);
        } else {
            require(sort.getInput(), orderColumnId, direction, sort, limit, false);
        }
    }

    private void requireSortedLimit(LogicalPlan plan, LimitPlan limit) {
        if (plan instanceof ProjectPlan project) {
            requireSortedLimit(project.getInput(), limit);
            return;
        }
        final SortPlan sort = (SortPlan) plan;
        final int orderColumnId = sort.getColumnIds().size() == 1 || limit.getHi() == null
                && !(limit.getLo() instanceof ConstantExpression lo && lo.getLongValue() < 0) ? sort.getColumnIds().getQuick(0) : -1;
        requireSortInput(sort, orderColumnId, sort.getDirections().getQuick(0), limit);
    }

    private void requireUnary(LogicalPlan plan, int orderColumnId, SortDirection direction, SortPlan order, LimitPlan limit, boolean isRowOrderRequired) {
        int inputOrderColumnId = -1;
        SortDirection inputDirection = null;
        SortPlan inputOrder = null;
        LimitPlan inputLimit = null;
        boolean isInputRowOrderRequired = isRowOrderRequired;
        switch (plan) {
            case FilterPlan filter -> {
                inputOrderColumnId = orderColumnId;
                inputDirection = direction;
                inputOrder = order;
                inputLimit = filter.getInput() instanceof ScanPlan ? limit : null;
            }
            case ProjectPlan project -> {
                project.setRequestedOrderColumnId(orderColumnId);
                project.getRequestedOrder().of(order);
                if (project.hasTimestampDeclaration()) {
                    isInputRowOrderRequired = true;
                }
                inputOrderColumnId = LogicalPlans.projectedSourceColumnId(project, orderColumnId);
                inputDirection = inputOrderColumnId < 0 ? null : direction;
                if (LogicalPlans.hasNativeFilterInput(project)) {
                    inputOrder = projectedOrder(project, order);
                    inputLimit = order == null || inputOrder != null ? limit : null;
                } else if (project.getInput() instanceof AggregatePlan || project.getInput() instanceof WindowPlan
                        || LogicalPlans.hasOrderedJoinMasterInput(project)) {
                    inputOrder = projectedOrder(project, order);
                }
            }
            case SortPlan sort -> {
                isInputRowOrderRequired = false;
                inputLimit = limit;
                if (sort.getColumnIds().size() == 1) {
                    inputOrderColumnId = sort.getColumnIds().getQuick(0);
                    inputDirection = sort.getDirections().getQuick(0);
                }
            }
            case LimitPlan limitPlan -> {
                inputLimit = limitPlan;
                isInputRowOrderRequired = true;
            }
            default -> {
            }
        }
        final LogicalPlan input = plan.inputAt(0);
        switch (plan) {
            case FilterPlan filter when filter.getPredicate() != null && input instanceof ScanPlan scan ->
                    requireScan(scan, scanDirection(scan, inputOrderColumnId, inputDirection), inputOrder, inputLimit, isInputRowOrderRequired);
            case LimitPlan _ when input instanceof DistinctPlan distinct -> requireNothing(distinct.getInput());
            case SortPlan sort -> requireSortInput(sort, inputOrderColumnId, inputDirection, inputLimit);
            default ->
                    require(input, inputOrderColumnId, inputDirection, inputOrder, inputLimit, isInputRowOrderRequired);
        }
    }

    private void requireWindow(WindowPlan window, int orderColumnId, SortDirection direction, SortPlan order, boolean isQueryOrder,
                               boolean isRowOrderRequired) {
        final LogicalPlan input = window.getInput();
        int inputOrderColumnId = -1;
        SortDirection inputDirection = null;
        if (orderColumnId >= 0) {
            if (input.getOutput().getColumnIndexById(orderColumnId) >= 0) {
                inputOrderColumnId = orderColumnId;
                inputDirection = direction;
            }
        } else if (hasNestedUnionAll(input)) {
            final ObjList<WindowSpec> specs = window.getSpecs();
            for (int i = 0, n = specs.size(); i < n; i++) {
                final WindowSpec spec = specs.getQuick(i);
                if (spec.getOrderByColumnIds().size() != 1) {
                    inputOrderColumnId = -1;
                    break;
                }
                final int columnId = spec.getOrderByColumnIds().getQuick(0);
                final SortDirection specDirection = spec.getOrderByDirections().getQuick(0);
                if (i > 0 && (columnId != inputOrderColumnId || specDirection != inputDirection)) {
                    inputOrderColumnId = -1;
                    break;
                }
                inputOrderColumnId = columnId;
                inputDirection = specDirection;
            }
        }
        window.getQueryOrder().of(isQueryOrder ? order : null);
        require(input, inputOrderColumnId, inputOrderColumnId < 0 ? null : inputDirection,
                hasColumns(input.getOutput(), order) && !order.hasAliasedKey() ? order : null, null,
                !isQueryOrder && isRowOrderRequired && !hasGroupByWindowFunction(window));
    }

    private void requireWindowJoin(WindowJoinPlan windowJoin) {
        requireNothing(windowJoin.getMaster());
        for (int i = 0, n = windowJoin.getSteps().size(); i < n; i++) {
            requireNothing(windowJoin.getSteps().getQuick(i).getSlave());
        }
    }
}
