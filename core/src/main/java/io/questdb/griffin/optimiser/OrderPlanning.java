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

import io.questdb.cairo.CairoConfiguration;
import io.questdb.griffin.FunctionFactoryCache;
import io.questdb.griffin.LogicalPlans;
import io.questdb.griffin.plan.logical.AggregatePlan;
import io.questdb.griffin.plan.logical.BoundExpression;
import io.questdb.griffin.plan.logical.ColumnExpression;
import io.questdb.griffin.plan.logical.ConstantExpression;
import io.questdb.griffin.plan.logical.DistinctPlan;
import io.questdb.griffin.plan.logical.DomainReadPlan;
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
import io.questdb.griffin.plan.logical.PlanToFactoryRules;
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
import io.questdb.std.IntHashSet;
import io.questdb.std.ObjList;
import io.questdb.std.ObjectPool;

/**
 * Pushes the order each consumer requires down the plan. As an optimiser pass it marks markout-horizon sorts, drops
 * the sorts whose order a consumer re-sorts or discards and the sorts of a single row that keep its designated
 * timestamp, which leaves the projections final and derives their column reference counts, see
 * {@link ProjectPlan#deriveColumnReferenceCounts}, then records the order each consumer requires where the generator picks a physical operator by it: the
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
    private final IntHashSet columnIds;
    private final CairoConfiguration configuration;
    private final FunctionFactoryCache functionFactoryCache;
    private final ObjectPool<OrderRequirement> requirements = new ObjectPool<>(OrderRequirement.FACTORY, 16);

    OrderPlanning(CairoConfiguration configuration, FunctionFactoryCache functionFactoryCache, IntHashSet columnIds) {
        this.configuration = configuration;
        this.functionFactoryCache = functionFactoryCache;
        this.columnIds = columnIds;
    }

    @Override
    public LogicalPlan apply(LogicalPlan plan) {
        markMarkoutHorizons(plan);
        final LogicalPlan root = removeReorderedSorts(plan);
        LogicalPlans.deriveColumnReferenceCounts(root, columnIds);
        require(root, requirements.next());
        return root;
    }

    @Override
    public void clear() {
        requirements.clear();
    }

    @Override
    public String getName() {
        return "order planning";
    }

    private static void deriveJoinTimestamps(JoinPlan join) {
        final ObjList<JoinInput> steps = join.getOrderedInputs();
        for (int i = 0, n = steps.size(); i < n; i++) {
            steps.getQuick(i).getOutput().setTimestampColumnId(LogicalPlans.joinTimestampColumnId(join, i + 1));
        }
        join.getOutput().setTimestampColumnId(LogicalPlans.joinTimestampColumnId(join, steps.size()));
    }

    private static boolean hasNestedUnionAll(LogicalPlan plan) {
        return LogicalPlans.skipProjectsAndFilters(plan) instanceof SetOperationPlan operation
                && operation.getOperation() == SetOperationKind.UNION_ALL;
    }

    private static boolean hasTimestampWindowFunction(WindowPlan window) {
        for (int i = 0, n = window.getFunctions().size(); i < n; i++) {
            if ((window.getFunctions().getQuick(i).getFunctionFlags() & BoundExpression.DESIGNATED_TIMESTAMP) != 0) {
                return true;
            }
        }
        return false;
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

    /**
     * True when the sort orders by every grouping key of the aggregate beneath it, through the projections and filters
     * between them: the aggregate emits one row per key, so no tie of the sort keeps the order of the aggregate's input.
     */
    private static boolean isKeyOrdered(SortPlan sort, AggregatePlan aggregate) {
        for (int k = 0, n = aggregate.getGroupingExpressions().size(); k < n; k++) {
            if (!isSortKey(sort, aggregate.getOutput().getColumnId(k))) {
                return false;
            }
        }
        return true;
    }

    /**
     * True when the input order, {@code orderColumnId} in {@code direction}, is the order of each window whose function
     * requires its input in the order of the window.
     */
    private static boolean isOrderedInputServed(WindowPlan window, int orderColumnId, SortDirection direction) {
        for (int i = 0, n = window.getFunctions().size(); i < n; i++) {
            final WindowSpec spec = window.getSpecs().getQuick(i);
            if ((window.getFunctions().getQuick(i).getFunctionFlags() & BoundExpression.ORDERED_INPUT) != 0
                    && (spec.getOrderByColumnIds().getQuick(0) != orderColumnId || spec.getOrderByDirections().getQuick(0) != direction)) {
                return false;
            }
        }
        return true;
    }

    private static boolean isSingleRow(LogicalPlan plan) {
        plan = LogicalPlans.skipProjectsAndFilters(plan);
        return plan instanceof AggregatePlan aggregate && aggregate.getGroupingExpressions().size() == 0;
    }

    /**
     * True when the join step reads its slave by random access: a SPLICE join, and an ASOF or LT join whose slave has
     * a column the full-fat join over a slave without random access cannot carry, see
     * {@link PlanToFactoryRules#fullFatTemporalRejectedColumn}.
     */
    private static boolean isSlaveRandomAccessRequired(JoinInput step) {
        return switch (step.getJoinType()) {
            case SPLICE -> true;
            case ASOF, LT -> PlanToFactoryRules.fullFatTemporalRejectedColumn(step) >= 0;
            default -> false;
        };
    }

    /**
     * True when a key of the sort is the column, as the projections and filters beneath the sort pass it on.
     */
    private static boolean isSortKey(SortPlan sort, int columnId) {
        for (int i = 0, n = sort.getColumnIds().size(); i < n; i++) {
            int keyId = sort.getColumnIds().getQuick(i);
            for (LogicalPlan plan = sort.getChild(); keyId >= 0 && keyId != columnId; plan = plan.inputAt(0)) {
                if (plan instanceof ProjectPlan project) {
                    keyId = LogicalPlans.projectedSourceColumnId(project, keyId);
                } else if (!(plan instanceof FilterPlan)) {
                    break;
                }
            }
            if (keyId == columnId) {
                return true;
            }
        }
        return false;
    }

    /**
     * True when the sort orders a time series join input in a way the join cannot read: by the input's designated
     * timestamp descending, or by a key that is not a timestamp, which leaves the sorted rows without one.
     */
    private static boolean isUnreadTemporalOrder(SortPlan sort) {
        final int timestampId = sort.getChild().getOutput().getTimestampColumnId();
        return timestampId >= 0 && (sort.getOutput().getTimestampIndex() < 0
                || sort.getColumnIds().getQuick(0) == timestampId && sort.getDirections().getQuick(0) == SortDirection.DESCENDING);
    }

    private static void markMarkoutHorizon(SortPlan sort) {
        if (sort.getColumnIds().size() != 1 || sort.getDirections().getQuick(0) != SortDirection.ASCENDING) {
            return;
        }
        int columnId = sort.getColumnIds().getQuick(0);
        LogicalPlan plan = sort.getChild();
        while (plan instanceof ProjectPlan project) {
            final ColumnExpression column = LogicalPlans.passedColumn(project, columnId);
            plan = project.getChild();
            if (column != null) {
                columnId = column.getColumnId();
                continue;
            }
            final int index = project.getOutput().getColumnIndexById(columnId);
            if (index >= 0 && project.getExpressions().getQuick(index) instanceof FunctionExpression call && call.getArgumentCount() == 2
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
            final ColumnExpression left = LogicalPlans.passedColumn(project, leftId);
            final ColumnExpression right = LogicalPlans.passedColumn(project, rightId);
            if (left == null || right == null) {
                return;
            }
            leftId = left.getColumnId();
            rightId = right.getColumnId();
            plan = project.getChild();
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
     * The window of the first function that requires its input in the order of its window, or null when none does.
     */
    private static WindowSpec orderedInputSpec(WindowPlan window) {
        for (int i = 0, n = window.getFunctions().size(); i < n; i++) {
            if ((window.getFunctions().getQuick(i).getFunctionFlags() & BoundExpression.ORDERED_INPUT) != 0) {
                return window.getSpecs().getQuick(i);
            }
        }
        return null;
    }

    /**
     * Drops the sorts whose order a consumer re-sorts or discards, and the sorts of a single row that keep its
     * designated timestamp; a window's own ORDER BY and a markout sort stay.
     */
    private static LogicalPlan removeReorderedSorts(LogicalPlan root) {
        return removeReorderedSorts0(root, false, false, false, null);
    }

    /**
     * {@code isReordered}: a consumer above re-sorts or discards the order of {@code plan};
     * {@code isSetBranchReordered}: that consumer is a set operation, whose branch order never survives;
     * {@code isTimestampRead}: the sort that re-sorts {@code plan}, or a window between them, reads the designated
     * timestamp of {@code plan}: a markout sort reads its input in that order, any other sort designates its own key;
     * {@code resortedAggregate}: the aggregate under that sort whose every grouping key it orders by, or null.
     */
    private static LogicalPlan removeReorderedSorts0(LogicalPlan plan, boolean isReordered, boolean isSetBranchReordered, boolean isTimestampRead,
                                                     AggregatePlan resortedAggregate) {
        switch (plan) {
            case SortPlan sort -> {
                final LogicalPlan source = LogicalPlans.skipProjects(sort.getChild());
                final AggregatePlan sortedAggregate = LogicalPlans.skipProjectsAndFilters(sort.getChild()) instanceof AggregatePlan aggregate
                        && isKeyOrdered(sort, aggregate) ? aggregate : null;
                final LogicalPlan input = removeReorderedSorts0(sort.getChild(), true, false, sort.isMarkoutHorizon(), sortedAggregate);
                if (source instanceof WindowPlan) {
                    sort.replaceInput(0, input);
                    return sort;
                }
                if (isReordered && !sort.isMarkoutHorizon() && (isSetBranchReordered && !(source instanceof FillPlan) || sort.getOutput().getTimestampIndex() < 0
                        || sort.getOutput().getTimestampIndex() == input.getOutput().getTimestampIndex()
                        || !isSetBranchReordered && !isTimestampRead)) {
                    return input;
                }
                if (isSingleRow(input) && sort.getOutput().getTimestampIndex() == input.getOutput().getTimestampIndex()) {
                    return input;
                }
                sort.replaceInput(0, input);
                return sort;
            }
            case ProjectPlan project -> {
                final boolean isSorted = project.getChild() instanceof SortPlan;
                replaceReorderedInput(project, isReordered && LogicalPlans.isOrderTransparent(project), isSetBranchReordered, isTimestampRead,
                        resortedAggregate);
                if (isSorted && project.getChild() instanceof ProjectPlan inner && isHiddenSortKeyPrefix(project, inner)) {
                    inner.getExpressions().setPos(project.getExpressions().size());
                    inner.getOutput().copyFrom(project.getOutput());
                    return inner;
                }
            }
            case FilterPlan filter ->
                    replaceReorderedInput(filter, isReordered && LogicalPlans.isOrderTransparent(filter), isSetBranchReordered,
                            isTimestampRead, resortedAggregate);
            case WindowPlan window ->
                    replaceReorderedInput(window, isReordered && LogicalPlans.isInputOrderIgnored(window), false,
                            isTimestampRead || hasTimestampWindowFunction(window), resortedAggregate);
            case JoinPlan join -> {
                if (isReordered && !isTimestampRead) {
                    removeUnreadTemporalOrder(join);
                }
                final ObjList<JoinInput> ordered = join.getOrderedInputs();
                final boolean isMasterReordered = isReordered && LogicalPlans.isMasterOrderPreserved(join);
                for (int i = 0, n = ordered.size(); i < n; i++) {
                    final JoinInput input = ordered.getQuick(i);
                    if (input.getInput() != null) {
                        input.setInput(removeReorderedSorts0(input.getInput(), i == 0 && isMasterReordered, false, isTimestampRead, resortedAggregate));
                    }
                }
                deriveJoinTimestamps(join);
            }
            case HorizonJoinPlan horizon -> {
                if (isReordered && !isTimestampRead) {
                    removeUnreadTemporalOrder(horizon);
                }
                for (int i = 0, n = horizon.inputCount(); i < n; i++) {
                    horizon.replaceInput(i, removeReorderedSorts0(horizon.inputAt(i), false, false, isTimestampRead, resortedAggregate));
                }
            }
            case AggregatePlan aggregate when aggregate == resortedAggregate && LogicalPlans.isAggregateOrderIndependent(aggregate) ->
                    replaceReorderedInput(aggregate, isReordered, false, isTimestampRead, resortedAggregate);
            case SetOperationPlan _ -> {
                for (int i = 0, n = plan.inputCount(); i < n; i++) {
                    plan.replaceInput(i, removeReorderedSorts0(plan.inputAt(i), isReordered, isReordered, isTimestampRead, resortedAggregate));
                }
            }
            default -> {
                for (int i = 0, n = plan.inputCount(); i < n; i++) {
                    plan.replaceInput(i, removeReorderedSorts0(plan.inputAt(i), false, false, isTimestampRead, resortedAggregate));
                }
            }
        }
        return plan;
    }

    /**
     * Drops the sorts a time series join cannot read from the top of its input, through the projections and filters
     * that pass the input's order on; a sort the join may read stays.
     */
    private static LogicalPlan removeUnreadOrder(LogicalPlan plan) {
        switch (plan) {
            case SortPlan sort when isUnreadTemporalOrder(sort) -> {
                return removeUnreadOrder(sort.getChild());
            }
            case FilterPlan filter when LogicalPlans.isOrderTransparent(filter) ->
                    filter.replaceInput(0, removeUnreadOrder(filter.getChild()));
            case ProjectPlan project when LogicalPlans.isOrderTransparent(project) -> {
                project.replaceInput(0, removeUnreadOrder(project.getChild()));
                if (project.getOutput().getTimestampIndex() < 0) {
                    project.getOutput().setTimestampIndex(LogicalPlans.projectedColumnIndex(project,
                            project.getChild().getOutput().getTimestampColumnId()));
                }
            }
            default -> {
            }
        }
        return plan;
    }

    private static void replaceReorderedInput(LogicalPlan plan, boolean isReordered, boolean isSetBranchReordered, boolean isTimestampRead,
                                              AggregatePlan resortedAggregate) {
        final int previousTimestampId = plan.inputAt(0).getOutput().getTimestampColumnId();
        final LogicalPlan replacement = removeReorderedSorts0(plan.inputAt(0), isReordered, isSetBranchReordered, isTimestampRead, resortedAggregate);
        plan.replaceInput(0, replacement);
        final int timestampId = replacement.getOutput().getTimestampColumnId();
        if (timestampId == previousTimestampId || plan.getOutput().getTimestampIndex() >= 0) {
            return;
        }
        if (plan instanceof ProjectPlan project) {
            project.getOutput().setTimestampIndex(LogicalPlans.projectedColumnIndex(project, timestampId));
        } else if (plan instanceof WindowPlan) {
            plan.getOutput().setTimestampColumnId(timestampId);
        }
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
     * True when a parallel filter cannot hold the last rows a constant negative LIMIT selects, which the LIMIT then
     * applies over all rows of the filter instead.
     */
    private boolean isPastFilterCapacity(LimitPlan limit) {
        return limit.getLo() instanceof ConstantExpression lo && LogicalPlans.mayBeNegativeLimit(lo)
                && LogicalPlans.limitValue(lo) < -configuration.getSqlMaxNegativeLimit();
    }

    /**
     * Records what the consumer of {@code plan} requires, see {@link OrderRequirement}, and pushes it on to the inputs.
     * A join step that reads its slave by random access, see {@link #isSlaveRandomAccessRequired}, needs a slave a merged
     * UNION ALL does not provide, so a set operation the slave reads through projections, filters, limits and sorts gets
     * no order request, and the sort over it sorts.
     */
    private void require(LogicalPlan plan, OrderRequirement requirement) {
        final boolean isRandomAccessRequired = requirement.isRandomAccessRequired();
        requirement.passTo(plan);
        switch (plan) {
            case WindowPlan window ->
                    requireWindow(window, requirement, window.isSelectOrdered() && requirement.isOrderOf(plan));
            case ProjectPlan project when PlanToFactoryRules.generatedWindow(project) instanceof WindowPlan window ->
                    requireWindow(window, requirements.next().inputOf(requirement)
                                    .orderBy(LogicalPlans.projectedSourceColumnId(project, requirement.getOrderColumnId()), requirement.getDirection())
                                    .order(project, requirement)
                                    .rowOrderRequired(requirement.isRowOrderRequired()),
                            window.isSelectOrdered() && requirement.isOrderOf(plan));
            case ProjectPlan project when PlanToFactoryRules.generatedWindowJoin(project) instanceof WindowJoinPlan windowJoin ->
                    requireWindowJoin(windowJoin, requirement);
            case LatestByPlan latest -> {
                final ScanPlan scan = PlanToFactoryRules.latestByScan(latest);
                if (scan != null) {
                    requireScan(scan, SortDirection.DESCENDING, requirements.next().inputOf(requirement));
                } else {
                    requireNothing(PlanToFactoryRules.latestByBase(latest), requirement);
                }
            }
            case SampleByPlan sample -> requireNothing(PlanToFactoryRules.sampleByBase(sample), requirement);
            case FillPlan fill -> requireNothing(fill.getChild(), requirement);
            case ScanPlan scan -> requireScan(scan, requirement.getOrderColumnId() >= 0
                    ? scanDirection(scan, requirement.getOrderColumnId(), requirement.getDirection()) : SortDirection.ASCENDING, requirements.next().inputOf(requirement));
            case FunctionSourcePlan _, DomainReadPlan _ -> {
            }
            case DistinctPlan distinct -> requireNothing(distinct.getChild(), requirement);
            case LimitPlan sortedLimit when LogicalPlans.hasSortUnderStableProjects(sortedLimit.getChild()) ->
                    requireSortedLimit(sortedLimit.getChild(), sortedLimit, requirement);
            case AggregatePlan aggregate -> requireAggregate(aggregate, requirement);
            case JoinPlan join -> requireJoin(join, requirement);
            case WindowJoinPlan windowJoin -> requireWindowJoin(windowJoin, requirement);
            case HorizonJoinPlan horizon -> {
                // Expansion counts output rows, not master rows. Neither LIMIT nor a requested
                // descending output order may cross it; every operand needs temporal row order.
                for (int i = 0, n = horizon.inputCount(); i < n; i++) {
                    require(horizon.inputAt(i), requirements.next().inputOf(requirement).rowOrderRequired(true));
                }
            }
            case SetOperationPlan operation when isRandomAccessRequired ->
                    requireSetOperation(operation, -1, null, requirement);
            case SetOperationPlan operation ->
                    requireSetOperation(operation, requirement.getOrderColumnId(), requirement.getDirection(), requirement);
            default -> requireUnary(plan, requirement);
        }
    }

    /**
     * An aggregate never emits its groups in the order of its input, so it requests no order an access path may
     * deliver.
     */
    private void requireAggregate(AggregatePlan aggregate, OrderRequirement requirement) {
        if (aggregate.getChild() instanceof HorizonJoinPlan horizon) {
            requireNothing(horizon.getMaster(), requirement);
            for (int i = 0, n = horizon.getSlaves().size(); i < n; i++) {
                requireNothing(horizon.getSlaves().getQuick(i).getInput(), requirement);
            }
            return;
        }
        int inputOrderColumnId = -1;
        final int orderIndex = aggregate.getAggregates().size() == 0 ? aggregate.getOutput().getColumnIndexById(requirement.getOrderColumnId()) : -1;
        if (orderIndex >= 0 && orderIndex < aggregate.getGroupingExpressions().size()
                && aggregate.getGroupingExpressions().getQuick(orderIndex) instanceof ColumnExpression key && key.isDirectReference()) {
            inputOrderColumnId = key.getColumnId();
        }
        final LogicalPlan input = LogicalPlans.skipRenames(aggregate.getChild());
        for (LogicalPlan plan = aggregate.getChild(); plan != input && inputOrderColumnId >= 0; plan = plan.inputAt(0)) {
            inputOrderColumnId = LogicalPlans.projectedSourceColumnId((ProjectPlan) plan, inputOrderColumnId);
        }
        require(PlanToFactoryRules.aggregateBase(aggregate), requirements.next().inputOf(requirement)
                .singleGroupInput(aggregate.getGroupingExpressions().size() == 0)
                .orderBy(inputOrderColumnId, inputOrderColumnId < 0 ? null : requirement.getDirection())
                .rowOrderRequired(LogicalPlans.hasTableOrderAggregate(aggregate.getAggregates())));
    }

    /**
     * A plain LIMIT reaches the master of a join that emits at least one row for each master row, in master order:
     * the first or last {@code n} joined rows come from the first or last {@code n} master rows. A LIMIT that may be
     * negative reaches the master of the join under it that may emit no row for a master row, see
     * {@link PlanToFactoryRules#masterSuffixJoin}. A constant LIMIT reaches the last input of a CROSS join without a
     * filter as well: the first or last {@code n} joined rows join the first or last {@code n} of its rows.
     */
    private void requireJoin(JoinPlan join, OrderRequirement requirement) {
        final ObjList<JoinInput> ordered = join.getOrderedInputs();
        final LogicalPlan master = ordered.getQuick(0).getInput();
        final boolean isMasterOrderPreserved = LogicalPlans.isMasterOrderPreserved(join);
        final OutputSchema masterOutput = master.getOutput();
        final int orderColumnId = requirement.getOrderColumnId();
        final LimitPlan limit = requirement.getLimit();
        final boolean isOrdered = !requirement.getOrder().isEmpty();
        final OrderRequirement masterRequirement = requirements.next().inputOf(requirement);
        if (isMasterOrderPreserved && orderColumnId >= 0 && masterOutput.getColumnIndexById(orderColumnId) >= 0) {
            masterRequirement.orderBy(orderColumnId, requirement.getDirection()).order(requirement, masterOutput)
                    .rowOrderRequired(requirement.isRowOrderRequired());
        } else if (isMasterOrderPreserved && requirement.isOrderIn(masterOutput)) {
            masterRequirement.order(requirement).rowOrderRequired(requirement.isRowOrderRequired());
        } else {
            final boolean isMasterLimited = !isOrdered && limit != null
                    && (LogicalPlans.isMasterRowPreserved(join) || PlanToFactoryRules.masterSuffixJoin(limit) == join);
            masterRequirement.limit(isMasterLimited ? limit : null)
                    .rowOrderRequired(LogicalPlans.consumesInputTimestamp(join, 0) || requirement.isRowOrderRequired());
        }
        require(master, masterRequirement);
        for (int i = 1, n = ordered.size(); i < n; i++) {
            final JoinInput step = ordered.getQuick(i);
            final JoinKind joinType = step.getJoinType();
            if (joinType != JoinKind.UNNEST) {
                final boolean isLimited = !isOrdered && limit != null && i == n - 1 && joinType == JoinKind.CROSS
                        && step.getPostJoinFilter() == null && limit.getLo() instanceof ConstantExpression;
                require(step.getInput(), requirements.next().inputOf(requirement).joinSlave(isSlaveRandomAccessRequired(step))
                        .limit(isLimited ? limit : null).rowOrderRequired(joinType.isTemporal() || requirement.isRowOrderRequired()));
            }
        }
    }

    private void requireNothing(LogicalPlan plan, OrderRequirement consumer) {
        require(plan, requirements.next().inputOf(consumer));
    }

    private void requireScan(ScanPlan scan, SortDirection direction, OrderRequirement requirement) {
        final LimitPlan limit = requirement.getLimit();
        scan.setScanDirection(direction);
        scan.getRequestedOrder().copyFrom(requirement.getOrder());
        scan.setLimit(limit == null ? null : limit.getLo(), limit == null ? null : limit.getHi());
        if (requirement.isRowOrderRequired()) {
            scan.setRowOrder(ScanPlan.RowOrder.REQUIRED);
        } else {
            scan.setRowOrder(requirement.isSingleGroupInput() ? ScanPlan.RowOrder.ANY_SINGLE_GROUP : ScanPlan.RowOrder.ANY);
        }
    }

    private void requireSetOperation(SetOperationPlan operation, int orderColumnId, SortDirection direction, OrderRequirement requirement) {
        operation.setRequestedOrder(orderColumnId, direction);
        final int orderIndex = operation.getOperation() == SetOperationKind.UNION_ALL ? operation.getOutput().getColumnIndexById(orderColumnId) : -1;
        require(operation.getLeft(), requirements.next().inputOf(requirement)
                .orderBy(orderIndex < 0 ? -1 : operation.getLeft().getOutput().getColumnId(orderIndex), direction)
                .rowOrderRequired(requirement.isRowOrderRequired()));
        require(operation.getRight(), requirements.next().inputOf(requirement)
                .orderBy(orderIndex < 0 ? -1 : operation.getRight().getOutput().getColumnId(orderIndex), direction)
                .rowOrderRequired(requirement.isRowOrderRequired()));
    }

    /**
     * A join slave never scans backward for a sort that it re-sorts anyway, unless the sort reverses a negative LIMIT.
     */
    private void requireSortInput(SortPlan sort, int orderColumnId, SortDirection direction, LimitPlan limit, OrderRequirement requirement) {
        final OrderRequirement input = requirements.next().inputOf(requirement).rowOrderRequired(false);
        if (!requirement.isJoinSlaveInput() || direction != SortDirection.DESCENDING || sort.isReversal()) {
            input.orderBy(orderColumnId, direction).order(sort).limit(limit);
        }
        require(sort.getChild(), input);
    }

    private void requireSortedLimit(LogicalPlan plan, LimitPlan limit, OrderRequirement requirement) {
        if (plan instanceof ProjectPlan project) {
            requireSortedLimit(project.getChild(), limit, requirement);
            return;
        }
        final SortPlan sort = (SortPlan) plan;
        final int orderColumnId = sort.getColumnIds().size() == 1 || limit.getHi() == null
                && !(limit.getLo() instanceof ConstantExpression lo && lo.getLongValue() < 0) ? sort.getColumnIds().getQuick(0) : -1;
        requireSortInput(sort, orderColumnId, sort.getDirections().getQuick(0), limit, requirement);
    }

    private void requireUnary(LogicalPlan plan, OrderRequirement requirement) {
        final int orderColumnId = requirement.getOrderColumnId();
        final SortDirection direction = requirement.getDirection();
        final LimitPlan limit = requirement.getLimit();
        final OrderRequirement input = requirements.next().inputOf(requirement);
        int inputOrderColumnId = -1;
        SortDirection inputDirection = null;
        LimitPlan inputLimit = null;
        boolean isInputRowOrderRequired = requirement.isRowOrderRequired();
        switch (plan) {
            case FilterPlan filter -> {
                inputOrderColumnId = orderColumnId;
                inputDirection = direction;
                if (PlanToFactoryRules.isFusedFilter(filter) || filter.getPredicate() instanceof ConstantExpression) {
                    input.order(requirement);
                }
                inputLimit = filter.getChild() instanceof ScanPlan && limit != null && !isPastFilterCapacity(limit) ? limit : null;
            }
            case ProjectPlan project -> {
                project.setRequestedOrderColumnId(orderColumnId);
                project.getRequestedOrder().copyFrom(requirement.getOrder());
                if (project.hasTimestampDeclaration()) {
                    isInputRowOrderRequired = true;
                }
                inputOrderColumnId = LogicalPlans.projectedSourceColumnId(project, orderColumnId);
                inputDirection = inputOrderColumnId < 0 ? null : direction;
                final boolean isOrdered = !requirement.getOrder().isEmpty();
                if (PlanToFactoryRules.hasNativeFilterInput(project)) {
                    input.order(project, requirement);
                    inputLimit = !isOrdered || !input.getOrder().isEmpty() ? limit : null;
                } else if (project.getChild() instanceof WindowPlan || PlanToFactoryRules.hasOrderedJoinMasterInput(project)) {
                    input.order(project, requirement);
                }
                if (!isOrdered && LogicalPlans.skipProjects(project.getChild()) instanceof JoinPlan) {
                    inputLimit = limit;
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
        final LogicalPlan child = plan.inputAt(0);
        switch (plan) {
            case FilterPlan filter when filter.getPredicate() != null && child instanceof ScanPlan scan ->
                    requireScan(scan, scanDirection(scan, inputOrderColumnId, inputDirection), input.limit(inputLimit).rowOrderRequired(isInputRowOrderRequired));
            case LimitPlan _ when child instanceof DistinctPlan distinct ->
                    requireNothing(distinct.getChild(), requirement);
            case SortPlan sort -> requireSortInput(sort, inputOrderColumnId, inputDirection, inputLimit, requirement);
            default ->
                    require(child, input.orderBy(inputOrderColumnId, inputDirection).limit(inputLimit).rowOrderRequired(isInputRowOrderRequired));
        }
    }

    private void requireWindow(WindowPlan window, OrderRequirement requirement, boolean isQueryOrder) {
        final LogicalPlan input = window.getChild();
        final int orderColumnId = requirement.getOrderColumnId();
        int inputOrderColumnId = -1;
        SortDirection inputDirection = null;
        if (orderColumnId >= 0) {
            if (input.getOutput().getColumnIndexById(orderColumnId) >= 0) {
                inputOrderColumnId = orderColumnId;
                inputDirection = requirement.getDirection();
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
        final WindowSpec orderedInput = orderedInputSpec(window);
        final boolean isInputOrderServed = isOrderedInputServed(window, inputOrderColumnId, inputDirection);
        if (isInputOrderServed && isQueryOrder) {
            window.getQueryOrder().copyFrom(requirement.getOrder());
        } else {
            window.getQueryOrder().clear();
        }
        if (isInputOrderServed) {
            require(input, requirements.next().inputOf(requirement)
                    .orderBy(inputOrderColumnId, inputOrderColumnId < 0 ? null : inputDirection)
                    .order(requirement, input.getOutput())
                    .rowOrderRequired(orderedInput != null || !isQueryOrder && requirement.isRowOrderRequired() && !hasGroupByWindowFunction(window)));
            return;
        }
        assert orderedInput != null;
        final int orderedInputColumnId = orderedInput.getOrderByColumnIds().getQuick(0);
        final SortDirection orderedInputDirection = orderedInput.getOrderByDirections().getQuick(0);
        if (isOrderedInputServed(window, orderedInputColumnId, orderedInputDirection)) {
            require(input, requirements.next().inputOf(requirement).orderBy(orderedInputColumnId, orderedInputDirection));
        } else {
            requireNothing(input, requirement);
        }
    }

    private void requireWindowJoin(WindowJoinPlan windowJoin, OrderRequirement requirement) {
        requireNothing(windowJoin.getMaster(), requirement);
        for (int i = 0, n = windowJoin.getSteps().size(); i < n; i++) {
            requireNothing(windowJoin.getSteps().getQuick(i).getSlave(), requirement);
        }
    }

    /**
     * Drops the sorts the time series steps of the join cannot read from their inputs, for a consumer that discards
     * the join's row order.
     */
    static void removeUnreadTemporalOrder(JoinPlan join) {
        final ObjList<JoinInput> steps = join.getOrderedInputs();
        boolean hasTemporalStep = false;
        for (int i = 1, n = steps.size(); i < n; i++) {
            final JoinInput step = steps.getQuick(i);
            if (step.getJoinType().isTemporal()) {
                hasTemporalStep = true;
                step.setInput(removeUnreadOrder(step.getInput()));
            }
        }
        if (hasTemporalStep) {
            steps.getQuick(0).setInput(removeUnreadOrder(steps.getQuick(0).getInput()));
            deriveJoinTimestamps(join);
        }
    }

    /**
     * Drops the sorts the horizon join cannot read from its master and slaves, for a consumer that discards their
     * row order.
     */
    static void removeUnreadTemporalOrder(HorizonJoinPlan horizon) {
        for (int i = 0, n = horizon.inputCount(); i < n; i++) {
            horizon.replaceInput(i, removeUnreadOrder(horizon.inputAt(i)));
        }
        horizon.getOutput().setTimestampIndex(horizon.getMaster().getOutput().getTimestampIndex());
    }
}
