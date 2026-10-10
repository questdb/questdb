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
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.IndexType;
import io.questdb.cairo.sql.TableAccessInfo;
import io.questdb.griffin.FunctionFactoryCache;
import io.questdb.griffin.LogicalPlans;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlKeywords;
import io.questdb.griffin.engine.groupby.SampleByFillRecordCursorFactory;
import io.questdb.griffin.engine.orderby.SortKeyEncoder;
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
import io.questdb.griffin.plan.logical.HorizonJoinSlave;
import io.questdb.griffin.plan.logical.JoinInput;
import io.questdb.griffin.plan.logical.JoinKind;
import io.questdb.griffin.plan.logical.JoinPlan;
import io.questdb.griffin.plan.logical.LatestByPlan;
import io.questdb.griffin.plan.logical.LimitPlan;
import io.questdb.griffin.plan.logical.LogicalPlan;
import io.questdb.griffin.plan.logical.OutputSchema;
import io.questdb.griffin.plan.logical.PhysicalProperties;
import io.questdb.griffin.plan.logical.PlanVisitor;
import io.questdb.griffin.plan.logical.ProjectPlan;
import io.questdb.griffin.plan.logical.SampleByPlan;
import io.questdb.griffin.plan.logical.ScanPlan;
import io.questdb.griffin.plan.logical.SetOperationKind;
import io.questdb.griffin.plan.logical.SetOperationPlan;
import io.questdb.griffin.plan.logical.SortDirection;
import io.questdb.griffin.plan.logical.SortKeys;
import io.questdb.griffin.plan.logical.SortPlan;
import io.questdb.griffin.plan.logical.TreeWalk;
import io.questdb.griffin.plan.logical.WindowJoinPlan;
import io.questdb.griffin.plan.logical.WindowJoinStep;
import io.questdb.griffin.plan.logical.WindowPlan;
import io.questdb.griffin.plan.logical.WindowSpec;
import io.questdb.std.Chars;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;
import io.questdb.std.ObjectPool;

/**
 * Owns the order requirements and the order-sensitive physical operators. As an optimiser pass it marks
 * markout-horizon sorts, drops the sorts whose order a consumer re-sorts or discards and the sorts of a single row
 * that keep its designated timestamp, then pushes the order each consumer requires down the plan and records it where
 * the generator picks a physical operator by it: the scan direction, the requested key order, the LIMIT a filtered
 * scan may stop at and whether row order matters on a {@link ScanPlan}, the requested order of a {@link ProjectPlan}
 * and a {@link SetOperationPlan}, and the query order a {@link WindowPlan} serves. {@link AccessPathPlanning} then
 * picks each scan's access path by them and, once it has planned the inputs of a node, calls {@link #planOperators},
 * which decides from the {@link PhysicalProperties} of the inputs how the generator implements the node and records
 * it on the plan: the sort algorithm, the operator that applies a LIMIT, the windows whose order the input delivers
 * and the window factory, the SAMPLE BY factory and the order a fill reads its input in, the merge of a UNION ALL,
 * the algorithm and master side of each join step, the LATEST BY algorithm of a derived input, the timestamp a
 * projection over a window join drops and whether a filter runs in parallel. Before it plans a query level, it counts
 * the decorrelation domains that re-read each aggregate, see {@link #countSharedConsumers}. The generator builds
 * exactly what it records. From
 * the same properties it rejects a time series join, a SAMPLE BY and an aggregate function whose input does not
 * deliver the ascending designated timestamp order they require.
 */
final class OrderPlanning implements OptimiserPass {
    private static final PlanVisitor MARKOUT_HORIZONS = plan -> {
        if (plan instanceof SortPlan sort) {
            markMarkoutHorizon(sort);
        }
        return TreeWalk.CONTINUE;
    };
    private static final PlanVisitor SHARED_CONSUMERS = plan -> {
        if (plan instanceof AggregatePlan aggregate && aggregate.getSharedSource() != null
                && sharedTarget(aggregate.getSharedSource().getInput()) instanceof AggregatePlan target) {
            target.setSharedConsumerCount(target.getSharedConsumerCount() + 1);
        }
        return TreeWalk.CONTINUE;
    };
    private static final PlanVisitor SHARED_CONSUMER_COUNTS = plan -> {
        if (plan instanceof AggregatePlan aggregate) {
            aggregate.setSharedConsumerCount(0);
        }
        return TreeWalk.CONTINUE;
    };
    private final CairoConfiguration configuration;
    private final OptimiserContext context;
    private final FunctionFactoryCache functionFactoryCache;
    private final IntList readIds;
    private final IntList readOrder;
    private final ObjectPool<SortPlan> sorts;
    private final ObjList<JoinInput> undecidedSteps = new ObjList<>();
    private boolean isFullFatJoins;
    private boolean isJoinSlaveInput;

    OrderPlanning(CairoConfiguration configuration, OptimiserContext context, FunctionFactoryCache functionFactoryCache,
                  ObjectPool<SortPlan> sorts, IntList readOrder, IntList readIds) {
        this.configuration = configuration;
        this.context = context;
        this.functionFactoryCache = functionFactoryCache;
        this.sorts = sorts;
        this.readOrder = readOrder;
        this.readIds = readIds;
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

    /**
     * True when the node reads the designated timestamp of its input at {@code inputIndex}, the ordered input of a
     * join: the master of a join with a temporal step and the slave of each such step, the master and slaves of a
     * horizon or window join, and the input of a SAMPLE BY over its designated timestamp.
     */
    private static boolean consumesInputTimestamp(LogicalPlan plan, int inputIndex) {
        return switch (plan) {
            case JoinPlan join -> {
                final ObjList<JoinInput> ordered = join.getOrderedInputs();
                if (inputIndex > 0) {
                    yield ordered.getQuick(inputIndex).getJoinType().isTemporal();
                }
                for (int i = 1, n = ordered.size(); i < n; i++) {
                    if (ordered.getQuick(i).getJoinType().isTemporal()) {
                        yield true;
                    }
                }
                yield false;
            }
            case SampleByPlan sample -> sample.isTimestampRequired();
            case HorizonJoinPlan _, WindowJoinPlan _ -> true;
            default -> false;
        };
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

    private static boolean hasEarlyExit(ObjList<FunctionExpression> aggregates) {
        for (int i = 0, n = aggregates.size(); i < n; i++) {
            if ((aggregates.getQuick(i).getFunctionFlags() & BoundExpression.EARLY_EXIT) == 0) {
                return false;
            }
        }
        return true;
    }

    private static boolean hasNestedUnionAll(LogicalPlan plan) {
        return LogicalPlans.skipProjectsAndFilters(plan) instanceof SetOperationPlan operation
                && operation.getOperation() == SetOperationKind.UNION_ALL;
    }

    private static boolean hasNoParallelism(ObjList<? extends BoundExpression> expressions) {
        for (int i = 0, n = expressions.size(); i < n; i++) {
            if ((expressions.getQuick(i).getFunctionFlags() & BoundExpression.NO_PARALLELISM) != 0) {
                return true;
            }
        }
        return false;
    }

    /**
     * True when the fill re-reads the row of a PREV value it cannot keep in a fixed-size slot.
     */
    private static boolean hasPrevReadBack(FillPlan fill) {
        final OutputSchema input = fill.getInput().getOutput();
        final IntList targets = fill.getTargetColumnIds();
        for (int i = 0, n = targets.size(); i < n; i++) {
            final int mode = fill.getModes().getQuick(i);
            if (mode != FillPlan.FILL_PREV && mode != FillPlan.FILL_PREV_COLUMN) {
                continue;
            }
            final int sourceId = mode == FillPlan.FILL_PREV ? targets.getQuick(i) : fill.getSourceColumnIds().getQuick(i);
            if (targets.indexOf(sourceId, 0, n) >= 0
                    && !SampleByFillRecordCursorFactory.isPrevSlotEligible(ColumnType.tagOf(input.getColumnType(input.getColumnIndexById(sourceId))))) {
                return true;
            }
        }
        return false;
    }

    private static boolean hasTimestampAggregate(ObjList<FunctionExpression> aggregates) {
        for (int i = 0, n = aggregates.size(); i < n; i++) {
            if ((aggregates.getQuick(i).getFunctionFlags() & (BoundExpression.ASCENDING_TIMESTAMP | BoundExpression.TIMESTAMP_ARGUMENT)) != 0) {
                return true;
            }
        }
        return false;
    }

    private static boolean isEncodedKey(WindowSpec spec, OutputSchema input) {
        final IntList keys = spec.getOrderByColumnIds();
        for (int i = 0, n = keys.size(); i < n; i++) {
            if (!SortKeyEncoder.isEncodable(ColumnType.tagOf(input.getColumnType(input.getColumnIndexById(keys.getQuick(i)))))) {
                return false;
            }
        }
        return true;
    }

    /**
     * True when every key of the SAMPLE BY is its timestamp or one SYMBOL column and every aggregate is first() or
     * last() of a non-array column, the shape a SAMPLE BY reads from the index of its symbol key.
     */
    private static boolean isFirstLastShape(SampleByPlan sample, int timestampIndex, int baseTimestampIndex, int symbolIndex) {
        final OutputSchema input = sample.getInput().getOutput();
        final ObjList<BoundExpression> keys = sample.getGroupingExpressions();
        for (int i = 0, n = keys.size(); i < n; i++) {
            final ColumnExpression column = (ColumnExpression) keys.getQuick(i);
            final int index = input.getColumnIndexById(column.getColumnId());
            if (index == timestampIndex && index == baseTimestampIndex) {
                continue;
            }
            if (input.getColumnType(index) != ColumnType.SYMBOL || index != symbolIndex || !column.isDirectReference()) {
                return false;
            }
        }
        final ObjList<FunctionExpression> aggregates = sample.getAggregates();
        for (int i = 0, n = aggregates.size(); i < n; i++) {
            final FunctionExpression call = aggregates.getQuick(i);
            if (call.getArgumentCount() != 1 || !(call.argumentAt(0) instanceof ColumnExpression column)
                    || !column.isDirectReference() || ColumnType.isArray(column.getDataType())
                    || !SqlKeywords.isFirstKeyword(call.getName()) && !SqlKeywords.isLastKeyword(call.getName())) {
                return false;
            }
        }
        return true;
    }

    /**
     * True when the generator gates its input once on the predicate, a runtime constant.
     */
    private static boolean isGate(BoundExpression predicate) {
        return !LogicalPlans.isConstant(predicate) && !isFiltering(predicate);
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
     * Whether a capability an algorithm choice reads holds; the plan must carry it.
     */
    private static boolean isKnownYes(PhysicalProperties.Capability capability) {
        if (capability == PhysicalProperties.Capability.UNKNOWN) {
            throw new IllegalStateException("physical capability is unknown at planning");
        }
        return capability == PhysicalProperties.Capability.YES;
    }

    /**
     * True when the generator runs the filter over the residual of a fused scan in parallel, given the parallel filter
     * is enabled: over page frames and within a pattern scan; over a covering index without a backup index scan,
     * unless a LIMIT that may be negative asks a multi-key covering index for the backward frames it cannot serve.
     */
    private static boolean isParallelResidual(ScanPlan scan) {
        return switch (scan.getAccessPath()) {
            case PAGE_FRAMES, SYMBOL_PATTERN -> true;
            case SYMBOL_INDEX -> {
                final BoundExpression limit = scan.getCoveredFilterLimit();
                yield !scan.hasCoveringBackup() && (limit == null || scan.getIndexKeys().size() == 1 || !LogicalPlans.mayBeNegativeLimit(limit));
            }
            default -> false;
        };
    }

    /**
     * True when the generator builds no factory, or a selection, for the projection, which passes its input's
     * filter through.
     */
    private static boolean isPassedThroughProjection(ProjectPlan project) {
        final LogicalPlan input = project.getInput();
        return !LogicalPlans.isComputedProjection(project)
                && !(input instanceof WindowPlan window && LogicalPlans.isWindowOutputProjection(project, window))
                && !(input instanceof WindowJoinPlan && LogicalPlans.isColumnOnlyProjection(project));
    }

    private static boolean isSingleRow(LogicalPlan plan) {
        plan = LogicalPlans.skipProjectsAndFilters(plan);
        return plan instanceof AggregatePlan aggregate && aggregate.getGroupingExpressions().size() == 0;
    }

    /**
     * True when the light cached window under the filter selects the filtered rows itself: the filter reads, at its
     * first position, the BOOLEAN result of the window's sole function, a row-selecting window function SUBSAMPLE
     * binds as its keep flag, which the projection above the filter drops.
     */
    private static boolean isWindowKeepFlag(FilterPlan filter) {
        final LogicalPlan input = filter.getInput();
        final WindowPlan window = LogicalPlans.generatedWindow(input);
        if (!(filter.getPredicate() instanceof ColumnExpression column) || window == null
                || window.getAlgorithm() != WindowPlan.Algorithm.CACHED_LIGHT || window.getFunctions().size() != 1
                || !window.getSpecs().getQuick(0).isSubsampleKeepFlag()) {
            return false;
        }
        final FunctionExpression function = window.getFunctions().getQuick(0);
        final int functionColumnId = window.getFunctionColumnIds().getQuick(0);
        final int functionIndex = input instanceof ProjectPlan project ? LogicalPlans.projectedColumnIndex(project, functionColumnId)
                : input.getOutput().getColumnIndexById(functionColumnId);
        return functionIndex >= 0 && functionIndex == input.getOutput().getColumnIndexById(column.getColumnId())
                && (function.getFunctionFlags() & BoundExpression.ROW_SELECTING) != 0 && function.getDataType() == ColumnType.BOOLEAN;
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

    /**
     * The aggregate a decorrelation domain re-reads through its shared source, as the generator finds it: under the
     * column projections and the leading branches of the set operations of the source's input.
     */
    private static LogicalPlan sharedTarget(LogicalPlan source) {
        while (true) {
            if (source instanceof ProjectPlan project && LogicalPlans.isColumnOnlyProjection(project)) {
                source = project.getInput();
            } else if (source instanceof SetOperationPlan operation) {
                source = operation.getLeft();
            } else {
                return source;
            }
        }
    }

    /**
     * Rejects an aggregate function that requires the designated timestamp of its input as its second argument when
     * the argument is not that column, at {@code timestampIndex} of {@code input}, and one that requires ascending
     * designated timestamp order when the input does not deliver it.
     */
    private static void validateTimestampAggregates(ObjList<FunctionExpression> aggregates, OutputSchema input, int timestampIndex,
                                                    boolean isAscending) throws SqlException {
        for (int i = 0, n = aggregates.size(); i < n; i++) {
            final FunctionExpression call = aggregates.getQuick(i);
            final int flags = call.getFunctionFlags();
            if ((flags & BoundExpression.TIMESTAMP_ARGUMENT) != 0 && (timestampIndex < 0
                    || !(call.argumentAt(1) instanceof ColumnExpression column) || input.getColumnIndexById(column.getColumnId()) != timestampIndex)) {
                throw SqlException.$(call.getPosition(), call.getName()).put("() requires the table's designated timestamp as the second argument");
            }
            if ((flags & BoundExpression.ASCENDING_TIMESTAMP) != 0 && !isAscending) {
                throw SqlException.$(call.getPosition(), call.getName()).put("() requires the base query to provide ascending designated timestamp order");
            }
        }
    }

    /**
     * The direction a validation reads; the plan must carry it.
     */
    private static PhysicalProperties.ScanDirection validatedDirection(PhysicalProperties.ScanDirection direction) {
        if (direction == PhysicalProperties.ScanDirection.UNKNOWN) {
            throw new IllegalStateException("scan direction is unknown at planning");
        }
        return direction;
    }

    /**
     * The GROUP BY the generator builds over the base it reads: the vectorised one for an aggregate of that shape
     * over page frames of a table whose parquet partitions store no column in a converted type, which the vectorised
     * one cannot read, else the parallel one when every key and aggregate function supports parallelism and the base
     * exposes page frames or a filter the GROUP BY steals, unless a keyless aggregate stops early over an unfiltered
     * base; serial otherwise, as is a consumer that re-reads a shared source's cursor.
     */
    private AggregatePlan.Algorithm aggregateAlgorithm(AggregatePlan aggregate, LogicalPlan base) {
        final SqlExecutionContext executionContext = context.getExecutionContext();
        final JoinInput source = aggregate.getSharedSource();
        if (!executionContext.isParallelGroupByEnabled() || source != null
                && PhysicalProperties.supportsSharedCursors(source.getInput()) != PhysicalProperties.Capability.NO) {
            return AggregatePlan.Algorithm.SERIAL;
        }
        final boolean isPageFrameSource = isPageFrameSource(base);
        if (isPageFrameSource && LogicalPlans.hasVectorShape(aggregate) && !hasParquetConvertedColumns(base)) {
            return AggregatePlan.Algorithm.VECTORISED;
        }
        final ObjList<BoundExpression> keys = aggregate.getGroupingExpressions();
        final ObjList<FunctionExpression> aggregates = aggregate.getAggregates();
        final boolean isFilterStolen = !isPageFrameSource && isStolenFilter(base, FilterConsumer.AGGREGATE);
        if (hasNoParallelism(keys) || hasNoParallelism(aggregates) || !isPageFrameSource && !isFilterStolen
                || keys.size() == 0 && hasEarlyExit(aggregates) && !hasFilter(base)) {
            return AggregatePlan.Algorithm.SERIAL;
        }
        return isFilterStolen ? AggregatePlan.Algorithm.PARALLEL_STOLEN_FILTER : AggregatePlan.Algorithm.PARALLEL;
    }

    /**
     * True when the factory the generator builds for the plan carries a filter: the parallel, serial or gating filter
     * over the predicate of a filter node or the residual of a scan, which a selection, a sort or LIMIT the input
     * implements, a single-input join and a null-extending window join pass through.
     */
    private boolean hasFilter(LogicalPlan plan) {
        while (true) {
            switch (plan) {
                case ProjectPlan project when isPassedThroughProjection(project) -> plan = project.getInput();
                case SortPlan sort when sort.getAlgorithm() == SortPlan.Algorithm.INPUT_ORDER
                        || sort.getAlgorithm() == SortPlan.Algorithm.TIMESTAMP_DECLARATION -> plan = sort.getInput();
                case LimitPlan limit when limit.getApplication() == LimitPlan.Application.INPUT ->
                        plan = limit.getInput();
                case JoinPlan join when join.getOrderedInputs().size() == 1 ->
                        plan = join.getOrderedInputs().getQuick(0).getInput();
                case FilterPlan filter when !LogicalPlans.isFusedFilter(filter) -> {
                    final BoundExpression predicate = filter.getPredicate();
                    if (!LogicalPlans.isConstant(predicate)) {
                        return true;
                    }
                    if (predicate instanceof ConstantExpression constant && constant.getLongValue() == 0) {
                        return false;
                    }
                    plan = filter.getInput();
                }
                case FilterPlan filter -> {
                    return hasScanFilter((ScanPlan) filter.getInput());
                }
                case ScanPlan scan -> {
                    return hasScanFilter(scan);
                }
                case WindowJoinPlan windowJoin -> {
                    if (windowJoin.isEmpty()) {
                        return false;
                    }
                    for (int i = 0, n = windowJoin.getSteps().size(); i < n; i++) {
                        if (!(windowJoin.getSteps().getQuick(i).getFilter() instanceof ConstantExpression constant) || constant.getLongValue() != 0) {
                            return false;
                        }
                    }
                    plan = windowJoin.getMaster();
                }
                default -> {
                    return false;
                }
            }
        }
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
     * True when the page frames the plan serves come from a table whose parquet partitions store a column under a
     * converted type.
     */
    private boolean hasParquetConvertedColumns(LogicalPlan plan) {
        while (plan.inputCount() == 1) {
            plan = plan.inputAt(0);
        }
        return plan instanceof ScanPlan scan && context.getTableAccessInfo(scan).hasParquetConvertedColumns();
    }

    private boolean hasScanFilter(ScanPlan scan) {
        final BoundExpression residual = scan.getResidual();
        return switch (scan.getAccessPath()) {
            case PAGE_FRAMES -> residual != null && !LogicalPlans.isConstant(residual);
            case SYMBOL_INDEX -> scan.getIndexRead() == ScanPlan.IndexRead.COVERING && residual != null;
            case SYMBOL_PATTERN -> true;
            case null, default -> false;
        };
    }

    /**
     * The HORIZON JOIN the generator builds: parallel over the page frames of its master, or over the frames under a
     * filter it steals from the master, when every key and aggregate function supports parallelism; serial otherwise.
     */
    private AggregatePlan.Algorithm horizonAlgorithm(AggregatePlan aggregate, LogicalPlan master) {
        if (!context.getExecutionContext().isParallelHorizonJoinEnabled()) {
            return AggregatePlan.Algorithm.SERIAL;
        }
        final boolean isPageFrameSource = isPageFrameSource(master);
        final boolean isFilterStolen = !isPageFrameSource && isStolenFilter(master, FilterConsumer.AGGREGATE);
        if (hasNoParallelism(aggregate.getGroupingExpressions()) || hasNoParallelism(aggregate.getAggregates())
                || !isPageFrameSource && !isFilterStolen) {
            return AggregatePlan.Algorithm.SERIAL;
        }
        return isFilterStolen ? AggregatePlan.Algorithm.PARALLEL_STOLEN_FILTER : AggregatePlan.Algorithm.PARALLEL;
    }

    private boolean isAdviceFollowed(LogicalPlan plan) {
        return PhysicalProperties.followsOrderAdvice(plan) == PhysicalProperties.Capability.YES;
    }

    /**
     * True when the SAMPLE BY reads the first and last values of its buckets from the index of its base: a table scan,
     * under a fused filter or identity projections, of the bitmap index of a single SYMBOL key without a residual, of a table whose parquet partitions store no
     * column under a converted type, which the SAMPLE BY keys by that symbol or by none.
     */
    private boolean isFirstLastIndexScan(SampleByPlan sample, LogicalPlan base) {
        while (base instanceof ProjectPlan project && LogicalPlans.isIdentityProjection(project)) {
            base = project.getInput();
        }
        if (base instanceof FilterPlan filter && LogicalPlans.isFusedFilter(filter)) {
            base = filter.getInput();
        }
        if (sample.getFillMode() != SampleByPlan.FILL_NONE || !(base instanceof ScanPlan scan)
                || scan.getAccessPath() != ScanPlan.AccessPath.SYMBOL_INDEX || scan.getIndexKeys().size() != 1
                || scan.getResidual() != null || scan.getIndexRead() == ScanPlan.IndexRead.COVERING
                || scan.getTableToken().isLiveView() && !scan.isUpdate()) {
            return false;
        }
        final TableAccessInfo table = context.getTableAccessInfo(scan);
        final OutputSchema output = scan.getOutput();
        final int symbolIndex = output.getColumnIndexById(scan.getIndexColumnId());
        if (table.hasParquetConvertedColumns() || !IndexType.isBitmap(table.getIndexType(table.getColumnIndex(output.getColumnName(symbolIndex))))) {
            return false;
        }
        final int baseTimestampIndex = timestampIndex(scan);
        final int timestampIndex = sample.isTimestampRequired() ? baseTimestampIndex
                : sample.getInput().getOutput().getColumnIndexById(sample.getTimestampColumnId());
        return isFirstLastShape(sample, timestampIndex, baseTimestampIndex, symbolIndex);
    }

    /**
     * True when the temporal join step matches on keys: a sole key pairing the designated timestamps of the master
     * and the slave adds no key.
     */
    private boolean isKeyedTemporalJoin(JoinPlan join, int inputIndex, JoinInput step) {
        final IntList masterKeys = step.getMasterKeyColumnIds();
        if (masterKeys.size() != 1) {
            return masterKeys.size() > 0;
        }
        final int masterIndex = join.getOrderedInputs().getQuick(inputIndex - 1).getOutput().getColumnIndexById(masterKeys.getQuick(0));
        final LogicalPlan slave = step.getInput();
        final int slaveIndex = slave.getOutput().getColumnIndexById(step.getSlaveKeyColumnIds().getQuick(0));
        return masterIndex != PhysicalProperties.masterTimestampIndex(join, inputIndex) || slaveIndex != timestampIndex(slave);
    }

    private boolean isPageFrameSource(LogicalPlan plan) {
        return PhysicalProperties.supportsPageFrameCursor(plan) == PhysicalProperties.Capability.YES;
    }


    private boolean isRandomAccess(LogicalPlan plan) {
        return PhysicalProperties.supportsRandomAccess(plan) == PhysicalProperties.Capability.YES;
    }

    /**
     * True when the consumer steals the filter the generator builds for the plan and reads the frames under it: a
     * parallel filter without a LIMIT, within a pattern scan or above it; the serial filter of a table scan, which a
     * top-K and a GROUP BY steal, and of a covering index scan, which a GROUP BY steals; and a gate over page frames,
     * which a temporal join steals when the factory under the filter serves time frames, as it must for any filter.
     */
    private boolean isStolenFilter(LogicalPlan plan, FilterConsumer consumer) {
        final FilterPlan filter = LogicalPlans.stolenFilter(plan);
        if (filter == null) {
            return false;
        }
        final boolean isSerialScanFilterStolen = consumer == FilterConsumer.TOP_K || consumer == FilterConsumer.AGGREGATE;
        final boolean isGateStolen = consumer == FilterConsumer.TEMPORAL_JOIN;
        final boolean isStolen;
        if (!LogicalPlans.isFusedFilter(filter)) {
            final BoundExpression predicate = filter.getPredicate();
            isStolen = isPageFrameSource(filter.getInput())
                    && (isFiltering(predicate) ? filter.getAlgorithm() == FilterPlan.Algorithm.PARALLEL : isGateStolen && isGate(predicate));
        } else {
            final ScanPlan scan = (ScanPlan) filter.getInput();
            final BoundExpression residual = scan.getResidual();
            final boolean isLimited = PhysicalProperties.implementsLimit(filter) == PhysicalProperties.Capability.YES;
            final boolean isCovering = scan.getIndexRead() == ScanPlan.IndexRead.COVERING;
            final boolean isParallel = scan.getResidualAlgorithm() == FilterPlan.Algorithm.PARALLEL;
            isStolen = switch (scan.getAccessPath()) {
                case PAGE_FRAMES -> residual != null && (isFiltering(residual)
                        ? (isParallel || isSerialScanFilterStolen) && !isLimited : isGateStolen && isGate(residual));
                case SYMBOL_INDEX ->
                        isCovering && residual != null && isFiltering(residual) && !scan.hasCoveringBackup()
                                && (isParallel ? !isLimited : consumer == FilterConsumer.AGGREGATE);
                case SYMBOL_PATTERN -> !isCovering ? isParallel : isParallel ? !isLimited : isSerialScanFilterStolen;
                case null, default -> false;
            };
        }
        return isStolen && (consumer != FilterConsumer.TEMPORAL_JOIN
                || PhysicalProperties.supportsLeafTimeFrameCursor(filter) == PhysicalProperties.Capability.YES);
    }

    /**
     * True when an ASOF join steals the filter of its slave, read directly or through the selection the generator
     * builds the slave from.
     */
    private boolean isStolenTemporalFilter(LogicalPlan slave) {
        final FilterPlan filter = LogicalPlans.temporalStolenFilter(slave);
        return filter != null && isStolenFilter(filter, FilterConsumer.TEMPORAL_JOIN);
    }

    /**
     * True when the sort's input emits rows in the order of the sort's first key, the input's designated timestamp.
     */
    private boolean isTimestampOrdered(SortPlan sort, int keyIndex, int timestampIndex) {
        return keyIndex == timestampIndex && PhysicalProperties.scanDirection(sort.getInput())
                == PhysicalProperties.ScanDirection.of(sort.getDirections().getQuick(0));
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
     * Chooses the parallel top-K of a bounded sort, which reads the page frames under its input: it builds at most one
     * projection over itself, see {@link LogicalPlans#parallelTopKProjection}, reads every sort key from the input of
     * that projection, and reads the page frames of that input, or steals its filter. When the projection's input is
     * another projection, the whole input must be a source of page frames.
     */
    private SortPlan.Algorithm parallelTopK(SortPlan sort) {
        final SqlExecutionContext executionContext = context.getExecutionContext();
        if (!executionContext.isParallelTopKEnabled()) {
            return SortPlan.Algorithm.LIMITED;
        }
        final LogicalPlan base = LogicalPlans.generatedPlan(sort.getInput());
        final ProjectPlan projection = LogicalPlans.parallelTopKProjection(sort);
        if (projection == null && LogicalPlans.isPeelableProjection(base)) {
            return isPageFrameSource(base) ? SortPlan.Algorithm.PARALLEL_TOP_K : SortPlan.Algorithm.LIMITED;
        }
        final LogicalPlan source = projection != null ? LogicalPlans.generatedPlan(projection.getInput()) : base;
        final SortPlan.Algorithm algorithm;
        if (isPageFrameSource(source)) {
            algorithm = SortPlan.Algorithm.PARALLEL_TOP_K;
        } else if (isStolenFilter(source, FilterConsumer.TOP_K)) {
            algorithm = SortPlan.Algorithm.PARALLEL_FILTERED_TOP_K;
        } else {
            return SortPlan.Algorithm.LIMITED;
        }
        if (projection == null || !LogicalPlans.isComputedProjection(projection)) {
            return algorithm;
        }
        final OutputSchema input = projection.getInput().getOutput();
        final OutputSchema sorted = sort.getInput().getOutput();
        for (int i = 0, n = sort.getColumnIds().size(); i < n; i++) {
            final int index = sorted.getColumnIndexById(sort.getColumnIds().getQuick(i));
            if (projection.hasUpdateConversions() || !(projection.getExpressions().getQuick(index) instanceof ColumnExpression column)
                    || input.getColumnIndexById(column.getColumnId()) < 0) {
                return SortPlan.Algorithm.LIMITED;
            }
        }
        return algorithm;
    }

    private void planAggregate(AggregatePlan aggregate) throws SqlException {
        if (aggregate.getInput() instanceof HorizonJoinPlan horizon) {
            final LogicalPlan master = horizon.getMaster();
            validateConsumedTimestamp(master);
            for (int i = 0, n = horizon.getSlaves().size(); i < n; i++) {
                validateConsumedTimestamp(horizon.getSlaves().getQuick(i).getInput());
            }
            validateTimestampAggregates(aggregate.getAggregates(), horizon.getOutput(), timestampIndex(master), false);
            for (int i = 0, n = horizon.getSlaves().size(); i < n; i++) {
                final HorizonJoinSlave slave = horizon.getSlaves().getQuick(i);
                validateTimeSeriesOrders(master, slave.getInput(), slave.getPosition());
            }
            aggregate.setAlgorithm(horizonAlgorithm(aggregate, horizon.getMaster()));
            return;
        }
        if (LogicalPlans.isCount(aggregate)
                || LogicalPlans.skipFilters(aggregate.getInput()) instanceof ScanPlan scan && scan.getAccessPath() == ScanPlan.AccessPath.POSTING_DISTINCT) {
            return;
        }
        final LogicalPlan input = LogicalPlans.skipRenames(aggregate.getInput());
        final boolean isTimestampDeclared = LogicalPlans.isTimestampDeclarationOnly(input);
        final LogicalPlan base = isTimestampDeclared ? input.inputAt(0) : input;
        readOrder(base, false, true);
        if (hasTimestampAggregate(aggregate.getAggregates())) {
            final int baseTimestampIndex = timestampIndex(base);
            final int timestampIndex = isTimestampDeclared ? LogicalPlans.projectedTimestampIndex((ProjectPlan) input, baseTimestampIndex) : baseTimestampIndex;
            validateTimestampAggregates(aggregate.getAggregates(), aggregate.getInput().getOutput(), timestampIndex,
                    baseTimestampIndex == timestampIndex && scanDirection(base) == PhysicalProperties.ScanDirection.FORWARD);
        }
        aggregate.setAlgorithm(aggregateAlgorithm(aggregate, base));
    }

    /**
     * Decides how the fill reads the buckets of its input and rejects a fill that would re-read the rows of an input
     * without random access.
     */
    private void planFill(FillPlan fill) {
        final LogicalPlan input = fill.getInput();
        final int timestampIndex = input.getOutput().getColumnIndexById(fill.getTimestampColumnId());
        final FillPlan.Algorithm algorithm;
        if (input instanceof SampleByPlan sample && sample.getAlgorithm() == SampleByPlan.Algorithm.FILL_NONE) {
            algorithm = FillPlan.Algorithm.SAMPLE_BY_ROWS;
        } else if (timestampIndex(input) == timestampIndex) {
            if (!isRandomAccess(input) && hasPrevReadBack(fill)) {
                throw CairoException.critical(0).put("FILL(PREV) cannot re-read rows of a base without random access");
            }
            algorithm = FillPlan.Algorithm.INPUT_ORDER;
        } else {
            algorithm = FillPlan.Algorithm.SORTED;
        }
        fill.setAlgorithm(algorithm);
    }

    /**
     * Decides whether the generator runs a filter in parallel: over the page frames of its input when the parallel
     * filter is enabled, recorded on the filter, or, for a filter fused into the scan under it, as
     * {@link #isParallelResidual} decides, recorded on the scan.
     */
    private void planFilter(FilterPlan filter) {
        final boolean isParallelEnabled = context.getExecutionContext().isParallelFilterEnabled();
        if (!LogicalPlans.isFusedFilter(filter)) {
            filter.setAlgorithm(!isFiltering(filter.getPredicate()) ? null
                    : isWindowKeepFlag(filter) ? FilterPlan.Algorithm.WINDOW_KEEP_FLAG
                      : isParallelEnabled && isKnownYes(PhysicalProperties.supportsPageFrameCursor(filter.getInput())) ? FilterPlan.Algorithm.PARALLEL : FilterPlan.Algorithm.SERIAL);
            return;
        }
        final ScanPlan scan = (ScanPlan) filter.getInput();
        scan.setResidualAlgorithm(!hasResidualFilterChoice(scan) ? null
                : isParallelEnabled && isParallelResidual(scan) ? FilterPlan.Algorithm.PARALLEL : FilterPlan.Algorithm.SERIAL);
    }

    private void planJoin(JoinPlan join) throws SqlException {
        final ObjList<JoinInput> ordered = join.getOrderedInputs();
        for (int i = 0, n = ordered.size(); i < n; i++) {
            final JoinInput step = ordered.getQuick(i);
            if (step.getInput() != null && consumesInputTimestamp(join, i)) {
                validateConsumedTimestamp(step.getInput());
            }
        }
        for (int i = 1, n = ordered.size(); i < n; i++) {
            final JoinInput step = ordered.getQuick(i);
            final LogicalPlan slave = step.getInput();
            switch (step.getJoinType()) {
                case UNNEST -> step.setAlgorithm(JoinInput.Algorithm.UNNEST);
                case ASOF, LT -> {
                    validateTimeSeriesOrders(join, i, slave, step.getPosition());
                    step.setAlgorithm(temporalAlgorithm(join, i, step));
                }
                case SPLICE -> {
                    validateTimeSeriesOrders(join, i, slave, step.getPosition());
                    step.setAlgorithm(isFullFatJoins ? JoinInput.Algorithm.FULL_FAT_SPLICE : JoinInput.Algorithm.SPLICE);
                }
                default -> {
                    final int markoutIndex = ordered.getQuick(i - 1).getOutput().getColumnIndexById(step.getMarkoutTimestampColumnId());
                    if (markoutIndex >= 0 && markoutIndex == PhysicalProperties.masterTimestampIndex(join, i)
                            && PhysicalProperties.masterSupportsRandomAccess(join, i) == PhysicalProperties.Capability.YES
                            && PhysicalProperties.isLongSequence(slave) == PhysicalProperties.Capability.YES) {
                        step.setAlgorithm(JoinInput.Algorithm.MARKOUT);
                    } else if (step.getMasterKeyColumnIds().size() == 0) {
                        step.setAlgorithm(JoinInput.Algorithm.NESTED_LOOP);
                    } else {
                        final boolean isSlaveRandomAccess = step.getJoinType() == JoinKind.LEFT_OUTER && step.getOnResidual() != null
                                && LogicalPlans.constantTruth(step.getOnResidual()) == 0 || isRandomAccess(slave);
                        step.setAlgorithm(isSlaveRandomAccess && !isFullFatJoins ? JoinInput.Algorithm.LIGHT_HASH : JoinInput.Algorithm.HASH);
                        if (step.getMasterSide() == null) {
                            if (PhysicalProperties.masterSupportsRandomAccess(join, i) == PhysicalProperties.Capability.NO) {
                                step.setMasterSide(JoinInput.MasterSide.FIXED);
                            } else {
                                undecidedSteps.add(step);
                            }
                        }
                    }
                }
            }
        }
    }

    private void planLatestBy(LatestByPlan latest) {
        final LogicalPlan input = latest.getInput();
        if ((input instanceof FilterPlan filter ? filter.getInput() : input) instanceof ScanPlan) {
            return;
        }
        final LogicalPlan base = LogicalPlans.isTimestampDeclarationOnly(input) ? input.inputAt(0) : input;
        boolean isAscending = false;
        if (latest.isTimestampOrderInherited() && input.getOutput().getColumnIndexById(latest.getTimestampColumnId())
                == PhysicalProperties.timestampIndex(base)) {
            readOrder(base, false, true);
            isAscending = PhysicalProperties.scanDirection(base) == PhysicalProperties.ScanDirection.FORWARD;
        }
        latest.setAlgorithm(!isRandomAccess(base) ? LatestByPlan.Algorithm.MATERIALIZED
                : isAscending ? LatestByPlan.Algorithm.ASCENDING_LIGHT : LatestByPlan.Algorithm.LIGHT);
    }

    /**
     * Decides which operator applies the LIMIT and, over a sort, how the sort implements it.
     */
    private void planLimit(LimitPlan limit) {
        final LogicalPlan limited = limit.getInput();
        if (!LogicalPlans.hasSortUnderStableProjects(limited)) {
            limit.setApplication(LogicalPlans.hasNativeFilterInput(limited)
                    && PhysicalProperties.implementsLimit(limited) == PhysicalProperties.Capability.YES
                    ? LimitPlan.Application.INPUT : LimitPlan.Application.OPERATOR);
            return;
        }
        final SortPlan sort = (SortPlan) LogicalPlans.skipProjects(limited);
        final LogicalPlan input = sort.getInput();
        final int keyIndex = input.getOutput().getColumnIndexById(sort.getColumnIds().getQuick(0));
        final boolean isTimestampOrdered = isTimestampOrdered(sort, keyIndex, PhysicalProperties.timestampIndex(input));
        final boolean isOrderDelivered = isAdviceFollowed(input) && LogicalPlans.hasAdvisedInput(input)
                || sort.getColumnIds().size() == 1 && isTimestampOrdered;
        if (LogicalPlans.hasNativeFilterInput(input) && PhysicalProperties.implementsLimit(input) == PhysicalProperties.Capability.YES) {
            limit.setApplication(LimitPlan.Application.INPUT);
            sort.setAlgorithm(isOrderDelivered ? SortPlan.Algorithm.INPUT_ORDER : sortAlgorithm(input));
            return;
        }
        if (isOrderDelivered) {
            limit.setApplication(LimitPlan.Application.OPERATOR);
            sort.setAlgorithm(SortPlan.Algorithm.INPUT_ORDER);
            return;
        }
        final BoundExpression lo = limit.getLo();
        final BoundExpression hi = limit.getHi();
        if (!isRandomAccess(input) || lo instanceof ConstantExpression loConstant && hi instanceof ConstantExpression hiConstant
                && LogicalPlans.limitValue(loConstant) >= 0 && LogicalPlans.limitValue(hiConstant) < 0) {
            limit.setApplication(LimitPlan.Application.OPERATOR);
            sort.setAlgorithm(sortAlgorithm(input));
            return;
        }
        limit.setApplication(LimitPlan.Application.SORT);
        final long count = !isTimestampOrdered && hi == null && lo instanceof ConstantExpression loConstant ? LogicalPlans.limitValue(loConstant) : 0;
        if (count <= 0 || count > Integer.MAX_VALUE) {
            sort.setAlgorithm(isTimestampOrdered ? SortPlan.Algorithm.PRESORTED_LIMITED : SortPlan.Algorithm.LIMITED);
        } else if (sort.getColumnIds().size() == 1
                && PhysicalProperties.supportsLongTopK(input, keyIndex) == PhysicalProperties.Capability.YES) {
            sort.setAlgorithm(SortPlan.Algorithm.LONG_TOP_K);
        } else {
            sort.setAlgorithm(parallelTopK(sort));
        }
    }

    private void planSampleBy(SampleByPlan sample) throws SqlException {
        final LogicalPlan sampled = sample.getInput();
        if (sample.isTimestampRequired()) {
            validateConsumedTimestamp(sampled);
        }
        final LogicalPlan base = !sample.isTimestampRequired() && LogicalPlans.isTimestampDeclarationOnly(sampled) ? sampled.inputAt(0) : sampled;
        readOrder(base, false, true);
        final int baseTimestampIndex = timestampIndex(base);
        final int timestampIndex = sample.isTimestampRequired() ? baseTimestampIndex : sampled.getOutput().getColumnIndexById(sample.getTimestampColumnId());
        if (timestampIndex < 0) {
            throw SqlException.$(sample.getPosition(), "base query does not provide designated TIMESTAMP column");
        }
        if (scanDirection(base) != PhysicalProperties.ScanDirection.FORWARD) {
            throw SqlException.$(sample.getPosition(), sample.isJoinInput()
                    ? "ASC order over TIMESTAMP column is required but not provided"
                    : "base query does not provide ASC order over designated TIMESTAMP column");
        }
        validateTimestampAggregates(sample.getAggregates(), sampled.getOutput(), timestampIndex, baseTimestampIndex == timestampIndex);
        sample.setAlgorithm(sampleByAlgorithm(sample, base));
    }

    /**
     * Decides whether a UNION ALL merges its branches in the requested timestamp order, sorting the right branch
     * into it when the branch emits its timestamp in the other direction.
     */
    private void planSetOperation(SetOperationPlan operation) {
        final SortDirection direction = operation.getRequestedOrderDirection();
        final int orderIndex = operation.getOutput().getColumnIndexById(operation.getRequestedOrderColumnId());
        if (operation.getOperation() != SetOperationKind.UNION_ALL || direction == null || orderIndex < 0) {
            operation.setMerge(false, null);
            return;
        }
        final PhysicalProperties.ScanDirection requested = PhysicalProperties.ScanDirection.of(direction);
        final LogicalPlan left = operation.getLeft();
        final LogicalPlan right = operation.getRight();
        final int rightTimestampIndex = PhysicalProperties.timestampIndex(right);
        PhysicalProperties.ScanDirection rightDirection = PhysicalProperties.scanDirection(right);
        SortPlan.Algorithm rightSort = null;
        if (operation.isTimestampOrderPushable(orderIndex) && !(LogicalPlans.skipProjects(right) instanceof SortPlan)
                && rightTimestampIndex == orderIndex) {
            readOrder(right, false, true);
            if (rightDirection != requested) {
                rightSort = sortAlgorithm(right);
                rightDirection = requested;
            }
        }
        boolean isMerged = false;
        if (orderIndex == PhysicalProperties.timestampIndex(left) && orderIndex == rightTimestampIndex
                && left.getOutput().getColumnType(orderIndex) == right.getOutput().getColumnType(orderIndex)) {
            readOrder(left, false, true);
            if (PhysicalProperties.scanDirection(left) == requested) {
                if (rightSort == null) {
                    readOrder(right, false, true);
                }
                isMerged = rightDirection == requested;
            }
        }
        operation.setMerge(isMerged, rightSort);
    }

    /**
     * Decides how the generator implements a sort without a LIMIT over it; the LIMIT over a bounded sort decides again.
     */
    private void planSort(SortPlan sort, boolean isTimestampRequired) {
        final LogicalPlan input = sort.getInput();
        final int keyIndex = input.getOutput().getColumnIndexById(sort.getColumnIds().getQuick(0));
        final int timestampIndex = PhysicalProperties.timestampIndex(input);
        readOrder(input, true, keyIndex == timestampIndex);
        final boolean isAdviceFollowed = isAdviceFollowed(input);
        if (isAdviceFollowed && LogicalPlans.hasAdvisedInput(input)) {
            sort.setAlgorithm(SortPlan.Algorithm.INPUT_ORDER);
        } else if (isAdviceFollowed && sort.isMarkoutHorizon()) {
            final int sortedIndex = sort.getOutput().getTimestampIndex();
            sort.setAlgorithm(sortedIndex < 0 || sortedIndex == timestampIndex || !isTimestampRequired
                    ? SortPlan.Algorithm.INPUT_ORDER : SortPlan.Algorithm.TIMESTAMP_DECLARATION);
        } else if (sort.getColumnIds().size() == 1 && isTimestampOrdered(sort, keyIndex, timestampIndex)) {
            sort.setAlgorithm(SortPlan.Algorithm.INPUT_ORDER);
        } else {
            sort.setAlgorithm(sortAlgorithm(input));
        }
    }

    private void planWindow(WindowPlan window) {
        final LogicalPlan input = window.getInput();
        final SortKeys queryOrder = window.getQueryOrder();
        final boolean isAdviceFollowed = isAdviceFollowed(input);
        final int timestampIndex = PhysicalProperties.timestampIndex(input);
        final PhysicalProperties.ScanDirection direction = PhysicalProperties.scanDirection(input);
        boolean isDirectionRead = false;
        for (int i = 0, n = window.getSpecs().size(); i < n; i++) {
            final WindowSpec spec = window.getSpecs().getQuick(i);
            final IntList order = spec.getOrderByColumnIds();
            final boolean isTimestampKey = order.size() == 1 && queryOrder.size() < 2
                    && input.getOutput().getColumnIndexById(order.getQuick(0)) == timestampIndex;
            final boolean isOrderDelivered = isAdviceFollowed && spec.isQueryOrderPrefix(queryOrder)
                    || isTimestampKey && direction == PhysicalProperties.ScanDirection.of(spec.getOrderByDirections().getQuick(0));
            spec.setOrderDelivered(isOrderDelivered);
            isDirectionRead |= isTimestampKey || isOrderDelivered;
        }
        readOrder(input, true, isDirectionRead);
        window.setAlgorithm(windowAlgorithm(window));
    }

    /**
     * A step joins in parallel when it reads the page frames of the master, or steals the master's parallel filter,
     * its aggregate functions support parallelism and its slave serves time frames. A step whose filter is constant
     * false null-extends the master instead, keeping its page frames for the next step; a step that joins leaves
     * neither page frames nor a filter to the next.
     */
    private void planWindowJoin(WindowJoinPlan windowJoin) throws SqlException {
        final LogicalPlan master = windowJoin.getMaster();
        validateConsumedTimestamp(master);
        for (int i = 0, n = windowJoin.getSteps().size(); i < n; i++) {
            validateConsumedTimestamp(windowJoin.getSteps().getQuick(i).getSlave());
        }
        final SqlExecutionContext executionContext = context.getExecutionContext();
        final int masterTimestampIndex = timestampIndex(master);
        boolean hasPageFrames = isPageFrameSource(master);
        boolean isFilterStolen = !hasPageFrames && isStolenFilter(master, FilterConsumer.WINDOW_JOIN);
        for (int i = 0, n = windowJoin.getSteps().size(); i < n; i++) {
            final WindowJoinStep step = windowJoin.getSteps().getQuick(i);
            validateTimeSeriesOrders(master, step.getSlave(), step.getPosition());
            validateTimestampAggregates(step.getAggregates(), step.getScope(), masterTimestampIndex, false);
            if (step.getFilter() instanceof ConstantExpression constant && constant.getLongValue() == 0) {
                isFilterStolen = false;
                continue;
            }
            final boolean isParallel = executionContext.isParallelWindowJoinEnabled() && (hasPageFrames || isFilterStolen)
                    && !hasNoParallelism(step.getAggregates())
                    && PhysicalProperties.supportsTimeFrameCursor(step.getSlave()) == PhysicalProperties.Capability.YES;
            step.setAlgorithm(!isParallel ? WindowJoinStep.Algorithm.SERIAL
                    : isFilterStolen ? WindowJoinStep.Algorithm.PARALLEL_STOLEN_FILTER : WindowJoinStep.Algorithm.PARALLEL);
            hasPageFrames = false;
            isFilterStolen = false;
        }
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
     * Reads the order of the factory of the first {@code inputCount} ordered inputs of the join, see
     * {@link #readOrder}.
     */
    private void readJoinOrder(JoinPlan join, int inputCount, boolean isAdviceRead, boolean isDirectionRead) {
        if (!isAdviceRead && !isDirectionRead) {
            return;
        }
        final ObjList<JoinInput> ordered = join.getOrderedInputs();
        if (inputCount == 1) {
            readOrder(ordered.getQuick(0).getInput(), isAdviceRead, isDirectionRead);
            return;
        }
        final JoinInput step = ordered.getQuick(inputCount - 1);
        final JoinKind joinType = step.getJoinType();
        final boolean isMasterNullExtended = joinType == JoinKind.RIGHT_OUTER || joinType == JoinKind.FULL_OUTER;
        BoundExpression filter = step.getPostJoinFilter();
        boolean isConstantFolded = joinType == JoinKind.UNNEST || filter instanceof ConstantExpression constant && constant.isLiteral();
        if (!joinType.isTemporal() && joinType != JoinKind.UNNEST && !isMasterNullExtended && joinType != JoinKind.LEFT_OUTER
                && step.getOnResidual() != null) {
            if (filter == null) {
                filter = step.getOnResidual();
                isConstantFolded = filter instanceof ConstantExpression constant && constant.isLiteral();
            } else {
                isConstantFolded = false;
            }
        }
        if (filter != null) {
            if (isConstantFolded && LogicalPlans.isConstant(filter)) {
                if (LogicalPlans.constantTruth(filter) == 0) {
                    return;
                }
            } else {
                isAdviceRead = false;
            }
        }
        switch (step.getAlgorithm()) {
            case MARKOUT -> readJoinOrder(join, inputCount - 1, false, isDirectionRead);
            case NESTED_LOOP -> {
                if (isMasterNullExtended) {
                    readOrder(step.getInput(), isAdviceRead, false);
                    readJoinOrder(join, inputCount - 1, false, isDirectionRead);
                } else {
                    readJoinOrder(join, inputCount - 1, isAdviceRead, isDirectionRead);
                }
            }
            case HASH, LIGHT_HASH -> {
                if (isMasterNullExtended) {
                    return;
                }
                if (step.getMasterSide() == null) {
                    if (isAdviceRead && PhysicalProperties.masterFollowsOrderAdvice(join, inputCount - 1) == PhysicalProperties.Capability.YES
                            || isDirectionRead && PhysicalProperties.masterScanDirection(join, inputCount - 1) != PhysicalProperties.ScanDirection.OTHER) {
                        step.setMasterSide(JoinInput.MasterSide.FIXED);
                    }
                }
                readJoinOrder(join, inputCount - 1, isAdviceRead, isDirectionRead);
            }
            case null -> {
            }
            default -> readJoinOrder(join, inputCount - 1, isAdviceRead, isDirectionRead);
        }
    }

    /**
     * A consumer reads, from the factory of the plan, whether it follows the order advice of its scans and the
     * direction it emits rows in. Fixes the master side of every light INNER hash join the factory hands the question
     * down to whose master's answer depends on the master's order.
     */
    private void readOrder(LogicalPlan plan, boolean isAdviceRead, boolean isDirectionRead) {
        if (!isAdviceRead && !isDirectionRead) {
            return;
        }
        switch (plan) {
            case FilterPlan filter -> {
                if (LogicalPlans.isFusedFilter(filter)) {
                    return;
                }
                if (LogicalPlans.isConstant(filter.getPredicate())) {
                    if (LogicalPlans.constantTruth(filter.getPredicate()) != 0) {
                        readOrder(filter.getInput(), isAdviceRead, isDirectionRead);
                    }
                } else {
                    readOrder(filter.getInput(), false, isDirectionRead);
                }
            }
            case ProjectPlan project -> readOrder(project.getInput(), isAdviceRead, isDirectionRead);
            case SortPlan sort -> {
                if (sort.getAlgorithm() == SortPlan.Algorithm.INPUT_ORDER || sort.getAlgorithm() == SortPlan.Algorithm.TIMESTAMP_DECLARATION) {
                    readOrder(sort.getInput(), isAdviceRead, isDirectionRead);
                }
            }
            case LimitPlan limit -> {
                if (limit.getApplication() != LimitPlan.Application.SORT) {
                    readOrder(limit.getInput(), isAdviceRead && limit.getApplication() == LimitPlan.Application.INPUT, isDirectionRead);
                }
            }
            case DistinctPlan distinct -> {
                final LogicalPlan input = distinct.getInput();
                if (isRandomAccess(input) && PhysicalProperties.timestampIndex(input) >= 0) {
                    readOrder(input, false, isDirectionRead);
                }
            }
            case WindowPlan window -> readOrder(window.getInput(), isAdviceRead, isDirectionRead);
            case WindowJoinPlan windowJoin -> readOrder(windowJoin.getMaster(), isAdviceRead, isDirectionRead);
            case JoinPlan join -> readJoinOrder(join, join.getOrderedInputs().size(), isAdviceRead, isDirectionRead);
            case SetOperationPlan operation -> {
                if (!operation.getOperation().isUnion()) {
                    readOrder(operation.getLeft(), false, isDirectionRead);
                }
            }
            default -> {
            }
        }
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
                final LogicalPlan source = latest.getInput() instanceof FilterPlan filter ? filter.getInput() : latest.getInput();
                if (source instanceof ScanPlan scan) {
                    requireScan(scan, SortDirection.DESCENDING, null, null, true);
                } else {
                    final LogicalPlan input = latest.getInput();
                    requireNothing(LogicalPlans.isTimestampDeclarationOnly(input) ? input.inputAt(0) : input);
                }
            }
            case SampleByPlan sample -> {
                final LogicalPlan sampled = sample.getInput();
                requireNothing(!sample.isTimestampRequired() && LogicalPlans.isTimestampDeclarationOnly(sampled) ? sampled.inputAt(0) : sampled);
            }
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
        require(LogicalPlans.isTimestampDeclarationOnly(input) ? input.inputAt(0) : input, inputOrderColumnId,
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

    /**
     * The SAMPLE BY factory: the interpolating one for FILL(LINEAR), the one that reads first and last values from
     * the index of its base, see {@link #isFirstLastIndexScan}, or the SAMPLE BY cursor, without a fill or filling
     * with constants.
     */
    private SampleByPlan.Algorithm sampleByAlgorithm(SampleByPlan sample, LogicalPlan base) {
        final ObjList<CharSequence> fill = sample.getFillTokens();
        final int fillCount = fill.size();
        if (fillCount == 1 && SqlKeywords.isLinearKeyword(fill.getQuick(0))) {
            return SampleByPlan.Algorithm.INTERPOLATE;
        }
        if (isFirstLastIndexScan(sample, base)) {
            return SampleByPlan.Algorithm.FIRST_LAST_INDEX;
        }
        return fillCount == 0 || fillCount == 1 && SqlKeywords.isNoneKeyword(fill.getQuick(0))
                ? SampleByPlan.Algorithm.FILL_NONE : SampleByPlan.Algorithm.FILL_VALUE;
    }

    private PhysicalProperties.ScanDirection scanDirection(LogicalPlan plan) {
        return validatedDirection(PhysicalProperties.scanDirection(plan));
    }

    private SortPlan.Algorithm sortAlgorithm(LogicalPlan input) {
        return isRandomAccess(input) ? SortPlan.Algorithm.LIGHT : SortPlan.Algorithm.MATERIALIZED;
    }

    /**
     * The ASOF or LT join the generator builds: full-fat over a slave without random access, one that steals the
     * filter of its slave, one that reads the time frames of its slave unless the {@code asof_linear} hint asks for
     * a linear scan or an LT join is keyed, and a linear scan of the slave otherwise.
     */
    private JoinInput.Algorithm temporalAlgorithm(JoinPlan join, int inputIndex, JoinInput step) {
        final LogicalPlan slave = step.getInput();
        if (isFullFatJoins || !isRandomAccess(slave)) {
            return JoinInput.Algorithm.FULL_FAT_TEMPORAL;
        }
        if ((step.getHints() & JoinInput.HINT_ASOF_LINEAR) != 0) {
            return JoinInput.Algorithm.TEMPORAL;
        }
        if (step.getJoinType() == JoinKind.ASOF) {
            if (isStolenTemporalFilter(slave)) {
                return JoinInput.Algorithm.TEMPORAL_STOLEN_FILTER;
            }
        } else if (isKeyedTemporalJoin(join, inputIndex, step)) {
            return JoinInput.Algorithm.TEMPORAL;
        }
        return isKnownYes(PhysicalProperties.supportsTimeFrameCursor(slave)) ? JoinInput.Algorithm.TEMPORAL_TIME_FRAME : JoinInput.Algorithm.TEMPORAL;
    }

    private int timestampIndex(LogicalPlan plan) {
        final int timestampIndex = PhysicalProperties.timestampIndex(plan);
        if (timestampIndex == PhysicalProperties.UNKNOWN_TIMESTAMP) {
            throw new IllegalStateException("designated timestamp is unknown at planning");
        }
        return timestampIndex;
    }

    private void validateConsumedTimestamp(LogicalPlan input) throws SqlException {
        if (!(input instanceof ProjectPlan) || PhysicalProperties.timestampIndex(input) != -1) {
            return;
        }
        LogicalPlan source = input;
        do {
            source = source.inputAt(0);
        } while (source instanceof ProjectPlan);
        if (source instanceof LatestByPlan latest && latest.isTimestampOrderInherited()) {
            throw SqlException.$(latest.getPosition(), "TIMESTAMP column is required but not provided");
        }
    }

    private void validateSlaveOrder(LogicalPlan slave, int position) throws SqlException {
        readOrder(slave, false, true);
        if (scanDirection(slave) != PhysicalProperties.ScanDirection.FORWARD) {
            throw SqlException.$(position, "right side of time series join doesn't have ASC timestamp order");
        }
    }

    /**
     * Rejects a time series join step whose master, the first {@code inputCount} ordered inputs of the join, or
     * whose slave does not emit its rows in ascending designated timestamp order.
     */
    private void validateTimeSeriesOrders(JoinPlan join, int inputCount, LogicalPlan slave, int position) throws SqlException {
        readJoinOrder(join, inputCount, false, true);
        if (validatedDirection(PhysicalProperties.masterScanDirection(join, inputCount)) != PhysicalProperties.ScanDirection.FORWARD) {
            throw SqlException.$(position, "left side of time series join doesn't have ASC timestamp order");
        }
        validateSlaveOrder(slave, position);
    }

    /**
     * Rejects a time series join step whose master or slave does not emit its rows in ascending designated timestamp
     * order.
     */
    private void validateTimeSeriesOrders(LogicalPlan master, LogicalPlan slave, int position) throws SqlException {
        readOrder(master, false, true);
        if (scanDirection(master) != PhysicalProperties.ScanDirection.FORWARD) {
            throw SqlException.$(position, "left side of time series join doesn't have ASC timestamp order");
        }
        validateSlaveOrder(slave, position);
    }

    /**
     * The window factory: the streaming one when every window function evaluates in one pass over rows its input
     * delivers in the window's order; else the light cached one when the configuration enables it and encoded sorts,
     * the input supports random access, every window that orders its own rows sorts by keys that encode and every
     * window result is fixed-width; else the cached one.
     */
    private WindowPlan.Algorithm windowAlgorithm(WindowPlan window) {
        final OutputSchema input = window.getInput().getOutput();
        boolean isStreamed = true;
        boolean isLight = configuration.isSqlWindowCachedLightEnabled() && configuration.isSqlOrderBySortEnabled()
                && isKnownYes(PhysicalProperties.supportsRandomAccess(window.getInput()));
        for (int i = 0, n = window.getSpecs().size(); i < n; i++) {
            final WindowSpec spec = window.getSpecs().getQuick(i);
            final FunctionExpression function = window.getFunctions().getQuick(i);
            final boolean isSorted = spec.getOrderByColumnIds().size() > 0 && !spec.isOrderDelivered();
            isStreamed &= !isSorted && (function.getFunctionFlags() & BoundExpression.MULTI_PASS) == 0;
            isLight &= !ColumnType.isVarSize(function.getDataType()) && (!isSorted || isEncodedKey(spec, input));
        }
        return isStreamed ? WindowPlan.Algorithm.STREAMING : isLight ? WindowPlan.Algorithm.CACHED_LIGHT : WindowPlan.Algorithm.CACHED;
    }

    /**
     * True when the generator builds a filter over the residual of the scan whose execution it chooses: a filter over
     * page frames, a fused live view's filter over its scan, the filter over a covering index and the pattern scan's
     * filter; an index scan applies its residual itself.
     */
    static boolean hasResidualFilterChoice(ScanPlan scan) {
        final BoundExpression residual = scan.getResidual();
        if (residual == null) {
            return false;
        }
        return switch (scan.getAccessPath()) {
            case PAGE_FRAMES, SORTED_SYMBOL_INDEX -> isFiltering(residual);
            case SYMBOL_INDEX -> scan.getIndexRead() == ScanPlan.IndexRead.COVERING;
            case SYMBOL_PATTERN -> true;
            case null, default -> false;
        };
    }

    /**
     * True when the generator builds a filter for the predicate, which it neither folds nor gates once.
     */
    static boolean isFiltering(BoundExpression predicate) {
        return !LogicalPlans.isConstant(predicate)
                && (predicate instanceof ColumnExpression || (predicate.getFunctionFlags() & BoundExpression.RUNTIME_CONSTANT) == 0);
    }

    /**
     * Records on every aggregate of the query level how many decorrelation domains of the level re-read it through
     * their shared source; the generator builds the consumers of each one and the aggregate shares its rows with them.
     */
    void countSharedConsumers(LogicalPlan root) {
        root.walkTopDown(SHARED_CONSUMER_COUNTS);
        root.walkTopDown(SHARED_CONSUMERS);
    }

    /**
     * Decides, once access path planning has planned every input of {@code plan}, how the generator implements the
     * order-sensitive operators of the node: whether a sort runs and how, which operator applies a LIMIT, whether a
     * window orders its rows, whether a UNION ALL merges its branches, how each join step joins and whether a filter
     * runs in parallel. Fixes the master side of every light INNER hash join whose order a decision reads, or whose
     * designated timestamp the consumer of the node requires: {@code isTimestampRequired}. Rejects an input the node consumes the designated timestamp of
     * when the input's factory designates none, see {@link #validateConsumedTimestamp}, and an input that does not
     * deliver the ascending designated timestamp order the node requires.
     */
    void planOperators(LogicalPlan plan, boolean isTimestampRequired) throws SqlException {
        switch (plan) {
            case FilterPlan filter -> planFilter(filter);
            case SortPlan sort -> planSort(sort, isTimestampRequired);
            case LimitPlan limit -> planLimit(limit);
            case WindowPlan window -> planWindow(window);
            case SetOperationPlan operation -> planSetOperation(operation);
            case JoinPlan join -> {
                planJoin(join);
                if (isTimestampRequired || join.hasExplicitTimestamp()) {
                    readJoinOrder(join, join.getOrderedInputs().size(), false, true);
                }
            }
            case WindowJoinPlan windowJoin -> planWindowJoin(windowJoin);
            case AggregatePlan aggregate -> planAggregate(aggregate);
            case SampleByPlan sample -> planSampleBy(sample);
            case FillPlan fill -> planFill(fill);
            case LatestByPlan latest -> planLatestBy(latest);
            case ProjectPlan project when project.getInput() instanceof WindowJoinPlan windowJoin && !LogicalPlans.isColumnOnlyProjection(project)
                    && !LogicalPlans.isWindowJoinTimestampKept(project, windowJoin, readOrder, readIds) ->
                    project.markTimestampDropped();
            default -> {
            }
        }
    }

    /**
     * True when the node requires the designated timestamp of its input at {@code inputIndex}, the ordered input of
     * a join: it consumes it, see {@link #consumesInputTimestamp}, or its own consumer requires its timestamp,
     * {@code isTimestampRequired}, and its factory designates that input's, see
     * {@link PhysicalProperties#timestampSource}. A sort designates its own key, so it requires nothing of its input.
     */
    boolean requiresInputTimestamp(LogicalPlan plan, int inputIndex, boolean isTimestampRequired) {
        return consumesInputTimestamp(plan, inputIndex)
                || isTimestampRequired && PhysicalProperties.timestampSource(plan) == inputIndex;
    }

    void setFullFatJoins(boolean isFullFatJoins) {
        this.isFullFatJoins = isFullFatJoins;
    }

    /**
     * Lets the smaller input drive every light INNER hash join whose order no decision read since
     * {@link #planOperators} planned it.
     */
    void settleMasterSides() {
        for (int i = 0, n = undecidedSteps.size(); i < n; i++) {
            final JoinInput step = undecidedSteps.getQuick(i);
            if (step.getMasterSide() == null) {
                step.setMasterSide(JoinInput.MasterSide.SMALLER);
            }
        }
        undecidedSteps.clear();
    }

    /**
     * The parallel operator that steals the filter of its input.
     */
    private enum FilterConsumer {
        AGGREGATE, TEMPORAL_JOIN, TOP_K, WINDOW_JOIN
    }
}
