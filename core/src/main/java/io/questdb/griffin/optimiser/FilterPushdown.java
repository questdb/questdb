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

import io.questdb.cairo.ColumnType;
import io.questdb.griffin.BoundExpressionRewriter.ConjunctSplit;
import io.questdb.griffin.BoundExpressionRewriter.ConjunctTest;
import io.questdb.griffin.LogicalPlans;
import io.questdb.griffin.OperatorExpression;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlKeywords;
import io.questdb.griffin.SymbolKeyExtractor;
import io.questdb.griffin.plan.logical.AggregatePlan;
import io.questdb.griffin.plan.logical.BindVariableExpression;
import io.questdb.griffin.plan.logical.BoundExpression;
import io.questdb.griffin.plan.logical.ColumnExpression;
import io.questdb.griffin.plan.logical.ComparisonOperator;
import io.questdb.griffin.plan.logical.ConstantExpression;
import io.questdb.griffin.plan.logical.CursorExpression;
import io.questdb.griffin.plan.logical.ExpressionVisitor;
import io.questdb.griffin.plan.logical.FillPlan;
import io.questdb.griffin.plan.logical.FilterPlan;
import io.questdb.griffin.plan.logical.FunctionExpression;
import io.questdb.griffin.plan.logical.HorizonJoinPlan;
import io.questdb.griffin.plan.logical.JoinInput;
import io.questdb.griffin.plan.logical.JoinKey;
import io.questdb.griffin.plan.logical.JoinKind;
import io.questdb.griffin.plan.logical.JoinPlan;
import io.questdb.griffin.plan.logical.LatestByPlan;
import io.questdb.griffin.plan.logical.LimitPlan;
import io.questdb.griffin.plan.logical.LogicalPlan;
import io.questdb.griffin.plan.logical.OutputSchema;
import io.questdb.griffin.plan.logical.PlanRewriter;
import io.questdb.griffin.plan.logical.PlanVisitor;
import io.questdb.griffin.plan.logical.ProjectPlan;
import io.questdb.griffin.plan.logical.ScanPlan;
import io.questdb.griffin.plan.logical.SetOperationPlan;
import io.questdb.griffin.plan.logical.SortDirection;
import io.questdb.griffin.plan.logical.SortPlan;
import io.questdb.griffin.plan.logical.TreeWalk;
import io.questdb.griffin.plan.logical.TypeExpression;
import io.questdb.griffin.plan.logical.UnaryPlan;
import io.questdb.griffin.plan.logical.WindowPlan;
import io.questdb.std.IntIntHashMap;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;
import io.questdb.std.ObjectPool;

/**
 * Pushes join filters into join inputs and filters down the plan, derives transitive filters over
 * equi-join keys and narrows decorrelation domains with their master's filters. A filter of one consumer stays above a
 * decorrelation domain other consumers read, see {@link AggregatePlan#getReaderCount()}.
 */
final class FilterPushdown implements OptimiserPass {
    private static final int OFFSET_LEAVES = 3;
    private static final ConjunctTest ORDER_INDEPENDENT_CONJUNCTS = LogicalPlans::isOrderIndependent;
    private static final int OTHER_LEAVES = 4;
    private static final int PLAIN_LEAVES = 2;
    private static final int RUNTIME_LEAVES = 1;
    private static final int STATIC_LEAVES = 0;
    private final AggregateInputOrder aggregateInputOrder;
    private final IntIntHashMap columnMap;
    private final ConjunctSplit conjunctSplit = new ConjunctSplit();
    private final OptimiserContext context;
    private final ObjectPool<FilterPlan> filters;
    private final ExpressionVisitor identityColumns = this::mapColumnToItself;
    private final IntList joinConjunctTargets;
    private final ConjunctTest keyConjuncts;
    private final ObjectPool<ColumnExpression> narrowingColumns;
    private final ObjList<BoundExpression> nullRejectingCopied = new ObjList<>();
    private final ObjList<BoundExpression> nullRejectingCopies = new ObjList<>();
    private final IntList nullRejectingCopySources = new IntList();
    private final PlanVisitor sharedDomainFilters = plan -> {
        filterSharedDomain(plan);
        return TreeWalk.CONTINUE;
    };
    private final ObjList<BoundExpression> tmpExpressions;
    private final IntList transitiveFactOrigins;
    private final ObjList<BoundExpression> transitiveFacts;
    private final ConjunctTest unhoistedJoinConjuncts = conjunct -> !hoistEarlierJoinConjunct(conjunct);
    private final ConjunctTest unpushedSetConjuncts = conjunct -> !pushSetConjunct(conjunct);
    private ProjectPlan conjunctScope;
    private final ConjunctTest computedProjectionConjuncts = conjunct -> isComputedProjectionPushable(conjunct, conjunctScope);
    private final ConjunctTest projectionConjuncts = conjunct -> canPushThroughProjection(conjunct, conjunctScope);
    private JoinPlan hoistJoin;
    private int hoistPosition;
    private boolean isLatestKeyScope;
    private int joinConjunctIndex;
    private LogicalPlan latestKeyLimit;
    private SortPlan latestKeySort;
    private SetOperationPlan pushedSetOperation;
    private int pushedSetTimestampIndex;
    private int selectedJoinSource;
    private final ConjunctTest selectedJoinConjuncts = this::isSelectedJoinConjunct;
    private final PlanRewriter joinFilters = this::pushJoinFilters;
    private int singleColumnId;
    private final ExpressionVisitor singleColumnReads = expression -> {
        if (expression instanceof ColumnExpression column) {
            if (singleColumnId >= 0 && singleColumnId != column.getColumnId()) {
                singleColumnId = -2;
                return TreeWalk.STOP;
            }
            singleColumnId = column.getColumnId();
        }
        return TreeWalk.CONTINUE;
    };

    FilterPushdown(
            OptimiserContext context,
            AggregateInputOrder aggregateInputOrder,
            ObjList<BoundExpression> tmpExpressions,
            ObjectPool<FilterPlan> filters,
            ObjectPool<ColumnExpression> narrowingColumns,
            IntIntHashMap columnMap,
            IntList joinConjunctTargets,
            IntList transitiveFactOrigins,
            ObjList<BoundExpression> transitiveFacts
    ) {
        this.context = context;
        this.aggregateInputOrder = aggregateInputOrder;
        this.tmpExpressions = tmpExpressions;
        this.filters = filters;
        this.narrowingColumns = narrowingColumns;
        this.columnMap = columnMap;
        this.keyConjuncts = conjunct -> LogicalPlans.readsOnly(conjunct, columnMap);
        this.joinConjunctTargets = joinConjunctTargets;
        this.transitiveFactOrigins = transitiveFactOrigins;
        this.transitiveFacts = transitiveFacts;
    }

    @Override
    public LogicalPlan apply(LogicalPlan plan) throws SqlException {
        plan.rewriteBottomUp(joinFilters);
        // Pushdown and pruning read the ordered-branch marks when they drop an aggregate's input order.
        aggregateInputOrder.collectOrderedBranchAggregates(plan);
        final LogicalPlan pushed = pushDownFilters(plan);
        pushed.walkBottomUp(sharedDomainFilters);
        return pushed;
    }

    @Override
    public void clear() {
        conjunctScope = null;
        conjunctSplit.clear();
        hoistJoin = null;
        isLatestKeyScope = false;
        latestKeyLimit = null;
        latestKeySort = null;
        nullRejectingCopied.clear();
        nullRejectingCopies.clear();
        nullRejectingCopySources.clear();
        pushedSetOperation = null;
    }

    @Override
    public String getName() {
        return "filter placement";
    }

    private static boolean canPushThroughProjection(BoundExpression expression, ProjectPlan project) {
        if (expression instanceof ColumnExpression column) {
            if (ColumnType.isTimestamp(column.getDataType())) {
                final int index = project.getOutput().getColumnIndexById(column.getColumnId());
                assert index >= 0;
                return ((ColumnExpression) project.getExpressions().getQuick(index)).isDirectReference();
            }
        } else if (expression instanceof FunctionExpression call) {
            for (int i = 0, n = call.getArgumentCount(); i < n; i++) {
                if (!canPushThroughProjection(call.argumentAt(i), project)) {
                    return false;
                }
            }
        }
        return true;
    }

    private static int factColumnId(BoundExpression fact) {
        final ColumnExpression column = transitiveFilterColumn(fact);
        assert column != null;
        return column.getColumnId();
    }

    private static boolean hasInputTimestamp(ProjectPlan project) {
        final int timestampId = project.getChild().getOutput().getTimestampColumnId();
        for (int i = 0, n = project.getExpressions().size(); i < n; i++) {
            if (project.getExpressions().getQuick(i) instanceof ColumnExpression column
                    && column.getColumnId() == timestampId) {
                return true;
            }
        }
        return false;
    }

    /**
     * Whether the expression, or any call a constant folds from, has a NULL literal argument.
     */
    private static boolean hasNullLiteral(BoundExpression expression) {
        return !expression.walk(FilterPushdown::nullLiteral);
    }

    private static boolean isColumnConstantComparison(BoundExpression predicate) {
        if (!(predicate instanceof FunctionExpression call) || call.getArgumentCount() != 2
                || !"=".equals(call.getName())) {
            return false;
        }
        final ColumnExpression column = transitiveFilterColumn(predicate);
        if (column == null) {
            return false;
        }
        final BoundExpression value = call.argumentAt(call.argumentAt(0) == column ? 1 : 0);
        return (value.getFunctionFlags() & (BoundExpression.CONSTANT | BoundExpression.RUNTIME_CONSTANT)) != 0;
    }

    private static boolean isColumnValueComparison(BoundExpression predicate) {
        if (!(predicate instanceof FunctionExpression call) || call.getArgumentCount() != 2
                || !"=".equals(call.getName())) {
            return false;
        }
        final ColumnExpression column = transitiveFilterColumn(predicate);
        return column != null && !LogicalPlans.readsColumn(call.argumentAt(call.argumentAt(0) == column ? 1 : 0));
    }

    /**
     * Whether the conjunct reads only plain projected columns and projected timestamp offsets (see
     * {@link #isTimestampOffset}). A conjunct over an offset compares it with constants only, so interval extraction
     * inverts the offset where the column reaches a scan as its designated timestamp.
     */
    private static boolean isComputedProjectionPushable(BoundExpression conjunct, ProjectPlan project) {
        final int leaves = projectedLeaves(conjunct, project);
        return LogicalPlans.isOrderIndependent(conjunct) && leaves != OTHER_LEAVES
                && (leaves != OFFSET_LEAVES || !hasInputTimestamp(project));
    }

    /**
     * Whether the value is spelled from literals, bind variables, operators, casts and runtime-constant
     * functions only. A constant folded from a call counts as that call.
     */
    private static boolean isConstantSpelling(BoundExpression expression) {
        if (expression instanceof ConstantExpression constant) {
            return constant.getSource() == null || isConstantSpelling(constant.getSource());
        }
        if (expression instanceof FunctionExpression call) {
            final String name = call.getName();
            if (!OperatorExpression.getRegistry().isOperator(name) && !"cast".equals(name)
                    && !call.getOverload().getFactory().isRuntimeConstant()) {
                return false;
            }
            for (int i = 0, n = call.getArgumentCount(); i < n; i++) {
                if (!isConstantSpelling(call.argumentAt(i))) {
                    return false;
                }
            }
            return true;
        }
        return expression instanceof BindVariableExpression || expression instanceof TypeExpression;
    }

    private static boolean isLatestKeyParameter(BoundExpression expression, int keyId, ScanPlan scan) {
        if (!(expression instanceof FunctionExpression call) || !"=".equals(call.getName())
                || call.getArgumentCount() != 2 || scan.getIndexedColumnIds().indexOf(keyId, 0, scan.getIndexedColumnIds().size()) >= 0) {
            return false;
        }
        final BoundExpression value = LogicalPlans.isKeyColumn(call.argumentAt(0), keyId) ? call.argumentAt(1)
                : LogicalPlans.isKeyColumn(call.argumentAt(1), keyId) ? call.argumentAt(0) : null;
        return value instanceof BindVariableExpression parameter && parameter.isPredefined()
                && (value.getDataType() == ColumnType.STRING || value.getDataType() == ColumnType.VARCHAR);
    }

    private static boolean isLatestKeyResidual(BoundExpression expression) {
        if (!(expression instanceof FunctionExpression call)) {
            return false;
        }
        if (call.getArgumentCount() == 1 && "not".equals(call.getName())) {
            return isLatestKeyResidual(call.argumentAt(0));
        }
        if (call.getArgumentCount() != 2 || ComparisonOperator.of(call.getName()) == null) {
            return false;
        }
        final BoundExpression left = call.argumentAt(0);
        final BoundExpression right = call.argumentAt(1);
        return left instanceof ColumnExpression l && l.isDirectReference() && right instanceof ConstantExpression rc && rc.isLiteral()
                || right instanceof ColumnExpression r && r.isDirectReference() && left instanceof ConstantExpression lc && lc.isLiteral();
    }

    private static boolean isLatestKeySelector(BoundExpression expression, int keyId) {
        if (!(expression instanceof FunctionExpression call)) {
            return false;
        }
        final String name = call.getName();
        if ("in".equals(name)) {
            if (call.getArgumentCount() < 2 || !LogicalPlans.isKeyColumn(call.argumentAt(0), keyId)) {
                return false;
            }
            for (int i = 1, n = call.getArgumentCount(); i < n; i++) {
                if (!SymbolKeyExtractor.isKeyConstant(call.argumentAt(i))) {
                    return false;
                }
            }
            return true;
        }
        return LogicalPlans.isKeyEqualityOr(call, keyId);
    }

    private static boolean isSetBranchSelectionBarrier(LogicalPlan plan) {
        return switch (plan) {
            case LimitPlan _, LatestByPlan _, WindowPlan _ -> true;
            case FilterPlan _, ProjectPlan _, SortPlan _ -> isSetBranchSelectionBarrier(plan.inputAt(0));
            default -> false;
        };
    }

    private static boolean isSingleKeySelector(BoundExpression selector) {
        final FunctionExpression call = (FunctionExpression) selector;
        final String name = call.getName();
        return "=".equals(name) || "in".equals(name) && call.getArgumentCount() == 2;
    }

    private static boolean isSubsampleKeepFilter(FilterPlan filter) {
        if (filter.getChild() instanceof WindowPlan window && filter.getPredicate() instanceof ColumnExpression column) {
            final IntList ids = window.getFunctionColumnIds();
            final int index = ids.indexOf(column.getColumnId(), 0, ids.size());
            return index >= 0 && window.getSpecs().getQuick(index).isSubsampleKeepFlag();
        }
        return false;
    }

    /**
     * A dateadd with a constant unit and INT stride over a plain input column. Over the input's designated timestamp
     * any unit qualifies; over another column only a fixed-duration unit does.
     */
    private static boolean isTimestampOffset(BoundExpression expression, OutputSchema input) {
        return expression instanceof FunctionExpression call && call.getArgumentCount() == 3
                && SqlKeywords.isDateaddKeyword(call.getName())
                && call.argumentAt(0) instanceof ConstantExpression
                && call.argumentAt(1) instanceof ConstantExpression stride && stride.isLiteral()
                && stride.getDataType() == ColumnType.INT
                && call.argumentAt(2) instanceof ColumnExpression column && column.isDirectReference()
                && (column.getColumnId() == input.getTimestampColumnId() || LogicalPlans.isFixedDurationDateaddUnit(call.argumentAt(0)));
    }

    /**
     * A conjunct that pins a direct column reference: column = constant or column ~ pattern, free of NULL literals.
     */
    private static boolean isTransitiveFact(BoundExpression predicate) {
        final ColumnExpression column = transitiveFilterColumn(predicate);
        if (column == null) {
            return false;
        }
        final FunctionExpression call = (FunctionExpression) predicate;
        final BoundExpression value = call.argumentAt(call.argumentAt(0) == column ? 1 : 0);
        return !hasNullLiteral(value) && switch (call.getName()) {
            case "=" -> isConstantSpelling(value);
            case "~" -> !LogicalPlans.readsColumn(value);
            default -> false;
        };
    }

    // Bind variables report non-determinism for plan caching, yet hold one value per execution.
    private static boolean isUnstable(BoundExpression predicate) {
        return !LogicalPlans.isStableWithinExecution(predicate);
    }

    /**
     * The join input the conjunct can move to, or -1 when it stays above the join.
     */
    private static int joinConjunctTarget(BoundExpression conjunct, JoinPlan join, int lastInput) {
        final int source = joinSourceOrdinal(conjunct, join);
        return source >= 0 && (LogicalPlans.isOrderIndependent(conjunct) || isColumnValueComparison(conjunct))
                && LogicalPlans.canPushJoinFilter(join, source, lastInput) ? source : -1;
    }

    private static int joinSourceOrdinal(BoundExpression expression, JoinPlan join) {
        if (expression instanceof ColumnExpression column) {
            return LogicalPlans.joinColumnSource(join, column.getColumnId());
        }
        int source = -1;
        if (expression instanceof FunctionExpression function) {
            for (int i = 0, n = function.getArgumentCount(); i < n; i++) {
                final int childSource = joinSourceOrdinal(function.argumentAt(i), join);
                if (childSource == -2 || source >= 0 && childSource >= 0 && childSource != source) {
                    return -2;
                }
                if (childSource >= 0) {
                    source = childSource;
                }
            }
        }
        return source;
    }

    private static int latestJoinPosition(BoundExpression expression, JoinPlan join) {
        if (expression instanceof ColumnExpression column) {
            final int source = LogicalPlans.findJoinColumnSource(join, column.getColumnId());
            return source < 0 ? Integer.MAX_VALUE : join.getOrderedInputs().indexOf(join.getInputs().getQuick(source));
        }
        int position = -1;
        if (expression instanceof FunctionExpression function) {
            for (int i = 0, n = function.getArgumentCount(); i < n; i++) {
                position = Math.max(position, latestJoinPosition(function.argumentAt(i), join));
            }
        } else if (!(expression instanceof ConstantExpression)) {
            return Integer.MAX_VALUE;
        }
        return position;
    }

    private static int nullLiteral(BoundExpression expression) {
        if (expression instanceof ConstantExpression constant) {
            return constant.getDataType() == ColumnType.NULL || constant.getSource() != null && hasNullLiteral(constant.getSource())
                    ? TreeWalk.STOP : TreeWalk.SKIP_CHILDREN;
        }
        return TreeWalk.CONTINUE;
    }

    private static int projectedLeaves(BoundExpression expression, ProjectPlan project) {
        if (expression instanceof ColumnExpression column) {
            final int index = project.getOutput().getColumnIndexById(column.getColumnId());
            final BoundExpression projected = project.getExpressions().getQuick(index);
            if (projected instanceof ColumnExpression reference) {
                return project.getChild().getOutput().getColumnIndexById(reference.getColumnId()) >= 0
                        && canPushThroughProjection(column, project) ? PLAIN_LEAVES : OTHER_LEAVES;
            }
            return isTimestampOffset(projected, project.getChild().getOutput()) ? OFFSET_LEAVES : OTHER_LEAVES;
        }
        if (expression instanceof FunctionExpression call) {
            final int flags = call.getFunctionFlags();
            if ((flags & BoundExpression.CONSTANT) != 0) {
                return STATIC_LEAVES;
            }
            if ((flags & BoundExpression.RUNTIME_CONSTANT) != 0 || call.getArgumentCount() == 0) {
                return RUNTIME_LEAVES;
            }
            int leaves = STATIC_LEAVES;
            boolean hasRuntimeArgument = false;
            for (int i = 0, n = call.getArgumentCount(); i < n; i++) {
                final int argument = projectedLeaves(call.argumentAt(i), project);
                if (argument == OTHER_LEAVES) {
                    return OTHER_LEAVES;
                }
                hasRuntimeArgument |= argument == RUNTIME_LEAVES;
                leaves = Math.max(leaves, argument);
            }
            return leaves == OFFSET_LEAVES && hasRuntimeArgument ? OTHER_LEAVES : leaves;
        }
        if (expression instanceof ConstantExpression || expression instanceof TypeExpression) {
            return STATIC_LEAVES;
        }
        return expression instanceof CursorExpression && !LogicalPlans.isOrderIndependent(expression) ? OTHER_LEAVES : RUNTIME_LEAVES;
    }

    private static int sourceColumnId(LogicalPlan plan, int columnId, LogicalPlan target) {
        while (plan != target) {
            switch (plan) {
                case ProjectPlan project -> {
                    final ColumnExpression column = LogicalPlans.passedColumn(project, columnId);
                    if (column == null || !column.isDirectReference()) {
                        return -1;
                    }
                    columnId = column.getColumnId();
                }
                case FilterPlan _, SortPlan _, LimitPlan _ -> {
                }
                default -> {
                    return -1;
                }
            }
            plan = plan.inputAt(0);
        }
        return columnId;
    }

    /**
     * Puts {@code input} in the place of the filter once no residual of its predicate remains, else leaves the residual
     * filtering {@code input}; returns {@code result}, the root of the chain the filter heads, unless {@code input}
     * takes the filter's place at the root.
     */
    private static LogicalPlan spliceResidual(LogicalPlan result, UnaryPlan parent, FilterPlan filter, LogicalPlan input, BoundExpression residual) {
        if (residual == null) {
            if (parent == null) {
                return input;
            }
            parent.replaceInput(0, input);
        } else if (residual != filter.getPredicate()) {
            filter.of(input, residual, filter.getPosition());
        }
        return result;
    }

    private static ColumnExpression transitiveFilterColumn(BoundExpression predicate) {
        if (predicate instanceof FunctionExpression call && call.getArgumentCount() == 2) {
            if (call.argumentAt(0) instanceof ColumnExpression column && column.isDirectReference()) {
                return column;
            }
            if ("=".equals(call.getName())
                    && call.argumentAt(1) instanceof ColumnExpression column && column.isDirectReference()) {
                return column;
            }
        }
        return null;
    }

    private static void unqualifyFilteredKeys(JoinInput step, BoundExpression filter) {
        if (filter instanceof ColumnExpression column) {
            final ObjList<JoinKey> keys = step.getKeys();
            for (int i = 0, n = keys.size(); i < n; i++) {
                if (keys.getQuick(i).getSlaveColumnId() == column.getColumnId()) {
                    keys.getQuick(i).unqualifySlaveName();
                }
            }
        } else if (filter instanceof FunctionExpression function) {
            for (int i = 0, n = function.getArgumentCount(); i < n; i++) {
                unqualifyFilteredKeys(step, function.argumentAt(i));
            }
        }
    }

    /**
     * Collects a copy of a conjunct that stays above the join because an outer step NULL-extends the one input it reads,
     * for that input to filter its rows too: the conjunct drops the rows the outer steps NULL-extend, see
     * {@link OptimiserContext#isNullRejecting}, so the rows the copy drops lose only the joined rows the conjunct drops.
     * A conjunct the pass copied once stays above the join, where the pass visits it again, and gets no second copy.
     */
    private void collectNullRejectingCopy(BoundExpression conjunct, JoinPlan join, int lastInput) {
        final int source = joinSourceOrdinal(conjunct, join);
        if (source >= 0 && nullRejectingCopied.indexOf(conjunct) < 0 && LogicalPlans.isOrderIndependent(conjunct)
                && LogicalPlans.canPushNullRejectingJoinFilter(join, source, lastInput) && context.isNullRejecting(conjunct)) {
            columnMap.clear();
            conjunct.walk(identityColumns);
            nullRejectingCopied.add(conjunct);
            nullRejectingCopies.add(context.getRewriter().copyRemappedColumns(conjunct, columnMap));
            nullRejectingCopySources.add(source);
        }
    }

    /**
     * Collects one transitive fact per column from the join's WHERE and INNER ON conjuncts. A later
     * conjunct replaces an earlier fact on the same column, except that an ON conjunct never replaces
     * a WHERE fact.
     */
    private void collectTransitiveFacts(JoinPlan join, boolean isInnerRegion) {
        transitiveFacts.clear();
        transitiveFactOrigins.clear();
        final ObjList<BoundExpression> conjuncts = join.getFilterConjuncts();
        final IntList origins = join.getFilterConjunctOrigins();
        for (int i = 0, n = conjuncts.size(); i < n; i++) {
            final BoundExpression conjunct = conjuncts.getQuick(i);
            final int origin = origins.getQuick(i);
            if (!isInnerRegion && origin >= 0 && join.getInputs().getQuick(origin).getJoinType().isBarrier()) {
                continue;
            }
            final ColumnExpression column = transitiveFilterColumn(conjunct);
            if (column == null || !isTransitiveFact(conjunct)) {
                continue;
            }
            final int columnId = column.getColumnId();
            int index = transitiveFacts.size() - 1;
            while (index >= 0 && factColumnId(transitiveFacts.getQuick(index)) != columnId) {
                index--;
            }
            if (index < 0) {
                transitiveFacts.add(conjunct);
                transitiveFactOrigins.add(origin);
            } else if (origin < 0 || transitiveFactOrigins.getQuick(index) >= 0) {
                transitiveFacts.setQuick(index, conjunct);
                transitiveFactOrigins.setQuick(index, origin);
            }
        }
    }

    private void deriveTransitiveFilter(BoundExpression predicate, int masterId, JoinInput slave, int keyIndex) throws SqlException {
        final int slaveId = slave.getKeys().getQuick(keyIndex).getSlaveColumnId();
        final ColumnExpression column = transitiveFilterColumn(predicate);
        final LogicalPlan target = slave.getInput();
        if (column == null || target == null || column.getColumnId() != masterId) {
            return;
        }
        final int index = target.getOutput().getColumnIndexById(slaveId);
        if (index < 0) {
            return;
        }
        final int slaveType = target.getOutput().getColumnType(index);
        if (slaveType != column.getDataType() && !LogicalPlans.isJoinKeyTypeCompatible(column.getDataType(), slaveType)) {
            return;
        }
        columnMap.clear();
        columnMap.put(masterId, slaveId);
        FunctionExpression derived = (FunctionExpression) context.getRewriter().copyRemappedColumns(predicate, columnMap);
        if (((FunctionExpression) predicate).argumentAt(1) == column) {
            derived = context.getRewriter().commuteEquality(derived);
        }
        final BoundExpression slaveFilter = slaveType == column.getDataType() ? derived : context.bindCall(derived.getName(),
                derived.getPosition(), narrowingColumns.next().of(slaveId, slaveType, column.getPosition()), derived.argumentAt(1),
                target.getOutput());
        if (pushSourceJoinFilter(slave, slaveFilter) && slave.isSubquery()) {
            slave.getKeys().getQuick(keyIndex).unqualifySlaveName();
        }
    }

    private void deriveTransitiveFilters(JoinPlan join, BoundExpression pushedPredicate) throws SqlException {
        final boolean isInnerRegion = !LogicalPlans.hasBarrierInput(join);
        if (pushedPredicate == null) {
            collectTransitiveFacts(join, isInnerRegion);
        }
        // Only original facts are consulted; a derived slave filter cannot pin another key.
        for (int i = 0, n = join.getInputs().size(); i < n; i++) {
            final JoinInput slave = join.getInputs().getQuick(i);
            if (slave.getJoinType() == JoinKind.SPLICE) {
                continue;
            }
            for (int k = 0, count = slave.getKeys().size(); k < count; k++) {
                final int masterId = slave.getKeys().getQuick(k).getMasterColumnId();
                if (pushedPredicate != null) {
                    deriveTransitiveFilter(pushedPredicate, masterId, slave, k);
                } else {
                    for (int f = 0, factCount = transitiveFacts.size(); f < factCount; f++) {
                        final BoundExpression fact = transitiveFacts.getQuick(f);
                        final ColumnExpression column = transitiveFilterColumn(fact);
                        assert column != null;
                        if (column.getColumnId() == masterId) {
                            final int origin = transitiveFactOrigins.getQuick(f);
                            if (isInnerRegion || (origin < 0
                                    ? isTransitiveSourceSafe(join, joinSourceOrdinal(column, join), fact)
                                    : isOnFactSafe(join, masterId, slave, origin, (FunctionExpression) fact))) {
                                deriveTransitiveFilter(fact, masterId, slave, k);
                            }
                            break;
                        }
                    }
                }
            }
        }
    }

    private void deriveTransitiveFiltersFromPushedPredicate(JoinPlan join, BoundExpression predicate) throws SqlException {
        if (!(predicate instanceof FunctionExpression call) || call.getArgumentCount() != 2) {
            return;
        }
        if (call.isAnd()) {
            deriveTransitiveFiltersFromPushedPredicate(join, call.argumentAt(0));
            deriveTransitiveFiltersFromPushedPredicate(join, call.argumentAt(1));
        } else if ("=".equals(call.getName()) && isColumnConstantComparison(call)) {
            final int source = joinSourceOrdinal(call, join);
            if (source >= 0 && isTransitiveSourceSafe(join, source, predicate)) {
                deriveTransitiveFilters(join, predicate);
            }
        }
    }

    /**
     * A decorrelation domain re-reads a master input; the master's own filters also narrow the
     * domain to the keys the join can match.
     */
    private void filterSharedDomain(LogicalPlan plan) {
        if (!(plan instanceof AggregatePlan aggregate) || aggregate.getSharedSource() == null
                || !(aggregate.getChild() instanceof ScanPlan scan)) {
            return;
        }
        final LogicalPlan source = LogicalPlans.skipFilters(aggregate.getSharedSource().getInput());
        if (source == aggregate.getSharedSource().getInput() || !(source instanceof ScanPlan sourceScan)
                || !sourceScan.getTableToken().equals(scan.getTableToken())) {
            return;
        }
        columnMap.clear();
        final IntList inputIds = aggregate.getSharedInputIds();
        final IntList sourceIds = aggregate.getSharedSourceIds();
        for (int i = 0, n = inputIds.size(); i < n; i++) {
            final int keyIndex = columnMap.keyIndex(sourceIds.getQuick(i));
            if (keyIndex > -1 && scan.getOutput().getColumnIndexById(inputIds.getQuick(i)) >= 0) {
                columnMap.putAt(keyIndex, sourceIds.getQuick(i), inputIds.getQuick(i));
            }
        }
        LogicalPlan input = scan;
        for (LogicalPlan filter = aggregate.getSharedSource().getInput(); filter != source; filter = filter.inputAt(0)) {
            final BoundExpression predicate = ((FilterPlan) filter).getPredicate();
            if (LogicalPlans.isStableWithinExecution(predicate) && LogicalPlans.readsOnly(predicate, columnMap)) {
                input = filters.next().of(input, context.getRewriter().copyRemappedColumns(predicate, columnMap), predicate.getPosition());
            }
        }
        aggregate.replaceInput(0, input);
    }

    /**
     * Moves the conjunct, which the step at {@link #hoistPosition} of {@link #hoistJoin} filters with, to the LEFT step
     * before it that joins the last input the conjunct reads, when only INNER, CROSS and LEFT steps lie between them.
     * Returns whether the conjunct moved.
     */
    private boolean hoistEarlierJoinConjunct(BoundExpression conjunct) throws SqlException {
        if (!LogicalPlans.isOrderIndependent(conjunct)) {
            return false;
        }
        final ObjList<JoinInput> ordered = hoistJoin.getOrderedInputs();
        final int target = latestJoinPosition(conjunct, hoistJoin);
        if (target < 1 || target >= hoistPosition || ordered.getQuick(target).getJoinType() != JoinKind.LEFT_OUTER) {
            return false;
        }
        for (int i = target + 1; i < hoistPosition; i++) {
            switch (ordered.getQuick(i).getJoinType()) {
                case INNER, CROSS, LEFT_OUTER -> {
                }
                default -> {
                    return false;
                }
            }
        }
        final JoinInput step = ordered.getQuick(target);
        step.setPostJoinFilter(context.getRewriter().combineConjunction(step.getPostJoinFilter(), conjunct, conjunct.getPosition()));
        return true;
    }

    /**
     * Hoists each conjunct of the predicate the step at {@code position} filters with that can move, see
     * {@link #hoistEarlierJoinConjunct}, and returns the conjuncts that stay.
     */
    private BoundExpression hoistEarlierJoinConjuncts(JoinPlan join, BoundExpression predicate, int position) throws SqlException {
        hoistJoin = join;
        hoistPosition = position;
        return context.getRewriter().retainConjuncts(predicate, unhoistedJoinConjuncts);
    }

    /**
     * An INNER ON fact holds for every output row only up to the first master-nulling join after its
     * column's source; rows that join NULL-extends satisfy it only when the fact rejects NULL.
     */
    private boolean isOnFactSafe(JoinPlan join, int columnId, JoinInput target, int origin, FunctionExpression fact) {
        final ObjList<JoinInput> ordered = join.getOrderedInputs();
        final int source = LogicalPlans.findJoinColumnSource(join, columnId);
        final int sourcePosition = source < 0 ? -1 : ordered.indexOf(join.getInputs().getQuick(source));
        int boundaryPosition = -1;
        for (int i = sourcePosition + 1, n = ordered.size(); i < n && boundaryPosition < 0; i++) {
            if (ordered.getQuick(i).getJoinType().isMasterNulling()) {
                boundaryPosition = i;
            }
        }
        if (sourcePosition < 0 || boundaryPosition < 0) {
            return true;
        }
        return ordered.indexOf(target) < boundaryPosition && (!"=".equals(fact.getName()) || context.isNullRejecting(fact)
                || ordered.indexOf(join.getInputs().getQuick(origin)) < boundaryPosition);
    }

    private boolean isSelectedJoinConjunct(BoundExpression conjunct) {
        return joinConjunctTargets.getQuick(joinConjunctIndex++) == selectedJoinSource;
    }

    private boolean isTransitiveSourceSafe(JoinPlan join, int source, BoundExpression fact) {
        final int last = join.getOrderedInputs().size() - 1;
        if (!LogicalPlans.hasBarrierInput(join)) {
            return LogicalPlans.canPushJoinFilter(join, source, last);
        }
        final JoinInput input = join.getInputs().getQuick(source);
        if (join.getOrderedInputs().indexOf(input) > 0 && input.getJoinType().isBarrier()) {
            return false;
        }
        if (LogicalPlans.lastMasterNullingStep(join) < 0) {
            return LogicalPlans.canPushJoinFilter(join, source, last);
        }
        final FunctionExpression call = (FunctionExpression) fact;
        if ("=".equals(call.getName())) {
            return context.isNullRejecting(call);
        }
        return call.argumentAt(1) instanceof ConstantExpression constant && constant.isLiteral()
                && ColumnType.tagOf(constant.getDataType()) != ColumnType.NULL;
    }

    private int mapColumnToItself(BoundExpression expression) {
        if (expression instanceof ColumnExpression column) {
            columnMap.put(column.getColumnId(), column.getColumnId());
        }
        return TreeWalk.CONTINUE;
    }

    /**
     * Returns the first conjunct that selects values of the single SYMBOL partition key with
     * literals. Filtering the key before LATEST BY keeps the latest row of every selected key. The
     * rule applies only on the root's column-projection chain over a plain table scan, and only when
     * every other conjunct compares a column with a constant; under LIMIT without ORDER BY on the
     * key, the selector must name a single key because the keyed cursor emits rows in key order.
     */
    private BoundExpression latestKeySelector(LatestByPlan latest, FilterPlan filter, LogicalPlan crossed) {
        final BoundExpression predicate = filter.getPredicate();
        if (!isLatestKeyScope || latest.getKeyColumnIds().size() != 1 || !(latest.getChild() instanceof ScanPlan scan)) {
            return null;
        }
        final int keyId = latest.getKeyColumnIds().getQuick(0);
        final OutputSchema output = latest.getOutput();
        final int keyIndex = output.getColumnIndexById(keyId);
        if (keyIndex < 0 || !ColumnType.isSymbol(output.getColumnType(keyIndex))) {
            return null;
        }
        tmpExpressions.clear();
        LogicalPlans.collectConjuncts(predicate, tmpExpressions);
        BoundExpression selector = null;
        for (int i = 0, n = tmpExpressions.size(); i < n; i++) {
            final BoundExpression conjunct = tmpExpressions.getQuick(i);
            if (isLatestKeySelector(conjunct, keyId) || isLatestKeyParameter(conjunct, keyId, scan)) {
                if (selector == null) {
                    selector = conjunct;
                }
            } else if (!isLatestKeyResidual(conjunct)) {
                selector = null;
                break;
            }
        }
        tmpExpressions.clear();
        if (selector == null) {
            return null;
        }
        boolean hasKeyOrder = false;
        if (latestKeySort != null) {
            final IntList sortIds = latestKeySort.getColumnIds();
            for (int i = 0, n = sortIds.size(); i < n; i++) {
                final int id = sourceColumnId(latestKeySort.getChild() == filter ? crossed : latestKeySort.getChild(),
                        sortIds.getQuick(i), latest);
                if (id < 0) {
                    return null;
                }
                hasKeyOrder |= id == keyId;
            }
        }
        if (latestKeyLimit != null && !hasKeyOrder && !isSingleKeySelector(selector)) {
            return null;
        }
        return selector;
    }

    /**
     * Maps each output column of the aggregate that a grouping key passes through to the input column it reads;
     * returns whether one does.
     */
    private boolean mapGroupingKeys(AggregatePlan aggregate) {
        columnMap.clear();
        final ObjList<BoundExpression> grouping = aggregate.getGroupingExpressions();
        for (int i = 0, n = grouping.size(); i < n; i++) {
            if (grouping.getQuick(i) instanceof ColumnExpression column && column.isDirectReference()) {
                columnMap.put(aggregate.getOutput().getColumnId(i), column.getColumnId());
            }
        }
        return columnMap.size() > 0;
    }

    private LogicalPlan pushDownFilter(FilterPlan filter) throws SqlException {
        LogicalPlan result = filter;
        UnaryPlan parent = null;
        ProjectPlan crossedProject = null;
        boolean isOrderCrossed = false;
        while (true) {
            final LogicalPlan input = filter.getChild();
            final UnaryPlan crossed;
            final BoundExpression predicate;
            if (input instanceof ProjectPlan project && !LogicalPlans.isColumnProjection(project)) {
                conjunctScope = project;
                context.getRewriter().splitConjuncts(filter.getPredicate(), computedProjectionConjuncts, conjunctSplit);
                final BoundExpression movable = conjunctSplit.getMatched();
                if (movable == null) {
                    return result;
                }
                final BoundExpression residual = conjunctSplit.getRemainder();
                final BoundExpression substituted = context.getRewriter().substituteProjection(movable, project);
                final FilterPlan pushed = filters.next().of(project.getChild(), substituted, substituted.getPosition());
                project.replaceInput(0, pushDownScopedFilter(pushed));
                return spliceResidual(result, parent, filter, project, residual);
            }
            if (input instanceof ProjectPlan project) {
                // A projected timestamp CAST keeps scalar precision in its consumer's scope.
                if (!canPushThroughProjection(filter.getPredicate(), project)) {
                    if (!LogicalPlans.isOrderIndependent(filter.getPredicate())) {
                        return result;
                    }
                    conjunctScope = project;
                    context.getRewriter().splitConjuncts(filter.getPredicate(), projectionConjuncts, conjunctSplit);
                    final BoundExpression movable = conjunctSplit.getMatched();
                    if (movable == null) {
                        return result;
                    }
                    final BoundExpression residual = conjunctSplit.getRemainder();
                    final BoundExpression remapped = context.getRewriter().remapColumns(movable, project);
                    final FilterPlan pushed = filters.next().of(project.getChild(), remapped, remapped.getPosition());
                    project.replaceInput(0, pushDownFilter(pushed));
                    return spliceResidual(result, parent, filter, project, residual);
                }
                crossed = project;
                crossedProject = project;
                // Remapping completes before edges change. The binder remains the sole
                // owner of any prepared closure; the rule owns descriptions only.
                predicate = context.getRewriter().remapColumns(filter.getPredicate(), project);
            } else if (input instanceof FilterPlan inner) {
                if (isSubsampleKeepFilter(inner) || isUnstable(inner.getPredicate()) || isUnstable(filter.getPredicate())) {
                    return result;
                }
                // The inner predicate must reject a row before the outer predicate evaluates it.
                tmpExpressions.clear();
                LogicalPlans.collectConjuncts(filter.getPredicate(), tmpExpressions);
                BoundExpression combined = inner.getPredicate();
                for (int i = 0, n = tmpExpressions.size(); i < n; i++) {
                    combined = context.getRewriter().combineConjunction(combined, tmpExpressions.getQuick(i), filter.getPosition());
                }
                tmpExpressions.clear();
                filter.of(inner.getChild(), combined, filter.getPosition());
                filter.deriveOutput();
                continue;
            } else if (input instanceof SortPlan sort) {
                if (!LogicalPlans.isOrderIndependent(filter.getPredicate())) {
                    context.getRewriter().splitConjuncts(filter.getPredicate(), ORDER_INDEPENDENT_CONJUNCTS, conjunctSplit);
                    final BoundExpression movable = conjunctSplit.getMatched();
                    if (movable == null) {
                        return result;
                    }
                    final BoundExpression residual = conjunctSplit.getRemainder();
                    final FilterPlan pushed = filters.next().of(sort.getChild(), movable, movable.getPosition());
                    sort.replaceInput(0, pushDownFilter(pushed));
                    return spliceResidual(result, parent, filter, sort, residual);
                }
                crossed = sort;
                predicate = filter.getPredicate();
                isOrderCrossed = true;
            } else if (input instanceof JoinPlan join) {
                final BoundExpression residual = pushSingleSourceJoinFilter(join, filter.getPredicate(), join.getOrderedInputs().size() - 1);
                deriveTransitiveFiltersFromPushedPredicate(join, filter.getPredicate());
                return spliceResidual(result, parent, filter, join, residual);
            } else if (input instanceof SetOperationPlan operation) {
                final int timestampIndex = LogicalPlans.setTimestampIndex(operation);
                if (timestampIndex < 0) {
                    return result;
                }
                return spliceResidual(result, parent, filter, operation, pushSetConjuncts(operation, filter.getPredicate(), timestampIndex));
            } else if (input instanceof AggregatePlan aggregate && !(aggregate.getChild() instanceof HorizonJoinPlan) && aggregate.getReaderCount() == 0) {
                if (!mapGroupingKeys(aggregate) || isUnstable(filter.getPredicate())) {
                    return result;
                }
                context.getRewriter().splitConjuncts(filter.getPredicate(), keyConjuncts, conjunctSplit);
                final BoundExpression movable = conjunctSplit.getMatched();
                if (movable == null) {
                    return result;
                }
                final BoundExpression residual = conjunctSplit.getRemainder();
                final BoundExpression remapped = context.getRewriter().remapColumns(movable, columnMap, aggregate.getChild().getOutput());
                final FilterPlan pushed = filters.next().of(aggregate.getChild(), remapped, remapped.getPosition());
                aggregate.replaceInput(0, pushDownScopedFilter(pushed));
                return spliceResidual(result, parent, filter, aggregate, residual);
            } else if (input instanceof FillPlan) {
                // Keyed fill emits the same buckets for each surviving key, so key-only
                // conjuncts filter the aggregate input instead.
                final LogicalPlan fillInput = input.inputAt(0);
                if (!(fillInput instanceof AggregatePlan aggregate) || !mapGroupingKeys(aggregate)
                        || !LogicalPlans.isOrderIndependent(filter.getPredicate())) {
                    return result;
                }
                context.getRewriter().splitConjuncts(filter.getPredicate(), keyConjuncts, conjunctSplit);
                final BoundExpression movable = conjunctSplit.getMatched();
                if (movable == null) {
                    return result;
                }
                final BoundExpression residual = conjunctSplit.getRemainder();
                final FilterPlan pushed = filters.next().of(fillInput, movable, movable.getPosition());
                input.replaceInput(0, pushDownFilter(pushed));
                return spliceResidual(result, parent, filter, input, residual);
            } else if (input instanceof LatestByPlan latest) {
                final BoundExpression selector = isOrderCrossed ? null : latestKeySelector(latest, filter, result);
                if (selector == null) {
                    // LATEST BY already emits rows in ascending timestamp order.
                    if (parent instanceof SortPlan sort && crossedProject != null && crossedProject.getChild() == sort) {
                        if (sort.getColumnIds().size() == 1
                                && sort.getColumnIds().getQuick(0) == input.getOutput().getTimestampColumnId()
                                && sort.getDirections().getQuick(0) == SortDirection.ASCENDING) {
                            crossedProject.replaceInput(0, filter);
                        }
                    }
                    return result;
                }
                input.replaceInput(0, filters.next().of(input.inputAt(0), selector, selector.getPosition()));
                return spliceResidual(result, parent, filter, input, context.getRewriter().removeConjunct(filter.getPredicate(), selector));
            } else {
                // In particular, never change which rows LATEST BY or LIMIT selects, or
                // move a predicate through computed projections.
                return result;
            }
            final LogicalPlan newInput = crossed.getChild();
            filter.of(newInput, predicate, filter.getPosition());
            filter.deriveOutput();
            crossed.replaceInput(0, filter);
            if (parent == null) {
                result = crossed;
            } else {
                parent.replaceInput(0, crossed);
            }
            parent = crossed;
        }
    }

    private LogicalPlan pushDownFilters(LogicalPlan root) throws SqlException {
        isLatestKeyScope = true;
        return pushDownFilters0(root);
    }

    private LogicalPlan pushDownFilters0(LogicalPlan plan) throws SqlException {
        final boolean wasLatestKeyScope = isLatestKeyScope;
        final LogicalPlan outerLimit = latestKeyLimit;
        final SortPlan outerSort = latestKeySort;
        switch (plan) {
            case LimitPlan limit -> {
                latestKeyLimit = latestKeySort == null ? limit : null;
                isLatestKeyScope &= latestKeyLimit != null;
            }
            case SortPlan sort -> latestKeySort = sort;
            case FilterPlan _ -> {
            }
            case ProjectPlan project -> isLatestKeyScope &= LogicalPlans.isColumnProjection(project);
            default -> isLatestKeyScope = false;
        }
        try {
            for (int i = 0, n = plan.inputCount(); i < n; i++) {
                plan.replaceInput(i, pushDownFilters0(plan.inputAt(i)));
            }
        } finally {
            isLatestKeyScope = wasLatestKeyScope;
            latestKeyLimit = outerLimit;
            latestKeySort = outerSort;
        }
        // One postorder visit per original node; only the newly exposed adjacent
        // boundaries are reconsidered after moving this filter.
        if (plan instanceof FilterPlan filter) {
            return pushDownFilter(filter);
        }
        if (plan instanceof AggregatePlan aggregate) {
            aggregateInputOrder.removeInputOrder(aggregate);
        }
        return plan;
    }

    private LogicalPlan pushDownScopedFilter(FilterPlan filter) throws SqlException {
        final boolean wasLatestKeyScope = isLatestKeyScope;
        isLatestKeyScope = false;
        try {
            return pushDownFilter(filter);
        } finally {
            isLatestKeyScope = wasLatestKeyScope;
        }
    }

    /**
     * Pushes key filters and single-source ON/WHERE conjuncts into the inputs of a join {@link JoinOrdering} ordered,
     * hoists conjuncts to the earliest step that can evaluate them and derives transitive filters over equi-join keys.
     */
    private LogicalPlan pushJoinFilters(LogicalPlan plan) throws SqlException {
        if (!(plan instanceof JoinPlan join)) {
            return plan;
        }
        final ObjList<JoinInput> ordered = join.getOrderedInputs();
        for (int i = 0, n = ordered.size(); i < n; i++) {
            final JoinInput step = ordered.getQuick(i);
            final BoundExpression keyFilter = step.getKeyFilter();
            if (keyFilter != null) {
                step.setKeyFilter(null);
                if (pushSourceJoinFilter(step, keyFilter) && step.isSubquery()) {
                    unqualifyFilteredKeys(step, keyFilter);
                }
            }
            // WHERE conjuncts go to their sources before inner ON conjuncts.
            step.setPostJoinFilter(pushSingleSourceJoinFilter(join, step.getPostJoinFilter(), i));
            if (!step.getJoinType().isBarrier()) {
                step.setOnResidual(hoistEarlierJoinConjuncts(join, pushSingleSourceJoinFilter(join, step.getOnResidual(), i), i));
                step.setPostJoinFilter(hoistEarlierJoinConjuncts(join, step.getPostJoinFilter(), i));
            }
        }
        deriveTransitiveFilters(join, null);
        join.getFilterConjuncts().clear();
        join.getFilterConjunctOrigins().clear();
        return plan;
    }

    private boolean pushSetBranch(SetOperationPlan operation, int branchIndex, BoundExpression predicate, int timestampIndex) throws SqlException {
        final LogicalPlan branch = operation.inputAt(branchIndex);
        final OutputSchema output = operation.getOutput();
        final int type = output.getColumnType(timestampIndex);
        if (branch.getOutput().getColumnType(timestampIndex) != type) {
            return false;
        }
        // Copying descriptions leaves the other branch and any retained residual's preparation untouched.
        columnMap.clear();
        columnMap.put(output.getColumnId(timestampIndex), branch.getOutput().getColumnId(timestampIndex));
        final BoundExpression replacement = context.getRewriter().copyRemappedColumns(predicate, columnMap);
        final FilterPlan pushed = filters.next().of(branch, replacement, predicate.getPosition());
        // A row-selection barrier keeps the filter above the whole branch.
        operation.replaceInput(branchIndex, isSetBranchSelectionBarrier(branch) ? pushed : pushDownScopedFilter(pushed));
        return true;
    }

    /**
     * Pushes the conjunct into both branches of {@link #pushedSetOperation} when it is stable and reads only the
     * column at {@link #pushedSetTimestampIndex}; returns whether both branches took it.
     */
    private boolean pushSetConjunct(BoundExpression conjunct) throws SqlException {
        final SetOperationPlan operation = pushedSetOperation;
        final int timestampIndex = pushedSetTimestampIndex;
        if (isUnstable(conjunct) || singleColumnId(conjunct) != operation.getOutput().getColumnId(timestampIndex)) {
            return false;
        }
        final boolean isLeftPushed = pushSetBranch(operation, 0, conjunct, timestampIndex);
        final boolean isRightPushed = pushSetBranch(operation, 1, conjunct, timestampIndex);
        return isLeftPushed && isRightPushed;
    }

    /**
     * Pushes each conjunct of the predicate that can reach both branches of the set operation, see
     * {@link #pushSetConjunct}, and returns the conjuncts that stay above it.
     */
    private BoundExpression pushSetConjuncts(SetOperationPlan operation, BoundExpression predicate, int timestampIndex) throws SqlException {
        // A branch can be a set operation itself, whose pushdown re-enters here before the outer conjuncts are done.
        final SetOperationPlan outerOperation = pushedSetOperation;
        final int outerTimestampIndex = pushedSetTimestampIndex;
        pushedSetOperation = operation;
        pushedSetTimestampIndex = timestampIndex;
        try {
            return context.getRewriter().retainConjuncts(predicate, unpushedSetConjuncts);
        } finally {
            pushedSetOperation = outerOperation;
            pushedSetTimestampIndex = outerTimestampIndex;
        }
    }

    private BoundExpression pushSingleSourceJoinFilter(JoinPlan join, BoundExpression predicate, int lastInput) throws SqlException {
        if (predicate == null) {
            return null;
        }
        final int base = joinConjunctTargets.size();
        final int copyBase = nullRejectingCopies.size();
        tmpExpressions.clear();
        LogicalPlans.collectConjuncts(predicate, tmpExpressions);
        for (int i = 0, n = tmpExpressions.size(); i < n; i++) {
            final BoundExpression conjunct = tmpExpressions.getQuick(i);
            final int target = joinConjunctTarget(conjunct, join, lastInput);
            joinConjunctTargets.add(target);
            if (target < 0) {
                collectNullRejectingCopy(conjunct, join, lastInput);
            }
        }
        tmpExpressions.clear();
        final BoundExpression residual = selectJoinConjuncts(predicate, base, -1);
        for (int i = 0, n = join.getInputs().size(); i < n; i++) {
            pushSourceJoinFilter(join.getInputs().getQuick(i), selectJoinConjuncts(predicate, base, i));
        }
        joinConjunctTargets.setPos(base);
        for (int i = copyBase, n = nullRejectingCopies.size(); i < n; i++) {
            pushSourceJoinFilter(join.getInputs().getQuick(nullRejectingCopySources.getQuick(i)), nullRejectingCopies.getQuick(i));
        }
        nullRejectingCopies.setPos(copyBase);
        nullRejectingCopySources.setPos(copyBase);
        return residual;
    }

    /**
     * Returns whether the filter moved below the input's top operator.
     */
    private boolean pushSourceJoinFilter(JoinInput occurrence, BoundExpression predicate) throws SqlException {
        if (predicate == null) {
            return false;
        }
        final LogicalPlan input = occurrence.getInput();
        assert input != null;
        final FilterPlan filter = filters.next().of(input, predicate, predicate.getPosition());
        final LogicalPlan pushed = pushDownScopedFilter(filter);
        occurrence.setInput(pushed);
        return pushed != filter;
    }

    private BoundExpression selectJoinConjuncts(BoundExpression predicate, int base, int source) throws SqlException {
        joinConjunctIndex = base;
        selectedJoinSource = source;
        return context.getRewriter().retainConjuncts(predicate, selectedJoinConjuncts);
    }

    private int singleColumnId(BoundExpression expression) {
        singleColumnId = -1;
        expression.walk(singleColumnReads);
        return singleColumnId;
    }


}
