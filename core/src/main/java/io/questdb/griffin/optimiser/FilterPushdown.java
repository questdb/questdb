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
import io.questdb.griffin.LogicalPlans;
import io.questdb.griffin.OperatorExpression;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlKeywords;
import io.questdb.griffin.plan.logical.AggregatePlan;
import io.questdb.griffin.plan.logical.BindVariableExpression;
import io.questdb.griffin.plan.logical.BoundExpression;
import io.questdb.griffin.plan.logical.ColumnExpression;
import io.questdb.griffin.plan.logical.ConstantExpression;
import io.questdb.griffin.plan.logical.CursorExpression;
import io.questdb.griffin.plan.logical.ExpressionVisitor;
import io.questdb.griffin.plan.logical.FillPlan;
import io.questdb.griffin.plan.logical.FilterPlan;
import io.questdb.griffin.plan.logical.FunctionExpression;
import io.questdb.griffin.plan.logical.HorizonJoinPlan;
import io.questdb.griffin.plan.logical.JoinInput;
import io.questdb.griffin.plan.logical.JoinKind;
import io.questdb.griffin.plan.logical.JoinPlan;
import io.questdb.griffin.plan.logical.LatestByPlan;
import io.questdb.griffin.plan.logical.LimitPlan;
import io.questdb.griffin.plan.logical.LogicalPlan;
import io.questdb.griffin.plan.logical.OutputSchema;
import io.questdb.griffin.plan.logical.ProjectPlan;
import io.questdb.griffin.plan.logical.ScanPlan;
import io.questdb.griffin.plan.logical.SetOperationPlan;
import io.questdb.griffin.plan.logical.SortDirection;
import io.questdb.griffin.plan.logical.SortPlan;
import io.questdb.griffin.plan.logical.TreeWalk;
import io.questdb.griffin.plan.logical.TypeExpression;
import io.questdb.griffin.plan.logical.UnaryPlan;
import io.questdb.griffin.plan.logical.WindowPlan;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;
import io.questdb.std.ObjectPool;

/**
 * Pushes join filters into join inputs and filters down the plan, derives transitive filters over
 * equi-join keys and narrows decorrelation domains with their master's filters.
 */
final class FilterPushdown implements OptimiserPass {
    private static final ExpressionVisitor NULL_LITERALS = FilterPushdown::nullLiteral;
    private static final int OFFSET_LEAVES = 3;
    private static final int OTHER_LEAVES = 4;
    private static final int PLAIN_LEAVES = 2;
    private static final int RUNTIME_LEAVES = 1;
    private static final int STATIC_LEAVES = 0;
    private final AggregateInputOrder aggregateInputOrder;
    private final OptimiserContext context;
    private final ObjList<BoundExpression> tmpExpressions;
    private final ObjectPool<FilterPlan> filters;
    private final IntList latestKeyPositions;
    private final ColumnExpression mappingColumn = new ColumnExpression();
    private final ProjectPlan mappingProjection = new ProjectPlan();
    private final ObjectPool<ColumnExpression> narrowingColumns;
    private final ObjectPool<ProjectPlan> narrowingProjects;
    private final IntList transitiveFactOrigins;
    private final ObjList<BoundExpression> transitiveFacts;
    private boolean isLatestKeyScope;
    private LogicalPlan latestKeyLimit;
    private SortPlan latestKeySort;
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
            ObjectPool<ProjectPlan> narrowingProjects,
            IntList latestKeyPositions,
            IntList transitiveFactOrigins,
            ObjList<BoundExpression> transitiveFacts
    ) {
        this.context = context;
        this.aggregateInputOrder = aggregateInputOrder;
        this.tmpExpressions = tmpExpressions;
        this.filters = filters;
        this.narrowingColumns = narrowingColumns;
        this.narrowingProjects = narrowingProjects;
        this.latestKeyPositions = latestKeyPositions;
        this.transitiveFactOrigins = transitiveFactOrigins;
        this.transitiveFacts = transitiveFacts;
    }

    @Override
    public LogicalPlan apply(LogicalPlan plan) throws SqlException {
        pushJoinFilters(plan);
        // Pushdown and pruning read the ordered-branch marks when they drop an aggregate's input order.
        aggregateInputOrder.collectOrderedBranchAggregates(plan);
        final LogicalPlan pushed = pushDownFilters(plan);
        filterSharedDomains(pushed);
        return pushed;
    }

    @Override
    public void clear() {
        isLatestKeyScope = false;
        latestKeyLimit = null;
        latestKeySort = null;
        mappingColumn.clear();
        mappingProjection.clear();
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

    private static BoundExpression firstLatestKeyEquality(FunctionExpression call) {
        BoundExpression expression = call;
        while (expression instanceof FunctionExpression or && or.isOr()) {
            expression = or.argumentAt(0);
        }
        return expression;
    }

    private static boolean hasInputTimestamp(ProjectPlan project) {
        final int timestampId = project.getInput().getOutput().getTimestampColumnId();
        for (int i = 0, n = project.getExpressions().size(); i < n; i++) {
            if (project.getExpressions().getQuick(i) instanceof ColumnExpression column
                    && column.getColumnId() == timestampId) {
                return true;
            }
        }
        return false;
    }

    private static boolean hasMasterNullingJoin(JoinPlan join) {
        for (int i = 1, n = join.getInputs().size(); i < n; i++) {
            final JoinKind type = join.getInputs().getQuick(i).getJoinType();
            if (type == JoinKind.RIGHT_OUTER || type == JoinKind.FULL_OUTER) {
                return true;
            }
        }
        return false;
    }

    /**
     * Whether a join step is neither INNER, CROSS nor LEFT/RIGHT/FULL OUTER, so equality facts cannot cross it.
     */
    private static boolean hasNonTransitiveJoin(JoinPlan join) {
        for (int i = 1, n = join.getInputs().size(); i < n; i++) {
            switch (join.getInputs().getQuick(i).getJoinType()) {
                case INNER, CROSS, LEFT_OUTER, RIGHT_OUTER, FULL_OUTER -> {
                }
                default -> {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * Whether the expression, or any call a constant folds from, has a NULL literal argument.
     */
    private static boolean hasNullLiteral(BoundExpression expression) {
        return !expression.walk(NULL_LITERALS);
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

    private static boolean isInnerJoinRegion(JoinPlan join) {
        for (int i = 1, n = join.getInputs().size(); i < n; i++) {
            final JoinKind type = join.getInputs().getQuick(i).getJoinType();
            if (type != JoinKind.INNER && type != JoinKind.CROSS) {
                return false;
            }
        }
        return true;
    }

    private static boolean isKeyColumn(BoundExpression expression, int keyId) {
        return expression instanceof ColumnExpression column && column.isDirectReference() && column.getColumnId() == keyId;
    }

    private static boolean isKeyLiteral(BoundExpression expression) {
        if (!(expression instanceof ConstantExpression constant) || !constant.isLiteral()) {
            return false;
        }
        return switch (ColumnType.tagOf(expression.getDataType())) {
            case ColumnType.NULL, ColumnType.CHAR, ColumnType.STRING, ColumnType.VARCHAR, ColumnType.SYMBOL -> true;
            default -> false;
        };
    }

    private static boolean isKeyOnly(BoundExpression expression, ProjectPlan keys) {
        if (expression instanceof ColumnExpression column) {
            return keys.getOutput().getColumnIndexById(column.getColumnId()) >= 0 && canPushThroughProjection(column, keys);
        }
        if (expression instanceof FunctionExpression call) {
            for (int i = 0, n = call.getArgumentCount(); i < n; i++) {
                if (!isKeyOnly(call.argumentAt(i), keys)) {
                    return false;
                }
            }
            return true;
        }
        return !(expression instanceof CursorExpression);
    }

    private static boolean isLatestKeyEquality(BoundExpression expression, int keyId) {
        return expression instanceof FunctionExpression call && "=".equals(call.getName())
                && call.getArgumentCount() == 2
                && (isKeyColumn(call.argumentAt(0), keyId) && isKeyLiteral(call.argumentAt(1))
                || isKeyColumn(call.argumentAt(1), keyId) && isKeyLiteral(call.argumentAt(0)));
    }

    private static boolean isLatestKeyOr(BoundExpression expression, int keyId) {
        if (expression instanceof FunctionExpression call && call.isOr()) {
            return isLatestKeyOr(call.argumentAt(0), keyId) && isLatestKeyOr(call.argumentAt(1), keyId);
        }
        return isLatestKeyEquality(expression, keyId);
    }

    private static boolean isLatestKeyParameter(BoundExpression expression, int keyId, ScanPlan scan) {
        if (!(expression instanceof FunctionExpression call) || !"=".equals(call.getName())
                || call.getArgumentCount() != 2 || scan.getIndexedColumnIds().indexOf(keyId, 0, scan.getIndexedColumnIds().size()) >= 0) {
            return false;
        }
        final BoundExpression value = isKeyColumn(call.argumentAt(0), keyId) ? call.argumentAt(1)
                : isKeyColumn(call.argumentAt(1), keyId) ? call.argumentAt(0) : null;
        return value instanceof BindVariableExpression parameter && parameter.isPredefined()
                && (value.getDataType() == ColumnType.STRING || value.getDataType() == ColumnType.VARCHAR);
    }

    private static boolean isLatestKeyResidual(BoundExpression expression) {
        if (!(expression instanceof FunctionExpression call) || call.getArgumentCount() != 2) {
            return false;
        }
        switch (call.getName()) {
            case "=", "!=", "<>", "<", "<=", ">", ">=" -> {
            }
            default -> {
                return false;
            }
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
            if (call.getArgumentCount() < 2 || !isKeyColumn(call.argumentAt(0), keyId)) {
                return false;
            }
            for (int i = 1, n = call.getArgumentCount(); i < n; i++) {
                if (!isKeyLiteral(call.argumentAt(i))) {
                    return false;
                }
            }
            return true;
        }
        if (call.isOr()) {
            return isLatestKeyOr(call, keyId);
        }
        return isLatestKeyEquality(call, keyId);
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
        if (filter.getInput() instanceof WindowPlan window && filter.getPredicate() instanceof ColumnExpression column) {
            final IntList ids = window.getFunctionColumnIds();
            final int index = ids.indexOf(column.getColumnId(), 0, ids.size());
            return index >= 0 && window.getSpecs().getQuick(index).isSubsampleKeepFlag();
        }
        return false;
    }

    private static boolean isTimestampOffset(BoundExpression expression, OutputSchema input) {
        return expression instanceof FunctionExpression call && call.getArgumentCount() == 3
                && SqlKeywords.isDateaddKeyword(call.getName())
                && call.argumentAt(0) instanceof ConstantExpression
                && call.argumentAt(1) instanceof ConstantExpression stride && stride.isLiteral()
                && stride.getDataType() == ColumnType.INT
                && call.argumentAt(2) instanceof ColumnExpression column && column.isDirectReference()
                && column.getColumnId() == input.getTimestampColumnId();
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

    private static int joinSourceOrdinal(BoundExpression expression, JoinPlan join) {
        if (expression instanceof ColumnExpression column) {
            for (int i = 0, n = join.getInputs().size(); i < n; i++) {
                if (join.getInputs().getQuick(i).getSourceOutput().getColumnIndexById(column.getColumnId()) >= 0) {
                    return i;
                }
            }
            throw new IllegalStateException("join predicate input has changed");
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
            for (int i = 0, n = join.getInputs().size(); i < n; i++) {
                final JoinInput input = join.getInputs().getQuick(i);
                if (input.getSourceOutput().getColumnIndexById(column.getColumnId()) >= 0) {
                    return join.getOrderedInputs().indexOf(input);
                }
            }
            return Integer.MAX_VALUE;
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
                return project.getInput().getOutput().getColumnIndexById(reference.getColumnId()) >= 0
                        && canPushThroughProjection(column, project) ? PLAIN_LEAVES : OTHER_LEAVES;
            }
            return isTimestampOffset(projected, project.getInput().getOutput()) ? OFFSET_LEAVES : OTHER_LEAVES;
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
            for (int i = 0, n = call.getArgumentCount(); i < n; i++) {
                final int argument = projectedLeaves(call.argumentAt(i), project);
                if (argument == OTHER_LEAVES) {
                    return OTHER_LEAVES;
                }
                leaves = Math.max(leaves, argument);
            }
            if (leaves == OFFSET_LEAVES) {
                for (int i = 0, n = call.getArgumentCount(); i < n; i++) {
                    if (projectedLeaves(call.argumentAt(i), project) == RUNTIME_LEAVES) {
                        return OTHER_LEAVES;
                    }
                }
            }
            return leaves;
        }
        if (expression instanceof ConstantExpression || expression instanceof TypeExpression) {
            return STATIC_LEAVES;
        }
        return expression instanceof CursorExpression ? OTHER_LEAVES : RUNTIME_LEAVES;
    }

    private static int sourceColumnId(LogicalPlan plan, int columnId, LogicalPlan target) {
        while (plan != target) {
            switch (plan) {
                case ProjectPlan project -> {
                    final int index = project.getOutput().getColumnIndexById(columnId);
                    if (index < 0 || !(project.getExpressions().getQuick(index) instanceof ColumnExpression column)
                            || !column.isDirectReference()) {
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
            final IntList slaveIds = step.getSlaveKeyColumnIds();
            for (int i = 0, n = slaveIds.size(); i < n; i++) {
                if (slaveIds.getQuick(i) == column.getColumnId()) {
                    step.unqualifySlaveKeyName(i);
                }
            }
        } else if (filter instanceof FunctionExpression function) {
            for (int i = 0, n = function.getArgumentCount(); i < n; i++) {
                unqualifyFilteredKeys(step, function.argumentAt(i));
            }
        }
    }

    private void collectLatestKeyValues(BoundExpression expression, int keyId, IntList positions) {
        final FunctionExpression call = (FunctionExpression) expression;
        if (call.isOr()) {
            collectLatestKeyValues(call.argumentAt(0), keyId, positions);
            collectLatestKeyValues(call.argumentAt(1), keyId, positions);
        } else {
            final int valueIndex = isKeyColumn(call.argumentAt(0), keyId) ? 1 : 0;
            tmpExpressions.add(call.argumentAt(valueIndex));
            positions.add(call.getArgumentPosition(valueIndex));
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
            if (!isInnerRegion && origin >= 0) {
                final JoinKind type = join.getInputs().getQuick(origin).getJoinType();
                if (type != JoinKind.INNER && type != JoinKind.CROSS) {
                    continue;
                }
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
        final int slaveId = slave.getSlaveKeyColumnIds().getQuick(keyIndex);
        final ColumnExpression column = transitiveFilterColumn(predicate);
        final LogicalPlan target = slave.getInput();
        if (column == null || target == null || column.getColumnId() != masterId) {
            return;
        }
        final int index = target.getOutput().getColumnIndexById(slaveId);
        if (index < 0 || target.getOutput().getColumnType(index) != column.getDataType()) {
            return;
        }
        mappingProjection.clear();
        mappingProjection.of(target, predicate.getPosition());
        mappingProjection.getOutput().add(masterId, target.getOutput().getColumnName(index), column.getDataType(), true);
        mappingProjection.getExpressions().add(mappingColumn.of(slaveId, column.getDataType(), column.getPosition()));
        FunctionExpression derived = (FunctionExpression) context.getRewriter().copyRemappedColumns(predicate, mappingProjection);
        if (((FunctionExpression) predicate).argumentAt(1) == column) {
            derived = context.getRewriter().commuteEquality(derived);
        }
        if (pushSourceJoinFilter(slave, derived) && slave.isSubquery()) {
            slave.unqualifySlaveKeyName(keyIndex);
        }
    }

    private void deriveTransitiveFilters(JoinPlan join, BoundExpression pushedPredicate) throws SqlException {
        final boolean isInnerRegion = isInnerJoinRegion(join);
        if (hasNonTransitiveJoin(join)) {
            return;
        }
        if (pushedPredicate == null) {
            collectTransitiveFacts(join, isInnerRegion);
        }
        // Only original facts are consulted; a derived slave filter cannot pin another key.
        for (int i = 0, n = join.getInputs().size(); i < n; i++) {
            final JoinInput slave = join.getInputs().getQuick(i);
            for (int k = 0, count = slave.getMasterKeyColumnIds().size(); k < count; k++) {
                final int masterId = slave.getMasterKeyColumnIds().getQuick(k);
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
    private void filterSharedDomains(LogicalPlan plan) {
        for (int i = 0, n = plan.inputCount(); i < n; i++) {
            filterSharedDomains(plan.inputAt(i));
        }
        if (!(plan instanceof AggregatePlan aggregate) || aggregate.getSharedSource() == null
                || !(aggregate.getInput() instanceof ScanPlan scan)) {
            return;
        }
        final LogicalPlan source = LogicalPlans.skipFilters(aggregate.getSharedSource().getInput());
        if (source == aggregate.getSharedSource().getInput() || !(source instanceof ScanPlan sourceScan)
                || !sourceScan.getTableToken().equals(scan.getTableToken())) {
            return;
        }
        final int position = aggregate.getPosition();
        final ProjectPlan mapping = narrowingProjects.next().of(scan, position);
        final IntList inputIds = aggregate.getSharedInputIds();
        final IntList sourceIds = aggregate.getSharedSourceIds();
        for (int i = 0, n = inputIds.size(); i < n; i++) {
            final int index = scan.getOutput().getColumnIndexById(inputIds.getQuick(i));
            if (index >= 0) {
                final int type = scan.getOutput().getColumnType(index);
                mapping.getExpressions().add(narrowingColumns.next().of(inputIds.getQuick(i), type, position));
                mapping.getOutput().add(sourceIds.getQuick(i), scan.getOutput().getColumnName(index), type, true);
            }
        }
        LogicalPlan input = scan;
        for (LogicalPlan filter = aggregate.getSharedSource().getInput(); filter != source; filter = filter.inputAt(0)) {
            final BoundExpression predicate = ((FilterPlan) filter).getPredicate();
            if (LogicalPlans.isStableWithinExecution(predicate) && LogicalPlans.readsOnly(predicate, mapping.getOutput())) {
                final FilterPlan copy = filters.next().of(input, context.getRewriter().copyRemappedColumns(predicate, mapping), predicate.getPosition());
                copy.deriveOutput();
                input = copy;
            }
        }
        aggregate.replaceInput(0, input);
    }

    private ProjectPlan groupingKeyView(AggregatePlan aggregate) {
        ProjectPlan view = null;
        final ObjList<BoundExpression> grouping = aggregate.getGroupingExpressions();
        final OutputSchema output = aggregate.getOutput();
        final OutputSchema input = aggregate.getInput().getOutput();
        for (int i = 0, n = grouping.size(); i < n; i++) {
            if (grouping.getQuick(i) instanceof ColumnExpression column && column.isDirectReference()) {
                if (view == null) {
                    view = narrowingProjects.next().of(aggregate.getInput(), aggregate.getPosition());
                }
                view.getExpressions().add(column);
                view.getOutput().add(output.getColumnId(i), output.getColumnName(i), output.getColumnType(i), true);
                view.getOutput().setSymbolTableStatic(view.getOutput().getColumnCount() - 1,
                        input.isSymbolTableStatic(input.getColumnIndexById(column.getColumnId())));
            }
        }
        return view;
    }

    private BoundExpression hoistEarlierJoinConjuncts(JoinPlan join, BoundExpression predicate, int position) throws SqlException {
        if (predicate instanceof FunctionExpression call && call.getArgumentCount() == 2 && call.isAnd()) {
            return context.getRewriter().replaceConjunction(call, hoistEarlierJoinConjuncts(join, call.argumentAt(0), position),
                    hoistEarlierJoinConjuncts(join, call.argumentAt(1), position));
        }
        if (predicate == null || !LogicalPlans.isOrderIndependent(predicate)) {
            return predicate;
        }
        final ObjList<JoinInput> ordered = join.getOrderedInputs();
        final int target = latestJoinPosition(predicate, join);
        if (target < 1 || target >= position || ordered.getQuick(target).getJoinType() != JoinKind.LEFT_OUTER) {
            return predicate;
        }
        for (int i = target + 1; i < position; i++) {
            switch (ordered.getQuick(i).getJoinType()) {
                case INNER, CROSS, LEFT_OUTER -> {
                }
                default -> {
                    return predicate;
                }
            }
        }
        final JoinInput step = ordered.getQuick(target);
        step.setPostJoinFilter(context.getRewriter().combineConjunction(step.getPostJoinFilter(), predicate, predicate.getPosition()));
        return null;
    }

    private boolean isNullRejectingFact(FunctionExpression fact) {
        return context.isNullRejecting(fact, fact.argumentAt(0) == transitiveFilterColumn(fact) ? 0 : 1);
    }

    /**
     * An INNER ON fact holds for every output row only up to the first master-nulling join after its
     * column's source; rows that join NULL-extends satisfy it only when the fact rejects NULL.
     */
    private boolean isOnFactSafe(JoinPlan join, int columnId, JoinInput target, int origin, FunctionExpression fact) {
        final ObjList<JoinInput> ordered = join.getOrderedInputs();
        int sourcePosition = -1;
        for (int i = 0, n = ordered.size(); i < n && sourcePosition < 0; i++) {
            if (ordered.getQuick(i).getSourceOutput().getColumnIndexById(columnId) >= 0) {
                sourcePosition = i;
            }
        }
        int boundaryPosition = -1;
        for (int i = sourcePosition + 1, n = ordered.size(); i < n && boundaryPosition < 0; i++) {
            if (ordered.getQuick(i).getJoinType().isMasterNulling()) {
                boundaryPosition = i;
            }
        }
        if (sourcePosition < 0 || boundaryPosition < 0) {
            return true;
        }
        return ordered.indexOf(target) < boundaryPosition && (!"=".equals(fact.getName()) || isNullRejectingFact(fact)
                || ordered.indexOf(join.getInputs().getQuick(origin)) < boundaryPosition);
    }

    private boolean isTransitiveSourceSafe(JoinPlan join, int source, BoundExpression fact) {
        final int last = join.getOrderedInputs().size() - 1;
        if (isInnerJoinRegion(join)) {
            return LogicalPlans.canPushJoinFilter(join, source, last);
        }
        final JoinInput input = join.getInputs().getQuick(source);
        if (join.getOrderedInputs().indexOf(input) > 0 && input.getJoinType() != JoinKind.INNER
                && input.getJoinType() != JoinKind.CROSS) {
            return false;
        }
        if (!hasMasterNullingJoin(join)) {
            return LogicalPlans.canPushJoinFilter(join, source, last);
        }
        final FunctionExpression call = (FunctionExpression) fact;
        if ("=".equals(call.getName())) {
            return isNullRejectingFact(call);
        }
        return call.argumentAt(1) instanceof ConstantExpression constant && constant.isLiteral()
                && ColumnType.tagOf(constant.getDataType()) != ColumnType.NULL;
    }

    private BoundExpression latestKeyList(LatestByPlan latest, BoundExpression selector) {
        if (!(selector instanceof FunctionExpression call) || !call.isOr()) {
            return selector;
        }
        final int keyId = latest.getKeyColumnIds().getQuick(0);
        tmpExpressions.clear();
        latestKeyPositions.clear();
        try {
            collectLatestKeyValues(call, keyId, latestKeyPositions);
            final FunctionExpression first = (FunctionExpression) firstLatestKeyEquality(call);
            final BoundExpression key = first.argumentAt(isKeyColumn(first.argumentAt(0), keyId) ? 0 : 1);
            return context.getRewriter().symbolIn(key, tmpExpressions, latestKeyPositions, call.getFunctionFlags(), call.getPosition());
        } finally {
            tmpExpressions.clear();
            latestKeyPositions.clear();
        }
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
        if (!isLatestKeyScope || latest.getKeyColumnIds().size() != 1 || !(latest.getInput() instanceof ScanPlan scan)) {
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
                final int id = sourceColumnId(latestKeySort.getInput() == filter ? crossed : latestKeySort.getInput(),
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

    private LogicalPlan pushDownFilter(FilterPlan filter) throws SqlException {
        LogicalPlan result = filter;
        UnaryPlan parent = null;
        ProjectPlan crossedProject = null;
        boolean isOrderCrossed = false;
        while (true) {
            final LogicalPlan input = filter.getInput();
            final UnaryPlan crossed;
            final BoundExpression predicate;
            if (input instanceof ProjectPlan project && !LogicalPlans.isColumnProjection(project)) {
                final BoundExpression movable = selectComputedProjectionConjuncts(filter.getPredicate(), project, true);
                if (movable != null) {
                    final BoundExpression residual = selectComputedProjectionConjuncts(filter.getPredicate(), project, false);
                    final BoundExpression substituted = context.getRewriter().substituteProjection(movable, project);
                    final FilterPlan pushed = filters.next().of(project.getInput(), substituted, substituted.getPosition());
                    pushed.deriveOutput();
                    project.replaceInput(0, pushDownScopedFilter(pushed));
                    if (residual != null) {
                        filter.of(project, residual, filter.getPosition());
                    } else if (parent == null) {
                        return project;
                    } else {
                        parent.replaceInput(0, project);
                    }
                }
                return result;
            }
            if (input instanceof ProjectPlan project) {
                // A projected timestamp CAST keeps scalar precision in its consumer's scope.
                if (!canPushThroughProjection(filter.getPredicate(), project)) {
                    if (!LogicalPlans.isOrderIndependent(filter.getPredicate())) {
                        return result;
                    }
                    final BoundExpression movable = selectProjectionConjuncts(filter.getPredicate(), project, true);
                    if (movable != null) {
                        final BoundExpression residual = selectProjectionConjuncts(filter.getPredicate(), project, false);
                        final BoundExpression remapped = context.getRewriter().remapColumns(movable, project);
                        final FilterPlan pushed = filters.next().of(project.getInput(), remapped, remapped.getPosition());
                        pushed.deriveOutput();
                        project.replaceInput(0, pushDownFilter(pushed));
                        filter.of(project, residual, filter.getPosition());
                    }
                    return result;
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
                filter.of(inner.getInput(), combined, filter.getPosition());
                filter.deriveOutput();
                continue;
            } else if (input instanceof SortPlan sort) {
                if (!LogicalPlans.isOrderIndependent(filter.getPredicate())) {
                    final BoundExpression movable = selectOrderIndependentConjuncts(filter.getPredicate(), true);
                    if (movable == null) {
                        return result;
                    }
                    final BoundExpression residual = selectOrderIndependentConjuncts(filter.getPredicate(), false);
                    final LogicalPlan belowSort = sort.getInput();
                    final FilterPlan pushed = filters.next().of(belowSort, movable, movable.getPosition());
                    pushed.deriveOutput();
                    sort.replaceInput(0, pushDownFilter(pushed));
                    filter.of(sort, residual, filter.getPosition());
                    return result;
                }
                crossed = sort;
                predicate = filter.getPredicate();
                isOrderCrossed = true;
            } else if (input instanceof JoinPlan join) {
                final BoundExpression residual = pushSingleSourceJoinFilter(join, filter.getPredicate(), join.getOrderedInputs().size() - 1);
                deriveTransitiveFiltersFromPushedPredicate(join, filter.getPredicate());
                if (residual == null) {
                    if (parent == null) {
                        return join;
                    }
                    parent.replaceInput(0, join);
                } else if (residual != filter.getPredicate()) {
                    filter.of(join, residual, filter.getPosition());
                }
                return result;
            } else if (input instanceof SetOperationPlan operation) {
                final int timestampIndex = LogicalPlans.setTimestampIndex(operation);
                if (timestampIndex < 0) {
                    return result;
                }
                final BoundExpression residual = pushSetConjuncts(operation, filter.getPredicate(), timestampIndex);
                if (residual == null) {
                    if (parent == null) {
                        return input;
                    }
                    parent.replaceInput(0, input);
                } else if (residual != filter.getPredicate()) {
                    filter.of(input, residual, filter.getPosition());
                }
                return result;
            } else if (input instanceof AggregatePlan aggregate && !(aggregate.getInput() instanceof HorizonJoinPlan)) {
                final ProjectPlan keys = groupingKeyView(aggregate);
                if (keys == null || isUnstable(filter.getPredicate())) {
                    return result;
                }
                final BoundExpression movable = selectKeyConjuncts(filter.getPredicate(), keys, true);
                if (movable == null) {
                    return result;
                }
                final BoundExpression residual = selectKeyConjuncts(filter.getPredicate(), keys, false);
                final BoundExpression remapped = context.getRewriter().remapColumns(movable, keys);
                final FilterPlan pushed = filters.next().of(aggregate.getInput(), remapped, remapped.getPosition());
                pushed.deriveOutput();
                aggregate.replaceInput(0, pushDownScopedFilter(pushed));
                if (residual != null) {
                    filter.of(input, residual, filter.getPosition());
                    return result;
                }
                if (parent == null) {
                    return input;
                }
                parent.replaceInput(0, input);
                return result;
            } else if (input instanceof FillPlan) {
                // Keyed fill emits the same buckets for each surviving key, so key-only
                // conjuncts filter the aggregate input instead.
                final LogicalPlan fillInput = input.inputAt(0);
                final ProjectPlan keys = fillInput instanceof AggregatePlan aggregate ? groupingKeyView(aggregate) : null;
                if (keys == null || !LogicalPlans.isOrderIndependent(filter.getPredicate())) {
                    return result;
                }
                final BoundExpression movable = selectKeyConjuncts(filter.getPredicate(), keys, true);
                if (movable == null) {
                    return result;
                }
                final BoundExpression residual = selectKeyConjuncts(filter.getPredicate(), keys, false);
                final FilterPlan pushed = filters.next().of(fillInput, movable, movable.getPosition());
                pushed.deriveOutput();
                input.replaceInput(0, pushDownFilter(pushed));
                if (residual != null) {
                    filter.of(input, residual, filter.getPosition());
                    return result;
                }
                if (parent == null) {
                    return input;
                }
                parent.replaceInput(0, input);
                return result;
            } else if (input instanceof LatestByPlan latest) {
                final BoundExpression selector = isOrderCrossed ? null : latestKeySelector(latest, filter, result);
                final BoundExpression keys = selector == null ? null : latestKeyList(latest, selector);
                if (keys == null) {
                    // LATEST BY already emits rows in ascending timestamp order.
                    if (parent instanceof SortPlan sort && crossedProject != null && crossedProject.getInput() == sort) {
                        if (sort.getColumnIds().size() == 1
                                && sort.getColumnIds().getQuick(0) == input.getOutput().getTimestampColumnId()
                                && sort.getDirections().getQuick(0) == SortDirection.ASCENDING) {
                            crossedProject.replaceInput(0, filter);
                        }
                    }
                    return result;
                }
                final LogicalPlan scan = input.inputAt(0);
                final FilterPlan pushed = filters.next().of(scan, keys, keys.getPosition());
                pushed.deriveOutput();
                input.replaceInput(0, pushed);
                final BoundExpression residual = withoutConjunct(filter.getPredicate(), selector);
                if (residual != null) {
                    filter.of(input, residual, filter.getPosition());
                    return result;
                }
                if (parent == null) {
                    return input;
                }
                parent.replaceInput(0, input);
                return result;
            } else {
                // In particular, never change which rows LATEST BY or LIMIT selects, or
                // move a predicate through computed projections.
                return result;
            }
            final LogicalPlan newInput = crossed.getInput();
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
     * Publishes the step order of two-input joins (the binder already ordered wider joins), then
     * pushes key filters and single-source ON/WHERE conjuncts into join inputs, hoists conjuncts to
     * the earliest step that can evaluate them and derives transitive filters over equi-join keys.
     */
    private void pushJoinFilters(LogicalPlan plan) throws SqlException {
        for (int i = 0, n = plan.inputCount(); i < n; i++) {
            pushJoinFilters(plan.inputAt(i));
        }
        if (!(plan instanceof JoinPlan join)) {
            return;
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
            if (step.getJoinType() == JoinKind.INNER || step.getJoinType() == JoinKind.CROSS) {
                step.setOnResidual(hoistEarlierJoinConjuncts(join, pushSingleSourceJoinFilter(join, step.getOnResidual(), i), i));
                step.setPostJoinFilter(hoistEarlierJoinConjuncts(join, step.getPostJoinFilter(), i));
            }
        }
        deriveTransitiveFilters(join, null);
        join.getFilterConjuncts().clear();
        join.getFilterConjunctOrigins().clear();
    }

    private boolean pushSetBranch(SetOperationPlan operation, int branchIndex, BoundExpression predicate, int timestampIndex) throws SqlException {
        final LogicalPlan branch = operation.inputAt(branchIndex);
        final OutputSchema output = operation.getOutput();
        final int type = output.getColumnType(timestampIndex);
        if (branch.getOutput().getColumnType(timestampIndex) != type) {
            return false;
        }
        // This one-column mapping is temporary only. Copying descriptions leaves
        // the other branch and any retained residual's preparation untouched.
        mappingProjection.clear();
        mappingProjection.of(branch, predicate.getPosition());
        mappingProjection.getOutput().add(output.getColumnId(timestampIndex), output.getColumnName(timestampIndex), type, true);
        mappingProjection.getExpressions().add(mappingColumn.of(branch.getOutput().getColumnId(timestampIndex), type, predicate.getPosition()));
        final BoundExpression replacement = context.getRewriter().copyRemappedColumns(predicate, mappingProjection);
        final FilterPlan pushed = filters.next().of(branch, replacement, predicate.getPosition());
        pushed.deriveOutput();
        // A row-selection barrier keeps the filter above the whole branch.
        operation.replaceInput(branchIndex, isSetBranchSelectionBarrier(branch) ? pushed : pushDownScopedFilter(pushed));
        return true;
    }

    private BoundExpression pushSetConjuncts(SetOperationPlan operation, BoundExpression predicate, int timestampIndex) throws SqlException {
        if (predicate instanceof FunctionExpression call && call.getArgumentCount() == 2 && call.isAnd()) {
            final BoundExpression left = pushSetConjuncts(operation, call.argumentAt(0), timestampIndex);
            final BoundExpression right = pushSetConjuncts(operation, call.argumentAt(1), timestampIndex);
            return context.getRewriter().replaceConjunction(call, left, right);
        }
        if (isUnstable(predicate) || singleColumnId(predicate) != operation.getOutput().getColumnId(timestampIndex)) {
            return predicate;
        }
        final boolean isLeftPushed = pushSetBranch(operation, 0, predicate, timestampIndex);
        final boolean isRightPushed = pushSetBranch(operation, 1, predicate, timestampIndex);
        return isLeftPushed && isRightPushed ? null : predicate;
    }

    private BoundExpression pushSingleSourceJoinFilter(JoinPlan join, BoundExpression predicate, int lastInput) throws SqlException {
        if (predicate == null) {
            return null;
        }
        final BoundExpression residual = selectJoinConjuncts(predicate, join, lastInput, -1);
        for (int i = 0, n = join.getInputs().size(); i < n; i++) {
            pushSourceJoinFilter(join.getInputs().getQuick(i), selectJoinConjuncts(predicate, join, lastInput, i));
        }
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
        filter.deriveOutput();
        final LogicalPlan pushed = pushDownScopedFilter(filter);
        occurrence.setInput(pushed);
        return pushed != filter;
    }

    /**
     * Selects conjuncts over plain projected columns and projected timestamp offsets: dateadd
     * with constant unit and INT stride over the input's designated timestamp. A conjunct over an
     * offset compares it with constants only, so interval extraction below can invert the offset.
     */
    private BoundExpression selectComputedProjectionConjuncts(BoundExpression predicate, ProjectPlan project, boolean isMovable) {
        if (predicate instanceof FunctionExpression call && call.getArgumentCount() == 2 && call.isAnd()) {
            return context.getRewriter().replaceConjunction(call,
                    selectComputedProjectionConjuncts(call.argumentAt(0), project, isMovable),
                    selectComputedProjectionConjuncts(call.argumentAt(1), project, isMovable));
        }
        final int leaves = projectedLeaves(predicate, project);
        final boolean isPushable = LogicalPlans.isOrderIndependent(predicate) && leaves != OTHER_LEAVES
                && (leaves != OFFSET_LEAVES || !hasInputTimestamp(project));
        return isPushable == isMovable ? predicate : null;
    }

    private BoundExpression selectJoinConjuncts(BoundExpression predicate, JoinPlan join, int lastInput, int selectedSource) {
        if (predicate instanceof FunctionExpression call && call.getArgumentCount() == 2
                && call.isAnd()) {
            final BoundExpression left = selectJoinConjuncts(call.argumentAt(0), join, lastInput, selectedSource);
            final BoundExpression right = selectJoinConjuncts(call.argumentAt(1), join, lastInput, selectedSource);
            return context.getRewriter().replaceConjunction(call, left, right);
        }
        final int source = joinSourceOrdinal(predicate, join);
        final int target = source >= 0 && (LogicalPlans.isOrderIndependent(predicate) || isColumnValueComparison(predicate))
                && LogicalPlans.canPushJoinFilter(join, source, lastInput) ? source : -1;
        return target == selectedSource ? predicate : null;
    }

    private BoundExpression selectKeyConjuncts(BoundExpression predicate, ProjectPlan keys, boolean isMovable) {
        if (predicate instanceof FunctionExpression call && call.getArgumentCount() == 2
                && call.isAnd()) {
            return context.getRewriter().replaceConjunction(call,
                    selectKeyConjuncts(call.argumentAt(0), keys, isMovable),
                    selectKeyConjuncts(call.argumentAt(1), keys, isMovable));
        }
        return isKeyOnly(predicate, keys) == isMovable ? predicate : null;
    }

    private BoundExpression selectOrderIndependentConjuncts(BoundExpression predicate, boolean isMovable) {
        if (predicate instanceof FunctionExpression call && call.getArgumentCount() == 2
                && call.isAnd()) {
            return context.getRewriter().replaceConjunction(call,
                    selectOrderIndependentConjuncts(call.argumentAt(0), isMovable),
                    selectOrderIndependentConjuncts(call.argumentAt(1), isMovable));
        }
        return LogicalPlans.isOrderIndependent(predicate) == isMovable ? predicate : null;
    }

    private BoundExpression selectProjectionConjuncts(BoundExpression predicate, ProjectPlan project, boolean isMovable) {
        if (predicate instanceof FunctionExpression call && call.getArgumentCount() == 2
                && call.isAnd()) {
            return context.getRewriter().replaceConjunction(call,
                    selectProjectionConjuncts(call.argumentAt(0), project, isMovable),
                    selectProjectionConjuncts(call.argumentAt(1), project, isMovable));
        }
        return canPushThroughProjection(predicate, project) == isMovable ? predicate : null;
    }

    private int singleColumnId(BoundExpression expression) {
        singleColumnId = -1;
        expression.walk(singleColumnReads);
        return singleColumnId;
    }

    private BoundExpression withoutConjunct(BoundExpression predicate, BoundExpression conjunct) {
        if (predicate == conjunct) {
            return null;
        }
        if (predicate instanceof FunctionExpression call && call.getArgumentCount() == 2 && call.isAnd()) {
            return context.getRewriter().replaceConjunction(call,
                    withoutConjunct(call.argumentAt(0), conjunct), withoutConjunct(call.argumentAt(1), conjunct));
        }
        return predicate;
    }
}
