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
import io.questdb.griffin.plan.logical.AggregatePlan;
import io.questdb.griffin.plan.logical.BindVariableExpression;
import io.questdb.griffin.plan.logical.BoundExpression;
import io.questdb.griffin.plan.logical.ColumnExpression;
import io.questdb.griffin.plan.logical.ComparisonOperator;
import io.questdb.griffin.plan.logical.ConstantExpression;
import io.questdb.griffin.plan.logical.CursorExpression;
import io.questdb.griffin.plan.logical.DistinctPlan;
import io.questdb.griffin.plan.logical.ExpressionVisitor;
import io.questdb.griffin.plan.logical.FilterPlan;
import io.questdb.griffin.plan.logical.FunctionExpression;
import io.questdb.griffin.plan.logical.FunctionSourcePlan;
import io.questdb.griffin.plan.logical.GroupingPlan;
import io.questdb.griffin.plan.logical.HorizonJoinPlan;
import io.questdb.griffin.plan.logical.JoinInput;
import io.questdb.griffin.plan.logical.JoinKind;
import io.questdb.griffin.plan.logical.JoinPlan;
import io.questdb.griffin.plan.logical.LimitPlan;
import io.questdb.griffin.plan.logical.LogicalPlan;
import io.questdb.griffin.plan.logical.OuterColumnExpression;
import io.questdb.griffin.plan.logical.OutputSchema;
import io.questdb.griffin.plan.logical.PlanVisitor;
import io.questdb.griffin.plan.logical.ProjectPlan;
import io.questdb.griffin.plan.logical.SampleByPlan;
import io.questdb.griffin.plan.logical.ScanPlan;
import io.questdb.griffin.plan.logical.SetOperationPlan;
import io.questdb.griffin.plan.logical.SortPlan;
import io.questdb.griffin.plan.logical.TreeWalk;
import io.questdb.griffin.plan.logical.TypeExpression;
import io.questdb.griffin.plan.logical.UnaryPlan;
import io.questdb.griffin.plan.logical.WindowJoinPlan;
import io.questdb.griffin.plan.logical.WindowPlan;
import io.questdb.griffin.plan.logical.WindowSpec;
import io.questdb.std.Chars;
import io.questdb.std.IntHashSet;
import io.questdb.std.IntIntHashMap;
import io.questdb.std.IntList;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;

/**
 * Stateless plan-node predicates, walkers and plan-property utilities shared by the binder, the optimiser and the plan generator.
 */
public final class LogicalPlans {
    public static final int SELF_COMPARISON_FALSE = 0;
    public static final int SELF_COMPARISON_NONE = -1;
    public static final int SELF_COMPARISON_TRUE = 1;
    private static final ExpressionVisitor COLUMN_READS = expression -> expression instanceof ColumnExpression ? TreeWalk.STOP : TreeWalk.CONTINUE;
    private static final PlanVisitor EXTERNAL_SOURCES = plan -> plan instanceof FunctionSourcePlan source && source.hasExternalDataSource()
            ? TreeWalk.STOP : TreeWalk.CONTINUE;
    private static final int ORDER_STABLE = 2;
    private static final ExpressionVisitor OUTER_COLUMN_READS = expression -> expression instanceof OuterColumnExpression ? TreeWalk.STOP : TreeWalk.CONTINUE;
    private static final int RESULT_STABLE = 1;
    private static final int SEQUENCE_STABLE = RESULT_STABLE | ORDER_STABLE;
    private static final ExpressionVisitor VOLATILE_NODES = expression -> !isStableWithinExecution(expression)
            || !(expression instanceof FunctionExpression || expression instanceof ColumnExpression || expression instanceof ConstantExpression
            || expression instanceof CursorExpression || expression instanceof BindVariableExpression || expression instanceof TypeExpression)
            ? TreeWalk.STOP : TreeWalk.CONTINUE;

    private LogicalPlans() {
    }

    public static boolean canPushJoinFilter(JoinPlan join, int source, int lastInput) {
        final ObjList<JoinInput> ordered = join.getOrderedInputs();
        return canPushJoinFilter(ordered, ordered.indexOf(join.getInputs().getQuick(source)), lastInput);
    }

    /**
     * Whether a filter of the columns of the join step at {@code position} alone can evaluate on its input before the
     * step at {@code lastPosition}: the step is not the outer or temporal side of its join, and no RIGHT or FULL join
     * up to that step joins after it.
     */
    public static boolean canPushJoinFilter(ObjList<JoinInput> steps, int position, int lastPosition) {
        if (position < 0 || position > lastPosition || steps.getQuick(position).getInput() == null) {
            return false;
        }
        if (position > 0 && steps.getQuick(position).getJoinType().isNulling()) {
            return false;
        }
        for (int i = position + 1; i <= lastPosition; i++) {
            if (steps.getQuick(i).getJoinType().isMasterNulling()) {
                return false;
            }
        }
        return true;
    }

    /**
     * Proves that a set timestamp predicate can reach every native source without changing precision.
     */
    public static boolean canPushSetTimestamp(LogicalPlan plan, int columnIndex) {
        final OutputSchema output = plan.getOutput();
        if (!ColumnType.isTimestamp(output.getColumnType(columnIndex))) {
            return false;
        }
        return switch (plan) {
            case ScanPlan scan -> output.getColumnId(columnIndex) == scan.getNativeTimestampColumnId();
            case ProjectPlan project -> {
                if (!isColumnProjection(project)) {
                    yield false;
                }
                final ColumnExpression column = (ColumnExpression) project.getExpressions().getQuick(columnIndex);
                final LogicalPlan input = project.getChild();
                final int inputIndex = input.getOutput().getColumnIndexById(column.getColumnId());
                yield inputIndex >= 0 && input.getOutput().getColumnType(inputIndex) == output.getColumnType(columnIndex)
                        && canPushSetTimestamp(input, inputIndex);
            }
            case FilterPlan filter ->
                    isOrderIndependent(filter.getPredicate()) && canPushSetTimestampThrough(filter, columnIndex);
            case SortPlan sort -> canPushSetTimestampThrough(sort, columnIndex);
            case SetOperationPlan operation -> canPushSetTimestampThrough(operation, columnIndex);
            default -> false;
        };
    }

    public static void collectConjuncts(BoundExpression predicate, ObjList<BoundExpression> sink) {
        if (predicate instanceof FunctionExpression call && call.isAnd()) {
            collectConjuncts(call.argumentAt(0), sink);
            collectConjuncts(call.argumentAt(1), sink);
        } else {
            sink.add(predicate);
        }
    }

    /**
     * Adds the ids of the input columns the expression reads that the sink does not hold yet, in the order a
     * projection reads them: a call's column arguments before its nested ones, the last argument of three or more
     * first.
     */
    public static void collectInputColumnIds(BoundExpression expression, OutputSchema input, IntList sink) {
        if (expression instanceof ColumnExpression column) {
            final int columnId = column.getColumnId();
            if (input.getColumnIndexById(columnId) >= 0 && sink.indexOf(columnId, 0, sink.size()) < 0) {
                sink.add(columnId);
            }
        } else if (expression instanceof FunctionExpression call) {
            final int count = call.getArgumentCount();
            for (int i = count < 3 ? count - 1 : count - 2; i >= 0; i--) {
                if (call.argumentAt(i) instanceof ColumnExpression) {
                    collectInputColumnIds(call.argumentAt(i), input, sink);
                }
            }
            if (count >= 3) {
                collectInputColumnIds(call.argumentAt(count - 1), input, sink);
            }
            for (int i = 0, n = count < 3 ? count : count - 1; i < n; i++) {
                if (!(call.argumentAt(i) instanceof ColumnExpression)) {
                    collectInputColumnIds(call.argumentAt(i), input, sink);
                }
            }
        }
    }

    /**
     * Records, for each key of the sort, the index of the projection expression that passes it through without a cast.
     */
    public static void collectSortKeyIndexes(ProjectPlan project, SortPlan sort, IntList sink) {
        sink.clear();
        for (int i = 0, n = sort.getColumnIds().size(); i < n; i++) {
            sink.add(projectedUncastColumnIndex(project, sort.getColumnIds().getQuick(i)));
        }
    }

    /**
     * The plan whose own expression or scan defines the column, followed through column-only projections, filters,
     * sorts and joins.
     */
    public static LogicalPlan columnSource(LogicalPlan plan, int id) {
        while (true) {
            switch (plan) {
                case ProjectPlan project -> {
                    final ColumnExpression column = passedColumn(project, id);
                    if (column == null) {
                        if (project.getOutput().getColumnIndexById(id) < 0) {
                            throw new IllegalStateException("predicate column is outside its projection");
                        }
                        return plan;
                    }
                    id = column.getColumnId();
                    plan = project.getChild();
                }
                case FilterPlan _, SortPlan _ -> plan = plan.inputAt(0);
                case JoinPlan join -> {
                    final JoinInput source = join.getInputs().getQuick(joinColumnSource(join, id));
                    if (source.getUnnest() != null) {
                        return plan;
                    }
                    plan = source.getInput();
                }
                default -> {
                    // LIMIT, grouping, DISTINCT and set operators define their own
                    // predicate scope. Existing timestamp provenance also stops here.
                    return plan;
                }
            }
        }
    }

    /**
     * The value of a constant predicate: 1 for true, 0 for false, -1 when the predicate is not a constant. Binding
     * folds every constant predicate to a literal, which {@code PlanVerifier} checks.
     */
    public static int constantTruth(BoundExpression predicate) {
        return predicate instanceof ConstantExpression constant ? constant.getLongValue() != 0 ? 1 : 0 : -1;
    }

    /**
     * True when the node reads the designated timestamp of its input at {@code inputIndex}, an index of the join's
     * {@link #orderedSteps}: the master of a join with a temporal step and the slave of each such step, the master and
     * slaves of a horizon or window join, and the input of a SAMPLE BY over its designated timestamp.
     */
    public static boolean consumesInputTimestamp(LogicalPlan plan, int inputIndex) {
        return switch (plan) {
            case JoinPlan join -> {
                final ObjList<JoinInput> steps = orderedSteps(join);
                if (inputIndex > 0) {
                    yield steps.getQuick(inputIndex).getJoinType().isTemporal();
                }
                for (int i = 1, n = steps.size(); i < n; i++) {
                    if (steps.getQuick(i).getJoinType().isTemporal()) {
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

    public static void deriveColumnReferenceCounts(LogicalPlan plan, IntHashSet columnIds) {
        for (int i = 0, n = plan.inputCount(); i < n; i++) {
            final LogicalPlan input = plan.inputAt(i);
            if (input != null) {
                deriveColumnReferenceCounts(input, columnIds);
            }
        }
        if (plan instanceof ProjectPlan project) {
            project.deriveColumnReferenceCounts(columnIds);
        }
    }

    /**
     * Lays out a chain of LIMITs again, bottom-up, after the input beneath the chain changed.
     */
    public static void deriveLimits(LogicalPlan plan) {
        if (plan instanceof LimitPlan limit) {
            deriveLimits(limit.getChild());
            limit.deriveOutput();
        }
    }

    /**
     * The column whose value in one row of its input the keyless aggregate returns: the argument of its only call when
     * that is a {@code min}, {@code max}, {@code first} or {@code last} of a direct column reference; null otherwise.
     */
    public static ColumnExpression endpointColumn(AggregatePlan aggregate) {
        if (aggregate.getGroupingExpressions().size() != 0 || aggregate.getAggregates().size() != 1) {
            return null;
        }
        final FunctionExpression call = aggregate.getAggregates().getQuick(0);
        return call.getArgumentCount() == 1 && call.argumentAt(0) instanceof ColumnExpression column && column.isDirectReference()
                && (isTimestampEndpointBackward(call) || Chars.equalsIgnoreCase(call.getName(), "min") || Chars.equalsIgnoreCase(call.getName(), "first"))
                ? column : null;
    }

    /**
     * The index of the join input whose output holds the column, or -1 when no input holds it.
     */
    public static int findJoinColumnSource(JoinPlan join, int columnId) {
        for (int i = 0, n = join.getInputs().size(); i < n; i++) {
            if (join.getInputs().getQuick(i).getSourceOutput().getColumnIndexById(columnId) >= 0) {
                return i;
            }
        }
        return -1;
    }

    /**
     * The index of the first grouping key that is a reference to the column, or -1 when none is.
     */
    public static int groupingColumnIndex(GroupingPlan grouping, int columnId) {
        final ObjList<BoundExpression> keys = grouping.getGroupingExpressions();
        for (int i = 0, n = keys.size(); i < n; i++) {
            if (keys.getQuick(i) instanceof ColumnExpression column && column.getColumnId() == columnId) {
                return i;
            }
        }
        return -1;
    }

    /**
     * True when a step of the join after its first input is a barrier.
     */
    public static boolean hasBarrierInput(JoinPlan join) {
        for (int i = 1, n = join.getInputs().size(); i < n; i++) {
            if (join.getInputs().getQuick(i).getJoinType().isBarrier()) {
                return true;
            }
        }
        return false;
    }

    /**
     * True when the plan, under any filters, is a join that declares its designated timestamp explicitly.
     */
    public static boolean hasExplicitJoinTimestamp(LogicalPlan plan) {
        plan = skipFilters(plan);
        return plan instanceof JoinPlan join && join.hasExplicitTimestamp();
    }

    /**
     * True when the value of an aggregate function is defined by the table order of its input rows.
     */
    public static boolean hasTableOrderAggregate(ObjList<FunctionExpression> aggregates) {
        for (int i = 0, n = aggregates.size(); i < n; i++) {
            if (aggregates.getQuick(i).getOverload().getOrderDependence() == FunctionFactory.OrderDependence.TABLE_ORDER) {
                return true;
            }
        }
        return false;
    }

    /**
     * True when the expression reads a column of an enclosing LATERAL's outer input.
     */
    public static boolean hasOuterColumn(BoundExpression expression) {
        return !expression.walk(OUTER_COLUMN_READS);
    }

    public static boolean hasRepeatedColumn(ProjectPlan project, IntHashSet columnIds) {
        columnIds.clear();
        final ObjList<BoundExpression> expressions = project.getExpressions();
        for (int i = 0, n = expressions.size(); i < n; i++) {
            if (expressions.getQuick(i) instanceof ColumnExpression column && !columnIds.add(column.getColumnId())) {
                return true;
            }
        }
        return false;
    }

    /**
     * Whether every column the expression reads, outer columns aside, has one {@link #columnSource} in the input.
     */
    public static boolean hasSingleColumnSource(BoundExpression expression, LogicalPlan input) {
        final LogicalPlan source = firstColumnSource(expression, input);
        return source == null || readsOnlySource(expression, input, source);
    }

    /**
     * True when the plan is a bounded sort under projections that compute nothing order-dependent, so a LIMIT
     * over the plan bounds the sort itself.
     */
    public static boolean hasSortUnderStableProjects(LogicalPlan plan) {
        while (plan instanceof ProjectPlan project) {
            if (!areOrderIndependent(project.getExpressions())) {
                return false;
            }
            plan = project.getChild();
        }
        return plan instanceof SortPlan sort && !sort.isMarkoutHorizon() && sort.isLimited();
    }

    /**
     * True when an aggregate function reads the designated timestamp of its input or requires its ascending order.
     */
    public static boolean hasTimestampAggregate(ObjList<FunctionExpression> aggregates) {
        for (int i = 0, n = aggregates.size(); i < n; i++) {
            if (isTimestampAggregate(aggregates.getQuick(i))) {
                return true;
            }
        }
        return false;
    }

    /**
     * True when the values of the aggregate do not depend on the order of its input rows: its grouping keys and the
     * arguments of its aggregate functions depend on no row order, and no aggregate function is order-sensitive.
     */
    public static boolean isAggregateOrderIndependent(AggregatePlan aggregate) {
        if (!areOrderIndependent(aggregate.getGroupingExpressions())) {
            return false;
        }
        for (int i = 0, n = aggregate.getAggregates().size(); i < n; i++) {
            if (!isOrderIndependentAggregate(aggregate.getAggregates().getQuick(i))) {
                return false;
            }
        }
        return true;
    }

    /**
     * True when the projection selects each of its columns as a distinct plain column reference.
     */
    public static boolean isColumnOnlyProjection(ProjectPlan project) {
        if (project.getDistinctColumnCount() >= 0) {
            return project.getDistinctColumnCount() == project.getExpressions().size();
        }
        for (int i = 0, n = project.getExpressions().size(); i < n; i++) {
            if (!(project.getExpressions().getQuick(i) instanceof ColumnExpression column)
                    || isColumnSelectedBefore(project, i, column.getColumnId())) {
                return false;
            }
        }
        return true;
    }

    public static boolean isColumnProjection(ProjectPlan project) {
        return !project.hasUpdateConversions() && !project.hasPrunedComputedColumns() && isColumnReferenceProjection(project);
    }

    /**
     * True when every expression of the projection is a column reference, cast or not.
     */
    public static boolean isColumnReferenceProjection(ProjectPlan project) {
        for (int i = 0, n = project.getExpressions().size(); i < n; i++) {
            if (!(project.getExpressions().getQuick(i) instanceof ColumnExpression)) {
                return false;
            }
        }
        return true;
    }

    /**
     * True when a column-reference projection selects the column again before expression {@code index}.
     */
    public static boolean isColumnSelectedBefore(ProjectPlan project, int index, int columnId) {
        for (int k = 0; k < index; k++) {
            if (((ColumnExpression) project.getExpressions().getQuick(k)).getColumnId() == columnId) {
                return true;
            }
        }
        return false;
    }

    /**
     * True when the projection computes a column: anything but plain, uncast column references.
     */
    public static boolean isComputedProjection(ProjectPlan project) {
        return project.hasUpdateConversions() || project.hasPrunedComputedColumns() || !isPlainColumnProjection(project);
    }

    /**
     * True when the predicate is a constant, whose value the generator reads once.
     */
    public static boolean isConstant(BoundExpression predicate) {
        return predicate instanceof ConstantExpression
                || !(predicate instanceof ColumnExpression) && (predicate.getFunctionFlags() & BoundExpression.CONSTANT) != 0;
    }

    /**
     * True when the aggregate only counts its input rows, which the generator does without reading them.
     */
    public static boolean isCount(AggregatePlan aggregate) {
        if (aggregate.getGroupingExpressions().size() != 0) {
            return false;
        }
        final ObjList<FunctionExpression> aggregates = aggregate.getAggregates();
        if (aggregates.size() == 0) {
            return true;
        }
        if (aggregates.size() != 1) {
            return false;
        }
        final FunctionExpression call = aggregates.getQuick(0);
        return call.getArgumentCount() == 0 && call.isAggregate() && SqlKeywords.isCountKeyword(call.getName());
    }

    /**
     * True when the join runs its first input first. Join ordering leads with another input only when the join
     * designates no timestamp or no part of the query requires it, and a join that leads with another input designates
     * no timestamp.
     */
    public static boolean isFirstInputLeading(JoinPlan join) {
        return orderedSteps(join).getQuick(0) == join.getInputs().getQuick(0);
    }

    /**
     * A dateadd unit literal that adds the same duration to every timestamp. Months and years clamp the day of
     * month, so January 29, 30 and 31 plus one month all land on the last day of February.
     */
    public static boolean isFixedDurationDateaddUnit(BoundExpression unit) {
        return unit instanceof ConstantExpression constant && constant.isLiteral() && constant.getDataType() == ColumnType.CHAR
                && switch ((char) constant.getLongValue()) {
            case 'n', 'u', 'U', 'T', 's', 'm', 'h', 'H', 'd', 'w' -> true;
            default -> false;
        };
    }

    /**
     * True when the window's results do not depend on its input's row order: every window orders its rows itself, and
     * none is a SUBSAMPLE keep flag, which selects among rows of equal timestamp in their input order.
     */
    public static boolean isInputOrderIgnored(WindowPlan window) {
        for (int i = 0, n = window.getSpecs().size(); i < n; i++) {
            final WindowSpec spec = window.getSpecs().getQuick(i);
            if (spec.getOrderByColumnIds().size() == 0 || spec.isSubsampleKeepFlag()) {
                return false;
            }
        }
        return true;
    }

    /**
     * An integer literal spelled in SQL, not folded from an expression.
     */
    public static boolean isIntegerLiteral(BoundExpression expression) {
        return expression instanceof ConstantExpression constant && constant.isLiteral() && constant.getSource() == null
                && (constant.getDataType() == ColumnType.INT || constant.getDataType() == ColumnType.LONG);
    }

    /**
     * Whether a join can key a master column of one type on a slave column of the other.
     */
    public static boolean isJoinKeyTypeCompatible(int masterType, int slaveType) {
        return masterType == slaveType
                || ColumnType.isSymbolOrStringOrVarchar(masterType) && ColumnType.isSymbolOrStringOrVarchar(slaveType)
                || ColumnType.isTimestamp(masterType) && ColumnType.isTimestamp(slaveType);
    }

    /**
     * The direct reference to the SYMBOL column.
     */
    public static boolean isKeyColumn(BoundExpression expression, int columnId) {
        return expression instanceof ColumnExpression column && column.isDirectReference() && column.getColumnId() == columnId
                && ColumnType.isSymbol(column.getDataType());
    }

    /**
     * An equality between the direct reference to the column and a key constant, see
     * {@link SymbolKeyExtractor#isKeyConstant}, or an OR of them on the same column.
     */
    public static boolean isKeyEqualityOr(BoundExpression expression, int columnId) {
        if (expression instanceof FunctionExpression call && call.isOr()) {
            return isKeyEqualityOr(call.argumentAt(0), columnId) && isKeyEqualityOr(call.argumentAt(1), columnId);
        }
        return isKeyEquality(expression, columnId);
    }

    /**
     * True when every step of the join emits rows in its master's order: inner, cross and left outer joins.
     */
    public static boolean isMasterOrderPreserved(JoinPlan join) {
        final ObjList<JoinInput> ordered = join.getOrderedInputs();
        for (int i = 1, n = ordered.size(); i < n; i++) {
            final JoinKind joinType = ordered.getQuick(i).getJoinType();
            if (joinType != JoinKind.INNER && joinType != JoinKind.CROSS && joinType != JoinKind.LEFT_OUTER) {
                return false;
            }
        }
        return true;
    }

    /**
     * True when every step of the join emits the rows each row of its master joins next to each other, in its master's
     * order, and they depend on that row alone: inner, cross, left outer, ASOF and LT joins.
     */
    public static boolean isMasterRowGrouped(JoinPlan join) {
        final ObjList<JoinInput> ordered = join.getOrderedInputs();
        for (int i = 1, n = ordered.size(); i < n; i++) {
            final JoinKind joinType = ordered.getQuick(i).getJoinType();
            if (joinType != JoinKind.INNER && joinType != JoinKind.CROSS && joinType != JoinKind.LEFT_OUTER
                    && joinType != JoinKind.ASOF && joinType != JoinKind.LT) {
                return false;
            }
        }
        return true;
    }

    /**
     * True when every step of the join emits at least one row for each row of its master, in its master's order: left
     * outer, ASOF and LT joins without a post-join filter.
     */
    public static boolean isMasterRowPreserved(JoinPlan join) {
        final ObjList<JoinInput> ordered = join.getOrderedInputs();
        for (int i = 1, n = ordered.size(); i < n; i++) {
            final JoinInput step = ordered.getQuick(i);
            final JoinKind joinType = step.getJoinType();
            if (joinType != JoinKind.LEFT_OUTER && joinType != JoinKind.ASOF && joinType != JoinKind.LT || step.getPostJoinFilter() != null) {
                return false;
            }
        }
        return true;
    }

    /**
     * True when the step can emit the columns of the input as NULL: the input's own step null-extends it, or a step
     * the join orders after the input null-extends its master.
     */
    public static boolean isNullingStep(JoinPlan join, JoinInput step, JoinInput input) {
        if (step == input) {
            return step.getJoinType().isSlaveNulling();
        }
        final ObjList<JoinInput> steps = orderedSteps(join);
        return step.getJoinType().isMasterNulling() && steps.indexOf(step) > steps.indexOf(input);
    }

    /**
     * True when the value the expression takes for a row does not depend on the order the rows are evaluated in:
     * every evaluation within one execution yields the same value for the same row. {@code now()} and a bind
     * variable vary across executions only, so they qualify; {@code rnd_*()} and {@code systimestamp()} do not.
     */
    public static boolean isOrderIndependent(BoundExpression expression) {
        return isStableWithinExecution(expression);
    }

    /**
     * True when the plan hands its input's rows on in their order, so a consumer that discards the order of the plan's
     * rows discards the order of its input's rows as well: a filter, or a projection that declares no designated
     * timestamp. What the plan computes does not count: a sub-query's ORDER BY orders the rows the sub-query returns,
     * not the evaluation of an {@code rnd_*()} call above it.
     */
    public static boolean isOrderTransparent(LogicalPlan plan) {
        return plan instanceof FilterPlan
                || plan instanceof ProjectPlan project && !project.hasTimestampDeclaration() && !project.hasUpdateConversions();
    }

    /**
     * True when every expression of the projection is a plain, uncast column reference.
     */
    public static boolean isPlainColumnProjection(ProjectPlan project) {
        for (int i = 0, n = project.getExpressions().size(); i < n; i++) {
            if (!(project.getExpressions().getQuick(i) instanceof ColumnExpression column) || column.isCast()) {
                return false;
            }
        }
        return true;
    }

    /**
     * Whether every evaluation of the plan within one execution yields the same multiset of rows, which is all the
     * value of a sub-query depends on: its consumers read a set or a single row. Unless {@code readColumnIds} is null,
     * only the computations of the columns it holds count, since column pruning removes the others.
     */
    public static boolean isResultStable(LogicalPlan plan, boolean isParallelGroupByEnabled, IntHashSet readColumnIds) {
        return (stability(plan, isParallelGroupByEnabled, readColumnIds) & RESULT_STABLE) != 0;
    }

    /**
     * Whether the join step matches the rows of its input by stable conditions alone and its input yields the same
     * rows on every evaluation, so that the step joins the same rows to the same master rows however often the join
     * evaluates it: a temporal or UNNEST step depends on the order of its inputs.
     */
    public static boolean isStableJoinStep(JoinInput step, boolean isParallelGroupByEnabled, IntHashSet readColumnIds) {
        return step.getJoinType().isRelational() && isStable(step.getKeyFilter()) && isStable(step.getOnResidual())
                && isStable(step.getPostJoinFilter()) && isResultStable(step.getInput(), isParallelGroupByEnabled, readColumnIds);
    }

    /**
     * Whether every evaluation of the expression within one execution yields the same value.
     */
    public static boolean isStableWithinExecution(BoundExpression expression) {
        return (expression.getFunctionFlags() & BoundExpression.STABLE_WITHIN_EXECUTION) != 0;
    }

    /**
     * True when the aggregate function reads the designated timestamp of its input or requires its ascending order.
     */
    public static boolean isTimestampAggregate(FunctionExpression aggregate) {
        return (aggregate.getFunctionFlags() & (BoundExpression.ASCENDING_TIMESTAMP | BoundExpression.TIMESTAMP_ARGUMENT)) != 0;
    }

    /**
     * True when the plan is a projection that only declares a designated timestamp over its unchanged input.
     */
    public static boolean isTimestampDeclarationOnly(LogicalPlan plan) {
        if (!(plan instanceof ProjectPlan project) || !project.hasTimestampDeclaration()) {
            return false;
        }
        final OutputSchema input = project.getChild().getOutput();
        if (project.getExpressions().size() != input.getColumnCount()) {
            return false;
        }
        for (int i = 0, n = project.getExpressions().size(); i < n; i++) {
            if (!(project.getExpressions().getQuick(i) instanceof ColumnExpression column) || column.isCast() || !column.isDirectReference()
                    || input.getColumnIndexById(column.getColumnId()) != i || input.getColumnType(i) != project.getOutput().getColumnType(i)
                    || !Chars.equals(input.getColumnName(i), project.getOutput().getColumnName(i))) {
                return false;
            }
        }
        return true;
    }

    /**
     * Whether the aggregate is a keyless {@code min}, {@code max}, {@code first} or {@code last} of the designated
     * timestamp of a table scan, optionally filtered: its value is the timestamp of the first row in
     * ascending ({@code min}, {@code first}) or descending ({@code max}, {@code last}) timestamp order.
     */
    public static boolean isTimestampEndpoint(AggregatePlan aggregate) {
        final ColumnExpression column = endpointColumn(aggregate);
        if (column == null) {
            return false;
        }
        final LogicalPlan input = aggregate.getChild();
        final LogicalPlan table = input instanceof FilterPlan filter ? filter.getChild() : input;
        return table instanceof ScanPlan scan && column.getColumnId() == scan.getNativeTimestampColumnId()
                && column.getColumnId() == input.getOutput().getTimestampColumnId();
    }

    /**
     * Whether a {@link #isTimestampEndpoint(AggregatePlan) timestamp endpoint} reads the last row in timestamp order.
     */
    public static boolean isTimestampEndpointBackward(FunctionExpression call) {
        return Chars.equalsIgnoreCase(call.getName(), "max") || Chars.equalsIgnoreCase(call.getName(), "last");
    }

    /**
     * Evaluating the expression twice may give two values: it reads a function or a sub-query whose value is not
     * stable within one execution.
     */
    public static boolean isVolatile(BoundExpression expression) {
        return !expression.walk(VOLATILE_NODES);
    }

    /**
     * The index of the join input whose output holds the column.
     */
    public static int joinColumnSource(JoinPlan join, int columnId) {
        final int source = findJoinColumnSource(join, columnId);
        if (source < 0) {
            throw new IllegalStateException("join column is outside its inputs");
        }
        return source;
    }

    /**
     * The column id of the designated timestamp that the first {@code stepCount} of the join's {@link #orderedSteps}
     * designate: the first input's while the join leads with it and none of those steps null-extends master rows, unless
     * the join declares its timestamp explicitly, or replaces the rows with a standalone UNNEST; -1 otherwise.
     */
    public static int joinTimestampColumnId(JoinPlan join, int stepCount) {
        if (!isFirstInputLeading(join)) {
            return -1;
        }
        final ObjList<JoinInput> steps = orderedSteps(join);
        for (int i = 1; i < stepCount; i++) {
            final JoinInput step = steps.getQuick(i);
            if (step.getUnnest() != null && step.getUnnest().isStandalone()
                    || step.getJoinType().isMasterNulling() && !join.hasExplicitTimestamp()) {
                return -1;
            }
        }
        return steps.getQuick(0).getSourceOutput().getTimestampColumnId();
    }

    /**
     * The ordered position of the last step of the join that can null-extend its master rows, or -1.
     */
    public static int lastMasterNullingStep(JoinPlan join) {
        final ObjList<JoinInput> steps = orderedSteps(join);
        for (int i = steps.size() - 1; i > 0; i--) {
            if (steps.getQuick(i).getJoinType().isMasterNulling()) {
                return i;
            }
        }
        return -1;
    }

    /**
     * The LONG value a LIMIT bound function returns for the constant.
     */
    public static long limitValue(ConstantExpression constant) {
        return switch (ColumnType.tagOf(constant.getDataType())) {
            case ColumnType.NULL -> Numbers.LONG_NULL;
            case ColumnType.INT -> Numbers.intToLong((int) constant.getLongValue());
            default -> constant.getLongValue();
        };
    }

    /**
     * Whether a LIMIT may evaluate to a negative count: one that is not a constant has no sign until execution.
     */
    public static boolean mayBeNegativeLimit(BoundExpression lo) {
        if (!(lo instanceof ConstantExpression constant)) {
            return true;
        }
        final long limit = limitValue(constant);
        return limit != Numbers.LONG_NULL && limit < 0;
    }

    /**
     * The operation {@code c * k}, {@code c + k} or {@code c - k}, either operand order, of an aggregate
     * {@code sum(c op k)} of the grouping, where {@code c} is an uncast BYTE, SHORT, INT or LONG column of the grouping's
     * input and {@code k} an integer literal; a projection over the grouping can compute such a sum from {@code sum(c)}
     * and {@code count(c)}, see {@link #sumNormalisingGrouping}. Null for any other aggregate.
     */
    public static FunctionExpression normalisableSumOperation(GroupingPlan grouping, FunctionExpression aggregate) {
        if (aggregate.getArgumentCount() != 1 || !Chars.equalsIgnoreCase(aggregate.getName(), "sum")
                || !(aggregate.argumentAt(0) instanceof FunctionExpression operation) || operation.getArgumentCount() != 2
                || operation.getName().length() != 1 || "*+-".indexOf(operation.getName().charAt(0)) < 0) {
            return null;
        }
        final boolean isColumnOnLeft = isIntegerLiteral(operation.argumentAt(1));
        if (!isColumnOnLeft && !isIntegerLiteral(operation.argumentAt(0))
                || !(operation.argumentAt(isColumnOnLeft ? 0 : 1) instanceof ColumnExpression column) || column.isCast()) {
            return null;
        }
        final OutputSchema input = grouping.getChild().getOutput();
        final int index = input.getColumnIndexById(column.getColumnId());
        if (index < 0) {
            return null;
        }
        final int type = input.getColumnType(index);
        return type == ColumnType.BYTE || type == ColumnType.SHORT || type == ColumnType.INT || type == ColumnType.LONG ? operation : null;
    }

    /**
     * The join's steps in the order the join runs them, or in binding order before the order is decided.
     */
    public static ObjList<JoinInput> orderedSteps(JoinPlan join) {
        return join.getOrderedInputs().size() > 0 ? join.getOrderedInputs() : join.getInputs();
    }

    /**
     * The column reference the projection selects as its output column {@code columnId}, or null when the projection
     * computes that column or does not select it. Every walk that follows a column down through projections takes
     * each step here and applies its own rule to the reference's cast and direct-reference flags.
     */
    public static ColumnExpression passedColumn(ProjectPlan project, int columnId) {
        final int index = project.getOutput().getColumnIndexById(columnId);
        return index >= 0 && project.getExpressions().getQuick(index) instanceof ColumnExpression column ? column : null;
    }

    public static int projectedColumnIndex(ProjectPlan project, int columnId) {
        final ObjList<BoundExpression> expressions = project.getExpressions();
        for (int i = 0, n = expressions.size(); i < n; i++) {
            if (expressions.getQuick(i) instanceof ColumnExpression column && column.getColumnId() == columnId) {
                return i;
            }
        }
        return -1;
    }

    /**
     * The input column the projection passes through as the given output column, or -1 when it computes that column.
     */
    public static int projectedSourceColumnId(ProjectPlan project, int columnId) {
        final ColumnExpression column = passedColumn(project, columnId);
        return column == null ? -1 : column.getColumnId();
    }

    /**
     * Index of the first expression that passes the column through without a cast.
     */
    public static int projectedUncastColumnIndex(ProjectPlan project, int columnId) {
        final ObjList<BoundExpression> expressions = project.getExpressions();
        for (int i = 0, n = expressions.size(); i < n; i++) {
            if (expressions.getQuick(i) instanceof ColumnExpression column && !column.isCast() && column.getColumnId() == columnId) {
                return i;
            }
        }
        return -1;
    }

    /**
     * The projection passes every key of the sort below it through unchanged.
     */
    public static boolean projectsSortKeys(ProjectPlan project, SortPlan sort) {
        for (int i = 0, n = sort.getColumnIds().size(); i < n; i++) {
            if (projectedUncastColumnIndex(project, sort.getColumnIds().getQuick(i)) < 0) {
                return false;
            }
        }
        return true;
    }

    /**
     * True when the expression reads a column of its input.
     */
    public static boolean readsColumn(BoundExpression expression) {
        return expression != null && !expression.walk(COLUMN_READS);
    }

    /**
     * True when the expression reads the column.
     */
    public static boolean readsColumn(BoundExpression expression, int columnId) {
        if (expression instanceof ColumnExpression column) {
            return column.getColumnId() == columnId;
        }
        if (expression instanceof FunctionExpression call) {
            for (int i = 0, n = call.getArgumentCount(); i < n; i++) {
                if (readsColumn(call.argumentAt(i), columnId)) {
                    return true;
                }
            }
        }
        return false;
    }

    public static boolean readsOnly(BoundExpression expression, OutputSchema output) {
        if (expression instanceof ColumnExpression column) {
            return output.getColumnIndexById(column.getColumnId()) >= 0;
        }
        if (expression instanceof FunctionExpression call) {
            for (int i = 0, n = call.getArgumentCount(); i < n; i++) {
                if (!readsOnly(call.argumentAt(i), output)) {
                    return false;
                }
            }
        }
        return !(expression instanceof CursorExpression);
    }

    /**
     * True when every column the expression reads is a key of the map and the expression reads no sub-query.
     */
    public static boolean readsOnly(BoundExpression expression, IntIntHashMap columnIds) {
        if (expression instanceof ColumnExpression column) {
            return columnIds.keyIndex(column.getColumnId()) < 0;
        }
        if (expression instanceof FunctionExpression call) {
            for (int i = 0, n = call.getArgumentCount(); i < n; i++) {
                if (!readsOnly(call.argumentAt(i), columnIds)) {
                    return false;
                }
            }
        }
        return !(expression instanceof CursorExpression);
    }

    /**
     * True when the call reads at least one outer column and no column of its own input.
     */
    public static boolean readsOnlyOuterColumns(FunctionExpression call) {
        return hasOuterColumn(call) && !readsColumn(call);
    }

    /**
     * The table scan the plan is, or the one directly under it when it is a filter; null otherwise.
     */
    public static ScanPlan scanThroughFilter(LogicalPlan plan) {
        return (plan instanceof FilterPlan filter ? filter.getChild() : plan) instanceof ScanPlan scan ? scan : null;
    }

    /**
     * The value a comparison of a column with itself folds to: {@link #SELF_COMPARISON_TRUE} for {@code =},
     * {@link #SELF_COMPARISON_FALSE} for {@code !=}, {@code <>}, {@code >} and {@code <}, otherwise
     * {@link #SELF_COMPARISON_NONE}.
     */
    public static int selfComparison(FunctionExpression call) {
        final ComparisonOperator operator = ComparisonOperator.of(call.getName());
        if (operator != null && call.getArgumentCount() == 2 && call.argumentAt(0) instanceof ColumnExpression left
                && call.argumentAt(1) instanceof ColumnExpression right && left.isDirectReference() && right.isDirectReference()
                && left.getColumnId() == right.getColumnId()) {
            return switch (operator) {
                case EQUAL -> SELF_COMPARISON_TRUE;
                case NOT_EQUAL, LESS, GREATER -> SELF_COMPARISON_FALSE;
                case LESS_OR_EQUAL, GREATER_OR_EQUAL -> SELF_COMPARISON_NONE;
            };
        }
        return SELF_COMPARISON_NONE;
    }

    public static int setTimestampIndex(LogicalPlan plan) {
        final int timestampIndex = plan.getOutput().getTimestampIndex();
        if (timestampIndex >= 0) {
            return timestampIndex;
        }
        if (plan instanceof SetOperationPlan) {
            final int leftIndex = setTimestampIndex(plan.inputAt(0));
            return leftIndex >= 0 ? leftIndex : setTimestampIndex(plan.inputAt(1));
        }
        return -1;
    }

    public static LogicalPlan skipFilters(LogicalPlan plan) {
        while (plan instanceof FilterPlan) {
            plan = plan.inputAt(0);
        }
        return plan;
    }

    public static LogicalPlan skipProjects(LogicalPlan plan) {
        while (plan instanceof ProjectPlan) {
            plan = plan.inputAt(0);
        }
        return plan;
    }

    public static LogicalPlan skipProjectsAndFilters(LogicalPlan plan) {
        while (plan instanceof ProjectPlan || plan instanceof FilterPlan) {
            plan = plan.inputAt(0);
        }
        return plan;
    }

    /**
     * Skips column projections that keep every input column at its position and type: only the names differ.
     */
    public static LogicalPlan skipRenames(LogicalPlan plan) {
        while (plan instanceof ProjectPlan project && !project.hasTimestampDeclaration() && !project.hasUpdateConversions()) {
            final OutputSchema input = project.getChild().getOutput();
            final OutputSchema output = project.getOutput();
            if (output.getColumnCount() != input.getColumnCount() || output.getTimestampIndex() != input.getTimestampIndex()) {
                return plan;
            }
            for (int i = 0, n = output.getColumnCount(); i < n; i++) {
                if (!(project.getExpressions().getQuick(i) instanceof ColumnExpression column) || !column.isDirectReference() || column.isCast()
                        || column.getColumnId() != input.getColumnId(i) || output.getColumnType(i) != input.getColumnType(i)) {
                    return plan;
                }
            }
            plan = project.getChild();
        }
        return plan;
    }

    /**
     * Moves the sort of the grouping's rows above the projection that reads them, once the projection computes in
     * place of keys it passed through: the sort keys become the projection columns at the indexes
     * {@link #collectSortKeyIndexes} recorded before the projection changed. Returns the sort.
     */
    public static SortPlan sortAboveProjection(SortPlan sort, ProjectPlan project, GroupingPlan grouping, IntList sortKeyIndexes) {
        final OutputSchema output = project.getOutput();
        for (int i = 0, n = sortKeyIndexes.size(); i < n; i++) {
            sort.getColumnIds().setQuick(i, output.getColumnId(sortKeyIndexes.getQuick(i)));
        }
        project.replaceInput(0, grouping);
        output.setTimestampIndex(projectedColumnIndex(project, grouping.getOutput().getTimestampColumnId()));
        sort.replaceInput(0, project);
        return sort;
    }

    /**
     * The grouping the projection reads directly, or through a sort of its rows whose keys it passes through
     * unchanged, when the grouping reads its tables directly and fills no gaps itself: the grouping whose
     * {@link #normalisableSumOperation normalisable sums} the projection computes from sums and counts of their
     * columns. Null otherwise.
     */
    public static GroupingPlan sumNormalisingGrouping(ProjectPlan project) {
        final GroupingPlan grouping;
        if (project.getChild() instanceof GroupingPlan direct) {
            grouping = direct;
        } else if (project.getChild() instanceof SortPlan sort && sort.getChild() instanceof GroupingPlan sorted && projectsSortKeys(project, sort)) {
            grouping = sorted;
        } else {
            return null;
        }
        return grouping.hasDirectTableInput() && !(grouping instanceof SampleByPlan sampleBy && sampleBy.getFillModes().size() > 0)
                ? grouping : null;
    }

    /**
     * The column of the plan that holds the designated timestamp of the rows a sort by a key that is not a timestamp
     * reorders: the sort designates none, and the column projections and filters over it pass that column on; -1
     * otherwise.
     */
    public static int unorderedTimestampColumnId(LogicalPlan plan) {
        return switch (plan) {
            case SortPlan sort when sort.getOutput().getTimestampIndex() < 0 ->
                    sort.getChild().getOutput().getTimestampColumnId();
            case FilterPlan filter -> unorderedTimestampColumnId(filter.getChild());
            case ProjectPlan project when isColumnProjection(project) -> {
                final int index = projectedColumnIndex(project, unorderedTimestampColumnId(project.getChild()));
                yield index < 0 ? -1 : project.getOutput().getColumnId(index);
            }
            default -> -1;
        };
    }

    /**
     * The plan under the column-only projections over it, see {@link #isColumnOnlyProjection}.
     */
    public static LogicalPlan unwrapColumnProjections(LogicalPlan plan) {
        while (plan instanceof ProjectPlan project && isColumnOnlyProjection(project)) {
            plan = plan.inputAt(0);
        }
        return plan;
    }

    /**
     * The type an UPDATE stores a value of type {@code type} as in a column of type {@code targetType}: the target
     * type for a built-in widening cast, except into a timestamp column from a type with a timestamp precision of its
     * own (DATE, text), which the update converts from; the value's own type otherwise.
     */
    public static int updateColumnType(int type, int targetType) {
        return targetType < 0 || !ColumnType.isBuiltInWideningCast(type, targetType)
                || ColumnType.isTimestamp(targetType) && ColumnType.getTimestampType(type) != ColumnType.UNDEFINED ? type : targetType;
    }

    /**
     * True when no expression depends on the order of the rows it is evaluated over, see {@link #isOrderIndependent}.
     */
    private static boolean areOrderIndependent(ObjList<? extends BoundExpression> expressions) {
        for (int i = 0, n = expressions.size(); i < n; i++) {
            if (!isOrderIndependent(expressions.getQuick(i))) {
                return false;
            }
        }
        return true;
    }

    private static boolean areStable(ObjList<? extends BoundExpression> expressions) {
        for (int i = 0, n = expressions.size(); i < n; i++) {
            if (!isStable(expressions.getQuick(i))) {
                return false;
            }
        }
        return true;
    }

    /**
     * True when every expression that computes a read column is stable: expression {@code i} computes the output column
     * at {@code firstIndex + i}, and every column is read when {@code readColumnIds} is null.
     */
    private static boolean areStable(ObjList<? extends BoundExpression> expressions, OutputSchema output, int firstIndex, IntHashSet readColumnIds) {
        for (int i = 0, n = expressions.size(); i < n; i++) {
            if ((readColumnIds == null || readColumnIds.contains(output.getColumnId(firstIndex + i))) && !isStable(expressions.getQuick(i))) {
                return false;
            }
        }
        return true;
    }

    /**
     * Walks the left-deep chain of set operations once: every operation keeps the column type and either declares
     * no timestamp or declares the column, and the bottom operation resolves its timestamp to the column.
     */
    private static boolean canPushSetTimestampThrough(SetOperationPlan operation, int columnIndex) {
        final int type = operation.getOutput().getColumnType(columnIndex);
        while (true) {
            final LogicalPlan left = operation.getLeft();
            final LogicalPlan right = operation.getRight();
            if (left.getOutput().getColumnType(columnIndex) != type || right.getOutput().getColumnType(columnIndex) != type
                    || !canPushSetTimestamp(right, columnIndex)) {
                return false;
            }
            if (!(left instanceof SetOperationPlan next)) {
                return setTimestampIndex(operation) == columnIndex && canPushSetTimestamp(left, columnIndex);
            }
            final int timestampIndex = operation.getOutput().getTimestampIndex();
            if (timestampIndex >= 0 && timestampIndex != columnIndex) {
                return false;
            }
            operation = next;
        }
    }

    private static boolean canPushSetTimestampThrough(UnaryPlan plan, int columnIndex) {
        final LogicalPlan input = plan.getChild();
        final int inputIndex = input.getOutput().getColumnIndexById(plan.getOutput().getColumnId(columnIndex));
        return inputIndex >= 0 && canPushSetTimestamp(input, inputIndex);
    }

    private static LogicalPlan firstColumnSource(BoundExpression expression, LogicalPlan input) {
        if (expression instanceof ColumnExpression column) {
            return columnSource(input, column.getColumnId());
        }
        if (expression instanceof FunctionExpression call) {
            for (int i = 0, n = call.getArgumentCount(); i < n; i++) {
                final LogicalPlan source = firstColumnSource(call.argumentAt(i), input);
                if (source != null) {
                    return source;
                }
            }
        }
        return null;
    }

    private static int groupingStability(AggregatePlan aggregate, boolean isParallelGroupByEnabled, IntHashSet readColumnIds) {
        if (aggregate.getSharedSource() != null || stability(aggregate.getChild(), isParallelGroupByEnabled, readColumnIds) != SEQUENCE_STABLE
                || !areStable(aggregate.getGroupingExpressions())
                || !areStable(aggregate.getAggregates(), aggregate.getOutput(), aggregate.getGroupingExpressions().size(), readColumnIds)) {
            return 0;
        }
        return aggregate.getGroupingExpressions().size() == 0 || !isParallelGroupByEnabled ? SEQUENCE_STABLE : RESULT_STABLE;
    }

    /**
     * An equality between the direct reference to the column and a key literal.
     */
    private static boolean isKeyEquality(BoundExpression expression, int columnId) {
        return expression instanceof FunctionExpression call && "=".equals(call.getName()) && call.getArgumentCount() == 2
                && (isKeyColumn(call.argumentAt(0), columnId) && SymbolKeyExtractor.isKeyConstant(call.argumentAt(1))
                || isKeyColumn(call.argumentAt(1), columnId) && SymbolKeyExtractor.isKeyConstant(call.argumentAt(0)));
    }

    /**
     * True when the value of the aggregate function does not depend on the order of its input rows: the function
     * has no order dependence, and its arguments depend on no row order.
     */
    private static boolean isOrderIndependentAggregate(FunctionExpression aggregate) {
        return aggregate.isAggregate() && aggregate.getOverload().getOrderDependence() == FunctionFactory.OrderDependence.NONE
                && areOrderIndependent(aggregate.getArguments());
    }

    private static boolean isStable(BoundExpression expression) {
        return expression == null || isStableWithinExecution(expression);
    }

    private static boolean readsOnlySource(BoundExpression expression, LogicalPlan input, LogicalPlan source) {
        if (expression instanceof ColumnExpression column) {
            return columnSource(input, column.getColumnId()) == source;
        }
        if (expression instanceof FunctionExpression call) {
            for (int i = 0, n = call.getArgumentCount(); i < n; i++) {
                if (!readsOnlySource(call.argumentAt(i), input, source)) {
                    return false;
                }
            }
        }
        return true;
    }

    private static int stability(LogicalPlan plan, boolean isParallelGroupByEnabled, IntHashSet readColumnIds) {
        return switch (plan) {
            case ScanPlan scan -> scan.getTableToken().isLiveView() ? 0 : SEQUENCE_STABLE;
            case FunctionSourcePlan source -> source.isDeterministic() ? SEQUENCE_STABLE : 0;
            case FilterPlan filter ->
                    isStable(filter.getPredicate()) ? stability(filter.getChild(), isParallelGroupByEnabled, readColumnIds) : 0;
            case ProjectPlan project -> areStable(project.getExpressions(), project.getOutput(), 0, readColumnIds)
                    ? stability(project.getChild(), isParallelGroupByEnabled, readColumnIds) : 0;
            case SortPlan sort ->
                    sort.isMarkoutHorizon() ? 0 : stability(sort.getChild(), isParallelGroupByEnabled, readColumnIds);
            case LimitPlan limit -> isStable(limit.getLo()) && isStable(limit.getHi())
                    && stability(limit.getChild(), isParallelGroupByEnabled, readColumnIds) == SEQUENCE_STABLE ? SEQUENCE_STABLE : 0;
            case AggregatePlan aggregate -> groupingStability(aggregate, isParallelGroupByEnabled, readColumnIds);
            case WindowPlan window -> windowStability(window, isParallelGroupByEnabled, readColumnIds);
            case DistinctPlan distinct ->
                    stability(distinct.getChild(), isParallelGroupByEnabled, readColumnIds) & RESULT_STABLE;
            case SetOperationPlan operation -> stability(operation.getLeft(), isParallelGroupByEnabled, readColumnIds)
                    & stability(operation.getRight(), isParallelGroupByEnabled, readColumnIds);
            default -> 0;
        };
    }

    /**
     * A window that computes no read column has the stability of its input. A read window result depends on the order
     * of the input rows, so the window is stable only when its input delivers the same rows in the same order and every
     * read window function and its partition keys are stable.
     */
    private static int windowStability(WindowPlan window, boolean isParallelGroupByEnabled, IntHashSet readColumnIds) {
        final int inputStability = stability(window.getChild(), isParallelGroupByEnabled, readColumnIds);
        boolean isComputing = false;
        for (int i = 0, n = window.getFunctions().size(); i < n; i++) {
            if (readColumnIds == null || readColumnIds.contains(window.getFunctionColumnIds().getQuick(i))) {
                if (!isStable(window.getFunctions().getQuick(i)) || !areStable(window.getSpecs().getQuick(i).getPartitionBy())) {
                    return 0;
                }
                isComputing = true;
            }
        }
        return isComputing && inputStability != SEQUENCE_STABLE ? 0 : inputStability;
    }

    /**
     * Whether the plan reads a table function over a data source outside the database.
     */
    static boolean hasExternalDataSource(LogicalPlan plan) {
        return !plan.walkTopDown(EXTERNAL_SOURCES);
    }
}
