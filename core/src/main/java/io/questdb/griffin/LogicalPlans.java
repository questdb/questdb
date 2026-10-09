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
import io.questdb.cairo.GeoHashes;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.griffin.engine.groupby.vect.VectorAggregateConstructors;
import io.questdb.griffin.engine.groupby.vect.VectorAggregateFunctionConstructor;
import io.questdb.griffin.plan.logical.AggregatePlan;
import io.questdb.griffin.plan.logical.BindVariableExpression;
import io.questdb.griffin.plan.logical.BoundExpression;
import io.questdb.griffin.plan.logical.ColumnExpression;
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
import io.questdb.griffin.plan.logical.PhysicalProperties;
import io.questdb.griffin.plan.logical.PlanVisitor;
import io.questdb.griffin.plan.logical.ProjectPlan;
import io.questdb.griffin.plan.logical.ScanPlan;
import io.questdb.griffin.plan.logical.SetOperationPlan;
import io.questdb.griffin.plan.logical.SortPlan;
import io.questdb.griffin.plan.logical.TreeWalk;
import io.questdb.griffin.plan.logical.TypeExpression;
import io.questdb.griffin.plan.logical.UnaryPlan;
import io.questdb.griffin.plan.logical.WindowJoinPlan;
import io.questdb.griffin.plan.logical.WindowPlan;
import io.questdb.std.Chars;
import io.questdb.std.IntList;
import io.questdb.std.LongList;
import io.questdb.std.NumericException;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import org.jetbrains.annotations.Nullable;

/**
 * Stateless plan-node predicates, walkers and plan-property utilities shared by the binder, the optimiser and the plan generator.
 */
public final class LogicalPlans {
    public static final int SELF_COMPARISON_FALSE = 0;
    public static final int SELF_COMPARISON_NONE = -1;
    public static final int SELF_COMPARISON_TRUE = 1;
    private static final int ORDER_STABLE = 2;
    private static final int RESULT_STABLE = 1;
    private static final int SEQUENCE_STABLE = RESULT_STABLE | ORDER_STABLE;
    private static final ExpressionVisitor COLUMN_READS = expression -> expression instanceof ColumnExpression ? TreeWalk.STOP : TreeWalk.CONTINUE;
    private static final PlanVisitor EXTERNAL_SOURCES = plan -> plan instanceof FunctionSourcePlan source && source.hasExternalDataSource()
            ? TreeWalk.STOP : TreeWalk.CONTINUE;
    private static final ExpressionVisitor OUTER_COLUMN_READS = expression -> expression instanceof OuterColumnExpression ? TreeWalk.STOP : TreeWalk.CONTINUE;
    private static final ExpressionVisitor VOLATILE_NODES = expression -> !isStableWithinExecution(expression)
            || !(expression instanceof FunctionExpression || expression instanceof ColumnExpression || expression instanceof ConstantExpression
            || expression instanceof CursorExpression || expression instanceof BindVariableExpression || expression instanceof TypeExpression)
            ? TreeWalk.STOP : TreeWalk.CONTINUE;

    private LogicalPlans() {
    }

    public static boolean canPushJoinFilter(JoinPlan join, int source, int lastInput) {
        if (join.getInputs().getQuick(source).getInput() == null) {
            return false;
        }
        final ObjList<JoinInput> ordered = join.getOrderedInputs();
        final int sourcePosition = ordered.indexOf(join.getInputs().getQuick(source));
        if (sourcePosition < 0 || sourcePosition > lastInput) {
            return false;
        }
        if (sourcePosition > 0) {
            switch (ordered.getQuick(sourcePosition).getJoinType()) {
                case LEFT_OUTER, RIGHT_OUTER, FULL_OUTER, ASOF, LT, SPLICE -> {
                    return false;
                }
                default -> {
                }
            }
        }
        for (int i = sourcePosition + 1; i <= lastInput; i++) {
            if (ordered.getQuick(i).getJoinType().isMasterNulling()) {
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
                final LogicalPlan input = project.getInput();
                final int inputIndex = input.getOutput().getColumnIndexById(column.getColumnId());
                yield inputIndex >= 0 && input.getOutput().getColumnType(inputIndex) == output.getColumnType(columnIndex)
                        && canPushSetTimestamp(input, inputIndex);
            }
            case FilterPlan filter ->
                    isOrderIndependent(filter.getPredicate()) && canPushSetTimestampThrough(filter, columnIndex);
            case SortPlan sort -> canPushSetTimestampThrough(sort, columnIndex);
            case SetOperationPlan operation -> {
                final int type = output.getColumnType(columnIndex);
                yield setTimestampIndex(operation) == columnIndex
                        && operation.getLeft().getOutput().getColumnType(columnIndex) == type
                        && operation.getRight().getOutput().getColumnType(columnIndex) == type
                        && canPushSetTimestamp(operation.getLeft(), columnIndex)
                        && canPushSetTimestamp(operation.getRight(), columnIndex);
            }
            default -> false;
        };
    }

    public static void collectConjuncts(BoundExpression predicate, ObjList<BoundExpression> sink) {
        if (predicate instanceof FunctionExpression call && call.getArgumentCount() == 2 && call.isAnd()) {
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
     * The plan whose own expression or scan defines the column, followed through column-only projections, filters,
     * sorts and joins.
     */
    public static LogicalPlan columnSource(LogicalPlan plan, int id) {
        while (true) {
            switch (plan) {
                case ProjectPlan project -> {
                    final int index = project.getOutput().getColumnIndexById(id);
                    if (index < 0) {
                        throw new IllegalStateException("predicate column is outside its projection");
                    }
                    if (!(project.getExpressions().getQuick(index) instanceof ColumnExpression column)) {
                        return plan;
                    }
                    id = column.getColumnId();
                    plan = project.getInput();
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
     * Lays out a chain of LIMITs again, bottom-up, after the input beneath the chain changed.
     */
    /**
     * The value of a constant predicate: 1 for true, 0 for false, -1 when the predicate is not a constant. Binding
     * folds every constant predicate to a literal, which {@code PlanVerifier} checks.
     */
    public static int constantTruth(BoundExpression predicate) {
        return predicate instanceof ConstantExpression constant ? constant.getLongValue() != 0 ? 1 : 0 : -1;
    }

    public static void deriveLimits(LogicalPlan plan) {
        if (plan instanceof LimitPlan limit) {
            deriveLimits(limit.getInput());
            limit.deriveOutput();
        }
    }

    /**
     * The first filter under the projections of the input, past the filters that are constant true.
     */
    public static FilterPlan firstFilter(LogicalPlan input) {
        while (true) {
            if (input instanceof ProjectPlan project) {
                input = project.getInput();
            } else if (input instanceof FilterPlan filter) {
                if (!(filter.getPredicate() instanceof ConstantExpression constant) || constant.getLongValue() == 0) {
                    return filter;
                }
                input = filter.getInput();
            } else {
                return null;
            }
        }
    }

    /**
     * The plan the generator builds the factory of {@code plan} from: a projection that passes its input through
     * and a filter it folds to true build none.
     */
    public static LogicalPlan generatedPlan(LogicalPlan plan) {
        while (true) {
            if (plan instanceof ProjectPlan project && isIdentityProjection(project)) {
                plan = project.getInput();
            } else if (plan instanceof FilterPlan filter && !isFusedFilter(filter)
                    && filter.getPredicate() instanceof ConstantExpression constant && constant.getLongValue() != 0) {
                plan = filter.getInput();
            } else {
                return plan;
            }
        }
    }

    /**
     * The window the generator builds the factory of {@code plan} from: the window itself, or the window under a
     * projection of its output, which the window factory builds; null otherwise.
     */
    public static WindowPlan generatedWindow(LogicalPlan plan) {
        if (plan instanceof WindowPlan window) {
            return window;
        }
        return plan instanceof ProjectPlan project && project.getInput() instanceof WindowPlan window
                && isWindowOutputProjection(project, window) ? window : null;
    }

    /**
     * The name the generator's factory gives column {@code index} of the plan: a count aggregate, read through
     * filters, names its column {@code count} under any spelling of that name.
     */
    public static CharSequence factoryColumnName(LogicalPlan plan, int index) {
        final CharSequence name = plan.getOutput().getColumnName(index);
        return skipFilters(plan) instanceof AggregatePlan aggregate && isCount(aggregate) && SqlKeywords.isCountKeyword(name)
                ? "count" : name;
    }

    /**
     * True when the order a sort requests reaches a filtered table scan, directly, through a window, or as
     * the master of a join that preserves master order.
     */
    public static boolean hasAdvisedInput(LogicalPlan plan) {
        plan = skipProjects(plan);
        if (plan instanceof WindowPlan) {
            return hasNativeFilterInput(plan.inputAt(0));
        }
        return hasNativeFilterInput(plan) || hasOrderedJoinMasterInput(plan);
    }

    /**
     * True when a step of the join after its first input is a barrier and none is a RIGHT or FULL join.
     */
    public static boolean hasBarrierInput(JoinPlan join) {
        boolean hasBarrier = false;
        for (int i = 1, n = join.getInputs().size(); i < n; i++) {
            final JoinKind type = join.getInputs().getQuick(i).getJoinType();
            if (type == JoinKind.RIGHT_OUTER || type == JoinKind.FULL_OUTER) {
                return false;
            }
            hasBarrier |= type.isBarrier();
        }
        return hasBarrier;
    }

    /**
     * True when the expression reads a column of an enclosing LATERAL's outer input.
     */
    /**
     * True when the plan, under any filters, is a join that declares its designated timestamp explicitly.
     */
    public static boolean hasExplicitJoinTimestamp(LogicalPlan plan) {
        plan = skipFilters(plan);
        return plan instanceof JoinPlan join && join.hasExplicitTimestamp();
    }

    /**
     * True when the plan, under any projections, filters a table scan, so the scan's access path serves the filter.
     */
    public static boolean hasNativeFilterInput(LogicalPlan plan) {
        plan = skipProjects(plan);
        return plan instanceof FilterPlan filter && filter.getInput() instanceof ScanPlan;
    }

    /**
     * True when the plan, under any projections, is a join that keeps its master's order over a filtered table scan.
     */
    public static boolean hasOrderedJoinMasterInput(LogicalPlan plan) {
        plan = skipProjects(plan);
        return plan instanceof JoinPlan join && isMasterOrderPreserved(join)
                && hasNativeFilterInput(join.getOrderedInputs().getQuick(0).getInput());
    }

    public static boolean hasOuterColumn(BoundExpression expression) {
        return !expression.walk(OUTER_COLUMN_READS);
    }

    public static boolean hasRepeatedColumn(ProjectPlan project) {
        final ObjList<BoundExpression> expressions = project.getExpressions();
        for (int i = 1, n = expressions.size(); i < n; i++) {
            if (expressions.getQuick(i) instanceof ColumnExpression column) {
                for (int k = 0; k < i; k++) {
                    if (expressions.getQuick(k) instanceof ColumnExpression other && other.getColumnId() == column.getColumnId()) {
                        return true;
                    }
                }
            }
        }
        return false;
    }

    public static boolean hasSingleColumnSource(BoundExpression expression, LogicalPlan input) {
        final LogicalPlan source = firstColumnSource(expression, input);
        return source == null || readsOnlySource(expression, input, source);
    }

    /**
     * Whether every column the expression reads, outer columns aside, has one {@link #columnSource} in the input.
     */
    /**
     * True when the plan is a bounded sort under projections that compute nothing order-dependent, so a LIMIT
     * over the plan bounds the sort itself.
     */
    public static boolean hasSortUnderStableProjects(LogicalPlan plan) {
        while (plan instanceof ProjectPlan project) {
            for (int i = 0, n = project.getExpressions().size(); i < n; i++) {
                if (!isOrderIndependent(project.getExpressions().getQuick(i))) {
                    return false;
                }
            }
            plan = project.getInput();
        }
        return plan instanceof SortPlan sort && !sort.isMarkoutHorizon() && sort.isLimited();
    }

    /**
     * Whether the aggregate has a single vector key and a vector implementation of every aggregate call.
     */
    public static boolean hasVectorShape(AggregatePlan plan) {
        final ObjList<BoundExpression> keys = plan.getGroupingExpressions();
        if (keys.size() != 1 || vectorKey(keys.getQuick(0)) == null) {
            return false;
        }
        for (int i = 0, n = plan.getAggregates().size(); i < n; i++) {
            if (vectorConstructor(plan.getAggregates().getQuick(i)) == null) {
                return false;
            }
        }
        return true;
    }

    /**
     * True when the projection selects each of its columns as a distinct plain column reference.
     */
    public static boolean isColumnOnlyProjection(ProjectPlan project) {
        for (int i = 0, n = project.getExpressions().size(); i < n; i++) {
            if (!(project.getExpressions().getQuick(i) instanceof ColumnExpression column)
                    || isColumnSelectedBefore(project, i, column.getColumnId())) {
                return false;
            }
        }
        return true;
    }

    public static boolean isColumnProjection(ProjectPlan project) {
        if (project.hasUpdateConversions() || project.hasPrunedComputedColumns()) {
            return false;
        }
        for (int i = 0, n = project.getExpressions().size(); i < n; i++) {
            if (!(project.getExpressions().getQuick(i) instanceof ColumnExpression)) {
                return false;
            }
        }
        return true;
    }

    /**
     * True when the projection computes a column: anything but plain, uncast column references.
     */
    public static boolean isComputedProjection(ProjectPlan project) {
        if (project.hasUpdateConversions() || project.hasPrunedComputedColumns()) {
            return true;
        }
        for (int i = 0, n = project.getExpressions().size(); i < n; i++) {
            if (!(project.getExpressions().getQuick(i) instanceof ColumnExpression column) || column.isCast()) {
                return true;
            }
        }
        return false;
    }

    /**
     * An integer literal spelled in SQL, not folded from an expression.
     */
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
     * True when the generator builds the filter into the factory of the table scan under it.
     */
    public static boolean isFusedFilter(FilterPlan filter) {
        return filter.getInput() instanceof ScanPlan scan && !scan.isWalClientUpdate();
    }

    /**
     * True when the generator builds no factory for the projection over the factory of its input, see
     * {@link #isIdentityProjection(ProjectPlan, RecordMetadata, int, int)}, whose columns order planning takes to be
     * the input's output schema. Over a join of several inputs, or a filter over one, the factory names its columns
     * qualifier.name, so the generator builds a selection where this answers true; the planner reads the answer only
     * to look through the projection for a filter, a scan or page frames, which such a join exposes no more than the
     * selection does.
     */
    public static boolean isIdentityProjection(ProjectPlan project) {
        return isIdentityProjection(project, null, PhysicalProperties.timestampIndex(project),
                PhysicalProperties.timestampIndex(project.getInput()));
    }

    /**
     * True when the generator builds no factory for the projection: it selects every input column, in order, under
     * the name and type the input's factory gives it, {@code layout}, or the input's output schema when null, and
     * designates the timestamp the input's factory designates. A SELECT list over GROUP BY keeps the key spelling when
     * it only changes the name case.
     */
    public static boolean isIdentityProjection(ProjectPlan project, @Nullable RecordMetadata layout, int timestampIndex, int inputTimestampIndex) {
        final LogicalPlan input = project.getInput();
        if (isComputedProjection(project) || input instanceof WindowPlan window && isWindowOutputProjection(project, window)
                || input instanceof WindowJoinPlan && isColumnOnlyProjection(project)) {
            return false;
        }
        final OutputSchema output = project.getOutput();
        final OutputSchema inputOutput = input.getOutput();
        if (output.getColumnCount() != (layout == null ? inputOutput.getColumnCount() : layout.getColumnCount())
                || timestampIndex != inputTimestampIndex) {
            return false;
        }
        final boolean isKeySpellingKept = skipFilters(input) instanceof AggregatePlan aggregate
                && aggregate.hasKeySpellingKept() && !(aggregate.getInput() instanceof HorizonJoinPlan);
        for (int i = 0, n = project.getExpressions().size(); i < n; i++) {
            final ColumnExpression column = (ColumnExpression) project.getExpressions().getQuick(i);
            final CharSequence name = output.getColumnName(i);
            final CharSequence inputName = layout == null ? factoryColumnName(input, i) : layout.getColumnName(i);
            if (inputOutput.getColumnIndexById(column.getColumnId()) != i
                    || output.getColumnType(i) != (layout == null ? inputOutput.getColumnType(i) : layout.getColumnType(i))
                    || !(isKeySpellingKept ? Chars.equalsIgnoreCase(name, inputName) : Chars.equals(name, inputName))) {
                return false;
            }
        }
        return true;
    }

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

    public static boolean isOrderIndependent(BoundExpression predicate) {
        return isStableWithinExecution(predicate) && (predicate.getFunctionFlags() & BoundExpression.NON_DETERMINISTIC) == 0;
    }

    /**
     * True when the generator builds a projection factory for the plan, which a parallel top-K can build over itself.
     */
    public static boolean isPeelableProjection(LogicalPlan plan) {
        if (!(plan instanceof ProjectPlan project)) {
            return false;
        }
        final LogicalPlan input = project.getInput();
        return !(input instanceof WindowPlan window && isWindowOutputProjection(project, window))
                && !(input instanceof WindowJoinPlan && isColumnOnlyProjection(project));
    }

    /**
     * Whether every evaluation of the expression within one execution yields the same value.
     */
    public static boolean isStableWithinExecution(BoundExpression expression) {
        return (expression.getFunctionFlags() & BoundExpression.STABLE_WITHIN_EXECUTION) != 0;
    }

    /**
     * Evaluating the expression twice may give two values: it reads a function or a sub-query whose value is not
     * stable within one execution.
     */
    /**
     * True when the plan is a projection that only declares a designated timestamp over its unchanged input.
     */
    public static boolean isTimestampDeclarationOnly(LogicalPlan plan) {
        if (!(plan instanceof ProjectPlan project) || !project.hasTimestampDeclaration()) {
            return false;
        }
        final OutputSchema input = project.getInput().getOutput();
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
        if (aggregate.getGroupingExpressions().size() != 0 || aggregate.getAggregates().size() != 1) {
            return false;
        }
        final FunctionExpression call = aggregate.getAggregates().getQuick(0);
        if (call.getArgumentCount() != 1 || !(call.argumentAt(0) instanceof ColumnExpression column) || !column.isDirectReference()
                || !isTimestampEndpointBackward(call) && !Chars.equalsIgnoreCase(call.getName(), "min") && !Chars.equalsIgnoreCase(call.getName(), "first")) {
            return false;
        }
        final LogicalPlan input = aggregate.getInput();
        final LogicalPlan table = input instanceof FilterPlan filter ? filter.getInput() : input;
        return table instanceof ScanPlan scan && column.getColumnId() == scan.getNativeTimestampColumnId()
                && column.getColumnId() == input.getOutput().getTimestampColumnId();
    }

    /**
     * Whether a {@link #isTimestampEndpoint(AggregatePlan) timestamp endpoint} reads the last row in timestamp order.
     */
    public static boolean isTimestampEndpointBackward(FunctionExpression call) {
        return Chars.equalsIgnoreCase(call.getName(), "max") || Chars.equalsIgnoreCase(call.getName(), "last");
    }

    public static boolean isVolatile(BoundExpression expression) {
        return !expression.walk(VOLATILE_NODES);
    }

    /**
     * The arithmetic {@code c * k}, {@code c + k} or {@code c - k}, either operand order, that an aggregate
     * reading tables without a sub-query sums, where {@code c} is a BYTE, SHORT, INT or LONG input column and
     * {@code k} an integer literal; otherwise null. {@link AggregateRewrite} normalises such a sum.
     */
    /**
     * The index of the join input whose output holds the column.
     */
    /**
     * True when a computing projection over the window join keeps the master's designated timestamp: the columns it
     * reads, in the order the SELECT list names them, list the master timestamp at its position among the master's
     * columns. {@code order} and {@code readIds} are scratch lists.
     */
    public static boolean isWindowJoinTimestampKept(ProjectPlan project, WindowJoinPlan windowJoin, IntList order, IntList readIds) {
        final OutputSchema input = project.getInput().getOutput();
        final ObjList<BoundExpression> expressions = project.getExpressions();
        order.clear();
        for (int i = 0, n = expressions.size(); i < n; i++) {
            final int position = expressions.getQuick(i).getPosition();
            int index = order.size();
            while (index > 0 && position < expressions.getQuick(order.getQuick(index - 1)).getPosition()) {
                index--;
            }
            order.insert(index, i);
        }
        readIds.clear();
        for (int i = 0, n = order.size(); i < n; i++) {
            collectInputColumnIds(expressions.getQuick(order.getQuick(i)), input, readIds);
        }
        final OutputSchema master = windowJoin.getMaster().getOutput();
        final int index = master.getTimestampIndex();
        return index >= 0 && index < readIds.size() && readIds.getQuick(index) == master.getColumnId(index);
    }

    /**
     * A projection the window factory can emit directly: plain column references that select every
     * window output once, at unchanged types.
     */
    public static boolean isWindowOutputProjection(ProjectPlan project, WindowPlan window) {
        if (project.hasTimestampDeclaration()) {
            return false;
        }
        final OutputSchema input = window.getOutput();
        int windowCount = 0;
        for (int i = 0, n = project.getExpressions().size(); i < n; i++) {
            if (!(project.getExpressions().getQuick(i) instanceof ColumnExpression column)) {
                return false;
            }
            final int index = input.getColumnIndexById(column.getColumnId());
            if (index < 0 || column.isCast() || !column.isDirectReference() || input.getColumnType(index) != project.getOutput().getColumnType(i)) {
                return false;
            }
            if (window.getFunctionColumnIds().indexOf(column.getColumnId(), 0, window.getFunctionColumnIds().size()) >= 0) {
                if (isColumnSelectedBefore(project, i, column.getColumnId())) {
                    return false;
                }
                windowCount++;
            }
        }
        return windowCount == window.getFunctionColumnIds().size();
    }

    public static int joinColumnSource(JoinPlan join, int columnId) {
        for (int i = 0, n = join.getInputs().size(); i < n; i++) {
            if (join.getInputs().getQuick(i).getSourceOutput().getColumnIndexById(columnId) >= 0) {
                return i;
            }
        }
        throw new IllegalStateException("join column is outside its inputs");
    }

    /**
     * True when the expression reads a column of its input.
     */
    /**
     * Whether a LIMIT may evaluate to a negative count: one that is not a constant has no sign until execution.
     */
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

    public static boolean mayBeNegativeLimit(BoundExpression lo) {
        if (!(lo instanceof ConstantExpression constant)) {
            return true;
        }
        final long limit = limitValue(constant);
        return limit != Numbers.LONG_NULL && limit < 0;
    }

    public static FunctionExpression normalisableSumOperation(GroupingPlan aggregate, FunctionExpression sum) {
        if (!aggregate.hasDirectTableInput() || !Chars.equalsIgnoreCase(sum.getName(), "sum") || sum.getArgumentCount() != 1
                || !(sum.argumentAt(0) instanceof FunctionExpression operation) || operation.getArgumentCount() != 2) {
            return null;
        }
        final String name = operation.getName();
        if (name.length() != 1 || name.charAt(0) != '*' && name.charAt(0) != '+' && name.charAt(0) != '-') {
            return null;
        }
        final boolean isColumnOnLeft = isIntegerLiteral(operation.argumentAt(1));
        if (!isColumnOnLeft && !isIntegerLiteral(operation.argumentAt(0))
                || !(operation.argumentAt(isColumnOnLeft ? 0 : 1) instanceof ColumnExpression column)) {
            return null;
        }
        final OutputSchema input = aggregate.getInput().getOutput();
        final int index = input.getColumnIndexById(column.getColumnId());
        if (index < 0) {
            return null;
        }
        final int type = input.getColumnType(index);
        return type == ColumnType.BYTE || type == ColumnType.SHORT || type == ColumnType.INT || type == ColumnType.LONG ? operation : null;
    }

    /**
     * The projection a parallel top-K over the sort's input builds over itself: the one projection the generator
     * builds for the input, unless the projection's input is another; null otherwise.
     */
    public static ProjectPlan parallelTopKProjection(SortPlan sort) {
        final LogicalPlan base = generatedPlan(sort.getInput());
        if (!isPeelableProjection(base)) {
            return null;
        }
        final ProjectPlan projection = (ProjectPlan) base;
        return isPeelableProjection(generatedPlan(projection.getInput())) ? null : projection;
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
     * Index of the first expression that passes the column through without a cast.
     */
    /**
     * The input column the projection passes through as the given output column, or -1 when it computes that column.
     */
    public static int projectedSourceColumnId(ProjectPlan project, int columnId) {
        final int index = project.getOutput().getColumnIndexById(columnId);
        return index >= 0 && project.getExpressions().getQuick(index) instanceof ColumnExpression column ? column.getColumnId() : -1;
    }

    /**
     * The designated timestamp of the factory of a projection over an input whose factory designates
     * {@code inputTimestampIndex}: the one it declares, else the column its requested order reaches when that column
     * is the input's timestamp, else the timestamp it selects, lost when that column only passes through an input
     * that designates none, or when order planning dropped it from a computing projection over a window join.
     */
    public static int projectedTimestampIndex(ProjectPlan project, int inputTimestampIndex) {
        final LogicalPlan input = project.getInput();
        final int timestampIndex = requestedTimestampIndex(project, inputTimestampIndex);
        if (timestampIndex < 0) {
            return -1;
        }
        if (!project.hasTimestampDeclaration() && inputTimestampIndex < 0 && !hasExplicitJoinTimestamp(input)
                && project.getExpressions().getQuick(timestampIndex) instanceof ColumnExpression timestamp
                && (input.getOutput().getTimestampIndex() < 0 || timestamp.getColumnId() == input.getOutput().getTimestampColumnId())) {
            return -1;
        }
        return input instanceof WindowJoinPlan && !isColumnOnlyProjection(project) && project.isTimestampDropped() ? -1 : timestampIndex;
    }

    public static int projectedUncastColumnIndex(ProjectPlan project, int columnId) {
        final ObjList<BoundExpression> expressions = project.getExpressions();
        for (int i = 0, n = expressions.size(); i < n; i++) {
            if (expressions.getQuick(i) instanceof ColumnExpression column && !column.isCast() && column.getColumnId() == columnId) {
                return i;
            }
        }
        return -1;
    }

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
     * True when the call reads at least one outer column and no column of its own input.
     */
    public static boolean readsOnlyOuterColumns(FunctionExpression call) {
        return hasOuterColumn(call) && !readsColumn(call);
    }

    /**
     * The value a comparison of a column with itself folds to: {@link #SELF_COMPARISON_TRUE} for {@code =},
     * {@link #SELF_COMPARISON_FALSE} for {@code !=}, {@code <>}, {@code >} and {@code <}, otherwise
     * {@link #SELF_COMPARISON_NONE}.
     */
    public static int selfComparison(FunctionExpression call) {
        if (call.getArgumentCount() == 2 && call.argumentAt(0) instanceof ColumnExpression left && call.argumentAt(1) instanceof ColumnExpression right
                && left.isDirectReference() && right.isDirectReference() && left.getColumnId() == right.getColumnId()) {
            return switch (call.getName()) {
                case "=" -> SELF_COMPARISON_TRUE;
                case "!=", "<>", ">", "<" -> SELF_COMPARISON_FALSE;
                default -> SELF_COMPARISON_NONE;
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
     * The type an UPDATE stores a value of type {@code type} as in a column of type {@code targetType}: the target
     * type for a built-in widening cast other than text to TIMESTAMP, the value's own type otherwise.
     */
    /**
     * Skips column projections that keep every input column at its position and type: only the names differ.
     */
    public static LogicalPlan skipRenames(LogicalPlan plan) {
        while (plan instanceof ProjectPlan project && !project.hasTimestampDeclaration() && !project.hasUpdateConversions()) {
            final OutputSchema input = project.getInput().getOutput();
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
            plan = project.getInput();
        }
        return plan;
    }

    /**
     * The filter node the generator builds the factory of {@code plan} from, which a parallel consumer of the plan
     * steals; null when it builds another factory.
     */
    public static FilterPlan stolenFilter(LogicalPlan plan) {
        return generatedPlan(plan) instanceof FilterPlan filter ? filter : null;
    }

    /**
     * The selection a temporal join reads the stolen filter of its slave through, or null when it reads the filter
     * directly.
     */
    public static ProjectPlan temporalSlaveProjection(LogicalPlan slave) {
        return generatedPlan(slave) instanceof ProjectPlan project && isPeelableProjection(project)
                && !isComputedProjection(project) ? project : null;
    }

    /**
     * The filter node a temporal join steals from its slave: the filter the generator builds the slave from, or the
     * one under the selection it builds the slave from.
     */
    public static FilterPlan temporalStolenFilter(LogicalPlan slave) {
        final ProjectPlan projection = temporalSlaveProjection(slave);
        return stolenFilter(projection != null ? projection.getInput() : slave);
    }

    public static int updateColumnType(int type, int targetType) {
        return targetType < 0 || !ColumnType.isBuiltInWideningCast(type, targetType)
                || targetType == ColumnType.TIMESTAMP && (type == ColumnType.STRING || type == ColumnType.VARCHAR) ? type : targetType;
    }

    /**
     * The vector implementation of an aggregate call over a direct column, or of {@code count()}; null when it has none.
     */
    public static VectorAggregateFunctionConstructor vectorConstructor(FunctionExpression call) {
        if (!call.isAggregate()) {
            return null;
        }
        final int count = call.getArgumentCount();
        if (count == 0) {
            return VectorAggregateConstructors.of(call.getName(), ColumnType.UNDEFINED, true);
        }
        if (count == 1 && call.argumentAt(0) instanceof ColumnExpression column) {
            return VectorAggregateConstructors.of(call.getName(), column.getDataType(), false);
        }
        return null;
    }

    /**
     * The column a vector GROUP BY keys on: an INT or SYMBOL column, or the timestamp under {@code hour()}; null when
     * the key has no vector form.
     */
    public static ColumnExpression vectorKey(BoundExpression key) {
        if (key instanceof ColumnExpression column) {
            return column.getDataType() == ColumnType.INT || column.getDataType() == ColumnType.SYMBOL ? column : null;
        }
        if (key instanceof FunctionExpression call && SqlKeywords.isHourKeyword(call.getName())
                && call.getArgumentCount() == 1 && call.argumentAt(0) instanceof ColumnExpression column
                && ColumnType.isTimestamp(column.getDataType())) {
            return column;
        }
        return null;
    }

    /**
     * The designated timestamp a column-only projection over a window join keeps: the master timestamp at
     * {@code timestampIndex}, while the projection selects only master columns, those below {@code splitIndex},
     * before it.
     */
    public static int windowJoinProjectionTimestampIndex(ProjectPlan projection, OutputSchema output, int timestampIndex, int splitIndex) {
        for (int i = 0, n = projection.getExpressions().size(); i < n; i++) {
            final int index = output.getColumnIndexById(((ColumnExpression) projection.getExpressions().getQuick(i)).getColumnId());
            if (index == timestampIndex) {
                return i;
            }
            if (index >= splitIndex) {
                return -1;
            }
        }
        return -1;
    }

    /**
     * Appends the output index and type of the column a within() call tests, then the GeoHash prefixes it matches
     * normalised to the column's precision; when a prefix is not a constant the column takes, restores the list and
     * returns false.
     */
    public static boolean withinPrefixes(FunctionExpression within, OutputSchema output, LongList prefixes) {
        final ColumnExpression column = (ColumnExpression) within.argumentAt(0);
        final int columnType = column.getDataType();
        final int start = prefixes.size();
        prefixes.add(output.getColumnIndexById(column.getColumnId()));
        prefixes.add(columnType);
        for (int i = 1, n = within.getArgumentCount(); i < n; i++) {
            if (!(within.argumentAt(i) instanceof ConstantExpression prefix)) {
                prefixes.setPos(start);
                return false;
            }
            try {
                GeoHashes.addNormalizedGeoPrefix(prefix.getLongValue(), prefix.getDataType(), columnType, prefixes);
            } catch (NumericException e) {
                prefixes.setPos(start);
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

    private static boolean canPushSetTimestampThrough(UnaryPlan plan, int columnIndex) {
        final LogicalPlan input = plan.getInput();
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

    private static int groupingStability(AggregatePlan aggregate, boolean isParallelGroupByEnabled) {
        if (aggregate.getSharedSource() != null || stability(aggregate.getInput(), isParallelGroupByEnabled) != SEQUENCE_STABLE
                || !areStable(aggregate.getGroupingExpressions()) || !areStable(aggregate.getAggregates())) {
            return 0;
        }
        return aggregate.getGroupingExpressions().size() == 0 || !isParallelGroupByEnabled ? SEQUENCE_STABLE : RESULT_STABLE;
    }

    private static boolean isColumnSelectedBefore(ProjectPlan project, int index, int columnId) {
        for (int k = 0; k < index; k++) {
            if (((ColumnExpression) project.getExpressions().getQuick(k)).getColumnId() == columnId) {
                return true;
            }
        }
        return false;
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

    /**
     * The designated timestamp a projection declares over an input whose factory designates
     * {@code inputTimestampIndex}: its own, else the column its requested order reaches when that column is the
     * input's timestamp.
     */
    private static int requestedTimestampIndex(ProjectPlan project, int inputTimestampIndex) {
        if (!project.getRequestedOrder().isEmpty() && inputTimestampIndex < 0 && hasNativeFilterInput(project)) {
            return -1;
        }
        final int orderColumnId = project.getRequestedOrderColumnId();
        final int inputOrderId = projectedSourceColumnId(project, orderColumnId);
        return project.getOutput().getTimestampIndex() < 0 && inputOrderId >= 0
                && inputTimestampIndex == project.getInput().getOutput().getColumnIndexById(inputOrderId)
                ? project.getOutput().getColumnIndexById(orderColumnId) : project.getOutput().getTimestampIndex();
    }

    private static int stability(LogicalPlan plan, boolean isParallelGroupByEnabled) {
        return switch (plan) {
            case ScanPlan scan -> scan.getTableToken().isLiveView() ? 0 : SEQUENCE_STABLE;
            case FunctionSourcePlan source -> source.isDeterministic() ? SEQUENCE_STABLE : 0;
            case FilterPlan filter ->
                    isStable(filter.getPredicate()) ? stability(filter.getInput(), isParallelGroupByEnabled) : 0;
            case ProjectPlan project ->
                    areStable(project.getExpressions()) ? stability(project.getInput(), isParallelGroupByEnabled) : 0;
            case SortPlan sort -> sort.isMarkoutHorizon() ? 0 : stability(sort.getInput(), isParallelGroupByEnabled);
            case LimitPlan limit -> isStable(limit.getLo()) && isStable(limit.getHi())
                    && stability(limit.getInput(), isParallelGroupByEnabled) == SEQUENCE_STABLE ? SEQUENCE_STABLE : 0;
            case AggregatePlan aggregate -> groupingStability(aggregate, isParallelGroupByEnabled);
            case DistinctPlan distinct -> stability(distinct.getInput(), isParallelGroupByEnabled) & RESULT_STABLE;
            case SetOperationPlan operation ->
                    stability(operation.getLeft(), isParallelGroupByEnabled) & stability(operation.getRight(), isParallelGroupByEnabled);
            default -> 0;
        };
    }

    /**
     * Whether the plan reads a table function over a data source outside the database.
     */
    static boolean hasExternalDataSource(LogicalPlan plan) {
        return !plan.walkTopDown(EXTERNAL_SOURCES);
    }

    /**
     * Whether every evaluation of the plan within one execution yields the same multiset of rows, which is all the
     * value of a sub-query depends on: its consumers read a set or a single row.
     */
    static boolean isResultStable(LogicalPlan plan, boolean isParallelGroupByEnabled) {
        return (stability(plan, isParallelGroupByEnabled) & RESULT_STABLE) != 0;
    }
}
