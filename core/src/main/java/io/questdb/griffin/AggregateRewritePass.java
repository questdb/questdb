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
import io.questdb.griffin.plan.logical.BoundExpression;
import io.questdb.griffin.plan.logical.ColumnExpression;
import io.questdb.griffin.plan.logical.ConstantExpression;
import io.questdb.griffin.plan.logical.DistinctPlan;
import io.questdb.griffin.plan.logical.FunctionExpression;
import io.questdb.griffin.plan.logical.GroupingPlan;
import io.questdb.griffin.plan.logical.HorizonJoinPlan;
import io.questdb.griffin.plan.logical.JoinPlan;
import io.questdb.griffin.plan.logical.LimitPlan;
import io.questdb.griffin.plan.logical.LogicalPlan;
import io.questdb.griffin.plan.logical.OutputSchema;
import io.questdb.griffin.plan.logical.ProjectPlan;
import io.questdb.griffin.plan.logical.SampleByPlan;
import io.questdb.griffin.plan.logical.SortPlan;
import io.questdb.griffin.plan.logical.WindowJoinPlan;
import io.questdb.std.Chars;
import io.questdb.std.IntList;
import io.questdb.std.Mutable;
import io.questdb.std.ObjList;
import io.questdb.std.ObjectPool;

/**
 * Rewrites an aggregate read by a projection, directly or through a sort of its rows, in two steps.
 * <p>
 * First, sum normalisation: a {@code sum(c * k)} the projection selects as a whole value becomes
 * {@code sum(c) * k}, and {@code sum(c + k)} or {@code sum(c - k)} becomes {@code sum(c) + count(c) * k}
 * or {@code sum(c) - count(c) * k}, either operand order, for a BYTE, SHORT, INT or LONG column
 * {@code c} and an integer literal {@code k}; BYTE and SHORT, which have no NULL, count with
 * {@code count()}. The aggregate must read tables without a sub-query, see
 * {@link AggregatePlan#hasDirectTableInput()}, and no SAMPLE BY FILL may read its aggregates by
 * position. The projection computes the arithmetic in place of each whole reference; an expression
 * that reads the sum keeps the original aggregate. The rewrite is exact except on overflow: the
 * original sums values computed per row in the column's type, the rewrite computes in LONG.
 * <p>
 * Second, key lifting, which reads whether the projection computes a value, so it sees the
 * normalised sums.
 * <p>
 * Drops a grouping key that is arithmetic over constants and one column the aggregate also groups by:
 * {@code GROUP BY a, a + 1} forms the same groups as {@code GROUP BY a}. The projection above the
 * aggregate computes the dropped key from the surviving one in place of its references, so the
 * projection's columns keep their ids.
 * <p>
 * Applies to a selected key, other than the key of the first GROUP BY expression, of a plain aggregate
 * (no SAMPLE BY, DISTINCT or join input) that groups explicitly or selects a computed value. A
 * projection over the computing one merges into it, see {@link ProjectionMergePass#mergeComputingProject}.
 * A LIMIT over the projection of an aggregate with two keys over one column moves below the projection,
 * see {@link ProjectionMergePass#limitBelowProjection}.
 */
final class AggregateRewritePass implements Mutable {
    private final ObjList<BoundExpression> callArguments;
    private final CharacterStore characterStore;
    private final ObjectPool<ColumnExpression> columns;
    private final OptimiserContext context;
    private final ProjectionMergePass projectionMerge;
    private final ObjectPool<ProjectPlan> projects;
    private final IntList removedAggregates;
    private final IntList replacedSumIds;
    private final IntList sortKeyIndexes;
    // The projection whose aggregate the last rewriteProjectedAggregate call lifted keys from.
    private ProjectPlan liftedProject;
    // The last aggregate, in postorder, with two keys over one column; a LIMIT above its projection moves below it.
    private AggregatePlan repeatedKeyAggregate;

    AggregateRewritePass(
            OptimiserContext context,
            ProjectionMergePass projectionMerge,
            CharacterStore characterStore,
            ObjectPool<ColumnExpression> columns,
            ObjectPool<ProjectPlan> projects,
            ObjList<BoundExpression> callArguments,
            IntList removedAggregates,
            IntList replacedSumIds,
            IntList sortKeyIndexes
    ) {
        this.context = context;
        this.projectionMerge = projectionMerge;
        this.characterStore = characterStore;
        this.columns = columns;
        this.projects = projects;
        this.callArguments = callArguments;
        this.removedAggregates = removedAggregates;
        this.replacedSumIds = replacedSumIds;
        this.sortKeyIndexes = sortKeyIndexes;
    }

    @Override
    public void clear() {
        liftedProject = null;
        repeatedKeyAggregate = null;
    }

    /**
     * Counts the columns of arithmetic over columns and constants, or -1 for any other expression.
     * A folded constant counts as the operator call it was folded from.
     */
    private static int arithmeticColumnCount(BoundExpression expression) {
        if (expression instanceof ColumnExpression) {
            return 1;
        }
        if (expression instanceof ConstantExpression constant) {
            if (constant.getSource() != null) {
                return arithmeticColumnCount(constant.getSource());
            }
            return constant.isLiteral() ? 0 : -1;
        }
        if (!(expression instanceof FunctionExpression call) || !isArithmetic(call)) {
            return -1;
        }
        int count = 0;
        for (int i = 0, n = call.getArgumentCount(); i < n; i++) {
            final int argumentCount = arithmeticColumnCount(call.argumentAt(i));
            if (argumentCount < 0) {
                return -1;
            }
            count += argumentCount;
        }
        return count;
    }

    /**
     * The index of the aggregate {@code name(column)}, or of {@code name()} when columnId is -1; -1 when absent.
     */
    private static int findAggregate(ObjList<FunctionExpression> aggregates, CharSequence name, int columnId) {
        for (int i = 0, n = aggregates.size(); i < n; i++) {
            final FunctionExpression aggregate = aggregates.getQuick(i);
            if (Chars.equalsIgnoreCase(aggregate.getName(), name) && (columnId < 0 ? aggregate.getArgumentCount() == 0
                    : aggregate.getArgumentCount() == 1 && aggregate.argumentAt(0) instanceof ColumnExpression column
                      && !column.isCast() && column.getColumnId() == columnId)) {
                return i;
            }
        }
        return -1;
    }

    private static ColumnExpression firstColumn(BoundExpression expression) {
        if (expression instanceof ColumnExpression column) {
            return column;
        }
        if (expression instanceof ConstantExpression constant) {
            return constant.getSource() != null ? firstColumn(constant.getSource()) : null;
        }
        if (expression instanceof FunctionExpression call) {
            for (int i = 0, n = call.getArgumentCount(); i < n; i++) {
                final ColumnExpression column = firstColumn(call.argumentAt(i));
                if (column != null) {
                    return column;
                }
            }
        }
        return null;
    }

    private static int groupingColumnIndex(AggregatePlan aggregate, int columnId) {
        final ObjList<BoundExpression> keys = aggregate.getGroupingExpressions();
        for (int i = 0, n = keys.size(); i < n; i++) {
            if (keys.getQuick(i) instanceof ColumnExpression column && column.getColumnId() == columnId) {
                return i;
            }
        }
        return -1;
    }

    private static boolean hasColumnName(OutputSchema output, CharSequence name) {
        for (int i = 0, n = output.getColumnCount(); i < n; i++) {
            if (Chars.equalsIgnoreCase(output.getColumnName(i), name)) {
                return true;
            }
        }
        return false;
    }

    /**
     * The projection reads the column inside an expression or through a cast.
     */
    private static boolean hasComputedColumn(ProjectPlan project) {
        final ObjList<BoundExpression> expressions = project.getExpressions();
        for (int i = 0, n = expressions.size(); i < n; i++) {
            if (!(expressions.getQuick(i) instanceof ColumnExpression column) || column.isCast()) {
                return true;
            }
        }
        return false;
    }

    private static boolean hasNestedReference(ProjectPlan project, int columnId) {
        final ObjList<BoundExpression> expressions = project.getExpressions();
        for (int i = 0, n = expressions.size(); i < n; i++) {
            final BoundExpression expression = expressions.getQuick(i);
            if (!isSelectedReference(expression, columnId) && BoundExpressionRewriter.references(expression, columnId)) {
                return true;
            }
        }
        return false;
    }

    private static boolean hasRepeatedKeyColumn(AggregatePlan aggregate) {
        final ObjList<BoundExpression> keys = aggregate.getGroupingExpressions();
        final OutputSchema input = aggregate.getInput().getOutput();
        for (int i = 1, n = keys.size(); i < n; i++) {
            final int columnId = keyColumnId(keys.getQuick(i), input);
            if (columnId >= 0) {
                for (int k = 0; k < i; k++) {
                    if (keyColumnId(keys.getQuick(k), input) == columnId) {
                        return true;
                    }
                }
            }
        }
        return false;
    }

    private static boolean hasSelectedReference(ProjectPlan project, int columnId) {
        final ObjList<BoundExpression> expressions = project.getExpressions();
        for (int i = 0, n = expressions.size(); i < n; i++) {
            if (isSelectedReference(expressions.getQuick(i), columnId)) {
                return true;
            }
        }
        return false;
    }

    private static boolean isArithmetic(FunctionExpression call) {
        final String name = call.getName();
        if (name.length() != 1) {
            return false;
        }
        final int count = call.getArgumentCount();
        return switch (name.charAt(0)) {
            case '*', '/', '%' -> count == 2;
            case '+', '-' -> count == 1 || count == 2;
            default -> false;
        };
    }

    private static boolean isJoinInput(AggregatePlan aggregate) {
        return switch (LogicalPlans.skipFilters(aggregate.getInput())) {
            case JoinPlan _, WindowJoinPlan _, HorizonJoinPlan _ -> true;
            default -> false;
        };
    }

    private static boolean isNullableInteger(int type) {
        return type == ColumnType.INT || type == ColumnType.LONG;
    }

    private static boolean isSelected(ProjectPlan project, int columnId) {
        final OutputSchema output = project.getOutput();
        for (int i = 0, n = project.getExpressions().size(); i < n; i++) {
            if (output.isVisible(i) && project.getExpressions().getQuick(i) instanceof ColumnExpression column
                    && column.getColumnId() == columnId) {
                return true;
            }
        }
        return false;
    }

    /**
     * The projection selects the column unchanged, as a whole value.
     */
    private static boolean isSelectedReference(BoundExpression expression, int columnId) {
        return expression instanceof ColumnExpression column && !column.isCast() && column.getColumnId() == columnId;
    }

    /**
     * No FILL reads the aggregates by position.
     */
    private static boolean isUnfilled(GroupingPlan aggregate) {
        return !(aggregate instanceof SampleByPlan sampleBy) || sampleBy.getFillTokens().size() == 0;
    }

    /**
     * The single column a key reads: the key itself, or the column of binary arithmetic over it and
     * constants ({@code a + 1}, {@code 2 * a - 3}); otherwise null.
     */
    private static ColumnExpression keyColumn(BoundExpression key) {
        if (key instanceof ColumnExpression column) {
            return column;
        }
        final BoundExpression call = key instanceof ConstantExpression constant ? constant.getSource() : key;
        return call instanceof FunctionExpression function && function.getArgumentCount() == 2 && arithmeticColumnCount(function) == 1
                ? firstColumn(function) : null;
    }

    private static int keyColumnId(BoundExpression key, OutputSchema input) {
        final ColumnExpression column = keyColumn(key);
        if (column == null || key instanceof ColumnExpression && input.getColumnIndexById(column.getColumnId()) < 0) {
            return -1;
        }
        return column.getColumnId();
    }

    /**
     * The key the grouping lists first: the key of the first GROUP BY expression, or without GROUP BY
     * the key of the first selected value; -1 when that expression forms no key.
     */
    private static int leadingKeyIndex(ProjectPlan project, AggregatePlan aggregate) {
        if (aggregate.hasExplicitGrouping()) {
            return aggregate.hasConstantLeadingGroupBy() ? -1 : 0;
        }
        if (project.getExpressions().size() > 0 && project.getExpressions().getQuick(0) instanceof ColumnExpression column) {
            final int index = aggregate.getOutput().getColumnIndexById(column.getColumnId());
            return index < aggregate.getGroupingExpressions().size() ? index : -1;
        }
        return -1;
    }

    private int addAggregate(GroupingPlan aggregate, FunctionExpression function, String name) {
        aggregate.getAggregates().add(function);
        aggregate.getOutput().add(context.newColumnId(), uniqueName(aggregate.getOutput(), name), function.getDataType(), false);
        return aggregate.getAggregates().size() - 1;
    }

    /**
     * Binds the aggregate {@code name} over {@link #callArguments}, which it clears.
     */
    private FunctionExpression bindAggregate(CharSequence name, int position, OutputSchema input) throws SqlException {
        final BoundExpression bound = context.bindCall(name, position, callArguments, input);
        callArguments.clear();
        if (bound instanceof FunctionExpression function && function.isAggregate()) {
            return function;
        }
        throw new IllegalStateException("aggregate expected");
    }

    private BoundExpression bindOperation(CharSequence operator, int position, BoundExpression left, BoundExpression right, OutputSchema input) throws SqlException {
        callArguments.clear();
        callArguments.add(left);
        callArguments.add(right);
        final BoundExpression bound = context.bindCall(operator, position, callArguments, input);
        callArguments.clear();
        return bound;
    }

    private ColumnExpression inputColumn(OutputSchema input, int index, int position) {
        return columns.next().of(input.getColumnId(index), input.getColumnType(index), position);
    }

    private void liftKey(ProjectPlan project, AggregatePlan aggregate, int keyIndex, ColumnExpression column, int columnKeyIndex) {
        final OutputSchema output = aggregate.getOutput();
        final ProjectPlan mapping = projects.next().of(aggregate, aggregate.getPosition());
        mapping.getExpressions().add(columns.next().of(output.getColumnId(columnKeyIndex), column.getDataType(), column.getPosition()));
        mapping.getOutput().add(column.getColumnId(), output.getColumnName(columnKeyIndex), column.getDataType(), false);
        final BoundExpression key = aggregate.getGroupingExpressions().getQuick(keyIndex);
        final int keyId = output.getColumnId(keyIndex);
        final ObjList<BoundExpression> expressions = project.getExpressions();
        boolean isMoved = false;
        for (int i = 0, n = expressions.size(); i < n; i++) {
            if (BoundExpressionRewriter.references(expressions.getQuick(i), keyId)) {
                final BoundExpression value = isMoved ? context.getRewriter().copyRemappedColumns(key, mapping) : context.getRewriter().remapColumns(key, mapping);
                isMoved = true;
                expressions.setQuick(i, context.getRewriter().moveToColumn(expressions.getQuick(i), keyId, value));
            }
        }
        aggregate.getGroupingExpressions().remove(keyIndex);
        output.remove(keyIndex);
    }

    private boolean liftKeys(ProjectPlan project, AggregatePlan aggregate) {
        if (aggregate.hasSampleByBucket() || isJoinInput(aggregate)
                || !aggregate.hasExplicitGrouping() && !hasComputedColumn(project)
                || !hasRepeatedKeyColumn(aggregate)) {
            return false;
        }
        repeatedKeyAggregate = aggregate;
        final ObjList<BoundExpression> keys = aggregate.getGroupingExpressions();
        final OutputSchema input = aggregate.getInput().getOutput();
        final int leadingIndex = leadingKeyIndex(project, aggregate);
        boolean isLifted = false;
        for (int i = keys.size() - 1; i >= 0; i--) {
            final BoundExpression key = keys.getQuick(i);
            if (i == leadingIndex || key instanceof ColumnExpression || !isSelected(project, aggregate.getOutput().getColumnId(i))) {
                continue;
            }
            final ColumnExpression column = keyColumn(key);
            if (column == null || input.getColumnIndexById(column.getColumnId()) < 0) {
                continue;
            }
            final int columnKeyIndex = groupingColumnIndex(aggregate, column.getColumnId());
            if (columnKeyIndex >= 0) {
                liftKey(project, aggregate, i, column, columnKeyIndex);
                isLifted = true;
            }
        }
        return isLifted;
    }

    /**
     * Rewrites each sum of the aggregate that the projection selects as a whole value; returns whether
     * the projection changed. The original sum stays while the projection reads it inside an expression.
     */
    private boolean normaliseSums(ProjectPlan project, GroupingPlan aggregate) throws SqlException {
        if (!isUnfilled(aggregate)) {
            return false;
        }
        final ObjList<FunctionExpression> functions = aggregate.getAggregates();
        final OutputSchema output = aggregate.getOutput();
        final OutputSchema input = aggregate.getInput().getOutput();
        final int keyCount = aggregate.getGroupingExpressions().size();
        removedAggregates.clear();
        replacedSumIds.clear();
        boolean isNormalised = false;
        for (int i = 0, n = functions.size(); i < n; i++) {
            final FunctionExpression sum = functions.getQuick(i);
            final FunctionExpression operation = LogicalPlans.normalisableSumOperation(aggregate, sum);
            final int sumId = output.getColumnId(keyCount + i);
            if (operation == null || !hasSelectedReference(project, sumId)) {
                continue;
            }
            final boolean isColumnOnLeft = LogicalPlans.isIntegerLiteral(operation.argumentAt(1));
            if (!(operation.argumentAt(isColumnOnLeft ? 0 : 1) instanceof ColumnExpression column)) {
                continue;
            }
            final int columnIndex = input.getColumnIndexById(column.getColumnId());
            final int columnType = input.getColumnType(columnIndex);
            final boolean isKept = hasNestedReference(project, sumId);
            int sumIndex = findAggregate(functions, "sum", column.getColumnId());
            if (sumIndex < 0) {
                callArguments.add(inputColumn(input, columnIndex, column.getPosition()));
                final FunctionExpression columnSum = bindAggregate("sum", sum.getPosition(), input);
                if (isKept) {
                    sumIndex = addAggregate(aggregate, columnSum, "sum");
                } else {
                    functions.setQuick(i, columnSum);
                    output.setColumnType(keyCount + i, columnSum.getDataType());
                    replacedSumIds.add(sumId);
                    sumIndex = i;
                }
            }
            if (!isKept && sumIndex != i) {
                removedAggregates.add(i);
            }
            int countIndex = -1;
            if (operation.getName().charAt(0) != '*') {
                final int countColumnId = isNullableInteger(columnType) ? column.getColumnId() : -1;
                countIndex = findAggregate(functions, "count", countColumnId);
                if (countIndex < 0) {
                    if (countColumnId >= 0) {
                        callArguments.add(inputColumn(input, columnIndex, column.getPosition()));
                    }
                    countIndex = addAggregate(aggregate, bindAggregate("count", sum.getPosition(), input), "COUNT");
                }
            }
            final ObjList<BoundExpression> expressions = project.getExpressions();
            for (int k = 0, m = expressions.size(); k < m; k++) {
                if (isSelectedReference(expressions.getQuick(k), sumId)) {
                    final BoundExpression normalised = normalisedSum(operation, isColumnOnLeft, output, keyCount + sumIndex,
                            countIndex < 0 ? -1 : keyCount + countIndex, sum.getPosition());
                    assert normalised.getDataType() == project.getOutput().getColumnType(k);
                    expressions.setQuick(k, normalised);
                }
            }
            isNormalised = true;
        }
        for (int i = removedAggregates.size() - 1; i >= 0; i--) {
            final int index = removedAggregates.getQuick(i);
            functions.remove(index);
            output.remove(keyCount + index);
        }
        for (int i = 0, n = replacedSumIds.size(); i < n; i++) {
            final int index = output.getColumnIndexById(replacedSumIds.getQuick(i));
            if (!Chars.equalsIgnoreCase(output.getColumnName(index), "sum")) {
                output.setColumnName(index, uniqueName(output, "sum"), output.getColumnQualifier(index));
            }
        }
        return isNormalised;
    }

    /**
     * {@code sum op k} or, with a count, {@code sum op count * k}, keeping the operand order of {@code operation}.
     */
    private BoundExpression normalisedSum(
            FunctionExpression operation, boolean isColumnOnLeft, OutputSchema output, int sumIndex, int countIndex, int position
    ) throws SqlException {
        BoundExpression value = operation.argumentAt(isColumnOnLeft ? 1 : 0);
        if (countIndex >= 0) {
            value = bindOperation("*", position, inputColumn(output, countIndex, position), value, output);
        }
        final ColumnExpression sum = inputColumn(output, sumIndex, position);
        return bindOperation(operation.getName(), operation.getPosition(), isColumnOnLeft ? sum : value, isColumnOnLeft ? value : sum, output);
    }

    private LogicalPlan rewrite(LogicalPlan plan) throws SqlException {
        for (int i = 0, n = plan.inputCount(); i < n; i++) {
            final LogicalPlan input = plan.inputAt(i);
            if (input != null) {
                liftedProject = null;
                final LogicalPlan rewritten = rewriteProjectedAggregate(rewrite(input), !(plan instanceof DistinctPlan));
                if (rewritten != input) {
                    plan.replaceInput(i, rewritten);
                }
                if (liftedProject != null && rewritten == liftedProject && plan instanceof ProjectPlan project) {
                    projectionMerge.mergeComputingProject(project, liftedProject);
                }
            }
        }
        if (!(plan instanceof LimitPlan limit) || repeatedKeyAggregate == null) {
            return plan;
        }
        final LogicalPlan input = limit.getInput();
        final SortPlan sort = input instanceof SortPlan sorted ? sorted : null;
        final LogicalPlan projected = sort == null ? input : sort.getInput();
        return projected instanceof ProjectPlan project && project.getInput() == repeatedKeyAggregate
                ? projectionMerge.limitBelowProjection(limit, sort, project, repeatedKeyAggregate) : limit;
    }

    /**
     * Normalises the selected integer sums of an aggregate read by a projection and lifts its derived
     * keys, directly or through a sort of the aggregate's rows. A projection that now computes a value
     * no longer passes the sort its keys unchanged, so the sort moves above it. Returns the plan that
     * replaces {@code plan}.
     */
    private LogicalPlan rewriteProjectedAggregate(LogicalPlan plan, boolean isKeyLiftable) throws SqlException {
        if (!(plan instanceof ProjectPlan project)) {
            return plan;
        }
        final LogicalPlan input = project.getInput();
        if (input instanceof GroupingPlan grouped) {
            normaliseSums(project, grouped);
            if (isKeyLiftable && grouped instanceof AggregatePlan aggregate && liftKeys(project, aggregate)) {
                liftedProject = project;
            }
            return project;
        }
        if (!(input instanceof SortPlan sort) || !(sort.getInput() instanceof GroupingPlan aggregate)
                || !ProjectionMergePass.projectsSortKeys(project, sort)) {
            return project;
        }
        sortKeyIndexes.clear();
        for (int i = 0, n = sort.getColumnIds().size(); i < n; i++) {
            sortKeyIndexes.add(LogicalPlans.projectedUncastColumnIndex(project, sort.getColumnIds().getQuick(i)));
        }
        project.replaceInput(0, aggregate);
        final boolean isNormalised = normaliseSums(project, aggregate);
        final boolean isLifted = isKeyLiftable && aggregate instanceof AggregatePlan grouped && liftKeys(project, grouped);
        if (!isNormalised && !isLifted) {
            project.replaceInput(0, sort);
            return project;
        }
        if (isLifted) {
            liftedProject = project;
        }
        final OutputSchema output = project.getOutput();
        for (int i = 0, n = sortKeyIndexes.size(); i < n; i++) {
            sort.getColumnIds().setQuick(i, output.getColumnId(sortKeyIndexes.getQuick(i)));
        }
        output.setTimestampIndex(LogicalPlans.projectedColumnIndex(project, aggregate.getOutput().getTimestampColumnId()));
        sort.replaceInput(0, project);
        sort.getOutput().copyFrom(output);
        final int firstIndex = sortKeyIndexes.getQuick(0);
        sort.getOutput().setTimestampIndex(ColumnType.isTimestamp(output.getColumnType(firstIndex)) ? firstIndex : -1);
        return sort;
    }

    private CharSequence uniqueName(OutputSchema output, String name) {
        if (!hasColumnName(output, name)) {
            return name;
        }
        for (int i = 1; ; i++) {
            final CharacterStoreEntry entry = characterStore.newEntry();
            entry.put(name).put(i);
            final CharSequence candidate = entry.toImmutable();
            if (!hasColumnName(output, candidate)) {
                return candidate;
            }
        }
    }

    LogicalPlan rewriteAggregates(LogicalPlan root) throws SqlException {
        repeatedKeyAggregate = null;
        liftedProject = null;
        return rewriteProjectedAggregate(rewrite(root), true);
    }
}
