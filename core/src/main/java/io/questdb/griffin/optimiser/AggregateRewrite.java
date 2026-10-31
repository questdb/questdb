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
import io.questdb.griffin.CharacterStore;
import io.questdb.griffin.LogicalPlans;
import io.questdb.griffin.SqlException;
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
import io.questdb.griffin.plan.logical.SortPlan;
import io.questdb.griffin.plan.logical.UniqueColumnNames;
import io.questdb.griffin.plan.logical.WindowJoinPlan;
import io.questdb.std.Chars;
import io.questdb.std.IntHashSet;
import io.questdb.std.IntIntHashMap;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;
import io.questdb.std.ObjectPool;

/**
 * Rewrites an aggregate read by a projection, directly or through a sort of its rows.
 * <p>
 * Drops a grouping key that is arithmetic over constants and one column the aggregate also groups by:
 * {@code GROUP BY a, a + 1} forms the same groups as {@code GROUP BY a}. The projection above the
 * aggregate computes the dropped key from the surviving one in place of its references, so the
 * projection's columns keep their ids.
 * <p>
 * Applies to a selected key, other than the key of the first GROUP BY expression, of a plain aggregate
 * (no SAMPLE BY, DISTINCT or join input) that groups explicitly or selects a computed value. A
 * projection over the computing one merges into it, see {@link ProjectionMerge#mergeComputingProject}.
 * A LIMIT over the projection of an aggregate with two keys over one column moves below the projection,
 * see {@link ProjectionMerge#limitBelowProjection}.
 * <p>
 * Before every other rewrite, {@link #normaliseSums} computes the integer sums over constants of a grouping from the
 * sums and counts of their columns.
 */
final class AggregateRewrite implements OptimiserPass {
    private final CharacterStore characterStore;
    private final ObjectPool<ColumnExpression> columns;
    private final OptimiserContext context;
    private final ProjectPlan mapping;
    private final ColumnExpression mappingColumn;
    // The columns a projection reads other than as whole, uncast values.
    private final IntHashSet nestedReads;
    // The next projection index that selects the same column whole, chained from selectionHeads; -1 ends a chain.
    private final IntList nextSelections;
    private final ProjectionMerge projectionMerge;
    private final IntList removedAggregates;
    private final IntList replacedSumIds;
    // The first projection index that selects each column whole, by column id.
    private final IntIntHashMap selectionHeads;
    private final IntList sortKeyIndexes;
    private final UniqueColumnNames takenNames;
    // The projection whose aggregate the last rewriteProjectedAggregate call lifted keys from.
    private ProjectPlan liftedProject;
    // The last aggregate, in postorder, with two keys over one column; a LIMIT above its projection moves below it.
    private AggregatePlan repeatedKeyAggregate;

    AggregateRewrite(
            OptimiserContext context,
            ProjectionMerge projectionMerge,
            CharacterStore characterStore,
            ObjectPool<ColumnExpression> columns,
            UniqueColumnNames takenNames,
            ProjectPlan mapping,
            ColumnExpression mappingColumn,
            IntList sortKeyIndexes,
            IntHashSet nestedReads,
            IntIntHashMap selectionHeads,
            IntList nextSelections,
            IntList removedAggregates,
            IntList replacedSumIds
    ) {
        this.context = context;
        this.projectionMerge = projectionMerge;
        this.characterStore = characterStore;
        this.columns = columns;
        this.takenNames = takenNames;
        this.mapping = mapping;
        this.mappingColumn = mappingColumn;
        this.sortKeyIndexes = sortKeyIndexes;
        this.nestedReads = nestedReads;
        this.selectionHeads = selectionHeads;
        this.nextSelections = nextSelections;
        this.removedAggregates = removedAggregates;
        this.replacedSumIds = replacedSumIds;
    }

    @Override
    public LogicalPlan apply(LogicalPlan plan) throws SqlException {
        return rewriteAggregates(plan);
    }

    @Override
    public void clear() {
        liftedProject = null;
        repeatedKeyAggregate = null;
    }

    @Override
    public String getName() {
        return "aggregate rewrite";
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

    /**
     * The index of the aggregate {@code name(column)} of the grouping, or of {@code name()} when columnId is -1;
     * -1 when the grouping has none.
     */
    private static int findColumnAggregate(GroupingPlan grouping, CharSequence name, int columnId) {
        final ObjList<FunctionExpression> aggregates = grouping.getAggregates();
        for (int i = 0, n = aggregates.size(); i < n; i++) {
            final FunctionExpression aggregate = aggregates.getQuick(i);
            if ((columnId < 0 ? aggregate.getArgumentCount() == 0
                    : aggregate.getArgumentCount() == 1 && aggregate.argumentAt(0) instanceof ColumnExpression column
                      && !column.isCast() && column.getColumnId() == columnId)
                    && Chars.equalsIgnoreCase(aggregate.getName(), name)) {
                return i;
            }
        }
        return -1;
    }

    private static boolean hasRepeatedKeyColumn(AggregatePlan aggregate) {
        final ObjList<BoundExpression> keys = aggregate.getGroupingExpressions();
        final OutputSchema input = aggregate.getChild().getOutput();
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
        return switch (LogicalPlans.skipFilters(aggregate.getChild())) {
            case JoinPlan _, WindowJoinPlan _, HorizonJoinPlan _ -> true;
            default -> false;
        };
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

    private int addAggregate(GroupingPlan grouping, FunctionExpression function, CharSequence name) {
        grouping.getAggregates().add(function);
        grouping.getOutput().add(context.newColumnId(), takenNames.take(name, characterStore), function.getDataType(), false);
        return grouping.getAggregates().size() - 1;
    }

    /**
     * Binds the aggregate {@code name} over the column {@code argument}, or over no argument when it is null.
     */
    private FunctionExpression bindAggregate(CharSequence name, int position, ColumnExpression argument, OutputSchema input) throws SqlException {
        final ObjList<BoundExpression> arguments = context.getCallArguments();
        arguments.clear();
        if (argument != null) {
            arguments.add(argument);
        }
        if (context.bindCall(name, position, input) instanceof FunctionExpression function && function.isAggregate()) {
            return function;
        }
        throw new IllegalStateException("aggregate expected");
    }

    /**
     * Indexes how the projection reads each column: {@link #selectionHeads} and {@link #nextSelections} chain, in
     * order, the expressions that select a column unchanged, as a whole value; {@link #nestedReads} holds the columns
     * any other expression reads.
     */
    private void indexReferences(ObjList<BoundExpression> expressions) {
        selectionHeads.clear();
        nestedReads.clear();
        nextSelections.setAll(expressions.size(), -1);
        for (int k = expressions.size() - 1; k >= 0; k--) {
            final BoundExpression expression = expressions.getQuick(k);
            if (expression instanceof ColumnExpression column && !column.isCast()) {
                nextSelections.setQuick(k, selectionHeads.get(column.getColumnId()));
                selectionHeads.put(column.getColumnId(), k);
            } else {
                markNestedReads(expression);
            }
        }
    }

    private ColumnExpression inputColumn(OutputSchema input, int index, int position) {
        return columns.next().of(input.getColumnId(index), input.getColumnType(index), position);
    }

    private void liftKey(ProjectPlan project, AggregatePlan aggregate, int keyIndex, ColumnExpression column, int columnKeyIndex) {
        final OutputSchema output = aggregate.getOutput();
        mapping.clear();
        mapping.of(aggregate, aggregate.getPosition());
        mapping.getExpressions().add(mappingColumn.of(output.getColumnId(columnKeyIndex), column.getDataType(), column.getPosition()));
        mapping.getOutput().add(column.getColumnId(), output.getColumnName(columnKeyIndex), column.getDataType(), false);
        final BoundExpression key = aggregate.getGroupingExpressions().getQuick(keyIndex);
        final int keyId = output.getColumnId(keyIndex);
        final ObjList<BoundExpression> expressions = project.getExpressions();
        boolean isMoved = false;
        for (int i = 0, n = expressions.size(); i < n; i++) {
            if (LogicalPlans.readsColumn(expressions.getQuick(i), keyId)) {
                final BoundExpression value = isMoved ? context.getRewriter().copyRemappedColumns(key, mapping) : context.getRewriter().remapColumns(key, mapping);
                isMoved = true;
                expressions.setQuick(i, context.getRewriter().moveToColumn(expressions.getQuick(i), keyId, value));
            }
        }
        aggregate.getGroupingExpressions().remove(keyIndex);
        output.remove(keyIndex);
    }

    private boolean liftKeys(ProjectPlan project, AggregatePlan aggregate) {
        if (aggregate.getBucketKeyIndex() >= 0 || isJoinInput(aggregate)
                || !aggregate.hasExplicitGrouping() && LogicalPlans.isPlainColumnProjection(project)
                || !hasRepeatedKeyColumn(aggregate)) {
            return false;
        }
        repeatedKeyAggregate = aggregate;
        final ObjList<BoundExpression> keys = aggregate.getGroupingExpressions();
        final OutputSchema input = aggregate.getChild().getOutput();
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
            final int columnKeyIndex = LogicalPlans.groupingColumnIndex(aggregate, column.getColumnId());
            if (columnKeyIndex >= 0) {
                liftKey(project, aggregate, i, column, columnKeyIndex);
                isLifted = true;
            }
        }
        return isLifted;
    }

    private void markNestedReads(BoundExpression expression) {
        if (expression instanceof ColumnExpression column) {
            nestedReads.add(column.getColumnId());
        } else if (expression instanceof FunctionExpression call) {
            for (int i = 0, n = call.getArgumentCount(); i < n; i++) {
                markNestedReads(call.argumentAt(i));
            }
        }
    }

    /**
     * Normalises the sums of the grouping the projection reads, see {@link #normaliseSums(ProjectPlan, GroupingPlan)}.
     * A projection that now computes a value no longer passes the sort below it its keys unchanged, so the sort moves
     * above it. Returns the plan that replaces {@code plan}.
     */
    private LogicalPlan normaliseProjectedSums(LogicalPlan plan) throws SqlException {
        if (!(plan instanceof ProjectPlan project)) {
            return plan;
        }
        final GroupingPlan grouping = LogicalPlans.sumNormalisingGrouping(project);
        if (grouping == null) {
            return plan;
        }
        if (!(project.getChild() instanceof SortPlan sort)) {
            normaliseSums(project, grouping);
            return project;
        }
        LogicalPlans.collectSortKeyIndexes(project, sort, sortKeyIndexes);
        return normaliseSums(project, grouping) ? LogicalPlans.sortAboveProjection(sort, project, grouping, sortKeyIndexes) : project;
    }

    /**
     * Rewrites each {@link LogicalPlans#normalisableSumOperation normalisable sum} of the grouping that the projection
     * selects as a whole value: {@code sum(c * k)} becomes {@code sum(c) * k}, and {@code sum(c + k)} or
     * {@code sum(c - k)} becomes {@code sum(c) + count(c) * k} or {@code sum(c) - count(c) * k}, either operand order;
     * BYTE and SHORT, which have no NULL, count with {@code count()}. All such sums of a column share one sum and one
     * count. The projection computes the arithmetic, at the position of the sum's operation, in place of each whole
     * reference; an expression that reads the sum keeps the original aggregate. The rewrite is exact except on
     * overflow: the original sums values computed per row in the column's type, the rewrite computes in LONG. Returns
     * whether the projection changed.
     */
    private boolean normaliseSums(ProjectPlan project, GroupingPlan grouping) throws SqlException {
        final ObjList<FunctionExpression> aggregates = grouping.getAggregates();
        final OutputSchema output = grouping.getOutput();
        final OutputSchema input = grouping.getChild().getOutput();
        final int keyCount = grouping.getGroupingExpressions().size();
        removedAggregates.clear();
        replacedSumIds.clear();
        final ObjList<BoundExpression> expressions = project.getExpressions();
        boolean isIndexed = false;
        boolean isNormalised = false;
        for (int i = 0, n = aggregates.size(); i < n; i++) {
            final FunctionExpression sum = aggregates.getQuick(i);
            final FunctionExpression operation = LogicalPlans.normalisableSumOperation(grouping, sum);
            if (operation == null) {
                continue;
            }
            if (!isIndexed) {
                indexReferences(expressions);
                takenNames.of(output);
                isIndexed = true;
            }
            final int sumId = output.getColumnId(keyCount + i);
            final int firstSelection = selectionHeads.get(sumId);
            if (firstSelection < 0) {
                continue;
            }
            final boolean isKept = nestedReads.contains(sumId);
            final boolean isColumnOnLeft = operation.argumentAt(0) instanceof ColumnExpression;
            final ColumnExpression column = (ColumnExpression) operation.argumentAt(isColumnOnLeft ? 0 : 1);
            final BoundExpression literal = operation.argumentAt(isColumnOnLeft ? 1 : 0);
            final int columnId = column.getColumnId();
            final int columnIndex = input.getColumnIndexById(columnId);
            final int position = sum.getPosition();
            int sumIndex = findColumnAggregate(grouping, "sum", columnId);
            if (sumIndex < 0) {
                final FunctionExpression columnSum = bindAggregate("sum", position, inputColumn(input, columnIndex, column.getPosition()), input);
                if (isKept) {
                    sumIndex = addAggregate(grouping, columnSum, "sum");
                } else {
                    aggregates.setQuick(i, columnSum);
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
                final int type = input.getColumnType(columnIndex);
                final int countColumnId = type == ColumnType.INT || type == ColumnType.LONG ? columnId : -1;
                countIndex = findColumnAggregate(grouping, "count", countColumnId);
                if (countIndex < 0) {
                    countIndex = addAggregate(grouping, bindAggregate("count", position,
                            countColumnId < 0 ? null : inputColumn(input, columnIndex, column.getPosition()), input), "COUNT");
                }
            }
            for (int k = firstSelection; k >= 0; k = nextSelections.getQuick(k)) {
                final BoundExpression normalised = normalisedSum(operation, literal, isColumnOnLeft, output, keyCount + sumIndex,
                        countIndex < 0 ? -1 : keyCount + countIndex, position);
                assert normalised.getDataType() == project.getOutput().getColumnType(k);
                expressions.setQuick(k, normalised);
            }
            isNormalised = true;
        }
        for (int i = removedAggregates.size() - 1; i >= 0; i--) {
            final int index = removedAggregates.getQuick(i);
            aggregates.remove(index);
            output.remove(keyCount + index);
        }
        if (replacedSumIds.size() > 0) {
            takenNames.of(output);
        }
        for (int i = 0, n = replacedSumIds.size(); i < n; i++) {
            final int index = output.getColumnIndexById(replacedSumIds.getQuick(i));
            final CharSequence name = output.getColumnName(index);
            if (!Chars.equalsIgnoreCase(name, "sum")) {
                output.setColumnName(index, takenNames.take("sum", characterStore), output.getColumnQualifier(index));
                takenNames.release(name);
            }
        }
        return isNormalised;
    }

    private LogicalPlan normaliseSumsBelow(LogicalPlan plan) throws SqlException {
        for (int i = 0, n = plan.inputCount(); i < n; i++) {
            final LogicalPlan input = plan.inputAt(i);
            if (input != null) {
                final LogicalPlan normalised = normaliseProjectedSums(normaliseSumsBelow(input));
                if (normalised != input) {
                    plan.replaceInput(i, normalised);
                }
            }
        }
        return plan;
    }

    /**
     * {@code sum op k} or, with a count, {@code sum op count * k}, keeping the operand order of the sum's operation.
     */
    private BoundExpression normalisedSum(
            FunctionExpression operation, BoundExpression value, boolean isColumnOnLeft, OutputSchema output,
            int sumIndex, int countIndex, int position
    ) throws SqlException {
        final BoundExpression term = countIndex < 0 ? value
                : context.bindCall("*", position, inputColumn(output, countIndex, position), value, output);
        final ColumnExpression sum = inputColumn(output, sumIndex, position);
        return context.bindCall(operation.getName(), operation.getPosition(), isColumnOnLeft ? sum : term, isColumnOnLeft ? term : sum, output);
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
        final LogicalPlan input = limit.getChild();
        final SortPlan sort = input instanceof SortPlan sorted ? sorted : null;
        final LogicalPlan projected = sort == null ? input : sort.getChild();
        return projected instanceof ProjectPlan project && project.getChild() == repeatedKeyAggregate
                ? projectionMerge.limitBelowProjection(limit, sort, project, repeatedKeyAggregate) : limit;
    }

    private LogicalPlan rewriteAggregates(LogicalPlan root) throws SqlException {
        repeatedKeyAggregate = null;
        liftedProject = null;
        return rewriteProjectedAggregate(rewrite(root), true);
    }

    /**
     * Lifts the derived keys of an aggregate read by a projection, directly or through a sort of the
     * aggregate's rows. A projection that now computes a value no longer passes the sort its keys
     * unchanged, so the sort moves above it. Returns the plan that replaces {@code plan}.
     */
    private LogicalPlan rewriteProjectedAggregate(LogicalPlan plan, boolean isKeyLiftable) {
        if (!isKeyLiftable || !(plan instanceof ProjectPlan project)) {
            return plan;
        }
        final LogicalPlan input = project.getChild();
        if (input instanceof AggregatePlan aggregate) {
            if (liftKeys(project, aggregate)) {
                liftedProject = project;
            }
            return project;
        }
        if (!(input instanceof SortPlan sort) || !(sort.getChild() instanceof AggregatePlan aggregate)
                || !LogicalPlans.projectsSortKeys(project, sort)) {
            return project;
        }
        LogicalPlans.collectSortKeyIndexes(project, sort, sortKeyIndexes);
        project.replaceInput(0, aggregate);
        if (!liftKeys(project, aggregate)) {
            project.replaceInput(0, sort);
            return project;
        }
        liftedProject = project;
        return LogicalPlans.sortAboveProjection(sort, project, aggregate, sortKeyIndexes);
    }

    /**
     * Rewrites every integer sum over a constant that a projection can compute from the sum and count of its column,
     * see {@link #normaliseSums(ProjectPlan, GroupingPlan)}, in the bound plan, so that join ordering and decorrelation
     * see the sums and counts it adds. Returns the plan that replaces {@code root}.
     */
    LogicalPlan normaliseSums(LogicalPlan root) throws SqlException {
        return normaliseProjectedSums(normaliseSumsBelow(root));
    }
}
