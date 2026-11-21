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
import io.questdb.griffin.SqlException;
import io.questdb.griffin.plan.logical.BoundExpression;
import io.questdb.griffin.plan.logical.ColumnExpression;
import io.questdb.griffin.plan.logical.FunctionExpression;
import io.questdb.griffin.plan.logical.GroupingPlan;
import io.questdb.griffin.plan.logical.LogicalPlan;
import io.questdb.griffin.plan.logical.LogicalPlans;
import io.questdb.griffin.plan.logical.OutputSchema;
import io.questdb.griffin.plan.logical.ProjectPlan;
import io.questdb.griffin.plan.logical.SortPlan;
import io.questdb.griffin.plan.logical.UniqueColumnNames;
import io.questdb.std.Chars;
import io.questdb.std.IntHashSet;
import io.questdb.std.IntIntHashMap;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;
import io.questdb.std.ObjectPool;

/**
 * Rewrites every integer sum over a constant that a projection can compute from the sum and count of its column, see
 * {@link #normaliseSums(ProjectPlan, GroupingPlan)}, before the aggregate rewrite, which lifts keys under the projection it
 * leaves computing.
 */
final class SumNormalisation implements OptimiserPass {
    private final CharacterStore characterStore;
    private final ObjectPool<ColumnExpression> columns;
    private final OptimiserContext context;
    // The columns a projection reads other than as whole, uncast values.
    private final IntHashSet nestedReads;
    // The next projection index that selects the same column whole, chained from selectionHeads; -1 ends a chain.
    private final IntList nextSelections;
    private final IntList removedAggregates;
    private final IntList replacedSumIds;
    // The first projection index that selects each column whole, by column id.
    private final IntIntHashMap selectionHeads;
    private final IntList sortKeyIndexes;
    private final UniqueColumnNames takenNames;

    SumNormalisation(
            OptimiserContext context,
            CharacterStore characterStore,
            ObjectPool<ColumnExpression> columns,
            UniqueColumnNames takenNames,
            IntList sortKeyIndexes,
            IntHashSet nestedReads,
            IntIntHashMap selectionHeads,
            IntList nextSelections,
            IntList removedAggregates,
            IntList replacedSumIds
    ) {
        this.context = context;
        this.characterStore = characterStore;
        this.columns = columns;
        this.takenNames = takenNames;
        this.sortKeyIndexes = sortKeyIndexes;
        this.nestedReads = nestedReads;
        this.selectionHeads = selectionHeads;
        this.nextSelections = nextSelections;
        this.removedAggregates = removedAggregates;
        this.replacedSumIds = replacedSumIds;
    }

    @Override
    public LogicalPlan apply(LogicalPlan plan) throws SqlException {
        return normaliseProjectedSums(normaliseSumsBelow(plan));
    }

    @Override
    public String getName() {
        return "sum normalisation";
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
}
