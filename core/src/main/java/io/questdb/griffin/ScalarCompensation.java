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
import io.questdb.griffin.model.QueryModel;
import io.questdb.griffin.plan.logical.AggregatePlan;
import io.questdb.griffin.plan.logical.BoundExpression;
import io.questdb.griffin.plan.logical.ColumnExpression;
import io.questdb.griffin.plan.logical.ConstantExpression;
import io.questdb.griffin.plan.logical.FillPlan;
import io.questdb.griffin.plan.logical.FilterPlan;
import io.questdb.griffin.plan.logical.FunctionExpression;
import io.questdb.griffin.plan.logical.JoinInput;
import io.questdb.griffin.plan.logical.JoinPlan;
import io.questdb.griffin.plan.logical.LimitPlan;
import io.questdb.griffin.plan.logical.LogicalPlan;
import io.questdb.griffin.plan.logical.OuterColumnExpression;
import io.questdb.griffin.plan.logical.OutputSchema;
import io.questdb.griffin.plan.logical.ProjectPlan;
import io.questdb.griffin.plan.logical.SampleByPlan;
import io.questdb.griffin.plan.logical.SortPlan;
import io.questdb.griffin.plan.logical.WindowPlan;
import io.questdb.std.Chars;
import io.questdb.std.IntIntHashMap;
import io.questdb.std.IntList;
import io.questdb.std.Mutable;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;

import static io.questdb.griffin.DecorrelationContext.appendMissingColumns;
import static io.questdb.griffin.DecorrelationContext.collectColumnIds;
import static io.questdb.griffin.DecorrelationContext.isTrue;
import static io.questdb.griffin.DecorrelationContext.keyColumnId;
import static io.questdb.griffin.DecorrelationContext.pairIndex;

/**
 * Restores the values a keyless aggregate takes over no rows once decorrelation keys it by the outer columns:
 * counts read zero, and a LIMIT or ON condition guards the restored values.
 */
final class ScalarCompensation implements Mutable {
    static final String OUTER_LIMIT_OVER_COUNT = "LIMIT referencing an outer column is not supported over a scalar count in a correlated lateral sub-query; use a constant or bind variable";
    private static final String CARRIER_PREFIX = "__qdb_count_carrier__";
    final IntList compensatedIds = new IntList();
    final ObjList<CharSequence> compensatedNames = new ObjList<>();
    final ObjList<ProjectPlan> compensatedScalars = new ObjList<>();
    final ObjList<BoundExpression> compensations = new ObjList<>();
    final IntIntHashMap consumerRemap = new IntIntHashMap();
    final ObjList<JoinInput> drivenInputs = new ObjList<>();
    final ObjList<ProjectPlan> drivenScalars = new ObjList<>();
    final ObjList<AggregatePlan> scalarAggregates = new ObjList<>();
    final ObjList<ProjectPlan> uncompensatedScalars = new ObjList<>();
    private final IntList carrierColumnIds = new IntList();
    private final DecorrelationContext ctx;
    private final DecorrelationDomains domains;
    private final ObjList<BoundExpression> hoistedConjuncts;
    AggregatePlan scalarAggregate;
    private BoundExpression pendingConjuncts;

    ScalarCompensation(DecorrelationContext ctx, DecorrelationDomains domains, ObjList<BoundExpression> hoistedConjuncts) {
        this.ctx = ctx;
        this.domains = domains;
        this.hoistedConjuncts = hoistedConjuncts;
    }

    @Override
    public void clear() {
        carrierColumnIds.clear();
        compensatedIds.clear();
        compensatedNames.clear();
        compensatedScalars.clear();
        compensations.clear();
        consumerRemap.clear();
        drivenInputs.clear();
        drivenScalars.clear();
        scalarAggregates.clear();
        uncompensatedScalars.clear();
        pendingConjuncts = null;
        scalarAggregate = null;
    }

    /**
     * The argument index of the count column in a comparison of a count of the scalar projection with an
     * integer constant, or -1.
     */
    private static int countComparisonSide(BoundExpression predicate, ProjectPlan scalar) {
        if (!(predicate instanceof FunctionExpression call) || call.getArgumentCount() != 2) {
            return -1;
        }
        for (int side = 0; side < 2; side++) {
            if (call.argumentAt(side) instanceof ColumnExpression column && isIntegerConstant(call.argumentAt(1 - side))) {
                final int index = scalar.getOutput().getColumnIndexById(column.getColumnId());
                if (index > -1 && scalar.getExpressions().getQuick(index) instanceof ColumnExpression count
                        && isZeroOnEmptyColumn(count.getColumnId(), (AggregatePlan) scalar.getInput())) {
                    return side;
                }
            }
        }
        return -1;
    }

    private static boolean hasAggregateColumn(BoundExpression expression, AggregatePlan aggregate) {
        if (expression instanceof ColumnExpression column) {
            return aggregate.getOutput().getColumnIndexById(column.getColumnId()) >= aggregate.getGroupingExpressions().size();
        }
        if (expression instanceof FunctionExpression call) {
            for (int i = 0, n = call.getArgumentCount(); i < n; i++) {
                if (hasAggregateColumn(call.argumentAt(i), aggregate)) {
                    return true;
                }
            }
        }
        return false;
    }

    private static boolean hasZeroOnEmpty(BoundExpression expression, AggregatePlan aggregate) {
        if (expression instanceof ColumnExpression column) {
            return isZeroOnEmptyColumn(column.getColumnId(), aggregate);
        }
        if (expression instanceof FunctionExpression call) {
            for (int i = 0, n = call.getArgumentCount(); i < n; i++) {
                if (hasZeroOnEmpty(call.argumentAt(i), aggregate)) {
                    return true;
                }
            }
        }
        return false;
    }

    private static boolean isIntegerConstant(BoundExpression expression) {
        if (!(expression instanceof ConstantExpression constant)) {
            return false;
        }
        return switch (ColumnType.tagOf(constant.getDataType())) {
            case ColumnType.BYTE, ColumnType.SHORT -> true;
            case ColumnType.INT -> constant.getLongValue() != Numbers.INT_NULL;
            case ColumnType.LONG -> constant.getLongValue() != Numbers.LONG_NULL;
            default -> false;
        };
    }

    private static boolean isNullPropagating(BoundExpression expression) {
        if (expression instanceof ColumnExpression || expression instanceof OuterColumnExpression) {
            return true;
        }
        if (!(expression instanceof FunctionExpression call) || call.getArgumentCount() != 2) {
            return false;
        }
        final String name = call.getName();
        return (Chars.equals(name, '+') || Chars.equals(name, '-') || Chars.equals(name, '*') || Chars.equals(name, '/'))
                && isNullPropagating(call.argumentAt(0)) && isNullPropagating(call.argumentAt(1));
    }

    private static boolean isZeroOnEmpty(FunctionExpression aggregate) {
        final String name = aggregate.getName();
        return Chars.equalsIgnoreCase(name, "count") || Chars.equalsIgnoreCase(name, "count_distinct")
                || Chars.equalsIgnoreCase(name, "approx_count_distinct");
    }

    private static boolean isZeroOnEmptyColumn(int columnId, AggregatePlan aggregate) {
        final int index = aggregate.getOutput().getColumnIndexById(columnId);
        final int keyCount = aggregate.getGroupingExpressions().size();
        return index >= keyCount && isZeroOnEmpty(aggregate.getAggregates().getQuick(index - keyCount));
    }

    private static boolean keepsEveryCount(BoundExpression predicate, ProjectPlan scalar) {
        if (predicate instanceof FunctionExpression call && call.isAnd()) {
            return keepsEveryCount(call.argumentAt(0), scalar) && keepsEveryCount(call.argumentAt(1), scalar);
        }
        final int countSide = countComparisonSide(predicate, scalar);
        if (countSide < 0) {
            return false;
        }
        final FunctionExpression call = (FunctionExpression) predicate;
        final long value = ((ConstantExpression) call.argumentAt(1 - countSide)).getLongValue();
        final String name = call.getName();
        if (Chars.equals(name, "!=") || Chars.equals(name, "<>")) {
            return value < 0;
        }
        if (Chars.equals(name, countSide == 0 ? ">=" : "<=")) {
            return value <= 0;
        }
        return Chars.equals(name, countSide == 0 ? ">" : "<") && value < 0;
    }

    /**
     * Gives a column of a join input a fresh id, so a projection over the join can define the old one.
     */
    private static void renameJoinedColumn(JoinPlan join, OutputSchema output, int index, int columnId) {
        final OutputSchema joined = join.getOutput();
        joined.setColumnId(joined.getColumnIndexById(output.getColumnId(index)), columnId);
        output.setColumnId(index, columnId);
    }

    private CharSequence carrierName() {
        final CharacterStoreEntry name = ctx.characterStore.newEntry();
        name.put(CARRIER_PREFIX).put(ctx.carrierSequence++);
        return name.toImmutable();
    }

    /**
     * The value a visible column of the projection over a keyless aggregate takes over no rows, over
     * {@code input}: counts read zero, other aggregates read their carried value (NULL), and the columns
     * {@link #substitution} holds read their substitutes. Null when the column reads aggregates none of which
     * counts: the LEFT join's NULL is then the value.
     */
    private BoundExpression emptyValue(ProjectPlan project, int index, OutputSchema input) throws SqlException {
        final BoundExpression expression = project.getExpressions().getQuick(index);
        final AggregatePlan aggregate = scalarAggregateBelow(project);
        if (aggregate == null) {
            return expression instanceof ColumnExpression column && scalarCountIndex(project.getInput(), column.getColumnId()) > -1
                    ? zeroCoalesce(ctx.planNodes.columns.next().of(project.getOutput().getColumnId(index), column.getDataType(), column.getPosition()), input)
                    : null;
        }
        if (!hasZeroOnEmpty(expression, aggregate) && hasAggregateColumn(expression, aggregate)) {
            return null;
        }
        if (expression instanceof ColumnExpression column && isZeroOnEmptyColumn(column.getColumnId(), aggregate)) {
            return zeroCoalesce(ctx.planNodes.columns.next().of(project.getOutput().getColumnId(index), column.getDataType(), column.getPosition()), input);
        }
        final int columnBase = ctx.scratch.size();
        collectColumnIds(expression, ctx.scratch);
        BoundExpression value = expression;
        for (int i = columnBase, n = ctx.scratch.size(); i < n; i++) {
            final int columnId = ctx.scratch.getQuick(i);
            final int aggregateIndex = aggregate.getOutput().getColumnIndexById(columnId);
            final int keyCount = aggregate.getGroupingExpressions().size();
            final int type = aggregate.getOutput().getColumnType(aggregateIndex);
            final BoundExpression replacement;
            if (aggregateIndex >= keyCount) {
                final ColumnExpression carrier = ctx.planNodes.columns.next().of(carrierColumnIds.getQuick(pairIndex(carrierColumnIds, columnId) + 1), type,
                        expression.getPosition());
                replacement = isZeroOnEmpty(aggregate.getAggregates().getQuick(aggregateIndex - keyCount)) ? zeroCoalesce(carrier, input) : carrier;
            } else {
                final int substitute = ctx.substitution.get(columnId);
                replacement = substitute < 0 ? null : ctx.planNodes.columns.next().of(substitute, type, expression.getPosition());
            }
            if (replacement != null) {
                value = ctx.functionBinder.substituteColumn(value, columnId, replacement);
            }
        }
        ctx.scratch.setPos(columnBase);
        return value;
    }

    private BoundExpression guarded(BoundExpression condition, BoundExpression value, OutputSchema input, int position) throws SqlException {
        ctx.callArguments.clear();
        ctx.callArguments.add(condition);
        ctx.callArguments.add(value);
        ctx.callArguments.add(ctx.planNodes.constants.next().ofNull(position));
        return ctx.bindCall("case", position, input);
    }

    private boolean isWrappedScalarColumn(BoundExpression expression, LogicalPlan input) {
        if (!isNullPropagating(expression)) {
            return false;
        }
        if (expression instanceof ColumnExpression column) {
            return scalarColumnIndex(input, column.getColumnId()) > -1;
        }
        final int columnBase = ctx.scratch.size();
        collectColumnIds(expression, ctx.scratch);
        boolean hasAggregate = false;
        boolean hasCount = false;
        for (int i = columnBase, n = ctx.scratch.size(); i < n; i++) {
            hasAggregate |= scalarColumnIndex(input, ctx.scratch.getQuick(i)) > -1;
            hasCount |= scalarCountIndex(input, ctx.scratch.getQuick(i)) > -1;
        }
        ctx.scratch.setPos(columnBase);
        return hasAggregate && !hasCount;
    }

    private boolean readsCompensated(BoundExpression expression) {
        final int columnBase = ctx.scratch.size();
        collectColumnIds(expression, ctx.scratch);
        boolean isFound = false;
        for (int i = columnBase, n = ctx.scratch.size(); i < n && !isFound; i++) {
            isFound = compensatedIds.indexOf(ctx.scratch.getQuick(i), 0, compensatedIds.size()) > -1;
        }
        ctx.scratch.setPos(columnBase);
        return isFound;
    }

    /**
     * The index in the scalar projection of the column {@code columnId} of {@code node} reads through column
     * projections and filters, or -1.
     */
    private int scalarColumnIndex(LogicalPlan node, int columnId) {
        int id = columnId;
        LogicalPlan current = node;
        while (true) {
            if (current instanceof ProjectPlan project) {
                final int index = project.getOutput().getColumnIndexById(id);
                if (index < 0) {
                    return -1;
                }
                if (uncompensatedScalars.indexOf(project) > -1 || isScalarProjection(project)) {
                    return index;
                }
                if (!(project.getExpressions().getQuick(index) instanceof ColumnExpression column)) {
                    return -1;
                }
                id = column.getColumnId();
            } else if (!(current instanceof FilterPlan) && !(current instanceof WindowPlan)) {
                return -1;
            }
            current = current.inputAt(0);
        }
    }

    private int scalarCountIndex(LogicalPlan node, int columnId) {
        final int index = scalarColumnIndex(node, columnId);
        if (index < 0) {
            return -1;
        }
        LogicalPlan current = node;
        while (!(current instanceof ProjectPlan project && (uncompensatedScalars.indexOf(project) > -1 || isScalarProjection(project)))) {
            current = current.inputAt(0);
        }
        final ProjectPlan scalar = (ProjectPlan) current;
        final BoundExpression expression = scalar.getExpressions().getQuick(index);
        return expression instanceof ColumnExpression column && isZeroOnEmptyColumn(column.getColumnId(), (AggregatePlan) scalar.getInput()) ? index : -1;
    }

    /**
     * Returns the conjuncts of the predicate that read no compensated column, appending the others to
     * {@link #hoistedConjuncts}.
     */
    private BoundExpression takeCompensatedConjuncts(BoundExpression predicate) {
        if (predicate == null) {
            return null;
        }
        if (predicate instanceof FunctionExpression call && call.isAnd()) {
            final BoundExpression left = takeCompensatedConjuncts(call.argumentAt(0));
            final BoundExpression right = takeCompensatedConjuncts(call.argumentAt(1));
            if (left == null) {
                return right;
            }
            if (right == null) {
                return left;
            }
            return left == call.argumentAt(0) && right == call.argumentAt(1) ? call : ctx.functionBinder.replaceConjunction(call, left, right);
        }
        if (readsCompensated(predicate)) {
            hoistedConjuncts.add(predicate);
            return null;
        }
        return predicate;
    }

    private BoundExpression zeroCoalesce(ColumnExpression value, OutputSchema input) throws SqlException {
        return ctx.bindCall("coalesce", value.getPosition(), value, ctx.planNodes.constants.next().ofInt(0, value.getPosition()), input);
    }

    static void forwardColumns(LogicalPlan node, LogicalPlan bottom) {
        if (node != bottom) {
            forwardColumns(node.inputAt(0), bottom);
            appendMissingColumns(node.getOutput(), node.inputAt(0).getOutput());
        }
    }

    static boolean isImplicitlyKeyedByOuterColumns(AggregatePlan aggregate) {
        if (aggregate.hasExplicitGrouping() || aggregate.hasSampleByBucket() || aggregate instanceof SampleByPlan) {
            return false;
        }
        for (int i = 0, n = aggregate.getGroupingExpressions().size(); i < n; i++) {
            if (!(aggregate.getGroupingExpressions().getQuick(i) instanceof OuterColumnExpression)) {
                return false;
            }
        }
        return true;
    }

    static BoundExpression outerLimit(LimitPlan limit) {
        if (limit.getLo() != null && LogicalPlans.hasOuterColumn(limit.getLo())) {
            return limit.getLo();
        }
        return limit.getHi() != null && LogicalPlans.hasOuterColumn(limit.getHi()) ? limit.getHi() : null;
    }

    static boolean rejectsZeroCount(BoundExpression predicate, ProjectPlan scalar) {
        if (predicate instanceof FunctionExpression call && call.isAnd()) {
            return rejectsZeroCount(call.argumentAt(0), scalar) || rejectsZeroCount(call.argumentAt(1), scalar);
        }
        final int countSide = countComparisonSide(predicate, scalar);
        if (countSide < 0) {
            return false;
        }
        final FunctionExpression call = (FunctionExpression) predicate;
        final long value = ((ConstantExpression) call.argumentAt(1 - countSide)).getLongValue();
        final long left = countSide == 0 ? 0 : value;
        final long right = countSide == 0 ? value : 0;
        final String name = call.getName();
        if (Chars.equals(name, '=')) {
            return left != right;
        }
        if (Chars.equals(name, "!=") || Chars.equals(name, "<>")) {
            return left == right;
        }
        if (Chars.equals(name, '<')) {
            return left >= right;
        }
        if (Chars.equals(name, "<=")) {
            return left > right;
        }
        if (Chars.equals(name, '>')) {
            return left <= right;
        }
        return Chars.equals(name, ">=") && left < right;
    }

    ProjectPlan compensableScalar(LogicalPlan plan) {
        return plan instanceof ProjectPlan project && uncompensatedScalars.indexOf(project) < 0
                && scalarAggregate != null && scalarAggregateBelow(project) != null ? project : null;
    }

    /**
     * A keyless aggregate inside the body, now keyed by the outer columns, still yields one row per outer
     * row: its domain joins LEFT to the projection over it, and a projection above restores the empty-input
     * values. The outer columns map to the domain.
     */
    LogicalPlan compensateBlock(ProjectPlan project, int base) throws SqlException {
        final int position = project.getPosition();
        exposeCarriers(project);
        domains.domainOuterIds.clear();
        for (int i = base, n = ctx.mappedOuterIds.size(); i < n; i++) {
            domains.domainOuterIds.add(ctx.mappedOuterIds.getQuick(i));
        }
        final int domainBase = ctx.mappedOuterIds.size();
        final AggregatePlan domain = domains.buildDomain(position);
        final JoinPlan join = ctx.planNodes.joins.next().of(position);
        final JoinInput projectInput = ctx.planNodes.joinInputs.next().of(project, QueryModel.JOIN_LEFT_OUTER, null, position);
        join.getInputs().add(ctx.planNodes.joinInputs.next().of(domain, QueryModel.JOIN_CROSS, domains.domainAlias(), position));
        join.getInputs().add(projectInput);
        join.getOrderedInputs().addAll(join.getInputs());
        join.getOutput().copyFrom(domain.getOutput());
        appendMissingColumns(join.getOutput(), project.getOutput());
        final OutputSchema output = project.getOutput();
        ctx.substitution.clear();
        for (int i = base; i < domainBase; i++) {
            final int columnId = ctx.mappedColumnIds.getQuick(i);
            final int domainId = ctx.mappedColumnIds.getQuick(i - base + domainBase);
            JoinBinder.addJoinKey(projectInput, domainId, columnId, domain.getOutput().getColumnName(domain.getOutput().getColumnIndexById(domainId)),
                    output.getColumnName(output.getColumnIndexById(columnId)), position);
            ctx.substitution.put(keyColumnId(project, columnId), domainId);
        }
        final ProjectPlan compensated = ctx.planNodes.projects.next().of(join, position);
        for (int i = 0, n = output.getColumnCount(); i < n; i++) {
            if (output.isVisible(i)) {
                final int columnId = output.getColumnId(i);
                renameJoinedColumn(join, output, i, ctx.nextColumnId++);
                final BoundExpression empty = emptyValue(project, i, join.getOutput());
                final BoundExpression value = empty != null ? empty : ctx.planNodes.columns.next().of(output.getColumnId(i), output.getColumnType(i), position);
                compensated.getExpressions().add(value);
                compensated.getOutput().add(columnId, output.getColumnName(i), value.getDataType(), output.getMetadata(i), true);
            }
        }
        for (int i = domainBase, n = ctx.mappedOuterIds.size(); i < n; i++) {
            ctx.exposeColumn(compensated, join.getOutput(), ctx.mappedColumnIds.getQuick(i), ctx.outerRefName(ctx.mappedOuterIds.getQuick(i)), position);
            ctx.mappedColumnIds.setQuick(i - domainBase + base, compensated.getOutput().getColumnId(compensated.getOutput().getColumnCount() - 1));
        }
        ctx.mappedOuterIds.setPos(domainBase);
        ctx.mappedColumnIds.setPos(domainBase);
        scalarAggregate = null;
        compensatedScalars.add(compensated);
        return compensated;
    }

    /**
     * Restores the compensated values in the consumer projection of the join itself, reading the hoisted WHERE
     * conjuncts above it. Returns null, changing nothing, when a hoisted conjunct reads a column the projection
     * does not select as is.
     */
    LogicalPlan compensateConsumer(ProjectPlan project) throws SqlException {
        final BoundExpression hoisted = pendingConjuncts;
        ctx.substitution.clear();
        if (hoisted != null) {
            final int columnBase = ctx.scratch.size();
            collectColumnIds(hoisted, ctx.scratch);
            for (int i = columnBase, n = ctx.scratch.size(); i < n; i++) {
                final int columnId = ctx.scratch.getQuick(i);
                final int index = LogicalPlans.projectedUncastColumnIndex(project, columnId);
                if (index < 0) {
                    ctx.scratch.setPos(columnBase);
                    return null;
                }
                ctx.substitution.put(columnId, project.getOutput().getColumnId(index));
            }
            ctx.scratch.setPos(columnBase);
        }
        final ObjList<BoundExpression> expressions = project.getExpressions();
        for (int i = 0, n = expressions.size(); i < n; i++) {
            BoundExpression expression = expressions.getQuick(i);
            for (int k = 0, m = compensatedIds.size(); k < m; k++) {
                expression = ctx.functionBinder.substituteColumn(expression, compensatedIds.getQuick(k), compensations.getQuick(k));
            }
            expressions.setQuick(i, expression);
        }
        compensatedIds.clear();
        compensatedNames.clear();
        compensations.clear();
        pendingConjuncts = null;
        if (hoisted == null) {
            return project;
        }
        final FilterPlan filter = ctx.planNodes.filters.next().of(project, ctx.functionBinder.remapColumns(hoisted, ctx.substitution), hoisted.getPosition());
        filter.getOutput().copyFrom(project.getOutput());
        return filter;
    }

    /**
     * Restores, in the consumer projection, the values each driven scalar count takes over no rows; its key
     * columns read the columns they join to.
     */
    void compensateDrivenInputs(JoinPlan join, int drivenBase, ProjectPlan consumer) throws SqlException {
        for (int d = drivenBase, m = drivenInputs.size(); d < m; d++) {
            final JoinInput input = drivenInputs.getQuick(d);
            final ProjectPlan scalar = drivenScalars.getQuick(d);
            if (input.getJoinType() != QueryModel.JOIN_LEFT_OUTER) {
                continue;
            }
            ctx.substitution.clear();
            for (int i = 0, n = input.getSlaveKeyColumnIds().size(); i < n; i++) {
                final int keyId = keyColumnId(scalar, input.getSlaveKeyColumnIds().getQuick(i));
                if (keyId > -1) {
                    ctx.substitution.put(keyId, input.getMasterKeyColumnIds().getQuick(i));
                }
            }
            scalarAggregate = (AggregatePlan) scalar.getInput();
            final OutputSchema output = scalar.getOutput();
            for (int i = 0, n = output.getColumnCount(); i < n; i++) {
                if (!output.isVisible(i)) {
                    continue;
                }
                final BoundExpression empty = emptyValue(scalar, i, join.getOutput());
                if (empty != null) {
                    final ObjList<BoundExpression> expressions = consumer.getExpressions();
                    for (int k = 0, e = expressions.size(); k < e; k++) {
                        expressions.setQuick(k, ctx.functionBinder.substituteColumn(expressions.getQuick(k), output.getColumnId(i), empty));
                    }
                }
            }
            scalarAggregate = null;
        }
    }

    /**
     * Records, for the projection over the join, the empty-input value of each visible column of the scalar
     * body, guarded by the body's LIMIT and by the ON condition of a LEFT LATERAL. The outer columns the body
     * maps read the master's columns.
     */
    void compensateStep(JoinInput step, ProjectPlan body, int base, BoundExpression guard, BoundExpression on) throws SqlException {
        final OutputSchema bodyOutput = body.getOutput();
        final OutputSchema input = ctx.master.getOutput();
        ctx.substitution.clear();
        for (int i = base, n = ctx.mappedOuterIds.size(); i < n; i++) {
            ctx.substitution.put(keyColumnId(body, ctx.mappedColumnIds.getQuick(i)), ctx.masterColumn(ctx.mappedOuterIds.getQuick(i)));
        }
        final int start = compensatedIds.size();
        for (int i = 0, n = bodyOutput.getColumnCount(); i < n; i++) {
            if (!bodyOutput.isVisible(i)) {
                continue;
            }
            final BoundExpression empty = emptyValue(body, i, input);
            if (empty != null || on != null || guard != null) {
                compensatedIds.add(bodyOutput.getColumnId(i));
                compensatedNames.add(null);
                compensations.add(empty != null ? empty
                        : ctx.planNodes.columns.next().of(bodyOutput.getColumnId(i), bodyOutput.getColumnType(i), step.getPosition()));
            }
        }
        if (on != null) {
            BoundExpression condition = on;
            for (int i = start, n = compensatedIds.size(); i < n; i++) {
                condition = ctx.functionBinder.substituteColumn(condition, compensatedIds.getQuick(i), compensations.getQuick(i));
            }
            for (int i = start, n = compensations.size(); i < n; i++) {
                compensations.setQuick(i, guarded(condition, compensations.getQuick(i), input, step.getPosition()));
            }
        }
        if (guard != null) {
            for (int i = start, n = compensations.size(); i < n; i++) {
                compensations.setQuick(i, guarded(guard, compensations.getQuick(i), input, step.getPosition()));
            }
        }
    }

    /**
     * Restores the compensated values in a projection over the join, for a consumer that is not a projection;
     * the consumers above read the projection's columns.
     */
    LogicalPlan consumerProjection(JoinPlan join) {
        final OutputSchema output = join.getOutput();
        final ProjectPlan project = ctx.planNodes.projects.next().of(join, join.getPosition());
        for (int i = 0, n = output.getColumnCount(); i < n; i++) {
            final int columnId = output.getColumnId(i);
            final int compensated = compensatedIds.indexOf(columnId, 0, compensatedIds.size());
            final BoundExpression value = compensated > -1 ? compensations.getQuick(compensated)
                    : ctx.planNodes.columns.next().of(columnId, output.getColumnType(i), join.getPosition());
            project.getExpressions().add(value);
            project.getOutput().add(ctx.nextColumnId, output.getColumnName(i), value.getDataType(), output.getMetadata(i), output.isVisible(i),
                    output.getColumnQualifier(i));
            consumerRemap.put(columnId, ctx.nextColumnId++);
        }
        for (int i = 0, n = compensatedIds.size(); i < n; i++) {
            final CharSequence name = compensatedNames.getQuick(i);
            if (name != null) {
                final BoundExpression value = compensations.getQuick(i);
                project.getExpressions().add(value);
                project.getOutput().add(ctx.nextColumnId, name, value.getDataType(), true);
                consumerRemap.put(compensatedIds.getQuick(i), ctx.nextColumnId++);
            }
        }
        project.getOutput().setTimestampIndex(output.getTimestampIndex());
        compensatedIds.clear();
        compensatedNames.clear();
        compensations.clear();
        final BoundExpression hoisted = pendingConjuncts;
        pendingConjuncts = null;
        if (hoisted == null) {
            return project;
        }
        final FilterPlan filter = ctx.planNodes.filters.next().of(project, ctx.functionBinder.remapColumns(hoisted, consumerRemap), hoisted.getPosition());
        filter.getOutput().copyFrom(project.getOutput());
        return filter;
    }

    /**
     * The projection of a scalar count the input joins without a condition, when only the consumer projection
     * of the join reads its columns; null otherwise.
     */
    ProjectPlan drivenScalar(JoinPlan join, JoinInput input) {
        if (!(input.getInput() instanceof ProjectPlan project) || !(project.getInput() instanceof AggregatePlan aggregate)
                || !isImplicitlyKeyedByOuterColumns(aggregate) || input.getPostJoinFilter() != null
                || input.getJoinType() != QueryModel.JOIN_CROSS && (input.getJoinType() != QueryModel.JOIN_INNER && input.getJoinType() != QueryModel.JOIN_LEFT_OUTER
                || input.getMasterKeyColumnIds().size() > 0 || !isTrue(input.getOnResidual()))) {
            return null;
        }
        boolean hasCount = false;
        for (int i = 0, n = project.getExpressions().size(); i < n; i++) {
            hasCount |= project.getOutput().isVisible(i) && hasZeroOnEmpty(project.getExpressions().getQuick(i), aggregate);
        }
        if (!hasCount) {
            return null;
        }
        final OutputSchema output = project.getOutput();
        for (int i = 1, n = join.getInputs().size(); i < n; i++) {
            final JoinInput other = join.getInputs().getQuick(i);
            if (ctx.readsAnyColumn(other.getOnResidual(), output) || ctx.readsAnyColumn(other.getPostJoinFilter(), output)) {
                return null;
            }
        }
        for (int i = ctx.chain.size() - 1; ctx.chain.getQuick(i) instanceof FilterPlan filter; i--) {
            if (ctx.readsAnyColumn(filter.getPredicate(), output)) {
                return null;
            }
        }
        return project;
    }

    /**
     * The projection of a block over a join whose scalar count inputs the domain of a LEFT LATERAL drives: each
     * such input joins LEFT to the column that maps its outer columns, and the projection restores its values
     * over no rows. Null when the block is not the body of a LEFT LATERAL or its chain reads more than the
     * projection and filters.
     */
    ProjectPlan drivingConsumer(int chainBase, boolean hasProject) {
        if (!hasProject || ctx.master == null || ctx.master.getInputs().getQuick(ctx.masterLimit).getJoinType() != QueryModel.JOIN_LEFT_OUTER
                || !(ctx.chain.getQuick(chainBase) instanceof ProjectPlan consumer)) {
            return null;
        }
        for (int i = chainBase + 1, n = ctx.chain.size(); i < n; i++) {
            if (!(ctx.chain.getQuick(i) instanceof FilterPlan)) {
                return null;
            }
        }
        return consumer;
    }

    /**
     * Exposes, as hidden columns of the projection, the aggregate outputs that the empty-input values of its
     * visible columns read through other expressions, recording each in {@link #carrierColumnIds}.
     */
    void exposeCarriers(ProjectPlan project) {
        final AggregatePlan aggregate = scalarAggregateBelow(project);
        final OutputSchema output = project.getOutput();
        final int keyCount = aggregate.getGroupingExpressions().size();
        for (int i = 0, n = output.getColumnCount(); i < n; i++) {
            final BoundExpression expression = project.getExpressions().getQuick(i);
            if (!output.isVisible(i) || expression instanceof ColumnExpression || !hasZeroOnEmpty(expression, aggregate)) {
                continue;
            }
            final int columnBase = ctx.scratch.size();
            collectColumnIds(expression, ctx.scratch);
            for (int k = columnBase, m = ctx.scratch.size(); k < m; k++) {
                final int columnId = ctx.scratch.getQuick(k);
                final int aggregateIndex = aggregate.getOutput().getColumnIndexById(columnId);
                if (aggregateIndex >= keyCount && pairIndex(carrierColumnIds, columnId) < 0) {
                    final int type = aggregate.getOutput().getColumnType(aggregateIndex);
                    project.getExpressions().add(ctx.planNodes.columns.next().of(columnId, type, project.getPosition()));
                    project.getOutput().add(ctx.nextColumnId, carrierName(), type, false);
                    carrierColumnIds.add(columnId);
                    carrierColumnIds.add(ctx.nextColumnId++);
                }
            }
            ctx.scratch.setPos(columnBase);
        }
    }

    boolean hasZeroOnEmptyColumn(ProjectPlan project) {
        final AggregatePlan aggregate = scalarAggregateBelow(project);
        for (int i = 0, n = project.getExpressions().size(); i < n; i++) {
            if (project.getOutput().isVisible(i) && project.getExpressions().getQuick(i) instanceof ColumnExpression column
                    && isZeroOnEmptyColumn(column.getColumnId(), aggregate)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Takes the WHERE conjuncts of the join that read compensated values, which must apply to the restored values.
     */
    void hoistCompensatedConjuncts(JoinPlan join) throws SqlException {
        BoundExpression hoisted = null;
        for (int i = 1, n = join.getInputs().size(); i < n; i++) {
            final JoinInput input = join.getInputs().getQuick(i);
            hoistedConjuncts.clear();
            input.setPostJoinFilter(takeCompensatedConjuncts(input.getPostJoinFilter()));
            for (int k = 0, m = hoistedConjuncts.size(); k < m; k++) {
                hoisted = hoisted == null ? hoistedConjuncts.getQuick(k) : ctx.functionBinder.combineConjunction(hoisted, hoistedConjuncts.getQuick(k), hoisted.getPosition());
            }
            hoistedConjuncts.clear();
        }
        final ObjList<BoundExpression> conjuncts = join.getFilterConjuncts();
        for (int i = conjuncts.size() - 1; i > -1; i--) {
            if (readsCompensated(conjuncts.getQuick(i))) {
                conjuncts.remove(i);
                join.getFilterConjunctOrigins().removeIndex(i);
            }
        }
        pendingConjuncts = hoisted;
    }

    /**
     * True when the projection reads a keyless aggregate, or one keyed only by outer columns, of a correlated
     * block, and selects only its aggregates.
     */
    boolean isScalarProjection(ProjectPlan project) {
        if (!(project.getInput() instanceof AggregatePlan aggregate) || !isImplicitlyKeyedByOuterColumns(aggregate)
                || !LogicalPlans.hasOuterColumn(project, ctx.scratch)) {
            return false;
        }
        final int keyCount = aggregate.getGroupingExpressions().size();
        for (int i = 0, n = project.getExpressions().size(); i < n; i++) {
            if (project.getOutput().isVisible(i) && !(project.getExpressions().getQuick(i) instanceof ColumnExpression column
                    && aggregate.getOutput().getColumnIndexById(column.getColumnId()) >= keyCount)) {
                return false;
            }
        }
        return true;
    }

    BoundExpression limitComparison(CharSequence operator, BoundExpression limit, BoundExpression rank, OutputSchema input, int position)
            throws SqlException {
        ctx.callArguments.clear();
        ctx.callArguments.add(limit);
        final BoundExpression guard = ctx.bindCall("__lateral_limit", limit.getPosition(), input);
        return ctx.bindCall(operator, position, guard, rank, input);
    }

    /**
     * Whether the body's LIMIT keeps the single row a scalar body yields.
     */
    BoundExpression limitGuard(BoundExpression lo, BoundExpression hi, OutputSchema input) throws SqlException {
        final ConstantExpression one = ctx.planNodes.constants.next().ofInt(1, lo.getPosition());
        if (hi == null) {
            return limitComparison(">=", lo, one, input, lo.getPosition());
        }
        final BoundExpression upper = limitComparison(">=", hi, one, input, hi.getPosition());
        return ctx.functionBinder.combineConjunction(limitComparison("<", lo, one, input, lo.getPosition()), upper, lo.getPosition());
    }

    /**
     * The LIMIT of a scalar aggregate keyed per outer row, which keeps the one row of each key or none.
     */
    LogicalPlan limitGuardFilter(LimitPlan limit, LogicalPlan input) throws SqlException {
        final BoundExpression outerLimit = outerLimit(limit);
        if (outerLimit != null) {
            throw SqlException.$(outerLimit.getPosition(), OUTER_LIMIT_OVER_COUNT);
        }
        final FilterPlan filter = ctx.planNodes.filters.next().of(input, limitGuard(limit.getLo(), limit.getHi(), input.getOutput()), limit.getPosition());
        filter.getOutput().copyFrom(input.getOutput());
        return filter;
    }

    AggregatePlan scalarAggregateBelow(ProjectPlan project) {
        LogicalPlan input = project.getInput();
        while (input instanceof SortPlan || input instanceof FillPlan) {
            input = input.inputAt(0);
        }
        return input == scalarAggregate ? scalarAggregate : null;
    }

    /**
     * The projection of a body that is a keyless aggregate keyed only by the decorrelation, or null.
     */
    ProjectPlan scalarBody(LogicalPlan body) {
        LogicalPlan node = body;
        while (node instanceof FilterPlan || node instanceof WindowPlan) {
            node = node.inputAt(0);
        }
        return node instanceof ProjectPlan project && scalarAggregate != null && scalarAggregateBelow(project) != null ? project : null;
    }

    /**
     * The projection at the top of a body that wraps {@code scalar}, once the scalar's aggregate is keyed by the
     * decorrelation, or null.
     */
    ProjectPlan wrappedBody(LogicalPlan body, ProjectPlan scalar) {
        LogicalPlan node = body;
        while (node instanceof FilterPlan || node instanceof WindowPlan) {
            node = node.inputAt(0);
        }
        return scalarAggregate != null && scalarAggregateBelow(scalar) != null ? (ProjectPlan) node : null;
    }

    /**
     * The scalar projection a body wraps, when the LEFT join's NULL is the value of every column of the body
     * over no rows but the bare counts: the body projects null-propagating arithmetic over the scalar's
     * aggregates and reads its counts only bare, through column projections and a WHERE that keeps every count.
     */
    ProjectPlan wrappedScalar(LogicalPlan body) {
        if (!(body instanceof ProjectPlan top)) {
            return null;
        }
        LogicalPlan node = top.getInput();
        FilterPlan filter = null;
        while (!(node instanceof ProjectPlan project && isScalarProjection(project))) {
            if (filter == null && node instanceof FilterPlan f) {
                filter = f;
            } else if (!(filter == null && node instanceof ProjectPlan project && LogicalPlans.isColumnProjection(project))) {
                return null;
            }
            node = node.inputAt(0);
        }
        final ProjectPlan scalar = (ProjectPlan) node;
        if (filter != null && !keepsEveryCount(filter.getPredicate(), scalar)) {
            return null;
        }
        for (int i = 0, n = top.getExpressions().size(); i < n; i++) {
            if (top.getOutput().isVisible(i) && !isWrappedScalarColumn(top.getExpressions().getQuick(i), top.getInput())) {
                return null;
            }
        }
        return scalar;
    }

    /**
     * The scalar projection under the single-input chain of the body whose WHERE drops the row of a count over
     * no rows, which then needs no compensation.
     */
    ProjectPlan zeroRejectingScalar(LogicalPlan body) {
        LogicalPlan node = body;
        while (node.inputCount() == 1 && !(node instanceof JoinPlan)) {
            if (node instanceof FilterPlan filter && filter.getInput() instanceof ProjectPlan project && isScalarProjection(project)
                    && rejectsZeroCount(filter.getPredicate(), project)) {
                return project;
            }
            node = node.inputAt(0);
        }
        return null;
    }
}
