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
import io.questdb.griffin.BoundExpressionRewriter.ConjunctTest;
import io.questdb.griffin.CharacterStoreEntry;
import io.questdb.griffin.LogicalPlans;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.plan.logical.AggregatePlan;
import io.questdb.griffin.plan.logical.BoundExpression;
import io.questdb.griffin.plan.logical.ColumnExpression;
import io.questdb.griffin.plan.logical.ConstantExpression;
import io.questdb.griffin.plan.logical.FillPlan;
import io.questdb.griffin.plan.logical.FilterPlan;
import io.questdb.griffin.plan.logical.FunctionExpression;
import io.questdb.griffin.plan.logical.JoinInput;
import io.questdb.griffin.plan.logical.JoinKind;
import io.questdb.griffin.plan.logical.JoinPlan;
import io.questdb.griffin.plan.logical.LimitPlan;
import io.questdb.griffin.plan.logical.LogicalPlan;
import io.questdb.griffin.plan.logical.OuterColumnExpression;
import io.questdb.griffin.plan.logical.OutputSchema;
import io.questdb.griffin.plan.logical.ProjectPlan;
import io.questdb.griffin.plan.logical.SortPlan;
import io.questdb.griffin.plan.logical.WindowPlan;
import io.questdb.std.Chars;
import io.questdb.std.IntIntHashMap;
import io.questdb.std.IntList;
import io.questdb.std.Mutable;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;

import static io.questdb.griffin.optimiser.DecorrelationContext.isTrue;
import static io.questdb.griffin.optimiser.DecorrelationContext.pairIndex;

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
    private final ObjList<BoundExpression> hoistedConjuncts = new ObjList<>();
    private final ConjunctTest uncompensatedConjuncts = this::keepsUncompensatedConjunct;
    AggregatePlan scalarAggregate;
    private BoundExpression pendingConjuncts;
    private ProjectPlan scalarProjection;

    ScalarCompensation(DecorrelationContext ctx, DecorrelationDomains domains) {
        this.ctx = ctx;
        this.domains = domains;
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
        hoistedConjuncts.clear();
        scalarAggregates.clear();
        uncompensatedScalars.clear();
        pendingConjuncts = null;
        scalarAggregate = null;
        scalarProjection = null;
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

    /**
     * The projection under the filters and windows at the top of the body, or null when another node is there.
     */
    private static ProjectPlan topProjection(LogicalPlan body) {
        LogicalPlan node = body;
        while (node instanceof FilterPlan || node instanceof WindowPlan) {
            node = node.inputAt(0);
        }
        return node instanceof ProjectPlan project ? project : null;
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
        final int columnBase = ctx.tmpColumnIds.size();
        ctx.collectColumnIds(expression, ctx.tmpColumnIds);
        BoundExpression value = expression;
        for (int i = columnBase, n = ctx.tmpColumnIds.size(); i < n; i++) {
            final int columnId = ctx.tmpColumnIds.getQuick(i);
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
                value = ctx.context.getRewriter().substituteColumn(value, columnId, replacement);
            }
        }
        ctx.tmpColumnIds.setPos(columnBase);
        return value;
    }

    private BoundExpression guarded(BoundExpression condition, BoundExpression value, OutputSchema input, int position) throws SqlException {
        return ctx.caseWhen(condition, value, ctx.planNodes.constants.next().ofNull(position), input, position);
    }

    /**
     * True when column {@code index} of {@link #scalarProjection} is a bare count.
     */
    private boolean isScalarCount(int index) {
        final BoundExpression expression = scalarProjection.getExpressions().getQuick(index);
        return expression instanceof ColumnExpression column && isZeroOnEmptyColumn(column.getColumnId(), (AggregatePlan) scalarProjection.getInput());
    }

    private boolean isWrappedScalarColumn(BoundExpression expression, LogicalPlan input) {
        if (!isNullPropagating(expression)) {
            return false;
        }
        if (expression instanceof ColumnExpression column) {
            return scalarColumnIndex(input, column.getColumnId()) > -1;
        }
        final int columnBase = ctx.tmpColumnIds.size();
        ctx.collectColumnIds(expression, ctx.tmpColumnIds);
        boolean hasAggregate = false;
        boolean hasCount = false;
        for (int i = columnBase, n = ctx.tmpColumnIds.size(); i < n; i++) {
            final int index = scalarColumnIndex(input, ctx.tmpColumnIds.getQuick(i));
            hasAggregate |= index > -1;
            hasCount |= index > -1 && isScalarCount(index);
        }
        ctx.tmpColumnIds.setPos(columnBase);
        return hasAggregate && !hasCount;
    }

    /**
     * Keeps a conjunct that reads no compensated column; appends the others to {@link #hoistedConjuncts}.
     */
    private boolean keepsUncompensatedConjunct(BoundExpression conjunct) {
        if (ctx.readsAnyColumn(conjunct, compensatedIds)) {
            hoistedConjuncts.add(conjunct);
            return false;
        }
        return true;
    }

    /**
     * The index in the scalar projection, which {@link #scalarProjection} then holds, of the column {@code columnId}
     * of {@code node} reads through column projections and filters, or -1.
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
                    scalarProjection = project;
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
        return index > -1 && isScalarCount(index) ? index : -1;
    }

    private BoundExpression zeroCoalesce(ColumnExpression value, OutputSchema input) throws SqlException {
        return ctx.context.bindCall("coalesce", value.getPosition(), value, ctx.planNodes.constants.next().ofInt(0, value.getPosition()), input);
    }


    static boolean isImplicitlyKeyedByOuterColumns(AggregatePlan aggregate) {
        if (aggregate.hasExplicitGrouping() || aggregate.hasSampleByBucket()) {
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
        final JoinInput projectInput = ctx.planNodes.joinInputs.next().of(project, JoinKind.LEFT_OUTER, null, position);
        join.addInput(ctx.planNodes.joinInputs.next().of(domain, JoinKind.CROSS, domains.domainAlias(), position));
        join.addInput(projectInput);
        join.getOutput().copyFrom(domain.getOutput());
        join.getOutput().addMissingColumnsFrom(project.getOutput());
        final OutputSchema output = project.getOutput();
        ctx.substitution.clear();
        for (int i = base; i < domainBase; i++) {
            final int columnId = ctx.mappedColumnIds.getQuick(i);
            final int domainId = ctx.mappedColumnIds.getQuick(i - base + domainBase);
            projectInput.addKey(domainId, columnId, domain.getOutput().getColumnName(domain.getOutput().getColumnIndexById(domainId)),
                    output.getColumnName(output.getColumnIndexById(columnId)), position);
            ctx.substitution.put(LogicalPlans.projectedSourceColumnId(project, columnId), domainId);
        }
        final ProjectPlan compensated = ctx.planNodes.projects.next().of(join, position);
        for (int i = 0, n = output.getColumnCount(); i < n; i++) {
            if (output.isVisible(i)) {
                final int columnId = output.getColumnId(i);
                renameJoinedColumn(join, output, i, ctx.context.newColumnId());
                final BoundExpression empty = emptyValue(project, i, join.getOutput());
                final BoundExpression value = empty != null ? empty : ctx.planNodes.columns.next().of(output.getColumnId(i), output.getColumnType(i), position);
                compensated.getExpressions().add(value);
                compensated.getOutput().add(columnId, output.getColumnName(i), value.getDataType(), output.getMetadata(i), true);
            }
        }
        for (int i = domainBase, n = ctx.mappedOuterIds.size(); i < n; i++) {
            ctx.mappedColumnIds.setQuick(i - domainBase + base,
                    ctx.exposeColumn(compensated, join.getOutput(), ctx.mappedColumnIds.getQuick(i), ctx.outerRefName(ctx.mappedOuterIds.getQuick(i)), position));
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
            final int columnBase = ctx.tmpColumnIds.size();
            ctx.collectColumnIds(hoisted, ctx.tmpColumnIds);
            for (int i = columnBase, n = ctx.tmpColumnIds.size(); i < n; i++) {
                final int columnId = ctx.tmpColumnIds.getQuick(i);
                final int index = LogicalPlans.projectedUncastColumnIndex(project, columnId);
                if (index < 0) {
                    ctx.tmpColumnIds.setPos(columnBase);
                    return null;
                }
                ctx.substitution.put(columnId, project.getOutput().getColumnId(index));
            }
            ctx.tmpColumnIds.setPos(columnBase);
        }
        final ObjList<BoundExpression> expressions = project.getExpressions();
        for (int i = 0, n = expressions.size(); i < n; i++) {
            BoundExpression expression = expressions.getQuick(i);
            for (int k = 0, m = compensatedIds.size(); k < m; k++) {
                expression = ctx.context.getRewriter().substituteColumn(expression, compensatedIds.getQuick(k), compensations.getQuick(k));
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
        return ctx.planNodes.filters.next().of(project, ctx.context.getRewriter().remapColumns(hoisted, ctx.substitution), hoisted.getPosition());
    }

    /**
     * Restores, in the consumer projection, the values each driven scalar count takes over no rows; its key
     * columns read the columns they join to.
     */
    void compensateDrivenInputs(JoinPlan join, int drivenBase, ProjectPlan consumer) throws SqlException {
        for (int d = drivenBase, m = drivenInputs.size(); d < m; d++) {
            final JoinInput input = drivenInputs.getQuick(d);
            final ProjectPlan scalar = drivenScalars.getQuick(d);
            if (input.getJoinType() != JoinKind.LEFT_OUTER) {
                continue;
            }
            ctx.substitution.clear();
            for (int i = 0, n = input.getSlaveKeyColumnIds().size(); i < n; i++) {
                final int keyId = LogicalPlans.projectedSourceColumnId(scalar, input.getSlaveKeyColumnIds().getQuick(i));
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
                        expressions.setQuick(k, ctx.context.getRewriter().substituteColumn(expressions.getQuick(k), output.getColumnId(i), empty));
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
            ctx.substitution.put(LogicalPlans.projectedSourceColumnId(body, ctx.mappedColumnIds.getQuick(i)), ctx.masterColumn(ctx.mappedOuterIds.getQuick(i)));
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
                condition = ctx.context.getRewriter().substituteColumn(condition, compensatedIds.getQuick(i), compensations.getQuick(i));
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
        final ProjectPlan project = ctx.forwardingProjection(join, consumerRemap, join.getPosition());
        for (int i = 0, n = compensatedIds.size(); i < n; i++) {
            final BoundExpression value = compensations.getQuick(i);
            final CharSequence name = compensatedNames.getQuick(i);
            if (name != null) {
                project.getExpressions().add(value);
                final int remappedId = ctx.context.newColumnId();
                project.getOutput().add(remappedId, name, value.getDataType(), true);
                consumerRemap.put(compensatedIds.getQuick(i), remappedId);
            } else {
                final int index = output.getColumnIndexById(compensatedIds.getQuick(i));
                project.getExpressions().setQuick(index, value);
                project.getOutput().setColumnType(index, value.getDataType());
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
        return ctx.planNodes.filters.next().of(project, ctx.context.getRewriter().remapColumns(hoisted, consumerRemap), hoisted.getPosition());
    }

    /**
     * The projection of a scalar count the input joins without a condition, when only the consumer projection
     * of the join reads its columns; null otherwise.
     */
    ProjectPlan drivenScalar(JoinPlan join, JoinInput input) {
        if (!(input.getInput() instanceof ProjectPlan project) || !(project.getInput() instanceof AggregatePlan aggregate)
                || !isImplicitlyKeyedByOuterColumns(aggregate) || input.getPostJoinFilter() != null
                || input.getJoinType() != JoinKind.CROSS && (input.getJoinType() != JoinKind.INNER && input.getJoinType() != JoinKind.LEFT_OUTER
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
        if (!hasProject || ctx.master == null || ctx.master.getInputs().getQuick(ctx.masterLimit).getJoinType() != JoinKind.LEFT_OUTER
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
            final int columnBase = ctx.tmpColumnIds.size();
            ctx.collectColumnIds(expression, ctx.tmpColumnIds);
            for (int k = columnBase, m = ctx.tmpColumnIds.size(); k < m; k++) {
                final int columnId = ctx.tmpColumnIds.getQuick(k);
                final int aggregateIndex = aggregate.getOutput().getColumnIndexById(columnId);
                if (aggregateIndex >= keyCount && pairIndex(carrierColumnIds, columnId) < 0) {
                    final int type = aggregate.getOutput().getColumnType(aggregateIndex);
                    project.getExpressions().add(ctx.planNodes.columns.next().of(columnId, type, project.getPosition()));
                    final int carrierId = ctx.context.newColumnId();
                    project.getOutput().add(carrierId, carrierName(), type, false);
                    carrierColumnIds.add(columnId);
                    carrierColumnIds.add(carrierId);
                }
            }
            ctx.tmpColumnIds.setPos(columnBase);
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
            input.setPostJoinFilter(ctx.context.getRewriter().retainConjuncts(input.getPostJoinFilter(), uncompensatedConjuncts));
            for (int k = 0, m = hoistedConjuncts.size(); k < m; k++) {
                hoisted = hoisted == null ? hoistedConjuncts.getQuick(k) : ctx.context.getRewriter().combineConjunction(hoisted, hoistedConjuncts.getQuick(k), hoisted.getPosition());
            }
            hoistedConjuncts.clear();
        }
        final ObjList<BoundExpression> conjuncts = join.getFilterConjuncts();
        for (int i = conjuncts.size() - 1; i > -1; i--) {
            if (ctx.readsAnyColumn(conjuncts.getQuick(i), compensatedIds)) {
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
                || !ctx.outerColumnReads.isReadBy(project, ctx.tmpColumnIds)) {
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
        ctx.context.getCallArguments().clear();
        ctx.context.getCallArguments().add(limit);
        final BoundExpression guard = ctx.context.bindCall("__lateral_limit", limit.getPosition(), input);
        return ctx.context.bindCall(operator, position, guard, rank, input);
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
        return ctx.context.getRewriter().combineConjunction(limitComparison("<", lo, one, input, lo.getPosition()), upper, lo.getPosition());
    }

    /**
     * The LIMIT of a scalar aggregate keyed per outer row, which keeps the one row of each key or none.
     */
    LogicalPlan limitGuardFilter(LimitPlan limit, LogicalPlan input) throws SqlException {
        final BoundExpression outerLimit = outerLimit(limit);
        if (outerLimit != null) {
            throw SqlException.$(outerLimit.getPosition(), OUTER_LIMIT_OVER_COUNT);
        }
        return ctx.planNodes.filters.next().of(input, limitGuard(limit.getLo(), limit.getHi(), input.getOutput()), limit.getPosition());
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
        final ProjectPlan project = topProjection(body);
        return project != null && scalarAggregate != null && scalarAggregateBelow(project) != null ? project : null;
    }

    /**
     * Forgets the driven inputs above {@code drivenBase} and the uncompensated scalars above
     * {@code uncompensatedBase}.
     */
    void truncateDriven(int drivenBase, int uncompensatedBase) {
        drivenInputs.setPos(drivenBase);
        drivenScalars.setPos(drivenBase);
        uncompensatedScalars.setPos(uncompensatedBase);
    }

    /**
     * The projection at the top of a body that wraps {@code scalar}, once the scalar's aggregate is keyed by the
     * decorrelation, or null.
     */
    ProjectPlan wrappedBody(LogicalPlan body, ProjectPlan scalar) {
        return scalarAggregate != null && scalarAggregateBelow(scalar) != null ? topProjection(body) : null;
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
