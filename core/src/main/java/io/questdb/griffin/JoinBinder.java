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

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.ColumnType;
import io.questdb.griffin.model.ExpressionNode;
import io.questdb.griffin.model.QueryModel;
import io.questdb.griffin.plan.logical.BoundExpression;
import io.questdb.griffin.plan.logical.ColumnExpression;
import io.questdb.griffin.plan.logical.ConstantExpression;
import io.questdb.griffin.plan.logical.FilterPlan;
import io.questdb.griffin.plan.logical.JoinInput;
import io.questdb.griffin.plan.logical.JoinKind;
import io.questdb.griffin.plan.logical.JoinPlan;
import io.questdb.griffin.plan.logical.LimitPlan;
import io.questdb.griffin.plan.logical.LogicalPlan;
import io.questdb.griffin.plan.logical.OutputSchema;
import io.questdb.griffin.plan.logical.ProjectPlan;
import io.questdb.griffin.plan.logical.ScanPlan;
import io.questdb.griffin.plan.logical.UnnestSpec;
import io.questdb.std.Chars;
import io.questdb.std.GenericLexer;
import io.questdb.std.IntHashSet;
import io.questdb.std.IntList;
import io.questdb.std.Misc;
import io.questdb.std.Mutable;
import io.questdb.std.ObjList;

import static io.questdb.griffin.BindContext.*;
import static io.questdb.griffin.TemporalJoinBinder.isTemporalAnchor;

final class JoinBinder implements Mutable {
    private final SqlBinder binder;
    private final CairoConfiguration configuration;
    private final BindContext ctx;
    private final IntList deferredJoinInputs = new IntList();
    private final ObjList<ExpressionNode> forwardJoinReferences = new ObjList<>();
    private final IntList forwardLeftJoinInputs = new IntList();
    private final ObjList<QueryModel> hintAliasModels = new ObjList<>();
    private final ObjList<CharSequence> hintAliases = new ObjList<>();
    private final ObjList<ExpressionNode> joinFilterNodes = new ObjList<>();
    private final ObjList<ExpressionNode> joinOnNodes = new ObjList<>();
    private final JoinOrderSolver joinOrder;
    private final ObjList<ExpressionNode> joinResidualNodes = new ObjList<>();
    private final IntList joinResidualOrigins = new IntList();
    private final LateralBinder lateralBinder;
    private final IntList lateralDependencyInputs = new IntList();
    private final IntList lateralDependencyParents = new IntList();
    private final IntList nonEquiNullingJoinInputs = new IntList();
    private final IntList nullingJoinBoundaries = new IntList();
    private boolean isForwardInnerFiltered;
    private boolean isForwardLeftKeyFiltered;

    JoinBinder(
            BindContext ctx,
            SqlBinder binder,
            CairoConfiguration configuration,
            LateralBinder lateralBinder,
            IntHashSet tmpIds,
            IntList stagedIndexes,
            IntList bestOrder,
            IntList roots
    ) {
        this.ctx = ctx;
        this.binder = binder;
        this.configuration = configuration;
        this.lateralBinder = lateralBinder;
        this.joinOrder = new JoinOrderSolver(nonEquiNullingJoinInputs, nullingJoinBoundaries, tmpIds, stagedIndexes, bestOrder, roots);
    }

    @Override
    public void clear() {
        hintAliasModels.clear();
        hintAliases.clear();
        joinFilterNodes.clear();
        joinOnNodes.clear();
        joinOrder.clear();
        joinResidualNodes.clear();
        forwardJoinReferences.clear();
        joinResidualOrigins.clear();
        lateralDependencyInputs.clear();
        lateralDependencyParents.clear();
    }

    private static boolean canExtractJoinKey(JoinPlan join, int left, int right, int origin, boolean hasNonEquiNullingJoin) {
        if (left == right || origin < 0 && hasNonEquiNullingJoin) {
            return false;
        }
        final int higher = Math.max(left, right);
        if (join.getInputs().getQuick(higher).getUnnest() != null) {
            return false;
        }
        if (origin >= 0 && JoinOrderSolver.isBarrier(join.getInputs().getQuick(origin).getJoinType())) {
            return higher == origin;
        }
        if (JoinOrderSolver.isBarrier(join.getInputs().getQuick(higher).getJoinType())) {
            return false;
        }
        final int last = origin < 0 ? join.getInputs().size() - 1 : origin;
        for (int i = (origin < 0 ? Math.min(left, right) : higher) + 1; i <= last; i++) {
            if (join.getInputs().getQuick(i).getJoinType().isMasterNulling()) {
                return false;
            }
        }
        return true;
    }

    /**
     * A comma binds looser than JOIN, so a nulling join's prefix starts at the last comma.
     */
    private static int commaGroupStart(QueryModel source, int input) {
        for (int i = input; i > 0; i--) {
            if (source.getJoinModels().getQuick(i).isCommaJoin()) {
                return i;
            }
        }
        return 0;
    }

    private static ExpressionNode findUnnestColumn(ExpressionNode node, int position) {
        if (node == null || node.type == ExpressionNode.QUERY) {
            return null;
        }
        if (node.type == ExpressionNode.LITERAL && node.position == position) {
            return node;
        }
        ExpressionNode column = findUnnestColumn(node.lhs, position);
        if (column == null) {
            column = findUnnestColumn(node.rhs, position);
        }
        for (int i = 0, n = node.args.size(); column == null && i < n; i++) {
            column = findUnnestColumn(node.args.getQuick(i), position);
        }
        return column;
    }

    private static SqlException forwardJoinReference(ExpressionNode reference) {
        return SqlException.$(reference.position, "join condition references a table joined later [column=").put(reference.token).put(']');
    }

    // A temporal join with an ON residual stays after an earlier RIGHT/FULL join, which drops the master timestamp.
    private static boolean hasPrecedingMasterNullingJoin(JoinPlan join, JoinInput slave) {
        if (join.hasExplicitTimestamp()) {
            return false;
        }
        for (int i = 1, n = join.getInputs().indexOf(slave); i < n; i++) {
            if (join.getInputs().getQuick(i).getJoinType().isMasterNulling()) {
                return true;
            }
        }
        return false;
    }

    private static boolean isCompileTimeJoinConstant(ExpressionNode expression) {
        if (expression == null) {
            return true;
        }
        return switch (expression.type) {
            case ExpressionNode.CONSTANT -> true;
            case ExpressionNode.OPERATION -> isCompileTimeJoinConstant(expression.lhs)
                    && isCompileTimeJoinConstant(expression.rhs);
            default -> false;
        };
    }

    private static JoinKind joinKind(int modelJoinType) {
        return switch (modelJoinType) {
            case QueryModel.JOIN_INNER, QueryModel.JOIN_LATERAL_INNER -> JoinKind.INNER;
            case QueryModel.JOIN_LEFT_OUTER, QueryModel.JOIN_LATERAL_LEFT -> JoinKind.LEFT_OUTER;
            case QueryModel.JOIN_RIGHT_OUTER -> JoinKind.RIGHT_OUTER;
            case QueryModel.JOIN_FULL_OUTER -> JoinKind.FULL_OUTER;
            case QueryModel.JOIN_CROSS, QueryModel.JOIN_LATERAL_CROSS -> JoinKind.CROSS;
            case QueryModel.JOIN_ASOF -> JoinKind.ASOF;
            case QueryModel.JOIN_LT -> JoinKind.LT;
            case QueryModel.JOIN_SPLICE -> JoinKind.SPLICE;
            default -> throw new IllegalStateException("unexpected join type in join block");
        };
    }

    private static int mergeJoinSources(int left, int right) {
        return left == -1 ? right : right == -1 || left == right ? left : -2;
    }

    private static void validateOuterJoinColumns(ExpressionNode expression, OutputSchema output) throws SqlException {
        if (expression == null) {
            return;
        }
        if (expression.type == ExpressionNode.LITERAL) {
            final CharSequence token = expression.token;
            final int dot = Chars.indexOfLastUnquoted(token, '.');
            if (dot > -1 && FunctionBinder.findColumn(expression, output, null) == -1
                    && !FunctionBinder.isUnknownQualifier(GenericLexer.unquote(token.subSequence(0, dot)), output, null)) {
                throw SqlException.position(expression.position).put("Invalid column: ").put(token, dot + 1, token.length());
            }
            return;
        }
        validateOuterJoinColumns(expression.lhs, output);
        validateOuterJoinColumns(expression.rhs, output);
        for (int i = 0, n = expression.args.size(); i < n; i++) {
            validateOuterJoinColumns(expression.args.getQuick(i), output);
        }
    }

    private void addJoinConstantFilter(JoinInput last, BoundExpression predicate, int position) throws SqlException {
        if (predicate instanceof ConstantExpression constant) {
            if (constant.getLongValue() == 0) {
                last.setPostJoinFilter(ctx.expressionRewriter.combineConjunction(last.getPostJoinFilter(), constant.markLiteral(), position));
            }
            return;
        }
        last.setPostJoinFilter(ctx.expressionRewriter.combineConjunction(last.getPostJoinFilter(), predicate, position));
    }

    private void addLateralDependencies(JoinPlan join, int index, int outerColumnBase) {
        final IntList outerColumnIds = ctx.functionBinder.getOuterColumnIds();
        for (int i = outerColumnBase, n = outerColumnIds.size(); i < n; i++) {
            final int columnId = outerColumnIds.getQuick(i);
            for (int k = 0; k < index; k++) {
                if (join.getInputs().getQuick(k).getSourceOutput().getColumnIndexById(columnId) > -1) {
                    lateralDependencyInputs.add(index);
                    lateralDependencyParents.add(k);
                    break;
                }
            }
        }
    }

    private ExpressionNode appendInnerOnGroup(ExpressionNode prefix, ExpressionNode expression, JoinPlan join, int group) throws SqlException {
        if (expression.paramCount == 2 && SqlKeywords.isAndKeyword(expression.token)) {
            return appendInnerOnGroup(appendInnerOnGroup(prefix, expression.lhs, join, group), expression.rhs, join, group);
        }
        if (innerOnGroup(expression, join) != group) {
            return prefix;
        }
        return appendJoinConjuncts(prefix, expression);
    }

    private ExpressionNode appendJoinConjuncts(ExpressionNode prefix, ExpressionNode expression) {
        if (expression.paramCount == 2 && SqlKeywords.isAndKeyword(expression.token)) {
            return appendJoinConjuncts(appendJoinConjuncts(prefix, expression.lhs), expression.rhs);
        }
        if (prefix == null) {
            return expression;
        }
        final ExpressionNode result = combineJoinPredicates(prefix, expression);
        result.precedence = OperatorExpression.chooseRegistry(configuration.getCairoSqlLegacyOperatorPrecedence())
                .getOperatorDefinition("and").precedence;
        return result;
    }

    private LogicalPlan bindJoinBlock(QueryModel source, ExpressionNode where, JoinPlan join, int dependencyBase,
                                      boolean hasUnnest, SqlExecutionContext executionContext) throws SqlException {
        final QueryModel rightModel = source.getJoinModels().getQuick(1);
        if (join.getInputs().size() > 2 || hasUnnest) {
            bindJoinConditions(join, source, where, dependencyBase, executionContext);
            return join;
        }
        final JoinInput master = join.getInputs().getQuick(0);
        final JoinInput slave = join.getInputs().getQuick(1);
        final JoinKind joinType = slave.getJoinType();
        join.getOrderedInputs().addAll(join.getInputs());
        final boolean isInner = joinType == JoinKind.INNER || joinType == JoinKind.CROSS;
        join.getOutput().setTimestampIndex(joinType.isMasterNulling() && !join.hasExplicitTimestamp()
                ? -1 : master.getSourceOutput().getTimestampIndex());
        ExpressionNode constantFilter = selectJoinConstantTerms(where, true);
        ExpressionNode postFilter = selectJoinConstantTerms(where, false);
        final ExpressionNode onCriteria = rightModel.getJoinCriteria();
        final ExpressionNode onConditions;
        if (isInner) {
            constantFilter = combineJoinPredicates(constantFilter, selectJoinConstantTerms(onCriteria, true));
            onConditions = selectJoinConstantTerms(onCriteria, false);
        } else {
            // Outer ON evaluates its complete matching predicate before NULL
            // extension; its constant terms never join the global WHERE group.
            onConditions = onCriteria;
        }
        if (joinType == JoinKind.SPLICE) {
            validateSpliceOnAnalysis(onConditions, join);
        }
        ExpressionNode onFilter = extractJoinKeys(onConditions, join, slave);
        if (isInner) {
            postFilter = extractJoinKeys(postFilter, join, slave);
        }
        final ObjList<ExpressionNode> shorthand = rightModel.getJoinColumns();
        for (int i = 0, n = shorthand.size(); i < n; i++) {
            final ExpressionNode column = shorthand.getQuick(i);
            final int leftIndex = ctx.bindColumnIndex(column, master.getSourceOutput(), master.getBindingAlias());
            final int rightIndex = ctx.bindColumnIndex(column, slave.getSourceOutput(), slave.getBindingAlias());
            addJoinKey(slave, master.getSourceOutput().getColumnId(leftIndex),
                    slave.getSourceOutput().getColumnId(rightIndex),
                    ctx.qualifiedJoinName(master.getBindingAlias(), column.token), ctx.qualifiedJoinName(slave.getBindingAlias(), column.token),
                    column.position);
        }
        if (joinType == JoinKind.CROSS && slave.getMasterKeyColumnIds().size() > 0) {
            slave.setJoinType(JoinKind.INNER);
        }
        if (isInner && onFilter != null) {
            // Inner ON conjuncts bind per source in join order: master-only, slave-only, then joined.
            onFilter = appendInnerOnGroup(appendInnerOnGroup(appendInnerOnGroup(null, onFilter, join, 0), onFilter, join, 1), onFilter, join, 2);
        }
        bindJoinOnResidual(onFilter, join, slave, source, master.getSourceOutput().getTimestampColumnId(), 1, executionContext);
        if (postFilter != null) {
            slave.setPostJoinFilter(bindJoinConjunct(postFilter, join, source,
                    1, -1, false, executionContext));
        }
        if (constantFilter != null) {
            addJoinConstantFilter(slave, bindJoinConstantFilter(constantFilter, join, source, executionContext), constantFilter.position);
        }
        for (int i = 0; i < 2; i++) {
            if (!LogicalPlans.canPushJoinFilter(join, i, 1)) {
                ctx.stopTimestampIntrinsics(join.getInputs().getQuick(i).getSourceOutput());
            }
        }
        return join;
    }

    private void bindJoinConditions(JoinPlan join, QueryModel source, ExpressionNode where, int dependencyBase,
                                    SqlExecutionContext executionContext) throws SqlException {
        joinOrder.of(join);
        joinResidualNodes.clear();
        joinResidualOrigins.clear();
        joinOnNodes.clear();
        joinOnNodes.setPos(join.getInputs().size());
        joinFilterNodes.clear();
        joinFilterNodes.setPos(join.getInputs().size());
        deferredJoinInputs.clear();
        forwardJoinReferences.clear();
        if (!isForwardLeftKeyFiltered) {
            forwardLeftJoinInputs.clear();
        }
        nonEquiNullingJoinInputs.clear();
        nullingJoinBoundaries.clear();
        boolean hasBarriers = false;
        for (int i = 1, n = join.getInputs().size(); i < n; i++) {
            final JoinKind type = join.getInputs().getQuick(i).getJoinType();
            hasBarriers |= JoinOrderSolver.isBarrier(type);
            if ((type == JoinKind.RIGHT_OUTER || type == JoinKind.FULL_OUTER) && i < source.getJoinModels().size()) {
                final ExpressionNode criteria = source.getJoinModels().getQuick(i).getJoinCriteria();
                if (criteria != null && !hasOwnJoinKey(criteria, join, i)) {
                    nonEquiNullingJoinInputs.add(i);
                }
            }
        }
        final boolean hasNonEquiNullingJoin = nonEquiNullingJoinInputs.size() > 0;
        try {
            ExpressionNode constantFilter = selectJoinConstantTerms(where, true);
            collectJoinConditions(selectJoinConstantTerms(where, false), join, -1, hasBarriers, hasNonEquiNullingJoin);
            final JoinInput master = join.getInputs().getQuick(0);
            for (int i = dependencyBase, n = lateralDependencyInputs.size(); i < n; i++) {
                joinOrder.addDependency(lateralDependencyParents.getQuick(i), lateralDependencyInputs.getQuick(i));
            }
            for (int i = 1, n = join.getInputs().size(); i < n; i++) {
                final QueryModel occurrence = source.getJoinModels().getQuick(i);
                final JoinInput slave = join.getInputs().getQuick(i);
                final boolean isBarrier = JoinOrderSolver.isBarrier(slave.getJoinType());
                ExpressionNode onCriteria = occurrence.getJoinCriteria();
                if (hasBarriers && !isBarrier && isPostJoinFilterReference(onCriteria, join, i)) {
                    collectJoinConditions(selectPostJoinFilterTerms(onCriteria, join, i, true), join, -1, true, hasNonEquiNullingJoin);
                    onCriteria = selectPostJoinFilterTerms(onCriteria, join, i, false);
                }
                final boolean hasForwardReference = findForwardJoinReference(onCriteria, join, i) != null;
                final boolean isDeferred = !isBarrier && hasForwardReference;
                final boolean isNonEquiNullingJoin = nonEquiNullingJoinInputs.contains(i);
                final boolean isForwardLeftJoin = hasForwardReference && slave.getJoinType() == JoinKind.LEFT_OUTER;
                if (isBarrier && hasForwardReference && !isForwardLeftJoin) {
                    throw forwardJoinReference(findForwardJoinReference(onCriteria, join, i));
                }
                if (isDeferred) {
                    deferredJoinInputs.add(i);
                }
                if (hasBarriers && !isNonEquiNullingJoin) {
                    collectJoinDependencies(onCriteria, join, i, isDeferred || isForwardLeftJoin);
                }
                if (isBarrier) {
                    if (slave.getJoinType() == JoinKind.SPLICE) {
                        validateSpliceOnAnalysis(onCriteria, join);
                    }
                    joinOnNodes.setQuick(i, collectJoinConditions(onCriteria, join, i, true, hasNonEquiNullingJoin));
                    if (isNonEquiNullingJoin) {
                        constrainNullingJoinPrefix(commaGroupStart(source, i), i);
                        joinOrder.addLateInput(i);
                        nullingJoinBoundaries.add(i);
                    } else {
                        final boolean isMasterNulling = slave.getJoinType().isMasterNulling();
                        int prefixStart = isMasterNulling ? commaGroupStart(source, i)
                                : slave.getJoinType() == JoinKind.LEFT_OUTER ? i - 1 : 0;
                        // A preceding LEFT join may wait for this one through a forward reference.
                        while (prefixStart > 0 && !isMasterNulling && forwardLeftJoinInputs.contains(prefixStart)) {
                            prefixStart--;
                        }
                        // A LEFT join whose ON reads no column stays unanchored, so it trails the order.
                        if (slave.getJoinType() != JoinKind.LEFT_OUTER || hasLiteral(onCriteria)) {
                            joinOrder.addOrderingConstraint(prefixStart, i);
                        }
                        if (isForwardLeftJoin) {
                            if (!forwardLeftJoinInputs.contains(i)) {
                                forwardLeftJoinInputs.add(i);
                            }
                            constrainNullingJoinPrefix(commaGroupStart(source, i), i);
                            joinOrder.addLateInput(i);
                        }
                        if (isMasterNulling) {
                            constrainNullingJoinPrefix(prefixStart, i);
                            for (int k = i + 1; k < n; k++) {
                                final JoinKind type = join.getInputs().getQuick(k).getJoinType();
                                if (type == JoinKind.INNER || type == JoinKind.CROSS || type.isMasterNulling()) {
                                    joinOrder.addOrderingConstraint(i, k);
                                }
                            }
                        }
                    }
                } else {
                    constantFilter = combineJoinPredicates(constantFilter, selectJoinConstantTerms(onCriteria, true));
                    collectJoinConditions(selectJoinConstantTerms(onCriteria, false), join, i, hasBarriers, hasNonEquiNullingJoin);
                }
                final ObjList<ExpressionNode> shorthand = occurrence.getJoinColumns();
                for (int k = 0, count = shorthand.size(); k < count; k++) {
                    final ExpressionNode column = shorthand.getQuick(k);
                    final int leftIndex = ctx.bindColumnIndex(column, master.getSourceOutput(), master.getBindingAlias());
                    final int rightIndex = ctx.bindColumnIndex(column, slave.getSourceOutput(), slave.getBindingAlias());
                    joinOrder.addEquality(0, master.getSourceOutput().getColumnId(leftIndex),
                            ctx.qualifiedJoinName(master.getBindingAlias(), column.token), column.position,
                            i, slave.getSourceOutput().getColumnId(rightIndex),
                            ctx.qualifiedJoinName(slave.getBindingAlias(), column.token), column.position, i);
                }
            }
            for (int i = 0, n = nullingJoinBoundaries.size(); i < n; i++) {
                final int boundary = nullingJoinBoundaries.getQuick(i);
                constrainNullingJoinConsumers(join, boundary, commaGroupStart(source, boundary));
            }
            if (!joinOrder.order()) {
                final boolean wasForwardInnerFiltered = isForwardInnerFiltered;
                final boolean wasForwardLeftKeyFiltered = isForwardLeftKeyFiltered;
                if (!isForwardInnerFiltered && deferredJoinInputs.size() > 0) {
                    isForwardInnerFiltered = true;
                } else if (!isForwardLeftKeyFiltered && forwardLeftJoinInputs.size() > 0) {
                    isForwardLeftKeyFiltered = true;
                }
                if (isForwardInnerFiltered != wasForwardInnerFiltered || isForwardLeftKeyFiltered != wasForwardLeftKeyFiltered) {
                    try {
                        bindJoinConditions(join, source, where, dependencyBase, executionContext);
                    } finally {
                        isForwardInnerFiltered = wasForwardInnerFiltered;
                        isForwardLeftKeyFiltered = wasForwardLeftKeyFiltered;
                    }
                    return;
                }
                if (forwardJoinReferences.size() == 0) {
                    throw new IllegalStateException("cyclic join dependencies without a forward ON reference");
                }
                throw forwardJoinReference(forwardJoinReferences.getQuick(0));
            }
            final ObjList<JoinInput> ordered = join.getOrderedInputs();
            int timestampId = ordered.getQuick(0).getSourceOutput().getTimestampColumnId();
            for (int i = 1, n = ordered.size(); i < n; i++) {
                final JoinInput step = ordered.getQuick(i);
                final ExpressionNode onFilter = joinOnNodes.getQuick(join.getInputs().indexOf(step));
                if ((step.getJoinType() == JoinKind.ASOF || step.getJoinType() == JoinKind.LT) && timestampId < 0) {
                    throw SqlException.$(step.getPosition(), "left side of time series join has no timestamp");
                }
                bindJoinOnResidual(onFilter, join, step, source, timestampId, i, executionContext);
                if (step.getJoinType().isMasterNulling() && !join.hasExplicitTimestamp()) {
                    timestampId = -1;
                }
            }
            join.getOutput().setTimestampIndex(join.getOutput().getColumnIndexById(timestampId));

            final ObjList<JoinOrderSolver.Equality> emitted = joinOrder.getSourceFilters();
            for (int i = 0, n = emitted.size(); i < n; i++) {
                final JoinOrderSolver.Equality equality = emitted.getQuick(i);
                final ExpressionNode comparison = ctx.bindingExpressions.next().of(ExpressionNode.OPERATION, "=", 0, equality.leftPosition);
                comparison.paramCount = 2;
                comparison.lhs = ctx.bindingExpressions.next().of(ExpressionNode.LITERAL, equality.leftName, 0, equality.leftPosition);
                comparison.rhs = ctx.bindingExpressions.next().of(ExpressionNode.LITERAL, equality.rightName, 0, equality.rightPosition);
                int origin = -1;
                if (hasBarriers && equality.originalOwners.indexOf(-1, 0, equality.originalOwners.size()) < 0) {
                    for (int k = 0, count = equality.originalOwners.size(); k < count; k++) {
                        final int owner = equality.originalOwners.getQuick(k);
                        if (origin < 0 || ordered.indexOf(join.getInputs().getQuick(owner)) > ordered.indexOf(join.getInputs().getQuick(origin))) {
                            origin = owner;
                        }
                    }
                }
                joinResidualNodes.add(comparison);
                joinResidualOrigins.add(origin);
            }
            for (int i = 0, n = joinResidualNodes.size(); i < n; i++) {
                final ExpressionNode predicate = joinResidualNodes.getQuick(i);
                final int origin = joinResidualOrigins.getQuick(i);
                int target = joinPredicateLastInput(predicate, join);
                if (target < 0) {
                    target = ordered.size() - 1;
                } else if (hasBarriers) {
                    if (origin >= 0) {
                        final int originPosition = ordered.indexOf(join.getInputs().getQuick(origin));
                        if (!isTemporalAnchor(ordered, target, originPosition) || !binder.hasSinglePredicateSource(predicate, join, null)) {
                            target = Math.max(target, originPosition);
                        }
                    } else {
                        for (int k = target + 1, count = ordered.size(); k < count; k++) {
                            if (ordered.getQuick(k).getJoinType().isMasterNulling()) {
                                target = k;
                            }
                        }
                    }
                }
                joinFilterNodes.setQuick(target, combineJoinPredicates(joinFilterNodes.getQuick(target), predicate));
            }
            for (int i = 0, n = joinFilterNodes.size(); i < n; i++) {
                final ExpressionNode expression = joinFilterNodes.getQuick(i);
                if (expression != null) {
                    final JoinInput input = ordered.getQuick(i);
                    final BoundExpression predicate = bindJoinConjunct(expression, join, source, i, -2, false, executionContext);
                    if (i == 0) {
                        final FilterPlan filter = ctx.filters.next().of(input.getInput(), predicate, expression.position);
                        filter.deriveOutput();
                        input.setInput(filter);
                    } else {
                        input.setPostJoinFilter(predicate);
                    }
                }
            }
            if (constantFilter != null) {
                addJoinConstantFilter(ordered.getLast(), bindJoinConstantFilter(constantFilter, join, source, executionContext), constantFilter.position);
            }
            for (int i = 0, n = join.getInputs().size(); i < n; i++) {
                if (!LogicalPlans.canPushJoinFilter(join, i, ordered.size() - 1)) {
                    ctx.stopTimestampIntrinsics(join.getInputs().getQuick(i).getSourceOutput());
                }
            }
        } finally {
            joinOrder.clear();
            joinOnNodes.clear();
            joinResidualNodes.clear();
            joinResidualOrigins.clear();
            joinFilterNodes.clear();
            forwardJoinReferences.clear();
        }
    }

    private BoundExpression bindJoinConjunct(ExpressionNode expression, JoinPlan join, QueryModel source,
                                             int lastInput, int originalOnSource, boolean isAndArgument, SqlExecutionContext executionContext) throws SqlException {
        if (expression.paramCount == 2 && SqlKeywords.isAndKeyword(expression.token)) {
            final BoundExpression left = bindJoinConjunct(expression.lhs, join, source, lastInput, originalOnSource, true, executionContext);
            final BoundExpression right = bindJoinConjunct(expression.rhs, join, source, lastInput, originalOnSource, true, executionContext);
            return ctx.expressionRewriter.combineConjunction(left, right, expression.position);
        }
        final int sourceIndex = joinExpressionSource(expression, join);
        if (sourceIndex >= 0 && LogicalPlans.canPushJoinFilter(join, sourceIndex, lastInput)
                && binder.distributeSetFilter(expression, join.getInputs().getQuick(sourceIndex).getInput(), join.getOutput(),
                sourceAlias(source), executionContext)) {
            return ctx.constants.next().ofBoolean(true, expression.position);
        }
        ctx.joinNativeTimestampIds.clear();
        collectJoinTimestampScopes(expression, join, lastInput);
        // Native precision belongs to this conjunct, not to every use of a
        // column elsewhere in the predicate. A mixed-source OR stays intact.
        BoundExpression predicate;
        final int subqueryMark = ctx.level.getSubqueryCount();
        try {
            predicate = ctx.functionBinder.toBooleanSubquery(ctx.functionBinder.bindPredicate(expression,
                    join.getOutput(), sourceAlias(source), ctx.joinNativeTimestampIds,
                    isAndArgument ? ColumnType.BOOLEAN : ColumnType.UNDEFINED, executionContext));
            if (predicate.getDataType() != ColumnType.BOOLEAN && (predicate.getDataType() != ColumnType.NULL || !isAndArgument)) {
                final boolean isTableFilter = sourceIndex >= 0 && join.getInputs().getQuick(sourceIndex).getInput() instanceof ScanPlan
                        && LogicalPlans.canPushJoinFilter(join, sourceIndex, lastInput);
                predicate = ctx.nonBooleanConjunct(expression, predicate.getDataType(), predicate.getPosition(), isTableFilter,
                        Math.max(sourceIndex, -1));
            }
        } catch (SqlException e) {
            predicate = ctx.deferConjunct(e, expression, join.getOutput(), sourceAlias(source), Math.max(sourceIndex, -1), subqueryMark);
        }
        if (originalOnSource == -2) {
            final int index = joinResidualNodes.indexOf(expression);
            if (index < 0) {
                return predicate;
            }
            originalOnSource = joinResidualOrigins.getQuick(index);
        }
        join.getFilterConjuncts().add(predicate);
        join.getFilterConjunctOrigins().add(originalOnSource);
        return predicate;
    }

    private BoundExpression bindJoinConstantFilter(ExpressionNode expression, JoinPlan join, QueryModel source,
                                                   SqlExecutionContext executionContext) throws SqlException {
        // Constant WHERE conjuncts bind as their own group. A lone NULL
        // must not acquire BOOLEAN type from an unrelated source predicate.
        final BoundExpression predicate = ctx.functionBinder.toBooleanSubquery(
                ctx.functionBinder.bind(expression, join.getOutput(), sourceAlias(source), executionContext));
        if (predicate.getDataType() != ColumnType.BOOLEAN) {
            throw SqlException.$(expression.position, "boolean expression expected");
        }
        return predicate;
    }

    private void bindJoinOnResidual(ExpressionNode onFilter, JoinPlan join, JoinInput slave, QueryModel source,
                                    int masterTimestampId, int lastInput, SqlExecutionContext executionContext) throws SqlException {
        if (onFilter == null) {
            return;
        }
        final JoinKind joinType = slave.getJoinType();
        if (joinType == JoinKind.SPLICE) {
            onFilter = appendJoinConjuncts(null, onFilter);
            for (int i = 0, n = join.getInputs().size(); i < n; i++) {
                validateSpliceOnSourceColumns(onFilter, join.getInputs().getQuick(i));
            }
            slave.setUnsupportedOnExpression(Misc.getThreadLocalSink().put(onFilter).toString(), onFilter.position);
        } else if (joinType == JoinKind.ASOF || joinType == JoinKind.LT) {
            if (masterTimestampId < 0 || hasPrecedingMasterNullingJoin(join, slave)) {
                throw SqlException.$(slave.getPosition(), "left side of time series join has no timestamp");
            }
            if (slave.getSourceOutput().getTimestampIndex() < 0) {
                throw SqlException.$(slave.getPosition(), "right side of time series join has no timestamp");
            }
            onFilter = appendJoinConjuncts(null, onFilter);
            throw SqlException.$(onFilter.position, "unsupported ").put(joinType == JoinKind.ASOF ? "ASOF" : "LT")
                    .put(" join expression [expr='").put(onFilter).put("']");
        } else {
            if (JoinOrderSolver.isBarrier(joinType)) {
                validateOuterJoinColumns(onFilter, join.getOutput());
            }
            slave.setOnResidual(bindJoinConjunct(onFilter, join, source,
                    JoinOrderSolver.isBarrier(joinType) ? -1 : lastInput, join.getInputs().indexOf(slave), false, executionContext));
        }
    }

    private LogicalPlan bindJoinSources(QueryModel source, ExpressionNode where, int sourceCount,
                                        SqlExecutionContext executionContext) throws SqlException {
        final ObjList<QueryModel> sources = source.getJoinModels();
        boolean hasTemporalJoin = false;
        boolean hasUnnest = false;
        for (int i = 1; i < sourceCount; i++) {
            final QueryModel occurrence = sources.getQuick(i);
            final int type = occurrence.getJoinType();
            final boolean isTemporal = type == QueryModel.JOIN_ASOF || type == QueryModel.JOIN_LT || type == QueryModel.JOIN_SPLICE;
            if (type != QueryModel.JOIN_CROSS && type != QueryModel.JOIN_INNER
                    && type != QueryModel.JOIN_LEFT_OUTER && type != QueryModel.JOIN_RIGHT_OUTER
                    && type != QueryModel.JOIN_FULL_OUTER && type != QueryModel.JOIN_UNNEST && !isTemporal
                    && !QueryModel.isLateralJoin(type)) {
                throw new IllegalStateException("unexpected join type in join block");
            }
            hasTemporalJoin |= isTemporal;
            hasUnnest |= type == QueryModel.JOIN_UNNEST;
        }
        final int dependencyBase = lateralDependencyInputs.size();
        final JoinPlan join = ctx.joins.next().of(source.getModelPosition());
        join.setExplicitTimestamp(source.hasExplicitTimestamp());
        for (int i = 0; i < sourceCount; i++) {
            final QueryModel occurrence = sources.getQuick(i);
            final CharSequence alias = occurrence.getName() == null ? hintAlias(occurrence) : sourceAlias(occurrence);
            if (alias != null) {
                for (int k = 0; k < i; k++) {
                    if (Chars.equalsIgnoreCase(alias, join.getInputs().getQuick(k).getBindingAlias())) {
                        final ExpressionNode name = occurrence.getAlias() != null ? occurrence.getAlias() : occurrence.getTableNameExpr();
                        throw SqlException.$(name == null ? 0 : name.position, "Duplicate table or alias: ")
                                .put(name == null ? alias : name.token);
                    }
                }
            }
            final JoinInput step;
            if (occurrence.getJoinType() == QueryModel.JOIN_UNNEST) {
                final UnnestSpec spec = bindUnnest(occurrence, join.getOutput(), executionContext);
                step = ctx.joinInputs.next().ofUnnest(spec, alias, occurrence.getJoinKeywordPosition());
                if (spec.isStandalone()) {
                    join.getOutput().clear();
                }
            } else {
                final JoinKind stepType = i == 0 ? JoinKind.CROSS : joinKind(occurrence.getJoinType());
                final boolean isDependent = QueryModel.isLateralJoin(occurrence.getJoinType());
                final LogicalPlan input;
                if (isDependent) {
                    final int outerColumnBase = ctx.functionBinder.getOuterColumnIds().size();
                    input = lateralBinder.bindLateral(source, join, i, executionContext);
                    addLateralDependencies(join, i, outerColumnBase);
                } else {
                    input = binder.bindSource(occurrence, executionContext);
                }
                if (hasTemporalJoin) {
                    retainImplicitTimestamp(input);
                }
                step = ctx.joinInputs.next().of(input, stepType, alias, occurrence.getJoinKeywordPosition());
                step.setSubquery(occurrence.getNestedModel() != null);
                step.setDependent(isDependent);
            }
            if (occurrence.getAsOfJoinTolerance() != null) {
                final ExpressionNode tolerance = occurrence.getAsOfJoinTolerance();
                step.setTolerance(tolerance.token, tolerance.position);
            }
            step.setHints(resolveJoinHints(source, occurrence));
            join.getInputs().add(step);
            addJoinOutput(join, step.getSourceOutput(), alias);
        }
        try {
            return bindJoinBlock(source, where, join, dependencyBase, hasUnnest, executionContext);
        } finally {
            lateralDependencyInputs.setPos(dependencyBase);
            lateralDependencyParents.setPos(dependencyBase);
        }
    }

    private UnnestSpec bindUnnest(QueryModel model, OutputSchema prefix, SqlExecutionContext executionContext) throws SqlException {
        final UnnestSpec spec = ctx.unnestSpecs.next().of(model.isStandaloneUnnest(), model.isUnnestOrdinality());
        spec.getColumnAliases().addAll(model.getUnnestColumnAliases());
        final int outputCount = model.getUnnestOutputColumnCount();
        final int totalColumns = outputCount + (model.isUnnestOrdinality() ? 1 : 0);
        int aliasIndex = 0;
        ctx.aliases.clear();
        ctx.aliasSequences.clear();
        for (int i = 0, n = model.getUnnestExpressions().size(); i < n; i++) {
            final ExpressionNode expression = model.getUnnestExpressions().getQuick(i);
            final BoundExpression bound;
            try {
                bound = ctx.functionBinder.bind(expression, prefix, null, executionContext);
            } catch (SqlException e) {
                final ExpressionNode column = findUnnestColumn(expression, e.getPosition());
                final CharSequence message = e.getFlyweightMessage();
                if (column != null) {
                    final int dot = Chars.indexOfLastUnquoted(column.token, '.');
                    if (dot >= 0) {
                        if (prefix.hasColumnQualifier(GenericLexer.unquote(column.token.subSequence(0, dot)))) {
                            if (Chars.startsWith(message, "Invalid column: ")
                                    && Chars.equals(column.token, message, 16, message.length())) {
                                // The error names the source's unqualified column.
                                throw SqlException.invalidColumn(e.getPosition(), Chars.toString(column.token, dot + 1, column.token.length()));
                            }
                        } else if (Chars.equals(message, "Invalid table name or alias")) {
                            throw SqlException.invalidColumn(e.getPosition(), column.token);
                        }
                    }
                }
                throw e;
            }
            spec.getExpressions().add(bound);
            final int type = bound.getDataType();
            if (model.isUnnestJsonSource(i)) {
                if (ColumnType.tagOf(type) != ColumnType.VARCHAR) {
                    throw SqlException.$(expression.position, "VARCHAR expected for JSON UNNEST, got ").put(ColumnType.nameOf(type));
                }
                final ObjList<CharSequence> names = model.getUnnestJsonColumnNames().getQuick(i);
                final IntList types = model.getUnnestJsonColumnTypes().getQuick(i);
                spec.getJsonColumnNames().add(names);
                spec.getJsonColumnTypes().add(types);
                for (int k = 0, count = names.size(); k < count; k++) {
                    final CharSequence name = aliasIndex < spec.getColumnAliases().size()
                            ? spec.getColumnAliases().getQuick(aliasIndex) : names.getQuick(k);
                    spec.getOutput().add(ctx.nextColumnId++, ctx.createOutputName(name), types.getQuick(k), true);
                    aliasIndex++;
                }
            } else {
                if (!ColumnType.isArray(type)) {
                    throw SqlException.$(expression.position, "array type expected in UNNEST, got ").put(ColumnType.nameOf(type));
                }
                spec.getJsonColumnNames().add(null);
                spec.getJsonColumnTypes().add(null);
                final int dimensions = ColumnType.decodeArrayDimensionality(type);
                final int elementType = ColumnType.decodeArrayElementType(type);
                final int outputType = dimensions > 1
                        ? ColumnType.encodeArrayType((short) elementType, dimensions - 1) : elementType;
                final CharSequence name;
                if (aliasIndex < spec.getColumnAliases().size()) {
                    name = spec.getColumnAliases().getQuick(aliasIndex);
                } else if (outputCount == 1) {
                    name = "value";
                } else {
                    final CharacterStoreEntry defaultName = ctx.characterStore.newEntry();
                    defaultName.put("value").put(aliasIndex + 1);
                    name = defaultName.toImmutable();
                }
                spec.getOutput().add(ctx.nextColumnId++, ctx.createOutputName(name), outputType, true);
                aliasIndex++;
            }
        }
        if (spec.hasOrdinality()) {
            final CharSequence name = spec.getColumnAliases().size() == totalColumns
                    ? spec.getColumnAliases().getQuick(outputCount) : "ordinality";
            spec.getOutput().add(ctx.nextColumnId++, ctx.createOutputName(name), ColumnType.LONG, true);
        }
        return spec;
    }

    private ExpressionNode collectJoinConditions(
            ExpressionNode expression, JoinPlan join, int origin, boolean hasBarriers, boolean hasNonEquiNullingJoin
    ) throws SqlException {
        if (expression == null) {
            return null;
        }
        if (expression.paramCount == 2 && SqlKeywords.isAndKeyword(expression.token)) {
            final ExpressionNode left = collectJoinConditions(expression.lhs, join, origin, hasBarriers, hasNonEquiNullingJoin);
            final ExpressionNode right = collectJoinConditions(expression.rhs, join, origin, hasBarriers, hasNonEquiNullingJoin);
            return combineJoinPredicates(left, right);
        }
        if (expression.paramCount == 2 && Chars.equals(expression.token, '=')
                && expression.lhs.type == ExpressionNode.LITERAL && expression.rhs.type == ExpressionNode.LITERAL
                && !isOuterColumn(expression.lhs, join) && !isOuterColumn(expression.rhs, join)) {
            final OutputSchema output = join.getOutput();
            final int leftIndex = ctx.bindColumnIndex(expression.lhs, output, (CharSequence) null);
            final int rightIndex = ctx.bindColumnIndex(expression.rhs, output, (CharSequence) null);
            final int leftId = output.getColumnId(leftIndex);
            final int rightId = output.getColumnId(rightIndex);
            final int leftSource = joinColumnSource(join, leftId);
            final int rightSource = joinColumnSource(join, rightId);
            if ((!hasBarriers || canExtractJoinKey(join, leftSource, rightSource, origin, hasNonEquiNullingJoin))
                    && !isForwardLeftFilteredKey(leftSource, rightSource, origin)) {
                joinOrder.addEquality(leftSource, leftId, joinKeyName(expression.lhs, output, leftIndex), expression.lhs.position,
                        rightSource, rightId, joinKeyName(expression.rhs, output, rightIndex), expression.rhs.position, origin);
                return null;
            }
        }
        if (origin >= 0 && JoinOrderSolver.isBarrier(join.getInputs().getQuick(origin).getJoinType())) {
            return expression;
        }
        joinResidualNodes.add(expression);
        joinResidualOrigins.add(origin);
        return null;
    }

    private void collectJoinDependencies(ExpressionNode expression, JoinPlan join, int origin, boolean isDeferred) throws SqlException {
        if (expression == null) {
            return;
        }
        if (expression.type == ExpressionNode.LITERAL && !isOuterColumn(expression, join)) {
            final int id = join.getOutput().getColumnId(ctx.bindColumnIndex(expression, join.getOutput(), (CharSequence) null));
            final int source = joinColumnSource(join, id);
            if (source > origin) {
                if (!isDeferred) {
                    throw new IllegalStateException("forward ON reference outside a deferred join");
                }
                forwardJoinReferences.add(expression);
            }
            joinOrder.addOrderingConstraint(source, origin);
        }
        collectJoinDependencies(expression.lhs, join, origin, isDeferred);
        collectJoinDependencies(expression.rhs, join, origin, isDeferred);
        for (int i = 0, n = expression.args.size(); i < n; i++) {
            collectJoinDependencies(expression.args.getQuick(i), join, origin, isDeferred);
        }
    }

    private void collectJoinTimestampScopes(ExpressionNode expression, JoinPlan join, int lastInput) throws SqlException {
        final int sourceIndex = joinExpressionSource(expression, join);
        if (sourceIndex >= 0 && LogicalPlans.canPushJoinFilter(join, sourceIndex, lastInput)
                && binder.hasSinglePredicateSource(expression, join, null)) {
            ctx.copyTimestampScope(join.getInputs().getQuick(sourceIndex).getSourceOutput());
        }
    }

    private void constrainNullingJoinConsumers(JoinPlan join, int boundary, int prefixStart) {
        boolean hasConsumer = false;
        for (int i = boundary + 1, n = join.getInputs().size(); i < n; i++) {
            final boolean isConsumer = switch (join.getInputs().getQuick(i).getJoinType()) {
                case INNER -> true;
                case CROSS -> joinOrder.hasJoinDependency(i);
                case RIGHT_OUTER, FULL_OUTER -> !nonEquiNullingJoinInputs.contains(i);
                default -> false;
            };
            if (isConsumer) {
                if (!hasConsumer) {
                    hasConsumer = true;
                    constrainNullingJoinPrefix(prefixStart, boundary);
                }
                joinOrder.addOrderingConstraint(boundary, i);
            }
        }
    }

    private void constrainNullingJoinPrefix(int start, int boundary) {
        for (int i = start; i < boundary; i++) {
            if (!deferredJoinInputs.contains(i)) {
                joinOrder.addOrderingConstraint(i, boundary);
            }
        }
    }

    private ExpressionNode extractJoinKeys(ExpressionNode expression, JoinPlan join, JoinInput slave) throws SqlException {
        if (expression == null) {
            return null;
        }
        if (expression.paramCount == 2 && SqlKeywords.isAndKeyword(expression.token)) {
            final ExpressionNode left = extractJoinKeys(expression.lhs, join, slave);
            final ExpressionNode right = extractJoinKeys(expression.rhs, join, slave);
            if (left == null) {
                return right;
            }
            if (right == null) {
                return left;
            }
            if (left == expression.lhs && right == expression.rhs) {
                return expression;
            }
            final ExpressionNode residual = ctx.bindingExpressions.next().of(ExpressionNode.OPERATION, "and", expression.precedence, expression.position);
            residual.paramCount = 2;
            residual.lhs = left;
            residual.rhs = right;
            return residual;
        }
        if (expression.paramCount == 2 && Chars.equals(expression.token, '=')
                && expression.lhs.type == ExpressionNode.LITERAL && expression.rhs.type == ExpressionNode.LITERAL
                && !isOuterColumn(expression.lhs, join) && !isOuterColumn(expression.rhs, join)) {
            final OutputSchema output = join.getOutput();
            final int leftIndex = ctx.bindColumnIndex(expression.lhs, output, (CharSequence) null);
            final int rightIndex = ctx.bindColumnIndex(expression.rhs, output, (CharSequence) null);
            final int leftId = output.getColumnId(leftIndex);
            final int rightId = output.getColumnId(rightIndex);
            final OutputSchema slaveOutput = slave.getSourceOutput();
            final boolean isLeftSlave = slaveOutput.getColumnIndexById(leftId) >= 0;
            final boolean isRightSlave = slaveOutput.getColumnIndexById(rightId) >= 0;
            if (isLeftSlave != isRightSlave) {
                // Hash equality uses the existing join key type/coercion rules,
                // which differ from scalar comparison overload resolution.
                final CharSequence leftName = joinKeyName(expression.lhs, output, leftIndex);
                final CharSequence rightName = joinKeyName(expression.rhs, output, rightIndex);
                addJoinKey(slave, isLeftSlave ? rightId : leftId, isLeftSlave ? leftId : rightId,
                        isLeftSlave ? rightName : leftName,
                        isLeftSlave ? leftName : rightName,
                        isLeftSlave ? expression.lhs.position : expression.rhs.position);
                return null;
            }
        }
        return expression;
    }

    private ExpressionNode findForwardJoinReference(ExpressionNode expression, JoinPlan join, int origin) throws SqlException {
        if (expression == null) {
            return null;
        }
        if (expression.type == ExpressionNode.LITERAL) {
            return !isOuterColumn(expression, join)
                    && joinColumnSource(join, join.getOutput().getColumnId(ctx.bindColumnIndex(expression, join.getOutput(), (CharSequence) null))) > origin
                    ? expression : null;
        }
        ExpressionNode reference = findForwardJoinReference(expression.lhs, join, origin);
        if (reference == null) {
            reference = findForwardJoinReference(expression.rhs, join, origin);
        }
        for (int i = 0, n = expression.args.size(); reference == null && i < n; i++) {
            reference = findForwardJoinReference(expression.args.getQuick(i), join, origin);
        }
        return reference;
    }

    private boolean hasOwnJoinKey(ExpressionNode expression, JoinPlan join, int input) throws SqlException {
        if (expression.paramCount == 2 && SqlKeywords.isAndKeyword(expression.token)) {
            return hasOwnJoinKey(expression.lhs, join, input) || hasOwnJoinKey(expression.rhs, join, input);
        }
        if (expression.paramCount == 2 && Chars.equals(expression.token, '=')
                && expression.lhs.type == ExpressionNode.LITERAL && expression.rhs.type == ExpressionNode.LITERAL
                && !isOuterColumn(expression.lhs, join) && !isOuterColumn(expression.rhs, join)) {
            final OutputSchema output = join.getOutput();
            final int left = joinColumnSource(join, output.getColumnId(ctx.bindColumnIndex(expression.lhs, output, (CharSequence) null)));
            final int right = joinColumnSource(join, output.getColumnId(ctx.bindColumnIndex(expression.rhs, output, (CharSequence) null)));
            return left != right && Math.max(left, right) == input;
        }
        return false;
    }

    private CharSequence hintAlias(QueryModel model) {
        final CharSequence name = model.getName();
        return name != null ? name : hintAliases.getQuick(hintAliasModels.indexOf(model));
    }

    private int innerOnGroup(ExpressionNode expression, JoinPlan join) throws SqlException {
        final int source = joinExpressionSource(expression, join);
        return source < 0 ? 2 : source;
    }

    // An inner ON conjunct that waits for a later source filters the joined rows once the source is joined
    // after an outer or temporal join, or when deferring its inner join would make the join order cyclic.
    // A key from a later source to a LEFT join that waits for a later source can make the order cyclic; it filters instead.
    private boolean isForwardLeftFilteredKey(int leftSource, int rightSource, int origin) {
        if (!isForwardLeftKeyFiltered) {
            return false;
        }
        final int lower = Math.min(leftSource, rightSource);
        return lower != origin && forwardLeftJoinInputs.contains(lower) && Math.max(leftSource, rightSource) > lower;
    }

    private boolean isOuterColumn(ExpressionNode literal, JoinPlan join) {
        return ctx.functionBinder.isOuterColumn(literal, join.getOutput(), null);
    }

    private boolean isPostJoinFilterReference(ExpressionNode expression, JoinPlan join, int origin) throws SqlException {
        final int last = lastJoinReferenceSource(expression, join);
        if (isForwardInnerFiltered) {
            return last > origin;
        }
        for (int i = origin + 1; i <= last; i++) {
            if (JoinOrderSolver.isBarrier(join.getInputs().getQuick(i).getJoinType())) {
                return true;
            }
        }
        return false;
    }

    private int joinExpressionSource(ExpressionNode expression, JoinPlan join) throws SqlException {
        if (expression == null) {
            return -1;
        }
        if (expression.type == ExpressionNode.LITERAL) {
            if (isOuterColumn(expression, join)) {
                return -1;
            }
            final int index = ctx.bindColumnIndex(expression, join.getOutput(), (CharSequence) null);
            final int columnId = join.getOutput().getColumnId(index);
            return joinColumnSource(join, columnId);
        }
        int index = mergeJoinSources(joinExpressionSource(expression.lhs, join), joinExpressionSource(expression.rhs, join));
        for (int i = 0, n = expression.args.size(); i < n; i++) {
            index = mergeJoinSources(index, joinExpressionSource(expression.args.getQuick(i), join));
        }
        return index;
    }

    private CharSequence joinKeyName(ExpressionNode node, OutputSchema output, int index) {
        final CharSequence name = output.getColumnName(index);
        final int dot = Chars.indexOfLastUnquoted(node.token, '.');
        if (dot > 0 && output.hasColumnQualifiers() && output.getColumnQualifier(index) != null
                && !output.hasColumnQualifier(GenericLexer.unquote(node.token.subSequence(0, dot)))) {
            return ctx.qualifiedJoinName(output.getColumnQualifier(index), name);
        }
        return output.isVisible(index) || Chars.equalsIgnoreCase(name, node.token, dot + 1, node.token.length()) ? node.token : name;
    }

    private int joinPredicateLastInput(ExpressionNode expression, JoinPlan join) throws SqlException {
        if (expression == null) {
            return -1;
        }
        if (expression.type == ExpressionNode.LITERAL) {
            if (isOuterColumn(expression, join)) {
                return -1;
            }
            final int columnId = join.getOutput().getColumnId(ctx.bindColumnIndex(expression, join.getOutput(), (CharSequence) null));
            return join.getOrderedInputs().indexOf(join.getInputs().getQuick(joinColumnSource(join, columnId)));
        }
        int last = Math.max(joinPredicateLastInput(expression.lhs, join), joinPredicateLastInput(expression.rhs, join));
        for (int i = 0, n = expression.args.size(); i < n; i++) {
            last = Math.max(last, joinPredicateLastInput(expression.args.getQuick(i), join));
        }
        return last;
    }

    private int lastJoinReferenceSource(ExpressionNode expression, JoinPlan join) throws SqlException {
        if (expression == null) {
            return -1;
        }
        if (expression.type == ExpressionNode.LITERAL) {
            return isOuterColumn(expression, join) ? -1
                    : joinColumnSource(join, join.getOutput().getColumnId(ctx.bindColumnIndex(expression, join.getOutput(), (CharSequence) null)));
        }
        int last = Math.max(lastJoinReferenceSource(expression.lhs, join), lastJoinReferenceSource(expression.rhs, join));
        for (int i = 0, n = expression.args.size(); i < n; i++) {
            last = Math.max(last, lastJoinReferenceSource(expression.args.getQuick(i), join));
        }
        return last;
    }

    private int resolveJoinHints(QueryModel master, QueryModel slave) {
        final CharSequence masterAlias = hintAlias(master);
        final CharSequence slaveAlias = hintAlias(slave);
        int hints = 0;
        if (SqlHints.hasHintWithParams(ctx.currentHints, SqlHints.ASOF_LINEAR_HINT, masterAlias, slaveAlias)) {
            hints |= JoinInput.HINT_ASOF_LINEAR;
        }
        if (SqlHints.hasHintWithParams(ctx.currentHints, SqlHints.ASOF_DENSE_HINT, masterAlias, slaveAlias)) {
            hints |= JoinInput.HINT_ASOF_DENSE;
        }
        if (SqlHints.hasHintWithParams(ctx.currentHints, SqlHints.ASOF_INDEX_HINT, masterAlias, slaveAlias)) {
            hints |= JoinInput.HINT_ASOF_INDEX;
        }
        if (SqlHints.hasHintWithParams(ctx.currentHints, SqlHints.ASOF_MEMOIZED_HINT, masterAlias, slaveAlias)) {
            hints |= JoinInput.HINT_ASOF_MEMOIZED;
        }
        if (SqlHints.hasHintWithParams(ctx.currentHints, SqlHints.ASOF_MEMOIZED_DRIVEBY_HINT, masterAlias, slaveAlias)) {
            hints |= JoinInput.HINT_ASOF_MEMOIZED_DRIVEBY;
        }
        if (SqlHints.hasHintWithParams(ctx.currentHints, SqlHints.MARKOUT_HORIZON_HINT, masterAlias, slaveAlias)) {
            hints |= JoinInput.HINT_MARKOUT_HORIZON;
        }
        return hints;
    }

    private void retainImplicitTimestamp(LogicalPlan plan) {
        if (plan.getOutput().getTimestampIndex() >= 0) {
            return;
        }
        if (plan instanceof ProjectPlan project) {
            for (int i = 0, n = project.getExpressions().size(); i < n; i++) {
                if (!(project.getExpressions().getQuick(i) instanceof ColumnExpression)) {
                    return;
                }
            }
            retainImplicitTimestamp(project.getInput());
            final OutputSchema input = project.getInput().getOutput();
            final int timestampIndex = input.getTimestampIndex();
            if (timestampIndex >= 0) {
                final int columnId = ctx.nextColumnId++;
                project.getExpressions().add(ctx.columns.next().of(input.getColumnId(timestampIndex),
                        input.getColumnType(timestampIndex), project.getPosition()));
                // An implicit timestamp belongs to the record layout, never the
                // enclosing query's SQL name scope or wildcard expansion.
                project.getOutput().add(columnId, "", input.getColumnType(timestampIndex), input.getMetadata(timestampIndex), false);
                project.getOutput().setTimestampIndex(project.getOutput().getColumnCount() - 1);
                ctx.inheritTimestampBinding(input.getColumnId(timestampIndex), columnId);
            }
        } else if (plan instanceof FilterPlan || plan instanceof LimitPlan) {
            final LogicalPlan input = plan.inputAt(0);
            retainImplicitTimestamp(input);
            plan.getOutput().copyFrom(input.getOutput());
        }
        // Other operators establish their own ordering or equality contract.
        // In particular, never enlarge a DISTINCT or set-operation tuple.
    }

    private ExpressionNode selectJoinConstantTerms(ExpressionNode expression, boolean isConstantTerms) {
        if (expression == null) {
            return null;
        }
        if (expression.paramCount == 2 && SqlKeywords.isAndKeyword(expression.token)) {
            final ExpressionNode left = selectJoinConstantTerms(expression.lhs, isConstantTerms);
            final ExpressionNode right = selectJoinConstantTerms(expression.rhs, isConstantTerms);
            return !isConstantTerms && left == expression.lhs && right == expression.rhs
                    ? expression : combineJoinPredicates(left, right);
        }
        // Match the existing join pass's syntactic constant classification, not
        // Function.isConstant(): function calls and parameters remain runtime
        // predicates even when their selected implementation folds at binding.
        final boolean isConstant = !hasColumnReference(expression) && isCompileTimeJoinConstant(expression);
        return isConstant == isConstantTerms ? expression : null;
    }

    private ExpressionNode selectPostJoinFilterTerms(ExpressionNode expression, JoinPlan join, int origin, boolean isSelected) throws SqlException {
        if (expression == null) {
            return null;
        }
        if (expression.paramCount == 2 && SqlKeywords.isAndKeyword(expression.token)) {
            return combineJoinPredicates(selectPostJoinFilterTerms(expression.lhs, join, origin, isSelected),
                    selectPostJoinFilterTerms(expression.rhs, join, origin, isSelected));
        }
        return isPostJoinFilterReference(expression, join, origin) == isSelected ? expression : null;
    }

    private void validateSpliceOnAnalysedColumns(ExpressionNode expression, JoinPlan join) throws SqlException {
        if (expression == null) {
            return;
        }
        if (expression.type == ExpressionNode.LITERAL) {
            ctx.bindColumnIndex(expression, join.getOutput(), (CharSequence) null);
        } else if (expression.paramCount < 3) {
            validateSpliceOnAnalysedColumns(expression.rhs, join);
            validateSpliceOnAnalysedColumns(expression.lhs, join);
        } else {
            for (int i = 0, n = expression.args.size(); i < n; i++) {
                validateSpliceOnAnalysedColumns(expression.args.getQuick(i), join);
            }
        }
    }

    private void validateSpliceOnAnalysis(ExpressionNode expression, JoinPlan join) throws SqlException {
        if (expression == null) {
            return;
        }
        if (expression.paramCount == 2 && SqlKeywords.isAndKeyword(expression.token)) {
            validateSpliceOnAnalysis(expression.lhs, join);
            validateSpliceOnAnalysis(expression.rhs, join);
        } else if (expression.token != null && (Chars.equals(expression.token, '=') || Chars.equals(expression.token, '~'))) {
            validateSpliceOnAnalysedColumns(expression.lhs, join);
            validateSpliceOnAnalysedColumns(expression.rhs, join);
        }
    }

    private void validateSpliceOnSourceColumn(ExpressionNode expression, JoinInput source) throws SqlException {
        if (expression != null && expression.type == ExpressionNode.LITERAL) {
            final int dot = Chars.indexOfLastUnquoted(expression.token, '.');
            if (dot >= 0 && Chars.equalsIgnoreCaseNc(GenericLexer.unquote(expression.token.subSequence(0, dot)), source.getBindingAlias())) {
                final CharSequence name = expression.token.subSequence(dot + 1, expression.token.length());
                if (getColumnIndexQuiet(source.getSourceOutput(), name) < 0) {
                    throw SqlException.invalidColumn(expression.position, name);
                }
            }
        }
    }

    private void validateSpliceOnSourceColumns(ExpressionNode expression, JoinInput source) throws SqlException {
        if (expression == null) {
            return;
        }
        validateSpliceOnSourceColumn(expression, source);
        // Non-equality ON references resolve against top-down source columns:
        // known qualifiers lose their prefix; bare and unknown names stay in ON.
        if (expression.paramCount < 3) {
            validateSpliceOnSourceColumn(expression.rhs, source);
            validateSpliceOnSourceColumn(expression.lhs, source);
            validateSpliceOnSourceColumns(expression.lhs, source);
            validateSpliceOnSourceColumns(expression.rhs, source);
        } else {
            for (int i = 1, n = expression.args.size(); i < n; i++) {
                validateSpliceOnSourceColumn(expression.args.getQuick(i), source);
            }
            validateSpliceOnSourceColumn(expression.args.getQuick(0), source);
            validateSpliceOnSourceColumns(expression.args.getQuick(0), source);
            for (int i = expression.args.size() - 1; i > 0; i--) {
                validateSpliceOnSourceColumns(expression.args.getQuick(i), source);
            }
        }
    }

    static void addJoinKey(JoinInput slave, int masterId, int slaveId, CharSequence masterName, CharSequence slaveName, int position) {
        for (int i = 0, n = slave.getMasterKeyColumnIds().size(); i < n; i++) {
            if (slave.getMasterKeyColumnIds().getQuick(i) == masterId && slave.getSlaveKeyColumnIds().getQuick(i) == slaveId) {
                return;
            }
        }
        slave.getMasterKeyColumnIds().add(masterId);
        slave.getSlaveKeyColumnIds().add(slaveId);
        slave.getMasterKeyNames().add(masterName);
        slave.getSlaveKeyNames().add(slaveName);
        slave.getKeyPositions().add(position);
    }

    static boolean hasBarrierInput(JoinPlan join) {
        boolean hasBarrier = false;
        for (int i = 1, n = join.getInputs().size(); i < n; i++) {
            final JoinKind type = join.getInputs().getQuick(i).getJoinType();
            if (type == JoinKind.RIGHT_OUTER || type == JoinKind.FULL_OUTER) {
                return false;
            }
            hasBarrier |= JoinOrderSolver.isBarrier(type);
        }
        return hasBarrier;
    }

    void addJoinOutput(JoinPlan join, OutputSchema output, CharSequence alias) {
        final OutputSchema target = join.getOutput();
        for (int i = 0, n = output.getColumnCount(); i < n; i++) {
            target.add(output.getColumnId(i), output.getColumnName(i), output.getColumnType(i), output.getMetadata(i), output.isVisible(i), alias);
            target.setSymbolTableStatic(target.getColumnCount() - 1, output.isSymbolTableStatic(i));
        }
    }

    LogicalPlan bindJoins(QueryModel model, QueryModel source, ExpressionNode where, SqlExecutionContext executionContext) throws SqlException {
        return bindJoins(model, source, where, source.getJoinModels().size(), executionContext);
    }

    LogicalPlan bindJoins(QueryModel model, QueryModel source, ExpressionNode where, int sourceCount,
                          SqlExecutionContext executionContext) throws SqlException {
        final boolean previous = ctx.isInsideJoin;
        ctx.isInsideJoin = true;
        try {
            return bindJoinSources(source, where, sourceCount, executionContext);
        } finally {
            ctx.isInsideJoin = previous;
        }
    }

    // Hints can name aliases assigned to unnamed subqueries before join rewriting.
    void collectHintAliases(QueryModel model) {
        if (model == null) {
            return;
        }
        if (model.getName() == null && hintAliasModels.indexOf(model) < 0) {
            hintAliasModels.add(model);
            final CharacterStoreEntry alias = ctx.characterStore.newEntry();
            alias.put(QueryModel.SUB_QUERY_ALIAS_PREFIX).put(hintAliases.size());
            hintAliases.add(alias.toImmutable());
        }
        final ObjList<QueryModel> sources = model.getJoinModels();
        for (int i = 1, n = sources.size(); i < n; i++) {
            collectHintAliases(sources.getQuick(i));
        }
        collectHintAliases(model.getNestedModel());
        collectHintAliases(model.getUnionModel());
    }

    ExpressionNode combineJoinPredicates(ExpressionNode left, ExpressionNode right) {
        if (left == null) {
            return right;
        }
        if (right == null) {
            return left;
        }
        final ExpressionNode result = ctx.bindingExpressions.next().of(ExpressionNode.OPERATION, "and", 0, right.position);
        result.paramCount = 2;
        result.lhs = left;
        result.rhs = right;
        return result;
    }

}
