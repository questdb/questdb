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

package io.questdb.griffin.bind;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.TimestampDriver;
import io.questdb.griffin.CharacterStoreEntry;
import io.questdb.griffin.JoinOrderSolver;
import io.questdb.griffin.LogicalPlans;
import io.questdb.griffin.OperatorExpression;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlHints;
import io.questdb.griffin.SqlKeywords;
import io.questdb.griffin.engine.groupby.TimestampSamplerFactory;
import io.questdb.griffin.model.ExpressionNode;
import io.questdb.griffin.model.QueryModel;
import io.questdb.griffin.plan.logical.BoundExpression;
import io.questdb.griffin.plan.logical.JoinDependency;
import io.questdb.griffin.plan.logical.JoinEquality;
import io.questdb.griffin.plan.logical.JoinGraph;
import io.questdb.griffin.plan.logical.JoinInput;
import io.questdb.griffin.plan.logical.JoinKind;
import io.questdb.griffin.plan.logical.JoinPlan;
import io.questdb.griffin.plan.logical.LogicalPlan;
import io.questdb.griffin.plan.logical.OutputSchema;
import io.questdb.griffin.plan.logical.ScanPlan;
import io.questdb.griffin.plan.logical.UnnestSpec;
import io.questdb.std.Chars;
import io.questdb.std.GenericLexer;
import io.questdb.std.IntHashSet;
import io.questdb.std.IntList;
import io.questdb.std.Mutable;
import io.questdb.std.ObjList;

import static io.questdb.griffin.bind.BindContext.*;

final class JoinBinder implements Mutable {
    private final SqlBinder binder;
    private final CairoConfiguration configuration;
    private final BindContext ctx;
    private final IntList deferredJoinInputs = new IntList();
    private final ObjList<JoinEquality> derivedEqualities = new ObjList<>();
    private final ObjList<ExpressionNode> forwardJoinReferences = new ObjList<>();
    private final IntList forwardLeftJoinInputs = new IntList();
    private final IntList inputModels = new IntList();
    private final ObjList<ExpressionNode> joinOnNodes = new ObjList<>();
    private final JoinOrderSolver joinOrder;
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
            JoinOrderSolver joinOrder
    ) {
        this.ctx = ctx;
        this.binder = binder;
        this.configuration = configuration;
        this.lateralBinder = lateralBinder;
        this.joinOrder = joinOrder;
    }

    @Override
    public void clear() {
        deferredJoinInputs.clear();
        derivedEqualities.clear();
        forwardJoinReferences.clear();
        forwardLeftJoinInputs.clear();
        inputModels.clear();
        joinOnNodes.clear();
        joinOrder.clear();
        lateralDependencyInputs.clear();
        lateralDependencyParents.clear();
        nonEquiNullingJoinInputs.clear();
        nullingJoinBoundaries.clear();
        isForwardInnerFiltered = false;
        isForwardLeftKeyFiltered = false;
    }

    private static void bindTolerance(JoinPlan join, JoinInput step, QueryModel occurrence, int masterTimestampId) throws SqlException {
        final ExpressionNode tolerance = occurrence.getAsOfJoinTolerance();
        if (tolerance == null || step.getJoinType() != JoinKind.ASOF && step.getJoinType() != JoinKind.LT) {
            return;
        }
        final OutputSchema output = join.getOutput();
        final OutputSchema slaveOutput = step.getSourceOutput();
        step.setToleranceInterval(tolerance(tolerance.token, tolerance.position,
                output.getColumnType(output.getColumnIndexById(masterTimestampId)), slaveOutput.getColumnType(slaveOutput.getTimestampIndex())));
    }

    private static boolean canExtractJoinKey(JoinPlan join, int left, int right, int origin, boolean hasNonEquiNullingJoin) {
        if (left == right || origin < 0 && hasNonEquiNullingJoin) {
            return false;
        }
        final int higher = Math.max(left, right);
        if (join.getInputs().getQuick(higher).getUnnest() != null) {
            return false;
        }
        if (origin >= 0 && join.getInputs().getQuick(origin).getJoinType().isBarrier()) {
            return higher == origin;
        }
        if (join.getInputs().getQuick(higher).getJoinType().isBarrier()) {
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
     * Whether a filter of the source's columns alone can evaluate on the source before the join step at
     * {@code lastInput}: the source is not the outer or temporal side of its join, and no RIGHT or FULL join up to
     * that step joins after it.
     */
    private static boolean canPushJoinFilter(JoinPlan join, int source, int lastInput) {
        final ObjList<JoinInput> inputs = join.getInputs();
        if (inputs.getQuick(source).getInput() == null || source > lastInput) {
            return false;
        }
        if (source > 0) {
            switch (inputs.getQuick(source).getJoinType()) {
                case LEFT_OUTER, RIGHT_OUTER, FULL_OUTER, ASOF, LT, SPLICE -> {
                    return false;
                }
                default -> {
                }
            }
        }
        for (int i = source + 1; i <= lastInput; i++) {
            if (inputs.getQuick(i).getJoinType().isMasterNulling()) {
                return false;
            }
        }
        return true;
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

    private static boolean hasTemporalJoin(ObjList<QueryModel> sources, int lo, int hi) {
        boolean hasTemporalJoin = false;
        for (int i = lo + 1; i < hi; i++) {
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
        }
        return hasTemporalJoin;
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

    /**
     * Whether the input joins after every other input: a late input that no other input depends on, through a key,
     * an ordering constraint or a lateral dependency.
     */
    private static boolean isJoinedLast(JoinGraph graph, int input) {
        if (!graph.getLateInputs().contains(input)) {
            return false;
        }
        final ObjList<JoinDependency> dependencies = graph.getDependencies();
        for (int i = 0, n = dependencies.size(); i < n; i++) {
            final JoinDependency dependency = dependencies.getQuick(i);
            if (i != input && dependency != null && dependency.getParents().contains(input)) {
                return false;
            }
        }
        return !isParent(graph.getOrderingConstraints(), input) && !isParent(graph.getLateralDependencies(), input);
    }

    private static boolean isParent(IntList edges, int input) {
        for (int i = 0, n = edges.size(); i < n; i += 2) {
            if (edges.getQuick(i) == input) {
                return true;
            }
        }
        return false;
    }

    private static boolean isTimestampKey(JoinInput step, int masterId, int slaveId, int masterTimestampId) {
        return masterId == masterTimestampId || slaveId == step.getSourceOutput().getTimestampColumnId();
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

    /**
     * The end of the comma group that starts at {@code start} when the group has a RIGHT, FULL or SPLICE join, which
     * null-extends only the inputs of its own group unless its ON clauses read an input before the group;
     * {@code start} otherwise.
     */
    private static int nullingCommaGroupEnd(ObjList<QueryModel> sources, int start, int sourceCount) {
        boolean hasMasterNullingJoin = false;
        int end = start + 1;
        for (; end < sourceCount && !sources.getQuick(end).isCommaJoin(); end++) {
            final int type = sources.getQuick(end).getJoinType();
            hasMasterNullingJoin |= type == QueryModel.JOIN_RIGHT_OUTER || type == QueryModel.JOIN_FULL_OUTER || type == QueryModel.JOIN_SPLICE;
        }
        return hasMasterNullingJoin ? end : start;
    }

    private static int statedOrigin(IntList owners) {
        int origin = -1;
        for (int i = 0, n = owners.size(); i < n; i++) {
            final int owner = owners.getQuick(i);
            if (owner < 0) {
                return -1;
            }
            origin = Math.max(origin, owner);
        }
        return origin;
    }

    private static long tolerance(CharSequence token, int position, int leftTimestampType, int rightTimestampType) throws SqlException {
        final int k = TimestampSamplerFactory.findPositiveIntervalEndIndex(token, position, "tolerance");
        assert token.length() > k;
        final char unit = token.charAt(k);
        final TimestampDriver timestampDriver = ColumnType.getTimestampDriver(ColumnType.getHigherPrecisionTimestampType(leftTimestampType, rightTimestampType));
        if (unit == 'n') {
            return timestampDriver.fromNanos(TimestampSamplerFactory.parsePositiveInterval(token, k, position, "tolerance", Integer.MAX_VALUE, unit));
        }
        final long multiplier = switch (unit) {
            case 'U' -> timestampDriver.fromMicros(1);
            case 'T' -> timestampDriver.fromMillis(1);
            case 's' -> timestampDriver.fromSeconds(1);
            case 'm' -> timestampDriver.fromMinutes(1);
            case 'h' -> timestampDriver.fromHours(1);
            case 'd' -> timestampDriver.fromDays(1);
            case 'w' -> timestampDriver.fromWeeks(1);
            default -> throw SqlException.$(position, "unsupported TOLERANCE unit [unit=").put(unit).put(']');
        };
        final int maxValue = (int) Math.min(Long.MAX_VALUE / multiplier, Integer.MAX_VALUE);
        return TimestampSamplerFactory.parsePositiveInterval(token, k, position, "tolerance", maxValue, unit) * multiplier;
    }

    private static void validateJoinKey(JoinPlan join, JoinInput slave, int masterId, int slaveId, int position) throws SqlException {
        final OutputSchema output = join.getOutput();
        final OutputSchema slaveOutput = slave.getSourceOutput();
        if (!LogicalPlans.isJoinKeyTypeCompatible(output.getColumnType(output.getColumnIndexById(masterId)),
                slaveOutput.getColumnType(slaveOutput.getColumnIndexById(slaveId)))) {
            throw SqlException.$(position, "join column type mismatch");
        }
    }

    /**
     * Validates the types of the keys an input joins by.
     */
    private static void validateJoinKeys(JoinPlan join, JoinDependency dependency) throws SqlException {
        if (dependency == null) {
            return;
        }
        final int source = dependency.getSlave();
        final JoinInput slave = join.getInputs().getQuick(source);
        final ObjList<JoinEquality> keys = dependency.getKeys();
        for (int i = 0, n = keys.size(); i < n; i++) {
            final JoinEquality key = keys.getQuick(i);
            final boolean isLeftSlave = key.getLeftSource() == source;
            validateJoinKey(join, slave, isLeftSlave ? key.getRightColumnId() : key.getLeftColumnId(),
                    isLeftSlave ? key.getLeftColumnId() : key.getRightColumnId(),
                    isLeftSlave ? key.getLeftPosition() : key.getRightPosition());
        }
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

    /**
     * Rejects an ASOF or LT step with several keys that keys the designated timestamp of its master
     * ({@code masterTimestampId}) or its own.
     */
    private static void validateTemporalKeys(JoinInput step, JoinDependency dependency, int masterTimestampId) throws SqlException {
        if (step.getJoinType() != JoinKind.ASOF && step.getJoinType() != JoinKind.LT || dependency == null || dependency.getKeys().size() < 2) {
            return;
        }
        final int source = dependency.getSlave();
        final ObjList<JoinEquality> keys = dependency.getKeys();
        for (int i = 0, n = keys.size(); i < n; i++) {
            final JoinEquality key = keys.getQuick(i);
            final boolean isLeftSlave = key.getLeftSource() == source;
            if (isTimestampKey(step, isLeftSlave ? key.getRightColumnId() : key.getLeftColumnId(),
                    isLeftSlave ? key.getLeftColumnId() : key.getRightColumnId(), masterTimestampId)) {
                throw SqlException.$(step.getPosition(), "ASOF/LT JOIN cannot use designated timestamp as a join key");
            }
        }
    }

    private void addLateralDependencies(JoinPlan join, int index, int outerColumnBase) {
        final IntList outerColumnIds = ctx.scope().outerColumnIds;
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

    /**
     * Binds the inputs of the later comma group from {@code lo} to {@code hi}, which has a RIGHT, FULL or SPLICE
     * join, then nests the group as one CROSS input of the join, since a comma binds looser than JOIN, unless an ON
     * clause of the group reads a column of an input before the group: the group's inputs then join the join
     * directly, so that the comma binds like CROSS JOIN.
     */
    private void bindCommaGroup(JoinPlan join, QueryModel source, int lo, int hi, boolean hasTemporalJoin,
                                SqlExecutionContext executionContext) throws SqlException {
        final ObjList<QueryModel> sources = source.getJoinModels();
        final int dependencyBase = lateralDependencyInputs.size();
        final int modelBase = inputModels.size();
        final JoinPlan group = ctx.planNodes.joins.next().of(sources.getQuick(lo).getModelPosition());
        group.setExplicitTimestamp(false);
        final boolean hasGroupTemporalJoin = hasTemporalJoin(sources, lo, hi);
        bindJoinInputs(group, source, lo, hi, hasGroupTemporalJoin, executionContext);
        final ObjList<JoinInput> inputs = group.getInputs();
        boolean hasPrefixReference = false;
        for (int i = 1, n = inputs.size(); i < n && !hasPrefixReference; i++) {
            hasPrefixReference = hasCommaPrefixReference(joinModel(source, modelBase, i).getJoinCriteria(), join, group);
        }
        if (hasPrefixReference) {
            final int offset = join.getInputs().size();
            for (int i = dependencyBase, n = lateralDependencyInputs.size(); i < n; i++) {
                lateralDependencyInputs.setQuick(i, lateralDependencyInputs.getQuick(i) + offset);
                lateralDependencyParents.setQuick(i, lateralDependencyParents.getQuick(i) + offset);
            }
            for (int i = 0, n = inputs.size(); i < n; i++) {
                final JoinInput step = inputs.getQuick(i);
                if (hasTemporalJoin && !hasGroupTemporalJoin && step.getInput() != null) {
                    ctx.retainImplicitTimestamp(step.getInput());
                }
                join.getInputs().add(step);
                addJoinOutput(join, step.getSourceOutput(), step.getBindingAlias());
            }
            return;
        }
        bindJoinConditions(group, source, null, dependencyBase, modelBase, executionContext);
        inputModels.setPos(modelBase);
        lateralDependencyInputs.setPos(dependencyBase);
        lateralDependencyParents.setPos(dependencyBase);
        inputModels.add(lo);
        join.getInputs().add(ctx.planNodes.joinInputs.next().of(group, JoinKind.CROSS, null, sources.getQuick(lo).getJoinKeywordPosition()));
        join.getOutput().addColumnsFrom(group.getOutput());
    }

    /**
     * Binds the join's conditions in source order and leaves its order to the optimiser:
     * collects the keys and constraints into the join's graph, checks that the join semantics admit an order, the
     * key types, the time series inputs and TOLERANCE, binds the outer ON residuals, and binds every other conjunct
     * into the graph for the optimiser to place.
     */
    private void bindJoinConditions(JoinPlan join, QueryModel source, ExpressionNode where, int dependencyBase, int modelBase,
                                    SqlExecutionContext executionContext) throws SqlException {
        final BindScope scope = ctx.scope();
        final ObjList<JoinInput> inputs = join.getInputs();
        boolean hasBarriers = false;
        for (int i = 1, n = inputs.size(); i < n; i++) {
            hasBarriers |= inputs.getQuick(i).getJoinType().isBarrier();
        }
        final JoinGraph graph = ctx.planNodes.joinGraphs.next();
        try {
            final ExpressionNode constantFilter = collectJoinGraph(join, graph, source, where, dependencyBase, modelBase, hasBarriers);
            derivedEqualities.addAll(joinOrder.getSourceFilters());
            joinOrder.clear();
            final int firstTimestampId = inputs.getQuick(0).getSourceOutput().getTimestampColumnId();
            int timestampId = firstTimestampId;
            boolean hasMasterNullingJoin = false;
            for (int i = 1, n = inputs.size(); i < n; i++) {
                final JoinInput step = inputs.getQuick(i);
                final JoinDependency dependency = graph.getDependencies().getQuick(i);
                if (step.getJoinType().isTemporal()) {
                    validateTimeSeriesTimestamps(step.getPosition(), timestampId, step.getSourceOutput());
                }
                validateJoinKeys(join, dependency);
                validateTemporalKeys(step, dependency, timestampId);
                bindJoinOnResidual(joinOnNodes.getQuick(i), join, step, source, i, executionContext);
                bindJoinResiduals(join, graph, source, i, executionContext);
                bindTolerance(join, step, joinModel(source, modelBase, i), timestampId);
                if (step.getJoinType().isMasterNulling() && !join.hasExplicitTimestamp()) {
                    hasMasterNullingJoin = true;
                    if (!isJoinedLast(graph, i)) {
                        timestampId = -1;
                    }
                }
            }
            join.getOutput().setTimestampIndex(hasMasterNullingJoin ? -1 : join.getOutput().getColumnIndexById(firstTimestampId));

            final int lastInput = inputs.size() - 1;
            bindJoinResiduals(join, graph, source, -1, executionContext);
            for (int i = 0, n = derivedEqualities.size(); i < n; i++) {
                final JoinEquality equality = derivedEqualities.getQuick(i);
                final ExpressionNode comparison = ctx.bindingExpressions.next().of(ExpressionNode.OPERATION, "=", 0, equality.getLeftPosition());
                comparison.paramCount = 2;
                comparison.lhs = ctx.bindingExpressions.next().of(ExpressionNode.LITERAL, equality.getLeftName(), 0, equality.getLeftPosition());
                comparison.rhs = ctx.bindingExpressions.next().of(ExpressionNode.LITERAL, equality.getRightName(), 0, equality.getRightPosition());
                final IntList owners = equality.getOwners();
                graph.addResidual(bindJoinConjunct(comparison, comparison, join, source, lastInput, statedOrigin(owners), executionContext), owners);
            }
            if (constantFilter != null) {
                graph.setConstantFilter(bindJoinConstantFilter(constantFilter, join, source, executionContext), constantFilter.position);
            }
            for (int i = 0, n = inputs.size(); i < n; i++) {
                if (!canPushJoinFilter(join, i, lastInput)) {
                    ctx.stopTimestampIntrinsics(inputs.getQuick(i).getSourceOutput());
                }
            }
            join.setGraph(graph);
        } finally {
            joinOrder.clear();
            derivedEqualities.clear();
            joinOnNodes.clear();
            scope.joinResidualNodes.clear();
            scope.joinResidualOrigins.clear();
            forwardJoinReferences.clear();
        }
    }

    /**
     * Binds a conjunct of the conjunction, a WHERE clause or an ON condition, which is the conjunct itself when it
     * stands alone.
     */
    private BoundExpression bindJoinConjunct(ExpressionNode expression, ExpressionNode conjunction, JoinPlan join, QueryModel source,
                                             int lastInput, int originalOnSource, SqlExecutionContext executionContext) throws SqlException {
        final BindScope scope = ctx.scope();
        if (expression.paramCount == 2 && SqlKeywords.isAndKeyword(expression.token)) {
            final BoundExpression left = bindJoinConjunct(expression.lhs, conjunction, join, source, lastInput, originalOnSource, executionContext);
            final BoundExpression right = bindJoinConjunct(expression.rhs, conjunction, join, source, lastInput, originalOnSource, executionContext);
            return ctx.expressionRewriter.combineConjunction(left, right, expression.position);
        }
        final boolean isAndArgument = expression != conjunction;
        final int sourceIndex = joinExpressionSource(expression, join);
        if (sourceIndex >= 0 && canPushJoinFilter(join, sourceIndex, lastInput)
                && binder.distributeSetFilter(expression, join.getInputs().getQuick(sourceIndex).getInput(), join.getOutput(),
                sourceAlias(source), executionContext)) {
            return ctx.planNodes.constants.next().ofBoolean(true, expression.position);
        }
        final IntHashSet nativeTimestampIds = scope.joinNativeTimestampIds;
        nativeTimestampIds.clear();
        collectJoinTimestampScopes(expression, join, lastInput);
        // Native precision belongs to this conjunct, not to every use of a
        // column elsewhere in the predicate. A mixed-source OR stays intact.
        final BoundExpression predicate = ctx.functionBinder.toBooleanSubquery(ctx.functionBinder.bindPredicate(expression,
                join.getOutput(), sourceAlias(source), nativeTimestampIds,
                isAndArgument ? ColumnType.BOOLEAN : ColumnType.UNDEFINED, executionContext));
        if (predicate.getDataType() != ColumnType.BOOLEAN && (predicate.getDataType() != ColumnType.NULL || !isAndArgument)) {
            final int filterInput = joinFilterInput(expression, join, lastInput);
            final boolean isTableFilter = filterInput >= 0 && join.getInputs().getQuick(filterInput).getInput() instanceof ScanPlan;
            throw BindContext.nonBooleanConjunct(expression, predicate.getDataType(), predicate.getPosition(), isTableFilter,
                    isAndArgument && countJoinFilterConjuncts(conjunction, filterInput, join, lastInput) > 1);
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

    private void bindJoinInputs(JoinPlan join, QueryModel source, int lo, int hi, boolean hasTemporalJoin,
                                SqlExecutionContext executionContext) throws SqlException {
        final ObjList<QueryModel> sources = source.getJoinModels();
        for (int i = lo; i < hi; i++) {
            final QueryModel occurrence = sources.getQuick(i);
            final CharSequence alias = inputAlias(occurrence);
            if (alias != null) {
                for (int k = 0; k < i; k++) {
                    if (Chars.equalsIgnoreCase(alias, inputAlias(sources.getQuick(k)))) {
                        final ExpressionNode name = occurrence.getAlias() != null ? occurrence.getAlias() : occurrence.getTableNameExpr();
                        throw SqlException.$(name == null ? 0 : name.position, "Duplicate table or alias: ")
                                .put(name == null ? alias : name.token);
                    }
                }
            }
            final int groupEnd = lo == 0 && i > 0 && occurrence.isCommaJoin() ? nullingCommaGroupEnd(sources, i, hi) : i;
            if (groupEnd > i) {
                bindCommaGroup(join, source, i, groupEnd, hasTemporalJoin, executionContext);
                i = groupEnd - 1;
                continue;
            }
            inputModels.add(i);
            final JoinInput step;
            if (occurrence.getJoinType() == QueryModel.JOIN_UNNEST) {
                final UnnestSpec spec = bindUnnest(occurrence, join.getOutput(), executionContext);
                step = ctx.planNodes.joinInputs.next().ofUnnest(spec, alias, occurrence.getJoinKeywordPosition());
                if (spec.isStandalone()) {
                    join.getOutput().clear();
                }
            } else {
                final int index = join.getInputs().size();
                final JoinKind stepType = index == 0 ? JoinKind.CROSS : joinKind(occurrence.getJoinType());
                final boolean isDependent = QueryModel.isLateralJoin(occurrence.getJoinType());
                final LogicalPlan input;
                if (isDependent) {
                    final int outerColumnBase = ctx.scope().outerColumnIds.size();
                    input = lateralBinder.bindLateral(occurrence, join, index, executionContext);
                    addLateralDependencies(join, index, outerColumnBase);
                } else {
                    input = binder.bindSource(occurrence, executionContext);
                }
                if (hasTemporalJoin) {
                    ctx.retainImplicitTimestamp(input);
                }
                step = ctx.planNodes.joinInputs.next().of(input, stepType, alias, occurrence.getJoinKeywordPosition());
                step.setSubquery(occurrence.getNestedModel() != null);
                step.setDependent(isDependent);
            }
            step.setHints(resolveJoinHints(source, occurrence));
            join.getInputs().add(step);
            addJoinOutput(join, step.getSourceOutput(), alias);
        }
    }

    private void bindJoinOnResidual(ExpressionNode onFilter, JoinPlan join, JoinInput slave, QueryModel source,
                                    int lastInput, SqlExecutionContext executionContext) throws SqlException {
        if (onFilter == null) {
            return;
        }
        final JoinKind joinType = slave.getJoinType();
        if (joinType == JoinKind.SPLICE) {
            rejectSpliceResidual(onFilter, join);
        } else if (joinType == JoinKind.ASOF || joinType == JoinKind.LT) {
            if (hasPrecedingMasterNullingJoin(join, slave)) {
                throw SqlException.$(slave.getPosition(), "left side of time series join has no timestamp");
            }
            onFilter = appendJoinConjuncts(null, onFilter);
            throw SqlException.$(onFilter.position, "unsupported ").put(joinType == JoinKind.ASOF ? "ASOF" : "LT")
                    .put(" join expression [expr='").put(onFilter).put("']");
        } else {
            if (joinType.isBarrier()) {
                validateOuterJoinColumns(onFilter, join.getOutput());
            }
            slave.setOnResidual(bindJoinConjunct(onFilter, onFilter, join, source,
                    joinType.isBarrier() ? -1 : lastInput, join.getInputs().indexOf(slave), executionContext));
        }
    }

    /**
     * Binds into the graph the residual conjuncts that the ON clause of input {@code origin}, or WHERE for -1, states:
     * each binds within the conjunction of the clause's residual conjuncts, which leaves out its join keys and constant
     * terms.
     */
    private void bindJoinResiduals(JoinPlan join, JoinGraph graph, QueryModel source, int origin,
                                   SqlExecutionContext executionContext) throws SqlException {
        final BindScope scope = ctx.scope();
        final ObjList<ExpressionNode> residuals = scope.joinResidualNodes;
        final IntList origins = scope.joinResidualOrigins;
        ExpressionNode conjunction = null;
        for (int i = 0, n = residuals.size(); i < n; i++) {
            if (origins.getQuick(i) == origin) {
                conjunction = combineJoinPredicates(conjunction, residuals.getQuick(i));
            }
        }
        final int lastInput = join.getInputs().size() - 1;
        for (int i = 0, n = residuals.size(); i < n; i++) {
            if (origins.getQuick(i) == origin) {
                final ExpressionNode residual = residuals.getQuick(i);
                final int scopeInput = origin < 0 ? lastInput : Math.max(origin, lastJoinReferenceSource(residual, join));
                graph.addResidual(bindJoinConjunct(residual, conjunction, join, source, scopeInput, origin, executionContext), origin);
            }
        }
    }

    private JoinPlan bindJoinSources(QueryModel source, ExpressionNode where, int lo, int hi,
                                     SqlExecutionContext executionContext) throws SqlException {
        final ObjList<QueryModel> sources = source.getJoinModels();
        final int dependencyBase = lateralDependencyInputs.size();
        final int modelBase = inputModels.size();
        final JoinPlan join = ctx.planNodes.joins.next().of(sources.getQuick(lo).getModelPosition());
        join.setExplicitTimestamp(lo == 0 && source.hasExplicitTimestamp());
        try {
            bindJoinInputs(join, source, lo, hi, hasTemporalJoin(sources, lo, hi), executionContext);
            bindJoinConditions(join, source, where, dependencyBase, modelBase, executionContext);
            return join;
        } finally {
            inputModels.setPos(modelBase);
            lateralDependencyInputs.setPos(dependencyBase);
            lateralDependencyParents.setPos(dependencyBase);
        }
    }

    private UnnestSpec bindUnnest(QueryModel model, OutputSchema prefix, SqlExecutionContext executionContext) throws SqlException {
        final BindScope scope = ctx.scope();
        final UnnestSpec spec = ctx.planNodes.unnestSpecs.next().of(model.isStandaloneUnnest(), model.isUnnestOrdinality());
        spec.getColumnAliases().addAll(model.getUnnestColumnAliases());
        final int outputCount = model.getUnnestOutputColumnCount();
        final int totalColumns = outputCount + (model.isUnnestOrdinality() ? 1 : 0);
        int aliasIndex = 0;
        scope.aliases.clear();
        scope.aliasSequences.clear();
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
                    spec.getOutput().add(ctx.planNodes.nextColumnId(), ctx.createOutputName(name), types.getQuick(k), true);
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
                spec.getOutput().add(ctx.planNodes.nextColumnId(), ctx.createOutputName(name), outputType, true);
                aliasIndex++;
            }
        }
        if (spec.hasOrdinality()) {
            final CharSequence name = spec.getColumnAliases().size() == totalColumns
                    ? spec.getColumnAliases().getQuick(outputCount) : "ordinality";
            spec.getOutput().add(ctx.planNodes.nextColumnId(), ctx.createOutputName(name), ColumnType.LONG, true);
        }
        return spec;
    }

    private ExpressionNode collectJoinConditions(
            ExpressionNode expression, JoinPlan join, int origin, boolean hasBarriers, boolean hasNonEquiNullingJoin
    ) throws SqlException {
        final BindScope scope = ctx.scope();
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
            final int leftSource = LogicalPlans.joinColumnSource(join, leftId);
            final int rightSource = LogicalPlans.joinColumnSource(join, rightId);
            if ((!hasBarriers || canExtractJoinKey(join, leftSource, rightSource, origin, hasNonEquiNullingJoin))
                    && !isForwardLeftFilteredKey(leftSource, rightSource, origin)) {
                joinOrder.addEquality(leftSource, leftId, joinKeyName(expression.lhs, output, leftIndex), expression.lhs.position,
                        rightSource, rightId, joinKeyName(expression.rhs, output, rightIndex), expression.rhs.position, origin);
                return null;
            }
        }
        if (origin >= 0 && join.getInputs().getQuick(origin).getJoinType().isBarrier()) {
            return expression;
        }
        scope.joinResidualNodes.add(expression);
        scope.joinResidualOrigins.add(origin);
        return null;
    }

    private void collectJoinDependencies(ExpressionNode expression, JoinPlan join, int origin, boolean isDeferred) {
        if (expression == null) {
            return;
        }
        final int source = expression.type == ExpressionNode.LITERAL ? joinReferenceSource(expression, join) : -1;
        if (source >= 0) {
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

    /**
     * Collects the join keys and ordering constraints of the join's ON conditions and WHERE clause into its graph,
     * filtering forward references when the constraints with them admit no order; returns the constant terms of the
     * WHERE clause and the inner ON conditions.
     */
    private ExpressionNode collectJoinGraph(JoinPlan join, JoinGraph graph, QueryModel source, ExpressionNode where, int dependencyBase,
                                            int modelBase, boolean hasBarriers) throws SqlException {
        final BindScope scope = ctx.scope();
        graph.clear();
        joinOrder.collect(join, graph);
        scope.joinResidualNodes.clear();
        scope.joinResidualOrigins.clear();
        joinOnNodes.clear();
        joinOnNodes.setPos(join.getInputs().size());
        deferredJoinInputs.clear();
        forwardJoinReferences.clear();
        if (!isForwardLeftKeyFiltered) {
            forwardLeftJoinInputs.clear();
        }
        nonEquiNullingJoinInputs.clear();
        nullingJoinBoundaries.clear();
        for (int i = 1, n = join.getInputs().size(); i < n; i++) {
            final JoinKind type = join.getInputs().getQuick(i).getJoinType();
            if (type == JoinKind.RIGHT_OUTER || type == JoinKind.FULL_OUTER) {
                final ExpressionNode criteria = joinModel(source, modelBase, i).getJoinCriteria();
                if (criteria != null && !hasOwnJoinKey(criteria, join, i)) {
                    nonEquiNullingJoinInputs.add(i);
                }
            }
        }
        final boolean hasNonEquiNullingJoin = nonEquiNullingJoinInputs.size() > 0;
        ExpressionNode constantFilter = selectJoinConstantTerms(where, true);
        collectJoinConditions(selectJoinConstantTerms(where, false), join, -1, hasBarriers, hasNonEquiNullingJoin);
        final JoinInput master = join.getInputs().getQuick(0);
        for (int i = dependencyBase, n = lateralDependencyInputs.size(); i < n; i++) {
            joinOrder.addDependency(lateralDependencyParents.getQuick(i), lateralDependencyInputs.getQuick(i));
        }
        for (int i = 1, n = join.getInputs().size(); i < n; i++) {
            final QueryModel occurrence = joinModel(source, modelBase, i);
            final JoinInput slave = join.getInputs().getQuick(i);
            final boolean isBarrier = slave.getJoinType().isBarrier();
            ExpressionNode onCriteria = occurrence.getJoinCriteria();
            if (hasBarriers && !isBarrier && isPostJoinFilterReference(onCriteria, join, i)) {
                final ExpressionNode postJoinFilter = selectPostJoinFilterTerms(onCriteria, join, i, true);
                rejectMasterNullingForwardReference(postJoinFilter, join, i);
                collectJoinConditions(postJoinFilter, join, -1, true, hasNonEquiNullingJoin);
                onCriteria = selectPostJoinFilterTerms(onCriteria, join, i, false);
            }
            if (slave.getJoinType() == JoinKind.SPLICE) {
                validateSpliceOn(onCriteria, join, slave);
            }
            final ExpressionNode forwardReference = findForwardJoinReference(onCriteria, join, i);
            final boolean hasForwardReference = forwardReference != null;
            final boolean isDeferred = !isBarrier && hasForwardReference;
            final boolean isNonEquiNullingJoin = nonEquiNullingJoinInputs.contains(i);
            final boolean isForwardLeftJoin = hasForwardReference && slave.getJoinType() == JoinKind.LEFT_OUTER;
            if (isBarrier && hasForwardReference && !isForwardLeftJoin) {
                throw forwardJoinReference(forwardReference);
            }
            if (isDeferred) {
                deferredJoinInputs.add(i);
            }
            if (hasBarriers && !isNonEquiNullingJoin) {
                collectJoinDependencies(onCriteria, join, i, isDeferred || isForwardLeftJoin);
            }
            if (isBarrier) {
                joinOnNodes.setQuick(i, collectJoinConditions(onCriteria, join, i, true, hasNonEquiNullingJoin));
                if (isNonEquiNullingJoin) {
                    constrainNullingJoinPrefix(0, i);
                    joinOrder.addLateInput(i);
                    nullingJoinBoundaries.add(i);
                } else {
                    final boolean isMasterNulling = slave.getJoinType().isMasterNulling();
                    int prefixStart = slave.getJoinType() == JoinKind.LEFT_OUTER ? i - 1 : 0;
                    // A preceding LEFT join may wait for this one through a forward reference.
                    while (prefixStart > 0 && forwardLeftJoinInputs.contains(prefixStart)) {
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
                        constrainNullingJoinPrefix(commaGroupStart(source, modelBase, i), i);
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
            constrainNullingJoinConsumers(join, boundary);
        }
        if (joinOrder.prepare()) {
            return constantFilter;
        }
        final boolean wasForwardInnerFiltered = isForwardInnerFiltered;
        final boolean wasForwardLeftKeyFiltered = isForwardLeftKeyFiltered;
        if (!isForwardInnerFiltered && deferredJoinInputs.size() > 0) {
            isForwardInnerFiltered = true;
        } else if (!isForwardLeftKeyFiltered && forwardLeftJoinInputs.size() > 0) {
            isForwardLeftKeyFiltered = true;
        }
        if (isForwardInnerFiltered != wasForwardInnerFiltered || isForwardLeftKeyFiltered != wasForwardLeftKeyFiltered) {
            try {
                return collectJoinGraph(join, graph, source, where, dependencyBase, modelBase, hasBarriers);
            } finally {
                isForwardInnerFiltered = wasForwardInnerFiltered;
                isForwardLeftKeyFiltered = wasForwardLeftKeyFiltered;
            }
        }
        if (forwardJoinReferences.size() == 0) {
            throw new IllegalStateException("cyclic join dependencies without a forward ON reference");
        }
        throw forwardJoinReference(forwardJoinReferences.getQuick(0));
    }

    private void collectJoinTimestampScopes(ExpressionNode expression, JoinPlan join, int lastInput) throws SqlException {
        final int sourceIndex = joinExpressionSource(expression, join);
        if (sourceIndex >= 0 && canPushJoinFilter(join, sourceIndex, lastInput)
                && binder.hasSinglePredicateSource(expression, join, null)) {
            ctx.copyTimestampScope(join.getInputs().getQuick(sourceIndex).getSourceOutput());
        }
    }

    /**
     * The first input of the comma group of the input: a comma binds looser than JOIN.
     */
    private int commaGroupStart(QueryModel source, int modelBase, int input) {
        for (int i = input; i > 0; i--) {
            if (joinModel(source, modelBase, i).isCommaJoin()) {
                return i;
            }
        }
        return 0;
    }

    private void constrainNullingJoinConsumers(JoinPlan join, int boundary) {
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
                    constrainNullingJoinPrefix(0, boundary);
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

    /**
     * The number of conjuncts of the conjunction that join the filter of the given join input, or the residual
     * filter for -1.
     */
    private int countJoinFilterConjuncts(ExpressionNode conjunction, int filterInput, JoinPlan join, int lastInput) throws SqlException {
        if (conjunction.paramCount == 2 && SqlKeywords.isAndKeyword(conjunction.token)) {
            return countJoinFilterConjuncts(conjunction.lhs, filterInput, join, lastInput)
                    + countJoinFilterConjuncts(conjunction.rhs, filterInput, join, lastInput);
        }
        return joinFilterInput(conjunction, join, lastInput) == filterInput ? 1 : 0;
    }

    private ExpressionNode findForwardJoinReference(ExpressionNode expression, JoinPlan join, int origin) {
        if (expression == null) {
            return null;
        }
        if (expression.type == ExpressionNode.LITERAL) {
            return joinReferenceSource(expression, join) > origin ? expression : null;
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

    /**
     * Whether the expression, an ON clause of a later comma group, reads a column of an input before the group: a
     * column that neither the group nor an outer query resolves, while the inputs before the group do.
     */
    private boolean hasCommaPrefixReference(ExpressionNode expression, JoinPlan join, JoinPlan group) {
        if (expression == null) {
            return false;
        }
        if (expression.type == ExpressionNode.LITERAL) {
            return FunctionBinder.findColumn(expression, group.getOutput(), null) == -1 && !isOuterColumn(expression, group)
                    && FunctionBinder.findColumn(expression, join.getOutput(), null) > -1;
        }
        if (hasCommaPrefixReference(expression.lhs, join, group) || hasCommaPrefixReference(expression.rhs, join, group)) {
            return true;
        }
        for (int i = 0, n = expression.args.size(); i < n; i++) {
            if (hasCommaPrefixReference(expression.args.getQuick(i), join, group)) {
                return true;
            }
        }
        return false;
    }

    private boolean hasOwnJoinKey(ExpressionNode expression, JoinPlan join, int input) {
        if (expression.paramCount == 2 && SqlKeywords.isAndKeyword(expression.token)) {
            return hasOwnJoinKey(expression.lhs, join, input) || hasOwnJoinKey(expression.rhs, join, input);
        }
        if (expression.paramCount == 2 && Chars.equals(expression.token, '=')
                && expression.lhs.type == ExpressionNode.LITERAL && expression.rhs.type == ExpressionNode.LITERAL
                && !isOuterColumn(expression.lhs, join) && !isOuterColumn(expression.rhs, join)) {
            final int left = joinReferenceSource(expression.lhs, join);
            final int right = joinReferenceSource(expression.rhs, join);
            return left >= 0 && right >= 0 && left != right && Math.max(left, right) == input;
        }
        return false;
    }

    private CharSequence hintAlias(QueryModel model) {
        final BindScope scope = ctx.scope();
        final CharSequence name = model.getName();
        return name != null ? name : scope.hintAliases.getQuick(scope.hintAliasModels.indexOf(model));
    }

    private CharSequence inputAlias(QueryModel occurrence) {
        return occurrence.getName() == null ? hintAlias(occurrence) : sourceAlias(occurrence);
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

    private boolean isPostJoinFilterReference(ExpressionNode expression, JoinPlan join, int origin) {
        final int last = lastJoinReferenceSource(expression, join);
        if (isForwardInnerFiltered) {
            return last > origin;
        }
        for (int i = origin + 1; i <= last; i++) {
            if (join.getInputs().getQuick(i).getJoinType().isBarrier()) {
                return true;
            }
        }
        return false;
    }


    private int joinExpressionSource(ExpressionNode expression, JoinPlan join) {
        if (expression == null) {
            return -1;
        }
        if (expression.type == ExpressionNode.LITERAL) {
            return joinReferenceSource(expression, join);
        }
        int index = mergeJoinSources(joinExpressionSource(expression.lhs, join), joinExpressionSource(expression.rhs, join));
        for (int i = 0, n = expression.args.size(); i < n; i++) {
            index = mergeJoinSources(index, joinExpressionSource(expression.args.getQuick(i), join));
        }
        return index;
    }

    /**
     * The index of the join input whose filter a conjunct of the conjunction joins, -1 for the residual filter.
     */
    private int joinFilterInput(ExpressionNode conjunct, JoinPlan join, int lastInput) throws SqlException {
        final int sourceIndex = joinExpressionSource(conjunct, join);
        return sourceIndex >= 0 && canPushJoinFilter(join, sourceIndex, lastInput) ? sourceIndex : -1;
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

    private QueryModel joinModel(QueryModel source, int modelBase, int input) {
        return source.getJoinModels().getQuick(inputModels.getQuick(modelBase + input));
    }

    /**
     * The input of the join that a column reference reads, or -1 for an outer column and for one the join cannot
     * resolve, which binding the conjunct reports.
     */
    private int joinReferenceSource(ExpressionNode literal, JoinPlan join) {
        if (isOuterColumn(literal, join)) {
            return -1;
        }
        final int index = FunctionBinder.findColumn(literal, join.getOutput(), null);
        return index < 0 ? -1 : LogicalPlans.joinColumnSource(join, join.getOutput().getColumnId(index));
    }

    private int lastJoinReferenceSource(ExpressionNode expression, JoinPlan join) {
        if (expression == null) {
            return -1;
        }
        if (expression.type == ExpressionNode.LITERAL) {
            return joinReferenceSource(expression, join);
        }
        int last = Math.max(lastJoinReferenceSource(expression.lhs, join), lastJoinReferenceSource(expression.rhs, join));
        for (int i = 0, n = expression.args.size(); i < n; i++) {
            last = Math.max(last, lastJoinReferenceSource(expression.args.getQuick(i), join));
        }
        return last;
    }

    /**
     * Rejects the forward references of an INNER ON clause that become a post-join filter when a RIGHT, FULL or
     * SPLICE join follows the clause's input: the filter runs over that join's output and drops the rows it
     * preserves with the input NULL-extended.
     */
    private void rejectMasterNullingForwardReference(ExpressionNode filter, JoinPlan join, int origin) throws SqlException {
        for (int i = origin + 1, n = join.getInputs().size(); i < n; i++) {
            if (join.getInputs().getQuick(i).getJoinType().isMasterNulling()) {
                throw forwardJoinReference(findForwardJoinReference(filter, join, origin));
            }
        }
    }

    /**
     * Rejects the conjuncts of a SPLICE join's ON clause that cannot key it, reporting a qualified column one of the
     * join's sources lacks first.
     */
    private void rejectSpliceResidual(ExpressionNode residual, JoinPlan join) throws SqlException {
        if (residual == null) {
            return;
        }
        residual = appendJoinConjuncts(null, residual);
        for (int i = 0, n = join.getInputs().size(); i < n; i++) {
            validateSpliceOnSourceColumns(residual, join.getInputs().getQuick(i));
        }
        throw SqlException.$(residual.position, "unsupported SPLICE join expression [expr='").put(residual).put("']");
    }

    private int resolveJoinHints(QueryModel master, QueryModel slave) {
        final BindScope scope = ctx.scope();
        final CharSequence masterAlias = hintAlias(master);
        final CharSequence slaveAlias = hintAlias(slave);
        int hints = 0;
        if (SqlHints.hasHintWithParams(scope.currentHints, SqlHints.ASOF_LINEAR_HINT, masterAlias, slaveAlias)) {
            hints |= JoinInput.HINT_ASOF_LINEAR;
        }
        if (SqlHints.hasHintWithParams(scope.currentHints, SqlHints.ASOF_DENSE_HINT, masterAlias, slaveAlias)) {
            hints |= JoinInput.HINT_ASOF_DENSE;
        }
        if (SqlHints.hasHintWithParams(scope.currentHints, SqlHints.ASOF_INDEX_HINT, masterAlias, slaveAlias)) {
            hints |= JoinInput.HINT_ASOF_INDEX;
        }
        if (SqlHints.hasHintWithParams(scope.currentHints, SqlHints.ASOF_MEMOIZED_HINT, masterAlias, slaveAlias)) {
            hints |= JoinInput.HINT_ASOF_MEMOIZED;
        }
        if (SqlHints.hasHintWithParams(scope.currentHints, SqlHints.ASOF_MEMOIZED_DRIVEBY_HINT, masterAlias, slaveAlias)) {
            hints |= JoinInput.HINT_ASOF_MEMOIZED_DRIVEBY;
        }
        if (SqlHints.hasHintWithParams(scope.currentHints, SqlHints.MARKOUT_HORIZON_HINT, masterAlias, slaveAlias)) {
            hints |= JoinInput.HINT_MARKOUT_HORIZON;
        }
        return hints;
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

    /**
     * Appends to {@code prefix} the conjuncts of a SPLICE join's ON clause that cannot key it: all but the equalities
     * of a column of the slave with a column of an earlier input.
     */
    private ExpressionNode spliceResidual(ExpressionNode prefix, ExpressionNode expression, JoinPlan join, JoinInput slave) throws SqlException {
        if (expression == null) {
            return prefix;
        }
        if (expression.paramCount == 2 && SqlKeywords.isAndKeyword(expression.token)) {
            return spliceResidual(spliceResidual(prefix, expression.lhs, join, slave), expression.rhs, join, slave);
        }
        if (expression.paramCount == 2 && Chars.equals(expression.token, '=')
                && expression.lhs.type == ExpressionNode.LITERAL && expression.rhs.type == ExpressionNode.LITERAL
                && !isOuterColumn(expression.lhs, join) && !isOuterColumn(expression.rhs, join)) {
            final OutputSchema output = join.getOutput();
            final OutputSchema slaveOutput = slave.getSourceOutput();
            final boolean isLeftSlave = slaveOutput.getColumnIndexById(output.getColumnId(ctx.bindColumnIndex(expression.lhs, output, (CharSequence) null))) >= 0;
            final boolean isRightSlave = slaveOutput.getColumnIndexById(output.getColumnId(ctx.bindColumnIndex(expression.rhs, output, (CharSequence) null))) >= 0;
            if (isLeftSlave != isRightSlave) {
                return prefix;
            }
        }
        return appendJoinConjuncts(prefix, expression);
    }

    /**
     * Validates the columns of a SPLICE join's ON clause before any other check reads them: the operands of its
     * equalities and matches, then the qualified columns of the conjuncts that cannot key the join, which
     * {@link #rejectSpliceResidual} rejects once the join's inputs validate.
     */
    private void validateSpliceOn(ExpressionNode on, JoinPlan join, JoinInput slave) throws SqlException {
        validateSpliceOnAnalysis(on, join);
        final ExpressionNode residual = spliceResidual(null, on, join, slave);
        for (int i = 0, n = join.getInputs().size(); residual != null && i < n; i++) {
            validateSpliceOnSourceColumns(residual, join.getInputs().getQuick(i));
        }
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

    static void validateTimeSeriesTimestamps(int position, int masterTimestampId, OutputSchema slave) throws SqlException {
        if (masterTimestampId < 0) {
            throw SqlException.$(position, "left side of time series join has no timestamp");
        }
        if (slave.getTimestampIndex() < 0) {
            throw SqlException.$(position, "right side of time series join has no timestamp");
        }
    }

    void addJoinOutput(JoinPlan join, OutputSchema output, CharSequence alias) {
        final OutputSchema target = join.getOutput();
        for (int i = 0, n = output.getColumnCount(); i < n; i++) {
            target.add(output.getColumnId(i), output.getColumnName(i), output.getColumnType(i), output.getMetadata(i), output.isVisible(i), alias);
            target.setSymbolTableStatic(target.getColumnCount() - 1, output.isSymbolTableStatic(i));
        }
    }

    LogicalPlan bindJoins(QueryModel source, ExpressionNode where, SqlExecutionContext executionContext) throws SqlException {
        return bindJoins(source, where, source.getJoinModels().size(), executionContext);
    }

    LogicalPlan bindJoins(QueryModel source, ExpressionNode where, int sourceCount,
                          SqlExecutionContext executionContext) throws SqlException {
        final BindScope scope = ctx.scope();
        final boolean previous = scope.isInsideJoin;
        scope.isInsideJoin = true;
        try {
            return bindJoinSources(source, where, 0, sourceCount, executionContext);
        } finally {
            scope.isInsideJoin = previous;
        }
    }

    // Hints can name aliases assigned to unnamed subqueries before join rewriting.
    void collectHintAliases(QueryModel model) {
        final BindScope scope = ctx.scope();
        if (model == null) {
            return;
        }
        if (model.getName() == null && scope.hintAliasModels.indexOf(model) < 0) {
            scope.hintAliasModels.add(model);
            final CharacterStoreEntry alias = ctx.characterStore.newEntry();
            alias.put(QueryModel.SUB_QUERY_ALIAS_PREFIX).put(scope.hintAliases.size());
            scope.hintAliases.add(alias.toImmutable());
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
