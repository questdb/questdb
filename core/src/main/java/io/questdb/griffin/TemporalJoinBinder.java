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
import io.questdb.griffin.model.ExpressionNode;
import io.questdb.griffin.model.HorizonJoinContext;
import io.questdb.griffin.model.PivotForColumn;
import io.questdb.griffin.model.QueryColumn;
import io.questdb.griffin.model.QueryModel;
import io.questdb.griffin.model.WindowJoinContext;
import io.questdb.griffin.plan.logical.BoundExpression;
import io.questdb.griffin.plan.logical.ColumnExpression;
import io.questdb.griffin.plan.logical.ConstantExpression;
import io.questdb.griffin.plan.logical.FilterPlan;
import io.questdb.griffin.plan.logical.FunctionExpression;
import io.questdb.griffin.plan.logical.HorizonJoinPlan;
import io.questdb.griffin.plan.logical.HorizonJoinSlave;
import io.questdb.griffin.plan.logical.JoinInput;
import io.questdb.griffin.plan.logical.LogicalPlan;
import io.questdb.griffin.plan.logical.OutputSchema;
import io.questdb.griffin.plan.logical.ProjectPlan;
import io.questdb.griffin.plan.logical.WindowJoinPlan;
import io.questdb.griffin.plan.logical.WindowJoinStep;
import io.questdb.std.Chars;
import io.questdb.std.GenericLexer;
import io.questdb.std.IntList;
import io.questdb.std.Mutable;
import io.questdb.std.Numbers;
import io.questdb.std.NumericException;
import io.questdb.std.ObjList;
import io.questdb.std.ObjectPool;

import static io.questdb.griffin.BindContext.*;

final class TemporalJoinBinder implements Mutable {
    private final AggregateBinder aggregateBinder;
    private final SqlBinder binder;
    private final BindContext ctx;
    private final ObjectPool<HorizonJoinPlan> horizonJoinPlans = new ObjectPool<>(HorizonJoinPlan.FACTORY, 2);
    private final ObjectPool<HorizonJoinSlave> horizonJoinSlaves = new ObjectPool<>(HorizonJoinSlave.FACTORY, 2);
    private final JoinBinder joinBinder;
    private final LateralBinder lateralBinder;
    private final OrderBinder orderBinder;
    private final IntList windowJoinAggregateSteps = new IntList();
    private final ObjectPool<WindowJoinPlan> windowJoinPlans = new ObjectPool<>(WindowJoinPlan.FACTORY, 2);
    private final ObjectPool<WindowJoinStep> windowJoinSteps = new ObjectPool<>(WindowJoinStep.FACTORY, 2);

    TemporalJoinBinder(
            BindContext ctx,
            SqlBinder binder,
            OrderBinder orderBinder,
            AggregateBinder aggregateBinder,
            JoinBinder joinBinder,
            LateralBinder lateralBinder
    ) {
        this.ctx = ctx;
        this.binder = binder;
        this.orderBinder = orderBinder;
        this.aggregateBinder = aggregateBinder;
        this.joinBinder = joinBinder;
        this.lateralBinder = lateralBinder;
    }

    @Override
    public void clear() {
        horizonJoinPlans.clear();
        horizonJoinSlaves.clear();
        windowJoinPlans.clear();
        windowJoinSteps.clear();
        windowJoinAggregateSteps.clear();
    }

    private static boolean hasUnresolvableReference(ExpressionNode node, OutputSchema scope, CharSequence alias) {
        if (node == null || node.queryModel != null) {
            return false;
        }
        if (node.type == ExpressionNode.LITERAL) {
            return FunctionBinder.findColumn(node, scope, alias) == -1;
        }
        if (hasUnresolvableReference(node.lhs, scope, alias) || hasUnresolvableReference(node.rhs, scope, alias)) {
            return true;
        }
        for (int i = 0, n = node.args.size(); i < n; i++) {
            if (hasUnresolvableReference(node.args.getQuick(i), scope, alias)) {
                return true;
            }
        }
        return false;
    }

    private static int horizonGroupByIndex(ExpressionNode key) {
        if (key.type != ExpressionNode.CONSTANT) {
            return -1;
        }
        try {
            return Numbers.parseInt(key.token);
        } catch (NumericException e) {
            return -1;
        }
    }

    private static boolean isHorizonGroupByMatch(ExpressionNode key, QueryColumn column) {
        return isSameIgnoringQualifier(key, column.getAst())
                || Chars.indexOfLastUnquoted(key.token, '.') < 0 && Chars.equalsIgnoreCase(key.token, column.getAlias());
    }

    private static boolean isHorizonOffsetModel(QueryModel model) {
        return model.getTableNameExpr() == null && model.getNestedModel() == null
                && model.getHorizonJoinContext().getAlias() != null;
    }

    private static boolean isSameIgnoringQualifier(ExpressionNode a, ExpressionNode b) {
        if (a == null || b == null) {
            return a == b;
        }
        if (a.type != b.type) {
            return false;
        }
        if (a.type == ExpressionNode.LITERAL) {
            return Chars.equalsIgnoreCase(a.token, b.token) || Chars.equalsIgnoreCase(unqualified(a.token), b.token)
                    || Chars.equalsIgnoreCase(a.token, unqualified(b.token));
        }
        if (a.type == ExpressionNode.FUNCTION ? !Chars.equalsIgnoreCase(a.token, b.token) : !Chars.equals(a.token, b.token)) {
            return false;
        }
        final int count = a.args.size();
        if (count != b.args.size()) {
            return false;
        }
        if (count < 3) {
            return isSameIgnoringQualifier(a.lhs, b.lhs) && isSameIgnoringQualifier(a.rhs, b.rhs);
        }
        for (int i = 0; i < count; i++) {
            if (!isSameIgnoringQualifier(a.args.getQuick(i), b.args.getQuick(i))) {
                return false;
            }
        }
        return true;
    }

    private static int mergeReferencePosition(int position, int child) {
        return position == -2 || child == -2 ? -2 : position == -1 ? child : position;
    }

    private static void rejectHorizonWhere(ExpressionNode node, OutputSchema master, CharSequence masterAlias) throws SqlException {
        if (node == null) {
            return;
        }
        if (SqlKeywords.isAndKeyword(node.token) && node.paramCount == 2) {
            rejectHorizonWhere(node.lhs, master, masterAlias);
            rejectHorizonWhere(node.rhs, master, masterAlias);
            return;
        }
        if (hasUnresolvableReference(node, master, masterAlias)) {
            throw SqlException.position(node.position).put("WHERE clause of HORIZON JOIN can only reference left-hand side columns");
        }
    }

    private static void rejectSlaveBoundReference(ExpressionNode node, CharSequence slaveAlias) throws SqlException {
        if (node == null) {
            return;
        }
        if (node.type == ExpressionNode.LITERAL) {
            final int dot = Chars.indexOfLastUnquoted(node.token, '.');
            if (dot > 0 && slaveAlias != null && Chars.equalsIgnoreCase(GenericLexer.unquote(node.token.subSequence(0, dot)), slaveAlias)) {
                throw SqlException.$(node.position, "RANGE BETWEEN expression must not reference right table columns");
            }
            return;
        }
        rejectSlaveBoundReference(node.lhs, slaveAlias);
        rejectSlaveBoundReference(node.rhs, slaveAlias);
        for (int i = 0, n = node.args.size(); i < n; i++) {
            rejectSlaveBoundReference(node.args.getQuick(i), slaveAlias);
        }
    }

    private static void rejectUnknownHorizonColumn(ExpressionNode node, OutputSchema output, CharSequence horizonAlias) throws SqlException {
        if (node == null) {
            return;
        }
        if (node.type == ExpressionNode.LITERAL) {
            final int dot = Chars.indexOfLastUnquoted(node.token, '.');
            if (dot > 0 && Chars.equalsIgnoreCase(GenericLexer.unquote(node.token.subSequence(0, dot)), horizonAlias)
                    && FunctionBinder.findColumn(node, output, null) < 0) {
                throw SqlException.invalidColumn(node.position, node.token);
            }
            return;
        }
        rejectUnknownHorizonColumn(node.lhs, output, horizonAlias);
        rejectUnknownHorizonColumn(node.rhs, output, horizonAlias);
        for (int i = 0, n = node.args.size(); i < n; i++) {
            rejectUnknownHorizonColumn(node.args.getQuick(i), output, horizonAlias);
        }
    }

    private static int timestampPredicatePosition(ExpressionNode node, CharSequence timestamp) {
        if (node.paramCount == 2 && SqlKeywords.isAndKeyword(node.token)) {
            final int position = timestampPredicatePosition(node.lhs, timestamp);
            return position >= 0 ? position : timestampPredicatePosition(node.rhs, timestamp);
        }
        return timestampReferencePosition(node, timestamp);
    }

    /**
     * Returns the first reference position when every literal names the timestamp, -1 when none does, -2 otherwise.
     */
    private static int timestampReferencePosition(ExpressionNode node, CharSequence timestamp) {
        if (node == null) {
            return -1;
        }
        if (node.type == ExpressionNode.LITERAL) {
            return Chars.equalsIgnoreCase(node.token, timestamp) ? node.position : -2;
        }
        int position = mergeReferencePosition(-1, timestampReferencePosition(node.lhs, timestamp));
        position = mergeReferencePosition(position, timestampReferencePosition(node.rhs, timestamp));
        for (int i = node.args.size() - 1; i >= 0; i--) {
            position = mergeReferencePosition(position, timestampReferencePosition(node.args.getQuick(i), timestamp));
        }
        return position;
    }

    private static void validateWindowJoinFilter(ExpressionNode node, CharSequence slaveAlias) throws SqlException {
        if (node == null || slaveAlias == null) {
            return;
        }
        if (node.type == ExpressionNode.LITERAL) {
            final int dot = Chars.indexOfLastUnquoted(node.token, '.');
            if (dot > 0 && Chars.equalsIgnoreCase(GenericLexer.unquote(node.token.subSequence(0, dot)), slaveAlias)) {
                throw SqlException.invalidColumn(node.position, node.token);
            }
            return;
        }
        validateWindowJoinFilter(node.lhs, slaveAlias);
        validateWindowJoinFilter(node.rhs, slaveAlias);
        for (int i = 0, n = node.args.size(); i < n; i++) {
            validateWindowJoinFilter(node.args.getQuick(i), slaveAlias);
        }
    }

    private static CharSequence windowJoinAggregateName(ExpressionNode node, QueryModel model) {
        for (int i = 0, n = model.getBottomUpColumns().size(); i < n; i++) {
            final QueryColumn column = model.getBottomUpColumns().getQuick(i);
            if (column.getAst() == node) {
                return column.getName();
            }
        }
        return node.token;
    }

    private void addHorizonOffset(HorizonJoinPlan plan, ExpressionNode offset, int position) {
        plan.getOffsets().add(offset.token);
        plan.getOffsetPositions().add(position);
    }

    private void bindHorizonKeys(ExpressionNode criteria, HorizonJoinSlave step, OutputSchema master, CharSequence masterAlias,
                                 OutputSchema slave, CharSequence slaveAlias) throws SqlException {
        if (criteria == null) {
            return;
        }
        if (SqlKeywords.isAndKeyword(criteria.token) && criteria.paramCount == 2) {
            bindHorizonKeys(criteria.lhs, step, master, masterAlias, slave, slaveAlias);
            bindHorizonKeys(criteria.rhs, step, master, masterAlias, slave, slaveAlias);
            return;
        }
        if (criteria.paramCount == 2 && Chars.equals(criteria.token, '=')
                && criteria.lhs.type == ExpressionNode.LITERAL && criteria.rhs.type == ExpressionNode.LITERAL) {
            final boolean isMasterLeft = FunctionBinder.findColumn(criteria.lhs, master, masterAlias) >= 0;
            final ExpressionNode masterNode = isMasterLeft ? criteria.lhs : criteria.rhs;
            final ExpressionNode slaveNode = isMasterLeft ? criteria.rhs : criteria.lhs;
            final int masterIndex = FunctionBinder.findColumn(masterNode, master, masterAlias);
            final int slaveIndex = FunctionBinder.findColumn(slaveNode, slave, slaveAlias);
            if (masterIndex >= 0 && slaveIndex >= 0) {
                step.getMasterKeyColumnIds().add(master.getColumnId(masterIndex));
                step.getSlaveKeyColumnIds().add(slave.getColumnId(slaveIndex));
                step.getKeyPositions().add(slaveNode.position);
                return;
            }
        }
        throw SqlException.$(criteria.position, "unsupported HORIZON join expression [expr='").put(criteria).put("']");
    }

    private void bindWindowJoinBounds(WindowJoinStep step, WindowJoinContext syntax, OutputSchema master,
                                      SqlExecutionContext executionContext) throws SqlException {
        final int loKind = syntax.getLoKind();
        if (loKind == WindowJoinContext.CURRENT) {
            step.setLo(0, null, 0, (char) 0, syntax.getLoKindPos());
        } else {
            final long value = windowJoinBoundValue(syntax.getLoExpr(), executionContext);
            if (value == -1) {
                step.setLo(0, windowJoinBoundExpression(syntax.getLoExpr(), master, step.getSlaveAlias(), executionContext),
                        loKind == WindowJoinContext.PRECEDING ? 1 : -1, syntax.getLoExprTimeUnit(), syntax.getLoExprPos());
            } else {
                final long lo = loKind == WindowJoinContext.PRECEDING ? value : value == Long.MAX_VALUE ? Long.MIN_VALUE : -value;
                if (lo == Long.MIN_VALUE || lo == Long.MAX_VALUE) {
                    throw SqlException.position(syntax.getLoKindPos()).put("unbounded preceding/following is not supported in WINDOW joins");
                }
                step.setLo(lo, null, 0, syntax.getLoExprTimeUnit(), syntax.getLoExprPos());
            }
        }
        final int hiKind = syntax.getHiKind();
        if (hiKind == WindowJoinContext.CURRENT) {
            step.setHi(0, null, 0, (char) 0, syntax.getHiKindPos());
        } else {
            final long value = windowJoinBoundValue(syntax.getHiExpr(), executionContext);
            if (value == -1) {
                step.setHi(0, windowJoinBoundExpression(syntax.getHiExpr(), master, step.getSlaveAlias(), executionContext),
                        hiKind == WindowJoinContext.FOLLOWING ? 1 : -1, syntax.getHiExprTimeUnit(), syntax.getHiExprPos());
            } else {
                final long hi = hiKind == WindowJoinContext.FOLLOWING ? value : value == Long.MAX_VALUE ? Long.MIN_VALUE : -value;
                if (hi == Long.MIN_VALUE || hi == Long.MAX_VALUE) {
                    throw SqlException.position(syntax.getHiKindPos()).put("unbounded preceding/following is not supported in WINDOW joins");
                }
                step.setHi(hi, null, 0, syntax.getHiExprTimeUnit(), syntax.getHiExprPos());
            }
        }
    }

    private void validateHorizonGroupBy(ObjList<QueryColumn> selected, ObjList<ExpressionNode> groupBy) throws SqlException {
        for (int i = 0, n = groupBy.size(); i < n; i++) {
            final ExpressionNode key = groupBy.getQuick(i);
            final int index = horizonGroupByIndex(key);
            if (index > 0) {
                if (index > selected.size()) {
                    throw SqlException.$(key.position, "GROUP BY position ").put(index).put(" is not in select list");
                }
                if (ctx.isAggregate(selected.getQuick(index - 1).getAst())) {
                    throw SqlException.$(key.position, "HORIZON JOIN GROUP BY cannot reference aggregate column at position ").put(index);
                }
                continue;
            }
            boolean isFound = false;
            for (int k = 0, m = selected.size(); k < m && !isFound; k++) {
                final QueryColumn column = selected.getQuick(k);
                isFound = !ctx.isAggregate(column.getAst()) && isHorizonGroupByMatch(key, column);
            }
            if (!isFound) {
                throw SqlException.$(key.position, "HORIZON JOIN GROUP BY column must match a non-aggregate SELECT column");
            }
        }
        for (int i = 0, n = selected.size(); i < n; i++) {
            final QueryColumn column = selected.getQuick(i);
            if (ctx.hasAggregate(column.getAst())) {
                continue;
            }
            boolean isCovered = false;
            for (int k = 0, m = groupBy.size(); k < m && !isCovered; k++) {
                final ExpressionNode key = groupBy.getQuick(k);
                isCovered = horizonGroupByIndex(key) == i + 1 || isHorizonGroupByMatch(key, column);
            }
            if (!isCovered) {
                throw SqlException.$(column.getAst().position, "non-aggregate column must be included in HORIZON JOIN GROUP BY clause");
            }
        }
    }

    private BoundExpression windowJoinBoundExpression(ExpressionNode expression, OutputSchema master, CharSequence slaveAlias,
                                                      SqlExecutionContext executionContext) throws SqlException {
        rejectSlaveBoundReference(expression, slaveAlias);
        return ctx.functionBinder.bind(expression, master, null, executionContext);
    }

    private long windowJoinBoundValue(ExpressionNode expression, SqlExecutionContext executionContext) throws SqlException {
        if (expression == null) {
            return Long.MAX_VALUE;
        }
        if (hasLiteral(expression)) {
            return -1;
        }
        final BoundExpression bound = ctx.functionBinder.bind(expression, ctx.emptySchema, null, executionContext);
        if (!(bound instanceof ConstantExpression constant)) {
            return -1;
        }
        final long value;
        switch (ColumnType.tagOf(constant.getDataType())) {
            case ColumnType.BYTE, ColumnType.SHORT, ColumnType.INT, ColumnType.LONG -> value = constant.getLongValue();
            case ColumnType.CHAR -> {
                final long digit = (byte) (constant.getLongValue() - '0');
                value = digit > -1 && digit < 10 ? digit : Numbers.LONG_NULL;
            }
            case ColumnType.NULL -> value = Numbers.LONG_NULL;
            default -> throw SqlException.$(expression.position, "integer expression expected");
        }
        if (value < 0) {
            throw SqlException.$(expression.position, "non-negative integer expression expected");
        }
        return value;
    }

    private ExpressionNode windowJoinCriteria(QueryModel occurrence, CharSequence masterAlias, CharSequence slaveAlias) {
        ExpressionNode criteria = occurrence.getJoinCriteria();
        final ObjList<ExpressionNode> shorthand = occurrence.getJoinColumns();
        for (int i = 0, n = shorthand.size(); i < n; i++) {
            final ExpressionNode column = shorthand.getQuick(i);
            final ExpressionNode equality = ctx.bindingExpressions.next().of(ExpressionNode.OPERATION, "=", 0, column.position);
            equality.paramCount = 2;
            equality.lhs = ctx.bindingExpressions.next().of(ExpressionNode.LITERAL, ctx.qualifiedJoinName(masterAlias, column.token), 0, column.position);
            equality.rhs = ctx.bindingExpressions.next().of(ExpressionNode.LITERAL, ctx.qualifiedJoinName(slaveAlias, column.token), 0, column.position);
            if (criteria == null) {
                criteria = equality;
            } else {
                final ExpressionNode and = ctx.bindingExpressions.next().of(ExpressionNode.OPERATION, "and", 0, column.position);
                and.paramCount = 2;
                and.lhs = criteria;
                and.rhs = equality;
                criteria = and;
            }
        }
        return criteria;
    }

    private int windowJoinReferencedStep(ExpressionNode node, WindowJoinPlan plan, OutputSchema master, int step, int position) throws SqlException {
        if (node == null) {
            return step;
        }
        if (node.type == ExpressionNode.LITERAL) {
            if (FunctionBinder.findColumn(node, master, plan.getSteps().getQuick(0).getMasterAlias()) >= 0) {
                return step;
            }
            for (int s = 0, n = plan.getSteps().size(); s < n; s++) {
                final WindowJoinStep candidate = plan.getSteps().getQuick(s);
                if (FunctionBinder.findColumn(node, candidate.getSlave().getOutput(), candidate.getSlaveAlias()) >= 0) {
                    if (step >= 0 && step != s) {
                        throw SqlException.$(position, "WINDOW join aggregate function cannot reference columns from multiple models");
                    }
                    return s;
                }
            }
            return step;
        }
        step = windowJoinReferencedStep(node.lhs, plan, master, step, position);
        step = windowJoinReferencedStep(node.rhs, plan, master, step, position);
        for (int i = 0, n = node.args.size(); i < n; i++) {
            step = windowJoinReferencedStep(node.args.getQuick(i), plan, master, step, position);
        }
        return step;
    }

    private int windowJoinStepOf(ExpressionNode node, WindowJoinPlan plan, OutputSchema master) throws SqlException {
        return Math.max(windowJoinReferencedStep(node, plan, master, -1, node.position), 0);
    }

    /**
     * A constant-offset dateadd over the designated timestamp keeps its order, so it designates the
     * projection when the timestamp itself is not selected.
     */
    static void designateTimestampOffset(ProjectPlan project) {
        final OutputSchema output = project.getOutput();
        final LogicalPlan input = project.getInput();
        final int timestampId = input.getOutput().getTimestampColumnId();
        if (output.getTimestampIndex() >= 0 || timestampId < 0 || input.getType() == LogicalPlan.Type.AGGREGATE
                || input.getType() == LogicalPlan.Type.SAMPLE_BY || input.getType() == LogicalPlan.Type.DISTINCT
                || input.getType() == LogicalPlan.Type.WINDOW) {
            return;
        }
        for (int i = 0, n = project.getExpressions().size(); i < n; i++) {
            if (project.getExpressions().getQuick(i) instanceof ColumnExpression column && column.isDirectReference()
                    && column.getColumnId() == timestampId) {
                return;
            }
        }
        for (int i = 0, n = project.getExpressions().size(); i < n; i++) {
            if (project.getExpressions().getQuick(i) instanceof FunctionExpression call && call.getArgumentCount() == 3
                    && SqlKeywords.isDateaddKeyword(call.getName())
                    && call.argumentAt(0) instanceof ConstantExpression
                    && call.argumentAt(1) instanceof ConstantExpression stride && stride.isLiteral()
                    && (ColumnType.tagOf(stride.getDataType()) == ColumnType.INT || ColumnType.tagOf(stride.getDataType()) == ColumnType.LONG
                    || ColumnType.tagOf(stride.getDataType()) == ColumnType.SHORT || ColumnType.tagOf(stride.getDataType()) == ColumnType.BYTE)
                    && call.argumentAt(2) instanceof ColumnExpression column && column.getColumnId() == timestampId
                    && ColumnType.isTimestamp(output.getColumnType(i))) {
                output.setTimestampIndex(i);
                return;
            }
        }
    }

    static int horizonJoinIndex(QueryModel source) {
        for (int i = 1, n = source.getJoinModels().size(); i < n; i++) {
            if (source.getJoinModels().getQuick(i).getJoinType() == QueryModel.JOIN_HORIZON) {
                return i;
            }
        }
        return -1;
    }

    /**
     * Returns whether an inner ON conjunct declared at {@code originPosition} binds at the join of the
     * input at {@code target} instead. That holds when the target precedes the origin, the origin join is
     * not a reordering barrier, the target is an ASOF, LT or SPLICE join, and no join between them can
     * null master rows.
     */
    static boolean isTemporalAnchor(ObjList<JoinInput> ordered, int target, int originPosition) {
        if (target >= originPosition || JoinOrderSolver.isBarrier(ordered.getQuick(originPosition).getJoinType())) {
            return false;
        }
        switch (ordered.getQuick(target).getJoinType()) {
            case QueryModel.JOIN_ASOF, QueryModel.JOIN_LT, QueryModel.JOIN_SPLICE -> {
            }
            default -> {
                return false;
            }
        }
        for (int i = target + 1; i < originPosition; i++) {
            if (LogicalPlans.isMasterNullingJoin(ordered.getQuick(i).getJoinType())) {
                return false;
            }
        }
        return true;
    }

    static void rejectWindowJoinSlaveColumn(ExpressionNode expression, OutputSchema master, WindowJoinPlan plan) throws SqlException {
        if (expression.type != ExpressionNode.LITERAL
                || FunctionBinder.findColumn(expression, master, plan.getSteps().getQuick(0).getMasterAlias()) >= 0) {
            return;
        }
        for (int s = 0, m = plan.getSteps().size(); s < m; s++) {
            final WindowJoinStep step = plan.getSteps().getQuick(s);
            if (FunctionBinder.findColumn(expression, step.getSlave().getOutput(), step.getSlaveAlias()) >= 0) {
                throw SqlException.position(expression.position)
                        .put("WINDOW join cannot reference right table non-aggregate column: ").put(expression.token);
            }
        }
    }

    /**
     * The first constant-offset dateadd over the designated timestamp inverts its stride for interval
     * pushdown, so the inverse must fit dateadd's INT stride. A projected plain timestamp takes precedence.
     */
    static void validateTimestampOffset(QueryModel model, LogicalPlan input, CharSequence alias) throws SqlException {
        final OutputSchema output = input.getOutput();
        final int timestampIndex = output.getTimestampIndex();
        if (timestampIndex < 0 || input.getType() == LogicalPlan.Type.AGGREGATE || input.getType() == LogicalPlan.Type.SAMPLE_BY
                || input.getType() == LogicalPlan.Type.DISTINCT || input.getType() == LogicalPlan.Type.WINDOW) {
            return;
        }
        final ObjList<QueryColumn> columns = model.getBottomUpColumns();
        for (int i = 0, n = columns.size(); i < n; i++) {
            final ExpressionNode ast = columns.getQuick(i).getAst();
            if (ast.type == ExpressionNode.LITERAL && FunctionBinder.findColumn(ast, output, alias) == timestampIndex) {
                return;
            }
        }
        for (int i = 0, n = columns.size(); i < n; i++) {
            final ExpressionNode ast = columns.getQuick(i).getAst();
            if (ast.type != ExpressionNode.FUNCTION || ast.paramCount != 3 || ast.args.size() != 3
                    || !SqlKeywords.isDateaddKeyword(ast.token) || ast.args.getQuick(2).type != ExpressionNode.CONSTANT
                    || ast.args.getQuick(0).type != ExpressionNode.LITERAL
                    || FunctionBinder.findColumn(ast.args.getQuick(0), output, alias) != timestampIndex) {
                continue;
            }
            final ExpressionNode stride = ast.args.getQuick(1);
            final boolean isNegated = stride.type == ExpressionNode.OPERATION && stride.paramCount == 1
                    && Chars.equals(stride.token, '-') && stride.rhs != null && stride.rhs.type == ExpressionNode.CONSTANT;
            if (stride.type != ExpressionNode.CONSTANT && !isNegated) {
                continue;
            }
            final long value;
            try {
                value = Numbers.parseLong(isNegated ? stride.rhs.token : stride.token);
            } catch (NumericException e) {
                continue;
            }
            final CharSequence unit = ast.args.getQuick(2).token;
            final int unitLength = unit.length();
            final long inverse = isNegated ? value : -value;
            if ((unitLength == 1 || unitLength == 3 && unit.charAt(0) == '\'' && unit.charAt(2) == '\'')
                    && (inverse < Integer.MIN_VALUE || inverse > Integer.MAX_VALUE)) {
                throw SqlException.position(stride.position).put("timestamp offset value ").put(inverse)
                        .put(" exceeds maximum allowed range for dateadd function (must be between ")
                        .put(Integer.MIN_VALUE).put(" and ").put(Integer.MAX_VALUE).put(')');
            }
            return;
        }
    }

    /**
     * Reports an invalid dateadd unit of a dateadd-designated timestamp at the WHERE clause's reference to
     * that timestamp, before the projection compiles, because the predicate binds below the projection.
     */
    static void validateTimestampOffsetUnit(QueryModel source) throws SqlException {
        final ExpressionNode timestamp = source.getTimestamp();
        if (timestamp == null || source.getWhereClause() == null) {
            return;
        }
        final QueryColumn column = source.getNestedModel().getAliasToColumnMap().get(timestamp.token);
        final ExpressionNode ast = column == null ? null : column.getAst();
        if (ast == null || ast.type != ExpressionNode.FUNCTION || !SqlKeywords.isDateaddKeyword(ast.token) || ast.args.size() != 3
                || ast.args.getQuick(0).type != ExpressionNode.LITERAL || ast.args.getQuick(2).type != ExpressionNode.CONSTANT) {
            return;
        }
        final CharSequence unit = ast.args.getQuick(2).token;
        final char period = unit.length() == 1 ? unit.charAt(0)
                : unit.length() == 3 && unit.charAt(0) == '\'' && unit.charAt(2) == '\'' ? unit.charAt(1) : 0;
        if (period == 0 || ColumnType.getTimestampDriver(ColumnType.TIMESTAMP_MICRO).getAddMethod(period) != null) {
            return;
        }
        final int position = timestampPredicatePosition(source.getWhereClause(), timestamp.token);
        if (position >= 0) {
            throw SqlException.$(position, "invalid time period [unit=").put(period).put(']');
        }
    }

    static int windowJoinIndex(QueryModel source) {
        for (int i = 1, n = source.getJoinModels().size(); i < n; i++) {
            if (source.getJoinModels().getQuick(i).getJoinType() == QueryModel.JOIN_WINDOW) {
                return i;
            }
        }
        return -1;
    }

    LogicalPlan bindHorizonJoin(QueryModel model, QueryModel source, ExpressionNode where,
                                SqlExecutionContext executionContext) throws SqlException {
        final ObjList<QueryModel> sources = source.getJoinModels();
        final QueryModel last = sources.getLast();
        final int sourceCount = sources.size();
        if (source.getSampleBy() != null) {
            throw SqlException.$(source.getSampleBy().position, "SAMPLE BY cannot be used with HORIZON JOIN");
        }
        final ObjList<QueryColumn> selected = model.getBottomUpColumns();
        for (int i = 0, n = selected.size(); i < n; i++) {
            final QueryColumn column = selected.getQuick(i);
            if (column.isWindowExpression()) {
                throw SqlException.$(column.getAst().position, "WINDOW functions are not allowed in HORIZON JOIN queries");
            }
        }
        final ObjList<ExpressionNode> groupBy = blockGroupBy(model, source);
        if (groupBy.size() > 0 && !model.isPivot()) {
            validateHorizonGroupBy(selected, groupBy);
        }
        int offsetIndex = -1;
        for (int i = 1; i < sourceCount && offsetIndex < 0; i++) {
            if (isHorizonOffsetModel(sources.getQuick(i))) {
                offsetIndex = i;
            }
        }
        if (offsetIndex < 0) {
            throw SqlException.position(sources.getQuick(1).getJoinKeywordPosition()).put("HORIZON JOIN requires offset configuration (RANGE or LIST)");
        }
        for (int i = offsetIndex + 2; i < sourceCount; i++) {
            if (!isHorizonOffsetModel(sources.getQuick(i))) {
                throw SqlException.position(sources.getQuick(i).getJoinKeywordPosition()).put("RANGE or LIST must only appear on the last HORIZON JOIN");
            }
        }
        final HorizonJoinContext horizon = sources.getQuick(offsetIndex).getHorizonJoinContext();
        final QueryModel masterModel = sources.getQuick(0);
        final CharSequence masterAlias = sourceAlias(masterModel);
        LogicalPlan master = binder.bindSource(model, masterModel, executionContext);
        if (ctx.lateralScopes.size() > 0) {
            master = lateralBinder.correlateSource(model, masterModel, masterAlias, master, where, executionContext);
            where = ctx.correlatedWhere;
        }
        if (where != null) {
            rejectHorizonWhere(where, master.getOutput(), masterAlias);
            final BoundExpression predicate = binder.bindPredicate(where, master, masterModel, executionContext);
            final FilterPlan filter = ctx.filters.next().of(master, predicate, predicate.getPosition());
            filter.getOutput().copyFrom(master.getOutput());
            master = filter;
        }
        final OutputSchema masterOutput = master.getOutput();
        if (masterOutput.getTimestampIndex() < 0) {
            final QueryModel first = sources.getQuick(offsetIndex == 1 ? 2 : 1);
            throw SqlException.$(first.getJoinKeywordPosition(), "left side of time series join has no timestamp");
        }
        final CharSequence horizonAlias = GenericLexer.unquote(horizon.getAlias().token);
        final HorizonJoinPlan plan = horizonJoinPlans.next().of(master, masterAlias, horizonAlias, horizon.getAliasPosition(),
                horizon.getMode() == HorizonJoinContext.MODE_RANGE ? HorizonJoinPlan.MODE_RANGE : HorizonJoinPlan.MODE_LIST,
                last.getJoinKeywordPosition());
        if (horizon.getMode() == HorizonJoinContext.MODE_RANGE) {
            addHorizonOffset(plan, horizon.getRangeFrom(), horizon.getRangeFromPosition());
            addHorizonOffset(plan, horizon.getRangeTo(), horizon.getRangeTo().position);
            addHorizonOffset(plan, horizon.getRangeStep(), horizon.getRangeStepPosition());
        } else {
            for (int i = 0, n = horizon.getListOffsets().size(); i < n; i++) {
                final ExpressionNode offset = horizon.getListOffsets().getQuick(i);
                addHorizonOffset(plan, offset, offset.position);
            }
        }
        final OutputSchema output = plan.getOutput();
        for (int i = 0, n = masterOutput.getColumnCount(); i < n; i++) {
            output.add(masterOutput.getColumnId(i), masterOutput.getColumnName(i), masterOutput.getColumnType(i),
                    masterOutput.getMetadata(i), masterOutput.isVisible(i), masterAlias);
            output.setSymbolTableStatic(i, masterOutput.isSymbolTableStatic(i));
        }
        output.setTimestampIndex(masterOutput.getTimestampIndex());
        copyCorrelatedAliases(masterOutput, output, masterOutput.getColumnCount());
        output.add(ctx.nextColumnId++, "offset", ColumnType.LONG, null, true, horizonAlias);
        output.add(ctx.nextColumnId++, "timestamp", masterOutput.getColumnType(masterOutput.getTimestampIndex()), null, true, horizonAlias);
        for (int i = 0, n = selected.size(); i < n; i++) {
            rejectUnknownHorizonColumn(selected.getQuick(i).getAst(), output, horizonAlias);
        }
        for (int i = 1; i < sourceCount; i++) {
            if (i == offsetIndex) {
                continue;
            }
            final QueryModel occurrence = sources.getQuick(i);
            final CharSequence slaveAlias = sourceAlias(occurrence);
            final LogicalPlan slave = binder.bindSource(model, occurrence, executionContext);
            final HorizonJoinSlave step = horizonJoinSlaves.next().of(slave, slaveAlias, occurrence.getJoinKeywordPosition());
            plan.getSlaves().add(step);
            final OutputSchema slaveOutput = slave.getOutput();
            for (int k = 0, m = slaveOutput.getColumnCount(); k < m; k++) {
                output.add(slaveOutput.getColumnId(k), slaveOutput.getColumnName(k), slaveOutput.getColumnType(k),
                        slaveOutput.getMetadata(k), slaveOutput.isVisible(k), slaveAlias);
                output.setSymbolTableStatic(output.getColumnCount() - 1, slaveOutput.isSymbolTableStatic(k));
            }
            bindHorizonKeys(occurrence.getJoinCriteria(), step, masterOutput, masterAlias, slaveOutput, slaveAlias);
            final ObjList<ExpressionNode> shorthand = occurrence.getJoinColumns();
            for (int k = 0, m = shorthand.size(); k < m; k++) {
                final ExpressionNode column = shorthand.getQuick(k);
                step.getMasterKeyColumnIds().add(masterOutput.getColumnId(ctx.bindColumnIndex(column, masterOutput, masterAlias)));
                step.getSlaveKeyColumnIds().add(slaveOutput.getColumnId(ctx.bindColumnIndex(column, slaveOutput, slaveAlias)));
                step.getKeyPositions().add(column.position);
            }
        }
        return plan;
    }

    WindowJoinPlan bindWindowJoin(
            QueryModel model, ObjList<QueryColumn> aggregateColumns, ExpressionNode aggregates, QueryModel source,
            ExpressionNode where, SqlExecutionContext executionContext
    ) throws SqlException {
        final ObjList<QueryModel> sources = source.getJoinModels();
        final int first = windowJoinIndex(source);
        if (source.getSampleBy() != null) {
            throw SqlException.$(source.getSampleBy().position, "SAMPLE BY cannot be used with WINDOW JOIN");
        }
        for (int i = 0, n = model.getBottomUpColumns().size(); i < n; i++) {
            final QueryColumn column = model.getBottomUpColumns().getQuick(i);
            if (column.isWindowExpression()) {
                throw SqlException.$(column.getAst().position, "WINDOW functions are not allowed in WINDOW JOIN queries");
            }
        }
        final QueryModel masterModel = sources.getQuick(0);
        final CharSequence masterAlias = first > 1 ? null : sourceAlias(masterModel);
        LogicalPlan master;
        if (first > 1) {
            master = joinBinder.bindJoins(model, source, where, first, executionContext);
            where = null;
        } else {
            master = binder.bindSource(model, masterModel, executionContext);
            if (ctx.lateralScopes.size() > 0) {
                master = lateralBinder.correlateSource(model, masterModel, masterAlias, master, where, executionContext);
                where = ctx.correlatedWhere;
            }
        }
        final WindowJoinPlan plan = windowJoinPlans.next().of(master, source.getModelPosition());
        for (int i = first, n = sources.size(); i < n; i++) {
            final QueryModel occurrence = sources.getQuick(i);
            final LogicalPlan slave = binder.bindSource(model, occurrence, executionContext);
            final WindowJoinStep step = windowJoinSteps.next().of(slave, masterAlias, sourceAlias(occurrence),
                    occurrence.getWindowJoinContext().isIncludePrevailing(), occurrence.getJoinKeywordPosition());
            step.setTableSource(occurrence.getNestedModel() == null && occurrence.getTableNameExpr() != null
                    && occurrence.getTableNameExpr().type == ExpressionNode.LITERAL);
            plan.getSteps().add(step);
        }
        final ObjList<PivotForColumn> forColumns = model.getPivotForColumns();
        for (int i = 0, n = forColumns.size(); i < n; i++) {
            rejectWindowJoinSlaveColumn(forColumns.getQuick(i).getInExpr(), master.getOutput(), plan);
        }
        boolean isEmpty = false;
        if (where != null) {
            for (int i = first, n = sources.size(); i < n; i++) {
                validateWindowJoinFilter(where, sourceAlias(sources.getQuick(i)));
            }
            final BoundExpression predicate = binder.bindPredicate(where, master, masterModel, executionContext);
            if (predicate instanceof ConstantExpression constant && constant.getLongValue() == 0) {
                isEmpty = true;
            } else {
                final FilterPlan filter = ctx.filters.next().of(master, predicate, predicate.getPosition());
                filter.getOutput().copyFrom(master.getOutput());
                master = filter;
                plan.replaceInput(0, master);
            }
        }
        plan.setEmpty(isEmpty);
        ctx.promoteNoArgFunctions(model, master.getOutput(), masterAlias);
        final OutputSchema output = plan.getOutput();
        final OutputSchema masterOutput = master.getOutput();
        for (int i = 0, n = masterOutput.getColumnCount(); i < n; i++) {
            output.add(masterOutput.getColumnId(i), masterOutput.getColumnName(i), masterOutput.getColumnType(i),
                    masterOutput.getMetadata(i), masterOutput.isVisible(i), masterAlias == null ? masterOutput.getColumnQualifier(i) : masterAlias);
            output.setSymbolTableStatic(i, masterOutput.isSymbolTableStatic(i));
        }
        output.setTimestampIndex(masterOutput.getTimestampIndex());
        copyCorrelatedAliases(masterOutput, output, masterOutput.getColumnCount());

        ctx.aggregateNodes.clear();
        if (aggregates != null) {
            aggregateBinder.collectAggregateNodes(aggregates, true);
        } else {
            for (int i = 0, n = aggregateColumns.size(); i < n; i++) {
                aggregateBinder.collectAggregateNodes(aggregateColumns.getQuick(i).getAst(), true);
            }
        }
        windowJoinAggregateSteps.clear();
        for (int i = 0, n = ctx.aggregateNodes.size(); i < n; i++) {
            windowJoinAggregateSteps.add(windowJoinStepOf(ctx.aggregateNodes.getQuick(i), plan, masterOutput));
        }
        ctx.aliases.clear();
        ctx.aliasSequences.clear();
        for (int i = 0, n = output.getColumnCount(); i < n; i++) {
            ctx.aliases.add(output.getColumnName(i));
        }
        for (int s = 0, m = plan.getSteps().size(); s < m; s++) {
            final WindowJoinStep step = plan.getSteps().getQuick(s);
            final QueryModel occurrence = sources.getQuick(first + s);
            step.getMasterScope().copyFrom(output);
            final OutputSchema scope = step.getScope();
            scope.copyFrom(output);
            final OutputSchema slaveOutput = step.getSlave().getOutput();
            for (int i = 0, n = slaveOutput.getColumnCount(); i < n; i++) {
                scope.add(slaveOutput.getColumnId(i), slaveOutput.getColumnName(i), slaveOutput.getColumnType(i),
                        slaveOutput.getMetadata(i), slaveOutput.isVisible(i), step.getSlaveAlias());
                scope.setSymbolTableStatic(scope.getColumnCount() - 1, slaveOutput.isSymbolTableStatic(i));
            }
            bindWindowJoinBounds(step, occurrence.getWindowJoinContext(), step.getMasterScope(), executionContext);
            final ExpressionNode criteria = windowJoinCriteria(occurrence, sourceAlias(masterModel), step.getSlaveAlias());
            if (criteria != null) {
                final BoundExpression filter = ctx.functionBinder.bind(criteria, scope, null, executionContext);
                if (filter.getDataType() != ColumnType.BOOLEAN) {
                    throw SqlException.$(criteria.position, "boolean expression expected");
                }
                step.setFilter(filter);
            }
            for (int i = 0, n = ctx.aggregateNodes.size(); i < n; i++) {
                if (windowJoinAggregateSteps.getQuick(i) != s) {
                    continue;
                }
                final ExpressionNode node = ctx.aggregateNodes.getQuick(i);
                final BoundExpression bound = ctx.functionBinder.bindGroupByExpression(node, scope, null, executionContext);
                if (!(bound instanceof FunctionExpression function) || !function.isAggregate()) {
                    throw SqlException.$(node.position, "expected aggregate function");
                }
                final int columnId = ctx.nextColumnId++;
                step.getAggregates().add(function);
                step.getAggregateColumnIds().add(columnId);
                output.add(columnId, ctx.createOutputName(windowJoinAggregateName(node, model)), function.getDataType(), false);
            }
        }
        return plan;
    }

    LogicalPlan bindWindowJoinQuery(
            QueryModel model, QueryModel source, ExpressionNode where, boolean isOrderAndLimitEnabled,
            SqlExecutionContext executionContext
    ) throws SqlException {
        final ObjList<ExpressionNode> groupBy = blockGroupBy(model, source);
        if (groupBy.size() > 0) {
            throw SqlException.$(groupBy.getQuick(0).position, "GROUP BY cannot be used with WINDOW JOIN");
        }
        final WindowJoinPlan plan = bindWindowJoin(model, model.getBottomUpColumns(), null, source, where, executionContext);
        final OutputSchema output = plan.getOutput();
        final CharSequence masterAlias = plan.getSteps().getQuick(0).getMasterAlias();
        final ObjList<ColumnExpression> aggregateColumns = ctx.substitutionColumns;
        final ObjList<ExpressionNode> aggregateSources = ctx.substitutionNodes;
        aggregateColumns.clear();
        aggregateSources.clear();
        for (int i = 0, n = model.getBottomUpColumns().size(); i < n; i++) {
            collectWindowJoinAggregateOccurrences(model.getBottomUpColumns().getQuick(i).getAst(), plan, aggregateSources, aggregateColumns);
        }
        ctx.aliases.clear();
        ctx.aliasSequences.clear();
        ctx.projectionAliasIndexes.clear();
        final ProjectPlan project = ctx.projects.next().of(plan, model.getModelPosition());
        ctx.sourceProjectionIndexes.setAll(output.getColumnCount(), -1);
        for (int i = 0, n = model.getBottomUpColumns().size(); i < n; i++) {
            final QueryColumn column = model.getBottomUpColumns().getQuick(i);
            final ExpressionNode expression = isWildcard(column.getAst()) ? column.getAst() : lateralBinder.substituteLateralCounts(column.getAst(), output);
            if (isWildcard(expression)) {
                for (int s = 0, m = plan.getSteps().size(); s < m; s++) {
                    final WindowJoinStep step = plan.getSteps().getQuick(s);
                    final OutputSchema slaveOutput = step.getSlave().getOutput();
                    for (int k = 0, count = slaveOutput.getColumnCount(); k < count; k++) {
                        if (ctx.isWildcardColumn(expression, slaveOutput, k, step.getSlaveAlias())) {
                            final SqlException e = SqlException.position(expression.position)
                                    .put("WINDOW join cannot reference right table non-aggregate column: ");
                            if (step.getSlaveAlias() != null) {
                                e.put(step.getSlaveAlias()).put('.');
                            }
                            throw e.put(slaveOutput.getColumnName(k));
                        }
                    }
                }
                for (int k = 0, count = output.getColumnCount(); k < count; k++) {
                    if (ctx.isWildcardColumn(expression, output, k, masterAlias)) {
                        ctx.addProjection(project, output, k, output.getColumnName(k), expression.position, true);
                    }
                }
            } else if (expression.type == ExpressionNode.LITERAL) {
                rejectWindowJoinSlaveColumn(expression, output, plan);
                final int resolved = ctx.bindColumnIndex(expression, output, masterAlias);
                ctx.addProjection(project, output, resolved, column.getAlias() != null ? column.getAlias() : output.getColumnName(resolved),
                        expression.position, true);
            } else {
                final BoundExpression bound = ctx.functionBinder.bind(expression, output, masterAlias, aggregateSources, aggregateColumns, executionContext);
                ctx.addProjection(project, bound, null, column.getName(), true);
                ctx.projectionAliasIndexes.add(project.getExpressions().size() - 1);
            }
        }
        aggregateSources.clear();
        aggregateColumns.clear();
        lateralBinder.exposeCorrelated(project, output, null);
        final LogicalPlan result = isOrderAndLimitEnabled && source.getOrderBy().size() > 0
                ? orderBinder.bindOutputOrder(model, plan, project, source, masterAlias, null, executionContext)
                : orderBinder.designateTimestamp(project);
        return isOrderAndLimitEnabled ? orderBinder.bindLimit(result, model, executionContext) : result;
    }

    void collectWindowJoinAggregateOccurrences(ExpressionNode node, WindowJoinPlan plan, ObjList<ExpressionNode> sources,
                                               ObjList<ColumnExpression> targets) {
        if (node == null) {
            return;
        }
        if (ctx.isAggregate(node)) {
            final int aggregate = aggregateBinder.findAggregate(node);
            final WindowJoinStep step = plan.getSteps().getQuick(windowJoinAggregateSteps.getQuick(aggregate));
            int ordinal = 0;
            for (int i = 0; i < aggregate; i++) {
                if (windowJoinAggregateSteps.getQuick(i) == windowJoinAggregateSteps.getQuick(aggregate)) {
                    ordinal++;
                }
            }
            sources.add(node);
            targets.add(ctx.columns.next().of(step.getAggregateColumnIds().getQuick(ordinal), step.getAggregates().getQuick(ordinal).getDataType(), node.position));
            return;
        }
        collectWindowJoinAggregateOccurrences(node.lhs, plan, sources, targets);
        collectWindowJoinAggregateOccurrences(node.rhs, plan, sources, targets);
        for (int i = 0, n = node.args.size(); i < n; i++) {
            collectWindowJoinAggregateOccurrences(node.args.getQuick(i), plan, sources, targets);
        }
    }

    boolean hasHorizonOuterProjection(QueryModel model) {
        for (int i = 0, n = model.getBottomUpColumns().size(); i < n; i++) {
            final ExpressionNode expression = model.getBottomUpColumns().getQuick(i).getAst();
            if (expression.type != ExpressionNode.LITERAL && expression.type != ExpressionNode.BIND_VARIABLE
                    && !ctx.isAggregate(expression) && !(expression.type == ExpressionNode.FUNCTION && ctx.functionFactoryCache.isCursor(expression.token))) {
                return true;
            }
        }
        return false;
    }
}
