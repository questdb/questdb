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
import io.questdb.griffin.model.QueryModel;
import io.questdb.griffin.model.QueryColumn;
import io.questdb.griffin.plan.logical.AggregatePlan;
import io.questdb.griffin.plan.logical.BoundExpression;
import io.questdb.griffin.plan.logical.ColumnExpression;
import io.questdb.griffin.plan.logical.CursorExpression;
import io.questdb.griffin.plan.logical.DistinctPlan;
import io.questdb.griffin.plan.logical.FilterPlan;
import io.questdb.griffin.plan.logical.FunctionExpression;
import io.questdb.griffin.plan.logical.LogicalPlan;
import io.questdb.griffin.plan.logical.OutputSchema;
import io.questdb.griffin.plan.logical.ProjectPlan;
import io.questdb.griffin.plan.logical.SampleByPlan;
import io.questdb.griffin.plan.logical.SortPlan;
import io.questdb.std.Chars;
import io.questdb.std.GenericLexer;
import io.questdb.std.IntList;
import io.questdb.std.Mutable;
import io.questdb.std.Numbers;
import io.questdb.std.NumericException;
import io.questdb.std.ObjList;

import static io.questdb.griffin.BindContext.blockGroupBy;
import static io.questdb.griffin.BindContext.findGroupingColumn;
import static io.questdb.griffin.BindContext.getColumnIndexQuiet;
import static io.questdb.griffin.BindContext.hasComputedProjection;
import static io.questdb.griffin.BindContext.isPlainColumnProjection;
import static io.questdb.griffin.BindContext.isRowCount;
import static io.questdb.griffin.BindContext.isWildcard;
import static io.questdb.griffin.BindContext.orderNotSelected;
import static io.questdb.griffin.BindContext.sourceAlias;
import static io.questdb.griffin.OrderBinder.orderProjectionIndex;

final class AggregateBinder implements Mutable {
    private final ObjList<ExpressionNode> aggregateNodeStack = new ObjList<>();
    private final ObjList<ExpressionNode> aggregateOrderExpressions = new ObjList<>();
    private final IntList aggregateProjectionIndexes = new IntList();
    private final SqlBinder binder;
    private final BindContext ctx;
    private final OrderBinder orderBinder;
    private final IntList orderOutputIndexes;

    AggregateBinder(BindContext ctx, SqlBinder binder, OrderBinder orderBinder) {
        this.ctx = ctx;
        this.binder = binder;
        this.orderBinder = orderBinder;
        this.orderOutputIndexes = orderBinder.orderOutputIndexes;
    }

    @Override
    public void clear() {
        aggregateNodeStack.clear();
        aggregateOrderExpressions.clear();
        aggregateProjectionIndexes.clear();
    }

    private static boolean containsNode(ExpressionNode expression, ExpressionNode node) {
        if (expression == null) {
            return false;
        }
        if (ExpressionNode.compareNodesExact(expression, node)) {
            return true;
        }
        if (containsNode(expression.lhs, node) || containsNode(expression.rhs, node)) {
            return true;
        }
        for (int i = 0, n = expression.args.size(); i < n; i++) {
            if (containsNode(expression.args.getQuick(i), node)) {
                return true;
            }
        }
        return false;
    }

    private static int findAggregateProjection(ProjectPlan project, BoundExpression expression) {
        for (int i = 0, n = project.getExpressions().size(); i < n; i++) {
            final BoundExpression candidate = project.getExpressions().getQuick(i);
            if (candidate == expression || candidate instanceof ColumnExpression left && expression instanceof ColumnExpression right
                    && left.getColumnId() == right.getColumnId()) {
                return i;
            }
        }
        return -1;
    }

    private static boolean hasDirectTableInput(QueryModel source) {
        final ObjList<QueryModel> joinModels = source.getJoinModels();
        for (int i = 0, n = joinModels.size(); i < n; i++) {
            if (joinModels.getQuick(i).getNestedModel() != null) {
                return false;
            }
        }
        return true;
    }

    private static boolean isProjectingAllGroupingKeys(AggregatePlan aggregate, ProjectPlan project) {
        final OutputSchema output = aggregate.getOutput();
        for (int k = 0, n = aggregate.getGroupingExpressions().size(); k < n; k++) {
            final int keyId = output.getColumnId(k);
            boolean isProjected = false;
            for (int i = 0, m = project.getExpressions().size(); i < m && !isProjected; i++) {
                isProjected = project.getExpressions().getQuick(i) instanceof ColumnExpression column && column.getColumnId() == keyId;
            }
            if (!isProjected) {
                return false;
            }
        }
        return true;
    }

    private static boolean isSameColumn(ExpressionNode left, ExpressionNode right, OutputSchema input, QueryModel source) {
        final int index = FunctionBinder.findColumn(left, input, sourceAlias(source));
        return index >= 0 && index == FunctionBinder.findColumn(right, input, sourceAlias(source));
    }

    private static boolean isSelectAlias(ExpressionNode literal, QueryModel model) {
        if (Chars.indexOfLastUnquoted(literal.token, '.') >= 0) {
            return false;
        }
        final CharSequence name = GenericLexer.unquote(literal.token);
        for (int i = 0, n = model.getBottomUpColumns().size(); i < n; i++) {
            if (Chars.equalsIgnoreCase(model.getBottomUpColumns().getQuick(i).getName(), name)) {
                return true;
            }
        }
        return false;
    }

    private static boolean isSelectedExpression(ExpressionNode expression, QueryModel model) {
        for (int i = 0, n = model.getBottomUpColumns().size(); i < n; i++) {
            if (ExpressionNode.compareNodesExact(expression, model.getBottomUpColumns().getQuick(i).getAst())) {
                return true;
            }
        }
        return false;
    }

    private static boolean readsProjectedColumns(BoundExpression expression, ProjectPlan project) {
        if (expression instanceof ColumnExpression column) {
            final int index = project.getOutput().getColumnIndexById(column.getColumnId());
            return index >= 0 && project.getExpressions().getQuick(index) instanceof ColumnExpression;
        }
        if (expression instanceof FunctionExpression call) {
            for (int i = 0, n = call.getArgumentCount(); i < n; i++) {
                if (!readsProjectedColumns(call.argumentAt(i), project)) {
                    return false;
                }
            }
            return true;
        }
        return !(expression instanceof CursorExpression);
    }

    private CharSequence aggregateOrderName(ExpressionNode order, QueryModel source) {
        for (int i = 0, n = source.getOrderBy().size(); i < n; i++) {
            if (aggregateOrderExpressions.getQuick(i) == order) {
                final ExpressionNode original = source.getOrderBy().getQuick(i);
                return original.type == ExpressionNode.LITERAL ? original.token
                        : SqlUtil.createColumnAlias(ctx.characterStore, original.token, Chars.indexOfLastUnquoted(original.token, '.'), ctx.aliases, ctx.aliasSequences, true);
            }
        }
        return order.token;
    }

    private CharSequence aggregateOutputName(ExpressionNode expression, QueryModel model) {
        for (int i = 0, n = model.getBottomUpColumns().size(); i < n; i++) {
            if (expression == ctx.aggregateSelectExpressions.getQuick(i)) {
                return model.getBottomUpColumns().getQuick(i).getName();
            }
        }
        // An aggregate first seen inside an expression keeps its generated name.
        for (int i = 0, n = model.getBottomUpColumns().size(); expression.type != ExpressionNode.LITERAL && !ctx.isAggregate(expression) && i < n; i++) {
            final QueryColumn column = model.getBottomUpColumns().getQuick(i);
            if (ExpressionNode.compareNodesExact(expression, ctx.aggregateSelectExpressions.getQuick(i))) {
                return column.getName();
            }
        }
        if (expression.type == ExpressionNode.LITERAL) {
            final int dot = Chars.indexOfLastUnquoted(expression.token, '.');
            return dot < 0 ? expression.token : expression.token.subSequence(dot + 1, expression.token.length());
        }
        return SqlUtil.createColumnAlias(ctx.characterStore, expression.token, -1, ctx.aliases, ctx.aliasSequences, true);
    }

    private int aggregateSelectedOrderIndex(ExpressionNode order, QueryModel model, QueryModel source, AggregatePlan aggregate, ProjectPlan selected) throws SqlException {
        final int ordinal = orderProjectionIndex(order, selected.getOutput(), selected.getExpressions().size());
        if (ordinal >= 0) {
            return ordinal;
        }
        for (int pass = 0; pass < 2; pass++) {
            int outputIndex = 0;
            for (int i = 0, n = model.getBottomUpColumns().size(); i < n; i++) {
                final ExpressionNode expression = ctx.aggregateSelectExpressions.getQuick(i);
                if (pass == 0 ? ExpressionNode.compareNodesExact(order, expression) : isSameInputColumn(order, expression, source, aggregate)) {
                    return outputIndex;
                }
                outputIndex += isWildcard(expression) ? ctx.wildcardExpansionCount(expression, aggregate.getInput().getOutput(), source) : 1;
            }
        }
        return -1;
    }

    private int aggregateSubstitutionIndex(ExpressionNode expression, QueryModel source, AggregatePlan aggregate) throws SqlException {
        int index = findGroupingExpression(expression, source, aggregate);
        if (index < 0 && ctx.isAggregate(expression)) {
            final int aggregateIndex = findAggregate(expression);
            if (aggregateIndex >= 0) {
                index = aggregate.getGroupingExpressions().size() + aggregateIndex;
            }
        }
        return index;
    }

    private LogicalPlan bindAggregateOrder(
            QueryModel model, QueryModel source, AggregatePlan aggregate, ProjectPlan project, ObjList<ExpressionNode> orders,
            boolean isOrderAndLimitEnabled, SqlExecutionContext executionContext
    ) throws SqlException {
        LogicalPlan result = project;
        final boolean isSingleRow = aggregate.getGroupingExpressions().size() == 0;
        final int visibleCount = project.getExpressions().size();
        if (source.getSampleBy() != null && source.getOrderBy().size() == 0 && project.getInput() instanceof SortPlan sort) {
            int timestampIndex = project.getOutput().getTimestampIndex();
            if (timestampIndex < 0) {
                timestampIndex = binder.sampleByBinder.sampleByTimestampAliasIndex(source, aggregate, project, visibleCount);
            }
            if (timestampIndex < 0) {
                for (int i = 0, n = ctx.aggregateSelectExpressions.size(); i < n; i++) {
                    final ExpressionNode expression = ctx.aggregateSelectExpressions.getQuick(i);
                    if (hasComputedProjection(project) || LogicalPlans.hasRepeatedColumn(project)
                            || binder.sampleByBinder.referencesSampleByTimestamp(expression, aggregate.getInput().getOutput(), source)) {
                        final OutputSchema output = sort.getOutput();
                        final int index = output.getTimestampIndex();
                        ctx.addProjection(project, ctx.columns.next().of(output.getColumnId(index), output.getColumnType(index), expression.position),
                                output.getMetadata(index), output.getColumnName(index), false);
                        timestampIndex = visibleCount;
                        break;
                    }
                }
            }
            if (timestampIndex >= 0) {
                project.replaceInput(0, sort.getInput());
                sort.replaceInput(0, project);
                sort.getColumnIds().setQuick(0, project.getOutput().getColumnId(timestampIndex));
                sort.deriveOutput();
                project.getOutput().setTimestampIndex(-1);
                result = sort;
                if (project.getExpressions().size() > visibleCount) {
                    final ProjectPlan visible = ctx.projects.next().of(sort, project.getPosition());
                    final OutputSchema output = project.getOutput();
                    for (int i = 0; i < visibleCount; i++) {
                        visible.getExpressions().add(ctx.columns.next().of(output.getColumnId(i), output.getColumnType(i), project.getExpressions().getQuick(i).getPosition()));
                        visible.getOutput().add(ctx.nextColumnId++, output.getColumnName(i), output.getColumnType(i), output.getMetadata(i), output.isVisible(i));
                        visible.getOutput().setSymbolTableStatic(i, output.isSymbolTableStatic(i));
                    }
                    result = visible;
                }
            }
        }
        if (isOrderAndLimitEnabled && source.getOrderBy().size() > 0) {
            orderOutputIndexes.clear();
            ProjectPlan ordering = null;
            boolean isProjectExposed = false;
            final SortPlan sort = ctx.sorts.next().of(project, source.getOrderByPosition());
            for (int i = 0, n = source.getOrderBy().size(); i < n; i++) {
                final ExpressionNode order = aggregateOrderExpressions.getQuick(i);
                int index = orderProjectionIndex(order, project.getOutput(), visibleCount);
                if (index < 0) {
                    // Reuse an already selected expression before introducing a
                    // hidden sort value. Aggregate calls remain one occurrence.
                    for (int pass = 0; index < 0 && pass < 2; pass++) {
                        int outputIndex = 0;
                        for (int k = 0, count = model.getBottomUpColumns().size(); k < count; k++) {
                            if (pass == 0 ? ExpressionNode.compareNodesExact(order, ctx.aggregateSelectExpressions.getQuick(k))
                                    : isSameInputColumn(order, ctx.aggregateSelectExpressions.getQuick(k), source, aggregate)) {
                                index = outputIndex;
                                break;
                            }
                            outputIndex += isWildcard(ctx.aggregateSelectExpressions.getQuick(k))
                                    ? ctx.wildcardExpansionCount(ctx.aggregateSelectExpressions.getQuick(k), aggregate.getInput().getOutput(), source) : 1;
                        }
                    }
                }
                for (int k = 0; index < 0 && k < i; k++) {
                    if (ExpressionNode.compareNodesExact(order, aggregateOrderExpressions.getQuick(k))) {
                        index = orderOutputIndexes.getQuick(k);
                    }
                }
                if (index < 0 && ordering == null && !isPlainColumnProjection(project) && order.type != ExpressionNode.LITERAL && !ctx.hasAggregate(order)) {
                    // A computed ORDER BY expression lives in the SELECT's own projection.
                    if (!isProjectExposed) {
                        exposeAggregateOutputs(aggregate, project);
                        isProjectExposed = true;
                    }
                    ctx.substitutionNodes.clear();
                    ctx.substitutionColumns.clear();
                    collectAggregateOrderSubstitutions(order, source, aggregate, project, visibleCount);
                    final BoundExpression expression = ctx.functionBinder.bind(order, project.getOutput(), null,
                            ctx.substitutionNodes, ctx.substitutionColumns, executionContext);
                    if (readsProjectedColumns(expression, project)) {
                        ctx.aliases.clear();
                        ctx.aliasSequences.clear();
                        for (int k = 0, count = project.getOutput().getColumnCount(); k < count; k++) {
                            ctx.aliases.add(project.getOutput().getColumnName(k));
                        }
                        index = project.getExpressions().size();
                        ctx.addProjection(project, ctx.functionBinder.substituteProjection(expression, project), null,
                                SqlUtil.createColumnAlias(ctx.characterStore, order.token, -1, ctx.aliases, ctx.aliasSequences, true), false);
                    }
                }
                if (index < 0) {
                    if (ordering == null) {
                        // Expose aggregate dependencies internally without adding
                        // ordering values to the grouping/equality tuple.
                        exposeAggregateOutputs(aggregate, project);
                        ordering = ctx.projects.next().of(project, source.getOrderByPosition());
                        for (int k = 0; k < visibleCount; k++) {
                            final OutputSchema output = project.getOutput();
                            ordering.getExpressions().add(ctx.columns.next().of(output.getColumnId(k), output.getColumnType(k),
                                    project.getExpressions().getQuick(k).getPosition()));
                            ordering.getOutput().add(output.getColumnId(k), output.getColumnName(k), output.getColumnType(k), output.getMetadata(k), output.isVisible(k));
                            ordering.getOutput().setSymbolTableStatic(ordering.getOutput().getColumnCount() - 1, output.isSymbolTableStatic(k));
                        }
                        if (isSingleRow) {
                            ordering.getOutput().setTimestampIndex(project.getOutput().getTimestampIndex());
                        }
                        ctx.aliases.clear();
                        ctx.aliasSequences.clear();
                        for (int k = 0; k < visibleCount; k++) {
                            ctx.aliases.add(ordering.getOutput().getColumnName(k));
                        }
                    }
                    ctx.substitutionNodes.clear();
                    ctx.substitutionColumns.clear();
                    collectAggregateOrderSubstitutions(order, source, aggregate, project,
                            model.isDistinct() && !hasAggregation(model, source) ? visibleCount : 0);
                    final BoundExpression expression = ctx.functionBinder.bind(order, project.getOutput(), null,
                            ctx.substitutionNodes, ctx.substitutionColumns, executionContext);
                    index = ordering.getExpressions().size();
                    final int projected = expression instanceof ColumnExpression column ? project.getOutput().getColumnIndexById(column.getColumnId()) : -1;
                    // The DISTINCT equality tuple includes a rewritten ORDER BY key, so it stays visible there.
                    ctx.addProjection(ordering, expression, null, projected < 0 ? aggregateOrderName(order, source) : project.getOutput().getColumnName(projected),
                            model.isDistinct() && order != orders.getQuick(i));
                }
                orderOutputIndexes.add(index);
                final OutputSchema output = ordering == null ? project.getOutput() : ordering.getOutput();
                final int type = output.getColumnType(index);
                if (!ColumnType.isComparable(type)) {
                    throw SqlException.$(order.position, ColumnType.nameOf(type)).put(" is not a supported type in ORDER BY clause");
                }
                final int id = output.getColumnId(index);
                if (!sort.getColumnIds().contains(id)) {
                    sort.getColumnIds().add(id);
                    sort.getDirections().add(source.getOrderByDirection().getQuick(i));
                }
            }
            final ProjectPlan sortInput = ordering == null ? project : ordering;
            sort.replaceInput(0, sortInput);
            if (isSingleRow && sortInput.getOutput().getTimestampIndex() >= 0) {
                // Any order of a single row keeps its designated timestamp.
                sort.getOutput().copyFrom(sortInput.getOutput());
            } else {
                sort.deriveOutput();
            }
            result = sort;
            if (visibleCount < sortInput.getExpressions().size()) {
                final ProjectPlan visible = ctx.projects.next().of(result, project.getPosition());
                final OutputSchema output = sortInput.getOutput();
                for (int i = 0, n = output.getColumnCount(); i < n; i++) {
                    if (output.isVisible(i)) {
                        visible.getExpressions().add(ctx.columns.next().of(output.getColumnId(i), output.getColumnType(i), sortInput.getExpressions().getQuick(i).getPosition()));
                        visible.getOutput().add(ctx.nextColumnId++, output.getColumnName(i), output.getColumnType(i), output.getMetadata(i), true);
                        visible.getOutput().setSymbolTableStatic(visible.getOutput().getColumnCount() - 1, output.isSymbolTableStatic(i));
                        if (i == result.getOutput().getTimestampIndex()) {
                            visible.getOutput().setTimestampIndex(visible.getExpressions().size() - 1);
                        }
                    }
                }
                result = visible;
            }
        }
        return isOrderAndLimitEnabled ? orderBinder.bindLimit(result, model, executionContext) : result;
    }

    private LogicalPlan bindDistinctAggregation(
            QueryModel model, QueryModel source, AggregatePlan aggregate, ProjectPlan selected,
            boolean isOrderAndLimitEnabled, SqlExecutionContext executionContext
    ) throws SqlException {
        final boolean hasOuterProjection = needsDistinctAggregateProjection(model, source, aggregate, selected, isOrderAndLimitEnabled);
        final int selectedCount = selected.getExpressions().size();
        final ProjectPlan tuple;
        if (hasOuterProjection) {
            tuple = selected;
        } else {
            tuple = ctx.projects.next().of(aggregate, selected.getPosition());
            ctx.aliases.clear();
            ctx.aliasSequences.clear();
            final OutputSchema output = aggregate.getOutput();
            for (int i = 0, n = output.getColumnCount(); i < n; i++) {
                final CharSequence name = i < aggregate.getGroupingExpressions().size()
                        ? distinctGroupingName(i, model, source, aggregate) : output.getColumnName(i);
                ctx.addProjection(tuple, ctx.columns.next().of(output.getColumnId(i), output.getColumnType(i), aggregate.getPosition()),
                        output.getMetadata(i), name, true);
            }
        }
        final ProjectPlan visible = ctx.projects.next().of(tuple, selected.getPosition());
        for (int i = 0; i < selectedCount; i++) {
            final int index = tuple == selected ? i : findAggregateProjection(tuple, selected.getExpressions().getQuick(i));
            if (index < 0) {
                throw new IllegalStateException("DISTINCT aggregate projection is outside its tuple");
            }
            final OutputSchema output = tuple.getOutput();
            visible.getExpressions().add(ctx.columns.next().of(output.getColumnId(index), output.getColumnType(index),
                    selected.getExpressions().getQuick(i).getPosition()));
            visible.getOutput().add(ctx.nextColumnId++, selected.getOutput().getColumnName(i), output.getColumnType(index), output.getMetadata(index), true);
            visible.getOutput().setSymbolTableStatic(visible.getOutput().getColumnCount() - 1, output.isSymbolTableStatic(index));
        }
        orderOutputIndexes.clear();
        boolean hasOrderWrapper = false;
        if (isOrderAndLimitEnabled) {
            for (int i = 0, n = source.getOrderBy().size(); i < n; i++) {
                final ExpressionNode order = resolveDistinctOrderOrdinal(aggregateOrderExpressions.getQuick(i), model, source,
                        aggregate, selected, selectedCount);
                final int selectedIndex = aggregateSelectedOrderIndex(order, model, source, aggregate, selected);
                int index = selectedIndex < 0 ? -1 : tuple == selected ? selectedIndex
                                                     : findAggregateProjection(tuple, selected.getExpressions().getQuick(selectedIndex));
                if (index < 0) {
                    for (int k = 0; k < i; k++) {
                        if (ExpressionNode.compareNodesExact(order, aggregateOrderExpressions.getQuick(k))) {
                            index = orderOutputIndexes.getQuick(k);
                            break;
                        }
                    }
                }
                if (index < 0) {
                    final BoundExpression expression = bindAggregateOutput(order, source, aggregate, executionContext);
                    index = findAggregateProjection(tuple, expression);
                    // An ORDER reference to a selected source column needs no
                    // wildcard wrapper, even when its qualification differs.
                    final int projected = findAggregateProjection(selected, expression);
                    if (projected < 0) {
                        hasOrderWrapper = true;
                    }
                    if (index < 0) {
                        final CharSequence orderName = aggregateOrderName(order, source);
                        final CharSequence name = SqlUtil.createColumnAlias(ctx.characterStore, orderName,
                                Chars.indexOfLastUnquoted(orderName, '.'), ctx.aliases, ctx.aliasSequences, true);
                        index = tuple.getExpressions().size();
                        ctx.addProjection(tuple, expression, null, name, false);
                    }
                }
                final int type = tuple.getOutput().getColumnType(index);
                if (!ColumnType.isComparable(type)) {
                    throw SqlException.$(order.position, ColumnType.nameOf(type)).put(" is not a supported type in ORDER BY clause");
                }
                orderOutputIndexes.add(index);
            }
        }
        final DistinctPlan distinct = ctx.distincts.next().of(tuple, model.getModelPosition());
        distinct.getOutput().copyFrom(tuple.getOutput());
        ctx.stopTimestampIntrinsics(distinct.getOutput());
        LogicalPlan result = distinct;
        if (orderOutputIndexes.size() > 0) {
            final SortPlan sort = ctx.sorts.next().of(distinct, source.getOrderByPosition());
            for (int i = 0, n = orderOutputIndexes.size(); i < n; i++) {
                final int id = distinct.getOutput().getColumnId(orderOutputIndexes.getQuick(i));
                if (!sort.getColumnIds().contains(id)) {
                    sort.getColumnIds().add(id);
                    sort.getDirections().add(source.getOrderByDirection().getQuick(i));
                }
            }
            sort.deriveOutput();
            result = sort;
        }
        if (hasOrderWrapper) {
            visible.replaceInput(0, result);
            final int timestampId = result.getOutput().getTimestampColumnId();
            for (int i = 0, n = visible.getExpressions().size(); i < n; i++) {
                if (((ColumnExpression) visible.getExpressions().getQuick(i)).getColumnId() == timestampId) {
                    visible.getOutput().setTimestampIndex(i);
                    break;
                }
            }
            result = visible;
        }
        return isOrderAndLimitEnabled ? orderBinder.bindLimit(result, model, executionContext) : result;
    }

    // Checks both operands of a node before it descends into either, lhs subtree first.
    private void collectAggregateChildSubstitutions(ExpressionNode expression, QueryModel source, AggregatePlan aggregate) throws SqlException {
        if (expression.paramCount < 3) {
            final boolean isRhsPending = expression.rhs != null && !substituteAggregateNode(expression.rhs, source, aggregate);
            if (expression.lhs != null && !substituteAggregateNode(expression.lhs, source, aggregate)) {
                collectAggregateChildSubstitutions(expression.lhs, source, aggregate);
            }
            if (isRhsPending) {
                collectAggregateChildSubstitutions(expression.rhs, source, aggregate);
            }
        } else {
            final ObjList<ExpressionNode> args = expression.args;
            for (int i = 1, n = args.size(); i < n; i++) {
                substituteAggregateNode(args.getQuick(i), source, aggregate);
            }
            if (!substituteAggregateNode(args.getQuick(0), source, aggregate)) {
                collectAggregateChildSubstitutions(args.getQuick(0), source, aggregate);
            }
            for (int i = args.size() - 1; i > 0; i--) {
                final ExpressionNode arg = args.getQuick(i);
                if (aggregateSubstitutionIndex(arg, source, aggregate) < 0) {
                    collectAggregateChildSubstitutions(arg, source, aggregate);
                }
            }
        }
    }

    private boolean collectAggregateNode(ExpressionNode expression, boolean isDeduplicationAllowed, boolean isNested) {
        if (!ctx.isAggregate(expression)) {
            return false;
        }
        // Without deduplication, an aggregate that also occurs inside an expression still shares its slot.
        if (findAggregate(expression) < 0 || !isDeduplicationAllowed && !isNested && !hasNestedAggregate(expression)) {
            ctx.aggregateNodes.add(expression);
        }
        return true;
    }

    private void collectAggregateOrderSubstitutions(
            ExpressionNode expression, QueryModel source, AggregatePlan aggregate,
            ProjectPlan project, int visibleCount
    ) throws SqlException {
        if (expression == null) {
            return;
        }
        if (expression.type == ExpressionNode.LITERAL
                && getColumnIndexQuiet(aggregate.getInput().getOutput(), expression.token) < 0
                && orderProjectionIndex(expression, project.getOutput(), visibleCount) >= 0) {
            return;
        }
        int index = findGroupingExpression(expression, source, aggregate);
        if (index < 0 && ctx.isAggregate(expression)) {
            final int aggregateIndex = findAggregate(expression);
            if (aggregateIndex >= 0) {
                index = aggregate.getGroupingExpressions().size() + aggregateIndex;
            }
        }
        if (index >= 0) {
            final OutputSchema output = project.getOutput();
            final int projectedIndex = aggregateProjectionIndexes.getQuick(index);
            ctx.substitutionNodes.add(expression);
            ctx.substitutionColumns.add(ctx.columns.next().of(output.getColumnId(projectedIndex), output.getColumnType(projectedIndex), expression.position));
            return;
        }
        if (expression.type == ExpressionNode.LITERAL) {
            throw orderNotSelected(expression);
        }
        if (expression.paramCount < 3) {
            collectAggregateOrderSubstitutions(expression.lhs, source, aggregate, project, visibleCount);
            collectAggregateOrderSubstitutions(expression.rhs, source, aggregate, project, visibleCount);
        } else {
            for (int i = 0, n = expression.args.size(); i < n; i++) {
                collectAggregateOrderSubstitutions(expression.args.getQuick(i), source, aggregate, project, visibleCount);
            }
        }
    }

    private void collectAggregateSubstitutions(ExpressionNode expression, QueryModel source, AggregatePlan aggregate) throws SqlException {
        if (expression != null && !substituteAggregateNode(expression, source, aggregate)) {
            collectAggregateChildSubstitutions(expression, source, aggregate);
        }
    }

    private CharSequence distinctGroupingName(int index, QueryModel model, QueryModel source, AggregatePlan aggregate) throws SqlException {
        final ObjList<ExpressionNode> groupBy = blockGroupBy(model, source);
        for (int i = 0, n = groupBy.size(); i < n; i++) {
            final ExpressionNode group = groupBy.getQuick(i);
            if (findGroupingExpression(resolveGroupBy(group, model), source, aggregate) == index) {
                if (group.type == ExpressionNode.LITERAL) {
                    final int dot = Chars.indexOfLastUnquoted(group.token, '.');
                    return GenericLexer.unquote(dot < 0 ? group.token : group.token.subSequence(dot + 1, group.token.length()));
                }
                if (group.type != ExpressionNode.CONSTANT || Numbers.parseIntQuiet(group.token) == Numbers.INT_NULL) {
                    return SqlUtil.createColumnAlias(ctx.characterStore, group.token, -1, ctx.aliases, ctx.aliasSequences, true);
                }
                return aggregate.getOutput().getColumnName(index);
            }
        }
        return aggregate.getOutput().getColumnName(index);
    }

    private boolean equalGroupingExpressions(ExpressionNode left, ExpressionNode right, QueryModel source, OutputSchema input) throws SqlException {
        if (left == right) {
            return true;
        }
        if (left == null || right == null || left.type != right.type || left.paramCount != right.paramCount) {
            return false;
        }
        if (left.type == ExpressionNode.LITERAL) {
            final boolean isLeftOuter = ctx.functionBinder.isOuterColumn(left, input, sourceAlias(source));
            final boolean isRightOuter = ctx.functionBinder.isOuterColumn(right, input, sourceAlias(source));
            if (isLeftOuter || isRightOuter) {
                return isLeftOuter && isRightOuter && Chars.equalsIgnoreCase(left.token, right.token);
            }
            return isSameColumn(left, right, input, source);
        }
        if (left.type == ExpressionNode.CONSTANT ? !Chars.equals(left.token, right.token) : !Chars.equalsIgnoreCase(left.token, right.token)) {
            return false;
        }
        if (left.paramCount < 3) {
            return equalGroupingExpressions(left.lhs, right.lhs, source, input) && equalGroupingExpressions(left.rhs, right.rhs, source, input);
        }
        for (int i = 0; i < left.paramCount; i++) {
            if (!equalGroupingExpressions(left.args.getQuick(i), right.args.getQuick(i), source, input)) {
                return false;
            }
        }
        return true;
    }

    private CharSequence explicitGroupingName(ExpressionNode group, ExpressionNode expression, QueryModel model) {
        if (expression != group && (group.type != ExpressionNode.LITERAL || group.token.charAt(0) != '"'
                && (expression.type != ExpressionNode.LITERAL || !Chars.equals(group.token, expression.token)))) {
            for (int i = 0, n = model.getBottomUpColumns().size(); i < n; i++) {
                if (model.getBottomUpColumns().getQuick(i).getAst() == expression) {
                    return model.getBottomUpColumns().getQuick(i).getName();
                }
            }
        }
        if (expression.type == ExpressionNode.LITERAL) {
            final int dot = Chars.indexOfLastUnquoted(expression.token, '.');
            return dot < 0 ? expression.token : expression.token.subSequence(dot + 1, expression.token.length());
        }
        return SqlUtil.createColumnAlias(ctx.characterStore, expression.token, -1, ctx.aliases, ctx.aliasSequences, true);
    }

    private void exposeAggregateOutputs(AggregatePlan aggregate, ProjectPlan project) {
        final OutputSchema input = aggregate.getOutput();
        aggregateProjectionIndexes.setAll(input.getColumnCount(), -1);
        for (int i = 0, n = project.getExpressions().size(); i < n; i++) {
            if (project.getExpressions().getQuick(i) instanceof ColumnExpression column) {
                aggregateProjectionIndexes.setQuick(input.getColumnIndexById(column.getColumnId()), i);
            }
        }
        for (int i = 0, n = input.getColumnCount(); i < n; i++) {
            if (aggregateProjectionIndexes.getQuick(i) < 0) {
                aggregateProjectionIndexes.setQuick(i, project.getExpressions().size());
                ctx.addProjection(project, ctx.columns.next().of(input.getColumnId(i), input.getColumnType(i), aggregate.getPosition()),
                        input.getMetadata(i), input.getColumnName(i), false);
            }
        }
    }

    private ExpressionNode findGroupByViolation(ExpressionNode expression) {
        if (expression == null) {
            return null;
        }
        if (expression.type == ExpressionNode.FUNCTION
                && (ctx.functionFactoryCache.isGroupBy(expression.token) || ctx.functionFactoryCache.isWindow(expression.token))) {
            return expression;
        }
        ExpressionNode violation = findGroupByViolation(expression.rhs);
        if (violation == null) {
            violation = findGroupByViolation(expression.lhs);
        }
        for (int i = expression.args.size() - 1; violation == null && i > -1; i--) {
            violation = findGroupByViolation(expression.args.getQuick(i));
        }
        return violation;
    }

    private boolean hasAggregateComputation() {
        for (int i = 0, n = ctx.aggregateSelectExpressions.size(); i < n; i++) {
            final ExpressionNode expression = ctx.aggregateSelectExpressions.getQuick(i);
            if (!ctx.isAggregate(expression) && ctx.hasAggregate(expression)) {
                return true;
            }
        }
        return false;
    }

    private boolean hasNestedAggregate(ExpressionNode aggregate) {
        for (int i = 0, n = ctx.aggregateSelectExpressions.size(); i < n; i++) {
            final ExpressionNode expression = ctx.aggregateSelectExpressions.getQuick(i);
            if (!ctx.isAggregate(expression) && containsNode(expression, aggregate)) {
                return true;
            }
        }
        return false;
    }

    private boolean hasTimestampComputation(OutputSchema input, QueryModel source) throws SqlException {
        for (int i = 0, n = ctx.aggregateSelectExpressions.size(); i < n; i++) {
            final ExpressionNode expression = ctx.aggregateSelectExpressions.getQuick(i);
            if ((expression.type == ExpressionNode.FUNCTION || expression.type == ExpressionNode.OPERATION)
                    && binder.sampleByBinder.referencesSampleByTimestamp(expression, input, source)) {
                return true;
            }
        }
        return false;
    }

    private boolean isAggregateOrderAvailable(
            ExpressionNode expression, QueryModel model, QueryModel source, AggregatePlan aggregate, boolean hasSelectAliases
    ) throws SqlException {
        if (expression == null || ctx.isAggregate(expression)) {
            return true;
        }
        if (hasSelectAliases && expression.type == ExpressionNode.LITERAL
                && getColumnIndexQuiet(aggregate.getInput().getOutput(), expression.token) < 0) {
            for (int i = 0, n = model.getBottomUpColumns().size(); i < n; i++) {
                if (Chars.equalsIgnoreCase(model.getBottomUpColumns().getQuick(i).getName(), expression.token)) {
                    return true;
                }
            }
        }
        if (findGroupingExpression(expression, source, aggregate) >= 0) {
            return true;
        }
        if (expression.type == ExpressionNode.LITERAL) {
            return false;
        }
        if (expression.paramCount < 3) {
            return isAggregateOrderAvailable(expression.lhs, model, source, aggregate, hasSelectAliases)
                    && isAggregateOrderAvailable(expression.rhs, model, source, aggregate, hasSelectAliases);
        }
        for (int i = 0, n = expression.args.size(); i < n; i++) {
            if (!isAggregateOrderAvailable(expression.args.getQuick(i), model, source, aggregate, hasSelectAliases)) {
                return false;
            }
        }
        return true;
    }

    private boolean isConstantGroupingExpression(ExpressionNode expression) {
        if (expression == null) {
            return true;
        }
        if (expression.type != ExpressionNode.OPERATION && expression.type != ExpressionNode.CONSTANT
                && expression.type != ExpressionNode.BIND_VARIABLE
                && !(expression.type == ExpressionNode.FUNCTION
                && (ctx.functionFactoryCache.isRuntimeConstant(expression.token) || SqlKeywords.isCastKeyword(expression.token)))) {
            return false;
        }
        if (!isConstantGroupingExpression(expression.lhs) || !isConstantGroupingExpression(expression.rhs)) {
            return false;
        }
        for (int i = 0, n = expression.args.size(); i < n; i++) {
            if (!isConstantGroupingExpression(expression.args.getQuick(i))) {
                return false;
            }
        }
        return true;
    }

    /**
     * The selected tuple determines the groups: every grouping key, including the SAMPLE BY bucket, is selected.
     */
    private boolean isDistinctGroupingSelected(QueryModel model, QueryModel source, OutputSchema input) throws SqlException {
        if (source.getSampleBy() != null) {
            final int timestampIndex = input.getTimestampIndex();
            for (int i = 0, n = model.getBottomUpColumns().size(); i < n && timestampIndex >= 0; i++) {
                final ExpressionNode column = model.getBottomUpColumns().getQuick(i).getAst();
                if (column.type == ExpressionNode.LITERAL && FunctionBinder.findColumn(column, input, sourceAlias(source)) == timestampIndex) {
                    return true;
                }
            }
            return false;
        }
        final ObjList<ExpressionNode> groupBy = blockGroupBy(model, source);
        for (int i = 0, n = groupBy.size(); i < n; i++) {
            final ExpressionNode group = resolveGroupBy(groupBy.getQuick(i), model);
            if (!isSelectedExpression(group, model) && (group.type != ExpressionNode.LITERAL || !isSelectedColumn(group, model, source, input))) {
                return false;
            }
        }
        return true;
    }

    /**
     * The aggregate is a sum the optimiser computes above the aggregate, see
     * {@link LogicalPlans#normalisableSumOperation}, so it is a computed value of the DISTINCT tuple.
     */
    private boolean isNormalisedSum(ExpressionNode expression, AggregatePlan aggregate) {
        final int index = findAggregate(expression);
        return index >= 0 && LogicalPlans.normalisableSumOperation(aggregate, aggregate.getAggregates().getQuick(index)) != null;
    }

    private boolean isSameInputColumn(ExpressionNode left, ExpressionNode right, QueryModel source, AggregatePlan aggregate) {
        if (left.type != ExpressionNode.LITERAL || right.type != ExpressionNode.LITERAL || isWildcard(left) || isWildcard(right)) {
            return false;
        }
        final OutputSchema input = aggregate.getInput().getType() == LogicalPlan.Type.WINDOW
                ? ctx.windowBindingScope(aggregate.getInput().getOutput()) : aggregate.getInput().getOutput();
        return isSameColumn(left, right, input, source);
    }

    /**
     * The literal does not name a source column the select list leaves out; a select alias or an
     * unknown name passes, and binding reports the unknown one.
     */
    private boolean isSelectedColumn(ExpressionNode literal, QueryModel model, QueryModel source, OutputSchema input) {
        final int index = FunctionBinder.findColumn(literal, input, sourceAlias(source));
        if (index < 0) {
            return true;
        }
        for (int i = 0, n = model.getBottomUpColumns().size(); i < n; i++) {
            final ExpressionNode column = model.getBottomUpColumns().getQuick(i).getAst();
            if (isWildcard(column) || column.type == ExpressionNode.LITERAL && FunctionBinder.findColumn(column, input, sourceAlias(source)) == index) {
                return true;
            }
        }
        return false;
    }

    private boolean needsDistinctAggregateProjection(
            QueryModel model, QueryModel source, AggregatePlan aggregate, ProjectPlan selected, boolean isOrderEnabled
    ) throws SqlException {
        final ObjList<ExpressionNode> groupBy = blockGroupBy(model, source);
        if (groupBy.size() == 0 || !isProjectingAllGroupingKeys(aggregate, selected)) {
            return true;
        }
        for (int i = 0, n = model.getBottomUpColumns().size(); i < n; i++) {
            final QueryColumn column = model.getBottomUpColumns().getQuick(i);
            final ExpressionNode expression = ctx.aggregateSelectExpressions.getQuick(i);
            if (ctx.isAggregate(expression)) {
                if (isNormalisedSum(expression, aggregate)) {
                    return true;
                }
                for (int k = 0; k < i; k++) {
                    if (ExpressionNode.compareNodesExact(expression, ctx.aggregateSelectExpressions.getQuick(k))) {
                        return true;
                    }
                }
            } else if (expression.type == ExpressionNode.LITERAL && !isWildcard(expression)) {
                final int keyIndex = findGroupingExpression(expression, source, aggregate);
                if (keyIndex != i) {
                    return true;
                }
                for (int k = 0, count = groupBy.size(); k < count; k++) {
                    final ExpressionNode group = groupBy.getQuick(k);
                    if (findGroupingExpression(resolveGroupBy(group, model), source, aggregate) == keyIndex) {
                        if (group.type == ExpressionNode.LITERAL
                                && !Chars.equalsIgnoreCase(distinctGroupingName(keyIndex, model, source, aggregate), GenericLexer.unquote(column.getName()))) {
                            return true;
                        }
                        break;
                    }
                }
            } else {
                return true;
            }
        }
        if (isOrderEnabled) {
            for (int i = 0, n = source.getOrderBy().size(); i < n; i++) {
                final ExpressionNode order = aggregateOrderExpressions.getQuick(i);
                if ((order.type == ExpressionNode.FUNCTION || order.type == ExpressionNode.OPERATION)
                        && (!ctx.isAggregate(order) || isNormalisedSum(order, aggregate))
                        && aggregateSelectedOrderIndex(order, model, source, aggregate, selected) < 0) {
                    return true;
                }
            }
        }
        return false;
    }

    private ExpressionNode resolveDistinctOrderOrdinal(
            ExpressionNode order, QueryModel model, QueryModel source, AggregatePlan aggregate,
            ProjectPlan selected, int selectedCount
    ) throws SqlException {
        if (order.type != ExpressionNode.CONSTANT) {
            return order;
        }
        final int ordinal;
        try {
            ordinal = Numbers.parseInt(order.token);
        } catch (NumericException e) {
            return order;
        }
        if (ordinal > 0 && ordinal <= selectedCount || getColumnIndexQuiet(selected.getOutput(), order.token) >= 0) {
            return order;
        }
        // The parser appends ORDER expressions before the ordinal pass,
        // including expressions occurring after this ordinal.
        int count = selectedCount;
        for (int i = 0, n = source.getOrderBy().size(); i < n; i++) {
            final ExpressionNode candidate = aggregateOrderExpressions.getQuick(i);
            if ((candidate.type != ExpressionNode.FUNCTION && candidate.type != ExpressionNode.OPERATION)
                    || aggregateSelectedOrderIndex(candidate, model, source, aggregate, selected) >= 0) {
                continue;
            }
            boolean duplicate = false;
            for (int k = 0; k < i; k++) {
                if (ExpressionNode.compareNodesExact(candidate, aggregateOrderExpressions.getQuick(k))) {
                    duplicate = true;
                    break;
                }
            }
            if (!duplicate && ++count == ordinal) {
                return candidate;
            }
        }
        throw SqlException.$(order.position, "order column position is out of range [max=").put(count).put(']');
    }

    private ExpressionNode resolveGroupBy(ExpressionNode group, QueryModel model) throws SqlException {
        if (group.type == ExpressionNode.CONSTANT) {
            try {
                final int ordinal = Numbers.parseInt(group.token);
                if (ordinal < 1 || ordinal > model.getBottomUpColumns().size()) {
                    throw SqlException.$(group.position, "GROUP BY position ").put(ordinal).put(" is not in select list");
                }
                return model.getBottomUpColumns().getQuick(ordinal - 1).getAst();
            } catch (NumericException ignored) {
                return group;
            }
        }
        if (group.type == ExpressionNode.LITERAL && Chars.indexOfLastUnquoted(group.token, '.') < 0) {
            for (int i = 0, n = model.getBottomUpColumns().size(); i < n; i++) {
                final QueryColumn column = model.getBottomUpColumns().getQuick(i);
                if (Chars.equalsIgnoreCase(column.getAlias(), GenericLexer.unquote(group.token))) {
                    return column.getAst();
                }
            }
        }
        return group;
    }

    private boolean rewriteCountDistinct(
            QueryModel model, QueryModel source, AggregatePlan aggregate, SqlExecutionContext executionContext
    ) throws SqlException {
        if (source.getNestedModel() != null || LogicalPlans.hasOuterColumn(aggregate.getInput(), ctx.outerColumnScratch)
                || source.getTableName() == null || source.getJoinModels().size() != 1
                || model.getJoinModels().size() != 1 || model.getWhereClause() != null
                || model.getSampleBy() != null || source.getSampleBy() != null
                || aggregate.getGroupingExpressions().size() != 0 || ctx.aggregateNodes.size() != 1) {
            return false;
        }
        final ExpressionNode expression = ctx.aggregateNodes.getQuick(0);
        if (!Chars.equalsIgnoreCase(expression.token, "count_distinct") || expression.paramCount != 1
                || expression.rhs == null || expression.rhs.type == ExpressionNode.CONSTANT) {
            return false;
        }
        final ExpressionNode argument = expression.rhs;
        if (argument.type == ExpressionNode.ARRAY_CONSTRUCTOR) {
            throw SqlException.$(argument.position, "unsupported type of expression");
        }
        final LogicalPlan input = aggregate.getInput();
        final LogicalPlan filterInput = input.getType() == LogicalPlan.Type.LATEST_BY ? input.inputAt(0) : input;
        if (argument.type == ExpressionNode.LITERAL
                && ColumnType.isSymbol(filterInput.getOutput().getColumnType(ctx.bindColumnIndex(argument, filterInput.getOutput(), source)))) {
            return false;
        }

        final OperatorExpression neq = OperatorExpression.chooseRegistry(ctx.configuration.getCairoSqlLegacyOperatorPrecedence())
                .getOperatorDefinition("!=");
        final ExpressionNode notNull = ctx.bindingExpressions.next().of(ExpressionNode.OPERATION, neq.operator.token, neq.precedence, 0);
        notNull.paramCount = 2;
        notNull.lhs = ctx.bindingExpressions.next().of(ExpressionNode.CONSTANT, "null", 0, 0);
        notNull.rhs = argument;
        BoundExpression predicate = binder.bindPredicate(notNull, filterInput, source, executionContext);
        LogicalPlan filtered = filterInput;
        if (filtered instanceof FilterPlan previous) {
            predicate = ctx.functionBinder.combineConjunction(previous.getPredicate(), predicate, 0);
            filtered = previous.getInput();
        }
        final FilterPlan filter = ctx.filters.next().of(filtered, predicate, 0);
        filter.getOutput().copyFrom(filtered.getOutput());
        if (input.getType() == LogicalPlan.Type.LATEST_BY) {
            input.replaceInput(0, filter);
            filtered = input;
        } else {
            filtered = filter;
        }

        final BoundExpression key = ctx.functionBinder.bind(argument, filtered.getOutput(), sourceAlias(source), executionContext);
        final AggregatePlan distinct = ctx.aggregates.next().of(filtered, expression.position);
        distinct.getGroupingExpressions().add(key);
        ctx.aliases.clear();
        ctx.aliasSequences.clear();
        final CharSequence name = SqlUtil.createColumnAlias(ctx.characterStore, argument.token,
                Chars.indexOfLastUnquoted(argument.token, '.'), ctx.aliases, ctx.aliasSequences, true);
        distinct.getOutput().add(ctx.nextColumnId++, ctx.createOutputName(name), key.getDataType(), false);
        aggregate.replaceInput(0, distinct);
        ctx.aliases.clear();
        ctx.aliasSequences.clear();
        return true;
    }

    private boolean substituteAggregateNode(ExpressionNode expression, QueryModel source, AggregatePlan aggregate) throws SqlException {
        final int index = aggregateSubstitutionIndex(expression, source, aggregate);
        if (index >= 0) {
            final OutputSchema output = aggregate.getOutput();
            ctx.substitutionNodes.add(expression);
            ctx.substitutionColumns.add(ctx.columns.next().of(output.getColumnId(index), output.getColumnType(index), expression.position,
                    expression.type == ExpressionNode.LITERAL));
            return true;
        }
        if (expression.type == ExpressionNode.LITERAL) {
            throw SqlException.$(expression.position, "column must appear in GROUP BY clause or aggregate function");
        }
        return false;
    }

    private void validateDistinctOrder(
            ExpressionNode node, QueryModel model, QueryModel source, OutputSchema input, boolean isGroupingSelected
    ) throws SqlException {
        if (node == null || isSelectedExpression(node, model)) {
            return;
        }
        if (node.type == ExpressionNode.LITERAL) {
            if (!isSelectedColumn(node, model, source, input)) {
                throw orderNotSelected(node);
            }
            return;
        }
        if (node.type != ExpressionNode.FUNCTION && node.type != ExpressionNode.OPERATION) {
            return;
        }
        if (ctx.isAggregate(node)) {
            if (!isGroupingSelected) {
                throw orderNotSelected(node);
            }
            return;
        }
        if (node.paramCount < 3) {
            validateDistinctOrder(node.lhs, model, source, input, isGroupingSelected);
            validateDistinctOrder(node.rhs, model, source, input, isGroupingSelected);
        } else {
            for (int i = 0, n = node.args.size(); i < n; i++) {
                validateDistinctOrder(node.args.getQuick(i), model, source, input, isGroupingSelected);
            }
        }
    }

    static boolean hasAggregateReference(ExpressionNode node, QueryModel model) {
        if (node.type == ExpressionNode.LITERAL) {
            final int dot = Chars.indexOfLastUnquoted(node.token, '.');
            return dot < 0 && model.getAliasToColumnMap().contains(node.token);
        }
        return hasAggregateReference(node.lhs, model) || hasAggregateReference(node.rhs, model);
    }

    void addAggregateInputColumns(BoundExpression expression, ProjectPlan project) {
        if (expression instanceof ColumnExpression column) {
            if (project.getOutput().getColumnIndexById(column.getColumnId()) < 0) {
                final OutputSchema input = project.getInput().getOutput();
                final int index = input.getColumnIndexById(column.getColumnId());
                final ExpressionNode spelling = ctx.columnSpellings.get(column.getColumnId());
                final CharSequence name;
                if (spelling == null) {
                    name = input.getColumnName(index);
                } else {
                    final CharacterStoreEntry entry = ctx.characterStore.newEntry();
                    entry.put(spelling.token, Chars.indexOfLastUnquoted(spelling.token, '.') + 1, spelling.token.length());
                    name = entry.toImmutable();
                }
                project.getExpressions().add(column);
                project.getOutput().add(column.getColumnId(), ctx.createOutputName(name), column.getDataType(),
                        input.getMetadata(index), input.isVisible(index), input.getColumnQualifier(index));
                final int outputIndex = project.getOutput().getColumnCount() - 1;
                project.getOutput().setSymbolTableStatic(outputIndex, input.isSymbolTableStatic(index));
                if (index == input.getTimestampIndex()) {
                    project.getOutput().setTimestampIndex(outputIndex);
                }
            }
        } else if (expression instanceof FunctionExpression call) {
            for (int i = 0, n = call.getArgumentCount(); i < n; i++) {
                addAggregateInputColumns(call.argumentAt(i), project);
            }
        }
    }

    void addGroupingExpression(ExpressionNode expression, QueryModel model, QueryModel source, AggregatePlan aggregate, SqlExecutionContext context) throws SqlException {
        addGroupingExpression(expression, null, false, model, source, aggregate, context);
    }

    void addGroupingExpression(
            ExpressionNode expression, CharSequence requestedName, boolean isDuplicateAllowed,
            QueryModel model, QueryModel source, AggregatePlan aggregate, SqlExecutionContext context
    ) throws SqlException {
        final OutputSchema input = aggregate.getInput().getOutput();
        if (isWildcard(expression)) {
            ctx.validateWildcard(expression, input, source);
            for (int i = 0, n = input.getColumnCount(); i < n; i++) {
                if (!ctx.isWildcardColumn(expression, input, i, sourceAlias(source))) {
                    continue;
                }
                if (findGroupingColumn(aggregate, input.getColumnId(i)) < 0) {
                    ctx.groupingNodes.add(null);
                    aggregate.getGroupingExpressions().add(ctx.columns.next().of(input.getColumnId(i), input.getColumnType(i), expression.position));
                    aggregate.getOutput().add(ctx.nextColumnId++, ctx.createOutputName(input.getColumnName(i)), input.getColumnType(i), input.getMetadata(i), false);
                    aggregate.getOutput().setSymbolTableStatic(aggregate.getOutput().getColumnCount() - 1, input.isSymbolTableStatic(i));
                }
            }
            return;
        }
        if (!isDuplicateAllowed && findGroupingExpression(expression, source, aggregate) >= 0) {
            return;
        }
        final OutputSchema bindingScope = aggregate.getInput().getType() == LogicalPlan.Type.WINDOW ? ctx.windowBindingScope(input) : input;
        final BoundExpression bound;
        if (expression.type == ExpressionNode.LITERAL && !ctx.functionBinder.isOuterColumn(expression, bindingScope, sourceAlias(source))) {
            final int index = ctx.bindColumnIndex(expression, bindingScope, source);
            bound = ctx.columns.next().of(input.getColumnId(index), input.getColumnType(index), expression.position);
        } else {
            bound = ctx.cursorNodes.size() == 0 ? ctx.functionBinder.bind(expression, bindingScope, sourceAlias(source), context)
                    : ctx.functionBinder.bind(expression, bindingScope, sourceAlias(source), ctx.cursorNodes, ctx.cursorColumns, context);
        }
        // Explicit constant keys retain keyed empty-input semantics. Implicit
        // SELECT constants never reach here and remain above the aggregate.
        ctx.groupingNodes.add(expression);
        aggregate.getGroupingExpressions().add(bound);
        final int index = bound instanceof ColumnExpression ref ? input.getColumnIndexById(ref.getColumnId()) : -1;
        aggregate.getOutput().add(ctx.nextColumnId++, ctx.createOutputName(requestedName != null ? requestedName : aggregateOutputName(expression, model)),
                bound.getDataType(), index < 0 ? null : input.getMetadata(index), false);
        aggregate.getOutput().setSymbolTableStatic(aggregate.getOutput().getColumnCount() - 1, index >= 0 && input.isSymbolTableStatic(index));
    }

    BoundExpression bindAggregateOutput(ExpressionNode expression, QueryModel source, AggregatePlan aggregate, SqlExecutionContext context) throws SqlException {
        ctx.substitutionNodes.clear();
        ctx.substitutionColumns.clear();
        collectAggregateSubstitutions(expression, source, aggregate);
        if (ctx.substitutionNodes.size() == 1 && ctx.substitutionNodes.getQuick(0) == expression) {
            return ctx.substitutionColumns.getQuick(0);
        }
        return ctx.functionBinder.bind(expression, aggregate.getOutput(), null, ctx.substitutionNodes, ctx.substitutionColumns, context);
    }

    LogicalPlan bindAggregation(
            QueryModel model, QueryModel source, LogicalPlan input,
            boolean isOrderAndLimitEnabled, ObjList<ExpressionNode> selectExpressions,
            BoundExpression sampleByBucket, SampleByPlan sampleBy, SqlExecutionContext executionContext
    ) throws SqlException {
        final AggregatePlan aggregate = sampleBy == null ? ctx.aggregates.next().of(input, model.getModelPosition())
                : sampleBy.of(input, model.getModelPosition());
        final ObjList<ExpressionNode> sampleFill = source.getSampleByFill();
        final boolean hasSampleByFill = source.getSampleBy() != null && sampleFill.size() > 0
                && (sampleBy != null || sampleFill.size() > 1 || !SqlKeywords.isNoneKeyword(sampleFill.getQuick(0).token));
        ctx.aggregateNodes.clear();
        ctx.groupingNodes.clear();
        final boolean retainDistinct = model.isDistinct() && !isDistinctRewritten(model, source, isOrderAndLimitEnabled);
        ctx.aggregateSelectExpressions.clear();
        for (int i = 0, n = model.getBottomUpColumns().size(); i < n; i++) {
            final ExpressionNode expression = selectExpressions == null
                    ? model.getBottomUpColumns().getQuick(i).getAst() : selectExpressions.getQuick(i);
            ctx.aggregateSelectExpressions.add(hasSampleByFill ? ExpressionNode.deepClone(ctx.bindingExpressions, expression) : expression);
        }
        aggregateOrderExpressions.clear();
        final ObjList<ExpressionNode> orders = selectExpressions == null ? source.getOrderBy() : ctx.windowOrderExpressions;
        for (int i = 0, n = orders.size(); i < n; i++) {
            aggregateOrderExpressions.add(orders.getQuick(i));
        }
        final ObjList<ExpressionNode> groupBy = blockGroupBy(model, source);
        aggregate.setExplicitGrouping(groupBy.size() > 0);
        aggregate.setSampleByBucket(sampleByBucket != null);
        aggregate.setDirectTableInput(hasDirectTableInput(source));
        aggregate.setKeySpellingKept(input.getType() == LogicalPlan.Type.HORIZON_JOIN ? binder.temporalJoinBinder.hasHorizonOuterProjection(model) : groupBy.size() > 0);
        if (sampleBy != null) {
            binder.sampleByBinder.collectSampleByCursorGrouping(model, source, sampleBy, executionContext);
        } else if (sampleByBucket != null) {
            binder.sampleByBinder.collectSampleByGrouping(model, source, aggregate, sampleByBucket, executionContext);
        } else if (groupBy.size() > 0) {
            int remainingGroups = groupBy.size();
            for (int i = 0, n = groupBy.size(); i < n; i++) {
                final ExpressionNode group = groupBy.getQuick(i);
                final ExpressionNode expression = resolveGroupBy(group, model);
                if (isWildcard(expression)) {
                    throw SqlException.$(expression.position, "'*' is not allowed in GROUP BY");
                }
                if (ctx.hasAggregate(expression) || expression.windowExpression != null) {
                    throw SqlException.$(group.position, "aggregate functions are not allowed in GROUP BY");
                }
                if (remainingGroups > 1 && isConstantGroupingExpression(expression)) {
                    if (i == 0) {
                        aggregate.setConstantLeadingGroupBy(true);
                    }
                    remainingGroups--;
                    continue;
                }
                final int previousCount = aggregate.getGroupingExpressions().size();
                addGroupingExpression(expression, explicitGroupingName(group, expression, model), false, model, source, aggregate, executionContext);
                if (aggregate.getGroupingExpressions().size() == previousCount) {
                    remainingGroups--;
                }
            }
        } else {
            for (int i = 0, n = model.getBottomUpColumns().size(); i < n; i++) {
                collectGroupingExpressions(ctx.aggregateSelectExpressions.getQuick(i), model, source, aggregate, executionContext);
            }
        }
        // ORDER expressions act as SELECT expressions during grouping
        // normalization. A dependency already available from a grouped key can
        // stay above the aggregate; an additional input dependency becomes a key.
        final boolean isHorizonJoin = input.getType() == LogicalPlan.Type.HORIZON_JOIN;
        if ((groupBy.size() == 0 || isHorizonJoin) && isOrderAndLimitEnabled) {
            final boolean hasSelectAliases = model.isDistinct() && !hasAggregation(model, source);
            final boolean hasVirtualSelection = hasAggregateComputation()
                    || sampleByBucket != null && hasTimestampComputation(input.getOutput(), source);
            for (int i = 0, n = source.getOrderBy().size(); i < n; i++) {
                final ExpressionNode order = source.getOrderBy().getQuick(i);
                if (sampleByBucket != null && (order.type == ExpressionNode.FUNCTION || order.type == ExpressionNode.OPERATION)
                        && !ctx.hasAggregate(order) && !binder.sampleByBinder.referencesSampleByTimestamp(order, input.getOutput(), source)
                        && !isSelectedExpression(order, model) && !hasVirtualSelection) {
                    // SAMPLE BY groups by a computed ORDER BY expression.
                    addGroupingExpression(order, model, source, aggregate, executionContext);
                    continue;
                }
                if ((order.type == ExpressionNode.FUNCTION || order.type == ExpressionNode.OPERATION
                        || isHorizonJoin && order.type == ExpressionNode.LITERAL && FunctionBinder.findColumn(order, input.getOutput(), null) >= 0)
                        && (!isHorizonJoin && !hasSelectAliases && !ctx.hasAggregate(order) && findGroupingExpression(order, source, aggregate) < 0
                        && !hasVirtualSelection
                        || !isAggregateOrderAvailable(order, model, source, aggregate, hasSelectAliases))) {
                    if ((sampleByBucket != null || sampleBy != null) && binder.sampleByBinder.referencesSampleByTimestamp(order, input.getOutput(), source)) {
                        binder.sampleByBinder.collectSampleByDependencies(order, model, source, aggregate, executionContext);
                    } else {
                        collectGroupingExpressions(order, model, source, aggregate, executionContext);
                    }
                }
            }
        }
        for (int i = 0, n = model.getBottomUpColumns().size(); i < n; i++) {
            collectAggregateNodes(ctx.aggregateSelectExpressions.getQuick(i), !hasSampleByFill);
        }
        if (isOrderAndLimitEnabled) {
            for (int i = 0, n = source.getOrderBy().size(); i < n; i++) {
                collectAggregateNodes(aggregateOrderExpressions.getQuick(i), true);
            }
        }

        final boolean isCountDistinctRewritten = rewriteCountDistinct(model, source, aggregate, executionContext);
        for (int i = 0; i < ctx.aggregateNodes.size(); ) {
            final ExpressionNode expression = ctx.aggregateNodes.getQuick(i);
            final ExpressionNode call = isCountDistinctRewritten
                    ? ctx.normalizedCount.of(ExpressionNode.FUNCTION, "count", expression.precedence, expression.position)
                    : isRowCount(expression) && expression.paramCount != 0
                      ? ctx.normalizedCount.of(ExpressionNode.FUNCTION, expression.token, expression.precedence, expression.position) : expression;
            final BoundExpression bound = ctx.functionBinder.bindGroupByExpression(call, selectExpressions == null ? aggregate.getInput().getOutput() : ctx.windowBindingScope(aggregate.getInput().getOutput()), sourceAlias(source), executionContext);
            if (bound instanceof FunctionExpression function && function.isAggregate()) {
                if (LogicalPlans.readsOnlyOuterColumns(function)) {
                    throw SqlException.$(expression.position, "aggregate functions are not allowed in FROM clause of their own query level");
                }
                aggregate.getAggregates().add(function);
                i++;
            } else {
                if (groupBy.size() > 0) {
                    throw SqlException.$(model.getModelPosition(), "not enough columns in group by");
                }
                ctx.groupingNodes.add(expression);
                aggregate.getGroupingExpressions().add(bound);
                aggregate.getOutput().add(ctx.nextColumnId++, ctx.createOutputName(aggregateOutputName(expression, model)), bound.getDataType(), false);
                ctx.aggregateNodes.remove(i);
            }
        }
        for (int i = 0, n = ctx.aggregateNodes.size(); i < n; i++) {
            final ExpressionNode expression = ctx.aggregateNodes.getQuick(i);
            final FunctionExpression bound = aggregate.getAggregates().getQuick(i);
            aggregate.getOutput().add(ctx.nextColumnId++, ctx.createOutputName(aggregateOutputName(expression, model)), bound.getDataType(), false);
            if (sampleBy != null) {
                final CharacterStoreEntry sql = ctx.characterStore.newEntry();
                expression.toSink(sql);
                sampleBy.getAggregateSql().add(sql.toImmutable());
            }
        }

        if (sampleBy != null) {
            binder.sampleByBinder.bindSampleByFill(source, sampleBy, executionContext);
        }

        ctx.aliases.clear();
        ctx.aliasSequences.clear();
        if (source.getNestedModel() == null && LogicalPlans.isTimestampEndpoint(aggregate)) {
            // A timestamp endpoint read straight from its FROM table designates its value.
            aggregate.getOutput().setTimestampIndex(0);
        }
        LogicalPlan aggregation = aggregate;
        if ((sampleByBucket != null || sampleBy != null) && SampleByBinder.isFillPlanned(source)) {
            final int timestampIndex = sampleBy == null ? aggregate.getGroupingExpressions().indexOf(sampleByBucket)
                    : findGroupingColumn(sampleBy, sampleBy.getTimestampColumnId());
            aggregation = binder.sampleByBinder.bindFill(source, aggregate, timestampIndex, executionContext);
        }
        if (sampleByBucket != null && source.getOrderBy().size() == 0) {
            final int timestampIndex = aggregate.getGroupingExpressions().indexOf(sampleByBucket);
            final SortPlan sort = ctx.sorts.next().of(aggregation, source.getSampleBy().position);
            sort.getColumnIds().add(aggregate.getOutput().getColumnId(timestampIndex));
            sort.getDirections().add(QueryModel.ORDER_DIRECTION_ASCENDING);
            sort.getOutput().copyFrom(aggregate.getOutput());
            sort.getOutput().setTimestampIndex(timestampIndex);
            aggregation = sort;
        }
        final ProjectPlan project = ctx.projects.next().of(aggregation, model.getModelPosition());
        int nonAggregateCount = 0;
        for (int i = 0, n = ctx.aggregateSelectExpressions.size(); i < n; i++) {
            if (!ctx.isAggregate(ctx.aggregateSelectExpressions.getQuick(i))) {
                nonAggregateCount++;
            }
        }
        for (int i = 0, n = model.getBottomUpColumns().size(); i < n; i++) {
            final QueryColumn column = model.getBottomUpColumns().getQuick(i);
            final ExpressionNode expression = ctx.aggregateSelectExpressions.getQuick(i);
            if (isWildcard(expression)) {
                final OutputSchema schema = input.getOutput();
                ctx.validateWildcard(expression, schema, source);
                for (int k = 0, count = schema.getColumnCount(); k < count; k++) {
                    if (!ctx.isWildcardColumn(expression, schema, k, sourceAlias(source))) {
                        continue;
                    }
                    final int index = findGroupingColumn(aggregate, schema.getColumnId(k));
                    if (index < 0) {
                        if (groupBy.size() > 0) {
                            throw SqlException.$(0, "not enough columns in group by");
                        }
                        throw SqlException.$(expression.position, "column must appear in GROUP BY clause or aggregate function");
                    }
                    final OutputSchema output = aggregate.getOutput();
                    ctx.addProjection(project, ctx.columns.next().of(output.getColumnId(index), output.getColumnType(index), expression.position),
                            output.getMetadata(index), schema.getColumnName(k), true);
                }
            } else {
                BoundExpression bound = bindAggregateOutput(expression, source, aggregate, executionContext);
                final int index = bound instanceof ColumnExpression ref ? aggregate.getOutput().getColumnIndexById(ref.getColumnId()) : -1;
                if (index >= 0 && i < groupBy.size() && nonAggregateCount == groupBy.size() && sampleBy == null && sampleByBucket == null && !((ColumnExpression) bound).isDirectReference()
                        && Chars.equalsIgnoreCase(expression.token, column.getName())
                        && findGroupingExpression(resolveGroupBy(groupBy.getQuick(i), model), source, aggregate) == index) {
                    bound = ctx.columns.next().of(((ColumnExpression) bound).getColumnId(), bound.getDataType(), bound.getPosition());
                }
                ctx.addProjection(project, bound, index < 0 ? null : aggregate.getOutput().getMetadata(index), column.getName(), true);
            }
        }
        for (int i = 0, n = project.getExpressions().size(); i < n; i++) {
            if (project.getExpressions().getQuick(i) instanceof ColumnExpression column
                    && (sampleBy == null || column.isDirectReference())
                    && column.getColumnId() == aggregation.getOutput().getTimestampColumnId()) {
                project.getOutput().setTimestampIndex(i);
            }
        }
        final boolean isDistinctBound = retainDistinct
                || model.isDistinct() && groupBy.size() > 0 && !isProjectingAllGroupingKeys(aggregate, project);
        final LogicalPlan result = isDistinctBound
                ? bindDistinctAggregation(model, source, aggregate, project, isOrderAndLimitEnabled, executionContext)
                : bindAggregateOrder(model, source, aggregate, project, orders, isOrderAndLimitEnabled, executionContext);
        if (sampleBy != null) {
            binder.sampleByBinder.projectSampleByKeys(sampleBy, model, source.getJoinModels().size() > 1);
        }
        return result;
    }

    LogicalPlan bindDistinct(
            QueryModel model, QueryModel source, LogicalPlan sourcePlan, ProjectPlan project,
            boolean isOrderEnabled, SqlExecutionContext executionContext
    ) throws SqlException {
        orderBinder.designateTimestamp(project);
        final int visibleCount = project.getOutput().getColumnCount();
        final ProjectPlan original = project;
        orderOutputIndexes.clear();
        if (isOrderEnabled) {
            for (int i = 0, n = source.getOrderBy().size(); i < n; i++) {
                final ExpressionNode order = source.getOrderBy().getQuick(i);
                int index = orderProjectionIndex(order, project.getOutput(), visibleCount);
                if (index < 0 && order.type == ExpressionNode.LITERAL) {
                    final int sourceIndex = ctx.bindColumnIndex(order, sourcePlan.getOutput(), sourceAlias(source));
                    final int columnId = sourcePlan.getOutput().getColumnId(sourceIndex);
                    for (int k = 0; k < visibleCount; k++) {
                        if (original.getExpressions().getQuick(k) instanceof ColumnExpression column && column.getColumnId() == columnId) {
                            index = k;
                            break;
                        }
                    }
                    if (index < 0) {
                        throw orderNotSelected(order);
                    }
                } else if (index < 0) {
                    int outputIndex = 0;
                    for (int k = 0, count = model.getBottomUpColumns().size(); k < count; k++) {
                        final ExpressionNode selected = model.getBottomUpColumns().getQuick(k).getAst();
                        if (ExpressionNode.compareNodesExact(selected, order)) {
                            index = outputIndex;
                            break;
                        }
                        outputIndex += isWildcard(selected)
                                ? ctx.wildcardExpansionCount(selected, sourcePlan.getOutput(), source) : 1;
                    }
                    for (int k = 0; index < 0 && k < i; k++) {
                        if (ExpressionNode.compareNodesExact(source.getOrderBy().getQuick(k), order)) {
                            index = orderOutputIndexes.getQuick(k);
                        }
                    }
                    if (index < 0) {
                        // A computed ORDER BY key evaluates above the DISTINCT, over the selected tuple.
                        if (project == original) {
                            project = ctx.projects.next().of(original, original.getPosition());
                            ctx.aliases.clear();
                            ctx.aliasSequences.clear();
                            for (int k = 0; k < visibleCount; k++) {
                                final OutputSchema inputSchema = original.getOutput();
                                ctx.addProjection(project, ctx.columns.next().of(inputSchema.getColumnId(k), inputSchema.getColumnType(k),
                                                original.getExpressions().getQuick(k).getPosition()),
                                        inputSchema.getMetadata(k), inputSchema.getColumnName(k), inputSchema.isVisible(k));
                            }
                            project.getOutput().setTimestampIndex(original.getOutput().getTimestampIndex());
                        }
                        ctx.substitutionNodes.clear();
                        ctx.substitutionColumns.clear();
                        orderBinder.collectOrderSubstitutions(order, sourcePlan.getOutput(), sourceAlias(source), original, visibleCount, false);
                        final BoundExpression bound = ctx.functionBinder.bind(order, original.getOutput(), null,
                                ctx.substitutionNodes, ctx.substitutionColumns, executionContext);
                        final CharSequence name = SqlUtil.createColumnAlias(ctx.characterStore, order.token,
                                Chars.indexOfLastUnquoted(order.token, '.'), ctx.aliases, ctx.aliasSequences, true);
                        index = project.getExpressions().size();
                        ctx.addProjection(project, bound, null, name, false);
                    }
                }
                final int type = project.getOutput().getColumnType(index);
                if (!ColumnType.isComparable(type)) {
                    throw SqlException.$(order.position, ColumnType.nameOf(type)).put(" is not a supported type in ORDER BY clause");
                }
                orderOutputIndexes.add(index);
            }
        }
        final DistinctPlan distinct = ctx.distincts.next().of(original, model.getModelPosition());
        distinct.getOutput().copyFrom(original.getOutput());
        ctx.stopTimestampIntrinsics(distinct.getOutput());
        if (orderOutputIndexes.size() == 0) {
            return distinct;
        }
        final LogicalPlan sortInput;
        if (project == original) {
            sortInput = distinct;
        } else {
            project.replaceInput(0, distinct);
            sortInput = project;
        }
        final SortPlan sort = ctx.sorts.next().of(sortInput, source.getOrderByPosition());
        for (int i = 0, n = orderOutputIndexes.size(); i < n; i++) {
            final int columnId = sortInput.getOutput().getColumnId(orderOutputIndexes.getQuick(i));
            if (!sort.getColumnIds().contains(columnId)) {
                sort.getColumnIds().add(columnId);
                sort.getDirections().add(source.getOrderByDirection().getQuick(i));
            }
        }
        sort.deriveOutput();
        return project == original ? sort : orderBinder.projectVisible(sort, project.getOutput(), project, visibleCount);
    }

    /**
     * Collects aggregates in the order the group-by rewrite emits them: a node's right operand
     * before its left, the left subtree before deferred right subtrees, and n-ary arguments in
     * storage order.
     */
    void collectAggregateNodes(ExpressionNode expression, boolean isDeduplicationAllowed) {
        if (expression == null || collectAggregateNode(expression, isDeduplicationAllowed, false)) {
            return;
        }
        final int base = aggregateNodeStack.size();
        ExpressionNode node = expression;
        while (aggregateNodeStack.size() > base || node != null) {
            if (node == null) {
                node = aggregateNodeStack.popLast();
            } else if (node.paramCount < 3) {
                if (node.rhs != null && !collectAggregateNode(node.rhs, isDeduplicationAllowed, true)) {
                    aggregateNodeStack.add(node.rhs);
                }
                node = node.lhs == null || collectAggregateNode(node.lhs, isDeduplicationAllowed, true) ? null : node.lhs;
            } else {
                for (int i = 0, n = node.args.size(); i < n; i++) {
                    final ExpressionNode argument = node.args.getQuick(i);
                    if (!collectAggregateNode(argument, isDeduplicationAllowed, true)) {
                        aggregateNodeStack.add(argument);
                    }
                }
                node = null;
            }
        }
    }

    void collectColumnSpellings(ExpressionNode node, OutputSchema input) {
        if (node == null) {
            return;
        }
        if (node.type == ExpressionNode.LITERAL) {
            final int index = FunctionBinder.findColumn(node, input, null);
            if (index >= 0 && ctx.columnSpellings.keyIndex(input.getColumnId(index)) > -1) {
                ctx.columnSpellings.put(input.getColumnId(index), node);
            }
            return;
        }
        collectColumnSpellings(node.lhs, input);
        collectColumnSpellings(node.rhs, input);
        for (int i = 0, n = node.args.size(); i < n; i++) {
            collectColumnSpellings(node.args.getQuick(i), input);
        }
    }

    void collectGroupingExpressions(ExpressionNode expression, QueryModel model, QueryModel source, AggregatePlan aggregate, SqlExecutionContext context) throws SqlException {
        collectGroupingExpressions(expression, model, source, aggregate, context, false);
    }

    void collectGroupingExpressions(
            ExpressionNode expression, QueryModel model, QueryModel source, AggregatePlan aggregate, SqlExecutionContext context, boolean isNested
    ) throws SqlException {
        if (expression == null || ctx.isAggregate(expression)) {
            return;
        }
        if (!ctx.hasAggregate(expression) && (!isNested || expression.type == ExpressionNode.LITERAL)) {
            if (!isConstantGroupingExpression(expression) || !isNested && expression.type != ExpressionNode.BIND_VARIABLE
                    && aggregate.getInput().getType() == LogicalPlan.Type.HORIZON_JOIN) {
                addGroupingExpression(expression, null, !isNested && expression.type == ExpressionNode.LITERAL
                        && Chars.indexOfLastUnquoted(expression.token, '.') > -1, model, source, aggregate, context);
            }
        } else if (expression.paramCount < 3) {
            collectGroupingExpressions(expression.lhs, model, source, aggregate, context, true);
            collectGroupingExpressions(expression.rhs, model, source, aggregate, context, true);
        } else {
            for (int i = 0, n = expression.args.size(); i < n; i++) {
                collectGroupingExpressions(expression.args.getQuick(i), model, source, aggregate, context, true);
            }
        }
    }

    int findAggregate(ExpressionNode expression) {
        for (int i = 0, n = ctx.aggregateNodes.size(); i < n; i++) {
            if (ctx.aggregateNodes.getQuick(i) == expression) {
                return i;
            }
        }
        for (int i = 0, n = ctx.aggregateNodes.size(); i < n; i++) {
            final ExpressionNode aggregate = ctx.aggregateNodes.getQuick(i);
            if (ExpressionNode.compareNodesExact(expression, aggregate) || isRowCount(expression) && isRowCount(aggregate)) {
                return i;
            }
        }
        return -1;
    }

    int findAggregatePosition(ExpressionNode expression) {
        if (expression == null) {
            return -1;
        }
        if (ctx.isAggregate(expression)) {
            return expression.position;
        }
        int position = findAggregatePosition(expression.lhs);
        if (position < 0) {
            position = findAggregatePosition(expression.rhs);
        }
        for (int i = 0, n = expression.args.size(); position < 0 && i < n; i++) {
            position = findAggregatePosition(expression.args.getQuick(i));
        }
        return position;
    }

    int findGroupingExpression(ExpressionNode expression, QueryModel source, AggregatePlan aggregate) throws SqlException {
        final OutputSchema bindingScope = aggregate.getInput().getType() == LogicalPlan.Type.WINDOW
                ? ctx.windowBindingScope(aggregate.getInput().getOutput()) : aggregate.getInput().getOutput();
        final int identity = ctx.groupingNodes.indexOf(expression);
        if (identity >= 0) {
            return identity;
        }
        for (int i = 0, n = ctx.groupingNodes.size(); i < n; i++) {
            if (ctx.groupingNodes.getQuick(i) != null && equalGroupingExpressions(expression, ctx.groupingNodes.getQuick(i), source, bindingScope)) {
                return i;
            }
        }
        if (expression.type == ExpressionNode.LITERAL && !isWildcard(expression)
                && !ctx.functionBinder.isOuterColumn(expression, bindingScope, sourceAlias(source))) {
            final int index = ctx.bindColumnIndex(expression, bindingScope, source);
            return findGroupingColumn(aggregate, bindingScope.getColumnId(index));
        }
        return -1;
    }

    boolean hasAggregation(QueryModel model, QueryModel source) {
        if (blockGroupBy(model, source).size() > 0) {
            return true;
        }
        for (int i = 0, n = model.getBottomUpColumns().size(); i < n; i++) {
            if (ctx.hasAggregate(model.getBottomUpColumns().getQuick(i).getAst())) {
                return true;
            }
        }
        for (int i = 0, n = source.getOrderBy().size(); i < n; i++) {
            if (ctx.hasAggregate(source.getOrderBy().getQuick(i))) {
                return true;
            }
        }
        return false;
    }

    boolean isDistinctRewritten(QueryModel model, QueryModel source, boolean isOrderEnabled) {
        for (int i = 0, n = model.getBottomUpColumns().size(); i < n; i++) {
            final QueryColumn column = model.getBottomUpColumns().getQuick(i);
            if (column.isWindowExpression() || ctx.isAggregate(column.getAst())) {
                return false;
            }
        }
        if (isOrderEnabled) {
            for (int i = 0, n = source.getOrderBy().size(); i < n; i++) {
                if (ctx.isAggregate(source.getOrderBy().getQuick(i))) {
                    return false;
                }
            }
        }
        return true;
    }

    SqlException updateAggregateException(QueryModel model) {
        for (int i = 0, n = model.getBottomUpColumns().size(); i < n; i++) {
            final ExpressionNode value = model.getBottomUpColumns().getQuick(i).getAst();
            if (value.type == ExpressionNode.FUNCTION && ctx.functionFactoryCache.isGroupBy(value.token)) {
                return SqlException.$(value.position, "Unsupported function in SET clause");
            }
        }
        return SqlException.$(model.getModelPosition(), "Unsupported SQL complexity for the UPDATE statement");
    }

    /**
     * Rejects an ORDER BY expression of a SELECT DISTINCT that the selected tuple does not determine:
     * one that reads a source column the select list leaves out, or an aggregate whose groups the
     * select list does not fix.
     */
    void validateDistinctOrder(QueryModel model, QueryModel source, OutputSchema input) throws SqlException {
        final boolean isGroupingSelected = isDistinctGroupingSelected(model, source, input);
        for (int i = 0, n = source.getOrderBy().size(); i < n; i++) {
            final ExpressionNode order = source.getOrderBy().getQuick(i);
            if (order.type != ExpressionNode.LITERAL || !isSelectAlias(order, model)) {
                validateDistinctOrder(order, model, source, input, isGroupingSelected);
            }
        }
    }

    void validateGroupByKeys(QueryModel model, QueryModel source) throws SqlException {
        final ObjList<ExpressionNode> groupBy = blockGroupBy(model, source);
        for (int i = 0, n = groupBy.size(); i < n; i++) {
            final ExpressionNode group = groupBy.getQuick(i);
            final ExpressionNode expression = resolveGroupBy(group, model);
            if (isWildcard(expression)) {
                throw SqlException.$(expression.position, "'*' is not allowed in GROUP BY");
            }
            final ExpressionNode violation = findGroupByViolation(expression);
            if (violation != null) {
                final int position = expression != group ? group.position : violation.position;
                throw SqlException.$(position, ctx.functionFactoryCache.isGroupBy(violation.token)
                        ? "aggregate functions are not allowed in GROUP BY" : "window functions are not allowed in GROUP BY");
            }
        }
    }
}
