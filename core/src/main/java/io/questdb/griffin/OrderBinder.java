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
import io.questdb.griffin.plan.logical.ConstantExpression;
import io.questdb.griffin.plan.logical.FunctionExpression;
import io.questdb.griffin.plan.logical.LimitPlan;
import io.questdb.griffin.plan.logical.LogicalPlan;
import io.questdb.griffin.plan.logical.OutputSchema;
import io.questdb.griffin.plan.logical.ProjectPlan;
import io.questdb.griffin.plan.logical.SortPlan;
import io.questdb.griffin.plan.logical.WindowPlan;
import io.questdb.std.Chars;
import io.questdb.std.Mutable;
import io.questdb.std.Numbers;
import io.questdb.std.NumericException;
import io.questdb.std.ObjList;

import static io.questdb.griffin.BindContext.getColumnIndexQuiet;
import static io.questdb.griffin.BindContext.hasComputedProjection;
import static io.questdb.griffin.BindContext.isRowCount;
import static io.questdb.griffin.BindContext.sourceAlias;
import static io.questdb.griffin.TemporalJoinBinder.designateTimestampOffset;

final class OrderBinder implements Mutable {
    final ExpressionNode normalizedCount = ExpressionNode.FACTORY.newInstance();
    private final BindContext ctx;
    private final OutputSchema emptySchema;
    private final SampleByBinder sampleByBinder;

    OrderBinder(BindContext ctx, OutputSchema emptySchema, SampleByBinder sampleByBinder) {
        this.ctx = ctx;
        this.emptySchema = emptySchema;
        this.sampleByBinder = sampleByBinder;
    }

    @Override
    public void clear() {
        normalizedCount.clear();
    }

    private static boolean isRepeatedColumn(ProjectPlan project, int columnId) {
        int count = 0;
        for (int i = 0, n = project.getExpressions().size(); i < n; i++) {
            if (project.getExpressions().getQuick(i) instanceof ColumnExpression column && column.getColumnId() == columnId) {
                count++;
            }
        }
        return count > 1;
    }

    private static int orderSourceIndex(ProjectPlan project, int index, OutputSchema input) {
        if (project.getExpressions().getQuick(index) instanceof ColumnExpression column) {
            return input.getColumnIndexById(column.getColumnId());
        }
        throw new IllegalStateException("source order over a computed projection");
    }

    private static void validateCorrelatedLimit(ExpressionNode limit, BoundExpression bound) throws SqlException {
        if (bound instanceof ConstantExpression constant && constant.getLongValue() < 0 && constant.getLongValue() != Numbers.LONG_NULL) {
            throw SqlException.$(limit.position, "negative LIMIT is not supported in a correlated lateral sub-query");
        }
    }

    private static void validateLimitType(int type, boolean isConstantOrRuntimeConstant, int position) throws SqlException {
        if (!isConstantOrRuntimeConstant || !ColumnType.isConvertibleFrom(type, ColumnType.LONG)) {
            throw SqlException.$(position, "LIMIT expressions must be convertible to INT");
        }
        switch (type) {
            case ColumnType.LONG, ColumnType.BYTE, ColumnType.SHORT, ColumnType.INT, ColumnType.UNDEFINED -> {
            }
            default -> throw SqlException.$(position, "invalid type: ").put(ColumnType.nameOf(type));
        }
    }

    private LogicalPlan bindOutputOrder(
            QueryModel model, LogicalPlan sourcePlan, ProjectPlan project, QueryModel source, CharSequence scopeAlias,
            ObjList<ExpressionNode> orderExpressions, ProjectPlan subsampleOrdering, SqlExecutionContext executionContext
    ) throws SqlException {
        final BindScope scope = ctx.scope();
        final int visibleCount = project.getExpressions().size();
        final OutputSchema sourceScope = orderExpressions == null ? sourcePlan.getOutput() : ctx.windowBindingScope(sourcePlan.getOutput());
        ProjectPlan ordering = subsampleOrdering;
        scope.orderOutputIndexes.clear();
        final SortPlan sort = ctx.planNodes.sorts.next().of(project, source.getOrderByPosition());
        for (int i = 0, n = source.getOrderBy().size(); i < n; i++) {
            final ExpressionNode originalOrder = source.getOrderBy().getQuick(i);
            ExpressionNode order = orderExpressions == null ? originalOrder : orderExpressions.getQuick(i);
            int index = orderProjectionIndex(order, project.getOutput(), visibleCount);
            if (index >= 0 && hasComputedProjection(project) && order.type == ExpressionNode.LITERAL
                    && project.getExpressions().getQuick(index) instanceof ColumnExpression column) {
                final OutputSchema projectInput = project.getInput().getOutput();
                final int inputIndex = projectInput.getColumnIndexById(column.getColumnId());
                if (inputIndex < 0 || !Chars.equalsIgnoreCase(order.token, projectInput.getColumnName(inputIndex))) {
                    sort.markAliasedKey();
                }
            }
            if (index < 0 && model != null && originalOrder.type != ExpressionNode.LITERAL) {
                int outputIndex = 0;
                for (int k = 0, count = model.getBottomUpColumns().size(); k < count; k++) {
                    final ExpressionNode selected = model.getBottomUpColumns().getQuick(k).getAst();
                    if (ExpressionNode.compareNodesExact(originalOrder, selected)) {
                        index = outputIndex;
                        break;
                    }
                    outputIndex += selected.isWildcard() ? ctx.wildcardExpansionCount(selected, sourcePlan.getOutput(), source) : 1;
                }
            }
            for (int k = 0; index < 0 && k < i; k++) {
                if (ExpressionNode.compareNodesExact(originalOrder, source.getOrderBy().getQuick(k))) {
                    index = scope.orderOutputIndexes.getQuick(k);
                }
            }
            if (index < 0) {
                if (ordering == null && hasOrderAliasReference(order, sourceScope, project, visibleCount)
                        && canBindOrderAliases(sourceScope, project, source, scopeAlias, visibleCount)) {
                    ordering = createOrderProjection(project, source.getOrderByPosition());
                }
                final BoundExpression expression;
                OutputSchema nested = null;
                final int dot = order.type == ExpressionNode.LITERAL ? Chars.indexOfLastUnquoted(order.token, '.') : -1;
                CharSequence hiddenName = dot < 0 ? order.token : order.token.subSequence(dot + 1, order.token.length());
                if (ordering != null) {
                    scope.substitutionNodes.clear();
                    scope.substitutionColumns.clear();
                    collectOrderSubstitutions(order, sourceScope, scopeAlias, project, visibleCount, source.getSubsample() != null);
                    expression = ctx.functionBinder.bind(order, project.getOutput(), null,
                            scope.substitutionNodes, scope.substitutionColumns, executionContext);
                    if (expression instanceof ColumnExpression column) {
                        for (int k = 0, count = ordering.getExpressions().size(); k < count; k++) {
                            if (ordering.getExpressions().getQuick(k) instanceof ColumnExpression projected && projected.getColumnId() == column.getColumnId()) {
                                index = k;
                                break;
                            }
                        }
                    }
                } else if (order.type == ExpressionNode.LITERAL) {
                    final int sourceIndex = ctx.bindColumnIndex(order, sourceScope, scopeAlias);
                    final int columnId = sourcePlan.getOutput().getColumnId(sourceIndex);
                    for (int k = 0, count = project.getExpressions().size(); k < count; k++) {
                        if (project.getExpressions().getQuick(k) instanceof ColumnExpression column && column.getColumnId() == columnId) {
                            index = k;
                            break;
                        }
                    }
                    expression = index < 0 ? ctx.planNodes.columns.next().of(columnId, sourcePlan.getOutput().getColumnType(sourceIndex), order.position) : null;
                    nested = sourcePlan.getOutput().getMetadata(sourceIndex);
                    hiddenName = sourcePlan.getOutput().getColumnName(sourceIndex);
                } else {
                    expression = ctx.functionBinder.bind(order, sourceScope, scopeAlias, executionContext);
                }
                if (index < 0) {
                    final ProjectPlan target = ordering == null ? project : ordering;
                    index = target.getExpressions().size();
                    ctx.addProjection(target, expression, nested, order.type == ExpressionNode.LITERAL ? hiddenName
                            : SqlUtil.createColumnAlias(ctx.characterStore, order.token, Chars.indexOfLastUnquoted(order.token, '.'), scope.aliases, scope.aliasSequences, true), false);
                }
            }
            scope.orderOutputIndexes.add(index);
            final OutputSchema output = (ordering == null ? project : ordering).getOutput();
            final int type = output.getColumnType(index);
            final int columnId = output.getColumnId(index);
            if (!ColumnType.isComparable(type)) {
                throw SqlException.$(order.position, ColumnType.nameOf(type)).put(" is not a supported type in ORDER BY clause");
            }
            if (!sort.getColumnIds().contains(columnId)) {
                sort.getColumnIds().add(columnId);
                sort.getDirections().add(SqlBinder.sortDirection(source.getOrderByDirection().getQuick(i)));
            }
        }
        if (subsampleOrdering != null) {
            subsampleOrdering.replaceInput(0, sampleByBinder.bindSubsample(project, sourcePlan, source, executionContext));
        }
        final ProjectPlan sortInput = ordering == null ? project : ordering;
        final OutputSchema output = sortInput.getOutput();
        final OutputSchema projectInput = project.getInput().getOutput();
        final int timestampIndex = projectInput.getTimestampIndex();
        if (timestampIndex >= 0) {
            final int timestampId = projectInput.getColumnId(timestampIndex);
            final CharSequence timestampName = projectInput.getColumnName(timestampIndex);
            final int firstOrderId = sort.getColumnIds().getQuick(0);
            boolean isTimestampSet = false;
            for (int i = 0, n = project.getExpressions().size(); i < n; i++) {
                if (project.getExpressions().getQuick(i) instanceof ColumnExpression column && column.getColumnId() == timestampId
                        && (!isTimestampSet || project.getOutput().getColumnId(i) == firstOrderId
                        || Chars.equalsIgnoreCase(project.getOutput().getColumnName(i), timestampName))) {
                    project.getOutput().setTimestampIndex(i);
                    isTimestampSet = true;
                }
            }
        }
        if (ordering != null) {
            final int projectedTimestampId = project.getOutput().getTimestampColumnId();
            for (int i = 0, n = ordering.getExpressions().size(); i < n; i++) {
                if (ordering.getExpressions().getQuick(i) instanceof ColumnExpression column
                        && column.getColumnId() == projectedTimestampId) {
                    ordering.getOutput().setTimestampIndex(i);
                    break;
                }
            }
        }
        sort.replaceInput(0, sortInput);
        if (visibleCount == output.getColumnCount()) {
            return sort;
        }
        return projectVisible(sort, output, project, visibleCount);
    }

    private boolean canBindOrderAliases(
            OutputSchema input, ProjectPlan project, QueryModel source, CharSequence scopeAlias, int visibleCount
    ) throws SqlException {
        // ORDER BY exposes unselected bare source columns before resolving SELECT expressions. That scope
        // cannot reference computed output aliases, so the existing diagnostics apply.
        boolean isAvailable = true;
        for (int i = 0, n = source.getOrderBy().size(); i < n; i++) {
            final ExpressionNode order = source.getOrderBy().getQuick(i);
            if (order.type != ExpressionNode.LITERAL || orderProjectionIndex(order, project.getOutput(), visibleCount) >= 0) {
                continue;
            }
            final int index = ctx.bindColumnIndex(order, input, scopeAlias);
            final int columnId = input.getColumnId(index);
            boolean isSelected = false;
            for (int k = 0; k < visibleCount; k++) {
                if (project.getExpressions().getQuick(k) instanceof ColumnExpression column && column.getColumnId() == columnId) {
                    isSelected = true;
                    break;
                }
            }
            isAvailable &= isSelected;
        }
        return isAvailable;
    }

    private ProjectPlan createOrderProjection(ProjectPlan input, int position) {
        final BindScope scope = ctx.scope();
        final ProjectPlan project = ctx.planNodes.projects.next().of(input, position);
        final OutputSchema output = input.getOutput();
        project.getOutput().copyFrom(output);
        scope.aliases.clear();
        scope.aliasSequences.clear();
        for (int i = 0, n = output.getColumnCount(); i < n; i++) {
            project.getExpressions().add(ctx.planNodes.columns.next().of(output.getColumnId(i), output.getColumnType(i), position));
            scope.aliases.add(output.getColumnName(i));
        }
        return project;
    }

    private void designateTimestamp(ProjectPlan project, int orderedProjectionIndex) {
        final OutputSchema input = project.getInput().getOutput();
        final OutputSchema output = project.getOutput();
        // Projection keeps fresh output identities; timestamp designation maps through
        // its explicit column references.
        output.setTimestampIndex(-1);
        final int inputTimestampIndex = input.getTimestampIndex();
        if (inputTimestampIndex < 0) {
            return;
        }
        final int timestampId = input.getColumnId(inputTimestampIndex);
        final CharSequence timestampName = input.getColumnName(inputTimestampIndex);
        // A computed projection designates the last plain reference, as the virtual factory does.
        final boolean isLastReference = hasComputedProjection(project);
        for (int i = 0, n = project.getExpressions().size(); i < n; i++) {
            if (project.getExpressions().getQuick(i) instanceof ColumnExpression column && column.getColumnId() == timestampId
                    && (column.isDirectReference() || i == orderedProjectionIndex)
                    && !ctx.scope().translatingCopyIds.contains(output.getColumnId(i))) {
                if (i == orderedProjectionIndex
                        || output.getTimestampIndex() < 0
                        || orderedProjectionIndex < 0 && (isLastReference
                        || Chars.equalsIgnoreCase(output.getColumnName(i), timestampName))) {
                    output.setTimestampIndex(i);
                }
            }
        }
        designateTimestampOffset(project);
    }

    private boolean hasOrderAliasReference(ExpressionNode expression, OutputSchema input, ProjectPlan project, int visibleCount) throws SqlException {
        if (expression == null) {
            return false;
        }
        if (expression.type == ExpressionNode.LITERAL) {
            return orderAliasIndex(expression, input, project, visibleCount, false) >= 0;
        }
        if (expression.paramCount < 3) {
            return hasOrderAliasReference(expression.lhs, input, project, visibleCount)
                    || hasOrderAliasReference(expression.rhs, input, project, visibleCount);
        }
        for (int i = 0, n = expression.args.size(); i < n; i++) {
            if (hasOrderAliasReference(expression.args.getQuick(i), input, project, visibleCount)) {
                return true;
            }
        }
        return false;
    }

    private int orderAliasIndex(
            ExpressionNode expression, OutputSchema input, ProjectPlan project, int visibleCount, boolean isAliasPreferred
    ) throws SqlException {
        // A bare ORDER BY name uses the output alias; names inside expressions prefer source columns,
        // except under SUBSAMPLE, whose ordering scope is the sampled output.
        return Chars.indexOfLastUnquoted(expression.token, '.') < 0 && (isAliasPreferred || getColumnIndexQuiet(input, expression.token) < 0)
                ? orderProjectionIndex(expression, project.getOutput(), visibleCount) : -1;
    }

    /**
     * The projection index of the timestamp the ORDER BY sorts its sorted input by first, or -1 when the
     * input has no designated timestamp or the visible projection does not select the sorted column.
     */
    private int orderedTimestampIndex(ProjectPlan project, QueryModel source) throws SqlException {
        final BindScope scope = ctx.scope();
        final OutputSchema input = project.getInput().getOutput();
        final int inputTimestampIndex = input.getTimestampIndex();
        if (inputTimestampIndex < 0) {
            return -1;
        }
        final int index = orderProjectionIndex(source.getOrderBy().getQuick(0), project.getOutput(), project.getExpressions().size());
        if (index >= 0) {
            return index;
        }
        final int timestampId = input.getColumnId(inputTimestampIndex);
        final CharSequence timestampName = input.getColumnName(inputTimestampIndex);
        boolean hasOuterProjection = false;
        for (int i = 0, n = scope.projectionAliasIndexes.size(); i < n; i++) {
            if (scope.projectionAliasIndexes.getQuick(i) != i) {
                hasOuterProjection = true;
                break;
            }
        }
        int orderedIndex = -1;
        for (int i = 0, n = project.getExpressions().size(); i < n; i++) {
            if (project.getExpressions().getQuick(i) instanceof ColumnExpression column && column.isDirectReference()
                    && column.getColumnId() == timestampId
                    && (!hasOuterProjection || Chars.equalsIgnoreCase(
                    project.getOutput().getColumnName(scope.projectionAliasIndexes.getQuick(i)), timestampName))) {
                orderedIndex = i;
            }
        }
        return orderedIndex;
    }

    static boolean hasComputedOrder(ProjectPlan project, QueryModel source) throws SqlException {
        for (int i = 0, n = source.getOrderBy().size(); i < n; i++) {
            final ExpressionNode order = source.getOrderBy().getQuick(i);
            final int index = orderProjectionIndex(order, project.getOutput(), project.getExpressions().size());
            if (index >= 0 && !(project.getExpressions().getQuick(index) instanceof ColumnExpression)
                    || order.type != ExpressionNode.LITERAL && order.type != ExpressionNode.CONSTANT) {
                return true;
            }
        }
        return false;
    }

    static boolean hasProjectedOrder(ProjectPlan project, QueryModel source) throws SqlException {
        for (int i = 0, n = source.getOrderBy().size(); i < n; i++) {
            final ExpressionNode order = source.getOrderBy().getQuick(i);
            final int dot = order.type == ExpressionNode.LITERAL ? Chars.indexOfLastUnquoted(order.token, '.') : -1;
            if (orderProjectionIndex(order, project.getOutput(), project.getExpressions().size()) >= 0) {
                return true;
            }
            final int index = dot > 0 ? project.getOutput().getColumnIndexQuiet(order.token, dot + 1, order.token.length()) : -1;
            if (index >= 0 && project.getExpressions().getQuick(index) instanceof ColumnExpression column && !isRepeatedColumn(project, column.getColumnId())) {
                return true;
            }
        }
        return false;
    }

    static boolean isRowCountOrder(QueryColumn column, QueryModel source) {
        for (int i = 0, n = source.getOrderBy().size(); i < n; i++) {
            final ExpressionNode order = source.getOrderBy().getQuick(i);
            if (order.type != ExpressionNode.CONSTANT && !isRowCount(order)
                    && (order.type != ExpressionNode.LITERAL || Chars.indexOfLastUnquoted(order.token, '.') >= 0
                    || !Chars.equalsIgnoreCase(order.token, column.getName()))) {
                return false;
            }
        }
        return true;
    }

    static boolean isSelectedOrder(ProjectPlan project, QueryModel source, OutputSchema scope, CharSequence alias) throws SqlException {
        for (int i = 0, n = source.getOrderBy().size(); i < n; i++) {
            final ExpressionNode order = source.getOrderBy().getQuick(i);
            if (orderProjectionIndex(order, project.getOutput(), project.getExpressions().size()) >= 0) {
                continue;
            }
            final int index = order.type == ExpressionNode.LITERAL ? FunctionBinder.findColumn(order, scope, alias) : -1;
            if (index < 0) {
                return false;
            }
            boolean isSelected = false;
            for (int k = 0, m = project.getExpressions().size(); k < m && !isSelected; k++) {
                isSelected = project.getExpressions().getQuick(k) instanceof ColumnExpression column && column.getColumnId() == scope.getColumnId(index);
            }
            if (!isSelected) {
                return false;
            }
        }
        return source.getOrderBy().size() > 0;
    }

    static int orderProjectionIndex(ExpressionNode order, OutputSchema output, int visibleCount) throws SqlException {
        if (order.type == ExpressionNode.CONSTANT) {
            try {
                final int ordinal = Numbers.parseInt(order.token);
                if (ordinal < 1 || ordinal > visibleCount) {
                    final int index = getColumnIndexQuiet(output, order.token);
                    if (index >= 0 && index < visibleCount) {
                        return index;
                    }
                    throw SqlException.$(order.position, "order column position is out of range [max=")
                            .put(visibleCount).put(']');
                }
                return ordinal - 1;
            } catch (NumericException e) {
                final char first = order.token.charAt(0);
                if (first < '0' || first > '9') {
                    throw SqlException.invalidColumn(order.position, order.token);
                }
                if (Chars.indexOf(order.token, '.') >= 0) {
                    throw SqlException.$(order.position, "Invalid table name or alias");
                }
                final int index = getColumnIndexQuiet(output, order.token);
                if (index < 0 || index >= visibleCount) {
                    throw SqlException.invalidColumn(order.position, order.token);
                }
                return index;
            }
        }
        if (order.type == ExpressionNode.LITERAL && Chars.indexOfLastUnquoted(order.token, '.') < 0) {
            final int index = getColumnIndexQuiet(output, order.token);
            return index < visibleCount ? index : -1;
        }
        return -1;
    }

    BoundExpression bindLimit(ExpressionNode expression, SqlExecutionContext executionContext) throws SqlException {
        if (ctx.isAggregate(expression)) {
            throw SqlException.$(expression.position, "LIMIT expressions must be convertible to INT");
        }
        final BoundExpression bound = ctx.functionBinder.bind(expression, emptySchema, null, ColumnType.LONG, executionContext);
        validateLimitType(bound.getDataType(),
                (bound.getFunctionFlags() & (BoundExpression.CONSTANT | BoundExpression.RUNTIME_CONSTANT)) != 0 || LogicalPlans.hasOuterColumn(bound),
                expression.position);
        return bound;
    }

    LogicalPlan bindLimit(LogicalPlan input, QueryModel model, SqlExecutionContext executionContext) throws SqlException {
        if (model.getLimitLo() == null && model.getLimitHi() == null) {
            if (LogicalPlans.skipProjects(input) instanceof SortPlan sort) {
                sort.markUnlimited();
            }
            return input;
        }
        final int position = model.getLimitLo() != null ? model.getLimitLo().position : model.getLimitHi().position;
        final BoundExpression lo = model.getLimitLo() == null ? ctx.planNodes.constants.next().ofLong(0, position) : bindLimit(model.getLimitLo(), executionContext);
        final BoundExpression hi = model.getLimitHi() == null ? null : bindLimit(model.getLimitHi(), executionContext);
        if (LogicalPlans.hasOuterColumn(input, ctx.tmpOuterColumns)) {
            validateCorrelatedLimit(model.getLimitLo(), lo);
            validateCorrelatedLimit(model.getLimitHi(), hi);
        }
        final LimitPlan limit = ctx.planNodes.limits.next().of(input, lo, hi, position);
        limit.deriveOutput();
        ctx.stopTimestampIntrinsics(limit.getOutput());
        return limit;
    }

    /**
     * Sorts the block's output by its ORDER BY. A block with SUBSAMPLE samples its output below the sort.
     */
    LogicalPlan bindOutputOrder(
            QueryModel model, LogicalPlan sourcePlan, ProjectPlan project, QueryModel source,
            CharSequence scopeAlias, ObjList<ExpressionNode> orderExpressions, SqlExecutionContext executionContext
    ) throws SqlException {
        final ProjectPlan subsampleOrdering = source.getSubsample() != null ? createOrderProjection(project, source.getOrderByPosition()) : null;
        return bindOutputOrder(model, sourcePlan, project, source, scopeAlias, orderExpressions, subsampleOrdering, executionContext);
    }

    LogicalPlan bindRowCount(QueryModel model, QueryModel source, LogicalPlan input, SqlExecutionContext executionContext) throws SqlException {
        final QueryColumn column = model.getBottomUpColumns().getQuick(0);
        final ExpressionNode expression = column.getAst();
        // Parser already normalizes COUNT(*). Match rewriteCount's non-NULL
        // literal normalization without mutating the parser's expression.
        final ExpressionNode call = expression.paramCount == 0 ? expression
                : normalizedCount.of(ExpressionNode.FUNCTION, expression.token, expression.precedence, expression.position);
        final FunctionExpression count = ctx.functionBinder.bindAggregate(call, input.getOutput(), sourceAlias(source), executionContext);
        final AggregatePlan aggregate = ctx.planNodes.aggregates.next().of(input, expression.position);
        aggregate.getAggregates().add(count);
        // Keep SQL names for an outer scope. The existing count specialization
        // canonicalizes its own COUNT alias; a real outer projection may rename it.
        aggregate.getOutput().add(ctx.scope().nextColumnId++, ctx.createOutputName(column.getName()), count.getDataType(), true);
        final boolean isOrderBound = !ctx.hasDeferredOrder(source);
        LogicalPlan result = aggregate;
        if (isOrderBound && source.getOrderBy().size() > 0) {
            final SortPlan sort = ctx.planNodes.sorts.next().of(aggregate, source.getOrderByPosition());
            for (int i = 0, n = source.getOrderBy().size(); i < n; i++) {
                final ExpressionNode order = source.getOrderBy().getQuick(i);
                if (orderProjectionIndex(order, aggregate.getOutput(), 1) < 0 && !isRowCount(order)) {
                    throw new IllegalStateException("unresolved row count order");
                }
                if (i == 0) {
                    sort.getColumnIds().add(aggregate.getOutput().getColumnId(0));
                    sort.getDirections().add(SqlBinder.sortDirection(source.getOrderByDirection().getQuick(i)));
                }
            }
            sort.deriveOutput();
            result = sort;
        }
        return isOrderBound ? bindLimit(result, model, executionContext) : result;
    }

    /**
     * Sorts the projection's input by the ORDER BY, so the projection keeps the order, and designates
     * the projected timestamp.
     */
    LogicalPlan bindSourceOrder(LogicalPlan sourcePlan, ProjectPlan project, QueryModel source, CharSequence scopeAlias) throws SqlException {
        if (source.getOrderBy().size() == 0) {
            return designateTimestamp(project);
        }
        final SortPlan sort = ctx.planNodes.sorts.next().of(project.getInput(), source.getOrderByPosition());
        for (int i = 0, n = source.getOrderBy().size(); i < n; i++) {
            final ExpressionNode order = source.getOrderBy().getQuick(i);
            final int projectionIndex = orderProjectionIndex(order, project.getOutput(), project.getExpressions().size());
            final int index;
            if (projectionIndex >= 0) {
                index = orderSourceIndex(project, projectionIndex, sourcePlan.getOutput());
            } else {
                if (order.type != ExpressionNode.LITERAL) {
                    throw new IllegalStateException("source order over an unresolved expression");
                }
                index = ctx.bindColumnIndex(order, sourcePlan.getOutput(), scopeAlias);
            }
            final int type = sourcePlan.getOutput().getColumnType(index);
            if (!ColumnType.isComparable(type)) {
                throw SqlException.$(order.position, ColumnType.nameOf(type))
                        .put(" is not a supported type in ORDER BY clause");
            }
            final int columnId = sourcePlan.getOutput().getColumnId(index);
            if (!sort.getColumnIds().contains(columnId)) {
                sort.getColumnIds().add(columnId);
                sort.getDirections().add(SqlBinder.sortDirection(source.getOrderByDirection().getQuick(i)));
            }
        }
        sort.deriveOutput();
        if (sort.getInput() instanceof WindowPlan window) {
            window.markSelectOrdered();
        }
        project.replaceInput(0, sort);
        final int orderedIndex = orderedTimestampIndex(project, source);
        if (orderedIndex >= 0) {
            designateTimestamp(project, orderedIndex);
        } else {
            // Source ordering needed a hidden output absent from the visible projection;
            // projecting the same value through another alias does not retain its designation.
            project.getOutput().setTimestampIndex(-1);
        }
        return project;
    }

    /**
     * Sorts the block's output that SUBSAMPLE then samples.
     */
    LogicalPlan bindSubsampleInputOrder(
            QueryModel model, LogicalPlan sourcePlan, ProjectPlan project, QueryModel source, CharSequence scopeAlias,
            SqlExecutionContext executionContext
    ) throws SqlException {
        return bindOutputOrder(model, sourcePlan, project, source, scopeAlias, null, null, executionContext);
    }

    void collectOrderSubstitutions(
            ExpressionNode expression, OutputSchema input, CharSequence scopeAlias, ProjectPlan project, int visibleCount,
            boolean isAliasPreferred
    ) throws SqlException {
        final BindScope scope = ctx.scope();
        if (expression == null) {
            return;
        }
        if (expression.type == ExpressionNode.LITERAL) {
            int index = orderAliasIndex(expression, input, project, visibleCount, isAliasPreferred);
            if (index < 0) {
                final int sourceIndex = ctx.bindColumnIndex(expression, input, scopeAlias);
                final int columnId = input.getColumnId(sourceIndex);
                for (int i = 0, n = project.getExpressions().size(); i < n; i++) {
                    if (project.getExpressions().getQuick(i) instanceof ColumnExpression column && column.getColumnId() == columnId) {
                        index = i;
                        break;
                    }
                }
                if (index < 0) {
                    index = project.getExpressions().size();
                    ctx.addProjection(project, ctx.planNodes.columns.next().of(columnId, input.getColumnType(sourceIndex), expression.position),
                            input.getMetadata(sourceIndex), input.getColumnName(sourceIndex), false);
                }
            }
            final OutputSchema output = project.getOutput();
            scope.substitutionNodes.add(expression);
            scope.substitutionColumns.add(ctx.planNodes.columns.next().of(output.getColumnId(index), output.getColumnType(index), expression.position));
            return;
        }
        if (expression.paramCount < 3) {
            collectOrderSubstitutions(expression.lhs, input, scopeAlias, project, visibleCount, isAliasPreferred);
            collectOrderSubstitutions(expression.rhs, input, scopeAlias, project, visibleCount, isAliasPreferred);
        } else {
            for (int i = 0, n = expression.args.size(); i < n; i++) {
                collectOrderSubstitutions(expression.args.getQuick(i), input, scopeAlias, project, visibleCount, isAliasPreferred);
            }
        }
    }

    /**
     * Designates the projected timestamp of a projection that keeps its input's order.
     */
    ProjectPlan designateTimestamp(ProjectPlan project) {
        designateTimestamp(project, -1);
        return project;
    }

    LogicalPlan projectBeforeDeclaredOrder(LogicalPlan input) {
        if (!(input instanceof ProjectPlan project) || !(project.getInput() instanceof SortPlan sort) || project.hasTimestampDeclaration()) {
            return input;
        }
        for (int i = 0, n = project.getExpressions().size(); i < n; i++) {
            if (!(project.getExpressions().getQuick(i) instanceof ColumnExpression)) {
                return input;
            }
        }
        for (int i = 0, n = sort.getColumnIds().size(); i < n; i++) {
            if (LogicalPlans.projectedColumnIndex(project, sort.getColumnIds().getQuick(i)) < 0) {
                return input;
            }
        }
        // Preserve the declared source's translated sort keys and narrow its
        // record before sorting. Every key must survive this pure projection.
        for (int i = 0, n = sort.getColumnIds().size(); i < n; i++) {
            final int index = LogicalPlans.projectedColumnIndex(project, sort.getColumnIds().getQuick(i));
            sort.getColumnIds().setQuick(i, project.getOutput().getColumnId(index));
        }
        project.replaceInput(0, sort.getInput());
        project.getOutput().setTimestampIndex(LogicalPlans.projectedColumnIndex(project, project.getInput().getOutput().getTimestampColumnId()));
        sort.replaceInput(0, project);
        return sort;
    }

    /**
     * Projects the first {@code visibleCount} columns of a sort whose input also carries hidden ORDER BY keys.
     */
    ProjectPlan projectVisible(SortPlan sort, OutputSchema output, ProjectPlan project, int visibleCount) {
        final ProjectPlan visible = ctx.planNodes.projects.next().of(sort, project.getPosition());
        for (int i = 0; i < visibleCount; i++) {
            visible.getExpressions().add(ctx.planNodes.columns.next().of(output.getColumnId(i), output.getColumnType(i), project.getExpressions().getQuick(i).getPosition()));
            visible.getOutput().add(ctx.scope().nextColumnId++, output.getColumnName(i), output.getColumnType(i), output.getMetadata(i), output.isVisible(i));
            visible.getOutput().setSymbolTableStatic(visible.getOutput().getColumnCount() - 1, output.isSymbolTableStatic(i));
            ctx.inheritTimestampBinding(output.getColumnId(i), visible.getOutput().getColumnId(i));
        }
        if (sort.getOutput().getTimestampIndex() < visibleCount) {
            visible.getOutput().setTimestampIndex(sort.getOutput().getTimestampIndex());
        }
        return visible;
    }
}
