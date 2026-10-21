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
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.griffin.CharacterStoreEntry;
import io.questdb.griffin.CompilerTemporaries;
import io.questdb.griffin.SqlCompilerImpl;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlUtil;
import io.questdb.griffin.model.ExpressionNode;
import io.questdb.griffin.model.PivotForColumn;
import io.questdb.griffin.model.QueryColumn;
import io.questdb.griffin.model.QueryModel;
import io.questdb.griffin.model.WindowExpression;
import io.questdb.griffin.plan.logical.AggregatePlan;
import io.questdb.griffin.plan.logical.BoundExpression;
import io.questdb.griffin.plan.logical.ColumnExpression;
import io.questdb.griffin.plan.logical.FunctionExpression;
import io.questdb.griffin.plan.logical.JoinPlan;
import io.questdb.griffin.plan.logical.LatestByPlan;
import io.questdb.griffin.plan.logical.LogicalPlan;
import io.questdb.griffin.plan.logical.OutputSchema;
import io.questdb.griffin.plan.logical.ProjectPlan;
import io.questdb.griffin.plan.logical.WindowJoinPlan;
import io.questdb.griffin.plan.logical.WindowJoinStep;
import io.questdb.griffin.plan.logical.WindowPlan;
import io.questdb.griffin.plan.logical.WindowSpec;
import io.questdb.std.CharSequenceHashSet;
import io.questdb.std.Chars;
import io.questdb.std.GenericLexer;
import io.questdb.std.IntList;
import io.questdb.std.Mutable;
import io.questdb.std.ObjList;
import io.questdb.std.ObjectPool;
import io.questdb.std.str.StringSink;

import static io.questdb.griffin.bind.BindContext.sourceAlias;
import static io.questdb.griffin.bind.TemporalJoinBinder.joinIndex;
import static io.questdb.griffin.bind.TemporalJoinBinder.rejectWindowJoinSlaveColumn;

final class PivotBinder implements Mutable {
    private final AggregateBinder aggregateBinder;
    private final SqlBinder binder;
    private final SqlCompilerImpl compiler;
    private final CairoConfiguration configuration;
    private final BindContext ctx;
    private final JoinBinder joinBinder;
    private final StringSink pivotValueSink;
    private final CharSequenceHashSet pivotValues;
    private final TemporalJoinBinder temporalJoinBinder;
    private final WindowBinder windowBinder;
    private final ObjectPool<WindowJoinPivotAggregate> windowJoinPivotAggregatePool;

    PivotBinder(
            BindContext ctx,
            SqlBinder binder,
            CairoConfiguration configuration,
            WindowBinder windowBinder,
            TemporalJoinBinder temporalJoinBinder,
            AggregateBinder aggregateBinder,
            JoinBinder joinBinder,
            CompilerTemporaries temporaries,
            SqlCompilerImpl compiler
    ) {
        this.ctx = ctx;
        this.binder = binder;
        this.configuration = configuration;
        this.windowJoinPivotAggregatePool = new ObjectPool<>(WindowJoinPivotAggregate::new, 4, configuration.getSqlColumnPoolCapacity());
        this.windowBinder = windowBinder;
        this.temporalJoinBinder = temporalJoinBinder;
        this.aggregateBinder = aggregateBinder;
        this.joinBinder = joinBinder;
        this.pivotValueSink = temporaries.sink;
        this.pivotValues = temporaries.tokens;
        this.compiler = compiler;
    }

    @Override
    public void clear() {
        windowJoinPivotAggregatePool.clear();
    }

    private static CharSequence pivotAggregateName(ExpressionNode aggregate, ObjList<QueryColumn> measures) {
        for (int i = 0, n = measures.size(); i < n; i++) {
            if (measures.getQuick(i).getAst() == aggregate) {
                return SqlUtil.toColumnName(measures.getQuick(i).getAlias());
            }
        }
        return aggregate.token;
    }

    private static WindowJoinStep windowJoinAggregateStep(WindowJoinPlan plan, int columnId) {
        for (int s = 0, m = plan.getSteps().size(); s < m; s++) {
            final WindowJoinStep step = plan.getSteps().getQuick(s);
            for (int i = 0, n = step.getAggregateColumnIds().size(); i < n; i++) {
                if (step.getAggregateColumnIds().getQuick(i) == columnId) {
                    return step;
                }
            }
        }
        throw new IllegalStateException("window join aggregate not found");
    }

    private static WindowJoinPivotKind windowJoinPivotKind(ExpressionNode call) throws SqlException {
        if (call.paramCount <= 1) {
            if (Chars.equalsIgnoreCase(call.token, "count") || Chars.equalsIgnoreCase(call.token, "sum")
                    || Chars.equalsIgnoreCase(call.token, "min") || Chars.equalsIgnoreCase(call.token, "max")) {
                return WindowJoinPivotKind.MERGE;
            }
            if (call.paramCount == 1) {
                if (Chars.equalsIgnoreCase(call.token, "avg")) {
                    return WindowJoinPivotKind.AVG;
                }
                if (Chars.equalsIgnoreCase(call.token, "first")) {
                    return WindowJoinPivotKind.FIRST;
                }
                if (Chars.equalsIgnoreCase(call.token, "last")) {
                    return WindowJoinPivotKind.LAST;
                }
            }
        }
        throw SqlException.$(call.position, "PIVOT over WINDOW JOIN supports only sum, count, min, max, avg, first and last aggregates");
    }

    private void addPivotKey(AggregatePlan grouped, ExpressionNode key, CharSequence alias, QueryModel input, SqlExecutionContext executionContext) throws SqlException {
        final BindScope bindScope = ctx.scope();
        final CharSequence name = SqlUtil.toColumnName(alias);
        bindScope.pivotKeyAliases.add(name);
        final int existing = aggregateBinder.findGroupingExpression(key, input, grouped);
        if (existing >= 0) {
            bindScope.pivotKeyIndexes.add(existing);
            return;
        }
        bindScope.pivotKeyIndexes.add(grouped.getGroupingExpressions().size());
        final OutputSchema scope = grouped.getChild().getOutput();
        final BoundExpression bound;
        final int index;
        if (key.type == ExpressionNode.LITERAL) {
            index = ctx.bindColumnIndex(key, scope, input);
            bound = ctx.planNodes.columns.next().of(scope.getColumnId(index), scope.getColumnType(index), key.position);
        } else {
            index = -1;
            bound = ctx.functionBinder.bindValue(key, scope, sourceAlias(input), executionContext);
        }
        bindScope.groupingNodes.add(key);
        grouped.getGroupingExpressions().add(bound);
        grouped.getOutput().add(ctx.planNodes.nextColumnId(), name, bound.getDataType(), index < 0 ? null : scope.getMetadata(index), true);
        grouped.getOutput().setSymbolTableStatic(grouped.getOutput().getColumnCount() - 1, index >= 0 && scope.isSymbolTableStatic(index));
    }

    private void addPivotKeyProjection(ProjectPlan project, AggregatePlan grouped, int key, int position) {
        final BindScope scope = ctx.scope();
        final OutputSchema keys = grouped.getOutput();
        final int index = scope.pivotKeyIndexes.getQuick(key);
        addPivotProjection(project, ctx.planNodes.columns.next().of(keys.getColumnId(index), keys.getColumnType(index), position),
                keys.getMetadata(index), scope.pivotKeyAliases.getQuick(key));
        project.getOutput().setSymbolTableStatic(project.getOutput().getColumnCount() - 1, keys.isSymbolTableStatic(index));
    }

    private void addPivotMeasureProjection(ProjectPlan project, QueryColumn measure, QueryModel input, AggregatePlan grouped,
                                           SqlExecutionContext executionContext) throws SqlException {
        addPivotProjection(project, aggregateBinder.bindAggregateOutput(measure.getAst(), input, grouped, executionContext), null,
                SqlUtil.toColumnName(measure.getAlias()));
    }

    private void addPivotProjection(ProjectPlan project, BoundExpression expression, OutputSchema metadata, CharSequence name) {
        project.getExpressions().add(expression);
        project.getOutput().add(ctx.planNodes.nextColumnId(), name, expression.getDataType(), metadata, true);
    }

    private void addPivotSubqueryValues(PivotForColumn column, int combinations, int limit, SqlExecutionContext executionContext) throws SqlException {
        final BindScope scope = ctx.scope();
        final ExpressionNode subquery = column.getSelectSubqueryExpr();
        final int position = subquery.position;
        // The values of the sub-query become output columns, so it is generated and run while binding.
        try (RecordCursorFactory factory = compiler.compileSubqueryFactory(subquery.queryModel, position, executionContext)) {
            final RecordMetadata metadata = factory.getMetadata();
            if (metadata.getColumnCount() != 1) {
                throw SqlException.$(position, "PIVOT IN subquery must return exactly one column, got ").put(metadata.getColumnCount());
            }
            final boolean isQuoted = switch (ColumnType.tagOf(metadata.getColumnType(0))) {
                case ColumnType.SYMBOL, ColumnType.STRING, ColumnType.VARCHAR, ColumnType.TIMESTAMP,
                     ColumnType.DATE, ColumnType.CHAR, ColumnType.UUID, ColumnType.IPv4, ColumnType.ARRAY,
                     ColumnType.LONG128, ColumnType.LONG256 -> true;
                default -> false;
            };
            pivotValues.clear();
            scope.resetAliases();
            try (RecordCursor cursor = factory.getCursor(executionContext)) {
                final Record record = cursor.getRecord();
                while (cursor.hasNext()) {
                    final boolean isNull = SqlUtil.printPivotValue(record, metadata, pivotValueSink, position);
                    final CharacterStoreEntry token = ctx.characterStore.newEntry();
                    if (isQuoted && !isNull) {
                        token.put('\'').put(pivotValueSink).put('\'');
                    } else {
                        token.put(pivotValueSink);
                    }
                    final CharSequence value = token.toImmutable();
                    if (!pivotValues.add(value)) {
                        continue;
                    }
                    final CharSequence alias = SqlUtil.createExprColumnAlias(ctx.characterStore, GenericLexer.unquote(value), scope.aliases,
                            scope.aliasSequences, configuration.getColumnAliasGeneratedMaxSize(), true);
                    scope.aliases.add(alias);
                    column.addValue(ctx.bindingExpressions.next().of(ExpressionNode.CONSTANT, value, 0, position), alias);
                    if ((long) combinations * column.getValueList().size() > limit) {
                        throw SqlException.$(position, "PIVOT produces too many columns: ")
                                .put((long) combinations * column.getValueList().size()).put(", limit is ").put(limit);
                    }
                }
            }
        }
        if (column.getValueList().size() == 0) {
            throw SqlException.$(position, "PIVOT IN subquery returned empty result set");
        }
        column.setSelectSubqueryExpr(null);
        column.setIsValueList(true);
    }

    private void addWindowJoinPivotMerges(AggregatePlan grouped, OutputSchema input, boolean isDirect,
                                          SqlExecutionContext executionContext) throws SqlException {
        final BindScope scope = ctx.scope();
        final OutputSchema output = grouped.getOutput();
        for (int i = 0, n = output.getColumnCount(); i < n; i++) {
            scope.aliases.add(output.getColumnName(i));
        }
        for (int i = 0, n = scope.windowJoinPivotAggregates.size(); i < n; i++) {
            final WindowJoinPivotAggregate aggregate = scope.windowJoinPivotAggregates.getQuick(i);
            if (aggregate.kind == WindowJoinPivotKind.MERGE) {
                continue;
            }
            if (!isDirect) {
                throw SqlException.$(aggregate.position, "PIVOT over WINDOW JOIN supports avg, first and last only as direct measures");
            }
            final FunctionExpression merged = bindWindowJoinPivotMerge(scope.aggregateNodes.getQuick(i), aggregate, true, input, executionContext);
            grouped.getAggregates().add(merged);
            final int columnId = ctx.planNodes.nextColumnId();
            final CharSequence name = SqlUtil.createColumnAlias(ctx.characterStore, "merged", -1, scope.aliases, scope.aliasSequences, false);
            scope.aliases.add(name);
            output.add(columnId, name, merged.getDataType(), false);
            aggregate.merged = ctx.planNodes.columns.next().of(columnId, merged.getDataType(), aggregate.position);
        }
    }

    /**
     * An aggregate over a window function ranks the pivot input first; the measure then
     * aggregates the window output column.
     */
    private LogicalPlan bindPivotWindows(QueryModel pivot, QueryModel windowScope, LogicalPlan source, SqlExecutionContext executionContext) throws SqlException {
        final ObjList<QueryColumn> measures = pivot.getPivotGroupByColumns();
        WindowPlan window = null;
        for (int i = 0, n = measures.size(); i < n; i++) {
            final QueryColumn measure = measures.getQuick(i);
            final ExpressionNode ast = measure.getAst();
            if (ast.windowExpression != null || windowBinder.findWindowPosition(ast) < 0) {
                continue;
            }
            windowBinder.validateAggregateOverWindow(ast);
            if (window == null) {
                window = ctx.planNodes.windowPlans.next().of(source, pivot.getModelPosition());
                window.getOutput().copyFrom(source.getOutput());
            }
            measure.of(measure.getAlias(), replacePivotWindows(ExpressionNode.deepClone(ctx.bindingExpressions, ast), source, window, pivot, windowScope, executionContext),
                    measure.isIncludeIntoWildcard(), measure.getColumnType());
        }
        return window == null ? source : window;
    }

    private void bindWindowJoinPivotAggregates(QueryModel pivot, WindowJoinPlan plan, ExpressionNode windowAggregates,
                                               SqlExecutionContext executionContext) throws SqlException {
        final BindScope bindScope = ctx.scope();
        bindScope.substitutionNodes.clear();
        bindScope.substitutionColumns.clear();
        temporalJoinBinder.collectWindowJoinAggregateOccurrences(windowAggregates, plan, bindScope.substitutionNodes, bindScope.substitutionColumns);
        bindScope.clearAggregateNodes();
        final ObjList<QueryColumn> measures = pivot.getPivotGroupByColumns();
        for (int i = 0, n = measures.size(); i < n; i++) {
            aggregateBinder.collectAggregateNodes(measures.getQuick(i).getAst(), true);
        }
        bindScope.windowJoinPivotAggregates.clear();
        for (int i = 0, n = bindScope.aggregateNodes.size(), next = 0; i < n; i++) {
            final ExpressionNode call = bindScope.aggregateNodes.getQuick(i);
            final WindowJoinPivotKind kind = windowJoinPivotKind(call);
            final ColumnExpression value = bindScope.substitutionColumns.getQuick(next++);
            final ColumnExpression count = kind == WindowJoinPivotKind.MERGE ? null : bindScope.substitutionColumns.getQuick(next++);
            if (kind == WindowJoinPivotKind.AVG) {
                final OutputSchema scope = windowJoinAggregateStep(plan, count.getColumnId()).getScope();
                final int type = ColumnType.tagOf(ctx.functionBinder.bind(call.rhs, scope, null, executionContext).getDataType());
                if (type != ColumnType.DOUBLE && type != ColumnType.FLOAT && type != ColumnType.INT
                        && type != ColumnType.LONG && type != ColumnType.SHORT) {
                    throw SqlException.$(call.position, "PIVOT over WINDOW JOIN supports avg only over DOUBLE, FLOAT, INT, LONG and SHORT values");
                }
            }
            bindScope.windowJoinPivotAggregates.add(windowJoinPivotAggregatePool.next().of(kind, value, count, call.position));
        }
    }

    private FunctionExpression bindWindowJoinPivotMerge(ExpressionNode call, WindowJoinPivotAggregate aggregate, boolean isHidden,
                                                        OutputSchema input, SqlExecutionContext executionContext) throws SqlException {
        final BindScope scope = ctx.scope();
        final int position = call.position;
        scope.substitutionNodes.clear();
        scope.substitutionColumns.clear();
        final ExpressionNode argument;
        final CharSequence merge;
        switch (aggregate.kind) {
            case AVG -> {
                merge = "sum";
                argument = windowJoinPivotPlaceholder(isHidden ? aggregate.count : aggregate.value, position);
            }
            case FIRST, LAST -> {
                merge = aggregate.kind == WindowJoinPivotKind.FIRST ? "first_not_null" : "last_not_null";
                argument = isHidden ? windowJoinPivotNullFlag(aggregate, position) : windowJoinPivotFirstLastValue(aggregate, position);
            }
            default -> {
                merge = Chars.equalsIgnoreCase(call.token, "count") ? "sum" : call.token;
                argument = windowJoinPivotPlaceholder(aggregate.value, position);
            }
        }
        final ExpressionNode node = ctx.bindingExpressions.next().of(ExpressionNode.FUNCTION, merge, 0, position);
        node.paramCount = 1;
        node.rhs = argument;
        final BoundExpression bound = ctx.functionBinder.bindGroupByExpression(node, input, scope.substitutionNodes, scope.substitutionColumns, executionContext);
        if (!(bound instanceof FunctionExpression function) || !function.isAggregate()) {
            throw SqlException.$(position, "expected aggregate function [col=").put(call).put(']');
        }
        return function;
    }

    /**
     * Advances the IN value index of each FOR column to the next combination, the last column fastest.
     */
    private void nextPivotCombination(ObjList<PivotForColumn> forColumns) {
        final IntList indexes = ctx.scope().pivotIndexes;
        for (int i = forColumns.size() - 1; i >= 0; i--) {
            final int next = indexes.getQuick(i) + 1;
            if (next < forColumns.getQuick(i).getValueList().size()) {
                indexes.setQuick(i, next);
                return;
            }
            indexes.setQuick(i, 0);
        }
    }

    private CharSequence pivotAlias(ExpressionNode expression) {
        final BindScope scope = ctx.scope();
        final CharSequence alias = SqlUtil.createExprColumnAlias(ctx.characterStore, expression, scope.aliases, scope.aliasSequences,
                configuration.getColumnAliasGeneratedMaxSize());
        scope.aliases.add(alias);
        return alias;
    }

    private ExpressionNode pivotFilter(QueryModel pivot) {
        ExpressionNode filter = null;
        final ObjList<PivotForColumn> forColumns = pivot.getPivotForColumns();
        for (int i = 0, n = forColumns.size(); i < n; i++) {
            final PivotForColumn column = forColumns.getQuick(i);
            final ExpressionNode expression = column.getInExpr();
            final ObjList<ExpressionNode> values = column.getValueList();
            final ExpressionNode in = ctx.bindingExpressions.next().of(ExpressionNode.FUNCTION, "IN", 0, expression.position);
            in.paramCount = values.size() + 1;
            if (in.paramCount == 2) {
                in.lhs = expression;
                in.rhs = values.getQuick(0);
            } else {
                in.args.addReverseAll(values);
                in.args.add(expression);
            }
            filter = filter == null ? in : ctx.binaryNode(ExpressionNode.OPERATION, "and", 0, filter, in, 0);
        }
        return filter;
    }

    private ExpressionNode pivotMeasure(ObjList<PivotForColumn> forColumns, QueryColumn measure, ExpressionNode measured, int measuredType) {
        final BindScope scope = ctx.scope();
        final int position = measure.getAst().position;
        final boolean isZeroOnEmpty = SqlUtil.isZeroOnEmptyAggregate(measure.getAst());
        final ExpressionNode otherwise = ctx.bindingExpressions.next().of(ExpressionNode.CONSTANT, isZeroOnEmpty ? "0" : "null", 0, position);
        final ExpressionNode value = widenNarrowMeasure(measured, measuredType, position);
        final ExpressionNode choice;
        if (forColumns.size() == 1) {
            choice = ctx.bindingExpressions.next().of(ExpressionNode.FUNCTION, "switch", 0, position);
            choice.paramCount = 4;
            choice.args.add(otherwise);
            choice.args.add(value);
            choice.args.add(forColumns.getQuick(0).getValueList().getQuick(scope.pivotIndexes.getQuick(0)));
            choice.args.add(ctx.bindingExpressions.next().of(ExpressionNode.LITERAL, quotedPivotName(scope.pivotForAliases.getQuick(0)), 0, position));
        } else {
            ExpressionNode condition = null;
            for (int i = 0, n = forColumns.size(); i < n; i++) {
                final ExpressionNode equals = ctx.binaryNode(ExpressionNode.OPERATION, "=", 0,
                        ctx.bindingExpressions.next().of(ExpressionNode.LITERAL, quotedPivotName(scope.pivotForAliases.getQuick(i)), 0, position),
                        forColumns.getQuick(i).getValueList().getQuick(scope.pivotIndexes.getQuick(i)), position);
                condition = condition == null ? equals : ctx.binaryNode(ExpressionNode.OPERATION, "and", 0, condition, equals, 0);
            }
            choice = ctx.bindingExpressions.next().of(ExpressionNode.FUNCTION, "case", 0, position);
            choice.paramCount = 3;
            choice.args.add(otherwise);
            choice.args.add(value);
            choice.args.add(condition);
        }
        final ExpressionNode aggregate = ctx.bindingExpressions.next().of(ExpressionNode.FUNCTION, isZeroOnEmpty ? "sum" : "first_not_null", 0, position);
        aggregate.paramCount = 1;
        aggregate.rhs = choice;
        return aggregate;
    }

    private int preparePivotValues(QueryModel pivot, SqlExecutionContext executionContext) throws SqlException {
        final ObjList<PivotForColumn> forColumns = pivot.getPivotForColumns();
        final int limit = configuration.getSqlPivotMaxProducedColumns();
        int combinations = 1;
        for (int i = 0, n = forColumns.size(); i < n; i++) {
            final PivotForColumn column = forColumns.getQuick(i);
            if (!column.isValueList()) {
                addPivotSubqueryValues(column, combinations, limit, executionContext);
                pivot.setCacheable(false);
            }
            combinations *= column.getValueList().size();
            if (combinations > limit) {
                throw SqlException.$(column.getInExpr().position, "PIVOT produces too many columns: ").put(combinations)
                        .put(", limit is ").put(limit);
            }
        }
        final int columnCount = combinations * pivot.getPivotGroupByColumns().size();
        if (columnCount > limit) {
            throw SqlException.$(pivot.getModelPosition(), "PIVOT produces too many columns: ").put(columnCount)
                    .put(", limit is ").put(limit);
        }
        return combinations;
    }

    private CharSequence quotedPivotName(CharSequence name) {
        if (Chars.indexOf(name, '.') < 0 || SqlUtil.isQuoteProtectedAlias(name)) {
            return name;
        }
        final CharacterStoreEntry quoted = ctx.characterStore.newEntry();
        quoted.put('"').put(name).put('"');
        return quoted.toImmutable();
    }

    private ExpressionNode replacePivotWindows(ExpressionNode node, LogicalPlan source, WindowPlan window, QueryModel pivot, QueryModel windowScope,
                                               SqlExecutionContext executionContext) throws SqlException {
        if (node == null) {
            return null;
        }
        if (node.windowExpression != null) {
            final WindowExpression syntax = windowBinder.resolveWindowSyntax(node.windowExpression, windowScope, executionContext);
            node.windowExpression = syntax;
            final QueryModel input = pivot.getNestedModel();
            final OutputSchema scope = source.getOutput();
            final CharSequence alias = sourceAlias(input);
            final WindowSpec spec = ctx.planNodes.windowSpecs.next().of(syntax);
            for (int k = 0, count = syntax.getPartitionBy().size(); k < count; k++) {
                spec.getPartitionBy().add(ctx.functionBinder.bind(syntax.getPartitionBy().getQuick(k), scope, alias, executionContext));
            }
            for (int k = 0, count = syntax.getOrderBy().size(); k < count; k++) {
                final ExpressionNode order = syntax.getOrderBy().getQuick(k);
                if (!(ctx.functionBinder.bind(order, scope, alias, executionContext) instanceof ColumnExpression column)) {
                    throw SqlException.invalidColumn(order.position, order.token);
                }
                spec.addOrderBy(column.getColumnId(), SqlBinder.sortDirection(syntax.getOrderByDirection().getQuick(k)),
                        scope.getColumnName(scope.getColumnIndexById(column.getColumnId())), order.position);
            }
            final FunctionExpression function = windowBinder.bindWindowFunction(node, spec, scope, input, executionContext);
            final int columnId = ctx.planNodes.nextColumnId();
            final CharacterStoreEntry entry = ctx.characterStore.newEntry();
            entry.put("__pivot_window_").put(columnId);
            final CharSequence name = entry.toImmutable();
            window.getFunctions().add(function);
            window.getSpecs().add(spec);
            window.getFunctionColumnIds().add(columnId);
            window.getOutput().add(columnId, name, function.getDataType(), true);
            return ctx.bindingExpressions.next().of(ExpressionNode.LITERAL, name, 0, node.position);
        }
        node.lhs = replacePivotWindows(node.lhs, source, window, pivot, windowScope, executionContext);
        node.rhs = replacePivotWindows(node.rhs, source, window, pivot, windowScope, executionContext);
        for (int i = 0, n = node.args.size(); i < n; i++) {
            node.args.setQuick(i, replacePivotWindows(node.args.getQuick(i), source, window, pivot, windowScope, executionContext));
        }
        return node;
    }

    private void validatePivotColumnNames(QueryModel pivot) throws SqlException {
        final BindScope scope = ctx.scope();
        scope.resetAliases();
        final ObjList<ExpressionNode> groupBy = pivot.getGroupBy();
        for (int i = 0, n = groupBy.size(); i < n; i++) {
            final ExpressionNode key = groupBy.getQuick(i);
            if (key.type == ExpressionNode.CONSTANT) {
                throw SqlException.$(key.position, "cannot use positional group by inside `PIVOT`");
            }
            pivotAlias(key);
        }
        final ObjList<PivotForColumn> forColumns = pivot.getPivotForColumns();
        for (int i = 0, n = forColumns.size(); i < n; i++) {
            pivotAlias(forColumns.getQuick(i).getInExpr());
        }
        final ObjList<QueryColumn> measures = pivot.getPivotGroupByColumns();
        for (int i = 0, n = measures.size(); i < n; i++) {
            final CharSequence name = measures.getQuick(i).getName();
            if (scope.aliases.contains(name)) {
                throw SqlException.duplicateColumn(0, name);
            }
            scope.aliases.add(name);
        }
    }

    /**
     * A PIVOT merge marks an absent measure with NULL, which BYTE and SHORT cannot hold, so such a measure widens
     * to INT, the type the merge returns.
     */
    private ExpressionNode widenNarrowMeasure(ExpressionNode measure, int type, int position) {
        return switch (ColumnType.tagOf(type)) {
            case ColumnType.BYTE, ColumnType.SHORT -> ctx.binaryNode(ExpressionNode.FUNCTION, "cast", 0, measure,
                    ctx.bindingExpressions.next().of(ExpressionNode.CONSTANT, "int", 0, position), position);
            default -> measure;
        };
    }

    private ExpressionNode windowJoinPivotCall(CharSequence name, ExpressionNode argument, int position) {
        final ExpressionNode call = ctx.bindingExpressions.next().of(ExpressionNode.FUNCTION, name, 0, position);
        call.paramCount = argument == null ? 0 : 1;
        call.rhs = argument;
        return call;
    }

    /**
     * The window value a first or last measure merges; a widened BYTE or SHORT value reads NULL for an empty window.
     */
    private ExpressionNode windowJoinPivotFirstLastValue(WindowJoinPivotAggregate aggregate, int position) {
        final ExpressionNode value = windowJoinPivotPlaceholder(aggregate.value, position);
        final ExpressionNode widened = widenNarrowMeasure(value, aggregate.value.getDataType(), position);
        if (widened == value) {
            return value;
        }
        final ExpressionNode isEmpty = ctx.binaryNode(ExpressionNode.OPERATION, "=", 0, windowJoinPivotPlaceholder(aggregate.count, position),
                ctx.bindingExpressions.next().of(ExpressionNode.CONSTANT, "0", 0, position), position);
        final ExpressionNode choice = ctx.bindingExpressions.next().of(ExpressionNode.FUNCTION, "case", 0, position);
        choice.paramCount = 3;
        choice.args.add(widened);
        choice.args.add(ctx.bindingExpressions.next().of(ExpressionNode.CONSTANT, "null", 0, position));
        choice.args.add(isEmpty);
        return choice;
    }

    private ExpressionNode windowJoinPivotInputs(QueryModel pivot) throws SqlException {
        final BindScope scope = ctx.scope();
        scope.clearAggregateNodes();
        final ObjList<QueryColumn> measures = pivot.getPivotGroupByColumns();
        for (int i = 0, n = measures.size(); i < n; i++) {
            aggregateBinder.collectAggregateNodes(measures.getQuick(i).getAst(), true);
        }
        final ExpressionNode aggregates = ctx.bindingExpressions.next().of(ExpressionNode.FUNCTION, "pivot", 0, pivot.getModelPosition());
        for (int i = 0, n = scope.aggregateNodes.size(); i < n; i++) {
            final ExpressionNode call = scope.aggregateNodes.getQuick(i);
            final int position = call.position;
            switch (windowJoinPivotKind(call)) {
                case AVG -> {
                    final ExpressionNode cast = ctx.binaryNode(ExpressionNode.FUNCTION, "cast", 0, call.rhs,
                            ctx.bindingExpressions.next().of(ExpressionNode.CONSTANT, "double", 0, position), position);
                    aggregates.args.add(windowJoinPivotCall("sum", cast, position));
                    aggregates.args.add(windowJoinPivotCall("count", cast, position));
                }
                case FIRST, LAST -> {
                    aggregates.args.add(call);
                    aggregates.args.add(windowJoinPivotCall("count", null, position));
                }
                default -> aggregates.args.add(call);
            }
        }
        aggregates.paramCount = Math.max(3, aggregates.args.size());
        return aggregates;
    }

    private ExpressionNode windowJoinPivotNullFlag(WindowJoinPivotAggregate aggregate, int position) {
        final ExpressionNode isEmpty = ctx.binaryNode(ExpressionNode.OPERATION, "=", 0, windowJoinPivotPlaceholder(aggregate.count, position),
                ctx.bindingExpressions.next().of(ExpressionNode.CONSTANT, "0", 0, position), position);
        final ExpressionNode isNull = ctx.binaryNode(ExpressionNode.OPERATION, "=", 0, windowJoinPivotPlaceholder(aggregate.value, position),
                ctx.bindingExpressions.next().of(ExpressionNode.CONSTANT, "null", 0, position), position);
        final ExpressionNode flag = ctx.bindingExpressions.next().of(ExpressionNode.FUNCTION, "case", 0, position);
        flag.paramCount = 5;
        flag.args.add(ctx.bindingExpressions.next().of(ExpressionNode.CONSTANT, "0", 0, position));
        flag.args.add(ctx.bindingExpressions.next().of(ExpressionNode.CONSTANT, "1", 0, position));
        flag.args.add(isNull);
        flag.args.add(ctx.bindingExpressions.next().of(ExpressionNode.CONSTANT, "null", 0, position));
        flag.args.add(isEmpty);
        return flag;
    }

    private ExpressionNode windowJoinPivotPlaceholder(ColumnExpression column, int position) {
        final BindScope scope = ctx.scope();
        final ExpressionNode placeholder = ctx.bindingExpressions.next().of(ExpressionNode.LITERAL, "merged", 0, position);
        scope.substitutionNodes.add(placeholder);
        scope.substitutionColumns.add(column);
        return placeholder;
    }

    private ExpressionNode windowJoinPivotValue(WindowJoinPivotAggregate aggregate, OutputSchema measured, int measureIndex, int position) {
        final ExpressionNode value = windowJoinPivotPlaceholder(
                ctx.planNodes.columns.next().of(measured.getColumnId(measureIndex), measured.getColumnType(measureIndex), position), position);
        switch (aggregate.kind) {
            case AVG -> {
                return ctx.binaryNode(ExpressionNode.OPERATION, "/", 0, value, windowJoinPivotPlaceholder(aggregate.merged, position), position);
            }
            case FIRST, LAST -> {
                final ExpressionNode isNull = ctx.binaryNode(ExpressionNode.OPERATION, "=", 0, windowJoinPivotPlaceholder(aggregate.merged, position),
                        ctx.bindingExpressions.next().of(ExpressionNode.CONSTANT, "1", 0, position), position);
                final ExpressionNode choice = ctx.bindingExpressions.next().of(ExpressionNode.FUNCTION, "case", 0, position);
                choice.paramCount = 3;
                choice.args.add(value);
                choice.args.add(ctx.bindingExpressions.next().of(ExpressionNode.CONSTANT, "null", 0, position));
                choice.args.add(isNull);
                return choice;
            }
            default -> {
                return value;
            }
        }
    }

    LogicalPlan rewritePivot(QueryModel pivot, SqlExecutionContext executionContext) throws SqlException {
        final BindScope scope = ctx.scope();
        final QueryModel windowScope = scope.namedWindowScope != null && scope.namedWindowScope.getNestedModel() == pivot
                ? scope.namedWindowScope : pivot;
        final QueryModel input = pivot.getNestedModel();
        final int combinations = preparePivotValues(pivot, executionContext);
        validatePivotColumnNames(pivot);
        final ExpressionNode pivotWhere = pivotFilter(pivot);
        final ExpressionNode userWhere = input.getWhereClause() == null ? null
                : SqlUtil.optimiseBooleanNot(ExpressionNode.deepClone(ctx.bindingExpressions, input.getWhereClause()), ctx.bindingExpressions);
        ExpressionNode where = userWhere == null ? pivotWhere : ctx.binaryNode(ExpressionNode.OPERATION, "and", 0, pivotWhere, userWhere, 0);
        LogicalPlan source;
        WindowJoinPlan windowJoin = null;
        if (joinIndex(input, QueryModel.JOIN_WINDOW) > 0) {
            final ExpressionNode windowAggregates = windowJoinPivotInputs(pivot);
            source = windowJoin = temporalJoinBinder.bindWindowJoin(pivot, null, windowAggregates, input, where, executionContext);
            where = null;
            bindWindowJoinPivotAggregates(pivot, windowJoin, windowAggregates, executionContext);
        } else if (joinIndex(input, QueryModel.JOIN_HORIZON) > 0) {
            source = temporalJoinBinder.bindHorizonJoin(pivot, input, where, executionContext);
            where = null;
        } else if (input.getJoinModels().size() > 1) {
            source = joinBinder.bindJoins(input, where, executionContext);
        } else {
            source = binder.bindSource(input, executionContext);
        }
        final LatestByPlan latest = binder.bindSourceLatestBy(source, input);
        if (where != null && !(source instanceof JoinPlan)) {
            final BoundExpression predicate;
            if (userWhere == null) {
                predicate = binder.bindPredicate(pivotWhere, source, input, executionContext);
            } else {
                final BoundExpression pivotPredicate = binder.bindConjuncts(pivotWhere, source, input, executionContext);
                predicate = ctx.functionBinder.toBooleanSubquery(ctx.expressionRewriter.combineConjunction(pivotPredicate,
                        binder.bindConjuncts(userWhere, source, input, executionContext), where.position));
            }
            source = ctx.planNodes.filters.next().of(source, predicate, predicate.getPosition());
        }
        if (latest != null) {
            latest.replaceInput(0, source);
            source = latest;
        }
        source = bindPivotWindows(pivot, windowScope, source, executionContext);

        final AggregatePlan grouped = ctx.aggregate(source, pivot.getModelPosition());
        grouped.setExplicitGrouping(true);
        scope.groupingNodes.clear();
        scope.clearAggregateNodes();
        scope.resetAliases();
        scope.pivotKeyAliases.clear();
        scope.pivotKeyIndexes.clear();
        final ObjList<ExpressionNode> groupBy = pivot.getGroupBy();
        for (int i = 0, n = groupBy.size(); i < n; i++) {
            final ExpressionNode key = groupBy.getQuick(i);
            if (windowJoin != null) {
                rejectWindowJoinSlaveColumn(key, windowJoin.getOutput(), windowJoin);
            }
            addPivotKey(grouped, key, pivotAlias(key), input, executionContext);
        }
        scope.pivotForAliases.clear();
        final ObjList<PivotForColumn> forColumns = pivot.getPivotForColumns();
        for (int i = 0, n = forColumns.size(); i < n; i++) {
            final ExpressionNode expression = forColumns.getQuick(i).getInExpr();
            final CharSequence alias = pivotAlias(expression);
            scope.pivotForAliases.add(alias);
            addPivotKey(grouped, expression, alias, input, executionContext);
        }
        final int keyCount = grouped.getGroupingExpressions().size();
        final ObjList<QueryColumn> measures = pivot.getPivotGroupByColumns();
        for (int i = 0, n = measures.size(); i < n; i++) {
            final ExpressionNode measure = measures.getQuick(i).getAst();
            final int windowPosition = windowBinder.findUnaggregatedWindowFunctionPosition(measure);
            if (windowPosition >= 0) {
                throw WindowBinder.windowInAggregation(windowPosition);
            }
            aggregateBinder.collectAggregateNodes(measure, true);
        }
        for (int i = 0, n = scope.aggregateNodes.size(); i < n; i++) {
            final ExpressionNode call = scope.aggregateNodes.getQuick(i);
            final BoundExpression bound = windowJoin != null
                    ? bindWindowJoinPivotMerge(call, scope.windowJoinPivotAggregates.getQuick(i), false, source.getOutput(), executionContext)
                    : ctx.functionBinder.bindGroupByExpression(call, source.getOutput(), sourceAlias(input), executionContext);
            if (!(bound instanceof FunctionExpression function) || !function.isAggregate()) {
                throw SqlException.$(call.position, "expected aggregate function [col=").put(call).put(']');
            }
            grouped.getAggregates().add(function);
        }
        boolean isDirect = scope.aggregateNodes.size() == measures.size() && scope.pivotKeyIndexes.size() == keyCount
                && keyCount == grouped.getGroupingExpressions().size();
        for (int i = 0, n = measures.size(); i < n && isDirect; i++) {
            isDirect = scope.aggregateNodes.getQuick(i) == measures.getQuick(i).getAst();
        }
        for (int i = 0, n = scope.aggregateNodes.size(); i < n; i++) {
            final CharSequence name = isDirect ? SqlUtil.toColumnName(measures.getQuick(i).getAlias()) : pivotAggregateName(scope.aggregateNodes.getQuick(i), measures);
            grouped.getOutput().add(ctx.planNodes.nextColumnId(), name, grouped.getAggregates().getQuick(i).getDataType(), isDirect);
        }
        if (windowJoin != null) {
            addWindowJoinPivotMerges(grouped, source.getOutput(), isDirect, executionContext);
        }

        LogicalPlan measured = grouped;
        final int firstMeasureIndex = groupBy.size() + forColumns.size();
        if (!isDirect) {
            final ProjectPlan project = ctx.planNodes.projects.next().of(grouped, pivot.getModelPosition());
            for (int i = 0, n = scope.pivotKeyIndexes.size(); i < n; i++) {
                addPivotKeyProjection(project, grouped, i, pivot.getModelPosition());
            }
            for (int i = 0, n = measures.size(); i < n; i++) {
                addPivotMeasureProjection(project, measures.getQuick(i), input, grouped, executionContext);
            }
            measured = project;
        }

        final AggregatePlan pivoted = ctx.planNodes.aggregates.next().of(measured, pivot.getModelPosition());
        pivoted.setExplicitGrouping(groupBy.size() > 0);
        final OutputSchema measuredOutput = measured.getOutput();
        for (int i = 0, n = groupBy.size(); i < n; i++) {
            pivoted.getGroupingExpressions().add(ctx.planNodes.columns.next().of(measuredOutput.getColumnId(i), measuredOutput.getColumnType(i), groupBy.getQuick(i).position));
            pivoted.getOutput().add(ctx.planNodes.nextColumnId(), measuredOutput.getColumnName(i), measuredOutput.getColumnType(i), measuredOutput.getMetadata(i), true);
        }
        scope.pivotIndexes.setAll(forColumns.size(), 0);
        for (int combination = 0; combination < combinations; combination++) {
            for (int k = 0, m = measures.size(); k < m; k++) {
                final QueryColumn measure = measures.getQuick(k);
                final int measureIndex = firstMeasureIndex + k;
                final int position = measure.getAst().position;
                scope.substitutionNodes.clear();
                scope.substitutionColumns.clear();
                final ExpressionNode value = windowJoin != null && isDirect
                        ? windowJoinPivotValue(scope.windowJoinPivotAggregates.getQuick(k), measuredOutput, measureIndex, position)
                        : ctx.bindingExpressions.next().of(ExpressionNode.LITERAL, quotedPivotName(measuredOutput.getColumnName(measureIndex)), 0, position);
                final ExpressionNode call = pivotMeasure(forColumns, measure, value, measuredOutput.getColumnType(measureIndex));
                final BoundExpression bound = ctx.functionBinder.bindGroupByExpression(call, measuredOutput, scope.substitutionNodes, scope.substitutionColumns, executionContext);
                pivoted.getAggregates().add((FunctionExpression) bound);
            }
            nextPivotCombination(forColumns);
        }
        scope.pivotIndexes.setAll(forColumns.size(), 0);
        for (int combination = 0; combination < combinations; combination++) {
            for (int k = 0, m = measures.size(); k < m; k++) {
                final CharacterStoreEntry name = ctx.characterStore.newEntry();
                for (int i = 0, n = forColumns.size(); i < n; i++) {
                    if (i > 0) {
                        name.put('_');
                    }
                    name.put(SqlUtil.toColumnName(forColumns.getQuick(i).getValueAliases().getQuick(scope.pivotIndexes.getQuick(i))));
                }
                if (!pivot.isPivotGroupByColumnHasNoAlias()) {
                    name.put('_').put(SqlUtil.toColumnName(measures.getQuick(k).getAlias()));
                }
                final int index = pivoted.getOutput().getColumnCount() - groupBy.size();
                final CharSequence columnName = name.toImmutable();
                pivoted.getOutput().add(ctx.planNodes.nextColumnId(), columnName, pivoted.getAggregates().getQuick(index).getDataType(), true);
                if (SqlUtil.protectColumnAlias(ctx.characterStore, columnName) != columnName) {
                    pivoted.getOutput().protectName(pivoted.getOutput().getColumnCount() - 1);
                }
            }
            nextPivotCombination(forColumns);
        }
        return pivoted;
    }

    static final class WindowJoinPivotAggregate implements Mutable {
        private ColumnExpression count;
        private WindowJoinPivotKind kind;
        private ColumnExpression merged;
        private int position;
        private ColumnExpression value;

        @Override
        public void clear() {
            count = null;
            kind = null;
            merged = null;
            value = null;
        }

        private WindowJoinPivotAggregate of(WindowJoinPivotKind kind, ColumnExpression value, ColumnExpression count, int position) {
            this.kind = kind;
            this.value = value;
            this.count = count;
            this.merged = null;
            this.position = position;
            return this;
        }
    }

    /**
     * How a PIVOT measure over a WINDOW JOIN merges the per-row window aggregates into its groups: as the same
     * aggregate (count as a sum), or, for avg, first and last, through a hidden count of the window rows.
     */
    private enum WindowJoinPivotKind {
        AVG, FIRST, LAST, MERGE
    }
}
