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
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.griffin.model.ExpressionNode;
import io.questdb.griffin.model.QueryModel;
import io.questdb.griffin.model.PivotForColumn;
import io.questdb.griffin.model.QueryColumn;
import io.questdb.griffin.model.WindowExpression;
import io.questdb.griffin.plan.logical.AggregatePlan;
import io.questdb.griffin.plan.logical.BoundExpression;
import io.questdb.griffin.plan.logical.ColumnExpression;
import io.questdb.griffin.plan.logical.FilterPlan;
import io.questdb.griffin.plan.logical.FunctionExpression;
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
import io.questdb.std.LowerCaseCharSequenceHashSet;
import io.questdb.std.LowerCaseCharSequenceIntHashMap;
import io.questdb.std.Mutable;
import io.questdb.std.ObjList;
import io.questdb.std.ObjectPool;
import io.questdb.std.str.StringSink;

import static io.questdb.griffin.BindContext.findGroupingColumn;
import static io.questdb.griffin.BindContext.sourceAlias;
import static io.questdb.griffin.TemporalJoinBinder.horizonJoinIndex;
import static io.questdb.griffin.TemporalJoinBinder.rejectWindowJoinSlaveColumn;
import static io.questdb.griffin.TemporalJoinBinder.windowJoinIndex;

final class PivotBinder implements Mutable {
    private final AggregateBinder aggregateBinder;
    private final SqlBinder binder;
    private final BindContext ctx;
    private final JoinBinder joinBinder;
    private final LateralBinder lateralBinder;
    private final LowerCaseCharSequenceHashSet pivotAliases;
    private final LowerCaseCharSequenceIntHashMap pivotAliasSequences;
    private final ObjList<CharSequence> pivotForAliases = new ObjList<>();
    private final IntList pivotIndexes;
    private final ObjList<CharSequence> pivotKeyAliases;
    private final IntList pivotKeyIndexes;
    private final CharSequenceHashSet pivotValues = new CharSequenceHashSet();
    private final StringSink pivotValueSink;
    private final TemporalJoinBinder temporalJoinBinder;
    private final WindowBinder windowBinder;
    private final ObjectPool<WindowJoinPivotAggregate> windowJoinPivotAggregatePool = new ObjectPool<>(WindowJoinPivotAggregate::new, 4);
    private final ObjList<WindowJoinPivotAggregate> windowJoinPivotAggregates = new ObjList<>();

    PivotBinder(
            BindContext ctx,
            SqlBinder binder,
            WindowBinder windowBinder,
            TemporalJoinBinder temporalJoinBinder,
            AggregateBinder aggregateBinder,
            JoinBinder joinBinder,
            LateralBinder lateralBinder,
            StringSink pivotValueSink
    ) {
        this.ctx = ctx;
        this.binder = binder;
        this.windowBinder = windowBinder;
        this.temporalJoinBinder = temporalJoinBinder;
        this.aggregateBinder = aggregateBinder;
        this.joinBinder = joinBinder;
        this.lateralBinder = lateralBinder;
        this.pivotValueSink = pivotValueSink;
        this.pivotAliases = ctx.aliases;
        this.pivotAliasSequences = ctx.aliasSequences;
        this.pivotIndexes = ctx.projectionAliasIndexes;
        this.pivotKeyAliases = windowBinder.windowNames;
        this.pivotKeyIndexes = ctx.sourceProjectionIndexes;
    }

    @Override
    public void clear() {
        windowJoinPivotAggregatePool.clear();
        windowJoinPivotAggregates.clear();
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

    private static int windowJoinPivotKind(ExpressionNode call) throws SqlException {
        if (call.paramCount <= 1) {
            if (Chars.equalsIgnoreCase(call.token, "count") || Chars.equalsIgnoreCase(call.token, "sum")
                    || Chars.equalsIgnoreCase(call.token, "min") || Chars.equalsIgnoreCase(call.token, "max")) {
                return WindowJoinPivotAggregate.MERGE;
            }
            if (call.paramCount == 1) {
                if (Chars.equalsIgnoreCase(call.token, "avg")) {
                    return WindowJoinPivotAggregate.AVG;
                }
                if (Chars.equalsIgnoreCase(call.token, "first")) {
                    return WindowJoinPivotAggregate.FIRST;
                }
                if (Chars.equalsIgnoreCase(call.token, "last")) {
                    return WindowJoinPivotAggregate.LAST;
                }
            }
        }
        throw SqlException.$(call.position, "PIVOT over WINDOW JOIN supports only sum, count, min, max, avg, first and last aggregates");
    }

    private void addPivotKey(AggregatePlan grouped, ExpressionNode key, CharSequence alias, QueryModel input, SqlExecutionContext executionContext) throws SqlException {
        final CharSequence name = SqlUtil.toColumnName(alias);
        pivotKeyAliases.add(name);
        final int existing = aggregateBinder.findGroupingExpression(key, input, grouped);
        if (existing >= 0) {
            pivotKeyIndexes.add(existing);
            return;
        }
        pivotKeyIndexes.add(grouped.getGroupingExpressions().size());
        final OutputSchema scope = grouped.getInput().getOutput();
        final BoundExpression bound;
        final int index;
        if (key.type == ExpressionNode.LITERAL) {
            index = ctx.bindColumnIndex(key, scope, input);
            bound = ctx.columns.next().of(scope.getColumnId(index), scope.getColumnType(index), key.position);
        } else {
            index = -1;
            bound = ctx.functionBinder.bind(key, scope, sourceAlias(input), executionContext);
        }
        ctx.groupingNodes.add(key);
        grouped.getGroupingExpressions().add(bound);
        grouped.getOutput().add(ctx.nextColumnId++, name, bound.getDataType(), index < 0 ? null : scope.getMetadata(index), true);
        grouped.getOutput().setSymbolTableStatic(grouped.getOutput().getColumnCount() - 1, index >= 0 && scope.isSymbolTableStatic(index));
    }

    private void addPivotKeyProjection(ProjectPlan project, AggregatePlan grouped, int key, int position) {
        final OutputSchema keys = grouped.getOutput();
        final int index = pivotKeyIndexes.getQuick(key);
        addPivotProjection(project, ctx.columns.next().of(keys.getColumnId(index), keys.getColumnType(index), position),
                keys.getMetadata(index), pivotKeyAliases.getQuick(key));
        project.getOutput().setSymbolTableStatic(project.getOutput().getColumnCount() - 1, keys.isSymbolTableStatic(index));
    }

    private void addPivotMeasureProjection(ProjectPlan project, QueryColumn measure, QueryModel input, AggregatePlan grouped,
                                           SqlExecutionContext executionContext) throws SqlException {
        addPivotProjection(project, aggregateBinder.bindAggregateOutput(measure.getAst(), input, grouped, executionContext), null,
                SqlUtil.toColumnName(measure.getAlias()));
    }

    private void addPivotProjection(ProjectPlan project, BoundExpression expression, OutputSchema metadata, CharSequence name) {
        project.getExpressions().add(expression);
        project.getOutput().add(ctx.nextColumnId++, name, expression.getDataType(), metadata, true);
    }

    private void addPivotSubqueryValues(PivotForColumn column, int combinations, int limit, SqlExecutionContext executionContext) throws SqlException {
        final ExpressionNode subquery = column.getSelectSubqueryExpr();
        final int position = subquery.position;
        try (RecordCursorFactory factory = binder.claimSubquery(binder.compileSubquery(subquery.queryModel, position, executionContext))) {
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
            pivotAliases.clear();
            pivotAliasSequences.clear();
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
                    final CharSequence alias = SqlUtil.createExprColumnAlias(ctx.characterStore, GenericLexer.unquote(value), pivotAliases,
                            pivotAliasSequences, ctx.configuration.getColumnAliasGeneratedMaxSize(), true);
                    pivotAliases.add(alias);
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
        final OutputSchema output = grouped.getOutput();
        for (int i = 0, n = output.getColumnCount(); i < n; i++) {
            pivotAliases.add(output.getColumnName(i));
        }
        for (int i = 0, n = windowJoinPivotAggregates.size(); i < n; i++) {
            final WindowJoinPivotAggregate aggregate = windowJoinPivotAggregates.getQuick(i);
            if (aggregate.kind == WindowJoinPivotAggregate.MERGE) {
                continue;
            }
            if (!isDirect) {
                throw SqlException.$(aggregate.position, "PIVOT over WINDOW JOIN supports avg, first and last only as direct measures");
            }
            final FunctionExpression merged = bindWindowJoinPivotMerge(ctx.aggregateNodes.getQuick(i), aggregate, true, input, executionContext);
            grouped.getAggregates().add(merged);
            final int columnId = ctx.nextColumnId++;
            final CharSequence name = SqlUtil.createColumnAlias(ctx.characterStore, "merged", -1, pivotAliases, pivotAliasSequences, false);
            pivotAliases.add(name);
            output.add(columnId, name, merged.getDataType(), false);
            aggregate.merged = ctx.columns.next().of(columnId, merged.getDataType(), aggregate.position);
        }
    }

    /**
     * An aggregate over a window function ranks the pivot input first; the measure then
     * aggregates the window output column.
     */
    private LogicalPlan bindPivotWindows(QueryModel pivot, QueryModel input, LogicalPlan source, SqlExecutionContext executionContext) throws SqlException {
        final ObjList<QueryColumn> measures = pivot.getPivotGroupByColumns();
        WindowPlan window = null;
        for (int i = 0, n = measures.size(); i < n; i++) {
            final QueryColumn measure = measures.getQuick(i);
            final ExpressionNode ast = measure.getAst();
            if (ast.windowExpression != null || windowBinder.findWindowPosition(ast, false, false) < 0) {
                continue;
            }
            if (!ctx.isAggregate(ast)) {
                final int aggregatePosition = windowBinder.findAggregateOverWindowPosition(ast);
                if (aggregatePosition >= 0) {
                    throw SqlException.$(aggregatePosition, "Aggregate over window function cannot be combined with other terms. Use a sub-query.");
                }
            }
            if (window == null) {
                window = ctx.windowPlans.next().of(source, pivot.getModelPosition());
                window.getOutput().copyFrom(source.getOutput());
            }
            measure.of(measure.getAlias(), replacePivotWindows(ExpressionNode.deepClone(ctx.bindingExpressions, ast), source, window, input, executionContext),
                    measure.isIncludeIntoWildcard(), measure.getColumnType());
        }
        return window == null ? source : window;
    }

    private void bindWindowJoinPivotAggregates(QueryModel pivot, WindowJoinPlan plan, ExpressionNode windowAggregates,
                                               SqlExecutionContext executionContext) throws SqlException {
        ctx.substitutionNodes.clear();
        ctx.substitutionColumns.clear();
        temporalJoinBinder.collectWindowJoinAggregateOccurrences(windowAggregates, plan, ctx.substitutionNodes, ctx.substitutionColumns);
        ctx.aggregateNodes.clear();
        final ObjList<QueryColumn> measures = pivot.getPivotGroupByColumns();
        for (int i = 0, n = measures.size(); i < n; i++) {
            aggregateBinder.collectAggregateNodes(measures.getQuick(i).getAst(), true);
        }
        windowJoinPivotAggregates.clear();
        for (int i = 0, n = ctx.aggregateNodes.size(), next = 0; i < n; i++) {
            final ExpressionNode call = ctx.aggregateNodes.getQuick(i);
            final int kind = windowJoinPivotKind(call);
            final ColumnExpression value = ctx.substitutionColumns.getQuick(next++);
            final ColumnExpression count = kind == WindowJoinPivotAggregate.MERGE ? null : ctx.substitutionColumns.getQuick(next++);
            if (kind == WindowJoinPivotAggregate.AVG) {
                final OutputSchema scope = windowJoinAggregateStep(plan, count.getColumnId()).getScope();
                final int type = ColumnType.tagOf(ctx.functionBinder.bind(call.rhs, scope, null, executionContext).getDataType());
                if (type != ColumnType.DOUBLE && type != ColumnType.FLOAT && type != ColumnType.INT
                        && type != ColumnType.LONG && type != ColumnType.SHORT) {
                    throw SqlException.$(call.position, "PIVOT over WINDOW JOIN supports avg only over DOUBLE, FLOAT, INT, LONG and SHORT values");
                }
            }
            windowJoinPivotAggregates.add(windowJoinPivotAggregatePool.next().of(kind, value, count, call.position));
        }
    }

    private FunctionExpression bindWindowJoinPivotMerge(ExpressionNode call, WindowJoinPivotAggregate aggregate, boolean isHidden,
                                                        OutputSchema input, SqlExecutionContext executionContext) throws SqlException {
        final int position = call.position;
        ctx.substitutionNodes.clear();
        ctx.substitutionColumns.clear();
        final ExpressionNode argument;
        final CharSequence merge;
        switch (aggregate.kind) {
            case WindowJoinPivotAggregate.AVG -> {
                merge = "sum";
                argument = windowJoinPivotPlaceholder(isHidden ? aggregate.count : aggregate.value, position);
            }
            case WindowJoinPivotAggregate.FIRST, WindowJoinPivotAggregate.LAST -> {
                merge = aggregate.kind == WindowJoinPivotAggregate.FIRST ? "first_not_null" : "last_not_null";
                argument = isHidden ? windowJoinPivotNullFlag(aggregate, position) : windowJoinPivotPlaceholder(aggregate.value, position);
            }
            default -> {
                merge = Chars.equalsIgnoreCase(call.token, "count") ? "sum" : call.token;
                argument = windowJoinPivotPlaceholder(aggregate.value, position);
            }
        }
        final ExpressionNode node = ctx.bindingExpressions.next().of(ExpressionNode.FUNCTION, merge, 0, position);
        node.paramCount = 1;
        node.rhs = argument;
        final BoundExpression bound = ctx.functionBinder.bindGroupByExpression(node, input, ctx.substitutionNodes, ctx.substitutionColumns, executionContext);
        if (!(bound instanceof FunctionExpression function) || !function.isAggregate()) {
            throw SqlException.$(position, "expected aggregate function [col=").put(call).put(']');
        }
        return function;
    }

    private CharSequence pivotAlias(ExpressionNode expression) {
        final CharacterStoreEntry text = ctx.characterStore.newEntry();
        expression.toSink(text);
        final CharSequence alias = SqlUtil.createExprColumnAlias(ctx.characterStore, text.toImmutable(), pivotAliases,
                pivotAliasSequences, ctx.configuration.getColumnAliasGeneratedMaxSize(), true);
        pivotAliases.add(alias);
        return alias;
    }

    private ExpressionNode pivotAnd(ExpressionNode left, ExpressionNode right) {
        final ExpressionNode and = ctx.bindingExpressions.next().of(ExpressionNode.OPERATION, "and", 0, 0);
        and.paramCount = 2;
        and.lhs = left;
        and.rhs = right;
        return and;
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
            filter = filter == null ? in : pivotAnd(filter, in);
        }
        return filter;
    }

    private ExpressionNode pivotMeasure(ObjList<PivotForColumn> forColumns, QueryColumn measure, ExpressionNode value) {
        final int position = measure.getAst().position;
        final boolean isZeroOnEmpty = SqlUtil.isZeroOnEmptyAggregate(measure.getAst());
        final ExpressionNode otherwise = ctx.bindingExpressions.next().of(ExpressionNode.CONSTANT, isZeroOnEmpty ? "0" : "null", 0, position);
        final ExpressionNode choice;
        if (forColumns.size() == 1) {
            choice = ctx.bindingExpressions.next().of(ExpressionNode.FUNCTION, "switch", 0, position);
            choice.paramCount = 4;
            choice.args.add(otherwise);
            choice.args.add(value);
            choice.args.add(forColumns.getQuick(0).getValueList().getQuick(pivotIndexes.getQuick(0)));
            choice.args.add(ctx.bindingExpressions.next().of(ExpressionNode.LITERAL, quotedPivotName(pivotForAliases.getQuick(0)), 0, position));
        } else {
            ExpressionNode condition = null;
            for (int i = 0, n = forColumns.size(); i < n; i++) {
                final ExpressionNode equals = ctx.bindingExpressions.next().of(ExpressionNode.OPERATION, "=", 0, position);
                equals.paramCount = 2;
                equals.lhs = ctx.bindingExpressions.next().of(ExpressionNode.LITERAL, quotedPivotName(pivotForAliases.getQuick(i)), 0, position);
                equals.rhs = forColumns.getQuick(i).getValueList().getQuick(pivotIndexes.getQuick(i));
                condition = condition == null ? equals : pivotAnd(condition, equals);
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
        final int limit = ctx.configuration.getSqlPivotMaxProducedColumns();
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

    private ExpressionNode replacePivotWindows(ExpressionNode node, LogicalPlan source, WindowPlan window, QueryModel input,
                                               SqlExecutionContext executionContext) throws SqlException {
        if (node == null) {
            return null;
        }
        if (node.windowExpression != null) {
            final WindowExpression syntax = node.windowExpression;
            final OutputSchema scope = source.getOutput();
            final CharSequence alias = sourceAlias(input);
            final WindowSpec spec = ctx.windowSpecs.next().of(syntax);
            for (int k = 0, count = syntax.getPartitionBy().size(); k < count; k++) {
                spec.getPartitionBy().add(ctx.functionBinder.bind(syntax.getPartitionBy().getQuick(k), scope, alias, executionContext));
            }
            for (int k = 0, count = syntax.getOrderBy().size(); k < count; k++) {
                final ExpressionNode order = syntax.getOrderBy().getQuick(k);
                if (!(ctx.functionBinder.bind(order, scope, alias, executionContext) instanceof ColumnExpression column)) {
                    throw SqlException.invalidColumn(order.position, order.token);
                }
                spec.getOrderByColumnIds().add(column.getColumnId());
                spec.getOrderByDirections().add(syntax.getOrderByDirection().getQuick(k));
                spec.getOrderByPositions().add(order.position);
                spec.getOrderByNames().add(scope.getColumnName(scope.getColumnIndexById(column.getColumnId())));
            }
            final FunctionExpression function = windowBinder.bindWindowFunction(node, spec, scope, input, executionContext);
            final int columnId = ctx.nextColumnId++;
            final CharacterStoreEntry entry = ctx.characterStore.newEntry();
            entry.put("__pivot_window_").put(columnId);
            final CharSequence name = entry.toImmutable();
            window.getFunctions().add(function);
            window.getSpecs().add(spec);
            window.getFunctionColumnIds().add(columnId);
            window.getOutput().add(columnId, name, function.getDataType(), true);
            return ctx.bindingExpressions.next().of(ExpressionNode.LITERAL, name, 0, node.position);
        }
        node.lhs = replacePivotWindows(node.lhs, source, window, input, executionContext);
        node.rhs = replacePivotWindows(node.rhs, source, window, input, executionContext);
        for (int i = 0, n = node.args.size(); i < n; i++) {
            node.args.setQuick(i, replacePivotWindows(node.args.getQuick(i), source, window, input, executionContext));
        }
        return node;
    }

    private void validatePivotColumnNames(QueryModel pivot) throws SqlException {
        pivotAliases.clear();
        pivotAliasSequences.clear();
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
            if (pivotAliases.contains(name)) {
                throw SqlException.duplicateColumn(0, name);
            }
            pivotAliases.add(name);
        }
    }

    private ExpressionNode windowJoinPivotCall(CharSequence name, ExpressionNode argument, int position) {
        final ExpressionNode call = ctx.bindingExpressions.next().of(ExpressionNode.FUNCTION, name, 0, position);
        call.paramCount = argument == null ? 0 : 1;
        call.rhs = argument;
        return call;
    }

    private ExpressionNode windowJoinPivotInputs(QueryModel pivot) throws SqlException {
        ctx.aggregateNodes.clear();
        final ObjList<QueryColumn> measures = pivot.getPivotGroupByColumns();
        for (int i = 0, n = measures.size(); i < n; i++) {
            aggregateBinder.collectAggregateNodes(measures.getQuick(i).getAst(), true);
        }
        final ExpressionNode aggregates = ctx.bindingExpressions.next().of(ExpressionNode.FUNCTION, "pivot", 0, pivot.getModelPosition());
        for (int i = 0, n = ctx.aggregateNodes.size(); i < n; i++) {
            final ExpressionNode call = ctx.aggregateNodes.getQuick(i);
            final int position = call.position;
            switch (windowJoinPivotKind(call)) {
                case WindowJoinPivotAggregate.AVG -> {
                    final ExpressionNode cast = ctx.bindingExpressions.next().of(ExpressionNode.FUNCTION, "cast", 0, position);
                    cast.paramCount = 2;
                    cast.lhs = call.rhs;
                    cast.rhs = ctx.bindingExpressions.next().of(ExpressionNode.CONSTANT, "double", 0, position);
                    aggregates.args.add(windowJoinPivotCall("sum", cast, position));
                    aggregates.args.add(windowJoinPivotCall("count", cast, position));
                }
                case WindowJoinPivotAggregate.FIRST, WindowJoinPivotAggregate.LAST -> {
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
        final ExpressionNode isEmpty = ctx.bindingExpressions.next().of(ExpressionNode.OPERATION, "=", 0, position);
        isEmpty.paramCount = 2;
        isEmpty.lhs = windowJoinPivotPlaceholder(aggregate.count, position);
        isEmpty.rhs = ctx.bindingExpressions.next().of(ExpressionNode.CONSTANT, "0", 0, position);
        final ExpressionNode isNull = ctx.bindingExpressions.next().of(ExpressionNode.OPERATION, "=", 0, position);
        isNull.paramCount = 2;
        isNull.lhs = windowJoinPivotPlaceholder(aggregate.value, position);
        isNull.rhs = ctx.bindingExpressions.next().of(ExpressionNode.CONSTANT, "null", 0, position);
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
        final ExpressionNode placeholder = ctx.bindingExpressions.next().of(ExpressionNode.LITERAL, "merged", 0, position);
        ctx.substitutionNodes.add(placeholder);
        ctx.substitutionColumns.add(column);
        return placeholder;
    }

    private ExpressionNode windowJoinPivotValue(WindowJoinPivotAggregate aggregate, OutputSchema measured, int measureIndex, int position) {
        final ExpressionNode value = windowJoinPivotPlaceholder(
                ctx.columns.next().of(measured.getColumnId(measureIndex), measured.getColumnType(measureIndex), position), position);
        switch (aggregate.kind) {
            case WindowJoinPivotAggregate.AVG -> {
                final ExpressionNode ratio = ctx.bindingExpressions.next().of(ExpressionNode.OPERATION, "/", 0, position);
                ratio.paramCount = 2;
                ratio.lhs = value;
                ratio.rhs = windowJoinPivotPlaceholder(aggregate.merged, position);
                return ratio;
            }
            case WindowJoinPivotAggregate.FIRST, WindowJoinPivotAggregate.LAST -> {
                final ExpressionNode isNull = ctx.bindingExpressions.next().of(ExpressionNode.OPERATION, "=", 0, position);
                isNull.paramCount = 2;
                isNull.lhs = windowJoinPivotPlaceholder(aggregate.merged, position);
                isNull.rhs = ctx.bindingExpressions.next().of(ExpressionNode.CONSTANT, "1", 0, position);
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
        final QueryModel input = pivot.getNestedModel();
        final int combinations = preparePivotValues(pivot, executionContext);
        validatePivotColumnNames(pivot);
        ExpressionNode where = pivotFilter(pivot);
        if (input.getWhereClause() != null) {
            where = pivotAnd(where, ExpressionNode.deepClone(ctx.bindingExpressions, input.getWhereClause()));
        }
        where = SqlUtil.optimiseBooleanNot(where, ctx.bindingExpressions);
        LogicalPlan source;
        WindowJoinPlan windowJoin = null;
        if (windowJoinIndex(input) > 0) {
            final ExpressionNode windowAggregates = windowJoinPivotInputs(pivot);
            source = windowJoin = temporalJoinBinder.bindWindowJoin(pivot, null, windowAggregates, input, where, executionContext);
            where = null;
            bindWindowJoinPivotAggregates(pivot, windowJoin, windowAggregates, executionContext);
        } else if (horizonJoinIndex(input) > 0) {
            source = temporalJoinBinder.bindHorizonJoin(pivot, input, where, executionContext);
            where = null;
        } else if (input.getJoinModels().size() > 1) {
            source = joinBinder.bindJoins(pivot, input, where, executionContext);
        } else {
            source = binder.bindSource(pivot, input, executionContext);
        }
        if (ctx.lateralScopes.size() > 0 && input.getJoinModels().size() == 1) {
            source = lateralBinder.correlateSource(pivot, input, input.getJoinModels().size() > 1 ? null : sourceAlias(input), source, where, executionContext);
            where = ctx.correlatedWhere;
        }
        final LatestByPlan latest = input.getLatestBy().size() > 0 ? binder.bindLatestBy(source, input) : null;
        final boolean isCorrelatedLatest = latest != null && ctx.lateralScopes.size() > 0 && source.getOutput().getCorrelatedAliasCount() > 0;
        if (where != null && source.getType() != LogicalPlan.Type.JOIN) {
            final BoundExpression predicate = binder.bindPredicate(where, source, input, executionContext);
            final FilterPlan filter = ctx.filters.next().of(source, predicate, predicate.getPosition());
            filter.getOutput().copyFrom(source.getOutput());
            source = filter;
        }
        if (isCorrelatedLatest) {
            source = windowBinder.bindLatestWindow(source, latest, pivot, input.getModelPosition(), executionContext);
        } else if (latest != null) {
            latest.replaceInput(0, source);
            source = latest;
        }
        source = bindPivotWindows(pivot, input, source, executionContext);

        final AggregatePlan grouped = ctx.aggregates.next().of(source, pivot.getModelPosition());
        grouped.setExplicitGrouping(true);
        ctx.groupingNodes.clear();
        ctx.aggregateNodes.clear();
        pivotAliases.clear();
        pivotAliasSequences.clear();
        pivotKeyAliases.clear();
        pivotKeyIndexes.clear();
        final ObjList<ExpressionNode> groupBy = pivot.getGroupBy();
        for (int i = 0, n = groupBy.size(); i < n; i++) {
            final ExpressionNode key = groupBy.getQuick(i);
            if (windowJoin != null) {
                rejectWindowJoinSlaveColumn(key, windowJoin.getOutput(), windowJoin);
            }
            addPivotKey(grouped, key, pivotAlias(key), input, executionContext);
        }
        pivotForAliases.clear();
        final ObjList<PivotForColumn> forColumns = pivot.getPivotForColumns();
        for (int i = 0, n = forColumns.size(); i < n; i++) {
            final ExpressionNode expression = forColumns.getQuick(i).getInExpr();
            final CharSequence alias = pivotAlias(expression);
            pivotForAliases.add(alias);
            addPivotKey(grouped, expression, alias, input, executionContext);
        }
        final int keyCount = grouped.getGroupingExpressions().size();
        lateralBinder.addCorrelatedKeys(grouped, source.getOutput(), pivot);
        final OutputSchema sourceOutput = source.getOutput();
        for (int i = 0, n = sourceOutput.getCorrelatedAliasCount(); i < n; i++) {
            grouped.getOutput().addCorrelatedAlias(sourceOutput.getCorrelatedAliasQualifier(i), sourceOutput.getCorrelatedAliasName(i),
                    findGroupingColumn(grouped, sourceOutput.getColumnId(sourceOutput.getCorrelatedAliasIndex(i))));
        }
        final ObjList<QueryColumn> measures = pivot.getPivotGroupByColumns();
        for (int i = 0, n = measures.size(); i < n; i++) {
            final ExpressionNode measure = measures.getQuick(i).getAst();
            final int windowPosition = measure.windowExpression != null ? measure.position : windowBinder.findWindowPosition(measure, true, true);
            if (windowPosition >= 0) {
                throw SqlException.$(windowPosition, "Window function is not allowed in context of aggregation. Use sub-query.");
            }
            aggregateBinder.collectAggregateNodes(measure, true);
        }
        for (int i = 0, n = ctx.aggregateNodes.size(); i < n; i++) {
            final ExpressionNode call = ctx.aggregateNodes.getQuick(i);
            final BoundExpression bound = windowJoin != null
                    ? bindWindowJoinPivotMerge(call, windowJoinPivotAggregates.getQuick(i), false, source.getOutput(), executionContext)
                    : ctx.functionBinder.bindGroupByExpression(call, source.getOutput(), sourceAlias(input), executionContext);
            if (!(bound instanceof FunctionExpression function) || !function.isAggregate()) {
                throw SqlException.$(call.position, "expected aggregate function [col=").put(call).put(']');
            }
            grouped.getAggregates().add(function);
        }
        boolean isDirect = ctx.aggregateNodes.size() == measures.size() && pivotKeyIndexes.size() == keyCount
                && keyCount == grouped.getGroupingExpressions().size();
        for (int i = 0, n = measures.size(); i < n && isDirect; i++) {
            isDirect = ctx.aggregateNodes.getQuick(i) == measures.getQuick(i).getAst();
        }
        for (int i = 0, n = ctx.aggregateNodes.size(); i < n; i++) {
            final CharSequence name = isDirect ? SqlUtil.toColumnName(measures.getQuick(i).getAlias()) : pivotAggregateName(ctx.aggregateNodes.getQuick(i), measures);
            grouped.getOutput().add(ctx.nextColumnId++, name, grouped.getAggregates().getQuick(i).getDataType(), isDirect);
        }
        if (windowJoin != null) {
            addWindowJoinPivotMerges(grouped, source.getOutput(), isDirect, executionContext);
        }

        LogicalPlan measured = grouped;
        final int firstMeasureIndex = groupBy.size() + forColumns.size();
        if (!isDirect) {
            final ProjectPlan project = ctx.projects.next().of(grouped, pivot.getModelPosition());
            for (int i = 0, n = pivotKeyIndexes.size(); i < n; i++) {
                addPivotKeyProjection(project, grouped, i, pivot.getModelPosition());
            }
            for (int i = 0, n = measures.size(); i < n; i++) {
                addPivotMeasureProjection(project, measures.getQuick(i), input, grouped, executionContext);
            }
            lateralBinder.exposeCorrelated(project, grouped.getOutput(), null);
            measured = project;
        }

        final AggregatePlan pivoted = ctx.aggregates.next().of(measured, pivot.getModelPosition());
        pivoted.setExplicitGrouping(groupBy.size() > 0);
        final OutputSchema measuredOutput = measured.getOutput();
        for (int i = 0, n = groupBy.size(); i < n; i++) {
            pivoted.getGroupingExpressions().add(ctx.columns.next().of(measuredOutput.getColumnId(i), measuredOutput.getColumnType(i), groupBy.getQuick(i).position));
            pivoted.getOutput().add(ctx.nextColumnId++, measuredOutput.getColumnName(i), measuredOutput.getColumnType(i), measuredOutput.getMetadata(i), true);
        }
        for (int i = 0, n = measuredOutput.getCorrelatedAliasCount(); i < n; i++) {
            final int index = measuredOutput.getCorrelatedAliasIndex(i);
            pivoted.getGroupingExpressions().add(ctx.columns.next().of(measuredOutput.getColumnId(index), measuredOutput.getColumnType(index), pivot.getModelPosition()));
            pivoted.getOutput().add(ctx.nextColumnId, lateralBinder.correlatedName(), measuredOutput.getColumnType(index), measuredOutput.getMetadata(index), false);
            ctx.nextColumnId++;
            pivoted.getOutput().addCorrelatedAlias(measuredOutput.getCorrelatedAliasQualifier(i), measuredOutput.getCorrelatedAliasName(i),
                    pivoted.getOutput().getColumnCount() - 1);
        }
        pivotIndexes.setAll(forColumns.size(), 0);
        for (int combination = 0; combination < combinations; combination++) {
            for (int k = 0, m = measures.size(); k < m; k++) {
                final QueryColumn measure = measures.getQuick(k);
                final int measureIndex = firstMeasureIndex + k;
                final int position = measure.getAst().position;
                ctx.substitutionNodes.clear();
                ctx.substitutionColumns.clear();
                final ExpressionNode value = windowJoin != null && isDirect
                        ? windowJoinPivotValue(windowJoinPivotAggregates.getQuick(k), measuredOutput, measureIndex, position)
                        : ctx.bindingExpressions.next().of(ExpressionNode.LITERAL, quotedPivotName(measuredOutput.getColumnName(measureIndex)), 0, position);
                final ExpressionNode call = pivotMeasure(forColumns, measure, value);
                final BoundExpression bound = ctx.functionBinder.bindGroupByExpression(call, measuredOutput, ctx.substitutionNodes, ctx.substitutionColumns, executionContext);
                pivoted.getAggregates().add((FunctionExpression) bound);
            }
            for (int i = forColumns.size() - 1; i >= 0; i--) {
                final int next = pivotIndexes.getQuick(i) + 1;
                if (next < forColumns.getQuick(i).getValueList().size()) {
                    pivotIndexes.setQuick(i, next);
                    break;
                }
                pivotIndexes.setQuick(i, 0);
            }
        }
        pivotIndexes.setAll(forColumns.size(), 0);
        for (int combination = 0; combination < combinations; combination++) {
            for (int k = 0, m = measures.size(); k < m; k++) {
                final CharacterStoreEntry name = ctx.characterStore.newEntry();
                for (int i = 0, n = forColumns.size(); i < n; i++) {
                    if (i > 0) {
                        name.put('_');
                    }
                    name.put(SqlUtil.toColumnName(forColumns.getQuick(i).getValueAliases().getQuick(pivotIndexes.getQuick(i))));
                }
                if (!pivot.isPivotGroupByColumnHasNoAlias()) {
                    name.put('_').put(SqlUtil.toColumnName(measures.getQuick(k).getAlias()));
                }
                final int index = pivoted.getOutput().getColumnCount() - groupBy.size() - measuredOutput.getCorrelatedAliasCount();
                final CharSequence columnName = name.toImmutable();
                pivoted.getOutput().add(ctx.nextColumnId++, columnName, pivoted.getAggregates().getQuick(index).getDataType(), true);
                if (SqlUtil.protectColumnAlias(ctx.characterStore, columnName) != columnName) {
                    pivoted.getOutput().protectName(pivoted.getOutput().getColumnCount() - 1);
                }
            }
            for (int i = forColumns.size() - 1; i >= 0; i--) {
                final int next = pivotIndexes.getQuick(i) + 1;
                if (next < forColumns.getQuick(i).getValueList().size()) {
                    pivotIndexes.setQuick(i, next);
                    break;
                }
                pivotIndexes.setQuick(i, 0);
            }
        }
        return pivoted;
    }

    private static final class WindowJoinPivotAggregate implements Mutable {
        private static final int AVG = 1;
        private static final int FIRST = 2;
        private static final int LAST = 3;
        private static final int MERGE = 0;
        private ColumnExpression count;
        private int kind;
        private ColumnExpression merged;
        private int position;
        private ColumnExpression value;

        @Override
        public void clear() {
            count = null;
            merged = null;
            value = null;
        }

        WindowJoinPivotAggregate of(int kind, ColumnExpression value, ColumnExpression count, int position) {
            this.kind = kind;
            this.value = value;
            this.count = count;
            this.merged = null;
            this.position = position;
            return this;
        }
    }
}
