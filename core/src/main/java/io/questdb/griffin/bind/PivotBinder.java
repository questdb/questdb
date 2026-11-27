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
import io.questdb.griffin.FunctionFactory;
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
import io.questdb.griffin.plan.logical.BoundExpressionRewriter;
import io.questdb.griffin.plan.logical.ColumnExpression;
import io.questdb.griffin.plan.logical.ConstantExpression;
import io.questdb.griffin.plan.logical.FilterPlan;
import io.questdb.griffin.plan.logical.FunctionExpression;
import io.questdb.griffin.plan.logical.HorizonJoinPlan;
import io.questdb.griffin.plan.logical.JoinInput;
import io.questdb.griffin.plan.logical.JoinPlan;
import io.questdb.griffin.plan.logical.LatestByPlan;
import io.questdb.griffin.plan.logical.LogicalPlan;
import io.questdb.griffin.plan.logical.LogicalPlans;
import io.questdb.griffin.plan.logical.OutputSchema;
import io.questdb.griffin.plan.logical.ProjectPlan;
import io.questdb.griffin.plan.logical.WindowJoinPlan;
import io.questdb.griffin.plan.logical.WindowJoinStep;
import io.questdb.griffin.plan.logical.WindowPlan;
import io.questdb.griffin.plan.logical.WindowSpec;
import io.questdb.std.CharSequenceHashSet;
import io.questdb.std.Chars;
import io.questdb.std.Decimal128;
import io.questdb.std.Decimal256;
import io.questdb.std.GenericLexer;
import io.questdb.std.IntList;
import io.questdb.std.Mutable;
import io.questdb.std.ObjList;
import io.questdb.std.ObjectPool;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf8s;
import org.jetbrains.annotations.Nullable;

import static io.questdb.griffin.bind.BindContext.sourceAlias;
import static io.questdb.griffin.bind.TemporalJoinBinder.joinIndex;

final class PivotBinder implements Mutable {
    private final AggregateBinder aggregateBinder;
    private final SqlBinder binder;
    // per PIVOT cell, the value the cell reads when no row reaches it, or null when the merge already reads it
    private final ObjList<BoundExpression> cellFillers = new ObjList<>();
    // per combination of IN values, whether a row reaches the cell, or null when no measure of the cell needs it
    private final ObjList<FunctionExpression> cellPresences = new ObjList<>();
    private final SqlCompilerImpl compiler;
    private final CairoConfiguration configuration;
    private final BoundExpressionRewriter.ConjunctSplit conjunctSplit = new BoundExpressionRewriter.ConjunctSplit();
    private final BindContext ctx;
    private final Decimal128 decimal128 = new Decimal128();
    private final Decimal256 decimal256 = new Decimal256();
    private final ObjList<ExpressionNode> fillerNodes = new ObjList<>();
    private final ObjList<BoundExpression> fillerValues = new ObjList<>();
    private final ObjList<CharSequence> inAliases = new ObjList<>();
    private final ObjList<BoundExpression> inArguments = new ObjList<>();
    private final IntList inBases = new IntList();
    /**
     * The FOR columns whose values an IN sub-query returned in this compile, the most recent last: each reads
     * {@link #inCounts} values of {@link #inValues}, which {@link #inAliases} name, from {@link #inBases} on. The FOR
     * columns of a PIVOT nested in another are its own, so neither reads the values of the other.
     */
    private final ObjList<PivotForColumn> inColumns = new ObjList<>();
    private final IntList inCounts = new IntList();
    private final ObjList<ConstantExpression> inValues = new ObjList<>();
    private final JoinBinder joinBinder;
    private final BoundExpressionRewriter.ConjunctTest leadingConjuncts = conjunct -> LogicalPlans.readsOnly(conjunct, this.leadingOutput);
    private final StringSink pivotValueSink;
    private final CharSequenceHashSet pivotValues;
    private final TemporalJoinBinder temporalJoinBinder;
    private final WindowBinder windowBinder;
    private final ObjList<ColumnExpression> windowColumns = new ObjList<>();
    private final ObjectPool<WindowJoinPivotAggregate> windowJoinPivotAggregatePool;
    private final ObjList<ExpressionNode> windowNodes = new ObjList<>();
    private OutputSchema leadingOutput;

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
        cellFillers.clear();
        cellPresences.clear();
        fillerNodes.clear();
        fillerValues.clear();
        inAliases.clear();
        inArguments.clear();
        inBases.clear();
        inColumns.clear();
        inCounts.clear();
        inValues.clear();
        leadingOutput = null;
        windowColumns.clear();
        windowJoinPivotAggregatePool.clear();
        windowNodes.clear();
    }

    /**
     * Whether a PIVOT over a WINDOW JOIN merges the window values of its rows by one aggregate of the same values: sums,
     * least or greatest values, see {@link FunctionFactory.PartialMerge}.
     */
    private static boolean isDirectMerge(FunctionFactory.PartialMerge merge) {
        return merge == FunctionFactory.PartialMerge.SUM || merge == FunctionFactory.PartialMerge.MIN || merge == FunctionFactory.PartialMerge.MAX;
    }

    /**
     * Whether the PIVOT merge of a cell reads {@code filler} where no row reaches the cell: a NULL, or false for a
     * BOOLEAN, which the merge reads as false. Answers false for a value it cannot classify, which costs a presence
     * test the cell does not need but never a wrong value.
     */
    private static boolean isNullOverNoRows(BoundExpression filler) {
        return filler instanceof ConstantExpression constant
                && (constant.isNull() || ColumnType.isBoolean(constant.getDataType()) && constant.getLongValue() == 0);
    }

    /**
     * The index of the inner aggregate that measure column {@code measureIndex} of {@code measured} reads alone, or -1
     * for a measure that computes over aggregates and keys.
     */
    private static int measureAggregateIndex(LogicalPlan measured, AggregatePlan grouped, int measureIndex) {
        final int keyCount = grouped.getGroupingExpressions().size();
        if (measured == grouped) {
            return measureIndex - keyCount;
        }
        if (!(((ProjectPlan) measured).getExpressions().getQuick(measureIndex) instanceof ColumnExpression column)) {
            return -1;
        }
        final int index = grouped.getOutput().getColumnIndexById(column.getColumnId());
        // SqlParser requires an aggregate in every measure, so a measure that binds to a lone column reads an aggregate.
        assert index >= keyCount : "a PIVOT measure reads a grouping key alone";
        return index - keyCount;
    }

    private static CharSequence pivotAggregateName(ExpressionNode aggregate, ObjList<QueryColumn> measures) {
        for (int i = 0, n = measures.size(); i < n; i++) {
            if (measures.getQuick(i).getAst() == aggregate) {
                return SqlUtil.toColumnName(measures.getQuick(i).getAlias());
            }
        }
        return aggregate.token;
    }

    private static SqlException unmergedWindowJoinMeasure(ExpressionNode call) {
        return SqlException.$(call.position, "PIVOT over WINDOW JOIN supports only sum, count, min, max, avg, first and last aggregates");
    }

    private static FunctionExpression windowJoinAggregate(WindowJoinPlan plan, int columnId) {
        final WindowJoinStep step = windowJoinAggregateStep(plan, columnId);
        final IntList columnIds = step.getAggregateColumnIds();
        return step.getAggregates().getQuick(columnIds.indexOf(columnId, 0, columnIds.size()));
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
        // The values of the sub-query become output columns, so it is generated and run while binding. A view compile
        // runs it again detached from any caller whenever it recompiles the view, so it flags the sub-query.
        final boolean wasViewMetadataSubquery = executionContext.isViewMetadataSubquery();
        executionContext.setViewMetadataSubquery(wasViewMetadataSubquery || executionContext.isViewCompile());
        try (RecordCursorFactory factory = compiler.compileSubqueryFactory(subquery.queryModel, position, executionContext)) {
            final RecordMetadata metadata = factory.getMetadata();
            if (metadata.getColumnCount() != 1) {
                throw SqlException.$(position, "PIVOT IN subquery must return exactly one column, got ").put(metadata.getColumnCount());
            }
            final int type = metadata.getColumnType(0);
            final boolean isQuoted = switch (ColumnType.tagOf(type)) {
                case ColumnType.SYMBOL, ColumnType.STRING, ColumnType.VARCHAR, ColumnType.TIMESTAMP,
                     ColumnType.DATE, ColumnType.CHAR, ColumnType.UUID, ColumnType.IPv4, ColumnType.ARRAY,
                     ColumnType.LONG128, ColumnType.LONG256 -> true;
                default -> false;
            };
            final int base = inValues.size();
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
                    inValues.add(pivotInConstant(record, type, isNull, position));
                    inAliases.add(alias);
                    final long count = inValues.size() - base;
                    if (combinations * count > limit) {
                        throw SqlException.$(position, "PIVOT produces too many columns: ").put(combinations * count).put(", limit is ").put(limit);
                    }
                }
            }
            if (inValues.size() == base) {
                throw SqlException.$(position, "PIVOT IN subquery returned empty result set");
            }
            inColumns.add(column);
            inBases.add(base);
            inCounts.add(inValues.size() - base);
        } finally {
            executionContext.setViewMetadataSubquery(wasViewMetadataSubquery);
        }
    }

    /**
     * Adds {@code aggregate}, bound over the scope of step {@code step} of the WINDOW JOIN, to the step, and returns its
     * output column, or the column of an equal aggregate the step already computes.
     */
    private ColumnExpression addWindowJoinPivotAggregate(WindowJoinPlan plan, int step, BoundExpression aggregate, CharSequence name, int position) {
        assert aggregate instanceof FunctionExpression call && call.isAggregate() : "a PIVOT window aggregate binds as an aggregate";
        final FunctionExpression function = (FunctionExpression) aggregate;
        final WindowJoinStep target = plan.getSteps().getQuick(step);
        for (int i = 0, n = target.getAggregates().size(); i < n; i++) {
            if (LogicalPlans.isSameExpression(target.getAggregates().getQuick(i), function)) {
                return ctx.planNodes.columns.next().of(target.getAggregateColumnIds().getQuick(i), function.getDataType(), position);
            }
        }
        return temporalJoinBinder.addWindowJoinAggregate(plan, step, function, name, position);
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
            if (isDirectMerge(aggregate.merge)) {
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
     * Binds the condition that selects the rows of {@code measured} in the cell of the current combination of IN
     * values: each FOR key, which follows the GROUP BY keys, equals its IN value.
     */
    private BoundExpression bindPivotCellCondition(ObjList<PivotForColumn> forColumns, OutputSchema measured, int groupByCount, int position,
                                                   SqlExecutionContext executionContext) throws SqlException {
        BoundExpression condition = null;
        for (int i = 0, n = forColumns.size(); i < n; i++) {
            final BoundExpression key = pivotForKey(measured, groupByCount + i, position);
            final BoundExpression equals = ctx.functionBinder.bindCall("=", position, key, pivotInValue(forColumns, i, measured, executionContext),
                    measured, executionContext);
            condition = condition == null ? equals : ctx.functionBinder.bindCall("and", position, condition, equals, measured, executionContext);
        }
        return condition;
    }

    /**
     * Binds whether a row of {@code measured} reaches the cell of the current combination of IN values, as an
     * aggregate of the PIVOT: false over a group with no such row, including the one group of a PIVOT without GROUP BY
     * over no rows.
     */
    private FunctionExpression bindPivotCellPresence(ObjList<PivotForColumn> forColumns, OutputSchema measured, int groupByCount, int position,
                                                     SqlExecutionContext executionContext) throws SqlException {
        final BoundExpression condition = bindPivotCellCondition(forColumns, measured, groupByCount, position, executionContext);
        return (FunctionExpression) bindUnaryCall("bool_or", position, condition, measured, executionContext);
    }

    /**
     * Binds the value {@code measure} takes over a group of no rows, the value of a cell no row reaches, over
     * {@code output}, the output of the PIVOT: each aggregate reads the value its factory declares over no rows, a
     * GROUP BY key reads the column of the PIVOT, and a FOR key the IN value of the current combination.
     */
    private BoundExpression bindPivotFiller(QueryColumn measure, QueryModel input, AggregatePlan grouped, @Nullable WindowJoinPlan windowJoin,
                                            OutputSchema output, ObjList<PivotForColumn> forColumns, int groupByCount,
                                            SqlExecutionContext executionContext) throws SqlException {
        final BindScope scope = ctx.scope();
        final ExpressionNode ast = measure.getAst();
        fillerNodes.clear();
        fillerValues.clear();
        try {
            aggregateBinder.collectAggregateOutputs(ast, input, grouped);
            final OutputSchema groupedOutput = grouped.getOutput();
            final int keyCount = grouped.getGroupingExpressions().size();
            for (int i = 0, n = scope.substitutionNodes.size(); i < n; i++) {
                // Only the aggregation of a query level retains a projection, keyed by the nodes of its own select list.
                assert scope.aggregateSubstitutions.getQuick(i) instanceof ColumnExpression : "a PIVOT measure reads a retained projection";
                final ColumnExpression column = (ColumnExpression) scope.aggregateSubstitutions.getQuick(i);
                final ExpressionNode node = scope.substitutionNodes.getQuick(i);
                final int index = groupedOutput.getColumnIndexById(column.getColumnId());
                final int type = groupedOutput.getColumnType(index);
                fillerNodes.add(node);
                fillerValues.add(index < keyCount
                        ? pivotKeySubstitute(index, type, node.position, output, forColumns, groupByCount, executionContext)
                        : emptyGroupConstant(emptyGroupValue(grouped, index - keyCount, windowJoin), type, node.position, output, executionContext));
            }
        } finally {
            scope.substitutionNodes.clear();
            scope.aggregateSubstitutions.clear();
        }
        return ctx.functionBinder.bindValue(ast, output, null, fillerNodes, fillerValues, executionContext);
    }

    /**
     * Binds the condition the FOR columns of the PIVOT put on the rows of its input, over {@code input}: each FOR key is
     * IN the list of its IN values.
     */
    private BoundExpression bindPivotForFilter(QueryModel pivot, OutputSchema input, CharSequence alias, SqlExecutionContext executionContext)
            throws SqlException {
        final ObjList<PivotForColumn> forColumns = pivot.getPivotForColumns();
        BoundExpression filter = null;
        for (int i = 0, n = forColumns.size(); i < n; i++) {
            final PivotForColumn column = forColumns.getQuick(i);
            final ExpressionNode key = column.getInExpr();
            final int valueCount = pivotInCount(column);
            // A sub-query IN value binds a PIVOT of its own, which stacks its arguments above these.
            final int base = inArguments.size();
            try {
                if (key.type == ExpressionNode.LITERAL) {
                    final int index = ctx.bindColumnIndex(key, input, alias);
                    inArguments.add(ctx.planNodes.columns.next().of(input.getColumnId(index), input.getColumnType(index), key.position));
                } else {
                    inArguments.add(ctx.functionBinder.bindValue(key, input, alias, executionContext));
                }
                for (int k = 0; k < valueCount; k++) {
                    inArguments.add(pivotInValue(column, k, input, executionContext));
                }
                for (int k = 0; k < valueCount; k++) {
                    final BoundExpression value = inArguments.getQuick(base + k + 1);
                    if ((value.getFunctionFlags() & (BoundExpression.CONSTANT | BoundExpression.RUNTIME_CONSTANT)) == 0) {
                        throw SqlException.$(value.getPosition(), "constant expected");
                    }
                }
                final ObjList<BoundExpression> arguments = ctx.functionBinder.getCallArguments();
                arguments.clear();
                for (int k = base, m = inArguments.size(); k < m; k++) {
                    arguments.add(inArguments.getQuick(k));
                }
            } finally {
                inArguments.setPos(base);
            }
            final BoundExpression in = ctx.functionBinder.bindCall("IN", key.position, input, executionContext);
            filter = filter == null ? in : ctx.expressionRewriter.combineConjunction(filter, in, key.position);
        }
        return filter;
    }

    /**
     * Binds the aggregate of the PIVOT that merges {@code value}, the measure of the current combination of IN values,
     * from the rows of {@code measured}: a cell matches at most one row, and the merge reads {@code emptyValue} where
     * none does.
     */
    private FunctionExpression bindPivotMeasure(ObjList<PivotForColumn> forColumns, OutputSchema measured, int groupByCount, BoundExpression value,
                                                FunctionFactory.EmptyGroupValue emptyValue, int position, SqlExecutionContext executionContext) throws SqlException {
        final int valueType = value.getDataType();
        final BoundExpression widened = widenNarrowMeasure(value, position, measured, executionContext);
        final BoundExpression otherwise = switch (emptyValue) {
            case NULL -> ctx.planNodes.constants.next().ofNull(position);
            case ZERO -> ctx.planNodes.constants.next().ofInt(0, position);
            case TRUE -> ctx.planNodes.constants.next().ofBoolean(true, position);
        };
        final BoundExpression choice;
        if (forColumns.size() == 1) {
            final BoundExpression key = pivotForKey(measured, groupByCount, position);
            final BoundExpression inValue = pivotInValue(forColumns, 0, measured, executionContext);
            final ObjList<BoundExpression> arguments = ctx.functionBinder.getCallArguments();
            arguments.clear();
            arguments.add(key);
            arguments.add(inValue);
            arguments.add(widened);
            arguments.add(otherwise);
            choice = ctx.functionBinder.bindCall("switch", position, measured, executionContext);
        } else {
            final BoundExpression condition = bindPivotCellCondition(forColumns, measured, groupByCount, position, executionContext);
            final ObjList<BoundExpression> arguments = ctx.functionBinder.getCallArguments();
            arguments.clear();
            arguments.add(condition);
            arguments.add(widened);
            arguments.add(otherwise);
            choice = ctx.functionBinder.bindCall("case", position, measured, executionContext);
        }
        // A cell matches at most one row, so sum and bool_and read the zero and the true of an absent cell; BOOLEAN has no
        // NULL for first_not_null to skip, and bool_or reads an absent cell as false.
        final CharSequence merge = switch (emptyValue) {
            case ZERO -> "sum";
            case TRUE -> "bool_and";
            case NULL -> ColumnType.isBoolean(valueType) ? "bool_or" : "first_not_null";
        };
        return (FunctionExpression) bindUnaryCall(merge, position, choice, measured, executionContext);
    }

    /**
     * Binds each window call of {@code node}, a measure of the PIVOT, into {@code window}, over the input of the PIVOT,
     * and records the window column that stands for it where the measure's aggregate reads it, see {@link #windowNodes}.
     */
    private void bindPivotWindowCalls(ExpressionNode node, LogicalPlan source, WindowPlan window, QueryModel pivot, QueryModel windowScope,
                                      SqlExecutionContext executionContext) throws SqlException {
        if (node == null) {
            return;
        }
        if (node.windowExpression == null) {
            for (int i = 0, n = node.operandCount(); i < n; i++) {
                bindPivotWindowCalls(node.operandAt(i), source, window, pivot, windowScope, executionContext);
            }
            return;
        }
        final WindowExpression syntax = windowBinder.resolveWindowSyntax(node.windowExpression, windowScope, executionContext);
        node.windowExpression = syntax;
        final QueryModel input = pivot.getNestedModel();
        final OutputSchema scope = source.getOutput();
        final CharSequence alias = sourceAlias(input);
        final WindowSpec spec = windowBinder.newSpec(syntax);
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
        window.getFunctions().add(function);
        window.getSpecs().add(spec);
        window.getFunctionColumnIds().add(columnId);
        window.getOutput().add(columnId, entry.toImmutable(), function.getDataType(), true);
        windowNodes.add(node);
        windowColumns.add(ctx.planNodes.columns.next().of(columnId, function.getDataType(), node.position));
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
            bindPivotWindowCalls(ast, source, window, pivot, windowScope, executionContext);
        }
        return window == null ? source : window;
    }

    private BoundExpression bindUnaryCall(CharSequence name, int position, BoundExpression argument, OutputSchema input,
                                          SqlExecutionContext executionContext) throws SqlException {
        final ObjList<BoundExpression> arguments = ctx.functionBinder.getCallArguments();
        arguments.clear();
        arguments.add(argument);
        return ctx.functionBinder.bindCall(name, position, input, executionContext);
    }

    /**
     * Binds into the steps of the WINDOW JOIN the window aggregates the PIVOT merges its measures from, see
     * {@link FunctionFactory#getPartialMerge}: a measure that merges by one aggregate of its window values reads the
     * measure itself; avg reads the sum and the count of its argument as DOUBLE; first and last read the measure and the
     * count of the rows of its window, which tells an empty window from a NULL value. Each step takes the aggregates of
     * the measures that read its window in the order of the measures, and an aggregate equal to one the step already
     * computes reads that one.
     */
    private void bindWindowJoinPivotAggregates(QueryModel pivot, WindowJoinPlan plan, SqlExecutionContext executionContext) throws SqlException {
        final BindScope bindScope = ctx.scope();
        bindScope.clearAggregateNodes();
        final ObjList<QueryColumn> measures = pivot.getPivotGroupByColumns();
        for (int i = 0, n = measures.size(); i < n; i++) {
            aggregateBinder.collectAggregateNodes(measures.getQuick(i).getAst(), true);
        }
        final ObjList<ExpressionNode> calls = bindScope.aggregateNodes;
        bindScope.windowJoinPivotAggregates.clear();
        for (int i = 0, n = calls.size(); i < n; i++) {
            final ExpressionNode call = calls.getQuick(i);
            if (call.paramCount > 1) {
                throw unmergedWindowJoinMeasure(call);
            }
            bindScope.windowJoinPivotAggregates.add(windowJoinPivotAggregatePool.next().of(call.position));
        }
        bindScope.resetAliases(plan.getOutput());
        for (int s = 0, m = plan.getSteps().size(); s < m; s++) {
            final OutputSchema scope = temporalJoinBinder.windowJoinStepScope(plan, s);
            for (int i = 0, n = calls.size(); i < n; i++) {
                final ExpressionNode call = calls.getQuick(i);
                if (temporalJoinBinder.windowJoinStepOf(call, plan) != s) {
                    continue;
                }
                final int position = call.position;
                final FunctionExpression measure = (FunctionExpression) ctx.functionBinder.bindGroupByExpression(call, scope, null, executionContext);
                final FunctionFactory.PartialMerge merge = measure.getPartialMerge();
                final WindowJoinPivotAggregate aggregate = bindScope.windowJoinPivotAggregates.getQuick(i);
                switch (merge) {
                    case NONE -> throw unmergedWindowJoinMeasure(call);
                    case AVERAGE -> {
                        final BoundExpression argument = ctx.functionBinder.bind(call.rhs, scope, null, executionContext);
                        final int type = ColumnType.tagOf(argument.getDataType());
                        if (type != ColumnType.DOUBLE && type != ColumnType.FLOAT && type != ColumnType.INT
                                && type != ColumnType.LONG && type != ColumnType.SHORT) {
                            throw SqlException.$(position, "PIVOT over WINDOW JOIN supports avg only over DOUBLE, FLOAT, INT, LONG and SHORT values");
                        }
                        final BoundExpression copy = ctx.expressionRewriter.copy(argument);
                        final BoundExpression sum = bindUnaryCall("sum", position, castTo(argument, ColumnType.DOUBLE, position, scope, executionContext),
                                scope, executionContext);
                        final BoundExpression count = bindUnaryCall("count", position, castTo(copy, ColumnType.DOUBLE, position, scope, executionContext),
                                scope, executionContext);
                        aggregate.ofWindow(merge, addWindowJoinPivotAggregate(plan, s, sum, "sum", position),
                                addWindowJoinPivotAggregate(plan, s, count, "count", position));
                    }
                    case FIRST, LAST -> {
                        final ColumnExpression value = addWindowJoinPivotAggregate(plan, s, measure, TemporalJoinBinder.windowJoinAggregateName(call, pivot), position);
                        ctx.functionBinder.getCallArguments().clear();
                        aggregate.ofWindow(merge, value,
                                addWindowJoinPivotAggregate(plan, s, ctx.functionBinder.bindCall("count", position, scope, executionContext), "count", position));
                    }
                    default -> aggregate.ofWindow(merge,
                            addWindowJoinPivotAggregate(plan, s, measure, TemporalJoinBinder.windowJoinAggregateName(call, pivot), position), null);
                }
            }
        }
    }

    private FunctionExpression bindWindowJoinPivotMerge(ExpressionNode call, WindowJoinPivotAggregate aggregate, boolean isHidden,
                                                        OutputSchema input, SqlExecutionContext executionContext) throws SqlException {
        final int position = call.position;
        final CharSequence merge = switch (aggregate.merge) {
            case SUM, AVERAGE -> "sum";
            case MIN -> "min";
            case MAX -> "max";
            case FIRST -> "first_not_null";
            case LAST -> "last_not_null";
            case NONE -> throw new AssertionError("a PIVOT window measure that does not merge fails to bind");
        };
        final BoundExpression argument = switch (aggregate.merge) {
            case AVERAGE -> copyColumn(isHidden ? aggregate.count : aggregate.value, position);
            case FIRST, LAST -> isHidden ? windowJoinPivotNullFlag(aggregate, position, input, executionContext)
                    : windowJoinPivotFirstLastValue(aggregate, position, input, executionContext);
            default -> copyColumn(aggregate.value, position);
        };
        final BoundExpression bound = bindUnaryCall(merge, position, argument, input, executionContext);
        if (!(bound instanceof FunctionExpression function) || !function.isAggregate()) {
            throw SqlException.$(position, "expected aggregate function [col=").put(call).put(']');
        }
        return function;
    }

    private BoundExpression castTo(BoundExpression value, int type, int position, OutputSchema input, SqlExecutionContext executionContext)
            throws SqlException {
        return ctx.functionBinder.bindCall("cast", position, value, ctx.planNodes.types.next().of(type, position), input, executionContext);
    }

    private BoundExpression castToInt(BoundExpression value, int position, OutputSchema input, SqlExecutionContext executionContext) throws SqlException {
        return castTo(value, ColumnType.INT, position, input, executionContext);
    }

    private ColumnExpression copyColumn(ColumnExpression column, int position) {
        return ctx.planNodes.columns.next().of(column.getColumnId(), column.getDataType(), position);
    }

    /**
     * The value an aggregate of {@code type} reports over no rows, as its factory declares it, see
     * {@link FunctionFactory#getEmptyGroupValue()}: a NULL of that type, its zero, or true.
     */
    private BoundExpression emptyGroupConstant(FunctionFactory.EmptyGroupValue value, int type, int position, OutputSchema output,
                                               SqlExecutionContext executionContext) throws SqlException {
        return switch (value) {
            case NULL -> ctx.functionBinder.bindCall("cast", position, ctx.planNodes.constants.next().ofNull(position),
                    ctx.planNodes.types.next().of(type, position), output, executionContext);
            case ZERO -> ColumnType.tagOf(type) == ColumnType.DOUBLE
                    ? ctx.planNodes.constants.next().ofDouble(0.0, position) : ctx.planNodes.constants.next().ofLong(0, position);
            case TRUE -> ctx.planNodes.constants.next().ofBoolean(true, position);
        };
    }

    /**
     * The value aggregate {@code index} of the inner aggregation reports over no rows, as its resolved overload declares
     * it; over a WINDOW JOIN, the value of the window aggregate it merges, as the merge of a count sums it.
     */
    private FunctionFactory.EmptyGroupValue emptyGroupValue(AggregatePlan grouped, int index, @Nullable WindowJoinPlan windowJoin) {
        if (windowJoin != null) {
            return windowJoinAggregate(windowJoin, ctx.scope().windowJoinPivotAggregates.getQuick(index).value.getColumnId()).getEmptyGroupValue();
        }
        return grouped.getAggregates().getQuick(index).getEmptyGroupValue();
    }

    /**
     * Filters {@code plan}, the input of the PIVOT or the master of its temporal join, by {@code filter}, the IN of each
     * FOR column, where the binder filters the plan by its WHERE: in the filter of the WHERE, ahead of its conjuncts;
     * below the LATEST ON of the leading input, which picks the latest of the rows the filter keeps, each IN that reads
     * only that input; and otherwise above the plan, where the optimiser pushes it down.
     */
    private LogicalPlan filterPivotInput(LogicalPlan plan, BoundExpression filter, boolean isLatestOverLeadingInput) throws SqlException {
        if (isLatestOverLeadingInput) {
            if (plan instanceof LatestByPlan latest) {
                latest.replaceInput(0, filterPivotInput(latest.getChild(), filter, false));
                return latest;
            }
            assert plan instanceof JoinPlan : "LATEST ON over the leading input of a join binds that input below the join";
            final ObjList<JoinInput> inputs = ((JoinPlan) plan).getInputs();
            if (LogicalPlans.canPushJoinFilter(inputs, 0, inputs.size() - 1)) {
                final JoinInput first = inputs.getQuick(0);
                leadingOutput = first.getSourceOutput();
                ctx.expressionRewriter.splitConjuncts(filter, leadingConjuncts, conjunctSplit);
                final BoundExpression leading = conjunctSplit.getMatched();
                final BoundExpression rest = conjunctSplit.getRemainder();
                if (leading != null) {
                    first.setInput(filterPivotInput(first.getInput(), leading, true));
                }
                if (rest == null) {
                    return plan;
                }
                filter = rest;
            }
        }
        if (plan instanceof FilterPlan where) {
            where.of(where.getChild(), ctx.expressionRewriter.combineConjunction(filter, where.getPredicate(), where.getPosition()), where.getPosition());
            return where;
        }
        return ctx.planNodes.filters.next().of(plan, filter, filter.getPosition());
    }

    /**
     * Advances the IN value index of each FOR column to the next combination, the last column fastest.
     */
    private void nextPivotCombination(ObjList<PivotForColumn> forColumns) {
        final IntList indexes = ctx.scope().pivotIndexes;
        for (int i = forColumns.size() - 1; i >= 0; i--) {
            final int next = indexes.getQuick(i) + 1;
            if (next < pivotInCount(forColumns.getQuick(i))) {
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

    private ColumnExpression pivotForKey(OutputSchema measured, int index, int position) {
        return ctx.planNodes.columns.next().of(measured.getColumnId(index), measured.getColumnType(index), position);
    }

    /**
     * The alias that names IN value {@code value} of FOR column {@code column} in the columns of the PIVOT.
     */
    private CharSequence pivotInAlias(PivotForColumn column, int value) {
        final int index = pivotInIndex(column);
        return index < 0 ? column.getValueAliases().getQuick(value) : inAliases.getQuick(inBases.getQuick(index) + value);
    }

    /**
     * The literal of the value of {@code type} in the only column of {@code record}, a row of an IN sub-query, which
     * stands for the literal an IN list spells: text, as a text literal binds, is a STRING, and a NULL is untyped.
     */
    private ConstantExpression pivotInConstant(Record record, int type, boolean isNull, int position) {
        final ConstantExpression constant = ctx.planNodes.constants.next();
        final ConstantExpression value = isNull ? constant.ofNull(position) : switch (ColumnType.tagOf(type)) {
            case ColumnType.STRING, ColumnType.ARRAY_STRING ->
                    constant.ofString(Chars.toString(record.getStrA(0)), position);
            case ColumnType.SYMBOL -> constant.ofString(Chars.toString(record.getSymA(0)), position);
            case ColumnType.VARCHAR -> constant.ofString(Utf8s.toString(record.getVarcharA(0)), position);
            case ColumnType.CHAR -> constant.ofChar(record.getChar(0), position);
            case ColumnType.BOOLEAN -> constant.ofBoolean(record.getBool(0), position);
            case ColumnType.BYTE -> constant.ofByte(record.getByte(0), position);
            case ColumnType.SHORT -> constant.ofShort(record.getShort(0), position);
            case ColumnType.INT -> constant.ofInt(record.getInt(0), position);
            case ColumnType.LONG -> constant.ofLong(record.getLong(0), position);
            case ColumnType.FLOAT -> constant.ofFloat(record.getFloat(0), position);
            case ColumnType.DOUBLE -> constant.ofDouble(record.getDouble(0), position);
            case ColumnType.DATE -> constant.ofDate(record.getDate(0), position);
            case ColumnType.TIMESTAMP -> constant.ofTimestamp(record.getTimestamp(0), type, position);
            case ColumnType.IPv4 -> constant.ofIPv4(record.getIPv4(0), position);
            case ColumnType.UUID -> constant.ofUuid(record.getLong128Lo(0), record.getLong128Hi(0), position);
            case ColumnType.LONG128 -> constant.ofLong128(record.getLong128Lo(0), record.getLong128Hi(0), position);
            case ColumnType.GEOBYTE -> constant.ofGeoHash(record.getGeoByte(0), type, position);
            case ColumnType.GEOSHORT -> constant.ofGeoHash(record.getGeoShort(0), type, position);
            case ColumnType.GEOINT -> constant.ofGeoHash(record.getGeoInt(0), type, position);
            case ColumnType.GEOLONG -> constant.ofGeoHash(record.getGeoLong(0), type, position);
            case ColumnType.DECIMAL8 -> constant.ofDecimal(type, 0, 0, 0, record.getDecimal8(0), position);
            case ColumnType.DECIMAL16 -> constant.ofDecimal(type, 0, 0, 0, record.getDecimal16(0), position);
            case ColumnType.DECIMAL32 -> constant.ofDecimal(type, 0, 0, 0, record.getDecimal32(0), position);
            case ColumnType.DECIMAL64 -> constant.ofDecimal(type, 0, 0, 0, record.getDecimal64(0), position);
            case ColumnType.DECIMAL128 -> {
                record.getDecimal128(0, decimal128);
                yield constant.ofDecimal(type, 0, 0, decimal128.getHigh(), decimal128.getLow(), position);
            }
            case ColumnType.DECIMAL256 -> {
                record.getDecimal256(0, decimal256);
                yield constant.ofDecimal(type, decimal256.getHh(), decimal256.getHl(), decimal256.getLh(), decimal256.getLl(), position);
            }
            default -> throw new AssertionError("printPivotValue() rejects an IN sub-query of another type");
        };
        return value.markLiteral();
    }

    private int pivotInCount(PivotForColumn column) {
        final int index = pivotInIndex(column);
        return index < 0 ? column.getValueList().size() : inCounts.getQuick(index);
    }

    /**
     * The index of {@code column} in {@link #inColumns}, or -1 for a FOR column of an IN list.
     */
    private int pivotInIndex(PivotForColumn column) {
        for (int i = inColumns.size() - 1; i >= 0; i--) {
            if (inColumns.getQuick(i) == column) {
                return i;
            }
        }
        return -1;
    }

    /**
     * Binds the IN value of FOR column {@code forColumn} in the current combination of IN values.
     */
    private BoundExpression pivotInValue(ObjList<PivotForColumn> forColumns, int forColumn, OutputSchema input,
                                         SqlExecutionContext executionContext) throws SqlException {
        return pivotInValue(forColumns.getQuick(forColumn), ctx.scope().pivotIndexes.getQuick(forColumn), input, executionContext);
    }

    /**
     * Binds IN value {@code value} of FOR column {@code column}: an expression of its IN list, or a value its IN
     * sub-query returned, see {@link #inColumns}.
     */
    private BoundExpression pivotInValue(PivotForColumn column, int value, OutputSchema input, SqlExecutionContext executionContext)
            throws SqlException {
        final int index = pivotInIndex(column);
        return index < 0 ? ctx.functionBinder.bindValue(column.getValueList().getQuick(value), input, null, executionContext)
                : ctx.expressionRewriter.copy(inValues.getQuick(inBases.getQuick(index) + value));
    }

    /**
     * The value grouping key {@code index} of the inner aggregate reads in a cell no row reaches, over {@code output}:
     * the column of a GROUP BY key, or the IN value of the current combination for a FOR key, as a value of the key's
     * {@code type}.
     */
    private BoundExpression pivotKeySubstitute(int index, int type, int position, OutputSchema output, ObjList<PivotForColumn> forColumns,
                                               int groupByCount, SqlExecutionContext executionContext) throws SqlException {
        final BindScope scope = ctx.scope();
        final int key = scope.pivotKeyIndexes.indexOf(index, 0, scope.pivotKeyIndexes.size());
        assert key >= 0 : "a grouping key of the PIVOT is no GROUP BY or FOR key";
        if (key < groupByCount) {
            return ctx.planNodes.columns.next().of(output.getColumnId(key), output.getColumnType(key), position);
        }
        final BoundExpression value = pivotInValue(forColumns, key - groupByCount, output, executionContext);
        return value.getDataType() == type ? value
                : ctx.functionBinder.bindCall("cast", position, value, ctx.planNodes.types.next().of(type, position), output, executionContext);
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
            combinations *= pivotInCount(column);
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

    /**
     * Projects the output of the PIVOT, reading the filler of each cell that has one where no row reaches the cell:
     * the PIVOT appends the presence test of each combination of IN values that needs one as a hidden aggregate, and
     * the projection reads the merge where the test holds and the filler otherwise. The merge alone cannot tell a cell
     * no row reaches from one whose value is NULL.
     */
    private ProjectPlan projectPivotFillers(AggregatePlan pivoted, int groupByCount, int measureCount, int fillerBase, int presenceBase,
                                            SqlExecutionContext executionContext) throws SqlException {
        final OutputSchema output = pivoted.getOutput();
        final int columnCount = output.getColumnCount();
        final int position = pivoted.getPosition();
        ctx.scope().resetAliases(output);
        for (int i = presenceBase, n = cellPresences.size(); i < n; i++) {
            final FunctionExpression presence = cellPresences.getQuick(i);
            if (presence != null) {
                pivoted.getAggregates().add(presence);
                output.add(ctx.planNodes.nextColumnId(), ctx.createOutputName("pivot_cell_present"), presence.getDataType(), false);
            }
        }
        final ProjectPlan project = ctx.planNodes.projects.next().of(pivoted, position);
        final OutputSchema projected = project.getOutput();
        int presenceIndex = columnCount - 1;
        for (int i = 0; i < columnCount; i++) {
            final ColumnExpression column = ctx.planNodes.columns.next().of(output.getColumnId(i), output.getColumnType(i), position);
            final int cell = i - groupByCount;
            if (cell >= 0 && cell % measureCount == 0 && cellPresences.getQuick(presenceBase + cell / measureCount) != null) {
                presenceIndex++;
            }
            final BoundExpression filler = cell < 0 ? null : cellFillers.getQuick(fillerBase + cell);
            if (filler == null) {
                project.getExpressions().add(column);
                projected.addColumnAs(output, i, ctx.planNodes.nextColumnId(), output.getColumnName(i), true);
                continue;
            }
            final ObjList<BoundExpression> arguments = ctx.functionBinder.getCallArguments();
            arguments.clear();
            arguments.add(ctx.planNodes.columns.next().of(output.getColumnId(presenceIndex), output.getColumnType(presenceIndex), position));
            arguments.add(column);
            arguments.add(filler);
            final BoundExpression value = ctx.functionBinder.bindCall("case", position, output, executionContext);
            project.getExpressions().add(value);
            projected.add(ctx.planNodes.nextColumnId(), output.getColumnName(i), value.getDataType(), true);
            if (output.isNameProtected(i)) {
                projected.protectName(projected.getColumnCount() - 1);
            }
        }
        return project;
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
    private BoundExpression widenNarrowMeasure(BoundExpression measure, int position, OutputSchema input, SqlExecutionContext executionContext)
            throws SqlException {
        return switch (ColumnType.tagOf(measure.getDataType())) {
            case ColumnType.BYTE, ColumnType.SHORT -> castToInt(measure, position, input, executionContext);
            default -> measure;
        };
    }

    /**
     * The window value a first or last measure merges; a BYTE, SHORT or BOOLEAN value widens to INT, which reads NULL
     * for an empty window.
     */
    private BoundExpression windowJoinPivotFirstLastValue(WindowJoinPivotAggregate aggregate, int position, OutputSchema input,
                                                          SqlExecutionContext executionContext) throws SqlException {
        final ColumnExpression value = copyColumn(aggregate.value, position);
        final BoundExpression widened = aggregate.carriedType != value.getDataType()
                ? castToInt(value, position, input, executionContext) : widenNarrowMeasure(value, position, input, executionContext);
        if (widened == value) {
            return value;
        }
        final BoundExpression isEmpty = windowJoinPivotIsEmpty(aggregate, position, input, executionContext);
        final ObjList<BoundExpression> arguments = ctx.functionBinder.getCallArguments();
        arguments.clear();
        arguments.add(isEmpty);
        arguments.add(ctx.planNodes.constants.next().ofNull(position));
        arguments.add(widened);
        return ctx.functionBinder.bindCall("case", position, input, executionContext);
    }

    private BoundExpression windowJoinPivotIsEmpty(WindowJoinPivotAggregate aggregate, int position, OutputSchema input,
                                                   SqlExecutionContext executionContext) throws SqlException {
        return ctx.functionBinder.bindCall("=", position, copyColumn(aggregate.count, position), ctx.planNodes.constants.next().ofInt(0, position),
                input, executionContext);
    }

    private BoundExpression windowJoinPivotNullFlag(WindowJoinPivotAggregate aggregate, int position, OutputSchema input,
                                                    SqlExecutionContext executionContext) throws SqlException {
        final BoundExpression isEmpty = windowJoinPivotIsEmpty(aggregate, position, input, executionContext);
        final BoundExpression isNull = ctx.functionBinder.bindCall("=", position, copyColumn(aggregate.value, position),
                ctx.planNodes.constants.next().ofNull(position), input, executionContext);
        final ObjList<BoundExpression> arguments = ctx.functionBinder.getCallArguments();
        arguments.clear();
        arguments.add(isEmpty);
        arguments.add(ctx.planNodes.constants.next().ofNull(position));
        arguments.add(isNull);
        arguments.add(ctx.planNodes.constants.next().ofInt(1, position));
        arguments.add(ctx.planNodes.constants.next().ofInt(0, position));
        return ctx.functionBinder.bindCall("case", position, input, executionContext);
    }

    private BoundExpression windowJoinPivotValue(WindowJoinPivotAggregate aggregate, OutputSchema measured, int measureIndex, int position,
                                                 SqlExecutionContext executionContext) throws SqlException {
        final ColumnExpression value = pivotForKey(measured, measureIndex, position);
        switch (aggregate.merge) {
            case AVERAGE -> {
                return ctx.functionBinder.bindCall("/", position, value, copyColumn(aggregate.merged, position), measured, executionContext);
            }
            case FIRST, LAST -> {
                final BoundExpression isNull = ctx.functionBinder.bindCall("=", position, copyColumn(aggregate.merged, position),
                        ctx.planNodes.constants.next().ofInt(1, position), measured, executionContext);
                final ObjList<BoundExpression> arguments = ctx.functionBinder.getCallArguments();
                arguments.clear();
                arguments.add(isNull);
                arguments.add(ctx.planNodes.constants.next().ofNull(position));
                arguments.add(value);
                final BoundExpression choice = ctx.functionBinder.bindCall("case", position, measured, executionContext);
                // "= 1" reads a BOOLEAN back from its INT, and the NULL of all-empty windows as false.
                return aggregate.carriedType != aggregate.value.getDataType() ? ctx.functionBinder.bindCall("=", position, choice,
                        ctx.planNodes.constants.next().ofInt(1, position), measured, executionContext) : choice;
            }
            default -> {
                return value;
            }
        }
    }

    LogicalPlan bindPivot(QueryModel pivot, SqlExecutionContext executionContext) throws SqlException {
        final BindScope scope = ctx.scope();
        final QueryModel windowScope = scope.namedWindowScope != null && scope.namedWindowScope.getNestedModel() == pivot
                ? scope.namedWindowScope : pivot;
        final QueryModel input = pivot.getNestedModel();
        final int combinations = preparePivotValues(pivot, executionContext);
        validatePivotColumnNames(pivot);
        final ExpressionNode where = input.getWhereClause() == null ? null
                : SqlUtil.optimiseBooleanNot(ExpressionNode.deepClone(ctx.bindingExpressions, input.getWhereClause()), ctx.bindingExpressions);
        final boolean isLatestOverLeadingInput = binder.isLatestOverLeadingInput(input);
        LogicalPlan source;
        WindowJoinPlan windowJoin = null;
        if (joinIndex(input, QueryModel.JOIN_WINDOW) > 0) {
            source = windowJoin = temporalJoinBinder.bindWindowJoin(pivot, null, input, where, executionContext);
            final LogicalPlan master = windowJoin.getMaster();
            final BoundExpression filter = bindPivotForFilter(pivot, master.getOutput(), windowJoin.getSteps().getQuick(0).getMasterAlias(), executionContext);
            bindWindowJoinPivotAggregates(pivot, windowJoin, executionContext);
            if (!windowJoin.isEmpty()) {
                windowJoin.replaceInput(0, filterPivotInput(master, filter, isLatestOverLeadingInput));
            }
        } else if (joinIndex(input, QueryModel.JOIN_HORIZON) > 0) {
            final HorizonJoinPlan horizon = (HorizonJoinPlan) temporalJoinBinder.bindHorizonJoin(pivot, input, where, executionContext);
            final LogicalPlan master = horizon.getMaster();
            horizon.replaceInput(0, filterPivotInput(master,
                    bindPivotForFilter(pivot, master.getOutput(), horizon.getMasterAlias(), executionContext), isLatestOverLeadingInput));
            source = horizon;
        } else if (input.getJoinModels().size() > 1) {
            source = joinBinder.bindJoins(input, where, executionContext);
            source = filterPivotInput(source, bindPivotForFilter(pivot, source.getOutput(), sourceAlias(input), executionContext),
                    isLatestOverLeadingInput);
        } else {
            source = binder.bindSource(input, executionContext);
            final LatestByPlan latest = binder.bindSourceLatestBy(source, input);
            final BoundExpression filter = bindPivotForFilter(pivot, source.getOutput(), sourceAlias(input), executionContext);
            final BoundExpression predicate = where == null ? filter : ctx.functionBinder.toBooleanSubquery(
                    ctx.expressionRewriter.combineConjunction(filter, binder.bindConjuncts(where, source, input, executionContext), where.position));
            source = ctx.planNodes.filters.next().of(source, predicate, predicate.getPosition());
            if (latest != null) {
                latest.replaceInput(0, source);
                source = latest;
            }
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
                temporalJoinBinder.rejectWindowJoinReads(key, windowJoin);
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
                    : ctx.functionBinder.bindGroupByExpression(call, source.getOutput(), sourceAlias(input), windowNodes, windowColumns, executionContext);
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
        if (!isDirect) {
            // Two aggregates of one name, such as count(x) and count(), read as columns of distinct names.
            scope.resetAliases(grouped.getOutput());
        }
        for (int i = 0, n = scope.aggregateNodes.size(); i < n; i++) {
            final CharSequence name = isDirect ? SqlUtil.toColumnName(measures.getQuick(i).getAlias())
                    : ctx.createOutputName(pivotAggregateName(scope.aggregateNodes.getQuick(i), measures));
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
        final int fillerBase = cellFillers.size();
        final int presenceBase = cellPresences.size();
        boolean hasFiller = false;
        scope.pivotIndexes.setAll(forColumns.size(), 0);
        for (int combination = 0; combination < combinations; combination++) {
            FunctionExpression presence = null;
            for (int k = 0, m = measures.size(); k < m; k++) {
                final QueryColumn measure = measures.getQuick(k);
                final int measureIndex = firstMeasureIndex + k;
                final ExpressionNode ast = measure.getAst();
                final int position = ast.position;
                // Only a lone aggregate in a group, which always has rows, merges its value over no rows without a presence test.
                final int aggregateIndex = measureAggregateIndex(measured, grouped, measureIndex);
                final FunctionFactory.EmptyGroupValue emptyValue = aggregateIndex >= 0
                        ? emptyGroupValue(grouped, aggregateIndex, windowJoin) : FunctionFactory.EmptyGroupValue.NULL;
                BoundExpression filler = null;
                if (aggregateIndex < 0) {
                    filler = bindPivotFiller(measure, input, grouped, windowJoin, pivoted.getOutput(), forColumns, groupBy.size(), executionContext);
                } else if (groupBy.size() == 0 && emptyValue != FunctionFactory.EmptyGroupValue.NULL) {
                    filler = emptyGroupConstant(emptyValue, measuredOutput.getColumnType(measureIndex), position, pivoted.getOutput(), executionContext);
                }
                if (filler != null && isNullOverNoRows(filler)) {
                    filler = null;
                }
                cellFillers.add(filler);
                if (filler != null) {
                    hasFiller = true;
                    if (presence == null) {
                        presence = bindPivotCellPresence(forColumns, measuredOutput, groupBy.size(), pivot.getModelPosition(), executionContext);
                    }
                }
                final WindowJoinPivotAggregate aggregate = windowJoin != null && isDirect ? scope.windowJoinPivotAggregates.getQuick(k) : null;
                final BoundExpression value = aggregate != null
                        ? windowJoinPivotValue(aggregate, measuredOutput, measureIndex, position, executionContext)
                        : pivotForKey(measuredOutput, measureIndex, position);
                pivoted.getAggregates().add(bindPivotMeasure(forColumns, measuredOutput, groupBy.size(), value,
                        filler != null ? FunctionFactory.EmptyGroupValue.NULL : emptyValue, position, executionContext));
            }
            cellPresences.add(presence);
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
                    name.put(SqlUtil.toColumnName(pivotInAlias(forColumns.getQuick(i), scope.pivotIndexes.getQuick(i))));
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
        try {
            return hasFiller ? projectPivotFillers(pivoted, groupBy.size(), measures.size(), fillerBase, presenceBase, executionContext) : pivoted;
        } finally {
            cellFillers.setPos(fillerBase);
            cellPresences.setPos(presenceBase);
        }
    }

    static final class WindowJoinPivotAggregate implements Mutable {
        /**
         * The type the merge carries the window value as: INT for a BOOLEAN first or last, since the merge marks an
         * empty window with NULL, which BOOLEAN cannot hold; the value's own type otherwise.
         */
        private int carriedType;
        private ColumnExpression count;
        private FunctionFactory.PartialMerge merge;
        private ColumnExpression merged;
        private int position;
        private ColumnExpression value;

        @Override
        public void clear() {
            count = null;
            merge = null;
            merged = null;
            value = null;
        }

        private WindowJoinPivotAggregate of(int position) {
            this.merge = null;
            this.value = null;
            this.count = null;
            this.merged = null;
            this.position = position;
            return this;
        }

        private void ofWindow(FunctionFactory.PartialMerge merge, ColumnExpression value, @Nullable ColumnExpression count) {
            this.merge = merge;
            this.value = value;
            this.count = count;
            this.carriedType = (merge == FunctionFactory.PartialMerge.FIRST || merge == FunctionFactory.PartialMerge.LAST) && ColumnType.isBoolean(value.getDataType())
                    ? ColumnType.INT : value.getDataType();
        }
    }
}
