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
import io.questdb.cairo.sql.Function;
import io.questdb.griffin.engine.groupby.GroupByUtils;
import io.questdb.griffin.engine.window.LiveViewWindowDescription;
import io.questdb.griffin.model.ExpressionNode;
import io.questdb.griffin.model.QueryModel;
import io.questdb.griffin.model.WindowExpression;
import io.questdb.griffin.plan.logical.AggregatePlan;
import io.questdb.griffin.plan.logical.BoundExpression;
import io.questdb.griffin.plan.logical.ColumnExpression;
import io.questdb.griffin.plan.logical.ConstantExpression;
import io.questdb.griffin.plan.logical.DistinctPlan;
import io.questdb.griffin.plan.logical.FillPlan;
import io.questdb.griffin.plan.logical.FilterPlan;
import io.questdb.griffin.plan.logical.FunctionExpression;
import io.questdb.griffin.plan.logical.GroupingPlan;
import io.questdb.griffin.plan.logical.LimitPlan;
import io.questdb.griffin.plan.logical.LogicalPlan;
import io.questdb.griffin.plan.logical.OutputSchema;
import io.questdb.griffin.plan.logical.ProjectPlan;
import io.questdb.griffin.plan.logical.SampleByPlan;
import io.questdb.griffin.plan.logical.SortDirection;
import io.questdb.griffin.plan.logical.SortPlan;
import io.questdb.griffin.plan.logical.WindowPlan;
import io.questdb.griffin.plan.logical.WindowSpec;
import io.questdb.std.Chars;
import io.questdb.std.Misc;
import io.questdb.std.Mutable;
import io.questdb.std.ObjList;
import io.questdb.std.datetime.CommonUtils;

import static io.questdb.griffin.BindContext.isWildcard;
import static io.questdb.griffin.BindContext.isWildcardColumn;
import static io.questdb.griffin.BindContext.sourceAlias;

final class SampleByBinder implements Mutable {
    private final SqlBinder binder;
    private final CairoConfiguration configuration;
    private final BindContext ctx;
    private final OutputSchema emptySchema;
    private final ObjList<BoundExpression> fillBindings = new ObjList<>();
    private final FunctionParser functionParser;
    private final JoinBinder joinBinder;
    private final WindowBinder windowBinder;

    SampleByBinder(
            BindContext ctx,
            SqlBinder binder,
            CairoConfiguration configuration,
            FunctionParser functionParser,
            OutputSchema emptySchema,
            WindowBinder windowBinder,
            JoinBinder joinBinder
    ) {
        this.ctx = ctx;
        this.binder = binder;
        this.configuration = configuration;
        this.functionParser = functionParser;
        this.emptySchema = emptySchema;
        this.windowBinder = windowBinder;
        this.joinBinder = joinBinder;
    }

    @Override
    public void clear() {
        fillBindings.clear();
    }

    private static boolean hasLinearFill(QueryModel source) {
        final ObjList<ExpressionNode> fill = source.getSampleByFill();
        for (int i = 0, n = fill.size(); i < n; i++) {
            if (SqlKeywords.isLinearKeyword(fill.getQuick(i).token)) {
                return true;
            }
        }
        return false;
    }

    private static boolean hasQuery(ExpressionNode node) {
        if (node == null) {
            return false;
        }
        if (node.type == ExpressionNode.QUERY || hasQuery(node.lhs) || hasQuery(node.rhs)) {
            return true;
        }
        for (int i = 0, n = node.args.size(); i < n; i++) {
            if (hasQuery(node.args.getQuick(i))) {
                return true;
            }
        }
        return false;
    }

    private static boolean hasSubsampleSourceTimestamp(LogicalPlan plan) {
        for (; plan != null; plan = plan.inputCount() > 0 ? plan.inputAt(0) : null) {
            if (plan.getOutput().getTimestampIndex() >= 0) {
                return true;
            }
        }
        return false;
    }

    private static boolean requiresSampleByCursorAlignment(QueryModel source) {
        final ExpressionNode from = source.getSampleByFrom();
        if (source.getSampleByOffset() == null || source.getSampleByUnit() != null
                || from != null && (from.type == ExpressionNode.BIND_VARIABLE || from.type == ExpressionNode.FUNCTION
                || from.type == ExpressionNode.OPERATION)) {
            return true;
        }
        return hasLinearFill(source);
    }

    private static void validateFillPrev(ExpressionNode expression) throws SqlException {
        if (expression.type != ExpressionNode.LITERAL
                && !(expression.type == ExpressionNode.FUNCTION && expression.paramCount == 1
                && expression.rhs != null && expression.rhs.type == ExpressionNode.LITERAL)) {
            throw SqlException.$(expression.position, "PREV argument must be a single column name");
        }
    }

    private BoundExpression bindFillValue(ExpressionNode expression, SqlExecutionContext executionContext) throws SqlException {
        if (expression.type == ExpressionNode.LITERAL || ctx.isAggregate(expression)) {
            throw GroupByUtils.invalidSampleByFillValue(expression.token, expression.position);
        }
        return ctx.functionBinder.bind(expression, emptySchema, null, executionContext);
    }

    private BoundExpression bindSampleByParameter(ExpressionNode expression, int type, SqlExecutionContext executionContext) throws SqlException {
        if (expression == null) {
            return null;
        }
        if (ctx.isAggregate(expression)) {
            throw SqlException.$(expression.position, "Aggregate function cannot be passed as an argument");
        }
        return ctx.functionBinder.bind(expression, emptySchema, null, type, executionContext);
    }

    /**
     * Binds the bound value as its own preparation, so the interval consumer adopts it, and compares the
     * timestamp against a placeholder for it.
     */
    private BoundExpression bindSampleByRangeComparison(LogicalPlan input, QueryModel source, CharSequence operator,
                                                        ExpressionNode bound, SqlExecutionContext executionContext) throws SqlException {
        final BoundExpression value = ctx.functionBinder.bind(bound, input.getOutput(), sourceAlias(source), executionContext);
        final int placeholderId = ctx.nextColumnId++;
        ctx.scratchScope.copyFrom(input.getOutput());
        ctx.scratchScope.add(placeholderId, "", value.getDataType(), false);
        ctx.substitutionNodes.clear();
        ctx.substitutionColumns.clear();
        ctx.substitutionNodes.add(bound);
        ctx.substitutionColumns.add(ctx.columns.next().of(placeholderId, value.getDataType(), bound.position));
        try {
            final BoundExpression comparison = ctx.functionBinder.bind(sampleByComparison(input.getOutput(), operator, bound),
                    ctx.scratchScope, sourceAlias(source), ctx.substitutionNodes, ctx.substitutionColumns, executionContext);
            return ctx.expressionRewriter.moveToColumn(comparison, placeholderId, value);
        } finally {
            ctx.substitutionNodes.clear();
            ctx.substitutionColumns.clear();
            ctx.scratchScope.clear();
        }
    }

    private ExpressionNode sampleByComparison(OutputSchema input, CharSequence operator, ExpressionNode bound) {
        final ExpressionNode comparison = ctx.bindingExpressions.next().of(ExpressionNode.OPERATION, operator, 0, 0);
        comparison.paramCount = 2;
        comparison.lhs = sampleByTimestamp(input, 0);
        comparison.rhs = bound;
        return comparison;
    }

    private ExpressionNode sampleByNull() {
        return ctx.bindingExpressions.next().of(ExpressionNode.CONSTANT, "null", 0, 0);
    }

    /**
     * The SAMPLE BY period as a stride literal; a constant period expression contributes its value.
     */
    private CharSequence sampleByPeriod(QueryModel source, GroupingPlan aggregate) {
        if (aggregate instanceof SampleByPlan sampleBy && sampleBy.getPeriod() instanceof ConstantExpression period) {
            final CharacterStoreEntry entry = ctx.characterStore.newEntry();
            entry.put(period.getLongValue()).put(sampleBy.getPeriodUnit());
            return entry.toImmutable();
        }
        return source.getSampleBy().token;
    }

    private ExpressionNode sampleByRangeBound(ExpressionNode bound, ExpressionNode timezone, int timestampType) {
        if (timezone == null) {
            return bound;
        }
        final ExpressionNode utc = sampleByToUtc(bound, timezone);
        if (timestampType == ColumnType.TIMESTAMP_MICRO) {
            return utc;
        }
        final ExpressionNode cast = ctx.bindingExpressions.next().of(ExpressionNode.FUNCTION, "cast", 0, bound.position);
        cast.paramCount = 2;
        cast.lhs = utc;
        cast.rhs = ctx.bindingExpressions.next().of(ExpressionNode.CONSTANT, ColumnType.nameOf(timestampType), 0, bound.position);
        return cast;
    }

    private ExpressionNode sampleByTimezone(QueryModel source) {
        final ExpressionNode timezone = source.getSampleByTimezoneName();
        return timezone != null && !SqlKeywords.isUTC(timezone.token) ? timezone : null;
    }

    private ExpressionNode sampleByToUtc(ExpressionNode value, ExpressionNode timezone) {
        final ExpressionNode call = ctx.bindingExpressions.next().of(ExpressionNode.FUNCTION, "to_utc", 0, value.position);
        call.paramCount = 2;
        call.lhs = value;
        call.rhs = timezone;
        return call;
    }

    private int subsampleTimestamp(LogicalPlan plan) {
        final OutputSchema output = plan.getOutput();
        switch (plan) {
            case ProjectPlan project -> {
                if (project.hasTimestampDeclaration()) {
                    return output.getTimestampColumnId();
                }
                final int inputId = subsampleTimestamp(project.getInput());
                for (int i = 0, n = project.getExpressions().size(); i < n; i++) {
                    if (project.getExpressions().getQuick(i) instanceof ColumnExpression column
                            && column.isDirectReference() && column.getColumnId() == inputId) {
                        return output.getColumnId(i);
                    }
                }
                return -1;
            }
            case AggregatePlan aggregate -> {
                final int inputId = subsampleTimestamp(aggregate.getInput());
                for (int i = 0, n = aggregate.getGroupingExpressions().size(); i < n; i++) {
                    if (aggregate.getGroupingExpressions().getQuick(i) instanceof ColumnExpression column
                            && column.isDirectReference() && column.getColumnId() == inputId) {
                        return output.getColumnId(i);
                    }
                }
                return -1;
            }
            case SortPlan _ -> {
                return output.getTimestampColumnId() >= 0 ? output.getTimestampColumnId() : subsampleTimestamp(plan.inputAt(0));
            }
            case DistinctPlan _, FilterPlan _, LimitPlan _, WindowPlan _ -> {
                final int id = subsampleTimestamp(plan.inputAt(0));
                return output.getColumnIndexById(id) < 0 ? -1 : id;
            }
            default -> {
                return output.getTimestampColumnId();
            }
        }
    }

    private int subsampleValueIndex(ExpressionNode value, OutputSchema output) throws SqlException {
        if (value.type != ExpressionNode.LITERAL) {
            if (value.type == ExpressionNode.CONSTANT) {
                throw SqlException.$(value.position, "SUBSAMPLE value argument must be a column name, not a constant");
            }
            if (value.type == ExpressionNode.BIND_VARIABLE) {
                throw SqlException.$(value.position, "SUBSAMPLE value argument must be a column name, not a bind variable");
            }
            throw SqlException.$(value.position,
                    "SUBSAMPLE value argument must be a column name; alias the expression in the SELECT list and reference the alias");
        }
        int index = output.getColumnIndexQuiet(value.token);
        if (index < 0) {
            index = output.getColumnIndexQuiet(SqlUtil.protectColumnAlias(ctx.characterStore, value.token));
        }
        if (index >= 0) {
            ctx.raiseDeferredColumn(output.getColumnId(index));
            return index;
        }
        if (Chars.indexOfLastUnquoted(value.token, '.') >= 0) {
            throw SqlException.$(value.position,
                    "qualified column names are not supported in SUBSAMPLE arguments; use the unqualified SELECT list name");
        }
        throw SqlException.$(value.position, "column not found in SELECT list: ").put(value.token);
    }

    private void validateSampleByQuery(QueryModel model, QueryModel source, OutputSchema input, boolean isBucket) throws SqlException {
        final ExpressionNode sampleBy = source.getSampleBy();
        if (source.getSampleByFrom() != null || source.getSampleByTo() != null) {
            boolean hasRangeTimestamp = source.getTimestamp() != null || source.getTableNameExpr() != null;
            if (!hasRangeTimestamp) {
                for (QueryModel current = model; current != null; current = current.getNestedModel()) {
                    if (current.getWhereClause() != null) {
                        hasRangeTimestamp = current.getTimestamp() != null || current.getTableNameExpr() != null;
                        break;
                    }
                }
            }
            if (input.getTimestampIndex() < 0 || !hasRangeTimestamp) {
                throw SqlException.$(sampleBy.position, "Sample by requires a designated TIMESTAMP");
            }
        }
        if (source.getGroupBy().size() > 0) {
            throw SqlException.$(source.getGroupBy().getQuick(0).position, "SELECT query must not contain both GROUP BY and SAMPLE BY");
        }
        boolean hasAggregate = false;
        ExpressionNode wildcard = null;
        for (int i = 0, n = model.getBottomUpColumns().size(); i < n; i++) {
            final ExpressionNode expression = model.getBottomUpColumns().getQuick(i).getAst();
            if (wildcard == null && isWildcard(expression)) {
                wildcard = expression;
            }
            hasAggregate |= ctx.hasAggregate(expression) || isBucket && expression.type == ExpressionNode.FUNCTION
                    && ctx.functionFactoryCache.isGroupBy(expression.token);
        }
        if (wildcard != null && input.getTimestampIndex() > -1 && !requiresSampleByCursorAlignment(source)) {
            throw SqlException.$(wildcard.position, "wildcard column select is not allowed in sample-by queries");
        }
        if (!hasAggregate) {
            throw SqlException.$(sampleBy.position, "at least one aggregation function must be present in 'select' clause");
        }
        if (input.getTimestampIndex() < 0) {
            throw SqlException.$(source.getModelPosition(), source.getJoinModels().size() > 1
                    ? "TIMESTAMP column is required but not provided"
                    : "base query does not provide designated TIMESTAMP column");
        }
        if (wildcard != null) {
            throw SqlException.$(wildcard.position, "wildcard column select is not allowed in sample-by queries");
        }
    }

    private void validateSdtCompdev(ExpressionNode compdev, SqlExecutionContext executionContext) throws SqlException {
        Function function = null;
        try {
            function = functionParser.parseFunction(ExpressionNode.deepClone(ctx.bindingExpressions, compdev), EmptyRecordMetadata.INSTANCE, executionContext);
            SubsampleValidator.validateSdtCompdev(function, compdev.position);
        } catch (SqlException e) {
            if (SubsampleValidator.hasUnresolvableSdtCompdevReference(compdev, executionContext)) {
                throw SqlException.$(compdev.position, "SUBSAMPLE sdt requires a constant, non-negative finite compdev");
            }
            throw e;
        } finally {
            Misc.free(function);
        }
    }

    /**
     * A {@link FillPlan} above the aggregate fills the gaps: any FILL other than NONE, in both SAMPLE BY
     * shapes. Only a FILL that interpolates stays in the SAMPLE BY cursor.
     */
    static boolean isFillPlanned(QueryModel source) {
        final ObjList<ExpressionNode> fill = source.getSampleByFill();
        return (fill.size() > 1 || fill.size() == 1 && !SqlKeywords.isNoneKeyword(fill.getQuick(0).token)) && !hasLinearFill(source);
    }

    static boolean requiresSampleByCursor(QueryModel source) {
        if (requiresSampleByCursorAlignment(source)) {
            return true;
        }
        final ExpressionNode from = source.getSampleByFrom();
        final ObjList<ExpressionNode> fill = source.getSampleByFill();
        return source.getTableNameExpr() == null && (source.getTimestamp() == null || from != null || source.getSampleByTo() != null
                || fill.size() > 1 || fill.size() == 1 && !SqlKeywords.isNoneKeyword(fill.getQuick(0).token));
    }

    /**
     * The FILL values follow the grouping's aggregates by position.
     */
    FillPlan bindFill(
            QueryModel source, GroupingPlan aggregate, ObjList<ExpressionNode> aggregateNodes, ObjList<ExpressionNode> fill,
            int timestampIndex, SqlExecutionContext executionContext
    ) throws SqlException {
        final int fillCount = fill.size();
        if (configuration.isValidateSampleByFillType()) {
            if (fillCount > 1 && fillCount < aggregate.getAggregates().size()) {
                boolean hasNone = false;
                for (int i = 0; i < fillCount; i++) {
                    hasNone |= SqlKeywords.isNoneKeyword(fill.getQuick(i).token);
                }
                if (!hasNone) {
                    throw SqlException.$(fill.getQuick(0).position, "not enough fill values");
                }
            }
            for (int i = 0, n = aggregate.getAggregates().size(); i < n; i++) {
                final ExpressionNode fillValue = fill.getQuick(Math.min(i, fillCount - 1));
                ctx.functionBinder.validateSampleByFill(aggregate.getAggregates().getQuick(i), fillValue.token,
                        fillValue.position, aggregateNodes.getQuick(i));
            }
        }
        if (fillCount > 1) {
            for (int i = 0; i < fillCount; i++) {
                final ExpressionNode expression = fill.getQuick(i);
                if (SqlKeywords.isNoneKeyword(expression.token)) {
                    throw SqlException.$(expression.position, "FILL(NONE) cannot be combined with other fill values");
                }
            }
        }
        fillBindings.clear();
        for (int i = 0; i < fillCount; i++) {
            final ExpressionNode expression = fill.getQuick(i);
            fillBindings.add(SqlKeywords.isPrevKeyword(expression.token) ? null : bindFillValue(expression, executionContext));
        }
        final FillPlan plan = ctx.fills.next().of(aggregate, source.getSampleBy().position);
        final OutputSchema output = aggregate.getOutput();
        plan.setTimestampColumnId(output.getColumnId(timestampIndex));
        plan.getOutput().setTimestampIndex(timestampIndex);
        final CharSequence period = sampleByPeriod(source, aggregate);
        plan.setPeriod(period, source.getSampleBy().position);
        final ExpressionNode timezone = sampleByTimezone(source);
        final boolean isSubDay = CommonUtils.isSubDayUnit(period.charAt(period.length() - 1));
        final ExpressionNode from = source.getSampleByFrom();
        final ExpressionNode to = source.getSampleByTo();
        final int timestampType = output.getColumnType(timestampIndex);
        plan.setFrom(bindSampleByParameter(from == null ? null : sampleByRangeBound(from, isSubDay ? timezone : null, timestampType),
                timestampType, executionContext));
        plan.setTo(bindSampleByParameter(to == null ? null : sampleByRangeBound(to, isSubDay ? timezone : null, timestampType),
                timestampType, executionContext));
        if (!isSubDay) {
            plan.setTimezone(bindSampleByParameter(timezone, ColumnType.STRING, executionContext));
        }
        final ExpressionNode offset = source.getSampleByOffset();
        if ((timezone == null || isSubDay && from != null || !isSubDay) && offset != null && offset != SqlParser.ZERO_OFFSET) {
            plan.setOffset(bindSampleByParameter(offset, ColumnType.STRING, executionContext));
        }
        final int aggregateCount = aggregate.getAggregates().size();
        final boolean isBroadcast = fillCount < aggregateCount;
        if (isBroadcast) {
            final ExpressionNode only = fillCount == 1 ? fill.getQuick(0) : null;
            if (only != null && SqlKeywords.isPrevKeyword(only.token) && only.type != ExpressionNode.LITERAL) {
                validateFillPrev(only);
                throw SqlException.$(only.position, "FILL(PREV(").put(only.rhs.token)
                        .put(")) cannot be broadcast across aggregates; specify one fill value per aggregate");
            }
            if (only == null || !(SqlKeywords.isPrevKeyword(only.token) && only.type == ExpressionNode.LITERAL
                    || SqlKeywords.isNullKeyword(only.token) || only.type == ExpressionNode.CONSTANT)) {
                throw SqlException.$(fill.getQuick(0).position, "not enough fill values");
            }
        }
        final int keyCount = aggregate.getGroupingExpressions().size();
        for (int i = 0; i < aggregateCount; i++) {
            final int fillIndex = isBroadcast ? 0 : i;
            final ExpressionNode expression = fill.getQuick(fillIndex);
            final int targetId = output.getColumnId(keyCount + i);
            int mode = FillPlan.FILL_VALUE;
            int sourceId = -1;
            int sourcePosition = 0;
            CharSequence token = expression.token;
            if (SqlKeywords.isPrevKeyword(expression.token)) {
                validateFillPrev(expression);
                mode = FillPlan.FILL_PREV;
                if (expression.type == ExpressionNode.FUNCTION) {
                    token = expression.rhs.token;
                    sourcePosition = expression.rhs.position;
                    int sourceIndex = -1;
                    final int lo = SqlUtil.isQuoteProtectedAlias(token) ? 1 : 0;
                    final int hi = lo == 0 ? token.length() : token.length() - 1;
                    for (int k = 0, n = output.getColumnCount(); k < n; k++) {
                        if (Chars.equalsIgnoreCase(output.getColumnName(k), token, lo, hi)) {
                            sourceIndex = k;
                            break;
                        }
                    }
                    if (sourceIndex < 0) {
                        throw SqlException.$(sourcePosition, "PREV(col): column not found in output: ").put(token);
                    }
                    if (sourceIndex == timestampIndex) {
                        throw SqlException.$(sourcePosition, "PREV cannot reference the designated timestamp column");
                    }
                    sourceId = output.getColumnId(sourceIndex);
                    if (sourceId != targetId) {
                        mode = FillPlan.FILL_PREV_COLUMN;
                    }
                }
            } else if (SqlKeywords.isNullKeyword(expression.token)) {
                mode = FillPlan.FILL_NULL;
            }
            plan.getTargetColumnIds().add(targetId);
            plan.getModes().add(mode);
            plan.getValues().add(mode == FillPlan.FILL_VALUE ? fillBindings.getQuick(fillIndex) : null);
            plan.getSourceColumnIds().add(sourceId);
            plan.getPositions().add(expression.position);
            plan.getSourcePositions().add(sourcePosition);
            plan.getTokens().add(token);
        }
        return plan;
    }

    SampleByPlan bindSampleBy(
            QueryModel model, QueryModel source, LogicalPlan input, SqlExecutionContext executionContext
    ) throws SqlException {
        final ExpressionNode sampleBy = source.getSampleBy();
        validateSampleByQuery(model, source, input.getOutput(), false);
        final int timestampIndex = input.getOutput().getTimestampIndex();

        final SampleByPlan plan = ctx.sampleByPlans.next().of(input, model.getModelPosition());
        plan.setTimestampColumnId(input.getOutput().getColumnId(timestampIndex));
        // LATEST ON names its timestamp without declaring the sampled input's order.
        plan.setTimestampRequired(source.getTimestamp() == null || !source.isExplicitTimestamp() && source.getLatestBy().size() > 0);
        plan.setJoinInput(source.getJoinModels().size() > 1);
        final int timestampType = input.getOutput().getColumnType(timestampIndex);
        plan.setTimezone(bindSampleByParameter(source.getSampleByTimezoneName(), ColumnType.STRING, executionContext));
        plan.setOffset(bindSampleByParameter(source.getSampleByOffset(), ColumnType.STRING, executionContext));
        plan.setFrom(bindSampleByParameter(source.getSampleByFrom(), timestampType, executionContext));
        plan.setTo(bindSampleByParameter(source.getSampleByTo(), timestampType, executionContext));
        final ExpressionNode unit = source.getSampleByUnit();
        if (unit == null) {
            plan.setPeriod(sampleBy.token, null, sampleBy.position,
                    sampleBy.token.isEmpty() ? (char) 0 : sampleBy.token.charAt(sampleBy.token.length() - 1), sampleBy.position);
        } else {
            final BoundExpression period = ctx.functionBinder.bind(sampleBy, emptySchema, null, executionContext);
            if (!(period instanceof ConstantExpression)
                    || period.getDataType() != ColumnType.INT && period.getDataType() != ColumnType.LONG) {
                throw SqlException.$(sampleBy.position, "sample by period must be a constant expression of INT or LONG type");
            }
            plan.setPeriod(null, period, sampleBy.position, unit.token.charAt(0), unit.position);
        }
        return plan;
    }

    BoundExpression bindSampleByBucket(
            QueryModel model, QueryModel source, OutputSchema input, SqlExecutionContext executionContext
    ) throws SqlException {
        final ExpressionNode sampleBy = source.getSampleBy();
        validateSampleByQuery(model, source, input, true);
        final ExpressionNode from = source.getSampleByFrom();
        final ExpressionNode timezone = sampleByTimezone(source);
        SqlUtil.validateSampleByTimezone(timezone, functionParser, executionContext);
        final boolean isSubDay = !sampleBy.token.isEmpty() && CommonUtils.isSubDayUnit(sampleBy.token.charAt(sampleBy.token.length() - 1));
        final ExpressionNode floor = ctx.bindingExpressions.next().of(ExpressionNode.FUNCTION, "timestamp_floor_utc", 0, 0);
        floor.paramCount = 5;
        floor.args.add(timezone != null && !(isSubDay && from != null) ? timezone : sampleByNull());
        floor.args.add(source.getSampleByOffset());
        floor.args.add(from == null ? sampleByNull() : isSubDay && timezone != null ? sampleByToUtc(from, timezone) : from);
        floor.args.add(sampleByTimestamp(input, sampleBy.position));
        final CharacterStoreEntry interval = ctx.characterStore.newEntry();
        interval.put('\'').put(sampleBy.token).put('\'');
        floor.args.add(ctx.bindingExpressions.next().of(ExpressionNode.CONSTANT, interval.toImmutable(), 0, sampleBy.position));
        return ctx.functionBinder.bind(floor, input, sourceAlias(source), executionContext);
    }

    void bindSampleByFill(QueryModel source, SampleByPlan plan, SqlExecutionContext executionContext) throws SqlException {
        if (!hasLinearFill(source)) {
            return;
        }
        final ObjList<ExpressionNode> fill = source.getSampleByFill();
        plan.setFillMode(fill.size() > 1 ? SampleByPlan.FILL_VALUE : SampleByPlan.FILL_NONE);
        for (int i = 0, n = fill.size(); i < n; i++) {
            final ExpressionNode value = fill.getQuick(i);
            final int mode = SqlKeywords.isNoneKeyword(value.token) ? SampleByPlan.FILL_NONE
                    : SqlKeywords.isPrevKeyword(value.token) ? SampleByPlan.FILL_PREV
                      : SqlKeywords.isNullKeyword(value.token) ? SampleByPlan.FILL_NULL
                        : SqlKeywords.isLinearKeyword(value.token) ? SampleByPlan.FILL_LINEAR : SampleByPlan.FILL_VALUE;
            if (n == 1) {
                plan.setFillMode(mode);
            }
            plan.getFillTokens().add(value.token);
            plan.getFillPositions().add(value.position);
            plan.getFillValues().add(mode == SampleByPlan.FILL_VALUE ? bindFillValue(value, executionContext) : null);
        }
    }

    LogicalPlan bindSampleByRange(QueryModel source, LogicalPlan input, SqlExecutionContext executionContext) throws SqlException {
        final ExpressionNode from = source.getSampleByFrom();
        final ExpressionNode to = source.getSampleByTo();
        if (from == null && to == null) {
            return input;
        }
        final ExpressionNode timezone = sampleByTimezone(source);
        final int timestampType = input.getOutput().getColumnType(input.getOutput().getTimestampIndex());
        final BoundExpression predicate;
        if (timezone == null) {
            final ExpressionNode lo = from == null ? null : sampleByComparison(input.getOutput(), ">=", from);
            final ExpressionNode hi = to == null ? null : sampleByComparison(input.getOutput(), "<", to);
            predicate = binder.bindPredicate(joinBinder.combineJoinPredicates(lo, hi), input, source, executionContext);
        } else if (to == null) {
            predicate = bindSampleByRangeComparison(input, source, ">=", sampleByRangeBound(from, timezone, timestampType), executionContext);
        } else if (from == null) {
            predicate = bindSampleByRangeComparison(input, source, "<", sampleByRangeBound(to, timezone, timestampType), executionContext);
        } else {
            final BoundExpression lo = bindSampleByRangeComparison(input, source, ">=",
                    sampleByRangeBound(from, timezone, timestampType), executionContext);
            final BoundExpression hi = bindSampleByRangeComparison(input, source, "<",
                    sampleByRangeBound(to, timezone, timestampType), executionContext);
            predicate = ctx.expressionRewriter.combineConjunction(lo, hi, hi.getPosition());
        }
        final FilterPlan filter = ctx.filters.next().of(input, predicate, predicate.getPosition());
        filter.deriveOutput();
        return filter;
    }

    LogicalPlan bindSubsample(
            LogicalPlan input, LogicalPlan sourcePlan, QueryModel source, SqlExecutionContext executionContext
    ) throws SqlException {
        final ExpressionNode subsample = source.getSubsample();
        final boolean isCadence = Chars.equalsIgnoreCase(subsample.token, "cadence");
        final boolean isPositionOnly = isCadence || Chars.equalsIgnoreCase(subsample.token, "uniform");
        final boolean isSdt = Chars.equalsIgnoreCase(subsample.token, "sdt");
        final boolean isLttb = Chars.equalsIgnoreCase(subsample.token, "lttb");
        validateSubsampleCall(subsample);
        final OutputSchema output = input.getOutput();
        final int timestampId = subsampleTimestamp(input);
        final int timestampIndex = output.getColumnIndexById(timestampId);
        int valueIndex = -1;
        if (!isPositionOnly && !isSdt) {
            valueIndex = subsampleValueIndex(subsample.args.getQuick(0), output);
        }
        if (timestampIndex < 0 || !output.isVisible(timestampIndex)) {
            throw subsampleTimestampMissing(source, sourcePlan);
        }
        if (isSdt) {
            valueIndex = subsampleValueIndex(subsample.args.getQuick(0), output);
        } else if (!isPositionOnly) {
            SubsampleValidator.validateNumericType(output.getColumnType(valueIndex), subsample.args.getQuick(0).position);
        }
        final ExpressionNode target = subsample.args.getQuick(isPositionOnly ? 0 : 1);
        if (!isSdt && target.type == ExpressionNode.LITERAL) {
            throw SqlException.$(target.position, isCadence ? "stride" : "target point count")
                    .put(" must be a constant or bind variable");
        }
        if (isCadence && subsample.paramCount == 2 && subsample.args.getQuick(1).type == ExpressionNode.LITERAL) {
            throw SqlException.$(subsample.args.getQuick(1).position, "seed must be a constant, bind variable, or NULL");
        }
        if (isSdt) {
            if (!hasQuery(target)) {
                validateSdtCompdev(target, executionContext);
            }
        } else if (!hasQuery(target)) {
            SubsampleValidator.validatePositionTargetOrThrow(ExpressionNode.deepClone(ctx.bindingExpressions, target), isCadence, functionParser, executionContext);
            if (isCadence && subsample.paramCount == 2 && !hasQuery(subsample.args.getQuick(1))) {
                SubsampleValidator.validateCadenceSeedOrThrow(ExpressionNode.deepClone(ctx.bindingExpressions, subsample.args.getQuick(1)), functionParser, executionContext);
            }
        }
        final ExpressionNode call = ctx.bindingExpressions.next().of(ExpressionNode.FUNCTION,
                subsample.token, 0, subsample.position);
        call.paramCount = isPositionOnly ? subsample.paramCount : subsample.paramCount + 1;
        if (!isPositionOnly) {
            if (isLttb && subsample.paramCount == 3) {
                call.args.add(ExpressionNode.deepClone(ctx.bindingExpressions, subsample.args.getQuick(2)));
            }
            call.args.add(ExpressionNode.deepClone(ctx.bindingExpressions, target));
            call.args.add(ctx.bindingExpressions.next().of(ExpressionNode.LITERAL,
                    SqlUtil.protectColumnAlias(ctx.characterStore, output.getColumnName(valueIndex)), 0, subsample.args.getQuick(0).position));
            call.args.add(ctx.bindingExpressions.next().of(ExpressionNode.LITERAL,
                    SqlUtil.protectColumnAlias(ctx.characterStore, output.getColumnName(timestampIndex)), 0, subsample.position));
        } else if (call.paramCount == 1) {
            call.rhs = ExpressionNode.deepClone(ctx.bindingExpressions, target);
        } else {
            call.lhs = ExpressionNode.deepClone(ctx.bindingExpressions, target);
            call.rhs = ExpressionNode.deepClone(ctx.bindingExpressions, subsample.args.getQuick(1));
        }
        final WindowExpression syntax = ctx.windowSyntax.next();
        syntax.setSubsampleKeepFlag(true);
        syntax.setPendingSubsample(subsample, source.getSubsamplePosition(), subsampleTimestamp(sourcePlan) >= 0);
        syntax.addOrderBy(ctx.bindingExpressions.next().of(ExpressionNode.LITERAL,
                SqlUtil.protectColumnAlias(ctx.characterStore, output.getColumnName(timestampIndex)), 0, subsample.position), QueryModel.ORDER_DIRECTION_ASCENDING);
        call.windowExpression = syntax;
        SqlUtil.normalizeWindowFrame(syntax, functionParser, executionContext);
        final WindowSpec spec = ctx.windowSpecs.next().of(syntax);
        if (executionContext.isLiveViewCompile()) {
            spec.setLiveViewDescription(LiveViewWindowDescription.of(syntax));
        }
        spec.getOrderByColumnIds().add(timestampId);
        spec.getOrderByDirections().add(SortDirection.ASCENDING);
        spec.getOrderByNames().add(output.getColumnName(timestampIndex));
        spec.getOrderByPositions().add(subsample.position);
        final FunctionExpression function;
        try {
            function = windowBinder.bindWindowFunction(call, spec, output, source, executionContext);
        } catch (SqlException e) {
            if (isSdt && SubsampleValidator.hasUnresolvableSdtCompdevReference(target, executionContext)) {
                throw SqlException.$(target.position, "SUBSAMPLE sdt requires a constant, non-negative finite compdev");
            }
            throw e;
        }
        final WindowPlan window = ctx.windowPlans.next().of(input, subsample.position);
        final int keepId = ctx.nextColumnId++;
        window.getFunctions().add(function);
        window.getSpecs().add(spec);
        window.getFunctionColumnIds().add(keepId);
        window.getOutput().copyFrom(output);
        ctx.aliases.clear();
        ctx.aliasSequences.clear();
        for (int i = 0, n = output.getColumnCount(); i < n; i++) {
            ctx.aliases.add(output.getColumnName(i));
        }
        window.getOutput().add(keepId, ctx.createOutputName("__keep_subsample"), ColumnType.BOOLEAN, false);
        final FilterPlan filter = ctx.filters.next().of(window, ctx.columns.next().of(keepId, ColumnType.BOOLEAN, subsample.position), subsample.position);
        filter.deriveOutput();
        final ProjectPlan project = ctx.projects.next().of(filter, subsample.position);
        project.getOutput().copyFrom(output);
        for (int i = 0, n = output.getColumnCount(); i < n; i++) {
            project.getExpressions().add(ctx.columns.next().of(output.getColumnId(i), output.getColumnType(i), subsample.position));
        }
        ctx.stopTimestampIntrinsics(project.getOutput());
        return project;
    }

    boolean hasSubsampleTimestampProjection(QueryModel model, QueryModel source, LogicalPlan sourcePlan) {
        final OutputSchema output = sourcePlan.getOutput();
        final int timestampId = subsampleTimestamp(sourcePlan);
        final int timestampIndex = timestampId < 0 ? -1 : output.getColumnIndexById(timestampId);
        for (int i = 0, n = model.getBottomUpColumns().size(); i < n; i++) {
            final ExpressionNode expression = model.getBottomUpColumns().getQuick(i).getAst();
            if (expression.type != ExpressionNode.LITERAL) {
                continue;
            }
            if (isWildcard(expression)) {
                if (timestampIndex < 0 || isWildcardColumn(expression, output, timestampIndex, sourceAlias(source))) {
                    return true;
                }
            } else if (timestampIndex >= 0 && FunctionBinder.findColumn(expression, output, sourceAlias(source)) == timestampIndex) {
                return true;
            }
        }
        return false;
    }

    boolean referencesSampleByTimestamp(ExpressionNode expression, OutputSchema input, QueryModel source) throws SqlException {
        if (expression == null || ctx.isAggregate(expression)) {
            return false;
        }
        if (expression.type == ExpressionNode.LITERAL) {
            return ctx.bindColumnIndex(expression, input, source) == input.getTimestampIndex();
        }
        if (expression.paramCount < 3) {
            return referencesSampleByTimestamp(expression.lhs, input, source) || referencesSampleByTimestamp(expression.rhs, input, source);
        }
        for (int i = 0, n = expression.args.size(); i < n; i++) {
            if (referencesSampleByTimestamp(expression.args.getQuick(i), input, source)) {
                return true;
            }
        }
        return false;
    }

    ExpressionNode sampleByTimestamp(OutputSchema input, int position) {
        final int index = input.getTimestampIndex();
        final CharSequence qualifier = input.getColumnQualifier(index);
        CharSequence name = input.getColumnName(index);
        if (qualifier != null) {
            final CharacterStoreEntry token = ctx.characterStore.newEntry();
            token.put('"').put(qualifier).put("\".").put(name);
            name = token.toImmutable();
        }
        return ctx.bindingExpressions.next().of(ExpressionNode.LITERAL, name, 0, position);
    }

    SqlException subsampleTimestampMissing(QueryModel source, LogicalPlan sourcePlan) {
        final ExpressionNode subsample = source.getSubsample();
        final SqlException exception = SqlException.$(Chars.equalsIgnoreCase(subsample.token, "sdt") ? subsample.position : source.getSubsamplePosition(),
                "SUBSAMPLE requires a designated timestamp column; ");
        return hasSubsampleSourceTimestamp(sourcePlan)
                ? exception.put("the SELECT list must include it unchanged")
                : exception.put("the query source has no designated timestamp");
    }

    void validateSubsampleCall(ExpressionNode subsample) throws SqlException {
        final boolean isCadence = Chars.equalsIgnoreCase(subsample.token, "cadence");
        final boolean isPositionOnly = isCadence || Chars.equalsIgnoreCase(subsample.token, "uniform");
        final boolean isSdt = Chars.equalsIgnoreCase(subsample.token, "sdt");
        final boolean isLttb = Chars.equalsIgnoreCase(subsample.token, "lttb");
        if (!isPositionOnly && !isSdt && !isLttb
                && !Chars.equalsIgnoreCase(subsample.token, "m4") && !Chars.equalsIgnoreCase(subsample.token, "minmax")) {
            throw SqlException.$(subsample.position, "unknown subsample method: ").put(subsample.token)
                    .put(". Supported methods: lttb, m4, minmax, uniform, cadence, sdt");
        }
        if (isPositionOnly && (isCadence ? subsample.paramCount < 1 || subsample.paramCount > 2 : subsample.paramCount != 1)) {
            throw SqlException.$(subsample.position, isCadence
                    ? "cadence() requires 1 or 2 arguments: stride and optional seed"
                    : "uniform() requires exactly 1 argument: target points");
        }
        if (!isPositionOnly && !isSdt) {
            if (subsample.paramCount < 2) {
                throw SqlException.$(subsample.position, subsample.token).put("() requires at least 2 arguments: column and target points");
            }
            final int maxArgs = isLttb ? 3 : 2;
            if (subsample.paramCount > maxArgs) {
                throw SqlException.$(subsample.args.getQuick(maxArgs).position, subsample.token).put(isLttb
                        ? "() accepts at most 3 arguments: column, target points, and optional gap threshold"
                        : "() accepts exactly 2 arguments: column and target points");
            }
        }
    }

    void validateSubsampleSelectValue(ExpressionNode value, QueryModel model) throws SqlException {
        if (value.type == ExpressionNode.LITERAL) {
            final CharSequence protectedName = SqlUtil.protectColumnAlias(ctx.characterStore, value.token);
            for (int i = 0, n = model.getBottomUpColumns().size(); i < n; i++) {
                final CharSequence name = model.getBottomUpColumns().getQuick(i).getName();
                if (Chars.equalsIgnoreCase(name, value.token) || Chars.equalsIgnoreCase(name, protectedName)) {
                    return;
                }
            }
        }
        subsampleValueIndex(value, emptySchema);
    }
}
