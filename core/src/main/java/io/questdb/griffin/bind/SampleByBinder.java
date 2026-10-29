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
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.ImplicitCastException;
import io.questdb.cairo.TimestampDriver;
import io.questdb.cairo.sql.Function;
import io.questdb.griffin.CharacterStoreEntry;
import io.questdb.griffin.FunctionResolver;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlKeywords;
import io.questdb.griffin.SqlParser;
import io.questdb.griffin.SqlUtil;
import io.questdb.griffin.SubsampleValidator;
import io.questdb.griffin.engine.functions.constants.BooleanConstant;
import io.questdb.griffin.engine.functions.constants.ByteConstant;
import io.questdb.griffin.engine.functions.constants.CharConstant;
import io.questdb.griffin.engine.functions.constants.DateConstant;
import io.questdb.griffin.engine.functions.constants.DoubleConstant;
import io.questdb.griffin.engine.functions.constants.FloatConstant;
import io.questdb.griffin.engine.functions.constants.IPv4Constant;
import io.questdb.griffin.engine.functions.constants.IntConstant;
import io.questdb.griffin.engine.functions.constants.LongConstant;
import io.questdb.griffin.engine.functions.constants.ShortConstant;
import io.questdb.griffin.engine.groupby.GroupByUtils;
import io.questdb.griffin.engine.groupby.TimestampSamplerFactory;
import io.questdb.griffin.engine.window.LiveViewWindowDescription;
import io.questdb.griffin.model.ExpressionNode;
import io.questdb.griffin.model.QueryModel;
import io.questdb.griffin.model.WindowExpression;
import io.questdb.griffin.plan.logical.AggregatePlan;
import io.questdb.griffin.plan.logical.BoundExpression;
import io.questdb.griffin.plan.logical.ColumnExpression;
import io.questdb.griffin.plan.logical.ConstantExpression;
import io.questdb.griffin.plan.logical.DistinctPlan;
import io.questdb.griffin.plan.logical.FillMode;
import io.questdb.griffin.plan.logical.FillPlan;
import io.questdb.griffin.plan.logical.FilterPlan;
import io.questdb.griffin.plan.logical.FunctionExpression;
import io.questdb.griffin.plan.logical.GroupingPlan;
import io.questdb.griffin.plan.logical.JoinPlan;
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
import io.questdb.std.IntList;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.NumericException;
import io.questdb.std.ObjList;
import io.questdb.std.Uuid;
import io.questdb.std.datetime.CommonUtils;
import io.questdb.std.datetime.DateLocaleFactory;

import static io.questdb.griffin.bind.BindContext.*;

final class SampleByBinder {
    private static final String FROM_BOUND_ERROR = "from lower bound must be a constant expression convertible to a TIMESTAMP";
    private static final String OFFSET_ERROR = "offset must be a constant expression of STRING or CHAR type";
    private static final String TO_BOUND_ERROR = "to upper bound must be a constant expression convertible to a TIMESTAMP";
    private final SqlBinder binder;
    private final CairoConfiguration configuration;
    private final BindContext ctx;
    private final Uuid fillUuid = new Uuid();
    private final FunctionResolver functionResolver;
    private final WindowBinder windowBinder;

    SampleByBinder(
            BindContext ctx,
            SqlBinder binder,
            CairoConfiguration configuration,
            FunctionResolver functionResolver,
            WindowBinder windowBinder
    ) {
        this.ctx = ctx;
        this.binder = binder;
        this.configuration = configuration;
        this.functionResolver = functionResolver;
        this.windowBinder = windowBinder;
    }

    /**
     * The column the buckets floor: the input's designated timestamp or, over a join whose step NULL-extends its first
     * input and so designates none, the first input's. Bucketing requires no row order, and the NULL-extended rows form
     * a NULL bucket.
     */
    private static int bucketTimestampColumnId(LogicalPlan input) {
        final int timestampId = input.getOutput().getTimestampColumnId();
        return timestampId < 0 && input instanceof JoinPlan join
                ? join.getInputs().getQuick(0).getSourceOutput().getTimestampColumnId()
                : timestampId;
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
                || from.type == ExpressionNode.OPERATION || from.type == ExpressionNode.QUERY)) {
            return true;
        }
        return hasLinearFill(source);
    }

    // Reads the value through the getter SampleByFillRecordCursorFactory's record uses for the target type.
    private static Function toFillConstant(Function fill, int targetType, CharSequence fillToken, int fillPosition) throws SqlException {
        try {
            return switch (ColumnType.tagOf(targetType)) {
                case ColumnType.BOOLEAN -> BooleanConstant.of(fill.getBool(null));
                case ColumnType.BYTE -> ByteConstant.newInstance((byte) fill.getInt(null));
                case ColumnType.SHORT -> ShortConstant.newInstance((short) fill.getInt(null));
                case ColumnType.CHAR -> CharConstant.newInstance(fill.getChar(null));
                case ColumnType.INT -> IntConstant.newInstance(fill.getInt(null));
                case ColumnType.IPv4 -> IPv4Constant.newInstance(fill.getIPv4(null));
                case ColumnType.LONG -> LongConstant.newInstance(fill.getLong(null));
                case ColumnType.DATE -> DateConstant.newInstance(fill.getLong(null));
                case ColumnType.FLOAT -> FloatConstant.newInstance(fill.getFloat(null));
                case ColumnType.DOUBLE -> DoubleConstant.newInstance(fill.getDouble(null));
                case ColumnType.LONG256 -> {
                    fill.getLong256A(null);
                    yield fill;
                }
                default -> fill;
            };
        } catch (ImplicitCastException | UnsupportedOperationException e) {
            throw GroupByUtils.invalidSampleByFillValue(fillToken, fillPosition);
        }
    }

    /**
     * Fails with {@code message} at the expression when its type, once known, does not convert to {@code type}.
     */
    private static void validateConvertible(BoundExpression expression, int type, CharSequence message) throws SqlException {
        if (expression != null && expression.getDataType() != ColumnType.UNDEFINED && !ColumnType.isConvertibleFrom(expression.getDataType(), type)) {
            throw SqlException.$(expression.getPosition(), message);
        }
    }

    private static void validateFillNull(int type, int position) throws SqlException {
        final int tag = ColumnType.tagOf(type);
        if (tag == ColumnType.BOOLEAN || tag == ColumnType.CHAR) {
            throw SqlException.$(position, "fill value of type NULL cannot fill column of type ")
                    .put(ColumnType.nameOf(type));
        }
    }

    private static void validateFillPrev(ExpressionNode expression) throws SqlException {
        if (expression.type != ExpressionNode.LITERAL
                && !(expression.type == ExpressionNode.FUNCTION && expression.paramCount == 1
                && expression.rhs != null && expression.rhs.type == ExpressionNode.LITERAL)) {
            throw SqlException.$(expression.position, "PREV argument must be a single column name");
        }
    }

    private static void validateFillPrevType(int targetType, int sourceType, CharSequence sourceName, int position) throws SqlException {
        final int targetTag = ColumnType.tagOf(targetType);
        final int sourceTag = ColumnType.tagOf(sourceType);
        if (targetTag == ColumnType.SYMBOL || sourceTag == ColumnType.SYMBOL) {
            throw SqlException.$(position, "FILL(PREV(").put(sourceName)
                    .put(")) is not supported on SYMBOL columns; use bare FILL(PREV) instead");
        }
        final boolean needsExactType = ColumnType.isDecimal(targetType) || ColumnType.isGeoHash(targetType)
                || targetTag == ColumnType.ARRAY || targetTag == ColumnType.TIMESTAMP || targetTag == ColumnType.INTERVAL;
        if (needsExactType ? targetType != sourceType : targetTag != sourceTag) {
            throw SqlException.$(position, "FILL(PREV(").put(sourceName).put(")): source type ")
                    .put(ColumnType.nameOf(sourceType)).put(" cannot fill target column of type ")
                    .put(ColumnType.nameOf(targetType));
        }
    }

    private static void validateRuntimeConstant(BoundExpression expression, int type, CharSequence message) throws SqlException {
        if (expression != null && expression.getDataType() != ColumnType.UNDEFINED
                && (expression.getFunctionFlags() & (BoundExpression.CONSTANT | BoundExpression.RUNTIME_CONSTANT)) == 0) {
            throw SqlException.$(expression.getPosition(), message);
        }
        validateConvertible(expression, type, message);
    }

    /**
     * The FILL value as a constant the fill reads for a column of {@code targetType}: a timestamp parses from the
     * quoted token and a convertible constant takes the target's representation. An array value stays as bound and
     * its consumer casts it.
     */
    private BoundExpression bindFillConstant(
            BoundExpression value,
            int targetType,
            CharSequence fillToken,
            int fillPosition,
            SqlExecutionContext executionContext
    ) throws SqlException {
        if (ColumnType.isTimestamp(targetType)) {
            // The target's driver parses the quoted token, so the value takes the target's precision.
            if (!Chars.isQuoted(fillToken)) {
                throw SqlException.position(fillPosition).put("Invalid fill value: '").put(fillToken)
                        .put("'. Timestamp fill value must be in quotes. Example: '2019-01-01T00:00:00.000Z'");
            }
            try {
                return ctx.planNodes.constants.next().ofTimestamp(
                        ColumnType.getTimestampDriver(targetType).parseQuotedLiteral(fillToken), targetType, value.getPosition());
            } catch (NumericException e) {
                throw GroupByUtils.invalidSampleByFillValue(fillToken, fillPosition);
            }
        }
        final int fillType = value.getDataType();
        if (fillType != ColumnType.UNDEFINED && !ColumnType.isConvertibleFrom(fillType, targetType)) {
            throw SqlException.$(fillPosition, "fill value of type ").put(ColumnType.nameOf(fillType))
                    .put(" cannot fill column of type ").put(ColumnType.nameOf(targetType));
        }
        if ((value.getFunctionFlags() & BoundExpression.NON_DETERMINISTIC) != 0 || (value.getFunctionFlags() & BoundExpression.CONSTANT) == 0) {
            throw SqlException.$(fillPosition, "fill value must be a constant expression");
        }
        Function fill = ctx.functionInstantiator.instantiate(value, ctx.emptySchema, executionContext);
        try {
            if (ColumnType.tagOf(targetType) == ColumnType.UUID) {
                validateUuidFill(fill, fillToken, fillPosition);
            }
            // An implicit cast factory turns e.g. INT into DECIMAL; without it the
            // fill record would call a getter the value's function does not implement.
            if (fillType != targetType && !ColumnType.isBuiltInWideningCast(fillType, targetType)) {
                final Function source = fill;
                fill = null;
                final Function cast;
                try {
                    cast = functionResolver.createImplicitCast(fillPosition, source, targetType, executionContext);
                } catch (ImplicitCastException e) {
                    throw GroupByUtils.invalidSampleByFillValue(fillToken, fillPosition);
                }
                fill = cast == null ? source : cast;
            }
            if (ColumnType.isArray(targetType)) {
                return value;
            }
            if (fill.getType() != ColumnType.NULL && ColumnType.tagOf(fill.getType()) != ColumnType.tagOf(targetType)) {
                final Function constant = toFillConstant(fill, targetType, fillToken, fillPosition);
                if (constant != fill) {
                    Misc.free(fill);
                    fill = constant;
                }
            }
            return ctx.functionBinder.constant(fill, value.getPosition());
        } finally {
            Misc.free(fill);
        }
    }

    private BoundExpression bindFillValue(ExpressionNode expression, SqlExecutionContext executionContext) throws SqlException {
        if (expression.type == ExpressionNode.LITERAL || ctx.isAggregate(expression)) {
            throw GroupByUtils.invalidSampleByFillValue(expression.token, expression.position);
        }
        return ctx.functionBinder.bind(expression, ctx.emptySchema, null, executionContext);
    }

    private BoundExpression bindSampleByBound(ExpressionNode bound, int timestampType, CharSequence error, SqlExecutionContext executionContext) throws SqlException {
        final BoundExpression expression = bindSampleByParameter(bound, timestampType, executionContext);
        validateRuntimeConstant(expression, timestampType, error);
        return expression;
    }

    /**
     * The OFFSET a SAMPLE BY cursor or fill reads, which must be a constant or runtime-constant expression of a type
     * convertible to STRING: the node {@link #sampleByOffset} returns, and, for an offset other than a literal,
     * {@code coalesce(offset, '00:00')}, which reads a NULL offset as the default when the cursor opens, so that a NULL
     * offset aligns the buckets as an omitted one does. Binds the offset once; an offset {@code coalesce} cannot take
     * is bound alone to report its own error or the OFFSET error.
     */
    private BoundExpression bindSampleByOffset(ExpressionNode offset, SqlExecutionContext executionContext) throws SqlException {
        if (offset == null || offset.type == ExpressionNode.CONSTANT) {
            final BoundExpression bound = bindSampleByParameter(offset, ColumnType.STRING, executionContext);
            validateRuntimeConstant(bound, ColumnType.STRING, OFFSET_ERROR);
            return bound;
        }
        final BoundExpression bound;
        try {
            bound = bindSampleByParameter(ctx.binaryNode(ExpressionNode.FUNCTION, "coalesce", 0, offset, SqlParser.ZERO_OFFSET, offset.position),
                    ColumnType.STRING, executionContext);
        } catch (SqlException e) {
            validateRuntimeConstant(bindSampleByParameter(offset, ColumnType.STRING, executionContext), ColumnType.STRING, OFFSET_ERROR);
            throw e;
        }
        validateRuntimeConstant(bound, ColumnType.STRING, OFFSET_ERROR);
        return bound;
    }

    private BoundExpression bindSampleByParameter(ExpressionNode expression, int type, SqlExecutionContext executionContext) throws SqlException {
        if (expression == null) {
            return null;
        }
        if (ctx.isAggregate(expression)) {
            throw SqlException.$(expression.position, "Aggregate function cannot be passed as an argument");
        }
        return ctx.functionBinder.bind(expression, ctx.emptySchema, null, type, executionContext);
    }

    /**
     * Binds the bound value as its own preparation, so the interval consumer adopts it, and compares the
     * timestamp against a placeholder for it.
     */
    private BoundExpression bindSampleByRangeComparison(LogicalPlan input, QueryModel source, CharSequence operator,
                                                        ExpressionNode bound, SqlExecutionContext executionContext) throws SqlException {
        final BindScope scope = ctx.scope();
        final BoundExpression value = ctx.functionBinder.bind(bound, input.getOutput(), sourceAlias(source), executionContext);
        final int placeholderId = ctx.planNodes.nextColumnId();
        ctx.tmpScope.copyFrom(input.getOutput());
        ctx.tmpScope.add(placeholderId, "", value.getDataType(), false);
        scope.substitutionNodes.clear();
        scope.substitutionColumns.clear();
        scope.substitutionNodes.add(bound);
        scope.substitutionColumns.add(ctx.planNodes.columns.next().of(placeholderId, value.getDataType(), bound.position));
        try {
            final BoundExpression comparison = ctx.functionBinder.bind(sampleByComparison(input.getOutput(), operator, bound),
                    ctx.tmpScope, sourceAlias(source), scope.substitutionNodes, scope.substitutionColumns, executionContext);
            return ctx.expressionRewriter.moveToColumn(comparison, placeholderId, value);
        } finally {
            scope.substitutionNodes.clear();
            scope.substitutionColumns.clear();
            ctx.tmpScope.clear();
        }
    }

    /**
     * Scalar sub-query bounds bind as runtime-constant timestamps on every SAMPLE BY path: the bucket, the cursor,
     * the fill and the range filter.
     */
    private void markTimestampSubqueries(QueryModel source, int timestampType) {
        final BindScope scope = ctx.scope();
        scope.timestampSubqueryType = timestampType;
        final ExpressionNode from = source.getSampleByFrom();
        scope.timestampFromSubquery = from != null && from.type == ExpressionNode.QUERY ? from.queryModel : null;
        final ExpressionNode to = source.getSampleByTo();
        scope.timestampToSubquery = to != null && to.type == ExpressionNode.QUERY ? to.queryModel : null;
    }

    private ExpressionNode sampleByComparison(OutputSchema input, CharSequence operator, ExpressionNode bound) {
        return ctx.binaryNode(ExpressionNode.OPERATION, operator, 0, sampleByTimestamp(input, 0), bound, 0);
    }

    private ExpressionNode sampleByNull() {
        return ctx.bindingExpressions.next().of(ExpressionNode.CONSTANT, "null", 0, 0);
    }

    /**
     * The OFFSET node the SAMPLE BY binds, see {@link #bindSampleByOffset}: none without an OFFSET, the default for a
     * NULL literal, and any other offset as written.
     */
    private ExpressionNode sampleByOffset(QueryModel source) {
        final ExpressionNode offset = source.getSampleByOffset();
        return offset != null && offset.type == ExpressionNode.CONSTANT && SqlKeywords.isNullKeyword(offset.token)
                ? SqlParser.ZERO_OFFSET : offset;
    }

    private ExpressionNode sampleByRangeBound(ExpressionNode bound, ExpressionNode timezone, int timestampType) {
        if (bound == null || timezone == null) {
            return bound;
        }
        final ExpressionNode utc = sampleByToUtc(bound, timezone);
        if (timestampType == ColumnType.TIMESTAMP_MICRO) {
            return utc;
        }
        return ctx.binaryNode(ExpressionNode.FUNCTION, "cast", 0, utc,
                ctx.bindingExpressions.next().of(ExpressionNode.CONSTANT, ColumnType.nameOf(timestampType), 0, bound.position), bound.position);
    }

    private ExpressionNode sampleByTimezone(QueryModel source) {
        final ExpressionNode timezone = source.getSampleByTimezoneName();
        return timezone != null && !SqlKeywords.isUTC(timezone.token) ? timezone : null;
    }

    private ExpressionNode sampleByToUtc(ExpressionNode value, ExpressionNode timezone) {
        return ctx.binaryNode(ExpressionNode.FUNCTION, "to_utc", 0, value, timezone, value.position);
    }

    /**
     * A minus sign over a numeric literal as one signed constant, so the fill parses the lowest INT and LONG literals
     * as their NULL and names the signed value in errors.
     */
    private ExpressionNode signedFillLiteral(ExpressionNode expression) {
        final ExpressionNode operand = expression.rhs;
        if (expression.type != ExpressionNode.OPERATION || expression.paramCount != 1 || !Chars.equals(expression.token, '-')
                || operand == null || operand.type != ExpressionNode.CONSTANT || operand.token.isEmpty()
                || !(Character.isDigit(operand.token.charAt(0)) || operand.token.charAt(0) == '.')) {
            return expression;
        }
        final CharacterStoreEntry entry = ctx.characterStore.newEntry();
        entry.put('-').put(operand.token);
        return ctx.bindingExpressions.next().of(ExpressionNode.CONSTANT, entry.toImmutable(), 0, expression.position);
    }

    /**
     * The function of a SUBSAMPLE argument bound on its own, see {@link FunctionBinder#bindStandalone}.
     */
    private Function subsampleArgument(ExpressionNode argument, SqlExecutionContext executionContext) throws SqlException {
        return ctx.preparedRoot(ctx.functionBinder.bindStandalone(argument, executionContext));
    }

    private int subsampleTimestamp(LogicalPlan plan) {
        final OutputSchema output = plan.getOutput();
        switch (plan) {
            case ProjectPlan project -> {
                if (project.hasTimestampDeclaration()) {
                    return output.getTimestampColumnId();
                }
                final int inputId = subsampleTimestamp(project.getChild());
                for (int i = 0, n = project.getExpressions().size(); i < n; i++) {
                    if (project.getExpressions().getQuick(i) instanceof ColumnExpression column
                            && column.isDirectReference() && column.getColumnId() == inputId) {
                        return output.getColumnId(i);
                    }
                }
                return -1;
            }
            case AggregatePlan aggregate -> {
                final int inputId = subsampleTimestamp(aggregate.getChild());
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
            return index;
        }
        if (FunctionBinder.qualifierEnd(value, output) >= 0) {
            throw SqlException.$(value.position,
                    "qualified column names are not supported in SUBSAMPLE arguments; use the unqualified SELECT list name");
        }
        throw SqlException.$(value.position, "column not found in SELECT list: ").put(value.token);
    }

    private void validateFill(
            ObjList<FunctionExpression> aggregates, ObjList<ExpressionNode> aggregateNodes, ObjList<ExpressionNode> fill
    ) throws SqlException {
        final int fillCount = fill.size();
        if (configuration.isValidateSampleByFillType()) {
            if (fillCount > 1 && fillCount < aggregates.size()) {
                boolean hasNone = false;
                for (int i = 0; i < fillCount; i++) {
                    hasNone |= SqlKeywords.isNoneKeyword(fill.getQuick(i).token);
                }
                if (!hasNone) {
                    throw SqlException.$(fill.getQuick(0).position, "not enough fill values");
                }
            }
            for (int i = 0, n = aggregates.size(); i < n; i++) {
                final ExpressionNode fillValue = fill.getQuick(Math.min(i, fillCount - 1));
                ctx.functionBinder.validateSampleByFill(aggregates.getQuick(i), fillValue.token,
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
    }

    private void validateSampleByQuery(QueryModel model, QueryModel source, OutputSchema input, boolean isBucket) throws SqlException {
        final int timestampIndex = sampleByTimestampIndex(input);
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
            if (timestampIndex < 0 || !hasRangeTimestamp) {
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
            if (wildcard == null && expression.isWildcard()) {
                wildcard = expression;
            }
            hasAggregate |= ctx.hasAggregate(expression) || isBucket && expression.type == ExpressionNode.FUNCTION
                    && ctx.functionFactoryCache.isGroupBy(expression.token);
        }
        if (wildcard != null && timestampIndex > -1 && !requiresSampleByCursorAlignment(source)) {
            throw SqlException.$(wildcard.position, "wildcard column select is not allowed in sample-by queries");
        }
        if (!hasAggregate) {
            throw SqlException.$(sampleBy.position, "at least one aggregation function must be present in 'select' clause");
        }
        if (timestampIndex < 0) {
            throw SqlException.$(source.getModelPosition(), source.getJoinModels().size() > 1
                    ? "TIMESTAMP column is required but not provided"
                    : "base query does not provide designated TIMESTAMP column");
        }
        if (wildcard != null) {
            throw SqlException.$(wildcard.position, "wildcard column select is not allowed in sample-by queries");
        }
    }

    private void validateSdtCompdev(ExpressionNode compdev, SqlExecutionContext executionContext) throws SqlException {
        try {
            SubsampleValidator.validateSdtCompdev(subsampleArgument(compdev, executionContext), compdev.position);
        } catch (SqlException e) {
            if (SubsampleValidator.hasUnresolvableSdtCompdevReference(compdev, executionContext)) {
                throw SqlException.$(compdev.position, "SUBSAMPLE sdt requires a constant, non-negative finite compdev");
            }
            throw e;
        }
    }

    private void validateUuidFill(Function fill, CharSequence fillToken, int fillPosition) throws SqlException {
        try {
            switch (ColumnType.tagOf(fill.getType())) {
                case ColumnType.STRING, ColumnType.SYMBOL ->
                        SqlUtil.implicitCastStrAsUuid(fill.getStrA(null), fillUuid);
                case ColumnType.VARCHAR -> SqlUtil.implicitCastStrAsUuid(fill.getVarcharA(null), fillUuid);
                default -> {
                }
            }
        } catch (ImplicitCastException e) {
            throw GroupByUtils.invalidSampleByFillValue(fillToken, fillPosition);
        }
    }

    /**
     * A {@link FillPlan} above the aggregate fills the gaps: any FILL other than NONE, in both SAMPLE BY
     * shapes. Only a FILL that interpolates stays in the SAMPLE BY cursor.
     */
    static boolean hasFill(QueryModel source) {
        final ObjList<ExpressionNode> fill = source.getSampleByFill();
        return fill.size() > 1 || fill.size() == 1 && !SqlKeywords.isNoneKeyword(fill.getQuick(0).token);
    }

    static boolean isFillPlanned(QueryModel source) {
        return hasFill(source) && !hasLinearFill(source);
    }

    static boolean requiresSampleByCursor(QueryModel source) {
        return requiresSampleByCursorAlignment(source) || source.getTableNameExpr() == null && (source.getTimestamp() == null
                || source.getSampleByFrom() != null || source.getSampleByTo() != null || hasFill(source));
    }

    /**
     * The index of the first argument of the SUBSAMPLE call whose value the binder validates, the target and then the
     * cadence seed, that reads a sub-query, or -1 when none does. Such an argument binds only with the window call,
     * which validates it and every validated argument after it; the binder validates the arguments before it on their
     * own.
     */
    static int pendingSubsampleArgument(ExpressionNode subsample, SubsampleMethod method) {
        final int targetIndex = method.isPositionOnly() ? 0 : 1;
        for (int i = targetIndex, n = method == SubsampleMethod.CADENCE ? subsample.paramCount : targetIndex + 1; i < n; i++) {
            if (hasSubQuery(subsample.args.getQuick(i))) {
                return i;
            }
        }
        return -1;
    }

    /**
     * The FILL values follow the grouping's aggregates by position.
     */
    FillPlan bindFill(
            QueryModel source, GroupingPlan aggregate, ObjList<ExpressionNode> aggregateNodes, int timestampIndex,
            SqlExecutionContext executionContext
    ) throws SqlException {
        final BindScope scope = ctx.scope();
        final ObjList<ExpressionNode> fill = source.getSampleByFill();
        final int fillCount = fill.size();
        validateFill(aggregate.getAggregates(), aggregateNodes, fill);
        scope.fillBindings.clear();
        for (int i = 0; i < fillCount; i++) {
            final ExpressionNode expression = signedFillLiteral(fill.getQuick(i));
            scope.fillBindings.add(SqlKeywords.isPrevKeyword(expression.token) ? null : bindFillValue(expression, executionContext));
        }
        final ExpressionNode period = source.getSampleBy();
        final FillPlan plan = ctx.planNodes.fills.next().of(aggregate, period.position);
        final OutputSchema output = aggregate.getOutput();
        plan.setTimestampColumnId(output.getColumnId(timestampIndex));
        plan.deriveOutput();
        final int timestampType = output.getColumnType(timestampIndex);
        if (aggregate instanceof SampleByPlan sampleBy) {
            plan.setPeriod(sampleBy.getStride(), sampleBy.getPeriodUnit(), sampleBy.getPeriodUnitPosition());
        } else {
            final int unitIndex = period.token.length() - 1;
            plan.setPeriod(TimestampSamplerFactory.parsePeriodStride(period.token, period.position), period.token.charAt(unitIndex), period.position + unitIndex);
        }
        final ExpressionNode timezone = sampleByTimezone(source);
        final boolean isSubDay = CommonUtils.isSubDayUnit(plan.getPeriodUnit());
        final ExpressionNode from = source.getSampleByFrom();
        final ExpressionNode to = source.getSampleByTo();
        final ExpressionNode rangeTimezone = isSubDay ? timezone : null;
        plan.setFrom(bindSampleByBound(sampleByRangeBound(from, rangeTimezone, timestampType), timestampType, FROM_BOUND_ERROR, executionContext));
        plan.setTo(bindSampleByBound(sampleByRangeBound(to, rangeTimezone, timestampType), timestampType, TO_BOUND_ERROR, executionContext));
        if (!isSubDay) {
            plan.setTimezone(bindSampleByParameter(timezone, ColumnType.STRING, executionContext));
        }
        final ExpressionNode offset = sampleByOffset(source);
        if ((timezone == null || isSubDay && from != null || !isSubDay) && offset != null && offset != SqlParser.ZERO_OFFSET) {
            plan.setOffset(bindSampleByOffset(offset, executionContext));
        }
        final int aggregateCount = aggregate.getAggregates().size();
        final boolean isBroadcast = fillCount < aggregateCount;
        if (isBroadcast) {
            final ExpressionNode only = fillCount == 1 ? signedFillLiteral(fill.getQuick(0)) : null;
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
            final ExpressionNode expression = signedFillLiteral(fill.getQuick(fillIndex));
            final int targetId = output.getColumnId(keyCount + i);
            final int targetType = output.getColumnType(keyCount + i);
            FillMode mode = FillMode.VALUE;
            int sourceId = -1;
            int sourcePosition;
            CharSequence token = expression.token;
            if (SqlKeywords.isPrevKeyword(expression.token)) {
                validateFillPrev(expression);
                mode = FillMode.PREV;
                if (expression.type == ExpressionNode.FUNCTION) {
                    token = expression.rhs.token;
                    sourcePosition = expression.rhs.position;
                    int sourceIndex = -1;
                    for (int k = 0, n = output.getColumnCount(); k < n; k++) {
                        if (isNameSpelling(token, output.getColumnName(k))) {
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
                        mode = FillMode.PREV_COLUMN;
                        validateFillPrevType(targetType, output.getColumnType(sourceIndex), token, sourcePosition);
                    }
                }
            } else if (SqlKeywords.isNullKeyword(expression.token)) {
                mode = FillMode.NULL;
                validateFillNull(targetType, expression.position);
            }
            plan.getTargetColumnIds().add(targetId);
            plan.getModes().add(mode);
            plan.getValues().add(mode == FillMode.VALUE
                    ? bindFillConstant(scope.fillBindings.getQuick(fillIndex), targetType, token, expression.position, executionContext) : null);
            plan.getSourceColumnIds().add(sourceId);
            plan.getTokens().add(token);
        }
        final IntList targetIds = plan.getTargetColumnIds();
        for (int i = 0, n = targetIds.size(); i < n; i++) {
            if (plan.getModes().getQuick(i) == FillMode.PREV_COLUMN) {
                FillMode sourceMode = FillMode.PREV;
                for (int k = 0; k < n; k++) {
                    if (targetIds.getQuick(k) == plan.getSourceColumnIds().getQuick(i)) {
                        sourceMode = plan.getModes().getQuick(k);
                        break;
                    }
                }
                if (sourceMode == FillMode.PREV_COLUMN) {
                    throw SqlException.$(fill.getQuick(i).position,
                            "FILL(PREV) chains are not supported: source column is itself a cross-column PREV");
                }
                if (sourceMode == FillMode.NULL || sourceMode == FillMode.VALUE) {
                    throw SqlException.$(fill.getQuick(i).position,
                            "FILL(PREV) cannot reference a column that is itself filled with a constant");
                }
            }
        }
        return plan;
    }

    SampleByPlan bindSampleBy(
            QueryModel model, QueryModel source, LogicalPlan input, SqlExecutionContext executionContext
    ) throws SqlException {
        ctx.scope().sampleByTimestampColumnId = input.getOutput().getTimestampColumnId();
        final ExpressionNode sampleBy = source.getSampleBy();
        validateSampleByQuery(model, source, input.getOutput(), false);
        final int timestampIndex = sampleByTimestampIndex(input.getOutput());
        final int timestampType = input.getOutput().getColumnType(timestampIndex);
        markTimestampSubqueries(source, timestampType);

        final SampleByPlan plan = ctx.planNodes.sampleByPlans.next().of(input, model.getModelPosition());
        plan.setTimestampColumnId(input.getOutput().getColumnId(timestampIndex));
        // LATEST ON names its timestamp without declaring the sampled input's order.
        plan.setTimestampRequired(source.getTimestamp() == null || !source.isExplicitTimestamp() && source.getLatestBy().size() > 0);
        plan.setJoinInput(source.getJoinModels().size() > 1);
        plan.setFillInput(isFillPlanned(source));
        plan.setTimezone(bindSampleByParameter(source.getSampleByTimezoneName(), ColumnType.STRING, executionContext));
        validateRuntimeConstant(plan.getTimezone(), ColumnType.STRING, "timezone must be a constant expression of STRING or CHAR type");
        plan.setOffset(bindSampleByOffset(sampleByOffset(source), executionContext));
        plan.setFrom(bindSampleByBound(source.getSampleByFrom(), timestampType, FROM_BOUND_ERROR, executionContext));
        plan.setTo(bindSampleByBound(source.getSampleByTo(), timestampType, TO_BOUND_ERROR, executionContext));
        final ExpressionNode unit = source.getSampleByUnit();
        final TimestampDriver driver = ColumnType.getTimestampDriver(timestampType);
        if (unit == null) {
            final long stride = TimestampSamplerFactory.parsePeriodStride(sampleBy.token, sampleBy.position);
            final int unitIndex = sampleBy.token.length() - 1;
            TimestampSamplerFactory.getSampleByInstance(driver, stride, sampleBy.token.charAt(unitIndex), sampleBy.position + unitIndex);
            plan.setPeriod(sampleBy.token, stride, sampleBy.position, sampleBy.token.charAt(unitIndex), sampleBy.position + unitIndex);
        } else {
            final BoundExpression bound = ctx.functionBinder.bind(sampleBy, ctx.emptySchema, null, executionContext);
            if (!(bound instanceof ConstantExpression period)
                    || bound.getDataType() != ColumnType.INT && bound.getDataType() != ColumnType.LONG) {
                throw SqlException.$(sampleBy.position, "sample by period must be a constant expression of INT or LONG type");
            }
            final long stride = period.getDataType() == ColumnType.INT ? Numbers.intToLong((int) period.getLongValue()) : period.getLongValue();
            TimestampSamplerFactory.getSampleByInstance(driver, stride, unit.token.charAt(0), sampleBy.position);
            plan.setPeriod(null, stride, sampleBy.position, unit.token.charAt(0), unit.position);
        }
        return plan;
    }

    BoundExpression bindSampleByBucket(
            QueryModel model, QueryModel source, LogicalPlan sourcePlan, SqlExecutionContext executionContext
    ) throws SqlException {
        final BindScope scope = ctx.scope();
        scope.sampleByTimestampColumnId = bucketTimestampColumnId(sourcePlan);
        final OutputSchema input = sourcePlan.getOutput();
        final ExpressionNode sampleBy = source.getSampleBy();
        validateSampleByQuery(model, source, input, true);
        final ExpressionNode from = source.getSampleByFrom();
        final int timestampType = input.getColumnType(sampleByTimestampIndex(input));
        markTimestampSubqueries(source, timestampType);
        bindSampleByBound(from, timestampType, FROM_BOUND_ERROR, executionContext);
        bindSampleByBound(source.getSampleByTo(), timestampType, TO_BOUND_ERROR, executionContext);
        final ExpressionNode timezoneName = source.getSampleByTimezoneName();
        scope.sampleByTimezone = timezoneName != null ? ctx.functionBinder.bindStandalone(timezoneName, executionContext) : null;
        validateRuntimeConstant(scope.sampleByTimezone, ColumnType.STRING, "timezone must be a constant expression of STRING or CHAR type");
        final ExpressionNode timezone = sampleByTimezone(source);
        final ExpressionNode offset = sampleByOffset(source);
        final boolean isSubDay = !sampleBy.token.isEmpty() && CommonUtils.isSubDayUnit(sampleBy.token.charAt(sampleBy.token.length() - 1));
        final ExpressionNode floor = ctx.bindingExpressions.next().of(ExpressionNode.FUNCTION, "timestamp_floor_utc", 0, sampleBy.position);
        floor.paramCount = 5;
        floor.args.add(timezone != null && !(isSubDay && from != null) ? timezone : sampleByNull());
        floor.args.add(offset);
        floor.args.add(from == null ? sampleByNull() : isSubDay && timezone != null ? sampleByToUtc(from, timezone) : from);
        floor.args.add(sampleByTimestamp(input, sampleBy.position));
        final CharacterStoreEntry interval = ctx.characterStore.newEntry();
        interval.put('\'').put(sampleBy.token).put('\'');
        floor.args.add(ctx.bindingExpressions.next().of(ExpressionNode.CONSTANT, interval.toImmutable(), 0, sampleBy.position));
        final BoundExpression bucket;
        try {
            bucket = ctx.functionBinder.bind(floor, input, sourceAlias(source), executionContext);
        } catch (SqlException e) {
            validateConvertible(ctx.functionBinder.bind(offset, input, sourceAlias(source), ColumnType.STRING, executionContext), ColumnType.STRING, OFFSET_ERROR);
            throw e;
        }
        TimestampSamplerFactory.getInstance(ColumnType.getTimestampDriver(bucket.getDataType()), CommonUtils.getStrideMultiple(sampleBy.token, sampleBy.position),
                CommonUtils.getStrideUnit(sampleBy.token, sampleBy.position), sampleBy.position + sampleBy.token.length() - 1);
        return bucket;
    }

    /**
     * The FILL values follow the SAMPLE BY's aggregates by position.
     */
    void bindSampleByFill(
            QueryModel source, SampleByPlan plan, ObjList<ExpressionNode> aggregateNodes, SqlExecutionContext executionContext
    ) throws SqlException {
        if (!hasLinearFill(source)) {
            return;
        }
        final ObjList<ExpressionNode> fill = source.getSampleByFill();
        for (int i = 0, n = fill.size(); i < n; i++) {
            final ExpressionNode value = signedFillLiteral(fill.getQuick(i));
            final FillMode mode = SqlKeywords.isNoneKeyword(value.token) ? FillMode.NONE
                    : SqlKeywords.isPrevKeyword(value.token) ? FillMode.PREV
                      : SqlKeywords.isNullKeyword(value.token) ? FillMode.NULL
                        : SqlKeywords.isLinearKeyword(value.token) ? FillMode.LINEAR : FillMode.VALUE;
            plan.getFillModes().add(mode);
            plan.getFillValues().add(mode == FillMode.VALUE ? bindFillValue(value, executionContext) : null);
        }
        final ObjList<FunctionExpression> aggregates = plan.getAggregates();
        validateFill(aggregates, aggregateNodes, fill);
        final int fillCount = fill.size();
        for (int k = 0, n = Math.min(fillCount, aggregates.size()); k < n; k++) {
            final ExpressionNode value = signedFillLiteral(fill.getQuick(k));
            final BoundExpression bound = plan.getFillValues().getQuick(k);
            if (bound != null) {
                plan.getFillValues().setQuick(k, bindFillConstant(bound, aggregates.getQuick(k).getDataType(), value.token, value.position, executionContext));
            } else if (SqlKeywords.isNullKeyword(value.token)) {
                for (int i = fillCount == 1 ? 0 : k, m = fillCount == 1 ? aggregates.size() : k + 1; i < m; i++) {
                    validateFillNull(aggregates.getQuick(i).getDataType(), value.position);
                }
            }
        }
        if (fillCount < 2) {
            return;
        }
        final ObjList<BoundExpression> keys = plan.getGroupingExpressions();
        for (int i = 0, n = keys.size(); i < n; i++) {
            if (!(keys.getQuick(i) instanceof ColumnExpression column) || column.getColumnId() != plan.getTimestampColumnId()) {
                throw SqlException.position(0).put("linear interpolation is not supported when using fill values for keyed sample by expression");
            }
        }
        for (int i = 0, n = aggregates.size(); i < n; i++) {
            if (i == fillCount) {
                throw SqlException.position(fill.getQuick(fillCount - 1).position)
                        .put("insufficient fill values for SAMPLE BY FILL: expected ")
                        .put(n)
                        .put(" values but only ")
                        .put(fillCount)
                        .put(" provided");
            }
            final int type = aggregates.getQuick(i).getDataType();
            if (SqlKeywords.isNullKeyword(fill.getQuick(i).token) && GroupByUtils.nullFillConstant(type) == null) {
                throw SqlException.$(aggregates.getQuick(i).getPosition(), "Unsupported type: ").put(ColumnType.nameOf(type));
            }
        }
    }

    LogicalPlan bindSampleByRange(QueryModel source, LogicalPlan input, SqlExecutionContext executionContext) throws SqlException {
        final ExpressionNode from = source.getSampleByFrom();
        final ExpressionNode to = source.getSampleByTo();
        if (from == null && to == null) {
            return input;
        }
        final ExpressionNode timezone = sampleByTimezone(source);
        final int timestampType = input.getOutput().getColumnType(sampleByTimestampIndex(input.getOutput()));
        final BoundExpression predicate;
        if (timezone == null) {
            final ExpressionNode lo = from == null ? null : sampleByComparison(input.getOutput(), ">=", from);
            final ExpressionNode hi = to == null ? null : sampleByComparison(input.getOutput(), "<", to);
            predicate = binder.bindPredicate(ctx.combinePredicates(lo, hi), input, source, executionContext);
        } else if (to == null) {
            predicate = bindSampleByRangeComparison(input, source, ">=", sampleByRangeBound(from, timezone, timestampType), executionContext);
        } else {
            final BoundExpression lo = from == null ? null
                    : bindSampleByRangeComparison(input, source, ">=", sampleByRangeBound(from, timezone, timestampType), executionContext);
            final BoundExpression hi = bindSampleByRangeComparison(input, source, "<", sampleByRangeBound(to, timezone, timestampType), executionContext);
            predicate = lo == null ? hi : ctx.expressionRewriter.combineConjunction(lo, hi, hi.getPosition());
        }
        return ctx.planNodes.filters.next().of(input, predicate, predicate.getPosition());
    }

    LogicalPlan bindSubsample(
            LogicalPlan input, LogicalPlan sourcePlan, QueryModel source, SqlExecutionContext executionContext
    ) throws SqlException {
        final BindScope scope = ctx.scope();
        final ExpressionNode subsample = source.getSubsample();
        final SubsampleMethod method = validateSubsampleCall(subsample);
        final boolean isCadence = method == SubsampleMethod.CADENCE;
        final boolean isPositionOnly = method.isPositionOnly();
        final boolean isSdt = method == SubsampleMethod.SDT;
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
        final int targetIndex = isPositionOnly ? 0 : 1;
        final ExpressionNode target = subsample.args.getQuick(targetIndex);
        if (!isSdt && target.type == ExpressionNode.LITERAL) {
            throw SqlException.$(target.position, isCadence ? "stride" : "target point count")
                    .put(" must be a constant or bind variable");
        }
        final ExpressionNode seed = isCadence && subsample.paramCount == 2 ? subsample.args.getQuick(1) : null;
        if (seed != null && seed.type == ExpressionNode.LITERAL) {
            throw SqlException.$(seed.position, "seed must be a constant, bind variable, or NULL");
        }
        final int pendingIndex = pendingSubsampleArgument(subsample, method);
        if (pendingIndex != targetIndex) {
            if (isSdt) {
                validateSdtCompdev(target, executionContext);
            } else {
                SubsampleValidator.validatePositionTargetOrThrow(subsampleArgument(target, executionContext), target.position, isCadence);
            }
            if (seed != null && pendingIndex < 0) {
                SubsampleValidator.validateCadenceSeedOrThrow(subsampleArgument(seed, executionContext), seed.position);
            }
        }
        final CharSequence timestampName = SqlUtil.protectColumnAlias(ctx.characterStore, output.getColumnName(timestampIndex));
        final ExpressionNode call = ctx.bindingExpressions.next().of(ExpressionNode.FUNCTION,
                subsample.token, 0, subsample.position);
        call.paramCount = isPositionOnly ? subsample.paramCount : subsample.paramCount + 1;
        if (!isPositionOnly) {
            if (method == SubsampleMethod.LTTB && subsample.paramCount == 3) {
                call.args.add(ExpressionNode.deepClone(ctx.bindingExpressions, subsample.args.getQuick(2)));
            }
            call.args.add(ExpressionNode.deepClone(ctx.bindingExpressions, target));
            call.args.add(ctx.bindingExpressions.next().of(ExpressionNode.LITERAL,
                    SqlUtil.protectColumnAlias(ctx.characterStore, output.getColumnName(valueIndex)), 0, subsample.args.getQuick(0).position));
            call.args.add(ctx.bindingExpressions.next().of(ExpressionNode.LITERAL, timestampName, 0, subsample.position));
        } else if (call.paramCount == 1) {
            call.rhs = ExpressionNode.deepClone(ctx.bindingExpressions, target);
        } else {
            call.lhs = ExpressionNode.deepClone(ctx.bindingExpressions, target);
            call.rhs = ExpressionNode.deepClone(ctx.bindingExpressions, seed);
        }
        final WindowExpression syntax = ctx.planNodes.windowSyntax.next();
        syntax.setSubsampleKeepFlag(true);
        syntax.setPendingSubsample(subsample, source.getSubsamplePosition(), subsampleTimestamp(sourcePlan) >= 0);
        syntax.addOrderBy(ctx.bindingExpressions.next().of(ExpressionNode.LITERAL, timestampName, 0, subsample.position),
                QueryModel.ORDER_DIRECTION_ASCENDING);
        call.windowExpression = syntax;
        windowBinder.normalizeWindowFrame(syntax, executionContext);
        final WindowSpec spec = ctx.planNodes.windowSpecs.next().of(syntax);
        if (executionContext.isLiveViewCompile()) {
            spec.setLiveViewDescription(LiveViewWindowDescription.of(syntax));
        }
        spec.addOrderBy(timestampId, SortDirection.ASCENDING, output.getColumnName(timestampIndex), subsample.position);
        final FunctionExpression function;
        try {
            function = windowBinder.bindWindowFunction(call, spec, output, source, executionContext);
        } catch (SqlException e) {
            if (isSdt && SubsampleValidator.hasUnresolvableSdtCompdevReference(target, executionContext)) {
                throw SqlException.$(target.position, "SUBSAMPLE sdt requires a constant, non-negative finite compdev");
            }
            throw e;
        }
        final WindowPlan window = ctx.planNodes.windowPlans.next().of(input, subsample.position);
        final int keepId = ctx.planNodes.nextColumnId();
        window.getFunctions().add(function);
        window.getSpecs().add(spec);
        window.getFunctionColumnIds().add(keepId);
        window.getOutput().copyFrom(output);
        scope.resetAliases(output);
        window.getOutput().add(keepId, ctx.createOutputName("__keep_subsample"), ColumnType.BOOLEAN, false);
        final FilterPlan filter = ctx.planNodes.filters.next().of(window, ctx.planNodes.columns.next().of(keepId, ColumnType.BOOLEAN, subsample.position), subsample.position);
        final ProjectPlan project = ctx.identityProjection(filter, subsample.position);
        project.getExpressions().remove(output.getColumnCount());
        project.getOutput().remove(output.getColumnCount());
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
            if (expression.isWildcard()) {
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
            return ctx.bindColumnIndex(expression, input, source) == sampleByTimestampIndex(input);
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
        final int index = sampleByTimestampIndex(input);
        final CharSequence qualifier = input.getColumnQualifier(index);
        CharSequence name = input.getColumnName(index);
        if (qualifier != null) {
            final CharacterStoreEntry token = ctx.characterStore.newEntry();
            token.put('"').put(qualifier).put("\".").put(name);
            name = token.toImmutable();
        }
        return ctx.bindingExpressions.next().of(ExpressionNode.LITERAL, name, 0, position);
    }

    int sampleByTimestampIndex(OutputSchema input) {
        return input.getColumnIndexById(ctx.scope().sampleByTimestampColumnId);
    }

    SqlException subsampleTimestampMissing(QueryModel source, LogicalPlan sourcePlan) {
        final ExpressionNode subsample = source.getSubsample();
        final SqlException exception = SqlException.$(SubsampleMethod.of(subsample.token) == SubsampleMethod.SDT ? subsample.position : source.getSubsamplePosition(),
                "SUBSAMPLE requires a designated timestamp column; ");
        return hasSubsampleSourceTimestamp(sourcePlan)
                ? exception.put("the SELECT list must include it unchanged")
                : exception.put("the query source has no designated timestamp");
    }

    void validateSampleByTimezone(SampleByPlan plan, SqlExecutionContext executionContext) throws SqlException {
        if (plan.getTimezone() instanceof ConstantExpression timezone && CommonUtils.isSubDayUnit(plan.getPeriodUnit())) {
            final OutputSchema input = plan.getChild().getOutput();
            final int timestampType = input.getColumnType(input.getColumnIndexById(plan.getTimestampColumnId()));
            try (Function zone = ctx.functionInstantiator.instantiate(timezone, ctx.emptySchema, executionContext)) {
                final CharSequence name = zone.getStrA(null);
                if (name != null) {
                    try {
                        ColumnType.getTimestampDriver(timestampType).getTimezoneRules(DateLocaleFactory.EN_LOCALE, name);
                    } catch (CairoException e) {
                        throw SqlException.$(plan.getTimezonePosition(), e.getFlyweightMessage());
                    }
                }
            }
        }
    }

    /**
     * Validates the SUBSAMPLE method and its argument count, and returns the method.
     */
    SubsampleMethod validateSubsampleCall(ExpressionNode subsample) throws SqlException {
        final SubsampleMethod method = SubsampleMethod.of(subsample.token);
        if (method == null) {
            throw SqlException.$(subsample.position, "unknown subsample method: ").put(subsample.token)
                    .put(". Supported methods: lttb, m4, minmax, uniform, cadence, sdt");
        }
        switch (method) {
            case CADENCE -> {
                if (subsample.paramCount < 1 || subsample.paramCount > 2) {
                    throw SqlException.$(subsample.position, "cadence() requires 1 or 2 arguments: stride and optional seed");
                }
            }
            case UNIFORM -> {
                if (subsample.paramCount != 1) {
                    throw SqlException.$(subsample.position, "uniform() requires exactly 1 argument: target points");
                }
            }
            case EXTREMA, LTTB -> {
                if (subsample.paramCount < 2) {
                    throw SqlException.$(subsample.position, subsample.token).put("() requires at least 2 arguments: column and target points");
                }
                final boolean isLttb = method == SubsampleMethod.LTTB;
                final int maxArgs = isLttb ? 3 : 2;
                if (subsample.paramCount > maxArgs) {
                    throw SqlException.$(subsample.args.getQuick(maxArgs).position, subsample.token).put(isLttb
                            ? "() accepts at most 3 arguments: column, target points, and optional gap threshold"
                            : "() accepts exactly 2 arguments: column and target points");
                }
            }
            case SDT -> {
            }
        }
        return method;
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
        subsampleValueIndex(value, ctx.emptySchema);
    }

    /**
     * The window function a SUBSAMPLE call binds as.
     */
    enum SubsampleMethod {
        CADENCE, EXTREMA, LTTB, SDT, UNIFORM;

        /**
         * The method the token names, or null when it names none.
         */
        static SubsampleMethod of(CharSequence token) {
            if (Chars.equalsIgnoreCase(token, "cadence")) {
                return CADENCE;
            }
            if (Chars.equalsIgnoreCase(token, "lttb")) {
                return LTTB;
            }
            if (Chars.equalsIgnoreCase(token, "m4") || Chars.equalsIgnoreCase(token, "minmax")) {
                return EXTREMA;
            }
            if (Chars.equalsIgnoreCase(token, "sdt")) {
                return SDT;
            }
            return Chars.equalsIgnoreCase(token, "uniform") ? UNIFORM : null;
        }

        /**
         * The method keeps rows by their position alone, rather than by the values of a column.
         */
        boolean isPositionOnly() {
            return this == CADENCE || this == UNIFORM;
        }
    }
}
