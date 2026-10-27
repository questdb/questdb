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

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.GeoHashes;
import io.questdb.cairo.ImplicitCastException;
import io.questdb.cairo.MillisTimestampDriver;
import io.questdb.cairo.TimestampDriver;
import io.questdb.cairo.sql.BindVariableService;
import io.questdb.cairo.sql.Function;
import io.questdb.griffin.BoundExpressionRewriter;
import io.questdb.griffin.CallBinder;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.FunctionFactoryDescriptor;
import io.questdb.griffin.FunctionInstantiator;
import io.questdb.griffin.CompilerTemporaries;
import io.questdb.griffin.FunctionParser;
import io.questdb.griffin.FunctionResolver;
import io.questdb.griffin.IntervalExtractor;
import io.questdb.griffin.PostOrderTreeTraversalAlgo;
import io.questdb.griffin.PreparedFunctions;
import io.questdb.griffin.SqlCompilerImpl;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlKeywords;
import io.questdb.griffin.SqlUtil;
import io.questdb.griffin.StaticTypeFunction;
import io.questdb.griffin.SubsampleValidator;
import io.questdb.griffin.TypeConstant;
import io.questdb.griffin.engine.functions.CursorFunction;
import io.questdb.griffin.engine.functions.GroupByFunction;
import io.questdb.griffin.engine.functions.ScalarSubQueryTimestampFunction;
import io.questdb.griffin.engine.functions.bool.BooleanSubQueryFunction;
import io.questdb.griffin.engine.functions.bool.InTimestampTimestampFunctionFactory;
import io.questdb.griffin.engine.functions.columns.BindableColumn;
import io.questdb.griffin.engine.functions.constants.BooleanConstant;
import io.questdb.griffin.engine.functions.constants.ConstantFunction;
import io.questdb.griffin.engine.functions.constants.DateConstant;
import io.questdb.griffin.engine.functions.constants.StrConstant;
import io.questdb.griffin.engine.functions.constants.TimestampConstant;
import io.questdb.griffin.engine.groupby.GroupByUtils;
import io.questdb.griffin.engine.window.WindowContext;
import io.questdb.griffin.engine.window.WindowFunction;
import io.questdb.griffin.model.ExpressionNode;
import io.questdb.griffin.model.IntervalOperation;
import io.questdb.griffin.model.IntervalUtils;
import io.questdb.griffin.plan.logical.BindVariableExpression;
import io.questdb.griffin.plan.logical.BoundExpression;
import io.questdb.griffin.plan.logical.ColumnExpression;
import io.questdb.griffin.plan.logical.ConstantExpression;
import io.questdb.griffin.plan.logical.CursorExpression;
import io.questdb.griffin.plan.logical.FunctionExpression;
import io.questdb.griffin.plan.logical.OuterColumnExpression;
import io.questdb.griffin.plan.logical.OutputSchema;
import io.questdb.griffin.plan.logical.Subquery;
import io.questdb.griffin.plan.logical.TypeExpression;
import io.questdb.std.Chars;
import io.questdb.std.Decimal128;
import io.questdb.std.Decimal256;
import io.questdb.std.GenericLexer;
import io.questdb.std.IntHashSet;
import io.questdb.std.IntList;
import io.questdb.std.Interval;
import io.questdb.std.LongList;
import io.questdb.std.Misc;
import io.questdb.std.Mutable;
import io.questdb.std.Numbers;
import io.questdb.std.NumericException;
import io.questdb.std.ObjList;
import io.questdb.std.ObjObjHashMap;
import io.questdb.std.str.StringSink;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

/**
 * Binds SQL expressions: walks each expression in post-order, resolves and builds every call through the
 * {@link FunctionResolver}, produces {@link BoundExpression} descriptions and hands each built root to
 * {@link PreparedFunctions}.
 */
public final class FunctionBinder implements CallBinder, FunctionResolver.TimestampTextError, PostOrderTreeTraversalAlgo.Visitor, Mutable {
    private final IntList argumentLeafMarks = new IntList();
    private final IntList argumentPositions = new IntList();
    private final IntList argumentTypes = new IntList();
    private final ObjList<BoundExpression> arguments = new ObjList<>();
    private final ObjList<BoundExpression> callArguments;
    private final ObjList<Function> callFunctions;
    private final IntList callPositions;
    private final SqlCompilerImpl compiler;
    private final BindContext ctx;
    private final Decimal128 decimal128 = new Decimal128();
    private final Decimal256 decimal256 = new Decimal256();
    private final IntList expressionLeafMarks = new IntList();
    private final ObjList<BoundExpression> expressionStack = new ObjList<>();
    private final ObjList<Function> functionStack = new ObjList<>();
    private final StringSink intervalSink = new StringSink();
    private final StringSink intervalText;
    private final LongList parsedIntervals;
    private final IntList positionStack = new IntList();
    private final FunctionResolver resolver;
    private final PostOrderTreeTraversalAlgo traverseAlgo;
    private final ObjList<BoundExpression> unconstructed = new ObjList<>();
    private final ObjObjHashMap<BoundExpression, BoundExpression> unconstructedIndex = new ObjObjHashMap<>();
    private SqlExecutionContext sqlExecutionContext;

    /**
     * Allocates descriptions from the context's pools and hands built roots to its prepared functions.
     */
    FunctionBinder(
            BindContext ctx,
            FunctionResolver resolver,
            SqlCompilerImpl compiler,
            PostOrderTreeTraversalAlgo traverseAlgo,
            CompilerTemporaries temporaries
    ) {
        this.ctx = ctx;
        this.resolver = resolver;
        this.compiler = compiler;
        this.traverseAlgo = traverseAlgo;
        this.callFunctions = temporaries.functions;
        this.callPositions = temporaries.functionPositions;
        this.callArguments = temporaries.expressions;
        this.intervalText = temporaries.sink;
        this.parsedIntervals = temporaries.longs;
    }

    /**
     * A binder over the context of a stand-alone statement binder of the compiler, which must outlive it.
     */
    @TestOnly
    public static FunctionBinder newStandalone(SqlCompilerImpl compiler, FunctionParser parser) {
        return compiler.newStandaloneBinder(parser).ctx.functionBinder;
    }

    /**
     * Input schema and alias are borrowed only for this call.
     */
    public BoundExpression bind(
            ExpressionNode node,
            OutputSchema input,
            CharSequence inputAlias,
            SqlExecutionContext executionContext
    ) throws SqlException {
        return bind(node, input, inputAlias, ColumnType.UNDEFINED, executionContext);
    }

    /**
     * The preferred type applies only to an otherwise untyped root parameter.
     */
    public BoundExpression bind(
            ExpressionNode node,
            OutputSchema input,
            CharSequence inputAlias,
            int preferredType,
            SqlExecutionContext executionContext
    ) throws SqlException {
        return bind(node, BindingRole.SCALAR, input, inputAlias, preferredType, null, null, null, executionContext);
    }

    /**
     * Binds a scalar expression above an aggregate using caller-selected subtree
     * replacements. Both lists are borrowed only for this call; matching uses AST
     * identity and never merges separate SQL occurrences.
     */
    public BoundExpression bind(
            ExpressionNode node,
            OutputSchema input,
            CharSequence inputAlias,
            ObjList<ExpressionNode> replacementNodes,
            ObjList<? extends BoundExpression> replacementColumns,
            SqlExecutionContext executionContext
    ) throws SqlException {
        return bind(node, BindingRole.SCALAR, input, inputAlias, ColumnType.UNDEFINED, replacementNodes, replacementColumns, null, executionContext);
    }

    /**
     * Binds one reviewed aggregate root; its arguments use ordinary scalar binding.
     */
    public FunctionExpression bindAggregate(
            ExpressionNode node,
            OutputSchema input,
            CharSequence inputAlias,
            SqlExecutionContext executionContext
    ) throws SqlException {
        assert ctx.isAggregate(node);
        return (FunctionExpression) bind(node, BindingRole.AGGREGATE, input, inputAlias, ColumnType.UNDEFINED, null, null, null, executionContext);
    }

    @Override
    public BoundExpression bindCall(
            CharSequence name,
            int position,
            ObjList<? extends BoundExpression> args,
            OutputSchema input,
            SqlExecutionContext executionContext
    ) throws SqlException {
        return bindRoot(null, name, position, args, input, null, ColumnType.UNDEFINED, executionContext);
    }

    @Override
    public BoundExpression bindCall(CharSequence name, int position, OutputSchema input, SqlExecutionContext executionContext) throws SqlException {
        final BoundExpression bound = bindCall(name, position, callArguments, input, executionContext);
        callArguments.clear();
        return bound;
    }

    @Override
    public BoundExpression bindCall(
            CharSequence name,
            int position,
            BoundExpression left,
            BoundExpression right,
            OutputSchema input,
            SqlExecutionContext executionContext
    ) throws SqlException {
        callArguments.clear();
        callArguments.add(left);
        callArguments.add(right);
        return bindCall(name, position, input, executionContext);
    }

    public BoundExpression bindGroupByExpression(
            ExpressionNode node,
            OutputSchema input,
            CharSequence inputAlias,
            SqlExecutionContext executionContext
    ) throws SqlException {
        assert ctx.isAggregate(node);
        return bind(node, BindingRole.GROUP_BY_EXPRESSION, input, inputAlias, ColumnType.UNDEFINED, null, null, null, executionContext);
    }

    public BoundExpression bindGroupByExpression(
            ExpressionNode node,
            OutputSchema input,
            ObjList<ExpressionNode> replacementNodes,
            ObjList<? extends BoundExpression> replacementColumns,
            SqlExecutionContext executionContext
    ) throws SqlException {
        assert ctx.isAggregate(node);
        return bind(node, BindingRole.GROUP_BY_EXPRESSION, input, null, ColumnType.UNDEFINED, replacementNodes, replacementColumns, null, executionContext);
    }

    /**
     * Binds a native-table predicate eligible for timestamp intrinsics. The
     * source filter parses direct literals at the designated column's
     * precision; projections and ordinary row filters use adaptive precision.
     */
    public BoundExpression bindPredicate(
            ExpressionNode node,
            OutputSchema input,
            CharSequence inputAlias,
            SqlExecutionContext executionContext
    ) throws SqlException {
        return bindPredicate(node, input, inputAlias, null, executionContext);
    }

    /**
     * Timestamp provenance sets are borrowed only for this call.
     */
    public BoundExpression bindPredicate(
            ExpressionNode node,
            OutputSchema input,
            CharSequence inputAlias,
            IntHashSet nativeTimestampIds,
            SqlExecutionContext executionContext
    ) throws SqlException {
        return bindPredicate(node, input, inputAlias, nativeTimestampIds, ColumnType.UNDEFINED, executionContext);
    }

    /**
     * The preferred type applies to an untyped conjunct parameter, before AND binding.
     */
    public BoundExpression bindPredicate(
            ExpressionNode node,
            OutputSchema input,
            CharSequence inputAlias,
            IntHashSet nativeTimestampIds,
            int preferredRootType,
            SqlExecutionContext executionContext
    ) throws SqlException {
        return bind(node, BindingRole.PREDICATE, input, inputAlias, preferredRootType, null, null, nativeTimestampIds, executionContext);
    }

    /**
     * An UPDATE target type also defines weak-dimension array roots.
     */
    public BoundExpression bindUpdateAssignment(
            ExpressionNode node,
            OutputSchema input,
            CharSequence inputAlias,
            int targetType,
            SqlExecutionContext executionContext
    ) throws SqlException {
        return bind(node, BindingRole.UPDATE_ASSIGNMENT, input, inputAlias, targetType, null, null, null, executionContext);
    }

    /**
     * Binds a root that a plan node outputs as a column: a projection, a grouping key or a value
     * computed over aggregates. An untyped root parameter becomes STRING.
     */
    public BoundExpression bindValue(
            ExpressionNode node,
            OutputSchema input,
            CharSequence inputAlias,
            SqlExecutionContext executionContext
    ) throws SqlException {
        return bind(node, BindingRole.SCALAR, input, inputAlias, ColumnType.STRING, null, null, null, executionContext);
    }

    /**
     * {@link #bindValue(ExpressionNode, OutputSchema, CharSequence, SqlExecutionContext)} with the subtree
     * replacements of {@link #bind(ExpressionNode, OutputSchema, CharSequence, ObjList, ObjList, SqlExecutionContext)}.
     */
    public BoundExpression bindValue(
            ExpressionNode node,
            OutputSchema input,
            CharSequence inputAlias,
            ObjList<ExpressionNode> replacementNodes,
            ObjList<? extends BoundExpression> replacementColumns,
            SqlExecutionContext executionContext
    ) throws SqlException {
        return bind(node, BindingRole.SCALAR, input, inputAlias, ColumnType.STRING, replacementNodes, replacementColumns, null, executionContext);
    }

    /**
     * Binds under the caller's configured WindowContext, which may carry partition key
     * types without a usable partition record or sink: the prepared window only types
     * and validates the call and is never adopted. The caller clears the context.
     */
    public FunctionExpression bindWindow(
            ExpressionNode node,
            OutputSchema input,
            CharSequence inputAlias,
            SqlExecutionContext executionContext
    ) throws SqlException {
        assert node.type == ExpressionNode.FUNCTION;
        if (executionContext.getWindowContext().isEmpty()) {
            throw SqlException.emptyWindowContext(node.position);
        }
        return (FunctionExpression) bind(node, BindingRole.WINDOW, input, inputAlias, ColumnType.UNDEFINED, null, null, null, executionContext);
    }

    @Override
    public void clear() {
        expressionStack.clear();
        expressionLeafMarks.clear();
        functionStack.clear();
        positionStack.clear();
        arguments.clear();
        argumentLeafMarks.clear();
        argumentPositions.clear();
        argumentTypes.clear();
        clearUnconstructed(0);
        sqlExecutionContext = null;
    }

    @TestOnly
    public void clearExpressions() {
        try {
            ctx.preparedFunctions.clear();
        } finally {
            try {
                ctx.functionInstantiator.clear();
            } finally {
                clear();
                ctx.expressionRewriter.clear();
            }
            ctx.planNodes.clearExpressions();
        }
    }

    @Override
    public boolean descend(ExpressionNode node) throws SqlException {
        final BindScope scope = ctx.scope();
        final ObjList<ExpressionNode> replacementNodes = scope.replacementNodes;
        if (replacementNodes != null) {
            for (int i = 0, n = replacementNodes.size(); i < n; i++) {
                if (replacementNodes.getQuick(i) == node) {
                    pushFunction(pushReplacement(scope.replacementColumns.getQuick(i), node.position), node.position);
                    return false;
                }
            }
        }
        validateNode(node);
        return true;
    }

    @Override
    public ObjList<BoundExpression> getCallArguments() {
        return callArguments;
    }

    @TestOnly
    public BoundExpressionRewriter getExpressionRewriter() {
        return ctx.expressionRewriter;
    }

    @TestOnly
    public FunctionInstantiator getFunctionInstantiator() {
        return ctx.functionInstantiator;
    }

    public boolean isGroupBy(CharSequence name) {
        return resolver.getFunctionFactoryCache().isGroupBy(name);
    }

    /**
     * The error of a comparison of a TIMESTAMP with constant text that does not parse as a timestamp. Interval
     * extraction over a designated timestamp once raised these messages, so a comparison reports them wherever it
     * binds: {@code =}, {@code !=} and {@code <>} report
     * {@code invalid timestamp} for a literal, otherwise {@code Invalid date [str=text]}, and for text with
     * {@code ;} {@code not a timestamp, use IN keyword with intervals} for a literal, otherwise
     * {@code Not a date, use IN keyword with intervals}; a range comparison reports {@code Invalid date [str=text]}
     * with a literal quoted; a BETWEEN bound reports a bare {@code Invalid date} for a literal. A comparison of a
     * DATE, which interval extraction never implements, and any other call report {@code Invalid date [str=text]}.
     */
    @Override
    public SqlException timestampTextError(CharSequence operator, int index, CharSequence text, int position) {
        final boolean isBetween = SqlKeywords.isBetweenKeyword(operator);
        if (!isBetween && !FunctionResolver.isTemporalComparisonOperator(operator)
                || arguments.getQuick(isBetween ? 0 : 1 - index).getDataType() == ColumnType.DATE) {
            return SqlException.invalidDate(text, position);
        }
        final boolean isLiteral = arguments.getQuick(index) instanceof ConstantExpression constant && IntervalExtractor.isStringLiteral(constant);
        if (isBetween) {
            return isLiteral ? SqlException.invalidDate(position) : SqlException.invalidDate(text, position);
        }
        if (Chars.equals(operator, '=') || FunctionResolver.isNotEqualsOperator(operator)) {
            if (Chars.indexOf(text, ';') >= 0) {
                return SqlException.$(position, isLiteral ? "not a timestamp, use IN keyword with intervals" : "Not a date, use IN keyword with intervals");
            }
            return isLiteral ? SqlException.$(position, "invalid timestamp") : SqlException.invalidDate(text, position);
        }
        return isLiteral ? SqlException.position(position).put("Invalid date [str='").put(text).put("']") : SqlException.invalidDate(text, position);
    }

    @Override
    public void visit(ExpressionNode node) throws SqlException {
        final int count = node.paramCount;
        final Function function;
        if (count == 0) {
            function = switch (node.type) {
                case ExpressionNode.LITERAL -> {
                    if (ctx.scope().bindingRole == BindingRole.STANDALONE) {
                        throw SqlException.invalidColumn(node.position, node.token);
                    }
                    final int index = findColumn(node, ctx.scope().expressionInput, ctx.scope().expressionInputAlias);
                    yield index == -1 && resolver.findNoArgFunction(node.token) && !isOuterScopeColumn(node)
                            ? resolveCall(node, 0) : createColumn(node, index);
                }
                case ExpressionNode.BIND_VARIABLE -> createParameter(node);
                case ExpressionNode.MEMBER_ACCESS -> captureConstant(new StrConstant(node.token), node.position, null);
                case ExpressionNode.CONSTANT ->
                        captureConstant(resolver.createConstant(node.position, node.token, sqlExecutionContext), node.position, node.token);
                case ExpressionNode.QUERY -> createCursorFunction(node, sqlExecutionContext);
                default -> resolveCall(node, 0);
            };
        } else {
            function = resolveCall(node, count);
        }
        pushFunction(function, node.position);
    }

    private static int getColumnIndexQuiet(OutputSchema input, CharSequence qualifier, CharSequence name, int lo, int hi) {
        final int index = input.getColumnIndexQuiet(qualifier, name, lo, hi);
        return index != -1 || !SqlUtil.isQuoteProtectedAlias(name, lo, hi)
                ? index : input.getColumnIndexQuiet(qualifier, name, lo + 1, hi - 1);
    }

    private static boolean hasOnlyColumnArguments(ExpressionNode node) {
        if (node.lhs != null && node.lhs.type != ExpressionNode.LITERAL
                || node.rhs != null && node.rhs.type != ExpressionNode.LITERAL) {
            return false;
        }
        for (int i = 0, n = node.args.size(); i < n; i++) {
            if (node.args.getQuick(i).type != ExpressionNode.LITERAL) {
                return false;
            }
        }
        return true;
    }

    private static boolean isCaseText(int type) {
        return type == ColumnType.STRING || type == ColumnType.VARCHAR || type == ColumnType.SYMBOL;
    }

    private static boolean isConnective(CharSequence name) {
        return SqlKeywords.isAndKeyword(name) || SqlKeywords.isOrKeyword(name);
    }

    private static boolean isLiteral(BoundExpression expression) {
        if (expression instanceof FunctionExpression call) {
            return isNegatedLiteral(call.getName(), call.getArguments());
        }
        return expression instanceof ConstantExpression constant && constant.isLiteral();
    }

    private static boolean isNegatedLiteral(CharSequence name, ObjList<BoundExpression> arguments) {
        return arguments.size() == 1 && Chars.equals(name, "-") && isLiteral(arguments.getQuick(0));
    }

    private static boolean isPrimitiveNumeric(int type) {
        return type == ColumnType.BYTE || type == ColumnType.SHORT || type == ColumnType.INT
                || type == ColumnType.LONG || type == ColumnType.FLOAT || type == ColumnType.DOUBLE;
    }

    /**
     * A TIMESTAMP or DATE value that is not a constant: the side of a comparison whose precision the
     * constants on the other side adopt.
     */
    private static boolean isTemporalOperand(Function function) {
        return !function.isConstant()
                && (ColumnType.tagOf(function.getType()) == ColumnType.TIMESTAMP || function.getType() == ColumnType.DATE);
    }

    private static boolean isTimestampConvertible(OutputSchema output, int timestampType) {
        if (output.getColumnCount() != 1) {
            return false;
        }
        final int columnType = output.getColumnType(0);
        return ColumnType.isNull(columnType) || ColumnType.isConvertibleFrom(columnType, timestampType);
    }

    private static boolean isTimestampText(TimestampDriver driver, CharSequence text) {
        try {
            IntervalUtils.parseTimestampText(driver, text);
            return true;
        } catch (NumericException e) {
            return false;
        }
    }

    private static CharSequence literalText(BoundExpression expression) {
        return expression instanceof ConstantExpression constant ? constant.getText() : null;
    }

    private static ConstantExpression markSource(ConstantExpression folded, BoundExpression source) {
        if (source instanceof ConstantExpression constant) {
            if (constant.isLiteral()) {
                return folded.markLiteral(constant.getSource());
            }
            return folded.withSource(constant.getSource());
        }
        if (source instanceof FunctionExpression call) {
            if (isLiteral(call)) {
                return folded.markLiteral(call);
            }
            return folded.withSource(call);
        }
        return folded;
    }

    private static boolean requiresConditionalRebuild(FunctionFactoryDescriptor overload, ObjList<Function> args) {
        final boolean isSwitch = overload.isSwitch();
        if (args == null || !isSwitch && !overload.isCase()) {
            return false;
        }
        final int count = args.size();
        final boolean hasElse = (count & 1) == (isSwitch ? 0 : 1);
        final int firstValue = isSwitch ? 2 : 1;
        int valueType = ColumnType.UNDEFINED;
        boolean hasText = false;
        boolean hasNonConstantChar = false;
        for (int i = firstValue; i < count; i++) {
            if (((i - firstValue) & 1) != 0 && !(hasElse && i == count - 1)) {
                continue;
            }
            final int type = args.getQuick(i).getType();
            if (type == ColumnType.NULL) {
                continue;
            }
            hasText |= isCaseText(type);
            hasNonConstantChar |= type == ColumnType.CHAR && !args.getQuick(i).isConstant();
            // CaseCommon wraps branches outside argsToPoke; only final-layout construction owns them.
            if (hasText && hasNonConstantChar) {
                return true;
            }
            if (valueType != ColumnType.UNDEFINED && valueType != type
                    && !(isPrimitiveNumeric(valueType) && isPrimitiveNumeric(type))
                    && !((isCaseText(valueType) || valueType == ColumnType.CHAR)
                    && (isCaseText(type) || type == ColumnType.CHAR))) {
                return true;
            }
            valueType = type;
        }
        return false;
    }

    private static TimestampDriver temporalDriver(int type) {
        return type == ColumnType.DATE ? MillisTimestampDriver.INSTANCE : ColumnType.getTimestampDriver(type);
    }

    /**
     * Returns the operand of a unary NOT, otherwise the node itself.
     */
    private static ExpressionNode unwrapNot(ExpressionNode node) {
        return node.paramCount == 1 && SqlKeywords.isNotKeyword(node.token) ? node.rhs : node;
    }

    private static int windowContextFlags(WindowContext context) {
        return (context.isDesignatedTimestampRead() ? BoundExpression.DESIGNATED_TIMESTAMP : 0)
                | (context.isTimestampOrderRead() ? BoundExpression.ORDERED_INPUT : 0);
    }

    private void beginArguments(int count) {
        arguments.clear();
        argumentLeafMarks.clear();
        for (int i = 0; i < count; i++) {
            final int index = expressionStack.size() - 1;
            arguments.add(expressionStack.getQuick(index));
            argumentLeafMarks.add(expressionLeafMarks.getQuick(index));
            expressionStack.remove(index);
            expressionLeafMarks.removeIndex(index);
        }
    }

    private void addUnconstructed(BoundExpression expression) {
        if (unconstructedIndex.putIfAbsent(expression, expression)) {
            unconstructed.add(expression);
        }
    }

    /**
     * Binds the expression tree of the node in its role: the role, the replacement lists and the native timestamp
     * ids hold for this binding only.
     */
    private BoundExpression bind(
            ExpressionNode node,
            BindingRole role,
            OutputSchema input,
            CharSequence inputAlias,
            int preferredType,
            ObjList<ExpressionNode> replacementNodes,
            ObjList<? extends BoundExpression> replacementColumns,
            IntHashSet nativeTimestampIds,
            SqlExecutionContext executionContext
    ) throws SqlException {
        final BindScope scope = ctx.scope();
        assert scope.bindingRole == null && (replacementNodes == null || replacementNodes.size() == replacementColumns.size());
        scope.bindingRole = role;
        scope.replacementNodes = replacementNodes;
        scope.replacementColumns = replacementColumns;
        scope.nativeTimestampIds = nativeTimestampIds;
        scope.nestedWindowPosition = -1;
        try {
            return bindRoot(node, null, 0, null, input, inputAlias, preferredType, executionContext);
        } finally {
            scope.bindingRole = null;
            scope.replacementNodes = null;
            scope.replacementColumns = null;
            scope.nativeTimestampIds = null;
        }
    }

    /**
     * Pushes the bound arguments as the operands of the named call, last argument first as a post-order walk visits
     * them, then resolves the call over them.
     */
    private void bindCallArguments(CharSequence name, int position, ObjList<? extends BoundExpression> args) throws SqlException {
        for (int i = args.size() - 1; i >= 0; i--) {
            pushArgument(args.getQuick(i));
        }
        pushFunction(resolveCall(name, position, isGroupBy(name) ? CallRole.AGGREGATE_ROOT : CallRole.ROOT, false, args.size()), position);
    }

    /**
     * Binds one root, the expression tree of the node or, without a node, the named call over already-bound
     * arguments, and hands its built root to the prepared functions.
     */
    private BoundExpression bindRoot(
            ExpressionNode node,
            CharSequence callName,
            int callPosition,
            ObjList<? extends BoundExpression> args,
            OutputSchema input,
            CharSequence inputAlias,
            int preferredType,
            SqlExecutionContext executionContext
    ) throws SqlException {
        final BindScope scope = ctx.scope();
        assert scope.currentPreparation == null;
        final int preparationMark = ctx.preparedFunctions.mark();
        final PreparedFunctions.Entry entry = ctx.preparedFunctions.begin();
        scope.currentPreparation = entry;
        scope.expressionInput = input;
        scope.expressionInputAlias = inputAlias;
        final SqlExecutionContext previousContext = sqlExecutionContext;
        sqlExecutionContext = executionContext;
        final int expressionMark = expressionStack.size();
        final int functionMark = functionStack.size();
        final int unconstructedMark = unconstructed.size();
        final int syntheticNodeMark = ctx.planNodes.syntheticNodes.getPos();
        final int staticTypeMark = ctx.planNodes.staticTypes.getPos();
        final boolean isUpdateAssignment = scope.bindingRole == BindingRole.UPDATE_ASSIGNMENT;
        try {
            if (node == null) {
                bindCallArguments(callName, callPosition, args);
            } else {
                bindTree(node, executionContext);
            }
            final int rootPosition = positionStack.getLast();
            Function function = foldRoot(popFunction(), rootPosition);
            assert functionStack.size() == functionMark;
            if (function instanceof StaticTypeFunction) {
                // UPDATE converts the owned root while binding; every other root is built by its generator.
                if (isUpdateAssignment) {
                    function = ctx.functionInstantiator.realize(expressionStack.getQuick(expressionMark), input, entry, executionContext);
                    ctx.preparedFunctions.own(entry, function);
                }
            } else {
                ctx.preparedFunctions.own(entry, function);
            }
            if (preferredType != ColumnType.UNDEFINED
                    && (isUpdateAssignment ? ColumnType.isUndefined(function.getType()) : function.isUndefined())) {
                function.assignType(preferredType, executionContext.getBindVariableService());
                finish(function);
            }
            assert expressionStack.size() == expressionMark + 1;
            ctx.preparedFunctions.describe(entry, expressionStack.getQuick(expressionMark));
            assert PreparedFunctions.hasOnlyReadLeaves(entry) : "bound leaf is not read by its description";
            return entry.getExpression();
        } catch (Throwable th) {
            // Release partially bound operands best-effort; only completed roots enter this scope.
            for (int i = functionStack.size() - 1; i >= functionMark; i--) {
                Misc.free(functionStack.getQuick(i), th);
            }
            ctx.preparedFunctions.closeOnFailure(preparationMark, th);
            throw th;
        } finally {
            sqlExecutionContext = previousContext;
            functionStack.setPos(functionMark);
            positionStack.setPos(functionMark);
            scope.bindingRoot = null;
            scope.projectedOffsets.clear();
            ctx.planNodes.rewindSyntheticNodes(syntheticNodeMark);
            scope.currentPreparation = null;
            scope.boundLowerBound = null;
            scope.boundLowerBoundNode = null;
            scope.expressionInput = null;
            scope.expressionInputAlias = null;
            arguments.clear();
            argumentLeafMarks.clear();
            argumentPositions.clear();
            expressionStack.setPos(expressionMark);
            expressionLeafMarks.setPos(expressionMark);
            clearUnconstructed(unconstructedMark);
            ctx.planNodes.staticTypes.rewind(staticTypeMark);
        }
    }

    /**
     * Timestamp interval analysis binds a sub-query BETWEEN bound pair low bound first.
     */
    private void bindTimestampBetweenLowerBound(ExpressionNode node, SqlExecutionContext executionContext) throws SqlException {
        final ExpressionNode lo = findTimestampBetweenLowerBound(node);
        if (lo != null) {
            final BindScope scope = ctx.scope();
            scope.boundLowerBound = compiler.bindSubquery(lo.queryModel, lo.position, executionContext);
            scope.boundLowerBoundNode = lo;
        }
    }

    /**
     * Walks the expression tree of the root in post-order, after the rewrites its role applies.
     */
    private void bindTree(ExpressionNode node, SqlExecutionContext executionContext) throws SqlException {
        final BindScope scope = ctx.scope();
        final boolean isRewritable = isAstRewritable() && (scope.bindingRole != BindingRole.WINDOW || !hasOnlyColumnArguments(node));
        if (isRewritable) {
            // DECLARE references may share parser nodes across occurrences.
            // Reassociation belongs to this binding, never to that shared AST.
            node = ExpressionNode.deepClone(ctx.planNodes.syntheticNodes, node);
        }
        scope.bindingRoot = node;
        if (scope.bindingRole == BindingRole.PREDICATE) {
            if (isRewritable) {
                rewriteAndOffsets(node);
            }
            bindTimestampBetweenLowerBound(node, executionContext);
        }
        if (isRewritable) {
            // Caller-selected group keys refer to exact subtrees. Reassociating
            // their ancestors here could move children across that boundary.
            node.reassociateConstants(resolver.getConfiguration().getCairoSqlLegacyOperatorPrecedence());
        }
        traverseAlgo.traverse(node, this);
    }

    /**
     * The leaf mark of the call whose arguments are being bound: its leaves follow it in the preparation.
     */
    private int callLeafMark() {
        final int count = argumentLeafMarks.size();
        return count > 0 ? argumentLeafMarks.getQuick(count - 1) : leafMark();
    }

    private CallRole callRole(ExpressionNode node) {
        final BindScope scope = ctx.scope();
        if (node == scope.bindingRoot) {
            return switch (scope.bindingRole) {
                case WINDOW -> CallRole.WINDOW_ROOT;
                case AGGREGATE, GROUP_BY_EXPRESSION -> CallRole.AGGREGATE_ROOT;
                case STANDALONE ->
                        node.type == ExpressionNode.FUNCTION && isGroupBy(node.token) ? CallRole.AGGREGATE_ROOT : CallRole.ROOT;
                default -> CallRole.ROOT;
            };
        }
        if (scope.projectedOffsets.indexOf(node) >= 0) {
            return CallRole.PROJECTED_OFFSET;
        }
        return isWindowArgument(node) ? CallRole.WINDOW_ARGUMENT : CallRole.ARGUMENT;
    }

    private Function canonicalizeTemporalBetween(ObjList<Function> args) throws SqlException {
        final Function operand = args.getQuick(0);
        if (!isTemporalOperand(operand)) {
            return null;
        }
        final int operandType = operand.getType();
        final TimestampDriver driver = temporalDriver(operandType);
        final int loType = temporalConstantType(args, 1, driver);
        final int hiType = temporalConstantType(args, 2, driver);
        if (loType != ColumnType.UNDEFINED) {
            validateTemporalConstant(args, 1, driver);
        }
        if (hiType != ColumnType.UNDEFINED) {
            validateTemporalConstant(args, 2, driver);
        }
        try {
            if (loType != ColumnType.UNDEFINED && hiType != ColumnType.UNDEFINED) {
                if (isOperandTyped(args, 1, operandType) && isOperandTyped(args, 2, operandType)) {
                    return null;
                }
                final long lo = temporalConstantValue(args, 1, loType, driver);
                final long hi = temporalConstantValue(args, 2, hiType, driver);
                if (lo == Numbers.LONG_NULL || hi == Numbers.LONG_NULL) {
                    return null;
                }
                final long low = driver.betweenLo(lo, loType, hi, hiType);
                final long high = driver.betweenHi(lo, loType, hi, hiType);
                if (low > high) {
                    return BooleanConstant.FALSE;
                }
                replaceTemporalConstant(args, 1, low, operandType, false);
                replaceTemporalConstant(args, 2, high, operandType, false);
            } else if (loType != ColumnType.UNDEFINED && !isOperandTyped(args, 1, operandType)) {
                replaceExactTemporalConstant(args, 1, loType, operandType, driver);
            } else if (hiType != ColumnType.UNDEFINED && !isOperandTyped(args, 2, operandType)) {
                replaceExactTemporalConstant(args, 2, hiType, operandType, driver);
            }
        } catch (NumericException | ImplicitCastException e) {
            return null;
        }
        return null;
    }

    private Function canonicalizeTemporalBinary(CharSequence operator, ObjList<Function> args) throws SqlException {
        final int operandIndex = isTemporalOperand(args.getQuick(0)) ? 0 : isTemporalOperand(args.getQuick(1)) ? 1 : -1;
        if (operandIndex < 0) {
            return null;
        }
        final int index = 1 - operandIndex;
        final int operandType = args.getQuick(operandIndex).getType();
        final TimestampDriver driver = temporalDriver(operandType);
        final int type = temporalConstantType(args, index, driver);
        if (type == ColumnType.UNDEFINED || isOperandTyped(args, index, operandType)) {
            return null;
        }
        validateTemporalConstant(args, index, driver);
        try {
            final long value = temporalConstantValue(args, index, type, driver);
            if (value == Numbers.LONG_NULL) {
                return null;
            }
            final long ceil = driver.ceilFrom(value, type);
            final long floor = driver.floorFrom(value, type);
            final boolean isExact = ceil == floor;
            if (Chars.equals(operator, '=') || FunctionResolver.isNotEqualsOperator(operator)) {
                if (!isExact) {
                    return Chars.equals(operator, '=') ? BooleanConstant.FALSE : BooleanConstant.TRUE;
                }
                replaceTemporalConstant(args, index, ceil, operandType, true);
            } else {
                replaceTemporalConstant(args, index, FunctionResolver.isRoundedUp(operator, index) ? ceil : floor, operandType, isExact);
            }
        } catch (NumericException | ImplicitCastException e) {
            return null;
        }
        return null;
    }

    /**
     * Rewrites a comparison of a TIMESTAMP or DATE operand with constants of another precision into the
     * same comparison at the operand's precision, so every consumer compares column-precision values.
     * The rounding is exact: a lower bound rounds up and an upper bound down (TimestampDriver.ceilFrom and
     * floorFrom), an equality with a value the operand cannot hold is false and its negation true, and an
     * IN element it cannot hold drops out. A constant outside the range of the operand's precision fails at its
     * position, see {@link #validateTemporalConstant}. Returns the boolean the comparison folds to, or null.
     */
    private Function canonicalizeTemporalComparison(CharSequence operator, ObjList<Function> args, IntList positions) throws SqlException {
        final int count = args == null ? 0 : args.size();
        if (count == 2 && FunctionResolver.isTemporalComparisonOperator(operator)) {
            return canonicalizeTemporalBinary(operator, args);
        }
        if (count == 3 && SqlKeywords.isBetweenKeyword(operator)) {
            return canonicalizeTemporalBetween(args);
        }
        if (SqlKeywords.isInKeyword(operator) && (count > 2 || count == 2 && !isCaseText(args.getQuick(1).getType()))) {
            return canonicalizeTemporalIn(args, positions);
        }
        return null;
    }

    private Function canonicalizeTemporalIn(ObjList<Function> args, IntList positions) throws SqlException {
        final Function operand = args.getQuick(0);
        if (!isTemporalOperand(operand)) {
            return null;
        }
        final int operandType = operand.getType();
        final TimestampDriver driver = temporalDriver(operandType);
        boolean isMatchable = false;
        for (int i = args.size() - 1; i > 0; i--) {
            final int type = isCaseText(args.getQuick(i).getType())
                    ? textConstantType(args, i, driver) : temporalConstantType(args, i, driver);
            if (type == ColumnType.UNDEFINED || isOperandTyped(args, i, operandType)) {
                isMatchable = true;
                continue;
            }
            validateTemporalConstant(args, i, driver);
            try {
                final long value = temporalConstantValue(args, i, type, driver);
                if (value == Numbers.LONG_NULL || driver.isExactFrom(value, type)) {
                    replaceTemporalConstant(args, i, driver.ceilFrom(value, type), operandType, true);
                    isMatchable = true;
                } else if (args.size() == 3
                        && ColumnType.isVarcharOrString(args.getQuick(3 - i).getType())
                        && textConstantType(args, 3 - i, driver) == ColumnType.UNDEFINED) {
                    replaceTemporalConstant(args, i, value, type, true);
                } else {
                    Misc.free(args.getQuick(i));
                    args.remove(i);
                    arguments.remove(i);
                    argumentLeafMarks.removeIndex(i);
                    if (positions != null) {
                        positions.removeIndex(i);
                    }
                }
            } catch (NumericException | ImplicitCastException e) {
                isMatchable = true;
            }
        }
        return isMatchable ? null : BooleanConstant.FALSE;
    }

    private Function captureConstant(Function function, int position, CharSequence token) {
        try {
            if (function instanceof TypeConstant) {
                push(type(function, position), leafMark());
                return function;
            }
            final ConstantExpression constant = constant(function, position).markLiteral(null);
            final int tag = ColumnType.tagOf(constant.getDataType());
            push(token != null && (tag == ColumnType.DOUBLE || tag == ColumnType.FLOAT)
                    ? constant.withLiteralText(Chars.toString(token)) : constant, leafMark());
            return function;
        } catch (Throwable th) {
            Misc.free(function, th);
            throw th;
        }
    }

    private Function captureImplicitConversion(int index, Function function, int position, Class<? extends FunctionFactory> factoryClass) {
        if (function.isConstant()) {
            final Function folded = resolver.functionToConstant(function, position);
            arguments.setQuick(index, constant(folded, position));
            return folded;
        }
        final ObjList<FunctionFactoryDescriptor> casts = resolver.getFunctionFactoryCache().getOverloadList("cast");
        for (int i = 0, n = casts.size(); i < n; i++) {
            final FunctionFactoryDescriptor overload = casts.getQuick(i);
            if (overload.getFactory().getClass() == factoryClass) {
                ctx.tmpArguments.clear();
                ctx.tmpPositions.clear();
                try {
                    ctx.tmpArguments.add(arguments.getQuick(index));
                    ctx.tmpArguments.add(ctx.planNodes.types.next().of(function.getType(), position));
                    ctx.tmpPositions.add(position);
                    ctx.tmpPositions.add(position);
                    final FunctionExpression conversion = ctx.planNodes.functions.next().of(overload, ctx.tmpArguments,
                            ctx.tmpPositions, function.getType(),
                            BoundExpression.functionFlags(function), position);
                    if (!unconstructedIndex.excludes(arguments.getQuick(index))) {
                        addUnconstructed(conversion);
                    }
                    arguments.setQuick(index, conversion);
                    return function;
                } finally {
                    ctx.tmpArguments.clear();
                    ctx.tmpPositions.clear();
                }
            }
        }
        throw new IllegalStateException("implicit conversion factory is not registered as a cast");
    }

    private void captureImplicitConversions(ObjList<Function> args, IntList positions) {
        for (int i = 0, n = args == null ? 0 : args.size(); i < n; i++) {
            final Class<? extends FunctionFactory> conversion = resolver.getImplicitConversion(i);
            if (conversion != null) {
                try {
                    args.setQuick(i, captureImplicitConversion(i, args.getQuick(i), positions.getQuick(i), conversion));
                } catch (Throwable th) {
                    Misc.freeObjList(args, th);
                    throw th;
                }
            }
        }
    }

    private void captureParameter(Function function, ExpressionNode node, boolean isPredefined) {
        try {
            final BindVariableExpression parameter = ctx.planNodes.parameters.next().of(node.token, function.getType(), BoundExpression.functionFlags(function), node.position);
            push(isPredefined ? parameter.markPredefined() : parameter, leafMark());
        } catch (Throwable th) {
            Misc.free(function, th);
            throw th;
        }
    }

    private void clearUnconstructed(int mark) {
        while (unconstructed.size() > mark) {
            unconstructedIndex.remove(unconstructed.popLast());
        }
    }

    /**
     * Closes the column leaves at {@code [from, to)} of the preparation: a fold evaluated the operand they belong
     * to, so the bound description no longer reads them.
     */
    private void closeLeaves(int from, int to) {
        if (from < to) {
            final ObjList<BindableColumn> leaves = ctx.scope().currentPreparation.leaves;
            for (int i = from; i < to; i++) {
                final BindableColumn leaf = leaves.getQuick(i);
                if (leaf.isOpen()) {
                    leaf.close();
                }
            }
        }
    }

    /**
     * Builds the arguments the binder typed without constructing them, for a parent that is constructed now.
     */
    private void constructArguments(ObjList<Function> args, int count, SqlExecutionContext executionContext) throws SqlException {
        for (int i = 0; i < count; i++) {
            final BoundExpression argument = arguments.getQuick(i);
            if (!unconstructedIndex.excludes(argument)) {
                final BindScope scope = ctx.scope();
                final Function placeholder = args.getQuick(i);
                args.setQuick(i, ctx.functionInstantiator.realize(argument, scope.expressionInput, scope.currentPreparation, executionContext));
                Misc.free(placeholder);
            }
        }
    }

    private Function createColumn(ExpressionNode node, int foundIndex) throws SqlException {
        final BindScope scope = ctx.scope();
        final OutputSchema input = scope.expressionInput;
        if (foundIndex == -1) {
            final Function outer = createOuterColumn(node);
            if (outer != null) {
                return outer;
            }
        }
        final int index = checkColumn(node, foundIndex, input, scope.expressionInputAlias);
        final int type = input.getColumnType(index);
        final int columnId = input.getColumnId(index);
        final int mark = leafMark();
        final Function leaf = scope.currentPreparation.addColumn(scope.bindingRole == BindingRole.WINDOW
                ? FunctionInstantiator.createColumnFunction(node.position, index, type, input)
                : FunctionInstantiator.createColumnLeaf(node.position, columnId, index, type, input));
        push(ctx.planNodes.columns.next().of(columnId, type, node.position), mark);
        return leaf;
    }

    private Function createCursorFunction(ExpressionNode node, SqlExecutionContext executionContext) throws SqlException {
        final BindScope scope = ctx.scope();
        final Subquery subquery;
        if (node == scope.boundLowerBoundNode) {
            subquery = scope.boundLowerBound;
            scope.boundLowerBound = null;
            scope.boundLowerBoundNode = null;
        } else {
            subquery = compiler.bindSubquery(node.queryModel, node.position, executionContext);
        }
        scope.currentPreparation.isRebuildRequired = true;
        final CursorExpression cursor = nextCursor().of(subquery, node.position);
        final Function function = new CursorFunction(subquery.getOutputMetadata());
        if ((node.queryModel == scope.timestampFromSubquery || node.queryModel == scope.timestampToSubquery)
                && isTimestampConvertible(subquery.getRoot().getOutput(), scope.timestampSubqueryType)) {
            push(nextCursor().ofTimestamp(cursor, scope.timestampSubqueryType,
                    BoundExpression.RUNTIME_CONSTANT | cursor.getFunctionFlags() & BoundExpression.STABLE_WITHIN_EXECUTION), leafMark());
            return new ScalarSubQueryTimestampFunction(function, node.position, scope.timestampSubqueryType);
        }
        push(cursor, leafMark());
        return function;
    }

    /**
     * Builds the description of a call over coerced arguments and constructs it, or leaves it unconstructed with
     * a static type placeholder when the binder can type it without construction.
     */
    private Function createFunction(
            FunctionFactoryDescriptor overload,
            CharSequence name,
            int position,
            CallRole role,
            boolean isSetOperation,
            ObjList<Function> args,
            IntList positions
    ) throws SqlException {
        final BindScope scope = ctx.scope();
        final int staticType;
        try {
            validateTimestampText(overload, name, args, sqlExecutionContext);
            final Function folded = canonicalizeTemporalComparison(name, args, positions);
            if (folded != null) {
                Misc.freeObjList(args);
                dropLeaves(callLeafMark(), leafMark());
                push(ctx.planNodes.constants.next().ofBoolean(folded.getBool(null), position).markLiteral(), leafMark());
                return folded;
            }
            final int count = args == null ? 0 : args.size();
            final int signatureCount = overload.getSigArgCount();
            final boolean variadic = signatureCount > 0
                    && FunctionFactoryDescriptor.toTypeTag(overload.getArgTypeWithFlags(signatureCount - 1)) == ColumnType.VAR_ARG;
            if (variadic ? count < signatureCount - 1 : count != signatureCount) {
                throw new IllegalStateException("selected function arity has changed");
            }
            // Check actual normalized children, not just signature types. Numeric
            // getter promotion does not introduce an implicit cast expression.
            for (int i = 0; i < count; i++) {
                final Function arg = args.getQuick(i);
                final int type = arg.getType();
                arguments.setQuick(i, normalizeArgument(arguments.getQuick(i), arg));
                final int tag = ColumnType.tagOf(type);
                final int signatureFlags = overload.getArgTypeWithFlags(Math.min(i, signatureCount - 1));
                final int signatureType = FunctionFactoryDescriptor.toTypeTag(signatureFlags);
                if (tag == ColumnType.ARRAY && !FunctionFactoryDescriptor.isArray(signatureFlags)
                        && signatureType != ColumnType.VAR_ARG && !overload.isArrayElementWiseScalar()
                        && !Chars.equals(overload.getName(), "array") && !Chars.equals(overload.getName(), "cast")
                        || arguments.getQuick(i).getDataType() != type) {
                    throw new IllegalStateException("bound argument does not match its function");
                }
            }
            if (count == 2 && scope.keySubqueryColumnIds.size() > 0 && SqlKeywords.isInKeyword(name)) {
                validateKeySubquery();
            }
            if (requiresConditionalRebuild(overload, args)) {
                scope.currentPreparation.isRebuildRequired = true;
            }
            argumentPositions.clear();
            if (positions != null) {
                argumentPositions.addAll(positions);
            }
            final int declaredType = staticResultType(overload, role, count);
            staticType = declaredType != ColumnType.UNDEFINED
                    && resolver.admitUnconstructed(overload, position, name, args, positions, sqlExecutionContext)
                    ? declaredType : ColumnType.UNDEFINED;
            if (staticType == ColumnType.UNDEFINED) {
                constructArguments(args, count, sqlExecutionContext);
            }
        } catch (Throwable th) {
            Misc.freeObjList(args, th);
            throw th;
        }
        if (staticType != ColumnType.UNDEFINED) {
            prepareUnconstructedArguments(args);
            Misc.freeObjList(args);
            args.clear();
            // The audit contract of RelocatableScalarFactories: a SYMBOL result or argument denies parallelism.
            int flags = BoundExpression.STABLE_WITHIN_EXECUTION | (ColumnType.tagOf(staticType) == ColumnType.SYMBOL ? BoundExpression.NO_PARALLELISM : 0);
            for (int i = 0, n = arguments.size(); i < n; i++) {
                flags |= arguments.getQuick(i).getFunctionFlags() & BoundExpression.NO_PARALLELISM;
            }
            addUnconstructed(pushCall(name, role, isSetOperation, ctx.planNodes.functions.next().of(overload, arguments, argumentPositions,
                    staticType, flags, position)));
            return ctx.planNodes.staticTypes.next().of(staticType, (flags & BoundExpression.NO_PARALLELISM) == 0);
        }
        final Function first = args != null && args.size() == 2 ? args.getQuick(0) : null;
        final Function second = first != null ? args.getQuick(1) : null;
        final Function function = resolver.createFunction(overload, position, name, args, positions, sqlExecutionContext);
        try {
            if (role == CallRole.WINDOW_ROOT) {
                if (!(function instanceof WindowFunction)) {
                    throw SqlException.$(position, "non-window function called in window context");
                }
                if (scope.nestedWindowPosition >= 0) {
                    throw SqlException.emptyWindowContext(scope.nestedWindowPosition);
                }
            }
            // Some factories return a folded constant and close unused children.
            // Preserve that replacement rather than retaining false dependencies.
            final BoundExpression expression;
            if (function instanceof ConstantFunction && !ColumnType.isArray(function.getType())) {
                expression = markSource(constant(function, position), overload, function.getType(), BoundExpression.functionFlags(function), position);
                dropLeaves(callLeafMark(), leafMark());
            } else if (first != null && isConnective(name) && (function == first || function == second)) {
                // A connective with a constant operand returns its other operand.
                expression = arguments.getQuick(function == first ? 0 : 1);
            } else {
                final int flags = role == CallRole.WINDOW_ROOT ? callFlags(function, arguments) | windowContextFlags(sqlExecutionContext.getWindowContext())
                        : callFlags(function, arguments);
                expression = ctx.planNodes.functions.next().of(overload, arguments, argumentPositions, function.getType(), flags, position);
            }
            pushCall(name, role, isSetOperation, expression);
            return function;
        } catch (Throwable th) {
            Misc.free(function, th);
            throw th;
        }
    }

    private Function createOuterColumn(ExpressionNode node) throws SqlException {
        final BindScope scope = ctx.scope();
        for (int i = scope.outerScopes.size() - 1; i > -1; i--) {
            final OutputSchema outer = scope.outerScopes.getQuick(i);
            final int index = findColumn(node, outer, null);
            if (index > -1) {
                final int columnId = outer.getColumnId(index);
                final int type = outer.getColumnType(index);
                scope.outerColumnIds.add(columnId);
                scope.currentPreparation.isRebuildRequired = true;
                push(ctx.planNodes.outerColumns.next().of(columnId, type, node.position), leafMark());
                return FunctionInstantiator.createColumnLeaf(node.position, columnId, index, type, outer);
            }
        }
        return null;
    }

    private Function createParameter(ExpressionNode node) throws SqlException {
        final boolean isPredefined = isBindVariableDefined(node.token);
        final Function parameter = resolver.createBindVariable(node.position, node.token, sqlExecutionContext);
        captureParameter(parameter, node, isPredefined);
        return parameter;
    }

    /**
     * Closes and removes the column leaves at {@code [from, to)} of the preparation: a fold dropped the operand they
     * belong to, so the bound description no longer reads them.
     */
    private void dropLeaves(int from, int to) {
        if (from < to) {
            closeLeaves(from, to);
            ctx.scope().currentPreparation.leaves.remove(from, to - 1);
        }
    }

    /**
     * The sub-query low bound of the first conjunct that compares a native timestamp column BETWEEN two sub-queries,
     * conjuncts left to right, or null.
     */
    private ExpressionNode findTimestampBetweenLowerBound(ExpressionNode node) {
        if (node.paramCount == 2 && SqlKeywords.isAndKeyword(node.token)) {
            final ExpressionNode lo = findTimestampBetweenLowerBound(node.lhs);
            return lo != null ? lo : findTimestampBetweenLowerBound(node.rhs);
        }
        final ExpressionNode conjunct = unwrapNot(node);
        if (conjunct.paramCount == 3 && SqlKeywords.isBetweenKeyword(conjunct.token)
                && conjunct.args.getQuick(0).type == ExpressionNode.QUERY
                && conjunct.args.getQuick(1).type == ExpressionNode.QUERY
                && conjunct.args.getQuick(2).type == ExpressionNode.LITERAL) {
            final BindScope scope = ctx.scope();
            final int columnIndex = findColumn(conjunct.args.getQuick(2), scope.expressionInput, scope.expressionInputAlias);
            if (columnIndex >= 0 && isNativeTimestampColumn(scope.expressionInput.getColumnId(columnIndex))) {
                return conjunct.args.getQuick(1);
            }
        }
        return null;
    }

    private void finish(Function function) {
        final int index = expressionStack.size() - 1;
        final BoundExpression normalized = normalizeArgument(expressionStack.getQuick(index), function);
        if (normalized instanceof ConstantExpression) {
            dropLeaves(expressionLeafMarks.getQuick(index), leafMark());
        }
        expressionStack.setQuick(index, normalized);
    }

    private Function foldArgument(int index, Function function, int position) {
        final Function argument = resolver.foldArgument(function, position);
        try {
            if (argument instanceof ConstantFunction && !(argument instanceof TypeConstant)) {
                final BoundExpression expression = arguments.getQuick(index);
                if (ColumnType.isArray(argument.getType())) {
                    arguments.setQuick(index, normalizeArgument(expression, argument));
                } else {
                    arguments.setQuick(index, markSource(constant(argument, position), expression));
                    closeLeaves(argumentLeafMarks.getQuick(index), index > 0 ? argumentLeafMarks.getQuick(index - 1) : leafMark());
                }
            }
            return argument;
        } catch (Throwable th) {
            Misc.free(argument, th);
            throw th;
        }
    }

    private Function foldRoot(Function function, int position) {
        final Function root = resolver.foldRoot(function, position);
        try {
            finish(root);
            return root;
        } catch (Throwable th) {
            Misc.free(root, th);
            throw th;
        }
    }

    /**
     * The spelling interval extraction parses text in: a string literal quoted as in SQL, which keeps a bare
     * number from reading as an epoch, other text as it is.
     */
    private CharSequence intervalSpelling(BoundExpression argument, CharSequence text) {
        if (argument instanceof ConstantExpression constant && IntervalExtractor.isStringLiteral(constant)) {
            intervalText.clear();
            intervalText.put('\'').put(text).put('\'');
            return intervalText;
        }
        return text;
    }

    private boolean isAstRewritable() {
        final ObjList<ExpressionNode> replacementNodes = ctx.scope().replacementNodes;
        return replacementNodes == null || replacementNodes.size() == 0;
    }

    private boolean isBindVariableDefined(CharSequence name) {
        final BindVariableService service = sqlExecutionContext.getBindVariableService();
        if (service == null) {
            return false;
        }
        if (name.charAt(0) == ':') {
            return service.getFunction(name) != null;
        }
        try {
            final int index = Numbers.parseInt(name, 1, name.length());
            return index > 0 && service.getFunction(index - 1) != null;
        } catch (NumericException e) {
            return false;
        }
    }

    private boolean isNativeTimestampColumn(int columnId) {
        final BindScope scope = ctx.scope();
        final OutputSchema input = scope.expressionInput;
        return scope.nativeTimestampIds != null ? scope.nativeTimestampIds.contains(columnId)
                : input.getTimestampIndex() >= 0 && columnId == input.getColumnId(input.getTimestampIndex());
    }

    /**
     * Whether a comparison constant already holds a value at the operand's precision: its type is the
     * operand's, and for a DATE operand it was not typed from text, which DATE typing truncates.
     */
    private boolean isOperandTyped(ObjList<Function> args, int index, int operandType) {
        return args.getQuick(index).getType() == operandType
                && (operandType != ColumnType.DATE || literalText(arguments.getQuick(index)) == null);
    }

    private boolean isStringLiteral(ObjList<Function> args, int index) {
        return args.getQuick(index).isConstant() && arguments.getQuick(index) instanceof ConstantExpression constant
                && IntervalExtractor.isStringLiteral(constant);
    }

    private boolean isWindowArgument(ExpressionNode node) {
        final ExpressionNode windowRoot = windowRoot();
        if (windowRoot == null) {
            return false;
        }
        if (windowRoot.paramCount < 3) {
            return windowRoot.lhs == node || windowRoot.rhs == node;
        }
        return windowRoot.args.indexOf(node) >= 0;
    }

    /**
     * The number of column leaves of the preparation, where the leaves of the next bound operand start.
     */
    private int leafMark() {
        final PreparedFunctions.Entry currentPreparation = ctx.scope().currentPreparation;
        return currentPreparation != null ? currentPreparation.leaves.size() : 0;
    }

    private ConstantExpression markSource(ConstantExpression folded, FunctionFactoryDescriptor overload, int type, int flags, int position) {
        return markSource(folded, ctx.planNodes.functions.next().of(overload, arguments, argumentPositions, type, flags, position));
    }

    private CursorExpression nextCursor() {
        return ctx.planNodes.cursors.next();
    }

    private BoundExpression normalizeArgument(BoundExpression expression, Function function) {
        if (function instanceof TypeConstant) {
            return expression;
        }
        if (expression instanceof CursorExpression cursor && function instanceof BooleanSubQueryFunction) {
            return nextCursor().ofBoolean(cursor, BoundExpression.functionFlags(function) & (cursor.getFunctionFlags() | ~BoundExpression.STABLE_WITHIN_EXECUTION));
        }
        if (ColumnType.isArray(function.getType()) && expression instanceof FunctionExpression call
                && (call.getDataType() != function.getType() || call.getFunctionFlags() != callFlags(function, call.getArguments()))) {
            ctx.tmpArguments.clear();
            ctx.tmpPositions.clear();
            try {
                for (int i = 0; i < call.getArgumentCount(); i++) {
                    ctx.tmpArguments.add(call.argumentAt(i));
                    ctx.tmpPositions.add(call.getArgumentPosition(i));
                }
                return ctx.planNodes.functions.next().of(call.getOverload(), ctx.tmpArguments, ctx.tmpPositions,
                        function.getType(), callFlags(function, ctx.tmpArguments), call.getPosition());
            } finally {
                ctx.tmpArguments.clear();
                ctx.tmpPositions.clear();
            }
        }
        if (function instanceof ConstantFunction && !ColumnType.isArray(function.getType()) && (!(expression instanceof ConstantExpression)
                || expression.getDataType() != function.getType())) {
            final ConstantExpression normalized = markSource(constant(function, expression.getPosition()), expression);
            if (ColumnType.isTimestamp(function.getType()) && expression instanceof ConstantExpression original) {
                normalized.withTimestampText(original.getText());
            }
            return normalized;
        }
        if (expression instanceof BindVariableExpression parameter && expression.getDataType() != function.getType()) {
            return ctx.planNodes.parameters.next().of(parameter.getName(), function.getType(), BoundExpression.functionFlags(function), parameter.getPosition(), parameter.isDirectReference());
        }
        return expression;
    }

    private Function popFunction() {
        positionStack.setPos(positionStack.size() - 1);
        return functionStack.popLast();
    }

    /**
     * Hands each constant argument and column leaf of a call left unconstructed to the prepared functions, which own
     * it until the generator adopts it for the call's description, so generation does not build it again. A
     * fixed-size literal is the exception: the generator builds it from its description, which costs less than
     * preparing it. The caller frees the remaining arguments; on failure this frees every argument it has not handed
     * over.
     */
    private void prepareUnconstructedArguments(ObjList<Function> args) {
        try {
            for (int i = 0, n = args.size(); i < n; i++) {
                final Function arg = args.getQuick(i);
                final BoundExpression argument = arguments.getQuick(i);
                if (arg instanceof ConstantFunction && !(arg instanceof TypeConstant) && argument instanceof ConstantExpression constant
                        && (!constant.isLiteral() || ColumnType.isVarSize(constant.getDataType())) && constant.getDataType() == arg.getType()) {
                    final PreparedFunctions.Entry entry = ctx.preparedFunctions.begin();
                    ctx.preparedFunctions.describe(entry, constant);
                    ctx.preparedFunctions.own(entry, arg);
                    args.setQuick(i, null);
                } else if (arg instanceof BindableColumn leaf && leaf.isOpen() && argument instanceof ColumnExpression column
                        && column.getColumnId() == leaf.getColumnId() && column.getDataType() == leaf.getType()) {
                    final ObjList<BindableColumn> leaves = ctx.scope().currentPreparation.leaves;
                    int index = leaves.size() - 1;
                    while (index >= 0 && leaves.getQuick(index) != leaf) {
                        index--;
                    }
                    if (index >= 0) {
                        final PreparedFunctions.Entry entry = ctx.preparedFunctions.begin();
                        ctx.preparedFunctions.describe(entry, column);
                        entry.leaves.add(leaf);
                        ctx.preparedFunctions.own(entry, leaf);
                        leaves.remove(index);
                        args.setQuick(i, null);
                    }
                }
            }
        } catch (Throwable th) {
            Misc.freeObjList(args, th);
            throw th;
        }
    }

    /**
     * Pushes a bound operand with the leaf mark where its leaves start; folds drop leaves by these marks.
     */
    private void push(BoundExpression expression, int leafMark) {
        expressionStack.add(expression);
        expressionLeafMarks.add(leafMark);
    }

    private void pushArgument(BoundExpression argument) throws SqlException {
        pushFunction(pushReplacement(argument, argument.getPosition()), argument.getPosition());
    }

    private BoundExpression pushCall(CharSequence name, CallRole role, boolean isSetOperation, BoundExpression expression) {
        if (isSetOperation && expression instanceof FunctionExpression call && SqlKeywords.isInKeyword(name)) {
            call.markSetOperation();
        }
        final BoundExpression pushed = role == CallRole.PROJECTED_OFFSET && expression instanceof FunctionExpression call
                ? ctx.planNodes.functions.next().ofProjectedOffset(call) : expression;
        push(pushed, callLeafMark());
        return pushed;
    }

    private void pushFunction(Function function, int position) {
        functionStack.add(function);
        positionStack.add(position);
    }

    /**
     * Pushes an already-bound expression as the operand at the position: a column reads a fresh leaf, anything else
     * is built from its selected overloads.
     */
    private Function pushReplacement(BoundExpression replacement, int position) throws SqlException {
        final BindScope scope = ctx.scope();
        final OutputSchema input = scope.expressionInput;
        if (!(replacement instanceof ColumnExpression column)) {
            // Leaves of a reconstructed subtree carry this layout's indexes.
            if (replacement instanceof FunctionExpression || replacement instanceof CursorExpression || replacement instanceof OuterColumnExpression) {
                scope.currentPreparation.isRebuildRequired = true;
            }
            final Function function = ctx.functionInstantiator.rebuild(replacement, input, sqlExecutionContext);
            push(replacement, leafMark());
            return function;
        }
        final int index = input.getColumnIndexById(column.getColumnId());
        if (index < 0 || input.getColumnType(index) != column.getDataType()) {
            throw new IllegalStateException("bound expression replacement input has changed");
        }
        final int mark = leafMark();
        final Function leaf = scope.currentPreparation.addColumn(
                FunctionInstantiator.createColumnLeaf(position, column.getColumnId(), index, column.getDataType(), input));
        push(ctx.planNodes.columns.next().of(column.getColumnId(), column.getDataType(), position,
                column.isDirectReference(), column.isCast()), mark);
        return leaf;
    }

    private boolean referencesColumn(ExpressionNode node, int index) {
        if (node == null) {
            return false;
        }
        if (node.type == ExpressionNode.LITERAL) {
            final BindScope scope = ctx.scope();
            return findColumn(node, scope.expressionInput, scope.expressionInputAlias) == index;
        }
        if (referencesColumn(node.lhs, index) || referencesColumn(node.rhs, index)) {
            return true;
        }
        for (int i = 0, n = node.args.size(); i < n; i++) {
            if (referencesColumn(node.args.getQuick(i), index)) {
                return true;
            }
        }
        return false;
    }

    private void replaceExactTemporalConstant(ObjList<Function> args, int index, int type, int operandType, TimestampDriver driver) throws NumericException {
        final long value = temporalConstantValue(args, index, type, driver);
        if (value != Numbers.LONG_NULL && driver.isExactFrom(value, type)) {
            replaceTemporalConstant(args, index, driver.ceilFrom(value, type), operandType, true);
        }
    }

    /**
     * Replaces a comparison constant by its value at the operand's precision. An exact text literal keeps
     * its text, which normalization attaches to the converted constant; a rounded one loses it, because
     * the text no longer spells the value.
     */
    private void replaceTemporalConstant(ObjList<Function> args, int index, long value, int operandType, boolean isExact) {
        final Function replacement = operandType == ColumnType.DATE
                ? DateConstant.newInstance(value) : TimestampConstant.newInstance(value, operandType);
        Misc.free(args.getQuick(index));
        args.setQuick(index, replacement);
        final BoundExpression argument = arguments.getQuick(index);
        if (!isExact || literalText(argument) == null) {
            arguments.setQuick(index, constant(replacement, argument.getPosition()).markLiteral());
        }
    }

    private Function resolveCall(ExpressionNode node, int count) throws SqlException {
        return resolveCall(node.token, node.position, callRole(node), node.type == ExpressionNode.SET_OPERATION, count);
    }

    /**
     * Resolves the call over its top operands: folds constant arguments, selects the overload, applies the checks
     * of the call's place in the tree and the argument coercions, then builds its description and function.
     */
    private Function resolveCall(CharSequence name, int position, CallRole role, boolean isSetOperation, int count) throws SqlException {
        beginArguments(count);
        // A float literal's spelling selects its exact DECIMAL cast, as in SQL text.
        final CharSequence literal = count == 2 && arguments.getQuick(0) instanceof ConstantExpression constant ? constant.getLiteralText() : null;
        final int literalPosition = literal != null ? arguments.getQuick(0).getPosition() : 0;
        ObjList<Function> args = null;
        IntList positions = null;
        if (count > 0) {
            args = callFunctions;
            positions = callPositions;
            args.clear();
            args.setPos(count);
            positions.clear();
            positions.setPos(count);
            for (int i = 0; i < count; i++) {
                final int argPosition = positionStack.getLast();
                Function arg = popFunction();
                try {
                    arg = foldArgument(i, arg, argPosition);
                } catch (Throwable th) {
                    Misc.freeObjList(args, th);
                    throw th;
                }
                args.setQuick(i, arg);
                positions.setQuick(i, argPosition);
                FunctionResolver.rejectAggregateArgument(args, i, argPosition);
            }
            FunctionResolver.wrapRuntimeConstants(args);
            try {
                typeTimestampComparedOperands(name, args);
            } catch (Throwable th) {
                Misc.freeObjList(args, th);
                throw th;
            }
        }
        if (role == CallRole.WINDOW_ROOT) {
            try {
                validateSubsampleArguments(ctx.scope().bindingRoot, args, positions);
            } catch (Throwable th) {
                Misc.freeObjList(args, th);
                throw th;
            }
        }
        final Function cast = resolver.resolveCastOrOverload(name, position, args, positions, literal, literalPosition, sqlExecutionContext);
        if (cast != null) {
            return cast == args.getQuick(0) ? returnCastArgument(cast, args, position) : captureConstant(cast, position, null);
        }
        final FunctionFactoryDescriptor overload = resolver.getSelectedOverload();
        validateFactory(overload, role, position, args);
        resolver.coerceArguments(overload, position, name, args, positions, this, sqlExecutionContext);
        captureImplicitConversions(args, positions);
        return createFunction(overload, name, position, role, isSetOperation, args, positions);
    }

    private Function returnCastArgument(Function function, ObjList<Function> args, int position) {
        try {
            final BoundExpression argument = normalizeArgument(arguments.getQuick(0), function);
            // No runtime cast is needed, but diagnostics must retain the CAST's
            // source position without mutating the already captured argument.
            final BoundExpression repositioned = switch (argument) {
                case ColumnExpression column ->
                        ctx.planNodes.columns.next().of(column.getColumnId(), column.getDataType(), position, false, true);
                case OuterColumnExpression outer ->
                        ctx.planNodes.outerColumns.next().of(outer.getColumnId(), outer.getDataType(), position);
                case BindVariableExpression parameter ->
                        ctx.planNodes.parameters.next().of(parameter.getName(), parameter.getDataType(), parameter.getFunctionFlags(), position, false);
                case FunctionExpression call -> ctx.planNodes.functions.next().of(call, position);
                // a scalar sub-query reports its row-count errors at its own position
                case CursorExpression cursor -> ctx.planNodes.cursors.next().of(cursor);
                default -> constant(function, position);
            };
            if (!unconstructedIndex.excludes(argument)) {
                addUnconstructed(repositioned);
            }
            push(repositioned, callLeafMark());
            args.clear();
            return function;
        } catch (Throwable th) {
            Misc.freeObjList(args, th);
            throw th;
        }
    }

    /**
     * Rewrites a well-formed and_offset(predicate, unit, offset) into the predicate over
     * dateadd(unit, -offset, timestamp), which interval extraction inverts as a projected offset.
     * Any other and_offset stays unknown to the function parser.
     */
    private void rewriteAndOffsets(ExpressionNode node) {
        if (node == null) {
            return;
        }
        rewriteAndOffsets(node.lhs);
        rewriteAndOffsets(node.rhs);
        for (int i = 0, n = node.args.size(); i < n; i++) {
            rewriteAndOffsets(node.args.getQuick(i));
        }
        if (node.type != ExpressionNode.FUNCTION || node.args.size() != 3 || !Chars.equalsIgnoreCase(node.token, "and_offset")) {
            return;
        }
        final ExpressionNode predicate = node.args.getQuick(2);
        final int timestampIndex = ctx.scope().expressionInput.getTimestampIndex();
        final ExpressionNode unit = node.args.getQuick(1);
        final int unitLength = unit.token == null ? 0 : unit.token.length();
        if (timestampIndex < 0 || !referencesColumn(predicate, timestampIndex) || unit.type != ExpressionNode.CONSTANT
                || unitLength != 1 && (unitLength != 3 || unit.token.charAt(0) != '\'' || unit.token.charAt(2) != '\'')) {
            return;
        }
        final int offset;
        try {
            offset = Numbers.parseInt(node.args.getQuick(0).token);
        } catch (NumericException e) {
            return;
        }
        wrapTimestampColumns(predicate, unit.token, Integer.toString(-offset), timestampIndex);
        node.copyFrom(predicate);
    }

    /**
     * The result type the selected overload declares when the call may need no construction while binding: an
     * audited scalar over columns and other such callArguments, with type operands and constants beside them. Construction
     * over non-constant arguments cannot vary in type with argument values; the factory vets the arguments on
     * admission ({@link FunctionResolver#admitUnconstructed}). {@link ColumnType#UNDEFINED} when the call must be
     * constructed.
     */
    private int staticResultType(FunctionFactoryDescriptor overload, CallRole role, int count) {
        final FunctionFactory factory = overload.getFactory();
        if (count == 0 || !overload.isRelocatableScalar() || overload.isCase() || overload.isSwitch()
                || factory.isGroupBy() || factory.isWindow() || factory.isCursor() || role == CallRole.WINDOW_ROOT || role == CallRole.AGGREGATE_ROOT) {
            return ColumnType.UNDEFINED;
        }
        boolean hasVariableArgument = false;
        argumentTypes.clear();
        for (int i = 0; i < count; i++) {
            final BoundExpression argument = arguments.getQuick(i);
            final int type = argument.getDataType();
            if (!(argument instanceof TypeExpression) && !(argument instanceof ConstantExpression)) {
                if (ColumnType.isArray(type) || (argument.getFunctionFlags() & ~BoundExpression.NO_PARALLELISM) != BoundExpression.STABLE_WITHIN_EXECUTION
                        || !(argument instanceof ColumnExpression && BindableColumn.isBindableType(type) || !unconstructedIndex.excludes(argument))) {
                    return ColumnType.UNDEFINED;
                }
                hasVariableArgument = true;
            }
            argumentTypes.add(type);
        }
        if (!hasVariableArgument) {
            return ColumnType.UNDEFINED;
        }
        final int type = factory.getResultType(argumentTypes);
        return ColumnType.isArray(type) || ColumnType.isCursor(type) ? ColumnType.UNDEFINED : type;
    }

    /**
     * The precision at which a TIMESTAMP or DATE comparison constant is exact: the precision of the literal
     * it was typed from, or its own type; UNDEFINED for anything else.
     */
    private int temporalConstantType(ObjList<Function> args, int index, TimestampDriver driver) {
        final Function constant = args.getQuick(index);
        final int type = constant.getType();
        if (!constant.isConstant() || ColumnType.tagOf(type) != ColumnType.TIMESTAMP && type != ColumnType.DATE) {
            return ColumnType.UNDEFINED;
        }
        final CharSequence text = literalText(arguments.getQuick(index));
        return text != null ? IntervalUtils.timestampTextType(driver, text) : type;
    }

    /**
     * The value of a comparison constant at {@code type}, its {@link #temporalConstantType} or
     * {@link #textConstantType}; {@code driver} is the temporal operand's.
     */
    private long temporalConstantValue(ObjList<Function> args, int index, int type, TimestampDriver driver) throws NumericException {
        final Function constant = args.getQuick(index);
        final CharSequence text = literalText(arguments.getQuick(index));
        if (text != null) {
            final long value = IntervalUtils.parseTimestampText(driver, text);
            if (ColumnType.isTimestampNano(type) && ColumnType.isTimestampMicro(constant.getType())
                    && Math.floorDiv(value, 1_000) != constant.getTimestamp(null)) {
                return ColumnType.getTimestampDriver(type).from(constant.getTimestamp(null), constant.getType());
            }
            return value;
        }
        return type == ColumnType.DATE ? constant.getDate(null) : constant.getTimestamp(null);
    }

    /**
     * The precision of a text IN element that spells a timestamp; UNDEFINED for anything else.
     */
    private int textConstantType(ObjList<Function> args, int index, TimestampDriver driver) {
        if (ColumnType.isVarcharOrString(args.getQuick(index).getType()) && args.getQuick(index).isNullConstant()) {
            return driver.getTimestampType();
        }
        final CharSequence text = args.getQuick(index).isConstant() ? literalText(arguments.getQuick(index)) : null;
        return text != null ? IntervalUtils.timestampTextType(driver, text) : ColumnType.UNDEFINED;
    }

    private TypeExpression type(Function function, int position) {
        return switch (ColumnType.tagOf(function.getType())) {
            case ColumnType.BOOLEAN, ColumnType.INT, ColumnType.LONG, ColumnType.FLOAT, ColumnType.DOUBLE,
                 ColumnType.TIMESTAMP, ColumnType.STRING, ColumnType.VARCHAR, ColumnType.SYMBOL, ColumnType.CHAR,
                 ColumnType.BYTE, ColumnType.SHORT,
                 ColumnType.DATE, ColumnType.IPv4, ColumnType.ARRAY, ColumnType.UUID, ColumnType.LONG256,
                 ColumnType.GEOBYTE, ColumnType.GEOSHORT, ColumnType.GEOINT, ColumnType.GEOLONG,
                 ColumnType.BINARY, ColumnType.INTERVAL, ColumnType.REGCLASS, ColumnType.REGPROCEDURE,
                 ColumnType.ARRAY_STRING,
                 ColumnType.DECIMAL8, ColumnType.DECIMAL16, ColumnType.DECIMAL32, ColumnType.DECIMAL64,
                 ColumnType.DECIMAL128, ColumnType.DECIMAL256 ->
                    ctx.planNodes.types.next().of(function.getType(), position);
            default -> throw new IllegalStateException("unexpected CAST target type");
        };
    }

    private void typeQuotedCharAsText(ObjList<Function> args, int index) {
        if (arguments.getQuick(index) instanceof ConstantExpression constant && constant.getDataType() == ColumnType.CHAR
                && IntervalExtractor.isQuotedLiteral(constant)) {
            final Function text = StrConstant.fromValue(String.valueOf(args.getQuick(index).getChar(null)));
            Misc.free(args.getQuick(index));
            args.setQuick(index, text);
            arguments.setQuick(index, constant(text, constant.getPosition()).markLiteral());
        }
    }

    /**
     * Types the operands compared with a TIMESTAMP or DATE operand, by a comparison, a BETWEEN or an IN list: a
     * quoted one-character literal as the text it spells, as a longer literal is, rather than as a CHAR; and an
     * untyped bind variable compared with a TIMESTAMP operand, or a BETWEEN bound of one, at the operand's
     * precision, so its value parses as the text literal it stands for rather than at the precision of the
     * overload's signature.
     */
    private void typeTimestampComparedOperands(CharSequence name, ObjList<Function> args) throws SqlException {
        final int count = args.size();
        final boolean isBetween = count == 3 && SqlKeywords.isBetweenKeyword(name);
        if (count > 1 && SqlKeywords.isInKeyword(name) || isBetween) {
            if (isTemporalOperand(args.getQuick(0))) {
                for (int i = 1; i < count; i++) {
                    typeQuotedCharAsText(args, i);
                }
            }
        } else if (count == 2 && FunctionResolver.isTemporalComparisonOperator(name)) {
            for (int i = 0; i < 2; i++) {
                if (isTemporalOperand(args.getQuick(1 - i))) {
                    typeQuotedCharAsText(args, i);
                }
            }
        }
        if (!isBetween && (count != 2 || !FunctionResolver.isTemporalComparisonOperator(name))) {
            return;
        }
        final int type = args.getQuick(isBetween || args.getQuick(1).isUndefined() ? 0 : 1).getType();
        if (ColumnType.tagOf(type) != ColumnType.TIMESTAMP) {
            return;
        }
        for (int i = 0; i < count; i++) {
            if (args.getQuick(i).isUndefined()) {
                args.getQuick(i).assignType(type, sqlExecutionContext.getBindVariableService());
            }
        }
    }

    /**
     * Rejects an overload the call's place in the tree does not admit: an aggregate outside its aggregate root, a
     * non-window function at the window root, a window function outside a window. A window function directly under
     * the window root is reported where the window is constructed.
     */
    private void validateFactory(FunctionFactoryDescriptor overload, CallRole role, int position, ObjList<Function> args) throws SqlException {
        final BindScope scope = ctx.scope();
        final FunctionFactory factory = overload.getFactory();
        if (role != CallRole.AGGREGATE_ROOT && role != CallRole.WINDOW_ROOT && factory.isGroupBy()) {
            final SqlException exception = role != CallRole.ROOT
                    ? SqlException.$(position, "Aggregate function cannot be passed as an argument")
                    : scope.bindingRole == BindingRole.PREDICATE ? SqlException.$(position, "boolean expression expected")
                      : SqlException.$(position, "aggregate functions are not allowed in this context");
            Misc.freeObjList(args, exception);
            throw exception;
        }
        if (role == CallRole.WINDOW_ROOT && !factory.isWindow()) {
            final SqlException exception = SqlException.$(position, "non-window function called in window context");
            Misc.freeObjList(args, exception);
            throw exception;
        }
        if (role != CallRole.WINDOW_ROOT && role != CallRole.AGGREGATE_ROOT && factory.isWindow()) {
            if (role == CallRole.WINDOW_ARGUMENT) {
                if (scope.nestedWindowPosition < 0) {
                    scope.nestedWindowPosition = position;
                }
                return;
            }
            final SqlException exception = SqlException.emptyWindowContext(position);
            Misc.freeObjList(args, exception);
            throw exception;
        }
        if (role == CallRole.AGGREGATE_ROOT && !factory.isGroupBy() && scope.bindingRole != BindingRole.STANDALONE
                && !(scope.bindingRole == BindingRole.GROUP_BY_EXPRESSION && overload.isArrayElementWiseScalar())) {
            final IllegalStateException exception = new IllegalStateException("aggregate root is bound to a non-aggregate factory");
            Misc.freeObjList(args, exception);
            throw exception;
        }
    }

    private void validateKeySubquery() throws SqlException {
        if (!(arguments.getQuick(0) instanceof ColumnExpression column) || !ctx.scope().keySubqueryColumnIds.contains(column.getColumnId())
                || !(arguments.getQuick(1) instanceof CursorExpression cursor) || cursor.isBoolean()) {
            return;
        }
        final OutputSchema output = cursor.getPlan().getOutput();
        final int type = output.getColumnType(0);
        switch (ColumnType.tagOf(type)) {
            case ColumnType.STRING, ColumnType.SYMBOL, ColumnType.VARCHAR -> {
            }
            default -> throw SqlException.position(SqlBinder.getOutputColumnPosition(cursor.getPlan(), 0))
                    .put("unsupported column type: ")
                    .put(output.getColumnName(0))
                    .put(": ")
                    .put(ColumnType.nameOf(type));
        }
    }

    private void validateNode(ExpressionNode node) {
        if (node.type == ExpressionNode.QUERY) {
            return;
        }
        if (node.windowExpression != null && node != windowRoot() && ctx.scope().bindingRole != BindingRole.STANDALONE
                || node.type != ExpressionNode.LITERAL && node.type != ExpressionNode.CONSTANT
                && node.type != ExpressionNode.FUNCTION && node.type != ExpressionNode.OPERATION
                && node.type != ExpressionNode.BIND_VARIABLE && node.type != ExpressionNode.MEMBER_ACCESS
                && node.type != ExpressionNode.ARRAY_CONSTRUCTOR && node.type != ExpressionNode.ARRAY_ACCESS
                && (node.type != ExpressionNode.SET_OPERATION
                || !SqlKeywords.isInKeyword(node.token) && !SqlKeywords.isBetweenKeyword(node.token))) {
            throw new IllegalStateException("unexpected expression node type");
        }
    }

    /**
     * Validates, as the SUBSAMPLE window call binds, the arguments the SAMPLE BY binder leaves to it, see
     * {@link SampleByBinder#pendingSubsampleArgument}, and the LTTB gap threshold.
     */
    private void validateSubsampleArguments(ExpressionNode node, ObjList<Function> args, IntList positions) throws SqlException {
        if (node.windowExpression == null || !node.windowExpression.isSubsampleKeepFlag() || args == null) {
            return;
        }
        final ExpressionNode subsample = node.windowExpression.getPendingSubsample();
        final SampleByBinder.SubsampleMethod method = SampleByBinder.SubsampleMethod.of(subsample.token);
        final int pendingIndex = SampleByBinder.pendingSubsampleArgument(subsample, method);
        switch (method) {
            case UNIFORM -> {
                if (pendingIndex == 0) {
                    SubsampleValidator.validatePositionTargetOrThrow(args.getQuick(0), positions.getQuick(0), false);
                }
            }
            case CADENCE -> {
                if (pendingIndex == 0) {
                    SubsampleValidator.validatePositionTargetOrThrow(args.getQuick(0), positions.getQuick(0), true);
                }
                if (args.size() == 2 && pendingIndex >= 0) {
                    SubsampleValidator.validateCadenceSeedOrThrow(args.getQuick(1), positions.getQuick(1));
                }
            }
            case EXTREMA, LTTB -> {
                if (pendingIndex == 1) {
                    SubsampleValidator.validatePositionTargetOrThrow(args.getQuick(2), positions.getQuick(2), false);
                }
                if (method == SampleByBinder.SubsampleMethod.LTTB && subsample.paramCount == 3) {
                    SubsampleValidator.validateLttbGapOrThrow(subsample.args.getQuick(2));
                }
            }
            case SDT -> {
                if (pendingIndex == 1) {
                    SubsampleValidator.validateSdtCompdev(args.getQuick(2), positions.getQuick(2));
                }
            }
        }
    }

    /**
     * Raises, where the comparison binds, the error of a TIMESTAMP or DATE constant the precision of the temporal
     * operand cannot hold, which the comparison would otherwise raise on the first row it reads.
     */
    private void validateTemporalConstant(ObjList<Function> args, int index, TimestampDriver driver) throws SqlException {
        final Function constant = args.getQuick(index);
        final int type = constant.getType();
        if (ColumnType.tagOf(type) != ColumnType.TIMESTAMP && type != ColumnType.DATE) {
            return;
        }
        try {
            driver.ceilFrom(type == ColumnType.DATE ? constant.getDate(null) : constant.getTimestamp(null), type);
        } catch (ImplicitCastException e) {
            throw SqlException.$(arguments.getQuick(index).getPosition(), e.getFlyweightMessage());
        }
    }

    /**
     * Raises, where the call binds, the error of TIMESTAMP text that converts neither to a timestamp nor, where
     * the call reads intervals, to an interval: a SYMBOL constant compared with a TIMESTAMP, the string literal of a
     * single-interval TIMESTAMP IN parsed in its quoted spelling, as interval extraction parses it, so a bare
     * number is an invalid interval rather than an epoch, and a text element of a TIMESTAMP IN list, last to first.
     */
    private void validateTimestampText(FunctionFactoryDescriptor overload, CharSequence operator, ObjList<Function> args,
                                       SqlExecutionContext executionContext) throws SqlException {
        final int count = args == null ? 0 : args.size();
        if (count == 2 && FunctionResolver.isTemporalComparisonOperator(operator)) {
            for (int i = 0; i < 2; i++) {
                final Function symbol = args.getQuick(i);
                final int timestampType = args.getQuick(1 - i).getType();
                if (symbol.getType() == ColumnType.SYMBOL && symbol.isConstant() && ColumnType.tagOf(timestampType) == ColumnType.TIMESTAMP) {
                    final CharSequence text = literalText(arguments.getQuick(i));
                    try {
                        ColumnType.getTimestampDriver(timestampType).implicitCast(text, ColumnType.SYMBOL);
                    } catch (ImplicitCastException e) {
                        throw timestampTextError(operator, i, text, arguments.getQuick(i).getPosition());
                    }
                }
            }
            return;
        }
        if (!(overload.getFactory() instanceof InTimestampTimestampFunctionFactory) || count < 2
                || ColumnType.tagOf(args.getQuick(0).getType()) != ColumnType.TIMESTAMP) {
            return;
        }
        final TimestampDriver driver = ColumnType.getTimestampDriver(args.getQuick(0).getType());
        if (count == 2 && ColumnType.isVarcharOrString(args.getQuick(1).getType())) {
            final BoundExpression argument = arguments.getQuick(1);
            final CharSequence text = isStringLiteral(args, 1) ? literalText(argument) : null;
            if (text != null && !InTimestampTimestampFunctionFactory.containsDateVariable(text)) {
                final CharSequence seq = intervalSpelling(argument, text);
                parsedIntervals.clear();
                IntervalUtils.parseTickExpr(driver, executionContext.getCairoEngine().getConfiguration(), seq, 1, seq.length() - 1,
                        argument.getPosition(), parsedIntervals, IntervalOperation.INTERSECT, intervalSink, true);
                parsedIntervals.clear();
            }
            return;
        }
        for (int i = count - 1; i > 0; i--) {
            final Function element = args.getQuick(i);
            final BoundExpression argument = arguments.getQuick(i);
            final CharSequence text = element.isConstant() && isCaseText(element.getType()) ? literalText(argument) : null;
            if (text != null && !isTimestampText(driver, text)) {
                final CharSequence seq = intervalSpelling(argument, text);
                final int lo = seq == text ? 0 : 1;
                if (!InTimestampTimestampFunctionFactory.parseIntervalElement(driver, executionContext.getCairoEngine().getConfiguration(),
                        seq, lo, seq.length() - lo, parsedIntervals, intervalSink)) {
                    throw seq != text ? SqlException.invalidDate(argument.getPosition()) : SqlException.invalidDate(text, argument.getPosition());
                }
            }
        }
    }

    private ExpressionNode windowRoot() {
        final BindScope scope = ctx.scope();
        return scope.bindingRole == BindingRole.WINDOW ? scope.bindingRoot : null;
    }

    private ExpressionNode wrapTimestampColumn(ExpressionNode node, CharSequence unit, CharSequence stride, int timestampIndex) {
        if (node == null) {
            return null;
        }
        if (node.type != ExpressionNode.LITERAL) {
            wrapTimestampColumns(node, unit, stride, timestampIndex);
            return node;
        }
        final BindScope scope = ctx.scope();
        if (findColumn(node, scope.expressionInput, scope.expressionInputAlias) != timestampIndex) {
            return node;
        }
        final ExpressionNode dateadd = ctx.planNodes.syntheticNodes.next().of(ExpressionNode.FUNCTION, "dateadd", 0, node.position);
        dateadd.paramCount = 3;
        dateadd.args.add(node);
        dateadd.args.add(ctx.planNodes.syntheticNodes.next().of(ExpressionNode.CONSTANT, stride, 0, node.position));
        dateadd.args.add(ctx.planNodes.syntheticNodes.next().of(ExpressionNode.CONSTANT, unit, 0, node.position));
        scope.projectedOffsets.add(dateadd);
        return dateadd;
    }

    private void wrapTimestampColumns(ExpressionNode node, CharSequence unit, CharSequence stride, int timestampIndex) {
        node.lhs = wrapTimestampColumn(node.lhs, unit, stride, timestampIndex);
        node.rhs = wrapTimestampColumn(node.rhs, unit, stride, timestampIndex);
        for (int i = 0, n = node.args.size(); i < n; i++) {
            node.args.setQuick(i, wrapTimestampColumn(node.args.getQuick(i), unit, stride, timestampIndex));
        }
    }

    static int callFlags(Function function, ObjList<BoundExpression> arguments) {
        int flags = BoundExpression.functionFlags(function);
        for (int i = 0, n = arguments.size(); i < n; i++) {
            flags &= arguments.getQuick(i).getFunctionFlags() | ~BoundExpression.STABLE_WITHIN_EXECUTION;
        }
        return flags;
    }

    /**
     * The index {@link #findColumn} found for the column the node names, or the error of a column it did not find.
     */
    static int checkColumn(ExpressionNode node, int index, OutputSchema input, CharSequence inputAlias) throws SqlException {
        if (index == OutputSchema.COLUMN_AMBIGUOUS) {
            throw SqlException.ambiguousColumn(node.position, node.token);
        }
        if (index < 0) {
            final int dot = qualifierEnd(node, input);
            if (dot > -1 && isUnknownQualifier(GenericLexer.unquote(node.token.subSequence(0, dot)), input, inputAlias)) {
                throw SqlException.$(node.position, "Invalid table name or alias");
            }
            throw SqlException.invalidColumn(node.position, node.token);
        }
        return index;
    }

    /**
     * The index of the input column the literal names, or -1. A quoted name names the column it spells out in full
     * when the input has one, whatever dots it holds; otherwise, as an unquoted name, its last dot separates a table
     * qualifier from the column name.
     */
    static int findColumn(ExpressionNode node, OutputSchema input, CharSequence inputAlias) {
        final CharSequence name = node.token;
        final int dot = qualifierEnd(node, input);
        if (dot < 0) {
            return getColumnIndexQuiet(input, null, name, 0, name.length());
        }
        final CharSequence qualifier = GenericLexer.unquote(name.subSequence(0, dot));
        if (input.hasColumnQualifiers()) {
            return getColumnIndexQuiet(input, qualifier, name, dot + 1, name.length());
        }
        return Chars.equalsIgnoreCaseNc(qualifier, inputAlias) ? getColumnIndexQuiet(input, null, name, dot + 1, name.length()) : -1;
    }

    static boolean isUnknownQualifier(CharSequence qualifier, OutputSchema input, CharSequence inputAlias) {
        if (input.hasColumnQualifiers()) {
            return !input.hasColumnQualifier(qualifier);
        }
        return !Chars.equalsIgnoreCaseNc(qualifier, inputAlias);
    }

    /**
     * The index of the dot that separates a literal's table qualifier from its column name, or -1 when the literal has
     * none. A quoted name has none, whatever dots it holds, where it names a column of {@code columns} in full, as
     * {@link #findColumn} resolves it, or where {@code columns} is null, which a caller passes when it matches the name
     * against output aliases. Otherwise the last dot outside quotes separates the qualifier.
     */
    static int qualifierEnd(ExpressionNode node, @Nullable OutputSchema columns) {
        final CharSequence name = node.token;
        final int dot = Chars.indexOfLastUnquoted(name, '.');
        return dot >= 0 && node.isQuoted && (columns == null || getColumnIndexQuiet(columns, null, name, 0, name.length()) >= 0) ? -1 : dot;
    }

    static int resolveColumn(ExpressionNode node, OutputSchema input, CharSequence inputAlias) throws SqlException {
        return checkColumn(node, findColumn(node, input, inputAlias), input, inputAlias);
    }

    /**
     * Binds an expression a clause evaluates once, on its own, see {@link BindingRole#STANDALONE}.
     */
    BoundExpression bindStandalone(ExpressionNode node, SqlExecutionContext executionContext) throws SqlException {
        return bind(node, BindingRole.STANDALONE, ctx.emptySchema, null, ColumnType.UNDEFINED, null, null, null, executionContext);
    }

    ConstantExpression constant(Function function, int position) {
        return switch (ColumnType.tagOf(function.getType())) {
            case ColumnType.TIMESTAMP ->
                    ctx.planNodes.constants.next().ofTimestamp(function.getTimestamp(null), function.getType(), position);
            case ColumnType.STRING ->
                    ctx.planNodes.constants.next().ofString(Chars.toString(function.getStrA(null)), position);
            case ColumnType.SYMBOL ->
                    ctx.planNodes.constants.next().ofSymbol(Chars.toString(function.getSymbol(null)), position);
            case ColumnType.VARCHAR -> ctx.planNodes.constants.next().ofVarchar(function.getVarcharA(null), position);
            case ColumnType.BYTE -> ctx.planNodes.constants.next().ofByte(function.getByte(null), position);
            case ColumnType.SHORT -> ctx.planNodes.constants.next().ofShort(function.getShort(null), position);
            case ColumnType.DATE -> ctx.planNodes.constants.next().ofDate(function.getDate(null), position);
            case ColumnType.IPv4 -> ctx.planNodes.constants.next().ofIPv4(function.getIPv4(null), position);
            case ColumnType.CHAR -> ctx.planNodes.constants.next().ofChar(function.getChar(null), position);
            case ColumnType.BOOLEAN -> ctx.planNodes.constants.next().ofBoolean(function.getBool(null), position);
            case ColumnType.INT -> ctx.planNodes.constants.next().ofInt(function.getInt(null), position);
            case ColumnType.LONG -> ctx.planNodes.constants.next().ofLong(function.getLong(null), position);
            case ColumnType.LONG256 -> ctx.planNodes.constants.next().ofLong256(function.getLong256A(null), position);
            case ColumnType.UUID ->
                    ctx.planNodes.constants.next().ofUuid(function.getLong128Lo(null), function.getLong128Hi(null), position);
            case ColumnType.LONG128 ->
                    ctx.planNodes.constants.next().ofLong128(function.getLong128Lo(null), function.getLong128Hi(null), position);
            case ColumnType.GEOBYTE, ColumnType.GEOSHORT, ColumnType.GEOINT, ColumnType.GEOLONG ->
                    ctx.planNodes.constants.next().ofGeoHash(GeoHashes.getGeoLong(function.getType(), function, null), function.getType(), position);
            case ColumnType.FLOAT -> ctx.planNodes.constants.next().ofFloat(function.getFloat(null), position);
            case ColumnType.DOUBLE -> ctx.planNodes.constants.next().ofDouble(function.getDouble(null), position);
            case ColumnType.NULL -> ctx.planNodes.constants.next().ofNull(position);
            case ColumnType.DECIMAL8 ->
                    ctx.planNodes.constants.next().ofDecimal(function.getType(), 0, 0, 0, function.getDecimal8(null), position);
            case ColumnType.DECIMAL16 ->
                    ctx.planNodes.constants.next().ofDecimal(function.getType(), 0, 0, 0, function.getDecimal16(null), position);
            case ColumnType.DECIMAL32 ->
                    ctx.planNodes.constants.next().ofDecimal(function.getType(), 0, 0, 0, function.getDecimal32(null), position);
            case ColumnType.DECIMAL64 ->
                    ctx.planNodes.constants.next().ofDecimal(function.getType(), 0, 0, 0, function.getDecimal64(null), position);
            case ColumnType.DECIMAL128 -> {
                function.getDecimal128(null, decimal128);
                yield ctx.planNodes.constants.next().ofDecimal(function.getType(), 0, 0, decimal128.getHigh(), decimal128.getLow(), position);
            }
            case ColumnType.DECIMAL256 -> {
                function.getDecimal256(null, decimal256);
                yield ctx.planNodes.constants.next().ofDecimal(function.getType(), decimal256.getHh(), decimal256.getHl(),
                        decimal256.getLh(), decimal256.getLl(), position);
            }
            case ColumnType.INTERVAL -> {
                final Interval interval = function.getInterval(null);
                yield ctx.planNodes.constants.next().ofInterval(interval.getLo(), interval.getHi(), function.getType(), position);
            }
            case ColumnType.BINARY -> {
                if (function.getBin(null) != null) {
                    throw new IllegalStateException("non-null BINARY constant");
                }
                yield ctx.planNodes.constants.next().ofBinaryNull(position);
            }
            default -> throw new IllegalStateException("unexpected constant type");
        };
    }

    /**
     * True when the name does not resolve in the input but does in an enclosing lateral scope.
     */
    boolean isOuterColumn(ExpressionNode node, OutputSchema input, CharSequence inputAlias) {
        return node.type == ExpressionNode.LITERAL && findColumn(node, input, inputAlias) == -1 && isOuterScopeColumn(node);
    }

    /**
     * True when the name resolves in an enclosing lateral scope.
     */
    boolean isOuterScopeColumn(ExpressionNode node) {
        final ObjList<OutputSchema> outerScopes = ctx.scope().outerScopes;
        for (int i = outerScopes.size() - 1; i > -1; i--) {
            if (findColumn(node, outerScopes.getQuick(i), null) > -1) {
                return true;
            }
        }
        return false;
    }

    BoundExpression toBooleanSubquery(BoundExpression expression) {
        if (expression instanceof CursorExpression cursor && ColumnType.tagOf(cursor.getPlan().getOutput().getColumnCount() == 1
                ? cursor.getPlan().getOutput().getColumnType(0) : ColumnType.UNDEFINED) == ColumnType.BOOLEAN) {
            return nextCursor().ofBoolean(cursor, BoundExpression.RUNTIME_CONSTANT | cursor.getFunctionFlags()
                    & (BoundExpression.STABLE_WITHIN_EXECUTION | BoundExpression.NON_DETERMINISTIC));
        }
        return expression;
    }

    void validateSampleByFill(
            FunctionExpression expression,
            CharSequence fillToken,
            int fillPosition,
            ExpressionNode aggregateNode
    ) throws SqlException {
        if (!(ctx.preparedRoot(expression) instanceof GroupByFunction function)) {
            throw new IllegalStateException("aggregate preparation is not a group-by function");
        }
        final CharSequence unsupportedFill = GroupByUtils.getUnsupportedSampleByFill(function, fillToken);
        if (unsupportedFill != null) {
            throw SqlException.$(fillPosition, "support for ").put(unsupportedFill)
                    .put(" fill is not yet implemented [function=").put(aggregateNode)
                    .put(", class=").put(function.getClass().getName()).put(']');
        }
    }

    /**
     * The place of the root a binding binds in its statement, which decides the rules of its calls.
     */
    enum BindingRole {
        AGGREGATE,
        GROUP_BY_EXPRESSION,
        PREDICATE,
        SCALAR,
        /**
         * An expression a clause evaluates once, on its own and over no columns, such as a frame bound or a SUBSAMPLE
         * argument: every name is an unknown column, an aggregate call binds as the aggregate it is, which no constant
         * is, and a window call, its OVER clause aside, binds as a call outside any window.
         */
        STANDALONE,
        UPDATE_ASSIGNMENT,
        WINDOW
    }

    /**
     * The place of a call in the expression tree a binding binds, which decides the factories it admits.
     */
    private enum CallRole {
        AGGREGATE_ROOT,
        ARGUMENT,
        PROJECTED_OFFSET,
        ROOT,
        WINDOW_ARGUMENT,
        WINDOW_ROOT
    }
}
