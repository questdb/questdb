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
import io.questdb.griffin.SubqueryCompiler;
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
import io.questdb.std.str.StringSink;
import org.jetbrains.annotations.TestOnly;

/**
 * Binds SQL expressions: walks each expression in post-order, resolves and builds every call through the
 * {@link FunctionResolver}, produces {@link BoundExpression} descriptions and hands each built root to
 * {@link PreparedFunctions}.
 */
public final class FunctionBinder implements CallBinder, PostOrderTreeTraversalAlgo.Visitor, Mutable {
    private static final int CALL_AGGREGATE_ROOT = 3;
    private static final int CALL_ARGUMENT = 0;
    private static final int CALL_ROOT = 2;
    private static final int CALL_WINDOW_ARGUMENT = 1;
    private static final int CALL_WINDOW_ROOT = 4;
    private final IntList argumentLeafMarks = new IntList();
    private final IntList argumentPositions = new IntList();
    private final IntList argumentTypes = new IntList();
    private final ObjList<BoundExpression> arguments = new ObjList<>();
    private final ObjList<Function> callArguments = new ObjList<>();
    private final IntList callPositions = new IntList();
    private final BindContext ctx;
    private final Decimal128 decimal128 = new Decimal128();
    private final Decimal256 decimal256 = new Decimal256();
    private final IntList expressionLeafMarks = new IntList();
    private final ObjList<BoundExpression> expressionStack = new ObjList<>();
    private final ObjList<Function> functionStack = new ObjList<>();
    private final StringSink intervalSink = new StringSink();
    private final StringSink intervalText = new StringSink();
    private final LongList parsedIntervals = new LongList();
    private final IntList positionStack = new IntList();
    private final FunctionResolver resolver;
    private final SubqueryCompiler subqueryCompiler;
    private final PostOrderTreeTraversalAlgo traverseAlgo = new PostOrderTreeTraversalAlgo();
    private final ObjList<BoundExpression> unconstructed = new ObjList<>();
    private SqlExecutionContext sqlExecutionContext;

    /**
     * Allocates descriptions from the context's pools and hands built roots to its prepared functions.
     */
    FunctionBinder(BindContext ctx, FunctionResolver resolver, SubqueryCompiler subqueryCompiler) {
        this.ctx = ctx;
        this.resolver = resolver;
        this.subqueryCompiler = subqueryCompiler;
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
        return bind(node, null, 0, null, input, inputAlias, preferredType, false, executionContext);
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
            ObjList<ColumnExpression> replacementColumns,
            SqlExecutionContext executionContext
    ) throws SqlException {
        return bind(node, input, inputAlias, ColumnType.UNDEFINED, replacementNodes, replacementColumns, executionContext);
    }

    public BoundExpression bind(
            ExpressionNode node,
            OutputSchema input,
            CharSequence inputAlias,
            int preferredType,
            ObjList<ExpressionNode> replacementNodes,
            ObjList<ColumnExpression> replacementColumns,
            SqlExecutionContext executionContext
    ) throws SqlException {
        final BindScope scope = ctx.scope();
        assert scope.replacementNodes == null && replacementNodes.size() == replacementColumns.size();
        scope.replacementNodes = replacementNodes;
        scope.replacementColumns = replacementColumns;
        try {
            return bind(node, input, inputAlias, preferredType, executionContext);
        } finally {
            scope.replacementNodes = null;
            scope.replacementColumns = null;
        }
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
        assert node.type == ExpressionNode.FUNCTION && node.windowExpression == null
                && resolver.getFunctionFactoryCache().isGroupBy(node.token);
        return (FunctionExpression) bindAggregateRoot(node, input, inputAlias, executionContext);
    }

    @Override
    public BoundExpression bindCall(
            CharSequence name,
            int position,
            ObjList<? extends BoundExpression> args,
            OutputSchema input,
            SqlExecutionContext executionContext
    ) throws SqlException {
        return bind(null, name, position, args, input, null, ColumnType.UNDEFINED, false, executionContext);
    }

    public BoundExpression bindGroupByExpression(
            ExpressionNode node,
            OutputSchema input,
            CharSequence inputAlias,
            SqlExecutionContext executionContext
    ) throws SqlException {
        final BindScope scope = ctx.scope();
        assert !scope.isBindingGroupByExpression;
        assert node.type == ExpressionNode.FUNCTION && node.windowExpression == null
                && resolver.getFunctionFactoryCache().isGroupBy(node.token);
        scope.isBindingGroupByExpression = true;
        try {
            return bindAggregateRoot(node, input, inputAlias, executionContext);
        } finally {
            scope.isBindingGroupByExpression = false;
        }
    }

    public BoundExpression bindGroupByExpression(
            ExpressionNode node,
            OutputSchema input,
            ObjList<ExpressionNode> replacementNodes,
            ObjList<ColumnExpression> replacementColumns,
            SqlExecutionContext executionContext
    ) throws SqlException {
        final BindScope scope = ctx.scope();
        assert scope.replacementNodes == null && replacementNodes.size() == replacementColumns.size();
        scope.replacementNodes = replacementNodes;
        scope.replacementColumns = replacementColumns;
        try {
            return bindGroupByExpression(node, input, null, executionContext);
        } finally {
            scope.replacementNodes = null;
            scope.replacementColumns = null;
        }
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
        final BindScope scope = ctx.scope();
        assert !scope.isBindingPredicate;
        scope.isBindingPredicate = true;
        scope.nativeTimestampIds = nativeTimestampIds;
        try {
            return bind(node, input, inputAlias, preferredRootType, executionContext);
        } finally {
            scope.isBindingPredicate = false;
            scope.nativeTimestampIds = null;
        }
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
        return bind(node, null, 0, null, input, inputAlias, targetType, true, executionContext);
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
        final BindScope scope = ctx.scope();
        assert scope.windowRoot == null && scope.aggregateRoot == null && !scope.isBindingPredicate;
        scope.windowRoot = node;
        scope.nestedWindowPosition = -1;
        try {
            return (FunctionExpression) bind(node, input, inputAlias, executionContext);
        } finally {
            scope.windowRoot = null;
        }
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
        callArguments.clear();
        callPositions.clear();
        unconstructed.clear();
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

    @Override
    public void visit(ExpressionNode node) throws SqlException {
        final int count = node.paramCount;
        final Function function;
        if (count == 0) {
            function = switch (node.type) {
                case ExpressionNode.LITERAL ->
                        isUnresolvedNoArgFunction(node) ? resolveCall(node, 0) : createColumn(node);
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

    private static boolean isNotEqualsOperator(CharSequence operator) {
        return Chars.equals(operator, "!=") || Chars.equals(operator, "<>");
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
            ColumnType.getTimestampDriver(IntervalUtils.literalTimestampType(driver, text)).parseFloorLiteral(text);
            return true;
        } catch (NumericException e) {
            return false;
        }
    }

    private static CharSequence literalText(BoundExpression expression) {
        if (!(expression instanceof ConstantExpression constant)) {
            return null;
        }
        return switch (ColumnType.tagOf(constant.getDataType())) {
            case ColumnType.STRING, ColumnType.SYMBOL -> constant.getStrValue();
            case ColumnType.VARCHAR ->
                    constant.getVarcharValue() == null ? null : constant.getVarcharValue().asAsciiCharSequence();
            default -> null;
        };
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
        if (!isSwitch && !overload.isCase()) {
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

    /**
     * Binds the expression tree of the node, or, without a node, the named call over already-bound arguments.
     */
    private BoundExpression bind(
            ExpressionNode node,
            CharSequence callName,
            int callPosition,
            ObjList<? extends BoundExpression> callArguments,
            OutputSchema input,
            CharSequence inputAlias,
            int preferredType,
            boolean isUpdateAssignment,
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
        final ExpressionNode originalAggregateRoot = scope.aggregateRoot;
        final ExpressionNode originalWindowRoot = scope.windowRoot;
        final int syntheticNodeMark = ctx.planNodes.syntheticNodes.getPos();
        final int staticTypeMark = ctx.planNodes.staticTypes.getPos();
        try {
            if (node == null) {
                bindCallArguments(callName, callPosition, callArguments);
            } else {
                if (isAstRewritable()) {
                    // DECLARE references may share parser nodes across occurrences.
                    // Reassociation belongs to this binding, never to that shared AST.
                    node = ExpressionNode.deepClone(ctx.planNodes.syntheticNodes, node);
                    if (originalAggregateRoot != null) {
                        scope.aggregateRoot = node;
                    }
                    if (originalWindowRoot != null) {
                        scope.windowRoot = node;
                    }
                }
                if (scope.isBindingPredicate) {
                    if (isAstRewritable()) {
                        rewriteAndOffsets(node);
                    }
                    bindTimestampBetweenLowerBound(node, executionContext);
                }
                scope.bindingRoot = node;
                if (isAstRewritable()) {
                    // Caller-selected group keys refer to exact subtrees. Reassociating
                    // their ancestors here could move children across that boundary.
                    node.reassociateConstants(resolver.getConfiguration().getCairoSqlLegacyOperatorPrecedence());
                }
                traverseAlgo.traverse(node, this);
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
            entry.expression = expressionStack.getQuick(expressionMark);
            assert PreparedFunctions.hasOnlyReadLeaves(entry) : "bound leaf is not read by its description";
            return entry.expression;
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
            scope.aggregateRoot = originalAggregateRoot;
            scope.windowRoot = originalWindowRoot;
            scope.bindingRoot = null;
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
            unconstructed.setPos(unconstructedMark);
            ctx.planNodes.staticTypes.rewind(staticTypeMark);
        }
    }

    private BoundExpression bindAggregateRoot(
            ExpressionNode node,
            OutputSchema input,
            CharSequence inputAlias,
            SqlExecutionContext executionContext
    ) throws SqlException {
        final BindScope scope = ctx.scope();
        assert scope.aggregateRoot == null && !scope.isBindingPredicate;
        scope.aggregateRoot = node;
        try {
            return bind(node, input, inputAlias, executionContext);
        } finally {
            scope.aggregateRoot = null;
        }
    }

    /**
     * Pushes the bound arguments as the operands of the named call, last argument first as a post-order walk visits
     * them, then resolves the call over them.
     */
    private void bindCallArguments(CharSequence name, int position, ObjList<? extends BoundExpression> args) throws SqlException {
        for (int i = args.size() - 1; i >= 0; i--) {
            final BoundExpression argument = args.getQuick(i);
            pushFunction(pushReplacement(argument, argument.getPosition()), argument.getPosition());
        }
        pushFunction(resolveCall(name, position, isGroupBy(name) ? CALL_AGGREGATE_ROOT : CALL_ROOT, false, args.size()), position);
    }

    /**
     * Timestamp interval analysis binds a sub-query BETWEEN bound pair low bound first.
     */
    private void bindTimestampBetweenLowerBound(ExpressionNode node, SqlExecutionContext executionContext) throws SqlException {
        final ExpressionNode lo = findTimestampBetweenLowerBound(node);
        if (lo != null) {
            final BindScope scope = ctx.scope();
            scope.boundLowerBound = subqueryCompiler.bindSubquery(lo.queryModel, lo.position, executionContext);
            scope.boundLowerBoundNode = lo;
        }
    }

    /**
     * The leaf mark of the call whose arguments are being bound: its leaves follow it in the preparation.
     */
    private int callLeafMark() {
        final int count = argumentLeafMarks.size();
        return count > 0 ? argumentLeafMarks.getQuick(count - 1) : leafMark();
    }

    private int callRole(ExpressionNode node) {
        final BindScope scope = ctx.scope();
        if (node == scope.windowRoot) {
            return CALL_WINDOW_ROOT;
        }
        if (node == scope.aggregateRoot) {
            return CALL_AGGREGATE_ROOT;
        }
        if (node == scope.bindingRoot) {
            return CALL_ROOT;
        }
        return isWindowArgument(node) ? CALL_WINDOW_ARGUMENT : CALL_ARGUMENT;
    }

    private Function canonicalizeTemporalBetween(ObjList<Function> args) {
        final Function operand = args.getQuick(0);
        if (!isTemporalOperand(operand)) {
            return null;
        }
        final int operandType = operand.getType();
        final TimestampDriver driver = temporalDriver(operandType);
        final int loType = temporalConstantType(args, 1, driver);
        final int hiType = temporalConstantType(args, 2, driver);
        try {
            if (loType != ColumnType.UNDEFINED && hiType != ColumnType.UNDEFINED) {
                if (isOperandTyped(args, 1, operandType) && isOperandTyped(args, 2, operandType)) {
                    return null;
                }
                final long lo = temporalConstantValue(args, 1, loType);
                final long hi = temporalConstantValue(args, 2, hiType);
                if (lo == Numbers.LONG_NULL || hi == Numbers.LONG_NULL) {
                    return null;
                }
                final long low = Math.min(driver.ceilFrom(lo, loType), driver.ceilFrom(hi, hiType));
                final long high = Math.max(driver.floorFrom(lo, loType), driver.floorFrom(hi, hiType));
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

    private Function canonicalizeTemporalBinary(CharSequence operator, ObjList<Function> args) {
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
        try {
            final long value = temporalConstantValue(args, index, type);
            if (value == Numbers.LONG_NULL) {
                return null;
            }
            final long ceil = driver.ceilFrom(value, type);
            final long floor = driver.floorFrom(value, type);
            final boolean isExact = ceil == floor;
            if (Chars.equals(operator, '=') || isNotEqualsOperator(operator)) {
                if (!isExact) {
                    return Chars.equals(operator, '=') ? BooleanConstant.FALSE : BooleanConstant.TRUE;
                }
                replaceTemporalConstant(args, index, ceil, operandType, true);
            } else {
                final boolean isCeil = (Chars.equals(operator, '<') || Chars.equals(operator, ">=")) == (operandIndex == 0);
                replaceTemporalConstant(args, index, isCeil ? ceil : floor, operandType, isExact);
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
     * IN element it cannot hold drops out. Returns the boolean the comparison folds to, or null.
     */
    private Function canonicalizeTemporalComparison(CharSequence operator, ObjList<Function> args, IntList positions) {
        final int count = args == null ? 0 : args.size();
        if (count == 2 && isTemporalComparisonOperator(operator)) {
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

    private Function canonicalizeTemporalIn(ObjList<Function> args, IntList positions) {
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
            try {
                final long value = temporalConstantValue(args, i, type);
                final long ceil = driver.ceilFrom(value, type);
                if (value == Numbers.LONG_NULL || ceil == driver.floorFrom(value, type)) {
                    replaceTemporalConstant(args, i, ceil, operandType, true);
                    isMatchable = true;
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
                    if (unconstructed.indexOf(arguments.getQuick(index)) >= 0) {
                        unconstructed.add(conversion);
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
            if (unconstructed.indexOf(argument) >= 0) {
                final BindScope scope = ctx.scope();
                final Function placeholder = args.getQuick(i);
                args.setQuick(i, ctx.functionInstantiator.realize(argument, scope.expressionInput, scope.currentPreparation, executionContext));
                Misc.free(placeholder);
            }
        }
    }

    /**
     * Converts the constant text a temporal comparison or BETWEEN compares with a TIMESTAMP before the resolver
     * coerces the arguments, so text that is not a timestamp reports the comparison's error
     * ({@link #timestampTextError}) rather than the resolver's.
     */
    private void convertTimestampText(FunctionFactoryDescriptor overload, CharSequence operator, ObjList<Function> args, IntList positions) throws SqlException {
        final int count = args == null ? 0 : args.size();
        if (!(count == 2 && isTemporalComparisonOperator(operator) || count == 3 && SqlKeywords.isBetweenKeyword(operator))) {
            return;
        }
        for (int i = 0, n = Math.min(count, overload.getSigArgCount()); i < n; i++) {
            final int sigArgType = overload.getArgTypeWithFlags(i);
            final Function arg = args.getQuick(i);
            if (FunctionFactoryDescriptor.toTypeTag(sigArgType) == ColumnType.TIMESTAMP && arg.isConstant() && isCaseText(ColumnType.tagOf(arg.getType()))) {
                final CharSequence text = arg.getStrA(null);
                try {
                    args.set(i, FunctionResolver.timestampConstant(text, sigArgType));
                } catch (NumericException e) {
                    final SqlException exception = timestampTextError(operator, i, text, positions.getQuick(i));
                    Misc.freeObjList(args, exception);
                    throw exception;
                }
            }
        }
    }

    private Function createColumn(ExpressionNode node) throws SqlException {
        final BindScope scope = ctx.scope();
        final OutputSchema input = scope.expressionInput;
        final CharSequence inputAlias = scope.expressionInputAlias;
        if (findColumn(node, input, inputAlias) == -1) {
            final Function outer = createOuterColumn(node);
            if (outer != null) {
                return outer;
            }
        }
        final int index = resolveColumn(node, input, inputAlias);
        final int type = input.getColumnType(index);
        final int columnId = input.getColumnId(index);
        final int mark = leafMark();
        final Function leaf;
        if (BindableColumn.isBindableType(type)) {
            final BindableColumn bindable = BindableColumn.newInstance(columnId, type, input.isSymbolTableStatic(index));
            scope.currentPreparation.leaves.add(bindable);
            leaf = bindable;
        } else {
            leaf = FunctionInstantiator.createColumnFunction(node.position, index, type, input);
            scope.currentPreparation.isRebuildRequired = true;
        }
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
            subquery = subqueryCompiler.bindSubquery(node.queryModel, node.position, executionContext);
        }
        scope.currentPreparation.isRebuildRequired = true;
        final CursorExpression cursor = nextCursor().of(subquery, node.position);
        final Function function = new CursorFunction(subquery.getOutputMetadata());
        if (scope.timestampSubqueries.indexOf(node.queryModel) >= 0 && isTimestampConvertible(subquery.getRoot().getOutput(), scope.timestampSubqueryType)) {
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
            int role,
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
            unconstructed.add(pushCall(name, isSetOperation, ctx.planNodes.functions.next().of(overload, arguments, argumentPositions,
                    staticType, flags, position)));
            return ctx.planNodes.staticTypes.next().of(staticType, (flags & BoundExpression.NO_PARALLELISM) == 0);
        }
        final Function first = args != null && args.size() == 2 ? args.getQuick(0) : null;
        final Function second = first != null ? args.getQuick(1) : null;
        final Function function = resolver.createFunction(overload, position, name, args, positions, sqlExecutionContext);
        try {
            if (role == CALL_WINDOW_ROOT) {
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
                expression = ctx.planNodes.functions.next().of(overload, arguments, argumentPositions,
                        function.getType(), callFlags(function, arguments), position);
            }
            pushCall(name, isSetOperation, expression);
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
                return BindableColumn.isBindableType(type) ? BindableColumn.newInstance(columnId, type, outer.isSymbolTableStatic(index))
                        : FunctionInstantiator.createColumnFunction(node.position, index, type, outer);
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
            final ObjList<BindableColumn> leaves = ctx.scope().currentPreparation.leaves;
            for (int i = from; i < to; i++) {
                final BindableColumn leaf = leaves.getQuick(i);
                if (leaf.isOpen()) {
                    leaf.close();
                }
            }
            leaves.remove(from, to - 1);
        }
    }

    /**
     * The sub-query low bound of the first conjunct that compares a native timestamp column BETWEEN two sub-queries,
     * conjuncts left to right, or null.
     */
    private ExpressionNode findTimestampBetweenLowerBound(ExpressionNode node) {
        if (node != null && node.token != null && SqlKeywords.isAndKeyword(node.token)) {
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
        final Function argument = function != null && function.isConstant() && function.extendedOps() == null
                && !(function instanceof TypeConstant) ? resolver.functionToConstant(function, position) : function;
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
        final Function root = function != null && function.isConstant() && function.extendedOps() == null
                ? resolver.functionToConstant(function, position) : function;
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

    private boolean isUnresolvedNoArgFunction(ExpressionNode node) {
        final BindScope scope = ctx.scope();
        return findColumn(node, scope.expressionInput, scope.expressionInputAlias) == -1 && resolver.findNoArgFunction(node.token);
    }

    private boolean isWindowArgument(ExpressionNode node) {
        final ExpressionNode windowRoot = ctx.scope().windowRoot;
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
                if (original.getDataType() == ColumnType.STRING || original.getDataType() == ColumnType.SYMBOL) {
                    normalized.withTimestampText(original.getStrValue());
                } else if (original.getDataType() == ColumnType.VARCHAR && original.getVarcharValue() != null) {
                    normalized.withTimestampText(original.getVarcharValue().asAsciiCharSequence());
                }
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
     * it until the generator adopts it for the call's description, so generation does not build it again. The caller
     * frees the remaining arguments; on failure this frees every argument it has not handed over.
     */
    private void prepareUnconstructedArguments(ObjList<Function> args) {
        try {
            for (int i = 0, n = args.size(); i < n; i++) {
                final Function arg = args.getQuick(i);
                final BoundExpression argument = arguments.getQuick(i);
                if (arg instanceof ConstantFunction && !(arg instanceof TypeConstant) && argument instanceof ConstantExpression constant
                        && constant.getDataType() == arg.getType()) {
                    final PreparedFunctions.Entry entry = ctx.preparedFunctions.begin();
                    entry.expression = constant;
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
                        entry.expression = column;
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

    private BoundExpression pushCall(CharSequence name, boolean isSetOperation, BoundExpression expression) {
        if (isSetOperation && expression instanceof FunctionExpression call && SqlKeywords.isInKeyword(name)) {
            call.markSetOperation();
        }
        final BoundExpression pushed = name == "dateadd" && expression instanceof FunctionExpression call
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
            if (replacement instanceof FunctionExpression || replacement instanceof CursorExpression) {
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
        final Function leaf;
        if (BindableColumn.isBindableType(column.getDataType())) {
            final BindableColumn bindable = BindableColumn.newInstance(column.getColumnId(), column.getDataType(), input.isSymbolTableStatic(index));
            scope.currentPreparation.leaves.add(bindable);
            leaf = bindable;
        } else {
            leaf = FunctionInstantiator.createColumnFunction(position, index, column.getDataType(), input);
            scope.currentPreparation.isRebuildRequired = true;
        }
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
        final long value = temporalConstantValue(args, index, type);
        final long ceil = driver.ceilFrom(value, type);
        if (value != Numbers.LONG_NULL && ceil == driver.floorFrom(value, type)) {
            replaceTemporalConstant(args, index, ceil, operandType, true);
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
    private Function resolveCall(CharSequence name, int position, int role, boolean isSetOperation, int count) throws SqlException {
        beginArguments(count);
        // A float literal's spelling selects its exact DECIMAL cast, as in SQL text.
        final CharSequence literal = count == 2 && arguments.getQuick(0) instanceof ConstantExpression constant ? constant.getLiteralText() : null;
        final int literalPosition = literal != null ? arguments.getQuick(0).getPosition() : 0;
        ObjList<Function> args = null;
        IntList positions = null;
        if (count > 0) {
            args = callArguments;
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
        }
        FunctionFactoryDescriptor overload;
        do {
            if (role == CALL_WINDOW_ROOT) {
                try {
                    validateSubsampleArguments(ctx.scope().windowRoot, args, positions);
                } catch (Throwable th) {
                    Misc.freeObjList(args, th);
                    throw th;
                }
            }
            final Function cast = resolver.resolveCast(name, args, literal, literalPosition, sqlExecutionContext);
            if (cast != null) {
                return cast == args.getQuick(0) ? returnCastArgument(cast, args, position) : captureConstant(cast, position, null);
            }
            overload = resolver.selectOverload(name, position, args, positions, sqlExecutionContext);
        } while (overload == null);
        validateFactory(overload, role, position, args);
        convertTimestampText(overload, name, args, positions);
        resolver.coerceArguments(overload, position, name, args, positions, sqlExecutionContext);
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
                default -> constant(function, position);
            };
            if (unconstructed.indexOf(argument) >= 0) {
                unconstructed.add(repositioned);
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
    private int staticResultType(FunctionFactoryDescriptor overload, int role, int count) {
        final FunctionFactory factory = overload.getFactory();
        if (count == 0 || !overload.isRelocatableScalar() || overload.isCase() || overload.isSwitch()
                || factory.isGroupBy() || factory.isWindow() || factory.isCursor() || role == CALL_WINDOW_ROOT || role == CALL_AGGREGATE_ROOT) {
            return ColumnType.UNDEFINED;
        }
        boolean hasVariableArgument = false;
        argumentTypes.clear();
        for (int i = 0; i < count; i++) {
            final BoundExpression argument = arguments.getQuick(i);
            final int type = argument.getDataType();
            if (!(argument instanceof TypeExpression) && !(argument instanceof ConstantExpression)) {
                if (ColumnType.isArray(type) || (argument.getFunctionFlags() & ~BoundExpression.NO_PARALLELISM) != BoundExpression.STABLE_WITHIN_EXECUTION
                        || !(argument instanceof ColumnExpression && BindableColumn.isBindableType(type) || unconstructed.indexOf(argument) >= 0)) {
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
        return text != null ? IntervalUtils.literalTimestampType(driver, text) : type;
    }

    private long temporalConstantValue(ObjList<Function> args, int index, int type) throws NumericException {
        final CharSequence text = literalText(arguments.getQuick(index));
        if (text != null) {
            return ColumnType.getTimestampDriver(type).parseFloorLiteral(text);
        }
        final Function constant = args.getQuick(index);
        return type == ColumnType.DATE ? constant.getDate(null) : constant.getTimestamp(null);
    }

    /**
     * The precision of a text IN element that spells a timestamp; UNDEFINED for anything else.
     */
    private int textConstantType(ObjList<Function> args, int index, TimestampDriver driver) {
        final CharSequence text = args.getQuick(index).isConstant() ? literalText(arguments.getQuick(index)) : null;
        return text != null ? IntervalUtils.literalTimestampType(driver, text) : ColumnType.UNDEFINED;
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
    private SqlException timestampTextError(CharSequence operator, int index, CharSequence text, int position) {
        final boolean isBetween = SqlKeywords.isBetweenKeyword(operator);
        if (!isBetween && !isTemporalComparisonOperator(operator)
                || arguments.getQuick(isBetween ? 0 : 1 - index).getDataType() == ColumnType.DATE) {
            return SqlException.invalidDate(text, position);
        }
        final boolean isLiteral = arguments.getQuick(index) instanceof ConstantExpression constant && IntervalExtractor.isStringLiteral(constant);
        if (isBetween) {
            return isLiteral ? SqlException.invalidDate(position) : SqlException.invalidDate(text, position);
        }
        if (Chars.equals(operator, '=') || isNotEqualsOperator(operator)) {
            if (Chars.indexOf(text, ';') >= 0) {
                return SqlException.$(position, isLiteral ? "not a timestamp, use IN keyword with intervals" : "Not a date, use IN keyword with intervals");
            }
            return isLiteral ? SqlException.$(position, "invalid timestamp") : SqlException.invalidDate(text, position);
        }
        return isLiteral ? SqlException.position(position).put("Invalid date [str='").put(text).put("']") : SqlException.invalidDate(text, position);
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

    /**
     * Rejects an overload the call's place in the tree does not admit: an aggregate outside its aggregate root, a
     * non-window function at the window root, a window function outside a window. A window function directly under
     * the window root is reported where the window is constructed.
     */
    private void validateFactory(FunctionFactoryDescriptor overload, int role, int position, ObjList<Function> args) throws SqlException {
        final BindScope scope = ctx.scope();
        final FunctionFactory factory = overload.getFactory();
        if (role != CALL_AGGREGATE_ROOT && role != CALL_WINDOW_ROOT && factory.isGroupBy()) {
            final SqlException exception = role != CALL_ROOT
                    ? SqlException.$(position, "Aggregate function cannot be passed as an argument")
                    : scope.isBindingPredicate ? SqlException.$(position, "boolean expression expected")
                      : SqlException.$(position, "aggregate functions are not allowed in this context");
            Misc.freeObjList(args, exception);
            throw exception;
        }
        if (role == CALL_WINDOW_ROOT && !factory.isWindow()) {
            final SqlException exception = SqlException.$(position, "non-window function called in window context");
            Misc.freeObjList(args, exception);
            throw exception;
        }
        if (role != CALL_WINDOW_ROOT && role != CALL_AGGREGATE_ROOT && factory.isWindow()) {
            if (role == CALL_WINDOW_ARGUMENT) {
                if (scope.nestedWindowPosition < 0) {
                    scope.nestedWindowPosition = position;
                }
                return;
            }
            final SqlException exception = SqlException.emptyWindowContext(position);
            Misc.freeObjList(args, exception);
            throw exception;
        }
        if (role == CALL_AGGREGATE_ROOT && !factory.isGroupBy()
                && !(scope.isBindingGroupByExpression && overload.isArrayElementWiseScalar())) {
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
        if (node.windowExpression != null && node != ctx.scope().windowRoot
                || node.type != ExpressionNode.LITERAL && node.type != ExpressionNode.CONSTANT
                && node.type != ExpressionNode.FUNCTION && node.type != ExpressionNode.OPERATION
                && node.type != ExpressionNode.BIND_VARIABLE && node.type != ExpressionNode.MEMBER_ACCESS
                && node.type != ExpressionNode.ARRAY_CONSTRUCTOR && node.type != ExpressionNode.ARRAY_ACCESS
                && (node.type != ExpressionNode.SET_OPERATION
                || !SqlKeywords.isInKeyword(node.token) && !SqlKeywords.isBetweenKeyword(node.token))) {
            throw new IllegalStateException("unexpected expression node type");
        }
    }

    private void validateSubsampleArguments(ExpressionNode node, ObjList<Function> args, IntList positions) throws SqlException {
        if (node.windowExpression == null || !node.windowExpression.isSubsampleKeepFlag() || args == null) {
            return;
        }
        if (Chars.equalsIgnoreCase(node.token, "uniform") && args.size() == 1) {
            SubsampleValidator.validatePositionTargetOrThrow(args.getQuick(0), positions.getQuick(0), false);
        } else if (Chars.equalsIgnoreCase(node.token, "cadence") && args.size() >= 1 && args.size() <= 2) {
            SubsampleValidator.validatePositionTargetOrThrow(args.getQuick(0), positions.getQuick(0), true);
            if (args.size() == 2) {
                SubsampleValidator.validateCadenceSeedOrThrow(args.getQuick(1), positions.getQuick(1));
            }
        } else if ((Chars.equalsIgnoreCase(node.token, "m4") || Chars.equalsIgnoreCase(node.token, "minmax")
                || Chars.equalsIgnoreCase(node.token, "lttb")) && args.size() >= 3) {
            SubsampleValidator.validatePositionTargetOrThrow(args.getQuick(2), positions.getQuick(2), false);
            final ExpressionNode raw = node.windowExpression.getPendingSubsample();
            if (Chars.equalsIgnoreCase(node.token, "lttb") && raw != null && raw.paramCount == 3) {
                SubsampleValidator.validateLttbGapOrThrow(raw.args.getQuick(2));
            }
        } else if (Chars.equalsIgnoreCase(node.token, "sdt") && args.size() == 3) {
            SubsampleValidator.validateSdtCompdev(args.getQuick(2), positions.getQuick(2));
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
        if (count == 2 && isTemporalComparisonOperator(operator)) {
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

    static int findColumn(ExpressionNode node, OutputSchema input, CharSequence inputAlias) {
        final CharSequence name = node.token;
        final int dot = Chars.indexOfLastUnquoted(name, '.');
        if (dot < 0) {
            return getColumnIndexQuiet(input, null, name, 0, name.length());
        }
        final CharSequence qualifier = GenericLexer.unquote(name.subSequence(0, dot));
        if (input.hasColumnQualifiers() ? input.hasColumnQualifier(qualifier)
                : Chars.equalsIgnoreCaseNc(qualifier, inputAlias)) {
            return getColumnIndexQuiet(input, input.hasColumnQualifiers() ? qualifier : null, name, dot + 1, name.length());
        }
        return -1;
    }

    static boolean isTemporalComparisonOperator(CharSequence operator) {
        return Chars.equals(operator, '=') || Chars.equals(operator, '<') || Chars.equals(operator, '>')
                || Chars.equals(operator, "<=") || Chars.equals(operator, ">=") || isNotEqualsOperator(operator);
    }

    static boolean isUnknownQualifier(CharSequence qualifier, OutputSchema input, CharSequence inputAlias) {
        if (input.hasColumnQualifiers()) {
            return !input.hasColumnQualifier(qualifier);
        }
        return !Chars.equalsIgnoreCaseNc(qualifier, inputAlias);
    }

    static int resolveColumn(ExpressionNode node, OutputSchema input, CharSequence inputAlias) throws SqlException {
        final int index = findColumn(node, input, inputAlias);
        if (index == OutputSchema.COLUMN_AMBIGUOUS) {
            throw SqlException.ambiguousColumn(node.position, node.token);
        }
        if (index < 0) {
            final int dot = Chars.indexOfLastUnquoted(node.token, '.');
            if (dot > -1 && isUnknownQualifier(GenericLexer.unquote(node.token.subSequence(0, dot)), input, inputAlias)) {
                throw SqlException.$(node.position, "Invalid table name or alias");
            }
            throw SqlException.invalidColumn(node.position, node.token);
        }
        return index;
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
        if (node.type != ExpressionNode.LITERAL || findColumn(node, input, inputAlias) != -1) {
            return false;
        }
        final ObjList<OutputSchema> outerScopes = ctx.scope().outerScopes;
        for (int i = outerScopes.size() - 1; i > -1; i--) {
            if (findColumn(node, outerScopes.getQuick(i), null) > -1) {
                return true;
            }
        }
        return false;
    }

    /**
     * Returns a borrowed converted root; its prepared slot retains ownership.
     */
    Function prepareUpdateAssignment(BoundExpression expression, int targetType, SqlExecutionContext executionContext) throws SqlException {
        final PreparedFunctions.Entry entry = ctx.preparedFunctions.findOwned(expression);
        if (entry == null) {
            throw new IllegalStateException("UPDATE assignment is not owned");
        }
        if (entry.updateTargetType >= 0) {
            throw new IllegalStateException("UPDATE assignment already prepared");
        }
        final int slot = ctx.preparedFunctions.reserve();
        final Function original = ctx.preparedFunctions.detach(entry);
        // convertUpdateFunction() frees the original root when the conversion fails
        final Function function = FunctionInstantiator.convertUpdateFunction(resolver, original, targetType, expression.getPosition(), executionContext);
        ctx.preparedFunctions.own(entry, slot, function);
        entry.updateTargetType = targetType;
        return function;
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
        final PreparedFunctions.Entry entry = ctx.preparedFunctions.findOwned(expression);
        if (entry == null || !(ctx.preparedFunctions.root(entry) instanceof GroupByFunction function)) {
            throw new IllegalStateException("aggregate preparation is not owned");
        }
        final CharSequence unsupportedFill = GroupByUtils.getUnsupportedSampleByFill(function, fillToken);
        if (unsupportedFill != null) {
            throw SqlException.$(fillPosition, "support for ").put(unsupportedFill)
                    .put(" fill is not yet implemented [function=").put(aggregateNode)
                    .put(", class=").put(function.getClass().getName()).put(']');
        }
    }
}
