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
import io.questdb.cairo.GeoHashes;
import io.questdb.cairo.ImplicitCastException;
import io.questdb.cairo.MillisTimestampDriver;
import io.questdb.cairo.TimestampDriver;
import io.questdb.cairo.sql.Function;
import io.questdb.griffin.engine.functions.GroupByFunction;
import io.questdb.griffin.engine.functions.SubqueryCursorFunction;
import io.questdb.griffin.engine.functions.bool.BooleanSubQueryFunction;
import io.questdb.griffin.engine.functions.bool.InTimestampTimestampFunctionFactory;
import io.questdb.griffin.engine.functions.columns.BindableColumn;
import io.questdb.griffin.engine.functions.constants.BooleanConstant;
import io.questdb.griffin.engine.functions.constants.ConstantFunction;
import io.questdb.griffin.engine.functions.constants.DateConstant;
import io.questdb.griffin.engine.functions.constants.TimestampConstant;
import io.questdb.griffin.engine.groupby.GroupByUtils;
import io.questdb.griffin.engine.window.WindowFunction;
import io.questdb.griffin.model.ExpressionNode;
import io.questdb.griffin.model.IntervalUtils;
import io.questdb.griffin.plan.logical.BindVariableExpression;
import io.questdb.griffin.plan.logical.BoundExpression;
import io.questdb.griffin.plan.logical.ColumnExpression;
import io.questdb.griffin.plan.logical.ConstantExpression;
import io.questdb.griffin.plan.logical.CursorExpression;
import io.questdb.griffin.plan.logical.FunctionExpression;
import io.questdb.griffin.plan.logical.LogicalPlan;
import io.questdb.griffin.plan.logical.OuterColumnExpression;
import io.questdb.griffin.plan.logical.OutputSchema;
import io.questdb.griffin.plan.logical.TypeExpression;
import io.questdb.std.Chars;
import io.questdb.std.Decimal128;
import io.questdb.std.Decimal256;
import io.questdb.std.GenericLexer;
import io.questdb.std.IntHashSet;
import io.questdb.std.IntList;
import io.questdb.std.Interval;
import io.questdb.std.Misc;
import io.questdb.std.Mutable;
import io.questdb.std.Numbers;
import io.questdb.std.NumericException;
import io.questdb.std.ObjList;
import io.questdb.std.ObjectPool;
import org.jetbrains.annotations.TestOnly;

/**
 * Binds SQL expressions by capturing the overloads {@link FunctionParser} selects while it builds them, producing
 * {@link BoundExpression} descriptions and handing each built root to {@link PreparedFunctions}.
 */
public final class FunctionBinder implements Mutable {
    private final IntList argumentLeafMarks = new IntList();
    private final IntList argumentPositions = new IntList();
    private final IntList argumentTypes = new IntList();
    private final ObjList<BoundExpression> arguments = new ObjList<>();
    private final ObjList<ExpressionNode> callArguments = new ObjList<>();
    private final BindContext ctx;
    private final ObjectPool<CursorExpression> cursors = new ObjectPool<>(CursorExpression.FACTORY, 4);
    private final Decimal128 decimal128 = new Decimal128();
    private final Decimal256 decimal256 = new Decimal256();
    private final ObjectPool<ExpressionNode> expressionNodes = new ObjectPool<>(ExpressionNode.FACTORY, 32);
    private final IntList expressionLeafMarks = new IntList();
    private final ObjList<BoundExpression> expressionStack = new ObjList<>();
    private final IntHashSet keySubqueryColumnIds = new IntHashSet();
    private final IntList outerColumnIds = new IntList();
    private final ObjList<OutputSchema> outerScopes = new ObjList<>();
    private final FunctionParser parser;
    private final ObjList<ExpressionNode> predicateConjuncts = new ObjList<>();
    private final ObjectPool<StaticTypeFunction> staticTypes = new ObjectPool<>(StaticTypeFunction::new, 8);
    private final ObjList<BoundExpression> unconstructed = new ObjList<>();
    private ExpressionNode aggregateRoot;
    private ExpressionNode bindingRoot;
    private int compiledLowerBoundIndex;
    private ExpressionNode compiledLowerBoundNode;
    private PreparedFunctions.Entry currentPreparation;
    private OutputSchema input;
    private CharSequence inputAlias;
    private boolean isBindingGroupByExpression;
    private boolean isBindingPredicate;
    private IntHashSet nativeTimestampIds;
    private int nestedWindowPosition;
    private ObjList<? extends BoundExpression> replacementExpressions;
    private ObjList<ExpressionNode> replacementNodes;
    private final SqlBinder subqueryBinder;
    private ExpressionNode windowRoot;

    /**
     * Allocates descriptions from the context's pools and hands built roots to its prepared functions; the
     * sub-query binder is null where no sub-query can occur.
     */
    FunctionBinder(BindContext ctx, FunctionParser parser, SqlBinder subqueryBinder) {
        this.ctx = ctx;
        this.parser = parser;
        this.subqueryBinder = subqueryBinder;
    }

    /**
     * A binder over its own stand-alone context, with no sub-query support.
     */
    @TestOnly
    public static FunctionBinder newStandalone(FunctionParser parser) {
        return new BindContext(parser, new ObjectPool<>(ExpressionNode.FACTORY, 32), new CharacterStore(1024, 16), null).functionBinder;
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
        return bind(node, input, inputAlias, preferredType, false, executionContext);
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
        assert this.replacementNodes == null && replacementNodes.size() == replacementColumns.size();
        this.replacementNodes = replacementNodes;
        this.replacementExpressions = replacementColumns;
        try {
            return bind(node, input, inputAlias, preferredType, executionContext);
        } finally {
            this.replacementNodes = null;
            this.replacementExpressions = null;
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
                && parser.getFunctionFactoryCache().isGroupBy(node.token);
        return (FunctionExpression) bindAggregateRoot(node, input, inputAlias, executionContext);
    }

    /**
     * Binds a call over already-bound arguments as if its SQL text had them as
     * children: the same overload selection, implicit casts, constant folding and
     * errors. A group-by name binds as an aggregate root. Arguments, their
     * columns' input and the argument list are borrowed only for this call.
     */
    public BoundExpression bindCall(
            CharSequence name,
            int position,
            ObjList<? extends BoundExpression> args,
            OutputSchema input,
            SqlExecutionContext executionContext
    ) throws SqlException {
        assert replacementNodes == null;
        final OperatorExpression operator = OperatorExpression.getRegistry().getOperatorDefinition(name);
        final ExpressionNode call = expressionNodes.next().of(operator == null ? ExpressionNode.FUNCTION
                : operator.type == OperatorExpression.SET ? ExpressionNode.SET_OPERATION : ExpressionNode.OPERATION, name, 0, position);
        final int count = args.size();
        for (int i = 0; i < count; i++) {
            final BoundExpression argument = args.getQuick(i);
            // A float literal's spelling selects its exact DECIMAL cast, as in SQL text.
            final CharSequence literalText = argument instanceof ConstantExpression constant ? constant.getLiteralText() : null;
            callArguments.add(expressionNodes.next().of(literalText != null ? ExpressionNode.CONSTANT : ExpressionNode.LITERAL,
                    literalText, 0, argument.getPosition()));
        }
        call.paramCount = count;
        if (count < 3) {
            call.lhs = count == 2 ? callArguments.getQuick(0) : null;
            call.rhs = count > 0 ? callArguments.getLast() : null;
        } else {
            for (int i = count - 1; i >= 0; i--) {
                call.args.add(callArguments.getQuick(i));
            }
        }
        replacementNodes = callArguments;
        replacementExpressions = args;
        try {
            return isGroupBy(name)
                    ? bindAggregateRoot(call, input, null, executionContext)
                    : bind(call, input, null, executionContext);
        } finally {
            replacementNodes = null;
            replacementExpressions = null;
            callArguments.clear();
        }
    }

    public BoundExpression bindGroupByExpression(
            ExpressionNode node,
            OutputSchema input,
            CharSequence inputAlias,
            SqlExecutionContext executionContext
    ) throws SqlException {
        assert !isBindingGroupByExpression;
        assert node.type == ExpressionNode.FUNCTION && node.windowExpression == null
                && parser.getFunctionFactoryCache().isGroupBy(node.token);
        isBindingGroupByExpression = true;
        try {
            return bindAggregateRoot(node, input, inputAlias, executionContext);
        } finally {
            isBindingGroupByExpression = false;
        }
    }

    public BoundExpression bindGroupByExpression(
            ExpressionNode node,
            OutputSchema input,
            ObjList<ExpressionNode> replacementNodes,
            ObjList<ColumnExpression> replacementColumns,
            SqlExecutionContext executionContext
    ) throws SqlException {
        assert this.replacementNodes == null && replacementNodes.size() == replacementColumns.size();
        this.replacementNodes = replacementNodes;
        this.replacementExpressions = replacementColumns;
        try {
            return bindGroupByExpression(node, input, null, executionContext);
        } finally {
            this.replacementNodes = null;
            this.replacementExpressions = null;
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
        assert !isBindingPredicate;
        isBindingPredicate = true;
        this.nativeTimestampIds = nativeTimestampIds;
        try {
            return bind(node, input, inputAlias, preferredRootType, executionContext);
        } finally {
            predicateConjuncts.clear();
            isBindingPredicate = false;
            this.nativeTimestampIds = null;
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
        return bind(node, input, inputAlias, targetType, true, executionContext);
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
        assert windowRoot == null && aggregateRoot == null && !isBindingPredicate;
        windowRoot = node;
        nestedWindowPosition = -1;
        try {
            return (FunctionExpression) bind(node, input, inputAlias, executionContext);
        } finally {
            windowRoot = null;
        }
    }

    @Override
    public void clear() {
        expressionStack.clear();
        expressionLeafMarks.clear();
        arguments.clear();
        argumentLeafMarks.clear();
        argumentPositions.clear();
        cursors.clear();
        outerColumnIds.clear();
        outerScopes.clear();
        keySubqueryColumnIds.clear();
        staticTypes.clear();
        unconstructed.clear();
        currentPreparation = null;
        input = null;
        inputAlias = null;
    }

    @TestOnly
    public void clearExpressions() {
        ctx.clearExpressions();
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
        return parser.getFunctionFactoryCache().isGroupBy(name);
    }

    /**
     * The flags of a call built while binding. Sub-query placeholders report stability, so a call stable only
     * through an argument stable with its sub-queries is itself stable with them.
     */
    private static int callFlags(Function function, int argumentStability) {
        final int flags = functionFlags(function);
        if ((flags & BoundExpression.STABLE_WITHIN_EXECUTION) == 0 || argumentStability != BoundExpression.STABLE_WITH_SUBQUERIES) {
            return flags;
        }
        return flags & ~BoundExpression.STABLE_WITHIN_EXECUTION | BoundExpression.STABLE_WITH_SUBQUERIES;
    }

    private static int callFlags(Function function, FunctionExpression call) {
        int stability = BoundExpression.STABLE_WITHIN_EXECUTION;
        for (int i = 0, n = call.getArgumentCount(); i < n; i++) {
            stability = conditionalStability(stability, call.argumentAt(i));
        }
        return callFlags(function, stability);
    }

    private static int callFlags(Function function, ObjList<BoundExpression> arguments) {
        int stability = BoundExpression.STABLE_WITHIN_EXECUTION;
        for (int i = 0, n = arguments.size(); i < n; i++) {
            stability = conditionalStability(stability, arguments.getQuick(i));
        }
        return callFlags(function, stability);
    }

    private static int conditionalStability(int stability, BoundExpression argument) {
        return LogicalPlans.stabilityFlags(argument) == BoundExpression.STABLE_WITH_SUBQUERIES
                ? BoundExpression.STABLE_WITH_SUBQUERIES : stability;
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

    static boolean isTemporalComparisonOperator(CharSequence operator) {
        return Chars.equals(operator, '=') || Chars.equals(operator, '<') || Chars.equals(operator, '>')
                || Chars.equals(operator, "<=") || Chars.equals(operator, ">=") || isNotEqualsOperator(operator);
    }

    /**
     * A TIMESTAMP or DATE value that is not a constant: the side of a comparison whose precision the
     * constants on the other side adopt.
     */
    private static boolean isTemporalOperand(Function function) {
        return !function.isConstant()
                && (ColumnType.tagOf(function.getType()) == ColumnType.TIMESTAMP || function.getType() == ColumnType.DATE);
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

    private BoundExpression bind(
            ExpressionNode node,
            OutputSchema input,
            CharSequence inputAlias,
            int preferredType,
            boolean isUpdateAssignment,
            SqlExecutionContext executionContext
    ) throws SqlException {
        assert currentPreparation == null;
        final PreparedFunctions.Entry entry = ctx.preparedFunctions.begin();
        currentPreparation = entry;
        this.input = input;
        this.inputAlias = inputAlias;
        expressionStack.clear();
        expressionLeafMarks.clear();
        final ExpressionNode originalAggregateRoot = aggregateRoot;
        final ExpressionNode originalWindowRoot = windowRoot;
        try {
            if (isAstRewritable()) {
                // DECLARE references may share parser nodes across occurrences.
                // Reassociation belongs to this binding, never to that shared AST.
                node = ExpressionNode.deepClone(expressionNodes, node);
                if (originalAggregateRoot != null) {
                    aggregateRoot = node;
                }
                if (originalWindowRoot != null) {
                    windowRoot = node;
                }
            }
            if (isBindingPredicate) {
                if (isAstRewritable()) {
                    rewriteAndOffsets(node);
                }
                collectPredicateConjuncts(node);
                compileTimestampBetweenLowerBound(executionContext);
            }
            bindingRoot = node;
            Function function = parser.parseFunction(node, executionContext, this);
            if (function instanceof StaticTypeFunction) {
                // UPDATE converts the owned root while binding; every other root is built by its generator.
                if (isUpdateAssignment) {
                    function = ctx.functionInstantiator.realize(expressionStack.getQuick(0), input, entry, executionContext);
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
            assert expressionStack.size() == 1;
            entry.expression = expressionStack.getQuick(0);
            assert PreparedFunctions.hasOnlyReadLeaves(entry) : "bound leaf is not read by its description";
            return entry.expression;
        } catch (Throwable th) {
            // The parser owns partial roots; only completed roots enter this scope.
            ctx.preparedFunctions.closeOnFailure(th);
            throw th;
        } finally {
            aggregateRoot = originalAggregateRoot;
            windowRoot = originalWindowRoot;
            bindingRoot = null;
            for (int i = 0, n = expressionNodes.getPos(); i < n; i++) {
                expressionNodes.peekQuick(i).clear();
            }
            expressionNodes.clear();
            currentPreparation = null;
            compiledLowerBoundNode = null;
            this.input = null;
            this.inputAlias = null;
            arguments.clear();
            argumentLeafMarks.clear();
            argumentPositions.clear();
            expressionStack.clear();
            expressionLeafMarks.clear();
            unconstructed.clear();
            staticTypes.clear();
        }
    }

    private BoundExpression bindAggregateRoot(
            ExpressionNode node,
            OutputSchema input,
            CharSequence inputAlias,
            SqlExecutionContext executionContext
    ) throws SqlException {
        assert aggregateRoot == null && !isBindingPredicate;
        aggregateRoot = node;
        try {
            return bind(node, input, inputAlias, executionContext);
        } finally {
            aggregateRoot = null;
        }
    }

    /**
     * The leaf mark of the call whose arguments are being bound: its leaves follow it in the preparation.
     */
    private int callLeafMark() {
        final int count = argumentLeafMarks.size();
        return count > 0 ? argumentLeafMarks.getQuick(count - 1) : leafMark();
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
    private Function canonicalizeTemporalComparison(ExpressionNode node, ObjList<Function> args, IntList positions) {
        final int count = args == null ? 0 : args.size();
        final CharSequence operator = node.token;
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

    /**
     * Records each text element of a predicate's TIMESTAMP IN list that does not parse as a timestamp as an
     * unparsed timestamp, so that whatever builds the list raises the element's parse error.
     */
    private void captureUnparsedInElements(FunctionFactoryDescriptor overload, ObjList<Function> args) {
        if (!isBindingPredicate || !(overload.getFactory() instanceof InTimestampTimestampFunctionFactory) || args.size() < 3
                || ColumnType.tagOf(args.getQuick(0).getType()) != ColumnType.TIMESTAMP) {
            return;
        }
        final int operandType = args.getQuick(0).getType();
        final TimestampDriver driver = ColumnType.getTimestampDriver(operandType);
        for (int i = 1, n = args.size(); i < n; i++) {
            final Function element = args.getQuick(i);
            final CharSequence text = element.isConstant() && isCaseText(element.getType()) ? literalText(arguments.getQuick(i)) : null;
            if (text != null && !isTimestampText(driver, text)) {
                args.setQuick(i, captureUnparsedTimestamp(i, text, operandType, arguments.getQuick(i).getPosition()));
            }
        }
    }

    private void collectPredicateConjuncts(ExpressionNode node) {
        if (node != null && node.token != null && SqlKeywords.isAndKeyword(node.token)) {
            collectPredicateConjuncts(node.lhs);
            collectPredicateConjuncts(node.rhs);
        } else {
            predicateConjuncts.add(node);
        }
    }

    /**
     * Timestamp interval analysis compiles a sub-query BETWEEN bound pair low bound first.
     */
    private void compileTimestampBetweenLowerBound(SqlExecutionContext executionContext) throws SqlException {
        if (subqueryBinder == null) {
            return;
        }
        for (int i = 0, n = predicateConjuncts.size(); i < n; i++) {
            final ExpressionNode conjunct = unwrapNot(predicateConjuncts.getQuick(i));
            if (conjunct.paramCount == 3 && SqlKeywords.isBetweenKeyword(conjunct.token)
                    && conjunct.args.getQuick(0).type == ExpressionNode.QUERY
                    && conjunct.args.getQuick(1).type == ExpressionNode.QUERY
                    && conjunct.args.getQuick(2).type == ExpressionNode.LITERAL) {
                final int columnIndex = findColumn(conjunct.args.getQuick(2), input, inputAlias);
                if (columnIndex >= 0 && isNativeTimestampColumn(input.getColumnId(columnIndex))) {
                    final ExpressionNode lo = conjunct.args.getQuick(1);
                    compiledLowerBoundIndex = subqueryBinder.compileSubquery(lo.queryModel, lo.position, executionContext);
                    compiledLowerBoundNode = lo;
                    return;
                }
            }
        }
    }

    private ConstantExpression constant(Function function, int position) {
        return switch (ColumnType.tagOf(function.getType())) {
            case ColumnType.TIMESTAMP ->
                    ctx.constants.next().ofTimestamp(function.getTimestamp(null), function.getType(), position);
            case ColumnType.STRING -> ctx.constants.next().ofString(Chars.toString(function.getStrA(null)), position);
            case ColumnType.SYMBOL -> ctx.constants.next().ofSymbol(Chars.toString(function.getSymbol(null)), position);
            case ColumnType.VARCHAR -> ctx.constants.next().ofVarchar(function.getVarcharA(null), position);
            case ColumnType.BYTE -> ctx.constants.next().ofByte(function.getByte(null), position);
            case ColumnType.SHORT -> ctx.constants.next().ofShort(function.getShort(null), position);
            case ColumnType.DATE -> ctx.constants.next().ofDate(function.getDate(null), position);
            case ColumnType.IPv4 -> ctx.constants.next().ofIPv4(function.getIPv4(null), position);
            case ColumnType.CHAR -> ctx.constants.next().ofChar(function.getChar(null), position);
            case ColumnType.BOOLEAN -> ctx.constants.next().ofBoolean(function.getBool(null), position);
            case ColumnType.INT -> ctx.constants.next().ofInt(function.getInt(null), position);
            case ColumnType.LONG -> ctx.constants.next().ofLong(function.getLong(null), position);
            case ColumnType.LONG256 -> ctx.constants.next().ofLong256(function.getLong256A(null), position);
            case ColumnType.UUID ->
                    ctx.constants.next().ofUuid(function.getLong128Lo(null), function.getLong128Hi(null), position);
            case ColumnType.LONG128 ->
                    ctx.constants.next().ofLong128(function.getLong128Lo(null), function.getLong128Hi(null), position);
            case ColumnType.GEOBYTE, ColumnType.GEOSHORT, ColumnType.GEOINT, ColumnType.GEOLONG ->
                    ctx.constants.next().ofGeoHash(GeoHashes.getGeoLong(function.getType(), function, null), function.getType(), position);
            case ColumnType.FLOAT -> ctx.constants.next().ofFloat(function.getFloat(null), position);
            case ColumnType.DOUBLE -> ctx.constants.next().ofDouble(function.getDouble(null), position);
            case ColumnType.NULL -> ctx.constants.next().ofNull(position);
            case ColumnType.DECIMAL8 ->
                    ctx.constants.next().ofDecimal(function.getType(), 0, 0, 0, function.getDecimal8(null), position);
            case ColumnType.DECIMAL16 ->
                    ctx.constants.next().ofDecimal(function.getType(), 0, 0, 0, function.getDecimal16(null), position);
            case ColumnType.DECIMAL32 ->
                    ctx.constants.next().ofDecimal(function.getType(), 0, 0, 0, function.getDecimal32(null), position);
            case ColumnType.DECIMAL64 ->
                    ctx.constants.next().ofDecimal(function.getType(), 0, 0, 0, function.getDecimal64(null), position);
            case ColumnType.DECIMAL128 -> {
                function.getDecimal128(null, decimal128);
                yield ctx.constants.next().ofDecimal(function.getType(), 0, 0, decimal128.getHigh(), decimal128.getLow(), position);
            }
            case ColumnType.DECIMAL256 -> {
                function.getDecimal256(null, decimal256);
                yield ctx.constants.next().ofDecimal(function.getType(), decimal256.getHh(), decimal256.getHl(),
                        decimal256.getLh(), decimal256.getLl(), position);
            }
            case ColumnType.INTERVAL -> {
                final Interval interval = function.getInterval(null);
                yield ctx.constants.next().ofInterval(interval.getLo(), interval.getHi(), function.getType(), position);
            }
            case ColumnType.BINARY -> {
                if (function.getBin(null) != null) {
                    throw new IllegalStateException("non-null BINARY constant");
                }
                yield ctx.constants.next().ofBinaryNull(position);
            }
            default -> throw new IllegalStateException("unexpected constant type");
        };
    }

    /**
     * Builds the arguments the binder typed without constructing them, for a parent that is constructed now.
     */
    private void constructArguments(ObjList<Function> args, int count, SqlExecutionContext executionContext) throws SqlException {
        for (int i = 0; i < count; i++) {
            final BoundExpression argument = arguments.getQuick(i);
            if (unconstructed.indexOf(argument) >= 0 || args.getQuick(i) instanceof StaticTypeFunction) {
                final Function placeholder = args.getQuick(i);
                args.setQuick(i, ctx.functionInstantiator.realize(argument, input, currentPreparation, executionContext));
                Misc.free(placeholder);
            }
        }
    }

    private Function createOuterColumn(ExpressionNode node) throws SqlException {
        for (int i = outerScopes.size() - 1; i > -1; i--) {
            final OutputSchema scope = outerScopes.getQuick(i);
            final int index = findColumn(node, scope, null);
            if (index > -1) {
                final int columnId = scope.getColumnId(index);
                final int type = scope.getColumnType(index);
                outerColumnIds.add(columnId);
                currentPreparation.isRebuildRequired = true;
                push(ctx.outerColumns.next().of(columnId, type, node.position), leafMark());
                return BindableColumn.isBindableType(type) ? BindableColumn.newInstance(columnId, type, scope.isSymbolTableStatic(index))
                        : FunctionInstantiator.createColumnFunction(node.position, index, type, scope);
            }
        }
        return null;
    }

    /**
     * Closes the column leaves at {@code [from, to)} of the preparation: a fold evaluated the operand they belong
     * to, so the bound description no longer reads them.
     */
    private void closeLeaves(int from, int to) {
        if (from < to) {
            final ObjList<BindableColumn> leaves = currentPreparation.leaves;
            for (int i = from; i < to; i++) {
                final BindableColumn leaf = leaves.getQuick(i);
                if (leaf.isOpen()) {
                    leaf.close();
                }
            }
        }
    }

    /**
     * Closes and removes the column leaves at {@code [from, to)} of the preparation: a fold dropped the operand they
     * belong to, so the bound description no longer reads them.
     */
    private void dropLeaves(int from, int to) {
        if (from < to) {
            final ObjList<BindableColumn> leaves = currentPreparation.leaves;
            for (int i = from; i < to; i++) {
                final BindableColumn leaf = leaves.getQuick(i);
                if (leaf.isOpen()) {
                    leaf.close();
                }
            }
            leaves.remove(from, to - 1);
        }
    }

    private void finish(Function function) {
        final int index = expressionStack.size() - 1;
        final BoundExpression normalized = normalizeArgument(expressionStack.getQuick(index), function);
        if (normalized instanceof ConstantExpression) {
            dropLeaves(expressionLeafMarks.getQuick(index), leafMark());
        }
        expressionStack.setQuick(index, normalized);
    }

    /**
     * Whether the call holds a timestamp literal that does not parse, which only an unconstructed call can hold
     * below it, or is a predicate comparison of a TIMESTAMP with a constant text that does not convert to one.
     */
    private boolean hasUnparsedTimestampText(ExpressionNode node, ObjList<Function> args) {
        final int count = args.size();
        for (int i = 0; i < count; i++) {
            final BoundExpression argument = arguments.getQuick(i);
            if ((argument instanceof ConstantExpression || unconstructed.indexOf(argument) >= 0)
                    && LogicalPlans.hasGenerationError(argument)) {
                return true;
            }
        }
        return count == 2 && isBindingPredicate && isTemporalComparisonOperator(node.token)
                && (LogicalPlans.isUnconvertibleSymbol(arguments.getQuick(0), args.getQuick(1).getType())
                || LogicalPlans.isUnconvertibleSymbol(arguments.getQuick(1), args.getQuick(0).getType()));
    }

    /**
     * A call over a text constant that does not convert to TIMESTAMP raises that conversion error whenever it is
     * built, whatever its other constant arguments, so it needs no vetting to wait for code generation.
     */
    private boolean isAdmittedUnconstructed(
            FunctionFactoryDescriptor overload,
            ExpressionNode node,
            ObjList<Function> args,
            IntList positions,
            SqlExecutionContext executionContext
    ) throws SqlException {
        if (hasUnparsedTimestampText(node, args)) {
            parser.admitUnvetted(overload, node.position, node.token, executionContext);
            return true;
        }
        return parser.admitUnconstructed(overload, node.position, node.token, args, positions, executionContext);
    }

    private boolean isNativeTimestampColumn(int columnId) {
        return nativeTimestampIds != null ? nativeTimestampIds.contains(columnId)
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

    private boolean isWindowArgument(ExpressionNode node) {
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
        return currentPreparation != null ? currentPreparation.leaves.size() : 0;
    }

    private ConstantExpression markSource(ConstantExpression folded, FunctionFactoryDescriptor overload, int type, int flags, int position) {
        return markSource(folded, ctx.functions.next().of(overload, arguments, argumentPositions, type, flags, position));
    }

    private BoundExpression normalizeArgument(BoundExpression expression, Function function) {
        if (function instanceof TypeConstant) {
            return expression;
        }
        if (expression instanceof CursorExpression cursor && function instanceof BooleanSubQueryFunction) {
            return cursors.next().ofBoolean(cursor, functionFlags(function) & ~BoundExpression.STABLE_WITHIN_EXECUTION
                    | cursor.getFunctionFlags() & BoundExpression.STABLE_WITHIN_EXECUTION);
        }
        if (ColumnType.isArray(function.getType()) && expression instanceof FunctionExpression call
                && (call.getDataType() != function.getType() || call.getFunctionFlags() != callFlags(function, call))) {
            ctx.tmpArguments.clear();
            ctx.tmpPositions.clear();
            try {
                for (int i = 0; i < call.getArgumentCount(); i++) {
                    ctx.tmpArguments.add(call.argumentAt(i));
                    ctx.tmpPositions.add(call.getArgumentPosition(i));
                }
                return ctx.functions.next().of(call.getOverload(), ctx.tmpArguments, ctx.tmpPositions,
                        function.getType(), callFlags(function, call), call.getPosition());
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
            return ctx.parameters.next().of(parameter.getName(), function.getType(), functionFlags(function), parameter.getPosition(), parameter.isDirectReference());
        }
        return expression;
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
                        && !constant.isUnparsedTimestamp() && constant.getDataType() == arg.getType()) {
                    final PreparedFunctions.Entry entry = ctx.preparedFunctions.begin();
                    entry.expression = constant;
                    ctx.preparedFunctions.own(entry, arg);
                    args.setQuick(i, null);
                } else if (arg instanceof BindableColumn leaf && leaf.isOpen() && argument instanceof ColumnExpression column
                        && column.getColumnId() == leaf.getColumnId() && column.getDataType() == leaf.getType()) {
                    final ObjList<BindableColumn> leaves = currentPreparation.leaves;
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

    private BoundExpression pushCall(ExpressionNode node, BoundExpression expression) {
        if (node.type == ExpressionNode.SET_OPERATION && expression instanceof FunctionExpression call) {
            call.markSetOperation();
        }
        final BoundExpression pushed = node.token == "dateadd" && expression instanceof FunctionExpression call
                ? ctx.functions.next().ofProjectedOffset(call) : expression;
        push(pushed, callLeafMark());
        return pushed;
    }

    private boolean referencesColumn(ExpressionNode node, int index) {
        if (node == null) {
            return false;
        }
        if (node.type == ExpressionNode.LITERAL) {
            return findColumn(node, input, inputAlias) == index;
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
        final int timestampIndex = input.getTimestampIndex();
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
     * audited scalar over columns and other such calls, with type operands and constants beside them. Construction
     * over non-constant arguments cannot vary in type with argument values; the factory vets the arguments on
     * admission ({@link FunctionParser#admitUnconstructed}). A call that holds timestamp text that does
     * not parse may have any other scalar arguments: building it raises that text's error.
     * {@link ColumnType#UNDEFINED} when the call must be constructed.
     */
    private int staticResultType(FunctionFactoryDescriptor overload, ExpressionNode node, ObjList<Function> args, int count) {
        final FunctionFactory factory = overload.getFactory();
        if (count == 0 || !overload.isRelocatableScalar() || overload.isCase() || overload.isSwitch()
                || factory.isGroupBy() || factory.isWindow() || factory.isCursor() || node == windowRoot || node == aggregateRoot) {
            return ColumnType.UNDEFINED;
        }
        final boolean hasUnparsedText = hasUnparsedTimestampText(node, args);
        boolean hasVariableArgument = false;
        argumentTypes.clear();
        for (int i = 0; i < count; i++) {
            final BoundExpression argument = arguments.getQuick(i);
            final int type = argument.getDataType();
            if (!(argument instanceof TypeExpression) && !(argument instanceof ConstantExpression)) {
                if (ColumnType.isArray(type) || !hasUnparsedText && (argument.getFunctionFlags() != BoundExpression.STABLE_WITHIN_EXECUTION
                        || !(argument instanceof ColumnExpression && BindableColumn.isBindableType(type) || unconstructed.indexOf(argument) >= 0))) {
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
                 ColumnType.DECIMAL128, ColumnType.DECIMAL256 -> ctx.types.next().of(function.getType(), position);
            default -> throw new IllegalStateException("unexpected CAST target type");
        };
    }

    private void validateKeySubquery() throws SqlException {
        if (!(arguments.getQuick(0) instanceof ColumnExpression column) || !keySubqueryColumnIds.contains(column.getColumnId())
                || !(arguments.getQuick(1) instanceof CursorExpression cursor) || cursor.isBoolean()) {
            return;
        }
        final OutputSchema output = cursor.getPlan().getOutput();
        final int type = output.getColumnType(0);
        switch (ColumnType.tagOf(type)) {
            case ColumnType.STRING, ColumnType.SYMBOL, ColumnType.VARCHAR -> {
            }
            default ->
                    throw SqlException.position(subqueryBinder.getSubqueryFirstColumnPosition(cursor.getSubqueryIndex()))
                            .put("unsupported column type: ")
                            .put(output.getColumnName(0))
                            .put(": ")
                            .put(ColumnType.nameOf(type));
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
        if (findColumn(node, input, inputAlias) != timestampIndex) {
            return node;
        }
        final ExpressionNode dateadd = expressionNodes.next().of(ExpressionNode.FUNCTION, "dateadd", 0, node.position);
        dateadd.paramCount = 3;
        dateadd.args.add(node);
        dateadd.args.add(expressionNodes.next().of(ExpressionNode.CONSTANT, stride, 0, node.position));
        dateadd.args.add(expressionNodes.next().of(ExpressionNode.CONSTANT, unit, 0, node.position));
        return dateadd;
    }

    private void wrapTimestampColumns(ExpressionNode node, CharSequence unit, CharSequence stride, int timestampIndex) {
        node.lhs = wrapTimestampColumn(node.lhs, unit, stride, timestampIndex);
        node.rhs = wrapTimestampColumn(node.rhs, unit, stride, timestampIndex);
        for (int i = 0, n = node.args.size(); i < n; i++) {
            node.args.setQuick(i, wrapTimestampColumn(node.args.getQuick(i), unit, stride, timestampIndex));
        }
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

    static int functionFlags(Function function) {
        return (function.isConstant() ? BoundExpression.CONSTANT : 0)
                | (function.isRuntimeConstant() ? BoundExpression.RUNTIME_CONSTANT : 0)
                | (function.isNonDeterministic() ? BoundExpression.NON_DETERMINISTIC : 0)
                | (function.isStableWithinExecution() ? BoundExpression.STABLE_WITHIN_EXECUTION : 0);
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

    static int updateColumnType(Function function, int targetType) {
        final int type = function.getType();
        return targetType < 0 || !ColumnType.isBuiltInWideningCast(type, targetType)
                || targetType == ColumnType.TIMESTAMP && (type == ColumnType.STRING || type == ColumnType.VARCHAR) ? type : targetType;
    }

    void beginArguments(int count) {
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

    void captureConstant(Function function, int position) {
        captureConstant(function, position, null);
    }

    void captureConstant(Function function, int position, CharSequence token) {
        try {
            if (function instanceof TypeConstant) {
                push(type(function, position), leafMark());
                return;
            }
            final ConstantExpression constant = constant(function, position).markLiteral(null);
            final int tag = ColumnType.tagOf(constant.getDataType());
            push(token != null && (tag == ColumnType.DOUBLE || tag == ColumnType.FLOAT)
                    ? constant.withLiteralText(Chars.toString(token)) : constant, leafMark());
        } catch (Throwable th) {
            Misc.free(function, th);
            throw th;
        }
    }

    /**
     * Records the parser's actual inserted cast; never resolves the overload again. Returns the argument the call
     * receives: a constant cast folds to the constant its bound form instantiates as.
     */
    Function captureImplicitConversion(int index, Function function, int position, Class<? extends FunctionFactory> factoryClass) {
        if (function.isConstant()) {
            final Function folded = parser.functionToConstant(function);
            arguments.setQuick(index, constant(folded, position));
            return folded;
        }
        final ObjList<FunctionFactoryDescriptor> casts = parser.getFunctionFactoryCache().getOverloadList("cast");
        for (int i = 0, n = casts.size(); i < n; i++) {
            final FunctionFactoryDescriptor overload = casts.getQuick(i);
            if (overload.getFactory().getClass() == factoryClass) {
                ctx.tmpArguments.clear();
                ctx.tmpPositions.clear();
                try {
                    ctx.tmpArguments.add(arguments.getQuick(index));
                    ctx.tmpArguments.add(ctx.types.next().of(function.getType(), position));
                    ctx.tmpPositions.add(position);
                    ctx.tmpPositions.add(position);
                    final FunctionExpression conversion = ctx.functions.next().of(overload, ctx.tmpArguments,
                            ctx.tmpPositions, function.getType(),
                            callFlags(function, LogicalPlans.stabilityFlags(arguments.getQuick(index))), position);
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

    void captureParameter(Function function, ExpressionNode node, boolean isPredefined) {
        try {
            final BindVariableExpression parameter = ctx.parameters.next().of(node.token, function.getType(), functionFlags(function), node.position);
            push(isPredefined ? parameter.markPredefined() : parameter, leafMark());
        } catch (Throwable th) {
            Misc.free(function, th);
            throw th;
        }
    }

    /**
     * Records a text constant the call converts to TIMESTAMP that does not parse as one. Whatever builds the
     * call raises the parse error, so the returned stand-in is never evaluated.
     */
    Function captureUnparsedTimestamp(int index, CharSequence text, int type, int position) {
        arguments.setQuick(index, markSource(ctx.constants.next().ofUnparsedTimestamp(text, type, position), arguments.getQuick(index)));
        return staticTypes.next().of(type);
    }

    /**
     * Hands the consumers of a completed sub-query its optimised plan and the stability its factory proves.
     */
    void completeCursors(int subqueryIndex, LogicalPlan plan, boolean isFactoryStable) {
        for (int i = 0, n = cursors.getPos(); i < n; i++) {
            final CursorExpression cursor = cursors.peekQuick(i);
            if (cursor.getSubqueryIndex() == subqueryIndex) {
                cursor.ofGenerated(plan, isFactoryStable);
            }
        }
    }

    Function createColumn(ExpressionNode node) throws SqlException {
        if (findColumn(node, input, inputAlias) == -1) {
            final Function outer = createOuterColumn(node);
            if (outer != null) {
                return outer;
            }
        }
        final int index = resolveColumn(node, input, inputAlias);
        final int type = input.getColumnType(index);
        final int columnId = input.getColumnId(index);
        ctx.raiseDeferredColumn(columnId);
        final int mark = leafMark();
        final Function leaf;
        if (BindableColumn.isBindableType(type)) {
            final BindableColumn bindable = BindableColumn.newInstance(columnId, type, input.isSymbolTableStatic(index));
            currentPreparation.leaves.add(bindable);
            leaf = bindable;
        } else {
            leaf = FunctionInstantiator.createColumnFunction(node.position, index, type, input);
            currentPreparation.isRebuildRequired = true;
        }
        push(ctx.columns.next().of(columnId, type, node.position), mark);
        return leaf;
    }

    Function createCursorFunction(ExpressionNode node, SqlExecutionContext executionContext) throws SqlException {
        final int index;
        if (node == compiledLowerBoundNode) {
            index = compiledLowerBoundIndex;
            compiledLowerBoundNode = null;
        } else {
            index = subqueryBinder.compileSubquery(node.queryModel, node.position, executionContext);
        }
        final LogicalPlan plan = subqueryBinder.getSubqueryPlan(index);
        final int flags = LogicalPlans.isResultStable(plan, executionContext) ? BoundExpression.STABLE_WITHIN_EXECUTION : 0;
        currentPreparation.isRebuildRequired = true;
        push(cursors.next().of(plan, index, flags, node.position), leafMark());
        return new SubqueryCursorFunction(subqueryBinder.getSubqueryMetadata(index), true);
    }

    Function createFunction(
            FunctionFactoryDescriptor overload,
            ExpressionNode node,
            ObjList<Function> args,
            IntList positions,
            SqlExecutionContext executionContext
    ) throws SqlException {
        validateFactory(overload, node, args);
        final int staticType;
        try {
            captureUnparsedInElements(overload, args);
            final Function folded = canonicalizeTemporalComparison(node, args, positions);
            if (folded != null) {
                Misc.freeObjList(args);
                dropLeaves(callLeafMark(), leafMark());
                push(ctx.constants.next().ofBoolean(folded.getBool(null), node.position).markLiteral(), leafMark());
                return folded;
            }
            final int count = args == null ? 0 : args.size();
            if (count == 0) {
                arguments.clear();
                argumentLeafMarks.clear();
            }
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
            if (count == 2 && keySubqueryColumnIds.size() > 0 && SqlKeywords.isInKeyword(node.token)) {
                validateKeySubquery();
            }
            if (requiresConditionalRebuild(overload, args)) {
                currentPreparation.isRebuildRequired = true;
            }
            argumentPositions.clear();
            if (positions != null) {
                argumentPositions.addAll(positions);
            }
            final int declaredType = staticResultType(overload, node, args, count);
            staticType = declaredType != ColumnType.UNDEFINED
                    && isAdmittedUnconstructed(overload, node, args, positions, executionContext)
                    ? declaredType : ColumnType.UNDEFINED;
            if (staticType == ColumnType.UNDEFINED) {
                constructArguments(args, count, executionContext);
            }
        } catch (Throwable th) {
            Misc.freeObjList(args, th);
            throw th;
        }
        if (staticType != ColumnType.UNDEFINED) {
            prepareUnconstructedArguments(args);
            Misc.freeObjList(args);
            args.clear();
            unconstructed.add(pushCall(node, ctx.functions.next().of(overload, arguments, argumentPositions,
                    staticType, BoundExpression.STABLE_WITHIN_EXECUTION, node.position)));
            return staticTypes.next().of(staticType);
        }
        final Function first = args != null && args.size() == 2 ? args.getQuick(0) : null;
        final Function second = first != null ? args.getQuick(1) : null;
        final Function function = parser.createFunction(overload, node.position, node.token, args, positions, executionContext);
        try {
            if (node == windowRoot) {
                if (!(function instanceof WindowFunction)) {
                    throw SqlException.$(node.position, "non-window function called in window context");
                }
                if (nestedWindowPosition >= 0) {
                    throw SqlException.emptyWindowContext(nestedWindowPosition);
                }
            }
            // Some factories return a folded constant and close unused children.
            // Preserve that replacement rather than retaining false dependencies.
            final BoundExpression expression;
            if (function instanceof ConstantFunction && !ColumnType.isArray(function.getType())) {
                expression = markSource(constant(function, node.position), overload, function.getType(), functionFlags(function), node.position);
                dropLeaves(callLeafMark(), leafMark());
            } else if (first != null && isConnective(node.token) && (function == first || function == second)) {
                // A connective with a constant operand returns its other operand.
                expression = arguments.getQuick(function == first ? 0 : 1);
            } else {
                expression = ctx.functions.next().of(overload, arguments, argumentPositions,
                        function.getType(), callFlags(function, arguments), node.position);
            }
            pushCall(node, expression);
            return function;
        } catch (Throwable th) {
            Misc.free(function, th);
            throw th;
        }
    }

    /**
     * The evaluation step for an argument: a constant call is evaluated to its constant and its transient
     * function closed, and the argument's description becomes that constant. Owns the function on failure.
     */
    Function foldArgument(int index, Function function, int position) {
        final Function argument = function != null && function.isConstant() && function.extendedOps() == null
                && !(function instanceof TypeConstant) ? parser.functionToConstant(function) : function;
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

    /**
     * The evaluation step for the root: a constant root is evaluated to its constant and its transient function
     * closed, and the root's description is normalized to the result. Owns the function on failure.
     */
    Function foldRoot(Function function) {
        final Function root = function != null && function.isConstant() && function.extendedOps() == null
                ? parser.functionToConstant(function) : function;
        try {
            finish(root);
            return root;
        } catch (Throwable th) {
            Misc.free(root, th);
            throw th;
        }
    }

    /**
     * Generates, in bind order, the pending sub-queries among the arguments of a call that failed to resolve or
     * construct: a sub-query argument is generated before its call, so its errors precede the call's.
     */
    void generateArgumentSubqueries(SqlExecutionContext executionContext) throws SqlException {
        for (int i = arguments.size() - 1; i > -1; i--) {
            if (arguments.getQuick(i) instanceof CursorExpression cursor) {
                subqueryBinder.completeSubquery(cursor.getSubqueryIndex(), executionContext);
            }
        }
    }

    IntHashSet getKeySubqueryColumnIds() {
        return keySubqueryColumnIds;
    }

    /**
     * Ids of the outer columns every binding since {@link #clear()} resolved, in resolution order and with
     * repeats; a caller reads the range its own binding appended.
     */
    IntList getOuterColumnIds() {
        return outerColumnIds;
    }

    /**
     * True while a LATERAL body binds.
     */
    boolean hasOuterScope() {
        return outerScopes.size() > 0;
    }

    /**
     * True when no replacement matches subtrees by AST identity, so binding may clone and reassociate the tree.
     */
    boolean isAstRewritable() {
        return replacementNodes == null || replacementNodes.size() == 0;
    }

    /**
     * True when the name does not resolve in the input but does in an enclosing lateral scope.
     */
    boolean isOuterColumn(ExpressionNode node, OutputSchema input, CharSequence inputAlias) {
        if (node.type != ExpressionNode.LITERAL || findColumn(node, input, inputAlias) != -1) {
            return false;
        }
        for (int i = outerScopes.size() - 1; i > -1; i--) {
            if (findColumn(node, outerScopes.getQuick(i), null) > -1) {
                return true;
            }
        }
        return false;
    }

    /**
     * Whether the call is a predicate comparison whose TIMESTAMP text operands interval extraction validates.
     */
    boolean isTimestampComparison(ExpressionNode node) {
        return isBindingPredicate && (isTemporalComparisonOperator(node.token) || SqlKeywords.isBetweenKeyword(node.token));
    }

    boolean isUnresolvedNoArgFunction(ExpressionNode node) {
        return findColumn(node, input, inputAlias) == -1 && parser.findNoArgFunction(node);
    }

    void popOuterScope() {
        outerScopes.remove(outerScopes.size() - 1);
    }

    /**
     * Returns a borrowed converted root; its prepared slot retains ownership.
     */
    Function prepareUpdateAssignment(BoundExpression expression, int targetType) throws SqlException {
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
        final Function function = FunctionInstantiator.convertUpdateFunction(parser, original, targetType, expression.getPosition());
        ctx.preparedFunctions.own(entry, slot, function);
        entry.updateTargetType = targetType;
        return function;
    }

    /**
     * Makes the columns of {@code scope} resolvable as outer columns until {@link #popOuterScope()}.
     */
    void pushOuterScope(OutputSchema scope) {
        outerScopes.add(scope);
    }

    Function replaceNode(ExpressionNode node, SqlExecutionContext executionContext) throws SqlException {
        if (replacementNodes != null) {
            for (int i = 0, n = replacementNodes.size(); i < n; i++) {
                if (replacementNodes.getQuick(i) == node) {
                    final BoundExpression replacement = replacementExpressions.getQuick(i);
                    if (!(replacement instanceof ColumnExpression column)) {
                        // Leaves of a reconstructed subtree carry this layout's indexes.
                        if (replacement instanceof FunctionExpression || replacement instanceof CursorExpression) {
                            currentPreparation.isRebuildRequired = true;
                        }
                        final Function function = ctx.functionInstantiator.rebuild(replacement, input, executionContext);
                        push(replacement, leafMark());
                        return function;
                    }
                    ctx.raiseDeferredColumn(column.getColumnId());
                    final int index = input.getColumnIndexById(column.getColumnId());
                    if (index < 0 || input.getColumnType(index) != column.getDataType()) {
                        throw new IllegalStateException("bound expression replacement input has changed");
                    }
                    final int mark = leafMark();
                    final Function leaf;
                    if (BindableColumn.isBindableType(column.getDataType())) {
                        final BindableColumn bindable = BindableColumn.newInstance(column.getColumnId(), column.getDataType(), input.isSymbolTableStatic(index));
                        currentPreparation.leaves.add(bindable);
                        leaf = bindable;
                    } else {
                        leaf = FunctionInstantiator.createColumnFunction(node.position, index, column.getDataType(), input);
                        currentPreparation.isRebuildRequired = true;
                    }
                    push(ctx.columns.next().of(column.getColumnId(), column.getDataType(), node.position,
                            column.isDirectReference(), column.isCast()), mark);
                    return leaf;
                }
            }
        }
        return null;
    }

    Function returnCastArgument(Function function, ObjList<Function> args, int position) {
        try {
            final BoundExpression argument = normalizeArgument(arguments.getQuick(0), function);
            // No runtime cast is needed, but diagnostics must retain the CAST's
            // source position without mutating the already captured argument.
            final BoundExpression repositioned = switch (argument) {
                case ColumnExpression column ->
                        ctx.columns.next().of(column.getColumnId(), column.getDataType(), position, false, true);
                case OuterColumnExpression outer ->
                        ctx.outerColumns.next().of(outer.getColumnId(), outer.getDataType(), position);
                case BindVariableExpression parameter ->
                        ctx.parameters.next().of(parameter.getName(), parameter.getDataType(), parameter.getFunctionFlags(), position, false);
                case FunctionExpression call -> ctx.functions.next().of(call, position);
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

    BoundExpression toBooleanSubquery(BoundExpression expression) {
        if (expression instanceof CursorExpression cursor && ColumnType.tagOf(cursor.getPlan().getOutput().getColumnCount() == 1
                ? cursor.getPlan().getOutput().getColumnType(0) : ColumnType.UNDEFINED) == ColumnType.BOOLEAN) {
            return cursors.next().ofBoolean(cursor, BoundExpression.RUNTIME_CONSTANT | cursor.getFunctionFlags()
                    & (BoundExpression.STABLE_WITHIN_EXECUTION | BoundExpression.NON_DETERMINISTIC));
        }
        return expression;
    }

    void validateFactory(FunctionFactoryDescriptor overload, ExpressionNode node, ObjList<Function> args) throws SqlException {
        if (node != aggregateRoot && node != windowRoot && overload.getFactory().isGroupBy()) {
            final SqlException exception = node != bindingRoot
                    ? SqlException.$(node.position, "Aggregate function cannot be passed as an argument")
                    : isBindingPredicate ? SqlException.$(node.position, "boolean expression expected")
                      : SqlException.$(node.position, "aggregate functions are not allowed in this context");
            Misc.freeObjList(args, exception);
            throw exception;
        }
        if (node == windowRoot && !overload.getFactory().isWindow()) {
            final SqlException exception = SqlException.$(node.position, "non-window function called in window context");
            Misc.freeObjList(args, exception);
            throw exception;
        }
        if (node != windowRoot && node != aggregateRoot && overload.getFactory().isWindow()) {
            if (isWindowArgument(node)) {
                if (nestedWindowPosition < 0) {
                    nestedWindowPosition = node.position;
                }
                return;
            }
            final SqlException exception = SqlException.emptyWindowContext(node.position);
            Misc.freeObjList(args, exception);
            throw exception;
        }
        if (node == aggregateRoot && !overload.getFactory().isGroupBy()
                && !(isBindingGroupByExpression && overload.isArrayElementWiseScalar())) {
            final IllegalStateException exception = new IllegalStateException("aggregate root is bound to a non-aggregate factory");
            Misc.freeObjList(args, exception);
            throw exception;
        }
    }

    void validateNode(ExpressionNode node) {
        if (node.type == ExpressionNode.QUERY && subqueryBinder != null) {
            return;
        }
        if (node.windowExpression != null && node != windowRoot
                || node.type != ExpressionNode.LITERAL && node.type != ExpressionNode.CONSTANT
                && node.type != ExpressionNode.FUNCTION && node.type != ExpressionNode.OPERATION
                && node.type != ExpressionNode.BIND_VARIABLE && node.type != ExpressionNode.MEMBER_ACCESS
                && node.type != ExpressionNode.ARRAY_CONSTRUCTOR && node.type != ExpressionNode.ARRAY_ACCESS
                && (node.type != ExpressionNode.SET_OPERATION
                || !SqlKeywords.isInKeyword(node.token) && !SqlKeywords.isBetweenKeyword(node.token))) {
            throw new IllegalStateException("unexpected expression node type");
        }
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

    void validateSubsampleArguments(ExpressionNode node, ObjList<Function> args, IntList positions) throws SqlException {
        if (node != windowRoot || node.windowExpression == null || !node.windowExpression.isSubsampleKeepFlag() || args == null) {
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
}
