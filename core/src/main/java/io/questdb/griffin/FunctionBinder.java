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

import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.GenericRecordMetadata;
import io.questdb.cairo.GeoHashes;
import io.questdb.cairo.ImplicitCastException;
import io.questdb.cairo.IndexType;
import io.questdb.cairo.MillisTimestampDriver;
import io.questdb.cairo.TableColumnMetadata;
import io.questdb.cairo.TimestampDriver;
import io.questdb.cairo.arr.ArrayView;
import io.questdb.cairo.sql.ArrayFunction;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.StaticSymbolTable;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.cairo.sql.VirtualRecord;
import io.questdb.griffin.engine.functions.AbstractGeoHashFunction;
import io.questdb.griffin.engine.functions.BooleanFunction;
import io.questdb.griffin.engine.functions.ByteFunction;
import io.questdb.griffin.engine.functions.CharFunction;
import io.questdb.griffin.engine.functions.CursorFunction;
import io.questdb.griffin.engine.functions.DateFunction;
import io.questdb.griffin.engine.functions.DoubleFunction;
import io.questdb.griffin.engine.functions.FloatFunction;
import io.questdb.griffin.engine.functions.GroupByFunction;
import io.questdb.griffin.engine.functions.IPv4Function;
import io.questdb.griffin.engine.functions.IntFunction;
import io.questdb.griffin.engine.functions.Long256Function;
import io.questdb.griffin.engine.functions.LongFunction;
import io.questdb.griffin.engine.functions.MonotonicTimestampFunction;
import io.questdb.griffin.engine.functions.RuntimeConstFunction;
import io.questdb.griffin.engine.functions.ScalarSubQueryBoundRefFunction;
import io.questdb.griffin.engine.functions.ShortFunction;
import io.questdb.griffin.engine.functions.StrFunction;
import io.questdb.griffin.engine.functions.SymbolFunction;
import io.questdb.griffin.engine.functions.TimestampFunction;
import io.questdb.griffin.engine.functions.UuidFunction;
import io.questdb.griffin.engine.functions.VarcharFunction;
import io.questdb.griffin.engine.functions.bool.BooleanSubQueryFunction;
import io.questdb.griffin.engine.functions.columns.RecordColumn;
import io.questdb.griffin.engine.functions.columns.SymbolColumn;
import io.questdb.griffin.engine.functions.constants.ArrayConstant;
import io.questdb.griffin.engine.functions.constants.BooleanConstant;
import io.questdb.griffin.engine.functions.constants.ByteConstant;
import io.questdb.griffin.engine.functions.constants.CharConstant;
import io.questdb.griffin.engine.functions.constants.ConstantFunction;
import io.questdb.griffin.engine.functions.constants.Constants;
import io.questdb.griffin.engine.functions.constants.DateConstant;
import io.questdb.griffin.engine.functions.constants.Decimal128Constant;
import io.questdb.griffin.engine.functions.constants.Decimal16Constant;
import io.questdb.griffin.engine.functions.constants.Decimal256Constant;
import io.questdb.griffin.engine.functions.constants.Decimal32Constant;
import io.questdb.griffin.engine.functions.constants.Decimal64Constant;
import io.questdb.griffin.engine.functions.constants.Decimal8Constant;
import io.questdb.griffin.engine.functions.constants.DecimalTypeConstant;
import io.questdb.griffin.engine.functions.constants.DoubleConstant;
import io.questdb.griffin.engine.functions.constants.FloatConstant;
import io.questdb.griffin.engine.functions.constants.GeoHashTypeConstant;
import io.questdb.griffin.engine.functions.constants.IPv4Constant;
import io.questdb.griffin.engine.functions.constants.IntConstant;
import io.questdb.griffin.engine.functions.constants.IntervalConstant;
import io.questdb.griffin.engine.functions.constants.Long256Constant;
import io.questdb.griffin.engine.functions.constants.LongConstant;
import io.questdb.griffin.engine.functions.constants.NullBinConstant;
import io.questdb.griffin.engine.functions.constants.NullConstant;
import io.questdb.griffin.engine.functions.constants.ShortConstant;
import io.questdb.griffin.engine.functions.constants.StrConstant;
import io.questdb.griffin.engine.functions.constants.SymbolConstant;
import io.questdb.griffin.engine.functions.constants.TimestampConstant;
import io.questdb.griffin.engine.functions.constants.UuidConstant;
import io.questdb.griffin.engine.functions.constants.VarcharConstant;
import io.questdb.griffin.engine.groupby.GroupByUtils;
import io.questdb.griffin.engine.window.WindowFunction;
import io.questdb.griffin.model.ExpressionNode;
import io.questdb.griffin.model.IntervalUtils;
import io.questdb.griffin.model.ScalarTimestampBoundHolder;
import io.questdb.griffin.plan.logical.BindVariableExpression;
import io.questdb.griffin.plan.logical.BoundExpression;
import io.questdb.griffin.plan.logical.ColumnExpression;
import io.questdb.griffin.plan.logical.ConstantExpression;
import io.questdb.griffin.plan.logical.CursorExpression;
import io.questdb.griffin.plan.logical.FunctionExpression;
import io.questdb.griffin.plan.logical.OuterColumnExpression;
import io.questdb.griffin.plan.logical.OutputSchema;
import io.questdb.griffin.plan.logical.ProjectPlan;
import io.questdb.griffin.plan.logical.TypeExpression;
import io.questdb.std.Chars;
import io.questdb.std.Decimal128;
import io.questdb.std.Decimal256;
import io.questdb.std.GenericLexer;
import io.questdb.std.IntHashSet;
import io.questdb.std.IntIntHashMap;
import io.questdb.std.IntList;
import io.questdb.std.Interval;
import io.questdb.std.Long256;
import io.questdb.std.Misc;
import io.questdb.std.Mutable;
import io.questdb.std.Numbers;
import io.questdb.std.NumericException;
import io.questdb.std.ObjList;
import io.questdb.std.ObjectPool;
import io.questdb.std.str.CharSink;
import io.questdb.std.str.Utf8Sequence;

import java.io.Closeable;

/**
 * Captures the existing parser's selected calls while it constructs them.
 * Descriptions belong to compilation; prepared roots belong to this scope until
 * instantiate() transfers them. An adopted function never retains the binder.
 */
public final class FunctionBinder implements Closeable, Mutable {
    private static final String NULL_PROBE_COLUMN = "null_probe";
    private final IntList argumentPositions = new IntList();
    private final ObjList<BoundExpression> arguments = new ObjList<>();
    private final ObjList<ExpressionNode> callArguments = new ObjList<>();
    private final ObjectPool<ColumnExpression> columns;
    private final ObjectPool<ConstantExpression> constants;
    private final ObjList<BoundExpression> conversionArguments = new ObjList<>(2);
    private final IntList conversionPositions = new IntList(2);
    private final ObjectPool<CursorExpression> cursors = new ObjectPool<>(CursorExpression.FACTORY, 4);
    private final Decimal128 decimal128 = new Decimal128();
    private final Decimal256 decimal256 = new Decimal256();
    private final ObjectPool<ExpressionNode> expressionNodes = new ObjectPool<>(ExpressionNode.FACTORY, 32);
    private final ObjList<BoundExpression> expressionStack = new ObjList<>();
    private final ObjectPool<FunctionExpression> functions = new ObjectPool<>(FunctionExpression.FACTORY, 16);
    private final ObjectPool<InstantiationArguments> instantiations = new ObjectPool<>(InstantiationArguments::new, 8);
    private final IntHashSet keySubqueryColumnIds = new IntHashSet();
    private final ObjList<Function> nullProbeConstants = new ObjList<>(1);
    private final GenericRecordMetadata nullProbeMetadata = new GenericRecordMetadata();
    private final VirtualRecord nullProbeRecord;
    private final OutputSchema nullProbeSchema;
    private final IntList outerColumnIds = new IntList();
    private final ObjectPool<OuterColumnExpression> outerColumns = new ObjectPool<>(OuterColumnExpression.FACTORY, 4);
    private final ObjList<OutputSchema> outerScopes = new ObjList<>();
    private final ObjectPool<BindVariableExpression> parameters = new ObjectPool<>(BindVariableExpression.FACTORY, 8);
    private final ObjList<CursorExpression> parkedCursors = new ObjList<>();
    private final ObjList<Function> parkedSubqueries = new ObjList<>();
    private final FunctionParser parser;
    private final ObjList<ExpressionNode> predicateConjuncts = new ObjList<>();
    private final ObjectPool<PreparationEntry> preparations = new ObjectPool<>(PreparationEntry::new, 4);
    private final ObjList<PreparationEntry> prepared = new ObjList<>();
    private final ResourceScope resources = new ResourceScope();
    private final ObjectPool<ObjList<BoundExpression>> rewriteArguments = new ObjectPool<>(ObjList::new, 8);
    private final ObjList<CursorExpression> sharedBoundCursors = new ObjList<>();
    private final ObjList<ScalarTimestampBoundHolder> sharedBoundHolders = new ObjList<>();
    private final ObjectPool<TypeExpression> types = new ObjectPool<>(TypeExpression.FACTORY, 8);
    private ExpressionNode aggregateRoot;
    private ExpressionNode bindingRoot;
    private int compiledLowerBoundIndex;
    private ExpressionNode compiledLowerBoundNode;
    private PreparationEntry currentPreparation;
    private OutputSchema input;
    private CharSequence inputAlias;
    private boolean isBindingGroupByExpression;
    private boolean isBindingPredicate;
    private IntHashSet nativeTimestampIds;
    private int nestedWindowPosition;
    private ObjList<? extends BoundExpression> replacementExpressions;
    private ObjList<ExpressionNode> replacementNodes;
    private SqlBinder subqueryBinder;
    private ExpressionNode windowRoot;
    private int workerCloneDepth;

    public FunctionBinder(FunctionParser parser) {
        this(parser, new ObjectPool<>(ColumnExpression.FACTORY, 16), new ObjectPool<>(ConstantExpression.FACTORY, 16), new OutputSchema());
    }

    /**
     * Allocates bound columns and constants from the given pools and empties them in {@link #clear()}.
     */
    FunctionBinder(
            FunctionParser parser,
            ObjectPool<ColumnExpression> columns,
            ObjectPool<ConstantExpression> constants,
            OutputSchema nullProbeSchema
    ) {
        this.parser = parser;
        this.columns = columns;
        this.constants = constants;
        this.nullProbeSchema = nullProbeSchema;
        nullProbeConstants.add(null);
        this.nullProbeRecord = new VirtualRecord(nullProbeConstants);
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
     * Binds under the caller's configured WindowContext. On success the prepared
     * window owns the context's partition functions; the caller closes them on
     * failure and clears the context after this call.
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
        try {
            resources.clear();
        } finally {
            Misc.freeObjListAndClear(parkedSubqueries);
            parkedCursors.clear();
            prepared.clear();
            preparations.clear();
            expressionStack.clear();
            arguments.clear();
            argumentPositions.clear();
            columns.clear();
            constants.clear();
            cursors.clear();
            functions.clear();
            instantiations.clear();
            outerColumnIds.clear();
            outerColumns.clear();
            outerScopes.clear();
            keySubqueryColumnIds.clear();
            parameters.clear();
            rewriteArguments.clear();
            sharedBoundCursors.clear();
            sharedBoundHolders.clear();
            types.clear();
            currentPreparation = null;
            input = null;
            inputAlias = null;
        }
    }

    @Override
    public void close() {
        clear();
    }

    public FunctionExpression commuteEquality(FunctionExpression original) {
        final FunctionFactoryDescriptor overload = original.getOverload().getCommutedEquality();
        if (original.getArgumentCount() != 2 || overload == null) {
            throw new IllegalArgumentException("registered binary equality required");
        }
        conversionArguments.clear();
        conversionPositions.clear();
        try {
            conversionArguments.add(original.argumentAt(1));
            conversionArguments.add(original.argumentAt(0));
            conversionPositions.add(original.getArgumentPosition(1));
            conversionPositions.add(original.getArgumentPosition(0));
            return functions.next().of(overload, conversionArguments, conversionPositions,
                    original.getDataType(), original.getFunctionFlags(), original.getPosition());
        } finally {
            conversionArguments.clear();
            conversionPositions.clear();
        }
    }

    /**
     * Copies an expression through a projection of plain columns, leaving its preparation with the original.
     */
    public BoundExpression copyRemappedColumns(BoundExpression expression, ProjectPlan projection) {
        rewriteArguments.clear();
        try {
            return remapColumns0(expression, projection, false);
        } finally {
            rewriteArguments.clear();
        }
    }

    /**
     * Transfers the unchanged prepared closure once, after assigning its final
     * input positions. The caller owns the returned function, including on later
     * cursor-construction failure. This overload requires an owned preparation;
     * the context-taking overload can reconstruct additional consumers.
     */
    public Function instantiate(BoundExpression expression, OutputSchema input) {
        if (hasArrayColumnLayoutDependency(expression)) {
            throw new IllegalStateException("array column function requires final-layout reconstruction");
        }
        for (int i = 0, n = prepared.size(); i < n; i++) {
            final PreparationEntry entry = prepared.getQuick(i);
            if (entry.expression == expression && entry.updateTargetType < 0 && entry.slot >= 0 && resources.resources.getQuick(entry.slot) != null) {
                if (entry.isRebuildRequired || entry.leaves.size() > 0 && requiresReconstruction(expression)) {
                    throw new IllegalStateException("bound function requires final-layout reconstruction");
                }
                return adoptPreparation(entry, input, null);
            }
        }
        throw new IllegalStateException("bound function is not owned");
    }

    /**
     * Adopts the original prepared root when it is available, otherwise builds an
     * independent closure from selected overloads. Used for rewritten expressions
     * and additional execution consumers; the returned root belongs to the caller.
     */
    public Function instantiate(BoundExpression expression, OutputSchema input, SqlExecutionContext executionContext) throws SqlException {
        return instantiate(expression, input, null, executionContext);
    }

    /**
     * Final metadata determines dictionary capabilities of the selected physical input.
     */
    public Function instantiate(
            BoundExpression expression,
            OutputSchema input,
            RecordMetadata metadata,
            SqlExecutionContext executionContext
    ) throws SqlException {
        if (metadata != null && metadata.getColumnCount() != input.getColumnCount()) {
            throw new IllegalStateException("bound function input metadata has changed");
        }
        instantiations.clear();
        try {
            return instantiateNew(expression, input, metadata, executionContext, true);
        } finally {
            instantiations.clear();
        }
    }

    /**
     * Builds reviewed aggregates with native column accessors after the physical
     * layout is final, preserving direct-input and static-symbol optimizations.
     * The unused preparation is closed; reconstruction uses selected registrations.
     * COUNT() can adopt its preparation unchanged. The caller
     * owns the returned root; metadata is borrowed only during construction.
     */
    public Function instantiateAggregate(
            FunctionExpression expression,
            OutputSchema input,
            RecordMetadata metadata,
            SqlExecutionContext executionContext
    ) throws SqlException {
        final FunctionFactoryDescriptor overload = expression.getOverload();
        assert expression.isAggregate();
        if (overload.isRowCount()) {
            return instantiate(expression, input, executionContext);
        }
        if (metadata == null || metadata.getColumnCount() != input.getColumnCount()) {
            throw new IllegalStateException("bound aggregate input metadata has changed");
        }
        closePreparation(expression);
        instantiations.clear();
        try {
            return instantiateNew(expression, input, metadata, executionContext, false);
        } finally {
            instantiations.clear();
        }
    }

    /**
     * Reconstructs a window under the caller's final WindowContext and physical
     * input layout. The returned window owns its arguments and partition functions.
     * On failure the caller still closes the context's partition function list.
     */
    public WindowFunction instantiateWindow(
            FunctionExpression expression,
            OutputSchema input,
            RecordMetadata metadata,
            SqlExecutionContext executionContext
    ) throws SqlException {
        assert expression.isWindow();
        if (executionContext.getWindowContext().isEmpty()) {
            throw SqlException.emptyWindowContext(expression.getPosition());
        }
        if (metadata == null || metadata.getColumnCount() != input.getColumnCount()) {
            throw new IllegalStateException("bound window input metadata has changed");
        }
        closePreparation(expression);
        instantiations.clear();
        try {
            return (WindowFunction) instantiateNew(expression, input, metadata, executionContext, false);
        } finally {
            instantiations.clear();
        }
    }

    public boolean isGroupBy(CharSequence name) {
        return parser.getFunctionFactoryCache().isGroupBy(name);
    }

    /**
     * Moves an expression through a projection of plain columns. Descriptions are
     * copied; an unadopted preparation follows the replacement and only its private
     * leaves change IDs. Call only when replacing the old expression occurrence.
     */
    public BoundExpression remapColumns(BoundExpression expression, ProjectPlan projection) {
        rewriteArguments.clear();
        try {
            return remapColumns0(expression, projection, true);
        } finally {
            rewriteArguments.clear();
        }
    }

    private static int conjunctionFlags(int leftFlags, int rightFlags) {
        int flags = leftFlags & rightFlags & (BoundExpression.CONSTANT | BoundExpression.STABLE_WITHIN_EXECUTION);
        flags |= (leftFlags | rightFlags) & BoundExpression.NON_DETERMINISTIC;
        if ((leftFlags & (BoundExpression.CONSTANT | BoundExpression.RUNTIME_CONSTANT)) != 0
                && (rightFlags & (BoundExpression.CONSTANT | BoundExpression.RUNTIME_CONSTANT)) != 0
                && ((leftFlags | rightFlags) & BoundExpression.RUNTIME_CONSTANT) != 0) {
            flags |= BoundExpression.RUNTIME_CONSTANT;
        }
        return flags;
    }

    private static Function createColumnFunction(int position, int index, int type, OutputSchema input) throws SqlException {
        if (ColumnType.tagOf(type) != ColumnType.RECORD) {
            return FunctionParser.createColumn(position, index, type, input.isSymbolTableStatic(index));
        }
        final OutputSchema record = input.getMetadata(index);
        if (record == null) {
            throw new IllegalStateException("record column has no metadata");
        }
        final GenericRecordMetadata metadata = new GenericRecordMetadata();
        for (int i = 0, n = record.getColumnCount(); i < n; i++) {
            metadata.add(new TableColumnMetadata(Chars.toString(record.getColumnName(i)), record.getColumnType(i)));
        }
        return new RecordColumn(index, metadata);
    }

    private static int functionFlags(Function function) {
        return (function.isConstant() ? BoundExpression.CONSTANT : 0)
                | (function.isRuntimeConstant() ? BoundExpression.RUNTIME_CONSTANT : 0)
                | (function.isNonDeterministic() ? BoundExpression.NON_DETERMINISTIC : 0)
                | (function.isStableWithinExecution() ? BoundExpression.STABLE_WITHIN_EXECUTION : 0);
    }

    private static int getColumnIndexQuiet(OutputSchema input, CharSequence qualifier, CharSequence name, int lo, int hi) {
        final int index = input.getColumnIndexQuiet(qualifier, name, lo, hi);
        return index != -1 || !SqlUtil.isQuoteProtectedAlias(name, lo, hi)
                ? index : input.getColumnIndexQuiet(qualifier, name, lo + 1, hi - 1);
    }

    private static boolean hasArrayColumnLayoutDependency(BoundExpression expression) {
        if (expression instanceof FunctionExpression call) {
            if (call.getOverload().isArrayColumnLayoutSensitive()) {
                return true;
            }
            for (int i = 0; i < call.getArgumentCount(); i++) {
                if (hasArrayColumnLayoutDependency(call.argumentAt(i))) {
                    return true;
                }
            }
        }
        return false;
    }

    private static boolean isBindableType(int type) {
        return switch (ColumnType.tagOf(type)) {
            case ColumnType.ARRAY, ColumnType.TIMESTAMP, ColumnType.STRING, ColumnType.SYMBOL, ColumnType.VARCHAR,
                 ColumnType.BYTE, ColumnType.SHORT, ColumnType.CHAR, ColumnType.DATE, ColumnType.IPv4, ColumnType.INT,
                 ColumnType.BOOLEAN, ColumnType.LONG, ColumnType.LONG256, ColumnType.UUID, ColumnType.GEOBYTE,
                 ColumnType.GEOSHORT, ColumnType.GEOINT, ColumnType.GEOLONG, ColumnType.FLOAT, ColumnType.DOUBLE ->
                    true;
            default -> false;
        };
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

    private static boolean isTemporalComparisonOperator(CharSequence operator) {
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

    private static CharSequence literalText(BoundExpression expression) {
        if (!(expression instanceof ConstantExpression constant)) {
            return null;
        }
        return switch (ColumnType.tagOf(constant.getDataType())) {
            case ColumnType.STRING, ColumnType.SYMBOL -> constant.getStrValue();
            case ColumnType.VARCHAR -> constant.getVarcharValue() == null ? null : constant.getVarcharValue().asAsciiCharSequence();
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
    private static BindableColumn newColumn(int columnId, int type, boolean isSymbolTableStatic) {
        return switch (ColumnType.tagOf(type)) {
            case ColumnType.ARRAY -> new BindableArrayColumn(columnId, type);
            case ColumnType.TIMESTAMP -> new BindableTimestampColumn(columnId, type);
            case ColumnType.STRING -> new BindableStrColumn(columnId);
            case ColumnType.SYMBOL -> new BindableSymbolColumn(columnId, isSymbolTableStatic);
            case ColumnType.VARCHAR -> new BindableVarcharColumn(columnId);
            case ColumnType.BYTE -> new BindableByteColumn(columnId);
            case ColumnType.SHORT -> new BindableShortColumn(columnId);
            case ColumnType.CHAR -> new BindableCharColumn(columnId);
            case ColumnType.DATE -> new BindableDateColumn(columnId);
            case ColumnType.IPv4 -> new BindableIPv4Column(columnId);
            case ColumnType.INT -> new BindableIntColumn(columnId);
            case ColumnType.BOOLEAN -> new BindableBooleanColumn(columnId);
            case ColumnType.LONG -> new BindableLongColumn(columnId);
            case ColumnType.LONG256 -> new BindableLong256Column(columnId);
            case ColumnType.UUID -> new BindableUuidColumn(columnId);
            case ColumnType.GEOBYTE, ColumnType.GEOSHORT, ColumnType.GEOINT, ColumnType.GEOLONG ->
                    new BindableGeoHashColumn(columnId, type);
            case ColumnType.FLOAT -> new BindableFloatColumn(columnId);
            case ColumnType.DOUBLE -> new BindableDoubleColumn(columnId);
            default -> throw new IllegalStateException("column type is not bindable");
        };
    }

    private static ColumnExpression projectionColumn(ProjectPlan projection, int columnId, int type) {
        final int index = projection.getOutput().getColumnIndexById(columnId);
        if (index < 0 || !(projection.getExpressions().getQuick(index) instanceof ColumnExpression column)
                || column.getDataType() != type) {
            throw new IllegalArgumentException("column-only projection with unchanged types required");
        }
        return column;
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

    private static boolean requiresReconstruction(BoundExpression expression) {
        if (expression instanceof FunctionExpression call) {
            final FunctionFactoryDescriptor overload = call.getOverload();
            if (!overload.isRelocatableScalar() && !overload.isArrayColumnLayoutSensitive()) {
                return true;
            }
            for (int i = 0, n = call.getArgumentCount(); i < n; i++) {
                if (requiresReconstruction(call.argumentAt(i))) {
                    return true;
                }
            }
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

    private Function adoptPreparation(PreparationEntry entry, OutputSchema input, RecordMetadata metadata) {
        if (((Function) resources.resources.getQuick(entry.slot)).isConstant()) {
            final Function function = (Function) resources.detach(entry.slot);
            entry.slot = -1;
            return function;
        }
        for (int k = 0, count = entry.leaves.size(); k < count; k++) {
            final BindableColumn leaf = entry.leaves.getQuick(k);
            // Audited NULL folds close discarded operands. These are borrows,
            // never separately owned leaves; a dead leaf needs no input slot.
            // A fold that drops an operand without closing it leaves a leaf the
            // bound description no longer reads.
            if (leaf.isOpen() && references(entry.expression, leaf.getColumnId())) {
                final int index = input.getColumnIndexById(leaf.getColumnId());
                if (index < 0 || input.getColumnType(index) != leaf.getType()
                        || metadata != null && metadata.getColumnType(index) != leaf.getType()
                        || leaf instanceof SymbolFunction symbol && symbol.isSymbolTableStatic() != (metadata == null ? input.isSymbolTableStatic(index) : metadata.isSymbolTableStatic(index))) {
                    throw new IllegalStateException("bound function input has changed");
                }
            }
        }
        for (int k = 0, count = entry.leaves.size(); k < count; k++) {
            final BindableColumn leaf = entry.leaves.getQuick(k);
            if (leaf.isOpen() && references(entry.expression, leaf.getColumnId())) {
                leaf.setColumnIndex(input.getColumnIndexById(leaf.getColumnId()));
            }
        }
        final Function function = (Function) resources.detach(entry.slot);
        entry.slot = -1;
        return function;
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
        final PreparationEntry entry = preparations.next();
        entry.slot = resources.reserve();
        prepared.add(entry);
        currentPreparation = entry;
        this.input = input;
        this.inputAlias = inputAlias;
        expressionStack.clear();
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
            final Function function = parser.parseFunction(node, executionContext, this);
            resources.own(entry.slot, function);
            if (preferredType != ColumnType.UNDEFINED
                    && (isUpdateAssignment ? ColumnType.isUndefined(function.getType()) : function.isUndefined())) {
                function.assignType(preferredType, executionContext.getBindVariableService());
                finish(function);
            }
            assert expressionStack.size() == 1;
            entry.expression = expressionStack.getQuick(0);
            return entry.expression;
        } catch (Throwable th) {
            // The parser owns partial roots; only completed roots enter this scope.
            resources.closeOwned(th);
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
            argumentPositions.clear();
            expressionStack.clear();
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

    private void closePreparation(BoundExpression expression) {
        for (int i = 0, n = prepared.size(); i < n; i++) {
            final PreparationEntry entry = prepared.getQuick(i);
            if (entry.expression == expression && entry.slot >= 0 && resources.resources.getQuick(entry.slot) != null) {
                final Closeable preparation = resources.detach(entry.slot);
                entry.slot = -1;
                Misc.free(preparation);
                break;
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

    private BoundExpression conjunction(FunctionFactoryDescriptor overload, BoundExpression left, BoundExpression right, int position) {
        if (left == null) {
            return right;
        }
        if (right == null) {
            return left;
        }
        // Match the selected factory's constant branches without constructing a
        // disposable function or taking ownership of a prepared child instance.
        if (left instanceof ConstantExpression constant) {
            // TRUE returns the unchanged right function, even raw NULL. A false
            // argument instead makes the factory return BOOLEAN FALSE.
            return constant.getLongValue() != 0 ? right
                    : left.getDataType() == ColumnType.BOOLEAN ? left : constants.next().ofBoolean(false, position);
        }
        if (right instanceof ConstantExpression constant) {
            return constant.getLongValue() != 0 ? left
                    : right.getDataType() == ColumnType.BOOLEAN ? right : constants.next().ofBoolean(false, position);
        }
        final int flags = conjunctionFlags(left.getFunctionFlags(), right.getFunctionFlags());
        conversionArguments.clear();
        conversionPositions.clear();
        try {
            conversionArguments.add(left);
            conversionArguments.add(right);
            conversionPositions.add(left.getPosition());
            conversionPositions.add(right.getPosition());
            return functions.next().of(overload, conversionArguments, conversionPositions,
                    ColumnType.BOOLEAN, flags, position);
        } finally {
            conversionArguments.clear();
            conversionPositions.clear();
        }
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

    private ConstantExpression constant(Function function, int position) {
        return switch (ColumnType.tagOf(function.getType())) {
            case ColumnType.TIMESTAMP ->
                    constants.next().ofTimestamp(function.getTimestamp(null), function.getType(), position);
            case ColumnType.STRING -> constants.next().ofString(Chars.toString(function.getStrA(null)), position);
            case ColumnType.SYMBOL -> constants.next().ofSymbol(Chars.toString(function.getSymbol(null)), position);
            case ColumnType.VARCHAR -> constants.next().ofVarchar(function.getVarcharA(null), position);
            case ColumnType.BYTE -> constants.next().ofByte(function.getByte(null), position);
            case ColumnType.SHORT -> constants.next().ofShort(function.getShort(null), position);
            case ColumnType.DATE -> constants.next().ofDate(function.getDate(null), position);
            case ColumnType.IPv4 -> constants.next().ofIPv4(function.getIPv4(null), position);
            case ColumnType.CHAR -> constants.next().ofChar(function.getChar(null), position);
            case ColumnType.BOOLEAN -> constants.next().ofBoolean(function.getBool(null), position);
            case ColumnType.INT -> constants.next().ofInt(function.getInt(null), position);
            case ColumnType.LONG -> constants.next().ofLong(function.getLong(null), position);
            case ColumnType.LONG256 -> constants.next().ofLong256(function.getLong256A(null), position);
            case ColumnType.UUID ->
                    constants.next().ofUuid(function.getLong128Lo(null), function.getLong128Hi(null), position);
            case ColumnType.GEOBYTE, ColumnType.GEOSHORT, ColumnType.GEOINT, ColumnType.GEOLONG ->
                    constants.next().ofGeoHash(GeoHashes.getGeoLong(function.getType(), function, null), function.getType(), position);
            case ColumnType.FLOAT -> constants.next().ofFloat(function.getFloat(null), position);
            case ColumnType.DOUBLE -> constants.next().ofDouble(function.getDouble(null), position);
            case ColumnType.NULL -> constants.next().ofNull(position);
            case ColumnType.DECIMAL8 ->
                    constants.next().ofDecimal(function.getType(), 0, 0, 0, function.getDecimal8(null), position);
            case ColumnType.DECIMAL16 ->
                    constants.next().ofDecimal(function.getType(), 0, 0, 0, function.getDecimal16(null), position);
            case ColumnType.DECIMAL32 ->
                    constants.next().ofDecimal(function.getType(), 0, 0, 0, function.getDecimal32(null), position);
            case ColumnType.DECIMAL64 ->
                    constants.next().ofDecimal(function.getType(), 0, 0, 0, function.getDecimal64(null), position);
            case ColumnType.DECIMAL128 -> {
                function.getDecimal128(null, decimal128);
                yield constants.next().ofDecimal(function.getType(), 0, 0, decimal128.getHigh(), decimal128.getLow(), position);
            }
            case ColumnType.DECIMAL256 -> {
                function.getDecimal256(null, decimal256);
                yield constants.next().ofDecimal(function.getType(), decimal256.getHh(), decimal256.getHl(),
                        decimal256.getLh(), decimal256.getLl(), position);
            }
            case ColumnType.INTERVAL -> {
                final Interval interval = function.getInterval(null);
                yield constants.next().ofInterval(interval.getLo(), interval.getHi(), function.getType(), position);
            }
            case ColumnType.BINARY -> {
                if (function.getBin(null) != null) {
                    throw new IllegalStateException("non-null BINARY constant");
                }
                yield constants.next().ofBinaryNull(position);
            }
            default -> throw new IllegalStateException("unexpected constant type");
        };
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
                expressionStack.add(outerColumns.next().of(columnId, type, node.position));
                return isBindableType(type) ? newColumn(columnId, type, scope.isSymbolTableStatic(index))
                        : createColumnFunction(node.position, index, type, scope);
            }
        }
        return null;
    }

    private boolean hasSharedBound(BoundExpression expression) {
        if (sharedBoundCursors.size() == 0) {
            return false;
        }
        if (expression instanceof CursorExpression cursor) {
            return sharedBoundCursors.indexOf(cursor) >= 0;
        }
        if (expression instanceof FunctionExpression call) {
            for (int i = 0, n = call.getArgumentCount(); i < n; i++) {
                if (hasSharedBound(call.argumentAt(i))) {
                    return true;
                }
            }
        }
        return false;
    }

    private boolean hasSharedBoundArgument(FunctionExpression call) {
        if (sharedBoundCursors.size() > 0) {
            for (int i = 0, n = call.getArgumentCount(); i < n; i++) {
                if (call.argumentAt(i) instanceof CursorExpression cursor && sharedBoundCursors.indexOf(cursor) >= 0) {
                    return true;
                }
            }
        }
        return false;
    }

    private Function instantiateNew(
            BoundExpression expression,
            OutputSchema input,
            RecordMetadata metadata,
            SqlExecutionContext executionContext,
            boolean isAdoptionAllowed
    ) throws SqlException {
        if (isAdoptionAllowed && !hasSharedBound(expression)) {
            // Independently bound conjuncts may now belong to a new AND
            // description. Adopt each exact owned root once; the parent's frame
            // owns it after detachment, including if a later sibling fails.
            for (int i = 0, n = prepared.size(); i < n; i++) {
                final PreparationEntry entry = prepared.getQuick(i);
                if (entry.expression == expression && entry.updateTargetType < 0 && entry.slot >= 0 && resources.resources.getQuick(entry.slot) != null) {
                    if (isPreparationCompatible(entry, input, metadata)) {
                        return adoptPreparation(entry, input, metadata);
                    }
                    final Closeable unused = resources.detach(entry.slot);
                    entry.slot = -1;
                    Misc.free(unused);
                    break;
                }
            }
        }
        if (expression instanceof ColumnExpression column) {
            final int index = input.getColumnIndexById(column.getColumnId());
            if (index < 0 || input.getColumnType(index) != column.getDataType()) {
                throw new IllegalStateException("bound function input has changed");
            }
            if (metadata != null) {
                if (metadata.getColumnType(index) != column.getDataType()) {
                    throw new IllegalStateException("bound aggregate input type has changed");
                }
                return FunctionParser.createColumn(column.getPosition(), index, metadata);
            }
            if (ColumnType.isArray(column.getDataType()) || !isBindableType(column.getDataType())) {
                return createColumnFunction(column.getPosition(), index, column.getDataType(), input);
            }
            final BindableColumn leaf = newColumn(column.getColumnId(), column.getDataType(), input.isSymbolTableStatic(index));
            leaf.setColumnIndex(index);
            return leaf;
        }
        if (expression instanceof ConstantExpression constant) {
            return switch (ColumnType.tagOf(constant.getDataType())) {
                case ColumnType.TIMESTAMP ->
                        TimestampConstant.newInstance(constant.getLongValue(), constant.getDataType());
                case ColumnType.STRING -> StrConstant.fromValue(constant.getStrValue());
                case ColumnType.SYMBOL -> SymbolConstant.fromValue(constant.getStrValue());
                case ColumnType.VARCHAR -> VarcharConstant.fromValue(constant.getVarcharValue());
                case ColumnType.BYTE -> ByteConstant.newInstance((byte) constant.getLongValue());
                case ColumnType.SHORT -> ShortConstant.newInstance((short) constant.getLongValue());
                case ColumnType.DATE -> DateConstant.newInstance(constant.getLongValue());
                case ColumnType.IPv4 -> IPv4Constant.newInstance((int) constant.getLongValue());
                case ColumnType.CHAR -> CharConstant.newInstance((char) constant.getLongValue());
                case ColumnType.BOOLEAN -> BooleanConstant.of(constant.getLongValue() != 0);
                case ColumnType.INT -> IntConstant.newInstance((int) constant.getLongValue());
                case ColumnType.LONG -> LongConstant.newInstance(constant.getLongValue());
                case ColumnType.LONG256 -> new Long256Constant(constant.getLong256Value());
                case ColumnType.UUID -> new UuidConstant(constant.getLong128Lo(), constant.getLong128Hi());
                case ColumnType.GEOBYTE, ColumnType.GEOSHORT, ColumnType.GEOINT, ColumnType.GEOLONG ->
                        Constants.getGeoHashConstantWithType(constant.getLongValue(), constant.getDataType());
                case ColumnType.FLOAT -> new FloatConstant(constant.getFloatValue());
                case ColumnType.DOUBLE -> new DoubleConstant(constant.getDoubleValue());
                case ColumnType.NULL -> NullConstant.NULL;
                case ColumnType.DECIMAL8 ->
                        new Decimal8Constant((byte) constant.getLongValue(), constant.getDataType());
                case ColumnType.DECIMAL16 ->
                        new Decimal16Constant((short) constant.getLongValue(), constant.getDataType());
                case ColumnType.DECIMAL32 ->
                        new Decimal32Constant((int) constant.getLongValue(), constant.getDataType());
                case ColumnType.DECIMAL64 -> new Decimal64Constant(constant.getLongValue(), constant.getDataType());
                case ColumnType.DECIMAL128 ->
                        new Decimal128Constant(constant.getDecimalLh(), constant.getLongValue(), constant.getDataType());
                case ColumnType.DECIMAL256 -> new Decimal256Constant(constant.getDecimalHh(), constant.getDecimalHl(),
                        constant.getDecimalLh(), constant.getLongValue(), constant.getDataType());
                case ColumnType.INTERVAL ->
                        new IntervalConstant(constant.getLongValue(), constant.getIntervalHi(), constant.getDataType());
                case ColumnType.BINARY -> NullBinConstant.INSTANCE;
                default -> throw new IllegalStateException("unexpected constant type");
            };
        }
        if (expression instanceof TypeExpression type) {
            final int dataType = type.getDataType();
            if (ColumnType.isGeoHash(dataType)) {
                return GeoHashTypeConstant.getInstanceByPrecision(ColumnType.getGeoHashBits(dataType));
            }
            if (ColumnType.isDecimal(dataType)) {
                return new DecimalTypeConstant(ColumnType.getDecimalPrecision(dataType), ColumnType.getDecimalScale(dataType));
            }
            return Constants.getTypeConstant(dataType);
        }
        if (expression instanceof CursorExpression cursor) {
            final int shared = sharedBoundCursors.indexOf(cursor);
            if (shared >= 0) {
                return new ScalarSubQueryBoundRefFunction(sharedBoundHolders.getQuick(shared));
            }
            final Function function = instantiateSubquery(cursor, executionContext);
            return cursor.isBoolean() ? BooleanSubQueryFunction.maybeWrap(function, cursor.getPosition()) : function;
        }
        if (expression instanceof BindVariableExpression parameter) {
            final Function function = parser.createBindVariable(executionContext, parameter.getPosition(),
                    parameter.getName(), ExpressionNode.BIND_VARIABLE);
            try {
                if (function.isUndefined()) {
                    function.assignType(parameter.getDataType(), executionContext.getBindVariableService());
                }
                if (function.getType() != parameter.getDataType()) {
                    throw SqlException.$(parameter.getPosition(), "bind variable type has changed: ").put(parameter.getName());
                }
                return function;
            } catch (Throwable th) {
                Misc.free(function, th);
                throw th;
            }
        }
        if (!(expression instanceof FunctionExpression call)) {
            throw new IllegalStateException("unexpected bound expression");
        }
        final InstantiationArguments frame = instantiations.next();
        final int count = call.getArgumentCount();
        frame.functions.setPos(count);
        frame.positions.setPos(count);
        try {
            boolean allConstOrRuntimeConst = true;
            boolean anyRuntimeConst = false;
            for (int i = 0; i < count; i++) {
                // Reserve every slot before construction; each returned child is
                // immediately owned by this frame until its factory consumes it.
                final Function child = instantiateNew(call.argumentAt(i), input, metadata, executionContext, isAdoptionAllowed);
                frame.functions.setQuick(i, child);
                frame.positions.setQuick(i, call.getArgumentPosition(i));
                final boolean runtimeConstant = child.isRuntimeConstant();
                allConstOrRuntimeConst &= child.isConstant() || runtimeConstant;
                anyRuntimeConst |= runtimeConstant;
            }
            // Match FunctionParser's runtime-constant boundary treatment. These
            // wrappers own the original child and preserve its semantic type.
            if (!(allConstOrRuntimeConst && anyRuntimeConst)) {
                for (int i = 0; i < count; i++) {
                    final Function child = frame.functions.getQuick(i);
                    if (RuntimeConstFunction.isFoldable(child)) {
                        frame.functions.setQuick(i, RuntimeConstFunction.newInstance(child));
                    }
                }
            }
            FunctionFactoryDescriptor overload = call.getOverload();
            final boolean hasSharedBound = hasSharedBoundArgument(call);
            if (hasSharedBound) {
                overload = sharedBoundOverload(overload);
            }
            Function function = parser.createFunction(overload, call.getPosition(), overload.getName(),
                    count == 0 ? null : frame.functions, count == 0 ? null : frame.positions, executionContext);
            if (hasSharedBound) {
                return function;
            }
            if (ColumnType.isArray(function.getType()) && function.isConstant()) {
                function = parser.functionToConstant(function);
            }
            try {
                // CAST can type an empty array without retaining a cast node.
                if (function instanceof ArrayConstant && ColumnType.isUndefined(function.getType())
                        && ColumnType.isArray(call.getDataType()) && function.getType() != call.getDataType()) {
                    function.assignType(call.getDataType(), executionContext.getBindVariableService());
                }
                // Serially generated sub-queries may prove a stability their parallel counterparts
                // do not; the bound flags stay the conservative ones optimisations relied on.
                final int flags = call.getFunctionFlags();
                if (function.getType() != call.getDataType()
                        || (functionFlags(function) & (flags | ~BoundExpression.STABLE_WITHIN_EXECUTION)) != flags) {
                    throw new IllegalStateException("bound function semantics have changed");
                }
                return function;
            } catch (Throwable th) {
                Misc.free(function, th);
                throw th;
            }
        } catch (Throwable th) {
            Misc.freeObjList(frame.functions, th);
            throw th;
        } finally {
            frame.clear();
        }
    }

    private boolean isNativeTimestampColumn(int columnId) {
        return nativeTimestampIds != null ? nativeTimestampIds.contains(columnId)
                : input.getTimestampIndex() >= 0 && columnId == input.getColumnId(input.getTimestampIndex());
    }

    private boolean isPreparationCompatible(PreparationEntry entry, OutputSchema input, RecordMetadata metadata) {
        if (entry.isRebuildRequired || hasArrayColumnLayoutDependency(entry.expression)
                || entry.leaves.size() > 0 && requiresReconstruction(entry.expression)) {
            return false;
        }
        for (int i = 0, n = entry.leaves.size(); i < n; i++) {
            final BindableColumn leaf = entry.leaves.getQuick(i);
            if (leaf.isOpen() && leaf instanceof SymbolFunction symbol && references(entry.expression, leaf.getColumnId())) {
                final int index = input.getColumnIndexById(leaf.getColumnId());
                if (index < 0 || input.getColumnType(index) != leaf.getType()
                        || metadata != null && metadata.getColumnType(index) != leaf.getType()) {
                    throw new IllegalStateException("bound function input has changed");
                }
                if (symbol.isSymbolTableStatic() != (metadata == null ? input.isSymbolTableStatic(index) : metadata.isSymbolTableStatic(index))) {
                    return false;
                }
            }
        }
        return true;
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

    private ConstantExpression markSource(ConstantExpression folded, FunctionFactoryDescriptor overload, int type, int flags, int position) {
        return markSource(folded, functions.next().of(overload, arguments, argumentPositions, type, flags, position));
    }

    private BoundExpression normalizeArgument(BoundExpression expression, Function function) {
        if (function instanceof TypeConstant) {
            return expression;
        }
        if (expression instanceof CursorExpression cursor && function instanceof BooleanSubQueryFunction) {
            return cursors.next().ofBoolean(cursor, functionFlags(function));
        }
        if (ColumnType.isArray(function.getType()) && expression instanceof FunctionExpression call
                && (call.getDataType() != function.getType() || call.getFunctionFlags() != functionFlags(function))) {
            conversionArguments.clear();
            conversionPositions.clear();
            try {
                for (int i = 0; i < call.getArgumentCount(); i++) {
                    conversionArguments.add(call.argumentAt(i));
                    conversionPositions.add(call.getArgumentPosition(i));
                }
                return functions.next().of(call.getOverload(), conversionArguments, conversionPositions,
                        function.getType(), functionFlags(function), call.getPosition());
            } finally {
                conversionArguments.clear();
                conversionPositions.clear();
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
            return parameters.next().of(parameter.getName(), function.getType(), functionFlags(function), parameter.getPosition(), parameter.isDirectReference());
        }
        return expression;
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

    private BoundExpression remapColumns0(BoundExpression expression, ProjectPlan projection, boolean isMoving) {
        if (expression instanceof ColumnExpression column) {
            final ColumnExpression input = projectionColumn(projection, column.getColumnId(), column.getDataType());
            final boolean isDirectReference = column.isDirectReference() && input.isDirectReference();
            final boolean isCast = column.isCast() || input.isCast();
            final BoundExpression replacement = input.getColumnId() == column.getColumnId()
                    && isDirectReference == column.isDirectReference() && isCast == column.isCast() ? column
                    : columns.next().of(input.getColumnId(), column.getDataType(), column.getPosition(), isDirectReference, isCast);
            if (isMoving) {
                retargetPreparation(expression, replacement, projection);
            }
            return replacement;
        }
        if (expression instanceof FunctionExpression call) {
            final ObjList<BoundExpression> args = rewriteArguments.next();
            final int count = call.getArgumentCount();
            args.setPos(count);
            boolean changed = false;
            for (int i = 0; i < count; i++) {
                final BoundExpression original = call.argumentAt(i);
                final BoundExpression replacement = remapColumns0(original, projection, isMoving);
                args.setQuick(i, replacement);
                changed |= original != replacement;
            }
            if (changed) {
                final FunctionExpression replacement = functions.next().of(call, args);
                args.clear();
                if (isMoving) {
                    retargetPreparation(expression, replacement, projection);
                }
                return replacement;
            }
            args.clear();
        }
        return expression;
    }

    private void retargetPreparation(BoundExpression expression, BoundExpression replacement, ProjectPlan projection) {
        if (replacement != expression) {
            for (int i = 0, n = prepared.size(); i < n; i++) {
                final PreparationEntry entry = prepared.getQuick(i);
                if (entry.expression == expression && entry.slot >= 0 && resources.resources.getQuick(entry.slot) != null) {
                    // Validate the entire live closure before changing any leaf.
                    for (int k = 0, count = entry.leaves.size(); k < count; k++) {
                        final BindableColumn leaf = entry.leaves.getQuick(k);
                        if (leaf.isOpen()) {
                            final ColumnExpression column = projectionColumn(projection, leaf.getColumnId(), leaf.getType());
                            if (leaf instanceof SymbolFunction symbol && symbol.isSymbolTableStatic()
                                    != projection.getInput().getOutput().isSymbolTableStatic(
                                    projection.getInput().getOutput().getColumnIndexById(column.getColumnId()))) {
                                throw new IllegalArgumentException("bound symbol table capability has changed");
                            }
                        }
                    }
                    for (int k = 0, count = entry.leaves.size(); k < count; k++) {
                        final BindableColumn leaf = entry.leaves.getQuick(k);
                        if (leaf.isOpen()) {
                            leaf.setColumnId(projectionColumn(projection, leaf.getColumnId(), leaf.getType()).getColumnId());
                        }
                    }
                    entry.expression = replacement;
                    break;
                }
            }
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

    // A shared bound reads a TIMESTAMP value, so select the same operator's timestamp overload.
    private FunctionFactoryDescriptor sharedBoundOverload(FunctionFactoryDescriptor overload) {
        final ObjList<FunctionFactoryDescriptor> candidates = parser.getFunctionFactoryCache().getOverloadList(overload.getName());
        final int count = overload.getSigArgCount();
        for (int i = 0, n = candidates.size(); i < n; i++) {
            final FunctionFactoryDescriptor candidate = candidates.getQuick(i);
            if (candidate.getSigArgCount() != count) {
                continue;
            }
            boolean isMatch = true;
            for (int j = 0; j < count && isMatch; j++) {
                final short type = FunctionFactoryDescriptor.toTypeTag(overload.getArgTypeWithFlags(j));
                isMatch = FunctionFactoryDescriptor.toTypeTag(candidate.getArgTypeWithFlags(j))
                        == (type == ColumnType.CURSOR ? ColumnType.TIMESTAMP : type);
            }
            if (isMatch) {
                return candidate;
            }
        }
        throw new IllegalStateException("shared scalar bound has no timestamp overload");
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
                 ColumnType.DECIMAL128, ColumnType.DECIMAL256 -> types.next().of(function.getType(), position);
            default -> throw new IllegalStateException("unexpected CAST target type");
        };
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

    private void validateKeySubquery(Function cursorFunction) throws SqlException {
        if (!(arguments.getQuick(0) instanceof ColumnExpression column) || !keySubqueryColumnIds.contains(column.getColumnId())
                || !(arguments.getQuick(1) instanceof CursorExpression cursor) || cursor.isBoolean()) {
            return;
        }
        final RecordMetadata metadata = cursorFunction.getRecordCursorFactory().getMetadata();
        final int type = metadata.getColumnType(0);
        switch (ColumnType.tagOf(type)) {
            case ColumnType.STRING, ColumnType.SYMBOL, ColumnType.VARCHAR -> {
            }
            default ->
                    throw SqlException.position(subqueryBinder.getSubqueryFirstColumnPosition(cursor.getSubqueryIndex()))
                            .put("unsupported column type: ")
                            .put(metadata.getColumnName(0))
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

    /**
     * Consumes the input and returns its only owning root.
     */
    static Function convertUpdateFunction(FunctionParser parser, Function function, int targetType, int position) throws SqlException {
        if (targetType >= 0 && function.getType() != targetType && !ColumnType.isBuiltInWideningCast(function.getType(), targetType)) {
            final Function cast = parser.createImplicitCast(position, function, targetType);
            if (cast != null) {
                function = cast;
            }
        }
        return targetType == ColumnType.SYMBOL && function instanceof NullConstant ? SymbolConstant.NULL : function;
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

    static boolean isUnknownQualifier(CharSequence qualifier, OutputSchema input, CharSequence inputAlias) {
        if (input.hasColumnQualifiers()) {
            return !input.hasColumnQualifier(qualifier);
        }
        return !Chars.equalsIgnoreCaseNc(qualifier, inputAlias);
    }

    static int monotonicTimestampColumnId(Function function) {
        while (function instanceof MonotonicTimestampFunction monotonic) {
            function = monotonic.getTimestampArg();
        }
        return function instanceof BindableColumn column && ColumnType.isTimestamp(function.getType())
                ? column.getColumnId() : -1;
    }

    static boolean references(BoundExpression expression, int columnId) {
        if (expression instanceof ColumnExpression column) {
            return column.getColumnId() == columnId;
        }
        if (expression instanceof FunctionExpression call) {
            for (int i = 0, n = call.getArgumentCount(); i < n; i++) {
                if (references(call.argumentAt(i), columnId)) {
                    return true;
                }
            }
        }
        return false;
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
        for (int i = 0; i < count; i++) {
            final int index = expressionStack.size() - 1;
            arguments.add(expressionStack.getQuick(index));
            expressionStack.remove(index);
        }
    }

    void beginWorkerClones() {
        workerCloneDepth++;
    }

    void captureConstant(Function function, int position) {
        captureConstant(function, position, null);
    }

    void captureConstant(Function function, int position, CharSequence token) {
        try {
            if (function instanceof TypeConstant) {
                expressionStack.add(type(function, position));
                return;
            }
            final ConstantExpression constant = constant(function, position).markLiteral(null);
            final int tag = ColumnType.tagOf(constant.getDataType());
            expressionStack.add(token != null && (tag == ColumnType.DOUBLE || tag == ColumnType.FLOAT)
                    ? constant.withLiteralText(Chars.toString(token)) : constant);
        } catch (Throwable th) {
            Misc.free(function, th);
            throw th;
        }
    }

    /**
     * Records the parser's actual inserted cast; never resolves the overload again.
     */
    void captureImplicitConversion(int index, Function function, int position, Class<? extends FunctionFactory> factoryClass) {
        if (function.isConstant()) {
            arguments.setQuick(index, constant(function, position));
            return;
        }
        final ObjList<FunctionFactoryDescriptor> casts = parser.getFunctionFactoryCache().getOverloadList("cast");
        for (int i = 0, n = casts.size(); i < n; i++) {
            final FunctionFactoryDescriptor overload = casts.getQuick(i);
            if (overload.getFactory().getClass() == factoryClass) {
                conversionArguments.clear();
                conversionPositions.clear();
                try {
                    conversionArguments.add(arguments.getQuick(index));
                    conversionArguments.add(types.next().of(function.getType(), position));
                    conversionPositions.add(position);
                    conversionPositions.add(position);
                    arguments.setQuick(index, functions.next().of(overload, conversionArguments,
                            conversionPositions, function.getType(), functionFlags(function), position));
                    return;
                } finally {
                    conversionArguments.clear();
                    conversionPositions.clear();
                }
            }
        }
        throw new IllegalStateException("implicit conversion factory is not registered as a cast");
    }

    void captureParameter(Function function, ExpressionNode node, boolean isPredefined) {
        try {
            final BindVariableExpression parameter = parameters.next().of(node.token, function.getType(), functionFlags(function), node.position);
            expressionStack.add(isPredefined ? parameter.markPredefined() : parameter);
        } catch (Throwable th) {
            Misc.free(function, th);
            throw th;
        }
    }

    Throwable closePrepared(Throwable primary) {
        return resources.closeOwned(-1, primary);
    }

    /**
     * Combines independently bound conjuncts without constructing executable
     * children again. Their preparations remain separately owned until generation.
     */
    BoundExpression combineConjunction(BoundExpression left, BoundExpression right, int position) throws SqlException {
        if (left == null) {
            return right;
        }
        if (right == null) {
            return left;
        }
        final int leftType = left.getDataType();
        final int rightType = right.getDataType();
        if (leftType != ColumnType.BOOLEAN && leftType != ColumnType.NULL
                || rightType != ColumnType.BOOLEAN && rightType != ColumnType.NULL) {
            // Match the single AND registration's first mismatching argument.
            final BoundExpression invalid = leftType != ColumnType.BOOLEAN ? left : right;
            throw SqlException.$(invalid.getPosition(), "expression type mismatch, expected: BOOLEAN, actual: ")
                    .put(ColumnType.nameOf(invalid.getDataType()));
        }
        final ObjList<FunctionFactoryDescriptor> overloads = parser.getFunctionFactoryCache().getOverloadList("and");
        if (overloads != null) {
            for (int i = 0, n = overloads.size(); i < n; i++) {
                final FunctionFactoryDescriptor overload = overloads.getQuick(i);
                if (overload.getSigArgCount() == 2
                        && overload.getArgTypeWithFlags(0) == ColumnType.BOOLEAN
                        && overload.getArgTypeWithFlags(1) == ColumnType.BOOLEAN) {
                    // Preserve registry priority: an override must be reviewed,
                    // never silently skipped in favour of the built-in factory.
                    if (!overload.isAnd()) {
                        throw new IllegalStateException("AND is not bound to the built-in factory");
                    }
                    return conjunction(overload, left, right, position);
                }
            }
        }
        throw new IllegalStateException("AND is not registered");
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
        final Function leaf;
        if (isBindableType(type)) {
            final BindableColumn bindable = newColumn(columnId, type, input.isSymbolTableStatic(index));
            currentPreparation.leaves.add(bindable);
            leaf = bindable;
        } else {
            leaf = createColumnFunction(node.position, index, type, input);
            currentPreparation.isRebuildRequired = true;
        }
        expressionStack.add(columns.next().of(columnId, type, node.position));
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
        final CursorFunction function = new CursorFunction(subqueryBinder.claimSubquery(index));
        expressionStack.add(cursors.next().of(subqueryBinder.getSubqueryPlan(index), index, functionFlags(function), node.position));
        return function;
    }

    Function createFunction(
            FunctionFactoryDescriptor overload,
            ExpressionNode node,
            ObjList<Function> args,
            IntList positions,
            SqlExecutionContext executionContext
    ) throws SqlException {
        validateFactory(overload, node, args);
        try {
            final Function folded = canonicalizeTemporalComparison(node, args, positions);
            if (folded != null) {
                Misc.freeObjList(args);
                expressionStack.add(constants.next().ofBoolean(folded.getBool(null), node.position).markLiteral());
                return folded;
            }
            final int count = args == null ? 0 : args.size();
            if (count == 0) {
                arguments.clear();
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
                validateKeySubquery(args.getQuick(1));
            }
            if (requiresConditionalRebuild(overload, args)) {
                currentPreparation.isRebuildRequired = true;
            }
            argumentPositions.clear();
            if (positions != null) {
                argumentPositions.addAll(positions);
            }
        } catch (Throwable th) {
            Misc.freeObjList(args, th);
            throw th;
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
            } else if (first != null && isConnective(node.token) && (function == first || function == second)) {
                // A connective with a constant operand returns its other operand.
                expression = arguments.getQuick(function == first ? 0 : 1);
            } else {
                expression = functions.next().of(overload, arguments, argumentPositions,
                        function.getType(), functionFlags(function), node.position);
            }
            if (node.type == ExpressionNode.SET_OPERATION && expression instanceof FunctionExpression call) {
                call.markSetOperation();
            }
            expressionStack.add(node.token == "dateadd" && expression instanceof FunctionExpression call
                    ? functions.next().ofProjectedOffset(call) : expression);
            return function;
        } catch (Throwable th) {
            Misc.free(function, th);
            throw th;
        }
    }

    /**
     * Describes the argument-free window function {@code name}, such as {@code row_number}, without preparing
     * it; the generator builds windows under their final window context.
     */
    FunctionExpression describeWindowCall(CharSequence name, int position) {
        final ObjList<FunctionFactoryDescriptor> overloads = parser.getFunctionFactoryCache().getOverloadList(name);
        for (int i = 0, n = overloads == null ? 0 : overloads.size(); i < n; i++) {
            final FunctionFactoryDescriptor overload = overloads.getQuick(i);
            if (overload.getSigArgCount() == 0 && overload.getFactory().isWindow()) {
                conversionArguments.clear();
                conversionPositions.clear();
                return functions.next().of(overload, conversionArguments, conversionPositions, ColumnType.LONG, 0, position);
            }
        }
        throw new IllegalStateException("window function is not registered");
    }

    void endWorkerClones() {
        workerCloneDepth--;
    }

    void finish(Function function) {
        final int index = expressionStack.size() - 1;
        expressionStack.setQuick(index, normalizeArgument(expressionStack.getQuick(index), function));
    }

    void foldArgument(int index, Function function, int position) {
        if (function instanceof ConstantFunction && !(function instanceof TypeConstant)) {
            final BoundExpression argument = arguments.getQuick(index);
            if (ColumnType.isArray(function.getType())) {
                arguments.setQuick(index, normalizeArgument(argument, function));
            } else {
                arguments.setQuick(index, markSource(constant(function, position), argument));
            }
        }
    }

    RecordCursorFactory generateSubquery(CursorExpression cursor, SqlExecutionContext executionContext) throws SqlException {
        return subqueryBinder.generateSubquery(cursor.getSubqueryIndex(), cursor.getPosition(), executionContext);
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

    int getScalarBoundDepth() {
        return subqueryBinder.getScalarBoundDepth();
    }

    /**
     * True while a LATERAL body binds.
     */
    boolean hasOuterScope() {
        return outerScopes.size() > 0;
    }

    Function instantiateSubquery(CursorExpression cursor, SqlExecutionContext executionContext) throws SqlException {
        final int parked = parkedCursors.indexOf(cursor);
        if (parked >= 0) {
            final Function function = parkedSubqueries.getQuick(parked);
            parkedCursors.remove(parked);
            parkedSubqueries.remove(parked);
            return function;
        }
        if (workerCloneDepth == 0) {
            return new CursorFunction(subqueryBinder.generateSubquery(cursor.getSubqueryIndex(), cursor.getPosition(), executionContext));
        }
        // Worker clones receive the owner's sub-query state, so generating their copy serially keeps
        // nested sub-queries from compiling once per worker at every nesting level.
        final boolean isParallelFilter = executionContext.isParallelFilterEnabled();
        final boolean isParallelGroupBy = executionContext.isParallelGroupByEnabled();
        final boolean isParallelHorizonJoin = executionContext.isParallelHorizonJoinEnabled();
        final boolean isParallelTopK = executionContext.isParallelTopKEnabled();
        final boolean isParallelWindowJoin = executionContext.isParallelWindowJoinEnabled();
        executionContext.setParallelFilterEnabled(false);
        executionContext.setParallelGroupByEnabled(false);
        executionContext.setParallelHorizonJoinEnabled(false);
        executionContext.setParallelTopKEnabled(false);
        executionContext.setParallelWindowJoinEnabled(false);
        try {
            return new CursorFunction(subqueryBinder.generateSubquery(cursor.getSubqueryIndex(), cursor.getPosition(), executionContext));
        } finally {
            executionContext.setParallelFilterEnabled(isParallelFilter);
            executionContext.setParallelGroupByEnabled(isParallelGroupBy);
            executionContext.setParallelHorizonJoinEnabled(isParallelHorizonJoin);
            executionContext.setParallelTopKEnabled(isParallelTopK);
            executionContext.setParallelWindowJoinEnabled(isParallelWindowJoin);
        }
    }

    Function instantiateUpdateAssignment(BoundExpression expression, int targetType, OutputSchema input,
                                         RecordMetadata metadata, SqlExecutionContext executionContext) throws SqlException {
        if (metadata.getColumnCount() != input.getColumnCount()) {
            throw new IllegalStateException("bound function input metadata has changed");
        }
        for (int i = 0, n = prepared.size(); i < n; i++) {
            final PreparationEntry entry = prepared.getQuick(i);
            if (entry.expression == expression && entry.updateTargetType == targetType
                    && entry.slot >= 0 && resources.resources.getQuick(entry.slot) != null) {
                if (isPreparationCompatible(entry, input, metadata)) {
                    return adoptPreparation(entry, input, metadata);
                }
                final Closeable unused = resources.detach(entry.slot);
                entry.slot = -1;
                Misc.free(unused);
                break;
            }
        }
        return convertUpdateFunction(parser, instantiate(expression, input, metadata, executionContext),
                targetType, expression.getPosition());
    }

    /**
     * True when no replacement matches subtrees by AST identity, so binding may clone and reassociate the tree.
     */
    boolean isAstRewritable() {
        return replacementNodes == null || replacementNodes.size() == 0;
    }

    /**
     * Evaluates a folded equality on the NULL its column takes when a join NULL-extends it.
     */
    boolean isNullRejecting(FunctionExpression call, int columnArgument, SqlExecutionContext executionContext) {
        if (!"=".equals(call.getName()) || !(call.argumentAt(1 - columnArgument) instanceof ConstantExpression)) {
            return false;
        }
        final ColumnExpression column = (ColumnExpression) call.argumentAt(columnArgument);
        final int type = column.getDataType();
        nullProbeSchema.clear();
        nullProbeSchema.add(column.getColumnId(), NULL_PROBE_COLUMN, type, true);
        if (nullProbeMetadata.getColumnCount() == 0 || nullProbeMetadata.getColumnType(0) != type) {
            nullProbeMetadata.clear();
            nullProbeMetadata.add(new TableColumnMetadata(NULL_PROBE_COLUMN, type, IndexType.NONE, 0, false, null));
        }
        Function function = null;
        try {
            nullProbeConstants.setQuick(0, Constants.getNullConstant(type));
            function = instantiate(call, nullProbeSchema, nullProbeMetadata, executionContext);
            return !function.getBool(nullProbeRecord);
        } catch (CairoException | ImplicitCastException | SqlException | UnsupportedOperationException e) {
            return false;
        } finally {
            Misc.free(function);
            Misc.freeObjList(nullProbeConstants);
        }
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

    boolean isUnresolvedNoArgFunction(ExpressionNode node) {
        return findColumn(node, input, inputAlias) == -1 && parser.findNoArgFunction(node);
    }

    BoundExpression newFalseConstant(int position) {
        return constants.next().ofBoolean(false, position);
    }

    /**
     * The next consumer of the cursor adopts this generated sub-query.
     */
    void parkSubquery(CursorExpression cursor, Function subquery) {
        final int index = parkedCursors.indexOf(cursor);
        if (index < 0) {
            parkedCursors.add(cursor);
            parkedSubqueries.add(subquery);
        } else {
            Misc.free(parkedSubqueries.getQuick(index));
            parkedSubqueries.setQuick(index, subquery);
        }
    }

    void popOuterScope() {
        outerScopes.remove(outerScopes.size() - 1);
    }

    /**
     * Returns a borrowed converted root; its prepared slot retains ownership.
     */
    Function prepareUpdateAssignment(BoundExpression expression, int targetType) throws SqlException {
        for (int i = 0, n = prepared.size(); i < n; i++) {
            final PreparationEntry entry = prepared.getQuick(i);
            if (entry.expression == expression && entry.slot >= 0 && resources.resources.getQuick(entry.slot) != null) {
                if (entry.updateTargetType >= 0) {
                    throw new IllegalStateException("UPDATE assignment already prepared");
                }
                final int slot = resources.reserve();
                final Function original = (Function) resources.detach(entry.slot);
                entry.slot = -1;
                // convertUpdateFunction() frees the original root when the conversion fails
                final Function function = convertUpdateFunction(parser, original, targetType, expression.getPosition());
                resources.own(slot, function);
                entry.slot = slot;
                entry.updateTargetType = targetType;
                return function;
            }
        }
        throw new IllegalStateException("UPDATE assignment is not owned");
    }

    /**
     * Makes the columns of {@code scope} resolvable as outer columns until {@link #popOuterScope()}.
     */
    void pushOuterScope(OutputSchema scope) {
        outerScopes.add(scope);
    }

    /**
     * Returns the expression with each column and outer column the map holds read under its mapped id, as a
     * column. Unchanged sub-expressions are shared; a changed call is a fresh description without a preparation.
     */
    BoundExpression remapColumns(BoundExpression expression, IntIntHashMap columnIds) {
        if (expression instanceof ColumnExpression column) {
            final int columnId = columnIds.get(column.getColumnId());
            return columnId < 0 ? column
                    : columns.next().of(columnId, column.getDataType(), column.getPosition(), column.isDirectReference(), column.isCast());
        }
        if (expression instanceof OuterColumnExpression outer) {
            final int columnId = columnIds.get(outer.getColumnId());
            return columnId < 0 ? outer : columns.next().of(columnId, outer.getDataType(), outer.getPosition());
        }
        if (expression instanceof FunctionExpression call) {
            return remapColumns(call, columnIds);
        }
        return expression;
    }

    FunctionExpression remapColumns(FunctionExpression call, IntIntHashMap columnIds) {
        final ObjList<BoundExpression> args = rewriteArguments.next();
        boolean isChanged = false;
        for (int i = 0, n = call.getArgumentCount(); i < n; i++) {
            final BoundExpression argument = call.argumentAt(i);
            final BoundExpression remapped = remapColumns(argument, columnIds);
            args.add(remapped);
            isChanged |= remapped != argument;
        }
        return isChanged ? functions.next().of(call, args) : call;
    }

    BoundExpression replaceConjunction(FunctionExpression original, BoundExpression left, BoundExpression right) {
        assert original.isAnd()
                && original.getArgumentCount() == 2;
        if (left == original.argumentAt(0) && right == original.argumentAt(1)) {
            return original;
        }
        return conjunction(original.getOverload(), left, right, original.getPosition());
    }

    Function replaceNode(ExpressionNode node, SqlExecutionContext executionContext) throws SqlException {
        if (replacementNodes != null) {
            for (int i = 0, n = replacementNodes.size(); i < n; i++) {
                if (replacementNodes.getQuick(i) == node) {
                    final BoundExpression replacement = replacementExpressions.getQuick(i);
                    if (!(replacement instanceof ColumnExpression column)) {
                        // Leaves of a reconstructed subtree carry this layout's indexes.
                        if (replacement instanceof FunctionExpression) {
                            currentPreparation.isRebuildRequired = true;
                        }
                        final Function function = instantiateNew(replacement, input, null, executionContext, false);
                        expressionStack.add(replacement);
                        return function;
                    }
                    final int index = input.getColumnIndexById(column.getColumnId());
                    if (index < 0 || input.getColumnType(index) != column.getDataType()) {
                        throw new IllegalStateException("bound expression replacement input has changed");
                    }
                    final Function leaf;
                    if (isBindableType(column.getDataType())) {
                        final BindableColumn bindable = newColumn(column.getColumnId(), column.getDataType(), input.isSymbolTableStatic(index));
                        currentPreparation.leaves.add(bindable);
                        leaf = bindable;
                    } else {
                        leaf = createColumnFunction(node.position, index, column.getDataType(), input);
                        currentPreparation.isRebuildRequired = true;
                    }
                    expressionStack.add(columns.next().of(column.getColumnId(), column.getDataType(), node.position,
                            column.isDirectReference(), column.isCast()));
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
            expressionStack.add(switch (argument) {
                case ColumnExpression column ->
                        columns.next().of(column.getColumnId(), column.getDataType(), position, false, true);
                case OuterColumnExpression outer -> outerColumns.next().of(outer.getColumnId(), outer.getDataType(), position);
                case BindVariableExpression parameter ->
                        parameters.next().of(parameter.getName(), parameter.getDataType(), parameter.getFunctionFlags(), position, false);
                case FunctionExpression call -> functions.next().of(call, position);
                default -> constant(function, position);
            });
            args.clear();
            return function;
        } catch (Throwable th) {
            Misc.freeObjList(args, th);
            throw th;
        }
    }

    void setSubqueryBinder(SqlBinder subqueryBinder) {
        this.subqueryBinder = subqueryBinder;
    }

    /**
     * Later consumers of the cursor read the value its pruning bound publishes once per execution.
     */
    void shareScalarBound(CursorExpression cursor, ScalarTimestampBoundHolder holder) {
        final int index = sharedBoundCursors.indexOf(cursor);
        if (index < 0) {
            sharedBoundCursors.add(cursor);
            sharedBoundHolders.add(holder);
        } else {
            sharedBoundHolders.setQuick(index, holder);
        }
    }

    /**
     * Copies the expression with every reference to the column replaced; preparations stay with the original.
     */
    BoundExpression substituteColumn(BoundExpression expression, int columnId, BoundExpression replacement) {
        if (expression instanceof ColumnExpression column) {
            return column.getColumnId() == columnId ? replacement : expression;
        }
        if (expression instanceof FunctionExpression call && references(call, columnId)) {
            final ObjList<BoundExpression> args = rewriteArguments.next();
            for (int i = 0, n = call.getArgumentCount(); i < n; i++) {
                args.add(substituteColumn(call.argumentAt(i), columnId, replacement));
            }
            return functions.next().of(call, args);
        }
        return expression;
    }

    /**
     * Replaces each column with the expression the projection computes for it: an input column or
     * a timestamp offset. Substituted functions are fresh descriptions, so the projection keeps its
     * own preparation.
     */
    BoundExpression substituteProjection(BoundExpression expression, ProjectPlan projection) {
        if (expression instanceof ColumnExpression column) {
            final int index = projection.getOutput().getColumnIndexById(column.getColumnId());
            final BoundExpression projected = projection.getExpressions().getQuick(index);
            if (projected instanceof ColumnExpression projectedColumn) {
                return columns.next().of(projectedColumn.getColumnId(), column.getDataType(), column.getPosition(),
                        column.isDirectReference() && projectedColumn.isDirectReference(), column.isCast() || projectedColumn.isCast());
            }
            return functions.next().ofProjectedOffset((FunctionExpression) projected);
        }
        if (expression instanceof FunctionExpression call) {
            final ObjList<BoundExpression> args = rewriteArguments.next();
            for (int i = 0, n = call.getArgumentCount(); i < n; i++) {
                args.add(substituteProjection(call.argumentAt(i), projection));
            }
            return functions.next().of(call, args);
        }
        return expression;
    }

    /**
     * Returns {@code key IN (values)} bound to the SYMBOL overload, or null when none is registered.
     */
    FunctionExpression symbolIn(BoundExpression key, ObjList<BoundExpression> values, IntList valuePositions, int functionFlags, int position) {
        final ObjList<FunctionFactoryDescriptor> overloads = parser.getFunctionFactoryCache().getOverloadList("in");
        if (overloads == null) {
            return null;
        }
        for (int i = 0, n = overloads.size(); i < n; i++) {
            final FunctionFactoryDescriptor overload = overloads.getQuick(i);
            if (overload.getSigArgCount() == 2
                    && FunctionFactoryDescriptor.toTypeTag(overload.getArgTypeWithFlags(0)) == ColumnType.SYMBOL
                    && FunctionFactoryDescriptor.toTypeTag(overload.getArgTypeWithFlags(1)) == ColumnType.VAR_ARG) {
                conversionArguments.clear();
                conversionPositions.clear();
                try {
                    conversionArguments.add(key);
                    conversionArguments.addAll(values);
                    conversionPositions.add(key.getPosition());
                    conversionPositions.addAll(valuePositions);
                    return functions.next().of(overload, conversionArguments, conversionPositions,
                            ColumnType.BOOLEAN, functionFlags, position);
                } finally {
                    conversionArguments.clear();
                    conversionPositions.clear();
                }
            }
        }
        return null;
    }

    BoundExpression toBooleanSubquery(BoundExpression expression) {
        if (expression instanceof CursorExpression cursor && ColumnType.tagOf(cursor.getPlan().getOutput().getColumnCount() == 1
                ? cursor.getPlan().getOutput().getColumnType(0) : ColumnType.UNDEFINED) == ColumnType.BOOLEAN) {
            return cursors.next().ofBoolean(cursor, BoundExpression.RUNTIME_CONSTANT | cursor.getFunctionFlags()
                    & (BoundExpression.STABLE_WITHIN_EXECUTION | BoundExpression.NON_DETERMINISTIC));
        }
        return expression;
    }

    /**
     * Removes the innermost projected timestamp offset, the one over the timestamp column.
     */
    BoundExpression unwrapProjectedOffsets(BoundExpression expression) {
        if (!(expression instanceof FunctionExpression call)) {
            return expression;
        }
        if (call.isProjectedOffset() && call.argumentAt(2) instanceof ColumnExpression) {
            return call.argumentAt(2);
        }
        final ObjList<BoundExpression> args = rewriteArguments.next();
        for (int i = 0, n = call.getArgumentCount(); i < n; i++) {
            args.add(unwrapProjectedOffsets(call.argumentAt(i)));
        }
        return functions.next().of(call, args);
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
        for (int i = 0, n = prepared.size(); i < n; i++) {
            final PreparationEntry entry = prepared.getQuick(i);
            if (entry.expression == expression && entry.slot >= 0
                    && resources.resources.getQuick(entry.slot) instanceof GroupByFunction function) {
                final CharSequence unsupportedFill = GroupByUtils.getUnsupportedSampleByFill(function, fillToken);
                if (unsupportedFill != null) {
                    throw SqlException.$(fillPosition, "support for ").put(unsupportedFill)
                            .put(" fill is not yet implemented [function=").put(aggregateNode)
                            .put(", class=").put(function.getClass().getName()).put(']');
                }
                return;
            }
        }
        throw new IllegalStateException("aggregate preparation is not owned");
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

    private interface BindableColumn extends Function {
        int getColumnId();

        /**
         * False once an audited NULL fold closes this discarded operand; a closed leaf needs no input slot.
         */
        boolean isOpen();

        void setColumnId(int columnId);

        void setColumnIndex(int columnIndex);
    }

    private static final class BindableArrayColumn extends ArrayFunction implements BindableColumn {
        private int columnId;
        private int columnIndex = -1;
        private boolean isOpen = true;

        private BindableArrayColumn(int columnId, int type) {
            this.columnId = columnId;
            this.type = type;
        }

        @Override
        public void close() {
            isOpen = false;
        }

        @Override
        public ArrayView getArray(Record record) {
            assert columnIndex >= 0;
            return record.getArray(columnIndex, type);
        }

        @Override
        public int getColumnId() {
            return columnId;
        }

        @Override
        public boolean isOpen() {
            return isOpen;
        }

        @Override
        public boolean isThreadSafe() {
            return true;
        }

        @Override
        public void setColumnId(int columnId) {
            assert columnIndex == -1 && isOpen;
            this.columnId = columnId;
        }

        @Override
        public void setColumnIndex(int columnIndex) {
            this.columnIndex = columnIndex;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.putColumnName(columnIndex);
        }
    }

    private static final class BindableBooleanColumn extends BooleanFunction implements BindableColumn {
        private int columnId;
        private int columnIndex = -1;
        private boolean isOpen = true;

        private BindableBooleanColumn(int columnId) {
            this.columnId = columnId;
        }

        @Override
        public void close() {
            isOpen = false;
        }

        @Override
        public boolean getBool(Record record) {
            assert columnIndex >= 0;
            return record.getBool(columnIndex);
        }

        @Override
        public int getColumnId() {
            return columnId;
        }

        @Override
        public boolean isOpen() {
            return isOpen;
        }

        @Override
        public boolean isThreadSafe() {
            return true;
        }

        @Override
        public void setColumnId(int columnId) {
            assert columnIndex == -1 && isOpen;
            this.columnId = columnId;
        }

        @Override
        public void setColumnIndex(int columnIndex) {
            this.columnIndex = columnIndex;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.putColumnName(columnIndex);
        }
    }

    private static final class BindableByteColumn extends ByteFunction implements BindableColumn {
        private int columnId;
        private int columnIndex = -1;
        private boolean isOpen = true;

        private BindableByteColumn(int columnId) {
            this.columnId = columnId;
        }

        @Override
        public void close() {
            isOpen = false;
        }

        @Override
        public byte getByte(Record record) {
            assert columnIndex >= 0;
            return record.getByte(columnIndex);
        }

        @Override
        public int getColumnId() {
            return columnId;
        }

        @Override
        public boolean isOpen() {
            return isOpen;
        }

        @Override
        public boolean isThreadSafe() {
            return true;
        }

        @Override
        public void setColumnId(int columnId) {
            assert columnIndex == -1 && isOpen;
            this.columnId = columnId;
        }

        @Override
        public void setColumnIndex(int columnIndex) {
            this.columnIndex = columnIndex;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.putColumnName(columnIndex);
        }
    }

    private static final class BindableCharColumn extends CharFunction implements BindableColumn {
        private int columnId;
        private int columnIndex = -1;
        private boolean isOpen = true;

        private BindableCharColumn(int columnId) {
            this.columnId = columnId;
        }

        @Override
        public void close() {
            isOpen = false;
        }

        @Override
        public char getChar(Record record) {
            assert columnIndex >= 0;
            return record.getChar(columnIndex);
        }

        @Override
        public int getColumnId() {
            return columnId;
        }

        @Override
        public boolean isOpen() {
            return isOpen;
        }

        @Override
        public boolean isThreadSafe() {
            return true;
        }

        @Override
        public void setColumnId(int columnId) {
            assert columnIndex == -1 && isOpen;
            this.columnId = columnId;
        }

        @Override
        public void setColumnIndex(int columnIndex) {
            this.columnIndex = columnIndex;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.putColumnName(columnIndex);
        }
    }

    private static final class BindableDateColumn extends DateFunction implements BindableColumn {
        private int columnId;
        private int columnIndex = -1;
        private boolean isOpen = true;

        private BindableDateColumn(int columnId) {
            this.columnId = columnId;
        }

        @Override
        public void close() {
            isOpen = false;
        }

        @Override
        public int getColumnId() {
            return columnId;
        }

        @Override
        public long getDate(Record record) {
            assert columnIndex >= 0;
            return record.getDate(columnIndex);
        }

        @Override
        public boolean isOpen() {
            return isOpen;
        }

        @Override
        public boolean isThreadSafe() {
            return true;
        }

        @Override
        public void setColumnId(int columnId) {
            assert columnIndex == -1 && isOpen;
            this.columnId = columnId;
        }

        @Override
        public void setColumnIndex(int columnIndex) {
            this.columnIndex = columnIndex;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.putColumnName(columnIndex);
        }
    }

    private static final class BindableDoubleColumn extends DoubleFunction implements BindableColumn {
        private int columnId;
        private int columnIndex = -1;
        private boolean isOpen = true;

        private BindableDoubleColumn(int columnId) {
            this.columnId = columnId;
        }

        @Override
        public void close() {
            isOpen = false;
        }

        @Override
        public int getColumnId() {
            return columnId;
        }

        @Override
        public double getDouble(Record record) {
            assert columnIndex >= 0;
            return record.getDouble(columnIndex);
        }

        @Override
        public boolean isOpen() {
            return isOpen;
        }

        @Override
        public boolean isThreadSafe() {
            return true;
        }

        @Override
        public void setColumnId(int columnId) {
            assert columnIndex == -1 && isOpen;
            this.columnId = columnId;
        }

        @Override
        public void setColumnIndex(int columnIndex) {
            this.columnIndex = columnIndex;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.putColumnName(columnIndex);
        }
    }

    private static final class BindableFloatColumn extends FloatFunction implements BindableColumn {
        private int columnId;
        private int columnIndex = -1;
        private boolean isOpen = true;

        private BindableFloatColumn(int columnId) {
            this.columnId = columnId;
        }

        @Override
        public void close() {
            isOpen = false;
        }

        @Override
        public int getColumnId() {
            return columnId;
        }

        @Override
        public float getFloat(Record record) {
            assert columnIndex >= 0;
            return record.getFloat(columnIndex);
        }

        @Override
        public boolean isOpen() {
            return isOpen;
        }

        @Override
        public boolean isThreadSafe() {
            return true;
        }

        @Override
        public void setColumnId(int columnId) {
            assert columnIndex == -1 && isOpen;
            this.columnId = columnId;
        }

        @Override
        public void setColumnIndex(int columnIndex) {
            this.columnIndex = columnIndex;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.putColumnName(columnIndex);
        }
    }

    private static final class BindableGeoHashColumn extends AbstractGeoHashFunction implements BindableColumn {
        private int columnId;
        private int columnIndex = -1;
        private boolean isOpen = true;

        private BindableGeoHashColumn(int columnId, int type) {
            super(type);
            this.columnId = columnId;
        }

        @Override
        public void close() {
            isOpen = false;
        }

        @Override
        public int getColumnId() {
            return columnId;
        }

        @Override
        public byte getGeoByte(Record record) {
            assert columnIndex >= 0 && ColumnType.tagOf(type) == ColumnType.GEOBYTE;
            return record.getGeoByte(columnIndex);
        }

        @Override
        public int getGeoInt(Record record) {
            assert columnIndex >= 0 && ColumnType.tagOf(type) == ColumnType.GEOINT;
            return record.getGeoInt(columnIndex);
        }

        @Override
        public long getGeoLong(Record record) {
            assert columnIndex >= 0 && ColumnType.tagOf(type) == ColumnType.GEOLONG;
            return record.getGeoLong(columnIndex);
        }

        @Override
        public short getGeoShort(Record record) {
            assert columnIndex >= 0 && ColumnType.tagOf(type) == ColumnType.GEOSHORT;
            return record.getGeoShort(columnIndex);
        }

        @Override
        public boolean isOpen() {
            return isOpen;
        }

        @Override
        public boolean isThreadSafe() {
            return true;
        }

        @Override
        public void setColumnId(int columnId) {
            assert columnIndex == -1 && isOpen;
            this.columnId = columnId;
        }

        @Override
        public void setColumnIndex(int columnIndex) {
            this.columnIndex = columnIndex;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.putColumnName(columnIndex);
        }
    }

    private static final class BindableIPv4Column extends IPv4Function implements BindableColumn {
        private int columnId;
        private int columnIndex = -1;
        private boolean isOpen = true;

        private BindableIPv4Column(int columnId) {
            this.columnId = columnId;
        }

        @Override
        public void close() {
            isOpen = false;
        }

        @Override
        public int getColumnId() {
            return columnId;
        }

        @Override
        public int getIPv4(Record record) {
            assert columnIndex >= 0;
            return record.getIPv4(columnIndex);
        }

        @Override
        public boolean isOpen() {
            return isOpen;
        }

        @Override
        public boolean isThreadSafe() {
            return true;
        }

        @Override
        public void setColumnId(int columnId) {
            assert columnIndex == -1 && isOpen;
            this.columnId = columnId;
        }

        @Override
        public void setColumnIndex(int columnIndex) {
            this.columnIndex = columnIndex;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.putColumnName(columnIndex);
        }
    }

    private static final class BindableIntColumn extends IntFunction implements BindableColumn {
        private int columnId;
        private int columnIndex = -1;
        private boolean isOpen = true;

        private BindableIntColumn(int columnId) {
            this.columnId = columnId;
        }

        @Override
        public void close() {
            isOpen = false;
        }

        @Override
        public int getColumnId() {
            return columnId;
        }

        @Override
        public int getInt(Record record) {
            assert columnIndex >= 0;
            return record.getInt(columnIndex);
        }

        @Override
        public boolean isOpen() {
            return isOpen;
        }

        @Override
        public boolean isThreadSafe() {
            // Column positions are assigned once before the graph reaches execution.
            return true;
        }

        @Override
        public void setColumnId(int columnId) {
            assert columnIndex == -1 && isOpen;
            this.columnId = columnId;
        }

        @Override
        public void setColumnIndex(int columnIndex) {
            this.columnIndex = columnIndex;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.putColumnName(columnIndex);
        }
    }

    private static final class BindableLong256Column extends Long256Function implements BindableColumn {
        private int columnId;
        private int columnIndex = -1;
        private boolean isOpen = true;

        private BindableLong256Column(int columnId) {
            this.columnId = columnId;
        }

        @Override
        public void close() {
            isOpen = false;
        }

        @Override
        public int getColumnId() {
            return columnId;
        }

        @Override
        public void getLong256(Record record, CharSink<?> sink) {
            assert columnIndex >= 0;
            record.getLong256(columnIndex, sink);
        }

        @Override
        public Long256 getLong256A(Record record) {
            assert columnIndex >= 0;
            return record.getLong256A(columnIndex);
        }

        @Override
        public Long256 getLong256B(Record record) {
            assert columnIndex >= 0;
            return record.getLong256B(columnIndex);
        }

        @Override
        public boolean isOpen() {
            return isOpen;
        }

        @Override
        public void setColumnId(int columnId) {
            assert columnIndex == -1 && isOpen;
            this.columnId = columnId;
        }

        @Override
        public void setColumnIndex(int columnIndex) {
            this.columnIndex = columnIndex;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.putColumnName(columnIndex);
        }
    }

    private static final class BindableLongColumn extends LongFunction implements BindableColumn {
        private int columnId;
        private int columnIndex = -1;
        private boolean isOpen = true;

        private BindableLongColumn(int columnId) {
            this.columnId = columnId;
        }

        @Override
        public void close() {
            isOpen = false;
        }

        @Override
        public int getColumnId() {
            return columnId;
        }

        @Override
        public long getLong(Record record) {
            assert columnIndex >= 0;
            return record.getLong(columnIndex);
        }

        @Override
        public boolean isOpen() {
            return isOpen;
        }

        @Override
        public boolean isThreadSafe() {
            return true;
        }

        @Override
        public void setColumnId(int columnId) {
            assert columnIndex == -1 && isOpen;
            this.columnId = columnId;
        }

        @Override
        public void setColumnIndex(int columnIndex) {
            this.columnIndex = columnIndex;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.putColumnName(columnIndex);
        }
    }

    private static final class BindableShortColumn extends ShortFunction implements BindableColumn {
        private int columnId;
        private int columnIndex = -1;
        private boolean isOpen = true;

        private BindableShortColumn(int columnId) {
            this.columnId = columnId;
        }

        @Override
        public void close() {
            isOpen = false;
        }

        @Override
        public int getColumnId() {
            return columnId;
        }

        @Override
        public short getShort(Record record) {
            assert columnIndex >= 0;
            return record.getShort(columnIndex);
        }

        @Override
        public boolean isOpen() {
            return isOpen;
        }

        @Override
        public boolean isThreadSafe() {
            return true;
        }

        @Override
        public void setColumnId(int columnId) {
            assert columnIndex == -1 && isOpen;
            this.columnId = columnId;
        }

        @Override
        public void setColumnIndex(int columnIndex) {
            this.columnIndex = columnIndex;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.putColumnName(columnIndex);
        }
    }

    private static final class BindableStrColumn extends StrFunction implements BindableColumn {
        private int columnId;
        private int columnIndex = -1;
        private boolean isOpen = true;

        private BindableStrColumn(int columnId) {
            this.columnId = columnId;
        }

        @Override
        public void close() {
            isOpen = false;
        }

        @Override
        public int getColumnId() {
            return columnId;
        }

        @Override
        public CharSequence getStrA(Record record) {
            assert columnIndex >= 0;
            return record.getStrA(columnIndex);
        }

        @Override
        public CharSequence getStrB(Record record) {
            assert columnIndex >= 0;
            return record.getStrB(columnIndex);
        }

        @Override
        public int getStrLen(Record record) {
            assert columnIndex >= 0;
            return record.getStrLen(columnIndex);
        }

        @Override
        public boolean isOpen() {
            return isOpen;
        }

        @Override
        public void setColumnId(int columnId) {
            assert columnIndex == -1 && isOpen;
            this.columnId = columnId;
        }

        @Override
        public void setColumnIndex(int columnIndex) {
            this.columnIndex = columnIndex;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.putColumnName(columnIndex);
        }
    }

    private static final class BindableSymbolColumn extends SymbolFunction implements BindableColumn {
        private final boolean isSymbolTableStatic;
        private SymbolColumn column;
        private int columnId;
        private boolean isOpen = true;

        private BindableSymbolColumn(int columnId, boolean isSymbolTableStatic) {
            this.columnId = columnId;
            this.isSymbolTableStatic = isSymbolTableStatic;
        }

        @Override
        public void close() {
            isOpen = false;
            if (column != null) {
                column.close();
            }
        }

        @Override
        public int getColumnId() {
            return columnId;
        }

        @Override
        public int getInt(Record rec) {
            return column.getInt(rec);
        }

        @Override
        public StaticSymbolTable getStaticSymbolTable() {
            return column == null ? null : column.getStaticSymbolTable();
        }

        @Override
        public CharSequence getSymbol(Record rec) {
            return column.getSymbol(rec);
        }

        @Override
        public CharSequence getSymbolB(Record rec) {
            return column.getSymbolB(rec);
        }

        @Override
        public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) {
            column.init(symbolTableSource, executionContext);
        }

        @Override
        public boolean isOpen() {
            return isOpen;
        }

        @Override
        public boolean isSymbolTableStatic() {
            return isSymbolTableStatic;
        }

        @Override
        public SymbolTable newSymbolTable() {
            return column.newSymbolTable();
        }

        @Override
        public void setColumnId(int columnId) {
            assert isOpen && column == null;
            this.columnId = columnId;
        }

        @Override
        public void setColumnIndex(int columnIndex) {
            assert column == null && columnIndex >= 0;
            column = new SymbolColumn(columnIndex, isSymbolTableStatic);
        }

        @Override
        public boolean supportsKeyValueAccess() {
            return column != null && column.supportsKeyValueAccess();
        }

        @Override
        public boolean supportsParallelism() {
            return true;
        }

        @Override
        public void toPlan(PlanSink sink) {
            column.toPlan(sink);
        }

        @Override
        public CharSequence valueBOf(int symbolKey) {
            return column.valueBOf(symbolKey);
        }

        @Override
        public CharSequence valueOf(int symbolKey) {
            return column.valueOf(symbolKey);
        }
    }

    private static final class BindableTimestampColumn extends TimestampFunction implements BindableColumn {
        private int columnId;
        private int columnIndex = -1;
        private boolean isOpen = true;

        private BindableTimestampColumn(int columnId, int type) {
            super(type);
            this.columnId = columnId;
        }

        @Override
        public void close() {
            isOpen = false;
        }

        @Override
        public int getColumnId() {
            return columnId;
        }

        @Override
        public long getTimestamp(Record record) {
            assert columnIndex >= 0;
            return record.getTimestamp(columnIndex);
        }

        @Override
        public boolean isOpen() {
            return isOpen;
        }

        @Override
        public boolean isThreadSafe() {
            return true;
        }

        @Override
        public void setColumnId(int columnId) {
            assert columnIndex == -1 && isOpen;
            this.columnId = columnId;
        }

        @Override
        public void setColumnIndex(int columnIndex) {
            this.columnIndex = columnIndex;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.putColumnName(columnIndex);
        }
    }

    private static final class BindableUuidColumn extends UuidFunction implements BindableColumn {
        private int columnId;
        private int columnIndex = -1;
        private boolean isOpen = true;

        private BindableUuidColumn(int columnId) {
            this.columnId = columnId;
        }

        @Override
        public void close() {
            isOpen = false;
        }

        @Override
        public int getColumnId() {
            return columnId;
        }

        @Override
        public long getLong128Hi(Record record) {
            assert columnIndex >= 0;
            return record.getLong128Hi(columnIndex);
        }

        @Override
        public long getLong128Lo(Record record) {
            assert columnIndex >= 0;
            return record.getLong128Lo(columnIndex);
        }

        @Override
        public boolean isOpen() {
            return isOpen;
        }

        @Override
        public boolean isThreadSafe() {
            return true;
        }

        @Override
        public void setColumnId(int columnId) {
            assert columnIndex == -1 && isOpen;
            this.columnId = columnId;
        }

        @Override
        public void setColumnIndex(int columnIndex) {
            this.columnIndex = columnIndex;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.putColumnName(columnIndex);
        }
    }

    private static final class BindableVarcharColumn extends VarcharFunction implements BindableColumn {
        private int columnId;
        private int columnIndex = -1;
        private boolean isOpen = true;

        private BindableVarcharColumn(int columnId) {
            this.columnId = columnId;
        }

        @Override
        public void close() {
            isOpen = false;
        }

        @Override
        public int getColumnId() {
            return columnId;
        }

        @Override
        public Utf8Sequence getVarcharA(Record record) {
            assert columnIndex >= 0;
            return record.getVarcharA(columnIndex);
        }

        @Override
        public Utf8Sequence getVarcharB(Record record) {
            assert columnIndex >= 0;
            return record.getVarcharB(columnIndex);
        }

        @Override
        public int getVarcharSize(Record record) {
            assert columnIndex >= 0;
            return record.getVarcharSize(columnIndex);
        }

        @Override
        public boolean isOpen() {
            return isOpen;
        }

        @Override
        public void setColumnId(int columnId) {
            assert columnIndex == -1 && isOpen;
            this.columnId = columnId;
        }

        @Override
        public void setColumnIndex(int columnIndex) {
            this.columnIndex = columnIndex;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.putColumnName(columnIndex);
        }
    }

    private static final class InstantiationArguments implements Mutable {
        private final ObjList<Function> functions = new ObjList<>();
        private final IntList positions = new IntList();

        @Override
        public void clear() {
            functions.clear();
            positions.clear();
        }
    }

    private static final class PreparationEntry implements Mutable {
        private final ObjList<BindableColumn> leaves = new ObjList<>();
        private BoundExpression expression;
        private boolean isRebuildRequired;
        private int slot = -1;
        private int updateTargetType = -1;

        @Override
        public void clear() {
            leaves.clear();
            expression = null;
            isRebuildRequired = false;
            slot = -1;
            updateTargetType = -1;
        }
    }
}
