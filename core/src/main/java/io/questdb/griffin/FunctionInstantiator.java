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
import io.questdb.cairo.ImplicitCastException;
import io.questdb.cairo.IndexType;
import io.questdb.cairo.TableColumnMetadata;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.VirtualRecord;
import io.questdb.griffin.engine.functions.MonotonicTimestampFunction;
import io.questdb.griffin.engine.functions.RuntimeConstFunction;
import io.questdb.griffin.engine.functions.ScalarSubQueryBoundRefFunction;
import io.questdb.griffin.engine.functions.SubqueryCursorFunction;
import io.questdb.griffin.engine.functions.SymbolFunction;
import io.questdb.griffin.engine.functions.bool.BooleanSubQueryFunction;
import io.questdb.griffin.engine.functions.columns.BindableColumn;
import io.questdb.griffin.engine.functions.columns.RecordColumn;
import io.questdb.griffin.engine.functions.constants.ArrayConstant;
import io.questdb.griffin.engine.functions.constants.BooleanConstant;
import io.questdb.griffin.engine.functions.constants.ByteConstant;
import io.questdb.griffin.engine.functions.constants.CharConstant;
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
import io.questdb.griffin.engine.functions.constants.Long128Constant;
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
import io.questdb.griffin.engine.window.WindowFunction;
import io.questdb.griffin.model.ExpressionNode;
import io.questdb.griffin.model.ScalarTimestampBoundHolder;
import io.questdb.griffin.plan.logical.BindVariableExpression;
import io.questdb.griffin.plan.logical.BoundExpression;
import io.questdb.griffin.plan.logical.ColumnExpression;
import io.questdb.griffin.plan.logical.ConstantExpression;
import io.questdb.griffin.plan.logical.CursorExpression;
import io.questdb.griffin.plan.logical.DeferredErrorExpression;
import io.questdb.griffin.plan.logical.FunctionExpression;
import io.questdb.griffin.plan.logical.OutputSchema;
import io.questdb.griffin.plan.logical.TypeExpression;
import io.questdb.std.Chars;
import io.questdb.std.IntList;
import io.questdb.std.Misc;
import io.questdb.std.Mutable;
import io.questdb.std.ObjList;
import io.questdb.std.ObjectPool;

/**
 * Turns bound expressions into executable functions for the generator: adopts each prepared root once, rebuilds
 * the rest from the selected overloads under the final input layout, and generates sub-queries on demand.
 */
public final class FunctionInstantiator implements Mutable {
    private static final String NULL_PROBE_COLUMN = "null_probe";
    private final ObjectPool<InstantiationArguments> instantiations = new ObjectPool<>(InstantiationArguments::new, 8);
    private final ObjList<Function> nullProbeConstants = new ObjList<>(1);
    private final GenericRecordMetadata nullProbeMetadata = new GenericRecordMetadata();
    private final VirtualRecord nullProbeRecord;
    private final OutputSchema nullProbeSchema;
    private final ObjList<CursorExpression> parkedCursors = new ObjList<>();
    private final ObjList<Function> parkedSubqueries = new ObjList<>();
    private final FunctionParser parser;
    private final PreparedFunctions prepared;
    private final ObjList<CursorExpression> sharedBoundCursors = new ObjList<>();
    private final ObjList<ScalarTimestampBoundHolder> sharedBoundHolders = new ObjList<>();
    private final SqlBinder subqueryBinder;
    private int bindingDepth;
    private int workerCloneDepth;

    /**
     * The NULL probe schema is borrowed for single calls only; the sub-query binder is null where no sub-query can occur.
     */
    FunctionInstantiator(FunctionParser parser, PreparedFunctions prepared, OutputSchema nullProbeSchema, SqlBinder subqueryBinder) {
        this.parser = parser;
        this.prepared = prepared;
        this.nullProbeSchema = nullProbeSchema;
        this.subqueryBinder = subqueryBinder;
        nullProbeConstants.add(null);
        this.nullProbeRecord = new VirtualRecord(nullProbeConstants);
    }

    @Override
    public void clear() {
        Misc.freeObjListAndClear(parkedSubqueries);
        parkedCursors.clear();
        instantiations.clear();
        sharedBoundCursors.clear();
        sharedBoundHolders.clear();
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
        final PreparedFunctions.Entry entry = prepared.findOwned(expression, -1);
        if (entry != null) {
            if (entry.isRebuildRequired || entry.leaves.size() > 0 && requiresReconstruction(expression)) {
                throw new IllegalStateException("bound function requires final-layout reconstruction");
            }
            return adoptPreparation(entry, input, null);
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
        if (expression instanceof DeferredErrorExpression deferred) {
            throw deferred.raise();
        }
        if (metadata != null && metadata.getColumnCount() != input.getColumnCount()) {
            throw new IllegalStateException("bound function input metadata has changed");
        }
        instantiations.clear();
        try {
            return instantiateNew(expression, input, metadata, executionContext, true, null);
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
            return instantiateNew(expression, input, metadata, executionContext, false, null);
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
            return (WindowFunction) instantiateNew(expression, input, metadata, executionContext, false, null);
        } finally {
            instantiations.clear();
        }
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

    private Function adoptPreparation(PreparedFunctions.Entry entry, OutputSchema input, RecordMetadata metadata) {
        assert !(entry.expression instanceof FunctionExpression call && call.isWindow()) : "bind-time window function is adopted";
        if (prepared.root(entry).isConstant()) {
            return prepared.detach(entry);
        }
        assert PreparedFunctions.hasOnlyReadLeaves(entry) : "prepared leaf is not read by its description";
        for (int k = 0, count = entry.leaves.size(); k < count; k++) {
            final BindableColumn leaf = entry.leaves.getQuick(k);
            // Audited NULL folds close discarded operands. These are borrows,
            // never separately owned leaves; a dead leaf needs no input slot.
            if (leaf.isOpen()) {
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
            if (leaf.isOpen()) {
                leaf.setColumnIndex(input.getColumnIndexById(leaf.getColumnId()));
            }
        }
        return prepared.detach(entry);
    }

    private void closePreparation(BoundExpression expression) {
        final PreparedFunctions.Entry entry = prepared.findOwned(expression);
        if (entry != null) {
            Misc.free(prepared.detach(entry));
        }
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
            boolean isAdoptionAllowed,
            PreparedFunctions.Entry preparation
    ) throws SqlException {
        if (isAdoptionAllowed && !hasSharedBound(expression)) {
            // Independently bound conjuncts may now belong to a new AND
            // description. Adopt each exact owned root once; the parent's frame
            // owns it after detachment, including if a later sibling fails.
            final PreparedFunctions.Entry entry = prepared.findOwned(expression, -1);
            if (entry != null) {
                if (isPreparationCompatible(entry, input, metadata)) {
                    return adoptPreparation(entry, input, metadata);
                }
                Misc.free(prepared.detach(entry));
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
            if (ColumnType.isArray(column.getDataType()) || !BindableColumn.isBindableType(column.getDataType())) {
                if (preparation != null) {
                    preparation.isRebuildRequired = true;
                }
                return createColumnFunction(column.getPosition(), index, column.getDataType(), input);
            }
            final BindableColumn leaf = BindableColumn.newInstance(column.getColumnId(), column.getDataType(), input.isSymbolTableStatic(index));
            if (preparation == null) {
                leaf.setColumnIndex(index);
            } else {
                // Adoption assigns the final input position.
                preparation.leaves.add(leaf);
            }
            return leaf;
        }
        if (expression instanceof ConstantExpression constant) {
            if (constant.isUnparsedTimestamp()) {
                throw SqlException.invalidDate(constant.getTimestampText(), constant.getPosition());
            }
            final PreparedFunctions.Entry entry = prepared.findOwned(constant, -1);
            if (entry != null) {
                return prepared.detach(entry);
            }
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
                case ColumnType.LONG128 -> new Long128Constant(constant.getLong128Lo(), constant.getLong128Hi());
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
        final BoundExpression error = LogicalPlans.firstGenerationError(call);
        if (error != null) {
            LogicalPlans.raiseGenerationError(error);
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
                final Function child = instantiateNew(call.argumentAt(i), input, metadata, executionContext, isAdoptionAllowed, preparation);
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
                // Optimiser-built calls may record a weaker stability than the built function proves, and a call
                // stable with its sub-queries is built over their proven stability; the bound flags stay the
                // conservative ones optimisations relied on.
                final int flags = call.getFunctionFlags() & ~BoundExpression.STABLE_WITH_SUBQUERIES;
                if (function.getType() != call.getDataType()
                        || (FunctionBinder.functionFlags(function) & (flags | ~BoundExpression.STABLE_WITHIN_EXECUTION)) != flags) {
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

    private boolean isPreparationCompatible(PreparedFunctions.Entry entry, OutputSchema input, RecordMetadata metadata) {
        if (entry.isRebuildRequired || hasArrayColumnLayoutDependency(entry.expression)
                || entry.leaves.size() > 0 && requiresReconstruction(entry.expression)) {
            return false;
        }
        assert PreparedFunctions.hasOnlyReadLeaves(entry) : "prepared leaf is not read by its description";
        for (int i = 0, n = entry.leaves.size(); i < n; i++) {
            final BindableColumn leaf = entry.leaves.getQuick(i);
            if (leaf.isOpen() && leaf instanceof SymbolFunction symbol) {
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

    static Function createColumnFunction(int position, int index, int type, OutputSchema input) throws SqlException {
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

    static int monotonicTimestampColumnId(Function function) {
        while (function instanceof MonotonicTimestampFunction monotonic) {
            function = monotonic.getTimestampArg();
        }
        return function instanceof BindableColumn column && ColumnType.isTimestamp(function.getType())
                ? column.getColumnId() : -1;
    }

    void beginWorkerClones() {
        workerCloneDepth++;
    }

    Throwable closePrepared(Throwable primary) {
        return prepared.closePrepared(primary);
    }

    void endWorkerClones() {
        workerCloneDepth--;
    }

    int getScalarBoundDepth() {
        return subqueryBinder.getScalarBoundDepth();
    }

    Function instantiateSubquery(CursorExpression cursor, SqlExecutionContext executionContext) throws SqlException {
        final int parked = parkedCursors.indexOf(cursor);
        if (parked >= 0) {
            final Function function = parkedSubqueries.getQuick(parked);
            parkedCursors.remove(parked);
            parkedSubqueries.remove(parked);
            return function;
        }
        final int index = cursor.getSubqueryIndex();
        if (bindingDepth > 0) {
            // Binding builds against the output metadata; the generator rebuilds the root.
            return new SubqueryCursorFunction(subqueryBinder.getSubqueryMetadata(index), true);
        }
        final boolean isStable = cursor.isStableWithinExecution();
        if (workerCloneDepth == 0) {
            return new SubqueryCursorFunction(subqueryBinder.takeSubquery(index, executionContext), isStable);
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
            return new SubqueryCursorFunction(subqueryBinder.generateSubquery(index, executionContext), isStable);
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
        final PreparedFunctions.Entry entry = prepared.findOwned(expression, targetType);
        if (entry != null) {
            if (isPreparationCompatible(entry, input, metadata)) {
                return adoptPreparation(entry, input, metadata);
            }
            Misc.free(prepared.detach(entry));
        }
        return convertUpdateFunction(parser, instantiate(expression, input, metadata, executionContext),
                targetType, expression.getPosition());
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

    /**
     * Builds a call the binder typed without constructing it, under the binding input, when a parent must be
     * constructed while binding; its column leaves join {@code preparation} so the prepared root stays relocatable.
     */
    Function realize(BoundExpression expression, OutputSchema input, PreparedFunctions.Entry preparation, SqlExecutionContext executionContext) throws SqlException {
        bindingDepth++;
        try {
            return instantiateNew(expression, input, null, executionContext, false, preparation);
        } finally {
            bindingDepth--;
        }
    }

    /**
     * Builds an independent closure from the selected overloads, never adopting a prepared root; the
     * binder rebuilds replacement arguments this way while the parser constructs their parent.
     */
    Function rebuild(BoundExpression expression, OutputSchema input, SqlExecutionContext executionContext) throws SqlException {
        bindingDepth++;
        try {
            return instantiateNew(expression, input, null, executionContext, false, null);
        } finally {
            bindingDepth--;
        }
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
     * An owned factory of the sub-query for a consumer that reads it directly.
     */
    RecordCursorFactory takeSubquery(CursorExpression cursor, SqlExecutionContext executionContext) throws SqlException {
        return subqueryBinder.takeSubquery(cursor.getSubqueryIndex(), executionContext);
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
}
