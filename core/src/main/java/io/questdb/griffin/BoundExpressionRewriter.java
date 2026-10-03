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
import io.questdb.griffin.engine.functions.SymbolFunction;
import io.questdb.griffin.engine.functions.columns.BindableColumn;
import io.questdb.griffin.plan.logical.BindVariableExpression;
import io.questdb.griffin.plan.logical.BoundExpression;
import io.questdb.griffin.plan.logical.ColumnExpression;
import io.questdb.griffin.plan.logical.ConstantExpression;
import io.questdb.griffin.plan.logical.CursorExpression;
import io.questdb.griffin.plan.logical.DeferredErrorExpression;
import io.questdb.griffin.plan.logical.FunctionExpression;
import io.questdb.griffin.plan.logical.OuterColumnExpression;
import io.questdb.griffin.plan.logical.ProjectPlan;
import io.questdb.griffin.plan.logical.TypeExpression;
import io.questdb.std.IntIntHashMap;
import io.questdb.std.IntList;
import io.questdb.std.Mutable;
import io.questdb.std.ObjList;
import io.questdb.std.ObjectPool;

/**
 * Rewrites bound expression trees into new descriptions over already-selected overloads: column remapping and
 * substitution, conjunction surgery and operand reshapes, never parsing SQL or building a function.
 */
public final class BoundExpressionRewriter implements Mutable {
    private final ObjList<BoundExpression> argumentScratch;
    private final ObjectPool<ColumnExpression> columns;
    private final ObjectPool<ConstantExpression> constants;
    private final FunctionFactoryCache functionFactoryCache;
    private final ObjectPool<FunctionExpression> functions;
    private final ObjectPool<OuterColumnExpression> outerColumns;
    private final ObjectPool<BindVariableExpression> parameters;
    private final IntList positionScratch;
    private final PreparedFunctions prepared;
    private final ObjectPool<ObjList<BoundExpression>> rewriteArguments = new ObjectPool<>(ObjList::new, 8);
    private final ObjectPool<TypeExpression> types;
    private boolean isReplacementPlaced;

    /**
     * Allocates descriptions from the given pools, which their owner empties; the scratch lists are borrowed for
     * single calls only.
     */
    BoundExpressionRewriter(
            FunctionFactoryCache functionFactoryCache,
            ObjectPool<ColumnExpression> columns,
            ObjectPool<ConstantExpression> constants,
            ObjectPool<FunctionExpression> functions,
            ObjectPool<OuterColumnExpression> outerColumns,
            ObjectPool<BindVariableExpression> parameters,
            ObjectPool<TypeExpression> types,
            PreparedFunctions prepared,
            ObjList<BoundExpression> argumentScratch,
            IntList positionScratch
    ) {
        this.functionFactoryCache = functionFactoryCache;
        this.columns = columns;
        this.constants = constants;
        this.functions = functions;
        this.outerColumns = outerColumns;
        this.parameters = parameters;
        this.types = types;
        this.prepared = prepared;
        this.argumentScratch = argumentScratch;
        this.positionScratch = positionScratch;
    }

    @Override
    public void clear() {
        rewriteArguments.clear();
    }

    public FunctionExpression commuteEquality(FunctionExpression original) {
        final FunctionFactoryDescriptor overload = original.getOverload().getCommutedEquality();
        if (original.getArgumentCount() != 2 || overload == null) {
            throw new IllegalArgumentException("registered binary equality required");
        }
        argumentScratch.clear();
        positionScratch.clear();
        try {
            argumentScratch.add(original.argumentAt(1));
            argumentScratch.add(original.argumentAt(0));
            positionScratch.add(original.getArgumentPosition(1));
            positionScratch.add(original.getArgumentPosition(0));
            return functions.next().of(overload, argumentScratch, positionScratch,
                    original.getDataType(), original.getFunctionFlags(), original.getPosition());
        } finally {
            argumentScratch.clear();
            positionScratch.clear();
        }
    }

    /**
     * Copies an expression through a projection of plain columns into fresh descriptions, leaving its preparation
     * with the original.
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

    private static int conjunctionFlags(BoundExpression left, BoundExpression right) {
        final int leftFlags = left.getFunctionFlags();
        final int rightFlags = right.getFunctionFlags();
        int flags = leftFlags & rightFlags & BoundExpression.CONSTANT;
        flags |= (leftFlags | rightFlags) & BoundExpression.NON_DETERMINISTIC;
        flags |= LogicalPlans.weakerStability(LogicalPlans.stabilityFlags(left), LogicalPlans.stabilityFlags(right));
        if ((leftFlags & (BoundExpression.CONSTANT | BoundExpression.RUNTIME_CONSTANT)) != 0
                && (rightFlags & (BoundExpression.CONSTANT | BoundExpression.RUNTIME_CONSTANT)) != 0
                && ((leftFlags | rightFlags) & BoundExpression.RUNTIME_CONSTANT) != 0) {
            flags |= BoundExpression.RUNTIME_CONSTANT;
        }
        return flags;
    }

    private static ColumnExpression projectionColumn(ProjectPlan projection, int columnId, int type) {
        final int index = projection.getOutput().getColumnIndexById(columnId);
        if (index < 0 || !(projection.getExpressions().getQuick(index) instanceof ColumnExpression column)
                || column.getDataType() != type) {
            throw new IllegalArgumentException("column-only projection with unchanged types required");
        }
        return column;
    }

    /**
     * The flags of a call whose column arguments were replaced by expressions: their non-determinism and
     * instability carry over to the call.
     */
    private static int substitutedFlags(FunctionExpression call, ObjList<BoundExpression> arguments) {
        int flags = call.getFunctionFlags();
        int combined = LogicalPlans.stabilityFlags(call);
        for (int i = 0, n = arguments.size(); i < n; i++) {
            final BoundExpression argument = arguments.getQuick(i);
            flags |= argument.getFunctionFlags() & BoundExpression.NON_DETERMINISTIC;
            combined = LogicalPlans.weakerStability(combined, LogicalPlans.stabilityFlags(argument));
        }
        return flags & ~(BoundExpression.STABLE_WITHIN_EXECUTION | BoundExpression.STABLE_WITH_SUBQUERIES) | combined;
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
        // A false argument keeps a conjunct that raises an error when built: a
        // deferred binding error or timestamp text that does not convert.
        if (left instanceof ConstantExpression constant && (constant.getLongValue() != 0 || !LogicalPlans.hasGenerationError(right))) {
            // TRUE returns the unchanged right function, even raw NULL. A false
            // argument instead makes the factory return BOOLEAN FALSE.
            return constant.getLongValue() != 0 ? right
                    : left.getDataType() == ColumnType.BOOLEAN ? left : constants.next().ofBoolean(false, position);
        }
        if (right instanceof ConstantExpression constant && (constant.getLongValue() != 0 || !LogicalPlans.hasGenerationError(left))) {
            return constant.getLongValue() != 0 ? left
                    : right.getDataType() == ColumnType.BOOLEAN ? right : constants.next().ofBoolean(false, position);
        }
        final int flags = conjunctionFlags(left, right);
        argumentScratch.clear();
        positionScratch.clear();
        try {
            argumentScratch.add(left);
            argumentScratch.add(right);
            positionScratch.add(left.getPosition());
            positionScratch.add(right.getPosition());
            return functions.next().of(overload, argumentScratch, positionScratch,
                    ColumnType.BOOLEAN, flags, position);
        } finally {
            argumentScratch.clear();
            positionScratch.clear();
        }
    }

    private BoundExpression copy(BoundExpression expression) {
        return switch (expression) {
            case ColumnExpression column ->
                    columns.next().of(column.getColumnId(), column.getDataType(), column.getPosition(),
                            column.isDirectReference(), column.isCast());
            case OuterColumnExpression outer ->
                    outerColumns.next().of(outer.getColumnId(), outer.getDataType(), outer.getPosition());
            case FunctionExpression call -> copy(call);
            case ConstantExpression constant ->
                    constants.next().of(constant, constant.getSource() == null ? null : copy(constant.getSource()));
            case BindVariableExpression parameter -> {
                final BindVariableExpression copy = parameters.next().of(parameter.getName(), parameter.getDataType(),
                        parameter.getFunctionFlags(), parameter.getPosition(), parameter.isDirectReference());
                yield parameter.isPredefined() ? copy.markPredefined() : copy;
            }
            case TypeExpression type -> types.next().of(type.getDataType(), type.getPosition());
            // A cursor stays shared: the instantiator keys sub-query reuse across its positions on its identity.
            case CursorExpression cursor -> cursor;
            case DeferredErrorExpression deferred -> deferred;
        };
    }

    private FunctionExpression copy(FunctionExpression call) {
        final ObjList<BoundExpression> args = rewriteArguments.next();
        for (int i = 0, n = call.getArgumentCount(); i < n; i++) {
            args.add(copy(call.argumentAt(i)));
        }
        return functions.next().of(call, args);
    }

    private BoundExpression remapColumns0(BoundExpression expression, ProjectPlan projection, boolean isMoving) {
        if (expression instanceof ColumnExpression column) {
            final ColumnExpression input = projectionColumn(projection, column.getColumnId(), column.getDataType());
            final boolean isDirectReference = column.isDirectReference() && input.isDirectReference();
            final boolean isCast = column.isCast() || input.isCast();
            final BoundExpression replacement = isMoving && input.getColumnId() == column.getColumnId()
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
            if (changed || !isMoving) {
                final FunctionExpression replacement = functions.next().of(call, args);
                args.clear();
                if (isMoving) {
                    retargetPreparation(expression, replacement, projection);
                }
                return replacement;
            }
            args.clear();
            return expression;
        }
        return isMoving ? expression : copy(expression);
    }

    private void retargetPreparation(BoundExpression expression, BoundExpression replacement, ProjectPlan projection) {
        if (replacement != expression) {
            final PreparedFunctions.Entry entry = prepared.findOwned(expression);
            if (entry != null) {
                assert PreparedFunctions.hasOnlyReadLeaves(entry) : "prepared leaf is not read by its description";
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
            }
        }
    }

    private BoundExpression substituteColumn0(BoundExpression expression, int columnId, BoundExpression replacement) {
        if (expression instanceof ColumnExpression column) {
            if (column.getColumnId() != columnId) {
                return expression;
            }
            if (isReplacementPlaced) {
                return copy(replacement);
            }
            isReplacementPlaced = true;
            return replacement;
        }
        if (expression instanceof FunctionExpression call && references(call, columnId)) {
            final ObjList<BoundExpression> args = rewriteArguments.next();
            for (int i = 0, n = call.getArgumentCount(); i < n; i++) {
                args.add(substituteColumn0(call.argumentAt(i), columnId, replacement));
            }
            return functions.next().of(call, args, substitutedFlags(call, args));
        }
        return expression;
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
        final ObjList<FunctionFactoryDescriptor> overloads = functionFactoryCache.getOverloadList("and");
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

    /**
     * Describes the argument-free window function {@code name}, such as {@code row_number}, without preparing
     * it; the generator builds windows under their final window context.
     */
    FunctionExpression describeWindowCall(CharSequence name, int position) {
        final ObjList<FunctionFactoryDescriptor> overloads = functionFactoryCache.getOverloadList(name);
        for (int i = 0, n = overloads == null ? 0 : overloads.size(); i < n; i++) {
            final FunctionFactoryDescriptor overload = overloads.getQuick(i);
            if (overload.getSigArgCount() == 0 && overload.getFactory().isWindow()) {
                argumentScratch.clear();
                positionScratch.clear();
                return functions.next().of(overload, argumentScratch, positionScratch, ColumnType.LONG, 0, position);
            }
        }
        throw new IllegalStateException("window function is not registered");
    }

    /**
     * Copies the expression with the value itself at the first reference to the column, so its preparation is
     * adopted there, and its own copy at every further reference; preparations of rewritten calls stay with the
     * original.
     */
    BoundExpression moveToColumn(BoundExpression expression, int columnId, BoundExpression value) {
        isReplacementPlaced = false;
        return substituteColumn0(expression, columnId, value);
    }

    BoundExpression newFalseConstant(int position) {
        return constants.next().ofBoolean(false, position);
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

    /**
     * Copies the expression with every reference to the column replaced by its own copy of the replacement,
     * which stays untouched; preparations stay with the original.
     */
    BoundExpression substituteColumn(BoundExpression expression, int columnId, BoundExpression replacement) {
        isReplacementPlaced = true;
        return substituteColumn0(expression, columnId, replacement);
    }

    /**
     * Replaces each column with the expression the projection computes for it: an input column or
     * a timestamp offset. Each reference reads its own copy of the projected expression, so the projection keeps
     * its own preparation.
     */
    BoundExpression substituteProjection(BoundExpression expression, ProjectPlan projection) {
        if (expression instanceof ColumnExpression column) {
            final int index = projection.getOutput().getColumnIndexById(column.getColumnId());
            final BoundExpression projected = projection.getExpressions().getQuick(index);
            if (projected instanceof ColumnExpression projectedColumn) {
                return columns.next().of(projectedColumn.getColumnId(), column.getDataType(), column.getPosition(),
                        column.isDirectReference() && projectedColumn.isDirectReference(), column.isCast() || projectedColumn.isCast());
            }
            return copy((FunctionExpression) projected).markProjectedOffset();
        }
        if (expression instanceof FunctionExpression call) {
            final ObjList<BoundExpression> args = rewriteArguments.next();
            for (int i = 0, n = call.getArgumentCount(); i < n; i++) {
                args.add(substituteProjection(call.argumentAt(i), projection));
            }
            return functions.next().of(call, args, substitutedFlags(call, args));
        }
        return expression;
    }

    /**
     * Returns {@code key IN (values)} bound to the SYMBOL overload, or null when none is registered.
     */
    FunctionExpression symbolIn(BoundExpression key, ObjList<BoundExpression> values, IntList valuePositions, int functionFlags, int position) {
        final ObjList<FunctionFactoryDescriptor> overloads = functionFactoryCache.getOverloadList("in");
        if (overloads == null) {
            return null;
        }
        for (int i = 0, n = overloads.size(); i < n; i++) {
            final FunctionFactoryDescriptor overload = overloads.getQuick(i);
            if (overload.getSigArgCount() == 2
                    && FunctionFactoryDescriptor.toTypeTag(overload.getArgTypeWithFlags(0)) == ColumnType.SYMBOL
                    && FunctionFactoryDescriptor.toTypeTag(overload.getArgTypeWithFlags(1)) == ColumnType.VAR_ARG) {
                argumentScratch.clear();
                positionScratch.clear();
                try {
                    argumentScratch.add(key);
                    argumentScratch.addAll(values);
                    positionScratch.add(key.getPosition());
                    positionScratch.addAll(valuePositions);
                    return functions.next().of(overload, argumentScratch, positionScratch,
                            ColumnType.BOOLEAN, functionFlags, position);
                } finally {
                    argumentScratch.clear();
                    positionScratch.clear();
                }
            }
        }
        return null;
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
}
