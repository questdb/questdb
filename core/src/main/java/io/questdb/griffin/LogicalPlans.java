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
import io.questdb.cairo.ImplicitCastException;
import io.questdb.griffin.plan.logical.AggregatePlan;
import io.questdb.griffin.plan.logical.BindVariableExpression;
import io.questdb.griffin.plan.logical.BoundExpression;
import io.questdb.griffin.plan.logical.ColumnExpression;
import io.questdb.griffin.plan.logical.ConstantExpression;
import io.questdb.griffin.plan.logical.CursorExpression;
import io.questdb.griffin.plan.logical.DeferredErrorExpression;
import io.questdb.griffin.plan.logical.DistinctPlan;
import io.questdb.griffin.plan.logical.FillPlan;
import io.questdb.griffin.plan.logical.FilterPlan;
import io.questdb.griffin.plan.logical.FunctionExpression;
import io.questdb.griffin.plan.logical.FunctionSourcePlan;
import io.questdb.griffin.plan.logical.GroupingPlan;
import io.questdb.griffin.plan.logical.JoinInput;
import io.questdb.griffin.plan.logical.JoinPlan;
import io.questdb.griffin.plan.logical.LimitPlan;
import io.questdb.griffin.plan.logical.LogicalPlan;
import io.questdb.griffin.plan.logical.OuterColumnExpression;
import io.questdb.griffin.plan.logical.OutputSchema;
import io.questdb.griffin.plan.logical.ProjectPlan;
import io.questdb.griffin.plan.logical.ScanPlan;
import io.questdb.griffin.plan.logical.SetOperationPlan;
import io.questdb.griffin.plan.logical.SortPlan;
import io.questdb.griffin.plan.logical.TypeExpression;
import io.questdb.griffin.plan.logical.UnaryPlan;
import io.questdb.griffin.plan.logical.WindowJoinPlan;
import io.questdb.griffin.plan.logical.WindowJoinStep;
import io.questdb.griffin.plan.logical.WindowPlan;
import io.questdb.std.Chars;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;

/**
 * Stateless plan-node predicates, walkers and plan-property utilities shared by the binder, the optimiser and the plan generator.
 */
final class LogicalPlans {
    private static final int ORDER_STABLE = 2;
    private static final int RESULT_STABLE = 1;
    private static final int SEQUENCE_STABLE = RESULT_STABLE | ORDER_STABLE;

    private LogicalPlans() {
    }

    private static boolean areStable(ObjList<? extends BoundExpression> expressions) {
        for (int i = 0, n = expressions.size(); i < n; i++) {
            if (!isStable(expressions.getQuick(i))) {
                return false;
            }
        }
        return true;
    }

    private static boolean areSubqueriesStable(BoundExpression expression) {
        if (expression instanceof CursorExpression cursor) {
            return cursor.isStableWithinExecution();
        }
        if (expression instanceof FunctionExpression call) {
            for (int i = 0, n = call.getArgumentCount(); i < n; i++) {
                if (!areSubqueriesStable(call.argumentAt(i))) {
                    return false;
                }
            }
        }
        return true;
    }

    private static boolean canPushSetTimestampThrough(UnaryPlan plan, int columnIndex) {
        final LogicalPlan input = plan.getInput();
        final int inputIndex = input.getOutput().getColumnIndexById(plan.getOutput().getColumnId(columnIndex));
        return inputIndex >= 0 && canPushSetTimestamp(input, inputIndex);
    }

    private static int groupingStability(AggregatePlan aggregate, boolean isParallelGroupByEnabled) {
        if (aggregate.getSharedSource() != null || stability(aggregate.getInput(), isParallelGroupByEnabled) != SEQUENCE_STABLE
                || !areStable(aggregate.getGroupingExpressions())
                || !areStable(aggregate.getAggregates())) {
            return 0;
        }
        // Only the parallel GROUP BY emits groups in no fixed order; with it disabled the generator groups serially.
        return aggregate.getGroupingExpressions().size() == 0 || !isParallelGroupByEnabled ? SEQUENCE_STABLE : RESULT_STABLE;
    }

    private static boolean hasInputColumn(BoundExpression expression) {
        if (expression instanceof ColumnExpression) {
            return true;
        }
        if (expression instanceof FunctionExpression call) {
            for (int i = 0, n = call.getArgumentCount(); i < n; i++) {
                if (hasInputColumn(call.argumentAt(i))) {
                    return true;
                }
            }
        }
        return false;
    }

    private static boolean isStable(BoundExpression expression) {
        return expression == null || isStableWithinExecution(expression);
    }

    private static boolean isTimestampComparison(FunctionExpression call) {
        return call.getArgumentCount() == 2 && switch (call.getName()) {
            case "=", "!=", "<>", "<", "<=", ">", ">=" -> true;
            default -> false;
        };
    }

    private static boolean isUnconvertedTimestampCall(FunctionExpression call) {
        for (int i = 0, n = call.getArgumentCount(); i < n; i++) {
            if (call.argumentAt(i) instanceof ConstantExpression constant && constant.isUnparsedTimestamp()) {
                return true;
            }
        }
        return isTimestampComparison(call)
                && (isUnconvertibleSymbol(call.argumentAt(0), call.argumentAt(1).getDataType())
                || isUnconvertibleSymbol(call.argumentAt(1), call.argumentAt(0).getDataType()));
    }

    /**
     * The stability bits of the plan's result within one execution: {@link #RESULT_STABLE} when every
     * evaluation yields the same multiset of rows, plus {@link #ORDER_STABLE} when it also yields them in the
     * same order. Operators whose stability is not proven report neither.
     */
    private static int stability(LogicalPlan plan, boolean isParallelGroupByEnabled) {
        return switch (plan) {
            case ScanPlan scan -> scan.getTableToken().isLiveView() ? 0 : SEQUENCE_STABLE;
            case FunctionSourcePlan source -> source.isSequenceStable() ? SEQUENCE_STABLE : 0;
            case FilterPlan filter ->
                    isStable(filter.getPredicate()) ? stability(filter.getInput(), isParallelGroupByEnabled) : 0;
            case ProjectPlan project ->
                    areStable(project.getExpressions()) ? stability(project.getInput(), isParallelGroupByEnabled) : 0;
            case SortPlan sort -> sort.isMarkoutHorizon() ? 0 : stability(sort.getInput(), isParallelGroupByEnabled);
            case LimitPlan limit -> isStable(limit.getLo()) && isStable(limit.getHi())
                    && stability(limit.getInput(), isParallelGroupByEnabled) == SEQUENCE_STABLE ? SEQUENCE_STABLE : 0;
            case AggregatePlan aggregate -> groupingStability(aggregate, isParallelGroupByEnabled);
            case DistinctPlan distinct -> stability(distinct.getInput(), isParallelGroupByEnabled) & RESULT_STABLE;
            case SetOperationPlan operation ->
                    stability(operation.getLeft(), isParallelGroupByEnabled) & stability(operation.getRight(), isParallelGroupByEnabled);
            default -> 0;
        };
    }

    static boolean canPushJoinFilter(JoinPlan join, int source, int lastInput) {
        if (join.getInputs().getQuick(source).getInput() == null) {
            return false;
        }
        final ObjList<JoinInput> ordered = join.getOrderedInputs();
        final int sourcePosition = ordered.indexOf(join.getInputs().getQuick(source));
        if (sourcePosition < 0 || sourcePosition > lastInput) {
            return false;
        }
        if (sourcePosition > 0) {
            switch (ordered.getQuick(sourcePosition).getJoinType()) {
                case LEFT_OUTER, RIGHT_OUTER, FULL_OUTER, ASOF, LT, SPLICE -> {
                    return false;
                }
                default -> {
                }
            }
        }
        for (int i = sourcePosition + 1; i <= lastInput; i++) {
            if (ordered.getQuick(i).getJoinType().isMasterNulling()) {
                return false;
            }
        }
        return true;
    }

    /**
     * Proves that a set timestamp predicate can reach every native source without changing precision.
     */
    static boolean canPushSetTimestamp(LogicalPlan plan, int columnIndex) {
        final OutputSchema output = plan.getOutput();
        if (!ColumnType.isTimestamp(output.getColumnType(columnIndex))) {
            return false;
        }
        return switch (plan) {
            case ScanPlan scan -> output.getColumnId(columnIndex) == scan.getNativeTimestampColumnId();
            case ProjectPlan project -> {
                if (!isColumnProjection(project)) {
                    yield false;
                }
                final ColumnExpression column = (ColumnExpression) project.getExpressions().getQuick(columnIndex);
                final LogicalPlan input = project.getInput();
                final int inputIndex = input.getOutput().getColumnIndexById(column.getColumnId());
                yield inputIndex >= 0 && input.getOutput().getColumnType(inputIndex) == output.getColumnType(columnIndex)
                        && canPushSetTimestamp(input, inputIndex);
            }
            case FilterPlan filter ->
                    isOrderIndependent(filter.getPredicate()) && !hasDeferredConjunct(filter.getPredicate())
                            && canPushSetTimestampThrough(filter, columnIndex);
            case SortPlan sort -> canPushSetTimestampThrough(sort, columnIndex);
            case SetOperationPlan operation -> {
                final int type = output.getColumnType(columnIndex);
                yield setTimestampIndex(operation) == columnIndex
                        && operation.getLeft().getOutput().getColumnType(columnIndex) == type
                        && operation.getRight().getOutput().getColumnType(columnIndex) == type
                        && canPushSetTimestamp(operation.getLeft(), columnIndex)
                        && canPushSetTimestamp(operation.getRight(), columnIndex);
            }
            default -> false;
        };
    }

    static void collectConjuncts(BoundExpression predicate, ObjList<BoundExpression> sink) {
        if (predicate instanceof FunctionExpression call && call.getArgumentCount() == 2 && call.isAnd()) {
            collectConjuncts(call.argumentAt(0), sink);
            collectConjuncts(call.argumentAt(1), sink);
        } else {
            sink.add(predicate);
        }
    }

    /**
     * Adds the ids of the outer columns the expression reads, with repeats.
     */
    static void collectOuterColumnIds(BoundExpression expression, IntList sink) {
        if (expression instanceof OuterColumnExpression outer) {
            sink.add(outer.getColumnId());
        } else if (expression instanceof FunctionExpression call) {
            for (int i = 0, n = call.getArgumentCount(); i < n; i++) {
                collectOuterColumnIds(call.argumentAt(i), sink);
            }
        }
    }

    static void collectOuterColumnIds(ObjList<? extends BoundExpression> expressions, IntList sink) {
        for (int i = 0, n = expressions.size(); i < n; i++) {
            collectOuterColumnIds(expressions.getQuick(i), sink);
        }
    }

    /**
     * Adds the ids of the outer columns the node's own expressions read, with repeats; its inputs are not visited.
     */
    static void collectOuterColumnIds(LogicalPlan plan, IntList sink) {
        switch (plan) {
            case FilterPlan filter -> collectOuterColumnIds(filter.getPredicate(), sink);
            case ProjectPlan project -> collectOuterColumnIds(project.getExpressions(), sink);
            case GroupingPlan aggregate -> {
                collectOuterColumnIds(aggregate.getGroupingExpressions(), sink);
                collectOuterColumnIds(aggregate.getAggregates(), sink);
            }
            case FillPlan fill -> collectOuterColumnIds(fill.getValues(), sink);
            case WindowPlan window -> {
                collectOuterColumnIds(window.getFunctions(), sink);
                for (int i = 0, n = window.getSpecs().size(); i < n; i++) {
                    collectOuterColumnIds(window.getSpecs().getQuick(i).getPartitionBy(), sink);
                }
            }
            case LimitPlan limit -> {
                collectOuterColumnIds(limit.getLo(), sink);
                collectOuterColumnIds(limit.getHi(), sink);
            }
            case JoinPlan join -> {
                for (int i = 0, n = join.getInputs().size(); i < n; i++) {
                    final JoinInput input = join.getInputs().getQuick(i);
                    collectOuterColumnIds(input.getKeyFilter(), sink);
                    collectOuterColumnIds(input.getOnResidual(), sink);
                    collectOuterColumnIds(input.getPostJoinFilter(), sink);
                    if (input.getUnnest() != null) {
                        collectOuterColumnIds(input.getUnnest().getExpressions(), sink);
                    }
                }
            }
            case WindowJoinPlan windowJoin -> {
                for (int i = 0, n = windowJoin.getSteps().size(); i < n; i++) {
                    final WindowJoinStep step = windowJoin.getSteps().getQuick(i);
                    collectOuterColumnIds(step.getAggregates(), sink);
                    collectOuterColumnIds(step.getFilter(), sink);
                    collectOuterColumnIds(step.getLoExpression(), sink);
                    collectOuterColumnIds(step.getHiExpression(), sink);
                }
            }
            default -> {
            }
        }
    }

    /**
     * The first error that building the expression raises, or null: a {@link DeferredErrorExpression}
     * conjunct, a conjunction over a conjunct that is not BOOLEAN, or a call whose conversion of timestamp
     * text the function parser fails. The parser builds calls in post order, the arguments of each call last
     * to first, and converts the literal arguments of a call, first to last, as it builds that call. A
     * conjunction checks its argument types, first to last, once it has built both.
     */
    static BoundExpression firstGenerationError(BoundExpression expression) {
        if (expression instanceof DeferredErrorExpression) {
            return expression;
        }
        if (!(expression instanceof FunctionExpression call)) {
            return null;
        }
        final boolean isAnd = call.isAnd();
        for (int i = call.getArgumentCount() - 1; i > -1; i--) {
            final BoundExpression argument = call.argumentAt(i);
            if (!isAnd || !(argument instanceof DeferredErrorExpression deferred) || !deferred.isNonBoolean()) {
                final BoundExpression error = firstGenerationError(argument);
                if (error != null) {
                    return error;
                }
            }
        }
        if (isAnd) {
            for (int i = 0, n = call.getArgumentCount(); i < n; i++) {
                if (call.argumentAt(i) instanceof DeferredErrorExpression deferred && deferred.isNonBoolean()) {
                    return call;
                }
            }
        }
        return isUnconvertedTimestampCall(call) ? call : null;
    }

    /**
     * Whether a conjunct of the predicate is a WHERE or ON conjunct that failed to bind. Such a conjunct never
     * evaluates and its filter's generation raises its error, so no pass folds it, reads it as a fact or moves
     * it out of the filter it was bound in, other than to the join input whose columns alone it reads.
     */
    static boolean hasDeferredConjunct(BoundExpression predicate) {
        if (predicate instanceof FunctionExpression call && call.getArgumentCount() == 2 && call.isAnd()) {
            return hasDeferredConjunct(call.argumentAt(0)) || hasDeferredConjunct(call.argumentAt(1));
        }
        return predicate instanceof DeferredErrorExpression;
    }

    /**
     * Whether building the expression raises an error the binder deferred: a {@link DeferredErrorExpression}
     * conjunct, an unparsed timestamp literal, or a SYMBOL constant compared with a TIMESTAMP that it does not
     * convert to.
     */
    static boolean hasGenerationError(BoundExpression expression) {
        return expression instanceof ConstantExpression constant ? constant.isUnparsedTimestamp()
                : firstGenerationError(expression) != null;
    }

    /**
     * True when the expression reads a column of an enclosing LATERAL's outer input.
     */
    static boolean hasOuterColumn(BoundExpression expression) {
        if (expression instanceof OuterColumnExpression) {
            return true;
        }
        if (expression instanceof FunctionExpression call) {
            for (int i = 0, n = call.getArgumentCount(); i < n; i++) {
                if (hasOuterColumn(call.argumentAt(i))) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * True when the plan or one of its inputs reads a column of an enclosing LATERAL's outer input;
     * {@code tmpColumnIds} is restored on return.
     */
    static boolean hasOuterColumn(LogicalPlan plan, IntList tmpColumnIds) {
        final int base = tmpColumnIds.size();
        collectOuterColumnIds(plan, tmpColumnIds);
        final boolean hasOwn = tmpColumnIds.size() > base;
        tmpColumnIds.setPos(base);
        if (hasOwn) {
            return true;
        }
        for (int i = 0, n = plan.inputCount(); i < n; i++) {
            if (hasOuterColumn(plan.inputAt(i), tmpColumnIds)) {
                return true;
            }
        }
        return false;
    }

    static boolean hasRepeatedColumn(ProjectPlan project) {
        final ObjList<BoundExpression> expressions = project.getExpressions();
        for (int i = 1, n = expressions.size(); i < n; i++) {
            if (expressions.getQuick(i) instanceof ColumnExpression column) {
                for (int k = 0; k < i; k++) {
                    if (expressions.getQuick(k) instanceof ColumnExpression other && other.getColumnId() == column.getColumnId()) {
                        return true;
                    }
                }
            }
        }
        return false;
    }

    static boolean isColumnProjection(ProjectPlan project) {
        if (project.hasUpdateConversions() || project.hasPrunedComputedColumns()) {
            return false;
        }
        for (int i = 0, n = project.getExpressions().size(); i < n; i++) {
            if (!(project.getExpressions().getQuick(i) instanceof ColumnExpression)) {
                return false;
            }
        }
        return true;
    }

    /**
     * An integer literal spelled in SQL, not folded from an expression.
     */
    static boolean isIntegerLiteral(BoundExpression expression) {
        return expression instanceof ConstantExpression constant && constant.isLiteral() && constant.getSource() == null
                && (constant.getDataType() == ColumnType.INT || constant.getDataType() == ColumnType.LONG);
    }

    static boolean isOrderIndependent(BoundExpression predicate) {
        return isStableWithinExecution(predicate) && (predicate.getFunctionFlags() & BoundExpression.NON_DETERMINISTIC) == 0;
    }

    /**
     * Whether every evaluation of the plan within one execution yields the same multiset of rows, which is
     * all the value of a sub-query depends on: its consumers read a set or a single row.
     */
    static boolean isResultStable(LogicalPlan plan, SqlExecutionContext executionContext) {
        return (stability(plan, executionContext.isParallelGroupByEnabled()) & RESULT_STABLE) != 0;
    }

    /**
     * Whether every evaluation of the plan within one execution yields the same rows in the same order.
     */
    static boolean isSequenceStable(LogicalPlan plan, SqlExecutionContext executionContext) {
        return stability(plan, executionContext.isParallelGroupByEnabled()) == SEQUENCE_STABLE;
    }

    /**
     * Whether every evaluation of the expression within one execution yields the same value: its flags prove it,
     * or it is stable whenever its sub-queries are and each sub-query it reads is proven stable. The one stability
     * query the optimiser and the generator ask of a bound expression.
     */
    static boolean isStableWithinExecution(BoundExpression expression) {
        if (expression instanceof CursorExpression cursor) {
            return cursor.isStableWithinExecution();
        }
        final int flags = expression.getFunctionFlags();
        return (flags & BoundExpression.STABLE_WITHIN_EXECUTION) != 0
                || (flags & BoundExpression.STABLE_WITH_SUBQUERIES) != 0 && areSubqueriesStable(expression);
    }

    /**
     * Whether the aggregate is a keyless {@code min}, {@code max}, {@code first} or {@code last} of the designated
     * timestamp of a table scan, optionally filtered: its value is the timestamp of the first row in
     * ascending ({@code min}, {@code first}) or descending ({@code max}, {@code last}) timestamp order.
     */
    static boolean isTimestampEndpoint(AggregatePlan aggregate) {
        if (aggregate.getGroupingExpressions().size() != 0 || aggregate.getAggregates().size() != 1) {
            return false;
        }
        final FunctionExpression call = aggregate.getAggregates().getQuick(0);
        if (call.getArgumentCount() != 1 || !(call.argumentAt(0) instanceof ColumnExpression column) || !column.isDirectReference()
                || !isTimestampEndpointBackward(call) && !Chars.equalsIgnoreCase(call.getName(), "min") && !Chars.equalsIgnoreCase(call.getName(), "first")) {
            return false;
        }
        final LogicalPlan input = aggregate.getInput();
        final LogicalPlan table = input instanceof FilterPlan filter ? filter.getInput() : input;
        return table instanceof ScanPlan scan && column.getColumnId() == scan.getNativeTimestampColumnId()
                && column.getColumnId() == input.getOutput().getTimestampColumnId();
    }

    /**
     * Whether a {@link #isTimestampEndpoint(AggregatePlan) timestamp endpoint} reads the last row in timestamp order.
     */
    static boolean isTimestampEndpointBackward(FunctionExpression call) {
        return Chars.equalsIgnoreCase(call.getName(), "max") || Chars.equalsIgnoreCase(call.getName(), "last");
    }

    /**
     * Whether the expression is a SYMBOL constant that does not convert to the TIMESTAMP type it is compared with.
     */
    static boolean isUnconvertibleSymbol(BoundExpression expression, int timestampType) {
        if (!(expression instanceof ConstantExpression symbol) || symbol.getDataType() != ColumnType.SYMBOL
                || ColumnType.tagOf(timestampType) != ColumnType.TIMESTAMP) {
            return false;
        }
        try {
            ColumnType.getTimestampDriver(timestampType).implicitCast(symbol.getStrValue(), ColumnType.SYMBOL);
            return false;
        } catch (ImplicitCastException e) {
            return true;
        }
    }

    /**
     * Evaluating the expression twice may give two values: it calls a function that is neither
     * deterministic nor stable within one execution, or reads a sub-query not proven stable.
     */
    static boolean isVolatile(BoundExpression expression) {
        if ((expression.getFunctionFlags() & BoundExpression.NON_DETERMINISTIC) != 0 && !isStableWithinExecution(expression)) {
            return true;
        }
        if (expression instanceof FunctionExpression call) {
            for (int i = 0, n = call.getArgumentCount(); i < n; i++) {
                if (isVolatile(call.argumentAt(i))) {
                    return true;
                }
            }
            return false;
        }
        if (expression instanceof CursorExpression cursor) {
            return !cursor.isStableWithinExecution();
        }
        return !(expression instanceof ColumnExpression || expression instanceof ConstantExpression
                || expression instanceof BindVariableExpression || expression instanceof TypeExpression);
    }

    /**
     * The arithmetic {@code c * k}, {@code c + k} or {@code c - k}, either operand order, that an aggregate
     * reading tables without a sub-query sums, where {@code c} is a BYTE, SHORT, INT or LONG input column and
     * {@code k} an integer literal; otherwise null. {@link AggregateRewritePass} normalises such a sum.
     */
    static FunctionExpression normalisableSumOperation(GroupingPlan aggregate, FunctionExpression sum) {
        if (!aggregate.hasDirectTableInput() || !Chars.equalsIgnoreCase(sum.getName(), "sum") || sum.getArgumentCount() != 1
                || !(sum.argumentAt(0) instanceof FunctionExpression operation) || operation.getArgumentCount() != 2) {
            return null;
        }
        final String name = operation.getName();
        if (name.length() != 1 || name.charAt(0) != '*' && name.charAt(0) != '+' && name.charAt(0) != '-') {
            return null;
        }
        final boolean isColumnOnLeft = isIntegerLiteral(operation.argumentAt(1));
        if (!isColumnOnLeft && !isIntegerLiteral(operation.argumentAt(0))
                || !(operation.argumentAt(isColumnOnLeft ? 0 : 1) instanceof ColumnExpression column)) {
            return null;
        }
        final OutputSchema input = aggregate.getInput().getOutput();
        final int index = input.getColumnIndexById(column.getColumnId());
        if (index < 0) {
            return null;
        }
        final int type = input.getColumnType(index);
        return type == ColumnType.BYTE || type == ColumnType.SHORT || type == ColumnType.INT || type == ColumnType.LONG ? operation : null;
    }

    static int projectedColumnIndex(ProjectPlan project, int columnId) {
        final ObjList<BoundExpression> expressions = project.getExpressions();
        for (int i = 0, n = expressions.size(); i < n; i++) {
            if (expressions.getQuick(i) instanceof ColumnExpression column && column.getColumnId() == columnId) {
                return i;
            }
        }
        return -1;
    }

    /**
     * Index of the first expression that passes the column through without a cast.
     */
    static int projectedUncastColumnIndex(ProjectPlan project, int columnId) {
        final ObjList<BoundExpression> expressions = project.getExpressions();
        for (int i = 0, n = expressions.size(); i < n; i++) {
            if (expressions.getQuick(i) instanceof ColumnExpression column && !column.isCast() && column.getColumnId() == columnId) {
                return i;
            }
        }
        return -1;
    }

    /**
     * Raises an error {@link #firstGenerationError} found: the deferred error, the type mismatch of a
     * conjunction's first conjunct that is not BOOLEAN, or the error building the call
     * raises for its first timestamp text that does not convert: the parse error of an unparsed literal, which
     * the parser raises before it builds the call, otherwise the cast error of the SYMBOL constant.
     */
    static void raiseGenerationError(BoundExpression error) throws SqlException {
        if (error instanceof DeferredErrorExpression deferred) {
            throw deferred.raise();
        }
        final FunctionExpression call = (FunctionExpression) error;
        if (call.isAnd()) {
            for (int i = 0, n = call.getArgumentCount(); i < n; i++) {
                if (call.argumentAt(i) instanceof DeferredErrorExpression deferred && deferred.isNonBoolean()) {
                    throw deferred.raiseTypeMismatch();
                }
            }
        }
        for (int i = 0, n = call.getArgumentCount(); i < n; i++) {
            if (call.argumentAt(i) instanceof ConstantExpression constant && constant.isUnparsedTimestamp()) {
                throw SqlException.invalidDate(constant.getTimestampText(), constant.getPosition());
            }
        }
        final int symbolIndex = isUnconvertibleSymbol(call.argumentAt(0), call.argumentAt(1).getDataType()) ? 0 : 1;
        final BoundExpression timestamp = call.argumentAt(1 - symbolIndex);
        ColumnType.getTimestampDriver(timestamp.getDataType())
                .implicitCast(((ConstantExpression) call.argumentAt(symbolIndex)).getStrValue(), ColumnType.SYMBOL);
        throw new IllegalStateException("timestamp text converts");
    }

    static boolean readsOnly(BoundExpression expression, OutputSchema output) {
        if (expression instanceof ColumnExpression column) {
            return output.getColumnIndexById(column.getColumnId()) >= 0;
        }
        if (expression instanceof FunctionExpression call) {
            for (int i = 0, n = call.getArgumentCount(); i < n; i++) {
                if (!readsOnly(call.argumentAt(i), output)) {
                    return false;
                }
            }
        }
        return !(expression instanceof CursorExpression);
    }

    /**
     * True when the call reads at least one outer column and no column of its own input.
     */
    static boolean readsOnlyOuterColumns(FunctionExpression call) {
        return hasOuterColumn(call) && !hasInputColumn(call);
    }

    static int setTimestampIndex(LogicalPlan plan) {
        final int timestampIndex = plan.getOutput().getTimestampIndex();
        if (timestampIndex >= 0) {
            return timestampIndex;
        }
        if (plan instanceof SetOperationPlan) {
            final int leftIndex = setTimestampIndex(plan.inputAt(0));
            return leftIndex >= 0 ? leftIndex : setTimestampIndex(plan.inputAt(1));
        }
        return -1;
    }

    static LogicalPlan skipFilters(LogicalPlan plan) {
        while (plan instanceof FilterPlan) {
            plan = plan.inputAt(0);
        }
        return plan;
    }

    static LogicalPlan skipProjects(LogicalPlan plan) {
        while (plan instanceof ProjectPlan) {
            plan = plan.inputAt(0);
        }
        return plan;
    }

    static LogicalPlan skipProjectsAndFilters(LogicalPlan plan) {
        while (plan instanceof ProjectPlan || plan instanceof FilterPlan) {
            plan = plan.inputAt(0);
        }
        return plan;
    }

    /**
     * The stability an expression's flags record: {@link BoundExpression#STABLE_WITHIN_EXECUTION},
     * {@link BoundExpression#STABLE_WITH_SUBQUERIES} or neither; a sub-query its rules do not prove stable is
     * stable with its sub-queries.
     */
    static int stabilityFlags(BoundExpression expression) {
        final int flags = expression.getFunctionFlags();
        if ((flags & BoundExpression.STABLE_WITHIN_EXECUTION) != 0) {
            return BoundExpression.STABLE_WITHIN_EXECUTION;
        }
        return (flags & BoundExpression.STABLE_WITH_SUBQUERIES) != 0 || expression instanceof CursorExpression
                ? BoundExpression.STABLE_WITH_SUBQUERIES : 0;
    }

    /**
     * The weaker of two {@link #stabilityFlags(BoundExpression)} results.
     */
    static int weakerStability(int left, int right) {
        if (left == 0 || right == 0) {
            return 0;
        }
        return left == right ? left : BoundExpression.STABLE_WITH_SUBQUERIES;
    }
}
