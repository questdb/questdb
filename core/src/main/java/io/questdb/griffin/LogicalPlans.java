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
import io.questdb.griffin.model.QueryModel;
import io.questdb.griffin.plan.logical.AggregatePlan;
import io.questdb.griffin.plan.logical.BoundExpression;
import io.questdb.griffin.plan.logical.ColumnExpression;
import io.questdb.griffin.plan.logical.ConstantExpression;
import io.questdb.griffin.plan.logical.CursorExpression;
import io.questdb.griffin.plan.logical.FilterPlan;
import io.questdb.griffin.plan.logical.FunctionExpression;
import io.questdb.griffin.plan.logical.JoinInput;
import io.questdb.griffin.plan.logical.JoinPlan;
import io.questdb.griffin.plan.logical.LatestByPlan;
import io.questdb.griffin.plan.logical.LimitPlan;
import io.questdb.griffin.plan.logical.LogicalPlan;
import io.questdb.griffin.plan.logical.OutputSchema;
import io.questdb.griffin.plan.logical.ProjectPlan;
import io.questdb.griffin.plan.logical.SampleByPlan;
import io.questdb.griffin.plan.logical.ScanPlan;
import io.questdb.griffin.plan.logical.SetOperationPlan;
import io.questdb.griffin.plan.logical.SortPlan;
import io.questdb.griffin.plan.logical.UnaryPlan;
import io.questdb.std.Chars;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;

/**
 * Stateless plan-node predicates, walkers and plan-property utilities shared by the binder, the optimiser and the plan generator.
 */
final class LogicalPlans {
    private LogicalPlans() {
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
                case QueryModel.JOIN_LEFT_OUTER, QueryModel.JOIN_RIGHT_OUTER, QueryModel.JOIN_ASOF, QueryModel.JOIN_LT,
                     QueryModel.JOIN_FULL_OUTER, QueryModel.JOIN_SPLICE -> {
                    return false;
                }
                default -> {
                }
            }
        }
        for (int i = sourcePosition + 1; i <= lastInput; i++) {
            if (isMasterNullingJoin(ordered.getQuick(i).getJoinType())) {
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
            case FilterPlan filter -> isOrderIndependent(filter.getPredicate()) && canPushSetTimestampThrough(filter, columnIndex);
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

    static boolean isMasterNullingJoin(int joinType) {
        return joinType == QueryModel.JOIN_RIGHT_OUTER || joinType == QueryModel.JOIN_FULL_OUTER
                || joinType == QueryModel.JOIN_SPLICE;
    }

    static boolean isOrderIndependent(BoundExpression predicate) {
        final int flags = predicate.getFunctionFlags();
        return (flags & BoundExpression.STABLE_WITHIN_EXECUTION) != 0
                && (flags & BoundExpression.NON_DETERMINISTIC) == 0;
    }

    /**
     * Predicts whether the factory generated for the plan supports random access, which the full-fat
     * temporal join decision needs before generation. Plans whose factory choice is not known before generation report false.
     */
    static boolean isRandomAccess(LogicalPlan plan) {
        return switch (plan) {
            case ScanPlan scan -> scan.isRandomAccess();
            case FilterPlan _, LimitPlan _ -> isRandomAccess(plan.inputAt(0));
            case ProjectPlan project -> {
                final ObjList<BoundExpression> expressions = project.getExpressions();
                for (int i = 0, n = expressions.size(); i < n; i++) {
                    if (hasNonRepeatableFunction(expressions.getQuick(i))) {
                        yield false;
                    }
                }
                yield isRandomAccess(plan.inputAt(0));
            }
            case SortPlan _, LatestByPlan _ -> true;
            case SampleByPlan sample -> sample.getFillMode() == SampleByPlan.FILL_LINEAR;
            case AggregatePlan aggregate -> aggregate.getGroupingExpressions().size() > 0;
            case SetOperationPlan operation -> operation.getOperation() != QueryModel.SET_OPERATION_UNION
                    && operation.getOperation() != QueryModel.SET_OPERATION_UNION_ALL
                    && !operation.isSymbolRestorationRequired() && isRandomAccess(operation.getLeft());
            default -> false;
        };
    }

    /**
     * Whether the aggregate is a keyless {@code min}, {@code max}, {@code first} or {@code last} of the designated
     * timestamp of a table scan, optionally filtered: its value is the timestamp of the first row in
     * ascending ({@code min}, {@code first}) or descending ({@code max}, {@code last}) timestamp order.
     */
    static boolean isTimestampEndpoint(AggregatePlan aggregate) {
        if (aggregate.getType() != LogicalPlan.Type.AGGREGATE || aggregate.getGroupingExpressions().size() != 0
                || aggregate.getAggregates().size() != 1) {
            return false;
        }
        final FunctionExpression call = aggregate.getAggregates().getQuick(0);
        if (call.getArgumentCount() != 1 || !(call.argumentAt(0) instanceof ColumnExpression column) || !column.isDirectReference()
                || !isTimestampEndpointBackward(call) && !Chars.equalsIgnoreCase(call.getName(), "min") && !Chars.equalsIgnoreCase(call.getName(), "first")) {
            return false;
        }
        final LogicalPlan input = aggregate.getInput();
        final LogicalPlan table = input.getType() == LogicalPlan.Type.FILTER ? input.inputAt(0) : input;
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
     * The arithmetic {@code c * k}, {@code c + k} or {@code c - k}, either operand order, that an aggregate
     * reading tables without a sub-query sums, where {@code c} is a BYTE, SHORT, INT or LONG input column and
     * {@code k} an integer literal; otherwise null. {@link AggregateRewritePass} normalises such a sum.
     */
    static FunctionExpression normalisableSumOperation(AggregatePlan aggregate, FunctionExpression sum) {
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
     * Whether the expression reads only columns of the output and no cursor.
     */
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

    static void retypeFullFatKeys(JoinInput step, OutputSchema output) {
        if (!step.isFullFat()) {
            return;
        }
        final IntList masterIds = step.getMasterKeyColumnIds();
        final IntList slaveIds = step.getSlaveKeyColumnIds();
        for (int i = 0, n = slaveIds.size(); i < n; i++) {
            final int slaveId = slaveIds.getQuick(i);
            // The last equality on a shared slave column decides its type.
            if (slaveIds.indexOf(slaveId, i + 1, n) < 0) {
                final int slaveIndex = output.getColumnIndexById(slaveId);
                final int masterIndex = output.getColumnIndexById(masterIds.getQuick(i));
                if (slaveIndex >= 0 && masterIndex >= 0 && ColumnType.isSymbol(output.getColumnType(slaveIndex))
                        && !ColumnType.isSymbol(output.getColumnType(masterIndex))) {
                    output.setColumnType(slaveIndex, output.getColumnType(masterIndex));
                    output.setSymbolTableStatic(slaveIndex, false);
                }
            }
        }
    }

    static int setTimestampIndex(LogicalPlan plan) {
        final int timestampIndex = plan.getOutput().getTimestampIndex();
        if (timestampIndex >= 0) {
            return timestampIndex;
        }
        if (plan.getType() == LogicalPlan.Type.SET_OPERATION) {
            final int leftIndex = setTimestampIndex(plan.inputAt(0));
            return leftIndex >= 0 ? leftIndex : setTimestampIndex(plan.inputAt(1));
        }
        return -1;
    }

    static LogicalPlan skipFilters(LogicalPlan plan) {
        while (plan.getType() == LogicalPlan.Type.FILTER) {
            plan = plan.inputAt(0);
        }
        return plan;
    }

    static LogicalPlan skipProjects(LogicalPlan plan) {
        while (plan.getType() == LogicalPlan.Type.PROJECT) {
            plan = plan.inputAt(0);
        }
        return plan;
    }

    static LogicalPlan skipProjectsAndFilters(LogicalPlan plan) {
        while (plan.getType() == LogicalPlan.Type.PROJECT || plan.getType() == LogicalPlan.Type.FILTER) {
            plan = plan.inputAt(0);
        }
        return plan;
    }

    private static boolean canPushSetTimestampThrough(UnaryPlan plan, int columnIndex) {
        final LogicalPlan input = plan.getInput();
        final int inputIndex = input.getOutput().getColumnIndexById(plan.getOutput().getColumnId(columnIndex));
        return inputIndex >= 0 && canPushSetTimestamp(input, inputIndex);
    }

    private static boolean hasNonRepeatableFunction(BoundExpression expression) {
        if (expression instanceof FunctionExpression function) {
            final String name = function.getName();
            if (Chars.startsWith(name, "rnd_") || Chars.equals(name, "timestamp_sequence") || Chars.equals(name, "within_box")
                    || Chars.equals(name, "within_radius") || Chars.equals(name, "geo_within_radius_latlon")) {
                return true;
            }
            for (int i = 0, n = function.getArgumentCount(); i < n; i++) {
                if (hasNonRepeatableFunction(function.argumentAt(i))) {
                    return true;
                }
            }
        }
        return false;
    }
}
