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

import io.questdb.griffin.engine.window.LiveViewWindowDescription;
import io.questdb.griffin.plan.logical.AggregatePlan;
import io.questdb.griffin.plan.logical.BindVariableExpression;
import io.questdb.griffin.plan.logical.BoundExpression;
import io.questdb.griffin.plan.logical.ColumnExpression;
import io.questdb.griffin.plan.logical.ConstantExpression;
import io.questdb.griffin.plan.logical.FilterPlan;
import io.questdb.griffin.plan.logical.FunctionExpression;
import io.questdb.griffin.plan.logical.GroupingPlan;
import io.questdb.griffin.plan.logical.LimitPlan;
import io.questdb.griffin.plan.logical.LogicalPlan;
import io.questdb.griffin.plan.logical.OutputSchema;
import io.questdb.griffin.plan.logical.ProjectPlan;
import io.questdb.griffin.plan.logical.SortPlan;
import io.questdb.griffin.plan.logical.TypeExpression;
import io.questdb.griffin.plan.logical.WindowPlan;
import io.questdb.griffin.plan.logical.WindowSpec;
import io.questdb.std.Chars;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;
import io.questdb.std.ObjectPool;

/**
 * Computes identical window calls once. A call merges into an equal earlier call of its window, or of a
 * window beneath it whose result reaches it, when both read the same values under the same
 * specification and neither evaluates a volatile function; its consumers then read the earlier result.
 */
final class WindowCsePass {
    private final ObjList<LogicalPlan> ancestors;
    private final ObjectPool<ColumnExpression> columns;
    private final OptimiserContext context;
    private final ObjectPool<ProjectPlan> projects;

    WindowCsePass(OptimiserContext context, ObjectPool<ColumnExpression> columns, ObjectPool<ProjectPlan> projects, ObjList<LogicalPlan> ancestors) {
        this.context = context;
        this.columns = columns;
        this.projects = projects;
        this.ancestors = ancestors;
    }

    /**
     * The expression that computes a column under the plan, or null when the column is a scan, join or
     * window result.
     */
    private static BoundExpression definitionOf(int columnId, LogicalPlan plan) {
        while (true) {
            if (plan instanceof ProjectPlan project) {
                final int index = project.getOutput().getColumnIndexById(columnId);
                if (index < 0) {
                    return null;
                }
                final BoundExpression expression = project.getExpressions().getQuick(index);
                if (!(expression instanceof ColumnExpression column) || column.getColumnId() != columnId) {
                    return expression;
                }
            } else if (!(plan instanceof WindowPlan window) || window.getFunctionColumnIds().contains(columnId)) {
                return null;
            }
            plan = plan.inputAt(0);
        }
    }

    private static boolean isSameCall(FunctionExpression call, FunctionExpression other) {
        if (call.getOverload() != other.getOverload() || call.getFunctionFlags() != other.getFunctionFlags()
                || call.isProjectedOffset() != other.isProjectedOffset() || call.isSetOperation() != other.isSetOperation()
                || call.getArgumentCount() != other.getArgumentCount()) {
            return false;
        }
        for (int i = 0, n = call.getArgumentCount(); i < n; i++) {
            if (!isSameExpression(call.argumentAt(i), other.argumentAt(i))) {
                return false;
            }
        }
        return true;
    }

    /**
     * Two ordering columns are the same when equal deterministic expressions compute them, as select aliases
     * of one expression do.
     */
    private static boolean isSameColumn(int columnId, int otherId, LogicalPlan scope) {
        if (columnId == otherId) {
            return true;
        }
        final BoundExpression definition = definitionOf(columnId, scope);
        final BoundExpression other = definitionOf(otherId, scope);
        return definition != null && other != null && !LogicalPlans.isVolatile(definition) && isSameExpression(definition, other);
    }

    private static boolean isSameExpression(BoundExpression expression, BoundExpression other) {
        if (expression.getClass() != other.getClass() || expression.getDataType() != other.getDataType()) {
            return false;
        }
        return switch (expression) {
            case ColumnExpression column -> column.getColumnId() == ((ColumnExpression) other).getColumnId()
                    && column.isCast() == ((ColumnExpression) other).isCast();
            case ConstantExpression constant -> constant.isSameValue((ConstantExpression) other);
            case BindVariableExpression variable ->
                    Chars.equals(variable.getName(), ((BindVariableExpression) other).getName());
            case TypeExpression _ -> true;
            case FunctionExpression call -> isSameCall(call, (FunctionExpression) other);
            default -> false;
        };
    }

    private static boolean isSameLiveViewDescription(LiveViewWindowDescription description, LiveViewWindowDescription other) {
        return description == null ? other == null : other != null
                                                     && description.getCanonicalWindowName().equals(other.getCanonicalWindowName())
                                                     && description.getOrderSignature().equals(other.getOrderSignature())
                                                     && description.getPartitionSignature().equals(other.getPartitionSignature())
                                                     && description.isAnchored() == other.isAnchored();
    }

    private static boolean isSameSpec(WindowSpec spec, WindowSpec other, LogicalPlan scope) {
        if (spec.getFramingMode() != other.getFramingMode() || spec.getRowsLo() != other.getRowsLo()
                || spec.getRowsHi() != other.getRowsHi() || spec.getRowsLoExprTimeUnit() != other.getRowsLoExprTimeUnit()
                || spec.getRowsHiExprTimeUnit() != other.getRowsHiExprTimeUnit() || spec.getExclusionKind() != other.getExclusionKind()
                || spec.isIgnoreNulls() != other.isIgnoreNulls() || spec.isSubsampleKeepFlag() != other.isSubsampleKeepFlag()
                || spec.getPartitionBy().size() != other.getPartitionBy().size()
                || !spec.getOrderByDirections().equals(other.getOrderByDirections())
                || !isSameLiveViewDescription(spec.getLiveViewDescription(), other.getLiveViewDescription())) {
            return false;
        }
        for (int i = 0, n = spec.getPartitionBy().size(); i < n; i++) {
            if (LogicalPlans.isVolatile(spec.getPartitionBy().getQuick(i))
                    || !isSameExpression(spec.getPartitionBy().getQuick(i), other.getPartitionBy().getQuick(i))) {
                return false;
            }
        }
        final IntList orderIds = spec.getOrderByColumnIds();
        for (int i = 0, n = orderIds.size(); i < n; i++) {
            if (!isSameColumn(orderIds.getQuick(i), other.getOrderByColumnIds().getQuick(i), scope)) {
                return false;
            }
        }
        return true;
    }

    private static boolean readsColumn(BoundExpression expression, int columnId) {
        if (expression instanceof ColumnExpression column) {
            return column.getColumnId() == columnId;
        }
        if (expression instanceof FunctionExpression call) {
            for (int i = 0, n = call.getArgumentCount(); i < n; i++) {
                if (readsColumn(call.argumentAt(i), columnId)) {
                    return true;
                }
            }
        }
        return false;
    }

    private static void removeColumn(OutputSchema output, int columnId) {
        final int index = output.getColumnIndexById(columnId);
        if (index >= 0) {
            output.remove(index);
        }
    }

    private static void replaceColumn(IntList columnIds, int columnId, int replacementId) {
        for (int i = 0, n = columnIds.size(); i < n; i++) {
            if (columnIds.getQuick(i) == columnId) {
                columnIds.setQuick(i, replacementId);
            }
        }
    }

    /**
     * The consumers of a window result, from the window's parent up to the first projection or
     * aggregate, which defines its own columns. Returns the number of such ancestors, or -1 when one of
     * them is a node whose reads this pass does not redirect.
     */
    private int consumerCount(int columnId) {
        for (int i = ancestors.size() - 1; i >= 0; i--) {
            final LogicalPlan consumer = ancestors.getQuick(i);
            if (consumer instanceof ProjectPlan || consumer instanceof AggregatePlan) {
                return ancestors.size() - i;
            }
            final int index = consumer.getOutput().getColumnIndexById(columnId);
            if (!(consumer instanceof WindowPlan || consumer instanceof FilterPlan || consumer instanceof SortPlan
                    || consumer instanceof LimitPlan) || index >= 0 && index == consumer.getOutput().getTimestampIndex()) {
                return -1;
            }
        }
        return -1;
    }

    private int findEqualCall(WindowPlan window, int index) {
        final FunctionExpression call = window.getFunctions().getQuick(index);
        final WindowSpec spec = window.getSpecs().getQuick(index);
        if (LogicalPlans.isVolatile(call)) {
            return -1;
        }
        final LogicalPlan scope = window.getInput();
        for (int i = 0; i < index; i++) {
            if (isSameCall(call, window.getFunctions().getQuick(i)) && isSameSpec(spec, window.getSpecs().getQuick(i), scope)) {
                return window.getFunctionColumnIds().getQuick(i);
            }
        }
        for (LogicalPlan plan = scope; plan instanceof ProjectPlan || plan instanceof WindowPlan; plan = plan.inputAt(0)) {
            if (plan instanceof WindowPlan lower) {
                for (int i = 0, n = lower.getFunctions().size(); i < n; i++) {
                    final int columnId = lower.getFunctionColumnIds().getQuick(i);
                    if (scope.getOutput().getColumnIndexById(columnId) >= 0 && isSameCall(call, lower.getFunctions().getQuick(i))
                            && isSameSpec(spec, lower.getSpecs().getQuick(i), scope)) {
                        return columnId;
                    }
                }
            }
        }
        return -1;
    }

    private void mergeEqualCalls(WindowPlan window) {
        for (int i = 0; i < window.getFunctions().size(); i++) {
            final int columnId = window.getFunctionColumnIds().getQuick(i);
            final int equalId = findEqualCall(window, i);
            final int consumerCount = equalId < 0 ? -1 : consumerCount(columnId);
            if (consumerCount < 0) {
                continue;
            }
            for (int k = ancestors.size() - consumerCount, n = ancestors.size(); k < n; k++) {
                redirectReads(ancestors.getQuick(k), columnId, equalId);
            }
            window.getFunctions().remove(i);
            window.getSpecs().remove(i);
            window.getFunctionColumnIds().removeIndex(i);
            removeColumn(window.getOutput(), columnId);
            i--;
        }
        if (window.getFunctions().size() == 0 && ancestors.size() > 0) {
            final LogicalPlan parent = ancestors.getLast();
            for (int i = 0, n = parent.inputCount(); i < n; i++) {
                if (parent.inputAt(i) == window) {
                    parent.replaceInput(i, window.getInput());
                }
            }
        }
    }

    private void mergeWindowCalls0(LogicalPlan plan) {
        ancestors.add(plan);
        for (int i = 0, n = plan.inputCount(); i < n; i++) {
            mergeWindowCalls0(plan.inputAt(i));
        }
        ancestors.setPos(ancestors.size() - 1);
        if (plan instanceof WindowPlan window) {
            mergeEqualCalls(window);
        }
    }

    private void redirectReads(LogicalPlan consumer, int columnId, int replacementId) {
        switch (consumer) {
            case ProjectPlan project -> {
                final ObjList<BoundExpression> expressions = project.getExpressions();
                for (int i = 0, n = expressions.size(); i < n; i++) {
                    expressions.setQuick(i, redirectReads(expressions.getQuick(i), project, columnId, replacementId));
                }
            }
            case GroupingPlan aggregate -> {
                final ObjList<BoundExpression> keys = aggregate.getGroupingExpressions();
                for (int i = 0, n = keys.size(); i < n; i++) {
                    keys.setQuick(i, redirectReads(keys.getQuick(i), aggregate, columnId, replacementId));
                }
                final ObjList<FunctionExpression> calls = aggregate.getAggregates();
                for (int i = 0, n = calls.size(); i < n; i++) {
                    calls.setQuick(i, (FunctionExpression) redirectReads(calls.getQuick(i), aggregate, columnId, replacementId));
                }
                if (aggregate instanceof AggregatePlan shared) {
                    replaceColumn(shared.getSharedInputIds(), columnId, replacementId);
                }
            }
            case WindowPlan window -> {
                for (int i = 0, n = window.getFunctions().size(); i < n; i++) {
                    window.getFunctions().setQuick(i, (FunctionExpression) redirectReads(window.getFunctions().getQuick(i), window, columnId, replacementId));
                    final ObjList<BoundExpression> partitions = window.getSpecs().getQuick(i).getPartitionBy();
                    for (int k = 0, count = partitions.size(); k < count; k++) {
                        partitions.setQuick(k, redirectReads(partitions.getQuick(k), window, columnId, replacementId));
                    }
                    replaceColumn(window.getSpecs().getQuick(i).getOrderByColumnIds(), columnId, replacementId);
                }
                removeColumn(window.getOutput(), columnId);
            }
            case FilterPlan filter -> {
                filter.of(filter.getInput(), redirectReads(filter.getPredicate(), filter, columnId, replacementId), filter.getPosition());
                removeColumn(filter.getOutput(), columnId);
            }
            case SortPlan sort -> {
                replaceColumn(sort.getColumnIds(), columnId, replacementId);
                removeColumn(sort.getOutput(), columnId);
            }
            default -> removeColumn(consumer.getOutput(), columnId);
        }
    }

    private BoundExpression redirectReads(BoundExpression expression, LogicalPlan consumer, int columnId, int replacementId) {
        if (!readsColumn(expression, columnId)) {
            return expression;
        }
        final OutputSchema input = consumer.inputAt(0).getOutput();
        final ProjectPlan renaming = projects.next().of(consumer.inputAt(0), consumer.getPosition());
        renaming.getOutput().copyFrom(input);
        for (int i = 0, n = input.getColumnCount(); i < n; i++) {
            final int id = input.getColumnId(i);
            renaming.getExpressions().add(columns.next().of(id == columnId ? replacementId : id, input.getColumnType(i), consumer.getPosition()));
        }
        return context.getRewriter().remapColumns(expression, renaming);
    }

    void mergeWindowCalls(LogicalPlan root) {
        ancestors.clear();
        mergeWindowCalls0(root);
    }
}
