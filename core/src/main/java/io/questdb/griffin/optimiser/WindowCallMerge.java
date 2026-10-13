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

package io.questdb.griffin.optimiser;

import io.questdb.griffin.LogicalPlans;
import io.questdb.griffin.engine.window.LiveViewWindowDescription;
import io.questdb.griffin.plan.logical.AggregatePlan;
import io.questdb.griffin.plan.logical.BindVariableExpression;
import io.questdb.griffin.plan.logical.BoundExpression;
import io.questdb.griffin.plan.logical.ColumnExpression;
import io.questdb.griffin.plan.logical.ConstantExpression;
import io.questdb.griffin.plan.logical.ExpressionVisitor;
import io.questdb.griffin.plan.logical.FilterPlan;
import io.questdb.griffin.plan.logical.ForwardingPlan;
import io.questdb.griffin.plan.logical.FunctionExpression;
import io.questdb.griffin.plan.logical.LimitPlan;
import io.questdb.griffin.plan.logical.LogicalPlan;
import io.questdb.griffin.plan.logical.OutputSchema;
import io.questdb.griffin.plan.logical.PlanExpressionVisitor;
import io.questdb.griffin.plan.logical.ProjectPlan;
import io.questdb.griffin.plan.logical.SortPlan;
import io.questdb.griffin.plan.logical.TreeWalk;
import io.questdb.griffin.plan.logical.TypeExpression;
import io.questdb.griffin.plan.logical.WindowPlan;
import io.questdb.griffin.plan.logical.WindowSpec;
import io.questdb.std.Chars;
import io.questdb.std.IntIntHashMap;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;
import io.questdb.std.ObjectPool;
import org.jetbrains.annotations.TestOnly;

/**
 * Computes identical window calls once. A call merges into an equal earlier call of its window, or of a
 * window beneath it whose result reaches it, when both read the same values under the same
 * specification and neither evaluates a volatile function; its consumers then read the earlier result.
 */
final class WindowCallMerge implements OptimiserPass, PlanExpressionVisitor, ExpressionVisitor {
    private final ObjList<LogicalPlan> ancestors;
    private final IntIntHashMap callHeads;
    private final IntList callNext;
    private final ObjectPool<ColumnExpression> columns;
    private final OptimiserContext context;
    private final IntList keptIndexes;
    private final IntList lowerCallIndexes;
    private final ObjList<WindowPlan> lowerWindows = new ObjList<>();
    private final IntIntHashMap mergedIds = new IntIntHashMap();
    private final ProjectPlan renaming;
    private long callComparisonCount;
    private LogicalPlan redirectConsumer;

    WindowCallMerge(OptimiserContext context, ObjectPool<ColumnExpression> columns, ProjectPlan renaming, ObjList<LogicalPlan> ancestors,
                    IntList keptIndexes, IntIntHashMap callHeads, IntList callNext, IntList lowerCallIndexes) {
        this.context = context;
        this.columns = columns;
        this.renaming = renaming;
        this.ancestors = ancestors;
        this.keptIndexes = keptIndexes;
        this.callHeads = callHeads;
        this.callNext = callNext;
        this.lowerCallIndexes = lowerCallIndexes;
    }

    @Override
    public LogicalPlan apply(LogicalPlan plan) {
        mergeWindowCalls(plan);
        return plan;
    }

    @Override
    public void clear() {
        lowerWindows.clear();
        callComparisonCount = 0;
        redirectConsumer = null;
    }

    @TestOnly
    public long getCallComparisonCount() {
        return callComparisonCount;
    }

    @Override
    public String getName() {
        return "window merge";
    }

    @Override
    public int visit(BoundExpression expression) {
        return expression instanceof ColumnExpression column && mergedIds.keyIndex(column.getColumnId()) < 0 ? TreeWalk.STOP : TreeWalk.CONTINUE;
    }

    @Override
    public int visitColumnId(int columnId, int position) {
        final int equalId = mergedIds.get(columnId);
        return equalId < 0 ? columnId : equalId;
    }

    @Override
    public BoundExpression visitExpression(BoundExpression expression) {
        if (expression.walk(this)) {
            return expression;
        }
        if (renaming.getChild() == null) {
            final LogicalPlan input = redirectConsumer.inputAt(0);
            final OutputSchema output = input.getOutput();
            renaming.of(input, redirectConsumer.getPosition());
            renaming.getOutput().copyFrom(output);
            for (int i = 0, n = output.getColumnCount(); i < n; i++) {
                renaming.getExpressions().add(columns.next().of(visitColumnId(output.getColumnId(i), -1), output.getColumnType(i), redirectConsumer.getPosition()));
            }
        }
        return context.getRewriter().remapColumns(expression, renaming);
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

    /**
     * A hash that agrees with {@link #isSameCall} and {@link #isSameSpec}: calls that merge hash equally.
     */
    private static int hashCall(FunctionExpression call, WindowSpec spec) {
        int hash = hashCall(call);
        hash = 31 * hash + spec.getFramingMode();
        hash = 31 * hash + Long.hashCode(spec.getRowsLo());
        hash = 31 * hash + Long.hashCode(spec.getRowsHi());
        hash = 31 * hash + spec.getRowsLoExprTimeUnit();
        hash = 31 * hash + spec.getRowsHiExprTimeUnit();
        hash = 31 * hash + spec.getExclusionKind();
        hash = 31 * hash + (spec.isIgnoreNulls() ? 1 : 0);
        hash = 31 * hash + (spec.isSubsampleKeepFlag() ? 1 : 0);
        for (int i = 0, n = spec.getOrderByDirections().size(); i < n; i++) {
            hash = 31 * hash + spec.getOrderByDirections().getQuick(i).ordinal();
        }
        for (int i = 0, n = spec.getPartitionBy().size(); i < n; i++) {
            hash = 31 * hash + hashExpression(spec.getPartitionBy().getQuick(i));
        }
        return hash;
    }

    private static int hashCall(FunctionExpression call) {
        int hash = System.identityHashCode(call.getOverload());
        hash = 31 * hash + call.getFunctionFlags();
        hash = 31 * hash + (call.isProjectedOffset() ? 1 : 0);
        hash = 31 * hash + (call.isSetOperation() ? 1 : 0);
        for (int i = 0, n = call.getArgumentCount(); i < n; i++) {
            hash = 31 * hash + hashExpression(call.argumentAt(i));
        }
        return hash;
    }

    private static int hashExpression(BoundExpression expression) {
        return 31 * expression.getDataType() + switch (expression) {
            case ColumnExpression column -> 2 * column.getColumnId() + (column.isCast() ? 1 : 0);
            case ConstantExpression constant -> constant.valueHash();
            case BindVariableExpression variable -> variable.getName() == null ? 0 : Chars.hashCode(variable.getName());
            case FunctionExpression call -> hashCall(call);
            default -> 0;
        };
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

    /**
     * The result of the equal call of the window at the lowest index, or else of the first equal call beneath it, or -1.
     * A candidate below the window's call count is its call at that index; one above it is a call beneath the window.
     */
    private int findEqualCall(WindowPlan window, int index, int hash) {
        final FunctionExpression call = window.getFunctions().getQuick(index);
        final WindowSpec spec = window.getSpecs().getQuick(index);
        final LogicalPlan scope = window.getChild();
        final int count = window.getFunctions().size();
        int equal = -1;
        for (int candidate = callHeads.get(hash); candidate >= 0; candidate = callNext.getQuick(candidate)) {
            callComparisonCount++;
            final WindowPlan owner = candidate < count ? window : lowerWindows.getQuick(candidate - count);
            final int ownerIndex = candidate < count ? candidate : lowerCallIndexes.getQuick(candidate - count);
            if ((equal < 0 || candidate < equal) && isSameCall(call, owner.getFunctions().getQuick(ownerIndex))
                    && isSameSpec(spec, owner.getSpecs().getQuick(ownerIndex), scope)) {
                equal = candidate;
            }
        }
        if (equal < 0) {
            return -1;
        }
        return equal < count ? window.getFunctionColumnIds().getQuick(equal)
                : lowerWindows.getQuick(equal - count).getFunctionColumnIds().getQuick(lowerCallIndexes.getQuick(equal - count));
    }

    private void indexCall(int candidate, int hash) {
        final int keyIndex = callHeads.keyIndex(hash);
        callNext.setQuick(candidate, keyIndex < 0 ? callHeads.valueAt(keyIndex) : -1);
        callHeads.putAt(keyIndex, hash, candidate);
    }

    /**
     * Indexes the non-volatile calls of the windows beneath the window whose results reach it.
     */
    private void indexLowerCalls(WindowPlan window) {
        if (callHeads.size() > 0) {
            callHeads.clear();
        }
        lowerWindows.clear();
        lowerCallIndexes.clear();
        final LogicalPlan scope = window.getChild();
        for (LogicalPlan plan = scope; plan instanceof ProjectPlan || plan instanceof WindowPlan; plan = plan.inputAt(0)) {
            if (plan instanceof WindowPlan lower) {
                for (int i = 0, n = lower.getFunctions().size(); i < n; i++) {
                    if (scope.getOutput().getColumnIndexById(lower.getFunctionColumnIds().getQuick(i)) >= 0
                            && !LogicalPlans.isVolatile(lower.getFunctions().getQuick(i))) {
                        lowerWindows.add(lower);
                        lowerCallIndexes.add(i);
                    }
                }
            }
        }
        final int count = window.getFunctions().size();
        callNext.setPos(count + lowerWindows.size());
        for (int i = 0, n = lowerWindows.size(); i < n; i++) {
            final WindowPlan lower = lowerWindows.getQuick(i);
            final int lowerIndex = lowerCallIndexes.getQuick(i);
            indexCall(count + i, hashCall(lower.getFunctions().getQuick(lowerIndex), lower.getSpecs().getQuick(lowerIndex)));
        }
    }

    private void mergeEqualCalls(WindowPlan window) {
        if (mergedIds.size() > 0) {
            mergedIds.clear();
        }
        indexLowerCalls(window);
        int consumerCount = -1;
        for (int i = 0, n = window.getFunctions().size(); i < n; i++) {
            final FunctionExpression call = window.getFunctions().getQuick(i);
            if (LogicalPlans.isVolatile(call)) {
                continue;
            }
            final int hash = hashCall(call, window.getSpecs().getQuick(i));
            final int equalId = findEqualCall(window, i, hash);
            if (equalId >= 0) {
                final int columnId = window.getFunctionColumnIds().getQuick(i);
                final int count = consumerCount(columnId);
                if (count >= 0) {
                    consumerCount = count;
                    mergedIds.put(columnId, equalId);
                    continue;
                }
            }
            indexCall(i, hash);
        }
        if (mergedIds.size() > 0) {
            final int lo = ancestors.size() - consumerCount;
            for (int k = lo, n = ancestors.size(); k < n; k++) {
                redirectReads(ancestors.getQuick(k));
            }
            removeMergedCalls(window);
            removeMergedColumns(window.getOutput());
            for (int k = ancestors.size() - 1; k >= lo; k--) {
                switch (ancestors.getQuick(k)) {
                    case ForwardingPlan forwarding -> forwarding.deriveOutput();
                    case WindowPlan consumer -> removeMergedColumns(consumer.getOutput());
                    default -> {
                    }
                }
            }
        }
        if (window.getFunctions().size() == 0 && ancestors.size() > 0) {
            final LogicalPlan parent = ancestors.getLast();
            for (int i = 0, n = parent.inputCount(); i < n; i++) {
                if (parent.inputAt(i) == window) {
                    parent.replaceInput(i, window.getChild());
                }
            }
        }
    }

    private void mergeWindowCalls(LogicalPlan root) {
        ancestors.clear();
        mergeWindowCalls0(root);
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

    private void redirectReads(LogicalPlan consumer) {
        redirectConsumer = consumer;
        renaming.clear();
        try {
            consumer.visitReads(this);
        } finally {
            redirectConsumer = null;
        }
    }

    private void removeMergedCalls(WindowPlan window) {
        final ObjList<FunctionExpression> functions = window.getFunctions();
        final ObjList<WindowSpec> specs = window.getSpecs();
        final IntList columnIds = window.getFunctionColumnIds();
        final int count = functions.size();
        int kept = 0;
        for (int i = 0; i < count; i++) {
            final int columnId = columnIds.getQuick(i);
            if (mergedIds.keyIndex(columnId) > -1) {
                functions.setQuick(kept, functions.getQuick(i));
                specs.setQuick(kept, specs.getQuick(i));
                columnIds.setQuick(kept++, columnId);
            }
        }
        functions.remove(kept, count - 1);
        specs.remove(kept, count - 1);
        columnIds.setPos(kept);
    }

    private void removeMergedColumns(OutputSchema output) {
        keptIndexes.clear();
        for (int i = 0, n = output.getColumnCount(); i < n; i++) {
            if (mergedIds.keyIndex(output.getColumnId(i)) > -1) {
                keptIndexes.add(i);
            }
        }
        if (keptIndexes.size() < output.getColumnCount()) {
            output.retain(keptIndexes);
        }
    }
}
