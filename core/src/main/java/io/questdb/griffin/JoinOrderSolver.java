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

import io.questdb.griffin.plan.logical.BoundExpression;
import io.questdb.griffin.plan.logical.ColumnExpression;
import io.questdb.griffin.plan.logical.FunctionExpression;
import io.questdb.griffin.plan.logical.JoinInput;
import io.questdb.griffin.plan.logical.JoinKind;
import io.questdb.griffin.plan.logical.JoinPlan;
import io.questdb.std.IntHashSet;
import io.questdb.std.IntList;
import io.questdb.std.IntSortedList;
import io.questdb.std.Mutable;
import io.questdb.std.ObjList;
import io.questdb.std.ObjectPool;
import org.jetbrains.annotations.TestOnly;

/**
 * The binding-time join-order constraint solver. The binder feeds it equi-join keys, ordering
 * constraints and late inputs, then binds the remaining predicates, types and designated timestamps
 * against the step order it selects. Every returned list is borrowed until clear(); no syntax or
 * runtime resources are retained.
 */
final class JoinOrderSolver implements Mutable {
    private final ObjList<JoinKind> bestJoinTypes = new ObjList<>();
    private final IntList bestOrder;
    private final IntList candidateOrder = new IntList();
    private final ObjectPool<Context> contextPool;
    private final ObjList<Context> contexts = new ObjList<>();
    private final ObjList<IntHashSet> dependencies = new ObjList<>();
    private final ObjectPool<IntHashSet> dependencyPool;
    private final ObjectPool<Equality> equalityPool;
    private final ObjList<JoinKind> joinTypes = new ObjList<>();
    private final IntList lateInputs = new IntList();
    private final IntHashSet markedIndexes;
    private final IntList orderingConstraints = new IntList();
    private final IntList pendingSources = new IntList();
    private final IntSortedList ready = new IntSortedList();
    private final IntList roots;
    private final ObjList<Equality> sourceFilters = new ObjList<>();
    private final ObjList<Context> stagedContexts = new ObjList<>();
    private final IntList stagedIndexes;
    private boolean isOrdered;
    private JoinPlan join;

    /**
     * Borrows the compiler's leaf temporaries as temporary lists of {@link #order()}.
     */
    JoinOrderSolver(int maxRetainedContexts, IntHashSet markedIndexes, IntList stagedIndexes, IntList bestOrder, IntList roots) {
        this.markedIndexes = markedIndexes;
        this.stagedIndexes = stagedIndexes;
        this.bestOrder = bestOrder;
        this.roots = roots;
        this.contextPool = new ObjectPool<>(Context::new, 8, maxRetainedContexts);
        this.dependencyPool = new ObjectPool<>(IntHashSet::new, 4, maxRetainedContexts);
        this.equalityPool = new ObjectPool<>(Equality::new, 8, maxRetainedContexts);
    }

    @TestOnly
    int getEqualityCapacity() {
        return equalityPool.getCapacity();
    }

    @Override
    public void clear() {
        candidateOrder.clear();
        contextPool.clear();
        contexts.clear();
        dependencies.clear();
        dependencyPool.clear();
        equalityPool.clear();
        joinTypes.clear();
        lateInputs.clear();
        orderingConstraints.clear();
        pendingSources.clear();
        ready.clear();
        sourceFilters.clear();
        stagedContexts.clear();
        isOrdered = false;
        join = null;
    }

    private void addContext(Context context) {
        final Context previous = contexts.getQuick(context.slave);
        contexts.setQuick(context.slave, previous == null ? context : merge(previous, context));
    }

    private void addParent(int parent, int child) {
        Context context = contexts.getQuick(child);
        if (context == null) {
            context = contextPool.next();
            context.slave = child;
            contexts.setQuick(child, context);
        }
        context.parents.add(parent);
        link(parent, child);
    }

    private void addUnnestDependencies(BoundExpression expression, int source) {
        if (expression instanceof ColumnExpression column) {
            for (int i = 0; i < source; i++) {
                if (join.getInputs().getQuick(i).getSourceOutput().getColumnIndexById(column.getColumnId()) >= 0) {
                    addParent(i, source);
                    return;
                }
            }
            throw new IllegalStateException("UNNEST argument is outside its preceding join inputs");
        }
        if (expression instanceof FunctionExpression function) {
            for (int i = 0, n = function.getArgumentCount(); i < n; i++) {
                addUnnestDependencies(function.argumentAt(i), source);
            }
        }
    }

    private boolean crossesBarrier(Context context) {
        int first = context.slave;
        for (int i = 0, n = context.parents.size(); i < n; i++) {
            first = Math.min(first, context.parents.get(i));
        }
        for (int i = first; i <= context.slave; i++) {
            if (isBarrier(joinTypes.getQuick(i))) {
                return true;
            }
        }
        return false;
    }

    private void emit(Equality a, boolean isLeftA, Equality b, boolean isLeftB, int deletedIndex) {
        markedIndexes.add(deletedIndex);
        final int leftSource = isLeftA ? a.leftSource : a.rightSource;
        final int leftId = isLeftA ? a.leftColumnId : a.rightColumnId;
        final int rightSource = isLeftB ? b.leftSource : b.rightSource;
        final int rightId = isLeftB ? b.leftColumnId : b.rightColumnId;
        if (leftSource == rightSource && leftId == rightId) {
            b.addOwners(a.originalOwners);
            return;
        }
        final Equality equality = equalityPool.next();
        equality.of(leftSource, leftId, isLeftA ? a.leftName : a.rightName, isLeftA ? a.leftPosition : a.rightPosition,
                rightSource, rightId, isLeftB ? b.leftName : b.rightName, isLeftB ? b.leftPosition : b.rightPosition);
        equality.addOwners(a.originalOwners);
        equality.addOwners(b.originalOwners);
        if (leftSource == rightSource) {
            sourceFilters.add(equality);
        } else {
            final Context context = contextPool.next();
            context.slave = Math.max(leftSource, rightSource);
            context.parents.add(Math.min(leftSource, rightSource));
            context.keys.add(equality);
            stagedContexts.add(context);
        }
    }

    private boolean hasOrderingConstraint(int parent, int child) {
        for (int i = 0, n = orderingConstraints.size(); i < n; i += 2) {
            if (orderingConstraints.getQuick(i) == parent && orderingConstraints.getQuick(i + 1) == child) {
                return true;
            }
        }
        return false;
    }

    private boolean hasOtherDependencyPath(Context source, int target) {
        pendingSources.clear();
        markedIndexes.clear();
        for (int i = 0, n = source.parents.size(); i < n; i++) {
            final int parent = source.parents.get(i);
            if (parent != target) {
                pendingSources.add(parent);
            }
        }
        while (pendingSources.size() > 0) {
            final int last = pendingSources.size() - 1;
            final int parent = pendingSources.getQuick(last);
            pendingSources.setPos(last);
            if (parent == target) {
                return true;
            }
            if (markedIndexes.add(parent)) {
                final Context context = contexts.getQuick(parent);
                if (context != null) {
                    for (int i = 0, n = context.parents.size(); i < n; i++) {
                        pendingSources.add(context.parents.get(i));
                    }
                }
            }
        }
        return false;
    }

    private void link(int parent, int child) {
        dependencies.getQuick(parent).add(child);
    }

    private Context merge(Context a, Context b) {
        assert a.slave == b.slave;
        if (crossesBarrier(a) || crossesBarrier(b)) {
            // Equality transitivity must not move a later INNER key into an
            // earlier outer match or a filter on its preserved input.
            b.keys.addAll(a.keys);
            b.parents.addAll(a.parents);
            return b;
        }
        markedIndexes.clear();
        final Context result = contextPool.next();
        result.slave = a.slave;
        for (int i = 0, n = b.keys.size(); i < n; i++) {
            final Equality next = b.keys.getQuick(i);
            for (int k = 0, count = a.keys.size(); k < count; k++) {
                final Equality previous = a.keys.getQuick(k);
                if (previous.leftColumnId == next.leftColumnId) {
                    emit(previous, false, next, false, k);
                    break;
                } else if (previous.rightColumnId == next.leftColumnId) {
                    emit(previous, true, next, false, k);
                    break;
                } else if (previous.leftColumnId == next.rightColumnId) {
                    emit(previous, false, next, true, k);
                    break;
                } else if (previous.rightColumnId == next.rightColumnId) {
                    emit(previous, true, next, true, k);
                    break;
                }
            }
            result.keys.add(next);
            final int parent = Math.min(next.leftSource, next.rightSource);
            result.parents.add(parent);
            link(parent, result.slave);
        }
        for (int i = 0, n = a.keys.size(); i < n; i++) {
            final Equality previous = a.keys.getQuick(i);
            final int parent = Math.min(previous.leftSource, previous.rightSource);
            if (markedIndexes.contains(i)) {
                if (result.parents.excludes(parent)) {
                    dependencies.getQuick(parent).remove(result.slave);
                }
            } else {
                result.keys.add(previous);
                result.parents.add(parent);
                link(parent, result.slave);
            }
        }
        return result;
    }

    private Context moveClauses(Context from, Context to) {
        final Context retained = contextPool.next();
        retained.slave = from.slave;
        int nextPosition = 0;
        for (int i = 0, n = from.keys.size(); i < n; i++) {
            final Context target;
            if (nextPosition < stagedIndexes.size() && i == stagedIndexes.getQuick(nextPosition)) {
                target = to;
                nextPosition++;
            } else {
                target = retained;
            }
            final Equality key = from.keys.getQuick(i);
            target.keys.add(key);
            final int parent = key.leftSource == target.slave ? key.rightSource : key.leftSource;
            target.parents.add(parent);
            // Old reverse edges may remain; only a context's current parent
            // count admits it to the queue.
            link(parent, target.slave);
        }
        return retained;
    }

    private void reorder() {
        bestJoinTypes.clear();
        bestOrder.clear();
        roots.clear();
        stagedContexts.clear();
        for (int i = 0, n = contexts.size(); i < n; i++) {
            final Context context = contexts.getQuick(i);
            if (context == null || context.parents.size() == 0) {
                roots.add(i);
            }
        }
        int bestCost = Integer.MAX_VALUE;
        boolean hasCandidate = false;
        for (int candidate = 0, rootCount = roots.size(); candidate < rootCount; candidate++) {
            for (int i = 0; i < rootCount; i++) {
                if (candidate != i) {
                    final int target = roots.getQuick(i);
                    // Scans around the position in the roots list, not around the target source ordinal.
                    for (int from = i - 1; from >= 0; from--) {
                        if (isBarrier(joinTypes.getQuick(from))) {
                            break;
                        }
                        swap(target, from);
                    }
                    for (int from = i + 1, n = contexts.size(); from < n; from++) {
                        if (isBarrier(joinTypes.getQuick(from))) {
                            break;
                        }
                        swap(target, from);
                    }
                }
            }
            for (int i = 0, n = orderingConstraints.size(); i < n; i += 2) {
                addParent(orderingConstraints.getQuick(i), orderingConstraints.getQuick(i + 1));
            }
            final int cost = topologicalOrder();
            if (!hasCandidate || cost < bestCost) {
                bestCost = cost;
                hasCandidate = true;
                bestOrder.clear();
                bestOrder.addAll(candidateOrder);
                bestJoinTypes.clear();
                bestJoinTypes.addAll(joinTypes);
                stagedContexts.clear();
                for (int source = 0, n = contexts.size(); source < n; source++) {
                    final Context context = contexts.getQuick(source);
                    if (context == null) {
                        stagedContexts.add(null);
                    } else {
                        final Context snapshot = contextPool.next();
                        snapshot.slave = context.slave;
                        snapshot.keys.addAll(context.keys);
                        snapshot.parents.addAll(context.parents);
                        stagedContexts.add(snapshot);
                    }
                }
            }
        }
        // Candidate search mutates contexts. Publish the winning order's keys,
        // not the final candidate's keys under an earlier candidate's order.
        contexts.clear();
        contexts.addAll(stagedContexts);
        joinTypes.clear();
        joinTypes.addAll(bestJoinTypes);
    }

    private void requireCollecting() {
        if (join == null || isOrdered) {
            throw new IllegalStateException("join analysis is not collecting predicates");
        }
    }

    private void swap(int target, int source) {
        final Context from = contexts.getQuick(source);
        if (isBarrier(joinTypes.getQuick(target)) || from == null || !from.parents.contains(target)
                || hasOrderingConstraint(target, source)) {
            return;
        }
        stagedIndexes.clear();
        for (int i = 0, n = from.keys.size(); i < n; i++) {
            final Equality key = from.keys.getQuick(i);
            if (key.leftSource == target || key.rightSource == target) {
                stagedIndexes.add(i);
            }
        }
        if (stagedIndexes.size() > 0 && stagedIndexes.size() < from.keys.size() && !hasOtherDependencyPath(from, target)) {
            // Reversing a direct edge while another path reaches the same donor
            // would introduce a cycle. Only reverse an independent edge.
            // An earlier donor may already have created this context. Reusing
            // the root search's original null would discard its stolen keys.
            Context targetContext = contexts.getQuick(target);
            if (targetContext == null) {
                targetContext = contextPool.next();
                contexts.setQuick(target, targetContext);
            }
            targetContext.slave = target;
            contexts.setQuick(source, moveClauses(from, targetContext));
            if (joinTypes.getQuick(target) == JoinKind.CROSS) {
                joinTypes.setQuick(target, JoinKind.INNER);
            }
        }
    }

    private int topologicalOrder() {
        stagedIndexes.clear();
        candidateOrder.clear();
        pendingSources.clear();
        ready.clear();
        for (int i = 0, n = contexts.size(); i < n; i++) {
            final Context context = contexts.getQuick(i);
            if (context == null || context.parents.size() == 0) {
                if (dependencies.getQuick(i).size() > 0) {
                    ready.add(i);
                } else {
                    stagedIndexes.add(i);
                }
            } else {
                context.inCount = context.parents.size();
            }
        }
        int cost = 0;
        int heldCount = 0;
        while (true) {
            final int source;
            if (ready.notEmpty()) {
                source = ready.poll();
                if (lateInputs.contains(source) && dependencies.getQuick(source).size() == 0) {
                    pendingSources.add(source);
                    continue;
                }
            } else if (heldCount < pendingSources.size()) {
                source = pendingSources.getQuick(heldCount++);
            } else {
                break;
            }
            candidateOrder.add(source);
            cost += joinTypes.getQuick(source) == JoinKind.CROSS ? 10 : 5;
            final IntHashSet children = dependencies.getQuick(source);
            for (int i = 0, n = children.size(); i < n; i++) {
                final int child = children.get(i);
                final Context context = contexts.getQuick(child);
                if (context != null && --context.inCount == 0) {
                    ready.add(child);
                }
            }
        }
        for (int i = 0, n = contexts.size(); i < n; i++) {
            final Context context = contexts.getQuick(i);
            if (context != null && context.inCount > 0) {
                return Integer.MAX_VALUE;
            }
        }
        candidateOrder.addAll(stagedIndexes);
        return cost;
    }

    private void validateColumn(int source, int columnId) {
        if (source < 0 || source >= contexts.size()
                || join.getInputs().getQuick(source).getSourceOutput().getColumnIndexById(columnId) < 0) {
            throw new IllegalArgumentException("join equality column is outside its source");
        }
    }

    private void validateOrder() {
        markedIndexes.clear();
        for (int i = 0, n = bestOrder.size(); i < n; i++) {
            final int source = bestOrder.getQuick(i);
            if (!markedIndexes.add(source)) {
                throw new IllegalStateException("duplicate input in logical join order");
            }
            final Context context = contexts.getQuick(source);
            if (context != null) {
                for (int k = 0, count = context.keys.size(); k < count; k++) {
                    final Equality key = context.keys.getQuick(k);
                    final int parent = key.leftSource == source ? key.rightSource : key.leftSource;
                    if (key.leftSource != source && key.rightSource != source || parent == source || markedIndexes.excludes(parent)) {
                        throw new IllegalStateException("join key parent is not ordered before its slave");
                    }
                }
            }
        }
    }

    static boolean isBarrier(JoinKind joinType) {
        return joinType != JoinKind.INNER && joinType != JoinKind.CROSS;
    }

    /**
     * The child reads columns of the parent without an equality between them, as a dependent join step
     * reads the columns of the inputs before it.
     */
    void addDependency(int parent, int child) {
        requireCollecting();
        addParent(parent, child);
    }

    /**
     * The caller visits WHERE before ON occurrences.
     */
    void addEquality(
            int leftSource,
            int leftColumnId,
            CharSequence leftName,
            int leftPosition,
            int rightSource,
            int rightColumnId,
            CharSequence rightName,
            int rightPosition,
            int originalOnSource
    ) {
        requireCollecting();
        if (originalOnSource < -1 || originalOnSource >= contexts.size()) {
            throw new IllegalArgumentException("invalid join predicate origin");
        }
        validateColumn(leftSource, leftColumnId);
        validateColumn(rightSource, rightColumnId);
        final Equality equality = equalityPool.next();
        equality.of(leftSource, leftColumnId, leftName, leftPosition,
                rightSource, rightColumnId, rightName, rightPosition);
        equality.originalOwners.add(originalOnSource);
        if (leftSource == rightSource) {
            // An original x=x remains a scalar predicate; only a tautology
            // derived from two already retained equalities can disappear.
            sourceFilters.add(equality);
            return;
        }
        // Canonicalize the first endpoint to the lower source.
        if (leftSource > rightSource) {
            equality.reverse();
        }
        final Context context = contextPool.next();
        context.slave = equality.rightSource;
        context.keys.add(equality);
        context.parents.add(equality.leftSource);
        addContext(context);
        link(equality.leftSource, equality.rightSource);
    }

    /**
     * Holds the input while nothing depends on it until no other input is ready, so a context-free outer join goes last.
     */
    void addLateInput(int source) {
        requireCollecting();
        lateInputs.add(source);
    }

    void addOrderingConstraint(int parent, int child) {
        requireCollecting();
        if (parent != child) {
            orderingConstraints.add(parent);
            orderingConstraints.add(child);
        }
    }

    ObjList<Equality> getSourceFilters() {
        return sourceFilters;
    }

    boolean hasJoinDependency(int source) {
        final Context context = contexts.getQuick(source);
        if (context != null && context.parents.size() > 0 || dependencies.getQuick(source).size() > 0) {
            return true;
        }
        for (int i = 0, n = orderingConstraints.size(); i < n; i++) {
            if (orderingConstraints.getQuick(i) == source) {
                return true;
            }
        }
        return false;
    }

    void of(JoinPlan join) {
        clear();
        if (join.getOrderedInputs().size() != 0) {
            throw new IllegalStateException("join order has already been selected");
        }
        for (int i = 0, n = join.getInputs().size(); i < n; i++) {
            final JoinInput input = join.getInputs().getQuick(i);
            final JoinKind type = input.getJoinType();
            if (input.getMasterKeyColumnIds().size() != 0 || input.getSlaveKeyColumnIds().size() != 0) {
                throw new IllegalStateException("join keys have already been published");
            }
            contexts.add(null);
            dependencies.add(dependencyPool.next());
            joinTypes.add(type);
        }
        this.join = join;
    }

    /**
     * Selects once and publishes only key values and input identities; false when dependencies are cyclic.
     */
    boolean order() {
        requireCollecting();
        // Merging an emitted context can emit another equality at a lower
        // source ordinal. Drain the queue: a captured size silently loses those
        // equalities. The decreasing owner ordinal bounds this local revisit.
        for (int i = 0; i < stagedContexts.size(); i++) {
            addContext(stagedContexts.getQuick(i));
        }
        // A newly emitted context may not have passed through addEquality.
        // Derive every dependency from the surviving contexts before ordering.
        for (int i = 0, n = dependencies.size(); i < n; i++) {
            dependencies.getQuick(i).clear();
        }
        for (int i = 0, n = contexts.size(); i < n; i++) {
            final Context context = contexts.getQuick(i);
            if (context != null) {
                for (int k = 0, count = context.parents.size(); k < count; k++) {
                    link(context.parents.get(k), i);
                }
            }
        }
        for (int i = 0, n = contexts.size(); i < n; i++) {
            final Context context = contexts.getQuick(i);
            final JoinKind type = joinTypes.getQuick(i);
            if (!isBarrier(type)) {
                joinTypes.setQuick(i, context != null && context.parents.size() > 0 ? JoinKind.INNER : JoinKind.CROSS);
            } else if (type.isTemporal()) {
                addParent(0, i);
            } else if (type == JoinKind.UNNEST) {
                if (i == 0) {
                    throw new IllegalStateException("UNNEST requires a master input");
                }
                addParent(0, i);
                final ObjList<BoundExpression> expressions = join.getInputs().getQuick(i).getUnnest().getExpressions();
                for (int k = 0, count = expressions.size(); k < count; k++) {
                    addUnnestDependencies(expressions.getQuick(k), i);
                }
            }
        }
        reorder();
        if (bestOrder.size() != contexts.size()) {
            return false;
        }
        validateOrder();
        final ObjList<JoinInput> inputs = join.getInputs();
        for (int i = 0, n = bestOrder.size(); i < n; i++) {
            final int source = bestOrder.getQuick(i);
            final JoinInput input = inputs.getQuick(source);
            input.setJoinType(joinTypes.getQuick(source));
            final Context context = contexts.getQuick(source);
            if (context != null) {
                for (int k = 0, count = context.keys.size(); k < count; k++) {
                    final Equality key = context.keys.getQuick(k);
                    final boolean isLeftSlave = key.leftSource == source;
                    input.getMasterKeyColumnIds().add(isLeftSlave ? key.rightColumnId : key.leftColumnId);
                    input.getSlaveKeyColumnIds().add(isLeftSlave ? key.leftColumnId : key.rightColumnId);
                    input.getMasterKeyNames().add(isLeftSlave ? key.rightName : key.leftName);
                    input.getSlaveKeyNames().add(isLeftSlave ? key.leftName : key.rightName);
                    input.getKeyPositions().add(isLeftSlave ? key.leftPosition : key.rightPosition);
                }
            }
            join.getOrderedInputs().add(input);
        }
        isOrdered = true;
        return true;
    }

    private static final class Context implements Mutable {
        private final ObjList<Equality> keys = new ObjList<>();
        private final IntHashSet parents = new IntHashSet(4);
        private int inCount;
        private int slave = -1;

        @Override
        public void clear() {
            keys.clear();
            parents.clear();
            inCount = 0;
            slave = -1;
        }
    }

    static final class Equality implements Mutable {
        final IntList originalOwners = new IntList();
        int leftColumnId;
        CharSequence leftName;
        int leftPosition;
        int leftSource;
        int rightColumnId;
        CharSequence rightName;
        int rightPosition;
        int rightSource;

        @Override
        public void clear() {
            originalOwners.clear();
            leftColumnId = rightColumnId = -1;
            leftName = rightName = null;
            leftPosition = rightPosition = -1;
            leftSource = rightSource = -1;
        }

        private void addOwners(IntList owners) {
            for (int i = 0, n = owners.size(); i < n; i++) {
                final int owner = owners.getQuick(i);
                if (originalOwners.indexOf(owner, 0, originalOwners.size()) < 0) {
                    originalOwners.add(owner);
                }
            }
        }

        private void of(int leftSource, int leftColumnId, CharSequence leftName, int leftPosition,
                        int rightSource, int rightColumnId, CharSequence rightName, int rightPosition) {
            this.leftSource = leftSource;
            this.leftColumnId = leftColumnId;
            this.leftName = leftName;
            this.leftPosition = leftPosition;
            this.rightSource = rightSource;
            this.rightColumnId = rightColumnId;
            this.rightName = rightName;
            this.rightPosition = rightPosition;
        }

        private void reverse() {
            final int source = leftSource;
            final int id = leftColumnId;
            final CharSequence name = leftName;
            final int position = leftPosition;
            leftSource = rightSource;
            leftColumnId = rightColumnId;
            leftName = rightName;
            leftPosition = rightPosition;
            rightSource = source;
            rightColumnId = id;
            rightName = name;
            rightPosition = position;
        }
    }
}
