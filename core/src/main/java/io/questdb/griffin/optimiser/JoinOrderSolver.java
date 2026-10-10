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

import io.questdb.griffin.PlanNodePools;
import io.questdb.griffin.plan.logical.JoinDependency;
import io.questdb.griffin.plan.logical.JoinEquality;
import io.questdb.griffin.plan.logical.JoinGraph;
import io.questdb.griffin.plan.logical.JoinInput;
import io.questdb.griffin.plan.logical.JoinKind;
import io.questdb.griffin.plan.logical.JoinPlan;
import io.questdb.std.IntHashSet;
import io.questdb.std.IntList;
import io.questdb.std.IntSortedList;
import io.questdb.std.Mutable;
import io.questdb.std.ObjList;
import io.questdb.std.ObjectPool;

/**
 * Selects the order of a join of more than two inputs from the {@link JoinGraph} binding collected for it, with the
 * first input first, and publishes the keys, join types and ordered inputs ({@link #order(JoinPlan)}). Dependencies
 * come from the statement's plan-node pools; every other list is borrowed until {@link #clear()}.
 */
final class JoinOrderSolver implements Mutable {
    private final ObjList<JoinKind> bestJoinTypes = new ObjList<>();
    private final IntList bestOrder;
    private final IntList candidateOrder = new IntList();
    private final ObjList<IntHashSet> children = new ObjList<>();
    private final ObjectPool<IntHashSet> childrenPool;
    private final ObjectPool<JoinDependency> dependencyPool;
    private final IntList inCounts = new IntList();
    private final ObjList<JoinKind> joinTypes = new ObjList<>();
    private final IntHashSet markedIndexes;
    private final ObjList<JoinEquality> movedKeys = new ObjList<>();
    private final IntList pendingSources = new IntList();
    private final IntSortedList ready = new IntSortedList();
    private final IntList roots;
    private final IntList semanticEdges = new IntList();
    private final ObjList<JoinDependency> stagedDependencies = new ObjList<>();
    private final IntList stagedIndexes;
    private ObjList<JoinDependency> dependencies;
    private JoinGraph graph;
    private JoinPlan join;

    /**
     * Borrows {@code markedIndexes}, {@code stagedIndexes}, {@code bestOrder} and {@code roots} as temporaries of
     * one call and leaves them empty.
     */
    JoinOrderSolver(PlanNodePools planNodes, IntHashSet markedIndexes, IntList stagedIndexes, IntList bestOrder, IntList roots) {
        this.childrenPool = new ObjectPool<>(IntHashSet::new, 4, planNodes.maxRetainedJoinContexts);
        this.dependencyPool = planNodes.joinDependencies;
        this.markedIndexes = markedIndexes;
        this.stagedIndexes = stagedIndexes;
        this.bestOrder = bestOrder;
        this.roots = roots;
    }

    @Override
    public void clear() {
        bestJoinTypes.clear();
        bestOrder.clear();
        candidateOrder.clear();
        children.clear();
        childrenPool.clear();
        inCounts.clear();
        joinTypes.clear();
        markedIndexes.clear();
        movedKeys.clear();
        pendingSources.clear();
        ready.clear();
        roots.clear();
        semanticEdges.clear();
        stagedDependencies.clear();
        stagedIndexes.clear();
        dependencies = null;
        graph = null;
        join = null;
    }

    /**
     * Selects the order of a join binding left a graph for, with its first input first, and publishes
     * the keys, join types and ordered inputs; the caller places the graph's filter conjuncts. An input binding added
     * after the graph joins as an unkeyed CROSS step.
     */
    void order(JoinPlan join) {
        of(join, join.getGraph());
        try {
            for (int i = dependencies.size(), n = join.getInputs().size(); i < n; i++) {
                dependencies.add(null);
            }
            linkDependencies();
            for (int i = 0, n = dependencies.size(); i < n; i++) {
                final JoinDependency dependency = dependencies.getQuick(i);
                final JoinKind type = joinTypes.getQuick(i);
                if (!type.isBarrier()) {
                    joinTypes.setQuick(i, dependency != null && dependency.getParents().size() > 0 ? JoinKind.INNER : JoinKind.CROSS);
                } else if (type.isTemporal() || type == JoinKind.UNNEST) {
                    if (i == 0 && type == JoinKind.UNNEST) {
                        throw new IllegalStateException("UNNEST requires a master input");
                    }
                    semanticEdges.clear();
                    JoinGraph.addSemanticEdges(join.getInputs(), i, type, semanticEdges);
                    for (int k = 0, count = semanticEdges.size(); k < count; k += 2) {
                        addParent(semanticEdges.getQuick(k), i);
                    }
                }
            }
            reorder();
            validateOrder();
            final ObjList<JoinInput> inputs = join.getInputs();
            for (int i = 0, n = bestOrder.size(); i < n; i++) {
                final int source = bestOrder.getQuick(i);
                final JoinInput input = inputs.getQuick(source);
                input.setJoinType(joinTypes.getQuick(source));
                final JoinDependency dependency = dependencies.getQuick(source);
                if (dependency != null) {
                    final ObjList<JoinEquality> keys = dependency.getKeys();
                    for (int k = 0, count = keys.size(); k < count; k++) {
                        final JoinEquality key = keys.getQuick(k);
                        final boolean isLeftSlave = key.getLeftSource() == source;
                        input.getMasterKeyColumnIds().add(isLeftSlave ? key.getRightColumnId() : key.getLeftColumnId());
                        input.getSlaveKeyColumnIds().add(isLeftSlave ? key.getLeftColumnId() : key.getRightColumnId());
                        input.getMasterKeyNames().add(isLeftSlave ? key.getRightName() : key.getLeftName());
                        input.getSlaveKeyNames().add(isLeftSlave ? key.getLeftName() : key.getRightName());
                        input.getKeyPositions().add(isLeftSlave ? key.getLeftPosition() : key.getRightPosition());
                    }
                }
                join.getOrderedInputs().add(input);
            }
        } finally {
            clear();
        }
    }

    private void addParent(int parent, int child) {
        JoinDependency dependency = dependencies.getQuick(child);
        if (dependency == null) {
            dependency = dependencyPool.next().of(child);
            dependencies.setQuick(child, dependency);
        }
        dependency.getParents().add(parent);
        link(parent, child);
    }

    /**
     * Appends the source to the candidate order, readies the inputs that waited for it alone, and returns its cost.
     */
    private int appendToOrder(int source) {
        candidateOrder.add(source);
        final IntHashSet sourceChildren = children.getQuick(source);
        for (int i = 0, n = sourceChildren.size(); i < n; i++) {
            final int child = sourceChildren.get(i);
            if (dependencies.getQuick(child) != null) {
                final int inCount = inCounts.getQuick(child) - 1;
                inCounts.setQuick(child, inCount);
                if (inCount == 0) {
                    ready.add(child);
                }
            }
        }
        return joinTypes.getQuick(source) == JoinKind.CROSS ? 10 : 5;
    }

    private boolean hasOrderingConstraint(int parent, int child) {
        final IntList constraints = graph.getOrderingConstraints();
        for (int i = 0, n = constraints.size(); i < n; i += 2) {
            if (constraints.getQuick(i) == parent && constraints.getQuick(i + 1) == child) {
                return true;
            }
        }
        return false;
    }

    private boolean hasOtherDependencyPath(JoinDependency source, int target) {
        pendingSources.clear();
        markedIndexes.clear();
        final IntHashSet sourceParents = source.getParents();
        for (int i = 0, n = sourceParents.size(); i < n; i++) {
            final int parent = sourceParents.get(i);
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
                final JoinDependency dependency = dependencies.getQuick(parent);
                if (dependency != null) {
                    final IntHashSet parents = dependency.getParents();
                    for (int i = 0, n = parents.size(); i < n; i++) {
                        pendingSources.add(parents.get(i));
                    }
                }
            }
        }
        return false;
    }

    private boolean isParentless(int source) {
        final JoinDependency dependency = dependencies.getQuick(source);
        return dependency == null || dependency.getParents().size() == 0;
    }

    private void link(int parent, int child) {
        children.getQuick(parent).add(child);
    }

    private void linkDependencies() {
        for (int i = 0, n = children.size(); i < n; i++) {
            children.getQuick(i).clear();
        }
        for (int i = 0, n = dependencies.size(); i < n; i++) {
            final JoinDependency dependency = dependencies.getQuick(i);
            if (dependency != null) {
                final IntHashSet parents = dependency.getParents();
                for (int k = 0, count = parents.size(); k < count; k++) {
                    link(parents.get(k), i);
                }
            }
        }
    }

    private JoinDependency moveClauses(JoinDependency from, JoinDependency to) {
        final JoinDependency retained = dependencyPool.next().of(from.getSlave());
        int nextPosition = 0;
        final ObjList<JoinEquality> keys = from.getKeys();
        for (int i = 0, n = keys.size(); i < n; i++) {
            final JoinDependency target;
            if (nextPosition < stagedIndexes.size() && i == stagedIndexes.getQuick(nextPosition)) {
                target = to;
                nextPosition++;
            } else {
                target = retained;
            }
            final JoinEquality key = keys.getQuick(i);
            target.getKeys().add(key);
            final int parent = key.getOtherSource(target.getSlave());
            target.getParents().add(parent);
            // Old reverse edges may remain; only a dependency's current parent
            // count admits it to the queue.
            link(parent, target.getSlave());
        }
        return retained;
    }

    private void of(JoinPlan join, JoinGraph graph) {
        clear();
        this.join = join;
        this.graph = graph;
        this.dependencies = graph.getDependencies();
        final ObjList<JoinInput> inputs = join.getInputs();
        for (int i = 0, n = inputs.size(); i < n; i++) {
            children.add(childrenPool.next());
            joinTypes.add(inputs.getQuick(i).getJoinType());
        }
    }

    /**
     * Orders by the edges the join semantics impose alone and keys every INNER or CROSS step by the equalities whose
     * other input the order joins earlier: the fallback when no candidate of the cost search orders the keys. A key
     * that reads an outer or temporal input stays where it is.
     */
    private void orderBySemantics() {
        movedKeys.clear();
        for (int i = 1, n = dependencies.size(); i < n; i++) {
            final JoinDependency dependency = dependencies.getQuick(i);
            if (dependency != null && !joinTypes.getQuick(i).isBarrier()) {
                dependencies.setQuick(i, null);
                final ObjList<JoinEquality> keys = dependency.getKeys();
                for (int k = 0, count = keys.size(); k < count; k++) {
                    final JoinEquality key = keys.getQuick(k);
                    final int other = key.getOtherSource(i);
                    if (joinTypes.getQuick(other).isBarrier()) {
                        addParent(other, i);
                        dependencies.getQuick(i).getKeys().add(key);
                    } else {
                        movedKeys.add(key);
                    }
                }
            }
        }
        final IntList lateral = graph.getLateralDependencies();
        for (int i = 0, n = lateral.size(); i < n; i += 2) {
            addParent(lateral.getQuick(i), lateral.getQuick(i + 1));
        }
        final IntList constraints = graph.getOrderingConstraints();
        for (int i = 0, n = constraints.size(); i < n; i += 2) {
            if (!joinTypes.getQuick(constraints.getQuick(i + 1)).isBarrier()) {
                addParent(constraints.getQuick(i), constraints.getQuick(i + 1));
            }
        }
        linkDependencies();
        if (topologicalOrder() == Integer.MAX_VALUE) {
            throw new IllegalStateException("join semantics admit no order");
        }
        for (int i = 0, n = movedKeys.size(); i < n; i++) {
            final JoinEquality key = movedKeys.getQuick(i);
            final boolean isLeftLater = position(key.getLeftSource()) > position(key.getRightSource());
            final int slave = isLeftLater ? key.getLeftSource() : key.getRightSource();
            addParent(isLeftLater ? key.getRightSource() : key.getLeftSource(), slave);
            dependencies.getQuick(slave).getKeys().add(key);
        }
        for (int i = 1, n = joinTypes.size(); i < n; i++) {
            if (!joinTypes.getQuick(i).isBarrier()) {
                final JoinDependency dependency = dependencies.getQuick(i);
                joinTypes.setQuick(i, dependency != null && dependency.getKeys().size() > 0 ? JoinKind.INNER : JoinKind.CROSS);
            }
        }
        bestOrder.clear();
        bestOrder.addAll(candidateOrder);
    }

    private int position(int source) {
        return candidateOrder.indexOf(source, 0, candidateOrder.size());
    }

    private void reorder() {
        bestJoinTypes.clear();
        bestOrder.clear();
        roots.clear();
        stagedDependencies.clear();
        for (int i = 0, n = dependencies.size(); i < n; i++) {
            final JoinDependency dependency = dependencies.getQuick(i);
            if (dependency == null || dependency.getParents().size() == 0) {
                roots.add(i);
            }
        }
        final IntList constraints = graph.getOrderingConstraints();
        int bestCost = Integer.MAX_VALUE;
        boolean hasCandidate = false;
        for (int candidate = 0, rootCount = roots.size(); candidate < rootCount; candidate++) {
            for (int i = 0; i < rootCount; i++) {
                if (candidate != i) {
                    final int target = roots.getQuick(i);
                    // Scans around the position in the roots list, not around the target source ordinal.
                    for (int from = i - 1; from >= 0; from--) {
                        if (joinTypes.getQuick(from).isBarrier()) {
                            break;
                        }
                        swap(target, from);
                    }
                    for (int from = i + 1, n = dependencies.size(); from < n; from++) {
                        if (joinTypes.getQuick(from).isBarrier()) {
                            break;
                        }
                        swap(target, from);
                    }
                }
            }
            for (int i = 0, n = constraints.size(); i < n; i += 2) {
                addParent(constraints.getQuick(i), constraints.getQuick(i + 1));
            }
            final int cost = topologicalOrder();
            if (cost != Integer.MAX_VALUE && (!hasCandidate || cost < bestCost)) {
                bestCost = cost;
                hasCandidate = true;
                bestOrder.clear();
                bestOrder.addAll(candidateOrder);
                bestJoinTypes.clear();
                bestJoinTypes.addAll(joinTypes);
                stagedDependencies.clear();
                for (int source = 0, n = dependencies.size(); source < n; source++) {
                    final JoinDependency dependency = dependencies.getQuick(source);
                    if (dependency == null) {
                        stagedDependencies.add(null);
                    } else {
                        final JoinDependency snapshot = dependencyPool.next().of(dependency.getSlave());
                        snapshot.getKeys().addAll(dependency.getKeys());
                        snapshot.getParents().addAll(dependency.getParents());
                        stagedDependencies.add(snapshot);
                    }
                }
            }
        }
        if (hasCandidate) {
            // Candidate search mutates dependencies. Publish the winning order's keys,
            // not the final candidate's keys under an earlier candidate's order.
            dependencies.clear();
            dependencies.addAll(stagedDependencies);
            joinTypes.clear();
            joinTypes.addAll(bestJoinTypes);
        } else {
            orderBySemantics();
        }
    }

    private void swap(int target, int source) {
        final JoinDependency from = dependencies.getQuick(source);
        if (target == 0 || joinTypes.getQuick(target).isBarrier() || from == null || !from.getParents().contains(target)
                || hasOrderingConstraint(target, source)) {
            return;
        }
        stagedIndexes.clear();
        final ObjList<JoinEquality> keys = from.getKeys();
        for (int i = 0, n = keys.size(); i < n; i++) {
            final JoinEquality key = keys.getQuick(i);
            if (key.getLeftSource() == target || key.getRightSource() == target) {
                stagedIndexes.add(i);
            }
        }
        if (stagedIndexes.size() > 0 && stagedIndexes.size() < keys.size() && !hasOtherDependencyPath(from, target)) {
            // Reversing a direct edge while another path reaches the same donor
            // would introduce a cycle. Only reverse an independent edge.
            // An earlier donor may already have created this dependency. Reusing
            // the root search's original null would discard its stolen keys.
            JoinDependency targetDependency = dependencies.getQuick(target);
            if (targetDependency == null) {
                targetDependency = dependencyPool.next().of(target);
                dependencies.setQuick(target, targetDependency);
            }
            dependencies.setQuick(source, moveClauses(from, targetDependency));
            if (joinTypes.getQuick(target) == JoinKind.CROSS) {
                joinTypes.setQuick(target, JoinKind.INNER);
            }
        }
    }

    /**
     * Kahn's order with the first input first; returns the order's cost, or {@link Integer#MAX_VALUE} when the
     * dependencies are cyclic. An unkeyed CROSS step, or a late input that nothing waits for, joins only when no
     * other input is ready, and an input that no edge ties to another joins last.
     */
    private int topologicalOrder() {
        stagedIndexes.clear();
        candidateOrder.clear();
        pendingSources.clear();
        ready.clear();
        final IntList lateInputs = graph.getLateInputs();
        inCounts.setAll(dependencies.size(), 0);
        for (int i = 0, n = dependencies.size(); i < n; i++) {
            final JoinDependency dependency = dependencies.getQuick(i);
            if (dependency != null && dependency.getParents().size() > 0) {
                inCounts.setQuick(i, dependency.getParents().size());
            } else if (i > 0) {
                if (children.getQuick(i).size() > 0) {
                    ready.add(i);
                } else {
                    stagedIndexes.add(i);
                }
            }
        }
        int cost = appendToOrder(0);
        int heldCount = 0;
        while (true) {
            final int source;
            if (ready.notEmpty()) {
                source = ready.poll();
                if (lateInputs.contains(source) && children.getQuick(source).size() == 0
                        || joinTypes.getQuick(source) == JoinKind.CROSS && isParentless(source)) {
                    pendingSources.add(source);
                    continue;
                }
            } else if (heldCount < pendingSources.size()) {
                source = pendingSources.getQuick(heldCount++);
            } else {
                break;
            }
            cost += appendToOrder(source);
        }
        for (int i = 0, n = dependencies.size(); i < n; i++) {
            if (dependencies.getQuick(i) != null && inCounts.getQuick(i) > 0) {
                return Integer.MAX_VALUE;
            }
        }
        candidateOrder.addAll(stagedIndexes);
        return cost;
    }

    private void validateOrder() {
        markedIndexes.clear();
        if (bestOrder.getQuick(0) != 0) {
            throw new IllegalStateException("the first join input is not ordered first");
        }
        for (int i = 0, n = bestOrder.size(); i < n; i++) {
            final int source = bestOrder.getQuick(i);
            if (!markedIndexes.add(source)) {
                throw new IllegalStateException("duplicate input in logical join order");
            }
            final JoinDependency dependency = dependencies.getQuick(source);
            if (dependency != null) {
                final ObjList<JoinEquality> keys = dependency.getKeys();
                for (int k = 0, count = keys.size(); k < count; k++) {
                    final JoinEquality key = keys.getQuick(k);
                    final int parent = key.getOtherSource(source);
                    if (key.getLeftSource() != source && key.getRightSource() != source || parent == source || markedIndexes.excludes(parent)) {
                        throw new IllegalStateException("join key parent is not ordered before its slave");
                    }
                }
            }
        }
    }
}
