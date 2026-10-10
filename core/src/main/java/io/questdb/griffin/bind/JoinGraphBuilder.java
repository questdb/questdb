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

package io.questdb.griffin.bind;

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
 * Collects the equalities, ordering constraints, lateral dependencies and late inputs of a join of more than two
 * inputs into the join's {@link JoinGraph}, merges the equalities that key one input on one column, and checks that
 * the join semantics admit an order ({@link #prepare()}); the optimiser's join order solver orders the graph.
 * Dependencies and equalities come from the statement's plan-node pools; every other list is borrowed until
 * {@link #clear()}.
 */
public final class JoinGraphBuilder implements Mutable {
    private final ObjList<IntHashSet> children = new ObjList<>();
    private final ObjectPool<IntHashSet> childrenPool;
    private final ObjectPool<JoinDependency> dependencyPool;
    private final ObjectPool<JoinEquality> equalityPool;
    private final IntList inCounts = new IntList();
    private final ObjList<JoinKind> joinTypes = new ObjList<>();
    private final IntHashSet markedIndexes;
    private final IntSortedList ready = new IntSortedList();
    private final IntList semanticEdges = new IntList();
    private final ObjList<JoinEquality> sourceFilters = new ObjList<>();
    private final ObjList<JoinDependency> stagedDependencies = new ObjList<>();
    private ObjList<JoinDependency> dependencies;
    private JoinGraph graph;
    private boolean isCollecting;
    private JoinPlan join;

    /**
     * Borrows {@code markedIndexes} as a temporary of one call and leaves it empty.
     */
    public JoinGraphBuilder(PlanNodePools planNodes, IntHashSet markedIndexes) {
        this.childrenPool = new ObjectPool<>(IntHashSet::new, 4, planNodes.maxRetainedJoinContexts);
        this.dependencyPool = planNodes.joinDependencies;
        this.equalityPool = planNodes.joinEqualities;
        this.markedIndexes = markedIndexes;
    }

    @Override
    public void clear() {
        children.clear();
        childrenPool.clear();
        inCounts.clear();
        joinTypes.clear();
        markedIndexes.clear();
        ready.clear();
        semanticEdges.clear();
        sourceFilters.clear();
        stagedDependencies.clear();
        dependencies = null;
        graph = null;
        isCollecting = false;
        join = null;
    }

    /**
     * The child reads columns of the parent without an equality between them, as a dependent join step
     * reads the columns of the inputs before it.
     */
    void addDependency(int parent, int child) {
        requireCollecting();
        graph.getLateralDependencies().add(parent);
        graph.getLateralDependencies().add(child);
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
        if (originalOnSource < -1 || originalOnSource >= dependencies.size()) {
            throw new IllegalArgumentException("invalid join predicate origin");
        }
        validateColumn(leftSource, leftColumnId);
        validateColumn(rightSource, rightColumnId);
        final JoinEquality equality = equalityPool.next().of(leftSource, leftColumnId, leftName, leftPosition,
                rightSource, rightColumnId, rightName, rightPosition);
        equality.getOwners().add(originalOnSource);
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
        final JoinDependency dependency = dependencyPool.next().of(equality.getRightSource());
        dependency.getKeys().add(equality);
        dependency.getParents().add(equality.getLeftSource());
        addContext(dependency);
        link(equality.getLeftSource(), equality.getRightSource());
    }

    /**
     * Holds the input while nothing depends on it until no other input is ready, so a context-free outer join goes last.
     */
    void addLateInput(int source) {
        requireCollecting();
        graph.getLateInputs().add(source);
    }

    void addOrderingConstraint(int parent, int child) {
        requireCollecting();
        if (parent != child) {
            graph.getOrderingConstraints().add(parent);
            graph.getOrderingConstraints().add(child);
        }
    }

    /**
     * Starts collecting the predicates and constraints of the join into its graph.
     */
    void collect(JoinPlan join, JoinGraph graph) {
        if (join.getOrderedInputs().size() != 0 || graph.getDependencies().size() != 0) {
            throw new IllegalStateException("join order has already been selected");
        }
        clear();
        this.join = join;
        this.graph = graph;
        this.dependencies = graph.getDependencies();
        final ObjList<JoinInput> inputs = join.getInputs();
        for (int i = 0, n = inputs.size(); i < n; i++) {
            children.add(childrenPool.next());
            joinTypes.add(inputs.getQuick(i).getJoinType());
            dependencies.add(null);
        }
        isCollecting = true;
    }

    /**
     * The derived equalities between two columns of one input, which the join evaluates as filters.
     */
    ObjList<JoinEquality> getSourceFilters() {
        return sourceFilters;
    }

    boolean hasJoinDependency(int source) {
        final JoinDependency dependency = dependencies.getQuick(source);
        if (dependency != null && dependency.getParents().size() > 0 || children.getQuick(source).size() > 0) {
            return true;
        }
        final IntList constraints = graph.getOrderingConstraints();
        for (int i = 0, n = constraints.size(); i < n; i++) {
            if (constraints.getQuick(i) == source) {
                return true;
            }
        }
        return false;
    }

    /**
     * Merges the collected equalities that key one input on one column and returns whether the join semantics admit
     * an order: the edges they impose whatever the keys are acyclic.
     */
    boolean prepare() {
        requireCollecting();
        // Merging an emitted dependency can emit another equality at a lower
        // source ordinal. Drain the queue: a captured size silently loses those
        // equalities. The decreasing owner ordinal bounds this local revisit.
        for (int i = 0; i < stagedDependencies.size(); i++) {
            addContext(stagedDependencies.getQuick(i));
        }
        stagedDependencies.clear();
        isCollecting = false;
        collectSemanticEdges();
        for (int i = 0, n = children.size(); i < n; i++) {
            children.getQuick(i).clear();
        }
        final int count = dependencies.size();
        inCounts.setAll(count, 0);
        for (int i = 0, n = semanticEdges.size(); i < n; i += 2) {
            if (children.getQuick(semanticEdges.getQuick(i)).add(semanticEdges.getQuick(i + 1))) {
                inCounts.increment(semanticEdges.getQuick(i + 1));
            }
        }
        ready.clear();
        for (int i = 0; i < count; i++) {
            if (inCounts.getQuick(i) == 0) {
                ready.add(i);
            }
        }
        int ordered = 0;
        while (ready.notEmpty()) {
            final IntHashSet sourceChildren = children.getQuick(ready.poll());
            ordered++;
            for (int i = 0, n = sourceChildren.size(); i < n; i++) {
                final int child = sourceChildren.get(i);
                final int inCount = inCounts.getQuick(child) - 1;
                inCounts.setQuick(child, inCount);
                if (inCount == 0) {
                    ready.add(child);
                }
            }
        }
        return ordered == count;
    }

    private static JoinEquality findKey(JoinDependency dependency, JoinEquality key) {
        final ObjList<JoinEquality> keys = dependency.getKeys();
        for (int i = 0, n = keys.size(); i < n; i++) {
            final JoinEquality candidate = keys.getQuick(i);
            if (candidate.getLeftColumnId() == key.getLeftColumnId() && candidate.getRightColumnId() == key.getRightColumnId()
                    || candidate.getLeftColumnId() == key.getRightColumnId() && candidate.getRightColumnId() == key.getLeftColumnId()) {
                return candidate;
            }
        }
        return null;
    }

    private void addContext(JoinDependency dependency) {
        final int slave = dependency.getSlave();
        final JoinDependency previous = dependencies.getQuick(slave);
        dependencies.setQuick(slave, previous == null ? dependency : merge(previous, dependency));
    }

    private void addKey(JoinDependency dependency, JoinEquality key) {
        final int parent = Math.min(key.getLeftSource(), key.getRightSource());
        dependency.getKeys().add(key);
        dependency.getParents().add(parent);
        link(parent, dependency.getSlave());
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
     * The edges the join semantics impose whatever the keys: ordering constraints, lateral dependencies, the
     * master of a temporal or UNNEST step, the inputs an UNNEST reads, the parents of an outer or temporal step,
     * and the outer or temporal input an INNER or CROSS step's key reads.
     */
    private void collectSemanticEdges() {
        semanticEdges.clear();
        semanticEdges.addAll(graph.getOrderingConstraints());
        semanticEdges.addAll(graph.getLateralDependencies());
        for (int i = 1, n = joinTypes.size(); i < n; i++) {
            final JoinKind type = joinTypes.getQuick(i);
            if (!type.isBarrier()) {
                final JoinDependency dependency = dependencies.getQuick(i);
                if (dependency != null) {
                    final ObjList<JoinEquality> keys = dependency.getKeys();
                    for (int k = 0, count = keys.size(); k < count; k++) {
                        final int other = keys.getQuick(k).getOtherSource(i);
                        if (joinTypes.getQuick(other).isBarrier()) {
                            semanticEdges.add(other);
                            semanticEdges.add(i);
                        }
                    }
                }
                continue;
            }
            JoinGraph.addSemanticEdges(join.getInputs(), i, type, semanticEdges);
            final JoinDependency dependency = dependencies.getQuick(i);
            if (dependency != null) {
                final IntHashSet parents = dependency.getParents();
                for (int k = 0, count = parents.size(); k < count; k++) {
                    semanticEdges.add(parents.get(k));
                    semanticEdges.add(i);
                }
            }
        }
    }

    private boolean crossesBarrier(JoinDependency dependency) {
        final int slave = dependency.getSlave();
        int first = slave;
        final IntHashSet parents = dependency.getParents();
        for (int i = 0, n = parents.size(); i < n; i++) {
            first = Math.min(first, parents.get(i));
        }
        for (int i = first; i <= slave; i++) {
            if (joinTypes.getQuick(i).isBarrier()) {
                return true;
            }
        }
        return false;
    }

    private void emit(JoinEquality a, boolean isLeftA, JoinEquality b, boolean isLeftB, int deletedIndex) {
        markedIndexes.add(deletedIndex);
        final int leftSource = isLeftA ? a.getLeftSource() : a.getRightSource();
        final int leftId = isLeftA ? a.getLeftColumnId() : a.getRightColumnId();
        final int rightSource = isLeftB ? b.getLeftSource() : b.getRightSource();
        final int rightId = isLeftB ? b.getLeftColumnId() : b.getRightColumnId();
        if (leftSource == rightSource && leftId == rightId) {
            b.addOwners(a.getOwners());
            return;
        }
        final JoinEquality equality = equalityPool.next().of(
                leftSource, leftId, isLeftA ? a.getLeftName() : a.getRightName(), isLeftA ? a.getLeftPosition() : a.getRightPosition(),
                rightSource, rightId, isLeftB ? b.getLeftName() : b.getRightName(), isLeftB ? b.getLeftPosition() : b.getRightPosition()
        );
        equality.addOwners(a.getOwners());
        equality.addOwners(b.getOwners());
        if (leftSource == rightSource) {
            sourceFilters.add(equality);
        } else {
            final JoinDependency dependency = dependencyPool.next().of(Math.max(leftSource, rightSource));
            dependency.getParents().add(Math.min(leftSource, rightSource));
            dependency.getKeys().add(equality);
            stagedDependencies.add(dependency);
        }
    }

    private void link(int parent, int child) {
        children.getQuick(parent).add(child);
    }

    private JoinDependency merge(JoinDependency a, JoinDependency b) {
        assert a.getSlave() == b.getSlave();
        if (crossesBarrier(a) || crossesBarrier(b)) {
            // Equality transitivity must not move a later INNER key into an
            // earlier outer match or a filter on its preserved input.
            final ObjList<JoinEquality> keys = b.getKeys();
            for (int i = 0, n = keys.size(); i < n; i++) {
                final JoinEquality key = keys.getQuick(i);
                final JoinEquality same = findKey(a, key);
                if (same == null) {
                    a.getKeys().add(key);
                } else {
                    same.addOwners(key.getOwners());
                }
            }
            a.getParents().addAll(b.getParents());
            return a;
        }
        markedIndexes.clear();
        final ObjList<JoinEquality> aKeys = a.getKeys();
        final ObjList<JoinEquality> bKeys = b.getKeys();
        for (int i = 0, n = bKeys.size(); i < n; i++) {
            final JoinEquality next = bKeys.getQuick(i);
            for (int k = 0, count = aKeys.size(); k < count; k++) {
                final JoinEquality previous = aKeys.getQuick(k);
                if (previous.getLeftColumnId() == next.getLeftColumnId()) {
                    emit(previous, false, next, false, k);
                    break;
                } else if (previous.getRightColumnId() == next.getLeftColumnId()) {
                    emit(previous, true, next, false, k);
                    break;
                } else if (previous.getLeftColumnId() == next.getRightColumnId()) {
                    emit(previous, false, next, true, k);
                    break;
                } else if (previous.getRightColumnId() == next.getRightColumnId()) {
                    emit(previous, true, next, true, k);
                    break;
                }
            }
        }
        final JoinDependency result = dependencyPool.next().of(a.getSlave());
        for (int i = 0, n = aKeys.size(); i < n; i++) {
            if (!markedIndexes.contains(i)) {
                addKey(result, aKeys.getQuick(i));
            }
        }
        for (int i = 0, n = bKeys.size(); i < n; i++) {
            addKey(result, bKeys.getQuick(i));
        }
        for (int i = 0, n = aKeys.size(); i < n; i++) {
            final JoinEquality previous = aKeys.getQuick(i);
            final int parent = Math.min(previous.getLeftSource(), previous.getRightSource());
            if (markedIndexes.contains(i) && result.getParents().excludes(parent)) {
                children.getQuick(parent).remove(result.getSlave());
            }
        }
        return result;
    }

    private void requireCollecting() {
        if (!isCollecting) {
            throw new IllegalStateException("join analysis is not collecting predicates");
        }
    }

    private void validateColumn(int source, int columnId) {
        if (source < 0 || source >= dependencies.size()
                || join.getInputs().getQuick(source).getSourceOutput().getColumnIndexById(columnId) < 0) {
            throw new IllegalArgumentException("join equality column is outside its source");
        }
    }
}
