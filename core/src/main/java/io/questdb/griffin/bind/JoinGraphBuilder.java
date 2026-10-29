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

import io.questdb.griffin.CompilerTemporaries;
import io.questdb.griffin.LogicalPlans;
import io.questdb.griffin.PlanNodePools;
import io.questdb.griffin.plan.logical.JoinDependency;
import io.questdb.griffin.plan.logical.JoinEquality;
import io.questdb.griffin.plan.logical.JoinGraph;
import io.questdb.griffin.plan.logical.JoinInput;
import io.questdb.griffin.plan.logical.JoinKind;
import io.questdb.griffin.plan.logical.JoinPlan;
import io.questdb.griffin.plan.logical.OutputSchema;
import io.questdb.std.IntHashSet;
import io.questdb.std.IntList;
import io.questdb.std.IntSortedList;
import io.questdb.std.LongList;
import io.questdb.std.Mutable;
import io.questdb.std.ObjList;
import io.questdb.std.ObjectPool;

/**
 * Collects the equalities, ordering constraints, lateral dependencies and late inputs of a join of more than two
 * inputs into the join's {@link JoinGraph}, merges the equalities that key one input on one column, keys an outer
 * input on the equalities a later INNER input implies for it, and checks that the join semantics admit an order
 * ({@link #prepare()}); the optimiser's join order solver orders the graph. Dependencies and equalities come from the
 * statement's plan-node pools; the source filters and the implied keys are its own until {@link #clear()}, and every
 * other list is a borrowed temporary of one collection.
 */
public final class JoinGraphBuilder implements Mutable {
    private final ObjList<IntHashSet> children;
    private final ObjectPool<IntHashSet> childrenPool;
    private final IntHashSet constrainedInputs = new IntHashSet();
    private final ObjectPool<JoinDependency> dependencyPool;
    private final ObjectPool<JoinEquality> equalityPool;
    private final ObjList<JoinEquality> impliedKeys = new ObjList<>();
    private final IntList inCounts;
    private final ObjList<JoinKind> joinTypes;
    private final IntHashSet markedIndexes;
    private final LongList reachableInputs;
    private final IntSortedList ready;
    private final IntList semanticEdges;
    private final ObjList<JoinEquality> sourceFilters = new ObjList<>();
    private final ObjList<JoinDependency> stagedDependencies;
    private ObjList<JoinDependency> dependencies;
    private JoinGraph graph;
    private boolean isCollecting;
    private JoinPlan join;

    /**
     * Borrows {@code markedIndexes} and {@code reachableInputs} as temporaries of one call, and the other lists and the
     * pool of child sets as temporaries of one collection.
     */
    public JoinGraphBuilder(PlanNodePools planNodes, CompilerTemporaries temporaries) {
        this.dependencyPool = planNodes.joinDependencies;
        this.equalityPool = planNodes.joinEqualities;
        this.markedIndexes = temporaries.ids;
        this.reachableInputs = temporaries.longs;
        this.inCounts = temporaries.masterKeys;
        this.semanticEdges = temporaries.slaveKeys;
        this.ready = temporaries.readyInputs;
        this.children = temporaries.joinChildren;
        this.childrenPool = temporaries.joinChildrenPool;
        this.joinTypes = temporaries.joinTypes;
        this.stagedDependencies = temporaries.joinDependencies;
    }

    @Override
    public void clear() {
        constrainedInputs.clear();
        impliedKeys.clear();
        sourceFilters.clear();
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
            constrainedInputs.add(parent);
            constrainedInputs.add(child);
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
        children.clear();
        childrenPool.clear();
        joinTypes.clear();
        stagedDependencies.clear();
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
        return dependency != null && dependency.getParents().size() > 0 || children.getQuick(source).size() > 0
                || constrainedInputs.contains(source);
    }

    /**
     * Merges the collected equalities that key one input on one column and returns whether the join semantics admit
     * an order: the edges they impose whatever the keys are acyclic. Keys each outer input on the equalities a later
     * INNER input implies for it, see {@link #addImpliedKeys}, and drops the ordering constraints that others imply,
     * see {@link #reduceOrderingConstraints}.
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
        final int count = dependencies.size();
        if (JoinGraph.sortTopologically(semanticEdges, count, children, inCounts, ready, semanticEdges) < count) {
            return false;
        }
        addImpliedKeys();
        reduceOrderingConstraints();
        return true;
    }

    /**
     * Whether the columns of the equality between two inputs of the join can key it, see
     * {@link LogicalPlans#isJoinKeyable}.
     */
    static boolean isKeyable(JoinPlan join, JoinEquality key) {
        return LogicalPlans.isJoinKeyable(join.getInputs().getQuick(key.getLeftSource()).getSourceOutput(), key.getLeftColumnId(),
                join.getInputs().getQuick(key.getRightSource()).getSourceOutput(), key.getRightColumnId());
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

    /**
     * Keys each outer input on the equalities a later INNER input implies for it, see {@link #stageImpliedKey}, unless
     * its ON clause states an equality the join can key, see {@link #isKeyable}, or the join semantics order the input
     * the equality reads after the outer input. The input then joins after the input the equality reads. Of the
     * equalities on one column of that input, the first keys the outer input.
     */
    private void addImpliedKeys() {
        for (int i = 0, n = impliedKeys.size(); i < n; i++) {
            final JoinEquality implied = impliedKeys.getQuick(i);
            final int master = implied.getLeftSource();
            final int step = implied.getRightSource();
            JoinDependency dependency = dependencies.getQuick(step);
            if (dependency != null && (hasKeyableStatedKey(dependency) || hasImpliedKey(dependency, implied.getLeftColumnId()))
                    || waitsFor(master, step)) {
                continue;
            }
            if (dependency == null) {
                dependency = dependencyPool.next().of(step);
                dependencies.setQuick(step, dependency);
            }
            addKey(dependency, implied);
        }
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

    private void emit(JoinEquality a, int aSource, JoinEquality b, int bSource, int deletedIndex) {
        markedIndexes.add(deletedIndex);
        if (aSource == bSource && a.getColumnId(aSource) == b.getColumnId(bSource)) {
            b.addOwners(a.getOwners());
            return;
        }
        final JoinEquality equality = equalityPool.next().ofDerived(a, aSource, b, bSource);
        if (aSource == bSource) {
            sourceFilters.add(equality);
        } else {
            final JoinDependency dependency = dependencyPool.next().of(Math.max(aSource, bSource));
            dependency.getParents().add(Math.min(aSource, bSource));
            dependency.getKeys().add(equality);
            stagedDependencies.add(dependency);
        }
    }

    private boolean hasImpliedKey(JoinDependency dependency, int masterColumnId) {
        final ObjList<JoinEquality> keys = dependency.getKeys();
        for (int i = 0, n = keys.size(); i < n; i++) {
            final JoinEquality key = keys.getQuick(i);
            if (key.getReplacedKey() != null && key.getLeftColumnId() == masterColumnId) {
                return true;
            }
        }
        return false;
    }

    private boolean hasKeyableStatedKey(JoinDependency dependency) {
        final ObjList<JoinEquality> keys = dependency.getKeys();
        for (int i = 0, n = keys.size(); i < n; i++) {
            final JoinEquality key = keys.getQuick(i);
            if (key.getReplacedKey() == null && isKeyable(join, key)) {
                return true;
            }
        }
        return false;
    }

    private void link(int parent, int child) {
        children.getQuick(parent).add(child);
    }

    /**
     * Merges the keys of two dependencies of one input. An input that joins across an outer or temporal step keeps
     * every key. Otherwise a key of {@code b} that shares a column with a key of {@code a} replaces it, and the
     * equality of their other columns filters the join, unless only one of the two keys can key the input, see
     * {@link #isKeyable}: binding then turns the other one into a filter, see {@code JoinBinder}.
     */
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
                    stageImpliedKeys(a, key);
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
            final boolean isNextKeyable = isKeyable(join, next);
            for (int k = 0, count = aKeys.size(); k < count; k++) {
                final JoinEquality previous = aKeys.getQuick(k);
                if (isKeyable(join, previous) != isNextKeyable) {
                    continue;
                }
                if (previous.getLeftColumnId() == next.getLeftColumnId()) {
                    emit(previous, previous.getRightSource(), next, next.getRightSource(), k);
                    break;
                } else if (previous.getRightColumnId() == next.getLeftColumnId()) {
                    emit(previous, previous.getLeftSource(), next, next.getRightSource(), k);
                    break;
                } else if (previous.getLeftColumnId() == next.getRightColumnId()) {
                    emit(previous, previous.getRightSource(), next, next.getLeftSource(), k);
                    break;
                } else if (previous.getRightColumnId() == next.getRightColumnId()) {
                    emit(previous, previous.getLeftSource(), next, next.getLeftSource(), k);
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

    /**
     * Drops each ordering constraint that a path of other ordering constraints implies. The order the constraints
     * admit stays the same, and the optimiser applies them in time linear in their count: a RIGHT or FULL join
     * constrains every input before it, and a chain of such joins would constrain each input once per later join.
     */
    private void reduceOrderingConstraints() {
        final IntList constraints = graph.getOrderingConstraints();
        final int count = dependencies.size();
        final int words = (count + 63) >>> 6;
        final int descendantsBase = count * words;
        JoinGraph.sortTopologically(constraints, count, children, inCounts, ready, semanticEdges);
        JoinGraph.setReachableInputs(semanticEdges, children, count, reachableInputs);
        int kept = 0;
        for (int i = 0, n = constraints.size(); i < n; i += 2) {
            final int parent = constraints.getQuick(i);
            final int child = constraints.getQuick(i + 1);
            final long bit = 1L << child;
            final boolean isImplied = (reachableInputs.getQuick(parent * words + (child >>> 6)) & bit) != 0;
            final int word = descendantsBase + parent * words + (child >>> 6);
            final long descendantWord = reachableInputs.getQuick(word);
            if (!isImplied && (descendantWord & bit) != 0) {
                reachableInputs.setQuick(word, descendantWord & ~bit);
                constraints.setQuick(kept++, parent);
                constraints.setQuick(kept++, child);
            }
        }
        constraints.setPos(kept);
    }

    private void requireCollecting() {
        if (!isCollecting) {
            throw new IllegalStateException("join analysis is not collecting predicates");
        }
    }

    private int sourceColumnType(int source, int columnId) {
        final OutputSchema output = join.getInputs().getQuick(source).getSourceOutput();
        return output.getColumnType(output.getColumnIndexById(columnId));
    }

    /**
     * Stages the equality that {@code stepKey} and {@code masterKey}, two keys of the INNER or CROSS input
     * {@code slave} on one of its columns, imply between the LEFT, RIGHT or FULL input {@code stepKey} reads and the
     * earlier input {@code masterKey} reads, see {@link JoinEquality#getReplacedKey}: no input between the outer input
     * and {@code slave} NULL-extends its master, and join keys on the two columns match exactly the values {@code =}
     * between them pairs, see {@link LogicalPlans#isKeyMatchType}.
     */
    private void stageImpliedKey(JoinEquality stepKey, JoinEquality masterKey, int slave) {
        final int step = stepKey.getOtherSource(slave);
        final int master = masterKey.getOtherSource(slave);
        if (master >= step || !joinTypes.getQuick(step).isOuter()) {
            return;
        }
        for (int i = step + 1; i < slave; i++) {
            if (joinTypes.getQuick(i).isMasterNulling()) {
                return;
            }
        }
        if (LogicalPlans.isKeyMatchType(sourceColumnType(master, masterKey.getColumnId(master)), sourceColumnType(step, stepKey.getColumnId(step)))) {
            impliedKeys.add(equalityPool.next().ofImplied(stepKey, masterKey, slave));
        }
    }

    /**
     * Stages the equalities that {@code key}, which the barrier branch of {@link #merge} keeps beside the keys of
     * {@code dependency}, implies with each of them on the same column of an INNER or CROSS input, see
     * {@link #stageImpliedKey}.
     */
    private void stageImpliedKeys(JoinDependency dependency, JoinEquality key) {
        final int slave = dependency.getSlave();
        if (joinTypes.getQuick(slave).isBarrier()) {
            return;
        }
        final ObjList<JoinEquality> keys = dependency.getKeys();
        for (int i = 0, n = keys.size(); i < n; i++) {
            final JoinEquality other = keys.getQuick(i);
            if (other.getColumnId(slave) == key.getColumnId(slave)) {
                final boolean isStepKey = key.getOtherSource(slave) > other.getOtherSource(slave);
                stageImpliedKey(isStepKey ? key : other, isStepKey ? other : key, slave);
            }
        }
    }

    private void validateColumn(int source, int columnId) {
        if (source < 0 || source >= dependencies.size()
                || join.getInputs().getQuick(source).getSourceOutput().getColumnIndexById(columnId) < 0) {
            throw new IllegalArgumentException("join equality column is outside its source");
        }
    }

    /**
     * Whether the edges the join semantics impose order {@code input} after {@code other}.
     */
    private boolean waitsFor(int input, int other) {
        markedIndexes.clear();
        ready.clear();
        ready.add(other);
        while (ready.notEmpty()) {
            final int source = ready.poll();
            if (source == input) {
                return true;
            }
            if (markedIndexes.add(source)) {
                final IntHashSet sourceChildren = children.getQuick(source);
                for (int i = 0, n = sourceChildren.size(); i < n; i++) {
                    ready.add(sourceChildren.get(i));
                }
            }
        }
        return false;
    }
}
