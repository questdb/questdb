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
import io.questdb.griffin.PlanNodePools;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.plan.logical.BoundExpression;
import io.questdb.griffin.plan.logical.ColumnExpression;
import io.questdb.griffin.plan.logical.ExpressionVisitor;
import io.questdb.griffin.plan.logical.JoinDependency;
import io.questdb.griffin.plan.logical.JoinEquality;
import io.questdb.griffin.plan.logical.JoinGraph;
import io.questdb.griffin.plan.logical.JoinInput;
import io.questdb.griffin.plan.logical.JoinKind;
import io.questdb.griffin.plan.logical.JoinPlan;
import io.questdb.griffin.plan.logical.OutputSchema;
import io.questdb.griffin.plan.logical.TreeWalk;
import io.questdb.std.IntHashSet;
import io.questdb.std.IntList;
import io.questdb.std.IntSortedList;
import io.questdb.std.LongList;
import io.questdb.std.Mutable;
import io.questdb.std.ObjList;
import io.questdb.std.ObjectPool;

/**
 * Selects the order of a join of more than two inputs from the {@link JoinGraph} binding collected for it, with the
 * first input fixed when its order carries semantics, and publishes the keys, join types and ordered inputs
 * ({@link #order(JoinPlan, boolean, IntHashSet, boolean)}). Dependencies come from the statement's plan-node pools;
 * the lists it owns stay until {@link #clear()}, and the borrowed ones are temporaries of one ordering.
 */
final class JoinOrderSolver implements Mutable {
    private final ObjList<JoinKind> bestJoinTypes = new ObjList<>();
    private final IntList bestOrder;
    private final IntList candidateOrder;
    private final ObjList<IntHashSet> children;
    private final ObjectPool<IntHashSet> childrenPool;
    private final OptimiserContext context;
    private final ObjectPool<JoinDependency> dependencyPool;
    private final IntList inCounts;
    private final ObjList<JoinKind> joinTypes;
    /**
     * For each input, a bit set of the inputs that a path of ordering constraints orders after it.
     */
    private final LongList laterInputs;
    private final IntHashSet markedIndexes;
    private final ObjList<JoinEquality> movedKeys = new ObjList<>();
    private final IntList pendingSources;
    private final IntSortedList ready;
    private final ObjList<JoinEquality> replacedKeys = new ObjList<>();
    private final IntList roots;
    private final IntList semanticEdges;
    private final ObjList<JoinDependency> stagedDependencies;
    private final IntList stagedIndexes;
    private ObjList<JoinDependency> dependencies;
    private JoinGraph graph;
    private boolean isFirstInputFixed;
    /**
     * Whether the order search changed the dependencies, their links or the join types since it last ordered them.
     */
    private boolean isOrderStale;
    private OutputSchema liftedOutput;
    private final ExpressionVisitor liftedInputReads = expression ->
            expression instanceof ColumnExpression column && liftedOutput.getColumnIndexById(column.getColumnId()) >= 0
                    ? TreeWalk.STOP : TreeWalk.CONTINUE;

    /**
     * Borrows {@code markedIndexes}, {@code stagedIndexes}, {@code bestOrder}, {@code roots}, {@code inCounts},
     * {@code candidateOrder} and {@code pendingSources} as temporaries of one call, and the other lists and the pool of
     * child sets as temporaries of one ordering.
     */
    JoinOrderSolver(
            OptimiserContext context,
            PlanNodePools planNodes,
            IntHashSet markedIndexes,
            IntList stagedIndexes,
            IntList bestOrder,
            IntList roots,
            IntList inCounts,
            IntList candidateOrder,
            IntList pendingSources,
            IntSortedList ready,
            ObjList<IntHashSet> children,
            ObjectPool<IntHashSet> childrenPool,
            ObjList<JoinKind> joinTypes,
            ObjList<JoinDependency> stagedDependencies,
            LongList laterInputs
    ) {
        this.context = context;
        this.dependencyPool = planNodes.joinDependencies;
        this.markedIndexes = markedIndexes;
        this.stagedIndexes = stagedIndexes;
        this.bestOrder = bestOrder;
        this.roots = roots;
        this.semanticEdges = roots;
        this.inCounts = inCounts;
        this.candidateOrder = candidateOrder;
        this.pendingSources = pendingSources;
        this.ready = ready;
        this.children = children;
        this.childrenPool = childrenPool;
        this.joinTypes = joinTypes;
        this.stagedDependencies = stagedDependencies;
        this.laterInputs = laterInputs;
    }

    @Override
    public void clear() {
        bestJoinTypes.clear();
        movedKeys.clear();
        replacedKeys.clear();
        dependencies = null;
        graph = null;
        isFirstInputFixed = false;
        isOrderStale = false;
        liftedOutput = null;
    }

    /**
     * Selects the order of a join binding left a graph for and publishes the keys, join types and ordered inputs; the
     * caller places the graph's filter conjuncts. An input binding added after the graph joins as an unkeyed CROSS
     * step. The first input stays first when the join designates its timestamp and a part of the query requires it,
     * {@code isTimestampRequired}; when the order leads with another input, the join designates no timestamp. It also
     * stays first when an input may yield other rows on another evaluation, judged by the columns the query reads,
     * {@code readColumnIds}. Unless decorrelation rewrites the join, {@code isDecorrelated}, a key-less CROSS input of
     * the master side of a RIGHT or FULL step joins after that step, see {@link #liftCrossInputs}: decorrelation, which
     * runs after ordering, adds domain inputs to the join and keys its steps on correlation columns, see
     * {@link CorrelationKeys}, while an {@link JoinKind#OUTER_CROSS} step must directly follow its outer step and hold
     * no key.
     */
    void order(JoinPlan join, boolean isTimestampRequired, IntHashSet readColumnIds, boolean isDecorrelated) throws SqlException {
        of(join, join.getGraph(), isTimestampRequired, readColumnIds);
        try {
            for (int i = dependencies.size(), n = join.getInputs().size(); i < n; i++) {
                dependencies.add(null);
            }
            join.setLeadingCrossJoinFixed(isFirstInputFixed && !hasLeadingJoinDependency(join));
            indexOrderingConstraints();
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
            for (int i = dependencies.size() - 1; i > 0; i--) {
                final JoinKind type = joinTypes.getQuick(i);
                if (type.isBarrier() && !type.isTemporal() && type != JoinKind.UNNEST && isAwaitedUnanchoredStep(i)) {
                    addParent(precedingAnchor(i), i);
                }
            }
            reorder();
            validateOrder();
            if (!isDecorrelated) {
                liftCrossInputs(join, readColumnIds);
            }
            final ObjList<JoinInput> inputs = join.getInputs();
            for (int i = 0, n = bestOrder.size(); i < n; i++) {
                final int source = bestOrder.getQuick(i);
                final JoinInput input = inputs.getQuick(source);
                input.setJoinType(joinTypes.getQuick(source));
                final JoinDependency dependency = dependencies.getQuick(source);
                if (dependency != null && dependency.hasImpliedKeys()) {
                    addImpliedKeys(join, input, source, dependency);
                } else if (dependency != null) {
                    final ObjList<JoinEquality> keys = dependency.getKeys();
                    for (int k = 0, count = keys.size(); k < count; k++) {
                        final JoinEquality key = keys.getQuick(k);
                        if (replacedKeys.indexOf(key) > -1) {
                            continue;
                        }
                        if (key.getLeftSource() == source) {
                            addKey(input, key.getRightColumnId(), key.getLeftColumnId(), key.getRightName(), key.getLeftName(), key.getLeftPosition());
                        } else {
                            addKey(input, key.getLeftColumnId(), key.getRightColumnId(), key.getLeftName(), key.getRightName(), key.getRightPosition());
                        }
                    }
                }
                join.getOrderedInputs().add(input);
            }
            join.getOutput().setTimestampColumnId(LogicalPlans.joinTimestampColumnId(join, bestOrder.size()));
        } finally {
            clear();
        }
    }

    /**
     * Keys the outer step on the equalities that a later INNER join implies between its input and an input the order
     * joins before it, see {@link JoinEquality#getReplacedKey}, turns the equalities its ON clause states into ON
     * residual conjuncts and records that the ON residual decides its NULL extension. Every row the step emits holds
     * the keys, so the later join no longer keys on the key each implied equality replaces.
     */
    private void addImpliedKeys(JoinPlan join, JoinInput input, int source, JoinDependency dependency) throws SqlException {
        final ObjList<JoinEquality> keys = dependency.getKeys();
        for (int i = 0, n = keys.size(); i < n; i++) {
            final JoinEquality key = keys.getQuick(i);
            final int master = key.getOtherSource(source);
            if (key.getReplacedKey() != null) {
                input.addKey(key.getColumnId(master), key.getColumnId(source), key.getName(master), key.getName(source),
                        key.getPosition(source));
                replacedKeys.add(key.getReplacedKey());
            } else {
                final int position = key.getPosition(source);
                final BoundExpression equality = context.bindEquality(key.getColumnId(master), key.getColumnId(source),
                        join.getOutput(), position);
                input.setOnResidual(input.getOnResidual() == null ? equality
                        : context.getRewriter().combineConjunction(equality, input.getOnResidual(), position));
            }
        }
        input.setNullExtension(JoinInput.NullExtension.BY_ON_RESIDUAL);
    }

    /**
     * Keys the input on the master and the slave column. A LEFT input that a key already keys on the master column
     * filters its input on the equality of the two slave columns instead, see {@link OptimiserContext#addKeyFilter}:
     * binding merges such keys only for an input it joins without crossing an outer or temporal step, see
     * {@code JoinGraphBuilder}, and a RIGHT or FULL input keeps the input rows the filter would drop.
     */
    private void addKey(JoinInput input, int masterId, int slaveId, CharSequence masterName, CharSequence slaveName, int position) throws SqlException {
        final int keyIndex = input.getJoinType() == JoinKind.LEFT_OUTER ? input.indexOfMasterKey(masterId) : -1;
        if (keyIndex > -1) {
            context.addKeyFilter(input, keyIndex, slaveId, input.getSourceOutput(), position);
        } else {
            input.addKey(masterId, slaveId, masterName, slaveName, position);
        }
    }

    private void addParent(int parent, int child) {
        JoinDependency dependency = dependencies.getQuick(child);
        if (dependency == null) {
            dependency = dependencyPool.next().of(child);
            dependencies.setQuick(child, dependency);
        }
        isOrderStale |= dependency.getParents().add(parent);
        isOrderStale |= link(parent, child);
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

    private boolean dependsOn(int source, int target) {
        pendingSources.clear();
        markedIndexes.clear();
        pendingSources.add(source);
        final IntList constraints = graph.getOrderingConstraints();
        while (pendingSources.size() > 0) {
            final int last = pendingSources.size() - 1;
            final int node = pendingSources.getQuick(last);
            pendingSources.setPos(last);
            if (node == target) {
                return true;
            }
            if (markedIndexes.add(node)) {
                final JoinDependency dependency = dependencies.getQuick(node);
                if (dependency != null) {
                    final IntHashSet parents = dependency.getParents();
                    for (int i = 0, n = parents.size(); i < n; i++) {
                        pendingSources.add(parents.get(i));
                    }
                }
                for (int i = 0, n = constraints.size(); i < n; i += 2) {
                    if (constraints.getQuick(i + 1) == node) {
                        pendingSources.add(constraints.getQuick(i));
                    }
                }
            }
        }
        return false;
    }

    private boolean hasLeadingJoinDependency(JoinPlan join) {
        final ObjList<JoinInput> inputs = join.getInputs();
        for (int i = 1, n = inputs.size(); i < n; i++) {
            final JoinInput input = inputs.getQuick(i);
            final JoinKind type = input.getJoinType();
            final JoinDependency dependency = dependencies.getQuick(i);
            if (type.isTemporal() || dependency != null && dependency.getParents().contains(0)
                    || i == 1 && type == JoinKind.LEFT_OUTER && input.getOnResidual() != null) {
                return true;
            }
        }
        final IntList constraints = graph.getOrderingConstraints();
        for (int i = 0, n = constraints.size(); i < n; i += 2) {
            if (constraints.getQuick(i) == 0 && inputs.getQuick(constraints.getQuick(i + 1)).getJoinType() != JoinKind.UNNEST) {
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

    /**
     * Indexes the ordering constraints of the graph into {@link #laterInputs}: links them into the child sets, which
     * {@link #linkDependencies} relinks, sorts the inputs topologically by them, and adds to each input, from the last
     * one back, its children and the inputs they order after them.
     */
    private void indexOrderingConstraints() {
        final IntList constraints = graph.getOrderingConstraints();
        final int count = dependencies.size();
        final int words = (count + 63) >>> 6;
        laterInputs.setAll(count * words, 0);
        inCounts.setAll(count, 0);
        for (int i = 0, n = constraints.size(); i < n; i += 2) {
            if (link(constraints.getQuick(i), constraints.getQuick(i + 1))) {
                inCounts.increment(constraints.getQuick(i + 1));
            }
        }
        candidateOrder.clear();
        ready.clear();
        for (int i = 0; i < count; i++) {
            if (inCounts.getQuick(i) == 0) {
                ready.add(i);
            }
        }
        while (ready.notEmpty()) {
            final int source = ready.poll();
            candidateOrder.add(source);
            final IntHashSet sourceChildren = children.getQuick(source);
            for (int i = 0, n = sourceChildren.size(); i < n; i++) {
                final int child = sourceChildren.get(i);
                final int inCount = inCounts.getQuick(child) - 1;
                inCounts.setQuick(child, inCount);
                if (inCount == 0) {
                    ready.add(child);
                }
            }
        }
        for (int i = candidateOrder.size() - 1; i >= 0; i--) {
            final int source = candidateOrder.getQuick(i);
            final IntHashSet sourceChildren = children.getQuick(source);
            final int row = source * words;
            for (int k = 0, n = sourceChildren.size(); k < n; k++) {
                final int child = sourceChildren.get(k);
                final int childRow = child * words;
                laterInputs.setQuick(row + (child >>> 6), laterInputs.getQuick(row + (child >>> 6)) | 1L << child);
                for (int w = 0; w < words; w++) {
                    laterInputs.setQuick(row + w, laterInputs.getQuick(row + w) | laterInputs.getQuick(childRow + w));
                }
            }
        }
    }

    /**
     * True when neither a key nor an ordering constraint orders the step after another input, while a key or an
     * ordering constraint orders another input after it: such a step would otherwise lead the order without a master.
     */
    private boolean isAwaitedUnanchoredStep(int source) {
        if (!isParentless(source)) {
            return false;
        }
        boolean isAwaited = children.getQuick(source).size() > 0;
        final IntList constraints = graph.getOrderingConstraints();
        for (int i = 0, n = constraints.size(); i < n; i += 2) {
            if (constraints.getQuick(i + 1) == source) {
                return false;
            }
            isAwaited |= constraints.getQuick(i) == source;
        }
        return isAwaited;
    }

    /**
     * True when a path of ordering constraints orders {@code parent} before {@code child}, see {@link #laterInputs}.
     */
    private boolean isConstrainedBefore(int parent, int child) {
        final int words = (dependencies.size() + 63) >>> 6;
        return (laterInputs.getQuick(parent * words + (child >>> 6)) & 1L << child) != 0;
    }

    /**
     * True when the CROSS step at {@code position} of the order can join after the RIGHT or FULL step at {@code outer}:
     * its rows do not vary across evaluations, judged by the columns the query reads, {@code readColumnIds}, no lateral
     * input depends on it, no step before the outer one waits for it, its own ON clause states no conjunct, and neither
     * a key nor an ON condition of a step up to the outer one, nor an ON conjunct of the graph reads it.
     */
    private boolean isLiftable(JoinPlan join, int source, int position, int outer, IntHashSet readColumnIds) {
        final ObjList<JoinInput> inputs = join.getInputs();
        final JoinInput input = inputs.getQuick(source);
        if (input.isDependent() || input.getInput() == null || !LogicalPlans.isResultStable(input.getInput(), true, readColumnIds)) {
            return false;
        }
        final IntList lateral = graph.getLateralDependencies();
        for (int i = 0, n = lateral.size(); i < n; i++) {
            if (lateral.getQuick(i) == source) {
                return false;
            }
        }
        final IntList constraints = graph.getOrderingConstraints();
        for (int i = 0, n = constraints.size(); i < n; i += 2) {
            if (constraints.getQuick(i) == source && bestOrder.indexOf(constraints.getQuick(i + 1), 0, bestOrder.size()) < outer) {
                return false;
            }
        }
        for (int i = 0, n = graph.getConstantFilters().size(); i < n; i++) {
            if (graph.getConstantFilterOrigin(i) == source) {
                return false;
            }
        }
        liftedOutput = input.getSourceOutput();
        for (int i = position + 1; i <= outer; i++) {
            final int step = bestOrder.getQuick(i);
            final JoinDependency dependency = dependencies.getQuick(step);
            if (dependency != null) {
                final ObjList<JoinEquality> keys = dependency.getKeys();
                for (int k = 0, count = keys.size(); k < count; k++) {
                    final JoinEquality key = keys.getQuick(k);
                    if (key.getLeftSource() == source || key.getRightSource() == source) {
                        return false;
                    }
                }
            }
            if (isLiftedInputRead(inputs.getQuick(step).getOnResidual())) {
                return false;
            }
        }
        final ObjList<BoundExpression> residuals = graph.getResiduals();
        for (int i = 0, n = residuals.size(); i < n; i++) {
            final boolean isRead = isLiftedInputRead(residuals.getQuick(i));
            for (int k = 0, count = graph.getResidualOwnerCount(i); k < count; k++) {
                final int owner = graph.getResidualOwner(i, k);
                if (owner == source || isRead && owner >= 0) {
                    return false;
                }
            }
        }
        return true;
    }

    private boolean isLiftedInputRead(BoundExpression expression) {
        return expression != null && !expression.walk(liftedInputReads);
    }

    private boolean isParentless(int source) {
        final JoinDependency dependency = dependencies.getQuick(source);
        return dependency == null || dependency.getParents().size() == 0;
    }

    /**
     * Joins a key-less CROSS input that the master side of a RIGHT or FULL step holds after that step instead, as an
     * {@link JoinKind#OUTER_CROSS} step, so the outer step joins each master row once instead of once per row of the
     * input. Only INNER and CROSS steps may lie between the input and the outer step, and the input must not be the
     * first, see {@link #isLiftable}. Per outer step it lifts the latest such input alone: the OUTER_CROSS step reads the
     * outer step's record and gates the outer step's master, see {@code OuterCrossJoinRecordCursorFactory}, so it must
     * directly follow the outer step.
     */
    private void liftCrossInputs(JoinPlan join, IntHashSet readColumnIds) {
        for (int outer = 2, n = bestOrder.size(); outer < n; outer++) {
            final JoinKind outerType = joinTypes.getQuick(bestOrder.getQuick(outer));
            if (outerType != JoinKind.RIGHT_OUTER && outerType != JoinKind.FULL_OUTER) {
                continue;
            }
            for (int position = outer - 1; position > 0; position--) {
                final int source = bestOrder.getQuick(position);
                final JoinKind type = joinTypes.getQuick(source);
                if (type.isBarrier()) {
                    break;
                }
                if (type == JoinKind.CROSS && isLiftable(join, source, position, outer, readColumnIds)) {
                    bestOrder.removeIndex(position);
                    bestOrder.insert(outer, source);
                    joinTypes.setQuick(source, JoinKind.OUTER_CROSS);
                    break;
                }
            }
        }
    }

    private boolean link(int parent, int child) {
        return children.getQuick(parent).add(child);
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

    private void of(JoinPlan join, JoinGraph graph, boolean isTimestampRequired, IntHashSet readColumnIds) {
        clear();
        children.clear();
        childrenPool.clear();
        joinTypes.clear();
        this.graph = graph;
        this.dependencies = graph.getDependencies();
        isFirstInputFixed = isTimestampRequired && join.getOutput().getTimestampIndex() >= 0 || join.hasExplicitTimestamp()
                || graph.getLateralDependencies().size() > 0;
        final ObjList<JoinInput> inputs = join.getInputs();
        for (int i = 0, n = inputs.size(); i < n; i++) {
            final JoinInput input = inputs.getQuick(i);
            final JoinKind type = input.getJoinType();
            children.add(childrenPool.next());
            joinTypes.add(type);
            isFirstInputFixed = isFirstInputFixed || type.isTemporal() || type == JoinKind.UNNEST || input.isDependent()
                    || !LogicalPlans.isResultStable(input.getInput(), true, readColumnIds);
        }
        for (int i = 0, n = graph.getResiduals().size(); !isFirstInputFixed && i < n; i++) {
            isFirstInputFixed = !LogicalPlans.isOrderIndependent(graph.getResiduals().getQuick(i));
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

    /**
     * The nearest input written before the step that does not wait for it, or the first input.
     */
    private int precedingAnchor(int source) {
        for (int anchor = source - 1; anchor > 0; anchor--) {
            if (!dependsOn(anchor, source)) {
                return anchor;
            }
        }
        return 0;
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
        isOrderStale = true;
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
            // A candidate that changed nothing orders as the last one did, at a cost that is not lower.
            if (!isOrderStale) {
                continue;
            }
            isOrderStale = false;
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
                || isConstrainedBefore(target, source)) {
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
            isOrderStale = true;
        }
    }

    /**
     * Kahn's order; returns the order's cost, or {@link Integer#MAX_VALUE} when the
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
            } else if (i > 0 || !isFirstInputFixed) {
                if (children.getQuick(i).size() > 0) {
                    ready.add(i);
                } else {
                    stagedIndexes.add(i);
                }
            }
        }
        int cost = isFirstInputFixed ? appendToOrder(0) : 0;
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
        if (isFirstInputFixed && bestOrder.getQuick(0) != 0) {
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
