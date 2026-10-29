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

package io.questdb.griffin.plan.logical;

import io.questdb.std.IntHashSet;
import io.questdb.std.IntList;
import io.questdb.std.IntSortedList;
import io.questdb.std.Mutable;
import io.questdb.std.ObjList;
import io.questdb.std.ObjectFactory;

/**
 * What binding knows about a join of more than two inputs and leaves for the optimiser to order: per input, the inputs
 * it waits for and the equalities that key it ({@link JoinDependency}, null for none), among them those a later INNER
 * input implies for an outer input, see {@link JoinEquality#getReplacedKey}; the ordering constraints and
 * lateral dependencies the join semantics impose, as parent and child input pairs; the inputs held until nothing
 * else is ready; whether the first input leads the order; and the bound filter conjuncts the order places, each with the inputs whose ON clause
 * states it, -1 for WHERE, and the constant conjuncts, each with the inner input whose ON clause states it, see
 * {@link #addConstantFilter}.
 */
public final class JoinGraph implements Mutable {
    public static final ObjectFactory<JoinGraph> FACTORY = JoinGraph::new;
    private final IntList constantFilterOrigins = new IntList();
    private final ObjList<BoundExpression> constantFilters = new ObjList<>();
    private final ObjList<JoinDependency> dependencies = new ObjList<>();
    private final IntList lateInputs = new IntList();
    private final IntList lateralDependencies = new IntList();
    private final IntList orderingConstraints = new IntList();
    private final IntList residualOwnerEnds = new IntList();
    private final IntList residualOwners = new IntList();
    private final ObjList<BoundExpression> residuals = new ObjList<>();

    /**
     * Appends, as parent and child pairs, the edges the join semantics impose on the outer or temporal step at
     * {@code step}, of join type {@code type}, whatever its keys: the first input before a temporal or UNNEST step,
     * and before an UNNEST each input it reads.
     */
    public static void addSemanticEdges(ObjList<JoinInput> inputs, int step, JoinKind type, IntList edges) {
        if (type.isTemporal() || type == JoinKind.UNNEST) {
            edges.add(0);
            edges.add(step);
        }
        if (type == JoinKind.UNNEST) {
            final ObjList<BoundExpression> expressions = inputs.getQuick(step).getUnnest().getExpressions();
            for (int i = 0, n = expressions.size(); i < n; i++) {
                addUnnestEdges(inputs, expressions.getQuick(i), step, edges);
            }
        }
    }

    /**
     * Links the parent and child pairs of {@code edges} into the child sets of the {@code count} inputs, which it clears
     * first, and appends the inputs to {@code order}, which may be {@code edges}, each after its parents and the lowest
     * ready one first; returns how many it appended, fewer than the inputs when the edges form a cycle.
     * {@code inCounts} and {@code ready} are temporaries of the call.
     */
    public static int sortTopologically(
            IntList edges,
            int count,
            ObjList<IntHashSet> children,
            IntList inCounts,
            IntSortedList ready,
            IntList order
    ) {
        for (int i = 0, n = children.size(); i < n; i++) {
            children.getQuick(i).clear();
        }
        inCounts.setAll(count, 0);
        for (int i = 0, n = edges.size(); i < n; i += 2) {
            if (children.getQuick(edges.getQuick(i)).add(edges.getQuick(i + 1))) {
                inCounts.increment(edges.getQuick(i + 1));
            }
        }
        order.clear();
        ready.clear();
        for (int i = 0; i < count; i++) {
            if (inCounts.getQuick(i) == 0) {
                ready.add(i);
            }
        }
        while (ready.notEmpty()) {
            final int source = ready.poll();
            order.add(source);
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
        return order.size();
    }

    /**
     * Adds a constant conjunct, which reads no input: one the ON clause of the INNER or CROSS input {@code origin}
     * states, or, for -1, the constant WHERE terms, or the constant terms of every clause of a join without an outer or
     * temporal input, which bind as one conjunction.
     */
    public void addConstantFilter(BoundExpression constantFilter, int origin) {
        constantFilters.add(constantFilter);
        constantFilterOrigins.add(origin);
    }

    public void addResidual(BoundExpression residual, int origin) {
        residuals.add(residual);
        residualOwners.add(origin);
        residualOwnerEnds.add(residualOwners.size());
    }

    public void addResidual(BoundExpression residual, IntList owners) {
        residuals.add(residual);
        residualOwners.addAll(owners);
        residualOwnerEnds.add(residualOwners.size());
    }

    @Override
    public void clear() {
        constantFilterOrigins.clear();
        constantFilters.clear();
        dependencies.clear();
        lateInputs.clear();
        lateralDependencies.clear();
        orderingConstraints.clear();
        residualOwnerEnds.clear();
        residualOwners.clear();
        residuals.clear();
    }

    public int getConstantFilterOrigin(int constantFilter) {
        return constantFilterOrigins.getQuick(constantFilter);
    }

    public ObjList<BoundExpression> getConstantFilters() {
        return constantFilters;
    }

    public ObjList<JoinDependency> getDependencies() {
        return dependencies;
    }

    public IntList getLateInputs() {
        return lateInputs;
    }

    public IntList getLateralDependencies() {
        return lateralDependencies;
    }

    public IntList getOrderingConstraints() {
        return orderingConstraints;
    }

    public int getResidualOwner(int residual, int index) {
        return residualOwners.getQuick(residualOwnersLo(residual) + index);
    }

    public int getResidualOwnerCount(int residual) {
        return residualOwnerEnds.getQuick(residual) - residualOwnersLo(residual);
    }

    public ObjList<BoundExpression> getResiduals() {
        return residuals;
    }

    private static void addUnnestEdges(ObjList<JoinInput> inputs, BoundExpression expression, int step, IntList edges) {
        if (expression instanceof ColumnExpression column) {
            for (int i = 0; i < step; i++) {
                if (inputs.getQuick(i).getSourceOutput().getColumnIndexById(column.getColumnId()) >= 0) {
                    edges.add(i);
                    edges.add(step);
                    return;
                }
            }
            throw new IllegalStateException("UNNEST argument is outside its preceding join inputs");
        }
        if (expression instanceof FunctionExpression function) {
            for (int i = 0, n = function.getArgumentCount(); i < n; i++) {
                addUnnestEdges(inputs, function.argumentAt(i), step, edges);
            }
        }
    }

    private int residualOwnersLo(int residual) {
        return residual == 0 ? 0 : residualOwnerEnds.getQuick(residual - 1);
    }

    void visitReads(PlanExpressionVisitor visitor) {
        PlanReads.expressions(residuals, visitor);
        PlanReads.expressions(constantFilters, visitor);
        for (int i = 0, n = dependencies.size(); i < n; i++) {
            final JoinDependency dependency = dependencies.getQuick(i);
            if (dependency != null) {
                dependency.visitReads(visitor);
            }
        }
    }
}
