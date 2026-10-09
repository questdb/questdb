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

import io.questdb.std.IntList;
import io.questdb.std.Mutable;
import io.questdb.std.ObjList;
import io.questdb.std.ObjectFactory;

/**
 * What binding knows about a join of more than two inputs and leaves for the optimiser to order: per input, the inputs
 * it waits for and the equalities that key it ({@link JoinDependency}, null for none); the ordering constraints and
 * lateral dependencies the join semantics impose, as parent and child input pairs; the inputs held until nothing
 * else is ready; whether the first input leads the order; and the bound filter conjuncts the order places, each with the inputs whose ON clause
 * states it, -1 for WHERE, and the constant conjuncts the last step evaluates.
 */
public final class JoinGraph implements Mutable {
    public static final ObjectFactory<JoinGraph> FACTORY = JoinGraph::new;
    private final ObjList<JoinDependency> dependencies = new ObjList<>();
    private final IntList lateInputs = new IntList();
    private final IntList lateralDependencies = new IntList();
    private final IntList orderingConstraints = new IntList();
    private final IntList residualOwnerEnds = new IntList();
    private final IntList residualOwners = new IntList();
    private final ObjList<BoundExpression> residuals = new ObjList<>();
    private BoundExpression constantFilter;
    private int constantFilterPosition;

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
        dependencies.clear();
        lateInputs.clear();
        lateralDependencies.clear();
        orderingConstraints.clear();
        residualOwnerEnds.clear();
        residualOwners.clear();
        residuals.clear();
        constantFilter = null;
        constantFilterPosition = -1;
    }

    public BoundExpression getConstantFilter() {
        return constantFilter;
    }

    public int getConstantFilterPosition() {
        return constantFilterPosition;
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

    public void setConstantFilter(BoundExpression constantFilter, int position) {
        this.constantFilter = constantFilter;
        this.constantFilterPosition = position;
    }

    private int residualOwnersLo(int residual) {
        return residual == 0 ? 0 : residualOwnerEnds.getQuick(residual - 1);
    }

    void visitReads(PlanExpressionVisitor visitor) {
        PlanReads.expressions(residuals, visitor);
        constantFilter = PlanReads.expression(constantFilter, visitor);
        for (int i = 0, n = dependencies.size(); i < n; i++) {
            final JoinDependency dependency = dependencies.getQuick(i);
            if (dependency != null) {
                dependency.visitReads(visitor);
            }
        }
    }
}
