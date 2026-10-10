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

import io.questdb.cairo.CairoConfiguration;
import io.questdb.griffin.model.ExpressionNode;
import io.questdb.griffin.model.WindowExpression;
import io.questdb.griffin.plan.logical.AggregatePlan;
import io.questdb.griffin.plan.logical.BindVariableExpression;
import io.questdb.griffin.plan.logical.ColumnExpression;
import io.questdb.griffin.plan.logical.ConstantExpression;
import io.questdb.griffin.plan.logical.CursorExpression;
import io.questdb.griffin.plan.logical.DistinctPlan;
import io.questdb.griffin.plan.logical.FillPlan;
import io.questdb.griffin.plan.logical.FilterPlan;
import io.questdb.griffin.plan.logical.FunctionExpression;
import io.questdb.griffin.plan.logical.FunctionSourcePlan;
import io.questdb.griffin.plan.logical.HorizonJoinPlan;
import io.questdb.griffin.plan.logical.HorizonJoinSlave;
import io.questdb.griffin.plan.logical.JoinDependency;
import io.questdb.griffin.plan.logical.JoinEquality;
import io.questdb.griffin.plan.logical.JoinGraph;
import io.questdb.griffin.plan.logical.JoinInput;
import io.questdb.griffin.plan.logical.JoinPlan;
import io.questdb.griffin.plan.logical.LatestByPlan;
import io.questdb.griffin.plan.logical.LimitPlan;
import io.questdb.griffin.plan.logical.OuterColumnExpression;
import io.questdb.griffin.plan.logical.ProjectPlan;
import io.questdb.griffin.plan.logical.SampleByPlan;
import io.questdb.griffin.plan.logical.ScanPlan;
import io.questdb.griffin.plan.logical.SetOperationPlan;
import io.questdb.griffin.plan.logical.SortPlan;
import io.questdb.griffin.plan.logical.Subquery;
import io.questdb.griffin.plan.logical.TypeExpression;
import io.questdb.griffin.plan.logical.UnnestSpec;
import io.questdb.griffin.plan.logical.WindowJoinPlan;
import io.questdb.griffin.plan.logical.WindowJoinStep;
import io.questdb.griffin.plan.logical.WindowPlan;
import io.questdb.griffin.plan.logical.WindowSpec;
import io.questdb.std.Mutable;
import io.questdb.std.ObjectPool;

/**
 * The pools the binder, at every nesting depth, and the optimiser draw plan nodes, sub-queries and bound expressions from. A
 * node lives until the compiler rebuilds the statement's plan, so the nodes of a sub-query stay valid while the query
 * that contains it consumes them. {@link #syntheticNodes} and {@link #staticTypes} live for one binding call instead: the call takes a
 * mark on entry and rewinds to it on exit, so a sub-query bound inside the call releases only its own. Between
 * statements a pool of bound expressions keeps at most the configured expression pool capacity and a pool of plan
 * nodes at most the configured model pool capacity. The pools also number the columns the nodes define: an id is
 * unique across the statement, whichever query level or pass allocates it, so a plan can refer to any column of the
 * statement and a subtree can move between levels without renumbering.
 */
public final class PlanNodePools implements Mutable {
    public final ObjectPool<AggregatePlan> aggregates;
    public final ObjectPool<ColumnExpression> columns;
    public final ObjectPool<ConstantExpression> constants;
    public final ObjectPool<CursorExpression> cursors;
    public final ObjectPool<DistinctPlan> distincts;
    public final ObjectPool<FillPlan> fills;
    public final ObjectPool<FilterPlan> filters;
    public final ObjectPool<FunctionSourcePlan> functionSources;
    public final ObjectPool<FunctionExpression> functions;
    public final ObjectPool<HorizonJoinPlan> horizonJoinPlans;
    public final ObjectPool<HorizonJoinSlave> horizonJoinSlaves;
    public final ObjectPool<JoinDependency> joinDependencies;
    public final ObjectPool<JoinEquality> joinEqualities;
    public final ObjectPool<JoinGraph> joinGraphs;
    public final ObjectPool<JoinInput> joinInputs;
    public final ObjectPool<JoinPlan> joins;
    public final ObjectPool<LatestByPlan> latestByPlans;
    public final ObjectPool<LimitPlan> limits;
    public final int maxRetainedExpressions;
    public final int maxRetainedJoinContexts;
    public final ObjectPool<OuterColumnExpression> outerColumns;
    public final ObjectPool<BindVariableExpression> parameters;
    public final ObjectPool<ProjectPlan> projects;
    public final ObjectPool<SampleByPlan> sampleByPlans;
    public final ObjectPool<ScanPlan> scans;
    public final ObjectPool<SetOperationPlan> setOperations;
    public final ObjectPool<SortPlan> sorts;
    public final ObjectPool<StaticTypeFunction> staticTypes;
    final ObjectPool<Subquery> subqueries;
    public final ObjectPool<ExpressionNode> syntheticNodes;
    public final ObjectPool<TypeExpression> types;
    public final WindowExpression unboundedWindow = WindowExpression.FACTORY.newInstance();
    public final ObjectPool<UnnestSpec> unnestSpecs;
    public final ObjectPool<WindowJoinPlan> windowJoinPlans;
    public final ObjectPool<WindowJoinStep> windowJoinSteps;
    public final ObjectPool<WindowPlan> windowPlans;
    public final ObjectPool<WindowSpec> windowSpecs;
    public final ObjectPool<WindowExpression> windowSyntax;
    private int nextColumnId;

    public PlanNodePools(CairoConfiguration configuration) {
        final int expressionCapacity = configuration.getSqlExpressionPoolCapacity();
        final int planCapacity = configuration.getSqlModelPoolCapacity();
        this.maxRetainedExpressions = expressionCapacity;
        this.maxRetainedJoinContexts = configuration.getSqlJoinContextPoolCapacity();
        this.aggregates = new ObjectPool<>(AggregatePlan.FACTORY, 4, planCapacity);
        this.columns = new ObjectPool<>(ColumnExpression.FACTORY, 16, expressionCapacity);
        this.constants = new ObjectPool<>(ConstantExpression.FACTORY, 4, expressionCapacity);
        this.cursors = new ObjectPool<>(CursorExpression.FACTORY, 4, expressionCapacity);
        this.distincts = new ObjectPool<>(DistinctPlan.FACTORY, 4, planCapacity);
        this.fills = new ObjectPool<>(FillPlan.FACTORY, 4, planCapacity);
        this.filters = new ObjectPool<>(FilterPlan.FACTORY, 4, planCapacity);
        this.functionSources = new ObjectPool<>(FunctionSourcePlan.FACTORY, 4, planCapacity);
        this.functions = new ObjectPool<>(FunctionExpression.FACTORY, 16, expressionCapacity);
        this.horizonJoinPlans = new ObjectPool<>(HorizonJoinPlan.FACTORY, 2, planCapacity);
        this.horizonJoinSlaves = new ObjectPool<>(HorizonJoinSlave.FACTORY, 2, planCapacity);
        this.joinDependencies = new ObjectPool<>(JoinDependency.FACTORY, 8, maxRetainedJoinContexts);
        this.joinEqualities = new ObjectPool<>(JoinEquality.FACTORY, 8, maxRetainedJoinContexts);
        this.joinGraphs = new ObjectPool<>(JoinGraph.FACTORY, 2, planCapacity);
        this.joinInputs = new ObjectPool<>(JoinInput.FACTORY, 4, planCapacity);
        this.joins = new ObjectPool<>(JoinPlan.FACTORY, 2, planCapacity);
        this.latestByPlans = new ObjectPool<>(LatestByPlan.FACTORY, 4, planCapacity);
        this.limits = new ObjectPool<>(LimitPlan.FACTORY, 4, planCapacity);
        this.outerColumns = new ObjectPool<>(OuterColumnExpression.FACTORY, 4, expressionCapacity);
        this.parameters = new ObjectPool<>(BindVariableExpression.FACTORY, 8, expressionCapacity);
        this.projects = new ObjectPool<>(ProjectPlan.FACTORY, 4, planCapacity);
        this.sampleByPlans = new ObjectPool<>(SampleByPlan.FACTORY, 4, planCapacity);
        this.scans = new ObjectPool<>(ScanPlan.FACTORY, 4, planCapacity);
        this.setOperations = new ObjectPool<>(SetOperationPlan.FACTORY, 4, planCapacity);
        this.sorts = new ObjectPool<>(SortPlan.FACTORY, 4, planCapacity);
        this.staticTypes = new ObjectPool<>(StaticTypeFunction::new, 8, expressionCapacity);
        this.subqueries = new ObjectPool<>(Subquery.FACTORY, 4, planCapacity);
        this.syntheticNodes = new ObjectPool<>(ExpressionNode.FACTORY, 32, expressionCapacity);
        this.types = new ObjectPool<>(TypeExpression.FACTORY, 8, expressionCapacity);
        this.unnestSpecs = new ObjectPool<>(UnnestSpec.FACTORY, 4, planCapacity);
        this.windowJoinPlans = new ObjectPool<>(WindowJoinPlan.FACTORY, 2, planCapacity);
        this.windowJoinSteps = new ObjectPool<>(WindowJoinStep.FACTORY, 2, planCapacity);
        this.windowPlans = new ObjectPool<>(WindowPlan.FACTORY, 4, planCapacity);
        this.windowSpecs = new ObjectPool<>(WindowSpec.FACTORY, 4, planCapacity);
        this.windowSyntax = new ObjectPool<>(WindowExpression.FACTORY, 4, expressionCapacity);
    }

    @Override
    public void clear() {
        clearExpressions();
        aggregates.clear();
        distincts.clear();
        fills.clear();
        filters.clear();
        functionSources.clear();
        horizonJoinPlans.clear();
        horizonJoinSlaves.clear();
        joinDependencies.clear();
        joinEqualities.clear();
        joinGraphs.clear();
        joinInputs.clear();
        joins.clear();
        latestByPlans.clear();
        limits.clear();
        projects.clear();
        sampleByPlans.clear();
        scans.clear();
        setOperations.clear();
        sorts.clear();
        staticTypes.clear();
        syntheticNodes.clear();
        unnestSpecs.clear();
        windowJoinPlans.clear();
        windowJoinSteps.clear();
        windowPlans.clear();
        windowSpecs.clear();
        windowSyntax.clear();
        nextColumnId = 0;
    }

    /**
     * Recycles the bound expressions and the sub-queries they read while the plan nodes stay: a standalone expression
     * keeps only the function it instantiated.
     */
    public void clearExpressions() {
        columns.clear();
        constants.clear();
        cursors.clear();
        functions.clear();
        outerColumns.clear();
        parameters.clear();
        subqueries.clear();
        types.clear();
    }

    /**
     * Allocates a column id no other column of the statement has.
     */
    public int nextColumnId() {
        return nextColumnId++;
    }

    public void rewindSyntheticNodes(int mark) {
        syntheticNodes.rewind(mark);
    }
}
