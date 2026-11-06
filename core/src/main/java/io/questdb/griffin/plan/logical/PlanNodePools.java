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

import io.questdb.ParanoiaState;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.griffin.StaticTypeFunction;
import io.questdb.std.IntList;
import io.questdb.std.Mutable;
import io.questdb.std.ObjectFactory;
import io.questdb.std.ObjectPool;

/**
 * The pools the binder, at every nesting depth, and the optimiser draw plan nodes, sub-queries and bound expressions from. A
 * node lives until the compiler rebuilds the statement's plan, so the nodes of a sub-query stay valid while the query
 * that contains it consumes them. {@link #staticTypes} live for one binding call instead: the call takes a
 * mark on entry and rewinds to it on exit, so a sub-query bound inside the call releases only its own. Between
 * statements a pool keeps what its recent uses took, see {@link io.questdb.std.RetainedCapacity}, and never trims
 * below the configured expression pool capacity for bound expressions or the configured model pool capacity for plan
 * nodes. The pools also number the columns the nodes define: an id is unique across the statement, whichever query
 * level or pass allocates it, so a plan can refer to any column of the statement and a subtree can move between levels
 * without renumbering. Under {@link ParanoiaState#PLAN_PARANOIA_MODE}, clearing the pools checks that every plan node
 * and part they keep holds the facts of a new one, see {@link PlanFactSource#visitFacts}.
 */
public final class PlanNodePools implements Mutable {
    public final ObjectPool<AggregatePlan> aggregates;
    public final ObjectPool<ColumnExpression> columns;
    public final ObjectPool<ConstantExpression> constants;
    public final ObjectPool<CursorExpression> cursors;
    public final ObjectPool<DistinctPlan> distincts;
    public final ObjectPool<DomainReadPlan> domainReads;
    public final ObjectPool<FillPlan> fills;
    public final ObjectPool<FilterPlan> filters;
    public final ObjectPool<FunctionSourcePlan> functionSources;
    public final ObjectPool<FunctionExpression> functions;
    public final ObjectPool<IntList> groupingSets;
    public final ObjectPool<HashJoinAggregation> hashJoinAggregations;
    public final ObjectPool<HorizonJoinPlan> horizonJoinPlans;
    public final ObjectPool<HorizonJoinSlave> horizonJoinSlaves;
    public final ObjectPool<JoinDependency> joinDependencies;
    public final ObjectPool<JoinEquality> joinEqualities;
    public final ObjectPool<JoinGraph> joinGraphs;
    public final ObjectPool<JoinInput> joinInputs;
    public final ObjectPool<JoinPlan> joins;
    public final ObjectPool<LatestByPlan> latestByPlans;
    public final ObjectPool<LimitPlan> limits;
    public final int minRetainedExpressions;
    public final int minRetainedJoinNodes;
    public final ObjectPool<OuterColumnExpression> outerColumns;
    public final ObjectPool<BindVariableExpression> parameters;
    public final ObjectPool<ProjectPlan> projects;
    public final ObjectPool<SampleByPlan> sampleByPlans;
    public final ObjectPool<ScanPlan> scans;
    public final ObjectPool<SetOperationPlan> setOperations;
    public final ObjectPool<SortPlan> sorts;
    public final ObjectPool<StaticTypeFunction> staticTypes;
    public final ObjectPool<Subquery> subqueries;
    public final ObjectPool<TypeExpression> types;
    public final ObjectPool<UnnestSpec> unnestSpecs;
    public final ObjectPool<WindowJoinPlan> windowJoinPlans;
    public final ObjectPool<WindowJoinStep> windowJoinSteps;
    public final ObjectPool<WindowPlan> windowPlans;
    public final ObjectPool<WindowSpec> windowSpecs;
    private final PlanFactRecorder clearedFacts = ParanoiaState.PLAN_PARANOIA_MODE ? new PlanFactRecorder() : null;
    private final PlanFactRecorder newFacts = ParanoiaState.PLAN_PARANOIA_MODE ? new PlanFactRecorder() : null;
    private boolean hasSharedSources;
    private int nextColumnId;

    public PlanNodePools(CairoConfiguration configuration) {
        final int expressionCapacity = configuration.getSqlExpressionPoolCapacity();
        final int planCapacity = configuration.getSqlModelPoolCapacity();
        this.minRetainedExpressions = expressionCapacity;
        this.minRetainedJoinNodes = configuration.getSqlJoinContextPoolCapacity();
        this.aggregates = new ObjectPool<>(AggregatePlan.FACTORY, 4, planCapacity);
        this.columns = new ObjectPool<>(ColumnExpression.FACTORY, 16, expressionCapacity);
        this.constants = new ObjectPool<>(ConstantExpression.FACTORY, 4, expressionCapacity);
        this.cursors = new ObjectPool<>(CursorExpression.FACTORY, 4, expressionCapacity);
        this.distincts = new ObjectPool<>(DistinctPlan.FACTORY, 4, planCapacity);
        this.domainReads = new ObjectPool<>(DomainReadPlan.FACTORY, 2, planCapacity);
        this.fills = new ObjectPool<>(FillPlan.FACTORY, 4, planCapacity);
        this.filters = new ObjectPool<>(FilterPlan.FACTORY, 4, planCapacity);
        this.functionSources = new ObjectPool<>(FunctionSourcePlan.FACTORY, 4, planCapacity);
        this.functions = new ObjectPool<>(FunctionExpression.FACTORY, 16, expressionCapacity);
        this.groupingSets = new ObjectPool<>(IntList::new, 8, planCapacity);
        this.hashJoinAggregations = new ObjectPool<>(HashJoinAggregation.FACTORY, 2, planCapacity);
        this.horizonJoinPlans = new ObjectPool<>(HorizonJoinPlan.FACTORY, 2, planCapacity);
        this.horizonJoinSlaves = new ObjectPool<>(HorizonJoinSlave.FACTORY, 2, planCapacity);
        this.joinDependencies = new ObjectPool<>(JoinDependency.FACTORY, 8, minRetainedJoinNodes);
        this.joinEqualities = new ObjectPool<>(JoinEquality.FACTORY, 8, minRetainedJoinNodes);
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
        this.types = new ObjectPool<>(TypeExpression.FACTORY, 8, expressionCapacity);
        this.unnestSpecs = new ObjectPool<>(UnnestSpec.FACTORY, 4, planCapacity);
        this.windowJoinPlans = new ObjectPool<>(WindowJoinPlan.FACTORY, 2, planCapacity);
        this.windowJoinSteps = new ObjectPool<>(WindowJoinStep.FACTORY, 2, planCapacity);
        this.windowPlans = new ObjectPool<>(WindowPlan.FACTORY, 4, planCapacity);
        this.windowSpecs = new ObjectPool<>(WindowSpec.FACTORY, 4, planCapacity);
    }

    @Override
    public void clear() {
        clearExpressions();
        aggregates.clear();
        distincts.clear();
        domainReads.clear();
        fills.clear();
        filters.clear();
        functionSources.clear();
        groupingSets.clear();
        hashJoinAggregations.clear();
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
        unnestSpecs.clear();
        windowJoinPlans.clear();
        windowJoinSteps.clear();
        windowPlans.clear();
        windowSpecs.clear();
        hasSharedSources = false;
        nextColumnId = 0;
        if (clearedFacts != null) {
            verifyCleared();
        }
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
     * Whether a decorrelation domain of the statement re-reads a join input it shares with its master, see
     * {@link #recordSharedSource()}.
     */
    public boolean hasSharedSources() {
        return hasSharedSources;
    }

    /**
     * Allocates a column id no other column of the statement has.
     */
    public int nextColumnId() {
        return nextColumnId++;
    }

    /**
     * Records that a decorrelation domain of the statement re-reads a join input it shares with its master, see
     * {@link AggregatePlan#getSharedSource()}.
     */
    public void recordSharedSource() {
        hasSharedSources = true;
    }

    private void verifyCleared() {
        verifyCleared(aggregates, AggregatePlan.FACTORY);
        verifyCleared(distincts, DistinctPlan.FACTORY);
        verifyCleared(domainReads, DomainReadPlan.FACTORY);
        verifyCleared(fills, FillPlan.FACTORY);
        verifyCleared(filters, FilterPlan.FACTORY);
        verifyCleared(functionSources, FunctionSourcePlan.FACTORY);
        verifyCleared(horizonJoinPlans, HorizonJoinPlan.FACTORY);
        verifyCleared(horizonJoinSlaves, HorizonJoinSlave.FACTORY);
        verifyCleared(joinInputs, JoinInput.FACTORY);
        verifyCleared(joins, JoinPlan.FACTORY);
        verifyCleared(latestByPlans, LatestByPlan.FACTORY);
        verifyCleared(limits, LimitPlan.FACTORY);
        verifyCleared(projects, ProjectPlan.FACTORY);
        verifyCleared(sampleByPlans, SampleByPlan.FACTORY);
        verifyCleared(scans, ScanPlan.FACTORY);
        verifyCleared(setOperations, SetOperationPlan.FACTORY);
        verifyCleared(sorts, SortPlan.FACTORY);
        verifyCleared(unnestSpecs, UnnestSpec.FACTORY);
        verifyCleared(windowJoinPlans, WindowJoinPlan.FACTORY);
        verifyCleared(windowJoinSteps, WindowJoinStep.FACTORY);
        verifyCleared(windowPlans, WindowPlan.FACTORY);
        verifyCleared(windowSpecs, WindowSpec.FACTORY);
    }

    /**
     * Fails when an object the pool keeps, which clearing the pool cleared, holds a fact a new object does not.
     */
    private <T extends PlanFactSource> void verifyCleared(ObjectPool<T> pool, ObjectFactory<T> factory) {
        newFacts.record(factory.newInstance());
        for (int i = 0, n = pool.getCapacity(); i < n; i++) {
            final T object = pool.peekQuick(i);
            clearedFacts.record(object);
            final String fact = clearedFacts.firstDifference(newFacts);
            if (fact != null) {
                throw new AssertionError(PlanFacts.CLEARED_FACT + ": " + object.getClass().getSimpleName() + '.' + fact);
            }
        }
    }
}
