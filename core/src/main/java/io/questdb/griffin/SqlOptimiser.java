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
import io.questdb.griffin.plan.logical.ConstantExpression;
import io.questdb.griffin.plan.logical.FilterPlan;
import io.questdb.griffin.plan.logical.JoinInput;
import io.questdb.griffin.plan.logical.LimitPlan;
import io.questdb.griffin.plan.logical.LogicalPlan;
import io.questdb.griffin.plan.logical.OutputSchema;
import io.questdb.griffin.plan.logical.ProjectPlan;
import io.questdb.griffin.plan.logical.SortPlan;
import io.questdb.std.IntHashSet;
import io.questdb.std.IntList;
import io.questdb.std.Mutable;
import io.questdb.std.ObjList;
import io.questdb.std.ObjectPool;

/**
 * Runs the semantics-preserving plan rewrites in a fixed order. The optimiser owns the scratch the
 * passes share and the {@link OptimiserContext} of the query level being rewritten, and hands them, with
 * the statement binder's plan-node pools, to each pass through its constructor.
 */
final class SqlOptimiser implements Mutable {
    private final AggregateInputOrderPass aggregateInputOrder;
    private final AggregateRewritePass aggregateRewrite;
    private final ColumnPruningPass columnPruning;
    private final ObjList<BoundExpression> conjunctScratch = new ObjList<>();
    private final OptimiserContext context = new OptimiserContext();
    private final DecorrelationPass decorrelation;
    private final ObjList<BoundExpression> expressionScratch = new ObjList<>();
    private final FilterPushdownPass filterPushdown;
    private final IntList indexScratch = new IntList();
    private final IntList keyScratch = new IntList();
    private final NegativeLimitReversalPass negativeLimitReversal;
    private final ObjList<LogicalPlan> planScratch = new ObjList<>();
    private final ProjectionMergePass projectionMerge;
    private final OutputSchema schemaScratch = new OutputSchema();
    private final ObjList<JoinInput> stepScratch = new ObjList<>();
    private final TimestampEndpointPass timestampEndpoint;
    private final IntList valueScratch = new IntList();
    private final PlanVerifier verifier;
    private final WindowCsePass windowCse;

    /**
     * Allocates plan nodes from the given pools, whose owner empties them once the optimised plan and
     * every nested sub-query plan it optimised are no longer used.
     */
    SqlOptimiser(CharacterStore characterStore, BindContext planNodes, IntHashSet columnIds) {
        final ObjectPool<ColumnExpression> columns = planNodes.columns;
        final ObjectPool<ConstantExpression> constants = planNodes.constants;
        final ObjectPool<FilterPlan> filters = planNodes.filters;
        final ObjectPool<LimitPlan> limits = planNodes.limits;
        final ObjectPool<ProjectPlan> projects = planNodes.projects;
        final ObjectPool<SortPlan> sorts = planNodes.sorts;
        decorrelation = new DecorrelationPass(context, planNodes, characterStore, expressionScratch, conjunctScratch, indexScratch, valueScratch,
                keyScratch, planScratch, schemaScratch, stepScratch);
        timestampEndpoint = new TimestampEndpointPass(constants, limits, sorts);
        aggregateInputOrder = new AggregateInputOrderPass();
        filterPushdown = new FilterPushdownPass(context, aggregateInputOrder, expressionScratch, filters, columns, projects,
                indexScratch, valueScratch, conjunctScratch);
        columnPruning = new ColumnPruningPass(context, aggregateInputOrder, expressionScratch, columns, projects, indexScratch, valueScratch,
                columnIds, planScratch, schemaScratch);
        projectionMerge = new ProjectionMergePass(context);
        aggregateRewrite = new AggregateRewritePass(context, projectionMerge, characterStore, columns, projects,
                expressionScratch, indexScratch, valueScratch, keyScratch);
        negativeLimitReversal = new NegativeLimitReversalPass(constants, sorts, indexScratch, planScratch);
        windowCse = new WindowCsePass(context, columns, projects, planScratch);
        verifier = new PlanVerifier(planScratch, schemaScratch, columnIds, stepScratch);
    }

    @Override
    public void clear() {
        conjunctScratch.clear();
        context.clear();
        expressionScratch.clear();
        indexScratch.clear();
        keyScratch.clear();
        planScratch.clear();
        schemaScratch.clear();
        stepScratch.clear();
        valueScratch.clear();
        aggregateInputOrder.clear();
        aggregateRewrite.clear();
        decorrelation.clear();
        columnPruning.clear();
        filterPushdown.clear();
    }

    /**
     * Rewrites a bound plan. Plan nodes the optimiser allocates stay valid until the pool owner empties
     * them, so one instance serves a statement and all of its nested sub-query plans. Expression rewrites
     * allocate from {@code rewriter}, which shares the pools of {@code functionBinder}, the binder that produced
     * {@code root}; new columns take ids from {@code nextColumnId}, the first id {@code root} does not use. The
     * passes read these through the {@link OptimiserContext} this method resets.
     */
    LogicalPlan optimise(LogicalPlan root, BoundExpressionRewriter rewriter, FunctionBinder functionBinder, FunctionInstantiator instantiator,
                         TableFunctionSources functionSources, int nextColumnId, SqlExecutionContext executionContext) throws SqlException {
        clear();
        context.of(rewriter, functionBinder, instantiator, functionSources, nextColumnId, executionContext);
        assert verifier.verifyBound(root);
        LogicalPlan plan = decorrelation.decorrelate(root);
        assert verifier.verify(plan, "DecorrelationPass.decorrelate");
        timestampEndpoint.limitEndpointInputs(plan);
        assert verifier.verify(plan, "TimestampEndpointPass.limitEndpointInputs");
        plan = aggregateRewrite.rewriteAggregates(plan);
        assert verifier.verify(plan, "AggregateRewritePass.rewriteAggregates");
        filterPushdown.pushJoinFilters(plan);
        assert verifier.verify(plan, "FilterPushdownPass.pushJoinFilters");
        aggregateInputOrder.collectOrderedBranchAggregates(plan);
        assert verifier.verify(plan, "AggregateInputOrderPass.collectOrderedBranchAggregates");
        plan = filterPushdown.pushDownFilters(plan);
        assert verifier.verify(plan, "FilterPushdownPass.pushDownFilters");
        filterPushdown.filterSharedDomains(plan);
        assert verifier.verify(plan, "FilterPushdownPass.filterSharedDomains");
        windowCse.mergeWindowCalls(plan);
        assert verifier.verify(plan, "WindowCsePass.mergeWindowCalls");
        columnPruning.prune(plan);
        assert verifier.verify(plan, "ColumnPruningPass.prune");
        plan = projectionMerge.collapseColumnProjects(plan);
        assert verifier.verify(plan, "ProjectionMergePass.collapseColumnProjects");
        plan = negativeLimitReversal.reverseNegativeLimits(plan);
        assert verifier.verify(plan, "NegativeLimitReversalPass.reverseNegativeLimits");
        SortEliminationPass.markMarkoutHorizons(plan);
        assert verifier.verify(plan, "SortEliminationPass.markMarkoutHorizons");
        plan = SortEliminationPass.removeReorderedSorts(plan, false, false);
        assert verifier.verify(plan, "SortEliminationPass.removeReorderedSorts");
        return plan;
    }
}
