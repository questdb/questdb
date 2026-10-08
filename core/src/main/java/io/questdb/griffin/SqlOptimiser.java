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
 * Runs the semantics-preserving plan rewrites in a fixed order. A step that may replace the plan root
 * returns the new root; a step that rewrites the plan in place returns nothing. The optimiser owns
 * the plan-shaped temporary lists the passes share and the {@link OptimiserContext} of the query level being
 * rewritten, borrows the compiler's leaf temporaries (the column-id set and three index lists, which the
 * generators and join ordering use in windows no optimisation can run in), and hands them, with the
 * statement binder's plan-node pools, to each pass through its constructor.
 */
final class SqlOptimiser implements Mutable {
    private final AggregateInputOrderPass aggregateInputOrder;
    private final AggregateRewritePass aggregateRewrite;
    private final ColumnPruningPass columnPruning;
    private final OptimiserContext context;
    private final DecorrelationPass decorrelation;
    private final FilterPushdownPass filterPushdown;
    private final NegativeLimitReversalPass negativeLimitReversal;
    private final ProjectionMergePass projectionMerge;
    private final TimestampEndpointPass timestampEndpoint;
    private final ObjList<BoundExpression> tmpConjuncts = new ObjList<>();
    private final ObjList<BoundExpression> tmpExpressions = new ObjList<>();
    private final ObjList<LogicalPlan> tmpPlans = new ObjList<>();
    private final OutputSchema tmpSchema = new OutputSchema();
    private final ObjList<JoinInput> tmpSteps = new ObjList<>();
    private final PlanVerifier verifier;
    private final WindowCsePass windowCse;

    /**
     * Allocates plan nodes from the given pools, whose owner empties them once the optimised plan and
     * every nested sub-query plan it optimised are no longer used. Expression rewrites allocate from
     * {@code rewriter}, which shares the pools of {@code functionBinder}, the binder that produces the plans.
     */
    SqlOptimiser(
            CharacterStore characterStore,
            PlanNodePools planNodes,
            IntHashSet columnIds,
            IntList tmpIndexes,
            IntList tmpValues,
            IntList tmpKeys,
            BoundExpressionRewriter rewriter,
            FunctionBinder functionBinder,
            FunctionInstantiator instantiator,
            TableFunctionSources functionSources
    ) {
        context = new OptimiserContext(rewriter, functionBinder, instantiator, functionSources);
        final ObjectPool<ColumnExpression> columns = planNodes.columns;
        final ObjectPool<ConstantExpression> constants = planNodes.constants;
        final ObjectPool<FilterPlan> filters = planNodes.filters;
        final ObjectPool<LimitPlan> limits = planNodes.limits;
        final ObjectPool<ProjectPlan> projects = planNodes.projects;
        final ObjectPool<SortPlan> sorts = planNodes.sorts;
        decorrelation = new DecorrelationPass(context, planNodes, characterStore, tmpExpressions, tmpConjuncts, tmpIndexes, tmpValues,
                tmpKeys, tmpPlans, tmpSchema, tmpSteps);
        timestampEndpoint = new TimestampEndpointPass(constants, limits, sorts);
        aggregateInputOrder = new AggregateInputOrderPass();
        filterPushdown = new FilterPushdownPass(context, aggregateInputOrder, tmpExpressions, filters, columns, projects,
                tmpIndexes, tmpValues, tmpConjuncts);
        columnPruning = new ColumnPruningPass(context, aggregateInputOrder, tmpExpressions, columns, projects, tmpIndexes, tmpValues,
                columnIds, tmpPlans, tmpSchema);
        projectionMerge = new ProjectionMergePass(context);
        aggregateRewrite = new AggregateRewritePass(context, projectionMerge, characterStore, columns, projects,
                tmpExpressions, tmpIndexes, tmpValues, tmpKeys);
        negativeLimitReversal = new NegativeLimitReversalPass(constants, sorts, tmpIndexes, tmpPlans);
        windowCse = new WindowCsePass(context, columns, projects, tmpPlans);
        verifier = SqlOptimiser.class.desiredAssertionStatus() ? new PlanVerifier(tmpPlans, tmpSchema, columnIds, tmpSteps) : null;
    }

    @Override
    public void clear() {
        tmpConjuncts.clear();
        context.clear();
        tmpExpressions.clear();
        tmpPlans.clear();
        tmpSchema.clear();
        tmpSteps.clear();
        aggregateInputOrder.clear();
        aggregateRewrite.clear();
        decorrelation.clear();
        columnPruning.clear();
        filterPushdown.clear();
    }

    /**
     * Rewrites a bound plan. Plan nodes the optimiser allocates stay valid until the pool owner empties
     * them, so one instance serves a statement and all of its nested sub-query plans. New columns take ids
     * from {@code nextColumnId}, the first id {@code root} does not use. The passes read it through the
     * {@link OptimiserContext} this method resets.
     */
    LogicalPlan optimise(LogicalPlan root, int nextColumnId, SqlExecutionContext executionContext) throws SqlException {
        clear();
        context.of(nextColumnId, executionContext);
        assert verifier.verifyBound(root);
        LogicalPlan plan = decorrelation.decorrelate(root);
        assert verifier.verify(plan, "decorrelation");

        // The endpoint LIMIT comes first so the later passes keep filters below it.
        timestampEndpoint.limitEndpointInputs(plan);
        plan = aggregateRewrite.rewriteAggregates(plan);
        assert verifier.verify(plan, "aggregate rewrite");

        // Pushdown and pruning read the ordered-branch marks when they drop an aggregate's input order.
        filterPushdown.pushJoinFilters(plan);
        aggregateInputOrder.collectOrderedBranchAggregates(plan);
        plan = filterPushdown.pushDownFilters(plan);
        filterPushdown.filterSharedDomains(plan);
        assert verifier.verify(plan, "filter placement");

        // Window calls merge before pruning drops the columns a merge leaves unread.
        windowCse.mergeWindowCalls(plan);
        columnPruning.prune(plan);
        plan = projectionMerge.collapseColumnProjects(plan);
        assert verifier.verify(plan, "column pruning");

        plan = negativeLimitReversal.reverseNegativeLimits(plan);
        SortEliminationPass.markMarkoutHorizons(plan);
        plan = SortEliminationPass.removeReorderedSorts(plan);
        assert verifier.verify(plan, "sort elimination");
        return plan;
    }
}
