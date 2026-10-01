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
import io.questdb.griffin.plan.logical.LimitPlan;
import io.questdb.griffin.plan.logical.LogicalPlan;
import io.questdb.griffin.plan.logical.ProjectPlan;
import io.questdb.griffin.plan.logical.SortPlan;
import io.questdb.std.IntHashSet;
import io.questdb.std.IntList;
import io.questdb.std.Mutable;
import io.questdb.std.ObjList;
import io.questdb.std.ObjectPool;

/**
 * Runs the semantics-preserving plan rewrites in a fixed order. The optimiser owns the scratch the
 * passes share and hands it, with the statement binder's plan-node pools, to each pass through its
 * constructor.
 */
final class SqlOptimiser implements Mutable {
    private final AggregateInputOrderPass aggregateInputOrder;
    private final AggregateRewritePass aggregateRewrite;
    private final ColumnPruningPass columnPruning;
    private final ObjList<BoundExpression> expressionScratch = new ObjList<>();
    private final FilterPushdownPass filterPushdown;
    private final IntList indexScratch = new IntList();
    private final NegativeLimitReversalPass negativeLimitReversal;
    private final ProjectionMergePass projectionMerge;
    private final TimestampEndpointPass timestampEndpoint;
    private final IntList valueScratch = new IntList();

    /**
     * Allocates plan nodes from the given pools, whose owner empties them once the optimised plan and
     * every nested sub-query plan it optimised are no longer used.
     */
    SqlOptimiser(
            CharacterStore characterStore,
            ObjectPool<ColumnExpression> columns,
            ObjectPool<ConstantExpression> constants,
            ObjectPool<FilterPlan> filters,
            ObjectPool<LimitPlan> limits,
            ObjectPool<ProjectPlan> projects,
            ObjectPool<SortPlan> sorts,
            IntHashSet columnIds
    ) {
        timestampEndpoint = new TimestampEndpointPass(constants, limits, sorts);
        aggregateInputOrder = new AggregateInputOrderPass();
        filterPushdown = new FilterPushdownPass(aggregateInputOrder, expressionScratch, filters, columns, projects,
                indexScratch, valueScratch);
        columnPruning = new ColumnPruningPass(aggregateInputOrder, expressionScratch, columns, projects, indexScratch, valueScratch,
                columnIds);
        projectionMerge = new ProjectionMergePass();
        aggregateRewrite = new AggregateRewritePass(projectionMerge, characterStore, columns, projects,
                expressionScratch, indexScratch, valueScratch);
        negativeLimitReversal = new NegativeLimitReversalPass(constants, sorts, indexScratch);
    }

    @Override
    public void clear() {
        expressionScratch.clear();
        indexScratch.clear();
        valueScratch.clear();
        aggregateInputOrder.clear();
        aggregateRewrite.clear();
        columnPruning.clear();
        filterPushdown.clear();
        negativeLimitReversal.clear();
        projectionMerge.clear();
    }

    /**
     * Rewrites a bound plan. Plan nodes the optimiser allocates stay valid until the pool owner empties
     * them, so one instance serves a statement and all of its nested sub-query plans. Expression rewrites
     * allocate from {@code functionBinder}, the binder that produced {@code root}; new columns take
     * ids from {@code nextColumnId}, the first id {@code root} does not use.
     */
    LogicalPlan optimise(LogicalPlan root, FunctionBinder functionBinder, int nextColumnId, SqlExecutionContext executionContext) throws SqlException {
        clear();
        timestampEndpoint.limitEndpointInputs(root);
        projectionMerge.of(functionBinder);
        LogicalPlan plan = aggregateRewrite.of(functionBinder, nextColumnId, executionContext).rewriteAggregates(root);
        filterPushdown.of(functionBinder, executionContext).pushJoinFilters(plan);
        aggregateInputOrder.collectOrderedBranchAggregates(plan);
        plan = filterPushdown.pushDownFilters(plan);
        filterPushdown.filterSharedDomains(plan);
        columnPruning.of(functionBinder).prune(plan);
        plan = projectionMerge.collapseColumnProjects(plan);
        plan = negativeLimitReversal.reverseNegativeLimits(plan);
        SortEliminationPass.markMarkoutHorizons(plan);
        return SortEliminationPass.removeReorderedSorts(plan, false, false);
    }
}
