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

import io.questdb.ParanoiaState;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.griffin.BoundExpressionRewriter;
import io.questdb.griffin.CharacterStore;
import io.questdb.griffin.FunctionFactoryCache;
import io.questdb.griffin.FunctionInstantiator;
import io.questdb.griffin.PlanNodePools;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.CallBinder;
import io.questdb.griffin.TableFunctionSources;
import io.questdb.griffin.PlanTables;
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
import io.questdb.griffin.plan.logical.Subquery;
import io.questdb.std.IntHashSet;
import io.questdb.std.IntList;
import io.questdb.std.Misc;
import io.questdb.std.Mutable;
import io.questdb.std.ObjList;
import io.questdb.std.ObjectPool;

/**
 * Runs the semantics-preserving plan rewrites, each an {@link OptimiserPass}, in a fixed order. The optimiser owns
 * the plan-shaped temporary lists the passes share and the {@link OptimiserContext} of the query level being
 * rewritten, borrows the compiler's leaf temporaries (the column-id set and four index lists, which the
 * generators and join ordering use in windows no optimisation can run in), and hands them, with the
 * statement binder's plan-node pools, to each pass through its constructor.
 */
public final class SqlOptimiser implements Mutable {
    private final AccessPathPlanning accessPathPlanning;
    private final AggregateInputOrder aggregateInputOrder;
    private final OptimiserContext context;
    private final Decorrelation decorrelation;
    private final OperatorPlanning operatorPlanning;
    private final ObjList<OptimiserPass> passes = new ObjList<>(10);
    private final ObjList<BoundExpression> tmpConjuncts = new ObjList<>();
    private final ObjList<BoundExpression> tmpExpressions = new ObjList<>();
    private final ObjList<LogicalPlan> tmpPlans = new ObjList<>();
    private final OutputSchema tmpSchema = new OutputSchema();
    private final ObjList<JoinInput> tmpSteps = new ObjList<>();
    private final PlanVerifier verifier;

    /**
     * Allocates plan nodes from the given pools, whose owner empties them once the optimised plan and
     * every nested sub-query plan it optimised are no longer used. Expression rewrites allocate from
     * {@code rewriter}, which shares the pools of the binder behind {@code callBinder}, the binder that produces the plans;
     * the passes read the access facts of the scanned tables from {@code planTables}, which code generation shares.
     */
    public SqlOptimiser(
            CairoConfiguration configuration,
            CharacterStore characterStore,
            FunctionFactoryCache functionFactoryCache,
            PlanNodePools planNodes,
            IntHashSet columnIds,
            IntList tmpIndexes,
            IntList tmpValues,
            IntList tmpKeys,
            IntList tmpSlaveKeys,
            BoundExpressionRewriter rewriter,
            CallBinder callBinder,
            FunctionInstantiator instantiator,
            TableFunctionSources functionSources,
            PlanTables planTables
    ) {
        context = new OptimiserContext(rewriter, callBinder, instantiator, functionSources, planNodes, planTables, characterStore,
                tmpExpressions);
        final ObjectPool<ColumnExpression> columns = planNodes.columns;
        final ObjectPool<ConstantExpression> constants = planNodes.constants;
        final ObjectPool<FilterPlan> filters = planNodes.filters;
        final ObjectPool<LimitPlan> limits = planNodes.limits;
        final ObjectPool<ProjectPlan> projects = planNodes.projects;
        final ObjectPool<SortPlan> sorts = planNodes.sorts;
        decorrelation = new Decorrelation(context, planNodes, characterStore, tmpConjuncts, tmpIndexes, tmpValues,
                tmpKeys, tmpPlans, tmpSchema, tmpSteps);
        aggregateInputOrder = new AggregateInputOrder();
        final FilterPushdown filterPushdown = new FilterPushdown(context, aggregateInputOrder, tmpExpressions, filters, columns, projects,
                tmpIndexes, tmpValues, tmpConjuncts);
        final ColumnPruning columnPruning = new ColumnPruning(context, aggregateInputOrder, tmpExpressions, columns, projects, tmpIndexes, tmpValues,
                columnIds, tmpPlans, tmpSchema);
        final ProjectionMerge projectionMerge = new ProjectionMerge(context);
        final AggregateRewrite aggregateRewrite = new AggregateRewrite(context, projectionMerge, columns, projects, tmpIndexes, tmpValues, tmpKeys);
        passes.add(new JoinOrdering(context, filters, new JoinOrderSolver(planNodes, columnIds, tmpIndexes, tmpValues, tmpSlaveKeys),
                tmpConjuncts));
        passes.add(decorrelation);
        // The endpoint LIMIT comes first so the later passes keep filters below it.
        passes.add(new TimestampEndpointLimit(constants, limits, sorts));
        passes.add(aggregateRewrite);
        passes.add(filterPushdown);
        // Window calls merge before pruning drops the columns a merge leaves unread.
        passes.add(new WindowCallMerge(context, columns, projects, tmpPlans));
        passes.add(columnPruning);
        passes.add(projectionMerge);
        passes.add(new NegativeLimitReversal(constants, sorts, tmpIndexes, tmpPlans));
        passes.add(new OrderPlanning(functionFactoryCache, sorts));
        operatorPlanning = new OperatorPlanning(configuration, context, tmpIndexes, tmpValues);
        accessPathPlanning = new AccessPathPlanning(configuration, context, operatorPlanning);
        verifier = ParanoiaState.PLAN_PARANOIA_MODE ? new PlanVerifier(tmpPlans, tmpSchema, columnIds, tmpSteps) : null;
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
        Misc.clearObjList(passes);
    }

    /**
     * Rewrites a bound plan. Plan nodes the optimiser allocates stay valid until the pool owner empties
     * them, so one instance serves a statement and all of its nested sub-query plans. New columns take ids
     * from the statement's sequence, which no column of any level uses. The passes read the execution context
     * through the {@link OptimiserContext} this method resets.
     */
    public LogicalPlan optimise(LogicalPlan root, SqlExecutionContext executionContext) throws SqlException {
        clear();
        context.of(executionContext);
        if (verifier != null) {
            verifier.verifyBound(root);
        }
        LogicalPlan plan = root;
        boolean hasDependentSteps = true;
        for (int i = 0, n = passes.size(); i < n; i++) {
            final OptimiserPass pass = passes.getQuick(i);
            if (verifier != null && pass.removesDependentSteps()) {
                verifier.recordOuterJoins(plan);
            }
            plan = pass.apply(plan);
            if (pass.removesDependentSteps()) {
                if (verifier != null) {
                    verifier.verifyDecorrelation(plan, pass.getName());
                }
                decorrelation.releaseCarriers();
            }
            hasDependentSteps &= !pass.removesDependentSteps();
            if (verifier != null) {
                if (hasDependentSteps) {
                    verifier.verifyBound(plan, pass.getName());
                } else {
                    verifier.verify(plan, pass.getName());
                }
            }
        }
        return plan;
    }

    /**
     * Decides how every scan of an optimised plan, and of each sub-query plan it reads, reads its table, see
     * {@link AccessPathPlanning}, and how the generator implements the order-sensitive operators over them, see
     * {@link OperatorPlanning#planOperators}. Runs just before the plan generates, at generation depth {@code depth},
     * under the execution context generation runs under. {@code isTimestampRequired}: the statement consuming the plan
     * requires its designated timestamp. Raises the static error generation raises for a scan before it reads the
     * scan's access path, and rejects a plan the generator cannot build.
     */
    public void planAccessPaths(LogicalPlan root, int depth, boolean isTimestampRequired, SqlExecutionContext executionContext) throws SqlException {
        context.of(executionContext);
        accessPathPlanning.plan(root, depth, isTimestampRequired);
        if (verifier != null) {
            verifier.verifyAccessPaths(root);
            final ObjList<Subquery> subqueries = accessPathPlanning.getSubqueries();
            for (int i = 0, n = subqueries.size(); i < n; i++) {
                verifier.verifyAccessPaths(subqueries.getQuick(i).getRoot());
            }
        }
    }

    /**
     * Makes every join keep copies of its slave rows instead of reading them back by row id.
     */
    public void setFullFatJoins(boolean isFullFatJoins) {
        operatorPlanning.setFullFatJoins(isFullFatJoins);
    }
}
