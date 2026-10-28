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
import io.questdb.griffin.CallBinder;
import io.questdb.griffin.CharacterStore;
import io.questdb.griffin.CompilerTemporaries;
import io.questdb.griffin.FunctionFactoryCache;
import io.questdb.griffin.FunctionInstantiator;
import io.questdb.griffin.OuterColumnReads;
import io.questdb.griffin.PlanNodePools;
import io.questdb.griffin.PlanTables;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.TableFunctionSources;
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
import io.questdb.griffin.plan.logical.UniqueColumnNames;
import io.questdb.std.IntHashSet;
import io.questdb.std.IntIntHashMap;
import io.questdb.std.IntList;
import io.questdb.std.Misc;
import io.questdb.std.Mutable;
import io.questdb.std.ObjList;
import io.questdb.std.ObjectPool;
import org.jetbrains.annotations.TestOnly;

/**
 * Runs the semantics-preserving plan rewrites, each an {@link OptimiserPass}, in a fixed order: {@link JoinOrdering},
 * then {@link Decorrelation}, then the passes it lists, in list order. The first two run outside the list
 * because the plan invariants after them differ: join ordering rewrites a bound plan whose dependent join steps still
 * read the inputs before them through outer columns, and decorrelation removes those steps, which
 * {@link PlanVerifier#verifyDecorrelation} checks against the barrier joins recorded before it, while the carriers it
 * records hold only until the next pass renames columns. Every later pass rewrites a plan without dependent steps. The
 * optimiser owns the temporary lists the passes hold across expression rewrites and call bindings and the
 * {@link OptimiserContext} of the query level being rewritten, borrows the leaf members of the compiler's
 * {@link CompilerTemporaries}, which the binder and the generators use in windows no optimisation can run in, and hands
 * them, with the statement binder's plan-node pools, to each pass through its constructor.
 */
public final class SqlOptimiser implements Mutable {
    private final AccessPathPlanning accessPathPlanning;
    private final AggregateInputOrder aggregateInputOrder;
    private final AggregateRewrite aggregateRewrite;
    private final OptimiserContext context;
    private final Decorrelation decorrelation;
    private final JoinOrdering joinOrdering;
    private final OperatorPlanning operatorPlanning;
    private final ObjList<OptimiserPass> passes = new ObjList<>(10);
    private final ColumnExpression tmpColumn = new ColumnExpression();
    private final IntIntHashMap tmpColumnMap = new IntIntHashMap();
    private final ObjList<BoundExpression> tmpConjuncts = new ObjList<>();
    private final IntList tmpOrder = new IntList();
    private final IntList tmpPairs = new IntList();
    private final IntList tmpPending = new IntList();
    private final ObjList<LogicalPlan> tmpPlanPath = new ObjList<>();
    private final ObjList<LogicalPlan> tmpPlans = new ObjList<>();
    private final ProjectPlan tmpProjection = new ProjectPlan();
    private final ObjList<JoinInput> tmpSteps = new ObjList<>();
    private final PlanVerifier verifier;
    private final WindowCallMerge windowCallMerge;

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
            CompilerTemporaries temporaries,
            OuterColumnReads outerColumnReads,
            BoundExpressionRewriter rewriter,
            CallBinder callBinder,
            FunctionInstantiator instantiator,
            TableFunctionSources functionSources,
            PlanTables planTables
    ) {
        final IntHashSet columnIds = temporaries.ids;
        final IntList tmpIndexes = temporaries.indexes;
        final IntList tmpValues = temporaries.values;
        final IntList tmpKeys = temporaries.masterKeys;
        final IntList tmpSlaveKeys = temporaries.slaveKeys;
        final ObjList<BoundExpression> tmpExpressions = temporaries.expressions;
        final OutputSchema tmpSchema = temporaries.scope;
        final IntIntHashMap substitution = temporaries.idMap;
        final UniqueColumnNames takenNames = temporaries.uniqueNames;
        context = new OptimiserContext(rewriter, callBinder, instantiator, functionSources, planNodes, planTables);
        final ObjectPool<ColumnExpression> columns = planNodes.columns;
        final ObjectPool<ConstantExpression> constants = planNodes.constants;
        final ObjectPool<FilterPlan> filters = planNodes.filters;
        final ObjectPool<LimitPlan> limits = planNodes.limits;
        final ObjectPool<ProjectPlan> projects = planNodes.projects;
        final ObjectPool<SortPlan> sorts = planNodes.sorts;
        decorrelation = new Decorrelation(context, planNodes, characterStore, tmpConjuncts, tmpIndexes, tmpValues,
                tmpKeys, tmpSlaveKeys, tmpOrder, tmpPending, tmpPairs, tmpPlans, tmpSchema, tmpSteps, substitution, tmpColumnMap, takenNames, outerColumnReads);
        aggregateInputOrder = new AggregateInputOrder();
        final ProjectionMerge projectionMerge = new ProjectionMerge(context, columnIds);
        // The endpoint LIMIT comes first so the later passes keep filters below it.
        passes.add(new TimestampEndpointLimit(constants, limits, sorts));
        aggregateRewrite = new AggregateRewrite(context, projectionMerge, characterStore, columns, takenNames, tmpProjection, tmpColumn,
                tmpKeys, columnIds, substitution, tmpSlaveKeys, tmpIndexes, tmpValues);
        passes.add(aggregateRewrite);
        final ColumnReads columnReads = new ColumnReads();
        joinOrdering = new JoinOrdering(context, columnReads, filters, new JoinOrderSolver(context, planNodes, columnIds, tmpIndexes, tmpValues, tmpSlaveKeys, tmpKeys,
                tmpOrder, tmpPending, temporaries.readyInputs, temporaries.joinChildren, temporaries.joinChildrenPool, temporaries.joinTypes,
                temporaries.joinDependencies, temporaries.longs), tmpConjuncts);
        passes.add(new FilterPushdown(context, aggregateInputOrder, tmpExpressions, filters, columns, tmpColumnMap, tmpSlaveKeys, tmpValues,
                tmpPairs, tmpConjuncts));
        passes.add(new JoinNesting(context, planNodes.joins, planNodes.joinInputs, filters));
        // Window calls merge before pruning drops the columns a merge leaves unread.
        windowCallMerge = new WindowCallMerge(context, tmpPlans, tmpPlanPath, tmpIndexes, substitution, tmpOrder, tmpPairs, tmpColumnMap);
        passes.add(windowCallMerge);
        passes.add(new ColumnPruning(aggregateInputOrder, projectionMerge, columnReads, tmpExpressions, columns, projects, tmpIndexes, tmpValues,
                columnIds, tmpPlans, tmpPlanPath, tmpSchema));
        passes.add(projectionMerge);
        passes.add(new NegativeLimitReversal(constants, sorts, tmpIndexes, tmpPlans));
        passes.add(new OrderPlanning(configuration, functionFactoryCache, columnIds));
        operatorPlanning = new OperatorPlanning(configuration, context, planNodes, tmpIndexes, tmpValues, tmpSteps);
        accessPathPlanning = new AccessPathPlanning(configuration, context, operatorPlanning, tmpConjuncts, tmpExpressions, tmpKeys, tmpValues,
                temporaries.sink, temporaries.longs);
        verifier = ParanoiaState.PLAN_PARANOIA_MODE
                ? new PlanVerifier(tmpPlans, tmpSchema, columnIds, tmpSteps, tmpExpressions, tmpPairs) : null;
    }

    @Override
    public void clear() {
        tmpColumn.clear();
        tmpColumnMap.clear();
        tmpConjuncts.clear();
        context.clear();
        tmpOrder.clear();
        tmpPairs.clear();
        tmpPending.clear();
        tmpPlanPath.clear();
        tmpPlans.clear();
        tmpProjection.clear();
        tmpSteps.clear();
        aggregateInputOrder.clear();
        joinOrdering.clear();
        decorrelation.clear();
        Misc.clearObjList(passes);
    }

    @TestOnly
    public long getWindowCallComparisonCount() {
        return windowCallMerge.getCallComparisonCount();
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
        LogicalPlan plan = aggregateRewrite.normaliseSums(root);
        if (verifier != null) {
            verifier.verifyBound(plan);
        }
        plan = joinOrdering.apply(plan);
        if (verifier != null) {
            verifier.verifyBound(plan, joinOrdering.getName());
            verifier.recordBarrierJoins(plan);
        }
        plan = decorrelation.apply(plan);
        if (verifier != null) {
            decorrelation.verify(plan, verifier);
        }
        if (verifier != null) {
            verifier.verify(plan, decorrelation.getName());
        }
        for (int i = 0, n = passes.size(); i < n; i++) {
            final OptimiserPass pass = passes.getQuick(i);
            plan = pass.apply(plan);
            if (verifier != null) {
                verifier.verify(plan, pass.getName());
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
            final boolean hasSharedSources = context.hasSharedSources();
            verifier.verifyAccessPaths(root);
            verifier.verifySharedSources(root, hasSharedSources);
            final ObjList<Subquery> subqueries = accessPathPlanning.getSubqueries();
            for (int i = 0, n = subqueries.size(); i < n; i++) {
                final LogicalPlan subqueryRoot = subqueries.getQuick(i).getRoot();
                verifier.verifyAccessPaths(subqueryRoot);
                verifier.verifySharedSources(subqueryRoot, hasSharedSources);
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
