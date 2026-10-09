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
import io.questdb.griffin.plan.logical.FilterPlan;
import io.questdb.griffin.plan.logical.FunctionExpression;
import io.questdb.griffin.plan.logical.JoinGraph;
import io.questdb.griffin.plan.logical.JoinInput;
import io.questdb.griffin.plan.logical.JoinPlan;
import io.questdb.griffin.plan.logical.LogicalPlan;
import io.questdb.std.ObjList;
import io.questdb.std.ObjectPool;

/**
 * Orders every join binding left a {@link JoinGraph} for and places the graph's filter conjuncts on the ordered steps.
 * A conjunct filters the step that joins the last input it reads, a conjunct that reads no input the last step. When
 * the join has an outer or temporal step, an ON conjunct filters no earlier than its own step unless it reads one input
 * and an ASOF, LT or SPLICE step anchors it, and a WHERE conjunct filters no earlier than the last RIGHT or FULL step
 * after it. A conjunct of the first step filters its input; the constant conjuncts filter the last step.
 */
final class JoinOrderPass {
    private final OptimiserContext context;
    private final ObjectPool<FilterPlan> filters;
    private final ObjList<BoundExpression> placed;
    private final JoinOrderSolver solver;

    JoinOrderPass(OptimiserContext context, ObjectPool<FilterPlan> filters, JoinOrderSolver solver, ObjList<BoundExpression> placed) {
        this.context = context;
        this.filters = filters;
        this.solver = solver;
        this.placed = placed;
    }

    /**
     * Whether an inner ON conjunct stated at {@code originPosition} filters the step at {@code target} instead: the
     * target precedes the origin, the origin step is INNER or CROSS, the target is an ASOF, LT or SPLICE step, and no
     * step between them nulls master rows.
     */
    private static boolean isTemporalAnchor(ObjList<JoinInput> ordered, int target, int originPosition) {
        if (target >= originPosition || JoinOrderSolver.isBarrier(ordered.getQuick(originPosition).getJoinType())) {
            return false;
        }
        switch (ordered.getQuick(target).getJoinType()) {
            case ASOF, LT, SPLICE -> {
            }
            default -> {
                return false;
            }
        }
        for (int i = target + 1; i < originPosition; i++) {
            if (ordered.getQuick(i).getJoinType().isMasterNulling()) {
                return false;
            }
        }
        return true;
    }

    private static int lastOrderedInput(BoundExpression expression, JoinPlan join) {
        if (expression instanceof ColumnExpression column) {
            return join.getOrderedInputs().indexOf(join.getInputs().getQuick(LogicalPlans.joinColumnSource(join, column.getColumnId())));
        }
        int last = -1;
        if (expression instanceof FunctionExpression call) {
            for (int i = 0, n = call.getArgumentCount(); i < n; i++) {
                last = Math.max(last, lastOrderedInput(call.argumentAt(i), join));
            }
        }
        return last;
    }

    /**
     * The ordered step whose ON clause states a conjunct that one or more clauses state: none, -1, when WHERE does,
     * otherwise the step the order joins last.
     */
    private static int originPosition(JoinGraph graph, int residual, JoinPlan join) {
        int position = -1;
        for (int i = 0, n = graph.getResidualOwnerCount(residual); i < n; i++) {
            final int owner = graph.getResidualOwner(residual, i);
            if (owner < 0) {
                return -1;
            }
            position = Math.max(position, join.getOrderedInputs().indexOf(join.getInputs().getQuick(owner)));
        }
        return position;
    }

    private void place(JoinPlan join, JoinGraph graph) throws SqlException {
        final ObjList<JoinInput> ordered = join.getOrderedInputs();
        final int last = ordered.size() - 1;
        boolean hasBarriers = false;
        for (int i = 1; i <= last; i++) {
            hasBarriers |= JoinOrderSolver.isBarrier(ordered.getQuick(i).getJoinType());
        }
        placed.clear();
        placed.setPos(ordered.size());
        final ObjList<BoundExpression> residuals = graph.getResiduals();
        for (int i = 0, n = residuals.size(); i < n; i++) {
            final BoundExpression residual = residuals.getQuick(i);
            int target = lastOrderedInput(residual, join);
            if (target < 0) {
                target = last;
            } else if (hasBarriers) {
                final int origin = originPosition(graph, i, join);
                if (origin >= 0) {
                    if (!isTemporalAnchor(ordered, target, origin) || !LogicalPlans.hasSingleColumnSource(residual, join)) {
                        target = Math.max(target, origin);
                    }
                } else {
                    for (int k = target + 1; k <= last; k++) {
                        if (ordered.getQuick(k).getJoinType().isMasterNulling()) {
                            target = k;
                        }
                    }
                }
            }
            placed.setQuick(target, context.getRewriter().combineConjunction(placed.getQuick(target), residual, residual.getPosition()));
        }
        for (int i = 0; i <= last; i++) {
            final BoundExpression predicate = placed.getQuick(i);
            if (predicate != null) {
                final JoinInput input = ordered.getQuick(i);
                if (i == 0) {
                    final FilterPlan filter = filters.next().of(input.getInput(), predicate, predicate.getPosition());
                    filter.deriveOutput();
                    input.setInput(filter);
                } else {
                    input.setPostJoinFilter(predicate);
                }
            }
        }
        if (graph.getConstantFilter() != null) {
            final JoinInput step = ordered.getQuick(last);
            step.setPostJoinFilter(context.getRewriter().combineConstantFilter(step.getPostJoinFilter(), graph.getConstantFilter(),
                    graph.getConstantFilterPosition()));
        }
    }

    void orderJoins(LogicalPlan plan) throws SqlException {
        for (int i = 0, n = plan.inputCount(); i < n; i++) {
            orderJoins(plan.inputAt(i));
        }
        if (plan instanceof JoinPlan join && join.getGraph() != null) {
            solver.order(join);
            place(join, join.getGraph());
            join.setGraph(null);
        }
    }
}
