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

import io.questdb.griffin.LogicalPlans;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.plan.logical.AggregatePlan;
import io.questdb.griffin.plan.logical.BoundExpression;
import io.questdb.griffin.plan.logical.ColumnExpression;
import io.questdb.griffin.plan.logical.DistinctPlan;
import io.questdb.griffin.plan.logical.ExpressionVisitor;
import io.questdb.griffin.plan.logical.FilterPlan;
import io.questdb.griffin.plan.logical.JoinGraph;
import io.questdb.griffin.plan.logical.JoinInput;
import io.questdb.griffin.plan.logical.JoinPlan;
import io.questdb.griffin.plan.logical.LimitPlan;
import io.questdb.griffin.plan.logical.LogicalPlan;
import io.questdb.griffin.plan.logical.OutputSchema;
import io.questdb.griffin.plan.logical.ProjectPlan;
import io.questdb.griffin.plan.logical.SetOperationPlan;
import io.questdb.griffin.plan.logical.SortPlan;
import io.questdb.griffin.plan.logical.TreeWalk;
import io.questdb.std.ObjList;
import io.questdb.std.ObjectPool;

/**
 * Orders every join binding left a {@link JoinGraph} for and places the graph's filter conjuncts on the ordered steps.
 * Inner and cross joins commute, but a join that designates its first input's timestamp keeps that input first while
 * any part of the query can see the join's rows in their order. The order stays invisible only along a path of nodes
 * that provably ignore it, see {@link #isInputOrderVisible}, up to the query result of a statement that stores no
 * designated timestamp; every other node, including any that a later change adds, sees it. A join that leads with
 * another input designates no timestamp, and neither does a node on that path that forwarded the join's.
 * A conjunct filters the step that joins the last input it reads, a conjunct that reads no input the last step. When
 * the join has an outer or temporal step, an ON conjunct filters no earlier than its own step unless it reads one input
 * and an ASOF, LT or SPLICE step anchors it, and a WHERE conjunct filters no earlier than the last RIGHT or FULL step
 * after it. A conjunct of the first step filters its input; the constant conjuncts filter the last step.
 */
final class JoinOrdering implements OptimiserPass {
    private final OptimiserContext context;
    private final ObjectPool<FilterPlan> filters;
    private final ObjList<BoundExpression> placed;
    private final ExpressionVisitor orderedInputReads = expression -> {
        if (expression instanceof ColumnExpression column) {
            final JoinPlan join = this.readJoin;
            final int source = LogicalPlans.joinColumnSource(join, column.getColumnId());
            this.lastPosition = Math.max(this.lastPosition, join.getOrderedInputs().indexOf(join.getInputs().getQuick(source)));
        }
        return TreeWalk.CONTINUE;
    };
    private final JoinOrderSolver solver;
    private int lastPosition;
    private JoinPlan readJoin;

    JoinOrdering(OptimiserContext context, ObjectPool<FilterPlan> filters, JoinOrderSolver solver, ObjList<BoundExpression> placed) {
        this.context = context;
        this.filters = filters;
        this.solver = solver;
        this.placed = placed;
    }

    @Override
    public LogicalPlan apply(LogicalPlan plan) throws SqlException {
        orderJoins(plan, context.getExecutionContext().isTimestampRequired());
        return plan;
    }

    @Override
    public void clear() {
        solver.clear();
        readJoin = null;
    }

    @Override
    public String getName() {
        return "join order";
    }

    /**
     * True when the plan sees the order of its input at {@code inputIndex}, an index of a join's
     * {@link LogicalPlans#orderedSteps}, given whether its own consumer sees the plan's order: {@code isOrderVisible}.
     * A sort orders the rows itself, and an aggregate groups them, so neither sees it unless the aggregate's values
     * depend on the order or an aggregate function reads the designated timestamp. A filter, a projection, a LIMIT
     * and a DISTINCT pass the order on, unless they compute something that depends on row order or the projection
     * declares a designated timestamp. A set operation passes it on from its left input and from each branch of a
     * UNION, while EXCEPT and INTERSECT read their right input as a set. A join passes it on to its first input,
     * shows it to the inputs whose timestamp a temporal step reads, and reads its other inputs as the rows it matches.
     * Any other node sees it.
     */
    private static boolean isInputOrderVisible(LogicalPlan plan, int inputIndex, boolean isOrderVisible) {
        return switch (plan) {
            case SortPlan _ -> false;
            case AggregatePlan aggregate -> LogicalPlans.hasTimestampAggregate(aggregate.getAggregates())
                    || !LogicalPlans.isAggregateOrderIndependent(aggregate);
            case FilterPlan filter -> isOrderVisible || !LogicalPlans.isOrderIndependent(filter.getPredicate());
            case ProjectPlan project -> isOrderVisible || project.hasTimestampDeclaration() || !isOrderIndependent(project.getExpressions());
            case LimitPlan _, DistinctPlan _ -> isOrderVisible;
            case SetOperationPlan operation -> isOrderVisible && (inputIndex == 0 || operation.getOperation().isUnion());
            case JoinPlan join -> inputIndex == 0 && isOrderVisible || LogicalPlans.consumesInputTimestamp(join, inputIndex);
            default -> true;
        };
    }

    private static boolean isOrderIndependent(ObjList<BoundExpression> expressions) {
        for (int i = 0, n = expressions.size(); i < n; i++) {
            if (!LogicalPlans.isOrderIndependent(expressions.getQuick(i))) {
                return false;
            }
        }
        return true;
    }

    /**
     * Whether an inner ON conjunct stated at {@code originPosition} filters the step at {@code target} instead: the
     * target precedes the origin, the origin step is INNER or CROSS, the target is an ASOF, LT or SPLICE step, and no
     * step between them nulls master rows.
     */
    private static boolean isTemporalAnchor(ObjList<JoinInput> ordered, int target, int originPosition) {
        if (target >= originPosition || ordered.getQuick(originPosition).getJoinType().isBarrier()) {
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

    /**
     * True when the plan, one that can pass its input's order on, designates the designated timestamp of its input at
     * {@code inputIndex} as its own: a filter, a LIMIT and a DISTINCT keep their input's designation, a projection
     * keeps it when its designated column reads it, an EXCEPT or INTERSECT designates the one of its left input and a
     * join the one of its first input.
     */
    private static boolean isTimestampForwarded(LogicalPlan plan, int inputIndex, LogicalPlan input) {
        final OutputSchema output = plan.getOutput();
        final OutputSchema source = input.getOutput();
        if (output.getTimestampIndex() < 0 || source.getTimestampIndex() < 0) {
            return false;
        }
        return switch (plan) {
            case FilterPlan _, LimitPlan _, DistinctPlan _ -> true;
            case ProjectPlan project -> project.getExpressions().getQuick(output.getTimestampIndex()) instanceof ColumnExpression column
                    && column.getColumnId() == source.getTimestampColumnId();
            case SetOperationPlan _, JoinPlan _ -> inputIndex == 0;
            default -> false;
        };
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

    private int lastOrderedInput(BoundExpression expression, JoinPlan join) {
        readJoin = join;
        lastPosition = -1;
        expression.walk(orderedInputReads);
        readJoin = null;
        return lastPosition;
    }

    /**
     * Orders the joins under the input at {@code inputIndex} of the plan, whose consumer sees the plan's order when
     * {@code isOrderVisible}. Once the input designates no timestamp, the plan stops designating the one it forwarded.
     */
    private void orderInputJoins(LogicalPlan plan, int inputIndex, LogicalPlan input, boolean isOrderVisible) throws SqlException {
        final boolean isForwarded = isTimestampForwarded(plan, inputIndex, input);
        orderJoins(input, isInputOrderVisible(plan, inputIndex, isOrderVisible));
        if (isForwarded && input.getOutput().getTimestampIndex() < 0) {
            plan.getOutput().setTimestampIndex(-1);
        }
    }

    /**
     * Orders every join under and at the plan, whose consumer sees its order when {@code isOrderVisible}.
     */
    private void orderJoins(LogicalPlan plan, boolean isOrderVisible) throws SqlException {
        if (plan instanceof JoinPlan join) {
            final ObjList<JoinInput> steps = LogicalPlans.orderedSteps(join);
            for (int i = 0, n = steps.size(); i < n; i++) {
                final LogicalPlan input = steps.getQuick(i).getInput();
                if (input != null) {
                    orderInputJoins(join, i, input, isOrderVisible);
                }
            }
            if (join.getGraph() != null) {
                solver.order(join, isOrderVisible);
                place(join, join.getGraph());
                join.setGraph(null);
            }
            return;
        }
        for (int i = 0, n = plan.inputCount(); i < n; i++) {
            orderInputJoins(plan, i, plan.inputAt(i), isOrderVisible);
        }
    }

    private void place(JoinPlan join, JoinGraph graph) throws SqlException {
        final ObjList<JoinInput> ordered = join.getOrderedInputs();
        final int last = ordered.size() - 1;
        boolean hasBarriers = false;
        for (int i = 1; i <= last; i++) {
            hasBarriers |= ordered.getQuick(i).getJoinType().isBarrier();
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
}
