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

import io.questdb.cairo.ColumnType;
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
import io.questdb.griffin.plan.logical.JoinKey;
import io.questdb.griffin.plan.logical.JoinKind;
import io.questdb.griffin.plan.logical.JoinPlan;
import io.questdb.griffin.plan.logical.LimitPlan;
import io.questdb.griffin.plan.logical.LogicalPlan;
import io.questdb.griffin.plan.logical.OutputSchema;
import io.questdb.griffin.plan.logical.ProjectPlan;
import io.questdb.griffin.plan.logical.SetOperationPlan;
import io.questdb.griffin.plan.logical.SortPlan;
import io.questdb.griffin.plan.logical.TreeWalk;
import io.questdb.griffin.plan.logical.WindowPlan;
import io.questdb.std.IntHashSet;
import io.questdb.std.IntIntHashMap;
import io.questdb.std.ObjList;
import io.questdb.std.ObjectPool;

/**
 * Orders every join binding left a {@link JoinGraph} for and places the graph's filter conjuncts on the ordered steps.
 * Inner and cross joins commute, but a join that designates its first input's timestamp keeps that input first while
 * any part of the query requires the designated timestamp of the join's rows, and so their order: a statement that
 * stores the designated timestamp, or a node that reads it or depends on its order along a path of nodes that pass the
 * requirement on, see {@link #isInputTimestampRequired}. A node the rule does not name, including any that a later
 * change adds, requires it. A join that leads with another input designates no timestamp, and neither does a node on
 * the path that forwarded the join's.
 * A conjunct filters the step that joins the last input it reads, a conjunct that reads no input the last step. When
 * the join has an outer or temporal step, an ON conjunct filters no earlier than its own step unless it reads one input
 * and an ASOF, LT or SPLICE step anchors it, and a WHERE conjunct filters no earlier than the last RIGHT or FULL step
 * after it. A conjunct of the first step filters its input; the constant conjuncts filter the last step.
 */
final class JoinOrdering implements OptimiserPass {
    private final ColumnReads columnReads;
    private final OptimiserContext context;
    private final ObjectPool<FilterPlan> filters;
    private final ObjList<BoundExpression> placed;
    private final IntHashSet readColumnIds = new IntHashSet();
    private final JoinOrderSolver solver;
    private final IntIntHashMap slaveKeyByMaster = new IntIntHashMap();
    private int lastPosition;
    private JoinPlan readJoin;
    private final ExpressionVisitor orderedInputReads = expression -> {
        if (expression instanceof ColumnExpression column) {
            final JoinPlan join = this.readJoin;
            final int source = LogicalPlans.joinColumnSource(join, column.getColumnId());
            this.lastPosition = Math.max(this.lastPosition, join.getOrderedInputs().indexOf(join.getInputs().getQuick(source)));
        }
        return TreeWalk.CONTINUE;
    };

    JoinOrdering(
            OptimiserContext context,
            ColumnReads columnReads,
            ObjectPool<FilterPlan> filters,
            JoinOrderSolver solver,
            ObjList<BoundExpression> placed
    ) {
        this.context = context;
        this.columnReads = columnReads;
        this.filters = filters;
        this.solver = solver;
        this.placed = placed;
    }

    @Override
    public LogicalPlan apply(LogicalPlan plan) throws SqlException {
        columnReads.of(plan, readColumnIds);
        orderJoins(plan, context.getExecutionContext().isTimestampRequired());
        return plan;
    }

    @Override
    public void clear() {
        solver.clear();
        columnReads.clear();
        readColumnIds.clear();
        slaveKeyByMaster.clear();
        readJoin = null;
    }

    @Override
    public String getName() {
        return "join order";
    }

    /**
     * Whether an inner ON conjunct stated at {@code originPosition} filters the step at {@code target} instead: the
     * target precedes the origin, the origin step is INNER or CROSS, the target is an ASOF, LT or SPLICE step, and no
     * step between them nulls master rows.
     */
    private static boolean isTemporalAnchor(ObjList<JoinInput> ordered, int target, int originPosition) {
        if (target >= originPosition || ordered.getQuick(originPosition).getJoinType().isBarrier()
                || !ordered.getQuick(target).getJoinType().isTemporal()) {
            return false;
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
            case ProjectPlan project ->
                    project.getExpressions().getQuick(output.getTimestampIndex()) instanceof ColumnExpression column
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

    private boolean hasReadTimestampAggregate(AggregatePlan aggregate) {
        final int keyCount = aggregate.getGroupingExpressions().size();
        for (int i = 0, n = aggregate.getAggregates().size(); i < n; i++) {
            if (columnReads.isRead(aggregate.getOutput().getColumnId(keyCount + i))
                    && LogicalPlans.isTimestampAggregate(aggregate.getAggregates().getQuick(i))) {
                return true;
            }
        }
        return false;
    }

    private boolean hasReadTimestampWindow(WindowPlan window) {
        for (int i = 0, n = window.getFunctionColumnIds().size(); i < n; i++) {
            if (columnReads.isRead(window.getFunctionColumnIds().getQuick(i))
                    && ((window.getFunctions().getQuick(i).getFunctionFlags() & BoundExpression.DESIGNATED_TIMESTAMP) != 0
                    || window.getSpecs().getQuick(i).isSubsampleKeepFlag())) {
                return true;
            }
        }
        return false;
    }

    /**
     * True when the plan requires the designated timestamp of its input at {@code inputIndex}, an index of a join's
     * {@link LogicalPlans#orderedSteps}, given whether its own consumer requires the plan's: {@code isTimestampRequired}.
     * A node requires it only through the columns the query reads from it, see {@link ColumnReads}: column pruning
     * removes the computations of every other column. A sort orders the rows itself, so it requires nothing of its
     * input, and an aggregate requires it only for a read aggregate function that reads the designated timestamp or
     * requires its ascending order. A filter, a LIMIT, a DISTINCT, a projection and a window pass the requirement on;
     * a projection that declares a designated timestamp requires it, and so does a window with a read function whose
     * factory relies on the designated timestamp or that keeps SUBSAMPLE rows. A set operation passes it on to its left
     * input and to each branch of a UNION, while EXCEPT and INTERSECT read their right input as a set. A join passes it
     * on to its first input and requires it of the inputs whose timestamp a temporal step reads. Any other node requires
     * it. Every other dependence on row order, such as an order-sensitive aggregate or a window over its input order,
     * reads the rows in the order the join chooses.
     */
    private boolean isInputTimestampRequired(LogicalPlan plan, int inputIndex, boolean isTimestampRequired) {
        return switch (plan) {
            case SortPlan _ -> false;
            case AggregatePlan aggregate -> hasReadTimestampAggregate(aggregate);
            case FilterPlan _, LimitPlan _, DistinctPlan _ -> isTimestampRequired;
            case ProjectPlan project -> isTimestampRequired || project.hasTimestampDeclaration();
            case WindowPlan window -> isTimestampRequired || hasReadTimestampWindow(window);
            case SetOperationPlan operation ->
                    isTimestampRequired && (inputIndex == 0 || operation.getOperation().isUnion());
            case JoinPlan join ->
                    inputIndex == 0 && isTimestampRequired || LogicalPlans.consumesInputTimestamp(join, inputIndex);
            default -> true;
        };
    }

    private int lastOrderedInput(BoundExpression expression, JoinPlan join) {
        readJoin = join;
        lastPosition = -1;
        expression.walk(orderedInputReads);
        readJoin = null;
        return lastPosition;
    }

    /**
     * Orders the joins under the input at {@code inputIndex} of the plan, whose consumer requires the plan's designated
     * timestamp when {@code isTimestampRequired}. Once the input designates no timestamp, the plan stops designating the
     * one it forwarded.
     */
    private void orderInputJoins(LogicalPlan plan, int inputIndex, LogicalPlan input, boolean isTimestampRequired) throws SqlException {
        final boolean isForwarded = isTimestampForwarded(plan, inputIndex, input);
        orderJoins(input, isInputTimestampRequired(plan, inputIndex, isTimestampRequired));
        if (isForwarded && input.getOutput().getTimestampIndex() < 0) {
            plan.getOutput().setTimestampIndex(-1);
        }
    }

    /**
     * Orders every join under and at the plan, whose consumer requires its designated timestamp when
     * {@code isTimestampRequired}.
     */
    private void orderJoins(LogicalPlan plan, boolean isTimestampRequired) throws SqlException {
        columnReads.retainOutputs(plan);
        columnReads.collectInputReads(plan);
        if (plan instanceof JoinPlan join) {
            final ObjList<JoinInput> steps = LogicalPlans.orderedSteps(join);
            for (int i = 0, n = steps.size(); i < n; i++) {
                final LogicalPlan input = steps.getQuick(i).getInput();
                if (input != null) {
                    orderInputJoins(join, i, input, isTimestampRequired);
                }
            }
            if (join.getGraph() != null) {
                solver.order(join, isTimestampRequired, readColumnIds);
                place(join, join.getGraph());
                reduceOuterKeys(join);
                join.setGraph(null);
            }
            return;
        }
        for (int i = 0, n = plan.inputCount(); i < n; i++) {
            orderInputJoins(plan, i, plan.inputAt(i), isTimestampRequired);
        }
    }

    /** Moves repeated-master textual LEFT keys into equality filters on the nullable input. */
    private void reduceOuterKeys(JoinPlan join) throws SqlException {
        final ObjList<JoinInput> steps = join.getOrderedInputs();
        for (int i = 1; i < steps.size(); i++) {
            final JoinInput step = steps.getQuick(i);
            if (step.getJoinType() != JoinKind.LEFT_OUTER || step.getKeys().size() < 2) {
                continue;
            }
            slaveKeyByMaster.clear();
            final OutputSchema output = step.getSourceOutput();
            final ObjList<JoinKey> keys = step.getKeys();
            for (int k = 0; k < keys.size(); ) {
                final JoinKey key = keys.getQuick(k);
                final int masterId = key.getMasterColumnId();
                final int previousId = slaveKeyByMaster.get(masterId);
                final int slaveId = key.getSlaveColumnId();
                if (previousId < 0) {
                    slaveKeyByMaster.put(masterId, slaveId);
                    k++;
                    continue;
                }
                final int masterType = join.getOutput().getColumnType(join.getOutput().getColumnIndexById(masterId));
                final int previousType = output.getColumnType(output.getColumnIndexById(previousId));
                final int slaveType = output.getColumnType(output.getColumnIndexById(slaveId));
                if (!ColumnType.isSymbolOrStringOrVarchar(masterType) || !ColumnType.isSymbolOrStringOrVarchar(previousType)
                        || !ColumnType.isSymbolOrStringOrVarchar(slaveType)) {
                    k++;
                    continue;
                }
                final int position = key.getPosition();
                final BoundExpression equality = context.bindCall("=", position,
                        new ColumnExpression().of(previousId, previousType, position),
                        new ColumnExpression().of(slaveId, slaveType, position), output);
                step.setKeyFilter(context.getRewriter().combineConjunction(step.getKeyFilter(), equality, position));
                step.removeKey(k);
            }
        }
    }

    private void place(JoinPlan join, JoinGraph graph) throws SqlException {
        final ObjList<JoinInput> ordered = join.getOrderedInputs();
        final int last = ordered.size() - 1;
        final boolean hasBarriers = LogicalPlans.hasBarrierInput(join);
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
