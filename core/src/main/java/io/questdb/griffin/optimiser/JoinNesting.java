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
import io.questdb.griffin.plan.logical.BoundExpression;
import io.questdb.griffin.plan.logical.ColumnExpression;
import io.questdb.griffin.plan.logical.ExpressionVisitor;
import io.questdb.griffin.plan.logical.FilterPlan;
import io.questdb.griffin.plan.logical.JoinInput;
import io.questdb.griffin.plan.logical.JoinKind;
import io.questdb.griffin.plan.logical.JoinPlan;
import io.questdb.griffin.plan.logical.LogicalPlan;
import io.questdb.griffin.plan.logical.PlanExpressionVisitor;
import io.questdb.griffin.plan.logical.PlanRewriter;
import io.questdb.griffin.plan.logical.TreeWalk;
import io.questdb.std.ObjList;
import io.questdb.std.ObjectPool;

/**
 * When the leading cross input is fixed, nests each run that reads no input ordered before it as one input of the join, which
 * a CROSS step joins. A run starts at an INNER or CROSS step that reads no earlier input and takes each following INNER
 * or CROSS step that reads an input of the run and none before it; every step of a run is
 * {@link LogicalPlans#isStableJoinStep stable}. In the left-deep order every later step of the run joins again for each
 * row of the inputs before the run; nested, the run is the one slave of a nested loop, which operator planning lets
 * copy its rows once. Runs after filter pushdown, which derives and pushes the join's conjuncts along the left-deep
 * order.
 */
final class JoinNesting implements OptimiserPass {
    private final OptimiserContext context;
    private final ObjectPool<FilterPlan> filters;
    private final ObjectPool<JoinInput> joinInputs;
    private final ObjectPool<JoinPlan> joins;
    private boolean isParallelGroupByEnabled;
    private int maxReadPosition;
    private int minReadPosition;
    private JoinPlan readJoin;
    private int readStepPosition;
    private final ExpressionVisitor expressionReads = expression -> {
        if (expression instanceof ColumnExpression column) {
            readColumn(column.getColumnId());
        }
        return TreeWalk.CONTINUE;
    };
    private final PlanExpressionVisitor stepReads = new PlanExpressionVisitor() {
        @Override
        public int visitColumnId(int columnId, int position) {
            readColumn(columnId);
            return columnId;
        }

        @Override
        public BoundExpression visitExpression(BoundExpression expression) {
            expression.walk(expressionReads);
            return expression;
        }
    };
    private final PlanRewriter runs = this::nestRuns;

    JoinNesting(OptimiserContext context, ObjectPool<JoinPlan> joins, ObjectPool<JoinInput> joinInputs, ObjectPool<FilterPlan> filters) {
        this.context = context;
        this.joins = joins;
        this.joinInputs = joinInputs;
        this.filters = filters;
    }

    @Override
    public LogicalPlan apply(LogicalPlan plan) throws SqlException {
        isParallelGroupByEnabled = context.getExecutionContext().isParallelGroupByEnabled();
        return plan.rewriteBottomUp(runs);
    }

    @Override
    public void clear() {
        readJoin = null;
    }

    @Override
    public String getName() {
        return "join nesting";
    }

    private boolean isRunStep(JoinInput step) {
        return !step.getJoinType().isBarrier() && LogicalPlans.isStableJoinStep(step, isParallelGroupByEnabled, null);
    }

    /**
     * Replaces the ordered steps from {@code start} to {@code end} with a CROSS step of a join of their inputs, which
     * the join keeps in occurrence order where the first of them stood. The first step's conditions, which read its
     * input alone, filter that input.
     */
    private void nest(JoinPlan join, int start, int end) throws SqlException {
        final ObjList<JoinInput> ordered = join.getOrderedInputs();
        final ObjList<JoinInput> inputs = join.getInputs();
        final JoinInput first = ordered.getQuick(start);
        final BoundExpression predicate = context.getRewriter().combineConjunction(first.getOnResidual(), first.getPostJoinFilter(),
                first.getPosition());
        if (predicate != null) {
            first.setInput(filters.next().of(first.getInput(), predicate, predicate.getPosition()));
            first.setOnResidual(null);
            first.setPostJoinFilter(null);
        }
        final JoinPlan nested = joins.next().of(first.getPosition());
        for (int i = start; i <= end; i++) {
            final JoinInput step = ordered.getQuick(i);
            nested.getOrderedInputs().add(step);
            nested.getOutput().addColumnsFrom(step.getSourceOutput(), step.getBindingAlias());
        }
        int occurrence = -1;
        for (int i = 0, n = inputs.size(); i < n; i++) {
            final JoinInput input = inputs.getQuick(i);
            if (nested.getOrderedInputs().indexOf(input) > -1) {
                nested.getInputs().add(input);
                if (occurrence < 0) {
                    occurrence = i;
                }
            }
        }
        join.nestSteps(start, end, nested, joinInputs.next().of(nested, JoinKind.CROSS, null, first.getPosition()), occurrence);
        nested.getOutput().setTimestampColumnId(LogicalPlans.joinTimestampColumnId(nested, end - start + 1));
    }

    private LogicalPlan nestRuns(LogicalPlan plan) throws SqlException {
        if (!(plan instanceof JoinPlan join) || !join.isLeadingCrossJoinFixed()) {
            return plan;
        }
        final ObjList<JoinInput> ordered = join.getOrderedInputs();
        for (int start = 1; start < ordered.size() - 1; start++) {
            if (!isRunStep(ordered.getQuick(start))) {
                continue;
            }
            readStep(join, start);
            if (maxReadPosition >= 0) {
                continue;
            }
            int end = start;
            while (end + 1 < ordered.size() && isRunStep(ordered.getQuick(end + 1))) {
                readStep(join, end + 1);
                if (minReadPosition < start || maxReadPosition < start) {
                    break;
                }
                end++;
            }
            if (end > start) {
                nest(join, start, end);
            }
        }
        return join;
    }

    private void readColumn(int columnId) {
        final int source = LogicalPlans.findJoinColumnSource(readJoin, columnId);
        if (source >= 0) {
            final int position = readJoin.getOrderedInputs().indexOf(readJoin.getInputs().getQuick(source));
            if (position < readStepPosition) {
                minReadPosition = Math.min(minReadPosition, position);
                maxReadPosition = Math.max(maxReadPosition, position);
            }
        }
    }

    /**
     * Collects the lowest and the highest ordered position of the inputs before the step at {@code position} that the
     * step reads, -1 for the highest when it reads none.
     */
    private void readStep(JoinPlan join, int position) {
        readJoin = join;
        readStepPosition = position;
        minReadPosition = Integer.MAX_VALUE;
        maxReadPosition = -1;
        join.getOrderedInputs().getQuick(position).visitReads(stepReads);
        readJoin = null;
    }
}
