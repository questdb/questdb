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
import io.questdb.griffin.plan.logical.CursorExpression;
import io.questdb.griffin.plan.logical.FunctionExpression;
import io.questdb.griffin.plan.logical.LogicalPlan;
import io.questdb.griffin.plan.logical.PlanExpressionVisitor;
import io.questdb.griffin.plan.logical.PlanVisitor;
import io.questdb.griffin.plan.logical.TreeWalk;

/**
 * Rejects, for a context that refuses non-deterministic functions, the non-deterministic calls an optimised plan keeps
 * and the sub-queries that read a data source outside the database. It searches the plan's inputs, its expressions and the
 * sub-queries those hold in the order generation creates their functions, a call's arguments last to first before
 * the call, so it reports the call generation would create first.
 */
public final class NonDeterministicCalls implements PlanExpressionVisitor, PlanVisitor {
    private FunctionExpression offender;

    public void reject(LogicalPlan plan, SqlExecutionContext executionContext) throws SqlException {
        if (executionContext.allowNonDeterministicFunctions()) {
            return;
        }
        try {
            plan.walkBottomUp(this);
            if (offender != null) {
                throw SqlException.nonDeterministicColumn(offender.getPosition(), offender.getName(), objectKind(executionContext));
            }
        } finally {
            offender = null;
        }
    }

    /**
     * Rejects the first non-deterministic function the resolver created for a statement whose binding failed:
     * generation creates a statement's functions before it validates what consumes them, so that call takes
     * precedence over the binding error.
     */
    public void rejectCreated(FunctionResolver resolver, SqlExecutionContext executionContext) throws SqlException {
        final int position = resolver.getNonDeterministicCallPosition();
        if (position > -1) {
            throw SqlException.nonDeterministicColumn(position, resolver.getNonDeterministicCallName(), objectKind(executionContext));
        }
    }

    /**
     * Rejects the sub-query at {@code position} when its plan reads a data source outside the database.
     */
    public void rejectExternalSource(LogicalPlan plan, int position, SqlExecutionContext executionContext) throws SqlException {
        if (!executionContext.allowNonDeterministicFunctions() && LogicalPlans.hasExternalDataSource(plan)) {
            throw SqlException.nonDeterministicColumn(position, "sub-query", objectKind(executionContext));
        }
    }

    @Override
    public int visit(LogicalPlan plan) {
        plan.visitReads(this);
        return offender == null ? TreeWalk.CONTINUE : TreeWalk.STOP;
    }

    @Override
    public int visitColumnId(int columnId, int position) {
        return columnId;
    }

    @Override
    public BoundExpression visitExpression(BoundExpression expression) {
        if (offender == null) {
            findOffender(expression);
        }
        return expression;
    }

    private static CharSequence objectKind(SqlExecutionContext executionContext) {
        return executionContext.isLiveViewCompile() ? "live view" : "materialized view";
    }

    private boolean findOffender(BoundExpression expression) {
        if (expression instanceof CursorExpression cursor) {
            cursor.getPlan().walkBottomUp(this);
        } else if (expression instanceof FunctionExpression call) {
            for (int i = call.getArgumentCount() - 1; i > -1; i--) {
                final BoundExpression argument = call.argumentAt(i);
                if (argument != null && findOffender(argument)) {
                    return true;
                }
            }
            if ((call.getFunctionFlags() & BoundExpression.NON_DETERMINISTIC) != 0) {
                offender = call;
            }
        }
        return offender != null;
    }
}
