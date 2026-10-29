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
import io.questdb.griffin.plan.logical.LogicalPlan;
import io.questdb.griffin.plan.logical.PlanExpressionVisitor;
import io.questdb.griffin.plan.logical.PlanVisitor;
import io.questdb.griffin.plan.logical.TreeWalk;

/**
 * Finds whether plan nodes read a sub-query.
 */
public final class SubqueryReads implements PlanExpressionVisitor, PlanVisitor {
    private boolean isRead;

    /**
     * True when the plan or one of its inputs evaluates a sub-query.
     */
    public boolean isReadBy(LogicalPlan plan) {
        try {
            return !plan.walkTopDown(this);
        } finally {
            isRead = false;
        }
    }

    @Override
    public int visit(LogicalPlan plan) {
        plan.visitReads(this);
        return isRead ? TreeWalk.STOP : TreeWalk.CONTINUE;
    }

    @Override
    public int visitColumnId(int columnId, int position) {
        return columnId;
    }

    @Override
    public BoundExpression visitExpression(BoundExpression expression) {
        isRead = isRead || LogicalPlans.hasSubquery(expression);
        return expression;
    }
}
