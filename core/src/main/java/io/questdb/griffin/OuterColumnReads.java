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
import io.questdb.griffin.plan.logical.ExpressionVisitor;
import io.questdb.griffin.plan.logical.LogicalPlan;
import io.questdb.griffin.plan.logical.OuterColumnExpression;
import io.questdb.griffin.plan.logical.PlanExpressionVisitor;
import io.questdb.griffin.plan.logical.PlanVisitor;
import io.questdb.griffin.plan.logical.TreeWalk;
import io.questdb.std.IntList;

/**
 * Finds the outer columns, those of an enclosing LATERAL's outer input, that plan nodes read.
 */
public final class OuterColumnReads implements ExpressionVisitor, PlanExpressionVisitor {
    private final PlanVisitor readingNodes = this::findReadingNode;
    private IntList sink;

    /**
     * Adds the ids of the outer columns the expression reads, with repeats.
     */
    public void collect(BoundExpression expression, IntList sink) {
        this.sink = sink;
        try {
            expression.walk(this);
        } finally {
            this.sink = null;
        }
    }

    /**
     * Adds the ids of the outer columns the node reads, with repeats; its inputs are not visited.
     */
    public void collect(LogicalPlan plan, IntList sink) {
        this.sink = sink;
        try {
            plan.visitReads(this);
        } finally {
            this.sink = null;
        }
    }

    /**
     * True when the plan or one of its inputs reads an outer column; {@code tmpColumnIds} is restored on return.
     */
    public boolean isReadBy(LogicalPlan plan, IntList tmpColumnIds) {
        final int base = tmpColumnIds.size();
        sink = tmpColumnIds;
        try {
            return !plan.walkTopDown(readingNodes);
        } finally {
            sink = null;
            tmpColumnIds.setPos(base);
        }
    }

    @Override
    public int visit(BoundExpression expression) {
        if (expression instanceof OuterColumnExpression outer) {
            sink.add(outer.getColumnId());
        }
        return TreeWalk.CONTINUE;
    }

    @Override
    public int visitColumnId(int columnId, int position) {
        return columnId;
    }

    @Override
    public BoundExpression visitExpression(BoundExpression expression) {
        expression.walk(this);
        return expression;
    }

    private int findReadingNode(LogicalPlan plan) {
        final int base = sink.size();
        plan.visitReads(this);
        return sink.size() > base ? TreeWalk.STOP : TreeWalk.CONTINUE;
    }
}
