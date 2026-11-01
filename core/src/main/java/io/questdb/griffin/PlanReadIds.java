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
import io.questdb.griffin.plan.logical.ExpressionVisitor;
import io.questdb.griffin.plan.logical.LogicalPlan;
import io.questdb.griffin.plan.logical.OuterColumnExpression;
import io.questdb.griffin.plan.logical.PlanExpressionVisitor;
import io.questdb.griffin.plan.logical.PlanVisitor;
import io.questdb.griffin.plan.logical.TreeWalk;
import io.questdb.std.IntList;

/**
 * Collects the ids of the columns plan nodes read: the outer columns, those of an enclosing LATERAL's outer input, or
 * every column, outer columns included.
 */
public final class PlanReadIds implements ExpressionVisitor, PlanExpressionVisitor {
    private final PlanVisitor readingNodes = this::findReadingNode;
    private IntList sink;
    private final ExpressionVisitor expressionReadIds = expression -> {
        if (expression instanceof ColumnExpression column) {
            sink.add(column.getColumnId());
        } else if (expression instanceof OuterColumnExpression outer) {
            sink.add(outer.getColumnId());
        }
        return TreeWalk.CONTINUE;
    };
    private final PlanExpressionVisitor nodeReadIds = new PlanExpressionVisitor() {
        @Override
        public int visitColumnId(int columnId, int position) {
            sink.add(columnId);
            return columnId;
        }

        @Override
        public BoundExpression visitExpression(BoundExpression expression) {
            expression.walk(expressionReadIds);
            return expression;
        }
    };
    private final PlanVisitor subtreeReadIds = plan -> {
        plan.visitReads(nodeReadIds);
        return TreeWalk.CONTINUE;
    };

    /**
     * Adds the ids of the outer columns the expression reads, with repeats.
     */
    public void collectOuterIds(BoundExpression expression, IntList sink) {
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
    public void collectOuterIds(LogicalPlan plan, IntList sink) {
        this.sink = sink;
        try {
            plan.visitReads(this);
        } finally {
            this.sink = null;
        }
    }

    /**
     * Adds the ids of the columns and outer columns the node reads, with repeats; its inputs are not visited.
     */
    public void collectReadIds(LogicalPlan plan, IntList sink) {
        this.sink = sink;
        try {
            plan.visitReads(nodeReadIds);
        } finally {
            this.sink = null;
        }
    }

    /**
     * Adds the ids of the columns and outer columns the plan and each of its inputs read, with repeats.
     */
    public void collectSubtreeReadIds(LogicalPlan plan, IntList sink) {
        this.sink = sink;
        try {
            plan.walkTopDown(subtreeReadIds);
        } finally {
            this.sink = null;
        }
    }

    /**
     * True when the plan or one of its inputs reads an outer column; {@code tmpColumnIds} is restored on return.
     */
    public boolean isOuterReadBy(LogicalPlan plan, IntList tmpColumnIds) {
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
