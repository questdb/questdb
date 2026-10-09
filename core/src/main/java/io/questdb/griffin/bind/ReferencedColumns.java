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

package io.questdb.griffin.bind;

import io.questdb.griffin.plan.logical.BoundExpression;
import io.questdb.griffin.plan.logical.ColumnExpression;
import io.questdb.griffin.plan.logical.ExpressionVisitor;
import io.questdb.griffin.plan.logical.LogicalPlan;
import io.questdb.griffin.plan.logical.OuterColumnExpression;
import io.questdb.griffin.plan.logical.OutputSchema;
import io.questdb.griffin.plan.logical.PlanExpressionVisitor;
import io.questdb.griffin.plan.logical.PlanVisitor;
import io.questdb.griffin.plan.logical.ProjectPlan;
import io.questdb.griffin.plan.logical.ScanPlan;
import io.questdb.griffin.plan.logical.SetOperationPlan;
import io.questdb.griffin.plan.logical.TreeWalk;
import io.questdb.std.IntIntHashMap;
import io.questdb.std.Mutable;

/**
 * Records on the scans of a bound query level the columns its text references, with the text position of the first
 * reference, which selecting from a table requires the permission on whatever the optimiser later prunes. Every
 * expression and column id a node reads references its columns, except in an implied projection, whose column is
 * referenced when a reference of its output column is; a set operation passes the references of its output columns
 * on to its inputs. A sub-query binds and marks its own scans, so its plan is not visited, and a scan that reads a
 * table through a view records none: the view's own text references its columns, and the view's permission covers
 * them.
 */
final class ReferencedColumns implements ExpressionVisitor, PlanExpressionVisitor, Mutable {
    private final PlanVisitor passedReads = this::passReferences;
    private final IntIntHashMap positions = new IntIntHashMap();
    private final PlanVisitor scans = this::markScan;
    private final PlanVisitor statedReads = this::collect;

    @Override
    public void clear() {
        positions.clear();
    }

    @Override
    public int visit(BoundExpression expression) {
        switch (expression) {
            case ColumnExpression column -> reference(column.getColumnId(), column.getPosition());
            case OuterColumnExpression outer -> reference(outer.getColumnId(), outer.getPosition());
            default -> {
            }
        }
        return TreeWalk.CONTINUE;
    }

    @Override
    public int visitColumnId(int columnId, int position) {
        reference(columnId, position < 0 ? Integer.MAX_VALUE : position);
        return columnId;
    }

    @Override
    public BoundExpression visitExpression(BoundExpression expression) {
        expression.walk(this);
        return expression;
    }

    private int collect(LogicalPlan plan) {
        if (!(plan instanceof ProjectPlan project && project.isImplied())) {
            plan.visitReads(this);
        }
        return TreeWalk.CONTINUE;
    }

    private int markScan(LogicalPlan plan) {
        if (plan instanceof ScanPlan scan && scan.getViewName() == null) {
            scan.markReferencedColumns(positions);
        }
        return TreeWalk.CONTINUE;
    }

    private int passReferences(LogicalPlan plan) {
        switch (plan) {
            case ProjectPlan project when project.isImplied() -> {
                final OutputSchema output = project.getOutput();
                for (int i = 0, n = output.getColumnCount(); i < n; i++) {
                    final int index = positions.keyIndex(output.getColumnId(i));
                    if (index < 0 && project.getExpressions().getQuick(i) instanceof ColumnExpression column) {
                        reference(column.getColumnId(), positions.valueAt(index));
                    }
                }
            }
            case SetOperationPlan operation -> {
                final OutputSchema output = operation.getOutput();
                for (int i = 0, n = output.getColumnCount(); i < n; i++) {
                    final int index = positions.keyIndex(output.getColumnId(i));
                    if (index < 0) {
                        final int position = positions.valueAt(index);
                        for (int k = 0, count = operation.inputCount(); k < count; k++) {
                            reference(operation.inputAt(k).getOutput().getColumnId(i), position);
                        }
                    }
                }
            }
            default -> {
            }
        }
        return TreeWalk.CONTINUE;
    }

    private void reference(int columnId, int position) {
        final int index = positions.keyIndex(columnId);
        if (index > -1 || position < positions.valueAt(index)) {
            positions.putAt(index, columnId, position);
        }
    }

    /**
     * Records the references of a bound query level on its scans, before any pass prunes their columns.
     */
    void mark(LogicalPlan plan) {
        positions.clear();
        plan.walkTopDown(statedReads);
        plan.walkTopDown(passedReads);
        plan.walkTopDown(scans);
    }
}
