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

package io.questdb.griffin.plan.logical;

import io.questdb.std.Mutable;

/**
 * A pooled logical operation, borrowed until its compiler is reset.
 * Clearing a node releases its own references, never its inputs or expressions.
 */
public abstract sealed class LogicalPlan implements Mutable
        permits UnaryPlan, FunctionSourcePlan, HorizonJoinPlan, JoinPlan, ScanPlan, SetOperationPlan, WindowJoinPlan {
    private final OutputSchema output = new OutputSchema();
    private int position = -1;

    @Override
    public void clear() {
        output.clear();
        position = -1;
    }

    public OutputSchema getOutput() {
        return output;
    }

    public int getPosition() {
        return position;
    }

    public abstract LogicalPlan inputAt(int index);

    public abstract int inputCount();

    public abstract void replaceInput(int index, LogicalPlan input);

    public void setPosition(int position) {
        this.position = position;
    }

    /**
     * Hands the visitor every expression and column id the node reads, not those of its inputs, and keeps what the
     * visitor returns.
     */
    public void visitReads(PlanExpressionVisitor visitor) {
    }

    /**
     * Visits the inputs of the plan, depth first, and then the plan, so a visitor may rewrite a node whose inputs it
     * has visited; returns false when the visitor stops the walk. A visitor's {@link TreeWalk#SKIP_CHILDREN} acts as
     * {@link TreeWalk#CONTINUE}: the children are visited first.
     */
    public final boolean walkBottomUp(PlanVisitor visitor) {
        for (int i = 0, n = inputCount(); i < n; i++) {
            final LogicalPlan input = inputAt(i);
            if (input != null && !input.walkBottomUp(visitor)) {
                return false;
            }
        }
        return visitor.visit(this) != TreeWalk.STOP;
    }

    /**
     * Visits the plan and then, unless the visitor skips them, its inputs, depth first; returns false when the
     * visitor stops the walk.
     */
    public final boolean walkTopDown(PlanVisitor visitor) {
        final int action = visitor.visit(this);
        if (action == TreeWalk.STOP) {
            return false;
        }
        if (action == TreeWalk.CONTINUE) {
            for (int i = 0, n = inputCount(); i < n; i++) {
                final LogicalPlan input = inputAt(i);
                if (input != null && !input.walkTopDown(visitor)) {
                    return false;
                }
            }
        }
        return true;
    }
}
