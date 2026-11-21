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

import io.questdb.griffin.SqlException;

/**
 * A pooled logical operation, borrowed until its compiler is reset.
 * Clearing a node releases its own references, never its inputs or expressions.
 */
public abstract sealed class LogicalPlan implements PlanFactSource
        permits UnaryPlan, DomainReadPlan, FunctionSourcePlan, HorizonJoinPlan, JoinPlan, ScanPlan, SetOperationPlan, WindowJoinPlan {
    private final OutputSchema output = new OutputSchema();
    private int position = -1;
    private PlanStage stage;

    @Override
    public void clear() {
        output.clear();
        position = -1;
        stage = null;
    }

    public OutputSchema getOutput() {
        return output;
    }

    public int getPosition() {
        return position;
    }

    /**
     * The stage of the plan the node belongs to, which the optimiser records on every node once it verified the plan at
     * that stage, under {@link io.questdb.ParanoiaState#PLAN_PARANOIA_MODE}; null for a node it has not.
     */
    public PlanStage getStage() {
        return stage;
    }

    public abstract LogicalPlan inputAt(int index);

    public abstract int inputCount();

    public abstract void replaceInput(int index, LogicalPlan input);

    /**
     * Rewrites the inputs of the plan, depth first, replacing each input the rewriter replaces, and then the plan;
     * returns the node that replaces the plan.
     */
    public final LogicalPlan rewriteBottomUp(PlanRewriter rewriter) throws SqlException {
        for (int i = 0, n = inputCount(); i < n; i++) {
            final LogicalPlan input = inputAt(i);
            if (input != null) {
                final LogicalPlan rewritten = input.rewriteBottomUp(rewriter);
                if (rewritten != input) {
                    replaceInput(i, rewritten);
                }
            }
        }
        return rewriter.rewrite(this);
    }

    public void setPosition(int position) {
        this.position = position;
    }

    public void setStage(PlanStage stage) {
        this.stage = stage;
    }

    @Override
    public void visitFacts(PlanFactRecorder visitor) {
        visitor.output("output", output);
        visitor.fact("position", position);
    }

    @Override
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

    /**
     * Fails, under paranoia, a read of the physical fact {@code fact} of a node at a stage before physical planning
     * decides it, see {@link #getStage} and {@link PlanStage#isRewritten}.
     */
    protected final void checkPhysicalRead(String fact) {
        PlanFacts.checkDecided(stage == null || stage.isRewritten(), fact);
    }

    /**
     * Fails, under paranoia, the rewrite {@code rewrite} of the inputs, the expressions or the order facts of a node at
     * a stage that fixes them, see {@link #getStage} and {@link PlanStage#isRewritten}.
     */
    protected final void checkRewritable(String rewrite) {
        PlanFacts.checkRewritable(stage, rewrite);
    }
}
