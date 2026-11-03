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

/**
 * The stages a logical plan passes through, in order. Each stage states the invariants every node of the plan holds and
 * the facts it carries; {@code SqlOptimiser} advances a plan from one stage to the next by its passes, each of which
 * requires a stage and leaves that stage or the next, and under
 * {@link io.questdb.ParanoiaState#PLAN_PARANOIA_MODE} it verifies the invariants of the stage a pass leaves and records
 * the stage on every node, see {@link LogicalPlan#getStage}.
 */
public enum PlanStage {
    /**
     * The binder's output. A join may hold an unordered join graph, and a dependent join step reads the inputs before
     * it through outer columns.
     */
    BOUND,
    /**
     * Join ordering has ordered every join and placed the conjuncts of its graph; a dependent join step still reads the
     * inputs before it through outer columns.
     */
    JOINS_ORDERED,
    /**
     * Decorrelation has rewritten every dependent join step into an ordinary one; no node reads an outer column. The
     * rewrite passes keep the plan at this stage.
     */
    DECORRELATED,
    /**
     * Every rewrite is done and order planning has recorded the order each consumer requires: the structure of the
     * plan, its expressions and its order facts are final. Physical planning reads them and records its decisions.
     */
    REWRITTEN,
    /**
     * Physical planning has decided the access path of every scan and the operator of every node the generator builds,
     * which the generator reads.
     */
    PHYSICAL;

    /**
     * Whether every rewrite is done: from {@link #REWRITTEN} on, physical planning reads the structure, the expressions
     * and the order facts of the plan, which therefore stay unchanged, and records its physical facts, which may be read.
     */
    public boolean isRewritten() {
        return this == REWRITTEN || this == PHYSICAL;
    }
}
