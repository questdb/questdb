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

import io.questdb.griffin.SqlException;
import io.questdb.griffin.plan.logical.LogicalPlan;
import io.questdb.griffin.plan.logical.PlanStage;
import io.questdb.std.Mutable;

/**
 * One rewrite of the optimiser's fixed sequence. A pass rewrites a plan at the stage it requires into a plan at the
 * stage it produces, see {@link PlanStage}. The optimiser runs every pass over the plan of a query level in order and,
 * under {@link io.questdb.ParanoiaState#PLAN_PARANOIA_MODE}, checks that the plan is at the stage the pass requires
 * before it and verifies the invariants of the stage the pass produces after it. Clearing a pass drops the state it
 * holds for a statement.
 */
interface OptimiserPass extends Mutable {

    /**
     * Rewrites the plan and returns its root, the same node when the pass rewrites in place.
     */
    LogicalPlan apply(LogicalPlan plan) throws SqlException;

    @Override
    default void clear() {
    }

    /**
     * The name an invariant failure after the pass reports.
     */
    String getName();

    /**
     * The stage of the plan the pass returns: the stage it rewrites, unless it moves the plan on.
     */
    default PlanStage getProducedStage() {
        return getRequiredStage();
    }

    /**
     * The stage of the plan the pass rewrites: a decorrelated plan, unless the pass needs an earlier one.
     */
    default PlanStage getRequiredStage() {
        return PlanStage.DECORRELATED;
    }

    /**
     * Records, under paranoia, the facts of the plan the pass rewrites that {@link #verify} compares the plan it
     * returned with.
     */
    default void record(LogicalPlan plan, PlanVerifier verifier) {
    }

    /**
     * Checks, under paranoia, the invariants only the pass knows of the plan it returned, before the optimiser verifies
     * the invariants of the stage it produces.
     */
    default void verify(LogicalPlan plan, PlanVerifier verifier) {
    }
}
