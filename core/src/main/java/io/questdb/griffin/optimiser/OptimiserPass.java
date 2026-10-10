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
import io.questdb.std.Mutable;

/**
 * One rewrite of the optimiser's fixed sequence. The optimiser runs every pass over the plan of a query level in
 * order and, under {@link io.questdb.ParanoiaState#PLAN_PARANOIA_MODE}, verifies the plan's invariants after each.
 * Clearing a pass drops the state it holds for a statement.
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
     * True when the plan the pass returns holds no dependent join step, which every later pass then preserves.
     */
    default boolean removesDependentSteps() {
        return false;
    }
}
