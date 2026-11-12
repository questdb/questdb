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

import io.questdb.ParanoiaState;

/**
 * The checks that keep the facts of plan nodes consistent with the stage of their plan, see {@link PlanStage}, under
 * {@link ParanoiaState#PLAN_PARANOIA_MODE}: nothing reads a physical fact before physical planning decides it, nothing
 * rewrites a node whose structure physical planning reads, and clearing a plan object resets every fact.
 */
public final class PlanFacts {
    public static final String CLEARED_FACT = "cleared plan object keeps a fact";
    public static final String LATE_REWRITE = "node is rewritten after the stage that fixes its structure";
    public static final String UNDECIDED_READ = "physical fact is read before physical planning decides it";

    private PlanFacts() {
    }

    /**
     * Fails a read of the physical fact {@code fact} that physical planning has not decided, {@code isDecided}.
     */
    public static void checkDecided(boolean isDecided, String fact) {
        if (ParanoiaState.PLAN_PARANOIA_MODE && !isDecided) {
            throw new AssertionError(UNDECIDED_READ + ": " + fact);
        }
    }

    /**
     * Fails the rewrite {@code rewrite} of a node at {@code stage}, see {@link LogicalPlan#getStage}, when the stage
     * fixes its structure, see {@link PlanStage#isRewritten}.
     */
    static void checkRewritable(PlanStage stage, String rewrite) {
        if (ParanoiaState.PLAN_PARANOIA_MODE && stage != null && stage.isRewritten()) {
            throw new AssertionError(LATE_REWRITE + ": " + rewrite);
        }
    }
}
