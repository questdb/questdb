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

package io.questdb.griffin.engine.table;

import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;

/**
 * The atom of a page frame sequence through which a {@link HashJoinBuildStage} dispatches the
 * rounds of its parallel build. The stage's round reducers receive the sequence alone, so they
 * resolve the stage that dispatched the round, and the slot each task runs on, through the
 * sequence's atom.
 * <p>
 * A task of a round holds no resource of the host's, but every task of a sequence takes a slot,
 * as the sequence's work-stealing strategy expects of its tasks. The slot's index addresses the
 * stage's per-slot state: -1 for the owner, and 0 to the worker count less one for the workers.
 */
public interface HashJoinBuildHost {

    /**
     * The stage whose round is in flight. The host sets it before the stage dispatches a round, and
     * keeps it until the round returns, so every task of the round finds it here.
     */
    HashJoinBuildStage getBuildingStage();

    /**
     * Takes the slot that a task runs on: -1 for the owner thread running a task of its own
     * sequence, and a worker's slot otherwise, which the caller hands back to
     * {@link #release(int)} once the task is done.
     */
    int maybeAcquire(int workerId, boolean isOwner, SqlExecutionCircuitBreaker breaker);

    /**
     * Hands back a slot that {@link #maybeAcquire} took.
     */
    void release(int slotId);
}
