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

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.StatefulAtom;
import io.questdb.griffin.engine.PerWorkerLockOwner;
import io.questdb.griffin.engine.PerWorkerLocks;
import org.jetbrains.annotations.TestOnly;

/**
 * The atom of the task-only sequence through which a parallel hash join builds its stages: a
 * sequence that {@code UnorderedPageFrameSequence.ofRounds()} binds without frames, since the frames
 * a build walks belong to its own build input. The owner names the stage it builds with
 * {@link #setBuildingStage}, and the stage's round reducers find it through
 * {@link #getBuildingStage()}; see {@link HashJoinBuildStage}.
 * <p>
 * Every task of a round takes a slot, as the sequence's work-stealing strategy expects of its tasks,
 * and the slot's index addresses the building stage's per-slot state. The atom guards those slots
 * with locks of its own, so that a round task never waits on a slot that a probe task holds, and
 * owns nothing else: the stages belong to the join's other atom.
 */
public final class HashJoinBuildRoundAtom implements StatefulAtom, HashJoinBuildHost, PerWorkerLockOwner {
    private final PerWorkerLocks perWorkerLocks;
    // The stage whose round is in flight; null between builds.
    private HashJoinBuildStage buildingStage;

    public HashJoinBuildRoundAtom(CairoConfiguration configuration, int workerCount) {
        this.perWorkerLocks = new PerWorkerLocks(configuration, workerCount);
    }

    @Override
    public void clear() {
        buildingStage = null;
    }

    @Override
    public HashJoinBuildStage getBuildingStage() {
        return buildingStage;
    }

    @Override
    @TestOnly
    public PerWorkerLocks getPerWorkerLocks() {
        return perWorkerLocks;
    }

    @Override
    public int maybeAcquire(int workerId, boolean isOwner, SqlExecutionCircuitBreaker breaker) {
        // The owner runs the tasks of its own sequence as worker -1.
        assert !isOwner || workerId == -1;
        return isOwner ? -1 : perWorkerLocks.acquireSlot(workerId, breaker);
    }

    @Override
    public void release(int slotId) {
        perWorkerLocks.releaseSlot(slotId);
    }

    /**
     * Names the stage whose rounds the owner dispatches next, which it keeps until the stage's build
     * returns: every task of those rounds finds the stage here.
     */
    public void setBuildingStage(HashJoinBuildStage buildingStage) {
        this.buildingStage = buildingStage;
    }
}
