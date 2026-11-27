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
import io.questdb.cairo.sql.async.PageFrameRoundDispatcher;
import io.questdb.griffin.engine.PerWorkerLockOwner;
import io.questdb.griffin.engine.PerWorkerLocks;
import io.questdb.std.LongList;
import io.questdb.std.ObjList;
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
 * <p>
 * The stages too small to build in rounds of their own build at once, when there are several, in
 * one round with one task per stage; see {@link #buildConcurrently}. Each task builds its stage
 * alone, on the slot it takes, through the stage's state of that slot, so the round's tasks share no
 * state.
 */
public final class HashJoinBuildRoundAtom implements StatefulAtom, HashJoinBuildHost, PerWorkerLockOwner {
    // The stages that the round of concurrent builds in flight builds, one per task, and the build
    // rows of each, by which the fiber dispatcher batches the tasks; empty between such rounds.
    private final LongList concurrentStageRowCounts = new LongList();
    private final ObjList<HashJoinBuildStage> concurrentStages = new ObjList<>();
    private final PerWorkerLocks perWorkerLocks;
    // The stage whose round is in flight; null between builds.
    private HashJoinBuildStage buildingStage;
    // The rounds of concurrent builds dispatched, over all executions; see
    // getConcurrentBuildRoundCount().
    private long concurrentBuildRoundCount;

    public HashJoinBuildRoundAtom(CairoConfiguration configuration, int workerCount) {
        this.perWorkerLocks = new PerWorkerLocks(configuration, workerCount);
    }

    @Override
    public void clear() {
        buildingStage = null;
        concurrentStages.clear();
        concurrentStageRowCounts.clear();
    }

    @Override
    public HashJoinBuildStage getBuildingStage() {
        return buildingStage;
    }

    /**
     * The rounds of concurrent builds that the atom's sequence ran since the atom was constructed,
     * one per execution that builds more than one small stage on the workers: a test reads it before
     * and after executions, to count the rounds of each.
     */
    @TestOnly
    public long getConcurrentBuildRoundCount() {
        return concurrentBuildRoundCount;
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

    /**
     * Adds a small stage to the next round of concurrent builds; the owner adds every stage of the
     * round once its frames are loaded, see {@link HashJoinBuildStage#prepareBuild()}. A stage must be
     * able to build on a worker's slot, see {@link HashJoinBuildStage#isConcurrentBuildCapable()}.
     */
    void addConcurrentStage(HashJoinBuildStage stage) {
        assert stage.isConcurrentBuildCapable() && !stage.isLargeBuild();
        concurrentStages.add(stage);
        concurrentStageRowCounts.add(stage.getBuildRowCount());
    }

    /**
     * Builds the stages added since the last round at once, in one round of the dispatcher's tasks,
     * one task per stage, see {@link HashJoinBuildStage#BUILD_STAGE}, on whichever threads run them,
     * the owner's included, and forgets the stages. Whether this returns or throws, no task of the
     * round is still queued or running; on failure the caller clears the stages, which closes their
     * builds, the half done ones included. The caller then ends each stage's build, see
     * {@link HashJoinBuildStage#finishBuild()}.
     *
     * @param dispatcher the sequence whose atom this is
     */
    void buildConcurrently(PageFrameRoundDispatcher dispatcher) {
        assert concurrentStages.size() > 1;
        concurrentBuildRoundCount++;
        try {
            dispatcher.dispatchRoundAndAwait(HashJoinBuildStage.BUILD_STAGE, concurrentStageRowCounts);
        } finally {
            concurrentStages.clear();
            concurrentStageRowCounts.clear();
        }
    }

    /**
     * The stage that the given task of the round of concurrent builds in flight builds.
     */
    HashJoinBuildStage getConcurrentStage(int task) {
        return concurrentStages.getQuick(task);
    }
}
