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

package io.questdb.test.griffin;

import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.sql.AtomicBooleanCircuitBreaker;
import io.questdb.griffin.engine.table.AsyncHashJoinGroupByAtom;
import org.jetbrains.annotations.Nullable;

import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BooleanSupplier;

/**
 * Observes the build of a fused hash join GROUP BY from inside the query's first read, and
 * cancels the query there on demand. The operator builds on its cursor's first read and probes in
 * the same call, so a test that cancels before the read cancels an execution without a build, and
 * one that cancels after it cancels nothing. This breaker acts at a breaker check instead:
 * {@link #cancellingInsideBuild} trips at the first check that the build itself makes, and
 * {@link #cancellingOnceBuilt} at the first one that finds the build ready, so that the cancelled
 * execution gives up a live build. {@link #meetingOnceBuilt} stops the owner at that same check
 * until other queries got there too, which is how a test makes several builds live at once.
 */
final class HashJoinBuildCircuitBreaker extends AtomicBooleanCircuitBreaker {
    private static final String ATOM_CLASS_NAME = AsyncHashJoinGroupByAtom.class.getName();
    private static final StackWalker STACK_WALKER = StackWalker.getInstance();
    private final AtomicLong buildChecks = new AtomicLong();
    // Where the owner waits for the other queries once its build is ready; null without a meeting.
    @Nullable
    private final CyclicBarrier built;
    private final boolean isCancelledInsideBuild;
    private final boolean isCancelledOnceBuilt;
    // Null unless the breaker acts once the build is ready, which only the owner may tell.
    @Nullable
    private final BooleanSupplier isBuilt;
    // True when the breaker walks the stack on every check, to tell the build's checks from the rest.
    private final boolean isWatchingBuildChecks;
    private final Thread owner = Thread.currentThread();
    // Owner only: true once the owner found the build ready.
    private boolean hasFoundBuild;

    private HashJoinBuildCircuitBreaker(
            CairoEngine engine,
            boolean isWatchingBuildChecks,
            boolean isCancelledInsideBuild,
            @Nullable BooleanSupplier isBuilt,
            @Nullable CyclicBarrier built,
            boolean isCancelledOnceBuilt
    ) {
        super(engine);
        this.isWatchingBuildChecks = isWatchingBuildChecks;
        this.isCancelledInsideBuild = isCancelledInsideBuild;
        this.isBuilt = isBuilt;
        this.built = built;
        this.isCancelledOnceBuilt = isCancelledOnceBuilt;
    }

    /**
     * Cancels at the first breaker check that the build makes, on whichever thread makes it: the
     * owner's walk of the build frames and the rest of its build() call, or a task of a parallel
     * build's rounds. Every build makes such a check.
     */
    static HashJoinBuildCircuitBreaker cancellingInsideBuild(CairoEngine engine) {
        return new HashJoinBuildCircuitBreaker(engine, true, true, null, null, false);
    }

    /**
     * Cancels at the owner's first breaker check that finds the build ready, and at the latest at
     * the check that follows the probe. Only the thread that creates the breaker, the query's
     * owner, evaluates the condition: the build publishes its state to that thread alone before
     * the probe starts.
     */
    static HashJoinBuildCircuitBreaker cancellingOnceBuilt(CairoEngine engine, BooleanSupplier isBuilt) {
        return new HashJoinBuildCircuitBreaker(engine, true, false, isBuilt, null, true);
    }

    /**
     * Never cancels; counts the build's breaker checks alone.
     */
    static HashJoinBuildCircuitBreaker counting(CairoEngine engine) {
        return new HashJoinBuildCircuitBreaker(engine, true, false, null, null, false);
    }

    /**
     * Stops the owner at its first breaker check that finds the build ready, at the latest at the
     * check that follows the probe, until every party of the barrier stopped at its own. Every
     * party then holds a live build at the same time. The stopped owner runs none of its own
     * tasks meanwhile; other threads may, and none of them consults the condition. Cancels the
     * query after the meeting when asked to. This breaker does not count the build's checks.
     */
    static HashJoinBuildCircuitBreaker meetingOnceBuilt(
            CairoEngine engine,
            BooleanSupplier isBuilt,
            CyclicBarrier built,
            boolean isCancelledOnceBuilt
    ) {
        return new HashJoinBuildCircuitBreaker(engine, false, false, isBuilt, built, isCancelledOnceBuilt);
    }

    @Override
    public boolean checkIfTripped() {
        onCheck();
        return super.checkIfTripped();
    }

    /**
     * Breaker checks that ran inside the build since construction, on any thread. Zero for a
     * breaker of {@link #meetingOnceBuilt}, which does not watch them.
     */
    public long getBuildChecks() {
        return buildChecks.get();
    }

    @Override
    public int getState() {
        onCheck();
        return super.getState();
    }

    @Override
    public void statefulThrowExceptionIfTrippedNoThrottle() {
        onCheck();
        super.statefulThrowExceptionIfTrippedNoThrottle();
    }

    // True on the owner inside the atom's build() call, and on any thread inside a task of its rounds.
    private static boolean isInsideBuild() {
        return STACK_WALKER.walk(frames -> frames.anyMatch(frame -> frame.getClassName().equals(ATOM_CLASS_NAME)
                && (frame.getMethodName().equals("build")
                || frame.getMethodName().equals("partitionFrame")
                || frame.getMethodName().equals("buildPartition"))));
    }

    private void onCheck() {
        if (isWatchingBuildChecks && isInsideBuild()) {
            buildChecks.incrementAndGet();
            if (isCancelledInsideBuild) {
                cancel();
            }
        }
        if (isBuilt != null && Thread.currentThread() == owner && !hasFoundBuild && isBuilt.getAsBoolean()) {
            hasFoundBuild = true;
            if (built != null) {
                try {
                    built.await(20, TimeUnit.SECONDS);
                } catch (Exception e) {
                    throw new AssertionError("the queries did not all reach a ready build", e);
                }
            }
            if (isCancelledOnceBuilt) {
                cancel();
            }
        }
    }
}
