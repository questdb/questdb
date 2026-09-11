/*******************************************************************************
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

package io.questdb.cairo.lv;

/**
 * Names the route a live view's one restart recovery attempt actually took.
 * <p>
 * {@link LiveViewInstance#isCheckpointRestoreSucceeded()} answers only whether the
 * attempt resolved the view's derived state at all, and both routes that resolve it
 * report the same {@code true}: a timeline restore rehydrates the window state from a
 * published root, while the applied-base rebuild throws that root away and recomputes
 * the whole window from the base table. Both produce correct rows, fault no refresh
 * cycle and fail no seal, so a recompute oracle cannot separate them either - which is
 * how a restore regression hides behind a fallback that quietly covers for it.
 * <p>
 * The route is the observation that separates them. {@link LiveViewRefreshJob} emits it
 * from the branch that finished the operation, never from a shared success setter, so a
 * route can only name work that actually ran. It pairs with the selected generation and
 * checkpoint id ({@link LiveViewInstance#getCheckpointRestoreGeneration()},
 * {@link LiveViewInstance#getCheckpointRestoreCheckpointId()}) and with the lifetime
 * rebuild and timeline-reset counters
 * ({@link LiveViewInstance#getCheckpointRebuildAttempts()},
 * {@link LiveViewInstance#getCheckpointTimelineResets()}), so a test can assert both
 * that a restart restored from an expected root and that nothing reset or rebuilt on
 * the way there.
 * <p>
 * {@link #UPGRADE_BLOCKED} is the one route no restore ran for: the timeline declares
 * a format version this build does not implement, so the refresh worker declined the
 * attempt instead of making one. {@link #REBUILD_BLOCKED} is the other decision rather
 * than outcome: the restore could not be used, the applied-base rebuild that covers for
 * it started, and {@link LiveViewRebuildRestatementGuard} refused it before it committed.
 * The upgrade rebuild that would follow a successful source-history preflight is
 * withdrawn with that preflight and has no route.
 */
public final class LiveViewCheckpointRestoreRoute {
    /**
     * The attempt ran and left the view without usable derived state. The applied-base
     * rebuild was the last resort and it failed too, so the view carries a pending
     * invalidation reason instead of a runtime. Neither {@code timeline_restore} nor
     * {@code fallback_rebuild} completed.
     */
    public static final int BLOCKED = 3;
    /**
     * No attempt was made: the view's checkpoint timeline declares a format version this
     * build does not implement, and {@link LiveViewCheckpointRecoveryPhase#BLOCKED} holds
     * its refresh. Distinct from {@link #BLOCKED}, which is an attempt that ran and left
     * the view without derived state; here the derived state on disk is intact and this
     * build simply may not touch it. The refresh worker emits it from the turn it
     * declined, so the route still names what actually happened.
     */
    public static final int UPGRADE_BLOCKED = 4;
    /**
     * The timeline was absent, unusable, or fenced off by a repair marker, and the view
     * recomputed its whole window from the applied base instead. Correct rows, but the
     * roots the previous process published are gone: the rebuild retires the timeline
     * before it replays.
     */
    public static final int FALLBACK_REBUILD = 2;
    /**
     * No restart recovery has completed for this instance. Either the refresh worker has
     * not reached its single restore attempt yet, or the attempt found identity state -
     * a never-materialized ACTIVE view whose exact runtime needs no root to prove it.
     */
    public static final int NONE = 0;
    /**
     * The timeline was absent, unusable, or fenced off by a repair marker, and the
     * applied-base rebuild that covers for it was refused before it committed: the base
     * no longer holds rows the view retains, so recomputing from it would have dropped
     * them. {@link LiveViewCheckpointRecoveryPhase#REBUILD_BLOCKED} holds the view's
     * refresh. Distinct from {@link #BLOCKED}, which is a rebuild that ran and failed and
     * leaves a durable invalidation behind; here nothing durable moved, and
     * {@link LiveViewInstance#getCheckpointRebuildAttempts()} counts the attempt that was
     * refused.
     */
    public static final int REBUILD_BLOCKED = 5;
    /**
     * The window state came back from a published timeline root.
     * {@link LiveViewInstance#getCheckpointRestoreGeneration()} and
     * {@link LiveViewInstance#getCheckpointRestoreCheckpointId()} name it.
     */
    public static final int TIMELINE_RESTORE = 1;

    private LiveViewCheckpointRestoreRoute() {
    }

    /**
     * @param route one of the {@code LiveViewCheckpointRestoreRoute} constants
     * @return the route's stable external name, or {@code null} for {@link #NONE},
     * which has no attempt to name
     */
    public static String name(int route) {
        return switch (route) {
            case TIMELINE_RESTORE -> "timeline_restore";
            case FALLBACK_REBUILD -> "fallback_rebuild";
            case BLOCKED -> "blocked";
            case UPGRADE_BLOCKED -> "upgrade_blocked";
            case REBUILD_BLOCKED -> "rebuild_blocked";
            default -> null;
        };
    }
}
