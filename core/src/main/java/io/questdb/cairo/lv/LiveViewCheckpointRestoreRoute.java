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
 * The upgrade rebuild an old-format timeline would take has no code here yet: this
 * branch keeps the anchor-root decoder rather than establishing a format boundary, so
 * no timeline needs one. It joins this list with that boundary.
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
            default -> null;
        };
    }
}
