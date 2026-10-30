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

package io.questdb.cairo.lv;

/**
 * Recovery outcome reported by live_views(). BLOCKED preserves an unsupported newer
 * checkpoint format; REBUILD_BLOCKED preserves output that the restatement guard
 * refuses to replace. Both stop refresh and report invalid status. REBUILD_DEFERRED
 * waits cooperatively for base apply, preserving status, scheduling and the WAL floor.
 * <p>
 * Phases are not persisted. Restart re-derives format blocks and pending upgrade
 * rebuilds from the superblock, and retries restore or guarded rebuild. Older-format
 * upgrade rebuilds bypass the guard and retire their directory only after commit.
 * <p>
 * A readable build can clear a format block; a successful restore or permitted rebuild
 * can clear a rebuild block. Recreating a view derives its output from surviving base
 * rows. Blocking releases the base WAL floor to avoid unbounded retention, so a later
 * restart can lose the WAL needed for restore and require a rebuild instead. Purged
 * backlog can also weaken guard evidence where legitimate removals are possible.
 * The phase and reason expose these outcomes without treating apply lag as failure.
 */
public final class LiveViewCheckpointRecoveryPhase {
    /**
     * The view's checkpoint timeline declares a newer format version than this
     * build implements. Refresh and checkpoint publication are stopped for the view,
     * and its checkpoint directory, materialized rows and watermarks are left
     * exactly as they are. The view stays queryable over the rows it had, reports
     * {@code invalid} as its status, and releases its base WAL floor.
     */
    public static final int BLOCKED = 1;
    /**
     * The view is on this build's own format, has no timeline at all, or is on an
     * older format whose upgrade rebuild is still to run. The ordinary lifecycle
     * applies.
     */
    public static final int NONE = 0;
    /**
     * A whole-view rebuild from the applied base would have dropped rows the view
     * retains, so {@link LiveViewRebuildRestatementGuard} refused it before it
     * committed. Refresh is stopped exactly as for {@link #BLOCKED}: the view's rows
     * and watermarks are as the refused rebuild found them, a timeline the rebuild
     * would have retired is still on disk, and the view stays queryable, reports
     * {@code invalid} and releases its base WAL floor. The reason names the route
     * that asked for the rebuild and the evidence that refused it.
     */
    public static final int REBUILD_BLOCKED = 2;
    /**
     * A running view owes a whole-view rebuild from the applied base, and the base
     * table has not yet applied every commit the view's table holds output of. The
     * rebuild waits for that apply rather than pin a snapshot that lacks them, and
     * nothing has moved: the rows, the watermarks and the timeline are as the fault
     * that asked for the rebuild left them. Not a block - the view stays
     * {@code active}, keeps its base WAL floor and is retried on the apply-lag
     * back-off - and it clears on its own: the rebuild runs once the base applies,
     * or a retry restores the view from its timeline first. The reason names the
     * recovery that asked for the rebuild, the base table and the commit it waits
     * for.
     */
    public static final int REBUILD_DEFERRED = 3;
    /**
     * Restart route: the timeline restore could not be used and the applied-base rebuild that
     * covers for it failed too, so the view carries a pending invalidation instead of a runtime.
     * <p>
     * The {@code ROUTE_*} values name the operation a view's single restart recovery attempt
     * finished with, as {@link LiveViewInstance#getCheckpointRestoreRoute()} reports it. The
     * refresh worker records each from the branch that finished that operation, so a route
     * names only work that ran, and a failed restore cannot hide behind the rebuild that
     * covered for it.
     */
    public static final int ROUTE_BLOCKED = 3;
    /**
     * Restart route: the timeline was absent, unusable or fenced off, and the view rebuilt its
     * whole window from the applied base, retiring the timeline before the replacement
     * committed.
     */
    public static final int ROUTE_FALLBACK_REBUILD = 2;
    /**
     * Restart route: no restart recovery has completed, or the view held identity state that
     * needs no root.
     */
    public static final int ROUTE_NONE = 0;
    /**
     * Restart route: the rebuild that covers for an unusable timeline was refused before it
     * committed, and the view is in {@link #REBUILD_BLOCKED}.
     */
    public static final int ROUTE_REBUILD_BLOCKED = 5;
    /**
     * Restart route: the window state came back from the published root that
     * {@link LiveViewInstance#getCheckpointRestoreGeneration()} and
     * {@link LiveViewInstance#getCheckpointRestoreCheckpointId()} name.
     */
    public static final int ROUTE_TIMELINE_RESTORE = 1;
    /**
     * Restart route: no attempt ran, because a refresh turn declined a view in
     * {@link #BLOCKED}.
     */
    public static final int ROUTE_UPGRADE_BLOCKED = 4;
    /**
     * Restart route: the timeline's format was older than this build reads, and the view
     * rebuilt from the applied base with the restatement guard standing down.
     */
    public static final int ROUTE_UPGRADE_REBUILD = 6;
    /**
     * Not a phase value but the name {@code live_views()} reports for a view carried over
     * from an older checkpoint format whose upgrade rebuild has not run yet. The view is
     * {@code active}, keeps refreshing from its first turn on, and the rebuild clears it.
     * {@link #REBUILD_DEFERRED} outranks it: its reason names the upgrade as the cause.
     */
    public static final String UPGRADE_REBUILD_PENDING_NAME = "upgrade_rebuild_pending";

    private LiveViewCheckpointRecoveryPhase() {
    }

    /**
     * @param phase one of the {@code LiveViewCheckpointRecoveryPhase} constants
     * @return true for the phases that stop the view - {@link #BLOCKED} and
     * {@link #REBUILD_BLOCKED} - as opposed to {@link #NONE} and the wait that is
     * {@link #REBUILD_DEFERRED}
     */
    public static boolean isBlocked(int phase) {
        return phase == BLOCKED || phase == REBUILD_BLOCKED;
    }

    /**
     * @param phase one of the {@code LiveViewCheckpointRecoveryPhase} constants
     * @return the phase's stable external name, or null for {@link #NONE}, which
     * is the absence of a recovery rather than a phase of one
     */
    public static String name(int phase) {
        return switch (phase) {
            case BLOCKED -> "blocked";
            case REBUILD_BLOCKED -> "rebuild_blocked";
            case REBUILD_DEFERRED -> "rebuild_deferred";
            default -> null;
        };
    }

    /**
     * @param phase                   one of the {@code LiveViewCheckpointRecoveryPhase} constants
     * @param isUpgradeRebuildPending whether the view still owes the rebuild an older checkpoint
     *                                format asks for
     * @return the name {@code live_views()} reports: the phase's own name when there is a phase,
     * {@link #UPGRADE_REBUILD_PENDING_NAME} when there is none and the upgrade rebuild is owed,
     * and null otherwise
     */
    public static String name(int phase, boolean isUpgradeRebuildPending) {
        return phase == NONE && isUpgradeRebuildPending ? UPGRADE_REBUILD_PENDING_NAME : name(phase);
    }

    /**
     * @param route one of the {@code ROUTE_*} constants
     * @return the route's stable name, or null for {@link #ROUTE_NONE}
     */
    public static String routeName(int route) {
        return switch (route) {
            case ROUTE_TIMELINE_RESTORE -> "timeline_restore";
            case ROUTE_FALLBACK_REBUILD -> "fallback_rebuild";
            case ROUTE_BLOCKED -> "blocked";
            case ROUTE_UPGRADE_BLOCKED -> "upgrade_blocked";
            case ROUTE_REBUILD_BLOCKED -> "rebuild_blocked";
            case ROUTE_UPGRADE_REBUILD -> "upgrade_rebuild";
            default -> null;
        };
    }
}
