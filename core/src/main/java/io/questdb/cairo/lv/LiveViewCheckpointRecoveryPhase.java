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
 * Names why a live view's recovery stopped rather than finished: a checkpoint
 * format this build cannot read, or a rebuild from the base table that would
 * change output the view already retains.
 * <p>
 * A checkpoint timeline declares its layout in {@code _timeline}'s superblock
 * ({@link LiveViewCheckpointSuperblock#SLOT_FORMAT_VERSION}). A build that meets
 * a version it does not implement knows one thing about the directory: another
 * build owns it. It cannot read the roots, cannot say what history they cover,
 * and cannot prove that rebuilding the view from the base table would reproduce
 * the rows the view is already serving - TTL, DROP/DETACH PARTITION and TRUNCATE
 * all remove source rows a live view keeps its own output for. So it neither
 * reads nor removes the directory: the view stops refreshing and everything it
 * has stays where it is, which is {@link #BLOCKED}.
 * <p>
 * The same hazard reaches a view on this build's own format through every other
 * route into the whole-view rebuild from the applied base: a restart that finds
 * no usable timeline, a lost base WAL segment, and a base schema change the view
 * survives or a refresh that failed mid-drain - those two only when the view
 * could not first restore its accumulators from its own timeline in place, the
 * way a restart does. There the build can look before it leaps, and
 * {@link LiveViewRebuildRestatementGuard} does: when the evidence shows the
 * rebuild would drop rows the view retains, the rebuild is refused before
 * anything durable moves and the view stops the same way, which is
 * {@link #REBUILD_BLOCKED}. The two phases behave alike and differ in what clears
 * them.
 * <p>
 * Neither phase is persisted. A format block is re-derived from the superblock on
 * every restart, so it survives a restart without a marker file of its own, and a
 * build that does implement the version simply never reaches it. A rebuild block
 * is re-derived by the restart's own recovery: the view restores from its timeline
 * if it can, and otherwise meets the same rebuild and the same refusal.
 * <p>
 * {@link LiveViewInstance#getCheckpointRecoveryReason()} carries the operator
 * text that goes with the phase, and {@code live_views()} publishes both as
 * {@code checkpoint_recovery_phase} and {@code checkpoint_recovery_reason}. A
 * blocked view also reports {@code view_status} as {@code invalid} and repeats
 * the reason through {@code invalidation_reason}: it is a stopped view, and the
 * queries operators already run to find stopped views must find it.
 *
 * <h3>Why the block is where this ends, rather than a recovery</h3>
 * A recovery would have to prove that replaying the source history still
 * available reproduces the output the view is already serving. QuestDB retains
 * no evidence that can prove it: WAL segments are purged once applied, dropped
 * and detached partitions are not archived, and TTL eviction keeps no journal of
 * what it removed. So the database does not decide. It stops, says why, and
 * leaves the decision to the operator, whose re-CREATE is an explicit act with
 * explicitly different historical results rather than a silent restatement.
 *
 * <h3>The exit</h3>
 * {@code SHOW CREATE LIVE VIEW} reproduces the definition, {@code DROP LIVE
 * VIEW} clears the blocked timeline with the view, and re-CREATE rebuilds from
 * the base rows that survive today. There is no unblock command. A format block
 * is re-derived from the superblock on every start, so it clears when - and only
 * when - the format becomes readable, which is what makes an accidental
 * downgrade recoverable by going back rather than by re-creating anything. A
 * rebuild block clears on a restart whose recovery no longer needs the rebuild -
 * a view whose timeline survived the refusal restores from it while the base WAL
 * its restore replays is still there - or once the operator turns
 * {@code cairo.live.view.rebuild.restatement.guard.enabled} off to let rebuilds
 * follow the base table.
 *
 * <h3>What the released WAL floor costs</h3>
 * A blocked view releases its base WAL floor, as an invalid view does. It has to:
 * a blocked view's floor never advances, so any hold it takes grows without
 * bound, on a base table every other writer and view shares. The price is paid on
 * the way back. A restore replays the base WAL between its head checkpoint's
 * boundary and the applied watermark, so a view whose block outlives a purge
 * sweep no longer has that WAL, and a later readable build takes the applied-base
 * rebuild instead - recomputing the view from whatever source rows survive today,
 * which is the outcome the block existed to avoid. Blocking buys time to go back
 * to a build that reads the format; it is not a state to rest in. A rebuild block
 * pays the same price for the same reason: the restart that could have restored
 * it from its timeline instead meets the missing WAL, and the rebuild it falls
 * back to meets the same refusal.
 */
public final class LiveViewCheckpointRecoveryPhase {
    /**
     * The view's checkpoint timeline declares a format version this build does
     * not implement. Refresh and checkpoint publication are stopped for the view,
     * and its checkpoint directory, materialized rows and watermarks are left
     * exactly as they are. The view stays queryable over the rows it had, reports
     * {@code invalid} as its status, and releases its base WAL floor.
     */
    public static final int BLOCKED = 1;
    /**
     * The view is on this build's own format, or has no timeline at all. The
     * ordinary lifecycle applies.
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

    private LiveViewCheckpointRecoveryPhase() {
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
            default -> null;
        };
    }
}
