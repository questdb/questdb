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
 * Names where a live view stands against the checkpoint format boundary.
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
 * The phase is derived, not persisted. Every restart re-reads the superblock and
 * re-reaches the same disposition, so a blocked view survives a restart without
 * a marker file of its own, and a build that does implement the version simply
 * never reaches the block.
 * <p>
 * {@link LiveViewInstance#getCheckpointRecoveryReason()} carries the operator
 * text that goes with the phase, and {@code live_views()} publishes both as
 * {@code checkpoint_recovery_phase} and {@code checkpoint_recovery_reason}.
 * <p>
 * The remaining phases the recovery policy names - a pending classification, a
 * rebuild in flight, and a completed one - arrive with the source-history
 * preflight and the recovery bundle that authorize a reset. Until then the only
 * outcome a foreign version can take is the blocking one, and there is nothing
 * for an operator to advance it through.
 */
public final class LiveViewCheckpointRecoveryPhase {
    /**
     * The view's checkpoint timeline declares a format version this build does
     * not implement. Refresh and checkpoint publication are stopped for the view,
     * its checkpoint directory, materialized rows and watermarks are left exactly
     * as they are, and its base table's WAL is retained whole. The view stays
     * queryable over the rows it had.
     */
    public static final int BLOCKED = 1;
    /**
     * The view is on this build's own format, or has no timeline at all. The
     * ordinary lifecycle applies.
     */
    public static final int NONE = 0;

    private LiveViewCheckpointRecoveryPhase() {
    }

    /**
     * @param phase one of the {@code LiveViewCheckpointRecoveryPhase} constants
     * @return the phase's stable external name, or null for {@link #NONE}, which
     * is the absence of a recovery rather than a phase of one
     */
    public static String name(int phase) {
        return phase == BLOCKED ? "blocked" : null;
    }
}
