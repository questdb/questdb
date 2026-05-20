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
 * Block-type identifiers for live-view checkpoint ({@code .cp}) files.
 * <p>
 * Each block in a checkpoint file is prefixed with a {@code blockType} INT
 * (one of the constants below) and a {@code blockLength} INT (payload byte
 * length, excluding the 8-byte block header). Unknown block types must be
 * skipped by readers - new block types are content-defined and do not
 * require a file-format version bump.
 */
public final class LiveViewCheckpointBlockType {

    /**
     * One per anchored named WINDOW block. Payload carries
     * {@code windowName}, partition-key column types, the anchor value type,
     * and per-partition {@code (keyValues..., lastAnchorValue)} tuples.
     * Non-anchored named windows do not produce a WINDOW_ANCHOR block.
     */
    public static final int BLOCK_WINDOW_ANCHOR = 2;

    /**
     * Present iff the checkpoint covers an in-progress backfill sweep
     * ({@link LiveViewCheckpointManifest#getKind() kind == BACKFILL}, written to
     * the {@code .bcp} namespace). Payload is two LONGs: the sweep's
     * data-cursor row offset (the {@code skipRows} resume position) and the
     * live-view row count at the checkpoint (the skip-write reconciliation
     * floor). The refresh worker writes one per backfill turn so a restart
     * mid-sweep resumes from the latest turn rather than re-sweeping.
     */
    public static final int BLOCK_BACKFILL_CURSOR = 1;

    /**
     * One per window function in the live view's compiled SELECT. Payload
     * carries {@code windowName} (which named WINDOW the function belongs to),
     * the fully-qualified factory name (for dispatch on restore), and the
     * length-prefixed function-private state bytes produced by
     * {@link io.questdb.griffin.engine.window.WindowFunction#snapshot}.
     */
    public static final int BLOCK_FUNCTION_SNAPSHOT = 3;

    /**
     * Required, exactly once per checkpoint file. Carries the manifest
     * (lvSeqTxn, lvRowPosition, baseSeqTxn, maxTimestamp, kind, window names).
     */
    public static final int BLOCK_MANIFEST = 0;

    private LiveViewCheckpointBlockType() {
    }

    public static boolean isKnown(int blockType) {
        return blockType == BLOCK_MANIFEST
                || blockType == BLOCK_BACKFILL_CURSOR
                || blockType == BLOCK_WINDOW_ANCHOR
                || blockType == BLOCK_FUNCTION_SNAPSHOT;
    }

    public static String nameOf(int blockType) {
        switch (blockType) {
            case BLOCK_MANIFEST:
                return "MANIFEST";
            case BLOCK_BACKFILL_CURSOR:
                return "BACKFILL_CURSOR";
            case BLOCK_WINDOW_ANCHOR:
                return "WINDOW_ANCHOR";
            case BLOCK_FUNCTION_SNAPSHOT:
                return "FUNCTION_SNAPSHOT";
            default:
                return "UNKNOWN(" + blockType + ")";
        }
    }
}
