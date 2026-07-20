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
 * Node framing for the persistent copy-on-write difference/prefix-sum B+ tree that
 * corrects a checkpoint suffix's cumulative {@code lvRowPosition} without a linear
 * suffix rewrite (Phase 1 step 3 of
 * {@code LIVE_VIEW_VERSIONED_CHECKPOINT_TIMELINE_DESIGN.md}, sections 7, 10.3, 12.5,
 * 20.3, invariant 10).
 * <p>
 * The timeline stores a per-root {@code baseLvRowPosition}; the effective
 * cumulative position of a logical checkpoint is
 * <pre>
 *   effectiveLvRowPosition(entry, G) =
 *       entry.baseLvRowPosition + G.rowPositionDeltaIndex.prefixSum(entry.searchKey)
 * </pre>
 * A localized O3 repair writes replay-derived positions for the {@code K} repaired
 * boundaries in {@code [C, H)} directly on the timeline, then shifts every reused
 * suffix root at or above {@code H} by the replacement's total output-row-count
 * change with a single <em>suffix range-add</em>. Rather than rewrite every later
 * leaf, the range-add is one point add into a difference array: {@code diff[H] +=
 * delta}. A {@code prefixSum(key)} query then sums every difference whose key is
 * {@code <=} the query key, so all suffix roots ({@code key >= H}) pick up
 * {@code delta} and no prefix root does. Both the point add and the query are
 * {@code O(log N)}, keeping O3 publication {@code O(log N + K)}.
 * <p>
 * The tree shares the metadata-segment page store, per-page checksums, and
 * copy-on-write publication model of the timeline tree
 * ({@link LiveViewCheckpointTimeline}) and is keyed by the same
 * {@code (maxTimestamp, checkpointId)} composite key (see
 * {@link LiveViewCheckpointTimeline#compareKey}), so the composite key ties break
 * exactly where the timeline's do.
 *
 * <h2>Leaf page payload</h2>
 * <pre>
 *   count            INT
 *   count x entry:
 *     keyMaxTimestamp     LONG   (+0)
 *     keyCheckpointId     LONG   (+8)
 *     diff                LONG   (+16)   difference value contributed at this key
 * </pre>
 * Entry stride is {@link #LEAF_ENTRY_STRIDE} (24 bytes). Entries are sorted
 * ascending by {@code (keyMaxTimestamp, keyCheckpointId)}.
 *
 * <h2>Internal page payload</h2>
 * <pre>
 *   count            INT   (number of children)
 *   count x child:
 *     minMaxTimestamp     LONG   (+0)   min key of the child subtree
 *     minCheckpointId     LONG   (+8)
 *     subtreeSum          LONG   (+16)  sum of every diff in the child subtree
 *     childRef            PAGE_REF, 20 bytes (+24)
 * </pre>
 * Child stride is {@link #INTERNAL_CHILD_STRIDE} (44 bytes). Storing each child's
 * subtree sum next to its minimum key lets a prefix-sum descent add the full sum
 * of every child strictly left of the descent path in {@code O(1)} per level.
 */
public final class LiveViewCheckpointRowPositionDelta {

    /**
     * Byte offset of a child's {@code minCheckpointId} within an internal child
     * record.
     */
    public static final int INTERNAL_CHILD_MIN_ID_OFFSET = 8;
    /**
     * Byte offset of a child's {@code minMaxTimestamp} within an internal child
     * record.
     */
    public static final int INTERNAL_CHILD_MIN_TS_OFFSET = 0;
    /**
     * Byte offset of a child's {@code childRef} within an internal child record.
     */
    public static final int INTERNAL_CHILD_REF_OFFSET = 24;
    /**
     * Byte offset of a child's {@code subtreeSum} (the sum of every diff in the
     * child subtree) within an internal child record.
     */
    public static final int INTERNAL_CHILD_SUBTREE_SUM_OFFSET = 16;
    /**
     * On-page size of one internal child record: two key LONGs, a subtree-sum LONG,
     * and a {@link LiveViewCheckpointPageRef}.
     */
    public static final int INTERNAL_CHILD_STRIDE = INTERNAL_CHILD_REF_OFFSET + LiveViewCheckpointPageRef.BYTES; // 44
    /**
     * Byte offset of the {@code diff} value within a leaf entry.
     */
    public static final int LEAF_DIFF_OFFSET = 16;
    /**
     * On-page size of one leaf entry: two key LONGs plus a {@code diff} LONG.
     */
    public static final int LEAF_ENTRY_STRIDE = LEAF_DIFF_OFFSET + Long.BYTES; // 24
    /**
     * Byte offset of {@code keyCheckpointId} within a leaf entry (low half of the
     * search key).
     */
    public static final int LEAF_KEY_CHECKPOINT_ID_OFFSET = 8;
    /**
     * Byte offset of {@code keyMaxTimestamp} within a leaf entry (high half of the
     * search key).
     */
    public static final int LEAF_KEY_MAX_TIMESTAMP_OFFSET = 0;
    /**
     * Byte offset of a node's {@code count} field at the start of its payload.
     */
    public static final int NODE_COUNT_OFFSET = 0;
    /**
     * Bytes ahead of the first entry/child record in a node payload.
     */
    public static final int NODE_HEADER_SIZE = Integer.BYTES; // 4
    /**
     * Page kind of an internal (branch) node in the row-position delta tree.
     */
    public static final int PAGE_KIND_INTERNAL = 0x14;
    /**
     * Page kind of a leaf node holding difference entries.
     */
    public static final int PAGE_KIND_LEAF = 0x13;

    private LiveViewCheckpointRowPositionDelta() {
    }
}
