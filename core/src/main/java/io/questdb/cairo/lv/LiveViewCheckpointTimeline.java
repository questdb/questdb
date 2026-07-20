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
 * Node framing and key ordering for the persistent copy-on-write B+ tree that
 * indexes logical checkpoints (Phase 1 step 2 of
 * {@code LIVE_VIEW_VERSIONED_CHECKPOINT_TIMELINE_DESIGN.md}, sections 7 and 8.1).
 * <p>
 * The timeline is ordered by {@code (maxTimestamp, checkpointId)} and stored as
 * immutable, individually-checksummed metadata pages (kind
 * {@link #PAGE_KIND_LEAF} or {@link #PAGE_KIND_INTERNAL}) packed into metadata
 * segments by {@link LiveViewCheckpointMetaSegmentWriter}. A mutation copies only
 * the search path (an {@code O(log N)} spine of new pages) into one fresh segment
 * and reuses every untouched subtree by its existing page reference, so a reader
 * of the prior generation keeps walking the old paths.
 *
 * <h2>Leaf page payload</h2>
 * <pre>
 *   count            INT
 *   count x LogicalCheckpointEntry:
 *     maxTimestamp        LONG   (+0)
 *     checkpointId        LONG   (+8)
 *     createdLvSeqTxn     LONG   (+16)
 *     baseLvRowPosition   LONG   (+24)
 *     logicalStateBytes   LONG   (+32)
 *     rootRef             PAGE_REF, 20 bytes (+40)
 * </pre>
 * Entry stride is {@link #LEAF_ENTRY_STRIDE} (60 bytes). Entries are sorted
 * ascending by {@code (maxTimestamp, checkpointId)}, which is unique because
 * {@code checkpointId} is monotonic.
 *
 * <h2>Internal page payload</h2>
 * <pre>
 *   count            INT   (number of children)
 *   count x child:
 *     minMaxTimestamp     LONG   (+0)   min key of the child subtree
 *     minCheckpointId     LONG   (+8)
 *     childRef            PAGE_REF, 20 bytes (+16)
 * </pre>
 * Child stride is {@link #INTERNAL_CHILD_STRIDE} (36 bytes). Storing each child's
 * minimum key (rather than {@code n-1} separators) keeps both descent and node
 * construction simple: the child that may hold a key {@code K} is the rightmost
 * child whose minimum key is {@code <= K}.
 */
public final class LiveViewCheckpointTimeline {

    /**
     * Byte offset of {@code baseLvRowPosition} within a leaf entry.
     */
    public static final int ENTRY_BASE_ROW_POSITION_OFFSET = 24;
    /**
     * Byte offset of {@code checkpointId} within a leaf entry.
     */
    public static final int ENTRY_CHECKPOINT_ID_OFFSET = 8;
    /**
     * Byte offset of {@code createdLvSeqTxn} within a leaf entry.
     */
    public static final int ENTRY_CREATED_LV_SEQTXN_OFFSET = 16;
    /**
     * Byte offset of {@code logicalStateBytes} within a leaf entry.
     */
    public static final int ENTRY_LOGICAL_STATE_BYTES_OFFSET = 32;
    /**
     * Byte offset of {@code maxTimestamp} within a leaf entry.
     */
    public static final int ENTRY_MAX_TIMESTAMP_OFFSET = 0;
    /**
     * Byte offset of the entry's {@code rootRef} ({@link LiveViewCheckpointPageRef})
     * within a leaf entry.
     */
    public static final int ENTRY_ROOT_REF_OFFSET = 40;
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
    public static final int INTERNAL_CHILD_REF_OFFSET = 16;
    /**
     * On-page size of one internal child record: two key LONGs plus a
     * {@link LiveViewCheckpointPageRef}.
     */
    public static final int INTERNAL_CHILD_STRIDE = INTERNAL_CHILD_REF_OFFSET + LiveViewCheckpointPageRef.BYTES; // 36
    /**
     * On-page size of one leaf entry: five LONGs plus a
     * {@link LiveViewCheckpointPageRef}.
     */
    public static final int LEAF_ENTRY_STRIDE = ENTRY_ROOT_REF_OFFSET + LiveViewCheckpointPageRef.BYTES; // 60
    /**
     * Byte offset of a node's {@code count} field at the start of its payload
     * (a leaf's entry count or an internal node's child count).
     */
    public static final int NODE_COUNT_OFFSET = 0;
    /**
     * Bytes ahead of the first entry/child record in a node payload.
     */
    public static final int NODE_HEADER_SIZE = Integer.BYTES; // 4
    /**
     * Page kind of an internal (branch) node in the timeline tree.
     */
    public static final int PAGE_KIND_INTERNAL = 0x12;
    /**
     * Page kind of a leaf node holding logical checkpoint entries.
     */
    public static final int PAGE_KIND_LEAF = 0x11;

    private LiveViewCheckpointTimeline() {
    }

    /**
     * Total ascending order over the timeline key {@code (maxTimestamp,
     * checkpointId)}, comparing timestamps first (signed, since
     * {@code Long.MAX_VALUE} is a valid designated timestamp) and breaking ties by
     * checkpoint id.
     */
    public static int compareKey(long maxTimestampA, long checkpointIdA, long maxTimestampB, long checkpointIdB) {
        if (maxTimestampA != maxTimestampB) {
            return Long.compare(maxTimestampA, maxTimestampB);
        }
        return Long.compare(checkpointIdA, checkpointIdB);
    }
}
