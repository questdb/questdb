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
 * Node framing for the persistent copy-on-write B+ tree that catalogues data
 * segments and their generation-transactional reference counts.
 * <p>
 * The directory is ordered by {@code segmentId} and stored as immutable,
 * individually-checksummed metadata pages (kind {@link #PAGE_KIND_LEAF} or
 * {@link #PAGE_KIND_INTERNAL}), exactly like the timeline tree beside it. A
 * publication copies only the search paths its mutations touch and reuses every
 * untouched subtree by page reference, so the metadata a seal writes follows the
 * segments it added or re-referenced rather than how many segments are live.
 *
 * <h2>Leaf page payload</h2>
 * <pre>
 *   count            INT
 *   count x entry:
 *     segmentId           LONG   (+0)
 *     fileLength          LONG   (+8)
 *     referenceCount      LONG   (+16)
 *     retireGeneration    LONG   (+24)
 * </pre>
 * Entry stride is {@link #LEAF_ENTRY_STRIDE} (32 bytes). Entries are sorted
 * ascending by {@code segmentId}, which is unique because segment ids are
 * monotonic within a history epoch and never reused.
 * <p>
 * Reference counts are per logical root, not per page: a root that names one
 * segment from several of its pages counts once. A zero count carries the
 * generation at which the segment became obsolete, so a later purge can prove no
 * reader can still reach it.
 *
 * <h2>Internal page payload</h2>
 * <pre>
 *   count            INT   (number of children)
 *   count x child:
 *     minSegmentId        LONG   (+0)   min key of the child subtree
 *     childRef            PAGE_REF, 20 bytes (+8)
 * </pre>
 * Child stride is {@link #INTERNAL_CHILD_STRIDE} (28 bytes). Storing each
 * child's minimum key rather than {@code n-1} separators keeps descent and node
 * construction simple: the child that may hold key {@code K} is the rightmost
 * child whose minimum key is {@code <=} {@code K}.
 */
public final class LiveViewCheckpointSegmentDirectory {

    /**
     * Byte offset of {@code fileLength} within a leaf entry.
     */
    public static final int ENTRY_FILE_LENGTH_OFFSET = 8;
    /**
     * Byte offset of {@code referenceCount} within a leaf entry.
     */
    public static final int ENTRY_REFERENCE_COUNT_OFFSET = 16;
    /**
     * Byte offset of {@code retireGeneration} within a leaf entry.
     */
    public static final int ENTRY_RETIRE_GENERATION_OFFSET = 24;
    /**
     * Byte offset of {@code segmentId} within a leaf entry.
     */
    public static final int ENTRY_SEGMENT_ID_OFFSET = 0;
    /**
     * Byte offset of a child's {@code minSegmentId} within an internal child
     * record.
     */
    public static final int INTERNAL_CHILD_MIN_SEGMENT_ID_OFFSET = 0;
    /**
     * Byte offset of a child's {@code childRef} within an internal child record.
     */
    public static final int INTERNAL_CHILD_REF_OFFSET = 8;
    /**
     * On-page size of one internal child record: one key LONG plus a
     * {@link LiveViewCheckpointPageRef}.
     */
    public static final int INTERNAL_CHILD_STRIDE = INTERNAL_CHILD_REF_OFFSET + LiveViewCheckpointPageRef.BYTES; // 28
    /**
     * On-page size of one leaf entry: four LONGs.
     */
    public static final int LEAF_ENTRY_STRIDE = ENTRY_RETIRE_GENERATION_OFFSET + Long.BYTES; // 32
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
     * Page kind of an internal (branch) node in the segment directory tree.
     */
    public static final int PAGE_KIND_INTERNAL = 0x1c;
    /**
     * Page kind of a leaf node holding segment catalogue entries.
     */
    public static final int PAGE_KIND_LEAF = 0x15;
    /**
     * {@code retireGeneration} of a segment that at least one current root still
     * references.
     */
    public static final long RETIRE_GENERATION_NONE = -1;

    private LiveViewCheckpointSegmentDirectory() {
    }
}
