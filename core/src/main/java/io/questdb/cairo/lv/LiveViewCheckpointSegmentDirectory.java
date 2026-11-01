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
 * Node framing for the persistent copy-on-write B+ tree that catalogues data and
 * metadata segments and their generation-transactional reference counts.
 * <p>
 * Like the timeline tree beside it, the directory is ordered by {@code segmentId} and
 * stored as immutable, individually-checksummed metadata pages
 * ({@link #PAGE_KIND_LEAF} or {@link #PAGE_KIND_INTERNAL}). A publication copies only
 * the search paths its mutations touch and reuses untouched subtrees by page
 * reference, so the metadata a seal writes follows the segments it added or
 * re-referenced, not the number of live segments.
 *
 * <h2>Leaf page payload</h2>
 * <pre>
 *   count            INT
 *   count x entry:
 *     segmentId           LONG   (+0)
 *     fileLength          LONG   (+8)
 *     referenceCount      LONG   (+16)
 *     retireGeneration    LONG   (+24)
 *     kind                LONG   (+32)
 * </pre>
 * Entry stride is {@link #LEAF_ENTRY_STRIDE} (40 bytes). Entries sort ascending by
 * {@code segmentId}, which is unique across both kinds: ids are monotonic within a
 * history epoch, never reused, and name at most one file in exactly one of
 * {@code data/} and {@code meta/}. That makes {@code kind} redundant, but storing it
 * lets catalogue walkers (compaction, purge) read the kind instead of probing paths,
 * and keeps a segment whose file is missing classifiable.
 * <p>
 * What {@code referenceCount} counts depends on the kind:
 * <ul>
 *     <li>{@link #SEGMENT_KIND_DATA}: logical roots, not pages - a root that names the
 *     segment from several of its pages counts once.</li>
 *     <li>{@link #SEGMENT_KIND_META}: pages of the three superblock-rooted trees that
 *     reside in it and are reachable from the current generation's roots. Each tree
 *     has one live version and a publication knows exactly which pages its path copy
 *     replaced, so pages are maintainable and the finest unit available.</li>
 *     <li>{@link #SEGMENT_KIND_BOUNDARY}: logical roots, like data. Boundary metadata
 *     (checkpoint, window and function roots and the partition-map pages below them)
 *     has one live version per surviving boundary, and boundaries retire in bulk.
 *     Each root carries the set of boundary segments its closure names, so a repair
 *     splice or truncate releases a whole boundary in one reference transaction.</li>
 * </ul>
 * Every kind reaches zero exactly when the current generation stops naming the file,
 * which is all the purge rule needs. A zero count records the generation at which the
 * segment became obsolete, so a later purge can prove no reader still reaches it.
 *
 * <h2>Internal page payload</h2>
 * <pre>
 *   count            INT   (number of children)
 *   count x child:
 *     minSegmentId        LONG   (+0)   min key of the child subtree
 *     childRef            PAGE_REF, 20 bytes (+8)
 * </pre>
 * Child stride is {@link #INTERNAL_CHILD_STRIDE} (28 bytes). Storing each child's
 * minimum key instead of {@code n-1} separators keeps descent and construction
 * simple: the child that may hold key {@code K} is the rightmost child whose
 * minimum key is {@code <= K}.
 */
public final class LiveViewCheckpointSegmentDirectory {

    /**
     * Byte offset of {@code fileLength} within a leaf entry.
     */
    public static final int ENTRY_FILE_LENGTH_OFFSET = 8;
    /**
     * Byte offset of {@code kind} within a leaf entry.
     */
    public static final int ENTRY_KIND_OFFSET = 32;
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
     * On-page size of one leaf entry: five LONGs.
     */
    public static final int LEAF_ENTRY_STRIDE = ENTRY_KIND_OFFSET + Long.BYTES; // 40
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
    /**
     * A {@code meta/m.<segmentId>} file holding one boundary's metadata: a
     * checkpoint root and its function directory, a window root, or a function
     * root and the partition-map pages its build path-copied. Its
     * {@code referenceCount} is the number of current logical roots whose closure
     * names it, exactly as a data segment's is, and it moves through
     * {@link LiveViewCheckpointSegmentDirectoryWriter#applyRootReferenceChanges}.
     */
    public static final long SEGMENT_KIND_BOUNDARY = 2;
    /**
     * A {@code data/d.<segmentId>} file holding encoded state pages. Its
     * {@code referenceCount} is the number of current logical roots that name it.
     */
    public static final long SEGMENT_KIND_DATA = 0;
    /**
     * A {@code meta/m.<segmentId>} file holding B+ tree pages of the timeline, the
     * row-position delta index or the segment catalogue itself. Its
     * {@code referenceCount} is the number of its pages the current generation's
     * superblock-rooted trees still reach - see
     * {@link LiveViewCheckpointSegmentDirectoryWriter#releaseMetadataPages}.
     */
    public static final long SEGMENT_KIND_META = 1;

    private LiveViewCheckpointSegmentDirectory() {
    }
}
