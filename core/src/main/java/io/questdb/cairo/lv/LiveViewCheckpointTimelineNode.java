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

import io.questdb.cairo.CairoException;
import io.questdb.cairo.vm.api.MemoryA;
import org.jetbrains.annotations.NotNull;

import static io.questdb.cairo.lv.LiveViewCheckpointTimeline.ENTRY_BASE_ROW_POSITION_OFFSET;
import static io.questdb.cairo.lv.LiveViewCheckpointTimeline.ENTRY_CHECKPOINT_ID_OFFSET;
import static io.questdb.cairo.lv.LiveViewCheckpointTimeline.ENTRY_CREATED_LV_SEQTXN_OFFSET;
import static io.questdb.cairo.lv.LiveViewCheckpointTimeline.ENTRY_LOGICAL_STATE_BYTES_OFFSET;
import static io.questdb.cairo.lv.LiveViewCheckpointTimeline.ENTRY_MAX_TIMESTAMP_OFFSET;
import static io.questdb.cairo.lv.LiveViewCheckpointTimeline.ENTRY_ROOT_REF_OFFSET;
import static io.questdb.cairo.lv.LiveViewCheckpointTimeline.INTERNAL_CHILD_MIN_ID_OFFSET;
import static io.questdb.cairo.lv.LiveViewCheckpointTimeline.INTERNAL_CHILD_MIN_TS_OFFSET;
import static io.questdb.cairo.lv.LiveViewCheckpointTimeline.INTERNAL_CHILD_REF_OFFSET;
import static io.questdb.cairo.lv.LiveViewCheckpointTimeline.INTERNAL_CHILD_STRIDE;
import static io.questdb.cairo.lv.LiveViewCheckpointTimeline.LEAF_ENTRY_STRIDE;
import static io.questdb.cairo.lv.LiveViewCheckpointTimeline.NODE_COUNT_OFFSET;
import static io.questdb.cairo.lv.LiveViewCheckpointTimeline.NODE_HEADER_SIZE;
import static io.questdb.cairo.lv.LiveViewCheckpointTimeline.PAGE_KIND_INTERNAL;
import static io.questdb.cairo.lv.LiveViewCheckpointTimeline.PAGE_KIND_LEAF;

/**
 * A reusable in-heap image of one timeline B+ tree node (leaf or internal),
 * decoupled from any file mapping. It both decodes an on-page node from a
 * positioned {@link LiveViewCheckpointMetaSegmentReader} and serializes a
 * (possibly mutated) node into a {@link LiveViewCheckpointMetaSegmentWriter}.
 * <p>
 * The reader decodes into a heap image so navigation can hold a parent and a
 * child at once without two overlapping mappings, and so range iteration keeps a
 * per-level image without re-validating a page on every step. The writer uses the
 * same image to insert, replace, and split during copy-on-write publication.
 * Arrays are grown on demand and reused across nodes; one instance is not thread
 * safe and is pooled per tree level by its owner.
 */
final class LiveViewCheckpointTimelineNode {

    // Internal child columns (valid when leaf == false).
    long[] childLength = new long[0];
    long[] childMinCheckpointId = new long[0];
    long[] childMinMaxTimestamp = new long[0];
    long[] childOffset = new long[0];
    long[] childSegmentId = new long[0];
    // Leaf entry columns (valid when leaf == true).
    long[] entryBaseRowPosition = new long[0];
    long[] entryCheckpointId = new long[0];
    long[] entryCreatedLvSeqTxn = new long[0];
    long[] entryLogicalStateBytes = new long[0];
    long[] entryMaxTimestamp = new long[0];
    long[] entryRootLength = new long[0];
    long[] entryRootOffset = new long[0];
    long[] entryRootSegmentId = new long[0];
    private int count;
    private boolean leaf;

    /**
     * Appends a child record (used when building a fresh internal node top-down).
     */
    void appendChild(long minMaxTimestamp, long minCheckpointId, long segmentId, long offset, long length) {
        assert !leaf;
        ensureInternalCapacity(count + 1);
        childMinMaxTimestamp[count] = minMaxTimestamp;
        childMinCheckpointId[count] = minCheckpointId;
        childSegmentId[count] = segmentId;
        childOffset[count] = offset;
        childLength[count] = length;
        count++;
    }

    /**
     * Locates the child whose subtree may contain key {@code (maxTimestamp,
     * checkpointId)}: the rightmost child whose minimum key is {@code <=} the key,
     * clamped to child 0 when the key is below every child minimum (an insert of a
     * new global minimum descends the first child).
     */
    int childIndexFor(long maxTimestamp, long checkpointId) {
        assert !leaf && count > 0;
        int lo = 0;
        int hi = count - 1;
        int res = 0;
        while (lo <= hi) {
            final int mid = (lo + hi) >>> 1;
            if (LiveViewCheckpointTimeline.compareKey(childMinMaxTimestamp[mid], childMinCheckpointId[mid], maxTimestamp, checkpointId) <= 0) {
                res = mid;
                lo = mid + 1;
            } else {
                hi = mid - 1;
            }
        }
        return res;
    }

    void copyEntryTo(int i, @NotNull LiveViewCheckpointTimelineEntry out) {
        assert leaf;
        out.maxTimestamp = entryMaxTimestamp[i];
        out.checkpointId = entryCheckpointId[i];
        out.createdLvSeqTxn = entryCreatedLvSeqTxn[i];
        out.baseLvRowPosition = entryBaseRowPosition[i];
        out.logicalStateBytes = entryLogicalStateBytes[i];
        out.rootRef.of(entryRootSegmentId[i], entryRootOffset[i], (int) entryRootLength[i]);
    }

    int count() {
        return count;
    }

    /**
     * Decodes the page the {@code reader} is currently positioned on (after a
     * successful {@code openPage}/{@code openPageAt}) into this image, validating
     * the page kind and that the declared count fits the payload before any field
     * read.
     */
    void decode(@NotNull LiveViewCheckpointMetaSegmentReader reader) {
        final int kind = reader.getPageKind();
        final int payloadLength = reader.getPagePayloadLength();
        if (payloadLength < NODE_HEADER_SIZE) {
            throw invalid("timeline node payload too small").put(", payloadLength=").put(payloadLength);
        }
        final int c = reader.getInt(NODE_COUNT_OFFSET);
        if (c < 0) {
            throw invalid("timeline node count negative").put(", count=").put(c);
        }
        if (kind == PAGE_KIND_LEAF) {
            leaf = true;
            final long need = (long) NODE_HEADER_SIZE + (long) c * LEAF_ENTRY_STRIDE;
            if (need > payloadLength) {
                throw invalid("timeline leaf node truncated")
                        .put(", count=").put(c)
                        .put(", payloadLength=").put(payloadLength);
            }
            ensureLeafCapacity(c);
            long base = NODE_HEADER_SIZE;
            for (int i = 0; i < c; i++) {
                entryMaxTimestamp[i] = reader.getLong(base + ENTRY_MAX_TIMESTAMP_OFFSET);
                entryCheckpointId[i] = reader.getLong(base + ENTRY_CHECKPOINT_ID_OFFSET);
                entryCreatedLvSeqTxn[i] = reader.getLong(base + ENTRY_CREATED_LV_SEQTXN_OFFSET);
                entryBaseRowPosition[i] = reader.getLong(base + ENTRY_BASE_ROW_POSITION_OFFSET);
                entryLogicalStateBytes[i] = reader.getLong(base + ENTRY_LOGICAL_STATE_BYTES_OFFSET);
                entryRootSegmentId[i] = reader.getLong(base + ENTRY_ROOT_REF_OFFSET);
                entryRootOffset[i] = reader.getLong(base + ENTRY_ROOT_REF_OFFSET + Long.BYTES);
                entryRootLength[i] = reader.getInt(base + ENTRY_ROOT_REF_OFFSET + Long.BYTES + Long.BYTES);
                base += LEAF_ENTRY_STRIDE;
            }
            count = c;
        } else if (kind == PAGE_KIND_INTERNAL) {
            leaf = false;
            final long need = (long) NODE_HEADER_SIZE + (long) c * INTERNAL_CHILD_STRIDE;
            if (need > payloadLength) {
                throw invalid("timeline internal node truncated")
                        .put(", count=").put(c)
                        .put(", payloadLength=").put(payloadLength);
            }
            ensureInternalCapacity(c);
            long base = NODE_HEADER_SIZE;
            for (int i = 0; i < c; i++) {
                childMinMaxTimestamp[i] = reader.getLong(base + INTERNAL_CHILD_MIN_TS_OFFSET);
                childMinCheckpointId[i] = reader.getLong(base + INTERNAL_CHILD_MIN_ID_OFFSET);
                childSegmentId[i] = reader.getLong(base + INTERNAL_CHILD_REF_OFFSET);
                childOffset[i] = reader.getLong(base + INTERNAL_CHILD_REF_OFFSET + Long.BYTES);
                childLength[i] = reader.getInt(base + INTERNAL_CHILD_REF_OFFSET + Long.BYTES + Long.BYTES);
                base += INTERNAL_CHILD_STRIDE;
            }
            count = c;
        } else {
            throw invalid("timeline node kind unknown").put(", kind=").put(kind);
        }
    }

    /**
     * Binary-searches a leaf for the exact key {@code (maxTimestamp,
     * checkpointId)}, returning its index or {@code -1} when absent.
     */
    int findEntry(long maxTimestamp, long checkpointId) {
        assert leaf;
        int lo = 0;
        int hi = count - 1;
        while (lo <= hi) {
            final int mid = (lo + hi) >>> 1;
            final int cmp = LiveViewCheckpointTimeline.compareKey(entryMaxTimestamp[mid], entryCheckpointId[mid], maxTimestamp, checkpointId);
            if (cmp < 0) {
                lo = mid + 1;
            } else if (cmp > 0) {
                hi = mid - 1;
            } else {
                return mid;
            }
        }
        return -1;
    }

    /**
     * Inserts a child record at internal position {@code pos}, shifting later
     * children up.
     */
    void insertChildAt(int pos, long minMaxTimestamp, long minCheckpointId, long segmentId, long offset, long length) {
        assert !leaf;
        ensureInternalCapacity(count + 1);
        for (int i = count; i > pos; i--) {
            childMinMaxTimestamp[i] = childMinMaxTimestamp[i - 1];
            childMinCheckpointId[i] = childMinCheckpointId[i - 1];
            childSegmentId[i] = childSegmentId[i - 1];
            childOffset[i] = childOffset[i - 1];
            childLength[i] = childLength[i - 1];
        }
        childMinMaxTimestamp[pos] = minMaxTimestamp;
        childMinCheckpointId[pos] = minCheckpointId;
        childSegmentId[pos] = segmentId;
        childOffset[pos] = offset;
        childLength[pos] = length;
        count++;
    }

    /**
     * Inserts {@code entry} at leaf position {@code pos}, shifting later entries up.
     */
    void insertEntryAt(int pos, @NotNull LiveViewCheckpointTimelineEntry entry) {
        assert leaf;
        ensureLeafCapacity(count + 1);
        for (int i = count; i > pos; i--) {
            entryMaxTimestamp[i] = entryMaxTimestamp[i - 1];
            entryCheckpointId[i] = entryCheckpointId[i - 1];
            entryCreatedLvSeqTxn[i] = entryCreatedLvSeqTxn[i - 1];
            entryBaseRowPosition[i] = entryBaseRowPosition[i - 1];
            entryLogicalStateBytes[i] = entryLogicalStateBytes[i - 1];
            entryRootSegmentId[i] = entryRootSegmentId[i - 1];
            entryRootOffset[i] = entryRootOffset[i - 1];
            entryRootLength[i] = entryRootLength[i - 1];
        }
        entryMaxTimestamp[pos] = entry.maxTimestamp;
        entryCheckpointId[pos] = entry.checkpointId;
        entryCreatedLvSeqTxn[pos] = entry.createdLvSeqTxn;
        entryBaseRowPosition[pos] = entry.baseLvRowPosition;
        entryLogicalStateBytes[pos] = entry.logicalStateBytes;
        entryRootSegmentId[pos] = entry.rootRef.getSegmentId();
        entryRootOffset[pos] = entry.rootRef.getOffset();
        entryRootLength[pos] = entry.rootRef.getLength();
        count++;
    }

    /**
     * First internal child index whose minimum {@code maxTimestamp >= bound} (a
     * lower bound on the timestamp dimension only), or {@link #count()} when every
     * child minimum is below {@code bound}.
     */
    int internalLowerBoundByTimestamp(long bound) {
        assert !leaf;
        int lo = 0;
        int hi = count;
        while (lo < hi) {
            final int mid = (lo + hi) >>> 1;
            if (childMinMaxTimestamp[mid] < bound) {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        return lo;
    }

    boolean isLeaf() {
        return leaf;
    }

    /**
     * Position at which key {@code (maxTimestamp, checkpointId)} sorts: the first
     * index whose key is {@code >=} the given key. For a unique key that is absent,
     * this is the insertion point.
     */
    int leafInsertPosition(long maxTimestamp, long checkpointId) {
        assert leaf;
        int lo = 0;
        int hi = count;
        while (lo < hi) {
            final int mid = (lo + hi) >>> 1;
            if (LiveViewCheckpointTimeline.compareKey(entryMaxTimestamp[mid], entryCheckpointId[mid], maxTimestamp, checkpointId) < 0) {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        return lo;
    }

    /**
     * First leaf index whose {@code maxTimestamp >= bound} (a lower bound on the
     * timestamp dimension only), or {@link #count()} when every entry is below
     * {@code bound}. Used to start range iteration and to derive predecessors.
     */
    int leafLowerBoundByTimestamp(long bound) {
        assert leaf;
        int lo = 0;
        int hi = count;
        while (lo < hi) {
            final int mid = (lo + hi) >>> 1;
            if (entryMaxTimestamp[mid] < bound) {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        return lo;
    }

    /**
     * Replaces the mutable leaf values (everything except the {@code (maxTimestamp,
     * checkpointId)} key) at {@code pos} from {@code entry}. The key must match:
     * splice preserves logical identity and re-versions only the root/positions.
     */
    void replaceEntryPayloadAt(int pos, @NotNull LiveViewCheckpointTimelineEntry entry) {
        assert leaf;
        assert entryMaxTimestamp[pos] == entry.maxTimestamp && entryCheckpointId[pos] == entry.checkpointId;
        entryCreatedLvSeqTxn[pos] = entry.createdLvSeqTxn;
        entryBaseRowPosition[pos] = entry.baseLvRowPosition;
        entryLogicalStateBytes[pos] = entry.logicalStateBytes;
        entryRootSegmentId[pos] = entry.rootRef.getSegmentId();
        entryRootOffset[pos] = entry.rootRef.getOffset();
        entryRootLength[pos] = entry.rootRef.getLength();
    }

    void resetInternal() {
        leaf = false;
        count = 0;
    }

    void resetLeaf() {
        leaf = true;
        count = 0;
    }

    /**
     * Updates child {@code i}'s minimum key and subtree reference. Used by append,
     * where a child's minimum can drop when a new global minimum is inserted.
     */
    void setChildEntry(int i, long minMaxTimestamp, long minCheckpointId, long segmentId, long offset, long length) {
        assert !leaf;
        childMinMaxTimestamp[i] = minMaxTimestamp;
        childMinCheckpointId[i] = minCheckpointId;
        childSegmentId[i] = segmentId;
        childOffset[i] = offset;
        childLength[i] = length;
    }

    /**
     * Repoints child {@code i} at a new subtree while keeping its minimum key.
     * Used by splice, which does not change any key.
     */
    void setChildRef(int i, long segmentId, long offset, long length) {
        assert !leaf;
        childSegmentId[i] = segmentId;
        childOffset[i] = offset;
        childLength[i] = length;
    }

    /**
     * Splits this over-capacity node, moving its upper half into {@code right} and
     * keeping the lower half here. Both sides stay non-empty when this node holds
     * at least two records. {@code right}'s minimum key is the promoted separator.
     */
    void splitInto(@NotNull LiveViewCheckpointTimelineNode right) {
        final int half = count >>> 1;
        final int moved = count - half;
        if (leaf) {
            right.resetLeaf();
            right.ensureLeafCapacity(moved);
            for (int i = 0; i < moved; i++) {
                final int src = half + i;
                right.entryMaxTimestamp[i] = entryMaxTimestamp[src];
                right.entryCheckpointId[i] = entryCheckpointId[src];
                right.entryCreatedLvSeqTxn[i] = entryCreatedLvSeqTxn[src];
                right.entryBaseRowPosition[i] = entryBaseRowPosition[src];
                right.entryLogicalStateBytes[i] = entryLogicalStateBytes[src];
                right.entryRootSegmentId[i] = entryRootSegmentId[src];
                right.entryRootOffset[i] = entryRootOffset[src];
                right.entryRootLength[i] = entryRootLength[src];
            }
        } else {
            right.resetInternal();
            right.ensureInternalCapacity(moved);
            for (int i = 0; i < moved; i++) {
                final int src = half + i;
                right.childMinMaxTimestamp[i] = childMinMaxTimestamp[src];
                right.childMinCheckpointId[i] = childMinCheckpointId[src];
                right.childSegmentId[i] = childSegmentId[src];
                right.childOffset[i] = childOffset[src];
                right.childLength[i] = childLength[src];
            }
        }
        right.count = moved;
        count = half;
    }

    /**
     * Serializes this node into a fresh page of the {@code writer}, filling
     * {@code out} with a reference to it. Leaf and internal nodes select their page
     * kind from {@link #leaf}.
     */
    void writeTo(@NotNull LiveViewCheckpointMetaSegmentWriter writer, @NotNull LiveViewCheckpointPageRef out) {
        final MemoryA payload = writer.beginPage(leaf ? PAGE_KIND_LEAF : PAGE_KIND_INTERNAL);
        payload.putInt(count);
        if (leaf) {
            for (int i = 0; i < count; i++) {
                payload.putLong(entryMaxTimestamp[i]);
                payload.putLong(entryCheckpointId[i]);
                payload.putLong(entryCreatedLvSeqTxn[i]);
                payload.putLong(entryBaseRowPosition[i]);
                payload.putLong(entryLogicalStateBytes[i]);
                payload.putLong(entryRootSegmentId[i]);
                payload.putLong(entryRootOffset[i]);
                payload.putInt((int) entryRootLength[i]);
            }
        } else {
            for (int i = 0; i < count; i++) {
                payload.putLong(childMinMaxTimestamp[i]);
                payload.putLong(childMinCheckpointId[i]);
                payload.putLong(childSegmentId[i]);
                payload.putLong(childOffset[i]);
                payload.putInt((int) childLength[i]);
            }
        }
        writer.endPage(out);
    }

    private static long[] grow(long[] src, int cap) {
        final long[] dst = new long[cap];
        System.arraycopy(src, 0, dst, 0, src.length);
        return dst;
    }

    private static CairoException invalid(CharSequence reason) {
        return CairoException.critical(CairoException.LV_CHECKPOINT_TIMELINE_INVALID)
                .put("live view checkpoint ").put(reason);
    }

    private void ensureInternalCapacity(int n) {
        if (childMinMaxTimestamp.length >= n) {
            return;
        }
        final int cap = Math.max(n, Math.max(4, childMinMaxTimestamp.length * 2));
        childMinMaxTimestamp = grow(childMinMaxTimestamp, cap);
        childMinCheckpointId = grow(childMinCheckpointId, cap);
        childSegmentId = grow(childSegmentId, cap);
        childOffset = grow(childOffset, cap);
        childLength = grow(childLength, cap);
    }

    private void ensureLeafCapacity(int n) {
        if (entryMaxTimestamp.length >= n) {
            return;
        }
        final int cap = Math.max(n, Math.max(4, entryMaxTimestamp.length * 2));
        entryMaxTimestamp = grow(entryMaxTimestamp, cap);
        entryCheckpointId = grow(entryCheckpointId, cap);
        entryCreatedLvSeqTxn = grow(entryCreatedLvSeqTxn, cap);
        entryBaseRowPosition = grow(entryBaseRowPosition, cap);
        entryLogicalStateBytes = grow(entryLogicalStateBytes, cap);
        entryRootSegmentId = grow(entryRootSegmentId, cap);
        entryRootOffset = grow(entryRootOffset, cap);
        entryRootLength = grow(entryRootLength, cap);
    }
}
