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

import static io.questdb.cairo.lv.LiveViewCheckpointRowPositionDelta.INTERNAL_CHILD_MIN_ID_OFFSET;
import static io.questdb.cairo.lv.LiveViewCheckpointRowPositionDelta.INTERNAL_CHILD_MIN_TS_OFFSET;
import static io.questdb.cairo.lv.LiveViewCheckpointRowPositionDelta.INTERNAL_CHILD_REF_OFFSET;
import static io.questdb.cairo.lv.LiveViewCheckpointRowPositionDelta.INTERNAL_CHILD_STRIDE;
import static io.questdb.cairo.lv.LiveViewCheckpointRowPositionDelta.INTERNAL_CHILD_SUBTREE_SUM_OFFSET;
import static io.questdb.cairo.lv.LiveViewCheckpointRowPositionDelta.LEAF_DIFF_OFFSET;
import static io.questdb.cairo.lv.LiveViewCheckpointRowPositionDelta.LEAF_ENTRY_STRIDE;
import static io.questdb.cairo.lv.LiveViewCheckpointRowPositionDelta.LEAF_KEY_CHECKPOINT_ID_OFFSET;
import static io.questdb.cairo.lv.LiveViewCheckpointRowPositionDelta.LEAF_KEY_MAX_TIMESTAMP_OFFSET;
import static io.questdb.cairo.lv.LiveViewCheckpointRowPositionDelta.NODE_COUNT_OFFSET;
import static io.questdb.cairo.lv.LiveViewCheckpointRowPositionDelta.NODE_HEADER_SIZE;
import static io.questdb.cairo.lv.LiveViewCheckpointRowPositionDelta.PAGE_KIND_INTERNAL;
import static io.questdb.cairo.lv.LiveViewCheckpointRowPositionDelta.PAGE_KIND_LEAF;

/**
 * A reusable in-heap image of one row-position delta B+ tree node (leaf or
 * internal), decoupled from any file mapping. It both decodes an on-page node from
 * a positioned {@link LiveViewCheckpointMetaSegmentReader} and serializes a
 * (possibly mutated) node into a {@link LiveViewCheckpointMetaSegmentWriter}.
 * <p>
 * Mirrors {@link LiveViewCheckpointTimelineNode}, adding the subtree-sum aggregate
 * that the prefix-sum descent depends on: an internal child record carries the sum
 * of every {@code diff} in the child subtree, and {@link #subtreeSum()} recomputes
 * that aggregate for the current image (a leaf sums its own diffs, an internal node
 * sums its children's subtree sums). Arrays are grown on demand and reused across
 * nodes; one instance is not thread safe and is pooled per tree level by its owner.
 */
final class LiveViewCheckpointRowPositionDeltaNode {

    // Internal child columns (valid when leaf == false).
    long[] childLength = new long[0];
    long[] childMinCheckpointId = new long[0];
    long[] childMinMaxTimestamp = new long[0];
    long[] childOffset = new long[0];
    long[] childSegmentId = new long[0];
    long[] childSubtreeSum = new long[0];
    // Leaf entry columns (valid when leaf == true).
    long[] entryCheckpointId = new long[0];
    long[] entryDiff = new long[0];
    long[] entryMaxTimestamp = new long[0];
    private int count;
    private boolean leaf;

    /**
     * Accumulates {@code delta} into the diff at leaf position {@code pos} (an O3
     * suffix range-add whose breakpoint key already exists).
     */
    void addToLeafDiffAt(int pos, long delta) {
        assert leaf;
        entryDiff[pos] += delta;
    }

    /**
     * Appends a child record (used when building a fresh internal node top-down).
     */
    void appendChild(long minMaxTimestamp, long minCheckpointId, long subtreeSum, long segmentId, long offset, long length) {
        assert !leaf;
        ensureInternalCapacity(count + 1);
        childMinMaxTimestamp[count] = minMaxTimestamp;
        childMinCheckpointId[count] = minCheckpointId;
        childSubtreeSum[count] = subtreeSum;
        childSegmentId[count] = segmentId;
        childOffset[count] = offset;
        childLength[count] = length;
        count++;
    }

    /**
     * Locates the child whose subtree may contain key {@code (maxTimestamp,
     * checkpointId)}: the rightmost child whose minimum key is {@code <=} the key,
     * clamped to child 0 when the key is below every child minimum.
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
            throw invalid("row position delta node payload too small").put(", payloadLength=").put(payloadLength);
        }
        final int c = reader.getInt(NODE_COUNT_OFFSET);
        if (c < 0) {
            throw invalid("row position delta node count negative").put(", count=").put(c);
        }
        if (kind == PAGE_KIND_LEAF) {
            leaf = true;
            final long need = (long) NODE_HEADER_SIZE + (long) c * LEAF_ENTRY_STRIDE;
            if (need > payloadLength) {
                throw invalid("row position delta leaf node truncated")
                        .put(", count=").put(c)
                        .put(", payloadLength=").put(payloadLength);
            }
            ensureLeafCapacity(c);
            long base = NODE_HEADER_SIZE;
            for (int i = 0; i < c; i++) {
                entryMaxTimestamp[i] = reader.getLong(base + LEAF_KEY_MAX_TIMESTAMP_OFFSET);
                entryCheckpointId[i] = reader.getLong(base + LEAF_KEY_CHECKPOINT_ID_OFFSET);
                entryDiff[i] = reader.getLong(base + LEAF_DIFF_OFFSET);
                base += LEAF_ENTRY_STRIDE;
            }
            count = c;
        } else if (kind == PAGE_KIND_INTERNAL) {
            leaf = false;
            final long need = (long) NODE_HEADER_SIZE + (long) c * INTERNAL_CHILD_STRIDE;
            if (need > payloadLength) {
                throw invalid("row position delta internal node truncated")
                        .put(", count=").put(c)
                        .put(", payloadLength=").put(payloadLength);
            }
            ensureInternalCapacity(c);
            long base = NODE_HEADER_SIZE;
            for (int i = 0; i < c; i++) {
                childMinMaxTimestamp[i] = reader.getLong(base + INTERNAL_CHILD_MIN_TS_OFFSET);
                childMinCheckpointId[i] = reader.getLong(base + INTERNAL_CHILD_MIN_ID_OFFSET);
                childSubtreeSum[i] = reader.getLong(base + INTERNAL_CHILD_SUBTREE_SUM_OFFSET);
                childSegmentId[i] = reader.getLong(base + INTERNAL_CHILD_REF_OFFSET);
                childOffset[i] = reader.getLong(base + INTERNAL_CHILD_REF_OFFSET + Long.BYTES);
                childLength[i] = reader.getInt(base + INTERNAL_CHILD_REF_OFFSET + Long.BYTES + Long.BYTES);
                base += INTERNAL_CHILD_STRIDE;
            }
            count = c;
        } else {
            throw invalid("row position delta node kind unknown").put(", kind=").put(kind);
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
    void insertChildAt(int pos, long minMaxTimestamp, long minCheckpointId, long subtreeSum, long segmentId, long offset, long length) {
        assert !leaf;
        ensureInternalCapacity(count + 1);
        for (int i = count; i > pos; i--) {
            childMinMaxTimestamp[i] = childMinMaxTimestamp[i - 1];
            childMinCheckpointId[i] = childMinCheckpointId[i - 1];
            childSubtreeSum[i] = childSubtreeSum[i - 1];
            childSegmentId[i] = childSegmentId[i - 1];
            childOffset[i] = childOffset[i - 1];
            childLength[i] = childLength[i - 1];
        }
        childMinMaxTimestamp[pos] = minMaxTimestamp;
        childMinCheckpointId[pos] = minCheckpointId;
        childSubtreeSum[pos] = subtreeSum;
        childSegmentId[pos] = segmentId;
        childOffset[pos] = offset;
        childLength[pos] = length;
        count++;
    }

    /**
     * Inserts a new leaf entry at position {@code pos}, shifting later entries up.
     */
    void insertEntryAt(int pos, long maxTimestamp, long checkpointId, long diff) {
        assert leaf;
        ensureLeafCapacity(count + 1);
        for (int i = count; i > pos; i--) {
            entryMaxTimestamp[i] = entryMaxTimestamp[i - 1];
            entryCheckpointId[i] = entryCheckpointId[i - 1];
            entryDiff[i] = entryDiff[i - 1];
        }
        entryMaxTimestamp[pos] = maxTimestamp;
        entryCheckpointId[pos] = checkpointId;
        entryDiff[pos] = diff;
        count++;
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
     * First leaf index whose key is strictly greater than {@code (maxTimestamp,
     * checkpointId)}. Entries {@code [0, result)} are exactly the ones whose key is
     * {@code <=} the query key, i.e. the diffs a prefix sum accumulates in this
     * leaf.
     */
    int leafUpperBound(long maxTimestamp, long checkpointId) {
        assert leaf;
        int lo = 0;
        int hi = count;
        while (lo < hi) {
            final int mid = (lo + hi) >>> 1;
            if (LiveViewCheckpointTimeline.compareKey(entryMaxTimestamp[mid], entryCheckpointId[mid], maxTimestamp, checkpointId) <= 0) {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        return lo;
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
     * Updates child {@code i}'s minimum key, subtree sum, and subtree reference.
     * Used after a child is rewritten copy-on-write: its minimum can drop when a new
     * global-minimum key is inserted, and its subtree sum changes by the added delta.
     */
    void setChildEntry(int i, long minMaxTimestamp, long minCheckpointId, long subtreeSum, long segmentId, long offset, long length) {
        assert !leaf;
        childMinMaxTimestamp[i] = minMaxTimestamp;
        childMinCheckpointId[i] = minCheckpointId;
        childSubtreeSum[i] = subtreeSum;
        childSegmentId[i] = segmentId;
        childOffset[i] = offset;
        childLength[i] = length;
    }

    /**
     * Splits this over-capacity node, moving its upper half into {@code right} and
     * keeping the lower half here. Both sides stay non-empty when this node holds at
     * least two records. {@code right}'s minimum key is the promoted separator; the
     * caller recomputes each half's subtree sum via {@link #subtreeSum()}.
     */
    void splitInto(@NotNull LiveViewCheckpointRowPositionDeltaNode right) {
        final int half = count >>> 1;
        final int moved = count - half;
        if (leaf) {
            right.resetLeaf();
            right.ensureLeafCapacity(moved);
            for (int i = 0; i < moved; i++) {
                final int src = half + i;
                right.entryMaxTimestamp[i] = entryMaxTimestamp[src];
                right.entryCheckpointId[i] = entryCheckpointId[src];
                right.entryDiff[i] = entryDiff[src];
            }
        } else {
            right.resetInternal();
            right.ensureInternalCapacity(moved);
            for (int i = 0; i < moved; i++) {
                final int src = half + i;
                right.childMinMaxTimestamp[i] = childMinMaxTimestamp[src];
                right.childMinCheckpointId[i] = childMinCheckpointId[src];
                right.childSubtreeSum[i] = childSubtreeSum[src];
                right.childSegmentId[i] = childSegmentId[src];
                right.childOffset[i] = childOffset[src];
                right.childLength[i] = childLength[src];
            }
        }
        right.count = moved;
        count = half;
    }

    /**
     * @return the sum of every diff reachable through this node: its own diffs for a
     * leaf, or its children's subtree sums for an internal node. This is the
     * aggregate a parent stores next to this node's reference.
     */
    long subtreeSum() {
        long sum = 0;
        if (leaf) {
            for (int i = 0; i < count; i++) {
                sum += entryDiff[i];
            }
        } else {
            for (int i = 0; i < count; i++) {
                sum += childSubtreeSum[i];
            }
        }
        return sum;
    }

    /**
     * Serializes this node into a fresh page of the {@code writer}, filling
     * {@code out} with a reference to it.
     */
    void writeTo(@NotNull LiveViewCheckpointMetaSegmentWriter writer, @NotNull LiveViewCheckpointPageRef out) {
        final MemoryA payload = writer.beginPage(leaf ? PAGE_KIND_LEAF : PAGE_KIND_INTERNAL);
        payload.putInt(count);
        if (leaf) {
            for (int i = 0; i < count; i++) {
                payload.putLong(entryMaxTimestamp[i]);
                payload.putLong(entryCheckpointId[i]);
                payload.putLong(entryDiff[i]);
            }
        } else {
            for (int i = 0; i < count; i++) {
                payload.putLong(childMinMaxTimestamp[i]);
                payload.putLong(childMinCheckpointId[i]);
                payload.putLong(childSubtreeSum[i]);
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
        childSubtreeSum = grow(childSubtreeSum, cap);
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
        entryDiff = grow(entryDiff, cap);
    }
}
