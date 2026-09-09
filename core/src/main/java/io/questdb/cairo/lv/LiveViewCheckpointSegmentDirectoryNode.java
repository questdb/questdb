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

import static io.questdb.cairo.lv.LiveViewCheckpointSegmentDirectory.ENTRY_FILE_LENGTH_OFFSET;
import static io.questdb.cairo.lv.LiveViewCheckpointSegmentDirectory.ENTRY_KIND_OFFSET;
import static io.questdb.cairo.lv.LiveViewCheckpointSegmentDirectory.ENTRY_REFERENCE_COUNT_OFFSET;
import static io.questdb.cairo.lv.LiveViewCheckpointSegmentDirectory.ENTRY_RETIRE_GENERATION_OFFSET;
import static io.questdb.cairo.lv.LiveViewCheckpointSegmentDirectory.ENTRY_SEGMENT_ID_OFFSET;
import static io.questdb.cairo.lv.LiveViewCheckpointSegmentDirectory.INTERNAL_CHILD_MIN_SEGMENT_ID_OFFSET;
import static io.questdb.cairo.lv.LiveViewCheckpointSegmentDirectory.INTERNAL_CHILD_REF_OFFSET;
import static io.questdb.cairo.lv.LiveViewCheckpointSegmentDirectory.INTERNAL_CHILD_STRIDE;
import static io.questdb.cairo.lv.LiveViewCheckpointSegmentDirectory.LEAF_ENTRY_STRIDE;
import static io.questdb.cairo.lv.LiveViewCheckpointSegmentDirectory.NODE_COUNT_OFFSET;
import static io.questdb.cairo.lv.LiveViewCheckpointSegmentDirectory.NODE_HEADER_SIZE;
import static io.questdb.cairo.lv.LiveViewCheckpointSegmentDirectory.PAGE_KIND_INTERNAL;
import static io.questdb.cairo.lv.LiveViewCheckpointSegmentDirectory.PAGE_KIND_LEAF;
import static io.questdb.cairo.lv.LiveViewCheckpointSegmentDirectory.RETIRE_GENERATION_NONE;
import static io.questdb.cairo.lv.LiveViewCheckpointSegmentDirectory.SEGMENT_KIND_BOUNDARY;
import static io.questdb.cairo.lv.LiveViewCheckpointSegmentDirectory.SEGMENT_KIND_DATA;
import static io.questdb.cairo.lv.LiveViewCheckpointSegmentDirectory.SEGMENT_KIND_META;

/**
 * A reusable in-heap image of one segment directory B+ tree node (leaf or
 * internal), decoupled from any file mapping. It both decodes an on-page node
 * from a positioned {@link LiveViewCheckpointMetaSegmentReader} and serializes a
 * (possibly mutated) node into a {@link LiveViewCheckpointMetaSegmentWriter}.
 * <p>
 * Decoding validates the page kind, the declared count against the payload
 * length, ascending unique segment ids, and the retirement state each entry
 * carries, so a caller never reads a field the page did not prove. Arrays are
 * grown on demand and reused across nodes; one instance is not thread safe and is
 * pooled per tree level by its owner.
 */
final class LiveViewCheckpointSegmentDirectoryNode {

    long[] childLength = new long[0];
    // Internal child columns (valid when leaf == false).
    long[] childMinSegmentId = new long[0];
    long[] childOffset = new long[0];
    long[] childSegmentId = new long[0];
    long[] entryFileLength = new long[0];
    long[] entryKind = new long[0];
    long[] entryReferenceCount = new long[0];
    long[] entryRetireGeneration = new long[0];
    // Leaf entry columns (valid when leaf == true).
    long[] entrySegmentId = new long[0];
    private int count;
    private boolean leaf;

    /**
     * Appends a child record (used when building a fresh internal node).
     */
    void appendChild(long minSegmentId, long segmentId, long offset, long length) {
        assert !leaf;
        ensureInternalCapacity(count + 1);
        childMinSegmentId[count] = minSegmentId;
        childSegmentId[count] = segmentId;
        childOffset[count] = offset;
        childLength[count] = length;
        count++;
    }

    /**
     * Locates the child whose subtree may contain {@code segmentId}: the
     * rightmost child whose minimum key is {@code <=} the key, clamped to child 0
     * when the key is below every child minimum (an insert of a new global
     * minimum descends the first child).
     */
    int childIndexFor(long segmentId) {
        assert !leaf && count > 0;
        int lo = 0;
        int hi = count - 1;
        int res = 0;
        while (lo <= hi) {
            final int mid = (lo + hi) >>> 1;
            if (childMinSegmentId[mid] <= segmentId) {
                res = mid;
                lo = mid + 1;
            } else {
                hi = mid - 1;
            }
        }
        return res;
    }

    void copyEntryTo(int i, @NotNull LiveViewCheckpointSegmentDirectoryEntry out) {
        assert leaf;
        out.of(entrySegmentId[i], entryFileLength[i], entryReferenceCount[i], entryRetireGeneration[i], entryKind[i]);
    }

    /**
     * Copies records {@code [from, to)} of this node into {@code dst}, which must
     * be a different instance. Used to break an over-capacity node into pieces.
     */
    void copyRangeInto(@NotNull LiveViewCheckpointSegmentDirectoryNode dst, int from, int to) {
        assert dst != this;
        final int moved = to - from;
        if (leaf) {
            dst.resetLeaf();
            dst.ensureLeafCapacity(moved);
            for (int i = 0; i < moved; i++) {
                final int src = from + i;
                dst.entrySegmentId[i] = entrySegmentId[src];
                dst.entryFileLength[i] = entryFileLength[src];
                dst.entryReferenceCount[i] = entryReferenceCount[src];
                dst.entryRetireGeneration[i] = entryRetireGeneration[src];
                dst.entryKind[i] = entryKind[src];
            }
        } else {
            dst.resetInternal();
            dst.ensureInternalCapacity(moved);
            for (int i = 0; i < moved; i++) {
                final int src = from + i;
                dst.childMinSegmentId[i] = childMinSegmentId[src];
                dst.childSegmentId[i] = childSegmentId[src];
                dst.childOffset[i] = childOffset[src];
                dst.childLength[i] = childLength[src];
            }
        }
        dst.count = moved;
    }

    int count() {
        return count;
    }

    /**
     * Decodes the page the {@code reader} is currently positioned on (after a
     * successful {@code openPage}/{@code openPageAt}) into this image.
     */
    void decode(@NotNull LiveViewCheckpointMetaSegmentReader reader) {
        final int kind = reader.getPageKind();
        final int payloadLength = reader.getPagePayloadLength();
        if (payloadLength < NODE_HEADER_SIZE) {
            throw invalid("segment directory node payload too small").put(", payloadLength=").put(payloadLength);
        }
        final int c = reader.getInt(NODE_COUNT_OFFSET);
        if (c < 0) {
            throw invalid("segment directory node count negative").put(", count=").put(c);
        }
        if (kind == PAGE_KIND_LEAF) {
            decodeLeaf(reader, c, payloadLength);
        } else if (kind == PAGE_KIND_INTERNAL) {
            decodeInternal(reader, c, payloadLength);
        } else {
            throw invalid("segment directory node page kind unknown").put(", kind=").put(kind);
        }
        count = c;
    }

    /**
     * Binary-searches a leaf for {@code segmentId}, returning its index or
     * {@code -1} when absent.
     */
    int findEntry(long segmentId) {
        assert leaf;
        int lo = 0;
        int hi = count - 1;
        while (lo <= hi) {
            final int mid = (lo + hi) >>> 1;
            final long value = entrySegmentId[mid];
            if (value < segmentId) {
                lo = mid + 1;
            } else if (value > segmentId) {
                hi = mid - 1;
            } else {
                return mid;
            }
        }
        return -1;
    }

    /**
     * Inserts an entry at leaf position {@code pos}, shifting later entries up.
     */
    void insertEntryAt(int pos, long segmentId, long fileLength, long referenceCount, long retireGeneration, long kind) {
        assert leaf;
        ensureLeafCapacity(count + 1);
        for (int i = count; i > pos; i--) {
            entrySegmentId[i] = entrySegmentId[i - 1];
            entryFileLength[i] = entryFileLength[i - 1];
            entryReferenceCount[i] = entryReferenceCount[i - 1];
            entryRetireGeneration[i] = entryRetireGeneration[i - 1];
            entryKind[i] = entryKind[i - 1];
        }
        entrySegmentId[pos] = segmentId;
        entryFileLength[pos] = fileLength;
        entryReferenceCount[pos] = referenceCount;
        entryRetireGeneration[pos] = retireGeneration;
        entryKind[pos] = kind;
        count++;
    }

    boolean isLeaf() {
        return leaf;
    }

    /**
     * Position at which {@code segmentId} sorts: the first index whose key is
     * {@code >=} it. For an absent key that is the insertion point.
     */
    int leafInsertPosition(long segmentId) {
        assert leaf;
        int lo = 0;
        int hi = count;
        while (lo < hi) {
            final int mid = (lo + hi) >>> 1;
            if (entrySegmentId[mid] < segmentId) {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        return lo;
    }

    /**
     * Minimum key held anywhere in this node's subtree, which is the separator a
     * parent stores for it.
     */
    long minKey() {
        assert count > 0;
        return leaf ? entrySegmentId[0] : childMinSegmentId[0];
    }

    /**
     * Removes the entry at leaf position {@code pos}, shifting later entries
     * down. A leaf may go empty this way; its owner then writes no page for it
     * and the parent keeps no child reference to it.
     */
    void removeEntryAt(int pos) {
        assert leaf;
        for (int i = pos; i < count - 1; i++) {
            entrySegmentId[i] = entrySegmentId[i + 1];
            entryFileLength[i] = entryFileLength[i + 1];
            entryReferenceCount[i] = entryReferenceCount[i + 1];
            entryRetireGeneration[i] = entryRetireGeneration[i + 1];
            entryKind[i] = entryKind[i + 1];
        }
        count--;
    }

    /**
     * Replaces the mutable values of the entry at {@code pos}. The key and the
     * kind are preserved: a publication re-versions counts, never identities.
     */
    void replaceEntryPayloadAt(int pos, long fileLength, long referenceCount, long retireGeneration) {
        assert leaf;
        entryFileLength[pos] = fileLength;
        entryReferenceCount[pos] = referenceCount;
        entryRetireGeneration[pos] = retireGeneration;
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
     * Serializes this node into a fresh page of the {@code writer}, filling
     * {@code out} with a reference to it.
     */
    void writeTo(@NotNull LiveViewCheckpointMetaSegmentWriter writer, @NotNull LiveViewCheckpointPageRef out) {
        final MemoryA payload = writer.beginPage(leaf ? PAGE_KIND_LEAF : PAGE_KIND_INTERNAL);
        payload.putInt(count);
        if (leaf) {
            for (int i = 0; i < count; i++) {
                payload.putLong(entrySegmentId[i]);
                payload.putLong(entryFileLength[i]);
                payload.putLong(entryReferenceCount[i]);
                payload.putLong(entryRetireGeneration[i]);
                payload.putLong(entryKind[i]);
            }
        } else {
            for (int i = 0; i < count; i++) {
                payload.putLong(childMinSegmentId[i]);
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

    private void decodeInternal(LiveViewCheckpointMetaSegmentReader reader, int c, int payloadLength) {
        leaf = false;
        final long need = (long) NODE_HEADER_SIZE + (long) c * INTERNAL_CHILD_STRIDE;
        if (need != payloadLength) {
            throw invalid("segment directory node payload length mismatch")
                    .put(" [count=").put(c)
                    .put(", expected=").put(need)
                    .put(", actual=").put(payloadLength)
                    .put(']');
        }
        ensureInternalCapacity(c);
        long previousSegmentId = -1;
        long base = NODE_HEADER_SIZE;
        for (int i = 0; i < c; i++, base += INTERNAL_CHILD_STRIDE) {
            final long minSegmentId = reader.getLong(base + INTERNAL_CHILD_MIN_SEGMENT_ID_OFFSET);
            if (minSegmentId < 0 || minSegmentId <= previousSegmentId) {
                throw invalid("segment directory ids not strictly increasing")
                        .put(" [previous=").put(previousSegmentId)
                        .put(", current=").put(minSegmentId)
                        .put(']');
            }
            childMinSegmentId[i] = minSegmentId;
            childSegmentId[i] = reader.getLong(base + INTERNAL_CHILD_REF_OFFSET);
            childOffset[i] = reader.getLong(base + INTERNAL_CHILD_REF_OFFSET + Long.BYTES);
            childLength[i] = reader.getInt(base + INTERNAL_CHILD_REF_OFFSET + Long.BYTES + Long.BYTES);
            previousSegmentId = minSegmentId;
        }
    }

    private void decodeLeaf(LiveViewCheckpointMetaSegmentReader reader, int c, int payloadLength) {
        leaf = true;
        final long need = (long) NODE_HEADER_SIZE + (long) c * LEAF_ENTRY_STRIDE;
        if (need != payloadLength) {
            throw invalid("segment directory node payload length mismatch")
                    .put(" [count=").put(c)
                    .put(", expected=").put(need)
                    .put(", actual=").put(payloadLength)
                    .put(']');
        }
        ensureLeafCapacity(c);
        long previousSegmentId = -1;
        long base = NODE_HEADER_SIZE;
        for (int i = 0; i < c; i++, base += LEAF_ENTRY_STRIDE) {
            final long segmentId = reader.getLong(base + ENTRY_SEGMENT_ID_OFFSET);
            final long fileLength = reader.getLong(base + ENTRY_FILE_LENGTH_OFFSET);
            final long referenceCount = reader.getLong(base + ENTRY_REFERENCE_COUNT_OFFSET);
            final long retireGeneration = reader.getLong(base + ENTRY_RETIRE_GENERATION_OFFSET);
            final long kind = reader.getLong(base + ENTRY_KIND_OFFSET);
            if (segmentId < 0 || segmentId <= previousSegmentId) {
                throw invalid("segment directory ids not strictly increasing")
                        .put(" [previous=").put(previousSegmentId)
                        .put(", current=").put(segmentId)
                        .put(']');
            }
            if (fileLength <= 0 || referenceCount < 0) {
                throw invalid("segment directory entry value invalid")
                        .put(" [segmentId=").put(segmentId)
                        .put(", fileLength=").put(fileLength)
                        .put(", referenceCount=").put(referenceCount)
                        .put(']');
            }
            if ((referenceCount == 0 && retireGeneration < 0)
                    || (referenceCount > 0 && retireGeneration != RETIRE_GENERATION_NONE)) {
                throw invalid("segment directory retirement state invalid")
                        .put(" [segmentId=").put(segmentId)
                        .put(", referenceCount=").put(referenceCount)
                        .put(", retireGeneration=").put(retireGeneration)
                        .put(']');
            }
            if (kind != SEGMENT_KIND_DATA && kind != SEGMENT_KIND_META && kind != SEGMENT_KIND_BOUNDARY) {
                throw invalid("segment directory entry kind unknown")
                        .put(" [segmentId=").put(segmentId)
                        .put(", kind=").put(kind)
                        .put(']');
            }
            entrySegmentId[i] = segmentId;
            entryFileLength[i] = fileLength;
            entryReferenceCount[i] = referenceCount;
            entryRetireGeneration[i] = retireGeneration;
            entryKind[i] = kind;
            previousSegmentId = segmentId;
        }
    }

    private void ensureInternalCapacity(int n) {
        if (childMinSegmentId.length >= n) {
            return;
        }
        final int cap = Math.max(n, Math.max(4, childMinSegmentId.length * 2));
        childMinSegmentId = grow(childMinSegmentId, cap);
        childSegmentId = grow(childSegmentId, cap);
        childOffset = grow(childOffset, cap);
        childLength = grow(childLength, cap);
    }

    private void ensureLeafCapacity(int n) {
        if (entrySegmentId.length >= n) {
            return;
        }
        final int cap = Math.max(n, Math.max(4, entrySegmentId.length * 2));
        entrySegmentId = grow(entrySegmentId, cap);
        entryFileLength = grow(entryFileLength, cap);
        entryReferenceCount = grow(entryReferenceCount, cap);
        entryRetireGeneration = grow(entryRetireGeneration, cap);
        entryKind = grow(entryKind, cap);
    }
}
