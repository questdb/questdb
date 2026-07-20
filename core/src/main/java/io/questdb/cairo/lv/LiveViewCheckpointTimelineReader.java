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

import io.questdb.cairo.CairoConfiguration;
import io.questdb.std.Misc;
import io.questdb.std.Transient;
import io.questdb.std.str.Path;
import org.jetbrains.annotations.NotNull;

import java.io.Closeable;

/**
 * Read-only navigator over the persistent copy-on-write timeline B+ tree (design
 * sections 7, 8.1), given a tree root {@link LiveViewCheckpointPageRef} (normally
 * a superblock generation's {@code timelineRootRef}). It resolves:
 * <ul>
 *     <li>{@link #predecessor} - the greatest logical checkpoint whose
 *     {@code maxTimestamp} is strictly below a correction timestamp {@code C},
 *     the O3-repair anchor lookup;</li>
 *     <li>{@link #range} - the roots with {@code C <= maxTimestamp < H}, the
 *     interval an O3 repair re-versions;</li>
 *     <li>{@link #findExact} - a point lookup by full key;</li>
 *     <li>{@link #iterateAll} / {@link #size} - a full ordered scan.</li>
 * </ul>
 * Predecessor and point lookup are {@code O(log N)}; range iteration is
 * {@code O(log N + K)}. Because the tree spans reused subtrees across many
 * metadata segments, the reader keeps a small fixed cache of segment readers so a
 * descent that stays inside one segment does not remap it. Decoding each visited
 * node into a per-level heap image lets navigation hold a parent and child at
 * once and keeps range iteration off the mapped page between steps. This class is
 * not thread safe; create one per navigating thread.
 */
public class LiveViewCheckpointTimelineReader implements Closeable {

    private static final int SEGMENT_CACHE_SIZE = 8;
    private final Path checkpointsDir = new Path();
    private final CairoConfiguration configuration;
    private final LiveViewCheckpointTimelineNode navNode = new LiveViewCheckpointTimelineNode();
    private final LiveViewCheckpointTimelineEntry scratchEntry = new LiveViewCheckpointTimelineEntry();
    private final long[] segReaderSegId = new long[SEGMENT_CACHE_SIZE];
    private final LiveViewCheckpointMetaSegmentReader[] segReaders = new LiveViewCheckpointMetaSegmentReader[SEGMENT_CACHE_SIZE];
    private LiveViewCheckpointTimelineNode[] nodePool = new LiveViewCheckpointTimelineNode[0];
    private int segReaderClock;

    public LiveViewCheckpointTimelineReader(@NotNull CairoConfiguration configuration) {
        this.configuration = configuration;
        for (int i = 0; i < SEGMENT_CACHE_SIZE; i++) {
            segReaderSegId[i] = -1;
        }
    }

    @Override
    public void close() {
        for (int i = 0; i < SEGMENT_CACHE_SIZE; i++) {
            segReaders[i] = Misc.free(segReaders[i]);
            segReaderSegId[i] = -1;
        }
        Misc.free(checkpointsDir);
    }

    /**
     * Point lookup by the full key {@code (maxTimestamp, checkpointId)}. Fills
     * {@code out} and returns true when the exact entry exists.
     */
    public boolean findExact(@NotNull LiveViewCheckpointPageRef rootRef, long maxTimestamp, long checkpointId, @NotNull LiveViewCheckpointTimelineEntry out) {
        if (rootRef.isNull()) {
            return false;
        }
        long seg = rootRef.getSegmentId();
        long off = rootRef.getOffset();
        long len = rootRef.getLength();
        while (true) {
            openAndDecode(seg, off, len, navNode);
            if (navNode.isLeaf()) {
                final int idx = navNode.findEntry(maxTimestamp, checkpointId);
                if (idx < 0) {
                    return false;
                }
                navNode.copyEntryTo(idx, out);
                return true;
            }
            final int ci = navNode.childIndexFor(maxTimestamp, checkpointId);
            seg = navNode.childSegmentId[ci];
            off = navNode.childOffset[ci];
            len = navNode.childLength[ci];
        }
    }

    /**
     * Visits every entry in ascending key order.
     */
    public void iterateAll(@NotNull LiveViewCheckpointPageRef rootRef, @NotNull Visitor visitor) {
        if (rootRef.isNull()) {
            return;
        }
        iterateRec(rootRef.getSegmentId(), rootRef.getOffset(), rootRef.getLength(), visitor, 0);
    }

    /**
     * Finds the greatest composite key in the tree in {@code O(log N)} time.
     * Normal checkpoint sealing uses this to enforce that an append-only cadence
     * boundary is strictly above the current head before it freezes any state.
     */
    public boolean last(@NotNull LiveViewCheckpointPageRef rootRef, @NotNull LiveViewCheckpointTimelineEntry out) {
        if (rootRef.isNull()) {
            return false;
        }
        long segmentId = rootRef.getSegmentId();
        long offset = rootRef.getOffset();
        long length = rootRef.getLength();
        while (true) {
            openAndDecode(segmentId, offset, length, navNode);
            final int count = navNode.count();
            if (count == 0) {
                throw LiveViewCheckpointMetadata.invalid("timeline node is empty");
            }
            if (navNode.isLeaf()) {
                navNode.copyEntryTo(count - 1, out);
                return true;
            }
            final int child = count - 1;
            segmentId = navNode.childSegmentId[child];
            offset = navNode.childOffset[child];
            length = navNode.childLength[child];
        }
    }

    /**
     * Opens the reader against a live view's {@code _checkpoints} directory. The
     * root reference passed to each query pins the generation; this call only
     * fixes where metadata segments live.
     */
    public void of(@Transient @NotNull Path checkpointsDir) {
        this.checkpointsDir.of(checkpointsDir);
        for (int i = 0; i < SEGMENT_CACHE_SIZE; i++) {
            segReaderSegId[i] = -1;
        }
        segReaderClock = 0;
    }

    /**
     * Finds the greatest entry whose {@code maxTimestamp} is strictly less than
     * {@code correctionTimestamp} (design section 7: the strict inequality
     * preserves a complete timestamp tie). Fills {@code out} and returns true when
     * such an entry exists; returns false for an empty tree or when every entry is
     * at or above {@code correctionTimestamp}.
     */
    public boolean predecessor(@NotNull LiveViewCheckpointPageRef rootRef, long correctionTimestamp, @NotNull LiveViewCheckpointTimelineEntry out) {
        if (rootRef.isNull()) {
            return false;
        }
        long seg = rootRef.getSegmentId();
        long off = rootRef.getOffset();
        long len = rootRef.getLength();
        while (true) {
            openAndDecode(seg, off, len, navNode);
            if (navNode.isLeaf()) {
                final int idx = navNode.leafLowerBoundByTimestamp(correctionTimestamp) - 1;
                if (idx < 0) {
                    return false;
                }
                navNode.copyEntryTo(idx, out);
                return true;
            }
            // Last child whose subtree minimum is strictly below the correction
            // timestamp; every later child holds only entries at or above it.
            final int ci = navNode.internalLowerBoundByTimestamp(correctionTimestamp) - 1;
            if (ci < 0) {
                return false;
            }
            seg = navNode.childSegmentId[ci];
            off = navNode.childOffset[ci];
            len = navNode.childLength[ci];
        }
    }

    /**
     * Visits every entry with {@code lowTimestampInclusive <= maxTimestamp <
     * highTimestampExclusive} in ascending key order. This is the {@code [C, H)}
     * interval an O3 repair re-versions; {@code highTimestampExclusive} is a plain
     * timestamp, so an unbounded (EOF) high bound is expressed by the caller as
     * {@code Long.MAX_VALUE} plus a separate tag - it is not this reader's concern.
     */
    public void range(@NotNull LiveViewCheckpointPageRef rootRef, long lowTimestampInclusive, long highTimestampExclusive, @NotNull Visitor visitor) {
        if (rootRef.isNull() || lowTimestampInclusive >= highTimestampExclusive) {
            return;
        }
        rangeRec(rootRef.getSegmentId(), rootRef.getOffset(), rootRef.getLength(), lowTimestampInclusive, highTimestampExclusive, visitor, 0);
    }

    /**
     * Number of children of the root node, or {@code 0} when the root is a leaf or
     * the tree is empty. Together with {@link #rootChildRef} this exposes the
     * top-level shape so a test can assert that an operation reused (kept the same
     * page reference for) the subtrees it did not touch.
     */
    public int rootChildCount(@NotNull LiveViewCheckpointPageRef rootRef) {
        if (rootRef.isNull()) {
            return 0;
        }
        openAndDecode(rootRef.getSegmentId(), rootRef.getOffset(), rootRef.getLength(), navNode);
        return navNode.isLeaf() ? 0 : navNode.count();
    }

    /**
     * Fills {@code out} with the page reference of the root's {@code i}-th child
     * subtree. The root must be an internal node (see {@link #rootChildCount}).
     */
    public void rootChildRef(@NotNull LiveViewCheckpointPageRef rootRef, int i, @NotNull LiveViewCheckpointPageRef out) {
        openAndDecode(rootRef.getSegmentId(), rootRef.getOffset(), rootRef.getLength(), navNode);
        assert !navNode.isLeaf() && i >= 0 && i < navNode.count();
        out.of(navNode.childSegmentId[i], navNode.childOffset[i], (int) navNode.childLength[i]);
    }

    /**
     * Total number of logical checkpoint entries in the tree.
     */
    public long size(@NotNull LiveViewCheckpointPageRef rootRef) {
        if (rootRef.isNull()) {
            return 0;
        }
        return sizeRec(rootRef.getSegmentId(), rootRef.getOffset(), rootRef.getLength(), 0);
    }

    /**
     * Finds the least entry whose {@code maxTimestamp} is greater than or equal to
     * {@code timestamp}. Timestamp ties return their lowest checkpoint id. This is
     * the timestamp-dimension successor paired with {@link #predecessor}; it is
     * {@code O(log N)} because only one candidate search path and, when necessary,
     * its next subtree are visited.
     */
    public boolean successor(@NotNull LiveViewCheckpointPageRef rootRef, long timestamp, @NotNull LiveViewCheckpointTimelineEntry out) {
        if (rootRef.isNull()) {
            return false;
        }
        return successorRec(rootRef.getSegmentId(), rootRef.getOffset(), rootRef.getLength(), timestamp, out, 0);
    }

    /**
     * Decodes the node located at {@code (segmentId, offset, length)} into
     * {@code node}, using the shared segment-reader cache. Package-private so the
     * writer can read old pages while copying a search path.
     */
    void openAndDecode(long segmentId, long offset, long length, @NotNull LiveViewCheckpointTimelineNode node) {
        final LiveViewCheckpointMetaSegmentReader reader = readerFor(segmentId);
        reader.openPageAt(offset, (int) length);
        node.decode(reader);
    }

    private LiveViewCheckpointTimelineNode nodeAt(int depth) {
        if (depth >= nodePool.length) {
            final LiveViewCheckpointTimelineNode[] grown = new LiveViewCheckpointTimelineNode[depth + 1];
            System.arraycopy(nodePool, 0, grown, 0, nodePool.length);
            nodePool = grown;
        }
        LiveViewCheckpointTimelineNode node = nodePool[depth];
        if (node == null) {
            node = new LiveViewCheckpointTimelineNode();
            nodePool[depth] = node;
        }
        return node;
    }

    private void iterateRec(long seg, long off, long len, Visitor visitor, int depth) {
        final LiveViewCheckpointTimelineNode node = nodeAt(depth);
        openAndDecode(seg, off, len, node);
        final int c = node.count();
        if (node.isLeaf()) {
            for (int i = 0; i < c; i++) {
                node.copyEntryTo(i, scratchEntry);
                visitor.onEntry(scratchEntry);
            }
        } else {
            for (int ci = 0; ci < c; ci++) {
                iterateRec(node.childSegmentId[ci], node.childOffset[ci], node.childLength[ci], visitor, depth + 1);
            }
        }
    }

    private LiveViewCheckpointMetaSegmentReader readerFor(long segmentId) {
        for (int i = 0; i < SEGMENT_CACHE_SIZE; i++) {
            if (segReaderSegId[i] == segmentId && segReaders[i] != null) {
                return segReaders[i];
            }
        }
        final int slot = segReaderClock;
        segReaderClock = segReaderClock + 1 == SEGMENT_CACHE_SIZE ? 0 : segReaderClock + 1;
        if (segReaders[slot] == null) {
            segReaders[slot] = new LiveViewCheckpointMetaSegmentReader(configuration);
        }
        segReaders[slot].of(checkpointsDir, segmentId);
        segReaderSegId[slot] = segmentId;
        return segReaders[slot];
    }

    private void rangeRec(long seg, long off, long len, long cLo, long hHi, Visitor visitor, int depth) {
        final LiveViewCheckpointTimelineNode node = nodeAt(depth);
        openAndDecode(seg, off, len, node);
        final int c = node.count();
        if (node.isLeaf()) {
            for (int i = node.leafLowerBoundByTimestamp(cLo); i < c; i++) {
                if (node.entryMaxTimestamp[i] >= hHi) {
                    break;
                }
                node.copyEntryTo(i, scratchEntry);
                visitor.onEntry(scratchEntry);
            }
        } else {
            for (int ci = 0; ci < c; ci++) {
                if (node.childMinMaxTimestamp[ci] >= hHi) {
                    // This child and every later one hold only entries >= hHi.
                    break;
                }
                // Skip a child entirely below cLo: it is entirely below when its
                // successor's minimum timestamp is still below cLo.
                if (ci + 1 == c || node.childMinMaxTimestamp[ci + 1] >= cLo) {
                    rangeRec(node.childSegmentId[ci], node.childOffset[ci], node.childLength[ci], cLo, hHi, visitor, depth + 1);
                }
            }
        }
    }

    private long sizeRec(long seg, long off, long len, int depth) {
        final LiveViewCheckpointTimelineNode node = nodeAt(depth);
        openAndDecode(seg, off, len, node);
        final int c = node.count();
        if (node.isLeaf()) {
            return c;
        }
        long total = 0;
        for (int ci = 0; ci < c; ci++) {
            total += sizeRec(node.childSegmentId[ci], node.childOffset[ci], node.childLength[ci], depth + 1);
        }
        return total;
    }

    private boolean successorRec(long seg, long off, long len, long timestamp, LiveViewCheckpointTimelineEntry out, int depth) {
        final LiveViewCheckpointTimelineNode node = nodeAt(depth);
        openAndDecode(seg, off, len, node);
        final int c = node.count();
        if (node.isLeaf()) {
            final int index = node.leafLowerBoundByTimestamp(timestamp);
            if (index == c) {
                return false;
            }
            node.copyEntryTo(index, out);
            return true;
        }
        final int lowerBound = node.internalLowerBoundByTimestamp(timestamp);
        final int firstChild = Math.max(0, lowerBound - 1);
        for (int i = firstChild; i < c; i++) {
            if (successorRec(node.childSegmentId[i], node.childOffset[i], node.childLength[i], timestamp, out, depth + 1)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Callback for {@link #range} and {@link #iterateAll}. The {@code entry} is a
     * reused flyweight valid only for the duration of the call; copy it to retain.
     */
    @FunctionalInterface
    public interface Visitor {
        void onEntry(LiveViewCheckpointTimelineEntry entry);
    }
}
