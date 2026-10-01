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
 * Read-only navigator over the persistent copy-on-write row-position delta
 * tree, given a tree root {@link LiveViewCheckpointPageRef} (normally a
 * superblock generation's {@code rowPositionDeltaRootRef}). Its primary query
 * is:
 * <ul>
 *     <li>{@link #prefixSum} - the sum of every difference whose key is {@code <=}
 *     the query key. A logical checkpoint's effective cumulative
 *     {@code lvRowPosition} is its stored {@code baseLvRowPosition} plus this prefix
 *     sum (see {@link #effectivePosition}).</li>
 * </ul>
 * The descent adds, at each internal node, the subtree sums of every child strictly
 * left of the descent child (their keys are all {@code <} the descent child's
 * minimum, hence {@code <=} the query key), then recurses into the child that may
 * straddle the query key; a leaf sums the diffs of the entries at or below the query
 * key. This is {@code O(log N)}. As with the timeline reader, a small fixed cache of
 * segment readers keeps a descent that stays inside one metadata segment from
 * remapping it, and each visited node decodes into a per-level heap image. This
 * class is not thread safe; create one per navigating thread.
 */
public class LiveViewCheckpointRowPositionDeltaReader implements Closeable {

    private static final int SEGMENT_CACHE_SIZE = 8;
    private final Path checkpointsDir = new Path();
    private final CairoConfiguration configuration;
    private final LiveViewCheckpointRowPositionDeltaNode navNode = new LiveViewCheckpointRowPositionDeltaNode();
    private final long[] segReaderSegId = new long[SEGMENT_CACHE_SIZE];
    private final LiveViewCheckpointMetaSegmentReader[] segReaders = new LiveViewCheckpointMetaSegmentReader[SEGMENT_CACHE_SIZE];
    private LiveViewCheckpointRowPositionDeltaNode[] nodePool = new LiveViewCheckpointRowPositionDeltaNode[0];
    private int segReaderClock;

    public LiveViewCheckpointRowPositionDeltaReader(@NotNull CairoConfiguration configuration) {
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
     * Unmaps every cached metadata segment while keeping the readers themselves,
     * so a reader that outlives one restore holds no mapping into files a later
     * retire, repair or compaction deletes.
     */
    public void detach() {
        for (int i = 0; i < SEGMENT_CACHE_SIZE; i++) {
            if (segReaders[i] != null) {
                segReaders[i].close();
            }
            segReaderSegId[i] = -1;
        }
        segReaderClock = 0;
    }

    /**
     * @return the effective cumulative {@code lvRowPosition} of {@code entry} in the
     * generation rooted at {@code rootRef}: the entry's stored
     * {@code baseLvRowPosition} plus the prefix sum at the entry's search key.
     * This is the recovery coordinate a suffix root reports after an O3 repair
     * shifted it without rewriting its leaf.
     */
    public long effectivePosition(@NotNull LiveViewCheckpointPageRef rootRef, @NotNull LiveViewCheckpointTimelineEntry entry) {
        return entry.baseLvRowPosition + prefixSum(rootRef, entry.maxTimestamp, entry.checkpointId);
    }

    /**
     * Visits every difference entry in ascending key order.
     */
    public void iterateAll(@NotNull LiveViewCheckpointPageRef rootRef, @NotNull Visitor visitor) {
        if (rootRef.isNull()) {
            return;
        }
        iterateRec(rootRef.getSegmentId(), rootRef.getOffset(), rootRef.getLength(), visitor, 0);
    }

    /**
     * Opens the reader against a live view's {@code _checkpoints} directory. The
     * root reference passed to each query pins the generation; this call only fixes
     * where metadata segments live.
     */
    public void of(@Transient @NotNull Path checkpointsDir) {
        this.checkpointsDir.of(checkpointsDir);
        for (int i = 0; i < SEGMENT_CACHE_SIZE; i++) {
            segReaderSegId[i] = -1;
        }
        segReaderClock = 0;
    }

    /**
     * Sum of every difference whose key {@code (maxTimestamp, checkpointId)} is
     * {@code <=} the query key. An empty tree returns {@code 0}. Because a suffix
     * range-add places its breakpoint at the first suffix key, every suffix root
     * ({@code key >= H}) sees the added delta and every prefix root does not.
     */
    public long prefixSum(@NotNull LiveViewCheckpointPageRef rootRef, long maxTimestamp, long checkpointId) {
        if (rootRef.isNull()) {
            return 0;
        }
        long seg = rootRef.getSegmentId();
        long off = rootRef.getOffset();
        long len = rootRef.getLength();
        long sum = 0;
        while (true) {
            openAndDecode(seg, off, len, navNode);
            if (navNode.isLeaf()) {
                final int upper = navNode.leafUpperBound(maxTimestamp, checkpointId);
                for (int i = 0; i < upper; i++) {
                    sum += navNode.entryDiff[i];
                }
                return sum;
            }
            final int ci = navNode.childIndexFor(maxTimestamp, checkpointId);
            // Every child strictly left of the descent child holds only keys below
            // the descent child's minimum, hence <= the query key: add their full
            // subtree sums in O(1) per child.
            for (int i = 0; i < ci; i++) {
                sum += navNode.childSubtreeSum[i];
            }
            seg = navNode.childSegmentId[ci];
            off = navNode.childOffset[ci];
            len = navNode.childLength[ci];
        }
    }

    /**
     * Number of children of the root node, or {@code 0} when the root is a leaf or
     * the tree is empty. Together with {@link #rootChildRef} this exposes the
     * top-level shape so a test can assert that a suffix range-add reused the
     * subtrees it did not touch.
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
     * Total number of difference breakpoints in the tree (one per suffix range-add
     * key, not per checkpoint).
     */
    public long size(@NotNull LiveViewCheckpointPageRef rootRef) {
        if (rootRef.isNull()) {
            return 0;
        }
        return sizeRec(rootRef.getSegmentId(), rootRef.getOffset(), rootRef.getLength(), 0);
    }

    /**
     * Decodes the node located at {@code (segmentId, offset, length)} into
     * {@code node}, using the shared segment-reader cache. Package-private so the
     * writer can read old pages while copying a search path.
     */
    void openAndDecode(long segmentId, long offset, long length, @NotNull LiveViewCheckpointRowPositionDeltaNode node) {
        final LiveViewCheckpointMetaSegmentReader reader = readerFor(segmentId);
        reader.openPageAt(offset, (int) length);
        node.decode(reader);
    }

    private LiveViewCheckpointRowPositionDeltaNode nodeAt(int depth) {
        if (depth >= nodePool.length) {
            final LiveViewCheckpointRowPositionDeltaNode[] grown = new LiveViewCheckpointRowPositionDeltaNode[depth + 1];
            System.arraycopy(nodePool, 0, grown, 0, nodePool.length);
            nodePool = grown;
        }
        LiveViewCheckpointRowPositionDeltaNode node = nodePool[depth];
        if (node == null) {
            node = new LiveViewCheckpointRowPositionDeltaNode();
            nodePool[depth] = node;
        }
        return node;
    }

    private void iterateRec(long seg, long off, long len, Visitor visitor, int depth) {
        final LiveViewCheckpointRowPositionDeltaNode node = nodeAt(depth);
        openAndDecode(seg, off, len, node);
        final int c = node.count();
        if (node.isLeaf()) {
            for (int i = 0; i < c; i++) {
                visitor.onEntry(node.entryMaxTimestamp[i], node.entryCheckpointId[i], node.entryDiff[i]);
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
        // Invalidate the slot BEFORE the open. of() closes and resets the reader up front and can
        // then throw, which would otherwise leave the slot still advertising the previous, healthy
        // segment id against a closed reader - so one corrupt segment poisons a healthy one, and a
        // later lookup can escalate that into "no usable root".
        segReaderSegId[slot] = -1;
        segReaders[slot].of(checkpointsDir, segmentId);
        segReaderSegId[slot] = segmentId;
        return segReaders[slot];
    }

    private long sizeRec(long seg, long off, long len, int depth) {
        final LiveViewCheckpointRowPositionDeltaNode node = nodeAt(depth);
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

    /**
     * Callback for {@link #iterateAll}, receiving one difference breakpoint's key
     * and diff value.
     */
    @FunctionalInterface
    public interface Visitor {
        void onEntry(long maxTimestamp, long checkpointId, long diff);
    }
}
