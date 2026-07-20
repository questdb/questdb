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
import io.questdb.cairo.CairoException;
import io.questdb.std.Misc;
import io.questdb.std.Transient;
import io.questdb.std.str.Path;
import org.jetbrains.annotations.NotNull;

import java.io.Closeable;

/**
 * Copy-on-write publisher for the row-position delta tree (design sections 7, 10.3,
 * 12.5). Each mutation reads the prior generation's tree through a {@link
 * LiveViewCheckpointRowPositionDeltaReader}, writes only the changed pages into one
 * fresh metadata segment, and returns the new tree root reference; the caller
 * commits the generation by publishing that root as {@code rowPositionDeltaRootRef}
 * in a superblock slot.
 * <p>
 * The sole mutation is {@link #suffixAdd}: an O3 repair's suffix range-add over
 * {@code [H, +inf)} is one difference-array point add at the breakpoint key
 * {@code H} - {@code diff[H] += delta}. It path-copies the {@code O(log N)} spine,
 * accumulating into an existing breakpoint or inserting a new one (splitting nodes
 * that overflow), and reuses every untouched subtree by its existing page reference.
 * Each ancestor's stored subtree sum for the descended child is updated to the
 * child's recomputed sum, so a later {@link LiveViewCheckpointRowPositionDeltaReader#prefixSum}
 * stays correct without walking the suffix. Metadata pages are immutable and never
 * rewritten in place, so a reader of the prior generation keeps walking the old
 * paths. The instance is reusable across mutations and is not thread safe.
 */
public class LiveViewCheckpointRowPositionDeltaWriter implements Closeable {

    private final Path checkpointsDir = new Path();
    private final int internalCapacity;
    private final int leafCapacity;
    private final LiveViewCheckpointRowPositionDeltaReader reader;
    private final LiveViewCheckpointMetaSegmentWriter segmentWriter;
    private long lastSegmentBytes;
    private int lastSegmentPageCount;
    private LiveViewCheckpointRowPositionDeltaNode[] leftPool = new LiveViewCheckpointRowPositionDeltaNode[0];
    private final LiveViewCheckpointRowPositionDeltaNode newRootBuilder = new LiveViewCheckpointRowPositionDeltaNode();
    private AddResult[] resultPool = new AddResult[0];
    private LiveViewCheckpointRowPositionDeltaNode[] rightPool = new LiveViewCheckpointRowPositionDeltaNode[0];

    public LiveViewCheckpointRowPositionDeltaWriter(@NotNull CairoConfiguration configuration) {
        this(configuration, 64, 64);
    }

    public LiveViewCheckpointRowPositionDeltaWriter(@NotNull CairoConfiguration configuration, int leafCapacity, int internalCapacity) {
        if (leafCapacity < 2 || internalCapacity < 2) {
            throw CairoException.critical(0)
                    .put("live view checkpoint row position delta node capacity must be at least 2, leaf=")
                    .put(leafCapacity).put(", internal=").put(internalCapacity);
        }
        this.leafCapacity = leafCapacity;
        this.internalCapacity = internalCapacity;
        this.reader = new LiveViewCheckpointRowPositionDeltaReader(configuration);
        this.segmentWriter = new LiveViewCheckpointMetaSegmentWriter(configuration);
    }

    @Override
    public void close() {
        Misc.free(reader);
        Misc.free(segmentWriter);
        Misc.free(checkpointsDir);
    }

    /**
     * @return byte size of the metadata segment written by the last mutation
     */
    public long getLastSegmentBytes() {
        return lastSegmentBytes;
    }

    /**
     * @return number of new metadata pages the last mutation wrote (its
     * copy-on-write cost); far below the total node count when subtrees are reused
     */
    public int getLastSegmentPageCount() {
        return lastSegmentPageCount;
    }

    /**
     * Points the writer at a live view's {@code _checkpoints} directory.
     */
    public void of(@Transient @NotNull Path checkpointsDir) {
        this.checkpointsDir.of(checkpointsDir);
        reader.of(checkpointsDir);
    }

    /**
     * Adds {@code delta} to the difference at breakpoint key {@code (fromMaxTimestamp,
     * fromCheckpointId)} in the tree rooted at {@code oldRoot} - null/empty for a
     * fresh tree - and fills {@code newRootOut} with the new tree root. This is one
     * O3 suffix range-add: after it, {@code prefixSum(key)} increases by {@code delta}
     * for every key at or above the breakpoint. All new pages land in a new metadata
     * segment {@code newSegmentId}, which must be unused.
     */
    public void suffixAdd(
            @NotNull LiveViewCheckpointPageRef oldRoot,
            long fromMaxTimestamp,
            long fromCheckpointId,
            long delta,
            long newSegmentId,
            @NotNull LiveViewCheckpointPageRef newRootOut
    ) {
        beginSegment(newSegmentId);
        if (oldRoot.isNull()) {
            final LiveViewCheckpointRowPositionDeltaNode leaf = leftAt(0);
            leaf.resetLeaf();
            leaf.insertEntryAt(0, fromMaxTimestamp, fromCheckpointId, delta);
            writePage(leaf, newRootOut);
        } else {
            addRec(oldRoot.getSegmentId(), oldRoot.getOffset(), oldRoot.getLength(), fromMaxTimestamp, fromCheckpointId, delta, 0);
            final AddResult root = resultAt(0);
            if (root.split) {
                newRootBuilder.resetInternal();
                newRootBuilder.appendChild(root.leftMinTs, root.leftMinId, root.leftSubtreeSum, root.leftRef.getSegmentId(), root.leftRef.getOffset(), root.leftRef.getLength());
                newRootBuilder.appendChild(root.rightMinTs, root.rightMinId, root.rightSubtreeSum, root.rightRef.getSegmentId(), root.rightRef.getOffset(), root.rightRef.getLength());
                writePage(newRootBuilder, newRootOut);
            } else {
                newRootOut.of(root.leftRef.getSegmentId(), root.leftRef.getOffset(), root.leftRef.getLength());
            }
        }
        commitSegment();
    }

    private void addRec(long seg, long off, long len, long ts, long id, long delta, int depth) {
        final LiveViewCheckpointRowPositionDeltaNode node = leftAt(depth);
        reader.openAndDecode(seg, off, len, node);
        final AddResult res = resultAt(depth);
        if (node.isLeaf()) {
            final int pos = node.findEntry(ts, id);
            if (pos >= 0) {
                node.addToLeafDiffAt(pos, delta);
            } else {
                node.insertEntryAt(node.leafInsertPosition(ts, id), ts, id, delta);
            }
            finishNode(node, res, depth, true);
            return;
        }
        final int ci = node.childIndexFor(ts, id);
        addRec(node.childSegmentId[ci], node.childOffset[ci], node.childLength[ci], ts, id, delta, depth + 1);
        final AddResult child = resultAt(depth + 1);
        node.setChildEntry(ci, child.leftMinTs, child.leftMinId, child.leftSubtreeSum, child.leftRef.getSegmentId(), child.leftRef.getOffset(), child.leftRef.getLength());
        if (child.split) {
            node.insertChildAt(ci + 1, child.rightMinTs, child.rightMinId, child.rightSubtreeSum, child.rightRef.getSegmentId(), child.rightRef.getOffset(), child.rightRef.getLength());
        }
        finishNode(node, res, depth, false);
    }

    private void beginSegment(long segmentId) {
        segmentWriter.of(checkpointsDir, segmentId);
        lastSegmentPageCount = 0;
    }

    private void commitSegment() {
        lastSegmentBytes = segmentWriter.commit();
    }

    private void finishNode(LiveViewCheckpointRowPositionDeltaNode node, AddResult res, int depth, boolean leaf) {
        final int capacity = leaf ? leafCapacity : internalCapacity;
        if (node.count() <= capacity) {
            writePage(node, res.leftRef);
            res.leftMinTs = leaf ? node.entryMaxTimestamp[0] : node.childMinMaxTimestamp[0];
            res.leftMinId = leaf ? node.entryCheckpointId[0] : node.childMinCheckpointId[0];
            res.leftSubtreeSum = node.subtreeSum();
            res.split = false;
            return;
        }
        // The node overflowed: move its upper half into a fresh sibling and write
        // both halves. The sibling's minimum key is the promoted separator.
        final LiveViewCheckpointRowPositionDeltaNode right = rightAt(depth);
        node.splitInto(right);
        writePage(node, res.leftRef);
        res.leftMinTs = leaf ? node.entryMaxTimestamp[0] : node.childMinMaxTimestamp[0];
        res.leftMinId = leaf ? node.entryCheckpointId[0] : node.childMinCheckpointId[0];
        res.leftSubtreeSum = node.subtreeSum();
        writePage(right, res.rightRef);
        res.rightMinTs = leaf ? right.entryMaxTimestamp[0] : right.childMinMaxTimestamp[0];
        res.rightMinId = leaf ? right.entryCheckpointId[0] : right.childMinCheckpointId[0];
        res.rightSubtreeSum = right.subtreeSum();
        res.split = true;
    }

    private LiveViewCheckpointRowPositionDeltaNode leftAt(int depth) {
        if (depth >= leftPool.length) {
            leftPool = growNodes(leftPool, depth + 1);
        }
        LiveViewCheckpointRowPositionDeltaNode node = leftPool[depth];
        if (node == null) {
            node = new LiveViewCheckpointRowPositionDeltaNode();
            leftPool[depth] = node;
        }
        return node;
    }

    private AddResult resultAt(int depth) {
        if (depth >= resultPool.length) {
            final AddResult[] grown = new AddResult[depth + 1];
            System.arraycopy(resultPool, 0, grown, 0, resultPool.length);
            resultPool = grown;
        }
        AddResult res = resultPool[depth];
        if (res == null) {
            res = new AddResult();
            resultPool[depth] = res;
        }
        return res;
    }

    private LiveViewCheckpointRowPositionDeltaNode rightAt(int depth) {
        if (depth >= rightPool.length) {
            rightPool = growNodes(rightPool, depth + 1);
        }
        LiveViewCheckpointRowPositionDeltaNode node = rightPool[depth];
        if (node == null) {
            node = new LiveViewCheckpointRowPositionDeltaNode();
            rightPool[depth] = node;
        }
        return node;
    }

    private void writePage(LiveViewCheckpointRowPositionDeltaNode node, LiveViewCheckpointPageRef out) {
        node.writeTo(segmentWriter, out);
        lastSegmentPageCount++;
    }

    private static LiveViewCheckpointRowPositionDeltaNode[] growNodes(LiveViewCheckpointRowPositionDeltaNode[] src, int size) {
        final LiveViewCheckpointRowPositionDeltaNode[] dst = new LiveViewCheckpointRowPositionDeltaNode[size];
        System.arraycopy(src, 0, dst, 0, src.length);
        return dst;
    }

    /**
     * Per-recursion-level carrier for an {@link #addRec} result: the new (left) node
     * reference, minimum key, and subtree sum, and - when the node overflowed and
     * split - the promoted right sibling reference, minimum key, subtree sum, and a
     * split flag.
     */
    private static final class AddResult {
        final LiveViewCheckpointPageRef leftRef = new LiveViewCheckpointPageRef();
        long leftMinId;
        long leftMinTs;
        long leftSubtreeSum;
        final LiveViewCheckpointPageRef rightRef = new LiveViewCheckpointPageRef();
        long rightMinId;
        long rightMinTs;
        long rightSubtreeSum;
        boolean split;
    }
}
