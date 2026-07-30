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
import io.questdb.std.LongList;
import io.questdb.std.Misc;
import io.questdb.std.Transient;
import io.questdb.std.str.Path;
import org.jetbrains.annotations.NotNull;

import java.io.Closeable;

/**
 * Copy-on-write publisher for the row-position delta tree. Each mutation reads
 * the prior generation's tree through a
 * {@link LiveViewCheckpointRowPositionDeltaReader}, writes only the changed
 * pages into one fresh metadata segment, and returns the new tree root
 * reference; the caller commits the generation by publishing that root as
 * {@code rowPositionDeltaRootRef} in a superblock slot.
 * <p>
 * The primary mutation is {@link #suffixAdd}: an O3 repair's suffix range-add over
 * {@code [H, +inf)} is one difference-array point add at the breakpoint key
 * {@code H} - {@code diff[H] += delta}. It path-copies the {@code O(log N)} spine,
 * accumulating into an existing breakpoint or inserting a new one (splitting nodes
 * that overflow), and reuses every untouched subtree by its existing page reference.
 * Each ancestor's stored subtree sum for the descended child is updated to the
 * child's recomputed sum, so a later {@link LiveViewCheckpointRowPositionDeltaReader#prefixSum}
 * stays correct without walking the suffix.
 * <p>
 * {@link #pruneBelow} is the retention horizon's counterpart: it discards every
 * breakpoint below the first surviving timeline key and folds their sum into that
 * key, so the index shrinks with the timeline while every surviving lookup keeps
 * reporting the prefix sum it reported before. Metadata pages are immutable and
 * never rewritten in place, so a reader of the prior generation keeps walking the
 * old paths. The instance is reusable across mutations and is not thread safe.
 */
public class LiveViewCheckpointRowPositionDeltaWriter implements Closeable {

    private final Path checkpointsDir = new Path();
    private final int internalCapacity;
    private final int leafCapacity;
    private final LiveViewCheckpointRowPositionDeltaReader reader;
    private final LiveViewCheckpointMetaSegmentWriter segmentWriter;
    private LiveViewCheckpointRowPositionDeltaNode[] dropPool = new LiveViewCheckpointRowPositionDeltaNode[0];
    private long lastSegmentBytes;
    private int lastSegmentPageCount;
    private LiveViewCheckpointRowPositionDeltaNode[] leftPool = new LiveViewCheckpointRowPositionDeltaNode[0];
    private final LiveViewCheckpointRowPositionDeltaNode newRootBuilder = new LiveViewCheckpointRowPositionDeltaNode();
    private boolean probeHasEntryAtOrAbove;
    private boolean probeHasEntryBelow;
    private final LiveViewCheckpointRowPositionDeltaNode probeNode = new LiveViewCheckpointRowPositionDeltaNode();
    private long probeSumBelow;
    private long prunedSum;
    private final LongList releasedSegmentIds = new LongList();
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
     * @return the metadata segment of every page the last mutation superseded,
     * one element per page, for the caller to release against the segment
     * catalogue
     */
    public LongList getLastReleasedSegmentIds() {
        return releasedSegmentIds;
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
     * Discards every breakpoint below key {@code (floorMaxTimestamp,
     * floorCheckpointId)} - the first timeline key a retention horizon keeps -
     * and folds their combined difference into that key, filling
     * {@code newRootOut} with the new tree root.
     * <p>
     * Deleting those breakpoints outright would be wrong: each difference applies
     * to the whole later suffix, so a breakpoint keyed to a retired boundary still
     * contributes to every surviving one. Folding the discarded prefix sum into
     * the floor key leaves
     * {@link LiveViewCheckpointRowPositionDeltaReader#prefixSum} returning exactly
     * what it returned before, for every key at or above the floor. The fold
     * accumulates into an existing breakpoint at the floor key when there is one
     * and inserts a new one otherwise.
     * <p>
     * All new pages land in a new metadata segment {@code newSegmentId}, which must
     * be unused. A tree with nothing below the floor is left alone: {@code newRootOut}
     * receives {@code oldRoot}, no segment is written, and this returns false. A
     * prune that discards every breakpoint for a combined difference of zero empties
     * the tree, leaving {@code newRootOut} null and again writing no segment.
     *
     * @return true when the prune changed the tree
     */
    public boolean pruneBelow(
            @NotNull LiveViewCheckpointPageRef oldRoot,
            long floorMaxTimestamp,
            long floorCheckpointId,
            long newSegmentId,
            @NotNull LiveViewCheckpointPageRef newRootOut
    ) {
        releasedSegmentIds.clear();
        lastSegmentBytes = 0;
        lastSegmentPageCount = 0;
        if (oldRoot.isNull()) {
            newRootOut.clear();
            return false;
        }
        probeBelow(oldRoot, floorMaxTimestamp, floorCheckpointId);
        if (!probeHasEntryBelow) {
            newRootOut.of(oldRoot.getSegmentId(), oldRoot.getOffset(), oldRoot.getLength());
            return false;
        }
        if (!probeHasEntryAtOrAbove && probeSumBelow == 0) {
            // Everything the tree holds sits below the floor and cancels out, so
            // there is nothing to fold forward. Release the lot and publish a null
            // root rather than open a segment for an empty tree.
            releaseSubtreeRec(oldRoot.getSegmentId(), oldRoot.getOffset(), oldRoot.getLength(), 0);
            newRootOut.clear();
            return true;
        }
        beginSegment(newSegmentId);
        prunedSum = 0;
        final boolean survived = pruneRec(oldRoot.getSegmentId(), oldRoot.getOffset(), oldRoot.getLength(), floorMaxTimestamp, floorCheckpointId, 0);
        commitSegment();
        // The probe proved either a surviving breakpoint or a non-zero prefix sum
        // to fold into the floor key, so the recursion always keeps one.
        assert survived;
        final AddResult root = resultAt(0);
        newRootOut.of(root.leftRef.getSegmentId(), root.leftRef.getOffset(), root.leftRef.getLength());
        return true;
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
        // A descended node is always written back, so decoding one is exactly what
        // supersedes the page holding it.
        releasedSegmentIds.add(seg);
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
        releasedSegmentIds.clear();
    }

    private void commitSegment() {
        lastSegmentBytes = segmentWriter.commit();
    }

    private LiveViewCheckpointRowPositionDeltaNode dropAt(int depth) {
        if (depth >= dropPool.length) {
            dropPool = growNodes(dropPool, depth + 1);
        }
        LiveViewCheckpointRowPositionDeltaNode node = dropPool[depth];
        if (node == null) {
            node = new LiveViewCheckpointRowPositionDeltaNode();
            dropPool[depth] = node;
        }
        return node;
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

    /**
     * Read-only probe of what a prune at {@code (maxTimestamp, checkpointId)}
     * would find: whether the tree holds a breakpoint below the floor, whether it
     * holds one at or above it, and the sum of the differences below. One
     * root-to-leaf descent answers all three, which lets
     * {@link #pruneBelow} refuse a prune that would discard nothing and take the
     * empty-tree disposition without opening a segment for it.
     */
    private void probeBelow(LiveViewCheckpointPageRef rootRef, long maxTimestamp, long checkpointId) {
        probeHasEntryAtOrAbove = false;
        probeHasEntryBelow = false;
        probeSumBelow = 0;
        long seg = rootRef.getSegmentId();
        long off = rootRef.getOffset();
        long len = rootRef.getLength();
        while (true) {
            reader.openAndDecode(seg, off, len, probeNode);
            if (probeNode.isLeaf()) {
                final int firstKept = probeNode.leafInsertPosition(maxTimestamp, checkpointId);
                for (int i = 0; i < firstKept; i++) {
                    probeSumBelow += probeNode.entryDiff[i];
                }
                probeHasEntryBelow |= firstKept > 0;
                probeHasEntryAtOrAbove |= firstKept < probeNode.count();
                return;
            }
            // Every child left of the descent child holds only keys below that
            // child's minimum, which is at or below the query key; every child
            // right of it holds only keys strictly above it.
            final int ci = probeNode.childIndexFor(maxTimestamp, checkpointId);
            for (int i = 0; i < ci; i++) {
                probeSumBelow += probeNode.childSubtreeSum[i];
            }
            probeHasEntryBelow |= ci > 0;
            probeHasEntryAtOrAbove |= ci < probeNode.count() - 1;
            seg = probeNode.childSegmentId[ci];
            off = probeNode.childOffset[ci];
            len = probeNode.childLength[ci];
        }
    }

    /**
     * Prunes the subtree rooted at {@code (seg, off, len)} to keys at or above the
     * floor, accumulating every discarded difference into {@link #prunedSum} on
     * the way down and folding the total into the floor key at the leaf. Writes
     * the surviving nodes bottom-up and leaves the result in {@link #resultAt}
     * ({@code depth}); the boolean says whether anything survived. A node that
     * keeps exactly one child returns that child's reference directly (inline
     * collapse), so no single-child internal node is ever written.
     */
    private boolean pruneRec(long seg, long off, long len, long floorTs, long floorId, int depth) {
        final LiveViewCheckpointRowPositionDeltaNode node = leftAt(depth);
        reader.openAndDecode(seg, off, len, node);
        // A descended node is always written back, so decoding one is exactly what
        // supersedes the page holding it.
        releasedSegmentIds.add(seg);
        final AddResult res = resultAt(depth);
        if (node.isLeaf()) {
            final int firstKept = node.leafInsertPosition(floorTs, floorId);
            for (int i = 0; i < firstKept; i++) {
                prunedSum += node.entryDiff[i];
            }
            node.retainSuffix(firstKept);
            if (node.count() > 0
                    && node.entryMaxTimestamp[0] == floorTs
                    && node.entryCheckpointId[0] == floorId) {
                node.addToLeafDiffAt(0, prunedSum);
            } else if (prunedSum != 0) {
                node.insertEntryAt(0, floorTs, floorId, prunedSum);
            }
            prunedSum = 0;
            if (node.count() == 0) {
                return false;
            }
            finishNode(node, res, depth, true);
            return true;
        }
        final int straddle = node.childIndexFor(floorTs, floorId);
        // Every child left of the descent holds only keys below the floor: their
        // differences fold into the floor key and their pages are released whole,
        // which costs what the prune discards.
        for (int i = 0; i < straddle; i++) {
            prunedSum += node.childSubtreeSum[i];
            releaseSubtreeRec(node.childSegmentId[i], node.childOffset[i], node.childLength[i], depth + 1);
        }
        final boolean straddleSurvived = pruneRec(node.childSegmentId[straddle], node.childOffset[straddle], node.childLength[straddle], floorTs, floorId, depth + 1);
        if (!straddleSurvived && straddle + 1 >= node.count()) {
            return false;
        }
        if (straddleSurvived) {
            final AddResult child = resultAt(depth + 1);
            node.setChildEntry(straddle, child.leftMinTs, child.leftMinId, child.leftSubtreeSum, child.leftRef.getSegmentId(), child.leftRef.getOffset(), child.leftRef.getLength());
            if (child.split) {
                node.insertChildAt(straddle + 1, child.rightMinTs, child.rightMinId, child.rightSubtreeSum, child.rightRef.getSegmentId(), child.rightRef.getOffset(), child.rightRef.getLength());
            }
        }
        node.retainSuffix(straddleSurvived ? straddle : straddle + 1);
        if (node.count() == 1) {
            // Sole surviving child: promote it as this subtree's root by
            // reference, writing no new internal page.
            res.leftRef.of(node.childSegmentId[0], node.childOffset[0], (int) node.childLength[0]);
            res.leftMinTs = node.childMinMaxTimestamp[0];
            res.leftMinId = node.childMinCheckpointId[0];
            res.leftSubtreeSum = node.childSubtreeSum[0];
            res.split = false;
            return true;
        }
        finishNode(node, res, depth, false);
        return true;
    }

    /**
     * Releases every page of the subtree at {@code (seg, off, len)}, which a prune
     * is discarding whole. The walk uses its own node pool, so it can run beside a
     * {@link #pruneRec} descent at the same depth.
     */
    private void releaseSubtreeRec(long seg, long off, long len, int depth) {
        final LiveViewCheckpointRowPositionDeltaNode node = dropAt(depth);
        reader.openAndDecode(seg, off, len, node);
        releasedSegmentIds.add(seg);
        if (node.isLeaf()) {
            return;
        }
        for (int i = 0, n = node.count(); i < n; i++) {
            releaseSubtreeRec(node.childSegmentId[i], node.childOffset[i], node.childLength[i], depth + 1);
        }
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
