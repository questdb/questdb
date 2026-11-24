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
import io.questdb.std.ObjList;
import io.questdb.std.Transient;
import io.questdb.std.str.Path;
import org.jetbrains.annotations.NotNull;

import java.io.Closeable;

/**
 * Copy-on-write publisher for the timeline B+ tree. Each mutation reads the
 * prior generation's tree through a {@link LiveViewCheckpointTimelineReader},
 * writes only the changed pages into one fresh metadata segment, and returns
 * the new tree root reference; the caller commits the generation by publishing
 * that root in a superblock slot.
 * <ul>
 *     <li>{@link #append} inserts one logical checkpoint entry, path-copying
 *     the {@code O(log N)} spine and splitting nodes that overflow, reusing
 *     every untouched subtree by its existing page reference.</li>
 *     <li>{@link #appendRemovingSecondLast} appends one entry above every key and
 *     drops the second-to-last entry in the same pass. The last entry moves down
 *     one position, so every node keeps its entry count, and only the at most two
 *     leaves holding the last two positions and their spines are copied.</li>
 *     <li>{@link #splice} re-versions a set of existing entries in place (same
 *     keys, new root/positions - the out-of-order repair of {@code [C, H)}). It
 *     copies only the leaves holding the affected keys and their ancestor
 *     spine, reusing the prefix and suffix subtrees. Keys are preserved, so no
 *     node splits or merges.</li>
 *     <li>{@link #truncateAbove} drops every entry whose {@code maxTimestamp}
 *     is at or above a floor - the highest-key suffix - and keeps the surviving
 *     prefix by page reference. It path-copies only the boundary spine, promotes
 *     a subtree that collapses to a single child so the published tree stays
 *     compact, and reuses every prefix subtree below the floor. This is the
 *     preserve-the-prefix half of an EOF or predecessor-resume out-of-order
 *     repair: the tail roots go, the long-term anchors stay.</li>
 * </ul>
 * Metadata pages are immutable and never rewritten in place, so a reader of the
 * prior generation keeps walking the old paths. The instance is reusable across
 * mutations and is not thread safe.
 */
public class LiveViewCheckpointTimelineWriter implements Closeable {

    private final Path checkpointsDir = new Path();
    private final int internalCapacity;
    private final int leafCapacity;
    private final LiveViewCheckpointTimelineReader reader;
    private final LiveViewCheckpointTimelineEntry scratchEntry = new LiveViewCheckpointTimelineEntry();
    private final LiveViewCheckpointMetaSegmentWriter segmentWriter;
    private long lastSegmentBytes;
    private int lastSegmentPageCount;
    // The key of the entry the last insertRec replacing the last entry dropped.
    private long replacedCheckpointId;
    private long replacedMaxTimestamp;
    private final ObjList<LiveViewCheckpointTimelineNode> dropPool = new ObjList<>();
    private final ObjList<LiveViewCheckpointTimelineNode> leftPool = new ObjList<>();
    private final LiveViewCheckpointTimelineNode newRootBuilder = new LiveViewCheckpointTimelineNode();
    private final LongList releasedSegmentIds = new LongList();
    private final ObjList<InsertResult> resultPool = new ObjList<>();
    private final ObjList<LiveViewCheckpointTimelineNode> rightPool = new ObjList<>();
    // The last entry appendRemovingSecondLast moves down one position.
    private final LiveViewCheckpointTimelineEntry shiftedEntry = new LiveViewCheckpointTimelineEntry();
    private final ObjList<LiveViewCheckpointPageRef> spliceRefPool = new ObjList<>();
    private final ObjList<LiveViewCheckpointPageRef> truncateRefPool = new ObjList<>();

    public LiveViewCheckpointTimelineWriter(@NotNull CairoConfiguration configuration) {
        this(configuration, 64, 64);
    }

    public LiveViewCheckpointTimelineWriter(@NotNull CairoConfiguration configuration, int leafCapacity, int internalCapacity) {
        if (leafCapacity < 2 || internalCapacity < 2) {
            throw CairoException.critical(0)
                    .put("live view checkpoint timeline node capacity must be at least 2, leaf=")
                    .put(leafCapacity).put(", internal=").put(internalCapacity);
        }
        this.leafCapacity = leafCapacity;
        this.internalCapacity = internalCapacity;
        this.reader = new LiveViewCheckpointTimelineReader(configuration);
        this.segmentWriter = new LiveViewCheckpointMetaSegmentWriter(configuration);
    }

    /**
     * Inserts {@code entry} (a unique {@code (maxTimestamp, checkpointId)} key)
     * into the tree rooted at {@code oldRoot} - null/empty for a fresh timeline -
     * and fills {@code newRootOut} with the new tree root. All new pages land in a
     * new metadata segment {@code newSegmentId}, which must be unused.
     */
    public void append(
            @NotNull LiveViewCheckpointPageRef oldRoot,
            @NotNull LiveViewCheckpointTimelineEntry entry,
            long newSegmentId,
            @NotNull LiveViewCheckpointPageRef newRootOut
    ) {
        beginSegment(newSegmentId);
        if (oldRoot.isNull()) {
            final LiveViewCheckpointTimelineNode leaf = leftAt(0);
            leaf.resetLeaf();
            leaf.insertEntryAt(0, entry);
            writePage(leaf, newRootOut);
        } else {
            insertRec(oldRoot.getSegmentId(), oldRoot.getOffset(), oldRoot.getLength(), entry, 0, false);
            final InsertResult root = resultAt(0);
            if (root.split) {
                newRootBuilder.resetInternal();
                newRootBuilder.appendChild(root.leftMinTs, root.leftMinId, root.leftRef.getSegmentId(), root.leftRef.getOffset(), root.leftRef.getLength());
                newRootBuilder.appendChild(root.rightMinTs, root.rightMinId, root.rightRef.getSegmentId(), root.rightRef.getOffset(), root.rightRef.getLength());
                writePage(newRootBuilder, newRootOut);
            } else {
                newRootOut.of(root.leftRef.getSegmentId(), root.leftRef.getOffset(), root.leftRef.getLength());
            }
        }
        commitSegment();
    }

    /**
     * Appends {@code entry}, whose key must sit above every key the tree holds, and
     * drops the tree's second-to-last entry in the same pass, filling
     * {@code newRootOut} with the new tree root: {@code [.., X, Y]} becomes
     * {@code [.., Y, Z]}. The last entry moves down one position and {@code entry}
     * takes the last, so every node keeps its entry count: nothing splits or merges,
     * and the separators stay sorted. The descent follows the rightmost path. A last
     * leaf holding two entries or more rewrites its last two slots; one holding only
     * the last entry takes {@code entry} in its one slot and hands the last entry to
     * the last leaf of its left neighbour, in place of the dropped entry. Only the at
     * most two leaves holding the last two positions and their spines are copied into
     * {@code newSegmentId}.
     *
     * @throws CairoException when the tree holds fewer than two entries, or when its
     *                        second-to-last key is not
     *                        {@code (removedMaxTimestamp, removedCheckpointId)}; the
     *                        segment is then discarded, and nothing the caller
     *                        publishes changes
     */
    public void appendRemovingSecondLast(
            @NotNull LiveViewCheckpointPageRef oldRoot,
            long removedMaxTimestamp,
            long removedCheckpointId,
            @NotNull LiveViewCheckpointTimelineEntry entry,
            long newSegmentId,
            @NotNull LiveViewCheckpointPageRef newRootOut
    ) {
        if (oldRoot.isNull()) {
            throw CairoException.critical(0).put("cannot remove the second-to-last entry of an empty live view checkpoint timeline");
        }
        beginSegment(newSegmentId);
        try {
            if (shiftLastRec(oldRoot.getSegmentId(), oldRoot.getOffset(), oldRoot.getLength(), entry, removedMaxTimestamp, removedCheckpointId, 0)) {
                throw CairoException.critical(0).put("cannot remove the second-to-last entry of a live view checkpoint timeline holding one entry");
            }
        } catch (Throwable t) {
            segmentWriter.discard();
            throw t;
        }
        final InsertResult root = resultAt(0);
        newRootOut.of(root.leftRef.getSegmentId(), root.leftRef.getOffset(), root.leftRef.getLength());
        commitSegment();
    }

    @Override
    public void close() {
        Misc.free(reader);
        Misc.free(segmentWriter);
        Misc.free(checkpointsDir);
    }

    /**
     * Releases the reader's mappings and any in-flight segment while keeping every
     * shell, so the writer can serve the next publication without holding a
     * mapping into a file a retire or compaction unlinks.
     */
    public void detach() {
        reader.detach();
        segmentWriter.discard();
        releasedSegmentIds.clear();
    }

    /**
     * @return the metadata segment of every page the last mutation superseded -
     * the path copy's own old pages plus every page of a subtree a truncate
     * dropped - one element per page, for the caller to release against the
     * segment catalogue
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
     * Replaces the tree's last entry with {@code entry}, whose key must sit above
     * every key the tree holds, and fills {@code newRootOut} with the new tree
     * root. The descent follows the rightmost path an append of {@code entry}
     * would take, and the leaf it lands in keeps its entry count, so nothing
     * splits and only that path is copied into {@code newSegmentId}.
     */
    public void replaceLast(
            @NotNull LiveViewCheckpointPageRef oldRoot,
            @NotNull LiveViewCheckpointTimelineEntry entry,
            long newSegmentId,
            @NotNull LiveViewCheckpointPageRef newRootOut
    ) {
        if (oldRoot.isNull()) {
            throw CairoException.critical(0).put("cannot replace the last entry of an empty live view checkpoint timeline");
        }
        beginSegment(newSegmentId);
        insertRec(oldRoot.getSegmentId(), oldRoot.getOffset(), oldRoot.getLength(), entry, 0, true);
        final InsertResult root = resultAt(0);
        assert !root.split;
        newRootOut.of(root.leftRef.getSegmentId(), root.leftRef.getOffset(), root.leftRef.getLength());
        commitSegment();
    }

    /**
     * Re-versions the {@code entryCount} entries in {@code entries} (sorted
     * ascending by key; every key must already exist in the tree) by replacing
     * their root/position payloads, filling {@code newRootOut} with the new tree
     * root. Keys are preserved, so the tree shape is unchanged and only the
     * affected leaves and their ancestor spine are copied into {@code newSegmentId}.
     * A zero-length replacement returns {@code oldRoot} unchanged without writing a
     * segment.
     */
    public void splice(
            @NotNull LiveViewCheckpointPageRef oldRoot,
            @NotNull ObjList<LiveViewCheckpointTimelineEntry> entries,
            int entryCount,
            long newSegmentId,
            @NotNull LiveViewCheckpointPageRef newRootOut
    ) {
        if (entryCount == 0) {
            newRootOut.of(oldRoot.getSegmentId(), oldRoot.getOffset(), oldRoot.getLength());
            lastSegmentBytes = 0;
            lastSegmentPageCount = 0;
            releasedSegmentIds.clear();
            return;
        }
        if (oldRoot.isNull()) {
            throw CairoException.critical(0).put("cannot splice an empty live view checkpoint timeline");
        }
        beginSegment(newSegmentId);
        spliceRec(oldRoot.getSegmentId(), oldRoot.getOffset(), oldRoot.getLength(), entries, 0, entryCount, 0);
        final LiveViewCheckpointPageRef rootRef = spliceRefAt(0);
        newRootOut.of(rootRef.getSegmentId(), rootRef.getOffset(), rootRef.getLength());
        commitSegment();
    }

    /**
     * Drops every entry whose {@code maxTimestamp} is at or above {@code floor}
     * (a contiguous suffix of the key space) and fills {@code newRootOut} with
     * the truncated tree root, reusing every surviving prefix subtree by its
     * existing page reference and path-copying only the boundary spine. A
     * subtree that collapses to a single surviving child is promoted by
     * reference rather than wrapped in a one-child internal node, so the
     * published tree stays as compact as an append would leave it.
     * <p>
     * Returns true when a non-empty prefix survived; returns false - leaving
     * {@code newRootOut} untouched - when {@code floor} is at or below every key
     * so the whole tree drops (an empty prefix has nothing to preserve, and the
     * caller falls back to a full retire). When {@code floor} is above every key
     * nothing is dropped and {@code oldRoot} is reused as-is without writing a
     * segment.
     */
    public boolean truncateAbove(
            @NotNull LiveViewCheckpointPageRef oldRoot,
            long floor,
            long newSegmentId,
            @NotNull LiveViewCheckpointPageRef newRootOut
    ) {
        releasedSegmentIds.clear();
        if (oldRoot.isNull()) {
            return false;
        }
        // Nothing at or above the floor: the tree is unchanged, reuse it without
        // rewriting the right spine into a new segment.
        if (reader.last(oldRoot, scratchEntry) && scratchEntry.maxTimestamp < floor) {
            newRootOut.of(oldRoot.getSegmentId(), oldRoot.getOffset(), oldRoot.getLength());
            lastSegmentBytes = 0;
            lastSegmentPageCount = 0;
            return true;
        }
        // Nothing below the floor: the whole tree drops. Return without opening a
        // segment so an empty truncation leaves no orphan page behind.
        if (!reader.predecessor(oldRoot, floor, scratchEntry)) {
            return false;
        }
        beginSegment(newSegmentId);
        final boolean survived = truncateRec(oldRoot.getSegmentId(), oldRoot.getOffset(), oldRoot.getLength(), floor, 0);
        commitSegment();
        // A prefix key exists (predecessor above), so the recursion always keeps it.
        assert survived;
        final LiveViewCheckpointPageRef rootRef = truncateRefAt(0);
        newRootOut.of(rootRef.getSegmentId(), rootRef.getOffset(), rootRef.getLength());
        return true;
    }

    private void beginSegment(long segmentId) {
        segmentWriter.of(checkpointsDir, segmentId);
        lastSegmentPageCount = 0;
        releasedSegmentIds.clear();
    }

    private void commitSegment() {
        lastSegmentBytes = segmentWriter.commit();
    }

    private LiveViewCheckpointTimelineNode dropAt(int depth) {
        growNodes(dropPool, depth + 1);
        LiveViewCheckpointTimelineNode node = dropPool.getQuick(depth);
        if (node == null) {
            node = new LiveViewCheckpointTimelineNode();
            dropPool.setQuick(depth, node);
        }
        return node;
    }

    private void finishNode(LiveViewCheckpointTimelineNode node, InsertResult res, int depth, boolean leaf) {
        final int capacity = leaf ? leafCapacity : internalCapacity;
        if (node.count() <= capacity) {
            writePage(node, res.leftRef);
            res.leftMinTs = leaf ? node.entryMaxTimestamp[0] : node.childMinMaxTimestamp[0];
            res.leftMinId = leaf ? node.entryCheckpointId[0] : node.childMinCheckpointId[0];
            res.split = false;
            return;
        }
        // The node overflowed: move its upper half into a fresh sibling and write
        // both halves. The sibling's minimum key is the promoted separator.
        final LiveViewCheckpointTimelineNode right = rightAt(depth);
        node.splitInto(right);
        writePage(node, res.leftRef);
        res.leftMinTs = leaf ? node.entryMaxTimestamp[0] : node.childMinMaxTimestamp[0];
        res.leftMinId = leaf ? node.entryCheckpointId[0] : node.childMinCheckpointId[0];
        writePage(right, res.rightRef);
        res.rightMinTs = leaf ? right.entryMaxTimestamp[0] : right.childMinMaxTimestamp[0];
        res.rightMinId = leaf ? right.entryCheckpointId[0] : right.childMinCheckpointId[0];
        res.split = true;
    }

    private void insertRec(
            long seg,
            long off,
            long len,
            LiveViewCheckpointTimelineEntry entry,
            int depth,
            boolean isReplacingLast
    ) {
        final LiveViewCheckpointTimelineNode node = leftAt(depth);
        reader.openAndDecode(seg, off, len, node);
        // A descended node is always written back, so decoding one is exactly what
        // supersedes the page holding it.
        releasedSegmentIds.add(seg);
        final InsertResult res = resultAt(depth);
        if (node.isLeaf()) {
            if (isReplacingLast) {
                // A key above every key descends to the leaf holding the tree's last
                // entry, which is the one it replaces.
                final int last = node.count() - 1;
                assert last >= 0 && LiveViewCheckpointTimeline.compareKey(
                        node.entryMaxTimestamp[last],
                        node.entryCheckpointId[last],
                        entry.maxTimestamp,
                        entry.checkpointId
                ) < 0 : "live view checkpoint replacement key not above the last entry";
                replacedMaxTimestamp = node.entryMaxTimestamp[last];
                replacedCheckpointId = node.entryCheckpointId[last];
                node.retainPrefix(last);
            }
            final int pos = node.leafInsertPosition(entry.maxTimestamp, entry.checkpointId);
            assert pos == node.count()
                    || LiveViewCheckpointTimeline.compareKey(node.entryMaxTimestamp[pos], node.entryCheckpointId[pos], entry.maxTimestamp, entry.checkpointId) != 0
                    : "duplicate live view checkpoint key on append";
            node.insertEntryAt(pos, entry);
            finishNode(node, res, depth, true);
            return;
        }
        final int ci = node.childIndexFor(entry.maxTimestamp, entry.checkpointId);
        assert !isReplacingLast || ci == node.count() - 1;
        insertRec(node.childSegmentId[ci], node.childOffset[ci], node.childLength[ci], entry, depth + 1, isReplacingLast);
        final InsertResult child = resultAt(depth + 1);
        node.setChildEntry(ci, child.leftMinTs, child.leftMinId, child.leftRef.getSegmentId(), child.leftRef.getOffset(), child.leftRef.getLength());
        if (child.split) {
            node.insertChildAt(ci + 1, child.rightMinTs, child.rightMinId, child.rightRef.getSegmentId(), child.rightRef.getOffset(), child.rightRef.getLength());
        }
        finishNode(node, res, depth, false);
    }

    private LiveViewCheckpointTimelineNode leftAt(int depth) {
        growNodes(leftPool, depth + 1);
        LiveViewCheckpointTimelineNode node = leftPool.getQuick(depth);
        if (node == null) {
            node = new LiveViewCheckpointTimelineNode();
            leftPool.setQuick(depth, node);
        }
        return node;
    }

    /**
     * Releases every page of the subtree at {@code (seg, off, len)}, which a
     * truncate is discarding whole. The walk uses its own node pool, so it can run
     * beside a {@link #truncateRec} descent at the same depth.
     */
    private void releaseSubtreeRec(long seg, long off, long len, int depth) {
        final LiveViewCheckpointTimelineNode node = dropAt(depth);
        reader.openAndDecode(seg, off, len, node);
        releasedSegmentIds.add(seg);
        if (node.isLeaf()) {
            return;
        }
        for (int i = 0, n = node.count(); i < n; i++) {
            releaseSubtreeRec(node.childSegmentId[i], node.childOffset[i], node.childLength[i], depth + 1);
        }
    }

    private void releaseSubtrees(LiveViewCheckpointTimelineNode node, int from, int to, int depth) {
        for (int i = from; i < to; i++) {
            releaseSubtreeRec(node.childSegmentId[i], node.childOffset[i], node.childLength[i], depth);
        }
    }

    private InsertResult resultAt(int depth) {
        while (resultPool.size() <= depth) {
            resultPool.add(null);
        }
        InsertResult res = resultPool.getQuick(depth);
        if (res == null) {
            res = new InsertResult();
            resultPool.setQuick(depth, res);
        }
        return res;
    }

    private LiveViewCheckpointTimelineNode rightAt(int depth) {
        growNodes(rightPool, depth + 1);
        LiveViewCheckpointTimelineNode node = rightPool.getQuick(depth);
        if (node == null) {
            node = new LiveViewCheckpointTimelineNode();
            rightPool.setQuick(depth, node);
        }
        return node;
    }

    /**
     * One level of {@link #appendRemovingSecondLast}'s descent down the rightmost path
     * of the subtree at {@code (seg, off, len)}, which leaves the new subtree in
     * {@link #resultAt}({@code depth}).
     *
     * @return true when the subtree's last leaf held only the last entry, which then
     * sits in {@link #shiftedEntry} for an ancestor to put in place of the dropped
     * entry, the last entry of the subtree left of this one
     */
    private boolean shiftLastRec(
            long seg,
            long off,
            long len,
            LiveViewCheckpointTimelineEntry entry,
            long removedMaxTimestamp,
            long removedCheckpointId,
            int depth
    ) {
        final LiveViewCheckpointTimelineNode node = leftAt(depth);
        reader.openAndDecode(seg, off, len, node);
        releasedSegmentIds.add(seg);
        final InsertResult res = resultAt(depth);
        final int c = node.count();
        if (node.isLeaf()) {
            // The reader's last() rejects an empty node before a caller gets here.
            assert c > 0 && LiveViewCheckpointTimeline.compareKey(
                    node.entryMaxTimestamp[c - 1],
                    node.entryCheckpointId[c - 1],
                    entry.maxTimestamp,
                    entry.checkpointId
            ) < 0 : "live view checkpoint appended key not above the last entry";
            node.copyEntryTo(c - 1, shiftedEntry);
            if (c == 1) {
                node.retainPrefix(0);
                node.insertEntryAt(0, entry);
                finishNode(node, res, depth, true);
                return true;
            }
            checkRemovedKey(node.entryMaxTimestamp[c - 2], node.entryCheckpointId[c - 2], removedMaxTimestamp, removedCheckpointId);
            node.retainPrefix(c - 2);
            node.insertEntryAt(c - 2, shiftedEntry);
            node.insertEntryAt(c - 1, entry);
            finishNode(node, res, depth, true);
            return false;
        }
        final int ci = c - 1;
        final boolean isCarried = shiftLastRec(node.childSegmentId[ci], node.childOffset[ci], node.childLength[ci], entry, removedMaxTimestamp, removedCheckpointId, depth + 1);
        final InsertResult child = resultAt(depth + 1);
        node.setChildEntry(ci, child.leftMinTs, child.leftMinId, child.leftRef.getSegmentId(), child.leftRef.getOffset(), child.leftRef.getLength());
        if (isCarried) {
            // An append splits an internal node into halves whose right one holds at least
            // two children, and a truncate promotes a node it leaves holding one child, so
            // no internal node on the rightmost path holds a single child: the last leaf's
            // left neighbour sits under this node.
            assert ci > 0 : "live view checkpoint timeline holds a single-child node on its rightmost path";
            insertRec(node.childSegmentId[ci - 1], node.childOffset[ci - 1], node.childLength[ci - 1], shiftedEntry, depth + 1, true);
            checkRemovedKey(replacedMaxTimestamp, replacedCheckpointId, removedMaxTimestamp, removedCheckpointId);
            final InsertResult previous = resultAt(depth + 1);
            assert !previous.split;
            node.setChildEntry(ci - 1, previous.leftMinTs, previous.leftMinId, previous.leftRef.getSegmentId(), previous.leftRef.getOffset(), previous.leftRef.getLength());
        }
        finishNode(node, res, depth, false);
        return false;
    }

    private LiveViewCheckpointPageRef spliceRefAt(int depth) {
        return refAt(spliceRefPool, depth);
    }

    private void spliceRec(long seg, long off, long len, ObjList<LiveViewCheckpointTimelineEntry> entries, int lo, int hi, int depth) {
        final LiveViewCheckpointTimelineNode node = leftAt(depth);
        reader.openAndDecode(seg, off, len, node);
        releasedSegmentIds.add(seg);
        if (node.isLeaf()) {
            for (int r = lo; r < hi; r++) {
                final LiveViewCheckpointTimelineEntry e = entries.getQuick(r);
                final int pos = node.findEntry(e.maxTimestamp, e.checkpointId);
                if (pos < 0) {
                    throw CairoException.critical(0)
                            .put("live view checkpoint splice key not found, maxTimestamp=")
                            .put(e.maxTimestamp).put(", checkpointId=").put(e.checkpointId);
                }
                node.replaceEntryPayloadAt(pos, e);
            }
            writePage(node, spliceRefAt(depth));
            return;
        }
        final int c = node.count();
        int r = lo;
        for (int ci = 0; ci < c && r < hi; ci++) {
            final int subLo = r;
            if (ci + 1 < c) {
                final long nextMinTs = node.childMinMaxTimestamp[ci + 1];
                final long nextMinId = node.childMinCheckpointId[ci + 1];
                while (r < hi && LiveViewCheckpointTimeline.compareKey(
                        entries.getQuick(r).maxTimestamp,
                        entries.getQuick(r).checkpointId,
                        nextMinTs,
                        nextMinId
                ) < 0) {
                    r++;
                }
            } else {
                r = hi;
            }
            if (subLo < r) {
                spliceRec(node.childSegmentId[ci], node.childOffset[ci], node.childLength[ci], entries, subLo, r, depth + 1);
                final LiveViewCheckpointPageRef childRef = spliceRefAt(depth + 1);
                node.setChildRef(ci, childRef.getSegmentId(), childRef.getOffset(), childRef.getLength());
            }
        }
        writePage(node, spliceRefAt(depth));
    }

    private LiveViewCheckpointPageRef truncateRefAt(int depth) {
        return refAt(truncateRefPool, depth);
    }

    /**
     * Truncates the subtree rooted at {@code (seg, off, len)} to keys strictly
     * below {@code floor}, writing the surviving nodes bottom-up. On a non-empty
     * result the survivor's page reference is left in {@link #truncateRefAt}
     * ({@code depth}); the boolean says whether anything survived. A node that
     * keeps exactly one child returns that child's reference directly (inline
     * collapse), so no single-child internal node is ever written - the reader's
     * {@code last()} rejects an empty node but tolerates an under-full one.
     */
    private boolean truncateRec(long seg, long off, long len, long floor, int depth) {
        final LiveViewCheckpointTimelineNode node = leftAt(depth);
        reader.openAndDecode(seg, off, len, node);
        releasedSegmentIds.add(seg);
        if (node.isLeaf()) {
            final int kept = node.leafLowerBoundByTimestamp(floor);
            if (kept == 0) {
                return false;
            }
            node.retainPrefix(kept);
            writePage(node, truncateRefAt(depth));
            return true;
        }
        // Every child below the straddle sits entirely under the floor and is
        // kept by reference; the straddling child is the last one whose subtree
        // minimum is below the floor; every later child holds only keys at or
        // above it and is dropped.
        final int count = node.count();
        final int straddle = node.internalLowerBoundByTimestamp(floor) - 1;
        // A dropped subtree is never descended into, so its pages have to be
        // walked to be released. The walk costs what the truncate discards.
        releaseSubtrees(node, straddle + 1, count, depth + 1);
        if (straddle < 0) {
            return false;
        }
        final boolean straddleSurvived = truncateRec(node.childSegmentId[straddle], node.childOffset[straddle], node.childLength[straddle], floor, depth + 1);
        final int keptChildren = straddleSurvived ? straddle + 1 : straddle;
        if (keptChildren == 0) {
            return false;
        }
        if (keptChildren == 1) {
            // Sole surviving child: promote it as this subtree's root by
            // reference, writing no new internal page.
            final LiveViewCheckpointPageRef out = truncateRefAt(depth);
            if (straddleSurvived) {
                final LiveViewCheckpointPageRef childRef = truncateRefAt(depth + 1);
                out.of(childRef.getSegmentId(), childRef.getOffset(), childRef.getLength());
            } else {
                out.of(node.childSegmentId[0], node.childOffset[0], (int) node.childLength[0]);
            }
            return true;
        }
        if (straddleSurvived) {
            final LiveViewCheckpointPageRef childRef = truncateRefAt(depth + 1);
            node.setChildRef(straddle, childRef.getSegmentId(), childRef.getOffset(), childRef.getLength());
            node.retainPrefix(straddle + 1);
        } else {
            node.retainPrefix(straddle);
        }
        writePage(node, truncateRefAt(depth));
        return true;
    }

    private void writePage(LiveViewCheckpointTimelineNode node, LiveViewCheckpointPageRef out) {
        node.writeTo(segmentWriter, out);
        lastSegmentPageCount++;
    }

    private static void checkRemovedKey(long maxTimestamp, long checkpointId, long removedMaxTimestamp, long removedCheckpointId) {
        if (maxTimestamp != removedMaxTimestamp || checkpointId != removedCheckpointId) {
            throw CairoException.critical(0)
                    .put("live view checkpoint timeline second-to-last key does not match the removed key [maxTimestamp=")
                    .put(maxTimestamp).put(", checkpointId=").put(checkpointId)
                    .put(", removedMaxTimestamp=").put(removedMaxTimestamp)
                    .put(", removedCheckpointId=").put(removedCheckpointId).put(']');
        }
    }

    private static void growNodes(ObjList<LiveViewCheckpointTimelineNode> pool, int size) {
        while (pool.size() < size) {
            pool.add(null);
        }
    }

    /**
     * @return the reference shell {@code pool} holds for {@code depth}, built on
     * first use. A tree deepens a level at a time and never shrinks, so the pool
     * stops growing as soon as the deepest descent has run once.
     */
    private static LiveViewCheckpointPageRef refAt(ObjList<LiveViewCheckpointPageRef> pool, int depth) {
        while (pool.size() <= depth) {
            pool.add(new LiveViewCheckpointPageRef());
        }
        return pool.getQuick(depth);
    }

    /**
     * Per-recursion-level carrier for an {@link #insertRec} result: the new (left)
     * node reference and minimum key, and - when the node overflowed and split -
     * the promoted right sibling reference, minimum key, and a split flag.
     */
    private static final class InsertResult {
        final LiveViewCheckpointPageRef leftRef = new LiveViewCheckpointPageRef();
        long leftMinId;
        long leftMinTs;
        final LiveViewCheckpointPageRef rightRef = new LiveViewCheckpointPageRef();
        long rightMinId;
        long rightMinTs;
        boolean split;
    }

}
