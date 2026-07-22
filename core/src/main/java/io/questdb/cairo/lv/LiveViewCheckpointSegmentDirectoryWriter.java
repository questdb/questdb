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
import io.questdb.std.LongHashSet;
import io.questdb.std.LongList;
import io.questdb.std.Misc;
import io.questdb.std.Transient;
import io.questdb.std.str.Path;
import org.jetbrains.annotations.NotNull;

import java.io.Closeable;

/**
 * Copy-on-write publisher for the segment directory B+ tree. One publication is
 * a session: {@link #begin} pins the prior generation's root, the catalogue
 * mutations stage against it, and {@link #publish} writes only the pages those
 * mutations changed into one fresh metadata segment and returns the new tree
 * root. Every untouched subtree is carried forward by its existing page
 * reference, so a seal's directory cost follows the segments it added or
 * re-referenced rather than how many segments are live - which is what keeps
 * publication metadata flat as the timeline grows.
 * <p>
 * Staging is what makes that possible. A repair applies one reference
 * transaction per repaired boundary, and the same segment can be released by one
 * boundary and taken by the next; the session folds those into a single value
 * per segment and touches each affected leaf once. It also keeps the old
 * validation contract: a rejected transaction leaves the staged image, and
 * therefore the reusable directory, exactly as it was.
 * <p>
 * The instance is reusable across publications and is not thread safe.
 */
public class LiveViewCheckpointSegmentDirectoryWriter implements Closeable {

    private static final int CHILD_STRIDE = 4;
    private static final int STAGED_FILE_LENGTH = 1;
    private static final int STAGED_FLAGS = 4;
    private static final long STAGED_FLAG_INSERT = 1;
    private static final long STAGED_FLAG_NONE = 0;
    private static final int STAGED_REFERENCE_COUNT = 2;
    private static final int STAGED_RETIRE_GENERATION = 3;
    private static final int STAGED_SEGMENT_ID = 0;
    private static final int STAGED_STRIDE = 5;
    private final LongHashSet addedSegmentIds = new LongHashSet();
    private final Path checkpointsDir = new Path();
    private final int internalCapacity;
    private final int leafCapacity;
    private final LiveViewCheckpointSegmentDirectoryEntry lookupEntry = new LiveViewCheckpointSegmentDirectoryEntry();
    private final LiveViewCheckpointPageRef oldRoot = new LiveViewCheckpointPageRef();
    private final LiveViewCheckpointPageRef pageRef = new LiveViewCheckpointPageRef();
    private final LiveViewCheckpointSegmentDirectoryReader reader;
    private final LongHashSet removedSegmentIds = new LongHashSet();
    private final LiveViewCheckpointMetaSegmentWriter segmentWriter;
    private final LongList staged = new LongList();
    private LiveViewCheckpointSegmentDirectoryNode[] builderPool = new LiveViewCheckpointSegmentDirectoryNode[0];
    private boolean isBegun;
    private long lastSegmentBytes;
    private int lastSegmentPageCount;
    private LiveViewCheckpointSegmentDirectoryNode[] nodePool = new LiveViewCheckpointSegmentDirectoryNode[0];
    private LongList[] outPool = new LongList[0];
    private LiveViewCheckpointSegmentDirectoryNode[] piecePool = new LiveViewCheckpointSegmentDirectoryNode[0];

    public LiveViewCheckpointSegmentDirectoryWriter(@NotNull CairoConfiguration configuration) {
        this(configuration, 64, 64);
    }

    public LiveViewCheckpointSegmentDirectoryWriter(
            @NotNull CairoConfiguration configuration,
            int leafCapacity,
            int internalCapacity
    ) {
        if (leafCapacity < 2 || internalCapacity < 2) {
            throw CairoException.critical(0)
                    .put("live view checkpoint segment directory node capacity must be at least 2, leaf=")
                    .put(leafCapacity).put(", internal=").put(internalCapacity);
        }
        this.leafCapacity = leafCapacity;
        this.internalCapacity = internalCapacity;
        this.reader = new LiveViewCheckpointSegmentDirectoryReader(configuration);
        this.segmentWriter = new LiveViewCheckpointMetaSegmentWriter(configuration);
    }

    /**
     * Registers a newly published segment with the number of logical roots that
     * reference it in the candidate generation.
     */
    public void addSegment(long segmentId, long fileLength, long referenceCount) {
        ensureBegun();
        if (segmentId < 0 || fileLength <= 0 || referenceCount <= 0) {
            throw CairoException.critical(0)
                    .put("invalid live view checkpoint segment directory entry")
                    .put(" [segmentId=").put(segmentId)
                    .put(", fileLength=").put(fileLength)
                    .put(", referenceCount=").put(referenceCount)
                    .put(']');
        }
        int index = stagedIndexOf(segmentId);
        if (index >= 0 || reader.find(segmentId, lookupEntry)) {
            throw CairoException.critical(0)
                    .put("duplicate live view checkpoint data segment, segmentId=")
                    .put(segmentId);
        }
        index = -index - 1;
        insertStaged(
                index,
                segmentId,
                fileLength,
                referenceCount,
                LiveViewCheckpointSegmentDirectory.RETIRE_GENERATION_NONE,
                STAGED_FLAG_INSERT
        );
    }

    /**
     * Applies one generation's root replacement. Repeated references to pages in
     * the same segment count once for each root side.
     */
    public void applyRootReferenceChanges(
            @NotNull LongList removedRootSegmentIds,
            @NotNull LongList addedRootSegmentIds,
            long generation
    ) {
        ensureBegun();
        if (generation < 0) {
            throw CairoException.critical(0)
                    .put("live view checkpoint retire generation must be non-negative, was ")
                    .put(generation);
        }
        removedSegmentIds.clear();
        for (int i = 0, n = removedRootSegmentIds.size(); i < n; i++) {
            final long segmentId = removedRootSegmentIds.getQuick(i);
            validateReferenceSegmentId(segmentId);
            removedSegmentIds.add(segmentId);
        }
        addedSegmentIds.clear();
        for (int i = 0, n = addedRootSegmentIds.size(); i < n; i++) {
            final long segmentId = addedRootSegmentIds.getQuick(i);
            validateReferenceSegmentId(segmentId);
            addedSegmentIds.add(segmentId);
        }

        // Validate the complete transaction before mutating a count. A failed
        // candidate build must leave the reusable directory image untouched.
        for (int i = 0, n = removedSegmentIds.size(); i < n; i++) {
            final long segmentId = removedSegmentIds.get(i);
            final int index = stageExisting(segmentId, false);
            if (staged.getQuick(index * STAGED_STRIDE + STAGED_REFERENCE_COUNT) <= 0) {
                throw CairoException.critical(0)
                        .put("live view checkpoint data segment reference count underflow, segmentId=")
                        .put(segmentId);
            }
        }
        for (int i = 0, n = addedSegmentIds.size(); i < n; i++) {
            final long segmentId = addedSegmentIds.get(i);
            final int index = stageExisting(segmentId, true);
            long count = staged.getQuick(index * STAGED_STRIDE + STAGED_REFERENCE_COUNT);
            if (removedSegmentIds.contains(segmentId)) {
                count--;
            }
            if (count == Long.MAX_VALUE) {
                throw CairoException.critical(0)
                        .put("live view checkpoint data segment reference count overflow, segmentId=")
                        .put(segmentId);
            }
        }

        for (int i = 0, n = removedSegmentIds.size(); i < n; i++) {
            addReference(removedSegmentIds.get(i), -1, generation);
        }
        for (int i = 0, n = addedSegmentIds.size(); i < n; i++) {
            addReference(addedSegmentIds.get(i), 1, generation);
        }
    }

    /**
     * Starts a publication against the generation rooted at {@code oldRoot}
     * (null for a fresh catalogue), discarding any staged mutations.
     */
    public void begin(@NotNull LiveViewCheckpointPageRef oldRoot) {
        staged.clear();
        this.oldRoot.of(oldRoot.getSegmentId(), oldRoot.getOffset(), oldRoot.getLength());
        reader.of(checkpointsDir, oldRoot);
        lastSegmentBytes = 0;
        lastSegmentPageCount = 0;
        isBegun = true;
    }

    @Override
    public void close() {
        Misc.free(reader);
        Misc.free(segmentWriter);
        Misc.free(checkpointsDir);
        staged.clear();
        isBegun = false;
    }

    /**
     * @return byte size of the metadata segment the last publication wrote, or
     * {@code 0} when it staged no change and reused the prior root
     */
    public long getLastSegmentBytes() {
        return lastSegmentBytes;
    }

    /**
     * @return number of new metadata pages the last publication wrote (its
     * copy-on-write cost); far below the catalogue's node count, which is the
     * point of the tree
     */
    public int getLastSegmentPageCount() {
        return lastSegmentPageCount;
    }

    /**
     * Reference count of {@code segmentId} in the candidate generation, staged
     * mutations included.
     */
    public long getReferenceCount(long segmentId) {
        final int index = stagedIndexOf(segmentId);
        return index >= 0
                ? staged.getQuick(index * STAGED_STRIDE + STAGED_REFERENCE_COUNT)
                : required(segmentId).referenceCount;
    }

    /**
     * Retire generation of {@code segmentId} in the candidate generation, staged
     * mutations included.
     */
    public long getRetireGeneration(long segmentId) {
        final int index = stagedIndexOf(segmentId);
        return index >= 0
                ? staged.getQuick(index * STAGED_STRIDE + STAGED_RETIRE_GENERATION)
                : required(segmentId).retireGeneration;
    }

    /**
     * Points the writer at a live view's {@code _checkpoints} directory.
     */
    public void of(@Transient @NotNull Path checkpointsDir) {
        this.checkpointsDir.of(checkpointsDir);
        isBegun = false;
    }

    /**
     * Writes the staged mutations as new pages of metadata segment
     * {@code newSegmentId}, which must be unused, and fills {@code newRootOut}
     * with the new tree root. A publication that staged nothing writes no segment
     * and reuses {@code oldRoot}.
     */
    public void publish(long newSegmentId, @NotNull LiveViewCheckpointPageRef newRootOut) {
        ensureBegun();
        isBegun = false;
        final int stagedCount = staged.size() / STAGED_STRIDE;
        if (stagedCount == 0) {
            newRootOut.of(oldRoot.getSegmentId(), oldRoot.getOffset(), oldRoot.getLength());
            lastSegmentBytes = 0;
            lastSegmentPageCount = 0;
            return;
        }
        segmentWriter.of(checkpointsDir, newSegmentId);
        lastSegmentPageCount = 0;
        if (oldRoot.isNull()) {
            buildFreshLeaf(stagedCount);
        } else {
            applyRec(oldRoot.getSegmentId(), oldRoot.getOffset(), oldRoot.getLength(), 0, stagedCount, 0);
        }

        // Collapse the emitted level into one root, adding a level whenever the
        // pieces a split produced no longer fit one node.
        LongList level = outAt(0);
        int depth = 0;
        while (level.size() > CHILD_STRIDE) {
            final LiveViewCheckpointSegmentDirectoryNode parent = builderAt(depth);
            parent.resetInternal();
            for (int i = 0, n = level.size(); i < n; i += CHILD_STRIDE) {
                parent.appendChild(
                        level.getQuick(i),
                        level.getQuick(i + 1),
                        level.getQuick(i + 2),
                        level.getQuick(i + 3)
                );
            }
            final LongList next = outAt(depth + 1);
            next.clear();
            emitNodes(parent, pieceAt(depth), next, internalCapacity);
            level = next;
            depth++;
        }
        newRootOut.of(level.getQuick(1), level.getQuick(2), (int) level.getQuick(3));
        lastSegmentBytes = segmentWriter.commit();
    }

    private static LiveViewCheckpointSegmentDirectoryNode[] growNodes(
            LiveViewCheckpointSegmentDirectoryNode[] src,
            int size
    ) {
        if (src.length >= size) {
            return src;
        }
        final LiveViewCheckpointSegmentDirectoryNode[] dst = new LiveViewCheckpointSegmentDirectoryNode[size];
        System.arraycopy(src, 0, dst, 0, src.length);
        return dst;
    }

    private static void validateReferenceSegmentId(long segmentId) {
        if (segmentId < 0) {
            throw CairoException.critical(0)
                    .put("live view checkpoint data segment reference id must be non-negative, was ")
                    .put(segmentId);
        }
    }

    private void addReference(long segmentId, int delta, long generation) {
        final int base = stagedIndexOf(segmentId) * STAGED_STRIDE;
        final long count = staged.getQuick(base + STAGED_REFERENCE_COUNT) + delta;
        staged.setQuick(base + STAGED_REFERENCE_COUNT, count);
        staged.setQuick(
                base + STAGED_RETIRE_GENERATION,
                count == 0 ? generation : LiveViewCheckpointSegmentDirectory.RETIRE_GENERATION_NONE
        );
    }

    /**
     * Rewrites the subtree at {@code (seg, off, len)} against staged records
     * {@code [lo, hi)}, emitting the pages that replace it into
     * {@code outAt(depth)} as {@code (minKey, segmentId, offset, length)}
     * quadruples. A subtree no staged record falls into is never read and keeps
     * its existing page reference.
     */
    private void applyRec(long seg, long off, long len, int lo, int hi, int depth) {
        final LiveViewCheckpointSegmentDirectoryNode node = nodeAt(depth);
        reader.openAndDecode(seg, off, len, node);
        final LongList out = outAt(depth);
        out.clear();
        if (node.isLeaf()) {
            for (int r = lo; r < hi; r++) {
                final int base = r * STAGED_STRIDE;
                final long segmentId = staged.getQuick(base + STAGED_SEGMENT_ID);
                final int pos = node.findEntry(segmentId);
                if (pos >= 0) {
                    node.replaceEntryPayloadAt(
                            pos,
                            staged.getQuick(base + STAGED_FILE_LENGTH),
                            staged.getQuick(base + STAGED_REFERENCE_COUNT),
                            staged.getQuick(base + STAGED_RETIRE_GENERATION)
                    );
                } else {
                    if (staged.getQuick(base + STAGED_FLAGS) != STAGED_FLAG_INSERT) {
                        // The record was staged from a lookup that found the
                        // segment, so descending to a leaf without it means the
                        // tree disagrees with itself.
                        throw CairoException.critical(CairoException.LV_CHECKPOINT_TIMELINE_INVALID)
                                .put("live view checkpoint segment directory lost a catalogued segment, segmentId=")
                                .put(segmentId);
                    }
                    node.insertEntryAt(
                            node.leafInsertPosition(segmentId),
                            segmentId,
                            staged.getQuick(base + STAGED_FILE_LENGTH),
                            staged.getQuick(base + STAGED_REFERENCE_COUNT),
                            staged.getQuick(base + STAGED_RETIRE_GENERATION)
                    );
                }
            }
            emitNodes(node, pieceAt(depth), out, leafCapacity);
            return;
        }
        final int count = node.count();
        if (count == 0) {
            throw CairoException.critical(CairoException.LV_CHECKPOINT_TIMELINE_INVALID)
                    .put("live view checkpoint segment directory node is empty");
        }
        final LiveViewCheckpointSegmentDirectoryNode parent = builderAt(depth);
        parent.resetInternal();
        int r = lo;
        for (int ci = 0; ci < count; ci++) {
            final int subLo = r;
            if (ci + 1 < count) {
                final long nextMinSegmentId = node.childMinSegmentId[ci + 1];
                while (r < hi && staged.getQuick(r * STAGED_STRIDE + STAGED_SEGMENT_ID) < nextMinSegmentId) {
                    r++;
                }
            } else {
                r = hi;
            }
            if (subLo == r) {
                parent.appendChild(
                        node.childMinSegmentId[ci],
                        node.childSegmentId[ci],
                        node.childOffset[ci],
                        node.childLength[ci]
                );
                continue;
            }
            applyRec(node.childSegmentId[ci], node.childOffset[ci], node.childLength[ci], subLo, r, depth + 1);
            final LongList childOut = outAt(depth + 1);
            for (int i = 0, n = childOut.size(); i < n; i += CHILD_STRIDE) {
                parent.appendChild(
                        childOut.getQuick(i),
                        childOut.getQuick(i + 1),
                        childOut.getQuick(i + 2),
                        childOut.getQuick(i + 3)
                );
            }
        }
        emitNodes(parent, pieceAt(depth), out, internalCapacity);
    }

    private void buildFreshLeaf(int stagedCount) {
        final LiveViewCheckpointSegmentDirectoryNode leaf = nodeAt(0);
        leaf.resetLeaf();
        for (int r = 0; r < stagedCount; r++) {
            final int base = r * STAGED_STRIDE;
            if (staged.getQuick(base + STAGED_FLAGS) != STAGED_FLAG_INSERT) {
                throw CairoException.critical(0)
                        .put("cannot re-reference a segment of an empty live view checkpoint directory, segmentId=")
                        .put(staged.getQuick(base + STAGED_SEGMENT_ID));
            }
            leaf.insertEntryAt(
                    leaf.count(),
                    staged.getQuick(base + STAGED_SEGMENT_ID),
                    staged.getQuick(base + STAGED_FILE_LENGTH),
                    staged.getQuick(base + STAGED_REFERENCE_COUNT),
                    staged.getQuick(base + STAGED_RETIRE_GENERATION)
            );
        }
        final LongList out = outAt(0);
        out.clear();
        emitNodes(leaf, pieceAt(0), out, leafCapacity);
    }

    private LiveViewCheckpointSegmentDirectoryNode builderAt(int depth) {
        builderPool = growNodes(builderPool, depth + 1);
        LiveViewCheckpointSegmentDirectoryNode node = builderPool[depth];
        if (node == null) {
            node = new LiveViewCheckpointSegmentDirectoryNode();
            builderPool[depth] = node;
        }
        return node;
    }

    /**
     * Writes {@code node} as one page when it fits {@code capacity}, otherwise
     * breaks it into the fewest equal pieces that do, appending one
     * {@code (minKey, ref)} quadruple per written page to {@code out}.
     */
    private void emitNodes(
            LiveViewCheckpointSegmentDirectoryNode node,
            LiveViewCheckpointSegmentDirectoryNode piece,
            LongList out,
            int capacity
    ) {
        final int count = node.count();
        assert count > 0;
        if (count <= capacity) {
            writePage(node, out);
            return;
        }
        final int parts = (count + capacity - 1) / capacity;
        final int perPart = (count + parts - 1) / parts;
        for (int start = 0; start < count; start += perPart) {
            node.copyRangeInto(piece, start, Math.min(count, start + perPart));
            writePage(piece, out);
        }
    }

    private void ensureBegun() {
        if (!isBegun) {
            throw CairoException.critical(0)
                    .put("live view checkpoint segment directory publication has not begun");
        }
    }

    private void insertStaged(
            int index,
            long segmentId,
            long fileLength,
            long referenceCount,
            long retireGeneration,
            long flags
    ) {
        final int base = index * STAGED_STRIDE;
        staged.insert(base, STAGED_STRIDE);
        staged.setQuick(base + STAGED_SEGMENT_ID, segmentId);
        staged.setQuick(base + STAGED_FILE_LENGTH, fileLength);
        staged.setQuick(base + STAGED_REFERENCE_COUNT, referenceCount);
        staged.setQuick(base + STAGED_RETIRE_GENERATION, retireGeneration);
        staged.setQuick(base + STAGED_FLAGS, flags);
    }

    private LiveViewCheckpointSegmentDirectoryNode nodeAt(int depth) {
        nodePool = growNodes(nodePool, depth + 1);
        LiveViewCheckpointSegmentDirectoryNode node = nodePool[depth];
        if (node == null) {
            node = new LiveViewCheckpointSegmentDirectoryNode();
            nodePool[depth] = node;
        }
        return node;
    }

    private LongList outAt(int depth) {
        if (depth >= outPool.length) {
            final LongList[] grown = new LongList[depth + 1];
            System.arraycopy(outPool, 0, grown, 0, outPool.length);
            outPool = grown;
        }
        LongList list = outPool[depth];
        if (list == null) {
            list = new LongList();
            outPool[depth] = list;
        }
        return list;
    }

    private LiveViewCheckpointSegmentDirectoryNode pieceAt(int depth) {
        piecePool = growNodes(piecePool, depth + 1);
        LiveViewCheckpointSegmentDirectoryNode node = piecePool[depth];
        if (node == null) {
            node = new LiveViewCheckpointSegmentDirectoryNode();
            piecePool[depth] = node;
        }
        return node;
    }

    private LiveViewCheckpointSegmentDirectoryEntry required(long segmentId) {
        if (!reader.find(segmentId, lookupEntry)) {
            throw CairoException.critical(0)
                    .put("unknown live view checkpoint data segment, segmentId=")
                    .put(segmentId);
        }
        return lookupEntry;
    }

    /**
     * Ensures the already-catalogued {@code segmentId} has a staged record,
     * loading its current values from the pinned tree on first touch. Staging
     * alone changes nothing: the record starts as a copy of what the tree holds.
     */
    private int stageExisting(long segmentId, boolean isAdd) {
        int index = stagedIndexOf(segmentId);
        if (index >= 0) {
            return index;
        }
        if (!reader.find(segmentId, lookupEntry)) {
            throw CairoException.critical(0)
                    .put("cannot ").put(isAdd ? "add" : "remove")
                    .put(" reference to unknown live view checkpoint data segment, segmentId=")
                    .put(segmentId);
        }
        index = -index - 1;
        insertStaged(
                index,
                lookupEntry.segmentId,
                lookupEntry.fileLength,
                lookupEntry.referenceCount,
                lookupEntry.retireGeneration,
                STAGED_FLAG_NONE
        );
        return index;
    }

    /**
     * Returns the staged record index for {@code segmentId}, or
     * {@code -insertionPoint - 1}. Records are kept sorted by id so a publication
     * can walk them alongside the tree in one pass.
     */
    private int stagedIndexOf(long segmentId) {
        int lo = 0;
        int hi = staged.size() / STAGED_STRIDE - 1;
        while (lo <= hi) {
            final int mid = (lo + hi) >>> 1;
            final long value = staged.getQuick(mid * STAGED_STRIDE + STAGED_SEGMENT_ID);
            if (value < segmentId) {
                lo = mid + 1;
            } else if (value > segmentId) {
                hi = mid - 1;
            } else {
                return mid;
            }
        }
        return -lo - 1;
    }

    private void writePage(LiveViewCheckpointSegmentDirectoryNode node, LongList out) {
        node.writeTo(segmentWriter, pageRef);
        lastSegmentPageCount++;
        out.add(node.minKey(), pageRef.getSegmentId(), pageRef.getOffset(), pageRef.getLength());
    }
}
