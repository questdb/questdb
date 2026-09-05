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
import org.jetbrains.annotations.TestOnly;

import java.io.Closeable;

/**
 * Copy-on-write builder for partition maps. A batch is applied to one transient
 * tree of copied paths and serialized post-order, so partitions touched by the
 * same checkpoint share copied ancestors and no intermediate metadata pages are
 * leaked. Untouched child references remain byte-for-byte identical.
 * <p>
 * A build also reports what it took away. Every decoded page it either rewrites
 * or drops stops being reachable from the map it produces, and
 * {@link #getLastReleasedSegmentIds()} lists the segment of each such page, once
 * per page. A page it decoded and left alone - the mutation turned out to be a
 * no-op, or the descent never reached it - is not listed, because the new map
 * still names it. Together with {@link #getLastSegmentPageCount()} that is
 * exactly the delta a caller needs to keep a per-segment reachable-page count
 * without walking the map.
 */
public class LiveViewCheckpointPartitionMapWriter implements Closeable {

    private final Path checkpointsDir = new Path();
    private final int internalCapacity;
    private final int leafCapacity;
    private final LiveViewCheckpointPartitionMapReader reader;
    private final LiveViewCheckpointPartitionMapObjectPool objectPool;
    private final LongList releasedSegmentIds = new LongList();
    private final LiveViewCheckpointMetaSegmentWriter segmentWriter;
    private int lastSegmentPageCount;
    private boolean isPreparedChanged;
    private LiveViewCheckpointPartitionMapNode mutateSibling;
    private LiveViewCheckpointPartitionMapNode preparedRoot;

    public LiveViewCheckpointPartitionMapWriter(@NotNull CairoConfiguration configuration) {
        this(configuration, 64, 64, new LiveViewCheckpointPartitionMapObjectPool());
    }

    public LiveViewCheckpointPartitionMapWriter(
            @NotNull CairoConfiguration configuration,
            int leafCapacity,
            int internalCapacity
    ) {
        this(configuration, leafCapacity, internalCapacity, new LiveViewCheckpointPartitionMapObjectPool());
    }

    @TestOnly
    public LiveViewCheckpointPartitionMapWriter(
            @NotNull CairoConfiguration configuration,
            int leafCapacity,
            int internalCapacity,
            @NotNull LiveViewCheckpointPartitionMapWriter poolOwner
    ) {
        this(configuration, leafCapacity, internalCapacity, poolOwner.objectPool);
    }

    LiveViewCheckpointPartitionMapWriter(
            @NotNull CairoConfiguration configuration,
            @NotNull LiveViewCheckpointPartitionMapObjectPool objectPool
    ) {
        this(configuration, 64, 64, objectPool);
    }

    private LiveViewCheckpointPartitionMapWriter(
            @NotNull CairoConfiguration configuration,
            int leafCapacity,
            int internalCapacity,
            @NotNull LiveViewCheckpointPartitionMapObjectPool objectPool
    ) {
        if (leafCapacity < 2 || internalCapacity < 2) {
            throw CairoException.critical(0).put("live view checkpoint partition map capacity must be at least 2");
        }
        this.leafCapacity = leafCapacity;
        this.internalCapacity = internalCapacity;
        this.objectPool = objectPool;
        this.reader = new LiveViewCheckpointPartitionMapReader(configuration);
        this.segmentWriter = new LiveViewCheckpointMetaSegmentWriter(configuration);
    }

    public void apply(
            @NotNull LiveViewCheckpointPageRef oldRoot,
            @NotNull LiveViewCheckpointMutationArena mutations,
            long newSegmentId,
            @NotNull LiveViewCheckpointPageRef newRootOut
    ) {
        lastSegmentPageCount = 0;
        prepare(oldRoot, mutations);
        if (!isPreparedChanged) {
            copy(oldRoot, newRootOut);
            return;
        }
        if (preparedRoot.count() == 0) {
            releaseSource(preparedRoot);
            newRootOut.clear();
            return;
        }
        segmentWriter.of(checkpointsDir, newSegmentId);
        writePrepared(preparedRoot, segmentWriter, newRootOut);
        segmentWriter.commit();
    }

    @Override
    public void close() {
        Misc.free(reader);
        Misc.free(segmentWriter);
        Misc.free(checkpointsDir);
    }

    /**
     * Releases every mapping and in-flight segment this build held while keeping
     * the reader, writer and pooled node shells, so the next build reuses them
     * without holding a mapping into a file a retire or compaction unlinks.
     */
    public void detach() {
        reader.detach();
        segmentWriter.discard();
        releasedSegmentIds.clear();
    }

    /**
     * @return the segment of every published page the last build superseded, one
     * element per page, in no particular order. Empty when the build changed
     * nothing.
     */
    public @NotNull LongList getLastReleasedSegmentIds() {
        return releasedSegmentIds;
    }

    public int getLastSegmentPageCount() {
        return lastSegmentPageCount;
    }

    @TestOnly
    public int getObjectPoolIdentityForTest() {
        return System.identityHashCode(objectPool);
    }

    @TestOnly
    public int getRetainedObjectCountForTest() {
        return objectPool.getRetainedObjectCount();
    }

    public void of(@Transient @NotNull Path checkpointsDir) {
        this.checkpointsDir.of(checkpointsDir);
        reader.of(checkpointsDir);
    }

    boolean applyToOpenSegment(
            @NotNull LiveViewCheckpointPageRef oldRoot,
            @NotNull LiveViewCheckpointMutationArena mutations,
            @NotNull LiveViewCheckpointMetaSegmentWriter writer,
            @NotNull LiveViewCheckpointPageRef newRootOut
    ) {
        lastSegmentPageCount = 0;
        prepare(oldRoot, mutations);
        if (!isPreparedChanged) {
            copy(oldRoot, newRootOut);
            return false;
        }
        writePrepared(preparedRoot, writer, newRootOut);
        return true;
    }

    private LiveViewCheckpointPartitionMapNode load(
            LiveViewCheckpointPageRef ref,
            LiveViewCheckpointMutationArena mutations
    ) {
        final LiveViewCheckpointPartitionMapNode node = nextNode();
        reader.openAndDecode(
                ref.getSegmentId(),
                ref.getOffset(),
                ref.getLength(),
                node,
                mutations,
                objectPool.decodedPageRefs()
        );
        node.sourceSegmentId = ref.getSegmentId();
        return node;
    }

    private boolean mutate(
            LiveViewCheckpointPartitionMapNode node,
            LiveViewCheckpointMutationArena mutations,
            int mutationIndex
    ) {
        mutateSibling = null;
        if (node.isLeaf()) {
            final int index = node.lowerBound(mutations, mutationIndex);
            final boolean exists = index < node.count() && node.keyEqualsAt(index, mutations, mutationIndex);
            if (mutations.operation(mutationIndex) == LiveViewCheckpointMutationArena.OP_REMOVE) {
                if (!exists) {
                    return false;
                }
                node.removeEntry(index);
            } else if (exists && node.valueEquals(index, mutations, mutationIndex)) {
                return false;
            } else {
                node.putEntry(index, mutations, mutationIndex);
            }
            if (node.count() > leafCapacity) {
                mutateSibling = nextNode();
                node.splitInto(mutateSibling);
            }
            return true;
        }

        final int childIndex = node.childIndex(mutations, mutationIndex);
        final LiveViewCheckpointPartitionMapNode existingDirty = node.childNodes[childIndex];
        final LiveViewCheckpointPartitionMapNode child =
                existingDirty != null ? existingDirty : load(node.childRefs[childIndex], mutations);
        if (!mutate(child, mutations, mutationIndex)) {
            return false;
        }
        final LiveViewCheckpointPartitionMapNode childSibling = mutateSibling;
        if (child.count() == 0) {
            // The child's own children were removed one at a time, each releasing
            // its page as it went, so the emptied node is the last page of the
            // subtree still to account for.
            releaseSource(child);
            node.removeChild(childIndex);
        } else {
            node.setChild(childIndex, child);
            if (childSibling != null) {
                node.insertChild(childIndex + 1, childSibling);
            }
        }
        if (node.count() > internalCapacity) {
            mutateSibling = nextNode();
            node.splitInto(mutateSibling);
        } else {
            mutateSibling = null;
        }
        return true;
    }

    private void prepare(LiveViewCheckpointPageRef oldRoot, LiveViewCheckpointMutationArena mutations) {
        releasedSegmentIds.clear();
        objectPool.reset();
        final int mutationCount = mutations.getMutationCount();
        if (mutationCount == 0) {
            preparedRoot = null;
            isPreparedChanged = false;
            return;
        }
        mutations.sortAndValidate();

        LiveViewCheckpointPartitionMapNode root;
        if (oldRoot.isNull()) {
            root = nextNode();
            root.resetLeaf();
        } else {
            LiveViewCheckpointMetadata.validateMetaRef(oldRoot, false, "partition map root");
            root = load(oldRoot, mutations);
        }
        boolean changed = false;
        for (int i = 0; i < mutationCount; i++) {
            final int mutationIndex = mutations.getSortedMutationIndex(i);
            if (mutations.operation(mutationIndex) == LiveViewCheckpointMutationArena.OP_DOMAIN) {
                continue;
            }
            if (mutate(root, mutations, mutationIndex)) {
                changed = true;
                if (mutateSibling != null) {
                    final LiveViewCheckpointPartitionMapNode newRoot = nextNode();
                    newRoot.resetInternal();
                    newRoot.insertChild(0, root);
                    newRoot.insertChild(1, mutateSibling);
                    root = newRoot;
                }
                while (!root.isLeaf() && root.count() == 1) {
                    // The collapsed root is not written and no parent names it,
                    // so its page goes with it. The promoted child is written
                    // whether or not a later mutation dirties it, and releases
                    // its own page at that point.
                    releaseSource(root);
                    root = root.childNodes[0] != null
                            ? root.childNodes[0]
                            : load(root.childRefs[0], mutations);
                }
            }
        }
        preparedRoot = root;
        isPreparedChanged = changed;
    }

    /**
     * Records that {@code node}'s decoded page stops being reachable, and marks
     * the node so a second visit cannot record it twice.
     */
    private void releaseSource(LiveViewCheckpointPartitionMapNode node) {
        if (node.sourceSegmentId != LiveViewCheckpointPartitionMapNode.NO_SOURCE_SEGMENT_ID) {
            releasedSegmentIds.add(node.sourceSegmentId);
            node.sourceSegmentId = LiveViewCheckpointPartitionMapNode.NO_SOURCE_SEGMENT_ID;
        }
    }

    private void serialize(
            LiveViewCheckpointPartitionMapNode node,
            LiveViewCheckpointMetaSegmentWriter writer,
            LiveViewCheckpointPageRef out
    ) {
        if (!node.isLeaf()) {
            for (int i = 0; i < node.count(); i++) {
                if (node.childNodes[i] != null) {
                    final LiveViewCheckpointPageRef childRef = nextPageRef();
                    serialize(node.childNodes[i], writer, childRef);
                    node.childRefs[i] = childRef;
                    node.childNodes[i] = null;
                }
            }
        }
        releaseSource(node);
        node.writeTo(writer, out);
        lastSegmentPageCount++;
    }

    private void writePrepared(
            LiveViewCheckpointPartitionMapNode root,
            LiveViewCheckpointMetaSegmentWriter writer,
            LiveViewCheckpointPageRef out
    ) {
        if (root.count() == 0) {
            releaseSource(root);
            out.clear();
        } else {
            serialize(root, writer, out);
        }
    }

    private static void copy(LiveViewCheckpointPageRef from, LiveViewCheckpointPageRef to) {
        to.of(from.getSegmentId(), from.getOffset(), from.getLength());
    }

    private LiveViewCheckpointPartitionMapNode nextNode() {
        return objectPool.nextNode();
    }

    private LiveViewCheckpointPageRef nextPageRef() {
        return objectPool.nextOutputPageRef();
    }

}
