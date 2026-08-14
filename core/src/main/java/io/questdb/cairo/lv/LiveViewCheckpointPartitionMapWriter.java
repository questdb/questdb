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
import java.util.Arrays;

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
    private final LongList releasedSegmentIds = new LongList();
    private final LiveViewCheckpointMetaSegmentWriter segmentWriter;
    private int lastSegmentPageCount;

    public LiveViewCheckpointPartitionMapWriter(@NotNull CairoConfiguration configuration) {
        this(configuration, 64, 64);
    }

    public LiveViewCheckpointPartitionMapWriter(
            @NotNull CairoConfiguration configuration,
            int leafCapacity,
            int internalCapacity
    ) {
        if (leafCapacity < 2 || internalCapacity < 2) {
            throw CairoException.critical(0).put("live view checkpoint partition map capacity must be at least 2");
        }
        this.leafCapacity = leafCapacity;
        this.internalCapacity = internalCapacity;
        this.reader = new LiveViewCheckpointPartitionMapReader(configuration);
        this.segmentWriter = new LiveViewCheckpointMetaSegmentWriter(configuration);
    }

    public void apply(
            @NotNull LiveViewCheckpointPageRef oldRoot,
            @NotNull Mutation[] mutations,
            int mutationCount,
            long newSegmentId,
            @NotNull LiveViewCheckpointPageRef newRootOut
    ) {
        lastSegmentPageCount = 0;
        final Prepared prepared = prepare(oldRoot, mutations, mutationCount);
        if (!prepared.changed) {
            copy(oldRoot, newRootOut);
            return;
        }
        if (prepared.root.count() == 0) {
            releaseSource(prepared.root);
            newRootOut.clear();
            return;
        }
        segmentWriter.of(checkpointsDir, newSegmentId);
        writePrepared(prepared.root, segmentWriter, newRootOut);
        segmentWriter.commit();
    }

    @Override
    public void close() {
        Misc.free(reader);
        Misc.free(segmentWriter);
        Misc.free(checkpointsDir);
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

    public void of(@Transient @NotNull Path checkpointsDir) {
        this.checkpointsDir.of(checkpointsDir);
        reader.of(checkpointsDir);
    }

    boolean applyToOpenSegment(
            @NotNull LiveViewCheckpointPageRef oldRoot,
            @NotNull Mutation[] mutations,
            int mutationCount,
            @NotNull LiveViewCheckpointMetaSegmentWriter writer,
            @NotNull LiveViewCheckpointPageRef newRootOut
    ) {
        lastSegmentPageCount = 0;
        final Prepared prepared = prepare(oldRoot, mutations, mutationCount);
        if (!prepared.changed) {
            copy(oldRoot, newRootOut);
            return false;
        }
        writePrepared(prepared.root, writer, newRootOut);
        return true;
    }

    private LiveViewCheckpointPartitionMapNode load(LiveViewCheckpointPageRef ref) {
        final LiveViewCheckpointPartitionMapNode node = new LiveViewCheckpointPartitionMapNode();
        reader.openAndDecode(ref.getSegmentId(), ref.getOffset(), ref.getLength(), node);
        node.sourceSegmentId = ref.getSegmentId();
        return node;
    }

    private boolean mutate(
            LiveViewCheckpointPartitionMapNode node,
            Mutation mutation,
            Split splitOut
    ) {
        splitOut.sibling = null;
        if (node.isLeaf()) {
            final int index = node.lowerBound(mutation.entry.getKey());
            final boolean exists = index < node.count()
                    && LiveViewCheckpointMetadata.compareBytes(node.keys[index], mutation.entry.getKey()) == 0;
            if (mutation.remove) {
                if (!exists) {
                    return false;
                }
                node.removeEntry(index);
            } else if (exists
                    && Arrays.equals(node.scalarStates[index], mutation.entry.getScalarState())
                    && LiveViewCheckpointPartitionMapEntry.refsEqual(node.statePageRefs[index], mutation.entry.statePageRefs())) {
                return false;
            } else {
                node.putEntry(index, mutation.entry);
            }
            if (node.count() > leafCapacity) {
                splitOut.sibling = node.split();
            }
            return true;
        }

        final int childIndex = node.childIndex(mutation.entry.getKey());
        final LiveViewCheckpointPartitionMapNode existingDirty = node.childNodes[childIndex];
        final LiveViewCheckpointPartitionMapNode child = existingDirty != null ? existingDirty : load(node.childRefs[childIndex]);
        final Split childSplit = new Split();
        if (!mutate(child, mutation, childSplit)) {
            return false;
        }
        if (child.count() == 0) {
            // The child's own children were removed one at a time, each releasing
            // its page as it went, so the emptied node is the last page of the
            // subtree still to account for.
            releaseSource(child);
            node.removeChild(childIndex);
        } else {
            node.setChild(childIndex, child);
            if (childSplit.sibling != null) {
                node.insertChild(childIndex + 1, childSplit.sibling);
            }
        }
        if (node.count() > internalCapacity) {
            splitOut.sibling = node.split();
        }
        return true;
    }

    private Prepared prepare(LiveViewCheckpointPageRef oldRoot, Mutation[] mutations, int mutationCount) {
        if (mutationCount < 0 || mutationCount > mutations.length) {
            throw CairoException.critical(0).put("invalid live view checkpoint partition mutation count, count=").put(mutationCount);
        }
        releasedSegmentIds.clear();
        if (mutationCount == 0) {
            return new Prepared(null, false);
        }
        final Mutation[] sorted = new Mutation[mutationCount];
        for (int i = 0; i < mutationCount; i++) {
            if (mutations[i] == null) {
                throw CairoException.critical(0).put("null live view checkpoint partition mutation");
            }
            mutations[i].validate();
            sorted[i] = mutations[i];
        }
        Arrays.sort(sorted, (a, b) -> LiveViewCheckpointMetadata.compareBytes(a.entry.getKey(), b.entry.getKey()));
        for (int i = 1; i < sorted.length; i++) {
            if (LiveViewCheckpointMetadata.compareBytes(sorted[i - 1].entry.getKey(), sorted[i].entry.getKey()) == 0) {
                throw CairoException.critical(0).put("duplicate live view checkpoint partition mutation key");
            }
        }

        LiveViewCheckpointPartitionMapNode root;
        if (oldRoot.isNull()) {
            root = new LiveViewCheckpointPartitionMapNode();
            root.resetLeaf();
        } else {
            LiveViewCheckpointMetadata.validateMetaRef(oldRoot, false, "partition map root");
            root = load(oldRoot);
        }
        boolean changed = false;
        final Split split = new Split();
        for (int i = 0; i < sorted.length; i++) {
            if (mutate(root, sorted[i], split)) {
                changed = true;
                if (split.sibling != null) {
                    final LiveViewCheckpointPartitionMapNode newRoot = new LiveViewCheckpointPartitionMapNode();
                    newRoot.resetInternal();
                    newRoot.insertChild(0, root);
                    newRoot.insertChild(1, split.sibling);
                    root = newRoot;
                }
                while (!root.isLeaf() && root.count() == 1) {
                    // The collapsed root is not written and no parent names it,
                    // so its page goes with it. The promoted child is written
                    // whether or not a later mutation dirties it, and releases
                    // its own page at that point.
                    releaseSource(root);
                    root = root.childNodes[0] != null ? root.childNodes[0] : load(root.childRefs[0]);
                }
            }
        }
        return new Prepared(root, changed);
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
                    final LiveViewCheckpointPageRef childRef = new LiveViewCheckpointPageRef();
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

    public static final class Mutation {
        private final LiveViewCheckpointPartitionMapEntry entry = new LiveViewCheckpointPartitionMapEntry();
        private boolean remove;

        public Mutation put(
                @NotNull byte[] key,
                @NotNull byte[] scalarState,
                @NotNull LiveViewCheckpointStatePageRef[] statePageRefs
        ) {
            entry.of(key, scalarState, statePageRefs);
            remove = false;
            return this;
        }

        public Mutation remove(@NotNull byte[] key) {
            entry.of(key, new byte[0], new LiveViewCheckpointStatePageRef[0]);
            remove = true;
            return this;
        }

        LiveViewCheckpointPartitionMapEntry entry() {
            return entry;
        }

        boolean isRemove() {
            return remove;
        }

        private void validate() {
            LiveViewCheckpointMetadata.validateByteArrayLength(entry.getKey().length, "partition key");
            LiveViewCheckpointMetadata.validateByteArrayLength(entry.getScalarState().length, "partition scalar state");
            if (entry.getStatePageCount() > LiveViewCheckpointMetadata.MAX_STATE_PAGE_REFS) {
                throw CairoException.critical(0).put("too many live view checkpoint partition state page references");
            }
            for (int i = 0; i < entry.getStatePageCount(); i++) {
                LiveViewCheckpointMetadata.validateStateRef(entry.getStatePageRef(i), false, "partition");
            }
        }
    }

    private static final class Prepared {
        private final boolean changed;
        private final LiveViewCheckpointPartitionMapNode root;

        private Prepared(LiveViewCheckpointPartitionMapNode root, boolean changed) {
            this.root = root;
            this.changed = changed;
        }
    }

    private static final class Split {
        private LiveViewCheckpointPartitionMapNode sibling;
    }
}
