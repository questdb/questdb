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
import java.util.Arrays;

/**
 * Logarithmic reader for a generation-pinned persistent partition map.
 */
public class LiveViewCheckpointPartitionMapReader implements Closeable {

    /**
     * Decoded nodes one bound root memoises. A seal looks the same root up once per
     * partition - both to find the previous boundary's entry and to carry the old
     * root's entry into the new one - and every lookup used to re-walk the same
     * root-to-leaf path, checksumming and decoding a metadata page per level and
     * rebuilding the whole page's entry image, state page references included, to
     * read one entry out of it.
     * <p>
     * Sized to hold a descent rather than a working set: the pages one path touches
     * stay resident, and a lookup that leaves the path evicts in clock order. The
     * memo covers one root at a time, so a page cached under a root cannot outlive
     * it - {@link #find} drops the memo as soon as another root is asked for.
     */
    private static final int NODE_CACHE_SIZE = 4;
    private static final int SEGMENT_CACHE_SIZE = 8;
    private final Path checkpointsDir = new Path();
    private final CairoConfiguration configuration;
    private final LiveViewCheckpointPartitionMapNode navNode = new LiveViewCheckpointPartitionMapNode();
    private final LiveViewCheckpointPartitionMapNode[] nodeCache = new LiveViewCheckpointPartitionMapNode[NODE_CACHE_SIZE];
    private final long[] nodeCacheOffset = new long[NODE_CACHE_SIZE];
    private final long[] nodeCacheSegmentId = new long[NODE_CACHE_SIZE];
    private final LiveViewCheckpointPartitionMapEntry scratchEntry = new LiveViewCheckpointPartitionMapEntry();
    private final long[] segmentIds = new long[SEGMENT_CACHE_SIZE];
    private final LiveViewCheckpointMetaSegmentReader[] segmentReaders = new LiveViewCheckpointMetaSegmentReader[SEGMENT_CACHE_SIZE];
    private long boundRootOffset = -1;
    private long boundRootSegmentId = -1;
    private int nodeCacheClock;
    private LiveViewCheckpointPartitionMapNode[] nodePool = new LiveViewCheckpointPartitionMapNode[0];
    private int segmentClock;

    public LiveViewCheckpointPartitionMapReader(@NotNull CairoConfiguration configuration) {
        this.configuration = configuration;
        for (int i = 0; i < SEGMENT_CACHE_SIZE; i++) {
            segmentIds[i] = -1;
        }
        clearNodeCache();
    }

    @Override
    public void close() {
        for (int i = 0; i < SEGMENT_CACHE_SIZE; i++) {
            segmentReaders[i] = Misc.free(segmentReaders[i]);
            segmentIds[i] = -1;
        }
        clearNodeCache();
        Arrays.fill(nodeCache, null);
        Misc.free(checkpointsDir);
    }

    /**
     * Unmaps every cached metadata segment while keeping the readers themselves,
     * so a reader that outlives one restore holds no mapping into files a later
     * retire, repair or compaction deletes.
     */
    public void detach() {
        for (int i = 0; i < SEGMENT_CACHE_SIZE; i++) {
            if (segmentReaders[i] != null) {
                segmentReaders[i].close();
            }
            segmentIds[i] = -1;
        }
        segmentClock = 0;
        clearNodeCache();
    }

    public boolean find(
            @NotNull LiveViewCheckpointPageRef rootRef,
            @NotNull byte[] key,
            @NotNull LiveViewCheckpointPartitionMapEntry out
    ) {
        if (rootRef.isNull()) {
            return false;
        }
        long segmentId = rootRef.getSegmentId();
        long offset = rootRef.getOffset();
        int length = rootRef.getLength();
        if (boundRootSegmentId != segmentId || boundRootOffset != offset) {
            // A page is immutable and the map is copy-on-write, so what one root
            // reaches cannot change under the memo. What a memo may not survive is
            // a rebuilt timeline re-minting the ids it keyed on, so the memo starts
            // over whenever another root is asked for.
            clearNodeCache();
            boundRootSegmentId = segmentId;
            boundRootOffset = offset;
        }
        while (true) {
            final LiveViewCheckpointPartitionMapNode node = decodedNode(segmentId, offset, length);
            if (node.isLeaf()) {
                final int index = node.find(key);
                if (index < 0) {
                    return false;
                }
                node.copyEntryTo(index, out);
                return true;
            }
            final int child = node.childIndex(key);
            final LiveViewCheckpointPageRef ref = node.childRefs[child];
            segmentId = ref.getSegmentId();
            offset = ref.getOffset();
            length = ref.getLength();
        }
    }

    public void iterateAll(@NotNull LiveViewCheckpointPageRef rootRef, @NotNull Visitor visitor) {
        if (!rootRef.isNull()) {
            iterate(rootRef, visitor, 0);
        }
    }

    public void of(@Transient @NotNull Path checkpointsDir) {
        this.checkpointsDir.of(checkpointsDir);
        for (int i = 0; i < SEGMENT_CACHE_SIZE; i++) {
            segmentIds[i] = -1;
        }
        segmentClock = 0;
        clearNodeCache();
    }

    public int rootChildCount(@NotNull LiveViewCheckpointPageRef rootRef) {
        if (rootRef.isNull()) {
            return 0;
        }
        openAndDecode(rootRef.getSegmentId(), rootRef.getOffset(), rootRef.getLength(), navNode);
        return navNode.isLeaf() ? 0 : navNode.count();
    }

    public void rootChildRef(@NotNull LiveViewCheckpointPageRef rootRef, int index, @NotNull LiveViewCheckpointPageRef out) {
        openAndDecode(rootRef.getSegmentId(), rootRef.getOffset(), rootRef.getLength(), navNode);
        if (navNode.isLeaf() || index < 0 || index >= navNode.count()) {
            throw LiveViewCheckpointMetadata.invalid("partition map root child index out of bounds, index=").put(index);
        }
        final LiveViewCheckpointPageRef ref = navNode.childRefs[index];
        out.of(ref.getSegmentId(), ref.getOffset(), ref.getLength());
    }

    public long size(@NotNull LiveViewCheckpointPageRef rootRef) {
        return rootRef.isNull() ? 0 : size(rootRef, 0);
    }

    void openAndDecode(long segmentId, long offset, int length, LiveViewCheckpointPartitionMapNode node) {
        final LiveViewCheckpointMetaSegmentReader reader = readerFor(segmentId);
        reader.openPageAt(offset, length);
        node.decode(reader);
    }

    private void clearNodeCache() {
        Arrays.fill(nodeCacheSegmentId, -1);
        Arrays.fill(nodeCacheOffset, -1);
        nodeCacheClock = 0;
        boundRootSegmentId = -1;
        boundRootOffset = -1;
    }

    /**
     * @return the decoded image of the page at {@code segmentId}/{@code offset},
     * out of the memo when the bound root already reached it. The caller must not
     * hold the node across another lookup, which may recycle its slot.
     */
    private LiveViewCheckpointPartitionMapNode decodedNode(long segmentId, long offset, int length) {
        for (int i = 0; i < NODE_CACHE_SIZE; i++) {
            if (nodeCacheSegmentId[i] == segmentId && nodeCacheOffset[i] == offset) {
                return nodeCache[i];
            }
        }
        final int slot = nodeCacheClock;
        nodeCacheClock = slot + 1 == NODE_CACHE_SIZE ? 0 : slot + 1;
        if (nodeCache[slot] == null) {
            nodeCache[slot] = new LiveViewCheckpointPartitionMapNode();
        }
        // A rejected page leaves the slot holding a half-decoded node, so drop the
        // slot's identity before the decode rather than let a throw leave a memo
        // entry claiming a page it does not hold.
        nodeCacheSegmentId[slot] = -1;
        nodeCacheOffset[slot] = -1;
        openAndDecode(segmentId, offset, length, nodeCache[slot]);
        nodeCacheSegmentId[slot] = segmentId;
        nodeCacheOffset[slot] = offset;
        return nodeCache[slot];
    }

    private void iterate(LiveViewCheckpointPageRef ref, Visitor visitor, int depth) {
        final LiveViewCheckpointPartitionMapNode node = nodeAt(depth);
        openAndDecode(ref.getSegmentId(), ref.getOffset(), ref.getLength(), node);
        if (node.isLeaf()) {
            for (int i = 0; i < node.count(); i++) {
                node.copyEntryTo(i, scratchEntry);
                visitor.onEntry(scratchEntry);
            }
        } else {
            for (int i = 0; i < node.count(); i++) {
                iterate(node.childRefs[i], visitor, depth + 1);
            }
        }
    }

    private LiveViewCheckpointPartitionMapNode nodeAt(int depth) {
        if (depth >= nodePool.length) {
            final LiveViewCheckpointPartitionMapNode[] grown = new LiveViewCheckpointPartitionMapNode[depth + 1];
            System.arraycopy(nodePool, 0, grown, 0, nodePool.length);
            nodePool = grown;
        }
        if (nodePool[depth] == null) {
            nodePool[depth] = new LiveViewCheckpointPartitionMapNode();
        }
        return nodePool[depth];
    }

    private LiveViewCheckpointMetaSegmentReader readerFor(long segmentId) {
        for (int i = 0; i < SEGMENT_CACHE_SIZE; i++) {
            if (segmentIds[i] == segmentId && segmentReaders[i] != null) {
                return segmentReaders[i];
            }
        }
        final int slot = segmentClock;
        segmentClock = segmentClock + 1 == SEGMENT_CACHE_SIZE ? 0 : segmentClock + 1;
        if (segmentReaders[slot] == null) {
            segmentReaders[slot] = new LiveViewCheckpointMetaSegmentReader(configuration);
        }
        // Invalidate the slot BEFORE the open. of() closes and resets the reader up front and can
        // then throw, which would otherwise leave the slot still advertising the previous, healthy
        // segment id against a closed reader - so one corrupt segment poisons a healthy one, and a
        // later lookup can escalate that into "no usable root".
        segmentIds[slot] = -1;
        segmentReaders[slot].of(checkpointsDir, segmentId);
        segmentIds[slot] = segmentId;
        return segmentReaders[slot];
    }

    private long size(LiveViewCheckpointPageRef ref, int depth) {
        final LiveViewCheckpointPartitionMapNode node = nodeAt(depth);
        openAndDecode(ref.getSegmentId(), ref.getOffset(), ref.getLength(), node);
        if (node.isLeaf()) {
            return node.count();
        }
        long size = 0;
        for (int i = 0; i < node.count(); i++) {
            size += size(node.childRefs[i], depth + 1);
        }
        return size;
    }

    @FunctionalInterface
    public interface Visitor {
        void onEntry(LiveViewCheckpointPartitionMapEntry entry);
    }
}
