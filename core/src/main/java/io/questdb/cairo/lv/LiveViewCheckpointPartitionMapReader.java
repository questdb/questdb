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
import io.questdb.std.LongList;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.Transient;
import io.questdb.std.str.Path;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.TestOnly;

import java.io.Closeable;

/**
 * Logarithmic reader for a generation-pinned persistent partition map.
 */
public class LiveViewCheckpointPartitionMapReader implements Closeable {

    /**
     * Key and scalar image bytes the decode pool of one node may keep once
     * {@link #detach()} ends an operation; a pool holding more drops every array.
     * <p>
     * While an operation runs, a pool keeps every array it lends: for every width, the
     * most arrays of that width a single page needed. A seal probes the previous root
     * in the order it walks its own keys rather than in key order, so it decodes a
     * different leaf on nearly every lookup, and the pool settles at the union of every
     * leaf's widths, which is more than any one page needs. A limit that applied within
     * the operation would make every decode past it allocate, so the pool only answers
     * to this limit when the operation ends: a view whose union exceeds it allocates
     * that union once per operation rather than on every decode.
     * <p>
     * The readers of a refresh worker live as long as the worker, so this, and not the
     * widths of the views the worker served before, bounds what an idle node keeps. A
     * seal over 32,768 keys of 32 to 512 characters pools about 8 MiB in one node.
     */
    public static final long MAX_NODE_RETAINED_BYTES = 16_777_216;
    /**
     * State page references the decode pool of one node may keep once {@link #detach()}
     * ends an operation, about 15 MiB with their array slots. A ring view over 1,024 keys
     * whose chunk counts spread from 1 to 256 pools about 214,000 references in one node.
     * {@link #MAX_NODE_RETAINED_BYTES} describes the policy.
     */
    public static final long MAX_NODE_RETAINED_STATE_PAGE_REFS = 262_144;
    /**
     * Levels the memo covers. Every tree a production capacity builds is far
     * shallower, but the writer accepts capacities as low as two, where a split
     * hands one child to the left node and an ascending build grows a level per
     * couple of keys. A descent past this point still answers, out of a scratch
     * node, rather than growing the memo with the tree.
     */
    private static final int MAX_MEMO_DEPTH = 64;
    private static final int SEGMENT_CACHE_SIZE = 8;
    private final Path checkpointsDir = new Path();
    private final CairoConfiguration configuration;
    private final LiveViewCheckpointPartitionMapNode deepNode = new LiveViewCheckpointPartitionMapNode();
    private final LiveViewCheckpointPartitionMapNode navNode = new LiveViewCheckpointPartitionMapNode();
    private final LiveViewCheckpointPartitionMapEntry scratchEntry = new LiveViewCheckpointPartitionMapEntry();
    private final long[] segmentIds = new long[SEGMENT_CACHE_SIZE];
    private final LiveViewCheckpointMetaSegmentReader[] segmentReaders = new LiveViewCheckpointMetaSegmentReader[SEGMENT_CACHE_SIZE];
    private long boundRootOffset = -1;
    private long boundRootSegmentId = -1;
    private long decodedPageCount;
    /**
     * Decoded nodes one bound root memoises, indexed by the depth they sit at. A
     * seal looks the same root up once per partition - both to find the previous
     * boundary's entry and to carry the old root's entry into the new one - and
     * every lookup would otherwise re-walk the same root-to-leaf path, checksumming
     * and decoding a metadata page per level and rebuilding the whole page's entry
     * image, state page references included, to read one entry out of it.
     * <p>
     * A B+ tree holds every leaf at the same depth and a page at exactly one level,
     * so indexing by depth memoises the whole descent and never has one level evict
     * another. The alternative - a fixed-slot cache in clock order - has to be at
     * least as deep as the tree or a descent evicts its own prefix: with four slots
     * the memo served descents up to depth four and then collapsed to nothing when
     * a growing map pushed the root down a level, taking the seal from 0.3s back to
     * 4.5s at around 2.4 million partitions. Depth-indexing removes the sizing
     * question rather than answering it.
     * <p>
     * The memo covers one root at a time, so a page cached under a root cannot
     * outlive it - {@link #find} drops the memo as soon as another root is asked
     * for.
     */
    private final ObjList<LiveViewCheckpointPartitionMapNode> nodeCache = new ObjList<>();
    private final LongList nodeCacheOffset = new LongList();
    private final LongList nodeCacheSegmentId = new LongList();
    private final ObjList<LiveViewCheckpointPartitionMapNode> nodePool = new ObjList<>();
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
        nodeCache.clear();
        nodePool.clear();
        Misc.free(checkpointsDir);
    }

    /**
     * Unmaps every cached metadata segment while keeping the readers themselves,
     * so a reader that outlives one restore holds no mapping into files a later
     * retire, repair or compaction deletes. An owner that outlives its operations
     * calls this when each one ends, so it also trims the decode pools of every node
     * and the width caches of the scratch entry to what an idle reader may keep.
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
        deepNode.trimDecodePools();
        navNode.trimDecodePools();
        for (int i = 0, n = nodeCache.size(); i < n; i++) {
            nodeCache.getQuick(i).trimDecodePools();
        }
        for (int i = 0, n = nodePool.size(); i < n; i++) {
            nodePool.getQuick(i).trimDecodePools();
        }
        scratchEntry.trimWidthCaches();
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
        int depth = 0;
        while (true) {
            final LiveViewCheckpointPartitionMapNode node = decodedNode(segmentId, offset, length, depth++);
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

    boolean adjustStateRefCounts(
            @NotNull LiveViewCheckpointPageRef rootRef,
            @NotNull LiveViewCheckpointMutationArena arena,
            int mutationIndex,
            @NotNull io.questdb.std.LongList counts,
            int delta
    ) {
        if (rootRef.isNull()) {
            return false;
        }
        long segmentId = rootRef.getSegmentId();
        long offset = rootRef.getOffset();
        int length = rootRef.getLength();
        if (boundRootSegmentId != segmentId || boundRootOffset != offset) {
            clearNodeCache();
            boundRootSegmentId = segmentId;
            boundRootOffset = offset;
        }
        int depth = 0;
        while (true) {
            final LiveViewCheckpointPartitionMapNode node = decodedNode(segmentId, offset, length, depth++);
            if (node.isLeaf()) {
                final int index = node.find(arena, mutationIndex);
                if (index < 0) {
                    return false;
                }
                for (int i = 0, n = node.statePageRefs[index].length; i < n; i++) {
                    LiveViewCheckpointMetadata.adjustSegmentUseCount(
                            counts,
                            node.statePageRefs[index][i].getSegmentId(),
                            delta
                    );
                }
                return true;
            }
            final int child = node.childIndex(arena, mutationIndex);
            final LiveViewCheckpointPageRef ref = node.childRefs[child];
            segmentId = ref.getSegmentId();
            offset = ref.getOffset();
            length = ref.getLength();
        }
    }

    /**
     * @return how many pages this reader has decoded since it was constructed. A
     * descent that the memo cannot serve decodes one page per level, so this is what
     * separates a memo holding a whole descent from one that evicts its own prefix -
     * a difference no lookup result shows.
     */
    @TestOnly
    public long getDecodedPageCount() {
        return decodedPageCount;
    }

    /**
     * @return image bytes of the largest single key or scalar pool this reader keeps for
     * reuse, headers excluded: the decode pool of one node, or the key or the scalar width
     * cache of its scratch entry
     */
    @TestOnly
    public long getLargestRetainedBufferBytesForTest() {
        long bytes = Math.max(
                Math.max(deepNode.getRetainedDecodedBytesForTest(), navNode.getRetainedDecodedBytesForTest()),
                scratchEntry.getLargestRetainedBufferBytesForTest()
        );
        for (int i = 0, n = nodeCache.size(); i < n; i++) {
            bytes = Math.max(bytes, nodeCache.getQuick(i).getRetainedDecodedBytesForTest());
        }
        for (int i = 0, n = nodePool.size(); i < n; i++) {
            bytes = Math.max(bytes, nodePool.getQuick(i).getRetainedDecodedBytesForTest());
        }
        return bytes;
    }

    /**
     * @return image bytes of every key and scalar array this reader keeps for reuse,
     * headers excluded: the decode pools of every node it owns and the width caches of
     * its scratch entry
     */
    @TestOnly
    public long getRetainedBufferBytesForTest() {
        long bytes = deepNode.getRetainedDecodedBytesForTest()
                + navNode.getRetainedDecodedBytesForTest()
                + scratchEntry.getRetainedBufferBytesForTest();
        for (int i = 0, n = nodeCache.size(); i < n; i++) {
            bytes += nodeCache.getQuick(i).getRetainedDecodedBytesForTest();
        }
        for (int i = 0, n = nodePool.size(); i < n; i++) {
            bytes += nodePool.getQuick(i).getRetainedDecodedBytesForTest();
        }
        return bytes;
    }

    /**
     * @return state page references of every reference array this reader keeps for
     * reuse: the decode pools of every node it owns and the reference cache of its
     * scratch entry
     */
    @TestOnly
    public long getRetainedStatePageRefCountForTest() {
        long refs = deepNode.getRetainedDecodedStatePageRefCountForTest()
                + navNode.getRetainedDecodedStatePageRefCountForTest()
                + scratchEntry.getRetainedStatePageRefCountForTest();
        for (int i = 0, n = nodeCache.size(); i < n; i++) {
            refs += nodeCache.getQuick(i).getRetainedDecodedStatePageRefCountForTest();
        }
        for (int i = 0, n = nodePool.size(); i < n; i++) {
            refs += nodePool.getQuick(i).getRetainedDecodedStatePageRefCountForTest();
        }
        return refs;
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
        decodedPageCount++;
    }

    void openAndDecode(
            long segmentId,
            long offset,
            int length,
            LiveViewCheckpointPartitionMapNode node,
            LiveViewCheckpointMutationArena arena,
            LiveViewCheckpointPageRefPool pageRefPool
    ) {
        final LiveViewCheckpointMetaSegmentReader reader = readerFor(segmentId);
        reader.openPageAt(offset, length);
        node.decode(reader, arena, pageRefPool);
        decodedPageCount++;
    }

    private void clearNodeCache() {
        nodeCacheSegmentId.setAll(nodeCacheSegmentId.size(), -1);
        nodeCacheOffset.setAll(nodeCacheOffset.size(), -1);
        boundRootSegmentId = -1;
        boundRootOffset = -1;
    }

    /**
     * @return the decoded image of the page at {@code segmentId}/{@code offset},
     * which sits {@code depth} levels below the bound root, out of the memo when a
     * previous descent already reached it. The caller must not hold the node across
     * another lookup, which may recycle its slot.
     */
    private LiveViewCheckpointPartitionMapNode decodedNode(long segmentId, long offset, int length, int depth) {
        if (depth >= MAX_MEMO_DEPTH) {
            openAndDecode(segmentId, offset, length, deepNode);
            return deepNode;
        }
        ensureNodeCacheCapacity(depth + 1);
        final LiveViewCheckpointPartitionMapNode cached = nodeCache.getQuick(depth);
        if (nodeCacheSegmentId.getQuick(depth) == segmentId && nodeCacheOffset.getQuick(depth) == offset) {
            return cached;
        }
        // A rejected page leaves the slot holding a half-decoded node, so drop the
        // slot's identity before the decode rather than let a throw leave a memo
        // entry claiming a page it does not hold.
        nodeCacheSegmentId.setQuick(depth, -1);
        nodeCacheOffset.setQuick(depth, -1);
        openAndDecode(segmentId, offset, length, cached);
        nodeCacheSegmentId.setQuick(depth, segmentId);
        nodeCacheOffset.setQuick(depth, offset);
        return cached;
    }

    private void ensureNodeCacheCapacity(int capacity) {
        while (nodeCache.size() < capacity) {
            nodeCache.add(new LiveViewCheckpointPartitionMapNode());
            nodeCacheOffset.add(-1);
            nodeCacheSegmentId.add(-1);
        }
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
        while (nodePool.size() <= depth) {
            nodePool.add(new LiveViewCheckpointPartitionMapNode());
        }
        return nodePool.getQuick(depth);
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
