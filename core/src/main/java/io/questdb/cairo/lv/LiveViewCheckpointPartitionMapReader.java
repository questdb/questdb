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
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.Transient;
import io.questdb.std.str.Path;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.TestOnly;

import java.io.Closeable;

/**
 * Logarithmic reader for a generation-pinned persistent partition map.
 * <p>
 * Every node the reader decodes into - the memo slots, the walk nodes and the two
 * scratch nodes - has a native {@link LiveViewCheckpointMutationArena} of its own that
 * holds the page's keys, scalars and state page references, so a decode allocates no
 * heap. A lookup or a walk hands an entry out by copying it into the caller's flyweight,
 * never by lending the node's arena, so re-decoding a memo slot cannot change an entry
 * a caller already holds. The arenas are untracked: the reader outlives the views it
 * serves.
 */
public class LiveViewCheckpointPartitionMapReader implements Closeable {

    /**
     * Native bytes the arena of one node may keep once {@link #detach()} ends an
     * operation; an arena holding more is freed and the next decode into its node
     * allocates again.
     * <p>
     * An arena holds one decoded page at a time and grows to the largest page its node
     * has decoded, so what it keeps is bounded by the widest page rather than by the
     * union of widths a view's keys span. The readers of a refresh worker live as long as
     * the worker, so this bounds what an idle node keeps from the views the worker served
     * before. A leaf of 64 keys of 512 characters needs about 66 KiB.
     */
    public static final long MAX_NODE_RETAINED_BYTES = 16_777_216;
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
    // Arenas are lazy, so none of these allocates native memory before its first decode.
    // A decoded node is live-view in-memory state, so its arena is accounted as such.
    private final LiveViewCheckpointMutationArena deepArena = newNodeArena();
    private final LiveViewCheckpointPartitionMapNode deepNode = new LiveViewCheckpointPartitionMapNode();
    private final LiveViewCheckpointMutationArena navArena = newNodeArena();
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
    // Index-aligned with nodeCache: the arena each memo slot decodes into.
    private final ObjList<LiveViewCheckpointMutationArena> nodeCacheArenas = new ObjList<>();
    private final LongList nodeCacheOffset = new LongList();
    private final LongList nodeCacheSegmentId = new LongList();
    private final ObjList<LiveViewCheckpointPartitionMapNode> nodePool = new ObjList<>();
    // Index-aligned with nodePool: the arena each walk node decodes into.
    private final ObjList<LiveViewCheckpointMutationArena> nodePoolArenas = new ObjList<>();
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
        Misc.freeObjListAndClear(nodeCacheArenas);
        Misc.freeObjListAndClear(nodePoolArenas);
        Misc.free(deepArena);
        Misc.free(navArena);
        Misc.free(scratchEntry);
        Misc.free(checkpointsDir);
    }

    /**
     * Unmaps every cached metadata segment while keeping the readers themselves,
     * so a reader that outlives one restore holds no mapping into files a later
     * retire, repair or compaction deletes. An owner that outlives its operations
     * calls this when each one ends, so it also frees every node arena grown past
     * {@link #MAX_NODE_RETAINED_BYTES} and trims the scratch entry to what an idle reader
     * may keep.
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
        trimArena(deepNode, deepArena);
        trimArena(navNode, navArena);
        for (int i = 0, n = nodeCache.size(); i < n; i++) {
            trimArena(nodeCache.getQuick(i), nodeCacheArenas.getQuick(i));
        }
        for (int i = 0, n = nodePool.size(); i < n; i++) {
            trimArena(nodePool.getQuick(i), nodePoolArenas.getQuick(i));
        }
        scratchEntry.trimWidthCaches();
    }

    /**
     * Looks the {@code keyLength} key bytes at {@code keyAddress} up under
     * {@code rootRef} and copies the entry into {@code out}. The key must not lie in
     * {@code out}'s own key buffer.
     */
    public boolean find(
            @NotNull LiveViewCheckpointPageRef rootRef,
            long keyAddress,
            int keyLength,
            @NotNull LiveViewCheckpointPartitionMapEntry out
    ) {
        if (rootRef.isNull()) {
            return false;
        }
        long segmentId = rootRef.getSegmentId();
        long offset = rootRef.getOffset();
        int length = rootRef.getLength();
        bindRoot(segmentId, offset);
        int depth = 0;
        while (true) {
            final LiveViewCheckpointPartitionMapNode node = decodedNode(segmentId, offset, length, depth++);
            if (node.isLeaf()) {
                final int index = node.find(keyAddress, keyLength);
                if (index < 0) {
                    return false;
                }
                node.copyEntryTo(index, out);
                return true;
            }
            final int child = node.childIndex(keyAddress, keyLength);
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
        bindRoot(segmentId, offset);
        int depth = 0;
        while (true) {
            final LiveViewCheckpointPartitionMapNode node = decodedNode(segmentId, offset, length, depth++);
            if (node.isLeaf()) {
                final int index = node.find(arena, mutationIndex);
                if (index < 0) {
                    return false;
                }
                node.adjustRefCountsAt(index, counts, delta);
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
     * @return native bytes of the largest single buffer this reader keeps for reuse: the
     * arena of one node, the key buffer of its scratch entry, or the image bytes of that
     * entry's scalar width cache
     */
    @TestOnly
    public long getLargestRetainedBufferBytesForTest() {
        long bytes = Math.max(
                Math.max(deepArena.getAllocatedBytes(), navArena.getAllocatedBytes()),
                scratchEntry.getLargestRetainedBufferBytesForTest()
        );
        for (int i = 0, n = nodeCacheArenas.size(); i < n; i++) {
            bytes = Math.max(bytes, nodeCacheArenas.getQuick(i).getAllocatedBytes());
        }
        for (int i = 0, n = nodePoolArenas.size(); i < n; i++) {
            bytes = Math.max(bytes, nodePoolArenas.getQuick(i).getAllocatedBytes());
        }
        return bytes;
    }

    /**
     * @return bytes of every buffer this reader keeps for reuse: the native arenas of
     * every node it owns, the key buffer of its scratch entry and the image bytes of that
     * entry's scalar width cache
     */
    @TestOnly
    public long getRetainedBufferBytesForTest() {
        long bytes = deepArena.getAllocatedBytes()
                + navArena.getAllocatedBytes()
                + scratchEntry.getRetainedBufferBytesForTest();
        for (int i = 0, n = nodeCacheArenas.size(); i < n; i++) {
            bytes += nodeCacheArenas.getQuick(i).getAllocatedBytes();
        }
        for (int i = 0, n = nodePoolArenas.size(); i < n; i++) {
            bytes += nodePoolArenas.getQuick(i).getAllocatedBytes();
        }
        return bytes;
    }

    /**
     * @return state page reference slots the reference cache of the scratch entry keeps;
     * the nodes keep their references in their native arenas
     */
    @TestOnly
    public long getRetainedStatePageRefCountForTest() {
        return scratchEntry.getRetainedStatePageRefCountForTest();
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
        openAndDecodeOwned(rootRef.getSegmentId(), rootRef.getOffset(), rootRef.getLength(), navNode, navArena);
        return navNode.isLeaf() ? 0 : navNode.count();
    }

    public void rootChildRef(@NotNull LiveViewCheckpointPageRef rootRef, int index, @NotNull LiveViewCheckpointPageRef out) {
        openAndDecodeOwned(rootRef.getSegmentId(), rootRef.getOffset(), rootRef.getLength(), navNode, navArena);
        if (navNode.isLeaf() || index < 0 || index >= navNode.count()) {
            throw LiveViewCheckpointMetadata.invalid("partition map root child index out of bounds, index=").put(index);
        }
        final LiveViewCheckpointPageRef ref = navNode.childRefs[index];
        out.of(ref.getSegmentId(), ref.getOffset(), ref.getLength());
    }

    public long size(@NotNull LiveViewCheckpointPageRef rootRef) {
        return rootRef.isNull() ? 0 : size(rootRef, 0);
    }

    /**
     * Decodes a page into a partition-map build's node, appending the page's entries to
     * the build's own arena. It never clears that arena: the build's staged mutations
     * live there too.
     */
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

    private static LiveViewCheckpointMutationArena newNodeArena() {
        return new LiveViewCheckpointMutationArena(null, MemoryTag.NATIVE_LIVE_VIEW_IN_MEM);
    }

    /**
     * Frees {@code arena} when it holds more than {@link #MAX_NODE_RETAINED_BYTES}, and
     * empties its node with it, so nothing can read the freed page before the next
     * decode refills both.
     */
    private static void trimArena(LiveViewCheckpointPartitionMapNode node, LiveViewCheckpointMutationArena arena) {
        if (arena.getAllocatedBytes() > MAX_NODE_RETAINED_BYTES) {
            arena.release();
            node.resetLeaf();
        }
    }

    private void bindRoot(long segmentId, long offset) {
        if (boundRootSegmentId != segmentId || boundRootOffset != offset) {
            // A page is immutable and the map is copy-on-write, so what one root
            // reaches cannot change under the memo. What a memo may not survive is
            // a rebuilt timeline re-minting the ids it keyed on, so the memo starts
            // over whenever another root is asked for.
            clearNodeCache();
            boundRootSegmentId = segmentId;
            boundRootOffset = offset;
        }
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
            openAndDecodeOwned(segmentId, offset, length, deepNode, deepArena);
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
        openAndDecodeOwned(segmentId, offset, length, cached, nodeCacheArenas.getQuick(depth));
        nodeCacheSegmentId.setQuick(depth, segmentId);
        nodeCacheOffset.setQuick(depth, offset);
        return cached;
    }

    private void ensureNodeCacheCapacity(int capacity) {
        while (nodeCache.size() < capacity) {
            nodeCache.add(new LiveViewCheckpointPartitionMapNode());
            nodeCacheArenas.add(newNodeArena());
            nodeCacheOffset.add(-1);
            nodeCacheSegmentId.add(-1);
        }
    }

    private void iterate(LiveViewCheckpointPageRef ref, Visitor visitor, int depth) {
        final LiveViewCheckpointPartitionMapNode node = nodeAt(depth);
        openAndDecodeOwned(ref.getSegmentId(), ref.getOffset(), ref.getLength(), node, nodePoolArenas.getQuick(depth));
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
            nodePoolArenas.add(newNodeArena());
        }
        return nodePool.getQuick(depth);
    }

    /**
     * Decodes a page into one of this reader's own nodes, through the arena that node
     * owns. The decode clears that arena first, which is why it has a name of its own: a
     * build's arena passed here would lose every mutation it had staged.
     */
    private void openAndDecodeOwned(
            long segmentId,
            long offset,
            int length,
            LiveViewCheckpointPartitionMapNode node,
            LiveViewCheckpointMutationArena nodeArena
    ) {
        final LiveViewCheckpointMetaSegmentReader reader = readerFor(segmentId);
        reader.openPageAt(offset, length);
        node.decodeOwned(reader, nodeArena);
        decodedPageCount++;
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
        openAndDecodeOwned(ref.getSegmentId(), ref.getOffset(), ref.getLength(), node, nodePoolArenas.getQuick(depth));
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
