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
 * Logarithmic reader for a generation-pinned persistent partition map.
 */
public class LiveViewCheckpointPartitionMapReader implements Closeable {

    private static final int SEGMENT_CACHE_SIZE = 8;
    private final Path checkpointsDir = new Path();
    private final CairoConfiguration configuration;
    private final LiveViewCheckpointPartitionMapNode navNode = new LiveViewCheckpointPartitionMapNode();
    private final LiveViewCheckpointPartitionMapEntry scratchEntry = new LiveViewCheckpointPartitionMapEntry();
    private final long[] segmentIds = new long[SEGMENT_CACHE_SIZE];
    private final LiveViewCheckpointMetaSegmentReader[] segmentReaders = new LiveViewCheckpointMetaSegmentReader[SEGMENT_CACHE_SIZE];
    private LiveViewCheckpointPartitionMapNode[] nodePool = new LiveViewCheckpointPartitionMapNode[0];
    private int segmentClock;

    public LiveViewCheckpointPartitionMapReader(@NotNull CairoConfiguration configuration) {
        this.configuration = configuration;
        for (int i = 0; i < SEGMENT_CACHE_SIZE; i++) {
            segmentIds[i] = -1;
        }
    }

    @Override
    public void close() {
        for (int i = 0; i < SEGMENT_CACHE_SIZE; i++) {
            segmentReaders[i] = Misc.free(segmentReaders[i]);
            segmentIds[i] = -1;
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
            if (segmentReaders[i] != null) {
                segmentReaders[i].close();
            }
            segmentIds[i] = -1;
        }
        segmentClock = 0;
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
        while (true) {
            openAndDecode(segmentId, offset, length, navNode);
            if (navNode.isLeaf()) {
                final int index = navNode.find(key);
                if (index < 0) {
                    return false;
                }
                navNode.copyEntryTo(index, out);
                return true;
            }
            final int child = navNode.childIndex(key);
            final LiveViewCheckpointPageRef ref = navNode.childRefs[child];
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
