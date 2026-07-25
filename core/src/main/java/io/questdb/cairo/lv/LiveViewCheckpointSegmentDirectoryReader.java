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
import io.questdb.std.Numbers;
import io.questdb.std.Transient;
import io.questdb.std.str.Path;
import org.jetbrains.annotations.NotNull;

import java.io.Closeable;
import java.util.Arrays;

/**
 * Read-only navigator over the persistent copy-on-write segment directory B+
 * tree. {@link #of} binds one generation's {@code segmentDirectoryRootRef} and
 * decodes its root node, which is all a bounded superblock validation needs: it
 * proves the root page's checksum, framing and node shape without following a
 * single child reference or opening a data file.
 * <ul>
 *     <li>{@link #find} / {@link #getFileLength} / {@link #getReferenceCount} -
 *     {@code O(log N)} point lookups, the bounds check every state page read
 *     goes through;</li>
 *     <li>{@link #lastSegmentId} - the greatest catalogued id, which a
 *     compaction target must exceed;</li>
 *     <li>{@link #iterateAll} - the ordered scan a purge sweep walks, bounded by
 *     the number of segments rather than by timeline length.</li>
 * </ul>
 * A descent keeps a small cache of metadata segment readers, so a path that
 * stays inside one segment does not remap it. This class is not thread safe;
 * create one per navigating thread.
 */
public class LiveViewCheckpointSegmentDirectoryReader implements Closeable {

    /**
     * Entries the bound root memoises, so a repeat lookup of the same segment id
     * costs a slot probe rather than a root-to-leaf descent that opens, checksums
     * and decodes a metadata page per level. A restore resolves the same handful
     * of ids over and over - once per state page it reads, and again for every
     * partition whose ring spans the same chunks - so the repeat is the common
     * case rather than the exception.
     * <p>
     * Direct-mapped on the segment id, which the seal mints sequentially, so a
     * span that fits collides nowhere; a span that does not degrades to a descent
     * for the ids that collide. Sized to the chunks one partition's ring may span,
     * which is the widest run of segments one restore walks.
     */
    private static final int ENTRY_CACHE_SIZE = Numbers.ceilPow2(LiveViewCheckpointRingSeal.MAX_LIVE_CHUNKS);
    private static final int SEGMENT_CACHE_SIZE = 4;
    private final Aggregate aggregate = new Aggregate();
    private final Path checkpointsDir = new Path();
    private final CairoConfiguration configuration;
    private final long[] entryFileLength = new long[ENTRY_CACHE_SIZE];
    private final long[] entryReferenceCount = new long[ENTRY_CACHE_SIZE];
    private final long[] entryRetireGeneration = new long[ENTRY_CACHE_SIZE];
    private final long[] entrySegmentId = new long[ENTRY_CACHE_SIZE];
    private final LiveViewCheckpointSegmentDirectoryEntry lookupEntry = new LiveViewCheckpointSegmentDirectoryEntry();
    private final LiveViewCheckpointSegmentDirectoryNode navNode = new LiveViewCheckpointSegmentDirectoryNode();
    private final LiveViewCheckpointPageRef rootRef = new LiveViewCheckpointPageRef();
    private final LiveViewCheckpointSegmentDirectoryEntry scratchEntry = new LiveViewCheckpointSegmentDirectoryEntry();
    private final long[] segReaderSegId = new long[SEGMENT_CACHE_SIZE];
    private final LiveViewCheckpointMetaSegmentReader[] segReaders = new LiveViewCheckpointMetaSegmentReader[SEGMENT_CACHE_SIZE];
    private LiveViewCheckpointSegmentDirectoryNode[] nodePool = new LiveViewCheckpointSegmentDirectoryNode[0];
    private int segReaderClock;

    public LiveViewCheckpointSegmentDirectoryReader(@NotNull CairoConfiguration configuration) {
        this.configuration = configuration;
        for (int i = 0; i < SEGMENT_CACHE_SIZE; i++) {
            segReaderSegId[i] = -1;
        }
        clearEntryCache();
    }

    /**
     * Unbinds the current root without releasing the segment readers.
     */
    public void clear() {
        rootRef.clear();
        clearEntryCache();
    }

    @Override
    public void close() {
        for (int i = 0; i < SEGMENT_CACHE_SIZE; i++) {
            segReaders[i] = Misc.free(segReaders[i]);
            segReaderSegId[i] = -1;
        }
        rootRef.clear();
        clearEntryCache();
        Misc.free(checkpointsDir);
    }

    /**
     * Unmaps every cached metadata segment while keeping the readers themselves,
     * so a reader that outlives one restore holds no mapping into files a later
     * retire, repair or compaction deletes.
     */
    public void detach() {
        for (int i = 0; i < SEGMENT_CACHE_SIZE; i++) {
            if (segReaders[i] != null) {
                segReaders[i].close();
            }
            segReaderSegId[i] = -1;
        }
        segReaderClock = 0;
        rootRef.clear();
        clearEntryCache();
    }

    /**
     * Point lookup by {@code segmentId}. Fills {@code out} and returns true when
     * the segment is catalogued. An id the bound root already resolved comes back
     * out of the entry cache instead of walking the tree again: the directory is
     * copy-on-write, so what a bound root holds cannot change until {@link #of}
     * binds another.
     */
    public boolean find(long segmentId, @NotNull LiveViewCheckpointSegmentDirectoryEntry out) {
        if (rootRef.isNull()) {
            return false;
        }
        // An empty slot holds -1, which no catalogued segment can carry, so a
        // negative id asks the tree rather than reading an empty slot as a hit.
        final int slot = (int) (segmentId & (ENTRY_CACHE_SIZE - 1));
        if (segmentId >= 0 && entrySegmentId[slot] == segmentId) {
            out.of(segmentId, entryFileLength[slot], entryReferenceCount[slot], entryRetireGeneration[slot]);
            return true;
        }
        long seg = rootRef.getSegmentId();
        long off = rootRef.getOffset();
        long len = rootRef.getLength();
        while (true) {
            openAndDecode(seg, off, len, navNode);
            if (navNode.isLeaf()) {
                final int index = navNode.findEntry(segmentId);
                if (index < 0) {
                    return false;
                }
                navNode.copyEntryTo(index, out);
                entryFileLength[slot] = out.fileLength;
                entryReferenceCount[slot] = out.referenceCount;
                entryRetireGeneration[slot] = out.retireGeneration;
                entrySegmentId[slot] = segmentId;
                return true;
            }
            if (navNode.count() == 0) {
                throw invalid("segment directory node is empty");
            }
            final int child = navNode.childIndexFor(segmentId);
            seg = navNode.childSegmentId[child];
            off = navNode.childOffset[child];
            len = navNode.childLength[child];
        }
    }

    /**
     * Published byte length of {@code segmentId}'s data file.
     *
     * @throws CairoException when the segment is not catalogued
     */
    public long getFileLength(long segmentId) {
        return required(segmentId).fileLength;
    }

    /**
     * Total byte size of catalogued segments no current root references. They are
     * unlinked once no reader can reach them.
     */
    public long getObsoleteBytes() {
        aggregate.run();
        return aggregate.obsoleteBytes;
    }

    /**
     * Total byte size of catalogued segments at least one current root
     * references.
     */
    public long getReferencedBytes() {
        aggregate.run();
        return aggregate.referencedBytes;
    }

    /**
     * Number of current logical roots that reference {@code segmentId}.
     *
     * @throws CairoException when the segment is not catalogued
     */
    public long getReferenceCount(long segmentId) {
        return required(segmentId).referenceCount;
    }

    /**
     * Generation at which {@code segmentId} retired, or
     * {@link LiveViewCheckpointSegmentDirectory#RETIRE_GENERATION_NONE} while it
     * is still referenced.
     *
     * @throws CairoException when the segment is not catalogued
     */
    public long getRetireGeneration(long segmentId) {
        return required(segmentId).retireGeneration;
    }

    /**
     * Visits every catalogued segment in ascending id order.
     */
    public void iterateAll(@NotNull Visitor visitor) {
        if (rootRef.isNull()) {
            return;
        }
        iterateRec(rootRef.getSegmentId(), rootRef.getOffset(), rootRef.getLength(), visitor, 0);
    }

    /**
     * Greatest catalogued segment id, or {@code -1} for an empty directory.
     * Compaction uses it to keep target ids monotonic.
     */
    public long lastSegmentId() {
        if (rootRef.isNull()) {
            return -1;
        }
        long seg = rootRef.getSegmentId();
        long off = rootRef.getOffset();
        long len = rootRef.getLength();
        while (true) {
            openAndDecode(seg, off, len, navNode);
            final int count = navNode.count();
            if (navNode.isLeaf()) {
                return count == 0 ? -1 : navNode.entrySegmentId[count - 1];
            }
            if (count == 0) {
                throw invalid("segment directory node is empty");
            }
            final int child = count - 1;
            seg = navNode.childSegmentId[child];
            off = navNode.childOffset[child];
            len = navNode.childLength[child];
        }
    }

    /**
     * Binds one generation's directory root and decodes it. A null root is a
     * valid empty catalogue.
     */
    public void of(@Transient @NotNull Path checkpointsDir, @NotNull LiveViewCheckpointPageRef rootRef) {
        this.checkpointsDir.of(checkpointsDir);
        for (int i = 0; i < SEGMENT_CACHE_SIZE; i++) {
            segReaderSegId[i] = -1;
        }
        segReaderClock = 0;
        this.rootRef.clear();
        // The entries the previous root published say nothing about this one.
        clearEntryCache();
        if (rootRef.isNull()) {
            return;
        }
        openAndDecode(rootRef.getSegmentId(), rootRef.getOffset(), rootRef.getLength(), navNode);
        this.rootRef.of(rootRef.getSegmentId(), rootRef.getOffset(), rootRef.getLength());
    }

    /**
     * Total number of catalogued segments. This walks every leaf, so it is
     * bounded by the segment count; it is not on the publication path.
     */
    public long size() {
        aggregate.run();
        return aggregate.count;
    }

    /**
     * Decodes the node at {@code (segmentId, offset, length)} into {@code node}
     * through the shared segment-reader cache. Package-private so the writer can
     * read old pages while it copies a search path.
     */
    void openAndDecode(long segmentId, long offset, long length, @NotNull LiveViewCheckpointSegmentDirectoryNode node) {
        final LiveViewCheckpointMetaSegmentReader reader = readerFor(segmentId);
        reader.openPageAt(offset, (int) length);
        node.decode(reader);
    }

    private static long checkedAdd(long a, long b, CharSequence what) {
        if (b > Long.MAX_VALUE - a) {
            throw CairoException.critical(0)
                    .put("live view checkpoint segment directory ")
                    .put(what).put(" overflow");
        }
        return a + b;
    }

    private static CairoException invalid(CharSequence reason) {
        return CairoException.critical(CairoException.LV_CHECKPOINT_TIMELINE_INVALID)
                .put("live view checkpoint ").put(reason);
    }

    private void clearEntryCache() {
        Arrays.fill(entrySegmentId, -1);
    }

    private void iterateRec(long seg, long off, long len, Visitor visitor, int depth) {
        final LiveViewCheckpointSegmentDirectoryNode node = nodeAt(depth);
        openAndDecode(seg, off, len, node);
        final int count = node.count();
        if (node.isLeaf()) {
            for (int i = 0; i < count; i++) {
                node.copyEntryTo(i, scratchEntry);
                visitor.onEntry(scratchEntry);
            }
        } else {
            for (int i = 0; i < count; i++) {
                iterateRec(node.childSegmentId[i], node.childOffset[i], node.childLength[i], visitor, depth + 1);
            }
        }
    }

    private LiveViewCheckpointSegmentDirectoryNode nodeAt(int depth) {
        if (depth >= nodePool.length) {
            final LiveViewCheckpointSegmentDirectoryNode[] grown = new LiveViewCheckpointSegmentDirectoryNode[depth + 1];
            System.arraycopy(nodePool, 0, grown, 0, nodePool.length);
            nodePool = grown;
        }
        LiveViewCheckpointSegmentDirectoryNode node = nodePool[depth];
        if (node == null) {
            node = new LiveViewCheckpointSegmentDirectoryNode();
            nodePool[depth] = node;
        }
        return node;
    }

    private LiveViewCheckpointMetaSegmentReader readerFor(long segmentId) {
        for (int i = 0; i < SEGMENT_CACHE_SIZE; i++) {
            if (segReaderSegId[i] == segmentId && segReaders[i] != null) {
                return segReaders[i];
            }
        }
        final int slot = segReaderClock;
        segReaderClock = segReaderClock + 1 == SEGMENT_CACHE_SIZE ? 0 : segReaderClock + 1;
        if (segReaders[slot] == null) {
            segReaders[slot] = new LiveViewCheckpointMetaSegmentReader(configuration);
        }
        segReaders[slot].of(checkpointsDir, segmentId);
        segReaderSegId[slot] = segmentId;
        return segReaders[slot];
    }

    private LiveViewCheckpointSegmentDirectoryEntry required(long segmentId) {
        if (!find(segmentId, lookupEntry)) {
            throw CairoException.critical(0)
                    .put("unknown live view checkpoint data segment, segmentId=")
                    .put(segmentId);
        }
        return lookupEntry;
    }

    /**
     * Callback for {@link #iterateAll}. The {@code entry} is a reused flyweight
     * valid only for the duration of the call; copy it to retain.
     */
    @FunctionalInterface
    public interface Visitor {
        void onEntry(LiveViewCheckpointSegmentDirectoryEntry entry);
    }

    /**
     * Reusable full-scan accumulator behind the catalogue-wide totals. They walk
     * every leaf, so they cost the segment count and stay off the publication
     * path.
     */
    private final class Aggregate implements Visitor {
        private long count;
        private long obsoleteBytes;
        private long referencedBytes;

        @Override
        public void onEntry(LiveViewCheckpointSegmentDirectoryEntry entry) {
            count++;
            if (entry.referenceCount > 0) {
                referencedBytes = checkedAdd(referencedBytes, entry.fileLength, "referenced byte count");
            } else {
                obsoleteBytes = checkedAdd(obsoleteBytes, entry.fileLength, "obsolete byte count");
            }
        }

        private void run() {
            count = 0;
            obsoleteBytes = 0;
            referencedBytes = 0;
            iterateAll(this);
        }
    }
}
