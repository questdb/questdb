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
import io.questdb.std.MemoryTracker;
import io.questdb.std.Misc;
import io.questdb.std.Transient;
import io.questdb.std.str.Path;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.Closeable;

/**
 * Builds one immutable root page over a partition map - a function root or a window
 * root - together with the map's changed paths, in the same metadata segment.
 * <p>
 * A build moves the root's segment use counts by its own delta rather than by a
 * reachability walk: the map pages the path copy released, the root page it supersedes
 * and the pages it writes. A subclass stages its mutations, may move the counts further
 * from what it staged, and writes the root page; everything else is here.
 */
abstract class AbstractLiveViewCheckpointStateRootBuilder implements Closeable {
    protected static final long NO_SEGMENT = -1;
    protected final Path checkpointsDir = new Path();
    protected final LiveViewCheckpointMutationArena mutations = new LiveViewCheckpointMutationArena();
    protected final LiveViewCheckpointPageRef oldPartitionMapRoot = new LiveViewCheckpointPageRef();
    protected final LiveViewCheckpointPartitionMapReader partitionMapReader;
    protected final LiveViewCheckpointPartitionMapWriter partitionMapWriter;
    protected final LongList segmentUseCounts = new LongList();
    private final LiveViewCheckpointPageRef builtPartitionMapRoot = new LiveViewCheckpointPageRef();
    private final String rootName;
    private final LiveViewCheckpointMetaSegmentWriter segmentWriter;
    protected boolean isInitialized;
    /**
     * Metadata segment holding the root page this build supersedes, or
     * {@link #NO_SEGMENT} when there is none to supersede.
     */
    protected long oldRootPageSegmentId = NO_SEGMENT;
    private long lastSegmentBytes;

    /**
     * @param rootName names the root in the not-initialized error
     */
    AbstractLiveViewCheckpointStateRootBuilder(
            @NotNull CairoConfiguration configuration,
            @NotNull LiveViewCheckpointPartitionMapObjectPool objectPool,
            @NotNull String rootName
    ) {
        this.rootName = rootName;
        partitionMapReader = new LiveViewCheckpointPartitionMapReader(configuration);
        partitionMapWriter = new LiveViewCheckpointPartitionMapWriter(configuration, objectPool);
        segmentWriter = new LiveViewCheckpointMetaSegmentWriter(configuration);
    }

    /**
     * Binds {@code memoryTracker} to the staging arena for the next build, after
     * freeing what the previous binding charged. The builder is shared across the
     * views one refresh worker seals, so retained native capacity must never
     * migrate from one view's tracker to another's.
     */
    public void bindMemoryTracker(@Nullable MemoryTracker memoryTracker) {
        mutations.bind(memoryTracker);
    }

    public void build(long metadataSegmentId, @NotNull LiveViewCheckpointPageRef out) {
        try {
            segmentWriter.of(checkpointsDir, metadataSegmentId);
            buildIntoOpenSegment(metadataSegmentId, segmentWriter, out);
            lastSegmentBytes = segmentWriter.commit();
        } finally {
            clearBorrowedCompiled();
        }
    }

    /**
     * Writes this root into an aggregate metadata segment owned by the caller.
     */
    public void buildIntoOpenSegment(
            long metadataSegmentId,
            @NotNull LiveViewCheckpointMetaSegmentWriter writer,
            @NotNull LiveViewCheckpointPageRef out
    ) {
        try {
            buildIntoOpenSegment0(metadataSegmentId, writer, out);
        } finally {
            clearBorrowedCompiled();
        }
    }

    @Override
    public void close() {
        Misc.free(mutations);
        Misc.free(partitionMapReader);
        Misc.free(partitionMapWriter);
        Misc.free(segmentWriter);
        Misc.free(checkpointsDir);
    }

    /**
     * Releases every mapping this build read and discards any in-flight segment,
     * keeping the reader, writer and staging shells for the next build. The
     * staging arena keeps its capacity, which belongs to the tracker bound by
     * {@link #bindMemoryTracker}; {@link #releaseMemoryTracker} frees it.
     */
    public void detach() {
        partitionMapReader.detach();
        partitionMapWriter.detach();
        segmentWriter.discard();
        mutations.clear();
        segmentUseCounts.clear();
    }

    public long getLastSegmentBytes() {
        return lastSegmentBytes;
    }

    /**
     * Frees the staging arena against the tracker that acquired it and detaches
     * that tracker, leaving the builder reusable by the next view.
     */
    public void releaseMemoryTracker() {
        mutations.release();
    }

    /**
     * Stages the removal of one partition. The {@code keyLength} key bytes at
     * {@code keyAddress} are copied before this returns, and a key the predecessor map
     * does not hold is a no-op.
     * <p>
     * A window root's forward freeze needs this because its puts are not the whole truth:
     * the frontier sweep takes keys out of the window's own map without the seal walking
     * what remains, so the removals arrive named rather than by omission.
     */
    public void removePartition(long keyAddress, int keyLength) {
        ensureInitialized();
        mutations.remove(keyAddress, keyLength);
    }

    /**
     * Drops the compiled arrays the next build would have written and marks the
     * builder uninitialized. Every build ends here, successful or not.
     */
    protected abstract void clearBorrowedCompiled();

    protected void ensureInitialized() {
        if (!isInitialized) {
            throw CairoException.critical(0)
                    .put("live view checkpoint ").put(rootName).put(" builder is not initialized");
        }
    }

    /**
     * Points the reader and writer at {@code checkpointsDir} and empties the staging
     * state, ahead of a subclass reading its predecessor root.
     */
    protected void ofCheckpointsDir(@Transient @NotNull Path checkpointsDir) {
        this.checkpointsDir.of(checkpointsDir);
        partitionMapReader.of(checkpointsDir);
        partitionMapWriter.of(checkpointsDir);
        mutations.clear();
        segmentUseCounts.clear();
    }

    /**
     * Prepares the staged mutations for the path copy, moving {@link #segmentUseCounts}
     * by whatever the mutations themselves reference.
     */
    protected abstract void stageMutations();

    /**
     * Writes the root page that names {@code partitionMapRoot} and the final
     * {@link #segmentUseCounts} into {@code writer}.
     */
    protected abstract void writeRoot(
            @NotNull LiveViewCheckpointMetaSegmentWriter writer,
            @NotNull LiveViewCheckpointPageRef partitionMapRoot,
            @NotNull LiveViewCheckpointPageRef out
    );

    private void buildIntoOpenSegment0(
            long metadataSegmentId,
            @NotNull LiveViewCheckpointMetaSegmentWriter writer,
            @NotNull LiveViewCheckpointPageRef out
    ) {
        ensureInitialized();
        if (writer.getSegmentId() != metadataSegmentId) {
            throw CairoException.critical(0).put("live view checkpoint aggregate segment id mismatch");
        }
        stageMutations();
        final LiveViewCheckpointPageRef partitionMapRoot = builtPartitionMapRoot;
        partitionMapWriter.applyToOpenSegment(
                oldPartitionMapRoot,
                mutations,
                writer,
                partitionMapRoot
        );
        // The metadata closure moves by exactly what the path copy took away and
        // what this segment gains: every superseded map page, the root page this
        // build replaces, and the pages written here - the map's copied path plus
        // the root page about to follow it.
        final LongList releasedSegmentIds = partitionMapWriter.getLastReleasedSegmentIds();
        for (int i = 0, n = releasedSegmentIds.size(); i < n; i++) {
            LiveViewCheckpointMetadata.adjustSegmentUseCount(segmentUseCounts, releasedSegmentIds.getQuick(i), -1);
        }
        if (oldRootPageSegmentId != NO_SEGMENT) {
            LiveViewCheckpointMetadata.adjustSegmentUseCount(segmentUseCounts, oldRootPageSegmentId, -1);
        }
        LiveViewCheckpointMetadata.adjustSegmentUseCount(
                segmentUseCounts,
                metadataSegmentId,
                partitionMapWriter.getLastSegmentPageCount() + 1
        );
        writeRoot(writer, partitionMapRoot, out);
        oldPartitionMapRoot.of(partitionMapRoot.getSegmentId(), partitionMapRoot.getOffset(), partitionMapRoot.getLength());
        oldRootPageSegmentId = metadataSegmentId;
    }
}
