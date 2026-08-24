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
import org.jetbrains.annotations.TestOnly;

import java.io.Closeable;
import java.util.Arrays;

/**
 * Builds one immutable anchor root and its changed anchor-map paths in the same
 * metadata segment.
 * <p>
 * A complete freeze treats its puts as the whole truth and removes every old entry
 * it did not put. A forward cadence freeze may instead supply only touched keys;
 * in that mode untouched entries remain in the predecessor map. The copy-on-write
 * writer drops equal puts in either mode, keeping a seal proportional to changed
 * partitions.
 * <p>
 * Because unchanged leaves stay where they were, the root that comes out of a
 * seal names pages several older metadata segments hold. {@link #build} keeps the
 * per-segment page counts that state exactly which, from the delta of one build:
 * the pages the path copy superseded, the root page it replaces, and the pages it
 * wrote.
 */
public class LiveViewCheckpointAnchorRootBuilder implements Closeable {

    private static final long NO_SEGMENT = -1;
    private final Path checkpointsDir = new Path();
    private final LiveViewCheckpointMutationArena mutations;
    private final MissingPartitionVisitor missingPartitionVisitor = new MissingPartitionVisitor();
    private final LiveViewCheckpointAnchorRoot oldAnchorRoot;
    private final LiveViewCheckpointPageRef oldPartitionMapRoot = new LiveViewCheckpointPageRef();
    private final LiveViewCheckpointPartitionMapReader partitionMapReader;
    private final LiveViewCheckpointPartitionMapWriter partitionMapWriter;
    private final LiveViewCheckpointAnchorRoot resultAnchorRoot;
    private final LongList segmentUseCounts = new LongList();
    private final LiveViewCheckpointMetaSegmentWriter segmentWriter;
    private int anchorValueType;
    private boolean isCompleteSnapshot;
    private boolean isInitialized;
    private byte[] keySchema = new byte[0];
    private long lastSegmentBytes;
    /**
     * Metadata segment holding the anchor-root page this build supersedes, or
     * {@link #NO_SEGMENT} for the first anchor root of a timeline.
     */
    private long oldRootPageSegmentId = NO_SEGMENT;
    private byte[] windowName = new byte[0];

    public LiveViewCheckpointAnchorRootBuilder(@NotNull CairoConfiguration configuration) {
        this(configuration, null);
    }

    public LiveViewCheckpointAnchorRootBuilder(
            @NotNull CairoConfiguration configuration,
            MemoryTracker memoryTracker
    ) {
        this(configuration, memoryTracker, new LiveViewCheckpointPartitionMapObjectPool());
    }

    LiveViewCheckpointAnchorRootBuilder(
            @NotNull CairoConfiguration configuration,
            MemoryTracker memoryTracker,
            @NotNull LiveViewCheckpointPartitionMapObjectPool objectPool
    ) {
        mutations = new LiveViewCheckpointMutationArena(memoryTracker);
        oldAnchorRoot = new LiveViewCheckpointAnchorRoot(configuration);
        partitionMapReader = new LiveViewCheckpointPartitionMapReader(configuration);
        partitionMapWriter = new LiveViewCheckpointPartitionMapWriter(configuration, objectPool);
        resultAnchorRoot = new LiveViewCheckpointAnchorRoot(configuration);
        segmentWriter = new LiveViewCheckpointMetaSegmentWriter(configuration);
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
     * The caller commits the segment after all roots in the pass have been
     * appended.
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

    private void buildIntoOpenSegment0(long metadataSegmentId, @NotNull LiveViewCheckpointMetaSegmentWriter writer, @NotNull LiveViewCheckpointPageRef out) {
        ensureInitialized();
        if (writer.getSegmentId() != metadataSegmentId) {
            throw CairoException.critical(0).put("live view checkpoint aggregate segment id mismatch");
        }
        if (isCompleteSnapshot) {
            mutations.sortAndValidate();
            partitionMapReader.iterateAll(oldPartitionMapRoot, missingPartitionVisitor);
        }

        final LiveViewCheckpointPageRef partitionMapRoot = new LiveViewCheckpointPageRef();
        partitionMapWriter.applyToOpenSegment(
                oldPartitionMapRoot,
                mutations,
                writer,
                partitionMapRoot
        );
        final LongList releasedSegmentIds = partitionMapWriter.getLastReleasedSegmentIds();
        for (int i = 0, n = releasedSegmentIds.size(); i < n; i++) {
            LiveViewCheckpointMetadata.adjustSegmentUseCount(segmentUseCounts, releasedSegmentIds.getQuick(i), -1);
        }
        if (oldRootPageSegmentId != NO_SEGMENT) {
            LiveViewCheckpointMetadata.adjustSegmentUseCount(segmentUseCounts, oldRootPageSegmentId, -1);
        }
        LiveViewCheckpointMetadata.adjustSegmentUseCount(segmentUseCounts, metadataSegmentId, partitionMapWriter.getLastSegmentPageCount() + 1);
        resultAnchorRoot.ofBuilder(windowName, anchorValueType, keySchema, partitionMapRoot, segmentUseCounts);
        resultAnchorRoot.writeTo(writer, out);
        oldPartitionMapRoot.of(
                partitionMapRoot.getSegmentId(),
                partitionMapRoot.getOffset(),
                partitionMapRoot.getLength()
        );
        oldRootPageSegmentId = metadataSegmentId;
    }

    @Override
    public void close() {
        Misc.free(oldAnchorRoot);
        Misc.free(mutations);
        Misc.free(partitionMapReader);
        Misc.free(partitionMapWriter);
        Misc.free(resultAnchorRoot);
        Misc.free(segmentWriter);
        Misc.free(checkpointsDir);
    }

    public long getLastSegmentBytes() {
        return lastSegmentBytes;
    }

    @TestOnly
    boolean isBorrowingCompiledForTest(byte[] windowName, byte[] keySchema) {
        return this.windowName == windowName && this.keySchema == keySchema;
    }

    public void of(
            @Transient @NotNull Path checkpointsDir,
            @NotNull LiveViewCheckpointPageRef oldAnchorRootRef,
            @NotNull byte[] windowName,
            int anchorValueType,
            @NotNull byte[] keySchema
    ) {
        of(checkpointsDir, oldAnchorRootRef, windowName, anchorValueType, keySchema, true);
    }

    public void of(
            @Transient @NotNull Path checkpointsDir,
            @NotNull LiveViewCheckpointPageRef oldAnchorRootRef,
            @NotNull byte[] windowName,
            int anchorValueType,
            @NotNull byte[] keySchema,
            boolean isCompleteSnapshot
    ) {
        of0(checkpointsDir, oldAnchorRootRef, windowName, anchorValueType, keySchema, isCompleteSnapshot, false);
    }

    /**
     * Borrows compiler-owned arrays through the synchronous {@link #build} call.
     * The caller must keep them immutable and alive until build returns.
     */
    void ofBorrowedCompiled(
            @Transient @NotNull Path checkpointsDir,
            @NotNull LiveViewCheckpointPageRef oldAnchorRootRef,
            @NotNull byte[] windowName,
            int anchorValueType,
            @NotNull byte[] keySchema,
            boolean isCompleteSnapshot
    ) {
        of0(checkpointsDir, oldAnchorRootRef, windowName, anchorValueType, keySchema, isCompleteSnapshot, true);
    }

    private void of0(
            @Transient @NotNull Path checkpointsDir,
            @NotNull LiveViewCheckpointPageRef oldAnchorRootRef,
            @NotNull byte[] windowName,
            int anchorValueType,
            @NotNull byte[] keySchema,
            boolean isCompleteSnapshot,
            boolean isBorrowed
    ) {
        isInitialized = false;
        clearBorrowedCompiled();
        if (windowName.length == 0 || keySchema.length < Integer.BYTES) {
            throw CairoException.critical(0).put("live view checkpoint anchor window name or key schema invalid");
        }
        LiveViewCheckpointMetadata.validateByteArrayLength(windowName.length, "anchor window name");
        LiveViewCheckpointMetadata.validateByteArrayLength(keySchema.length, "anchor key schema");
        this.checkpointsDir.of(checkpointsDir);
        partitionMapReader.of(checkpointsDir);
        partitionMapWriter.of(checkpointsDir);
        this.windowName = isBorrowed ? windowName : windowName.clone();
        this.anchorValueType = anchorValueType;
        this.keySchema = isBorrowed ? keySchema : keySchema.clone();
        this.isCompleteSnapshot = isCompleteSnapshot;
        mutations.clear();
        segmentUseCounts.clear();
        boolean isInitializationComplete = false;
        try {
            // A predecessor that is not an anchor root at all is the fused window root, and
            // this build is the conversion away from it: nothing of it can be shared, so the
            // tree starts empty and every live key is imaged, exactly as the first anchor
            // root of a timeline does. Its pages are not this root's to release either -
            // they retire with the boundary that still names them.
            final boolean hasAnchorPredecessor = !oldAnchorRootRef.isNull()
                    && oldAnchorRoot.ofIfAnchorRoot(checkpointsDir, oldAnchorRootRef);
            oldRootPageSegmentId = hasAnchorPredecessor ? oldAnchorRootRef.getSegmentId() : NO_SEGMENT;
            if (!hasAnchorPredecessor) {
                oldPartitionMapRoot.clear();
            } else {
                if (!Arrays.equals(windowName, oldAnchorRoot.getWindowName())
                        || !Arrays.equals(keySchema, oldAnchorRoot.getKeySchema())
                        || anchorValueType != oldAnchorRoot.getAnchorValueType()) {
                    throw CairoException.critical(0).put("live view checkpoint anchor root identity or schema mismatch");
                }
                oldAnchorRoot.getPartitionMapRootRef(oldPartitionMapRoot);
                for (int i = 0, n = oldAnchorRoot.getSegmentUseCountSize(); i < n; i++) {
                    segmentUseCounts.add(oldAnchorRoot.getSegmentId(i), oldAnchorRoot.getSegmentUseCount(i));
                }
            }
            isInitialized = true;
            isInitializationComplete = true;
        } finally {
            if (!isInitializationComplete) {
                clearBorrowedCompiled();
            }
        }
    }

    private void clearBorrowedCompiled() {
        resultAnchorRoot.clearBorrowedCompiled();
        isInitialized = false;
        windowName = null;
        keySchema = null;
    }

    public void putPartition(@NotNull byte[] key, long anchorValue) {
        ensureInitialized();
        mutations.putAnchor(key, anchorValue);
    }

    /**
     * Drops one entry the predecessor map holds. A forward freeze needs this because its
     * puts are not the whole truth: the frontier sweep takes keys out of the anchor map
     * without the seal walking what remains, so the removals arrive named rather than by
     * omission. A key the tree does not hold is a no-op, which is what a key created and
     * evicted inside one cadence lands on.
     * <p>
     * A complete snapshot removes by omission in {@link #build}, so pairing that mode
     * with this call risks two mutations naming one key, which the partition-map writer
     * rejects.
     */
    public void removePartition(@NotNull byte[] key) {
        ensureInitialized();
        mutations.remove(key);
    }

    private void ensureInitialized() {
        if (!isInitialized) {
            throw CairoException.critical(0).put("live view checkpoint anchor root builder is not initialized");
        }
    }

    private final class MissingPartitionVisitor implements LiveViewCheckpointPartitionMapReader.Visitor {
        @Override
        public void onEntry(@NotNull LiveViewCheckpointPartitionMapEntry entry) {
            if (!mutations.containsSortedKey(entry.getKey())) {
                mutations.remove(entry.getKey());
            }
        }
    }

}
