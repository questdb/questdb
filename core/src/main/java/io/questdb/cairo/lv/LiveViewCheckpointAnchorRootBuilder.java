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
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashSet;

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
    private static final LiveViewCheckpointStatePageRef[] NO_STATE_PAGES = new LiveViewCheckpointStatePageRef[0];
    private final Path checkpointsDir = new Path();
    private final LiveViewCheckpointAnchorRoot oldAnchorRoot;
    private final LiveViewCheckpointPageRef oldPartitionMapRoot = new LiveViewCheckpointPageRef();
    private final LiveViewCheckpointPartitionMapReader partitionMapReader;
    private final LiveViewCheckpointPartitionMapWriter partitionMapWriter;
    private final HashSet<ByteBuffer> putKeys = new HashSet<>();
    private final LiveViewCheckpointAnchorRoot resultAnchorRoot;
    private final LongList segmentUseCounts = new LongList();
    private final LiveViewCheckpointMetaSegmentWriter segmentWriter;
    private int anchorValueType;
    private boolean isCompleteSnapshot;
    private boolean isInitialized;
    private byte[] keySchema = new byte[0];
    private long lastSegmentBytes;
    private int mutationCount;
    private LiveViewCheckpointPartitionMapWriter.Mutation[] mutations = new LiveViewCheckpointPartitionMapWriter.Mutation[8];
    /**
     * Metadata segment holding the anchor-root page this build supersedes, or
     * {@link #NO_SEGMENT} for the first anchor root of a timeline.
     */
    private long oldRootPageSegmentId = NO_SEGMENT;
    private byte[] windowName = new byte[0];

    public LiveViewCheckpointAnchorRootBuilder(@NotNull CairoConfiguration configuration) {
        oldAnchorRoot = new LiveViewCheckpointAnchorRoot(configuration);
        partitionMapReader = new LiveViewCheckpointPartitionMapReader(configuration);
        partitionMapWriter = new LiveViewCheckpointPartitionMapWriter(configuration);
        resultAnchorRoot = new LiveViewCheckpointAnchorRoot(configuration);
        segmentWriter = new LiveViewCheckpointMetaSegmentWriter(configuration);
    }

    public void build(long metadataSegmentId, @NotNull LiveViewCheckpointPageRef out) {
        ensureInitialized();
        if (isCompleteSnapshot) {
            partitionMapReader.iterateAll(oldPartitionMapRoot, entry -> {
                if (!putKeys.contains(ByteBuffer.wrap(entry.getKey()))) {
                    mutationAt(mutationCount++).remove(entry.getKey());
                }
            });
        }

        segmentWriter.of(checkpointsDir, metadataSegmentId);
        final LiveViewCheckpointPageRef partitionMapRoot = new LiveViewCheckpointPageRef();
        partitionMapWriter.applyToOpenSegment(
                oldPartitionMapRoot,
                mutations,
                mutationCount,
                segmentWriter,
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
        resultAnchorRoot.writeTo(segmentWriter, out);
        lastSegmentBytes = segmentWriter.commit();
        oldPartitionMapRoot.of(
                partitionMapRoot.getSegmentId(),
                partitionMapRoot.getOffset(),
                partitionMapRoot.getLength()
        );
        oldRootPageSegmentId = metadataSegmentId;
        mutationCount = 0;
        putKeys.clear();
    }

    @Override
    public void close() {
        Misc.free(oldAnchorRoot);
        Misc.free(partitionMapReader);
        Misc.free(partitionMapWriter);
        Misc.free(resultAnchorRoot);
        Misc.free(segmentWriter);
        Misc.free(checkpointsDir);
    }

    public long getLastSegmentBytes() {
        return lastSegmentBytes;
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
        isInitialized = false;
        if (windowName.length == 0 || keySchema.length < Integer.BYTES) {
            throw CairoException.critical(0).put("live view checkpoint anchor window name or key schema invalid");
        }
        LiveViewCheckpointMetadata.validateByteArrayLength(windowName.length, "anchor window name");
        LiveViewCheckpointMetadata.validateByteArrayLength(keySchema.length, "anchor key schema");
        this.checkpointsDir.of(checkpointsDir);
        partitionMapReader.of(checkpointsDir);
        partitionMapWriter.of(checkpointsDir);
        this.windowName = Arrays.copyOf(windowName, windowName.length);
        this.anchorValueType = anchorValueType;
        this.keySchema = Arrays.copyOf(keySchema, keySchema.length);
        this.isCompleteSnapshot = isCompleteSnapshot;
        mutationCount = 0;
        putKeys.clear();
        segmentUseCounts.clear();
        oldRootPageSegmentId = oldAnchorRootRef.isNull() ? NO_SEGMENT : oldAnchorRootRef.getSegmentId();
        if (oldAnchorRootRef.isNull()) {
            oldPartitionMapRoot.clear();
        } else {
            oldAnchorRoot.of(checkpointsDir, oldAnchorRootRef);
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
    }

    public void putPartition(@NotNull byte[] key, long anchorValue) {
        ensureInitialized();
        if (isCompleteSnapshot) {
            // Only a complete snapshot needs the put domain, and only to name the
            // entries it must remove. A forward freeze pays neither the key copy nor
            // the set insert: duplicates still raise, one layer down, where
            // LiveViewCheckpointPartitionMapWriter sorts the mutations and rejects
            // two that name the same key.
            putKeys.add(ByteBuffer.wrap(Arrays.copyOf(key, key.length)));
        }
        mutationAt(mutationCount++).put(
                key,
                LiveViewCheckpointAnchorRoot.encodeAnchorValue(anchorValue),
                NO_STATE_PAGES
        );
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
        mutationAt(mutationCount++).remove(key);
    }

    private void ensureInitialized() {
        if (!isInitialized) {
            throw CairoException.critical(0).put("live view checkpoint anchor root builder is not initialized");
        }
    }

    private LiveViewCheckpointPartitionMapWriter.Mutation mutationAt(int index) {
        if (index >= mutations.length) {
            mutations = Arrays.copyOf(mutations, mutations.length * 2);
        }
        if (mutations[index] == null) {
            mutations[index] = new LiveViewCheckpointPartitionMapWriter.Mutation();
        }
        return mutations[index];
    }
}
