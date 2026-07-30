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
 * A freeze enumerates the complete live anchor map, so {@link #build} treats the
 * puts it received as the whole truth: every old entry the freeze did not put is
 * removed. The copy-on-write map writer drops a put whose key and value already
 * match, which is what keeps an adjacent seal proportional to the partitions
 * whose anchor value actually moved rather than to the map's size.
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
    private boolean initialized;
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
        partitionMapReader.iterateAll(oldPartitionMapRoot, entry -> {
            if (!putKeys.contains(ByteBuffer.wrap(entry.getKey()))) {
                mutationAt(mutationCount++).remove(entry.getKey());
            }
        });

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
        initialized = false;
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
        initialized = true;
    }

    public void putPartition(@NotNull byte[] key, long anchorValue) {
        ensureInitialized();
        if (!putKeys.add(ByteBuffer.wrap(Arrays.copyOf(key, key.length)))) {
            throw CairoException.critical(0).put("duplicate live view checkpoint anchor partition key");
        }
        mutationAt(mutationCount++).put(
                key,
                LiveViewCheckpointAnchorRoot.encodeAnchorValue(anchorValue),
                NO_STATE_PAGES
        );
    }

    private void ensureInitialized() {
        if (!initialized) {
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
