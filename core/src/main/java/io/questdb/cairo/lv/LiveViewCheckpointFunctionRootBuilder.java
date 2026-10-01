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
 * Builds one immutable function root and its changed partition-map paths in the
 * same metadata segment. Segment use counts are adjusted from only the changed
 * old/new partition entries, avoiding a full-map reachability walk at seal time.
 * <p>
 * The same counts carry the root's metadata closure. A function root's data
 * entries count the state-page references its map holds in each data segment; its
 * metadata entries count the pages of each boundary-metadata segment the root
 * itself reaches - its own page plus the partition-map pages below it. Both are
 * maintained the same way, from the delta of one build rather than a walk, and
 * the metadata half is what lets a parent checkpoint root state the complete set
 * of metadata segments a boundary names, so retiring the boundary releases them
 * in one reference transaction. The two id spaces are disjoint - one id names at
 * most one file, in exactly one of {@code data/} and {@code meta/} - so they
 * share the list without ambiguity.
 */
public class LiveViewCheckpointFunctionRootBuilder implements Closeable {

    private static final long NO_SEGMENT = -1;
    private final Path checkpointsDir = new Path();
    private byte[] functionIdentity = new byte[0];
    private byte[] keySchema = new byte[0];
    private final LiveViewCheckpointFunctionRoot oldFunctionRoot;
    private final LiveViewCheckpointPageRef oldPartitionMapRoot = new LiveViewCheckpointPageRef();
    private final LiveViewCheckpointStatePageRef oldScalarStateRef = new LiveViewCheckpointStatePageRef();
    private final LiveViewCheckpointPartitionMapEntry oldEntry = new LiveViewCheckpointPartitionMapEntry();
    private final LiveViewCheckpointPartitionMapReader partitionMapReader;
    private final LiveViewCheckpointPartitionMapWriter partitionMapWriter;
    private final LiveViewCheckpointFunctionRoot resultFunctionRoot;
    private final LongList segmentUseCounts = new LongList();
    private final LiveViewCheckpointMetaSegmentWriter segmentWriter;
    private final LiveViewCheckpointStatePageRef scalarStateRef = new LiveViewCheckpointStatePageRef();
    private LiveViewCheckpointPartitionMapWriter.Mutation[] mutations = new LiveViewCheckpointPartitionMapWriter.Mutation[8];
    private boolean initialized;
    private long lastSegmentBytes;
    private int mutationCount;
    /**
     * Metadata segment holding the function-root page this build supersedes, or
     * {@link #NO_SEGMENT} for the first root of a function.
     */
    private long oldRootPageSegmentId = NO_SEGMENT;
    private int stateFormatVersion;

    public LiveViewCheckpointFunctionRootBuilder(@NotNull CairoConfiguration configuration) {
        oldFunctionRoot = new LiveViewCheckpointFunctionRoot(configuration);
        partitionMapReader = new LiveViewCheckpointPartitionMapReader(configuration);
        partitionMapWriter = new LiveViewCheckpointPartitionMapWriter(configuration);
        resultFunctionRoot = new LiveViewCheckpointFunctionRoot(configuration);
        segmentWriter = new LiveViewCheckpointMetaSegmentWriter(configuration);
    }

    @Override
    public void close() {
        Misc.free(oldFunctionRoot);
        Misc.free(partitionMapReader);
        Misc.free(partitionMapWriter);
        Misc.free(resultFunctionRoot);
        Misc.free(segmentWriter);
        Misc.free(checkpointsDir);
    }

    public void build(long metadataSegmentId, @NotNull LiveViewCheckpointPageRef out) {
        ensureInitialized();
        final LongList candidateCounts = new LongList(segmentUseCounts);
        for (int i = 0; i < mutationCount; i++) {
            final LiveViewCheckpointPartitionMapWriter.Mutation mutation = mutations[i];
            final boolean hadOld = partitionMapReader.find(oldPartitionMapRoot, mutation.entry().getKey(), oldEntry);
            if (hadOld) {
                adjustRefs(candidateCounts, oldEntry, -1);
            }
            if (!mutation.isRemove()) {
                adjustRefs(candidateCounts, mutation.entry(), 1);
            }
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
        // The metadata closure moves by exactly what the path copy took away and
        // what this segment gains: every superseded map page, the root page this
        // build replaces, and the pages written here - the map's copied path plus
        // the root page about to follow it.
        final LongList releasedSegmentIds = partitionMapWriter.getLastReleasedSegmentIds();
        for (int i = 0, n = releasedSegmentIds.size(); i < n; i++) {
            LiveViewCheckpointMetadata.adjustSegmentUseCount(candidateCounts, releasedSegmentIds.getQuick(i), -1);
        }
        if (oldRootPageSegmentId != NO_SEGMENT) {
            LiveViewCheckpointMetadata.adjustSegmentUseCount(candidateCounts, oldRootPageSegmentId, -1);
        }
        LiveViewCheckpointMetadata.adjustSegmentUseCount(candidateCounts, metadataSegmentId, partitionMapWriter.getLastSegmentPageCount() + 1);
        resultFunctionRoot.ofBuilder(
                functionIdentity,
                stateFormatVersion,
                keySchema,
                scalarStateRef,
                partitionMapRoot,
                candidateCounts
        );
        resultFunctionRoot.writeTo(segmentWriter, out);
        lastSegmentBytes = segmentWriter.commit();
        segmentUseCounts.clear();
        segmentUseCounts.add(candidateCounts);
        oldPartitionMapRoot.of(partitionMapRoot.getSegmentId(), partitionMapRoot.getOffset(), partitionMapRoot.getLength());
        oldRootPageSegmentId = metadataSegmentId;
        mutationCount = 0;
    }

    public void of(
            @Transient @NotNull Path checkpointsDir,
            @NotNull LiveViewCheckpointPageRef oldFunctionRootRef,
            @NotNull byte[] functionIdentity,
            int stateFormatVersion,
            @NotNull byte[] keySchema
    ) {
        initialized = false;
        if (functionIdentity.length == 0 || stateFormatVersion <= 0) {
            throw CairoException.critical(0).put("live view checkpoint function identity or state version invalid");
        }
        LiveViewCheckpointMetadata.validateByteArrayLength(functionIdentity.length, "function identity");
        LiveViewCheckpointMetadata.validateByteArrayLength(keySchema.length, "function key schema");
        this.checkpointsDir.of(checkpointsDir);
        partitionMapReader.of(checkpointsDir);
        partitionMapWriter.of(checkpointsDir);
        this.functionIdentity = Arrays.copyOf(functionIdentity, functionIdentity.length);
        this.keySchema = Arrays.copyOf(keySchema, keySchema.length);
        this.stateFormatVersion = stateFormatVersion;
        mutationCount = 0;
        segmentUseCounts.clear();
        oldRootPageSegmentId = oldFunctionRootRef.isNull() ? NO_SEGMENT : oldFunctionRootRef.getSegmentId();
        if (oldFunctionRootRef.isNull()) {
            oldPartitionMapRoot.clear();
            oldScalarStateRef.clear();
            scalarStateRef.clear();
        } else {
            oldFunctionRoot.of(checkpointsDir, oldFunctionRootRef);
            if (!Arrays.equals(functionIdentity, oldFunctionRoot.getFunctionIdentity())
                    || !Arrays.equals(keySchema, oldFunctionRoot.getKeySchema())
                    || stateFormatVersion != oldFunctionRoot.getStateFormatVersion()) {
                throw CairoException.critical(0).put("live view checkpoint function root identity or schema mismatch");
            }
            oldFunctionRoot.getPartitionMapRootRef(oldPartitionMapRoot);
            oldFunctionRoot.getScalarStateRef(oldScalarStateRef);
            copyStateRef(oldScalarStateRef, scalarStateRef);
            for (int i = 0; i < oldFunctionRoot.getSegmentUseCountSize(); i++) {
                segmentUseCounts.add(oldFunctionRoot.getSegmentId(i), oldFunctionRoot.getSegmentUseCount(i));
            }
        }
        initialized = true;
    }

    public long getLastSegmentBytes() {
        return lastSegmentBytes;
    }

    public void putPartition(
            @NotNull byte[] key,
            @NotNull byte[] scalarState,
            @NotNull LiveViewCheckpointStatePageRef[] statePageRefs
    ) {
        ensureInitialized();
        mutationAt(mutationCount++).put(key, scalarState, statePageRefs);
    }

    public void removePartition(@NotNull byte[] key) {
        ensureInitialized();
        mutationAt(mutationCount++).remove(key);
    }

    public void setScalarStateRef(@NotNull LiveViewCheckpointStatePageRef ref) {
        ensureInitialized();
        LiveViewCheckpointMetadata.validateStateRef(ref, true, "function scalar");
        if (!oldScalarStateRef.isNull()) {
            LiveViewCheckpointMetadata.adjustSegmentUseCount(segmentUseCounts, oldScalarStateRef.getSegmentId(), -1);
        }
        copyStateRef(ref, scalarStateRef);
        if (!ref.isNull()) {
            LiveViewCheckpointMetadata.adjustSegmentUseCount(segmentUseCounts, ref.getSegmentId(), 1);
        }
        copyStateRef(ref, oldScalarStateRef);
    }

    private static void adjustRefs(LongList counts, LiveViewCheckpointPartitionMapEntry entry, int delta) {
        for (int i = 0; i < entry.getStatePageCount(); i++) {
            LiveViewCheckpointMetadata.adjustSegmentUseCount(counts, entry.getStatePageRef(i).getSegmentId(), delta);
        }
    }

    private static void copyStateRef(LiveViewCheckpointStatePageRef from, LiveViewCheckpointStatePageRef to) {
        to.of(from.getSegmentId(), from.getOffset(), from.getStoredLength(), from.getDecodedLength(),
                from.getPageKind(), from.getCodec(), from.getRowCount(), from.getFlags());
    }

    private void ensureInitialized() {
        if (!initialized) {
            throw CairoException.critical(0).put("live view checkpoint function root builder is not initialized");
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
