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
 */
public class LiveViewCheckpointFunctionRootBuilder implements Closeable {

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
            adjustSegment(segmentUseCounts, oldScalarStateRef.getSegmentId(), -1);
        }
        copyStateRef(ref, scalarStateRef);
        if (!ref.isNull()) {
            adjustSegment(segmentUseCounts, ref.getSegmentId(), 1);
        }
        copyStateRef(ref, oldScalarStateRef);
    }

    private static void adjustRefs(LongList counts, LiveViewCheckpointPartitionMapEntry entry, int delta) {
        for (int i = 0; i < entry.getStatePageCount(); i++) {
            adjustSegment(counts, entry.getStatePageRef(i).getSegmentId(), delta);
        }
    }

    private static void adjustSegment(LongList counts, long segmentId, int delta) {
        int lo = 0;
        int hi = counts.size() / 2;
        while (lo < hi) {
            final int mid = (lo + hi) >>> 1;
            if (counts.getQuick(mid * 2) < segmentId) {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        if (lo < counts.size() / 2 && counts.getQuick(lo * 2) == segmentId) {
            final int countIndex = lo * 2 + 1;
            final long oldCount = counts.getQuick(countIndex);
            if (delta < 0 && oldCount == 1) {
                counts.removeIndex(countIndex);
                counts.removeIndex(countIndex - 1);
            } else {
                if ((delta < 0 && oldCount <= 0) || (delta > 0 && oldCount == Long.MAX_VALUE)) {
                    throw CairoException.critical(0).put("live view checkpoint function segment use count overflow");
                }
                counts.setQuick(countIndex, oldCount + delta);
            }
        } else if (delta > 0) {
            counts.add(lo * 2, segmentId);
            counts.add(lo * 2 + 1, 1);
        } else {
            throw CairoException.critical(0).put("live view checkpoint function segment use count underflow, segmentId=").put(segmentId);
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
