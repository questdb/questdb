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
import io.questdb.std.Transient;
import io.questdb.std.str.Path;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.TestOnly;

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
public class LiveViewCheckpointFunctionRootBuilder extends AbstractLiveViewCheckpointStateRootBuilder {

    private byte[] functionIdentity = new byte[0];
    private byte[] keySchema = new byte[0];
    private final LiveViewCheckpointFunctionRoot oldFunctionRoot;
    private final LiveViewCheckpointStatePageRef oldScalarStateRef = new LiveViewCheckpointStatePageRef();
    private final LiveViewCheckpointFunctionRoot resultFunctionRoot;
    private final LiveViewCheckpointStatePageRef scalarStateRef = new LiveViewCheckpointStatePageRef();
    private int stateFormatVersion;

    public LiveViewCheckpointFunctionRootBuilder(@NotNull CairoConfiguration configuration) {
        this(configuration, new LiveViewCheckpointPartitionMapObjectPool());
    }

    LiveViewCheckpointFunctionRootBuilder(
            @NotNull CairoConfiguration configuration,
            @NotNull LiveViewCheckpointPartitionMapObjectPool objectPool
    ) {
        super(configuration, objectPool, "function root");
        oldFunctionRoot = new LiveViewCheckpointFunctionRoot(configuration);
        resultFunctionRoot = new LiveViewCheckpointFunctionRoot(configuration);
    }

    @Override
    public void close() {
        Misc.free(oldFunctionRoot);
        Misc.free(resultFunctionRoot);
        super.close();
    }

    @Override
    protected void stageMutations() {
        mutations.sortAndValidate();
        for (int i = 0, n = mutations.getMutationCount(); i < n; i++) {
            final int mutationIndex = mutations.getSortedMutationIndex(i);
            partitionMapReader.adjustStateRefCounts(oldPartitionMapRoot, mutations, mutationIndex, segmentUseCounts, -1);
            if (mutations.operation(mutationIndex) != LiveViewCheckpointMutationArena.OP_REMOVE) {
                mutations.adjustRefCounts(segmentUseCounts, mutationIndex, 1);
            }
        }
    }

    @Override
    protected void writeRoot(
            @NotNull LiveViewCheckpointMetaSegmentWriter writer,
            @NotNull LiveViewCheckpointPageRef partitionMapRoot,
            @NotNull LiveViewCheckpointPageRef out
    ) {
        resultFunctionRoot.ofBuilder(
                functionIdentity,
                stateFormatVersion,
                keySchema,
                scalarStateRef,
                partitionMapRoot,
                segmentUseCounts
        );
        resultFunctionRoot.writeTo(writer, out);
    }

    @TestOnly
    boolean isBorrowingCompiledForTest(byte[] functionIdentity, byte[] keySchema) {
        return this.functionIdentity == functionIdentity && this.keySchema == keySchema;
    }

    public void of(
            @Transient @NotNull Path checkpointsDir,
            @NotNull LiveViewCheckpointPageRef oldFunctionRootRef,
            @NotNull byte[] functionIdentity,
            int stateFormatVersion,
            @NotNull byte[] keySchema
    ) {
        of0(checkpointsDir, oldFunctionRootRef, functionIdentity, stateFormatVersion, keySchema, false);
    }

    /**
     * Borrows compiler-owned arrays through the synchronous {@link #build} call.
     * The caller must keep them immutable and alive until build returns.
     */
    void ofBorrowedCompiled(
            @Transient @NotNull Path checkpointsDir,
            @NotNull LiveViewCheckpointPageRef oldFunctionRootRef,
            @NotNull byte[] functionIdentity,
            int stateFormatVersion,
            @NotNull byte[] keySchema
    ) {
        of0(checkpointsDir, oldFunctionRootRef, functionIdentity, stateFormatVersion, keySchema, true);
    }

    private void of0(
            @Transient @NotNull Path checkpointsDir,
            @NotNull LiveViewCheckpointPageRef oldFunctionRootRef,
            @NotNull byte[] functionIdentity,
            int stateFormatVersion,
            @NotNull byte[] keySchema,
            boolean isBorrowed
    ) {
        clearBorrowedCompiled();
        if (functionIdentity.length == 0 || stateFormatVersion <= 0) {
            throw CairoException.critical(0).put("live view checkpoint function identity or state version invalid");
        }
        LiveViewCheckpointMetadata.validateByteArrayLength(functionIdentity.length, "function identity");
        LiveViewCheckpointMetadata.validateByteArrayLength(keySchema.length, "function key schema");
        ofCheckpointsDir(checkpointsDir);
        this.functionIdentity = isBorrowed ? functionIdentity : functionIdentity.clone();
        this.keySchema = isBorrowed ? keySchema : keySchema.clone();
        this.stateFormatVersion = stateFormatVersion;
        boolean isInitializationComplete = false;
        try {
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
            isInitialized = true;
            isInitializationComplete = true;
        } finally {
            if (!isInitializationComplete) {
                clearBorrowedCompiled();
            }
        }
    }

    @Override
    protected void clearBorrowedCompiled() {
        resultFunctionRoot.clearBorrowedCompiled();
        isInitialized = false;
        functionIdentity = null;
        keySchema = null;
    }

    @Override
    public void detach() {
        oldFunctionRoot.detach();
        resultFunctionRoot.detach();
        super.detach();
    }

    /**
     * Stages one partition. The {@code keyLength} key bytes at {@code keyAddress} and the
     * {@code scalarLength} scalar bytes at {@code scalarAddress} are copied before this
     * returns.
     */
    public void putPartition(
            long keyAddress,
            int keyLength,
            long scalarAddress,
            int scalarLength,
            @NotNull LiveViewCheckpointStatePageRef[] statePageRefs
    ) {
        putPartition(keyAddress, keyLength, scalarAddress, scalarLength, statePageRefs, statePageRefs.length);
    }

    /**
     * Stages one partition whose references are the first {@code statePageRefCount} of
     * {@code statePageRefs}. The {@code keyLength} key bytes at {@code keyAddress}, the
     * {@code scalarLength} scalar bytes at {@code scalarAddress} and those references are
     * copied before this returns.
     */
    public void putPartition(
            long keyAddress,
            int keyLength,
            long scalarAddress,
            int scalarLength,
            @NotNull LiveViewCheckpointStatePageRef[] statePageRefs,
            int statePageRefCount
    ) {
        ensureInitialized();
        mutations.put(keyAddress, keyLength, scalarAddress, scalarLength, statePageRefs, statePageRefCount);
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

    private static void copyStateRef(LiveViewCheckpointStatePageRef from, LiveViewCheckpointStatePageRef to) {
        to.of(from.getSegmentId(), from.getOffset(), from.getStoredLength(), from.getDecodedLength(),
                from.getPageKind(), from.getCodec(), from.getRowCount(), from.getFlags());
    }

}
