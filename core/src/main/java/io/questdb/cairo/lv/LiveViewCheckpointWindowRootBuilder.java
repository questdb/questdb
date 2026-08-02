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
import org.jetbrains.annotations.Nullable;

import java.io.Closeable;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashSet;

/**
 * Builds one immutable {@link LiveViewCheckpointWindowRoot} and its changed
 * partition-map paths in the same metadata segment. The fused counterpart of
 * {@link LiveViewCheckpointAnchorRootBuilder}, and it works the same way: a complete
 * freeze treats its puts as the whole truth and removes every old entry it did not put,
 * a forward cadence freeze supplies only touched keys and leaves the rest where the
 * predecessor left them, and the copy-on-write writer drops equal puts either way.
 *
 * <h2>A manifest change is not an incremental seal</h2>
 * {@link #isCompatiblePredecessor} is what the caller has to ask before it may build on
 * a predecessor at all. Partition-map leaves are shared copy-on-write across
 * generations, so publishing a new manifest over leaves an older manifest wrote is a
 * silent misread rather than a rejection - the decoder finds the total length it expects
 * and reads the wrong fields out of it. Four things must therefore match, not one: the
 * window identity, the key schema, the anchor value type <b>and</b> the manifest, byte
 * for byte. Anything else - a legacy anchor root below, a component codec bump that left
 * {@code definitionTxn} alone, a reordered component - makes the seal start from an
 * empty tree and image every live key.
 */
public class LiveViewCheckpointWindowRootBuilder implements Closeable {

    private static final long NO_SEGMENT = -1;
    private static final LiveViewCheckpointStatePageRef[] NO_STATE_PAGES = new LiveViewCheckpointStatePageRef[0];
    private final Path checkpointsDir = new Path();
    private final LiveViewCheckpointPageRef oldPartitionMapRoot = new LiveViewCheckpointPageRef();
    private final LiveViewCheckpointWindowRoot oldWindowRoot;
    private final LiveViewCheckpointPartitionMapReader partitionMapReader;
    private final LiveViewCheckpointPartitionMapWriter partitionMapWriter;
    private final HashSet<ByteBuffer> putKeys = new HashSet<>();
    private final LiveViewCheckpointWindowRoot resultWindowRoot;
    private final LongList segmentUseCounts = new LongList();
    private final LiveViewCheckpointMetaSegmentWriter segmentWriter;
    private int anchorValueType;
    private boolean isCompleteSnapshot;
    private boolean isInitialized;
    private byte[] keySchema = new byte[0];
    private long lastSegmentBytes;
    private byte[] manifest = new byte[0];
    private int mutationCount;
    private LiveViewCheckpointPartitionMapWriter.Mutation[] mutations = new LiveViewCheckpointPartitionMapWriter.Mutation[8];
    /**
     * The repair's key domain, or null for a whole-truth build. It narrows removal by
     * omission to the keys a replay describes, exactly as the function path narrows it:
     * a key outside {@code Q} has no qualifying row in the replaced interval, so the
     * entry the old root wrote for it - anchor value and every component together - is
     * still the truth and is neither replaced nor removed.
     */
    private LiveViewCheckpointOutputKeyDomain outputKeys;
    /**
     * Metadata segment holding the window-root page this build supersedes, or
     * {@link #NO_SEGMENT} for the first window root of a timeline.
     */
    private long oldRootPageSegmentId = NO_SEGMENT;
    private int totalInlineStateBytes;
    private byte[] windowIdentity = new byte[0];

    public LiveViewCheckpointWindowRootBuilder(@NotNull CairoConfiguration configuration) {
        oldWindowRoot = new LiveViewCheckpointWindowRoot(configuration);
        partitionMapReader = new LiveViewCheckpointPartitionMapReader(configuration);
        partitionMapWriter = new LiveViewCheckpointPartitionMapWriter(configuration);
        resultWindowRoot = new LiveViewCheckpointWindowRoot(configuration);
        segmentWriter = new LiveViewCheckpointMetaSegmentWriter(configuration);
    }

    public void build(long metadataSegmentId, @NotNull LiveViewCheckpointPageRef out) {
        ensureInitialized();
        if (isCompleteSnapshot) {
            partitionMapReader.iterateAll(oldPartitionMapRoot, entry -> {
                if (outputKeys != null && !outputKeys.contains(entry.getKey())) {
                    return;
                }
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
        LiveViewCheckpointMetadata.adjustSegmentUseCount(
                segmentUseCounts,
                metadataSegmentId,
                partitionMapWriter.getLastSegmentPageCount() + 1
        );
        resultWindowRoot.ofBuilder(
                windowIdentity,
                anchorValueType,
                keySchema,
                manifest,
                totalInlineStateBytes,
                partitionMapRoot,
                segmentUseCounts
        );
        resultWindowRoot.writeTo(segmentWriter, out);
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
        Misc.free(oldWindowRoot);
        Misc.free(partitionMapReader);
        Misc.free(partitionMapWriter);
        Misc.free(resultWindowRoot);
        Misc.free(segmentWriter);
        Misc.free(checkpointsDir);
    }

    public long getLastSegmentBytes() {
        return lastSegmentBytes;
    }

    /**
     * Whether the root at {@code stateRootRef} may be built on incrementally by a seal
     * that lays its entries out the given way. False for a null reference, for a legacy
     * anchor root, and for any window root whose identity, key schema, anchor type or
     * manifest differs - all of which take the same full-scan conversion path.
     */
    public boolean isCompatiblePredecessor(
            @Transient @NotNull Path checkpointsDir,
            @NotNull LiveViewCheckpointPageRef stateRootRef,
            byte @NotNull [] windowIdentity,
            int anchorValueType,
            byte @NotNull [] keySchema,
            byte @NotNull [] manifest
    ) {
        if (stateRootRef.isNull() || !oldWindowRoot.ofIfWindowRoot(checkpointsDir, stateRootRef)) {
            return false;
        }
        return Arrays.equals(windowIdentity, oldWindowRoot.getWindowIdentity())
                && Arrays.equals(keySchema, oldWindowRoot.getKeySchema())
                && anchorValueType == oldWindowRoot.getAnchorValueType()
                && Arrays.equals(manifest, oldWindowRoot.getManifest());
    }

    /**
     * Starts one window root.
     *
     * @param oldStateRootRef  the boundary's predecessor state root. A reference this
     *                         build is not {@link #isCompatiblePredecessor compatible}
     *                         with is treated as absent: the tree is built from empty
     *                         and every live key is imaged, which is what conversion
     *                         from a legacy predecessor or across a manifest change
     *                         means
     * @param isCompleteSnapshot whether the puts that follow are the whole truth, so
     *                         {@link #build} may remove by omission
     * @param outputKeys       the repair's key domain, narrowing that removal to the
     *                         keys its replay describes; null for a whole-truth build
     */
    public void of(
            @Transient @NotNull Path checkpointsDir,
            @NotNull LiveViewCheckpointPageRef oldStateRootRef,
            byte @NotNull [] windowIdentity,
            int anchorValueType,
            byte @NotNull [] keySchema,
            byte @NotNull [] manifest,
            int totalInlineStateBytes,
            boolean isCompleteSnapshot,
            @Nullable LiveViewCheckpointOutputKeyDomain outputKeys
    ) {
        isInitialized = false;
        if (windowIdentity.length == 0 || keySchema.length < Integer.BYTES || manifest.length == 0
                || totalInlineStateBytes <= LiveViewWindowStatePlan.ANCHOR_STATE_BYTES) {
            throw CairoException.critical(0).put("live view checkpoint window state root identity or layout invalid");
        }
        LiveViewCheckpointMetadata.validateByteArrayLength(windowIdentity.length, "window state identity");
        LiveViewCheckpointMetadata.validateByteArrayLength(keySchema.length, "window state key schema");
        LiveViewCheckpointMetadata.validateByteArrayLength(manifest.length, "window state manifest");
        this.checkpointsDir.of(checkpointsDir);
        partitionMapReader.of(checkpointsDir);
        partitionMapWriter.of(checkpointsDir);
        this.windowIdentity = Arrays.copyOf(windowIdentity, windowIdentity.length);
        this.anchorValueType = anchorValueType;
        this.keySchema = Arrays.copyOf(keySchema, keySchema.length);
        this.manifest = Arrays.copyOf(manifest, manifest.length);
        this.totalInlineStateBytes = totalInlineStateBytes;
        this.isCompleteSnapshot = isCompleteSnapshot;
        this.outputKeys = outputKeys;
        mutationCount = 0;
        putKeys.clear();
        segmentUseCounts.clear();
        final boolean hasCompatiblePredecessor = isCompatiblePredecessor(
                checkpointsDir,
                oldStateRootRef,
                windowIdentity,
                anchorValueType,
                keySchema,
                manifest
        );
        oldRootPageSegmentId = hasCompatiblePredecessor ? oldStateRootRef.getSegmentId() : NO_SEGMENT;
        if (!hasCompatiblePredecessor) {
            // An incompatible predecessor's pages are not this root's to share or to
            // release: the conversion seal writes a whole new tree, and the old root's
            // segments retire with the boundary that still names them.
            oldPartitionMapRoot.clear();
        } else {
            oldWindowRoot.getPartitionMapRootRef(oldPartitionMapRoot);
            for (int i = 0, n = oldWindowRoot.getSegmentUseCountSize(); i < n; i++) {
                segmentUseCounts.add(oldWindowRoot.getSegmentId(i), oldWindowRoot.getSegmentUseCount(i));
            }
        }
        isInitialized = true;
    }

    /**
     * Stages one fused entry: the whole scalar payload for {@code key}, anchor value and
     * every component together. The array is stored rather than copied, so the caller
     * must hand over a fresh one per partition.
     * <p>
     * {@code isUnchanged} names a key whose payload the predecessor entry already holds
     * byte for byte. It is still part of a complete snapshot's put domain - the key is
     * live, and removal by omission would otherwise take out an entry nothing replaced -
     * but no mutation is staged for it, so a full-scan seal over a cold key set neither
     * allocates a mutation nor descends the tree per key. Nothing published distinguishes
     * the two: the partition-map writer drops an equal put anyway.
     */
    public void putPartition(byte @NotNull [] key, byte @NotNull [] scalarState, boolean isUnchanged) {
        ensureInitialized();
        if (scalarState.length != totalInlineStateBytes) {
            throw CairoException.critical(0)
                    .put("live view checkpoint window state payload width does not match the manifest")
                    .put(" [expected=").put(totalInlineStateBytes)
                    .put(", actual=").put(scalarState.length).put(']');
        }
        if (isCompleteSnapshot) {
            // Only a complete snapshot needs the put domain, and only to name the entries
            // it must remove. A forward freeze pays neither the key copy nor the set
            // insert: duplicates still raise one layer down, where the partition-map
            // writer sorts the mutations and rejects two that name the same key.
            putKeys.add(ByteBuffer.wrap(Arrays.copyOf(key, key.length)));
        }
        if (!isUnchanged) {
            mutationAt(mutationCount++).put(key, scalarState, NO_STATE_PAGES);
        }
    }

    /**
     * Drops one entry the predecessor map holds. A forward freeze needs this because its
     * puts are not the whole truth: the frontier sweep takes keys out of the window's own
     * map without the seal walking what remains, so the removals arrive named rather than
     * by omission. A key the tree does not hold is a no-op.
     */
    public void removePartition(byte @NotNull [] key) {
        ensureInitialized();
        mutationAt(mutationCount++).remove(key);
    }

    private void ensureInitialized() {
        if (!isInitialized) {
            throw CairoException.critical(0)
                    .put("live view checkpoint window state root builder is not initialized");
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
