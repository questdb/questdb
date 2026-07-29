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
import io.questdb.cairo.map.Map;
import io.questdb.cairo.map.MapKey;
import io.questdb.cairo.map.MapValue;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryCARW;
import io.questdb.griffin.engine.window.WindowFunction;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.Transient;
import io.questdb.std.str.Path;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.Closeable;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/**
 * Resolves and restores one exact logical checkpoint from a pinned timeline
 * generation. Startup reconciliation and root selection are intentionally left
 * to the recovery layer; this component owns only indexed lookup, lazy deep
 * validation, and rehydration of a caller-owned runtime.
 */
public class LiveViewCheckpointTimelineStoreReader implements Closeable {

    private static final int DATA_READER_CACHE_SIZE = 8;
    // Predecessors {@link #restoreLatestCompatible} retries past corrupt selected
    // roots before it declares the checkpoint storage unrecoverable. A single
    // damaged data page is the realistic case; a longer contiguous run means the
    // storage is broadly compromised and the caller rebuilds from the applied base
    // rather than trusting more of the timeline.
    private static final int MAX_CORRUPT_ROOT_FALLBACKS = 8;
    private final LiveViewCheckpointAnchorRoot anchorRoot;
    private final Path checkpointsDir = new Path();
    private final CairoConfiguration configuration;
    private final LiveViewCheckpointDataSegmentReader[] dataReaders =
            new LiveViewCheckpointDataSegmentReader[DATA_READER_CACHE_SIZE];
    private final long[] dataSegmentIds = new long[DATA_READER_CACHE_SIZE];
    private final LiveViewCheckpointRowPositionDeltaReader deltaReader;
    private final LiveViewCheckpointFunctionDirectory functionDirectory;
    private final LiveViewCheckpointFunctionRoot functionRoot;
    private final MemoryCARW keyMemory;
    private final LiveViewStatePageReader keyPageReader = new LiveViewStatePageReader();
    private final LiveViewCheckpointMetaStore metaStore;
    private final LiveViewCheckpointPartitionMapReader partitionReader;
    private final LiveViewCheckpointRangeRingStateReader ringStateReader;
    private final LiveViewCheckpointRoot root;
    private final LiveViewCheckpointSegmentDirectoryReader segmentDirectory;
    private final LiveViewCheckpointSegmentDirectoryEntry segmentDirectoryEntry = new LiveViewCheckpointSegmentDirectoryEntry();
    private final LiveViewStatePageReader statePageReader = new LiveViewStatePageReader();
    private final LiveViewCheckpointTimelineReader timelineReader;
    private int dataReaderClock;
    private boolean isOpen;

    public LiveViewCheckpointTimelineStoreReader(@NotNull CairoConfiguration configuration) {
        this.configuration = configuration;
        anchorRoot = new LiveViewCheckpointAnchorRoot(configuration);
        deltaReader = new LiveViewCheckpointRowPositionDeltaReader(configuration);
        functionDirectory = new LiveViewCheckpointFunctionDirectory(configuration);
        functionRoot = new LiveViewCheckpointFunctionRoot(configuration);
        keyMemory = Vm.getCARWInstance(4096, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT);
        metaStore = new LiveViewCheckpointMetaStore(configuration);
        partitionReader = new LiveViewCheckpointPartitionMapReader(configuration);
        ringStateReader = new LiveViewCheckpointRangeRingStateReader(configuration);
        root = new LiveViewCheckpointRoot(configuration);
        segmentDirectory = new LiveViewCheckpointSegmentDirectoryReader(configuration);
        timelineReader = new LiveViewCheckpointTimelineReader(configuration);
        Arrays.fill(dataSegmentIds, -1);
    }

    @Override
    public void close() {
        Misc.free(anchorRoot);
        for (int i = 0; i < DATA_READER_CACHE_SIZE; i++) {
            dataReaders[i] = Misc.free(dataReaders[i]);
            dataSegmentIds[i] = -1;
        }
        Misc.free(deltaReader);
        Misc.free(functionDirectory);
        Misc.free(functionRoot);
        Misc.free(keyMemory);
        Misc.free(metaStore);
        Misc.free(partitionReader);
        Misc.free(ringStateReader);
        Misc.free(root);
        Misc.free(segmentDirectory);
        Misc.free(timelineReader);
        Misc.free(checkpointsDir);
        isOpen = false;
    }

    /**
     * Ends this binding, unmapping every file the restore touched but keeping the
     * readers themselves, so the next {@link #of(Path)} rebinds a reader that is
     * already built instead of building one. A worker holds one reader across its
     * whole life this way, rather than a reader per restored root.
     * <p>
     * Nothing derived from the previous binding survives: mappings are dropped
     * because the timeline they name may be retired, repaired, compacted - or
     * rebuilt from a segment id space that restarts - before the next bind, and
     * the meta store re-reads the superblock for the same reason.
     */
    public void detach() {
        for (int i = 0; i < DATA_READER_CACHE_SIZE; i++) {
            if (dataReaders[i] != null) {
                dataReaders[i].close();
            }
            dataSegmentIds[i] = -1;
        }
        dataReaderClock = 0;
        anchorRoot.detach();
        deltaReader.detach();
        functionDirectory.detach();
        functionRoot.detach();
        metaStore.detach();
        partitionReader.detach();
        ringStateReader.detach();
        root.detach();
        segmentDirectory.detach();
        timelineReader.detach();
        isOpen = false;
    }

    public void of(@Transient @NotNull Path checkpointsDir) {
        if (isOpen) {
            throw CairoException.critical(0).put("live view checkpoint timeline restore reader already open");
        }
        this.checkpointsDir.of(checkpointsDir);
        try {
            metaStore.of(checkpointsDir);
            if (!metaStore.isValid()) {
                throw invalid("has no valid generation to restore");
            }
            deltaReader.of(checkpointsDir);
            partitionReader.of(checkpointsDir);
            timelineReader.of(checkpointsDir);
        } catch (Throwable t) {
            // A reader that outlives the failed bind must not keep the meta store
            // half-open: the next bind would meet it already open and fail there
            // instead, for good.
            detach();
            throw t;
        }
        isOpen = true;
    }

    /**
     * Restores the exact composite timeline key into the supplied, already-open
     * runtime. The caller owns the runtime and may discard it if a function
     * decoder reports semantic corruption.
     */
    public Result restore(
            long maxTimestamp,
            long checkpointId,
            long expectedDefinitionTxn,
            @NotNull ObjList<WindowFunction> functions,
            @Nullable LiveViewWindow anchorWindow
    ) {
        ensureOpen();
        try (LiveViewCheckpointGenerationPin pin = metaStore.pin()) {
            return restorePinned(
                    pin,
                    maxTimestamp,
                    checkpointId,
                    expectedDefinitionTxn,
                    functions,
                    anchorWindow,
                    Numbers.LONG_NULL
            );
        }
    }

    /**
     * Restores the newest logical root the current generation holds, with no
     * durable-materialization compatibility check.
     * <p>
     * This is the seed sweep's resume: mid-sweep the live view is still SEEDING
     * on disk, so there is no reconciled frontier to select against - the sweep
     * resumes from wherever the newest root left the cursor, and the caller
     * cross-checks that root's row position against the live-view table itself.
     * The restored {@link Result#seedCursorOffset} is
     * {@link Numbers#LONG_NULL} when a steady seal or a repair published this
     * generation, which tells the caller the newest root is not a resume point.
     */
    public Result restoreLatest(
            long expectedDefinitionTxn,
            @NotNull ObjList<WindowFunction> functions,
            @Nullable LiveViewWindow anchorWindow
    ) {
        ensureOpen();
        try (LiveViewCheckpointGenerationPin pin = metaStore.pin()) {
            final LiveViewCheckpointSuperblock superblock = metaStore.getSuperblock();
            if (superblock.definitionTxn != expectedDefinitionTxn) {
                throw invalid("definition identity mismatch [stored=").put(superblock.definitionTxn)
                        .put(", expected=").put(expectedDefinitionTxn).put(']');
            }
            final LiveViewCheckpointTimelineEntry entry = new LiveViewCheckpointTimelineEntry();
            if (!timelineReader.last(pin.getTimelineRootRef(), entry)) {
                throw invalid("holds no logical root");
            }
            return restorePinned(
                    pin,
                    entry.maxTimestamp,
                    entry.checkpointId,
                    expectedDefinitionTxn,
                    functions,
                    anchorWindow,
                    Numbers.LONG_NULL
            );
        }
    }

    /**
     * Selects and restores the newest root compatible with the reconciled
     * durable live-view coordinates. Selection and lazy root/page validation
     * run under the same generation pin, so publication cannot mix tree roots
     * from different generations.
     * <p>
     * A structurally invalid data page in the selected root does not fail the
     * whole generation: the design isolates that damage to the one root version.
     * This method retries a bounded run of predecessors under the same pin,
     * skipping each corrupt root until one restores, and reports the highest
     * skipped boundary in {@link Result#corruptCeilingMaxTs} so the caller can
     * reconstruct the same logical checkpoint ids. Only a run longer than
     * {@link #MAX_CORRUPT_ROOT_FALLBACKS}, or a corrupt oldest root with no
     * predecessor left, surfaces as checkpoint-storage corruption; generation and
     * superblock corruption still propagate directly, before the fallback runs.
     */
    public Result restoreLatestCompatible(
            long durableFrontierTimestamp,
            long durableBaseSeqTxn,
            long durableLvSeqTxn,
            long durableLvRowCount,
            long expectedDefinitionTxn,
            @NotNull ObjList<WindowFunction> functions,
            @Nullable LiveViewWindow anchorWindow
    ) {
        ensureOpen();
        try (LiveViewCheckpointGenerationPin pin = metaStore.pin()) {
            final LiveViewCheckpointSuperblock superblock = metaStore.getSuperblock();
            if (superblock.definitionTxn != expectedDefinitionTxn) {
                throw invalid("definition identity mismatch [stored=").put(superblock.definitionTxn)
                        .put(", expected=").put(expectedDefinitionTxn).put(']');
            }
            if (pin.getNormalizedBaseSeqTxn() > durableBaseSeqTxn
                    || pin.getCoveredLvSeqTxn() > durableLvSeqTxn) {
                throw invalid("generation is ahead of durable materialization")
                        .put(" [generationBase=").put(pin.getNormalizedBaseSeqTxn())
                        .put(", durableBase=").put(durableBaseSeqTxn)
                        .put(", generationLv=").put(pin.getCoveredLvSeqTxn())
                        .put(", durableLv=").put(durableLvSeqTxn).put(']');
            }

            final LiveViewCheckpointTimelineEntry entry = new LiveViewCheckpointTimelineEntry();
            if (!timelineReader.floor(pin.getTimelineRootRef(), durableFrontierTimestamp, entry)) {
                throw invalid("has no root at or below durable frontier [frontier=")
                        .put(durableFrontierTimestamp).put(']');
            }
            // Bounded predecessor fallback: the floor root is the newest compatible
            // one, but its data page may be structurally invalid. That damage is
            // scoped to the one root version, so retry the next-older boundary under
            // this same pin until one restores, and remember the highest corrupt
            // boundary so the caller can reconstruct exactly those ids. The floor is
            // the only candidate the durable-compatibility gate rejects outright: an
            // older predecessor covers strictly fewer rows and cannot be ahead of the
            // durable materialization, so it is skipped only when its own restore
            // fails.
            long corruptCeilingMaxTs = Numbers.LONG_NULL;
            int fallbacks = 0;
            boolean isFloor = true;
            while (true) {
                final long effectiveLvRowPosition = deltaReader.effectivePosition(
                        pin.getRowPositionDeltaRootRef(),
                        entry
                );
                final boolean compatible = entry.createdLvSeqTxn <= durableLvSeqTxn
                        && effectiveLvRowPosition >= 0
                        && effectiveLvRowPosition <= durableLvRowCount;
                if (compatible) {
                    try {
                        return restorePinned(
                                pin,
                                entry.maxTimestamp,
                                entry.checkpointId,
                                expectedDefinitionTxn,
                                functions,
                                anchorWindow,
                                corruptCeilingMaxTs
                        );
                    } catch (CairoException e) {
                        if (e.getErrno() != CairoException.LV_CHECKPOINT_TIMELINE_INVALID) {
                            throw e;
                        }
                        // The selected root's data page is invalid. Fall through to walk
                        // to its predecessor; restorePinned validates before it mutates
                        // the runtime, so the failed candidate left it untouched (or the
                        // next restore re-clears it), and this pin still holds the same
                        // generation for the retry.
                    }
                } else if (isFloor) {
                    throw invalid("logical root is incompatible with durable materialization")
                            .put(" [createdLvSeqTxn=").put(entry.createdLvSeqTxn)
                            .put(", durableLvSeqTxn=").put(durableLvSeqTxn)
                            .put(", effectiveLvRowPosition=").put(effectiveLvRowPosition)
                            .put(", durableLvRowCount=").put(durableLvRowCount).put(']');
                }
                if (corruptCeilingMaxTs == Numbers.LONG_NULL) {
                    corruptCeilingMaxTs = entry.maxTimestamp;
                }
                if (++fallbacks > MAX_CORRUPT_ROOT_FALLBACKS
                        || !timelineReader.predecessor(pin.getTimelineRootRef(), entry.maxTimestamp, entry)) {
                    throw invalid("storage is corrupt: no usable root at or below the corruption ceiling")
                            .put(" [corruptCeilingMaxTs=").put(corruptCeilingMaxTs)
                            .put(", fallbacks=").put(fallbacks).put(']');
                }
                isFloor = false;
            }
        }
    }

    /**
     * Finds the newest logical boundary whose {@code maxTimestamp} is strictly
     * below {@code correctionTimestamp}, copies it into {@code out} and returns its
     * lifetime output row position. The strict inequality preserves a complete
     * timestamp tie: a boundary at exactly the correction covers only part of the
     * rows sitting there.
     * <p>
     * The position comes back with the boundary because a caller vetting an anchor
     * needs both, and reading them apart would need a second pin: the raw
     * {@link LiveViewCheckpointTimelineEntry#baseLvRowPosition} is only half the
     * figure, and the generation's row-position delta index supplies the rest.
     * <p>
     * Lookup runs under one generation pin, released before this returns. The
     * caller re-identifies the boundary by its composite key when it restores, so
     * a generation published in between is caught there rather than silently
     * mixed in here.
     *
     * @return the boundary's effective lifetime row position, or
     * {@link Numbers#LONG_NULL} when the current generation holds no boundary
     * below the correction - in which case {@code out} is left untouched
     */
    public long predecessorLvRowPosition(long correctionTimestamp, @NotNull LiveViewCheckpointTimelineEntry out) {
        ensureOpen();
        try (LiveViewCheckpointGenerationPin pin = metaStore.pin()) {
            if (!timelineReader.predecessor(pin.getTimelineRootRef(), correctionTimestamp, out)) {
                return Numbers.LONG_NULL;
            }
            return deltaReader.effectivePosition(pin.getRowPositionDeltaRootRef(), out);
        }
    }

    private static CairoException invalid(CharSequence reason) {
        return CairoException.critical(CairoException.LV_CHECKPOINT_TIMELINE_INVALID)
                .put("live view checkpoint ").put(reason);
    }

    private void ensureOpen() {
        if (!isOpen) {
            throw CairoException.critical(0).put("live view checkpoint timeline restore reader is not open");
        }
    }

    private boolean functionCatalogueContains(long segmentId) {
        int lo = 0;
        int hi = functionRoot.getSegmentUseCountSize();
        while (lo < hi) {
            final int mid = (lo + hi) >>> 1;
            if (functionRoot.getSegmentId(mid) < segmentId) {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        return lo < functionRoot.getSegmentUseCountSize() && functionRoot.getSegmentId(lo) == segmentId;
    }

    /**
     * Frames one encoded partition key as a bounded page reader, so a key
     * decoder cannot read past the reference the map entry published.
     */
    private LiveViewStatePageReader openKeyPage(@NotNull byte[] encodedKey) {
        keyMemory.jumpTo(0);
        for (int i = 0; i < encodedKey.length; i++) {
            keyMemory.putByte(encodedKey[i]);
        }
        return keyPageReader.of(keyMemory, 0, encodedKey.length);
    }

    private LiveViewCheckpointDataSegmentReader openStatePage(@NotNull LiveViewCheckpointStatePageRef ref) {
        final LiveViewCheckpointDataSegmentReader reader = readerFor(
                ref.getSegmentId(),
                validateStatePageSegment(ref)
        );
        if (ref.getStoredLength() != ref.getDecodedLength() || ref.getRowCount() != 1) {
            throw invalid("raw state page length or row count invalid");
        }
        reader.openPage(
                ref,
                LiveViewCheckpointTimelineStoreWriter.FUNCTION_STATE_PAGE_KIND,
                LiveViewCheckpointTimelineStoreWriter.RAW_CODEC,
                0,
                1,
                Integer.MAX_VALUE
        );
        reader.openStatePageReader(statePageReader);
        return reader;
    }

    private LiveViewCheckpointDataSegmentReader readerFor(long segmentId, long fileLength) {
        for (int i = 0; i < DATA_READER_CACHE_SIZE; i++) {
            if (dataSegmentIds[i] == segmentId) {
                return dataReaders[i];
            }
        }
        final int slot = dataReaderClock;
        dataReaderClock = dataReaderClock + 1 == DATA_READER_CACHE_SIZE ? 0 : dataReaderClock + 1;
        if (dataReaders[slot] == null) {
            dataReaders[slot] = new LiveViewCheckpointDataSegmentReader(configuration);
        }
        // Invalidate the slot BEFORE the open. of() closes and resets the reader up front and can
        // then throw, which would otherwise leave the slot still advertising the previous, healthy
        // segment id against a closed reader - so one corrupt segment poisons a healthy one, and a
        // later lookup can escalate that into "no usable root".
        dataSegmentIds[slot] = -1;
        dataReaders[slot].of(checkpointsDir, segmentId, fileLength);
        dataSegmentIds[slot] = segmentId;
        return dataReaders[slot];
    }

    private Result restorePinned(
            @NotNull LiveViewCheckpointGenerationPin pin,
            long maxTimestamp,
            long checkpointId,
            long expectedDefinitionTxn,
            @NotNull ObjList<WindowFunction> functions,
            @Nullable LiveViewWindow anchorWindow,
            long corruptCeilingMaxTs
    ) {
        final LiveViewCheckpointTimelineEntry entry = new LiveViewCheckpointTimelineEntry();
        if (!timelineReader.findExact(pin.getTimelineRootRef(), maxTimestamp, checkpointId, entry)) {
            throw invalid("logical root not found [maxTimestamp=").put(maxTimestamp)
                    .put(", checkpointId=").put(checkpointId).put(']');
        }
        // Read before anything else navigates the tree and overwrites it.
        final int lookupDepth = timelineReader.getLastLookupDepth();
        root.of(checkpointsDir, entry.rootRef);
        if (root.getCheckpointId() != checkpointId
                || root.getMaxTimestamp() != maxTimestamp
                || root.getDefinitionTxn() != expectedDefinitionTxn) {
            throw invalid("logical entry and checkpoint root identity mismatch");
        }
        final LiveViewCheckpointPageRef functionDirectoryRef = new LiveViewCheckpointPageRef();
        root.getFunctionDirectoryRef(functionDirectoryRef);
        functionDirectory.of(checkpointsDir, functionDirectoryRef);
        segmentDirectory.of(checkpointsDir, pin.getSegmentDirectoryRootRef());

        validateAnchor(anchorWindow);
        validateFunctions(functions);
        restoreFunctions(functions);
        restoreAnchor(anchorWindow);

        final long effectiveLvRowPosition = deltaReader.effectivePosition(
                pin.getRowPositionDeltaRootRef(),
                entry
        );
        return new Result(
                pin.getGeneration(),
                pin.getNormalizedBaseSeqTxn(),
                pin.getCoveredLvSeqTxn(),
                entry.createdLvSeqTxn,
                entry.maxTimestamp,
                entry.checkpointId,
                effectiveLvRowPosition,
                entry.logicalStateBytes,
                metaStore.getSuperblock().seedCursorOffset,
                lookupDepth,
                corruptCeilingMaxTs
        );
    }

    private void restoreAnchor(@Nullable LiveViewWindow anchorWindow) {
        if (anchorWindow == null) {
            return;
        }
        final LiveViewCheckpointPageRef anchorRootRef = new LiveViewCheckpointPageRef();
        root.getAnchorRootRef(anchorRootRef);
        anchorRoot.of(checkpointsDir, anchorRootRef);
        final LiveViewCheckpointPageRef anchorMapRootRef = new LiveViewCheckpointPageRef();
        anchorRoot.getPartitionMapRootRef(anchorMapRootRef);
        // validateAnchor already walked every entry, so the map cannot be
        // half-restored by a framing failure discovered mid-iteration.
        anchorWindow.beginCheckpointRestore();
        partitionReader.iterateAll(anchorMapRootRef, entry -> anchorWindow.restoreCheckpointEntry(
                openKeyPage(entry.getKey()),
                LiveViewCheckpointAnchorRoot.readAnchorValue(entry)
        ));
    }

    private void restoreFunction(WindowFunction function, LiveViewCheckpointPageRef functionRootRef) {
        functionRoot.of(checkpointsDir, functionRootRef);
        function.onCheckpointRestoreBegin();
        final LiveViewCheckpointStatePageRef scalarRef = new LiveViewCheckpointStatePageRef();
        functionRoot.getScalarStateRef(scalarRef);
        if (!scalarRef.isNull()) {
            final LiveViewCheckpointDataSegmentReader reader = openStatePage(scalarRef);
            final long consumed = function.restoreCheckpointState(statePageReader, 0, null);
            reader.assertFullyConsumed(scalarRef.getStoredLength(), consumed, 1);
            return;
        }
        final Map map = function.getPartitionMap();
        final boolean isRingShaped = function.supportsCheckpointRingState();
        final LiveViewCheckpointPageRef partitionRootRef = new LiveViewCheckpointPageRef();
        functionRoot.getPartitionMapRootRef(partitionRootRef);
        partitionReader.iterateAll(partitionRootRef, entry -> {
            final byte[] encodedKey = entry.getKey();
            final MapKey key = map.withKey();
            final long keyBytes = LiveViewSnapshotKeyCodec.readKey(
                    key,
                    openKeyPage(encodedKey),
                    0,
                    function.getCheckpointKeyColumnTypes()
            );
            if (keyBytes != encodedKey.length) {
                throw invalid("partition key decoder did not consume reference exactly");
            }
            final MapValue value = key.createValue();
            if (!value.isNew()) {
                throw invalid("function root contains a duplicate partition key");
            }
            if (isRingShaped) {
                ringStateReader.of(checkpointsDir, segmentDirectory, entry);
                function.restoreCheckpointRingState(ringStateReader, value);
                return;
            }
            final LiveViewCheckpointStatePageRef ref = entry.getStatePageRef(0);
            final LiveViewCheckpointDataSegmentReader reader = openStatePage(ref);
            final long consumed = function.restoreCheckpointState(statePageReader, 0, value);
            reader.assertFullyConsumed(ref.getStoredLength(), consumed, 1);
        });
    }

    private void restoreFunctions(@NotNull ObjList<WindowFunction> functions) {
        final LiveViewCheckpointPageRef functionRootRef = new LiveViewCheckpointPageRef();
        for (int i = 0, n = functions.size(); i < n; i++) {
            final WindowFunction function = functions.getQuick(i);
            if (!function.supportsCheckpointState()) {
                continue;
            }
            functionDirectory.find(function.checkpointFunctionIdentity().getEncoded(), functionRootRef);
            restoreFunction(function, functionRootRef);
        }
    }

    private boolean rootCatalogueContains(long segmentId) {
        int lo = 0;
        int hi = root.getSegmentIdCount();
        while (lo < hi) {
            final int mid = (lo + hi) >>> 1;
            if (root.getSegmentId(mid) < segmentId) {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        return lo < root.getSegmentIdCount() && root.getSegmentId(lo) == segmentId;
    }

    private void validateAnchor(@Nullable LiveViewWindow anchorWindow) {
        final LiveViewCheckpointPageRef anchorRootRef = new LiveViewCheckpointPageRef();
        root.getAnchorRootRef(anchorRootRef);
        if ((anchorWindow == null) != anchorRootRef.isNull()) {
            throw invalid("anchor presence does not match the compiled runtime");
        }
        if (anchorWindow == null) {
            return;
        }
        anchorRoot.of(checkpointsDir, anchorRootRef);
        if (!Arrays.equals(
                anchorWindow.getWindowName().getBytes(StandardCharsets.UTF_8),
                anchorRoot.getWindowName()
        )) {
            throw invalid("anchor window name does not match the compiled runtime");
        }
        if (anchorRoot.getAnchorValueType() != anchorWindow.getAnchorValueType()) {
            throw invalid("anchor value type does not match the compiled runtime");
        }
        if (!Arrays.equals(
                LiveViewCheckpointMetadata.encodeKeySchema(anchorWindow.getPartitionKeyTypes()),
                anchorRoot.getKeySchema()
        )) {
            throw invalid("anchor key schema does not match the compiled runtime");
        }
        final LiveViewCheckpointPageRef anchorMapRootRef = new LiveViewCheckpointPageRef();
        anchorRoot.getPartitionMapRootRef(anchorMapRootRef);
        partitionReader.iterateAll(anchorMapRootRef, entry -> {
            LiveViewCheckpointAnchorRoot.readAnchorValue(entry);
            anchorWindow.validateCheckpointEntry(openKeyPage(entry.getKey()));
        });
    }

    private void validateFunction(WindowFunction function, LiveViewCheckpointPageRef functionRootRef) {
        functionRoot.of(checkpointsDir, functionRootRef);
        final byte[] identity = function.checkpointFunctionIdentity().getEncoded();
        if (!Arrays.equals(identity, functionRoot.getFunctionIdentity())) {
            throw invalid("function directory and root identities differ");
        }
        if (!Arrays.equals(
                LiveViewCheckpointMetadata.encodeKeySchema(function.getCheckpointKeyColumnTypes()),
                functionRoot.getKeySchema()
        )) {
            throw invalid("function key schema does not match the compiled runtime");
        }
        // The identity above already carries the state layout version, so a root written
        // under a different layout cannot reach here. Checking the root's own copy keeps
        // the two agreeing: a root whose identity and version disagree is malformed, and
        // decoding it with the running layout would read foreign bytes.
        final int formatVersion = functionRoot.getStateFormatVersion();
        if (formatVersion != function.checkpointStateFormatVersion()) {
            throw invalid("function state format version does not match the compiled runtime, version=")
                    .put(formatVersion);
        }
        for (int i = 0, n = functionRoot.getSegmentUseCountSize(); i < n; i++) {
            final long segmentId = functionRoot.getSegmentId(i);
            if (!rootCatalogueContains(segmentId) || !segmentDirectory.find(segmentId, segmentDirectoryEntry)) {
                throw invalid("function data segment is absent from its parent root, segmentId=").put(segmentId);
            }
        }
        final LiveViewCheckpointStatePageRef scalarRef = new LiveViewCheckpointStatePageRef();
        functionRoot.getScalarStateRef(scalarRef);
        final Map map = function.getPartitionMap();
        if ((map == null) != !scalarRef.isNull()) {
            throw invalid("function scalar/partition shape does not match the compiled runtime");
        }
        if (!scalarRef.isNull()) {
            openStatePage(scalarRef);
            return;
        }
        final boolean isRingShaped = function.supportsCheckpointRingState();
        final LiveViewCheckpointPageRef partitionRootRef = new LiveViewCheckpointPageRef();
        functionRoot.getPartitionMapRootRef(partitionRootRef);
        partitionReader.iterateAll(partitionRootRef, entry -> {
            final byte[] encodedKey = entry.getKey();
            final long consumed = LiveViewSnapshotKeyCodec.validateKey(
                    openKeyPage(encodedKey),
                    0,
                    function.getCheckpointKeyColumnTypes()
            );
            if (consumed != encodedKey.length) {
                throw invalid("partition key decoder did not consume reference exactly");
            }
            if (isRingShaped) {
                // The chunk reader validates the entry's own scalar payload and
                // page references; what it cannot see is whether the segments
                // those references name belong to this root at all, so that is
                // checked here, exactly as openStatePage does for a whole image.
                ringStateReader.ofMetadata(entry);
                for (int i = 0, n = entry.getStatePageCount(); i < n; i++) {
                    validateStatePageSegment(entry.getStatePageRef(i));
                }
                return;
            }
            if (entry.getScalarState().length != 0 || entry.getStatePageCount() != 1) {
                throw invalid("function partition entry shape invalid");
            }
            openStatePage(entry.getStatePageRef(0));
        });
    }

    /**
     * Proves one data-segment reference is reachable from this root: named by the
     * root's own catalogue, by the function root's, and by the published segment
     * directory with a live reference count.
     *
     * @return the segment's published file length, the bound every page read of it
     * is checked against
     */
    private long validateStatePageSegment(@NotNull LiveViewCheckpointStatePageRef ref) {
        if (!rootCatalogueContains(ref.getSegmentId()) || !functionCatalogueContains(ref.getSegmentId())) {
            throw invalid("state page segment is absent from its root catalogue, segmentId=")
                    .put(ref.getSegmentId());
        }
        if (!segmentDirectory.find(ref.getSegmentId(), segmentDirectoryEntry)
                || segmentDirectoryEntry.referenceCount <= 0) {
            throw invalid("state page segment is absent from the published directory, segmentId=")
                    .put(ref.getSegmentId());
        }
        return segmentDirectoryEntry.fileLength;
    }

    private void validateFunctions(@NotNull ObjList<WindowFunction> functions) {
        int capableCount = 0;
        final LiveViewCheckpointPageRef functionRootRef = new LiveViewCheckpointPageRef();
        for (int i = 0, n = functions.size(); i < n; i++) {
            final WindowFunction function = functions.getQuick(i);
            if (!function.supportsCheckpointState()) {
                continue;
            }
            capableCount++;
            if (function.checkpointFunctionIdentity() == null || function.checkpointDependency() == null) {
                throw invalid("checkpoint-capable function has no compiler metadata");
            }
            if (!functionDirectory.find(function.checkpointFunctionIdentity().getEncoded(), functionRootRef)) {
                throw invalid("checkpoint root is missing a compiled function");
            }
            validateFunction(function, functionRootRef);
        }
        if (capableCount != functionDirectory.size()) {
            throw invalid("checkpoint function count does not match the compiled runtime");
        }
    }

    public static final class Result {
        public final long checkpointId;
        /**
         * When {@link #restoreLatestCompatible} fell back past one or more corrupt
         * selected roots to restore a safe predecessor, the {@code maxTimestamp} of
         * the highest corrupt root it skipped; {@link Numbers#LONG_NULL} when the
         * floor root restored cleanly with no fallback. Together with
         * {@link #maxTimestamp} (the restored predecessor's) it bounds the range the
         * caller reconstructs: every logical boundary in
         * {@code (maxTimestamp, corruptCeilingMaxTs]}.
         */
        public final long corruptCeilingMaxTs;
        public final long coveredLvSeqTxn;
        public final long createdLvSeqTxn;
        public final long effectiveLvRowPosition;
        public final long generation;
        public final long logicalStateBytes;
        /**
         * Metadata pages the point lookup that found this root decoded, which is
         * the timeline tree's height.
         */
        public final int lookupDepth;
        public final long maxTimestamp;
        public final long normalizedBaseSeqTxn;
        /**
         * The seed sweep's base-cursor row offset this generation was published
         * at, or {@link Numbers#LONG_NULL} when a steady seal or a repair
         * published it. See {@link LiveViewCheckpointSuperblock#seedCursorOffset}.
         */
        public final long seedCursorOffset;

        private Result(
                long generation,
                long normalizedBaseSeqTxn,
                long coveredLvSeqTxn,
                long createdLvSeqTxn,
                long maxTimestamp,
                long checkpointId,
                long effectiveLvRowPosition,
                long logicalStateBytes,
                long seedCursorOffset,
                int lookupDepth,
                long corruptCeilingMaxTs
        ) {
            this.generation = generation;
            this.normalizedBaseSeqTxn = normalizedBaseSeqTxn;
            this.coveredLvSeqTxn = coveredLvSeqTxn;
            this.createdLvSeqTxn = createdLvSeqTxn;
            this.maxTimestamp = maxTimestamp;
            this.checkpointId = checkpointId;
            this.effectiveLvRowPosition = effectiveLvRowPosition;
            this.logicalStateBytes = logicalStateBytes;
            this.seedCursorOffset = seedCursorOffset;
            this.lookupDepth = lookupDepth;
            this.corruptCeilingMaxTs = corruptCeilingMaxTs;
        }
    }
}
