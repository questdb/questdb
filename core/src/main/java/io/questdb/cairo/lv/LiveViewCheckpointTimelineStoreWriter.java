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
import io.questdb.cairo.ColumnTypes;
import io.questdb.cairo.map.Map;
import io.questdb.cairo.map.MapRecord;
import io.questdb.cairo.map.MapRecordCursor;
import io.questdb.cairo.map.MapValue;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryA;
import io.questdb.cairo.vm.api.MemoryCARW;
import io.questdb.griffin.engine.window.WindowFunction;
import io.questdb.std.LongList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.Transient;
import io.questdb.std.str.Path;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

import java.io.Closeable;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;

/**
 * Composes the immutable page/root stores into one crash-ordered checkpoint
 * publication. Two publications exist, and they differ only in which logical
 * entries they touch:
 * <ul>
 *     <li>{@link #append} is the strictly in-order cadence seal. It adds one
 *     logical boundary above the current head and reuses every older root.</li>
 *     <li>{@link #beginRepair} plus {@link #publishRepair} is the localized
 *     out-of-order range splice. It re-versions the roots a repair replayed
 *     over - same {@code checkpointId}, new state and position - while the
 *     prefix below the correction floor and the converged suffix at or above
 *     {@code H} keep their existing payload roots, and one persistent
 *     row-position range-add corrects the suffix's cumulative recovery
 *     coordinate without walking it.</li>
 * </ul>
 * Both end in the same commit point: the inactive superblock slot, published
 * last, carrying the generation watermark that declares prefix, repaired
 * interval and suffix all valid against one pinned base snapshot.
 */
public class LiveViewCheckpointTimelineStoreWriter implements Closeable {

    public static final int FUNCTION_STATE_PAGE_KIND = 0x41;
    public static final int RAW_CODEC = 0;
    @TestOnly
    public static final int TEST_FAIL_AFTER_DATA_PUBLISH = 1;
    @TestOnly
    public static final int TEST_FAIL_AFTER_METADATA_PUBLISH = 2;

    private final HashSet<String> lifecycleReconciledDirs = new HashSet<>();
    private final CairoConfiguration configuration;
    private final MemoryCARW keyBuffer;
    @TestOnly
    private int testFailureStage;

    public LiveViewCheckpointTimelineStoreWriter(@NotNull CairoConfiguration configuration) {
        this.configuration = configuration;
        this.keyBuffer = Vm.getCARWInstance(4096, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT);
    }

    public Result append(
            @Transient @NotNull Path checkpointsDir,
            @NotNull ObjList<WindowFunction> functions,
            @Nullable LiveViewWindow anchorWindow,
            long definitionTxn,
            long createdLvSeqTxn,
            long normalizedBaseSeqTxn,
            long coveredLvSeqTxn,
            long historyEpoch,
            boolean primaryOwner,
            long maxTimestamp,
            long effectiveLvRowPosition
    ) {
        if (!primaryOwner) {
            throw CairoException.critical(0).put("replica must not publish a live view checkpoint timeline");
        }
        final String lifecycleKey = checkpointsDir.toString();
        boolean epochRetry = false;
        while (true) {
            long orphanUpperBound = 0;
            if (!lifecycleReconciledDirs.contains(lifecycleKey)) {
                final LiveViewCheckpointLifecycle.ReconcileResult reconciliation =
                        LiveViewCheckpointLifecycle.reconcile(
                                configuration,
                                checkpointsDir,
                                definitionTxn,
                                historyEpoch,
                                true
                        );
                orphanUpperBound = reconciliation.getFinalOrphanUpperBound();
                if (reconciliation.getFailedOrphanCount() == 0
                        && reconciliation.getFailedPurgeCount() == 0
                        && reconciliation.getFailedRepairCount() == 0) {
                    lifecycleReconciledDirs.add(lifecycleKey);
                }
            }
            try {
                return append0(
                        checkpointsDir,
                        functions,
                        anchorWindow,
                        definitionTxn,
                        createdLvSeqTxn,
                        normalizedBaseSeqTxn,
                        coveredLvSeqTxn,
                        historyEpoch,
                        maxTimestamp,
                        effectiveLvRowPosition,
                        orphanUpperBound
                );
            } catch (HistoryEpochChangedException e) {
                lifecycleReconciledDirs.remove(lifecycleKey);
                if (epochRetry) {
                    throw CairoException.critical(0).put("could not replace live view checkpoint history epoch");
                }
                epochRetry = true;
            } catch (RuntimeException | Error e) {
                lifecycleReconciledDirs.remove(lifecycleKey);
                throw e;
            }
        }
    }

    /**
     * Opens a repair capture against the current published generation. The
     * returned capture owns one data segment into which the replay freezes the
     * state of every logical boundary it crosses; nothing it writes is reachable
     * until {@link #publishRepair} commits the superblock, so a discarded capture
     * is a temporary file and nothing else.
     * <p>
     * The capture pins the generation it was opened against. A normal cadence
     * seal (or another repair) publishing in between invalidates every old root
     * reference the capture holds, so {@link #publishRepair} refuses rather than
     * splicing stale references into a newer tree.
     */
    public RepairCapture beginRepair(@Transient @NotNull Path checkpointsDir) {
        ensureDirectories(checkpointsDir);
        try (LiveViewCheckpointMetaStore metaStore = new LiveViewCheckpointMetaStore(configuration)) {
            metaStore.of(checkpointsDir);
            if (!metaStore.isValid()) {
                throw CairoException.critical(0)
                        .put("cannot repair a live view checkpoint timeline with no valid generation");
            }
            final LiveViewCheckpointSuperblock superblock = metaStore.getSuperblock();
            return new RepairCapture(
                    checkpointsDir,
                    skipPublishedSegmentIds(checkpointsDir, superblock.nextSegmentId),
                    superblock.generation,
                    superblock.timelineRootRef
            );
        }
    }

    @Override
    public void close() {
        Misc.free(keyBuffer);
        lifecycleReconciledDirs.clear();
    }

    /**
     * Publishes one localized out-of-order repair as a timeline range splice.
     * <p>
     * The splice preserves every logical key: the captured boundaries keep their
     * {@code checkpointId}, {@code maxTimestamp} and {@code createdLvSeqTxn} and
     * receive a new root version plus the replay-derived position, while the
     * prefix and the converged suffix keep their existing payload roots by page
     * reference. {@code suffixRowDelta} is the replacement's total output-row
     * count change; it lands as one difference-array point add at the first
     * logical key at or above {@code highTsExclusive}, so every suffix root
     * reports a corrected cumulative {@code lvRowPosition} without the splice
     * walking or rewriting it.
     * <p>
     * A repair that crossed no logical boundary and moved no row still publishes:
     * the new generation watermark is what declares the whole reused timeline
     * valid against the repair's pinned base snapshot.
     *
     * @param capture              boundaries frozen by the replay, ascending by key
     * @param definitionTxn        live-view definition identity
     * @param normalizedBaseSeqTxn {@code E}, the pinned base snapshot the whole
     *                             timeline is now valid against
     * @param coveredLvSeqTxn      live-view writer {@code seqTxn} the replacement
     *                             reached
     * @param historyEpoch         current history epoch
     * @param primaryOwner         false for a read-only replica, which must not publish
     * @param highTsExclusive      {@code H}, the exclusive convergence boundary the
     *                             suffix starts at
     * @param suffixRowDelta       output rows the replacement added (negative when it
     *                             removed rows)
     */
    public RepairResult publishRepair(
            @NotNull RepairCapture capture,
            long definitionTxn,
            long normalizedBaseSeqTxn,
            long coveredLvSeqTxn,
            long historyEpoch,
            boolean primaryOwner,
            long highTsExclusive,
            long suffixRowDelta
    ) {
        if (!primaryOwner) {
            throw CairoException.critical(0).put("replica must not publish a live view checkpoint timeline");
        }
        capture.validateAgainst(highTsExclusive);
        final Path checkpointsDir = capture.checkpointsDir;
        final int boundaryCount = capture.size();

        try (
                LiveViewCheckpointMetaStore metaStore = new LiveViewCheckpointMetaStore(configuration);
                LiveViewCheckpointTimelineReader timelineReader = new LiveViewCheckpointTimelineReader(configuration);
                LiveViewCheckpointRowPositionDeltaReader deltaReader = new LiveViewCheckpointRowPositionDeltaReader(configuration);
                LiveViewCheckpointRoot oldCheckpointRoot = new LiveViewCheckpointRoot(configuration);
                LiveViewCheckpointFunctionDirectory oldFunctionDirectory = new LiveViewCheckpointFunctionDirectory(configuration);
                LiveViewCheckpointSegmentDirectory segmentDirectory = new LiveViewCheckpointSegmentDirectory(configuration);
                LiveViewCheckpointMetaSegmentWriter directoryWriter = new LiveViewCheckpointMetaSegmentWriter(configuration);
                RootBuilders roots = new RootBuilders();
                LiveViewCheckpointTimelineWriter timelineWriter = new LiveViewCheckpointTimelineWriter(configuration);
                LiveViewCheckpointRowPositionDeltaWriter deltaWriter = new LiveViewCheckpointRowPositionDeltaWriter(configuration)
        ) {
            metaStore.of(checkpointsDir);
            timelineReader.of(checkpointsDir);
            deltaReader.of(checkpointsDir);
            timelineWriter.of(checkpointsDir);
            deltaWriter.of(checkpointsDir);

            if (!metaStore.isValid()) {
                throw CairoException.critical(0)
                        .put("cannot repair a live view checkpoint timeline with no valid generation");
            }
            final LiveViewCheckpointSuperblock superblock = metaStore.getSuperblock();
            if (superblock.definitionTxn != definitionTxn || superblock.historyEpoch != historyEpoch) {
                throw CairoException.critical(CairoException.LV_CHECKPOINT_TIMELINE_INVALID)
                        .put("live view checkpoint repair definition identity mismatch");
            }
            if (superblock.generation != capture.generation) {
                throw CairoException.critical(0)
                        .put("live view checkpoint timeline moved under the repair capture")
                        .put(" [captured=").put(capture.generation)
                        .put(", current=").put(superblock.generation).put(']');
            }
            if (normalizedBaseSeqTxn < superblock.normalizedBaseSeqTxn
                    || coveredLvSeqTxn < superblock.coveredLvSeqTxn) {
                throw CairoException.critical(0)
                        .put("live view checkpoint generation watermarks must not move backwards")
                        .put(" [storedBase=").put(superblock.normalizedBaseSeqTxn)
                        .put(", nextBase=").put(normalizedBaseSeqTxn)
                        .put(", storedLv=").put(superblock.coveredLvSeqTxn)
                        .put(", nextLv=").put(coveredLvSeqTxn).put(']');
            }

            final long generation = checkedIncrement(superblock.generation, "generation");
            final LiveViewCheckpointPageRef oldTimelineRoot = copy(superblock.timelineRootRef);
            final LiveViewCheckpointPageRef oldDeltaRoot = copy(superblock.rowPositionDeltaRootRef);
            final LiveViewCheckpointPageRef oldDirectoryRoot = copy(superblock.segmentDirectoryRootRef);
            segmentDirectory.of(checkpointsDir, oldDirectoryRoot);

            // The data segment reaches its final name before any metadata can
            // reference it, exactly as the cadence seal orders it. An empty
            // capture publishes no data at all - only the suffix correction and
            // the generation watermark move.
            final long dataSegmentBytes = boundaryCount > 0 ? capture.commitData() : 0;
            if (testFailureStage == TEST_FAIL_AFTER_DATA_PUBLISH) {
                throw CairoException.critical(0).put("test failure after live view checkpoint data publication");
            }

            long nextSegmentId = boundaryCount > 0
                    ? Math.max(superblock.nextSegmentId, capture.dataSegmentId + 1)
                    : superblock.nextSegmentId;
            roots.of(checkpointsDir, nextSegmentId);
            final LiveViewCheckpointTimelineEntry[] newEntries = new LiveViewCheckpointTimelineEntry[boundaryCount];
            final LongList removedSegmentIds = new LongList();
            final LongList addedSegmentIds = new LongList();
            final LiveViewCheckpointPageRef oldAnchorRootRef = new LiveViewCheckpointPageRef();
            final LiveViewCheckpointPageRef oldFunctionDirectoryRef = new LiveViewCheckpointPageRef();
            final LiveViewCheckpointPageRef newRootRef = new LiveViewCheckpointPageRef();
            for (int i = 0; i < boundaryCount; i++) {
                final FrozenBoundary boundary = capture.boundaries.getQuick(i);
                final LiveViewCheckpointTimelineEntry oldEntry = boundary.oldEntry;
                oldCheckpointRoot.of(checkpointsDir, oldEntry.rootRef);
                if (oldCheckpointRoot.getCheckpointId() != oldEntry.checkpointId
                        || oldCheckpointRoot.getMaxTimestamp() != oldEntry.maxTimestamp
                        || oldCheckpointRoot.getDefinitionTxn() != definitionTxn) {
                    throw CairoException.critical(CairoException.LV_CHECKPOINT_TIMELINE_INVALID)
                            .put("live view checkpoint repair root identity mismatch [checkpointId=")
                            .put(oldEntry.checkpointId).put(']');
                }
                oldCheckpointRoot.getAnchorRootRef(oldAnchorRootRef);
                oldCheckpointRoot.getFunctionDirectoryRef(oldFunctionDirectoryRef);
                oldFunctionDirectory.of(checkpointsDir, oldFunctionDirectoryRef);
                roots.buildRoot(
                        boundary,
                        oldAnchorRootRef,
                        oldFunctionDirectory,
                        oldEntry.checkpointId,
                        oldEntry.maxTimestamp,
                        definitionTxn,
                        newRootRef,
                        addedSegmentIds
                );
                // The old root released every data segment it referenced and the
                // new one takes its own; a segment no current root names any more
                // retires at this generation and the purge job unlinks it once no
                // reader can reach it. Applied per root because repeated
                // references inside one root count once per side.
                removedSegmentIds.clear();
                for (int s = 0, n = oldCheckpointRoot.getSegmentIdCount(); s < n; s++) {
                    removedSegmentIds.add(oldCheckpointRoot.getSegmentId(s));
                }
                dropSegmentId(addedSegmentIds, capture.dataSegmentId);
                segmentDirectory.applyRootReferenceChanges(removedSegmentIds, addedSegmentIds, generation);

                final long prefixCorrection = deltaReader.prefixSum(
                        oldDeltaRoot,
                        oldEntry.maxTimestamp,
                        oldEntry.checkpointId
                );
                final long baseLvRowPosition;
                try {
                    baseLvRowPosition = Math.subtractExact(boundary.effectiveLvRowPosition, prefixCorrection);
                } catch (ArithmeticException e) {
                    throw CairoException.critical(0).put("live view checkpoint row position overflow");
                }
                final LiveViewCheckpointTimelineEntry newEntry = new LiveViewCheckpointTimelineEntry().of(
                        oldEntry.maxTimestamp,
                        oldEntry.checkpointId,
                        oldEntry.createdLvSeqTxn,
                        baseLvRowPosition,
                        boundary.logicalStateBytes
                );
                newEntry.rootRef.of(newRootRef.getSegmentId(), newRootRef.getOffset(), newRootRef.getLength());
                newEntries[i] = newEntry;
            }
            nextSegmentId = roots.nextSegmentId;
            long metadataBytesAdded = roots.metadataBytesAdded;
            if (boundaryCount > 0) {
                segmentDirectory.addSegment(capture.dataSegmentId, dataSegmentBytes, boundaryCount);
            }

            final LiveViewCheckpointPageRef newTimelineRoot = new LiveViewCheckpointPageRef();
            if (boundaryCount > 0) {
                nextSegmentId = skipPublishedSegmentIds(checkpointsDir, nextSegmentId);
                timelineWriter.splice(oldTimelineRoot, newEntries, boundaryCount, nextSegmentId++, newTimelineRoot);
                metadataBytesAdded = checkedAdd(metadataBytesAdded, timelineWriter.getLastSegmentBytes());
            } else {
                copy(oldTimelineRoot, newTimelineRoot);
            }

            // The breakpoint is resolved against the OLD tree on purpose: a splice
            // preserves every key, so the first suffix key is the same in both, and
            // reading it here needs no page from the segment just written.
            final LiveViewCheckpointPageRef newDeltaRoot = new LiveViewCheckpointPageRef();
            copy(oldDeltaRoot, newDeltaRoot);
            long suffixBreakpointTimestamp = Numbers.LONG_NULL;
            if (suffixRowDelta != 0) {
                final LiveViewCheckpointTimelineEntry suffixEntry = new LiveViewCheckpointTimelineEntry();
                if (timelineReader.successor(oldTimelineRoot, highTsExclusive, suffixEntry)) {
                    nextSegmentId = skipPublishedSegmentIds(checkpointsDir, nextSegmentId);
                    deltaWriter.suffixAdd(
                            oldDeltaRoot,
                            suffixEntry.maxTimestamp,
                            suffixEntry.checkpointId,
                            suffixRowDelta,
                            nextSegmentId++,
                            newDeltaRoot
                    );
                    metadataBytesAdded = checkedAdd(metadataBytesAdded, deltaWriter.getLastSegmentBytes());
                    suffixBreakpointTimestamp = suffixEntry.maxTimestamp;
                }
            }

            nextSegmentId = skipPublishedSegmentIds(checkpointsDir, nextSegmentId);
            directoryWriter.of(checkpointsDir, nextSegmentId++);
            final LiveViewCheckpointPageRef newDirectoryRoot = new LiveViewCheckpointPageRef();
            segmentDirectory.writeTo(directoryWriter, newDirectoryRoot);
            metadataBytesAdded = checkedAdd(metadataBytesAdded, directoryWriter.commit());
            if (testFailureStage == TEST_FAIL_AFTER_METADATA_PUBLISH) {
                throw CairoException.critical(0).put("test failure after live view checkpoint metadata publication");
            }

            superblock.generation = generation;
            superblock.normalizedBaseSeqTxn = normalizedBaseSeqTxn;
            superblock.coveredLvSeqTxn = coveredLvSeqTxn;
            superblock.nextSegmentId = nextSegmentId;
            superblock.metadataBytes = checkedAdd(superblock.metadataBytes, metadataBytesAdded);
            superblock.dataBytes = checkedAdd(superblock.dataBytes, dataSegmentBytes);
            copy(newTimelineRoot, superblock.timelineRootRef);
            copy(newDeltaRoot, superblock.rowPositionDeltaRootRef);
            copy(newDirectoryRoot, superblock.segmentDirectoryRootRef);
            metaStore.publish();

            return new RepairResult(
                    generation,
                    boundaryCount,
                    suffixRowDelta,
                    suffixBreakpointTimestamp,
                    dataSegmentBytes,
                    metadataBytesAdded,
                    metaStore.getWalPurgeFloor()
            );
        }
    }

    @TestOnly
    public void setTestFailureStage(int testFailureStage) {
        this.testFailureStage = testFailureStage;
    }

    private Result append0(
            @Transient @NotNull Path checkpointsDir,
            @NotNull ObjList<WindowFunction> functions,
            @Nullable LiveViewWindow anchorWindow,
            long definitionTxn,
            long createdLvSeqTxn,
            long normalizedBaseSeqTxn,
            long coveredLvSeqTxn,
            long historyEpoch,
            long maxTimestamp,
            long effectiveLvRowPosition,
            long orphanUpperBound
    ) {
        if (definitionTxn < 0
                || createdLvSeqTxn < 0
                || historyEpoch < 0
                || normalizedBaseSeqTxn < 0
                || coveredLvSeqTxn < 0
                || effectiveLvRowPosition < 0
                || createdLvSeqTxn > coveredLvSeqTxn) {
            throw CairoException.critical(0).put("invalid live view normal checkpoint coordinates");
        }
        ensureDirectories(checkpointsDir);

        try (
                LiveViewCheckpointMetaStore metaStore = new LiveViewCheckpointMetaStore(configuration);
                LiveViewCheckpointTimelineReader timelineReader = new LiveViewCheckpointTimelineReader(configuration);
                LiveViewCheckpointRowPositionDeltaReader deltaReader = new LiveViewCheckpointRowPositionDeltaReader(configuration);
                LiveViewCheckpointRoot oldCheckpointRoot = new LiveViewCheckpointRoot(configuration);
                LiveViewCheckpointFunctionDirectory oldFunctionDirectory = new LiveViewCheckpointFunctionDirectory(configuration);
                LiveViewCheckpointSegmentDirectory segmentDirectory = new LiveViewCheckpointSegmentDirectory(configuration);
                LiveViewCheckpointDataSegmentWriter dataWriter = new LiveViewCheckpointDataSegmentWriter(configuration);
                LiveViewCheckpointMetaSegmentWriter directoryWriter = new LiveViewCheckpointMetaSegmentWriter(configuration);
                RootBuilders roots = new RootBuilders();
                LiveViewCheckpointTimelineWriter timelineWriter = new LiveViewCheckpointTimelineWriter(configuration)
        ) {
            metaStore.of(checkpointsDir);
            timelineReader.of(checkpointsDir);
            deltaReader.of(checkpointsDir);
            timelineWriter.of(checkpointsDir);

            final LiveViewCheckpointSuperblock superblock = metaStore.getSuperblock();
            final long protectedSegmentIdCeiling = metaStore.isValid() ? superblock.getNextSegmentIdCeiling() : 0;
            if (metaStore.isValid() && (superblock.definitionTxn != definitionTxn || superblock.historyEpoch != historyEpoch)) {
                throw HistoryEpochChangedException.INSTANCE;
            }
            if (metaStore.isValid()
                    && (normalizedBaseSeqTxn < superblock.normalizedBaseSeqTxn
                    || coveredLvSeqTxn < superblock.coveredLvSeqTxn)) {
                throw CairoException.critical(0)
                        .put("live view checkpoint generation watermarks must not move backwards")
                        .put(" [storedBase=").put(superblock.normalizedBaseSeqTxn)
                        .put(", nextBase=").put(normalizedBaseSeqTxn)
                        .put(", storedLv=").put(superblock.coveredLvSeqTxn)
                        .put(", nextLv=").put(coveredLvSeqTxn).put(']');
            }

            final long generation = metaStore.isValid()
                    ? checkedIncrement(superblock.generation, "generation")
                    : 1;
            final long checkpointId = metaStore.isValid() ? superblock.nextCheckpointId : 0;
            checkedIncrement(checkpointId, "checkpoint id");

            final LiveViewCheckpointPageRef oldTimelineRoot = copy(superblock.timelineRootRef);
            final LiveViewCheckpointPageRef oldDeltaRoot = copy(superblock.rowPositionDeltaRootRef);
            final LiveViewCheckpointPageRef oldDirectoryRoot = copy(superblock.segmentDirectoryRootRef);
            final LiveViewCheckpointTimelineEntry previousEntry = new LiveViewCheckpointTimelineEntry();
            final boolean hasPrevious = timelineReader.last(oldTimelineRoot, previousEntry);
            if (hasPrevious && maxTimestamp <= previousEntry.maxTimestamp) {
                throw CairoException.critical(0)
                        .put("normal live view checkpoint boundary overlaps current head")
                        .put(" [head=").put(previousEntry.maxTimestamp)
                        .put(", candidate=").put(maxTimestamp).put(']');
            }

            final LiveViewCheckpointPageRef oldAnchorRootRef = new LiveViewCheckpointPageRef();
            final LiveViewCheckpointPageRef oldFunctionDirectoryRef = new LiveViewCheckpointPageRef();
            if (hasPrevious) {
                oldCheckpointRoot.of(checkpointsDir, previousEntry.rootRef);
                if (oldCheckpointRoot.getDefinitionTxn() != definitionTxn) {
                    throw CairoException.critical(CairoException.LV_CHECKPOINT_TIMELINE_INVALID)
                            .put("live view checkpoint root definition identity mismatch");
                }
                oldCheckpointRoot.getAnchorRootRef(oldAnchorRootRef);
                oldCheckpointRoot.getFunctionDirectoryRef(oldFunctionDirectoryRef);
                oldFunctionDirectory.of(checkpointsDir, oldFunctionDirectoryRef);
            }
            segmentDirectory.of(checkpointsDir, oldDirectoryRoot);

            long nextSegmentId = metaStore.isValid() ? superblock.nextSegmentId : 0;
            nextSegmentId = Math.max(nextSegmentId, orphanUpperBound);
            nextSegmentId = skipPublishedSegmentIds(checkpointsDir, nextSegmentId);
            final long dataSegmentId = nextSegmentId++;
            dataWriter.of(checkpointsDir, dataSegmentId);

            final FrozenBoundary boundary = freezeBoundary(dataWriter, functions, anchorWindow);
            final long dataSegmentBytes = dataWriter.commit();
            if (testFailureStage == TEST_FAIL_AFTER_DATA_PUBLISH) {
                throw CairoException.critical(0).put("test failure after live view checkpoint data publication");
            }

            roots.of(checkpointsDir, nextSegmentId);
            final LiveViewCheckpointPageRef checkpointRootRef = new LiveViewCheckpointPageRef();
            final LongList reusedSegmentIds = new LongList();
            roots.buildRoot(
                    boundary,
                    oldAnchorRootRef,
                    hasPrevious ? oldFunctionDirectory : null,
                    checkpointId,
                    maxTimestamp,
                    definitionTxn,
                    checkpointRootRef,
                    reusedSegmentIds
            );
            nextSegmentId = roots.nextSegmentId;
            long metadataBytesAdded = roots.metadataBytesAdded;

            final long prefixCorrection = deltaReader.prefixSum(oldDeltaRoot, maxTimestamp, checkpointId);
            final long baseLvRowPosition;
            try {
                baseLvRowPosition = Math.subtractExact(effectiveLvRowPosition, prefixCorrection);
            } catch (ArithmeticException e) {
                throw CairoException.critical(0).put("live view checkpoint row position overflow");
            }
            final LiveViewCheckpointTimelineEntry entry = new LiveViewCheckpointTimelineEntry()
                    .of(maxTimestamp, checkpointId, createdLvSeqTxn, baseLvRowPosition, boundary.logicalStateBytes);
            entry.rootRef.of(
                    checkpointRootRef.getSegmentId(),
                    checkpointRootRef.getOffset(),
                    checkpointRootRef.getLength()
            );
            nextSegmentId = skipPublishedSegmentIds(checkpointsDir, nextSegmentId);
            final LiveViewCheckpointPageRef newTimelineRoot = new LiveViewCheckpointPageRef();
            timelineWriter.append(oldTimelineRoot, entry, nextSegmentId++, newTimelineRoot);
            metadataBytesAdded = checkedAdd(metadataBytesAdded, timelineWriter.getLastSegmentBytes());

            segmentDirectory.addSegment(dataSegmentId, dataSegmentBytes, 1);
            dropSegmentId(reusedSegmentIds, dataSegmentId);
            if (reusedSegmentIds.size() > 0) {
                segmentDirectory.applyRootReferenceChanges(new LongList(), reusedSegmentIds, generation);
            }
            nextSegmentId = skipPublishedSegmentIds(checkpointsDir, nextSegmentId);
            directoryWriter.of(checkpointsDir, nextSegmentId++);
            final LiveViewCheckpointPageRef newDirectoryRoot = new LiveViewCheckpointPageRef();
            segmentDirectory.writeTo(directoryWriter, newDirectoryRoot);
            metadataBytesAdded = checkedAdd(metadataBytesAdded, directoryWriter.commit());
            if (testFailureStage == TEST_FAIL_AFTER_METADATA_PUBLISH) {
                throw CairoException.critical(0).put("test failure after live view checkpoint metadata publication");
            }

            superblock.generation = generation;
            superblock.definitionTxn = definitionTxn;
            superblock.historyEpoch = historyEpoch;
            superblock.normalizedBaseSeqTxn = normalizedBaseSeqTxn;
            superblock.coveredLvSeqTxn = coveredLvSeqTxn;
            superblock.nextCheckpointId = checkedIncrement(checkpointId, "checkpoint id");
            superblock.nextSegmentId = nextSegmentId;
            superblock.metadataBytes = checkedAdd(metaStore.isValid() ? superblock.metadataBytes : 0, metadataBytesAdded);
            superblock.dataBytes = checkedAdd(metaStore.isValid() ? superblock.dataBytes : 0, dataSegmentBytes);
            copy(newTimelineRoot, superblock.timelineRootRef);
            copy(oldDeltaRoot, superblock.rowPositionDeltaRootRef);
            copy(newDirectoryRoot, superblock.segmentDirectoryRootRef);
            metaStore.publish();

            LiveViewCheckpointLifecycle.purgeFinalOrphans(
                    configuration,
                    checkpointsDir,
                    protectedSegmentIdCeiling,
                    orphanUpperBound,
                    true
            );
            return new Result(
                    generation,
                    checkpointId,
                    boundary.logicalStateBytes,
                    dataSegmentBytes,
                    metadataBytesAdded,
                    metaStore.getWalPurgeFloor()
            );
        }
    }

    private static long checkedAdd(long a, long b) {
        try {
            return Math.addExact(a, b);
        } catch (ArithmeticException e) {
            throw CairoException.critical(0).put("live view checkpoint byte count overflow");
        }
    }

    private static int checkedIntLength(long value, CharSequence what) {
        if (value <= 0 || value > Integer.MAX_VALUE) {
            throw CairoException.critical(0).put("live view checkpoint ").put(what)
                    .put(" length out of bounds, bytes=").put(value);
        }
        return (int) value;
    }

    private static long checkedIncrement(long value, CharSequence what) {
        if (value == Long.MAX_VALUE) {
            throw CairoException.critical(0).put("live view checkpoint ").put(what).put(" exhausted");
        }
        return value + 1;
    }

    private static LiveViewCheckpointPageRef copy(LiveViewCheckpointPageRef from) {
        return new LiveViewCheckpointPageRef().of(from.getSegmentId(), from.getOffset(), from.getLength());
    }

    private static void copy(LiveViewCheckpointPageRef from, LiveViewCheckpointPageRef to) {
        to.of(from.getSegmentId(), from.getOffset(), from.getLength());
    }

    /**
     * Removes {@code segmentId} from a root's referenced-segment list. The
     * segment a publication introduces is registered with its own root count, so
     * it must not also be counted through the per-root reference changes.
     */
    private static void dropSegmentId(LongList segmentIds, long segmentId) {
        for (int i = 0, n = segmentIds.size(); i < n; i++) {
            if (segmentIds.getQuick(i) == segmentId) {
                segmentIds.removeIndex(i);
                return;
            }
        }
    }

    private static void removeMissingPartitions(
            Path checkpointsDir,
            LiveViewCheckpointPageRef oldFunctionRootRef,
            LiveViewCheckpointFunctionRoot oldFunctionRoot,
            LiveViewCheckpointPartitionMapReader oldPartitionReader,
            FrozenFunction frozen,
            LiveViewCheckpointFunctionRootBuilder builder
    ) {
        if (oldFunctionRootRef.isNull()) {
            return;
        }
        oldFunctionRoot.of(checkpointsDir, oldFunctionRootRef);
        final LiveViewCheckpointPageRef oldPartitionRoot = new LiveViewCheckpointPageRef();
        oldFunctionRoot.getPartitionMapRootRef(oldPartitionRoot);
        oldPartitionReader.iterateAll(oldPartitionRoot, entry -> {
            if (!frozen.currentKeys.contains(ByteBuffer.wrap(entry.getKey()))) {
                builder.removePartition(entry.getKey());
            }
        });
    }

    private void ensureDirectories(Path checkpointsDir) {
        try (Path path = new Path()) {
            LiveViewCheckpointLayout.metaDirPath(path, checkpointsDir).slash();
            if (configuration.getFilesFacade().mkdirs(path, configuration.getMkDirMode()) != 0) {
                throw CairoException.critical(configuration.getFilesFacade().errno())
                        .put("could not create live view checkpoint metadata directory [path=").put(path).put(']');
            }
            LiveViewCheckpointLayout.dataDirPath(path, checkpointsDir).slash();
            if (configuration.getFilesFacade().mkdirs(path, configuration.getMkDirMode()) != 0) {
                throw CairoException.critical(configuration.getFilesFacade().errno())
                        .put("could not create live view checkpoint data directory [path=").put(path).put(']');
            }
        }
    }

    /**
     * Freezes the runtime's current window state - the optional anchor plus every
     * checkpoint-capable function - into immutable pages of the open data
     * segment, and returns the reference set one checkpoint root is built from.
     * A cadence seal freezes once; a repair freezes once per logical boundary its
     * replay crosses, all into the same segment.
     * <p>
     * Anchor entries are the exception: one is a key plus its last-seen anchor
     * value, so they are carried to publication as values and land in the
     * anchor-map metadata pages rather than in the data segment.
     */
    private FrozenBoundary freezeBoundary(
            LiveViewCheckpointDataSegmentWriter dataWriter,
            @NotNull ObjList<WindowFunction> functions,
            @Nullable LiveViewWindow anchorWindow
    ) {
        final FrozenBoundary boundary = new FrozenBoundary();
        long logicalStateBytes = 0;
        if (anchorWindow != null) {
            final FrozenAnchor anchor = new FrozenAnchor(
                    anchorWindow.getWindowName().getBytes(StandardCharsets.UTF_8),
                    anchorWindow.getAnchorValueType(),
                    LiveViewCheckpointMetadata.encodeKeySchema(anchorWindow.getPartitionKeyTypes())
            );
            anchorWindow.freezeCheckpointEntries(keyBuffer, anchor.keys, anchor.anchorValues);
            for (int i = 0, n = anchor.keys.size(); i < n; i++) {
                logicalStateBytes = checkedAdd(logicalStateBytes, anchor.keys.getQuick(i).length);
                logicalStateBytes = checkedAdd(logicalStateBytes, LiveViewCheckpointAnchorRoot.ENTRY_STATE_SIZE);
            }
            boundary.anchor = anchor;
        }
        for (int i = 0, n = functions.size(); i < n; i++) {
            final WindowFunction function = functions.getQuick(i);
            if (!function.supportsCheckpointState()) {
                continue;
            }
            final LiveViewCheckpointFunctionIdentity identity = function.checkpointFunctionIdentity();
            if (identity == null || function.checkpointDependency() == null) {
                throw CairoException.critical(0)
                        .put("checkpoint-capable live view function has no compiler metadata");
            }
            final FrozenFunction frozen = new FrozenFunction(
                    identity.getEncoded(),
                    function.checkpointStateFormatVersion(),
                    LiveViewCheckpointMetadata.encodeKeySchema(function.getCheckpointKeyColumnTypes())
            );
            logicalStateBytes = checkedAdd(logicalStateBytes, freezeFunction(dataWriter, function, frozen));
            boundary.functions.add(frozen);
        }
        if (boundary.functions.size() == 0) {
            throw CairoException.critical(0).put("cannot seal live view checkpoint without functions");
        }
        boundary.logicalStateBytes = logicalStateBytes;
        return boundary;
    }

    private long freezeFunction(
            LiveViewCheckpointDataSegmentWriter dataWriter,
            WindowFunction function,
            FrozenFunction frozen
    ) {
        final Map map = function.getPartitionMap();
        if (map == null) {
            final LiveViewCheckpointStatePageRef ref = freezeStatePage(dataWriter, function, null);
            frozen.scalarStateRef = ref;
            return ref.getDecodedLength();
        }

        long logicalBytes = 0;
        final ColumnTypes keyTypes = function.getCheckpointKeyColumnTypes();
        final int keyStartIndex = function.getCheckpointKeyStartIndex();
        final int tombstoneIndex = function.getTombstoneValueIndex();
        final MapRecordCursor cursor = map.getCursor();
        final MapRecord record = map.getRecord();
        while (cursor.hasNext()) {
            final MapValue value = record.getValue();
            if (tombstoneIndex >= 0 && value.getByte(tombstoneIndex) == 1) {
                continue;
            }
            keyBuffer.jumpTo(0);
            LiveViewSnapshotKeyCodec.writeKey(keyBuffer, record, keyTypes, keyStartIndex);
            final int keyLength = checkedIntLength(keyBuffer.getAppendOffset(), "partition key");
            final byte[] key = new byte[keyLength];
            for (int i = 0; i < keyLength; i++) {
                key[i] = keyBuffer.getByte(i);
            }
            final LiveViewCheckpointStatePageRef stateRef = freezeStatePage(dataWriter, function, value);
            frozen.addPartition(key, stateRef);
            logicalBytes = checkedAdd(logicalBytes, keyLength);
            logicalBytes = checkedAdd(logicalBytes, stateRef.getDecodedLength());
        }
        return logicalBytes;
    }

    private LiveViewCheckpointStatePageRef freezeStatePage(
            LiveViewCheckpointDataSegmentWriter dataWriter,
            WindowFunction function,
            @Nullable MapValue value
    ) {
        final MemoryA sink = dataWriter.beginPage();
        final LiveViewStatePageWriter pageWriter = new LiveViewStatePageWriter().of(sink);
        function.freezeCheckpointState(pageWriter, value);
        final int bytes = checkedIntLength(pageWriter.size(), "function state");
        final LiveViewCheckpointStatePageRef ref = new LiveViewCheckpointStatePageRef();
        dataWriter.endPage(ref, bytes, FUNCTION_STATE_PAGE_KIND, RAW_CODEC, 1, 0);
        return ref;
    }

    private long skipPublishedSegmentIds(Path checkpointsDir, long candidate) {
        try (Path path = new Path()) {
            while (true) {
                LiveViewCheckpointLayout.metaSegmentPath(path, checkpointsDir, candidate);
                final boolean metaExists = configuration.getFilesFacade().exists(path.$());
                LiveViewCheckpointLayout.dataSegmentPath(path, checkpointsDir, candidate);
                if (!metaExists && !configuration.getFilesFacade().exists(path.$())) {
                    if (candidate == Long.MAX_VALUE) {
                        throw CairoException.critical(0).put("live view checkpoint segment id exhausted");
                    }
                    return candidate;
                }
                if (candidate == Long.MAX_VALUE) {
                    throw CairoException.critical(0).put("live view checkpoint segment id exhausted");
                }
                candidate++;
            }
        }
    }

    /**
     * One boundary's anchor map: the window identity the root records, plus the
     * live {@code (key, last-seen anchor value)} pairs, index-aligned.
     */
    private static final class FrozenAnchor {
        private final int anchorValueType;
        private final LongList anchorValues = new LongList();
        private final byte[] keySchema;
        private final ObjList<byte[]> keys = new ObjList<>();
        private final byte[] windowName;

        private FrozenAnchor(byte[] windowName, int anchorValueType, byte[] keySchema) {
            this.windowName = windowName;
            this.anchorValueType = anchorValueType;
            this.keySchema = keySchema;
        }
    }

    /**
     * One logical boundary's frozen state: the optional anchor map plus one
     * entry per checkpoint-capable function. Function state is held only as page
     * references into an already-written data segment, so a capture that spans a
     * whole replay costs metadata rather than a copy of every state image.
     */
    private static final class FrozenBoundary {
        private final ObjList<FrozenFunction> functions = new ObjList<>();
        private final LiveViewCheckpointTimelineEntry oldEntry = new LiveViewCheckpointTimelineEntry();
        private FrozenAnchor anchor;
        private long effectiveLvRowPosition;
        private long logicalStateBytes;
    }

    private static final class FrozenFunction {
        private final HashSet<ByteBuffer> currentKeys = new HashSet<>();
        private final byte[] identity;
        private final byte[] keySchema;
        private final ObjList<FrozenPartition> partitions = new ObjList<>();
        private LiveViewCheckpointStatePageRef scalarStateRef;
        private final int stateFormatVersion;

        private FrozenFunction(byte[] identity, int stateFormatVersion, byte[] keySchema) {
            this.identity = identity;
            this.stateFormatVersion = stateFormatVersion;
            this.keySchema = keySchema;
        }

        private void addPartition(byte[] key, LiveViewCheckpointStatePageRef stateRef) {
            currentKeys.add(ByteBuffer.wrap(key));
            partitions.add(new FrozenPartition(
                    key,
                    new byte[0],
                    new LiveViewCheckpointStatePageRef[]{stateRef}
            ));
        }
    }

    private static final class FrozenPartition {
        private final byte[] key;
        private final byte[] scalarState;
        private final LiveViewCheckpointStatePageRef[] statePageRefs;

        private FrozenPartition(byte[] key, byte[] scalarState, LiveViewCheckpointStatePageRef[] statePageRefs) {
            this.key = key;
            this.scalarState = scalarState;
            this.statePageRefs = statePageRefs;
        }
    }

    private static final class HistoryEpochChangedException extends RuntimeException {
        private static final HistoryEpochChangedException INSTANCE = new HistoryEpochChangedException();

        private HistoryEpochChangedException() {
        }
    }

    /**
     * Result of one localized repair publication.
     */
    public static final class RepairResult {
        private final long dataBytesAdded;
        private final long generation;
        private final long metadataBytesAdded;
        private final int rootsVersioned;
        private final long suffixBreakpointTimestamp;
        private final long suffixRowDelta;
        private final long walPurgeFloor;

        private RepairResult(
                long generation,
                int rootsVersioned,
                long suffixRowDelta,
                long suffixBreakpointTimestamp,
                long dataBytesAdded,
                long metadataBytesAdded,
                long walPurgeFloor
        ) {
            this.generation = generation;
            this.rootsVersioned = rootsVersioned;
            this.suffixRowDelta = suffixRowDelta;
            this.suffixBreakpointTimestamp = suffixBreakpointTimestamp;
            this.dataBytesAdded = dataBytesAdded;
            this.metadataBytesAdded = metadataBytesAdded;
            this.walPurgeFloor = walPurgeFloor;
        }

        public long getDataBytesAdded() {
            return dataBytesAdded;
        }

        public long getGeneration() {
            return generation;
        }

        public long getMetadataBytesAdded() {
            return metadataBytesAdded;
        }

        public int getRootsVersioned() {
            return rootsVersioned;
        }

        /**
         * @return the logical key the suffix range-add landed on, or
         * {@link Numbers#LONG_NULL} when no suffix root exists or no row moved
         */
        public long getSuffixBreakpointTimestamp() {
            return suffixBreakpointTimestamp;
        }

        public long getSuffixRowDelta() {
            return suffixRowDelta;
        }

        public long getWalPurgeFloor() {
            return walPurgeFloor;
        }
    }

    public static final class Result {
        private final long checkpointId;
        private final long dataBytesAdded;
        private final long generation;
        private final long logicalStateBytes;
        private final long metadataBytesAdded;
        private final long walPurgeFloor;

        private Result(
                long generation,
                long checkpointId,
                long logicalStateBytes,
                long dataBytesAdded,
                long metadataBytesAdded,
                long walPurgeFloor
        ) {
            this.generation = generation;
            this.checkpointId = checkpointId;
            this.logicalStateBytes = logicalStateBytes;
            this.dataBytesAdded = dataBytesAdded;
            this.metadataBytesAdded = metadataBytesAdded;
            this.walPurgeFloor = walPurgeFloor;
        }

        public long getCheckpointId() {
            return checkpointId;
        }

        public long getDataBytesAdded() {
            return dataBytesAdded;
        }

        public long getGeneration() {
            return generation;
        }

        public long getLogicalStateBytes() {
            return logicalStateBytes;
        }

        public long getMetadataBytesAdded() {
            return metadataBytesAdded;
        }

        public long getWalPurgeFloor() {
            return walPurgeFloor;
        }
    }

    /**
     * The state one localized repair froze, and the generation it was frozen
     * against. Created by {@link #beginRepair}, given its schedule by
     * {@link RepairCapture#collectBoundaries}, filled by the replay through
     * {@link #capture} as it crosses each logical boundary in {@code [C, H)},
     * and consumed by {@link #publishRepair}.
     * <p>
     * Everything it writes lands in one temporary data segment, so a capture that
     * is closed without publishing leaves an unreferenced temp file and nothing
     * else - no metadata names it and no generation can reach it.
     */
    public class RepairCapture implements Closeable {
        private final ObjList<FrozenBoundary> boundaries = new ObjList<>();
        private final Path checkpointsDir = new Path();
        private final LiveViewCheckpointDataSegmentWriter dataWriter =
                new LiveViewCheckpointDataSegmentWriter(configuration);
        private final long dataSegmentId;
        private final long generation;
        private final LiveViewCheckpointPageRef timelineRootRef = new LiveViewCheckpointPageRef();
        private boolean isDataOpen;
        private boolean isDataPublished;

        private RepairCapture(
                Path checkpointsDir,
                long dataSegmentId,
                long generation,
                LiveViewCheckpointPageRef timelineRootRef
        ) {
            this.checkpointsDir.of(checkpointsDir);
            this.dataSegmentId = dataSegmentId;
            this.generation = generation;
            copy(timelineRootRef, this.timelineRootRef);
        }

        /**
         * Freezes the runtime's current window state as the new root version of
         * {@code entry}, a logical boundary the replay has just finished
         * reproducing.
         *
         * @param entry                   the boundary's current logical entry,
         *                                including the root version being superseded
         * @param functions               the live compiled window functions, standing
         *                                at exactly {@code entry.maxTimestamp}
         * @param anchorWindow            the live anchor window, or null
         * @param effectiveLvRowPosition  replay-derived cumulative live-view row
         *                                position at the boundary
         */
        public void capture(
                @NotNull LiveViewCheckpointTimelineEntry entry,
                @NotNull ObjList<WindowFunction> functions,
                @Nullable LiveViewWindow anchorWindow,
                long effectiveLvRowPosition
        ) {
            if (isDataPublished) {
                throw CairoException.critical(0).put("live view checkpoint repair capture is already published");
            }
            if (effectiveLvRowPosition < 0) {
                throw CairoException.critical(0)
                        .put("negative live view checkpoint repair row position, position=")
                        .put(effectiveLvRowPosition);
            }
            if (entry.rootRef.isNull()) {
                throw CairoException.critical(0).put("live view checkpoint repair boundary has no root version");
            }
            final int size = boundaries.size();
            if (size > 0) {
                final LiveViewCheckpointTimelineEntry previous = boundaries.getQuick(size - 1).oldEntry;
                if (LiveViewCheckpointTimeline.compareKey(
                        previous.maxTimestamp,
                        previous.checkpointId,
                        entry.maxTimestamp,
                        entry.checkpointId
                ) >= 0) {
                    throw CairoException.critical(0)
                            .put("live view checkpoint repair boundaries must ascend [previous=")
                            .put(previous.maxTimestamp).put(", next=").put(entry.maxTimestamp).put(']');
                }
            }
            if (!isDataOpen) {
                dataWriter.of(checkpointsDir, dataSegmentId);
                isDataOpen = true;
            }
            final FrozenBoundary boundary = freezeBoundary(dataWriter, functions, anchorWindow);
            boundary.oldEntry.copyFrom(entry);
            boundary.effectiveLvRowPosition = effectiveLvRowPosition;
            boundaries.add(boundary);
        }

        @Override
        public void close() {
            Misc.free(dataWriter);
            if (isDataOpen && !isDataPublished) {
                try (Path path = new Path()) {
                    LiveViewCheckpointLayout.dataSegmentTmpPath(path, checkpointsDir, dataSegmentId);
                    configuration.getFilesFacade().removeQuiet(path.$());
                }
            }
            boundaries.clear();
            Misc.free(checkpointsDir);
        }

        /**
         * Lists, ascending by key, the logical boundaries the repair has to
         * re-version: every entry with
         * {@code lowTsInclusive <= maxTimestamp < highTsExclusive} in the
         * generation this capture was opened against - the same one
         * {@link #publishRepair} refuses to splice past. That is the repaired
         * {@code [C, H)} interval - the prefix below {@code C} and the
         * converged suffix at or above {@code H} keep their existing payload
         * roots, so neither appears here.
         * <p>
         * The replay hands each one back through {@link #capture} as it crosses
         * it, so the list is also the schedule the replay segments itself on.
         * Entries are copies: the reader's flyweight is only valid inside its
         * own callback.
         */
        public void collectBoundaries(
                long lowTsInclusive,
                long highTsExclusive,
                @NotNull ObjList<LiveViewCheckpointTimelineEntry> out
        ) {
            out.clear();
            if (timelineRootRef.isNull()) {
                return;
            }
            try (LiveViewCheckpointTimelineReader reader = new LiveViewCheckpointTimelineReader(configuration)) {
                reader.of(checkpointsDir);
                reader.range(
                        timelineRootRef,
                        lowTsInclusive,
                        highTsExclusive,
                        entry -> out.add(new LiveViewCheckpointTimelineEntry().copyFrom(entry))
                );
            }
        }

        /**
         * @return the temporary data segment this capture freezes every boundary
         * into. It reaches its final name only at {@link #publishRepair}, so until
         * then the repair's own descriptor is the sole record that it exists.
         */
        public long getDataSegmentId() {
            return dataSegmentId;
        }

        /**
         * @return the generation this capture was opened against
         */
        public long getGeneration() {
            return generation;
        }

        public int size() {
            return boundaries.size();
        }

        private long commitData() {
            final long bytes = dataWriter.commit();
            isDataPublished = true;
            return bytes;
        }

        private void validateAgainst(long highTsExclusive) {
            for (int i = 0, n = boundaries.size(); i < n; i++) {
                final LiveViewCheckpointTimelineEntry entry = boundaries.getQuick(i).oldEntry;
                if (entry.maxTimestamp >= highTsExclusive) {
                    throw CairoException.critical(0)
                            .put("live view checkpoint repair boundary is at or above the convergence bound")
                            .put(" [boundary=").put(entry.maxTimestamp)
                            .put(", highTsExclusive=").put(highTsExclusive).put(']');
                }
            }
        }
    }

    /**
     * The metadata writers one publication builds its checkpoint roots with, plus
     * the running segment-id cursor and metadata byte count they share. One
     * instance serves every boundary of a repair, so a K-root splice allocates
     * its builders once.
     */
    private final class RootBuilders implements Closeable {
        private final LiveViewCheckpointAnchorRootBuilder anchorRootBuilder =
                new LiveViewCheckpointAnchorRootBuilder(configuration);
        private final LiveViewCheckpointRootBuilder checkpointRootBuilder =
                new LiveViewCheckpointRootBuilder(configuration);
        private final Path checkpointsDir = new Path();
        private final LiveViewCheckpointFunctionRootBuilder functionRootBuilder =
                new LiveViewCheckpointFunctionRootBuilder(configuration);
        private final LiveViewCheckpointFunctionRoot oldFunctionRoot =
                new LiveViewCheckpointFunctionRoot(configuration);
        private final LiveViewCheckpointPartitionMapReader oldPartitionReader =
                new LiveViewCheckpointPartitionMapReader(configuration);
        private long metadataBytesAdded;
        private long nextSegmentId;

        /**
         * Writes the anchor root, one function root per frozen function, and the
         * checkpoint root itself. The two old-root arguments are the boundary's
         * predecessor: the builders start from its anchor/function/partition-map
         * paths, so an unchanged entry is reused by reference rather than
         * rewritten. Both are empty for the first root of a timeline.
         */
        private void buildRoot(
                FrozenBoundary boundary,
                LiveViewCheckpointPageRef oldAnchorRootRef,
                @Nullable LiveViewCheckpointFunctionDirectory oldFunctionDirectory,
                long checkpointId,
                long maxTimestamp,
                long definitionTxn,
                LiveViewCheckpointPageRef rootRefOut,
                LongList referencedSegmentIdsOut
        ) {
            final LiveViewCheckpointPageRef anchorRootRef = new LiveViewCheckpointPageRef();
            if (boundary.anchor != null) {
                final FrozenAnchor anchor = boundary.anchor;
                anchorRootBuilder.of(
                        checkpointsDir,
                        oldAnchorRootRef,
                        anchor.windowName,
                        anchor.anchorValueType,
                        anchor.keySchema
                );
                for (int i = 0, n = anchor.keys.size(); i < n; i++) {
                    anchorRootBuilder.putPartition(anchor.keys.getQuick(i), anchor.anchorValues.getQuick(i));
                }
                nextSegmentId = skipPublishedSegmentIds(checkpointsDir, nextSegmentId);
                anchorRootBuilder.build(nextSegmentId++, anchorRootRef);
                metadataBytesAdded = checkedAdd(metadataBytesAdded, anchorRootBuilder.getLastSegmentBytes());
            }

            nextSegmentId = skipPublishedSegmentIds(checkpointsDir, nextSegmentId);
            checkpointRootBuilder.begin(
                    checkpointsDir,
                    checkpointId,
                    maxTimestamp,
                    definitionTxn,
                    anchorRootRef
            );
            final LiveViewCheckpointPageRef oldFunctionRootRef = new LiveViewCheckpointPageRef();
            final LiveViewCheckpointPageRef functionRootRef = new LiveViewCheckpointPageRef();
            for (int i = 0, n = boundary.functions.size(); i < n; i++) {
                final FrozenFunction frozen = boundary.functions.getQuick(i);
                oldFunctionRootRef.clear();
                if (oldFunctionDirectory != null) {
                    oldFunctionDirectory.find(frozen.identity, oldFunctionRootRef);
                }
                functionRootBuilder.of(
                        checkpointsDir,
                        oldFunctionRootRef,
                        frozen.identity,
                        frozen.stateFormatVersion,
                        frozen.keySchema
                );
                if (frozen.scalarStateRef != null) {
                    functionRootBuilder.setScalarStateRef(frozen.scalarStateRef);
                } else {
                    removeMissingPartitions(
                            checkpointsDir,
                            oldFunctionRootRef,
                            oldFunctionRoot,
                            oldPartitionReader,
                            frozen,
                            functionRootBuilder
                    );
                    for (int p = 0, m = frozen.partitions.size(); p < m; p++) {
                        final FrozenPartition partition = frozen.partitions.getQuick(p);
                        functionRootBuilder.putPartition(
                                partition.key,
                                partition.scalarState,
                                partition.statePageRefs
                        );
                    }
                }
                nextSegmentId = skipPublishedSegmentIds(checkpointsDir, nextSegmentId);
                functionRootBuilder.build(nextSegmentId++, functionRootRef);
                metadataBytesAdded = checkedAdd(metadataBytesAdded, functionRootBuilder.getLastSegmentBytes());
                checkpointRootBuilder.addFunction(functionRootRef);
            }

            nextSegmentId = skipPublishedSegmentIds(checkpointsDir, nextSegmentId);
            checkpointRootBuilder.build(nextSegmentId++, rootRefOut);
            metadataBytesAdded = checkedAdd(metadataBytesAdded, checkpointRootBuilder.getLastSegmentBytes());
            checkpointRootBuilder.getReferencedSegmentIds(referencedSegmentIdsOut);
        }

        @Override
        public void close() {
            Misc.free(anchorRootBuilder);
            Misc.free(checkpointRootBuilder);
            Misc.free(functionRootBuilder);
            Misc.free(oldFunctionRoot);
            Misc.free(oldPartitionReader);
            Misc.free(checkpointsDir);
        }

        private void of(Path checkpointsDir, long nextSegmentId) {
            this.checkpointsDir.of(checkpointsDir);
            this.oldPartitionReader.of(checkpointsDir);
            this.nextSegmentId = nextSegmentId;
            this.metadataBytesAdded = 0;
        }
    }
}
