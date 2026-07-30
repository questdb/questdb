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
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.LongList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.Transient;
import io.questdb.std.Vect;
import io.questdb.std.str.Path;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

import java.io.Closeable;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
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
    /**
     * Throws once the superblock has committed, so the caller observes a failed
     * publication over a durably advanced generation. Unlike the two stages
     * above, {@link #publishCompaction} is the only path that honours it -
     * compaction is the only publication that stages a data segment an abort
     * could unlink.
     */
    @TestOnly
    public static final int TEST_FAIL_AFTER_SUPERBLOCK_PUBLISH = 3;

    private static final Log LOG = LogFactory.getLog(LiveViewCheckpointTimelineStoreWriter.class);
    /**
     * Published data segments one seal may hold mapped at once while it compares
     * cold keys against their previous pages. Elision spreads a boundary's live
     * references over the segments each key was last written into, so a wide key
     * set walks more segments than one; a set wider than the cache re-maps rather
     * than failing, which still costs less than re-imaging every key.
     */
    private static final int PREVIOUS_DATA_READER_CACHE_SIZE = 8;

    private final HashSet<String> lifecycleReconciledDirs = new HashSet<>();
    private final CairoConfiguration configuration;
    // Read-only argument of a cadence seal's reference transaction, which only
    // ever adds; kept per instance so the seal path allocates nothing for it.
    private final LongList emptySegmentIds = new LongList();
    private final MemoryCARW keyBuffer;
    private final LiveViewCheckpointRingSeal ringSeal;
    // One whole-state image at a time, encoded here before the freeze decides
    // whether it has to become a page at all.
    private final MemoryCARW stateBuffer;
    private final LiveViewStatePageWriter statePageWriter = new LiveViewStatePageWriter();
    @TestOnly
    private int testFailureStage;

    public LiveViewCheckpointTimelineStoreWriter(@NotNull CairoConfiguration configuration) {
        this.configuration = configuration;
        this.keyBuffer = Vm.getCARWInstance(4096, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT);
        this.ringSeal = new LiveViewCheckpointRingSeal(configuration, null);
        this.stateBuffer = Vm.getCARWInstance(4096, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT);
    }

    /**
     * Appends one logical boundary strictly above the current head and publishes
     * the generation that carries it.
     *
     * @param batchMinTs       the lowest designated timestamp the runtime has
     *                         processed since the current head boundary, or
     *                         {@link Numbers#LONG_NULL} when the caller cannot say.
     *                         A ring-shaped function shares the head root's chunk
     *                         pages only when this proves the batch sits strictly
     *                         above that boundary; anything weaker writes a complete
     *                         state image, which costs more but cannot splice a prefix
     *                         the runtime no longer holds
     * @param seedCursorOffset the seed sweep's base-cursor row offset when a
     *                         mid-sweep cadence event drives this append, so a
     *                         restart can resume the sweep from the root this
     *                         publishes; {@link Numbers#LONG_NULL} for a steady
     *                         cadence seal, which is what tells a restart the
     *                         newest root is not a resume point
     */
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
            long effectiveLvRowPosition,
            long batchMinTs,
            long seedCursorOffset
    ) {
        if (!primaryOwner) {
            throw CairoException.critical(0).put("replica must not publish a live view checkpoint timeline");
        }
        final String lifecycleKey = checkpointsDir.toString();
        boolean epochRetry = false;
        while (true) {
            long orphanUpperBound = 0;
            // A view created in this process reconciles here rather than at
            // startup, so this is its only chance to learn what its catalogue
            // holds. LONG_NULL when the reconciliation was skipped or adopted no
            // generation, which leaves whatever an earlier sweep reported.
            long liveSegmentCount = Numbers.LONG_NULL;
            long obsoleteSegmentBytes = Numbers.LONG_NULL;
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
                if (reconciliation.getStats() != null) {
                    liveSegmentCount = reconciliation.getLiveSegmentCount();
                    obsoleteSegmentBytes = reconciliation.getObsoleteSegmentBytes();
                }
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
                        batchMinTs,
                        seedCursorOffset,
                        orphanUpperBound,
                        liveSegmentCount,
                        obsoleteSegmentBytes
                );
            } catch (HistoryEpochChangedException e) {
                lifecycleReconciledDirs.remove(lifecycleKey);
                if (epochRetry) {
                    throw CairoException.critical(0).put("could not replace live view checkpoint history epoch");
                }
                epochRetry = true;
            } catch (BoundaryNotAboveHeadException e) {
                // The append refused before touching a file, and the reconciliation
                // above still holds, so the key stays: this seal is skipped, not failed.
                throw e;
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
        Misc.free(ringSeal);
        Misc.free(stateBuffer);
        lifecycleReconciledDirs.clear();
    }

    /**
     * Publishes one physical compaction as a timeline splice that relocates state
     * pages without changing a single logical coordinate.
     * <p>
     * The driver has already repacked every drained page into the plan's target
     * segment; this method rebuilds only the roots that name a drained segment,
     * swapping each such page reference to its relocated one and reusing every
     * other root, anchor and function tree by reference. A rebuilt root keeps its
     * {@code checkpointId}, {@code maxTimestamp}, {@code createdLvSeqTxn},
     * {@code baseLvRowPosition} and {@code logicalStateBytes} - only the physical
     * location of its bytes moves - so the row-position delta index, the watermarks
     * and the checkpoint-id counter all carry forward untouched.
     * <p>
     * The publication is one atomic A/B superblock swap: the old generation is
     * fully valid until it commits and the new one is fully valid after, so a crash
     * in between leaves the pre-compaction generation intact and the committed
     * target segment as an ordinary final-name orphan the next seal reclaims. No
     * durable repair marker is needed.
     * <p>
     * The drained source segments retire at the new generation once no rebuilt root
     * names them, and the purge job reclaims them after the fallback A/B slot
     * advances past the old generation and no reader pins it.
     *
     * @param plan the target segment and physical-page redirect the driver's repack
     *             produced against the current generation
     */
    public CompactionResult publishCompaction(
            @Transient @NotNull Path checkpointsDir,
            long definitionTxn,
            long historyEpoch,
            boolean primaryOwner,
            @NotNull LiveViewCheckpointCompactionPlan plan
    ) {
        if (!primaryOwner) {
            throw CairoException.critical(0).put("replica must not publish a live view checkpoint timeline");
        }
        try (
                LiveViewCheckpointMetaStore metaStore = new LiveViewCheckpointMetaStore(configuration);
                LiveViewCheckpointTimelineReader timelineReader = new LiveViewCheckpointTimelineReader(configuration);
                LiveViewCheckpointSegmentDirectoryWriter directoryWriter = new LiveViewCheckpointSegmentDirectoryWriter(configuration);
                RootBuilders roots = new RootBuilders();
                LiveViewCheckpointTimelineWriter timelineWriter = new LiveViewCheckpointTimelineWriter(configuration)
        ) {
            metaStore.of(checkpointsDir);
            timelineReader.of(checkpointsDir);
            timelineWriter.of(checkpointsDir);
            directoryWriter.of(checkpointsDir);

            if (!metaStore.isValid()) {
                throw CairoException.critical(0)
                        .put("cannot compact a live view checkpoint timeline with no valid generation");
            }
            final LiveViewCheckpointSuperblock superblock = metaStore.getSuperblock();
            if (superblock.definitionTxn != definitionTxn || superblock.historyEpoch != historyEpoch) {
                throw CairoException.critical(CairoException.LV_CHECKPOINT_TIMELINE_INVALID)
                        .put("live view checkpoint compaction definition identity mismatch");
            }
            if (superblock.generation != plan.getGeneration()) {
                throw CairoException.critical(0)
                        .put("live view checkpoint timeline moved under the compaction plan")
                        .put(" [planned=").put(plan.getGeneration())
                        .put(", current=").put(superblock.generation).put(']');
            }

            final long generation = checkedIncrement(superblock.generation, "generation");
            final LiveViewCheckpointPageRef oldTimelineRoot = copy(superblock.timelineRootRef);
            final LiveViewCheckpointPageRef oldDirectoryRoot = copy(superblock.segmentDirectoryRootRef);
            directoryWriter.begin(oldDirectoryRoot);
            registerPendingDirectorySegment(directoryWriter, superblock);

            final long targetSegmentId = plan.getTargetSegmentId();
            long nextSegmentId = Math.max(superblock.nextSegmentId, targetSegmentId + 1);
            roots.of(checkpointsDir, nextSegmentId);

            // One ordered pass over the timeline. Rebuilding a root reads separate
            // metadata (checkpoint roots, function roots, partition maps) and writes
            // its own fresh segment, so it never touches the timeline reader's cursor
            // and runs inside the visitor without a materialized entry-per-root copy.
            // Only the changed entries - bounded by the roots that name a drained
            // segment - are copied, for the splice that follows.
            final ObjList<LiveViewCheckpointTimelineEntry> changedEntries = new ObjList<>();
            final LongList removedSegmentIds = new LongList();
            final LongList addedSegmentIds = new LongList();
            final LiveViewCheckpointPageRef newRootRef = new LiveViewCheckpointPageRef();
            final int[] targetSegmentRootRefs = {0};
            timelineReader.iterateAll(oldTimelineRoot, entry -> {
                if (!roots.buildRedirectedRoot(
                        entry.rootRef,
                        definitionTxn,
                        plan,
                        newRootRef,
                        removedSegmentIds,
                        addedSegmentIds
                )) {
                    // Names no drained segment, so every page it holds stays put and
                    // the entry keeps its existing root by reference.
                    return;
                }
                final LiveViewCheckpointTimelineEntry newEntry = new LiveViewCheckpointTimelineEntry().copyFrom(entry);
                newEntry.rootRef.of(newRootRef.getSegmentId(), newRootRef.getOffset(), newRootRef.getLength());
                changedEntries.add(newEntry);
                // The introduced target segment carries its own root-reference count
                // through addSegment, so it must not also be counted per root.
                if (dropSegmentId(addedSegmentIds, targetSegmentId)) {
                    targetSegmentRootRefs[0]++;
                }
                registerBoundarySegments(directoryWriter, roots.writtenMetaSegments, addedSegmentIds);
                directoryWriter.applyRootReferenceChanges(removedSegmentIds, addedSegmentIds, generation);
            });

            if (targetSegmentRootRefs[0] == 0) {
                // A non-empty plan whose pages no surviving root names is an
                // inconsistency between planning and publication; refuse rather
                // than leave the committed target segment referenced by nobody.
                throw CairoException.critical(CairoException.LV_CHECKPOINT_TIMELINE_INVALID)
                        .put("live view checkpoint compaction plan redirected no live root");
            }
            nextSegmentId = roots.nextSegmentId;
            long metadataBytesAdded = roots.metadataBytesAdded;
            directoryWriter.addSegment(targetSegmentId, plan.getTargetSegmentBytes(), targetSegmentRootRefs[0]);

            final LiveViewCheckpointTimelineEntry[] spliced = new LiveViewCheckpointTimelineEntry[changedEntries.size()];
            for (int i = 0; i < spliced.length; i++) {
                spliced[i] = changedEntries.getQuick(i);
            }
            nextSegmentId = skipPublishedSegmentIds(checkpointsDir, nextSegmentId);
            final long timelineSegmentId = nextSegmentId++;
            final LiveViewCheckpointPageRef newTimelineRoot = new LiveViewCheckpointPageRef();
            timelineWriter.splice(oldTimelineRoot, spliced, spliced.length, timelineSegmentId, newTimelineRoot);
            metadataBytesAdded = checkedAdd(metadataBytesAdded, timelineWriter.getLastSegmentBytes());
            registerMetadataSegment(
                    directoryWriter,
                    timelineSegmentId,
                    timelineWriter.getLastSegmentBytes(),
                    timelineWriter.getLastSegmentPageCount()
            );
            directoryWriter.releaseMetadataPages(timelineWriter.getLastReleasedSegmentIds(), generation);

            nextSegmentId = skipPublishedSegmentIds(checkpointsDir, nextSegmentId);
            final long directorySegmentId = nextSegmentId++;
            final LiveViewCheckpointPageRef newDirectoryRoot = new LiveViewCheckpointPageRef();
            directoryWriter.publish(directorySegmentId, generation, newDirectoryRoot);
            metadataBytesAdded = checkedAdd(metadataBytesAdded, directoryWriter.getLastSegmentBytes());
            if (testFailureStage == TEST_FAIL_AFTER_METADATA_PUBLISH) {
                throw CairoException.critical(0).put("test failure after live view checkpoint metadata publication");
            }

            superblock.generation = generation;
            superblock.nextSegmentId = nextSegmentId;
            carryPendingDirectorySegment(directoryWriter, superblock, directorySegmentId);
            superblock.metadataBytes = checkedAdd(superblock.metadataBytes, metadataBytesAdded);
            superblock.dataBytes = checkedAdd(superblock.dataBytes, plan.getTargetSegmentBytes());
            // Compaction relocates bytes without changing any logical coordinate, so
            // the logical state total, row-position delta index and its root, the
            // base and live-view watermarks, the checkpoint-id counter and the
            // mid-sweep seed cursor all carry forward untouched.
            copy(newTimelineRoot, superblock.timelineRootRef);
            copy(newDirectoryRoot, superblock.segmentDirectoryRootRef);
            metaStore.publish();
            if (testFailureStage == TEST_FAIL_AFTER_SUPERBLOCK_PUBLISH) {
                throw CairoException.critical(0).put("test failure after live view checkpoint superblock publication");
            }

            return new CompactionResult(
                    generation,
                    changedEntries.size(),
                    targetSegmentId,
                    plan.getTargetSegmentBytes(),
                    metadataBytesAdded,
                    metaStore.getWalPurgeFloor(),
                    new LiveViewCheckpointTimelineStats()
                            .of(superblock, checkedAdd(plan.getTargetSegmentBytes(), metadataBytesAdded))
            );
        }
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
     * @param primaryOwner         false refuses the publication; every live-view caller
     *                             passes true on either role, since each node owns the
     *                             timeline it sealed (see
     *                             {@link LiveViewCheckpointLifecycle#reconcile})
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
                LiveViewCheckpointSegmentDirectoryWriter directoryWriter = new LiveViewCheckpointSegmentDirectoryWriter(configuration);
                RootBuilders roots = new RootBuilders();
                LiveViewCheckpointTimelineWriter timelineWriter = new LiveViewCheckpointTimelineWriter(configuration);
                LiveViewCheckpointRowPositionDeltaWriter deltaWriter = new LiveViewCheckpointRowPositionDeltaWriter(configuration)
        ) {
            metaStore.of(checkpointsDir);
            timelineReader.of(checkpointsDir);
            deltaReader.of(checkpointsDir);
            timelineWriter.of(checkpointsDir);
            deltaWriter.of(checkpointsDir);
            directoryWriter.of(checkpointsDir);

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
            directoryWriter.begin(oldDirectoryRoot);
            registerPendingDirectorySegment(directoryWriter, superblock);

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
            // Roots that actually name the capture's segment. A boundary whose
            // rings all carried the previous boundary's chunks forward names
            // nothing in it, and counting it would leave the segment referenced
            // after every root that reads it is gone.
            int captureSegmentRootRefs = 0;
            // Signed: a re-versioned root can hold less state than the one it
            // replaces, so the generation's logical total moves either way.
            long logicalStateBytesDelta = 0;
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
                if (dropSegmentId(addedSegmentIds, capture.dataSegmentId)) {
                    captureSegmentRootRefs++;
                }
                registerBoundarySegments(directoryWriter, roots.writtenMetaSegments, addedSegmentIds);
                directoryWriter.applyRootReferenceChanges(removedSegmentIds, addedSegmentIds, generation);

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
                logicalStateBytesDelta += boundary.logicalStateBytes - oldEntry.logicalStateBytes;
            }
            nextSegmentId = roots.nextSegmentId;
            long metadataBytesAdded = roots.metadataBytesAdded;
            if (dataSegmentBytes > 0) {
                directoryWriter.addSegment(capture.dataSegmentId, dataSegmentBytes, captureSegmentRootRefs);
            }

            final LiveViewCheckpointPageRef newTimelineRoot = new LiveViewCheckpointPageRef();
            if (boundaryCount > 0) {
                nextSegmentId = skipPublishedSegmentIds(checkpointsDir, nextSegmentId);
                final long timelineSegmentId = nextSegmentId++;
                timelineWriter.splice(oldTimelineRoot, newEntries, boundaryCount, timelineSegmentId, newTimelineRoot);
                metadataBytesAdded = checkedAdd(metadataBytesAdded, timelineWriter.getLastSegmentBytes());
                registerMetadataSegment(
                        directoryWriter,
                        timelineSegmentId,
                        timelineWriter.getLastSegmentBytes(),
                        timelineWriter.getLastSegmentPageCount()
                );
                directoryWriter.releaseMetadataPages(timelineWriter.getLastReleasedSegmentIds(), generation);
            } else {
                copy(oldTimelineRoot, newTimelineRoot);
            }

            // The newest logical key the published timeline holds, resolved against
            // the OLD tree for the reason the breakpoint below states. The caller
            // needs it because a splice appends no root: it seals a fresh boundary
            // of its own only when its runtime frontier has run past this one.
            final LiveViewCheckpointTimelineEntry headEntry = new LiveViewCheckpointTimelineEntry();
            final long headRootMaxTimestamp = timelineReader.last(oldTimelineRoot, headEntry)
                    ? headEntry.maxTimestamp
                    : Numbers.LONG_NULL;

            // The breakpoint is resolved against the OLD tree on purpose: a splice
            // preserves every key, so the first suffix key is the same in both, and
            // reading it here needs no page from the segment just written.
            final LiveViewCheckpointPageRef newDeltaRoot = new LiveViewCheckpointPageRef();
            copy(oldDeltaRoot, newDeltaRoot);
            long suffixBreakpointTimestamp = Numbers.LONG_NULL;
            long rowPositionDeltaBytesAdded = 0;
            if (suffixRowDelta != 0) {
                final LiveViewCheckpointTimelineEntry suffixEntry = new LiveViewCheckpointTimelineEntry();
                if (timelineReader.successor(oldTimelineRoot, highTsExclusive, suffixEntry)) {
                    nextSegmentId = skipPublishedSegmentIds(checkpointsDir, nextSegmentId);
                    final long deltaSegmentId = nextSegmentId++;
                    deltaWriter.suffixAdd(
                            oldDeltaRoot,
                            suffixEntry.maxTimestamp,
                            suffixEntry.checkpointId,
                            suffixRowDelta,
                            deltaSegmentId,
                            newDeltaRoot
                    );
                    rowPositionDeltaBytesAdded = deltaWriter.getLastSegmentBytes();
                    metadataBytesAdded = checkedAdd(metadataBytesAdded, rowPositionDeltaBytesAdded);
                    suffixBreakpointTimestamp = suffixEntry.maxTimestamp;
                    registerMetadataSegment(
                            directoryWriter,
                            deltaSegmentId,
                            deltaWriter.getLastSegmentBytes(),
                            deltaWriter.getLastSegmentPageCount()
                    );
                    directoryWriter.releaseMetadataPages(deltaWriter.getLastReleasedSegmentIds(), generation);
                }
            }

            nextSegmentId = skipPublishedSegmentIds(checkpointsDir, nextSegmentId);
            final long directorySegmentId = nextSegmentId++;
            final LiveViewCheckpointPageRef newDirectoryRoot = new LiveViewCheckpointPageRef();
            directoryWriter.publish(directorySegmentId, generation, newDirectoryRoot);
            metadataBytesAdded = checkedAdd(metadataBytesAdded, directoryWriter.getLastSegmentBytes());
            if (testFailureStage == TEST_FAIL_AFTER_METADATA_PUBLISH) {
                throw CairoException.critical(0).put("test failure after live view checkpoint metadata publication");
            }

            superblock.generation = generation;
            superblock.normalizedBaseSeqTxn = normalizedBaseSeqTxn;
            superblock.coveredLvSeqTxn = coveredLvSeqTxn;
            superblock.nextSegmentId = nextSegmentId;
            superblock.metadataBytes = checkedAdd(superblock.metadataBytes, metadataBytesAdded);
            superblock.dataBytes = checkedAdd(superblock.dataBytes, dataSegmentBytes);
            superblock.logicalStateBytes = checkedAdd(superblock.logicalStateBytes, logicalStateBytesDelta);
            superblock.rowPositionDeltaBytes = checkedAdd(superblock.rowPositionDeltaBytes, rowPositionDeltaBytesAdded);
            // A repair only ever runs on an ACTIVE view, so the generation it
            // publishes is never a mid-sweep resume point. Clear the cursor
            // explicitly rather than letting the selected slot's value ride
            // along.
            superblock.seedCursorOffset = Numbers.LONG_NULL;
            carryPendingDirectorySegment(directoryWriter, superblock, directorySegmentId);
            copy(newTimelineRoot, superblock.timelineRootRef);
            copy(newDeltaRoot, superblock.rowPositionDeltaRootRef);
            copy(newDirectoryRoot, superblock.segmentDirectoryRootRef);
            metaStore.publish();

            return new RepairResult(
                    generation,
                    boundaryCount,
                    headRootMaxTimestamp,
                    suffixRowDelta,
                    suffixBreakpointTimestamp,
                    dataSegmentBytes,
                    metadataBytesAdded,
                    metaStore.getWalPurgeFloor(),
                    new LiveViewCheckpointTimelineStats()
                            .of(superblock, checkedAdd(dataSegmentBytes, metadataBytesAdded))
            );
        }
    }

    /**
     * Preserves the timeline's prefix below {@code floorTimestamp} and drops
     * every logical root at or above it, publishing a new generation over the
     * surviving prefix. This is the prefix-preserving alternative to a
     * whole-timeline retire for an out-of-order repair whose influence reaches
     * the runtime frontier (EOF) or that resumes from a predecessor: the tail
     * roots the repair is about to rewrite go, but every long-term anchor below
     * the floor stays addressable and the checkpoint id space carries forward.
     * <p>
     * The dropped roots release the data segments they referenced, which retire
     * at this generation for the purge job to reclaim. The row-position delta
     * root and the checkpoint-id counter carry forward unchanged, the logical
     * state total sheds the dropped roots' contribution, and - deliberately - the
     * base watermark is left where it was. That leaves the superblock momentarily
     * naming a head the truncate discarded; the caller closes the gap by writing
     * a {@link LiveViewCheckpointRepairMarker} before calling this, so a crash
     * before the post-replay seal forces a restart to rebuild from the applied
     * base rather than trust the truncated head.
     *
     * @return a result whose {@link TruncateResult#isPublished()} is false when
     * no prefix survives below the floor (the whole timeline sits at or above
     * it); nothing is published and the caller falls back to a full retire
     */
    public TruncateResult publishTruncate(
            @Transient @NotNull Path checkpointsDir,
            long definitionTxn,
            long historyEpoch,
            long floorTimestamp,
            boolean primaryOwner
    ) {
        if (!primaryOwner) {
            throw CairoException.critical(0).put("replica must not publish a live view checkpoint timeline");
        }
        try (
                LiveViewCheckpointMetaStore metaStore = new LiveViewCheckpointMetaStore(configuration);
                LiveViewCheckpointTimelineReader timelineReader = new LiveViewCheckpointTimelineReader(configuration);
                LiveViewCheckpointRoot oldCheckpointRoot = new LiveViewCheckpointRoot(configuration);
                LiveViewCheckpointSegmentDirectoryWriter directoryWriter = new LiveViewCheckpointSegmentDirectoryWriter(configuration);
                LiveViewCheckpointTimelineWriter timelineWriter = new LiveViewCheckpointTimelineWriter(configuration)
        ) {
            metaStore.of(checkpointsDir);
            timelineReader.of(checkpointsDir);
            timelineWriter.of(checkpointsDir);
            directoryWriter.of(checkpointsDir);

            if (!metaStore.isValid()) {
                throw CairoException.critical(0)
                        .put("cannot truncate a live view checkpoint timeline with no valid generation");
            }
            final LiveViewCheckpointSuperblock superblock = metaStore.getSuperblock();
            if (superblock.definitionTxn != definitionTxn || superblock.historyEpoch != historyEpoch) {
                throw CairoException.critical(CairoException.LV_CHECKPOINT_TIMELINE_INVALID)
                        .put("live view checkpoint truncate definition identity mismatch");
            }

            final LiveViewCheckpointPageRef oldTimelineRoot = copy(superblock.timelineRootRef);
            final LiveViewCheckpointPageRef oldDirectoryRoot = copy(superblock.segmentDirectoryRootRef);

            // No boundary below the floor: there is no prefix to preserve, so
            // publish nothing and let the caller retire the whole timeline.
            final LiveViewCheckpointTimelineEntry probe = new LiveViewCheckpointTimelineEntry();
            if (!timelineReader.predecessor(oldTimelineRoot, floorTimestamp, probe)) {
                return TruncateResult.NOT_PUBLISHED;
            }

            final long generation = checkedIncrement(superblock.generation, "generation");
            directoryWriter.begin(oldDirectoryRoot);
            registerPendingDirectorySegment(directoryWriter, superblock);

            // Release every root at or above the floor: each drops the data
            // segments it referenced, so a segment no surviving root names retires
            // at this generation for the purge job to reclaim once no reader can
            // reach it. Applied per root because repeated references inside one
            // root count once per side.
            final LongList removedSegmentIds = new LongList();
            final long[] droppedAccumulators = new long[1]; // {logicalStateBytes}
            timelineReader.range(oldTimelineRoot, floorTimestamp, Long.MAX_VALUE, entry -> {
                oldCheckpointRoot.of(checkpointsDir, entry.rootRef);
                if (oldCheckpointRoot.getCheckpointId() != entry.checkpointId
                        || oldCheckpointRoot.getMaxTimestamp() != entry.maxTimestamp
                        || oldCheckpointRoot.getDefinitionTxn() != definitionTxn) {
                    throw CairoException.critical(CairoException.LV_CHECKPOINT_TIMELINE_INVALID)
                            .put("live view checkpoint truncate root identity mismatch [checkpointId=")
                            .put(entry.checkpointId).put(']');
                }
                removedSegmentIds.clear();
                for (int s = 0, n = oldCheckpointRoot.getSegmentIdCount(); s < n; s++) {
                    removedSegmentIds.add(oldCheckpointRoot.getSegmentId(s));
                }
                directoryWriter.applyRootReferenceChanges(removedSegmentIds, emptySegmentIds, generation);
                droppedAccumulators[0] = checkedAdd(droppedAccumulators[0], entry.logicalStateBytes);
            });

            long nextSegmentId = skipPublishedSegmentIds(checkpointsDir, superblock.nextSegmentId);
            final long timelineSegmentId = nextSegmentId++;
            final LiveViewCheckpointPageRef newTimelineRoot = new LiveViewCheckpointPageRef();
            final boolean survived = timelineWriter.truncateAbove(oldTimelineRoot, floorTimestamp, timelineSegmentId, newTimelineRoot);
            // The predecessor probe above proved a prefix key exists below the floor.
            assert survived;
            long metadataBytesAdded = timelineWriter.getLastSegmentBytes();
            registerMetadataSegment(
                    directoryWriter,
                    timelineSegmentId,
                    timelineWriter.getLastSegmentBytes(),
                    timelineWriter.getLastSegmentPageCount()
            );
            directoryWriter.releaseMetadataPages(timelineWriter.getLastReleasedSegmentIds(), generation);

            nextSegmentId = skipPublishedSegmentIds(checkpointsDir, nextSegmentId);
            final long directorySegmentId = nextSegmentId++;
            final LiveViewCheckpointPageRef newDirectoryRoot = new LiveViewCheckpointPageRef();
            directoryWriter.publish(directorySegmentId, generation, newDirectoryRoot);
            metadataBytesAdded = checkedAdd(metadataBytesAdded, directoryWriter.getLastSegmentBytes());
            if (testFailureStage == TEST_FAIL_AFTER_METADATA_PUBLISH) {
                throw CairoException.critical(0).put("test failure after live view checkpoint metadata publication");
            }

            superblock.generation = generation;
            // The base and live-view watermarks carry forward unchanged: this
            // publication moves no coordinate, and correctness on a mid-repair
            // crash comes from the repair marker (see the method javadoc), not
            // from this watermark, which still names the discarded head until the
            // post-replay seal advances it.
            superblock.nextSegmentId = nextSegmentId;
            superblock.metadataBytes = checkedAdd(superblock.metadataBytes, metadataBytesAdded);
            superblock.logicalStateBytes = checkedAdd(superblock.logicalStateBytes, -droppedAccumulators[0]);
            // A truncate leaves no mid-sweep resume point behind.
            superblock.seedCursorOffset = Numbers.LONG_NULL;
            carryPendingDirectorySegment(directoryWriter, superblock, directorySegmentId);
            copy(newTimelineRoot, superblock.timelineRootRef);
            // The row-position delta root carries forward unchanged: dropping the
            // suffix moves no surviving prefix key's cumulative recovery position.
            copy(newDirectoryRoot, superblock.segmentDirectoryRootRef);
            metaStore.publish();

            return new TruncateResult(
                    generation,
                    metadataBytesAdded,
                    metaStore.getWalPurgeFloor(),
                    new LiveViewCheckpointTimelineStats().of(superblock, metadataBytesAdded)
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
            long batchMinTs,
            long seedCursorOffset,
            long orphanUpperBound,
            long liveSegmentCount,
            long obsoleteSegmentBytes
    ) {
        if (definitionTxn < 0
                || createdLvSeqTxn < 0
                || historyEpoch < 0
                || normalizedBaseSeqTxn < 0
                || coveredLvSeqTxn < 0
                || effectiveLvRowPosition < 0
                || createdLvSeqTxn > coveredLvSeqTxn
                || (seedCursorOffset < 0 && seedCursorOffset != Numbers.LONG_NULL)) {
            throw CairoException.critical(0).put("invalid live view normal checkpoint coordinates");
        }
        ensureDirectories(checkpointsDir);

        try (
                LiveViewCheckpointMetaStore metaStore = new LiveViewCheckpointMetaStore(configuration);
                LiveViewCheckpointTimelineReader timelineReader = new LiveViewCheckpointTimelineReader(configuration);
                LiveViewCheckpointRowPositionDeltaReader deltaReader = new LiveViewCheckpointRowPositionDeltaReader(configuration);
                LiveViewCheckpointRoot oldCheckpointRoot = new LiveViewCheckpointRoot(configuration);
                LiveViewCheckpointFunctionDirectory oldFunctionDirectory = new LiveViewCheckpointFunctionDirectory(configuration);
                LiveViewCheckpointDataSegmentWriter dataWriter = new LiveViewCheckpointDataSegmentWriter(configuration);
                LiveViewCheckpointSegmentDirectoryWriter directoryWriter = new LiveViewCheckpointSegmentDirectoryWriter(configuration);
                RootBuilders roots = new RootBuilders();
                LiveViewCheckpointTimelineWriter timelineWriter = new LiveViewCheckpointTimelineWriter(configuration)
        ) {
            metaStore.of(checkpointsDir);
            timelineReader.of(checkpointsDir);
            deltaReader.of(checkpointsDir);
            timelineWriter.of(checkpointsDir);
            directoryWriter.of(checkpointsDir);

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
            if (hasPrevious && maxTimestamp == previousEntry.maxTimestamp) {
                // The head's own timestamp group grew. Nothing to seal - see
                // BoundaryNotAboveHeadException, which the caller treats as a skipped
                // cadence rather than a failed one.
                throw BoundaryNotAboveHeadException.INSTANCE;
            }
            if (hasPrevious && maxTimestamp < previousEntry.maxTimestamp) {
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
            directoryWriter.begin(oldDirectoryRoot);
            registerPendingDirectorySegment(directoryWriter, superblock);

            long nextSegmentId = metaStore.isValid() ? superblock.nextSegmentId : 0;
            nextSegmentId = Math.max(nextSegmentId, orphanUpperBound);
            nextSegmentId = skipPublishedSegmentIds(checkpointsDir, nextSegmentId);
            final long dataSegmentId = nextSegmentId++;
            dataWriter.of(checkpointsDir, dataSegmentId);

            // A cadence seal only ever runs on changes above the current head, and
            // the chunk sharing needs that proven per row rather than assumed: an
            // unknown or overlapping minimum keeps the sharing off and writes a
            // complete image, which is what the seal did before the chunk layer.
            final boolean isStrictlyForward = hasPrevious
                    && batchMinTs != Numbers.LONG_NULL
                    && batchMinTs > previousEntry.maxTimestamp;
            final FrozenBoundary boundary;
            try (RootPreviousBoundary previousBoundary = isStrictlyForward ? new RootPreviousBoundary(
                    checkpointsDir,
                    oldFunctionDirectory,
                    oldDirectoryRoot,
                    previousEntry.maxTimestamp
            ) : null) {
                boundary = freezeBoundary(dataWriter, functions, anchorWindow, previousBoundary);
            }
            final boolean hasData = !dataWriter.isEmpty();
            final long dataSegmentBytes;
            if (hasData) {
                dataSegmentBytes = dataWriter.commit();
            } else {
                // Every ring shared its pages and nothing new was encoded. The
                // reserved id stays burned rather than reused: ids are monotonic.
                dataWriter.discard();
                dataSegmentBytes = 0;
            }
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
            registerBoundarySegments(directoryWriter, roots.writtenMetaSegments, reusedSegmentIds);

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
            final long timelineSegmentId = nextSegmentId++;
            final LiveViewCheckpointPageRef newTimelineRoot = new LiveViewCheckpointPageRef();
            timelineWriter.append(oldTimelineRoot, entry, timelineSegmentId, newTimelineRoot);
            metadataBytesAdded = checkedAdd(metadataBytesAdded, timelineWriter.getLastSegmentBytes());
            registerMetadataSegment(
                    directoryWriter,
                    timelineSegmentId,
                    timelineWriter.getLastSegmentBytes(),
                    timelineWriter.getLastSegmentPageCount()
            );
            directoryWriter.releaseMetadataPages(timelineWriter.getLastReleasedSegmentIds(), generation);

            if (hasData) {
                directoryWriter.addSegment(dataSegmentId, dataSegmentBytes, 1);
                dropSegmentId(reusedSegmentIds, dataSegmentId);
            }
            if (reusedSegmentIds.size() > 0) {
                directoryWriter.applyRootReferenceChanges(emptySegmentIds, reusedSegmentIds, generation);
            }
            nextSegmentId = skipPublishedSegmentIds(checkpointsDir, nextSegmentId);
            final long directorySegmentId = nextSegmentId++;
            final LiveViewCheckpointPageRef newDirectoryRoot = new LiveViewCheckpointPageRef();
            directoryWriter.publish(directorySegmentId, generation, newDirectoryRoot);
            metadataBytesAdded = checkedAdd(metadataBytesAdded, directoryWriter.getLastSegmentBytes());
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
            superblock.logicalStateBytes = checkedAdd(
                    metaStore.isValid() ? superblock.logicalStateBytes : 0,
                    boundary.logicalStateBytes
            );
            // A cadence seal appends above every existing key, so it writes no
            // row-position delta node and the running total carries forward.
            superblock.rowPositionDeltaBytes = metaStore.isValid() ? superblock.rowPositionDeltaBytes : 0;
            superblock.seedCursorOffset = seedCursorOffset;
            carryPendingDirectorySegment(directoryWriter, superblock, directorySegmentId);
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
                    metaStore.getWalPurgeFloor(),
                    new LiveViewCheckpointTimelineStats()
                            .of(superblock, checkedAdd(dataSegmentBytes, metadataBytesAdded)),
                    liveSegmentCount,
                    obsoleteSegmentBytes
            );
        }
    }

    /**
     * Records the metadata segment the publication's segment directory landed in,
     * which its own catalogue could not list, so the next publication registers
     * it. A publication that staged no catalogue mutation reused the previous
     * root and wrote no segment, and leaves nothing pending.
     */
    private static void carryPendingDirectorySegment(
            LiveViewCheckpointSegmentDirectoryWriter directoryWriter,
            LiveViewCheckpointSuperblock superblock,
            long directorySegmentId
    ) {
        final long segmentBytes = directoryWriter.getLastSegmentBytes();
        final boolean hasSegment = segmentBytes > 0;
        superblock.pendingDirectorySegmentId = hasSegment ? directorySegmentId : Numbers.LONG_NULL;
        superblock.pendingDirectorySegmentBytes = hasSegment ? segmentBytes : 0;
        superblock.pendingDirectorySegmentPages = hasSegment ? directoryWriter.getLastSegmentPageCount() : 0;
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
     *
     * @return true when the root did reference it
     */
    private static boolean dropSegmentId(LongList segmentIds, long segmentId) {
        for (int i = 0, n = segmentIds.size(); i < n; i++) {
            if (segmentIds.getQuick(i) == segmentId) {
                segmentIds.removeIndex(i);
                return true;
            }
        }
        return false;
    }

    private static CairoException missingRedirect(LiveViewCheckpointStatePageRef ref) {
        // The planner walked every root, so a live page in a drained segment must be
        // in the redirect. Reaching here means planning and publication disagree.
        return CairoException.critical(CairoException.LV_CHECKPOINT_TIMELINE_INVALID)
                .put("live view checkpoint compaction missing redirect [segmentId=")
                .put(ref.getSegmentId()).put(", offset=").put(ref.getOffset()).put(']');
    }

    /**
     * Narrows a previous boundary's reference to one this freeze may reuse: a
     * single whole-state image under the framing {@link #freezeStatePage} mints.
     * Anything else - a ring chunk, a page written by another codec, a reference
     * whose stored and decoded lengths disagree - comes back null, and the freeze
     * writes its own page.
     */
    private static @Nullable LiveViewCheckpointStatePageRef rawStatePageRef(
            @Nullable LiveViewCheckpointStatePageRef ref
    ) {
        if (ref == null
                || ref.isNull()
                || ref.getPageKind() != FUNCTION_STATE_PAGE_KIND
                || ref.getCodec() != RAW_CODEC
                || ref.getRowCount() != 1
                || ref.getFlags() != 0
                || ref.getStoredLength() != ref.getDecodedLength()) {
            return null;
        }
        return ref;
    }

    /**
     * Catalogues the metadata segments one boundary's build wrote, and takes each
     * out of the root reference set it is already part of. A boundary segment is
     * named by exactly one root - the boundary that wrote it - so it enters the
     * catalogue with a count of one and moves from there exactly as a data segment
     * does: the next boundary that reuses one of its pages takes a reference, and
     * retiring the last boundary naming it releases the file.
     */
    private static void registerBoundarySegments(
            LiveViewCheckpointSegmentDirectoryWriter directoryWriter,
            LongList writtenMetaSegments,
            LongList rootSegmentIds
    ) {
        for (int i = 0, n = writtenMetaSegments.size(); i < n; i += 2) {
            final long segmentId = writtenMetaSegments.getQuick(i);
            if (!dropSegmentId(rootSegmentIds, segmentId)) {
                // The root is built from the very roots that named these
                // segments, so one it does not name means the closure the root
                // publishes and the files the build wrote have diverged.
                throw CairoException.critical(CairoException.LV_CHECKPOINT_TIMELINE_INVALID)
                        .put("live view checkpoint root does not name the metadata segment its build wrote, segmentId=")
                        .put(segmentId);
            }
            directoryWriter.addSegment(
                    segmentId,
                    writtenMetaSegments.getQuick(i + 1),
                    1,
                    LiveViewCheckpointSegmentDirectory.SEGMENT_KIND_BOUNDARY
            );
        }
    }

    /**
     * Catalogues the metadata segment a tree writer just published. Its reference
     * count is the number of pages the writer wrote, because a B+ tree page is
     * named exactly once - by its parent, or by the root reference the superblock
     * carries. A writer that reused its previous root wrote no segment and is
     * skipped.
     */
    private static void registerMetadataSegment(
            LiveViewCheckpointSegmentDirectoryWriter directoryWriter,
            long segmentId,
            long segmentBytes,
            int pageCount
    ) {
        if (segmentBytes > 0) {
            directoryWriter.addSegment(
                    segmentId,
                    segmentBytes,
                    pageCount,
                    LiveViewCheckpointSegmentDirectory.SEGMENT_KIND_META
            );
        }
    }

    /**
     * Registers the segment directory segment the previous publication left
     * unregistered. Every publication owes this to the one before it: without the
     * entry, the pages this publication path-copies out of that segment have
     * nothing to be released against and the file is never reclaimed. Called
     * first because that is where it reads clearly, not because the staging order
     * matters - the releases run at {@code publish} time.
     */
    private static void registerPendingDirectorySegment(
            LiveViewCheckpointSegmentDirectoryWriter directoryWriter,
            LiveViewCheckpointSuperblock superblock
    ) {
        if (superblock.pendingDirectorySegmentId != Numbers.LONG_NULL) {
            directoryWriter.addSegment(
                    superblock.pendingDirectorySegmentId,
                    superblock.pendingDirectorySegmentBytes,
                    superblock.pendingDirectorySegmentPages,
                    LiveViewCheckpointSegmentDirectory.SEGMENT_KIND_META
            );
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
            if (!frozen.partitionsByKey.containsKey(ByteBuffer.wrap(entry.getKey()))) {
                builder.removePartition(entry.getKey());
            }
        });
    }

    /**
     * The previous boundary's whole-state page for one partition, or null when
     * its entry holds something a whole-state freeze cannot reuse - a ring entry,
     * a function-owned scalar payload beside the page, or no page at all.
     */
    private static @Nullable LiveViewCheckpointStatePageRef wholeStatePageRef(
            @Nullable LiveViewCheckpointPartitionMapEntry entry
    ) {
        if (entry == null || entry.getScalarState().length != 0 || entry.getStatePageCount() != 1) {
            return null;
        }
        return rawStatePageRef(entry.getStatePageRef(0));
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
     * {@code previousBoundary} is the boundary immediately below this one, when
     * one exists and this freeze is provably above it. A ring-shaped function
     * carries that boundary's chunk pages forward by reference and encodes only
     * the rows above it, so a seal costs the rows the batch added rather than the
     * live frame. Everything else writes one complete state image per root.
     * <p>
     * Anchor entries are the exception: one is a key plus its last-seen anchor
     * value, so they are carried to publication as values and land in the
     * anchor-map metadata pages rather than in the data segment.
     */
    private FrozenBoundary freezeBoundary(
            LiveViewCheckpointDataSegmentWriter dataWriter,
            @NotNull ObjList<WindowFunction> functions,
            @Nullable LiveViewWindow anchorWindow,
            @Nullable PreviousBoundary previousBoundary
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
            logicalStateBytes = checkedAdd(
                    logicalStateBytes,
                    freezeFunction(dataWriter, function, frozen, previousBoundary)
            );
            boundary.functions.add(frozen);
        }
        // A view every one of whose window functions is stateless seals an empty function set,
        // and that is the whole of its state: the root still records the boundary a resume
        // rolls back to, and a restore from it has nothing to put back because there was
        // nothing to take. What stays a break is a factory with no window function at all -
        // the live view eligibility gate does not admit one, so reaching here means the
        // compiled runtime the seal was handed is not the one the view was built from.
        if (functions.size() == 0) {
            throw CairoException.critical(0).put("cannot seal live view checkpoint without functions");
        }
        boundary.logicalStateBytes = logicalStateBytes;
        return boundary;
    }

    private long freezeFunction(
            LiveViewCheckpointDataSegmentWriter dataWriter,
            WindowFunction function,
            FrozenFunction frozen,
            @Nullable PreviousBoundary previousBoundary
    ) {
        final Map map = function.getPartitionMap();
        final boolean isRingShaped = function.supportsCheckpointRingState();
        if (map == null) {
            if (isRingShaped) {
                throw CairoException.critical(0)
                        .put("live view checkpoint ring state requires a partition map");
            }
            final LiveViewCheckpointStatePageRef previousScalarRef = previousBoundary == null
                    ? null
                    : rawStatePageRef(
                    previousBoundary.findScalarStatePage(frozen.identity, frozen.stateFormatVersion)
            );
            final LiveViewCheckpointStatePageRef ref =
                    freezeStatePage(dataWriter, function, null, previousBoundary, previousScalarRef);
            frozen.scalarStateRef = ref;
            return ref.getDecodedLength();
        }

        long logicalBytes = 0;
        final ColumnTypes keyTypes = function.getCheckpointKeyColumnTypes();
        final int keyStartIndex = function.getCheckpointKeyStartIndex();
        final int tombstoneIndex = function.getTombstoneValueIndex();
        final MapRecordCursor cursor = map.getCursor();
        final MapRecord record = map.getRecord();
        final LiveViewCheckpointPartitionMapEntry ringEntry =
                isRingShaped ? new LiveViewCheckpointPartitionMapEntry() : null;
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
            logicalBytes = checkedAdd(logicalBytes, keyLength);
            if (isRingShaped) {
                final LiveViewCheckpointPartitionMapEntry previous = previousBoundary == null
                        ? null
                        : previousBoundary.find(frozen.identity, frozen.stateFormatVersion, key);
                logicalBytes = checkedAdd(logicalBytes, ringSeal.seal(
                        dataWriter,
                        function,
                        value,
                        key,
                        previous,
                        previousBoundary == null ? Numbers.LONG_NULL : previousBoundary.getMaxTimestamp(),
                        ringEntry
                ));
                frozen.addPartition(ringEntry);
            } else {
                final LiveViewCheckpointPartitionMapEntry previous = previousBoundary == null
                        ? null
                        : previousBoundary.find(frozen.identity, frozen.stateFormatVersion, key);
                final LiveViewCheckpointStatePageRef stateRef = freezeStatePage(
                        dataWriter,
                        function,
                        value,
                        previousBoundary,
                        wholeStatePageRef(previous)
                );
                frozen.addPartition(key, stateRef);
                logicalBytes = checkedAdd(logicalBytes, stateRef.getDecodedLength());
            }
        }
        return logicalBytes;
    }

    /**
     * Encodes one whole-state image and names it with a page reference.
     * <p>
     * The image goes into a reusable scratch buffer rather than straight into the
     * segment, so a key the batch did not touch can be compared against the page
     * the previous boundary wrote for it and reuse that page's reference instead
     * of writing an identical copy. The elision then carries into metadata for
     * free: the partition-map entry a reused reference produces is byte-identical
     * to the one already stored, and
     * {@link LiveViewCheckpointPartitionMapWriter} drops a put whose key and value
     * both match, so neither the leaf nor its ancestors are rewritten either.
     * <p>
     * A cold key therefore costs one encode and one comparison per seal instead
     * of a state page plus its share of a full partition-map rewrite. The cost it
     * adds is the comparison read: the previous page usually sits in an older
     * segment that has to be mapped.
     */
    private LiveViewCheckpointStatePageRef freezeStatePage(
            LiveViewCheckpointDataSegmentWriter dataWriter,
            WindowFunction function,
            @Nullable MapValue value,
            @Nullable PreviousBoundary previousBoundary,
            @Nullable LiveViewCheckpointStatePageRef previousRef
    ) {
        stateBuffer.jumpTo(0);
        final LiveViewStatePageWriter pageWriter = statePageWriter.of(stateBuffer);
        function.freezeCheckpointState(pageWriter, value);
        final int bytes = checkedIntLength(pageWriter.size(), "function state");
        // After the encode: an extending put moves the buffer.
        final long address = stateBuffer.addressOf(0);
        if (previousBoundary != null
                && previousRef != null
                && previousRef.getStoredLength() == bytes
                && previousBoundary.isStatePageEqual(previousRef, address, bytes)) {
            return LiveViewCheckpointPartitionMapEntry.copyRef(previousRef);
        }
        final MemoryA sink = dataWriter.beginPage();
        sink.putBlockOfBytes(address, bytes);
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
        private final byte[] identity;
        private final byte[] keySchema;
        private final HashMap<ByteBuffer, FrozenPartition> partitionsByKey = new HashMap<>();
        private final ObjList<FrozenPartition> partitions = new ObjList<>();
        private LiveViewCheckpointStatePageRef scalarStateRef;
        private final int stateFormatVersion;

        private FrozenFunction(byte[] identity, int stateFormatVersion, byte[] keySchema) {
            this.identity = identity;
            this.stateFormatVersion = stateFormatVersion;
            this.keySchema = keySchema;
        }

        private void addPartition(byte[] key, LiveViewCheckpointStatePageRef stateRef) {
            addPartition(new FrozenPartition(
                    key,
                    new byte[0],
                    new LiveViewCheckpointStatePageRef[]{stateRef}
            ));
        }

        /**
         * Takes a ring seal's entry by copy: the seal reuses one flyweight for
         * every partition it freezes.
         */
        private void addPartition(LiveViewCheckpointPartitionMapEntry entry) {
            final LiveViewCheckpointStatePageRef[] refs =
                    new LiveViewCheckpointStatePageRef[entry.getStatePageCount()];
            for (int i = 0; i < refs.length; i++) {
                refs[i] = LiveViewCheckpointPartitionMapEntry.copyRef(entry.getStatePageRef(i));
            }
            addPartition(new FrozenPartition(
                    Arrays.copyOf(entry.getKey(), entry.getKey().length),
                    Arrays.copyOf(entry.getScalarState(), entry.getScalarState().length),
                    refs
            ));
        }

        private void addPartition(FrozenPartition partition) {
            partitionsByKey.put(ByteBuffer.wrap(partition.key), partition);
            partitions.add(partition);
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

        private void copyTo(LiveViewCheckpointPartitionMapEntry out) {
            out.of(key, scalarState, statePageRefs);
        }
    }

    /**
     * The boundary a freeze may carry unchanged chunk pages forward from: the
     * root immediately below it on the cadence path, the boundary the replay
     * captured immediately before it on the repair path.
     * <p>
     * A lookup returns null whenever the previous boundary has nothing to share -
     * no such function, a different state layout, or a partition that did not
     * exist there - and the freeze then writes that partition from empty.
     */
    private interface PreviousBoundary {

        @Nullable
        LiveViewCheckpointPartitionMapEntry find(byte[] functionIdentity, int stateFormatVersion, byte[] key);

        /**
         * The previous boundary's whole-state page for a function that keeps no
         * partition map, or null when it holds none this freeze can compare
         * against.
         */
        @Nullable
        LiveViewCheckpointStatePageRef findScalarStatePage(byte[] functionIdentity, int stateFormatVersion);

        long getMaxTimestamp();

        /**
         * Compares the {@code length} freshly encoded bytes at {@code address}
         * with the payload {@code ref} names. Answers false rather than raising
         * when that payload cannot be read: a previous boundary with nothing to
         * share costs one fresh page, which is what every freeze wrote before
         * this comparison existed.
         */
        boolean isStatePageEqual(LiveViewCheckpointStatePageRef ref, long address, int length);
    }

    /**
     * Shares against a boundary the replay froze earlier in the same repair
     * capture. Its chunks sit in the capture's own unpublished temporary
     * segment, which is exactly why the sharing path reads no data page.
     */
    private static final class CapturedPreviousBoundary implements PreviousBoundary {
        private final FrozenBoundary boundary;
        private final LiveViewCheckpointDataSegmentWriter dataWriter;
        private final LiveViewCheckpointPartitionMapEntry entry = new LiveViewCheckpointPartitionMapEntry();
        private final long maxTimestamp;

        private CapturedPreviousBoundary(
                FrozenBoundary boundary,
                long maxTimestamp,
                LiveViewCheckpointDataSegmentWriter dataWriter
        ) {
            this.boundary = boundary;
            this.maxTimestamp = maxTimestamp;
            this.dataWriter = dataWriter;
        }

        @Override
        public @Nullable LiveViewCheckpointPartitionMapEntry find(
                byte[] functionIdentity,
                int stateFormatVersion,
                byte[] key
        ) {
            final FrozenFunction function = findFunction(functionIdentity, stateFormatVersion);
            if (function == null) {
                return null;
            }
            final FrozenPartition partition = function.partitionsByKey.get(ByteBuffer.wrap(key));
            if (partition == null) {
                return null;
            }
            partition.copyTo(entry);
            return entry;
        }

        @Override
        public @Nullable LiveViewCheckpointStatePageRef findScalarStatePage(
                byte[] functionIdentity,
                int stateFormatVersion
        ) {
            final FrozenFunction function = findFunction(functionIdentity, stateFormatVersion);
            return function == null ? null : function.scalarStateRef;
        }

        @Override
        public long getMaxTimestamp() {
            return maxTimestamp;
        }

        @Override
        public boolean isStatePageEqual(LiveViewCheckpointStatePageRef ref, long address, int length) {
            // Every page a captured boundary holds went into this capture's own
            // segment: the first boundary of a repair shares against nothing, so
            // no reference the replay can hand back here names a published one.
            // Anything else is left to the freeze rather than read blind.
            if (ref.getSegmentId() != dataWriter.getSegmentId()) {
                return false;
            }
            return Vect.memeq(dataWriter.addressOfPage(ref.getOffset(), length), address, length);
        }

        private @Nullable FrozenFunction findFunction(byte[] functionIdentity, int stateFormatVersion) {
            for (int i = 0, n = boundary.functions.size(); i < n; i++) {
                final FrozenFunction function = boundary.functions.getQuick(i);
                if (function.stateFormatVersion == stateFormatVersion
                        && Arrays.equals(function.identity, functionIdentity)) {
                    return function;
                }
            }
            return null;
        }
    }

    /**
     * Signals a cadence seal whose candidate boundary lands exactly on the timeline
     * head's {@code maxTimestamp}: every row the cycle produced shared that timestamp,
     * so the group the head already covers grew rather than a new one opening above it.
     * <p>
     * A normal root may only extend the timeline strictly upwards - a restore reads a
     * root's {@code maxTimestamp} as "everything at or below this is covered" and
     * replays from one tick above it, and the seal's chunk sharing rests on the batch
     * sitting strictly above the head - so there is nothing to append. This is ordinary
     * data rather than a fault: a designated timestamp that spans two refresh cycles
     * produces it. The caller skips the seal and leaves its cadence counters open, so
     * the next cycle to reach a higher timestamp seals both cycles' rows at once.
     * <p>
     * A candidate strictly <em>below</em> the head is a different matter - it means a
     * cycle emitted output under a sealed boundary without retiring it - and keeps
     * throwing {@link CairoException}.
     */
    public static final class BoundaryNotAboveHeadException extends RuntimeException {
        public static final BoundaryNotAboveHeadException INSTANCE = new BoundaryNotAboveHeadException();

        private BoundaryNotAboveHeadException() {
            super(null, null, false, false);
        }
    }

    /**
     * Result of one physical compaction publication.
     */
    public static final class CompactionResult {
        private final long dataBytesAdded;
        private final long generation;
        private final long metadataBytesAdded;
        private final int rootsRewritten;
        private final LiveViewCheckpointTimelineStats stats;
        private final long targetSegmentId;
        private final long walPurgeFloor;

        private CompactionResult(
                long generation,
                int rootsRewritten,
                long targetSegmentId,
                long dataBytesAdded,
                long metadataBytesAdded,
                long walPurgeFloor,
                LiveViewCheckpointTimelineStats stats
        ) {
            this.generation = generation;
            this.rootsRewritten = rootsRewritten;
            this.targetSegmentId = targetSegmentId;
            this.dataBytesAdded = dataBytesAdded;
            this.metadataBytesAdded = metadataBytesAdded;
            this.walPurgeFloor = walPurgeFloor;
            this.stats = stats;
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

        /**
         * @return the number of logical roots this compaction re-versioned onto the
         * relocated pages
         */
        public int getRootsRewritten() {
            return rootsRewritten;
        }

        /**
         * @return the shape of the generation this compaction committed
         */
        public LiveViewCheckpointTimelineStats getStats() {
            return stats;
        }

        public long getTargetSegmentId() {
            return targetSegmentId;
        }

        public long getWalPurgeFloor() {
            return walPurgeFloor;
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
        private final long headRootMaxTimestamp;
        private final long metadataBytesAdded;
        private final int rootsVersioned;
        private final LiveViewCheckpointTimelineStats stats;
        private final long suffixBreakpointTimestamp;
        private final long suffixRowDelta;
        private final long walPurgeFloor;

        private RepairResult(
                long generation,
                int rootsVersioned,
                long headRootMaxTimestamp,
                long suffixRowDelta,
                long suffixBreakpointTimestamp,
                long dataBytesAdded,
                long metadataBytesAdded,
                long walPurgeFloor,
                LiveViewCheckpointTimelineStats stats
        ) {
            this.generation = generation;
            this.rootsVersioned = rootsVersioned;
            this.headRootMaxTimestamp = headRootMaxTimestamp;
            this.suffixRowDelta = suffixRowDelta;
            this.suffixBreakpointTimestamp = suffixBreakpointTimestamp;
            this.dataBytesAdded = dataBytesAdded;
            this.metadataBytesAdded = metadataBytesAdded;
            this.walPurgeFloor = walPurgeFloor;
            this.stats = stats;
        }

        public long getDataBytesAdded() {
            return dataBytesAdded;
        }

        public long getGeneration() {
            return generation;
        }

        /**
         * @return the newest logical key the spliced timeline holds, or
         * {@link Numbers#LONG_NULL} when it holds none. A splice appends no root,
         * so this is the boundary the caller's post-repair seal must clear before
         * it may claim the generation covers a later base transaction.
         */
        public long getHeadRootMaxTimestamp() {
            return headRootMaxTimestamp;
        }

        public long getMetadataBytesAdded() {
            return metadataBytesAdded;
        }

        public int getRootsVersioned() {
            return rootsVersioned;
        }

        /**
         * @return the shape of the generation this splice committed
         */
        public LiveViewCheckpointTimelineStats getStats() {
            return stats;
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

    /**
     * Result of one prefix-preserving truncate publication. When
     * {@link #isPublished()} is false no prefix survived below the floor and
     * nothing was published; every other field is unset.
     */
    public static final class TruncateResult {
        static final TruncateResult NOT_PUBLISHED = new TruncateResult(-1, 0, -1, null, false);
        private final long generation;
        private final long metadataBytesAdded;
        private final boolean published;
        private final LiveViewCheckpointTimelineStats stats;
        private final long walPurgeFloor;

        private TruncateResult(
                long generation,
                long metadataBytesAdded,
                long walPurgeFloor,
                LiveViewCheckpointTimelineStats stats,
                boolean published
        ) {
            this.generation = generation;
            this.metadataBytesAdded = metadataBytesAdded;
            this.walPurgeFloor = walPurgeFloor;
            this.stats = stats;
            this.published = published;
        }

        private TruncateResult(
                long generation,
                long metadataBytesAdded,
                long walPurgeFloor,
                LiveViewCheckpointTimelineStats stats
        ) {
            this(generation, metadataBytesAdded, walPurgeFloor, stats, true);
        }

        public long getGeneration() {
            return generation;
        }

        public long getMetadataBytesAdded() {
            return metadataBytesAdded;
        }

        /**
         * @return the shape of the generation this truncate committed, or null
         * when nothing was published
         */
        public LiveViewCheckpointTimelineStats getStats() {
            return stats;
        }

        public long getWalPurgeFloor() {
            return walPurgeFloor;
        }

        public boolean isPublished() {
            return published;
        }
    }

    public static final class Result {
        private final long checkpointId;
        private final long dataBytesAdded;
        private final long generation;
        private final long liveSegmentCount;
        private final long logicalStateBytes;
        private final long metadataBytesAdded;
        private final long obsoleteSegmentBytes;
        private final LiveViewCheckpointTimelineStats stats;
        private final long walPurgeFloor;

        private Result(
                long generation,
                long checkpointId,
                long logicalStateBytes,
                long dataBytesAdded,
                long metadataBytesAdded,
                long walPurgeFloor,
                LiveViewCheckpointTimelineStats stats,
                long liveSegmentCount,
                long obsoleteSegmentBytes
        ) {
            this.generation = generation;
            this.checkpointId = checkpointId;
            this.logicalStateBytes = logicalStateBytes;
            this.dataBytesAdded = dataBytesAdded;
            this.metadataBytesAdded = metadataBytesAdded;
            this.walPurgeFloor = walPurgeFloor;
            this.stats = stats;
            this.liveSegmentCount = liveSegmentCount;
            this.obsoleteSegmentBytes = obsoleteSegmentBytes;
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

        /**
         * @return data segments a current logical root named when this seal's
         * lifecycle reconciliation swept the catalogue, or
         * {@link Numbers#LONG_NULL} when this seal ran no sweep. Reconciliation
         * runs once per writer per directory, so a steady cadence reports it on
         * the first seal alone
         */
        public long getLiveSegmentCount() {
            return liveSegmentCount;
        }

        public long getLogicalStateBytes() {
            return logicalStateBytes;
        }

        public long getMetadataBytesAdded() {
            return metadataBytesAdded;
        }

        /**
         * @return bytes of retired data segments that sweep left on disk, or
         * {@link Numbers#LONG_NULL} when this seal ran no sweep
         */
        public long getObsoleteSegmentBytes() {
            return obsoleteSegmentBytes;
        }

        /**
         * @return the shape of the generation this seal committed
         */
        public LiveViewCheckpointTimelineStats getStats() {
            return stats;
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
         * @param entry                  the boundary's current logical entry,
         *                               including the root version being superseded
         * @param functions              the live compiled window functions, standing
         *                               at exactly {@code entry.maxTimestamp}
         * @param anchorWindow           the live anchor window, or null
         * @param effectiveLvRowPosition replay-derived cumulative live-view row
         *                               position at the boundary
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
            // The replay feeds rows in canonical timestamp order and captures at
            // each boundary it crosses, so every row behind this boundary and
            // ahead of the previous one sits strictly above the previous one's
            // maxTimestamp - the same proof the cadence seal needs from its
            // caller, here by construction.
            final PreviousBoundary previousBoundary = size == 0
                    ? null
                    : new CapturedPreviousBoundary(
                    boundaries.getQuick(size - 1),
                    boundaries.getQuick(size - 1).oldEntry.maxTimestamp,
                    dataWriter
            );
            final FrozenBoundary boundary = freezeBoundary(dataWriter, functions, anchorWindow, previousBoundary);
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

        /**
         * @return the published segment's exact length, or 0 when the capture's
         * boundaries all shared their pages and encoded nothing new
         */
        private long commitData() {
            if (!isDataOpen || dataWriter.isEmpty()) {
                if (isDataOpen) {
                    dataWriter.discard();
                }
                isDataPublished = true;
                return 0;
            }
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
     * Shares against the published root immediately below this seal. Resolves a
     * function root once and then probes its persistent partition map per key.
     */
    private final class RootPreviousBoundary implements PreviousBoundary, Closeable {
        private final Path checkpointsDir = new Path();
        private final long[] dataReaderSegmentIds = new long[PREVIOUS_DATA_READER_CACHE_SIZE];
        private final LiveViewCheckpointDataSegmentReader[] dataReaders =
                new LiveViewCheckpointDataSegmentReader[PREVIOUS_DATA_READER_CACHE_SIZE];
        private final LiveViewCheckpointPartitionMapEntry entry = new LiveViewCheckpointPartitionMapEntry();
        private final LiveViewCheckpointFunctionDirectory functionDirectory;
        private final LiveViewCheckpointFunctionRoot functionRoot =
                new LiveViewCheckpointFunctionRoot(configuration);
        private final long maxTimestamp;
        private final LiveViewCheckpointPartitionMapReader partitionReader =
                new LiveViewCheckpointPartitionMapReader(configuration);
        private final LiveViewCheckpointPageRef partitionRootRef = new LiveViewCheckpointPageRef();
        private final LiveViewCheckpointPageRef functionRootRef = new LiveViewCheckpointPageRef();
        private final LiveViewCheckpointStatePageRef scalarStateRef = new LiveViewCheckpointStatePageRef();
        private final LiveViewCheckpointSegmentDirectoryReader segmentDirectory =
                new LiveViewCheckpointSegmentDirectoryReader(configuration);
        private final LiveViewCheckpointSegmentDirectoryEntry segmentDirectoryEntry =
                new LiveViewCheckpointSegmentDirectoryEntry();
        private int dataReaderClock;
        private boolean isUnreadablePageLogged;
        private byte[] resolvedIdentity;

        private RootPreviousBoundary(
                Path checkpointsDir,
                LiveViewCheckpointFunctionDirectory functionDirectory,
                LiveViewCheckpointPageRef segmentDirectoryRootRef,
                long maxTimestamp
        ) {
            this.checkpointsDir.of(checkpointsDir);
            this.functionDirectory = functionDirectory;
            this.maxTimestamp = maxTimestamp;
            this.partitionReader.of(checkpointsDir);
            // The published catalogue is what bounds every comparison read: a page
            // is only opened against the exact file length its entry records.
            this.segmentDirectory.of(checkpointsDir, segmentDirectoryRootRef);
            Arrays.fill(dataReaderSegmentIds, -1);
        }

        @Override
        public void close() {
            for (int i = 0; i < PREVIOUS_DATA_READER_CACHE_SIZE; i++) {
                dataReaders[i] = Misc.free(dataReaders[i]);
                dataReaderSegmentIds[i] = -1;
            }
            Misc.free(functionRoot);
            Misc.free(partitionReader);
            Misc.free(segmentDirectory);
            Misc.free(checkpointsDir);
        }

        @Override
        public @Nullable LiveViewCheckpointPartitionMapEntry find(
                byte[] functionIdentity,
                int stateFormatVersion,
                byte[] key
        ) {
            if (!resolveFunction(functionIdentity, stateFormatVersion)) {
                return null;
            }
            return partitionReader.find(partitionRootRef, key, entry) ? entry : null;
        }

        @Override
        public @Nullable LiveViewCheckpointStatePageRef findScalarStatePage(
                byte[] functionIdentity,
                int stateFormatVersion
        ) {
            if (!resolveFunction(functionIdentity, stateFormatVersion)) {
                return null;
            }
            return scalarStateRef.isNull() ? null : scalarStateRef;
        }

        @Override
        public long getMaxTimestamp() {
            return maxTimestamp;
        }

        @Override
        public boolean isStatePageEqual(LiveViewCheckpointStatePageRef ref, long address, int length) {
            try {
                if (!segmentDirectory.find(ref.getSegmentId(), segmentDirectoryEntry)
                        || segmentDirectoryEntry.referenceCount <= 0
                        || segmentDirectoryEntry.fileLength <= 0) {
                    return false;
                }
                final LiveViewCheckpointDataSegmentReader reader =
                        readerFor(ref.getSegmentId(), segmentDirectoryEntry.fileLength);
                reader.openPage(ref, FUNCTION_STATE_PAGE_KIND, RAW_CODEC, 0, 1, Integer.MAX_VALUE);
                return reader.getPageStoredLength() == length
                        && Vect.memeq(reader.getPageAddress(), address, length);
            } catch (CairoException e) {
                // The seal writes its own image instead, so a predecessor it cannot read
                // costs bytes rather than the publication - and the root that still names
                // that page is a restore's problem, which restore reports on its own. One
                // line per boundary: a segment that has gone missing fails every key.
                if (!isUnreadablePageLogged) {
                    isUnreadablePageLogged = true;
                    LOG.error().$("could not read live view checkpoint state page for reuse [segmentId=")
                            .$(ref.getSegmentId())
                            .$(", offset=").$(ref.getOffset())
                            .$(", error=").$safe(e.getFlyweightMessage())
                            .I$();
                }
                return false;
            }
        }

        private LiveViewCheckpointDataSegmentReader readerFor(long segmentId, long fileLength) {
            for (int i = 0; i < PREVIOUS_DATA_READER_CACHE_SIZE; i++) {
                if (dataReaderSegmentIds[i] == segmentId) {
                    return dataReaders[i];
                }
            }
            final int slot = dataReaderClock;
            dataReaderClock = dataReaderClock + 1 == PREVIOUS_DATA_READER_CACHE_SIZE ? 0 : dataReaderClock + 1;
            if (dataReaders[slot] == null) {
                dataReaders[slot] = new LiveViewCheckpointDataSegmentReader(configuration);
            }
            // Invalidated before the open, which resets the reader and can then throw:
            // the slot must not go on advertising the segment it held against a reader
            // that no longer holds it.
            dataReaderSegmentIds[slot] = -1;
            dataReaders[slot].of(checkpointsDir, segmentId, fileLength);
            dataReaderSegmentIds[slot] = segmentId;
            return dataReaders[slot];
        }

        /**
         * Binds the previous root's map and scalar references for one function.
         * A seal freezes each function's partitions in one run, so the resolution
         * is memoised on the identity and the root is read once per function.
         */
        private boolean resolveFunction(byte[] functionIdentity, int stateFormatVersion) {
            if (Arrays.equals(resolvedIdentity, functionIdentity)) {
                return true;
            }
            resolvedIdentity = null;
            partitionRootRef.clear();
            scalarStateRef.clear();
            if (!functionDirectory.find(functionIdentity, functionRootRef)) {
                return false;
            }
            functionRoot.of(checkpointsDir, functionRootRef);
            if (functionRoot.getStateFormatVersion() != stateFormatVersion) {
                return false;
            }
            functionRoot.getPartitionMapRootRef(partitionRootRef);
            functionRoot.getScalarStateRef(scalarStateRef);
            resolvedIdentity = functionIdentity;
            return true;
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
        private final LiveViewCheckpointPageRef redirectAnchorRootRef = new LiveViewCheckpointPageRef();
        private final LiveViewCheckpointRoot redirectCheckpointRoot =
                new LiveViewCheckpointRoot(configuration);
        private final LiveViewCheckpointFunctionDirectory redirectFunctionDirectory =
                new LiveViewCheckpointFunctionDirectory(configuration);
        private final LiveViewCheckpointPageRef redirectFunctionDirectoryRef = new LiveViewCheckpointPageRef();
        private final LiveViewCheckpointPageRef redirectNewFunctionRootRef = new LiveViewCheckpointPageRef();
        private final LiveViewCheckpointPageRef redirectOldFunctionRootRef = new LiveViewCheckpointPageRef();
        private final LiveViewCheckpointPageRef redirectPartitionMapRoot = new LiveViewCheckpointPageRef();
        private final LiveViewCheckpointStatePageRef redirectScalarRef = new LiveViewCheckpointStatePageRef();
        /**
         * {@code (segmentId, fileLength)} of every metadata segment the boundary
         * built last wrote, for the caller to catalogue. Reset per boundary, so a
         * repair's per-root reference transaction sees only its own.
         */
        private final LongList writtenMetaSegments = new LongList();
        private long metadataBytesAdded;
        private long nextSegmentId;

        /**
         * Rebuilds the checkpoint root at {@code oldRootRef}, swapping every state
         * page reference that names a drained segment for its relocated reference in
         * the plan's target segment. Anchor state and every function whose pages all
         * survive are reused by reference; only the function roots that name a
         * drained page are rewritten.
         * <p>
         * Returns true when the root named a drained segment and was rebuilt -
         * {@code rootRefOut} names the new root, {@code removedSegmentIdsOut} holds
         * the old root's referenced segments and {@code addedSegmentIdsOut} the new
         * root's. Returns false when it named none and is reused as-is, leaving the
         * out parameters untouched.
         */
        private boolean buildRedirectedRoot(
                LiveViewCheckpointPageRef oldRootRef,
                long definitionTxn,
                LiveViewCheckpointCompactionPlan plan,
                LiveViewCheckpointPageRef rootRefOut,
                LongList removedSegmentIdsOut,
                LongList addedSegmentIdsOut
        ) {
            writtenMetaSegments.clear();
            redirectCheckpointRoot.of(checkpointsDir, oldRootRef);
            if (redirectCheckpointRoot.getDefinitionTxn() != definitionTxn) {
                throw CairoException.critical(CairoException.LV_CHECKPOINT_TIMELINE_INVALID)
                        .put("live view checkpoint compaction root definition identity mismatch");
            }
            // A root that names no drained segment shares every page it holds, so it
            // keeps its existing root by reference without reading its functions.
            boolean referencesDrained = false;
            for (int s = 0, n = redirectCheckpointRoot.getSegmentIdCount(); s < n; s++) {
                if (plan.isDrainedSegment(redirectCheckpointRoot.getSegmentId(s))) {
                    referencesDrained = true;
                    break;
                }
            }
            if (!referencesDrained) {
                return false;
            }
            removedSegmentIdsOut.clear();
            for (int s = 0, n = redirectCheckpointRoot.getSegmentIdCount(); s < n; s++) {
                removedSegmentIdsOut.add(redirectCheckpointRoot.getSegmentId(s));
            }

            // Anchor entries carry no data-segment state, so the anchor root is
            // reused by reference untouched across a compaction.
            redirectCheckpointRoot.getAnchorRootRef(redirectAnchorRootRef);
            redirectCheckpointRoot.getFunctionDirectoryRef(redirectFunctionDirectoryRef);
            redirectFunctionDirectory.of(checkpointsDir, redirectFunctionDirectoryRef);

            nextSegmentId = skipPublishedSegmentIds(checkpointsDir, nextSegmentId);
            checkpointRootBuilder.begin(
                    checkpointsDir,
                    redirectCheckpointRoot.getCheckpointId(),
                    redirectCheckpointRoot.getMaxTimestamp(),
                    definitionTxn,
                    redirectAnchorRootRef
            );
            for (int i = 0, n = redirectFunctionDirectory.size(); i < n; i++) {
                redirectFunctionDirectory.getRootRef(i, redirectOldFunctionRootRef);
                if (buildRedirectedFunctionRoot(redirectOldFunctionRootRef, plan, redirectNewFunctionRootRef)) {
                    checkpointRootBuilder.addFunction(redirectNewFunctionRootRef);
                } else {
                    checkpointRootBuilder.addFunction(redirectOldFunctionRootRef);
                }
            }

            nextSegmentId = skipPublishedSegmentIds(checkpointsDir, nextSegmentId);
            final long rootSegmentId = nextSegmentId++;
            checkpointRootBuilder.build(rootSegmentId, rootRefOut);
            metadataBytesAdded = checkedAdd(metadataBytesAdded, checkpointRootBuilder.getLastSegmentBytes());
            writtenMetaSegments.add(rootSegmentId, checkpointRootBuilder.getLastSegmentBytes());
            checkpointRootBuilder.getReferencedSegmentIds(addedSegmentIdsOut);
            return true;
        }

        /**
         * Rebuilds one function root, swapping the scalar state reference and every
         * partition state reference that names a drained segment for its relocated
         * reference. A function that names no drained page is left untouched and this
         * returns false, so the checkpoint root reuses it by reference.
         *
         * @return true when the function was rewritten ({@code newRootRefOut} names
         * the new function root)
         */
        private boolean buildRedirectedFunctionRoot(
                LiveViewCheckpointPageRef oldFunctionRootRef,
                LiveViewCheckpointCompactionPlan plan,
                LiveViewCheckpointPageRef newRootRefOut
        ) {
            oldFunctionRoot.of(checkpointsDir, oldFunctionRootRef);
            boolean referencesDrained = false;
            for (int s = 0, n = oldFunctionRoot.getSegmentUseCountSize(); s < n; s++) {
                if (plan.isDrainedSegment(oldFunctionRoot.getSegmentId(s))) {
                    referencesDrained = true;
                    break;
                }
            }
            if (!referencesDrained) {
                return false;
            }
            oldFunctionRoot.getScalarStateRef(redirectScalarRef);
            oldFunctionRoot.getPartitionMapRootRef(redirectPartitionMapRoot);
            functionRootBuilder.of(
                    checkpointsDir,
                    oldFunctionRootRef,
                    oldFunctionRoot.getFunctionIdentity(),
                    oldFunctionRoot.getStateFormatVersion(),
                    oldFunctionRoot.getKeySchema()
            );
            if (!redirectScalarRef.isNull() && plan.isDrainedSegment(redirectScalarRef.getSegmentId())) {
                final LiveViewCheckpointStatePageRef target = plan.redirect(redirectScalarRef);
                if (target == null) {
                    throw missingRedirect(redirectScalarRef);
                }
                functionRootBuilder.setScalarStateRef(target);
            }
            oldPartitionReader.iterateAll(redirectPartitionMapRoot, entry -> redirectPartition(entry, plan));
            nextSegmentId = skipPublishedSegmentIds(checkpointsDir, nextSegmentId);
            final long functionSegmentId = nextSegmentId++;
            functionRootBuilder.build(functionSegmentId, newRootRefOut);
            metadataBytesAdded = checkedAdd(metadataBytesAdded, functionRootBuilder.getLastSegmentBytes());
            writtenMetaSegments.add(functionSegmentId, functionRootBuilder.getLastSegmentBytes());
            return true;
        }

        /**
         * Stages a putPartition mutation redirecting {@code entry}'s drained state
         * pages onto their relocated references, or leaves the partition untouched
         * (reused by reference in the partition-map copy-on-write) when it names no
         * drained page.
         */
        private void redirectPartition(
                LiveViewCheckpointPartitionMapEntry entry,
                LiveViewCheckpointCompactionPlan plan
        ) {
            final int count = entry.getStatePageCount();
            LiveViewCheckpointStatePageRef[] newRefs = null;
            for (int i = 0; i < count; i++) {
                final LiveViewCheckpointStatePageRef ref = entry.getStatePageRef(i);
                if (plan.isDrainedSegment(ref.getSegmentId())) {
                    final LiveViewCheckpointStatePageRef target = plan.redirect(ref);
                    if (target == null) {
                        throw missingRedirect(ref);
                    }
                    if (newRefs == null) {
                        newRefs = new LiveViewCheckpointStatePageRef[count];
                        for (int j = 0; j < i; j++) {
                            newRefs[j] = LiveViewCheckpointPartitionMapEntry.copyRef(entry.getStatePageRef(j));
                        }
                    }
                    newRefs[i] = LiveViewCheckpointPartitionMapEntry.copyRef(target);
                } else if (newRefs != null) {
                    newRefs[i] = LiveViewCheckpointPartitionMapEntry.copyRef(entry.getStatePageRef(i));
                }
            }
            if (newRefs != null) {
                functionRootBuilder.putPartition(entry.getKey(), entry.getScalarState(), newRefs);
            }
        }

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
            writtenMetaSegments.clear();
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
                final long anchorSegmentId = nextSegmentId++;
                anchorRootBuilder.build(anchorSegmentId, anchorRootRef);
                metadataBytesAdded = checkedAdd(metadataBytesAdded, anchorRootBuilder.getLastSegmentBytes());
                writtenMetaSegments.add(anchorSegmentId, anchorRootBuilder.getLastSegmentBytes());
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
                final long functionSegmentId = nextSegmentId++;
                functionRootBuilder.build(functionSegmentId, functionRootRef);
                metadataBytesAdded = checkedAdd(metadataBytesAdded, functionRootBuilder.getLastSegmentBytes());
                writtenMetaSegments.add(functionSegmentId, functionRootBuilder.getLastSegmentBytes());
                checkpointRootBuilder.addFunction(functionRootRef);
            }

            nextSegmentId = skipPublishedSegmentIds(checkpointsDir, nextSegmentId);
            final long rootSegmentId = nextSegmentId++;
            checkpointRootBuilder.build(rootSegmentId, rootRefOut);
            metadataBytesAdded = checkedAdd(metadataBytesAdded, checkpointRootBuilder.getLastSegmentBytes());
            writtenMetaSegments.add(rootSegmentId, checkpointRootBuilder.getLastSegmentBytes());
            checkpointRootBuilder.getReferencedSegmentIds(referencedSegmentIdsOut);
        }

        @Override
        public void close() {
            Misc.free(anchorRootBuilder);
            Misc.free(checkpointRootBuilder);
            Misc.free(functionRootBuilder);
            Misc.free(oldFunctionRoot);
            Misc.free(oldPartitionReader);
            Misc.free(redirectCheckpointRoot);
            Misc.free(redirectFunctionDirectory);
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
