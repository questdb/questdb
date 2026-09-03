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
import io.questdb.cairo.map.MapKey;
import io.questdb.cairo.map.MapRecord;
import io.questdb.cairo.map.MapRecordCursor;
import io.questdb.cairo.map.MapValue;
import io.questdb.cairo.vm.MemoryCARWImpl;
import io.questdb.cairo.vm.api.MemoryA;
import io.questdb.griffin.engine.window.WindowFunction;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.BoolList;
import io.questdb.std.IntList;
import io.questdb.std.IntObjHashMap;
import io.questdb.std.LongList;
import io.questdb.std.MemoryTag;
import io.questdb.std.MemoryTracker;
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
import java.util.Arrays;

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
    /**
     * Throws where {@link #TEST_FAIL_AFTER_METADATA_PUBLISH} would, but only in
     * {@link #publishCompaction}, so a seal on the same writer gets through and the
     * compaction that follows it does not. That is the failure shape no
     * reconciliation ever sees: a seal failure re-arms the reconciliation on the
     * next one, while a compaction or repair failure re-arms nothing at all, so the
     * files it renamed into place are named by neither the catalogue nor the id
     * ceiling a later seal steps over.
     */
    @TestOnly
    public static final int TEST_FAIL_AFTER_COMPACTION_METADATA_PUBLISH = 4;
    @TestOnly
    public static final int TEST_FAIL_AFTER_DATA_PUBLISH = 1;
    @TestOnly
    public static final int TEST_FAIL_AFTER_METADATA_PUBLISH = 2;
    /**
     * Throws once the superblock has committed, so the caller observes a failed
     * publication over a durably advanced generation. Unlike the stages above,
     * {@link #publishCompaction} is the only path that honours it -
     * compaction is the only publication that stages a data segment an abort
     * could unlink.
     */
    @TestOnly
    public static final int TEST_FAIL_AFTER_SUPERBLOCK_PUBLISH = 3;

    private static final Log LOG = LogFactory.getLog(LiveViewCheckpointTimelineStoreWriter.class);
    private static final byte[] NO_BYTES = new byte[0];
    /**
     * What an inlined entry names instead of a state page. The image sits in the
     * leaf's scalar slot, so the entry references no data page at all - which is
     * why reference accounting and physical compaction need no case for it: both
     * walk the reference array, and this one is empty.
     */
    private static final LiveViewCheckpointStatePageRef[] NO_STATE_PAGES = new LiveViewCheckpointStatePageRef[0];
    /**
     * Published data segments one seal may hold mapped at once while it compares
     * cold keys against their previous pages. Elision spreads a boundary's live
     * references over the segments each key was last written into, so a wide key
     * set walks more segments than one; a set wider than the cache re-maps rather
     * than failing, which still costs less than re-imaging every key.
     */
    private static final int PREVIOUS_DATA_READER_CACHE_SIZE = 8;
    /**
     * Capacity ceiling of the reusable freeze scratch buffers: 2^19 pages of
     * 4 KiB, exactly 2 GiB. A state image's page length is int-typed, so no
     * valid image needs more; an encode that tries to grow past this fails at
     * the allocation instead of after producing an image no page can store.
     */
    private static final int SCRATCH_MAX_PAGES = 524_288;
    private static final long SCRATCH_PAGE_SIZE = 4096;
    // A fail-closed physical scan remains as a safety net, but at one pass per
    // 1024 GC sweeps it stays below the benchmark's P99 population.
    private static final int ORPHAN_SAFETY_SCAN_INTERVAL = 1024;

    // The runtime-only members whose own predecessor root the freeze cannot build on, and
    // the scratch their shared walk fans into. Kept per instance so a seal allocates none
    // of it once the widths have settled; see freezeGroupedFunctions.
    private FreezeScratch activeScratch;
    private final CairoConfiguration configuration;
    // Read-only argument of a cadence seal's reference transaction, which only
    // ever adds; kept per instance so the seal path allocates nothing for it.
    private final LiveViewCheckpointCompactionScratch compactionScratch;
    private final LongList emptySegmentIds = new LongList();
    // The pre-publication identity probe's own store and paths. They are used
    // before a publication leases its shells and released before it does, so they
    // never overlap one - and they keep a seal from opening a catalogue shell of
    // its own on the way in.
    private final LiveViewCheckpointMetaStore identityCheckMetaStore;
    private final Path identityCheckPath = new Path();
    private final Path directoryScratchPath = new Path();
    private final Path segmentIdProbePath = new Path();
    private final MissingPartitionVisitor missingPartitionVisitor = new MissingPartitionVisitor();
    /**
     * The seal's view of the published root below it. A repair chain resolves the
     * same shape against its own shell ({@link ChainedPreviousBoundary}), because a
     * post-splice frontier seal runs while that capture is still open.
     */
    private final RootPreviousBoundary sealPreviousBoundary;
    // The key domain one bucket's shared walk produces, common to every member in it.
    private final LiveViewCheckpointPartitionMapObjectPool partitionMapObjectPool =
            new LiveViewCheckpointPartitionMapObjectPool();
    private final PublicationScratch publicationShells;
    private final FreezeScratch publicationScratch = new FreezeScratch();
    private final ObjList<FreezeScratch> repairScratchPool = new ObjList<>();
    private boolean isPartitionMapObjectPoolLeased;
    private boolean isPublicationShellsLeased;
    @TestOnly
    private long lastBoundaryPartitionPuts;
    // Catalogue entries a reconciliation's sweep left naming an unlinked file,
    // per checkpoint directory, waiting for the next seal of that view to carry
    // them out of the tree. A view whose seal is skipped keeps its proposal.
    private final LiveViewCheckpointLifecycleState lifecycleState;
    private final boolean ownsLifecycleState;
    private final LiveViewCheckpointRingSeal ringSeal;
    private final RetirementQueueSeedVisitor retirementQueueSeedVisitor = new RetirementQueueSeedVisitor();
    private final RedirectPartitionVisitor redirectPartitionVisitor = new RedirectPartitionVisitor();
    private final RedirectTimelineVisitor redirectTimelineVisitor = new RedirectTimelineVisitor();
    // One whole-state image at a time, encoded here before the freeze decides
    // whether it has to become a page at all.
    private final LiveViewStatePageWriter statePageWriter = new LiveViewStatePageWriter();
    private final TruncateVisitor truncateVisitor = new TruncateVisitor();
    @TestOnly
    private int testFailureStage;

    public LiveViewCheckpointTimelineStoreWriter(@NotNull CairoConfiguration configuration) {
        this(configuration, new LiveViewCheckpointLifecycleState(), true);
    }

    public LiveViewCheckpointTimelineStoreWriter(
            @NotNull CairoConfiguration configuration,
            @NotNull LiveViewCheckpointLifecycleState lifecycleState
    ) {
        this(configuration, lifecycleState, false);
    }

    private LiveViewCheckpointTimelineStoreWriter(
            @NotNull CairoConfiguration configuration,
            @NotNull LiveViewCheckpointLifecycleState lifecycleState,
            boolean ownsLifecycleState
    ) {
        this.configuration = configuration;
        this.lifecycleState = lifecycleState;
        this.ownsLifecycleState = ownsLifecycleState;
        this.compactionScratch = new LiveViewCheckpointCompactionScratch(configuration);
        this.identityCheckMetaStore = new LiveViewCheckpointMetaStore(configuration);
        this.ringSeal = new LiveViewCheckpointRingSeal(configuration, null);
        // Built last: every shell they own read the configuration this constructor
        // has only just assigned, so a field initializer would see it null.
        this.publicationShells = new PublicationScratch();
        this.sealPreviousBoundary = new RootPreviousBoundary();
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
     * @param memoryTracker    the sealed view's refresh workload tracker, which
     *                         the freeze scratch buffers charge while this
     *                         append runs, or null to account against process
     *                         totals only. The append frees the scratch and
     *                         detaches the tracker before returning on every
     *                         path, so no capacity and no charge outlive it -
     *                         the writer is shared across every view its worker
     *                         seals
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
            long lifecycleIdentity,
            boolean primaryOwner,
            long maxTimestamp,
            long effectiveLvRowPosition,
            long batchMinTs,
            long seedCursorOffset,
            @Nullable MemoryTracker memoryTracker,
            @Nullable LiveViewSymbolIdRegistry partitionKeyRegistry
    ) {
        if (!primaryOwner) {
            throw CairoException.critical(0).put("replica must not publish a live view checkpoint timeline");
        }
        LiveViewCheckpointLayout.timelinePath(identityCheckPath, checkpointsDir);
        if (configuration.getFilesFacade().exists(identityCheckPath.$())) {
            final LiveViewCheckpointMetaStore metaStore = identityCheckMetaStore;
            try {
                metaStore.of(checkpointsDir);
                if (metaStore.isValid()) {
                    final LiveViewCheckpointSuperblock superblock = metaStore.getSuperblock();
                    if (superblock.definitionTxn != definitionTxn || superblock.historyEpoch != historyEpoch) {
                        throw CairoException.critical(CairoException.LV_CHECKPOINT_TIMELINE_INVALID)
                                .put("live view checkpoint append definition identity mismatch");
                    }
                }
            } finally {
                metaStore.detach();
            }
        }
        boolean epochRetry = false;
        bindScratchBuffers(memoryTracker);
        try {
            while (true) {
                long orphanUpperBound = 0;
                // A view created in this process reconciles here rather than at
                // startup, so this is its only chance to learn what its catalogue
                // holds. LONG_NULL when the reconciliation was skipped or adopted no
                // generation, which leaves whatever an earlier sweep reported.
                long liveSegmentCount = Numbers.LONG_NULL;
                long obsoleteSegmentBytes = Numbers.LONG_NULL;
                if (!lifecycleState.isReconciled(lifecycleIdentity)) {
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
                    // The sweep unlinked these files but could not rewrite the
                    // catalogue, because only a publication may. This seal is that
                    // publication.
                    final LongList retirable = reconciliation.getRetirableSegmentIds();
                    if (retirable.size() > 0) {
                        lifecycleState.replacePendingRetirements(lifecycleIdentity, retirable);
                    }
                    if (reconciliation.getFailedOrphanCount() == 0
                            && reconciliation.getFailedPurgeCount() == 0
                            && reconciliation.getFailedRepairCount() == 0) {
                        lifecycleState.markReconciled(lifecycleIdentity);
                    }
                }
                try {
                    final Result result = append0(
                            checkpointsDir,
                            functions,
                            anchorWindow,
                            definitionTxn,
                            createdLvSeqTxn,
                            normalizedBaseSeqTxn,
                            coveredLvSeqTxn,
                            historyEpoch,
                            lifecycleIdentity,
                            maxTimestamp,
                            effectiveLvRowPosition,
                            batchMinTs,
                            seedCursorOffset,
                            orphanUpperBound,
                            liveSegmentCount,
                            obsoleteSegmentBytes,
                            lifecycleState.getPendingRetirements(lifecycleIdentity),
                            partitionKeyRegistry
                    );
                    lifecycleState.clearPendingRetirements(lifecycleIdentity);
                    return result;
                } catch (HistoryEpochChangedException e) {
                    if (epochRetry) {
                        throw CairoException.critical(0).put("could not replace live view checkpoint history epoch");
                    }
                    epochRetry = true;
                } catch (BoundaryNotAboveHeadException e) {
                    // The append refused before touching a file, and the reconciliation
                    // above still holds, so the key stays: this seal is skipped, not
                    // failed, and the entries it would have retired wait for the next one.
                    throw e;
                } catch (RuntimeException | Error e) {
                    lifecycleState.clearReconciled(lifecycleIdentity);
                    lifecycleState.clearPendingRetirements(lifecycleIdentity);
                    throw e;
                }
            }
        } finally {
            releaseScratchBuffers();
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
     *
     * @param outputKeys    {@code Q}, the keys this repair's replay describes, or null
     *                      when it describes every live key. The capture takes its own
     *                      copy: the plan it comes from is refilled by the next repair
     *                      this worker runs, while a capture may be parked across turns
     * @param memoryTracker the repaired view's refresh workload tracker, which
     *                      the freeze scratch buffers charge from here until the
     *                      capture closes, or null to account against process
     *                      totals only. {@link RepairCapture#close()} frees the
     *                      scratch and detaches the tracker on the publish and
     *                      the discard path alike
     * @param chained       true to freeze and publish the boundaries as a chain: each
     *                      one imaging only the keys the replay touched since the one
     *                      below it, and each one's root seeded from that one's new
     *                      root rather than from its own pre-repair root. It is what
     *                      makes a K-boundary repair cost the keys its replay touched
     *                      once instead of K complete walks of the live domain, and the
     *                      caller owes two things for it: a replay that reconstructs
     *                      every key (so {@code outputKeys} must be null), and a
     *                      runtime standing at the predecessor root when the first
     *                      boundary is captured, so the keys it has marked dirty since
     *                      are exactly the ones that moved
     */
    public RepairCapture beginRepair(
            @Transient @NotNull Path checkpointsDir,
            @Transient @Nullable LiveViewCheckpointOutputKeyDomain outputKeys,
            @Nullable MemoryTracker memoryTracker,
            boolean chained
    ) {
        if (chained && outputKeys != null) {
            // A key-domain repair leaves every key outside Q to the old root, which is
            // the one thing a chain cannot do: its boundaries are seeded from each
            // other rather than from the roots they replace, so an unimaged key would
            // take the predecessor's entry instead of its own boundary's.
            throw CairoException.critical(0)
                    .put("live view checkpoint repair cannot chain over a partial key domain");
        }
        ensureDirectories(checkpointsDir);
        final LiveViewCheckpointMetaStore metaStore = identityCheckMetaStore;
        try {
            metaStore.of(checkpointsDir);
            if (!metaStore.isValid()) {
                throw CairoException.critical(0)
                        .put("cannot repair a live view checkpoint timeline with no valid generation");
            }
            final LiveViewCheckpointSuperblock superblock = metaStore.getSuperblock();
            final FreezeScratch scratch = acquireRepairScratch(memoryTracker);
            try {
                return new RepairCapture(
                        checkpointsDir,
                        skipPublishedSegmentIds(checkpointsDir, superblock.nextSegmentId),
                        superblock.generation,
                        superblock.timelineRootRef,
                        superblock.rowPositionDeltaRootRef,
                        outputKeys,
                        chained,
                        scratch
                );
            } catch (Throwable th) {
                releaseRepairScratch(scratch);
                throw th;
            }
        } finally {
            metaStore.detach();
        }
    }

    @Override
    public void close() {
        activeScratch = null;
        Misc.free(compactionScratch);
        Misc.free(directoryScratchPath);
        Misc.free(identityCheckMetaStore);
        Misc.free(identityCheckPath);
        Misc.free(segmentIdProbePath);
        Misc.free(publicationShells);
        Misc.free(publicationScratch);
        sealPreviousBoundary.free();
        Misc.freeObjList(repairScratchPool);
        Misc.free(ringSeal);
        partitionMapObjectPool.clear();
        if (ownsLifecycleState) {
            lifecycleState.clear();
        }
    }

    /**
     * @return partition puts the last boundary this writer built staged into its
     * function roots. A key whose frozen state matches the one the predecessor root
     * already holds is short-circuited before it reaches a root builder, so this is
     * the count of keys a seal really re-imaged.
     * <p>
     * It exists because losing that short-circuit changes nothing observable in what
     * gets published: the partition-map writer drops an equal put of its own accord
     * and reuses the old tree root either way. What it would cost is a mutation, an
     * entry copy and a tree descent per live key on every full-scan seal - real work,
     * invisible in the artifacts, and only measurable here.
     */
    LiveViewCheckpointCompactionScratch getCompactionScratch() {
        return compactionScratch;
    }

    @TestOnly
    public int getCompactionCandidateIdentityForTest() {
        return compactionScratch.getCandidateIdentityForTest();
    }

    @TestOnly
    public int getCompactionLastLivePageCountForTest() {
        return compactionScratch.getLastLivePageCountForTest();
    }

    @TestOnly
    public LiveViewCheckpointLifecycleState getLifecycleStateForTest() {
        return lifecycleState;
    }

    @TestOnly
    public int getCompactionLastLiveSegmentCountForTest() {
        return compactionScratch.getLastLiveSegmentCountForTest();
    }

    @TestOnly
    public int getCompactionLastSelectedSegmentCountForTest() {
        return compactionScratch.getLastSelectedSegmentCountForTest();
    }

    @TestOnly
    public int getCompactionLastTargetPageCountForTest() {
        return compactionScratch.getLastTargetPageCountForTest();
    }

    @TestOnly
    public int getCompactionOpenReaderCountForTest() {
        return compactionScratch.getOpenReaderCountForTest();
    }

    @TestOnly
    public void setCompactionTestFailAfterReaderOpenCount(int count) {
        compactionScratch.setTestFailAfterReaderOpenCount(count);
    }

    @TestOnly
    public void setCompactionTestFailAfterRepackedPageCount(int count) {
        compactionScratch.setTestFailAfterRepackedPageCount(count);
    }

    @TestOnly
    public int getCompactionPlanIdentityForTest() {
        return compactionScratch.getPlanIdentityForTest();
    }

    @TestOnly
    public int getCompactionReaderShellCountForTest() {
        return compactionScratch.getReaderShellCountForTest();
    }

    @TestOnly
    public Object getCompactionReaderShellForTest(int index) {
        return compactionScratch.getReaderShellForTest(index);
    }

    @TestOnly
    public int getCompactionVisitorShellIdentityForTest(int index) {
        return compactionScratch.getVisitorShellIdentityForTest(index);
    }

    @TestOnly
    public boolean isCompactionVisitorShellStateClearForTest() {
        return compactionScratch.isVisitorShellStateClearForTest();
    }

    @TestOnly
    public int getRedirectRefWidthLookupCountForTest() {
        return publicationShells.roots.getRedirectRefWidthLookupCountForTest();
    }

    @TestOnly
    public int getRootBuilderVisitorShellIdentityForTest(int index) {
        switch (index) {
            case 0:
                return System.identityHashCode(redirectTimelineVisitor);
            case 1:
                return System.identityHashCode(redirectPartitionVisitor);
            default:
                return System.identityHashCode(missingPartitionVisitor);
        }
    }

    @TestOnly
    public boolean isRootBuilderVisitorShellStateClearForTest() {
        return redirectTimelineVisitor.roots == null
                && redirectTimelineVisitor.addedSegmentIds == null
                && redirectTimelineVisitor.changedEntries == null
                && redirectTimelineVisitor.directoryWriter == null
                && redirectTimelineVisitor.newRootRef == null
                && redirectTimelineVisitor.plan == null
                && redirectTimelineVisitor.removedSegmentIds == null
                && redirectPartitionVisitor.roots == null
                && redirectPartitionVisitor.plan == null
                && missingPartitionVisitor.roots == null
                && missingPartitionVisitor.frozen == null;
    }

    @TestOnly
    public long getLastBoundaryPartitionPuts() {
        return lastBoundaryPartitionPuts;
    }

    @TestOnly
    public int getPartitionMapObjectPoolIdentityForTest() {
        return System.identityHashCode(partitionMapObjectPool);
    }

    @TestOnly
    public int getFirstRetainedPartitionMapNodeIdentityForTest() {
        return partitionMapObjectPool.getFirstRetainedNodeIdentityForTest();
    }

    @TestOnly
    public int getRetainedPartitionMapObjectCountForTest() {
        return partitionMapObjectPool.getRetainedObjectCount();
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
            long lifecycleIdentity,
            boolean primaryOwner,
            @Nullable MemoryTracker memoryTracker,
            @NotNull LiveViewCheckpointCompactionPlan plan
    ) {
        if (!primaryOwner) {
            throw CairoException.critical(0).put("replica must not publish a live view checkpoint timeline");
        }
        boolean hasPriorOrphanRisk;
        final PublicationScratch shells = acquirePublicationShells(memoryTracker);
        try {
            final LiveViewCheckpointMetaStore metaStore = shells.metaStore;
            final LiveViewCheckpointTimelineReader timelineReader = shells.timelineReader;
            final LiveViewCheckpointSegmentDirectoryWriter directoryWriter = shells.directoryWriter;
            final RootBuilders roots = shells.roots;
            final LiveViewCheckpointTimelineWriter timelineWriter = shells.timelineWriter;
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
            hasPriorOrphanRisk = lifecycleState.beginPublication(lifecycleIdentity);
            if (superblock.generation != plan.getGeneration()) {
                throw CairoException.critical(0)
                        .put("live view checkpoint timeline moved under the compaction plan")
                        .put(" [planned=").put(plan.getGeneration())
                        .put(", current=").put(superblock.generation).put(']');
            }

            final long generation = checkedIncrement(superblock.generation, "generation");
            final LiveViewCheckpointPageRef oldTimelineRoot = copyInto(superblock.timelineRootRef, shells.oldTimelineRoot);
            final LiveViewCheckpointPageRef oldDirectoryRoot = copyInto(superblock.segmentDirectoryRootRef, shells.oldDirectoryRoot);
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
            final ObjList<LiveViewCheckpointTimelineEntry> changedEntries = shells.changedEntries;
            final LongList removedSegmentIds = shells.removedSegmentIds;
            final LongList addedSegmentIds = shells.addedSegmentIds;
            final LiveViewCheckpointPageRef newRootRef = shells.newRootRef;
            removedSegmentIds.clear();
            addedSegmentIds.clear();
            final int targetSegmentRootRefs = roots.redirectTimelineEntries(
                    timelineReader,
                    oldTimelineRoot,
                    definitionTxn,
                    plan,
                    newRootRef,
                    removedSegmentIds,
                    addedSegmentIds,
                    changedEntries,
                    directoryWriter,
                    generation,
                    targetSegmentId
            );

            if (targetSegmentRootRefs == 0) {
                // A non-empty plan whose pages no surviving root names is an
                // inconsistency between planning and publication; refuse rather
                // than leave the committed target segment referenced by nobody.
                throw CairoException.critical(CairoException.LV_CHECKPOINT_TIMELINE_INVALID)
                        .put("live view checkpoint compaction plan redirected no live root");
            }
            nextSegmentId = roots.nextSegmentId;
            long metadataBytesAdded = roots.metadataBytesAdded;
            directoryWriter.addSegment(targetSegmentId, plan.getTargetSegmentBytes(), targetSegmentRootRefs);

            final int splicedCount = roots.lastChangedEntryCount;
            nextSegmentId = skipPublishedSegmentIds(checkpointsDir, nextSegmentId);
            final long timelineSegmentId = nextSegmentId++;
            final LiveViewCheckpointPageRef newTimelineRoot = shells.newTimelineRoot;
            timelineWriter.splice(oldTimelineRoot, changedEntries, splicedCount, timelineSegmentId, newTimelineRoot);
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
            final LiveViewCheckpointPageRef newDirectoryRoot = shells.newDirectoryRoot;
            directoryWriter.publish(directorySegmentId, generation, newDirectoryRoot);
            hasPriorOrphanRisk |= persistRetirementQueue(
                    shells,
                    checkpointsDir,
                    definitionTxn,
                    historyEpoch,
                    lifecycleIdentity,
                    directoryWriter,
                    newDirectoryRoot,
                    generation
            );
            metadataBytesAdded = checkedAdd(metadataBytesAdded, directoryWriter.getLastSegmentBytes());
            if (testFailureStage == TEST_FAIL_AFTER_METADATA_PUBLISH
                    || testFailureStage == TEST_FAIL_AFTER_COMPACTION_METADATA_PUBLISH) {
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
            lifecycleState.finishPublication(lifecycleIdentity, hasPriorOrphanRisk);
            if (testFailureStage == TEST_FAIL_AFTER_SUPERBLOCK_PUBLISH) {
                throw CairoException.critical(0).put("test failure after live view checkpoint superblock publication");
            }

            return shells.compactionResult.of(
                    generation,
                    splicedCount,
                    targetSegmentId,
                    plan.getTargetSegmentBytes(),
                    metadataBytesAdded,
                    metaStore.getWalPurgeFloor(),
                    shells.compactionStats
                            .of(superblock, checkedAdd(plan.getTargetSegmentBytes(), metadataBytesAdded))
            );
        } finally {
            releasePublicationShells();
        }
    }

    /**
     * Publishes one localized out-of-order repair as a timeline range splice.
     * <p>
     * The splice preserves every logical key: the captured boundaries keep their
     * {@code checkpointId}, {@code maxTimestamp} and {@code createdLvSeqTxn} and
     * receive a new root version plus the replay-derived position, while the
     * prefix and the converged suffix keep their existing payload roots by page
     * reference. It preserves every <i>partition</i> key too: a capture carrying an
     * output key domain re-versions the entries of the keys its replay describes and
     * leaves the rest of each root as it found them.
     * <p>
     * {@code suffixRowDelta} is the replacement's total output-row
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
            long lifecycleIdentity,
            boolean primaryOwner,
            long highTsExclusive,
            long suffixRowDelta,
            @Nullable LiveViewSymbolIdRegistry partitionKeyRegistry
    ) {
        if (!primaryOwner) {
            throw CairoException.critical(0).put("replica must not publish a live view checkpoint timeline");
        }
        capture.validateAgainst(highTsExclusive);
        final Path checkpointsDir = capture.checkpointsDir;
        final int boundaryCount = capture.size();
        boolean hasPriorOrphanRisk;

        final PublicationScratch shells = acquirePublicationShells(capture.scratch.memoryTracker);
        try {
            final LiveViewCheckpointMetaStore metaStore = shells.metaStore;
            final LiveViewCheckpointTimelineReader timelineReader = shells.timelineReader;
            final LiveViewCheckpointRowPositionDeltaReader deltaReader = shells.deltaReader;
            final LiveViewCheckpointRoot oldCheckpointRoot = shells.oldCheckpointRoot;
            final LiveViewCheckpointFunctionDirectory oldFunctionDirectory = shells.oldFunctionDirectory;
            final LiveViewCheckpointRoot seedCheckpointRoot = shells.seedCheckpointRoot;
            final LiveViewCheckpointFunctionDirectory seedFunctionDirectory = shells.seedFunctionDirectory;
            final LiveViewCheckpointSegmentDirectoryWriter directoryWriter = shells.directoryWriter;
            final RootBuilders roots = shells.roots;
            final LiveViewCheckpointTimelineWriter timelineWriter = shells.timelineWriter;
            final LiveViewCheckpointRowPositionDeltaWriter deltaWriter = shells.deltaWriter;
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
            hasPriorOrphanRisk = lifecycleState.beginPublication(lifecycleIdentity);
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
            final LiveViewCheckpointPageRef oldTimelineRoot = copyInto(superblock.timelineRootRef, shells.oldTimelineRoot);
            final LiveViewCheckpointPageRef oldDeltaRoot = copyInto(superblock.rowPositionDeltaRootRef, shells.oldDeltaRoot);
            final LiveViewCheckpointPageRef oldDirectoryRoot =
                    copyInto(superblock.segmentDirectoryRootRef, shells.oldDirectoryRoot);
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
            final ObjList<LiveViewCheckpointTimelineEntry> newEntries = shells.entries(boundaryCount);
            final LongList removedSegmentIds = shells.removedSegmentIds;
            final LongList addedSegmentIds = shells.addedSegmentIds;
            final LongList batchedReferenceDeltas = shells.batchedReferenceDeltas;
            final LiveViewCheckpointPageRef oldStateRootRef = shells.oldStateRootRef;
            final LiveViewCheckpointPageRef oldFunctionDirectoryRef = shells.oldFunctionDirectoryRef;
            final LiveViewCheckpointPageRef oldKeyDictionaryRef = shells.oldKeyDictionaryRef;
            final LiveViewCheckpointPageRef newRootRef = shells.newRootRef;
            removedSegmentIds.clear();
            addedSegmentIds.clear();
            batchedReferenceDeltas.clear();
            // The root each boundary's builders start from. For a capture that does not
            // chain it is the boundary's own pre-repair root, read inside the loop. For
            // one that does it is the previous link: the published predecessor for
            // boundary 0, and the root this loop built for boundary i - 1 after that.
            // The freeze imaged only the keys the replay touched between those two, so
            // the tree its puts go into has to be the one holding the rest - the chain's,
            // not the stale one this boundary replaces.
            final LiveViewCheckpointPageRef seedRootRef = shells.seedRootRef;
            seedRootRef.clear();
            if (capture.isChained() && capture.hasPredecessor) {
                copy(capture.predecessorEntry.rootRef, seedRootRef);
            }
            // Roots that actually name the capture's segment. A boundary whose
            // rings all carried the previous boundary's chunks forward names
            // nothing in it, and counting it would leave the segment referenced
            // after every root that reads it is gone.
            int captureSegmentRootRefs = 0;
            // Signed: a re-versioned root can hold less state than the one it
            // replaces, so the generation's logical total moves either way.
            long logicalStateBytesDelta = 0;
            // The customer repair is a non-chained, partial-key capture. Its
            // roots are independent, so every state root can be written in one
            // pass and every checkpoint root in a second pass. The chained
            // whole-domain route keeps the per-boundary fallback: its boundary
            // i + 1 must read boundary i's newly committed root.
            final BatchedRepairRoots batchedRoots = !capture.isChained()
                    && capture.outputKeys != null
                    && boundaryCount > 0
                    ? roots.buildRepairRootsBatched(capture, definitionTxn, partitionKeyRegistry)
                    : null;
            if (batchedRoots != null) {
                registerAggregateBoundarySegment(
                        directoryWriter,
                        batchedRoots.stateSegmentId,
                        batchedRoots.stateSegmentBytes,
                        batchedRoots.referencedSegmentIds,
                        batchedRoots.boundaryCount
                );
                registerAggregateBoundarySegment(
                        directoryWriter,
                        batchedRoots.rootSegmentId,
                        batchedRoots.rootSegmentBytes,
                        batchedRoots.referencedSegmentIds,
                        batchedRoots.boundaryCount
                );
            }
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
                final LiveViewCheckpointFunctionDirectory seedDirectory;
                if (!capture.isChained()) {
                    oldCheckpointRoot.getStateRootRef(oldStateRootRef);
                    oldCheckpointRoot.getFunctionDirectoryRef(oldFunctionDirectoryRef);
                    oldFunctionDirectory.of(checkpointsDir, oldFunctionDirectoryRef);
                    oldCheckpointRoot.getKeyDictionaryRef(oldKeyDictionaryRef);
                    seedDirectory = oldFunctionDirectory;
                } else if (seedRootRef.isNull()) {
                    // Nothing under the chain: the repaired interval starts below every
                    // boundary the timeline held. The freeze knew it - it had no
                    // incremental base either, and froze this boundary complete - so the
                    // builders start from empty.
                    oldStateRootRef.clear();
                    oldKeyDictionaryRef.clear();
                    seedDirectory = null;
                } else {
                    seedCheckpointRoot.of(checkpointsDir, seedRootRef);
                    seedCheckpointRoot.getStateRootRef(oldStateRootRef);
                    seedCheckpointRoot.getFunctionDirectoryRef(oldFunctionDirectoryRef);
                    seedFunctionDirectory.of(checkpointsDir, oldFunctionDirectoryRef);
                    // The chain's own predecessor dictionary, not this boundary's stale
                    // pre-repair one - the next boundary must path-copy the chunks THIS
                    // one just wrote, the same reasoning seedRootRef itself already
                    // carries for the state/function roots.
                    seedCheckpointRoot.getKeyDictionaryRef(oldKeyDictionaryRef);
                    seedDirectory = seedFunctionDirectory;
                }
                if (batchedRoots == null) {
                    roots.buildRoot(
                            boundary,
                            oldStateRootRef,
                            seedDirectory,
                            capture.outputKeys,
                            oldEntry.checkpointId,
                            oldEntry.maxTimestamp,
                            definitionTxn,
                            oldKeyDictionaryRef,
                            partitionKeyRegistry,
                            newRootRef,
                            addedSegmentIds
                    );
                } else {
                    copy(batchedRoots.rootRefs.getQuick(i), newRootRef);
                    addedSegmentIds.clear();
                    addedSegmentIds.add(batchedRoots.referencedSegmentIds.getQuick(i));
                }
                if (capture.isChained()) {
                    // The next boundary builds on this one. Copied after buildRoot, which
                    // is what wrote the root the reference names.
                    copy(newRootRef, seedRootRef);
                }
                // The old root released every data segment it referenced and the
                // new one takes its own; a segment no current root names any more
                // retires at this generation and the purge job unlinks it once no
                // reader can reach it. The aggregate route validates each root's
                // closure here but applies its sorted net deltas once below.
                removedSegmentIds.clear();
                for (int s = 0, n = oldCheckpointRoot.getSegmentIdCount(); s < n; s++) {
                    removedSegmentIds.add(oldCheckpointRoot.getSegmentId(s));
                }
                if (dropSegmentId(addedSegmentIds, capture.dataSegmentId)) {
                    captureSegmentRootRefs++;
                }
                if (batchedRoots == null) {
                    registerBoundarySegments(directoryWriter, roots.writtenMetaSegments, addedSegmentIds);
                }
                if (batchedRoots == null) {
                    directoryWriter.applyRootReferenceChanges(removedSegmentIds, addedSegmentIds, generation);
                } else {
                    accumulateReferenceDeltas(batchedReferenceDeltas, removedSegmentIds, -1);
                    accumulateReferenceDeltas(batchedReferenceDeltas, addedSegmentIds, 1);
                }

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
                // A key-domain splice images only the keys the replay describes and
                // leaves every other one exactly as the old root wrote it, so what the
                // freeze counted is a share of the boundary rather than the whole of
                // it. Recomputing the whole would need the old root's per-key state
                // sizes in the freeze's own units - a ring entry's logical size is the
                // row stream it holds rather than the pages it stores - so the boundary
                // keeps the total it already had instead of shedding every key the
                // repair did not touch.
                final long logicalStateBytes = capture.outputKeys != null
                        ? oldEntry.logicalStateBytes
                        : boundary.logicalStateBytes;
                final LiveViewCheckpointTimelineEntry newEntry = newEntries.getQuick(i).of(
                        oldEntry.maxTimestamp,
                        oldEntry.checkpointId,
                        oldEntry.createdLvSeqTxn,
                        baseLvRowPosition,
                        logicalStateBytes
                );
                newEntry.rootRef.of(newRootRef.getSegmentId(), newRootRef.getOffset(), newRootRef.getLength());
                logicalStateBytesDelta += logicalStateBytes - oldEntry.logicalStateBytes;
            }
            if (batchedRoots != null) {
                directoryWriter.applyRootReferenceDeltas(batchedReferenceDeltas, generation);
            }
            nextSegmentId = roots.nextSegmentId;
            long metadataBytesAdded = roots.metadataBytesAdded;
            if (dataSegmentBytes > 0) {
                directoryWriter.addSegment(capture.dataSegmentId, dataSegmentBytes, captureSegmentRootRefs);
            }

            final LiveViewCheckpointPageRef newTimelineRoot = shells.newTimelineRoot;
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
            final LiveViewCheckpointTimelineEntry headEntry = shells.headEntry;
            final long headRootMaxTimestamp = timelineReader.last(oldTimelineRoot, headEntry)
                    ? headEntry.maxTimestamp
                    : Numbers.LONG_NULL;

            // The breakpoint is resolved against the OLD tree on purpose: a splice
            // preserves every key, so the first suffix key is the same in both, and
            // reading it here needs no page from the segment just written.
            final LiveViewCheckpointPageRef newDeltaRoot = shells.newDeltaRoot;
            copy(oldDeltaRoot, newDeltaRoot);
            long suffixBreakpointTimestamp = Numbers.LONG_NULL;
            long rowPositionDeltaBytesAdded = 0;
            if (suffixRowDelta != 0) {
                final LiveViewCheckpointTimelineEntry suffixEntry = shells.suffixEntry;
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
            final LiveViewCheckpointPageRef newDirectoryRoot = shells.newDirectoryRoot;
            directoryWriter.publish(directorySegmentId, generation, newDirectoryRoot);
            hasPriorOrphanRisk |= persistRetirementQueue(
                    shells,
                    checkpointsDir,
                    definitionTxn,
                    historyEpoch,
                    lifecycleIdentity,
                    directoryWriter,
                    newDirectoryRoot,
                    generation
            );
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
            lifecycleState.finishPublication(lifecycleIdentity, hasPriorOrphanRisk);

            return shells.repairResult.of(
                    generation,
                    boundaryCount,
                    headRootMaxTimestamp,
                    suffixRowDelta,
                    suffixBreakpointTimestamp,
                    dataSegmentBytes,
                    metadataBytesAdded,
                    metaStore.getWalPurgeFloor(),
                    shells.repairStats
                            .of(superblock, checkedAdd(dataSegmentBytes, metadataBytesAdded))
            );
        } finally {
            releasePublicationShells();
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
            long lifecycleIdentity,
            long floorTimestamp,
            boolean primaryOwner
    ) {
        if (!primaryOwner) {
            throw CairoException.critical(0).put("replica must not publish a live view checkpoint timeline");
        }
        boolean hasPriorOrphanRisk;
        final PublicationScratch shells = acquirePublicationShells(null);
        try {
            final LiveViewCheckpointMetaStore metaStore = shells.metaStore;
            final LiveViewCheckpointTimelineReader timelineReader = shells.timelineReader;
            final LiveViewCheckpointRoot oldCheckpointRoot = shells.oldCheckpointRoot;
            final LiveViewCheckpointSegmentDirectoryWriter directoryWriter = shells.directoryWriter;
            final LiveViewCheckpointTimelineWriter timelineWriter = shells.timelineWriter;
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
            hasPriorOrphanRisk = lifecycleState.beginPublication(lifecycleIdentity);

            final LiveViewCheckpointPageRef oldTimelineRoot = copyInto(superblock.timelineRootRef, shells.oldTimelineRoot);
            final LiveViewCheckpointPageRef oldDirectoryRoot =
                    copyInto(superblock.segmentDirectoryRootRef, shells.oldDirectoryRoot);

            // No boundary below the floor: there is no prefix to preserve, so
            // publish nothing and let the caller retire the whole timeline.
            final LiveViewCheckpointTimelineEntry probe = shells.probeEntry;
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
            final LongList removedSegmentIds = shells.removedSegmentIds;
            removedSegmentIds.clear();
            truncateVisitor.of(checkpointsDir, oldCheckpointRoot, directoryWriter, removedSegmentIds, definitionTxn, generation);
            final long droppedLogicalStateBytes;
            final long droppedBoundaryCount;
            try {
                timelineReader.range(oldTimelineRoot, floorTimestamp, Long.MAX_VALUE, truncateVisitor);
                droppedLogicalStateBytes = truncateVisitor.droppedLogicalStateBytes;
                droppedBoundaryCount = truncateVisitor.droppedBoundaryCount;
            } finally {
                truncateVisitor.clearBindings();
            }

            long nextSegmentId = skipPublishedSegmentIds(checkpointsDir, superblock.nextSegmentId);
            final long timelineSegmentId = nextSegmentId++;
            final LiveViewCheckpointPageRef newTimelineRoot = shells.newTimelineRoot;
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
            final LiveViewCheckpointPageRef newDirectoryRoot = shells.newDirectoryRoot;
            directoryWriter.publish(directorySegmentId, generation, newDirectoryRoot);
            hasPriorOrphanRisk |= persistRetirementQueue(
                    shells,
                    checkpointsDir,
                    definitionTxn,
                    historyEpoch,
                    lifecycleIdentity,
                    directoryWriter,
                    newDirectoryRoot,
                    generation
            );
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
            superblock.logicalStateBytes = checkedAdd(superblock.logicalStateBytes, -droppedLogicalStateBytes);
            superblock.retiredCheckpointCount = checkedAdd(superblock.retiredCheckpointCount, droppedBoundaryCount);
            // A truncate leaves no mid-sweep resume point behind.
            superblock.seedCursorOffset = Numbers.LONG_NULL;
            carryPendingDirectorySegment(directoryWriter, superblock, directorySegmentId);
            copy(newTimelineRoot, superblock.timelineRootRef);
            // The row-position delta root carries forward unchanged: dropping the
            // suffix moves no surviving prefix key's cumulative recovery position.
            copy(newDirectoryRoot, superblock.segmentDirectoryRootRef);
            metaStore.publish();
            lifecycleState.finishPublication(lifecycleIdentity, hasPriorOrphanRisk);

            return shells.truncateResult.of(
                    generation,
                    metadataBytesAdded,
                    metaStore.getWalPurgeFloor(),
                    shells.truncateStats.of(superblock, metadataBytesAdded),
                    probe.maxTimestamp,
                    probe.checkpointId
            );
        } finally {
            releasePublicationShells();
        }
    }

    @TestOnly
    public void setTestFailureStage(int testFailureStage) {
        this.testFailureStage = testFailureStage;
    }

    /**
     * Unlinks every catalogued segment the current generation no longer reaches
     * and every final-name file no generation ever catalogued, and stages the
     * catalogue entries the pass leaves naming nothing for the next seal of this
     * directory to remove.
     * <p>
     * The second half is what a publication that renamed its segments into place
     * and then failed leaves behind. Nothing but the catalogue can name those
     * files: only {@code append} re-arms the reconciliation that would have read
     * the id ceiling naming them, so a failed compaction or repair
     * publication left its segments where they were and the next seal's id skip
     * stepped over them and put them out of the ceiling rule's reach for good.
     * {@link LiveViewCheckpointLifecycle#reconcile} applies the same catalogue rule
     * over every generation it adopts, so the two differ in when they run rather
     * than in what they decide, and this is the one that does not wait for a
     * restart. See {@link LiveViewCheckpointLifecycle#purgeUncataloguedSegments}.
     * <p>
     * This is the reclamation half of {@link LiveViewCheckpointLifecycle#reconcile}
     * on its own, without the epoch, repair-descriptor and orphan rules that only a
     * directory nobody has published under yet needs. A worker reconciles a
     * directory once - at its first seal of it - so without this pass the segments
     * every later seal, repair and compaction publication supersedes
     * wait for a restart before their bytes come back, and so do their catalogue
     * entries.
     * <p>
     * The pass publishes no generation of its own, which is exactly why the entry
     * removals cannot travel with it: the catalogue is copy-on-write and named by
     * the superblock, so only a publication may rewrite it. The proposal is held
     * per checkpoint directory and re-derived by the next sweep, so a seal that is
     * skipped, refused or crashes loses nothing.
     * <p>
     * The caller must serialize this with publication and pin acquisition exactly
     * as it serializes reconciliation - the live-view integration runs it on the
     * refresh worker, after the seal whose cadence fired it.
     */
    public SweepResult sweep(
            @Transient @NotNull Path checkpointsDir,
            long definitionTxn,
            long historyEpoch,
            long lifecycleIdentity,
            boolean primaryOwner
    ) {
        if (!primaryOwner) {
            return SweepResult.NOT_SWEPT;
        }
        final PublicationScratch shells = acquirePublicationShells(null);
        try {
            final LiveViewCheckpointMetaStore metaStore = shells.metaStore;
            metaStore.of(checkpointsDir);
            if (!metaStore.isValid()) {
                return SweepResult.NOT_SWEPT;
            }
            final LiveViewCheckpointSuperblock superblock = metaStore.getSuperblock();
            if (superblock.definitionTxn != definitionTxn || superblock.historyEpoch != historyEpoch) {
                // A generation of some other history epoch owns this directory.
                // Reconciliation retires it whole rather than collecting under it,
                // so this pass leaves every file where it is.
                return SweepResult.NOT_SWEPT;
            }
            final LiveViewCheckpointDataStore.PurgeResult purge;
            final LiveViewCheckpointDataStore dataStore = shells.dataStore;
            try {
                dataStore.of(checkpointsDir);
                purge = dataStore.purge();
            } finally {
                dataStore.detach();
            }
            // The catalogue decides the fate of every segment a generation ever
            // named; this decides the fate of the files it never named at all. The
            // two are disjoint by construction and the second is the half no
            // reconciliation can do late, because the ceiling it would have read is
            // gone by then.
            int removedOrphans = 0;
            int failedOrphans = 0;
            int physicalEntriesVisited = 0;
            final int sweepsSinceScan = lifecycleState.incrementSweepsSinceOrphanScan(
                    lifecycleIdentity
            );
            if (!lifecycleState.isOrphanScanCompleted(lifecycleIdentity)
                    || lifecycleState.isOrphanScanNeeded(lifecycleIdentity)
                    || purge.requiresPhysicalOrphanScan()
                    || sweepsSinceScan >= ORPHAN_SAFETY_SCAN_INTERVAL) {
                final LiveViewCheckpointLifecycle.CleanupStats orphans =
                        LiveViewCheckpointLifecycle.purgeUncataloguedSegments(
                                configuration,
                                checkpointsDir,
                                superblock,
                                true
                        );
                removedOrphans = orphans.getRemovedCount();
                failedOrphans = orphans.getFailedCount();
                physicalEntriesVisited = orphans.getVisitedCount();
                lifecycleState.markOrphanScanCompleted(
                        lifecycleIdentity,
                        failedOrphans != 0
                );
            }
            // The sweep re-proposes every entry whose file is already gone, so this
            // list supersedes whatever an earlier sweep left pending rather than
            // adding to it: an entry missing from it is one a publication has
            // already carried out of the tree.
            final LongList retirable = purge.getRetirableSegmentIds();
            if (retirable.size() > 0) {
                lifecycleState.replacePendingRetirements(lifecycleIdentity, retirable);
            } else {
                lifecycleState.clearPendingRetirements(lifecycleIdentity);
            }
            return shells.sweepResult.of(
                    purge.getPurgedSegmentCount(),
                    purge.getPurgedBytes(),
                    purge.getFailedSegmentCount(),
                    retirable.size(),
                    purge.getLiveSegmentCount(),
                    purge.getObsoleteBytes(),
                    removedOrphans,
                    failedOrphans,
                    purge.getQueueEntriesVisited(),
                    purge.getCatalogueEntriesVisited(),
                    physicalEntriesVisited
            );
        } finally {
            releasePublicationShells();
        }
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
            long lifecycleIdentity,
            long maxTimestamp,
            long effectiveLvRowPosition,
            long batchMinTs,
            long seedCursorOffset,
            long orphanUpperBound,
            long liveSegmentCount,
            long obsoleteSegmentBytes,
            @Nullable LongList retirableSegmentIds,
            @Nullable LiveViewSymbolIdRegistry partitionKeyRegistry
    ) {
        boolean hasPriorOrphanRisk;
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

        final PublicationScratch shells = acquirePublicationShells(publicationScratch.memoryTracker);
        try {
            final LiveViewCheckpointMetaStore metaStore = shells.metaStore;
            final LiveViewCheckpointTimelineReader timelineReader = shells.timelineReader;
            final LiveViewCheckpointRowPositionDeltaReader deltaReader = shells.deltaReader;
            final LiveViewCheckpointRoot oldCheckpointRoot = shells.oldCheckpointRoot;
            final LiveViewCheckpointFunctionDirectory oldFunctionDirectory = shells.oldFunctionDirectory;
            final LiveViewCheckpointDataSegmentWriter dataWriter = shells.dataWriter;
            final LiveViewCheckpointSegmentDirectoryWriter directoryWriter = shells.directoryWriter;
            final RootBuilders roots = shells.roots;
            final LiveViewCheckpointTimelineWriter timelineWriter = shells.timelineWriter;
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
            hasPriorOrphanRisk = lifecycleState.beginPublication(lifecycleIdentity);
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

            final LiveViewCheckpointPageRef oldTimelineRoot = copyInto(superblock.timelineRootRef, shells.oldTimelineRoot);
            final LiveViewCheckpointPageRef oldDeltaRoot = copyInto(superblock.rowPositionDeltaRootRef, shells.oldDeltaRoot);
            final LiveViewCheckpointPageRef oldDirectoryRoot =
                    copyInto(superblock.segmentDirectoryRootRef, shells.oldDirectoryRoot);
            final LiveViewCheckpointTimelineEntry previousEntry = shells.previousEntry;
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

            final LiveViewCheckpointPageRef oldStateRootRef = shells.oldStateRootRef;
            final LiveViewCheckpointPageRef oldFunctionDirectoryRef = shells.oldFunctionDirectoryRef;
            final LiveViewCheckpointPageRef oldKeyDictionaryRef = shells.oldKeyDictionaryRef;
            oldStateRootRef.clear();
            oldFunctionDirectoryRef.clear();
            oldKeyDictionaryRef.clear();
            if (hasPrevious) {
                oldCheckpointRoot.of(checkpointsDir, previousEntry.rootRef);
                if (oldCheckpointRoot.getDefinitionTxn() != definitionTxn) {
                    throw CairoException.critical(CairoException.LV_CHECKPOINT_TIMELINE_INVALID)
                            .put("live view checkpoint root definition identity mismatch");
                }
                oldCheckpointRoot.getStateRootRef(oldStateRootRef);
                oldCheckpointRoot.getFunctionDirectoryRef(oldFunctionDirectoryRef);
                oldFunctionDirectory.of(checkpointsDir, oldFunctionDirectoryRef);
                oldCheckpointRoot.getKeyDictionaryRef(oldKeyDictionaryRef);
            }
            directoryWriter.begin(oldDirectoryRoot);
            registerPendingDirectorySegment(directoryWriter, superblock);
            retireCatalogueEntries(directoryWriter, retirableSegmentIds);

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
            try (RootPreviousBoundary previousBoundary = isStrictlyForward ? sealPreviousBoundary.of(
                    checkpointsDir,
                    oldFunctionDirectory,
                    oldDirectoryRoot,
                    oldStateRootRef,
                    previousEntry.maxTimestamp
            ) : null) {
                // The generation the seal is building on top of. onCheckpointPersisted
                // hands the runtime the generation this seal publishes, so the two match
                // only when no other publication slipped in between.
                boundary = freezeBoundary(
                        dataWriter,
                        functions,
                        anchorWindow,
                        previousBoundary,
                        null,
                        metaStore.isValid() ? superblock.generation : Numbers.LONG_NULL
                );
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
            final LiveViewCheckpointPageRef checkpointRootRef = shells.checkpointRootRef;
            final LongList reusedSegmentIds = shells.reusedSegmentIds;
            reusedSegmentIds.clear();
            roots.buildRoot(
                    boundary,
                    oldStateRootRef,
                    hasPrevious ? oldFunctionDirectory : null,
                    null,
                    checkpointId,
                    maxTimestamp,
                    definitionTxn,
                    oldKeyDictionaryRef,
                    partitionKeyRegistry,
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
            final LiveViewCheckpointTimelineEntry entry = shells.entries(1).getQuick(0)
                    .of(maxTimestamp, checkpointId, createdLvSeqTxn, baseLvRowPosition, boundary.logicalStateBytes);
            entry.rootRef.of(
                    checkpointRootRef.getSegmentId(),
                    checkpointRootRef.getOffset(),
                    checkpointRootRef.getLength()
            );
            nextSegmentId = skipPublishedSegmentIds(checkpointsDir, nextSegmentId);
            final long timelineSegmentId = nextSegmentId++;
            final LiveViewCheckpointPageRef newTimelineRoot = shells.newTimelineRoot;
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
            final LiveViewCheckpointPageRef newDirectoryRoot = shells.newDirectoryRoot;
            directoryWriter.publish(directorySegmentId, generation, newDirectoryRoot);
            hasPriorOrphanRisk |= persistRetirementQueue(
                    shells,
                    checkpointsDir,
                    definitionTxn,
                    historyEpoch,
                    lifecycleIdentity,
                    directoryWriter,
                    newDirectoryRoot,
                    generation
            );
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
            lifecycleState.finishPublication(lifecycleIdentity, hasPriorOrphanRisk);
            adoptBoundaryBaseline(boundary, generation);

            LiveViewCheckpointLifecycle.purgeFinalOrphans(
                    configuration,
                    checkpointsDir,
                    protectedSegmentIdCeiling,
                    orphanUpperBound,
                    true
            );
            return shells.appendResult.of(
                    generation,
                    checkpointId,
                    boundary.logicalStateBytes,
                    dataSegmentBytes,
                    metadataBytesAdded,
                    metaStore.getWalPurgeFloor(),
                    shells.appendStats
                            .of(superblock, checkedAdd(dataSegmentBytes, metadataBytesAdded)),
                    liveSegmentCount,
                    obsoleteSegmentBytes
            );
        } finally {
            releasePublicationShells();
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

    private static void accumulateReferenceDeltas(LongList deltas, LongList segmentIds, long delta) {
        for (int i = 0, n = segmentIds.size(); i < n; i++) {
            final long segmentId = segmentIds.getQuick(i);
            int lo = 0;
            int hi = deltas.size() / 2;
            while (lo < hi) {
                final int mid = (lo + hi) >>> 1;
                if (deltas.getQuick(mid * 2) < segmentId) {
                    lo = mid + 1;
                } else {
                    hi = mid;
                }
            }
            final int offset = lo * 2;
            if (offset < deltas.size() && deltas.getQuick(offset) == segmentId) {
                final long net = Math.addExact(deltas.getQuick(offset + 1), delta);
                if (net == 0) {
                    deltas.removeIndex(offset);
                    deltas.removeIndex(offset);
                } else {
                    deltas.setQuick(offset + 1, net);
                }
            } else {
                deltas.insert(offset, 2);
                deltas.setQuick(offset, segmentId);
                deltas.setQuick(offset + 1, delta);
            }
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

    /**
     * Copies {@code from} into the caller's reusable {@code into} shell and returns
     * it, so a publication reads a superblock reference without building one.
     */
    private static LiveViewCheckpointPageRef copyInto(LiveViewCheckpointPageRef from, LiveViewCheckpointPageRef into) {
        copy(from, into);
        return into;
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

    /**
     * @return the logical bytes a predecessor root charges for one partition: its
     * key plus every byte of state the entry names. An incremental seal subtracts
     * this and adds the fresh figure, so the running total stays "this boundary's
     * whole live state" rather than a delta.
     */
    private static long logicalPartitionBytes(@Nullable LiveViewCheckpointPartitionMapEntry entry) {
        if (entry == null) {
            return 0;
        }
        long bytes = checkedAdd(entry.getKey().length, entry.getScalarState().length);
        for (int i = 0, n = entry.getStatePageCount(); i < n; i++) {
            bytes = checkedAdd(bytes, entry.getStatePageRef(i).getDecodedLength());
        }
        return bytes;
    }

    /**
     * Whether {@code plan} carries {@code function} as a <b>durable</b> projection, and so
     * holds its state in the fused root rather than in a root of its own.
     * <p>
     * A runtime-only member answers false: the group holds its accumulator and it still
     * publishes a root, which {@link #freezeGroupedFunctions} writes out of that group.
     */
    private static boolean isDurableGroupedProjection(
            @Nullable LiveViewWindowStatePlan plan,
            WindowFunction function
    ) {
        if (plan == null) {
            return false;
        }
        final int projectionIndex = plan.indexOfProjectionFunction(function);
        return projectionIndex >= 0 && plan.isDurableProjection(projectionIndex);
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
     * Registers one aggregate boundary-metadata segment and removes it from
     * each logical root's ordinary reference delta. The catalogue count is the
     * number of independently replaceable roots whose closure names the file;
     * replacing one boundary therefore releases one reference, and the shared
     * file retires only after the last boundary moves away from it.
     */
    private static void registerAggregateBoundarySegment(
            LiveViewCheckpointSegmentDirectoryWriter directoryWriter,
            long segmentId,
            long segmentBytes,
            ObjList<LongList> rootSegmentIds,
            int rootCount
    ) {
        if (segmentBytes == 0) {
            return;
        }
        long referenceCount = 0;
        for (int i = 0; i < rootCount; i++) {
            if (dropSegmentId(rootSegmentIds.getQuick(i), segmentId)) {
                referenceCount++;
            }
        }
        if (referenceCount == 0) {
            throw CairoException.critical(CairoException.LV_CHECKPOINT_TIMELINE_INVALID)
                    .put("live view checkpoint aggregate segment is not named by any root, segmentId=")
                    .put(segmentId);
        }
        directoryWriter.addSegment(
                segmentId,
                segmentBytes,
                referenceCount,
                LiveViewCheckpointSegmentDirectory.SEGMENT_KIND_BOUNDARY
        );
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
     * Persists zero-reference transitions before the superblock commit. A queue
     * for the immediately preceding generation advances from its exact live-data
     * count in O(new retirements). A missing, corrupt or stale queue is rebuilt
     * conservatively from the candidate directory once; subsequent publications
     * return to bounded work.
     */
    private boolean persistRetirementQueue(
            PublicationScratch shells,
            Path checkpointsDir,
            long definitionTxn,
            long historyEpoch,
            long lifecycleIdentity,
            LiveViewCheckpointSegmentDirectoryWriter directoryWriter,
            LiveViewCheckpointPageRef newDirectoryRoot,
            long generation
    ) {
        final LongList existing = shells.retirementExisting;
        final LongList seed = shells.retirementSeed;
        final LiveViewCheckpointRetirementQueue.State state = shells.retirementState;
        existing.clear();
        seed.clear();
        state.clear();
        final boolean queueValid = LiveViewCheckpointRetirementQueue.read(
                configuration,
                shells.retirementQueueScratch,
                checkpointsDir,
                existing,
                state
        );
        final boolean advancesExisting = queueValid && state.generation + 1 == generation;
        final boolean hasPriorOrphanRisk = queueValid && state.generation == generation;
        if (queueValid && state.generation == generation) {
            // A queue for the generation we are only now attempting can only
            // have been staged by an earlier publication which failed before its
            // superblock commit. Preserve that durable orphan-recovery signal
            // even though this publication replaces the queue image.
            lifecycleState.markOrphanRisk(lifecycleIdentity);
        }
        final long liveDataSegmentCount;
        if (advancesExisting) {
            liveDataSegmentCount = checkedAdd(state.liveDataSegmentCount, directoryWriter.getLiveDataSegmentDelta());
            if (liveDataSegmentCount < 0) {
                throw CairoException.critical(CairoException.LV_CHECKPOINT_TIMELINE_INVALID)
                        .put("live view checkpoint live data segment count underflow");
            }
        } else {
            final long rebuiltLiveDataSegmentCount;
            final LiveViewCheckpointSegmentDirectoryReader reader = shells.directorySeedReader;
            try {
                reader.of(checkpointsDir, newDirectoryRoot);
                retirementQueueSeedVisitor.of(seed);
                try {
                    reader.iterateAll(retirementQueueSeedVisitor);
                    rebuiltLiveDataSegmentCount = retirementQueueSeedVisitor.liveDataSegmentCount;
                } finally {
                    retirementQueueSeedVisitor.clearBindings();
                }
            } finally {
                reader.detach();
            }
            liveDataSegmentCount = rebuiltLiveDataSegmentCount;
        }
        // The read above is the only one this publication owes the queue: it
        // already carries the image and the generation test the merge would
        // otherwise recompute over a second open, mmap and full-image checksum.
        LiveViewCheckpointRetirementQueue.mergeAndWrite(
                configuration,
                shells.retirementQueueScratch,
                checkpointsDir,
                directoryWriter.getRetirementTransitions(),
                advancesExisting ? existing : seed,
                generation,
                liveDataSegmentCount
        );
        return hasPriorOrphanRisk;
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

    /**
     * Carries away the catalogue entries a reconciliation's purge sweep left
     * naming an unlinked file. The sweep proves an entry dead - zero references,
     * past both generation gates, file gone - but publishes no generation of its
     * own, so this seal is what removes the entries; without it the catalogue
     * holds one per segment ever written and its own tree grows with the view's
     * age rather than with what the view currently holds.
     */
    private static void retireCatalogueEntries(
            LiveViewCheckpointSegmentDirectoryWriter directoryWriter,
            @Nullable LongList retirableSegmentIds
    ) {
        if (retirableSegmentIds == null) {
            return;
        }
        for (int i = 0, n = retirableSegmentIds.size(); i < n; i++) {
            directoryWriter.removeSegment(retirableSegmentIds.getQuick(i));
        }
    }

    /**
     * The previous boundary's whole-state page for one partition, or null when
     * its entry holds something a whole-state freeze cannot reuse - a ring entry,
     * an image the leaf inlines, or no page at all. Only the page-backed arm of
     * the freeze asks: a function that inlines compares scalar bytes instead, and
     * every entry its own predecessor root holds is one of its own.
     */
    private static @Nullable LiveViewCheckpointStatePageRef wholeStatePageRef(
            @Nullable LiveViewCheckpointPartitionMapEntry entry
    ) {
        if (entry == null || entry.getScalarState().length != 0 || entry.getStatePageCount() != 1) {
            return null;
        }
        return rawStatePageRef(entry.getStatePageRef(0));
    }

    /**
     * Hands every runtime target this boundary was frozen from the root it now stands
     * on, so its next freeze may image only the keys touched from here.
     * <p>
     * Two callers, and the only difference between them is which generation they name.
     * The cadence seal passes the generation it has just durably published, which is
     * the ordinary contract {@code onCheckpointPersisted} documents. A chaining repair
     * capture passes {@link LiveViewCheckpointContracts#REPAIR_BASELINE_GENERATION}
     * instead - it has published nothing, and will not until its whole chain of
     * boundaries splices - so what the runtime carries between capture points is a
     * stamp no real generation can match. Either way the dirty sets reset here, which
     * is what makes the next freeze name the keys touched since this boundary rather
     * than since the last publication.
     *
     * @param generation the generation the roots this boundary produced belong to, or
     *                   the provisional repair stamp
     */
    private void adoptBoundaryBaseline(FrozenBoundary boundary, long generation) {
        if (boundary.anchor != null) {
            boundary.anchor.window.onCheckpointPersisted(boundary.anchor.logicalStateBytes, generation);
        }
        if (boundary.windowState != null) {
            final FrozenWindowState windowState = boundary.windowState;
            windowState.window.onCheckpointPersisted(windowState.logicalStateBytes, generation);
            // A durable projection charges nothing of its own - the fused entry's
            // whole width is the window's - but it still has to be told the seal
            // happened, or its dirty set would grow for the life of the view and its
            // baseline would never reach the generation the next seal builds on. A
            // runtime-only member is told below instead, with the figure its own root
            // charges, which is the one its next incremental freeze builds on.
            for (int i = 0, n = windowState.plan.getProjectionCount(); i < n; i++) {
                if (windowState.plan.isDurableProjection(i)) {
                    windowState.plan.getProjectionFunction(i).onCheckpointPersisted(0, generation);
                }
            }
        }
        for (int i = 0, n = boundary.functions.size(); i < n; i++) {
            final FrozenFunction frozen = boundary.functions.getQuick(i);
            frozen.function.onCheckpointPersisted(frozen.logicalStateBytes, generation);
        }
    }

    private void bindScratchBuffers(@Nullable MemoryTracker memoryTracker) {
        // The post-splice frontier seal begins while the repair capture that
        // produced the splice is still open, so the scratch may still hold
        // that capture's last image. The frozen boundaries carry their own
        // copies, which makes the held image dead weight - hand it back
        // against the tracker that grew it before binding the caller's, so
        // no charge ever migrates between trackers.
        publicationScratch.bind(memoryTracker);
        activeScratch = publicationScratch;
    }

    /**
     * Images one partition key into the active scratch key buffer and returns its length. The
     * buffer is rewound first, so the image starts at offset 0 and the caller may
     * read it back through {@link LiveViewSnapshotKeyCodec#readKey} or copy it out.
     */
    private int encodeCheckpointKey(MapRecord record, ColumnTypes keyTypes, int keyStartIndex) {
        activeScratch.keyBuffer.jumpTo(0);
        LiveViewSnapshotKeyCodec.writeKey(activeScratch.keyBuffer, record, keyTypes, keyStartIndex);
        return checkedIntLength(activeScratch.keyBuffer.getAppendOffset(), "partition key");
    }

    private void ensureDirectories(Path checkpointsDir) {
        final Path path = directoryScratchPath;
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
     * anchor-map metadata pages rather than in the data segment. They are also the
     * exception to {@code outputKeys}: an anchor value is the anchor-period floor of
     * a key's last row, so a key the replay carried out of a truncated history holds
     * a strictly older floor there and its next row resets it, and a key the replay
     * never carried is simply absent and keeps the entry the old anchor root wrote.
     * <p>
     * A view whose anchored window compiled a {@link LiveViewWindowStatePlan} freezes a
     * {@code FrozenWindowState} in the anchor's place instead, and the functions that
     * plan groups get no {@code FrozenFunction} at all: their state is components of
     * that one root rather than a root per SELECT-list call. The exemption above does
     * not survive fusion - see {@link #freezeWindowState} - because the anchor value and
     * the components it is fused with are one entry.
     *
     * @param outputKeys         {@code Q}, when the replay describes those keys and no
     *                           others, or null when it describes every live key. A key
     *                           outside it is not imaged at all: the root it is being
     *                           frozen for keeps the entry the old root already holds
     * @param baselineGeneration the generation of the root this freeze sits on top of,
     *                           or {@link Numbers#LONG_NULL} when nothing names that
     *                           root. An incremental freeze is valid only against the
     *                           root the runtime's own last freeze produced, and this is
     *                           what the runtime compares its baseline to: a repair,
     *                           truncate or compaction publishing in between moves the
     *                           generation on and demotes the freeze to a full scan. A
     *                           chaining repair capture passes
     *                           {@link LiveViewCheckpointContracts#REPAIR_BASELINE_GENERATION}
     *                           here, because the root it builds on is one of its own
     *                           and no generation names it yet
     */
    private FrozenBoundary freezeBoundary(
            LiveViewCheckpointDataSegmentWriter dataWriter,
            @NotNull ObjList<WindowFunction> functions,
            @Nullable LiveViewWindow anchorWindow,
            @Nullable PreviousBoundary previousBoundary,
            @Nullable LiveViewCheckpointOutputKeyDomain outputKeys,
            long baselineGeneration
    ) {
        final FrozenBoundary boundary = nextFrozenBoundary().of();
        long logicalStateBytes = 0;
        // Runtime-only members are parked in these two buckets by the loop below and frozen
        // together afterwards, one shared walk of the key domain per bucket.
        activeScratch.incrementalMembers.clear();
        activeScratch.incrementalMemberProjections.clear();
        activeScratch.completeMembers.clear();
        activeScratch.completeMemberProjections.clear();
        // The compiled fused group, or null for a view that has none. It decides both
        // halves of this freeze at once: which state root the boundary writes, and which
        // functions still get a root of their own.
        final LiveViewWindowStatePlan plan = anchorWindow == null
                ? null
                : anchorWindow.getCheckpointWindowStatePlan();
        if (plan != null) {
            final FrozenWindowState windowState =
                    freezeWindowState(anchorWindow, plan, previousBoundary, outputKeys, baselineGeneration);
            logicalStateBytes = checkedAdd(logicalStateBytes, windowState.logicalStateBytes);
            boundary.windowState = windowState;
        } else if (anchorWindow != null) {
            final FrozenAnchor anchor = nextFrozenAnchor().of(
                    anchorWindow,
                    anchorWindow.borrowCheckpointWindowNameUtf8(),
                    anchorWindow.getAnchorValueType(),
                    anchorWindow.borrowCheckpointKeySchema()
            );
            anchor.isIncremental = previousBoundary != null
                    && previousBoundary.isIncrementalBase()
                    && previousBoundary.hasAnchorRoot()
                    && anchorWindow.canFreezeCheckpointIncrementally(baselineGeneration);
            anchor.logicalStateBytes = anchorWindow.freezeCheckpointEntries(
                    activeScratch.keyBuffer,
                    anchor.keys,
                    anchor.anchorValues,
                    anchor.removedKeys,
                    anchor.isIncremental,
                    activeScratch.frozenByteArrays
            );
            logicalStateBytes = checkedAdd(logicalStateBytes, anchor.logicalStateBytes);
            boundary.anchor = anchor;
        }
        for (int i = 0, n = functions.size(); i < n; i++) {
            final WindowFunction function = functions.getQuick(i);
            if (!function.supportsCheckpointState()) {
                continue;
            }
            if (isDurableGroupedProjection(plan, function)) {
                // The fused root holds this function's state, so it gets no root of its
                // own and never reaches the function directory. Its identity is not
                // persisted anywhere for this boundary, which is the point: the durable
                // unit is the accumulator component, not the SELECT-list call.
                continue;
            }
            final LiveViewCheckpointFunctionIdentity identity = function.checkpointFunctionIdentity();
            if (identity == null || function.checkpointDependency() == null) {
                throw CairoException.critical(0)
                        .put("checkpoint-capable live view function has no compiler metadata");
            }
            final FrozenFunction frozen = nextFrozenFunction().of(
                    function,
                    identity.borrowEncoded(),
                    function.checkpointStateFormatVersion(),
                    identity.borrowEncodedKeySchema()
            );
            // A runtime-only member has no map of its own left to walk: the window owns
            // its slots. Its root is written from the group's key domain instead, which
            // is the whole of what step 8.1 changes about where a function's bytes come
            // from. Every such member reads the same keys out of the same map, so they are
            // parked here and frozen together below rather than each walking that map.
            final int memberProjectionIndex = plan == null ? -1 : plan.indexOfProjectionFunction(function);
            if (memberProjectionIndex >= 0) {
                frozen.isIncremental = previousBoundary != null
                        && previousBoundary.isIncrementalBase()
                        && !function.isCheckpointFullScanRequired()
                        && function.getCheckpointBaselineGeneration() == baselineGeneration
                        && anchorWindow.canFreezeCheckpointIncrementally(baselineGeneration)
                        && previousBoundary.hasFunctionRoot(frozen.identity, frozen.stateFormatVersion);
                if (frozen.isIncremental) {
                    activeScratch.incrementalMembers.add(frozen);
                    activeScratch.incrementalMemberProjections.add(memberProjectionIndex);
                } else {
                    activeScratch.completeMembers.add(frozen);
                    activeScratch.completeMemberProjections.add(memberProjectionIndex);
                }
                // Its logical charge is filled in by the shared walk below.
                boundary.functions.add(frozen);
                continue;
            }
            final long functionLogicalStateBytes =
                    freezeFunction(dataWriter, function, frozen, previousBoundary, outputKeys, baselineGeneration);
            frozen.logicalStateBytes = functionLogicalStateBytes;
            logicalStateBytes = checkedAdd(logicalStateBytes, functionLogicalStateBytes);
            boundary.functions.add(frozen);
        }
        if (anchorWindow != null) {
            logicalStateBytes = checkedAdd(logicalStateBytes, freezeGroupedFunctions(
                    anchorWindow,
                    activeScratch.incrementalMembers,
                    activeScratch.incrementalMemberProjections,
                    activeScratch.incrementalMemberImages,
                    true,
                    previousBoundary,
                    outputKeys
            ));
            logicalStateBytes = checkedAdd(logicalStateBytes, freezeGroupedFunctions(
                    anchorWindow,
                    activeScratch.completeMembers,
                    activeScratch.completeMemberProjections,
                    activeScratch.completeMemberImages,
                    false,
                    previousBoundary,
                    outputKeys
            ));
            // The buckets have served their walks. Dropping them here rather than at the
            // next seal's start keeps this writer - one worker's, shared by every view it
            // seals - from pinning the last boundary's frozen functions in between.
            activeScratch.incrementalMembers.clear();
            activeScratch.incrementalMemberProjections.clear();
            activeScratch.completeMembers.clear();
            activeScratch.completeMemberProjections.clear();
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
            @Nullable PreviousBoundary previousBoundary,
            @Nullable LiveViewCheckpointOutputKeyDomain outputKeys,
            long baselineGeneration
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

        // Incremental only against the root this function's own last publication
        // produced: a repair, truncate or compaction publishing in between moves the
        // generation on, and the untouched keys the seal would leave alone belong to
        // a root the function never saw. The predecessor must also actually hold a root
        // for it - a function whose state the previous boundary fused into the window
        // root has a current baseline and no root of its own, and putting only the
        // touched keys into a tree built from empty would drop the rest.
        final Map dirtyMap = previousBoundary != null
                && previousBoundary.isIncrementalBase()
                && !isRingShaped
                && !function.isCheckpointFullScanRequired()
                && function.getCheckpointBaselineGeneration() == baselineGeneration
                && previousBoundary.hasFunctionRoot(frozen.identity, frozen.stateFormatVersion)
                ? function.getCheckpointDirtyPartitionMap()
                : null;
        final boolean isIncremental = dirtyMap != null;
        frozen.isIncremental = isIncremental;
        // Whether this function's whole-state image goes into the leaf rather than
        // into a data page it names. Read once per function: the answer is a property
        // of the compiled implementation, not of the partition being frozen.
        final boolean hasInlineState = !isRingShaped
                && LiveViewCheckpointContracts.isInlineableStateLength(function.checkpointStateFixedLength());
        long logicalBytes = isIncremental ? function.getCheckpointLogicalStateBytes() : 0;
        final ColumnTypes keyTypes = function.getCheckpointKeyColumnTypes();
        final int keyStartIndex = function.getCheckpointKeyStartIndex();
        final int tombstoneIndex = function.getTombstoneValueIndex();
        final Map scanMap = isIncremental ? dirtyMap : map;
        // Pass one names the keys this freeze will image and reads no predecessor at
        // all. The lookups pass two makes land in a tree whose leaves are laid out in
        // the key's own byte order, and the reader memoises one decoded node per depth,
        // so taking them in a map cursor's order - hash-slot order, once the key is
        // narrow enough for an unordered map - decodes a leaf per key rather than a leaf
        // per leaf. The keys have to be in hand before that order can be computed.
        final ObjList<byte[]> keys = activeScratch.functionFreezeKeys;
        final IntList dirtyTombstones = activeScratch.functionFreezeDirtyTombstones;
        keys.clear();
        dirtyTombstones.clear();
        final MapRecordCursor cursor = scanMap.getCursor();
        final MapRecord record = scanMap.getRecord();
        while (cursor.hasNext()) {
            if (!isIncremental) {
                // A full scan walks the state map itself and so has the value in hand,
                // which is what lets it drop a tombstoned key before paying for the key
                // image at all.
                if (tombstoneIndex >= 0 && record.getValue().getByte(tombstoneIndex) == 1) {
                    continue;
                }
            }
            final int keyLength = encodeCheckpointKey(record, keyTypes, keyStartIndex);
            keys.add(activeScratch.frozenByteArrays.copy(activeScratch.keyBuffer, 0, keyLength));
            // The dirty map carries keys and nothing but the eviction marker the frontier
            // sweep writes into the borrowed tombstone slot, and that marker is the one
            // thing pass two cannot recover for itself: the key it belongs to is already
            // gone from the live map.
            dirtyTombstones.add(isIncremental && tombstoneIndex >= 0
                    ? record.getValue().getByte(tombstoneIndex)
                    : 0);
        }

        // Pass two does every lookup, seal and charge, walking the keys in the
        // predecessor tree's own order instead of the cursor's. The order is a walk
        // order and nothing else - the root builder sorts the puts it is handed - so it
        // changes only which leaves this seal decodes and the order its state pages land
        // in the data segment. It costs one live-map probe per key: the incremental arm
        // was already paying for one to tell a live key from an evicted one, and a
        // complete scan was getting its value from the cursor for free.
        final int keyCount = keys.size();
        LiveViewCheckpointMetadata.sortKeyOrdinals(
                keys,
                activeScratch.keyLookupPairs,
                activeScratch.keyLookupOrder
        );
        final LiveViewCheckpointPartitionMapEntry ringEntry =
                isRingShaped ? new LiveViewCheckpointPartitionMapEntry() : null;
        for (int o = 0; o < keyCount; o++) {
            final int i = activeScratch.keyLookupOrder.getQuick(o);
            final byte[] key = keys.getQuick(i);
            final int keyLength = key.length;
            activeScratch.keyBuffer.jumpTo(0);
            LiveViewCheckpointMetadata.putBytes(activeScratch.keyBuffer, key);
            final MapKey liveKey = map.withKey();
            LiveViewSnapshotKeyCodec.readKey(liveKey, activeScratch.keyBuffer, 0, keyTypes);
            final MapValue value = liveKey.findValue();
            boolean isEvicted = false;
            if (value == null) {
                // The frontier sweep is the only thing that removes a key from a
                // function's state map, and it records every key it drops, so a dirty key
                // the live map does not hold and that carries no eviction marker is a
                // broken invariant rather than a removal. Reading it as one would delete
                // live window state from the root, and the wrong result would only
                // surface after a restart. A complete scan reaches this only when a key
                // it took out of that same map fails to find itself through its own
                // image, which is the same class of breakage and earns the same refusal.
                // The check runs ahead of the output-key filter below because a key
                // outside the replay's domain is no less broken for being outside it.
                if (!isIncremental || dirtyTombstones.getQuick(i) != 1) {
                    throw CairoException.critical(0)
                            .put("live view checkpoint partition key is missing from function state");
                }
                isEvicted = true;
            }
            // An evicted key has no live value left, so there is no tombstone bit to
            // read, and a complete scan dropped its tombstoned keys in pass one rather
            // than carrying them here - so only an incremental walk still holds one.
            final boolean isTombstoned = isIncremental
                    && !isEvicted
                    && tombstoneIndex >= 0
                    && value.getByte(tombstoneIndex) == 1;
            if (outputKeys != null && !outputKeys.contains(key)) {
                // Outside the replay's key domain: the state the runtime holds here was
                // reconstructed from whatever rows happened to fall inside [L, H), so
                // imaging it would publish a truncated history. The root keeps the entry
                // the boundary already had, and no page is written for it at all.
                continue;
            }
            final LiveViewCheckpointPartitionMapEntry previous = previousBoundary == null
                    ? null
                    : previousBoundary.find(frozen.identity, frozen.stateFormatVersion, key);
            if (isTombstoned || isEvicted) {
                // Incremental only: the key died since the predecessor root - tombstoned
                // by a reset no row cancelled, or dropped by the frontier sweep - so the
                // root has to drop the entry it still holds for it. A null predecessor
                // means the root never held the key (created and evicted inside one
                // cadence), so there is nothing to remove and nothing to un-charge.
                if (previous != null) {
                    frozen.removedPartitions.add(key);
                    logicalBytes = checkedAdd(logicalBytes, -logicalPartitionBytes(previous));
                }
                continue;
            }
            if (isRingShaped) {
                logicalBytes = checkedAdd(logicalBytes, keyLength);
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
                final long stateLength;
                if (hasInlineState) {
                    final byte[] scalarState = freezeInlineState(function, value);
                    // The predecessor's image is already in the decoded leaf entry this
                    // freeze holds, so the elision costs a byte compare and no longer has
                    // to map the older data segment the page-backed arm below reads. The
                    // zero-reference test is what keeps the short-circuit honest: skipping
                    // the put leaves the predecessor's whole entry standing, and an entry
                    // carrying a page beside these bytes is not the one this freeze means.
                    final boolean isUnchanged = previousBoundary != null
                            && previousBoundary.isIncrementalBase()
                            && previous != null
                            && previous.getStatePageCount() == 0
                            && Arrays.equals(previous.getScalarState(), scalarState);
                    frozen.addPartition(key, scalarState, isUnchanged);
                    stateLength = scalarState.length;
                } else {
                    final LiveViewCheckpointStatePageRef previousRef = wholeStatePageRef(previous);
                    final LiveViewCheckpointStatePageRef stateRef = freezeStatePage(
                            dataWriter,
                            function,
                            value,
                            previousBoundary,
                            previousRef
                    );
                    final boolean isUnchanged = previousBoundary != null
                            && previousBoundary.isIncrementalBase()
                            && previousRef != null
                            && previousRef.getSegmentId() == stateRef.getSegmentId()
                            && previousRef.getOffset() == stateRef.getOffset();
                    frozen.addPartition(key, stateRef, isUnchanged);
                    stateLength = stateRef.getDecodedLength();
                }
                // The two shapes charge the same figure: an inlined image and a page
                // named by a reference hold the same state bytes, and logical accounting
                // counts the state rather than the framing that reaches it. That is what
                // lets a root convert entry by entry without the running total moving.
                if (isIncremental) {
                    final long newLogicalBytes = checkedAdd(keyLength, stateLength);
                    logicalBytes = checkedAdd(
                            logicalBytes,
                            checkedAdd(newLogicalBytes, -logicalPartitionBytes(previous))
                    );
                } else {
                    logicalBytes = checkedAdd(logicalBytes, keyLength);
                    logicalBytes = checkedAdd(logicalBytes, stateLength);
                }
            }
        }
        return logicalBytes;
    }

    /**
     * Freezes the runtime-only members that share one incremental disposition, out of the
     * group's map rather than out of the private maps they no longer have.
     * <p>
     * Everything about each root is what it always was - the same identity, the same state
     * format version, the same inline whole-state image, the same incremental removals - and
     * only where the bytes come from moved. That is deliberate: a checkpoint written before
     * the member joined the group reads back into one written after it without conversion,
     * which is what keeps the group a runtime decision.
     * <p>
     * The image is always inline. A member reached the group through the compiler's
     * inline-budget gate, so its declared width fits
     * {@code MAX_INLINE_COMPONENT_STATE_BYTES} and no data page is ever written here - which
     * is also why this takes no data-segment writer.
     * <p>
     * Two of the three questions an incremental freeze asks are the group's now. Which keys
     * moved and which the sweep dropped are the window's one dirty set's, because a bound
     * function marks nothing of its own; what stays each function's is only whether its own
     * root is the one the predecessor holds at the generation being built on. That last one
     * is what the caller buckets on, because it is the only one that can differ between two
     * members of one group - a state-format version bump leaves one member without a
     * matching predecessor root while its siblings keep theirs - and it decides which map
     * the walk reads. Everything else about the walk is the key's, so the members that
     * agree on it share one.
     */
    private long freezeGroupedFunctions(
            @NotNull LiveViewWindow window,
            @NotNull ObjList<FrozenFunction> members,
            @NotNull IntList projectionIndexes,
            @NotNull ObjList<ObjList<byte[]>> memberImages,
            boolean isIncremental,
            @Nullable PreviousBoundary previousBoundary,
            @Nullable LiveViewCheckpointOutputKeyDomain outputKeys
    ) {
        final int memberCount = members.size();
        if (memberCount == 0) {
            return 0;
        }
        try {
            activeScratch.groupedFreezeKeys.clear();
            activeScratch.groupedFreezeRemovedKeys.clear();
            activeScratch.groupedFreezeLogicalBytes.clear();
            // One image list per member, grown once and reused: a bucket's width follows the
            // compiled plan, so after the first seal this allocates nothing. The walk clears
            // each list it is handed.
            for (int m = 0; m < memberCount; m++) {
                activeScratch.groupedFreezeLogicalBytes.add(
                        members.getQuick(m).function.getCheckpointLogicalStateBytes()
                );
                if (memberImages.size() <= m) {
                    memberImages.add(new ObjList<>());
                }
            }
            window.freezeCheckpointMemberEntries(
                    activeScratch.keyBuffer,
                    projectionIndexes,
                    activeScratch.groupedFreezeKeys,
                    memberImages,
                    activeScratch.groupedFreezeRemovedKeys,
                    isIncremental,
                    activeScratch.groupedFreezeLogicalBytes,
                    activeScratch.frozenByteArrays
            );
            // Every member looks the same keys up in a root of its own, so the order that
            // makes those lookups walk one leaf at a time is worth computing once, ahead of
            // them all. Where a member's own partitions then land is free: the mutation
            // arena sorts what it is handed.
            final int keyCount = activeScratch.groupedFreezeKeys.size();
            LiveViewCheckpointMetadata.sortKeyOrdinals(
                    activeScratch.groupedFreezeKeys,
                    activeScratch.keyLookupPairs,
                    activeScratch.keyLookupOrder
            );
            long logicalStateBytes = 0;
            for (int m = 0; m < memberCount; m++) {
                final FrozenFunction frozen = members.getQuick(m);
                final ObjList<byte[]> images = memberImages.getQuick(m);
                for (int o = 0; o < keyCount; o++) {
                    final int i = activeScratch.keyLookupOrder.getQuick(o);
                    final byte[] key = activeScratch.groupedFreezeKeys.getQuick(i);
                    if (outputKeys != null && !outputKeys.contains(key)) {
                        // Outside the replay's key domain, exactly as in freezeFunction: the
                        // state the group holds for this key was rebuilt from whatever rows
                        // fell inside [L, H), so the root keeps the entry it already had.
                        continue;
                    }
                    final byte[] image = images.getQuick(i);
                    final LiveViewCheckpointPartitionMapEntry previous = previousBoundary == null
                            ? null
                            : previousBoundary.find(frozen.identity, frozen.stateFormatVersion, key);
                    final boolean isUnchanged = previousBoundary != null
                            && previousBoundary.isIncrementalBase()
                            && previous != null
                            && previous.getStatePageCount() == 0
                            && Arrays.equals(previous.getScalarState(), image);
                    frozen.addPartition(key, image, isUnchanged);
                }
                for (int i = 0, n = activeScratch.groupedFreezeRemovedKeys.size(); i < n; i++) {
                    frozen.removedPartitions.add(activeScratch.groupedFreezeRemovedKeys.getQuick(i));
                }
                frozen.logicalStateBytes = activeScratch.groupedFreezeLogicalBytes.getQuick(m);
                logicalStateBytes = checkedAdd(logicalStateBytes, frozen.logicalStateBytes);
            }
            return logicalStateBytes;
        } finally {
            // The lists are pooled, their contents are not. This writer is one worker's and
            // is shared by every view it seals, so holding a seal's keys and images past the
            // seal would pin one view's whole key domain against the next view's work.
            activeScratch.releaseGroupedFreezeScratch(memberImages);
        }
    }

    /**
     * Freezes one anchored window's fused state: one entry per live key, holding the
     * anchor value and every grouped accumulator component the plan names, laid out at
     * the manifest's offsets.
     * <p>
     * The anchor map is the authoritative key domain - it is what {@code processRow}
     * writes first and what the frontier sweep rebuilds - so the walk is the anchor's
     * own, and each key's component slices are read by probing the contributing
     * function's map through the same encoded key. The two maps may be different
     * {@link Map} implementations, which is why the probe goes through the key codec
     * rather than through either implementation's key.
     * <p>
     * {@code outputKeys} applies to the whole entry here, where the legacy anchor path
     * exempted itself from it. A fused entry is one unit: a key outside {@code Q} has
     * component state a truncated replay reconstructed, and there is no way to publish a
     * fresh anchor value for it without publishing those components too. Keeping the
     * predecessor's whole entry is at least as correct as the exemption's own argument -
     * a key with no qualifying row in the replaced interval is one whose anchor value did
     * not move either.
     */
    private FrozenWindowState freezeWindowState(
            LiveViewWindow window,
            LiveViewWindowStatePlan plan,
            @Nullable PreviousBoundary previousBoundary,
            @Nullable LiveViewCheckpointOutputKeyDomain outputKeys,
            long baselineGeneration
    ) {
        final FrozenWindowState frozen = nextFrozenWindowState().of(
                window,
                plan,
                plan.borrowWindowIdentity(),
                window.getAnchorValueType(),
                window.borrowCheckpointKeySchema(),
                plan.getManifest().borrowEncoded(),
                plan.getTotalInlineStateBytes()
        );
        // Four things have to match before a seal may build on the predecessor's leaves,
        // and the manifest is the one nothing else covers: a recompile can change it
        // without moving definitionTxn, and publishing it over leaves an older manifest
        // wrote is a silent misread rather than a rejection.
        final boolean hasCompatiblePredecessor = previousBoundary != null
                && previousBoundary.isCompatibleWindowRoot(
                frozen.windowIdentity,
                frozen.anchorValueType,
                frozen.keySchema,
                frozen.manifest
        );
        frozen.isIncremental = hasCompatiblePredecessor
                && previousBoundary.isIncrementalBase()
                && window.canFreezeCheckpointIncrementally(baselineGeneration);
        // One walk of one map produces the keys, the anchor values and the whole fused
        // payloads together: the window owns the group's runtime state, so a component's
        // bytes come off the same loaded value the anchor value does rather than out of a
        // probe per component.
        frozen.logicalStateBytes = window.freezeCheckpointEntries(
                activeScratch.keyBuffer,
                frozen.keys,
                frozen.anchorValues,
                frozen.removedKeys,
                frozen.isIncremental,
                frozen.totalInlineStateBytes,
                frozen.payloads,
                activeScratch.frozenByteArrays
        );
        // The freeze names its keys in the map cursor's order, which is hash-slot order
        // once the key is narrow enough for an unordered map, and every lookup below then
        // lands in a different leaf of the predecessor's tree. Taking them in the tree's
        // own order instead decodes each leaf once. The lists stay in the freeze's order -
        // only the walk moves - so isUnchanged is filled by index rather than appended.
        final int keyCount = frozen.keys.size();
        LiveViewCheckpointMetadata.sortKeyOrdinals(
                frozen.keys,
                activeScratch.keyLookupPairs,
                activeScratch.keyLookupOrder
        );
        frozen.isUnchanged.setAll(keyCount, false);
        for (int o = 0; o < keyCount; o++) {
            final int i = activeScratch.keyLookupOrder.getQuick(o);
            final byte[] key = frozen.keys.getQuick(i);
            if (outputKeys != null && !outputKeys.contains(key)) {
                frozen.payloads.setQuick(i, null);
                frozen.isUnchanged.setQuick(i, true);
                continue;
            }
            // The predecessor's payload is already in the decoded leaf entry, so the
            // elision is a byte compare against bytes this seal holds. The zero-reference
            // test keeps it honest: skipping the put leaves the predecessor's whole entry
            // standing, and an entry naming a page beside these bytes is not that entry.
            final LiveViewCheckpointPartitionMapEntry previous = hasCompatiblePredecessor
                    ? previousBoundary.findWindowState(key)
                    : null;
            frozen.isUnchanged.setQuick(i, previous != null
                    && previous.getStatePageCount() == 0
                    && Arrays.equals(previous.getScalarState(), frozen.payloads.getQuick(i)));
        }
        return frozen;
    }

    /**
     * Encodes one whole-state image into the bytes a partition-map leaf carries in
     * place of a state page.
     * <p>
     * The encode runs through the same scratch buffer and the same
     * {@link LiveViewStatePageWriter#freeze} the page-backed arm uses, so the image
     * is verified against the width its function declared before it can reach a
     * leaf that holds no length of its own to check it against later.
     */
    private byte[] freezeInlineState(WindowFunction function, @Nullable MapValue value) {
        activeScratch.stateBuffer.jumpTo(0);
        final LiveViewStatePageWriter pageWriter = statePageWriter.of(activeScratch.stateBuffer);
        final int bytes = checkedIntLength(pageWriter.freeze(function, value), "function state");
        return activeScratch.frozenByteArrays.copy(activeScratch.stateBuffer, 0, bytes);
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
        activeScratch.stateBuffer.jumpTo(0);
        final LiveViewStatePageWriter pageWriter = statePageWriter.of(activeScratch.stateBuffer);
        final int bytes = checkedIntLength(pageWriter.freeze(function, value), "function state");
        // After the encode: an extending put moves the buffer.
        final long address = activeScratch.stateBuffer.addressOf(0);
        if (previousBoundary != null
                && previousRef != null
                && previousRef.getStoredLength() == bytes
                && previousBoundary.isStatePageEqual(previousRef, address, bytes)) {
            return copyStateRef(previousRef, nextFrozenStateRef());
        }
        final MemoryA sink = dataWriter.beginPage();
        sink.putBlockOfBytes(address, bytes);
        final LiveViewCheckpointStatePageRef ref = nextFrozenStateRef();
        dataWriter.endPage(ref, bytes, FUNCTION_STATE_PAGE_KIND, RAW_CODEC, 1, 0);
        return ref;
    }

    /**
     * Frees both freeze scratch buffers and detaches the tracker they were
     * charged to. The writer is shared across every view its worker seals and
     * outlives any one view's operation, so neither the capacity nor the
     * tracker charge may outlive the operation that grew it: retained capacity
     * would pin one outlier image's footprint for the worker's lifetime, and a
     * surviving charge would recycle the view's pooled tracker dirty. Freeing
     * runs against the still-bound tracker, so the charge returns to zero
     * before the tracker detaches.
     * <p>
     * The grouped-freeze scratch goes back here too, which is what covers a seal that
     * threw part-way: {@code freezeGroupedFunctions} releases its own on every path it
     * reaches, and this is the outer net for the paths that never reach it.
     */
    private void releaseScratchBuffers() {
        publicationScratch.release();
        if (activeScratch == publicationScratch) {
            activeScratch = null;
        }
    }

    private FreezeScratch acquireRepairScratch(@Nullable MemoryTracker memoryTracker) {
        FreezeScratch scratch = null;
        for (int i = 0, n = repairScratchPool.size(); i < n; i++) {
            final FreezeScratch candidate = repairScratchPool.getQuick(i);
            if (!candidate.isLeased) {
                scratch = candidate;
                break;
            }
        }
        if (scratch == null) {
            scratch = new FreezeScratch();
            repairScratchPool.add(scratch);
        }
        scratch.isLeased = true;
        try {
            scratch.bind(memoryTracker);
            return scratch;
        } catch (Throwable th) {
            scratch.isLeased = false;
            throw th;
        }
    }

    private void activateRepairScratch(FreezeScratch scratch) {
        if (!scratch.isLeased) {
            throw CairoException.critical(0).put("live view checkpoint repair scratch is not leased");
        }
        activeScratch = scratch;
    }

    private void releaseRepairScratch(FreezeScratch scratch) {
        if (!scratch.isLeased) {
            throw CairoException.critical(0).put("live view checkpoint repair scratch is not leased");
        }
        try {
            scratch.release();
            if (activeScratch == scratch) {
                activeScratch = null;
            }
        } finally {
            scratch.isLeased = false;
        }
    }

    /**
     * Leases the writer's one publication shell graph and binds {@code memoryTracker}
     * to every native owner inside it. Publications never nest, so one owner
     * serves them all; the lease turns an accidental nesting into an immediate
     * failure rather than two operations quietly sharing a reader.
     */
    private PublicationScratch acquirePublicationShells(@Nullable MemoryTracker memoryTracker) {
        if (isPublicationShellsLeased) {
            throw CairoException.critical(0).put("live view checkpoint publication scratch is already in use");
        }
        if (isPartitionMapObjectPoolLeased) {
            throw CairoException.critical(0).put("live view checkpoint partition map scratch is already in use");
        }
        isPublicationShellsLeased = true;
        isPartitionMapObjectPoolLeased = true;
        try {
            publicationShells.begin(memoryTracker);
            return publicationShells;
        } catch (Throwable th) {
            isPublicationShellsLeased = false;
            isPartitionMapObjectPoolLeased = false;
            throw th;
        }
    }

    /**
     * Releases every mapping, in-flight segment and tracker-bound allocation the
     * publication held, and hands the shells back for the next one. Called from a
     * finally block on every path, so a failed publication leaves the writer as
     * reusable as a successful one.
     */
    private void releasePublicationShells() {
        if (!isPublicationShellsLeased) {
            throw CairoException.critical(0).put("live view checkpoint publication scratch is not leased");
        }
        try {
            publicationShells.end();
        } finally {
            isPublicationShellsLeased = false;
            isPartitionMapObjectPoolLeased = false;
        }
    }

    private long skipPublishedSegmentIds(Path checkpointsDir, long candidate) {
        final Path path = segmentIdProbePath;
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

    /**
     * One operation's complete freeze scratch graph. The writer keeps one owner for
     * cadence publications and leases additional owners to repair captures until they
     * close. A parked capture therefore retains its own frozen holders and arrays while
     * another view can bind and use a different owner on the same refresh worker.
     */
    private final class FreezeScratch implements Closeable {
        private final ObjList<ObjList<byte[]>> completeMemberImages = new ObjList<>();
        private final IntList completeMemberProjections = new IntList();
        private final ObjList<FrozenFunction> completeMembers = new ObjList<>();
        private final LiveViewCheckpointByteArrayPool frozenByteArrays = new LiveViewCheckpointByteArrayPool();
        private final ObjList<FrozenAnchor> frozenAnchorPool = new ObjList<>();
        private final ObjList<FrozenBoundary> frozenBoundaryPool = new ObjList<>();
        private final ObjList<FrozenFunction> frozenFunctionPool = new ObjList<>();
        private final ObjList<FrozenPartition> frozenPartitionPool = new ObjList<>();
        private final ObjList<LiveViewCheckpointStatePageRef> frozenStateRefPool = new ObjList<>();
        private final ObjList<FrozenWindowState> frozenWindowStatePool = new ObjList<>();
        // One function freeze's two passes. The keys the cursor named, and - for an
        // incremental walk only - the eviction marker each dirty record carried, which
        // the second pass cannot read back off a key the sweep has already dropped. The
        // arrays are the pool's and the list only borrows them, so it is emptied with
        // the rest of the operation's scratch rather than left holding one view's key
        // domain against the next view's work.
        private final IntList functionFreezeDirtyTombstones = new IntList();
        private final ObjList<byte[]> functionFreezeKeys = new ObjList<>();
        private final ObjList<byte[]> groupedFreezeKeys = new ObjList<>();
        private final LongList groupedFreezeLogicalBytes = new LongList();
        private final ObjList<byte[]> groupedFreezeRemovedKeys = new ObjList<>();
        private final ObjList<ObjList<byte[]>> incrementalMemberImages = new ObjList<>();
        private final IntList incrementalMemberProjections = new IntList();
        private final ObjList<FrozenFunction> incrementalMembers = new ObjList<>();
        private final MemoryCARWImpl keyBuffer =
                new MemoryCARWImpl(SCRATCH_PAGE_SIZE, SCRATCH_MAX_PAGES, MemoryTag.NATIVE_DEFAULT);
        // The order the freeze's predecessor lookups take, which is the tree's rather than
        // the map cursor's, and the pair buffer that computes it. One of each serves every
        // walk of a boundary: they run one after another, and each fills them before it
        // reads them.
        private final IntList keyLookupOrder = new IntList();
        private final LongList keyLookupPairs = new LongList();
        private final MemoryCARWImpl stateBuffer =
                new MemoryCARWImpl(SCRATCH_PAGE_SIZE, SCRATCH_MAX_PAGES, MemoryTag.NATIVE_DEFAULT);
        private int frozenAnchorPoolCursor;
        private int frozenBoundaryPoolCursor;
        private int frozenFunctionPoolCursor;
        private int frozenPartitionPoolCursor;
        private int frozenStateRefPoolCursor;
        private int frozenWindowStatePoolCursor;
        private boolean isLeased;
        private MemoryTracker memoryTracker;

        @Override
        public void close() {
            release();
            Misc.free(keyBuffer);
            Misc.free(stateBuffer);
            keyLookupOrder.clear();
            keyLookupPairs.clear();
            completeMemberImages.clear();
            completeMembers.clear();
            completeMemberProjections.clear();
            frozenAnchorPool.clear();
            frozenBoundaryPool.clear();
            frozenFunctionPool.clear();
            frozenPartitionPool.clear();
            frozenStateRefPool.clear();
            frozenWindowStatePool.clear();
            incrementalMemberImages.clear();
            incrementalMembers.clear();
            incrementalMemberProjections.clear();
        }

        private void bind(@Nullable MemoryTracker memoryTracker) {
            release();
            frozenByteArrays.reset();
            frozenAnchorPoolCursor = 0;
            frozenBoundaryPoolCursor = 0;
            frozenFunctionPoolCursor = 0;
            frozenPartitionPoolCursor = 0;
            frozenStateRefPoolCursor = 0;
            frozenWindowStatePoolCursor = 0;
            this.memoryTracker = memoryTracker;
            keyBuffer.setMemoryTracker(memoryTracker);
            stateBuffer.setMemoryTracker(memoryTracker);
        }

        private void release() {
            functionFreezeDirtyTombstones.clear();
            functionFreezeKeys.clear();
            keyBuffer.clear();
            keyBuffer.setMemoryTracker(null);
            keyLookupOrder.clear();
            keyLookupPairs.clear();
            stateBuffer.clear();
            stateBuffer.setMemoryTracker(null);
            releaseGroupedFreezeScratch(incrementalMemberImages);
            releaseGroupedFreezeScratch(completeMemberImages);
            incrementalMembers.clear();
            incrementalMemberProjections.clear();
            completeMembers.clear();
            completeMemberProjections.clear();
            memoryTracker = null;
        }

        private void releaseGroupedFreezeScratch(@NotNull ObjList<ObjList<byte[]>> memberImages) {
            groupedFreezeKeys.clear();
            groupedFreezeRemovedKeys.clear();
            groupedFreezeLogicalBytes.clear();
            for (int m = 0, n = memberImages.size(); m < n; m++) {
                memberImages.getQuick(m).clear();
            }
        }
    }

    /**
     * One boundary's anchor map: the window identity the root records, plus the
     * live {@code (key, last-seen anchor value)} pairs, index-aligned. An incremental
     * freeze adds the keys the frontier sweep dropped, which its puts cannot express.
     */
    private static final class FrozenAnchor {
        private final LongList anchorValues = new LongList();
        private final ObjList<byte[]> keys = new ObjList<>();
        private final ObjList<byte[]> removedKeys = new ObjList<>();
        private int anchorValueType;
        private boolean isIncremental;
        private byte[] keySchema;
        private long logicalStateBytes;
        private LiveViewWindow window;
        private byte[] windowName;

        private FrozenAnchor of(
                LiveViewWindow window,
                byte[] windowName,
                int anchorValueType,
                byte[] keySchema
        ) {
            this.window = window;
            this.windowName = windowName;
            this.anchorValueType = anchorValueType;
            this.keySchema = keySchema;
            anchorValues.clear();
            keys.clear();
            removedKeys.clear();
            isIncremental = false;
            logicalStateBytes = 0;
            return this;
        }
    }

    /**
     * One boundary's fused window state: the root identity, plus one complete scalar
     * payload per live key holding the anchor value and every grouped component
     * together. {@link #keys}, {@link #payloads} and {@link #isUnchanged} stay
     * index-aligned; a payload is null exactly when a repair's key domain excluded the
     * key, in which case the predecessor's whole entry stands.
     * <p>
     * The anchor arm of the same boundary is null whenever this one is set: the fused
     * root replaces the legacy anchor root as the boundary's one state root, and the
     * functions it groups are omitted from the function directory entirely.
     */
    private static final class FrozenWindowState {
        private final LongList anchorValues = new LongList();
        private final BoolList isUnchanged = new BoolList();
        private final ObjList<byte[]> keys = new ObjList<>();
        private final ObjList<byte[]> payloads = new ObjList<>();
        private final ObjList<byte[]> removedKeys = new ObjList<>();
        private int anchorValueType;
        private boolean isIncremental;
        private byte[] keySchema;
        private long logicalStateBytes;
        private byte[] manifest;
        private LiveViewWindowStatePlan plan;
        private int totalInlineStateBytes;
        private LiveViewWindow window;
        private byte[] windowIdentity;

        private FrozenWindowState of(
                LiveViewWindow window,
                LiveViewWindowStatePlan plan,
                byte[] windowIdentity,
                int anchorValueType,
                byte[] keySchema,
                byte[] manifest,
                int totalInlineStateBytes
        ) {
            this.window = window;
            this.plan = plan;
            this.windowIdentity = windowIdentity;
            this.anchorValueType = anchorValueType;
            this.keySchema = keySchema;
            this.manifest = manifest;
            this.totalInlineStateBytes = totalInlineStateBytes;
            anchorValues.clear();
            isUnchanged.clear();
            keys.clear();
            payloads.clear();
            removedKeys.clear();
            isIncremental = false;
            logicalStateBytes = 0;
            return this;
        }
    }

    /**
     * One logical boundary's frozen state: the optional anchor map plus one
     * entry per checkpoint-capable function. Function state is held only as page
     * references into an already-written data segment, so a capture that spans a
     * whole replay costs metadata rather than a copy of every state image.
     * <p>
     * Exactly one of {@link #anchor} and {@link #windowState} is ever set, and both are
     * null for a view with no anchored window. They are the two shapes of the same one
     * state root the boundary publishes.
     */
    private static final class FrozenBoundary {
        private final ObjList<FrozenFunction> functions = new ObjList<>();
        private final LiveViewCheckpointTimelineEntry oldEntry = new LiveViewCheckpointTimelineEntry();
        private FrozenAnchor anchor;
        private long effectiveLvRowPosition;
        private long logicalStateBytes;
        private FrozenWindowState windowState;

        private FrozenBoundary of() {
            functions.clear();
            oldEntry.clear();
            anchor = null;
            effectiveLvRowPosition = 0;
            logicalStateBytes = 0;
            windowState = null;
            return this;
        }
    }

    private final class FrozenFunction {
        private final LiveViewCheckpointBinaryKeyIndex partitionIndexes =
                new LiveViewCheckpointBinaryKeyIndex();
        private final ObjList<FrozenPartition> partitions = new ObjList<>();
        private final ObjList<byte[]> removedPartitions = new ObjList<>();
        private WindowFunction function;
        private byte[] identity;
        private boolean isIncremental;
        private byte[] keySchema;
        private long logicalStateBytes;
        private LiveViewCheckpointStatePageRef scalarStateRef;
        private int stateFormatVersion;

        private FrozenFunction of(
                WindowFunction function,
                byte[] identity,
                int stateFormatVersion,
                byte[] keySchema
        ) {
            this.function = function;
            this.identity = identity;
            this.stateFormatVersion = stateFormatVersion;
            this.keySchema = keySchema;
            partitionIndexes.clear();
            partitions.clear();
            removedPartitions.clear();
            isIncremental = false;
            logicalStateBytes = 0;
            scalarStateRef = null;
            return this;
        }

        /**
         * Takes one whole-state image the leaf carries itself. The image is already
         * a fresh array per partition, so it is stored rather than copied again.
         */
        private void addPartition(byte[] key, byte[] scalarState, boolean isUnchanged) {
            addPartition(nextFrozenPartition().of(key, scalarState, NO_STATE_PAGES, isUnchanged));
        }

        private void addPartition(byte[] key, LiveViewCheckpointStatePageRef stateRef, boolean isUnchanged) {
            addPartition(nextFrozenPartition().of(key, NO_BYTES, stateRef, isUnchanged));
        }

        /**
         * Takes a ring seal's entry by copy: the seal reuses one flyweight for
         * every partition it freezes.
         */
        private void addPartition(LiveViewCheckpointPartitionMapEntry entry) {
            addPartition(nextFrozenPartition().of(
                    activeScratch.frozenByteArrays.copy(entry.getKey()),
                    activeScratch.frozenByteArrays.copy(entry.getScalarState()),
                    entry,
                    false
            ));
        }

        private void addPartition(FrozenPartition partition) {
            // partitionIndexes serves two readers, and an incremental freeze has
            // neither: removeMissingPartitions, which only a full scan runs, and
            // CapturedPreviousBoundary, which only a repair capture builds - and a
            // capture always freezes completely. Skipping the insert keeps a
            // touched-key seal off one hash entry per key.
            if (!isIncremental) {
                partitionIndexes.put(0, 0, partition.key, partitions.size());
            }
            partitions.add(partition);
        }
    }

    private static final class FrozenPartition {
        // The predecessor already names these exact bytes. A full scan still keeps
        // the partition in partitionsByKey so missing-key detection sees the
        // complete live domain, but no no-op put reaches the persistent map builder.
        private boolean isUnchanged;
        private byte[] key;
        private byte[] scalarState;
        private LiveViewCheckpointStatePageRef[] statePageRefs = NO_STATE_PAGES;

        private FrozenPartition of(
                byte[] key,
                byte[] scalarState,
                LiveViewCheckpointStatePageRef[] statePageRefs,
                boolean isUnchanged
        ) {
            this.key = key;
            this.scalarState = scalarState;
            this.statePageRefs = statePageRefs;
            this.isUnchanged = isUnchanged;
            return this;
        }

        private FrozenPartition of(
                byte[] key,
                byte[] scalarState,
                LiveViewCheckpointPartitionMapEntry entry,
                boolean isUnchanged
        ) {
            final int count = entry.getStatePageCount();
            if (statePageRefs.length != count) {
                statePageRefs = new LiveViewCheckpointStatePageRef[count];
            }
            for (int i = 0; i < count; i++) {
                LiveViewCheckpointStatePageRef ref = statePageRefs[i];
                if (ref == null) {
                    ref = statePageRefs[i] = new LiveViewCheckpointStatePageRef();
                }
                copyStateRef(entry.getStatePageRef(i), ref);
            }
            return of(key, scalarState, statePageRefs, isUnchanged);
        }

        /**
         * Takes the page-backed arm's whole-state reference by copy, exactly as the
         * ring entry above does.
         * <p>
         * Storing the argument instead would put a borrowed object in this shell:
         * {@code freezeStatePage} names its page with a reference drawn from the seal's
         * pooled scratch, and the next seal rewinds that pool and hands the same object
         * out again. The shell is pooled too, so a shell this arm filled at one seal is
         * reused by the ring arm at the next - and a valueless one-chunk ring matches
         * the single-element width this arm always writes, so the ring copies straight
         * into the borrowed object. Both functions would then name one reference, and
         * the root {@code buildRoot} publishes - it reads every shell after every
         * function is frozen - would give one of them the other's page.
         */
        private FrozenPartition of(
                byte[] key,
                byte[] scalarState,
                LiveViewCheckpointStatePageRef statePageRef,
                boolean isUnchanged
        ) {
            if (statePageRefs.length != 1) {
                statePageRefs = new LiveViewCheckpointStatePageRef[1];
            }
            LiveViewCheckpointStatePageRef ref = statePageRefs[0];
            if (ref == null) {
                ref = statePageRefs[0] = new LiveViewCheckpointStatePageRef();
            }
            copyStateRef(statePageRef, ref);
            return of(key, scalarState, statePageRefs, isUnchanged);
        }

        private void copyTo(LiveViewCheckpointPartitionMapEntry out) {
            out.of(key, scalarState, statePageRefs);
        }
    }

    private FrozenAnchor nextFrozenAnchor() {
        if (activeScratch.frozenAnchorPoolCursor == activeScratch.frozenAnchorPool.size()) {
            activeScratch.frozenAnchorPool.add(new FrozenAnchor());
        }
        return activeScratch.frozenAnchorPool.getQuick(activeScratch.frozenAnchorPoolCursor++);
    }

    /**
     * @return the next boundary shell of the active capture. A repair holds every
     * boundary it froze at once, so the cursor advances per boundary and rewinds
     * only when the scratch is bound to its next operation.
     */
    private FrozenBoundary nextFrozenBoundary() {
        if (activeScratch.frozenBoundaryPoolCursor == activeScratch.frozenBoundaryPool.size()) {
            activeScratch.frozenBoundaryPool.add(new FrozenBoundary());
        }
        return activeScratch.frozenBoundaryPool.getQuick(activeScratch.frozenBoundaryPoolCursor++);
    }

    private FrozenFunction nextFrozenFunction() {
        if (activeScratch.frozenFunctionPoolCursor == activeScratch.frozenFunctionPool.size()) {
            activeScratch.frozenFunctionPool.add(new FrozenFunction());
        }
        return activeScratch.frozenFunctionPool.getQuick(activeScratch.frozenFunctionPoolCursor++);
    }

    private FrozenWindowState nextFrozenWindowState() {
        if (activeScratch.frozenWindowStatePoolCursor == activeScratch.frozenWindowStatePool.size()) {
            activeScratch.frozenWindowStatePool.add(new FrozenWindowState());
        }
        return activeScratch.frozenWindowStatePool.getQuick(activeScratch.frozenWindowStatePoolCursor++);
    }

    private FrozenPartition nextFrozenPartition() {
        if (activeScratch.frozenPartitionPoolCursor == activeScratch.frozenPartitionPool.size()) {
            activeScratch.frozenPartitionPool.add(new FrozenPartition());
        }
        return activeScratch.frozenPartitionPool.getQuick(activeScratch.frozenPartitionPoolCursor++);
    }

    private LiveViewCheckpointStatePageRef nextFrozenStateRef() {
        if (activeScratch.frozenStateRefPoolCursor == activeScratch.frozenStateRefPool.size()) {
            activeScratch.frozenStateRefPool.add(new LiveViewCheckpointStatePageRef());
        }
        return activeScratch.frozenStateRefPool.getQuick(activeScratch.frozenStateRefPoolCursor++).clear();
    }

    private static LiveViewCheckpointStatePageRef copyStateRef(
            LiveViewCheckpointStatePageRef source,
            LiveViewCheckpointStatePageRef out
    ) {
        return out.of(
                source.getSegmentId(),
                source.getOffset(),
                source.getStoredLength(),
                source.getDecodedLength(),
                source.getPageKind(),
                source.getCodec(),
                source.getRowCount(),
                source.getFlags()
        );
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

        /**
         * The previous boundary's fused entry for one key, or null when it holds none
         * this freeze may compare against. Callable only after
         * {@link #isCompatibleWindowRoot} has answered true - a manifest the entry was
         * not written under makes its bytes mean something else.
         */
        @Nullable
        LiveViewCheckpointPartitionMapEntry findWindowState(byte[] key);

        long getMaxTimestamp();

        /**
         * Whether the previous boundary's state root is a legacy anchor root, and so is
         * one an incremental anchor freeze may put touched keys into.
         * <p>
         * A boundary has exactly one state root and it is a tagged union, so a fused
         * predecessor answers false here: an incremental freeze over it would put the
         * touched keys into a tree built from empty and silently drop every key the
         * batch did not touch.
         */
        boolean hasAnchorRoot();

        /**
         * Whether the previous boundary holds a root for this function under this state
         * layout. An incremental freeze needs one for the same reason: its puts are only
         * the touched keys, and the untouched ones have to already be somewhere.
         */
        boolean hasFunctionRoot(byte[] functionIdentity, int stateFormatVersion);

        /**
         * Whether the previous boundary's state root is a window root this seal's own
         * layout may be built on: same window identity, key schema, anchor value type
         * <b>and</b> manifest, byte for byte. Anything else - a legacy anchor root, a
         * component codec bump, a reordered component - forces the full-scan conversion
         * seal.
         */
        boolean isCompatibleWindowRoot(byte[] windowIdentity, int anchorValueType, byte[] keySchema, byte[] manifest);

        /**
         * Whether the tree this freeze will be built on top of is the one this previous
         * boundary describes, whole. Every incremental affordance rests on it - freezing
         * only the touched keys, and eliding a key whose image the predecessor already
         * holds - because both leave every other key to whatever tree the root builder
         * was seeded with.
         * <p>
         * True for the published root immediately below a cadence seal, and for a
         * capture that chains: its boundary {@code i} is seeded from boundary
         * {@code i - 1}'s new root, so the two agree. False for a capture that does not,
         * which re-versions each boundary out of its own pre-repair root while sharing
         * pages against the one the replay froze before it - two different trees, so a
         * key left unimaged there would keep the stale entry rather than the fresh one.
         */
        boolean isIncrementalBase();

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
        private final LiveViewCheckpointPartitionMapEntry entry = new LiveViewCheckpointPartitionMapEntry();
        private FrozenBoundary boundary;
        private LiveViewCheckpointDataSegmentWriter dataWriter;
        private long maxTimestamp;

        private CapturedPreviousBoundary of(
                FrozenBoundary boundary,
                long maxTimestamp,
                LiveViewCheckpointDataSegmentWriter dataWriter
        ) {
            this.boundary = boundary;
            this.maxTimestamp = maxTimestamp;
            this.dataWriter = dataWriter;
            entry.clear();
            return this;
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
            final int partitionIndex = function.partitionIndexes.get(0, 0, key);
            if (partitionIndex < 0) {
                return null;
            }
            function.partitions.getQuick(partitionIndex).copyTo(entry);
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

        /**
         * Nothing. A capture re-versions the boundaries a replay produced rather than
         * appending above the runtime's own last publication, so its fused entries are
         * never the ones an unchanged-key short-circuit could stand on.
         */
        @Override
        public @Nullable LiveViewCheckpointPartitionMapEntry findWindowState(byte[] key) {
            return null;
        }

        @Override
        public long getMaxTimestamp() {
            return maxTimestamp;
        }

        /**
         * False, like every other incremental affordance a capture declines: it
         * re-versions boundaries a whole replay produced rather than appending above a
         * publication, and freezes each of them completely.
         */
        @Override
        public boolean hasAnchorRoot() {
            return false;
        }

        @Override
        public boolean hasFunctionRoot(byte[] functionIdentity, int stateFormatVersion) {
            return findFunction(functionIdentity, stateFormatVersion) != null;
        }

        @Override
        public boolean isCompatibleWindowRoot(
                byte[] windowIdentity,
                int anchorValueType,
                byte[] keySchema,
                byte[] manifest
        ) {
            return false;
        }

        /**
         * False. A non-chaining capture seeds boundary {@code i}'s root builders from
         * that boundary's own pre-repair root, while this object describes the boundary
         * the replay froze before it - so what an incremental freeze would leave unimaged
         * is not what the tree would then hold.
         */
        @Override
        public boolean isIncrementalBase() {
            return false;
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
    /**
     * One compaction's outcome, in a flyweight the writer owns and refills. It stays
     * valid until the same writer's next {@link #publishCompaction}, which is enough for
     * every caller: a refresh turn reads what it needs before it publishes
     * again. Do not retain it across publications or hand it to another thread.
     */
    public static final class CompactionResult {
        private long dataBytesAdded;
        private long generation;
        private long metadataBytesAdded;
        private int rootsRewritten;
        private LiveViewCheckpointTimelineStats stats;
        private long targetSegmentId;
        private long walPurgeFloor;

        private CompactionResult of(
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
            return this;
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
     * One localized repair publication's outcome, in a flyweight the writer owns and refills. It stays
     * valid until the same writer's next {@link #publishRepair}, which is enough for
     * every caller: a refresh turn reads what it needs before it publishes
     * again. Do not retain it across publications or hand it to another thread.
     */
    public static final class RepairResult {
        private long dataBytesAdded;
        private long generation;
        private long headRootMaxTimestamp;
        private long metadataBytesAdded;
        private int rootsVersioned;
        private LiveViewCheckpointTimelineStats stats;
        private long suffixBreakpointTimestamp;
        private long suffixRowDelta;
        private long walPurgeFloor;

        private RepairResult of(
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
            return this;
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
    /**
     * One truncate's outcome, in a flyweight the writer owns and refills. It stays
     * valid until the same writer's next {@link #publishTruncate}, which is enough for
     * every caller: a refresh turn reads what it needs before it publishes
     * again. Do not retain it across publications or hand it to another thread.
     */
    public static final class TruncateResult {
        static final TruncateResult NOT_PUBLISHED =
                new TruncateResult().of(-1, 0, -1, null, false, Numbers.LONG_NULL, Numbers.LONG_NULL);
        private long generation;
        private long headCheckpointId;
        private long headMaxTimestamp;
        private long metadataBytesAdded;
        private boolean published;
        private LiveViewCheckpointTimelineStats stats;
        private long walPurgeFloor;

        private TruncateResult of(
                long generation,
                long metadataBytesAdded,
                long walPurgeFloor,
                LiveViewCheckpointTimelineStats stats,
                boolean published,
                long headMaxTimestamp,
                long headCheckpointId
        ) {
            this.generation = generation;
            this.metadataBytesAdded = metadataBytesAdded;
            this.walPurgeFloor = walPurgeFloor;
            this.stats = stats;
            this.published = published;
            this.headMaxTimestamp = headMaxTimestamp;
            this.headCheckpointId = headCheckpointId;
            return this;
        }

        private TruncateResult of(
                long generation,
                long metadataBytesAdded,
                long walPurgeFloor,
                LiveViewCheckpointTimelineStats stats,
                long headMaxTimestamp,
                long headCheckpointId
        ) {
            return of(generation, metadataBytesAdded, walPurgeFloor, stats, true, headMaxTimestamp, headCheckpointId);
        }

        public long getGeneration() {
            return generation;
        }

        /**
         * @return the {@code checkpointId} of the newest boundary the truncate kept -
         * the head of the generation it published - or {@link Numbers#LONG_NULL} when
         * nothing was published. Paired with {@link #getHeadMaxTimestamp()} it names the
         * root a post-truncate seal builds on top of, which is what a caller needs to
         * decide whether the runtime it holds may adopt that root as its incremental
         * baseline.
         */
        public long getHeadCheckpointId() {
            return headCheckpointId;
        }

        /**
         * @return the {@code maxTimestamp} of the newest boundary the truncate kept, or
         * {@link Numbers#LONG_NULL} when nothing was published. See
         * {@link #getHeadCheckpointId()}.
         */
        public long getHeadMaxTimestamp() {
            return headMaxTimestamp;
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

    /**
     * One seal's outcome, in a flyweight the writer owns and refills. It stays
     * valid until the same writer's next {@link #append}, which is enough for
     * every caller: a refresh turn reads what it needs before it publishes
     * again. Do not retain it across publications or hand it to another thread.
     */
    public static final class Result {
        private long checkpointId;
        private long dataBytesAdded;
        private long generation;
        private long liveSegmentCount;
        private long logicalStateBytes;
        private long metadataBytesAdded;
        private long obsoleteSegmentBytes;
        private LiveViewCheckpointTimelineStats stats;
        private long walPurgeFloor;

        private Result of(
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
            return this;
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
     * Result of one cadence purge sweep. When {@link #isSwept()} is false the
     * directory held no generation this caller owns and nothing was walked; every
     * other field is zero.
     */
    /**
     * One sweep's outcome, in a flyweight the writer owns and refills. It stays
     * valid until the same writer's next {@link #sweep}, which is enough for
     * every caller: a refresh turn reads what it needs before it publishes
     * again. Do not retain it across publications or hand it to another thread.
     */
    public static final class SweepResult {
        static final SweepResult NOT_SWEPT = new SweepResult().of(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, false);
        private int catalogueEntriesVisited;
        private int failedOrphanCount;
        private int failedSegmentCount;
        private int liveSegmentCount;
        private long obsoleteBytes;
        private long purgedBytes;
        private int purgedSegmentCount;
        private int physicalEntriesVisited;
        private int queueEntriesVisited;
        private int removedOrphanCount;
        private int retirableEntryCount;
        private boolean swept;

        private SweepResult of(
                int purgedSegmentCount,
                long purgedBytes,
                int failedSegmentCount,
                int retirableEntryCount,
                int liveSegmentCount,
                long obsoleteBytes,
                int removedOrphanCount,
                int failedOrphanCount,
                int queueEntriesVisited,
                int catalogueEntriesVisited,
                int physicalEntriesVisited,
                boolean swept
        ) {
            this.purgedSegmentCount = purgedSegmentCount;
            this.purgedBytes = purgedBytes;
            this.failedSegmentCount = failedSegmentCount;
            this.retirableEntryCount = retirableEntryCount;
            this.liveSegmentCount = liveSegmentCount;
            this.obsoleteBytes = obsoleteBytes;
            this.removedOrphanCount = removedOrphanCount;
            this.failedOrphanCount = failedOrphanCount;
            this.queueEntriesVisited = queueEntriesVisited;
            this.catalogueEntriesVisited = catalogueEntriesVisited;
            this.physicalEntriesVisited = physicalEntriesVisited;
            this.swept = swept;
            return this;
        }

        private SweepResult of(
                int purgedSegmentCount,
                long purgedBytes,
                int failedSegmentCount,
                int retirableEntryCount,
                int liveSegmentCount,
                long obsoleteBytes,
                int removedOrphanCount,
                int failedOrphanCount,
                int queueEntriesVisited,
                int catalogueEntriesVisited,
                int physicalEntriesVisited
        ) {
            return of(
                    purgedSegmentCount,
                    purgedBytes,
                    failedSegmentCount,
                    retirableEntryCount,
                    liveSegmentCount,
                    obsoleteBytes,
                    removedOrphanCount,
                    failedOrphanCount,
                    queueEntriesVisited,
                    catalogueEntriesVisited,
                    physicalEntriesVisited,
                    true
            );
        }

        public int getCatalogueEntriesVisited() {
            return catalogueEntriesVisited;
        }

        /**
         * @return uncatalogued files this sweep could not unlink; the next one
         * re-derives them, since nothing durable records the attempt
         */
        public int getFailedOrphanCount() {
            return failedOrphanCount;
        }

        /**
         * @return segments this sweep could not unlink, which stay queued for the
         * next one
         */
        public int getFailedSegmentCount() {
            return failedSegmentCount;
        }

        /**
         * @return data segments a current logical root still names
         */
        public int getLiveSegmentCount() {
            return liveSegmentCount;
        }

        /**
         * @return bytes held by retired segments this sweep may not collect yet -
         * still protected by the fallback slot or a reader pin - over data and
         * metadata segments alike
         */
        public long getObsoleteBytes() {
            return obsoleteBytes;
        }

        public long getPurgedBytes() {
            return purgedBytes;
        }

        public int getPhysicalEntriesVisited() {
            return physicalEntriesVisited;
        }

        public int getPurgedSegmentCount() {
            return purgedSegmentCount;
        }

        public int getQueueEntriesVisited() {
            return queueEntriesVisited;
        }

        /**
         * @return final-name files this sweep unlinked because no generation
         * catalogues them - what a publication that renamed its segments into
         * place and then failed left behind
         */
        public int getRemovedOrphanCount() {
            return removedOrphanCount;
        }

        /**
         * @return catalogue entries this sweep left naming no file, staged for the
         * next seal of this directory to remove from the tree
         */
        public int getRetirableEntryCount() {
            return retirableEntryCount;
        }

        public boolean isSwept() {
            return swept;
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
     *
     * <h2>What the caller has to have proved</h2>
     * A frozen boundary images the runtime as the replay left it, and the
     * publication makes that image the whole of the boundary: a key the replay
     * never carried is <b>removed</b> from the root it re-versions, and a key it
     * carried is re-imaged from the rows it carried. So a capture is sound only for
     * a replay whose state at every boundary it crosses is the state a whole-history
     * replay would hold there, for every live key rather than for the keys the
     * repair's bounds were derived for - which is
     * {@link LiveViewCheckpointRepairPlan#isReplayStateKeyComplete()}, and which a
     * ROWS dependency does not give. The runtime is not held to the same standard
     * because the scratch overlay puts the pre-repair state back over it; a
     * published root has nothing to put it back from, and a later resume or restart
     * reads it as the whole truth.
     * <p>
     * A ROWS repair meets that standard the other way round, by naming the keys its
     * replay <b>does</b> describe. A capture opened with a
     * {@link LiveViewCheckpointOutputKeyDomain} images only those keys and neither
     * removes nor re-images any other, so the boundary keeps the entries the old root
     * wrote for every key the change did not touch. Both halves are load-bearing: a
     * key outside {@code Q} the replay never carried would otherwise be dropped, and
     * one it did carry would otherwise be re-imaged from a truncated history.
     */
    public class RepairCapture implements Closeable {
        private final ObjList<FrozenBoundary> boundaries = new ObjList<>();
        // The view of the boundary below the one being frozen, rebound per boundary:
        // a non-chaining capture reads only the newest staged boundary, so one shell
        // serves the whole schedule.
        private final CapturedPreviousBoundary capturedPreviousBoundary = new CapturedPreviousBoundary();
        private final CollectBoundaryVisitor collectBoundaryVisitor = new CollectBoundaryVisitor();
        private final Path checkpointsDir = new Path();
        private final Path discardPath = new Path();
        private final LiveViewCheckpointDataSegmentWriter dataWriter =
                new LiveViewCheckpointDataSegmentWriter(configuration);
        private final long dataSegmentId;
        private final long generation;
        private final boolean isChained;
        private final LiveViewCheckpointOutputKeyDomain outputKeys;
        // The entry immediately below the repaired interval, for a chaining capture:
        // the root its first boundary is seeded from and frozen against. Resolved by
        // collectBoundaries, which is already reading the pinned timeline.
        private final LiveViewCheckpointTimelineEntry predecessorEntry =
                new LiveViewCheckpointTimelineEntry();
        private final LiveViewCheckpointPageRef rowPositionDeltaRootRef = new LiveViewCheckpointPageRef();
        private final FreezeScratch scratch;
        private final LiveViewCheckpointPageRef timelineRootRef = new LiveViewCheckpointPageRef();
        // The merged view of everything below the boundary being frozen - published
        // predecessor plus the boundaries this capture has already staged over it -
        // held open across the whole chain. Null for a capture that does not chain.
        private ChainedPreviousBoundary chain;
        private boolean hasPredecessor;
        private boolean isClosed;
        private boolean isDataOpen;
        private boolean isDataPublished;

        private RepairCapture(
                Path checkpointsDir,
                long dataSegmentId,
                long generation,
                LiveViewCheckpointPageRef timelineRootRef,
                LiveViewCheckpointPageRef rowPositionDeltaRootRef,
                @Nullable LiveViewCheckpointOutputKeyDomain outputKeys,
                boolean chained,
                FreezeScratch scratch
        ) {
            this.checkpointsDir.of(checkpointsDir);
            this.dataSegmentId = dataSegmentId;
            this.generation = generation;
            this.isChained = chained;
            this.scratch = scratch;
            copy(timelineRootRef, this.timelineRootRef);
            copy(rowPositionDeltaRootRef, this.rowPositionDeltaRootRef);
            if (outputKeys != null) {
                this.outputKeys = new LiveViewCheckpointOutputKeyDomain();
                this.outputKeys.copyFrom(outputKeys);
            } else {
                this.outputKeys = null;
            }
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
            if (isClosed) {
                throw CairoException.critical(0).put("live view checkpoint repair capture is closed");
            }
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
            activateRepairScratch(scratch);
            // The replay feeds rows in canonical timestamp order and captures at
            // each boundary it crosses, so every row behind this boundary and
            // ahead of the previous one sits strictly above the previous one's
            // maxTimestamp - the same proof the cadence seal needs from its
            // caller, here by construction.
            final PreviousBoundary previousBoundary;
            final long baselineGeneration;
            if (isChained) {
                // Everything below this boundary in the repaired chain, published
                // predecessor included, as one tree - which is exactly the tree
                // publishRepair will seed this boundary's builders from. That agreement
                // is the whole licence for imaging only the keys the replay touched
                // since the boundary below.
                previousBoundary = chain;
                baselineGeneration = LiveViewCheckpointContracts.REPAIR_BASELINE_GENERATION;
            } else {
                // A non-chaining capture never freezes incrementally: it hands a
                // CapturedPreviousBoundary, which is not an incremental base, and it
                // re-versions boundaries a whole replay produced rather than appending
                // above the runtime's own last publication. LONG_NULL states that.
                previousBoundary = size == 0
                        ? null
                        : capturedPreviousBoundary.of(
                        boundaries.getQuick(size - 1),
                        boundaries.getQuick(size - 1).oldEntry.maxTimestamp,
                        dataWriter
                );
                baselineGeneration = Numbers.LONG_NULL;
            }
            final FrozenBoundary boundary = freezeBoundary(
                    dataWriter,
                    functions,
                    anchorWindow,
                    previousBoundary,
                    outputKeys,
                    baselineGeneration
            );
            boundary.oldEntry.copyFrom(entry);
            boundary.effectiveLvRowPosition = effectiveLvRowPosition;
            boundaries.add(boundary);
            if (isChained) {
                // Both ordered after the add, and both before the replay folds another
                // row: the chain has to describe the tree through this boundary before
                // the next freeze reads it, and the runtime has to name this boundary
                // rather than the one below it before another key is marked dirty. The
                // pair is what makes the next boundary cost only the keys the replay
                // touches from here.
                chain.absorb(boundary);
                adoptBoundaryBaseline(boundary, LiveViewCheckpointContracts.REPAIR_BASELINE_GENERATION);
            }
        }

        @Override
        public void close() {
            if (isClosed) {
                return;
            }
            isClosed = true;
            try {
                chain = Misc.free(chain);
                Misc.free(dataWriter);
                if (isDataOpen && !isDataPublished) {
                    LiveViewCheckpointLayout.dataSegmentTmpPath(discardPath, checkpointsDir, dataSegmentId);
                    configuration.getFilesFacade().removeQuiet(discardPath.$());
                }
            } finally {
                try {
                    boundaries.clear();
                    Misc.free(discardPath);
                    Misc.free(checkpointsDir);
                } finally {
                    releaseRepairScratch(scratch);
                }
            }
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
         * own callback. They are pooled per capture, so they stay valid until
         * this capture collects again - which no caller does.
         */
        public void collectBoundaries(
                long lowTsInclusive,
                long highTsExclusive,
                @NotNull ObjList<LiveViewCheckpointTimelineEntry> out
        ) {
            out.clear();
            hasPredecessor = false;
            chain = Misc.free(chain);
            if (timelineRootRef.isNull()) {
                return;
            }
            try (LiveViewCheckpointTimelineReader reader = new LiveViewCheckpointTimelineReader(configuration)) {
                reader.of(checkpointsDir);
                collectBoundaryVisitor.of(out);
                try {
                    reader.range(timelineRootRef, lowTsInclusive, highTsExclusive, collectBoundaryVisitor);
                } finally {
                    collectBoundaryVisitor.clearBindings();
                }
                if (isChained) {
                    // The root below the whole repaired interval: the one boundary 0 is
                    // seeded from and frozen against, and the one every later boundary
                    // reaches through the chain for a key none of them imaged. The
                    // splice re-versions nothing below its low bound, so it stays where
                    // it is and its pages stay there to share against.
                    //
                    // A repair with no predecessor is not a fault - the interval simply
                    // starts below every boundary the timeline holds. The chain then has
                    // no incremental base, boundary 0 freezes complete, and the
                    // boundaries above it chain off that.
                    hasPredecessor = reader.predecessor(timelineRootRef, lowTsInclusive, predecessorEntry);
                }
            }
            openChain();
        }

        private final class CollectBoundaryVisitor implements LiveViewCheckpointTimelineReader.Visitor {
            // The entries handed to out, as a high-water pool rather than one allocation
            // per boundary: a capture collects once, and a repair over the same view
            // re-versions about as many boundaries each time. of() rewinds the pool, so
            // what a previous collect handed out stays valid only until the next one -
            // which is what the single collect per capture the callers make guarantees.
            private final ObjList<LiveViewCheckpointTimelineEntry> pool = new ObjList<>();
            private ObjList<LiveViewCheckpointTimelineEntry> out;
            private int poolCount;

            private void clearBindings() {
                out = null;
            }

            private void of(ObjList<LiveViewCheckpointTimelineEntry> out) {
                this.out = out;
                this.poolCount = 0;
            }

            @Override
            public void onEntry(@NotNull LiveViewCheckpointTimelineEntry entry) {
                if (poolCount == pool.size()) {
                    pool.add(new LiveViewCheckpointTimelineEntry());
                }
                out.add(pool.getQuick(poolCount++).copyFrom(entry));
            }
        }

        /**
         * Resolves the effective cumulative row position of each captured timeline
         * entry against the delta root pinned with this repair generation.
         */
        public void collectEffectiveRowPositions(
                @NotNull ObjList<LiveViewCheckpointTimelineEntry> entries,
                @NotNull LongList out
        ) {
            out.clear();
            try (LiveViewCheckpointRowPositionDeltaReader deltaReader =
                         new LiveViewCheckpointRowPositionDeltaReader(configuration)) {
                deltaReader.of(checkpointsDir);
                for (int i = 0, n = entries.size(); i < n; i++) {
                    final LiveViewCheckpointTimelineEntry entry = entries.getQuick(i);
                    try {
                        out.add(Math.addExact(
                                entry.baseLvRowPosition,
                                deltaReader.prefixSum(
                                        rowPositionDeltaRootRef,
                                        entry.maxTimestamp,
                                        entry.checkpointId
                                )
                        ));
                    } catch (ArithmeticException e) {
                        throw CairoException.critical(0)
                                .put("live view checkpoint row position overflow");
                    }
                }
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

        @TestOnly
        public int getFunctionIdentityQualificationCountForTest() {
            return chain == null ? 0 : chain.getFunctionIdentityQualificationCountForTest();
        }

        @TestOnly
        public void resetFunctionIdentityQualificationCountForTest() {
            if (chain != null) {
                chain.resetFunctionIdentityQualificationCountForTest();
            }
        }

        /**
         * @return whether this capture chains: every boundary frozen against - and
         * published on top of - the one below it in the repaired interval, rather than
         * each re-versioned out of its own pre-repair root. See
         * {@link #beginRepair} for what a caller has to guarantee to ask for it
         */
        public boolean isChained() {
            return isChained;
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

        /**
         * Opens the merged view a chaining capture freezes against, over the
         * predecessor {@link #collectBoundaries} just resolved. A capture that does not
         * chain opens none, and one whose interval starts below every boundary opens one
         * with no published base - which answers "the tree holds nothing" to every probe,
         * so its first boundary freezes complete.
         */
        private void openChain() {
            if (!isChained) {
                return;
            }
            chain = new ChainedPreviousBoundary(
                    hasPredecessor ? predecessorEntry : null,
                    dataWriter
            );
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

        /**
         * The tree a chaining capture's next boundary is built on: the published root
         * immediately below the repaired interval, with every put and removal the
         * boundaries frozen so far have staged over it.
         * <p>
         * It exists because a chained freeze images only the keys the replay touched
         * since the boundary below, so the two questions it asks about every other key -
         * does the tree hold this key, and does it hold these exact bytes - are about a
         * tree that is part published and part staged in this capture's own unpublished
         * segment. Answering them by walking the frozen boundaries would cost
         * {@code O(boundaries)} per key; keeping the merged view current as each
         * boundary lands answers them in one probe.
         * <p>
         * A removal is recorded rather than erased. A key the frontier sweep dropped
         * mid-repair is absent from the staged half while the published root still holds
         * it, and reading that as "not staged, ask the published root" would re-admit the
         * entry the repair has just taken out.
         * <p>
         * Shape questions - is the state root a legacy anchor, is it a window root this
         * seal's layout may build on, does the tree hold a root for this function - go to
         * the newest frozen boundary when there is one, because that is what
         * {@code publishRepair} will have written by the time the next boundary is built
         * on it, and to the published root otherwise.
         */
        private final class ChainedPreviousBoundary implements PreviousBoundary, Closeable {
            private final LiveViewCheckpointDataSegmentWriter dataWriter;
            private final LiveViewCheckpointPartitionMapEntry entry = new LiveViewCheckpointPartitionMapEntry();
            private final LiveViewCheckpointBinaryKeyIndex functionOrdinalIndex =
                    new LiveViewCheckpointBinaryKeyIndex();
            @TestOnly
            private int functionIdentityQualificationCountForTest;
            private final LiveViewCheckpointBinaryKeyIndex partitionIndex =
                    new LiveViewCheckpointBinaryKeyIndex();
            private final ObjList<FrozenPartition> stagedPartitions = new ObjList<>();
            // A flyweight of its own, as the published boundary keeps: the fused entry
            // and a function's entry are asked for by different halves of one freeze, and
            // sharing one would have the second overwrite what the first handed back.
            private final LiveViewCheckpointPartitionMapEntry windowEntry =
                    new LiveViewCheckpointPartitionMapEntry();
            // Null when the repaired interval starts below every boundary the timeline
            // holds, in which case nothing published sits under the chain at all.
            private final RootPreviousBoundary published;
            // Owned here rather than by the boundary, which takes the cadence seal's
            // directory by reference and does not free it.
            private final LiveViewCheckpointFunctionDirectory publishedFunctionDirectory;
            private final LiveViewCheckpointPageRef publishedFunctionDirectoryRef = new LiveViewCheckpointPageRef();
            private final LiveViewCheckpointMetaStore publishedMetaStore;
            private final RootPreviousBoundary publishedPreviousBoundary;
            private final LiveViewCheckpointRoot publishedRoot;
            private final LiveViewCheckpointPageRef publishedStateRootRef = new LiveViewCheckpointPageRef();
            private final LiveViewCheckpointBinaryKeyIndex windowPayloadIndex =
                    new LiveViewCheckpointBinaryKeyIndex();
            private final ObjList<byte[]> windowPayloads = new ObjList<>();
            // Function ordinals remain stable for this chain's lifetime. The partition
            // index combines one with the state version and binary key, so no joined key
            // array or nested wrapper map is needed.
            private FrozenBoundary newest;

            private ChainedPreviousBoundary(
                    @Nullable LiveViewCheckpointTimelineEntry predecessor,
                    LiveViewCheckpointDataSegmentWriter dataWriter
            ) {
                this.dataWriter = dataWriter;
                if (predecessor == null) {
                    this.publishedFunctionDirectory = null;
                    this.publishedMetaStore = null;
                    this.publishedPreviousBoundary = null;
                    this.publishedRoot = null;
                    this.published = null;
                } else {
                    this.publishedFunctionDirectory = new LiveViewCheckpointFunctionDirectory(configuration);
                    this.publishedMetaStore = new LiveViewCheckpointMetaStore(configuration);
                    this.publishedPreviousBoundary = new RootPreviousBoundary();
                    this.publishedRoot = new LiveViewCheckpointRoot(configuration);
                    try {
                        this.published = openPublished(predecessor);
                    } catch (Throwable th) {
                        // A constructor that throws never publishes this, so openChain
                        // leaves the chain null and the capture's close() frees nothing.
                        // The metadata openPublished reads is one a repair is allowed to
                        // find unusable - the caller logs the failure and retires the
                        // timeline instead - so the shells above have to be released
                        // here, where they are still reachable, exactly as close() does.
                        publishedPreviousBoundary.close();
                        publishedPreviousBoundary.free();
                        Misc.free(publishedFunctionDirectory);
                        Misc.free(publishedMetaStore);
                        Misc.free(publishedRoot);
                        throw th;
                    }
                }
            }

            @Override
            public void close() {
                if (published != null) {
                    published.close();
                    published.free();
                }
                Misc.free(publishedFunctionDirectory);
                Misc.free(publishedMetaStore);
                Misc.free(publishedRoot);
                functionOrdinalIndex.clear();
                partitionIndex.clear();
                stagedPartitions.clear();
                windowPayloadIndex.clear();
                windowPayloads.clear();
                newest = null;
            }

            @Override
            public @Nullable LiveViewCheckpointPartitionMapEntry find(
                    byte[] functionIdentity,
                    int stateFormatVersion,
                    byte[] key
            ) {
                final int functionOrdinal = functionOrdinal(functionIdentity, false);
                if (functionOrdinal > -1) {
                    final int stagedIndex = partitionIndex.get(functionOrdinal, stateFormatVersion, key);
                    if (stagedIndex == 0) {
                        // The chain removed it. The published root may still hold an
                        // entry, and it is exactly the one that must not be seen.
                        return null;
                    }
                    if (stagedIndex > 0) {
                        stagedPartitions.getQuick(stagedIndex - 1).copyTo(entry);
                        return entry;
                    }
                }
                return published == null ? null : published.find(functionIdentity, stateFormatVersion, key);
            }

            @Override
            public @Nullable LiveViewCheckpointStatePageRef findScalarStatePage(
                    byte[] functionIdentity,
                    int stateFormatVersion
            ) {
                final FrozenFunction frozen = findNewestFunction(functionIdentity, stateFormatVersion);
                if (frozen != null && frozen.scalarStateRef != null) {
                    return frozen.scalarStateRef;
                }
                return published == null
                        ? null
                        : published.findScalarStatePage(functionIdentity, stateFormatVersion);
            }

            @Override
            public @Nullable LiveViewCheckpointPartitionMapEntry findWindowState(byte[] key) {
                final int stagedIndex = windowPayloadIndex.get(0, 0, key);
                if (stagedIndex == 0) {
                    return null;
                }
                if (stagedIndex > 0) {
                    windowEntry.of(key, windowPayloads.getQuick(stagedIndex - 1), NO_STATE_PAGES);
                    return windowEntry;
                }
                return published == null ? null : published.findWindowState(key);
            }

            @Override
            public long getMaxTimestamp() {
                if (newest != null) {
                    return newest.oldEntry.maxTimestamp;
                }
                return published == null ? Numbers.LONG_NULL : published.getMaxTimestamp();
            }

            @Override
            public boolean hasAnchorRoot() {
                if (newest != null) {
                    return newest.anchor != null;
                }
                return published != null && published.hasAnchorRoot();
            }

            @Override
            public boolean hasFunctionRoot(byte[] functionIdentity, int stateFormatVersion) {
                if (newest != null) {
                    return findNewestFunction(functionIdentity, stateFormatVersion) != null;
                }
                return published != null && published.hasFunctionRoot(functionIdentity, stateFormatVersion);
            }

            @Override
            public boolean isCompatibleWindowRoot(
                    byte[] windowIdentity,
                    int anchorValueType,
                    byte[] keySchema,
                    byte[] manifest
            ) {
                // Asked of the published root whatever the answer is taken from, because
                // asking is also what resolves its window map - and findWindowState falls
                // through to that map for every key no boundary of this chain has staged.
                // It memoises on the first call, so the chain pays one root read for the
                // whole of it.
                final boolean isPublishedCompatible = published != null
                        && published.isCompatibleWindowRoot(windowIdentity, anchorValueType, keySchema, manifest);
                if (newest == null) {
                    return isPublishedCompatible;
                }
                final FrozenWindowState state = newest.windowState;
                return state != null
                        && Arrays.equals(windowIdentity, state.windowIdentity)
                        && Arrays.equals(keySchema, state.keySchema)
                        && anchorValueType == state.anchorValueType
                        && Arrays.equals(manifest, state.manifest);
            }

            @Override
            public boolean isIncrementalBase() {
                return true;
            }

            @Override
            public boolean isStatePageEqual(LiveViewCheckpointStatePageRef ref, long address, int length) {
                if (ref.getSegmentId() == dataWriter.getSegmentId()) {
                    // Staged by an earlier boundary of this same capture, so the bytes
                    // are in the segment this writer still holds open.
                    return Vect.memeq(dataWriter.addressOfPage(ref.getOffset(), length), address, length);
                }
                return published != null && published.isStatePageEqual(ref, address, length);
            }

            /**
             * Folds one just-frozen boundary into the merged view, in the order the root
             * builder will apply it: removals first, then puts. An unchanged put is
             * absorbed like any other - the tree keeps the entry below and this carries
             * the same bytes, so the two agree either way.
             */
            private void absorb(FrozenBoundary boundary) {
                final FrozenWindowState windowState = boundary.windowState;
                if (windowState != null) {
                    for (int i = 0, n = windowState.removedKeys.size(); i < n; i++) {
                        windowPayloadIndex.put(0, 0, windowState.removedKeys.getQuick(i), 0);
                    }
                    for (int i = 0, n = windowState.keys.size(); i < n; i++) {
                        final byte[] payload = windowState.payloads.getQuick(i);
                        if (payload != null) {
                            // A null payload is a key the repair's domain excluded, whose
                            // entry the tree keeps untouched - so it is not staged either.
                            windowPayloads.add(payload);
                            windowPayloadIndex.put(0, 0, windowState.keys.getQuick(i), windowPayloads.size());
                        }
                    }
                }
                for (int f = 0, m = boundary.functions.size(); f < m; f++) {
                    final FrozenFunction frozen = boundary.functions.getQuick(f);
                    final int functionOrdinal = functionOrdinal(frozen.identity, true);
                    for (int i = 0, n = frozen.removedPartitions.size(); i < n; i++) {
                        partitionIndex.put(
                                functionOrdinal,
                                frozen.stateFormatVersion,
                                frozen.removedPartitions.getQuick(i),
                                0
                        );
                    }
                    for (int i = 0, n = frozen.partitions.size(); i < n; i++) {
                        final FrozenPartition partition = frozen.partitions.getQuick(i);
                        stagedPartitions.add(partition);
                        partitionIndex.put(
                                functionOrdinal,
                                frozen.stateFormatVersion,
                                partition.key,
                                stagedPartitions.size()
                        );
                    }
                }
                newest = boundary;
            }

            private int functionOrdinal(byte[] functionIdentity, boolean isCreate) {
                assert isCreate || incrementFunctionIdentityQualificationCountForTest();
                final int existingOrdinal = functionOrdinalIndex.get(0, 0, functionIdentity);
                if (existingOrdinal > -1) {
                    return existingOrdinal;
                }
                if (isCreate) {
                    final int ordinal = functionOrdinalIndex.size();
                    functionOrdinalIndex.put(0, 0, functionIdentity, ordinal);
                    return ordinal;
                }
                return -1;
            }

            @TestOnly
            private int getFunctionIdentityQualificationCountForTest() {
                return functionIdentityQualificationCountForTest;
            }

            private boolean incrementFunctionIdentityQualificationCountForTest() {
                functionIdentityQualificationCountForTest++;
                return true;
            }

            @TestOnly
            private void resetFunctionIdentityQualificationCountForTest() {
                functionIdentityQualificationCountForTest = 0;
            }

            private @Nullable FrozenFunction findNewestFunction(byte[] functionIdentity, int stateFormatVersion) {
                if (newest == null) {
                    return null;
                }
                for (int i = 0, n = newest.functions.size(); i < n; i++) {
                    final FrozenFunction frozen = newest.functions.getQuick(i);
                    if (frozen.stateFormatVersion == stateFormatVersion
                            && Arrays.equals(frozen.identity, functionIdentity)) {
                        return frozen;
                    }
                }
                return null;
            }

            private RootPreviousBoundary openPublished(LiveViewCheckpointTimelineEntry predecessor) {
                try {
                    publishedMetaStore.of(checkpointsDir);
                    if (!publishedMetaStore.isValid()) {
                        throw CairoException.critical(0)
                                .put("live view checkpoint repair chain has no valid generation to build on");
                    }
                    publishedRoot.of(checkpointsDir, predecessor.rootRef);
                    publishedRoot.getStateRootRef(publishedStateRootRef);
                    publishedRoot.getFunctionDirectoryRef(publishedFunctionDirectoryRef);
                    publishedFunctionDirectory.of(checkpointsDir, publishedFunctionDirectoryRef);
                    return publishedPreviousBoundary.of(
                            checkpointsDir,
                            publishedFunctionDirectory,
                            publishedMetaStore.getSuperblock().segmentDirectoryRootRef,
                            publishedStateRootRef,
                            predecessor.maxTimestamp
                    );
                } finally {
                    publishedRoot.detach();
                    publishedMetaStore.detach();
                }
            }

        }
    }

    /**
     * Shares against the published root immediately below this seal. Resolves a
     * function root once and then probes its persistent partition map per key.
     */
    private final class RootPreviousBoundary implements PreviousBoundary, Closeable {
        private final LiveViewCheckpointAnchorRoot anchorRoot = new LiveViewCheckpointAnchorRoot(configuration);
        private final Path checkpointsDir = new Path();
        private final long[] dataReaderSegmentIds = new long[PREVIOUS_DATA_READER_CACHE_SIZE];
        private final LiveViewCheckpointDataSegmentReader[] dataReaders =
                new LiveViewCheckpointDataSegmentReader[PREVIOUS_DATA_READER_CACHE_SIZE];
        private final LiveViewCheckpointPartitionMapEntry entry = new LiveViewCheckpointPartitionMapEntry();
        private final LiveViewCheckpointFunctionRoot functionRoot =
                new LiveViewCheckpointFunctionRoot(configuration);
        private final LiveViewCheckpointPartitionMapReader partitionReader =
                new LiveViewCheckpointPartitionMapReader(configuration);
        private final LiveViewCheckpointPageRef partitionRootRef = new LiveViewCheckpointPageRef();
        private final LiveViewCheckpointPageRef functionRootRef = new LiveViewCheckpointPageRef();
        private final LiveViewCheckpointStatePageRef scalarStateRef = new LiveViewCheckpointStatePageRef();
        private final LiveViewCheckpointSegmentDirectoryReader segmentDirectory =
                new LiveViewCheckpointSegmentDirectoryReader(configuration);
        private final LiveViewCheckpointSegmentDirectoryEntry segmentDirectoryEntry =
                new LiveViewCheckpointSegmentDirectoryEntry();
        private final LiveViewCheckpointPageRef stateRootRef = new LiveViewCheckpointPageRef();
        private final LiveViewCheckpointPartitionMapEntry windowEntry = new LiveViewCheckpointPartitionMapEntry();
        private final LiveViewCheckpointPageRef windowMapRootRef = new LiveViewCheckpointPageRef();
        private final LiveViewCheckpointWindowRoot windowRoot = new LiveViewCheckpointWindowRoot(configuration);
        private LiveViewCheckpointFunctionDirectory functionDirectory;
        private long maxTimestamp;
        private int dataReaderClock;
        private boolean isAnchorRoot;
        private boolean isAnchorRootResolved;
        private boolean isUnreadablePageLogged;
        private boolean isWindowRootCompatible;
        private boolean isWindowRootResolved;
        private byte[] resolvedIdentity;

        /**
         * Binds this shell to the published root a seal or a repair chain builds on.
         * Every reader it owns is retained, so a seal that runs once per cadence
         * event builds none of them; {@link #close()} - which each user calls at the
         * end of its own turn - releases the mappings and leaves the shell reusable.
         */
        private RootPreviousBoundary of(
                Path checkpointsDir,
                LiveViewCheckpointFunctionDirectory functionDirectory,
                LiveViewCheckpointPageRef segmentDirectoryRootRef,
                LiveViewCheckpointPageRef stateRootRef,
                long maxTimestamp
        ) {
            this.checkpointsDir.of(checkpointsDir);
            this.functionDirectory = functionDirectory;
            this.maxTimestamp = maxTimestamp;
            this.partitionReader.of(checkpointsDir);
            // The published catalogue is what bounds every comparison read: a page
            // is only opened against the exact file length its entry records.
            this.segmentDirectory.of(checkpointsDir, segmentDirectoryRootRef);
            copy(stateRootRef, this.stateRootRef);
            entry.clear();
            windowEntry.clear();
            functionRootRef.clear();
            partitionRootRef.clear();
            scalarStateRef.clear();
            windowMapRootRef.clear();
            resolvedIdentity = null;
            isAnchorRoot = false;
            isAnchorRootResolved = false;
            isUnreadablePageLogged = false;
            isWindowRootCompatible = false;
            isWindowRootResolved = false;
            dataReaderClock = 0;
            Arrays.fill(dataReaderSegmentIds, -1);
            return this;
        }

        /**
         * Releases every mapping this turn opened while keeping the shells. Named
         * {@code close} so the callers that borrow it can keep saying so, but it
         * frees nothing the next turn would have to rebuild.
         */
        @Override
        public void close() {
            for (int i = 0; i < PREVIOUS_DATA_READER_CACHE_SIZE; i++) {
                if (dataReaders[i] != null) {
                    dataReaders[i].close();
                }
                dataReaderSegmentIds[i] = -1;
            }
            anchorRoot.detach();
            functionRoot.detach();
            partitionReader.detach();
            segmentDirectory.detach();
            windowRoot.detach();
            functionDirectory = null;
            resolvedIdentity = null;
        }

        private void free() {
            for (int i = 0; i < PREVIOUS_DATA_READER_CACHE_SIZE; i++) {
                dataReaders[i] = Misc.free(dataReaders[i]);
                dataReaderSegmentIds[i] = -1;
            }
            Misc.free(anchorRoot);
            Misc.free(functionRoot);
            Misc.free(partitionReader);
            Misc.free(segmentDirectory);
            Misc.free(windowRoot);
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
        public @Nullable LiveViewCheckpointPartitionMapEntry findWindowState(byte[] key) {
            if (!isWindowRootCompatible) {
                return null;
            }
            return partitionReader.find(windowMapRootRef, key, windowEntry) ? windowEntry : null;
        }

        @Override
        public long getMaxTimestamp() {
            return maxTimestamp;
        }

        @Override
        public boolean hasAnchorRoot() {
            if (!isAnchorRootResolved) {
                isAnchorRootResolved = true;
                isAnchorRoot = !stateRootRef.isNull() && anchorRoot.ofIfAnchorRoot(checkpointsDir, stateRootRef);
            }
            return isAnchorRoot;
        }

        @Override
        public boolean hasFunctionRoot(byte[] functionIdentity, int stateFormatVersion) {
            return resolveFunction(functionIdentity, stateFormatVersion);
        }

        /**
         * Resolves the predecessor's state root once per seal - a boundary has exactly
         * one, and the freeze asks about it before it walks a single key.
         */
        @Override
        public boolean isCompatibleWindowRoot(
                byte[] windowIdentity,
                int anchorValueType,
                byte[] keySchema,
                byte[] manifest
        ) {
            if (isWindowRootResolved) {
                return isWindowRootCompatible;
            }
            isWindowRootResolved = true;
            isWindowRootCompatible = !stateRootRef.isNull()
                    && windowRoot.ofIfWindowRoot(checkpointsDir, stateRootRef)
                    && Arrays.equals(windowIdentity, windowRoot.getWindowIdentity())
                    && Arrays.equals(keySchema, windowRoot.getKeySchema())
                    && anchorValueType == windowRoot.getAnchorValueType()
                    && Arrays.equals(manifest, windowRoot.getManifest());
            if (isWindowRootCompatible) {
                windowRoot.getPartitionMapRootRef(windowMapRootRef);
            }
            return isWindowRootCompatible;
        }

        /**
         * True: a cadence seal builds its root on exactly the published root this
         * describes, so a key the freeze leaves unimaged keeps that root's entry.
         */
        @Override
        public boolean isIncrementalBase() {
            return true;
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
    private final class RetirementQueueSeedVisitor implements LiveViewCheckpointSegmentDirectoryReader.Visitor {
        private long liveDataSegmentCount;
        private LongList seed;

        private void clearBindings() {
            liveDataSegmentCount = 0;
            seed = null;
        }

        private void of(LongList seed) {
            this.seed = seed;
            liveDataSegmentCount = 0;
        }

        @Override
        public void onEntry(@NotNull LiveViewCheckpointSegmentDirectoryEntry entry) {
            if (entry.referenceCount == 0) {
                seed.add(entry.segmentId, entry.fileLength, entry.retireGeneration, entry.kind);
            } else if (!entry.isMetadata()) {
                liveDataSegmentCount++;
            }
        }
    }

    private final class TruncateVisitor implements LiveViewCheckpointTimelineReader.Visitor {
        private Path checkpointsDir;
        private long definitionTxn;
        private LiveViewCheckpointSegmentDirectoryWriter directoryWriter;
        private long droppedBoundaryCount;
        private long droppedLogicalStateBytes;
        private long generation;
        private LiveViewCheckpointRoot oldCheckpointRoot;
        private LongList removedSegmentIds;

        private void clearBindings() {
            checkpointsDir = null;
            definitionTxn = 0;
            directoryWriter = null;
            droppedBoundaryCount = 0;
            droppedLogicalStateBytes = 0;
            generation = 0;
            oldCheckpointRoot = null;
            removedSegmentIds = null;
        }

        private void of(
                Path checkpointsDir,
                LiveViewCheckpointRoot oldCheckpointRoot,
                LiveViewCheckpointSegmentDirectoryWriter directoryWriter,
                LongList removedSegmentIds,
                long definitionTxn,
                long generation
        ) {
            this.checkpointsDir = checkpointsDir;
            this.oldCheckpointRoot = oldCheckpointRoot;
            this.directoryWriter = directoryWriter;
            this.removedSegmentIds = removedSegmentIds;
            this.definitionTxn = definitionTxn;
            this.generation = generation;
            droppedBoundaryCount = 0;
            droppedLogicalStateBytes = 0;
        }

        @Override
        public void onEntry(@NotNull LiveViewCheckpointTimelineEntry entry) {
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
            droppedLogicalStateBytes = checkedAdd(droppedLogicalStateBytes, entry.logicalStateBytes);
            droppedBoundaryCount++;
        }
    }

    /**
     * One boundary's aggregate-pass output. The shells grow to the widest repair
     * this worker has published and every narrower one after it reuses them,
     * so a boundary contributes references rather than objects.
     */
    private static final class BatchedBoundaryState {
        private final LiveViewCheckpointPageRef stateRootRef = new LiveViewCheckpointPageRef();
        private final LiveViewCheckpointPageRef oldKeyDictionaryRef = new LiveViewCheckpointPageRef();
        private final ObjList<LiveViewCheckpointPageRef> functionRootRefs = new ObjList<>();
        private int functionRootRefCount;

        private void clear() {
            stateRootRef.clear();
            oldKeyDictionaryRef.clear();
            functionRootRefCount = 0;
        }

        private LiveViewCheckpointPageRef nextFunctionRootRef() {
            if (functionRootRefCount == functionRootRefs.size()) {
                functionRootRefs.add(new LiveViewCheckpointPageRef());
            }
            return functionRootRefs.getQuick(functionRootRefCount++);
        }
    }

    private static final class BatchedRepairRoots {
        private final ObjList<LongList> referencedSegmentIds = new ObjList<>();
        private final ObjList<LiveViewCheckpointPageRef> rootRefs = new ObjList<>();
        private int boundaryCount;
        private long rootSegmentBytes;
        private long rootSegmentId = Numbers.LONG_NULL;
        private long stateSegmentBytes;
        private long stateSegmentId = Numbers.LONG_NULL;

        private void of(int boundaryCount) {
            while (rootRefs.size() < boundaryCount) {
                referencedSegmentIds.add(new LongList());
                rootRefs.add(new LiveViewCheckpointPageRef());
            }
            for (int i = 0; i < boundaryCount; i++) {
                referencedSegmentIds.getQuick(i).clear();
                rootRefs.getQuick(i).clear();
            }
            this.boundaryCount = boundaryCount;
            rootSegmentBytes = 0;
            rootSegmentId = Numbers.LONG_NULL;
            stateSegmentBytes = 0;
            stateSegmentId = Numbers.LONG_NULL;
        }
    }

    /**
     * The Java shells one publication runs on. {@link #append}, {@link #publishRepair},
     * {@link #publishCompaction}, {@link #publishTruncate} and {@link #sweep} are all
     * top-level entry points on one refresh worker and never nest, so a single owner
     * serves them all: none of them constructs a store, reader, writer, root,
     * directory, page reference, id list or timeline entry of its own.
     * <p>
     * {@link #begin} binds the sealed view's tracker to every native owner inside;
     * {@link #end} releases every mapping, discards every in-flight segment and frees
     * every tracker-bound allocation before the writer serves another view. The
     * writer is shared across views, so nothing charged to one view's tracker may
     * outlive its publication.
     */
    private final class PublicationScratch implements Closeable {
        private final LongList addedSegmentIds = new LongList();
        private final Result appendResult = new Result();
        private final LiveViewCheckpointTimelineStats appendStats = new LiveViewCheckpointTimelineStats();
        private final LongList batchedReferenceDeltas = new LongList();
        private final ObjList<LiveViewCheckpointTimelineEntry> changedEntries = new ObjList<>();
        private final LiveViewCheckpointPageRef checkpointRootRef = new LiveViewCheckpointPageRef();
        private final CompactionResult compactionResult = new CompactionResult();
        private final LiveViewCheckpointTimelineStats compactionStats = new LiveViewCheckpointTimelineStats();
        private final LiveViewCheckpointDataSegmentWriter dataWriter =
                new LiveViewCheckpointDataSegmentWriter(configuration);
        private final LiveViewCheckpointDataStore dataStore;
        private final LiveViewCheckpointRowPositionDeltaReader deltaReader =
                new LiveViewCheckpointRowPositionDeltaReader(configuration);
        private final LiveViewCheckpointRowPositionDeltaWriter deltaWriter =
                new LiveViewCheckpointRowPositionDeltaWriter(configuration);
        private final LiveViewCheckpointSegmentDirectoryReader directorySeedReader =
                new LiveViewCheckpointSegmentDirectoryReader(configuration);
        private final LiveViewCheckpointSegmentDirectoryWriter directoryWriter =
                new LiveViewCheckpointSegmentDirectoryWriter(configuration);
        /**
         * One entry per spliced boundary, grown to the widest repair this worker has
         * published and reused by every narrower one after it.
         */
        private final ObjList<LiveViewCheckpointTimelineEntry> entryPool = new ObjList<>();
        private final LiveViewCheckpointTimelineEntry headEntry = new LiveViewCheckpointTimelineEntry();
        private final LiveViewCheckpointMetaStore metaStore = new LiveViewCheckpointMetaStore(configuration);
        private final LiveViewCheckpointPageRef newDeltaRoot = new LiveViewCheckpointPageRef();
        private final LiveViewCheckpointPageRef newDirectoryRoot = new LiveViewCheckpointPageRef();
        private final LiveViewCheckpointPageRef newRootRef = new LiveViewCheckpointPageRef();
        private final LiveViewCheckpointPageRef newTimelineRoot = new LiveViewCheckpointPageRef();
        private final LiveViewCheckpointPageRef oldStateRootRef = new LiveViewCheckpointPageRef();
        private final LiveViewCheckpointRoot oldCheckpointRoot = new LiveViewCheckpointRoot(configuration);
        private final LiveViewCheckpointPageRef oldDeltaRoot = new LiveViewCheckpointPageRef();
        private final LiveViewCheckpointPageRef oldDirectoryRoot = new LiveViewCheckpointPageRef();
        private final LiveViewCheckpointFunctionDirectory oldFunctionDirectory =
                new LiveViewCheckpointFunctionDirectory(configuration);
        private final LiveViewCheckpointPageRef oldFunctionDirectoryRef = new LiveViewCheckpointPageRef();
        private final LiveViewCheckpointPageRef oldKeyDictionaryRef = new LiveViewCheckpointPageRef();
        private final LiveViewCheckpointPageRef oldTimelineRoot = new LiveViewCheckpointPageRef();
        private final LiveViewCheckpointTimelineEntry previousEntry = new LiveViewCheckpointTimelineEntry();
        private final LiveViewCheckpointTimelineEntry probeEntry = new LiveViewCheckpointTimelineEntry();
        private final LongList removedSegmentIds = new LongList();
        private final RepairResult repairResult = new RepairResult();
        private final LiveViewCheckpointTimelineStats repairStats = new LiveViewCheckpointTimelineStats();
        private final LongList retirementExisting = new LongList();
        private final LiveViewCheckpointRetirementQueueScratch retirementQueueScratch =
                new LiveViewCheckpointRetirementQueueScratch();
        private final LongList retirementSeed = new LongList();
        private final LiveViewCheckpointRetirementQueue.State retirementState =
                new LiveViewCheckpointRetirementQueue.State();
        private final LongList reusedSegmentIds = new LongList();
        private final RootBuilders roots = new RootBuilders();
        private final LiveViewCheckpointRoot seedCheckpointRoot = new LiveViewCheckpointRoot(configuration);
        private final LiveViewCheckpointFunctionDirectory seedFunctionDirectory =
                new LiveViewCheckpointFunctionDirectory(configuration);
        private final LiveViewCheckpointPageRef seedRootRef = new LiveViewCheckpointPageRef();
        private final LiveViewCheckpointTimelineEntry suffixEntry = new LiveViewCheckpointTimelineEntry();
        private final SweepResult sweepResult = new SweepResult();
        private final LiveViewCheckpointTimelineReader timelineReader =
                new LiveViewCheckpointTimelineReader(configuration);
        private final LiveViewCheckpointTimelineWriter timelineWriter =
                new LiveViewCheckpointTimelineWriter(configuration);
        private final TruncateResult truncateResult = new TruncateResult();
        private final LiveViewCheckpointTimelineStats truncateStats = new LiveViewCheckpointTimelineStats();

        private PublicationScratch() {
            dataStore = new LiveViewCheckpointDataStore(configuration, metaStore);
        }

        @Override
        public void close() {
            Misc.free(dataStore);
            Misc.free(dataWriter);
            Misc.free(deltaReader);
            Misc.free(deltaWriter);
            Misc.free(directorySeedReader);
            Misc.free(directoryWriter);
            Misc.free(metaStore);
            Misc.free(oldCheckpointRoot);
            Misc.free(oldFunctionDirectory);
            Misc.free(retirementQueueScratch);
            Misc.free(roots);
            Misc.free(seedCheckpointRoot);
            Misc.free(seedFunctionDirectory);
            Misc.free(timelineReader);
            Misc.free(timelineWriter);
            changedEntries.clear();
            entryPool.clear();
        }

        /**
         * @return the {@code count} reusable timeline entries this publication
         * splices, cleared and index-aligned with the boundaries they carry
         */
        private ObjList<LiveViewCheckpointTimelineEntry> entries(int count) {
            while (entryPool.size() < count) {
                entryPool.add(new LiveViewCheckpointTimelineEntry());
            }
            return entryPool;
        }

        private void begin(@Nullable MemoryTracker memoryTracker) {
            roots.bind(memoryTracker);
        }

        private void end() {
            try {
                dataStore.detach();
                dataWriter.discard();
                deltaReader.detach();
                deltaWriter.detach();
                directorySeedReader.detach();
                directoryWriter.detach();
                metaStore.detach();
                oldCheckpointRoot.detach();
                oldFunctionDirectory.detach();
                seedCheckpointRoot.detach();
                seedFunctionDirectory.detach();
                timelineReader.detach();
                timelineWriter.detach();
                addedSegmentIds.clear();
                batchedReferenceDeltas.clear();
                removedSegmentIds.clear();
                retirementExisting.clear();
                retirementSeed.clear();
                reusedSegmentIds.clear();
                // changedEntries and entryPool keep their shells: they hold no
                // native memory and the next publication over any view reuses them.
            } finally {
                roots.end();
            }
        }
    }

    private final class RootBuilders implements Closeable {
        private final LiveViewCheckpointAnchorRootBuilder anchorRootBuilder;
        private final LiveViewCheckpointMetaSegmentWriter aggregateRootWriter =
                new LiveViewCheckpointMetaSegmentWriter(configuration);
        private final LiveViewCheckpointMetaSegmentWriter aggregateStateWriter =
                new LiveViewCheckpointMetaSegmentWriter(configuration);
        private final LiveViewCheckpointRoot batchCheckpointRoot =
                new LiveViewCheckpointRoot(configuration);
        private final LiveViewCheckpointFunctionDirectory batchFunctionDirectory =
                new LiveViewCheckpointFunctionDirectory(configuration);
        private final LiveViewCheckpointPageRef batchFunctionDirectoryRef = new LiveViewCheckpointPageRef();
        private final LiveViewCheckpointPageRef batchOldStateRootRef = new LiveViewCheckpointPageRef();
        private final LiveViewCheckpointRootBuilder checkpointRootBuilder =
                new LiveViewCheckpointRootBuilder(configuration);
        private final Path checkpointsDir = new Path();
        private final LiveViewCheckpointKeyDictionaryWriter dictionaryWriter =
                new LiveViewCheckpointKeyDictionaryWriter(configuration);
        private final LiveViewCheckpointPageRef dictionaryRefOut = new LiveViewCheckpointPageRef();
        private final LongList dictionarySegmentIds = new LongList();
        /**
         * Opened only by {@link #carryKeyDictionaryThroughRedirect} - a redirected root's own
         * dictionary chunks never move, so this reader exists only to re-enumerate the segments
         * an unchanged {@code keyDictionaryRef} still names, not to read or rewrite them.
         */
        private final LiveViewCheckpointKeyDictionaryReader redirectDictionaryReader =
                new LiveViewCheckpointKeyDictionaryReader(configuration);
        private final LiveViewCheckpointPageRef redirectKeyDictionaryRef = new LiveViewCheckpointPageRef();
        private final LiveViewCheckpointFunctionRootBuilder functionRootBuilder;
        private final LiveViewCheckpointFunctionRoot oldFunctionRoot =
                new LiveViewCheckpointFunctionRoot(configuration);
        private final LiveViewCheckpointPartitionMapReader oldPartitionReader =
                new LiveViewCheckpointPartitionMapReader(configuration);
        private final LiveViewCheckpointPageRef redirectStateRootRef = new LiveViewCheckpointPageRef();
        private final LiveViewCheckpointRoot redirectCheckpointRoot =
                new LiveViewCheckpointRoot(configuration);
        private final LiveViewCheckpointFunctionDirectory redirectFunctionDirectory =
                new LiveViewCheckpointFunctionDirectory(configuration);
        private final LiveViewCheckpointPageRef redirectFunctionDirectoryRef = new LiveViewCheckpointPageRef();
        private final LiveViewCheckpointPageRef redirectNewFunctionRootRef = new LiveViewCheckpointPageRef();
        private final LiveViewCheckpointPageRef redirectOldFunctionRootRef = new LiveViewCheckpointPageRef();
        private final LiveViewCheckpointPageRef redirectPartitionMapRoot = new LiveViewCheckpointPageRef();
        private final ObjList<BatchedBoundaryState> batchedBoundaryStates = new ObjList<>();
        private final BatchedRepairRoots batchedRepairRoots = new BatchedRepairRoots();
        private final LiveViewCheckpointPageRef buildStateRootRef = new LiveViewCheckpointPageRef();
        private final LiveViewCheckpointPageRef buildFunctionRootRef = new LiveViewCheckpointPageRef();
        private final LiveViewCheckpointPageRef buildOldFunctionRootRef = new LiveViewCheckpointPageRef();
        private final LiveViewCheckpointPageRef aggregateFunctionRootRef = new LiveViewCheckpointPageRef();
        private final LiveViewCheckpointPageRef aggregateOldFunctionRootRef = new LiveViewCheckpointPageRef();
        private final LiveViewCheckpointPageRef redirectOldPartitionRoot = new LiveViewCheckpointPageRef();
        private final IntObjHashMap<LiveViewCheckpointStatePageRef[]> redirectRefBuffersByWidth = new IntObjHashMap<>();
        private final LiveViewCheckpointStatePageRef redirectScalarRef = new LiveViewCheckpointStatePageRef();
        private final LiveViewCheckpointWindowRootBuilder windowRootBuilder;
        /**
         * {@code (segmentId, fileLength)} of every metadata segment the boundary
         * built last wrote, for the caller to catalogue. Reset per boundary, so a
         * repair's per-root reference transaction sees only its own.
         */
        private final LongList writtenMetaSegments = new LongList();
        private int redirectRefWidthLookupCountForTest;
        /**
         * Entries the last {@link #redirectTimelineEntries} rewrote. The list they
         * sit in is a high-water pool, so this - not its size - is what the splice
         * that follows must read.
         */
        private int lastChangedEntryCount;
        private long metadataBytesAdded;
        private long nextSegmentId;

        private RootBuilders() {
            anchorRootBuilder = new LiveViewCheckpointAnchorRootBuilder(
                    configuration,
                    null,
                    partitionMapObjectPool
            );
            functionRootBuilder = new LiveViewCheckpointFunctionRootBuilder(
                    configuration,
                    null,
                    partitionMapObjectPool
            );
            windowRootBuilder = new LiveViewCheckpointWindowRootBuilder(
                    configuration,
                    null,
                    partitionMapObjectPool
            );
        }

        /**
         * Binds {@code memoryTracker} to every staging arena for the publication
         * that is starting. The builders are retained across views, so each bind
         * frees what the previous view's tracker charged before the new one
         * acquires anything.
         */
        private void bind(@Nullable MemoryTracker memoryTracker) {
            anchorRootBuilder.bindMemoryTracker(memoryTracker);
            functionRootBuilder.bindMemoryTracker(memoryTracker);
            windowRootBuilder.bindMemoryTracker(memoryTracker);
        }

        /**
         * Releases every mapping and in-flight segment the publication held, then
         * frees the staging arenas against the tracker that acquired them. The
         * Java shells stay, so the next publication builds none of them.
         */
        private void end() {
            try {
                anchorRootBuilder.detach();
                functionRootBuilder.detach();
                windowRootBuilder.detach();
                aggregateRootWriter.discard();
                aggregateStateWriter.discard();
                batchCheckpointRoot.detach();
                batchFunctionDirectory.detach();
                checkpointRootBuilder.detach();
                dictionaryWriter.detach();
                redirectDictionaryReader.detach();
                oldFunctionRoot.detach();
                oldPartitionReader.detach();
                redirectCheckpointRoot.detach();
                redirectFunctionDirectory.detach();
                writtenMetaSegments.clear();
                metadataBytesAdded = 0;
                nextSegmentId = 0;
            } finally {
                anchorRootBuilder.releaseMemoryTracker();
                functionRootBuilder.releaseMemoryTracker();
                windowRootBuilder.releaseMemoryTracker();
            }
        }

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

            // Neither shape of state root names a data segment - an anchor entry carries
            // its anchor value in the leaf scalar, a fused entry its whole component
            // payload - so the state root is reused by reference untouched across a
            // compaction, whichever arm of the tagged union it holds.
            redirectCheckpointRoot.getStateRootRef(redirectStateRootRef);
            redirectCheckpointRoot.getFunctionDirectoryRef(redirectFunctionDirectoryRef);
            redirectFunctionDirectory.of(checkpointsDir, redirectFunctionDirectoryRef);

            nextSegmentId = skipPublishedSegmentIds(checkpointsDir, nextSegmentId);
            checkpointRootBuilder.begin(
                    checkpointsDir,
                    redirectCheckpointRoot.getCheckpointId(),
                    redirectCheckpointRoot.getMaxTimestamp(),
                    definitionTxn,
                    redirectStateRootRef
            );
            carryKeyDictionaryThroughRedirect();
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
         * Carries a redirected root's key dictionary reference forward unchanged. Section 6.2
         * keeps dictionary chunks metadata-resident precisely so compaction never has to redirect
         * them - only the segments they already sit in stay live, which is why this reads and
         * re-folds their ids rather than calling {@link #dictionaryWriter} at all. A root with no
         * key dictionary (no bound SYMBOL partition column) leaves {@code checkpointRootBuilder}
         * untouched, publishing null exactly as {@code begin()} already left it.
         */
        private void carryKeyDictionaryThroughRedirect() {
            redirectCheckpointRoot.getKeyDictionaryRef(redirectKeyDictionaryRef);
            if (redirectKeyDictionaryRef.isNull()) {
                return;
            }
            redirectDictionaryReader.of(checkpointsDir, redirectKeyDictionaryRef);
            dictionarySegmentIds.clear();
            dictionarySegmentIds.add(redirectKeyDictionaryRef.getSegmentId());
            for (int c = 0, cn = redirectDictionaryReader.getColumnCount(); c < cn; c++) {
                for (int k = 0, kn = redirectDictionaryReader.getChunkCount(c); k < kn; k++) {
                    dictionarySegmentIds.add(redirectDictionaryReader.getChunkRef(c, k).getSegmentId());
                }
            }
            checkpointRootBuilder.setKeyDictionaryRef(redirectKeyDictionaryRef, dictionarySegmentIds);
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
            functionRootBuilder.ofBorrowedCompiled(
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
            redirectPartitionVisitor.of(this, plan);
            try {
                oldPartitionReader.iterateAll(redirectPartitionMapRoot, redirectPartitionVisitor);
            } finally {
                redirectPartitionVisitor.clearBindings();
            }
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
                        newRefs = redirectRefs(count);
                        for (int j = 0; j < i; j++) {
                            copyStateRef(entry.getStatePageRef(j), newRefs[j]);
                        }
                    }
                    copyStateRef(target, newRefs[i]);
                } else if (newRefs != null) {
                    copyStateRef(entry.getStatePageRef(i), newRefs[i]);
                }
            }
            if (newRefs != null) {
                functionRootBuilder.putPartition(entry.getKey(), entry.getScalarState(), newRefs);
            }
        }

        private void copyStateRef(LiveViewCheckpointStatePageRef from, LiveViewCheckpointStatePageRef to) {
            to.of(
                    from.getSegmentId(), from.getOffset(), from.getStoredLength(), from.getDecodedLength(),
                    from.getPageKind(), from.getCodec(), from.getRowCount(), from.getFlags()
            );
        }

        private LiveViewCheckpointStatePageRef[] redirectRefs(int count) {
            assert isRedirectRefWidthLookupRecordedForTest();
            LiveViewCheckpointStatePageRef[] refs = redirectRefBuffersByWidth.get(count);
            if (refs != null) {
                return refs;
            }
            refs = new LiveViewCheckpointStatePageRef[count];
            for (int i = 0; i < count; i++) {
                refs[i] = new LiveViewCheckpointStatePageRef();
            }
            redirectRefBuffersByWidth.put(count, refs);
            return refs;
        }

        @TestOnly
        private int getRedirectRefWidthLookupCountForTest() {
            return redirectRefWidthLookupCountForTest;
        }

        private boolean isRedirectRefWidthLookupRecordedForTest() {
            redirectRefWidthLookupCountForTest++;
            return true;
        }

        /**
         * Builds a non-chained partial-key repair in two aggregate commits. The
         * first pass writes every state root, making those pages readable; the
         * second writes every function directory and checkpoint root. Nothing
         * is published until the caller has subsequently spliced the timeline,
         * updated the segment directory and committed the superblock.
         */
        private BatchedRepairRoots buildRepairRootsBatched(
                RepairCapture capture,
                long definitionTxn,
                @Nullable LiveViewSymbolIdRegistry partitionKeyRegistry
        ) {
            assert !capture.isChained() && capture.outputKeys != null;
            final int boundaryCount = capture.size();
            final ObjList<BatchedBoundaryState> states = batchedBoundaryStates;
            while (states.size() < boundaryCount) {
                states.add(new BatchedBoundaryState());
            }
            final BatchedRepairRoots result = batchedRepairRoots;
            result.of(boundaryCount);

            nextSegmentId = skipPublishedSegmentIds(checkpointsDir, nextSegmentId);
            final long stateSegmentId = nextSegmentId++;
            result.stateSegmentId = stateSegmentId;
            aggregateStateWriter.of(checkpointsDir, stateSegmentId);
            try {
                for (int i = 0; i < boundaryCount; i++) {
                    final FrozenBoundary boundary = capture.boundaries.getQuick(i);
                    final LiveViewCheckpointTimelineEntry oldEntry = boundary.oldEntry;
                    batchCheckpointRoot.of(checkpointsDir, oldEntry.rootRef);
                    if (batchCheckpointRoot.getCheckpointId() != oldEntry.checkpointId
                            || batchCheckpointRoot.getMaxTimestamp() != oldEntry.maxTimestamp
                            || batchCheckpointRoot.getDefinitionTxn() != definitionTxn) {
                        throw CairoException.critical(CairoException.LV_CHECKPOINT_TIMELINE_INVALID)
                                .put("live view checkpoint repair root identity mismatch [checkpointId=")
                                .put(oldEntry.checkpointId).put(']');
                    }
                    batchCheckpointRoot.getStateRootRef(batchOldStateRootRef);
                    batchCheckpointRoot.getFunctionDirectoryRef(batchFunctionDirectoryRef);
                    batchFunctionDirectory.of(checkpointsDir, batchFunctionDirectoryRef);
                    final BatchedBoundaryState state = states.getQuick(i);
                    state.clear();
                    // Each boundary here is independent (buildRepairRootsBatched asserts
                    // !capture.isChained()), so this is that boundary's own pre-repair
                    // dictionary, not a chain - captured now, while batchCheckpointRoot
                    // still names it, for the second loop below to path-copy from.
                    batchCheckpointRoot.getKeyDictionaryRef(state.oldKeyDictionaryRef);
                    buildBoundaryStateIntoAggregate(
                            boundary,
                            batchOldStateRootRef,
                            batchFunctionDirectory,
                            capture.outputKeys,
                            stateSegmentId,
                            aggregateStateWriter,
                            state
                    );
                }
                if (aggregateStateWriter.getPageCount() == 0) {
                    aggregateStateWriter.discard();
                    result.stateSegmentId = Numbers.LONG_NULL;
                } else {
                    result.stateSegmentBytes = aggregateStateWriter.commit();
                    metadataBytesAdded = checkedAdd(metadataBytesAdded, result.stateSegmentBytes);
                }
            } catch (Throwable th) {
                aggregateStateWriter.discard();
                throw th;
            }

            nextSegmentId = skipPublishedSegmentIds(checkpointsDir, nextSegmentId);
            final long rootSegmentId = nextSegmentId++;
            result.rootSegmentId = rootSegmentId;
            aggregateRootWriter.of(checkpointsDir, rootSegmentId);
            try {
                for (int i = 0; i < boundaryCount; i++) {
                    final FrozenBoundary boundary = capture.boundaries.getQuick(i);
                    final LiveViewCheckpointTimelineEntry oldEntry = boundary.oldEntry;
                    final BatchedBoundaryState state = states.getQuick(i);
                    checkpointRootBuilder.begin(
                            checkpointsDir,
                            oldEntry.checkpointId,
                            oldEntry.maxTimestamp,
                            definitionTxn,
                            state.stateRootRef
                    );
                    for (int f = 0, n = state.functionRootRefCount; f < n; f++) {
                        checkpointRootBuilder.addFunction(state.functionRootRefs.getQuick(f));
                    }
                    if (partitionKeyRegistry != null) {
                        final LiveViewCheckpointKeyDictionaryColumnSource columnSource =
                                partitionKeyRegistry.newDictionaryColumnSource();
                        if (columnSource.getColumnCount() > 0) {
                            dictionaryWriter.writeIntoOpenSegment(
                                    state.oldKeyDictionaryRef,
                                    columnSource,
                                    aggregateRootWriter,
                                    dictionaryRefOut
                            );
                            checkpointRootBuilder.setKeyDictionaryRef(dictionaryRefOut, dictionaryWriter.getReferencedSegmentIds());
                        }
                    }
                    checkpointRootBuilder.buildIntoOpenSegment(
                            rootSegmentId,
                            aggregateRootWriter,
                            result.rootRefs.getQuick(i)
                    );
                    checkpointRootBuilder.getReferencedSegmentIds(result.referencedSegmentIds.getQuick(i));
                }
                result.rootSegmentBytes = aggregateRootWriter.commit();
                metadataBytesAdded = checkedAdd(metadataBytesAdded, result.rootSegmentBytes);
            } catch (Throwable th) {
                aggregateRootWriter.discard();
                throw th;
            }
            return result;
        }

        private void buildBoundaryStateIntoAggregate(
                FrozenBoundary boundary,
                LiveViewCheckpointPageRef oldStateRootRef,
                LiveViewCheckpointFunctionDirectory oldFunctionDirectory,
                LiveViewCheckpointOutputKeyDomain outputKeys,
                long stateSegmentId,
                LiveViewCheckpointMetaSegmentWriter writer,
                BatchedBoundaryState out
        ) {
            lastBoundaryPartitionPuts = 0;
            if (boundary.windowState != null) {
                final FrozenWindowState windowState = boundary.windowState;
                windowRootBuilder.ofBorrowedCompiled(
                        checkpointsDir,
                        oldStateRootRef,
                        windowState.windowIdentity,
                        windowState.anchorValueType,
                        windowState.keySchema,
                        windowState.manifest,
                        windowState.totalInlineStateBytes,
                        !windowState.isIncremental,
                        outputKeys
                );
                for (int i = 0, n = windowState.removedKeys.size(); i < n; i++) {
                    windowRootBuilder.removePartition(windowState.removedKeys.getQuick(i));
                }
                for (int i = 0, n = windowState.keys.size(); i < n; i++) {
                    final byte[] payload = windowState.payloads.getQuick(i);
                    if (payload == null) {
                        continue;
                    }
                    final boolean isUnchanged = windowState.isUnchanged.get(i);
                    windowRootBuilder.putPartition(windowState.keys.getQuick(i), payload, isUnchanged);
                    if (!isUnchanged) {
                        lastBoundaryPartitionPuts++;
                    }
                }
                windowRootBuilder.buildIntoOpenSegment(stateSegmentId, writer, out.stateRootRef);
            } else if (boundary.anchor != null) {
                final FrozenAnchor anchor = boundary.anchor;
                anchorRootBuilder.ofBorrowedCompiled(
                        checkpointsDir,
                        oldStateRootRef,
                        anchor.windowName,
                        anchor.anchorValueType,
                        anchor.keySchema,
                        !anchor.isIncremental
                );
                for (int i = 0, n = anchor.removedKeys.size(); i < n; i++) {
                    anchorRootBuilder.removePartition(anchor.removedKeys.getQuick(i));
                }
                for (int i = 0, n = anchor.keys.size(); i < n; i++) {
                    anchorRootBuilder.putPartition(anchor.keys.getQuick(i), anchor.anchorValues.getQuick(i));
                }
                anchorRootBuilder.buildIntoOpenSegment(stateSegmentId, writer, out.stateRootRef);
            }

            final LiveViewCheckpointPageRef oldFunctionRootRef = aggregateOldFunctionRootRef;
            final LiveViewCheckpointPageRef functionRootRef = aggregateFunctionRootRef;
            for (int i = 0, n = boundary.functions.size(); i < n; i++) {
                final FrozenFunction frozen = boundary.functions.getQuick(i);
                oldFunctionRootRef.clear();
                oldFunctionDirectory.find(frozen.identity, oldFunctionRootRef);
                functionRootBuilder.ofBorrowedCompiled(
                        checkpointsDir,
                        oldFunctionRootRef,
                        frozen.identity,
                        frozen.stateFormatVersion,
                        frozen.keySchema
                );
                if (frozen.scalarStateRef != null) {
                    functionRootBuilder.setScalarStateRef(frozen.scalarStateRef);
                } else {
                    if (!frozen.isIncremental) {
                        removeMissingPartitions(oldFunctionRootRef, frozen, outputKeys);
                    }
                    for (int p = 0, m = frozen.removedPartitions.size(); p < m; p++) {
                        functionRootBuilder.removePartition(frozen.removedPartitions.getQuick(p));
                    }
                    for (int p = 0, m = frozen.partitions.size(); p < m; p++) {
                        final FrozenPartition partition = frozen.partitions.getQuick(p);
                        if (!partition.isUnchanged) {
                            functionRootBuilder.putPartition(
                                    partition.key,
                                    partition.scalarState,
                                    partition.statePageRefs
                            );
                            lastBoundaryPartitionPuts++;
                        }
                    }
                }
                functionRootBuilder.buildIntoOpenSegment(stateSegmentId, writer, functionRootRef);
                copy(functionRootRef, out.nextFunctionRootRef());
            }
        }

        /**
         * Writes the boundary's one state root - fused window root or legacy anchor
         * root, whichever the freeze produced - one function root per frozen function,
         * and the checkpoint root itself. The two old-root arguments are the boundary's
         * predecessor: the builders start from its state/function/partition-map
         * paths, so an unchanged entry is reused by reference rather than
         * rewritten. Both are empty for the first root of a timeline.
         * <p>
         * {@code oldStateRootRef} is the predecessor's state root whichever kind it is.
         * A window root over a legacy predecessor - or over one whose manifest moved -
         * simply finds nothing to build on and writes a whole new tree, which is what
         * the format-conversion seal is.
         * <p>
         * {@code outputKeys} is the repair capture's key domain, and it decides only
         * which of the predecessor's entries this root may retire - the freeze already
         * left every key outside it unimaged, so the put loop needs no filter of its
         * own. Null is the whole-truth build every cadence seal makes.
         * <p>
         * {@code oldKeyDictionaryRef} is the predecessor's key dictionary reference, empty for
         * the first root of a timeline or a predecessor with no bound SYMBOL partition column -
         * the same "empty rather than null" convention {@code oldStateRootRef} already follows.
         * {@code partitionKeyRegistry} is the view's own translator registry, or null for a view
         * whose classifier never bound one; when it is non-null but has bound no source column
         * (an expression-keyed or non-SYMBOL-keyed view, translator aside), this root still
         * publishes no key dictionary reference, exactly as {@link LiveViewCheckpointRootBuilder
         * #setKeyDictionaryRef} already documents as optional.
         */
        private void buildRoot(
                FrozenBoundary boundary,
                LiveViewCheckpointPageRef oldStateRootRef,
                @Nullable LiveViewCheckpointFunctionDirectory oldFunctionDirectory,
                @Nullable LiveViewCheckpointOutputKeyDomain outputKeys,
                long checkpointId,
                long maxTimestamp,
                long definitionTxn,
                LiveViewCheckpointPageRef oldKeyDictionaryRef,
                @Nullable LiveViewSymbolIdRegistry partitionKeyRegistry,
                LiveViewCheckpointPageRef rootRefOut,
                LongList referencedSegmentIdsOut
        ) {
            writtenMetaSegments.clear();
            lastBoundaryPartitionPuts = 0;
            final LiveViewCheckpointPageRef stateRootRef = buildStateRootRef;
            stateRootRef.clear();
            if (boundary.windowState != null) {
                final FrozenWindowState windowState = boundary.windowState;
                windowRootBuilder.ofBorrowedCompiled(
                        checkpointsDir,
                        oldStateRootRef,
                        windowState.windowIdentity,
                        windowState.anchorValueType,
                        windowState.keySchema,
                        windowState.manifest,
                        windowState.totalInlineStateBytes,
                        !windowState.isIncremental,
                        outputKeys
                );
                // Removals first, mirroring the anchor and function paths below. A
                // complete snapshot carries none - it removes by omission in build() - so
                // the two rules never name one key twice.
                for (int i = 0, n = windowState.removedKeys.size(); i < n; i++) {
                    windowRootBuilder.removePartition(windowState.removedKeys.getQuick(i));
                }
                for (int i = 0, n = windowState.keys.size(); i < n; i++) {
                    final byte[] payload = windowState.payloads.getQuick(i);
                    if (payload == null) {
                        // Outside the repair's key domain: the freeze imaged nothing for
                        // it and the predecessor's entry stands, so it is not put and -
                        // because the removal pass filters by the same domain - not
                        // removed either.
                        continue;
                    }
                    final boolean isUnchanged = windowState.isUnchanged.get(i);
                    windowRootBuilder.putPartition(windowState.keys.getQuick(i), payload, isUnchanged);
                    if (!isUnchanged) {
                        lastBoundaryPartitionPuts++;
                    }
                }
                nextSegmentId = skipPublishedSegmentIds(checkpointsDir, nextSegmentId);
                final long windowSegmentId = nextSegmentId++;
                windowRootBuilder.build(windowSegmentId, stateRootRef);
                metadataBytesAdded = checkedAdd(metadataBytesAdded, windowRootBuilder.getLastSegmentBytes());
                writtenMetaSegments.add(windowSegmentId, windowRootBuilder.getLastSegmentBytes());
            } else if (boundary.anchor != null) {
                final FrozenAnchor anchor = boundary.anchor;
                anchorRootBuilder.ofBorrowedCompiled(
                        checkpointsDir,
                        oldStateRootRef,
                        anchor.windowName,
                        anchor.anchorValueType,
                        anchor.keySchema,
                        !anchor.isIncremental
                );
                // Removals first, mirroring the function path below. A complete snapshot
                // carries none - it removes by omission in build() - so the two rules
                // never name one key twice.
                for (int i = 0, n = anchor.removedKeys.size(); i < n; i++) {
                    anchorRootBuilder.removePartition(anchor.removedKeys.getQuick(i));
                }
                for (int i = 0, n = anchor.keys.size(); i < n; i++) {
                    anchorRootBuilder.putPartition(anchor.keys.getQuick(i), anchor.anchorValues.getQuick(i));
                }
                nextSegmentId = skipPublishedSegmentIds(checkpointsDir, nextSegmentId);
                final long anchorSegmentId = nextSegmentId++;
                anchorRootBuilder.build(anchorSegmentId, stateRootRef);
                metadataBytesAdded = checkedAdd(metadataBytesAdded, anchorRootBuilder.getLastSegmentBytes());
                writtenMetaSegments.add(anchorSegmentId, anchorRootBuilder.getLastSegmentBytes());
            }

            nextSegmentId = skipPublishedSegmentIds(checkpointsDir, nextSegmentId);
            checkpointRootBuilder.begin(
                    checkpointsDir,
                    checkpointId,
                    maxTimestamp,
                    definitionTxn,
                    stateRootRef
            );
            final LiveViewCheckpointPageRef oldFunctionRootRef = buildOldFunctionRootRef;
            final LiveViewCheckpointPageRef functionRootRef = buildFunctionRootRef;
            for (int i = 0, n = boundary.functions.size(); i < n; i++) {
                final FrozenFunction frozen = boundary.functions.getQuick(i);
                oldFunctionRootRef.clear();
                if (oldFunctionDirectory != null) {
                    oldFunctionDirectory.find(frozen.identity, oldFunctionRootRef);
                }
                functionRootBuilder.ofBorrowedCompiled(
                        checkpointsDir,
                        oldFunctionRootRef,
                        frozen.identity,
                        frozen.stateFormatVersion,
                        frozen.keySchema
                );
                if (frozen.scalarStateRef != null) {
                    functionRootBuilder.setScalarStateRef(frozen.scalarStateRef);
                } else {
                    if (!frozen.isIncremental) {
                        removeMissingPartitions(oldFunctionRootRef, frozen, outputKeys);
                    }
                    for (int p = 0, m = frozen.removedPartitions.size(); p < m; p++) {
                        functionRootBuilder.removePartition(frozen.removedPartitions.getQuick(p));
                    }
                    for (int p = 0, m = frozen.partitions.size(); p < m; p++) {
                        final FrozenPartition partition = frozen.partitions.getQuick(p);
                        if (!partition.isUnchanged) {
                            functionRootBuilder.putPartition(
                                    partition.key,
                                    partition.scalarState,
                                    partition.statePageRefs
                            );
                            lastBoundaryPartitionPuts++;
                        }
                    }
                }
                nextSegmentId = skipPublishedSegmentIds(checkpointsDir, nextSegmentId);
                final long functionSegmentId = nextSegmentId++;
                functionRootBuilder.build(functionSegmentId, functionRootRef);
                metadataBytesAdded = checkedAdd(metadataBytesAdded, functionRootBuilder.getLastSegmentBytes());
                writtenMetaSegments.add(functionSegmentId, functionRootBuilder.getLastSegmentBytes());
                checkpointRootBuilder.addFunction(functionRootRef);
            }

            // The dictionary root has to exist and be durable before the checkpoint root that
            // names it - the same ordering every function root above already follows - so it is
            // written here, after every function, and before checkpointRootBuilder.build.
            if (partitionKeyRegistry != null) {
                final LiveViewCheckpointKeyDictionaryColumnSource columnSource = partitionKeyRegistry.newDictionaryColumnSource();
                if (columnSource.getColumnCount() > 0) {
                    nextSegmentId = skipPublishedSegmentIds(checkpointsDir, nextSegmentId);
                    final long dictionarySegmentId = nextSegmentId++;
                    dictionaryWriter.write(oldKeyDictionaryRef, columnSource, dictionarySegmentId, dictionaryRefOut);
                    metadataBytesAdded = checkedAdd(metadataBytesAdded, dictionaryWriter.getLastSegmentBytes());
                    writtenMetaSegments.add(dictionarySegmentId, dictionaryWriter.getLastSegmentBytes());
                    checkpointRootBuilder.setKeyDictionaryRef(dictionaryRefOut, dictionaryWriter.getReferencedSegmentIds());
                }
            }

            nextSegmentId = skipPublishedSegmentIds(checkpointsDir, nextSegmentId);
            final long rootSegmentId = nextSegmentId++;
            checkpointRootBuilder.build(rootSegmentId, rootRefOut);
            metadataBytesAdded = checkedAdd(metadataBytesAdded, checkpointRootBuilder.getLastSegmentBytes());
            writtenMetaSegments.add(rootSegmentId, checkpointRootBuilder.getLastSegmentBytes());
            checkpointRootBuilder.getReferencedSegmentIds(referencedSegmentIdsOut);
        }

        private void removeMissingPartitions(
                LiveViewCheckpointPageRef oldFunctionRootRef,
                FrozenFunction frozen,
                @Nullable LiveViewCheckpointOutputKeyDomain outputKeys
        ) {
            if (oldFunctionRootRef.isNull()) {
                return;
            }
            oldFunctionRoot.of(checkpointsDir, oldFunctionRootRef);
            final LiveViewCheckpointPageRef oldPartitionRoot = redirectOldPartitionRoot;
            oldFunctionRoot.getPartitionMapRootRef(oldPartitionRoot);
            if (outputKeys != null) {
                for (int i = 0, n = outputKeys.getSlotCount(); i < n; i++) {
                    final byte[] key = outputKeys.getKeyAt(i);
                    if (key != null && frozen.partitionIndexes.get(0, 0, key) < 0) {
                        functionRootBuilder.removePartition(key);
                    }
                }
                return;
            }
            missingPartitionVisitor.of(this, frozen);
            try {
                oldPartitionReader.iterateAll(oldPartitionRoot, missingPartitionVisitor);
            } finally {
                missingPartitionVisitor.clearBindings();
            }
        }

        private int redirectTimelineEntries(
                LiveViewCheckpointTimelineReader timelineReader,
                LiveViewCheckpointPageRef oldTimelineRoot,
                long definitionTxn,
                LiveViewCheckpointCompactionPlan plan,
                LiveViewCheckpointPageRef newRootRef,
                LongList removedSegmentIds,
                LongList addedSegmentIds,
                ObjList<LiveViewCheckpointTimelineEntry> changedEntries,
                LiveViewCheckpointSegmentDirectoryWriter directoryWriter,
                long generation,
                long targetSegmentId
        ) {
            redirectTimelineVisitor.of(
                    this,
                    definitionTxn,
                    plan,
                    newRootRef,
                    removedSegmentIds,
                    addedSegmentIds,
                    changedEntries,
                    directoryWriter,
                    generation,
                    targetSegmentId
            );
            try {
                timelineReader.iterateAll(oldTimelineRoot, redirectTimelineVisitor);
                lastChangedEntryCount = redirectTimelineVisitor.changedEntryCount;
                return redirectTimelineVisitor.targetSegmentRootRefs;
            } finally {
                redirectTimelineVisitor.clearBindings();
            }
        }

        @Override
        public void close() {
            Misc.free(anchorRootBuilder);
            Misc.free(aggregateRootWriter);
            Misc.free(aggregateStateWriter);
            Misc.free(batchCheckpointRoot);
            Misc.free(batchFunctionDirectory);
            Misc.free(checkpointRootBuilder);
            Misc.free(dictionaryWriter);
            Misc.free(redirectDictionaryReader);
            Misc.free(functionRootBuilder);
            Misc.free(oldFunctionRoot);
            Misc.free(oldPartitionReader);
            Misc.free(redirectCheckpointRoot);
            Misc.free(redirectFunctionDirectory);
            Misc.free(windowRootBuilder);
            Misc.free(checkpointsDir);
        }

        private void of(Path checkpointsDir, long nextSegmentId) {
            this.checkpointsDir.of(checkpointsDir);
            this.oldPartitionReader.of(checkpointsDir);
            this.dictionaryWriter.of(checkpointsDir);
            this.nextSegmentId = nextSegmentId;
            this.metadataBytesAdded = 0;
        }

    }

    private final class MissingPartitionVisitor implements LiveViewCheckpointPartitionMapReader.Visitor {
        private FrozenFunction frozen;
        private RootBuilders roots;

        private void clearBindings() {
            frozen = null;
            roots = null;
        }

        private void of(RootBuilders roots, FrozenFunction frozen) {
            this.roots = roots;
            this.frozen = frozen;
        }

        @Override
        public void onEntry(@NotNull LiveViewCheckpointPartitionMapEntry entry) {
            if (frozen.partitionIndexes.get(0, 0, entry.getKey()) < 0) {
                roots.functionRootBuilder.removePartition(entry.getKey());
            }
        }
    }

    private final class RedirectPartitionVisitor implements LiveViewCheckpointPartitionMapReader.Visitor {
        private LiveViewCheckpointCompactionPlan plan;
        private RootBuilders roots;

        private void clearBindings() {
            plan = null;
            roots = null;
        }

        private void of(RootBuilders roots, LiveViewCheckpointCompactionPlan plan) {
            this.roots = roots;
            this.plan = plan;
        }

        @Override
        public void onEntry(@NotNull LiveViewCheckpointPartitionMapEntry entry) {
            roots.redirectPartition(entry, plan);
        }
    }

    private final class RedirectTimelineVisitor implements LiveViewCheckpointTimelineReader.Visitor {
        private LongList addedSegmentIds;
        private ObjList<LiveViewCheckpointTimelineEntry> changedEntries;
        private long definitionTxn;
        private LiveViewCheckpointSegmentDirectoryWriter directoryWriter;
        private long generation;
        private LiveViewCheckpointPageRef newRootRef;
        private LiveViewCheckpointCompactionPlan plan;
        private LongList removedSegmentIds;
        private RootBuilders roots;
        private long targetSegmentId;
        private int targetSegmentRootRefs;
        private int changedEntryCount;

        private void clearBindings() {
            addedSegmentIds = null;
            changedEntries = null;
            definitionTxn = 0;
            directoryWriter = null;
            generation = 0;
            newRootRef = null;
            plan = null;
            removedSegmentIds = null;
            roots = null;
            targetSegmentId = 0;
            targetSegmentRootRefs = 0;
        }

        private void of(
                RootBuilders roots,
                long definitionTxn,
                LiveViewCheckpointCompactionPlan plan,
                LiveViewCheckpointPageRef newRootRef,
                LongList removedSegmentIds,
                LongList addedSegmentIds,
                ObjList<LiveViewCheckpointTimelineEntry> changedEntries,
                LiveViewCheckpointSegmentDirectoryWriter directoryWriter,
                long generation,
                long targetSegmentId
        ) {
            this.roots = roots;
            this.definitionTxn = definitionTxn;
            this.plan = plan;
            this.newRootRef = newRootRef;
            this.removedSegmentIds = removedSegmentIds;
            this.addedSegmentIds = addedSegmentIds;
            this.changedEntries = changedEntries;
            this.directoryWriter = directoryWriter;
            this.generation = generation;
            this.targetSegmentId = targetSegmentId;
            targetSegmentRootRefs = 0;
            changedEntryCount = 0;
        }

        @Override
        public void onEntry(@NotNull LiveViewCheckpointTimelineEntry entry) {
            if (!roots.buildRedirectedRoot(entry.rootRef, definitionTxn, plan, newRootRef, removedSegmentIds, addedSegmentIds)) {
                return;
            }
            // The changed entries are a high-water pool rather than a list built per
            // pass: a compaction rewrites the roots that name a drained segment, and
            // the next one over the same view rewrites about as many.
            if (changedEntryCount == changedEntries.size()) {
                changedEntries.add(new LiveViewCheckpointTimelineEntry());
            }
            final LiveViewCheckpointTimelineEntry newEntry =
                    changedEntries.getQuick(changedEntryCount++).copyFrom(entry);
            newEntry.rootRef.of(newRootRef.getSegmentId(), newRootRef.getOffset(), newRootRef.getLength());
            if (dropSegmentId(addedSegmentIds, targetSegmentId)) {
                targetSegmentRootRefs++;
            }
            registerBoundarySegments(directoryWriter, roots.writtenMetaSegments, addedSegmentIds);
            directoryWriter.applyRootReferenceChanges(removedSegmentIds, addedSegmentIds, generation);
        }
    }
}
