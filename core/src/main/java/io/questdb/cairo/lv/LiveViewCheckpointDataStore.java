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
import io.questdb.cairo.vm.api.MemoryA;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.FilesFacade;
import io.questdb.std.LongHashSet;
import io.questdb.std.LongIntHashMap;
import io.questdb.std.LongList;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.Transient;
import io.questdb.std.str.Path;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.TestOnly;

import java.io.Closeable;

/**
 * Owns the immutable data-segment lifecycle. Compaction candidates protect all
 * source and target segments until redirected metadata is published or the
 * candidate is abandoned. Purge combines candidate ownership with the A/B slot
 * floor and live generation pins before unlinking a zero-reference segment.
 *
 * <p>Repack validates metadata bounds and copies encoded bytes without decoding
 * or calculating a data checksum. Repeated references to one physical page are
 * written once and retain sharing in the returned references. The caller writes
 * those references into candidate roots and publishes them through the metadata
 * store; this component remains independent of the Phase-3 checkpoint API.</p>
 */
public class LiveViewCheckpointDataStore implements Closeable {

    private static final Log LOG = LogFactory.getLog(LiveViewCheckpointDataStore.class);
    private final LongIntHashMap candidateOwnershipCounts = new LongIntHashMap();
    private final CatalogueRecoveryVisitor catalogueRecoveryVisitor = new CatalogueRecoveryVisitor();
    private final Path checkpointsDir = new Path();
    private final CairoConfiguration configuration;
    private final FilesFacade ff;
    private final LiveViewCheckpointSegmentDirectoryEntry lookupEntry = new LiveViewCheckpointSegmentDirectoryEntry();
    private final LiveViewCheckpointMetaStore metaStore;
    private final LongList retirementEntries = new LongList();
    private final LongList retirementRemaining = new LongList();
    private final LiveViewCheckpointRetirementQueue.State retirementState =
            new LiveViewCheckpointRetirementQueue.State();
    private final LiveViewCheckpointSegmentDirectoryReader segmentDirectory;
    private final PurgeSweep sweep = new PurgeSweep();
    private boolean isOpen;

    public LiveViewCheckpointDataStore(
            @NotNull CairoConfiguration configuration,
            @NotNull LiveViewCheckpointMetaStore metaStore
    ) {
        this.configuration = configuration;
        this.ff = configuration.getFilesFacade();
        this.metaStore = metaStore;
        this.segmentDirectory = new LiveViewCheckpointSegmentDirectoryReader(configuration);
    }

    public Candidate beginCandidate() {
        ensureOpen();
        return new Candidate(configuration).of(this);
    }

    Candidate beginCandidate(@NotNull LiveViewCheckpointCompactionScratch scratch) {
        ensureOpen();
        return scratch.acquireCandidate(this);
    }

    @Override
    public synchronized void close() {
        assert candidateOwnershipCounts.size() == 0
                : "live view checkpoint compaction candidates leaked: " + candidateOwnershipCounts.size();
        candidateOwnershipCounts.clear();
        Misc.free(segmentDirectory);
        Misc.free(checkpointsDir);
        isOpen = false;
    }

    public void of(@Transient @NotNull Path checkpointsDir) {
        if (isOpen) {
            throw CairoException.critical(0).put("live view checkpoint data store already open");
        }
        this.checkpointsDir.of(checkpointsDir);
        isOpen = true;
    }

    /**
     * Processes the current checksummed zero-reference queue. Failed unlinks
     * remain queued and are retried on the next call.
     */
    public synchronized PurgeResult purge() {
        ensureOpen();
        try (LiveViewCheckpointGenerationPin pin = metaStore.pin()) {
            segmentDirectory.of(checkpointsDir, pin.getSegmentDirectoryRootRef());
            final boolean queueMatchesGeneration = LiveViewCheckpointRetirementQueue.read(
                    configuration,
                    checkpointsDir,
                    retirementEntries,
                    retirementState
            ) && retirementState.generation == pin.getGeneration();
            final boolean requiresPhysicalOrphanScan = !queueMatchesGeneration;
            int catalogueEntriesVisited = 0;
            if (!queueMatchesGeneration) {
                // Upgrade/corruption recovery is deliberately the one full scan:
                // rebuild the durable work set from the selected catalogue, then
                // every steady sweep point-looks only these zero-reference ids.
                retirementEntries.clear();
                retirementState.generation = pin.getGeneration();
                retirementState.liveDataSegmentCount = 0;
                catalogueRecoveryVisitor.reset();
                try {
                    segmentDirectory.iterateAll(catalogueRecoveryVisitor);
                    catalogueEntriesVisited = catalogueRecoveryVisitor.catalogueEntriesVisited;
                } finally {
                    catalogueRecoveryVisitor.clear();
                }
            }
            sweep.of(
                    metaStore.getOldestValidSuperblockGeneration(),
                    metaStore.getMinPinnedGeneration()
            );
            sweep.liveSegments = retirementState.liveDataSegmentCount > Integer.MAX_VALUE
                    ? Integer.MAX_VALUE
                    : (int) retirementState.liveDataSegmentCount;
            retirementRemaining.clear();
            for (int i = 0, n = retirementEntries.size(); i < n; i += LiveViewCheckpointRetirementQueue.ENTRY_STRIDE) {
                final long segmentId = retirementEntries.getQuick(i);
                if (!segmentDirectory.find(segmentId, lookupEntry)) {
                    // The normal publication after a successful unlink pruned
                    // the catalogue entry. Only then can the durable work item go.
                    continue;
                }
                if (lookupEntry.referenceCount != 0) {
                    // A failed publication can queue a false retirement, and a
                    // later generation can revive a zero-reference segment. The
                    // selected catalogue is authoritative in both cases.
                    continue;
                }
                sweep.onEntry(lookupEntry);
                retirementRemaining.add(
                        lookupEntry.segmentId,
                        lookupEntry.fileLength,
                        lookupEntry.retireGeneration,
                        lookupEntry.kind
                );
            }
            LiveViewCheckpointRetirementQueue.write(
                    configuration,
                    checkpointsDir,
                    retirementRemaining,
                    pin.getGeneration(),
                    retirementState.liveDataSegmentCount
            );
            return new PurgeResult(
                    sweep.purgedSegments,
                    sweep.failedSegments,
                    sweep.purgedBytes,
                    sweep.liveSegments,
                    sweep.obsoleteBytes,
                    sweep.retirableSegments,
                    requiresPhysicalOrphanScan,
                    retirementEntries.size() / LiveViewCheckpointRetirementQueue.ENTRY_STRIDE,
                    catalogueEntriesVisited
            );
        }
    }

    @TestOnly
    public int getPurgeVisitorShellIdentityForTest() {
        return System.identityHashCode(catalogueRecoveryVisitor);
    }

    @TestOnly
    public boolean isPurgeVisitorShellStateClearForTest() {
        return catalogueRecoveryVisitor.catalogueEntriesVisited == 0;
    }

    private final class CatalogueRecoveryVisitor implements LiveViewCheckpointSegmentDirectoryReader.Visitor {
        private int catalogueEntriesVisited;

        private void clear() {
            catalogueEntriesVisited = 0;
        }

        private void reset() {
            catalogueEntriesVisited = 0;
        }

        @Override
        public void onEntry(@NotNull LiveViewCheckpointSegmentDirectoryEntry entry) {
            catalogueEntriesVisited++;
            if (entry.referenceCount == 0) {
                retirementEntries.add(entry.segmentId, entry.fileLength, entry.retireGeneration, entry.kind);
            } else if (!entry.isMetadata()) {
                retirementState.liveDataSegmentCount++;
            }
        }
    }

    /**
     * Abandons a candidate, unlinking the targets it staged.
     * <p>
     * A publication commits when it writes the inactive superblock slot's CRC and
     * then keeps running. The {@code msync} that follows under a non-NOSYNC commit
     * mode throws on EIO, which unwinds out of {@code publish()} itself, and the
     * result tail past it can still exhaust the heap. The candidate is then
     * abandoned over a generation that already names its target, so the
     * final-name unlink is gated on the durable catalogue rather than on
     * {@link Candidate#published} alone - a flag no marking order can set once the
     * commit and the throw share a frame. The temporary name is never published,
     * so it always goes.
     */
    private synchronized void abortCandidate(@NotNull Candidate candidate) {
        // The gate below opens and maps catalogue files, so this loop can now throw
        // where it once could not. Ownership must be released either way, or
        // close() trips its leaked-candidate assert.
        try {
            for (int i = 0, n = candidate.targetSegmentIds.size(); i < n; i++) {
                final long segmentId = candidate.targetSegmentIds.get(i);
                try (Path path = new Path()) {
                    LiveViewCheckpointLayout.dataSegmentTmpPath(path, checkpointsDir, segmentId);
                    ff.removeQuiet(path.$());
                    LiveViewCheckpointLayout.dataSegmentPath(path, checkpointsDir, segmentId);
                    if (!ff.exists(path.$())) {
                        // The repack never reached its rename, which is the common
                        // abort. Nothing to weigh against the catalogue, so skip the
                        // open the gate would otherwise pay for.
                        continue;
                    }
                    if (isSegmentDurablyCatalogued(segmentId)) {
                        LOG.info()
                                .$("keeping an abandoned live view checkpoint compaction target the durable catalogue names [dir=")
                                .$(checkpointsDir)
                                .$(", segmentId=").$(segmentId).I$();
                        continue;
                    }
                    ff.removeQuiet(path.$());
                }
            }
        } finally {
            releaseCandidate(candidate);
        }
    }

    private static long checkedAdd(long a, long b) {
        if (b > Long.MAX_VALUE - a) {
            throw CairoException.critical(0).put("live view checkpoint purged byte count overflow");
        }
        return a + b;
    }

    private static void copyRef(
            @NotNull LiveViewCheckpointStatePageRef from,
            @NotNull LiveViewCheckpointStatePageRef to
    ) {
        to.of(
                from.getSegmentId(),
                from.getOffset(),
                from.getStoredLength(),
                from.getDecodedLength(),
                from.getPageKind(),
                from.getCodec(),
                from.getRowCount(),
                from.getFlags()
        );
    }

    private void ensureOpen() {
        if (!isOpen) {
            throw CairoException.critical(0).put("live view checkpoint data store is not open");
        }
    }

    private boolean isCandidateOwned(long segmentId) {
        return candidateOwnershipCounts.get(segmentId) > 0;
    }

    /**
     * Re-reads the catalogue off disk and reports whether the newest durable
     * generation catalogues {@code segmentId}.
     * <p>
     * The answer has to come off disk rather than off {@link #metaStore}, whose
     * snapshot predates the publication that may have just committed. It is
     * fail-closed: anything short of a clean read of the newest durable
     * catalogue answers true.
     * <p>
     * The asymmetry is what justifies that. Keeping a segment costs disk -
     * {@link LiveViewCheckpointLifecycle#purgeUncataloguedSegments} reclaims it at
     * the next cadence sweep or reconciliation, whatever the durable
     * {@code nextSegmentId} ceiling has done in the meantime, and holds it only
     * while this same fail-closed read keeps answering true. That is the same
     * best-effort reclaim the metadata
     * segments of a failed publication already rely on. Unlinking a live target
     * costs the whole timeline.
     */
    private boolean isSegmentDurablyCatalogued(long segmentId) {
        try (
                LiveViewCheckpointMetaStore durable = new LiveViewCheckpointMetaStore(configuration);
                LiveViewCheckpointSegmentDirectoryReader durableDirectory = new LiveViewCheckpointSegmentDirectoryReader(configuration)
        ) {
            durable.of(checkpointsDir);
            if (!durable.isValid()) {
                // No slot was selected, so no catalogue was read at all. Nothing here
                // is evidence the target is free.
                return true;
            }
            final LiveViewCheckpointSuperblock superblock = durable.getSuperblock();
            if (!superblock.isSelectedSlotNewest()) {
                // This open fell back over a root that failed bounded validation. That
                // failure is not durable - validation never opens a data segment, so a
                // later open can select the newer slot again and only fail deep, at page
                // read - so the catalogue in hand is not necessarily the one that names
                // the target.
                return true;
            }
            durableDirectory.of(checkpointsDir, superblock.segmentDirectoryRootRef);
            return durableDirectory.find(segmentId, lookupEntry);
        } catch (CairoException e) {
            LOG.error()
                    .$("could not read the live view checkpoint catalogue while abandoning a compaction target [dir=")
                    .$(checkpointsDir)
                    .$(", segmentId=").$(segmentId)
                    .$(", errno=").$(e.getErrno())
                    .$(", error=").$safe(e.getFlyweightMessage()).I$();
            return true;
        }
    }

    private void own(@NotNull Candidate candidate, long segmentId) {
        if (segmentId < 0) {
            throw CairoException.critical(0)
                    .put("live view checkpoint candidate segment id must be non-negative, was ")
                    .put(segmentId);
        }
        if (!candidate.ownedSegmentIds.add(segmentId)) {
            return;
        }
        final int count = candidateOwnershipCounts.get(segmentId);
        if (count == Integer.MAX_VALUE) {
            candidate.ownedSegmentIds.remove(segmentId);
            throw CairoException.critical(0)
                    .put("live view checkpoint candidate ownership count overflow, segmentId=")
                    .put(segmentId);
        }
        candidateOwnershipCounts.put(segmentId, count < 0 ? 1 : count + 1);
    }

    private synchronized long repack(
            @NotNull Candidate candidate,
            long targetSegmentId,
            @NotNull LiveViewCheckpointCompactionScratch scratch
    ) {
        ensureOpen();
        if (candidate.targetSegmentIds.contains(targetSegmentId)) {
            throw CairoException.critical(0)
                    .put("duplicate live view checkpoint compaction target, segmentId=")
                    .put(targetSegmentId);
        }

        try (LiveViewCheckpointGenerationPin pin = metaStore.pin()) {
            segmentDirectory.of(checkpointsDir, pin.getSegmentDirectoryRootRef());
            if (targetSegmentId <= segmentDirectory.lastSegmentId()) {
                throw CairoException.critical(0)
                        .put("live view checkpoint compaction target id must be monotonic, segmentId=")
                        .put(targetSegmentId);
            }
            try (LiveViewCheckpointDataSegmentWriter writer = new LiveViewCheckpointDataSegmentWriter(configuration)) {
                writer.of(checkpointsDir, targetSegmentId);
                candidate.targetSegmentIds.add(targetSegmentId);
                own(candidate, targetSegmentId);
                boolean hasSourcePage = false;
                scratch.startSelectedPageIteration();
                try {
                    while (scratch.nextSelectedPage(candidate.sourceRef)) {
                        hasSourcePage = true;
                        final long sourceSegmentId = candidate.sourceRef.getSegmentId();
                        if (sourceSegmentId == targetSegmentId) {
                            throw CairoException.critical(0)
                                    .put("live view checkpoint compaction target aliases a source segment, segmentId=")
                                    .put(targetSegmentId);
                        }
                        if (segmentDirectory.getReferenceCount(sourceSegmentId) <= 0) {
                            throw CairoException.critical(0)
                                    .put("cannot repack obsolete live view checkpoint segment, segmentId=")
                                    .put(sourceSegmentId);
                        }
                        own(candidate, sourceSegmentId);
                        final LiveViewCheckpointDataSegmentReader reader = scratch.openReader(
                                checkpointsDir, sourceSegmentId, segmentDirectory.getFileLength(sourceSegmentId)
                        );
                        reader.openPage(
                                candidate.sourceRef,
                                candidate.sourceRef.getPageKind(),
                                candidate.sourceRef.getCodec(),
                                candidate.sourceRef.getFlags(),
                                candidate.sourceRef.getRowCount(),
                                candidate.sourceRef.getDecodedLength()
                        );
                        final MemoryA mem = writer.beginPage();
                        mem.putBlockOfBytes(reader.getPageAddress(), reader.getPageStoredLength());
                        writer.endPage(
                                candidate.targetRef,
                                candidate.sourceRef.getDecodedLength(),
                                candidate.sourceRef.getPageKind(),
                                candidate.sourceRef.getCodec(),
                                candidate.sourceRef.getRowCount(),
                                candidate.sourceRef.getFlags()
                        );
                        scratch.recordTarget(candidate.targetRef);
                    }
                    if (!hasSourcePage) {
                        throw CairoException.critical(0).put("cannot repack an empty live view checkpoint page set");
                    }
                    return writer.commit();
                } finally {
                    scratch.finishRepack();
                }
            }
        }
    }

    private synchronized void releaseCandidate(@NotNull Candidate candidate) {
        for (int i = 0, n = candidate.ownedSegmentIds.size(); i < n; i++) {
            final long segmentId = candidate.ownedSegmentIds.get(i);
            final int count = candidateOwnershipCounts.get(segmentId);
            assert count > 0;
            if (count == 1) {
                candidateOwnershipCounts.remove(segmentId);
            } else {
                candidateOwnershipCounts.put(segmentId, count - 1);
            }
        }
        candidate.ownedSegmentIds.clear();
        candidate.targetSegmentIds.clear();
    }

    public static final class Candidate implements Closeable {

        private final LiveViewCheckpointCompactionScratch compatibilityScratch;
        private final LongHashSet ownedSegmentIds = new LongHashSet();
        private final ObjList<LiveViewCheckpointStatePageRef> outputRefPool = new ObjList<>();
        private final LiveViewCheckpointStatePageRef sourceRef = new LiveViewCheckpointStatePageRef();
        private final LiveViewCheckpointStatePageRef targetFlyweight = new LiveViewCheckpointStatePageRef();
        private final LiveViewCheckpointStatePageRef targetRef = new LiveViewCheckpointStatePageRef();
        private final LongHashSet targetSegmentIds = new LongHashSet();
        private boolean failed;
        private LiveViewCheckpointDataStore owner;
        private boolean published;

        Candidate(@NotNull CairoConfiguration configuration) {
            compatibilityScratch = new LiveViewCheckpointCompactionScratch(configuration, false);
        }

        @Override
        public void close() {
            final LiveViewCheckpointDataStore o = owner;
            if (o == null) {
                return;
            }
            owner = null;
            if (published) {
                o.releaseCandidate(this);
            } else {
                o.abortCandidate(this);
            }
        }

        /**
         * Called only after the metadata generation commit point.
         */
        public void markPublished() {
            ensureOpen();
            published = true;
        }

        public long repack(
                long targetSegmentId,
                @NotNull ObjList<LiveViewCheckpointStatePageRef> sourceRefs,
                @NotNull ObjList<LiveViewCheckpointStatePageRef> targetRefs
        ) {
            ensureOpen();
            compatibilityScratch.begin(null);
            try {
                for (int i = 0, n = sourceRefs.size(); i < n; i++) {
                    final LiveViewCheckpointStatePageRef source = sourceRefs.getQuick(i);
                    if (source == null || source.isNull()) {
                        throw CairoException.critical(CairoException.LV_CHECKPOINT_TIMELINE_INVALID)
                                .put("live view checkpoint compaction source reference is null");
                    }
                    compatibilityScratch.addLivePage(source);
                    compatibilityScratch.addSelectedSegment(source.getSegmentId());
                }
                final long bytes = repack(targetSegmentId, compatibilityScratch);
                targetRefs.clear();
                for (int i = 0, n = sourceRefs.size(); i < n; i++) {
                    final LiveViewCheckpointStatePageRef target = compatibilityScratch.redirect(
                            sourceRefs.getQuick(i), targetFlyweight
                    );
                    if (target == null) {
                        throw CairoException.critical(CairoException.LV_CHECKPOINT_TIMELINE_INVALID)
                                .put("live view checkpoint compaction compatibility redirect missing");
                    }
                    final LiveViewCheckpointStatePageRef copy;
                    if (i < outputRefPool.size()) {
                        copy = outputRefPool.getQuick(i);
                    } else {
                        copy = new LiveViewCheckpointStatePageRef();
                        outputRefPool.add(copy);
                    }
                    copyRef(target, copy);
                    targetRefs.add(copy);
                }
                return bytes;
            } finally {
                compatibilityScratch.end();
            }
        }

        long repack(long targetSegmentId, @NotNull LiveViewCheckpointCompactionScratch scratch) {
            ensureOpen();
            try {
                return owner.repack(this, targetSegmentId, scratch);
            } catch (RuntimeException | Error th) {
                failed = true;
                throw th;
            }
        }

        Candidate of(@NotNull LiveViewCheckpointDataStore owner) {
            if (this.owner != null) {
                throw CairoException.critical(0).put("live view checkpoint compaction candidate already active");
            }
            this.owner = owner;
            failed = false;
            published = false;
            return this;
        }

        void closeReusableResources() {
            if (owner != null) {
                throw CairoException.critical(0).put("cannot close an active live view checkpoint compaction candidate");
            }
            compatibilityScratch.close();
            outputRefPool.clear();
        }

        private void ensureOpen() {
            if (owner == null) {
                throw CairoException.critical(0).put("live view checkpoint compaction candidate is closed");
            }
            if (published) {
                throw CairoException.critical(0).put("live view checkpoint compaction candidate is already published");
            }
            if (failed) {
                throw CairoException.critical(0).put("live view checkpoint compaction candidate has failed");
            }
        }
    }

    public static final class PurgeResult {
        private final int failedSegmentCount;
        private final int catalogueEntriesVisited;
        private final int liveSegmentCount;
        private final long obsoleteBytes;
        private final long purgedBytes;
        private final int purgedSegmentCount;
        private final int queueEntriesVisited;
        private final boolean requiresPhysicalOrphanScan;
        private final LongList retirableSegmentIds;

        private PurgeResult(
                int purgedSegmentCount,
                int failedSegmentCount,
                long purgedBytes,
                int liveSegmentCount,
                long obsoleteBytes,
                @NotNull LongList retirableSegmentIds,
                boolean requiresPhysicalOrphanScan,
                int queueEntriesVisited,
                int catalogueEntriesVisited
        ) {
            this.purgedSegmentCount = purgedSegmentCount;
            this.failedSegmentCount = failedSegmentCount;
            this.purgedBytes = purgedBytes;
            this.liveSegmentCount = liveSegmentCount;
            this.obsoleteBytes = obsoleteBytes;
            this.retirableSegmentIds = new LongList(retirableSegmentIds);
            this.requiresPhysicalOrphanScan = requiresPhysicalOrphanScan;
            this.queueEntriesVisited = queueEntriesVisited;
            this.catalogueEntriesVisited = catalogueEntriesVisited;
        }

        public int getCatalogueEntriesVisited() {
            return catalogueEntriesVisited;
        }

        public int getFailedSegmentCount() {
            return failedSegmentCount;
        }

        /**
         * @return data segments a current logical root still names, counted over
         * the same ordered walk the sweep makes
         */
        public int getLiveSegmentCount() {
            return liveSegmentCount;
        }

        /**
         * @return bytes held by retired segments this sweep could not unlink -
         * still protected by the fallback slot or a reader pin, or failed to
         * remove. The collection lag in bytes, over data and metadata segments
         * alike, since both retire under the same rule
         */
        public long getObsoleteBytes() {
            return obsoleteBytes;
        }

        public long getPurgedBytes() {
            return purgedBytes;
        }

        public int getPurgedSegmentCount() {
            return purgedSegmentCount;
        }

        public int getQueueEntriesVisited() {
            return queueEntriesVisited;
        }

        /**
         * @return ascending ids of the catalogued segments this sweep left with no
         * file - the ones it unlinked, plus the ones an earlier sweep unlinked and
         * no publication has carried away yet. Their entries are dead weight the
         * next publication removes; a sweep that runs first re-proposes whatever a
         * skipped or crashed publication did not take
         */
        public LongList getRetirableSegmentIds() {
            return retirableSegmentIds;
        }

        /**
         * Returns whether purge had to rebuild its work set because the durable
         * queue was absent, invalid, or named another generation. A queue from a
         * future generation is the durable trace of a pre-superblock publication
         * failure, so the caller must run one physical orphan scan as well.
         */
        public boolean requiresPhysicalOrphanScan() {
            return requiresPhysicalOrphanScan;
        }
    }

    /**
     * Reusable ordered sweep over the pinned generation's catalogue. A segment -
     * data or metadata - is unlinked only when it is unreferenced, unowned by a
     * compaction candidate, unreachable from both valid superblock slots, and
     * older than every live reader pin. A failed unlink stays catalogued and is
     * retried on the next call.
     * <p>
     * The two kinds reach zero for different reasons - a data segment when the
     * last root naming it goes, a metadata segment when the last of its pages is
     * path-copied away - but the rule that decides when zero is safe to act on is
     * the same, so the sweep needs only the path to differ.
     * <p>
     * An unlinked segment leaves its catalogue entry behind, because the sweep
     * publishes no generation and only a publication may rewrite the catalogue.
     * It collects those ids instead, and the next publication retires the
     * entries. The collection is a proposal rather than a hand-off: a sweep
     * re-proposes every entry whose file is already gone, so nothing is lost when
     * the publication that would have taken them is skipped or crashes.
     */
    private final class PurgeSweep implements LiveViewCheckpointSegmentDirectoryReader.Visitor {

        private final LongList retirableSegments = new LongList();
        private int failedSegments;
        private int liveSegments;
        private long minPinnedGeneration;
        private long obsoleteBytes;
        private long oldestValidSlotGeneration;
        private long purgedBytes;
        private int purgedSegments;

        @Override
        public void onEntry(LiveViewCheckpointSegmentDirectoryEntry entry) {
            final long segmentId = entry.segmentId;
            // Data segments only: the count is what live_views() publishes as
            // checkpoint_data_segment_count, and cataloguing metadata segments
            // beside them must not silently redefine it.
            if (entry.referenceCount != 0 && !entry.isMetadata()) {
                liveSegments++;
            }
            if (entry.referenceCount != 0 || isCandidateOwned(segmentId)) {
                return;
            }
            if (oldestValidSlotGeneration < entry.retireGeneration || minPinnedGeneration <= entry.retireGeneration) {
                // Retired but still protected by a slot or a reader, so its bytes
                // are garbage this sweep may not collect yet.
                obsoleteBytes = checkedAdd(obsoleteBytes, entry.fileLength);
                return;
            }
            try (Path path = new Path()) {
                // The catalogue's kind decides the directory: the id namespace is
                // shared, so one id names a file in exactly one of the two.
                if (entry.isMetadata()) {
                    LiveViewCheckpointLayout.metaSegmentPath(path, checkpointsDir, segmentId);
                } else {
                    LiveViewCheckpointLayout.dataSegmentPath(path, checkpointsDir, segmentId);
                }
                if (!ff.exists(path.$())) {
                    // An earlier sweep unlinked it and no publication has carried
                    // the entry away yet. Propose it again: the entry now names
                    // nothing, and re-proposing is what makes the hand-off survive
                    // a crash or a skipped seal.
                    retirableSegments.add(segmentId);
                    return;
                }
                if (ff.removeQuiet(path.$())) {
                    purgedSegments++;
                    purgedBytes = checkedAdd(purgedBytes, entry.fileLength);
                    retirableSegments.add(segmentId);
                } else {
                    failedSegments++;
                    obsoleteBytes = checkedAdd(obsoleteBytes, entry.fileLength);
                    LOG.error()
                            .$("could not purge live view checkpoint segment [path=")
                            .$(path)
                            .$(',').$(" errno=").$(ff.errno()).I$();
                }
            }
        }

        private void of(long oldestValidSlotGeneration, long minPinnedGeneration) {
            this.oldestValidSlotGeneration = oldestValidSlotGeneration;
            this.minPinnedGeneration = minPinnedGeneration;
            purgedBytes = 0;
            purgedSegments = 0;
            failedSegments = 0;
            liveSegments = 0;
            obsoleteBytes = 0;
            retirableSegments.clear();
        }
    }
}
