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
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.LongHashSet;
import io.questdb.std.LongList;
import io.questdb.std.LongObjHashMap;
import io.questdb.std.ObjList;
import io.questdb.std.Transient;
import io.questdb.std.str.Path;
import org.jetbrains.annotations.NotNull;

import java.util.HashMap;

/**
 * The production driver for physical checkpoint compaction: it decides which data
 * segments are worth reclaiming, repacks their still-live state pages into one
 * fresh segment, and publishes a generation that redirects every root onto the
 * relocated pages.
 *
 * <p>Repairs re-version roots, so a page a root once named is superseded when no
 * surviving root names it; the segment holding it keeps the file - the catalogue
 * cannot free a segment with even one live page - so its dead bytes accumulate.
 * Compaction walks the pinned generation's roots to measure how much of each
 * segment is still live, selects the sparsest referenced segments (dead bytes
 * dominate), copies their live pages forward into a new target and publishes the
 * redirect. The drained segments then hold no live page and retire for the purge
 * job to reclaim.</p>
 *
 * <p>Compaction is a maintenance operation, not a per-commit one: it runs
 * occasionally and touches metadata proportional to the roots that name a drained
 * segment, so it allocates per call rather than pooling state. Every step is
 * best-effort - a failure abandons the candidate, which unlinks the half-written
 * target and leaves the published generation byte-identical.</p>
 */
public final class LiveViewCheckpointCompaction {

    private static final Log LOG = LogFactory.getLog(LiveViewCheckpointCompaction.class);

    private LiveViewCheckpointCompaction() {
    }

    /**
     * Runs one compaction pass over the timeline in {@code checkpointsDir}.
     *
     * @param writer                 the caller's timeline store writer, reused to
     *                               publish the redirect
     * @param maxLiveFractionPercent a referenced segment qualifies when its live
     *                               bytes are at or below this percent of its file
     *                               length; higher lets denser segments in
     * @param minSourceSegments      the pass does nothing unless at least this many
     *                               segments qualify, so a lone sparse segment does
     *                               not trigger a whole-timeline rewrite
     * @param maxSourceSegments      the most segments one pass drains, capping the
     *                               copy and the metadata churn; also the disabled
     *                               sentinel at zero
     * @return the outcome; {@link Result#isPublished()} is false when nothing
     * qualified or the driver is disabled
     */
    public static Result compact(
            @NotNull CairoConfiguration configuration,
            @Transient @NotNull Path checkpointsDir,
            @NotNull LiveViewCheckpointTimelineStoreWriter writer,
            long definitionTxn,
            long historyEpoch,
            boolean primaryOwner,
            int maxLiveFractionPercent,
            int minSourceSegments,
            int maxSourceSegments
    ) {
        if (!primaryOwner || maxSourceSegments <= 0 || minSourceSegments <= 0 || maxLiveFractionPercent < 0) {
            return Result.NOTHING;
        }
        try (LiveViewCheckpointMetaStore metaStore = new LiveViewCheckpointMetaStore(configuration)) {
            metaStore.of(checkpointsDir);
            if (!metaStore.isValid()) {
                return Result.NOTHING;
            }
            final LiveViewCheckpointSuperblock superblock = metaStore.getSuperblock();
            if (superblock.definitionTxn != definitionTxn || superblock.historyEpoch != historyEpoch) {
                return Result.NOTHING;
            }
            final long generation = superblock.generation;

            // Distinct live pages across every root: a page shared by many roots is
            // counted once here and repacked once, so the redirect stays shared.
            final HashMap<PageKey, LiveViewCheckpointStatePageRef> livePages = new HashMap<>();
            final LongObjHashMap<long[]> liveBytesBySegment = new LongObjHashMap<>();
            final LongHashSet selectedSegments = new LongHashSet();
            final ObjList<LiveViewCheckpointStatePageRef> sourceRefs = new ObjList<>();
            final long targetSegmentId;
            try (
                    LiveViewCheckpointGenerationPin pin = metaStore.pin();
                    LiveViewCheckpointTimelineReader timelineReader = new LiveViewCheckpointTimelineReader(configuration);
                    LiveViewCheckpointRoot checkpointRoot = new LiveViewCheckpointRoot(configuration);
                    LiveViewCheckpointFunctionDirectory functionDirectory = new LiveViewCheckpointFunctionDirectory(configuration);
                    LiveViewCheckpointFunctionRoot functionRoot = new LiveViewCheckpointFunctionRoot(configuration);
                    LiveViewCheckpointPartitionMapReader partitionReader = new LiveViewCheckpointPartitionMapReader(configuration);
                    LiveViewCheckpointSegmentDirectoryReader segmentDirectory = new LiveViewCheckpointSegmentDirectoryReader(configuration)
            ) {
                timelineReader.of(checkpointsDir);
                partitionReader.of(checkpointsDir);
                segmentDirectory.of(checkpointsDir, pin.getSegmentDirectoryRootRef());

                final LiveViewCheckpointPageRef functionDirectoryRef = new LiveViewCheckpointPageRef();
                final LiveViewCheckpointPageRef functionRootRef = new LiveViewCheckpointPageRef();
                final LiveViewCheckpointStatePageRef scalarRef = new LiveViewCheckpointStatePageRef();
                final LiveViewCheckpointPageRef partitionMapRoot = new LiveViewCheckpointPageRef();
                timelineReader.iterateAll(pin.getTimelineRootRef(), entry -> {
                    checkpointRoot.of(checkpointsDir, entry.rootRef);
                    checkpointRoot.getFunctionDirectoryRef(functionDirectoryRef);
                    functionDirectory.of(checkpointsDir, functionDirectoryRef);
                    for (int i = 0, n = functionDirectory.size(); i < n; i++) {
                        functionDirectory.getRootRef(i, functionRootRef);
                        functionRoot.of(checkpointsDir, functionRootRef);
                        functionRoot.getScalarStateRef(scalarRef);
                        if (!scalarRef.isNull()) {
                            collectPage(livePages, liveBytesBySegment, scalarRef);
                        }
                        functionRoot.getPartitionMapRootRef(partitionMapRoot);
                        partitionReader.iterateAll(partitionMapRoot, pe -> {
                            for (int p = 0, m = pe.getStatePageCount(); p < m; p++) {
                                collectPage(livePages, liveBytesBySegment, pe.getStatePageRef(p));
                            }
                        });
                    }
                });

                // Select the sparsest referenced segments in catalogue order (oldest
                // first, most likely superseded), capped so one pass stays bounded.
                segmentDirectory.iterateAll(entry -> {
                    if (selectedSegments.size() >= maxSourceSegments || entry.referenceCount <= 0) {
                        return;
                    }
                    final long[] liveBytes = liveBytesBySegment.get(entry.segmentId);
                    if (liveBytes == null || liveBytes[0] <= 0 || liveBytes[0] >= entry.fileLength) {
                        return;
                    }
                    // liveBytes * 100 <= fileLength * maxLiveFractionPercent, in a
                    // form that cannot overflow for realistic segment sizes.
                    if (liveBytes[0] <= entry.fileLength / 100.0 * maxLiveFractionPercent) {
                        selectedSegments.add(entry.segmentId);
                    }
                });

                if (selectedSegments.size() < minSourceSegments) {
                    return Result.NOTHING;
                }
                for (LiveViewCheckpointStatePageRef ref : livePages.values()) {
                    if (selectedSegments.contains(ref.getSegmentId())) {
                        sourceRefs.add(ref);
                    }
                }
                if (sourceRefs.size() == 0) {
                    return Result.NOTHING;
                }
                targetSegmentId = nextFreeSegmentId(configuration, checkpointsDir, superblock.nextSegmentId);
            }

            try (LiveViewCheckpointDataStore dataStore = new LiveViewCheckpointDataStore(configuration, metaStore)) {
                dataStore.of(checkpointsDir);
                try (LiveViewCheckpointDataStore.Candidate candidate = dataStore.beginCandidate()) {
                    final ObjList<LiveViewCheckpointStatePageRef> targetRefs = new ObjList<>();
                    final long targetBytes = candidate.repack(targetSegmentId, sourceRefs, targetRefs);
                    final LiveViewCheckpointCompactionPlan plan =
                            new LiveViewCheckpointCompactionPlan(targetSegmentId, targetBytes, generation);
                    for (int i = 0, n = sourceRefs.size(); i < n; i++) {
                        plan.addRedirect(sourceRefs.getQuick(i), targetRefs.getQuick(i));
                    }
                    final LiveViewCheckpointTimelineStoreWriter.CompactionResult result =
                            writer.publishCompaction(checkpointsDir, definitionTxn, historyEpoch, primaryOwner, plan);
                    candidate.markPublished();
                    LOG.info().$("compacted live view checkpoint timeline [dir=").$(checkpointsDir)
                            .$(", sources=").$(selectedSegments.size())
                            .$(", pages=").$(sourceRefs.size())
                            .$(", target=").$(targetSegmentId)
                            .$(", targetBytes=").$(targetBytes)
                            .$(", rootsRewritten=").$(result.getRootsRewritten())
                            .$(", generation=").$(result.getGeneration()).I$();
                    return new Result(
                            true,
                            result.getRootsRewritten(),
                            toSortedList(selectedSegments),
                            sourceRefs.size(),
                            targetSegmentId,
                            targetBytes,
                            result.getGeneration()
                    );
                }
            }
        }
    }

    private static void collectPage(
            HashMap<PageKey, LiveViewCheckpointStatePageRef> livePages,
            LongObjHashMap<long[]> liveBytesBySegment,
            LiveViewCheckpointStatePageRef ref
    ) {
        final PageKey key = new PageKey(ref.getSegmentId(), ref.getOffset(), ref.getStoredLength());
        if (livePages.containsKey(key)) {
            return;
        }
        livePages.put(key, LiveViewCheckpointPartitionMapEntry.copyRef(ref));
        long[] acc = liveBytesBySegment.get(ref.getSegmentId());
        if (acc == null) {
            acc = new long[]{0};
            liveBytesBySegment.put(ref.getSegmentId(), acc);
        }
        acc[0] += ref.getStoredLength();
    }

    private static long nextFreeSegmentId(CairoConfiguration configuration, Path checkpointsDir, long candidate) {
        try (Path path = new Path()) {
            while (candidate < Long.MAX_VALUE) {
                LiveViewCheckpointLayout.metaSegmentPath(path, checkpointsDir, candidate);
                final boolean metaExists = configuration.getFilesFacade().exists(path.$());
                LiveViewCheckpointLayout.dataSegmentPath(path, checkpointsDir, candidate);
                if (!metaExists && !configuration.getFilesFacade().exists(path.$())) {
                    return candidate;
                }
                candidate++;
            }
        }
        throw CairoException.critical(0).put("live view checkpoint segment id exhausted");
    }

    private static LongList toSortedList(LongHashSet set) {
        final LongList list = new LongList(Math.max(1, set.size()));
        for (int i = 0, n = set.size(); i < n; i++) {
            list.add(set.get(i));
        }
        list.sort();
        return list;
    }

    /**
     * Result of one compaction pass. When {@link #isPublished()} is false nothing
     * qualified and every other field is unset.
     */
    public static final class Result {
        static final Result NOTHING = new Result(false, 0, new LongList(), 0, -1, 0, -1);
        private final long generation;
        private final long pagesRelocated;
        private final boolean published;
        private final int rootsRewritten;
        private final LongList sourceSegmentIds;
        private final long targetSegmentBytes;
        private final long targetSegmentId;

        Result(
                boolean published,
                int rootsRewritten,
                LongList sourceSegmentIds,
                long pagesRelocated,
                long targetSegmentId,
                long targetSegmentBytes,
                long generation
        ) {
            this.published = published;
            this.rootsRewritten = rootsRewritten;
            this.sourceSegmentIds = sourceSegmentIds;
            this.pagesRelocated = pagesRelocated;
            this.targetSegmentId = targetSegmentId;
            this.targetSegmentBytes = targetSegmentBytes;
            this.generation = generation;
        }

        public long getGeneration() {
            return generation;
        }

        public long getPagesRelocated() {
            return pagesRelocated;
        }

        public int getRootsRewritten() {
            return rootsRewritten;
        }

        /**
         * @return the data segments this pass drained, ascending. Every page they
         * held has been relocated into the target, so no surviving root names them
         * and the purge job unlinks them once the fallback slot and every reader
         * pin have moved past the generation this published
         */
        public LongList getSourceSegmentIds() {
            return sourceSegmentIds;
        }

        public int getSourceSegments() {
            return sourceSegmentIds.size();
        }

        public long getTargetSegmentBytes() {
            return targetSegmentBytes;
        }

        public long getTargetSegmentId() {
            return targetSegmentId;
        }

        public boolean isPublished() {
            return published;
        }
    }

    private static final class PageKey {
        private final long offset;
        private final long segmentId;
        private final int storedLength;

        private PageKey(long segmentId, long offset, int storedLength) {
            this.segmentId = segmentId;
            this.offset = offset;
            this.storedLength = storedLength;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (!(obj instanceof PageKey)) {
                return false;
            }
            final PageKey that = (PageKey) obj;
            return segmentId == that.segmentId && offset == that.offset && storedLength == that.storedLength;
        }

        @Override
        public int hashCode() {
            long hash = segmentId * 31 + offset;
            hash = hash * 31 + storedLength;
            return (int) (hash ^ (hash >>> 32));
        }
    }
}
