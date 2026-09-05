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
import io.questdb.std.MemoryTracker;
import io.questdb.std.Transient;
import io.questdb.std.str.Path;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

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
 * <p>A pass runs on the writer's {@link LiveViewCheckpointCompactionScratch},
 * which owns every store, reader, reference and result it needs: the driver
 * constructs none of them per call, and {@code end()} releases every mapping and
 * every tracker-bound allocation before the worker moves to another view. Every
 * step is best-effort, and what abandoning the candidate does depends on where the
 * failure landed: before the metadata commit point it unlinks the half-written
 * target and leaves the published generation byte-identical, while past that
 * point it keeps the target, because the committed generation already names
 * it.</p>
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
            long lifecycleIdentity,
            boolean primaryOwner,
            @Nullable MemoryTracker memoryTracker,
            int maxLiveFractionPercent,
            int minSourceSegments,
            int maxSourceSegments
    ) {
        if (!primaryOwner || maxSourceSegments <= 0 || minSourceSegments <= 0 || maxLiveFractionPercent < 0) {
            return Result.NOTHING;
        }
        final LiveViewCheckpointCompactionScratch scratch = writer.getCompactionScratch();
        scratch.begin(memoryTracker);
        try {
            final LiveViewCheckpointMetaStore metaStore = scratch.getMetaStore();
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
            final long targetSegmentId;
            final LiveViewCheckpointTimelineReader timelineReader = scratch.getTimelineReader();
            final LiveViewCheckpointPartitionMapReader partitionReader = scratch.getPartitionReader();
            final LiveViewCheckpointSegmentDirectoryReader segmentDirectory = scratch.getSegmentDirectory();
            try (LiveViewCheckpointGenerationPin pin = metaStore.pin()) {
                timelineReader.of(checkpointsDir);
                partitionReader.of(checkpointsDir);
                segmentDirectory.of(checkpointsDir, pin.getSegmentDirectoryRootRef());

                scratch.collectLivePages(
                        timelineReader,
                        pin.getTimelineRootRef(),
                        checkpointsDir,
                        scratch.getCheckpointRoot(),
                        scratch.getFunctionDirectory(),
                        scratch.getFunctionDirectoryRef(),
                        scratch.getFunctionRoot(),
                        scratch.getFunctionRootRef(),
                        scratch.getScalarRef(),
                        partitionReader,
                        scratch.getPartitionMapRoot()
                );

                // Select the sparsest referenced segments in catalogue order (oldest
                // first, most likely superseded), capped so one pass stays bounded.
                scratch.selectSegments(segmentDirectory, maxSourceSegments, maxLiveFractionPercent);

                if (scratch.getSelectedSegmentCount() < minSourceSegments) {
                    return Result.NOTHING;
                }
                targetSegmentId = nextFreeSegmentId(configuration, scratch.getPathScratch(), checkpointsDir, superblock.nextSegmentId);
            }

            final LiveViewCheckpointDataStore dataStore = scratch.getDataStore();
            dataStore.of(checkpointsDir);
            try (LiveViewCheckpointDataStore.Candidate candidate = dataStore.beginCandidate(scratch)) {
                final long targetBytes = candidate.repack(targetSegmentId, scratch);
                final LiveViewCheckpointCompactionPlan plan =
                        scratch.ofPlan(targetSegmentId, targetBytes, generation);
                final LiveViewCheckpointTimelineStoreWriter.CompactionResult result =
                        writer.publishCompaction(
                                checkpointsDir, definitionTxn, historyEpoch, lifecycleIdentity, primaryOwner, memoryTracker, plan
                        );
                candidate.markPublished();
                LOG.info().$("compacted live view checkpoint timeline [dir=").$(checkpointsDir)
                        .$(", sources=").$(scratch.getSelectedSegmentCount())
                        .$(", pages=").$(plan.size())
                        .$(", target=").$(targetSegmentId)
                        .$(", targetBytes=").$(targetBytes)
                        .$(", rootsRewritten=").$(result.getRootsRewritten())
                        .$(", generation=").$(result.getGeneration()).I$();
                return scratch.getResult().of(
                        true,
                        result.getRootsRewritten(),
                        targetSegmentId,
                        result.getGeneration()
                );
            }
        } finally {
            scratch.end();
        }
    }

    private static long nextFreeSegmentId(
            CairoConfiguration configuration,
            Path path,
            Path checkpointsDir,
            long candidate
    ) {
        while (candidate < Long.MAX_VALUE) {
            LiveViewCheckpointLayout.metaSegmentPath(path, checkpointsDir, candidate);
            final boolean metaExists = configuration.getFilesFacade().exists(path.$());
            LiveViewCheckpointLayout.dataSegmentPath(path, checkpointsDir, candidate);
            if (!metaExists && !configuration.getFilesFacade().exists(path.$())) {
                return candidate;
            }
            candidate++;
        }
        throw CairoException.critical(0).put("live view checkpoint segment id exhausted");
    }

    /**
     * Result of one compaction pass. When {@link #isPublished()} is false nothing
     * qualified and every other field is unset.
     */
    public static final class Result {
        static final Result NOTHING = new Result().of(false, 0, -1, -1);
        private long generation;
        private boolean published;
        private int rootsRewritten;
        private long targetSegmentId;

        Result of(
                boolean published,
                int rootsRewritten,
                long targetSegmentId,
                long generation
        ) {
            this.published = published;
            this.rootsRewritten = rootsRewritten;
            this.targetSegmentId = targetSegmentId;
            this.generation = generation;
            return this;
        }

        public long getGeneration() {
            return generation;
        }

        public int getRootsRewritten() {
            return rootsRewritten;
        }

        public long getTargetSegmentId() {
            return targetSegmentId;
        }

        public boolean isPublished() {
            return published;
        }
    }


}
