/*******************************************************************************
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
import io.questdb.std.Chars;
import io.questdb.std.FilesFacade;
import io.questdb.std.NumericException;
import io.questdb.std.Numbers;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf8s;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Primary-owned lifecycle operations for the versioned checkpoint timeline.
 *
 * <p>Startup reconciliation is bounded by physical segment count. It discards
 * the candidate of every crashed repair through
 * {@link LiveViewCheckpointRepairState#sweep}, removes recognized temporary
 * files, and records final-name files above both valid A/B slot id ceilings.
 * Final names are removed only after a new slot advances past them, preserving
 * monotonic allocation. Reconciliation never walks logical checkpoint leaves.
 * Zero-reference deletion delegates to {@link LiveViewCheckpointDataStore},
 * retaining its old-slot, reader-pin, candidate-ownership, and retry guards.</p>
 *
 * <p>Ahead of all of that, reconciliation classifies the directory as a whole.
 * A {@code _timeline} carrying a foreign layout version, or a top-level entry
 * outside the current layout, means a build with a different on-disk format
 * owned this directory. Since live views are unreleased, such a directory is
 * removed rather than migrated or partially recovered: the primary rebuilds the
 * timeline from the base table on its next refresh, and no reconciliation rule
 * ever meets a mix of two formats.</p>
 *
 * <p>Callers serialize reconciliation, epoch replacement, and retirement with
 * timeline publication, repair descriptor writes, and pin acquisition. The
 * live-view integration does so with the refresh latch (and fences DROP before
 * retirement), so the repair sweep only ever meets descriptors of repairs that
 * are not running. Tests may pass an open metadata store to
 * {@link #retireTimeline} to assert that a live pin defers deletion.</p>
 */
public final class LiveViewCheckpointLifecycle {

    private static final Log LOG = LogFactory.getLog(LiveViewCheckpointLifecycle.class);

    private LiveViewCheckpointLifecycle() {
    }

    /**
     * Reconciles one primary-owned timeline before recovery or a retrying
     * publication. A replica must pass {@code false}; that path is a strict
     * no-op and does not even create/open {@code _timeline}.
     * <p>
     * A directory written under a foreign layout short-circuits every other
     * rule: it is removed whole and the result reports
     * {@link ReconcileResult#isFormatReset()}, leaving the caller with the same
     * disposition a live view that never checkpointed has.
     */
    public static ReconcileResult reconcile(
            @NotNull CairoConfiguration configuration,
            @NotNull Path checkpointsDir,
            long expectedDefinitionTxn,
            long expectedHistoryEpoch,
            boolean primaryOwner
    ) {
        if (!primaryOwner) {
            return ReconcileResult.NOT_OWNER;
        }
        if (expectedDefinitionTxn < 0 || expectedHistoryEpoch < 0) {
            throw CairoException.critical(0)
                    .put("invalid live view checkpoint history identity")
                    .put(" [definitionTxn=").put(expectedDefinitionTxn)
                    .put(", historyEpoch=").put(expectedHistoryEpoch).put(']');
        }

        // A directory this build cannot read as a whole goes before anything
        // reads part of it, including the repair sweep below.
        if (isForeignFormat(configuration, checkpointsDir)) {
            resetForeignFormat(configuration, checkpointsDir);
            return ReconcileResult.FORMAT_RESET;
        }

        // A descriptor left behind is a repair that crashed mid-candidate. Its
        // pinned snapshot cannot be reopened, so the candidate is discarded and
        // replanned rather than resumed; sweeping first also releases the
        // temporary segments it owned before the orphan pass counts them.
        final LiveViewCheckpointRepairState.SweepResult repairSweep =
                LiveViewCheckpointRepairState.sweep(configuration, checkpointsDir, primaryOwner);

        final FilesFacade ff = configuration.getFilesFacade();
        try (Path timelinePath = new Path()) {
            LiveViewCheckpointLayout.timelinePath(timelinePath, checkpointsDir);
            if (!ff.exists(timelinePath.$())) {
                final CleanupResult cleanup = cleanupOrphans(configuration, checkpointsDir, 0);
                return result(false, -1, Numbers.LONG_NULL, cleanup, 0, 0, repairSweep);
            }
        }

        boolean replaceEpoch = false;
        long nextSegmentIdCeiling = 0;
        long walPurgeFloor = -1;
        long normalizedBaseSeqTxn = Numbers.LONG_NULL;
        int purgedSegments = 0;
        int failedPurges = 0;
        try (LiveViewCheckpointMetaStore metaStore = new LiveViewCheckpointMetaStore(configuration)) {
            metaStore.of(checkpointsDir);
            if (metaStore.isValid()) {
                final LiveViewCheckpointSuperblock superblock = metaStore.getSuperblock();
                replaceEpoch = superblock.definitionTxn != expectedDefinitionTxn
                        || superblock.historyEpoch != expectedHistoryEpoch;
                if (!replaceEpoch) {
                    nextSegmentIdCeiling = superblock.getNextSegmentIdCeiling();
                    walPurgeFloor = metaStore.getWalPurgeFloor();
                    normalizedBaseSeqTxn = superblock.normalizedBaseSeqTxn;
                    try (LiveViewCheckpointDataStore dataStore = new LiveViewCheckpointDataStore(
                            configuration,
                            metaStore
                    )) {
                        dataStore.of(checkpointsDir);
                        final LiveViewCheckpointDataStore.PurgeResult purgeResult = dataStore.purge();
                        purgedSegments = purgeResult.getPurgedSegmentCount();
                        failedPurges = purgeResult.getFailedSegmentCount();
                    }
                }
            }
        }

        if (replaceEpoch) {
            // No generation has been exposed by this reconciler. Its caller owns
            // the refresh/drop exclusion, so retirement cannot race a new pin.
            if (!retireTimeline(configuration, checkpointsDir, null, true)) {
                throw CairoException.critical(configuration.getFilesFacade().errno())
                        .put("could not retire live view checkpoint history epoch [path=")
                        .put(checkpointsDir).put(']');
            }
            return new ReconcileResult(
                    true,
                    false,
                    -1,
                    Numbers.LONG_NULL,
                    0,
                    0,
                    0,
                    0,
                    0,
                    repairSweep.getDiscardedRepairCount(),
                    repairSweep.getFailedCount()
            );
        }

        final CleanupResult cleanup = cleanupOrphans(configuration, checkpointsDir, nextSegmentIdCeiling);
        return result(false, walPurgeFloor, normalizedBaseSeqTxn, cleanup, purgedSegments, failedPurges, repairSweep);
    }

    /** Removes final-name orphans after a new slot durably advances past them. */
    public static CleanupStats purgeFinalOrphans(
            @NotNull CairoConfiguration configuration,
            @NotNull Path checkpointsDir,
            long protectedCeiling,
            long orphanUpperBound,
            boolean primaryOwner
    ) {
        final CleanupResult result = new CleanupResult(protectedCeiling);
        if (!primaryOwner || orphanUpperBound <= protectedCeiling) {
            return new CleanupStats(0, 0);
        }
        try (Path path = new Path()) {
            purgeFinalOrphansInDir(
                    configuration.getFilesFacade(),
                    LiveViewCheckpointLayout.metaDirPath(path, checkpointsDir),
                    LiveViewCheckpointLayout.META_SEGMENT_PREFIX,
                    protectedCeiling,
                    orphanUpperBound,
                    result
            );
            purgeFinalOrphansInDir(
                    configuration.getFilesFacade(),
                    LiveViewCheckpointLayout.dataDirPath(path, checkpointsDir),
                    LiveViewCheckpointLayout.DATA_SEGMENT_PREFIX,
                    protectedCeiling,
                    orphanUpperBound,
                    result
            );
        }
        return new CleanupStats(result.removed, result.failed);
    }


    /**
     * Retires the timeline-owned files. When {@code pinOwner} is supplied, a
     * live generation pin defers retirement and the caller retries after release.
     * The caller must serialize the zero-pin check with new pin acquisition.
     */
    public static boolean retireTimeline(
            @NotNull CairoConfiguration configuration,
            @NotNull Path checkpointsDir,
            @Nullable LiveViewCheckpointMetaStore pinOwner,
            boolean primaryOwner
    ) {
        if (!primaryOwner) {
            return false;
        }
        if (pinOwner != null && pinOwner.getActivePinCount() > 0) {
            return false;
        }

        final FilesFacade ff = configuration.getFilesFacade();
        boolean success = true;
        try (Path path = new Path()) {
            // Remove the publication point first. With pin acquisition excluded,
            // no new reader can discover the roots whose directories follow.
            LiveViewCheckpointLayout.timelinePath(path, checkpointsDir);
            if (ff.exists(path.$()) && !ff.removeQuiet(path.$())) {
                success = false;
                logRemoveFailure(ff, path);
            }
            success &= removeTree(ff, LiveViewCheckpointLayout.metaDirPath(path, checkpointsDir));
            success &= removeTree(ff, LiveViewCheckpointLayout.dataDirPath(path, checkpointsDir));
            success &= removeTree(ff, path.of(checkpointsDir).concat(LiveViewCheckpointLayout.REPAIR_DIR_NAME));
        }
        return success;
    }

    private static CleanupResult cleanupOrphans(
            @NotNull CairoConfiguration configuration,
            @NotNull Path checkpointsDir,
            long nextSegmentIdCeiling
    ) {
        final CleanupResult result = new CleanupResult(nextSegmentIdCeiling);
        try (Path path = new Path()) {
            cleanupSegmentDir(
                    configuration.getFilesFacade(),
                    LiveViewCheckpointLayout.metaDirPath(path, checkpointsDir),
                    LiveViewCheckpointLayout.META_SEGMENT_PREFIX,
                    nextSegmentIdCeiling,
                    result
            );
            cleanupSegmentDir(
                    configuration.getFilesFacade(),
                    LiveViewCheckpointLayout.dataDirPath(path, checkpointsDir),
                    LiveViewCheckpointLayout.DATA_SEGMENT_PREFIX,
                    nextSegmentIdCeiling,
                    result
            );
        }
        return result;
    }

    private static void cleanupSegmentDir(
            @NotNull FilesFacade ff,
            @NotNull Path dir,
            @NotNull CharSequence prefix,
            long nextSegmentIdCeiling,
            @NotNull CleanupResult result
    ) {
        if (!ff.exists(dir.$())) {
            return;
        }
        final int dirLen = dir.size();
        final StringSink name = new StringSink();
        final long findPtr = ff.findFirst(dir.$());
        if (findPtr == 0) {
            return;
        }
        try {
            do {
                final long namePtr = ff.findName(findPtr);
                if (namePtr == 0) {
                    continue;
                }
                name.clear();
                if (!Utf8s.utf8ToUtf16Z(namePtr, name)
                        || Chars.equals(name, ".")
                        || Chars.equals(name, "..")
                        || !Chars.startsWith(name, prefix)) {
                    continue;
                }
                final boolean temporary = Chars.endsWith(name, LiveViewCheckpointLayout.TMP_SUFFIX);
                final int hi = name.length() - (temporary ? LiveViewCheckpointLayout.TMP_SUFFIX.length() : 0);
                final long segmentId = parseSegmentId(name, prefix.length(), hi);
                if (segmentId < 0) {
                    continue;
                }
                if (!temporary) {
                    if (segmentId >= nextSegmentIdCeiling && segmentId < Long.MAX_VALUE) {
                        result.finalOrphanUpperBound = Math.max(result.finalOrphanUpperBound, segmentId + 1);
                    }
                } else {
                    removeFile(ff, dir, dirLen, name, result);
                }
            } while (ff.findNext(findPtr) > 0);
        } finally {
            ff.findClose(findPtr);
            dir.trimTo(dirLen);
        }
    }

    /**
     * Reports whether {@code checkpointsDir} holds a top-level entry outside the
     * current layout. Everything this build writes there is one of four names -
     * the {@code _timeline} superblock and the {@code meta}, {@code data} and
     * {@code repair} directories - so anything else came from a build that
     * arranged checkpoint state differently. Earlier development builds left the
     * {@code _ring} manifest and per-checkpoint {@code .cp} / {@code .scp} files
     * at this level, which is what the check most often finds.
     */
    private static boolean hasUnknownEntry(@NotNull FilesFacade ff, @NotNull Path checkpointsDir) {
        if (!ff.exists(checkpointsDir.$())) {
            return false;
        }
        final long findPtr = ff.findFirst(checkpointsDir.$());
        if (findPtr == 0) {
            return false;
        }
        final StringSink name = new StringSink();
        try {
            do {
                final long namePtr = ff.findName(findPtr);
                if (namePtr == 0) {
                    continue;
                }
                name.clear();
                if (!Utf8s.utf8ToUtf16Z(namePtr, name)
                        || Chars.equals(name, ".")
                        || Chars.equals(name, "..")
                        || Chars.equals(name, LiveViewCheckpointLayout.TIMELINE_FILE_NAME)
                        || Chars.equals(name, LiveViewCheckpointLayout.META_DIR_NAME)
                        || Chars.equals(name, LiveViewCheckpointLayout.DATA_DIR_NAME)
                        || Chars.equals(name, LiveViewCheckpointLayout.REPAIR_DIR_NAME)) {
                    continue;
                }
                LOG.info().$("live view checkpoint directory holds an entry outside the current layout [path=")
                        .$(checkpointsDir).$(", name=").$safe(name).I$();
                return true;
            } while (ff.findNext(findPtr) > 0);
        } finally {
            ff.findClose(findPtr);
        }
        return false;
    }

    private static boolean isForeignFormat(
            @NotNull CairoConfiguration configuration,
            @NotNull Path checkpointsDir
    ) {
        final FilesFacade ff = configuration.getFilesFacade();
        try (Path path = new Path()) {
            LiveViewCheckpointLayout.timelinePath(path, checkpointsDir);
            if (ff.exists(path.$()) && LiveViewCheckpointSuperblock.isForeignFormat(ff, path.$())) {
                LOG.info().$("live view checkpoint timeline carries a foreign layout version [path=")
                        .$(checkpointsDir).I$();
                return true;
            }
        }
        return hasUnknownEntry(ff, checkpointsDir);
    }

    private static void purgeFinalOrphansInDir(
            @NotNull FilesFacade ff,
            @NotNull Path dir,
            @NotNull CharSequence prefix,
            long lo,
            long hi,
            @NotNull CleanupResult result
    ) {
        if (!ff.exists(dir.$())) {
            return;
        }
        final int dirLen = dir.size();
        final StringSink name = new StringSink();
        final long findPtr = ff.findFirst(dir.$());
        if (findPtr == 0) {
            return;
        }
        try {
            do {
                final long namePtr = ff.findName(findPtr);
                if (namePtr == 0) {
                    continue;
                }
                name.clear();
                if (!Utf8s.utf8ToUtf16Z(namePtr, name)
                        || !Chars.startsWith(name, prefix)
                        || Chars.endsWith(name, LiveViewCheckpointLayout.TMP_SUFFIX)) {
                    continue;
                }
                final long segmentId = parseSegmentId(name, prefix.length(), name.length());
                if (segmentId >= lo && segmentId < hi) {
                    removeFile(ff, dir, dirLen, name, result);
                }
            } while (ff.findNext(findPtr) > 0);
        } finally {
            ff.findClose(findPtr);
            dir.trimTo(dirLen);
        }
    }

    private static void removeFile(
            FilesFacade ff,
            Path dir,
            int dirLen,
            CharSequence name,
            CleanupResult result
    ) {
        dir.trimTo(dirLen).slash().put(name);
        if (ff.removeQuiet(dir.$())) {
            result.removed++;
        } else {
            result.failed++;
            logRemoveFailure(ff, dir);
        }
    }

    private static void logRemoveFailure(FilesFacade ff, Path path) {
        LOG.error().$("could not remove live view checkpoint lifecycle file [path=")
                .$(path).$(',').$(" errno=").$(ff.errno()).I$();
    }

    private static long parseSegmentId(CharSequence name, int lo, int hi) {
        if (lo >= hi) {
            return -1;
        }
        try {
            return Numbers.parseLong(name, lo, hi);
        } catch (NumericException e) {
            return -1;
        }
    }

    private static boolean removeTree(FilesFacade ff, Path path) {
        if (!ff.exists(path.$())) {
            return true;
        }
        if (ff.rmdir(path, false)) {
            return true;
        }
        logRemoveFailure(ff, path);
        return false;
    }

    /**
     * Removes the whole checkpoint directory so the primary rebuilds it from the
     * base table. Checkpoint state is derived, so discarding it costs one
     * rebuild; reading half of it under one layout and half under another is
     * what the reset exists to prevent, which is why a partial removal fails the
     * reconciliation instead of proceeding. The next reconciliation sees the
     * survivors, classifies the directory as foreign again, and retries.
     */
    private static void resetForeignFormat(
            @NotNull CairoConfiguration configuration,
            @NotNull Path checkpointsDir
    ) {
        final FilesFacade ff = configuration.getFilesFacade();
        try (Path path = new Path()) {
            if (!removeTree(ff, path.of(checkpointsDir))) {
                throw CairoException.critical(ff.errno())
                        .put("could not reset live view checkpoint directory [path=")
                        .put(checkpointsDir).put(']');
            }
        }
        LOG.info().$("reset live view checkpoint directory, rebuilding from the base table [path=")
                .$(checkpointsDir).I$();
    }

    private static ReconcileResult result(
            boolean epochReplaced,
            long walPurgeFloor,
            long normalizedBaseSeqTxn,
            CleanupResult cleanup,
            int purgedSegments,
            int failedPurges,
            LiveViewCheckpointRepairState.SweepResult repairSweep
    ) {
        return new ReconcileResult(
                epochReplaced,
                false,
                walPurgeFloor,
                normalizedBaseSeqTxn,
                cleanup.removed,
                cleanup.failed,
                cleanup.finalOrphanUpperBound,
                purgedSegments,
                failedPurges,
                repairSweep.getDiscardedRepairCount(),
                repairSweep.getFailedCount()
        );
    }

    private static final class CleanupResult {
        private long finalOrphanUpperBound;
        private int failed;
        private int removed;

        private CleanupResult(long finalOrphanUpperBound) {
            this.finalOrphanUpperBound = finalOrphanUpperBound;
        }
    }

    public static final class ReconcileResult {
        private static final ReconcileResult FORMAT_RESET =
                new ReconcileResult(false, true, -1, Numbers.LONG_NULL, 0, 0, 0, 0, 0, 0, 0);
        private static final ReconcileResult NOT_OWNER =
                new ReconcileResult(false, false, -1, Numbers.LONG_NULL, 0, 0, 0, 0, 0, 0, 0);
        private final int discardedRepairCount;
        private final boolean epochReplaced;
        private final int failedOrphanCount;
        private final int failedPurgeCount;
        private final int failedRepairCount;
        private final long finalOrphanUpperBound;
        private final boolean formatReset;
        private final long normalizedBaseSeqTxn;
        private final int purgedSegmentCount;
        private final int removedOrphanCount;
        private final long walPurgeFloor;

        private ReconcileResult(
                boolean epochReplaced,
                boolean formatReset,
                long walPurgeFloor,
                long normalizedBaseSeqTxn,
                int removedOrphanCount,
                int failedOrphanCount,
                long finalOrphanUpperBound,
                int purgedSegmentCount,
                int failedPurgeCount,
                int discardedRepairCount,
                int failedRepairCount
        ) {
            this.epochReplaced = epochReplaced;
            this.formatReset = formatReset;
            this.walPurgeFloor = walPurgeFloor;
            this.normalizedBaseSeqTxn = normalizedBaseSeqTxn;
            this.removedOrphanCount = removedOrphanCount;
            this.failedOrphanCount = failedOrphanCount;
            this.finalOrphanUpperBound = finalOrphanUpperBound;
            this.purgedSegmentCount = purgedSegmentCount;
            this.failedPurgeCount = failedPurgeCount;
            this.discardedRepairCount = discardedRepairCount;
            this.failedRepairCount = failedRepairCount;
        }

        /**
         * @return crashed repair candidates this reconciliation discarded
         */
        public int getDiscardedRepairCount() {
            return discardedRepairCount;
        }

        public int getFailedOrphanCount() {
            return failedOrphanCount;
        }

        public int getFailedPurgeCount() {
            return failedPurgeCount;
        }

        /**
         * @return repair descriptors this reconciliation could not unlink; the
         * next reconciliation retries them
         */
        public int getFailedRepairCount() {
            return failedRepairCount;
        }

        public long getFinalOrphanUpperBound() {
            return finalOrphanUpperBound;
        }

        /**
         * @return the selected generation's {@code normalizedBaseSeqTxn}, or
         * {@link Numbers#LONG_NULL} when no valid slot was adopted. This is the
         * base-transaction coordinate every current root is validated through, so
         * startup can publish the checkpoint head before a refresh worker pins the
         * generation and reads the root itself.
         */
        public long getNormalizedBaseSeqTxn() {
            return normalizedBaseSeqTxn;
        }

        public int getPurgedSegmentCount() {
            return purgedSegmentCount;
        }

        public int getRemovedOrphanCount() {
            return removedOrphanCount;
        }

        public long getWalPurgeFloor() {
            return walPurgeFloor;
        }

        public boolean isEpochReplaced() {
            return epochReplaced;
        }

        /**
         * @return true when this reconciliation removed a checkpoint directory
         * written under a layout this build cannot read, leaving the primary to
         * rebuild the timeline from the base table
         */
        public boolean isFormatReset() {
            return formatReset;
        }
    }

    public static final class CleanupStats {
        private final int failedCount;
        private final int removedCount;

        private CleanupStats(int removedCount, int failedCount) {
            this.removedCount = removedCount;
            this.failedCount = failedCount;
        }

        public int getFailedCount() {
            return failedCount;
        }

        public int getRemovedCount() {
            return removedCount;
        }
    }
}
