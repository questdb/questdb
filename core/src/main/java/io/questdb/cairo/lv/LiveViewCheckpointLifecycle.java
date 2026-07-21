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
 * <p>Startup reconciliation is bounded by physical segment count. It removes
 * recognized temporary files and records final-name files above both valid A/B
 * slot id ceilings. Final names are removed only after a new slot advances past
 * them, preserving monotonic allocation. Reconciliation never walks logical
 * checkpoint leaves. Zero-reference deletion delegates to
 * {@link LiveViewCheckpointDataStore}, retaining its old-slot, reader-pin,
 * candidate-ownership, and retry guards.</p>
 *
 * <p>Callers serialize reconciliation, epoch replacement, and retirement with
 * timeline publication and pin acquisition. The live-view integration does so
 * with the refresh latch (and fences DROP before retirement). Tests may pass an
 * open metadata store to {@link #retireTimeline} to assert that a live pin
 * defers deletion.</p>
 */
public final class LiveViewCheckpointLifecycle {

    private static final Log LOG = LogFactory.getLog(LiveViewCheckpointLifecycle.class);

    private LiveViewCheckpointLifecycle() {
    }

    /**
     * Reconciles one primary-owned timeline before recovery or a retrying
     * publication. A replica must pass {@code false}; that path is a strict
     * no-op and does not even create/open {@code _timeline}.
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

        final FilesFacade ff = configuration.getFilesFacade();
        try (Path timelinePath = new Path()) {
            LiveViewCheckpointLayout.timelinePath(timelinePath, checkpointsDir);
            if (!ff.exists(timelinePath.$())) {
                final CleanupResult cleanup = cleanupOrphans(configuration, checkpointsDir, 0);
                return result(false, -1, cleanup, 0, 0);
            }
        }

        boolean replaceEpoch = false;
        long nextSegmentIdCeiling = 0;
        long walPurgeFloor = -1;
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
            return new ReconcileResult(true, -1, 0, 0, 0, 0, 0);
        }

        final CleanupResult cleanup = cleanupOrphans(configuration, checkpointsDir, nextSegmentIdCeiling);
        return result(false, walPurgeFloor, cleanup, purgedSegments, failedPurges);
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

    private static ReconcileResult result(
            boolean epochReplaced,
            long walPurgeFloor,
            CleanupResult cleanup,
            int purgedSegments,
            int failedPurges
    ) {
        return new ReconcileResult(
                epochReplaced,
                walPurgeFloor,
                cleanup.removed,
                cleanup.failed,
                cleanup.finalOrphanUpperBound,
                purgedSegments,
                failedPurges
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
        private static final ReconcileResult NOT_OWNER = new ReconcileResult(false, -1, 0, 0, 0, 0, 0);
        private final boolean epochReplaced;
        private final int failedOrphanCount;
        private final int failedPurgeCount;
        private final long finalOrphanUpperBound;
        private final int purgedSegmentCount;
        private final int removedOrphanCount;
        private final long walPurgeFloor;

        private ReconcileResult(
                boolean epochReplaced,
                long walPurgeFloor,
                int removedOrphanCount,
                int failedOrphanCount,
                long finalOrphanUpperBound,
                int purgedSegmentCount,
                int failedPurgeCount
        ) {
            this.epochReplaced = epochReplaced;
            this.walPurgeFloor = walPurgeFloor;
            this.removedOrphanCount = removedOrphanCount;
            this.failedOrphanCount = failedOrphanCount;
            this.finalOrphanUpperBound = finalOrphanUpperBound;
            this.purgedSegmentCount = purgedSegmentCount;
            this.failedPurgeCount = failedPurgeCount;
        }

        public int getFailedOrphanCount() {
            return failedOrphanCount;
        }

        public int getFailedPurgeCount() {
            return failedPurgeCount;
        }

        public long getFinalOrphanUpperBound() {
            return finalOrphanUpperBound;
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
