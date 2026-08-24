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
import io.questdb.std.LongList;
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
 * files, removes every final name the generation it adopts does not catalogue,
 * and records the final-name files above both valid A/B slot id ceilings that it
 * had no catalogue to ask about. Those last are removed only after a new slot
 * advances past them, preserving monotonic allocation. Reconciliation walks the
 * segment catalogue but never the logical checkpoint leaves below it.
 * Zero-reference deletion delegates to {@link LiveViewCheckpointDataStore},
 * retaining its old-slot, reader-pin, candidate-ownership, and retry guards.</p>
 *
 * <p>That id-ceiling rule needs a reconciliation to read the ceiling before the
 * next publication moves it, which only a restart and a failed {@code append}
 * give it. {@link #purgeUncataloguedSegments} states the same disposition
 * against the catalogue instead, so it holds whatever the ceiling has since done.
 * Reconciliation applies it to every generation it adopts and the purge cadence
 * applies it on every sweep, so the two rules no longer overlap: the ceiling
 * decides only what no catalogue can speak for, which is a directory carrying no
 * valid generation at all. There every final name is an orphan by definition, and
 * no publication is there to have moved the ceiling off it.</p>
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
     * Reconciles one node-owned timeline before recovery or a retrying
     * publication. {@code primaryOwner} false is a strict no-op that does not
     * even create/open {@code _timeline}; under symmetric local refresh every
     * node owns the timeline it sealed, so every live-view caller passes
     * {@code true} regardless of role and the flag survives only as the
     * ownership assertion this class refuses to write without.
     * <p>
     * A directory written under a foreign layout short-circuits every other
     * rule: it is removed whole and the result reports
     * {@link ReconcileResult#isFormatReset()}, leaving the caller with the same
     * disposition a live view that never checkpointed has.
     * <p>
     * A reconciliation that adopts a generation reclaims under both halves of the
     * catalogue's reachability rule, exactly as a cadence sweep does: the segments
     * the adopted generation no longer reaches through
     * {@link LiveViewCheckpointDataStore#purge}, and the final-name files it never
     * catalogued at all through {@link #purgeUncataloguedSegments}. Only where no
     * generation is adopted does the deferred id-ceiling rule decide anything, and
     * there the ceiling is zero, so it names every final-name file on disk.
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
                return result(false, -1, Numbers.LONG_NULL, cleanup, null, null, repairSweep);
            }
        }

        boolean replaceEpoch = false;
        boolean catalogueMismatch = false;
        long nextSegmentIdCeiling = 0;
        long walPurgeFloor = -1;
        long normalizedBaseSeqTxn = Numbers.LONG_NULL;
        LiveViewCheckpointDataStore.PurgeResult purgeResult = null;
        LiveViewCheckpointTimelineStats stats = null;
        CleanupStats uncatalogued = CleanupStats.NONE;
        try (LiveViewCheckpointMetaStore metaStore = new LiveViewCheckpointMetaStore(configuration)) {
            metaStore.of(checkpointsDir);
            if (metaStore.isValid()) {
                final LiveViewCheckpointSuperblock superblock = metaStore.getSuperblock();
                replaceEpoch = superblock.definitionTxn != expectedDefinitionTxn
                        || superblock.historyEpoch != expectedHistoryEpoch;
                catalogueMismatch = !replaceEpoch
                        && hasUnregisteredRootSegment(configuration, checkpointsDir, superblock);
                if (!replaceEpoch && !catalogueMismatch) {
                    nextSegmentIdCeiling = superblock.getNextSegmentIdCeiling();
                    walPurgeFloor = metaStore.getWalPurgeFloor();
                    normalizedBaseSeqTxn = superblock.normalizedBaseSeqTxn;
                    // This reconciliation publishes no root of its own, so the
                    // adopted generation's last publication cost is not its to
                    // report.
                    stats = new LiveViewCheckpointTimelineStats().of(superblock, 0);
                    try (LiveViewCheckpointDataStore dataStore = new LiveViewCheckpointDataStore(
                            configuration,
                            metaStore
                    )) {
                        dataStore.of(checkpointsDir);
                        purgeResult = dataStore.purge();
                    }
                    // The sweep above decides the fate of every segment this
                    // generation ever named; this decides the fate of the files it
                    // never named at all. Deciding here rather than recording an id
                    // range for the next publication to act on is what makes the
                    // rule survive: a ceiling stops naming a file as soon as any
                    // publication steps over its id, and a catalogue that never held
                    // the id keeps saying so.
                    uncatalogued = purgeUncataloguedSegments(configuration, checkpointsDir, superblock, true);
                }
            }
        }

        if (catalogueMismatch) {
            // The generation names a metadata segment its own catalogue does not
            // hold, so nothing can decide that file's fate and no count describes
            // what still reaches it. The timeline is derived state: discard it and
            // rebuild from the base table rather than publish over a catalogue
            // that has already lost track of its files.
            resetForeignFormat(configuration, checkpointsDir);
            return ReconcileResult.FORMAT_RESET;
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
                    null,
                    null,
                    repairSweep.getDiscardedRepairCount(),
                    repairSweep.getFailedCount()
            );
        }

        // Whatever the pass above removed is gone before this one looks, so the
        // upper bound it records covers only the names the catalogue could not
        // speak for. When a generation was adopted that is nothing at all: every
        // catalogued id sits below the ceiling by construction, so the bound stays
        // at the ceiling and the deferred rule has no range left to apply.
        final CleanupResult cleanup = cleanupOrphans(configuration, checkpointsDir, nextSegmentIdCeiling);
        cleanup.add(uncatalogued);
        return result(false, walPurgeFloor, normalizedBaseSeqTxn, cleanup, purgeResult, stats, repairSweep);
    }

    /**
     * Removes final-name orphans after a new slot durably advances past them.
     * <p>
     * This is the id-ceiling rule, and it only ever holds for the files a
     * publication left above the ceiling the reconciliation beside it read. Once
     * any later publication has stepped its own allocation over one of those ids,
     * the file drops below the ceiling and this rule can no longer name it -
     * {@link #purgeUncataloguedSegments} is the one that still can.
     * <p>
     * What is left to it is the case that rule cannot reach from the other side: a
     * directory carrying no valid generation, and therefore no catalogue to ask.
     * A reconciliation there reads a ceiling of zero, so every final name is an
     * orphan, and the deferral costs one publication rather than a restart. Where
     * a generation was adopted, reconciliation has already removed every
     * uncatalogued final name and the range this receives is empty.
     */
    public static CleanupStats purgeFinalOrphans(
            @NotNull CairoConfiguration configuration,
            @NotNull Path checkpointsDir,
            long protectedCeiling,
            long orphanUpperBound,
            boolean primaryOwner
    ) {
        // Ordered before the scratch: a steady cadence seal reaches this with an
        // upper bound the ceiling already covers, so the common path builds nothing.
        if (!primaryOwner || orphanUpperBound <= protectedCeiling) {
            return CleanupStats.NONE;
        }
        final CleanupResult result = new CleanupResult(protectedCeiling);
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
        return new CleanupStats(result.removed, result.failed, result.visited);
    }

    /**
     * Removes every final-name segment file the newest durable generation neither
     * catalogues nor names as its pending directory segment.
     * <p>
     * Since Phase 2a/2b the catalogue holds an entry for every segment a published
     * root can reach - data, tree metadata and boundary metadata alike - and the
     * one documented exception is the segment carrying the directory root itself,
     * which the superblock names as pending because a tree cannot list the file it
     * is being written into. A final-name file outside both sets is therefore
     * reachable from nothing, whatever its id: it is what a publication that
     * renamed its segments into place and then failed left behind.
     * <p>
     * The rule this applies is strictly stronger than
     * {@link #purgeFinalOrphans}'s, and it is stronger in the direction that
     * matters. The id ceiling decays - {@code skipPublishedSegmentIds} steps the
     * next publication's allocation over an orphan, the ceiling rises past it, and
     * no later reconciliation can tell it from a live segment again - while a
     * catalogue that never held the id keeps saying so. That is why a failed
     * compaction or repair publication used to leak its segments for the life of
     * the directory: neither re-runs a reconciliation, so nothing read the ceiling
     * before the next seal moved it.
     * <p>
     * Both collectors apply it. The purge cadence runs it beside every sweep, so a
     * running process collects without waiting; {@link #reconcile} runs it over
     * every generation it adopts, so a process that swept nothing - the cadence
     * disabled, or a view that never sealed again after the failure - collects at
     * its next restart rather than never.
     * <p>
     * Nothing has to advance past these ids first, because the monotonic
     * {@code nextSegmentId} is what preserves allocation order on its own -
     * unlinking a file never lowers it, and the id skip only ever moves forward.
     * The pass publishes no generation, so a fault costs one deferred collection.
     * <p>
     * It is fail-closed at every step it cannot complete: a catalogue this build
     * cannot read, a directory root the superblock does not name, or a slot that
     * lost the newest-generation race all leave every file where it is. Keeping a
     * dead file costs disk; unlinking a live one costs the timeline.
     * <p>
     * Temporary files are not this pass's business. A {@code .tmp} belongs to
     * whichever writer is holding it open - a repair capture spans several
     * {@code capture} calls before it commits - so ownership rather than
     * reachability decides its fate, and reconciliation, which runs where no
     * writer can own one, is where that decision belongs.
     */
    public static CleanupStats purgeUncataloguedSegments(
            @NotNull CairoConfiguration configuration,
            @NotNull Path checkpointsDir,
            @NotNull LiveViewCheckpointSuperblock superblock,
            boolean primaryOwner
    ) {
        if (!primaryOwner || !superblock.isSelectedSlotNewest() || superblock.segmentDirectoryRootRef.isNull()) {
            // A root that failed bounded validation, or a generation whose
            // catalogue has no root at all, is no evidence that anything on disk
            // is free.
            return new CleanupStats(0, 0, 0);
        }
        final CleanupResult result = new CleanupResult(0);
        try (LiveViewCheckpointSegmentDirectoryReader directory =
                     new LiveViewCheckpointSegmentDirectoryReader(configuration)) {
            directory.of(checkpointsDir, superblock.segmentDirectoryRootRef);
            final LiveViewCheckpointSegmentDirectoryEntry entry = new LiveViewCheckpointSegmentDirectoryEntry();
            try (Path path = new Path()) {
                purgeUncataloguedSegmentsInDir(
                        configuration.getFilesFacade(),
                        LiveViewCheckpointLayout.metaDirPath(path, checkpointsDir),
                        LiveViewCheckpointLayout.META_SEGMENT_PREFIX,
                        directory,
                        entry,
                        superblock.pendingDirectorySegmentId,
                        result
                );
                purgeUncataloguedSegmentsInDir(
                        configuration.getFilesFacade(),
                        LiveViewCheckpointLayout.dataDirPath(path, checkpointsDir),
                        LiveViewCheckpointLayout.DATA_SEGMENT_PREFIX,
                        directory,
                        entry,
                        superblock.pendingDirectorySegmentId,
                        result
                );
            }
        } catch (CairoException e) {
            LOG.error().$("could not read the live view checkpoint catalogue while collecting orphans [path=")
                    .$(checkpointsDir).$(", error=").$safe(e.getFlyweightMessage()).I$();
        }
        return new CleanupStats(result.removed, result.failed, result.visited);
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
            // The prefix-preservation repair marker is void once the timeline it
            // guarded is gone: a stale marker left behind would force every restart
            // to rebuild against the fresh timeline this retire precedes. The
            // staged sibling counts as a marker too, so it has to go with it.
            LiveViewCheckpointLayout.repairingMarkerPath(path, checkpointsDir);
            if (ff.exists(path.$()) && !ff.removeQuiet(path.$())) {
                success = false;
                logRemoveFailure(ff, path);
            }
            LiveViewCheckpointLayout.repairingMarkerPath(path, checkpointsDir);
            path.put(LiveViewCheckpointLayout.TMP_SUFFIX);
            if (ff.exists(path.$()) && !ff.removeQuiet(path.$())) {
                success = false;
                logRemoveFailure(ff, path);
            }
            LiveViewCheckpointLayout.retirementQueuePath(path, checkpointsDir);
            if (ff.exists(path.$()) && !ff.removeQuiet(path.$())) {
                success = false;
                logRemoveFailure(ff, path);
            }
            path.put(LiveViewCheckpointLayout.TMP_SUFFIX);
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
                result.visited++;
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
     * current layout. Everything this build writes there is one of six names -
     * the {@code _timeline} superblock, the {@code _repairing} prefix-preservation
     * marker, the {@code _retirements} work set, and the {@code meta}, {@code data}
     * and {@code repair} directories - so anything else came from a build that
     * arranged checkpoint state differently. Earlier development builds left the
     * {@code _ring} manifest and
     * per-checkpoint {@code .cp} / {@code .scp} files at this level, which is what
     * the check most often finds.
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
                // The marker prefix covers both _repairing and a crash-orphaned
                // _repairing.tmp; nothing else this build writes starts with it.
                if (!Utf8s.utf8ToUtf16Z(namePtr, name)
                        || Chars.equals(name, ".")
                        || Chars.equals(name, "..")
                        || Chars.equals(name, LiveViewCheckpointLayout.TIMELINE_FILE_NAME)
                        || Chars.startsWith(name, LiveViewCheckpointLayout.REPAIRING_MARKER_FILE_NAME)
                        || Chars.equals(name, LiveViewCheckpointLayout.RETIREMENT_QUEUE_FILE_NAME)
                        || Chars.equals(name, LiveViewCheckpointLayout.RETIREMENT_QUEUE_TMP_FILE_NAME)
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

    /**
     * Checks the one crash-safety exception the catalogue allows. Every metadata
     * segment a published root names is registered by the publication that wrote
     * it, except the one carrying the segment directory itself: a directory tree
     * cannot list the file it is being written into, so the superblock names it as
     * pending and the next publication registers it. Any other root segment the
     * catalogue does not hold means the generation and its own catalogue disagree,
     * and no reference count then describes what still reaches that file.
     * <p>
     * The three superblock-rooted trees are what this walks. The boundary metadata
     * below a timeline entry obeys the same rule, but proving it needs the whole
     * retained closure - one partition-map walk per surviving boundary - which is
     * the sweep the accounting exists to avoid.
     */
    private static boolean hasUnregisteredRootSegment(
            @NotNull CairoConfiguration configuration,
            @NotNull Path checkpointsDir,
            @NotNull LiveViewCheckpointSuperblock superblock
    ) {
        try (LiveViewCheckpointSegmentDirectoryReader directory =
                     new LiveViewCheckpointSegmentDirectoryReader(configuration)) {
            directory.of(checkpointsDir, superblock.segmentDirectoryRootRef);
            final LiveViewCheckpointSegmentDirectoryEntry entry = new LiveViewCheckpointSegmentDirectoryEntry();
            return isUnregistered(directory, entry, superblock.timelineRootRef, superblock, checkpointsDir, "timeline")
                    || isUnregistered(directory, entry, superblock.rowPositionDeltaRootRef, superblock, checkpointsDir, "row position delta")
                    || isUnregistered(directory, entry, superblock.segmentDirectoryRootRef, superblock, checkpointsDir, "segment directory");
        } catch (CairoException e) {
            LOG.error().$("could not read the live view checkpoint segment catalogue [path=")
                    .$(checkpointsDir).$(", error=").$safe(e.getFlyweightMessage()).I$();
            return true;
        }
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

    private static boolean isUnregistered(
            @NotNull LiveViewCheckpointSegmentDirectoryReader directory,
            @NotNull LiveViewCheckpointSegmentDirectoryEntry entry,
            @NotNull LiveViewCheckpointPageRef rootRef,
            @NotNull LiveViewCheckpointSuperblock superblock,
            @NotNull Path checkpointsDir,
            @NotNull CharSequence what
    ) {
        if (rootRef.isNull()
                || rootRef.getSegmentId() == superblock.pendingDirectorySegmentId
                || directory.find(rootRef.getSegmentId(), entry)) {
            return false;
        }
        LOG.error().$("live view checkpoint root names a segment its own catalogue does not hold [path=")
                .$(checkpointsDir).$(", root=").$(what)
                .$(", segmentId=").$(rootRef.getSegmentId())
                .$(", generation=").$(superblock.generation).I$();
        return true;
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

    private static void purgeUncataloguedSegmentsInDir(
            @NotNull FilesFacade ff,
            @NotNull Path dir,
            @NotNull CharSequence prefix,
            @NotNull LiveViewCheckpointSegmentDirectoryReader directory,
            @NotNull LiveViewCheckpointSegmentDirectoryEntry entry,
            long pendingDirectorySegmentId,
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
                result.visited++;
                name.clear();
                if (!Utf8s.utf8ToUtf16Z(namePtr, name)
                        || !Chars.startsWith(name, prefix)
                        || Chars.endsWith(name, LiveViewCheckpointLayout.TMP_SUFFIX)) {
                    continue;
                }
                final long segmentId = parseSegmentId(name, prefix.length(), name.length());
                // An unparsable name is not a segment this build wrote, the pending
                // id is the one live segment no catalogue may hold, and everything
                // the catalogue does hold is either live or the purge sweep's to
                // decide on.
                if (segmentId < 0
                        || segmentId == pendingDirectorySegmentId
                        || directory.find(segmentId, entry)) {
                    continue;
                }
                LOG.info().$("removing a live view checkpoint segment no generation catalogues [dir=")
                        .$(dir).$(", name=").$safe(name).I$();
                removeFile(ff, dir, dirLen, name, result);
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
            @Nullable LiveViewCheckpointDataStore.PurgeResult purge,
            @Nullable LiveViewCheckpointTimelineStats stats,
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
                purge,
                stats,
                repairSweep.getDiscardedRepairCount(),
                repairSweep.getFailedCount()
        );
    }

    private static final class CleanupResult {
        private long finalOrphanUpperBound;
        private int failed;
        private int removed;
        private int visited;

        private CleanupResult(long finalOrphanUpperBound) {
            this.finalOrphanUpperBound = finalOrphanUpperBound;
        }

        /**
         * Folds in what a pass that removed files under its own rule accounted for,
         * so one reconciliation reports one removal count however many rules
         * produced it.
         */
        private void add(CleanupStats stats) {
            removed += stats.removedCount;
            failed += stats.failedCount;
        }
    }

    public static final class ReconcileResult {
        private static final LongList EMPTY_SEGMENT_IDS = new LongList();
        private static final ReconcileResult FORMAT_RESET =
                new ReconcileResult(false, true, -1, Numbers.LONG_NULL, 0, 0, 0, null, null, 0, 0);
        private static final ReconcileResult NOT_OWNER =
                new ReconcileResult(false, false, -1, Numbers.LONG_NULL, 0, 0, 0, null, null, 0, 0);
        private final int discardedRepairCount;
        private final boolean epochReplaced;
        private final int failedOrphanCount;
        private final int failedPurgeCount;
        private final int failedRepairCount;
        private final long finalOrphanUpperBound;
        private final boolean formatReset;
        private final int liveSegmentCount;
        private final long normalizedBaseSeqTxn;
        private final long obsoleteSegmentBytes;
        private final int purgedSegmentCount;
        private final int removedOrphanCount;
        private final LongList retirableSegmentIds;
        private final LiveViewCheckpointTimelineStats stats;
        private final long walPurgeFloor;

        private ReconcileResult(
                boolean epochReplaced,
                boolean formatReset,
                long walPurgeFloor,
                long normalizedBaseSeqTxn,
                int removedOrphanCount,
                int failedOrphanCount,
                long finalOrphanUpperBound,
                @Nullable LiveViewCheckpointDataStore.PurgeResult purge,
                @Nullable LiveViewCheckpointTimelineStats stats,
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
            this.purgedSegmentCount = purge == null ? 0 : purge.getPurgedSegmentCount();
            this.failedPurgeCount = purge == null ? 0 : purge.getFailedSegmentCount();
            this.liveSegmentCount = purge == null ? 0 : purge.getLiveSegmentCount();
            this.obsoleteSegmentBytes = purge == null ? 0 : purge.getObsoleteBytes();
            this.retirableSegmentIds = purge == null ? EMPTY_SEGMENT_IDS : purge.getRetirableSegmentIds();
            this.stats = stats;
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

        /**
         * @return the exclusive id bound the deferred orphan rule applies over,
         * for the next publication to hand back to
         * {@link #purgeFinalOrphans}. It exceeds the ceiling only where this
         * reconciliation adopted no generation and so had no catalogue to ask;
         * where it adopted one, it has already removed every uncatalogued final
         * name outright and this equals the ceiling
         */
        public long getFinalOrphanUpperBound() {
            return finalOrphanUpperBound;
        }

        /**
         * @return data segments a current logical root still names, as counted by
         * the purge sweep this reconciliation ran. Zero when no valid slot was
         * adopted, so the sweep never walked the catalogue
         */
        public int getLiveSegmentCount() {
            return liveSegmentCount;
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

        /**
         * @return bytes of retired data segments the sweep left on disk, still
         * protected by the fallback slot or a reader pin
         */
        public long getObsoleteSegmentBytes() {
            return obsoleteSegmentBytes;
        }

        public int getPurgedSegmentCount() {
            return purgedSegmentCount;
        }

        public int getRemovedOrphanCount() {
            return removedOrphanCount;
        }

        /**
         * @return catalogue entries whose segment file the sweep left unlinked, in
         * ascending id order. Nothing else removes an entry, so a publication has
         * to carry these away for the catalogue to stop growing with the view's
         * age; empty when this reconciliation adopted no generation
         */
        public LongList getRetirableSegmentIds() {
            return retirableSegmentIds;
        }

        /**
         * @return the shape of the adopted generation, or null when this
         * reconciliation adopted none
         */
        public @Nullable LiveViewCheckpointTimelineStats getStats() {
            return stats;
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
        private static final CleanupStats NONE = new CleanupStats(0, 0, 0);
        private final int failedCount;
        private final int removedCount;
        private final int visitedCount;

        private CleanupStats(int removedCount, int failedCount, int visitedCount) {
            this.removedCount = removedCount;
            this.failedCount = failedCount;
            this.visitedCount = visitedCount;
        }

        public int getFailedCount() {
            return failedCount;
        }

        public int getRemovedCount() {
            return removedCount;
        }

        public int getVisitedCount() {
            return visitedCount;
        }
    }
}
