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
import io.questdb.cairo.CommitMode;
import io.questdb.cairo.lv.LiveViewCheckpointContracts.HighBoundTag;
import io.questdb.cairo.lv.LiveViewCheckpointContracts.RepairPublicationStage;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryCMR;
import io.questdb.cairo.vm.api.MemoryMARW;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.Chars;
import io.questdb.std.Files;
import io.questdb.std.FilesFacade;
import io.questdb.std.LongList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.NumericException;
import io.questdb.std.Numbers;
import io.questdb.std.Transient;
import io.questdb.std.Zip;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf8s;
import org.jetbrains.annotations.NotNull;

import java.io.Closeable;

/**
 * The durable descriptor of one in-progress localized out-of-order repair:
 * {@code _checkpoints/repair/r.<repairId>}.
 * <p>
 * A repair reads and replays a bounded base interval, freezes a new root version
 * for every logical checkpoint boundary it crosses into a temporary data segment,
 * and publishes all of it as one timeline range splice. Until that splice commits
 * the superblock, none of the work is reachable from any generation. The
 * descriptor records, durably, that the work exists: which snapshot the repair
 * pinned, which bounds it derived, how far its replay got, which publication
 * stage it reached, and - the part nothing else on disk can express - which
 * temporary segments it owns.
 *
 * <h2>What the descriptor is for, and what it is not for</h2>
 * It makes discard-and-replan crash-safe and bounded. It does <b>not</b> make a
 * partial replay resumable across a crash: the repair's pinned
 * {@code TableReader} dies with the process and QuestDB exposes no as-of reader,
 * so the snapshot {@code E} the bounds were derived against cannot be reopened.
 * Startup therefore validates temporary-segment ownership, discards the partial
 * candidate through {@link #sweep}, and lets a later refresh turn replan at a
 * freshly pinned {@code E}. Within one process a repair that yields on its turn
 * budget does continue - from the live {@link LiveViewCheckpointRepairSession},
 * which still holds the pinned reader; what this record contributes there is the
 * ownership claim over the segments the parked repair has already staged.
 *
 * <h2>Best-effort updates</h2>
 * {@link #begin} and {@link #sweep} report failure to their callers: the first
 * runs before the repair commits to anything, and the second is the cleanup path
 * itself. The in-flight updates - {@link #recordStage} and
 * {@link #recordProgress} - are deliberately best-effort and log rather than
 * throw. Once a replay is under way, abandoning it because a bookkeeping file
 * could not be rewritten is strictly worse than leaving a stale descriptor
 * behind, which the next sweep removes anyway.
 *
 * <h2>File format</h2>
 * A fixed header, then the owned-segment id array, then a CRC32 over the whole
 * record. Every write stages the complete record as {@code r.<repairId>.tmp} and
 * renames it into place, so a reader observes either the previous record or the
 * new one, and a crash mid-write leaves only a {@code .tmp} orphan. That holds
 * on POSIX; on Windows a rewrite unlinks the previous record before renaming, so
 * a reader can instead observe no descriptor at all - see
 * {@link LiveViewCheckpointLayout#publishOverwrite}.
 * <p>
 * Callers serialize descriptor writes with {@link #sweep} the same way they
 * serialize timeline publication with reconciliation - in the live-view
 * integration, through the refresh latch. A sweep therefore only ever meets
 * descriptors of repairs that are not running, which is exactly the crashed case
 * it exists to clean up.
 */
public final class LiveViewCheckpointRepairState implements Closeable {
    private static final HighBoundTag[] HIGH_BOUND_TAGS = HighBoundTag.values();
    private static final RepairPublicationStage[] REPAIR_PUBLICATION_STAGES = RepairPublicationStage.values();

    /**
     * Descriptor format version. Bump on an incompatible framing change.
     */
    public static final int FORMAT_VERSION = 1;
    public static final int FORMAT_VERSION_OFFSET = 8;
    /**
     * Bytes ahead of the owned-segment id array.
     */
    public static final int HEADER_SIZE = 120;
    /**
     * Magic marking a repair descriptor: ASCII {@code "LVRPST"} with a trailing
     * version nibble, so a foreign or zeroed file is rejected before the checksum
     * runs.
     */
    public static final long MAGIC = 0x4C56_5250_5354_0001L;
    public static final int MAGIC_OFFSET = 0;
    /**
     * Upper bound on the owned-segment id array, so a corrupt count cannot make a
     * reader map or walk an arbitrary extent. A repair owns one data segment per
     * turn it stages work in, so this also bounds how many turns one descriptor
     * can carry.
     */
    public static final int OWNED_SEGMENT_LIMIT = 1024;
    private static final int CORRECTION_TS_OFFSET = 64;
    private static final int DEFINITION_TXN_OFFSET = 24;
    private static final int GENERATION_OFFSET = 40;
    private static final int HIGH_BOUND_TAG_OFFSET = 96;
    private static final int HIGH_TS_EXCLUSIVE_OFFSET = 88;
    private static final int HISTORY_EPOCH_OFFSET = 32;
    private static final int LAST_COMPLETED_TS_GROUP_OFFSET = 104;
    private static final Log LOG = LogFactory.getLog(LiveViewCheckpointRepairState.class);
    private static final int NEXT_CHECKPOINT_ID_OFFSET = 112;
    /**
     * Stage value stored for a repair that recorded none.
     */
    private static final int NO_STAGE = -1;
    private static final int OUTPUT_LOW_TS_OFFSET = 80;
    private static final int OWNED_SEGMENT_COUNT_OFFSET = 100;
    private static final int PINNED_BASE_SEQ_TXN_OFFSET = 56;
    private static final int REPAIR_ID_OFFSET = 16;
    private static final int REPLAY_LOW_TS_OFFSET = 72;
    private static final int STAGE_OFFSET = 12;
    private static final int TRIGGER_BASE_SEQ_TXN_OFFSET = 48;
    /**
     * Mapping size one descriptor write allocates before it truncates to the
     * exact record size. Ample for the header plus the owned-segment array.
     */
    private static final int WRITE_EXTEND_SIZE = 16 * 1024;
    private final Path checkpointsDir = new Path();
    private final int commitMode;
    private final FilesFacade ff;
    private final Path finalPath = new Path();
    private final MemoryMARW mem;
    private final int mkDirMode;
    private final LongList ownedSegmentIds = new LongList();
    private final Path tmpPath = new Path();
    private long correctionTs = Numbers.LONG_NULL;
    private long definitionTxn;
    private long generation;
    private HighBoundTag highBoundTag = HighBoundTag.EOF;
    private long highTsExclusive = Numbers.LONG_NULL;
    private long historyEpoch;
    private boolean isOpen;
    private long lastCompletedTimestampGroup = Numbers.LONG_NULL;
    private long nextCheckpointId = Numbers.LONG_NULL;
    private long outputLowTs = Numbers.LONG_NULL;
    private long pinnedBaseSeqTxn;
    private long repairId = -1;
    private long replayLowTs = Numbers.LONG_NULL;
    private RepairPublicationStage stage;
    private long triggerBaseSeqTxn;

    public LiveViewCheckpointRepairState(@NotNull CairoConfiguration configuration) {
        this.ff = configuration.getFilesFacade();
        this.commitMode = configuration.getCommitMode();
        this.mkDirMode = configuration.getMkDirMode();
        this.mem = Vm.getCMARWInstance();
    }

    /**
     * Discards every repair descriptor under {@code checkpointsDir} together with
     * the temporary segments it owns. This is the crash-time
     * discard-and-replan: the pinned snapshot a descriptor names cannot be
     * reopened, so its candidate is worthless, while the temporary segments it
     * staged are real files whose only ownership record is the descriptor itself.
     * <p>
     * A descriptor that fails validation is removed too. Its ownership claim is
     * unreadable, and whatever it staged is covered by the generic {@code .tmp}
     * cleanup that runs alongside this sweep.
     * <p>
     * Final-name segments are never removed here. A repair that renamed a segment
     * into its final name before crashing leaves it above the published
     * next-segment ceiling, where the monotonic-allocation rule protects it until
     * a later publication durably advances past it.
     *
     * @return what the sweep found and removed
     */
    public static SweepResult sweep(
            @NotNull CairoConfiguration configuration,
            @Transient @NotNull Path checkpointsDir,
            boolean primaryOwner
    ) {
        if (!primaryOwner) {
            return SweepResult.EMPTY;
        }
        final FilesFacade ff = configuration.getFilesFacade();
        // Negative entries are torn (.tmp) descriptors, encoded so one list
        // carries both kinds out of the directory walk.
        final LongList repairIds = new LongList();
        try (Path dir = new Path()) {
            LiveViewCheckpointLayout.repairDirPath(dir, checkpointsDir);
            if (!ff.exists(dir.$())) {
                return SweepResult.EMPTY;
            }
            final long findPtr = ff.findFirst(dir.$());
            if (findPtr == 0) {
                return SweepResult.EMPTY;
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
                            || !Chars.startsWith(name, LiveViewCheckpointLayout.REPAIR_DESCRIPTOR_PREFIX)) {
                        continue;
                    }
                    final boolean temporary = Chars.endsWith(name, LiveViewCheckpointLayout.TMP_SUFFIX);
                    final int hi = name.length()
                            - (temporary ? LiveViewCheckpointLayout.TMP_SUFFIX.length() : 0);
                    final long id = parseId(name, LiveViewCheckpointLayout.REPAIR_DESCRIPTOR_PREFIX.length(), hi);
                    if (id < 0) {
                        continue;
                    }
                    repairIds.add(temporary ? -id - 1 : id);
                } while (ff.findNext(findPtr) > 0);
            } finally {
                ff.findClose(findPtr);
            }
        }

        int discarded = 0;
        int segments = 0;
        int failed = 0;
        try (Path path = new Path()) {
            for (int i = 0, n = repairIds.size(); i < n; i++) {
                final long encoded = repairIds.getQuick(i);
                if (encoded < 0) {
                    // A .tmp descriptor is a torn write with no readable ownership
                    // claim; unlink it and let the same id's final name, if it has
                    // one, be swept as its own entry.
                    LiveViewCheckpointLayout.repairDescriptorTmpPath(path, checkpointsDir, -encoded - 1);
                    if (!ff.removeQuiet(path.$())) {
                        failed++;
                        logRemoveFailure(ff, path);
                    }
                    continue;
                }
                discarded++;
                segments += discardOwnedSegments(configuration, checkpointsDir, encoded);
                LiveViewCheckpointLayout.repairDescriptorPath(path, checkpointsDir, encoded);
                if (!ff.removeQuiet(path.$())) {
                    failed++;
                    logRemoveFailure(ff, path);
                }
            }
        }
        if (discarded > 0 || failed > 0) {
            LOG.info().$("swept live view checkpoint repair descriptors [path=").$(checkpointsDir)
                    .$(", discarded=").$(discarded)
                    .$(", segments=").$(segments)
                    .$(", failed=").$(failed).I$();
        }
        return new SweepResult(discarded, segments, failed);
    }

    /**
     * Claims one more temporary segment for the repair, durably. The claim is
     * published before the segment it names is written, which is the order a
     * crash-time sweep needs: an unclaimed file is indistinguishable from any
     * other orphan.
     */
    public void addOwnedSegmentId(long segmentId) {
        ensureOpen();
        if (segmentId < 0) {
            throw CairoException.critical(0)
                    .put("live view checkpoint repair segment id must be non-negative, was ")
                    .put(segmentId);
        }
        if (ownedSegmentIds.size() >= OWNED_SEGMENT_LIMIT) {
            throw CairoException.critical(0)
                    .put("live view checkpoint repair owns too many temporary segments, limit=")
                    .put(OWNED_SEGMENT_LIMIT);
        }
        ownedSegmentIds.add(segmentId);
        persist();
    }

    /**
     * Opens a repair descriptor and writes it at
     * {@link RepairPublicationStage#PLAN}. The live-view integration identifies a
     * repair by the base snapshot it pinned, so a repair repeated against the same
     * {@code E} - a deferred replacement the next turn re-materialises - rewrites
     * its own descriptor instead of leaving a second one behind.
     *
     * @param repairId          the repair's identity within its history epoch
     * @param generation        the timeline generation the repair's capture pinned
     * @param pinnedBaseSeqTxn  {@code E}, the applied base {@code seqTxn} the
     *                          repair pinned and derived every bound against
     * @param triggerBaseSeqTxn base {@code seqTxn} that triggered the repair; with
     *                          {@code E} it identifies the change set the repair
     *                          classified, including any apply-ahead range
     * @param correctionTs      {@code C}, the floor roots are re-versioned from
     * @param replayLowTs       {@code L}, the lowest base row the replay reads
     * @param outputLowTs       {@code R}, the lowest output row the replay emits
     * @param highTsExclusive   {@code H}, or {@link Numbers#LONG_NULL} under
     *                          {@link HighBoundTag#EOF}
     */
    public void begin(
            @Transient @NotNull Path checkpointsDir,
            long repairId,
            long definitionTxn,
            long historyEpoch,
            long generation,
            long pinnedBaseSeqTxn,
            long triggerBaseSeqTxn,
            long correctionTs,
            long replayLowTs,
            long outputLowTs,
            long highTsExclusive,
            @NotNull HighBoundTag highBoundTag
    ) {
        if (repairId < 0 || definitionTxn < 0 || historyEpoch < 0 || generation < 0) {
            throw CairoException.critical(0)
                    .put("invalid live view checkpoint repair identity [repairId=").put(repairId)
                    .put(", definitionTxn=").put(definitionTxn)
                    .put(", historyEpoch=").put(historyEpoch)
                    .put(", generation=").put(generation).put(']');
        }
        clear();
        this.checkpointsDir.of(checkpointsDir);
        this.repairId = repairId;
        this.definitionTxn = definitionTxn;
        this.historyEpoch = historyEpoch;
        this.generation = generation;
        this.pinnedBaseSeqTxn = pinnedBaseSeqTxn;
        this.triggerBaseSeqTxn = triggerBaseSeqTxn;
        this.correctionTs = correctionTs;
        this.replayLowTs = replayLowTs;
        this.outputLowTs = outputLowTs;
        this.highTsExclusive = highTsExclusive;
        this.highBoundTag = highBoundTag;
        this.stage = RepairPublicationStage.PLAN;
        this.isOpen = true;
        ensureRepairDir();
        persist();
    }

    @Override
    public void close() {
        Misc.free(mem);
        Misc.free(tmpPath);
        Misc.free(finalPath);
        Misc.free(checkpointsDir);
        clear();
    }

    /**
     * Unlinks the descriptor and forgets its ownership claim. Called when the
     * repair's splice publishes - its segments are reachable from a generation by
     * then - and on every path that abandons the candidate instead.
     *
     * @return true when no descriptor is left on disk
     */
    public boolean discard() {
        if (!isOpen) {
            return true;
        }
        boolean removed = true;
        LiveViewCheckpointLayout.repairDescriptorTmpPath(tmpPath, checkpointsDir, repairId);
        if (ff.exists(tmpPath.$()) && !ff.removeQuiet(tmpPath.$())) {
            removed = false;
            logRemoveFailure(ff, tmpPath);
        }
        LiveViewCheckpointLayout.repairDescriptorPath(finalPath, checkpointsDir, repairId);
        if (ff.exists(finalPath.$()) && !ff.removeQuiet(finalPath.$())) {
            removed = false;
            logRemoveFailure(ff, finalPath);
        }
        clear();
        return removed;
    }

    /**
     * @return {@code C}, the correction floor the repair re-versions roots from
     */
    public long getCorrectionTs() {
        return correctionTs;
    }

    public long getDefinitionTxn() {
        return definitionTxn;
    }

    /**
     * @return the timeline generation the repair's capture pinned
     */
    public long getGeneration() {
        return generation;
    }

    public HighBoundTag getHighBoundTag() {
        return highBoundTag;
    }

    /**
     * @return {@code H}, or {@link Numbers#LONG_NULL} under {@link HighBoundTag#EOF}
     */
    public long getHighTsExclusive() {
        return highTsExclusive;
    }

    public long getHistoryEpoch() {
        return historyEpoch;
    }

    /**
     * @return the designated timestamp of the last complete timestamp group the
     * replay reproduced, or {@link Numbers#LONG_NULL} before it completed one
     */
    public long getLastCompletedTimestampGroup() {
        return lastCompletedTimestampGroup;
    }

    /**
     * @return the {@code checkpointId} of the next logical boundary the replay
     * still owes a root version, or {@link Numbers#LONG_NULL} when it owes none
     */
    public long getNextCheckpointId() {
        return nextCheckpointId;
    }

    /**
     * @return {@code R}, the output floor the replacement starts at
     */
    public long getOutputLowTs() {
        return outputLowTs;
    }

    public long getOwnedSegmentId(int index) {
        return ownedSegmentIds.getQuick(index);
    }

    public int getOwnedSegmentIdCount() {
        return ownedSegmentIds.size();
    }

    /**
     * @return {@code E}, the applied base {@code seqTxn} the repair pinned
     */
    public long getPinnedBaseSeqTxn() {
        return pinnedBaseSeqTxn;
    }

    public long getRepairId() {
        return repairId;
    }

    /**
     * @return {@code L}, the dependency floor the replay reads from
     */
    public long getReplayLowTs() {
        return replayLowTs;
    }

    /**
     * @return the last publication stage the repair recorded, or null when the
     * descriptor holds none
     */
    public RepairPublicationStage getStage() {
        return stage;
    }

    /**
     * @return the base {@code seqTxn} that triggered the repair
     */
    public long getTriggerBaseSeqTxn() {
        return triggerBaseSeqTxn;
    }

    public boolean isOpen() {
        return isOpen;
    }

    /**
     * Reads the descriptor for {@code repairId} into this instance. This is the
     * read side, used by cleanup and by tests; it does not leave the descriptor
     * open for writing.
     *
     * @return true when a complete, checksum-valid descriptor was read
     */
    public boolean load(@Transient @NotNull Path checkpointsDir, long repairId) {
        clear();
        try (
                Path path = new Path();
                MemoryCMR reader = Vm.getCMRInstance()
        ) {
            LiveViewCheckpointLayout.repairDescriptorPath(path, checkpointsDir, repairId);
            final long fileSize = ff.length(path.$());
            if (fileSize < recordSize(0) || fileSize > recordSize(OWNED_SEGMENT_LIMIT)) {
                return false;
            }
            reader.of(
                    ff,
                    path.$(),
                    ff.getPageSize(),
                    fileSize,
                    MemoryTag.MMAP_DEFAULT,
                    CairoConfiguration.O_NONE,
                    -1
            );
            if (reader.getLong(MAGIC_OFFSET) != MAGIC
                    || reader.getInt(FORMAT_VERSION_OFFSET) != FORMAT_VERSION) {
                return false;
            }
            final int ownedSegmentCount = reader.getInt(OWNED_SEGMENT_COUNT_OFFSET);
            if (ownedSegmentCount < 0
                    || ownedSegmentCount > OWNED_SEGMENT_LIMIT
                    || fileSize != recordSize(ownedSegmentCount)) {
                return false;
            }
            final int crcCoverage = (int) (fileSize - Integer.BYTES);
            if (Zip.crc32(0, reader.addressOf(0), crcCoverage) != reader.getInt(crcCoverage)) {
                return false;
            }
            final int storedStage = reader.getInt(STAGE_OFFSET);
            final int storedTag = reader.getInt(HIGH_BOUND_TAG_OFFSET);
            if (storedStage < NO_STAGE
                    || storedStage >= REPAIR_PUBLICATION_STAGES.length
                    || storedTag < 0
                    || storedTag >= HIGH_BOUND_TAGS.length
                    || reader.getLong(REPAIR_ID_OFFSET) != repairId) {
                return false;
            }
            this.repairId = repairId;
            this.stage = storedStage == NO_STAGE ? null : REPAIR_PUBLICATION_STAGES[storedStage];
            this.highBoundTag = HIGH_BOUND_TAGS[storedTag];
            this.definitionTxn = reader.getLong(DEFINITION_TXN_OFFSET);
            this.historyEpoch = reader.getLong(HISTORY_EPOCH_OFFSET);
            this.generation = reader.getLong(GENERATION_OFFSET);
            this.triggerBaseSeqTxn = reader.getLong(TRIGGER_BASE_SEQ_TXN_OFFSET);
            this.pinnedBaseSeqTxn = reader.getLong(PINNED_BASE_SEQ_TXN_OFFSET);
            this.correctionTs = reader.getLong(CORRECTION_TS_OFFSET);
            this.replayLowTs = reader.getLong(REPLAY_LOW_TS_OFFSET);
            this.outputLowTs = reader.getLong(OUTPUT_LOW_TS_OFFSET);
            this.highTsExclusive = reader.getLong(HIGH_TS_EXCLUSIVE_OFFSET);
            this.lastCompletedTimestampGroup = reader.getLong(LAST_COMPLETED_TS_GROUP_OFFSET);
            this.nextCheckpointId = reader.getLong(NEXT_CHECKPOINT_ID_OFFSET);
            for (int i = 0; i < ownedSegmentCount; i++) {
                ownedSegmentIds.add(reader.getLong(HEADER_SIZE + (long) i * Long.BYTES));
            }
            return true;
        }
    }

    /**
     * Records how far the replay got: the last complete timestamp group it
     * reproduced, and the {@code checkpointId} of the next logical boundary it
     * still owes a root version. Best-effort - see the class notes.
     */
    public void recordProgress(long lastCompletedTimestampGroup, long nextCheckpointId) {
        if (!isOpen) {
            return;
        }
        this.lastCompletedTimestampGroup = lastCompletedTimestampGroup;
        this.nextCheckpointId = nextCheckpointId;
        persistQuiet();
    }

    /**
     * Mirrors one publication-stage advance into the descriptor. Best-effort -
     * see the class notes.
     */
    public void recordStage(@NotNull RepairPublicationStage stage) {
        if (!isOpen) {
            return;
        }
        this.stage = stage;
        persistQuiet();
    }

    private static int discardOwnedSegments(
            CairoConfiguration configuration,
            Path checkpointsDir,
            long repairId
    ) {
        final FilesFacade ff = configuration.getFilesFacade();
        int removed = 0;
        try (LiveViewCheckpointRepairState state = new LiveViewCheckpointRepairState(configuration)) {
            if (!state.load(checkpointsDir, repairId)) {
                LOG.error().$("live view checkpoint repair descriptor is unreadable, discarding it [path=")
                        .$(checkpointsDir).$(", repairId=").$(repairId).I$();
                return 0;
            }
            LOG.info().$("discarding a crashed live view checkpoint repair [path=").$(checkpointsDir)
                    .$(", repairId=").$(repairId)
                    .$(", stage=").$(state.stage == null ? "NONE" : state.stage.name())
                    .$(", pinnedBaseSeqTxn=").$(state.pinnedBaseSeqTxn)
                    .$(", correctionTs=").$(state.correctionTs)
                    .$(", replayLowTs=").$(state.replayLowTs)
                    .$(", outputLowTs=").$(state.outputLowTs)
                    .$(", highTsExclusive=").$(state.highTsExclusive)
                    .$(", ownedSegments=").$(state.ownedSegmentIds.size()).I$();
            try (Path path = new Path()) {
                for (int i = 0, n = state.ownedSegmentIds.size(); i < n; i++) {
                    final long segmentId = state.ownedSegmentIds.getQuick(i);
                    LiveViewCheckpointLayout.dataSegmentTmpPath(path, checkpointsDir, segmentId);
                    if (ff.exists(path.$()) && ff.removeQuiet(path.$())) {
                        removed++;
                    }
                    LiveViewCheckpointLayout.metaSegmentTmpPath(path, checkpointsDir, segmentId);
                    if (ff.exists(path.$()) && ff.removeQuiet(path.$())) {
                        removed++;
                    }
                }
            }
        }
        return removed;
    }

    private static void logRemoveFailure(FilesFacade ff, Path path) {
        LOG.error().$("could not remove a live view checkpoint repair descriptor [path=")
                .$(path).$(", errno=").$(ff.errno()).I$();
    }

    private static long parseId(CharSequence name, int lo, int hi) {
        if (lo >= hi) {
            return -1;
        }
        try {
            return Numbers.parseLong(name, lo, hi);
        } catch (NumericException e) {
            return -1;
        }
    }

    private static long recordSize(int ownedSegmentCount) {
        return HEADER_SIZE + (long) ownedSegmentCount * Long.BYTES + Integer.BYTES;
    }

    private void clear() {
        isOpen = false;
        repairId = -1;
        definitionTxn = 0;
        historyEpoch = 0;
        generation = 0;
        pinnedBaseSeqTxn = 0;
        triggerBaseSeqTxn = 0;
        correctionTs = Numbers.LONG_NULL;
        replayLowTs = Numbers.LONG_NULL;
        outputLowTs = Numbers.LONG_NULL;
        highTsExclusive = Numbers.LONG_NULL;
        highBoundTag = HighBoundTag.EOF;
        lastCompletedTimestampGroup = Numbers.LONG_NULL;
        nextCheckpointId = Numbers.LONG_NULL;
        stage = null;
        ownedSegmentIds.clear();
    }

    private void ensureOpen() {
        if (!isOpen) {
            throw CairoException.critical(0).put("live view checkpoint repair descriptor is not open");
        }
    }

    private void ensureRepairDir() {
        try (Path path = new Path()) {
            LiveViewCheckpointLayout.repairDirPath(path, checkpointsDir).slash();
            if (ff.mkdirs(path, mkDirMode) != 0) {
                throw CairoException.critical(ff.errno())
                        .put("could not create live view checkpoint repair directory [path=").put(path).put(']');
            }
        }
    }

    /**
     * Stages the whole record as {@code r.<repairId>.tmp} and renames it over the
     * current descriptor, so a reader sees either the previous record or this one.
     * On Windows the rewrite unlinks the previous record first, so a reader can
     * instead observe no descriptor at all for the length of that window. See
     * {@link LiveViewCheckpointLayout#publishOverwrite}.
     */
    private void persist() {
        ensureOpen();
        final int ownedSegmentCount = ownedSegmentIds.size();
        final int crcCoverage = (int) (recordSize(ownedSegmentCount) - Integer.BYTES);
        LiveViewCheckpointLayout.repairDescriptorTmpPath(tmpPath, checkpointsDir, repairId);
        mem.of(
                ff,
                tmpPath.$(),
                WRITE_EXTEND_SIZE,
                -1,
                MemoryTag.MMAP_DEFAULT,
                CairoConfiguration.O_NONE,
                -1
        );
        try {
            mem.jumpTo(0);
            mem.putLong(MAGIC);
            mem.putInt(FORMAT_VERSION);
            mem.putInt(stage == null ? NO_STAGE : stage.ordinal());
            mem.putLong(repairId);
            mem.putLong(definitionTxn);
            mem.putLong(historyEpoch);
            mem.putLong(generation);
            mem.putLong(triggerBaseSeqTxn);
            mem.putLong(pinnedBaseSeqTxn);
            mem.putLong(correctionTs);
            mem.putLong(replayLowTs);
            mem.putLong(outputLowTs);
            mem.putLong(highTsExclusive);
            mem.putInt(highBoundTag.ordinal());
            mem.putInt(ownedSegmentCount);
            mem.putLong(lastCompletedTimestampGroup);
            mem.putLong(nextCheckpointId);
            assert mem.getAppendOffset() == HEADER_SIZE;
            for (int i = 0; i < ownedSegmentCount; i++) {
                mem.putLong(ownedSegmentIds.getQuick(i));
            }
            // CRC last, covering the whole record ahead of it.
            mem.putInt(Zip.crc32(0, mem.addressOf(0), crcCoverage));
            if (commitMode != CommitMode.NOSYNC) {
                mem.sync(commitMode == CommitMode.ASYNC);
            }
        } finally {
            // Close before rename: on Windows the rename fails with the file open,
            // and on POSIX it avoids a stale mapping to the pre-rename inode.
            mem.close(true, Vm.TRUNCATE_TO_POINTER);
        }
        LiveViewCheckpointLayout.repairDescriptorPath(finalPath, checkpointsDir, repairId);
        // begin() publishes the descriptor and every later update republishes over
        // that same name, so the destination normally already exists.
        final int renameResult = LiveViewCheckpointLayout.publishOverwrite(ff, tmpPath.$(), finalPath.$());
        if (renameResult != Files.FILES_RENAME_OK) {
            throw CairoException.critical(ff.errno())
                    .put("could not rename a live view checkpoint repair descriptor, repairId=")
                    .put(repairId)
                    .put(", renameResult=")
                    .put(renameResult);
        }
    }

    /**
     * {@link #persist()} for the in-flight updates: a failure disables the
     * descriptor rather than unwinding a replay that is otherwise healthy. The
     * stale record it leaves behind is removed by the next {@link #sweep}.
     * <p>
     * On Windows a rewrite that fails after its unlink leaves no record at all
     * rather than a stale one, so that repair's temporary segments outlive the
     * sweep - which reads the descriptor to learn what to reclaim. The generic
     * orphan pass collects them instead, so reclamation is delayed rather than
     * lost.
     */
    private void persistQuiet() {
        try {
            persist();
        } catch (CairoException e) {
            LOG.critical().$("could not update a live view checkpoint repair descriptor, disabling it [path=")
                    .$(checkpointsDir)
                    .$(", repairId=").$(repairId)
                    .$(", error=").$safe(e.getFlyweightMessage()).I$();
            clear();
        }
    }

    /**
     * What one {@link #sweep} found and removed.
     */
    public static final class SweepResult {
        private static final SweepResult EMPTY = new SweepResult(0, 0, 0);
        private final int discardedRepairCount;
        private final int failedCount;
        private final int removedSegmentCount;

        private SweepResult(int discardedRepairCount, int removedSegmentCount, int failedCount) {
            this.discardedRepairCount = discardedRepairCount;
            this.removedSegmentCount = removedSegmentCount;
            this.failedCount = failedCount;
        }

        /**
         * @return crashed repair candidates discarded
         */
        public int getDiscardedRepairCount() {
            return discardedRepairCount;
        }

        /**
         * @return descriptors the sweep could not unlink; the next sweep retries
         */
        public int getFailedCount() {
            return failedCount;
        }

        /**
         * @return temporary segment files removed through a descriptor's own
         * ownership claim
         */
        public int getRemovedSegmentCount() {
            return removedSegmentCount;
        }
    }
}
