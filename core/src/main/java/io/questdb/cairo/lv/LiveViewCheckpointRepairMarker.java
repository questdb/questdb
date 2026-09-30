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
import io.questdb.cairo.CommitMode;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryMARW;
import io.questdb.std.Files;
import io.questdb.std.FilesFacade;
import io.questdb.std.MemoryTag;
import io.questdb.std.Numbers;
import io.questdb.std.Transient;
import io.questdb.std.Zip;
import io.questdb.std.str.Path;
import org.jetbrains.annotations.NotNull;

/**
 * The durable "prefix-preserving out-of-order repair in progress" marker,
 * {@code _checkpoints/_repairing}. It closes the crash-safety gap a
 * prefix-preserving repair opens: such a repair truncates the timeline down to
 * its surviving prefix (making an old prefix root the head) and only re-seals a
 * fresh head after the replay. Between the truncate and the re-seal the
 * superblock's watermark still names the discarded higher head, so an
 * incremental restore of the truncated head would replay base WAL from the
 * wrong coordinate and rehydrate silently wrong state.
 * <p>
 * The marker forecloses that: it is written durably <em>before</em> the
 * truncate and cleared only <em>after</em> the post-replay seal. A restart that
 * finds it present treats the timeline as if retired and rebuilds from the
 * fully durable applied base table (see {@code LiveViewRefreshJob}'s restore
 * path), which is always correct.
 * <p>
 * A recorded {@link #readBaseGeneration(CairoConfiguration, Path) base
 * generation} - the timeline generation the repair started from - lets a
 * restart distinguish a live repair from a stale marker that a crash left
 * behind after a successful seal: the truncate publishes
 * {@code baseGeneration + 1} and the seal publishes
 * {@code baseGeneration + 2}, so a superblock generation strictly greater than
 * {@code baseGeneration + 1} proves the repair completed and the marker is safe
 * to ignore. A torn or unreadable marker (only reachable if the marker write
 * itself crashed, before any truncate) reads as {@link Numbers#LONG_NULL} and
 * forces the conservative rebuild.
 * <p>
 * A recorded {@link #readLvSeqTxn(CairoConfiguration, Path) live view seqTxn} -
 * the last transaction the view's own WAL had committed when the marker was
 * written - covers the other end of the window. A repair writes the marker
 * immediately before the commit that replaces its output, and a truncate or a
 * splice publishes a generation past {@code baseGeneration}. So a superblock
 * still at {@code baseGeneration}, a view sequencer that still ends at the
 * recorded seqTxn and a view table that has applied nothing past it prove the
 * repair moved nothing durable: the timeline still describes the output on
 * disk, and the marker is stale. The table's own applied seqTxn is part of the
 * proof because, under the NOSYNC and ASYNC commit modes, an OS crash can drop
 * the sequencer's record of a replacement the table kept. {@link Numbers#LONG_NULL}
 * means the seqTxn was not recorded, and the generation rule decides alone.
 * <p>
 * Two layouts exist, and every field both carry sits at the same offset.
 * Format version 2 ({@link #SIZE} bytes) carries the seqTxn at
 * {@link #LV_SEQ_TXN_OFFSET} and its CRC after it. Version 1
 * ({@link #V1_SIZE} bytes), which builds before it wrote (10.0.x among them),
 * ends at the floor timestamp and has its CRC at {@link #V1_CRC_OFFSET}. This
 * build writes version 2 and reads both; a version 1 record reports no seqTxn.
 * A build that reads only version 1 rejects a version 2 record on its size and
 * reads it as torn, which forces the conservative rebuild. 10.0.x never meets
 * one: its startup reconciliation removes a checkpoint directory in this
 * build's timeline format before anything reads the marker.
 * <p>
 * The file is a single fixed-size, CRC-checked record staged through a
 * {@code .tmp} sibling and renamed into place. A crash ahead of the rename that
 * rewrites an existing marker keeps the previous record under the final name
 * beside the staged one, and the final name decides. With no previous record,
 * the crash leaves the record under the staged name alone: complete when the
 * crash came after the staged write, torn when it came during it. The staged
 * write maps an existing {@code .tmp} without truncating it, though: when it
 * runs over a complete leftover record and the crash comes before it has
 * changed any of that record's bytes, the older record survives intact and
 * CRC-valid, and that record decides. Either older record decides soundly: the
 * marker write precedes the interrupted repair's truncate and commit, so that
 * repair moved nothing durable and the older record's staleness verdict still
 * holds. Rewriting an existing marker is atomic on POSIX; on Windows it
 * briefly unlinks the previous record first, so a crash inside that window
 * leaves the staged record alone after all - the older record gone, the
 * complete new one staged - see
 * {@link LiveViewCheckpointLayout#publishOverwrite}. {@link #exists} counts the
 * staged record as a marker, and the reads fall back to it when no final name
 * exists, so a restart decides on a complete staged record exactly as it would
 * once the rename had published it. A torn one reads as
 * {@link Numbers#LONG_NULL} and forces the conservative rebuild. This type is
 * stateless; every method is static.
 */
public final class LiveViewCheckpointRepairMarker {

    public static final int BASE_GENERATION_OFFSET = 32;
    public static final int CRC_OFFSET = 56;
    public static final int DEFINITION_TXN_OFFSET = 16;
    public static final int FLOOR_TIMESTAMP_OFFSET = 40;
    public static final int FORMAT_VERSION = 2;
    public static final int FORMAT_VERSION_OFFSET = 8;
    public static final int HISTORY_EPOCH_OFFSET = 24;
    public static final int LV_SEQ_TXN_OFFSET = 48;
    public static final int MAGIC_OFFSET = 0;
    /**
     * Magic marking the repair marker file: ASCII {@code "LVRPMK"} with a
     * trailing version nibble. Both layouts carry it; the format version
     * field tells them apart.
     */
    public static final long MARKER_MAGIC = 0x4C56_5250_4D4B_0001L;
    public static final int SIZE = CRC_OFFSET + Integer.BYTES; // 60
    // The version 1 layout: the same fields up to the floor timestamp, then the CRC.
    public static final int V1_CRC_OFFSET = LV_SEQ_TXN_OFFSET;
    public static final int V1_FORMAT_VERSION = 1;
    public static final int V1_SIZE = V1_CRC_OFFSET + Integer.BYTES; // 52
    static final int RESERVED_OFFSET = 12;

    private LiveViewCheckpointRepairMarker() {
    }

    /**
     * Removes the marker and its staged {@code .tmp} sibling, in that order. Best
     * effort: a record a failed unlink leaves behind reads as stale once the
     * superblock generation runs past {@code baseGeneration + 1}, or, while it
     * still equals {@code baseGeneration}, under the seqTxn rule of
     * {@code LiveViewRefreshJob.isRepairMarkerLive}: the record carries a seqTxn,
     * the view's sequencer still ends at it, and the view's table has applied
     * nothing past it. Otherwise it sends a restart to the conservative rebuild,
     * which the restatement guard may refuse. A crash between the two unlinks
     * leaves the staged record alone, which the reads then fall back to.
     */
    public static void clear(@NotNull FilesFacade ff, @Transient @NotNull Path checkpointsDir) {
        try (Path path = new Path()) {
            LiveViewCheckpointLayout.repairingMarkerPath(path, checkpointsDir);
            ff.removeQuiet(path.$());
            path.put(LiveViewCheckpointLayout.TMP_SUFFIX);
            ff.removeQuiet(path.$());
        }
    }

    /**
     * A staged {@code .tmp} with no final name counts as present. Two crashes
     * leave a complete record in that shape: one between the staged write and
     * its rename, and one inside the unlink the Windows rewrite needs (see
     * {@link LiveViewCheckpointLayout#publishOverwrite}), where the previous
     * record is already gone and the replacement not yet in place. The disk
     * cannot tell them apart, and the decision does not need it to: either way
     * the staged record is the only one left, and it is the record the rename
     * would have published - the one a POSIX rewrite puts in the previous
     * record's place atomically. {@link #readBaseGeneration} and
     * {@link #readLvSeqTxn} therefore read it when no final name exists, and the
     * restart applies to it the staleness rule it applies to a published record.
     * <p>
     * A crash during the staged write itself leaves a torn staged record, unless
     * the write ran over a complete leftover record whose bytes it had not yet
     * changed, which then survives intact and decides (see the class doc). A
     * torn record reports {@link Numbers#LONG_NULL} for both fields. With no
     * generation to test nothing proves the timeline unmoved, so the restart
     * reads it as live and rebuilds. That rebuild retires the timeline, which
     * removes both names, but the restatement guard may refuse it, and a refused
     * rebuild leaves the torn record in place for the next restart to meet again.
     * A final record that fails its checks reads as torn too; the reads never
     * fall back from it to a staged one, since the final name is the record that
     * took effect.
     *
     * @return true when the marker file or its staged sibling is present,
     * regardless of whether the contents validate
     */
    public static boolean exists(@NotNull FilesFacade ff, @Transient @NotNull Path checkpointsDir) {
        try (Path path = new Path()) {
            LiveViewCheckpointLayout.repairingMarkerPath(path, checkpointsDir);
            if (ff.exists(path.$())) {
                return true;
            }
            path.put(LiveViewCheckpointLayout.TMP_SUFFIX);
            return ff.exists(path.$());
        }
    }

    /**
     * Reads the base generation the in-progress repair started from, or
     * {@link Numbers#LONG_NULL} when the marker is absent, has a size neither
     * layout has, or fails its magic/format/CRC checks. The record comes from the
     * final name, or from the staged {@code .tmp} when no final name exists (see
     * {@link #exists}). A {@code LONG_NULL} result must be treated as a live repair
     * (force a rebuild): a torn marker is only reachable before any truncate, so a
     * rebuild is always safe.
     */
    public static long readBaseGeneration(@NotNull CairoConfiguration configuration, @Transient @NotNull Path checkpointsDir) {
        return readField(configuration, checkpointsDir, BASE_GENERATION_OFFSET);
    }

    /**
     * Reads the live view seqTxn the marker recorded, or {@link Numbers#LONG_NULL}
     * when the marker is absent, fails the checks {@link #readBaseGeneration}
     * applies, or is a version 1 record, which carries none. A {@code LONG_NULL}
     * result proves nothing about the replacement commit, so it must never read
     * as stale.
     */
    public static long readLvSeqTxn(@NotNull CairoConfiguration configuration, @Transient @NotNull Path checkpointsDir) {
        return readField(configuration, checkpointsDir, LV_SEQ_TXN_OFFSET);
    }

    /**
     * Durably writes the marker, staged through {@code _repairing.tmp} and
     * renamed into place. Must be ordered before the repair's truncate
     * publication and its replacement commit.
     *
     * @param baseGeneration the timeline generation the repair started from
     * @param floorTimestamp the truncate floor {@code R} (diagnostic)
     * @param lvSeqTxn       the last seqTxn the live view's own WAL has committed;
     *                       the repair's replacement must be the next commit
     */
    public static void write(
            @NotNull CairoConfiguration configuration,
            @Transient @NotNull Path checkpointsDir,
            long definitionTxn,
            long historyEpoch,
            long baseGeneration,
            long floorTimestamp,
            long lvSeqTxn
    ) {
        final FilesFacade ff = configuration.getFilesFacade();
        final int commitMode = configuration.getCommitMode();
        try (Path tmpPath = new Path(); Path finalPath = new Path()) {
            LiveViewCheckpointLayout.repairingMarkerPath(finalPath, checkpointsDir);
            LiveViewCheckpointLayout.repairingMarkerPath(tmpPath, checkpointsDir);
            tmpPath.put(LiveViewCheckpointLayout.TMP_SUFFIX);
            final MemoryMARW mem = Vm.getCMARWInstance();
            try {
                mem.of(ff, tmpPath.$(), SIZE, -1, MemoryTag.MMAP_DEFAULT, CairoConfiguration.O_NONE, -1);
                mem.putLong(MAGIC_OFFSET, MARKER_MAGIC);
                mem.putInt(FORMAT_VERSION_OFFSET, FORMAT_VERSION);
                mem.putInt(RESERVED_OFFSET, 0);
                mem.putLong(DEFINITION_TXN_OFFSET, definitionTxn);
                mem.putLong(HISTORY_EPOCH_OFFSET, historyEpoch);
                mem.putLong(BASE_GENERATION_OFFSET, baseGeneration);
                mem.putLong(FLOOR_TIMESTAMP_OFFSET, floorTimestamp);
                mem.putLong(LV_SEQ_TXN_OFFSET, lvSeqTxn);
                // The CRC covers everything before it.
                final int crc = Zip.crc32(0, mem.addressOf(0), CRC_OFFSET);
                mem.putInt(CRC_OFFSET, crc);
                if (commitMode != CommitMode.NOSYNC) {
                    mem.sync(commitMode == CommitMode.ASYNC);
                }
            } finally {
                // Close before rename: Windows rejects a rename over an open
                // file, and POSIX would leave a stale mapping to the old inode.
                mem.close(false);
            }
            // A second repair rewrites the fixed-name marker, so the destination
            // can already exist.
            if (LiveViewCheckpointLayout.publishOverwrite(ff, tmpPath.$(), finalPath.$()) != Files.FILES_RENAME_OK) {
                ff.removeQuiet(tmpPath.$());
                throw CairoException.critical(ff.errno())
                        .put("could not publish live view checkpoint repair marker");
            }
        }
    }

    /**
     * Reads the long at {@code fieldOffset} out of a marker that passes its size, magic,
     * format and CRC checks, or {@link Numbers#LONG_NULL}. The record is the final name's
     * when that exists, and the staged {@code .tmp}'s otherwise. The size picks the layout,
     * and the format version must agree with it. A field a version 1 record does not carry
     * reads as {@code LONG_NULL}.
     */
    private static long readField(
            @NotNull CairoConfiguration configuration,
            @Transient @NotNull Path checkpointsDir,
            int fieldOffset
    ) {
        final FilesFacade ff = configuration.getFilesFacade();
        try (Path path = new Path()) {
            LiveViewCheckpointLayout.repairingMarkerPath(path, checkpointsDir);
            if (!ff.exists(path.$())) {
                // No published record, so the staged one is the only record on disk: the one
                // the rename would have published. A final record that fails its checks
                // below does not fall back here - it is the record that took effect.
                path.put(LiveViewCheckpointLayout.TMP_SUFFIX);
                if (!ff.exists(path.$())) {
                    return Numbers.LONG_NULL;
                }
            }
            final long size = ff.length(path.$());
            final int crcOffset;
            final int formatVersion;
            if (size == SIZE) {
                crcOffset = CRC_OFFSET;
                formatVersion = FORMAT_VERSION;
            } else if (size == V1_SIZE) {
                if (fieldOffset >= V1_CRC_OFFSET) {
                    return Numbers.LONG_NULL;
                }
                crcOffset = V1_CRC_OFFSET;
                formatVersion = V1_FORMAT_VERSION;
            } else {
                return Numbers.LONG_NULL;
            }
            final MemoryMARW mem = Vm.getCMARWInstance();
            try {
                mem.of(ff, path.$(), size, -1, MemoryTag.MMAP_DEFAULT, CairoConfiguration.O_NONE, -1);
                if (mem.getLong(MAGIC_OFFSET) != MARKER_MAGIC
                        || mem.getInt(FORMAT_VERSION_OFFSET) != formatVersion) {
                    return Numbers.LONG_NULL;
                }
                // The CRC covers everything before it.
                final int computedCrc = Zip.crc32(0, mem.addressOf(0), crcOffset);
                if (computedCrc != mem.getInt(crcOffset)) {
                    return Numbers.LONG_NULL;
                }
                return mem.getLong(fieldOffset);
            } finally {
                mem.close(false);
            }
        }
    }
}
