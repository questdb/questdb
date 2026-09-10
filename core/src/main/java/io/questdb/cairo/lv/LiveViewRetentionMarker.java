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
 * The durable "rows were removed from the live view's table and its checkpoint
 * timeline does not know yet" marker, {@code _checkpoints/_retention}.
 * <p>
 * Every logical checkpoint root carries the cumulative output position the view had
 * reached at its boundary, counted from row 0 of the table, and restart recovery
 * compares that position against the table's durable row count to decide which root
 * it may restore from. TTL enforcement and {@code DROP PARTITION} shrink the table
 * without touching the stored positions, so between the removal and the timeline
 * publication that accounts for it every root above the removed range overstates the
 * output. A row-count mismatch usually exposes that, but not always: output appended
 * in the same apply, or rows tied at a boundary timestamp, can leave the count equal
 * by coincidence while the root's saved state is missing rows. The marker is what
 * makes the gap detectable without relying on the count.
 * <p>
 * {@link io.questdb.cairo.TableWriter} writes it before the commit that makes the
 * first removal of a live view's partition durable, whichever job drives that apply,
 * so it covers the refresh worker's inline apply and the global apply a
 * refresh-disabled node runs alike. The refresh worker clears it only once the
 * publication that accounts for the removal is durable - the ordinary
 * {@code LiveViewCheckpointTimelineStoreWriter.publishRetention}, or the repair splice
 * that carries a batch of its own when the removal landed inside a replacement's apply
 * - and {@link LiveViewCheckpointLifecycle#retireTimeline} removes it with the timeline
 * it guarded when the worker retires instead. A restart that finds it present rebuilds from the applied base instead of
 * restoring from the timeline. There is no staleness rule: unlike the repair marker,
 * no later generation proves a removal was accounted for, so present means live.
 * <p>
 * The record binds itself to the table id, which is the live view's checkpoint
 * history identity, and to the seqTxn of the transaction whose apply removed the
 * rows, so a reader can tell which local apply the evidence belongs to. It is a single
 * fixed-size, CRC-checked record staged through a {@code .tmp} sibling and renamed into
 * place; {@link #exists} reads the staged sibling as a marker too, so a crash inside
 * the publish still forces the conservative path. OSS snapshots exclude the whole
 * {@code _checkpoints} directory and restore clears it, so the marker is discarded
 * together with the timeline it guards, which is the consistent outcome: a restored
 * view rebuilds its timeline from scratch. This type is stateless; every method is
 * static.
 */
public final class LiveViewRetentionMarker {

    public static final int CRC_OFFSET = 32;
    public static final int FORMAT_VERSION = 1;
    public static final int FORMAT_VERSION_OFFSET = 8;
    public static final int MAGIC_OFFSET = 0;
    /**
     * Magic marking the retention marker file: ASCII {@code "LVRTMK"} with a trailing
     * version nibble.
     */
    public static final long MARKER_MAGIC = 0x4C56_5254_4D4B_0001L;
    public static final int SEQ_TXN_OFFSET = 24;
    public static final int SIZE = CRC_OFFSET + Integer.BYTES; // 36
    public static final int TABLE_ID_OFFSET = 16;
    // The CRC covers everything before it.
    static final int CRC_COVERAGE = CRC_OFFSET;
    static final int RESERVED_OFFSET = 12;

    private LiveViewRetentionMarker() {
    }

    /**
     * Removes the marker and any orphan {@code .tmp}. Best effort: a marker that
     * survives a failed unlink only forces one extra rebuild on the next restart.
     */
    public static void clear(@NotNull FilesFacade ff, @Transient @NotNull Path checkpointsDir) {
        try (Path path = new Path()) {
            LiveViewCheckpointLayout.retentionMarkerPath(path, checkpointsDir);
            ff.removeQuiet(path.$());
            LiveViewCheckpointLayout.retentionMarkerPath(path, checkpointsDir);
            path.put(LiveViewCheckpointLayout.TMP_SUFFIX);
            ff.removeQuiet(path.$());
        }
    }

    /**
     * True when the marker, or its staged {@code .tmp} sibling, is present. A torn
     * or unreadable record counts as present: the marker's only job is to say that
     * something was removed, and a crash inside its own publish cannot have happened
     * before a removal was about to commit.
     */
    public static boolean exists(@NotNull FilesFacade ff, @Transient @NotNull Path checkpointsDir) {
        try (Path path = new Path()) {
            LiveViewCheckpointLayout.retentionMarkerPath(path, checkpointsDir);
            if (ff.exists(path.$())) {
                return true;
            }
            LiveViewCheckpointLayout.retentionMarkerPath(path, checkpointsDir);
            path.put(LiveViewCheckpointLayout.TMP_SUFFIX);
            return ff.exists(path.$());
        }
    }

    /**
     * Reads the seqTxn of the local apply the marker records, or {@link Numbers#LONG_NULL}
     * when the marker is absent, the wrong size, fails its magic/format/CRC checks or
     * belongs to a different table id. Diagnostic: a {@code LONG_NULL} from a marker
     * that {@link #exists} still reports must be treated as a live marker.
     */
    public static long readSeqTxn(@NotNull CairoConfiguration configuration, @Transient @NotNull Path checkpointsDir, long expectedTableId) {
        final FilesFacade ff = configuration.getFilesFacade();
        try (Path path = new Path()) {
            LiveViewCheckpointLayout.retentionMarkerPath(path, checkpointsDir);
            if (!ff.exists(path.$()) || ff.length(path.$()) != SIZE) {
                return Numbers.LONG_NULL;
            }
            final MemoryMARW mem = Vm.getCMARWInstance();
            try {
                mem.of(ff, path.$(), SIZE, -1, MemoryTag.MMAP_DEFAULT, CairoConfiguration.O_NONE, -1);
                if (mem.getLong(MAGIC_OFFSET) != MARKER_MAGIC
                        || mem.getInt(FORMAT_VERSION_OFFSET) != FORMAT_VERSION) {
                    return Numbers.LONG_NULL;
                }
                final int computedCrc = Zip.crc32(0, mem.addressOf(0), CRC_COVERAGE);
                if (computedCrc != mem.getInt(CRC_OFFSET)) {
                    return Numbers.LONG_NULL;
                }
                if (mem.getLong(TABLE_ID_OFFSET) != expectedTableId) {
                    return Numbers.LONG_NULL;
                }
                return mem.getLong(SEQ_TXN_OFFSET);
            } finally {
                mem.close(false);
            }
        }
    }

    /**
     * Durably writes the marker, staged through {@code _retention.tmp} and renamed into
     * place. Must be ordered before the commit that makes the removal durable. Creates
     * the checkpoint directory when it is missing, so a table whose directory lost it
     * still records the evidence rather than failing the removal.
     *
     * @param tableId the live view's table id, its checkpoint history identity
     * @param seqTxn  the seqTxn of the transaction whose apply removes the rows
     */
    public static void write(
            @NotNull CairoConfiguration configuration,
            @Transient @NotNull Path checkpointsDir,
            long tableId,
            long seqTxn
    ) {
        final FilesFacade ff = configuration.getFilesFacade();
        final int commitMode = configuration.getCommitMode();
        try (Path tmpPath = new Path(); Path finalPath = new Path()) {
            tmpPath.of(checkpointsDir).slash();
            if (ff.mkdirs(tmpPath, configuration.getMkDirMode()) != 0) {
                throw CairoException.critical(ff.errno())
                        .put("could not create live view checkpoints directory for the retention marker [path=")
                        .put(tmpPath).put(']');
            }
            LiveViewCheckpointLayout.retentionMarkerPath(finalPath, checkpointsDir);
            LiveViewCheckpointLayout.retentionMarkerPath(tmpPath, checkpointsDir);
            tmpPath.put(LiveViewCheckpointLayout.TMP_SUFFIX);
            final MemoryMARW mem = Vm.getCMARWInstance();
            try {
                mem.of(ff, tmpPath.$(), SIZE, -1, MemoryTag.MMAP_DEFAULT, CairoConfiguration.O_NONE, -1);
                mem.putLong(MAGIC_OFFSET, MARKER_MAGIC);
                mem.putInt(FORMAT_VERSION_OFFSET, FORMAT_VERSION);
                mem.putInt(RESERVED_OFFSET, 0);
                mem.putLong(TABLE_ID_OFFSET, tableId);
                mem.putLong(SEQ_TXN_OFFSET, seqTxn);
                final int crc = Zip.crc32(0, mem.addressOf(0), CRC_COVERAGE);
                mem.putInt(CRC_OFFSET, crc);
                if (commitMode != CommitMode.NOSYNC) {
                    mem.sync(commitMode == CommitMode.ASYNC);
                }
            } finally {
                // Close before rename: Windows rejects a rename over an open file, and
                // POSIX would leave a stale mapping to the old inode.
                mem.close(false);
            }
            // Every removal rewrites the fixed-name marker, so the destination can
            // already exist.
            if (LiveViewCheckpointLayout.publishOverwrite(ff, tmpPath.$(), finalPath.$()) != Files.FILES_RENAME_OK) {
                ff.removeQuiet(tmpPath.$());
                throw CairoException.critical(ff.errno())
                        .put("could not publish live view retention marker");
            }
        }
    }
}
