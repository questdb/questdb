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

package io.questdb.cairo;

import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryCMR;
import io.questdb.cairo.vm.api.MemoryMARW;
import io.questdb.std.Files;
import io.questdb.std.FilesFacade;
import io.questdb.std.LongList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.Transient;
import io.questdb.std.Zip;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;
import org.jetbrains.annotations.NotNull;

/**
 * The durable "these partitions were decoded from parquet for a replace-range commit and
 * still owe a re-encode" marker, {@code _parquet_restore} in the table directory.
 * <p>
 * A replace-mode commit cannot rewrite a parquet partition, so
 * {@code TableWriter.dropParquetFormatForReplaceRange} decodes the partitions the range
 * covers back to native and
 * {@code TableWriter.restoreParquetFormatAfterReplaceRange} encodes them again once the
 * replacement is durable. The decode has to commit its own {@code _txn} first, because
 * both it and the O3 replacement name their output directory after the current txn, so
 * the operation spans two commits. A crash between them used to lose the user's
 * compaction for good: the WAL replays the replacement, but the second pass finds the
 * partitions already native and records nothing, so nothing was left to say a re-encode
 * was owed. This marker is that evidence. {@code TableWriter} reads it when it opens the
 * table and finishes the interrupted restore before anything else runs.
 * <p>
 * The record binds itself to the table id, so a marker that travelled with a copied
 * table directory is ignored rather than acted on, and carries the seqTxn of the
 * replace transaction that decoded the partitions for diagnostics. It is a single
 * CRC-checked record staged through a {@code .tmp} sibling and renamed into place, so a
 * torn write never reaches the final name. Unlike
 * {@link io.questdb.cairo.lv.LiveViewRetentionMarker}, a marker that fails its checks is
 * discarded instead of forcing a conservative path: it guards compaction, not
 * correctness, and without a readable partition list there is nothing to act on anyway.
 * This type is stateless; every method is static.
 */
public final class ParquetRestoreMarker {

    public static final int FORMAT_VERSION = 1;
    public static final int FORMAT_VERSION_OFFSET = 8;
    public static final int MAGIC_OFFSET = 0;
    /**
     * Magic marking the parquet restore marker file: ASCII {@code "PQRSTO"} with a
     * trailing version nibble.
     */
    public static final long MARKER_MAGIC = 0x5051_5253_544F_0001L;
    public static final String MARKER_FILE_NAME = "_parquet_restore";
    public static final int PARTITIONS_OFFSET = 32;
    public static final int PARTITION_COUNT_OFFSET = 12;
    public static final int SEQ_TXN_OFFSET = 24;
    public static final int TABLE_ID_OFFSET = 16;
    static final String TMP_SUFFIX = ".tmp";

    private ParquetRestoreMarker() {
    }

    /**
     * Removes the marker and any orphan {@code .tmp}. Best effort: a marker that survives
     * a failed unlink only makes the next writer open re-encode partitions that are
     * already parquet, which every conversion treats as a no-op.
     */
    public static void clear(@NotNull FilesFacade ff, @Transient @NotNull Path tableDir) {
        final int tableDirLen = tableDir.size();
        try {
            ff.removeQuiet(tableDir.concat(MARKER_FILE_NAME).$());
            tableDir.trimTo(tableDirLen);
            ff.removeQuiet(tableDir.concat(MARKER_FILE_NAME).put(TMP_SUFFIX).$());
        } finally {
            tableDir.trimTo(tableDirLen);
        }
    }

    /**
     * Reads the partition timestamps the marker owes a re-encode into {@code out}, and
     * returns the seqTxn of the replace transaction that decoded them. Returns
     * {@link Numbers#LONG_NULL} when the marker is absent, too short for
     * the count it declares, fails its magic/format/CRC checks or belongs to a different
     * table id; {@code out} is then left empty. The caller owns the file either way and
     * is expected to {@link #clear} it once it has acted.
     */
    public static long read(
            @NotNull CairoConfiguration configuration,
            @Transient @NotNull Path tableDir,
            int expectedTableId,
            @Transient @NotNull LongList out
    ) {
        out.clear();
        final FilesFacade ff = configuration.getFilesFacade();
        final int tableDirLen = tableDir.size();
        MemoryCMR mem = null;
        try {
            final LPSZ markerPath = tableDir.concat(MARKER_FILE_NAME).$();
            if (!ff.exists(markerPath)) {
                return Numbers.LONG_NULL;
            }
            final long fileSize = ff.length(markerPath);
            // The declared count has to account for the file exactly: a corrupt length can
            // then never drive an allocation or a read past the mapping.
            if (fileSize < size(0) || (fileSize - size(0)) % Long.BYTES != 0) {
                return Numbers.LONG_NULL;
            }
            final int count = (int) ((fileSize - size(0)) / Long.BYTES);
            mem = Vm.getCMRInstance(ff, markerPath, fileSize, MemoryTag.MMAP_DEFAULT);
            if (mem.getLong(MAGIC_OFFSET) != MARKER_MAGIC
                    || mem.getInt(FORMAT_VERSION_OFFSET) != FORMAT_VERSION
                    || mem.getInt(PARTITION_COUNT_OFFSET) != count) {
                return Numbers.LONG_NULL;
            }
            final int crcOffset = crcOffset(count);
            if (Zip.crc32(0, mem.addressOf(0), crcOffset) != mem.getInt(crcOffset)) {
                return Numbers.LONG_NULL;
            }
            if (mem.getLong(TABLE_ID_OFFSET) != expectedTableId) {
                return Numbers.LONG_NULL;
            }
            for (int i = 0; i < count; i++) {
                out.add(mem.getLong(PARTITIONS_OFFSET + (long) i * Long.BYTES));
            }
            return mem.getLong(SEQ_TXN_OFFSET);
        } finally {
            Misc.free(mem);
            tableDir.trimTo(tableDirLen);
        }
    }

    /**
     * Durably writes the marker, staged through {@code _parquet_restore.tmp} and renamed
     * into place. Must be ordered before the commit that makes the decode durable, since
     * after that commit the partitions are native and nothing else says a re-encode is
     * owed.
     *
     * @param tableId             the table id the marker belongs to
     * @param seqTxn              the seqTxn of the replace transaction that decoded the partitions
     * @param partitionTimestamps logical timestamps of the decoded partitions
     */
    public static void write(
            @NotNull CairoConfiguration configuration,
            @Transient @NotNull Path tableDir,
            int tableId,
            long seqTxn,
            @Transient @NotNull LongList partitionTimestamps
    ) {
        final FilesFacade ff = configuration.getFilesFacade();
        final int commitMode = configuration.getCommitMode();
        final int count = partitionTimestamps.size();
        final int tableDirLen = tableDir.size();
        try (Path tmpPath = new Path()) {
            tmpPath.of(tableDir).concat(MARKER_FILE_NAME).put(TMP_SUFFIX);
            // The record is variable length, so a leftover .tmp from a longer list would keep its
            // tail past what this one writes and the reader would reject the published marker.
            ff.removeQuiet(tmpPath.$());
            final long fileSize = size(count);
            final MemoryMARW mem = Vm.getCMARWInstance();
            try {
                mem.of(ff, tmpPath.$(), fileSize, -1, MemoryTag.MMAP_DEFAULT, CairoConfiguration.O_NONE, -1);
                mem.putLong(MAGIC_OFFSET, MARKER_MAGIC);
                mem.putInt(FORMAT_VERSION_OFFSET, FORMAT_VERSION);
                mem.putInt(PARTITION_COUNT_OFFSET, count);
                mem.putLong(TABLE_ID_OFFSET, tableId);
                mem.putLong(SEQ_TXN_OFFSET, seqTxn);
                for (int i = 0; i < count; i++) {
                    mem.putLong(PARTITIONS_OFFSET + (long) i * Long.BYTES, partitionTimestamps.getQuick(i));
                }
                final int crcOffset = crcOffset(count);
                mem.putInt(crcOffset, Zip.crc32(0, mem.addressOf(0), crcOffset));
                if (commitMode != CommitMode.NOSYNC) {
                    mem.sync(commitMode == CommitMode.ASYNC);
                }
            } finally {
                // Close before rename: Windows rejects a rename over an open file, and POSIX
                // would leave a stale mapping to the old inode.
                mem.close(false);
            }
            final LPSZ markerPath = tableDir.concat(MARKER_FILE_NAME).$();
            // A second replace commit under the same writer rewrites the fixed-name marker,
            // so the destination can already exist.
            if (publishOverwrite(ff, tmpPath.$(), markerPath) != Files.FILES_RENAME_OK) {
                ff.removeQuiet(tmpPath.$());
                throw CairoException.critical(ff.errno()).put("could not publish parquet restore marker [path=").put(markerPath).put(']');
            }
        } finally {
            tableDir.trimTo(tableDirLen);
        }
    }

    private static int crcOffset(int count) {
        return PARTITIONS_OFFSET + count * Long.BYTES;
    }

    /**
     * Renames over an existing destination. Windows reports the destination as existing
     * instead of replacing it, so the retry unlinks first; the errno the caller reports
     * is the one from the rename that actually matters.
     */
    private static int publishOverwrite(@NotNull FilesFacade ff, @NotNull LPSZ tmpPath, @NotNull LPSZ finalPath) {
        final int result = ff.rename(tmpPath, finalPath);
        if (result == Files.FILES_RENAME_OK) {
            return result;
        }
        final int errno = ff.errno();
        if (errno != CairoException.ERRNO_ALREADY_EXISTS_WIN && errno != CairoException.ERRNO_FILE_EXISTS_WIN) {
            return result;
        }
        ff.removeQuiet(finalPath);
        return ff.rename(tmpPath, finalPath);
    }

    private static int size(int count) {
        return crcOffset(count) + Integer.BYTES;
    }
}
