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

import io.questdb.cairo.CairoException;
import io.questdb.std.Files;
import io.questdb.std.FilesFacade;
import io.questdb.std.Transient;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;
import org.jetbrains.annotations.NotNull;

/**
 * Names, paths, and file framing for the versioned checkpoint timeline.
 * <p>
 * Except for {@link #CHECKPOINT_DIR_NAME} itself, all names here are relative to
 * a live view's {@code _checkpoints} directory: a caller builds that directory
 * path however it likes and passes it in. The layout is:
 * <pre>
 *   &lt;live-view-table&gt;/_checkpoints/
 *     _timeline                 fixed A/B superblock (LiveViewCheckpointSuperblock)
 *     _retirements              checksummed zero-reference reclamation work set
 *     _retirements.tmp          copy-on-write queue publication
 *     meta/
 *       m.&lt;segmentId&gt;           immutable, per-page-checksummed metadata segment
 *       m.&lt;segmentId&gt;.tmp       unpublished metadata segment
 *     data/
 *       d.&lt;segmentId&gt;           immutable checkpoint data bytes, no CRC
 *       d.&lt;segmentId&gt;.tmp       unpublished data segment
 *     repair/
 *       r.&lt;repairId&gt;            checksummed resumable-repair descriptor
 * </pre>
 * Metadata segment IDs are monotonic within a history epoch and never reused,
 * even after purge; the zero-padded encoding makes lexical enumeration equal
 * numeric ordering.
 *
 * <h2>Metadata segment framing</h2>
 * A metadata segment ({@code m.<segmentId>}) begins with a fixed,
 * self-checksummed header and then packs immutable pages back to back. Each
 * page carries its own CRC32 so a localized read validates one page without
 * touching the rest of the segment. Data segments, by contrast, carry no CRC
 * and are handled elsewhere.
 */
public final class LiveViewCheckpointLayout {

    /**
     * Directory under a live view's table directory holding all of its
     * checkpoint state.
     */
    public static final String CHECKPOINT_DIR_NAME = "_checkpoints";
    /**
     * Subdirectory holding immutable checkpoint data segments.
     */
    public static final String DATA_DIR_NAME = "data";
    /**
     * Filename prefix for a data segment: {@code d.<segmentId>}.
     */
    public static final String DATA_SEGMENT_PREFIX = "d.";
    /**
     * Zero-pad width for a segment or repair id in a filename, so lexical
     * enumeration equals numeric ordering.
     */
    public static final int ID_PAD_LEN = 16;
    /**
     * Subdirectory holding immutable, per-page-checksummed metadata segments.
     */
    public static final String META_DIR_NAME = "meta";
    /**
     * Filename prefix for a metadata segment: {@code m.<segmentId>}.
     */
    public static final String META_SEGMENT_PREFIX = "m.";
    /**
     * Byte offset of the per-page CRC32 within a page header. The CRC covers the
     * bytes from {@link #PAGE_LENGTH_OFFSET} through the end of the payload, so a
     * corrupted length or kind field is detected too.
     */
    public static final int PAGE_CRC_OFFSET = 0;
    /**
     * Byte offset of the page kind tag within a page header.
     */
    public static final int PAGE_KIND_OFFSET = PAGE_CRC_OFFSET + Integer.BYTES + Integer.BYTES; // 8
    /**
     * Byte offset of the payload length within a page header.
     */
    public static final int PAGE_LENGTH_OFFSET = PAGE_CRC_OFFSET + Integer.BYTES; // 4
    /**
     * Bytes ahead of a metadata page payload: {@code crc32} INT,
     * {@code payloadLength} INT, {@code pageKind} INT.
     */
    public static final int PAGE_HEADER_SIZE = PAGE_KIND_OFFSET + Integer.BYTES; // 12
    /**
     * Top-level marker file naming a prefix-preserving out-of-order repair that
     * is in progress: {@code _checkpoints/_repairing}. Its presence forces a
     * restart to rebuild from the applied base table rather than trust a
     * timeline whose head has been truncated but not yet re-sealed. See
     * {@link LiveViewCheckpointRepairMarker}.
     */
    public static final String REPAIRING_MARKER_FILE_NAME = "_repairing";
    /**
     * CRC-checked copy-on-write work set of zero-reference segments awaiting
     * generation-safe physical removal.
     */
    public static final String RETIREMENT_QUEUE_FILE_NAME = "_retirements";
    /**
     * Filename prefix for a repair descriptor: {@code r.<repairId>}.
     */
    public static final String REPAIR_DESCRIPTOR_PREFIX = "r.";
    /**
     * Subdirectory holding checksummed resumable-repair descriptors.
     */
    public static final String REPAIR_DIR_NAME = "repair";
    /**
     * Byte offset of the self-checksum within a metadata segment header. The CRC
     * covers {@link #SEG_HEADER_CRC_COVERAGE} bytes from offset zero.
     */
    public static final int SEG_HEADER_CRC_OFFSET = 20;
    /**
     * Bytes the metadata segment header CRC32 covers: magic, format version,
     * segment id, and page count.
     */
    public static final int SEG_HEADER_CRC_COVERAGE = SEG_HEADER_CRC_OFFSET; // 20
    /**
     * Metadata segment header size: magic INT, formatVersion INT, segmentId LONG,
     * pageCount INT, headerCrc INT. The first page begins at this offset.
     */
    public static final int SEG_HEADER_SIZE = SEG_HEADER_CRC_OFFSET + Integer.BYTES; // 24
    /**
     * Byte offset of the self-identifying segment id (LONG) within a segment
     * header. A reader cross-checks it against the id it was asked to open.
     */
    public static final int SEG_ID_OFFSET = 8;
    /**
     * Byte offset of the metadata segment magic (INT) within a segment header.
     */
    public static final int SEG_MAGIC_OFFSET = 0;
    /**
     * Byte offset of the format version (INT) within a segment header.
     */
    public static final int SEG_FORMAT_VERSION_OFFSET = 4;
    /**
     * Metadata segment format version. Bump on an incompatible framing change.
     */
    public static final int SEG_FORMAT_VERSION = 1;
    /**
     * Magic marking a metadata segment file: ASCII {@code "LVMS"}.
     */
    public static final int SEG_MAGIC = 0x4C56_4D53;
    /**
     * Byte offset of the page count (INT) within a segment header, patched when
     * the segment is committed.
     */
    public static final int SEG_PAGE_COUNT_OFFSET = 16;
    /**
     * The fixed A/B superblock filename under {@code _checkpoints}.
     */
    public static final String TIMELINE_FILE_NAME = "_timeline";
    /**
     * Suffix for an unpublished ({@code .tmp}) metadata or data segment.
     */
    public static final String TMP_SUFFIX = ".tmp";
    public static final String RETIREMENT_QUEUE_TMP_FILE_NAME = RETIREMENT_QUEUE_FILE_NAME + TMP_SUFFIX;

    private LiveViewCheckpointLayout() {
    }

    /**
     * Points {@code dst} at {@code <checkpointsDir>/data}.
     */
    public static Path dataDirPath(@NotNull Path dst, @Transient @NotNull Path checkpointsDir) {
        return dst.of(checkpointsDir).concat(DATA_DIR_NAME);
    }

    /**
     * Points {@code dst} at {@code <checkpointsDir>/data/d.<segmentId>}.
     */
    public static Path dataSegmentPath(@NotNull Path dst, @Transient @NotNull Path checkpointsDir, long segmentId) {
        dataDirPath(dst, checkpointsDir).slash();
        dst.put(DATA_SEGMENT_PREFIX);
        appendPaddedId(dst, segmentId, "segment");
        return dst;
    }

    /**
     * Points {@code dst} at {@code <checkpointsDir>/data/d.<segmentId>.tmp}.
     */
    public static Path dataSegmentTmpPath(@NotNull Path dst, @Transient @NotNull Path checkpointsDir, long segmentId) {
        dataSegmentPath(dst, checkpointsDir, segmentId);
        dst.put(TMP_SUFFIX);
        return dst;
    }

    /**
     * Points {@code dst} at {@code <checkpointsDir>/meta}.
     */
    public static Path metaDirPath(@NotNull Path dst, @Transient @NotNull Path checkpointsDir) {
        return dst.of(checkpointsDir).concat(META_DIR_NAME);
    }

    /**
     * Points {@code dst} at {@code <checkpointsDir>/meta/m.<segmentId>}.
     */
    public static Path metaSegmentPath(@NotNull Path dst, @Transient @NotNull Path checkpointsDir, long segmentId) {
        metaDirPath(dst, checkpointsDir).slash();
        dst.put(META_SEGMENT_PREFIX);
        appendPaddedId(dst, segmentId, "segment");
        return dst;
    }

    /**
     * Points {@code dst} at {@code <checkpointsDir>/meta/m.<segmentId>.tmp}.
     */
    public static Path metaSegmentTmpPath(@NotNull Path dst, @Transient @NotNull Path checkpointsDir, long segmentId) {
        metaSegmentPath(dst, checkpointsDir, segmentId);
        dst.put(TMP_SUFFIX);
        return dst;
    }

    /**
     * Publishes a staged {@code .tmp} file over a final name that may already
     * hold a previous version of the same record, and returns the
     * {@link Files} rename result.
     * <p>
     * POSIX {@code rename} replaces the destination atomically, so the first
     * attempt succeeds and this never takes a second step. Windows
     * {@code MoveFileW} instead refuses an existing destination, so on a
     * collision errno - and only on one of those - the destination is unlinked
     * and the rename retried once.
     * <p>
     * The retry is not atomic: a Windows reader can observe the final name
     * missing between the unlink and the rename. That state is <em>not</em> the
     * one a crash between staging and publication leaves - a crash there keeps
     * the previous record and adds a {@code .tmp} sibling, whereas this window
     * has no record at all - so a caller that distinguishes "record absent" from
     * "record present" must tolerate the former appearing spuriously on Windows.
     * Restoring atomicity needs a replace-capable native rename
     * ({@code MoveFileExW} with {@code MOVEFILE_REPLACE_EXISTING}); until then
     * this is the closest behaviour that keeps the caller working at all, since
     * today every such re-publication simply fails on Windows.
     * <p>
     * Gating on the errno rather than on the destination existing matters: a
     * rename that failed for any other reason leaves the previously published
     * record in place, exactly as it did before this method existed. Closing the
     * mapping before publishing is the caller's job on both platforms.
     * <p>
     * Only for names a caller deliberately rewrites: the repair marker and the
     * repair descriptor. Published metadata and data segments never reach an
     * existing final name - their ids are allocated past every published one -
     * so those paths rename directly and rely on that invariant rather than on
     * replace semantics.
     */
    public static int publishOverwrite(@NotNull FilesFacade ff, @NotNull LPSZ tmpPath, @NotNull LPSZ finalPath) {
        final int result = ff.rename(tmpPath, finalPath);
        if (result == Files.FILES_RENAME_OK) {
            return result;
        }
        // Read the errno before anything else can overwrite it: the caller builds
        // its exception from ff.errno() once this returns.
        final int errno = ff.errno();
        if (errno != CairoException.ERRNO_ALREADY_EXISTS_WIN && errno != CairoException.ERRNO_FILE_EXISTS_WIN) {
            return result;
        }
        // Retry even when the unlink reports failure: the rename is the operation
        // whose errno the caller reports, and an unlink that could not clear the
        // way simply makes it fail again with the reason that actually matters.
        ff.removeQuiet(finalPath);
        return ff.rename(tmpPath, finalPath);
    }

    /**
     * Points {@code dst} at {@code <checkpointsDir>/repair/r.<repairId>}.
     */
    public static Path repairDescriptorPath(@NotNull Path dst, @Transient @NotNull Path checkpointsDir, long repairId) {
        repairDirPath(dst, checkpointsDir).slash();
        dst.put(REPAIR_DESCRIPTOR_PREFIX);
        appendPaddedId(dst, repairId, "repair");
        return dst;
    }

    /**
     * Points {@code dst} at {@code <checkpointsDir>/repair/r.<repairId>.tmp}.
     */
    public static Path repairDescriptorTmpPath(@NotNull Path dst, @Transient @NotNull Path checkpointsDir, long repairId) {
        repairDescriptorPath(dst, checkpointsDir, repairId);
        dst.put(TMP_SUFFIX);
        return dst;
    }

    /**
     * Points {@code dst} at {@code <checkpointsDir>/repair}.
     */
    public static Path repairDirPath(@NotNull Path dst, @Transient @NotNull Path checkpointsDir) {
        return dst.of(checkpointsDir).concat(REPAIR_DIR_NAME);
    }

    /**
     * Points {@code dst} at {@code <checkpointsDir>/_repairing}.
     */
    public static Path repairingMarkerPath(@NotNull Path dst, @Transient @NotNull Path checkpointsDir) {
        return dst.of(checkpointsDir).concat(REPAIRING_MARKER_FILE_NAME);
    }

    public static Path retirementQueuePath(@NotNull Path dst, @Transient @NotNull Path checkpointsDir) {
        return dst.of(checkpointsDir).concat(RETIREMENT_QUEUE_FILE_NAME);
    }

    /**
     * Points {@code dst} at {@code <checkpointsDir>/_timeline}.
     */
    public static Path timelinePath(@NotNull Path dst, @Transient @NotNull Path checkpointsDir) {
        return dst.of(checkpointsDir).concat(TIMELINE_FILE_NAME);
    }

    static void appendPaddedId(@NotNull Path path, long id, @NotNull CharSequence what) {
        if (id < 0) {
            throw CairoException.critical(0)
                    .put("live view checkpoint ").put(what)
                    .put(" id must be non-negative, was ")
                    .put(id);
        }
        // Manual zero-pad, avoiding a String.format allocation on the write path.
        final int digits = id == 0 ? 1 : (int) Math.floor(Math.log10(id)) + 1;
        for (int i = digits; i < ID_PAD_LEN; i++) {
            path.put('0');
        }
        path.put(id);
    }
}
