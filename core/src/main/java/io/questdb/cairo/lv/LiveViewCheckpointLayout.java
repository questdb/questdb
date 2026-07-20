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
import io.questdb.std.Transient;
import io.questdb.std.Zip;
import io.questdb.std.str.Path;
import org.jetbrains.annotations.NotNull;

/**
 * Names, paths, and file framing for the versioned checkpoint timeline (Phase 1
 * of {@code LIVE_VIEW_VERSIONED_CHECKPOINT_TIMELINE_DESIGN.md}, section 8.4).
 * <p>
 * All names here are relative to a live view's {@code _checkpoints} directory;
 * this class deliberately does not own the {@code _checkpoints} name itself, so
 * a caller can build that directory path however it likes and pass it in. The
 * layout below is the new timeline surface that eventually replaces the
 * {@code .cp}/{@code _ring} files:
 * <pre>
 *   &lt;live-view-table&gt;/_checkpoints/
 *     _timeline                 fixed A/B superblock (LiveViewCheckpointSuperblock)
 *     meta/
 *       m.&lt;segmentId&gt;           immutable, per-page-checksummed metadata segment
 *       m.&lt;segmentId&gt;.tmp       unpublished metadata segment
 *     data/
 *       d.&lt;segmentId&gt;           immutable checkpoint data bytes, no CRC (Phase 2)
 *       d.&lt;segmentId&gt;.tmp       unpublished data segment
 *     repair/
 *       r.&lt;repairId&gt;            checksummed resumable-repair descriptor (Phase 8)
 * </pre>
 * Metadata segment IDs are monotonic within a history epoch and never reused,
 * even after purge; the zero-padded encoding makes lexical enumeration equal
 * numeric ordering, matching the {@code .cp} filename convention.
 *
 * <h2>Metadata segment framing</h2>
 * A metadata segment ({@code m.<segmentId>}) begins with a fixed, self-checksummed
 * header and then packs immutable pages back to back. Each page carries its own
 * CRC32 so a localized read validates one page without touching the rest of the
 * segment (design section 9.4). Data segments, by contrast, carry no CRC (design
 * section 9.3) and are handled elsewhere.
 */
public final class LiveViewCheckpointLayout {

    /**
     * Subdirectory holding immutable checkpoint data segments (Phase 2).
     */
    public static final String DATA_DIR_NAME = "data";
    /**
     * Filename prefix for a data segment: {@code d.<segmentId>}.
     */
    public static final String DATA_SEGMENT_PREFIX = "d.";
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
     * Subdirectory holding checksummed resumable-repair descriptors (Phase 8).
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
     * Zero-pad width for a segment id in a filename, matching the {@code .cp}
     * convention so lexical enumeration equals numeric ordering.
     */
    public static final int SEGMENT_ID_PAD_LEN = 16;
    /**
     * The fixed A/B superblock filename under {@code _checkpoints}.
     */
    public static final String TIMELINE_FILE_NAME = "_timeline";
    /**
     * Suffix for an unpublished ({@code .tmp}) metadata or data segment.
     */
    public static final String TMP_SUFFIX = ".tmp";

    private LiveViewCheckpointLayout() {
    }

    /**
     * CRC32 (seed zero) over {@code size} bytes at {@code address}. Metadata
     * pages and superblock slots are far below 2 GiB, so a single call suffices.
     */
    public static int checksum(long address, int size) {
        return Zip.crc32(0, address, size);
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
        appendPaddedSegmentId(dst, segmentId);
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
        appendPaddedSegmentId(dst, segmentId);
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
     * Points {@code dst} at {@code <checkpointsDir>/_timeline}.
     */
    public static Path timelinePath(@NotNull Path dst, @Transient @NotNull Path checkpointsDir) {
        return dst.of(checkpointsDir).concat(TIMELINE_FILE_NAME);
    }

    static void appendPaddedSegmentId(@NotNull Path path, long segmentId) {
        if (segmentId < 0) {
            throw CairoException.critical(0)
                    .put("live view checkpoint segment id must be non-negative, was ")
                    .put(segmentId);
        }
        // Manual zero-pad, avoiding a String.format allocation on the write path.
        final int digits = segmentId == 0 ? 1 : (int) Math.floor(Math.log10(segmentId)) + 1;
        for (int i = digits; i < SEGMENT_ID_PAD_LEN; i++) {
            path.put('0');
        }
        path.put(segmentId);
    }
}
