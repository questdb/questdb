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
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryCMR;
import io.questdb.std.FilesFacade;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.Transient;
import io.questdb.std.Zip;
import io.questdb.std.str.Path;
import org.jetbrains.annotations.NotNull;

import java.io.Closeable;

import static io.questdb.cairo.lv.LiveViewCheckpointLayout.PAGE_CRC_OFFSET;
import static io.questdb.cairo.lv.LiveViewCheckpointLayout.PAGE_HEADER_SIZE;
import static io.questdb.cairo.lv.LiveViewCheckpointLayout.PAGE_KIND_OFFSET;
import static io.questdb.cairo.lv.LiveViewCheckpointLayout.PAGE_LENGTH_OFFSET;
import static io.questdb.cairo.lv.LiveViewCheckpointLayout.SEG_FORMAT_VERSION;
import static io.questdb.cairo.lv.LiveViewCheckpointLayout.SEG_FORMAT_VERSION_OFFSET;
import static io.questdb.cairo.lv.LiveViewCheckpointLayout.SEG_HEADER_CRC_COVERAGE;
import static io.questdb.cairo.lv.LiveViewCheckpointLayout.SEG_HEADER_CRC_OFFSET;
import static io.questdb.cairo.lv.LiveViewCheckpointLayout.SEG_HEADER_SIZE;
import static io.questdb.cairo.lv.LiveViewCheckpointLayout.SEG_ID_OFFSET;
import static io.questdb.cairo.lv.LiveViewCheckpointLayout.SEG_MAGIC;
import static io.questdb.cairo.lv.LiveViewCheckpointLayout.SEG_MAGIC_OFFSET;
import static io.questdb.cairo.lv.LiveViewCheckpointLayout.SEG_PAGE_COUNT_OFFSET;

/**
 * Reads an immutable metadata segment ({@code m.<segmentId>}) written by
 * {@link LiveViewCheckpointMetaSegmentWriter}, validating the self-checksummed
 * header once at open and each page's own CRC32 on access.
 * <p>
 * Page access is bounded: {@link #openPage(LiveViewCheckpointPageRef)} verifies
 * segment identity, file-length-checked offset/length arithmetic, and the
 * reference-vs-header length agreement before the CRC check, so a malformed
 * length can never drive an out-of-bounds read. After a page is opened,
 * {@link #getInt(long)} / {@link #getLong(long)} read fields within the payload
 * with their own bounds checks.
 * <p>
 * Structural failures raise {@link CairoException} with errno
 * {@link CairoException#LV_CHECKPOINT_TIMELINE_INVALID}: timeline state is
 * derived, so a caller treats a bad page as invalidating one root version and
 * schedules its reconstruction rather than failing the view.
 */
public class LiveViewCheckpointMetaSegmentReader implements Closeable {

    private final FilesFacade ff;
    private final MemoryCMR mem;
    private final Path path = new Path();
    private long fileSize;
    private boolean isOpen;
    private int pageCount;
    private int pageKind;
    private long pagePayloadFileOffset;
    private int pagePayloadLength;
    private long pageTotalLength;
    private long segmentId = -1;

    public LiveViewCheckpointMetaSegmentReader(@NotNull CairoConfiguration configuration) {
        this.ff = configuration.getFilesFacade();
        this.mem = Vm.getCMRInstance();
    }

    @Override
    public void close() {
        Misc.free(mem);
        Misc.free(path);
        reset();
    }

    /**
     * @return the file offset one past the last page, i.e. the exclusive walk
     * terminator for sequential iteration from {@link #firstPageOffset()}
     */
    public long endOffset() {
        ensureOpen();
        return fileSize;
    }

    /**
     * @return byte offset of the first page in the segment
     */
    public long firstPageOffset() {
        ensureOpen();
        return SEG_HEADER_SIZE;
    }

    /**
     * Reads one byte at {@code payloadOffset} within the currently open page.
     */
    public byte getByte(long payloadOffset) {
        boundsCheck(payloadOffset, Byte.BYTES);
        return mem.getByte(pagePayloadFileOffset + payloadOffset);
    }

    /**
     * Reads a signed 32-bit field at {@code payloadOffset} within the currently
     * open page, bounds-checked against the page payload length.
     */
    public int getInt(long payloadOffset) {
        boundsCheck(payloadOffset, Integer.BYTES);
        return mem.getInt(pagePayloadFileOffset + payloadOffset);
    }

    /**
     * Reads a signed 64-bit field at {@code payloadOffset} within the currently
     * open page, bounds-checked against the page payload length.
     */
    public long getLong(long payloadOffset) {
        boundsCheck(payloadOffset, Long.BYTES);
        return mem.getLong(pagePayloadFileOffset + payloadOffset);
    }

    /**
     * @return number of pages the segment header declares
     */
    public int getPageCount() {
        ensureOpen();
        return pageCount;
    }

    /**
     * @return the currently open page's kind tag
     */
    public int getPageKind() {
        ensureOpen();
        return pageKind;
    }

    /**
     * @return absolute address of the currently open page's payload
     */
    public long getPagePayloadAddress() {
        ensureOpen();
        return mem.addressOf(pagePayloadFileOffset);
    }

    /**
     * @return the currently open page's payload length in bytes
     */
    public int getPagePayloadLength() {
        ensureOpen();
        return pagePayloadLength;
    }

    /**
     * @return the id this segment self-reports (validated to match the requested id)
     */
    public long getSegmentId() {
        ensureOpen();
        return segmentId;
    }

    /**
     * Opens {@code <checkpointsDir>/meta/m.<segmentId>} for reading and validates
     * the self-checksummed segment header (magic, format version, self-reported
     * id, page count).
     */
    public void of(@Transient @NotNull Path checkpointsDir, long segmentId) {
        if (isOpen) {
            mem.close();
            reset();
        }
        LiveViewCheckpointLayout.metaSegmentPath(path, checkpointsDir, segmentId);
        fileSize = ff.length(path.$());
        if (fileSize < SEG_HEADER_SIZE) {
            throw invalid("metadata segment file too small")
                    .put(", size=").put(fileSize)
                    .put(", segmentId=").put(segmentId);
        }
        mem.of(
                ff,
                path.$(),
                ff.getPageSize(),
                fileSize,
                MemoryTag.MMAP_DEFAULT,
                CairoConfiguration.O_NONE,
                -1
        );
        isOpen = true;
        try {
            validateHeader(segmentId);
        } catch (Throwable t) {
            mem.close();
            reset();
            throw t;
        }
    }

    /**
     * Validates and exposes the page located by {@code ref}. On success the page
     * getters read the payload; the return value is the file offset one past this
     * page, usable to walk to the next page.
     */
    public long openPage(@NotNull LiveViewCheckpointPageRef ref) {
        ensureOpen();
        if (ref.isNull()) {
            throw invalid("null metadata page reference");
        }
        if (ref.getSegmentId() != segmentId) {
            throw invalid("metadata page reference segment mismatch")
                    .put(", expected=").put(segmentId)
                    .put(", actual=").put(ref.getSegmentId());
        }
        return openPageAt(ref.getOffset(), ref.getLength());
    }

    /**
     * Validates and exposes the page at {@code offset}. When
     * {@code expectedTotalLength} is non-negative it must equal the page's
     * header-described total length, cross-checking a reference against the page.
     * Pass {@code -1} when walking pages sequentially without a reference.
     *
     * @return the file offset one past this page
     */
    public long openPageAt(long offset, int expectedTotalLength) {
        ensureOpen();
        if (offset < SEG_HEADER_SIZE || offset > fileSize - PAGE_HEADER_SIZE) {
            throw invalid("metadata page offset out of bounds")
                    .put(", offset=").put(offset)
                    .put(", fileSize=").put(fileSize);
        }
        final int payloadLength = mem.getInt(offset + PAGE_LENGTH_OFFSET);
        if (payloadLength < 0) {
            throw invalid("metadata page payload length negative")
                    .put(", offset=").put(offset)
                    .put(", payloadLength=").put(payloadLength);
        }
        final long total = (long) PAGE_HEADER_SIZE + payloadLength;
        if (offset + total > fileSize) {
            throw invalid("metadata page extends beyond segment")
                    .put(", offset=").put(offset)
                    .put(", total=").put(total)
                    .put(", fileSize=").put(fileSize);
        }
        if (expectedTotalLength >= 0 && expectedTotalLength != total) {
            throw invalid("metadata page length mismatch")
                    .put(", refLength=").put(expectedTotalLength)
                    .put(", headerLength=").put(total);
        }
        // CRC over [length, kind, payload] = [offset + PAGE_LENGTH_OFFSET, offset + total).
        final long crcStart = offset + PAGE_LENGTH_OFFSET;
        final int computedCrc = Zip.crc32(0, mem.addressOf(crcStart), (int) (offset + total - crcStart));
        final int storedCrc = mem.getInt(offset + PAGE_CRC_OFFSET);
        if (computedCrc != storedCrc) {
            throw invalid("metadata page checksum mismatch")
                    .put(", offset=").put(offset)
                    .put(", expected=").put(storedCrc)
                    .put(", computed=").put(computedCrc);
        }
        this.pageKind = mem.getInt(offset + PAGE_KIND_OFFSET);
        this.pagePayloadFileOffset = offset + PAGE_HEADER_SIZE;
        this.pagePayloadLength = payloadLength;
        this.pageTotalLength = total;
        return offset + total;
    }

    private void boundsCheck(long payloadOffset, int size) {
        ensureOpen();
        if (pageTotalLength == 0) {
            throw invalid("no metadata page open");
        }
        if (payloadOffset < 0 || payloadOffset > (long) pagePayloadLength - size) {
            throw invalid("metadata page field read out of bounds")
                    .put(", payloadOffset=").put(payloadOffset)
                    .put(", size=").put(size)
                    .put(", payloadLength=").put(pagePayloadLength);
        }
    }

    private void ensureOpen() {
        if (!isOpen) {
            throw CairoException.critical(0)
                    .put("live view checkpoint metadata segment reader is not open");
        }
    }

    private CairoException invalid(CharSequence reason) {
        return CairoException.critical(CairoException.LV_CHECKPOINT_TIMELINE_INVALID)
                .put("live view checkpoint ").put(reason);
    }

    private void reset() {
        isOpen = false;
        fileSize = 0;
        pageCount = 0;
        pageKind = 0;
        pagePayloadFileOffset = 0;
        pagePayloadLength = 0;
        pageTotalLength = 0;
        segmentId = -1;
    }

    private void validateHeader(long expectedSegmentId) {
        final int magic = mem.getInt(SEG_MAGIC_OFFSET);
        if (magic != SEG_MAGIC) {
            throw invalid("metadata segment magic mismatch")
                    .put(", expected=").put(SEG_MAGIC)
                    .put(", actual=").put(magic);
        }
        // Verify the header CRC before trusting any other header field: a
        // bit-rotted version or page count must read as corruption, not as a
        // real format difference.
        final int computedCrc = Zip.crc32(0, mem.addressOf(0), SEG_HEADER_CRC_COVERAGE);
        final int storedCrc = mem.getInt(SEG_HEADER_CRC_OFFSET);
        if (computedCrc != storedCrc) {
            throw invalid("metadata segment header checksum mismatch")
                    .put(", expected=").put(storedCrc)
                    .put(", computed=").put(computedCrc);
        }
        final int formatVersion = mem.getInt(SEG_FORMAT_VERSION_OFFSET);
        if (formatVersion != SEG_FORMAT_VERSION) {
            throw invalid("metadata segment format version not supported")
                    .put(", version=").put(formatVersion)
                    .put(", supported=").put(SEG_FORMAT_VERSION);
        }
        final long selfId = mem.getLong(SEG_ID_OFFSET);
        if (selfId != expectedSegmentId) {
            throw invalid("metadata segment id mismatch")
                    .put(", expected=").put(expectedSegmentId)
                    .put(", actual=").put(selfId);
        }
        final int pc = mem.getInt(SEG_PAGE_COUNT_OFFSET);
        if (pc < 0) {
            throw invalid("metadata segment page count negative")
                    .put(", pageCount=").put(pc);
        }
        this.segmentId = selfId;
        this.pageCount = pc;
    }
}
