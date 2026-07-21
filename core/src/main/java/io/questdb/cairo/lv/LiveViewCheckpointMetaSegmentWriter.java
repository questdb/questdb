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
import io.questdb.cairo.vm.api.MemoryA;
import io.questdb.cairo.vm.api.MemoryMARW;
import io.questdb.std.Files;
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
import static io.questdb.cairo.lv.LiveViewCheckpointLayout.SEG_HEADER_CRC_COVERAGE;
import static io.questdb.cairo.lv.LiveViewCheckpointLayout.SEG_HEADER_CRC_OFFSET;
import static io.questdb.cairo.lv.LiveViewCheckpointLayout.SEG_HEADER_SIZE;
import static io.questdb.cairo.lv.LiveViewCheckpointLayout.SEG_MAGIC;
import static io.questdb.cairo.lv.LiveViewCheckpointLayout.SEG_PAGE_COUNT_OFFSET;

/**
 * Writes one immutable metadata segment ({@code m.<segmentId>}) for the
 * versioned checkpoint timeline. A segment packs many small,
 * individually-checksummed metadata pages into a single file, so the store
 * avoids one file per partition/tree-node while still validating each page in
 * isolation on read.
 * <p>
 * The writer stages into {@code m.<segmentId>.tmp}, syncs per
 * {@code cairo.commit.mode}, and atomically renames to the final
 * {@code m.<segmentId>} name at {@link #commit()}. Nothing references a segment
 * until that rename completes, so a crash mid-write leaves only an orphan
 * {@code .tmp} for startup to remove. A published segment is never modified.
 * <p>
 * Usage:
 * <pre>
 *   writer.of(checkpointsDir, segmentId);
 *   MemoryA payload = writer.beginPage(pageKind);
 *   payload.putLong(...); payload.putInt(...);   // page body
 *   writer.endPage(ref);                          // ref now locates the page
 *   ... more pages ...
 *   long bytes = writer.commit();                 // publish m.<segmentId>
 * </pre>
 * The instance is reusable across segments; {@link #close()} releases the
 * mapping and paths.
 */
public class LiveViewCheckpointMetaSegmentWriter implements Closeable {

    private final Path checkpointsDirCopy = new Path();
    private final int commitMode;
    private final long extendSize;
    private final FilesFacade ff;
    private final Path finalPath = new Path();
    private final MemoryMARW mem;
    private final Path tmpPath = new Path();
    private boolean isOpen;
    private int pageCount;
    private long pageHeaderOffset = -1;
    private int pendingPageKind;
    private long segmentId = -1;

    public LiveViewCheckpointMetaSegmentWriter(@NotNull CairoConfiguration configuration) {
        this.commitMode = configuration.getCommitMode();
        this.ff = configuration.getFilesFacade();
        // 64 KiB is ample for a segment of small tree/root pages; the mapping
        // grows on demand if a single segment packs more.
        this.extendSize = 64 * 1024;
        this.mem = Vm.getCMARWInstance();
    }

    /**
     * Reserves a page header at the current append offset and returns the sink
     * the caller writes the page body into. Must be paired with
     * {@link #endPage(LiveViewCheckpointPageRef)}.
     */
    public MemoryA beginPage(int pageKind) {
        ensureOpen();
        if (pageHeaderOffset != -1) {
            throw CairoException.critical(0)
                    .put("previous live view checkpoint metadata page must be ended before starting a new one");
        }
        pageHeaderOffset = mem.getAppendOffset();
        pendingPageKind = pageKind;
        // Reserve the header; the caller appends the payload after it, and
        // endPage() patches length, kind, and CRC back into these bytes.
        mem.jumpTo(pageHeaderOffset + PAGE_HEADER_SIZE);
        return mem;
    }

    @Override
    public void close() {
        Misc.free(mem);
        Misc.free(tmpPath);
        Misc.free(finalPath);
        Misc.free(checkpointsDirCopy);
        isOpen = false;
        pageHeaderOffset = -1;
        pageCount = 0;
        segmentId = -1;
    }

    /**
     * Finalizes the segment: patches the page count, self-checksums the header,
     * syncs per {@code cairo.commit.mode}, closes the mapping (truncating to the
     * exact written size), and renames {@code m.<segmentId>.tmp} to
     * {@code m.<segmentId>}.
     *
     * @return total bytes written to the segment file
     */
    public long commit() {
        ensureOpen();
        if (pageHeaderOffset != -1) {
            throw CairoException.critical(0)
                    .put("live view checkpoint metadata page in progress at commit");
        }
        // Patch the page count, then self-checksum the header over [0, coverage).
        mem.putInt(SEG_PAGE_COUNT_OFFSET, pageCount);
        final int headerCrc = Zip.crc32(0, mem.addressOf(0), SEG_HEADER_CRC_COVERAGE);
        mem.putInt(SEG_HEADER_CRC_OFFSET, headerCrc);

        final long total = mem.getAppendOffset();
        if (commitMode != CommitMode.NOSYNC) {
            mem.sync(commitMode == CommitMode.ASYNC);
        }
        // Close before rename: on Windows the rename fails with the file open,
        // and on POSIX it avoids a stale mapping to the pre-rename inode.
        mem.close(true, Vm.TRUNCATE_TO_POINTER);

        LiveViewCheckpointLayout.metaSegmentPath(finalPath, checkpointsDirCopy, segmentId);
        final int renameResult = ff.rename(tmpPath.$(), finalPath.$());
        if (renameResult != Files.FILES_RENAME_OK) {
            final int errno = ff.errno();
            final long failedSegmentId = segmentId;
            isOpen = false;
            pageHeaderOffset = -1;
            pageCount = 0;
            segmentId = -1;
            throw CairoException.critical(errno)
                    .put("could not rename live view checkpoint metadata segment, segmentId=")
                    .put(failedSegmentId)
                    .put(", renameResult=")
                    .put(renameResult);
        }

        isOpen = false;
        pageHeaderOffset = -1;
        pageCount = 0;
        segmentId = -1;
        return total;
    }

    /**
     * Closes the in-flight page: computes its payload length, patches the header
     * (length, kind, CRC32), and fills {@code out} with a reference locating the
     * page. The CRC covers the length and kind fields plus the payload, so a
     * corrupted framing field is detected on read.
     */
    public void endPage(@NotNull LiveViewCheckpointPageRef out) {
        ensureOpen();
        if (pageHeaderOffset == -1) {
            throw CairoException.critical(0)
                    .put("no live view checkpoint metadata page in progress");
        }
        final long payloadEnd = mem.getAppendOffset();
        final long payloadLength = payloadEnd - pageHeaderOffset - PAGE_HEADER_SIZE;
        if (payloadLength < 0 || payloadLength > Integer.MAX_VALUE - PAGE_HEADER_SIZE) {
            throw CairoException.critical(0)
                    .put("live view checkpoint metadata page size out of range, bytes=")
                    .put(payloadLength);
        }
        mem.putInt(pageHeaderOffset + PAGE_LENGTH_OFFSET, (int) payloadLength);
        mem.putInt(pageHeaderOffset + PAGE_KIND_OFFSET, pendingPageKind);
        // CRC over [length, kind, payload] = [pageHeaderOffset + 4, payloadEnd).
        final long crcStart = pageHeaderOffset + PAGE_LENGTH_OFFSET;
        final int crc = Zip.crc32(0, mem.addressOf(crcStart), (int) (payloadEnd - crcStart));
        mem.putInt(pageHeaderOffset + PAGE_CRC_OFFSET, crc);

        out.of(segmentId, pageHeaderOffset, (int) (PAGE_HEADER_SIZE + payloadLength));
        pageCount++;
        pageHeaderOffset = -1;
    }

    /**
     * Opens {@code <checkpointsDir>/meta/m.<segmentId>.tmp} for writing and lays
     * down the segment header (with a placeholder page count patched at
     * {@link #commit()}). A pre-existing {@code .tmp} at the same path is a crash
     * orphan and is overwritten.
     */
    public void of(@Transient @NotNull Path checkpointsDir, long segmentId) {
        if (isOpen) {
            throw CairoException.critical(0)
                    .put("live view checkpoint metadata segment writer already open");
        }
        if (segmentId < 0) {
            throw CairoException.critical(0)
                    .put("live view checkpoint metadata segment id must be non-negative, was ")
                    .put(segmentId);
        }
        checkpointsDirCopy.of(checkpointsDir);
        LiveViewCheckpointLayout.metaSegmentTmpPath(tmpPath, checkpointsDir, segmentId);
        mem.of(
                ff,
                tmpPath.$(),
                extendSize,
                -1,
                MemoryTag.MMAP_DEFAULT,
                CairoConfiguration.O_NONE,
                Files.POSIX_MADV_SEQUENTIAL
        );
        this.segmentId = segmentId;
        this.pageCount = 0;
        this.pageHeaderOffset = -1;
        this.isOpen = true;

        // Header: magic, formatVersion, segmentId, pageCount placeholder,
        // headerCrc placeholder. Both placeholders are patched at commit().
        mem.putInt(SEG_MAGIC);
        mem.putInt(SEG_FORMAT_VERSION);
        mem.putLong(segmentId);
        mem.putInt(0); // pageCount placeholder
        mem.putInt(0); // headerCrc placeholder
        assert mem.getAppendOffset() == SEG_HEADER_SIZE;
    }

    private void ensureOpen() {
        if (!isOpen) {
            throw CairoException.critical(0)
                    .put("live view checkpoint metadata segment writer is not open");
        }
    }
}
