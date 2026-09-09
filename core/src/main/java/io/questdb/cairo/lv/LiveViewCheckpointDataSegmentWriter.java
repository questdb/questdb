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
import io.questdb.std.str.Path;
import org.jetbrains.annotations.NotNull;

import java.io.Closeable;

/**
 * Packs encoded checkpoint state pages into one immutable, version-named data
 * segment. The file contains payload bytes only: all framing is held by
 * checksummed {@link LiveViewCheckpointStatePageRef metadata references}, and no
 * production CRC is calculated over data bytes.
 */
public class LiveViewCheckpointDataSegmentWriter implements Closeable {

    private final Path checkpointsDirCopy = new Path();
    private final int commitMode;
    private final FilesFacade ff;
    private final Path finalPath = new Path();
    private final MemoryMARW mem = Vm.getCMARWInstance();
    private final Path tmpPath = new Path();
    private boolean isOpen;
    private long pageOffset = -1;
    private long segmentId = -1;

    public LiveViewCheckpointDataSegmentWriter(@NotNull CairoConfiguration configuration) {
        commitMode = configuration.getCommitMode();
        ff = configuration.getFilesFacade();
    }

    /**
     * Address of a payload range this segment already holds. A repair capture
     * compares one boundary's freshly encoded state against the page a lower
     * boundary of the same capture wrote, and this is the only way to read it:
     * the segment is still a temporary file with no final name and no reader.
     */
    public long addressOfPage(long offset, int length) {
        ensureOpen();
        if (offset < 0 || length <= 0 || offset > mem.getAppendOffset() - length) {
            throw CairoException.critical(0)
                    .put("live view checkpoint data page range outside the open segment")
                    .put(" [offset=").put(offset)
                    .put(", length=").put(length)
                    .put(", appendOffset=").put(mem.getAppendOffset())
                    .put(']');
        }
        return mem.addressOf(offset);
    }

    /**
     * Starts a payload at the current append offset. Calls must be paired with
     * {@link #endPage}.
     */
    public MemoryA beginPage() {
        ensureOpen();
        if (pageOffset != -1) {
            throw CairoException.critical(0)
                    .put("previous live view checkpoint data page must be ended before starting a new one");
        }
        pageOffset = mem.getAppendOffset();
        return mem;
    }

    @Override
    public void close() {
        Misc.free(mem);
        Misc.free(tmpPath);
        Misc.free(finalPath);
        Misc.free(checkpointsDirCopy);
        reset();
    }

    /**
     * Syncs, closes, and renames the non-empty temporary segment. The final name
     * must not already exist: segment ids are monotonic and published files are
     * immutable.
     *
     * @return the exact published file length
     */
    public long commit() {
        ensureOpen();
        if (pageOffset != -1) {
            throw CairoException.critical(0).put("live view checkpoint data page in progress at commit");
        }
        final long fileLength = mem.getAppendOffset();
        if (fileLength <= 0) {
            throw CairoException.critical(0).put("cannot publish empty live view checkpoint data segment");
        }
        if (commitMode != CommitMode.NOSYNC) {
            mem.sync(commitMode == CommitMode.ASYNC);
        }
        mem.close(true, Vm.TRUNCATE_TO_POINTER);

        LiveViewCheckpointLayout.dataSegmentPath(finalPath, checkpointsDirCopy, segmentId);
        if (ff.exists(finalPath.$())) {
            final long duplicateId = segmentId;
            reset();
            throw CairoException.critical(0)
                    .put("live view checkpoint data segment id already published, segmentId=")
                    .put(duplicateId);
        }
        final int renameResult = ff.rename(tmpPath.$(), finalPath.$());
        if (renameResult != Files.FILES_RENAME_OK) {
            final int errno = ff.errno();
            final long failedSegmentId = segmentId;
            reset();
            throw CairoException.critical(errno)
                    .put("could not rename live view checkpoint data segment, segmentId=")
                    .put(failedSegmentId)
                    .put(", renameResult=")
                    .put(renameResult);
        }
        reset();
        return fileLength;
    }

    /**
     * Closes and unlinks the temporary segment without publishing it. A seal
     * whose functions all shared their pages with the previous boundary writes
     * no payload at all, and an empty segment cannot be published: the format
     * has no representation for a zero-length data file, and nothing would
     * reference it.
     */
    public void discard() {
        if (!isOpen) {
            return;
        }
        mem.close(false);
        ff.removeQuiet(tmpPath.$());
        reset();
    }

    /**
     * Finishes the current payload and fills its metadata reference.
     */
    public void endPage(
            @NotNull LiveViewCheckpointStatePageRef out,
            int decodedLength,
            int pageKind,
            int codec,
            int rowCount,
            int flags
    ) {
        ensureOpen();
        if (pageOffset == -1) {
            throw CairoException.critical(0).put("no live view checkpoint data page in progress");
        }
        final long storedLength = mem.getAppendOffset() - pageOffset;
        if (storedLength <= 0 || storedLength > Integer.MAX_VALUE) {
            throw CairoException.critical(0)
                    .put("live view checkpoint data page size out of range, bytes=")
                    .put(storedLength);
        }
        if (decodedLength < 0 || pageKind < 0 || codec < 0 || rowCount < 0) {
            throw CairoException.critical(0)
                    .put("negative live view checkpoint data page metadata")
                    .put(" [decodedLength=").put(decodedLength)
                    .put(", pageKind=").put(pageKind)
                    .put(", codec=").put(codec)
                    .put(", rowCount=").put(rowCount)
                    .put(']');
        }
        out.of(segmentId, pageOffset, (int) storedLength, decodedLength, pageKind, codec, rowCount, flags);
        pageOffset = -1;
    }

    /**
     * @return the id of the open segment, which every page reference this writer
     * mints carries
     */
    public long getSegmentId() {
        ensureOpen();
        return segmentId;
    }

    /**
     * @return true when the open segment holds no payload yet
     */
    public boolean isEmpty() {
        ensureOpen();
        return mem.getAppendOffset() == 0;
    }

    /**
     * Opens {@code data/d.<segmentId>.tmp}. A stale temp file may be overwritten,
     * but an existing final file is rejected so an id can never be reused.
     */
    public void of(@Transient @NotNull Path checkpointsDir, long segmentId) {
        if (isOpen) {
            throw CairoException.critical(0).put("live view checkpoint data segment writer already open");
        }
        if (segmentId < 0) {
            throw CairoException.critical(0)
                    .put("live view checkpoint data segment id must be non-negative, was ")
                    .put(segmentId);
        }
        LiveViewCheckpointLayout.dataSegmentPath(finalPath, checkpointsDir, segmentId);
        if (ff.exists(finalPath.$())) {
            throw CairoException.critical(0)
                    .put("live view checkpoint data segment id already published, segmentId=")
                    .put(segmentId);
        }
        checkpointsDirCopy.of(checkpointsDir);
        LiveViewCheckpointLayout.dataSegmentTmpPath(tmpPath, checkpointsDir, segmentId);
        mem.of(
                ff,
                tmpPath.$(),
                64 * 1024,
                -1,
                MemoryTag.MMAP_DEFAULT,
                CairoConfiguration.O_NONE,
                Files.POSIX_MADV_SEQUENTIAL
        );
        this.segmentId = segmentId;
        this.pageOffset = -1;
        this.isOpen = true;
    }

    private void ensureOpen() {
        if (!isOpen) {
            throw CairoException.critical(0).put("live view checkpoint data segment writer is not open");
        }
    }

    private void reset() {
        isOpen = false;
        pageOffset = -1;
        segmentId = -1;
    }
}
