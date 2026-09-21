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
import io.questdb.std.str.Path;
import org.jetbrains.annotations.NotNull;

import java.io.Closeable;

/**
 * Bounded reader for one immutable checkpoint data segment. Opening cross-checks
 * the physical file against the exact length stored in the checksummed segment
 * directory. Opening a page validates every framing field in its checksummed
 * metadata reference before exposing any payload address.
 */
public class LiveViewCheckpointDataSegmentReader implements Closeable {

    private final FilesFacade ff;
    private final MemoryCMR mem = Vm.getCMRInstance();
    private final Path path = new Path();
    private int currentRefDecodedLength;
    private int currentRefRowCount;
    private long fileLength;
    private boolean isOpen;
    private long pageOffset = -1;
    private int pageStoredLength;
    private long segmentId = -1;

    public LiveViewCheckpointDataSegmentReader(@NotNull CairoConfiguration configuration) {
        ff = configuration.getFilesFacade();
    }

    /**
     * Verifies that a decoder consumed exactly the bounded encoded payload and
     * produced exactly the decoded length and row count promised by metadata.
     */
    public void assertFullyConsumed(long storedBytesConsumed, long decodedBytesProduced, long rowsDecoded) {
        ensurePageOpen();
        if (storedBytesConsumed != pageStoredLength
                || decodedBytesProduced != currentRefDecodedLength
                || rowsDecoded != currentRefRowCount) {
            throw invalid("data page decoder did not consume reference exactly")
                    .put(" [stored=").put(storedBytesConsumed).put('/').put(pageStoredLength)
                    .put(", decoded=").put(decodedBytesProduced).put('/').put(currentRefDecodedLength)
                    .put(", rows=").put(rowsDecoded).put('/').put(currentRefRowCount)
                    .put(']');
        }
    }

    @Override
    public void close() {
        Misc.free(mem);
        Misc.free(path);
        reset();
    }

    public byte getByte(long storedOffset) {
        boundsCheck(storedOffset, Byte.BYTES);
        return mem.getByte(pageOffset + storedOffset);
    }

    public long getFileLength() {
        ensureOpen();
        return fileLength;
    }

    public int getInt(long storedOffset) {
        boundsCheck(storedOffset, Integer.BYTES);
        return mem.getInt(pageOffset + storedOffset);
    }

    public long getLong(long storedOffset) {
        boundsCheck(storedOffset, Long.BYTES);
        return mem.getLong(pageOffset + storedOffset);
    }

    public long getPageAddress() {
        ensurePageOpen();
        return mem.addressOf(pageOffset);
    }

    public int getPageStoredLength() {
        ensurePageOpen();
        return pageStoredLength;
    }

    public void openStatePageReader(@NotNull LiveViewStatePageReader out) {
        ensurePageOpen();
        out.of(mem, pageOffset, pageStoredLength);
    }

    /**
     * Maps a published segment only when its exact physical length agrees with
     * the checksummed directory entry.
     */
    public void of(@Transient @NotNull Path checkpointsDir, long segmentId, long expectedFileLength) {
        if (isOpen) {
            mem.close();
            reset();
        }
        if (segmentId < 0 || expectedFileLength <= 0) {
            throw invalid("data segment identity or length invalid")
                    .put(" [segmentId=").put(segmentId)
                    .put(", expectedFileLength=").put(expectedFileLength)
                    .put(']');
        }
        LiveViewCheckpointLayout.dataSegmentPath(path, checkpointsDir, segmentId);
        final long actualLength = ff.length(path.$());
        // A negative length means the stat failed. A MISSING segment is genuine structural
        // invalidity - the root references a segment that is not there - so it falls through to
        // the mismatch branch below and keeps raising the errno the restore fallback keys on. Any
        // other failure (EACCES, EIO, a transient NFS blip) is an IO error, not corruption:
        // condemning the root on one blip would make restore skip an intact root.
        if (actualLength < 0) {
            final int errno = ff.errno();
            if (errno != CairoException.ERRNO_FILE_DOES_NOT_EXIST && errno != CairoException.ERRNO_FILE_DOES_NOT_EXIST_WIN) {
                throw CairoException.critical(errno)
                        .put("could not read live view checkpoint data segment length [segmentId=")
                        .put(segmentId)
                        .put(", path=").put(path)
                        .put(']');
            }
        }
        if (actualLength != expectedFileLength) {
            throw invalid("data segment file length mismatch")
                    .put(" [segmentId=").put(segmentId)
                    .put(", expected=").put(expectedFileLength)
                    .put(", actual=").put(actualLength)
                    .put(']');
        }
        mem.of(
                ff,
                path.$(),
                ff.getPageSize(),
                actualLength,
                MemoryTag.MMAP_DEFAULT,
                CairoConfiguration.O_NONE,
                -1
        );
        this.segmentId = segmentId;
        this.fileLength = actualLength;
        this.isOpen = true;
    }

    /**
     * Validates a state-page reference and the function/codec-specific bounds
     * supplied by its owner before exposing the payload.
     */
    public void openPage(
            @NotNull LiveViewCheckpointStatePageRef ref,
            int expectedPageKind,
            int expectedCodec,
            int allowedFlags,
            int maxRowCount,
            int maxDecodedLength
    ) {
        ensureOpen();
        pageOffset = -1;
        pageStoredLength = 0;
        if (ref.isNull()) {
            throw invalid("null data page reference");
        }
        if (ref.getSegmentId() != segmentId) {
            throw invalid("data page reference segment mismatch")
                    .put(" [expected=").put(segmentId)
                    .put(", actual=").put(ref.getSegmentId())
                    .put(']');
        }
        final int storedLength = ref.getStoredLength();
        final long offset = ref.getOffset();
        if (storedLength <= 0 || offset < 0 || offset > fileLength - (long) storedLength) {
            throw invalid("data page range out of bounds")
                    .put(" [offset=").put(offset)
                    .put(", storedLength=").put(storedLength)
                    .put(", fileLength=").put(fileLength)
                    .put(']');
        }
        if (ref.getDecodedLength() < 0 || maxDecodedLength < 0 || ref.getDecodedLength() > maxDecodedLength) {
            throw invalid("data page decoded length out of bounds")
                    .put(" [decodedLength=").put(ref.getDecodedLength())
                    .put(", max=").put(maxDecodedLength)
                    .put(']');
        }
        if (ref.getPageKind() != expectedPageKind) {
            throw invalid("data page kind mismatch")
                    .put(" [expected=").put(expectedPageKind)
                    .put(", actual=").put(ref.getPageKind())
                    .put(']');
        }
        if (ref.getCodec() != expectedCodec) {
            throw invalid("data page codec mismatch")
                    .put(" [expected=").put(expectedCodec)
                    .put(", actual=").put(ref.getCodec())
                    .put(']');
        }
        if ((ref.getFlags() & ~allowedFlags) != 0) {
            throw invalid("data page flags unsupported")
                    .put(" [flags=").put(ref.getFlags())
                    .put(", allowed=").put(allowedFlags)
                    .put(']');
        }
        if (ref.getRowCount() < 0 || maxRowCount < 0 || ref.getRowCount() > maxRowCount) {
            throw invalid("data page row count out of bounds")
                    .put(" [rowCount=").put(ref.getRowCount())
                    .put(", max=").put(maxRowCount)
                    .put(']');
        }
        pageOffset = offset;
        pageStoredLength = storedLength;
        currentRefDecodedLength = ref.getDecodedLength();
        currentRefRowCount = ref.getRowCount();
    }

    private void boundsCheck(long offset, int size) {
        ensurePageOpen();
        if (offset < 0 || offset > (long) pageStoredLength - size) {
            throw invalid("data page read out of bounds")
                    .put(" [offset=").put(offset)
                    .put(", size=").put(size)
                    .put(", storedLength=").put(pageStoredLength)
                    .put(']');
        }
    }

    private void ensureOpen() {
        if (!isOpen) {
            throw CairoException.critical(0).put("live view checkpoint data segment reader is not open");
        }
    }

    private void ensurePageOpen() {
        ensureOpen();
        if (pageOffset < 0) {
            throw invalid("no data page open");
        }
    }

    private CairoException invalid(CharSequence reason) {
        return CairoException.critical(CairoException.LV_CHECKPOINT_TIMELINE_INVALID)
                .put("live view checkpoint ").put(reason);
    }

    private void reset() {
        isOpen = false;
        segmentId = -1;
        fileLength = 0;
        pageOffset = -1;
        pageStoredLength = 0;
        currentRefDecodedLength = 0;
        currentRefRowCount = 0;
    }
}
