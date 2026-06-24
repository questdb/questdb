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

package io.questdb.cairo.vm;

import io.questdb.cairo.CairoException;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.vm.api.MemoryCARW;
import io.questdb.cairo.vm.api.MemoryCMARW;
import io.questdb.cairo.vm.api.MemoryMAR;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.Files;
import io.questdb.std.FilesFacade;
import io.questdb.std.Long256Acceptor;
import io.questdb.std.MemoryTag;
import io.questdb.std.Numbers;
import io.questdb.std.Vect;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Utf8Sequence;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

// contiguous mapped appendable readable writable
public class MemoryCMARWImpl extends AbstractMemoryCR implements MemoryCMARW, MemoryCARW, MemoryMAR {
    private static final Log LOG = LogFactory.getLog(MemoryCMARWImpl.class);
    private final Long256Acceptor long256Acceptor = this::putLong256;
    private long appendAddress = 0;
    // When true this memory is strictly append-only (no in-place put*(offset,..) below the
    // high-water mark), so sync() may narrow the msync to the appended range and skip when
    // nothing new was appended. Defaults to false: the non-appendOnly path is byte-identical
    // to the original full-extent msync.
    private boolean appendOnly = false;
    private boolean closeFdOnClose = true;
    private long extendSegmentMsb;
    private long fd = -1;
    // Append offset covered by the last msync (append-only path only). Kept in lock-step with the
    // skip decision in sync(): it can only ever be stale-LOW (a fresh/remapped/truncated memory
    // resets it to 0), never stale-HIGH, so the worst case is one extra harmless msync.
    private long lastSyncedAppendOffset = 0;
    private long lastSyncedSize = 0;
    private int madviseOpts = -1;
    private int memoryTag = MemoryTag.MMAP_DEFAULT;
    private long minMappedMemorySize = -1;

    public MemoryCMARWImpl(FilesFacade ff, LPSZ name, long extendSegmentSizePow2, long size, int memoryTag, int opts) {
        of(ff, name, extendSegmentSizePow2, size, memoryTag, opts, -1);
    }

    public MemoryCMARWImpl() {
    }

    @Override
    public long addressHi() {
        return lim;
    }

    @Override
    public long appendAddressFor(long bytes) {
        checkAndExtend(appendAddress + bytes);
        final long result = appendAddress;
        appendAddress += bytes;
        return result;
    }

    @Override
    public long appendAddressFor(long offset, long bytes) {
        checkAndExtend(pageAddress + offset + bytes);
        return pageAddress + offset;
    }

    @Override
    public void changeSize(long dataSize) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void close(boolean truncate, byte truncateMode) {
        if (pageAddress != 0) {
            final long truncateSize;
            if (truncate) {
                long appendOffset = getAppendOffset();
                truncateSize = truncateMode == Vm.TRUNCATE_TO_PAGE ? Files.ceilPageSize(appendOffset) : appendOffset;

                long sz = Math.min(size, truncateSize);
                if (appendOffset < sz) {
                    try {
                        // If this is a lazy close of unused memory the underlying file can already be truncated
                        //  using another fd and memset can lead to SIGBUS on Linux.
                        // Check the physical file length before trying to memset to the mapped memory.
                        sz = Math.min(sz, ff.length(fd));
                        if (appendOffset < sz) {
                            Vect.memset(pageAddress + appendOffset, sz - appendOffset, 0);
                        }
                    } catch (CairoException e) {
                        LOG.error().$("cannot determine file length to safely truncate [fd=").$(fd)
                                .$(", msg=").$safe(e.getFlyweightMessage())
                                .$(", errno=").$(e.getErrno())
                                .I$();
                    }
                }
            } else {
                truncateSize = -1L;
            }
            ff.munmap(pageAddress, size, memoryTag);
            this.pageAddress = 0;
            try {
                if (closeFdOnClose) {
                    Vm.bestEffortClose(ff, LOG, fd, truncateSize, truncateMode);
                } else {
                    Vm.bestEffortTruncate(ff, LOG, fd, truncateSize, truncateMode);
                }
            } finally {
                fd = -1;
            }
        }
        if (ff != null) {
            if (closeFdOnClose) {
                ff.close(fd);
                LOG.debug().$("closed [fd=").$(fd).I$();
            }
            fd = -1;
        }
        size = 0;
        ff = null;
    }

    @Override
    public void close() {
        // we have to clear the underling memory
        // to ensure direct strings obtained from
        // this memory do not segfault
        clear();
        close(true);
    }

    @Override
    public long detachFdClose() {
        try {
            long fd = this.fd;
            this.closeFdOnClose = false;
            close();
            assert this.fd == -1;
            return fd;
        } finally {
            closeFdOnClose = true;
        }
    }

    @Override
    public void extend(long newSize) {
        if (newSize > size) {
            extend0(newSize);
        }
    }

    @Override
    public long getAppendAddress() {
        return appendAddress;
    }

    @Override
    public long getAppendAddressSize() {
        return lim - appendAddress;
    }

    @Override
    public long getAppendOffset() {
        return appendAddress - pageAddress;
    }

    @Override
    public long getExtendSegmentSize() {
        return extendSegmentMsb;
    }

    @Override
    public long getFd() {
        return fd;
    }

    @Override
    public boolean isFileBased() {
        return true;
    }

    @Override
    public void jumpTo(long offset) {
        checkAndExtend(pageAddress + offset);
        appendAddress = pageAddress + offset;
        assert appendAddress <= lim;
    }

    @Override
    public void of(FilesFacade ff, LPSZ name, long extendSegmentSize, int memoryTag, int opts) {
        of(ff, name, extendSegmentSize, -1, memoryTag, opts);
    }

    @Override
    public void of(FilesFacade ff, LPSZ name, long extendSegmentSizePow2, long size, int memoryTag, int opts, int madviseOpts) {
        this.extendSegmentMsb = Numbers.msb(extendSegmentSizePow2);
        this.minMappedMemorySize = extendSegmentSizePow2;
        this.madviseOpts = madviseOpts;
        openFile(ff, name, opts);
        try {
            map(ff, name, size, memoryTag);
        } catch (Throwable th) {
            ff.close(fd);
            fd = -1;
            throw th;
        }
    }

    @Override
    public void of(FilesFacade ff, long fd, @Nullable LPSZ fileName, long size, int memoryTag) {
        close();
        assert fd > 0;
        this.ff = ff;
        this.extendSegmentMsb = Numbers.msb(ff.getMapPageSize());
        this.minMappedMemorySize = ff.getMapPageSize();
        this.fd = fd;
        map(ff, fileName, size, memoryTag);
    }

    @Override
    public void of(FilesFacade ff, long fd, boolean keepFdOpen, @Nullable LPSZ fileName, long extendSegmentSize, long size, int memoryTag) {
        this.closeFdOnClose = !keepFdOpen;
        of(ff, fd, null, size, memoryTag);
        this.extendSegmentMsb = Numbers.msb(extendSegmentSize);
    }

    @Override
    public void putLong256(@NotNull CharSequence hexString, int start, int end) {
        putLong256(hexString, start, end, long256Acceptor);
    }

    @Override
    public void setTruncateSize(long size) {
        jumpTo(size);
    }

    @Override
    public void skip(long bytes) {
        checkAndExtend(appendAddress + bytes);
        appendAddress += bytes;
    }

    public void swapState(MemoryCMARWImpl other) {
        long tFd = this.fd;
        this.fd = other.fd;
        other.fd = tFd;

        long tPage = this.pageAddress;
        this.pageAddress = other.pageAddress;
        other.pageAddress = tPage;

        long tLim = this.lim;
        this.lim = other.lim;
        other.lim = tLim;

        long tSize = this.size;
        this.size = other.size;
        other.size = tSize;

        long tApp = this.appendAddress;
        this.appendAddress = other.appendAddress;
        other.appendAddress = tApp;

        FilesFacade tFf = this.ff;
        this.ff = other.ff;
        other.ff = tFf;

        long tSeg = this.extendSegmentMsb;
        this.extendSegmentMsb = other.extendSegmentMsb;
        other.extendSegmentMsb = tSeg;

        long tMin = this.minMappedMemorySize;
        this.minMappedMemorySize = other.minMappedMemorySize;
        other.minMappedMemorySize = tMin;

        int tMad = this.madviseOpts;
        this.madviseOpts = other.madviseOpts;
        other.madviseOpts = tMad;

        int tTag = this.memoryTag;
        this.memoryTag = other.memoryTag;
        other.memoryTag = tTag;

        boolean tCof = this.closeFdOnClose;
        this.closeFdOnClose = other.closeFdOnClose;
        other.closeFdOnClose = tCof;

        long tLastSynced = this.lastSyncedSize;
        this.lastSyncedSize = other.lastSyncedSize;
        other.lastSyncedSize = tLastSynced;

        // The last-synced append offset describes the swapped page/append cursor, so it must travel
        // with them (alongside lastSyncedSize) to stay consistent. appendOnly is the per-file policy
        // and likewise belongs to the identity being swapped.
        long tLastSyncedAo = this.lastSyncedAppendOffset;
        this.lastSyncedAppendOffset = other.lastSyncedAppendOffset;
        other.lastSyncedAppendOffset = tLastSyncedAo;

        boolean tAppendOnly = this.appendOnly;
        this.appendOnly = other.appendOnly;
        other.appendOnly = tAppendOnly;
    }

    @Override
    public void switchTo(FilesFacade ff, long fd, long extendSegmentSizePow2, long offset, boolean truncate, byte truncateMode) {
        this.ff = ff;
        this.extendSegmentMsb = Numbers.msb(extendSegmentSizePow2);
        close(truncate, truncateMode);
        this.fd = fd;
        map(ff, null, offset, memoryTag);
    }

    public void sync(boolean async) {
        if (appendOnly) {
            // Append-only memory: the only durable-relevant dirty bytes are those between the last
            // synced append offset and the current append offset. Narrow the msync to the written
            // range, and skip entirely when nothing new was appended since the last sync.
            final long ao = getAppendOffset();
            if (ao == lastSyncedAppendOffset && size == lastSyncedSize) {
                // C: nothing appended and no extend since last sync -> no dirty data to flush.
                return;
            }
            if (ao > 0) {
                // D: flush only [0, appendOffset). Starting at the mapping base keeps the call simple
                // and page-aligned; the tail beyond appendOffset is never written so it stays clean.
                ff.msync(pageAddress, ao, async);
            }
            lastSyncedAppendOffset = ao;
        } else {
            ff.msync(pageAddress, size, async);
        }
        // SYNC mode: also make a file EXTEND durable. msync flushes data pages but not the inode
        // size after a posix_fallocate/ftruncate grow; fdatasync the fd when the file grew since the
        // last sync so a crash cannot lose the just-committed extent (P2). fdatasync is cheaper than
        // fsync (skips mtime/ctime inode metadata) but still persists data + i_size on Linux. ASYNC
        // stays non-blocking.
        if (!async && size > lastSyncedSize) {
            ff.fdatasync(fd);
            lastSyncedSize = size;
        }
    }

    @Override
    public void syncFlushKick() {
        // Batched SYNC stage 1: msync(MS_ASYNC) the dirty range to push mmap-dirty pages into the page
        // cache so the following sync_file_range can see them. Mirrors sync()'s msync-range selection
        // (append-only narrows to [0, appendOffset); else full [0, size)) but is always ASYNC and issues
        // NO fdatasync. CRITICAL: this NEVER touches lastSyncedAppendOffset/lastSyncedSize — advancing a
        // watermark here would make syncFlushFinishIfExtended() wrongly skip the extend fdatasync.
        if (appendOnly) {
            final long ao = getAppendOffset();
            if (ao > 0) {
                ff.msync(pageAddress, ao, true);
            }
        } else {
            ff.msync(pageAddress, size, true);
        }
    }

    @Override
    public void syncFlushDrain() {
        // Batched SYNC stage 2: sync_file_range(WRITE | WAIT_AFTER) writes the (now page-cache-dirty) range
        // back to the device cache and WAITS. NO device flush, NO watermark mutation. The mapping is rooted
        // at file offset 0, so the fd-relative dirty range equals the in-mapping range: append-only ->
        // [0, appendOffset); full -> [0, size). WAIT_AFTER is mandatory: without it the writeback may not
        // reach the device before the _cv device flush, and the content would be lost (model self-test 4/5).
        final long dirtyLen;
        if (appendOnly) {
            dirtyLen = getAppendOffset();
        } else {
            dirtyLen = size;
        }
        if (dirtyLen > 0) {
            ff.syncFileRange(fd, 0, dirtyLen, Files.SYNC_FILE_RANGE_WRITE | Files.SYNC_FILE_RANGE_WAIT_AFTER);
        }
    }

    @Override
    public void syncFlushFinishIfExtended() {
        // Batched SYNC stage 3: persist an EXTEND only. Content durability for non-extending files is
        // provided by the batch's _cv device flush (so we do NOT msync here). If the file grew since the
        // last sync, fdatasync journals the new i_size (and, being a device flush, also makes the
        // already-drained device-cache content durable) and we advance BOTH watermarks so a subsequent
        // sync()/finish correctly sees no further extend. Keeping lastSyncedAppendOffset in lock-step with
        // the (now-durable) append offset matches sync()'s post-msync bookkeeping for the append-only path.
        if (size > lastSyncedSize) {
            ff.fdatasync(fd);
            lastSyncedSize = size;
            if (appendOnly) {
                lastSyncedAppendOffset = getAppendOffset();
            }
        }
    }

    @Override
    public void setAppendOnly(boolean appendOnly) {
        this.appendOnly = appendOnly;
    }

    @Override
    public void truncate() {
        if (pageAddress != 0) {
            // try to remap to min size
            final long fileSize = ff.length(fd);
            long sz = Math.min(fileSize, minMappedMemorySize);
            try {
                // we are remapping file to make it smaller, should not need
                // to allocate space; we already have it
                this.pageAddress = TableUtils.mremap(
                        ff,
                        fd,
                        pageAddress,
                        size,
                        sz,
                        Files.MAP_RW,
                        memoryTag
                );
            } catch (Throwable e) {
                appendAddress = pageAddress;
                long truncatedToSize = Vm.bestEffortTruncate(ff, LOG, fd, 0);
                if (truncatedToSize != 0) {
                    if (truncatedToSize > 0) {
                        Vect.memset(pageAddress, truncatedToSize, 0);
                        this.size = sz;
                    } else {
                        Vect.memset(pageAddress, size, 0);
                    }
                    this.lim = pageAddress + size;
                }
                throw e;
            }

            this.size = sz;
            lastSyncedSize = sz;
            // truncate() resets the append cursor to the mapping base (appendOffset == 0) and zeroes
            // the page, so the last-synced append offset must reset too -> stale-LOW, never -HIGH.
            lastSyncedAppendOffset = 0;
            this.lim = pageAddress + sz;
            appendAddress = pageAddress;
            Vect.memset(pageAddress, sz, 0);

            // try to truncate the file to remove tail data
            if (ff.truncate(fd, Files.ceilPageSize(size))) {
                return;
            }

            // we could not truncate, this might happen on Windows when area of the same file is mapped
            // by another process

            long mem = TableUtils.mapRW(ff, fd, ff.length(fd), memoryTag);
            Vect.memset(mem + sz, fileSize - sz, 0);
            ff.munmap(mem, fileSize, memoryTag);
        }
    }

    @Override
    public void zero() {
        long baseLength = lim - pageAddress;
        Vect.memset(pageAddress, baseLength, 0);
    }

    private void checkAndExtend(long address) {
        if (address <= lim) {
            return;
        }
        extend0(address - pageAddress);
    }

    private void extend0(long newSize) {
        long nPages = (newSize >>> extendSegmentMsb) + 1;
        newSize = nPages << extendSegmentMsb;
        long offset = appendAddress - pageAddress;
        long previousSize = size;
        assert size > 0;
        TableUtils.allocateDiskSpace(ff, fd, newSize);
        try {
            this.pageAddress = TableUtils.mremap(
                    ff,
                    fd,
                    pageAddress,
                    previousSize,
                    newSize,
                    Files.MAP_RW,
                    memoryTag
            );
            ff.madvise(pageAddress, newSize, madviseOpts);
        } catch (Throwable e) {
            appendAddress = pageAddress + previousSize;
            close(false);
            throw e;
        }
        size = newSize;
        lim = pageAddress + newSize;
        appendAddress = pageAddress + offset;
    }

    private void map0(FilesFacade ff, long size) {
        try {
            this.pageAddress = TableUtils.mapRW(ff, fd, size, memoryTag);
            this.lim = pageAddress + size;
            ff.madvise(pageAddress, size, madviseOpts);
        } catch (Throwable e) {
            close(false);
            throw e;
        }
    }

    private void openFile(FilesFacade ff, LPSZ name, int opts) {
        close();
        this.ff = ff;
        fd = TableUtils.openFileRWOrFail(ff, name, opts);
    }

    protected void map(FilesFacade ff, @Nullable Utf8Sequence name, long size, int memoryTag) {
        this.memoryTag = memoryTag;
        // file either did not exist when length() was called or empty
        if (size < 1) {
            this.size = minMappedMemorySize;
            TableUtils.allocateDiskSpace(ff, fd, this.size);
            map0(ff, minMappedMemorySize);
            this.appendAddress = pageAddress;
        } else {
            this.size = size;
            map0(ff, size);
            this.appendAddress = pageAddress + size;
        }
        this.lastSyncedSize = this.size;
        // Fresh mapping (open / remap via of()/switchTo()): the append cursor is back at the file's
        // existing size, so nothing in the new mapping has been synced by THIS object yet. Resetting
        // to 0 keeps lastSyncedAppendOffset stale-LOW (never stale-HIGH): the first sync after a
        // remap cannot skip and will re-msync the live range. Append offset for a re-opened file is
        // its current size; we conservatively reset to 0 so the first sync flushes [0, appendOffset).
        this.lastSyncedAppendOffset = 0;
        if (name != null) {
            LOG.debug().$("open [file=").$(name)
                    .$(", fd=").$(fd)
                    .$(", pageSize=").$(size)
                    .$(", size=").$(this.size)
                    .I$();
        } else {
            LOG.debug().$("open [fd=").$(fd)
                    .$(", pageSize=").$(size)
                    .$(", size=").$(this.size).I$();
        }
    }
}
