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
    // allocates exactly the requested size instead of TableUtils.alignedSize()
    private final boolean exact;
    private final Long256Acceptor long256Acceptor = this::putLong256;
    // How far this object's own allocations backed allocatedFd; see
    // TableUtils.allocateDiskSpaceAligned(ff, fd, allocatedHi, size). Valid only
    // while fd == allocatedFd. A QuestDB fd carries a per-open counter (FdCache),
    // so a reopen gets a new fd even on the same OS fd, and needs no reset.
    private long allocatedFd = -1;
    private long allocatedHi;
    private long appendAddress = 0;
    private boolean closeFdOnClose = true;
    private long extendSegmentMsb;
    private long fd = -1;
    // When true, this memory is append-only (no in-place put*(offset, ..) below the cursor), so sync() may
    // narrow the msync to the appended range and skip it when nothing was appended. When false, sync()
    // msyncs the full extent.
    private boolean isAppendOnly = false;
    // Append offset covered by the last msync (append-only path only). It can only be stale-low (a fresh,
    // remapped or truncated memory resets it to 0), never stale-high, so the worst case is one extra msync.
    private long lastSyncedAppendOffset = 0;
    private long lastSyncedSize = 0;
    private int madviseOpts = -1;
    private int memoryTag = MemoryTag.MMAP_DEFAULT;
    private long minMappedMemorySize = -1;

    public MemoryCMARWImpl(FilesFacade ff, LPSZ name, long extendSegmentSizePow2, long size, int memoryTag, int opts) {
        this(false);
        of(ff, name, extendSegmentSizePow2, size, memoryTag, opts, -1);
    }

    public MemoryCMARWImpl() {
        this(false);
    }

    public MemoryCMARWImpl(boolean exact) {
        this.exact = exact;
    }

    @Override
    public long addressHi() {
        return lim;
    }

    @Override
    public void allocate(long size) {
        allocateDiskSpace(size);
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
        // Drop the append bounds of the mapping we no longer own. checkAndExtend() returns
        // early for any address at or below lim, so a stale lim lets jumpTo()/appendAddressFor()
        // hand out pointers into unmapped space instead of failing, and zero() -- which memsets
        // lim - pageAddress bytes from pageAddress, already nulled above -- would memset lim
        // bytes starting at address 0. Reset unconditionally: a close on an instance that never
        // mapped anything must end in the same clean state, same as size and ff.
        lim = 0;
        appendAddress = 0;
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
    public void flushFile(boolean barrier) {
        if (pageAddress != 0 && !ff.isSyncfsFileSystemWide()) {
            // Off Linux, write the mapped pages into the file before the flush (see MemoryMA.flushFile), over the
            // range sync(true) used to msync.
            final long len = isAppendOnly ? getAppendOffset() : size;
            if (len > 0) {
                ff.msync(pageAddress, len, true);
            }
        }
        if (barrier) {
            ff.barrierFsync(fd);
        } else {
            ff.fdatasync(fd);
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
        // sync() skips the msync when the append offset and size are unchanged since the last sync. A
        // backwards jump breaks that: WalWriter.rollback0() -> setAppendPosition() -> jumpTo(lower) rewinds
        // the cursor, and the next commit re-appends different bytes over the same range. For a fixed-width
        // column the cursor ends on the old offset, both watermarks match, and the rewritten bytes would never
        // be msynced. Lowering the watermark to the rewind point makes the next sync() flush them. Memories
        // that are not append-only never skip, so they keep a plain jumpTo.
        if (isAppendOnly && offset < lastSyncedAppendOffset) {
            lastSyncedAppendOffset = offset;
        }
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
        assert exact == other.exact;
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

        // The disk-space reservation describes the swapped fd, so it travels with it.
        long tAllocatedFd = this.allocatedFd;
        this.allocatedFd = other.allocatedFd;
        other.allocatedFd = tAllocatedFd;

        long tAllocatedHi = this.allocatedHi;
        this.allocatedHi = other.allocatedHi;
        other.allocatedHi = tAllocatedHi;

        long tLastSynced = this.lastSyncedSize;
        this.lastSyncedSize = other.lastSyncedSize;
        other.lastSyncedSize = tLastSynced;

        // The last-synced append offset describes the swapped append cursor, so it travels with it, as does
        // lastSyncedSize. isAppendOnly is a per-file policy and belongs to the swapped identity too.
        long tLastSyncedAo = this.lastSyncedAppendOffset;
        this.lastSyncedAppendOffset = other.lastSyncedAppendOffset;
        other.lastSyncedAppendOffset = tLastSyncedAo;

        boolean tAppendOnly = this.isAppendOnly;
        this.isAppendOnly = other.isAppendOnly;
        other.isAppendOnly = tAppendOnly;
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
        if (pageAddress == 0) {
            // Closed or unmapped memory has no dirty pages. The adaptive column-sync loop can still visit a
            // partition column that a parquet conversion freed, and close() nulls ff.
            return;
        }
        // An async sync is a no-op: msync(MS_ASYNC) has been a no-op on Linux since 2.6.19, and the
        // Windows msync ignores the flag and flushes synchronously. The OS writes dirty pages back
        // on its own.
        if (isAppendOnly) {
            // Append-only memory: the only dirty bytes lie between the last synced append offset and the
            // current one. Narrow the msync to the written range, and skip it when nothing was appended.
            final long ao = getAppendOffset();
            if (ao == lastSyncedAppendOffset && size == lastSyncedSize) {
                // The cursor has not passed the last-synced watermark and the memory has not grown, so no byte
                // above the watermark changed since the last msync. A rewind and re-append can return the
                // cursor to its old value with different bytes in between; jumpTo() and truncate() lower the
                // watermark on any backwards move, so that case reaches the msync below. The caller sets
                // isAppendOnly only on memories that never write in place below the cursor (MemoryMA.setAppendOnly).
                return;
            }
            if (ao > 0 && !async) {
                // Flush only [0, appendOffset): starting at the mapping base keeps the call page-aligned, and
                // the tail past the append offset is never written.
                ff.msync(pageAddress, ao, false);
            }
            lastSyncedAppendOffset = ao;
        } else if (!async) {
            ff.msync(pageAddress, size, false);
        }
    }

    @Override
    public void syncFlushDrain() {
        if (pageAddress == 0) {
            return; // closed/unmapped (e.g. a parquet-converted column): nothing to drain, fd is -1
        }
        // Batched flush, stage 2: sync_file_range(WRITE | WAIT_AFTER) waits for the writeback stage 1 started
        // and writes back anything it left; no device flush, no watermark change. Waiting here is what reports
        // a writeback error: the wait consumes it for this open file, so a later fdatasync would not see it.
        final long dirtyLen = syncFlushLen();
        if (dirtyLen > 0 && ff.syncFileRange(fd, 0, dirtyLen, Files.SYNC_FILE_RANGE_WRITE | Files.SYNC_FILE_RANGE_WAIT_AFTER) != 0) {
            throw CairoException.dataSyncFailureOfFd(ff, fd, ff.errno(), "sync_file_range", "could not sync_file_range");
        }
    }

    @Override
    public void syncFlushFinishIfExtended() {
        // Batched flush, stage 3: watermark bookkeeping only. The caller (TableWriter.syncColumnsBatchedSync)
        // has already issued one syncfs over the table's filesystem, which wrote back this column's data and
        // journaled the new size and extent conversions in one device flush, so a per-file fdatasync would be
        // a second flush. Advance the watermarks to the durable size, as sync() does after its msync.
        if (size > lastSyncedSize) {
            lastSyncedSize = size;
            if (isAppendOnly) {
                lastSyncedAppendOffset = getAppendOffset();
            }
        }
    }

    @Override
    public void syncFlushKick() {
        if (pageAddress == 0) {
            return; // closed/unmapped (e.g. a parquet-converted column): nothing to flush, fd is -1
        }
        // Batched flush, stage 1: sync_file_range(WRITE) starts writeback of the written range and returns, so
        // the writeback of every file in the batch runs at once before stage 2 waits. Pages dirtied through the
        // mapping need no msync first: Linux tracks them as dirty. No device flush, no watermark change. A
        // transient failure leaves the pages dirty for stage 2 and the caller's flush; any other is a flush
        // failure (see Vm.checkWritebackKick()).
        final long dirtyLen = syncFlushLen();
        if (dirtyLen > 0 && ff.syncFileRange(fd, 0, dirtyLen, Files.SYNC_FILE_RANGE_WRITE) != 0) {
            Vm.checkWritebackKick(ff, fd, ff.errno());
        }
    }

    @Override
    public void setAppendOnly(boolean isAppendOnly) {
        this.isAppendOnly = isAppendOnly;
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
                // As in jumpTo(), the cursor goes back to 0 while the watermark may sit above it. Drop the
                // watermark so a reused memory cannot skip the msync of rewritten bytes.
                lastSyncedAppendOffset = 0;
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
            // truncate() resets the append cursor to the mapping base and zeroes the page, so the
            // last-synced append offset resets too.
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

    private void allocateDiskSpace(long size) {
        final long hi = allocatedFd == fd ? allocatedHi : 0;
        allocatedHi = exact
                ? TableUtils.allocateDiskSpaceUnaligned(ff, fd, hi, size)
                : TableUtils.allocateDiskSpaceAligned(ff, fd, hi, size);
        allocatedFd = fd;
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
        allocateDiskSpace(newSize);
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
            allocateDiskSpace(size);
            this.pageAddress = TableUtils.mapRWNoAlloc(ff, fd, size, 0, memoryTag);
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

    // The range the batched flush writes back. The mapping starts at file offset 0, so it is
    // [0, appendOffset) for append-only memory and [0, size) otherwise, the range sync() selects.
    private long syncFlushLen() {
        return isAppendOnly ? getAppendOffset() : size;
    }

    protected void map(FilesFacade ff, @Nullable Utf8Sequence name, long size, int memoryTag) {
        this.memoryTag = memoryTag;
        // file either did not exist when length() was called or empty
        if (size < 1) {
            this.size = minMappedMemorySize;
            map0(ff, minMappedMemorySize);
            this.appendAddress = pageAddress;
        } else {
            this.size = size;
            map0(ff, size);
            this.appendAddress = pageAddress + size;
        }
        this.lastSyncedSize = this.size;
        // A fresh mapping (open, or remap through of() or switchTo()) has synced nothing through this object.
        // Resetting to 0 keeps the watermark stale-low: the first sync after a remap flushes [0, appendOffset).
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
