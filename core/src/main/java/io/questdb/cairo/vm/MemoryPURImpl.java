/*******************************************************************************
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
import io.questdb.cairo.vm.api.MemoryMAR;
import io.questdb.cairo.wal.WalWriterRingManager;
import io.questdb.cairo.wal.WalWriterRingManager.WalWriterRingColumn;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.FilesFacade;
import io.questdb.std.IntList;
import io.questdb.std.LongList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import io.questdb.std.Vect;
import io.questdb.std.str.LPSZ;
import org.jetbrains.annotations.TestOnly;

// paged io_uring-backed appendable readable
public class MemoryPURImpl extends MemoryPARWImpl implements MemoryMAR, WalWriterRingColumn {

    static final int CONFIRMED = 2;
    static final int ERROR = 3;
    static final int SUBMITTED = 1;
    static final int WRITING = 0;
    private static final Log LOG = LogFactory.getLog(MemoryPURImpl.class);
    private final IntList pageStates = new IntList();
    private final LongList pageExpectedLens = new LongList();
    private final WalWriterRingManager ringManager;
    private long allocatedFileSize;
    private int cqeError;
    private int columnSlot = -1;
    private boolean distressed;
    private long fd = -1;
    private FilesFacade ff;
    // Debug counters for leak isolation (bytes and counts)
    private long dbgPageBytesAlloc;
    private long dbgPageBytesFree;
    private long dbgPageAllocs;
    private long dbgPageFrees;
    private long dbgSnapshotBytesAlloc;
    private long dbgSnapshotBytesFree;
    private long dbgSnapshotAllocs;
    private long dbgSnapshotFrees;
    private boolean snapshotInFlight;
    private long snapshotBufAddr;
    private long snapshotBufSize;

    @TestOnly
    public MemoryPURImpl(FilesFacade ff, LPSZ name, long pageSize, int memoryTag, int opts,
                         WalWriterRingManager ringManager) {
        this(ringManager);
        of(ff, name, pageSize, 0, memoryTag, opts, -1);
    }

    public MemoryPURImpl(WalWriterRingManager ringManager) {
        this.ringManager = ringManager;
    }

    @Override
    public long addressOf(long offset) {
        if (offset >= getAppendOffset()) {
            throw CairoException.critical(0)
                    .put("addressOf beyond append offset [offset=").put(offset)
                    .put(", appendOffset=").put(getAppendOffset()).put(']');
        }
        return super.addressOf(offset);
    }

    @Override
    public void close() {
        close(true);
    }

    public final void close(boolean truncate, byte truncateMode) {
        if (fd != -1) {
            // Flush any remaining WRITING pages before draining.
            flushAllWritingPages();
            // Drain all in-flight CQEs before freeing any buffers.
            ringManager.waitForAll();
            freeSnapshotBuffer();
            long sz = truncate ? getAppendOffset() : -1L;
            super.close();
            clearPageStates();
            try {
                Vm.bestEffortClose(ff, LOG, fd, sz, truncateMode);
            } finally {
                fd = -1;
            }
        } else {
            // Even if fd is already detached, ensure buffers are released.
            freeSnapshotBuffer();
            super.close();
            clearPageStates();
        }
        allocatedFileSize = 0;
        distressed = false;
        logDebugLeakCounters("close");
    }

    @Override
    public long detachFdClose() {
        long detachedFd = this.fd;
        // Flush and drain while fd is still valid.
        flushAllWritingPages();
        ringManager.waitForAll();
        freeSnapshotBuffer();
        super.close();
        clearPageStates();
        // Detach fd without closing it — caller takes ownership.
        this.fd = -1;
        allocatedFileSize = 0;
        distressed = false;
        logDebugLeakCounters("detach");
        return detachedFd;
    }

    @Override
    public long getFd() {
        return fd;
    }

    @Override
    public FilesFacade getFilesFacade() {
        return ff;
    }

    @Override
    public long getPageAddress(int page) {
        if (page < pages.size()) {
            long addr = pages.getQuick(page);
            if (addr != 0) {
                return addr;
            }
            // Page was evicted. Pread it back.
            if (page < pageStates.size()) {
                int state = pageStates.getQuick(page);
                if (state == CONFIRMED || state == ERROR) {
                    return preadPage(page);
                }
            }
        }
        return 0L;
    }

    @Override
    public boolean isDistressed() {
        return distressed;
    }

    @Override
    public boolean isPageConfirmed(long pageId) {
        int page = (int) pageId;
        if (page < pageStates.size()) {
            return pageStates.getQuick(page) == CONFIRMED;
        }
        return false;
    }

    @Override
    public void jumpTo(long offset) {
        // Wait for any SUBMITTED pages between the target and current append position.
        int targetPage = pageIndex(offset);
        int currentPage = getAppendOffset() > 0 ? pageIndex(getAppendOffset() - 1) : -1;

        // Wait for SUBMITTED pages that we're rolling back over.
        for (int i = targetPage; i <= currentPage && i < pageStates.size(); i++) {
            if (pageStates.getQuick(i) == SUBMITTED) {
                ringManager.waitForPage(columnSlot, i);
            }
        }
        super.jumpTo(offset);
    }

    @Override
    public void of(FilesFacade ff, LPSZ name, long extendSegmentSize, long size, int memoryTag, int opts, int madviseOpts) {
        close();
        this.memoryTag = MemoryTag.NATIVE_TABLE_WAL_WRITER;
        this.ff = ff;
        setExtendSegmentSize(extendSegmentSize);
        fd = TableUtils.openFileRWOrFail(ff, name, opts);
        long fileLen = ff.length(fd);
        allocatedFileSize = fileLen > 0 ? fileLen : 0;
        distressed = false;
        if (columnSlot == -1) {
            columnSlot = ringManager.registerColumn(this);
        }
        LOG.debug().$("open ").$(name).$(" [fd=").$(fd).$(", extendSegmentSize=").$(extendSegmentSize).$(']').$();
    }

    @Override
    public final void of(FilesFacade ff, LPSZ name, long extendSegmentSize, int memoryTag, int opts) {
        of(ff, name, extendSegmentSize, 0, memoryTag, opts, -1);
    }

    @Override
    public void of(FilesFacade ff, LPSZ name, long extendSegmentSize, long size, int memoryTag, int opts) {
        of(ff, name, extendSegmentSize, size, memoryTag, opts, -1);
    }

    @Override
    public void onSnapshotCompleted(int cqeRes) {
        if (cqeRes < 0 || cqeRes != snapshotBufSize) {
            distressed = true;
            cqeError = cqeRes < 0 ? -cqeRes : 0;
        }
        freeSnapshotBuffer();
    }

    public void refreshCurrentPageFromFile() {
        if (fd == -1) {
            return;
        }
        long appendOffset = getAppendOffset();
        if (appendOffset <= 0) {
            return;
        }
        int currentPage = pageIndex(appendOffset - 1);
        if (currentPage >= pages.size()) {
            return;
        }
        long addr = pages.getQuick(currentPage);
        if (addr == 0) {
            return;
        }
        long pageStart = pageOffset(currentPage);
        int len = (int) (appendOffset - pageStart);
        if (len <= 0) {
            return;
        }
        long bytesRead = ff.read(fd, addr, len, pageStart);
        if (bytesRead < 0) {
            distressed = true;
            cqeError = ff.errno();
            throw CairoException.critical(cqeError)
                    .put("pread failed [fd=").put(fd).put(", offset=").put(pageStart).put(']');
        }
        if (bytesRead != len) {
            distressed = true;
            cqeError = 0;
            throw CairoException.critical(0)
                    .put("pread incomplete [fd=").put(fd)
                    .put(", offset=").put(pageStart)
                    .put(", len=").put(len)
                    .put(", read=").put(bytesRead)
                    .put(']');
        }
    }

    @Override
    public void onWriteCompleted(long pageId, int cqeRes) {
        int page = (int) pageId;
        if (page >= pageExpectedLens.size()) {
            distressed = true;
            cqeError = 0;
            return;
        }
        long expectedLen = pageExpectedLens.getQuick(page);
        if (cqeRes < 0) {
            distressed = true;
            cqeError = -cqeRes;
            setPageState(page, ERROR);
        } else if (cqeRes != expectedLen) {
            distressed = true;
            cqeError = 0;
            setPageState(page, ERROR);
        } else {
            setPageState(page, CONFIRMED);
        }
    }

    @Override
    public void switchTo(FilesFacade ff, long fd, long extendSegmentSize, long offset, boolean truncate, byte truncateMode) {
        // Close the old fd first (flushes all pages, drains CQEs, truncates).
        close(truncate, truncateMode);
        // Now set up for the new fd.
        this.ff = ff;
        this.fd = fd;
        long fileLen = ff.length(fd);
        this.allocatedFileSize = fileLen > 0 ? fileLen : 0;
        this.distressed = false;
        setExtendSegmentSize(extendSegmentSize);
        jumpTo(offset);
    }

    @Override
    public void sync(boolean async) {
        checkDistressed();

        if (async) {
            syncAsync();
        } else {
            syncSync();
        }
    }

    @Override
    public void truncate() {
        if (fd == -1) {
            return;
        }
        ringManager.waitForAll();
        super.close();
        clearPageStates();
        if (!ff.truncate(Math.abs(fd), getExtendSegmentSize())) {
            throw CairoException.critical(ff.errno())
                    .put("Cannot truncate fd=").put(fd).put(" to ").put(getExtendSegmentSize()).put(" bytes");
        }
        allocatedFileSize = getExtendSegmentSize();
        jumpTo(0);
        LOG.debug().$("truncated [fd=").$(fd).$(']').$();
    }

    int getPageState(int page) {
        if (page < pageStates.size()) {
            return pageStates.getQuick(page);
        }
        return -1;
    }

    @Override
    protected long mapWritePage(int page, long offset) {
        checkDistressed();

        // Submit previous page's pwrite if it's in WRITING state.
        submitPreviousPage(page);

        // Non-blocking drain to surface errors early.
        ringManager.drainCqes();

        // Check if page already exists.
        if (page < pages.size()) {
            long existingAddr = pages.getQuick(page);
            if (existingAddr != 0) {
                int state = page < pageStates.size() ? pageStates.getQuick(page) : -1;
                if (state == CONFIRMED) {
                    // Re-entering a confirmed page for writing (e.g. after rollback).
                    setPageState(page, WRITING);
                    return existingAddr;
                }
                if (state == WRITING) {
                    return existingAddr;
                }
            }
        }

        // Evict confirmed pages to bound memory.
        evictConfirmedPages(page);

        // Ensure file is large enough.
        ensureFileSize(pageOffset(page) + getExtendSegmentSize());

        // Allocate new page buffer.
        long addr = Unsafe.malloc(getExtendSegmentSize(), MemoryTag.NATIVE_TABLE_WAL_WRITER);
        dbgPageAllocs++;
        dbgPageBytesAlloc += getExtendSegmentSize();
        // If the file already contains data for this page, pread it to avoid clobbering
        // existing contents (e.g., pre-initialized null vectors).
        long fileLen = ff.length(fd);
        if (fileLen > allocatedFileSize) {
            allocatedFileSize = fileLen;
        }
        if (fileLen > 0) {
            long pageStart = pageOffset(page);
            if (pageStart < fileLen) {
                int readLen = (int) Math.min(getExtendSegmentSize(), fileLen - pageStart);
                long bytesRead = ff.read(fd, addr, readLen, pageStart);
                if (bytesRead < 0) {
                    Unsafe.free(addr, getExtendSegmentSize(), MemoryTag.NATIVE_TABLE_WAL_WRITER);
                    dbgPageFrees++;
                    dbgPageBytesFree += getExtendSegmentSize();
                    distressed = true;
                    cqeError = ff.errno();
                    throw CairoException.critical(cqeError)
                            .put("pread failed [fd=").put(fd).put(", offset=").put(pageStart).put(']');
                }
                if (bytesRead != readLen) {
                    Unsafe.free(addr, getExtendSegmentSize(), MemoryTag.NATIVE_TABLE_WAL_WRITER);
                    dbgPageFrees++;
                    dbgPageBytesFree += getExtendSegmentSize();
                    distressed = true;
                    cqeError = 0;
                    throw CairoException.critical(0)
                            .put("pread incomplete [fd=").put(fd)
                            .put(", offset=").put(pageStart)
                            .put(", len=").put(readLen)
                            .put(", read=").put(bytesRead)
                            .put(']');
                }
            }
        }
        cachePageAddress(page, addr);
        setPageState(page, WRITING);
        setPageExpectedLen(page, 0);

        return addr;
    }

    @Override
    protected void release(long address) {
        if (address != 0) {
            // Clear hot-page cache if the freed address falls within the cached range.
            clearHotPage();
            Unsafe.free(address, getPageSize(), MemoryTag.NATIVE_TABLE_WAL_WRITER);
            dbgPageFrees++;
            dbgPageBytesFree += getPageSize();
        }
    }

    private void checkDistressed() {
        if (distressed) {
            throw CairoException.critical(cqeError).put("io_uring write error, column distressed");
        }
    }

    private void clearPageStates() {
        pageStates.clear();
        pageExpectedLens.clear();
    }

    private void flushAllWritingPages() {
        long appendOffset = getAppendOffset();
        for (int i = 0, n = Math.min(pages.size(), pageStates.size()); i < n; i++) {
            long addr = pages.getQuick(i);
            if (addr != 0 && pageStates.getQuick(i) == WRITING) {
                long fileOffset = pageOffset(i);
                long pageEnd = fileOffset + getExtendSegmentSize();
                // Determine how much of this page is dirty.
                int writeLen;
                if (appendOffset <= fileOffset) {
                    continue; // Page is beyond append offset, nothing to write.
                } else if (appendOffset >= pageEnd) {
                    writeLen = (int) getExtendSegmentSize(); // Full page.
                } else {
                    writeLen = (int) (appendOffset - fileOffset); // Partial page.
                }
                ensureFileSize(fileOffset + writeLen);
                ringManager.enqueueWrite(columnSlot, i, fd, fileOffset, addr, writeLen);
                setPageExpectedLen(i, writeLen);
                setPageState(i, SUBMITTED);
            }
        }
    }

    private void ensureFileSize(long requiredSize) {
        if (requiredSize <= allocatedFileSize) {
            return;
        }
        // Allocate in chunks aligned to page size.
        long chunkSize = getExtendSegmentSize() * 4;
        long newSize = Math.max(requiredSize, allocatedFileSize + chunkSize);
        // Round up to page size.
        newSize = ((newSize + getExtendSegmentSize() - 1) / getExtendSegmentSize()) * getExtendSegmentSize();

        if (!ff.fallocateKeepSize(fd, allocatedFileSize, newSize - allocatedFileSize)) {
            // Fallback to allocate (changes visible size).
            LOG.info().$("fallocateKeepSize failed, falling back to allocate [fd=").$(fd).$(']').$();
            if (!ff.allocate(fd, newSize)) {
                throw CairoException.critical(ff.errno())
                        .put("Cannot extend file fd=").put(fd).put(" to ").put(newSize);
            }
        }
        allocatedFileSize = newSize;
    }

    private void evictConfirmedPages(int currentPage) {
        for (int i = 0, n = Math.min(pages.size(), pageStates.size()); i < n; i++) {
            if (i == currentPage) {
                continue;
            }
            long addr = pages.getQuick(i);
            if (addr != 0 && pageStates.getQuick(i) == CONFIRMED) {
                release(addr);
                pages.setQuick(i, 0);
            }
        }
    }

    private void freeSnapshotBuffer() {
        if (snapshotBufAddr != 0) {
            dbgSnapshotFrees++;
            dbgSnapshotBytesFree += snapshotBufSize;
            Unsafe.free(snapshotBufAddr, snapshotBufSize, MemoryTag.NATIVE_TABLE_WAL_WRITER);
            snapshotBufAddr = 0;
            snapshotBufSize = 0;
        }
        snapshotInFlight = false;
    }

    private long preadPage(int page) {
        long pageSize = getExtendSegmentSize();
        long buf = Unsafe.malloc(pageSize, MemoryTag.NATIVE_TABLE_WAL_WRITER);
        dbgPageAllocs++;
        dbgPageBytesAlloc += pageSize;
        long fileOffset = pageOffset(page);
        long bytesRead = ff.read(fd, buf, pageSize, fileOffset);
        if (bytesRead < 0) {
            Unsafe.free(buf, pageSize, MemoryTag.NATIVE_TABLE_WAL_WRITER);
            dbgPageFrees++;
            dbgPageBytesFree += pageSize;
            throw CairoException.critical(ff.errno())
                    .put("pread failed [fd=").put(fd).put(", offset=").put(fileOffset).put(']');
        }
        cachePageAddress(page, buf);
        setPageState(page, CONFIRMED);
        return buf;
    }

    private void setPageExpectedLen(int page, long len) {
        while (pageExpectedLens.size() <= page) {
            pageExpectedLens.add(0);
        }
        pageExpectedLens.setQuick(page, len);
    }

    private void setPageState(int page, int state) {
        while (pageStates.size() <= page) {
            pageStates.add(-1);
        }
        pageStates.setQuick(page, state);
    }

    private void syncAsync() {
        // Backpressure: wait for previous snapshot if still in-flight.
        if (snapshotInFlight) {
            ringManager.waitForAll();
            // onSnapshotCompleted will have been called, freeing the buffer.
            if (snapshotInFlight) {
                // Defensive: if CQE was lost or column detached, ensure we don't leak.
                LOG.info().$("snapshot still in flight after wait; forcing free [fd=").$(fd).$(", columnSlot=").$(columnSlot).$(']').$();
                freeSnapshotBuffer();
            }
        }

        long appendOffset = getAppendOffset();
        if (appendOffset <= 0) {
            return;
        }
        int currentPage = pageIndex(appendOffset - 1);
        if (currentPage >= pageStates.size() || pageStates.getQuick(currentPage) != WRITING) {
            return;
        }
        long addr = currentPage < pages.size() ? pages.getQuick(currentPage) : 0;
        if (addr == 0) {
            return;
        }
        long pageStart = pageOffset(currentPage);
        int dirtyLen = (int) (appendOffset - pageStart);
        if (dirtyLen <= 0) {
            return;
        }

        // Snapshot the dirty range into a temp buffer.
        if (snapshotBufAddr != 0) {
            // Defensive: should not happen, but avoid leaking the previous buffer.
            LOG.info().$("snapshot buffer not cleared before alloc; forcing free [fd=").$(fd).$(", columnSlot=").$(columnSlot).$(']').$();
            freeSnapshotBuffer();
        }
        snapshotBufAddr = Unsafe.malloc(dirtyLen, MemoryTag.NATIVE_TABLE_WAL_WRITER);
        snapshotBufSize = dirtyLen;
        dbgSnapshotAllocs++;
        dbgSnapshotBytesAlloc += dirtyLen;
        Vect.memcpy(snapshotBufAddr, addr, dirtyLen);

        // Submit snapshot pwrite.
        try {
            ensureFileSize(pageStart + dirtyLen);
            ringManager.enqueueSnapshotWrite(columnSlot, fd, pageStart, snapshotBufAddr, dirtyLen);
            snapshotInFlight = true;
        } catch (Throwable th) {
            // Prevent leak if any step fails.
            freeSnapshotBuffer();
            throw th;
        }
        // Do NOT block. The WRITING page remains writable.
    }

    private void logDebugLeakCounters(CharSequence stage) {
        if ((dbgPageBytesAlloc != dbgPageBytesFree) || (dbgSnapshotBytesAlloc != dbgSnapshotBytesFree)) {
            LOG.info().$("wal writer mem stats [stage=").$safe(stage)
                    .$(", fd=").$(fd)
                    .$(", columnSlot=").$(columnSlot)
                    .$(", pageAlloc=").$(dbgPageAllocs)
                    .$(", pageFree=").$(dbgPageFrees)
                    .$(", pageBytesAlloc=").$(dbgPageBytesAlloc)
                    .$(", pageBytesFree=").$(dbgPageBytesFree)
                    .$(", snapAlloc=").$(dbgSnapshotAllocs)
                    .$(", snapFree=").$(dbgSnapshotFrees)
                    .$(", snapBytesAlloc=").$(dbgSnapshotBytesAlloc)
                    .$(", snapBytesFree=").$(dbgSnapshotBytesFree)
                    .$(", snapshotInFlight=").$(snapshotInFlight)
                    .$(", inFlight=").$(ringManager != null ? ringManager.getInFlightCount() : -1)
                    .$(", snapshotBufSize=").$(snapshotBufSize)
                    .I$();
        }
    }

    private void syncSync() {
        // Submit dirty range of current WRITING page.
        submitCurrentPageDirtyRange();
        // Wait for all writes (including the partial page write).
        ringManager.waitForAll();
        // Submit fsync and wait.
        ringManager.enqueueFsync(columnSlot, fd);
        ringManager.waitForAll();
        // Evict confirmed pages, but keep the current page resident so that
        // continued appends reuse the existing buffer (with its synced data intact).
        int activePage = getAppendOffset() > 0 ? pageIndex(getAppendOffset() - 1) : -1;
        evictConfirmedPages(activePage);
    }

    private void submitCurrentPageDirtyRange() {
        long appendOffset = getAppendOffset();
        if (appendOffset <= 0) {
            return;
        }
        int currentPage = pageIndex(appendOffset - 1);
        if (currentPage >= pageStates.size() || pageStates.getQuick(currentPage) != WRITING) {
            return;
        }
        long addr = currentPage < pages.size() ? pages.getQuick(currentPage) : 0;
        if (addr == 0) {
            return;
        }
        long pageStart = pageOffset(currentPage);
        int dirtyLen = (int) (appendOffset - pageStart);
        if (dirtyLen <= 0) {
            return;
        }
        ensureFileSize(pageStart + dirtyLen);
        ringManager.enqueueWrite(columnSlot, currentPage, fd, pageStart, addr, dirtyLen);
        setPageExpectedLen(currentPage, dirtyLen);
        setPageState(currentPage, SUBMITTED);
    }

    private void submitPreviousPage(int currentPage) {
        if (currentPage == 0) {
            return;
        }
        int prevPage = currentPage - 1;
        if (prevPage < pageStates.size() && pageStates.getQuick(prevPage) == WRITING) {
            long prevAddr = pages.getQuick(prevPage);
            if (prevAddr != 0) {
                long pageSize = getExtendSegmentSize();
                long fileOffset = pageOffset(prevPage);
                ringManager.enqueueWrite(columnSlot, prevPage, fd, fileOffset, prevAddr, (int) pageSize);
                setPageExpectedLen(prevPage, pageSize);
                setPageState(prevPage, SUBMITTED);
            }
        }
    }
}
