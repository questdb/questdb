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
import io.questdb.cairo.wal.WalWriterBufferPool;
import io.questdb.cairo.wal.WalWriterRingManager;
import io.questdb.cairo.wal.WalWriterRingManager.WalWriterRingColumn;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.FilesFacade;
import io.questdb.std.IntList;
import io.questdb.std.LongList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import io.questdb.std.str.LPSZ;
import org.jetbrains.annotations.TestOnly;

/**
 * Paged io_uring-backed appendable memory for WAL column files.
 * <p>
 * Data is written into pool-acquired pages and flushed to disk via io_uring pwrite.
 * Pages transition through WRITING -> SUBMITTED -> CONFIRMED. In ASYNC commit mode,
 * {@link #syncSwap()} submits the current buffer as a swap-write and acquires a fresh
 * buffer from the pool, allowing the writer to continue appending without blocking.
 * <p>
 * Callers that already enforce a barrier (e.g., {@code ringManager.waitForAll()})
 * may use {@link #syncSubmitInPlace()} to submit the live buffer directly, and then call
 * {@link #resumeWriteAfterSync()} to restore the WRITING state of the current page.
 * <p>
 * Not thread-safe; intended for a single WAL writer thread with CQE callbacks
 * delivered by {@link WalWriterRingManager}.
 */
public class MemoryPURImpl extends MemoryPARWImpl implements MemoryMAR, WalWriterRingColumn {

    static final int CONFIRMED = 2;
    static final int ERROR = 3;
    static final int SUBMITTED = 1;
    static final int WRITING = 0;
    private static final Log LOG = LogFactory.getLog(MemoryPURImpl.class);
    private final IntList pageBufferIndices = new IntList();
    private final LongList pageExpectedLens = new LongList();
    private final LongList pageDirtyStarts = new LongList();
    private final IntList pageStates = new IntList();
    private final WalWriterRingManager ringManager;
    private long allocatedFileSize;
    private int cqeError;
    private int columnSlot = -1;
    private boolean distressed;
    private long fd = -1;
    private FilesFacade ff;
    private long lastSyncedAppendOffset;
    private WalWriterBufferPool pool;

    @TestOnly
    public MemoryPURImpl(FilesFacade ff, LPSZ name, long pageSize, int memoryTag, int opts,
                         WalWriterRingManager ringManager, WalWriterBufferPool pool) {
        this(ringManager, pool);
        of(ff, name, pageSize, 0, memoryTag, opts, -1);
    }

    public MemoryPURImpl(WalWriterRingManager ringManager, WalWriterBufferPool pool) {
        this.ringManager = ringManager;
        this.pool = pool;
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
            flushAllWritingPages();
            ringManager.waitForAll();
            long sz = truncate ? getAppendOffset() : -1L;
            releaseAllPageBuffersToPool();
            super.close();
            clearPageStates();
            try {
                Vm.bestEffortClose(ff, LOG, fd, sz, truncateMode);
            } finally {
                fd = -1;
            }
        } else {
            releaseAllPageBuffersToPool();
            super.close();
            clearPageStates();
        }
        allocatedFileSize = 0;
        distressed = false;
        lastSyncedAppendOffset = 0;
    }

    @Override
    public long detachFdClose() {
        long detachedFd = this.fd;
        flushAllWritingPages();
        ringManager.waitForAll();
        releaseAllPageBuffersToPool();
        super.close();
        clearPageStates();
        this.fd = -1;
        allocatedFileSize = 0;
        distressed = false;
        lastSyncedAppendOffset = 0;
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
        int targetPage = pageIndex(offset);
        int currentPage = getAppendOffset() > 0 ? pageIndex(getAppendOffset() - 1) : -1;

        for (int i = targetPage; i <= currentPage && i < pageStates.size(); i++) {
            if (pageStates.getQuick(i) == SUBMITTED) {
                ringManager.waitForPage(columnSlot, i);
            }
        }
        // If rolling back within the target page, reset dirty start so that
        // subsequent writes from the rollback point are flushed correctly.
        if (offset < getPageDirtyStart(targetPage)) {
            setPageDirtyStart(targetPage, offset);
        }
        super.jumpTo(offset);
        if (offset < lastSyncedAppendOffset) {
            lastSyncedAppendOffset = 0;
        }
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
    public void onFsyncCompleted(int cqeRes) {
        distressed = true;
        cqeError = cqeRes < 0 ? -cqeRes : 0;
    }

    @Override
    public void onSnapshotCompleted(int cqeRes) {
        // Legacy snapshot path — kept for interface compatibility.
        // With pool-based swap sync, snapshots are no longer used.
    }

    @Override
    public void onSwapWriteError(int cqeRes) {
        distressed = true;
        cqeError = cqeRes < 0 ? -cqeRes : 0;
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

    /**
     * Restore the current page to WRITING after a ring barrier completes.
     * Must be called after {@code ringManager.waitForAll()} when the last sync used
     * {@link #syncSubmitInPlace()}.
     */
    public void resumeWriteAfterSync() {
        long appendOffset = getAppendOffset();
        if (appendOffset <= 0) {
            return;
        }
        int currentPage = pageIndex(appendOffset - 1);
        if (currentPage < pageStates.size()) {
            int state = pageStates.getQuick(currentPage);
            if (state == CONFIRMED) {
                setPageState(currentPage, WRITING);
                setPageDirtyStart(currentPage, appendOffset);
            } else if (state == SUBMITTED) {
                LOG.info().$("page still submitted after wait [fd=").$(fd)
                        .$(", columnSlot=").$(columnSlot)
                        .$(", page=").$(currentPage)
                        .$(", appendOffset=").$(appendOffset)
                        .I$();
            }
        }
    }

    @Override
    public void switchTo(FilesFacade ff, long fd, long extendSegmentSize, long offset, boolean truncate, byte truncateMode) {
        close(truncate, truncateMode);
        this.ff = ff;
        this.fd = fd;
        long fileLen = ff.length(fd);
        this.allocatedFileSize = fileLen > 0 ? fileLen : 0;
        this.distressed = false;
        if (columnSlot == -1) {
            columnSlot = ringManager.registerColumn(this);
        }
        setExtendSegmentSize(extendSegmentSize);
        jumpTo(offset);
    }

    @Override
    public void sync(boolean async) {
        checkDistressed();
        if (async) {
            syncSwap();
        } else {
            syncSync();
        }
    }

    /**
     * Submit the current dirty range using the live page buffer (no swap, no copy).
     * <p>
     * Lifecycle contract:
     * - Caller must ensure no further writes happen until a ring barrier completes
     * (typically {@code ringManager.waitForAll()}).
     * - After the barrier, caller must invoke {@link #resumeWriteAfterSync()} before
     * any subsequent appends or commits.
     */
    public void syncSubmitInPlace() {
        checkDistressed();
        long appendOffset = getAppendOffset();
        if (appendOffset == lastSyncedAppendOffset) {
            return;
        }
        submitCurrentPageDirtyRange();
        lastSyncedAppendOffset = appendOffset;
    }

    /**
     * Swap-based async sync: submit the current buffer via swap-write and acquire
     * a fresh buffer from the pool. The writer can continue appending immediately.
     * <p>
     * The old buffer is released back to the pool when the CQE completes.
     * No memcpy, no blocking, no snapshot buffer management.
     */
    public void syncSwap() {
        checkDistressed();
        long appendOffset = getAppendOffset();
        if (appendOffset == lastSyncedAppendOffset) {
            return;
        }
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

        long dirtyStart = getPageDirtyStart(currentPage);
        long pageStart = pageOffset(currentPage);
        long bufOffset = dirtyStart - pageStart;
        int dirtyLen = (int) (appendOffset - dirtyStart);
        if (dirtyLen <= 0) {
            return;
        }

        ensureFileSize(dirtyStart + dirtyLen);

        // Submit old buffer as swap-write (CQE will release it to pool).
        int oldBufIdx = getPageBufferIndex(currentPage);
        ringManager.enqueueSwapWrite(columnSlot, oldBufIdx, fd, dirtyStart, addr + bufOffset, dirtyLen);

        // Acquire fresh buffer from pool.
        int newBufIdx = pool.acquire();
        long newAddr = pool.address(newBufIdx);
        cachePageAddress(currentPage, newAddr);
        setPageBufferIndex(currentPage, newBufIdx);
        setPageDirtyStart(currentPage, appendOffset);

        // Adjust hot-path pointers to point into new buffer.
        long offsetInPage = appendOffset - pageStart;
        updateWritePointers(currentPage, newAddr, offsetInPage);

        lastSyncedAppendOffset = appendOffset;
    }

    @Override
    public void truncate() {
        if (fd == -1) {
            return;
        }
        ringManager.waitForAll();
        releaseAllPageBuffersToPool();
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

        submitPreviousPage(page);
        ringManager.drainCqes();

        if (page < pages.size()) {
            long existingAddr = pages.getQuick(page);
            if (existingAddr != 0) {
                int state = page < pageStates.size() ? pageStates.getQuick(page) : -1;
                if (state == CONFIRMED) {
                    setPageState(page, WRITING);
                    return existingAddr;
                }
                if (state == WRITING) {
                    return existingAddr;
                }
            }
        }

        evictConfirmedPages(page);
        ensureFileSize(pageOffset(page) + getExtendSegmentSize());

        // Acquire buffer from pool.
        int bufIdx = pool.acquire();
        long addr = pool.address(bufIdx);

        // Pread existing file data if needed.
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
                    pool.release(bufIdx);
                    distressed = true;
                    cqeError = ff.errno();
                    throw CairoException.critical(cqeError)
                            .put("pread failed [fd=").put(fd).put(", offset=").put(pageStart).put(']');
                }
                if (bytesRead != readLen) {
                    pool.release(bufIdx);
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
        setPageBufferIndex(page, bufIdx);
        setPageDirtyStart(page, pageOffset(page));

        return addr;
    }

    @Override
    protected void release(long address) {
        if (address != 0) {
            clearHotPage();
            if (pool != null) {
                pool.releaseByAddress(address);
            } else {
                Unsafe.free(address, getPageSize(), MemoryTag.NATIVE_TABLE_WAL_WRITER);
            }
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
        pageDirtyStarts.clear();
        pageBufferIndices.clear();
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

    private void ensureFileSize(long requiredSize) {
        if (requiredSize <= allocatedFileSize) {
            return;
        }
        long chunkSize = getExtendSegmentSize() * 4;
        long newSize = Math.max(requiredSize, allocatedFileSize + chunkSize);
        newSize = ((newSize + getExtendSegmentSize() - 1) / getExtendSegmentSize()) * getExtendSegmentSize();

        if (!ff.fallocateKeepSize(fd, allocatedFileSize, newSize - allocatedFileSize)) {
            LOG.info().$("fallocateKeepSize failed, falling back to allocate [fd=").$(fd).$(']').$();
            if (!ff.allocate(fd, newSize)) {
                throw CairoException.critical(ff.errno())
                        .put("Cannot extend file fd=").put(fd).put(" to ").put(newSize);
            }
        }
        allocatedFileSize = newSize;
    }

    private void flushAllWritingPages() {
        long appendOffset = getAppendOffset();
        for (int i = 0, n = Math.min(pages.size(), pageStates.size()); i < n; i++) {
            long addr = pages.getQuick(i);
            if (addr != 0 && pageStates.getQuick(i) == WRITING) {
                long dirtyStart = getPageDirtyStart(i);
                long fileOffset = pageOffset(i);
                long pageEnd = fileOffset + getExtendSegmentSize();
                long writeEnd;
                if (appendOffset <= fileOffset) {
                    continue;
                } else if (appendOffset >= pageEnd) {
                    writeEnd = pageEnd;
                } else {
                    writeEnd = appendOffset;
                }
                long bufOffset = dirtyStart - fileOffset;
                int writeLen = (int) (writeEnd - dirtyStart);
                if (writeLen <= 0) {
                    continue;
                }
                ensureFileSize(dirtyStart + writeLen);
                int bufIdx = getPageBufferIndex(i);
                ringManager.enqueueWrite(columnSlot, i, bufIdx, fd, dirtyStart, addr + bufOffset, writeLen);
                setPageExpectedLen(i, writeLen);
                setPageState(i, SUBMITTED);
            }
        }
    }

    private int getPageBufferIndex(int page) {
        if (page < pageBufferIndices.size()) {
            return pageBufferIndices.getQuick(page);
        }
        return -1;
    }

    private long getPageDirtyStart(int page) {
        if (page < pageDirtyStarts.size()) {
            return pageDirtyStarts.getQuick(page);
        }
        return pageOffset(page);
    }

    private long preadPage(int page) {
        long pageSize = getExtendSegmentSize();
        int bufIdx = pool.acquire();
        long buf = pool.address(bufIdx);
        long fileOffset = pageOffset(page);
        long bytesRead = ff.read(fd, buf, pageSize, fileOffset);
        if (bytesRead < 0) {
            pool.release(bufIdx);
            throw CairoException.critical(ff.errno())
                    .put("pread failed [fd=").put(fd).put(", offset=").put(fileOffset).put(']');
        }
        cachePageAddress(page, buf);
        setPageState(page, CONFIRMED);
        setPageBufferIndex(page, bufIdx);
        return buf;
    }

    /**
     * Release all page buffers back to the pool before super.close() frees them.
     * We release by index (O(1)) where available, then zero the pages list
     * so that super.close() / releaseAllPagesButFirst() won't double-free.
     */
    private void releaseAllPageBuffersToPool() {
        if (pool == null) {
            return;
        }
        for (int i = 0, n = pages.size(); i < n; i++) {
            long addr = pages.getQuick(i);
            if (addr != 0) {
                int bufIdx = getPageBufferIndex(i);
                if (bufIdx >= 0) {
                    pool.release(bufIdx);
                } else {
                    pool.releaseByAddress(addr);
                }
                pages.setQuick(i, 0);
            }
        }
    }

    private void setPageBufferIndex(int page, int bufIdx) {
        while (pageBufferIndices.size() <= page) {
            pageBufferIndices.add(-1);
        }
        pageBufferIndices.setQuick(page, bufIdx);
    }

    private void setPageDirtyStart(int page, long offset) {
        while (pageDirtyStarts.size() <= page) {
            pageDirtyStarts.add(0);
        }
        pageDirtyStarts.setQuick(page, offset);
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
        long dirtyStart = getPageDirtyStart(currentPage);
        long pageStart = pageOffset(currentPage);
        long bufOffset = dirtyStart - pageStart;
        int dirtyLen = (int) (appendOffset - dirtyStart);
        if (dirtyLen <= 0) {
            return;
        }
        ensureFileSize(dirtyStart + dirtyLen);
        int bufIdx = getPageBufferIndex(currentPage);
        ringManager.enqueueWrite(columnSlot, currentPage, bufIdx, fd, dirtyStart, addr + bufOffset, dirtyLen);
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
                long dirtyStart = getPageDirtyStart(prevPage);
                long fileOffset = pageOffset(prevPage);
                long pageSize = getExtendSegmentSize();
                long bufOffset = dirtyStart - fileOffset;
                int writeLen = (int) (fileOffset + pageSize - dirtyStart);
                if (writeLen > 0) {
                    int bufIdx = getPageBufferIndex(prevPage);
                    ringManager.enqueueWrite(columnSlot, prevPage, bufIdx, fd, dirtyStart, prevAddr + bufOffset, writeLen);
                    setPageExpectedLen(prevPage, writeLen);
                    setPageState(prevPage, SUBMITTED);
                }
            }
        }
    }

    private void syncSync() {
        submitCurrentPageDirtyRange();
        ringManager.waitForAll();
        lastSyncedAppendOffset = getAppendOffset();
        ringManager.enqueueFsync(columnSlot, fd);
        ringManager.waitForAll();
        int activePage = getAppendOffset() > 0 ? pageIndex(getAppendOffset() - 1) : -1;
        evictConfirmedPages(activePage);
        if (activePage > -1) {
            setPageState(activePage, WRITING);
            // Reset dirty start after sync — the whole page is on disk.
            setPageDirtyStart(activePage, pageOffset(activePage));
        }
    }

    /**
     * Update hot-path write pointers after swapping the page buffer.
     * Mirrors the logic in MemoryPARWImpl.jumpTo0() / updateLimits().
     */
    private void updateWritePointers(int page, long newAddr, long offsetInPage) {
        long pageSize = getExtendSegmentSize();
        pageLo = newAddr - 1;
        pageHi = newAddr + pageSize;
        baseOffset = pageOffset(page + 1) - pageHi;
        appendPointer = newAddr + offsetInPage;
        computeHotPage(page, newAddr);
    }
}
