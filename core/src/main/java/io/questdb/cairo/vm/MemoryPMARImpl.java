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

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.CommitMode;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.vm.api.MemoryMAR;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.Files;
import io.questdb.std.FilesFacade;
import io.questdb.std.Os;
import io.questdb.std.str.LPSZ;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

// paged mapped appendable readable
public class MemoryPMARImpl extends MemoryPARWImpl implements MemoryMAR {
    private static final Log LOG = LogFactory.getLog(MemoryPMARImpl.class);
    private final CairoConfiguration configuration;
    // When true, this memory is append-only, so sync() narrows the current-page msync to the bytes written
    // into that page. sync() never skips, so no offset state carries across syncs. Set only on ADAPTIVE WAL
    // columns and ADAPTIVE table columns, so it also picks the page-release grade.
    private boolean appendOnly = false;
    // When true, this is a table partition column under CommitMode.ADAPTIVE: page release and close skip
    // the msync, because the column is a rebuildable cache of the durable WAL, made durable by the save
    // point and recovery. TableWriter sets it only for partition columns under ADAPTIVE, never for WAL
    // segment columns, which fdatasync on each commit.
    private boolean applyLazy = false;
    // The commit mode threaded in by the opening caller (TableWriter.configureColumn), mirroring
    // applyLazy. CommitMode.UNSET (default) => never threaded, so read the instance-global
    // configuration.getCommitMode() instead. TableWriter threads its own grade because a table that is
    // not yet enrolled in adaptive runs at SYNC while the instance runs ADAPTIVE.
    private int commitMode = CommitMode.UNSET;
    private long fd = -1;
    private FilesFacade ff;
    private int madviseOpts = -1;
    private int mappedPage;
    private long pageAddress = 0;

    @TestOnly
    public MemoryPMARImpl(FilesFacade ff, LPSZ name, long pageSize, int memoryTag, int opts) {
        this(null);
        of(ff, name, pageSize, 0, memoryTag, opts, -1);
    }

    public MemoryPMARImpl(@Nullable CairoConfiguration configuration) {
        this.configuration = configuration;
    }

    public final void close(boolean truncate, byte truncateMode) {
        long sz = truncate ? getAppendOffset() : -1L;
        // releaseCurrentPage() msyncs under every mode but NOSYNC, so it can fail (a simulated crash, or EIO).
        // It unmaps the page itself, but the rest of close must still run: otherwise `fd` stays set on an
        // object the caller has detached, and the descriptor leaks or a later cleanup closes it twice. The
        // fault is rethrown after the frees.
        Throwable closeError = null;
        try {
            releaseCurrentPage();
        } catch (Throwable th) {
            closeError = th;
        }
        try {
            super.close();
            if (fd != -1) {
                try {
                    Vm.bestEffortClose(ff, LOG, fd, sz, truncateMode);
                } finally {
                    fd = -1;
                }
            }
        } catch (Throwable th) {
            if (closeError == null) {
                closeError = th;
            }
        }
        CairoException.rethrowCleanupFailure(closeError);
    }

    @Override
    public void close() {
        close(true);
    }

    @Override
    public long detachFdClose() {
        long fd = this.fd;
        this.fd = -1;
        close(false);
        return fd;
    }

    @Override
    public void flushFile(boolean barrier) {
        if (pageAddress != 0 && !ff.isSyncfsFileSystemWide()) {
            // Off Linux, write the mapped page into the file before the flush (see MemoryMA.flushFile), over the
            // range sync(true) used to msync.
            final long len = appendOnly ? inPageWritten() : getPageSize();
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

    public long getFd() {
        return fd;
    }

    @Override
    public FilesFacade getFilesFacade() {
        return ff;
    }

    @Override
    public long getPageAddress(int page) {
        if (page == mappedPage) {
            return pageAddress;
        }
        return 0L;
    }

    public long mapPage(int page) {
        // set page to "not mapped" in case mapping fails
        final long address = TableUtils.mapRW(ff, fd, getExtendSegmentSize(), pageOffset(page), memoryTag);
        mappedPage = page;
        ff.madvise(address, getExtendSegmentSize(), madviseOpts);
        return address;
    }

    @Override
    public final void of(FilesFacade ff, LPSZ name, long extendSegmentSize, int memoryTag, int opts) {
        of(ff, name, extendSegmentSize, 0, memoryTag, opts, -1);
    }

    @Override
    public void of(FilesFacade ff, LPSZ name, long extendSegmentSize, long size, int memoryTag, int opts) {
        of(ff, name, extendSegmentSize, memoryTag, opts);
    }

    @Override
    public void of(FilesFacade ff, LPSZ name, long extendSegmentSize, long size, int memoryTag, int opts, int madviseOpts) {
        close();
        this.memoryTag = memoryTag;
        this.madviseOpts = madviseOpts;
        this.ff = ff;
        mappedPage = -1;
        setExtendSegmentSize(extendSegmentSize);
        fd = TableUtils.openFileRWOrFail(ff, name, opts);
        LOG.debug().$("open ").$(name).$(" [fd=").$(fd).$(", extendSegmentSize=").$(extendSegmentSize).$(']').$();
    }

    @Override
    public void setAppendOnly(boolean appendOnly) {
        this.appendOnly = appendOnly;
    }

    @Override
    public void setApplyLazy(boolean applyLazy) {
        this.applyLazy = applyLazy;
    }

    @Override
    public void setCommitMode(int commitMode) {
        this.commitMode = commitMode;
    }

    @Override
    public void switchTo(FilesFacade ff, long fd, long extendSegmentSize, long offset, boolean truncate, byte truncateMode) {
        this.ff = ff;
        setExtendSegmentSize(extendSegmentSize);
        close(truncate, truncateMode);
        this.fd = fd;
        jumpTo(offset);
    }

    public void sync(boolean async) {
        // An async sync is a no-op: msync(MS_ASYNC) has been a no-op on Linux since 2.6.19, and the
        // Windows msync ignores the flag and flushes synchronously. The OS writes dirty pages back
        // on its own.
        if (!async && pageAddress != 0) {
            if (appendOnly) {
                // Flush only the bytes written into the mapped page: the active partition's partial last page
                // is synced on every commit, and most of it is clean. The length comes from the live append
                // offset on each call (see inPageWritten()), so a jumpTo or rollback cannot make it stale. The
                // page-flip release() msyncs the whole page it releases (MS_ASYNC for an ADAPTIVE WAL column on
                // Linux, whose commit fdatasyncs the file).
                final long inPageWritten = inPageWritten();
                if (inPageWritten > 0) {
                    ff.msync(pageAddress, inPageWritten, false);
                }
            } else {
                ff.msync(pageAddress, getPageSize(), false);
            }
        }
    }

    @Override
    public void syncFlushDrain() {
        // Batched flush, stage 2: sync_file_range(WRITE | WAIT_AFTER) waits for the writeback stage 1 started
        // and writes back anything it left; no device flush. Waiting reports a writeback error, as in
        // MemoryCMARWImpl.
        if (syncFlushRange(Files.SYNC_FILE_RANGE_WRITE | Files.SYNC_FILE_RANGE_WAIT_AFTER) != 0) {
            throw CairoException.dataSyncFailureOfFd(ff, fd, ff.errno(), "sync_file_range", "could not sync_file_range");
        }
    }

    @Override
    public void syncFlushFinishIfExtended() {
        // Batched flush, stage 3: nothing to do. The caller (TableWriter.syncColumnsBatchedSync) has already
        // issued one syncfs over the table's filesystem, which journaled the new size and extent conversions
        // in one device flush. Unlike MemoryCMARWImpl there is no watermark to advance: sync() recomputes the
        // dirty range from the live append offset and never skips. A skip fast path would need a watermark
        // advanced here, never in syncFlushKick() or syncFlushDrain().
    }

    @Override
    public void syncFlushKick() {
        // Batched flush, stage 1: sync_file_range(WRITE) starts writeback of the current page's dirty bytes and
        // returns, so every file's writeback runs at once before stage 2 waits. No device flush. A transient
        // failure is left to stage 2 and the caller's flush, as in MemoryCMARWImpl; any other is a flush failure.
        if (syncFlushRange(Files.SYNC_FILE_RANGE_WRITE) != 0) {
            Vm.checkWritebackKick(ff, fd, ff.errno());
        }
    }

    @Override
    public void truncate() {
        if (fd == -1) {
            // are we closed ?
            return;
        }
        releaseCurrentPage();
        if (!ff.truncate(Math.abs(fd), getExtendSegmentSize())) {
            throw CairoException.critical(ff.errno()).put("Cannot truncate fd=").put(fd).put(" to ").put(getExtendSegmentSize()).put(" bytes");
        }
        updateLimits(0, pageAddress = mapPage(0));
        LOG.debug().$("truncated [fd=").$(fd).$(']').$();
    }

    @Override
    protected long mapWritePage(int page, long offset) {
        releaseCurrentPage();
        return pageAddress = mapPage(page);
    }

    /**
     * Bytes written into the currently mapped page, clamped to [0, pageSize]. The append cursor is always
     * within the mapped page (jumpTo remaps to the target page), so this is appendOffset - pageStart.
     * Recomputed from the live append offset on every call, so it cannot go stale.
     */
    private long inPageWritten() {
        long inPageWritten = getAppendOffset() - pageOffset(mappedPage);
        if (inPageWritten < 0) {
            return 0;
        }
        final long pageSize = getPageSize();
        return inPageWritten > pageSize ? pageSize : inPageWritten;
    }

    // The page sits at file offset pageOffset(mappedPage), so the range is [pageStart, pageStart + dirtyLen),
    // the bytes sync() selects.
    private int syncFlushRange(int flags) {
        if (pageAddress != 0) {
            final long pageStart = pageOffset(mappedPage);
            final long dirtyLen = appendOnly ? inPageWritten() : getPageSize();
            if (dirtyLen > 0) {
                return ff.syncFileRange(fd, pageStart, dirtyLen, flags);
            }
        }
        return 0;
    }

    @Override
    protected void release(long address) {
        if (address != 0) {
            // The munmap must run even if the page-release msync throws (a disk fault, or a simulated power
            // loss): reclaiming memory cannot depend on the sync. MemoryCMARWImpl.close likewise unmaps before
            // its fd sync. Otherwise a sync fault strands the mapped page until the process exits.
            try {
                // The mode set through setCommitMode(), or the instance-global mode when it is UNSET.
                int commitMode = this.commitMode != CommitMode.UNSET
                        ? this.commitMode
                        : (configuration != null ? configuration.getCommitMode() : CommitMode.NOSYNC);
                // applyLazy (an adaptive table partition column) skips the page-release msync: the column is a
                // rebuildable cache of the durable WAL, made durable by the save point and recovery. WAL
                // segment columns never set it. An ADAPTIVE WAL column (appendOnly) releases with MS_ASYNC on
                // Linux: the page stays dirty in the page cache, and the commit's fdatasync of this file writes
                // it back before the commit is sequenced. Elsewhere that fdatasync is not known to cover a page
                // after it is unmapped.
                if (commitMode != CommitMode.NOSYNC && !applyLazy) {
                    ff.msync(address, getPageSize(), commitMode == CommitMode.ASYNC
                            || (appendOnly && commitMode == CommitMode.ADAPTIVE && Os.isLinux()));
                }
            } finally {
                ff.munmap(address, getPageSize(), memoryTag);
            }
        }
    }

    void releaseCurrentPage() {
        if (pageAddress != 0) {
            // Clear pageAddress even if release() rethrows its msync fault (the page is unmapped in release()'s
            // finally), so a later close cannot double-munmap the same address.
            try {
                release(pageAddress);
            } finally {
                pageAddress = 0;
            }
        }
    }
}
