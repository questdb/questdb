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
import io.questdb.std.str.LPSZ;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

// paged mapped appendable readable
public class MemoryPMARImpl extends MemoryPARWImpl implements MemoryMAR {
    private static final Log LOG = LogFactory.getLog(MemoryPMARImpl.class);
    private final CairoConfiguration configuration;
    // When true this memory is strictly append-only, so sync() narrows the current-page msync to the
    // bytes actually written into that page instead of flushing the whole page window. NARROW-ONLY:
    // we do not skip msync for PMAR (see sync()), so there is no cross-sync offset state to go stale.
    // Defaults to false: the non-appendOnly path is byte-identical to the original full-page msync.
    private boolean appendOnly = false;
    // When true this is a TABLE PARTITION column under CommitMode.ADAPTIVE: the page-release / close
    // path skips its msync so the materialized column stays NON-DURABLE on the apply side (lazy apply;
    // durability via the durable epoch + recovery roll-forward, Plan 3). Set ONLY by TableWriter for
    // partition columns under adaptive; never for WAL segment columns (explicit per-commit fdatasync)
    // nor under any other mode, so all those paths are byte-identical. Default false.
    private boolean applyLazy = false;
    // The per-table EFFECTIVE commit mode (CommitMode.*) threaded in by the opening caller
    // (TableWriter.configureColumn), mirroring applyLazy. CommitMode.UNSET (default) => defer to the
    // instance-global configuration.getCommitMode(), so any PMAR that is never threaded a mode (WAL
    // segment columns, the @TestOnly ctor) stays byte-identical to the original global-mode read. release()
    // uses THIS so a WITH commit_mode='sync' column on a nosync instance still msyncs its completed pages.
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
        // releaseCurrentPage() carries a durability barrier (msync) under any non-NOSYNC commit mode, so it
        // can fail -- on a simulated crash, or a genuine EIO. Its own finallys already unmap the page, but the
        // rest of this close must still complete: without it `fd` stays set on an object the caller has
        // already detached, so the descriptor is either leaked outright or closed a second time by a later
        // cleanup path. Fail-stop is preserved -- the fault is rethrown -- but only after the frees are done.
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
        if (closeError != null) {
            if (closeError instanceof Error) {
                throw (Error) closeError;
            }
            if (closeError instanceof RuntimeException) {
                throw (RuntimeException) closeError;
            }
            throw new CairoException().put("could not close memory [msg=").put(closeError.getMessage()).put(']');
        }
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
        if (pageAddress != 0) {
            if (appendOnly) {
                // NARROW: flush only the bytes written into the currently mapped page, not the whole
                // page window. The append cursor is always within the mapped page (jumpTo remaps to
                // the target page), so the in-page written length is appendOffset - pageStart, clamped
                // to [0, pageSize]. This is the main win: the active partition's partial last page is
                // synced on every commit, and most of it is unwritten/clean. We recompute the length
                // from the live append offset each call, so prior jumpTo/rollback cannot make it stale
                // (NARROW-ONLY: no msync skip for PMAR). The page-flip release() still msyncs the full
                // page it releases, so completed pages are fully flushed.
                final long inPageWritten = inPageWritten();
                if (inPageWritten > 0) {
                    ff.msync(pageAddress, inPageWritten, async);
                }
            } else {
                ff.msync(pageAddress, getPageSize(), async);
            }
        }
    }

    @Override
    public void syncFlushKick() {
        // Batched SYNC stage 1: msync(MS_ASYNC) the dirty bytes of the currently mapped page into the page
        // cache so the following sync_file_range can see them. Mirrors sync()'s range selection (append-only
        // narrows to the in-page written length; else the full page) but is always ASYNC and issues NO
        // fdatasync.
        if (pageAddress != 0) {
            if (appendOnly) {
                final long inPageWritten = inPageWritten();
                if (inPageWritten > 0) {
                    ff.msync(pageAddress, inPageWritten, true);
                }
            } else {
                ff.msync(pageAddress, getPageSize(), true);
            }
        }
    }

    @Override
    public void syncFlushDrain() {
        // Batched SYNC stage 2: sync_file_range(WRITE | WAIT_AFTER) writes the page-cache-dirty bytes of the
        // current page back to the device cache and WAITS. NO device flush, NO watermark mutation. PMAR maps
        // a single page at file offset pageOffset(mappedPage), so the fd-relative dirty range is
        // [pageStart, pageStart + dirtyLen). WAIT_AFTER is mandatory for durability (see CMARW).
        if (pageAddress != 0) {
            final long pageStart = pageOffset(mappedPage);
            final long dirtyLen = appendOnly ? inPageWritten() : getPageSize();
            if (dirtyLen > 0) {
                ff.syncFileRange(fd, pageStart, dirtyLen, Files.SYNC_FILE_RANGE_WRITE | Files.SYNC_FILE_RANGE_WAIT_AFTER);
            }
        }
    }

    @Override
    public void syncFlushFinishIfExtended() {
        // Batched SYNC stage 3: a genuine NO-OP for PMAR. The caller (TableWriter.syncColumnsBatchedSync)
        // has already issued ONE syncfs(fd) over this table's filesystem, journaling the new i_size +
        // extent conversions and flushing the device once, so a per-file fdatasync here would be a
        // redundant second flush.
        //
        // Unlike MemoryCMARWImpl there is deliberately NO watermark to advance: PMAR's sync() is
        // NARROW-ONLY (it recomputes the dirty range from the live append offset on every call and never
        // SKIPS), so it has no extend/skip check that a watermark could feed. An earlier revision kept a
        // `lastSyncedSize` field here; it was written by of()/switchTo()/truncate()/this method and read
        // by nothing that affected durability, so it was removed rather than left as misleading state.
        // If PMAR ever gains a skip fast path, reintroduce the watermark HERE (never in syncFlushKick /
        // syncFlushDrain — advancing it there would let this method wrongly conclude "no extend").
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
     * Recomputed from the live append offset on every call (NARROW-ONLY: no cross-call state to go stale).
     */
    private long inPageWritten() {
        long inPageWritten = getAppendOffset() - pageOffset(mappedPage);
        if (inPageWritten < 0) {
            return 0;
        }
        final long pageSize = getPageSize();
        return inPageWritten > pageSize ? pageSize : inPageWritten;
    }

    @Override
    protected void release(long address) {
        if (address != 0) {
            // The munmap MUST run even if the page-release msync throws (a genuine disk fault, or a power loss
            // the crash harness simulates by throwing on msync): memory reclamation cannot depend on the
            // durability sync succeeding. Doing the msync inside a try/finally-munmap mirrors
            // MemoryCMARWImpl.close (which unmaps before its fd sync); without it a sync fault here strands the
            // mapped page until process exit (the MMAP_TABLE_WAL_WRITER leak a crash mid drop-close produced).
            try {
                // Prefer the per-table EFFECTIVE mode threaded in via setCommitMode(); fall back to the
                // instance-global mode only when it was never set (UNSET), so untouched memories stay
                // byte-identical. A WITH commit_mode='sync' column on a nosync instance MUST msync its
                // completed pages here even though the global mode is NOSYNC.
                int commitMode = this.commitMode != CommitMode.UNSET
                        ? this.commitMode
                        : (configuration != null ? configuration.getCommitMode() : CommitMode.NOSYNC);
                // applyLazy (adaptive table partition column) skips the page-release msync — the column is
                // a rebuildable cache of the durable WAL, made durable only by the epoch + recovery. WAL
                // segment columns never set applyLazy, so their explicit per-commit fdatasync is unaffected.
                if (commitMode != CommitMode.NOSYNC && !applyLazy) {
                    ff.msync(address, getPageSize(), commitMode == CommitMode.ASYNC);
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
