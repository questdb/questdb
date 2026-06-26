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

package io.questdb.cairo.vm.api;

import io.questdb.cairo.vm.Vm;
import io.questdb.std.FilesFacade;
import io.questdb.std.str.LPSZ;

// mapped appendable
public interface MemoryMA extends MemoryM, MemoryA {

    default void close(boolean truncate) {
        close(truncate, Vm.TRUNCATE_TO_PAGE);
    }

    void close(boolean truncate, byte truncateMode);

    long getAppendAddress();

    long getAppendAddressSize();

    void of(FilesFacade ff, LPSZ name, long extendSegmentSize, int memoryTag, int opts);

    /**
     * Marks this memory as strictly append-only so that {@link #sync(boolean)} may narrow the
     * msync to the written range (and skip when nothing new was appended) instead of flushing the
     * full mapped extent. Only safe for memories whose writes are exclusively appends
     * ({@code put*(value)} / {@code jumpTo} / {@code truncate}); memories that perform in-place
     * {@code put*(offset, value)} updates below the high-water mark must stay full-extent (default).
     * Default is a no-op so implementations that always full-sync are unaffected.
     */
    default void setAppendOnly(boolean appendOnly) {
    }

    /**
     * Marks this append memory as an ADAPTIVE LAZY-APPLY column: its page-release / close path skips
     * the per-page {@code msync} that would otherwise make the column durable on the apply side. Used
     * ONLY for TABLE PARTITION columns under {@link io.questdb.cairo.CommitMode#ADAPTIVE}, where the
     * materialized column is a rebuildable cache of the durable WAL (durability comes from the epoch +
     * recovery roll-forward, Plan 3). It is NOT set for WAL segment columns (whose durability is an
     * explicit per-commit fdatasync) nor under any other commit mode, so those paths are unaffected.
     * Default is a no-op so implementations that do not flush on release are unaffected.
     */
    default void setApplyLazy(boolean applyLazy) {
    }

    default void setSize(long size) {
        jumpTo(size);
    }

    void switchTo(FilesFacade ff, long fd, long extendSegmentSize, long offset, boolean truncate, byte truncateMode);

    void sync(boolean async);

    /**
     * Phase 1 of the batched SYNC-mode flush (Linux only). Pushes this memory's dirty mmap pages to the
     * page cache via {@code msync(MS_ASYNC)} over the written range so that a subsequent
     * {@link #syncFlushDrain()} ({@code sync_file_range}) can see them. Does NOT issue a device flush and
     * MUST NOT advance any {@code lastSynced*} watermark (that is reserved for
     * {@link #syncFlushFinishIfExtended()}): advancing it here would let the extend check there wrongly skip
     * the {@code fdatasync} that journals the inode size, silently losing the extend on a crash.
     * <p>
     * Default keeps current behavior: a no-op. The full content durability is then provided entirely by the
     * fallback {@code sync(false)} in {@link #syncFlushFinishIfExtended()}, so memories that do not override
     * the 3-phase API remain byte-identical to the original per-file {@code sync(false)}.
     */
    default void syncFlushKick() {
    }

    /**
     * Phase 2 of the batched SYNC-mode flush (Linux only). Writes this memory's (already page-cache-dirty,
     * see {@link #syncFlushKick()}) pages back to the device's write cache via
     * {@code sync_file_range(WRITE | WAIT_AFTER)} and WAITS for that writeback to complete. Issues NO device
     * flush — the content sits in the device cache until a later device flush (the {@code _cv} commit's
     * {@code msync(MS_SYNC)}) makes it (and everything else in the device cache) durable. MUST NOT advance
     * any {@code lastSynced*} watermark, for the same reason as {@link #syncFlushKick()}.
     * <p>
     * Default keeps current behavior: a no-op (see {@link #syncFlushKick()}).
     */
    default void syncFlushDrain() {
    }

    /**
     * Phase 3 of the batched SYNC-mode flush, run AFTER the caller's single {@code syncfs(fd)} (see
     * {@code TableWriter.syncColumnsBatchedSync}). For memories that participated in
     * {@link #syncFlushKick()}/{@link #syncFlushDrain()}, that {@code syncfs} has already written back their
     * data and journaled the whole filesystem's metadata — including this inode's new size and any ext4
     * unwritten-&gt;written extent conversions — in one device flush. So those implementations issue NO
     * per-file {@code fdatasync} here (it would be a redundant second device flush); they only ADVANCE their
     * {@code lastSynced*} watermark to match the now-durable size, so a subsequent {@link #sync(boolean)}
     * correctly sees no further extend.
     * <p>
     * Default keeps the conservative behavior: a full {@link #sync(boolean) sync(false)} ({@code msync(MS_SYNC)}
     * + fdatasync-on-extend). This is the fallback for memories that did NOT push their content to the page
     * cache via {@link #syncFlushKick()}/{@link #syncFlushDrain()} (both no-ops by default) and are therefore
     * NOT covered by the batch's {@code syncfs} — they must still self-flush to be durable.
     */
    default void syncFlushFinishIfExtended() {
        sync(false);
    }

    default void toTop() {
        jumpTo(0);
    }
}
