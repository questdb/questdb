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
     * Marks this memory as append-only, so {@link #sync(boolean)} may narrow the msync to the written range,
     * and skip it when nothing was appended, instead of flushing the full mapped extent. Only safe for
     * memories written exclusively by appends ({@code put*(value)}, {@code jumpTo}, {@code truncate});
     * memories that update in place with {@code put*(offset, value)} below the cursor must keep the
     * default. The default implementation does nothing.
     */
    default void setAppendOnly(boolean appendOnly) {
    }

    /**
     * Marks this append memory as an adaptive lazy-apply column: page release and close skip the per-page
     * {@code msync}. Used only for table partition columns under
     * {@link io.questdb.cairo.CommitMode#ADAPTIVE}, where the column is a rebuildable cache of the durable
     * WAL, made durable by the save point and recovery. WAL segment columns, which fdatasync on each commit,
     * and other commit modes never set it. The default implementation does nothing.
     */
    default void setApplyLazy(boolean applyLazy) {
    }

    /**
     * Passes the owning writer's commit mode ({@link io.questdb.cairo.CommitMode}) to this memory, so the
     * page-release and close flush match the grade that writer commits under: a table not yet enrolled in
     * adaptive runs at SYNC while the instance runs ADAPTIVE. {@link io.questdb.cairo.CommitMode#UNSET}, the
     * default, makes the memory use the instance-global mode.
     */
    default void setCommitMode(int commitMode) {
    }

    default void setSize(long size) {
        jumpTo(size);
    }

    void switchTo(FilesFacade ff, long fd, long extendSegmentSize, long offset, boolean truncate, byte truncateMode);

    void sync(boolean async);

    /**
     * Stage 2 of the batched flush (Linux only): waits, with {@code sync_file_range(WRITE | WAIT_AFTER)}, for
     * the writeback {@link #syncFlushKick()} started, and writes back what it left. Issues no device flush: the
     * caller's later {@code syncfs} or per-file {@code fdatasync} makes the content durable. Must not advance a
     * {@code lastSynced*} watermark, as in {@link #syncFlushKick()}. A writeback error is thrown as a data-sync
     * failure: Linux reports it once per open file, and this wait consumes it, so a later {@code fdatasync}
     * would not see it.
     * <p>
     * The default does nothing (see {@link #syncFlushKick()}).
     */
    default void syncFlushDrain() {
    }

    /**
     * Stage 3 of the batched flush, run after the caller's single {@code syncfs} (see
     * {@code TableWriter.syncColumnsBatchedSync}). For a memory that took part in {@link #syncFlushKick()} and
     * {@link #syncFlushDrain()}, that {@code syncfs} has already written back its data and journaled its new
     * size and extent conversions in one device flush, so an implementation issues no per-file
     * {@code fdatasync} here and only advances its {@code lastSynced*} watermark to the durable size.
     * <p>
     * The default runs a full {@link #sync(boolean) sync(false)}: a memory that does not implement the first
     * two stages is not covered by the batch's {@code syncfs} and must flush itself.
     */
    default void syncFlushFinishIfExtended() {
        sync(false);
    }

    /**
     * Stage 1 of the batched flush (Linux only): starts writeback of this memory's written range with
     * {@code sync_file_range(WRITE)} and returns without waiting, so the caller can start every file of the
     * batch before {@link #syncFlushDrain()} waits on any, and the writebacks overlap. Pages dirtied through the
     * mapping need no {@code msync} first: Linux tracks them as dirty. A speed aid only: it issues no device
     * flush and must not advance a {@code lastSynced*} watermark; only {@link #syncFlushFinishIfExtended()},
     * which runs once the data is durable, may do that.
     * <p>
     * The default does nothing; {@link #syncFlushFinishIfExtended()} then falls back to {@code sync(false)}.
     */
    default void syncFlushKick() {
    }

    default void toTop() {
        jumpTo(0);
    }
}
