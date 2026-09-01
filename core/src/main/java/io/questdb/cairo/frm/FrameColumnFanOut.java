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

package io.questdb.cairo.frm;

import io.questdb.MessageBus;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.TableWriter;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.RingQueue;
import io.questdb.mp.SOUnboundedCountDownLatch;
import io.questdb.mp.Sequence;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.tasks.ColumnTask;
import org.jetbrains.annotations.Nullable;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Runs one {@link FrameAlgebra} operation's per-column work across the shared column-task pool, instead
 * of one column after another on the calling thread. Every column of a frame writes its own files, at
 * the same row offset, so the columns of a single append or merge have nothing to say to each other -
 * which is what makes the loop worth splitting in the first place.
 * <p>
 * One instance belongs to one writable {@link Frame} and runs one operation at a time, so its state
 * needs no lock of its own. The work goes out as {@link ColumnTask}s on {@link MessageBus}'s shared
 * column-task queue - the same queue and the same work-stealing wait
 * {@link TableWriter#dispatchColumnTasks} uses for WAL lag merges - and the call returns only once
 * every column has finished.
 * <p>
 * The operation runs in three phases, and only the middle one is parallel:
 * <ol>
 *     <li>OPEN, on the calling thread. A frame builds a column's file path by appending the column name
 *     to the ONE {@code Path} it owns and trimming it back afterwards, so two columns cannot open at the
 *     same time. This phase also settles every read of the frame's shared, non-thread-safe state - its
 *     metadata, its column-version view, its tracked tops.</li>
 *     <li>COPY, one task per column. Each task touches only its own two or three {@link FrameColumn}s:
 *     its own file descriptors, its own mapping, its own posting-index writer.</li>
 *     <li>REPORT and CLOSE, on the calling thread again. {@link Frame#saveChanges} lands each column's
 *     top and the columns go back to the pool.</li>
 * </ol>
 * Holding a whole batch of columns open across the copy, rather than two at a time, is what the middle
 * phase costs - the same shape the classic per-column O3 rewrite already has, where
 * {@code O3OpenColumnJob} opens one task per column too. A table wider than {@code MAX_OPEN_COLUMNS}
 * runs the three phases once per batch, which is what keeps that cost bounded.
 */
public class FrameColumnFanOut implements TableWriter.ColumnTaskHandler {
    /**
     * How many columns are held open at once. The copy runs while every column of a batch is open, so
     * this is what bounds the file descriptors and mappings an operation adds - a wide table would
     * otherwise hold every one of its columns open at the same time. Comfortably wider than any worker
     * pool, so batching costs parallelism only on tables far wider than this, and it fits the shared
     * column-task queue (128 slots by default) even with another writer dispatching alongside.
     */
    private static final int MAX_OPEN_COLUMNS = 64;
    private static final Log LOG = LogFactory.getLog(FrameColumnFanOut.class);
    private final SOUnboundedCountDownLatch doneLatch = new SOUnboundedCountDownLatch();
    private final AtomicInteger errorCount = new AtomicInteger();
    private final MessageBus messageBus;
    private final ObjList<FrameColumn> source1Columns = new ObjList<>();
    private final ObjList<FrameColumn> source2Columns = new ObjList<>();
    private final ObjList<FrameColumn> targetColumns = new ObjList<>();
    private int commitMode;
    private volatile Throwable error;
    private boolean isMerge;
    private long mergeIndexAddr;
    private long mergeIndexRows;
    private long source1Hi;
    private long source1Lo;
    private long source2Hi;
    private long source2Lo;
    private long targetRowCount;
    private long upcomingTableTxn;

    public FrameColumnFanOut(MessageBus messageBus) {
        this.messageBus = messageBus;
    }

    /**
     * Parallel counterpart of {@link FrameAlgebra#append}'s per-column loop. Same bounds, same result,
     * same reporting through {@link Frame#saveChanges} - only the copy itself is spread out.
     */
    public void append(Frame target, Frame source, long sourceLo, long sourceHi, long upcomingTableTxn, int commitMode) {
        this.isMerge = false;
        this.source1Lo = sourceLo;
        this.source1Hi = sourceHi;
        this.upcomingTableTxn = upcomingTableTxn;
        this.commitMode = commitMode;
        execute(target, source, null);
    }

    /**
     * Whether it is worth handing {@code columnCount} columns to the pool at all. A frame with a single
     * column has nothing to spread, and without a bus there is nowhere to spread it to.
     * <p>
     * There is deliberately no row-count floor. Publishing and stealing back one task costs a couple of
     * hundred nanoseconds per column, against the file open, {@code fstat} and mapping every column of
     * this operation already pays for on the calling thread - so even a one-row append spends a small
     * fraction of what it was going to spend anyway, and every operation, large or small, takes the same
     * code path.
     */
    public boolean isWorthwhile(int columnCount) {
        return messageBus != null && columnCount > 1;
    }

    /**
     * Parallel counterpart of {@link FrameAlgebra#merge}'s per-column loop.
     */
    public void merge(
            Frame target,
            Frame source1,
            long source1Lo,
            long source1Hi,
            Frame source2,
            long source2Lo,
            long source2Hi,
            long mergeIndexAddr,
            long mergeIndexRows,
            long upcomingTableTxn,
            int commitMode
    ) {
        this.isMerge = true;
        this.source1Lo = source1Lo;
        this.source1Hi = source1Hi;
        this.source2Lo = source2Lo;
        this.source2Hi = source2Hi;
        this.mergeIndexAddr = mergeIndexAddr;
        this.mergeIndexRows = mergeIndexRows;
        this.upcomingTableTxn = upcomingTableTxn;
        this.commitMode = commitMode;
        execute(target, source1, source2);
    }

    /**
     * One column's share of the operation. Everything it reads was settled by the OPEN phase and
     * everything it writes belongs to this column alone.
     */
    @Override
    public void run(
            int columnIndex,
            int columnType,
            long timestampColumnIndex,
            long long0,
            long long1,
            long long2,
            long long3,
            long long4
    ) {
        if (errorCount.get() > 0) {
            // Another column already failed and the operation is going to be abandoned, so there is no
            // point writing more bytes into a partition nobody will publish.
            return;
        }
        try {
            final FrameColumn targetColumn = targetColumns.getQuick(columnIndex);
            targetColumn.setUpcomingTableTxn(upcomingTableTxn);
            if (isMerge) {
                targetColumn.merge(
                        targetRowCount,
                        source1Columns.getQuick(columnIndex),
                        source1Lo,
                        source1Hi,
                        source2Columns.getQuick(columnIndex),
                        source2Lo,
                        source2Hi,
                        mergeIndexAddr,
                        mergeIndexRows,
                        commitMode
                );
            } else {
                FrameAlgebra.append(
                        targetColumn,
                        targetRowCount,
                        source1Columns.getQuick(columnIndex),
                        source1Lo,
                        source1Hi,
                        commitMode
                );
            }
        } catch (Throwable th) {
            onError(columnIndex, th);
        }
    }

    private void closeColumns(int columnLo, int columnHi) {
        for (int i = columnLo; i < columnHi; i++) {
            targetColumns.setQuick(i, Misc.free(targetColumns.getQuick(i)));
            source1Columns.setQuick(i, Misc.free(source1Columns.getQuick(i)));
            source2Columns.setQuick(i, Misc.free(source2Columns.getQuick(i)));
        }
    }

    private void dispatchColumns(int columnLo, int columnHi) {
        final Sequence pubSeq = messageBus.getColumnTaskPubSeq();
        final RingQueue<ColumnTask> queue = messageBus.getColumnTaskQueue();
        doneLatch.reset();
        int queuedCount = 0;
        for (int i = columnLo; i < columnHi; i++) {
            if (!isLiveColumn(i)) {
                continue;
            }
            final long cursor = pubSeq.next();
            if (cursor > -1) {
                try {
                    // Only the column index travels in the task: the frames and the bounds are this
                    // object's own fields, and this object IS the handler the task carries.
                    queue.get(cursor).of(doneLatch, i, 0, 0, 0, 0, 0, 0, 0, this);
                } finally {
                    queuedCount++;
                    pubSeq.done(cursor);
                }
            } else {
                // Queue full. Run the column here rather than wait for room, the same way
                // TableWriter#dispatchColumnTasks does - and this is also what makes progress
                // guaranteed when nothing else is draining the queue.
                run(i, 0, 0, 0, 0, 0, 0, 0);
            }
        }
        // Work stealing: the calling thread runs whatever it can reach, including tasks other writers
        // published, until every task of THIS operation has counted down.
        TableWriter.consumeColumnTasks0(queue, queuedCount, messageBus.getColumnTaskSubSeq(), doneLatch);
    }

    private void execute(Frame target, Frame source1, @Nullable Frame source2) {
        final int columnCount = source1.columnCount();
        errorCount.set(0);
        error = null;
        targetRowCount = target.getRowCount();
        targetColumns.setAll(columnCount, null);
        source1Columns.setAll(columnCount, null);
        source2Columns.setAll(columnCount, null);
        try {
            for (int columnLo = 0; columnLo < columnCount; columnLo += MAX_OPEN_COLUMNS) {
                final int columnHi = Math.min(columnLo + MAX_OPEN_COLUMNS, columnCount);
                try {
                    openColumns(target, source1, source2, columnLo, columnHi);
                    dispatchColumns(columnLo, columnHi);
                    throwOnError();
                    for (int i = columnLo; i < columnHi; i++) {
                        if (isLiveColumn(i)) {
                            target.saveChanges(targetColumns.getQuick(i));
                        }
                    }
                } finally {
                    closeColumns(columnLo, columnHi);
                }
            }
        } finally {
            // Nothing is open by now - every batch closed its own - so this only drops the references.
            targetColumns.clear();
            source1Columns.clear();
            source2Columns.clear();
        }
    }

    private boolean isLiveColumn(int columnIndex) {
        return source1Columns.getQuick(columnIndex).getColumnType() >= 0;
    }

    private void onError(int columnIndex, Throwable th) {
        LOG.error().$("frame column task failed [columnIndex=").$(columnIndex)
                .$(", error=").$(th)
                .I$();
        if (errorCount.getAndIncrement() == 0) {
            error = th;
        }
    }

    /**
     * Opens one batch of columns up front, on the calling thread - see the class doc for why this
     * cannot overlap with the copy. A throw part-way leaves whatever opened so far in the lists for
     * {@link #closeColumns} to release.
     */
    private void openColumns(Frame target, Frame source1, @Nullable Frame source2, int columnLo, int columnHi) {
        for (int i = columnLo; i < columnHi; i++) {
            source1Columns.setQuick(i, source1.createColumn(i));
            if (!isLiveColumn(i)) {
                // A dropped column: the serial loop skips it as well, and neither the source nor the
                // target opens a file for it.
                continue;
            }
            if (source2 != null) {
                source2Columns.setQuick(i, source2.createColumn(i));
            }
            targetColumns.setQuick(i, target.createColumn(i));
        }
    }

    private void throwOnError() {
        final Throwable th = error;
        if (th != null) {
            error = null;
            // Rethrown as it was raised, so a caller that already distinguishes a CairoException from a
            // CairoError - o3 failure handling does - keeps seeing what the serial loop showed it.
            if (th instanceof RuntimeException re) {
                throw re;
            }
            if (th instanceof Error err) {
                throw err;
            }
            throw CairoException.critical(0).put("frame column task failed [error=").put(th.getMessage()).put(']');
        }
    }
}
