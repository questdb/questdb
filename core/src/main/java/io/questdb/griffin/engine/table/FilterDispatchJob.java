/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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

package io.questdb.griffin.engine.table;

import io.questdb.MessageBus;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.PageAddressCache;
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.mp.*;
import io.questdb.std.DirectLongList;
import io.questdb.std.LongList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.tasks.FilterDispatchTask;
import io.questdb.tasks.PageFrameTask;

import java.util.Deque;

public class FilterDispatchJob implements Job {

    private final MessageBus bus;
    private final MCSequence dispatchSubSeq;
    private final RingQueue<FilterDispatchTask> dispatchQueue;

    public FilterDispatchJob(MessageBus messageBus) {
        this.bus = messageBus;
        this.dispatchSubSeq = messageBus.getFilterDispatchSubSeq();
        this.dispatchQueue = messageBus.getFilterDispatchQueue();
    }

    @Override
    public boolean run(int workerId) {
        final long dispatchCursor = dispatchSubSeq.next();
        if (dispatchCursor > 0) {
            final FilterDispatchTask tsk = dispatchQueue.get(dispatchCursor);
            final int shard = tsk.getShard();
            final SCSequence recycleSubSeq = tsk.getRecycleSubSeq();
            final PageAddressCache pageAddressCache = tsk.getPageAddressCache();
            final int frameCount = tsk.getFrameCount();
            final SymbolTableSource symbolTableSource = tsk.getSymbolTableSource();
            final long producerId = tsk.getProducerId();
            final Deque<DirectLongList> rowsListStack = tsk.getRowsListStack();
            final LongList frameRowCounts = tsk.getFrameRowCounts();
            final Function filter = tsk.getFilter();

            final RingQueue<PageFrameTask> queue = bus.getPageFrameQueue(shard);
            // publisher sequence to pass work to worker jobs
            final MPSequence pubSeq = bus.getPageFramePubSeq(shard);

            // The recycle subscriber sequence should pick up queue items that
            // has been released by the user code. The motivation is to place used
            // row lists back on stack to be reused
            final FanOut recycleFanOut = bus.getPageFrameRecycleFanOut(shard);
            recycleFanOut.and(recycleSubSeq);

            long cursor;
            long listsReconciled = 0;
            for (int i = 0; i < frameCount; i++) {
                // We cannot process work on this thread. If we do the consumer will
                // never get the executions results. Consumer only picks ready to go
                // tasks from the queue.
                cursor = pubSeq.nextBully();
                queue.get(cursor).of(
                        producerId,
                        pageAddressCache,
                        i,
                        frameCount,
                        frameRowCounts.getQuick(i),
                        symbolTableSource,
                        filter,
                        popRows(rowsListStack)
                );

                pubSeq.done(cursor);
                cursor = recycleSubSeq.next();

                if (cursor > -1) {
                    reconcileRowList(recycleSubSeq, producerId, rowsListStack, queue, cursor);
                    listsReconciled++;
                }
            }

            while (listsReconciled < frameCount) {
                reconcileRowList(recycleSubSeq, producerId, rowsListStack, queue, recycleSubSeq.nextBully());
                listsReconciled++;
            }

            // detach our sequence from the recycle fanout, we no longer need it
            recycleFanOut.remove(recycleSubSeq);

            // at this point all results are consumed (we received all the recycle events)
            // it is safe to clear the page cache
            pageAddressCache.clear();
            frameRowCounts.clear();
            Misc.free(symbolTableSource);
            dispatchSubSeq.done(dispatchCursor);

            // todo: tidy up rows dequeue

            return true;
        }
        return false;
    }

    private static DirectLongList popRows(Deque<DirectLongList> deque) {
        DirectLongList result = deque.getFirst();
        if (result == null) {
            result = new DirectLongList(1024, MemoryTag.NATIVE_LONG_LIST);
        }
        return result;
    }

    private void reconcileRowList(
            SCSequence subSeq,
            long producerId,
            Deque<DirectLongList> listDeque,
            RingQueue<PageFrameTask> queue,
            long cursor
    ) {
        final PageFrameTask tsk = queue.get(cursor);
        // each producer will see "returns" for all active producers on this
        // queue, therefore we will only grab our own row lists to avoid potential
        // imbalance between producers. By imbalance, I mean one producer could be
        // allocating lists while another could be stashing it on its dequeue.
        if (tsk.getProducerId() == producerId) {
            listDeque.push(tsk.getRows());
        }
        subSeq.done(cursor);
    }

}
