/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2019 Appsicle
 *
 * This program is free software: you can redistribute it and/or  modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 ******************************************************************************/

package com.questdb.cairo;

import com.questdb.mp.Job;
import com.questdb.mp.RingQueue;
import com.questdb.mp.SOCountDownLatch;
import com.questdb.mp.Sequence;

class ColumnIndexerJob implements Job {
    private final RingQueue<ColumnIndexerEntry> queue;
    private final Sequence sequence;

    public ColumnIndexerJob(CairoWorkScheduler workScheduler) {
        this.queue = workScheduler.getIndexerQueue();
        this.sequence = workScheduler.getIndexerSubSequence();
    }

    @Override
    public boolean run() {
        long cursor = sequence.next();
        if (cursor < 0) {
            return false;
        }

        ColumnIndexerEntry queueItem = queue.get(cursor);
        // copy values and release queue item
        final ColumnIndexer indexer = queueItem.indexer;
        final long lo = queueItem.lo;
        final long hi = queueItem.hi;
        final long indexSequence = queueItem.sequence;
        final SOCountDownLatch latch = queueItem.countDownLatch;
        sequence.done(cursor);

        // On the face of it main thread could have consumed same sequence as
        // child workers. The reason it is undesirable is because all writers
        // share the same queue and main thread end up indexing content for other writers.
        // Using CAS allows main thread to steal only parts of its own job.
        if (indexer.tryLock(indexSequence)) {
            TableWriter.indexAndCountDown(indexer, lo, hi, latch);
            return true;
        }
        // This is hard to test. Condition occurs when main thread successfully steals
        // work from under nose of this worker.
        return false;
    }
}
