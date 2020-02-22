/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
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

package io.questdb.cairo;

import io.questdb.MessageBus;
import io.questdb.cairo.sql.scopes.ColumnIndexerScope;
import io.questdb.mp.Job;
import io.questdb.mp.RingQueue;
import io.questdb.mp.SOCountDownLatch;
import io.questdb.mp.Sequence;

public class ColumnIndexerJob implements Job {
    private final RingQueue<ColumnIndexerScope> queue;
    private final Sequence subSeq;

    public ColumnIndexerJob(MessageBus messageBus) {
        this.queue = messageBus.getIndexerQueue();
        this.subSeq = messageBus.getIndexerSubSequence();
    }

    @Override
    public boolean run() {
        long cursor = subSeq.next();
        if (cursor < 0) {
            return false;
        }

        final ColumnIndexerScope queueItem = queue.get(cursor);
        // copy values and release queue item
        final ColumnIndexer indexer = queueItem.indexer;
        final long lo = queueItem.lo;
        final long hi = queueItem.hi;
        final long indexSequence = queueItem.sequence;
        final SOCountDownLatch latch = queueItem.countDownLatch;
        subSeq.done(cursor);

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
