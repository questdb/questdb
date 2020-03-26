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

package io.questdb;

import io.questdb.mp.MCSequence;
import io.questdb.mp.MPSequence;
import io.questdb.mp.RingQueue;
import io.questdb.mp.Sequence;
import io.questdb.tasks.ColumnIndexerTask;
import io.questdb.tasks.VectorAggregateTask;

public class MessageBusImpl implements MessageBus {
    private final RingQueue<ColumnIndexerTask> indexerQueue = new RingQueue<>(ColumnIndexerTask::new, 1024);
    private final MPSequence indexerPubSeq = new MPSequence(indexerQueue.getCapacity());
    private final MCSequence indexerSubSeq = new MCSequence(indexerQueue.getCapacity());

    private final RingQueue<VectorAggregateTask> vectorAggregaterQueue = new RingQueue<>(VectorAggregateTask::new, 1024);
    private final MPSequence vectorAggregatePubSeq = new MPSequence(vectorAggregaterQueue.getCapacity());
    private final MCSequence vectorAggregateSubSeq = new MCSequence(vectorAggregaterQueue.getCapacity());

    public MessageBusImpl() {
        this.indexerPubSeq.then(this.indexerSubSeq).then(this.indexerPubSeq);
        this.vectorAggregatePubSeq.then(vectorAggregateSubSeq).then(vectorAggregatePubSeq);
    }

    @Override
    public Sequence getIndexerPubSequence() {
        return indexerPubSeq;
    }

    @Override
    public RingQueue<ColumnIndexerTask> getIndexerQueue() {
        return indexerQueue;
    }

    @Override
    public Sequence getIndexerSubSequence() {
        return indexerSubSeq;
    }

    @Override
    public RingQueue<VectorAggregateTask> getVectorAggregateQueue() {
        return vectorAggregaterQueue;
    }

    @Override
    public Sequence getVectorAggregatePubSequence() {
        return vectorAggregatePubSeq;
    }

    @Override
    public Sequence getVectorAggregateSubSequence() {
        return vectorAggregateSubSeq;
    }
}
