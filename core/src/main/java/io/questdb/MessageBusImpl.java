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

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.TableBlockWriter.TableBlockWriterTaskHolder;
import io.questdb.mp.*;
import io.questdb.tasks.ColumnIndexerTask;
import io.questdb.tasks.OutOfOrderInsertTask;
import io.questdb.tasks.VectorAggregateTask;
import org.jetbrains.annotations.NotNull;

public class MessageBusImpl implements MessageBus {
    private final RingQueue<ColumnIndexerTask> indexerQueue;
    private final MPSequence indexerPubSeq;
    private final MCSequence indexerSubSeq;

    private final RingQueue<VectorAggregateTask> vectorAggregateQueue;
    private final MPSequence vectorAggregatePubSeq;
    private final MCSequence vectorAggregateSubSeq;

    private final RingQueue<TableBlockWriterTaskHolder> tableBlockWriterQueue;
    private final MPSequence tableBlockWriterPubSeq;
    private final MCSequence tableBlockWriterSubSeq;

    private final RingQueue<OutOfOrderInsertTask> outOfOrderInsertQueue;
    private final SPSequence outOfOrderInsertPubSeq;
    private final MCSequence outOfOrderInsertSubSeq;

    private final CairoConfiguration configuration;

    public MessageBusImpl(@NotNull CairoConfiguration configuration) {
        this.configuration = configuration;

        this.indexerQueue = new RingQueue<>(ColumnIndexerTask::new, 1024);
        this.indexerPubSeq = new MPSequence(indexerQueue.getCapacity());
        this.indexerSubSeq = new MCSequence(indexerQueue.getCapacity());
        indexerPubSeq.then(indexerSubSeq).then(indexerPubSeq);

        this.vectorAggregateQueue = new RingQueue<>(VectorAggregateTask::new, 1024);
        this.vectorAggregatePubSeq = new MPSequence(vectorAggregateQueue.getCapacity());
        this.vectorAggregateSubSeq = new MCSequence(vectorAggregateQueue.getCapacity());
        vectorAggregatePubSeq.then(vectorAggregateSubSeq).then(vectorAggregatePubSeq);

        this.tableBlockWriterQueue = new RingQueue<>(TableBlockWriterTaskHolder::new, configuration.getTableBlockWriterQueueSize());
        this.tableBlockWriterPubSeq = new MPSequence(tableBlockWriterQueue.getCapacity());
        this.tableBlockWriterSubSeq = new MCSequence(tableBlockWriterQueue.getCapacity());
        tableBlockWriterPubSeq.then(tableBlockWriterSubSeq).then(tableBlockWriterPubSeq);

        this.outOfOrderInsertQueue = new RingQueue<>(OutOfOrderInsertTask::new, 1024);
        this.outOfOrderInsertPubSeq = new SPSequence(this.outOfOrderInsertQueue.getCapacity());
        this.outOfOrderInsertSubSeq = new MCSequence(this.outOfOrderInsertQueue.getCapacity());
        outOfOrderInsertPubSeq.then(outOfOrderInsertSubSeq).then(outOfOrderInsertPubSeq);
    }

    @Override
    public SPSequence getOutOfOrderInsertPubSeq() {
        return outOfOrderInsertPubSeq;
    }

    @Override
    public RingQueue<OutOfOrderInsertTask> getOutOfOrderInsertQueue() {
        return outOfOrderInsertQueue;
    }

    @Override
    public MCSequence getOutOfOrderInsertSubSeq() {
        return outOfOrderInsertSubSeq;
    }

    @Override
    public CairoConfiguration getConfiguration() {
        return configuration;
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
        return vectorAggregateQueue;
    }

    @Override
    public Sequence getVectorAggregatePubSequence() {
        return vectorAggregatePubSeq;
    }

    @Override
    public Sequence getVectorAggregateSubSequence() {
        return vectorAggregateSubSeq;
    }

    @Override
    public RingQueue<TableBlockWriterTaskHolder> getTableBlockWriterQueue() {
        return tableBlockWriterQueue;
    }

    @Override
    public Sequence getTableBlockWriterPubSequence() {
        return tableBlockWriterPubSeq;
    }

    @Override
    public Sequence getTableBlockWriterSubSequence() {
        return tableBlockWriterSubSeq;
    }
}
