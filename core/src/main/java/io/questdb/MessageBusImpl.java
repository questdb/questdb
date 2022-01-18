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

package io.questdb;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.mp.*;
import io.questdb.std.MemoryTag;
import io.questdb.tasks.*;
import org.jetbrains.annotations.NotNull;

public class MessageBusImpl implements MessageBus {
    private final RingQueue<ColumnIndexerTask> indexerQueue;
    private final MPSequence indexerPubSeq;
    private final MCSequence indexerSubSeq;

    private final RingQueue<VectorAggregateTask> vectorAggregateQueue;
    private final MPSequence vectorAggregatePubSeq;
    private final MCSequence vectorAggregateSubSeq;

    private final RingQueue<O3CallbackTask> o3CallbackQueue;
    private final MPSequence o3CallbackPubSeq;
    private final MCSequence o3CallbackSubSeq;

    private final RingQueue<O3PurgeDiscoveryTask> o3PurgeDiscoveryQueue;
    private final MPSequence o3PurgeDiscoveryPubSeq;
    private final MCSequence o3PurgeDiscoverySubSeq;

    private final RingQueue<O3PartitionTask> o3PartitionQueue;
    private final MPSequence o3PartitionPubSeq;
    private final MCSequence o3PartitionSubSeq;

    private final RingQueue<O3OpenColumnTask> o3OpenColumnQueue;
    private final MPSequence o3OpenColumnPubSeq;
    private final MCSequence o3OpenColumnSubSeq;

    private final RingQueue<O3CopyTask> o3CopyQueue;
    private final MPSequence o3CopyPubSeq;
    private final MCSequence o3CopySubSeq;

    private final RingQueue<LatestByTask> latestByQueue;
    private final MPSequence latestByPubSeq;
    private final MCSequence latestBySubSeq;

    private final RingQueue<TableWriterTask> tableWriterCommandQueue;
    private final MPSequence tableWriterCommandPubSeq;
    private final FanOut tableWriterCommandSubSeq;

    private final RingQueue<TableWriterTask> tableWriterEventQueue;
    private final MPSequence tableWriterEventPubSeq;
    private final FanOut tableWriterEventSubSeq;
    private final CairoConfiguration configuration;

    public MessageBusImpl(@NotNull CairoConfiguration configuration) {
        this.configuration = configuration;
        this.indexerQueue = new RingQueue<>(ColumnIndexerTask::new, configuration.getColumnIndexerQueueCapacity());
        this.indexerPubSeq = new MPSequence(indexerQueue.getCycle());
        this.indexerSubSeq = new MCSequence(indexerQueue.getCycle());
        indexerPubSeq.then(indexerSubSeq).then(indexerPubSeq);

        this.vectorAggregateQueue = new RingQueue<>(VectorAggregateTask::new, configuration.getVectorAggregateQueueCapacity());
        this.vectorAggregatePubSeq = new MPSequence(vectorAggregateQueue.getCycle());
        this.vectorAggregateSubSeq = new MCSequence(vectorAggregateQueue.getCycle());
        vectorAggregatePubSeq.then(vectorAggregateSubSeq).then(vectorAggregatePubSeq);

        this.o3CallbackQueue = new RingQueue<>(O3CallbackTask::new, configuration.getO3CallbackQueueCapacity());
        this.o3CallbackPubSeq = new MPSequence(this.o3CallbackQueue.getCycle());
        this.o3CallbackSubSeq = new MCSequence(this.o3CallbackQueue.getCycle());
        o3CallbackPubSeq.then(o3CallbackSubSeq).then(o3CallbackPubSeq);

        this.o3PartitionQueue = new RingQueue<>(O3PartitionTask::new, configuration.getO3PartitionQueueCapacity());
        this.o3PartitionPubSeq = new MPSequence(this.o3PartitionQueue.getCycle());
        this.o3PartitionSubSeq = new MCSequence(this.o3PartitionQueue.getCycle());
        o3PartitionPubSeq.then(o3PartitionSubSeq).then(o3PartitionPubSeq);

        this.o3OpenColumnQueue = new RingQueue<>(O3OpenColumnTask::new, configuration.getO3OpenColumnQueueCapacity());
        this.o3OpenColumnPubSeq = new MPSequence(this.o3OpenColumnQueue.getCycle());
        this.o3OpenColumnSubSeq = new MCSequence(this.o3OpenColumnQueue.getCycle());
        o3OpenColumnPubSeq.then(o3OpenColumnSubSeq).then(o3OpenColumnPubSeq);

        this.o3CopyQueue = new RingQueue<>(O3CopyTask::new, configuration.getO3CopyQueueCapacity());
        this.o3CopyPubSeq = new MPSequence(this.o3CopyQueue.getCycle());
        this.o3CopySubSeq = new MCSequence(this.o3CopyQueue.getCycle());
        o3CopyPubSeq.then(o3CopySubSeq).then(o3CopyPubSeq);

        this.o3PurgeDiscoveryQueue = new RingQueue<>(O3PurgeDiscoveryTask::new, configuration.getO3PurgeDiscoveryQueueCapacity());
        this.o3PurgeDiscoveryPubSeq = new MPSequence(this.o3PurgeDiscoveryQueue.getCycle());
        this.o3PurgeDiscoverySubSeq = new MCSequence(this.o3PurgeDiscoveryQueue.getCycle());
        this.o3PurgeDiscoveryPubSeq.then(this.o3PurgeDiscoverySubSeq).then(o3PurgeDiscoveryPubSeq);

        this.latestByQueue = new RingQueue<>(LatestByTask::new, configuration.getLatestByQueueCapacity());
        this.latestByPubSeq = new MPSequence(latestByQueue.getCycle());
        this.latestBySubSeq = new MCSequence(latestByQueue.getCycle());
        latestByPubSeq.then(latestBySubSeq).then(latestByPubSeq);

        // todo: move to configuration
        this.tableWriterCommandQueue = new RingQueue<>(
                TableWriterTask::new,
                2048,
                configuration.getWriterCommandQueueCapacity(),
                MemoryTag.NATIVE_REPL
        );
        this.tableWriterCommandPubSeq = new MPSequence(this.tableWriterCommandQueue.getCycle());
        this.tableWriterCommandSubSeq = new FanOut();
        this.tableWriterCommandPubSeq.then(this.tableWriterCommandSubSeq).then(this.tableWriterCommandPubSeq);

        this.tableWriterEventQueue = new RingQueue<>(
                TableWriterTask::new,
                2048,
                configuration.getWriterCommandQueueCapacity(),
                MemoryTag.NATIVE_REPL

        );
        this.tableWriterEventPubSeq = new MPSequence(this.tableWriterCommandQueue.getCycle());
        this.tableWriterEventSubSeq = new FanOut();
        this.tableWriterEventPubSeq.then(this.tableWriterEventSubSeq).then(this.tableWriterEventPubSeq);
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
    public Sequence getLatestByPubSeq() {
        return latestByPubSeq;
    }

    @Override
    public RingQueue<LatestByTask> getLatestByQueue() {
        return latestByQueue;
    }

    @Override
    public Sequence getLatestBySubSeq() {
        return latestBySubSeq;
    }

    @Override
    public MPSequence getO3CallbackPubSeq() {
        return o3CallbackPubSeq;
    }

    @Override
    public RingQueue<O3CallbackTask> getO3CallbackQueue() {
        return o3CallbackQueue;
    }

    @Override
    public MCSequence getO3CallbackSubSeq() {
        return o3CallbackSubSeq;
    }

    @Override
    public MPSequence getO3CopyPubSeq() {
        return o3CopyPubSeq;
    }

    @Override
    public RingQueue<O3CopyTask> getO3CopyQueue() {
        return o3CopyQueue;
    }

    @Override
    public MCSequence getO3CopySubSeq() {
        return o3CopySubSeq;
    }

    @Override
    public MPSequence getO3OpenColumnPubSeq() {
        return o3OpenColumnPubSeq;
    }

    @Override
    public RingQueue<O3OpenColumnTask> getO3OpenColumnQueue() {
        return o3OpenColumnQueue;
    }

    @Override
    public MCSequence getO3OpenColumnSubSeq() {
        return o3OpenColumnSubSeq;
    }

    @Override
    public MPSequence getO3PartitionPubSeq() {
        return o3PartitionPubSeq;
    }

    @Override
    public RingQueue<O3PartitionTask> getO3PartitionQueue() {
        return o3PartitionQueue;
    }

    @Override
    public MCSequence getO3PartitionSubSeq() {
        return o3PartitionSubSeq;
    }

    @Override
    public MPSequence getO3PurgeDiscoveryPubSeq() {
        return o3PurgeDiscoveryPubSeq;
    }

    @Override
    public RingQueue<O3PurgeDiscoveryTask> getO3PurgeDiscoveryQueue() {
        return o3PurgeDiscoveryQueue;
    }

    @Override
    public MCSequence getO3PurgeDiscoverySubSeq() {
        return o3PurgeDiscoverySubSeq;
    }

    @Override
    public MPSequence getTableWriterCommandPubSeq() {
        return tableWriterCommandPubSeq;
    }

    @Override
    public RingQueue<TableWriterTask> getTableWriterCommandQueue() {
        return tableWriterCommandQueue;
    }

    @Override
    public FanOut getTableWriterCommandFanOut() {
        return tableWriterCommandSubSeq;
    }

    @Override
    public MPSequence getTableWriterEventPubSeq() {
        return tableWriterEventPubSeq;
    }

    @Override
    public RingQueue<TableWriterTask> getTableWriterEventQueue() {
        return tableWriterEventQueue;
    }

    @Override
    public FanOut getTableWriterEventFanOut() {
        return tableWriterEventSubSeq;
    }

    @Override
    public Sequence getVectorAggregatePubSeq() {
        return vectorAggregatePubSeq;
    }

    @Override
    public RingQueue<VectorAggregateTask> getVectorAggregateQueue() {
        return vectorAggregateQueue;
    }

    @Override
    public Sequence getVectorAggregateSubSeq() {
        return vectorAggregateSubSeq;
    }
}
