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
import io.questdb.cairo.sql.async.PageFrameReduceTask;
import io.questdb.cutlass.text.TextImportRequestTask;
import io.questdb.cutlass.text.TextImportTask;
import io.questdb.mp.*;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.tasks.*;
import org.jetbrains.annotations.NotNull;

public class MessageBusImpl implements MessageBus {
    private final CairoConfiguration configuration;

    private final RingQueue<ColumnIndexerTask> indexerQueue;
    private final MPSequence indexerPubSeq;
    private final MCSequence indexerSubSeq;

    private final RingQueue<VectorAggregateTask> vectorAggregateQueue;
    private final MPSequence vectorAggregatePubSeq;
    private final MCSequence vectorAggregateSubSeq;

    private final RingQueue<O3CallbackTask> o3CallbackQueue;
    private final MPSequence o3CallbackPubSeq;
    private final MCSequence o3CallbackSubSeq;

    private final RingQueue<O3PartitionPurgeTask> o3PurgeDiscoveryQueue;
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

    private final RingQueue<TableWriterTask> tableWriterEventQueue;
    private final MPSequence tableWriterEventPubSeq;
    private final FanOut tableWriterEventSubSeq;

    private final MPSequence queryCacheEventPubSeq;
    private final FanOut queryCacheEventSubSeq;

    private final int pageFrameReduceShardCount;
    private final MPSequence[] pageFrameReducePubSeq;
    private final MCSequence[] pageFrameReduceSubSeq;
    private final RingQueue<PageFrameReduceTask>[] pageFrameReduceQueue;
    private final FanOut[] pageFrameCollectFanOut;
    private final RingQueue<ColumnPurgeTask> columnPurgeQueue;
    private final SCSequence columnPurgeSubSeq;
    private final MPSequence columnPurgePubSeq;
    private final RingQueue<PartitionPurgeTask> partitionPurgeQueue;
    private final SCSequence partitionPurgeSubSeq;
    private final MPSequence partitionPurgePubSeq;
    private final RingQueue<TextImportTask> textImportQueue;
    private final SPSequence textImportPubSeq;
    private final MCSequence textImportSubSeq;
    private final SCSequence textImportColSeq;
    private final RingQueue<TextImportRequestTask> textImportRequestQueue;
    private final MPSequence textImportRequestPubSeq;
    private final SCSequence textImportRequestSubSeq;

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
        this.o3CallbackPubSeq = new MPSequence(o3CallbackQueue.getCycle());
        this.o3CallbackSubSeq = new MCSequence(o3CallbackQueue.getCycle());
        o3CallbackPubSeq.then(o3CallbackSubSeq).then(o3CallbackPubSeq);

        this.o3PartitionQueue = new RingQueue<>(O3PartitionTask::new, configuration.getO3PartitionQueueCapacity());
        this.o3PartitionPubSeq = new MPSequence(o3PartitionQueue.getCycle());
        this.o3PartitionSubSeq = new MCSequence(o3PartitionQueue.getCycle());
        o3PartitionPubSeq.then(o3PartitionSubSeq).then(o3PartitionPubSeq);

        this.o3OpenColumnQueue = new RingQueue<>(O3OpenColumnTask::new, configuration.getO3OpenColumnQueueCapacity());
        this.o3OpenColumnPubSeq = new MPSequence(o3OpenColumnQueue.getCycle());
        this.o3OpenColumnSubSeq = new MCSequence(o3OpenColumnQueue.getCycle());
        o3OpenColumnPubSeq.then(o3OpenColumnSubSeq).then(o3OpenColumnPubSeq);

        this.o3CopyQueue = new RingQueue<>(O3CopyTask::new, configuration.getO3CopyQueueCapacity());
        this.o3CopyPubSeq = new MPSequence(o3CopyQueue.getCycle());
        this.o3CopySubSeq = new MCSequence(o3CopyQueue.getCycle());
        o3CopyPubSeq.then(o3CopySubSeq).then(o3CopyPubSeq);

        this.o3PurgeDiscoveryQueue = new RingQueue<>(O3PartitionPurgeTask::new, configuration.getO3PurgeDiscoveryQueueCapacity());
        this.o3PurgeDiscoveryPubSeq = new MPSequence(o3PurgeDiscoveryQueue.getCycle());
        this.o3PurgeDiscoverySubSeq = new MCSequence(o3PurgeDiscoveryQueue.getCycle());
        this.o3PurgeDiscoveryPubSeq.then(o3PurgeDiscoverySubSeq).then(o3PurgeDiscoveryPubSeq);

        this.latestByQueue = new RingQueue<>(LatestByTask::new, configuration.getLatestByQueueCapacity());
        this.latestByPubSeq = new MPSequence(latestByQueue.getCycle());
        this.latestBySubSeq = new MCSequence(latestByQueue.getCycle());
        latestByPubSeq.then(latestBySubSeq).then(latestByPubSeq);

        this.tableWriterEventQueue = new RingQueue<>(
                TableWriterTask::new,
                configuration.getWriterCommandQueueSlotSize(),
                configuration.getWriterCommandQueueCapacity(),
                MemoryTag.NATIVE_REPL
        );
        this.tableWriterEventPubSeq = new MPSequence(tableWriterEventQueue.getCycle());
        this.tableWriterEventSubSeq = new FanOut();
        this.tableWriterEventPubSeq.then(tableWriterEventSubSeq).then(tableWriterEventPubSeq);

        this.queryCacheEventPubSeq = new MPSequence(configuration.getQueryCacheEventQueueCapacity());
        this.queryCacheEventSubSeq = new FanOut();
        this.queryCacheEventPubSeq.then(queryCacheEventSubSeq).then(queryCacheEventPubSeq);

        this.columnPurgeQueue = new RingQueue<>(ColumnPurgeTask::new, configuration.getColumnPurgeQueueCapacity());
        this.columnPurgeSubSeq = new SCSequence();
        this.columnPurgePubSeq = new MPSequence(columnPurgeQueue.getCycle());
        this.columnPurgePubSeq.then(columnPurgeSubSeq).then(columnPurgePubSeq);

        this.partitionPurgeQueue = new RingQueue<>(PartitionPurgeTask::new, configuration.getPartitionPurgeQueueCapacity());
        this.partitionPurgeSubSeq = new SCSequence();
        this.partitionPurgePubSeq = new MPSequence(partitionPurgeQueue.getCycle());
        this.columnPurgePubSeq.then(partitionPurgeSubSeq).then(partitionPurgePubSeq);

        this.pageFrameReduceShardCount = configuration.getPageFrameReduceShardCount();

        //noinspection unchecked
        pageFrameReduceQueue = new RingQueue[pageFrameReduceShardCount];
        pageFrameReducePubSeq = new MPSequence[pageFrameReduceShardCount];
        pageFrameReduceSubSeq = new MCSequence[pageFrameReduceShardCount];
        pageFrameCollectFanOut = new FanOut[pageFrameReduceShardCount];

        int reduceQueueCapacity = configuration.getPageFrameReduceQueueCapacity();
        for (int i = 0; i < pageFrameReduceShardCount; i++) {
            final RingQueue<PageFrameReduceTask> queue = new RingQueue<PageFrameReduceTask>(
                    () -> new PageFrameReduceTask(configuration),
                    reduceQueueCapacity
            );

            final MPSequence reducePubSeq = new MPSequence(reduceQueueCapacity);
            final MCSequence reduceSubSeq = new MCSequence(reduceQueueCapacity);
            final FanOut collectFanOut = new FanOut();
            reducePubSeq.then(reduceSubSeq).then(collectFanOut).then(reducePubSeq);

            pageFrameReduceQueue[i] = queue;
            pageFrameReducePubSeq[i] = reducePubSeq;
            pageFrameReduceSubSeq[i] = reduceSubSeq;
            pageFrameCollectFanOut[i] = collectFanOut;
        }

        this.textImportQueue = new RingQueue<>(TextImportTask::new, configuration.getSqlCopyQueueCapacity());
        this.textImportPubSeq = new SPSequence(textImportQueue.getCycle());
        this.textImportSubSeq = new MCSequence(textImportQueue.getCycle());
        this.textImportColSeq = new SCSequence();
        textImportPubSeq.then(textImportSubSeq).then(textImportColSeq).then(textImportPubSeq);

        // We allow only a single parallel import to be in-flight, hence queue size of 1.
        this.textImportRequestQueue = new RingQueue<>(TextImportRequestTask::new, 1);
        this.textImportRequestPubSeq = new MPSequence(textImportRequestQueue.getCycle());
        this.textImportRequestSubSeq = new SCSequence();
        textImportRequestPubSeq.then(textImportRequestSubSeq).then(textImportRequestPubSeq);
    }

    @Override
    public void close() {
        // We need to close only queues with native backing memory.
        Misc.free(getTableWriterEventQueue());
        Misc.free(pageFrameReduceQueue);
    }

    @Override
    public Sequence getColumnPurgePubSeq() {
        return columnPurgePubSeq;
    }

    @Override
    public RingQueue<PartitionPurgeTask> getPartitionPurgeQueue() {
        return partitionPurgeQueue;
    }

    @Override
    public Sequence getPartitionPurgeSubSeq() {
        return partitionPurgeSubSeq;
    }

    @Override
    public Sequence getPartitionPurgePubSeq() {
        return partitionPurgePubSeq;
    }

    @Override
    public RingQueue<ColumnPurgeTask> getColumnPurgeQueue() {
        return columnPurgeQueue;
    }

    @Override
    public SCSequence getColumnPurgeSubSeq() {
        return columnPurgeSubSeq;
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
    public RingQueue<O3PartitionPurgeTask> getO3PurgeDiscoveryQueue() {
        return o3PurgeDiscoveryQueue;
    }

    @Override
    public MCSequence getO3PurgeDiscoverySubSeq() {
        return o3PurgeDiscoverySubSeq;
    }

    @Override
    public FanOut getPageFrameCollectFanOut(int shard) {
        return pageFrameCollectFanOut[shard];
    }

    @Override
    public MPSequence getPageFrameReducePubSeq(int shard) {
        return pageFrameReducePubSeq[shard];
    }

    @Override
    public RingQueue<PageFrameReduceTask> getPageFrameReduceQueue(int shard) {
        return pageFrameReduceQueue[shard];
    }

    @Override
    public int getPageFrameReduceShardCount() {
        return pageFrameReduceShardCount;
    }

    @Override
    public MCSequence getPageFrameReduceSubSeq(int shard) {
        return pageFrameReduceSubSeq[shard];
    }

    @Override
    public FanOut getTableWriterEventFanOut() {
        return tableWriterEventSubSeq;
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

    @Override
    public MPSequence getQueryCacheEventPubSeq() {
        return queryCacheEventPubSeq;
    }

    @Override
    public FanOut getQueryCacheEventFanOut() {
        return queryCacheEventSubSeq;
    }

    @Override
    public RingQueue<TextImportTask> getTextImportQueue() {
        return textImportQueue;
    }

    @Override
    public Sequence getTextImportPubSeq() {
        return textImportPubSeq;
    }

    @Override
    public Sequence getTextImportSubSeq() {
        return textImportSubSeq;
    }

    @Override
    public SCSequence getTextImportColSeq() {
        return textImportColSeq;
    }

    @Override
    public RingQueue<TextImportRequestTask> getTextImportRequestQueue() {
        return textImportRequestQueue;
    }

    @Override
    public MPSequence getTextImportRequestPubSeq() {
        return textImportRequestPubSeq;
    }

    @Override
    public Sequence getTextImportRequestSubSeq() {
        return textImportRequestSubSeq;
    }
}
