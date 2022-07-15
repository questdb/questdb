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

package io.questdb.cutlass.text;

import io.questdb.MessageBus;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.sql.async.PageFrameReduceTask;
import io.questdb.mp.*;
import io.questdb.tasks.*;

import java.io.IOException;
import java.util.concurrent.locks.Lock;

public class MessageBusStub implements MessageBus {
    @Override
    public void close() throws IOException {

    }

    @Override
    public Sequence getColumnPurgePubSeq() {
        return null;
    }

    @Override
    public RingQueue<ColumnPurgeTask> getColumnPurgeQueue() {
        return null;
    }

    @Override
    public Sequence getColumnPurgeSubSeq() {
        return null;
    }

    @Override
    public CairoConfiguration getConfiguration() {
        return null;
    }

    @Override
    public Sequence getIndexerPubSequence() {
        return null;
    }

    @Override
    public RingQueue<ColumnIndexerTask> getIndexerQueue() {
        return null;
    }

    @Override
    public Sequence getIndexerSubSequence() {
        return null;
    }

    @Override
    public Sequence getLatestByPubSeq() {
        return null;
    }

    @Override
    public RingQueue<LatestByTask> getLatestByQueue() {
        return null;
    }

    @Override
    public Sequence getLatestBySubSeq() {
        return null;
    }

    @Override
    public MPSequence getO3CallbackPubSeq() {
        return null;
    }

    @Override
    public RingQueue<O3CallbackTask> getO3CallbackQueue() {
        return null;
    }

    @Override
    public MCSequence getO3CallbackSubSeq() {
        return null;
    }

    @Override
    public MPSequence getO3CopyPubSeq() {
        return null;
    }

    @Override
    public RingQueue<O3CopyTask> getO3CopyQueue() {
        return null;
    }

    @Override
    public MCSequence getO3CopySubSeq() {
        return null;
    }

    @Override
    public MPSequence getO3OpenColumnPubSeq() {
        return null;
    }

    @Override
    public RingQueue<O3OpenColumnTask> getO3OpenColumnQueue() {
        return null;
    }

    @Override
    public MCSequence getO3OpenColumnSubSeq() {
        return null;
    }

    @Override
    public MPSequence getO3PartitionPubSeq() {
        return null;
    }

    @Override
    public RingQueue<O3PartitionTask> getO3PartitionQueue() {
        return null;
    }

    @Override
    public MCSequence getO3PartitionSubSeq() {
        return null;
    }

    @Override
    public MPSequence getO3PurgeDiscoveryPubSeq() {
        return null;
    }

    @Override
    public RingQueue<O3PartitionPurgeTask> getO3PurgeDiscoveryQueue() {
        return null;
    }

    @Override
    public MCSequence getO3PurgeDiscoverySubSeq() {
        return null;
    }

    @Override
    public FanOut getPageFrameCollectFanOut(int shard) {
        return null;
    }

    @Override
    public MPSequence getPageFrameReducePubSeq(int shard) {
        return null;
    }

    @Override
    public RingQueue<PageFrameReduceTask> getPageFrameReduceQueue(int shard) {
        return null;
    }

    @Override
    public int getPageFrameReduceShardCount() {
        return 0;
    }

    @Override
    public MCSequence getPageFrameReduceSubSeq(int shard) {
        return null;
    }

    @Override
    public FanOut getTableWriterEventFanOut() {
        return null;
    }

    @Override
    public MPSequence getTableWriterEventPubSeq() {
        return null;
    }

    @Override
    public RingQueue<TableWriterTask> getTableWriterEventQueue() {
        return null;
    }

    @Override
    public Sequence getVectorAggregatePubSeq() {
        return null;
    }

    @Override
    public RingQueue<VectorAggregateTask> getVectorAggregateQueue() {
        return null;
    }

    @Override
    public Sequence getVectorAggregateSubSeq() {
        return null;
    }

    @Override
    public MPSequence getQueryCacheEventPubSeq() {
        return null;
    }

    @Override
    public FanOut getQueryCacheEventFanOut() {
        return null;
    }

    @Override
    public RingQueue<TextImportTask> getTextImportQueue() {
        return null;
    }

    @Override
    public Sequence getTextImportPubSeq() {
        return null;
    }

    @Override
    public Sequence getTextImportSubSeq() {
        return null;
    }

    @Override
    public SCSequence getTextImportColSeq() {
        return null;
    }

    @Override
    public RingQueue<TextImportRequestTask> getTextImportRequestCollectingQueue() {
        return null;
    }

    @Override
    public Sequence getTextImportRequestCollectingPubSeq() {
        return null;
    }

    @Override
    public Sequence getTextImportRequestCollectingSubSeq() {
        return null;
    }

    @Override
    public RingQueue<TextImportRequestTask> getTextImportRequestProcessingQueue() {
        return null;
    }

    @Override
    public Sequence getTextImportRequestProcessingPubSeq() {
        return null;
    }

    @Override
    public Sequence getTextImportRequestProcessingSubSeq() {
        return null;
    }

    @Override
    public Sequence getTextImportRequestProcessingComplSeq() {
        return null;
    }

    @Override
    public FanOut getTextImportResponseFanOut() {
        return null;
    }
}
