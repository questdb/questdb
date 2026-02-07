/*******************************************************************************
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

package io.questdb;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.sql.async.PageFrameReduceTask;
import io.questdb.cairo.sql.async.UnorderedPageFrameReduceTask;
import io.questdb.cutlass.parquet.CopyExportRequestTask;
import io.questdb.cutlass.text.CopyImportRequestTask;
import io.questdb.cutlass.text.CopyImportTask;
import io.questdb.metrics.QueryTrace;
import io.questdb.mp.ConcurrentQueue;
import io.questdb.mp.FanOut;
import io.questdb.mp.MCSequence;
import io.questdb.mp.MPSequence;
import io.questdb.mp.RingQueue;
import io.questdb.mp.SCSequence;
import io.questdb.mp.SPSequence;
import io.questdb.tasks.ColumnIndexerTask;
import io.questdb.tasks.ColumnPurgeTask;
import io.questdb.tasks.ColumnTask;
import io.questdb.tasks.GroupByLongTopKTask;
import io.questdb.tasks.GroupByMergeShardTask;
import io.questdb.tasks.LatestByTask;
import io.questdb.tasks.O3CopyTask;
import io.questdb.tasks.O3OpenColumnTask;
import io.questdb.tasks.O3PartitionPurgeTask;
import io.questdb.tasks.O3PartitionTask;
import io.questdb.tasks.TableWriterTask;
import io.questdb.tasks.VectorAggregateTask;
import io.questdb.tasks.WalTxnNotificationTask;

import java.io.Closeable;

public interface MessageBus extends Closeable {

    MPSequence getColumnPurgePubSeq();

    RingQueue<ColumnPurgeTask> getColumnPurgeQueue();

    SCSequence getColumnPurgeSubSeq();

    MPSequence getColumnTaskPubSeq();

    RingQueue<ColumnTask> getColumnTaskQueue();

    MCSequence getColumnTaskSubSeq();

    CairoConfiguration getConfiguration();

    MPSequence getCopyExportRequestPubSeq();

    RingQueue<CopyExportRequestTask> getCopyExportRequestQueue();

    MCSequence getCopyExportRequestSubSeq();

    SCSequence getCopyImportColSeq();

    SPSequence getCopyImportPubSeq();

    RingQueue<CopyImportTask> getCopyImportQueue();

    SPSequence getCopyImportRequestPubSeq();

    RingQueue<CopyImportRequestTask> getCopyImportRequestQueue();

    SCSequence getCopyImportRequestSubSeq();

    MCSequence getCopyImportSubSeq();

    MPSequence getGroupByLongTopKPubSeq();

    RingQueue<GroupByLongTopKTask> getGroupByLongTopKQueue();

    MCSequence getGroupByLongTopKSubSeq();

    MPSequence getGroupByMergeShardPubSeq();

    RingQueue<GroupByMergeShardTask> getGroupByMergeShardQueue();

    MCSequence getGroupByMergeShardSubSeq();

    MPSequence getIndexerPubSequence();

    RingQueue<ColumnIndexerTask> getIndexerQueue();

    MCSequence getIndexerSubSequence();

    MPSequence getLatestByPubSeq();

    RingQueue<LatestByTask> getLatestByQueue();

    MCSequence getLatestBySubSeq();

    MPSequence getO3CopyPubSeq();

    RingQueue<O3CopyTask> getO3CopyQueue();

    MCSequence getO3CopySubSeq();

    MPSequence getO3OpenColumnPubSeq();

    RingQueue<O3OpenColumnTask> getO3OpenColumnQueue();

    MCSequence getO3OpenColumnSubSeq();

    MPSequence getO3PartitionPubSeq();

    RingQueue<O3PartitionTask> getO3PartitionQueue();

    MCSequence getO3PartitionSubSeq();

    MPSequence getO3PurgeDiscoveryPubSeq();

    RingQueue<O3PartitionPurgeTask> getO3PurgeDiscoveryQueue();

    MCSequence getO3PurgeDiscoverySubSeq();

    FanOut getPageFrameCollectFanOut(int shard);

    MPSequence getPageFrameReducePubSeq(int shard);

    RingQueue<PageFrameReduceTask> getPageFrameReduceQueue(int shard);

    int getPageFrameReduceShardCount();

    MCSequence getPageFrameReduceSubSeq(int shard);

    MPSequence getUnorderedPageFrameReducePubSeq();

    RingQueue<UnorderedPageFrameReduceTask> getUnorderedPageFrameReduceQueue();

    MCSequence getUnorderedPageFrameReduceSubSeq();

    MPSequence getQueryCacheEventPubSeq();

    MCSequence getQueryCacheEventSubSeq();

    ConcurrentQueue<QueryTrace> getQueryTraceQueue();

    FanOut getTableWriterEventFanOut();

    MPSequence getTableWriterEventPubSeq();

    RingQueue<TableWriterTask> getTableWriterEventQueue();

    MPSequence getVectorAggregatePubSeq();

    RingQueue<VectorAggregateTask> getVectorAggregateQueue();

    MCSequence getVectorAggregateSubSeq();

    MPSequence getWalTxnNotificationPubSequence();

    RingQueue<WalTxnNotificationTask> getWalTxnNotificationQueue();

    MCSequence getWalTxnNotificationSubSequence();
}
