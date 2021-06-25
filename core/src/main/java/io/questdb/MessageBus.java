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
import io.questdb.std.Misc;
import io.questdb.tasks.*;

import java.io.Closeable;

public interface MessageBus extends Closeable {
    @Override
    default void close() {
        Misc.free(getO3PartitionQueue());
    }

    CairoConfiguration getConfiguration();

    Sequence getIndexerPubSequence();

    RingQueue<ColumnIndexerTask> getIndexerQueue();

    Sequence getIndexerSubSequence();

    MPSequence getO3PurgeDiscoveryPubSeq();

    RingQueue<O3PurgeDiscoveryTask> getO3PurgeDiscoveryQueue();

    MCSequence getO3PurgeDiscoverySubSeq();

    MPSequence getO3PurgePubSeq();

    RingQueue<O3PurgeTask> getO3PurgeQueue();

    MCSequence getO3PurgeSubSeq();

    MPSequence getO3CopyPubSeq();

    RingQueue<O3CopyTask> getO3CopyQueue();

    MCSequence getO3CopySubSeq();

    MPSequence getO3OpenColumnPubSeq();

    RingQueue<O3OpenColumnTask> getO3OpenColumnQueue();

    MCSequence getO3OpenColumnSubSeq();

    MPSequence getO3PartitionPubSeq();

    RingQueue<O3PartitionTask> getO3PartitionQueue();

    MCSequence getO3PartitionSubSeq();

    MPSequence getO3CallbackPubSeq();

    RingQueue<O3CallbackTask> getO3CallbackQueue();

    MCSequence getO3CallbackSubSeq();

    default Sequence getTableBlockWriterPubSeq() {
        return null;
    }

    default RingQueue<TableBlockWriterTaskHolder> getTableBlockWriterQueue() {
        return null;
    }

    default Sequence getTableBlockWriterSubSeq() {
        return null;
    }

    Sequence getVectorAggregatePubSeq();

    RingQueue<VectorAggregateTask> getVectorAggregateQueue();

    Sequence getVectorAggregateSubSeq();

    Sequence getLatestByPubSeq();

    RingQueue<LatestByTask> getLatestByQueue();

    Sequence getLatestBySubSeq();
}
