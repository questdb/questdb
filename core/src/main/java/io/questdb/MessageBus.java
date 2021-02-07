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
import io.questdb.mp.MCSequence;
import io.questdb.mp.MPSequence;
import io.questdb.mp.RingQueue;
import io.questdb.mp.Sequence;
import io.questdb.std.Misc;
import io.questdb.tasks.*;

import java.io.Closeable;

public interface MessageBus extends Closeable {
    @Override
    default void close() {
        Misc.free(getOutOfOrderPartitionQueue());
    }

    CairoConfiguration getConfiguration();

    Sequence getIndexerPubSequence();

    RingQueue<ColumnIndexerTask> getIndexerQueue();

    Sequence getIndexerSubSequence();

    MPSequence getOutOfOrderCopyPubSequence();

    RingQueue<OutOfOrderCopyTask> getOutOfOrderCopyQueue();

    MCSequence getOutOfOrderCopySubSequence();

    MPSequence getOutOfOrderOpenColumnPubSequence();

    RingQueue<OutOfOrderOpenColumnTask> getOutOfOrderOpenColumnQueue();

    MCSequence getOutOfOrderOpenColumnSubSequence();

    MPSequence getOutOfOrderPartitionPubSeq();

    RingQueue<OutOfOrderPartitionTask> getOutOfOrderPartitionQueue();

    MCSequence getOutOfOrderPartitionSubSeq();

    MPSequence getOutOfOrderSortPubSeq();

    RingQueue<OutOfOrderSortTask> getOutOfOrderSortQueue();

    MCSequence getOutOfOrderSortSubSeq();

    MPSequence getOutOfOrderUpdPartitionSizePubSequence();

    RingQueue<OutOfOrderUpdPartitionSizeTask> getOutOfOrderUpdPartitionSizeQueue();

    // todo: this should be single consumer sequence, but i had problems using it
    MCSequence getOutOfOrderUpdPartitionSizeSubSequence();

    default Sequence getTableBlockWriterPubSequence() {
        return null;
    }

    default RingQueue<TableBlockWriterTaskHolder> getTableBlockWriterQueue() {
        return null;
    }

    default Sequence getTableBlockWriterSubSequence() {
        return null;
    }

    Sequence getVectorAggregatePubSequence();

    RingQueue<VectorAggregateTask> getVectorAggregateQueue();

    Sequence getVectorAggregateSubSequence();
}
