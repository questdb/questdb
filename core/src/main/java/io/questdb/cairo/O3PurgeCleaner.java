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
import io.questdb.mp.RingQueue;
import io.questdb.mp.Sequence;
import io.questdb.std.LongList;
import io.questdb.std.str.MutableCharSink;
import io.questdb.std.str.NativeLPSZ;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import io.questdb.tasks.O3PurgeDiscoveryTask;
import io.questdb.tasks.O3PurgeTask;

import java.io.Closeable;

public class O3PurgeCleaner implements Closeable {
    private final CairoConfiguration configuration;
    private final MutableCharSink sink = new StringSink();
    private final NativeLPSZ nativeLPSZ = new NativeLPSZ();
    private final LongList txnList = new LongList();
    private final RingQueue<O3PurgeDiscoveryTask> purgeDiscoveryQueue;
    private final Sequence purgeDiscoverySubSeq;
    private final RingQueue<O3PurgeTask> purgeQueue;
    private final Sequence purgeSubSeq;

    public O3PurgeCleaner(MessageBus messageBus) {
        this.purgeDiscoveryQueue = messageBus.getO3PurgeDiscoveryQueue();
        this.purgeDiscoverySubSeq = messageBus.getO3PurgeDiscoverySubSeq();
        this.purgeQueue = messageBus.getO3PurgeQueue();
        this.purgeSubSeq = messageBus.getO3PurgeSubSeq();
        this.configuration = messageBus.getConfiguration();
    }

    @Override
    public void close() {
        while (true) {
            final long cursor = purgeDiscoverySubSeq.next();
            if (cursor > -1) {
                O3PurgeDiscoveryTask task = purgeDiscoveryQueue.get(cursor);
                long pTxnScoreboard = task.getTxnScoreboard();
                try {
                    O3PurgeDiscoveryJob.discoverPartitions(
                            configuration.getFilesFacade(),
                            sink,
                            nativeLPSZ,
                            txnList,
                            null,
                            null,
                            configuration.getRoot(),
                            task.getTableName(),
                            task.getPartitionBy(),
                            task.getTimestamp(),
                            pTxnScoreboard
                    );
                } finally {
                    purgeDiscoverySubSeq.done(cursor);
                }
            } else if (cursor == -1) {
                break;
            }
        }

        while (true) {
            final long cursor = purgeSubSeq.next();
            if (cursor > -1) {
                O3PurgeTask task = purgeQueue.get(cursor);
                long pTxnScoreboard = task.getTxnScoreboard();
                try {
                    O3PurgeJob.purgePartitionDir(
                            configuration.getFilesFacade(),
                            Path.getThreadLocal(configuration.getRoot()).concat(task.getTableName()),
                            task.getPartitionBy(),
                            task.getTimestamp(),
                            pTxnScoreboard,
                            task.getNameTxnToRemove(),
                            task.getMinTxnToExpect()
                    );
                } finally {
                    TxnScoreboard.close(pTxnScoreboard);
                    purgeSubSeq.done(cursor);
                }
            } else if (cursor == -1) {
                break;
            }
        }
    }
}
