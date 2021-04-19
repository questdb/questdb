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
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.AbstractQueueConsumerJob;
import io.questdb.mp.RingQueue;
import io.questdb.mp.Sequence;
import io.questdb.std.*;
import io.questdb.std.str.MutableCharSink;
import io.questdb.std.str.NativeLPSZ;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import io.questdb.tasks.O3PurgeDiscoveryTask;
import io.questdb.tasks.O3PurgeTask;
import org.jetbrains.annotations.Nullable;

public class O3PurgeDiscoveryJob extends AbstractQueueConsumerJob<O3PurgeDiscoveryTask> {

    private final static Log LOG = LogFactory.getLog(O3PurgeDiscoveryJob.class);
    private final CairoConfiguration configuration;
    private final MutableCharSink[] sink;
    private final NativeLPSZ[] nativeLPSZ;
    private final LongList[] txnList;
    private final RingQueue<O3PurgeTask> purgeQueue;
    private final Sequence purgePubSeq;

    public O3PurgeDiscoveryJob(MessageBus messageBus, int workerCount) {
        super(messageBus.getO3PurgeDiscoveryQueue(), messageBus.getO3PurgeDiscoverySubSeq());
        this.configuration = messageBus.getConfiguration();
        this.purgeQueue = messageBus.getO3PurgeQueue();
        this.purgePubSeq = messageBus.getO3PurgePubSeq();
        this.sink = new MutableCharSink[workerCount];
        this.nativeLPSZ = new NativeLPSZ[workerCount];
        this.txnList = new LongList[workerCount];
        for (int i = 0; i < workerCount; i++) {
            sink[i] = new StringSink();
            nativeLPSZ[i] = new NativeLPSZ();
            txnList[i] = new LongList();
        }
    }

    public static boolean discoverPartitions(
            FilesFacade ff,
            MutableCharSink sink,
            NativeLPSZ nativeLPSZ,
            LongList txnList,
            RingQueue<O3PurgeTask> purgeQueue,
            @Nullable Sequence purgePubSeq,
            CharSequence root,
            CharSequence tableName,
            int partitionBy,
            long partitionTimestamp,
            long txnScoreboard
    ) {
        LOG.info().$("processing [table=").$(tableName)
                .$(", ts=").$ts(partitionTimestamp)
                .I$();
        try {
            Path path = Path.getThreadLocal(root);
            path.concat(tableName).$$dir();
            sink.clear();
            TableUtils.setPathForPartition(sink, partitionBy, partitionTimestamp, false, false);
            path.$$dir();

            txnList.clear();

            long p = ff.findFirst(path);
            if (p > 0) {
                try {
                    do {
                        processDir(sink, nativeLPSZ, tableName, txnList, ff.findName(p), ff.findType(p));
                    } while (ff.findNext(p) > 0);
                } finally {
                    ff.findClose(p);
                }
            }

            if (txnList.size() > 1) {
                txnList.sort();

                for (int i = 0, n = txnList.size() - 1; i < n; i++) {
                    final long nameTxnToRemove = txnList.getQuick(i);
                    final long minTxnToExpect = txnList.getQuick(i + 1);
                    if (!O3PurgeJob.purgePartitionDir(
                            ff,
                            path.of(root).concat(tableName),
                            partitionBy,
                            partitionTimestamp,
                            txnScoreboard,
                            nameTxnToRemove,
                            minTxnToExpect
                    )) {
                        // queue the job
                        if (purgePubSeq != null) {
                            long cursor = purgePubSeq.next();
                            if (cursor > -1) {
                                LOG.error()
                                        .$("queuing [table=").$(tableName)
                                        .$(", ts=").$ts(partitionTimestamp)
                                        .$(", txn=").$(nameTxnToRemove)
                                        .$(']').$();
                                O3PurgeTask task = purgeQueue.get(cursor);
                                task.of(
                                        tableName,
                                        partitionBy,
                                        TxnScoreboard.newRef(txnScoreboard),
                                        partitionTimestamp,
                                        nameTxnToRemove,
                                        minTxnToExpect
                                );
                                purgePubSeq.done(cursor);
                            } else {
                                LOG.error()
                                        .$("purge queue is full [table=").$(tableName)
                                        .$(", ts=").$ts(partitionTimestamp)
                                        .$(", txn=").$(nameTxnToRemove)
                                        .$(']').$();
                            }
                        } else {
                            // todo:  decide what to do here? we cannot remove file and neither we can queue
                            //    we call this from cleaner
                            LOG.error()
                                    .$("could not purge [table=").$(tableName)
                                    .$(", ts=").$ts(partitionTimestamp)
                                    .$(", txn=").$(nameTxnToRemove)
                                    .$(", errno=").$(ff.errno())
                                    .$(']').$();
                        }
                    }
                }
                return true;
            }
            return false;
        } finally {
            TxnScoreboard.close(txnScoreboard);
        }
    }

    private static void processDir(
            MutableCharSink sink,
            NativeLPSZ nativeLPSZ,
            CharSequence tableName,
            LongList txnList,
            long name,
            int type
    ) {
        if (type == Files.DT_DIR) {
            nativeLPSZ.of(name);
            if (Chars.notDots(nativeLPSZ) && Chars.startsWith(nativeLPSZ, sink)) {
                // extract txn from name
                int index = Chars.lastIndexOf(nativeLPSZ, '.');
                if (index < 0) {
                    txnList.add(-1);
                } else {
                    try {
                        txnList.add(Numbers.parseLong(nativeLPSZ, index + 1, nativeLPSZ.length()));
                    } catch (NumericException e) {
                        LOG.error().$("unknown directory [table=").utf8(tableName).$(", dir=").utf8(nativeLPSZ).$(']').$();
                    }
                }
            }
        }
    }

    @Override
    protected boolean doRun(int workerId, long cursor) {
        final O3PurgeDiscoveryTask task = queue.get(cursor);
        try {
            return discoverPartitions(
                    configuration.getFilesFacade(),
                    sink[workerId],
                    nativeLPSZ[workerId],
                    txnList[workerId],
                    purgeQueue,
                    purgePubSeq,
                    configuration.getRoot(),
                    task.getTableName(),
                    task.getPartitionBy(),
                    task.getTimestamp(),
                    task.getTxnScoreboard()
            );
        } finally {
            subSeq.done(cursor);
        }
    }
}
