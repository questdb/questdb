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

public class O3PurgeDiscoveryJob extends AbstractQueueConsumerJob<O3PurgeDiscoveryTask> {

    private final static Log LOG = LogFactory.getLog(O3PurgeDiscoveryJob.class);
    private final CairoConfiguration configuration;
    private final MutableCharSink sink = new StringSink();
    private final NativeLPSZ nativeLPSZ = new NativeLPSZ();
    private final LongList txnList = new LongList();
    private final RingQueue<O3PurgeTask> purgeQueue;
    private final Sequence purgePubSeq;

    public O3PurgeDiscoveryJob(
            CairoConfiguration configuration,
            RingQueue<O3PurgeDiscoveryTask> queue,
            Sequence subSeq,
            RingQueue<O3PurgeTask> purgeQueue,
            Sequence purgePubSeq
    ) {
        super(queue, subSeq);
        this.configuration = configuration;
        this.purgeQueue = purgeQueue;
        this.purgePubSeq = purgePubSeq;
    }

    @Override
    protected boolean doRun(int workerId, long cursor) {
        final O3PurgeDiscoveryTask task = queue.get(cursor);
        try {
            return discoverPartitions(
                    configuration.getFilesFacade(),
                    sink,
                    nativeLPSZ,
                    txnList,
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

    public static boolean discoverPartitions(
            FilesFacade ff,
            MutableCharSink sink,
            NativeLPSZ nativeLPSZ,
            LongList txnList,
            RingQueue<O3PurgeTask> purgeQueue,
            Sequence purgePubSeq,
            CharSequence root,
            CharSequence tableName,
            int partitionBy,
            long partitionTimestamp,
            long txnScoreboard
    ) {
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
                final long minTxnToExpect = txnList.getQuick(i+1);
                path.of(root).concat(tableName);
                if (!O3PurgeJob.purgePartitionDir(
                        ff,
                        path,
                        partitionBy,
                        partitionTimestamp,
                        txnScoreboard,
                        nameTxnToRemove,
                        minTxnToExpect
                )) {
                    // queue the job
                    long cursor = purgePubSeq.next();
                    if (cursor > -1) {
                        O3PurgeTask task = purgeQueue.get(cursor);
                        task.of(
                                tableName,
                                partitionBy,
                                txnScoreboard,
                                partitionTimestamp,
                                nameTxnToRemove,
                                minTxnToExpect
                        );
                        purgePubSeq.done(cursor);
                    } else {
                        LOG.error().$("purge queue is full [table=").$(tableName).$(", ts=").$ts(partitionTimestamp).$(']').$();
                    }
                }
            }
            return true;
        }

        return false;
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
}
