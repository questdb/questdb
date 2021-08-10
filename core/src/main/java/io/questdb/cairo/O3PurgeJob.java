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
import io.questdb.std.FilesFacade;
import io.questdb.std.str.Path;
import io.questdb.tasks.O3PurgeTask;

public class O3PurgeJob extends AbstractQueueConsumerJob<O3PurgeTask> {

    private static final Log LOG = LogFactory.getLog(O3PurgeJob.class);

    private final CairoConfiguration configuration;

    public O3PurgeJob(MessageBus messageBus) {
        super(messageBus.getO3PurgeQueue(), messageBus.getO3PurgeSubSeq());
        this.configuration = messageBus.getConfiguration();
    }

    public static int purgePartitionDir(
            FilesFacade ff,
            Path path,
            int partitionBy,
            long partitionTimestamp,
            TxnScoreboard txnScoreboard,
            long nameTxnToRemove,
            long minTxnToExpect
    ) {
        final long readerTxn = txnScoreboard.getMin();
        final long readerTxnCount = txnScoreboard.getActiveReaderCount(readerTxn);
        int errno = -1;
        if (txnScoreboard.isTxnAvailable(minTxnToExpect)) {
            LOG.info().
                    $("purging [path=").$(path)
                    .$(", readerTxn=").$(readerTxn)
                    .$(", readerTxnCount=").$(readerTxnCount)
                    .$(", minTxnToExpect=").$(minTxnToExpect)
                    .$(", nameTxnToRemove=").$(nameTxnToRemove)
                    .I$();
            TableUtils.setPathForPartition(path, partitionBy, partitionTimestamp, false);
            TableUtils.txnPartitionConditionally(path, nameTxnToRemove);
            path.slash$();
            if ((errno = ff.rmdir(path)) == 0) {
                LOG.info().
                        $("purged [path=").$(path)
                        .$(", readerTxn=").$(readerTxn)
                        .$(", readerTxnCount=").$(readerTxnCount)
                        .$(", minTxnToExpect=").$(minTxnToExpect)
                        .$(", nameTxnToRemove=").$(nameTxnToRemove)
                        .I$();
                return 0;
            }
        }
        return errno;
    }

    @Override
    protected boolean doRun(int workerId, long cursor) {
        final O3PurgeTask task = queue.get(cursor);
        int errno = purgePartitionDir(
                configuration.getFilesFacade(),
                Path.getThreadLocal(configuration.getRoot()).concat(task.getTableName()),
                task.getPartitionBy(),
                task.getTimestamp(),
                task.getTxnScoreboard(),
                task.getNameTxnToRemove(),
                task.getMinTxnToExpect()
        );
        subSeq.done(cursor);

        if (errno == 0) {
            return true;
        } else {
            LOG.info()
                    .$("could not purge, re-queue? [table=").$(task.getTableName())
                    .$(", ts=").$ts(task.getTimestamp())
                    .$(", txn=").$(task.getNameTxnToRemove())
                    .$(", errno=").$(errno)
                    .$(']').$();
            return false;
        }
    }
}
