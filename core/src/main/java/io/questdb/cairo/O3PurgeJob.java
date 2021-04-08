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

    public static boolean purgePartitionDir(
            FilesFacade ff,
            Path path,
            int partitionBy,
            long partitionTimestamp,
            long txnScoreboard,
            long nameTxnToRemove,
            long minTxnToExpect
    ) {
        if (TxnScoreboard.isTxnUnused(minTxnToExpect, txnScoreboard)) {
            TableUtils.setPathForPartition(path, partitionBy, partitionTimestamp, false);
            TableUtils.txnPartitionConditionally(path, nameTxnToRemove);
            path.$$dir();
            if (ff.rmdir(path)) {
                LOG.info().$("purged [path=").$(path).$(']').$();
                return true;
            }
        }
        LOG.info().$("queuing [path=").$(path).$(']').$();
        return false;
    }

    @Override
    protected boolean doRun(int workerId, long cursor) {
        final O3PurgeTask task = queue.get(cursor);
        try {
            purgePartitionDir(
                    configuration.getFilesFacade(),
                    Path.getThreadLocal(configuration.getRoot()).concat(task.getTableName()),
                    task.getPartitionBy(),
                    task.getTimestamp(),
                    task.getTxnScoreboard(),
                    task.getNameTxnToRemove(),
                    task.getMinTxnToExpect()
            );
            return true;
        } finally {
            subSeq.done(cursor);
        }
    }
}
