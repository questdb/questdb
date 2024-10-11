/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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

package io.questdb.mp;

import io.questdb.MessageBus;
import io.questdb.cairo.*;
import io.questdb.cairo.sql.async.PageFrameReduceJob;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.engine.groupby.GroupByMergeShardJob;
import io.questdb.griffin.engine.groupby.vect.GroupByVectorAggregateJob;
import io.questdb.griffin.engine.table.LatestByAllIndexedJob;
import io.questdb.std.NanosecondClock;
import io.questdb.std.Rnd;
import io.questdb.std.datetime.microtime.MicrosecondClock;

public class WorkerPoolUtils {

    public static void setupQueryJobs(
            WorkerPool workerPool,
            CairoEngine cairoEngine
    ) {
        final CairoConfiguration configuration = cairoEngine.getConfiguration();
        final MessageBus messageBus = cairoEngine.getMessageBus();
        final int workerCount = workerPool.getWorkerCount();

        workerPool.assign(new LatestByAllIndexedJob(messageBus));

        if (configuration.isSqlParallelGroupByEnabled()) {
            workerPool.assign(new GroupByVectorAggregateJob(messageBus));
            workerPool.assign(new GroupByMergeShardJob(messageBus));
        }

        if (configuration.isSqlParallelFilterEnabled() || configuration.isSqlParallelGroupByEnabled()) {
            final MicrosecondClock microsecondClock = messageBus.getConfiguration().getMicrosecondClock();
            final NanosecondClock nanosecondClock = messageBus.getConfiguration().getNanosecondClock();
            for (int i = 0; i < workerCount; i++) {
                // create job per worker to allow each worker to have own shard walk sequence
                final PageFrameReduceJob pageFrameReduceJob = new PageFrameReduceJob(
                        messageBus,
                        new Rnd(microsecondClock.getTicks(), nanosecondClock.getTicks()),
                        configuration.getCircuitBreakerConfiguration()
                );
                workerPool.assign(i, pageFrameReduceJob);
                workerPool.freeOnExit(pageFrameReduceJob);
            }
        }
    }

    public static void setupWriterJobs(WorkerPool workerPool, CairoEngine cairoEngine) throws SqlException {
        final MessageBus messageBus = cairoEngine.getMessageBus();
        final O3PartitionPurgeJob purgeDiscoveryJob = new O3PartitionPurgeJob(
                cairoEngine,
                workerPool.getWorkerCount()
        );
        workerPool.freeOnExit(purgeDiscoveryJob);
        workerPool.assign(purgeDiscoveryJob);

        // ColumnPurgeJob has expensive init (it creates a table), disable it in some tests.
        if (!cairoEngine.getConfiguration().disableColumnPurgeJob()) {
            final ColumnPurgeJob columnPurgeJob = new ColumnPurgeJob(cairoEngine);
            workerPool.freeOnExit(columnPurgeJob);
            workerPool.assign(columnPurgeJob);
        }

        workerPool.assign(new ColumnIndexerJob(messageBus));
        workerPool.assign(new O3PartitionJob(messageBus));
        workerPool.assign(new O3OpenColumnJob(messageBus));
        workerPool.assign(new O3CopyJob(messageBus));
        workerPool.assign(new ColumnTaskJob(messageBus));
    }
}
