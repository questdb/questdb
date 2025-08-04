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
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.ColumnIndexerJob;
import io.questdb.cairo.ColumnPurgeJob;
import io.questdb.cairo.ColumnTaskJob;
import io.questdb.cairo.O3CopyJob;
import io.questdb.cairo.O3OpenColumnJob;
import io.questdb.cairo.O3PartitionJob;
import io.questdb.cairo.O3PartitionPurgeJob;
import io.questdb.cairo.sql.async.PageFrameReduceJob;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.engine.groupby.GroupByMergeShardJob;
import io.questdb.griffin.engine.groupby.vect.GroupByVectorAggregateJob;
import io.questdb.griffin.engine.table.LatestByAllIndexedJob;
import io.questdb.std.Rnd;
import io.questdb.std.datetime.Clock;

public class WorkerPoolUtils {

    public static void setupQueryJobs(
            WorkerPool sharedPoolQuery,
            CairoEngine cairoEngine
    ) {
        final CairoConfiguration configuration = cairoEngine.getConfiguration();
        final MessageBus messageBus = cairoEngine.getMessageBus();
        final int workerCount = sharedPoolQuery.getWorkerCount();

        sharedPoolQuery.assign(new LatestByAllIndexedJob(messageBus));

        if (configuration.isSqlParallelGroupByEnabled()) {
            sharedPoolQuery.assign(new GroupByVectorAggregateJob(messageBus));
            sharedPoolQuery.assign(new GroupByMergeShardJob(messageBus));
        }

        if (configuration.isSqlParallelFilterEnabled() || configuration.isSqlParallelGroupByEnabled()) {
            final io.questdb.std.datetime.Clock microsecondClock = messageBus.getConfiguration().getMicrosecondClock();
            final Clock nanosecondClock = messageBus.getConfiguration().getNanosecondClock();
            for (int i = 0; i < workerCount; i++) {
                // create job per worker to allow each worker to have own shard walk sequence
                final PageFrameReduceJob pageFrameReduceJob = new PageFrameReduceJob(
                        messageBus,
                        new Rnd(microsecondClock.getTicks(), nanosecondClock.getTicks()),
                        configuration.getCircuitBreakerConfiguration()
                );
                sharedPoolQuery.assign(i, pageFrameReduceJob);
                sharedPoolQuery.freeOnExit(pageFrameReduceJob);
            }
        }
    }

    public static void setupWriterJobs(WorkerPool sharedPoolWrite, CairoEngine cairoEngine) throws SqlException {
        final MessageBus messageBus = cairoEngine.getMessageBus();
        final O3PartitionPurgeJob purgeDiscoveryJob = new O3PartitionPurgeJob(
                cairoEngine,
                sharedPoolWrite.getWorkerCount()
        );
        sharedPoolWrite.freeOnExit(purgeDiscoveryJob);
        sharedPoolWrite.assign(purgeDiscoveryJob);

        // ColumnPurgeJob has expensive init (it creates a table), disable it in some tests.
        if (!cairoEngine.getConfiguration().disableColumnPurgeJob()) {
            final ColumnPurgeJob columnPurgeJob = new ColumnPurgeJob(cairoEngine);
            sharedPoolWrite.freeOnExit(columnPurgeJob);
            sharedPoolWrite.assign(columnPurgeJob);
        }

        sharedPoolWrite.assign(new ColumnIndexerJob(messageBus));
        sharedPoolWrite.assign(new O3PartitionJob(messageBus));
        sharedPoolWrite.assign(new O3OpenColumnJob(messageBus));
        sharedPoolWrite.assign(new O3CopyJob(messageBus));
        sharedPoolWrite.assign(new ColumnTaskJob(messageBus));
    }
}
