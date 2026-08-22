/*+*****************************************************************************
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
import io.questdb.cairo.PostingSealPurgeJob;
import io.questdb.cairo.sql.async.PageFrameReduceDispatcher;
import io.questdb.cairo.sql.async.PageFrameReduceJob;
import io.questdb.cairo.sql.async.QueryParallelFiberDispatcher;
import io.questdb.cairo.sql.async.UnorderedPageFrameReduceJob;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.engine.groupby.GroupByLongTopKJob;
import io.questdb.griffin.engine.groupby.GroupByMergeShardJob;
import io.questdb.griffin.engine.groupby.vect.GroupByVectorAggregateJob;
import io.questdb.griffin.engine.table.LatestByAllIndexedJob;
import io.questdb.std.AsyncMunmapJob;
import io.questdb.std.Files;
import io.questdb.std.Misc;
import io.questdb.std.Os;
import io.questdb.std.Rnd;
import io.questdb.std.datetime.Clock;

public class WorkerPoolUtils {

    public static void setupAsyncMunmapJob(WorkerPool pool, CairoEngine engine) {
        CairoConfiguration config = engine.getConfiguration();
        if (config.getAsyncMunmapEnabled()) {
            assert Os.isPosix();
            Files.ASYNC_MUNMAP_ENABLED = true;
            AsyncMunmapJob asyncMunmapJob = new AsyncMunmapJob();
            pool.assign(asyncMunmapJob);
        } else {
            Files.ASYNC_MUNMAP_ENABLED = false;
        }
    }

    public static void setupQueryJobs(
            WorkerPool sharedPoolQuery,
            CairoEngine cairoEngine
    ) {
        setupQueryJobs(sharedPoolQuery, cairoEngine, false);
    }

    /**
     * @param isFiberDispatcherAllowed pass true only when {@code sharedPoolQuery} is dedicated to
     *                                 query work. A pool that also hosts protocol fibers must not own
     *                                 a query dispatcher because same-runtime fan-out is refused.
     */
    public static void setupQueryJobs(
            WorkerPool sharedPoolQuery,
            CairoEngine cairoEngine,
            boolean isFiberDispatcherAllowed
    ) {
        final CairoConfiguration configuration = cairoEngine.getConfiguration();
        final MessageBus messageBus = cairoEngine.getMessageBus();

        if (isFiberDispatcherAllowed && sharedPoolQuery.isFiberHost()) {
            final QueryParallelFiberDispatcher dispatcher = new QueryParallelFiberDispatcher(
                    cairoEngine,
                    messageBus,
                    sharedPoolQuery.getFiberRuntime()
            );
            try {
                messageBus.setQueryParallelFiberDispatcher(dispatcher);
                sharedPoolQuery.freeResourceOnExit(dispatcher);
            } catch (Throwable th) {
                Misc.free(dispatcher, th);
                throw th;
            }
        }

        sharedPoolQuery.assign(new LatestByAllIndexedJob(messageBus));

        if (configuration.isSqlParallelGroupByEnabled()) {
            sharedPoolQuery.assign(new GroupByVectorAggregateJob(messageBus));
            sharedPoolQuery.assign(new GroupByMergeShardJob(messageBus));
            sharedPoolQuery.assign(new GroupByLongTopKJob(messageBus));
        }

        if (configuration.isSqlParallelFilterEnabled() || configuration.isSqlParallelGroupByEnabled()) {
            if (isFiberDispatcherAllowed && sharedPoolQuery.isFiberHost()) {
                final PageFrameReduceDispatcher dispatcher = new PageFrameReduceDispatcher(
                        cairoEngine,
                        messageBus,
                        sharedPoolQuery.getFiberRuntime()
                );
                try {
                    messageBus.setPageFrameReduceDispatcher(dispatcher);
                    sharedPoolQuery.freeResourceOnExit(dispatcher);
                } catch (Throwable th) {
                    Misc.free(dispatcher, th);
                    throw th;
                }
            }
            final io.questdb.std.datetime.Clock microsecondClock = messageBus.getConfiguration().getMicrosecondClock();
            final Clock nanosecondClock = messageBus.getConfiguration().getNanosecondClock();
            sharedPoolQuery.assign(new PageFrameReduceJob(
                    cairoEngine,
                    messageBus,
                    new Rnd(microsecondClock.getTicks(), nanosecondClock.getTicks())
            ));
            sharedPoolQuery.assign(new UnorderedPageFrameReduceJob(cairoEngine, messageBus));
        }
    }

    public static void setupWriterJobs(WorkerPool sharedPoolWrite, CairoEngine cairoEngine) throws SqlException {
        final MessageBus messageBus = cairoEngine.getMessageBus();
        sharedPoolWrite.assign(new O3PartitionPurgeJob(cairoEngine));

        // ColumnPurgeJob has expensive init (it creates a table), disable it in some tests.
        if (!cairoEngine.getConfiguration().disableColumnPurgeJob()) {
            final ColumnPurgeJob columnPurgeJob = new ColumnPurgeJob(cairoEngine);
            sharedPoolWrite.freeOnExit(columnPurgeJob);
            sharedPoolWrite.assign(columnPurgeJob);

            final PostingSealPurgeJob postingSealPurgeJob = new PostingSealPurgeJob(cairoEngine);
            sharedPoolWrite.freeOnExit(postingSealPurgeJob);
            sharedPoolWrite.assign(postingSealPurgeJob);
        }

        sharedPoolWrite.assign(new ColumnIndexerJob(messageBus));
        sharedPoolWrite.assign(new O3PartitionJob(messageBus));
        sharedPoolWrite.assign(new O3OpenColumnJob(messageBus));
        sharedPoolWrite.assign(new O3CopyJob(messageBus));
        sharedPoolWrite.assign(new ColumnTaskJob(messageBus));
    }
}
