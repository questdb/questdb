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

package io.questdb;

import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.metrics.Target;
import io.questdb.mp.Worker;
import io.questdb.mp.WorkerPool;
import io.questdb.mp.WorkerPoolConfiguration;
import io.questdb.std.CharSequenceObjHashMap;
import io.questdb.std.ObjList;
import io.questdb.std.str.BorrowableUtf8Sink;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.concurrent.atomic.AtomicBoolean;

public abstract class WorkerPoolManager implements Target {

    private static final Log LOG = LogFactory.getLog(WorkerPoolManager.class);
    protected final WorkerPool sharedPoolIO;
    // When parellel querying is disabled, query pool will be null. All IO and Writer pools will always be created.
    @Nullable
    protected final WorkerPool sharedPoolQuery;
    protected final WorkerPool sharedPoolWrite;
    private final AtomicBoolean closed = new AtomicBoolean();
    private final CharSequenceObjHashMap<WorkerPool> dedicatedPools = new CharSequenceObjHashMap<>(4);
    private final AtomicBoolean running = new AtomicBoolean();

    public WorkerPoolManager(ServerConfiguration config) {
        sharedPoolIO = new WorkerPool(config.getIOWorkerPoolConfiguration());
        sharedPoolQuery = config.getQueryWorkerPoolConfiguration().getWorkerCount() > 0 ? new WorkerPool(config.getQueryWorkerPoolConfiguration()) : null;
        sharedPoolWrite = new WorkerPool(config.getWriteWorkerPoolConfiguration());

        WorkerPool queryPool = sharedPoolQuery != null ? sharedPoolQuery : sharedPoolIO;
        configureSharedPool(sharedPoolIO, queryPool, sharedPoolWrite); // abstract method giving callers the chance to assign jobs
        config.getMetrics().addScrapable(this);
    }

    public WorkerPool getInstanceIO(@NotNull WorkerPoolConfiguration config, @NotNull Requester requester) {
        return getWorkerPool(config, requester, sharedPoolIO);
    }

    public WorkerPool getInstanceWrite(@NotNull WorkerPoolConfiguration config, @NotNull Requester requester) {
        return getWorkerPool(config, requester, sharedPoolWrite);
    }

    public WorkerPool getSharedPoolIO() {
        return sharedPoolIO;
    }

    public int getSharedWorkerCount() {
        return sharedPoolIO.getWorkerCount();
    }

    public void halt() {
        // halt is idempotent, and start may have not been called, still
        // we want to free pool resources, so we do not check the closed
        // flag, but we ensure it is true at the end.
        ObjList<CharSequence> poolNames = dedicatedPools.keys();
        for (int i = 0, limit = poolNames.size(); i < limit; i++) {
            CharSequence name = poolNames.getQuick(i);
            WorkerPool pool = dedicatedPools.get(name);
            closePool(pool, "closing dedicated pool [name=");
        }
        dedicatedPools.clear();

        closePool(sharedPoolIO, "closing shared IO pool [name=");
        closePool(sharedPoolQuery, "closing shared Read pool [name=");
        closePool(sharedPoolWrite, "closing shared Write pool [name=");

        closed.set(true);
    }

    @Override
    public void scrapeIntoPrometheus(@NotNull BorrowableUtf8Sink sink) {
        long now = Worker.CLOCK_MICROS.getTicks();
        sharedPoolIO.updateWorkerMetrics(now);
        if (sharedPoolQuery != null) {
            sharedPoolQuery.updateWorkerMetrics(now);
        }
        sharedPoolWrite.updateWorkerMetrics(now);
        ObjList<CharSequence> poolNames = dedicatedPools.keys();
        for (int i = 0, limit = poolNames.size(); i < limit; i++) {
            dedicatedPools.get(poolNames.getQuick(i)).updateWorkerMetrics(now);
        }
    }

    public void start(Log sharedPoolLog) {
        if (running.compareAndSet(false, true)) {
            startWorkerPool(sharedPoolLog, sharedPoolIO, "started shared pool [name=");
            startWorkerPool(sharedPoolLog, sharedPoolQuery, "started shared pool [name=");
            startWorkerPool(sharedPoolLog, sharedPoolWrite, "started shared pool [name=");

            ObjList<CharSequence> poolNames = dedicatedPools.keys();
            for (int i = 0, limit = poolNames.size(); i < limit; i++) {
                CharSequence name = poolNames.get(i);
                WorkerPool pool = dedicatedPools.get(name);

                startWorkerPool(sharedPoolLog, pool, "started dedicated pool [name=");
            }
        }
    }

    private static void startWorkerPool(Log sharedPoolLog, WorkerPool p, String msg) {
        if (p != null) {
            p.start(sharedPoolLog);
            LOG.info().$(msg).$(p.getPoolName())
                    .$(", workers=").$(p.getWorkerCount())
                    .I$();
        }
    }

    private void closePool(WorkerPool p, String message) {
        if (p != null) {
            LOG.info().$(message).$(p.getPoolName())
                    .$(", workers=").$(p.getWorkerCount())
                    .I$();
            p.halt();
        }
    }

    @NotNull
    private WorkerPool getWorkerPool(@NotNull WorkerPoolConfiguration config, @NotNull Requester requester, WorkerPool sharedPool) {
        if (running.get() || closed.get()) {
            throw new IllegalStateException("can only get instance before start");
        }

        if (config.getWorkerCount() < 1) {
            LOG.info().$("using SHARED pool [requester=").$(requester)
                    .$(", workers=").$(sharedPool.getWorkerCount())
                    .$(", pool=").$(sharedPool.getPoolName())
                    .I$();
            return sharedPool;
        }

        String poolName = config.getPoolName();
        WorkerPool pool = dedicatedPools.get(poolName);
        if (pool == null) {
            pool = new WorkerPool(config);
            dedicatedPools.put(poolName, pool);
        }
        LOG.info().$("new DEDICATED pool [name=").$(poolName)
                .$(", requester=").$(requester)
                .$(", workers=").$(pool.getWorkerCount())
                .$(", priority=").$(config.workerPoolPriority())
                .I$();
        return pool;
    }

    /**
     * @param sharedPoolIo A reference to the IO SHARED pool
     * @param sharedPoolR  A reference to the READ SHARED pool
     * @param sharedPoolW  A reference to the WRITE SHARED pool
     */
    protected abstract void configureSharedPool(final WorkerPool sharedPoolIo, final WorkerPool sharedPoolR, final WorkerPool sharedPoolW);

    public enum Requester {

        HTTP_SERVER("http"),
        HTTP_MIN_SERVER("min-http"),
        PG_WIRE_SERVER("pg-wire"),
        LINE_TCP_IO("line-tcp-io"),
        LINE_TCP_WRITER("line-tcp-writer"),
        OTHER("other"),
        WAL_APPLY("wal-apply"),
        MAT_VIEW_REFRESH("mat-view-refresh");

        private final String requester;

        Requester(String requester) {
            this.requester = requester;
        }

        @Override
        public String toString() {
            return requester;
        }
    }
}
