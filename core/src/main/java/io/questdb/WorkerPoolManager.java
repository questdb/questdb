/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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
import io.questdb.metrics.Scrapable;
import io.questdb.mp.Worker;
import io.questdb.mp.WorkerPool;
import io.questdb.mp.WorkerPoolConfiguration;
import io.questdb.std.CharSequenceObjHashMap;
import io.questdb.std.ObjList;
import io.questdb.std.str.BorrowableUtf8Sink;
import org.jetbrains.annotations.NotNull;

import java.util.concurrent.atomic.AtomicBoolean;

public abstract class WorkerPoolManager implements Scrapable {

    private static final Log LOG = LogFactory.getLog(WorkerPoolManager.class);
    protected final WorkerPool sharedPool;
    private final AtomicBoolean closed = new AtomicBoolean();
    private final CharSequenceObjHashMap<WorkerPool> dedicatedPools = new CharSequenceObjHashMap<>(4);
    private final AtomicBoolean running = new AtomicBoolean();

    public WorkerPoolManager(ServerConfiguration config, Metrics metrics) {
        sharedPool = new WorkerPool(config.getWorkerPoolConfiguration(), metrics);
        configureSharedPool(sharedPool); // abstract method giving callers the chance to assign jobs
        metrics.addScrapable(this);
    }

    public WorkerPool getInstance(@NotNull WorkerPoolConfiguration config, @NotNull Metrics metrics, @NotNull Requester requester) {
        if (running.get() || closed.get()) {
            throw new IllegalStateException("can only get instance before start");
        }

        if (config.getWorkerCount() < 1) {
            LOG.info().$("using SHARED pool [requester=").$(requester)
                    .$(", workers=").$(sharedPool.getWorkerCount())
                    .I$();
            return sharedPool;
        }

        String poolName = config.getPoolName();
        WorkerPool pool = dedicatedPools.get(poolName);
        if (pool == null) {
            pool = new WorkerPool(config, metrics);
            dedicatedPools.put(poolName, pool);
        }
        LOG.info().$("new DEDICATED pool [name=").$(poolName)
                .$(", requester=").$(requester)
                .$(", workers=").$(pool.getWorkerCount())
                .I$();
        return pool;
    }

    public WorkerPool getSharedPool() {
        return sharedPool;
    }

    public int getSharedWorkerCount() {
        return sharedPool.getWorkerCount();
    }

    public void halt() {
        // halt is idempotent, and start may have not been called, still
        // we want to free pool resources, so we do not check the closed
        // flag, but we ensure it is true at the end.
        ObjList<CharSequence> poolNames = dedicatedPools.keys();
        for (int i = 0, limit = poolNames.size(); i < limit; i++) {
            CharSequence name = poolNames.getQuick(i);
            WorkerPool pool = dedicatedPools.get(name);
            LOG.info().$("closing dedicated pool [name=").$(name)
                    .$(", workers=").$(pool.getWorkerCount())
                    .I$();
            pool.halt();
        }
        dedicatedPools.clear();

        LOG.info().$("closing shared pool [name=").$(sharedPool.getPoolName())
                .$(", workers=").$(sharedPool.getWorkerCount())
                .I$();
        sharedPool.halt();
        closed.set(true);
    }

    @Override
    public void scrapeIntoPrometheus(@NotNull BorrowableUtf8Sink sink) {
        long now = Worker.CLOCK_MICROS.getTicks();
        sharedPool.updateWorkerMetrics(now);
        ObjList<CharSequence> poolNames = dedicatedPools.keys();
        for (int i = 0, limit = poolNames.size(); i < limit; i++) {
            dedicatedPools.get(poolNames.getQuick(i)).updateWorkerMetrics(now);
        }
    }

    public void start(Log sharedPoolLog) {
        if (running.compareAndSet(false, true)) {
            sharedPool.start(sharedPoolLog);
            LOG.info().$("started shared pool [name=").$(sharedPool.getPoolName())
                    .$(", workers=").$(sharedPool.getWorkerCount())
                    .I$();

            ObjList<CharSequence> poolNames = dedicatedPools.keys();
            for (int i = 0, limit = poolNames.size(); i < limit; i++) {
                CharSequence name = poolNames.get(i);
                WorkerPool pool = dedicatedPools.get(name);
                pool.start(sharedPoolLog);
                LOG.info().$("started dedicated pool [name=").$(name)
                        .$(", workers=").$(pool.getWorkerCount())
                        .I$();
            }
        }
    }

    /**
     * @param sharedPool A reference to the SHARED pool
     */
    protected abstract void configureSharedPool(final WorkerPool sharedPool);

    public enum Requester {

        HTTP_SERVER("http"),
        HTTP_MIN_SERVER("min-http"),
        PG_WIRE_SERVER("pg-wire"),
        LINE_TCP_IO("line-tcp-io"),
        LINE_TCP_WRITER("line-tcp-writer"),
        OTHER("other"),
        WAL_APPLY("wal-apply");

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
