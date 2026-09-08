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

package io.questdb;

import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.metrics.Target;
import io.questdb.mp.Worker;
import io.questdb.mp.WorkerPool;
import io.questdb.mp.WorkerPoolConfiguration;
import io.questdb.std.CharSequenceObjHashMap;
import io.questdb.std.Misc;
import io.questdb.std.ReadOnlyObjList;
import io.questdb.std.str.BorrowableUtf8Sink;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

public abstract class WorkerPoolManager implements Target {

    private static final Log LOG = LogFactory.getLog(WorkerPoolManager.class);
    protected final WorkerPool sharedPoolNetwork;
    // When parallel querying is disabled, query pool will be null. All Network and Write pools will always be created.
    @Nullable
    protected final WorkerPool sharedPoolQuery;
    protected final WorkerPool sharedPoolWrite;
    private final AtomicBoolean closed = new AtomicBoolean();
    private final CharSequenceObjHashMap<WorkerPool> dedicatedPools = new CharSequenceObjHashMap<>(4);
    @Nullable
    private Throwable haltFailure;
    private final ReentrantLock haltLock = new ReentrantLock();
    @Nullable
    private WorkerPool lineTcpIOPool;
    @Nullable
    private WorkerPool lineTcpWriterPool;
    private final AtomicBoolean running = new AtomicBoolean();

    public WorkerPoolManager(ServerConfiguration config) {
        WorkerPool networkPool = null;
        WorkerPool queryPool = null;
        WorkerPool writePool = null;
        try {
            networkPool = new WorkerPool(config.getSharedWorkerPoolNetworkConfiguration());
            queryPool = config.getSharedWorkerPoolQueryConfiguration().getWorkerCount() > 0
                    ? new WorkerPool(config.getSharedWorkerPoolQueryConfiguration())
                    : null;
            writePool = new WorkerPool(config.getSharedWorkerPoolWriteConfiguration());
            sharedPoolNetwork = networkPool;
            sharedPoolQuery = queryPool;
            sharedPoolWrite = writePool;

            configureWorkerPools(queryPool != null ? queryPool : networkPool, writePool);
            config.getMetrics().addScrapable(this);
        } catch (Throwable th) {
            rollbackConstruction(networkPool, queryPool, writePool, th);
            throw th;
        }
    }

    @Nullable
    public Throwable getHaltFailure() {
        haltLock.lock();
        try {
            return haltFailure;
        } finally {
            haltLock.unlock();
        }
    }

    public WorkerPool getSharedPoolNetwork(@NotNull WorkerPoolConfiguration config, @NotNull RequesterName requesterName) {
        return getWorkerPool(config, requesterName, sharedPoolNetwork);
    }

    public WorkerPool getSharedPoolNetwork() {
        return sharedPoolNetwork;
    }

    public WorkerPool getSharedPoolWrite(@NotNull WorkerPoolConfiguration config, @NotNull RequesterName requesterName) {
        return getWorkerPool(config, requesterName, sharedPoolWrite);
    }

    public int getSharedQueryWorkerCount() {
        return sharedPoolQuery != null ? sharedPoolQuery.getWorkerCount() : 0;
    }

    @NotNull
    public WorkerPool getWorkerPool(@NotNull WorkerPoolConfiguration config, @NotNull RequesterName requesterName, WorkerPool sharedPool) {
        if (running.get() || closed.get()) {
            throw new IllegalStateException("can only get instance before start");
        }

        final WorkerPool pool;
        if (config.getWorkerCount() < 1) {
            LOG.info().$("default thread pool [requester=").$(requesterName)
                    .$(", workers=").$(sharedPool.getWorkerCount())
                    .$(", pool=").$(sharedPool.getPoolName())
                    .I$();
            pool = sharedPool;
        } else {
            String poolName = config.getPoolName();
            WorkerPool dedicatedPool = dedicatedPools.get(poolName);
            if (dedicatedPool == null) {
                dedicatedPool = new WorkerPool(config);
                dedicatedPools.put(poolName, dedicatedPool);
            }
            LOG.info().$("custom thread pool [name=").$(poolName)
                    .$(", requester=").$(requesterName)
                    .$(", workers=").$(dedicatedPool.getWorkerCount())
                    .$(", priority=").$(config.workerPoolPriority())
                    .I$();
            pool = dedicatedPool;
        }
        recordPoolRole(pool, requesterName);
        return pool;
    }

    public void halt() {
        haltAndReportCompletion();
    }

    /**
     * Halts every managed pool, bounding the combined wait by an absolute deadline.
     * <p>
     * The deadline is shared across all pools: each pool gets the time remaining until the deadline,
     * so a single wedged pool cannot reset the budget for the next one. This keeps server shutdown
     * bounded even when a worker thread is stuck. Timed-out pools retain worker-owned resources
     * so a later {@link #haltBy(long)} attempt can finish safely.
     *
     * @param deadlineNanos absolute deadline from {@link System#nanoTime()} by which all pools should be halted
     * @deprecated use {@link #haltBy(long)} and inspect its completion result
     */
    @Deprecated
    public void halt(long deadlineNanos) {
        haltBy(deadlineNanos);
    }

    /**
     * Attempts to halt every managed pool and reports whether all cleanup completed.
     */
    public boolean haltAndReportCompletion() {
        haltLock.lock();
        try {
            return isHaltComplete(0);
        } finally {
            haltLock.unlock();
        }
    }

    /**
     * Attempts to halt every managed pool by one absolute {@link System#nanoTime()} deadline
     * shared across all pools, so a wedged pool cannot multiply the budget. A timed-out pool
     * retains its object graph and stays retryable.
     */
    public boolean haltBy(long deadlineNanos) {
        boolean isInterrupted = false;
        boolean isLockAcquired = haltLock.tryLock();
        try {
            while (!isLockAcquired) {
                final long remainingNanos = deadlineNanos - System.nanoTime();
                if (remainingNanos <= 0) {
                    return false;
                }
                try {
                    isLockAcquired = haltLock.tryLock(remainingNanos, TimeUnit.NANOSECONDS);
                } catch (InterruptedException e) {
                    isInterrupted = true;
                }
            }
            return isHaltComplete(deadlineNanos);
        } finally {
            if (isLockAcquired) {
                haltLock.unlock();
            }
            if (isInterrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }

    public boolean haltWithin(long timeoutNanos) {
        return haltBy(System.nanoTime() + timeoutNanos);
    }

    @Override
    public void scrapeIntoPrometheus(@NotNull BorrowableUtf8Sink sink) {
        long now = Worker.CLOCK_MICROS.getTicks();
        sharedPoolNetwork.updateWorkerMetrics(now);
        if (sharedPoolQuery != null) {
            sharedPoolQuery.updateWorkerMetrics(now);
        }
        sharedPoolWrite.updateWorkerMetrics(now);
        ReadOnlyObjList<CharSequence> poolNames = dedicatedPools.keys();
        for (int i = 0, limit = poolNames.size(); i < limit; i++) {
            dedicatedPools.get(poolNames.getQuick(i)).updateWorkerMetrics(now);
        }
    }

    public void start(Log sharedPoolLog) {
        if (running.compareAndSet(false, true)) {
            startWorkerPool(sharedPoolLog, sharedPoolNetwork, "started shared pool [name=");
            startWorkerPool(sharedPoolLog, sharedPoolQuery, "started shared pool [name=");
            startWorkerPool(sharedPoolLog, sharedPoolWrite, "started shared pool [name=");

            ReadOnlyObjList<CharSequence> poolNames = dedicatedPools.keys();
            for (int i = 0, limit = poolNames.size(); i < limit; i++) {
                CharSequence name = poolNames.get(i);
                WorkerPool pool = dedicatedPools.get(name);

                startWorkerPool(sharedPoolLog, pool, "started dedicated pool [name=");
            }
        }
    }

    private static boolean haltPool(WorkerPool p, long deadlineNanos) {
        if (deadlineNanos != 0) {
            return p.haltBy(deadlineNanos);
        }
        p.halt();
        return true;
    }

    private static void startWorkerPool(Log sharedPoolLog, WorkerPool p, String msg) {
        if (p != null) {
            p.start(sharedPoolLog);
            LOG.debug().$(msg).$(p.getPoolName())
                    .$(", workers=").$(p.getWorkerCount())
                    .I$();
        }
    }

    private boolean closePool(WorkerPool p, String message, long deadlineNanos) {
        if (p != null) {
            try {
                LOG.debug().$(message).$(p.getPoolName())
                        .$(", workers=").$(p.getWorkerCount())
                        .I$();
            } catch (Throwable ignore) {
            }
            try {
                return haltPool(p, deadlineNanos);
            } catch (Throwable th) {
                recordHaltFailure(th);
                try {
                    LOG.error().$("worker pool cleanup failed [pool=").$(p.getPoolName())
                            .$(", error=").$(th).I$();
                } catch (Throwable ignore) {
                }
                try {
                    return haltPool(p, deadlineNanos);
                } catch (Throwable retryFailure) {
                    recordHaltFailure(retryFailure);
                    try {
                        LOG.error().$("worker pool cleanup retry failed [pool=").$(p.getPoolName())
                                .$(", error=").$(retryFailure).I$();
                    } catch (Throwable ignore) {
                    }
                    return false;
                }
            }
        }
        return true;
    }

    private boolean isHaltComplete(long deadlineNanos) {
        boolean isHaltComplete = true;
        final boolean isLineTcpIOHaltComplete = closePool(
                lineTcpIOPool,
                "closing Line TCP I/O pool [name=",
                deadlineNanos
        );
        isHaltComplete &= isLineTcpIOHaltComplete;
        if (sharedPoolNetwork != lineTcpIOPool && sharedPoolNetwork != lineTcpWriterPool) {
            isHaltComplete &= closePool(sharedPoolNetwork, "closing shared Network pool [name=", deadlineNanos);
        }

        ReadOnlyObjList<CharSequence> poolNames = dedicatedPools.keys();
        for (int i = 0, limit = poolNames.size(); i < limit; i++) {
            CharSequence name = poolNames.getQuick(i);
            WorkerPool pool = dedicatedPools.get(name);
            if (pool != lineTcpIOPool && pool != lineTcpWriterPool) {
                isHaltComplete &= closePool(pool, "closing dedicated pool [name=", deadlineNanos);
            }
        }

        if (sharedPoolQuery != lineTcpIOPool
                && sharedPoolQuery != lineTcpWriterPool
                && sharedPoolQuery != sharedPoolNetwork) {
            isHaltComplete &= closePool(sharedPoolQuery, "closing shared Query pool [name=", deadlineNanos);
        }
        if (isLineTcpIOHaltComplete && lineTcpWriterPool != lineTcpIOPool) {
            isHaltComplete &= closePool(lineTcpWriterPool, "closing Line TCP writer pool [name=", deadlineNanos);
        } else if (!isLineTcpIOHaltComplete && lineTcpWriterPool != null && lineTcpWriterPool != lineTcpIOPool) {
            try {
                LOG.error().$("retaining Line TCP writer pool because Line TCP I/O pool did not halt [name=")
                        .$(lineTcpWriterPool.getPoolName()).I$();
            } catch (Throwable ignore) {
            }
        }
        if (sharedPoolWrite != lineTcpIOPool
                && sharedPoolWrite != lineTcpWriterPool
                && sharedPoolWrite != sharedPoolNetwork
                && sharedPoolWrite != sharedPoolQuery) {
            isHaltComplete &= closePool(sharedPoolWrite, "closing shared Write pool [name=", deadlineNanos);
        }

        closed.set(true);
        if (isHaltComplete) {
            dedicatedPools.clear();
        }
        return isHaltComplete;
    }

    private void recordHaltFailure(Throwable failure) {
        if (haltFailure == null) {
            haltFailure = failure;
        } else if (haltFailure != failure) {
            haltFailure.addSuppressed(failure);
        }
    }

    private void recordPoolRole(WorkerPool pool, RequesterName requesterName) {
        if (requesterName == Requester.LINE_TCP_IO) {
            lineTcpIOPool = pool;
        } else if (requesterName == Requester.LINE_TCP_WRITER) {
            lineTcpWriterPool = pool;
        }
    }

    private void rollbackConstruction(
            WorkerPool networkPool,
            WorkerPool queryPool,
            WorkerPool writePool,
            Throwable primary
    ) {
        closed.set(true);
        ReadOnlyObjList<CharSequence> poolNames = dedicatedPools.keys();
        for (int i = 0, limit = poolNames.size(); i < limit; i++) {
            Misc.free(dedicatedPools.get(poolNames.getQuick(i)), primary);
        }
        dedicatedPools.clear();
        Misc.free(networkPool, primary);
        Misc.free(queryPool, primary);
        Misc.free(writePool, primary);
    }

    /**
     * @param sharedPoolQuery A reference to the QUERY SHARED pool
     * @param sharedPoolWrite A reference to the WRITE SHARED pool
     */
    protected abstract void configureWorkerPools(
            final WorkerPool sharedPoolQuery,
            final WorkerPool sharedPoolWrite
    );

    public interface RequesterName {
        String toString();
    }

    public enum Requester implements RequesterName {

        HTTP_SERVER("http"),
        HTTP_MIN_SERVER("min-http"),
        PG_WIRE_SERVER("pg-wire"),
        LINE_TCP_IO("line-tcp-io"),
        LINE_TCP_WRITER("line-tcp-writer"),
        OTHER("other"),
        WAL_APPLY("wal-apply"),
        VIEW_COMPILER("view-compiler"),
        MAT_VIEW_REFRESH("mat-view-refresh"),
        LIVE_VIEW_REFRESH("live-view-refresh"),
        EXPORT("export");

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
