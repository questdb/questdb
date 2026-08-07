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

package io.questdb.cairo.wal;

import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.TableToken;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.Job;
import io.questdb.mp.MCSequence;
import io.questdb.mp.RingQueue;
import io.questdb.mp.continuation.Fiber;
import io.questdb.mp.continuation.FiberRuntime;
import io.questdb.mp.continuation.FiberRuntimeState;
import io.questdb.mp.continuation.LaunchResult;
import io.questdb.std.ConcurrentHashMap;
import io.questdb.std.Os;
import io.questdb.tasks.WalTxnNotificationTask;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

import java.io.Closeable;

public final class WalApplyFiberJob implements Closeable, Job {
    private static final Log LOG = LogFactory.getLog(WalApplyFiberJob.class);
    private volatile @Nullable Runnable beforeEvictForTesting;
    private final CairoEngine engine;
    private final WalApplyExecutorPool executorPool;
    private volatile boolean isClosed;
    private final RingQueue<WalTxnNotificationTask> queue;
    private final FiberRuntime runtime;
    private final MCSequence subSeq;
    private final ConcurrentHashMap<WalApplyFiberTask> tasks = new ConcurrentHashMap<>();

    public WalApplyFiberJob(CairoEngine engine, int sharedQueryWorkerCount, FiberRuntime runtime) {
        this(engine, sharedQueryWorkerCount, runtime, 1);
    }

    public WalApplyFiberJob(
            CairoEngine engine,
            int sharedQueryWorkerCount,
            FiberRuntime runtime,
            int maxRetainedExecutorCount
    ) {
        if (maxRetainedExecutorCount < 1) {
            throw new IllegalArgumentException("WAL apply executor retention must be positive");
        }
        this.engine = engine;
        this.runtime = runtime;
        this.executorPool = new WalApplyExecutorPool(
                engine,
                sharedQueryWorkerCount,
                runtime.getMaxLiveFiberCount(),
                Math.min(maxRetainedExecutorCount, runtime.getMaxLiveFiberCount())
        );
        this.queue = engine.getMessageBus().getWalTxnNotificationQueue();
        this.subSeq = engine.getMessageBus().getWalTxnNotificationSubSequence();
    }

    @Override
    public synchronized void close() {
        if (isClosed) {
            return;
        }
        isClosed = true;
        try {
            executorPool.close();
        } finally {
            tasks.clear();
        }
    }

    @TestOnly
    public int getExecutorCount() {
        return executorPool.getCreatedCount();
    }

    @TestOnly
    public int getFreeExecutorCount() {
        return executorPool.getFreeCount();
    }

    @TestOnly
    public int getTaskCount() {
        return tasks.size();
    }

    @Override
    public boolean run(@NotNull WorkerContext workerContext) {
        if (isClosed) {
            return false;
        }
        if (runtime.state() != FiberRuntimeState.OPEN) {
            return false;
        }
        if (!hasNotification()) {
            return false;
        }

        final Fiber fiber = runtime.tryReserveFiber();
        if (fiber == null) {
            return false;
        }
        final long fiberReservationEpoch = fiber.getReservationEpoch();
        ApplyWal2TableJob executor = null;
        long cursor = -1;
        TableToken tableToken = null;
        WalApplyFiberTask fiberTask = null;
        boolean isTaskBound = false;
        try {
            executor = executorPool.tryAcquire();
            if (executor == null) {
                return false;
            }
            cursor = nextCursor();
            if (cursor < 0) {
                return false;
            }
            tableToken = queue.get(cursor).getTableToken();
            if (engine.isWalApplySuspended(tableToken)) {
                return true;
            }

            fiberTask = getTask(tableToken);
            fiberTask.signal();
            if (!fiberTask.tryBind(executor)) {
                return true;
            }
            isTaskBound = true;
            executor = null;

            final LaunchResult result = runtime.launchReserved(
                    fiber,
                    fiberReservationEpoch,
                    fiberTask,
                    fiberTask.getIncarnation()
            );
            if (result == LaunchResult.LAUNCHED) {
                isTaskBound = false;
                return true;
            }

            fiberTask.releaseAfterLaunchFailure(result != LaunchResult.QUIESCING);
            isTaskBound = false;
            if (result != LaunchResult.QUIESCING) {
                LOG.critical().$("could not launch WAL apply fiber [table=").$(tableToken)
                        .$(", result=").$(result)
                        .I$();
            }
            return true;
        } catch (Throwable th) {
            if (isTaskBound) {
                try {
                    fiberTask.releaseAfterLaunchFailure(false);
                } catch (Throwable cleanupFailure) {
                    if (cleanupFailure != th) {
                        th.addSuppressed(cleanupFailure);
                    }
                }
            }
            if (tableToken != null && runtime.state() == FiberRuntimeState.OPEN) {
                try {
                    engine.notifyWalTxnRepublisher(tableToken);
                } catch (Throwable cleanupFailure) {
                    if (cleanupFailure != th) {
                        th.addSuppressed(cleanupFailure);
                    }
                }
            }
            throw th;
        } finally {
            try {
                if (cursor > -1) {
                    subSeq.done(cursor);
                }
            } finally {
                try {
                    if (executor != null) {
                        executorPool.release(executor);
                    }
                } finally {
                    runtime.releaseReservedFiber(fiber, fiberReservationEpoch);
                }
            }
        }
    }

    @TestOnly
    public void setBeforeExecutorCreateForTesting(@Nullable Runnable beforeCreateForTesting) {
        executorPool.setBeforeCreateForTesting(beforeCreateForTesting);
    }

    @TestOnly
    public void setBeforeEvictForTesting(@Nullable Runnable beforeEvictForTesting) {
        this.beforeEvictForTesting = beforeEvictForTesting;
    }

    @TestOnly
    public void setTaskScheduleStateForTesting(TableToken tableToken, int expectedState, int targetState) {
        final WalApplyFiberTask task = tasks.get(tableToken.getDirName());
        if (task == null) {
            throw new IllegalStateException("WAL apply fiber task does not exist");
        }
        task.setScheduleStateForTesting(expectedState, targetState);
    }

    void evict(WalApplyFiberTask task) {
        final Runnable hook = beforeEvictForTesting;
        if (hook != null) {
            hook.run();
        }
        tasks.remove(task.getTableDirName(), task);
    }

    private synchronized WalApplyFiberTask createTask(TableToken tableToken) {
        final String dirName = tableToken.getDirName();
        WalApplyFiberTask task = tasks.get(dirName);
        if (task == null) {
            task = new WalApplyFiberTask(engine, this, runtime, executorPool, tableToken);
            tasks.put(dirName, task);
        }
        return task;
    }

    private WalApplyFiberTask getTask(TableToken tableToken) {
        final String dirName = tableToken.getDirName();
        final WalApplyFiberTask task = tasks.get(dirName);
        return task != null ? task : createTask(tableToken);
    }

    private boolean hasNotification() {
        final long next = subSeq.current() + 1;
        return subSeq.getBarrier().availableIndex(next) >= next;
    }

    private long nextCursor() {
        while (true) {
            final long cursor = subSeq.next();
            if (cursor != -2) {
                return cursor;
            }
            Os.pause();
        }
    }
}
