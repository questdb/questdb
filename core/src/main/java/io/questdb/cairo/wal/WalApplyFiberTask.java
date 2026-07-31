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
import io.questdb.cairo.CairoException;
import io.questdb.cairo.TableToken;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.Job;
import io.questdb.mp.continuation.FiberRuntime;
import io.questdb.mp.continuation.FiberRuntimeState;
import io.questdb.mp.continuation.FiberTask;
import io.questdb.std.Unsafe;
import org.jetbrains.annotations.Nullable;

final class WalApplyFiberTask extends FiberTask implements Job.WorkerContext {
    private static final int LEASE_BOUND = 1;
    private static final int LEASE_EVICTED = 2;
    private static final int LEASE_IDLE = 0;
    private static final long LEASE_STATE_OFFSET = Unsafe.getFieldOffset(WalApplyFiberTask.class, "leaseState");
    private static final Log LOG = LogFactory.getLog(WalApplyFiberTask.class);
    private static final long REQUEST_VERSION_OFFSET = Unsafe.getFieldOffset(
            WalApplyFiberTask.class,
            "requestVersion"
    );
    private final CairoEngine engine;
    private final WalApplyExecutorPool executorPool;
    private final WalApplyFiberJob job;
    private final FiberRuntime runtime;
    private final TableToken tableToken;
    private ApplyWal2TableJob executor;
    private boolean isForceRepublish;
    private boolean isReusable;
    @SuppressWarnings("FieldMayBeFinal")
    private volatile int leaseState = LEASE_IDLE;
    @SuppressWarnings("FieldMayBeFinal")
    private volatile long requestVersion;
    private long runVersion;

    WalApplyFiberTask(
            CairoEngine engine,
            WalApplyFiberJob job,
            FiberRuntime runtime,
            WalApplyExecutorPool executorPool,
            TableToken tableToken
    ) {
        this.engine = engine;
        this.executorPool = executorPool;
        this.job = job;
        this.runtime = runtime;
        this.tableToken = tableToken;
    }

    @Override
    public int carrierId() {
        return -1;
    }

    @Override
    public boolean isTerminating() {
        return runtime.state() != FiberRuntimeState.OPEN;
    }

    private static Throwable addCleanupFailure(@Nullable Throwable primary, Throwable failure) {
        if (primary == null) {
            return failure;
        }
        if (primary != failure) {
            primary.addSuppressed(failure);
        }
        return primary;
    }

    private static IllegalStateException nonIdleTask(int state) {
        return new IllegalStateException("idle WAL apply lease has non-idle task [state=" + state + ']');
    }

    private void releaseLease(boolean isLeaseReusable, boolean isRepublishForced) {
        final ApplyWal2TableJob executor = this.executor;
        final long completedVersion = runVersion;
        if (executor == null) {
            throw new IllegalStateException("WAL apply fiber lease has no executor");
        }
        boolean isDropped = false;
        this.executor = null;
        this.isForceRepublish = false;
        this.isReusable = false;
        runVersion = 0;
        Throwable cleanupFailure = null;
        try {
            isDropped = engine.isWalTableDropped(tableToken.getDirName());
        } catch (Throwable th) {
            cleanupFailure = th;
        }
        try {
            executorPool.release(executor);
        } catch (Throwable th) {
            cleanupFailure = addCleanupFailure(cleanupFailure, th);
        }

        boolean isScheduleReusable = getScheduleState() == STATE_IDLE;
        if (!isDropped && !isScheduleReusable) {
            try {
                isScheduleReusable = tryReopen();
            } catch (Throwable th) {
                cleanupFailure = addCleanupFailure(cleanupFailure, th);
            }
        }
        boolean isEvicted = isDropped || !isScheduleReusable;
        if (!Unsafe.cas(
                this,
                LEASE_STATE_OFFSET,
                LEASE_BOUND,
                isEvicted ? LEASE_EVICTED : LEASE_IDLE
        )) {
            cleanupFailure = addCleanupFailure(
                    cleanupFailure,
                    new IllegalStateException("WAL apply fiber lease is not bound")
            );
            isEvicted = true;
        }

        if (isEvicted) {
            try {
                job.evict(this);
            } catch (Throwable th) {
                cleanupFailure = addCleanupFailure(cleanupFailure, th);
            }
            if (!isDropped && runtime.state() == FiberRuntimeState.OPEN) {
                try {
                    engine.notifyWalTxnCommitted(tableToken);
                } catch (Throwable th) {
                    cleanupFailure = addCleanupFailure(cleanupFailure, th);
                }
            }
            CairoException.rethrowCleanupFailure(cleanupFailure);
            return;
        }

        boolean isDroppedAfterRelease = false;
        try {
            if (engine.isWalTableDropped(tableToken.getDirName())
                    && Unsafe.cas(
                    this,
                    LEASE_STATE_OFFSET,
                    LEASE_IDLE,
                    LEASE_EVICTED
            )) {
                job.evict(this);
                isDroppedAfterRelease = true;
            }
        } catch (Throwable th) {
            cleanupFailure = addCleanupFailure(cleanupFailure, th);
        }
        if (isDroppedAfterRelease) {
            CairoException.rethrowCleanupFailure(cleanupFailure);
            return;
        }

        if (runtime.state() == FiberRuntimeState.OPEN
                && (isRepublishForced
                || (isLeaseReusable && requestVersion != completedVersion))) {
            try {
                engine.notifyWalTxnCommitted(tableToken);
            } catch (Throwable th) {
                cleanupFailure = addCleanupFailure(cleanupFailure, th);
            }
        }
        CairoException.rethrowCleanupFailure(cleanupFailure);
    }

    String getTableDirName() {
        return tableToken.getDirName();
    }

    @Override
    protected void onAbandoned() {
        isForceRepublish = true;
        isReusable = false;
    }

    @Override
    protected void onDone() {
        releaseLease(isReusable, isForceRepublish);
    }

    @Override
    protected void onError(Throwable th) {
        LOG.critical().$("WAL apply fiber failed [table=").$(tableToken).$(", error=").$(th).I$();
        isForceRepublish = true;
    }

    void releaseAfterLaunchFailure(boolean isRepublish) {
        if (executor != null) {
            releaseLease(false, isRepublish);
        }
    }

    @Override
    protected boolean runStep() {
        final ApplyWal2TableJob executor = this.executor;
        if (executor == null) {
            throw new IllegalStateException("WAL apply fiber has no executor");
        }
        executor.applyWal(tableToken, this);
        return true;
    }

    void signal() {
        Unsafe.getAndAddLong(this, REQUEST_VERSION_OFFSET, 1);
    }

    boolean hasBound(ApplyWal2TableJob executor) {
        if (!Unsafe.cas(this, LEASE_STATE_OFFSET, LEASE_IDLE, LEASE_BOUND)) {
            return false;
        }
        if (getScheduleState() != STATE_IDLE) {
            leaseState = LEASE_IDLE;
            throw nonIdleTask(getScheduleState());
        }
        this.executor = executor;
        isForceRepublish = false;
        isReusable = true;
        runVersion = requestVersion;
        return true;
    }
}
