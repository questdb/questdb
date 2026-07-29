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
import io.questdb.mp.continuation.FiberRuntime;
import io.questdb.mp.continuation.FiberRuntimeState;
import io.questdb.mp.continuation.FiberTask;
import io.questdb.std.Unsafe;

final class WalApplyFiberTask extends FiberTask implements Job.WorkerContext {
    private static final int LEASE_BOUND = 1;
    private static final int LEASE_IDLE = 0;
    private static final long LEASE_STATE_OFFSET = Unsafe.getFieldOffset(WalApplyFiberTask.class, "leaseState");
    private static final Log LOG = LogFactory.getLog(WalApplyFiberTask.class);
    private static final long REQUEST_VERSION_OFFSET = Unsafe.getFieldOffset(
            WalApplyFiberTask.class,
            "requestVersion"
    );
    private final CairoEngine engine;
    private ApplyWal2TableJob executor;
    private final WalApplyExecutorPool executorPool;
    private boolean isForceRepublish;
    private boolean isReusable;
    @SuppressWarnings("FieldMayBeFinal")
    private volatile int leaseState = LEASE_IDLE;
    @SuppressWarnings("FieldMayBeFinal")
    private volatile long requestVersion;
    private long runVersion;
    private final FiberRuntime runtime;
    private final TableToken tableToken;

    WalApplyFiberTask(
            CairoEngine engine,
            FiberRuntime runtime,
            WalApplyExecutorPool executorPool,
            TableToken tableToken
    ) {
        this.engine = engine;
        this.executorPool = executorPool;
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

    void releaseAfterLaunchFailure(boolean isRepublish) {
        if (executor != null) {
            releaseLease(false, isRepublish);
        }
    }

    void signal() {
        Unsafe.getAndAddLong(this, REQUEST_VERSION_OFFSET, 1);
    }

    boolean tryBind(ApplyWal2TableJob executor) {
        if (!Unsafe.cas(this, LEASE_STATE_OFFSET, LEASE_IDLE, LEASE_BOUND)) {
            return false;
        }
        if (getScheduleState() != STATE_IDLE) {
            leaseState = LEASE_IDLE;
            throw new IllegalStateException(
                    "idle WAL apply lease has non-idle task [state=" + getScheduleState() + ']'
            );
        }
        this.executor = executor;
        isForceRepublish = false;
        isReusable = true;
        runVersion = requestVersion;
        return true;
    }

    @Override
    protected void onAbandoned() {
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

    @Override
    protected boolean runStep() {
        final ApplyWal2TableJob executor = this.executor;
        if (executor == null) {
            throw new IllegalStateException("WAL apply fiber has no executor");
        }
        executor.applyWal(tableToken, this);
        return true;
    }

    private void releaseLease(boolean isReusable, boolean isForceRepublish) {
        final ApplyWal2TableJob executor = this.executor;
        final long completedVersion = runVersion;
        if (executor == null) {
            throw new IllegalStateException("WAL apply fiber lease has no executor");
        }
        this.executor = null;
        this.isForceRepublish = false;
        this.isReusable = false;
        runVersion = 0;
        executorPool.release(executor);
        if (isReusable) {
            reopen();
        }
        if (!Unsafe.cas(this, LEASE_STATE_OFFSET, LEASE_BOUND, LEASE_IDLE)) {
            throw new IllegalStateException("WAL apply fiber lease is not bound");
        }
        if (runtime.state() == FiberRuntimeState.OPEN
                && (isForceRepublish
                || (isReusable && requestVersion != completedVersion))) {
            engine.notifyWalTxnCommitted(tableToken);
        }
    }
}
