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

import io.questdb.Metrics;
import io.questdb.log.Log;
import io.questdb.mp.continuation.FiberRuntime;
import io.questdb.mp.continuation.FiberRuntimeState;
import io.questdb.mp.continuation.SuspensionScope;
import io.questdb.std.CarrierLocal;
import io.questdb.std.ObjHashSet;
import io.questdb.std.Os;
import io.questdb.std.Unsafe;
import org.jetbrains.annotations.Nullable;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;

public class Worker extends Thread {
    public static final int NO_THREAD_AFFINITY = -1;
    private static final CarrierLocal<Worker> CURRENT = new CarrierLocal<>();
    private final int affinity;
    private final String criticalErrorLine;
    private final FiberRuntime.OwnerContext fiberOwnerContext;
    private final FiberRuntime fiberRuntime;
    private final SOCountDownLatch haltLatch;
    private final boolean haltOnError;
    private final AtomicLong jobStartNanos = new AtomicLong(System.nanoTime());
    private final ObjHashSet<? extends Job> jobs;
    private final AtomicReference<WorkerLifecycle> lifecycle = new AtomicReference<>(WorkerLifecycle.BORN);
    private final Log log;
    private final Metrics metrics;
    private final long napThreshold;
    private final OnHaltAction onHaltAction;
    private final String poolName;
    private final long sleepMs;
    private final long sleepNanos;
    private final long sleepThreshold;
    private final WorkerWakeController wakeController;
    private final Job.WorkerContext workerContext;
    private final int workerId;
    private final long yieldThreshold;
    private SuspensionScope.CarrierScope carrierScope;
    private int jobStartIndex;

    public Worker(
            String poolName,
            int workerId,
            int affinity,
            ObjHashSet<? extends Job> jobs,
            SOCountDownLatch haltLatch,
            @Nullable OnHaltAction onHaltAction,
            boolean haltOnError,
            long yieldThreshold,
            long napThreshold,
            long sleepThreshold,
            long sleepMs,
            Metrics metrics,
            @Nullable Log log
    ) {
        this(
                poolName,
                workerId,
                affinity,
                jobs,
                haltLatch,
                onHaltAction,
                haltOnError,
                yieldThreshold,
                napThreshold,
                sleepThreshold,
                sleepMs,
                metrics,
                null,
                null,
                null,
                log
        );
    }

    Worker(
            String poolName,
            int workerId,
            int affinity,
            ObjHashSet<? extends Job> jobs,
            SOCountDownLatch haltLatch,
            @Nullable OnHaltAction onHaltAction,
            boolean haltOnError,
            long yieldThreshold,
            long napThreshold,
            long sleepThreshold,
            long sleepMs,
            Metrics metrics,
            @Nullable FiberRuntime fiberRuntime,
            @Nullable FiberRuntime.OwnerContext fiberOwnerContext,
            @Nullable WorkerWakeController wakeController,
            @Nullable Log log
    ) {
        assert yieldThreshold > 0L;
        if ((fiberRuntime == null) != (fiberOwnerContext == null)) {
            throw new IllegalArgumentException("Fiber runtime and owner context must be installed together");
        }
        if (fiberOwnerContext != null
                && (!fiberOwnerContext.isOwnedBy(fiberRuntime)
                || fiberOwnerContext.getWorkerId() != workerId)) {
            throw new IllegalArgumentException("Fiber owner context does not match the Worker runtime");
        }
        if ((fiberOwnerContext == null) != (wakeController == null)) {
            throw new IllegalArgumentException("Fiber owner context and wake controller must be installed together");
        }
        setName(poolName + '_' + workerId);
        this.poolName = poolName;
        this.workerId = workerId;
        this.workerContext = new Job.WorkerContext() {
            @Override
            public int carrierId() {
                return Worker.this.workerId;
            }

            @Override
            public boolean isTerminating() {
                return lifecycle.get() != WorkerLifecycle.RUNNING;
            }
        };
        this.affinity = affinity;
        this.jobs = jobs;
        this.haltLatch = haltLatch;
        this.onHaltAction = onHaltAction;
        this.haltOnError = haltOnError;
        this.criticalErrorLine = "0000-00-00T00:00:00.000000Z C Unhandled exception in worker " + getName();
        this.yieldThreshold = yieldThreshold;
        this.napThreshold = napThreshold;
        this.sleepThreshold = sleepThreshold;
        this.sleepMs = sleepMs;
        this.sleepNanos = TimeUnit.MILLISECONDS.toNanos(sleepMs);
        this.metrics = metrics;
        this.fiberRuntime = fiberRuntime;
        this.fiberOwnerContext = fiberOwnerContext;
        this.wakeController = wakeController;
        this.log = log;
    }

    public static @Nullable Worker current() {
        return CURRENT.get();
    }

    public String getPoolName() {
        return poolName;
    }

    public @Nullable FiberRuntime.OwnerContext getFiberOwnerContext() {
        return fiberOwnerContext;
    }

    public int getWorkerId() {
        return workerId;
    }

    public void halt() {
        lifecycle.set(WorkerLifecycle.HALTED);
        if (wakeController != null) {
            wakeController.wakeOne(workerId);
        }
    }

    @Override
    public void run() {
        Throwable ex = null;
        boolean isFiberOwnerActive = false;
        try {
            if (lifecycle.compareAndSet(WorkerLifecycle.BORN, WorkerLifecycle.RUNNING)
                    || lifecycle.get() == WorkerLifecycle.HALTING) {
                CarrierIdentity.bind();
                CURRENT.set(this);
                if (fiberRuntime != null) {
                    fiberRuntime.initializeCarrier();
                    carrierScope = SuspensionScope.scope();
                    fiberRuntime.activateOwner(fiberOwnerContext);
                    isFiberOwnerActive = true;
                }

                final String workerName = getName();
                if (affinity > NO_THREAD_AFFINITY) {
                    if (Os.setCurrentThreadAffinity(affinity) == 0) {
                        if (log != null) {
                            log.info().$("affinity set [cpu=").$(affinity).$(", name=").$(workerName).I$();
                        }
                    } else if (log != null) {
                        log.error().$("could not set affinity [cpu=").$(affinity).$(", name=").$(workerName).I$();
                    }
                } else if (log != null) {
                    log.debug().$("os scheduled worker started [name=").$(workerName).I$();
                }

                for (int i = 0, n = jobs.size(); i < n; i++) {
                    Unsafe.loadFence();
                    try {
                        final Job job = jobs.get(i);
                        if (job instanceof EagerThreadSetup eagerThreadSetup) {
                            eagerThreadSetup.setup();
                        }
                    } finally {
                        Unsafe.storeFence();
                    }
                }
                loopBody();
            }
        } catch (Throwable e) {
            ex = e;
            reportUnhandledError("loop", e);
        } finally {
            lifecycle.set(WorkerLifecycle.HALTED);
            carrierScope = null;
            if (wakeController != null) {
                wakeController.unregisterReady(workerId);
            }
            if (isFiberOwnerActive) {
                try {
                    fiberRuntime.onOwnerExit(fiberOwnerContext);
                } catch (Throwable t) {
                    reportUnhandledError("owner exit", t);
                }
            }
            if (onHaltAction != null) {
                try {
                    onHaltAction.run(ex);
                    if (log != null) {
                        log.debug().$("cleaned worker [name=").$(poolName).$(", worker=").$(workerId).I$();
                    }
                } catch (Throwable t) {
                    stdErrCritical(t);
                }
            }
            try {
                if (log != null) {
                    log.debug().$("os scheduled worker stopped [name=").$(getName()).I$();
                }
            } finally {
                try {
                    CURRENT.remove();
                } finally {
                    try {
                        CarrierIdentity.unbind();
                    } finally {
                        haltLatch.countDown();
                    }
                }
            }
        }
    }

    private void loopBody() {
        if (fiberRuntime == null) {
            loopLegacy();
        } else {
            loopFiberHost();
        }
    }

    private void loopFiberHost() {
        long ticker = 0L;
        while (true) {
            // Preserve an interrupt for at least one complete Worker iteration. If it arrives
            // after this snapshot, parkFiberHost() refuses to sleep and the next iteration gives
            // every Job and selected Fiber an opportunity to observe the status.
            final boolean isInterruptedAtLoopStart = Thread.currentThread().isInterrupted();
            final WorkerLifecycle state = lifecycle.get();
            if (state == WorkerLifecycle.HALTED) {
                break;
            }
            boolean isRunAsap = false;
            if (state == WorkerLifecycle.RUNNING) {
                isRunAsap = runJobs();
            }
            if (state == WorkerLifecycle.HALTING && fiberRuntime.state() == FiberRuntimeState.CLOSED) {
                break;
            }
            isRunAsap |= fiberRuntime.drainOwned(fiberOwnerContext, fiberRuntime.getMountBudget()) > 0;
            if (state == WorkerLifecycle.HALTING && fiberRuntime.state() == FiberRuntimeState.CLOSED) {
                break;
            }

            if (isRunAsap) {
                ticker = 0;
                continue;
            }
            ticker++;
            if (ticker > sleepThreshold) {
                if (parkFiberHost(sleepNanos, isInterruptedAtLoopStart)) {
                    ticker = 0;
                }
            } else if (ticker > napThreshold) {
                if (parkFiberHost(1_000_000L, isInterruptedAtLoopStart)) {
                    ticker = 0;
                }
            } else if (ticker > yieldThreshold) {
                Os.pause();
            }
        }
    }

    private void loopLegacy() {
        long ticker = 0L;
        while (true) {
            final WorkerLifecycle state = lifecycle.get();
            if (state == WorkerLifecycle.HALTED) {
                break;
            }
            if (state == WorkerLifecycle.HALTING) {
                break;
            }
            if (runJobs()) {
                ticker = 0;
                continue;
            }
            ticker++;
            if (ticker > sleepThreshold) {
                Os.sleep(sleepMs);
            } else if (ticker > napThreshold) {
                Os.sleep(1);
            } else if (ticker > yieldThreshold) {
                Os.pause();
            }
        }
    }

    private boolean runJobs() {
        boolean isRunAsap = false;
        final SuspensionScope.CarrierScope suspensionScope = carrierScope;
        final SuspensionScope.Mode previousMode = suspensionScope != null
                ? SuspensionScope.enterBlocking(suspensionScope)
                : null;
        jobStartNanos.lazySet(System.nanoTime());
        try {
            final int n = jobs.size();
            int jobIndex = jobStartIndex;
            for (int i = 0; i < n; i++) {
                final Job job = jobs.get(jobIndex);
                Unsafe.loadFence();
                try {
                    isRunAsap |= job.run(workerContext);
                } catch (Throwable e) {
                    if (metrics.isEnabled()) {
                        try {
                            metrics.healthMetrics().incrementUnhandledErrors();
                        } catch (Throwable t) {
                            stdErrCritical(t);
                        }
                    }
                    if (log != null) {
                        log.critical().$("unhandled error [job=").$(job.toString()).$(", ex=").$(e).I$();
                    } else {
                        stdErrCritical(e);
                    }
                    if (haltOnError) {
                        throw e;
                    }
                } finally {
                    Unsafe.storeFence();
                }
                if (++jobIndex == n) {
                    jobIndex = 0;
                }
            }
            if (n > 0 && ++jobStartIndex == n) {
                jobStartIndex = 0;
            }
            return isRunAsap;
        } finally {
            if (suspensionScope != null) {
                SuspensionScope.restoreMode(suspensionScope, previousMode);
            }
        }
    }

    private void reportUnhandledError(String stage, Throwable e) {
        stdErrCritical(e);
        if (metrics.isEnabled()) {
            metrics.healthMetrics().incrementUnhandledErrors();
        }
        if (log != null) {
            log.critical().$("unhandled error in worker [name=").$(getName()).$(", stage=").$(stage).$(", ex=").$(e).I$();
        }
    }

    private void stdErrCritical(Throwable e) {
        System.err.println(criticalErrorLine);
        e.printStackTrace(System.err);
    }

    long getJobStartNanos() {
        return jobStartNanos.get();
    }

    void haltAfterFiberDrain() {
        while (true) {
            final WorkerLifecycle state = lifecycle.get();
            if (state == WorkerLifecycle.HALTED || state == WorkerLifecycle.HALTING) {
                return;
            }
            if (lifecycle.compareAndSet(state, WorkerLifecycle.HALTING)) {
                if (wakeController != null) {
                    wakeController.wakeOne(workerId);
                }
                return;
            }
        }
    }

    private boolean parkFiberHost(long nanos, boolean isInterruptedAtLoopStart) {
        if (nanos <= 0) {
            return false;
        }

        if (Thread.currentThread().isInterrupted()) {
            if (!isInterruptedAtLoopStart) {
                // The interrupt arrived after user work started this iteration. Keep the status
                // set and run another complete iteration before considering a blocking park.
                return false;
            }
            // User work has had a complete iteration in which to observe this interrupt. Clear
            // the status and consume the associated LockSupport permit before advertising this
            // Worker as a fresh wake target; otherwise the next park can return immediately and
            // turn an ignored interrupt into a permanent idle spin.
            Thread.interrupted();
            LockSupport.parkNanos(this, 1L);
        }

        if (!wakeController.registerReady(workerId)) {
            return false;
        }
        try {
            if (!isFiberParkAllowed()) {
                return false;
            }
            if (fiberRuntime.hasWorkAfterReady(fiberOwnerContext)) {
                // A Worker that found work is no longer an idle wake target. Clear the bit before
                // mounting: a continuation may run for an arbitrary time, during which a publisher
                // must be able to claim a genuinely parked sibling instead.
                wakeController.unregisterReady(workerId);
                return fiberRuntime.drainOneBeforePark(fiberOwnerContext);
            }
            if (!isFiberParkAllowed() || !wakeController.isReady(workerId)) {
                return false;
            }
            LockSupport.parkNanos(this, nanos);
            return false;
        } finally {
            wakeController.unregisterReady(workerId);
        }
    }

    private boolean isFiberParkAllowed() {
        final WorkerLifecycle state = lifecycle.get();
        return state != WorkerLifecycle.HALTED
                && (state != WorkerLifecycle.HALTING || fiberRuntime.state() != FiberRuntimeState.CLOSED);
    }

    @FunctionalInterface
    public interface OnHaltAction {
        void run(Throwable ex);
    }
}
