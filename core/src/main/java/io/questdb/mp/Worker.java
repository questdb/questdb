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
import io.questdb.std.datetime.Clock;
import io.questdb.std.datetime.microtime.MicrosecondClockImpl;
import org.jetbrains.annotations.Nullable;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

public class Worker extends Thread {
    public static final Clock CLOCK_MICROS = MicrosecondClockImpl.INSTANCE;
    public static final int NO_THREAD_AFFINITY = -1;
    private static final CarrierLocal<Worker> CURRENT = new CarrierLocal<>();
    private final int affinity;
    private final String criticalErrorLine;
    private final FiberRuntime fiberRuntime;
    private final SOCountDownLatch haltLatch;
    private final boolean haltOnError;
    private final AtomicLong jobStartMicros = new AtomicLong();
    private final ObjHashSet<? extends Job> jobs;
    private final AtomicReference<WorkerLifecycle> lifecycle = new AtomicReference<>(WorkerLifecycle.BORN);
    private final Log log;
    private final Metrics metrics;
    private final long napThreshold;
    private final OnHaltAction onHaltAction;
    private final String poolName;
    private final long sleepMs;
    private final long sleepThreshold;
    private final Job.WorkerContext workerContext;
    private final int workerId;
    private final long yieldThreshold;
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
            @Nullable FiberRuntime fiberRuntime,
            @Nullable Log log
    ) {
        assert yieldThreshold > 0L;
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
        this.metrics = metrics;
        this.fiberRuntime = fiberRuntime;
        this.log = log;
    }

    public static @Nullable Worker current() {
        return CURRENT.get();
    }

    public String getPoolName() {
        return poolName;
    }

    public int getWorkerId() {
        return workerId;
    }

    public void halt() {
        lifecycle.set(WorkerLifecycle.HALTED);
    }

    @Override
    public void run() {
        Throwable ex = null;
        try {
            if (lifecycle.compareAndSet(WorkerLifecycle.BORN, WorkerLifecycle.RUNNING)
                    || lifecycle.get() == WorkerLifecycle.HALTING) {
                CarrierIdentity.bind();
                CURRENT.set(this);
                if (fiberRuntime != null) {
                    fiberRuntime.initializeCarrier();
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
            stdErrCritical(e);
        } finally {
            lifecycle.set(WorkerLifecycle.HALTED);
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
        long ticker = 0L;
        while (true) {
            final WorkerLifecycle state = lifecycle.get();
            if (state == WorkerLifecycle.HALTED) {
                break;
            }
            boolean isRunAsap = false;
            if (state == WorkerLifecycle.RUNNING) {
                isRunAsap = runJobs();
            }
            if (fiberRuntime != null) {
                if (state == WorkerLifecycle.HALTING && fiberRuntime.state() == FiberRuntimeState.CLOSED) {
                    break;
                }
                isRunAsap |= fiberRuntime.drain(fiberRuntime.getMountBudget()) > 0;
                if (state == WorkerLifecycle.HALTING && fiberRuntime.state() == FiberRuntimeState.CLOSED) {
                    break;
                }
            } else if (state == WorkerLifecycle.HALTING) {
                break;
            }

            if (isRunAsap) {
                ticker = 0;
                continue;
            }
            if (++ticker < 0) {
                ticker = sleepThreshold + 1;
            }
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
        final SuspensionScope.CarrierScope suspensionScope = fiberRuntime != null
                ? SuspensionScope.scope()
                : null;
        final SuspensionScope.Mode previousMode = suspensionScope != null
                ? SuspensionScope.enterBlocking(suspensionScope)
                : null;
        jobStartMicros.lazySet(CLOCK_MICROS.getTicks());
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

    private void stdErrCritical(Throwable e) {
        System.err.println(criticalErrorLine);
        e.printStackTrace(System.err);
    }

    long getJobStartMicros() {
        return jobStartMicros.get();
    }

    void haltAfterFiberDrain() {
        while (true) {
            final WorkerLifecycle state = lifecycle.get();
            if (state == WorkerLifecycle.HALTED || state == WorkerLifecycle.HALTING) {
                return;
            }
            if (lifecycle.compareAndSet(state, WorkerLifecycle.HALTING)) {
                return;
            }
        }
    }

    @FunctionalInterface
    public interface OnHaltAction {
        void run(Throwable ex);
    }
}
