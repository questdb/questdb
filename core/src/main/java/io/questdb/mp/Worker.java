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

import io.questdb.Metrics;
import io.questdb.log.Log;
import io.questdb.std.ObjHashSet;
import io.questdb.std.Os;
import io.questdb.std.Unsafe;
import io.questdb.std.datetime.microtime.MicrosecondClock;
import io.questdb.std.datetime.microtime.MicrosecondClockImpl;
import org.jetbrains.annotations.Nullable;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

public class Worker extends Thread {
    public static final MicrosecondClock CLOCK_MICROS = MicrosecondClockImpl.INSTANCE;
    public static final int NO_THREAD_AFFINITY = -1;
    private final int affinity;
    private final String criticalErrorLine;
    private final SOCountDownLatch haltLatch;
    private final boolean haltOnError;
    private final AtomicLong jobStartMicros = new AtomicLong();
    private final ObjHashSet<? extends Job> jobs;
    private final AtomicReference<Lifecycle> lifecycle = new AtomicReference<>(Lifecycle.BORN);
    private final Log log;
    private final Metrics metrics;
    private final long napThreshold;
    private final OnHaltAction onHaltAction;
    private final String poolName;
    private final Job.RunStatus runStatus = () -> lifecycle.get() == Lifecycle.HALTED;
    private final long sleepMs;
    private final long sleepThreshold;
    private final WorkerStats stats;
    private final WorkerPoolMetrics poolMetrics;
    private final int workerId;
    private final long yieldThreshold;

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
            @Nullable Log log,
            WorkerPoolMetrics poolMetrics
    ) {
        assert yieldThreshold > 0L;
        this.setName(poolName + '_' + workerId);
        this.poolName = poolName;
        this.workerId = workerId;
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
        this.log = log;
        this.poolMetrics = poolMetrics;
        this.stats = new WorkerStats();
    }

    public String getPoolName() {
        return poolName;
    }

    /**
     * Returns the worker's utilization percentage based on a sliding window of recent iterations.
     * Utilization is calculated as the percentage of iterations where at least one job returned
     * true (indicating useful work was performed).
     * <p>
     * The calculation prioritizes recent activity using a sliding window of the last 1000 iterations.
     * If fewer than 1000 iterations have occurred, uses all available iterations for the calculation.
     *
     * @return utilization percentage (0.0 to 100.0), where 100.0 indicates the worker is always
     * finding useful work and 0.0 indicates all jobs are returning false (idle)
     */
    public double getUtilizationPercentage() {
        return stats.getUtilizationPercentage();
    }

    public int getWorkerId() {
        return workerId;
    }

    public void halt() {
        lifecycle.set(Lifecycle.HALTED);
    }

    @Override
    public void run() {
        Throwable ex = null;
        try {
            if (lifecycle.compareAndSet(Lifecycle.BORN, Lifecycle.RUNNING)) {

                String workerName = getName();

                // set affinity
                if (affinity > NO_THREAD_AFFINITY) {
                    if (Os.setCurrentThreadAffinity(affinity) == 0) {
                        if (log != null) {
                            log.info().$("affinity set [cpu=").$(affinity).$(", name=").$(workerName).I$();
                        }
                    } else {
                        if (log != null) {
                            log.error().$("could not set affinity [cpu=").$(affinity).$(", name=").$(workerName).I$();
                        }
                    }
                } else {
                    if (log != null) {
                        log.info().$("os scheduled worker started [name=").$(workerName).I$();
                    }
                }

                // setup eager jobs
                for (int i = 0, n = jobs.size(); i < n; i++) {
                    Unsafe.getUnsafe().loadFence();
                    try {
                        Job job = jobs.get(i);
                        if (job instanceof EagerThreadSetup) {
                            ((EagerThreadSetup) job).setup();
                        }
                    } finally {
                        Unsafe.getUnsafe().storeFence();
                    }
                }

                // enter main loop
                long ticker = 0L;
                while (lifecycle.get() == Lifecycle.RUNNING) {
                    boolean runAsap = false;
                    int usefulJobs = 0;
                    long iterationStart = CLOCK_MICROS.getTicks();

                    // measure latency of all jobs tick
                    jobStartMicros.lazySet(iterationStart);
                    for (int i = 0, n = jobs.size(); i < n; i++) {
                        Unsafe.getUnsafe().loadFence();
                        try {
                            boolean jobWasUseful = jobs.get(i).run(workerId, runStatus);
                            runAsap |= jobWasUseful;
                            if (jobWasUseful) {
                                usefulJobs++;
                            }
                        } catch (Throwable e) {
                            if (metrics.isEnabled()) {
                                try {
                                    metrics.healthMetrics().incrementUnhandledErrors();
                                } catch (Throwable t) {
                                    stdErrCritical(t);
                                }
                            }
                            if (log != null) {
                                log.critical().$("unhandled error [job=").$(jobs.get(i).toString()).$(", ex=").$(e).I$();
                            } else {
                                stdErrCritical(e); // log regardless
                            }
                            if (haltOnError) {
                                throw e;
                            }
                        } finally {
                            Unsafe.getUnsafe().storeFence();
                        }
                    }

                    // Record iteration stats
                    stats.recordIteration(usefulJobs > 0);
                    
                    // Update pool metrics periodically (every 100 iterations)
                    if (stats.getTotalIterations() % 100 == 0) {
                        poolMetrics.recordUtilization(workerId, stats.getUtilizationPercentage());
                    }

                    if (runAsap) {
                        ticker = 0;
                        continue;
                    }
                    if (++ticker < 0) {
                        ticker = sleepThreshold + 1; // overflow
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
        } catch (Throwable e) {
            ex = e;
            stdErrCritical(e);
        } finally {
            if (onHaltAction != null) {
                try {
                    onHaltAction.run(ex);
                    if (log != null) {
                        log.info().$("cleaned worker [name=").$(poolName).$(", worker=").$(workerId).I$();
                    }
                } catch (Throwable t) {
                    stdErrCritical(t);
                }
            }
            haltLatch.countDown();
            if (log != null) {
                log.info().$("os scheduled worker stopped [name=").$(getName()).I$();
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

    private enum Lifecycle {
        BORN, RUNNING, HALTED
    }

    @FunctionalInterface
    public interface OnHaltAction {
        void run(Throwable ex);
    }

    private static class WorkerStats {
        private static final int SLIDING_WINDOW_SIZE = 1000; // Number of worker iterations (not time-based)
        private final boolean[] slidingWindow = new boolean[SLIDING_WINDOW_SIZE]; // Circular buffer tracking last N iterations
        private long totalIterations;
        private long usefulIterations;
        private boolean windowFull = false;
        private int windowIndex = 0;

        long getTotalIterations() {
            return totalIterations;
        }

        private double getUtilizationPercentage() {
            long total = totalIterations;
            if (total == 0) {
                return 0.0;
            }

            if (!windowFull && windowIndex > 0) {
                int useful = 0;
                for (int i = 0; i < windowIndex; i++) {
                    if (slidingWindow[i]) useful++;
                }
                return (double) useful / windowIndex * 100.0;
            } else if (windowFull) {
                int useful = 0;
                for (boolean wasUseful : slidingWindow) {
                    if (wasUseful) useful++;
                }
                return (double) useful / SLIDING_WINDOW_SIZE * 100.0;
            }

            return (double) usefulIterations / total * 100.0;
        }

        private void recordIteration(boolean wasUseful) {
            totalIterations++;
            if (wasUseful) {
                usefulIterations++;
            }

            slidingWindow[windowIndex] = wasUseful;
            windowIndex = (windowIndex + 1) % SLIDING_WINDOW_SIZE;
            if (!windowFull && windowIndex == 0) {
                windowFull = true;
            }
        }
    }
}
