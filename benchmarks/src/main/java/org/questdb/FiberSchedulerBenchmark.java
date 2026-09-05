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

package org.questdb;

import io.questdb.Metrics;
import io.questdb.mp.Worker;
import io.questdb.mp.WorkerPool;
import io.questdb.mp.WorkerPoolConfiguration;
import io.questdb.mp.WorkerPoolMode;
import io.questdb.mp.continuation.Fiber;
import io.questdb.mp.continuation.FiberRuntime;
import io.questdb.mp.continuation.FiberRuntimeState;
import io.questdb.mp.continuation.FiberTask;
import io.questdb.mp.continuation.FiberWaitCoordinator;
import io.questdb.mp.continuation.FiberWakeSink;
import io.questdb.mp.continuation.FiberWalWaitQueue;
import io.questdb.mp.continuation.FiberWalWaitRegistration;
import io.questdb.mp.continuation.LaunchResult;
import io.questdb.mp.continuation.SourceRegistrationResult;
import io.questdb.std.Os;
import org.openjdk.jmh.annotations.AuxCounters;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Isolates the scheduler costs that application benchmarks cannot attribute: the no-idle wake
 * fast path, a ready-bit claim, one local queue round trip, and external resume-to-repark latency.
 * This is a head-only component benchmark because it uses the scheduler's new test seams. Use
 * {@link FiberSchedulerWorkloadBenchmark} for exact-source base/head comparisons.
 */
@BenchmarkMode(Mode.AverageTime)
@Fork(
        value = 1,
        jvmArgsAppend = {
                "--add-exports=java.base/jdk.internal.vm=ALL-UNNAMED",
                "--enable-native-access=ALL-UNNAMED"
        }
)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
public class FiberSchedulerBenchmark {
    private static final long AWAIT_TIMEOUT_NANOS = TimeUnit.SECONDS.toNanos(30);

    public static void main(String[] args) throws RunnerException {
        final Options options = new OptionsBuilder()
                .include(FiberSchedulerBenchmark.class.getSimpleName())
                .warmupIterations(3)
                .measurementIterations(5)
                .forks(1)
                .build();
        new Runner(options).run();
    }

    @Benchmark
    @Threads(1)
    public long externalResumeToRepark(IdleResumeState state, ResumeCounters counters) {
        final long expectedResumeCount = state.task.resumeCount.get() + 1;
        state.waitQueue.fire(1, false);
        awaitResumeAndRepark(state, expectedResumeCount);
        counters.mounts++;
        if (state.task.isLastResumeOnSameWorker) {
            counters.sameLastMounter++;
        }
        return expectedResumeCount;
    }

    @Benchmark
    public Fiber localQueueRoundTrip(LocalQueueState state) {
        if (!state.runtime.offerLocalForTesting(0, state.fiber)) {
            throw new IllegalStateException("local Fiber queue unexpectedly rejected an empty-slot offer");
        }
        final Fiber fiber = state.runtime.tryDequeueLocalForTesting(0);
        if (fiber == null) {
            throw new IllegalStateException("local Fiber queue lost a committed entry");
        }
        return fiber;
    }

    @Benchmark
    public boolean wakeNoIdleGeneric(WakeControllerState state) {
        return state.pool.wakeOneForTesting(FiberRuntime.NO_WORKER);
    }

    @Benchmark
    public boolean wakeNoIdlePreferred(WakeControllerState state) {
        return state.pool.wakeOneForTesting(state.preferredWorkerId);
    }

    @Benchmark
    public boolean wakeReadyGeneric(WakeControllerState state) {
        if (!state.pool.registerReadyWorkerForTesting(state.preferredWorkerId)) {
            throw new IllegalStateException("could not register benchmark Worker as ready");
        }
        return state.pool.wakeOneForTesting(FiberRuntime.NO_WORKER);
    }

    @Benchmark
    public boolean wakeReadyPreferred(WakeControllerState state) {
        if (!state.pool.registerReadyWorkerForTesting(state.preferredWorkerId)) {
            throw new IllegalStateException("could not register benchmark Worker as ready");
        }
        return state.pool.wakeOneForTesting(state.preferredWorkerId);
    }

    private static void awaitReadyWorkers(WorkerPool pool, int workerCount) {
        final long deadline = System.nanoTime() + AWAIT_TIMEOUT_NANOS;
        while (pool.getReadyWorkerCountForTesting() != workerCount) {
            if (System.nanoTime() - deadline >= 0) {
                throw new IllegalStateException("timed out waiting for Fiber-host Workers to park");
            }
            Os.pause();
        }
    }

    private static void awaitResumeAndRepark(IdleResumeState state, long expectedResumeCount) {
        final long deadline = System.nanoTime() + AWAIT_TIMEOUT_NANOS;
        while (state.task.resumeCount.get() != expectedResumeCount
                || state.waitQueue.size() != 1
                || state.pool.getReadyWorkerCountForTesting() != state.workerCount) {
            if (System.nanoTime() - deadline >= 0) {
                throw new IllegalStateException("timed out waiting for the resumed Fiber to repark");
            }
            Os.pause();
        }
    }

    @AuxCounters(AuxCounters.Type.EVENTS)
    @State(Scope.Thread)
    public static class ResumeCounters {
        public long mounts;
        public long sameLastMounter;

        @Setup(Level.Iteration)
        public void reset() {
            mounts = 0;
            sameLastMounter = 0;
        }
    }

    @State(Scope.Benchmark)
    public static class IdleResumeState {
        private WorkerPool pool;
        private ResumeLoopTask task;
        private final FiberWalWaitQueue waitQueue = new FiberWalWaitQueue();
        @Param({"1", "2", "8"})
        public int workerCount;

        @Setup(Level.Trial)
        public void setup() {
            pool = new WorkerPool(fiberPoolConfiguration("fiber-resume-benchmark", workerCount));
            task = new ResumeLoopTask(waitQueue);
            pool.start();
            awaitReadyWorkers(pool, workerCount);
            if (pool.getFiberRuntime().launch(task) != LaunchResult.LAUNCHED) {
                throw new IllegalStateException("could not launch resume benchmark Fiber");
            }
            awaitReadyWorkers(pool, workerCount);
            if (waitQueue.size() != 1) {
                throw new IllegalStateException("resume benchmark Fiber did not enter its wait");
            }
        }

        @TearDown(Level.Trial)
        public void close() {
            try {
                task.isStopped = true;
                waitQueue.fire(1, false);
            } finally {
                pool.halt();
            }
        }
    }

    @State(Scope.Thread)
    public static class LocalQueueState {
        private Fiber fiber;
        private FiberRuntime runtime;
        @Param({"1", "8", "32"})
        public int workerCount;

        @Setup(Level.Trial)
        public void setup() {
            runtime = new FiberRuntime(256, 256, 64, workerCount, FiberWakeSink.NO_OP);
            fiber = runtime.tryReserveFiber();
            if (fiber == null) {
                throw new IllegalStateException("could not reserve local queue benchmark Fiber");
            }
        }

        @TearDown(Level.Trial)
        public void close() {
            runtime.releaseReservedFiber(fiber, fiber.getReservationEpoch());
            runtime.beginQuiesce();
            final long deadline = System.nanoTime() + AWAIT_TIMEOUT_NANOS;
            while (runtime.state() != FiberRuntimeState.CLOSED && System.nanoTime() - deadline < 0) {
                runtime.drain(64);
            }
            if (!runtime.awaitClosed(deadline)) {
                throw new IllegalStateException("local queue benchmark runtime did not close");
            }
            runtime.closeAfterDrained();
        }
    }

    @State(Scope.Thread)
    public static class WakeControllerState {
        private WorkerPool pool;
        private int preferredWorkerId;
        @Param({"1", "8", "32", "65"})
        public int workerCount;

        @Setup(Level.Trial)
        public void setup() {
            pool = new WorkerPool(fiberPoolConfiguration("fiber-wake-benchmark", workerCount));
            preferredWorkerId = workerCount / 2;
            for (int i = 0; i < workerCount; i++) {
                pool.registerWakeTargetForTesting(i, new Thread());
            }
        }

        @TearDown(Level.Trial)
        public void close() {
            pool.halt();
        }
    }

    private static WorkerPoolConfiguration fiberPoolConfiguration(String poolName, int workerCount) {
        return new WorkerPoolConfiguration() {
            @Override
            public int getFiberMaxLiveCount() {
                return Math.max(64, workerCount * 8);
            }

            @Override
            public int getFiberRetainedCount() {
                return 16;
            }

            @Override
            public Metrics getMetrics() {
                return Metrics.DISABLED;
            }

            @Override
            public long getNapThreshold() {
                return 2;
            }

            @Override
            public String getPoolName() {
                return poolName;
            }

            @Override
            public long getSleepThreshold() {
                return 3;
            }

            @Override
            public long getSleepTimeout() {
                return TimeUnit.SECONDS.toMillis(60);
            }

            @Override
            public int getWorkerCount() {
                return workerCount;
            }

            @Override
            public WorkerPoolMode getWorkerPoolMode() {
                return WorkerPoolMode.FIBER_HOST;
            }

            @Override
            public long getYieldThreshold() {
                return 1;
            }

            @Override
            public boolean isDaemonPool() {
                return true;
            }
        };
    }

    private static class ResumeLoopTask extends FiberTask {
        private volatile boolean isLastResumeOnSameWorker;
        private volatile boolean isStopped;
        private final AtomicLong resumeCount = new AtomicLong();
        private final FiberWalWaitQueue waitQueue;

        private ResumeLoopTask(FiberWalWaitQueue waitQueue) {
            this.waitQueue = waitQueue;
        }

        @Override
        protected boolean runStep() {
            int previousWorkerId = Objects.requireNonNull(Worker.current()).getWorkerId();
            while (!isStopped) {
                final Fiber fiber = Objects.requireNonNull(Fiber.current());
                final FiberWaitCoordinator coordinator = fiber.getWaitCoordinator();
                final long token = fiber.beginWaitBuild(1);
                final FiberWalWaitRegistration registration = coordinator.acquireWal(token, 1);
                try {
                    if (registration.register(waitQueue) != SourceRegistrationResult.ACCEPTED) {
                        throw new IllegalStateException("resume benchmark wait registration failed");
                    }
                    final int reason = fiber.suspendWait(token);
                    if (isStopped) {
                        return true;
                    }
                    if (reason != FiberWaitCoordinator.REASON_WAL) {
                        throw new IllegalStateException("unexpected resume benchmark wait reason");
                    }
                    final int currentWorkerId = Objects.requireNonNull(Worker.current()).getWorkerId();
                    isLastResumeOnSameWorker = currentWorkerId == previousWorkerId;
                    previousWorkerId = currentWorkerId;
                    resumeCount.incrementAndGet();
                } finally {
                    registration.cancel();
                    coordinator.abort(token);
                    coordinator.consume(token);
                }
            }
            return true;
        }
    }
}
