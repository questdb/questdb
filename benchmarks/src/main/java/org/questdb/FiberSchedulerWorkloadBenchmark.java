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
import io.questdb.mp.Job;
import io.questdb.mp.Worker;
import io.questdb.mp.WorkerPool;
import io.questdb.mp.WorkerPoolConfiguration;
import io.questdb.mp.WorkerPoolMode;
import io.questdb.mp.continuation.Fiber;
import io.questdb.mp.continuation.FiberRuntime;
import io.questdb.mp.continuation.FiberTask;
import io.questdb.mp.continuation.FiberWaitCoordinator;
import io.questdb.mp.continuation.FiberWalWaitQueue;
import io.questdb.mp.continuation.FiberWalWaitRegistration;
import io.questdb.mp.continuation.LaunchResult;
import io.questdb.mp.continuation.SourceRegistrationResult;
import io.questdb.std.Os;
import org.jetbrains.annotations.NotNull;
import org.openjdk.jmh.annotations.AuxCounters;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongArray;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Base/head workload benchmark for scheduler policy. This source intentionally uses only APIs
 * available at the comparison base commit, so the exact same file can be compiled and run on
 * both revisions.
 *
 * <p>The external workload leaves a real Fiber-host pool idle before a foreign thread publishes
 * each completion. The same-runtime workload has every Worker publish a bounded stream of short
 * Fiber tasks into its own runtime; this models pool Jobs handing query fragments to Fibers and
 * makes global-queue contention, local placement, stealing, and mount migration observable at a
 * complete task boundary. The burst workload keeps one publisher active while its peers become
 * idle, then publishes several independent tasks in one Job pass. This is the QuestDB HTTP/retry
 * dispatch shape in which waking a same-runtime peer could be useful.</p>
 */
@Fork(
        value = 1,
        jvmArgsAppend = {
                "--add-exports=java.base/jdk.internal.vm=ALL-UNNAMED",
                "--enable-native-access=ALL-UNNAMED"
        }
)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
public class FiberSchedulerWorkloadBenchmark {
    private static final long AWAIT_TIMEOUT_NANOS = TimeUnit.SECONDS.toNanos(30);
    private static final int SAME_RUNTIME_OPERATIONS = 262_144;

    public static void main(String[] args) throws RunnerException {
        final Options options = new OptionsBuilder()
                .include(FiberSchedulerWorkloadBenchmark.class.getSimpleName())
                .warmupIterations(3)
                .measurementIterations(5)
                .forks(1)
                .build();
        new Runner(options).run();
    }

    @Benchmark
    @BenchmarkMode(Mode.SampleTime)
    @Threads(1)
    public long externalResumeLatency(ExternalResumeState state, ResumeCounters counters) {
        final long expectedResumeCount = state.task.resumeCount.get() + 1;
        final long start = System.nanoTime();
        state.waitQueue.fire(1, false);
        awaitExternalResume(state, expectedResumeCount);
        final long elapsed = System.nanoTime() - start;
        counters.mounts++;
        if (state.task.isLastResumeOnSameWorker) {
            counters.sameMounter++;
        }
        return elapsed;
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    @OperationsPerInvocation(SAME_RUNTIME_OPERATIONS)
    @Threads(1)
    public long sameRuntimePublishAndComplete(SameRuntimeState state, PublishCounters counters) {
        state.awaitBatch();
        final long sameMounterCount = state.sum(state.sameMounterCount) - state.sameMounterBaseline;
        counters.mounts += SAME_RUNTIME_OPERATIONS;
        counters.sameMounter += sameMounterCount;
        return state.sum(state.checksums);
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    @Threads(1)
    public long sameRuntimeSinglePublisherBurst(SameRuntimeBurstState state, BurstCounters counters) {
        state.runBatch(counters);
        long checksum = 0;
        for (int i = 0; i < state.burstSize; i++) {
            checksum += state.checksums[i];
        }
        return checksum;
    }

    private static void awaitExternalResume(ExternalResumeState state, long expectedResumeCount) {
        final long deadline = System.nanoTime() + AWAIT_TIMEOUT_NANOS;
        while (state.task.resumeCount.get() != expectedResumeCount || state.waitQueue.size() != 1) {
            if (System.nanoTime() - deadline >= 0) {
                throw new IllegalStateException("timed out waiting for external Fiber resume");
            }
            Os.pause();
        }
    }

    private static WorkerPoolConfiguration configuration(String poolName, int workerCount, long sleepMillis) {
        return new WorkerPoolConfiguration() {
            @Override
            public int getFiberMaxLiveCount() {
                return Math.max(64, workerCount * 8);
            }

            @Override
            public int getFiberRetainedCount() {
                return Math.max(1, workerCount);
            }

            @Override
            public Metrics getMetrics() {
                return Metrics.DISABLED;
            }

            @Override
            public long getNapThreshold() {
                return 0;
            }

            @Override
            public String getPoolName() {
                return poolName;
            }

            @Override
            public long getSleepThreshold() {
                return 0;
            }

            @Override
            public long getSleepTimeout() {
                return sleepMillis;
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

    @AuxCounters(AuxCounters.Type.EVENTS)
    @State(Scope.Thread)
    public static class PublishCounters {
        public long mounts;
        public long sameMounter;

        @Setup(Level.Iteration)
        public void reset() {
            mounts = 0;
            sameMounter = 0;
        }
    }

    @AuxCounters(AuxCounters.Type.EVENTS)
    @State(Scope.Thread)
    public static class BurstCounters {
        public long bursts;
        public long tasks;
        public long workersUsed;

        @Setup(Level.Iteration)
        public void reset() {
            bursts = 0;
            tasks = 0;
            workersUsed = 0;
        }
    }

    @AuxCounters(AuxCounters.Type.EVENTS)
    @State(Scope.Thread)
    public static class ResumeCounters {
        public long mounts;
        public long sameMounter;

        @Setup(Level.Iteration)
        public void reset() {
            mounts = 0;
            sameMounter = 0;
        }
    }

    @State(Scope.Benchmark)
    public static class ExternalResumeState {
        private WorkerPool pool;
        private ExternalResumeTask task;
        private final FiberWalWaitQueue waitQueue = new FiberWalWaitQueue();
        @Param({"1", "2", "8"})
        public int workerCount;

        @Setup(Level.Trial)
        public void setup() {
            pool = new WorkerPool(configuration("fiber-external-workload", workerCount, 10));
            task = new ExternalResumeTask(waitQueue);
            pool.start();
            if (pool.getFiberRuntime().launch(task) != LaunchResult.LAUNCHED) {
                throw new IllegalStateException("could not launch external-resume workload Fiber");
            }
            final long deadline = System.nanoTime() + AWAIT_TIMEOUT_NANOS;
            while (waitQueue.size() != 1) {
                if (System.nanoTime() - deadline >= 0) {
                    throw new IllegalStateException("external-resume workload Fiber did not park");
                }
                Os.pause();
            }
        }

        @Setup(Level.Invocation)
        public void awaitIdlePool() {
            // Both revisions use a 10 ms long-idle timeout. Two complete timeouts put the pool
            // beyond yield/nap and make each measured publication target an idle Worker rather
            // than accidentally catching the carrier at the end of the preceding invocation.
            Os.sleep(20);
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

    @State(Scope.Benchmark)
    public static class SameRuntimeState {
        private AtomicLongArray checksums;
        private AtomicLongArray completedCount;
        private long completedCountBaseline;
        private AtomicReference<Throwable> failure;
        private AtomicIntegerArray inFlight;
        private WorkerPool pool;
        private AtomicIntegerArray remaining;
        private long sameMounterBaseline;
        private AtomicLongArray sameMounterCount;
        private PublishTask[] tasks;
        @Param({"1", "2", "8", "32"})
        public int workerCount;
        @Param({"0", "64", "512"})
        public int workTokens;

        @Setup(Level.Trial)
        public void setup() {
            checksums = new AtomicLongArray(workerCount);
            completedCount = new AtomicLongArray(workerCount);
            failure = new AtomicReference<>();
            inFlight = new AtomicIntegerArray(workerCount);
            remaining = new AtomicIntegerArray(workerCount);
            sameMounterCount = new AtomicLongArray(workerCount);
            tasks = new PublishTask[workerCount];
            final WorkerPoolConfiguration configuration = configuration(
                    "fiber-same-runtime-workload",
                    workerCount,
                    10
            );
            pool = new WorkerPool(configuration);
            final FiberRuntime runtime = pool.getFiberRuntime();
            for (int i = 0; i < workerCount; i++) {
                tasks[i] = new PublishTask(this, i);
            }
            pool.assign(new PublishJob(this, runtime));
            pool.start();
        }

        @Setup(Level.Invocation)
        public void prepareBatch() {
            awaitTasksTerminal();
            final Throwable error = failure.get();
            if (error != null) {
                throw new IllegalStateException("same-runtime workload failed", error);
            }
            sameMounterBaseline = sum(sameMounterCount);
            completedCountBaseline = sum(completedCount);
            final int operationsPerWorker = SAME_RUNTIME_OPERATIONS / workerCount;
            for (int i = 0; i < workerCount; i++) {
                if (remaining.get(i) != 0 || inFlight.get(i) != 0) {
                    throw new IllegalStateException("same-runtime workload batch overlapped");
                }
                remaining.set(i, operationsPerWorker);
            }
        }

        @TearDown(Level.Trial)
        public void close() {
            try {
                for (int i = 0; i < workerCount; i++) {
                    remaining.set(i, 0);
                }
                awaitTasksTerminal();
            } finally {
                pool.halt();
            }
        }

        private void awaitBatch() {
            final long expected = completedCountBaseline + SAME_RUNTIME_OPERATIONS;
            final long deadline = System.nanoTime() + AWAIT_TIMEOUT_NANOS;
            while (sum(completedCount) != expected || !areTasksTerminal()) {
                final Throwable error = failure.get();
                if (error != null) {
                    throw new IllegalStateException("same-runtime workload failed", error);
                }
                if (System.nanoTime() - deadline >= 0) {
                    throw new IllegalStateException("timed out waiting for same-runtime Fiber batch");
                }
                Os.pause();
            }
        }

        private void awaitTasksTerminal() {
            final long deadline = System.nanoTime() + AWAIT_TIMEOUT_NANOS;
            while (!areTasksTerminal()) {
                if (System.nanoTime() - deadline >= 0) {
                    throw new IllegalStateException("timed out waiting for same-runtime tasks to finish");
                }
                Os.pause();
            }
        }

        private boolean areTasksTerminal() {
            for (int i = 0; i < workerCount; i++) {
                if (inFlight.get(i) != 0 || (!tasks[i].isDone() && completedCount.get(i) != 0)) {
                    return false;
                }
            }
            return true;
        }

        private long sum(AtomicLongArray values) {
            long sum = 0;
            for (int i = 0; i < workerCount; i++) {
                sum += values.get(i);
            }
            return sum;
        }
    }

    @State(Scope.Benchmark)
    public static class SameRuntimeBurstState {
        private final AtomicInteger completedCount = new AtomicInteger();
        private final AtomicReference<Throwable> failure = new AtomicReference<>();
        private long[] checksums;
        private volatile int generation;
        private volatile boolean isPublisherStarted;
        private WorkerPool pool;
        private BurstTask[] tasks;
        private int[] workerIds;
        @Param({"2", "8", "64"})
        public int burstSize;
        @Param({"8"})
        public int workerCount;
        @Param({"512", "8192"})
        public int workTokens;

        @Setup(Level.Trial)
        public void setup() {
            if (burstSize > Math.max(64, workerCount * 8)) {
                throw new IllegalArgumentException("burst exceeds the Fiber admission limit");
            }
            checksums = new long[burstSize];
            tasks = new BurstTask[burstSize];
            workerIds = new int[burstSize];
            for (int i = 0; i < burstSize; i++) {
                tasks[i] = new BurstTask(this, i);
                workerIds[i] = -1;
            }
            pool = new WorkerPool(configuration("fiber-single-publisher-burst", workerCount, 10));
            pool.assign(0, new BurstPublishJob(this, pool.getFiberRuntime()));
            pool.start();
            final long deadline = System.nanoTime() + AWAIT_TIMEOUT_NANOS;
            while (!isPublisherStarted) {
                if (System.nanoTime() - deadline >= 0) {
                    throw new IllegalStateException("single-publisher Job did not start");
                }
                Os.pause();
            }
        }

        @Setup(Level.Invocation)
        public void prepareBatch() {
            awaitTasksTerminal();
            final Throwable error = failure.get();
            if (error != null) {
                throw new IllegalStateException("single-publisher burst failed", error);
            }
            completedCount.set(0);
            for (int i = 0; i < burstSize; i++) {
                final BurstTask task = tasks[i];
                if (task.isDone()) {
                    task.reopen();
                }
                checksums[i] = 0;
                workerIds[i] = -1;
            }
            // Worker 0 remains active in its Job. Two complete idle timeouts make the other
            // Workers overwhelmingly likely to be registered sleepers before publication.
            Os.sleep(20);
        }

        @TearDown(Level.Trial)
        public void close() {
            try {
                awaitTasksTerminal();
            } finally {
                pool.halt();
            }
        }

        private void awaitTasksTerminal() {
            final long deadline = System.nanoTime() + AWAIT_TIMEOUT_NANOS;
            while (true) {
                boolean isTerminal = true;
                for (int i = 0; i < burstSize; i++) {
                    final BurstTask task = tasks[i];
                    if (generation != 0 && !task.isDone()) {
                        isTerminal = false;
                        break;
                    }
                }
                if (isTerminal) {
                    return;
                }
                final Throwable error = failure.get();
                if (error != null) {
                    throw new IllegalStateException("single-publisher burst failed", error);
                }
                if (System.nanoTime() - deadline >= 0) {
                    throw new IllegalStateException("timed out waiting for burst tasks to finish");
                }
                Os.pause();
            }
        }

        private void runBatch(BurstCounters counters) {
            final int requestedGeneration = generation + 1;
            generation = requestedGeneration;
            final long deadline = System.nanoTime() + AWAIT_TIMEOUT_NANOS;
            while (completedCount.get() != burstSize) {
                final Throwable error = failure.get();
                if (error != null) {
                    throw new IllegalStateException("single-publisher burst failed", error);
                }
                if (System.nanoTime() - deadline >= 0) {
                    throw new IllegalStateException("timed out waiting for single-publisher burst");
                }
                Os.pause();
            }
            long workerMask = 0;
            for (int i = 0; i < burstSize; i++) {
                final int workerId = workerIds[i];
                if (workerId < 0 || workerId >= workerCount || workerId >= Long.SIZE) {
                    throw new IllegalStateException("invalid burst mounter [index=" + i
                            + ", workerId=" + workerId + ']');
                }
                workerMask |= 1L << workerId;
            }
            counters.bursts++;
            counters.tasks += burstSize;
            counters.workersUsed += Long.bitCount(workerMask);
        }
    }

    private static final class ExternalResumeTask extends FiberTask {
        private volatile boolean isLastResumeOnSameWorker;
        private volatile boolean isStopped;
        private final AtomicLong resumeCount = new AtomicLong();
        private final FiberWalWaitQueue waitQueue;

        private ExternalResumeTask(FiberWalWaitQueue waitQueue) {
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
                        throw new IllegalStateException("external-resume wait registration failed");
                    }
                    final int reason = fiber.suspendWait(token);
                    if (isStopped) {
                        return true;
                    }
                    if (reason != FiberWaitCoordinator.REASON_WAL) {
                        throw new IllegalStateException("unexpected external-resume wait reason");
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

    private static final class PublishJob implements Job {
        private final SameRuntimeState state;
        private final FiberRuntime runtime;

        private PublishJob(SameRuntimeState state, FiberRuntime runtime) {
            this.state = state;
            this.runtime = runtime;
        }

        @Override
        public boolean run(@NotNull WorkerContext workerContext) {
            final int workerId = workerContext.carrierId();
            if (state.remaining.get(workerId) <= 0) {
                return state.inFlight.get(workerId) != 0;
            }
            if (!state.inFlight.compareAndSet(workerId, 0, 1)) {
                return true;
            }
            final PublishTask task = state.tasks[workerId];
            if (task.isDone()) {
                task.reopen();
            }
            final LaunchResult result = runtime.launch(task);
            if (result != LaunchResult.LAUNCHED) {
                state.inFlight.set(workerId, 0);
                if (result != LaunchResult.ALREADY_OWNED) {
                    state.failure.compareAndSet(
                            null,
                            new IllegalStateException("same-runtime Fiber launch failed [result=" + result + ']')
                    );
                }
            }
            return true;
        }
    }

    private static final class PublishTask extends FiberTask {
        private final int publishingWorkerId;
        private final SameRuntimeState state;

        private PublishTask(SameRuntimeState state, int publishingWorkerId) {
            this.state = state;
            this.publishingWorkerId = publishingWorkerId;
        }

        @Override
        protected void onDone() {
            state.inFlight.set(publishingWorkerId, 0);
        }

        @Override
        protected void onError(Throwable th) {
            state.failure.compareAndSet(null, th);
            state.inFlight.set(publishingWorkerId, 0);
        }

        @Override
        protected boolean runStep() {
            long value = state.completedCount.get(publishingWorkerId) + publishingWorkerId + 1L;
            for (int i = 0; i < state.workTokens; i++) {
                value ^= value << 13;
                value ^= value >>> 7;
                value ^= value << 17;
            }
            state.checksums.lazySet(publishingWorkerId, value);
            if (Objects.requireNonNull(Worker.current()).getWorkerId() == publishingWorkerId) {
                state.sameMounterCount.incrementAndGet(publishingWorkerId);
            }
            state.remaining.decrementAndGet(publishingWorkerId);
            state.completedCount.incrementAndGet(publishingWorkerId);
            return true;
        }
    }

    private static final class BurstPublishJob implements Job {
        private int observedGeneration;
        private final FiberRuntime runtime;
        private final SameRuntimeBurstState state;

        private BurstPublishJob(SameRuntimeBurstState state, FiberRuntime runtime) {
            this.state = state;
            this.runtime = runtime;
        }

        @Override
        public boolean run(@NotNull WorkerContext workerContext) {
            state.isPublisherStarted = true;
            final int requestedGeneration = state.generation;
            if (requestedGeneration == observedGeneration) {
                // Keep only this publisher active while every peer follows the normal idle path.
                return true;
            }
            if (requestedGeneration != observedGeneration + 1) {
                state.failure.compareAndSet(null, new IllegalStateException(
                        "single-publisher generation skipped [expected=" + (observedGeneration + 1)
                                + ", actual=" + requestedGeneration + ']'
                ));
                observedGeneration = requestedGeneration;
                return true;
            }
            observedGeneration = requestedGeneration;
            for (int i = 0; i < state.burstSize; i++) {
                final LaunchResult result = runtime.launch(state.tasks[i]);
                if (result != LaunchResult.LAUNCHED) {
                    state.failure.compareAndSet(null, new IllegalStateException(
                            "single-publisher Fiber launch failed [index=" + i + ", result=" + result + ']'
                    ));
                    return true;
                }
            }
            return true;
        }
    }

    private static final class BurstTask extends FiberTask {
        private final int index;
        private final SameRuntimeBurstState state;

        private BurstTask(SameRuntimeBurstState state, int index) {
            this.state = state;
            this.index = index;
        }

        @Override
        protected void onError(Throwable th) {
            state.failure.compareAndSet(null, th);
        }

        @Override
        protected boolean runStep() {
            long value = ((long) state.generation << 32) ^ (index + 1L);
            for (int i = 0; i < state.workTokens; i++) {
                value ^= value << 13;
                value ^= value >>> 7;
                value ^= value << 17;
            }
            state.checksums[index] = value;
            state.workerIds[index] = Objects.requireNonNull(Worker.current()).getWorkerId();
            state.completedCount.incrementAndGet();
            return true;
        }
    }
}
