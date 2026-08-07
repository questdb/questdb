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

import io.questdb.mp.continuation.Fiber;
import io.questdb.mp.continuation.FiberRuntime;
import io.questdb.mp.continuation.FiberRuntimeState;
import io.questdb.mp.continuation.FiberTask;
import io.questdb.mp.continuation.FiberWaitCoordinator;
import io.questdb.mp.continuation.FiberWalWaitQueue;
import io.questdb.mp.continuation.FiberWalWaitRegistration;
import io.questdb.mp.continuation.LaunchResult;
import io.questdb.mp.continuation.SourceRegistrationResult;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
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

@BenchmarkMode(Mode.AverageTime)
@Fork(
        value = 1,
        jvmArgsAppend = {
                "--add-exports=java.base/jdk.internal.vm=ALL-UNNAMED",
                "--enable-native-access=ALL-UNNAMED"
        }
)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
public class FiberRuntimeBenchmark {

    public static void main(String[] args) throws RunnerException {
        final Options options = new OptionsBuilder()
                .include(FiberRuntimeBenchmark.class.getSimpleName())
                .warmupIterations(3)
                .measurementIterations(5)
                .forks(1)
                .build();
        new Runner(options).run();
    }

    @Benchmark
    @Threads(4)
    public boolean launchAndCompleteContended(RuntimeState runtimeState, TaskState taskState) {
        return launchAndComplete(runtimeState.runtime, taskState.task);
    }

    @Benchmark
    @Threads(1)
    public boolean launchAndCompleteSingle(RuntimeState runtimeState, TaskState taskState) {
        return launchAndComplete(runtimeState.runtime, taskState.task);
    }

    @Benchmark
    @Threads(4)
    public boolean parkAndWakeContended(RuntimeState runtimeState, WaitTaskState taskState) {
        return parkAndWake(runtimeState.runtime, taskState.task, taskState.waitQueue);
    }

    @Benchmark
    @Threads(1)
    public boolean parkAndWakeSingle(RuntimeState runtimeState, WaitTaskState taskState) {
        return parkAndWake(runtimeState.runtime, taskState.task, taskState.waitQueue);
    }

    private static boolean launchAndComplete(FiberRuntime runtime, OneShotTask task) {
        if (task.isDone()) {
            task.reopen();
        }
        while (runtime.launch(task) != LaunchResult.LAUNCHED) {
            runtime.drain(64);
        }
        while (!task.isDone()) {
            runtime.drain(64);
        }
        return true;
    }

    private static boolean parkAndWake(
            FiberRuntime runtime,
            WaitTask task,
            FiberWalWaitQueue waitQueue
    ) {
        if (task.isDone()) {
            task.reopen();
        }
        while (runtime.launch(task) != LaunchResult.LAUNCHED) {
            runtime.drain(64);
        }
        while (waitQueue.size() == 0) {
            runtime.drain(64);
        }
        waitQueue.fire(1, false);
        while (!task.isDone()) {
            runtime.drain(64);
        }
        return true;
    }

    @State(Scope.Benchmark)
    public static class RuntimeState {
        private final FiberRuntime runtime = new FiberRuntime(64, 256);

        @TearDown(Level.Trial)
        public void close() {
            runtime.initializeCarrier();
            runtime.beginQuiesce();
            final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
            while (runtime.state() != FiberRuntimeState.CLOSED && System.nanoTime() < deadline) {
                runtime.drain(256);
            }
            if (!runtime.awaitClosed(deadline)) {
                throw new IllegalStateException("fiber runtime did not close");
            }
            runtime.closeAfterDrained();
        }
    }

    @State(Scope.Thread)
    public static class TaskState {
        private final OneShotTask task = new OneShotTask();

        @Setup(Level.Trial)
        public void setup(RuntimeState runtimeState) {
            runtimeState.runtime.initializeCarrier();
        }
    }

    @State(Scope.Thread)
    public static class WaitTaskState {
        private final WaitTask task;
        private final FiberWalWaitQueue waitQueue = new FiberWalWaitQueue();

        public WaitTaskState() {
            task = new WaitTask(waitQueue);
        }

        @Setup(Level.Trial)
        public void setup(RuntimeState runtimeState) {
            runtimeState.runtime.initializeCarrier();
        }
    }

    private static class OneShotTask extends FiberTask {
        @Override
        protected boolean runStep() {
            return true;
        }
    }

    private static class WaitTask extends FiberTask {
        private final FiberWalWaitQueue waitQueue;

        private WaitTask(FiberWalWaitQueue waitQueue) {
            this.waitQueue = waitQueue;
        }

        @Override
        protected boolean runStep() {
            final Fiber fiber = Objects.requireNonNull(Fiber.current());
            final FiberWaitCoordinator coordinator = fiber.getWaitCoordinator();
            final long token = fiber.beginWaitBuild(1);
            final FiberWalWaitRegistration registration = coordinator.acquireWal(token, 1);
            try {
                if (registration.register(waitQueue) != SourceRegistrationResult.ACCEPTED) {
                    throw new IllegalStateException("wait registration failed");
                }
                if (fiber.suspendWait(token) != FiberWaitCoordinator.REASON_WAL) {
                    throw new IllegalStateException("unexpected wait reason");
                }
                return true;
            } finally {
                registration.cancel();
                coordinator.abort(token);
                coordinator.consume(token);
            }
        }
    }
}
