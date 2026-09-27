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

import io.questdb.mp.CarrierIdentity;
import io.questdb.mp.continuation.FiberRuntime;
import io.questdb.mp.continuation.FiberRuntimeState;
import io.questdb.mp.continuation.FiberTask;
import io.questdb.mp.continuation.LaunchResult;
import io.questdb.std.CarrierLocal;
import io.questdb.std.FiberLocal;
import io.questdb.std.Misc;
import io.questdb.std.Os;
import io.questdb.std.str.StringSink;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.TimeValue;

import java.util.concurrent.TimeUnit;

@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
public class FiberLocalBenchmark {
    private static final FiberLocal<Object> FIBER_LOCAL;
    private static final int MOUNTED_OPS = 1_000_000;
    private static final int PRODUCTION_KEY_COUNT = 69;
    private final CarrierLocal<Object> carrierLocal = CarrierLocal.withInitial(Object::new);
    private final CarrierLocalTask carrierLocalTask = new CarrierLocalTask();
    private final FiberLocalTask fiberLocalTask = new FiberLocalTask();
    private final FiberRuntime runtime = new FiberRuntime(64, 256);
    private final SinkTask sinkTask = new SinkTask();

    public static void main(String[] args) throws RunnerException {
        final Options options = new OptionsBuilder()
                .include(FiberLocalBenchmark.class.getSimpleName())
                .jvmArgsAppend("--add-exports=java.base/jdk.internal.vm=ALL-UNNAMED", "--enable-native-access=ALL-UNNAMED")
                .warmupIterations(3)
                .warmupTime(TimeValue.seconds(2))
                .measurementIterations(5)
                .measurementTime(TimeValue.seconds(3))
                .forks(1)
                .build();
        new Runner(options).run();
    }

    @Setup(Level.Trial)
    public void setup() {
        Os.init();
        CarrierIdentity.bind();
        runtime.initializeCarrier();
        carrierLocal.get();
        FIBER_LOCAL.get();
        Misc.getThreadLocalSink();
        runMounted(fiberLocalTask);
    }

    @TearDown(Level.Trial)
    public void tearDown() {
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

    @Benchmark
    public Object testCarrierLocalGet() {
        return carrierLocal.get();
    }

    @Benchmark
    @OperationsPerInvocation(MOUNTED_OPS)
    public Object testCarrierLocalGetMounted() {
        return runMounted(carrierLocalTask);
    }

    @Benchmark
    public Object testFiberLocalGet() {
        return FIBER_LOCAL.get();
    }

    @Benchmark
    @OperationsPerInvocation(MOUNTED_OPS)
    public Object testFiberLocalGetMounted() {
        return runMounted(fiberLocalTask);
    }

    @Benchmark
    public StringSink testThreadLocalSinkGet() {
        return Misc.getThreadLocalSink();
    }

    @Benchmark
    @OperationsPerInvocation(MOUNTED_OPS)
    public Object testThreadLocalSinkGetMounted() {
        return runMounted(sinkTask);
    }

    private Object runMounted(LoopTask task) {
        if (task.isDone()) {
            task.reopen();
        }
        while (runtime.launch(task) != LaunchResult.LAUNCHED) {
            runtime.drain(64);
        }
        while (!task.isDone()) {
            runtime.drain(64);
        }
        return task.last;
    }

    private final class CarrierLocalTask extends LoopTask {
        @Override
        protected boolean runStep() {
            Object value = null;
            for (int i = 0; i < MOUNTED_OPS; i++) {
                value = carrierLocal.get();
            }
            last = value;
            return true;
        }
    }

    private final class FiberLocalTask extends LoopTask {
        @Override
        protected boolean runStep() {
            Object value = null;
            for (int i = 0; i < MOUNTED_OPS; i++) {
                value = FIBER_LOCAL.get();
            }
            last = value;
            return true;
        }
    }

    private abstract static class LoopTask extends FiberTask {
        Object last;
    }

    private static final class SinkTask extends LoopTask {
        @Override
        protected boolean runStep() {
            Object value = null;
            for (int i = 0; i < MOUNTED_OPS; i++) {
                value = Misc.getThreadLocalSink();
            }
            last = value;
            return true;
        }
    }

    static {
        FiberLocal<Object> key = null;
        for (int i = 0; i < PRODUCTION_KEY_COUNT; i++) {
            key = new FiberLocal<>(Object::new);
        }
        FIBER_LOCAL = key;
    }
}
