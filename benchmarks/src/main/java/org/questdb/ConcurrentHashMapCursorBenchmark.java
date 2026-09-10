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
import io.questdb.std.ConcurrentHashMap;
import io.questdb.std.Os;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@OperationsPerInvocation(256)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(value = 2, jvmArgsAppend = {"--add-exports=java.base/jdk.internal.vm=ALL-UNNAMED", "--enable-native-access=ALL-UNNAMED"})
public class ConcurrentHashMapCursorBenchmark {
    private static final int SCANS = 256;
    @Param({"0", "1", "16", "256"})
    public int size;
    private final ConcurrentHashMap.EntryCursor<Integer> cursor = new ConcurrentHashMap.EntryCursor<>();
    private final ConcurrentHashMap.EntryCursor<Integer> inner = new ConcurrentHashMap.EntryCursor<>();
    private Iterator<Map.Entry<CharSequence, Integer>> iterator;
    private ConcurrentHashMap<Integer> map;
    private FiberRuntime runtime;
    private final ScanTask task = new ScanTask();

    @Benchmark
    public long cachedIteratorScan() {
        return run(0);
    }

    @Benchmark
    public long cachedIteratorToTop() {
        return run(1);
    }

    @Benchmark
    public long cursorNestedScan() {
        return run(4);
    }

    @Benchmark
    public long cursorScan() {
        return run(2);
    }

    @Benchmark
    public long cursorToTop() {
        return run(3);
    }

    @Setup
    public void setup() {
        Os.init();
        CarrierIdentity.bind();
        map = new ConcurrentHashMap<>(Math.max(1, size));
        for (int i = 0; i < size; i++) {
            map.put("key" + i, i);
        }
        cursor.of(map);
        inner.of(map);
        runtime = new FiberRuntime(1);
        runtime.initializeCarrier();
    }

    @TearDown
    public void tearDown() {
        try {
            runtime.beginQuiesce();
            final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
            while (runtime.state() != FiberRuntimeState.CLOSED && System.nanoTime() < deadline) {
                runtime.drain(64);
            }
            if (!runtime.awaitClosed(deadline)) {
                throw new IllegalStateException("Fiber runtime did not close");
            }
            runtime.closeAfterDrained();
            cursor.clear();
            inner.clear();
        } finally {
            CarrierIdentity.unbind();
        }
    }

    private long run(int operation) {
        if (task.isDone()) {
            task.reopen();
        }
        task.operation = operation;
        if (runtime.launch(task) != LaunchResult.LAUNCHED) {
            throw new IllegalStateException("Fiber launch failed");
        }
        while (!task.isDone()) {
            runtime.drain(64);
        }
        if (task.failure != null) {
            throw new IllegalStateException("Fiber failed", task.failure);
        }
        return task.result;
    }

    private final class ScanTask extends FiberTask {
        private Throwable failure;
        private int operation;
        private long result;

        @Override
        protected void onError(Throwable th) {
            failure = th;
        }

        @Override
        protected boolean runStep() {
            long sum = 0;
            switch (operation) {
                case 0 -> {
                    for (int i = 0; i < SCANS; i++) {
                        iterator = map.entrySet().iterator();
                        while (iterator.hasNext()) {
                            final Map.Entry<CharSequence, Integer> entry = iterator.next();
                            sum += entry.getKey().length() + entry.getValue();
                        }
                    }
                }
                case 1 -> {
                    for (int i = 0; i < SCANS; i++) {
                        iterator = map.entrySet().iterator();
                        sum += iterator.hasNext() ? 1 : 0;
                    }
                }
                case 2 -> {
                    for (int i = 0; i < SCANS; i++) {
                        cursor.toTop();
                        while (cursor.hasNext()) {
                            sum += cursor.getKey().length() + cursor.getValue();
                        }
                    }
                }
                case 3 -> {
                    for (int i = 0; i < SCANS; i++) {
                        cursor.toTop();
                        sum += cursor.hasNext() ? 1 : 0;
                    }
                }
                case 4 -> {
                    for (int i = 0; i < SCANS; i++) {
                        cursor.toTop();
                        while (cursor.hasNext()) {
                            inner.toTop();
                            while (inner.hasNext()) {
                                sum += cursor.getValue() + inner.getValue();
                            }
                        }
                    }
                }
                default -> throw new IllegalStateException("unknown operation");
            }
            result = sum;
            return true;
        }
    }
}
