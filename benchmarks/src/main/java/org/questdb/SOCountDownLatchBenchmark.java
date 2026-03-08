/*******************************************************************************
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

import io.questdb.mp.SOCountDownLatch;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
public class SOCountDownLatchBenchmark {

    private final static int SIZE = 1024;

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(SOCountDownLatchBenchmark.class.getSimpleName())
                .warmupIterations(1)
                .measurementIterations(3)
                .forks(1)
                .build();

        new Runner(opt).run();
    }

    @Benchmark
    public void testBaseline(Blackhole bh) {
        SOCountDownLatch latch = new SOCountDownLatch(2);
        Thread thA = new Thread(() -> {
            for (int i = 0; i < SIZE; i++) {
                bh.consume(i);
            }
            latch.countDown();
        });
        Thread thB = new Thread(() -> {
            for (int i = 0; i < SIZE; i++) {
                bh.consume(i);
            }
            latch.countDown();
        });
        thA.start();
        thB.start();
        latch.await();
    }

    @Benchmark
    public void testLatchPingPong() {
        SOCountDownLatch latchA = new SOCountDownLatch(SIZE);
        SOCountDownLatch latchB = new SOCountDownLatch(SIZE);
        SOCountDownLatch latch = new SOCountDownLatch(2);
        Thread thA = new Thread(() -> {
            for (int i = 0; i < SIZE; i++) {
                latchA.countDown();
            }
            latchB.await();
            latch.countDown();
        });
        Thread thB = new Thread(() -> {
            latchA.await();
            for (int i = 0; i < SIZE; i++) {
                latchB.countDown();
            }
            latch.countDown();
        });
        thA.start();
        thB.start();
        latch.await();
    }
}
