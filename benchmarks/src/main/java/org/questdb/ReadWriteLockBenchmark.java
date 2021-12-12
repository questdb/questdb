/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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

import io.questdb.std.BiasedReadWriteLock;
import io.questdb.std.Rnd;
import io.questdb.std.SimpleReadWriteLock;
import io.questdb.std.XBiasedReadWriteLock;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

@Threads(Threads.MAX)
@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
public class ReadWriteLockBenchmark {

    private static final int NUM_SPINS = 10;

    private final Rnd rnd = new Rnd();

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(ReadWriteLockBenchmark.class.getSimpleName())
                .warmupIterations(3)
                .measurementIterations(3)
                .addProfiler("gc")
                .forks(1)
                .build();

        new Runner(opt).run();
    }

    @Benchmark
    public void testBaseline(Blackhole bh) {
        int sum = 0;
        for (int i = 0; i < NUM_SPINS; i++) {
            sum += rnd.nextInt();
        }
        bh.consume(sum);
    }

    @Benchmark
    public void testLock(BenchmarkState state, Blackhole bh) {
        final Lock rLock = state.lock.readLock();
        rLock.lock();
        // emulate some work
        int sum = 0;
        for (int i = 0; i < NUM_SPINS; i++) {
            sum += rnd.nextInt();
        }
        bh.consume(sum);
        rLock.unlock();
    }

    @State(Scope.Benchmark)
    public static class BenchmarkState {

        @Param({"JUC", "SIMPLE", "BIASED", "XBIASED"})
        public LockType type;
        public ReadWriteLock lock;

        @Setup(Level.Trial)
        public void setUp() {
            switch (type) {
                case JUC:
                    lock = new ReentrantReadWriteLock();
                    break;
                case SIMPLE:
                    lock = new SimpleReadWriteLock();
                    break;
                case BIASED:
                    lock = new BiasedReadWriteLock();
                    break;
                case XBIASED:
                    lock = new XBiasedReadWriteLock();
                    break;
                default:
                    throw new IllegalStateException("unknown lock type: " + type);
            }
        }
    }

    public enum LockType {
        JUC, SIMPLE, BIASED, XBIASED
    }
}
