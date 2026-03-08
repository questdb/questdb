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

import io.questdb.cairo.TxnScoreboardV2;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Group;
import org.openjdk.jmh.annotations.GroupThreads;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@BenchmarkMode(Mode.AverageTime)
public class TxnScoreboardV2Benchmark {

    private static final int RANGE_SIZE = 20;
    private final int entryCount = 320;
    private TxnScoreboardV2 scoreboard;

    public static void main(String[] args) throws Exception {
        Options opt = new OptionsBuilder()
                .include(TxnScoreboardV2Benchmark.class.getSimpleName())
                .warmupIterations(1)
                .measurementIterations(2)
                .forks(1)
                .build();

        new Runner(opt).run();
    }

    @Setup(Level.Trial)
    public void setup() {
        scoreboard = new TxnScoreboardV2(entryCount);
        for (int i = 0; i < entryCount; i++) {
            if (i % 10 == 0) {
                scoreboard.acquireTxn(i, i);
            }
        }
    }

    @TearDown(Level.Trial)
    public void tearDown() {
        scoreboard.close();
    }

    @Benchmark
    @Group("acquireAndRelease")
    @GroupThreads(4)
    public void testAcquireRelease(Blackhole bh) {
        int id = ThreadLocalRandom.current().nextInt(entryCount);
        long txn = System.nanoTime();

        if (scoreboard.acquireTxn(id, txn)) {
            bh.consume(scoreboard.releaseTxn(id, txn));
        }
    }

    @Benchmark
    @Group("activeReaderCount")
    @GroupThreads(4)
    public void testActiveReaderCount(Blackhole bh) {
        bh.consume(scoreboard.getActiveReaderCount(100));
    }

    @Benchmark
    @Group("mixed")
    @GroupThreads(4)
    public void testMixedWorkload(Blackhole bh) {
        if (ThreadLocalRandom.current().nextBoolean()) {
            testAcquireRelease(bh);
        } else {
            testRangeCheck(bh);
        }
    }

    @Benchmark
    @Group("rangeAvailable")
    @GroupThreads(4)
    public void testRangeCheck(Blackhole bh) {
        long from = ThreadLocalRandom.current().nextLong(entryCount - RANGE_SIZE);
        long to = from + RANGE_SIZE;
        bh.consume(scoreboard.isRangeAvailable(from, to));
    }
}
