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

import io.questdb.cairo.ev.RowExpiryHeap;
import io.questdb.std.Rnd;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.concurrent.TimeUnit;

@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
public class RowExpiryHeapBenchmark {

    @Param({"100", "1000", "10000"})
    public int partitionCount;
    private final Rnd rnd = new Rnd();
    private RowExpiryHeap heap;

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(RowExpiryHeapBenchmark.class.getSimpleName())
                .warmupIterations(3)
                .measurementIterations(5)
                .forks(1)
                .build();

        new Runner(opt).run();
    }

    @Setup(Level.Iteration)
    public void setup() {
        heap = new RowExpiryHeap();
        rnd.reset();
        for (int i = 0; i < partitionCount; i++) {
            heap.upsert(i, rnd.nextPositiveLong());
        }
    }

    @Benchmark
    public long testPeekNextExpiry() {
        return heap.peekNextExpiry();
    }

    @Benchmark
    public int testPollAndReinsert() {
        int idx = heap.pollPartitionIndex();
        heap.upsert(idx, rnd.nextPositiveLong());
        return idx;
    }

    @Benchmark
    public void testUpsertExisting() {
        int idx = rnd.nextInt(partitionCount);
        heap.upsert(idx, rnd.nextPositiveLong());
    }

    @Benchmark
    public void testUpsertNew() {
        // Upsert a new partition beyond existing range, then remove it to keep state stable
        int newIdx = partitionCount + 1;
        heap.upsert(newIdx, rnd.nextPositiveLong());
        heap.remove(newIdx);
    }
}
