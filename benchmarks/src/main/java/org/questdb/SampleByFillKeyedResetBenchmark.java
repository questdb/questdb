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

import io.questdb.std.BitSet;
import io.questdb.std.IntList;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.Arrays;
import java.util.concurrent.TimeUnit;

/**
 * Gate benchmark for Phase 17 m3 refactor of SampleByFillCursor
 * (17-02-PLAN.md D-16). Compares three reset paths that could stand in for
 * the per-bucket clear of {@code keyPresent} and the cold init of
 * {@code outputColToKeyPos}:
 * <p>
 * 1) {@code resetPrimitiveArrays} -- baseline: boolean[] + int[] with
 * Arrays.fill. This is what shipped.
 * 2) {@code resetBitSet} -- candidate that replaces boolean[] with BitSet.
 * Per-bucket set() + clear() path.
 * 3) {@code resetIntListSetAll} -- cold-path init of IntList.setAll() used
 * as a one-shot constructor-time replacement for
 * new int[n] + Arrays.fill(-1).
 * <p>
 * Outcome captured on sm_fill_prev_fast_path at 2026-04-22: at uniqueKeys=1000
 * BitSet is ~20x slower than boolean[] (set() is the dominant cost because
 * it walks wordIndex + checkCapacity + OR per call); the 5%-at-1000 gate from
 * D-16 therefore reverts the boolean[] -> BitSet swap. IntList.setAll()
 * at uniqueKeys=1000 is ~1.5x faster than new int[n] + Arrays.fill because
 * the backing array is reused across cursor instances; the int[] -> IntList
 * swap for outputColToKeyPos landed.
 * <p>
 * Benchmark retained so a future refactor can re-evaluate if BitSet ever
 * gains a faster set() (e.g. via page-sized words or bulk set-range).
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 3)
@Measurement(iterations = 5)
@Fork(1)
public class SampleByFillKeyedResetBenchmark {

    @Param({"10", "100", "1000", "10000"})
    public int uniqueKeys;

    private BitSet bitSet;
    private boolean[] boolArray;
    private IntList outputColToKeyPosIntList;
    private int[] outputColToKeyPosPrim;

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(SampleByFillKeyedResetBenchmark.class.getSimpleName())
                .build();
        new Runner(opt).run();
    }

    @Benchmark
    public void resetBitSet(Blackhole bh) {
        // Candidate path: BitSet.clear() backs Arrays.fill(long[], 0).
        // Mark every entry to simulate full-density bucket before clearing.
        for (int i = 0; i < uniqueKeys; i++) {
            bitSet.set(i);
        }
        bitSet.clear();
        bh.consume(bitSet.get(0));
    }

    @Benchmark
    public void resetIntListSetAll(Blackhole bh) {
        // Cold-path benchmark for outputColToKeyPos initialization (setAll
        // is called once per cursor instance, not per bucket). Included to
        // catch any regression relative to new int[] + Arrays.fill.
        outputColToKeyPosIntList.setAll(uniqueKeys, -1);
        bh.consume(outputColToKeyPosIntList.getQuick(0));
    }

    @Benchmark
    public void resetPrimitiveArrays(Blackhole bh) {
        // Baseline: boolean[] + int[] primitive arrays with Arrays.fill.
        for (int i = 0; i < uniqueKeys; i++) {
            boolArray[i] = true;
        }
        Arrays.fill(boolArray, 0, uniqueKeys, false);
        Arrays.fill(outputColToKeyPosPrim, 0, uniqueKeys, -1);
        bh.consume(boolArray[0]);
        bh.consume(outputColToKeyPosPrim[0]);
    }

    @Setup(Level.Iteration)
    public void setup() {
        boolArray = new boolean[uniqueKeys];
        outputColToKeyPosPrim = new int[uniqueKeys];
        bitSet = new BitSet(uniqueKeys);
        outputColToKeyPosIntList = new IntList(uniqueKeys);
    }
}
