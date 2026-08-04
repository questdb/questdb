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

import io.questdb.std.Os;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.concurrent.TimeUnit;

/**
 * Compares {@link Os#pause()} and {@link Os#sleep(long)} against the
 * {@code Thread.sleep}-based implementations they replaced.
 * <p>
 * Read {@code ·gc.alloc.rate.norm} first: since JDK 25 {@code Thread.beforeSleep}
 * constructs a {@code jdk.internal.event.ThreadSleepEvent} before testing
 * {@code isEnabled()}, so every {@code Thread.sleep} call allocates 40 bytes even
 * with JFR off. The legacy rows should report ~40 B/op and the current rows 0 B/op.
 * <p>
 * Read the timings second -- they are the no-regression check. Sleep duration is
 * dominated by the OS timer, so {@code testLegacySleep*} and {@code testOsSleep*}
 * must land within noise of each other; a native sleep that overshoots would show
 * up here. {@code testOsPause} vs {@code testLegacyPause} compares the two spin
 * yields, where the syscall dominates and the calling convention does not.
 */
@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 2, time = 2)
@Measurement(iterations = 3, time = 3)
public class OsSleepBenchmark {

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(OsSleepBenchmark.class.getSimpleName())
                .addProfiler("gc")
                .forks(1)
                .build();

        new Runner(opt).run();
    }

    @Setup(Level.Trial)
    public void setup() {
        Os.init();
        // Resolve the qdb_sleep_millis downcall handle outside the measured region.
        Os.sleep(1);
    }

    @Benchmark
    @OutputTimeUnit(TimeUnit.NANOSECONDS)
    public void testLegacyPause() {
        legacyPause();
    }

    @Benchmark
    public void testLegacySleep1() {
        legacySleep(1);
    }

    @Benchmark
    public void testLegacySleep10() {
        legacySleep(10);
    }

    @Benchmark
    @OutputTimeUnit(TimeUnit.NANOSECONDS)
    public void testOsPause() {
        Os.pause();
    }

    @Benchmark
    public void testOsSleep1() {
        Os.sleep(1);
    }

    @Benchmark
    public void testOsSleep10() {
        Os.sleep(10);
    }

    private static void legacyPause() {
        try {
            Thread.sleep(0);
        } catch (InterruptedException ignore) {
        }
    }

    private static void legacySleep(long millis) {
        long t = System.currentTimeMillis();
        long deadline = millis;
        while (deadline > 0) {
            try {
                Thread.sleep(deadline);
                break;
            } catch (InterruptedException e) {
                long t2 = System.currentTimeMillis();
                deadline -= t2 - t;
                t = t2;
            }
        }
    }
}
