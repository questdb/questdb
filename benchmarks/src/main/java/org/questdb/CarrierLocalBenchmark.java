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
import io.questdb.std.CarrierLocal;
import io.questdb.std.Os;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.concurrent.TimeUnit;

@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
public class CarrierLocalBenchmark {

    private final CarrierLocal<Object> carrierLocal = CarrierLocal.withInitial(Object::new);
    private final ThreadLocal<Object> threadLocal = ThreadLocal.withInitial(Object::new);
    private Object value;

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(CarrierLocalBenchmark.class.getSimpleName())
                .warmupIterations(2)
                .measurementIterations(3)
                .forks(1)
                .build();

        new Runner(opt).run();
    }

    @Setup(Level.Trial)
    public void setup() {
        Os.init();
        // Bind the benchmark thread once so CarrierLocal hits its array path
        // rather than the per-Thread fallback.
        CarrierIdentity.bind();
        value = new Object();
        // Prime the slot so get() takes the hot fast path (no initial.apply).
        carrierLocal.set(value);
        threadLocal.set(value);
    }

    @Benchmark
    public int testCarrierIdentityCurrent() {
        return CarrierIdentity.current();
    }

    @Benchmark
    public Object testCarrierLocalGet() {
        return carrierLocal.get();
    }

    @Benchmark
    public void testCarrierLocalGetSet(Blackhole bh) {
        bh.consume(carrierLocal.get());
    }

    @Benchmark
    public Object testCarrierLocalSet() {
        carrierLocal.set(value);
        return carrierLocal.get();
    }

    @Benchmark
    public Object testThreadLocalGet() {
        return threadLocal.get();
    }

    @Benchmark
    public Object testThreadLocalGetSet() {
        threadLocal.set(value);
        return threadLocal.get();
    }

    @Benchmark
    public void testThreadLocalSet(Blackhole bh) {
        threadLocal.set(value);
        bh.consume(value);
    }
}