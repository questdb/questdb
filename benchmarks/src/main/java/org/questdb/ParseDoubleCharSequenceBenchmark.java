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

import io.questdb.std.Numbers;
import io.questdb.std.NumericException;
import io.questdb.std.Os;
import io.questdb.std.Rnd;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
public class ParseDoubleCharSequenceBenchmark {
    private final static List<String> doubles = new ArrayList<>();
    private static final int N = 100;

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(ParseDoubleCharSequenceBenchmark.class.getSimpleName())
                .warmupIterations(2)
                .measurementIterations(2)
                .addProfiler("gc")
                .forks(1)
                .build();
        new Runner(opt).run();
    }

    @Setup(Level.Iteration)
    public void setup() {
        Os.init();
        doubles.clear();
        Rnd rnd= new Rnd();
        for (int i = 0; i < N; i++) {
            doubles.add(Double.toString(rnd.nextDouble()));
        }
    }

    @Benchmark
    public double testNumbersParseDouble() throws NumericException {
        double sum = 0;
        for (int i = 0; i<N; i++) {
            sum += Numbers.parseDouble(doubles.get(i));
        }
        return sum;
    }

    @Benchmark
    public double testDoubleParseDouble() {
        double sum = 0;
        for (int i = 0; i<N; i++) {
            sum += Double.parseDouble(doubles.get(i));
        }
        return sum;
    }
}
