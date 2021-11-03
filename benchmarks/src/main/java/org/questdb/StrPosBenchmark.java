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

import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.engine.functions.StrFunction;
import io.questdb.griffin.engine.functions.constants.CharConstant;
import io.questdb.griffin.engine.functions.str.StrPosCharFunctionFactory;
import io.questdb.griffin.engine.functions.str.StrPosFunctionFactory;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.profile.GCProfiler;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class StrPosBenchmark {

    private static final int N = 1_000_000;

    private static Function strFunc;
    private static Function substrFunc;
    private static Record[] records;
    private static String[] strings;

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(StrPosBenchmark.class.getSimpleName())
                .addProfiler(GCProfiler.class)
                .warmupIterations(2)
                .measurementIterations(3)
                .forks(1)
                .build();

        new Runner(opt).run();
    }

    public StrPosBenchmark() {
        strFunc = new StrFunction() {
            @Override
            public CharSequence getStr(Record rec) {
                return rec.getStr(0);
            }

            @Override
            public CharSequence getStrB(Record rec) {
                return rec.getStr(0);
            }
        };
        substrFunc = new CharConstant(',');

        strings = new String[N];
        for (int i = 0; i < N; i++) {
            int startLen = ThreadLocalRandom.current().nextInt(1, 1000);
            strings[i] = "a".repeat(startLen) + ",b";
        }

        records = new Record[N];
        for (int i = 0; i < N; i++) {
            int finalI = i;
            records[i] = new Record() {
                @Override
                public String getStr(int col) {
                    return strings[finalI];
                }
            };
        }
    }

    @Benchmark
    public int testBaseline() {
        Function function = new StrPosFunctionFactory.Func(strFunc, substrFunc);
        int sum = 0;
        for (int i = 0; i < N; i++) {
            sum += function.getInt(records[i]);
        }
        return sum;
    }

    @Benchmark
    public int testCharOverload() {
        Function function = new StrPosCharFunctionFactory.Func(strFunc, substrFunc);
        int sum = 0;
        for (int i = 0; i < N; i++) {
            sum += function.getInt(records[i]);
        }
        return sum;
    }
}
