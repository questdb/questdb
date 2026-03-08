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

import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.engine.functions.CharFunction;
import io.questdb.griffin.engine.functions.StrFunction;
import io.questdb.griffin.engine.functions.str.StrPosCharFunctionFactory;
import io.questdb.griffin.engine.functions.str.StrPosFunctionFactory;
import io.questdb.std.Rnd;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.concurrent.TimeUnit;

@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
public class StrPosBenchmark {

    private static final int N = 1_000_000;
    private final Function charConstFunc;
    private final Function charFunc;
    private final Record[] records;
    private final Rnd rnd = new Rnd();
    private final Function strConstFunc;
    private final Function strFunc;
    private final String[] strings;

    public StrPosBenchmark() {
        StringBuilder builder = new StringBuilder();
        strings = new String[N];
        for (int i = 0; i < N; i++) {
            builder.setLength(0);
            int startLen = rnd.nextInt(32);
            for (int j = 0; j < startLen; j++) {
                builder.append('a');
            }
            builder.append(",b");
            strings[i] = builder.toString();
        }

        records = new Record[N];
        for (int i = 0; i < N; i++) {
            int finalI = i;
            records[i] = new Record() {
                @Override
                public String getStrA(int col) {
                    return strings[finalI];
                }
            };
        }

        final Function strInputFunc = new StrFunction() {
            @Override
            public CharSequence getStrA(Record rec) {
                return rec.getStrA(0);
            }

            @Override
            public CharSequence getStrB(Record rec) {
                return rec.getStrA(0);
            }
        };
        final Function substrStrInputFunc = new StrFunction() {
            @Override
            public CharSequence getStrA(Record rec) {
                return ",";
            }

            @Override
            public CharSequence getStrB(Record rec) {
                return ",";
            }
        };
        final Function substrCharInputFunc = new CharFunction() {
            @Override
            public char getChar(Record rec) {
                return ',';
            }

            @Override
            public boolean isThreadSafe() {
                return true;
            }
        };

        strFunc = new StrPosFunctionFactory.Func(strInputFunc, substrStrInputFunc);
        strConstFunc = new StrPosFunctionFactory.ConstFunc(strInputFunc, ",");
        charFunc = new StrPosCharFunctionFactory.Func(strInputFunc, substrCharInputFunc);
        charConstFunc = new StrPosCharFunctionFactory.ConstFunc(strInputFunc, ',');
    }

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(StrPosBenchmark.class.getSimpleName())
                .warmupIterations(2)
                .measurementIterations(3)
                .forks(1)
                .build();

        new Runner(opt).run();
    }

    @Benchmark
    public int testBaseline() {
        return rnd.nextInt(N);
    }

    @Benchmark
    public int testCharConstOverload() {
        int i = rnd.nextInt(N);
        return charConstFunc.getInt(records[i]);
    }

    @Benchmark
    public int testCharOverload() {
        int i = rnd.nextInt(N);
        return charFunc.getInt(records[i]);
    }

    @Benchmark
    public int testStrConstOverload() {
        int i = rnd.nextInt(N);
        return strConstFunc.getInt(records[i]);
    }

    @Benchmark
    public int testStrOverload() {
        int i = rnd.nextInt(N);
        return strFunc.getInt(records[i]);
    }
}
