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
import io.questdb.griffin.engine.functions.LongFunction;
import io.questdb.griffin.engine.functions.conditional.CoalesceFunctionFactory;
import io.questdb.griffin.engine.functions.constants.LongConstant;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.concurrent.TimeUnit;

@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
public class CoalesceBenchmark {
    private static final int N = 1000;
    private static ObjList<Function> constFunctions;
    private static Record[] records;

    public CoalesceBenchmark() {
        constFunctions = new ObjList<>(2);
        constFunctions.add(new LongFunction() {
            @Override
            public long getLong(Record rec) {
                return rec.getLong(0);
            }

            @Override
            public boolean isThreadSafe() {
                return true;
            }
        });
        constFunctions.add(new LongConstant(10L));
        records = new Record[N];
        for (int i = 0; i < N; i++) {
            final long value = i % 2 == 0 ? Numbers.LONG_NULL : i;
            records[i] = new Record() {
                @Override
                public long getLong(int col) {
                    return value;
                }
            };
        }
    }

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(CoalesceBenchmark.class.getSimpleName())
                .warmupIterations(2)
                .measurementIterations(3)
                .forks(1)
                .build();

        new Runner(opt).run();
    }

    @Benchmark
    public long testBaseline() {
        Function function = new CoalesceFunctionFactory.LongCoalesceFunction(constFunctions, constFunctions.size());
        long sum = 0;
        for (int i = 0; i < N; i++) {
            sum += function.getLong(records[i]);
        }
        return sum;
    }

    @Benchmark
    public long testTwoLongCoalesceDedicated() {
        Function function = new CoalesceFunctionFactory.TwoLongCoalesceFunction(constFunctions);
        long sum = 0;
        for (int i = 0; i < N; i++) {
            sum += function.getLong(records[i]);
        }
        return sum;
    }
}
