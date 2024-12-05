/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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

import io.questdb.cairo.*;
import io.questdb.griffin.*;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.SECONDS)
public class InsertIntoSelectBenchmark {

    // Should be set close enough to the cairo.max.uncommitted.rows default value.
    private static final int ROWS_PER_ITERATION = 1;
    private final CairoConfiguration configuration = new DefaultCairoConfiguration(".");
    @Param({"8192", "16384", "32786", "131072"})
    public String batchSize;
    //@Param({"10000", "1000000", "100000000", "1000000000"})
    @Param({"1000000000"})
    public String size;

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(InsertIntoSelectBenchmark.class.getSimpleName())
                .warmupIterations(1)
                .warmupBatchSize(10)
                .measurementBatchSize(10)
                .measurementIterations(3)
                .forks(0)
                .build();

        new Runner(opt).run();
    }


    @Setup(Level.Iteration)
    public void setup() {
        executeDdl("drop table if exists test1", configuration);
        executeDdl("create table test1 ( l long )", configuration);
    }

    @TearDown(Level.Iteration)
    public void tearDown() {
        executeDdl("drop table if exists test1", configuration);
    }

    @Benchmark
    @Fork(0)
    public void testInsertIntoSelectAtomic() {
        executeDdl("insert atomic into test1 select * from long_sequence(" + size + ") as l", configuration);
    }

    @Benchmark
    @Fork(0)
    public void testInsertIntoSelectBatched() {
        executeDdl("insert batch " + batchSize + "into test1 select * from long_sequence(" + size + ") as l", configuration);
    }

    private void executeDdl(String ddl, CairoConfiguration configuration) {
        try (CairoEngine engine = new CairoEngine(configuration)) {
            SqlExecutionContext sqlExecutionContext = new SqlExecutionContextImpl(engine, 1)
                    .with(
                            configuration.getFactoryProvider().getSecurityContextFactory().getRootContext(),
                            null,
                            null,
                            -1,
                            null
                    );
            try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
                compiler.compile(ddl, sqlExecutionContext);

            } catch (SqlException e) {
                e.printStackTrace();
            }
        }
    }
}
