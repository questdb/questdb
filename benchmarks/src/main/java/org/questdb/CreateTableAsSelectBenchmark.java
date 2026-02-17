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

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.DefaultCairoConfiguration;
import io.questdb.griffin.SqlCompilerImpl;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlExecutionContextImpl;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
public class CreateTableAsSelectBenchmark {
    private final CairoConfiguration configuration = new DefaultCairoConfiguration(".");
    @Param({"8192", "16384", "32786", "131072"})
    public String batchSize;
    @Param({"10000", "1000000", "100000000", "1000000000"})
    public String size;

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(CreateTableAsSelectBenchmark.class.getSimpleName())
                .warmupIterations(1)
                .warmupBatchSize(1)
                .measurementBatchSize(1)
                .measurementIterations(3)
                .operationsPerInvocation(1)
                .forks(0)
                .build();

        new Runner(opt).run();
    }

    @TearDown(Level.Iteration)
    public void tearDown() {
        executeDdl("drop table test1", configuration);
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    public void testCreateAsSelectAtomic() {
        executeDdl("create atomic table if not exists test1 as ( select x::long as l, x::timestamp as ts from long_sequence(" + size + ") as x ) timestamp(ts) partition by none bypass wal", configuration);
        executeDdl("select count(*) from test1", configuration);
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    public void testCreateAsSelectBatched() {
        executeDdl("create batch " + batchSize + " table if not exists test1 as ( select x::long as l, x::timestamp as ts from long_sequence(" + size + ") as x ) timestamp(ts) partition by none bypass wal", configuration);
        executeDdl("select count(*) from test1", configuration);
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
