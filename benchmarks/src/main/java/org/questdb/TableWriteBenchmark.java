/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
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
import io.questdb.cairo.security.AllowAllCairoSecurityContext;
import io.questdb.griffin.*;
import io.questdb.log.LogFactory;
import io.questdb.std.Rnd;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
public class TableWriteBenchmark {

    private static TableWriter writer;
    private static final CairoConfiguration configuration = new DefaultCairoConfiguration(".");
    private static QueryConstantsImpl queryConstants = new QueryConstantsImpl(configuration.getMicrosecondClock());

    private final Rnd rnd = new Rnd();

    public static void main(String[] args) throws RunnerException {
        try (CairoEngine engine = new CairoEngine(configuration)) {
            SqlExecutionContext sqlExecutionContext = new SqlExecutionContextImpl(engine, 1, null).with(
                    AllowAllCairoSecurityContext.INSTANCE,
                    null,
                    null,
                    -1,
                    null,
                    queryConstants);
            try (SqlCompiler compiler = new SqlCompiler(engine)) {
                compiler.compile("create table test1(f long)", sqlExecutionContext);
            } catch (SqlException e) {
                e.printStackTrace();
            }
        }
        Options opt = new OptionsBuilder()
                .include(TableWriteBenchmark.class.getSimpleName())
                .warmupIterations(5)
                .measurementIterations(5)
                .forks(1)
                .build();

        new Runner(opt).run();

        LogFactory.INSTANCE.haltThread();
    }

    @TearDown(Level.Iteration)
    public void tearDown() {
        System.out.println("writer size = " + writer.size());
        writer.commit();
        writer.truncate();
        writer.close();
    }

    @Setup(Level.Iteration)
    public void reset() {
        writer = new TableWriter(configuration, "test1");
        rnd.reset();
    }

    @Benchmark
    public void testRnd() {
        rnd.nextLong();
    }

    @Benchmark
    public void testWriteAsync() {
        TableWriter.Row r = writer.newRow();
        r.putLong(0, rnd.nextLong());
        r.append();
        writer.commit(CommitMode.ASYNC);
    }

    @Benchmark
    public void testWriteNoCommit() {
        TableWriter.Row r = writer.newRow();
        r.putLong(0, rnd.nextLong());
        r.append();
    }

    @Benchmark
    public void testWriteNoSync() {
        TableWriter.Row r = writer.newRow();
        r.putLong(0, rnd.nextLong());
        r.append();
        writer.commit(CommitMode.NOSYNC);
    }

    @Benchmark
    public void testWriteSync() {
        TableWriter.Row r = writer.newRow();
        r.putLong(0, rnd.nextLong());
        r.append();
        writer.commit(CommitMode.SYNC);
    }

}
