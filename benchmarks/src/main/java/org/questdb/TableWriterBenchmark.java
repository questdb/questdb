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

import io.questdb.Metrics;
import io.questdb.cairo.*;
import io.questdb.cairo.security.AllowAllCairoSecurityContext;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlExecutionContextImpl;
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
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class TableWriterBenchmark {

    // Should be set close enough to the cairo.max.uncommitted.rows default value.
    private static final int ROWS_PER_ITERATION = 500_000;

    private static TableWriter writer;
    private static TableWriter writer2;
    private static TableWriter writer3;
    private long ts;
    @Param({"NOSYNC", "SYNC", "ASYNC"})
    public WriterCommitMode writerCommitMode;

    private final Rnd rnd = new Rnd();

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(TableWriterBenchmark.class.getSimpleName())
                .warmupIterations(1)
                .measurementIterations(3)
                .forks(1)
                .build();

        new Runner(opt).run();
    }

    @Setup(Level.Iteration)
    public void setup() throws Exception {
        final CairoConfiguration configuration = getConfiguration();

        executeDdl("create table if not exists test1(f long)", configuration);
        executeDdl("create table if not exists test2(f timestamp) timestamp (f)", configuration);
        executeDdl("create table if not exists test3(f timestamp) timestamp (f) PARTITION BY DAY", configuration);

        LogFactory.haltInstance();

        writer = new TableWriter(configuration, "test1", Metrics.disabled());
        writer2 = new TableWriter(configuration, "test2", Metrics.disabled());
        writer3 = new TableWriter(configuration, "test3", Metrics.disabled());
        rnd.reset();
    }

    @TearDown(Level.Iteration)
    public void tearDown() {
        writer.commit();
        writer.truncate();
        writer.close();
        writer2.commit();
        writer2.truncate();
        writer2.close();
        writer3.commit();
        writer3.truncate();
        writer3.close();

        ts = 0;

        final CairoConfiguration configuration = getConfiguration();
        executeDdl("drop table test1", configuration);
        executeDdl("drop table test2", configuration);
        executeDdl("drop table test3", configuration);
    }

    @Benchmark
    public void testWrite() {
        for (int i = 0; i < ROWS_PER_ITERATION; i++) {
            TableWriter.Row r = writer.newRow();
            r.putLong(0, rnd.nextLong());
            r.append();
        }
        writer.commit();
    }

    @Benchmark
    public void testWriteNoCommit() {
        for (int i = 0; i < ROWS_PER_ITERATION; i++) {
            TableWriter.Row r = writer.newRow();
            r.putLong(0, rnd.nextLong());
            r.append();
        }
    }

    @Benchmark
    public void testWriteTimestamp() {
        for (int i = 0; i < ROWS_PER_ITERATION; i++) {
            TableWriter.Row r = writer2.newRow(ts++ << 8);
            r.append();
        }
        writer2.commit();
    }

    @Benchmark
    public void testWriteTimestampNoCommit() {
        for (int i = 0; i < ROWS_PER_ITERATION; i++) {
            TableWriter.Row r = writer2.newRow(ts++ << 8);
            r.append();
        }
    }

    @Benchmark
    public void testWritePartitionedTimestampNoCommit() {
        for (int i = 0; i < ROWS_PER_ITERATION; i++) {
            TableWriter.Row r = writer3.newRow(ts++ << 8);
            r.append();
        }
    }

    @Benchmark
    public void testWritePartitionedTimestamp() {
        for (int i = 0; i < ROWS_PER_ITERATION; i++) {
            TableWriter.Row r = writer3.newRow(ts++ << 8);
            r.append();
        }
        writer3.commit();
    }

    private void executeDdl(String ddl, CairoConfiguration configuration) {
        try (CairoEngine engine = new CairoEngine(configuration)) {
            SqlExecutionContext sqlExecutionContext = new SqlExecutionContextImpl(engine, 1)
                    .with(
                            AllowAllCairoSecurityContext.INSTANCE,
                            null,
                            null,
                            -1,
                            null
                    );
            try (SqlCompiler compiler = new SqlCompiler(engine)) {
                compiler.compile(ddl, sqlExecutionContext);
            } catch (SqlException e) {
                e.printStackTrace();
            }
        }
    }

    private CairoConfiguration getConfiguration() {
        final int commitMode;
        switch (writerCommitMode) {
            case NOSYNC:
                commitMode = CommitMode.NOSYNC;
                break;
            case SYNC:
                commitMode = CommitMode.SYNC;
                break;
            case ASYNC:
                commitMode = CommitMode.ASYNC;
                break;
            default:
                throw new IllegalStateException("Unexpected commit mode: " + writerCommitMode);
        }

        return new DefaultCairoConfiguration(".") {
            @Override
            public int getCommitMode() {
                return commitMode;
            }
        };
    }

    public enum WriterCommitMode {
        NOSYNC, SYNC, ASYNC
    }
}
