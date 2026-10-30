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

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CommitMode;
import io.questdb.cairo.DefaultCairoConfiguration;
import io.questdb.cairo.TableWriter;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.log.LogFactory;
import io.questdb.std.Rnd;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
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
@Fork(value = 1, jvmArgsAppend = "--add-exports=java.base/jdk.internal.vm=ALL-UNNAMED")
public class TableWriterBenchmark {
    // Should be set close enough to the cairo.max.uncommitted.rows default value.
    private static final int ROWS_PER_ITERATION = 1;
    private final Rnd rnd = new Rnd();
    @Param({"NOSYNC", "SYNC", "ASYNC"})
    public WriterCommitMode writerCommitMode;
    private CairoEngine engine;
    private SqlExecutionContext sqlExecutionContext;
    private long ts;
    private TableWriter writer;
    private TableWriter writer2;
    private TableWriter writer3;

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
    public void setup() throws SqlException {
        final CairoConfiguration configuration = getConfiguration();
        engine = new CairoEngine(configuration);
        sqlExecutionContext = new SqlExecutionContextImpl(engine, 1)
                .with(
                        configuration.getFactoryProvider().getSecurityContextFactory().getRootContext(),
                        null,
                        null,
                        -1,
                        null
                );

        engine.execute("DROP TABLE IF EXISTS test1", sqlExecutionContext);
        engine.execute("DROP TABLE IF EXISTS test2", sqlExecutionContext);
        engine.execute("DROP TABLE IF EXISTS test3", sqlExecutionContext);
        engine.execute("CREATE TABLE test1 (f LONG)", sqlExecutionContext);
        engine.execute("CREATE TABLE test2 (f TIMESTAMP) TIMESTAMP(f)", sqlExecutionContext);
        engine.execute("CREATE TABLE test3 (f TIMESTAMP) TIMESTAMP(f) PARTITION BY DAY BYPASS WAL", sqlExecutionContext);

        LogFactory.haltInstance();

        writer = engine.getWriter(engine.verifyTableName("test1"), "benchmark");
        writer2 = engine.getWriter(engine.verifyTableName("test2"), "benchmark");
        writer3 = engine.getWriter(engine.verifyTableName("test3"), "benchmark");
        rnd.reset();
    }

    @TearDown(Level.Iteration)
    public void tearDown() throws SqlException {
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

        engine.execute("DROP TABLE test1", sqlExecutionContext);
        engine.execute("DROP TABLE test2", sqlExecutionContext);
        engine.execute("DROP TABLE test3", sqlExecutionContext);
        engine.close();
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
    public void testWritePartitionedTimestamp() {
        for (int i = 0; i < ROWS_PER_ITERATION; i++) {
            TableWriter.Row r = writer3.newRow(ts++ << 8);
            r.append();
        }
        writer3.commit();
    }

    @Benchmark
    public void testWritePartitionedTimestampNoCommit() {
        for (int i = 0; i < ROWS_PER_ITERATION; i++) {
            TableWriter.Row r = writer3.newRow(ts++ << 8);
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

    private CairoConfiguration getConfiguration() {
        final int commitMode = switch (writerCommitMode) {
            case NOSYNC -> CommitMode.NOSYNC;
            case SYNC -> CommitMode.SYNC;
            case ASYNC -> CommitMode.ASYNC;
        };

        return new DefaultCairoConfiguration(System.getProperty("java.io.tmpdir")) {
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
