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

import io.questdb.MessageBusImpl;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CommitMode;
import io.questdb.cairo.DefaultCairoConfiguration;
import io.questdb.cairo.DefaultDdlListener;
import io.questdb.cairo.DefaultLifecycleManager;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableWriter;
import io.questdb.griffin.SqlCompilerImpl;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.log.LogFactory;
import io.questdb.std.Rnd;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
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
public class TableWriterBenchmark {
    // Should be set close enough to the cairo.max.uncommitted.rows default value.
    private static final int ROWS_PER_ITERATION = 1;
    private static final CairoConfiguration configuration = new DefaultCairoConfiguration(System.getProperty("java.io.tmpdir"));
    private static CairoEngine cairoEngine;
    private static TableWriter writer;
    private static TableWriter writer2;
    private static TableWriter writer3;
    private final Rnd rnd = new Rnd();
    @Param({"NOSYNC", "SYNC", "ASYNC"})
    public WriterCommitMode writerCommitMode;
    private long ts;

    public static void main(String[] args) throws RunnerException {
        cairoEngine = new CairoEngine(configuration);

        Options opt = new OptionsBuilder()
                .include(TableWriterBenchmark.class.getSimpleName())
                .warmupIterations(1)
                .measurementIterations(3)
                .forks(1)
                .build();

        new Runner(opt).run();
    }

    @Setup(Level.Iteration)
    public void setup() {
        final CairoConfiguration configuration = getConfiguration();

        executeDdl("create table if not exists test1(f long)", configuration);
        executeDdl("create table if not exists test2(f timestamp) timestamp (f)", configuration);
        executeDdl("create table if not exists test3(f timestamp) timestamp (f) PARTITION BY DAY", configuration);

        LogFactory.haltInstance();

        TableToken tableToken1 = new TableToken("test1", "test1", null, 0, false, false, false);
        TableToken tableToken2 = new TableToken("test2", "test2", null, 0, false, false, false);
        TableToken tableToken3 = new TableToken("test3", "test3", null, 0, false, false, false);

        writer = new TableWriter(
                configuration,
                tableToken1,
                null,
                new MessageBusImpl(configuration),
                true,
                DefaultLifecycleManager.INSTANCE,
                configuration.getDbRoot(),
                DefaultDdlListener.INSTANCE,
                cairoEngine
        );
        writer2 = new TableWriter(
                configuration,
                tableToken2,
                null,
                new MessageBusImpl(configuration),
                true,
                DefaultLifecycleManager.INSTANCE,
                configuration.getDbRoot(),
                DefaultDdlListener.INSTANCE,
                cairoEngine
        );
        writer3 = new TableWriter(
                configuration,
                tableToken3,
                null,
                new MessageBusImpl(configuration),
                true,
                DefaultLifecycleManager.INSTANCE,
                configuration.getDbRoot(),
                DefaultDdlListener.INSTANCE,
                cairoEngine
        );
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
                e.printStackTrace(System.out);
            }
        }
    }

    private CairoConfiguration getConfiguration() {
        final int commitMode = switch (writerCommitMode) {
            case NOSYNC -> CommitMode.NOSYNC;
            case SYNC -> CommitMode.SYNC;
            case ASYNC -> CommitMode.ASYNC;
        };

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
