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
@OutputTimeUnit(TimeUnit.MICROSECONDS)
public class TableWriteBenchmark {

    private static TableWriter writer;
    private static TableWriter writer2;
    private static TableWriter writer3;
    private long ts;
    @Param({"NOSYNC", "SYNC", "ASYNC"})
    public WriterCommitMode writerCommitMode;

    private final Rnd rnd = new Rnd();

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(TableWriteBenchmark.class.getSimpleName())
                .warmupIterations(5)
                .measurementIterations(5)
                .forks(1)
                .build();

        new Runner(opt).run();
    }

    @Setup(Level.Iteration)
    public void setup() throws Exception {
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

        final CairoConfiguration configuration = new DefaultCairoConfiguration(".") {
            @Override
            public int getCommitMode() {
                return commitMode;
            }
        };

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
                compiler.compile("create table if not exists test1(f long) ", sqlExecutionContext);
                compiler.compile("create table if not exists test2(f timestamp) timestamp (f)", sqlExecutionContext);
                compiler.compile("create table if not exists test3(f timestamp) timestamp (f) PARTITION BY DAY", sqlExecutionContext);
            } catch (SqlException e) {
                e.printStackTrace();
            }
        }

        LogFactory.INSTANCE.haltThread();

        writer = new TableWriter(configuration, "test1");
        writer2 = new TableWriter(configuration, "test2");
        writer3 = new TableWriter(configuration, "test3");
        rnd.reset();
    }

    @TearDown(Level.Iteration)
    public void tearDown() {
        System.out.println("writer size = " + Math.max(writer.size(), writer2.size()));
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
    }

    @Benchmark
    public void testRnd() {
        rnd.nextLong();
    }

    @Benchmark
    public void testWrite() {
        TableWriter.Row r = writer.newRow();
        r.putLong(0, rnd.nextLong());
        r.append();
        writer.commit();
    }

    @Benchmark
    public void testWriteNoCommit() {
        TableWriter.Row r = writer.newRow();
        r.putLong(0, rnd.nextLong());
        r.append();
    }

    @Benchmark
    public void testWriteTimestamp() {
        TableWriter.Row r = writer2.newRow(ts++ << 8);
        r.append();
        writer2.commit();
    }

    @Benchmark
    public void testWriteTimestampNoCommit() {
        TableWriter.Row r = writer2.newRow(ts++ << 8);
        r.append();
    }

    @Benchmark
    public void testWritePartitionedTimestampNoCommit() {
        TableWriter.Row r = writer3.newRow(ts++ << 8);
        r.append();
    }

    @Benchmark
    public void testWritePartitionedTimestamp() {
        TableWriter.Row r = writer3.newRow(ts++ << 8);
        r.append();
        writer3.commit();
    }

    public enum WriterCommitMode {
        NOSYNC, SYNC, ASYNC
    }
}
