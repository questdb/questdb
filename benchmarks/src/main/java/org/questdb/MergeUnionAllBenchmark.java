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
import io.questdb.cairo.DefaultCairoConfiguration;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlCompilerImpl;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.log.LogFactory;
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
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class MergeUnionAllBenchmark {

    private static final CairoConfiguration configuration = new DefaultCairoConfiguration(System.getProperty("java.io.tmpdir"));
    @Param({"100000", "1000000", "5000000"})
    public int rowsPerBranch;
    private RecordCursorFactory cachedWindowFactory;
    private RecordCursorFactory cachedWindowLimitFactory;
    private SqlCompilerImpl compiler;
    private SqlExecutionContextImpl ctx;
    private CairoEngine engine;
    private RecordCursorFactory mergeFactory;
    private RecordCursorFactory mergeLimitFactory;
    private RecordCursorFactory sortFactory;
    private RecordCursorFactory sortLimitFactory;
    private RecordCursorFactory windowFactory;
    private RecordCursorFactory windowLimitFactory;

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(MergeUnionAllBenchmark.class.getSimpleName())
                .warmupIterations(3)
                .measurementIterations(5)
                .forks(1)
                .build();
        new Runner(opt).run();
        LogFactory.haltInstance();
    }

    @Benchmark
    public long cachedWindowFullScan() throws SqlException {
        return drain(cachedWindowFactory);
    }

    @Benchmark
    public long cachedWindowLimit10() throws SqlException {
        return drain(cachedWindowLimitFactory);
    }

    @Benchmark
    public long mergeFullScan() throws SqlException {
        return drain(mergeFactory);
    }

    @Benchmark
    public long mergeLimit10() throws SqlException {
        return drain(mergeLimitFactory);
    }

    @Setup(Level.Trial)
    public void setup() throws Exception {
        engine = new CairoEngine(configuration);
        ctx = new SqlExecutionContextImpl(engine, 1).with(
                configuration.getFactoryProvider().getSecurityContextFactory().getRootContext(),
                null,
                null,
                -1,
                null
        );
        compiler = new SqlCompilerImpl(engine);

        engine.execute("drop table if exists a", ctx);
        engine.execute("drop table if exists b", ctx);
        engine.execute("drop table if exists b_ns", ctx);
        // interleave the branches' timestamps (a: even, b: odd) so the merge alternates legs every row.
        engine.execute("create table a as (select" +
                " rnd_long() i, timestamp_sequence(0, 2) ts" +
                " from long_sequence(" + rowsPerBranch + ")) timestamp(ts)", ctx);
        engine.execute("create table b as (select" +
                " rnd_long() i, timestamp_sequence(1, 2) ts" +
                " from long_sequence(" + rowsPerBranch + ")) timestamp(ts)", ctx);
        // b_ns is b with a nanosecond designated timestamp. Unioning it with a (microseconds) mismatches
        // the branch timestamp precision, so the merge declines and the window buffers + sorts instead.
        engine.execute("create table b_ns as (select i, ts::timestamp_ns ts from b) timestamp(ts)", ctx);

        // ORDER BY ts -> Union All Merge (streaming)
        // ORDER BY ts, i -> Union All + Encode sort.
        final String union = "(select * from a) union all (select * from b)";
        mergeFactory = compiler.compile(union + " order by ts", ctx).getRecordCursorFactory();
        sortFactory = compiler.compile(union + " order by ts, i", ctx).getRecordCursorFactory();
        mergeLimitFactory = compiler.compile(union + " order by ts limit 10", ctx).getRecordCursorFactory();
        sortLimitFactory = compiler.compile(union + " order by ts, i limit 10", ctx).getRecordCursorFactory();

        // Windowed OVER(ORDER BY ts): over the mergeable union it streams (Window); over the mismatched
        // union the merge declines, so it buffers every row and sorts (CachedWindow)
        final String streamWindow = "select ts, first_value(i) over (order by ts) from (" + union + ")";
        final String cachedWindow = "select ts, first_value(i) over (order by ts) from ((select * from a) union all (select * from b_ns))";
        windowFactory = compiler.compile(streamWindow, ctx).getRecordCursorFactory();
        cachedWindowFactory = compiler.compile(cachedWindow, ctx).getRecordCursorFactory();
        windowLimitFactory = compiler.compile(streamWindow + " limit 10", ctx).getRecordCursorFactory();
        cachedWindowLimitFactory = compiler.compile(cachedWindow + " limit 10", ctx).getRecordCursorFactory();
    }

    @Benchmark
    public long sortFullScan() throws SqlException {
        return drain(sortFactory);
    }

    @Benchmark
    public long sortLimit10() throws SqlException {
        return drain(sortLimitFactory);
    }

    @TearDown(Level.Trial)
    public void tearDown() {
        cachedWindowFactory.close();
        cachedWindowLimitFactory.close();
        mergeFactory.close();
        sortFactory.close();
        mergeLimitFactory.close();
        sortLimitFactory.close();
        windowFactory.close();
        windowLimitFactory.close();
        compiler.close();
        engine.close();
    }

    @Benchmark
    public long windowFullScan() throws SqlException {
        return drain(windowFactory);
    }

    @Benchmark
    public long windowLimit10() throws SqlException {
        return drain(windowLimitFactory);
    }

    private long drain(RecordCursorFactory factory) throws SqlException {
        long count = 0;
        try (RecordCursor cursor = factory.getCursor(ctx)) {
            while (cursor.hasNext()) {
                count++;
            }
        }
        return count;
    }
}
