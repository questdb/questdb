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

import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.DefaultCairoConfiguration;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.mp.WorkerPool;
import io.questdb.mp.WorkerPoolUtils;
import io.questdb.std.Misc;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.concurrent.TimeUnit;

/**
 * Times the first execution of a parallel hash GROUP BY: a fresh factory has no key count from an earlier run,
 * so its merge destinations start small and the merges grow them.
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.SingleShotTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 10)
@Measurement(iterations = 10)
@Fork(2)
public class GroupByManyKeysBenchmark {
    @Param({"2000000"})
    public int keyCount;
    @Param({"4"})
    public int workers;
    private SqlCompiler compiler;
    private SqlExecutionContextImpl context;
    private CairoEngine engine;
    private RecordCursorFactory factory;
    private Path root;
    private WorkerPool workerPool;

    @Benchmark
    public void firstExecution(Blackhole blackhole) throws Exception {
        try (RecordCursor cursor = factory.getCursor(context)) {
            final Record record = cursor.getRecord();
            while (cursor.hasNext()) {
                blackhole.consume(record.getLong(0));
            }
        }
    }

    @Setup(Level.Invocation)
    public void compile() throws Exception {
        factory = compiler.compile("select id, count() from t", context).getRecordCursorFactory();
    }

    @TearDown(Level.Invocation)
    public void freeFactory() {
        factory = Misc.free(factory);
    }

    @Setup
    public void setUp() throws Exception {
        root = Files.createTempDirectory("group-by-many-keys-");
        final DefaultCairoConfiguration configuration = new DefaultCairoConfiguration(root.toString());
        engine = new CairoEngine(configuration);
        workerPool = new WorkerPool(() -> workers);
        WorkerPoolUtils.setupQueryJobs(workerPool, engine);
        workerPool.start();
        context = new SqlExecutionContextImpl(engine, workers)
                .with(configuration.getFactoryProvider().getSecurityContextFactory().getRootContext());
        compiler = engine.getSqlCompiler();
        engine.execute("create table t as (select x id, timestamp_sequence(0, 1000L) ts from long_sequence(" + keyCount + ")) "
                + "timestamp(ts) partition by day", context);
    }

    @TearDown
    public void tearDown() throws Exception {
        workerPool.halt();
        compiler = Misc.free(compiler);
        context = Misc.free(context);
        engine = Misc.free(engine);
        try (var paths = Files.walk(root)) {
            for (Path path : paths.sorted(Comparator.reverseOrder()).toList()) {
                Files.deleteIfExists(path);
            }
        }
    }
}
