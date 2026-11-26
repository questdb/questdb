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
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableWriter;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.log.LogFactory;
import io.questdb.std.Files;
import io.questdb.std.Rnd;
import io.questdb.std.str.Path;
import io.questdb.std.str.Utf8String;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.concurrent.TimeUnit;

/**
 * Appends to a non-WAL table through {@link TableWriter}: one fixed-size column (LONG), one
 * var-size column (VARCHAR) and the designated timestamp. Every 10th row leaves the LONG unset and
 * every 7th leaves the VARCHAR unset, so the writer's NULL appenders run too.
 * <ul>
 *     <li>{@link #inOrder}: a batch of rows after the last timestamp, then a commit.</li>
 *     <li>{@link #outOfOrder}: a batch in order, committed, then a batch whose timestamps fall
 *     inside the first batch's range, committed, which merges out of order.</li>
 * </ul>
 * Each iteration starts from an empty table and runs a fixed number of batches (single shot), so
 * every build does the same work.
 */
@State(Scope.Thread)
@BenchmarkMode(Mode.SingleShotTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 3, batchSize = TableWriterAppendBenchmark.BATCHES)
@Measurement(iterations = 10, batchSize = TableWriterAppendBenchmark.BATCHES)
public class TableWriterAppendBenchmark {
    static final int BATCHES = 200;
    private static final int ROWS_PER_BATCH = 1_000;
    private static final long ROW_STEP_MICROS = 1_000L;
    private final Rnd rnd = new Rnd();
    private final Utf8String[] strings = new Utf8String[16];
    private CairoEngine engine;
    private long nextTs;
    private String root;
    private TableWriter writer;

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(TableWriterAppendBenchmark.class.getSimpleName())
                .forks(1)
                .build();
        new Runner(opt).run();
    }

    @Benchmark
    public void inOrder() {
        appendBatch(nextTs, ROW_STEP_MICROS);
        nextTs += ROWS_PER_BATCH * ROW_STEP_MICROS;
        writer.commit();
    }

    @Benchmark
    public void outOfOrder() {
        final long lo = nextTs;
        appendBatch(lo, ROW_STEP_MICROS);
        writer.commit();
        // the second batch lands between the first batch's rows
        appendBatch(lo + ROW_STEP_MICROS / 2, ROW_STEP_MICROS);
        writer.commit();
        nextTs += ROWS_PER_BATCH * ROW_STEP_MICROS;
    }

    @Setup(Level.Iteration)
    public void setUp() throws Exception {
        LogFactory.haltInstance();
        root = System.getProperty("java.io.tmpdir") + Files.SEPARATOR + "table-writer-append-bench-" + System.nanoTime();
        try (Path path = new Path()) {
            Files.mkdirs(path.of(root).slash(), 509);
        }
        final CairoConfiguration configuration = new DefaultCairoConfiguration(root);
        engine = new CairoEngine(configuration);
        try (SqlExecutionContextImpl ctx = new SqlExecutionContextImpl(engine, 1)) {
            ctx.with(configuration.getFactoryProvider().getSecurityContextFactory().getRootContext(), null, null, -1, null);
            engine.execute("CREATE TABLE t (l LONG, v VARCHAR, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL", ctx);
        }
        final TableToken token = engine.verifyTableName("t");
        writer = engine.getWriter(token, "bench");
        for (int i = 0; i < strings.length; i++) {
            strings[i] = new Utf8String("value-" + i + "-" + "x".repeat(i));
        }
        rnd.reset();
        nextTs = 1_700_000_000_000_000L;
    }

    @TearDown(Level.Iteration)
    public void tearDown() {
        writer.close();
        engine.close();
        try (Path path = new Path()) {
            Files.rmdir(path.of(root).slash(), true);
        }
    }

    private void appendBatch(long ts, long step) {
        for (int i = 0; i < ROWS_PER_BATCH; i++) {
            final TableWriter.Row row = writer.newRow(ts + i * step);
            if (i % 10 != 0) {
                row.putLong(0, rnd.nextLong());
            }
            if (i % 7 != 0) {
                row.putVarchar(1, strings[i & 15]);
            }
            row.append();
        }
    }
}
