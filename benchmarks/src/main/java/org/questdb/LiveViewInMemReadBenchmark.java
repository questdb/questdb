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
import io.questdb.cairo.lv.LiveViewInMemoryBuffer;
import io.questdb.cairo.lv.LiveViewInMemoryTier;
import io.questdb.cairo.lv.LiveViewInstance;
import io.questdb.cairo.lv.LiveViewRefreshJob;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.wal.ApplyWal2TableJob;
import io.questdb.cairo.wal.CheckWalTransactionsJob;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.griffin.engine.lv.LiveViewRecordCursor;
import io.questdb.griffin.engine.lv.LiveViewRecordCursorFactory;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
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

import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.concurrent.TimeUnit;

/**
 * Quantifies the in-memory tier (Mode B) read benefit for a live view, gating
 * the "keep Mode B on by default?" decision (RFC v2, open question 3).
 * <p>
 * A SYMBOL-free {@code row_number()} view (so {@code SELECT * FROM lv} routes
 * through the tier) is populated with {@code rows} ticks at a 1ms step, all
 * inside the {@code IN MEMORY 60m} window - so the whole view is resident and
 * Mode B serves the entire scan from RAM (seam = min ts). This is the upper
 * bound of the benefit: a real query reads only its recent suffix from RAM and
 * the older prefix from disk, and the recent disk partition is often already in
 * the page cache. If even this best case is within noise, defaulting Mode B off
 * for V1 is justified.
 * <p>
 * The two arms read the identical {@code SELECT * FROM lv}; the {@code routing}
 * param decides whether the cursor takes Mode B or the disk-only fallback. The
 * disk-only arm mismatches both slot stamps so the seqTxn fence fails - the same
 * forced-disk-only technique the differential-oracle tests use - so both arms
 * exercise the production read path with the only difference being the routing.
 * Setup asserts each arm actually took the intended path.
 * <p>
 * Numbers get filled in by the first run; see the PR description for the
 * baseline. Reported as average wall time per full scan.
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
public class LiveViewInMemReadBenchmark {

    // 1ms step keeps `rows` ticks inside the default 60-minute IN MEMORY window
    // (1M rows span ~16.7 min), so the whole view stays resident in the tier.
    private static final long STEP_MICROS = 1_000L;

    @Param({"100000", "1000000"})
    public int rows;

    @Param({"modeB", "diskOnly"})
    public String routing;

    private Path dbRoot;
    private CairoEngine engine;
    private RecordCursorFactory factory;
    private SqlExecutionContext sqlCtx;

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(LiveViewInMemReadBenchmark.class.getSimpleName())
                .warmupIterations(3)
                .measurementIterations(5)
                .forks(1)
                .build();
        new Runner(opt).run();
    }

    @Setup(Level.Trial)
    public void setup() throws Exception {
        dbRoot = Files.createTempDirectory("lvInMemReadBench-");
        final CairoConfiguration configuration = new DefaultCairoConfiguration(dbRoot.toString()) {
            @Override
            public boolean isDevModeEnabled() {
                return true;
            }
        };
        engine = new CairoEngine(configuration);
        engine.load();
        sqlCtx = new SqlExecutionContextImpl(engine, 1).with(
                configuration.getFactoryProvider().getSecurityContextFactory().getRootContext(),
                null, null, -1, null
        );

        engine.execute("create table base (ts timestamp, i long) timestamp(ts) partition by DAY WAL", sqlCtx);
        // SYMBOL-free output so SELECT * FROM lv is a full-schema fixed-width
        // projection - the precondition for Mode B routing.
        engine.execute(
                "create live view lv flush every 100ms in memory 60m as "
                        + "select ts, i, row_number() over () as rn from base",
                sqlCtx
        );

        // Data starts 30 days in the future so every row sits above the
        // non-backfill view's CREATE-moment lower bound (the wall clock now).
        final long baseTs = System.currentTimeMillis() * 1000L + 30L * 86_400L * 1_000_000L;
        engine.execute(
                "insert into base select (" + baseTs + " + (x - 1) * " + STEP_MICROS + ")::timestamp, x "
                        + "from long_sequence(" + rows + ")",
                sqlCtx
        );
        drainWal(engine);

        // Drive one refresh cycle to materialise the view and populate the tier,
        // clearing the FLUSH EVERY gate so the flush is not deferred.
        final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
        try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
            for (int i = 0; i < 64; i++) {
                instance.setLastFlushTimeUs(Numbers.LONG_NULL);
                boolean progressed = false;
                //noinspection StatementWithEmptyBody
                while (job.run()) {
                    progressed = true;
                }
                drainWal(engine);
                if (!progressed) {
                    break;
                }
            }
        }

        final LiveViewInMemoryTier tier = instance.getInMemoryTier();
        if (tier == null) {
            throw new IllegalStateException("in-mem tier was not allocated");
        }
        final LiveViewInMemoryBuffer slot = tier.getSlot(tier.getPublishedIdx());
        if (slot.rowCount() != rows) {
            throw new IllegalStateException("tier not fully populated: expected " + rows + ", got " + slot.rowCount());
        }

        if ("diskOnly".equals(routing)) {
            // Mismatch both slot stamps so the seqTxn fence never engages: the
            // cursor falls back to the disk-only path on every getCursor.
            tier.getSlot(0).setLvSeqTxn(tier.getSlot(0).lvSeqTxn() + 1_000_000L);
            tier.getSlot(1).setLvSeqTxn(tier.getSlot(1).lvSeqTxn() + 1_000_000L);
        }

        // Cache the compiled plan so the timed method measures the read, not the
        // compile. The factory is independent of the compiler, so return the
        // pooled compiler immediately (otherwise it is left behind on engine
        // shutdown). Assert the arm actually took the intended routing.
        try (SqlCompiler compiler = engine.getSqlCompiler()) {
            factory = compiler.compile("SELECT * FROM lv", sqlCtx).getRecordCursorFactory();
        }
        assertRouting();
    }

    @TearDown(Level.Trial)
    public void tearDown() throws Exception {
        // Do not halt the shared log instance here: with multiple trials (one per
        // param combo) a later trial's setup would log against a stopped worker.
        factory = Misc.free(factory);
        engine = Misc.free(engine);
        deleteRecursively(dbRoot);
    }

    @Benchmark
    @Fork(1)
    public long readView() throws Exception {
        long sum = 0;
        try (RecordCursor cursor = factory.getCursor(sqlCtx)) {
            final Record record = cursor.getRecord();
            while (cursor.hasNext()) {
                sum += record.getLong(1); // i
            }
        }
        return sum;
    }

    // Opens the inner LiveViewRecordCursor once and verifies the routing matches
    // the param, so a silently mis-routed arm (e.g. a future change that breaks
    // the fence) surfaces loudly instead of reporting a phantom win or loss.
    private void assertRouting() throws Exception {
        RecordCursorFactory f = factory;
        while (f != null && !(f instanceof LiveViewRecordCursorFactory)) {
            f = f.getBaseFactory();
        }
        if (f == null) {
            throw new IllegalStateException("expected a LiveViewRecordCursorFactory in the plan");
        }
        try (LiveViewRecordCursor cursor = (LiveViewRecordCursor) f.getCursor(sqlCtx)) {
            final boolean expectedModeB = "modeB".equals(routing);
            if (cursor.isRoutingEligible() != expectedModeB) {
                throw new IllegalStateException(
                        "routing mismatch: expected modeB=" + expectedModeB + ", got " + cursor.isRoutingEligible());
            }
        }
    }

    private static void deleteRecursively(Path dir) throws IOException {
        if (dir == null || !Files.exists(dir)) {
            return;
        }
        Files.walkFileTree(dir, new SimpleFileVisitor<>() {
            @Override
            public FileVisitResult postVisitDirectory(Path d, IOException exc) throws IOException {
                Files.delete(d);
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                Files.delete(file);
                return FileVisitResult.CONTINUE;
            }
        });
    }

    private static void drainWal(CairoEngine engine) {
        try (ApplyWal2TableJob walApplyJob = new ApplyWal2TableJob(engine, 0)) {
            //noinspection StatementWithEmptyBody
            while (walApplyJob.run()) ;
            if (new CheckWalTransactionsJob(engine).run()) {
                //noinspection StatementWithEmptyBody
                while (walApplyJob.run()) ;
            }
        }
    }
}
