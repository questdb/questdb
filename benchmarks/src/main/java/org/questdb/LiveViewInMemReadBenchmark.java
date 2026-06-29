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
 * Quantifies the in-memory tier read benefit for a live view, gating the "keep
 * the tier on by default?" decision (RFC v2, open question 3) and reporting the
 * Mode A (lead-from-RAM) vs Mode B (disk subset) vs disk-only net.
 * <p>
 * A SYMBOL-free {@code row_number()} view (so {@code SELECT * FROM lv} routes
 * through the tier) is populated with {@code rows} ticks at a 1ms step. All three
 * read arms scan the identical {@code SELECT * FROM lv}; the {@code routing} param
 * decides what the cursor serves:
 * <ul>
 *   <li><b>modeA</b> - the tier leads disk: the last {@link #LEAD_ROWS} rows are
 *   refreshed into the tier but held un-flushed (the in-RAM lead), the rest are
 *   applied to disk. The read serves the lead and the overlap from RAM.</li>
 *   <li><b>modeB</b> - the whole view is flushed, so the tier is a strict subset
 *   of disk; the read serves the resident overlap from RAM (no lead).</li>
 *   <li><b>diskOnly</b> - the whole view is flushed and both slot stamps are
 *   mismatched so the seqTxn fence fails; the read serves everything from disk
 *   (the same forced-disk-only technique the differential-oracle tests use).</li>
 * </ul>
 * The {@code shape} param sets how much of the view is resident:
 * <ul>
 *   <li><b>whole</b> - {@code IN MEMORY 60m} keeps every row resident (seam = min
 *   ts), so the tier serves the entire scan from RAM. This is the upper bound of
 *   the benefit.</li>
 *   <li><b>seamSplit</b> - the {@code IN MEMORY} window covers only the recent
 *   half of the span, so the seam falls mid-view: the tier serves the recent half
 *   from RAM and disk serves the older half. This is the realistic shape - the
 *   tier arms still pay for the disk prefix, so the win over disk-only shrinks.</li>
 * </ul>
 * Honest caveats for reading the numbers: (1) modeA and modeB read cost is
 * essentially identical - a lead row and an overlap row take the same hot path,
 * so modeA's distinct value is freshness, not throughput; the arms differ only in
 * what is served, not how. (2) The disk-only arm reads from the OS page cache
 * once warm (JMH steady state), so the reported tier win is the conservative
 * warm-cache figure; a cold cache would widen it. (3) The seam skip - not the
 * lead - is the read-throughput lever, visible as {modeA, modeB} vs diskOnly.
 * <p>
 * Setup asserts each arm took the intended path. Reported as average wall time
 * per full scan.
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
public class LiveViewInMemReadBenchmark {

    // The trailing rows refreshed but held un-flushed (the in-RAM lead) in the
    // modeA arm. Small relative to `rows` and well inside the IN MEMORY window in
    // either shape, so it is always resident.
    private static final int LEAD_ROWS = 5_000;
    // 1ms step. In the `whole` shape `rows` ticks stay inside the 60-minute IN
    // MEMORY window (1M rows span ~16.7 min) so the whole view is resident; the
    // `seamSplit` shape sizes its window to the recent half of the span instead.
    private static final long STEP_MICROS = 1_000L;

    @Param({"100000", "1000000"})
    public int rows;

    @Param({"modeA", "modeB", "diskOnly"})
    public String routing;

    @Param({"whole", "seamSplit"})
    public String shape;

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
        // projection - the precondition for tier routing. The `whole` shape keeps
        // every row resident; `seamSplit` sizes the window to the recent half of
        // the span so the seam falls mid-view.
        final boolean modeA = "modeA".equals(routing);
        final boolean seamSplit = "seamSplit".equals(shape);
        final long spanSeconds = Math.max(1L, (long) rows * STEP_MICROS / 1_000_000L);
        final String inMemory = seamSplit ? "in memory " + Math.max(1L, spanSeconds / 2) + "s " : "in memory 60m ";
        engine.execute(
                "create live view lv flush every 100ms " + inMemory + "as "
                        + "select ts, i, row_number() over () as rn from base",
                sqlCtx
        );

        // Data starts 30 days in the future so every row sits above the
        // non-backfill view's CREATE-moment lower bound (the wall clock now). In
        // modeA the trailing LEAD_ROWS rows are held back to be the un-flushed
        // lead; the rest are applied to disk first.
        final long baseTs = System.currentTimeMillis() * 1000L + 30L * 86_400L * 1_000_000L;
        final int appliedRows = modeA ? rows - LEAD_ROWS : rows;
        engine.execute(
                "insert into base select (" + baseTs + " + (x - 1) * " + STEP_MICROS + ")::timestamp, x "
                        + "from long_sequence(" + appliedRows + ")",
                sqlCtx
        );
        drainWal(engine);

        final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
        try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
            // Materialise the applied portion and populate the tier, clearing the
            // FLUSH EVERY gate each pass so the flush is not deferred.
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

            if (modeA) {
                // Pin the flush clock far in the future so the next refresh
                // publishes the trailing rows into the tier as the un-flushed lead
                // (the tier leads disk) without crossing FLUSH EVERY.
                instance.setLastFlushTimeUs(engine.getConfiguration().getMicrosecondClock().getTicks() + 3_600_000_000L);
                engine.execute(
                        "insert into base select (" + baseTs + " + (" + appliedRows + " + x - 1) * " + STEP_MICROS + ")::timestamp, "
                                + appliedRows + " + x from long_sequence(" + LEAD_ROWS + ")",
                        sqlCtx
                );
                drainWal(engine);
                //noinspection StatementWithEmptyBody
                while (job.run()) ; // refresh only -> lead in RAM, no flush
                drainWal(engine);
            }
        }

        final LiveViewInMemoryTier tier = instance.getInMemoryTier();
        if (tier == null) {
            throw new IllegalStateException("in-mem tier was not allocated");
        }
        final LiveViewInMemoryBuffer slot = tier.getSlot(tier.getPublishedIdx());
        // In the `whole` shape every row is resident; in `seamSplit` only the
        // recent window is, so just require a populated slot there.
        if (!seamSplit && slot.rowCount() != rows) {
            throw new IllegalStateException("tier not fully populated: expected " + rows + ", got " + slot.rowCount());
        }
        if (seamSplit && slot.rowCount() <= 0) {
            throw new IllegalStateException("seam-split tier was not populated");
        }
        if (modeA && slot.leadRowCount() != LEAD_ROWS) {
            throw new IllegalStateException("expected an un-flushed lead of " + LEAD_ROWS + ", got " + slot.leadRowCount());
        }
        if (!modeA && slot.leadRowCount() != 0) {
            throw new IllegalStateException("expected no lead, got " + slot.leadRowCount());
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

    // Opens the inner LiveViewRecordCursor once, drains it, and verifies the
    // routing matches the param, so a silently mis-routed arm (e.g. a future
    // change that breaks the fence) surfaces loudly instead of reporting a phantom
    // win or loss. For modeA it also confirms the cursor actually served the lead.
    private void assertRouting() throws Exception {
        RecordCursorFactory f = factory;
        while (f != null && !(f instanceof LiveViewRecordCursorFactory)) {
            f = f.getBaseFactory();
        }
        if (f == null) {
            throw new IllegalStateException("expected a LiveViewRecordCursorFactory in the plan");
        }
        try (LiveViewRecordCursor cursor = (LiveViewRecordCursor) f.getCursor(sqlCtx)) {
            final boolean expectedTier = !"diskOnly".equals(routing);
            if (cursor.isRoutingEligible() != expectedTier) {
                throw new IllegalStateException(
                        "routing mismatch: expected tier=" + expectedTier + ", got " + cursor.isRoutingEligible());
            }
            //noinspection StatementWithEmptyBody
            while (cursor.hasNext()) {
            }
            if ("modeA".equals(routing) && cursor.leadRowsServed() != LEAD_ROWS) {
                throw new IllegalStateException(
                        "modeA must serve the lead from RAM: expected " + LEAD_ROWS + ", got " + cursor.leadRowsServed());
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
