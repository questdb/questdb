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
import io.questdb.cairo.lv.LiveViewInstance;
import io.questdb.cairo.lv.LiveViewRefreshJob;
import io.questdb.cairo.wal.ApplyWal2TableJob;
import io.questdb.cairo.wal.CheckWalTransactionsJob;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.datetime.microtime.Micros;
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
 * One out-of-order repair of a live view whose checkpoint ladder holds R roots above the late
 * row's anchor: the refresh that resumes from the anchor, replays every base row above it and
 * either keeps the ladder as a chain - re-imaging, at every root, each function a seal walks
 * whole - or truncates it and seals one head. A chain is kept only while the keys its roots
 * image whole stay within the larger of the resume's estimated base rows and
 * {@code cairo.live.view.checkpoint.repair.scan.max.keys}.
 * <p>
 * Each iteration builds a fresh database: K keys in one commit (the anchor), then R in-order
 * commits on one key, one root each ({@code checkpoint.rows = 1}). The measured call commits
 * the late rows and drives the refresh job to quiescence. The scenario reads
 * {@code view:keys:roots:late}:
 * <ul>
 *     <li>view - ROWS (sum over 2 preceding rows), RANGE24H (sum over 24 hours), RANGE1M (sum over
 *     one minute), TWO_ROWS (a ROWS sum and a ROWS avg), ANCHOR_RANK, ANCHOR_LAG, ANCHOR_EXCLUDE_AVG
 *     (avg over an EXCLUDE CURRENT ROW frame), ANCHOR_SUM (the one family whose chain stays
 *     incremental);</li>
 *     <li>late - ONE (one row on an existing key below the first root), NEW_BELOW / NEW_ACROSS /
 *     NEW_SPREAD (100,000 rows on keys the view never held, below the first root, across it, or
 *     spread over every root), OLD (100,000 rows on the existing keys), DENSE (one row, after
 *     in-order commits each as wide as the key domain, so the replay pays for the chain), JITTERY
 *     (100 commits, each 1,000 rows reaching 0.5 s below the previous frontier). The head-miss
 *     shapes put one row at the day's start, below every root and with no root under it, so the
 *     repair rebuilds the day and splices every root above the row: SEGMENT_START (the roots as
 *     above), SEGMENT_START_DENSE (the roots as DENSE's), CLOSED_SEGMENT (a row in the next day
 *     closes the repaired one first, so the repair converges below the frontier), and
 *     LATE_WITH_CURRENT (a row below the first root and one above the frontier in one commit, for
 *     a bounded RANGE view the late row localizes behind the end of the table).</li>
 * </ul>
 * <pre>
 * mvn -pl benchmarks -am package -o -DskipTests
 * java --enable-native-access=ALL-UNNAMED \
 *      --add-exports=java.base/jdk.internal.vm=ALL-UNNAMED \
 *      -cp benchmarks/target/benchmarks.jar org.questdb.LiveViewLateRowRepairBenchmark
 * </pre>
 */
@State(Scope.Thread)
@BenchmarkMode(Mode.SingleShotTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 2)
@Measurement(iterations = 5)
@Fork(value = 1, jvmArgsAppend = {"--add-exports=java.base/jdk.internal.vm=ALL-UNNAMED", "--enable-native-access=ALL-UNNAMED"})
public class LiveViewLateRowRepairBenchmark {
    // 2026-01-01T00:00:00Z
    private static final long DAY_ZERO_MICROS = 1_767_225_600_000_000L;
    private static final long LATE_ROWS = 100_000;
    @Param({
            "ROWS:100000:1:ONE",
            "ROWS:100000:13:ONE",
            "ROWS:100000:50:ONE",
            "RANGE24H:100000:13:ONE",
            "TWO_ROWS:100000:13:ONE",
            "ANCHOR_RANK:100000:13:ONE",
            "ANCHOR_LAG:100000:13:ONE",
            "ANCHOR_EXCLUDE_AVG:100000:13:ONE",
            "ANCHOR_SUM:100000:13:ONE",
            "ROWS:10000:10:ONE",
            "RANGE24H:10000:10:ONE",
            "ROWS:10000:10:DENSE",
            "ROWS:100002:1:JITTERY",
            "ROWS:1000:12:NEW_BELOW",
            "ROWS:1000:12:NEW_ACROSS",
            "ROWS:1000:12:NEW_SPREAD",
            "ROWS:1000:12:OLD",
            "ANCHOR_SUM:100000:13:SEGMENT_START",
            "ANCHOR_SUM:100000:49:SEGMENT_START",
            "ANCHOR_LAG:100000:13:SEGMENT_START",
            "ANCHOR_LAG:1000:120:SEGMENT_START_DENSE",
            "RANGE1M:100000:13:LATE_WITH_CURRENT",
            "ANCHOR_SUM:100000:13:CLOSED_SEGMENT"
    })
    public String scenario;
    private CairoEngine engine;
    private LiveViewInstance instance;
    private LiveViewRefreshJob job;
    private int keys;
    private String late;
    private Path root;
    private int roots;
    private SqlExecutionContext sqlCtx;

    public static void main(String[] args) throws RunnerException {
        final Options opt = new OptionsBuilder()
                .include(LiveViewLateRowRepairBenchmark.class.getSimpleName())
                .build();
        new Runner(opt).run();
    }

    @Benchmark
    public long lateRowRepair() throws SqlException {
        switch (late) {
            case "ONE", "DENSE" -> execute("INSERT INTO base VALUES (" + (firstRootTs() - 30 * Micros.SECOND_MICROS) + "::timestamp, 's2', 1.0)");
            case "SEGMENT_START", "SEGMENT_START_DENSE", "CLOSED_SEGMENT" -> execute("INSERT INTO base VALUES (" + DAY_ZERO_MICROS + "::timestamp, 's2', 1.0)");
            case "LATE_WITH_CURRENT" -> execute("INSERT INTO base VALUES (" + (firstRootTs() - 30 * Micros.SECOND_MICROS) + "::timestamp, 's2', 1.0), ("
                    + (rootTs(roots) + Micros.SECOND_MICROS) + "::timestamp, 's1', 1.0)");
            case "NEW_BELOW" -> execute("INSERT INTO base SELECT (" + (firstRootTs() - 30 * Micros.SECOND_MICROS) + " + x)::timestamp, ('n' || x)::symbol, 1.0 FROM long_sequence(" + LATE_ROWS + ")");
            case "NEW_ACROSS" -> execute("INSERT INTO base SELECT CASE WHEN x = 1 THEN (" + (firstRootTs() - 30 * Micros.SECOND_MICROS) + ")::timestamp"
                    + " ELSE (" + (firstRootTs() + 30 * Micros.SECOND_MICROS) + " + x)::timestamp END, ('n' || x)::symbol, 1.0 FROM long_sequence(" + LATE_ROWS + ")");
            case "NEW_SPREAD" -> {
                final long lo = firstRootTs() - 30 * Micros.SECOND_MICROS;
                final long step = (rootTs(roots) - Micros.SECOND_MICROS - lo) / LATE_ROWS;
                execute("INSERT INTO base SELECT (" + lo + " + x * " + step + ")::timestamp, ('n' || x)::symbol, 1.0 FROM long_sequence(" + LATE_ROWS + ")");
            }
            case "OLD" -> execute("INSERT INTO base SELECT (" + (firstRootTs() - 30 * Micros.SECOND_MICROS) + " + x)::timestamp,"
                    + " ('s' || (((x - 1) % " + keys + ") + 1))::symbol, 1.0 FROM long_sequence(" + LATE_ROWS + ")");
            case "JITTERY" -> {
                long frontier = rootTs(roots);
                for (int batch = 0; batch < 100; batch++) {
                    // 1,000 rows over the 1.5 s below the new frontier, half a second under the old one.
                    final long newFrontier = frontier + Micros.SECOND_MICROS;
                    execute("INSERT INTO base SELECT (" + (newFrontier - 1_500_000L) + " + x * 1_500)::timestamp,"
                            + " ('s' || (x % " + keys + " + 1))::symbol, 1.0 FROM long_sequence(1000)");
                    drainWal();
                    drainLiveView();
                    frontier = newFrontier;
                }
                return instance.getLvRowsTotal();
            }
            default -> throw new IllegalArgumentException("unknown late shape: " + late);
        }
        drainWal();
        drainLiveView();
        return instance.getLvRowsTotal();
    }

    @Setup(Level.Iteration)
    public void setUp() throws Exception {
        final String[] parts = scenario.split(":");
        final String view = parts[0];
        keys = Integer.parseInt(parts[1]);
        roots = Integer.parseInt(parts[2]);
        late = parts[3];
        root = Files.createTempDirectory("lv-late-row-repair-");
        final CairoConfiguration configuration = new DefaultCairoConfiguration(root.toString()) {
            @Override
            public long getLiveViewCheckpointMaxDurationMicros() {
                return 24 * Micros.HOUR_MICROS;
            }

            @Override
            public long getLiveViewCheckpointRows() {
                return 1;
            }

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
        execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL CAPACITY 262144 NOCACHE, x DOUBLE)"
                + " TIMESTAMP(ts) PARTITION BY DAY WAL DEDUP UPSERT KEYS(ts, sym)");
        execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM BEGINNING AS " + viewSql(view));
        instance = engine.getLiveViewRegistry().getViewInstance("lv");
        job = new LiveViewRefreshJob(0, engine, 1);
        drainLiveView();
        // The anchor: every key in one commit, the last at 01:00, a minute below the first root.
        execute("INSERT INTO base SELECT (" + (DAY_ZERO_MICROS + Micros.HOUR_MICROS - keys) + " + x)::timestamp,"
                + " ('s' || x)::symbol, 1.0 FROM long_sequence(" + keys + ")");
        drainWal();
        drainLiveView();
        // R roots above it, one in-order commit each; DENSE makes each as wide as the key domain.
        for (int i = 1; i <= roots; i++) {
            if ("DENSE".equals(late) || "SEGMENT_START_DENSE".equals(late)) {
                execute("INSERT INTO base SELECT (" + (rootTs(i) - keys - 1) + " + x)::timestamp,"
                        + " ('s' || ((x - 1) % " + keys + " + 1))::symbol, 1.0 FROM long_sequence(" + (keys + 1) + ")");
            } else {
                execute("INSERT INTO base VALUES (" + rootTs(i) + "::timestamp, 's1', 1.0)");
            }
            drainWal();
            drainLiveView();
        }
        if ("CLOSED_SEGMENT".equals(late)) {
            execute("INSERT INTO base VALUES (" + (DAY_ZERO_MICROS + Micros.DAY_MICROS + Micros.MINUTE_MICROS) + "::timestamp, 's1', 1.0)");
            drainWal();
            drainLiveView();
        }
    }

    @TearDown(Level.Iteration)
    public void tearDown() throws IOException {
        job = Misc.free(job);
        engine = Misc.free(engine);
        instance = null;
        deleteRecursively(root);
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

    // Root i sits at 01:00 + i minutes, above the anchor's last row at 01:00.
    private static long rootTs(int i) {
        return DAY_ZERO_MICROS + Micros.HOUR_MICROS + i * Micros.MINUTE_MICROS;
    }

    private static String viewSql(String view) {
        final String anchor = " WINDOW w AS (PARTITION BY sym ORDER BY ts ANCHOR DAILY '00:00')";
        return switch (view) {
            case "ROWS" -> "SELECT ts, sym, x, sum(x) OVER (PARTITION BY sym ORDER BY ts ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) s FROM base";
            case "RANGE24H" -> "SELECT ts, sym, x, sum(x) OVER (PARTITION BY sym ORDER BY ts RANGE BETWEEN '24' HOUR PRECEDING AND CURRENT ROW) s FROM base";
            case "RANGE1M" -> "SELECT ts, sym, x, sum(x) OVER (PARTITION BY sym ORDER BY ts RANGE BETWEEN '1' MINUTE PRECEDING AND CURRENT ROW) s FROM base";
            case "TWO_ROWS" -> "SELECT ts, sym, x, sum(x) OVER (PARTITION BY sym ORDER BY ts ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) s,"
                    + " avg(x) OVER (PARTITION BY sym ORDER BY ts ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) a FROM base";
            case "ANCHOR_RANK" -> "SELECT ts, sym, x, rank() OVER w r FROM base" + anchor;
            case "ANCHOR_LAG" -> "SELECT ts, sym, x, lag(x) OVER w p FROM base" + anchor;
            case "ANCHOR_EXCLUDE_AVG" -> "SELECT ts, sym, x, avg(x) OVER w a FROM base"
                    + " WINDOW w AS (PARTITION BY sym ORDER BY ts RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW EXCLUDE CURRENT ROW ANCHOR DAILY '00:00')";
            case "ANCHOR_SUM" -> "SELECT ts, sym, x, sum(x) OVER w s FROM base" + anchor;
            default -> throw new IllegalArgumentException("unknown view: " + view);
        };
    }

    private void drainLiveView() {
        // Force each batch across FLUSH EVERY: the cadence is a wall-clock policy, and waiting
        // it out would add idle time rather than measure the repair.
        instance.setLastFlushTimeUs(Numbers.LONG_NULL);
        for (int i = 0; i < 1_024; i++) {
            boolean progressed = false;
            while (job.run()) {
                progressed = true;
            }
            drainWal();
            if (!progressed) {
                return;
            }
            instance.setLastFlushTimeUs(Numbers.LONG_NULL);
        }
        throw new IllegalStateException("live view did not quiesce");
    }

    private void drainWal() {
        try (ApplyWal2TableJob walApplyJob = new ApplyWal2TableJob(engine, 0)) {
            while (walApplyJob.run()) {
                // drain
            }
            if (new CheckWalTransactionsJob(engine).run()) {
                while (walApplyJob.run()) {
                    // drain
                }
            }
        }
    }

    private void execute(String sql) throws SqlException {
        engine.execute(sql, sqlCtx);
    }

    private long firstRootTs() {
        return rootTs(1);
    }
}
