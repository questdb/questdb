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

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.DefaultCairoConfiguration;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.view.ViewCompilerJob;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.log.LogFactory;
import io.questdb.mp.WorkerPool;
import io.questdb.mp.WorkerPoolConfiguration;
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
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.CommandLineOptions;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.concurrent.TimeUnit;

/**
 * Reproduces six optimiser gaps hit when per-symbol views over one table (the "one view per
 * permitted symbol" pattern) are combined with UNION ALL or JOIN. Each {@code issue} runs twice:
 * <ul>
 *   <li>{@code arm=views}: the query as a user of the views writes it.</li>
 *   <li>{@code arm=rewrite}: a hand-written equivalent over the base table, i.e. the plan an
 *       optimiser that closed the gap could produce. views / rewrite is the cost of the gap.</li>
 * </ul>
 * Issues:
 * <ol>
 *   <li>I1 / I1b: UNION ALL of disjoint same-table views scans the table once per branch
 *       (rewrite: one scan with {@code sym IN (..)}); I1 streams the rows, I1b aggregates them.</li>
 *   <li>I2a: SAMPLE BY directly over the union (now served by the timestamp-ordered merge;
 *       it used to fail without ORDER BY + TIMESTAMP(ts)); I2b: LATEST ON falls
 *       back to the generic LatestBy over both full scans.</li>
 *   <li>I3a / I3b: a {@code ts} interval on the left side does not reach the right side through
 *       {@code a.ts = b.ts} (hash join) or ASOF, so the right side scans the whole table.</li>
 *   <li>I4: a join on {@code sym} between {@code sym='A'} and {@code sym='B'} can never match,
 *       yet both sides are scanned and hashed (rewrite: empty result).</li>
 *   <li>I5: a pair self-join reads the table twice and hashes one side (rewrite: one scan pivoted
 *       by symbol with GROUP BY ts).</li>
 *   <li>I6a / I6b: ASOF with the union as master / slave.</li>
 * </ol>
 * {@code table=t} has no index on sym; {@code table=ti} has {@code INDEX} on sym. The data has one row
 * per symbol per timestamp (8 symbols, S0..S7) so pair joins on ts match every row; views are
 * {@code v{A,B}_{table}} over S0 / S1.
 * <p>
 * {@code main verify} prints each query's plan, a row count, an order-independent checksum and one
 * timing, and fails if an arm pair disagrees. Any other args go to JMH.
 * <pre>
 * mvn -pl benchmarks -am package -DskipTests -Plocal-client
 * java --add-exports=java.base/jdk.internal.vm=ALL-UNNAMED --add-opens=java.base/java.lang=ALL-UNNAMED \
 *      --sun-misc-unsafe-memory-access=allow --enable-native-access=ALL-UNNAMED \
 *      -cp benchmarks/target/benchmarks.jar org.questdb.ViewSelfJoinBenchmark verify
 * java ... org.questdb.ViewSelfJoinBenchmark ViewSelfJoinBenchmark -p issue=I1,I2b
 * </pre>
 * Tunables (system properties): {@code viewbench.rows} (default 32,000,000),
 * {@code viewbench.workers} (default 8), {@code viewbench.root}.
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 2, time = 2)
@Measurement(iterations = 3, time = 3)
@Fork(0)
public class ViewSelfJoinBenchmark {

    private static final String DAY = "2024-01-09";
    private static final String DAY_MINUS_1H = "2024-01-08T23:00:00.000000Z";
    private static long lastRows;
    private static final long ROWS = Long.getLong("viewbench.rows", 32_000_000L);
    private static final String ROOT = System.getProperty(
            "viewbench.root",
            System.getProperty("java.io.tmpdir") + java.io.File.separator + "view-self-join-bench"
    );
    private static final int SYMBOLS = 8;
    private static final int WORKERS = Integer.getInteger("viewbench.workers", 8);
    private static final CairoConfiguration configuration = new DefaultCairoConfiguration(ROOT);
    private static SqlCompiler compiler;
    private static SqlExecutionContext ctx;
    private static CairoEngine engine;
    private static WorkerPool pool;
    @Param({"views", "rewrite"})
    public String arm;
    @Param({"I1", "I1b", "I2a", "I2b", "I3a", "I3b", "I4", "I5", "I6a", "I6b"})
    public String issue;
    @Param({"t", "ti"})
    public String table;
    private RecordCursorFactory factory;

    public static void main(String[] args) throws Exception {
        java.nio.file.Files.createDirectories(java.nio.file.Paths.get(ROOT));
        buildData();
        try {
            if (args.length > 0 && "verify".equals(args[0])) {
                verify();
                return;
            }
            final Options opt = args.length > 0
                    ? new CommandLineOptions(args)
                    : new OptionsBuilder().include(ViewSelfJoinBenchmark.class.getSimpleName()).build();
            new Runner(opt).run();
        } finally {
            shutdown();
            LogFactory.haltInstance();
        }
    }

    @Benchmark
    public long run(Blackhole bh) throws SqlException {
        return drain(factory, bh);
    }

    @Setup(Level.Trial)
    public void setUp() throws SqlException {
        ensureEngine();
        factory = compiler.compile(query(issue, arm, table), ctx).getRecordCursorFactory();
    }

    @TearDown(Level.Trial)
    public void tearDown() {
        factory = Misc.free(factory);
    }

    private static void buildData() throws SqlException {
        ensureEngine();
        if (engine.getTableTokenIfExists("ti") != null
                && engine.getTableTokenIfExists("vB_ti") != null
                && rowCount("t") == ROWS && rowCount("ti") == ROWS) {
            System.out.println("view-bench data reused: " + ROWS + " rows/table in " + ROOT);
            return;
        }
        for (String tab : new String[]{"t", "ti"}) {
            engine.execute("DROP VIEW IF EXISTS vA_" + tab, ctx);
            engine.execute("DROP VIEW IF EXISTS vB_" + tab, ctx);
            engine.execute("DROP TABLE IF EXISTS " + tab, ctx);
        }
        engine.execute("CREATE TABLE t (ts TIMESTAMP, sym SYMBOL, px DOUBLE, qty LONG)" +
                " TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL", ctx);
        engine.execute("CREATE TABLE ti (ts TIMESTAMP, sym SYMBOL INDEX, px DOUBLE, qty LONG)" +
                " TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL", ctx);
        // One row per symbol per tick, spread over ~16 day partitions whatever the row count.
        final long ticks = ROWS / SYMBOLS;
        final long spacingUs = Math.max(1L, 16L * 86_400_000_000L / ticks);
        final String gen = "SELECT" +
                " ('2024-01-01'::TIMESTAMP + ((x - 1) / " + SYMBOLS + ") * " + spacingUs + "L)::timestamp," +
                " ('S' || ((x - 1) % " + SYMBOLS + "))::symbol," +
                " rnd_double() * 100," +
                " x" +
                " FROM long_sequence(" + ROWS + ")";
        final long t0 = System.nanoTime();
        engine.execute("INSERT INTO t " + gen, ctx);
        engine.execute("INSERT INTO ti SELECT * FROM t", ctx);
        for (String tab : new String[]{"t", "ti"}) {
            engine.execute("CREATE VIEW vA_" + tab + " AS (SELECT * FROM " + tab + " WHERE sym = 'S0')", ctx);
            engine.execute("CREATE VIEW vB_" + tab + " AS (SELECT * FROM " + tab + " WHERE sym = 'S1')", ctx);
        }
        try (ViewCompilerJob job = new ViewCompilerJob(0, engine)) {
            //noinspection StatementWithEmptyBody
            while (job.run()) {
            }
        }
        engine.releaseAllWriters();
        System.out.println("view-bench data built: " + ROWS + " rows/table in " + (System.nanoTime() - t0) / 1_000_000 + "ms");
    }

    // Consumes every column of every row; returns an order-independent checksum.
    private static long drain(RecordCursorFactory factory, Blackhole bh) throws SqlException {
        long checksum = 0;
        long rows = 0;
        try (RecordCursor cursor = factory.getCursor(ctx)) {
            final RecordMetadata m = factory.getMetadata();
            final int n = m.getColumnCount();
            final Record rec = cursor.getRecord();
            while (cursor.hasNext()) {
                long h = 1;
                for (int c = 0; c < n; c++) {
                    final long v;
                    switch (ColumnType.tagOf(m.getColumnType(c))) {
                        case ColumnType.DOUBLE:
                            final double d = rec.getDouble(c);
                            // round away float summation-order noise from parallel aggregation
                            v = Double.isNaN(d) ? Long.MIN_VALUE : Math.round(d * 1e6);
                            break;
                        case ColumnType.SYMBOL:
                            final CharSequence s = rec.getSymA(c);
                            v = s == null ? 0 : s.toString().hashCode();
                            break;
                        case ColumnType.LONG:
                        case ColumnType.TIMESTAMP:
                            v = rec.getLong(c);
                            break;
                        default:
                            v = rec.getInt(c);
                            break;
                    }
                    h = h * 31 + v;
                }
                if (bh != null) {
                    bh.consume(h);
                }
                checksum += h * 0x9E3779B97F4A7C15L ^ (h >>> 29);
                rows++;
            }
        }
        lastRows = rows;
        return checksum;
    }

    private static synchronized void ensureEngine() {
        if (engine != null) {
            return;
        }
        engine = new CairoEngine(configuration);
        // what server boot does: load view definitions of an existing root (the data is reused)
        engine.buildViewGraphs();
        pool = new WorkerPool(new WorkerPoolConfiguration() {
            @Override
            public String getPoolName() {
                return "view-self-join-bench";
            }

            @Override
            public int getWorkerCount() {
                return WORKERS;
            }
        });
        WorkerPoolUtils.setupQueryJobs(pool, engine);
        pool.start();
        compiler = engine.getSqlCompiler();
        ctx = new SqlExecutionContextImpl(engine, WORKERS) {
            @Override
            public boolean shouldLogSql() {
                return false;
            }
        }.with(
                configuration.getFactoryProvider().getSecurityContextFactory().getRootContext(),
                null, null, -1, null
        );
    }

    private static String query(String issue, String arm, String tab) {
        final boolean views = "views".equals(arm);
        final String a = "vA_" + tab;
        final String b = "vB_" + tab;
        final String pair = "('S0','S1')";
        final String union = "(SELECT * FROM " + a + " UNION ALL SELECT * FROM " + b + ")";
        switch (issue) {
            case "I1":
                return views
                        ? "SELECT * FROM " + union
                        : "SELECT * FROM " + tab + " WHERE sym IN " + pair;
            case "I1b":
                return views
                        ? "SELECT sym, count(), avg(px), sum(qty) FROM " + union + " ORDER BY sym"
                        : "SELECT sym, count(), avg(px), sum(qty) FROM " + tab + " WHERE sym IN " + pair + " ORDER BY sym";
            case "I2a":
                return views
                        ? "SELECT ts, sym, avg(px), sum(qty) FROM " + union + " SAMPLE BY 1h"
                        : "SELECT ts, sym, avg(px), sum(qty) FROM " + tab + " WHERE sym IN " + pair + " SAMPLE BY 1h";
            case "I6a":
                // ASOF with the union as master; pairs S0/S1 rows with the latest S2 row
                return views
                        ? "SELECT a.ts, a.px, c.px FROM " + union + " a ASOF JOIN (" + tab + " WHERE sym = 'S2') c"
                        : "SELECT a.ts, a.px, c.px FROM (" + tab + " WHERE sym IN " + pair + ") a ASOF JOIN (" + tab + " WHERE sym = 'S2') c";
            case "I6b":
                // ASOF with the union as slave
                return views
                        ? "SELECT c.ts, c.px, b.px FROM (" + tab + " WHERE sym = 'S2') c ASOF JOIN " + union + " b"
                        : "SELECT c.ts, c.px, b.px FROM (" + tab + " WHERE sym = 'S2') c ASOF JOIN (" + tab + " WHERE sym IN " + pair + ") b";
            case "I2b":
                return views
                        ? "SELECT * FROM " + union + " LATEST ON ts PARTITION BY sym"
                        : "SELECT * FROM " + tab + " WHERE sym IN " + pair + " LATEST ON ts PARTITION BY sym";
            case "I3a":
                return "SELECT a.ts, a.px, b.px FROM " + a + " a JOIN " + b + " b ON (ts) WHERE a.ts IN '" + DAY + "'"
                        + (views ? "" : " AND b.ts IN '" + DAY + "'");
            case "I3b":
                // The rewrite bounds the right side to the left interval plus 1h of lookback. That is
                // equivalent here only because S1 has a row every tick; an optimiser would instead seek
                // backwards per left row, as "Filtered AsOf Join Fast" already does on the unindexed table.
                return views
                        ? "SELECT a.ts, a.px, b.px FROM " + a + " a ASOF JOIN " + b + " b WHERE a.ts IN '" + DAY + "'"
                        : "SELECT a.ts, a.px, b.px FROM " + a + " a ASOF JOIN (" + b
                        + " WHERE ts BETWEEN '" + DAY_MINUS_1H + "' AND '" + DAY + "T23:59:59.999999Z') b"
                        + " WHERE a.ts IN '" + DAY + "'";
            case "I4":
                return views
                        ? "SELECT count() FROM " + a + " a JOIN " + b + " b ON (sym)"
                        : "SELECT count() FROM " + a + " WHERE 1 = 0";
            case "I5":
                return views
                        ? "SELECT a.ts, a.px pa, b.px pb FROM " + a + " a JOIN " + b + " b ON (ts)"
                        : "SELECT ts, max(CASE WHEN sym = 'S0' THEN px END) pa, max(CASE WHEN sym = 'S1' THEN px END) pb"
                        + " FROM " + tab + " WHERE sym IN " + pair;
            default:
                throw new IllegalArgumentException("unknown issue: " + issue);
        }
    }

    private static long rowCount(String tab) throws SqlException {
        try (
                RecordCursorFactory f = compiler.compile("SELECT count() FROM " + tab, ctx).getRecordCursorFactory();
                RecordCursor c = f.getCursor(ctx)
        ) {
            return c.hasNext() ? c.getRecord().getLong(0) : -1;
        }
    }

    private static void shutdown() {
        compiler = Misc.free(compiler);
        if (pool != null) {
            pool.halt();
            pool = null;
        }
        engine = Misc.free(engine);
    }

    private static void verify() throws SqlException {
        int mismatches = 0;
        for (String tab : new String[]{"t", "ti"}) {
            for (String issue : new String[]{"I1", "I1b", "I2a", "I2b", "I3a", "I3b", "I4", "I5", "I6a", "I6b"}) {
                final long[] sums = new long[2];
                for (int i = 0; i < 2; i++) {
                    final String arm = i == 0 ? "views" : "rewrite";
                    final String sql = query(issue, arm, tab);
                    System.out.println("\n=== " + issue + " / " + arm + " / " + tab + " ===\n" + sql);
                    try (RecordCursorFactory plan = compiler.compile("EXPLAIN " + sql, ctx).getRecordCursorFactory();
                         RecordCursor pc = plan.getCursor(ctx)) {
                        while (pc.hasNext()) {
                            System.out.println(pc.getRecord().getStrA(0));
                        }
                    }
                    try (RecordCursorFactory f = compiler.compile(sql, ctx).getRecordCursorFactory()) {
                        drain(f, null); // warm
                        final long t0 = System.nanoTime();
                        sums[i] = drain(f, null);
                        System.out.printf("rows=%d checksum=%x time=%.1fms%n", lastRows, sums[i], (System.nanoTime() - t0) / 1e6);
                    }
                }
                if (sums[0] != sums[1]) {
                    mismatches++;
                    System.out.println("!!! CHECKSUM MISMATCH " + issue + " / " + tab);
                }
            }
        }
        System.out.println("\nverify done, mismatches=" + mismatches);
        if (mismatches > 0) {
            throw new AssertionError(mismatches + " arm pairs disagree");
        }
    }
}
