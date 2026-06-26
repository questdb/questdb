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
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.log.LogFactory;
import io.questdb.mp.WorkerPool;
import io.questdb.mp.WorkerPoolConfiguration;
import io.questdb.mp.WorkerPoolUtils;
import io.questdb.std.Files;
import io.questdb.std.Misc;
import io.questdb.std.str.Path;
import io.questdb.std.str.Utf8Sequence;
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
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.concurrent.TimeUnit;

/**
 * COLD-cache companion to {@link CoveredIndexDecodeBenchmark}. Before every measured shot it
 * unmaps the table readers ({@link CairoEngine#releaseAllReaders()}) and evicts the db files from
 * the OS page cache ({@code posix_fadvise(DONTNEED)}), so the query reads from disk. This is the
 * realistic first-query / data-doesn't-fit-in-RAM scenario, and it is where a covering index pays
 * off most: a full scan reads every row's columns off disk, while the covered plan reads only the
 * matching rows' columns, packed per symbol.
 * <p>
 * The metric that matters is the {@code disk_MB} secondary counter, not the wall-clock score. On a
 * fast NVMe the covered path's fixed CPU (reopen every partition's readers + traverse the posting
 * index + decode) dwarfs the I/O it saves, so the wall-clock can show covered <em>slower</em> even
 * though it reads far fewer bytes. {@code disk_MB} (bytes fetched from the block device, mmap faults
 * included, via {@code /proc/self/io}) is hardware-independent: covered reads strictly fewer bytes,
 * and a full scan reads a <em>constant</em> amount regardless of selectivity while covered scales
 * with it. Once storage is bandwidth-bound (HDD/network), cold time → fixed_cpu + bytes/B and covered
 * wins by ~the {@code disk_MB} ratio.
 * <p>
 * SingleShotTime (one query per measured iteration); {@code config} = PARALLEL_COV vs PARALLEL_REF
 * (covered vs a parallel non-indexed scan), swept over selectivity. Requires a real (non-tmpfs) db
 * root for the eviction to mean anything. Run exactly like {@link CoveredIndexDecodeBenchmark} (see
 * that class's javadoc); data is shared via the same root. NOTE: invoke via {@code -jar} (so JMH's
 * launcher applies the include regex); the data is built by {@link #main} on the first run and reused
 * thereafter ({@code -Dcovered.bench.skipBuild=true} to force-skip a rebuild).
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.SingleShotTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
// Warmup shots warm the JIT while staying cache-cold (the @Setup(Invocation) evicts before every
// shot, warmup included), so the measured shots isolate cold-DISK time from cold-JIT compilation.
@Warmup(iterations = 4)
@Measurement(iterations = 6)
@Fork(0)
public class CoveredIndexDecodeColdBenchmark {

    // Linux posix_fadvise advice value; QuestDB's Files exposes fadvise() but not this constant.
    private static final int POSIX_FADV_DONTNEED = 4;
    private static final int PARALLEL_WORKERS = Integer.getInteger("covered.bench.workers", 8);
    private static final long ROWS = Long.getLong("covered.bench.rows", 50_000_000L);
    private static final String ROOT = System.getProperty("java.io.tmpdir") + java.io.File.separator + "covered-index-decode-bench";
    private static final CairoConfiguration configuration = new DefaultCairoConfiguration(ROOT) {
        @Override
        public int getSqlPageFrameMaxRows() {
            return 100_000;
        }

        @Override
        public boolean isSqlParallelGroupByEnabled() {
            return true;
        }
    };

    private static CairoEngine engine;
    private static WorkerPool pool;
    private static SqlCompiler compiler;
    private static SqlExecutionContext ctx;

    @Param({"sum", "first_last", "residual", "filter_project", "groupby_symbol", "count"})
    public String shape;

    @Param({"PARALLEL_COV", "PARALLEL_REF"})
    public String config;

    @Param({"1", "10", "50"})
    public String selectivity;

    private RecordCursorFactory factory;

    public static void main(String[] args) throws Exception {
        java.nio.file.Files.createDirectories(java.nio.file.Paths.get(ROOT));
        // Lets you build the data once on fast storage, then re-run the measurement phase against the
        // same root exposed through a slow-storage emulation layer (e.g. a latency-injecting FUSE
        // mirror) without rebuilding through that layer. -Dcovered.bench.skipBuild=true reuses existing data.
        if (!Boolean.getBoolean("covered.bench.skipBuild"))
            try (CairoEngine e = new CairoEngine(configuration)) {
                final SqlExecutionContext c = newContext(e, 1);
                e.execute("DROP TABLE IF EXISTS cov", c);
                e.execute("DROP TABLE IF EXISTS ref", c);
                e.execute("CREATE TABLE cov (ts TIMESTAMP, sym SYMBOL INDEX TYPE POSTING INCLUDE (px, qty, grp, tag)," +
                        " px DOUBLE, qty LONG, grp SYMBOL, tag VARCHAR) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL", c);
                e.execute("CREATE TABLE ref (ts TIMESTAMP, sym SYMBOL, px DOUBLE, qty LONG, grp SYMBOL, tag VARCHAR)" +
                        " TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL", c);
                final long spacingUs = Math.max(1L, 16L * 86_400_000_000L / ROWS);
                final String gen =
                        "SELECT ('2024-01-01'::TIMESTAMP + (x - 1) * " + spacingUs + "L)::timestamp," +
                                " (CASE" +
                                "   WHEN (x % 1000) = 0 THEN 'sel0_1'" +
                                "   WHEN (x % 1000) BETWEEN 1 AND 10 THEN 'sel1'" +
                                "   WHEN (x % 1000) BETWEEN 11 AND 60 THEN 'sel5'" +
                                "   WHEN (x % 1000) BETWEEN 61 AND 160 THEN 'sel10'" +
                                "   WHEN (x % 1000) BETWEEN 161 AND 410 THEN 'sel25'" +
                                "   WHEN (x % 1000) BETWEEN 411 AND 910 THEN 'sel50'" +
                                "   ELSE 'noise' || (x % 64) END)::symbol," +
                                " (x % 997)::double, (x % 1000)::long, ('G' || (x % 8))::symbol, ('T' || (x % 8))::varchar" +
                                " FROM long_sequence(" + ROWS + ")";
                e.execute("INSERT INTO cov " + gen, c);
                e.execute("INSERT INTO ref " + gen, c);
                e.releaseAllWriters();
                System.out.println("covered-bench data built: " + ROWS + " rows/table");
            }
        final Options opt = args.length > 0
                ? new org.openjdk.jmh.runner.options.CommandLineOptions(args)
                : new OptionsBuilder().include(CoveredIndexDecodeColdBenchmark.class.getSimpleName()).build();
        new Runner(opt).run();
        LogFactory.haltInstance();
    }

    @Benchmark
    public void run(Blackhole bh, DiskCounters disk) throws SqlException {
        // The wall-clock score is fixed-CPU-bound on fast NVMe (reader reopen + index traversal +
        // decode dwarf the I/O saved), so it understates covered on cold/slow storage. The meaningful,
        // hardware-independent signal is disk_MB below: bytes actually fetched from the block device
        // (read_bytes counts mmap page faults). Covered reads strictly fewer bytes than a full scan,
        // so on any bandwidth-bound device cold time -> fixed_cpu + bytes/B and covered wins by ~that ratio.
        final long readBytes0 = readBytes();
        try (RecordCursor cursor = factory.getCursor(ctx)) {
            final RecordMetadata m = factory.getMetadata();
            final int n = m.getColumnCount();
            final Record rec = cursor.getRecord();
            while (cursor.hasNext()) {
                for (int col = 0; col < n; col++) {
                    switch (ColumnType.tagOf(m.getColumnType(col))) {
                        case ColumnType.DOUBLE:
                        case ColumnType.FLOAT:
                            bh.consume(rec.getDouble(col));
                            break;
                        case ColumnType.LONG:
                        case ColumnType.TIMESTAMP:
                        case ColumnType.DATE:
                            bh.consume(rec.getLong(col));
                            break;
                        case ColumnType.INT:
                        case ColumnType.SYMBOL:
                        case ColumnType.IPv4:
                            bh.consume(rec.getInt(col));
                            break;
                        case ColumnType.VARCHAR:
                            final Utf8Sequence v = rec.getVarcharA(col);
                            bh.consume(v == null ? 0 : v.size());
                            break;
                        default:
                            bh.consume(1);
                            break;
                    }
                }
            }
        }
        disk.disk_MB = (readBytes() - readBytes0) / 1_000_000;
    }

    // Bytes fetched from the block device by this process since boot (Linux /proc/self/io:read_bytes).
    // Includes mmap page-fault reads, which is how QuestDB reads columns. Returns 0 if unavailable.
    private static long readBytes() {
        try {
            for (String line : java.nio.file.Files.readAllLines(java.nio.file.Paths.get("/proc/self/io"))) {
                if (line.startsWith("read_bytes:")) {
                    return Long.parseLong(line.substring(line.indexOf(':') + 1).trim());
                }
            }
        } catch (Exception ignore) {
        }
        return 0;
    }

    // JMH secondary counter: reports disk_MB (bytes read from storage during the query) alongside the
    // wall-clock score. This is the cold metric that matters -- see run()'s comment.
    @State(Scope.Thread)
    @org.openjdk.jmh.annotations.AuxCounters(org.openjdk.jmh.annotations.AuxCounters.Type.OPERATIONS)
    public static class DiskCounters {
        public long disk_MB;

        @Setup(Level.Invocation)
        public void clean() {
            disk_MB = 0;
        }
    }

    @Setup(Level.Trial)
    public void setUpTrial() throws Exception {
        ensureEngine();
        final String table = "PARALLEL_REF".equals(config) ? "ref" : "cov";
        factory = compiler.compile(query(shape, table, keyFor(selectivity)), ctx).getRecordCursorFactory();
    }

    // Before EACH measured shot: drop the readers (unmap) and evict the db files from the page cache
    // so the query that follows reads cold from disk.
    @Setup(Level.Invocation)
    public void goCold() {
        engine.releaseAllReaders();
        evictPageCache();
    }

    @TearDown(Level.Trial)
    public void tearDownTrial() {
        factory = Misc.free(factory);
    }

    private static synchronized void ensureEngine() {
        if (engine != null) {
            return;
        }
        engine = new CairoEngine(configuration);
        pool = new WorkerPool(new WorkerPoolConfiguration() {
            @Override
            public String getPoolName() {
                return "covered-index-decode-cold-bench";
            }

            @Override
            public int getWorkerCount() {
                return PARALLEL_WORKERS;
            }
        });
        WorkerPoolUtils.setupQueryJobs(pool, engine);
        pool.start();
        compiler = engine.getSqlCompiler();
        ctx = newContext(engine, PARALLEL_WORKERS);
    }

    private static void evictPageCache() {
        try (Path path = new Path()) {
            try (java.util.stream.Stream<java.nio.file.Path> files = java.nio.file.Files.walk(java.nio.file.Paths.get(ROOT))) {
                files.filter(java.nio.file.Files::isRegularFile).forEach(p -> {
                    path.of(p.toString()).$();
                    final long fd = Files.openRO(path.$());
                    if (fd > 0) {
                        final long len = Files.length(fd);
                        if (len > 0) {
                            Files.fadvise(fd, 0, len, POSIX_FADV_DONTNEED);
                        }
                        Files.close(fd);
                    }
                });
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static SqlExecutionContext newContext(CairoEngine engine, int workerCount) {
        return new SqlExecutionContextImpl(engine, workerCount) {
            @Override
            public boolean shouldLogSql() {
                return false;
            }
        }.with(
                configuration.getFactoryProvider().getSecurityContextFactory().getRootContext(),
                null, null, -1, null
        );
    }

    private static String keyFor(String selectivity) {
        switch (selectivity) {
            case "0.1":
                return "sel0_1";
            case "1":
                return "sel1";
            case "5":
                return "sel5";
            case "10":
                return "sel10";
            case "25":
                return "sel25";
            case "50":
                return "sel50";
            default:
                throw new IllegalArgumentException("unknown selectivity: " + selectivity);
        }
    }

    private static String query(String shape, String table, String key) {
        final String w = " WHERE sym = '" + key + "'";
        switch (shape) {
            case "sum":
                return "SELECT sum(px) FROM " + table + w;
            case "multi_agg":
                return "SELECT sum(px), count(), avg(px), min(px), max(px) FROM " + table + w;
            case "first_last":
                return "SELECT first(px), last(px) FROM " + table + w;
            case "count":
                return "SELECT count() FROM " + table + w;
            case "residual":
                return "SELECT sum(px) FROM " + table + w + " AND px > 500";
            case "groupby_symbol":
                return "SELECT grp, sum(px), count() FROM " + table + w + " GROUP BY grp ORDER BY grp";
            case "groupby_varchar":
                return "SELECT tag, sum(px), count() FROM " + table + w + " GROUP BY tag ORDER BY tag";
            case "filter_project":
                return "SELECT ts, px, qty, grp FROM " + table + w;
            default:
                throw new IllegalArgumentException("unknown shape: " + shape);
        }
    }
}
