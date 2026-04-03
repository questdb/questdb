package org.questdb;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.DefaultCairoConfiguration;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlCompilerImpl;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.log.LogFactory;
import io.questdb.std.LongHashSet;
import io.questdb.std.str.Utf8Sequence;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.concurrent.atomic.AtomicLong;

/**
 * SQL-level covering index benchmark: measures end-to-end query speedup and
 * storage compression across scenarios that showcase the covering index
 * advantage over non-covering index and full-scan paths.
 *
 * <pre>
 * mvn install -pl questdb/core -DskipTests -q
 * mvn package -pl questdb/benchmarks -DskipTests -q
 * java -Xmx8g -cp questdb/benchmarks/target/benchmarks.jar org.questdb.CoveringQueryBenchmark
 * </pre>
 */
public class CoveringQueryBenchmark {

    private static final int WARMUP = 3;
    private static final int RUNS = 5;
    private static final int PAGE_SIZE = 4096;

    // I/O cost per 4KB page fault (microseconds) for different storage tiers.
    private static final double NVME_US_PER_PAGE = 80;     // NVMe SSD random 4KB read
    private static final double HDD_US_PER_PAGE = 4000;    // Spinning disk random 4KB read
    private static final double CLOUD_US_PER_PAGE = 1000;  // Cloud block storage (EBS/PD) random 4KB read

    // All benchmark output goes to stderr to avoid interleaving with QuestDB's log
    private static final java.io.PrintStream out = System.err;

    public static void main(String[] args) throws Exception {
        LogFactory.haltInstance();
        Path tmpDir = Files.createTempDirectory("covering-query-bench");
        CairoConfiguration config = new DefaultCairoConfiguration(tmpDir.toString()) {
            @Override
            public int getRndFunctionMemoryMaxPages() {
                return 4096; // 32 MB — enough for high-cardinality rnd_symbol
            }
        };

        try (CairoEngine engine = new CairoEngine(config)) {
            SqlExecutionContextImpl ctx = new SqlExecutionContextImpl(engine, 1)
                    .with(config.getFactoryProvider().getSecurityContextFactory().getRootContext(),
                            null, null, -1, null);
            try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
                runAllScenarios(engine, compiler, ctx, tmpDir);
            }
        }

        deleteDir(tmpDir.toFile());
        System.exit(0);
    }

    private static void runAllScenarios(
            CairoEngine engine, SqlCompilerImpl compiler,
            SqlExecutionContextImpl ctx, Path dbDir
    ) throws SqlException, IOException {
        out.println();

        // S1: High cardinality, selective point lookups (~20 rows returned)
        runScenario(engine, compiler, ctx, dbDir,
                "S1: Selective point lookup (20K keys x 500 rows, VARCHAR+DOUBLE)",
                "s1",
                """
                        CREATE TABLE s1 (
                            ts TIMESTAMP,
                            sym SYMBOL INDEX TYPE POSTING INCLUDE (name, price),
                            name VARCHAR,
                            price DOUBLE
                        ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                        """,
                """
                        INSERT INTO s1
                        SELECT dateadd('T', (x + __OFFSET__)::INT, '2024-01-01')::TIMESTAMP,
                            rnd_symbol(20000, 4, 8, 0),
                            rnd_str('alpha_order_confirmation_region_001',
                                     'beta_order_confirmation_region_002',
                                     'gamma_order_confirmation_region_003',
                                     'delta_order_confirmation_region_004'),
                            rnd_double() * 1000
                        FROM long_sequence(__COUNT__)
                        """,
                10_000_000,
                new Query[]{
                        new Query("covering", "SELECT name, price FROM s1 WHERE sym = '__KEY__'", true),
                        new Query("non_covering", "SELECT /*+ no_covering */ name, price FROM s1 WHERE sym = '__KEY__'", false),
                        new Query("full_scan", "SELECT /*+ no_index */ name, price FROM s1 WHERE sym = '__KEY__'", false),
                }
        );

        // S2: Wide table (10 columns, only 2 covered)
        runScenario(engine, compiler, ctx, dbDir,
                "S2: Wide table narrow projection (20K keys x 500 rows, 10 cols, 2 covered)",
                "s2",
                """
                        CREATE TABLE s2 (
                            ts TIMESTAMP,
                            sym SYMBOL INDEX TYPE POSTING INCLUDE (val, tag),
                            val DOUBLE,
                            tag VARCHAR,
                            c1 LONG, c2 LONG, c3 LONG, c4 LONG,
                            c5 DOUBLE, c6 DOUBLE, c7 DOUBLE, c8 DOUBLE
                        ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                        """,
                """
                        INSERT INTO s2
                        SELECT dateadd('T', (x + __OFFSET__)::INT, '2024-01-01')::TIMESTAMP,
                            rnd_symbol(20000, 4, 8, 0),
                            rnd_double() * 100,
                            rnd_str('sensor_reading_type_a', 'sensor_reading_type_b',
                                     'sensor_reading_type_c', 'sensor_reading_type_d'),
                            rnd_long(), rnd_long(), rnd_long(), rnd_long(),
                            rnd_double(), rnd_double(), rnd_double(), rnd_double()
                        FROM long_sequence(__COUNT__)
                        """,
                10_000_000,
                new Query[]{
                        new Query("covering_2col", "SELECT val, tag FROM s2 WHERE sym = '__KEY__'", true),
                        new Query("non_covering_2col", "SELECT /*+ no_covering */ val, tag FROM s2 WHERE sym = '__KEY__'", false),
                        new Query("non_covering_4col", "SELECT val, tag, c1, c5 FROM s2 WHERE sym = '__KEY__'", false),
                        new Query("full_scan_2col", "SELECT /*+ no_index */ val, tag FROM s2 WHERE sym = '__KEY__'", false),
                }
        );

        // S3: LATEST ON — get last value per key
        runScenario(engine, compiler, ctx, dbDir,
                "S3: LATEST ON (20K keys x 500 rows, DOUBLE)",
                "s3",
                """
                        CREATE TABLE s3 (
                            ts TIMESTAMP,
                            sym SYMBOL INDEX TYPE POSTING INCLUDE (price),
                            price DOUBLE
                        ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                        """,
                """
                        INSERT INTO s3
                        SELECT dateadd('T', (x + __OFFSET__)::INT, '2024-01-01')::TIMESTAMP,
                            rnd_symbol(20000, 4, 8, 0),
                            rnd_double() * 500
                        FROM long_sequence(__COUNT__)
                        """,
                10_000_000,
                new Query[]{
                        new Query("latest_all_cov", "SELECT ts, sym, price FROM s3 LATEST ON ts PARTITION BY sym", true),
                        new Query("latest_all_nc", "SELECT /*+ no_covering */ ts, sym, price FROM s3 LATEST ON ts PARTITION BY sym", false),
                        new Query("latest_1key_cov", "SELECT ts, sym, price FROM s3 WHERE sym = '__KEY__' LATEST ON ts PARTITION BY sym", true),
                        new Query("latest_1key_nc", "SELECT /*+ no_covering */ ts, sym, price FROM s3 WHERE sym = '__KEY__' LATEST ON ts PARTITION BY sym", false),
                }
        );

        // S4: IN-list with selective results
        runScenario(engine, compiler, ctx, dbDir,
                "S4: IN-list (20K keys x 500 rows, INT covered)",
                "s4",
                """
                        CREATE TABLE s4 (
                            ts TIMESTAMP,
                            sym SYMBOL INDEX TYPE POSTING INCLUDE (amount),
                            amount INT
                        ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                        """,
                """
                        INSERT INTO s4
                        SELECT dateadd('T', (x + __OFFSET__)::INT, '2024-01-01')::TIMESTAMP,
                            rnd_symbol(20000, 4, 8, 0),
                            rnd_int(0, 10000, 0)
                        FROM long_sequence(__COUNT__)
                        """,
                10_000_000,
                new Query[]{
                        new Query("in_covering",
                                "SELECT amount FROM s4 WHERE sym IN (__KEYS__)", true),
                        new Query("in_non_covering",
                                "SELECT /*+ no_covering */ amount FROM s4 WHERE sym IN (__KEYS__)", false),
                        new Query("in_full_scan",
                                "SELECT /*+ no_index */ amount FROM s4 WHERE sym IN (__KEYS__)", false),
                }
        );

        // S5: Aggregation — SUM/AVG/COUNT on covered column
        runScenario(engine, compiler, ctx, dbDir,
                "S5: Aggregation on covered column (1K keys x 10K rows, DOUBLE)",
                "s5",
                """
                        CREATE TABLE s5 (
                            ts TIMESTAMP,
                            sym SYMBOL INDEX TYPE POSTING INCLUDE (price),
                            price DOUBLE,
                            payload VARCHAR
                        ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                        """,
                """
                        INSERT INTO s5
                        SELECT dateadd('T', (x + __OFFSET__)::INT, '2024-01-01')::TIMESTAMP,
                            rnd_symbol(1000, 4, 8, 0),
                            rnd_double() * 1000,
                            rnd_str('payload_data_block_aaaa', 'payload_data_block_bbbb',
                                     'payload_data_block_cccc', 'payload_data_block_dddd')
                        FROM long_sequence(__COUNT__)
                        """,
                10_000_000,
                new Query[]{
                        new Query("avg_covering", "SELECT avg(price) FROM s5 WHERE sym = '__KEY__'", true),
                        new Query("avg_non_covering", "SELECT /*+ no_covering */ avg(price) FROM s5 WHERE sym = '__KEY__'", false),
                        new Query("sum_covering", "SELECT sum(price) FROM s5 WHERE sym = '__KEY__'", true),
                        new Query("sum_non_covering", "SELECT /*+ no_covering */ sum(price) FROM s5 WHERE sym = '__KEY__'", false),
                        new Query("count_covering", "SELECT count() FROM s5 WHERE sym = '__KEY__'", true),
                        new Query("count_non_covering", "SELECT /*+ no_covering */ count() FROM s5 WHERE sym = '__KEY__'", false),
                }
        );

        // S6: Bulk throughput — large result set with VARCHAR + DOUBLE
        runScenario(engine, compiler, ctx, dbDir,
                "S6: Bulk throughput (200 keys x 500K rows, VARCHAR+DOUBLE, FSST)",
                "s6",
                """
                        CREATE TABLE s6 (
                            ts TIMESTAMP,
                            sym SYMBOL INDEX TYPE POSTING INCLUDE (name, price),
                            name VARCHAR,
                            price DOUBLE
                        ) TIMESTAMP(ts) PARTITION BY HOUR BYPASS WAL
                        """,
                """
                        INSERT INTO s6
                        SELECT dateadd('T', (x + __OFFSET__)::INT, '2024-01-01')::TIMESTAMP,
                            rnd_symbol(200, 4, 8, 0),
                            rnd_str('order_confirmation_alpha_2024_region_us_east_001',
                                     'order_confirmation_beta_2024_region_eu_west_002',
                                     'order_confirmation_gamma_2024_region_ap_south_003',
                                     'order_confirmation_delta_2024_region_us_west_004'),
                            rnd_double() * 100
                        FROM long_sequence(__COUNT__)
                        """,
                100_000_000,
                new Query[]{
                        new Query("bulk_covering", "SELECT name, price FROM s6 WHERE sym = '__KEY__'", true),
                        new Query("bulk_non_covering", "SELECT /*+ no_covering */ name, price FROM s6 WHERE sym = '__KEY__'", false),
                        new Query("bulk_full_scan", "SELECT /*+ no_index */ name, price FROM s6 WHERE sym = '__KEY__'", false),
                }
        );
    }

    private static void runScenario(
            CairoEngine engine, SqlCompilerImpl compiler, SqlExecutionContextImpl ctx,
            Path dbDir, String title, String tableName, String ddl, String insertTemplate,
            long totalRows, Query[] queries
    ) throws SqlException, IOException {
        out.println("=== " + title + " ===");

        engine.execute(ddl, ctx);
        out.print("    Loading data...");
        long t0 = System.nanoTime();

        long batchSize = 10_000_000;
        long inserted = 0;
        while (inserted < totalRows) {
            long count = Math.min(batchSize, totalRows - inserted);
            String batchInsert = insertTemplate
                    .replace("__COUNT__", String.valueOf(count))
                    .replace("__OFFSET__", String.valueOf(inserted));
            engine.execute(batchInsert, ctx);
            inserted += count;
            out.printf("\r    Loading data... %,d / %,d (%.0f%%)",
                    inserted, totalRows, 100.0 * inserted / totalRows);
        }
        engine.releaseAllWriters();

        double loadSec = (System.nanoTime() - t0) / 1e9;
        out.printf("\r    Loaded %,d rows in %.1f s%n", totalRows, loadSec);

        long[] storageSizes = printTableStorage(dbDir, tableName);
        long columnBytes = storageSizes[0];
        long sidecarBytes = storageSizes[1];

        // Resolve placeholder keys from actual data
        String firstKey = resolveFirstKey(compiler, ctx, tableName);
        String tenKeys = resolveTenKeys(compiler, ctx, tableName);

        // Collect column type sizes for non-covering page counting
        int[] colTypeSizes = collectProjectedColumnSizes(compiler, ctx, queries[0].sql
                .replace("__KEY__", firstKey).replace("__KEYS__", tenKeys));

        out.printf("    %-22s  %8s  %8s  %6s  %10s  %10s  %10s%n",
                "query", "rows", "warm ms", "pages", "+ NVMe", "+ cloud", "+ HDD");
        out.println("    " + "-".repeat(86));

        for (Query q : queries) {
            String sql = q.sql.replace("__KEY__", firstKey).replace("__KEYS__", tenKeys);

            for (int w = 0; w < WARMUP; w++) {
                drainQuery(compiler, ctx, sql);
            }
            long warmNs = 0;
            long rowCount = 0;
            for (int r = 0; r < RUNS; r++) {
                long start = System.nanoTime();
                rowCount = drainQuery(compiler, ctx, sql);
                warmNs += System.nanoTime() - start;
            }
            double warmMs = warmNs / 1e6 / RUNS;

            int pages;
            if (q.isCovering) {
                // Covering reads from sidecar files. Estimate pages from
                // sidecar size proportional to the result fraction.
                long sidecarPages = (sidecarBytes + PAGE_SIZE - 1) / PAGE_SIZE;
                double fraction = totalRows > 0 ? (double) rowCount / totalRows : 1.0;
                pages = Math.max(1, (int) Math.ceil(sidecarPages * fraction));
            } else {
                // Non-covering reads from column .d files at scattered row offsets.
                // Count exact distinct pages from row IDs.
                pages = countPagesTouched(compiler, ctx, sql, colTypeSizes);
            }

            if (pages >= 0) {
                double nvmeMs = warmMs + pages * NVME_US_PER_PAGE / 1000.0;
                double cloudMs = warmMs + pages * CLOUD_US_PER_PAGE / 1000.0;
                double hddMs = warmMs + pages * HDD_US_PER_PAGE / 1000.0;
                out.printf("    %-22s  %,8d  %8.1f  %,6d  %8.1f ms  %8.1f ms  %8.1f ms%n",
                        q.name, rowCount, warmMs, pages, nvmeMs, cloudMs, hddMs);
            } else {
                out.printf("    %-22s  %,8d  %8.1f  %6s  %10s  %10s  %10s%n",
                        q.name, rowCount, warmMs, "n/a", "n/a", "n/a", "n/a");
            }
        }
        out.println();

        engine.execute("DROP TABLE " + tableName, ctx);
        engine.releaseAllWriters();
    }

    private static long drainQuery(SqlCompilerImpl compiler, SqlExecutionContextImpl ctx, String sql) throws SqlException {
        long count = 0;
        long sink = 0;
        try (RecordCursorFactory factory = compiler.compile(sql, ctx).getRecordCursorFactory();
             RecordCursor cursor = factory.getCursor(ctx)) {
            Record record = cursor.getRecord();
            int colCount = factory.getMetadata().getColumnCount();
            while (cursor.hasNext()) {
                for (int c = 0; c < colCount; c++) {
                    int type = factory.getMetadata().getColumnType(c);
                    sink += switch (ColumnType.tagOf(type)) {
                        case ColumnType.DOUBLE -> Double.doubleToRawLongBits(record.getDouble(c));
                        case ColumnType.INT -> record.getInt(c);
                        case ColumnType.LONG -> record.getLong(c);
                        case ColumnType.TIMESTAMP -> record.getTimestamp(c);
                        case ColumnType.VARCHAR -> {
                            Utf8Sequence s = record.getVarcharA(c);
                            yield s != null ? s.size() : 0;
                        }
                        case ColumnType.SYMBOL -> {
                            CharSequence s = record.getSymA(c);
                            yield s != null ? s.length() : 0;
                        }
                        default -> 0;
                    };
                }
                count++;
            }
        }
        if (sink == Long.MIN_VALUE) throw new IllegalStateException();
        return count;
    }

    private static String resolveFirstKey(SqlCompilerImpl compiler, SqlExecutionContextImpl ctx, String table) throws SqlException {
        try (RecordCursorFactory f = compiler.compile("SELECT sym FROM " + table + " WHERE sym IS NOT NULL LIMIT 1", ctx).getRecordCursorFactory();
             RecordCursor c = f.getCursor(ctx)) {
            if (c.hasNext()) {
                CharSequence s = c.getRecord().getSymA(0);
                return s != null ? s.toString() : "???";
            }
        }
        return "???";
    }

    private static String resolveTenKeys(SqlCompilerImpl compiler, SqlExecutionContextImpl ctx, String table) throws SqlException {
        java.util.LinkedHashSet<String> keys = new java.util.LinkedHashSet<>();
        try (RecordCursorFactory f = compiler.compile("SELECT sym FROM " + table + " LIMIT 10000", ctx).getRecordCursorFactory();
             RecordCursor c = f.getCursor(ctx)) {
            while (c.hasNext() && keys.size() < 10) {
                CharSequence s = c.getRecord().getSymA(0);
                if (s != null) keys.add(s.toString());
            }
        }
        StringBuilder sb = new StringBuilder();
        int i = 0;
        for (String k : keys) {
            if (i++ > 0) sb.append(',');
            sb.append('\'').append(k).append('\'');
        }
        return sb.toString();
    }

    /**
     * Counts distinct 4KB pages touched to serve a query result.
     * Uses getRowId() from the cursor to compute exact page offsets
     * for each projected column's .d file.
     */
    /**
     * Counts distinct 4KB pages touched to serve a query result.
     * Uses getRowId() from the cursor to compute exact page offsets
     * for each projected column's data file. Falls back to -1 if
     * the cursor doesn't support getRowId() (e.g., full scan).
     */
    private static int countPagesTouched(SqlCompilerImpl compiler, SqlExecutionContextImpl ctx,
                                         String sql, int[] colTypeSizes) throws SqlException {
        LongHashSet pages = new LongHashSet();
        try (RecordCursorFactory factory = compiler.compile(sql, ctx).getRecordCursorFactory();
             RecordCursor cursor = factory.getCursor(ctx)) {
            int colCount = factory.getMetadata().getColumnCount();
            Record record = cursor.getRecord();
            while (cursor.hasNext()) {
                long rowId;
                try {
                    rowId = record.getRowId();
                } catch (UnsupportedOperationException e) {
                    return -1; // cursor doesn't track row IDs (full scan)
                }
                for (int c = 0; c < colCount && c < colTypeSizes.length; c++) {
                    int typeSize = colTypeSizes[c];
                    if (typeSize > 0) {
                        long pageNum = (rowId * typeSize) / PAGE_SIZE;
                        pages.add(((long) c << 40) | pageNum);
                    }
                }
            }
        }
        return pages.size();
    }

    /**
     * Determines the byte size of each projected column for page counting.
     * Returns 0 for var-sized columns (VARCHAR/STRING) which use aux files.
     */
    private static int[] collectProjectedColumnSizes(SqlCompilerImpl compiler, SqlExecutionContextImpl ctx, String sql) throws SqlException {
        try (RecordCursorFactory factory = compiler.compile(sql, ctx).getRecordCursorFactory()) {
            int colCount = factory.getMetadata().getColumnCount();
            int[] sizes = new int[colCount];
            for (int c = 0; c < colCount; c++) {
                int type = factory.getMetadata().getColumnType(c);
                sizes[c] = ColumnType.isVarSize(type) ? 0 : ColumnType.sizeOf(type);
            }
            return sizes;
        }
    }

    /**
     * @return {columnBytes, sidecarBytes}
     */
    private static long[] printTableStorage(Path dbDir, String tableName) throws IOException {
        Path tableDir = dbDir.resolve(tableName);
        if (!Files.exists(tableDir)) return new long[]{0, 0};

        AtomicLong sidecarTotal = new AtomicLong();
        AtomicLong postingTotal = new AtomicLong();
        AtomicLong columnTotal = new AtomicLong();
        AtomicLong allTotal = new AtomicLong();

        Files.walkFileTree(tableDir, new SimpleFileVisitor<>() {
            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) {
                String name = file.getFileName().toString();
                long size = attrs.size();
                allTotal.addAndGet(size);
                if (name.contains(".pc") && !name.endsWith(".pci")) {
                    sidecarTotal.addAndGet(size);
                } else if (name.contains(".pk") || name.contains(".pv") || name.endsWith(".pci") || name.endsWith(".pd")) {
                    postingTotal.addAndGet(size);
                } else if (name.endsWith(".d") || name.endsWith(".i") || name.endsWith(".o") || name.endsWith(".c")) {
                    columnTotal.addAndGet(size);
                }
                return FileVisitResult.CONTINUE;
            }
        });

        out.printf("    Storage: total=%.0f MB, columns=%.0f MB, index=%.1f MB, sidecars=%.1f MB",
                allTotal.get() / 1_048_576.0, columnTotal.get() / 1_048_576.0,
                postingTotal.get() / 1_048_576.0, sidecarTotal.get() / 1_048_576.0);
        if (columnTotal.get() > 0 && sidecarTotal.get() > 0) {
            out.printf(", sidecar/col=%.1f%%", 100.0 * sidecarTotal.get() / columnTotal.get());
        }
        out.println();
        return new long[]{columnTotal.get(), sidecarTotal.get()};
    }

    private static void deleteDir(File dir) {
        File[] files = dir.listFiles();
        if (files != null) {
            for (File f : files) {
                if (f.isDirectory()) deleteDir(f);
                else f.delete();
            }
        }
        dir.delete();
    }

    record Query(String name, String sql, boolean isCovering) {}
}
