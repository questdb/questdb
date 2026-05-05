package org.questdb;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.ColumnVersionReader;
import io.questdb.cairo.DefaultCairoConfiguration;
import io.questdb.cairo.GenericRecordMetadata;
import io.questdb.cairo.IndexType;
import io.questdb.cairo.TableColumnMetadata;
import io.questdb.cairo.idx.BitmapIndexFwdReader;
import io.questdb.cairo.idx.BitmapIndexWriter;
import io.questdb.cairo.idx.BitpackUtils;
import io.questdb.cairo.idx.CoveringRowCursor;
import io.questdb.cairo.idx.IndexReader;
import io.questdb.cairo.idx.PostingIndexFwdReader;
import io.questdb.cairo.idx.PostingIndexNative;
import io.questdb.cairo.idx.PostingIndexUtils;
import io.questdb.cairo.idx.PostingIndexWriter;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RowCursor;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryMA;
import io.questdb.griffin.SqlCompilerImpl;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.log.LogFactory;
import io.questdb.std.FilesFacade;
import io.questdb.std.LongHashSet;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.Rnd;
import io.questdb.std.Unsafe;
import io.questdb.std.str.Path;
import org.jetbrains.annotations.NotNull;
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
import org.openjdk.jmh.results.RunResult;
import org.openjdk.jmh.results.format.ResultFormatType;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import static io.questdb.cairo.TableUtils.COLUMN_NAME_TXN_NONE;

/**
 * JMH benchmark suite for posting and covering indices.
 * Covers index-comparison, decode, sidecar, SQL, and write benchmarks.
 * Designed to complete in ~5 minutes with {@code @Fork(0)}.
 *
 * <pre>
 * mvn install -pl questdb/core -DskipTests -q
 * mvn package -pl questdb/benchmarks -DskipTests -q
 * java -Xmx4g -Dquestdb.log.level=E -cp questdb/benchmarks/target/benchmarks.jar org.questdb.PostingIndexBenchmarkSuite
 * </pre>
 */
public class PostingIndexBenchmarkSuite {

    private static final double CLOUD_US_PER_PAGE = 1_000;
    private static final long COL_TXN = COLUMN_NAME_TXN_NONE;
    private static final double HDD_US_PER_PAGE = 4_000;
    private static final boolean IS_DELTA = "delta".equals(System.getProperty("questdb.posting.format", "ef"));
    private static final double NVME_US_PER_PAGE = 80;
    private static final int PAGE_SIZE = 4096;
    // SQL keyword for the posting index type: "POSTING" (EF default) or "POSTING DELTA"
    private static final String POSTING_SQL = IS_DELTA ? "POSTING DELTA" : "POSTING";

    private static final PrintStream out = System.err;

    public static void main(String[] args) throws Exception {
        System.setProperty("questdb.log.level", "E");
        LogFactory.haltInstance();

        Collection<RunResult> results = new Runner(new OptionsBuilder()
                .include(PostingIndexBenchmarkSuite.class.getSimpleName() + "\\.")
                .warmupIterations(2).warmupTime(org.openjdk.jmh.runner.options.TimeValue.seconds(1))
                .measurementIterations(3).measurementTime(org.openjdk.jmh.runner.options.TimeValue.seconds(1))
                .forks(0)
                .resultFormat(ResultFormatType.TEXT)
                .build()
        ).run();

        printSummary(results);
        runPageFaultAnalysis();
    }

    /**
     * Per-commit write profiling: measures incremental add+commit cost (market data pattern).
     */
    @Benchmark
    public void commitProfile(CommitState s) {
        // Each invocation does one full write cycle: 56 commits of 512 keys × 128 values
        String dir = System.getProperty("java.io.tmpdir") + File.separator + "suite_commit_" + System.nanoTime();
        new File(dir).mkdirs();
        try {
            initPosting(s.config, dir);
            try (Path path = new Path().of(dir)) {
                PostingIndexWriter writer = new PostingIndexWriter(s.config);
                writer.of(path, "test", COL_TXN, false);
                int rowId = 0;
                for (int c = 0; c < CommitState.COMMITS; c++) {
                    for (int k = 0; k < CommitState.KEYS; k++) {
                        for (int v = 0; v < CommitState.VALUES_PER_COMMIT; v++) {
                            writer.add(k, rowId++);
                        }
                    }
                    writer.setMaxValue(rowId - 1);
                    writer.commit();
                }
                writer.close(); // seal
            }
        } finally {
            deleteDir(dir);
        }
    }

    // ==================================================================================
    // Summary: structured output for feedback/iteration cycles
    // ==================================================================================

    @Benchmark
    public void decode(DecodeState s) {
        BitpackUtils.unpackValuesFrom(s.packedAddr, 0, s.batchSize, s.bitWidth, s.minValue, s.destAddr);
    }

    // ==================================================================================
    // Post-JMH: Page fault projection for covering vs baseline
    // ==================================================================================

    @Benchmark
    public void indexPointRead(IndexState s) {
        try (Path path = new Path().of(s.dir)) {
            IndexReader reader = openReader(s.config, path, s.isPosting);
            try {
                for (int key : s.pointKeys) {
                    try (RowCursor c = reader.getCursor(key, 0, Long.MAX_VALUE)) {
                        while (c.hasNext()) c.next();
                    }
                }
            } finally {
                Misc.free(reader);
            }
        }
    }

    // ==================================================================================
    // Section 1: Index Comparison — Legacy vs Posting, all 7 scenarios
    // ==================================================================================

    @Benchmark
    public void indexRangeRead(IndexState s) {
        try (Path path = new Path().of(s.dir)) {
            IndexReader reader = openReader(s.config, path, s.isPosting);
            try {
                for (int key : s.rangeKeys) {
                    try (RowCursor c = reader.getCursor(key, s.maxRow / 4, s.maxRow * 3 / 4)) {
                        while (c.hasNext()) c.next();
                    }
                }
            } finally {
                Misc.free(reader);
            }
        }
    }

    @Benchmark
    public void indexScanRead(IndexState s) {
        try (Path path = new Path().of(s.dir)) {
            IndexReader reader = openReader(s.config, path, s.isPosting);
            try {
                for (int key = 0; key < s.keyCount; key++) {
                    try (RowCursor c = reader.getCursor(key, 0, Long.MAX_VALUE)) {
                        while (c.hasNext()) c.next();
                    }
                }
            } finally {
                Misc.free(reader);
            }
        }
    }

    @Benchmark
    public long sidecarRead(SidecarState s) {
        long sum = 0;
        try (Path path = new Path().of(s.dir)) {
            try (PostingIndexFwdReader reader = new PostingIndexFwdReader(
                    s.config, path, "test", COLUMN_NAME_TXN_NONE, 0, 0,
                    s.coverMetadata, s.cvr, 0)) {
                for (int key : s.readKeys) {
                    try (RowCursor cursor = reader.getCursor(key, 0, Long.MAX_VALUE)) {
                        if ("covering".equals(s.mode) && cursor instanceof CoveringRowCursor crc && crc.isCoveredAvailable(0)) {
                            while (crc.hasNext()) {
                                crc.next();
                                sum += switch (s.columnType) {
                                    case "DOUBLE" -> (long) crc.getCoveredDouble(0);
                                    case "FLOAT" -> (long) crc.getCoveredFloat(0);
                                    case "LONG", "DECIMAL64" -> crc.getCoveredLong(0);
                                    case "INT", "DECIMAL32" -> crc.getCoveredInt(0);
                                    case "SHORT" -> crc.getCoveredShort(0);
                                    default -> 0;
                                };
                            }
                        } else {
                            while (cursor.hasNext()) {
                                long rowId = cursor.next();
                                sum += switch (s.columnType) {
                                    case "DOUBLE" -> (long) Unsafe.getDouble(s.colAddr + rowId * 8);
                                    case "FLOAT" -> (long) Unsafe.getFloat(s.colAddr + rowId * 4);
                                    case "LONG", "DECIMAL64" -> Unsafe.getLong(s.colAddr + rowId * 8);
                                    case "INT", "DECIMAL32" -> Unsafe.getInt(s.colAddr + rowId * 4);
                                    case "SHORT" -> Unsafe.getShort(s.colAddr + rowId * 2);
                                    default -> 0;
                                };
                            }
                        }
                    }
                }
            }
        }
        return sum;
    }

    @Benchmark
    public long sqlQuery(SqlState s) throws Exception {
        long sum = 0;
        try (RecordCursorFactory factory = s.compiler.compile(s.sql, s.ctx).getRecordCursorFactory()) {
            if (factory == null) return 0;
            try (RecordCursor cursor = factory.getCursor(s.ctx)) {
                Record rec = cursor.getRecord();
                while (cursor.hasNext()) {
                    for (int c = 0, n = factory.getMetadata().getColumnCount(); c < n; c++) {
                        int type = factory.getMetadata().getColumnType(c);
                        sum += switch (ColumnType.tagOf(type)) {
                            case ColumnType.DOUBLE -> (long) rec.getDouble(c);
                            case ColumnType.LONG -> rec.getLong(c);
                            case ColumnType.INT -> rec.getInt(c);
                            default -> 1;
                        };
                    }
                }
            }
        }
        return sum;
    }

    // ==================================================================================
    // Section 2: Decode Throughput
    // ==================================================================================

    @Benchmark
    public void writeInsert(WriteState s) throws Exception {
        s.engine.execute(s.ddl, s.ctx);
        s.engine.execute(s.insertSql, s.ctx);
        s.engine.releaseAllWriters();
        s.engine.execute("DROP TABLE wbench", s.ctx);
    }

    private static DefaultCairoConfiguration benchConfig(String root) {
        return new DefaultCairoConfiguration(root) {
            @Override
            public byte getPostingIndexRowIdEncoding() {
                return IS_DELTA ? PostingIndexUtils.ENCODING_DELTA : PostingIndexUtils.ENCODING_ADAPTIVE;
            }
        };
    }

    private static int[] buildRoundRobin(int totalRows, int keyCount) {
        int[] a = new int[totalRows];
        for (int i = 0; i < totalRows; i++) a[i] = i % keyCount;
        return a;
    }

    // ==================================================================================
    // Section 3: Covering Sidecar Compression
    // ==================================================================================

    private static int[] buildShuffled(int totalRows, int keyCount) {
        int[] a = new int[totalRows];
        for (int i = 0; i < totalRows; i++) a[i] = i % keyCount;
        Random rng = new Random(42);
        for (int i = totalRows - 1; i > 0; i--) {
            int j = rng.nextInt(i + 1);
            int tmp = a[i];
            a[i] = a[j];
            a[j] = tmp;
        }
        return a;
    }

    private static int[] buildZipfian(int totalRows, int keyCount) {
        double[] cdf = new double[keyCount];
        double sum = 0;
        for (int i = 0; i < keyCount; i++) {
            sum += 1.0 / (i + 1);
            cdf[i] = sum;
        }
        for (int i = 0; i < keyCount; i++) cdf[i] /= sum;
        int[] a = new int[totalRows];
        Random rng = new Random(42);
        for (int i = 0; i < totalRows; i++) {
            int key = Arrays.binarySearch(cdf, rng.nextDouble());
            if (key < 0) key = -key - 1;
            a[i] = Math.min(key, keyCount - 1);
        }
        return a;
    }

    // ==================================================================================
    // Section 4: SQL Covering Queries
    // ==================================================================================

    private static void deleteDir(String path) {
        File dir = new File(path);
        if (dir.exists()) {
            File[] files = dir.listFiles();
            if (files != null) for (File f : files) f.delete();
            dir.delete();
        }
    }

    private static void deleteDirRecursive(File dir) {
        try {
            Files.walkFileTree(dir.toPath(), new SimpleFileVisitor<>() {
                @Override
                public @NotNull FileVisitResult postVisitDirectory(java.nio.file.@NotNull Path d, IOException e) throws IOException {
                    Files.delete(d);
                    return FileVisitResult.CONTINUE;
                }

                @Override
                public @NotNull FileVisitResult visitFile(java.nio.file.@NotNull Path file, @NotNull BasicFileAttributes a) throws IOException {
                    Files.delete(file);
                    return FileVisitResult.CONTINUE;
                }
            });
        } catch (IOException ignored) {
        }
    }

    // ==================================================================================
    // Section 5: Write Overhead
    // ==================================================================================

    private static void doCovBaselineRead(CairoConfiguration config, String dir, int[] keys,
                                          long colAddr, CoverType ct) {
        try (Path path = new Path().of(dir)) {
            try (PostingIndexFwdReader reader = new PostingIndexFwdReader(config, path, "test", COL_TXN, 0, 0)) {
                for (int key : keys) {
                    try (RowCursor cursor = reader.getCursor(key, 0, Long.MAX_VALUE)) {
                        while (cursor.hasNext()) {
                            long rowId = cursor.next();
                            switch (ct) {
                                case DOUBLE -> Unsafe.getDouble(colAddr + rowId * 8);
                                case FLOAT -> Unsafe.getFloat(colAddr + rowId * 4);
                                case LONG -> Unsafe.getLong(colAddr + rowId * 8);
                                case INT -> Unsafe.getInt(colAddr + rowId * 4);
                            }
                        }
                    }
                }
            }
        }
    }

    private static void doCovCoveringRead(CairoConfiguration config, String dir, int[] keys, CoverType ct) {
        try (Path path = new Path().of(dir)) {
            try (PostingIndexFwdReader reader = new PostingIndexFwdReader(config, path, "test", COL_TXN, 0, 0)) {
                for (int key : keys) {
                    try (RowCursor cursor = reader.getCursor(key, 0, Long.MAX_VALUE)) {
                        if (cursor instanceof CoveringRowCursor crc && crc.isCoveredAvailable(0)) {
                            while (crc.hasNext()) {
                                crc.next();
                                switch (ct) {
                                    case DOUBLE -> crc.getCoveredDouble(0);
                                    case FLOAT -> crc.getCoveredFloat(0);
                                    case LONG -> crc.getCoveredLong(0);
                                    case INT -> crc.getCoveredInt(0);
                                }
                            }
                        } else {
                            while (cursor.hasNext()) cursor.next();
                        }
                    }
                }
            }
        }
    }

    // ==================================================================================
    // Shared utilities
    // ==================================================================================

    private static long getDirectorySize(String path) {
        File dir = new File(path);
        long size = 0;
        File[] files = dir.listFiles();
        if (files != null) for (File f : files) size += f.length();
        return size;
    }

    private static void initPosting(CairoConfiguration config, String dir) {
        try (Path path = new Path().of(dir)) {
            int plen = path.size();
            FilesFacade ff = config.getFilesFacade();
            try (MemoryMA mem = Vm.getSmallCMARWInstance(ff, PostingIndexUtils.keyFileName(path, "test", COL_TXN),
                    MemoryTag.MMAP_DEFAULT, config.getWriterFileOpenOpts())) {
                PostingIndexWriter.initKeyMemory(mem);
            }
            // Fresh file: sealTxn starts equal to postingColumnNameTxn (no seal performed yet).
            ff.touch(PostingIndexUtils.valueFileName(path.trimTo(plen), "test", COL_TXN, COL_TXN));
        }
    }

    private static IndexReader openReader(CairoConfiguration config, Path path, boolean posting) {
        return posting
                ? new PostingIndexFwdReader(config, path, "test", COL_TXN, -1, 0)
                : new BitmapIndexFwdReader(config, path, "test", COL_TXN, -1, 0);
    }

    private static void printSummary(Collection<RunResult> results) {
        // Index results by benchmark name and params
        Map<String, Double> scores = new LinkedHashMap<>();
        for (RunResult rr : results) {
            String label = rr.getParams().getBenchmark().replace("org.questdb.PostingIndexBenchmarkSuite.", "");
            Map<String, String> params = rr.getParams().getParamsKeys().stream()
                    .collect(LinkedHashMap::new, (m, k) -> m.put(k, rr.getParams().getParam(k)), Map::putAll);
            String key = label + params.values().stream()
                    .filter(v -> !"N/A".equals(v))
                    .reduce("", (a, v) -> a + "/" + v);
            scores.put(key, rr.getPrimaryResult().getScore());
        }

        out.println();
        out.println("╔══════════════════════════════════════════════════════════════════════════════════╗");
        out.printf("║              POSTING INDEX BENCHMARK SUMMARY  [encoding: %-5s]                  ║%n", IS_DELTA ? "DELTA" : "EF");
        out.println("╚══════════════════════════════════════════════════════════════════════════════════╝");

        // --- Decode ---
        out.println();
        out.println("── Decode Throughput (ops/s, higher=better) ──────────────────────────────────────");
        out.printf("  %-8s", "batch");
        for (String bw : new String[]{"1", "8", "12", "16", "20", "32"}) out.printf(" %12s", bw + "-bit");
        out.println();
        for (String batch : new String[]{"64", "256", "1024"}) {
            out.printf("  %-8s", batch);
            for (String bw : new String[]{"1", "8", "12", "16", "20", "32"}) {
                Double v = scores.get("decode/" + batch + "/" + bw);
                out.printf(" %,12.0f", v != null ? v : 0);
            }
            out.println();
        }

        // --- Index Comparison ---
        out.println();
        out.println("── Index Comparison: Posting vs Legacy (ops/s, higher=better) ────────────────────");
        String[] scenarios = {"S1", "S2", "S3", "S4", "S5", "S6", "S7", "S8"};
        for (String bench : new String[]{"indexPointRead", "indexScanRead", "indexRangeRead"}) {
            out.printf("  %s:%n", bench);
            out.printf("  %-6s %10s %10s %8s%n", "", "LEGACY", "POSTING", "ratio");
            for (String s : scenarios) {
                Double leg = scores.get(bench + "/LEGACY/" + s);
                Double post = scores.get(bench + "/POSTING/" + s);
                if (leg != null && post != null) {
                    String ratio = String.format("%.2fx", post / leg);
                    String marker = post < leg ? " ◄" : "";
                    out.printf("  %-6s %,10.0f %,10.0f %8s%s%n", s, leg, post, ratio, marker);
                }
            }
            out.println();
        }

        // --- Sidecar ---
        out.println("── Sidecar Compression (ops/s, higher=better) ────────────────────────────────────");
        out.printf("  %-8s %10s %10s %8s%n", "type", "baseline", "covering", "ratio");
        for (String t : new String[]{"DOUBLE", "FLOAT", "LONG", "INT", "DECIMAL64", "DECIMAL32", "SHORT"}) {
            Double base = scores.get("sidecarRead/" + t + "/baseline");
            Double cov = scores.get("sidecarRead/" + t + "/covering");
            if (base != null && cov != null) {
                out.printf("  %-8s %,10.0f %,10.0f %7.2fx%n", t, base, cov, cov / base);
            }
        }

        // --- SQL Queries ---
        out.println();
        out.println("── SQL Queries (ops/s, higher=better) ────────────────────────────────────────────");
        String[][] sqlGroups = {
                {"Point lookup", "covering_where", "non_covering_where"},
                {"Aggregation", "covering_agg", "covering_sum", "covering_count"},
                {"Filter", "residual_filter", "non_covering_filter", "no_index_filter"},
                {"Filter IN-list", "residual_filter_in", "non_covering_filter_in"},
                {"VARCHAR/FSST", "varchar_fsst", "varchar_non_covering", "varchar_in_covering"},
                {"O3", "o3_covering", "o3_non_covering", "o3_distinct"},
                {"Misc", "latest_on", "in_list", "wide_table"},
                {"Bulk throughput", "bulk_covering", "bulk_non_covering"},
        };
        for (String[] group : sqlGroups) {
            out.printf("  %s:%n", group[0]);
            for (int i = 1; i < group.length; i++) {
                Double v = scores.get("sqlQuery/" + group[i]);
                if (v != null) {
                    out.printf("    %-28s %,12.0f ops/s%n", group[i], v);
                }
            }
        }

        // --- Write ---
        out.println();
        out.println("── Write Overhead (ops/s, higher=better) ─────────────────────────────────────────");
        for (String cfg : new String[]{"no_index", "bitmap", "posting", "posting_covering", "posting_varchar"}) {
            Double v = scores.get("writeInsert/" + cfg);
            if (v != null) {
                out.printf("  %-22s %,10.1f ops/s%n", cfg, v);
            }
        }

        // --- Commit ---
        Double commit = scores.get("commitProfile");
        if (commit != null) {
            out.println();
            out.printf("── Commit Profile (512 keys × 56 commits × 128 v/commit + seal) ──────────────────%n");
            out.printf("  %-22s %,10.1f ops/s  (%.1f ms/cycle)%n", "full write+seal cycle", commit, 1000.0 / commit);
        }

        out.println();
    }

    private static String resolveKey(SqlCompilerImpl compiler, SqlExecutionContextImpl ctx, String table) throws Exception {
        try (RecordCursorFactory f = compiler.compile("SELECT sym FROM " + table + " LIMIT 1", ctx).getRecordCursorFactory()) {
            try (RecordCursor c = f.getCursor(ctx)) {
                if (c.hasNext()) return c.getRecord().getSymA(0).toString();
            }
        }
        return "A";
    }

    private static String resolveNKeys(SqlCompilerImpl compiler, SqlExecutionContextImpl ctx, String table, int n) throws Exception {
        StringBuilder sb = new StringBuilder();
        try (RecordCursorFactory f = compiler.compile("SELECT DISTINCT sym FROM " + table + " LIMIT " + n, ctx).getRecordCursorFactory()) {
            try (RecordCursor c = f.getCursor(ctx)) {
                while (c.hasNext()) {
                    if (!sb.isEmpty()) sb.append(',');
                    sb.append('\'').append(c.getRecord().getSymA(0)).append('\'');
                }
            }
        }
        return sb.toString();
    }

    private static void runPageFaultAnalysis() {
        System.err.println();
        System.err.println("══════════════════════════════════════════════════════════════════════════════════════════════════");
        System.err.println("  Covering Index I/O Projection (CPU + predicted page faults)");
        System.err.println("══════════════════════════════════════════════════════════════════════════════════════════════════");

        int keys = SidecarState.KEYS;
        int rows = SidecarState.ROWS;
        int readKeys = SidecarState.READ_KEYS;
        int warmup = 3;
        int runs = 5;

        LogFactory.haltInstance();
        String tmpDir = System.getProperty("java.io.tmpdir");
        CairoConfiguration config = benchConfig(tmpDir);
        int[] keyAssignment = buildShuffled(rows, keys);
        int[] queryKeys = new int[readKeys];
        Random rng = new Random(99);
        for (int i = 0; i < readKeys; i++) queryKeys[i] = rng.nextInt(keys);

        System.err.printf("  %,d keys × %,d v/k = %,d rows, querying %,d keys%n%n", keys, SidecarState.VPK, rows, readKeys);
        System.err.printf("  %-14s  %6s  %6s  %8s  %8s  %8s  %8s  %8s%n",
                "type", "pages", "pages", "CPU ms", "CPU ms", "+ NVMe", "+ cloud", "+ HDD");
        System.err.printf("  %-14s  %6s  %6s  %8s  %8s  %8s  %8s  %8s%n",
                "", "base", "cover", "base", "cover", "cover", "cover", "cover");
        System.err.println("  " + "─".repeat(88));

        for (CoverType ct : CoverType.values()) {
            String baseDir = tmpDir + File.separator + "pf_base_" + ct + "_" + System.nanoTime();
            String covDir = tmpDir + File.separator + "pf_cov_" + ct + "_" + System.nanoTime();
            new File(baseDir).mkdirs();
            new File(covDir).mkdirs();

            long colAddr = Unsafe.malloc((long) rows * ct.size, MemoryTag.NATIVE_DEFAULT);
            Random dataRng = new Random(42);
            for (int i = 0; i < rows; i++) {
                switch (ct) {
                    case DOUBLE -> Unsafe.putDouble(colAddr + (long) i * 8, 100.0 + dataRng.nextInt(90_000) * 0.01);
                    case FLOAT -> Unsafe.putFloat(colAddr + (long) i * 4, 20.0f + dataRng.nextInt(500) * 0.01f);
                    case LONG ->
                            Unsafe.putLong(colAddr + (long) i * 8, 1_700_000_000_000_000L + dataRng.nextInt(1_000_000));
                    case INT -> Unsafe.putInt(colAddr + (long) i * 4, dataRng.nextInt(10_000));
                }
            }

            try {
                // Write baseline (no covering) and covering index
                writeCovIndex(config, baseDir, keyAssignment, null, 0, null, null, null);
                writeCovIndex(config, covDir, keyAssignment,
                        new long[]{colAddr}, 1, new int[]{ct.shift}, new int[]{ct.columnType}, new long[]{0});

                // Count distinct pages for baseline reads
                LongHashSet baselinePages = new LongHashSet();
                try (Path path = new Path().of(baseDir)) {
                    try (PostingIndexFwdReader reader = new PostingIndexFwdReader(config, path, "test", COL_TXN, 0, 0)) {
                        for (int key : queryKeys) {
                            try (RowCursor cursor = reader.getCursor(key, 0, Long.MAX_VALUE)) {
                                while (cursor.hasNext()) {
                                    long rowId = cursor.next();
                                    long pageNum = (rowId * ct.size) / PAGE_SIZE;
                                    baselinePages.add(pageNum);
                                }
                            }
                        }
                    }
                }
                int basePages = baselinePages.size();

                // Covering pages: sidecar file size, proportional to queried fraction
                long baseSize = getDirectorySize(baseDir);
                long covSize = getDirectorySize(covDir);
                long sidecarBytes = covSize - baseSize;
                long sidecarTotalPages = (sidecarBytes + PAGE_SIZE - 1) / PAGE_SIZE;
                double fraction = (double) readKeys / keys;
                int covPages = Math.max(1, (int) Math.ceil(sidecarTotalPages * fraction));

                // Measure CPU time
                for (int w = 0; w < warmup; w++) {
                    doCovBaselineRead(config, baseDir, queryKeys, colAddr, ct);
                    doCovCoveringRead(config, covDir, queryKeys, ct);
                }

                long baseCpuNs = 0;
                for (int i = 0; i < runs; i++) {
                    long t0 = System.nanoTime();
                    doCovBaselineRead(config, baseDir, queryKeys, colAddr, ct);
                    baseCpuNs += System.nanoTime() - t0;
                }
                double baseCpuMs = baseCpuNs / (runs * 1e6);

                long covCpuNs = 0;
                for (int i = 0; i < runs; i++) {
                    long t0 = System.nanoTime();
                    doCovCoveringRead(config, covDir, queryKeys, ct);
                    covCpuNs += System.nanoTime() - t0;
                }
                double covCpuMs = covCpuNs / (runs * 1e6);

                double nvmeMs = covCpuMs + covPages * NVME_US_PER_PAGE / 1_000.0;
                double cloudMs = covCpuMs + covPages * CLOUD_US_PER_PAGE / 1_000.0;
                double hddMs = covCpuMs + covPages * HDD_US_PER_PAGE / 1_000.0;
                double baseNvmeMs = baseCpuMs + basePages * NVME_US_PER_PAGE / 1_000.0;

                System.err.printf("  %-14s  %,6d  %,6d  %8.1f  %8.1f  %8.1f  %8.1f  %8.1f%n",
                        ct.name(), basePages, covPages, baseCpuMs, covCpuMs, nvmeMs, cloudMs, hddMs);
                System.err.printf("  %-14s  %6s  %6s  %8s  %8s  %8.1f  %8s  %8s    (baseline + NVMe for comparison)%n",
                        "", "", "", "", "", baseNvmeMs, "", "");
            } finally {
                Unsafe.free(colAddr, (long) rows * ct.size, MemoryTag.NATIVE_DEFAULT);
                deleteDir(baseDir);
                deleteDir(covDir);
            }
        }
        System.err.println();
    }

    private static int[] selectRandomKeys(int keyCount, int n) {
        int[] keys = new int[n];
        Random rng = new Random(99);
        for (int i = 0; i < n; i++) keys[i] = rng.nextInt(keyCount);
        return keys;
    }

    private static void writeCovIndex(CairoConfiguration config, String dir, int[] keyAssignment,
                                      long[] colAddrs, int coverCount,
                                      int[] shifts, int[] types, long[] tops) {
        try (Path path = new Path().of(dir)) {
            try (PostingIndexWriter writer = new PostingIndexWriter(config, path, "test", COLUMN_NAME_TXN_NONE)) {
                if (coverCount > 0) {
                    int[] indices = new int[coverCount];
                    for (int i = 0; i < coverCount; i++) indices[i] = i + 2;
                    writer.configureCovering(colAddrs, tops, shifts, indices, types, coverCount);
                }
                for (int rowId = 0; rowId < keyAssignment.length; rowId++) {
                    writer.add(keyAssignment[rowId], rowId);
                }
                writer.setMaxValue(keyAssignment.length - 1);
            }
        }
    }

    enum CoverType {
        DOUBLE(ColumnType.DOUBLE, 3, 8),
        FLOAT(ColumnType.FLOAT, 2, 4),
        LONG(ColumnType.LONG, 3, 8),
        INT(ColumnType.INT, 2, 4);

        final int columnType, shift, size;

        CoverType(int columnType, int shift, int size) {
            this.columnType = columnType;
            this.shift = shift;
            this.size = size;
        }
    }

    // ==================================================================================
    // Page fault analysis helpers
    // ==================================================================================

    /**
     * State for per-commit write profiling (market data pattern from IndexScenarioBenchmark).
     * 512 keys, 56 commits, 128 values per key per commit = 3.67M rows.
     * Measures the full add→commit→seal cycle cost.
     */
    @State(Scope.Benchmark)
    @BenchmarkMode(Mode.SingleShotTime)
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    public static class CommitState {
        static final int COMMITS = 56;
        static final int KEYS = 512;
        static final int VALUES_PER_COMMIT = 128;

        CairoConfiguration config;

        @Setup(Level.Trial)
        public void setup() {
            String tmpDir = System.getProperty("java.io.tmpdir");
            config = benchConfig(tmpDir);
        }
    }

    @State(Scope.Benchmark)
    @BenchmarkMode(Mode.AverageTime)
    @OutputTimeUnit(TimeUnit.NANOSECONDS)
    public static class DecodeState {
        static final int TOTAL = 65_536;
        @Param({"64", "256", "1024"})
        int batchSize;
        @Param({"1", "8", "12", "16", "20", "32"})
        int bitWidth;
        long destAddr;
        long minValue = 1_000_000L;
        long packedAddr;
        int packedSize;

        @Setup(Level.Trial)
        public void setup() {
            long maxOffset = (1L << bitWidth) - 1;
            packedSize = BitpackUtils.packedDataSize(TOTAL, bitWidth);
            packedAddr = Unsafe.malloc(packedSize, MemoryTag.NATIVE_DEFAULT);
            Unsafe.setMemory(packedAddr, packedSize, (byte) 0);

            long valuesSize = (long) TOTAL * Long.BYTES;
            long valuesAddr = Unsafe.malloc(valuesSize, MemoryTag.NATIVE_DEFAULT);
            for (int i = 0; i < TOTAL; i++) {
                Unsafe.putLong(valuesAddr + (long) i * Long.BYTES,
                        minValue + (i % (maxOffset + 1)));
            }
            PostingIndexNative.packValuesNativeFallback(valuesAddr, TOTAL, minValue, bitWidth, packedAddr);
            Unsafe.free(valuesAddr, valuesSize, MemoryTag.NATIVE_DEFAULT);

            destAddr = Unsafe.malloc((long) batchSize * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
        }

        @TearDown(Level.Trial)
        public void tearDown() {
            Unsafe.free(destAddr, (long) batchSize * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
            Unsafe.free(packedAddr, packedSize, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @State(Scope.Benchmark)
    @BenchmarkMode(Mode.AverageTime)
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    public static class IndexState {
        CairoConfiguration config;
        // Written index directory
        String dir;
        @Param({"LEGACY", "POSTING"})
        String format;
        boolean isPosting;
        int keyCount;
        long maxRow;
        int[] pointKeys;
        int[] rangeKeys;
        long rowIdBase;
        @Param({"S1", "S2", "S3", "S4", "S5", "S6", "S7", "S8"})
        String scenario;
        int totalRows;

        @Setup(Level.Trial)
        public void setup() {
            String tmpDir = System.getProperty("java.io.tmpdir");
            config = benchConfig(tmpDir);
            isPosting = "POSTING".equals(format);

            // Scaled data sizes: preserve distribution, ~1-2M rows for speed
            int commitInterval;
            int[] keys;
            switch (scenario) {
                case "S1" -> {
                    keyCount = 500_000;
                    totalRows = 2_000_000;
                    commitInterval = totalRows;
                    keys = buildShuffled(totalRows, keyCount);
                }
                case "S2" -> {
                    keyCount = 512;
                    totalRows = 1_024_000;
                    commitInterval = 65_536;
                    keys = buildRoundRobin(totalRows, keyCount);
                }
                case "S3" -> { // streaming handled inline
                    keyCount = 10_000;
                    setupStreaming();
                    return;
                }
                case "S4" -> {
                    keyCount = 2_000;
                    totalRows = 2_000_000;
                    commitInterval = totalRows;
                    keys = buildRoundRobin(totalRows, keyCount);
                }
                case "S5" -> {
                    keyCount = 500;
                    totalRows = 1_000_000;
                    commitInterval = totalRows;
                    keys = buildZipfian(totalRows, keyCount);
                }
                case "S6" -> {
                    keyCount = 200_000;
                    totalRows = 1_200_000;
                    commitInterval = totalRows;
                    keys = buildShuffled(totalRows, keyCount);
                }
                case "S7" -> {
                    keyCount = 1_000_000;
                    totalRows = 2_000_000;
                    commitInterval = totalRows;
                    keys = buildShuffled(totalRows, keyCount);
                }
                case "S8" -> {
                    // Row offset dimension: 1B base offset forces 30-bit row IDs (wider bitpacking)
                    keyCount = 5_000;
                    totalRows = 1_000_000;
                    commitInterval = totalRows;
                    keys = buildRoundRobin(totalRows, keyCount);
                    rowIdBase = 1_000_000_000L;
                }
                default -> throw new IllegalArgumentException(scenario);
            }

            maxRow = rowIdBase + totalRows - 1;
            pointKeys = selectRandomKeys(keyCount, Math.min(5_000, keyCount));
            rangeKeys = selectRandomKeys(keyCount, Math.min(500, keyCount));

            dir = tmpDir + File.separator + "suite_" + scenario + "_" + format + "_" + System.nanoTime();
            new File(dir).mkdirs();

            if (isPosting) {
                initPosting(config, dir);
                try (Path path = new Path().of(dir)) {
                    PostingIndexWriter writer = new PostingIndexWriter(config);
                    writer.of(path, "test", COL_TXN, false);
                    for (int i = 0; i < totalRows; i++) {
                        writer.add(keys[i], rowIdBase + i);
                        if ((i + 1) % commitInterval == 0) {
                            writer.setMaxValue(rowIdBase + i);
                            writer.commit();
                        }
                    }
                    if (totalRows % commitInterval != 0) {
                        writer.setMaxValue(rowIdBase + totalRows - 1);
                        writer.commit();
                    }
                    writer.close(); // seal
                }
            } else {
                int blockCap = Math.max(8, Numbers.ceilPow2(totalRows / Math.max(keyCount, 1)));
                try (Path path = new Path().of(dir)) {
                    try (BitmapIndexWriter w = new BitmapIndexWriter(config)) {
                        w.of(path, "test", COL_TXN, blockCap);
                        for (int i = 0; i < totalRows; i++) w.add(keys[i], rowIdBase + i);
                    }
                }
            }
        }

        @TearDown(Level.Trial)
        public void tearDown() {
            deleteDir(dir);
        }

        private void setupStreaming() {
            int commits = 100;
            int activePerCommit = 200;
            int valsPerActive = 10;
            Rnd rnd = new Rnd(12345, 67890);

            int[][] activeKeyArrays = new int[commits][];
            totalRows = 0;
            for (int c = 0; c < commits; c++) {
                int[] pool = new int[keyCount];
                for (int i = 0; i < keyCount; i++) pool[i] = i;
                for (int i = 0; i < activePerCommit; i++) {
                    int j = i + rnd.nextPositiveInt() % (keyCount - i);
                    int tmp = pool[i];
                    pool[i] = pool[j];
                    pool[j] = tmp;
                }
                activeKeyArrays[c] = Arrays.copyOf(pool, activePerCommit);
                Arrays.sort(activeKeyArrays[c]);
                totalRows += activePerCommit * valsPerActive;
            }
            maxRow = totalRows - 1;
            pointKeys = selectRandomKeys(keyCount, 5_000);
            rangeKeys = selectRandomKeys(keyCount, 500);

            String tmpDir = System.getProperty("java.io.tmpdir");
            dir = tmpDir + File.separator + "suite_S3_" + format + "_" + System.nanoTime();
            new File(dir).mkdirs();

            if (isPosting) {
                initPosting(config, dir);
                try (Path path = new Path().of(dir)) {
                    PostingIndexWriter writer = new PostingIndexWriter(config);
                    writer.of(path, "test", COL_TXN, false);
                    int rowId = 0;
                    for (int c = 0; c < commits; c++) {
                        for (int key : activeKeyArrays[c]) {
                            for (int v = 0; v < valsPerActive; v++) writer.add(key, rowId++);
                        }
                        writer.setMaxValue(rowId - 1);
                        writer.commit();
                    }
                    writer.close();
                }
            } else {
                try (Path path = new Path().of(dir)) {
                    try (BitmapIndexWriter w = new BitmapIndexWriter(config)) {
                        w.of(path, "test", COL_TXN, 64);
                        int rowId = 0;
                        for (int c = 0; c < commits; c++) {
                            for (int key : activeKeyArrays[c]) {
                                for (int v = 0; v < valsPerActive; v++) w.add(key, rowId++);
                            }
                        }
                    }
                }
            }
        }
    }

    @State(Scope.Benchmark)
    @BenchmarkMode(Mode.AverageTime)
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    public static class SidecarState {
        static final int COVER_WRITER_IDX = 2;
        static final int KEYS = 500;
        static final int READ_KEYS = 200;
        static final int VPK = 2_000;
        static final int ROWS = KEYS * VPK;
        long colAddr;
        int colType, colShift, colSize;
        @Param({"DOUBLE", "FLOAT", "LONG", "INT", "DECIMAL64", "DECIMAL32", "SHORT"})
        String columnType;
        CairoConfiguration config;
        GenericRecordMetadata coverMetadata;
        ColumnVersionReader cvr;
        String dir;
        @Param({"baseline", "covering"})
        String mode;
        int[] readKeys;

        @Setup(Level.Trial)
        public void setup() {
            switch (columnType) {
                case "DOUBLE" -> {
                    colType = ColumnType.DOUBLE;
                    colShift = 3;
                    colSize = 8;
                }
                case "FLOAT" -> {
                    colType = ColumnType.FLOAT;
                    colShift = 2;
                    colSize = 4;
                }
                case "LONG" -> {
                    colType = ColumnType.LONG;
                    colShift = 3;
                    colSize = 8;
                }
                case "INT" -> {
                    colType = ColumnType.INT;
                    colShift = 2;
                    colSize = 4;
                }
                case "DECIMAL64" -> {
                    colType = ColumnType.DECIMAL64;
                    colShift = 3;
                    colSize = 8;
                }
                case "DECIMAL32" -> {
                    colType = ColumnType.DECIMAL32;
                    colShift = 2;
                    colSize = 4;
                }
                case "SHORT" -> {
                    colType = ColumnType.SHORT;
                    colShift = 1;
                    colSize = 2;
                }
                default -> throw new IllegalArgumentException(columnType);
            }

            String tmpDir = System.getProperty("java.io.tmpdir");
            config = benchConfig(tmpDir);
            dir = tmpDir + File.separator + "suite_cov_" + columnType + "_" + mode + "_" + System.nanoTime();
            new File(dir).mkdirs();

            coverMetadata = new GenericRecordMetadata();
            for (int i = 0; i <= COVER_WRITER_IDX; i++) {
                int t = (i == COVER_WRITER_IDX) ? colType : ColumnType.LONG;
                coverMetadata.add(new TableColumnMetadata(
                        "c" + i, t, IndexType.NONE, 0, false, null, i, false));
            }
            cvr = new ColumnVersionReader();

            colAddr = Unsafe.malloc((long) ROWS * colSize, MemoryTag.NATIVE_DEFAULT);
            Random rng = new Random(42);
            for (int i = 0; i < ROWS; i++) {
                switch (columnType) {
                    case "DOUBLE" -> Unsafe.putDouble(colAddr + (long) i * 8, 100.0 + rng.nextInt(90_000) * 0.01);
                    case "FLOAT" -> Unsafe.putFloat(colAddr + (long) i * 4, 20.0f + rng.nextInt(500) * 0.01f);
                    case "LONG" ->
                            Unsafe.putLong(colAddr + (long) i * 8, 1_700_000_000_000_000L + rng.nextInt(1_000_000));
                    case "INT" -> Unsafe.putInt(colAddr + (long) i * 4, rng.nextInt(10_000));
                    case "DECIMAL64" ->
                        // Prices with scale=2: 100.00–1000.00 → unscaled 10_000–100_000
                            Unsafe.putLong(colAddr + (long) i * 8, 10_000L + rng.nextInt(90_000));
                    case "DECIMAL32" ->
                        // Prices with scale=2: 20.00–25.00 → unscaled 2_000–2_500
                            Unsafe.putInt(colAddr + (long) i * 4, 2_000 + rng.nextInt(500));
                    case "SHORT" -> Unsafe.putShort(colAddr + (long) i * 2, (short) rng.nextInt(1_000));
                }
            }

            readKeys = new int[READ_KEYS];
            rng = new Random(99);
            for (int i = 0; i < READ_KEYS; i++) readKeys[i] = rng.nextInt(KEYS);

            int[] keyAssignment = buildShuffled(ROWS, KEYS);
            boolean covering = "covering".equals(mode);
            try (Path path = new Path().of(dir)) {
                try (PostingIndexWriter writer = new PostingIndexWriter(config, path, "test", COLUMN_NAME_TXN_NONE)) {
                    if (covering) {
                        writer.configureCovering(
                                new long[]{colAddr}, new long[]{0},
                                new int[]{colShift}, new int[]{2},
                                new int[]{colType}, 1);
                    }
                    for (int i = 0; i < ROWS; i++) writer.add(keyAssignment[i], i);
                    writer.setMaxValue(ROWS - 1);
                }
            }
        }

        @TearDown(Level.Trial)
        public void tearDown() {
            Misc.free(cvr);
            Unsafe.free(colAddr, (long) ROWS * colSize, MemoryTag.NATIVE_DEFAULT);
            deleteDir(dir);
        }
    }

    @State(Scope.Benchmark)
    @BenchmarkMode(Mode.AverageTime)
    @OutputTimeUnit(TimeUnit.MICROSECONDS)
    public static class SqlState {
        SqlCompilerImpl compiler;
        SqlExecutionContextImpl ctx;
        CairoEngine engine;
        @Param({
                // Core covering queries
                "covering_where", "covering_agg", "covering_sum", "covering_count",
                "latest_on", "in_list",
                // Residual filter variants (CoveringFilterBenchmark)
                "residual_filter", "non_covering_filter", "no_index_filter",
                "residual_filter_in", "non_covering_filter_in",
                // VARCHAR/FSST variants (CoveringVarcharBenchmark)
                "varchar_fsst", "varchar_non_covering", "varchar_in_covering",
                // Wide table, O3, non-covering baseline
                "wide_table", "o3_covering", "o3_non_covering", "o3_distinct",
                "non_covering_where",
                // Bulk throughput (CoveringQueryBenchmark S6)
                "bulk_covering", "bulk_non_covering"
        })
        String queryType;
        String sql;
        java.nio.file.Path tmpDir;

        @Setup(Level.Trial)
        public void setup() throws Exception {
            tmpDir = Files.createTempDirectory("suite-sql");
            CairoConfiguration config = new DefaultCairoConfiguration(tmpDir.toString()) {
                @Override
                public byte getPostingIndexRowIdEncoding() {
                    return IS_DELTA ? PostingIndexUtils.ENCODING_DELTA : PostingIndexUtils.ENCODING_ADAPTIVE;
                }

                @Override
                public int getRndFunctionMemoryMaxPages() {
                    return 4096;
                }
            };
            engine = new CairoEngine(config);
            ctx = new SqlExecutionContextImpl(engine, 1)
                    .with(config.getFactoryProvider().getSecurityContextFactory().getRootContext(),
                            null, null, -1, null);
            compiler = new SqlCompilerImpl(engine);

            // Core table: sym with covering index on price (200 keys, 400K rows)
            engine.execute("CREATE TABLE bench (" +
                    "ts TIMESTAMP, sym SYMBOL INDEX TYPE " + POSTING_SQL + " INCLUDE (price), price DOUBLE" +
                    ") TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL", ctx);
            engine.execute("INSERT INTO bench SELECT dateadd('T', x::INT, '2024-01-01')::TIMESTAMP, " +
                    "rnd_symbol(200, 4, 8, 0), rnd_double() * 1000 FROM long_sequence(400000)", ctx);

            // No-index version of bench for full-scan filter comparison
            engine.execute("CREATE TABLE bench_noidx (" +
                    "ts TIMESTAMP, sym SYMBOL, price DOUBLE" +
                    ") TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL", ctx);
            engine.execute("INSERT INTO bench_noidx SELECT dateadd('T', x::INT, '2024-01-01')::TIMESTAMP, " +
                    "rnd_symbol(200, 4, 8, 0), rnd_double() * 1000 FROM long_sequence(400000)", ctx);

            // Non-covering version of bench (bitmap index, no INCLUDE)
            engine.execute("CREATE TABLE bench_nc (" +
                    "ts TIMESTAMP, sym SYMBOL INDEX, price DOUBLE" +
                    ") TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL", ctx);
            engine.execute("INSERT INTO bench_nc SELECT dateadd('T', x::INT, '2024-01-01')::TIMESTAMP, " +
                    "rnd_symbol(200, 4, 8, 0), rnd_double() * 1000 FROM long_sequence(400000)", ctx);

            // Wide table: 8 columns, covering 2 (200 keys, 200K rows)
            engine.execute("CREATE TABLE wide (" +
                    "ts TIMESTAMP, sym SYMBOL INDEX TYPE " + POSTING_SQL + " INCLUDE (c1, c2), " +
                    "c1 DOUBLE, c2 INT, c3 DOUBLE, c4 INT, c5 DOUBLE, c6 INT, c7 DOUBLE, c8 INT" +
                    ") TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL", ctx);
            engine.execute("INSERT INTO wide SELECT dateadd('T', x::INT, '2024-01-01')::TIMESTAMP, " +
                    "rnd_symbol(200, 4, 8, 0), rnd_double()*100, rnd_int(0,1000,0), " +
                    "rnd_double()*100, rnd_int(0,1000,0), rnd_double()*100, rnd_int(0,1000,0), " +
                    "rnd_double()*100, rnd_int(0,1000,0) FROM long_sequence(200000)", ctx);

            // VARCHAR/FSST table: covering includes VARCHAR (200 keys, 200K rows)
            engine.execute("CREATE TABLE vchar (" +
                    "ts TIMESTAMP, sym SYMBOL INDEX TYPE " + POSTING_SQL + " INCLUDE (name, price), " +
                    "name VARCHAR, price DOUBLE" +
                    ") TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL", ctx);
            engine.execute("INSERT INTO vchar SELECT dateadd('T', x::INT, '2024-01-01')::TIMESTAMP, " +
                    "rnd_symbol(200, 4, 8, 0), rnd_varchar(10, 30, 0), rnd_double() * 1000 " +
                    "FROM long_sequence(200000)", ctx);

            // Bulk table: few keys, many rows per key — tests sustained output throughput
            // 20 keys × 50K rows = 1M rows (covering VARCHAR + DOUBLE, FSST compressed)
            engine.execute("CREATE TABLE bulk (" +
                    "ts TIMESTAMP, sym SYMBOL INDEX TYPE " + POSTING_SQL + " INCLUDE (name, price), " +
                    "name VARCHAR, price DOUBLE" +
                    ") TIMESTAMP(ts) PARTITION BY HOUR BYPASS WAL", ctx);
            engine.execute("INSERT INTO bulk SELECT dateadd('T', x::INT, '2024-01-01')::TIMESTAMP, " +
                    "rnd_symbol(20, 4, 8, 0), rnd_varchar(10, 30, 0), rnd_double() * 1000 " +
                    "FROM long_sequence(1000000)", ctx);

            // O3 table: in-order insert then out-of-order insert
            engine.execute("CREATE TABLE o3bench (" +
                    "ts TIMESTAMP, sym SYMBOL INDEX TYPE " + POSTING_SQL + " INCLUDE (price), price DOUBLE" +
                    ") TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL", ctx);
            engine.execute("INSERT INTO o3bench SELECT dateadd('s', x::INT, '2024-01-01')::TIMESTAMP, " +
                    "rnd_symbol(50, 4, 8, 0), rnd_double() * 1000 FROM long_sequence(100000)", ctx);
            // O3 insert: timestamps interleaved with existing data
            engine.execute("INSERT INTO o3bench SELECT dateadd('s', x::INT, '2024-01-01T00:00:00.500000')::TIMESTAMP, " +
                    "rnd_symbol(50, 4, 8, 0), rnd_double() * 1000 FROM long_sequence(100000)", ctx);

            engine.releaseAllWriters();

            String key = resolveKey(compiler, ctx, "bench");
            String ncKey = resolveKey(compiler, ctx, "bench_nc");
            String niKey = resolveKey(compiler, ctx, "bench_noidx");
            String wideKey = resolveKey(compiler, ctx, "wide");
            String vcharKey = resolveKey(compiler, ctx, "vchar");
            String o3Key = resolveKey(compiler, ctx, "o3bench");
            String bulkKey = resolveKey(compiler, ctx, "bulk");
            String tenKeys = resolveNKeys(compiler, ctx, "bench", 10);
            String tenNcKeys = resolveNKeys(compiler, ctx, "bench_nc", 10);

            sql = switch (queryType) {
                // Core covering
                case "covering_where" -> "SELECT price FROM bench WHERE sym = '" + key + "'";
                case "covering_agg" -> "SELECT avg(price) FROM bench WHERE sym = '" + key + "'";
                case "covering_sum" -> "SELECT sum(price) FROM bench WHERE sym = '" + key + "'";
                case "covering_count" -> "SELECT count() FROM bench WHERE sym = '" + key + "'";
                case "latest_on" -> "SELECT ts, sym, price FROM bench LATEST ON ts PARTITION BY sym";
                case "in_list" -> "SELECT price FROM bench WHERE sym IN (" + tenKeys + ")";
                // Residual filter variants
                case "residual_filter" -> "SELECT price FROM bench WHERE sym = '" + key + "' AND price > 500";
                case "non_covering_filter" -> "SELECT price FROM bench_nc WHERE sym = '" + ncKey + "' AND price > 500";
                case "no_index_filter" -> "SELECT price FROM bench_noidx WHERE sym = '" + niKey + "' AND price > 500";
                case "residual_filter_in" -> "SELECT price FROM bench WHERE sym IN (" + tenKeys + ") AND price > 500";
                case "non_covering_filter_in" ->
                        "SELECT price FROM bench_nc WHERE sym IN (" + tenNcKeys + ") AND price > 500";
                // VARCHAR/FSST variants
                case "varchar_fsst" -> "SELECT name, price FROM vchar WHERE sym = '" + vcharKey + "'";
                case "varchar_non_covering" -> "SELECT ts, name, price FROM vchar WHERE sym = '" + vcharKey + "'";
                case "varchar_in_covering" ->
                        "SELECT name, price FROM vchar WHERE sym IN (" + resolveNKeys(compiler, ctx, "vchar", 5) + ")";
                // Wide table, O3, non-covering baseline
                case "wide_table" -> "SELECT c1, c2 FROM wide WHERE sym = '" + wideKey + "'";
                case "o3_covering" -> "SELECT price FROM o3bench WHERE sym = '" + o3Key + "'";
                case "o3_non_covering" -> "SELECT ts FROM o3bench WHERE sym = '" + o3Key + "'";
                case "o3_distinct" -> "SELECT DISTINCT sym FROM o3bench";
                case "non_covering_where" -> "SELECT ts FROM bench WHERE sym = '" + key + "'";
                // Bulk throughput
                case "bulk_covering" -> "SELECT name, price FROM bulk WHERE sym = '" + bulkKey + "'";
                case "bulk_non_covering" -> "SELECT ts, name, price FROM bulk WHERE sym = '" + bulkKey + "'";
                default -> throw new IllegalArgumentException(queryType);
            };
        }

        @TearDown(Level.Trial)
        public void tearDown() {
            Misc.free(compiler);
            Misc.free(engine);
            deleteDirRecursive(tmpDir.toFile());
        }
    }

    @State(Scope.Benchmark)
    @BenchmarkMode(Mode.SingleShotTime)
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    public static class WriteState {
        SqlCompilerImpl compiler;
        SqlExecutionContextImpl ctx;
        String ddl;
        CairoEngine engine;
        @Param({"no_index", "bitmap", "posting", "posting_covering", "posting_varchar"})
        String indexType;
        String insertSql;
        java.nio.file.Path tmpDir;

        @Setup(Level.Trial)
        public void setup() throws Exception {
            tmpDir = Files.createTempDirectory("suite-write");
            CairoConfiguration config = new DefaultCairoConfiguration(tmpDir.toString()) {
                @Override
                public byte getPostingIndexRowIdEncoding() {
                    return IS_DELTA ? PostingIndexUtils.ENCODING_DELTA : PostingIndexUtils.ENCODING_ADAPTIVE;
                }

                @Override
                public int getRndFunctionMemoryMaxPages() {
                    return 4096;
                }
            };
            engine = new CairoEngine(config);
            ctx = new SqlExecutionContextImpl(engine, 1)
                    .with(config.getFactoryProvider().getSecurityContextFactory().getRootContext(),
                            null, null, -1, null);
            compiler = new SqlCompilerImpl(engine);

            ddl = switch (indexType) {
                case "no_index" -> "CREATE TABLE wbench (ts TIMESTAMP, sym SYMBOL, price DOUBLE, name VARCHAR) " +
                        "TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL";
                case "bitmap" -> "CREATE TABLE wbench (ts TIMESTAMP, sym SYMBOL INDEX, price DOUBLE, name VARCHAR) " +
                        "TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL";
                case "posting" ->
                        "CREATE TABLE wbench (ts TIMESTAMP, sym SYMBOL INDEX TYPE " + POSTING_SQL + ", price DOUBLE, name VARCHAR) " +
                                "TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL";
                case "posting_covering" ->
                        "CREATE TABLE wbench (ts TIMESTAMP, sym SYMBOL INDEX TYPE " + POSTING_SQL + " INCLUDE (price), " +
                                "price DOUBLE, name VARCHAR) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL";
                case "posting_varchar" ->
                        "CREATE TABLE wbench (ts TIMESTAMP, sym SYMBOL INDEX TYPE " + POSTING_SQL + " INCLUDE (price, name), " +
                                "price DOUBLE, name VARCHAR) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL";
                default -> throw new IllegalArgumentException(indexType);
            };
            insertSql = "INSERT INTO wbench SELECT dateadd('T', x::INT, '2024-01-01')::TIMESTAMP, " +
                    "rnd_symbol(50, 4, 8, 0), rnd_double() * 1000, rnd_varchar(10, 20, 0) " +
                    "FROM long_sequence(50000)";
        }

        @TearDown(Level.Trial)
        public void tearDown() {
            Misc.free(compiler);
            Misc.free(engine);
            deleteDirRecursive(tmpDir.toFile());
        }
    }
}
