package org.questdb;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.DefaultCairoConfiguration;
import io.questdb.cairo.idx.*;
import io.questdb.cairo.sql.RowCursor;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryMA;
import io.questdb.std.*;
import io.questdb.std.str.Path;

import java.io.File;
import java.util.*;

import static io.questdb.cairo.TableUtils.COLUMN_NAME_TXN_NONE;

/**
 * Comprehensive index comparison: Legacy vs Posting across all scenarios.
 * Measures storage (unsealed/sealed), read latency (point/scan/range, unsealed/sealed),
 * and write throughput. Prints per-scenario results and geomean summary.
 *
 * <pre>
 * mvn install -pl questdb/core -DskipTests -q
 * mvn package -pl questdb/benchmarks -DskipTests -q
 * java -Xmx8g -cp questdb/benchmarks/target/benchmarks.jar org.questdb.IndexComparisonBenchmark
 * </pre>
 */
public class IndexComparisonBenchmark {

    private static final long COL_TXN = COLUMN_NAME_TXN_NONE;
    private static final int READ_RUNS = 5;
    private static final int POINT_QUERY_KEYS = 10_000;
    private static final int RANGE_QUERY_KEYS = 1_000;

    enum Format {LEGACY, POSTING}

    static class Metrics {
        double writeTotalMs;
        double writePerCommitMs;
        double sizeUnsealedMB;
        double sizeSealedMB;
        double bPerValSealed;
        double readPointUnsealedMs;
        double readPointSealedMs;
        double readScanUnsealedMs;
        double readScanSealedMs;
        double readRangeUnsealedMs;
        double readRangeSealedMs;
    }

    static class ScenarioResult {
        String name;
        int totalRows;
        final Map<Format, Metrics> metrics = new EnumMap<>(Format.class);
    }

    // ========================= Scenarios =========================

    interface KeyAssignmentBuilder {
        int[] build();
    }

    public static void main(String[] args) {
        String tmpDir = System.getProperty("java.io.tmpdir");
        CairoConfiguration config = new DefaultCairoConfiguration(tmpDir);

        // Optional: pass scenario filter as args (e.g., "S1" "S6,S7")
        Set<String> scenarioFilter = new HashSet<>();
        for (String arg : args) {
            scenarioFilter.addAll(Arrays.asList(arg.split(",")));
        }

        Format[] formats = Format.values();
        List<ScenarioResult> results = new ArrayList<>();

        if (shouldRun(scenarioFilter, "S1"))
            results.add(runScenario(config, tmpDir, "S1: 5M keys x 4 v/k",
                    5_000_000, 20_000_000, 20_000_000,
                    () -> buildShuffled(20_000_000, 5_000_000), formats));

        if (shouldRun(scenarioFilter, "S2"))
            results.add(runScenario(config, tmpDir, "S2: 512 keys x 7K v/k (56 commits)",
                    512, 3_670_016, 65_536,
                    () -> buildRoundRobin(3_670_016, 512), formats));

        if (shouldRun(scenarioFilter, "S3"))
            results.add(runStreamingScenario(config, tmpDir, formats));

        if (shouldRun(scenarioFilter, "S4"))
            results.add(runScenario(config, tmpDir, "S4: 8K keys x 10K v/k",
                    8_000, 80_000_000, 80_000_000,
                    () -> buildRoundRobin(80_000_000, 8_000), formats));

        if (shouldRun(scenarioFilter, "S5"))
            results.add(runScenario(config, tmpDir, "S5: 1K keys, 10M rows, Zipfian",
                    1_000, 10_000_000, 10_000_000,
                    () -> buildZipfian(10_000_000, 1_000), formats));

        if (shouldRun(scenarioFilter, "S6"))
            results.add(runScenario(config, tmpDir, "S6: 2M keys x 6 v/k",
                    2_000_000, 12_000_000, 12_000_000,
                    () -> buildShuffled(12_000_000, 2_000_000), formats));

        if (shouldRun(scenarioFilter, "S7"))
            results.add(runScenario(config, tmpDir, "S7: 10M keys x 4 v/k",
                    10_000_000, 40_000_000, 40_000_000,
                    () -> buildShuffled(40_000_000, 10_000_000), formats));

        printSummary(results);
    }

    private static boolean shouldRun(Set<String> filter, String scenario) {
        return filter.isEmpty() || filter.stream().anyMatch(scenario::startsWith);
    }

    // ========================= Generic scenario runner =========================

    private static ScenarioResult runScenario(
            CairoConfiguration config, String tmpDir,
            String name, int keyCount, int totalRows, int commitInterval,
            KeyAssignmentBuilder assignmentBuilder, Format[] formats
    ) {
        System.out.printf("%n=== %s ===%n", name);
        System.out.printf("    %,d keys, %,d rows, commit every %,d rows%n", keyCount, totalRows, commitInterval);

        System.out.print("    Building key assignment... ");
        System.out.flush();
        long t0 = System.nanoTime();
        int[] keys = assignmentBuilder.build();
        System.out.printf("done in %.1f s%n%n", (System.nanoTime() - t0) / 1e9);

        int[] pointKeys = selectRandomKeys(keyCount, Math.min(POINT_QUERY_KEYS, keyCount));
        int[] rangeKeys = selectRandomKeys(keyCount, Math.min(RANGE_QUERY_KEYS, keyCount));

        ScenarioResult result = new ScenarioResult();
        result.name = name;
        result.totalRows = totalRows;

        for (Format fmt : formats) {
            String dir = tmpDir + File.separator + "cmp_" + fmt + "_" + System.nanoTime();
            new File(dir).mkdirs();
            try {
                Metrics m = runFormat(config, dir, fmt, keys, keyCount, totalRows, commitInterval, pointKeys, rangeKeys);
                result.metrics.put(fmt, m);
                printFormatLine(fmt, m);
            } finally {
                deleteDir(dir);
            }
        }
        return result;
    }

    private static Metrics runFormat(
            CairoConfiguration config, String dir, Format fmt,
            int[] keys, int keyCount, int totalRows, int commitInterval,
            int[] pointKeys, int[] rangeKeys
    ) {
        Metrics m = new Metrics();
        long maxRow = totalRows - 1;

        switch (fmt) {
            case LEGACY -> {
                // Legacy: no seal concept — write, close, measure once
                long wt0 = System.nanoTime();
                int blockCap = Math.max(8, Numbers.ceilPow2(totalRows / Math.max(keyCount, 1)));
                try (Path path = new Path().of(dir)) {
                    try (BitmapIndexWriter w = new BitmapIndexWriter(config)) {
                        w.of(path, "test", COL_TXN, blockCap);
                        for (int i = 0; i < totalRows; i++) w.add(keys[i], i);
                    }
                }
                m.writeTotalMs = (System.nanoTime() - wt0) / 1e6;
                m.writePerCommitMs = m.writeTotalMs; // single commit
                long size = getDirectorySize(dir);
                m.sizeUnsealedMB = size / (1024.0 * 1024.0);
                m.sizeSealedMB = m.sizeUnsealedMB;
                m.bPerValSealed = (double) size / totalRows;

                // Read
                m.readPointUnsealedMs = measureRead(config, dir, fmt, pointKeys);
                m.readPointSealedMs = m.readPointUnsealedMs;
                m.readScanUnsealedMs = measureScan(config, dir, fmt, keyCount);
                m.readScanSealedMs = m.readScanUnsealedMs;
                m.readRangeUnsealedMs = measureRange(config, dir, fmt, rangeKeys, maxRow / 4, maxRow * 3 / 4);
                m.readRangeSealedMs = m.readRangeUnsealedMs;
            }
            case POSTING -> {
                CairoConfiguration fmtConfig = configForFormat(config, fmt);
                initPosting(fmtConfig, dir);

                int commitCount = 0;
                long wt0 = System.nanoTime();
                PostingIndexWriter writer;
                try (Path path = new Path().of(dir)) {
                    writer = new PostingIndexWriter(fmtConfig);
                    writer.of(path, "test", COL_TXN, false);
                    for (int i = 0; i < totalRows; i++) {
                        writer.add(keys[i], i);
                        if ((i + 1) % commitInterval == 0) {
                            writer.commit();
                            commitCount++;
                        }
                    }
                    if (totalRows % commitInterval != 0) {
                        writer.commit();
                        commitCount++;
                    }
                }
                m.writeTotalMs = (System.nanoTime() - wt0) / 1e6;
                m.writePerCommitMs = commitCount > 0 ? m.writeTotalMs / commitCount : m.writeTotalMs;

                // Unsealed
                long unsealedSize = getDirectorySize(dir);
                m.sizeUnsealedMB = unsealedSize / (1024.0 * 1024.0);
                m.readPointUnsealedMs = measureRead(fmtConfig, dir, fmt, pointKeys);
                m.readScanUnsealedMs = measureScan(fmtConfig, dir, fmt, keyCount);
                m.readRangeUnsealedMs = measureRange(fmtConfig, dir, fmt, rangeKeys, maxRow / 4, maxRow * 3 / 4);

                // Seal + compact (close triggers both)
                writer.close();
                long sealedSize = getDirectorySize(dir);
                m.sizeSealedMB = sealedSize / (1024.0 * 1024.0);
                m.bPerValSealed = (double) sealedSize / totalRows;

                // Sealed reads
                m.readPointSealedMs = measureRead(fmtConfig, dir, fmt, pointKeys);
                m.readScanSealedMs = measureScan(fmtConfig, dir, fmt, keyCount);
                m.readRangeSealedMs = measureRange(fmtConfig, dir, fmt, rangeKeys, maxRow / 4, maxRow * 3 / 4);
            }
        }
        return m;
    }

    // ========================= Streaming scenario (custom commit pattern) =========================

    private static ScenarioResult runStreamingScenario(CairoConfiguration config, String tmpDir, Format[] formats) {
        int keyCount = 50_000;
        int commits = 500;
        int activePerCommit = 1_000; // 2%
        int valsPerActive = 10;

        System.out.printf("%n=== S3: Streaming (50K keys, 500 commits, 2%% activity) ===%n");
        System.out.printf("    %,d keys, %d commits, %d active/commit, %d vals/active%n", keyCount, commits, activePerCommit, valsPerActive);

        Rnd rnd = new Rnd(12345, 67890);
        int[][] activeKeys = new int[commits][];
        int totalRows = 0;
        for (int c = 0; c < commits; c++) {
            int[] pool = new int[keyCount];
            for (int i = 0; i < keyCount; i++) pool[i] = i;
            for (int i = 0; i < activePerCommit; i++) {
                int j = i + rnd.nextPositiveInt() % (keyCount - i);
                int tmp = pool[i]; pool[i] = pool[j]; pool[j] = tmp;
            }
            activeKeys[c] = Arrays.copyOf(pool, activePerCommit);
            Arrays.sort(activeKeys[c]);
            totalRows += activePerCommit * valsPerActive;
        }
        final int finalTotalRows = totalRows;
        long maxRow = totalRows - 1;

        int[] pointKeys = selectRandomKeys(keyCount, POINT_QUERY_KEYS);
        int[] rangeKeys = selectRandomKeys(keyCount, RANGE_QUERY_KEYS);
        System.out.printf("    total rows: %,d%n%n", totalRows);

        ScenarioResult result = new ScenarioResult();
        result.name = "S3: Streaming (50K, 500 commits)";
        result.totalRows = totalRows;

        for (Format fmt : formats) {
            String dir = tmpDir + File.separator + "cmp_st_" + fmt + "_" + System.nanoTime();
            new File(dir).mkdirs();
            try {
                Metrics m = runStreamingFormat(config, dir, fmt, activeKeys, commits, keyCount,
                        valsPerActive, finalTotalRows, maxRow, pointKeys, rangeKeys);
                result.metrics.put(fmt, m);
                printFormatLine(fmt, m);
            } finally {
                deleteDir(dir);
            }
        }
        return result;
    }

    private static Metrics runStreamingFormat(
            CairoConfiguration config, String dir, Format fmt,
            int[][] activeKeys, int commits, int keyCount, int valsPerActive,
            int totalRows, long maxRow, int[] pointKeys, int[] rangeKeys
    ) {
        Metrics m = new Metrics();

        switch (fmt) {
            case LEGACY -> {
                long wt0 = System.nanoTime();
                try (Path path = new Path().of(dir)) {
                    try (BitmapIndexWriter w = new BitmapIndexWriter(config)) {
                        w.of(path, "test", COL_TXN, 64);
                        int rowId = 0;
                        for (int c = 0; c < commits; c++) {
                            for (int key : activeKeys[c]) {
                                for (int v = 0; v < valsPerActive; v++) w.add(key, rowId++);
                            }
                        }
                    }
                }
                m.writeTotalMs = (System.nanoTime() - wt0) / 1e6;
                m.writePerCommitMs = m.writeTotalMs / commits;
                long size = getDirectorySize(dir);
                m.sizeUnsealedMB = m.sizeSealedMB = size / (1024.0 * 1024.0);
                m.bPerValSealed = (double) size / totalRows;
                m.readPointUnsealedMs = m.readPointSealedMs = measureRead(config, dir, fmt, pointKeys);
                m.readScanUnsealedMs = m.readScanSealedMs = measureScan(config, dir, fmt, keyCount);
                m.readRangeUnsealedMs = m.readRangeSealedMs = measureRange(config, dir, fmt, rangeKeys, maxRow / 4, maxRow * 3 / 4);
            }
            case POSTING -> {
                CairoConfiguration fmtConfig = configForFormat(config, fmt);
                initPosting(fmtConfig, dir);
                long wt0 = System.nanoTime();
                PostingIndexWriter writer;
                try (Path path = new Path().of(dir)) {
                    writer = new PostingIndexWriter(fmtConfig);
                    writer.of(path, "test", COL_TXN, false);
                    int rowId = 0;
                    for (int c = 0; c < commits; c++) {
                        for (int key : activeKeys[c]) {
                            for (int v = 0; v < valsPerActive; v++) writer.add(key, rowId++);
                        }
                        writer.commit();
                    }
                }
                m.writeTotalMs = (System.nanoTime() - wt0) / 1e6;
                m.writePerCommitMs = m.writeTotalMs / commits;
                m.sizeUnsealedMB = getDirectorySize(dir) / (1024.0 * 1024.0);
                m.readPointUnsealedMs = measureRead(fmtConfig, dir, fmt, pointKeys);
                m.readScanUnsealedMs = measureScan(fmtConfig, dir, fmt, keyCount);
                m.readRangeUnsealedMs = measureRange(fmtConfig, dir, fmt, rangeKeys, maxRow / 4, maxRow * 3 / 4);
                writer.close(); // seal + compact
                long sealedSize = getDirectorySize(dir);
                m.sizeSealedMB = sealedSize / (1024.0 * 1024.0);
                m.bPerValSealed = (double) sealedSize / totalRows;
                m.readPointSealedMs = measureRead(fmtConfig, dir, fmt, pointKeys);
                m.readScanSealedMs = measureScan(fmtConfig, dir, fmt, keyCount);
                m.readRangeSealedMs = measureRange(fmtConfig, dir, fmt, rangeKeys, maxRow / 4, maxRow * 3 / 4);
            }
        }
        return m;
    }

    // ========================= Read measurements =========================

    private static double measureRead(CairoConfiguration config, String dir, Format fmt, int[] keys) {
        // Warmup
        doRead(config, dir, fmt, keys);
        long total = 0;
        for (int i = 0; i < READ_RUNS; i++) {
            long t0 = System.nanoTime();
            doRead(config, dir, fmt, keys);
            total += System.nanoTime() - t0;
        }
        return total / (READ_RUNS * 1e6);
    }

    private static double measureScan(CairoConfiguration config, String dir, Format fmt, int keyCount) {
        doScan(config, dir, fmt, keyCount);
        long total = 0;
        for (int i = 0; i < READ_RUNS; i++) {
            long t0 = System.nanoTime();
            doScan(config, dir, fmt, keyCount);
            total += System.nanoTime() - t0;
        }
        return total / (READ_RUNS * 1e6);
    }

    private static double measureRange(CairoConfiguration config, String dir, Format fmt, int[] keys, long minRow, long maxRow) {
        doRange(config, dir, fmt, keys, minRow, maxRow);
        long total = 0;
        for (int i = 0; i < READ_RUNS; i++) {
            long t0 = System.nanoTime();
            doRange(config, dir, fmt, keys, minRow, maxRow);
            total += System.nanoTime() - t0;
        }
        return total / (READ_RUNS * 1e6);
    }

    private static void doRead(CairoConfiguration config, String dir, Format fmt, int[] keys) {
        try (Path path = new Path().of(dir)) {
            BitmapIndexReader reader = openReader(config, path, fmt);
            try {
                for (int key : keys) {
                    RowCursor c = reader.getCursor(true, key, 0, Long.MAX_VALUE);
                    while (c.hasNext()) c.next();
                }
            } finally {
                Misc.free(reader);
            }
        }
    }

    private static void doScan(CairoConfiguration config, String dir, Format fmt, int keyCount) {
        try (Path path = new Path().of(dir)) {
            BitmapIndexReader reader = openReader(config, path, fmt);
            try {
                for (int key = 0; key < keyCount; key++) {
                    RowCursor c = reader.getCursor(true, key, 0, Long.MAX_VALUE);
                    while (c.hasNext()) c.next();
                }
            } finally {
                Misc.free(reader);
            }
        }
    }

    private static void doRange(CairoConfiguration config, String dir, Format fmt, int[] keys, long minRow, long maxRow) {
        try (Path path = new Path().of(dir)) {
            BitmapIndexReader reader = openReader(config, path, fmt);
            try {
                for (int key : keys) {
                    RowCursor c = reader.getCursor(true, key, minRow, maxRow);
                    while (c.hasNext()) c.next();
                }
            } finally {
                Misc.free(reader);
            }
        }
    }

    private static CairoConfiguration configForFormat(CairoConfiguration base, Format fmt) {
        return base;
    }

    private static BitmapIndexReader openReader(CairoConfiguration config, Path path, Format fmt) {
        return switch (fmt) {
            case LEGACY -> new BitmapIndexFwdReader(config, path, "test", COL_TXN, -1, 0);
            case POSTING -> new PostingIndexFwdReader(config, path, "test", COL_TXN, -1, 0);
        };
    }

    private static void initPosting(CairoConfiguration config, String dir) {
        try (Path path = new Path().of(dir)) {
            int plen = path.size();
            FilesFacade ff = config.getFilesFacade();
            try (MemoryMA mem = Vm.getSmallCMARWInstance(ff, PostingIndexUtils.keyFileName(path, "test", COL_TXN),
                    MemoryTag.MMAP_DEFAULT, config.getWriterFileOpenOpts())) {
                PostingIndexWriter.initKeyMemory(mem, PostingIndexUtils.BLOCK_CAPACITY);
            }
            ff.touch(PostingIndexUtils.valueFileName(path.trimTo(plen), "test", COL_TXN));
        }
    }

    // ========================= Key assignment builders =========================

    private static int[] buildShuffled(int totalRows, int keyCount) {
        int[] a = new int[totalRows];
        for (int i = 0; i < totalRows; i++) a[i] = i % keyCount;
        Random rng = new Random(42);
        for (int i = totalRows - 1; i > 0; i--) {
            int j = rng.nextInt(i + 1);
            int tmp = a[i]; a[i] = a[j]; a[j] = tmp;
        }
        return a;
    }

    private static int[] buildRoundRobin(int totalRows, int keyCount) {
        int[] a = new int[totalRows];
        for (int i = 0; i < totalRows; i++) a[i] = i % keyCount;
        return a;
    }

    private static int[] buildZipfian(int totalRows, int keyCount) {
        double[] cdf = new double[keyCount];
        double sum = 0;
        for (int i = 0; i < keyCount; i++) { sum += 1.0 / (i + 1); cdf[i] = sum; }
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

    private static int[] selectRandomKeys(int keyCount, int n) {
        int[] keys = new int[n];
        Random rng = new Random(99);
        for (int i = 0; i < n; i++) keys[i] = rng.nextInt(keyCount);
        return keys;
    }

    // ========================= Output =========================

    private static void printFormatLine(Format fmt, Metrics m) {
        System.out.printf("    %-16s size: %6.1f/%6.1f MB (unsealed/sealed)  B/val: %4.1f  write: %6.0f ms  " +
                        "point: %5.1f/%5.1f  scan: %6.1f/%6.1f  range: %5.1f/%5.1f ms%n",
                fmt, m.sizeUnsealedMB, m.sizeSealedMB, m.bPerValSealed, m.writeTotalMs,
                m.readPointUnsealedMs, m.readPointSealedMs,
                m.readScanUnsealedMs, m.readScanSealedMs,
                m.readRangeUnsealedMs, m.readRangeSealedMs);
    }

    private static void printSummary(List<ScenarioResult> results) {
        if (results.isEmpty()) return;
        // Derive formats from what was actually run
        Format[] formats = results.get(0).metrics.keySet().stream()
                .sorted(Comparator.comparingInt(Enum::ordinal))
                .toArray(Format[]::new);

        System.out.printf("%n%n");
        System.out.println("=".repeat(160));
        System.out.println("SUMMARY");
        System.out.println("=".repeat(160));

        // Header
        System.out.printf("%n%-38s", "");
        for (Format f : formats) {
            System.out.printf(" %15s", f);
        }
        System.out.println();

        printMetricSection(results, formats, "Size Sealed (MB)", "MB", m -> m.sizeSealedMB);
        printMetricSection(results, formats, "Size Unsealed (MB)", "MB", m -> m.sizeUnsealedMB);
        printMetricSection(results, formats, "Bytes/Value (sealed)", "   ", m -> m.bPerValSealed);
        printMetricSection(results, formats, "Write Total (ms)", "ms", m -> m.writeTotalMs);
        printMetricSection(results, formats, "Point Sealed (ms)", "ms", m -> m.readPointSealedMs);
        printMetricSection(results, formats, "Point Unsealed (ms)", "ms", m -> m.readPointUnsealedMs);
        printMetricSection(results, formats, "Scan Sealed (ms)", "ms", m -> m.readScanSealedMs);
        printMetricSection(results, formats, "Scan Unsealed (ms)", "ms", m -> m.readScanUnsealedMs);
        printMetricSection(results, formats, "Range Sealed (ms)", "ms", m -> m.readRangeSealedMs);
        printMetricSection(results, formats, "Range Unsealed (ms)", "ms", m -> m.readRangeUnsealedMs);
    }

    interface MetricExtractor {
        double extract(Metrics m);
    }

    private static void printMetricSection(List<ScenarioResult> results, Format[] formats,
                                            String title, String unit, MetricExtractor extractor) {
        System.out.printf("%n--- %s ---%n", title);
        double[] geo = new double[formats.length];
        Arrays.fill(geo, 1.0);
        for (ScenarioResult r : results) {
            System.out.printf("  %-36s", r.name);
            for (int i = 0; i < formats.length; i++) {
                Metrics m = r.metrics.get(formats[i]);
                double val = extractor.extract(m);
                System.out.printf(" %10.1f %s ", val, unit);
                geo[i] *= val;
            }
            System.out.println();
        }
        System.out.printf("%-38s", "  Geomean");
        for (int i = 0; i < formats.length; i++) {
            System.out.printf(" %10.1f %s ", Math.pow(geo[i], 1.0 / results.size()), unit);
        }
        System.out.println();
    }

    // ========================= Utilities =========================

    private static long getDirectorySize(String path) {
        File dir = new File(path);
        long size = 0;
        File[] files = dir.listFiles();
        if (files != null) for (File f : files) size += f.length();
        return size;
    }

    private static void deleteDir(String path) {
        File dir = new File(path);
        if (dir.exists()) {
            File[] files = dir.listFiles();
            if (files != null) for (File f : files) f.delete();
            dir.delete();
        }
    }
}
