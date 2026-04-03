package org.questdb;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.DefaultCairoConfiguration;
import io.questdb.cairo.idx.PostingIndexFwdReader;
import io.questdb.cairo.idx.PostingIndexUtils;
import io.questdb.cairo.idx.PostingIndexWriter;
import io.questdb.cairo.idx.BitmapIndexFwdReader;
import io.questdb.cairo.idx.BitmapIndexWriter;
import io.questdb.cairo.sql.RowCursor;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryMA;
import io.questdb.std.FilesFacade;
import io.questdb.std.MemoryTag;
import io.questdb.std.str.Path;

import java.io.File;
import java.util.Random;

import static io.questdb.cairo.TableUtils.COLUMN_NAME_TXN_NONE;

/**
 * Focused benchmark: 30M keys x 4 vals/key, plus market-data write profiling.
 *
 * <pre>
 * mvn package -pl questdb/core,questdb/benchmarks -DskipTests
 * java -Xmx4g -cp questdb/benchmarks/target/benchmarks.jar org.questdb.IndexScenarioBenchmark
 * </pre>
 */
public class IndexScenarioBenchmark {

    private static final long COLUMN_NAME_TXN = COLUMN_NAME_TXN_NONE;

    // === Scenario 1: 30M high-cardinality ===
    private static final int HC_KEY_COUNT = 30_000_000;
    private static final int HC_VALUES_PER_KEY = 4;
    private static final int HC_TOTAL_ROWS = HC_KEY_COUNT * HC_VALUES_PER_KEY;
    private static final int HC_BLOCK_VALUES = 4;
    private static final int HC_READ_BATCH = 10_000;
    private static final int HC_LEGACY_BLOCK_CAPACITY = 8;

    // === Scenario 2: Market data ===
    private static final int MD_KEY_COUNT = 512;
    private static final int MD_BLOCK_VALUES = 128;
    private static final int MD_COMMITS = 56;
    private static final int MD_VALUES_PER_KEY = MD_BLOCK_VALUES * MD_COMMITS;
    private static final int MD_ROWS_PER_COMMIT = MD_KEY_COUNT * MD_BLOCK_VALUES;
    private static final int MD_TOTAL_ROWS = MD_KEY_COUNT * MD_VALUES_PER_KEY;

    // === Scenario 3: Streaming (sparse, many small commits) ===
    private static final int ST_KEY_COUNT = 50_000;
    private static final int ST_COMMITS = 500;
    private static final double ST_KEY_ACTIVITY_RATIO = 0.02; // 2% of keys active per commit
    private static final int ST_VALUES_PER_ACTIVE_KEY = 10;
    private static final int ST_ACTIVE_KEYS_PER_COMMIT = (int) (ST_KEY_COUNT * ST_KEY_ACTIVITY_RATIO); // 1000
    private static final int ST_LEGACY_BLOCK_CAPACITY = 64;

    // === Scenario 4: Extreme high-cardinality (70M keys) ===
    private static final int XHC_KEY_COUNT = 70_000_000;
    private static final int XHC_VALUES_PER_KEY = 6;
    private static final int XHC_TOTAL_ROWS = XHC_KEY_COUNT * XHC_VALUES_PER_KEY;
    private static final int XHC_READ_BATCH = 10_000;

    // === Scenario 5: Dense instruments (8K keys x 10K rows) ===
    private static final int DI_KEY_COUNT = 8_000;
    private static final int DI_VALUES_PER_KEY = 10_000;
    private static final int DI_TOTAL_ROWS = DI_KEY_COUNT * DI_VALUES_PER_KEY;
    private static final int DI_READ_BATCH = 8_000;

    // === Scenario 6: Zipfian (1K keys, 10M rows) ===
    private static final int ZF_KEY_COUNT = 1_000;
    private static final int ZF_TOTAL_ROWS = 10_000_000;
    private static final int ZF_READ_BATCH = 1_000;

    public static void main(String[] args) {
        String tmpDir = System.getProperty("java.io.tmpdir");
        CairoConfiguration config = new DefaultCairoConfiguration(tmpDir);

        boolean runAll = args.length == 0 || "all".equals(args[0]);

        if (runAll || "1".equals(args[0])) {
            runHighCardinality(config, tmpDir);
            System.out.println();
        }
        if (runAll || "2".equals(args[0])) {
            runMarketDataWrites(config, tmpDir);
            System.out.println();
        }
        if (runAll || "3".equals(args[0])) {
            runStreaming(config, tmpDir);
            System.out.println();
        }
        if (runAll || "4".equals(args[0])) {
            runExtremeHighCardinality(config, tmpDir);
            System.out.println();
        }
        if (runAll || "5".equals(args[0])) {
            runDenseInstruments(config, tmpDir);
            System.out.println();
        }
        if (runAll || "6".equals(args[0])) {
            runZipfian(config, tmpDir);
        }
    }

    private static void runHighCardinality(CairoConfiguration config, String tmpDir) {
        System.out.printf("=== Scenario 1: 30M keys x %d vals/key = %,d total rows ===%n%n", HC_VALUES_PER_KEY, HC_TOTAL_ROWS);

        System.out.println("Building key assignment (full random shuffle)...");
        long t0 = System.nanoTime();
        int[] keyAssignment = buildRandomAssignment(HC_TOTAL_ROWS, HC_KEY_COUNT);
        System.out.printf("  done in %.1f s%n%n", (System.nanoTime() - t0) / 1e9);

        int[] readKeys = new int[HC_READ_BATCH];
        Random rng = new Random(99);
        for (int i = 0; i < HC_READ_BATCH; i++) {
            readKeys[i] = rng.nextInt(HC_KEY_COUNT);
        }

        String legacyDir = tmpDir + File.separator + "sc_legacy_" + System.nanoTime();
        String postingDir = tmpDir + File.separator + "sc_posting_" + System.nanoTime();

        new File(legacyDir).mkdirs();
        new File(postingDir).mkdirs();

        try {
            System.out.println("Write time (add + flush):");

            // Legacy
            System.out.printf("  %-30s ... ", "Legacy (block=8)");
            System.out.flush();
            t0 = System.nanoTime();
            try (Path path = new Path().of(legacyDir)) {
                try (BitmapIndexWriter writer = new BitmapIndexWriter(config)) {
                    writer.of(path, "test", COLUMN_NAME_TXN, HC_LEGACY_BLOCK_CAPACITY);
                    for (int rowId = 0; rowId < HC_TOTAL_ROWS; rowId++) writer.add(keyAssignment[rowId], rowId);
                }
            }
            long legacySize = getDirectorySize(legacyDir);
            System.out.printf("%7.1f MB in %5.2f s%n", legacySize / (1024.0 * 1024.0), (System.nanoTime() - t0) / 1e9);

            // Posting
            System.out.printf("  %-30s ... ", "Posting");
            System.out.flush();
            t0 = System.nanoTime();
            createPostingIndex(config, postingDir, HC_BLOCK_VALUES);
            try (Path path = new Path().of(postingDir)) {
                try (PostingIndexWriter writer = new PostingIndexWriter(config)) {
                    writer.of(path, "test", COLUMN_NAME_TXN, false);
                    for (int rowId = 0; rowId < HC_TOTAL_ROWS; rowId++) writer.add(keyAssignment[rowId], rowId);
                }
            }
            long postingSize = getDirectorySize(postingDir);
            System.out.printf("%7.1f MB in %5.2f s%n", postingSize / (1024.0 * 1024.0), (System.nanoTime() - t0) / 1e9);

            // Summary
            System.out.println();
            System.out.printf("  %-20s %10s %10s%n", "Format", "Size (MB)", "vs Legacy");
            System.out.println("  " + "-".repeat(42));
            printRow("Legacy", legacySize, legacySize);
            printRow("Posting", postingSize, legacySize);

            // Read latency
            System.out.println();
            System.out.printf("Read latency (%,d random keys, %d values each):%n", HC_READ_BATCH, HC_VALUES_PER_KEY);

            measureReadLatency("Legacy", () -> {
                try (Path path = new Path().of(legacyDir)) {
                    try (BitmapIndexFwdReader reader = new BitmapIndexFwdReader(config, path, "test", COLUMN_NAME_TXN, -1, 0)) {
                        return readBatch(reader, readKeys);
                    }
                }
            });
            measureReadLatency("Posting", () -> {
                try (Path path = new Path().of(postingDir)) {
                    try (PostingIndexFwdReader reader = new PostingIndexFwdReader(config, path, "test", COLUMN_NAME_TXN, -1, 0)) {
                        return readBatch(reader, readKeys);
                    }
                }
            });
        } finally {
            deleteDir(legacyDir);
            deleteDir(postingDir);
        }
    }

    // ========================= Scenario 2: Market Data Write Profiling =========================

    private static void runMarketDataWrites(CairoConfiguration config, String tmpDir) {
        System.out.printf("=== Scenario 2: Market Data Write Profiling ===%n");
        System.out.printf("    %d keys, %d commits of %d values/key each%n%n", MD_KEY_COUNT, MD_COMMITS, MD_BLOCK_VALUES);

        int[] keyAssignment = new int[MD_TOTAL_ROWS];
        for (int i = 0; i < MD_TOTAL_ROWS; i++) {
            keyAssignment[i] = i % MD_KEY_COUNT;
        }

        int[] readKeys = new int[MD_KEY_COUNT];
        for (int i = 0; i < MD_KEY_COUNT; i++) readKeys[i] = i;

        // Posting incremental — measure per-commit breakdown + unsealed vs sealed
        String postingDir = tmpDir + File.separator + "md_posting_" + System.nanoTime();
        new File(postingDir).mkdirs();

        System.out.printf("  %-35s", "Posting (" + MD_COMMITS + " commits)");
        System.out.flush();
        long postingAddTotal = 0, postingCommitTotal = 0;
        createPostingIndex(config, postingDir, PostingIndexUtils.BLOCK_CAPACITY);
        PostingIndexWriter postingWriter;
        try (Path path = new Path().of(postingDir)) {
            postingWriter = new PostingIndexWriter(config);
            postingWriter.of(path, "test", COLUMN_NAME_TXN, false);
            for (int commit = 0; commit < MD_COMMITS; commit++) {
                long addT0 = System.nanoTime();
                int startRow = commit * MD_ROWS_PER_COMMIT;
                int endRow = startRow + MD_ROWS_PER_COMMIT;
                for (int rowId = startRow; rowId < endRow; rowId++) {
                    postingWriter.add(keyAssignment[rowId], rowId);
                }
                long addT1 = System.nanoTime();
                postingAddTotal += addT1 - addT0;

                postingWriter.commit();
                postingCommitTotal += System.nanoTime() - addT1;
            }
        }
        long postingUnsealedSize = getDirectorySize(postingDir);
        System.out.printf("  %7.1f MB in %6.1f ms%n", postingUnsealedSize / (1024.0 * 1024.0), (postingAddTotal + postingCommitTotal) / 1e6);
        System.out.printf("    add():    %6.1f ms  (%.0f%%)%n", postingAddTotal / 1e6, 100.0 * postingAddTotal / (postingAddTotal + postingCommitTotal));
        System.out.printf("    commit(): %6.1f ms  (%.0f%%)%n",
                postingCommitTotal / 1e6, 100.0 * postingCommitTotal / (postingAddTotal + postingCommitTotal));
        System.out.printf("    per commit: %.2f ms%n", postingCommitTotal / (MD_COMMITS * 1e6));

        // Seal + compact
        long postingSealT0 = System.nanoTime();
        postingWriter.close(); // seal + compact
        long postingSealTime = System.nanoTime() - postingSealT0;
        long postingSealedSize = getDirectorySize(postingDir);
        System.out.printf("    seal():   %6.1f ms  -> %5.1f MB (was %.1f MB)%n",
                postingSealTime / 1e6, postingSealedSize / (1024.0 * 1024.0), postingUnsealedSize / (1024.0 * 1024.0));

        // Legacy for comparison
        String legacyDir = tmpDir + File.separator + "md_legacy_" + System.nanoTime();
        new File(legacyDir).mkdirs();
        System.out.printf("  %-35s", "Legacy (block=256)");
        System.out.flush();
        long t0 = System.nanoTime();
        try (Path path = new Path().of(legacyDir)) {
            try (BitmapIndexWriter writer = new BitmapIndexWriter(config)) {
                writer.of(path, "test", COLUMN_NAME_TXN, 256);
                for (int rowId = 0; rowId < MD_TOTAL_ROWS; rowId++) {
                    writer.add(keyAssignment[rowId], rowId);
                }
            }
        }
        long legacySize = getDirectorySize(legacyDir);
        System.out.printf("  %7.1f MB in %6.1f ms%n", legacySize / (1024.0 * 1024.0), (System.nanoTime() - t0) / 1e6);

        // Summary
        System.out.println();
        System.out.printf("  %-25s %10s %10s%n", "Format", "Size (MB)", "vs Legacy");
        System.out.println("  " + "-".repeat(47));
        printRow("Legacy", legacySize, legacySize);
        printRow("Posting unsealed (56 gens)", postingUnsealedSize, legacySize);
        printRow("Posting sealed (1 gen)", postingSealedSize, legacySize);

        // Read comparison — unsealed and sealed
        System.out.println();
        System.out.printf("  Read latency (all %d keys, %,d values each):%n", MD_KEY_COUNT, MD_VALUES_PER_KEY);

        measureReadLatency("Legacy", () -> {
            try (Path path = new Path().of(legacyDir)) {
                try (BitmapIndexFwdReader reader = new BitmapIndexFwdReader(config, path, "test", COLUMN_NAME_TXN, -1, 0)) {
                    return readBatch(reader, readKeys);
                }
            }
        });
        measureReadLatency("Posting sealed (1 gen)", () -> {
            try (Path path = new Path().of(postingDir)) {
                try (PostingIndexFwdReader reader = new PostingIndexFwdReader(config, path, "test", COLUMN_NAME_TXN, -1, 0)) {
                    return readBatch(reader, readKeys);
                }
            }
        });

        deleteDir(postingDir);
        deleteDir(legacyDir);
    }

    // ========================= Scenario 3: Streaming (sparse commits) =========================

    private static void runStreaming(CairoConfiguration config, String tmpDir) {
        System.out.printf("=== Scenario 3: Streaming (sparse commits, %d%% key activity) ===%n", (int) (ST_KEY_ACTIVITY_RATIO * 100));
        System.out.printf("    %,d keys, %d commits, %d active keys/commit, %d values/active key%n%n",
                ST_KEY_COUNT, ST_COMMITS, ST_ACTIVE_KEYS_PER_COMMIT, ST_VALUES_PER_ACTIVE_KEY);

        io.questdb.std.Rnd rnd = new io.questdb.std.Rnd(12345, 67890);
        int totalRows = 0;
        int[][] activeKeys = new int[ST_COMMITS][];
        for (int c = 0; c < ST_COMMITS; c++) {
            int[] keys = new int[ST_KEY_COUNT];
            for (int i = 0; i < ST_KEY_COUNT; i++) keys[i] = i;
            for (int i = 0; i < ST_ACTIVE_KEYS_PER_COMMIT; i++) {
                int j = i + rnd.nextPositiveInt() % (ST_KEY_COUNT - i);
                int tmp = keys[i]; keys[i] = keys[j]; keys[j] = tmp;
            }
            activeKeys[c] = java.util.Arrays.copyOf(keys, ST_ACTIVE_KEYS_PER_COMMIT);
            java.util.Arrays.sort(activeKeys[c]);
            totalRows += ST_ACTIVE_KEYS_PER_COMMIT * ST_VALUES_PER_ACTIVE_KEY;
        }
        final int finalTotalRows = totalRows;

        int[] readKeys = new int[200];
        for (int i = 0; i < readKeys.length; i++) {
            readKeys[i] = rnd.nextPositiveInt() % ST_KEY_COUNT;
        }

        String legacyDir = tmpDir + File.separator + "st_legacy_" + System.nanoTime();
        String postingDir = tmpDir + File.separator + "st_posting_" + System.nanoTime();
        new File(legacyDir).mkdirs();
        new File(postingDir).mkdirs();

        try {
            System.out.println("Storage & write time:");

            // Legacy
            long legacySize = buildAndMeasure("Legacy (streaming, " + ST_COMMITS + " commits)", legacyDir, () -> {
                try (Path path = new Path().of(legacyDir)) {
                    try (BitmapIndexWriter writer = new BitmapIndexWriter(config)) {
                        writer.of(path, "test", COLUMN_NAME_TXN, ST_LEGACY_BLOCK_CAPACITY);
                        int rowId = 0;
                        for (int c = 0; c < ST_COMMITS; c++) {
                            for (int key : activeKeys[c]) {
                                for (int v = 0; v < ST_VALUES_PER_ACTIVE_KEY; v++) {
                                    writer.add(key, rowId++);
                                }
                            }
                        }
                    }
                }
            });

            // Posting: incremental streaming commits, measure before and after seal
            long postingUnsealedSize;
            long postingSealedSize;
            {
                createPostingIndex(config, postingDir, PostingIndexUtils.BLOCK_CAPACITY);
                try (Path path = new Path().of(postingDir)) {
                    PostingIndexWriter postingWriter = new PostingIndexWriter(config);
                    postingWriter.of(path, "test", COLUMN_NAME_TXN, false);

                    long t0 = System.nanoTime();
                    int rowId = 0;
                    for (int c = 0; c < ST_COMMITS; c++) {
                        for (int key : activeKeys[c]) {
                            for (int v = 0; v < ST_VALUES_PER_ACTIVE_KEY; v++) {
                                postingWriter.add(key, rowId++);
                            }
                        }
                        postingWriter.commit();
                    }
                    long elapsed = System.nanoTime() - t0;
                    postingUnsealedSize = getDirectorySize(postingDir);
                    System.out.printf("  %-40s %8.1f MB in %5.2f s%n",
                            "Posting (" + ST_COMMITS + " gens, before seal)", postingUnsealedSize / (1024.0 * 1024.0), elapsed / 1e9);

                    long sealT0 = System.nanoTime();
                    postingWriter.close(); // seal + compact
                    long sealTime = System.nanoTime() - sealT0;
                    postingSealedSize = getDirectorySize(postingDir);
                    System.out.printf("  %-40s %8.1f MB (seal took %.1f ms)%n",
                            "Posting (sealed)", postingSealedSize / (1024.0 * 1024.0), sealTime / 1e6);
                }
            }

            // Summary
            System.out.println();
            System.out.printf("  %-30s %10s %7s %10s%n", "Format", "Size (MB)", "B/val", "vs Legacy");
            System.out.println("  " + "-".repeat(60));
            printRowFull("Legacy", legacySize, finalTotalRows, legacySize);
            printRowFull("Posting (500 gens)", postingUnsealedSize, finalTotalRows, legacySize);
            printRowFull("Posting (sealed)", postingSealedSize, finalTotalRows, legacySize);

            // Read latency — sealed
            System.out.println();
            System.out.printf("  Read latency after seal (%d random keys):%n", readKeys.length);

            measureReadLatency("Legacy", () -> {
                try (Path p = new Path().of(legacyDir)) {
                    try (BitmapIndexFwdReader reader = new BitmapIndexFwdReader(config, p, "test", COLUMN_NAME_TXN, -1, 0)) {
                        return readBatch(reader, readKeys);
                    }
                }
            });
            measureReadLatency("Posting (sealed)", () -> {
                try (Path p = new Path().of(postingDir)) {
                    try (PostingIndexFwdReader reader = new PostingIndexFwdReader(config, p, "test", COLUMN_NAME_TXN, -1, 0)) {
                        return readBatch(reader, readKeys);
                    }
                }
            });

            // Full scan — sealed
            System.out.println();
            System.out.printf("  Full scan after seal (%,d keys):%n", ST_KEY_COUNT);

            measureReadLatency("Legacy scan", () -> {
                try (Path p = new Path().of(legacyDir)) {
                    try (BitmapIndexFwdReader reader = new BitmapIndexFwdReader(config, p, "test", COLUMN_NAME_TXN, -1, 0)) {
                        return scanAll(reader, ST_KEY_COUNT);
                    }
                }
            });
            measureReadLatency("Posting (sealed) scan", () -> {
                try (Path p = new Path().of(postingDir)) {
                    try (PostingIndexFwdReader reader = new PostingIndexFwdReader(config, p, "test", COLUMN_NAME_TXN, -1, 0)) {
                        return scanAll(reader, ST_KEY_COUNT);
                    }
                }
            });
        } finally {
            deleteDir(legacyDir);
            deleteDir(postingDir);
        }
    }

    // ========================= Scenario 4: 70M keys x ~6 vals/key =========================

    private static void runExtremeHighCardinality(CairoConfiguration config, String tmpDir) {
        System.out.printf("=== Scenario 4: 70M keys x %d vals/key = %,d total rows ===%n%n", XHC_VALUES_PER_KEY, XHC_TOTAL_ROWS);

        System.out.println("Building key assignment (random, 4-8 vals/key)...");
        long t0 = System.nanoTime();
        int[] keyAssignment = buildRandomAssignment(XHC_TOTAL_ROWS, XHC_KEY_COUNT);
        System.out.printf("  done in %.1f s%n%n", (System.nanoTime() - t0) / 1e9);

        int[] readKeys = selectRandomKeys(XHC_KEY_COUNT, XHC_READ_BATCH);

        runScenario("Scenario 4", config, tmpDir, keyAssignment, XHC_KEY_COUNT, XHC_TOTAL_ROWS, readKeys, XHC_VALUES_PER_KEY);
    }

    // ========================= Scenario 5: 8K keys x 10K rows/key =========================

    private static void runDenseInstruments(CairoConfiguration config, String tmpDir) {
        System.out.printf("=== Scenario 5: %,d keys x %,d vals/key = %,d total rows ===%n%n",
                DI_KEY_COUNT, DI_VALUES_PER_KEY, DI_TOTAL_ROWS);

        System.out.println("Building key assignment (round-robin)...");
        long t0 = System.nanoTime();
        int[] keyAssignment = new int[DI_TOTAL_ROWS];
        for (int i = 0; i < DI_TOTAL_ROWS; i++) {
            keyAssignment[i] = i % DI_KEY_COUNT;
        }
        System.out.printf("  done in %.1f s%n%n", (System.nanoTime() - t0) / 1e9);

        int[] readKeys = new int[DI_READ_BATCH];
        for (int i = 0; i < DI_READ_BATCH; i++) readKeys[i] = i;

        runScenario("Scenario 5", config, tmpDir, keyAssignment, DI_KEY_COUNT, DI_TOTAL_ROWS, readKeys, DI_VALUES_PER_KEY);
    }

    // ========================= Scenario 6: 1K keys, 10M rows, Zipfian =========================

    private static void runZipfian(CairoConfiguration config, String tmpDir) {
        System.out.printf("=== Scenario 6: %,d keys, %,d rows, Zipfian distribution ===%n%n",
                ZF_KEY_COUNT, ZF_TOTAL_ROWS);

        System.out.println("Building Zipfian key assignment...");
        long t0 = System.nanoTime();
        int[] keyAssignment = buildZipfianAssignment(ZF_TOTAL_ROWS, ZF_KEY_COUNT, 1.0);
        System.out.printf("  done in %.1f s%n", (System.nanoTime() - t0) / 1e9);

        // Count per-key to show distribution shape
        int[] counts = new int[ZF_KEY_COUNT];
        for (int k : keyAssignment) counts[k]++;
        int maxCount = 0, minCount = Integer.MAX_VALUE;
        for (int c : counts) {
            if (c > maxCount) maxCount = c;
            if (c < minCount) minCount = c;
        }
        java.util.Arrays.sort(counts);
        int top10Sum = 0;
        for (int i = Math.max(0, counts.length - 10); i < counts.length; i++) {
            top10Sum += counts[i];
        }
        System.out.printf("  key distribution: min=%,d, max=%,d, top-1=%,d, top-10=%,d%n%n",
                counts[0], counts[counts.length - 1], counts[counts.length - 1], top10Sum);

        int[] readKeys = new int[ZF_READ_BATCH];
        for (int i = 0; i < ZF_READ_BATCH; i++) readKeys[i] = i;

        runScenario("Scenario 6", config, tmpDir, keyAssignment, ZF_KEY_COUNT, ZF_TOTAL_ROWS, readKeys, ZF_TOTAL_ROWS / ZF_KEY_COUNT);
    }

    // ========================= Generic scenario runner =========================

    private static void runScenario(String label, CairoConfiguration config, String tmpDir,
                                     int[] keyAssignment, int keyCount, int totalRows,
                                     int[] readKeys, int avgValsPerKey) {
        String legacyDir = tmpDir + File.separator + label + "_legacy_" + System.nanoTime();
        String postingDir = tmpDir + File.separator + label + "_posting_" + System.nanoTime();

        new File(legacyDir).mkdirs();
        new File(postingDir).mkdirs();

        try {
            int blockCapacity = Math.max(8, io.questdb.std.Numbers.ceilPow2(avgValsPerKey));

            // Legacy
            System.out.printf("  %-30s ... ", "Legacy (block=" + blockCapacity + ")");
            System.out.flush();
            long t0 = System.nanoTime();
            try (Path path = new Path().of(legacyDir)) {
                try (BitmapIndexWriter writer = new BitmapIndexWriter(config)) {
                    writer.of(path, "test", COLUMN_NAME_TXN, blockCapacity);
                    for (int i = 0; i < totalRows; i++) writer.add(keyAssignment[i], i);
                }
            }
            long legacySize = getDirectorySize(legacyDir);
            System.out.printf("%7.1f MB in %5.2f s%n", legacySize / (1024.0 * 1024.0), (System.nanoTime() - t0) / 1e9);

            // Posting — commit every 500K rows to bound memory for high-key-count scenarios
            int postingCommitInterval = 500_000;
            System.out.printf("  %-30s ... ", "Posting");
            System.out.flush();
            t0 = System.nanoTime();
            createPostingIndex(config, postingDir, PostingIndexUtils.BLOCK_CAPACITY);
            try (Path path = new Path().of(postingDir)) {
                try (PostingIndexWriter writer = new PostingIndexWriter(config)) {
                    writer.of(path, "test", COLUMN_NAME_TXN, false);
                    for (int i = 0; i < totalRows; i++) {
                        writer.add(keyAssignment[i], i);
                        if ((i + 1) % postingCommitInterval == 0) {
                            writer.commit();
                        }
                    }
                }
            }
            long postingSize = getDirectorySize(postingDir);
            System.out.printf("%7.1f MB in %5.2f s%n", postingSize / (1024.0 * 1024.0), (System.nanoTime() - t0) / 1e9);

            // Summary
            System.out.println();
            System.out.printf("  %-25s %10s %7s %10s%n", "Format", "Size (MB)", "B/val", "vs Legacy");
            System.out.println("  " + "-".repeat(55));
            printRowFull("Legacy", legacySize, totalRows, legacySize);
            printRowFull("Posting", postingSize, totalRows, legacySize);

            // Read latency
            System.out.println();
            System.out.printf("  Read latency (%,d keys):%n", readKeys.length);

            measureReadLatency("Legacy", () -> {
                try (Path path = new Path().of(legacyDir)) {
                    try (BitmapIndexFwdReader reader = new BitmapIndexFwdReader(config, path, "test", COLUMN_NAME_TXN, -1, 0)) {
                        return readBatch(reader, readKeys);
                    }
                }
            });
            measureReadLatency("Posting", () -> {
                try (Path path = new Path().of(postingDir)) {
                    try (PostingIndexFwdReader reader = new PostingIndexFwdReader(config, path, "test", COLUMN_NAME_TXN, -1, 0)) {
                        return readBatch(reader, readKeys);
                    }
                }
            });
        } finally {
            deleteDir(legacyDir);
            deleteDir(postingDir);
        }
    }

    // ========================= Helpers =========================

    private static int[] selectRandomKeys(int keyCount, int n) {
        int[] keys = new int[n];
        Random rng = new Random(99);
        for (int i = 0; i < n; i++) keys[i] = rng.nextInt(keyCount);
        return keys;
    }

    private static int[] buildZipfianAssignment(int totalRows, int keyCount, double skew) {
        // Pre-compute CDF for Zipfian distribution
        double[] cdf = new double[keyCount];
        double sum = 0;
        for (int i = 0; i < keyCount; i++) {
            sum += 1.0 / Math.pow(i + 1, skew);
            cdf[i] = sum;
        }
        for (int i = 0; i < keyCount; i++) cdf[i] /= sum;

        int[] assignment = new int[totalRows];
        Random rng = new Random(42);
        for (int i = 0; i < totalRows; i++) {
            double u = rng.nextDouble();
            int key = java.util.Arrays.binarySearch(cdf, u);
            if (key < 0) key = -key - 1;
            if (key >= keyCount) key = keyCount - 1;
            assignment[i] = key;
        }
        return assignment;
    }

    private static void printRowFull(String label, long size, int totalRows, long legacySize) {
        System.out.printf("  %-25s %8.1f MB %5.1f %8.2fx%n",
                label, size / (1024.0 * 1024.0), (double) size / totalRows, (double) legacySize / size);
    }

    private static long buildAndMeasure(String label, String dir, Runnable builder) {
        System.out.printf("  %-40s ... ", label);
        System.out.flush();
        long t0 = System.nanoTime();
        builder.run();
        long elapsed = System.nanoTime() - t0;
        long size = getDirectorySize(dir);
        System.out.printf("%8.1f MB in %5.2f s%n", size / (1024.0 * 1024.0), elapsed / 1e9);
        return size;
    }

    private static long scanAll(io.questdb.cairo.idx.BitmapIndexReader reader, int keyCount) {
        long sum = 0;
        for (int key = 0; key < keyCount; key++) {
            RowCursor cursor = reader.getCursor(true, key, 0, Long.MAX_VALUE);
            while (cursor.hasNext()) sum += cursor.next();
        }
        return sum;
    }

    private static int[] buildRandomAssignment(int totalRows, int keyCount) {
        int[] a = new int[totalRows];
        for (int i = 0; i < totalRows; i++) a[i] = i % keyCount;
        Random random = new Random(42);
        for (int i = totalRows - 1; i > 0; i--) {
            int j = random.nextInt(i + 1);
            int tmp = a[i]; a[i] = a[j]; a[j] = tmp;
        }
        return a;
    }

    private static long readBatch(io.questdb.cairo.idx.BitmapIndexReader reader, int[] keys) {
        long sum = 0;
        for (int key : keys) {
            RowCursor cursor = reader.getCursor(true, key, 0, Long.MAX_VALUE);
            while (cursor.hasNext()) sum += cursor.next();
        }
        return sum;
    }

    private static void measureReadLatency(String label, ReadTest test) {
        test.run();
        long total = 0;
        int runs = 5;
        for (int i = 0; i < runs; i++) {
            long t0 = System.nanoTime();
            test.run();
            total += System.nanoTime() - t0;
        }
        System.out.printf("    %-40s %8.3f ms%n", label, total / (runs * 1e6));
    }

    private static void createPostingIndex(CairoConfiguration config, String root, int blockCapacity) {
        try (Path path = new Path().of(root)) {
            int plen = path.size();
            FilesFacade ff = config.getFilesFacade();
            try (MemoryMA mem = Vm.getSmallCMARWInstance(ff, PostingIndexUtils.keyFileName(path, "test", COLUMN_NAME_TXN), MemoryTag.MMAP_DEFAULT, config.getWriterFileOpenOpts())) {
                PostingIndexWriter.initKeyMemory(mem, blockCapacity);
            }
            ff.touch(PostingIndexUtils.valueFileName(path.trimTo(plen), "test", COLUMN_NAME_TXN));
        }
    }

    private static void printRow(String label, long size, long legacySize) {
        System.out.printf("  %-25s %7.1f MB %8.2fx%n", label, size / (1024.0 * 1024.0), (double) legacySize / size);
    }

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

    @FunctionalInterface
    private interface ReadTest { long run(); }
}
