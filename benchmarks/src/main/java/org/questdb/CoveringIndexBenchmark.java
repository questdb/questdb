package org.questdb;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.DefaultCairoConfiguration;
import io.questdb.cairo.idx.CoveringRowCursor;
import io.questdb.cairo.idx.PostingIndexFwdReader;
import io.questdb.cairo.idx.PostingIndexWriter;
import io.questdb.cairo.sql.RowCursor;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import io.questdb.std.str.Path;

import java.io.File;
import java.util.Random;

import static io.questdb.cairo.TableUtils.COLUMN_NAME_TXN_NONE;

/**
 * Benchmark comparing covered-column reads from sidecar files vs scattered
 * random reads from column files.
 *
 * Scenario: trades table (ts, sym, price DOUBLE, qty INT) with sym indexed
 * using POSTING INCLUDE (price, qty). Query: SELECT price, qty WHERE sym = key.
 *
 * <pre>
 * mvn package -pl questdb/core,questdb/benchmarks -DskipTests -q
 * java -Xmx4g -cp questdb/benchmarks/target/benchmarks.jar org.questdb.CoveringIndexBenchmark
 * </pre>
 */
public class CoveringIndexBenchmark {

    private static final int KEY_COUNT = 8_000;
    private static final int VALUES_PER_KEY = 10_000;
    private static final int TOTAL_ROWS = KEY_COUNT * VALUES_PER_KEY;
    private static final int READ_KEYS = 1_000;
    private static final int WARMUP = 3;
    private static final int ITERS = 10;

    public static void main(String[] args) {
        String tmpDir = System.getProperty("java.io.tmpdir");
        CairoConfiguration config = new DefaultCairoConfiguration(tmpDir);

        String baselineDir = tmpDir + File.separator + "cov_baseline_" + System.nanoTime();
        String coveringDir = tmpDir + File.separator + "cov_covering_" + System.nanoTime();
        new File(baselineDir).mkdirs();
        new File(coveringDir).mkdirs();

        System.out.printf("=== Covering Index Benchmark ===%n");
        System.out.printf("    %,d keys x %,d values/key = %,d total rows%n", KEY_COUNT, VALUES_PER_KEY, TOTAL_ROWS);
        System.out.printf("    Covered columns: price (DOUBLE), qty (INT)%n");
        System.out.printf("    Read batch: %,d random keys%n%n", READ_KEYS);

        // Build key assignment: row i → key (round-robin shuffled)
        System.out.print("Building row→key assignment... ");
        System.out.flush();
        long t0 = System.nanoTime();
        int[] keyAssignment = buildShuffledAssignment(TOTAL_ROWS, KEY_COUNT);
        System.out.printf("done in %.1f s%n", (System.nanoTime() - t0) / 1e9);

        // Allocate fake column data (price: DOUBLE, qty: INT)
        long priceAddr = Unsafe.malloc((long) TOTAL_ROWS * Double.BYTES, MemoryTag.NATIVE_DEFAULT);
        long qtyAddr = Unsafe.malloc((long) TOTAL_ROWS * Integer.BYTES, MemoryTag.NATIVE_DEFAULT);
        Random rng = new Random(7);
        for (int i = 0; i < TOTAL_ROWS; i++) {
            Unsafe.getUnsafe().putDouble(priceAddr + (long) i * Double.BYTES, 100.0 + rng.nextDouble() * 900.0);
            Unsafe.getUnsafe().putInt(qtyAddr + (long) i * Integer.BYTES, rng.nextInt(10_000));
        }

        try {
            // === Write baseline index (no covering) ===
            System.out.print("Writing baseline index (no covering)... ");
            System.out.flush();
            t0 = System.nanoTime();
            try (Path path = new Path().of(baselineDir)) {
                try (PostingIndexWriter writer = new PostingIndexWriter(config, path, "test", COLUMN_NAME_TXN_NONE)) {
                    for (int rowId = 0; rowId < TOTAL_ROWS; rowId++) {
                        writer.add(keyAssignment[rowId], rowId);
                    }
                    writer.setMaxValue(TOTAL_ROWS - 1);
                }
            }
            long baselineSize = getDirectorySize(baselineDir);
            System.out.printf("%.1f MB in %.2f s%n", baselineSize / (1024.0 * 1024.0), (System.nanoTime() - t0) / 1e9);

            // === Write covering index ===
            System.out.print("Writing covering index (INCLUDE price, qty)... ");
            System.out.flush();
            t0 = System.nanoTime();
            try (Path path = new Path().of(coveringDir)) {
                try (PostingIndexWriter writer = new PostingIndexWriter(config, path, "test", COLUMN_NAME_TXN_NONE)) {
                    writer.configureCovering(
                            new long[]{priceAddr, qtyAddr},
                            new long[]{0, 0},
                            new int[]{3, 2}, // DOUBLE shift=3, INT shift=2
                            new int[]{2, 3}, // column indices
                            new int[]{ColumnType.DOUBLE, ColumnType.INT},
                            2
                    );
                    for (int rowId = 0; rowId < TOTAL_ROWS; rowId++) {
                        writer.add(keyAssignment[rowId], rowId);
                    }
                    writer.setMaxValue(TOTAL_ROWS - 1);
                }
            }
            long coveringSize = getDirectorySize(coveringDir);
            System.out.printf("%.1f MB in %.2f s%n", coveringSize / (1024.0 * 1024.0), (System.nanoTime() - t0) / 1e9);

            System.out.printf("%n  %-20s %10s%n", "Format", "Size (MB)");
            System.out.println("  " + "-".repeat(32));
            System.out.printf("  %-20s %10.1f%n", "Baseline (no cover)", baselineSize / (1024.0 * 1024.0));
            System.out.printf("  %-20s %10.1f%n", "Covering", coveringSize / (1024.0 * 1024.0));

            // Pick random read keys
            int[] readKeys = new int[READ_KEYS];
            Random readRng = new Random(99);
            for (int i = 0; i < READ_KEYS; i++) {
                readKeys[i] = readRng.nextInt(KEY_COUNT);
            }

            System.out.printf("%nRead benchmark: sum price + qty for %,d random keys (%,d values/key)%n%n",
                    READ_KEYS, VALUES_PER_KEY);

            // === Baseline: index scan + scattered column-file reads ===
            System.out.printf("  %-45s", "Baseline (index + column-file random reads)");
            System.out.flush();
            {
                // Warmup
                for (int w = 0; w < WARMUP; w++) {
                    baselineRead(config, baselineDir, readKeys, priceAddr, qtyAddr);
                }
                long total = 0;
                for (int i = 0; i < ITERS; i++) {
                    long start = System.nanoTime();
                    baselineRead(config, baselineDir, readKeys, priceAddr, qtyAddr);
                    total += System.nanoTime() - start;
                }
                System.out.printf(" %8.3f ms%n", total / (ITERS * 1e6));
            }

            // === Covering: index scan + sequential sidecar reads ===
            System.out.printf("  %-45s", "Covering (sidecar sequential reads)");
            System.out.flush();
            {
                // Warmup
                for (int w = 0; w < WARMUP; w++) {
                    coveringRead(config, coveringDir, readKeys);
                }
                long total = 0;
                for (int i = 0; i < ITERS; i++) {
                    long start = System.nanoTime();
                    coveringRead(config, coveringDir, readKeys);
                    total += System.nanoTime() - start;
                }
                System.out.printf(" %8.3f ms%n", total / (ITERS * 1e6));
            }

        } finally {
            Unsafe.free(priceAddr, (long) TOTAL_ROWS * Double.BYTES, MemoryTag.NATIVE_DEFAULT);
            Unsafe.free(qtyAddr, (long) TOTAL_ROWS * Integer.BYTES, MemoryTag.NATIVE_DEFAULT);
            deleteDir(baselineDir);
            deleteDir(coveringDir);
        }
    }

    /**
     * Baseline read: get row IDs from posting index, then random-read price and qty
     * from column memory (simulates scattered column file access).
     */
    private static long baselineRead(CairoConfiguration config, String dir, int[] keys,
                                     long priceAddr, long qtyAddr) {
        long sum = 0;
        try (Path path = new Path().of(dir)) {
            try (PostingIndexFwdReader reader = new PostingIndexFwdReader(
                    config, path, "test", COLUMN_NAME_TXN_NONE, 0, 0)) {
                for (int key : keys) {
                    RowCursor cursor = reader.getCursor(true, key, 0, Long.MAX_VALUE);
                    while (cursor.hasNext()) {
                        long rowId = cursor.next();
                        // Scattered random reads from "column files"
                        double price = Unsafe.getUnsafe().getDouble(priceAddr + rowId * Double.BYTES);
                        int qty = Unsafe.getUnsafe().getInt(qtyAddr + rowId * Integer.BYTES);
                        sum += (long) price + qty;
                    }
                }
            }
        }
        return sum;
    }

    /**
     * Covering read: get row IDs + covered values from the sidecar in one pass.
     * No column file access needed.
     */
    private static long coveringRead(CairoConfiguration config, String dir, int[] keys) {
        long sum = 0;
        try (Path path = new Path().of(dir)) {
            try (PostingIndexFwdReader reader = new PostingIndexFwdReader(
                    config, path, "test", COLUMN_NAME_TXN_NONE, 0, 0)) {
                for (int key : keys) {
                    RowCursor cursor = reader.getCursor(true, key, 0, Long.MAX_VALUE);
                    if (cursor instanceof CoveringRowCursor crc && crc.hasCovering()) {
                        while (crc.hasNext()) {
                            crc.next();
                            double price = crc.getCoveredDouble(0);
                            int qty = crc.getCoveredInt(1);
                            sum += (long) price + qty;
                        }
                    } else {
                        // Fallback — shouldn't happen in this benchmark
                        while (cursor.hasNext()) {
                            sum += cursor.next();
                        }
                    }
                }
            }
        }
        return sum;
    }

    private static int[] buildShuffledAssignment(int totalRows, int keyCount) {
        int[] a = new int[totalRows];
        for (int i = 0; i < totalRows; i++) a[i] = i % keyCount;
        Random random = new Random(42);
        for (int i = totalRows - 1; i > 0; i--) {
            int j = random.nextInt(i + 1);
            int tmp = a[i];
            a[i] = a[j];
            a[j] = tmp;
        }
        return a;
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
}
