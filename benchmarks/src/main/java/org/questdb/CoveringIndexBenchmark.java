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
 * Benchmark for covering index sidecar compression across column types.
 * Measures write time, sidecar file size, and read throughput for:
 * - DOUBLE (ALP-compressed)
 * - FLOAT (FoR-compressed as int32)
 * - LONG (FoR-compressed)
 * - INT (FoR-compressed)
 * <p>
 * Each type is tested with realistic data patterns and compared against
 * baseline (index scan + random column-file reads).
 *
 * <pre>
 * mvn install -pl questdb/core -DskipTests -q
 * mvn package -pl questdb/benchmarks -DskipTests -q
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

        System.out.printf("=== Covering Index Compression Benchmark ===%n");
        System.out.printf("    %,d keys x %,d values/key = %,d total rows%n", KEY_COUNT, VALUES_PER_KEY, TOTAL_ROWS);
        System.out.printf("    Read: %,d random keys per iteration%n%n", READ_KEYS);

        System.out.print("Building row→key assignment... ");
        System.out.flush();
        long t0 = System.nanoTime();
        int[] keyAssignment = buildShuffledAssignment(TOTAL_ROWS, KEY_COUNT);
        System.out.printf("done in %.1f s%n%n", (System.nanoTime() - t0) / 1e9);

        int[] readKeys = new int[READ_KEYS];
        Random readRng = new Random(99);
        for (int i = 0; i < READ_KEYS; i++) {
            readKeys[i] = readRng.nextInt(KEY_COUNT);
        }

        System.out.printf("  %-35s %10s %10s %10s %12s %12s%n",
                "Column Type", "Raw (MB)", "Sidecar", "Ratio", "Baseline ms", "Covering ms");
        System.out.println("  " + "-".repeat(95));

        for (CoverType ct : CoverType.values()) {
            runTypeVariant(config, tmpDir, keyAssignment, readKeys, ct);
        }
    }

    private static long baselineRead(CairoConfiguration config, String dir, int[] keys,
                                     long colAddr, CoverType ct) {
        long sum = 0;
        try (Path path = new Path().of(dir)) {
            try (PostingIndexFwdReader reader = new PostingIndexFwdReader(
                    config, path, "test", COLUMN_NAME_TXN_NONE, 0, 0)) {
                for (int key : keys) {
                    RowCursor cursor = reader.getCursor(true, key, 0, Long.MAX_VALUE);
                    while (cursor.hasNext()) {
                        long rowId = cursor.next();
                        sum += switch (ct) {
                            case DOUBLE -> (long) Unsafe.getUnsafe().getDouble(colAddr + rowId * Double.BYTES);
                            case FLOAT -> (long) Unsafe.getUnsafe().getFloat(colAddr + rowId * Float.BYTES);
                            case LONG -> Unsafe.getUnsafe().getLong(colAddr + rowId * Long.BYTES);
                            case INT -> Unsafe.getUnsafe().getInt(colAddr + rowId * Integer.BYTES);
                        };
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

    private static long coveringRead(CairoConfiguration config, String dir, int[] keys, CoverType ct) {
        long sum = 0;
        try (Path path = new Path().of(dir)) {
            try (PostingIndexFwdReader reader = new PostingIndexFwdReader(
                    config, path, "test", COLUMN_NAME_TXN_NONE, 0, 0)) {
                for (int key : keys) {
                    RowCursor cursor = reader.getCursor(true, key, 0, Long.MAX_VALUE);
                    if (cursor instanceof CoveringRowCursor crc && crc.hasCovering()) {
                        while (crc.hasNext()) {
                            crc.next();
                            sum += switch (ct) {
                                case DOUBLE -> (long) crc.getCoveredDouble(0);
                                case FLOAT -> (long) crc.getCoveredFloat(0);
                                case LONG -> crc.getCoveredLong(0);
                                case INT -> crc.getCoveredInt(0);
                            };
                        }
                    } else {
                        while (cursor.hasNext()) {
                            sum += cursor.next();
                        }
                    }
                }
            }
        }
        return sum;
    }

    private static void deleteDir(String path) {
        File dir = new File(path);
        if (dir.exists()) {
            File[] files = dir.listFiles();
            if (files != null) for (File f : files) f.delete();
            dir.delete();
        }
    }

    private static void fillColumnData(long addr, CoverType ct, Random rng) {
        for (int i = 0; i < TOTAL_ROWS; i++) {
            switch (ct) {
                case DOUBLE -> Unsafe.getUnsafe().putDouble(addr + (long) i * Double.BYTES,
                        100.0 + rng.nextInt(90_000) * 0.01); // 100.00-999.99
                case FLOAT -> Unsafe.getUnsafe().putFloat(addr + (long) i * Float.BYTES,
                        20.0f + rng.nextInt(500) * 0.01f); // 20.00-24.99
                case LONG -> Unsafe.getUnsafe().putLong(addr + (long) i * Long.BYTES,
                        1_700_000_000_000_000L + rng.nextInt(1_000_000)); // tight µs range
                case INT -> Unsafe.getUnsafe().putInt(addr + (long) i * Integer.BYTES,
                        rng.nextInt(10_000)); // 0-9999
            }
        }
    }

    private static long getDirectorySize(String path) {
        File dir = new File(path);
        long size = 0;
        File[] files = dir.listFiles();
        if (files != null) for (File f : files) size += f.length();
        return size;
    }

    private static void runTypeVariant(CairoConfiguration config, String tmpDir,
                                       int[] keyAssignment, int[] readKeys, CoverType ct) {
        String baseDir = tmpDir + File.separator + "cov_base_" + ct.name() + "_" + System.nanoTime();
        String covDir = tmpDir + File.separator + "cov_comp_" + ct.name() + "_" + System.nanoTime();
        new File(baseDir).mkdirs();
        new File(covDir).mkdirs();

        // Allocate column data
        long colAddr = Unsafe.malloc((long) TOTAL_ROWS * ct.size, MemoryTag.NATIVE_DEFAULT);
        Random rng = new Random(42);
        fillColumnData(colAddr, ct, rng);

        try {
            // Raw column data size
            double rawMB = (double) TOTAL_ROWS * ct.size / (1024.0 * 1024.0);

            // Write baseline (no covering)
            writeIndex(config, baseDir, keyAssignment, null, 0, null, null, null, 0);

            // Write covering
            writeIndex(config, covDir, keyAssignment, new long[]{colAddr}, 1,
                    new int[]{ct.shift}, new int[]{ct.columnType}, new long[]{0}, ct.columnType);

            // Sidecar size = total covering dir size - baseline dir size
            long baselineSize = getDirectorySize(baseDir);
            long coveringSize = getDirectorySize(covDir);
            long sidecarSize = coveringSize - baselineSize;
            double sidecarMB = sidecarSize / (1024.0 * 1024.0);
            double ratio = rawMB / sidecarMB;

            // Benchmark: baseline (index + random column read)
            for (int w = 0; w < WARMUP; w++) {
                baselineRead(config, baseDir, readKeys, colAddr, ct);
            }
            long baseTotal = 0;
            for (int i = 0; i < ITERS; i++) {
                long start = System.nanoTime();
                baselineRead(config, baseDir, readKeys, colAddr, ct);
                baseTotal += System.nanoTime() - start;
            }
            double baseMs = baseTotal / (ITERS * 1e6);

            // Benchmark: covering read
            for (int w = 0; w < WARMUP; w++) {
                coveringRead(config, covDir, readKeys, ct);
            }
            long covTotal = 0;
            for (int i = 0; i < ITERS; i++) {
                long start = System.nanoTime();
                coveringRead(config, covDir, readKeys, ct);
                covTotal += System.nanoTime() - start;
            }
            double covMs = covTotal / (ITERS * 1e6);

            System.out.printf("  %-35s %10.1f %8.1f MB %9.1fx %10.1f ms %10.1f ms%n",
                    ct.name() + " (" + ct.description + ")",
                    rawMB, sidecarMB, ratio, baseMs, covMs);

        } finally {
            Unsafe.free(colAddr, (long) TOTAL_ROWS * ct.size, MemoryTag.NATIVE_DEFAULT);
            deleteDir(baseDir);
            deleteDir(covDir);
        }
    }

    private static void writeIndex(CairoConfiguration config, String dir, int[] keyAssignment,
                                   long[] colAddrs, int coverCount,
                                   int[] shifts, int[] types, long[] tops, int colType) {
        try (Path path = new Path().of(dir)) {
            try (PostingIndexWriter writer = new PostingIndexWriter(config, path, "test", COLUMN_NAME_TXN_NONE)) {
                if (coverCount > 0) {
                    int[] indices = new int[coverCount];
                    for (int i = 0; i < coverCount; i++) indices[i] = i + 2;
                    writer.configureCovering(colAddrs, tops, shifts, indices, types, coverCount);
                }
                for (int rowId = 0; rowId < TOTAL_ROWS; rowId++) {
                    writer.add(keyAssignment[rowId], rowId);
                }
                writer.setMaxValue(TOTAL_ROWS - 1);
            }
        }
    }

    enum CoverType {
        DOUBLE(ColumnType.DOUBLE, 3, Double.BYTES, "Prices 100.00-999.99"),
        FLOAT(ColumnType.FLOAT, 2, Float.BYTES, "Sensor readings 20.0-25.0"),
        LONG(ColumnType.LONG, 3, Long.BYTES, "Timestamps (µs, tight range)"),
        INT(ColumnType.INT, 2, Integer.BYTES, "Quantities 0-9999");

        final int columnType;
        final String description;
        final int shift;
        final int size;

        CoverType(int columnType, int shift, int size, String description) {
            this.columnType = columnType;
            this.shift = shift;
            this.size = size;
            this.description = description;
        }
    }
}
