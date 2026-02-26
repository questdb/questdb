package org.questdb;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.DefaultCairoConfiguration;
import io.questdb.cairo.idx.BPBitmapIndexFwdReader;
import io.questdb.cairo.idx.BPBitmapIndexUtils;
import io.questdb.cairo.idx.BPBitmapIndexWriter;
import io.questdb.cairo.idx.BitmapIndexFwdReader;
import io.questdb.cairo.idx.BitmapIndexWriter;
import io.questdb.cairo.idx.FSSTBitmapIndexFwdReader;
import io.questdb.cairo.idx.FSSTBitmapIndexUtils;
import io.questdb.cairo.idx.FSSTBitmapIndexWriter;
import io.questdb.cairo.idx.FORBitmapIndexFwdReader;
import io.questdb.cairo.idx.FORBitmapIndexUtils;
import io.questdb.cairo.idx.FORBitmapIndexWriter;
import io.questdb.cairo.idx.LZ4BitmapIndexFwdReader;
import io.questdb.cairo.idx.LZ4BitmapIndexUtils;
import io.questdb.cairo.idx.LZ4BitmapIndexWriter;
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
 * Focused benchmark: 30M keys × 4 vals/key, plus market-data write profiling.
 *
 * <pre>
 * mvn package -pl questdb/core,questdb/benchmarks -DskipTests
 * java -Xmx4g -cp questdb/benchmarks/target/benchmarks.jar org.questdb.FSSTScaleBenchmark
 * </pre>
 */
public class FSSTScaleBenchmark {

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

    public static void main(String[] args) {
        String tmpDir = System.getProperty("java.io.tmpdir");
        CairoConfiguration config = new DefaultCairoConfiguration(tmpDir);

        runHighCardinality(config, tmpDir);
        System.out.println();
        runMarketDataWrites(config, tmpDir);
    }

    private static void runHighCardinality(CairoConfiguration config, String tmpDir) {
        System.out.printf("=== Scenario 1: 30M keys × %d vals/key = %,d total rows ===%n%n", HC_VALUES_PER_KEY, HC_TOTAL_ROWS);

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
        String forDir = tmpDir + File.separator + "sc_for_" + System.nanoTime();
        String lz4Dir = tmpDir + File.separator + "sc_lz4_" + System.nanoTime();
        String fsstDir = tmpDir + File.separator + "sc_fsst_" + System.nanoTime();
        String bpDir = tmpDir + File.separator + "sc_bp_" + System.nanoTime();

        new File(legacyDir).mkdirs();
        new File(forDir).mkdirs();
        new File(lz4Dir).mkdirs();
        new File(fsstDir).mkdirs();
        new File(bpDir).mkdirs();

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

            // FOR
            System.out.printf("  %-30s ... ", "FOR");
            System.out.flush();
            t0 = System.nanoTime();
            createFORIndex(config, forDir);
            try (Path path = new Path().of(forDir)) {
                try (FORBitmapIndexWriter writer = new FORBitmapIndexWriter(config)) {
                    writer.of(path, "test", COLUMN_NAME_TXN);
                    for (int rowId = 0; rowId < HC_TOTAL_ROWS; rowId++) writer.add(keyAssignment[rowId], rowId);
                }
            }
            long forSize = getDirectorySize(forDir);
            System.out.printf("%7.1f MB in %5.2f s%n", forSize / (1024.0 * 1024.0), (System.nanoTime() - t0) / 1e9);

            // LZ4 4KB
            System.out.printf("  %-30s ... ", "LZ4 4KB");
            System.out.flush();
            t0 = System.nanoTime();
            createLZ4Index(config, lz4Dir, HC_BLOCK_VALUES, 4096);
            try (Path path = new Path().of(lz4Dir)) {
                try (LZ4BitmapIndexWriter writer = new LZ4BitmapIndexWriter(config, path, "test", COLUMN_NAME_TXN)) {
                    for (int rowId = 0; rowId < HC_TOTAL_ROWS; rowId++) writer.add(keyAssignment[rowId], rowId);
                }
            }
            long lz4Size = getDirectorySize(lz4Dir);
            System.out.printf("%7.1f MB in %5.2f s%n", lz4Size / (1024.0 * 1024.0), (System.nanoTime() - t0) / 1e9);

            // FSST
            System.out.printf("  %-30s ... ", "FSST");
            System.out.flush();
            t0 = System.nanoTime();
            createFSSTIndex(config, fsstDir, HC_BLOCK_VALUES);
            try (Path path = new Path().of(fsstDir)) {
                try (FSSTBitmapIndexWriter writer = new FSSTBitmapIndexWriter(config)) {
                    writer.of(path, "test", COLUMN_NAME_TXN, false);
                    for (int rowId = 0; rowId < HC_TOTAL_ROWS; rowId++) writer.add(keyAssignment[rowId], rowId);
                }
            }
            long fsstSize = getDirectorySize(fsstDir);
            System.out.printf("%7.1f MB in %5.2f s%n", fsstSize / (1024.0 * 1024.0), (System.nanoTime() - t0) / 1e9);

            // BP
            System.out.printf("  %-30s ... ", "BP");
            System.out.flush();
            t0 = System.nanoTime();
            createBPIndex(config, bpDir, HC_BLOCK_VALUES);
            try (Path path = new Path().of(bpDir)) {
                try (BPBitmapIndexWriter writer = new BPBitmapIndexWriter(config)) {
                    writer.of(path, "test", COLUMN_NAME_TXN, false);
                    for (int rowId = 0; rowId < HC_TOTAL_ROWS; rowId++) writer.add(keyAssignment[rowId], rowId);
                }
            }
            long bpSize = getDirectorySize(bpDir);
            System.out.printf("%7.1f MB in %5.2f s%n", bpSize / (1024.0 * 1024.0), (System.nanoTime() - t0) / 1e9);

            // Summary
            System.out.println();
            System.out.printf("  %-20s %10s %10s%n", "Format", "Size (MB)", "vs Legacy");
            System.out.println("  " + "-".repeat(42));
            printRow("Legacy", legacySize, legacySize);
            printRow("FOR", forSize, legacySize);
            printRow("LZ4 4KB", lz4Size, legacySize);
            printRow("FSST", fsstSize, legacySize);
            printRow("BP", bpSize, legacySize);

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
            measureReadLatency("FOR", () -> {
                try (Path path = new Path().of(forDir)) {
                    try (FORBitmapIndexFwdReader reader = new FORBitmapIndexFwdReader(config, path, "test", COLUMN_NAME_TXN, -1, 0)) {
                        return readBatch(reader, readKeys);
                    }
                }
            });
            measureReadLatency("LZ4 4KB (kpp=" + LZ4BitmapIndexUtils.computeKeysPerPage(HC_BLOCK_VALUES, 4096) + ")", () -> {
                try (Path path = new Path().of(lz4Dir)) {
                    try (LZ4BitmapIndexFwdReader reader = new LZ4BitmapIndexFwdReader(config, path, "test", COLUMN_NAME_TXN, -1, 0)) {
                        return readBatch(reader, readKeys);
                    }
                }
            });
            measureReadLatency("FSST", () -> {
                try (Path path = new Path().of(fsstDir)) {
                    try (FSSTBitmapIndexFwdReader reader = new FSSTBitmapIndexFwdReader(config, path, "test", COLUMN_NAME_TXN, -1, 0)) {
                        return readBatch(reader, readKeys);
                    }
                }
            });
            measureReadLatency("BP", () -> {
                try (Path path = new Path().of(bpDir)) {
                    try (BPBitmapIndexFwdReader reader = new BPBitmapIndexFwdReader(config, path, "test", COLUMN_NAME_TXN, -1, 0)) {
                        return readBatch(reader, readKeys);
                    }
                }
            });
        } finally {
            deleteDir(legacyDir);
            deleteDir(forDir);
            deleteDir(lz4Dir);
            deleteDir(fsstDir);
            deleteDir(bpDir);
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

        // LZ4 4KB incremental
        String lz4Dir = tmpDir + File.separator + "md_lz4_" + System.nanoTime();
        new File(lz4Dir).mkdirs();

        System.out.printf("  %-35s", "LZ4 4KB (" + MD_COMMITS + " commits)");
        System.out.flush();
        long t0 = System.nanoTime();
        createLZ4Index(config, lz4Dir, MD_BLOCK_VALUES, 4096);
        try (Path path = new Path().of(lz4Dir)) {
            try (LZ4BitmapIndexWriter writer = new LZ4BitmapIndexWriter(config, path, "test", COLUMN_NAME_TXN)) {
                for (int commit = 0; commit < MD_COMMITS; commit++) {
                    int startRow = commit * MD_ROWS_PER_COMMIT;
                    int endRow = startRow + MD_ROWS_PER_COMMIT;
                    for (int rowId = startRow; rowId < endRow; rowId++) {
                        writer.add(keyAssignment[rowId], rowId);
                    }
                    writer.commit();
                }
            }
        }
        long lz4Time = System.nanoTime() - t0;
        long lz4Size = getDirectorySize(lz4Dir);
        System.out.printf("  %7.1f MB in %6.1f ms%n", lz4Size / (1024.0 * 1024.0), lz4Time / 1e6);

        // FSST incremental — measure per-commit breakdown + seal
        String fsstDir = tmpDir + File.separator + "md_fsst_" + System.nanoTime();
        new File(fsstDir).mkdirs();

        System.out.printf("  %-35s", "FSST (" + MD_COMMITS + " commits)");
        System.out.flush();
        long fsstAddTotal = 0, fsstCommitTotal = 0;
        createFSSTIndex(config, fsstDir, MD_BLOCK_VALUES);
        FSSTBitmapIndexWriter fsstWriter;
        try (Path path = new Path().of(fsstDir)) {
            fsstWriter = new FSSTBitmapIndexWriter(config);
            fsstWriter.of(path, "test", COLUMN_NAME_TXN, false);
            for (int commit = 0; commit < MD_COMMITS; commit++) {
                long addT0 = System.nanoTime();
                int startRow = commit * MD_ROWS_PER_COMMIT;
                int endRow = startRow + MD_ROWS_PER_COMMIT;
                for (int rowId = startRow; rowId < endRow; rowId++) {
                    fsstWriter.add(keyAssignment[rowId], rowId);
                }
                long addT1 = System.nanoTime();
                fsstAddTotal += addT1 - addT0;

                fsstWriter.commit();
                fsstCommitTotal += System.nanoTime() - addT1;
            }
        }
        long fsstSize = getDirectorySize(fsstDir);
        System.out.printf("  %7.1f MB in %6.1f ms%n", fsstSize / (1024.0 * 1024.0), (fsstAddTotal + fsstCommitTotal) / 1e6);
        System.out.printf("    add():    %6.1f ms  (%.0f%%)%n", fsstAddTotal / 1e6, 100.0 * fsstAddTotal / (fsstAddTotal + fsstCommitTotal));
        System.out.printf("    commit(): %6.1f ms  (%.0f%%)  ← includes training (1st only) + encoding + I/O%n",
                fsstCommitTotal / 1e6, 100.0 * fsstCommitTotal / (fsstAddTotal + fsstCommitTotal));
        System.out.printf("    per commit: %.2f ms%n", fsstCommitTotal / (MD_COMMITS * 1e6));

        // Seal: retrain + merge all gens → 1 gen
        long sealT0 = System.nanoTime();
        fsstWriter.seal();
        long sealTime = System.nanoTime() - sealT0;
        fsstWriter.close();

        long sealedSize = getDirectorySize(fsstDir);
        System.out.printf("    seal():   %6.1f ms  → %5.1f MB (was %.1f MB)%n",
                sealTime / 1e6, sealedSize / (1024.0 * 1024.0), fsstSize / (1024.0 * 1024.0));

        // Legacy for comparison
        String legacyDir = tmpDir + File.separator + "md_legacy_" + System.nanoTime();
        new File(legacyDir).mkdirs();
        System.out.printf("  %-35s", "Legacy (block=256)");
        System.out.flush();
        t0 = System.nanoTime();
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

        // Read comparison
        System.out.println();
        System.out.printf("  Read latency (all %d keys, %,d values each):%n", MD_KEY_COUNT, MD_VALUES_PER_KEY);

        measureReadLatency("Legacy", () -> {
            try (Path path = new Path().of(legacyDir)) {
                try (BitmapIndexFwdReader reader = new BitmapIndexFwdReader(config, path, "test", COLUMN_NAME_TXN, -1, 0)) {
                    return readBatch(reader, readKeys);
                }
            }
        });
        measureReadLatency("LZ4 4KB (56 gens)", () -> {
            try (Path path = new Path().of(lz4Dir)) {
                try (LZ4BitmapIndexFwdReader reader = new LZ4BitmapIndexFwdReader(config, path, "test", COLUMN_NAME_TXN, -1, 0)) {
                    return readBatch(reader, readKeys);
                }
            }
        });
        measureReadLatency("FSST sealed (1 gen)", () -> {
            try (Path path = new Path().of(fsstDir)) {
                try (FSSTBitmapIndexFwdReader reader = new FSSTBitmapIndexFwdReader(config, path, "test", COLUMN_NAME_TXN, -1, 0)) {
                    return readBatch(reader, readKeys);
                }
            }
        });

        deleteDir(lz4Dir);
        deleteDir(fsstDir);
        deleteDir(legacyDir);
    }

    // ========================= Helpers =========================

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

    private static void createFORIndex(CairoConfiguration config, String root) {
        try (Path path = new Path().of(root)) {
            int plen = path.size();
            FilesFacade ff = config.getFilesFacade();
            try (MemoryMA mem = Vm.getSmallCMARWInstance(ff, FORBitmapIndexUtils.keyFileName(path, "test", COLUMN_NAME_TXN), MemoryTag.MMAP_DEFAULT, config.getWriterFileOpenOpts())) {
                FORBitmapIndexWriter.initKeyMemory(mem);
            }
            ff.touch(FORBitmapIndexUtils.valueFileName(path.trimTo(plen), "test", COLUMN_NAME_TXN));
        }
    }

    private static void createLZ4Index(CairoConfiguration config, String root, int blockValues, int targetPageSize) {
        try (Path path = new Path().of(root)) {
            int plen = path.size();
            FilesFacade ff = config.getFilesFacade();
            try (MemoryMA mem = Vm.getSmallCMARWInstance(ff, LZ4BitmapIndexUtils.keyFileName(path, "test", COLUMN_NAME_TXN), MemoryTag.MMAP_DEFAULT, config.getWriterFileOpenOpts())) {
                LZ4BitmapIndexWriter.initKeyMemory(mem, blockValues, LZ4BitmapIndexUtils.computeKeysPerPage(blockValues, targetPageSize));
            }
            ff.touch(LZ4BitmapIndexUtils.valueFileName(path.trimTo(plen), "test", COLUMN_NAME_TXN));
        }
    }

    private static void createFSSTIndex(CairoConfiguration config, String root, int blockValues) {
        try (Path path = new Path().of(root)) {
            int plen = path.size();
            FilesFacade ff = config.getFilesFacade();
            try (MemoryMA mem = Vm.getSmallCMARWInstance(ff, FSSTBitmapIndexUtils.keyFileName(path, "test", COLUMN_NAME_TXN), MemoryTag.MMAP_DEFAULT, config.getWriterFileOpenOpts())) {
                FSSTBitmapIndexWriter.initKeyMemory(mem, blockValues);
            }
            ff.touch(FSSTBitmapIndexUtils.valueFileName(path.trimTo(plen), "test", COLUMN_NAME_TXN));
        }
    }

    private static void createBPIndex(CairoConfiguration config, String root, int blockCapacity) {
        try (Path path = new Path().of(root)) {
            int plen = path.size();
            FilesFacade ff = config.getFilesFacade();
            try (MemoryMA mem = Vm.getSmallCMARWInstance(ff, BPBitmapIndexUtils.keyFileName(path, "test", COLUMN_NAME_TXN), MemoryTag.MMAP_DEFAULT, config.getWriterFileOpenOpts())) {
                BPBitmapIndexWriter.initKeyMemory(mem, blockCapacity);
            }
            ff.touch(BPBitmapIndexUtils.valueFileName(path.trimTo(plen), "test", COLUMN_NAME_TXN));
        }
    }

    private static void printRow(String label, long size, long legacySize) {
        System.out.printf("  %-20s %7.1f MB %8.2fx%n", label, size / (1024.0 * 1024.0), (double) legacySize / size);
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
