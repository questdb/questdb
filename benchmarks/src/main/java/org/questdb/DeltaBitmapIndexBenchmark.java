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
import io.questdb.cairo.DefaultCairoConfiguration;
import io.questdb.cairo.idx.BPBitmapIndexFwdReader;
import io.questdb.cairo.idx.BPBitmapIndexUtils;
import io.questdb.cairo.idx.BPBitmapIndexWriter;
import io.questdb.cairo.idx.BitmapIndexFwdReader;
import io.questdb.cairo.idx.BitmapIndexWriter;
import io.questdb.cairo.idx.DeltaBitmapIndexFwdReader;
import io.questdb.cairo.idx.DeltaBitmapIndexUtils;
import io.questdb.cairo.idx.DeltaBitmapIndexWriter;
import io.questdb.cairo.idx.FORBitmapIndexFwdReader;
import io.questdb.cairo.idx.FORBitmapIndexUtils;
import io.questdb.cairo.idx.FORBitmapIndexWriter;
import io.questdb.cairo.idx.FSSTBitmapIndexFwdReader;
import io.questdb.cairo.idx.FSSTBitmapIndexUtils;
import io.questdb.cairo.idx.FSSTBitmapIndexWriter;
import io.questdb.cairo.idx.LZ4BitmapIndexFwdReader;
import io.questdb.cairo.idx.LZ4BitmapIndexUtils;
import io.questdb.cairo.idx.LZ4BitmapIndexWriter;
import io.questdb.cairo.sql.RowCursor;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryMA;
import io.questdb.std.FilesFacade;
import io.questdb.std.MemoryTag;
import io.questdb.std.Rnd;
import io.questdb.std.str.Path;

import java.io.File;
import java.util.Random;

import static io.questdb.cairo.TableUtils.COLUMN_NAME_TXN_NONE;

/**
 * Benchmark comparing Legacy, Delta-encoded, FOR, and LZ4 Bitmap Indexes
 * across two scenarios:
 * <p>
 * <b>Scenario 1 — High-cardinality:</b> 5M keys × 4 values/key = 20M rows.
 * Each key's 4 row IDs are randomly scattered across [0, 20M).
 * Single commit. Tests LZ4 with multiple page sizes (4KB–64KB).
 * <p>
 * <b>Scenario 2 — Market data:</b> 512 keys × 7168 values/key = 3.67M rows.
 * Simulates 1 hour at 2 rows/sec/key. 56 incremental commits of 128 values/key.
 * <p>
 * <b>How to run:</b>
 * <pre>
 * mvn install -pl questdb/core -DskipTests
 * mvn package -pl questdb/benchmarks -DskipTests
 * java -Xmx2g -cp questdb/benchmarks/target/benchmarks.jar org.questdb.DeltaBitmapIndexBenchmark
 * </pre>
 */
public class DeltaBitmapIndexBenchmark {

    private static final long COLUMN_NAME_TXN = COLUMN_NAME_TXN_NONE;

    // === Scenario 1: High-cardinality ===
    private static final int HC_KEY_COUNT = 5_000_000;
    private static final int HC_VALUES_PER_KEY = 4;
    private static final int HC_TOTAL_ROWS = HC_KEY_COUNT * HC_VALUES_PER_KEY;
    private static final int HC_BLOCK_VALUES = 4;
    private static final int HC_READ_BATCH = 10_000;
    private static final int HC_LEGACY_BLOCK_CAPACITY = 8;

    // === Scenario 2: Market data (1 hour at 2 rows/sec/key) ===
    private static final int MD_KEY_COUNT = 512;
    private static final int MD_BLOCK_VALUES = 128;
    private static final int MD_COMMITS = 56;
    private static final int MD_VALUES_PER_KEY = MD_BLOCK_VALUES * MD_COMMITS; // 7168
    private static final int MD_ROWS_PER_COMMIT = MD_KEY_COUNT * MD_BLOCK_VALUES; // 65536
    private static final int MD_TOTAL_ROWS = MD_KEY_COUNT * MD_VALUES_PER_KEY; // 3,670,016
    private static final int MD_LEGACY_BLOCK_CAPACITY = 256;

    // LZ4 page sizes to test (powers of 2)
    private static final int[] PAGE_SIZES = {4096, 8192, 16384, 32768, 65536};

    public static void main(String[] args) {
        String tmpDir = System.getProperty("java.io.tmpdir");
        CairoConfiguration config = new DefaultCairoConfiguration(tmpDir);

        System.out.println("=== Scenario 1: High-Cardinality ===");
        System.out.printf("    %,d keys, %d values/key, %,d total rows%n%n", HC_KEY_COUNT, HC_VALUES_PER_KEY, HC_TOTAL_ROWS);
        runHighCardinality(config, tmpDir);

        System.out.println();
        System.out.println("=== Scenario 2: Market Data (1 hour, 2 rows/sec/key) ===");
        System.out.printf("    %,d keys, %,d values/key, %,d total rows, %d commits of %d values/key%n%n",
                MD_KEY_COUNT, MD_VALUES_PER_KEY, MD_TOTAL_ROWS, MD_COMMITS, MD_BLOCK_VALUES);
        runMarketData(config, tmpDir);
    }

    // ========================= Scenario 1: High-Cardinality =========================

    private static void runHighCardinality(CairoConfiguration config, String tmpDir) {
        System.out.println("Building key assignment (full random shuffle)...");
        long t0 = System.nanoTime();
        int[] keyAssignment = buildRandomAssignment(HC_TOTAL_ROWS, HC_KEY_COUNT);
        System.out.printf("  done in %.1f s%n%n", (System.nanoTime() - t0) / 1e9);

        int[] readKeys = new int[HC_READ_BATCH];
        Random rng = new Random(99);
        for (int i = 0; i < HC_READ_BATCH; i++) {
            readKeys[i] = rng.nextInt(HC_KEY_COUNT);
        }

        // Storage + write comparison across formats
        System.out.println("Storage & write time:");
        String legacyDir = tmpDir + File.separator + "hc_legacy_" + System.nanoTime();
        String deltaDir = tmpDir + File.separator + "hc_delta_" + System.nanoTime();
        String forDir = tmpDir + File.separator + "hc_for_" + System.nanoTime();
        String lz4Dir = tmpDir + File.separator + "hc_lz4_" + System.nanoTime();

        new File(legacyDir).mkdirs();
        new File(deltaDir).mkdirs();
        new File(forDir).mkdirs();
        new File(lz4Dir).mkdirs();

        try {
            long legacySize = buildAndMeasure("Legacy (block=" + HC_LEGACY_BLOCK_CAPACITY + ")", config, legacyDir, () -> {
                try (Path path = new Path().of(legacyDir)) {
                    try (BitmapIndexWriter writer = new BitmapIndexWriter(config)) {
                        writer.of(path, "test", COLUMN_NAME_TXN, HC_LEGACY_BLOCK_CAPACITY);
                        writeInterleaved(writer, keyAssignment);
                    }
                }
            });

            long deltaSize = buildAndMeasure("Delta", config, deltaDir, () -> {
                createDeltaIndex(config, deltaDir);
                try (Path path = new Path().of(deltaDir)) {
                    try (DeltaBitmapIndexWriter writer = new DeltaBitmapIndexWriter(config, path, "test", COLUMN_NAME_TXN)) {
                        writeInterleaved(writer, keyAssignment);
                    }
                }
            });

            long forSize = buildAndMeasure("FOR", config, forDir, () -> {
                createFORIndex(config, forDir);
                try (Path path = new Path().of(forDir)) {
                    try (FORBitmapIndexWriter writer = new FORBitmapIndexWriter(config)) {
                        writer.of(path, "test", COLUMN_NAME_TXN);
                        writeInterleaved(writer, keyAssignment);
                    }
                }
            });

            // LZ4 with default 64KB pages
            long lz4Size = buildAndMeasure("LZ4 (64KB pages)", config, lz4Dir, () -> {
                createLZ4Index(config, lz4Dir, HC_BLOCK_VALUES, 65536);
                try (Path path = new Path().of(lz4Dir)) {
                    try (LZ4BitmapIndexWriter writer = new LZ4BitmapIndexWriter(config, path, "test", COLUMN_NAME_TXN)) {
                        writeInterleaved(writer, keyAssignment);
                    }
                }
            });

            System.out.println();
            System.out.printf("  %-25s %10s %10s%n", "Format", "Size (MB)", "vs Legacy");
            System.out.println("  " + "-".repeat(47));
            printRow("Legacy", legacySize, legacySize);
            printRow("Delta", deltaSize, legacySize);
            printRow("FOR", forSize, legacySize);
            printRow("LZ4 (64KB)", lz4Size, legacySize);

            // LZ4 page size comparison
            System.out.println();
            System.out.println("LZ4 page size comparison:");
            System.out.printf("  %-25s %10s %10s %15s%n", "Page Size", "Size (MB)", "vs Legacy", "keysPerPage");
            System.out.println("  " + "-".repeat(62));
            for (int pageSize : PAGE_SIZES) {
                String dir = tmpDir + File.separator + "hc_lz4p_" + pageSize + "_" + System.nanoTime();
                new File(dir).mkdirs();
                try {
                    int kpp = LZ4BitmapIndexUtils.computeKeysPerPage(HC_BLOCK_VALUES, pageSize);
                    long size = buildAndMeasure(null, config, dir, () -> {
                        createLZ4Index(config, dir, HC_BLOCK_VALUES, pageSize);
                        try (Path path = new Path().of(dir)) {
                            try (LZ4BitmapIndexWriter writer = new LZ4BitmapIndexWriter(config, path, "test", COLUMN_NAME_TXN)) {
                                writeInterleaved(writer, keyAssignment);
                            }
                        }
                    });
                    double mb = size / (1024.0 * 1024.0);
                    double vs = (double) legacySize / size;
                    System.out.printf("  %-25s %7.1f MB %8.2fx %15d%n",
                            pageSize / 1024 + " KB", mb, vs, kpp);
                } finally {
                    deleteDir(dir);
                }
            }

            // Read latency comparison
            System.out.println();
            System.out.printf("Read latency (%,d random keys, %d values each):%n", HC_READ_BATCH, HC_VALUES_PER_KEY);

            measureReadLatency("Legacy", () -> {
                try (Path path = new Path().of(legacyDir)) {
                    try (BitmapIndexFwdReader reader = new BitmapIndexFwdReader(config, path, "test", COLUMN_NAME_TXN, -1, 0)) {
                        return readBatch(reader, readKeys);
                    }
                }
            });

            measureReadLatency("Delta", () -> {
                try (Path path = new Path().of(deltaDir)) {
                    try (DeltaBitmapIndexFwdReader reader = new DeltaBitmapIndexFwdReader(config, path, "test", COLUMN_NAME_TXN, -1, 0)) {
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

            // LZ4 reads at different page sizes
            for (int pageSize : PAGE_SIZES) {
                String dir = tmpDir + File.separator + "hc_lz4r_" + pageSize + "_" + System.nanoTime();
                new File(dir).mkdirs();
                try {
                    createLZ4Index(config, dir, HC_BLOCK_VALUES, pageSize);
                    try (Path path = new Path().of(dir)) {
                        try (LZ4BitmapIndexWriter writer = new LZ4BitmapIndexWriter(config, path, "test", COLUMN_NAME_TXN)) {
                            writeInterleaved(writer, keyAssignment);
                        }
                    }

                    int kpp = LZ4BitmapIndexUtils.computeKeysPerPage(HC_BLOCK_VALUES, pageSize);
                    measureReadLatency("LZ4 " + pageSize / 1024 + "KB (kpp=" + kpp + ")", () -> {
                        try (Path path = new Path().of(dir)) {
                            try (LZ4BitmapIndexFwdReader reader = new LZ4BitmapIndexFwdReader(config, path, "test", COLUMN_NAME_TXN, -1, 0)) {
                                return readBatch(reader, readKeys);
                            }
                        }
                    });
                } finally {
                    deleteDir(dir);
                }
            }

            // === Full-index scan speed ===
            System.out.println();
            System.out.printf("Full scan (%,d keys, %,d total values):%n", HC_KEY_COUNT, HC_TOTAL_ROWS);

            measureReadLatency("Legacy scan", () -> {
                try (Path path = new Path().of(legacyDir)) {
                    try (BitmapIndexFwdReader reader = new BitmapIndexFwdReader(config, path, "test", COLUMN_NAME_TXN, -1, 0)) {
                        return scanAll(reader, HC_KEY_COUNT);
                    }
                }
            });

            measureReadLatency("Delta scan", () -> {
                try (Path path = new Path().of(deltaDir)) {
                    try (DeltaBitmapIndexFwdReader reader = new DeltaBitmapIndexFwdReader(config, path, "test", COLUMN_NAME_TXN, -1, 0)) {
                        return scanAll(reader, HC_KEY_COUNT);
                    }
                }
            });

            measureReadLatency("FOR scan", () -> {
                try (Path path = new Path().of(forDir)) {
                    try (FORBitmapIndexFwdReader reader = new FORBitmapIndexFwdReader(config, path, "test", COLUMN_NAME_TXN, -1, 0)) {
                        return scanAll(reader, HC_KEY_COUNT);
                    }
                }
            });

            measureReadLatency("LZ4 64KB scan", () -> {
                try (Path path = new Path().of(lz4Dir)) {
                    try (LZ4BitmapIndexFwdReader reader = new LZ4BitmapIndexFwdReader(config, path, "test", COLUMN_NAME_TXN, -1, 0)) {
                        return scanAll(reader, HC_KEY_COUNT);
                    }
                }
            });

            // FSST read
            {
                String dir = tmpDir + File.separator + "hc_fsst_" + System.nanoTime();
                new File(dir).mkdirs();
                try {
                    createFSSTIndex(config, dir, HC_BLOCK_VALUES);
                    try (Path path = new Path().of(dir)) {
                        try (FSSTBitmapIndexWriter writer = new FSSTBitmapIndexWriter(config)) {
                            writer.of(path, "test", COLUMN_NAME_TXN, false);
                            writeInterleaved(writer, keyAssignment);
                        }
                    }

                    long fsstSize = getDirectorySize(dir);
                    System.out.printf("  FSST: %7.1f MB%n", fsstSize / (1024.0 * 1024.0));

                    measureReadLatency("FSST", () -> {
                        try (Path path = new Path().of(dir)) {
                            try (FSSTBitmapIndexFwdReader reader = new FSSTBitmapIndexFwdReader(config, path, "test", COLUMN_NAME_TXN, -1, 0)) {
                                return readBatch(reader, readKeys);
                            }
                        }
                    });

                    measureReadLatency("FSST scan", () -> {
                        try (Path path = new Path().of(dir)) {
                            try (FSSTBitmapIndexFwdReader reader = new FSSTBitmapIndexFwdReader(config, path, "test", COLUMN_NAME_TXN, -1, 0)) {
                                return scanAll(reader, HC_KEY_COUNT);
                            }
                        }
                    });
                } finally {
                    deleteDir(dir);
                }
            }

            // BP (Delta + FoR64 BitPacking) read
            {
                String dir = tmpDir + File.separator + "hc_bp_" + System.nanoTime();
                new File(dir).mkdirs();
                try {
                    createBPIndex(config, dir, HC_BLOCK_VALUES);
                    try (Path path = new Path().of(dir)) {
                        try (BPBitmapIndexWriter writer = new BPBitmapIndexWriter(config)) {
                            writer.of(path, "test", COLUMN_NAME_TXN, false);
                            writeInterleaved(writer, keyAssignment);
                        }
                    }

                    long bpSize = getDirectorySize(dir);
                    System.out.printf("  BP:   %7.1f MB%n", bpSize / (1024.0 * 1024.0));

                    measureReadLatency("BP", () -> {
                        try (Path path = new Path().of(dir)) {
                            try (BPBitmapIndexFwdReader reader = new BPBitmapIndexFwdReader(config, path, "test", COLUMN_NAME_TXN, -1, 0)) {
                                return readBatch(reader, readKeys);
                            }
                        }
                    });

                    measureReadLatency("BP scan", () -> {
                        try (Path path = new Path().of(dir)) {
                            try (BPBitmapIndexFwdReader reader = new BPBitmapIndexFwdReader(config, path, "test", COLUMN_NAME_TXN, -1, 0)) {
                                return scanAll(reader, HC_KEY_COUNT);
                            }
                        }
                    });
                } finally {
                    deleteDir(dir);
                }
            }
        } finally {
            deleteDir(legacyDir);
            deleteDir(deltaDir);
            deleteDir(forDir);
            deleteDir(lz4Dir);
        }
    }

    // ========================= Scenario 2: Market Data =========================

    private static void runMarketData(CairoConfiguration config, String tmpDir) {
        // Round-robin assignment: key = rowId % MD_KEY_COUNT
        // This gives uniform 128 values/key per commit batch
        int[] keyAssignment = new int[MD_TOTAL_ROWS];
        for (int i = 0; i < MD_TOTAL_ROWS; i++) {
            keyAssignment[i] = i % MD_KEY_COUNT;
        }

        int[] readKeys = new int[MD_KEY_COUNT]; // read ALL keys
        for (int i = 0; i < MD_KEY_COUNT; i++) {
            readKeys[i] = i;
        }

        String legacyDir = tmpDir + File.separator + "md_legacy_" + System.nanoTime();
        String lz4Dir = tmpDir + File.separator + "md_lz4_" + System.nanoTime();

        new File(legacyDir).mkdirs();
        new File(lz4Dir).mkdirs();

        try {
            // Legacy: single commit
            System.out.println("Storage & write time:");
            long legacySize = buildAndMeasure("Legacy (block=" + MD_LEGACY_BLOCK_CAPACITY + ")", config, legacyDir, () -> {
                try (Path path = new Path().of(legacyDir)) {
                    try (BitmapIndexWriter writer = new BitmapIndexWriter(config)) {
                        writer.of(path, "test", COLUMN_NAME_TXN, MD_LEGACY_BLOCK_CAPACITY);
                        writeInterleaved(writer, keyAssignment);
                    }
                }
            });

            // LZ4: incremental commits
            for (int pageSize : PAGE_SIZES) {
                String dir = (pageSize == 65536) ? lz4Dir : tmpDir + File.separator + "md_lz4p_" + pageSize + "_" + System.nanoTime();
                if (pageSize != 65536) new File(dir).mkdirs();
                try {
                    long size = buildAndMeasure("LZ4 " + pageSize / 1024 + "KB (" + MD_COMMITS + " commits)", config, dir, () -> {
                        createLZ4Index(config, dir, MD_BLOCK_VALUES, pageSize);
                        try (Path path = new Path().of(dir)) {
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
                    });
                    double mb = size / (1024.0 * 1024.0);
                    double vs = (double) legacySize / size;
                    int kpp = LZ4BitmapIndexUtils.computeKeysPerPage(MD_BLOCK_VALUES, pageSize);
                    System.out.printf("    → %7.1f MB  %6.2fx vs Legacy  (kpp=%d, %d gens, %d total pages)%n",
                            mb, vs, kpp,
                            MD_COMMITS,
                            MD_COMMITS * ((MD_KEY_COUNT + kpp - 1) / kpp));
                } finally {
                    if (pageSize != 65536) deleteDir(dir);
                }
            }

            // Read latency
            System.out.println();
            System.out.printf("Read latency (all %d keys, %,d values each):%n", MD_KEY_COUNT, MD_VALUES_PER_KEY);

            measureReadLatency("Legacy", () -> {
                try (Path path = new Path().of(legacyDir)) {
                    try (BitmapIndexFwdReader reader = new BitmapIndexFwdReader(config, path, "test", COLUMN_NAME_TXN, -1, 0)) {
                        return readBatch(reader, readKeys);
                    }
                }
            });

            for (int pageSize : PAGE_SIZES) {
                String dir;
                boolean needsCleanup;
                if (pageSize == 65536) {
                    dir = lz4Dir;
                    needsCleanup = false;
                } else {
                    dir = tmpDir + File.separator + "md_lz4r_" + pageSize + "_" + System.nanoTime();
                    new File(dir).mkdirs();
                    needsCleanup = true;
                    createLZ4Index(config, dir, MD_BLOCK_VALUES, pageSize);
                    try (Path path = new Path().of(dir)) {
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
                }

                try {
                    int kpp = LZ4BitmapIndexUtils.computeKeysPerPage(MD_BLOCK_VALUES, pageSize);
                    measureReadLatency("LZ4 " + pageSize / 1024 + "KB (kpp=" + kpp + ", " + MD_COMMITS + " gens)", () -> {
                        try (Path path = new Path().of(dir)) {
                            try (LZ4BitmapIndexFwdReader reader = new LZ4BitmapIndexFwdReader(config, path, "test", COLUMN_NAME_TXN, -1, 0)) {
                                return readBatch(reader, readKeys);
                            }
                        }
                    });
                } finally {
                    if (needsCleanup) deleteDir(dir);
                }
            }

            // FSST reads with incremental commits + seal
            String fsstDir;
            String fsstSealedDir;
            {
                fsstDir = tmpDir + File.separator + "md_fsst_" + System.nanoTime();
                new File(fsstDir).mkdirs();
                fsstSealedDir = fsstDir; // will be the same dir after seal
                try {
                    createFSSTIndex(config, fsstDir, MD_BLOCK_VALUES);
                    try (Path path = new Path().of(fsstDir)) {
                        FSSTBitmapIndexWriter writer = new FSSTBitmapIndexWriter(config);
                        writer.of(path, "test", COLUMN_NAME_TXN, false);
                        for (int commit = 0; commit < MD_COMMITS; commit++) {
                            int startRow = commit * MD_ROWS_PER_COMMIT;
                            int endRow = startRow + MD_ROWS_PER_COMMIT;
                            for (int rowId = startRow; rowId < endRow; rowId++) {
                                writer.add(keyAssignment[rowId], rowId);
                            }
                            writer.commit();
                        }

                        long fsstSize = getDirectorySize(fsstDir);
                        System.out.printf("  FSST: %7.1f MB (%d gens)%n",
                                fsstSize / (1024.0 * 1024.0), MD_COMMITS);

                        measureReadLatency("FSST (" + MD_COMMITS + " gens, before seal)", () -> {
                            try (Path p = new Path().of(fsstDir)) {
                                try (FSSTBitmapIndexFwdReader reader = new FSSTBitmapIndexFwdReader(config, p, "test", COLUMN_NAME_TXN, -1, 0)) {
                                    return readBatch(reader, readKeys);
                                }
                            }
                        });

                        // Seal: retrain + merge all gens into one
                        long sealT0 = System.nanoTime();
                        writer.seal();
                        long sealTime = System.nanoTime() - sealT0;
                        writer.close();

                        long sealedSize = getDirectorySize(fsstDir);
                        System.out.printf("  FSST sealed: %5.1f MB (seal took %.1f ms)%n",
                                sealedSize / (1024.0 * 1024.0), sealTime / 1e6);

                        measureReadLatency("FSST (sealed, 1 gen)", () -> {
                            try (Path p = new Path().of(fsstDir)) {
                                try (FSSTBitmapIndexFwdReader reader = new FSSTBitmapIndexFwdReader(config, p, "test", COLUMN_NAME_TXN, -1, 0)) {
                                    return readBatch(reader, readKeys);
                                }
                            }
                        });
                    }
                } catch (Throwable e) {
                    deleteDir(fsstDir);
                    throw e;
                }
            }

            // BP reads with incremental commits + seal
            String bpDir;
            {
                bpDir = tmpDir + File.separator + "md_bp_" + System.nanoTime();
                new File(bpDir).mkdirs();
                try {
                    createBPIndex(config, bpDir, MD_BLOCK_VALUES);
                    try (Path path = new Path().of(bpDir)) {
                        BPBitmapIndexWriter writer = new BPBitmapIndexWriter(config);
                        writer.of(path, "test", COLUMN_NAME_TXN, false);
                        for (int commit = 0; commit < MD_COMMITS; commit++) {
                            int startRow = commit * MD_ROWS_PER_COMMIT;
                            int endRow = startRow + MD_ROWS_PER_COMMIT;
                            for (int rowId = startRow; rowId < endRow; rowId++) {
                                writer.add(keyAssignment[rowId], rowId);
                            }
                            writer.commit();
                        }

                        long bpSize = getDirectorySize(bpDir);
                        System.out.printf("  BP:   %7.1f MB (%d gens)%n",
                                bpSize / (1024.0 * 1024.0), MD_COMMITS);

                        measureReadLatency("BP (" + MD_COMMITS + " gens, before seal)", () -> {
                            try (Path p = new Path().of(bpDir)) {
                                try (BPBitmapIndexFwdReader reader = new BPBitmapIndexFwdReader(config, p, "test", COLUMN_NAME_TXN, -1, 0)) {
                                    return readBatch(reader, readKeys);
                                }
                            }
                        });

                        long sealT0 = System.nanoTime();
                        writer.seal();
                        long sealTime = System.nanoTime() - sealT0;
                        writer.close();

                        long sealedSize = getDirectorySize(bpDir);
                        System.out.printf("  BP sealed:   %5.1f MB (seal took %.1f ms)%n",
                                sealedSize / (1024.0 * 1024.0), sealTime / 1e6);

                        measureReadLatency("BP (sealed, 1 gen)", () -> {
                            try (Path p = new Path().of(bpDir)) {
                                try (BPBitmapIndexFwdReader reader = new BPBitmapIndexFwdReader(config, p, "test", COLUMN_NAME_TXN, -1, 0)) {
                                    return readBatch(reader, readKeys);
                                }
                            }
                        });
                    }
                } catch (Throwable e) {
                    deleteDir(bpDir);
                    throw e;
                }
            }

            // === Full-index scan speed ===
            System.out.println();
            System.out.printf("Full scan (%d keys, %,d total values):%n", MD_KEY_COUNT, MD_TOTAL_ROWS);

            measureReadLatency("Legacy scan", () -> {
                try (Path path = new Path().of(legacyDir)) {
                    try (BitmapIndexFwdReader reader = new BitmapIndexFwdReader(config, path, "test", COLUMN_NAME_TXN, -1, 0)) {
                        return scanAll(reader, MD_KEY_COUNT);
                    }
                }
            });

            measureReadLatency("LZ4 64KB scan", () -> {
                try (Path path = new Path().of(lz4Dir)) {
                    try (LZ4BitmapIndexFwdReader reader = new LZ4BitmapIndexFwdReader(config, path, "test", COLUMN_NAME_TXN, -1, 0)) {
                        return scanAll(reader, MD_KEY_COUNT);
                    }
                }
            });

            measureReadLatency("FSST (sealed) scan", () -> {
                try (Path path = new Path().of(fsstSealedDir)) {
                    try (FSSTBitmapIndexFwdReader reader = new FSSTBitmapIndexFwdReader(config, path, "test", COLUMN_NAME_TXN, -1, 0)) {
                        return scanAll(reader, MD_KEY_COUNT);
                    }
                }
            });

            measureReadLatency("BP (sealed) scan", () -> {
                try (Path path = new Path().of(bpDir)) {
                    try (BPBitmapIndexFwdReader reader = new BPBitmapIndexFwdReader(config, path, "test", COLUMN_NAME_TXN, -1, 0)) {
                        return scanAll(reader, MD_KEY_COUNT);
                    }
                }
            });

            deleteDir(fsstSealedDir);
            deleteDir(bpDir);
        } finally {
            deleteDir(legacyDir);
            deleteDir(lz4Dir);
        }
    }

    // ========================= Helpers =========================

    private static int[] buildRandomAssignment(int totalRows, int keyCount) {
        int[] assignment = new int[totalRows];
        for (int i = 0; i < totalRows; i++) {
            assignment[i] = i % keyCount;
        }
        // Full Fisher-Yates shuffle
        Random random = new Random(42);
        for (int i = totalRows - 1; i > 0; i--) {
            int j = random.nextInt(i + 1);
            int tmp = assignment[i];
            assignment[i] = assignment[j];
            assignment[j] = tmp;
        }
        return assignment;
    }

    private static long buildAndMeasure(String label, CairoConfiguration config, String dir, Runnable builder) {
        if (label != null) {
            System.out.printf("  %-40s ... ", label);
            System.out.flush();
        }
        long t0 = System.nanoTime();
        builder.run();
        long elapsed = System.nanoTime() - t0;
        long size = getDirectorySize(dir);
        if (label != null) {
            System.out.printf("%8.1f MB in %5.2f s%n", size / (1024.0 * 1024.0), elapsed / 1e9);
        }
        return size;
    }

    private static long readBatch(io.questdb.cairo.idx.BitmapIndexReader reader, int[] keys) {
        long sum = 0;
        for (int key : keys) {
            RowCursor cursor = reader.getCursor(true, key, 0, Long.MAX_VALUE);
            while (cursor.hasNext()) {
                sum += cursor.next();
            }
        }
        return sum;
    }

    private static long scanAll(io.questdb.cairo.idx.BitmapIndexReader reader, int keyCount) {
        long sum = 0;
        for (int key = 0; key < keyCount; key++) {
            RowCursor cursor = reader.getCursor(true, key, 0, Long.MAX_VALUE);
            while (cursor.hasNext()) {
                sum += cursor.next();
            }
        }
        return sum;
    }

    private static void measureReadLatency(String label, ReadTest test) {
        // Warmup
        test.run();

        // Measure 5 iterations
        long total = 0;
        int runs = 5;
        for (int i = 0; i < runs; i++) {
            long t0 = System.nanoTime();
            test.run();
            total += System.nanoTime() - t0;
        }
        double avgMs = total / (runs * 1e6);
        System.out.printf("  %-45s %8.3f ms%n", label, avgMs);
    }

    private static void writeInterleaved(BitmapIndexWriter writer, int[] keyAssignment) {
        for (int rowId = 0; rowId < keyAssignment.length; rowId++) {
            writer.add(keyAssignment[rowId], rowId);
        }
    }

    private static void writeInterleaved(DeltaBitmapIndexWriter writer, int[] keyAssignment) {
        for (int rowId = 0; rowId < keyAssignment.length; rowId++) {
            writer.add(keyAssignment[rowId], rowId);
        }
    }

    private static void writeInterleaved(FORBitmapIndexWriter writer, int[] keyAssignment) {
        for (int rowId = 0; rowId < keyAssignment.length; rowId++) {
            writer.add(keyAssignment[rowId], rowId);
        }
    }

    private static void writeInterleaved(LZ4BitmapIndexWriter writer, int[] keyAssignment) {
        for (int rowId = 0; rowId < keyAssignment.length; rowId++) {
            writer.add(keyAssignment[rowId], rowId);
        }
    }

    private static void writeInterleaved(FSSTBitmapIndexWriter writer, int[] keyAssignment) {
        for (int rowId = 0; rowId < keyAssignment.length; rowId++) {
            writer.add(keyAssignment[rowId], rowId);
        }
    }

    private static void writeInterleaved(BPBitmapIndexWriter writer, int[] keyAssignment) {
        for (int rowId = 0; rowId < keyAssignment.length; rowId++) {
            writer.add(keyAssignment[rowId], rowId);
        }
    }

    private static void createDeltaIndex(CairoConfiguration config, String root) {
        try (Path path = new Path().of(root)) {
            int plen = path.size();
            FilesFacade ff = config.getFilesFacade();
            try (MemoryMA mem = Vm.getSmallCMARWInstance(
                    ff,
                    DeltaBitmapIndexUtils.keyFileName(path, "test", COLUMN_NAME_TXN),
                    MemoryTag.MMAP_DEFAULT,
                    config.getWriterFileOpenOpts()
            )) {
                DeltaBitmapIndexWriter.initKeyMemory(mem);
            }
            ff.touch(DeltaBitmapIndexUtils.valueFileName(path.trimTo(plen), "test", COLUMN_NAME_TXN));
        }
    }

    private static void createFORIndex(CairoConfiguration config, String root) {
        try (Path path = new Path().of(root)) {
            int plen = path.size();
            FilesFacade ff = config.getFilesFacade();
            try (MemoryMA mem = Vm.getSmallCMARWInstance(
                    ff,
                    FORBitmapIndexUtils.keyFileName(path, "test", COLUMN_NAME_TXN),
                    MemoryTag.MMAP_DEFAULT,
                    config.getWriterFileOpenOpts()
            )) {
                FORBitmapIndexWriter.initKeyMemory(mem);
            }
            ff.touch(FORBitmapIndexUtils.valueFileName(path.trimTo(plen), "test", COLUMN_NAME_TXN));
        }
    }

    private static void createLZ4Index(CairoConfiguration config, String root, int blockValues, int targetPageSize) {
        try (Path path = new Path().of(root)) {
            int plen = path.size();
            FilesFacade ff = config.getFilesFacade();
            try (MemoryMA mem = Vm.getSmallCMARWInstance(
                    ff,
                    LZ4BitmapIndexUtils.keyFileName(path, "test", COLUMN_NAME_TXN),
                    MemoryTag.MMAP_DEFAULT,
                    config.getWriterFileOpenOpts()
            )) {
                int keysPerPage = LZ4BitmapIndexUtils.computeKeysPerPage(blockValues, targetPageSize);
                LZ4BitmapIndexWriter.initKeyMemory(mem, blockValues, keysPerPage);
            }
            ff.touch(LZ4BitmapIndexUtils.valueFileName(path.trimTo(plen), "test", COLUMN_NAME_TXN));
        }
    }

    private static void createFSSTIndex(CairoConfiguration config, String root, int blockValues) {
        try (Path path = new Path().of(root)) {
            int plen = path.size();
            FilesFacade ff = config.getFilesFacade();
            try (MemoryMA mem = Vm.getSmallCMARWInstance(
                    ff,
                    FSSTBitmapIndexUtils.keyFileName(path, "test", COLUMN_NAME_TXN),
                    MemoryTag.MMAP_DEFAULT,
                    config.getWriterFileOpenOpts()
            )) {
                FSSTBitmapIndexWriter.initKeyMemory(mem, blockValues);
            }
            ff.touch(FSSTBitmapIndexUtils.valueFileName(path.trimTo(plen), "test", COLUMN_NAME_TXN));
        }
    }

    private static void createBPIndex(CairoConfiguration config, String root, int blockCapacity) {
        try (Path path = new Path().of(root)) {
            int plen = path.size();
            FilesFacade ff = config.getFilesFacade();
            try (MemoryMA mem = Vm.getSmallCMARWInstance(
                    ff,
                    BPBitmapIndexUtils.keyFileName(path, "test", COLUMN_NAME_TXN),
                    MemoryTag.MMAP_DEFAULT,
                    config.getWriterFileOpenOpts()
            )) {
                BPBitmapIndexWriter.initKeyMemory(mem, blockCapacity);
            }
            ff.touch(BPBitmapIndexUtils.valueFileName(path.trimTo(plen), "test", COLUMN_NAME_TXN));
        }
    }

    private static void printRow(String label, long size, long legacySize) {
        double mb = size / (1024.0 * 1024.0);
        double vs = (double) legacySize / size;
        System.out.printf("  %-25s %7.1f MB %8.2fx%n", label, mb, vs);
    }

    private static long getDirectorySize(String path) {
        File dir = new File(path);
        long size = 0;
        File[] files = dir.listFiles();
        if (files != null) {
            for (File file : files) {
                size += file.length();
            }
        }
        return size;
    }

    private static void deleteDir(String path) {
        File dir = new File(path);
        if (dir.exists()) {
            File[] files = dir.listFiles();
            if (files != null) {
                for (File file : files) {
                    file.delete();
                }
            }
            dir.delete();
        }
    }

    @FunctionalInterface
    private interface ReadTest {
        long run();
    }
}
