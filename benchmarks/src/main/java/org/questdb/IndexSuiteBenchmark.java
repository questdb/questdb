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
import io.questdb.cairo.idx.BitmapIndexReader;
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
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.str.Path;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static io.questdb.cairo.TableUtils.COLUMN_NAME_TXN_NONE;

/**
 * Comprehensive bitmap index benchmark sweeping over:
 * <ul>
 *   <li>Distribution: RANDOM (Fisher-Yates), ROUND_ROBIN (modulo)</li>
 *   <li>Values/key: 4, 8, 16, 32, 64, 128</li>
 *   <li>Row offset: 0 (25-bit), 1B (30-bit), 1T (40-bit)</li>
 * </ul>
 *
 * 20M total rows, 6 formats (Legacy, Delta, FOR, LZ4 4KB, FSST, BP), single commit.
 * 36 scenarios x 6 formats = 216 measurements.
 *
 * <pre>
 * mvn install -pl questdb/core -DskipTests -q
 * mvn package -pl questdb/benchmarks -DskipTests -q
 * java -Xmx2g -cp questdb/benchmarks/target/benchmarks.jar org.questdb.IndexSuiteBenchmark
 * </pre>
 */
public class IndexSuiteBenchmark {

    private static final long COLUMN_NAME_TXN = COLUMN_NAME_TXN_NONE;
    private static final int TOTAL_ROWS = 20_000_000;
    private static final int LZ4_PAGE_SIZE = 4096;
    private static final int BP_COMMIT_INTERVAL = 500_000;
    private static final int READ_WARMUP = 1;
    private static final int READ_RUNS = 5;
    private static final int MAX_READ_KEYS = 10_000;

    private static final int[] VALS_PER_KEY = {4, 8, 16, 32, 64, 128};
    private static final long[] ROW_OFFSETS = {0L, 1_000_000_000L, 1_000_000_000_000L};
    private static final String[] OFFSET_LABELS = {"0", "1B", "1T"};
    private static final String[] OFFSET_BITS = {"25-bit", "30-bit", "40-bit"};

    enum Format {LEGACY, DELTA, FOR, LZ4, FSST, BP}

    enum Distribution {RANDOM, ROUND_ROBIN}

    static class ScenarioResult {
        final Distribution dist;
        final int valsPerKey;
        final long rowOffset;
        final String offsetLabel;
        final int keyCount;
        final double[] sizeMB = new double[Format.values().length];
        final double[] bPerVal = new double[Format.values().length];
        final double[] writeSec = new double[Format.values().length];
        final double[] readMs = new double[Format.values().length];

        ScenarioResult(Distribution dist, int valsPerKey, long rowOffset, String offsetLabel) {
            this.dist = dist;
            this.valsPerKey = valsPerKey;
            this.rowOffset = rowOffset;
            this.offsetLabel = offsetLabel;
            this.keyCount = TOTAL_ROWS / valsPerKey;
        }
    }

    public static void main(String[] args) {
        String tmpDir = System.getProperty("java.io.tmpdir");
        CairoConfiguration config = new DefaultCairoConfiguration(tmpDir);

        System.out.printf("=== Index Suite Benchmark ===%n");
        System.out.printf("    Total rows: %,d%n", TOTAL_ROWS);
        System.out.printf("    Formats: Legacy, Delta, FOR, LZ4 4KB, FSST, BP%n");
        System.out.printf("    Distributions: RANDOM, ROUND_ROBIN%n");
        System.out.printf("    Values/key: 4, 8, 16, 32, 64, 128%n");
        System.out.printf("    Row offsets: 0 (25-bit), 1B (30-bit), 1T (40-bit)%n%n");

        List<ScenarioResult> allResults = new ArrayList<>();

        for (Distribution dist : Distribution.values()) {
            // Build key assignment once per (dist, valsPerKey) — reuse across offsets
            int prevValsPerKey = -1;
            int[] keyAssignment = null;

            for (int valsPerKey : VALS_PER_KEY) {
                int keyCount = TOTAL_ROWS / valsPerKey;

                if (valsPerKey != prevValsPerKey) {
                    System.out.printf("Building %s key assignment: %,d keys x %d vals/key ...%n", dist, keyCount, valsPerKey);
                    long t0 = System.nanoTime();
                    keyAssignment = buildKeyAssignment(dist, TOTAL_ROWS, keyCount);
                    System.out.printf("  done in %.1f s%n", (System.nanoTime() - t0) / 1e9);
                    prevValsPerKey = valsPerKey;
                }

                for (int oi = 0; oi < ROW_OFFSETS.length; oi++) {
                    long rowOffset = ROW_OFFSETS[oi];
                    ScenarioResult result = new ScenarioResult(dist, valsPerKey, rowOffset, OFFSET_LABELS[oi]);

                    System.out.printf("%n=== %s, %d vals/key, %,dK keys, offset=%s (%s) ===%n%n",
                            dist, valsPerKey, keyCount / 1000, OFFSET_LABELS[oi], OFFSET_BITS[oi]);
                    System.out.printf("  %-14s %10s %7s %10s %10s%n", "Format", "Size (MB)", "B/val", "Write (s)", "Read (ms)");
                    System.out.println("  " + "─".repeat(55));

                    int[] readKeys = selectReadKeys(keyCount);

                    for (Format format : Format.values()) {
                        String dir = tmpDir + File.separator + "idx_suite_" + format + "_" + System.nanoTime();
                        new File(dir).mkdirs();
                        try {
                            FormatResult fr = runFormat(format, config, dir, keyAssignment, valsPerKey, rowOffset, readKeys);
                            int fi = format.ordinal();
                            result.sizeMB[fi] = fr.sizeMB;
                            result.bPerVal[fi] = fr.bPerVal;
                            result.writeSec[fi] = fr.writeSec;
                            result.readMs[fi] = fr.readMs;

                            System.out.printf("  %-14s %8.1f MB %5.1f %8.2f s %8.1f ms%n",
                                    formatLabel(format), fr.sizeMB, fr.bPerVal, fr.writeSec, fr.readMs);
                        } finally {
                            deleteDir(dir);
                        }
                    }

                    allResults.add(result);
                }
            }
        }

        printSummaryTables(allResults);
    }

    // ========================= Per-format runner =========================

    static class FormatResult {
        double sizeMB;
        double bPerVal;
        double writeSec;
        double readMs;
    }

    private static void createBPIndex(CairoConfiguration config, String dir, int[] keyAssignment, long rowOffset) {
        try (Path path = new Path().of(dir)) {
            int plen = path.size();
            FilesFacade ff = config.getFilesFacade();
            try (MemoryMA mem = Vm.getSmallCMARWInstance(
                    ff,
                    BPBitmapIndexUtils.keyFileName(path, "test", COLUMN_NAME_TXN),
                    MemoryTag.MMAP_DEFAULT,
                    config.getWriterFileOpenOpts()
            )) {
                BPBitmapIndexWriter.initKeyMemory(mem, BPBitmapIndexUtils.BLOCK_CAPACITY);
            }
            ff.touch(BPBitmapIndexUtils.valueFileName(path.trimTo(plen), "test", COLUMN_NAME_TXN));
        }
        try (Path path = new Path().of(dir)) {
            try (BPBitmapIndexWriter writer = new BPBitmapIndexWriter(config)) {
                writer.of(path, "test", COLUMN_NAME_TXN, false);
                for (int i = 0; i < keyAssignment.length; i++) {
                    writer.add(keyAssignment[i], (long) i + rowOffset);
                    if ((i + 1) % BP_COMMIT_INTERVAL == 0) {
                        writer.commit();
                    }
                }
            }
        }
    }

    // ========================= Index creation (write) =========================

    private static void createLegacyIndex(CairoConfiguration config, String dir, int[] keyAssignment, int valsPerKey, long rowOffset) {
        int blockCapacity = Numbers.ceilPow2(valsPerKey);
        try (Path path = new Path().of(dir)) {
            try (BitmapIndexWriter writer = new BitmapIndexWriter(config)) {
                writer.of(path, "test", COLUMN_NAME_TXN, blockCapacity);
                for (int i = 0; i < keyAssignment.length; i++) {
                    writer.add(keyAssignment[i], (long) i + rowOffset);
                }
            }
        }
    }

    private static void createDeltaIndex(CairoConfiguration config, String dir, int[] keyAssignment, long rowOffset) {
        try (Path path = new Path().of(dir)) {
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
        try (Path path = new Path().of(dir)) {
            try (DeltaBitmapIndexWriter writer = new DeltaBitmapIndexWriter(config, path, "test", COLUMN_NAME_TXN)) {
                for (int i = 0; i < keyAssignment.length; i++) {
                    writer.add(keyAssignment[i], (long) i + rowOffset);
                }
            }
        }
    }

    private static void createFORIndex(CairoConfiguration config, String dir, int[] keyAssignment, long rowOffset) {
        try (Path path = new Path().of(dir)) {
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
        try (Path path = new Path().of(dir)) {
            try (FORBitmapIndexWriter writer = new FORBitmapIndexWriter(config)) {
                writer.of(path, "test", COLUMN_NAME_TXN);
                for (int i = 0; i < keyAssignment.length; i++) {
                    writer.add(keyAssignment[i], (long) i + rowOffset);
                }
            }
        }
    }

    private static void createLZ4Index(CairoConfiguration config, String dir, int[] keyAssignment, int valsPerKey, long rowOffset) {
        int keysPerPage = LZ4BitmapIndexUtils.computeKeysPerPage(valsPerKey, LZ4_PAGE_SIZE);
        try (Path path = new Path().of(dir)) {
            int plen = path.size();
            FilesFacade ff = config.getFilesFacade();
            try (MemoryMA mem = Vm.getSmallCMARWInstance(
                    ff,
                    LZ4BitmapIndexUtils.keyFileName(path, "test", COLUMN_NAME_TXN),
                    MemoryTag.MMAP_DEFAULT,
                    config.getWriterFileOpenOpts()
            )) {
                LZ4BitmapIndexWriter.initKeyMemory(mem, valsPerKey, keysPerPage);
            }
            ff.touch(LZ4BitmapIndexUtils.valueFileName(path.trimTo(plen), "test", COLUMN_NAME_TXN));
        }
        try (Path path = new Path().of(dir)) {
            try (LZ4BitmapIndexWriter writer = new LZ4BitmapIndexWriter(config, path, "test", COLUMN_NAME_TXN)) {
                for (int i = 0; i < keyAssignment.length; i++) {
                    writer.add(keyAssignment[i], (long) i + rowOffset);
                }
            }
        }
    }

    private static void createFSSTIndex(CairoConfiguration config, String dir, int[] keyAssignment, int valsPerKey, long rowOffset) {
        try (Path path = new Path().of(dir)) {
            int plen = path.size();
            FilesFacade ff = config.getFilesFacade();
            try (MemoryMA mem = Vm.getSmallCMARWInstance(
                    ff,
                    FSSTBitmapIndexUtils.keyFileName(path, "test", COLUMN_NAME_TXN),
                    MemoryTag.MMAP_DEFAULT,
                    config.getWriterFileOpenOpts()
            )) {
                FSSTBitmapIndexWriter.initKeyMemory(mem, valsPerKey);
            }
            ff.touch(FSSTBitmapIndexUtils.valueFileName(path.trimTo(plen), "test", COLUMN_NAME_TXN));
        }
        try (Path path = new Path().of(dir)) {
            try (FSSTBitmapIndexWriter writer = new FSSTBitmapIndexWriter(config)) {
                writer.of(path, "test", COLUMN_NAME_TXN, false);
                for (int i = 0; i < keyAssignment.length; i++) {
                    writer.add(keyAssignment[i], (long) i + rowOffset);
                }
            }
        }
    }

    private static double measureReadLatency(Format format, CairoConfiguration config, String dir, int[] readKeys) {
        ReadTest test = () -> {
            try (Path path = new Path().of(dir)) {
                BitmapIndexReader reader = switch (format) {
                    case LEGACY -> new BitmapIndexFwdReader(config, path, "test", COLUMN_NAME_TXN, -1, 0);
                    case DELTA -> new DeltaBitmapIndexFwdReader(config, path, "test", COLUMN_NAME_TXN, -1, 0);
                    case FOR -> new FORBitmapIndexFwdReader(config, path, "test", COLUMN_NAME_TXN, -1, 0);
                    case LZ4 -> new LZ4BitmapIndexFwdReader(config, path, "test", COLUMN_NAME_TXN, -1, 0);
                    case FSST -> new FSSTBitmapIndexFwdReader(config, path, "test", COLUMN_NAME_TXN, -1, 0);
                    case BP -> new BPBitmapIndexFwdReader(config, path, "test", COLUMN_NAME_TXN, -1, 0);
                };
                try {
                    readBatch(reader, readKeys);
                } finally {
                    Misc.free(reader);
                }
            }
        };

        // Warmup
        for (int i = 0; i < READ_WARMUP; i++) {
            test.run();
        }

        // Measure
        long total = 0;
        for (int i = 0; i < READ_RUNS; i++) {
            long t0 = System.nanoTime();
            test.run();
            total += System.nanoTime() - t0;
        }
        return total / (READ_RUNS * 1e6);
    }

    // ========================= Read =========================

    private static void readBatch(BitmapIndexReader reader, int[] keys) {
        for (int key : keys) {
            RowCursor cursor = reader.getCursor(true, key, 0, Long.MAX_VALUE);
            while (cursor.hasNext()) {
                cursor.next();
            }
        }
    }

    private static FormatResult runFormat(
            Format format,
            CairoConfiguration config,
            String dir,
            int[] keyAssignment,
            int valsPerKey,
            long rowOffset,
            int[] readKeys
    ) {
        FormatResult fr = new FormatResult();

        // Write phase
        long writeT0 = System.nanoTime();
        switch (format) {
            case LEGACY:
                createLegacyIndex(config, dir, keyAssignment, valsPerKey, rowOffset);
                break;
            case DELTA:
                createDeltaIndex(config, dir, keyAssignment, rowOffset);
                break;
            case FOR:
                createFORIndex(config, dir, keyAssignment, rowOffset);
                break;
            case LZ4:
                createLZ4Index(config, dir, keyAssignment, valsPerKey, rowOffset);
                break;
            case FSST:
                createFSSTIndex(config, dir, keyAssignment, valsPerKey, rowOffset);
                break;
            case BP:
                createBPIndex(config, dir, keyAssignment, rowOffset);
                break;
        }
        fr.writeSec = (System.nanoTime() - writeT0) / 1e9;

        // Size
        long sizeBytes = getDirectorySize(dir);
        fr.sizeMB = sizeBytes / (1024.0 * 1024.0);
        fr.bPerVal = (double) sizeBytes / TOTAL_ROWS;

        // Read latency
        fr.readMs = measureReadLatency(format, config, dir, readKeys);

        return fr;
    }

    // ========================= Key assignment =========================

    private static int[] buildKeyAssignment(Distribution dist, int totalRows, int keyCount) {
        int[] assignment = new int[totalRows];
        switch (dist) {
            case ROUND_ROBIN:
                for (int i = 0; i < totalRows; i++) {
                    assignment[i] = i % keyCount;
                }
                break;
            case RANDOM:
                // Start with round-robin, then Fisher-Yates shuffle
                for (int i = 0; i < totalRows; i++) {
                    assignment[i] = i % keyCount;
                }
                Random random = new Random(42);
                for (int i = totalRows - 1; i > 0; i--) {
                    int j = random.nextInt(i + 1);
                    int tmp = assignment[i];
                    assignment[i] = assignment[j];
                    assignment[j] = tmp;
                }
                break;
        }
        return assignment;
    }

    private static int[] selectReadKeys(int keyCount) {
        int n = Math.min(MAX_READ_KEYS, keyCount);
        int[] keys = new int[n];
        Random rng = new Random(99);
        for (int i = 0; i < n; i++) {
            keys[i] = rng.nextInt(keyCount);
        }
        return keys;
    }

    // ========================= Output =========================

    private static String formatLabel(Format format) {
        return switch (format) {
            case LEGACY -> "Legacy";
            case DELTA -> "Delta";
            case FOR -> "FOR";
            case LZ4 -> "LZ4 4KB";
            case FSST -> "FSST";
            case BP -> "BP";
        };
    }

    private static void printSummaryTables(List<ScenarioResult> allResults) {
        Format[] formats = Format.values();

        // Size summary
        System.out.printf("%n%n=== SUMMARY: Size (MB) ===%n%n");
        System.out.printf("  %-45s", "Scenario");
        for (Format f : formats) {
            System.out.printf(" %10s", formatLabel(f));
        }
        System.out.println();
        String dashes = "  " + "─".repeat(45 + formats.length * 11);
        System.out.println(dashes);

        for (ScenarioResult r : allResults) {
            System.out.printf("  %-45s",
                    String.format("%s %3d v/k %,6dK keys off=%s", r.dist.name().substring(0, 3), r.valsPerKey, r.keyCount / 1000, r.offsetLabel));
            for (Format f : formats) {
                System.out.printf(" %10.1f", r.sizeMB[f.ordinal()]);
            }
            System.out.println();
        }

        // Read latency summary
        System.out.printf("%n%n=== SUMMARY: Read Latency (ms) ===%n%n");
        System.out.printf("  %-45s", "Scenario");
        for (Format f : formats) {
            System.out.printf(" %10s", formatLabel(f));
        }
        System.out.println();
        System.out.println(dashes);

        for (ScenarioResult r : allResults) {
            System.out.printf("  %-45s",
                    String.format("%s %3d v/k %,6dK keys off=%s", r.dist.name().substring(0, 3), r.valsPerKey, r.keyCount / 1000, r.offsetLabel));
            for (Format f : formats) {
                System.out.printf(" %10.1f", r.readMs[f.ordinal()]);
            }
            System.out.println();
        }

        // B/val summary
        System.out.printf("%n%n=== SUMMARY: Bytes/Value ===%n%n");
        System.out.printf("  %-45s", "Scenario");
        for (Format f : formats) {
            System.out.printf(" %10s", formatLabel(f));
        }
        System.out.println();
        System.out.println(dashes);

        for (ScenarioResult r : allResults) {
            System.out.printf("  %-45s",
                    String.format("%s %3d v/k %,6dK keys off=%s", r.dist.name().substring(0, 3), r.valsPerKey, r.keyCount / 1000, r.offsetLabel));
            for (Format f : formats) {
                System.out.printf(" %10.1f", r.bPerVal[f.ordinal()]);
            }
            System.out.println();
        }
    }

    // ========================= Utilities =========================

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
        void run();
    }
}
