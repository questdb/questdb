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

import io.questdb.cairo.idx.BitmapIndexBwdReader;
import io.questdb.cairo.idx.BitmapIndexFwdReader;
import io.questdb.cairo.idx.BitmapIndexWriter;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.DefaultCairoConfiguration;
import io.questdb.cairo.idx.RoaringBitmapIndexBwdReader;
import io.questdb.cairo.idx.RoaringBitmapIndexFwdReader;
import io.questdb.cairo.idx.RoaringBitmapIndexWriter;
import io.questdb.cairo.sql.RowCursor;
import io.questdb.std.Rnd;
import io.questdb.std.str.Path;
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
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.io.File;
import java.util.concurrent.TimeUnit;

import static io.questdb.cairo.TableUtils.COLUMN_NAME_TXN_NONE;

/**
 * Benchmark comparing QuestDB's existing BitmapIndex with the new Roaring Bitmap Index.
 * <p>
 * Tests write performance, forward read performance, and backward read performance
 * across different data sizes and densities.
 * <p>
 * <b>How to run:</b>
 * <pre>
 * # Build the benchmark JAR:
 * mvn clean package -pl benchmarks -DskipTests
 *
 * # Run storage size comparison only:
 * java -jar benchmarks/target/benchmarks.jar BitmapIndexBenchmark sizeOnly
 *
 * # Run full JMH benchmarks:
 * java -jar benchmarks/target/benchmarks.jar BitmapIndexBenchmark
 *
 * # Run specific benchmark:
 * java -jar benchmarks/target/benchmarks.jar BitmapIndexBenchmark.writeLegacy
 * </pre>
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(1)
public class BitmapIndexBenchmark {

    private static final long COLUMN_NAME_TXN = COLUMN_NAME_TXN_NONE;

    @Param({"10000", "100000", "1000000"})
    public int valueCount;

    @Param({"1", "10", "100"})
    public int keyCount;

    @Param({"DENSE", "SPARSE"})
    public DataPattern dataPattern;

    private CairoConfiguration configuration;
    private String legacyRoot;
    private String roaringRoot;
    private Rnd rnd;

    // Pre-built indexes for read benchmarks
    private String legacyReadRoot;
    private String roaringReadRoot;

    // Pre-allocated readers for read benchmarks (avoid allocation during measurement)
    private BitmapIndexFwdReader legacyFwdReader;
    private BitmapIndexBwdReader legacyBwdReader;
    private RoaringBitmapIndexFwdReader roaringFwdReader;
    private RoaringBitmapIndexBwdReader roaringBwdReader;
    private Path legacyReadPath;
    private Path roaringReadPath;

    public static void main(String[] args) throws RunnerException {
        // Run storage size comparison first
        System.out.println("=== Storage Size Comparison ===\n");
        compareStorageSizes();
        System.out.println();

        // Check if we should skip JMH (useful for quick size comparisons)
        if (args.length > 0 && "sizeOnly".equals(args[0])) {
            System.out.println("Skipping JMH benchmarks (sizeOnly mode)");
            return;
        }

        // Then run JMH benchmarks
        Options opt = new OptionsBuilder()
                .include(BitmapIndexBenchmark.class.getSimpleName())
                .build();

        new Runner(opt).run();
    }

    /**
     * Compares storage sizes between legacy and roaring indexes.
     */
    public static void compareStorageSizes() {
        String tmpDir = System.getProperty("java.io.tmpdir");
        CairoConfiguration config = new DefaultCairoConfiguration(tmpDir);
        Rnd rnd = new Rnd();

        int[] valueCounts = {10_000, 100_000, 1_000_000};
        int[] keyCounts = {1, 10, 100};
        DataPattern[] patterns = {DataPattern.DENSE, DataPattern.SPARSE};

        System.out.printf("%-12s %-8s %-8s %-15s %-15s %-10s%n",
                "Values", "Keys", "Pattern", "Legacy (KB)", "Roaring (KB)", "Ratio");
        System.out.println("-".repeat(75));

        for (int valueCount : valueCounts) {
            for (int keyCount : keyCounts) {
                for (DataPattern pattern : patterns) {
                    String legacyDir = tmpDir + File.separator + "legacy_size_" + System.nanoTime();
                    String roaringDir = tmpDir + File.separator + "roaring_size_" + System.nanoTime();

                    new File(legacyDir).mkdirs();
                    new File(roaringDir).mkdirs();

                    try {
                        // Build legacy index
                        rnd.reset();
                        try (Path path = new Path().of(legacyDir)) {
                            try (BitmapIndexWriter writer = new BitmapIndexWriter(config)) {
                                writer.of(path, "test", COLUMN_NAME_TXN, 256);
                                writeDataStatic(writer, valueCount, keyCount, pattern, rnd);
                            }
                        }

                        // Build roaring index
                        rnd.reset();
                        try (Path path = new Path().of(roaringDir)) {
                            try (RoaringBitmapIndexWriter writer = new RoaringBitmapIndexWriter(config, path, "test", COLUMN_NAME_TXN)) {
                                writeDataStaticRoaring(writer, valueCount, keyCount, pattern, rnd);
                            }
                        }

                        // Measure sizes
                        long legacySize = getDirectorySize(legacyDir);
                        long roaringSize = getDirectorySize(roaringDir);
                        double ratio = (double) roaringSize / legacySize;

                        System.out.printf("%-12d %-8d %-8s %-15.1f %-15.1f %-10.2f%n",
                                valueCount, keyCount, pattern,
                                legacySize / 1024.0, roaringSize / 1024.0, ratio);
                    } finally {
                        deleteDir(legacyDir);
                        deleteDir(roaringDir);
                    }
                }
            }
        }
    }

    private static void writeDataStatic(BitmapIndexWriter writer, int valueCount, int keyCount, DataPattern pattern, Rnd rnd) {
        int valuesPerKey = valueCount / keyCount;
        for (int key = 0; key < keyCount; key++) {
            long baseValue = (long) key * valuesPerKey;
            for (int i = 0; i < valuesPerKey; i++) {
                long value = pattern == DataPattern.DENSE
                        ? baseValue + i
                        : baseValue + (long) i * 10 + rnd.nextInt(5);
                writer.add(key, value);
            }
        }
    }

    private static void writeDataStaticRoaring(RoaringBitmapIndexWriter writer, int valueCount, int keyCount, DataPattern pattern, Rnd rnd) {
        int valuesPerKey = valueCount / keyCount;
        for (int key = 0; key < keyCount; key++) {
            long baseValue = (long) key * valuesPerKey;
            for (int i = 0; i < valuesPerKey; i++) {
                long value = pattern == DataPattern.DENSE
                        ? baseValue + i
                        : baseValue + (long) i * 10 + rnd.nextInt(5);
                writer.add(key, value);
            }
        }
        writer.commit();
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

    @Setup(Level.Trial)
    public void setupTrial() {
        String tmpDir = System.getProperty("java.io.tmpdir");
        configuration = new DefaultCairoConfiguration(tmpDir);
        rnd = new Rnd();

        // Create directories for read benchmarks and pre-populate indexes
        legacyReadRoot = tmpDir + File.separator + "legacy_read_" + System.nanoTime();
        roaringReadRoot = tmpDir + File.separator + "roaring_read_" + System.nanoTime();

        new File(legacyReadRoot).mkdirs();
        new File(roaringReadRoot).mkdirs();

        // Pre-build indexes for read benchmarks
        buildLegacyIndex(legacyReadRoot);
        buildRoaringIndex(roaringReadRoot);

        // Pre-allocate readers (avoid allocation during measurement)
        legacyReadPath = new Path().of(legacyReadRoot);
        roaringReadPath = new Path().of(roaringReadRoot);

        legacyFwdReader = new BitmapIndexFwdReader(
                configuration, legacyReadPath, "test", COLUMN_NAME_TXN, -1, 0);
        legacyReadPath.trimTo(legacyReadPath.size() - legacyReadRoot.length()).of(legacyReadRoot);

        legacyBwdReader = new BitmapIndexBwdReader(
                configuration, legacyReadPath, "test", COLUMN_NAME_TXN, -1, 0);
        legacyReadPath.trimTo(legacyReadPath.size() - legacyReadRoot.length()).of(legacyReadRoot);

        roaringFwdReader = new RoaringBitmapIndexFwdReader(
                configuration, roaringReadPath, "test", COLUMN_NAME_TXN, -1, 0);
        roaringReadPath.trimTo(roaringReadPath.size() - roaringReadRoot.length()).of(roaringReadRoot);

        roaringBwdReader = new RoaringBitmapIndexBwdReader(
                configuration, roaringReadPath, "test", COLUMN_NAME_TXN, -1, 0);
    }

    @Setup(Level.Invocation)
    public void setupInvocation() {
        String tmpDir = System.getProperty("java.io.tmpdir");
        legacyRoot = tmpDir + File.separator + "legacy_" + System.nanoTime();
        roaringRoot = tmpDir + File.separator + "roaring_" + System.nanoTime();

        new File(legacyRoot).mkdirs();
        new File(roaringRoot).mkdirs();

        rnd.reset();
    }

    @TearDown(Level.Invocation)
    public void tearDownInvocation() {
        deleteDirectory(legacyRoot);
        deleteDirectory(roaringRoot);
    }

    @TearDown(Level.Trial)
    public void tearDownTrial() {
        // Close pre-allocated readers
        if (legacyFwdReader != null) {
            legacyFwdReader.close();
        }
        if (legacyBwdReader != null) {
            legacyBwdReader.close();
        }
        if (roaringFwdReader != null) {
            roaringFwdReader.close();
        }
        if (roaringBwdReader != null) {
            roaringBwdReader.close();
        }
        if (legacyReadPath != null) {
            legacyReadPath.close();
        }
        if (roaringReadPath != null) {
            roaringReadPath.close();
        }

        deleteDirectory(legacyReadRoot);
        deleteDirectory(roaringReadRoot);
    }

    // ==================== Write Benchmarks ====================

    @Benchmark
    public void writeLegacy(Blackhole bh) {
        try (Path path = new Path().of(legacyRoot)) {
            try (BitmapIndexWriter writer = new BitmapIndexWriter(configuration)) {
                writer.of(path, "test", COLUMN_NAME_TXN, 256);  // 256 is typical block capacity
                writeData(writer);
                bh.consume(writer.getMaxValue());
            }
        }
    }

    @Benchmark
    public void writeRoaring(Blackhole bh) {
        try (Path path = new Path().of(roaringRoot)) {
            try (RoaringBitmapIndexWriter writer = new RoaringBitmapIndexWriter(configuration, path, "test", COLUMN_NAME_TXN)) {
                writeDataRoaring(writer);
                bh.consume(writer.getMaxValue());
            }
        }
    }

    // ==================== Forward Read Benchmarks ====================

    @Benchmark
    public long readForwardLegacy(Blackhole bh) {
        long sum = 0;
        for (int key = 0; key < keyCount; key++) {
            RowCursor cursor = legacyFwdReader.getCursor(true, key, 0, Long.MAX_VALUE);
            while (cursor.hasNext()) {
                sum += cursor.next();
            }
        }
        bh.consume(sum);
        return sum;
    }

    @Benchmark
    public long readForwardRoaring(Blackhole bh) {
        long sum = 0;
        for (int key = 0; key < keyCount; key++) {
            RowCursor cursor = roaringFwdReader.getCursor(true, key, 0, Long.MAX_VALUE);
            while (cursor.hasNext()) {
                sum += cursor.next();
            }
        }
        bh.consume(sum);
        return sum;
    }

    // ==================== Backward Read Benchmarks ====================

    @Benchmark
    public long readBackwardLegacy(Blackhole bh) {
        long sum = 0;
        for (int key = 0; key < keyCount; key++) {
            RowCursor cursor = legacyBwdReader.getCursor(true, key, 0, Long.MAX_VALUE);
            while (cursor.hasNext()) {
                sum += cursor.next();
            }
        }
        bh.consume(sum);
        return sum;
    }

    @Benchmark
    public long readBackwardRoaring(Blackhole bh) {
        long sum = 0;
        for (int key = 0; key < keyCount; key++) {
            RowCursor cursor = roaringBwdReader.getCursor(true, key, 0, Long.MAX_VALUE);
            while (cursor.hasNext()) {
                sum += cursor.next();
            }
        }
        bh.consume(sum);
        return sum;
    }

    // ==================== Range Query Benchmarks ====================

    @Benchmark
    public long rangeQueryLegacy(Blackhole bh) {
        long sum = 0;
        int valuesPerKey = valueCount / keyCount;
        long rangeStart = valuesPerKey / 4;
        long rangeEnd = valuesPerKey * 3 / 4;

        for (int key = 0; key < keyCount; key++) {
            long keyBase = (long) key * valuesPerKey;
            RowCursor cursor = legacyFwdReader.getCursor(true, key, keyBase + rangeStart, keyBase + rangeEnd);
            while (cursor.hasNext()) {
                sum += cursor.next();
            }
        }
        bh.consume(sum);
        return sum;
    }

    @Benchmark
    public long rangeQueryRoaring(Blackhole bh) {
        long sum = 0;
        int valuesPerKey = valueCount / keyCount;
        long rangeStart = valuesPerKey / 4;
        long rangeEnd = valuesPerKey * 3 / 4;

        for (int key = 0; key < keyCount; key++) {
            long keyBase = (long) key * valuesPerKey;
            RowCursor cursor = roaringFwdReader.getCursor(true, key, keyBase + rangeStart, keyBase + rangeEnd);
            while (cursor.hasNext()) {
                sum += cursor.next();
            }
        }
        bh.consume(sum);
        return sum;
    }

    // ==================== Helper Methods ====================

    private void writeData(BitmapIndexWriter writer) {
        int valuesPerKey = valueCount / keyCount;
        rnd.reset();

        for (int key = 0; key < keyCount; key++) {
            long baseValue = (long) key * valuesPerKey;
            for (int i = 0; i < valuesPerKey; i++) {
                long value = dataPattern == DataPattern.DENSE
                        ? baseValue + i
                        : baseValue + (long) i * 10 + rnd.nextInt(5);
                writer.add(key, value);
            }
        }
    }

    private void writeDataRoaring(RoaringBitmapIndexWriter writer) {
        int valuesPerKey = valueCount / keyCount;
        rnd.reset();

        for (int key = 0; key < keyCount; key++) {
            long baseValue = (long) key * valuesPerKey;
            for (int i = 0; i < valuesPerKey; i++) {
                long value = dataPattern == DataPattern.DENSE
                        ? baseValue + i
                        : baseValue + (long) i * 10 + rnd.nextInt(5);
                writer.add(key, value);
            }
        }
        writer.commit();
    }

    private void buildLegacyIndex(String root) {
        try (Path path = new Path().of(root)) {
            try (BitmapIndexWriter writer = new BitmapIndexWriter(configuration)) {
                writer.of(path, "test", COLUMN_NAME_TXN, 256);
                writeData(writer);
            }
        }
    }

    private void buildRoaringIndex(String root) {
        try (Path path = new Path().of(root)) {
            try (RoaringBitmapIndexWriter writer = new RoaringBitmapIndexWriter(configuration, path, "test", COLUMN_NAME_TXN)) {
                writeDataRoaring(writer);
            }
        }
    }

    private void deleteDirectory(String path) {
        if (path == null) {
            return;
        }
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

    public enum DataPattern {
        DENSE,  // Consecutive row IDs (best case for RLE)
        SPARSE  // Row IDs with gaps (typical case)
    }
}
