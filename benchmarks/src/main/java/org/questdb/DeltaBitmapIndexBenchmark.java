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

import io.questdb.cairo.BitmapIndexBwdReader;
import io.questdb.cairo.BitmapIndexFwdReader;
import io.questdb.cairo.BitmapIndexWriter;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.DefaultCairoConfiguration;
import io.questdb.cairo.DeltaBitmapIndexBwdReader;
import io.questdb.cairo.DeltaBitmapIndexFwdReader;
import io.questdb.cairo.DeltaBitmapIndexUtils;
import io.questdb.cairo.DeltaBitmapIndexWriter;
import io.questdb.cairo.FORBitmapIndexBwdReader;
import io.questdb.cairo.FORBitmapIndexFwdReader;
import io.questdb.cairo.FORBitmapIndexUtils;
import io.questdb.cairo.FORBitmapIndexWriter;
import io.questdb.cairo.sql.RowCursor;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryMA;
import io.questdb.std.FilesFacade;
import io.questdb.std.MemoryTag;
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
 * Benchmark comparing QuestDB's existing BitmapIndex with the new Delta-encoded Bitmap Index.
 * <p>
 * Tests write performance, forward read performance, backward read performance,
 * and storage sizes across different data sizes and densities.
 * <p>
 * Delta encoding achieves compression by storing deltas between consecutive values
 * using variable-length encoding (1-9 bytes per delta depending on size).
 * <p>
 * <b>How to run:</b>
 * <pre>
 * # Build the benchmark JAR:
 * mvn clean package -pl benchmarks -DskipTests
 *
 * # Run storage size comparison only:
 * java -jar benchmarks/target/benchmarks.jar DeltaBitmapIndexBenchmark.main sizeOnly
 *
 * # Run full JMH benchmarks:
 * java -jar benchmarks/target/benchmarks.jar DeltaBitmapIndexBenchmark
 *
 * # Run with GC profiler to verify no allocations:
 * java -jar benchmarks/target/benchmarks.jar DeltaBitmapIndexBenchmark -prof gc
 *
 * # Run specific benchmark:
 * java -jar benchmarks/target/benchmarks.jar DeltaBitmapIndexBenchmark.writeDelta
 * </pre>
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(1)
public class DeltaBitmapIndexBenchmark {

    private static final long COLUMN_NAME_TXN = COLUMN_NAME_TXN_NONE;
    @Param({"SEQUENTIAL", "SMALL_GAPS", "LARGE_GAPS"})
    public DataPattern dataPattern;
    @Param({"1", "10", "100"})
    public int keyCount;
    @Param({"10000", "100000", "1000000"})
    public int valueCount;
    private CairoConfiguration configuration;
    private DeltaBitmapIndexBwdReader deltaBwdReader;
    private DeltaBitmapIndexFwdReader deltaFwdReader;
    private Path deltaReadPath;
    private String deltaReadRoot;
    private String deltaRoot;
    private FORBitmapIndexBwdReader forBwdReader;
    private FORBitmapIndexFwdReader forFwdReader;
    private Path forReadPath;
    private String forReadRoot;
    private String forRoot;
    private BitmapIndexBwdReader legacyBwdReader;
    // Pre-allocated readers for read benchmarks (avoid allocation during measurement)
    private BitmapIndexFwdReader legacyFwdReader;
    private Path legacyReadPath;
    // Pre-built indexes for read benchmarks
    private String legacyReadRoot;
    private String legacyRoot;
    private Rnd rnd;

    /**
     * Compares storage sizes between legacy, delta-encoded, and FOR indexes.
     */
    public static void compareStorageSizes() {
        String tmpDir = System.getProperty("java.io.tmpdir");
        CairoConfiguration config = new DefaultCairoConfiguration(tmpDir);
        Rnd rnd = new Rnd();

        int[] valueCounts = {10_000, 100_000, 1_000_000};
        int[] keyCounts = {1, 10, 100};
        DataPattern[] patterns = {DataPattern.SEQUENTIAL, DataPattern.SMALL_GAPS, DataPattern.LARGE_GAPS};

        System.out.printf("%-12s %-8s %-12s %-15s %-15s %-15s %-15s %-15s%n",
                "Values", "Keys", "Pattern", "Legacy (KB)", "Delta (KB)", "FOR (KB)", "Delta Ratio", "FOR Ratio");
        System.out.println("-".repeat(120));

        for (int valueCount : valueCounts) {
            for (int keyCount : keyCounts) {
                for (DataPattern pattern : patterns) {
                    String legacyDir = tmpDir + File.separator + "legacy_size_" + System.nanoTime();
                    String deltaDir = tmpDir + File.separator + "delta_size_" + System.nanoTime();
                    String forDir = tmpDir + File.separator + "for_size_" + System.nanoTime();

                    new File(legacyDir).mkdirs();
                    new File(deltaDir).mkdirs();
                    new File(forDir).mkdirs();

                    try {
                        // Build legacy index
                        rnd.reset();
                        try (Path path = new Path().of(legacyDir)) {
                            try (BitmapIndexWriter writer = new BitmapIndexWriter(config)) {
                                writer.of(path, "test", COLUMN_NAME_TXN, 256);
                                writeDataStatic(writer, valueCount, keyCount, pattern, rnd);
                            }
                        }

                        // Build delta index
                        rnd.reset();
                        createDeltaIndex(config, deltaDir);
                        try (Path path = new Path().of(deltaDir)) {
                            try (DeltaBitmapIndexWriter writer = new DeltaBitmapIndexWriter(config, path, "test", COLUMN_NAME_TXN)) {
                                writeDataStaticDelta(writer, valueCount, keyCount, pattern, rnd);
                            }
                        }

                        // Build FOR index
                        rnd.reset();
                        createFORIndex(config, forDir);
                        try (Path path = new Path().of(forDir)) {
                            try (FORBitmapIndexWriter writer = new FORBitmapIndexWriter(config)) {
                                writer.of(path, "test", COLUMN_NAME_TXN);
                                writeDataStaticFOR(writer, valueCount, keyCount, pattern, rnd);
                            }
                        }

                        // Measure sizes
                        long legacySize = getDirectorySize(legacyDir);
                        long deltaSize = getDirectorySize(deltaDir);
                        long forSize = getDirectorySize(forDir);
                        double deltaRatio = (double) legacySize / deltaSize;
                        double forRatio = (double) legacySize / forSize;

                        System.out.printf("%-12d %-8d %-12s %-15.1f %-15.1f %-15.1f %-15.2fx %-15.2fx%n",
                                valueCount, keyCount, pattern,
                                legacySize / 1024.0, deltaSize / 1024.0, forSize / 1024.0, deltaRatio, forRatio);
                    } finally {
                        deleteDir(legacyDir);
                        deleteDir(deltaDir);
                        deleteDir(forDir);
                    }
                }
            }
        }
    }

    public static void main(String[] args) throws RunnerException {
        // Run storage size comparison first
        System.out.println("=== Storage Size Comparison: Legacy vs Delta vs FOR Index ===\n");
        compareStorageSizes();
        System.out.println();

        // Check if we should skip JMH (useful for quick size comparisons)
        if (args.length > 0 && "sizeOnly".equals(args[0])) {
            System.out.println("Skipping JMH benchmarks (sizeOnly mode)");
            return;
        }

        // Then run JMH benchmarks
        Options opt = new OptionsBuilder()
                .include(DeltaBitmapIndexBenchmark.class.getSimpleName())
                .addProfiler("gc")  // Add GC profiler to track allocations
                .build();

        new Runner(opt).run();
    }

    @Benchmark
    public long rangeQueryDelta(Blackhole bh) {
        long sum = 0;
        int valuesPerKey = valueCount / keyCount;
        long multiplier = getPatternMultiplier(dataPattern);

        for (int key = 0; key < keyCount; key++) {
            long keyBase = (long) key * valuesPerKey * multiplier;
            long rangeStart = keyBase + valuesPerKey / 4;
            long rangeEnd = keyBase + valuesPerKey * 3 / 4;
            RowCursor cursor = deltaFwdReader.getCursor(true, key, rangeStart, rangeEnd);
            while (cursor.hasNext()) {
                sum += cursor.next();
            }
        }
        bh.consume(sum);
        return sum;
    }

    @Benchmark
    public long rangeQueryFOR(Blackhole bh) {
        long sum = 0;
        int valuesPerKey = valueCount / keyCount;
        long multiplier = getPatternMultiplier(dataPattern);

        for (int key = 0; key < keyCount; key++) {
            long keyBase = (long) key * valuesPerKey * multiplier;
            long rangeStart = keyBase + valuesPerKey / 4;
            long rangeEnd = keyBase + valuesPerKey * 3 / 4;
            RowCursor cursor = forFwdReader.getCursor(true, key, rangeStart, rangeEnd);
            while (cursor.hasNext()) {
                sum += cursor.next();
            }
        }
        bh.consume(sum);
        return sum;
    }

    @Benchmark
    public long rangeQueryLegacy(Blackhole bh) {
        long sum = 0;
        int valuesPerKey = valueCount / keyCount;
        long multiplier = getPatternMultiplier(dataPattern);

        for (int key = 0; key < keyCount; key++) {
            long keyBase = (long) key * valuesPerKey * multiplier;
            long rangeStart = keyBase + valuesPerKey / 4;
            long rangeEnd = keyBase + valuesPerKey * 3 / 4;
            RowCursor cursor = legacyFwdReader.getCursor(true, key, rangeStart, rangeEnd);
            while (cursor.hasNext()) {
                sum += cursor.next();
            }
        }
        bh.consume(sum);
        return sum;
    }

    @Benchmark
    public long readBackwardDelta(Blackhole bh) {
        long sum = 0;
        for (int key = 0; key < keyCount; key++) {
            RowCursor cursor = deltaBwdReader.getCursor(true, key, 0, Long.MAX_VALUE);
            while (cursor.hasNext()) {
                sum += cursor.next();
            }
        }
        bh.consume(sum);
        return sum;
    }

    @Benchmark
    public long readBackwardFOR(Blackhole bh) {
        long sum = 0;
        for (int key = 0; key < keyCount; key++) {
            RowCursor cursor = forBwdReader.getCursor(true, key, 0, Long.MAX_VALUE);
            while (cursor.hasNext()) {
                sum += cursor.next();
            }
        }
        bh.consume(sum);
        return sum;
    }

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
    public long readForwardDelta(Blackhole bh) {
        long sum = 0;
        for (int key = 0; key < keyCount; key++) {
            RowCursor cursor = deltaFwdReader.getCursor(true, key, 0, Long.MAX_VALUE);
            while (cursor.hasNext()) {
                sum += cursor.next();
            }
        }
        bh.consume(sum);
        return sum;
    }

    @Benchmark
    public long readForwardFOR(Blackhole bh) {
        long sum = 0;
        for (int key = 0; key < keyCount; key++) {
            RowCursor cursor = forFwdReader.getCursor(true, key, 0, Long.MAX_VALUE);
            while (cursor.hasNext()) {
                sum += cursor.next();
            }
        }
        bh.consume(sum);
        return sum;
    }

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

    @Setup(Level.Invocation)
    public void setupInvocation() {
        String tmpDir = System.getProperty("java.io.tmpdir");
        legacyRoot = tmpDir + File.separator + "legacy_" + System.nanoTime();
        deltaRoot = tmpDir + File.separator + "delta_" + System.nanoTime();
        forRoot = tmpDir + File.separator + "for_" + System.nanoTime();

        new File(legacyRoot).mkdirs();
        new File(deltaRoot).mkdirs();
        new File(forRoot).mkdirs();

        rnd.reset();
    }

    @Setup(Level.Trial)
    public void setupTrial() {
        String tmpDir = System.getProperty("java.io.tmpdir");
        configuration = new DefaultCairoConfiguration(tmpDir);
        rnd = new Rnd();

        // Create directories for read benchmarks and pre-populate indexes
        legacyReadRoot = tmpDir + File.separator + "legacy_read_" + System.nanoTime();
        deltaReadRoot = tmpDir + File.separator + "delta_read_" + System.nanoTime();
        forReadRoot = tmpDir + File.separator + "for_read_" + System.nanoTime();

        new File(legacyReadRoot).mkdirs();
        new File(deltaReadRoot).mkdirs();
        new File(forReadRoot).mkdirs();

        // Pre-build indexes for read benchmarks
        buildLegacyIndex(legacyReadRoot);
        buildDeltaIndex(deltaReadRoot);
        buildFORIndex(forReadRoot);

        // Pre-allocate readers (avoid allocation during measurement)
        legacyReadPath = new Path().of(legacyReadRoot);
        deltaReadPath = new Path().of(deltaReadRoot);
        forReadPath = new Path().of(forReadRoot);

        legacyFwdReader = new BitmapIndexFwdReader(
                configuration, legacyReadPath, "test", COLUMN_NAME_TXN, -1, 0);
        legacyReadPath.trimTo(legacyReadPath.size() - legacyReadRoot.length()).of(legacyReadRoot);

        legacyBwdReader = new BitmapIndexBwdReader(
                configuration, legacyReadPath, "test", COLUMN_NAME_TXN, -1, 0);
        legacyReadPath.trimTo(legacyReadPath.size() - legacyReadRoot.length()).of(legacyReadRoot);

        deltaFwdReader = new DeltaBitmapIndexFwdReader(
                configuration, deltaReadPath, "test", COLUMN_NAME_TXN, -1, 0);
        deltaReadPath.trimTo(deltaReadPath.size() - deltaReadRoot.length()).of(deltaReadRoot);

        deltaBwdReader = new DeltaBitmapIndexBwdReader(
                configuration, deltaReadPath, "test", COLUMN_NAME_TXN, -1, 0);
        deltaReadPath.trimTo(deltaReadPath.size() - deltaReadRoot.length()).of(deltaReadRoot);

        forFwdReader = new FORBitmapIndexFwdReader(
                configuration, forReadPath, "test", COLUMN_NAME_TXN, -1, 0);
        forReadPath.trimTo(forReadPath.size() - forReadRoot.length()).of(forReadRoot);

        forBwdReader = new FORBitmapIndexBwdReader(
                configuration, forReadPath, "test", COLUMN_NAME_TXN, -1, 0);
    }

    @TearDown(Level.Invocation)
    public void tearDownInvocation() {
        deleteDirectory(legacyRoot);
        deleteDirectory(deltaRoot);
        deleteDirectory(forRoot);
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
        if (deltaFwdReader != null) {
            deltaFwdReader.close();
        }
        if (deltaBwdReader != null) {
            deltaBwdReader.close();
        }
        if (forFwdReader != null) {
            forFwdReader.close();
        }
        if (forBwdReader != null) {
            forBwdReader.close();
        }
        if (legacyReadPath != null) {
            legacyReadPath.close();
        }
        if (deltaReadPath != null) {
            deltaReadPath.close();
        }
        if (forReadPath != null) {
            forReadPath.close();
        }

        deleteDirectory(legacyReadRoot);
        deleteDirectory(deltaReadRoot);
        deleteDirectory(forReadRoot);
    }

    // ==================== Write Benchmarks ====================

    @Benchmark
    public void writeDelta(Blackhole bh) {
        createDeltaIndex(configuration, deltaRoot);
        try (Path path = new Path().of(deltaRoot)) {
            try (DeltaBitmapIndexWriter writer = new DeltaBitmapIndexWriter(configuration, path, "test", COLUMN_NAME_TXN)) {
                writeDataDelta(writer);
                bh.consume(writer.getMaxValue());
            }
        }
    }

    @Benchmark
    public void writeFOR(Blackhole bh) {
        createFORIndex(configuration, forRoot);
        try (Path path = new Path().of(forRoot)) {
            try (FORBitmapIndexWriter writer = new FORBitmapIndexWriter(configuration)) {
                writer.of(path, "test", COLUMN_NAME_TXN);
                writeDataFOR(writer);
                bh.consume(writer.getMaxValue());
            }
        }
    }

    @Benchmark
    public void writeLegacy(Blackhole bh) {
        try (Path path = new Path().of(legacyRoot)) {
            try (BitmapIndexWriter writer = new BitmapIndexWriter(configuration)) {
                writer.of(path, "test", COLUMN_NAME_TXN, 256);
                writeData(writer);
                bh.consume(writer.getMaxValue());
            }
        }
    }

    // ==================== Forward Read Benchmarks ====================

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

    // ==================== Backward Read Benchmarks ====================

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

    private static long getNextDelta(DataPattern pattern, Rnd rnd) {
        switch (pattern) {
            case SEQUENTIAL:
                return 1;  // Delta = 1, best compression (1 byte per value)
            case SMALL_GAPS:
                return 1 + rnd.nextInt(10);  // Delta 1-10, good compression (1-2 bytes)
            case LARGE_GAPS:
                return 100 + rnd.nextInt(1000);  // Delta 100-1100, moderate compression (2-4 bytes)
            default:
                return 1;
        }
    }

    private static int getPatternMultiplier(DataPattern pattern) {
        switch (pattern) {
            case SEQUENTIAL:
                return 1;
            case SMALL_GAPS:
                return 5;
            case LARGE_GAPS:
                return 1000;
            default:
                return 1;
        }
    }

    // ==================== Range Query Benchmarks ====================

    private static void writeDataStatic(BitmapIndexWriter writer, int valueCount, int keyCount, DataPattern pattern, Rnd rnd) {
        int valuesPerKey = valueCount / keyCount;
        for (int key = 0; key < keyCount; key++) {
            long value = (long) key * valuesPerKey * getPatternMultiplier(pattern);
            for (int i = 0; i < valuesPerKey; i++) {
                writer.add(key, value);
                value += getNextDelta(pattern, rnd);
            }
        }
    }

    private static void writeDataStaticDelta(DeltaBitmapIndexWriter writer, int valueCount, int keyCount, DataPattern pattern, Rnd rnd) {
        int valuesPerKey = valueCount / keyCount;
        for (int key = 0; key < keyCount; key++) {
            long value = (long) key * valuesPerKey * getPatternMultiplier(pattern);
            for (int i = 0; i < valuesPerKey; i++) {
                writer.add(key, value);
                value += getNextDelta(pattern, rnd);
            }
        }
    }

    private static void writeDataStaticFOR(FORBitmapIndexWriter writer, int valueCount, int keyCount, DataPattern pattern, Rnd rnd) {
        int valuesPerKey = valueCount / keyCount;
        for (int key = 0; key < keyCount; key++) {
            long value = (long) key * valuesPerKey * getPatternMultiplier(pattern);
            for (int i = 0; i < valuesPerKey; i++) {
                writer.add(key, value);
                value += getNextDelta(pattern, rnd);
            }
        }
    }

    // ==================== Helper Methods ====================

    private void buildDeltaIndex(String root) {
        createDeltaIndex(configuration, root);
        try (Path path = new Path().of(root)) {
            try (DeltaBitmapIndexWriter writer = new DeltaBitmapIndexWriter(configuration, path, "test", COLUMN_NAME_TXN)) {
                writeDataDelta(writer);
            }
        }
    }

    private void buildFORIndex(String root) {
        createFORIndex(configuration, root);
        try (Path path = new Path().of(root)) {
            try (FORBitmapIndexWriter writer = new FORBitmapIndexWriter(configuration)) {
                writer.of(path, "test", COLUMN_NAME_TXN);
                writeDataFOR(writer);
            }
        }
    }

    private void buildLegacyIndex(String root) {
        try (Path path = new Path().of(root)) {
            try (BitmapIndexWriter writer = new BitmapIndexWriter(configuration)) {
                writer.of(path, "test", COLUMN_NAME_TXN, 256);
                writeData(writer);
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

    private void writeData(BitmapIndexWriter writer) {
        int valuesPerKey = valueCount / keyCount;
        rnd.reset();

        for (int key = 0; key < keyCount; key++) {
            long value = (long) key * valuesPerKey * getPatternMultiplier(dataPattern);
            for (int i = 0; i < valuesPerKey; i++) {
                writer.add(key, value);
                value += getNextDelta(dataPattern, rnd);
            }
        }
    }

    private void writeDataDelta(DeltaBitmapIndexWriter writer) {
        int valuesPerKey = valueCount / keyCount;
        rnd.reset();

        for (int key = 0; key < keyCount; key++) {
            long value = (long) key * valuesPerKey * getPatternMultiplier(dataPattern);
            for (int i = 0; i < valuesPerKey; i++) {
                writer.add(key, value);
                value += getNextDelta(dataPattern, rnd);
            }
        }
    }

    private void writeDataFOR(FORBitmapIndexWriter writer) {
        int valuesPerKey = valueCount / keyCount;
        rnd.reset();

        for (int key = 0; key < keyCount; key++) {
            long value = (long) key * valuesPerKey * getPatternMultiplier(dataPattern);
            for (int i = 0; i < valuesPerKey; i++) {
                writer.add(key, value);
                value += getNextDelta(dataPattern, rnd);
            }
        }
    }

    /**
     * Data patterns for benchmarking different compression scenarios.
     */
    public enum DataPattern {
        /**
         * Consecutive row IDs (delta=1). Best case for delta encoding.
         * Expected: ~8x compression (8 bytes -> 1 byte per value after first)
         */
        SEQUENTIAL,
        /**
         * Small gaps between values (delta 1-10). Good compression.
         * Expected: ~4-6x compression (1-2 bytes per delta)
         */
        SMALL_GAPS,
        /**
         * Large gaps between values (delta 100-1100). Moderate compression.
         * Expected: ~2-3x compression (2-4 bytes per delta)
         */
        LARGE_GAPS
    }
}
