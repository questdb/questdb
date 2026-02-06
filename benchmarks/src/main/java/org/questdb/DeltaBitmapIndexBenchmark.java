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
import io.questdb.cairo.idx.BitmapIndexFwdReader;
import io.questdb.cairo.idx.BitmapIndexWriter;
import io.questdb.cairo.idx.DeltaBitmapIndexFwdReader;
import io.questdb.cairo.idx.DeltaBitmapIndexUtils;
import io.questdb.cairo.idx.DeltaBitmapIndexWriter;
import io.questdb.cairo.idx.FORBitmapIndexFwdReader;
import io.questdb.cairo.idx.FORBitmapIndexUtils;
import io.questdb.cairo.idx.FORBitmapIndexWriter;
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
import java.util.Random;
import java.util.concurrent.TimeUnit;

import static io.questdb.cairo.TableUtils.COLUMN_NAME_TXN_NONE;

/**
 * Benchmark comparing Legacy, Delta-encoded, and FOR Bitmap Indexes
 * with a realistic interleaved-key workload.
 * <p>
 * Simulates 10,000 symbol keys with 1,000 row IDs each (10M total).
 * Row IDs are randomly assigned to keys (uniform distribution), so within
 * each key the values have large, non-consecutive gaps — matching real
 * time-series symbol column behavior.
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
    private static final int KEY_COUNT = 10_000;
    private static final int VALUES_PER_KEY = 1_000;
    private static final int TOTAL_ROW_IDS = KEY_COUNT * VALUES_PER_KEY;
    private static final int READ_BATCH_SIZE = 100;

    private CairoConfiguration configuration;
    private DeltaBitmapIndexFwdReader deltaFwdReader;
    private Path deltaReadPath;
    private String deltaReadRoot;
    private String deltaRoot;
    private DeltaBitmapIndexWriter deltaWriter;
    private Path deltaWritePath;
    private FORBitmapIndexFwdReader forFwdReader;
    private Path forReadPath;
    private String forReadRoot;
    private String forRoot;
    private FORBitmapIndexWriter forWriter;
    private Path forWritePath;
    private BitmapIndexFwdReader legacyFwdReader;
    private Path legacyReadPath;
    private String legacyReadRoot;
    private String legacyRoot;
    private BitmapIndexWriter legacyWriter;
    private Path legacyWritePath;
    // Pre-computed: keyAssignment[rowId] = key that owns this row ID
    private int[] keyAssignment;
    // Pre-computed: random keys to read during read benchmarks
    private int[] readKeys;
    private Rnd rnd;

    /**
     * Compares storage sizes between legacy, delta-encoded, and FOR indexes.
     */
    public static void compareStorageSizes() {
        String tmpDir = System.getProperty("java.io.tmpdir");
        CairoConfiguration config = new DefaultCairoConfiguration(tmpDir);

        int[] keyAssignment = buildKeyAssignment();

        System.out.printf("%-15s %-15s %-15s %-15s %-15s%n",
                "Legacy (KB)", "Delta (KB)", "FOR (KB)", "Delta Ratio", "FOR Ratio");
        System.out.println("-".repeat(75));

        String legacyDir = tmpDir + File.separator + "legacy_size_" + System.nanoTime();
        String deltaDir = tmpDir + File.separator + "delta_size_" + System.nanoTime();
        String forDir = tmpDir + File.separator + "for_size_" + System.nanoTime();

        new File(legacyDir).mkdirs();
        new File(deltaDir).mkdirs();
        new File(forDir).mkdirs();

        try {
            // Build legacy index
            try (Path path = new Path().of(legacyDir)) {
                try (BitmapIndexWriter writer = new BitmapIndexWriter(config)) {
                    writer.of(path, "test", COLUMN_NAME_TXN, 256);
                    writeInterleaved(writer, keyAssignment);
                }
            }

            // Build delta index
            createDeltaIndex(config, deltaDir);
            try (Path path = new Path().of(deltaDir)) {
                try (DeltaBitmapIndexWriter writer = new DeltaBitmapIndexWriter(config, path, "test", COLUMN_NAME_TXN)) {
                    writeInterleaved(writer, keyAssignment);
                }
            }

            // Build FOR index
            createFORIndex(config, forDir);
            try (Path path = new Path().of(forDir)) {
                try (FORBitmapIndexWriter writer = new FORBitmapIndexWriter(config)) {
                    writer.of(path, "test", COLUMN_NAME_TXN);
                    writeInterleaved(writer, keyAssignment);
                }
            }

            // Measure sizes
            long legacySize = getDirectorySize(legacyDir);
            long deltaSize = getDirectorySize(deltaDir);
            long forSize = getDirectorySize(forDir);
            double deltaRatio = (double) legacySize / deltaSize;
            double forRatio = (double) legacySize / forSize;

            System.out.printf("%-15.1f %-15.1f %-15.1f %-15.2fx %-15.2fx%n",
                    legacySize / 1024.0, deltaSize / 1024.0, forSize / 1024.0, deltaRatio, forRatio);
        } finally {
            deleteDir(legacyDir);
            deleteDir(deltaDir);
            deleteDir(forDir);
        }
    }

    public static void main(String[] args) throws RunnerException {
        System.out.println("=== Storage Size Comparison: Legacy vs Delta vs FOR Index ===");
        System.out.printf("    %d keys, %d values/key, %d total row IDs (interleaved)%n%n",
                KEY_COUNT, VALUES_PER_KEY, TOTAL_ROW_IDS);
        compareStorageSizes();
        System.out.println();

        if (args.length > 0 && "sizeOnly".equals(args[0])) {
            System.out.println("Skipping JMH benchmarks (sizeOnly mode)");
            return;
        }

        // Run write benchmarks without GC profiler (writes allocate inherently)
        Options writeOpt = new OptionsBuilder()
                .include(DeltaBitmapIndexBenchmark.class.getSimpleName() + ".write.*")
                .build();
        new Runner(writeOpt).run();

        // Run read benchmarks with GC profiler to verify zero-alloc reads
        Options readOpt = new OptionsBuilder()
                .include(DeltaBitmapIndexBenchmark.class.getSimpleName() + ".read.*")
                .addProfiler("gc")
                .build();
        new Runner(readOpt).run();
    }

    @Benchmark
    public long readForwardDelta(Blackhole bh) {
        long sum = 0;
        for (int i = 0; i < READ_BATCH_SIZE; i++) {
            RowCursor cursor = deltaFwdReader.getCursor(true, readKeys[i], 0, Long.MAX_VALUE);
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
        for (int i = 0; i < READ_BATCH_SIZE; i++) {
            RowCursor cursor = forFwdReader.getCursor(true, readKeys[i], 0, Long.MAX_VALUE);
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
        for (int i = 0; i < READ_BATCH_SIZE; i++) {
            RowCursor cursor = legacyFwdReader.getCursor(true, readKeys[i], 0, Long.MAX_VALUE);
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

        // Pre-allocate writers so benchmark methods measure only write throughput
        legacyWritePath = new Path().of(legacyRoot);
        legacyWriter = new BitmapIndexWriter(configuration);
        legacyWriter.of(legacyWritePath, "test", COLUMN_NAME_TXN, 256);

        createDeltaIndex(configuration, deltaRoot);
        deltaWritePath = new Path().of(deltaRoot);
        deltaWriter = new DeltaBitmapIndexWriter(configuration, deltaWritePath, "test", COLUMN_NAME_TXN);

        createFORIndex(configuration, forRoot);
        forWritePath = new Path().of(forRoot);
        forWriter = new FORBitmapIndexWriter(configuration);
        forWriter.of(forWritePath, "test", COLUMN_NAME_TXN);
    }

    @Setup(Level.Trial)
    public void setupTrial() {
        String tmpDir = System.getProperty("java.io.tmpdir");
        configuration = new DefaultCairoConfiguration(tmpDir);
        rnd = new Rnd();

        // Pre-compute interleaved key assignments
        keyAssignment = buildKeyAssignment();

        // Pre-compute random keys for read benchmarks
        readKeys = new int[READ_BATCH_SIZE];
        for (int i = 0; i < READ_BATCH_SIZE; i++) {
            readKeys[i] = rnd.nextInt(KEY_COUNT);
        }

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

        // Pre-allocate readers
        legacyReadPath = new Path().of(legacyReadRoot);
        deltaReadPath = new Path().of(deltaReadRoot);
        forReadPath = new Path().of(forReadRoot);

        legacyFwdReader = new BitmapIndexFwdReader(
                configuration, legacyReadPath, "test", COLUMN_NAME_TXN, -1, 0);
        legacyReadPath.trimTo(legacyReadPath.size() - legacyReadRoot.length()).of(legacyReadRoot);

        deltaFwdReader = new DeltaBitmapIndexFwdReader(
                configuration, deltaReadPath, "test", COLUMN_NAME_TXN, -1, 0);
        deltaReadPath.trimTo(deltaReadPath.size() - deltaReadRoot.length()).of(deltaReadRoot);

        forFwdReader = new FORBitmapIndexFwdReader(
                configuration, forReadPath, "test", COLUMN_NAME_TXN, -1, 0);
        forReadPath.trimTo(forReadPath.size() - forReadRoot.length()).of(forReadRoot);
    }

    @TearDown(Level.Invocation)
    public void tearDownInvocation() {
        if (legacyWriter != null) {
            legacyWriter.close();
            legacyWriter = null;
        }
        if (deltaWriter != null) {
            deltaWriter.close();
            deltaWriter = null;
        }
        if (forWriter != null) {
            forWriter.close();
            forWriter = null;
        }
        if (legacyWritePath != null) {
            legacyWritePath.close();
            legacyWritePath = null;
        }
        if (deltaWritePath != null) {
            deltaWritePath.close();
            deltaWritePath = null;
        }
        if (forWritePath != null) {
            forWritePath.close();
            forWritePath = null;
        }
        deleteDirectory(legacyRoot);
        deleteDirectory(deltaRoot);
        deleteDirectory(forRoot);
    }

    @TearDown(Level.Trial)
    public void tearDownTrial() {
        if (legacyFwdReader != null) {
            legacyFwdReader.close();
        }
        if (deltaFwdReader != null) {
            deltaFwdReader.close();
        }
        if (forFwdReader != null) {
            forFwdReader.close();
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

    @Benchmark
    public void writeDelta(Blackhole bh) {
        writeInterleaved(deltaWriter, keyAssignment);
        bh.consume(deltaWriter.getMaxValue());
    }

    @Benchmark
    public void writeFOR(Blackhole bh) {
        writeInterleaved(forWriter, keyAssignment);
        bh.consume(forWriter.getMaxValue());
    }

    @Benchmark
    public void writeLegacy(Blackhole bh) {
        writeInterleaved(legacyWriter, keyAssignment);
        bh.consume(legacyWriter.getMaxValue());
    }

    // ==================== Helper Methods ====================

    /**
     * Builds the key assignment array: each key gets exactly VALUES_PER_KEY row IDs,
     * shuffled via Fisher-Yates so that row IDs are interleaved across keys.
     */
    private static int[] buildKeyAssignment() {
        int[] assignment = new int[TOTAL_ROW_IDS];
        // Fill: key k owns indices [k*VALUES_PER_KEY .. (k+1)*VALUES_PER_KEY)
        for (int k = 0; k < KEY_COUNT; k++) {
            for (int i = 0; i < VALUES_PER_KEY; i++) {
                assignment[k * VALUES_PER_KEY + i] = k;
            }
        }
        // Fisher-Yates shuffle
        Random random = new Random(42);
        for (int i = TOTAL_ROW_IDS - 1; i > 0; i--) {
            int j = random.nextInt(i + 1);
            int tmp = assignment[i];
            assignment[i] = assignment[j];
            assignment[j] = tmp;
        }
        return assignment;
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

    /**
     * Writes data with interleaved keys: iterates row IDs 0..TOTAL_ROW_IDS-1 in order,
     * assigning each to keyAssignment[rowId]. This satisfies the ascending-order-per-key
     * constraint since row IDs are encountered in ascending order globally.
     */
    private static void writeInterleaved(BitmapIndexWriter writer, int[] keyAssignment) {
        for (int rowId = 0; rowId < TOTAL_ROW_IDS; rowId++) {
            writer.add(keyAssignment[rowId], rowId);
        }
    }

    private static void writeInterleaved(DeltaBitmapIndexWriter writer, int[] keyAssignment) {
        for (int rowId = 0; rowId < TOTAL_ROW_IDS; rowId++) {
            writer.add(keyAssignment[rowId], rowId);
        }
    }

    private static void writeInterleaved(FORBitmapIndexWriter writer, int[] keyAssignment) {
        for (int rowId = 0; rowId < TOTAL_ROW_IDS; rowId++) {
            writer.add(keyAssignment[rowId], rowId);
        }
    }

    private void buildDeltaIndex(String root) {
        createDeltaIndex(configuration, root);
        try (Path path = new Path().of(root)) {
            try (DeltaBitmapIndexWriter writer = new DeltaBitmapIndexWriter(configuration, path, "test", COLUMN_NAME_TXN)) {
                writeInterleaved(writer, keyAssignment);
            }
        }
    }

    private void buildFORIndex(String root) {
        createFORIndex(configuration, root);
        try (Path path = new Path().of(root)) {
            try (FORBitmapIndexWriter writer = new FORBitmapIndexWriter(configuration)) {
                writer.of(path, "test", COLUMN_NAME_TXN);
                writeInterleaved(writer, keyAssignment);
            }
        }
    }

    private void buildLegacyIndex(String root) {
        try (Path path = new Path().of(root)) {
            try (BitmapIndexWriter writer = new BitmapIndexWriter(configuration)) {
                writer.of(path, "test", COLUMN_NAME_TXN, 256);
                writeInterleaved(writer, keyAssignment);
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
}
