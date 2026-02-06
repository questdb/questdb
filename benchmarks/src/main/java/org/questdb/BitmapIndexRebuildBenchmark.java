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

import io.questdb.cairo.BitmapIndexUtils;
import io.questdb.cairo.BitmapIndexWriter;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.DefaultCairoConfiguration;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryMA;
import io.questdb.std.FilesFacade;
import io.questdb.std.IntIntHashMap;
import io.questdb.std.MemoryTag;
import io.questdb.std.Numbers;
import io.questdb.std.Rnd;
import io.questdb.std.str.Path;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.io.File;
import java.util.concurrent.TimeUnit;

/**
 * Benchmark comparing bulk build (histogram + pre-allocation) vs direct BitmapIndexWriter.add() calls.
 * <p>
 * This measures the performance benefit of the two-pass histogram approach for index rebuilding,
 * which pre-allocates space and writes sequentially vs dynamic block allocation.
 * <p>
 * Tests low cardinality (512 keys), high cardinality (50,000 keys), and Zipf distribution (realistic skew).
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.SECONDS)
public class BitmapIndexRebuildBenchmark {
    private static final long COLUMN_NAME_TXN_NONE = -1L;
    private static final int HIGH_CARDINALITY = 50_000;   // High cardinality
    private static final int LOW_CARDINALITY = 512;       // Low cardinality
    // 256MiB of index data (8 bytes per rowId)
    private static final int ROW_COUNT = 256 * 1024 * 1024 / 8;
    private static final int ZIPF_CARDINALITY = 15_000;   // Zipf distribution - realistic skewed data
    private CairoConfiguration configuration;
    private int[] highCardinalityKeys;
    private int[] lowCardinalityKeys;
    private Path path;
    private String tempDir;
    private BitmapIndexWriter writerForDirectHighCard;
    private BitmapIndexWriter writerForDirectLowCard;
    private BitmapIndexWriter writerForDirectZipf;
    private BitmapIndexWriter writerForBulkHighCard;
    private BitmapIndexWriter writerForBulkLowCard;
    private BitmapIndexWriter writerForBulkZipf;
    private int[] zipfKeys;

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(BitmapIndexRebuildBenchmark.class.getSimpleName())
                .warmupIterations(1)
                .measurementIterations(2)
                .forks(1)
                .build();

        new Runner(opt).run();
    }

    @Setup(Level.Iteration)
    public void setup() {
        // Create fresh temp directory for each iteration to avoid OS cache effects
        tempDir = System.getProperty("java.io.tmpdir") + File.separator + "qdb_bench_" + System.nanoTime();
        new File(tempDir).mkdirs();

        configuration = new DefaultCairoConfiguration(tempDir);

        path = new Path();
        path.of(tempDir);
        int plen = path.size();

        // Create indexes for direct write benchmarks
        createIndex(configuration, path.trimTo(plen), "idx_direct_low", configuration.getIndexValueBlockSize());
        writerForDirectLowCard = new BitmapIndexWriter(configuration, path.trimTo(plen), "idx_direct_low", COLUMN_NAME_TXN_NONE);

        createIndex(configuration, path.trimTo(plen), "idx_direct_high", configuration.getIndexValueBlockSize());
        writerForDirectHighCard = new BitmapIndexWriter(configuration, path.trimTo(plen), "idx_direct_high", COLUMN_NAME_TXN_NONE);

        createIndex(configuration, path.trimTo(plen), "idx_direct_zipf", configuration.getIndexValueBlockSize());
        writerForDirectZipf = new BitmapIndexWriter(configuration, path.trimTo(plen), "idx_direct_zipf", COLUMN_NAME_TXN_NONE);

        // Create indexes for bulk build benchmarks
        createIndex(configuration, path.trimTo(plen), "idx_bulk_low", configuration.getIndexValueBlockSize());
        writerForBulkLowCard = new BitmapIndexWriter(configuration, path.trimTo(plen), "idx_bulk_low", COLUMN_NAME_TXN_NONE);

        createIndex(configuration, path.trimTo(plen), "idx_bulk_high", configuration.getIndexValueBlockSize());
        writerForBulkHighCard = new BitmapIndexWriter(configuration, path.trimTo(plen), "idx_bulk_high", COLUMN_NAME_TXN_NONE);

        createIndex(configuration, path.trimTo(plen), "idx_bulk_zipf", configuration.getIndexValueBlockSize());
        writerForBulkZipf = new BitmapIndexWriter(configuration, path.trimTo(plen), "idx_bulk_zipf", COLUMN_NAME_TXN_NONE);

        // Pre-generate key distributions for consistent comparison
        Rnd rnd = new Rnd();

        lowCardinalityKeys = new int[ROW_COUNT];
        for (int i = 0; i < ROW_COUNT; i++) {
            lowCardinalityKeys[i] = rnd.nextPositiveInt() % LOW_CARDINALITY;
        }

        highCardinalityKeys = new int[ROW_COUNT];
        for (int i = 0; i < ROW_COUNT; i++) {
            highCardinalityKeys[i] = rnd.nextPositiveInt() % HIGH_CARDINALITY;
        }

        // Zipf distribution: key k has probability proportional to 1/k^s (s=1 here)
        // This creates a realistic skewed distribution where a few keys are very frequent
        // Pre-compute cumulative distribution for efficient sampling
        zipfKeys = new int[ROW_COUNT];
        double[] cumulativeProbs = new double[ZIPF_CARDINALITY];
        double harmonicSum = 0;
        for (int k = 1; k <= ZIPF_CARDINALITY; k++) {
            harmonicSum += 1.0 / k;
        }
        double cumulative = 0;
        for (int k = 1; k <= ZIPF_CARDINALITY; k++) {
            cumulative += (1.0 / k) / harmonicSum;
            cumulativeProbs[k - 1] = cumulative;
        }
        // Sample from the distribution
        for (int i = 0; i < ROW_COUNT; i++) {
            double u = rnd.nextDouble();
            // Binary search for the key
            int lo = 0, hi = ZIPF_CARDINALITY - 1;
            while (lo < hi) {
                int mid = (lo + hi) >>> 1;
                if (cumulativeProbs[mid] < u) {
                    lo = mid + 1;
                } else {
                    hi = mid;
                }
            }
            zipfKeys[i] = lo;
        }
    }

    @TearDown(Level.Iteration)
    public void tearDown() {
        if (writerForDirectLowCard != null) {
            writerForDirectLowCard.close();
            writerForDirectLowCard = null;
        }
        if (writerForDirectHighCard != null) {
            writerForDirectHighCard.close();
            writerForDirectHighCard = null;
        }
        if (writerForDirectZipf != null) {
            writerForDirectZipf.close();
            writerForDirectZipf = null;
        }
        if (writerForBulkLowCard != null) {
            writerForBulkLowCard.close();
            writerForBulkLowCard = null;
        }
        if (writerForBulkHighCard != null) {
            writerForBulkHighCard.close();
            writerForBulkHighCard = null;
        }
        if (writerForBulkZipf != null) {
            writerForBulkZipf.close();
            writerForBulkZipf = null;
        }
        if (path != null) {
            path.close();
            path = null;
        }

        // Clean up temp directory
        if (tempDir != null) {
            deleteDirectory(new File(tempDir));
            tempDir = null;
        }
    }

    /**
     * High cardinality with direct writes.
     * Baseline for high cardinality comparison.
     */
    @Benchmark
    public void testHighCardinalityDirect() {
        for (int i = 0; i < ROW_COUNT; i++) {
            writerForDirectHighCard.add(highCardinalityKeys[i], i);
        }
    }

    /**
     * Low cardinality with direct writes.
     * Baseline for low cardinality comparison.
     */
    @Benchmark
    public void testLowCardinalityDirect() {
        for (int i = 0; i < ROW_COUNT; i++) {
            writerForDirectLowCard.add(lowCardinalityKeys[i], i);
        }
    }

    /**
     * Zipf distribution with direct writes.
     * Baseline for Zipf distribution comparison.
     */
    @Benchmark
    public void testZipfDirect() {
        for (int i = 0; i < ROW_COUNT; i++) {
            writerForDirectZipf.add(zipfKeys[i], i);
        }
    }

    /**
     * High cardinality with bulk build (histogram + pre-allocation).
     */
    @Benchmark
    public void testHighCardinalityBulk() {
        // Pass 1: Build histogram
        IntIntHashMap histogram = new IntIntHashMap();
        int maxKey = -1;
        for (int i = 0; i < ROW_COUNT; i++) {
            int key = highCardinalityKeys[i];
            int idx = histogram.keyIndex(key);
            if (idx >= 0) {
                histogram.putAt(idx, key, 1);
            } else {
                histogram.putAt(idx, key, histogram.valueAt(idx) + 1);
            }
            if (key > maxKey) {
                maxKey = key;
            }
        }

        // Initialize bulk build
        writerForBulkHighCard.initBulkBuild(histogram, maxKey);

        // Pass 2: Write values
        for (int i = 0; i < ROW_COUNT; i++) {
            writerForBulkHighCard.addBulk(highCardinalityKeys[i], i);
        }

        // Finalize
        writerForBulkHighCard.finalizeBulkBuild(ROW_COUNT - 1);
    }

    /**
     * Low cardinality with bulk build (histogram + pre-allocation).
     */
    @Benchmark
    public void testLowCardinalityBulk() {
        // Pass 1: Build histogram
        IntIntHashMap histogram = new IntIntHashMap();
        int maxKey = -1;
        for (int i = 0; i < ROW_COUNT; i++) {
            int key = lowCardinalityKeys[i];
            int idx = histogram.keyIndex(key);
            if (idx >= 0) {
                histogram.putAt(idx, key, 1);
            } else {
                histogram.putAt(idx, key, histogram.valueAt(idx) + 1);
            }
            if (key > maxKey) {
                maxKey = key;
            }
        }

        // Initialize bulk build
        writerForBulkLowCard.initBulkBuild(histogram, maxKey);

        // Pass 2: Write values
        for (int i = 0; i < ROW_COUNT; i++) {
            writerForBulkLowCard.addBulk(lowCardinalityKeys[i], i);
        }

        // Finalize
        writerForBulkLowCard.finalizeBulkBuild(ROW_COUNT - 1);
    }

    /**
     * Zipf distribution with bulk build (histogram + pre-allocation).
     */
    @Benchmark
    public void testZipfBulk() {
        // Pass 1: Build histogram
        IntIntHashMap histogram = new IntIntHashMap();
        int maxKey = -1;
        for (int i = 0; i < ROW_COUNT; i++) {
            int key = zipfKeys[i];
            int idx = histogram.keyIndex(key);
            if (idx >= 0) {
                histogram.putAt(idx, key, 1);
            } else {
                histogram.putAt(idx, key, histogram.valueAt(idx) + 1);
            }
            if (key > maxKey) {
                maxKey = key;
            }
        }

        // Initialize bulk build
        writerForBulkZipf.initBulkBuild(histogram, maxKey);

        // Pass 2: Write values
        for (int i = 0; i < ROW_COUNT; i++) {
            writerForBulkZipf.addBulk(zipfKeys[i], i);
        }

        // Finalize
        writerForBulkZipf.finalizeBulkBuild(ROW_COUNT - 1);
    }

    private static void createIndex(CairoConfiguration configuration, Path path, CharSequence name, int valueBlockCapacity) {
        int plen = path.size();
        try {
            final FilesFacade ff = configuration.getFilesFacade();
            try (
                    MemoryMA mem = Vm.getSmallCMARWInstance(
                            ff,
                            BitmapIndexUtils.keyFileName(path, name, COLUMN_NAME_TXN_NONE),
                            MemoryTag.MMAP_DEFAULT,
                            configuration.getWriterFileOpenOpts()
                    )
            ) {
                BitmapIndexWriter.initKeyMemory(mem, Numbers.ceilPow2(valueBlockCapacity));
            }
            ff.touch(BitmapIndexUtils.valueFileName(path.trimTo(plen), name, COLUMN_NAME_TXN_NONE));
        } finally {
            path.trimTo(plen);
        }
    }

    private static void deleteDirectory(File dir) {
        if (dir.isDirectory()) {
            File[] files = dir.listFiles();
            if (files != null) {
                for (File file : files) {
                    deleteDirectory(file);
                }
            }
        }
        dir.delete();
    }
}
