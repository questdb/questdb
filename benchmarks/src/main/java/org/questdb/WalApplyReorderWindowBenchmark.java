/*+*****************************************************************************
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

import com.sun.management.OperatingSystemMXBean;
import com.sun.management.ThreadMXBean;
import io.questdb.Metrics;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.DefaultCairoConfiguration;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.TableWriterMetrics;
import io.questdb.cairo.wal.ApplyWal2TableJob;
import io.questdb.cairo.wal.WalWriter;
import io.questdb.cairo.wal.seq.SeqTxnTracker;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.log.LogFactory;
import io.questdb.metrics.MetricsRegistryImpl;
import io.questdb.std.datetime.microtime.Micros;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Arrays;
import java.util.Locale;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

/**
 * Measures the storage-work and latency trade-off of the WAL apply reorder window.
 *
 * <p>Each iteration prepares two committed WAL transactions per table. The first
 * transaction contains a newer timestamp range. The second contains an older range
 * in the same otherwise-empty daily partition. Disabled runs drain WAL between the
 * commits and therefore rewrite the first batch. The enabled run consumes one apply
 * notification between commits, waits for the real timer, and applies both transactions
 * as one block. The table's {@code maxUncommittedRows} is sized from {@code rowsPerTxn}
 * so the pair fits the existing WAL block limit.
 *
 * <p>The benchmark runs disabled-A, enabled, disabled-B so disabled throughput, CPU,
 * and allocation variance are visible in one result. Several tables have live backlogs
 * at once; per-table visibility latency spread is reported as a fairness signal. All
 * scenario directories are children created by this process and are deleted on exit.
 *
 * <p>Arguments: {@code root rowsPerTxn iterations warmupIterations windowMicros tableCount}.
 * The default root is {@code /mnt/pcie5}; large runs must stay off tmpfs.
 *
 * <p>Run the shaded benchmark jar with the same VM access used by core tests:
 * {@code java -ea --sun-misc-unsafe-memory-access=allow --enable-native-access=ALL-UNNAMED
 * --add-opens=java.base/java.lang=ALL-UNNAMED
 * --add-exports=java.base/jdk.internal.vm=ALL-UNNAMED
 * -cp benchmarks/target/benchmarks.jar org.questdb.WalApplyReorderWindowBenchmark}.
 */
public class WalApplyReorderWindowBenchmark {
    private static final long BASE_TIMESTAMP_MICROS = 1_704_067_200_000_000L;
    private static final int DEFAULT_ITERATIONS = 5;
    private static final Path DEFAULT_ROOT = Path.of("/mnt/pcie5");
    private static final int DEFAULT_ROWS_PER_TXN = 50_000;
    private static final int DEFAULT_TABLE_COUNT = 4;
    private static final long DEFAULT_WINDOW_MICROS = 50_000;
    private static final int DEFAULT_WARMUP_ITERATIONS = 2;
    private static final OperatingSystemMXBean OS_MX_BEAN =
            (OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();
    private static final ThreadMXBean THREAD_MX_BEAN =
            (ThreadMXBean) ManagementFactory.getThreadMXBean();

    public static void main(String[] args) throws Exception {
        Locale.setDefault(Locale.ROOT);
        final Path parentRoot = args.length > 0 ? Path.of(args[0]).toAbsolutePath() : DEFAULT_ROOT;
        final int rowsPerTxn = parsePositiveInt(args, 1, DEFAULT_ROWS_PER_TXN, "rowsPerTxn");
        final int iterations = parsePositiveInt(args, 2, DEFAULT_ITERATIONS, "iterations");
        final int warmupIterations = parseNonNegativeInt(args, 3, DEFAULT_WARMUP_ITERATIONS, "warmupIterations");
        final long windowMicros = parsePositiveLong(args, 4, DEFAULT_WINDOW_MICROS, "windowMicros");
        final int tableCount = parsePositiveInt(args, 5, DEFAULT_TABLE_COUNT, "tableCount");

        if (!Files.isDirectory(parentRoot)) {
            throw new IllegalArgumentException("benchmark root is not a directory: " + parentRoot);
        }
        if (!Files.isWritable(parentRoot)) {
            throw new IllegalArgumentException("benchmark root is not writable: " + parentRoot);
        }
        enableAllocationTracking();

        System.out.printf(
                "WAL apply reorder benchmark [root=%s, tables=%d, rowsPerTxn=%d, iterations=%d, warmup=%d, window=%dus]%n",
                parentRoot,
                tableCount,
                rowsPerTxn,
                iterations,
                warmupIterations,
                windowMicros
        );
        System.out.println(
                "scenario      window_us logical_rows physical_rows write_amp o3_commits apply_commits " +
                        "throughput_rows_s apply_rows_s process_cpu_ms allocated_mb alloc_b_row apply_ms " +
                        "visibility_p50_ms visibility_p99_ms fairness_max_spread_ms"
        );

        try {
            final ScenarioResult disabledA = runScenario(
                    parentRoot,
                    "disabled-a",
                    0,
                    rowsPerTxn,
                    iterations,
                    warmupIterations,
                    tableCount
            );
            print(disabledA);
            final ScenarioResult enabled = runScenario(
                    parentRoot,
                    "enabled",
                    windowMicros,
                    rowsPerTxn,
                    iterations,
                    warmupIterations,
                    tableCount
            );
            print(enabled);
            final ScenarioResult disabledB = runScenario(
                    parentRoot,
                    "disabled-b",
                    0,
                    rowsPerTxn,
                    iterations,
                    warmupIterations,
                    tableCount
            );
            print(disabledB);
            printComparison(disabledA, enabled, disabledB);
        } finally {
            LogFactory.haltInstance();
        }
    }

    private static void appendRange(WalWriter writer, long timestampMicros, int rowCount, long valueBase) {
        for (int i = 0; i < rowCount; i++) {
            final TableWriter.Row row = writer.newRow(timestampMicros + i);
            row.putLong(1, valueBase + i);
            row.append();
        }
    }

    private static void closeWriters(WalWriter[] writers) {
        for (int i = 0; i < writers.length; i++) {
            if (writers[i] != null) {
                writers[i].close();
            }
        }
    }

    private static long currentJvmAllocation() {
        if (!THREAD_MX_BEAN.isThreadAllocatedMemorySupported()
                || !THREAD_MX_BEAN.isThreadAllocatedMemoryEnabled()) {
            return -1;
        }
        final long[] allocatedBytes = THREAD_MX_BEAN.getThreadAllocatedBytes(THREAD_MX_BEAN.getAllThreadIds());
        long total = 0;
        for (int i = 0; i < allocatedBytes.length; i++) {
            if (allocatedBytes[i] > 0) {
                total += allocatedBytes[i];
            }
        }
        return total;
    }

    private static void deleteRecursively(Path dir) throws IOException {
        if (!Files.exists(dir)) {
            return;
        }
        Files.walkFileTree(dir, new SimpleFileVisitor<>() {
            @Override
            public FileVisitResult postVisitDirectory(Path current, IOException exc) throws IOException {
                Files.delete(current);
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                Files.delete(file);
                return FileVisitResult.CONTINUE;
            }
        });
    }

    private static long drainUntilCaughtUp(
            ApplyWal2TableJob applyJob,
            SeqTxnTracker[] trackers,
            long[] firstCommitNanos,
            long[] visibilityNanos,
            int visibilityOffset,
            long timeoutNanos
    ) {
        final boolean[] caughtUp = new boolean[trackers.length];
        final long deadline = System.nanoTime() + timeoutNanos;
        int remaining = trackers.length;
        long applyNanos = 0;
        while (remaining > 0) {
            final long applyStart = System.nanoTime();
            final boolean ran = applyJob.run();
            applyNanos += System.nanoTime() - applyStart;

            final long now = System.nanoTime();
            for (int i = 0; i < trackers.length; i++) {
                if (!caughtUp[i] && trackers[i].getWriterTxn() >= trackers[i].getSeqTxn()) {
                    caughtUp[i] = true;
                    visibilityNanos[visibilityOffset + i] = now - firstCommitNanos[i];
                    remaining--;
                }
            }
            if (remaining == 0) {
                return applyNanos;
            }
            if (now >= deadline) {
                throw new IllegalStateException("timed out waiting for WAL apply [remaining=" + remaining + ']');
            }
            if (!ran) {
                LockSupport.parkNanos(100_000);
            }
        }
        return applyNanos;
    }

    private static void enableAllocationTracking() {
        if (THREAD_MX_BEAN.isThreadAllocatedMemorySupported()
                && !THREAD_MX_BEAN.isThreadAllocatedMemoryEnabled()) {
            THREAD_MX_BEAN.setThreadAllocatedMemoryEnabled(true);
        }
    }

    private static int parseNonNegativeInt(String[] args, int index, int defaultValue, String name) {
        if (args.length <= index) {
            return defaultValue;
        }
        final int value = Integer.parseInt(args[index]);
        if (value < 0) {
            throw new IllegalArgumentException(name + " must be non-negative");
        }
        return value;
    }

    private static int parsePositiveInt(String[] args, int index, int defaultValue, String name) {
        final int value = parseNonNegativeInt(args, index, defaultValue, name);
        if (value == 0) {
            throw new IllegalArgumentException(name + " must be positive");
        }
        return value;
    }

    private static long parsePositiveLong(String[] args, int index, long defaultValue, String name) {
        if (args.length <= index) {
            return defaultValue;
        }
        final long value = Long.parseLong(args[index]);
        if (value <= 0) {
            throw new IllegalArgumentException(name + " must be positive");
        }
        return value;
    }

    private static double percentChange(double current, double baseline) {
        return baseline == 0 ? 0 : 100.0 * (current - baseline) / baseline;
    }

    private static void print(ScenarioResult result) {
        System.out.printf(
                "%-13s %9d %12d %13d %9.3f %10d %13d %17.0f %12.0f %14.3f %12.3f %11.3f " +
                        "%8.3f %17.3f %17.3f %22.3f%n",
                result.name,
                result.windowMicros,
                result.logicalRows,
                result.physicalRows,
                result.physicalRows / (double) result.logicalRows,
                result.o3Commits,
                result.applyCommits,
                result.logicalRows * 1_000_000_000.0 / result.wallNanos,
                result.logicalRows * 1_000_000_000.0 / result.applyNanos,
                result.processCpuNanos / 1_000_000.0,
                result.allocatedBytes / (1024.0 * 1024.0),
                result.allocatedBytes / (double) result.logicalRows,
                result.applyNanos / 1_000_000.0,
                result.visibilityP50Nanos / 1_000_000.0,
                result.visibilityP99Nanos / 1_000_000.0,
                result.maxFairnessSpreadNanos / 1_000_000.0
        );
    }

    private static void printComparison(
            ScenarioResult disabledA,
            ScenarioResult enabled,
            ScenarioResult disabledB
    ) {
        final double baselinePhysicalRows = (disabledA.physicalRows + disabledB.physicalRows) / 2.0;
        final double baselineThroughput = (
                disabledA.logicalRows * 1_000_000_000.0 / disabledA.wallNanos
                        + disabledB.logicalRows * 1_000_000_000.0 / disabledB.wallNanos
        ) / 2.0;
        final double baselineCpuPerRow = (
                disabledA.processCpuNanos / (double) disabledA.logicalRows
                        + disabledB.processCpuNanos / (double) disabledB.logicalRows
        ) / 2.0;
        final double baselineAllocationPerRow = (
                disabledA.allocatedBytes / (double) disabledA.logicalRows
                        + disabledB.allocatedBytes / (double) disabledB.logicalRows
        ) / 2.0;
        final double disabledThroughputDrift = percentChange(
                disabledB.logicalRows * 1_000_000_000.0 / disabledB.wallNanos,
                disabledA.logicalRows * 1_000_000_000.0 / disabledA.wallNanos
        );
        final double disabledCpuDrift = percentChange(
                disabledB.processCpuNanos / (double) disabledB.logicalRows,
                disabledA.processCpuNanos / (double) disabledA.logicalRows
        );
        final double disabledAllocationDrift = percentChange(
                disabledB.allocatedBytes / (double) disabledB.logicalRows,
                disabledA.allocatedBytes / (double) disabledA.logicalRows
        );
        if (enabled.physicalRows >= Math.min(disabledA.physicalRows, disabledB.physicalRows)) {
            throw new IllegalStateException(
                    "enabled run did not reduce physical writes [enabled=" + enabled.physicalRows +
                            ", disabledA=" + disabledA.physicalRows +
                            ", disabledB=" + disabledB.physicalRows + ']'
            );
        }

        System.out.printf(
                "%nphysical-row reduction vs disabled mean: %.2f%%%n",
                100.0 * (baselinePhysicalRows - enabled.physicalRows) / baselinePhysicalRows
        );
        System.out.printf(
                "enabled vs disabled mean: throughput %+.2f%%, CPU/row %+.2f%%, allocation/row %+.2f%%%n",
                percentChange(
                        enabled.logicalRows * 1_000_000_000.0 / enabled.wallNanos,
                        baselineThroughput
                ),
                percentChange(enabled.processCpuNanos / (double) enabled.logicalRows, baselineCpuPerRow),
                percentChange(enabled.allocatedBytes / (double) enabled.logicalRows, baselineAllocationPerRow)
        );
        System.out.printf(
                "disabled B vs A drift: throughput %+.2f%%, CPU/row %+.2f%%, allocation/row %+.2f%%%n",
                disabledThroughputDrift,
                disabledCpuDrift,
                disabledAllocationDrift
        );
    }

    private static ScenarioResult runScenario(
            Path parentRoot,
            String name,
            long windowMicros,
            int rowsPerTxn,
            int iterations,
            int warmupIterations,
            int tableCount
    ) throws Exception {
        final Path root = Files.createTempDirectory(parentRoot, "qdb-wal-reorder-" + name + '-');
        try {
            final ScenarioConfiguration configuration = new ScenarioConfiguration(root.toString(), windowMicros);
            try (CairoEngine engine = new CairoEngine(configuration)) {
                final SqlExecutionContext sqlExecutionContext = new SqlExecutionContextImpl(engine, 1).with(
                        configuration.getFactoryProvider().getSecurityContextFactory().getRootContext(),
                        null,
                        null,
                        -1,
                        null
                );
                final TableToken[] tableTokens = new TableToken[tableCount];
                final SeqTxnTracker[] trackers = new SeqTxnTracker[tableCount];
                final WalWriter[] newerWriters = new WalWriter[tableCount];
                final WalWriter[] olderWriters = new WalWriter[tableCount];
                final int maxUncommittedRows = Math.multiplyExact(rowsPerTxn, 2);
                try (ApplyWal2TableJob applyJob = new ApplyWal2TableJob(engine, 0)) {
                    for (int i = 0; i < tableCount; i++) {
                        final String tableName = "x" + i;
                        engine.execute(
                                "create table " + tableName +
                                        " (ts timestamp, value long) timestamp(ts) partition by day wal " +
                                        "with maxUncommittedRows=" + maxUncommittedRows,
                                sqlExecutionContext
                        );
                        tableTokens[i] = engine.verifyTableName(tableName);
                        trackers[i] = engine.getTableSequencerAPI().getTxnTracker(tableTokens[i]);
                        newerWriters[i] = engine.getWalWriter(tableTokens[i]);
                        olderWriters[i] = engine.getWalWriter(tableTokens[i]);
                    }

                    for (int i = 0; i < warmupIterations; i++) {
                        runIteration(
                                engine,
                                applyJob,
                                trackers,
                                newerWriters,
                                olderWriters,
                                rowsPerTxn,
                                i,
                                windowMicros,
                                null,
                                0
                        );
                    }

                    final TableWriterMetrics writerMetrics = engine.getMetrics().tableWriterMetrics();
                    final long physicalRowsBefore = writerMetrics.getPhysicallyWrittenRows();
                    final long o3CommitsBefore = writerMetrics.getO3CommitCount();
                    final long applyCommitsBefore = writerMetrics.getCommitCount();
                    final long processCpuBefore = OS_MX_BEAN.getProcessCpuTime();
                    final long allocatedBefore = currentJvmAllocation();
                    final long wallBefore = System.nanoTime();
                    final long[] visibilityNanos = new long[iterations * tableCount];
                    long applyNanos = 0;
                    long maxFairnessSpreadNanos = 0;

                    for (int i = 0; i < iterations; i++) {
                        final int visibilityOffset = i * tableCount;
                        applyNanos += runIteration(
                                engine,
                                applyJob,
                                trackers,
                                newerWriters,
                                olderWriters,
                                rowsPerTxn,
                                warmupIterations + i,
                                windowMicros,
                                visibilityNanos,
                                visibilityOffset
                        );
                        long min = Long.MAX_VALUE;
                        long max = Long.MIN_VALUE;
                        for (int table = 0; table < tableCount; table++) {
                            final long latency = visibilityNanos[visibilityOffset + table];
                            min = Math.min(min, latency);
                            max = Math.max(max, latency);
                        }
                        maxFairnessSpreadNanos = Math.max(maxFairnessSpreadNanos, max - min);
                    }

                    final long wallNanos = System.nanoTime() - wallBefore;
                    final long allocatedAfter = currentJvmAllocation();
                    final long processCpuNanos = OS_MX_BEAN.getProcessCpuTime() - processCpuBefore;
                    final long logicalRows = (long) iterations * tableCount * rowsPerTxn * 2L;
                    final long expectedTotalRows =
                            (long) (iterations + warmupIterations) * tableCount * rowsPerTxn * 2L;
                    long totalRows = 0;
                    for (int i = 0; i < tableCount; i++) {
                        try (TableReader reader = engine.getReader(tableTokens[i])) {
                            totalRows += reader.size();
                        }
                    }
                    if (totalRows != expectedTotalRows) {
                        throw new IllegalStateException(
                                "unexpected final row count [expected=" + expectedTotalRows + ", actual=" + totalRows + ']'
                        );
                    }
                    if (engine.getTimerShards().size() != 0) {
                        throw new IllegalStateException(
                                "timer entries remain after scenario [count=" + engine.getTimerShards().size() + ']'
                        );
                    }

                    Arrays.sort(visibilityNanos);
                    return new ScenarioResult(
                            name,
                            windowMicros,
                            logicalRows,
                            writerMetrics.getPhysicallyWrittenRows() - physicalRowsBefore,
                            writerMetrics.getO3CommitCount() - o3CommitsBefore,
                            writerMetrics.getCommitCount() - applyCommitsBefore,
                            wallNanos,
                            processCpuNanos,
                            allocatedBefore < 0 || allocatedAfter < 0 ? -1 : allocatedAfter - allocatedBefore,
                            applyNanos,
                            percentile(visibilityNanos, 50),
                            percentile(visibilityNanos, 99),
                            maxFairnessSpreadNanos
                    );
                } finally {
                    closeWriters(newerWriters);
                    closeWriters(olderWriters);
                }
            }
        } finally {
            deleteRecursively(root);
        }
    }

    private static long runIteration(
            CairoEngine engine,
            ApplyWal2TableJob applyJob,
            SeqTxnTracker[] trackers,
            WalWriter[] newerWriters,
            WalWriter[] olderWriters,
            int rowsPerTxn,
            int iteration,
            long windowMicros,
            long[] visibilityNanos,
            int visibilityOffset
    ) {
        final int tableCount = trackers.length;
        final long[] firstCommitNanos = new long[tableCount];
        final long dayStart = BASE_TIMESTAMP_MICROS + iteration * Micros.DAY_MICROS;
        final long newerTimestamp = dayStart + 12 * Micros.HOUR_MICROS;
        final long valueStride = 2L * rowsPerTxn;

        for (int i = 0; i < tableCount; i++) {
            final long valueBase = ((long) iteration * tableCount + i) * valueStride;
            appendRange(newerWriters[i], newerTimestamp, rowsPerTxn, valueBase + rowsPerTxn);
            appendRange(olderWriters[i], dayStart, rowsPerTxn, valueBase);
        }

        long applyNanos = 0;
        for (int i = 0; i < tableCount; i++) {
            firstCommitNanos[i] = System.nanoTime();
            newerWriters[i].commit();
            if (windowMicros > 0) {
                final long applyStart = System.nanoTime();
                applyJob.run();
                applyNanos += System.nanoTime() - applyStart;
                if (trackers[i].getReorderState() != SeqTxnTracker.REORDER_DEFERRED) {
                    throw new IllegalStateException(
                            "window expired before the second commit; increase windowMicros [table=" + i + ']'
                    );
                }
            }
        }

        if (windowMicros == 0) {
            final long applyStart = System.nanoTime();
            applyJob.drain(0);
            applyNanos += System.nanoTime() - applyStart;
        }

        for (int i = 0; i < tableCount; i++) {
            olderWriters[i].commit();
        }

        final long[] iterationVisibility = visibilityNanos != null
                ? visibilityNanos
                : new long[tableCount];
        final int iterationVisibilityOffset = visibilityNanos != null ? visibilityOffset : 0;
        final long timeoutNanos = Math.max(
                TimeUnit.SECONDS.toNanos(30),
                TimeUnit.MICROSECONDS.toNanos(windowMicros) * 20
        );
        applyNanos += drainUntilCaughtUp(
                applyJob,
                trackers,
                firstCommitNanos,
                iterationVisibility,
                iterationVisibilityOffset,
                timeoutNanos
        );
        return applyNanos;
    }

    private static long percentile(long[] sorted, int percentile) {
        if (sorted.length == 0) {
            return 0;
        }
        final int index = Math.min(
                sorted.length - 1,
                Math.max(0, (int) Math.ceil(percentile * sorted.length / 100.0) - 1)
        );
        return sorted[index];
    }

    private static final class ScenarioConfiguration extends DefaultCairoConfiguration {
        private final Metrics metrics = new Metrics(true, new MetricsRegistryImpl());
        private final long reorderWindowMicros;

        private ScenarioConfiguration(CharSequence root, long reorderWindowMicros) {
            super(root);
            this.reorderWindowMicros = reorderWindowMicros;
        }

        @Override
        public Metrics getMetrics() {
            return metrics;
        }

        @Override
        public long getWalApplyReorderWindow() {
            return reorderWindowMicros;
        }
    }

    private static final class ScenarioResult {
        private final long allocatedBytes;
        private final long applyCommits;
        private final long applyNanos;
        private final long logicalRows;
        private final long maxFairnessSpreadNanos;
        private final String name;
        private final long o3Commits;
        private final long physicalRows;
        private final long processCpuNanos;
        private final long visibilityP50Nanos;
        private final long visibilityP99Nanos;
        private final long wallNanos;
        private final long windowMicros;

        private ScenarioResult(
                String name,
                long windowMicros,
                long logicalRows,
                long physicalRows,
                long o3Commits,
                long applyCommits,
                long wallNanos,
                long processCpuNanos,
                long allocatedBytes,
                long applyNanos,
                long visibilityP50Nanos,
                long visibilityP99Nanos,
                long maxFairnessSpreadNanos
        ) {
            this.name = name;
            this.windowMicros = windowMicros;
            this.logicalRows = logicalRows;
            this.physicalRows = physicalRows;
            this.o3Commits = o3Commits;
            this.applyCommits = applyCommits;
            this.wallNanos = wallNanos;
            this.processCpuNanos = processCpuNanos;
            this.allocatedBytes = allocatedBytes;
            this.applyNanos = applyNanos;
            this.visibilityP50Nanos = visibilityP50Nanos;
            this.visibilityP99Nanos = visibilityP99Nanos;
            this.maxFairnessSpreadNanos = maxFairnessSpreadNanos;
        }
    }
}
