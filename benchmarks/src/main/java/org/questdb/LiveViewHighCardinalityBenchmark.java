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

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.DefaultCairoConfiguration;
import io.questdb.cairo.lv.LiveViewInstance;
import io.questdb.cairo.lv.LiveViewRefreshJob;
import io.questdb.cairo.wal.ApplyWal2TableJob;
import io.questdb.cairo.wal.CheckWalTransactionsJob;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.datetime.microtime.Micros;

import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/**
 * End-to-end reproduction of the ordered-ingestion live-view workload.
 *
 * <p>The base timestamps are strictly increasing, every row has a distinct account,
 * and the view maintains the customer's daily cumulative sum and count. Each
 * cardinality runs in a fresh database so the reported refresh rate is associated
 * with a known state size. The base symbol can be pre-sized to remove metadata
 * changes from the steady-state measurement.</p>
 *
 * <p>This is deliberately a standalone benchmark instead of a JMH steady-state
 * benchmark: growing cardinality is the independent variable, checkpoint seals are
 * sparse events, and averaging the two together would hide the stalls we need to
 * expose.</p>
 *
 * <p>Both JVM flags are required. Without the export, every worker thread fails to
 * initialize {@code WorkerContinuation} and the run reports numbers taken with the
 * continuation path dead.</p>
 *
 * <pre>
 * mvn -pl benchmarks -am package -o -DskipTests
 * java --enable-native-access=ALL-UNNAMED \
 *      --add-exports=java.base/jdk.internal.vm=ALL-UNNAMED \
 *      -cp benchmarks/target/benchmarks.jar \
 *      org.questdb.LiveViewHighCardinalityBenchmark \
 *      --cardinalities=10000,100000,1000000 --batch=100000
 * </pre>
 */
public class LiveViewHighCardinalityBenchmark {

    private static final int DEFAULT_BATCH_ROWS = 100_000;
    private static final long DEFAULT_CHECKPOINT_ROWS = 1_000_000L;
    private static final long START_TS = 1_785_496_035_000_000L; // 2026-07-31T17:27:15Z
    // Logical source rate: approximately 2,250 rows/second.
    private static final long TS_STEP_MICROS = 444L;

    public static void main(String[] args) throws Exception {
        final Options options = Options.parse(args);
        System.out.printf(
                Locale.ROOT,
                "# batch=%d checkpointRows=%d checkpointDurationUs=%d preSizeSymbol=%s%n",
                options.batchRows,
                options.checkpointRows,
                options.checkpointDurationMicros,
                options.preSizeSymbol
        );
        System.out.println(
                "state_rows\tseed_ms\tbatch_rows\tbase_apply_ms\trefresh_ms\trows_per_sec"
                        + "\tcheckpoint\tcheckpoint_ms\tstate_bytes\tlag_seqtxn\trefresh_faults"
        );
        for (long cardinality : options.cardinalities) {
            runScenario(cardinality, options);
        }
    }

    private static void deleteRecursively(Path dir) throws IOException {
        if (dir == null || !Files.exists(dir)) {
            return;
        }
        Files.walkFileTree(dir, new SimpleFileVisitor<>() {
            @Override
            public FileVisitResult postVisitDirectory(Path d, IOException exc) throws IOException {
                Files.delete(d);
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                Files.delete(file);
                return FileVisitResult.CONTINUE;
            }
        });
    }

    private static void drainLiveView(CairoEngine engine, LiveViewInstance instance, LiveViewRefreshJob job) {
        // Force this benchmark's explicit batches across FLUSH EVERY. Production
        // cadence is a wall-clock batching policy; waiting five seconds here would
        // add idle time rather than measure refresh work.
        instance.setLastFlushTimeUs(Numbers.LONG_NULL);
        for (int i = 0; i < 1_024; i++) {
            boolean progressed = false;
            while (job.run()) {
                progressed = true;
            }
            drainWal(engine);
            if (!progressed) {
                return;
            }
            instance.setLastFlushTimeUs(Numbers.LONG_NULL);
        }
        throw new IllegalStateException("live view did not quiesce");
    }

    private static void drainWal(CairoEngine engine) {
        try (ApplyWal2TableJob walApplyJob = new ApplyWal2TableJob(engine, 0)) {
            while (walApplyJob.run()) {
                // drain
            }
            if (new CheckWalTransactionsJob(engine).run()) {
                while (walApplyJob.run()) {
                    // drain
                }
            }
        }
    }

    private static String insertSql(long firstRow, long rows) {
        return "insert into mm_transaction_live_created_at "
                + "select (" + START_TS + " + (x + " + (firstRow - 1) + ") * " + TS_STEP_MICROS + ")::timestamp, "
                + "'acct-' || (x + " + (firstRow - 1) + ")::string, "
                + "((x + " + (firstRow - 1) + ") % 2001 - 1000) * 0.01 "
                + "from long_sequence(" + rows + ")";
    }

    private static int symbolCapacity(long rows) {
        long capacity = 16;
        while (capacity < rows && capacity < (1L << 30)) {
            capacity <<= 1;
        }
        return (int) Math.min(capacity, 1L << 30);
    }

    private static void runScenario(long cardinality, Options options) throws Exception {
        final Path dbRoot = Files.createTempDirectory("lv-high-cardinality-");
        CairoEngine engine = null;
        try {
            final CairoConfiguration configuration = new DefaultCairoConfiguration(dbRoot.toString()) {
                @Override
                public long getLiveViewCheckpointMaxDurationMicros() {
                    return options.checkpointDurationMicros;
                }

                @Override
                public long getLiveViewCheckpointRows() {
                    return options.checkpointRows;
                }

                @Override
                public boolean isDevModeEnabled() {
                    return true;
                }
            };
            engine = new CairoEngine(configuration);
            engine.load();
            final SqlExecutionContext sqlCtx = new SqlExecutionContextImpl(engine, 1).with(
                    configuration.getFactoryProvider().getSecurityContextFactory().getRootContext(),
                    null, null, -1, null
            );

            final String capacity = options.preSizeSymbol
                    ? " capacity " + symbolCapacity(cardinality + options.batchRows)
                    : "";
            engine.execute(
                    "create table mm_transaction_live_created_at ("
                            + "created_at timestamp, "
                            + "cod_acct_no symbol" + capacity + " nocache index capacity 4, "
                            + "amt_txn double"
                            + ") timestamp(created_at) partition by hour wal",
                    sqlCtx
            );
            engine.execute(insertSql(1, cardinality), sqlCtx);
            drainWal(engine);

            engine.execute(
                    "create live view mm_transaction_live_created_at_view "
                            + "flush every 5s start from beginning as "
                            + "select created_at, cod_acct_no, "
                            + "sum(amt_txn) over w as cumulative_sum, "
                            + "count(cod_acct_no) over w as cumulative_count "
                            + "from mm_transaction_live_created_at "
                            + "window w as (partition by cod_acct_no order by created_at anchor daily '12:00')",
                    sqlCtx
            );
            final LiveViewInstance instance = engine.getLiveViewRegistry()
                    .getViewInstance("mm_transaction_live_created_at_view");

            final long seedStart = System.nanoTime();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                drainLiveView(engine, instance, job);
                final long seedNanos = System.nanoTime() - seedStart;
                if (instance.getLvRowsTotal() != cardinality) {
                    throw new IllegalStateException(
                            "seed row mismatch: expected " + cardinality + ", got " + instance.getLvRowsTotal()
                    );
                }

                final long checkpointBefore = instance.getHeadCheckpointRootId();
                engine.execute(insertSql(cardinality + 1, options.batchRows), sqlCtx);
                final long baseStart = System.nanoTime();
                drainWal(engine);
                final long baseNanos = System.nanoTime() - baseStart;

                final long refreshStart = System.nanoTime();
                drainLiveView(engine, instance, job);
                final long refreshNanos = System.nanoTime() - refreshStart;

                final long expectedRows = cardinality + options.batchRows;
                if (instance.getLvRowsTotal() != expectedRows) {
                    throw new IllegalStateException(
                            "incremental row mismatch: expected " + expectedRows + ", got " + instance.getLvRowsTotal()
                    );
                }
                final long checkpointAfter = instance.getHeadCheckpointRootId();
                final boolean checkpointWritten = checkpointAfter != checkpointBefore;
                final double refreshSeconds = refreshNanos / 1_000_000_000.0;
                final double rowsPerSecond = options.batchRows / refreshSeconds;
                final long baseSeqTxn = engine.getTableSequencerAPI()
                        .getTxnTracker(engine.getTableTokenIfExists("mm_transaction_live_created_at"))
                        .getWriterTxn();
                final long lagSeqTxn = baseSeqTxn - instance.getLastProcessedSeqTxn();

                System.out.printf(
                        Locale.ROOT,
                        "%d\t%.3f\t%d\t%.3f\t%.3f\t%.0f\t%s\t%.3f\t%d\t%d\t%d%n",
                        cardinality,
                        seedNanos / 1_000_000.0,
                        options.batchRows,
                        baseNanos / 1_000_000.0,
                        refreshNanos / 1_000_000.0,
                        rowsPerSecond,
                        checkpointWritten,
                        instance.getHeadCheckpointWriteMicros() / 1_000.0,
                        instance.getHeadCheckpointStateBytes(),
                        lagSeqTxn,
                        instance.getRefreshFaultCount()
                );
            }
        } finally {
            engine = Misc.free(engine);
            deleteRecursively(dbRoot);
        }
    }

    private static final class Options {
        private final int batchRows;
        private final long[] cardinalities;
        private final long checkpointDurationMicros;
        private final long checkpointRows;
        private final boolean preSizeSymbol;

        private Options(
                long[] cardinalities,
                int batchRows,
                long checkpointRows,
                long checkpointDurationMicros,
                boolean preSizeSymbol
        ) {
            this.cardinalities = cardinalities;
            this.batchRows = batchRows;
            this.checkpointRows = checkpointRows;
            this.checkpointDurationMicros = checkpointDurationMicros;
            this.preSizeSymbol = preSizeSymbol;
        }

        private static Options parse(String[] args) {
            long[] cardinalities = {10_000L, 100_000L, 1_000_000L};
            int batchRows = DEFAULT_BATCH_ROWS;
            long checkpointRows = DEFAULT_CHECKPOINT_ROWS;
            long checkpointDurationMicros = 24L * Micros.HOUR_MICROS;
            boolean preSizeSymbol = true;
            for (String arg : args) {
                if (arg.startsWith("--cardinalities=")) {
                    final String[] values = arg.substring("--cardinalities=".length()).split(",");
                    final List<Long> parsed = new ArrayList<>(values.length);
                    for (String value : values) {
                        parsed.add(Long.parseLong(value.trim()));
                    }
                    cardinalities = new long[parsed.size()];
                    for (int i = 0; i < cardinalities.length; i++) {
                        cardinalities[i] = parsed.get(i);
                    }
                } else if (arg.startsWith("--batch=")) {
                    batchRows = Integer.parseInt(arg.substring("--batch=".length()));
                } else if (arg.startsWith("--checkpoint-rows=")) {
                    checkpointRows = Long.parseLong(arg.substring("--checkpoint-rows=".length()));
                } else if (arg.startsWith("--checkpoint-duration-us=")) {
                    checkpointDurationMicros = Long.parseLong(arg.substring("--checkpoint-duration-us=".length()));
                } else if (arg.startsWith("--presize-symbol=")) {
                    preSizeSymbol = Boolean.parseBoolean(arg.substring("--presize-symbol=".length()));
                } else {
                    throw new IllegalArgumentException("unknown argument: " + arg);
                }
            }
            return new Options(
                    cardinalities,
                    batchRows,
                    checkpointRows,
                    checkpointDurationMicros,
                    preSizeSymbol
            );
        }
    }
}
