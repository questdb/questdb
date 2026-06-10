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
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.DefaultCairoConfiguration;
import io.questdb.cairo.ParquetTimestampFinder;
import io.questdb.cairo.TableReader;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.griffin.engine.table.parquet.ParquetPartitionDecoder;
import io.questdb.log.LogFactory;
import io.questdb.std.Misc;
import io.questdb.std.Rnd;
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
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.concurrent.TimeUnit;

/**
 * Measures {@link ParquetTimestampFinder#timestampAt(long)} (single-value timestamp
 * decode from a Parquet partition) for PLAIN vs DELTA_BINARY_PACKED, sweeping the
 * decoded row's offset within its row group.
 * <p>
 * timestampAt decodes a one-row range {@code decodeRowGroup(.., rowLo, rowLo + 1)}.
 * Plain's per-value skip is O(1) but the whole (compressed) page is decompressed
 * first, so plain is flat across offsets; delta has no random access and replays the
 * prefix sum up to rowLo, so it grows with offset. {@code IntervalBwdPartitionFrameCursor}
 * calls {@code timestampAt(rowCount - 1)} (PARTITION_END, the worst case) per partition.
 * <p>
 * Rebuild the native lib first (the committed one may write plain), then:
 * <pre>
 *   mvn -pl core install -DskipTests &amp;&amp; mvn -pl benchmarks package -DskipTests
 *   java -jar benchmarks/target/benchmarks.jar ParquetTimestampPointDecodeBenchmark
 * </pre>
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 3, time = 2)
@Measurement(iterations = 5, time = 2)
@Fork(1)
public class ParquetTimestampPointDecodeBenchmark {

    // ~1.8M rows fall in partition 0 (the rest spill to a 2nd partition). CONVERT
    // leaves the active partition native, so we target partition 0.
    private static final int ROW_COUNT = 2_000_000;
    private static final long TS_BASE = 1_704_067_200_000_000L;
    private static final long TS_STEP_MICROS = 2_000;
    private static final int TARGET_PARTITION = 0;

    @Param({"PLAIN", "DELTA"})
    public String tsEncoding;

    @Param({"GROUP_START", "GROUP_MID", "GROUP_END", "PARTITION_END"})
    public String target;

    // REGULAR: constant cadence -> zero-bit residuals, delta decode near-free (unrealistic).
    // JITTER: uniform per-step offset -> real bit-packed residuals (the realistic case).
    @Param({"REGULAR", "JITTER"})
    public String tsPattern;

    private ParquetPartitionDecoder parquetDecoder;
    private ParquetTimestampFinder finder;
    private CairoEngine engine;
    private TableReader reader;
    private Path root;
    private long targetRow;

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(ParquetTimestampPointDecodeBenchmark.class.getSimpleName())
                .build();
        new Runner(opt).run();
        LogFactory.haltInstance();
    }

    @Benchmark
    public long run() {
        // Re-decoded each call (no caching at this layer) -> per-call decode cost.
        return finder.timestampAt(targetRow);
    }

    @Setup(Level.Trial)
    public void setUp() throws Exception {
        root = Files.createTempDirectory("parquet-ts-point-bench-");
        final CairoConfiguration configuration = new DefaultCairoConfiguration(root.toString());
        engine = new CairoEngine(configuration);
        // Fixed seed: PLAIN and DELTA arms get identical data, only the encoding differs.
        final SqlExecutionContextImpl ctx = new SqlExecutionContextImpl(engine, 1)
                .with(configuration.getFactoryProvider().getSecurityContextFactory().getRootContext(),
                        null, new Rnd(0x5f3759dfL, 0x2545f4914f6cdd1dL), -1, null);

        engine.execute(
                "CREATE TABLE trades (price DOUBLE, timestamp TIMESTAMP)" +
                        " TIMESTAMP(timestamp) PARTITION BY HOUR BYPASS WAL",
                ctx
        );
        // jitter < step keeps timestamps strictly increasing (required by the ordered insert).
        final String tsExpr = "JITTER".equals(tsPattern)
                ? "(" + TS_BASE + " + (x - 1) * " + TS_STEP_MICROS + " + rnd_long(0, " + (TS_STEP_MICROS - 1) + ", 0))::timestamp"
                : "timestamp_sequence(" + TS_BASE + ", " + TS_STEP_MICROS + ")";
        engine.execute(
                "INSERT INTO trades SELECT rnd_double(), " + tsExpr +
                        " FROM long_sequence(" + ROW_COUNT + ") x",
                ctx
        );
        if ("PLAIN".equals(tsEncoding)) {
            engine.execute("ALTER TABLE trades ALTER COLUMN timestamp SET PARQUET(PLAIN)", ctx);
        }
        engine.execute("ALTER TABLE trades CONVERT PARTITION TO PARQUET WHERE timestamp >= 0", ctx);

        reader = engine.getReader("trades");
        if (reader.getPartitionFormatFromMetadata(TARGET_PARTITION) != io.questdb.cairo.sql.PartitionFormat.PARQUET) {
            throw new IllegalStateException("target partition is not parquet (CONVERT leaves the active partition native)");
        }
        final int timestampIndex = reader.getMetadata().getTimestampIndex();
        parquetDecoder = new ParquetPartitionDecoder();
        finder = new ParquetTimestampFinder(parquetDecoder);
        // Mirror IntervalBwdPartitionFrameCursor: of() -> openPartition() -> prepare().
        finder.of(reader, TARGET_PARTITION, timestampIndex);
        reader.openPartition(TARGET_PARTITION);
        finder.prepare();

        final long rowGroupSize = parquetDecoder.metadata().getRowGroupSize(0);
        final long partitionRows = reader.getPartitionRowCountFromMetadata(TARGET_PARTITION);
        switch (target) {
            case "GROUP_START":
                targetRow = 0;
                break;
            case "GROUP_MID":
                targetRow = rowGroupSize / 2;
                break;
            case "GROUP_END":
                targetRow = rowGroupSize - 1;
                break;
            case "PARTITION_END":
                targetRow = partitionRows - 1;
                break;
            default:
                throw new IllegalArgumentException("unknown target: " + target);
        }
        // Validate the finder returns the correct value, not a stale/garbage decode.
        final long got = finder.timestampAt(targetRow);
        final long slotLo = TS_BASE + targetRow * TS_STEP_MICROS;
        if ("REGULAR".equals(tsPattern)) {
            if (got != slotLo) {
                throw new IllegalStateException("timestampAt(" + targetRow + ")=" + got + " expected=" + slotLo);
            }
        } else if (got < slotLo || got >= slotLo + TS_STEP_MICROS) {
            throw new IllegalStateException("timestampAt(" + targetRow + ")=" + got + " outside jitter slot [" + slotLo + "," + (slotLo + TS_STEP_MICROS) + ")");
        }
        System.out.println("[bench] tsEncoding=" + tsEncoding + " tsPattern=" + tsPattern + " target=" + target
                + " rowGroupSize=" + rowGroupSize + " rowGroups=" + parquetDecoder.metadata().getRowGroupCount()
                + " partitionRows=" + partitionRows + " targetRow=" + targetRow + " valueOk=" + got);
    }

    @TearDown(Level.Trial)
    public void tearDown() throws Exception {
        finder = Misc.free(finder);
        parquetDecoder = Misc.free(parquetDecoder);
        reader = Misc.free(reader);
        engine = Misc.free(engine);
        if (root != null && Files.exists(root)) {
            try (java.util.stream.Stream<Path> stream = Files.walk(root)) {
                stream.sorted(Comparator.reverseOrder()).forEach(path -> {
                    try {
                        Files.deleteIfExists(path);
                    } catch (Exception ignore) {
                    }
                });
            }
            root = null;
        }
    }
}
