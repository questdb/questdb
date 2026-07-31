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
import io.questdb.cairo.lv.LiveViewCheckpointDataSegmentWriter;
import io.questdb.cairo.lv.LiveViewCheckpointLayout;
import io.questdb.cairo.lv.LiveViewCheckpointPageRef;
import io.questdb.cairo.lv.LiveViewCheckpointPartitionMapEntry;
import io.questdb.cairo.lv.LiveViewCheckpointRangeRingStateBuilder;
import io.questdb.cairo.lv.LiveViewCheckpointRangeRingStateReader;
import io.questdb.cairo.lv.LiveViewCheckpointRingStateSource;
import io.questdb.cairo.lv.LiveViewCheckpointSegmentDirectoryReader;
import io.questdb.cairo.lv.LiveViewCheckpointSegmentDirectoryWriter;
import io.questdb.cairo.lv.LiveViewCheckpointStateCodec;
import io.questdb.cairo.lv.LiveViewCheckpointStatePageRef;
import io.questdb.std.FilesFacade;
import io.questdb.std.Misc;
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
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.concurrent.TimeUnit;

/**
 * Measures a whole live-view RANGE ring through the checkpoint data path, which is what
 * {@link LiveViewCheckpointStateCodecBenchmark} deliberately leaves out: the segment
 * writer's mmap, the commit and the reader's page mapping.
 * <ul>
 *   <li>{@code seal} builds a 16,384-row ring from empty, encodes every chunk into a fresh
 *       data segment and commits it, returning the published segment length. That is one
 *       boundary's data work for a function whose frame spans four chunks; the metadata
 *       publication a real boundary also performs is codec-independent and excluded.</li>
 *   <li>{@code restoreWarm} decodes every live row of a committed ring in canonical order
 *       with the segment already mapped - the steady-state restore of a reader that walks
 *       the same ring again.</li>
 *   <li>{@code restoreCold} unmaps first, so each invocation re-opens and re-maps the
 *       segment file. The difference against {@code restoreWarm} is the mapping cost.</li>
 * </ul>
 * Trial setup prints one {@code #} line per shape with the committed segment length, the
 * raw payload it stands for and the codec every page landed under, so the size and the
 * rate come from the same ring.
 * <p>
 * Each shape seals the same 16,384 rows under a regular millisecond cadence, so the
 * timestamp pages are identical across shapes and only the value pages differ.
 * {@code DECIMAL128_SUM} spends two words per row, so it seals twice the value payload of
 * the one-word shapes over twice as many chunks - compare it per byte, not per row.
 * {@code DOUBLE_NAN} and {@code LONG_RANDOM} are the raw-storage baseline.
 * <p>
 * The benchmark writes into {@code $TMPDIR/live-view-checkpoint-seal-bench} and clears it
 * at setup and teardown. A seal publishes an immutable segment per invocation, so a
 * measurement iteration leaves thousands of small files behind; the iteration teardown
 * unlinks them.
 * <p>
 * Build (note {@code -am} so the benchmark links the in-tree core, not the installed jar)
 * and run:
 * <pre>
 * mvn -pl benchmarks -am package -o -DskipTests
 * java --add-exports=java.base/jdk.internal.vm=ALL-UNNAMED --add-opens=java.base/java.lang=ALL-UNNAMED \
 *      --sun-misc-unsafe-memory-access=allow --enable-native-access=ALL-UNNAMED \
 *      -jar benchmarks/target/benchmarks.jar LiveViewCheckpointRingSealBenchmark
 * </pre>
 * Extra args are passed through to JMH (e.g. {@code -p shape=DOUBLE_PRICES -wi 1 -i 3}).
 */
@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(1)
public class LiveViewCheckpointRingSealBenchmark {

    private static final long FIXTURE_SEGMENT_ID = 1;
    private static final byte[] KEY = new byte[]{1, 2, 3};
    private static final String LV_DIR = "lv_range_seal_bench";
    // The fixture publishes the catalogue once, into an empty directory, so it
    // retires nothing and carries the first generation.
    private static final long META_GENERATION = 1;
    private static final long META_SEGMENT_ID = 1_000;
    private static final int ROWS = 4 * LiveViewCheckpointStateCodec.CHUNK_ROWS;
    private static final String ROOT =
            System.getProperty("java.io.tmpdir") + java.io.File.separator + "live-view-checkpoint-seal-bench";
    // Seal segments are published and immutable, so every invocation mints a new id. The
    // fixture the restore benchmarks read owns a lower one.
    private static final long SEAL_SEGMENT_ID_BASE = 1_000_000;

    @Param({"DOUBLE_PRICES", "DOUBLE_NAN", "LONG_NARROW", "LONG_RANDOM", "DECIMAL128_SUM"})
    public Shape shape;

    private final LiveViewCheckpointRingStateSource.Decimal128RowConsumer decimal128Consumer =
            (timestamp, hi, lo) -> this.accumulator += timestamp ^ hi ^ lo;
    private final LiveViewCheckpointPageRef directoryRoot = new LiveViewCheckpointPageRef();
    private final LiveViewCheckpointPartitionMapEntry fixture = new LiveViewCheckpointPartitionMapEntry();
    private final LiveViewCheckpointRingStateSource.RowConsumer rowConsumer =
            (timestamp, valueBits) -> this.accumulator += timestamp ^ valueBits;
    private final LiveViewCheckpointPartitionMapEntry sealEntry = new LiveViewCheckpointPartitionMapEntry();
    private long accumulator;
    private LiveViewCheckpointRangeRingStateBuilder builder;
    private Path checkpointsDir;
    private CairoConfiguration configuration;
    private LiveViewCheckpointDataSegmentWriter dataWriter;
    private LiveViewCheckpointSegmentDirectoryReader directory;
    private FilesFacade ff;
    private long firstUnlinkedSealSegmentId = SEAL_SEGMENT_ID_BASE;
    private long lastSealSegmentId = SEAL_SEGMENT_ID_BASE;
    private LiveViewCheckpointRangeRingStateReader reader;
    private Path scratchPath;
    private long[] timestamps;
    private long[] values;

    public static void main(String[] args) throws RunnerException {
        final Options opt = new OptionsBuilder()
                .include(LiveViewCheckpointRingSealBenchmark.class.getSimpleName())
                .build();
        new Runner(opt).run();
    }

    @Benchmark
    public long restoreCold() {
        // Drops every cached mapping, so the walk pays for opening and mapping the
        // segment file again - what a restore after a restart does.
        reader.detach();
        reader.of(checkpointsDir, directory, fixture);
        return walk();
    }

    @Benchmark
    public long restoreWarm() {
        reader.of(checkpointsDir, directory, fixture);
        return walk();
    }

    @Benchmark
    public long seal() {
        builder.ofEmpty(shape.valueKind, 1);
        dataWriter.of(checkpointsDir, ++lastSealSegmentId);
        appendRows();
        builder.freeze(dataWriter, KEY, 0, 0, 0, 0, ROWS, sealEntry);
        return dataWriter.commit();
    }

    @Setup(Level.Trial)
    public void setUp() throws Exception {
        // The configuration probes the root for mixed-IO support, so it must exist first.
        java.nio.file.Files.createDirectories(java.nio.file.Paths.get(ROOT));
        configuration = new DefaultCairoConfiguration(ROOT);
        ff = configuration.getFilesFacade();
        checkpointsDir = new Path().of(ROOT).concat(LV_DIR).concat(LiveViewCheckpointLayout.CHECKPOINT_DIR_NAME);
        scratchPath = new Path();
        // Every shape is a trial of its own and mints the same segment ids, so the
        // previous trial's published segments have to go before this one writes.
        ff.rmdir(scratchPath.of(checkpointsDir));
        ff.mkdirs(LiveViewCheckpointLayout.dataDirPath(scratchPath, checkpointsDir).slash(), configuration.getMkDirMode());
        ff.mkdirs(LiveViewCheckpointLayout.metaDirPath(scratchPath, checkpointsDir).slash(), configuration.getMkDirMode());

        firstUnlinkedSealSegmentId = SEAL_SEGMENT_ID_BASE;
        lastSealSegmentId = SEAL_SEGMENT_ID_BASE;
        generateRows();
        builder = new LiveViewCheckpointRangeRingStateBuilder(configuration);
        dataWriter = new LiveViewCheckpointDataSegmentWriter(configuration);
        reader = new LiveViewCheckpointRangeRingStateReader(configuration);
        directory = new LiveViewCheckpointSegmentDirectoryReader(configuration);

        // The ring the restore benchmarks walk is sealed once, here, and published into
        // the segment directory a chunk read validates its page references against.
        builder.ofEmpty(shape.valueKind, 1);
        dataWriter.of(checkpointsDir, FIXTURE_SEGMENT_ID);
        appendRows();
        builder.freeze(dataWriter, KEY, 0, 0, 0, 0, ROWS, fixture);
        final long segmentBytes = dataWriter.commit();
        try (LiveViewCheckpointSegmentDirectoryWriter directoryWriter =
                     new LiveViewCheckpointSegmentDirectoryWriter(configuration)) {
            directoryWriter.of(checkpointsDir);
            directoryWriter.begin(directoryRoot);
            directoryWriter.addSegment(FIXTURE_SEGMENT_ID, segmentBytes, 1);
            directoryWriter.publish(META_SEGMENT_ID, META_GENERATION, directoryRoot);
            directory.of(checkpointsDir, directoryRoot);
        }
        printFixture(segmentBytes);
    }

    @TearDown(Level.Trial)
    public void tearDown() {
        builder = Misc.free(builder);
        dataWriter = Misc.free(dataWriter);
        reader = Misc.free(reader);
        directory = Misc.free(directory);
        unlinkSealSegments();
        ff.rmdir(scratchPath.of(checkpointsDir));
        checkpointsDir = Misc.free(checkpointsDir);
        scratchPath = Misc.free(scratchPath);
    }

    @TearDown(Level.Iteration)
    public void unlinkSealSegments() {
        while (firstUnlinkedSealSegmentId < lastSealSegmentId) {
            final long segmentId = ++firstUnlinkedSealSegmentId;
            ff.removeQuiet(LiveViewCheckpointLayout.dataSegmentPath(scratchPath, checkpointsDir, segmentId).$());
        }
    }

    private static String codecName(int codec) {
        if (codec == LiveViewCheckpointStateCodec.RAW_64) {
            return "raw";
        }
        return codec == LiveViewCheckpointStateCodec.COVERING_DOUBLE ? "covering-double" : "covering-long";
    }

    private void appendRows() {
        if (shape.valueWords == 2) {
            for (int i = 0; i < ROWS; i++) {
                builder.append(dataWriter, timestamps[i], values[2 * i], values[2 * i + 1]);
            }
            return;
        }
        for (int i = 0; i < ROWS; i++) {
            builder.append(dataWriter, timestamps[i], values[i]);
        }
    }

    /**
     * Materializes the rows a seal appends up front, so neither benchmark pays for
     * generating them. Every shape shares one regular millisecond cadence: the timestamp
     * pages are then identical across shapes and the value pages are the only difference.
     */
    private void generateRows() {
        final Rnd rnd = new Rnd(0x9876_5432L, 0x1020_3040L);
        timestamps = new long[ROWS];
        values = new long[ROWS * shape.valueWords];
        for (int i = 0; i < ROWS; i++) {
            timestamps[i] = 1_700_000_000_000_000L + i * 1_000L;
            switch (shape) {
                case DOUBLE_PRICES:
                    values[i] = Double.doubleToRawLongBits(100.0 + i * 0.01);
                    break;
                case DOUBLE_NAN:
                    values[i] = 0x7ff8_dead_beef_1234L;
                    break;
                case LONG_NARROW:
                    values[i] = i % 1024;
                    break;
                case LONG_RANDOM:
                    values[i] = rnd.nextLong();
                    break;
                case DECIMAL128_SUM:
                    // A running DECIMAL128 sum: the high word repeats, the low word grows.
                    values[2 * i] = 0;
                    values[2 * i + 1] = 500_000_000L + i * 1_237L;
                    break;
                default:
                    throw new IllegalArgumentException("unknown shape: " + shape);
            }
        }
    }

    private void printFixture(long segmentBytes) {
        final long rawPayload = (long) ROWS * Long.BYTES * (1 + shape.valueWords);
        final StringBuilder pages = new StringBuilder();
        for (int i = 0, n = fixture.getStatePageCount(); i < n; i++) {
            final LiveViewCheckpointStatePageRef ref = fixture.getStatePageRef(i);
            if (i > 0) {
                pages.append(", ");
            }
            pages.append(codecName(ref.getCodec())).append(':').append(ref.getStoredLength()).append('B');
        }
        System.out.println("# " + shape
                + " ring: rows=" + ROWS
                + ", pages=" + fixture.getStatePageCount()
                + ", segment=" + segmentBytes + "B"
                + ", raw payload=" + rawPayload + "B"
                + ", ratio=" + String.format("%.3f", (double) segmentBytes / rawPayload)
                + ", pageCodecs=[" + pages + ']');
    }

    private long walk() {
        accumulator = 0;
        if (shape.valueWords == 2) {
            reader.forEachRow(decimal128Consumer);
        } else {
            reader.forEachRow(rowConsumer);
        }
        return accumulator;
    }

    /**
     * The ring shapes worth measuring end to end: a compressible and a raw case for each
     * of the two one-word codec families, plus a flattened wide-decimal word stream.
     */
    public enum Shape {
        DOUBLE_PRICES(LiveViewCheckpointRangeRingStateReader.VALUE_KIND_DOUBLE, 1),
        DOUBLE_NAN(LiveViewCheckpointRangeRingStateReader.VALUE_KIND_DOUBLE, 1),
        LONG_NARROW(LiveViewCheckpointRangeRingStateReader.VALUE_KIND_LONG, 1),
        LONG_RANDOM(LiveViewCheckpointRangeRingStateReader.VALUE_KIND_LONG, 1),
        DECIMAL128_SUM(LiveViewCheckpointRangeRingStateReader.VALUE_KIND_DECIMAL128, 2);

        private final int valueKind;
        private final int valueWords;

        Shape(int valueKind, int valueWords) {
            this.valueKind = valueKind;
            this.valueWords = valueWords;
        }
    }
}
