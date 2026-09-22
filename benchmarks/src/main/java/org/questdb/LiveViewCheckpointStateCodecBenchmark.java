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

import io.questdb.cairo.lv.LiveViewCheckpointStateCodec;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryCARW;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.Rnd;
import io.questdb.std.Unsafe;
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
 * Measures what the format-1 live-view checkpoint codec costs per page. One page is
 * a full 4096-word chunk, which is what {@code LiveViewCheckpointStateCodec.CHUNK_ROWS}
 * caps a sealed chunk at, so every trial encodes and decodes 32 KiB of payload:
 * <ul>
 *   <li>{@code seal} trials the covering candidates the page kind allows and copies
 *       the shortest one into an in-memory sink - the work
 *       {@code LiveViewCheckpointRangeRingStateBuilder.sealTail()} does per page;</li>
 *   <li>{@code restore} runs one stored page through the bounded checked decoder -
 *       the work {@code LiveViewCheckpointRangeRingStateReader}'s walk does per page.</li>
 * </ul>
 * Neither method touches a file: {@link LiveViewCheckpointRingSealBenchmark} covers the
 * segment-writer and mmap cost end to end.
 * <p>
 * The distributions are the ones the size table in the state-codec design pins, plus a
 * bursty cadence and the flattened DECIMAL128/256 word streams. {@code LONG_RANDOM} and
 * {@code DOUBLE_NAN} are the raw-storage baseline: their pages lose to raw, so their
 * numbers are the cost of a rejected candidate plus a 32 KiB copy, which is what every
 * compressed page has to be read against.
 * <p>
 * Trial setup prints one {@code #} line per distribution with the codec the page landed
 * under and its exact stored length, so a rate can be paired with the size that bought it.
 * <p>
 * Build (note {@code -am} so the benchmark links the in-tree core, not the installed jar)
 * and run:
 * <pre>
 * mvn -pl benchmarks -am package -o -DskipTests
 * java --add-exports=java.base/jdk.internal.vm=ALL-UNNAMED --add-opens=java.base/java.lang=ALL-UNNAMED \
 *      --sun-misc-unsafe-memory-access=allow --enable-native-access=ALL-UNNAMED \
 *      -jar benchmarks/target/benchmarks.jar LiveViewCheckpointStateCodecBenchmark
 * </pre>
 * Extra args are passed through to JMH (e.g. {@code -p distribution=LONG_NARROW -wi 1 -i 3}).
 */
@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(1)
public class LiveViewCheckpointStateCodecBenchmark {

    /**
     * Mirrors {@code CoveringCompressor.LINEAR_PRED_FLAG}, which is package-private. Used
     * only to label a covering long page as plain or linear-prediction FoR in the printed
     * size line; nothing the benchmark measures depends on it.
     */
    private static final int LINEAR_PRED_FLAG = 0xC0;
    private static final int PAGE_WORDS = LiveViewCheckpointStateCodec.CHUNK_ROWS;
    // One sink page large enough to hold any encoded chunk contiguously, so the decode
    // benchmark can read a stored page back through a single address.
    private static final int SINK_PAGE_SIZE = 1024 * 1024;

    @Param({"TIMESTAMP_REGULAR", "TIMESTAMP_JITTERED", "TIMESTAMP_BURSTY",
            "LONG_CONSTANT", "LONG_NARROW", "LONG_RANDOM",
            "DECIMAL128_WORDS", "DECIMAL256_WORDS",
            "DOUBLE_CONSTANT", "DOUBLE_PRICES", "DOUBLE_NAN"})
    public Distribution distribution;

    private MemoryCARW encoded;
    private long encodedAddress;
    private int encodedCodec;
    private int encodedLength;
    private MemoryCARW sink;
    private LiveViewCheckpointStateCodec.Scratch source;
    private long sourceAddress;
    private LiveViewCheckpointStateCodec.Scratch target;
    private long targetAddress;

    public static void main(String[] args) throws RunnerException {
        final Options opt = new OptionsBuilder()
                .include(LiveViewCheckpointStateCodecBenchmark.class.getSimpleName())
                .build();
        new Runner(opt).run();
    }

    @Benchmark
    public int restore() {
        if (distribution.family == Family.DOUBLE) {
            return LiveViewCheckpointStateCodec.decodeDoubles(
                    encodedAddress, encodedLength, encodedCodec, PAGE_WORDS, targetAddress, PAGE_WORDS, target
            );
        }
        return LiveViewCheckpointStateCodec.decodeLongs(
                encodedAddress, encodedLength, encodedCodec, PAGE_WORDS, targetAddress, PAGE_WORDS, target
        );
    }

    @Benchmark
    public int seal() {
        sink.jumpTo(0);
        return encode(sink);
    }

    @Setup(Level.Trial)
    public void setUp() {
        source = new LiveViewCheckpointStateCodec.Scratch(null);
        target = new LiveViewCheckpointStateCodec.Scratch(null);
        sink = Vm.getCARWInstance(SINK_PAGE_SIZE, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT);
        encoded = Vm.getCARWInstance(SINK_PAGE_SIZE, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT);
        sourceAddress = distribution.family == Family.TIMESTAMP
                ? source.timestampsAddress()
                : source.valuesAddress();
        targetAddress = target.valuesAddress();
        fill(distribution, sourceAddress);

        // The page the restore benchmark decodes is encoded once, here, so the two
        // methods measure the same bytes the seal method produces.
        encodedCodec = encode(encoded);
        encodedLength = (int) encoded.getAppendOffset();
        encodedAddress = encoded.addressOf(0);
        System.out.println("# " + distribution
                + ": codec=" + codecName(encodedAddress, encodedCodec)
                + ", stored=" + encodedLength + "B"
                + ", raw=" + PAGE_WORDS * Long.BYTES + "B"
                + ", ratio=" + String.format("%.3f", (double) encodedLength / (PAGE_WORDS * Long.BYTES)));
    }

    @TearDown(Level.Trial)
    public void tearDown() {
        source = Misc.free(source);
        target = Misc.free(target);
        sink = Misc.free(sink);
        encoded = Misc.free(encoded);
    }

    private static String codecName(long pageAddress, int codec) {
        if (codec == LiveViewCheckpointStateCodec.RAW_64) {
            return "raw";
        }
        if (codec == LiveViewCheckpointStateCodec.COVERING_DOUBLE) {
            return "covering-double-alp";
        }
        return (Unsafe.getByte(pageAddress + Integer.BYTES) & LINEAR_PRED_FLAG) == LINEAR_PRED_FLAG
                ? "covering-long-linear-for"
                : "covering-long-plain-for";
    }

    /**
     * Writes {@code PAGE_WORDS} words of the given distribution, so every page carries
     * the same 32 KiB payload whatever shape produced it. A wide decimal spends several
     * words per row - 2048 DECIMAL128 rows or 1024 DECIMAL256 rows fill the same page -
     * exactly as the ring builder flattens them, most significant word first.
     */
    private static void fill(Distribution distribution, long address) {
        final Rnd rnd = new Rnd(0x9876_5432L, 0x1020_3040L);
        long timestamp = 1_700_000_000_000_000L;
        switch (distribution) {
            case TIMESTAMP_REGULAR:
                for (int i = 0; i < PAGE_WORDS; i++) {
                    put(address, i, timestamp + i * 1_000L);
                }
                break;
            case TIMESTAMP_JITTERED:
                for (int i = 0; i < PAGE_WORDS; i++) {
                    timestamp += (i % 7) * 100L;
                    put(address, i, timestamp);
                }
                break;
            case TIMESTAMP_BURSTY:
                // 64-row bursts a microsecond apart, separated by second-long idle gaps:
                // a cadence neither a plain nor a linear stride fits well.
                for (int i = 0; i < PAGE_WORDS; i++) {
                    timestamp += (i % 64) == 0 ? 1_000_000_000L : 1_000L;
                    put(address, i, timestamp);
                }
                break;
            case LONG_CONSTANT:
                for (int i = 0; i < PAGE_WORDS; i++) {
                    put(address, i, -12_345L);
                }
                break;
            case LONG_NARROW:
                for (int i = 0; i < PAGE_WORDS; i++) {
                    put(address, i, i % 1024);
                }
                break;
            case LONG_RANDOM:
                for (int i = 0; i < PAGE_WORDS; i++) {
                    put(address, i, rnd.nextLong());
                }
                break;
            case DECIMAL128_WORDS:
                // A running DECIMAL128 sum: the high word repeats, the low word grows.
                for (int i = 0; i < PAGE_WORDS / 2; i++) {
                    put(address, 2 * i, 0L);
                    put(address, 2 * i + 1, 500_000_000L + i * 1_237L);
                }
                break;
            case DECIMAL256_WORDS:
                for (int i = 0; i < PAGE_WORDS / 4; i++) {
                    put(address, 4 * i, 0L);
                    put(address, 4 * i + 1, 0L);
                    put(address, 4 * i + 2, 0L);
                    put(address, 4 * i + 3, 500_000_000L + i * 1_237L);
                }
                break;
            case DOUBLE_CONSTANT:
                for (int i = 0; i < PAGE_WORDS; i++) {
                    put(address, i, Double.doubleToRawLongBits(42.5));
                }
                break;
            case DOUBLE_PRICES:
                for (int i = 0; i < PAGE_WORDS; i++) {
                    put(address, i, Double.doubleToRawLongBits(100.0 + i * 0.01));
                }
                break;
            case DOUBLE_NAN:
                // ALP cannot represent an arbitrary NaN payload at all: every value
                // becomes an exception and the page falls back to raw.
                for (int i = 0; i < PAGE_WORDS; i++) {
                    put(address, i, 0x7ff8_dead_beef_1234L);
                }
                break;
            default:
                throw new IllegalArgumentException("unknown distribution: " + distribution);
        }
    }

    private static void put(long address, int index, long value) {
        Unsafe.putLong(address + (long) index * Long.BYTES, value);
    }

    private int encode(MemoryCARW pageSink) {
        switch (distribution.family) {
            case TIMESTAMP:
                return LiveViewCheckpointStateCodec.encodeTimestamps(pageSink, source, sourceAddress, PAGE_WORDS);
            case DOUBLE:
                return LiveViewCheckpointStateCodec.encodeDoubles(pageSink, source, sourceAddress, PAGE_WORDS);
            default:
                return LiveViewCheckpointStateCodec.encodeLongs(pageSink, source, sourceAddress, PAGE_WORDS);
        }
    }

    /**
     * The page kind's codec family, which decides which candidates a seal trials: a
     * timestamp page also trials linear-prediction FoR, an integer-oriented value page
     * trials plain FoR only, and a double page trials ALP.
     */
    public enum Family {
        DOUBLE, LONG, TIMESTAMP
    }

    public enum Distribution {
        TIMESTAMP_REGULAR(Family.TIMESTAMP),
        TIMESTAMP_JITTERED(Family.TIMESTAMP),
        TIMESTAMP_BURSTY(Family.TIMESTAMP),
        LONG_CONSTANT(Family.LONG),
        LONG_NARROW(Family.LONG),
        LONG_RANDOM(Family.LONG),
        DECIMAL128_WORDS(Family.LONG),
        DECIMAL256_WORDS(Family.LONG),
        DOUBLE_CONSTANT(Family.DOUBLE),
        DOUBLE_PRICES(Family.DOUBLE),
        DOUBLE_NAN(Family.DOUBLE);

        private final Family family;

        Distribution(Family family) {
            this.family = family;
        }
    }
}
