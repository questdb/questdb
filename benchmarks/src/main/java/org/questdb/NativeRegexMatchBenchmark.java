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

import io.questdb.std.MemoryTag;
import io.questdb.std.Os;
import io.questdb.std.Unsafe;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
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

import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.Linker;
import java.lang.foreign.SymbolLookup;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandle;
import java.nio.charset.StandardCharsets;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Compares regex match throughput for the {@code ~} operator over VARCHAR:
 * <ul>
 *   <li>{@code jdk} — {@code java.util.regex.Matcher#find()} over a UTF-16 {@code String}
 *       (models today's inherited {@code MatchStr} path);</li>
 *   <li>{@code nativeZeroCopy} — Rust {@code regex} crate via a JDK 25 FFM {@code critical}
 *       downcall, matching directly over the column's native UTF-8 bytes (the prototype fast path);</li>
 *   <li>{@code nativeCopyThenMatch} — same downcall but first copying the bytes into a scratch
 *       buffer (models the on-heap / non-contiguous fallback, to isolate the copy cost).</li>
 * </ul>
 * <p>
 * Self-contained: it binds the {@code qdb_regex_*} symbols exported by {@code libquestdbr}
 * itself (the production binding lives in the package-private
 * {@code io.questdb.griffin.engine.functions.regex.NativeRegex}, not visible to this module).
 * <p>
 * Run (the native library must be built and on the path, and FFM native access granted):
 * <pre>
 * mvn -pl benchmarks compile
 * java --enable-native-access=ALL-UNNAMED -cp ... org.questdb.NativeRegexMatchBenchmark
 * </pre>
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 3, time = 2)
@Measurement(iterations = 5, time = 2)
@Fork(value = 1, jvmArgsAppend = {"--enable-native-access=ALL-UNNAMED", "--enable-native-access=io.questdb.benchmarks"})
public class NativeRegexMatchBenchmark {

    private static final int N = 4096;
    private static final String PATTERN = "[a-f][0-9]{2,4}x";

    @Param({"8", "32", "128"})
    private int len;

    private long dataBase;
    private long dataSize;
    private long handle;
    private Matcher jdkMatcher;
    private int[] lens;
    private MethodHandle mhFind;
    private MethodHandle mhFree;
    private long[] ptrs;
    private long scratch;
    private long scratchCap;
    private String[] samples;

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(NativeRegexMatchBenchmark.class.getSimpleName())
                .build();
        new Runner(opt).run();
    }

    @Benchmark
    @OperationsPerInvocation(N)
    public void jdk(Blackhole bh) {
        final String[] s = samples;
        final Matcher m = jdkMatcher;
        for (int i = 0; i < N; i++) {
            bh.consume(m.reset(s[i]).find());
        }
    }

    @Benchmark
    @OperationsPerInvocation(N)
    public void nativeCopyThenMatch(Blackhole bh) throws Throwable {
        final long h = handle;
        final long dst = scratch;
        for (int i = 0; i < N; i++) {
            final int n = lens[i];
            Unsafe.copyMemory(ptrs[i], dst, n);
            bh.consume((int) mhFind.invokeExact(h, dst, (long) n) != 0);
        }
    }

    @Benchmark
    @OperationsPerInvocation(N)
    public void nativeZeroCopy(Blackhole bh) throws Throwable {
        final long h = handle;
        for (int i = 0; i < N; i++) {
            bh.consume((int) mhFind.invokeExact(h, ptrs[i], (long) lens[i]) != 0);
        }
    }

    @Setup(Level.Trial)
    public void setup() throws Throwable {
        Os.init(); // ensure libquestdbr is loaded before loaderLookup()
        final Linker linker = Linker.nativeLinker();
        final SymbolLookup lookup = SymbolLookup.loaderLookup();
        final MethodHandle mhCompile = linker.downcallHandle(
                lookup.find("qdb_regex_compile").orElseThrow(),
                FunctionDescriptor.of(ValueLayout.JAVA_LONG, ValueLayout.JAVA_LONG, ValueLayout.JAVA_LONG)
        );
        mhFind = linker.downcallHandle(
                lookup.find("qdb_regex_find").orElseThrow(),
                FunctionDescriptor.of(ValueLayout.JAVA_INT, ValueLayout.JAVA_LONG, ValueLayout.JAVA_LONG, ValueLayout.JAVA_LONG),
                Linker.Option.critical(false)
        );
        mhFree = linker.downcallHandle(
                lookup.find("qdb_regex_free").orElseThrow(),
                FunctionDescriptor.ofVoid(ValueLayout.JAVA_LONG)
        );

        // compile the pattern with both engines
        final byte[] pb = PATTERN.getBytes(StandardCharsets.UTF_8);
        final long pp = Unsafe.malloc(pb.length, MemoryTag.NATIVE_DEFAULT);
        for (int i = 0; i < pb.length; i++) {
            Unsafe.getUnsafe().putByte(pp + i, pb[i]);
        }
        handle = (long) mhCompile.invokeExact(pp, (long) pb.length);
        Unsafe.free(pp, pb.length, MemoryTag.NATIVE_DEFAULT);
        if (handle == 0) {
            throw new IllegalStateException("native regex unavailable or pattern unsupported");
        }
        jdkMatcher = Pattern.compile(PATTERN).matcher("");

        // generate sample data and lay UTF-8 bytes out contiguously in native memory
        final Random rnd = new Random(42);
        samples = new String[N];
        lens = new int[N];
        final byte[][] enc = new byte[N][];
        long total = 0;
        int maxLen = 1;
        for (int i = 0; i < N; i++) {
            final String s = randomSample(rnd, len);
            samples[i] = s;
            final byte[] b = s.getBytes(StandardCharsets.UTF_8);
            enc[i] = b;
            lens[i] = b.length;
            total += b.length;
            maxLen = Math.max(maxLen, b.length);
        }
        dataSize = total;
        dataBase = Unsafe.malloc(total, MemoryTag.NATIVE_DEFAULT);
        ptrs = new long[N];
        long off = dataBase;
        for (int i = 0; i < N; i++) {
            final byte[] b = enc[i];
            for (int j = 0; j < b.length; j++) {
                Unsafe.getUnsafe().putByte(off + j, b[j]);
            }
            ptrs[i] = off;
            off += b.length;
        }
        scratchCap = maxLen;
        scratch = Unsafe.malloc(scratchCap, MemoryTag.NATIVE_DEFAULT);
    }

    @TearDown(Level.Trial)
    public void tearDown() throws Throwable {
        if (handle != 0) {
            mhFree.invokeExact(handle);
            handle = 0;
        }
        if (dataBase != 0) {
            Unsafe.free(dataBase, dataSize, MemoryTag.NATIVE_DEFAULT);
            dataBase = 0;
        }
        if (scratch != 0) {
            Unsafe.free(scratch, scratchCap, MemoryTag.NATIVE_DEFAULT);
            scratch = 0;
        }
    }

    private static String randomSample(Random rnd, int len) {
        // mostly lowercase letters/digits; ~50% of rows are seeded to contain a match
        final char[] c = new char[len];
        for (int i = 0; i < len; i++) {
            final int r = rnd.nextInt(36);
            c[i] = (char) (r < 26 ? 'a' + r : '0' + (r - 26));
        }
        if (len >= 6 && rnd.nextBoolean()) {
            final int p = rnd.nextInt(len - 5);
            c[p] = (char) ('a' + rnd.nextInt(6)); // [a-f]
            c[p + 1] = (char) ('0' + rnd.nextInt(10));
            c[p + 2] = (char) ('0' + rnd.nextInt(10));
            c[p + 3] = (char) ('0' + rnd.nextInt(10));
            c[p + 4] = 'x';
        }
        return new String(c);
    }
}
