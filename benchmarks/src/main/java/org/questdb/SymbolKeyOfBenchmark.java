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
import io.questdb.cairo.SymbolMapReader;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.sql.StaticSymbolTable;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.std.Misc;
import io.questdb.std.Rnd;
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
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.concurrent.TimeUnit;

/**
 * Measures {@code keyOf()} on a table reader's symbol map. {@code viewKeyOf} goes through a symbol
 * table view, as the fused hash join's SYMBOL key translation does per value. {@code readerKeyOf}
 * goes through the reader's own symbol map, as the SYMBOL join key mappings and window joins do per
 * row; each of its lookups takes a cursor from the index reader's pool and returns it.
 * <p>
 * {@code symbolCount} sets the number of distinct values. The column capacity is the server
 * default, 256, or the symbol count when that is larger, and the index has
 * {@code ceilPow2(capacity / 2)} hash buckets. {@code missPercent} sets the share of probe values
 * that are absent from the table. A missing value whose hash falls past the index's highest used
 * bucket gets an empty cursor; with few symbols that is a sizeable share of misses. Each
 * invocation looks up {@link #PROBE_COUNT} values, so the score is the cost of one lookup.
 * <p>
 * Build and run (note {@code -am} so the benchmark links the in-tree core):
 * <pre>
 * mvn -pl benchmarks -am package -o -DskipTests
 * java --add-exports=java.base/jdk.internal.vm=ALL-UNNAMED --sun-misc-unsafe-memory-access=allow \
 *      --enable-native-access=ALL-UNNAMED -cp benchmarks/target/benchmarks.jar \
 *      org.questdb.SymbolKeyOfBenchmark
 * </pre>
 */
@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(value = 1, jvmArgsAppend = {
        "--add-exports=java.base/jdk.internal.vm=ALL-UNNAMED",
        "--sun-misc-unsafe-memory-access=allow",
        "--enable-native-access=ALL-UNNAMED"
})
public class SymbolKeyOfBenchmark {
    // The server's default symbol capacity.
    private static final int DEFAULT_CAPACITY = 256;
    private static final int PROBE_COUNT = 4096;
    private final String[] probes = new String[PROBE_COUNT];
    @Param({"0", "50", "100"})
    public int missPercent;
    @Param({"10", "100", "10000", "1000000"})
    public int symbolCount;
    private CairoEngine engine;
    private TableReader reader;
    private SymbolMapReader symbolMapReader;
    private StaticSymbolTable symbolTable;

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(SymbolKeyOfBenchmark.class.getSimpleName())
                .addProfiler("gc")
                .build();
        new Runner(opt).run();
    }

    @Benchmark
    @OperationsPerInvocation(PROBE_COUNT)
    public long readerKeyOf() {
        long sum = 0;
        for (int i = 0; i < PROBE_COUNT; i++) {
            sum += symbolMapReader.keyOf(probes[i]);
        }
        return sum;
    }

    @Benchmark
    @OperationsPerInvocation(PROBE_COUNT)
    public long viewKeyOf() {
        long sum = 0;
        for (int i = 0; i < PROBE_COUNT; i++) {
            sum += symbolTable.keyOf(probes[i]);
        }
        return sum;
    }

    @Setup(Level.Trial)
    public void setUp() throws Exception {
        final String root = System.getProperty("java.io.tmpdir") + File.separator + "symbol-key-of-bench";
        Files.createDirectories(Paths.get(root));
        final CairoConfiguration configuration = new DefaultCairoConfiguration(root);
        engine = new CairoEngine(configuration);
        final SqlExecutionContext ctx = new SqlExecutionContextImpl(engine, 1).with(
                configuration.getFactoryProvider().getSecurityContextFactory().getRootContext(),
                null, null, -1, null
        );
        engine.execute("DROP TABLE IF EXISTS t", ctx);
        engine.execute("CREATE TABLE t (s SYMBOL CAPACITY " + Math.max(symbolCount, DEFAULT_CAPACITY) + " NOCACHE)", ctx);
        engine.execute("INSERT INTO t SELECT 'sym_' || x FROM long_sequence(" + symbolCount + ")", ctx);
        engine.releaseAllWriters();

        reader = engine.getReader("t");
        symbolMapReader = reader.getSymbolMapReader(0);
        symbolTable = reader.newSymbolTable(0);

        final Rnd rnd = new Rnd();
        for (int i = 0; i < PROBE_COUNT; i++) {
            final boolean isMiss = rnd.nextInt(100) < missPercent;
            final int n = 1 + rnd.nextInt(symbolCount);
            probes[i] = isMiss ? "miss_" + n : "sym_" + n;
        }
    }

    @TearDown(Level.Trial)
    public void tearDown() {
        Misc.freeIfCloseable(symbolTable);
        reader = Misc.free(reader);
        engine = Misc.free(engine);
    }
}
