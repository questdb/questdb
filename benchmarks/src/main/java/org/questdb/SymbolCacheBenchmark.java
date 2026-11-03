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
import io.questdb.cairo.TableReader;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.std.Misc;
import io.questdb.std.str.Utf8StringSink;
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
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.concurrent.TimeUnit;

/**
 * Measures CACHE symbol reads, symbol filters and JSON encoding through SQL. Table {@code t} has
 * 1,000,000 distinct symbols: its first day holds each key once in key order, its second day holds
 * 1,000 sparse keys above 4,095. Table {@code low} has 1,000 distinct symbols.
 * <p>
 * The {@code firstScan} benchmarks open a new reader per invocation; {@code gc.alloc.rate.norm}
 * then shows the heap each reader needs for its values.
 * <p>
 * Build and run:
 * <pre>
 * mvn -pl benchmarks -am package -o -DskipTests
 * java --add-exports=java.base/jdk.internal.vm=ALL-UNNAMED --sun-misc-unsafe-memory-access=allow \
 *      --enable-native-access=ALL-UNNAMED -cp benchmarks/target/benchmarks.jar \
 *      org.questdb.SymbolCacheBenchmark
 * </pre>
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 5, time = 2)
@Measurement(iterations = 5, time = 2)
@Fork(value = 2, jvmArgsAppend = {
        "--add-exports=java.base/jdk.internal.vm=ALL-UNNAMED",
        "--sun-misc-unsafe-memory-access=allow",
        "--enable-native-access=ALL-UNNAMED"
})
public class SymbolCacheBenchmark {
    private static final String DAY_ONE = "ts IN '2024-01-01'";
    private static final String DAY_TWO = "ts IN '2024-01-02'";
    private final Utf8StringSink sink = new Utf8StringSink();
    private SqlExecutionContext context;
    private RecordCursorFactory denseHighKeys;
    private CairoEngine engine;
    private RecordCursorFactory hashStringIn;
    private RecordCursorFactory hashStringInSerial;
    private RecordCursorFactory likeDense;
    private RecordCursorFactory likeSparse;
    private RecordCursorFactory lowKeys;
    private RecordCursorFactory lowStrings;
    private RecordCursorFactory sparseHighKeys;

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(SymbolCacheBenchmark.class.getSimpleName())
                .addProfiler("gc")
                .build();
        new Runner(opt).run();
    }

    @Benchmark
    @BenchmarkMode(Mode.SingleShotTime)
    @Warmup(iterations = 5, batchSize = 1)
    @Measurement(iterations = 20, batchSize = 1)
    public long firstScanNoOtherReader(NoOtherReader state) throws SqlException {
        return readSymbols(denseHighKeys);
    }

    @Benchmark
    @BenchmarkMode(Mode.SingleShotTime)
    @Warmup(iterations = 5, batchSize = 1)
    @Measurement(iterations = 20, batchSize = 1)
    public long firstScanWithOpenReader(OpenReader state) throws SqlException {
        return readSymbols(denseHighKeys);
    }

    @Benchmark
    public long hashStringIn() throws SqlException {
        return readCount(hashStringIn);
    }

    @Benchmark
    public long hashStringInSerial() throws SqlException {
        return readCount(hashStringInSerial);
    }

    @Benchmark
    public long jsonLowKeys() throws SqlException {
        return encodeJson(lowKeys, true);
    }

    @Benchmark
    public long jsonStrings() throws SqlException {
        return encodeJson(lowStrings, false);
    }

    @Benchmark
    public long likeDense() throws SqlException {
        return readCount(likeDense);
    }

    @Benchmark
    public long likeSparse() throws SqlException {
        return readCount(likeSparse);
    }

    @Benchmark
    public long readDenseHighKeys() throws SqlException {
        return readSymbols(denseHighKeys);
    }

    @Benchmark
    public long readLowKeys() throws SqlException {
        return readSymbols(lowKeys);
    }

    @Benchmark
    public long readSparseHighKeys() throws SqlException {
        return readSymbols(sparseHighKeys);
    }

    @Setup(Level.Trial)
    public void setUp() throws Exception {
        final String root = System.getProperty("java.io.tmpdir") + File.separator + "symbol-cache-bench";
        Files.createDirectories(Paths.get(root));
        final CairoConfiguration configuration = new DefaultCairoConfiguration(root);
        engine = new CairoEngine(configuration);
        context = new SqlExecutionContextImpl(engine, 1).with(
                configuration.getFactoryProvider().getSecurityContextFactory().getRootContext(),
                null, null, -1, null
        );
        engine.execute("DROP TABLE IF EXISTS t", context);
        engine.execute("DROP TABLE IF EXISTS low", context);
        engine.execute("CREATE TABLE t (s SYMBOL CAPACITY 1048576 CACHE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL", context);
        engine.execute("INSERT INTO t SELECT 'symbol' || (x - 1), timestamp_sequence('2024-01-01', 1) FROM long_sequence(1_000_000)", context);
        engine.execute("INSERT INTO t SELECT 'symbol' || (x % 1_000 * 900 + 5_000), timestamp_sequence('2024-01-02', 1) FROM long_sequence(1_000_000)", context);
        engine.execute("CREATE TABLE low (s SYMBOL CAPACITY 1024 CACHE, str STRING, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL", context);
        engine.execute("INSERT INTO low SELECT 'symbol' || (x % 1_000), 'symbol' || (x % 1_000), timestamp_sequence('2024-01-01', 1) FROM long_sequence(1_000_000)", context);
        engine.releaseAllWriters();

        denseHighKeys = engine.select("SELECT s FROM t WHERE " + DAY_ONE, context);
        sparseHighKeys = engine.select("SELECT s FROM t WHERE " + DAY_TWO, context);
        lowKeys = engine.select("SELECT s FROM low", context);
        lowStrings = engine.select("SELECT str FROM low", context);
        likeDense = engine.select("SELECT count() FROM t WHERE " + DAY_ONE + " AND s LIKE '%1%'", context);
        likeSparse = engine.select("SELECT count() FROM t WHERE " + DAY_ONE + " AND s LIKE '%99999%'", context);
        final String stringIn = "SELECT count() FROM t WHERE " + DAY_ONE + " AND s::STRING IN ('symbol1', 'symbol2', 'symbol3')";
        hashStringIn = engine.select(stringIn, context);
        context.setParallelFilterEnabled(false);
        hashStringInSerial = engine.select(stringIn, context);
        context.setParallelFilterEnabled(true);
    }

    @TearDown(Level.Trial)
    public void tearDown() {
        denseHighKeys = Misc.free(denseHighKeys);
        sparseHighKeys = Misc.free(sparseHighKeys);
        lowKeys = Misc.free(lowKeys);
        lowStrings = Misc.free(lowStrings);
        likeDense = Misc.free(likeDense);
        likeSparse = Misc.free(likeSparse);
        hashStringIn = Misc.free(hashStringIn);
        hashStringInSerial = Misc.free(hashStringInSerial);
        engine = Misc.free(engine);
    }

    private long encodeJson(RecordCursorFactory factory, boolean isSymbol) throws SqlException {
        long sum = 0;
        try (RecordCursor cursor = factory.getCursor(context)) {
            final Record record = cursor.getRecord();
            while (cursor.hasNext()) {
                sink.clear();
                sink.putQuote().escapeJsonStr(isSymbol ? record.getSymA(0) : record.getStrA(0)).putQuote();
                sum += sink.size();
            }
        }
        return sum;
    }

    private long readCount(RecordCursorFactory factory) throws SqlException {
        try (RecordCursor cursor = factory.getCursor(context)) {
            final Record record = cursor.getRecord();
            return cursor.hasNext() ? record.getLong(0) : -1;
        }
    }

    private long readSymbols(RecordCursorFactory factory) throws SqlException {
        long sum = 0;
        try (RecordCursor cursor = factory.getCursor(context)) {
            final Record record = cursor.getRecord();
            while (cursor.hasNext()) {
                final CharSequence value = record.getSymA(0);
                sum += value.length() + value.charAt(value.length() - 1);
            }
        }
        return sum;
    }

    @State(Scope.Benchmark)
    public static class NoOtherReader {
        @Setup(Level.Invocation)
        public void setUp(SymbolCacheBenchmark benchmark) {
            benchmark.engine.releaseInactive();
        }
    }

    @State(Scope.Benchmark)
    public static class OpenReader {
        private SymbolCacheBenchmark benchmark;
        private TableReader reader;

        @Setup(Level.Invocation)
        public void setUp() {
            benchmark.engine.releaseInactive();
        }

        @Setup(Level.Trial)
        public void setUpReader(SymbolCacheBenchmark benchmark) throws SqlException {
            this.benchmark = benchmark;
            reader = benchmark.engine.getReader("t");
            benchmark.readSymbols(benchmark.denseHighKeys);
        }

        @TearDown(Level.Trial)
        public void tearDown() {
            reader = Misc.free(reader);
        }
    }
}
