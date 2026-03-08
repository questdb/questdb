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
import io.questdb.cairo.CairoConfigurationWrapper;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.ColumnTypes;
import io.questdb.cairo.DefaultCairoConfiguration;
import io.questdb.cairo.EntityColumnFilter;
import io.questdb.cairo.LoopingRecordSink;
import io.questdb.cairo.RecordSink;
import io.questdb.cairo.RecordSinkFactory;
import io.questdb.cairo.RecordSinkSPI;
import io.questdb.cairo.arr.ArrayView;
import io.questdb.cairo.sql.Record;
import io.questdb.std.BinarySequence;
import io.questdb.std.BytecodeAssembler;
import io.questdb.std.Decimal128;
import io.questdb.std.Decimal256;
import io.questdb.std.Interval;
import io.questdb.std.Long256;
import io.questdb.std.Rnd;
import io.questdb.std.str.Utf8Sequence;
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
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.concurrent.TimeUnit;

/**
 * Benchmark comparing bytecode-generated vs chunked vs loop-based RecordSink implementations.
 * <p>
 * This benchmark measures the throughput of copying records between Record and RecordSinkSPI
 * using different implementation strategies:
 * 1. BYTECODE - Single-method bytecode generation (fastest, limited to ~600 columns)
 * 2. CHUNKED - Multi-method chunked bytecode generation (fast, handles more columns)
 * 3. LOOP - Loop-based interpretation (slowest, handles unlimited columns)
 * <p>
 * Run with:
 * mvn clean package -DskipTests -pl benchmarks -am 2>&1 | tail -3
 * java -jar benchmarks/target/benchmarks.jar RecordSinkBenchmark 2>&1 | tee /tmp/record_sink_benchmark.txt
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 3, time = 1)
@Fork(value = 1, jvmArgs = {"-Xms2G", "-Xmx2G"})
public class RecordSinkBenchmark {
    private static final int BATCH_SIZE = 10000;
    private static final int RANDOM_SEED = 42;
    // Column types to randomly choose from (simple types that don't need special handling)
    private static final int[] SIMPLE_TYPES = {
            ColumnType.INT,
            ColumnType.LONG,
            ColumnType.DOUBLE,
            ColumnType.FLOAT,
            ColumnType.SHORT,
            ColumnType.BYTE,
            ColumnType.BOOLEAN,
            ColumnType.TIMESTAMP
    };
    @Param({"10", "50", "100", "200", "500", "1000", "2000", "3000", "4000", "5000", "6000"})
    private int columnCount;
    @Param({"BYTECODE", "CHUNKED", "LOOP"})
    private String implementation;
    private MockRecord record;
    private RecordSink sink;
    private MockRecordSinkSPI spi;

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(RecordSinkBenchmark.class.getSimpleName())
                .forks(1)
                .jvmArgs("-Xms2G", "-Xmx2G")
                .build();
        new Runner(opt).run();
    }

    /**
     * Measures throughput by copying many records in a batch.
     */
    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    @OutputTimeUnit(TimeUnit.SECONDS)
    public void benchmarkBatchCopy(Blackhole bh) {
        for (int i = 0; i < BATCH_SIZE; i++) {
            sink.copy(record, spi);
        }
        bh.consume(spi);
    }

    @Setup(Level.Trial)
    public void setup() {
        Rnd rnd = new Rnd(RANDOM_SEED, RANDOM_SEED);

        // Generate random column types (deterministic with fixed seed)
        int[] columnTypes = new int[columnCount];
        for (int i = 0; i < columnCount; i++) {
            columnTypes[i] = SIMPLE_TYPES[rnd.nextInt(SIMPLE_TYPES.length)];
        }

        // Create mock metadata
        MockColumnTypes types = new MockColumnTypes(columnTypes);
        EntityColumnFilter columnFilter = new EntityColumnFilter();
        columnFilter.of(columnCount);

        // Create sink based on implementation type
        CairoConfiguration baseConfig = new DefaultCairoConfiguration(System.getProperty("java.io.tmpdir"));
        switch (implementation) {
            case "BYTECODE":
                // Force single-method bytecode generation by using very high limit
                CairoConfiguration configNoChunking = new CairoConfigurationWrapper(baseConfig) {
                    @Override
                    public boolean isCopierChunkedEnabled() {
                        return false;
                    }
                };
                sink = RecordSinkFactory.getInstance(
                        configNoChunking,
                        new BytecodeAssembler(),
                        types,
                        columnFilter,
                        null,
                        null,
                        null,
                        null,
                        null,
                        Integer.MAX_VALUE  // Very high limit to force single-method
                );
                break;
            case "CHUNKED":
                // Use chunked bytecode generation
                CairoConfiguration configWithChunking = new CairoConfigurationWrapper(baseConfig) {
                    @Override
                    public boolean isCopierChunkedEnabled() {
                        return true;
                    }
                };
                sink = RecordSinkFactory.getInstance(
                        configWithChunking,
                        new BytecodeAssembler(),
                        types,
                        columnFilter,
                        null,
                        null,
                        null,
                        null,
                        null,
                        RecordSinkFactory.DEFAULT_METHOD_SIZE_LIMIT
                );
                break;
            case "LOOP":
                // Use loop implementation directly
                sink = new LoopingRecordSink(types, columnFilter, null, null, null, null);
                break;
            default:
                throw new IllegalArgumentException("Unknown implementation: " + implementation);
        }

        // Create mock record and spi
        record = new MockRecord(rnd);
        spi = new MockRecordSinkSPI();
    }

    /**
     * Mock ColumnTypes implementation for benchmark.
     */
    private record MockColumnTypes(int[] types) implements ColumnTypes {

        @Override
        public int getColumnCount() {
            return types.length;
        }

        @Override
        public int getColumnType(int columnIndex) {
            return types[columnIndex];
        }
    }

    /**
     * Mock Record implementation that returns random values.
     */
    private record MockRecord(Rnd rnd) implements Record {

        @Override
        public boolean getBool(int col) {
            return rnd.nextBoolean();
        }

        @Override
        public byte getByte(int col) {
            return rnd.nextByte();
        }

        @Override
        public char getChar(int col) {
            return rnd.nextChar();
        }

        @Override
        public long getDate(int col) {
            return rnd.nextLong();
        }

        @Override
        public double getDouble(int col) {
            return rnd.nextDouble();
        }

        @Override
        public float getFloat(int col) {
            return rnd.nextFloat();
        }

        @Override
        public int getInt(int col) {
            return rnd.nextInt();
        }

        @Override
        public long getLong(int col) {
            return rnd.nextLong();
        }

        @Override
        public long getRowId() {
            return 0;
        }

        @Override
        public short getShort(int col) {
            return rnd.nextShort();
        }

        @Override
        public CharSequence getStrA(int col) {
            return null;
        }

        @Override
        public CharSequence getStrB(int col) {
            return null;
        }

        @Override
        public int getStrLen(int col) {
            return 0;
        }

        @Override
        public CharSequence getSymA(int col) {
            return null;
        }

        @Override
        public CharSequence getSymB(int col) {
            return null;
        }

        @Override
        public long getTimestamp(int col) {
            return rnd.nextLong();
        }
    }

    /**
     * Mock RecordSinkSPI implementation that discards all writes.
     * This isolates the sink logic from actual I/O.
     */
    private static class MockRecordSinkSPI implements RecordSinkSPI {
        @Override
        public void putArray(ArrayView array) {
        }

        @Override
        public void putBin(BinarySequence binarySequence) {
        }

        @Override
        public void putBool(boolean value) {
        }

        @Override
        public void putByte(byte value) {
        }

        @Override
        public void putChar(char value) {
        }

        @Override
        public void putDate(long value) {
        }

        @Override
        public void putDecimal128(Decimal128 value) {
        }

        @Override
        public void putDecimal256(Decimal256 value) {
        }

        @Override
        public void putDouble(double value) {
        }

        @Override
        public void putFloat(float value) {
        }

        @Override
        public void putIPv4(int value) {
        }

        @Override
        public void putInt(int value) {
        }

        @Override
        public void putInterval(Interval interval) {
        }

        @Override
        public void putLong(long value) {
        }

        @Override
        public void putLong128(long lo, long hi) {
        }

        @Override
        public void putLong256(Long256 value) {
        }

        @Override
        public void putLong256(long l0, long l1, long l2, long l3) {
        }

        @Override
        public void putRecord(Record value) {
        }

        @Override
        public void putShort(short value) {
        }

        @Override
        public void putStr(CharSequence value) {
        }

        @Override
        public void putStr(CharSequence value, int lo, int hi) {
        }

        @Override
        public void putTimestamp(long value) {
        }

        @Override
        public void putVarchar(CharSequence value) {
        }

        @Override
        public void putVarchar(Utf8Sequence value) {
        }

        @Override
        public void skip(int bytes) {
        }
    }
}
