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
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.ColumnTypes;
import io.questdb.cairo.DefaultCairoConfiguration;
import io.questdb.cairo.EntityColumnFilter;
import io.questdb.cairo.TableColumnMetadata;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.arr.ArrayView;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.griffin.LoopingRecordToRowCopier;
import io.questdb.griffin.RecordToRowCopier;
import io.questdb.griffin.RecordToRowCopierUtils;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.std.BinarySequence;
import io.questdb.std.BytecodeAssembler;
import io.questdb.std.Decimal256;
import io.questdb.std.Long256;
import io.questdb.std.Rnd;
import io.questdb.std.str.DirectUtf8Sequence;
import io.questdb.std.str.Utf8Sequence;
import org.jetbrains.annotations.NotNull;
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
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.concurrent.TimeUnit;

/**
 * Comprehensive benchmark comparing bytecode-generated vs loop-based RecordToRowCopier implementations.
 * <p>
 * This benchmark measures:
 * 1. Compile time - how long it takes to create the copier
 * 2. Copy execution - time to copy records once copier is created
 * 3. Throughput - records copied per second
 * <p>
 * The benchmark uses mock Record and Row implementations to isolate the copier
 * performance from I/O overhead.
 * <p>
 * For forks(1) builds:
 * mvn clean package -DskipTests -pl benchmarks -am 2>&1 | tail -3
 * java -jar benchmarks/target/benchmarks.jar RecordToRowCopierBenchmark 2>&1 | tee /tmp/benchmark_fallback_test.txt
 * <p>
 * Otherwise, run in IntelliJ.
 * <p>
 * forks(1) is necessary if investigating inlining/C2 compilation behaviour:
 * .forks(1)
 * .jvmArgs("-Xms2G", "-Xmx2G", "-XX:+PrintCompilation", "-XX:+UnlockDiagnosticVMOptions", "-XX:+PrintInlining")
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 3, time = 1)
@Fork(value = 1, jvmArgs = {"-Xms2G", "-Xmx2G"})
public class RecordToRowCopierBenchmark {

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

    @Param({"10", "50", "100", "200", "459", "460", "500", "1000", "2000", "3000", "4000", "5000", "6000"})
    //    @Param({"450", "451", "452", "453", "454", "455", "456", "457", "458", "459", "500"})
//    @Param({"459", "460"})
    private int columnCount;

    private SqlExecutionContext context;
    // Pre-created objects for execution benchmarks
    private RecordToRowCopier copier;
    // Engine resources
    private CairoEngine engine;
    @Param({"BYTECODE", "CHUNKED", "LOOP"})
    private String implementation;
    private MockRecord record;
    private MockRow row;
    private String tempDbRoot;

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(RecordToRowCopierBenchmark.class.getSimpleName())
                // Use fork(1) to get diagnostic output from @Fork jvmArgs
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
            copier.copy(context, record, row);
        }
        bh.consume(row);
    }

    /*
     * Measures the time to create a copier (compile time).
     * For bytecode implementation, this includes bytecode generation.
     * For loop implementation, this is just constructor time.
     */
//    @Benchmark
//    @BenchmarkMode(Mode.AverageTime)
//    @OutputTimeUnit(TimeUnit.MICROSECONDS)
//    public RecordToRowCopier benchmarkCompileTime() {
//        if ("BYTECODE".equals(implementation)) {
//            return RecordToRowCopierUtils.generateCopier(
//                    new BytecodeAssembler(), fromTypes, toMetadata, columnFilter, Integer.MAX_VALUE);
//        } else {
//            return new LoopingRecordToRowCopier(fromTypes, toMetadata, columnFilter);
//        }
//    }

    /**
     * Measures the time to copy a single record.
     */
//    @Benchmark
//    @BenchmarkMode(Mode.AverageTime)
//    @OutputTimeUnit(TimeUnit.NANOSECONDS)
//    public void benchmarkSingleCopy(Blackhole bh) {
//        copier.copy(context, record, row);
//        bh.consume(row);
//    }
    @Setup(Level.Trial)
    public void setup() throws IOException {
        // Create temporary directory for engine
        File tempDir = File.createTempFile("questdb-bench-", "");
        tempDir.delete();
        tempDir.mkdir();
        tempDbRoot = tempDir.getAbsolutePath();

        CairoConfiguration configuration = new DefaultCairoConfiguration(tempDbRoot);
        engine = new CairoEngine(configuration);

        // Create execution context from engine
        context = new SqlExecutionContextImpl(engine, 1)
                .with(
                        configuration.getFactoryProvider().getSecurityContextFactory().getRootContext(),
                        null,
                        null,
                        -1,
                        null
                );

        Rnd rnd = new Rnd(RANDOM_SEED, RANDOM_SEED);

        // Generate random column types (deterministic with fixed seed)
        int[] columnTypes = new int[columnCount];
        for (int i = 0; i < columnCount; i++) {
            columnTypes[i] = SIMPLE_TYPES[rnd.nextInt(SIMPLE_TYPES.length)];
        }

        // Create mock metadata
        // For compile benchmark
        MockColumnTypes fromTypes = new MockColumnTypes(columnTypes);
        MockRecordMetadata toMetadata = new MockRecordMetadata(columnTypes);
        EntityColumnFilter columnFilter = new EntityColumnFilter();
        columnFilter.of(columnCount);

        // Create copier based on implementation type
        if ("BYTECODE".equals(implementation)) {
            // Use the default method size limit (8000 bytes) which will auto-fallback to loop
            copier = RecordToRowCopierUtils.generateCopier(
                    new BytecodeAssembler(),
                    fromTypes,
                    toMetadata,
                    columnFilter,
                    RecordToRowCopierUtils.DEFAULT_HUGE_METHOD_LIMIT,
                    new DefaultCairoConfiguration(tempDbRoot) {
                        @Override
                        public boolean isCopierChunkedEnabled() {
                            return false;
                        }
                    }
            );
        } else if ("CHUNKED".equals(implementation)) {
            // For proper chunking test, we need:
            // 1. estimatedSize > methodSizeLimit (to enter chunked path at line 170)
            // 2. individual chunk size <= methodSizeLimit (to pass validation at line 1784)
            //
            // With CHUNK_TARGET_SIZE=6000, chunks are ~6000 bytes max.
            // Use methodSizeLimit=6500 to allow chunks but force chunking for larger tables.
            // NOTE: For small column counts (<300 cols, ~6000 bytes), this will use single-method
            // (identical to BYTECODE) since estimatedSize <= 6500 or only 1 chunk is created.
            copier = RecordToRowCopierUtils.generateCopier(
                    new BytecodeAssembler(), fromTypes, toMetadata, columnFilter,
                    RecordToRowCopierUtils.DEFAULT_HUGE_METHOD_LIMIT, configuration);
        } else {
            // Use loop implementation directly
            copier = new LoopingRecordToRowCopier(fromTypes, toMetadata, columnFilter);
        }

        // Create mock record and row
        record = new MockRecord(rnd);
        row = new MockRow();
    }

    @TearDown(Level.Trial)
    public void tearDown() throws Exception {
        if (engine != null) {
            engine.close();
        }

        // Clean up temporary directory
        if (tempDbRoot != null) {
            java.nio.file.Files.walkFileTree(java.nio.file.Paths.get(tempDbRoot), new SimpleFileVisitor<>() {
                @Override
                public @NotNull FileVisitResult postVisitDirectory(@NotNull Path dir, IOException exc) throws IOException {
                    java.nio.file.Files.delete(dir);
                    return FileVisitResult.CONTINUE;
                }

                @Override
                public @NotNull FileVisitResult visitFile(@NotNull Path file, @NotNull BasicFileAttributes attrs) throws IOException {
                    java.nio.file.Files.delete(file);
                    return FileVisitResult.CONTINUE;
                }
            });
        }
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
     * Mock RecordMetadata implementation for benchmark.
     */
    private record MockRecordMetadata(int[] types) implements RecordMetadata {

        @Override
        public int getColumnCount() {
            return types.length;
        }

        @Override
        public int getColumnIndexQuiet(CharSequence columnName, int lo, int hi) {
            return -1;
        }

        @Override
        public TableColumnMetadata getColumnMetadata(int columnIndex) {
            return null;
        }

        @Override
        public String getColumnName(int columnIndex) {
            return "col" + columnIndex;
        }

        @Override
        public int getColumnType(int columnIndex) {
            return types[columnIndex];
        }

        @Override
        public int getIndexValueBlockCapacity(int columnIndex) {
            return 0;
        }

        @Override
        public int getTimestampIndex() {
            return -1; // No timestamp column
        }

        @Override
        public int getWriterIndex(int columnIndex) {
            return columnIndex;
        }

        @Override
        public boolean hasColumn(int columnIndex) {
            return columnIndex >= 0 && columnIndex < types.length;
        }

        @Override
        public boolean isColumnIndexed(int columnIndex) {
            return false;
        }

        @Override
        public boolean isDedupKey(int columnIndex) {
            return false;
        }

        @Override
        public boolean isSymbolTableStatic(int columnIndex) {
            return false;
        }
    }

    /**
     * Mock Row implementation that discards all writes.
     * This isolates the copier logic from actual I/O.
     */
    private static class MockRow implements TableWriter.Row {
        @Override
        public void append() {
        }

        @Override
        public void cancel() {
        }

        @Override
        public void putArray(int columnIndex, @NotNull ArrayView array) {
        }

        @Override
        public void putBin(int columnIndex, long address, long len) {
        }

        @Override
        public void putBin(int columnIndex, BinarySequence sequence) {
        }

        @Override
        public void putLong256Utf8(int columnIndex, Utf8Sequence hexString) {
        }

        @Override
        public void putBool(int columnIndex, boolean value) {
        }

        @Override
        public void putByte(int columnIndex, byte value) {
        }

        @Override
        public void putChar(int columnIndex, char value) {
        }

        @Override
        public void putDate(int columnIndex, long value) {
        }

        @Override
        public void putDecimal(int columnIndex, Decimal256 value) {
        }

        @Override
        public void putDecimal128(int columnIndex, long high, long low) {
        }

        @Override
        public void putDecimal256(int columnIndex, long hh, long hl, long lh, long ll) {
        }

        @Override
        public void putDecimalStr(int columnIndex, CharSequence decimalValue) {
        }

        @Override
        public void putDouble(int columnIndex, double value) {
        }

        @Override
        public void putFloat(int columnIndex, float value) {
        }

        @Override
        public void putGeoHash(int columnIndex, long value) {
        }

        @Override
        public void putGeoHashDeg(int columnIndex, double lat, double lon) {
        }

        @Override
        public void putGeoStr(int columnIndex, CharSequence value) {
        }

        @Override
        public void putGeoVarchar(int columnIndex, Utf8Sequence value) {
        }

        @Override
        public void putIPv4(int columnIndex, int value) {
        }

        @Override
        public void putInt(int columnIndex, int value) {
        }

        @Override
        public void putLong(int columnIndex, long value) {
        }

        @Override
        public void putLong128(int columnIndex, long lo, long hi) {
        }

        @Override
        public void putLong256(int columnIndex, long l0, long l1, long l2, long l3) {
        }

        @Override
        public void putLong256(int columnIndex, Long256 value) {
        }

        @Override
        public void putLong256(int columnIndex, CharSequence hexString) {
        }

        @Override
        public void putLong256(int columnIndex, @NotNull CharSequence hexString, int start, int end) {
        }

        @Override
        public void putLong256Utf8(int columnIndex, DirectUtf8Sequence hexString) {
        }

        @Override
        public void putShort(int columnIndex, short value) {
        }

        @Override
        public void putStr(int columnIndex, CharSequence value) {
        }

        @Override
        public void putStr(int columnIndex, char value) {
        }

        @Override
        public void putStr(int columnIndex, CharSequence value, int pos, int len) {
        }

        @Override
        public void putStrUtf8(int columnIndex, Utf8Sequence value) {
        }

        @Override
        public void putStrUtf8(int columnIndex, DirectUtf8Sequence value) {
        }

        @Override
        public void putSym(int columnIndex, CharSequence value) {
        }

        @Override
        public void putSym(int columnIndex, char value) {
        }

        @Override
        public void putSymIndex(int columnIndex, int key) {
        }

        @Override
        public void putSymUtf8(int columnIndex, DirectUtf8Sequence value) {
        }

        @Override
        public void putTimestamp(int columnIndex, long value) {
        }

        @Override
        public void putUuid(int columnIndex, CharSequence uuid) {
        }

        @Override
        public void putUuidUtf8(int columnIndex, Utf8Sequence uuid) {
        }

        @Override
        public void putVarchar(int columnIndex, char value) {
        }

        @Override
        public void putVarchar(int columnIndex, Utf8Sequence value) {
        }
    }
}