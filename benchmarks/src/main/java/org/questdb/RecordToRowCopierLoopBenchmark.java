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
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.DefaultCairoConfiguration;
import io.questdb.cairo.EntityColumnFilter;
import io.questdb.cairo.GenericRecordMetadata;
import io.questdb.cairo.TableColumnMetadata;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.arr.ArrayView;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.LoopingRecordToRowCopier;
import io.questdb.griffin.RecordToRowCopier;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.std.BinarySequence;
import io.questdb.std.Decimal256;
import io.questdb.std.Files;
import io.questdb.std.Long256;
import io.questdb.std.Rnd;
import io.questdb.std.str.DirectUtf8Sequence;
import io.questdb.std.str.Path;
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

import java.util.concurrent.TimeUnit;

/**
 * Throughput of the looping copier, {@link LoopingRecordToRowCopier}, the arm INSERT AS SELECT
 * takes above the bytecode size limit. The LOOP leg of {@link RecordToRowCopierBenchmark} cannot
 * run: its mock metadata has no column metadata, which the looping copier copies. This benchmark
 * gives the copier real column metadata and otherwise measures the same way: a mock record that
 * returns random values and a mock row that discards them, so only the copier's per-column
 * dispatch and the getter and putter calls are timed.
 * <p>
 * {@code types=SIMPLE} draws columns from {@link RecordToRowCopierBenchmark}'s eight types;
 * {@code types=MIXED} adds DATE, CHAR, IPv4, the four geohash widths and UUID, so more copier arms
 * run per row.
 * <p>
 * Run from the command line:
 * <pre>
 * mvn clean package -DskipTests -pl benchmarks -am
 * java -jar benchmarks/target/benchmarks.jar RecordToRowCopierLoopBenchmark
 * </pre>
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(value = 1, jvmArgs = {"-Xms2G", "-Xmx2G"})
public class RecordToRowCopierLoopBenchmark {
    private static final int BATCH_SIZE = 10_000;
    private static final int[] MIXED_TYPES = {
            ColumnType.INT,
            ColumnType.LONG,
            ColumnType.DOUBLE,
            ColumnType.FLOAT,
            ColumnType.SHORT,
            ColumnType.BYTE,
            ColumnType.BOOLEAN,
            ColumnType.TIMESTAMP,
            ColumnType.DATE,
            ColumnType.CHAR,
            ColumnType.IPv4,
            ColumnType.getGeoHashTypeWithBits(5),
            ColumnType.getGeoHashTypeWithBits(10),
            ColumnType.getGeoHashTypeWithBits(20),
            ColumnType.getGeoHashTypeWithBits(40),
            ColumnType.UUID
    };
    private static final int RANDOM_SEED = 42;
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
    @Param({"10", "100", "1000", "6000"})
    private int columnCount;
    private SqlExecutionContext context;
    private RecordToRowCopier copier;
    private CairoEngine engine;
    private MockRecord record;
    private MockRow row;
    private String tempDbRoot;
    @Param({"SIMPLE", "MIXED"})
    private String types;

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(RecordToRowCopierLoopBenchmark.class.getSimpleName())
                .forks(1)
                .jvmArgs("-Xms2G", "-Xmx2G")
                .build();
        new Runner(opt).run();
    }

    @Benchmark
    public void benchmarkBatchCopy(Blackhole bh) {
        for (int i = 0; i < BATCH_SIZE; i++) {
            copier.copy(context, record, row);
        }
        bh.consume(row);
    }

    @Setup(Level.Trial)
    public void setup() throws Exception {
        tempDbRoot = java.nio.file.Files.createTempDirectory("questdb-bench-").toString();
        final CairoConfiguration configuration = new DefaultCairoConfiguration(tempDbRoot);
        engine = new CairoEngine(configuration);
        context = new SqlExecutionContextImpl(engine, 1).with(
                configuration.getFactoryProvider().getSecurityContextFactory().getRootContext(),
                null,
                null,
                -1,
                null
        );

        final Rnd rnd = new Rnd(RANDOM_SEED, RANDOM_SEED);
        final int[] pool = "MIXED".equals(types) ? MIXED_TYPES : SIMPLE_TYPES;
        // the source and the target have the same types, so every column takes its type's same-type arm
        final GenericRecordMetadata metadata = new GenericRecordMetadata();
        for (int i = 0; i < columnCount; i++) {
            metadata.add(new TableColumnMetadata("col" + i, pool[rnd.nextInt(pool.length)]));
        }
        final EntityColumnFilter columnFilter = new EntityColumnFilter();
        columnFilter.of(columnCount);
        copier = new LoopingRecordToRowCopier(metadata, metadata, columnFilter);
        record = new MockRecord(rnd);
        row = new MockRow();
    }

    @TearDown(Level.Trial)
    public void tearDown() {
        if (engine != null) {
            engine.close();
        }
        if (tempDbRoot != null) {
            try (Path path = new Path()) {
                Files.rmdir(path.of(tempDbRoot), true);
            }
        }
    }

    /**
     * Returns a random value from every getter the copier calls.
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
        public byte getGeoByte(int col) {
            return rnd.nextByte();
        }

        @Override
        public int getGeoInt(int col) {
            return rnd.nextInt();
        }

        @Override
        public long getGeoLong(int col) {
            return rnd.nextLong();
        }

        @Override
        public short getGeoShort(int col) {
            return rnd.nextShort();
        }

        @Override
        public int getIPv4(int col) {
            return rnd.nextInt();
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
        public long getLong128Hi(int col) {
            return rnd.nextLong();
        }

        @Override
        public long getLong128Lo(int col) {
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
        public long getTimestamp(int col) {
            return rnd.nextLong();
        }
    }

    /**
     * Discards every write, so the copier is measured without I/O.
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
        public void putDecimalChar(int columnIndex, char decimalValue) {
        }

        @Override
        public void putDecimalStr(int columnIndex, CharSequence decimalValue) {
        }

        @Override
        public void putDecimalVarchar(int columnIndex, Utf8Sequence decimalValue) {
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
        public void putLong256Utf8(int columnIndex, Utf8Sequence hexString) {
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
        public void putStrUtf8(int columnIndex, DirectUtf8Sequence value) {
        }

        @Override
        public void putStrUtf8(int columnIndex, Utf8Sequence value) {
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
