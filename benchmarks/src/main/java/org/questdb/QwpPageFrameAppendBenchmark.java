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

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.idx.IndexReader;
import io.questdb.cairo.sql.PageFrame;
import io.questdb.cairo.sql.PartitionFormat;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.cutlass.qwp.codec.QwpEgressColumnDef;
import io.questdb.cutlass.qwp.codec.QwpEgressConnSymbolDict;
import io.questdb.cutlass.qwp.codec.QwpResultBatchBuffer;
import io.questdb.std.MemoryTag;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.Rnd;
import io.questdb.std.Unsafe;
import io.questdb.std.str.StringSink;
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
 * Cost of appending one page frame to a QWP egress batch, forward and backward, for each
 * column type with a columnar kernel. The frame holds 65,536 rows of one column, about
 * one in ten NULL where the type has NULL, and the benchmark appends it in the 1,024-row
 * slices the egress streaming loop uses: forward from the first slice up, backward from
 * the last slice down with each slice reversed, as a descending scan streams it.
 * <p>
 * Build (note {@code -am} so the benchmark links the in-tree core) and run:
 * <pre>
 * mvn -pl benchmarks -am package -DskipTests
 * java --sun-misc-unsafe-memory-access=allow --enable-native-access=ALL-UNNAMED \
 *      -cp benchmarks/target/benchmarks.jar org.questdb.QwpPageFrameAppendBenchmark
 * </pre>
 */
@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 3, time = 2)
@Measurement(iterations = 5, time = 2)
@Fork(value = 1, jvmArgsAppend = {
        "--sun-misc-unsafe-memory-access=allow",
        "--enable-native-access=ALL-UNNAMED",
        "--add-exports=java.base/jdk.internal.vm=ALL-UNNAMED",
        "--add-opens=java.base/java.lang=ALL-UNNAMED"
})
public class QwpPageFrameAppendBenchmark {

    private static final int ROWS = 65_536;
    private static final int SLICE = 1_024;
    private final ObjList<QwpEgressColumnDef> defs = new ObjList<>();
    private final SymbolTableSource symbolTables = new KeySymbolTableSource();
    private QwpResultBatchBuffer buffer;
    private QwpEgressConnSymbolDict dict;
    private OneColumnFrame frame;
    @Param({"LONG", "TIMESTAMP", "DOUBLE", "INT", "FLOAT", "SHORT", "BYTE", "BOOLEAN", "SYMBOL"})
    public String type;

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(QwpPageFrameAppendBenchmark.class.getSimpleName())
                .build();
        new Runner(opt).run();
    }

    @Benchmark
    public int backward() {
        buffer.beginBatch(defs, symbolTables, dict);
        for (int hi = ROWS; hi > 0; hi -= SLICE) {
            buffer.appendPageFrame(frame, null, hi - SLICE, hi, true);
        }
        return buffer.getRowCount();
    }

    @Benchmark
    public int forward() {
        buffer.beginBatch(defs, symbolTables, dict);
        for (int lo = 0; lo < ROWS; lo += SLICE) {
            buffer.appendPageFrame(frame, null, lo, lo + SLICE, false);
        }
        return buffer.getRowCount();
    }

    @Setup(Level.Trial)
    public void setUp() {
        final int columnType = switch (type) {
            case "LONG" -> ColumnType.LONG;
            case "TIMESTAMP" -> ColumnType.TIMESTAMP;
            case "DOUBLE" -> ColumnType.DOUBLE;
            case "INT" -> ColumnType.INT;
            case "FLOAT" -> ColumnType.FLOAT;
            case "SHORT" -> ColumnType.SHORT;
            case "BYTE" -> ColumnType.BYTE;
            case "BOOLEAN" -> ColumnType.BOOLEAN;
            case "SYMBOL" -> ColumnType.SYMBOL;
            default -> throw new IllegalArgumentException(type);
        };
        final QwpEgressColumnDef def = new QwpEgressColumnDef();
        def.of("c", columnType);
        defs.add(def);
        buffer = new QwpResultBatchBuffer();
        dict = new QwpEgressConnSymbolDict();
        frame = new OneColumnFrame(ColumnType.sizeOf(columnType));
        final Rnd rnd = new Rnd();
        final long base = frame.address;
        for (int r = 0; r < ROWS; r++) {
            final boolean isNull = rnd.nextInt(10) == 0;
            switch (ColumnType.tagOf(columnType)) {
                case ColumnType.LONG, ColumnType.TIMESTAMP ->
                        Unsafe.putLong(base + 8L * r, isNull ? Numbers.LONG_NULL : rnd.nextLong());
                case ColumnType.DOUBLE -> Unsafe.putDouble(base + 8L * r, isNull ? Double.NaN : rnd.nextDouble());
                case ColumnType.INT -> Unsafe.putInt(base + 4L * r, isNull ? Numbers.INT_NULL : rnd.nextInt());
                case ColumnType.FLOAT -> Unsafe.putFloat(base + 4L * r, isNull ? Float.NaN : rnd.nextFloat());
                case ColumnType.SHORT -> Unsafe.putShort(base + 2L * r, rnd.nextShort());
                case ColumnType.BYTE -> Unsafe.putByte(base + r, rnd.nextByte());
                case ColumnType.BOOLEAN -> Unsafe.putByte(base + r, (byte) (rnd.nextBoolean() ? 1 : 0));
                // 64 distinct keys: the dictionary fills on the first pass, later passes hit its cache
                case ColumnType.SYMBOL ->
                        Unsafe.putInt(base + 4L * r, isNull ? SymbolTable.VALUE_IS_NULL : rnd.nextInt(64));
                default -> throw new IllegalArgumentException(type);
            }
        }
    }

    @TearDown(Level.Trial)
    public void tearDown() {
        buffer.close();
        dict.close();
        frame.close();
    }

    private static final class KeySymbolTable implements SymbolTable {
        private final StringSink sink = new StringSink();

        @Override
        public boolean supportsKeyValueAccess() {
            return true;
        }

        @Override
        public CharSequence valueBOf(int key) {
            return valueOf(key);
        }

        @Override
        public CharSequence valueOf(int key) {
            sink.clear();
            sink.put("symbol_").put(key);
            return sink;
        }
    }

    private static final class KeySymbolTableSource implements SymbolTableSource {
        private final KeySymbolTable table = new KeySymbolTable();

        @Override
        public SymbolTable getSymbolTable(int columnIndex) {
            return table;
        }

        @Override
        public SymbolTable newSymbolTable(int columnIndex) {
            return new KeySymbolTable();
        }
    }

    private static final class OneColumnFrame implements PageFrame {
        private final long address;
        private final long size;

        OneColumnFrame(int valueSize) {
            size = (long) ROWS * valueSize;
            address = Unsafe.malloc(size, MemoryTag.NATIVE_DEFAULT);
        }

        @Override
        public long getAuxPageAddress(int columnIndex) {
            return 0;
        }

        @Override
        public long getAuxPageSize(int columnIndex) {
            return 0;
        }

        @Override
        public int getColumnCount() {
            return 1;
        }

        @Override
        public byte getFormat() {
            return PartitionFormat.NATIVE;
        }

        @Override
        public IndexReader getIndexReader(int columnIndex, int direction) {
            throw new UnsupportedOperationException();
        }

        @Override
        public long getPageAddress(int columnIndex) {
            return address;
        }

        @Override
        public long getPageSize(int columnIndex) {
            return size;
        }

        @Override
        public int getParquetRowGroup() {
            return -1;
        }

        @Override
        public int getParquetRowGroupHi() {
            return -1;
        }

        @Override
        public int getParquetRowGroupLo() {
            return -1;
        }

        @Override
        public long getPartitionHi() {
            return ROWS;
        }

        @Override
        public int getPartitionIndex() {
            return 0;
        }

        @Override
        public long getPartitionLo() {
            return 0;
        }

        void close() {
            Unsafe.free(address, size, MemoryTag.NATIVE_DEFAULT);
        }
    }
}
