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

import io.questdb.cairo.TableWriter;
import io.questdb.cairo.arr.ArrayView;
import io.questdb.cutlass.text.types.CharAdapter;
import io.questdb.std.BinarySequence;
import io.questdb.std.Decimal256;
import io.questdb.std.Long256;
import io.questdb.std.Misc;
import io.questdb.std.str.DirectUtf8Sequence;
import io.questdb.std.str.DirectUtf8Sink;
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
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.concurrent.TimeUnit;

/**
 * Per-value cost of a CSV import writing a CHAR column through {@link CharAdapter}: an ASCII
 * letter, a two-byte UTF-8 character and the NULL keyword. The adapter decodes the value's UTF-8
 * to one UTF-16 character; a mock row takes the character, so the table writer stays out of the
 * measurement.
 */
@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 3)
@Measurement(iterations = 5)
@Fork(1)
public class CharAdapterBenchmark {
    @Param({"ASCII", "TWO_BYTE", "NULL"})
    public String value;
    private final MockRow row = new MockRow();
    private DirectUtf8Sink text;

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(CharAdapterBenchmark.class.getSimpleName())
                .build();
        new Runner(opt).run();
    }

    @Setup(Level.Trial)
    public void setUp() {
        text = new DirectUtf8Sink(16);
        switch (value) {
            case "ASCII" -> text.put('a');
            // U+00E9, two bytes in UTF-8
            case "TWO_BYTE" -> text.put("\u00e9");
            case "NULL" -> text.put("null");
            default -> throw new IllegalArgumentException("unknown value: " + value);
        }
    }

    @TearDown(Level.Trial)
    public void tearDown() {
        text = Misc.free(text);
    }

    @Benchmark
    public char write() {
        CharAdapter.INSTANCE.write(row, 0, text);
        return row.value;
    }

    /**
     * Keeps the character the adapter writes and discards every other write.
     */
    private static class MockRow implements TableWriter.Row {
        private char value;

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
            this.value = value;
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
