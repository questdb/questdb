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

package io.questdb.test.fuzz;

import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.GeoHashes;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.TableWriterAPI;
import io.questdb.cairo.arr.ArrayView;
import io.questdb.cairo.arr.DirectArray;
import io.questdb.std.BinarySequence;
import io.questdb.std.CarrierLocal;
import io.questdb.std.Chars;
import io.questdb.std.Hash;
import io.questdb.std.IntHashSet;
import io.questdb.std.Long256;
import io.questdb.std.Long256Impl;
import io.questdb.std.LongList;
import io.questdb.std.Numbers;
import io.questdb.std.Rnd;
import io.questdb.std.Uuid;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf8Sequence;
import io.questdb.std.str.Utf8StringSink;
import io.questdb.std.str.Utf8s;
import io.questdb.test.AbstractTest;
import io.questdb.test.cairo.TestRecord;
import io.questdb.test.griffin.RowAsserter;
import org.jetbrains.annotations.NotNull;

/**
 * Writes the row of an insert operation again, with the values of the original row in the dedup key columns.
 * Unless the copy is identical, it changes the value of one other column that the row writes, or of all of them,
 * which it picks at random for each copy. A changed value can be a NULL in place of a value, or the other way round.
 * <p>
 * {@link FuzzInsertOperation#apply} draws the column order, the not-set and NULL decisions and the column values of
 * a row from one {@link Rnd}, so the copy makes the same draws to repeat the dedup key values. For a changed column,
 * the copy draws the value of the original row too, and a {@link ChangedValueRow} writes a different value in its
 * place, drawn from a {@link Rnd} of its own.
 * <p>
 * The draws also depend on the columns of the table, so the copy repeats the dedup keys only when the table has the
 * columns it had for the original row: a commit of copies has to follow the commit of the originals with no column
 * change in between.
 */
public class DuplicateFuzzInsertOperation extends FuzzInsertOperation {
    private static final CarrierLocal<ChangedValueRow> tlChangedValueRow = new CarrierLocal<>(ChangedValueRow::new);
    private final IntHashSet dedupKeyIndexes;
    private final boolean isIdentical;
    private int changedColumnCount;
    private boolean isChangingAllColumns;

    public DuplicateFuzzInsertOperation(FuzzInsertOperation insertOperation, IntHashSet dedupKeyIndexes, boolean isIdentical) {
        super(insertOperation);
        this.dedupKeyIndexes = dedupKeyIndexes;
        this.isIdentical = isIdentical;
    }

    @Override
    public boolean apply(Rnd rnd, CairoEngine engine, TableWriterAPI tableWriter, int virtualTimestampIndex, LongList excludedTsIntervals) {
        changedColumnCount = 0;
        return super.apply(rnd, engine, tableWriter, virtualTimestampIndex, excludedTsIntervals);
    }

    @Override
    protected void appendColumnValue(
            Rnd rnd,
            int type,
            TableWriter.Row row,
            int columnIndex,
            boolean isNull,
            Utf8StringSink utf8StringSink,
            TestRecord.ArrayBinarySequence binarySequence
    ) {
        if (isIdentical || (changedColumnCount > 0 && !isChangingAllColumns) || dedupKeyIndexes.contains(columnIndex)) {
            super.appendColumnValue(rnd, type, row, columnIndex, isNull, utf8StringSink, binarySequence);
            return;
        }

        final ChangedValueRow changedValueRow = tlChangedValueRow.get();
        if (changedColumnCount == 0) {
            // The state of the shared rnd seeds the changed values, so the copy writes the same row to the reference
            // table and to the dedup table, and takes no draw from the shared rnd
            changedValueRow.reset(rnd.getSeed0(), rnd.getSeed1());
            // For a copy that changes one column only, the check whether an UPSERT commit carries the values of the
            // partition has to find that one column
            isChangingAllColumns = changedValueRow.nextBoolean();
        }
        super.appendColumnValue(rnd, type, changedValueRow.of(row, type, this), columnIndex, isNull, utf8StringSink, binarySequence);
        if (changedValueRow.isChanged()) {
            changedColumnCount++;
        }
    }

    /**
     * Takes the value that {@link FuzzInsertOperation#appendColumnValue} writes for the original row and writes a
     * different one to the row of the copy. {@link RowAsserter} fails a put that this class does not override, so a
     * column type that FuzzInsertOperation starts to write fails the copies until this class handles it.
     */
    private static class ChangedValueRow extends RowAsserter {
        private static final int MAX_ARRAY_ATTEMPTS = 16;
        // TestUtils.assertEquals() compares FLOAT and DOUBLE values with a tolerance, a changed value stays clear of it
        private static final double MIN_DIFF = 0.001;
        private final TestRecord.ArrayBinarySequence binarySequence = new TestRecord.ArrayBinarySequence();
        private final Rnd rnd = new Rnd();
        private final StringSink stringSink = new StringSink();
        private final Utf8StringSink utf8StringSink = new Utf8StringSink();
        private DirectArray array;
        private int columnType;
        private boolean isChanged;
        private FuzzInsertOperation operation;
        private TableWriter.Row row;

        public boolean isChanged() {
            return isChanged;
        }

        public boolean nextBoolean() {
            return rnd.nextBoolean();
        }

        public ChangedValueRow of(TableWriter.Row row, int columnType, FuzzInsertOperation operation) {
            this.row = row;
            this.columnType = columnType;
            this.operation = operation;
            this.isChanged = true;
            return this;
        }

        @Override
        public void putArray(int columnIndex, @NotNull ArrayView value) {
            if (array == null) {
                array = new DirectArray();
                AbstractTest.CLOSEABLE.add(array);
            }
            if (value.isNull() || !isNextNull()) {
                // FuzzInsertOperation fills arrays with NaN, so another array has another shape
                int attempts = 0;
                do {
                    rnd.nextDoubleArray(ColumnType.decodeArrayDimensionality(columnType), array, 1, 8, 0);
                } while (array.arrayEquals(value) && ++attempts < MAX_ARRAY_ATTEMPTS);
                if (array.arrayEquals(value)) {
                    // the value is not NULL here, so a NULL array differs from it
                    array.ofNull();
                }
            } else {
                array.ofNull();
            }
            row.putArray(columnIndex, array);
        }

        @Override
        public void putBin(int columnIndex, BinarySequence value) {
            if (value != null && isNextNull()) {
                row.putBin(columnIndex, null);
                return;
            }

            final int len = value != null ? (int) value.length() : 0;
            byte[] bytes;
            if (len > 0 && rnd.nextBoolean()) {
                // the same length with one other byte
                bytes = new byte[len];
                for (int i = 0; i < len; i++) {
                    bytes[i] = value.byteAt(i);
                }
                final int changedIndex = rnd.nextInt(len);
                bytes[changedIndex] = (byte) (bytes[changedIndex] + 1 + rnd.nextInt(255));
            } else {
                bytes = rnd.nextBytes(nextVarSize());
                if (value != null && isSameBytes(bytes, value)) {
                    bytes = rnd.nextBytes(bytes.length + 1);
                }
            }
            row.putBin(columnIndex, binarySequence.of(bytes));
        }

        @Override
        public void putBool(int columnIndex, boolean value) {
            row.putBool(columnIndex, !value);
        }

        @Override
        public void putByte(int columnIndex, byte value) {
            row.putByte(columnIndex, (byte) (value + 1 + rnd.nextInt(255)));
        }

        @Override
        public void putChar(int columnIndex, char value) {
            row.putChar(columnIndex, (char) (value + 1 + rnd.nextInt(65_535)));
        }

        @Override
        public void putDate(int columnIndex, long value) {
            row.putDate(columnIndex, nextLong(value));
        }

        @Override
        public void putDouble(int columnIndex, double value) {
            double changed = Double.NaN;
            if (Double.isNaN(value) || !isNextNull()) {
                do {
                    changed = rnd.nextDouble();
                } while (Math.abs(changed - value) < MIN_DIFF);
            }
            row.putDouble(columnIndex, changed);
        }

        @Override
        public void putFloat(int columnIndex, float value) {
            float changed = Float.NaN;
            if (Float.isNaN(value) || !isNextNull()) {
                do {
                    changed = rnd.nextFloat();
                } while (Math.abs(changed - value) < MIN_DIFF);
            }
            row.putFloat(columnIndex, changed);
        }

        @Override
        public void putGeoHash(int columnIndex, long value) {
            // the column keeps the low bytes of the value, see WriterRowUtils.putGeoHash()
            final long stored = narrowGeoHash(value);
            long changed = GeoHashes.NULL;
            if (stored == GeoHashes.NULL || !isNextNull()) {
                do {
                    changed = rnd.nextLong();
                } while (narrowGeoHash(changed) == stored || narrowGeoHash(changed) == GeoHashes.NULL);
            }
            row.putGeoHash(columnIndex, changed);
        }

        @Override
        public void putInt(int columnIndex, int value) {
            final int nullValue = ColumnType.tagOf(columnType) == ColumnType.IPv4 ? Numbers.IPv4_NULL : Numbers.INT_NULL;
            int changed = nullValue;
            if (value == nullValue || !isNextNull()) {
                do {
                    changed = rnd.nextInt();
                } while (changed == value || changed == nullValue);
            }
            row.putInt(columnIndex, changed);
        }

        @Override
        public void putLong(int columnIndex, long value) {
            row.putLong(columnIndex, nextLong(value));
        }

        @Override
        public void putLong128(int columnIndex, long lo, long hi) {
            // LONG128 and UUID share the NULL value
            long changedLo = Numbers.LONG_NULL;
            long changedHi = Numbers.LONG_NULL;
            if (Uuid.isNull(lo, hi) || !isNextNull()) {
                do {
                    changedLo = rnd.nextLong();
                    changedHi = rnd.nextLong();
                } while ((changedLo == lo && changedHi == hi) || Uuid.isNull(changedLo, changedHi));
            }
            row.putLong128(columnIndex, changedLo, changedHi);
        }

        @Override
        public void putLong256(int columnIndex, long l0, long l1, long l2, long l3) {
            putChangedLong256(columnIndex, l0, l1, l2, l3);
        }

        @Override
        public void putLong256(int columnIndex, Long256 value) {
            putChangedLong256(columnIndex, value.getLong0(), value.getLong1(), value.getLong2(), value.getLong3());
        }

        @Override
        public void putShort(int columnIndex, short value) {
            row.putShort(columnIndex, (short) (value + 1 + rnd.nextInt(65_535)));
        }

        @Override
        public void putStr(int columnIndex, CharSequence value) {
            if (value != null && isNextNull()) {
                row.putStr(columnIndex, null);
                return;
            }

            stringSink.clear();
            final int len = value != null ? value.length() : 0;
            if (len > 0 && rnd.nextBoolean()) {
                // the same length with one other char
                stringSink.put(value);
                final int changedIndex = rnd.nextInt(len);
                stringSink.setCharAt(changedIndex, nextOtherChar(value.charAt(changedIndex)));
            } else {
                rnd.nextChars(stringSink, nextVarSize());
                if (value != null && Chars.equals(stringSink, value)) {
                    stringSink.put(rnd.nextChar());
                }
            }
            row.putStr(columnIndex, stringSink);
        }

        @Override
        public void putSym(int columnIndex, CharSequence value) {
            if (value != null && isNextNull()) {
                row.putSym(columnIndex, null);
                return;
            }

            // another of the symbols that FuzzInsertOperation draws from
            final String[] symbols = operation.getSymbols();
            final int symbolCount = symbols != null ? symbols.length : 0;
            final int start = symbolCount > 0 ? rnd.nextInt(symbolCount) : 0;
            for (int i = 0; i < symbolCount; i++) {
                final String symbol = symbols[(start + i) % symbolCount];
                if (!Chars.equalsNc(symbol, value)) {
                    row.putSym(columnIndex, symbol);
                    return;
                }
            }
            // with no other symbol, a NULL in place of a value is the change, and a NULL stays as it is
            isChanged = value != null;
            row.putSym(columnIndex, null);
        }

        @Override
        public void putTimestamp(int columnIndex, long value) {
            row.putTimestamp(columnIndex, nextLong(value));
        }

        @Override
        public void putVarchar(int columnIndex, Utf8Sequence value) {
            if (value != null && isNextNull()) {
                row.putVarchar(columnIndex, null);
                return;
            }

            utf8StringSink.clear();
            final int size = value != null ? value.size() : 0;
            if (size > 0 && rnd.nextBoolean()) {
                // The same size with another last byte. The last byte of valid UTF-8 is either ASCII or a
                // continuation byte, and it stays one, so the value stays valid UTF-8.
                for (int i = 0; i < size - 1; i++) {
                    utf8StringSink.putAny(value.byteAt(i));
                }
                final byte last = value.byteAt(size - 1);
                if (last >= 0) {
                    // a printable ASCII char, as Rnd.nextUtf8Str() draws them
                    utf8StringSink.putAny((byte) (32 + (last - 32 + 1 + rnd.nextInt(94)) % 95));
                } else {
                    utf8StringSink.putAny((byte) (0x80 | ((last & 0x3f) + 1 + rnd.nextInt(63)) % 64));
                }
            } else {
                rnd.nextUtf8Str(nextVarSize(), utf8StringSink);
                if (value != null && Utf8s.equals(utf8StringSink, value)) {
                    utf8StringSink.putAscii(rnd.nextChar());
                }
            }
            row.putVarchar(columnIndex, utf8StringSink);
        }

        public void reset(long seed0, long seed1) {
            // mixes the state of the shared rnd, so the changed values do not repeat its draws
            rnd.reset(Hash.murmur3ToLong(seed0), Hash.murmur3ToLong(seed1));
        }

        private static boolean isSameBytes(byte[] bytes, BinarySequence value) {
            if (bytes.length != value.length()) {
                return false;
            }
            for (int i = 0; i < bytes.length; i++) {
                if (bytes[i] != value.byteAt(i)) {
                    return false;
                }
            }
            return true;
        }

        // a NULL in place of a value as often as FuzzInsertOperation writes a NULL
        private boolean isNextNull() {
            return rnd.nextDouble() < operation.getNullSet();
        }

        private long narrowGeoHash(long value) {
            return switch (ColumnType.tagOf(columnType)) {
                case ColumnType.GEOBYTE -> (byte) value;
                case ColumnType.GEOSHORT -> (short) value;
                case ColumnType.GEOINT -> (int) value;
                default -> value;
            };
        }

        private long nextLong(long value) {
            long changed = Numbers.LONG_NULL;
            if (value == Numbers.LONG_NULL || !isNextNull()) {
                do {
                    changed = rnd.nextLong();
                } while (changed == value || changed == Numbers.LONG_NULL);
            }
            return changed;
        }

        // a char of the 'B'..'Z' range of Rnd.nextChar() other than the given one
        private char nextOtherChar(char c) {
            final char changed = rnd.nextChar();
            return changed != c ? changed : (char) (changed == 'Z' ? 'B' : changed + 1);
        }

        // the size of a variable size value, as FuzzInsertOperation draws it
        private int nextVarSize() {
            final int strLen = operation.getStrLen();
            return strLen > 0 ? (int) Math.pow(strLen, rnd.nextDouble()) : 0;
        }

        private void putChangedLong256(int columnIndex, long l0, long l1, long l2, long l3) {
            if (!Long256Impl.isNull(l0, l1, l2, l3) && isNextNull()) {
                row.putLong256(columnIndex, Long256Impl.NULL_LONG256);
                return;
            }

            long changed0, changed1, changed2, changed3;
            do {
                changed0 = rnd.nextLong();
                changed1 = rnd.nextLong();
                changed2 = rnd.nextLong();
                changed3 = rnd.nextLong();
            } while ((changed0 == l0 && changed1 == l1 && changed2 == l2 && changed3 == l3)
                    || Long256Impl.isNull(changed0, changed1, changed2, changed3));
            row.putLong256(columnIndex, changed0, changed1, changed2, changed3);
        }
    }
}
