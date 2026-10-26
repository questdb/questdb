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

package io.questdb.test.cairo;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.StringTypeDriver;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.VarcharTypeDriver;
import io.questdb.cairo.arr.ArrayTypeDriver;
import io.questdb.cairo.arr.DirectArray;
import io.questdb.cairo.frm.FrameAlgebra;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryCARW;
import io.questdb.std.BinarySequence;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import io.questdb.std.str.Utf8String;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

/**
 * {@link FrameAlgebra#isColumnMergeIdentical} lets an all-duplicate parquet dedup commit
 * skip the merge, so a false "identical" loses data. Any differing value (NULL vs value,
 * empty vs NULL, any byte) must report a difference for every parquet column type. Each
 * case keeps existing rows 0, 2, 4 and replaces rows 1, 3 with O3 rows 0, 1.
 */
public class FrameAlgebraMergeIdenticalTest extends AbstractCairoTest {
    private static final int[] FIXED_TYPES = {
            ColumnType.BOOLEAN,
            ColumnType.BYTE,
            ColumnType.getGeoHashTypeWithBits(5),
            ColumnType.getDecimalType(2, 1),
            ColumnType.SHORT,
            ColumnType.CHAR,
            ColumnType.getGeoHashTypeWithBits(15),
            ColumnType.getDecimalType(4, 2),
            ColumnType.INT,
            ColumnType.FLOAT,
            ColumnType.SYMBOL,
            ColumnType.IPv4,
            ColumnType.getGeoHashTypeWithBits(25),
            ColumnType.getDecimalType(9, 3),
            ColumnType.LONG,
            ColumnType.DOUBLE,
            ColumnType.DATE,
            ColumnType.TIMESTAMP_MICRO,
            ColumnType.TIMESTAMP_NANO,
            ColumnType.getGeoHashTypeWithBits(50),
            ColumnType.getDecimalType(18, 4),
            ColumnType.UUID,
            ColumnType.LONG128,
            ColumnType.getDecimalType(38, 6),
            ColumnType.LONG256,
            ColumnType.getDecimalType(76, 10),
    };
    private static final int ROWS = 5;

    @Test
    public void testArray() throws Exception {
        assertMemoryLeak(() -> {
            final int type = ColumnType.encodeArrayType(ColumnType.DOUBLE, 2);
            try (
                    DirectArray a = array2d(2, 3, 0);
                    DirectArray b = array2d(2, 3, 1);
                    DirectArray transposed = array2d(3, 2, 0);
                    DirectArray empty = array2d(0, 0, 0);
                    DirectArray nan = array2d(1, 1, Double.NaN)
            ) {
                // same values
                assertArray(type, true, a, a);
                assertArray(type, true, nan, nan);
                // one element differs
                assertArray(type, false, a, b);
                // the same six doubles in another shape
                assertArray(type, false, a, transposed);
                // NULL against a value, either way
                assertArray(type, false, null, a);
                assertArray(type, false, a, null);
                assertArray(type, true, null, null);
                // an empty array is not NULL
                assertArray(type, false, null, empty);
                assertArray(type, false, empty, null);
                assertArray(type, true, empty, empty);
            }
        });
    }

    @Test
    public void testBinary() throws Exception {
        assertMemoryLeak(() -> {
            final byte[] value = {1, 2, 3, 4, 5};
            final byte[] lastDiffers = {1, 2, 3, 4, 6};
            final byte[] empty = {};
            assertBinary(true, value, value);
            assertBinary(false, value, lastDiffers);
            assertBinary(false, value, new byte[]{1, 2, 3, 4});
            assertBinary(false, null, value);
            assertBinary(false, value, null);
            assertBinary(true, null, null);
            assertBinary(false, null, empty);
            assertBinary(false, empty, null);
            assertBinary(true, empty, empty);
        });
    }

    @Test
    public void testDoubleZeroSignAndNaN() throws Exception {
        assertMemoryLeak(() -> {
            // bitwise, as the native check: -0.0 replacing 0.0 is a change, the NULL NaN is not
            assertFixed(ColumnType.DOUBLE, false, Double.doubleToRawLongBits(0.0), Double.doubleToRawLongBits(-0.0));
            assertFixed(ColumnType.DOUBLE, true, Double.doubleToRawLongBits(Double.NaN), Double.doubleToRawLongBits(Double.NaN));
            assertFixed(ColumnType.FLOAT, false, Float.floatToRawIntBits(0.0f), Float.floatToRawIntBits(-0.0f));
            assertFixed(ColumnType.FLOAT, true, Float.floatToRawIntBits(Float.NaN), Float.floatToRawIntBits(Float.NaN));
        });
    }

    @Test
    public void testFixedSizeTypes() throws Exception {
        assertMemoryLeak(() -> {
            for (int type : FIXED_TYPES) {
                final int size = ColumnType.sizeOf(type);
                try (
                        MemoryCARW existing = Vm.getCARWInstance(1024, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT);
                        MemoryCARW o3 = Vm.getCARWInstance(1024, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT)
                ) {
                    // existing row 3 is NULL
                    for (int row = 0; row < ROWS; row++) {
                        if (row == 3) {
                            putNull(existing, type);
                        } else {
                            putValue(existing, size, row + 1, -1);
                        }
                    }
                    final String name = ColumnType.nameOf(type);

                    // the replacing rows are equal: row 1 and NULL
                    o3.jumpTo(0);
                    putValue(o3, size, 2, -1);
                    putNull(o3, type);
                    Assert.assertTrue(name + ": equal", isIdentical(type, existing, null, o3, null));

                    // a NULL replaced with a value
                    o3.jumpTo(0);
                    putValue(o3, size, 2, -1);
                    putValue(o3, size, 9, -1);
                    Assert.assertFalse(name + ": value replaces NULL", isIdentical(type, existing, null, o3, null));

                    // a value replaced with NULL
                    o3.jumpTo(0);
                    putNull(o3, type);
                    putNull(o3, type);
                    Assert.assertFalse(name + ": NULL replaces value", isIdentical(type, existing, null, o3, null));

                    // one byte of the value differs, at every byte position
                    for (int b = 0; b < size; b++) {
                        o3.jumpTo(0);
                        putValue(o3, size, 2, b);
                        putNull(o3, type);
                        Assert.assertFalse(name + ": byte " + b + " differs", isIdentical(type, existing, null, o3, null));
                    }
                }
            }
        });
    }

    @Test
    public void testString() throws Exception {
        assertMemoryLeak(() -> {
            assertString(true, "abc", "abc");
            assertString(true, "a much longer string value", "a much longer string value");
            assertString(false, "a much longer string value", "a much longer string valuf");
            assertString(false, "abc", "abd");
            assertString(false, "abc", "abcd");
            assertString(false, null, "abc");
            assertString(false, "abc", null);
            assertString(true, null, null);
            assertString(false, null, "");
            assertString(false, "", null);
            assertString(true, "", "");
        });
    }

    @Test
    public void testVarchar() throws Exception {
        assertMemoryLeak(() -> {
            // inlined in the aux entry up to 9 bytes
            assertVarchar(true, "abc", "abc");
            assertVarchar(true, "123456789", "123456789");
            assertVarchar(false, "123456789", "123456780");
            assertVarchar(false, "12345678", "123456789");
            // 10 bytes and longer: a 6 byte prefix in the aux entry, the value in data
            assertVarchar(true, "1234567890", "1234567890");
            assertVarchar(false, "1234567890", "1234567891");
            assertVarchar(false, "123456789", "1234567890");
            assertVarchar(true, "1234567890abcdefghij", "1234567890abcdefghij");
            assertVarchar(false, "1234567890abcdefghij", "1234567890abcdefghik");
            assertVarchar(false, "1234567890abcdefghij", "0234567890abcdefghij");
            // non-ASCII
            assertVarchar(true, "zażółć gęślą jaźń", "zażółć gęślą jaźń");
            assertVarchar(false, "zażółć gęślą jaźń", "zażółć gęślą jaźn");
            assertVarchar(false, null, "abc");
            assertVarchar(false, "abc", null);
            assertVarchar(true, null, null);
            assertVarchar(false, null, "");
            assertVarchar(false, "", null);
            assertVarchar(true, "", "");
        });
    }

    private static DirectArray array2d(int rows, int cols, double base) {
        final DirectArray array = new DirectArray();
        array.setType(ColumnType.encodeArrayType(ColumnType.DOUBLE, 2));
        array.getShape().set(0, rows);
        array.getShape().set(1, cols);
        array.applyShape();
        for (int i = 0, n = rows * cols; i < n; i++) {
            array.putDouble(i, Double.isNaN(base) ? Double.NaN : base + i);
        }
        return array;
    }

    private static void assertArray(int type, boolean isExpectedIdentical, DirectArray existingValue, DirectArray o3Value) {
        assertVar(type, isExpectedIdentical, (aux, data, isExisting, row) -> {
            if (isExisting && row != 3) {
                try (DirectArray kept = array2d(1, 2, row)) {
                    ArrayTypeDriver.appendValue(aux, data, kept);
                }
                return;
            }
            final DirectArray value = isExisting ? existingValue : o3Value;
            if (value == null) {
                ArrayTypeDriver.INSTANCE.appendNull(aux, data);
            } else {
                ArrayTypeDriver.appendValue(aux, data, value);
            }
        });
    }

    private static void assertBinary(boolean isExpectedIdentical, byte[] existingValue, byte[] o3Value) {
        assertVar(ColumnType.BINARY, isExpectedIdentical, (aux, data, isExisting, row) -> {
            final byte[] value = isExisting && row != 3 ? new byte[]{(byte) row} : (isExisting ? existingValue : o3Value);
            if (value == null) {
                aux.putLong(data.putNullBin());
            } else {
                aux.putLong(data.putBin(new BinarySequence() {
                    @Override
                    public byte byteAt(long index) {
                        return value[(int) index];
                    }

                    @Override
                    public long length() {
                        return value.length;
                    }
                }));
            }
        });
    }

    // Existing row 3 holds existingValue and O3 row 1, which replaces it, holds o3Value. O3
    // row 0 replaces existing row 1 with an equal value.
    private static void assertFixed(int type, boolean isExpectedIdentical, long existingValue, long o3Value) {
        final int size = ColumnType.sizeOf(type);
        try (
                MemoryCARW existing = Vm.getCARWInstance(1024, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT);
                MemoryCARW o3 = Vm.getCARWInstance(1024, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT)
        ) {
            for (int row = 0; row < ROWS; row++) {
                putLong(existing, size, row == 3 ? existingValue : row + 1);
            }
            putLong(o3, size, 2);
            putLong(o3, size, o3Value);
            Assert.assertEquals(ColumnType.nameOf(type), isExpectedIdentical, isIdentical(type, existing, null, o3, null));
        }
    }

    private static void assertString(boolean isExpectedIdentical, String existingValue, String o3Value) {
        assertVar(ColumnType.STRING, isExpectedIdentical, (aux, data, isExisting, row) -> {
            final String value = isExisting && row != 3 ? "kept" + row : (isExisting ? existingValue : o3Value);
            if (value == null) {
                StringTypeDriver.INSTANCE.appendNull(aux, data);
            } else {
                StringTypeDriver.appendValue(aux, data, value);
            }
        });
    }

    /**
     * Builds the existing rows (row 3 from the case, the others fixed values) and the O3
     * rows (row 0 equal to existing row 1, row 1 from the case) of a var-size column, and
     * checks the result. STRING and BINARY aux vectors start with a 0 offset.
     */
    private static void assertVar(int type, boolean isExpectedIdentical, VarValueWriter writer) {
        final short tag = ColumnType.tagOf(type);
        final boolean hasLeadingOffset = tag == ColumnType.STRING || tag == ColumnType.BINARY;
        try (
                MemoryCARW existingAux = Vm.getCARWInstance(1024, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT);
                MemoryCARW existingData = Vm.getCARWInstance(1024, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT);
                MemoryCARW o3Aux = Vm.getCARWInstance(1024, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT);
                MemoryCARW o3Data = Vm.getCARWInstance(1024, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT)
        ) {
            if (hasLeadingOffset) {
                existingAux.putLong(0);
                o3Aux.putLong(0);
            }
            for (int row = 0; row < ROWS; row++) {
                writer.write(existingAux, existingData, true, row);
            }
            // O3 row 0 equals existing row 1: write it as a kept row 1
            writer.write(o3Aux, o3Data, true, 1);
            writer.write(o3Aux, o3Data, false, 3);
            Assert.assertEquals(ColumnType.nameOf(type), isExpectedIdentical, isIdentical(type, existingData, existingAux, o3Data, o3Aux));
        }
    }

    private static void assertVarchar(boolean isExpectedIdentical, String existingValue, String o3Value) {
        assertVar(ColumnType.VARCHAR, isExpectedIdentical, (aux, data, isExisting, row) -> {
            final String value = isExisting && row != 3 ? "kept row " + row : (isExisting ? existingValue : o3Value);
            VarcharTypeDriver.appendValue(aux, data, value == null ? null : new Utf8String(value));
        });
    }

    private static boolean isIdentical(int type, MemoryCARW existingData, MemoryCARW existingAux, MemoryCARW o3Data, MemoryCARW o3Aux) {
        // (timestamp, row id) entries; the top bit marks an existing row the merge keeps
        final long indexSize = ROWS * 2L * Long.BYTES;
        final long index = Unsafe.malloc(indexSize, MemoryTag.NATIVE_DEFAULT);
        try {
            for (int row = 0; row < ROWS; row++) {
                final long entry = index + row * 2L * Long.BYTES;
                Unsafe.putLong(entry, row);
                Unsafe.putLong(entry + Long.BYTES, row == 1 ? 0 : (row == 3 ? 1 : (row | Long.MIN_VALUE)));
            }
            return FrameAlgebra.isColumnMergeIdentical(
                    type,
                    ROWS,
                    existingAux != null ? existingAux.addressOf(0) : 0,
                    existingData.addressOf(0),
                    o3Aux != null ? o3Aux.addressOf(0) : 0,
                    o3Data.addressOf(0),
                    index
            );
        } finally {
            Unsafe.free(index, indexSize, MemoryTag.NATIVE_DEFAULT);
        }
    }

    // writes the low size bytes of value, little-endian; sizes above 8 repeat it per long
    private static void putLong(MemoryCARW mem, int size, long value) {
        switch (size) {
            case 1 -> mem.putByte((byte) value);
            case 2 -> mem.putShort((short) value);
            case 4 -> mem.putInt((int) value);
            default -> {
                for (int i = 0; i < size / Long.BYTES; i++) {
                    mem.putLong(value);
                }
            }
        }
    }

    private static void putNull(MemoryCARW mem, int type) {
        final int size = ColumnType.sizeOf(type);
        if (size <= Long.BYTES) {
            putLong(mem, size, TableUtils.getNullLong(type, 0));
        } else {
            for (int i = 0; i < size / Long.BYTES; i++) {
                mem.putLong(TableUtils.getNullLong(type, i));
            }
        }
    }

    // A value of size bytes from seed, each byte distinct and not a NULL pattern. With
    // flipByte >= 0, that byte is changed.
    private static void putValue(MemoryCARW mem, int size, int seed, int flipByte) {
        for (int b = 0; b < size; b++) {
            byte value = (byte) (seed * 16 + b + 1);
            if (b == flipByte) {
                value ^= 0x40;
            }
            mem.putByte(value);
        }
    }

    @FunctionalInterface
    private interface VarValueWriter {
        void write(MemoryCARW aux, MemoryCARW data, boolean isExisting, int row);
    }
}
