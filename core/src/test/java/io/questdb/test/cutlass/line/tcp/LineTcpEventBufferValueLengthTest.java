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

package io.questdb.test.cutlass.line.tcp;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.arr.BorrowedArray;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.cutlass.line.tcp.LineTcpEventBuffer;
import io.questdb.cutlass.line.tcp.LineTcpParser;
import io.questdb.std.Decimal256;
import io.questdb.std.Long128;
import io.questdb.std.MemoryTag;
import io.questdb.std.Numbers;
import io.questdb.std.Unsafe;
import io.questdb.std.str.DirectUtf8String;
import io.questdb.std.str.Utf8Sequence;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.nio.charset.StandardCharsets;

/**
 * Covers {@link LineTcpEventBuffer#columnValueLength(byte, long)}, which the non-WAL ILP writer
 * calls to step over a value whose column the table has already dropped. Every entity the buffer
 * can write needs an arm returning exactly the number of bytes the writer emitted after the entity
 * type byte: a missing arm throws and the caller then cancels the whole row, and a wrong count
 * desynchronises every column that follows.
 * <p>
 * The expected lengths below come from the layouts the {@code add*} methods write, not from the
 * constants those methods use, so changing a constant on one side alone fails the test. The buffer
 * is pre-filled with a sentinel byte, which pins down where each writer stopped.
 */
public class LineTcpEventBufferValueLengthTest {
    private static final int BUF_SIZE = 4096;
    private static final long DECIMAL_VALUE_BYTES = Integer.BYTES + Byte.BYTES + 4L * Long.BYTES;
    private static final byte UNWRITTEN = (byte) 0xA7;

    @Test
    public void testDecimalValueLengthMatchesWrittenBytes() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final long buf = Unsafe.malloc(BUF_SIZE, MemoryTag.NATIVE_DEFAULT);
            try {
                final LineTcpEventBuffer buffer = new LineTcpEventBuffer(buf, BUF_SIZE);
                final Decimal256 decimal = new Decimal256();
                decimal.of(1, 2, 3, 4, 5);
                assertDecimal(buffer, buf, decimal, ColumnType.getDecimalType(76, 5));
                decimal.of(Long.MIN_VALUE, -1, -1, -1, 38);
                assertDecimal(buffer, buf, decimal, ColumnType.getDecimalType(76, 38));
                decimal.ofNull();
                assertDecimal(buffer, buf, decimal, ColumnType.getDecimalType(10, 2));
            } finally {
                Unsafe.free(buf, BUF_SIZE, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testEveryWrittenEntityTypeIsSkippable() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final long buf = Unsafe.malloc(BUF_SIZE, MemoryTag.NATIVE_DEFAULT);
            final long src = Unsafe.malloc(BUF_SIZE, MemoryTag.NATIVE_DEFAULT);
            try {
                fill(buf, BUF_SIZE);
                final LineTcpEventBuffer buffer = new LineTcpEventBuffer(buf, BUF_SIZE);
                final DirectUtf8String utf8 = new DirectUtf8String();
                final Decimal256 decimal = new Decimal256();
                decimal.of(9, 8, 7, 6, 12);

                long lo = buf;
                long hi = buffer.addBoolean(lo, (byte) 1);
                lo = assertSkip(buffer, lo, hi, LineTcpParser.ENTITY_TYPE_BOOLEAN, Byte.BYTES);
                hi = buffer.addByte(lo, (byte) 7);
                lo = assertSkip(buffer, lo, hi, LineTcpParser.ENTITY_TYPE_BYTE, Byte.BYTES);
                hi = buffer.addChar(lo, 'q');
                lo = assertSkip(buffer, lo, hi, LineTcpParser.ENTITY_TYPE_CHAR, Character.BYTES);
                hi = buffer.addDate(lo, 1_700_000_000_000L);
                lo = assertSkip(buffer, lo, hi, LineTcpParser.ENTITY_TYPE_DATE, Long.BYTES);
                hi = buffer.addDecimal(lo, decimal, ColumnType.getDecimalType(76, 12));
                lo = assertSkip(buffer, lo, hi, LineTcpParser.ENTITY_TYPE_DECIMAL, DECIMAL_VALUE_BYTES);
                hi = buffer.addDouble(lo, 1.5);
                lo = assertSkip(buffer, lo, hi, LineTcpParser.ENTITY_TYPE_DOUBLE, Double.BYTES);
                hi = buffer.addFloat(lo, 2.5f);
                lo = assertSkip(buffer, lo, hi, LineTcpParser.ENTITY_TYPE_FLOAT, Float.BYTES);
                hi = buffer.addGeoHash(lo, directUtf8(src, utf8, "q"), geoMeta(5, ColumnType.GEOBYTE));
                lo = assertSkip(buffer, lo, hi, LineTcpParser.ENTITY_TYPE_GEOBYTE, Byte.BYTES);
                hi = buffer.addGeoHash(lo, directUtf8(src, utf8, "que"), geoMeta(15, ColumnType.GEOSHORT));
                lo = assertSkip(buffer, lo, hi, LineTcpParser.ENTITY_TYPE_GEOSHORT, Short.BYTES);
                hi = buffer.addGeoHash(lo, directUtf8(src, utf8, "quest"), geoMeta(25, ColumnType.GEOINT));
                lo = assertSkip(buffer, lo, hi, LineTcpParser.ENTITY_TYPE_GEOINT, Integer.BYTES);
                hi = buffer.addGeoHash(lo, directUtf8(src, utf8, "questdb"), geoMeta(35, ColumnType.GEOLONG));
                lo = assertSkip(buffer, lo, hi, LineTcpParser.ENTITY_TYPE_GEOLONG, Long.BYTES);
                hi = buffer.addInt(lo, 42);
                lo = assertSkip(buffer, lo, hi, LineTcpParser.ENTITY_TYPE_INTEGER, Integer.BYTES);
                hi = buffer.addLong(lo, 42L);
                lo = assertSkip(buffer, lo, hi, LineTcpParser.ENTITY_TYPE_LONG, Long.BYTES);
                hi = buffer.addLong256(lo, directUtf8(src, utf8, "0x123456789a"));
                lo = assertSkip(buffer, lo, hi, LineTcpParser.ENTITY_TYPE_LONG256, 12 * 2L + Integer.BYTES);
                hi = buffer.addNull(lo);
                lo = assertSkip(buffer, lo, hi, LineTcpParser.ENTITY_TYPE_NULL, 0);
                hi = buffer.addShort(lo, (short) 3);
                lo = assertSkip(buffer, lo, hi, LineTcpParser.ENTITY_TYPE_SHORT, Short.BYTES);
                hi = buffer.addString(lo, directUtf8(src, utf8, "abcde"));
                lo = assertSkip(buffer, lo, hi, LineTcpParser.ENTITY_TYPE_STRING, 5 * 2L + Integer.BYTES);
                hi = buffer.addSymbol(lo, directUtf8(src, utf8, "cached"), value -> 11);
                lo = assertSkip(buffer, lo, hi, LineTcpParser.ENTITY_TYPE_CACHED_TAG, Integer.BYTES);
                hi = buffer.addSymbol(lo, directUtf8(src, utf8, "uncached"), value -> SymbolTable.VALUE_NOT_FOUND);
                lo = assertSkip(buffer, lo, hi, LineTcpParser.ENTITY_TYPE_TAG, 8 * 2L + Integer.BYTES);
                hi = buffer.addTimestamp(lo, 1_700_000_000_000_000L);
                lo = assertSkip(buffer, lo, hi, LineTcpParser.ENTITY_TYPE_TIMESTAMP, Long.BYTES);
                hi = buffer.addUuid(lo, directUtf8(src, utf8, "11111111-2222-3333-4444-555555555555"));
                lo = assertSkip(buffer, lo, hi, LineTcpParser.ENTITY_TYPE_UUID, Long128.BYTES);
                hi = buffer.addVarchar(lo, directUtf8(src, utf8, "abc"));
                lo = assertSkip(buffer, lo, hi, LineTcpParser.ENTITY_TYPE_VARCHAR, Byte.BYTES + Integer.BYTES + 3L);

                // a 1-D DOUBLE array, kept away from the region the utf8 values reuse
                final long shape = src + BUF_SIZE / 2;
                final long values = shape + Integer.BYTES;
                Unsafe.putInt(shape, 2);
                Unsafe.putDouble(values, 1.5);
                Unsafe.putDouble(values + Double.BYTES, 2.5);
                final BorrowedArray array = new BorrowedArray()
                        .of(ColumnType.encodeArrayType(ColumnType.DOUBLE, 1), shape, values, 2 * Double.BYTES);
                hi = buffer.addArray(lo, array);
                lo = assertSkip(buffer, lo, hi, LineTcpParser.ENTITY_TYPE_ARRAY, 3L * Integer.BYTES + 2 * Double.BYTES);
                hi = buffer.addArray(lo, null);
                lo = assertSkip(buffer, lo, hi, LineTcpParser.ENTITY_TYPE_NULL, 0);

                Assert.assertEquals(UNWRITTEN, buffer.readByte(lo));
                assertWalkEndsAt(buffer, buf, lo);
            } finally {
                Unsafe.free(src, BUF_SIZE, MemoryTag.NATIVE_DEFAULT);
                Unsafe.free(buf, BUF_SIZE, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testVarcharValueLengthMatchesWrittenBytes() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final long buf = Unsafe.malloc(BUF_SIZE, MemoryTag.NATIVE_DEFAULT);
            final long src = Unsafe.malloc(BUF_SIZE, MemoryTag.NATIVE_DEFAULT);
            try {
                final LineTcpEventBuffer buffer = new LineTcpEventBuffer(buf, BUF_SIZE);
                final DirectUtf8String utf8 = new DirectUtf8String();
                assertVarchar(buffer, buf, src, utf8, "");
                assertVarchar(buffer, buf, src, utf8, "abc");
                assertVarchar(buffer, buf, src, utf8, "café über 中文");
                // a size the writer cannot fit into a single byte
                assertVarchar(buffer, buf, src, utf8, "x".repeat(1_000));
            } finally {
                Unsafe.free(src, BUF_SIZE, MemoryTag.NATIVE_DEFAULT);
                Unsafe.free(buf, BUF_SIZE, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    private static void assertDecimal(LineTcpEventBuffer buffer, long lo, Decimal256 decimal, int columnType) {
        fill(lo, BUF_SIZE);
        final long hi = buffer.addDecimal(lo, decimal, columnType);
        // the entity type byte is consumed before the value length is queried
        final long valueLo = lo + Byte.BYTES;
        Assert.assertEquals(LineTcpParser.ENTITY_TYPE_DECIMAL, buffer.readByte(lo));
        Assert.assertEquals(DECIMAL_VALUE_BYTES, buffer.columnValueLength(LineTcpParser.ENTITY_TYPE_DECIMAL, valueLo));
        Assert.assertEquals(hi, valueLo + DECIMAL_VALUE_BYTES);
        Assert.assertEquals(UNWRITTEN, buffer.readByte(hi));

        Assert.assertEquals(columnType, buffer.readInt(valueLo));
        Assert.assertEquals(decimal.getScale(), buffer.readByte(valueLo + Integer.BYTES));
        final long limbs = valueLo + Integer.BYTES + Byte.BYTES;
        Assert.assertEquals(decimal.getHh(), buffer.readLong(limbs));
        Assert.assertEquals(decimal.getHl(), buffer.readLong(limbs + Long.BYTES));
        Assert.assertEquals(decimal.getLh(), buffer.readLong(limbs + 2 * Long.BYTES));
        Assert.assertEquals(decimal.getLl(), buffer.readLong(limbs + 3 * Long.BYTES));

        final Decimal256 readBack = new Decimal256();
        Assert.assertEquals(columnType, buffer.readDecimal(valueLo, readBack));
        Assert.assertEquals(decimal, readBack);
    }

    private static long assertSkip(LineTcpEventBuffer buffer, long lo, long hi, byte expectedType, long expectedLength) {
        final String message = "entity type " + expectedType;
        Assert.assertEquals(message, expectedType, buffer.readByte(lo));
        final long valueLo = lo + Byte.BYTES;
        Assert.assertEquals(message, expectedLength, buffer.columnValueLength(expectedType, valueLo));
        Assert.assertEquals(message, hi, valueLo + expectedLength);
        return hi;
    }

    private static void assertVarchar(LineTcpEventBuffer buffer, long lo, long src, DirectUtf8String flyweight, String value) {
        fill(lo, BUF_SIZE);
        final byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        final boolean isAscii = bytes.length == value.length();
        final long hi = buffer.addVarchar(lo, directUtf8(src, flyweight, value));

        final long valueLo = lo + Byte.BYTES;
        final long expectedLength = Byte.BYTES + Integer.BYTES + bytes.length;
        Assert.assertEquals(LineTcpParser.ENTITY_TYPE_VARCHAR, buffer.readByte(lo));
        Assert.assertEquals(expectedLength, buffer.columnValueLength(LineTcpParser.ENTITY_TYPE_VARCHAR, valueLo));
        Assert.assertEquals(hi, valueLo + expectedLength);
        Assert.assertEquals(UNWRITTEN, buffer.readByte(hi));

        Assert.assertEquals(isAscii ? 0 : 1, buffer.readByte(valueLo));
        Assert.assertEquals(bytes.length, buffer.readInt(valueLo + Byte.BYTES));
        final Utf8Sequence stored = buffer.readVarchar(valueLo + Byte.BYTES, isAscii);
        Assert.assertEquals(bytes.length, stored.size());
        Assert.assertEquals(isAscii, stored.isAscii());
        Assert.assertEquals(value, stored.toString());
    }

    // mirrors the skip loop the writer runs over a row whose columns are all gone
    private static void assertWalkEndsAt(LineTcpEventBuffer buffer, long lo, long hi) {
        long address = lo;
        while (address < hi) {
            final byte entityType = buffer.readByte(address);
            address += Byte.BYTES;
            address += buffer.columnValueLength(entityType, address);
        }
        Assert.assertEquals(hi, address);
    }

    private static DirectUtf8String directUtf8(long address, DirectUtf8String flyweight, String value) {
        final byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        for (int i = 0; i < bytes.length; i++) {
            Unsafe.putByte(address + i, bytes[i]);
        }
        return flyweight.of(address, address + bytes.length, bytes.length == value.length());
    }

    private static void fill(long address, int size) {
        for (int i = 0; i < size; i++) {
            Unsafe.putByte(address + i, UNWRITTEN);
        }
    }

    private static int geoMeta(int bits, short columnTypeTag) {
        return Numbers.encodeLowHighShorts((short) bits, columnTypeTag);
    }
}
