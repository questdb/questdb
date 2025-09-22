/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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

package io.questdb.test.griffin;

import io.questdb.cairo.ArrayColumnTypes;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.GenericRecordMetadata;
import io.questdb.cairo.ImplicitCastException;
import io.questdb.cairo.ListColumnFilter;
import io.questdb.cairo.TableColumnMetadata;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.arr.ArrayView;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.RecordToRowCopier;
import io.questdb.griffin.RecordToRowCopierUtils;
import io.questdb.std.BinarySequence;
import io.questdb.std.BytecodeAssembler;
import io.questdb.std.Decimal256;
import io.questdb.std.Decimals;
import io.questdb.std.Long256;
import io.questdb.std.Numbers;
import io.questdb.std.NumericException;
import io.questdb.std.str.DirectUtf8Sequence;
import io.questdb.std.str.Utf8Sequence;
import io.questdb.test.AbstractCairoTest;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.Assert;
import org.junit.Test;

import java.math.BigDecimal;

public class RecordToRowCopierUtilsTest extends AbstractCairoTest {
    @Test
    public void testCopierByteToDecimal() {
        RecordToRowCopier copier = generateCopier(ColumnType.BYTE, ColumnType.getDecimalType(16, 3));
        copier.copy(sqlExecutionContext, getLongRecord(ColumnType.BYTE, 123), getLongAsserter(123000));
    }

    @Test(expected = ImplicitCastException.class)
    public void testCopierByteToDecimalOverflow() {
        RecordToRowCopier copier = generateCopier(ColumnType.BYTE, ColumnType.getDecimalType(4, 3));
        copier.copy(sqlExecutionContext, getLongRecord(ColumnType.BYTE, 99), new RowAsserter());
    }

    @Test
    public void testCopierDecimal8ToDecimal32() {
        // Converting 1.2 from a Decimal8 with a scale of 1 (1.2) to a Decimal32 with a scale of 3 (1.200)
        int fromType = ColumnType.getDecimalType(2, 1);
        Assert.assertEquals("Unexpected from type", ColumnType.DECIMAL8, ColumnType.tagOf(fromType));
        int toType = ColumnType.getDecimalType(8, 3);
        Assert.assertEquals("Unexpected to type", ColumnType.DECIMAL32, ColumnType.tagOf(toType));

        RecordToRowCopier copier = generateCopier(fromType, toType);

        Record rec = new Record() {
            @Override
            public byte getDecimal8(int col) {
                return 12;
            }
        };

        copier.copy(sqlExecutionContext, rec, getIntAsserter(1200));
    }

    @Test
    public void testCopierDecimalCases() {
        Decimal256 d = new Decimal256();
        Decimal256 c = new Decimal256();
        Decimal256 dnull = new Decimal256();
        BytecodeAssembler asm = new BytecodeAssembler();
        dnull.ofNull();
        boolean[] isNull = new boolean[]{false, true};
        int[] fromPrecisions = new int[]{1, 2, 3, 4, 5, 8, 15, 25, 30, 50, 75};
        int[] toPrecisions = new int[]{1, 2, 3, 4, 5, 8, 15, 25, 30, 50, 75};
        for (int fromPrecision : fromPrecisions) {
            for (int toPrecision : toPrecisions) {
                for (boolean nulled : isNull) {
                    if (nulled) {
                        testDecimalCast(asm, fromPrecision, toPrecision, 0, dnull, true);
                        continue;
                    }

                    for (int fromScale = 1; fromScale <= fromPrecision; fromScale <<= 1) {
                        for (int toScale = 1; toScale <= toPrecision; toScale <<= 1) {
                            boolean fit;
                            generateValue(d, fromPrecision, fromScale);
                            try {
                                c.copyFrom(d);
                                c.rescale(toScale);
                                fit = c.comparePrecision(toPrecision);
                            } catch (NumericException ignored) {
                                fit = false;
                            }
                            testDecimalCast(asm, fromPrecision, toPrecision, toScale, d, fit);
                            d.negate();
                            testDecimalCast(asm, fromPrecision, toPrecision, toScale, d, fit);
                        }
                    }
                }
            }
        }
    }

    @Test
    public void testCopierIntToDecimal() {
        RecordToRowCopier copier = generateCopier(ColumnType.INT, ColumnType.getDecimalType(16, 3));
        copier.copy(sqlExecutionContext, getLongRecord(ColumnType.INT, 123456), getLongAsserter(123456000));
    }

    @Test(expected = ImplicitCastException.class)
    public void testCopierIntToDecimalOverflow() {
        RecordToRowCopier copier = generateCopier(ColumnType.INT, ColumnType.getDecimalType(8, 3));
        copier.copy(sqlExecutionContext, getLongRecord(ColumnType.INT, 999999), new RowAsserter());
    }

    @Test
    public void testCopierLongToDecimal() {
        RecordToRowCopier copier = generateCopier(ColumnType.LONG, ColumnType.getDecimalType(16, 3));
        copier.copy(sqlExecutionContext, getLongRecord(ColumnType.LONG, 123456L), getLongAsserter(123456000L));
    }

    @Test(expected = ImplicitCastException.class)
    public void testCopierLongToDecimalOverflow() {
        RecordToRowCopier copier = generateCopier(ColumnType.LONG, ColumnType.getDecimalType(8, 3));
        copier.copy(sqlExecutionContext, getLongRecord(ColumnType.LONG, 999999L), new RowAsserter());
    }

    @Test
    public void testCopierNullByteToDecimal() {
        RecordToRowCopier copier = generateCopier(ColumnType.BYTE, ColumnType.getDecimalType(16, 3));
        copier.copy(sqlExecutionContext, getLongRecord(ColumnType.BYTE, Numbers.BYTE_NULL), getLongAsserter(Decimals.DECIMAL64_NULL));
    }

    @Test
    public void testCopierNullIntToDecimal() {
        RecordToRowCopier copier = generateCopier(ColumnType.INT, ColumnType.getDecimalType(16, 3));
        copier.copy(sqlExecutionContext, getLongRecord(ColumnType.INT, Numbers.INT_NULL), getLongAsserter(Decimals.DECIMAL64_NULL));
    }

    @Test
    public void testCopierNullLongToDecimal() {
        RecordToRowCopier copier = generateCopier(ColumnType.LONG, ColumnType.getDecimalType(16, 3));
        copier.copy(sqlExecutionContext, getLongRecord(ColumnType.LONG, Numbers.LONG_NULL), getLongAsserter(Decimals.DECIMAL64_NULL));
    }

    @Test
    public void testCopierNullShortToDecimal() {
        RecordToRowCopier copier = generateCopier(ColumnType.SHORT, ColumnType.getDecimalType(16, 3));
        copier.copy(sqlExecutionContext, getLongRecord(ColumnType.SHORT, Numbers.SHORT_NULL), getLongAsserter(Decimals.DECIMAL64_NULL));
    }

    @Test
    public void testCopierShortToDecimal() {
        RecordToRowCopier copier = generateCopier(ColumnType.SHORT, ColumnType.getDecimalType(16, 3));
        copier.copy(sqlExecutionContext, getLongRecord(ColumnType.SHORT, 1234), getLongAsserter(1234000));
    }

    @Test(expected = ImplicitCastException.class)
    public void testCopierShortToDecimalOverflow() {
        RecordToRowCopier copier = generateCopier(ColumnType.SHORT, ColumnType.getDecimalType(5, 3));
        copier.copy(sqlExecutionContext, getLongRecord(ColumnType.SHORT, 9999), new RowAsserter());
    }

    private static @NotNull Record getDecimalRecord(int fromType, Decimal256 value) {
        int fromTag = ColumnType.tagOf(fromType);
        return new Record() {
            @Override
            public long getDecimal128Hi(int col) {
                Assert.assertEquals(ColumnType.DECIMAL128, fromTag);
                return value.isNull() ? Decimals.DECIMAL128_HI_NULL : value.getLh();
            }

            @Override
            public long getDecimal128Lo(int col) {
                Assert.assertEquals(ColumnType.DECIMAL128, fromTag);
                return value.isNull() ? Decimals.DECIMAL128_LO_NULL : value.getLl();
            }

            @Override
            public short getDecimal16(int col) {
                Assert.assertEquals(ColumnType.DECIMAL16, fromTag);
                return value.isNull() ? Short.MIN_VALUE : (short) value.getLl();
            }

            @Override
            public long getDecimal256HH(int col) {
                Assert.assertEquals(ColumnType.DECIMAL256, fromTag);
                return value.getHh();
            }

            @Override
            public long getDecimal256HL(int col) {
                Assert.assertEquals(ColumnType.DECIMAL256, fromTag);
                return value.getHl();
            }

            @Override
            public long getDecimal256LH(int col) {
                Assert.assertEquals(ColumnType.DECIMAL256, fromTag);
                return value.getLh();
            }

            @Override
            public long getDecimal256LL(int col) {
                Assert.assertEquals(ColumnType.DECIMAL256, fromTag);
                return value.getLl();
            }

            @Override
            public int getDecimal32(int col) {
                Assert.assertEquals(ColumnType.DECIMAL32, fromTag);
                return value.isNull() ? Integer.MIN_VALUE : (int) value.getLl();
            }

            @Override
            public long getDecimal64(int col) {
                Assert.assertEquals(ColumnType.DECIMAL64, fromTag);
                return value.isNull() ? Long.MIN_VALUE : value.getLl();
            }

            @Override
            public byte getDecimal8(int col) {
                Assert.assertEquals(ColumnType.DECIMAL8, fromTag);
                return value.isNull() ? Byte.MIN_VALUE : (byte) value.getLl();
            }
        };
    }

    private static @NotNull TableWriter.Row getDecimalRow(int toType, @Nullable Decimal256 expectedValue) {
        short toTag = ColumnType.tagOf(toType);
        return new RowAsserter() {
            @Override
            public void putByte(int col, byte value) {
                Assert.assertEquals(ColumnType.DECIMAL8, toTag);
                if (expectedValue != null) {
                    Assert.assertEquals((byte) (expectedValue.isNull() ? Byte.MIN_VALUE : expectedValue.getLl()), value);
                } else {
                    Assert.fail("Expected casting to fail");
                }
            }

            @Override
            public void putDecimal128(int col, long high, long low) {
                Assert.assertEquals(ColumnType.DECIMAL128, toTag);
                if (expectedValue != null) {
                    Assert.assertEquals(expectedValue.isNull() ? Long.MIN_VALUE : expectedValue.getLh(), high);
                    Assert.assertEquals(expectedValue.getLl(), low);
                } else {
                    Assert.fail("Expected casting to fail");
                }
            }

            @Override
            public void putDecimal256(int col, long hh, long hl, long lh, long ll) {
                Assert.assertEquals(ColumnType.DECIMAL256, toTag);
                if (expectedValue != null) {
                    Assert.assertEquals(expectedValue.getHh(), hh);
                    Assert.assertEquals(expectedValue.getHl(), hl);
                    Assert.assertEquals(expectedValue.getLh(), lh);
                    Assert.assertEquals(expectedValue.getLl(), ll);
                } else {
                    Assert.fail("Expected casting to fail");
                }
            }

            @Override
            public void putInt(int col, int value) {
                Assert.assertEquals(ColumnType.DECIMAL32, toTag);
                if (expectedValue != null) {
                    Assert.assertEquals((int) (expectedValue.isNull() ? Integer.MIN_VALUE : expectedValue.getLl()), value);
                } else {
                    Assert.fail("Expected casting to fail");
                }
            }

            @Override
            public void putLong(int col, long value) {
                Assert.assertEquals(ColumnType.DECIMAL64, toTag);
                if (expectedValue != null) {
                    Assert.assertEquals(expectedValue.isNull() ? Long.MIN_VALUE : expectedValue.getLl(), value);
                } else {
                    Assert.fail("Expected casting to fail");
                }
            }

            @Override
            public void putShort(int col, short value) {
                Assert.assertEquals(ColumnType.DECIMAL16, toTag);
                if (expectedValue != null) {
                    Assert.assertEquals((short) (expectedValue.isNull() ? Short.MIN_VALUE : expectedValue.getLl()), value);
                } else {
                    Assert.fail("Expected casting to fail");
                }
            }
        };
    }

    private static @NotNull Record getLongRecord(int fromType, long value) {
        return new Record() {
            @Override
            public byte getByte(int col) {
                Assert.assertEquals(ColumnType.BYTE, fromType);
                return (byte) value;
            }

            @Override
            public int getInt(int col) {
                Assert.assertEquals(ColumnType.INT, fromType);
                return (int) value;
            }

            @Override
            public long getLong(int col) {
                Assert.assertEquals(ColumnType.LONG, fromType);
                return value;
            }

            @Override
            public short getShort(int col) {
                Assert.assertEquals(ColumnType.SHORT, fromType);
                return (short) value;
            }
        };
    }

    private RecordToRowCopier generateCopier(int fromType, int toType) {
        ArrayColumnTypes from = new ArrayColumnTypes();
        from.add(fromType);
        GenericRecordMetadata to = new GenericRecordMetadata();
        TableColumnMetadata toCol = new TableColumnMetadata("x", toType);
        to.add(toCol);
        ListColumnFilter mapping = new ListColumnFilter();
        mapping.add(1);
        return RecordToRowCopierUtils.generateCopier(new BytecodeAssembler(), from, to, mapping);
    }

    private void generateValue(Decimal256 d, int precision, int scale) {
        String maxValue = "98765432109876543210987654321098765432109876543210987654321098765432109876543210";
        BigDecimal value = new BigDecimal(maxValue.substring(0, precision));
        Decimal256 v = Decimal256.fromBigDecimal(value);
        d.of(v.getHh(), v.getHl(), v.getLh(), v.getLl(), scale);
    }

    private TableWriter.Row getIntAsserter(int expected) {
        return new RowAsserter() {
            @Override
            public void putInt(int col, int value) {
                Assert.assertEquals(expected, value);
            }
        };
    }

    private TableWriter.Row getLongAsserter(long expected) {
        return new RowAsserter() {
            @Override
            public void putLong(int col, long value) {
                Assert.assertEquals(expected, value);
            }
        };
    }

    private void testDecimalCast(BytecodeAssembler asm, int fromPrecision, int toPrecision, int toScale, Decimal256 value, boolean fitInTargetType) {
        int fromType = ColumnType.getDecimalType(fromPrecision, value.getScale());
        int toType = ColumnType.getDecimalType(toPrecision, toScale);
        ArrayColumnTypes from = new ArrayColumnTypes();
        from.add(fromType);
        GenericRecordMetadata to = new GenericRecordMetadata();
        TableColumnMetadata toCol = new TableColumnMetadata("x", toType);
        to.add(toCol);
        ListColumnFilter mapping = new ListColumnFilter();
        mapping.add(1);
        RecordToRowCopier copier = RecordToRowCopierUtils.generateCopier(asm, from, to, mapping);

        Decimal256 expectedValue;
        if (fitInTargetType) {
            expectedValue = new Decimal256();
            expectedValue.copyFrom(value);
            expectedValue.rescale(toScale);
        } else {
            expectedValue = null;
        }

        Record rec = getDecimalRecord(fromType, value);
        TableWriter.Row row = getDecimalRow(toType, expectedValue);
        try {
            copier.copy(sqlExecutionContext, rec, row);
            if (!fitInTargetType) {
                Assert.fail(String.format("Expected cast to fail from (%s - p:%s - s:%s) to (%s - p:%s - s:%s) for %s",
                        ColumnType.nameOf(ColumnType.tagOf(fromType)), ColumnType.getDecimalPrecision(fromType), ColumnType.getDecimalScale(fromType),
                        ColumnType.nameOf(ColumnType.tagOf(toType)), ColumnType.getDecimalPrecision(toType), ColumnType.getDecimalScale(toType),
                        value));
            }
        } catch (ImplicitCastException e) {
            if (fitInTargetType) {
                throw e;
            }
        } catch (AssertionError ignored) {
            Assert.fail(String.format("Cast failed from (%s - p:%s - s:%s) to (%s - p:%s - s:%s) for %s",
                    ColumnType.nameOf(ColumnType.tagOf(fromType)), ColumnType.getDecimalPrecision(fromType), ColumnType.getDecimalScale(fromType),
                    ColumnType.nameOf(ColumnType.tagOf(toType)), ColumnType.getDecimalPrecision(toType), ColumnType.getDecimalScale(toType),
                    value));
        }
    }

    private static class RowAsserter implements TableWriter.Row {
        @Override
        public void append() {
            Assert.fail("Unexpected call to append");
        }

        @Override
        public void cancel() {
            Assert.fail("Unexpected call to cancel");
        }

        @Override
        public void putArray(int columnIndex, @NotNull ArrayView array) {
            Assert.fail("Unexpected call to putArray");
        }

        @Override
        public void putBin(int columnIndex, long address, long len) {
            Assert.fail("Unexpected call to putBin");
        }

        @Override
        public void putBin(int columnIndex, BinarySequence sequence) {
            Assert.fail("Unexpected call to putBin");
        }

        @Override
        public void putBool(int columnIndex, boolean value) {
            Assert.fail("Unexpected call to putBool");
        }

        @Override
        public void putByte(int columnIndex, byte value) {
            Assert.fail("Unexpected call to putByte");
        }

        @Override
        public void putChar(int columnIndex, char value) {
            Assert.fail("Unexpected call to putChar");
        }

        @Override
        public void putDate(int columnIndex, long value) {
            Assert.fail("Unexpected call to putDate");
        }

        @Override
        public void putDecimal(int columnIndex, Decimal256 value) {
            Assert.fail("Unexpected call to putDecimal");
        }

        @Override
        public void putDecimal128(int columnIndex, long high, long low) {
            Assert.fail("Unexpected call to putDecimal128");
        }

        @Override
        public void putDecimal256(int columnIndex, long hh, long hl, long lh, long ll) {
            Assert.fail("Unexpected call to putDecimal256");
        }

        @Override
        public void putDecimalStr(int columnIndex, CharSequence cs, Decimal256 decimal) {
            Assert.fail("Unexpected call to putDecimalStr");
        }

        @Override
        public void putDouble(int columnIndex, double value) {
            Assert.fail("Unexpected call to putDouble");
        }

        @Override
        public void putFloat(int columnIndex, float value) {
            Assert.fail("Unexpected call to putFloat");
        }

        @Override
        public void putGeoHash(int columnIndex, long value) {
            Assert.fail("Unexpected call to putGeoHash");
        }

        @Override
        public void putGeoHashDeg(int columnIndex, double lat, double lon) {
            Assert.fail("Unexpected call to putGeoHashDeg");
        }

        @Override
        public void putGeoStr(int columnIndex, CharSequence value) {
            Assert.fail("Unexpected call to putGeoStr");
        }

        @Override
        public void putGeoVarchar(int columnIndex, Utf8Sequence value) {
            Assert.fail("Unexpected call to putGeoVarchar");
        }

        @Override
        public void putIPv4(int columnIndex, int value) {
            Assert.fail("Unexpected call to putIPv4");
        }

        @Override
        public void putInt(int columnIndex, int value) {
            Assert.fail("Unexpected call to putInt");
        }

        @Override
        public void putLong(int columnIndex, long value) {
            Assert.fail("Unexpected call to putLong");
        }

        @Override
        public void putLong128(int columnIndex, long lo, long hi) {
            Assert.fail("Unexpected call to putLong128");
        }

        @Override
        public void putLong256(int columnIndex, long l0, long l1, long l2, long l3) {
            Assert.fail("Unexpected call to putLong256");
        }

        @Override
        public void putLong256(int columnIndex, Long256 value) {
            Assert.fail("Unexpected call to putLong256");
        }

        @Override
        public void putLong256(int columnIndex, CharSequence hexString) {
            Assert.fail("Unexpected call to putLong256");
        }

        @Override
        public void putLong256(int columnIndex, @NotNull CharSequence hexString, int start, int end) {
            Assert.fail("Unexpected call to putLong256");
        }

        @Override
        public void putLong256Utf8(int columnIndex, DirectUtf8Sequence hexString) {
            Assert.fail("Unexpected call to putLong256Utf8");
        }

        @Override
        public void putShort(int columnIndex, short value) {
            Assert.fail("Unexpected call to putShort");
        }

        @Override
        public void putStr(int columnIndex, CharSequence value) {
            Assert.fail("Unexpected call to putStr");
        }

        @Override
        public void putStr(int columnIndex, char value) {
            Assert.fail("Unexpected call to putStr");
        }

        @Override
        public void putStr(int columnIndex, CharSequence value, int pos, int len) {
            Assert.fail("Unexpected call to putStr");
        }

        @Override
        public void putStrUtf8(int columnIndex, DirectUtf8Sequence value) {
            Assert.fail("Unexpected call to putStrUtf8");
        }

        @Override
        public void putSym(int columnIndex, CharSequence value) {
            Assert.fail("Unexpected call to putSym");
        }

        @Override
        public void putSym(int columnIndex, char value) {
            Assert.fail("Unexpected call to putSym");
        }

        @Override
        public void putSymIndex(int columnIndex, int key) {
            Assert.fail("Unexpected call to putSymIndex");
        }

        @Override
        public void putSymUtf8(int columnIndex, DirectUtf8Sequence value) {
            Assert.fail("Unexpected call to putSymUtf8");
        }

        @Override
        public void putTimestamp(int columnIndex, long value) {
            Assert.fail("Unexpected call to putTimestamp");
        }

        @Override
        public void putUuid(int columnIndex, CharSequence uuid) {
            Assert.fail("Unexpected call to putUuid");
        }

        @Override
        public void putUuidUtf8(int columnIndex, Utf8Sequence uuid) {
            Assert.fail("Unexpected call to putUuidUtf8");
        }

        @Override
        public void putVarchar(int columnIndex, char value) {
            Assert.fail("Unexpected call to putVarchar");
        }

        @Override
        public void putVarchar(int columnIndex, Utf8Sequence value) {
            Assert.fail("Unexpected call to putVarchar");
        }
    }
}
