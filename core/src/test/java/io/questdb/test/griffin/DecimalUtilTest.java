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

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.vm.MemoryCARWImpl;
import io.questdb.cairo.vm.api.MemoryCR;
import io.questdb.griffin.DecimalUtil;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.engine.functions.constants.ByteConstant;
import io.questdb.griffin.engine.functions.constants.ConstantFunction;
import io.questdb.griffin.engine.functions.constants.DateConstant;
import io.questdb.griffin.engine.functions.constants.Decimal128Constant;
import io.questdb.griffin.engine.functions.constants.Decimal16Constant;
import io.questdb.griffin.engine.functions.constants.Decimal256Constant;
import io.questdb.griffin.engine.functions.constants.Decimal32Constant;
import io.questdb.griffin.engine.functions.constants.Decimal64Constant;
import io.questdb.griffin.engine.functions.constants.Decimal8Constant;
import io.questdb.griffin.engine.functions.constants.IntConstant;
import io.questdb.griffin.engine.functions.constants.LongConstant;
import io.questdb.griffin.engine.functions.constants.ShortConstant;
import io.questdb.griffin.engine.functions.constants.TimestampConstant;
import io.questdb.griffin.engine.functions.decimal.Decimal8Function;
import io.questdb.std.Decimal128;
import io.questdb.std.Decimal256;
import io.questdb.std.Decimal64;
import io.questdb.std.Decimals;
import io.questdb.std.MemoryTag;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.Assert;
import org.junit.Test;

public class DecimalUtilTest extends AbstractCairoTest {
    private final Decimal128 decimal128 = new Decimal128();
    private final Decimal256 decimal256 = new Decimal256();

    public static @NotNull Record getDecimalRecord(int fromType, Decimal256 value) {
        int fromTag = ColumnType.tagOf(fromType);
        return new Record() {
            @Override
            public void getDecimal128(int col, Decimal128 decimal128) {
                Assert.assertEquals(ColumnType.DECIMAL128, fromTag);
                if (value.isNull()) {
                    decimal128.ofRawNull();
                } else {
                    decimal128.ofRaw(
                            value.getLh(),
                            value.getLl()
                    );
                }
            }

            @Override
            public short getDecimal16(int col) {
                Assert.assertEquals(ColumnType.DECIMAL16, fromTag);
                return value.isNull() ? Short.MIN_VALUE : (short) value.getLl();
            }

            @Override
            public void getDecimal256(int col, Decimal256 decimal256) {
                Assert.assertEquals(ColumnType.DECIMAL256, fromTag);
                decimal256.copyRaw(value);
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

    public static @NotNull TableWriter.Row getRowAsserter(int columnType, int columnIndex, @Nullable Decimal256 expectedValue) {
        short toTag = ColumnType.tagOf(columnType);
        return new RowAsserter() {
            @Override
            public void putByte(int col, byte value) {
                Assert.assertEquals(columnIndex, col);
                Assert.assertEquals(ColumnType.DECIMAL8, toTag);
                if (expectedValue != null) {
                    Assert.assertEquals((byte) (expectedValue.isNull() ? Decimals.DECIMAL8_NULL : expectedValue.getLl()), value);
                } else {
                    Assert.fail("Expected casting to fail");
                }
            }

            @Override
            public void putDecimal128(int col, long high, long low) {
                Assert.assertEquals(columnIndex, col);
                Assert.assertEquals(ColumnType.DECIMAL128, toTag);
                if (expectedValue != null) {
                    Assert.assertEquals(expectedValue.isNull() ? Decimals.DECIMAL128_HI_NULL : expectedValue.getLh(), high);
                    Assert.assertEquals(expectedValue.getLl(), low);
                } else {
                    Assert.fail("Expected casting to fail");
                }
            }

            @Override
            public void putDecimal256(int col, long hh, long hl, long lh, long ll) {
                Assert.assertEquals(columnIndex, col);
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
                Assert.assertEquals(columnIndex, col);
                Assert.assertEquals(ColumnType.DECIMAL32, toTag);
                if (expectedValue != null) {
                    Assert.assertEquals((int) (expectedValue.isNull() ? Decimals.DECIMAL32_NULL : expectedValue.getLl()), value);
                } else {
                    Assert.fail("Expected casting to fail");
                }
            }

            @Override
            public void putLong(int col, long value) {
                Assert.assertEquals(columnIndex, col);
                Assert.assertEquals(ColumnType.DECIMAL64, toTag);
                if (expectedValue != null) {
                    Assert.assertEquals(expectedValue.isNull() ? Decimals.DECIMAL64_NULL : expectedValue.getLl(), value);
                } else {
                    Assert.fail("Expected casting to fail");
                }
            }

            @Override
            public void putShort(int col, short value) {
                Assert.assertEquals(columnIndex, col);
                Assert.assertEquals(ColumnType.DECIMAL16, toTag);
                if (expectedValue != null) {
                    Assert.assertEquals((short) (expectedValue.isNull() ? Decimals.DECIMAL16_NULL : expectedValue.getLl()), value);
                } else {
                    Assert.fail("Expected casting to fail");
                }
            }
        };
    }

    @Test
    public void testCreateDecimalConstantDecimal128() {
        ConstantFunction result = DecimalUtil.createDecimalConstant(0, 0, 123L, 456L, 20, 0);
        Assert.assertTrue(result instanceof Decimal128Constant);
    }

    @Test
    public void testCreateDecimalConstantDecimal16() {
        ConstantFunction result = DecimalUtil.createDecimalConstant(0, 0, 0, 9999, 4, 0);
        Assert.assertTrue(result instanceof Decimal16Constant);
        Assert.assertEquals(9999, result.getDecimal16(null));
    }

    @Test
    public void testCreateDecimalConstantDecimal256() {
        ConstantFunction result = DecimalUtil.createDecimalConstant(1L, 2L, 3L, 4L, 40, 0);
        Assert.assertTrue(result instanceof Decimal256Constant);
    }

    @Test
    public void testCreateDecimalConstantDecimal32() {
        ConstantFunction result = DecimalUtil.createDecimalConstant(0, 0, 0, 123456789, 9, 0);
        Assert.assertTrue(result instanceof Decimal32Constant);
        Assert.assertEquals(123456789, result.getDecimal32(null));
    }

    @Test
    public void testCreateDecimalConstantDecimal64() {
        ConstantFunction result = DecimalUtil.createDecimalConstant(0, 0, 0, 1234567890123456L, 16, 0);
        Assert.assertTrue(result instanceof Decimal64Constant);
        Assert.assertEquals(1234567890123456L, result.getDecimal64(null));
    }

    @Test
    public void testCreateDecimalConstantDecimal8() {
        ConstantFunction result = DecimalUtil.createDecimalConstant(0, 0, 0, 123, 2, 0);
        Assert.assertTrue(result instanceof Decimal8Constant);
        Assert.assertEquals(123, result.getDecimal8(null));
    }

    @Test
    public void testCreateNullDecimal128Constant() {
        try (Function func = DecimalUtil.createNullDecimalConstant(30, 15)) {
            func.getDecimal128(null, decimal128);
            Assert.assertEquals(Decimals.DECIMAL128_HI_NULL, decimal128.getHigh());
            Assert.assertEquals(Decimals.DECIMAL128_LO_NULL, decimal128.getLow());
            Assert.assertTrue(func.isNullConstant());
            Assert.assertEquals(30, ColumnType.getDecimalPrecision(func.getType()));
            Assert.assertEquals(15, ColumnType.getDecimalScale(func.getType()));
        }
    }

    @Test
    public void testCreateNullDecimal16Constant() {
        try (Function func = DecimalUtil.createNullDecimalConstant(4, 1)) {
            Assert.assertEquals(Decimals.DECIMAL16_NULL, func.getDecimal16(null));
            Assert.assertTrue(func.isNullConstant());
            Assert.assertEquals(4, ColumnType.getDecimalPrecision(func.getType()));
            Assert.assertEquals(1, ColumnType.getDecimalScale(func.getType()));
        }
    }

    @Test
    public void testCreateNullDecimal256Constant() {
        try (Function func = DecimalUtil.createNullDecimalConstant(72, 25)) {
            func.getDecimal256(null, decimal256);
            Assert.assertEquals(Decimals.DECIMAL256_HH_NULL, decimal256.getHh());
            Assert.assertEquals(Decimals.DECIMAL256_HL_NULL, decimal256.getHl());
            Assert.assertEquals(Decimals.DECIMAL256_LH_NULL, decimal256.getLh());
            Assert.assertEquals(Decimals.DECIMAL256_LL_NULL, decimal256.getLl());
            Assert.assertTrue(func.isNullConstant());
            Assert.assertEquals(72, ColumnType.getDecimalPrecision(func.getType()));
            Assert.assertEquals(25, ColumnType.getDecimalScale(func.getType()));
        }
    }

    @Test
    public void testCreateNullDecimal32Constant() {
        try (Function func = DecimalUtil.createNullDecimalConstant(8, 3)) {
            Assert.assertEquals(Decimals.DECIMAL32_NULL, func.getDecimal32(null));
            Assert.assertTrue(func.isNullConstant());
            Assert.assertEquals(8, ColumnType.getDecimalPrecision(func.getType()));
            Assert.assertEquals(3, ColumnType.getDecimalScale(func.getType()));
        }
    }

    @Test
    public void testCreateNullDecimal64Constant() {
        try (Function func = DecimalUtil.createNullDecimalConstant(16, 5)) {
            Assert.assertEquals(Decimals.DECIMAL64_NULL, func.getDecimal64(null));
            Assert.assertTrue(func.isNullConstant());
            Assert.assertEquals(16, ColumnType.getDecimalPrecision(func.getType()));
            Assert.assertEquals(5, ColumnType.getDecimalScale(func.getType()));
        }
    }

    @Test
    public void testCreateNullDecimal8Constant() {
        try (Function func = DecimalUtil.createNullDecimalConstant(2, 0)) {
            Assert.assertEquals(Decimals.DECIMAL8_NULL, func.getDecimal8(null));
            Assert.assertTrue(func.isNullConstant());
            Assert.assertEquals(2, ColumnType.getDecimalPrecision(func.getType()));
            Assert.assertEquals(0, ColumnType.getDecimalScale(func.getType()));
        }
    }

    @Test
    public void testGetDecimal128() {
        Assert.assertTrue(DecimalUtil.getDecimal(sqlExecutionContext, Decimal128.MAX_PRECISION) instanceof Decimal128);
        Assert.assertTrue(DecimalUtil.getDecimal(sqlExecutionContext, 19) instanceof Decimal128);
    }

    @Test
    public void testGetDecimal256() {
        Assert.assertTrue(DecimalUtil.getDecimal(sqlExecutionContext, Decimals.MAX_PRECISION) instanceof Decimal256);
        Assert.assertTrue(DecimalUtil.getDecimal(sqlExecutionContext, 39) instanceof Decimal256);
    }

    @Test
    public void testGetDecimal64() {
        Assert.assertTrue(DecimalUtil.getDecimal(sqlExecutionContext, Decimal64.MAX_PRECISION) instanceof Decimal64);
        Assert.assertTrue(DecimalUtil.getDecimal(sqlExecutionContext, 8) instanceof Decimal64);
        Assert.assertTrue(DecimalUtil.getDecimal(sqlExecutionContext, 4) instanceof Decimal64);
        Assert.assertTrue(DecimalUtil.getDecimal(sqlExecutionContext, 2) instanceof Decimal64);
    }

    @Test
    public void testGetTypePrecisionScaleByte() {
        int result = DecimalUtil.getTypePrecisionScale(ColumnType.BYTE);
        Assert.assertEquals(3, io.questdb.std.Numbers.decodeLowShort(result));  // precision
        Assert.assertEquals(0, io.questdb.std.Numbers.decodeHighShort(result)); // scale
    }

    @Test
    public void testGetTypePrecisionScaleDate() {
        int result = DecimalUtil.getTypePrecisionScale(ColumnType.DATE);
        Assert.assertEquals(19, io.questdb.std.Numbers.decodeLowShort(result)); // precision
        Assert.assertEquals(0, io.questdb.std.Numbers.decodeHighShort(result));  // scale
    }

    @Test
    public void testGetTypePrecisionScaleDecimalEdgeCases() {
        // Test maximum precision and scale
        int maxType = ColumnType.getDecimalType(76, 76);
        int result = DecimalUtil.getTypePrecisionScale(maxType);
        Assert.assertEquals(76, io.questdb.std.Numbers.decodeLowShort(result));  // precision
        Assert.assertEquals(76, io.questdb.std.Numbers.decodeHighShort(result)); // scale

        // Test minimum precision and scale
        int minType = ColumnType.getDecimalType(1, 0);
        result = DecimalUtil.getTypePrecisionScale(minType);
        Assert.assertEquals(1, io.questdb.std.Numbers.decodeLowShort(result));  // precision
        Assert.assertEquals(0, io.questdb.std.Numbers.decodeHighShort(result)); // scale

        // Test precision without scale
        int noScaleType = ColumnType.getDecimalType(38, 0);
        result = DecimalUtil.getTypePrecisionScale(noScaleType);
        Assert.assertEquals(38, io.questdb.std.Numbers.decodeLowShort(result));  // precision
        Assert.assertEquals(0, io.questdb.std.Numbers.decodeHighShort(result));  // scale
    }

    @Test
    public void testGetTypePrecisionScaleDecimalTypes() {
        // Test DECIMAL8 (precision 2, scale 1)
        int decimal8Type = ColumnType.getDecimalType(2, 1);
        int result = DecimalUtil.getTypePrecisionScale(decimal8Type);
        Assert.assertEquals(2, io.questdb.std.Numbers.decodeLowShort(result));  // precision
        Assert.assertEquals(1, io.questdb.std.Numbers.decodeHighShort(result)); // scale

        // Test DECIMAL16 (precision 4, scale 2)
        int decimal16Type = ColumnType.getDecimalType(4, 2);
        result = DecimalUtil.getTypePrecisionScale(decimal16Type);
        Assert.assertEquals(4, io.questdb.std.Numbers.decodeLowShort(result));  // precision
        Assert.assertEquals(2, io.questdb.std.Numbers.decodeHighShort(result)); // scale

        // Test DECIMAL32 (precision 9, scale 5)
        int decimal32Type = ColumnType.getDecimalType(9, 5);
        result = DecimalUtil.getTypePrecisionScale(decimal32Type);
        Assert.assertEquals(9, io.questdb.std.Numbers.decodeLowShort(result));  // precision
        Assert.assertEquals(5, io.questdb.std.Numbers.decodeHighShort(result)); // scale

        // Test DECIMAL64 (precision 18, scale 10)
        int decimal64Type = ColumnType.getDecimalType(18, 10);
        result = DecimalUtil.getTypePrecisionScale(decimal64Type);
        Assert.assertEquals(18, io.questdb.std.Numbers.decodeLowShort(result));  // precision
        Assert.assertEquals(10, io.questdb.std.Numbers.decodeHighShort(result)); // scale

        // Test DECIMAL128 (precision 38, scale 20)
        int decimal128Type = ColumnType.getDecimalType(38, 20);
        result = DecimalUtil.getTypePrecisionScale(decimal128Type);
        Assert.assertEquals(38, io.questdb.std.Numbers.decodeLowShort(result));  // precision
        Assert.assertEquals(20, io.questdb.std.Numbers.decodeHighShort(result)); // scale

        // Test DECIMAL256 (precision 76, scale 40)
        int decimal256Type = ColumnType.getDecimalType(76, 40);
        result = DecimalUtil.getTypePrecisionScale(decimal256Type);
        Assert.assertEquals(76, io.questdb.std.Numbers.decodeLowShort(result));  // precision
        Assert.assertEquals(40, io.questdb.std.Numbers.decodeHighShort(result)); // scale
    }

    @Test
    public void testGetTypePrecisionScaleInt() {
        int result = DecimalUtil.getTypePrecisionScale(ColumnType.INT);
        Assert.assertEquals(10, io.questdb.std.Numbers.decodeLowShort(result)); // precision
        Assert.assertEquals(0, io.questdb.std.Numbers.decodeHighShort(result));  // scale
    }

    @Test
    public void testGetTypePrecisionScaleLong() {
        int result = DecimalUtil.getTypePrecisionScale(ColumnType.LONG);
        Assert.assertEquals(19, io.questdb.std.Numbers.decodeLowShort(result)); // precision
        Assert.assertEquals(0, io.questdb.std.Numbers.decodeHighShort(result));  // scale
    }

    @Test
    public void testGetTypePrecisionScaleShort() {
        int result = DecimalUtil.getTypePrecisionScale(ColumnType.SHORT);
        Assert.assertEquals(5, io.questdb.std.Numbers.decodeLowShort(result));  // precision
        Assert.assertEquals(0, io.questdb.std.Numbers.decodeHighShort(result)); // scale
    }

    @Test
    public void testGetTypePrecisionScaleTimestamp() {
        int result = DecimalUtil.getTypePrecisionScale(ColumnType.TIMESTAMP);
        Assert.assertEquals(19, io.questdb.std.Numbers.decodeLowShort(result)); // precision
        Assert.assertEquals(0, io.questdb.std.Numbers.decodeHighShort(result));  // scale
    }

    @Test
    public void testGetTypePrecisionScaleUnsupportedTypes() {
        // Test BOOLEAN - should return 0
        Assert.assertEquals(0, DecimalUtil.getTypePrecisionScale(ColumnType.BOOLEAN));

        // Test STRING - should return 0
        Assert.assertEquals(0, DecimalUtil.getTypePrecisionScale(ColumnType.STRING));

        // Test SYMBOL - should return 0
        Assert.assertEquals(0, DecimalUtil.getTypePrecisionScale(ColumnType.SYMBOL));

        // Test DOUBLE - should return 0
        Assert.assertEquals(0, DecimalUtil.getTypePrecisionScale(ColumnType.DOUBLE));

        // Test FLOAT - should return 0
        Assert.assertEquals(0, DecimalUtil.getTypePrecisionScale(ColumnType.FLOAT));

        // Test BINARY - should return 0
        Assert.assertEquals(0, DecimalUtil.getTypePrecisionScale(ColumnType.BINARY));

        // Test CHAR - should return 0
        Assert.assertEquals(0, DecimalUtil.getTypePrecisionScale(ColumnType.CHAR));

        // Test UUID - should return 0
        Assert.assertEquals(0, DecimalUtil.getTypePrecisionScale(ColumnType.UUID));

        // Test VARCHAR - should return 0
        Assert.assertEquals(0, DecimalUtil.getTypePrecisionScale(ColumnType.VARCHAR));

        // Test INTERVAL - should return 0
        Assert.assertEquals(0, DecimalUtil.getTypePrecisionScale(ColumnType.INTERVAL));
    }

    @Test
    public void testLoadDecimal128Record() {
        assertLoadNullDecimal(new Decimal256(0, 0, Decimals.DECIMAL128_HI_NULL, Decimals.DECIMAL128_LO_NULL, 0), ColumnType.getDecimalType(38, 0));
        assertLoadDecimal(Decimal256.fromLong(123456789012345L, 1), ColumnType.getDecimalType(38, 1));
        assertLoadDecimal(Decimal256.fromLong(-123456789012345L, 2), ColumnType.getDecimalType(38, 2));
        assertLoadDecimal(new Decimal256(
                        0,
                        0,
                        Decimal128.MAX_VALUE.getHigh(),
                        Decimal128.MAX_VALUE.getLow(),
                        3
                ),
                ColumnType.getDecimalType(38, 3)
        );
        assertLoadDecimal(new Decimal256(
                        -1,
                        -1,
                        Decimal128.MIN_VALUE.getHigh(),
                        Decimal128.MIN_VALUE.getLow(),
                        3
                ),
                ColumnType.getDecimalType(38, 3)
        );
    }

    @Test
    public void testLoadDecimal16Record() {
        assertLoadNullDecimal(Decimal256.fromLong(Decimals.DECIMAL16_NULL, 0), ColumnType.getDecimalType(4, 0));
        assertLoadDecimal(Decimal256.fromLong(1234, 1), ColumnType.getDecimalType(4, 1));
        assertLoadDecimal(Decimal256.fromLong(-1234, 1), ColumnType.getDecimalType(4, 1));
        assertLoadDecimal(Decimal256.fromLong(9999, 1), ColumnType.getDecimalType(4, 1));
        assertLoadDecimal(Decimal256.fromLong(-9999, 1), ColumnType.getDecimalType(4, 1));
    }

    @Test
    public void testLoadDecimal256Record() {
        assertLoadNullDecimal(Decimal256.NULL_VALUE, ColumnType.getDecimalType(76, 0));
        assertLoadDecimal(Decimal256.fromLong(123456789012345L, 1), ColumnType.getDecimalType(76, 1));
        assertLoadDecimal(Decimal256.fromLong(-123456789012345L, 2), ColumnType.getDecimalType(76, 2));
        assertLoadDecimal(Decimal256.MIN_VALUE, ColumnType.getDecimalType(76, 0));
        assertLoadDecimal(Decimal256.MAX_VALUE, ColumnType.getDecimalType(76, 0));
    }

    @Test
    public void testLoadDecimal32Record() {
        assertLoadNullDecimal(Decimal256.fromLong(Decimals.DECIMAL32_NULL, 0), ColumnType.getDecimalType(8, 0));
        assertLoadDecimal(Decimal256.fromLong(12345678, 1), ColumnType.getDecimalType(8, 1));
        assertLoadDecimal(Decimal256.fromLong(-12345678, 2), ColumnType.getDecimalType(8, 2));
        assertLoadDecimal(Decimal256.fromLong(99999999, 3), ColumnType.getDecimalType(8, 3));
        assertLoadDecimal(Decimal256.fromLong(-99999999, 4), ColumnType.getDecimalType(8, 4));
    }

    @Test
    public void testLoadDecimal64Record() {
        assertLoadNullDecimal(Decimal256.fromLong(Decimals.DECIMAL64_NULL, 0), ColumnType.getDecimalType(18, 0));
        assertLoadDecimal(Decimal256.fromLong(123456789012345L, 1), ColumnType.getDecimalType(18, 1));
        assertLoadDecimal(Decimal256.fromLong(-123456789012345L, 2), ColumnType.getDecimalType(18, 2));
        assertLoadDecimal(Decimal256.fromLong(9999999999999999L, 3), ColumnType.getDecimalType(18, 3));
        assertLoadDecimal(Decimal256.fromLong(-9999999999999999L, 4), ColumnType.getDecimalType(18, 4));
    }

    @Test
    public void testLoadDecimal8Record() {
        assertLoadNullDecimal(Decimal256.fromLong(Decimals.DECIMAL8_NULL, 0), ColumnType.getDecimalType(2, 0));
        assertLoadDecimal(Decimal256.fromLong(12, 1), ColumnType.getDecimalType(2, 1));
        assertLoadDecimal(Decimal256.fromLong(-12, 1), ColumnType.getDecimalType(2, 1));
        assertLoadDecimal(Decimal256.fromLong(99, 1), ColumnType.getDecimalType(2, 1));
        assertLoadDecimal(Decimal256.fromLong(-99, 1), ColumnType.getDecimalType(2, 1));
    }

    @Test
    public void testLoadDecimalsFromByteConstant() {
        assertLoadDecimal256("0", ByteConstant.newInstance((byte) 0));
        assertLoadDecimal256("127", ByteConstant.newInstance((byte) 127));
        assertLoadDecimal256("-128", ByteConstant.newInstance((byte) -128));
    }

    @Test
    public void testLoadDecimalsFromDateConstant() {
        assertLoadDecimal256("0", DateConstant.newInstance(0));
        assertLoadDecimal256("1234567890", DateConstant.newInstance(1234567890));
        assertLoadDecimal256("9223372036854775807", DateConstant.newInstance(Long.MAX_VALUE));
        assertLoadDecimal256("-9223372036854775807", DateConstant.newInstance(Long.MIN_VALUE + 1));
    }

    @Test
    public void testLoadDecimalsFromDateConstantNull() {
        assertLoadDecimal256("", DateConstant.NULL);
    }

    @Test
    public void testLoadDecimalsFromDecimal128Constant() {
        // Decimal128: 19-38 digit precision
        assertLoadDecimal256("0", new Decimal128Constant(0L, 0L, ColumnType.getDecimalType(38, 0)));
        assertLoadDecimal256("1234567890123456789", new Decimal128Constant(0L, 1234567890123456789L, ColumnType.getDecimalType(38, 0)));
        assertLoadDecimal256("-1234567890123456789", new Decimal128Constant(-1L, -1234567890123456789L, ColumnType.getDecimalType(20, 0)));
        assertLoadDecimal256("1234567890.123456789", new Decimal128Constant(0L, 1234567890123456789L, ColumnType.getDecimalType(38, 9)));
        assertLoadDecimal256("", new Decimal128Constant(Decimals.DECIMAL128_HI_NULL, Decimals.DECIMAL128_LO_NULL, ColumnType.getDecimalType(38, 0)));
    }

    @Test
    public void testLoadDecimalsFromDecimal16Constant() {
        assertLoadDecimal256("0", new Decimal16Constant((short) 0, ColumnType.getDecimalType(4, 0)));
        assertLoadDecimal256("9999", new Decimal16Constant((short) 9999, ColumnType.getDecimalType(4, 0)));
        assertLoadDecimal256("-9999", new Decimal16Constant((short) -9999, ColumnType.getDecimalType(4, 0)));
        assertLoadDecimal256("123.4", new Decimal16Constant((short) 1234, ColumnType.getDecimalType(4, 1)));
        assertLoadDecimal256("1.234", new Decimal16Constant((short) 1234, ColumnType.getDecimalType(4, 3)));
        assertLoadDecimal256("", new Decimal16Constant(Decimals.DECIMAL16_NULL, ColumnType.getDecimalType(3, 0)));
    }

    @Test
    public void testLoadDecimalsFromDecimal256Constant() {
        // Decimal256: 39-76 digit precision
        assertLoadDecimal256("0", new Decimal256Constant(0L, 0L, 0L, 0L, ColumnType.getDecimalType(76, 0)));
        assertLoadDecimal256("1234567890123456789", new Decimal256Constant(0L, 0L, 0L, 1234567890123456789L, ColumnType.getDecimalType(40, 0)));
        assertLoadDecimal256("-1234567890123456789", new Decimal256Constant(-1L, -1L, -1L, -1234567890123456789L, ColumnType.getDecimalType(40, 0)));
        assertLoadDecimal256("0.1234567890123456789", new Decimal256Constant(0L, 0L, 0L, 1234567890123456789L, ColumnType.getDecimalType(40, 19)));
        assertLoadDecimal256("", new Decimal256Constant(Decimals.DECIMAL256_HH_NULL, Decimals.DECIMAL256_HL_NULL, Decimals.DECIMAL256_LH_NULL, Decimals.DECIMAL256_LL_NULL, ColumnType.getDecimalType(76, 0)));
    }

    @Test
    public void testLoadDecimalsFromDecimal32Constant() {
        assertLoadDecimal256("0", new Decimal32Constant(0, ColumnType.getDecimalType(9, 0)));
        assertLoadDecimal256("123456789", new Decimal32Constant(123456789, ColumnType.getDecimalType(9, 0)));
        assertLoadDecimal256("-123456789", new Decimal32Constant(-123456789, ColumnType.getDecimalType(9, 0)));
        assertLoadDecimal256("12345.6789", new Decimal32Constant(123456789, ColumnType.getDecimalType(9, 4)));
        assertLoadDecimal256("1.23456789", new Decimal32Constant(123456789, ColumnType.getDecimalType(9, 8)));
        assertLoadDecimal256("", new Decimal32Constant(Decimals.DECIMAL32_NULL, ColumnType.getDecimalType(9, 0)));
    }

    @Test
    public void testLoadDecimalsFromDecimal64Constant() {
        assertLoadDecimal256("0", new Decimal64Constant(0L, ColumnType.getDecimalType(18, 0)));
        assertLoadDecimal256("123456789012345678", new Decimal64Constant(123456789012345678L, ColumnType.getDecimalType(18, 0)));
        assertLoadDecimal256("-123456789012345678", new Decimal64Constant(-123456789012345678L, ColumnType.getDecimalType(18, 0)));
        assertLoadDecimal256("12345678.9012345678", new Decimal64Constant(123456789012345678L, ColumnType.getDecimalType(18, 10)));
        assertLoadDecimal256("1.23456789012345678", new Decimal64Constant(123456789012345678L, ColumnType.getDecimalType(18, 17)));
        assertLoadDecimal256("", new Decimal64Constant(Decimals.DECIMAL64_NULL, ColumnType.getDecimalType(18, 0)));
    }

    @Test
    public void testLoadDecimalsFromDecimal8Constant() {
        assertLoadDecimal256("0", new Decimal8Constant((byte) 0, ColumnType.getDecimalType(2, 0)));
        assertLoadDecimal256("9", new Decimal8Constant((byte) 9, ColumnType.getDecimalType(1, 0)));
        assertLoadDecimal256("99", new Decimal8Constant((byte) 99, ColumnType.getDecimalType(2, 0)));
        assertLoadDecimal256("-99", new Decimal8Constant((byte) -99, ColumnType.getDecimalType(2, 0)));
        assertLoadDecimal256("1.2", new Decimal8Constant((byte) 12, ColumnType.getDecimalType(2, 1)));
        assertLoadDecimal256("0.12", new Decimal8Constant((byte) 12, ColumnType.getDecimalType(2, 2)));
        assertLoadDecimal256("", new Decimal8Constant(Decimals.DECIMAL8_NULL, ColumnType.getDecimalType(2, 2)));
    }

    @Test
    public void testLoadDecimalsFromIntConstant() {
        assertLoadDecimal256("123", IntConstant.newInstance(123));
        assertLoadDecimal256("987654321", IntConstant.newInstance(987654321));
        assertLoadDecimal256("-3630", IntConstant.newInstance(-3630));
    }

    @Test
    public void testLoadDecimalsFromIntConstantNull() {
        assertLoadDecimal256("", IntConstant.NULL);
    }

    @Test
    public void testLoadDecimalsFromLongConstant() {
        assertLoadDecimal256("0", LongConstant.newInstance(0));
        assertLoadDecimal256("1234567890", LongConstant.newInstance(1234567890));
        assertLoadDecimal256("9223372036854775807", LongConstant.newInstance(Long.MAX_VALUE));
        assertLoadDecimal256("-9223372036854775807", LongConstant.newInstance(Long.MIN_VALUE + 1));
    }

    @Test
    public void testLoadDecimalsFromLongConstantNull() {
        assertLoadDecimal256("", LongConstant.NULL);
    }

    @Test
    public void testLoadDecimalsFromShortConstant() {
        assertLoadDecimal256("0", ShortConstant.newInstance((short) 0));
        assertLoadDecimal256("12345", ShortConstant.newInstance((short) 12345));
        assertLoadDecimal256("32767", ShortConstant.newInstance(Short.MAX_VALUE));
        assertLoadDecimal256("-32768", ShortConstant.newInstance(Short.MIN_VALUE));
    }

    @Test
    public void testLoadDecimalsFromTimestampMicroConstant() {
        assertLoadDecimal256("0", TimestampConstant.newInstance(0, ColumnType.TIMESTAMP_MICRO));
        assertLoadDecimal256("1234567890", TimestampConstant.newInstance(1234567890, ColumnType.TIMESTAMP_MICRO));
        assertLoadDecimal256("9223372036854775807", TimestampConstant.newInstance(Long.MAX_VALUE, ColumnType.TIMESTAMP_MICRO));
        assertLoadDecimal256("-9223372036854775807", TimestampConstant.newInstance(Long.MIN_VALUE + 1, ColumnType.TIMESTAMP_MICRO));
    }

    @Test
    public void testLoadDecimalsFromTimestampMicroConstantNull() {
        assertLoadDecimal256("", TimestampConstant.TIMESTAMP_MICRO_NULL);
    }

    @Test
    public void testLoadDecimalsFromTimestampNanoConstant() {
        assertLoadDecimal256("0", TimestampConstant.newInstance(0, ColumnType.TIMESTAMP_NANO));
        assertLoadDecimal256("1234567890", TimestampConstant.newInstance(1234567890, ColumnType.TIMESTAMP_NANO));
        assertLoadDecimal256("9223372036854775807", TimestampConstant.newInstance(Long.MAX_VALUE, ColumnType.TIMESTAMP_NANO));
        assertLoadDecimal256("-9223372036854775807", TimestampConstant.newInstance(Long.MIN_VALUE + 1, ColumnType.TIMESTAMP_NANO));
    }

    @Test
    public void testLoadDecimalsFromTimestampNanoConstantNull() {
        assertLoadDecimal256("", TimestampConstant.TIMESTAMP_NANO_NULL);
    }

    @Test
    public void testMaybeRescaleDecimalConstantLargerTargetScale() {
        final Decimal256 decimal = new Decimal256();
        final int precision = 20;
        final int scale = 2;
        final int targetPrecision = 15;
        final int targetScale = 5;
        final ConstantFunction constantFunc = DecimalUtil.createDecimalConstant(0, 0, 0, 12345, precision, scale);
        final Function result = DecimalUtil.maybeRescaleDecimalConstant(constantFunc, decimal, decimal128, targetPrecision, targetScale);
        Assert.assertNotSame(constantFunc, result);
        Assert.assertTrue(result.isConstant());
        Assert.assertEquals(precision + targetScale - scale, ColumnType.getDecimalPrecision(result.getType()));
        Assert.assertEquals(targetScale, ColumnType.getDecimalScale(result.getType()));
    }

    @Test
    public void testMaybeRescaleDecimalConstantNonConstantFunction() {
        final Decimal256 decimal = new Decimal256();
        final Function nonConstantFunc = new TestNonConstantDecimalFunction();
        final Function result = DecimalUtil.maybeRescaleDecimalConstant(nonConstantFunc, decimal, decimal128, 10, 5);
        Assert.assertSame(nonConstantFunc, result);
    }

    @Test
    public void testMaybeRescaleDecimalConstantNull() {
        final Decimal256 decimal = new Decimal256();
        final int precision = 13;
        final int scale = 2;
        final int targetPrecision = 20;
        final int targetScale = 5;
        final ConstantFunction constantFunc = new Decimal64Constant(Decimals.DECIMAL64_NULL, ColumnType.getDecimalType(precision, scale));
        final Function result = DecimalUtil.maybeRescaleDecimalConstant(constantFunc, decimal, decimal128, targetPrecision, targetScale);
        Assert.assertNotSame(constantFunc, result);
        Assert.assertTrue(result.isConstant());
        Assert.assertTrue(result.isNullConstant());
        Assert.assertEquals(targetPrecision, ColumnType.getDecimalPrecision(result.getType()));
        Assert.assertEquals(targetScale, ColumnType.getDecimalScale(result.getType()));
    }

    @Test
    public void testMaybeRescaleDecimalConstantSameScale() {
        final Decimal256 decimal = new Decimal256();
        final int precision = 10;
        final int scale = 5;
        final int targetPrecision = 10;
        final int targetScale = 5;
        final ConstantFunction constantFunc = DecimalUtil.createDecimalConstant(0, 0, 0, 12345, precision, scale);
        final Function result = DecimalUtil.maybeRescaleDecimalConstant(constantFunc, decimal, decimal128, targetPrecision, targetScale);
        Assert.assertSame(constantFunc, result);
    }

    @Test
    public void testMaybeRescaleDecimalConstantSmallerTargetScale() {
        final Decimal256 decimal = new Decimal256();
        final int precision = 10;
        final int scale = 5;
        final int targetPrecision = 10;
        final int targetScale = 2;
        final ConstantFunction constantFunc = DecimalUtil.createDecimalConstant(0, 0, 0, 12345, precision, scale);
        final Function result = DecimalUtil.maybeRescaleDecimalConstant(constantFunc, decimal, decimal128, targetPrecision, targetScale);
        Assert.assertSame(constantFunc, result);
    }

    @Test
    public void testParseDecimalConstantComplexDecimalWithMSuffix() throws SqlException {
        ConstantFunction result = DecimalUtil.parseDecimalConstant(0, sqlExecutionContext, "-0012.345M", -1, -1);
        Assert.assertTrue(result instanceof Decimal32Constant);
        Assert.assertEquals(-12345, result.getDecimal32(null));
    }

    @Test
    public void testParseDecimalConstantDecimal128() throws SqlException {
        ConstantFunction result = DecimalUtil.parseDecimalConstant(0, sqlExecutionContext, "12345678901234567890", -1, -1);
        Assert.assertTrue(result instanceof Decimal128Constant);
    }

    @Test
    public void testParseDecimalConstantDecimal16() throws SqlException {
        ConstantFunction result = DecimalUtil.parseDecimalConstant(0, sqlExecutionContext, "9999", -1, -1);
        Assert.assertTrue(result instanceof Decimal16Constant);
        Assert.assertEquals(9999, result.getDecimal16(null));
    }

    @Test
    public void testParseDecimalConstantDecimal256() throws SqlException {
        String largeNumber = "1234567890123456789012345678901234567890";
        ConstantFunction result = DecimalUtil.parseDecimalConstant(0, sqlExecutionContext, largeNumber, -1, -1);
        Assert.assertTrue(result instanceof Decimal256Constant);
    }

    @Test
    public void testParseDecimalConstantDecimal32() throws SqlException {
        ConstantFunction result = DecimalUtil.parseDecimalConstant(0, sqlExecutionContext, "123456789", -1, -1);
        Assert.assertTrue(result instanceof Decimal32Constant);
        Assert.assertEquals(123456789, result.getDecimal32(null));
    }

    @Test
    public void testParseDecimalConstantDecimal64() throws SqlException {
        ConstantFunction result = DecimalUtil.parseDecimalConstant(0, sqlExecutionContext, "1234567890123456", -1, -1);
        Assert.assertTrue(result instanceof Decimal64Constant);
        Assert.assertEquals(1234567890123456L, result.getDecimal64(null));
    }

    @Test
    public void testParseDecimalConstantDecimalAtEnd() throws SqlException {
        ConstantFunction result = DecimalUtil.parseDecimalConstant(0, sqlExecutionContext, "123.", -1, -1);
        Assert.assertTrue(result instanceof Decimal16Constant);
        Assert.assertEquals(123, result.getDecimal16(null));
    }

    @Test
    public void testParseDecimalConstantDecimalAtStart() throws SqlException {
        ConstantFunction result = DecimalUtil.parseDecimalConstant(0, sqlExecutionContext, ".123", -1, -1);
        Assert.assertTrue(result instanceof Decimal16Constant);
        Assert.assertEquals(123, result.getDecimal16(null));
    }

    @Test
    public void testParseDecimalConstantEmptyString() {
        try {
            DecimalUtil.parseDecimalConstant(0, sqlExecutionContext, "", -1, -1);
            Assert.fail("Expected SqlException");
        } catch (SqlException e) {
            TestUtils.assertContains(e.getMessage(), "invalid decimal: empty value");
        }
    }

    @Test
    public void testParseDecimalConstantExceedsMaxPrecision() {
        String tooLarge = "1234567890123456789012345678901234567890123456789012345678901234567890123456789"; // 80 digits
        try {
            DecimalUtil.parseDecimalConstant(0, sqlExecutionContext, tooLarge, -1, -1);
            Assert.fail("Expected SqlException");
        } catch (SqlException e) {
            TestUtils.assertContains(e.getMessage(), "exceeds maximum allowed precision of 76");
        }
    }

    @Test
    public void testParseDecimalConstantExceedsMaxScale() {
        // Create a number with 77 decimal places
        String number = "1." + new String(new char[77]).replace('\0', '1');
        try {
            DecimalUtil.parseDecimalConstant(0, sqlExecutionContext, number, -1, -1);
            Assert.fail("Expected SqlException");
        } catch (SqlException e) {
            TestUtils.assertContains(e.getMessage(), "exceeds maximum allowed scale of 76");
        }
    }

    @Test
    public void testParseDecimalConstantExceedsPrecision() {
        // Try to parse "123456" with precision 5 (should fail)
        try {
            DecimalUtil.parseDecimalConstant(0, sqlExecutionContext, "123456", 5, -1);
            Assert.fail("Expected SqlException");
        } catch (SqlException e) {
            TestUtils.assertContains(e.getMessage(), "decimal '123456' requires precision of 6 but is limited to 5");
        }
    }

    @Test
    public void testParseDecimalConstantExceedsScale() {
        // Try to parse "123.456" with scale 2 (should fail as it has 3 decimal places)
        try {
            DecimalUtil.parseDecimalConstant(0, sqlExecutionContext, "123.456", -1, 2);
            Assert.fail("Expected SqlException");
        } catch (SqlException e) {
            TestUtils.assertContains(e.getMessage(), "decimal '123.456' has 3 decimal places but scale is limited to 2");
        }
    }

    @Test
    public void testParseDecimalConstantIntegerWithScale() throws SqlException {
        // Parse integer "123" with scale 0
        ConstantFunction result = DecimalUtil.parseDecimalConstant(0, sqlExecutionContext, "123", -1, 0);
        Assert.assertTrue(result instanceof Decimal16Constant);
        Assert.assertEquals(123, result.getDecimal16(null));
    }

    @Test
    public void testParseDecimalConstantInvalidCharacter() {
        try {
            DecimalUtil.parseDecimalConstant(0, sqlExecutionContext, "123a45", -1, -1);
            Assert.fail("Expected SqlException");
        } catch (SqlException e) {
            TestUtils.assertContains(e.getMessage(), "contains invalid character 'a'");
        }
    }

    @Test
    public void testParseDecimalConstantMaxPrecisionBoundary() throws SqlException {
        String maxPrecision = new String(new char[76]).replace('\0', '9');
        ConstantFunction result = DecimalUtil.parseDecimalConstant(0, sqlExecutionContext, maxPrecision, -1, -1);
        Assert.assertTrue(result instanceof Decimal256Constant);
    }

    @Test
    public void testParseDecimalConstantMixedLeadingZerosAndSpaces() throws SqlException {
        ConstantFunction result = DecimalUtil.parseDecimalConstant(0, sqlExecutionContext, "  00123.45", -1, -1);
        Assert.assertTrue(result instanceof Decimal32Constant);
        Assert.assertEquals(12345, result.getDecimal32(null));
    }

    @Test
    public void testParseDecimalConstantMultipleDots() {
        try {
            DecimalUtil.parseDecimalConstant(0, sqlExecutionContext, "12.34.56", -1, -1);
            Assert.fail("Expected SqlException");
        } catch (SqlException e) {
            TestUtils.assertContains(e.getMessage(), "contains invalid character '.'");
        }
    }

    @Test
    public void testParseDecimalConstantNegativeWithDecimal() throws SqlException {
        ConstantFunction result = DecimalUtil.parseDecimalConstant(0, sqlExecutionContext, "-123.45", -1, -1);
        Assert.assertTrue(result instanceof Decimal32Constant);
        Assert.assertEquals(-12345, result.getDecimal32(null));
    }

    @Test
    public void testParseDecimalConstantNegativeWithPrecisionAndScale() throws SqlException {
        // Parse "-123.45" with precision 10 and scale 2
        ConstantFunction result = DecimalUtil.parseDecimalConstant(0, sqlExecutionContext, "-123.45", 10, 2);
        Assert.assertTrue(result instanceof Decimal64Constant);
        Assert.assertEquals(-12345, result.getDecimal64(null));
    }

    @Test
    public void testParseDecimalConstantOnlyDecimalPoint() {
        try {
            DecimalUtil.parseDecimalConstant(0, sqlExecutionContext, ".", -1, -1);
            Assert.fail("Expected SqlException");
        } catch (SqlException e) {
            TestUtils.assertContains(e.getMessage(), "invalid decimal: '.' contains no digits");
        }
    }

    @Test
    public void testParseDecimalConstantOnlySign() {
        try {
            DecimalUtil.parseDecimalConstant(0, sqlExecutionContext, "-", -1, -1);
            Assert.fail("Expected SqlException");
        } catch (SqlException e) {
            TestUtils.assertContains(e.getMessage(), "invalid decimal: empty value");
        }
    }

    @Test
    public void testParseDecimalConstantOnlySpaces() {
        try {
            DecimalUtil.parseDecimalConstant(0, sqlExecutionContext, "   ", -1, -1);
            Assert.fail("Expected SqlException");
        } catch (SqlException e) {
            TestUtils.assertContains(e.getMessage(), "invalid decimal: empty value");
        }
    }

    @Test
    public void testParseDecimalConstantOnlyZeros() throws SqlException {
        // "0" is valid and should return 0
        ConstantFunction result = DecimalUtil.parseDecimalConstant(0, sqlExecutionContext, "0", -1, -1);
        Assert.assertTrue(result instanceof Decimal8Constant);
        Assert.assertEquals(0, result.getDecimal8(null));
    }

    @Test
    public void testParseDecimalConstantPrecisionSmallerThanActualDigits() {
        // Try to parse "12345" with precision 3 (should fail)
        try {
            DecimalUtil.parseDecimalConstant(0, sqlExecutionContext, "12345", 3, -1);
            Assert.fail("Expected SqlException");
        } catch (SqlException e) {
            TestUtils.assertContains(e.getMessage(), "decimal '12345' requires precision of 5 but is limited to 3");
        }
    }

    @Test
    public void testParseDecimalConstantSignAndDot() {
        try {
            DecimalUtil.parseDecimalConstant(0, sqlExecutionContext, "-.", -1, -1);
            Assert.fail("Expected SqlException");
        } catch (SqlException e) {
            TestUtils.assertContains(e.getMessage(), "invalid decimal: '-.' contains no digits");
        }
    }

    @Test
    public void testParseDecimalConstantSmallNegative() throws SqlException {
        ConstantFunction result = DecimalUtil.parseDecimalConstant(0, sqlExecutionContext, "-123", -1, -1);
        Assert.assertTrue(result instanceof Decimal16Constant);
        Assert.assertEquals(-123, result.getDecimal16(null));
    }

    @Test
    public void testParseDecimalConstantSmallPositive() throws SqlException {
        ConstantFunction result = DecimalUtil.parseDecimalConstant(0, sqlExecutionContext, "123", -1, -1);
        Assert.assertTrue(result instanceof Decimal16Constant);
        Assert.assertEquals(123, result.getDecimal16(null));
    }

    @Test
    public void testParseDecimalConstantSpecificScaleExceedsMax() {
        try {
            // Try to parse with scale 77 (exceeds maximum)
            DecimalUtil.parseDecimalConstant(0, sqlExecutionContext, "123", -1, 77);
            Assert.fail("Expected SqlException");
        } catch (SqlException e) {
            TestUtils.assertContains(e.getMessage(), "exceeds maximum allowed scale of 76");
        }
    }

    @Test
    public void testParseDecimalConstantWithBothPrecisionAndScale() throws SqlException {
        // Parse "123.45" with precision 10 and scale 2
        ConstantFunction result = DecimalUtil.parseDecimalConstant(0, sqlExecutionContext, "123.45", 10, 2);
        Assert.assertTrue(result instanceof Decimal64Constant);
        Assert.assertEquals(12345, result.getDecimal64(null));
    }

    @Test
    public void testParseDecimalConstantWithDecimalAndScale() throws SqlException {
        ConstantFunction result = DecimalUtil.parseDecimalConstant(0, sqlExecutionContext, "123.456789", -1, -1);
        Assert.assertTrue(result instanceof Decimal32Constant);
        Assert.assertEquals(123456789, result.getDecimal32(null));
    }

    @Test
    public void testParseDecimalConstantWithDecimalPoint() throws SqlException {
        ConstantFunction result = DecimalUtil.parseDecimalConstant(0, sqlExecutionContext, "123.45", -1, -1);
        Assert.assertTrue(result instanceof Decimal32Constant);
        Assert.assertEquals(12345, result.getDecimal32(null));
    }

    @Test
    public void testParseDecimalConstantWithLeadingSpaces() throws SqlException {
        ConstantFunction result = DecimalUtil.parseDecimalConstant(0, sqlExecutionContext, "  123", -1, -1);
        Assert.assertTrue(result instanceof Decimal16Constant);
        Assert.assertEquals(123, result.getDecimal16(null));
    }

    @Test
    public void testParseDecimalConstantWithLeadingZeros() throws SqlException {
        ConstantFunction result = DecimalUtil.parseDecimalConstant(0, sqlExecutionContext, "00123", -1, -1);
        Assert.assertTrue(result instanceof Decimal16Constant);
        Assert.assertEquals(123, result.getDecimal16(null));
    }

    @Test
    public void testParseDecimalConstantWithPositiveSign() throws SqlException {
        ConstantFunction result = DecimalUtil.parseDecimalConstant(0, sqlExecutionContext, "+123", -1, -1);
        Assert.assertTrue(result instanceof Decimal16Constant);
        Assert.assertEquals(123, result.getDecimal16(null));
    }

    @Test
    public void testParseDecimalConstantWithPrecisionAndScaleLimits() throws SqlException {
        // Test with max precision and scale
        ConstantFunction result = DecimalUtil.parseDecimalConstant(0, sqlExecutionContext, "1.23", 76, 75);
        Assert.assertTrue(result instanceof Decimal256Constant);
    }

    @Test
    public void testParseDecimalConstantWithPrecisionForcesSmallerType() throws SqlException {
        // Large number but with small precision forces smaller type
        ConstantFunction result = DecimalUtil.parseDecimalConstant(0, sqlExecutionContext, "123", 4, -1);
        Assert.assertTrue(result instanceof Decimal16Constant);
        Assert.assertEquals(123, result.getDecimal16(null));
    }

    @Test
    public void testParseDecimalConstantWithScalePadsZeros() throws SqlException {
        // Parse "123" with scale 2 should be like "123.00"
        ConstantFunction result = DecimalUtil.parseDecimalConstant(0, sqlExecutionContext, "123", -1, 2);
        Assert.assertTrue(result instanceof Decimal32Constant);
        // The internal representation should be 12300 (123 * 10^2)
        Assert.assertEquals(12300, result.getDecimal32(null));
    }

    @Test
    public void testParseDecimalConstantWithSpecifiedPrecision() throws SqlException {
        // Parse "123.45" with precision 10 (total digits allowed)
        ConstantFunction result = DecimalUtil.parseDecimalConstant(0, sqlExecutionContext, "123.45", 10, -1);
        Assert.assertTrue(result instanceof Decimal64Constant);
        Assert.assertEquals(12345, result.getDecimal64(null));
    }

    @Test
    public void testParseDecimalConstantWithSpecifiedScale() throws SqlException {
        // Parse "123.45" with scale 2 (decimal places)
        ConstantFunction result = DecimalUtil.parseDecimalConstant(0, sqlExecutionContext, "123.45", -1, 2);
        Assert.assertTrue(result instanceof Decimal32Constant);
        Assert.assertEquals(12345, result.getDecimal32(null));
    }

    @Test
    public void testParseDecimalConstantWithTrailingCapitalM() throws SqlException {
        ConstantFunction result = DecimalUtil.parseDecimalConstant(0, sqlExecutionContext, "123", -1, -1);
        Assert.assertTrue(result instanceof Decimal16Constant);
        Assert.assertEquals(123, result.getDecimal16(null));
    }

    @Test
    public void testParseDecimalConstantWithTrailingM() throws SqlException {
        ConstantFunction result = DecimalUtil.parseDecimalConstant(0, sqlExecutionContext, "123m", -1, -1);
        Assert.assertTrue(result instanceof Decimal16Constant);
        Assert.assertEquals(123, result.getDecimal16(null));
    }

    @Test
    public void testParseDecimalConstantZeroWithDecimal() throws SqlException {
        ConstantFunction result = DecimalUtil.parseDecimalConstant(0, sqlExecutionContext, "0.123", -1, -1);
        Assert.assertTrue(result instanceof Decimal16Constant);
        Assert.assertEquals(123, result.getDecimal16(null));
    }

    @Test
    public void testParsePrecisionEmpty() {
        try {
            DecimalUtil.parsePrecision(0, "", 0, 0);
            Assert.fail("Expected SqlException");
        } catch (SqlException e) {
            TestUtils.assertContains(e.getMessage(), "Invalid decimal type. The precision ('') must be a number");
        }
    }

    @Test
    public void testParsePrecisionInvalidNonNumeric() {
        try {
            DecimalUtil.parsePrecision(0, "abc", 0, 3);
            Assert.fail("Expected SqlException");
        } catch (SqlException e) {
            TestUtils.assertContains(e.getMessage(), "Invalid decimal type. The precision ('abc') must be a number");
        }
    }

    @Test
    public void testParsePrecisionLargeValue() throws SqlException {
        int precision = DecimalUtil.parsePrecision(0, "76", 0, 2);
        Assert.assertEquals(76, precision);
    }

    @Test
    public void testParsePrecisionSingleDigit() throws SqlException {
        int precision = DecimalUtil.parsePrecision(0, "5", 0, 1);
        Assert.assertEquals(5, precision);
    }

    @Test
    public void testParsePrecisionValid() throws SqlException {
        int precision = DecimalUtil.parsePrecision(0, "10", 0, 2);
        Assert.assertEquals(10, precision);
    }

    @Test
    public void testParseScaleDoubleDigit() throws SqlException {
        int scale = DecimalUtil.parseScale(0, "15", 0, 2);
        Assert.assertEquals(15, scale);
    }

    @Test
    public void testParseScaleEmpty() {
        try {
            DecimalUtil.parseScale(0, "", 0, 0);
            Assert.fail("Expected SqlException");
        } catch (SqlException e) {
            TestUtils.assertContains(e.getMessage(), "Invalid decimal type. The scale ('') must be a number");
        }
    }

    @Test
    public void testParseScaleInvalidNonNumeric() {
        try {
            DecimalUtil.parseScale(0, "xyz", 0, 3);
            Assert.fail("Expected SqlException");
        } catch (SqlException e) {
            TestUtils.assertContains(e.getMessage(), "Invalid decimal type. The scale ('xyz') must be a number");
        }
    }

    @Test
    public void testParseScaleValid() throws SqlException {
        int scale = DecimalUtil.parseScale(0, "5", 0, 1);
        Assert.assertEquals(5, scale);
    }

    @Test
    public void testParseScaleZero() throws SqlException {
        int scale = DecimalUtil.parseScale(0, "0", 0, 1);
        Assert.assertEquals(0, scale);
    }

    @Test
    public void testStoreDecimal128() {
        assertStoreDecimal128(new Decimal128(0x90ABBA0990ABBA90L, 0xCDEFFEDCCDEFFEDCL, 0));
        assertStoreDecimal128(new Decimal128(Decimals.DECIMAL128_HI_NULL, Decimals.DECIMAL128_LO_NULL, 0));
        assertStoreDecimal128(Decimal128.MIN_VALUE);
        assertStoreDecimal128(Decimal128.MAX_VALUE);
        assertStoreDecimal128(Decimal128.fromLong(123456, 0));
        assertStoreDecimal128(Decimal128.fromLong(-123456, 0));
    }

    @Test
    public void testStoreDecimal16() {
        int type = ColumnType.getDecimalType(4, 0);
        MemLongGetter getter = (mem) -> mem.getDecimal16(0);
        assertStoreDecimal64(0x5678, Short.BYTES, type, getter);
        assertStoreDecimal64Null(Decimals.DECIMAL16_NULL, Short.BYTES, type, getter);
        assertStoreDecimal64(9999, Short.BYTES, type, getter);
        assertStoreDecimal64(-9999, Short.BYTES, type, getter);
        assertStoreDecimal64(1234, Short.BYTES, type, getter);
        assertStoreDecimal64(-1234, Short.BYTES, type, getter);
    }

    @Test
    public void testStoreDecimal256() {
        assertStoreDecimal256(new Decimal256(0x1234432112344321L, 0x5678876556788765L, 0x90ABBA0990ABBA90L, 0xCDEFFEDCCDEFFEDCL, 0));
        assertStoreDecimal256(Decimal256.NULL_VALUE);
        assertStoreDecimal256(Decimal256.MIN_VALUE);
        assertStoreDecimal256(Decimal256.MAX_VALUE);
        assertStoreDecimal256(Decimal256.fromLong(123456, 0));
        assertStoreDecimal256(Decimal256.fromLong(-123456, 0));
    }

    @Test
    public void testStoreDecimal32() {
        int type = ColumnType.getDecimalType(9, 0);
        MemLongGetter getter = (mem) -> mem.getDecimal32(0);
        assertStoreDecimal64(0x12345678, Integer.BYTES, type, getter);
        assertStoreDecimal64Null(Decimals.DECIMAL32_NULL, Integer.BYTES, type, getter);
        assertStoreDecimal64(999999999, Integer.BYTES, type, getter);
        assertStoreDecimal64(-999999999, Integer.BYTES, type, getter);
        assertStoreDecimal64(123456, Integer.BYTES, type, getter);
        assertStoreDecimal64(-123456, Integer.BYTES, type, getter);
    }

    @Test
    public void testStoreDecimal64() {
        int type = ColumnType.getDecimalType(18, 0);
        MemLongGetter getter = (mem) -> mem.getDecimal64(0);
        assertStoreDecimal64(0xDEADBEEFL, Long.BYTES, type, getter);
        assertStoreDecimal64Null(Decimals.DECIMAL64_NULL, Long.BYTES, type, getter);
        assertStoreDecimal64(Decimal64.MIN_VALUE.getValue(), Long.BYTES, type, getter);
        assertStoreDecimal64(Decimal64.MAX_VALUE.getValue(), Long.BYTES, type, getter);
        assertStoreDecimal64(123456L, Long.BYTES, type, getter);
        assertStoreDecimal64(-123456L, Long.BYTES, type, getter);
    }

    @Test
    public void testStoreDecimal8() {
        int type = ColumnType.getDecimalType(2, 0);
        MemLongGetter getter = (mem) -> mem.getDecimal8(0);
        assertStoreDecimal64(0x12, Byte.BYTES, type, getter);
        assertStoreDecimal64Null(Decimals.DECIMAL8_NULL, Byte.BYTES, type, getter);
        assertStoreDecimal64(99, Byte.BYTES, type, getter);
        assertStoreDecimal64(-99, Byte.BYTES, type, getter);
        assertStoreDecimal64(12, Byte.BYTES, type, getter);
        assertStoreDecimal64(-12, Byte.BYTES, type, getter);
    }

    @Test
    public void testStoreRow() {
        Decimal256 value = new Decimal256();
        value.ofNull();
        for (int i = 1; i <= Decimals.MAX_PRECISION; i++) {
            assertStoreRow(value, i);
        }

        value.ofLong(12, 0);
        assertStoreRow(value, 2);
        value.ofLong(1234, 0);
        assertStoreRow(value, 4);
        value.ofLong(12345678, 0);
        assertStoreRow(value, 8);
        value.ofLong(12345678901234L, 0);
        assertStoreRow(value, 18);
        value.ofLong(12345678901234L, 0);
        assertStoreRow(value, 36);
        value.ofLong(12345678901234L, 0);
        assertStoreRow(value, 72);
    }

    private void assertLoadDecimal(Decimal256 value, int type) {
        var rec = getDecimalRecord(type, value);
        Decimal256 actual = new Decimal256();
        DecimalUtil.load(actual, decimal128, rec, 0, type);
        Assert.assertEquals(value, actual);
    }

    private void assertLoadDecimal256(CharSequence expected, Function value) {
        Decimal256 decimal256 = new Decimal256();
        DecimalUtil.load(decimal256, decimal128, value, null);
        Assert.assertEquals(expected, decimal256.toString());
    }

    private void assertLoadNullDecimal(Decimal256 value, int type) {
        var rec = getDecimalRecord(type, value);
        Decimal256 actual = new Decimal256();
        DecimalUtil.load(actual, decimal128, rec, 0, type);
        Assert.assertTrue(actual.isNull());
    }

    private void assertStoreDecimal128(Decimal128 decimal128) {
        try (MemoryCARWImpl mem = new MemoryCARWImpl(16, 1, MemoryTag.NATIVE_DEFAULT)) {
            long s = 0xDEADBEEFCAFEBABEL;
            Decimal256 d = new Decimal256(s, s, decimal128.getHigh(), decimal128.getLow(), 0);
            if (decimal128.isNull()) {
                d.ofNull();
            }
            DecimalUtil.store(d, mem, ColumnType.getDecimalType(38, 0));
            Decimal128 result = new Decimal128();
            mem.getDecimal128(0, result);
            Assert.assertEquals(decimal128, result);
        }
    }

    private void assertStoreDecimal256(Decimal256 decimal256) {
        try (MemoryCARWImpl mem = new MemoryCARWImpl(Decimal256.BYTES, 1, MemoryTag.NATIVE_DEFAULT)) {
            DecimalUtil.store(decimal256, mem, ColumnType.getDecimalType(76, 0));
            Decimal256 result = new Decimal256();
            mem.getDecimal256(0, result);
            Assert.assertEquals(String.format("assertion failed, expected %s but got %s", decimal256, result), decimal256, result);
        }
    }

    private void assertStoreDecimal64(long value, int size, int type, MemLongGetter getter) {
        try (MemoryCARWImpl mem = new MemoryCARWImpl(size, 1, MemoryTag.NATIVE_DEFAULT)) {
            long s = 0xDEADBEEFCAFEBABEL;
            Decimal256 d = new Decimal256(s, s, s, value, 0);
            DecimalUtil.store(d, mem, type);
            long actual = getter.run(mem);
            Assert.assertEquals(value, actual);
        }
    }

    private void assertStoreDecimal64Null(long expected, int size, int type, MemLongGetter getter) {
        try (MemoryCARWImpl mem = new MemoryCARWImpl(size, 1, MemoryTag.NATIVE_DEFAULT)) {
            DecimalUtil.store(Decimal256.NULL_VALUE, mem, type);
            long actual = getter.run(mem);
            Assert.assertEquals(expected, actual);
        }
    }

    private void assertStoreRow(Decimal256 value, int precision) {
        int type = ColumnType.getDecimalType(precision, value.getScale());
        TableWriter.Row row = getRowAsserter(type, -1, value);
        DecimalUtil.store(value, row, -1, type);
    }

    @FunctionalInterface
    public interface MemLongGetter {
        long run(MemoryCR mem);
    }

    private static class TestNonConstantDecimalFunction extends Decimal8Function {

        public TestNonConstantDecimalFunction() {
            super(ColumnType.getDecimalType(10, 2));
        }

        @Override
        public byte getDecimal8(Record rec) {
            return 0;
        }

        @Override
        public boolean isConstant() {
            return false;
        }
    }
}