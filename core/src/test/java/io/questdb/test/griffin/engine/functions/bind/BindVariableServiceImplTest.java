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

package io.questdb.test.griffin.engine.functions.bind;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.ImplicitCastException;
import io.questdb.cairo.sql.BindVariableService;
import io.questdb.cairo.sql.Function;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.engine.functions.bind.BindVariableServiceImpl;
import io.questdb.std.Decimal128;
import io.questdb.std.Decimal256;
import io.questdb.std.Decimals;
import io.questdb.std.Long256Impl;
import io.questdb.std.Numbers;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf8String;
import io.questdb.test.cairo.DefaultTestCairoConfiguration;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.nio.charset.StandardCharsets;

import static io.questdb.test.tools.TestUtils.assertMemoryLeak;

public class BindVariableServiceImplTest {
    private final static BindVariableService bindVariableService = new BindVariableServiceImpl(new DefaultTestCairoConfiguration(null));

    @Before
    public void setUp() {
        bindVariableService.clear();
    }

    @Test
    public void testBinIndexedOverride() throws Exception {
        assertMemoryLeak(() -> {
            bindVariableService.setLong(0, 10);
            try {
                bindVariableService.setBin(0, null);
                Assert.fail();
            } catch (SqlException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "bind variable at 0 is already defined as LONG");
            }
        });
    }

    @Test
    public void testBinOverride() throws Exception {
        assertMemoryLeak(() -> {
            bindVariableService.setLong("a", 10);
            try {
                bindVariableService.setBin("a", null);
                Assert.fail();
            } catch (SqlException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "bind variable 'a' is already defined as LONG");
            }
        });
    }

    @Test
    public void testBooleanIndexedOverride() throws Exception {
        assertMemoryLeak(() -> {
            bindVariableService.setLong(0, 10);
            try {
                bindVariableService.setBoolean(0, false);
                Assert.fail();
            } catch (SqlException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "bind variable at 0 is defined as LONG and cannot accept BOOLEAN");
            }
        });
    }

    @Test
    public void testBooleanOverride() throws Exception {
        assertMemoryLeak(() -> {
            bindVariableService.setLong("a", 10);
            try {
                bindVariableService.setBoolean("a", false);
                Assert.fail();
            } catch (SqlException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "bind variable 'a' is defined as LONG and cannot accept BOOLEAN");
            }
        });
    }

    @Test
    public void testBooleanVarSetToInt() throws Exception {
        assertMemoryLeak(() -> {
            bindVariableService.setBoolean(0);
            bindVariableService.setBoolean(0, true);
            Assert.assertTrue(bindVariableService.getFunction(0).getBool(null));

            try {
                bindVariableService.setInt(0, 123);
                Assert.fail();
            } catch (SqlException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "bind variable at 0 is defined as BOOLEAN and cannot accept INT");
            }
        });
    }

    @Test
    public void testCharIndexedOverride() throws Exception {
        assertMemoryLeak(() -> {
            bindVariableService.setInt(2, 10);
            try {
                bindVariableService.setChar(2, 'o');
                Assert.fail();
            } catch (ImplicitCastException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "inconvertible value");
            }
        });
    }

    @Test
    public void testCharOverride() throws Exception {
        assertMemoryLeak(() -> {
            bindVariableService.setLong("a", 10);
            try {
                bindVariableService.setChar("a", 'k');
                Assert.fail();
            } catch (ImplicitCastException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "inconvertible value");
            }
        });
    }

    @Test
    public void testCharVarSetToChar() throws Exception {
        assertMemoryLeak(() -> {
            bindVariableService.setChar(0);
            bindVariableService.setChar(0, 'X');
            Assert.assertEquals('X', bindVariableService.getFunction(0).getChar(null));

            bindVariableService.setChar(0, 'R');
            Assert.assertEquals('R', bindVariableService.getFunction(0).getChar(null));
        });
    }

    @Test
    public void testDateVarSetToInt() throws Exception {
        assertMemoryLeak(() -> {
            bindVariableService.setDate(0);
            bindVariableService.setDate(0, 99999001);
            Assert.assertEquals(99999001, bindVariableService.getFunction(0).getDate(null));

            bindVariableService.setInt(0, 450);
            Assert.assertEquals(450, bindVariableService.getFunction(0).getDate(null));

            bindVariableService.setInt(0, Numbers.INT_NULL);
            final long d = bindVariableService.getFunction(0).getDate(null);
            Assert.assertEquals(Numbers.LONG_NULL, d);
        });
    }

    @Test
    public void testDateVarSetToLong() throws Exception {
        assertMemoryLeak(() -> {
            bindVariableService.setDate(0);
            bindVariableService.setDate(0, 99999001);
            Assert.assertEquals(99999001, bindVariableService.getFunction(0).getDate(null));

            bindVariableService.setLong(0, 450);
            Assert.assertEquals(450, bindVariableService.getFunction(0).getDate(null));

            bindVariableService.setLong(0, Numbers.LONG_NULL);
            final long d = bindVariableService.getFunction(0).getDate(null);
            Assert.assertEquals(Numbers.LONG_NULL, d);
        });
    }

    @Test
    public void testDateVarSetToTimestamp() throws Exception {
        assertMemoryLeak(() -> {
            bindVariableService.setDate(0);
            bindVariableService.setTimestamp(0, 99999001L);
            Assert.assertEquals(99999, bindVariableService.getFunction(0).getDate(null));

            bindVariableService.setTimestamp(0, Numbers.LONG_NULL);
            final long d = bindVariableService.getFunction(0).getDate(null);
            Assert.assertEquals(Numbers.LONG_NULL, d);
        });
    }

    @Test
    public void testDateVarSetToTimestampNS() throws Exception {
        assertMemoryLeak(() -> {
            bindVariableService.setDate(0);
            bindVariableService.setTimestampNano(0, 99999000001L);
            Assert.assertEquals(99999, bindVariableService.getFunction(0).getDate(null));

            bindVariableService.setTimestampNano(0, Numbers.LONG_NULL);
            final long d = bindVariableService.getFunction(0).getDate(null);
            Assert.assertEquals(Numbers.LONG_NULL, d);
        });
    }

    @Test
    public void testDoubleIndexedOverride() throws Exception {
        assertMemoryLeak(() -> {
            bindVariableService.setInt(2, 10);
            try {
                bindVariableService.setDouble(2, 5.4);
                Assert.fail();
            } catch (SqlException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "bind variable at 2 is defined as INT and cannot accept DOUBLE");
            }
        });
    }

    @Test
    public void testDoubleOverride() throws Exception {
        assertMemoryLeak(() -> {
            bindVariableService.setInt("a", 10);
            try {
                bindVariableService.setDouble("a", 5.4);
                Assert.fail();
            } catch (SqlException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "bind variable 'a' is defined as INT and cannot accept DOUBLE");
            }
        });
    }

    @Test
    public void testDoubleVarSetToFloat() throws Exception {
        assertMemoryLeak(() -> {
            bindVariableService.setDouble(0);
            bindVariableService.setDouble(0, 1000.88);
            Assert.assertEquals(1000.88, bindVariableService.getFunction(0).getDouble(null), 0.000001);

            bindVariableService.setFloat(0, 451f);
            Assert.assertEquals(451, bindVariableService.getFunction(0).getDouble(null), 0.000001);

            bindVariableService.setFloat(0, Float.NaN);
            final double d = bindVariableService.getFunction(0).getDouble(null);
            Assert.assertTrue(d != d);
        });
    }

    @Test
    public void testDoubleVarSetToInt() throws Exception {
        assertMemoryLeak(() -> {
            bindVariableService.setDouble(0);
            bindVariableService.setDouble(0, 1000.88);
            Assert.assertEquals(1000.88, bindVariableService.getFunction(0).getDouble(null), 0.000001);

            bindVariableService.setInt(0, 450);
            Assert.assertEquals(450, bindVariableService.getFunction(0).getDouble(null), 0.000001);

            bindVariableService.setInt(0, Numbers.INT_NULL);
            final double d = bindVariableService.getFunction(0).getDouble(null);
            Assert.assertTrue(d != d);
        });
    }

    @Test
    public void testDoubleVarSetToLong() throws Exception {
        assertMemoryLeak(() -> {
            bindVariableService.setDouble(0);
            bindVariableService.setDouble(0, 91.3);
            Assert.assertEquals(91.3, bindVariableService.getFunction(0).getDouble(null), 0.00001);

            bindVariableService.setLong(0, 450);
            Assert.assertEquals(450, bindVariableService.getFunction(0).getDouble(null), 0.00001);

            bindVariableService.setLong(0, Numbers.LONG_NULL);
            final double f = bindVariableService.getFunction(0).getDouble(null);
            Assert.assertTrue(f != f);
        });
    }

    @Test
    public void testFloatIndexedOverride() throws Exception {
        assertMemoryLeak(() -> {
            bindVariableService.setLong(1, 10);
            try {
                bindVariableService.setFloat(1, 5);
                Assert.fail();
            } catch (SqlException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "bind variable at 1 is defined as LONG and cannot accept FLOAT");
            }
        });
    }

    @Test
    public void testFloatOverride() throws Exception {
        assertMemoryLeak(() -> {
            bindVariableService.setLong("a", 10);
            try {
                bindVariableService.setFloat("a", 5);
                Assert.fail();
            } catch (SqlException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "bind variable 'a' is defined as LONG and cannot accept FLOAT");
            }
        });
    }

    @Test
    public void testFloatVarSetToInt() throws Exception {
        assertMemoryLeak(() -> {
            bindVariableService.setFloat(0);
            bindVariableService.setFloat(0, 1000.88f);
            Assert.assertEquals(1000.88f, bindVariableService.getFunction(0).getFloat(null), 0.000001);

            bindVariableService.setInt(0, 450);
            Assert.assertEquals(450, bindVariableService.getFunction(0).getFloat(null), 0.000001);

            bindVariableService.setInt(0, Numbers.INT_NULL);
            final float d = bindVariableService.getFunction(0).getFloat(null);
            Assert.assertTrue(d != d);
        });
    }

    @Test
    public void testFloatVarSetToLong() throws Exception {
        assertMemoryLeak(() -> {
            bindVariableService.setFloat(0);
            bindVariableService.setFloat(0, 91.3f);
            Assert.assertEquals(91.3f, bindVariableService.getFunction(0).getFloat(null), 0.00001);

            bindVariableService.setLong(0, 450);
            Assert.assertEquals(450, bindVariableService.getFunction(0).getFloat(null), 0.00001);

            bindVariableService.setLong(0, Numbers.LONG_NULL);
            final float f = bindVariableService.getFunction(0).getFloat(null);
            Assert.assertTrue(f != f);
        });
    }

    @Test
    public void testIntVarSetToInt() throws Exception {
        assertMemoryLeak(() -> {
            bindVariableService.setInt(0);
            bindVariableService.setInt(0, 99999001);
            Assert.assertEquals(99999001, bindVariableService.getFunction(0).getInt(null));
        });
    }

    @Test
    public void testLongIndexedOverride() throws Exception {
        assertMemoryLeak(() -> {
            bindVariableService.setInt(0, 10);
            bindVariableService.setLong(0, 5);
            Assert.assertEquals(5, bindVariableService.getFunction(0).getInt(null));
        });
    }

    @Test
    public void testLongOverride() throws Exception {
        assertMemoryLeak(() -> {
            bindVariableService.setInt("a", 10);
            bindVariableService.setLong("a", 5);
            // allow INT to long truncate
            Assert.assertEquals(5, bindVariableService.getFunction(":a").getInt(null));
        });
    }

    @Test
    public void testLongVarSetToInt() throws Exception {
        assertMemoryLeak(() -> {
            bindVariableService.setLong(0);
            bindVariableService.setLong(0, 99999001);
            Assert.assertEquals(99999001, bindVariableService.getFunction(0).getLong(null));

            bindVariableService.setInt(0, 450);
            Assert.assertEquals(450, bindVariableService.getFunction(0).getLong(null));

            bindVariableService.setInt(0, Numbers.INT_NULL);
            final long d = bindVariableService.getFunction(0).getLong(null);
            Assert.assertEquals(Numbers.LONG_NULL, d);
        });
    }

    @Test
    public void testNamedSetBooleanToBooleanNoDefine() throws Exception {
        assertMemoryLeak(() -> {
            bindVariableService.setBoolean("x", true);
            Assert.assertTrue(bindVariableService.getFunction(":x").getBool(null));
            bindVariableService.setBoolean("x", false);
            Assert.assertFalse(bindVariableService.getFunction(":x").getBool(null));
        });
    }

    @Test
    public void testNamedSetByteToByteNoDefine() throws Exception {
        assertMemoryLeak(() -> {
            bindVariableService.setByte("x", (byte) 23);
            Assert.assertEquals(23, bindVariableService.getFunction(":x").getByte(null));
            bindVariableService.setByte("x", (byte) 45);
            Assert.assertEquals(45, bindVariableService.getFunction(":x").getByte(null));
        });
    }

    @Test
    public void testNamedSetCharToCharNoDefine() throws Exception {
        assertMemoryLeak(() -> {
            bindVariableService.setChar("x", 'V');
            Assert.assertEquals('V', bindVariableService.getFunction(":x").getChar(null));
            bindVariableService.setChar("x", 'Z');
            Assert.assertEquals('Z', bindVariableService.getFunction(":x").getChar(null));
        });
    }

    @Test
    public void testNamedSetDateToDateNoDefine() throws Exception {
        assertMemoryLeak(() -> {
            bindVariableService.setDate("x", 9990000011L);
            Assert.assertEquals(9990000011L, bindVariableService.getFunction(":x").getDate(null));
            bindVariableService.setDate("x", 9990000022L);
            Assert.assertEquals(9990000022L, bindVariableService.getFunction(":x").getDate(null));
        });
    }

    @Test
    public void testNamedSetDecimalNull() throws Exception {
        assertMemoryLeak(() -> {
            bindVariableService.setDecimal("x", Decimals.DECIMAL256_HH_NULL, Decimals.DECIMAL256_HL_NULL, Decimals.DECIMAL256_LH_NULL, Decimals.DECIMAL256_LL_NULL, ColumnType.getDecimalType(12, 0));
            Function function = bindVariableService.getFunction(":x");
            Assert.assertEquals(Decimals.DECIMAL8_NULL, function.getDecimal8(null));
            Assert.assertEquals(Decimals.DECIMAL16_NULL, function.getDecimal16(null));
            Assert.assertEquals(Decimals.DECIMAL32_NULL, function.getDecimal32(null));
            Assert.assertEquals(Decimals.DECIMAL64_NULL, function.getDecimal64(null));
            var decimal128 = new Decimal128();
            function.getDecimal128(null, decimal128);
            Assert.assertTrue(decimal128.isNull());
            var decimal256 = new Decimal256();
            function.getDecimal256(null, decimal256);
            Assert.assertTrue(decimal256.isNull());
        });
    }

    @Test
    public void testNamedSetDecimalOverflow() throws Exception {
        assertMemoryLeak(() -> {
            bindVariableService.setDecimal("x", 0, 0, 0, 0, ColumnType.getDecimalType(76, 70));
            try {
                bindVariableService.setDecimal("x", 0, 0, 0, 900000000, ColumnType.getDecimalType(16, 0));
                Assert.fail();
            } catch (SqlException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "inconvertible types: DECIMAL(16,0) -> DECIMAL(76,70) [varName=x]");
            }
        });
    }

    @Test
    public void testNamedSetDecimalOverflowPrecision() throws Exception {
        assertMemoryLeak(() -> {
            bindVariableService.setDecimal("x", 0, 0, 0, 0, ColumnType.getDecimalType(5, 4));
            try {
                bindVariableService.setDecimal("x", 0, 0, 0, 5000, ColumnType.getDecimalType(7, 2));
                Assert.fail();
            } catch (SqlException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "inconvertible types: DECIMAL(7,2) -> DECIMAL(5,4) [varName=x]");
            }
        });
    }

    @Test
    public void testNamedSetDecimalScale() throws Exception {
        assertMemoryLeak(() -> {
            bindVariableService.setDecimal("x", 0, 0, 0, 5, ColumnType.getDecimalType(12, 2));
            bindVariableService.setDecimal("x", 0, 0, 0, 5, ColumnType.getDecimalType(12, 0));
            Function function = bindVariableService.getFunction(":x");
            Assert.assertEquals(500, function.getDecimal32(null));
        });
    }

    @Test
    public void testNamedSetDoubleToDoubleNoDefine() throws Exception {
        assertMemoryLeak(() -> {
            bindVariableService.setDouble("x", 17.3);
            Assert.assertEquals(17.299999237060547, bindVariableService.getFunction(":x").getDouble(null), 0.00001);
            bindVariableService.setDouble("x", 19.3);
            Assert.assertEquals(19.299999237060547, bindVariableService.getFunction(":x").getDouble(null), 0.00001);
        });
    }

    @Test
    public void testNamedSetFloatToFloatNoDefine() throws Exception {
        assertMemoryLeak(() -> {
            bindVariableService.setFloat("x", 17.3f);
            Assert.assertEquals(17.299999237060547f, bindVariableService.getFunction(":x").getFloat(null), 0.00001);
            bindVariableService.setFloat("x", 19.3f);
            Assert.assertEquals(19.299999237060547, bindVariableService.getFunction(":x").getFloat(null), 0.00001);
        });
    }

    @Test
    public void testNamedSetIntToIntNoDefine() throws Exception {
        assertMemoryLeak(() -> {
            bindVariableService.setInt("x", 90001);
            Assert.assertEquals(90001, bindVariableService.getFunction(":x").getInt(null));
            bindVariableService.setInt("x", 90002);
            Assert.assertEquals(90002, bindVariableService.getFunction(":x").getInt(null));
        });
    }

    @Test
    public void testNamedSetLong256ToLong256() throws Exception {
        assertMemoryLeak(() -> {
            bindVariableService.setLong256("x");
            bindVariableService.setLong256("x", 888, 777, 6666, 5555);
            StringSink sink = new StringSink();
            bindVariableService.getFunction(":x").getLong256(null, sink);
            TestUtils.assertEquals("0x15b30000000000001a0a00000000000003090000000000000378", sink);
        });
    }

    @Test
    public void testNamedSetLong256ToLong256AsObj() throws Exception {
        assertMemoryLeak(() -> {
            final Long256Impl long256 = new Long256Impl();
            long256.setAll(888, 999, 777, 111);
            bindVariableService.setLong256("x");
            bindVariableService.setLong256("x", long256);
            final StringSink sink = new StringSink();
            bindVariableService.getFunction(":x").getLong256(null, sink);
            TestUtils.assertEquals("0x6f000000000000030900000000000003e70000000000000378", sink);
        });
    }

    @Test
    public void testNamedSetLong256ToLong256NoDefine() throws Exception {
        assertMemoryLeak(() -> {
            bindVariableService.setLong256("x", 888, 777, 6666, 5555);
            StringSink sink = new StringSink();
            bindVariableService.getFunction(":x").getLong256(null, sink);
            TestUtils.assertEquals("0x15b30000000000001a0a00000000000003090000000000000378", sink);
        });
    }

    @Test
    public void testNamedSetShortToShortNoDefine() throws Exception {
        assertMemoryLeak(() -> {
            bindVariableService.setShort("x", (short) 1201);
            Assert.assertEquals(1201, bindVariableService.getFunction(":x").getShort(null));
            bindVariableService.setShort("x", (short) 1203);
            Assert.assertEquals(1203, bindVariableService.getFunction(":x").getShort(null));
        });
    }

    @Test
    public void testNamedSetStrToStrNoDefine() throws Exception {
        assertMemoryLeak(() -> {
            bindVariableService.setStr("x", "hello_x");
            TestUtils.assertEquals("hello_x", bindVariableService.getFunction(":x").getStrA(null));
            bindVariableService.setStr("x", "hello_y");
            TestUtils.assertEquals("hello_y", bindVariableService.getFunction(":x").getStrA(null));
        });
    }

    @Test
    public void testNamedSetTimestampToTimestampNoDefine() throws Exception {
        assertMemoryLeak(() -> {
            bindVariableService.setTimestamp("x", 9990000011L);
            Assert.assertEquals(9990000011L, bindVariableService.getFunction(":x").getTimestamp(null));
            bindVariableService.setTimestamp("x", 9990000022L);
            Assert.assertEquals(9990000022L, bindVariableService.getFunction(":x").getTimestamp(null));
            bindVariableService.setTimestampNano("x", 9990000033000L);
            Assert.assertEquals(9990000033L, bindVariableService.getFunction(":x").getTimestamp(null));
        });
    }

    @Test
    public void testSetBinToStr() throws Exception {
        assertMemoryLeak(() -> {
            bindVariableService.define(0, ColumnType.BINARY, 0);
            try {
                bindVariableService.setStr(0, "21.2");
            } catch (SqlException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "bind variable at 0 is defined as BINARY and cannot accept STRING");
            }
        });
    }

    @Test
    public void testSetBooleanToByte() throws Exception {
        assertMemoryLeak(() -> {
            bindVariableService.define(0, ColumnType.BOOLEAN, 0);
            try {
                bindVariableService.setByte(0, (byte) 10);
                Assert.fail();
            } catch (SqlException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "bind variable at 0 is defined as BOOLEAN and cannot accept BYTE");
            }
        });
    }

    @Test
    public void testSetBooleanToLong256() throws Exception {
        assertMemoryLeak(() -> {
            bindVariableService.define(0, ColumnType.BOOLEAN, 0);
            try {
                bindVariableService.setLong256(0, 888, 777, 6666, 5555);
                Assert.fail();
            } catch (SqlException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "bind variable at 0 is defined as BOOLEAN and cannot accept LONG256");
            }
        });
    }

    @Test
    public void testSetBooleanToStr() throws Exception {
        assertMemoryLeak(() -> {
            bindVariableService.define(0, ColumnType.BOOLEAN, 0);
            bindVariableService.setStr(0, "true");
            Assert.assertTrue(bindVariableService.getFunction(0).getBool(null));
            bindVariableService.setStr(0, "false");
            Assert.assertFalse(bindVariableService.getFunction(0).getBool(null));
        });
    }

    @Test
    public void testSetByteToByte() throws Exception {
        assertMemoryLeak(() -> {
            bindVariableService.define(0, ColumnType.BYTE, 0);
            bindVariableService.setByte(0, (byte) 10);
            Assert.assertEquals(10, bindVariableService.getFunction(0).getByte(null));
            bindVariableService.setByte(0, (byte) 22);
            Assert.assertEquals(22, bindVariableService.getFunction(0).getByte(null));
        });
    }

    @Test
    public void testSetByteToStr() throws Exception {
        assertMemoryLeak(() -> {
            bindVariableService.define(0, ColumnType.BYTE, 0);
            bindVariableService.setStr(0, "21");
            Assert.assertEquals(21, bindVariableService.getFunction(0).getByte(null));
        });
    }

    @Test
    public void testSetByteVarToShort() throws Exception {
        assertMemoryLeak(() -> {
            bindVariableService.setByte(0, (byte) 99);
            Assert.assertEquals(99, bindVariableService.getFunction(0).getByte(null));
            bindVariableService.setShort(0, (short) 5);
            Assert.assertEquals(5, bindVariableService.getFunction(0).getByte(null));
        });
    }

    @Test
    public void testSetCharToStr() throws Exception {
        assertMemoryLeak(() -> {
            bindVariableService.define(0, ColumnType.CHAR, 0);
            bindVariableService.setStr(0, "6");
            Assert.assertEquals('6', bindVariableService.getFunction(0).getChar(null));
            bindVariableService.setStr(0, "");
            Assert.assertEquals(0, bindVariableService.getFunction(0).getChar(null));
            bindVariableService.setStr(0, null);
            Assert.assertEquals(0, bindVariableService.getFunction(0).getChar(null));
        });
    }

    @Test
    public void testSetDateToByte() throws Exception {
        assertMemoryLeak(() -> {
            bindVariableService.define(0, ColumnType.DATE, 0);
            bindVariableService.setByte(0, (byte) 10);
            Assert.assertEquals(10, bindVariableService.getFunction(0).getDate(null));
            bindVariableService.setByte(0, (byte) 22);
            Assert.assertEquals(22, bindVariableService.getFunction(0).getDate(null));
        });
    }

    @Test
    public void testSetDateToStr() throws Exception {
        assertMemoryLeak(() -> {
            bindVariableService.define(0, ColumnType.DATE, 0);
            bindVariableService.setStr(0, "21");
            Assert.assertEquals(21, bindVariableService.getFunction(0).getDate(null));
            bindVariableService.setStr(0, null);
            Assert.assertEquals(Numbers.LONG_NULL, bindVariableService.getFunction(0).getDate(null));
        });
    }

    @Test
    public void testSetDateVarToShort() throws Exception {
        assertMemoryLeak(() -> {
            bindVariableService.setDate(0, 10);
            Assert.assertEquals(10, bindVariableService.getFunction(0).getDate(null));
            bindVariableService.setShort(0, (short) 5);
            Assert.assertEquals(5, bindVariableService.getFunction(0).getDate(null));
        });
    }

    @Test
    public void testSetDecimalInvalidFunctionType() throws Exception {
        assertMemoryLeak(() -> {
            bindVariableService.define(0, ColumnType.VARCHAR, 0);
            try {
                bindVariableService.setDecimal(0, 0, 0, 0, 1, ColumnType.getDecimalType(16, 0));
                Assert.fail();
            } catch (SqlException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "bind variable at 0 is defined as VARCHAR and cannot accept DECIMAL(16,0)");
            }
        });
    }

    @Test
    public void testSetDecimalNull() throws Exception {
        assertMemoryLeak(() -> {
            bindVariableService.define(0, ColumnType.getDecimalType(12, 2), 0);
            bindVariableService.setDecimal(0, Decimals.DECIMAL256_HH_NULL, Decimals.DECIMAL256_HL_NULL, Decimals.DECIMAL256_LH_NULL, Decimals.DECIMAL256_LL_NULL, ColumnType.getDecimalType(12, 0));
            Function function = bindVariableService.getFunction(0);
            Assert.assertEquals(Decimals.DECIMAL8_NULL, function.getDecimal8(null));
            Assert.assertEquals(Decimals.DECIMAL16_NULL, function.getDecimal16(null));
            Assert.assertEquals(Decimals.DECIMAL32_NULL, function.getDecimal32(null));
            Assert.assertEquals(Decimals.DECIMAL64_NULL, function.getDecimal64(null));
            var decimal128 = new Decimal128();
            function.getDecimal128(null, decimal128);
            Assert.assertTrue(decimal128.isNull());
            var decimal256 = new Decimal256();
            function.getDecimal256(null, decimal256);
            Assert.assertTrue(decimal256.isNull());
        });
    }

    @Test
    public void testSetDecimalOverflow() throws Exception {
        assertMemoryLeak(() -> {
            bindVariableService.define(0, ColumnType.getDecimalType(76, 70), 0);
            try {
                bindVariableService.setDecimal(0, 0, 0, 0, 900000000, ColumnType.getDecimalType(16, 0));
                Assert.fail();
            } catch (SqlException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "inconvertible types: DECIMAL(16,0) -> DECIMAL(76,70) [varIndex=0]");
            }
        });
    }

    @Test
    public void testSetDecimalOverflowPrecision() throws Exception {
        assertMemoryLeak(() -> {
            bindVariableService.define(0, ColumnType.getDecimalType(5, 4), 0);
            try {
                bindVariableService.setDecimal(0, 0, 0, 0, 5000, ColumnType.getDecimalType(7, 2));
                Assert.fail();
            } catch (SqlException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "inconvertible types: DECIMAL(7,2) -> DECIMAL(5,4) [varIndex=0]");
            }
        });
    }

    @Test
    public void testSetDecimalScale() throws Exception {
        assertMemoryLeak(() -> {
            bindVariableService.define(0, ColumnType.getDecimalType(12, 2), 0);
            bindVariableService.setDecimal(0, 0, 0, 0, 5, ColumnType.getDecimalType(12, 0));
            Function function = bindVariableService.getFunction(0);
            Assert.assertEquals(500, function.getDecimal32(null));
        });
    }

    @Test
    public void testSetDoubleToByte() throws Exception {
        assertMemoryLeak(() -> {
            bindVariableService.define(0, ColumnType.DOUBLE, 0);
            bindVariableService.setByte(0, (byte) 10);
            Assert.assertEquals(10, bindVariableService.getFunction(0).getDouble(null), 0.000001);
            bindVariableService.setByte(0, (byte) 22);
            Assert.assertEquals(22, bindVariableService.getFunction(0).getDouble(null), 0.000001);
        });
    }

    @Test
    public void testSetDoubleToStr() throws Exception {
        assertMemoryLeak(() -> {
            bindVariableService.define(0, ColumnType.DOUBLE, 0);
            bindVariableService.setStr(0, "21.2");
            Assert.assertEquals(21.2, bindVariableService.getFunction(0).getDouble(null), 0.00001);
            bindVariableService.setStr(0, null);
            final double d = bindVariableService.getFunction(0).getDouble(null);
            Assert.assertTrue(d != d);
        });
    }

    @Test
    public void testSetDoubleVarToShort() throws Exception {
        assertMemoryLeak(() -> {
            bindVariableService.setDouble(0, 10.2);
            Assert.assertEquals(10.2, bindVariableService.getFunction(0).getDouble(null), 0.00001);
            bindVariableService.setShort(0, (short) 5);
            Assert.assertEquals(5, bindVariableService.getFunction(0).getDouble(null), 0.00001);
        });
    }

    @Test
    public void testSetFloatToByte() throws Exception {
        assertMemoryLeak(() -> {
            bindVariableService.define(0, ColumnType.FLOAT, 0);
            bindVariableService.setByte(0, (byte) 10);
            Assert.assertEquals(10, bindVariableService.getFunction(0).getFloat(null), 0.000001);
            bindVariableService.setByte(0, (byte) 22);
            Assert.assertEquals(22, bindVariableService.getFunction(0).getFloat(null), 0.000001);
        });
    }

    @Test
    public void testSetFloatToStr() throws Exception {
        assertMemoryLeak(() -> {
            bindVariableService.define(0, ColumnType.FLOAT, 0);
            bindVariableService.setStr(0, "21.2");
            Assert.assertEquals(21.2, bindVariableService.getFunction(0).getFloat(null), 0.00001);
            bindVariableService.setStr(0, null);
            final float f = bindVariableService.getFunction(0).getFloat(null);
            Assert.assertTrue(f != f);

            try {
                bindVariableService.setStr(0, "xyz");
                Assert.fail();
            } catch (ImplicitCastException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "inconvertible value: `xyz` [STRING -> FLOAT]");
            }
        });
    }

    @Test
    public void testSetFloatVarToShort() throws Exception {
        assertMemoryLeak(() -> {
            bindVariableService.setFloat(0, 10.2f);
            Assert.assertEquals(10.2f, bindVariableService.getFunction(0).getFloat(null), 0.00001);
            bindVariableService.setShort(0, (short) 5);
            Assert.assertEquals(5f, bindVariableService.getFunction(0).getFloat(null), 0.00001);
        });
    }

    @Test
    public void testSetGeoByteToLessAccurateGeoByte() throws Exception {
        assertMemoryLeak(() -> {
            bindVariableService.define(0, ColumnType.getGeoHashTypeWithBits(5), 0);
            try {
                bindVariableService.setGeoHash(0, 3, ColumnType.getGeoHashTypeWithBits(3));
                Assert.fail();
            } catch (SqlException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "inconvertible types: GEOHASH(3b) -> GEOHASH(1c) [varIndex=0]");
            }
        });
    }

    @Test
    public void testSetIntToByte() throws Exception {
        assertMemoryLeak(() -> {
            bindVariableService.setInt("a", 10);
            Assert.assertEquals(10, bindVariableService.getFunction(":a").getInt(null));
            bindVariableService.setByte("a", (byte) 5);
            Assert.assertEquals(5, bindVariableService.getFunction(":a").getInt(null));
        });
    }

    @Test
    public void testSetIntToStr() throws Exception {
        assertMemoryLeak(() -> {
            bindVariableService.define(0, ColumnType.INT, 0);
            bindVariableService.setStr(0, "21");
            Assert.assertEquals(21, bindVariableService.getFunction(0).getInt(null));
            bindVariableService.setStr(0, null);
            Assert.assertEquals(Numbers.INT_NULL, bindVariableService.getFunction(0).getInt(null));
        });
    }

    @Test
    public void testSetIntVarToShort() throws Exception {
        assertMemoryLeak(() -> {
            bindVariableService.setInt(0, 10);
            Assert.assertEquals(10, bindVariableService.getFunction(0).getInt(null));
            bindVariableService.setShort(0, (short) 5);
            Assert.assertEquals(5, bindVariableService.getFunction(0).getLong(null));
        });
    }

    @Test
    public void testSetLong256ToLong256() throws Exception {
        assertMemoryLeak(() -> {
            bindVariableService.define(0, ColumnType.LONG256, 0);
            bindVariableService.setLong256(0, 888, 777, 6666, 5555);
            StringSink sink = new StringSink();
            bindVariableService.getFunction(0).getLong256(null, sink);
            TestUtils.assertEquals("0x15b30000000000001a0a00000000000003090000000000000378", sink);
        });
    }

    @Test
    public void testSetLongToByte() throws Exception {
        assertMemoryLeak(() -> {
            bindVariableService.setLong("a", 10);
            Assert.assertEquals(10, bindVariableService.getFunction(":a").getLong(null));
            bindVariableService.setByte("a", (byte) 5);
            Assert.assertEquals(5, bindVariableService.getFunction(":a").getLong(null));
        });
    }

    @Test
    public void testSetLongToStr() throws Exception {
        assertMemoryLeak(() -> {
            bindVariableService.define(0, ColumnType.LONG, 0);
            bindVariableService.setStr(0, "21");
            Assert.assertEquals(21, bindVariableService.getFunction(0).getLong(null));
            bindVariableService.setStr(0, null);
            Assert.assertEquals(Numbers.LONG_NULL, bindVariableService.getFunction(0).getLong(null));
        });
    }

    @Test
    public void testSetLongVarToShort() throws Exception {
        assertMemoryLeak(() -> {
            bindVariableService.setLong(0, 10);
            Assert.assertEquals(10, bindVariableService.getFunction(0).getLong(null));
            bindVariableService.setShort(0, (short) 5);
            Assert.assertEquals(5, bindVariableService.getFunction(0).getLong(null));
        });
    }

    @Test
    public void testSetShortToByte() throws Exception {
        assertMemoryLeak(() -> {
            bindVariableService.define(0, ColumnType.SHORT, 0);
            bindVariableService.setByte(0, (byte) 10);
            Assert.assertEquals(10, bindVariableService.getFunction(0).getShort(null));
            bindVariableService.setByte(0, (byte) 22);
            Assert.assertEquals(22, bindVariableService.getFunction(0).getShort(null));
        });
    }

    @Test
    public void testSetShortToStr() throws Exception {
        assertMemoryLeak(() -> {
            bindVariableService.define(0, ColumnType.SHORT, 0);
            bindVariableService.setStr(0, "21");
            Assert.assertEquals(21, bindVariableService.getFunction(0).getShort(null));
        });
    }

    @Test
    public void testSetShortVarToShort() throws Exception {
        assertMemoryLeak(() -> {
            bindVariableService.setShort(0);
            bindVariableService.setShort(0, (short) 99);
            Assert.assertEquals(99, bindVariableService.getFunction(0).getShort(null));
            bindVariableService.setShort(0, (short) 71);
            Assert.assertEquals(71, bindVariableService.getFunction(0).getShort(null));
        });
    }

    @Test
    public void testSetStrToBoolean() throws Exception {
        assertMemoryLeak(() -> {
            bindVariableService.define(0, ColumnType.STRING, 0);
            bindVariableService.setBoolean(0, true);
            TestUtils.assertEquals("true", bindVariableService.getFunction(0).getStrA(null));
            bindVariableService.setBoolean(0, false);
            TestUtils.assertEquals("false", bindVariableService.getFunction(0).getStrA(null));
        });
    }

    @Test
    public void testSetStrToByte() throws Exception {
        assertMemoryLeak(() -> {
            bindVariableService.define(0, ColumnType.STRING, 0);
            bindVariableService.setByte(0, (byte) 22);
            TestUtils.assertEquals("22", bindVariableService.getFunction(0).getStrA(null));
        });
    }

    @Test
    public void testSetStrToLong256() throws Exception {
        assertMemoryLeak(() -> {
            bindVariableService.define(0, ColumnType.STRING, 0);
            bindVariableService.setLong256(0, 888, 777, 6666, 5555);
            TestUtils.assertEquals("0x15b30000000000001a0a00000000000003090000000000000378", bindVariableService.getFunction(0).getStrA(null));
            bindVariableService.setLong256(0);
            TestUtils.assertEquals("", bindVariableService.getFunction(0).getStrA(null));
        });
    }

    @Test
    public void testSetStrVarToShort() throws Exception {
        assertMemoryLeak(() -> {
            bindVariableService.setStr(0, "ello");
            TestUtils.assertEquals("ello", bindVariableService.getFunction(0).getStrA(null));
            bindVariableService.setShort(0, (short) 5);
            TestUtils.assertEquals("5", bindVariableService.getFunction(0).getStrA(null));
        });
    }

    @Test
    public void testSetTimestampNSToStr() throws Exception {
        assertMemoryLeak(() -> {
            bindVariableService.define(0, ColumnType.TIMESTAMP_NANO, 0);
            try {
                bindVariableService.setStr(0, "hello");
                Assert.fail();
            } catch (ImplicitCastException ignored) {
            }
            bindVariableService.setStr(0, "21");
            Assert.assertEquals(21, bindVariableService.getFunction(0).getTimestamp(null));
            bindVariableService.setStr(0, null);
            Assert.assertEquals(Numbers.LONG_NULL, bindVariableService.getFunction(0).getTimestamp(null));
            bindVariableService.setStr(0, "2019-10-31 15:05:22+08:00");
            Assert.assertEquals(1572505522000000000L, bindVariableService.getFunction(0).getTimestamp(null));
        });
    }

    @Test
    public void testSetTimestampToByte() throws Exception {
        assertMemoryLeak(() -> {
            bindVariableService.define(0, ColumnType.TIMESTAMP, 0);
            bindVariableService.setByte(0, (byte) 10);
            Assert.assertEquals(10, bindVariableService.getFunction(0).getTimestamp(null));
            bindVariableService.setByte(0, (byte) 22);
            Assert.assertEquals(22, bindVariableService.getFunction(0).getTimestamp(null));
            bindVariableService.define(1, ColumnType.TIMESTAMP_NANO, 0);
            bindVariableService.setByte(0, (byte) 19);
            Assert.assertEquals(19, bindVariableService.getFunction(0).getTimestamp(null));
            bindVariableService.setByte(0, (byte) 33);
            Assert.assertEquals(33, bindVariableService.getFunction(0).getTimestamp(null));
        });
    }

    @Test
    public void testSetTimestampToStr() throws Exception {
        assertMemoryLeak(() -> {
            bindVariableService.define(0, ColumnType.TIMESTAMP, 0);
            try {
                bindVariableService.setStr(0, "hello");
                Assert.fail();
            } catch (ImplicitCastException ignored) {
            }
            bindVariableService.setStr(0, "21");
            Assert.assertEquals(21, bindVariableService.getFunction(0).getTimestamp(null));
            bindVariableService.setStr(0, null);
            Assert.assertEquals(Numbers.LONG_NULL, bindVariableService.getFunction(0).getTimestamp(null));
            bindVariableService.setStr(0, "2019-10-31 15:05:22+08:00");
            Assert.assertEquals(1572505522000000L, bindVariableService.getFunction(0).getTimestamp(null));
        });
    }

    @Test
    public void testSetTimestampVarToShort() throws Exception {
        assertMemoryLeak(() -> {
            bindVariableService.setTimestamp(0, 10L);
            Assert.assertEquals(10, bindVariableService.getFunction(0).getTimestamp(null));
            bindVariableService.setShort(0, (short) 5);
            Assert.assertEquals(5, bindVariableService.getFunction(0).getTimestamp(null));

            bindVariableService.setTimestampNano(1, 29L);
            Assert.assertEquals(29, bindVariableService.getFunction(1).getTimestamp(null));
            bindVariableService.setShort(1, (short) 50);
            Assert.assertEquals(50, bindVariableService.getFunction(1).getTimestamp(null));
        });
    }

    @Test
    public void testStrIndexedOverride() throws Exception {
        assertMemoryLeak(() -> {
            bindVariableService.setLong(0, 10);
            try {
                bindVariableService.setStr(0, "ok");
                Assert.fail();
            } catch (ImplicitCastException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "inconvertible value: `ok` [STRING -> LONG]");
            }
        });
    }

    @Test
    public void testStrOverride() throws Exception {
        assertMemoryLeak(() -> {
            bindVariableService.setLong("a", 10);
            try {
                bindVariableService.setStr("a", "ok");
                Assert.fail();
            } catch (ImplicitCastException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "inconvertible value: `ok` [STRING -> LONG]");
            }
        });
    }

    @Test
    public void testStrVarSetToChar() throws Exception {
        assertMemoryLeak(() -> {
            bindVariableService.setStr(0);
            bindVariableService.setStr(0, "perfecto");
            TestUtils.assertEquals("perfecto", bindVariableService.getFunction(0).getStrA(null));

            bindVariableService.setChar(0, 'R');
            TestUtils.assertEquals("R", bindVariableService.getFunction(0).getStrA(null));
        });
    }

    @Test
    public void testStrVarSetToFloat() throws Exception {
        assertMemoryLeak(() -> {
            bindVariableService.setStr(0);
            bindVariableService.setStr(0, "1000.88");
            TestUtils.assertEquals("1000.88", bindVariableService.getFunction(0).getStrA(null));

            bindVariableService.setFloat(0, 451f);
            TestUtils.assertEquals("451.0", bindVariableService.getFunction(0).getStrA(null));

            bindVariableService.setFloat(0, Float.NaN);
            Assert.assertNull(bindVariableService.getFunction(0).getStrA(null));
        });
    }

    @Test
    public void testStrVarSetToLong() throws Exception {
        assertMemoryLeak(() -> {
            bindVariableService.setStr(0);
            bindVariableService.setStr(0, "perfecto");
            TestUtils.assertEquals("perfecto", bindVariableService.getFunction(0).getStrA(null));

            bindVariableService.setLong(0, 450);
            TestUtils.assertEquals("450", bindVariableService.getFunction(0).getStrA(null));

            bindVariableService.setLong(0, Numbers.LONG_NULL);
            Assert.assertNull(bindVariableService.getFunction(0).getStrA(null));
        });
    }

    @Test
    public void testStringVarSetToDouble() throws Exception {
        assertMemoryLeak(() -> {
            bindVariableService.setStr(0);
            bindVariableService.setStr(0, "test1");
            TestUtils.assertEquals("test1", bindVariableService.getFunction(0).getStrA(null));
            TestUtils.assertEquals("test1", bindVariableService.getFunction(0).getStrB(null));
            Assert.assertEquals(5, bindVariableService.getFunction(0).getStrLen(null));
            bindVariableService.setDouble(0, 123.456d);
            TestUtils.assertEquals("123.456", bindVariableService.getFunction(0).getStrA(null));
            bindVariableService.setDouble(0, Double.NaN);
            Assert.assertNull(bindVariableService.getFunction(0).getStrA(null));
        });
    }

    @Test
    public void testStringVarSetToInt() throws Exception {
        assertMemoryLeak(() -> {
            bindVariableService.setStr(0);
            bindVariableService.setStr(0, "test1");
            TestUtils.assertEquals("test1", bindVariableService.getFunction(0).getStrA(null));

            bindVariableService.setInt(0, 450);
            TestUtils.assertEquals("450", bindVariableService.getFunction(0).getStrA(null));

            bindVariableService.setInt(0, Numbers.INT_NULL);
            Assert.assertNull(bindVariableService.getFunction(0).getStrA(null));
        });
    }

    @Test
    public void testTimestampNSVarSetToDate() throws Exception {
        assertMemoryLeak(() -> {
            bindVariableService.setTimestampNano(0);
            bindVariableService.setDate(0, 99999001L);
            Assert.assertEquals(99999001000000L, bindVariableService.getFunction(0).getTimestamp(null));

            bindVariableService.setTimestamp(0, Numbers.LONG_NULL);
            final long d = bindVariableService.getFunction(0).getTimestamp(null);
            Assert.assertEquals(Numbers.LONG_NULL, d);
        });
    }

    @Test
    public void testTimestampNSVarSetToInt() throws Exception {
        assertMemoryLeak(() -> {
            bindVariableService.setTimestampNano(0);
            bindVariableService.setTimestampNano(0, 99999001L);
            Assert.assertEquals(99999001, bindVariableService.getFunction(0).getTimestamp(null));

            bindVariableService.setInt(0, 450);
            Assert.assertEquals(450, bindVariableService.getFunction(0).getTimestamp(null));

            bindVariableService.setInt(0, Numbers.INT_NULL);
            final long d = bindVariableService.getFunction(0).getTimestamp(null);
            Assert.assertEquals(Numbers.LONG_NULL, d);
        });
    }

    @Test
    public void testTimestampNSVarSetToTimestamp() throws Exception {
        assertMemoryLeak(() -> {
            bindVariableService.setTimestampNano(0);
            bindVariableService.setTimestamp(0, 99999001L);
            Assert.assertEquals(99999001000L, bindVariableService.getFunction(0).getTimestamp(null));
        });
    }

    @Test
    public void testTimestampOverride() throws Exception {
        assertMemoryLeak(() -> {
            bindVariableService.setLong("a", 10);
            Assert.assertEquals(10, bindVariableService.getFunction(":a").getLong(null));
            bindVariableService.setTimestamp("a", 5);
            Assert.assertEquals(5, bindVariableService.getFunction(":a").getLong(null));
            bindVariableService.setTimestampNano("a", 2);
            Assert.assertEquals(2, bindVariableService.getFunction(":a").getLong(null));
        });
    }

    @Test
    public void testTimestampVarSetToDate() throws Exception {
        assertMemoryLeak(() -> {
            bindVariableService.setTimestamp(0);
            bindVariableService.setDate(0, 99999001L);
            Assert.assertEquals(99999001000L, bindVariableService.getFunction(0).getTimestamp(null));

            bindVariableService.setTimestamp(0, Numbers.LONG_NULL);
            final long d = bindVariableService.getFunction(0).getTimestamp(null);
            Assert.assertEquals(Numbers.LONG_NULL, d);
        });
    }

    @Test
    public void testTimestampVarSetToInt() throws Exception {
        assertMemoryLeak(() -> {
            bindVariableService.setTimestamp(0);
            bindVariableService.setTimestamp(0, 99999001L);
            Assert.assertEquals(99999001, bindVariableService.getFunction(0).getTimestamp(null));

            bindVariableService.setInt(0, 450);
            Assert.assertEquals(450, bindVariableService.getFunction(0).getTimestamp(null));

            bindVariableService.setInt(0, Numbers.INT_NULL);
            final long d = bindVariableService.getFunction(0).getTimestamp(null);
            Assert.assertEquals(Numbers.LONG_NULL, d);
        });
    }

    @Test
    public void testTimestampVarSetToTimestampNS() throws Exception {
        assertMemoryLeak(() -> {
            bindVariableService.setTimestamp(0);
            bindVariableService.setTimestampNano(0, 99999000001L);
            Assert.assertEquals(99999000, bindVariableService.getFunction(0).getTimestamp(null));
        });
    }

    @Test
    public void testVarcharClearResetsState() throws Exception {
        assertMemoryLeak(() -> {
            bindVariableService.define(0, ColumnType.VARCHAR, 0);
            bindVariableService.setStr(0, "some value");
            Assert.assertNotNull(bindVariableService.getFunction(0).getVarcharA(null));
            Assert.assertEquals(10, bindVariableService.getFunction(0).getVarcharSize(null));

            bindVariableService.clear();

            bindVariableService.define(0, ColumnType.VARCHAR, 0);
            Assert.assertNull(bindVariableService.getFunction(0).getVarcharA(null));
            Assert.assertEquals(-1, bindVariableService.getFunction(0).getVarcharSize(null));
        });
    }

    @Test
    public void testVarcharGetStrABConsistency() throws Exception {
        assertMemoryLeak(() -> {
            bindVariableService.define(0, ColumnType.VARCHAR, 0);
            bindVariableService.setStr(0, "consistent");

            TestUtils.assertEquals("consistent", bindVariableService.getFunction(0).getStrA(null));
            TestUtils.assertEquals("consistent", bindVariableService.getFunction(0).getStrB(null));
            Assert.assertEquals(10, bindVariableService.getFunction(0).getStrLen(null));

            bindVariableService.setStr(0, null);
            Assert.assertNull(bindVariableService.getFunction(0).getStrA(null));
            Assert.assertNull(bindVariableService.getFunction(0).getStrB(null));
            Assert.assertEquals(-1, bindVariableService.getFunction(0).getStrLen(null));
        });
    }

    @Test
    public void testVarcharIndexedVariableSetToMultipleTypes() throws Exception {
        assertMemoryLeak(() -> {
            bindVariableService.setVarchar(0);

            bindVariableService.setInt(0, 999);
            TestUtils.assertEquals("999", bindVariableService.getFunction(0).getVarcharA(null));

            bindVariableService.setChar(0, 'Z');
            TestUtils.assertEquals("Z", bindVariableService.getFunction(0).getVarcharA(null));

            bindVariableService.setLong(0, 123456789L);
            TestUtils.assertEquals("123456789", bindVariableService.getFunction(0).getVarcharA(null));

            bindVariableService.setBoolean(0, false);
            TestUtils.assertEquals("false", bindVariableService.getFunction(0).getVarcharA(null));
        });
    }

    @Test
    public void testVarcharMultipleUpdates() throws Exception {
        assertMemoryLeak(() -> {
            bindVariableService.define(0, ColumnType.VARCHAR, 0);

            bindVariableService.setInt(0, 123);
            TestUtils.assertEquals("123", bindVariableService.getFunction(0).getVarcharA(null));
            Assert.assertEquals(3, bindVariableService.getFunction(0).getVarcharSize(null));

            bindVariableService.setStr(0, "text");
            TestUtils.assertEquals("text", bindVariableService.getFunction(0).getVarcharA(null));
            Assert.assertEquals(4, bindVariableService.getFunction(0).getVarcharSize(null));

            bindVariableService.setDouble(0, 456.789);
            TestUtils.assertEquals("456.789", bindVariableService.getFunction(0).getVarcharA(null));
            Assert.assertTrue(bindVariableService.getFunction(0).getVarcharSize(null) > 0);

            bindVariableService.setBoolean(0, true);
            TestUtils.assertEquals("true", bindVariableService.getFunction(0).getVarcharA(null));
            Assert.assertEquals(4, bindVariableService.getFunction(0).getVarcharSize(null));

            bindVariableService.setVarchar(0, new Utf8String("foo"));
            TestUtils.assertEquals("foo", bindVariableService.getFunction(0).getVarcharA(null));
            Assert.assertEquals(3, bindVariableService.getFunction(0).getVarcharSize(null));
        });
    }

    @Test
    public void testVarcharSetToBoolean() throws Exception {
        assertMemoryLeak(() -> {
            bindVariableService.define(0, ColumnType.VARCHAR, 0);
            bindVariableService.setBoolean(0, true);
            TestUtils.assertEquals("true", bindVariableService.getFunction(0).getVarcharA(null));
            Assert.assertEquals(4, bindVariableService.getFunction(0).getVarcharSize(null));

            bindVariableService.setBoolean(0, false);
            TestUtils.assertEquals("false", bindVariableService.getFunction(0).getVarcharA(null));
            Assert.assertEquals(5, bindVariableService.getFunction(0).getVarcharSize(null));
        });
    }

    @Test
    public void testVarcharSetToByte() throws Exception {
        assertMemoryLeak(() -> {
            bindVariableService.define(0, ColumnType.VARCHAR, 0);
            bindVariableService.setByte(0, (byte) 127);
            TestUtils.assertEquals("127", bindVariableService.getFunction(0).getVarcharA(null));
            Assert.assertEquals(3, bindVariableService.getFunction(0).getVarcharSize(null));

            bindVariableService.setByte(0, (byte) -128);
            TestUtils.assertEquals("-128", bindVariableService.getFunction(0).getVarcharA(null));
            Assert.assertEquals(4, bindVariableService.getFunction(0).getVarcharSize(null));

            bindVariableService.setByte(0, (byte) 0);
            TestUtils.assertEquals("0", bindVariableService.getFunction(0).getVarcharA(null));
            Assert.assertEquals(1, bindVariableService.getFunction(0).getVarcharSize(null));
        });
    }

    @Test
    public void testVarcharSetToChar() throws Exception {
        assertMemoryLeak(() -> {
            bindVariableService.define(0, ColumnType.VARCHAR, 0);
            bindVariableService.setChar(0, 'A');
            TestUtils.assertEquals("A", bindVariableService.getFunction(0).getVarcharA(null));
            Assert.assertEquals(1, bindVariableService.getFunction(0).getVarcharSize(null));

            bindVariableService.setChar(0, '9');
            TestUtils.assertEquals("9", bindVariableService.getFunction(0).getVarcharA(null));
            Assert.assertEquals(1, bindVariableService.getFunction(0).getVarcharSize(null));

            bindVariableService.setChar(0, '\u03B1'); // Greek alpha
            TestUtils.assertEquals("α", bindVariableService.getFunction(0).getVarcharA(null));
            Assert.assertEquals(2, bindVariableService.getFunction(0).getVarcharSize(null)); // UTF-8 encoding
        });
    }

    @Test
    public void testVarcharSetToDate() throws Exception {
        assertMemoryLeak(() -> {
            bindVariableService.define(0, ColumnType.VARCHAR, 0);
            bindVariableService.setDate(0, 1609459200000L); // 2021-01-01 (in millis)

            Assert.assertNotNull(bindVariableService.getFunction(0).getVarcharA(null));
            Assert.assertTrue(bindVariableService.getFunction(0).getVarcharSize(null) > 0);

            bindVariableService.setDate(0, Numbers.LONG_NULL);
            Assert.assertNull(bindVariableService.getFunction(0).getVarcharA(null));
            Assert.assertEquals(-1, bindVariableService.getFunction(0).getVarcharSize(null));
        });
    }

    @Test
    public void testVarcharSetToDouble() throws Exception {
        assertMemoryLeak(() -> {
            bindVariableService.define(0, ColumnType.VARCHAR, 0);
            bindVariableService.setDouble(0, 3.141592653589793);
            TestUtils.assertEquals("3.141592653589793", bindVariableService.getFunction(0).getVarcharA(null));
            Assert.assertTrue(bindVariableService.getFunction(0).getVarcharSize(null) > 0);

            bindVariableService.setDouble(0, -1.23456789E-100);
            TestUtils.assertEquals("-1.23456789E-100", bindVariableService.getFunction(0).getVarcharA(null));
            Assert.assertTrue(bindVariableService.getFunction(0).getVarcharSize(null) > 0);

            bindVariableService.setDouble(0, Double.NaN);
            Assert.assertNull(bindVariableService.getFunction(0).getVarcharA(null));
            Assert.assertEquals(-1, bindVariableService.getFunction(0).getVarcharSize(null));

            bindVariableService.setDouble(0, Double.NEGATIVE_INFINITY);
            Assert.assertNull(bindVariableService.getFunction(0).getVarcharA(null));
            Assert.assertEquals(-1, bindVariableService.getFunction(0).getVarcharSize(null));
        });
    }

    @Test
    public void testVarcharSetToFloat() throws Exception {
        assertMemoryLeak(() -> {
            bindVariableService.define(0, ColumnType.VARCHAR, 0);
            bindVariableService.setFloat(0, 3.14159f);
            TestUtils.assertEquals("3.14159", bindVariableService.getFunction(0).getVarcharA(null));
            Assert.assertTrue(bindVariableService.getFunction(0).getVarcharSize(null) > 0);

            bindVariableService.setFloat(0, -0.0001f);
            TestUtils.assertEquals("-1.0E-4", bindVariableService.getFunction(0).getVarcharA(null));
            Assert.assertTrue(bindVariableService.getFunction(0).getVarcharSize(null) > 0);

            bindVariableService.setFloat(0, Float.NaN);
            Assert.assertNull(bindVariableService.getFunction(0).getVarcharA(null));
            Assert.assertEquals(-1, bindVariableService.getFunction(0).getVarcharSize(null));

            bindVariableService.setFloat(0, Float.POSITIVE_INFINITY);
            Assert.assertNull(bindVariableService.getFunction(0).getVarcharA(null));
            Assert.assertEquals(-1, bindVariableService.getFunction(0).getVarcharSize(null));
        });
    }

    @Test
    public void testVarcharSetToInt() throws Exception {
        assertMemoryLeak(() -> {
            bindVariableService.define(0, ColumnType.VARCHAR, 0);
            bindVariableService.setInt(0, 2147483647);
            TestUtils.assertEquals("2147483647", bindVariableService.getFunction(0).getVarcharA(null));
            Assert.assertEquals(10, bindVariableService.getFunction(0).getVarcharSize(null));

            bindVariableService.setInt(0, -1234567890);
            TestUtils.assertEquals("-1234567890", bindVariableService.getFunction(0).getVarcharA(null));
            Assert.assertEquals(11, bindVariableService.getFunction(0).getVarcharSize(null));

            bindVariableService.setInt(0, Numbers.INT_NULL);
            Assert.assertNull(bindVariableService.getFunction(0).getVarcharA(null));
            Assert.assertEquals(-1, bindVariableService.getFunction(0).getVarcharSize(null));
        });
    }

    @Test
    public void testVarcharSetToLong() throws Exception {
        assertMemoryLeak(() -> {
            bindVariableService.define(0, ColumnType.VARCHAR, 0);
            bindVariableService.setLong(0, 9223372036854775807L);
            TestUtils.assertEquals("9223372036854775807", bindVariableService.getFunction(0).getVarcharA(null));
            Assert.assertEquals(19, bindVariableService.getFunction(0).getVarcharSize(null));

            bindVariableService.setLong(0, -1234567890123456789L);
            TestUtils.assertEquals("-1234567890123456789", bindVariableService.getFunction(0).getVarcharA(null));
            Assert.assertEquals(20, bindVariableService.getFunction(0).getVarcharSize(null));

            bindVariableService.setLong(0, Numbers.LONG_NULL);
            Assert.assertNull(bindVariableService.getFunction(0).getVarcharA(null));
            Assert.assertEquals(-1, bindVariableService.getFunction(0).getVarcharSize(null));
        });
    }

    @Test
    public void testVarcharSetToLong256() throws Exception {
        assertMemoryLeak(() -> {
            bindVariableService.define(0, ColumnType.VARCHAR, 0);
            bindVariableService.setLong256(0, 888, 777, 6666, 5555);
            TestUtils.assertEquals("0x15b30000000000001a0a00000000000003090000000000000378", bindVariableService.getFunction(0).getVarcharA(null));
            Assert.assertEquals(54, bindVariableService.getFunction(0).getVarcharSize(null));

            bindVariableService.setLong256(0, 0, 0, 0, 0);
            TestUtils.assertEquals("0x00", bindVariableService.getFunction(0).getVarcharA(null));
            Assert.assertEquals(4, bindVariableService.getFunction(0).getVarcharSize(null));
        });
    }

    @Test
    public void testVarcharSetToShort() throws Exception {
        assertMemoryLeak(() -> {
            bindVariableService.define(0, ColumnType.VARCHAR, 0);
            bindVariableService.setShort(0, (short) 32767);
            TestUtils.assertEquals("32767", bindVariableService.getFunction(0).getVarcharA(null));
            Assert.assertEquals(5, bindVariableService.getFunction(0).getVarcharSize(null));

            bindVariableService.setShort(0, (short) -32768);
            TestUtils.assertEquals("-32768", bindVariableService.getFunction(0).getVarcharA(null));
            Assert.assertEquals(6, bindVariableService.getFunction(0).getVarcharSize(null));

            bindVariableService.setShort(0, (short) 0);
            TestUtils.assertEquals("0", bindVariableService.getFunction(0).getVarcharA(null));
            Assert.assertEquals(1, bindVariableService.getFunction(0).getVarcharSize(null));
        });
    }

    @Test
    public void testVarcharSetToString() throws Exception {
        assertMemoryLeak(() -> {
            bindVariableService.define(0, ColumnType.VARCHAR, 0);
            bindVariableService.setStr(0, "Hello World!");
            TestUtils.assertEquals("Hello World!", bindVariableService.getFunction(0).getVarcharA(null));
            Assert.assertEquals(12, bindVariableService.getFunction(0).getVarcharSize(null));

            bindVariableService.setStr(0, "");
            TestUtils.assertEquals("", bindVariableService.getFunction(0).getVarcharA(null));
            Assert.assertEquals(0, bindVariableService.getFunction(0).getVarcharSize(null));

            bindVariableService.setStr(0, null);
            Assert.assertNull(bindVariableService.getFunction(0).getVarcharA(null));
            Assert.assertEquals(-1, bindVariableService.getFunction(0).getVarcharSize(null));

            bindVariableService.setStr(0, "Hello 世界 🌍");
            TestUtils.assertEquals("Hello 世界 🌍", bindVariableService.getFunction(0).getVarcharA(null));
            Assert.assertEquals("Hello 世界 🌍".getBytes(StandardCharsets.UTF_8).length, bindVariableService.getFunction(0).getVarcharSize(null));
        });
    }

    @Test
    public void testVarcharSetToTimestamp() throws Exception {
        assertMemoryLeak(() -> {
            bindVariableService.define(0, ColumnType.VARCHAR, 0);
            bindVariableService.setTimestamp(0, 1609459200000000L); // 2021-01-01 00:00:00.000000

            Assert.assertNotNull(bindVariableService.getFunction(0).getVarcharA(null));
            TestUtils.assertEquals("2021-01-01T00:00:00.000000Z", bindVariableService.getFunction(0).getVarcharA(null));
            Assert.assertTrue(bindVariableService.getFunction(0).getVarcharSize(null) > 0);

            bindVariableService.setTimestamp(0, Numbers.LONG_NULL);
            Assert.assertNull(bindVariableService.getFunction(0).getVarcharA(null));
            Assert.assertEquals(-1, bindVariableService.getFunction(0).getVarcharSize(null));
        });
    }

    @Test
    public void testVarcharSetToUuid() throws Exception {
        assertMemoryLeak(() -> {
            bindVariableService.define(0, ColumnType.VARCHAR, 0);
            bindVariableService.setUuid(0, 0x550e8400e29b41d4L, 0xa7164d67e84bcL);
            TestUtils.assertEquals("000a7164-d67e-84bc-550e-8400e29b41d4", bindVariableService.getFunction(0).getVarcharA(null));
            Assert.assertEquals(36, bindVariableService.getFunction(0).getVarcharSize(null));

            bindVariableService.setUuid(0, 0L, 0L);
            TestUtils.assertEquals("00000000-0000-0000-0000-000000000000", bindVariableService.getFunction(0).getVarcharA(null));
            Assert.assertEquals(36, bindVariableService.getFunction(0).getVarcharSize(null));

            bindVariableService.setUuid(0, Numbers.LONG_NULL, Numbers.LONG_NULL);
            Assert.assertNull(bindVariableService.getFunction(0).getVarcharA(null));
            Assert.assertEquals(-1, bindVariableService.getFunction(0).getVarcharSize(null));
        });
    }
}
