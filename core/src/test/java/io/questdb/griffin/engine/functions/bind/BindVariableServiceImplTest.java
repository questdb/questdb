/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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

package io.questdb.griffin.engine.functions.bind;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.DefaultCairoConfiguration;
import io.questdb.cairo.ImplicitCastException;
import io.questdb.cairo.sql.BindVariableService;
import io.questdb.griffin.SqlException;
import io.questdb.std.Long256Impl;
import io.questdb.std.Numbers;
import io.questdb.std.str.StringSink;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class BindVariableServiceImplTest {
    private final static BindVariableService bindVariableService = new BindVariableServiceImpl(new DefaultCairoConfiguration(null));

    @Before
    public void setUp() {
        bindVariableService.clear();
    }

    @Test
    public void testBinIndexedOverride() throws SqlException {
        bindVariableService.setLong(0, 10);
        try {
            bindVariableService.setBin(0, null);
            Assert.fail();
        } catch (SqlException e) {
            TestUtils.assertContains(e.getFlyweightMessage(), "bind variable at 0 is already defined as LONG");
        }
    }

    @Test
    public void testBinOverride() throws SqlException {
        bindVariableService.setLong("a", 10);
        try {
            bindVariableService.setBin("a", null);
            Assert.fail();
        } catch (SqlException e) {
            TestUtils.assertContains(e.getFlyweightMessage(), "bind variable 'a' is already defined as LONG");
        }
    }

    @Test
    public void testBooleanIndexedOverride() throws SqlException {
        bindVariableService.setLong(0, 10);
        try {
            bindVariableService.setBoolean(0, false);
            Assert.fail();
        } catch (SqlException e) {
            TestUtils.assertContains(e.getFlyweightMessage(), "bind variable at 0 is defined as LONG and cannot accept BOOLEAN");
        }
    }

    @Test
    public void testBooleanOverride() throws SqlException {
        bindVariableService.setLong("a", 10);
        try {
            bindVariableService.setBoolean("a", false);
            Assert.fail();
        } catch (SqlException e) {
            TestUtils.assertContains(e.getFlyweightMessage(), "bind variable 'a' is defined as LONG and cannot accept BOOLEAN");
        }
    }

    @Test
    public void testBooleanVarSetToInt() throws SqlException {
        bindVariableService.setBoolean(0);
        bindVariableService.setBoolean(0, true);
        Assert.assertTrue(bindVariableService.getFunction(0).getBool(null));

        try {
            bindVariableService.setInt(0, 123);
            Assert.fail();
        } catch (SqlException e) {
            TestUtils.assertContains(e.getFlyweightMessage(), "bind variable at 0 is defined as BOOLEAN and cannot accept INT");
        }
    }

    @Test
    public void testCharIndexedOverride() throws SqlException {
        bindVariableService.setInt(2, 10);
        try {
            bindVariableService.setChar(2, 'o');
            Assert.fail();
        } catch (ImplicitCastException e) {
            TestUtils.assertContains(e.getFlyweightMessage(), "inconvertible value");
        }
    }

    @Test
    public void testCharOverride() throws SqlException {
        bindVariableService.setLong("a", 10);
        try {
            bindVariableService.setChar("a", 'k');
            Assert.fail();
        } catch (ImplicitCastException e) {
            TestUtils.assertContains(e.getFlyweightMessage(), "inconvertible value");
        }
    }

    @Test
    public void testCharVarSetToChar() throws SqlException {
        bindVariableService.setChar(0);
        bindVariableService.setChar(0, 'X');
        Assert.assertEquals('X', bindVariableService.getFunction(0).getChar(null));

        bindVariableService.setChar(0, 'R');
        Assert.assertEquals('R', bindVariableService.getFunction(0).getChar(null));
    }

    @Test
    public void testDateVarSetToInt() throws SqlException {
        bindVariableService.setDate(0);
        bindVariableService.setDate(0, 99999001);
        Assert.assertEquals(99999001, bindVariableService.getFunction(0).getDate(null));

        bindVariableService.setInt(0, 450);
        Assert.assertEquals(450, bindVariableService.getFunction(0).getDate(null));

        bindVariableService.setInt(0, Numbers.INT_NaN);
        final long d = bindVariableService.getFunction(0).getDate(null);
        Assert.assertEquals(Numbers.LONG_NaN, d);
    }

    @Test
    public void testDateVarSetToLong() throws SqlException {
        bindVariableService.setDate(0);
        bindVariableService.setDate(0, 99999001);
        Assert.assertEquals(99999001, bindVariableService.getFunction(0).getDate(null));

        bindVariableService.setLong(0, 450);
        Assert.assertEquals(450, bindVariableService.getFunction(0).getDate(null));

        bindVariableService.setLong(0, Numbers.LONG_NaN);
        final long d = bindVariableService.getFunction(0).getDate(null);
        Assert.assertEquals(Numbers.LONG_NaN, d);
    }

    @Test
    public void testDateVarSetToTimestamp() throws SqlException {
        bindVariableService.setDate(0);
        bindVariableService.setTimestamp(0, 99999001);
        Assert.assertEquals(99999, bindVariableService.getFunction(0).getDate(null));

        bindVariableService.setTimestamp(0, Numbers.LONG_NaN);
        final long d = bindVariableService.getFunction(0).getDate(null);
        Assert.assertEquals(Numbers.LONG_NaN, d);
    }

    @Test
    public void testDoubleIndexedOverride() throws SqlException {
        bindVariableService.setInt(2, 10);
        try {
            bindVariableService.setDouble(2, 5.4);
            Assert.fail();
        } catch (SqlException e) {
            TestUtils.assertContains(e.getFlyweightMessage(), "bind variable at 2 is defined as INT and cannot accept DOUBLE");
        }
    }

    @Test
    public void testDoubleOverride() throws SqlException {
        bindVariableService.setInt("a", 10);
        try {
            bindVariableService.setDouble("a", 5.4);
            Assert.fail();
        } catch (SqlException e) {
            TestUtils.assertContains(e.getFlyweightMessage(), "bind variable 'a' is defined as INT and cannot accept DOUBLE");
        }
    }

    @Test
    public void testDoubleVarSetToFloat() throws SqlException {
        bindVariableService.setDouble(0);
        bindVariableService.setDouble(0, 1000.88);
        Assert.assertEquals(1000.88, bindVariableService.getFunction(0).getDouble(null), 0.000001);

        bindVariableService.setFloat(0, 451f);
        Assert.assertEquals(451, bindVariableService.getFunction(0).getDouble(null), 0.000001);

        bindVariableService.setFloat(0, Float.NaN);
        final double d = bindVariableService.getFunction(0).getDouble(null);
        Assert.assertTrue(d != d);
    }

    @Test
    public void testDoubleVarSetToInt() throws SqlException {
        bindVariableService.setDouble(0);
        bindVariableService.setDouble(0, 1000.88);
        Assert.assertEquals(1000.88, bindVariableService.getFunction(0).getDouble(null), 0.000001);

        bindVariableService.setInt(0, 450);
        Assert.assertEquals(450, bindVariableService.getFunction(0).getDouble(null), 0.000001);

        bindVariableService.setInt(0, Numbers.INT_NaN);
        final double d = bindVariableService.getFunction(0).getDouble(null);
        Assert.assertTrue(d != d);
    }

    @Test
    public void testDoubleVarSetToLong() throws SqlException {
        bindVariableService.setDouble(0);
        bindVariableService.setDouble(0, 91.3);
        Assert.assertEquals(91.3, bindVariableService.getFunction(0).getDouble(null), 0.00001);

        bindVariableService.setLong(0, 450);
        Assert.assertEquals(450, bindVariableService.getFunction(0).getDouble(null), 0.00001);

        bindVariableService.setLong(0, Numbers.LONG_NaN);
        final double f = bindVariableService.getFunction(0).getDouble(null);
        Assert.assertTrue(f != f);
    }

    @Test
    public void testFloatIndexedOverride() throws SqlException {
        bindVariableService.setLong(1, 10);
        try {
            bindVariableService.setFloat(1, 5);
            Assert.fail();
        } catch (SqlException e) {
            TestUtils.assertContains(e.getFlyweightMessage(), "bind variable at 1 is defined as LONG and cannot accept FLOAT");
        }
    }

    @Test
    public void testFloatOverride() throws SqlException {
        bindVariableService.setLong("a", 10);
        try {
            bindVariableService.setFloat("a", 5);
            Assert.fail();
        } catch (SqlException e) {
            TestUtils.assertContains(e.getFlyweightMessage(), "bind variable 'a' is defined as LONG and cannot accept FLOAT");
        }
    }

    @Test
    public void testFloatVarSetToInt() throws SqlException {
        bindVariableService.setFloat(0);
        bindVariableService.setFloat(0, 1000.88f);
        Assert.assertEquals(1000.88f, bindVariableService.getFunction(0).getFloat(null), 0.000001);

        bindVariableService.setInt(0, 450);
        Assert.assertEquals(450, bindVariableService.getFunction(0).getFloat(null), 0.000001);

        bindVariableService.setInt(0, Numbers.INT_NaN);
        final float d = bindVariableService.getFunction(0).getFloat(null);
        Assert.assertTrue(d != d);
    }

    @Test
    public void testFloatVarSetToLong() throws SqlException {
        bindVariableService.setFloat(0);
        bindVariableService.setFloat(0, 91.3f);
        Assert.assertEquals(91.3f, bindVariableService.getFunction(0).getFloat(null), 0.00001);

        bindVariableService.setLong(0, 450);
        Assert.assertEquals(450, bindVariableService.getFunction(0).getFloat(null), 0.00001);

        bindVariableService.setLong(0, Numbers.LONG_NaN);
        final float f = bindVariableService.getFunction(0).getFloat(null);
        Assert.assertTrue(f != f);
    }

    @Test
    public void testIntVarSetToInt() throws SqlException {
        bindVariableService.setInt(0);
        bindVariableService.setInt(0, 99999001);
        Assert.assertEquals(99999001, bindVariableService.getFunction(0).getInt(null));
    }

    @Test
    public void testLongIndexedOverride() throws SqlException {
        bindVariableService.setInt(0, 10);
        bindVariableService.setLong(0, 5);
        Assert.assertEquals(5, bindVariableService.getFunction(0).getInt(null));
    }

    @Test
    public void testLongOverride() throws SqlException {
        bindVariableService.setInt("a", 10);
        bindVariableService.setLong("a", 5);
        // allow INT to long truncate
        Assert.assertEquals(5, bindVariableService.getFunction(":a").getInt(null));
    }

    @Test
    public void testLongVarSetToInt() throws SqlException {
        bindVariableService.setLong(0);
        bindVariableService.setLong(0, 99999001);
        Assert.assertEquals(99999001, bindVariableService.getFunction(0).getLong(null));

        bindVariableService.setInt(0, 450);
        Assert.assertEquals(450, bindVariableService.getFunction(0).getLong(null));

        bindVariableService.setInt(0, Numbers.INT_NaN);
        final long d = bindVariableService.getFunction(0).getLong(null);
        Assert.assertEquals(Numbers.LONG_NaN, d);
    }

    @Test
    public void testNamedSetBooleanToBooleanNoDefine() throws SqlException {
        bindVariableService.setBoolean("x", true);
        Assert.assertTrue(bindVariableService.getFunction(":x").getBool(null));
        bindVariableService.setBoolean("x", false);
        Assert.assertFalse(bindVariableService.getFunction(":x").getBool(null));
    }

    @Test
    public void testNamedSetByteToByteNoDefine() throws SqlException {
        bindVariableService.setByte("x", (byte) 23);
        Assert.assertEquals(23, bindVariableService.getFunction(":x").getByte(null));
        bindVariableService.setByte("x", (byte) 45);
        Assert.assertEquals(45, bindVariableService.getFunction(":x").getByte(null));
    }

    @Test
    public void testNamedSetCharToCharNoDefine() throws SqlException {
        bindVariableService.setChar("x", 'V');
        Assert.assertEquals('V', bindVariableService.getFunction(":x").getChar(null));
        bindVariableService.setChar("x", 'Z');
        Assert.assertEquals('Z', bindVariableService.getFunction(":x").getChar(null));
    }

    @Test
    public void testNamedSetDateToDateNoDefine() throws SqlException {
        bindVariableService.setDate("x", 9990000011L);
        Assert.assertEquals(9990000011L, bindVariableService.getFunction(":x").getDate(null));
        bindVariableService.setDate("x", 9990000022L);
        Assert.assertEquals(9990000022L, bindVariableService.getFunction(":x").getDate(null));
    }

    @Test
    public void testNamedSetDoubleToDoubleNoDefine() throws SqlException {
        bindVariableService.setDouble("x", 17.3);
        Assert.assertEquals(17.299999237060547, bindVariableService.getFunction(":x").getDouble(null), 0.00001);
        bindVariableService.setDouble("x", 19.3);
        Assert.assertEquals(19.299999237060547, bindVariableService.getFunction(":x").getDouble(null), 0.00001);
    }

    @Test
    public void testNamedSetFloatToFloatNoDefine() throws SqlException {
        bindVariableService.setFloat("x", 17.3f);
        Assert.assertEquals(17.299999237060547f, bindVariableService.getFunction(":x").getFloat(null), 0.00001);
        bindVariableService.setFloat("x", 19.3f);
        Assert.assertEquals(19.299999237060547, bindVariableService.getFunction(":x").getFloat(null), 0.00001);
    }

    @Test
    public void testNamedSetIntToIntNoDefine() throws SqlException {
        bindVariableService.setInt("x", 90001);
        Assert.assertEquals(90001, bindVariableService.getFunction(":x").getInt(null));
        bindVariableService.setInt("x", 90002);
        Assert.assertEquals(90002, bindVariableService.getFunction(":x").getInt(null));
    }

    @Test
    public void testNamedSetLong256ToLong256() throws SqlException {
        bindVariableService.setLong256("x");
        bindVariableService.setLong256("x", 888, 777, 6666, 5555);
        StringSink sink = new StringSink();
        bindVariableService.getFunction(":x").getLong256(null, sink);
        TestUtils.assertEquals("0x15b30000000000001a0a00000000000003090000000000000378", sink);
    }

    @Test
    public void testNamedSetLong256ToLong256AsObj() throws SqlException {
        final Long256Impl long256 = new Long256Impl();
        long256.setAll(888, 999, 777, 111);
        bindVariableService.setLong256("x");
        bindVariableService.setLong256("x", long256);
        final StringSink sink = new StringSink();
        bindVariableService.getFunction(":x").getLong256(null, sink);
        TestUtils.assertEquals("0x6f000000000000030900000000000003e70000000000000378", sink);
    }

    @Test
    public void testNamedSetLong256ToLong256NoDefine() throws SqlException {
        bindVariableService.setLong256("x", 888, 777, 6666, 5555);
        StringSink sink = new StringSink();
        bindVariableService.getFunction(":x").getLong256(null, sink);
        TestUtils.assertEquals("0x15b30000000000001a0a00000000000003090000000000000378", sink);
    }

    @Test
    public void testNamedSetShortToShortNoDefine() throws SqlException {
        bindVariableService.setShort("x", (short) 1201);
        Assert.assertEquals(1201, bindVariableService.getFunction(":x").getShort(null));
        bindVariableService.setShort("x", (short) 1203);
        Assert.assertEquals(1203, bindVariableService.getFunction(":x").getShort(null));
    }

    @Test
    public void testNamedSetStrToStrNoDefine() throws SqlException {
        bindVariableService.setStr("x", "hello_x");
        TestUtils.assertEquals("hello_x", bindVariableService.getFunction(":x").getStr(null));
        bindVariableService.setStr("x", "hello_y");
        TestUtils.assertEquals("hello_y", bindVariableService.getFunction(":x").getStr(null));
    }

    @Test
    public void testNamedSetTimestampToTimestampNoDefine() throws SqlException {
        bindVariableService.setTimestamp("x", 9990000011L);
        Assert.assertEquals(9990000011L, bindVariableService.getFunction(":x").getTimestamp(null));
        bindVariableService.setTimestamp("x", 9990000022L);
        Assert.assertEquals(9990000022L, bindVariableService.getFunction(":x").getTimestamp(null));
    }

    @Test
    public void testSetBinToStr() throws SqlException {
        bindVariableService.define(0, ColumnType.BINARY, 0);
        try {
            bindVariableService.setStr(0, "21.2");
        } catch (SqlException e) {
            TestUtils.assertContains(e.getFlyweightMessage(), "bind variable at 0 is defined as BINARY and cannot accept STRING");
        }
    }

    @Test
    public void testSetBooleanToByte() throws SqlException {
        bindVariableService.define(0, ColumnType.BOOLEAN, 0);
        try {
            bindVariableService.setByte(0, (byte) 10);
            Assert.fail();
        } catch (SqlException e) {
            TestUtils.assertContains(e.getFlyweightMessage(), "bind variable at 0 is defined as BOOLEAN and cannot accept BYTE");
        }
    }

    @Test
    public void testSetGeoByteToLessAccurateGeoByte() throws SqlException {
        bindVariableService.define(0, ColumnType.getGeoHashTypeWithBits(5), 0);
        try {
            bindVariableService.setGeoHash(0, 3, ColumnType.getGeoHashTypeWithBits(3));
            Assert.fail();
        } catch (SqlException e) {
            TestUtils.assertContains(e.getFlyweightMessage(), "inconvertible types: GEOHASH(3b) -> GEOHASH(1c) [varIndex=0]");
        }
    }

    @Test
    public void testSetBooleanToLong256() throws SqlException {
        bindVariableService.define(0, ColumnType.BOOLEAN, 0);
        try {
            bindVariableService.setLong256(0, 888, 777, 6666, 5555);
            Assert.fail();
        } catch (SqlException e) {
            TestUtils.assertContains(e.getFlyweightMessage(), "bind variable at 0 is defined as BOOLEAN and cannot accept LONG256");
        }
    }

    @Test
    public void testSetBooleanToStr() throws SqlException {
        bindVariableService.define(0, ColumnType.BOOLEAN, 0);
        bindVariableService.setStr(0, "true");
        Assert.assertTrue(bindVariableService.getFunction(0).getBool(null));
        bindVariableService.setStr(0, "false");
        Assert.assertFalse(bindVariableService.getFunction(0).getBool(null));
    }

    @Test
    public void testSetByteToByte() throws SqlException {
        bindVariableService.define(0, ColumnType.BYTE, 0);
        bindVariableService.setByte(0, (byte) 10);
        Assert.assertEquals(10, bindVariableService.getFunction(0).getByte(null));
        bindVariableService.setByte(0, (byte) 22);
        Assert.assertEquals(22, bindVariableService.getFunction(0).getByte(null));
    }

    @Test
    public void testSetByteToStr() throws SqlException {
        bindVariableService.define(0, ColumnType.BYTE, 0);
        bindVariableService.setStr(0, "21");
        Assert.assertEquals(21, bindVariableService.getFunction(0).getByte(null));
    }

    @Test
    public void testSetByteVarToShort() throws SqlException {
        bindVariableService.setByte(0, (byte) 99);
        Assert.assertEquals(99, bindVariableService.getFunction(0).getByte(null));
        bindVariableService.setShort(0, (short) 5);
        Assert.assertEquals(5, bindVariableService.getFunction(0).getByte(null));
    }

    @Test
    public void testSetCharToStr() throws SqlException {
        bindVariableService.define(0, ColumnType.CHAR, 0);
        bindVariableService.setStr(0, "65");
        Assert.assertEquals('A', bindVariableService.getFunction(0).getChar(null));
        bindVariableService.setStr(0, "");
        Assert.assertEquals(0, bindVariableService.getFunction(0).getChar(null));
        bindVariableService.setStr(0, null);
        Assert.assertEquals(0, bindVariableService.getFunction(0).getChar(null));
    }

    @Test
    public void testSetDateToByte() throws SqlException {
        bindVariableService.define(0, ColumnType.DATE, 0);
        bindVariableService.setByte(0, (byte) 10);
        Assert.assertEquals(10, bindVariableService.getFunction(0).getDate(null));
        bindVariableService.setByte(0, (byte) 22);
        Assert.assertEquals(22, bindVariableService.getFunction(0).getDate(null));
    }

    @Test
    public void testSetDateToStr() throws SqlException {
        bindVariableService.define(0, ColumnType.DATE, 0);
        bindVariableService.setStr(0, "21");
        Assert.assertEquals(21, bindVariableService.getFunction(0).getDate(null));
        bindVariableService.setStr(0, null);
        Assert.assertEquals(Numbers.LONG_NaN, bindVariableService.getFunction(0).getDate(null));
    }

    @Test
    public void testSetDateVarToShort() throws SqlException {
        bindVariableService.setDate(0, 10);
        Assert.assertEquals(10, bindVariableService.getFunction(0).getDate(null));
        bindVariableService.setShort(0, (short) 5);
        Assert.assertEquals(5, bindVariableService.getFunction(0).getDate(null));
    }

    @Test
    public void testSetDoubleToByte() throws SqlException {
        bindVariableService.define(0, ColumnType.DOUBLE, 0);
        bindVariableService.setByte(0, (byte) 10);
        Assert.assertEquals(10, bindVariableService.getFunction(0).getDouble(null), 0.000001);
        bindVariableService.setByte(0, (byte) 22);
        Assert.assertEquals(22, bindVariableService.getFunction(0).getDouble(null), 0.000001);
    }

    @Test
    public void testSetDoubleToStr() throws SqlException {
        bindVariableService.define(0, ColumnType.DOUBLE, 0);
        bindVariableService.setStr(0, "21.2");
        Assert.assertEquals(21.2, bindVariableService.getFunction(0).getDouble(null), 0.00001);
        bindVariableService.setStr(0, null);
        final double d = bindVariableService.getFunction(0).getDouble(null);
        Assert.assertTrue(d != d);
    }

    @Test
    public void testSetDoubleVarToShort() throws SqlException {
        bindVariableService.setDouble(0, 10.2);
        Assert.assertEquals(10.2, bindVariableService.getFunction(0).getDouble(null), 0.00001);
        bindVariableService.setShort(0, (short) 5);
        Assert.assertEquals(5, bindVariableService.getFunction(0).getDouble(null), 0.00001);
    }

    @Test
    public void testSetFloatToByte() throws SqlException {
        bindVariableService.define(0, ColumnType.FLOAT, 0);
        bindVariableService.setByte(0, (byte) 10);
        Assert.assertEquals(10, bindVariableService.getFunction(0).getFloat(null), 0.000001);
        bindVariableService.setByte(0, (byte) 22);
        Assert.assertEquals(22, bindVariableService.getFunction(0).getFloat(null), 0.000001);
    }

    @Test
    public void testSetFloatToStr() throws SqlException {
        bindVariableService.define(0, ColumnType.FLOAT, 0);
        bindVariableService.setStr(0, "21.2");
        Assert.assertEquals(21.2, bindVariableService.getFunction(0).getFloat(null), 0.00001);
        bindVariableService.setStr(0, null);
        final float f = bindVariableService.getFunction(0).getFloat(null);
        Assert.assertTrue(f != f);

        try {
            bindVariableService.setStr(0, "xyz");
            Assert.fail();
        } catch (SqlException e) {
            TestUtils.assertContains(e.getFlyweightMessage(), "could not parse [value='xyz', as=FLOAT, index=0]");
        }
    }

    @Test
    public void testSetFloatVarToShort() throws SqlException {
        bindVariableService.setFloat(0, 10.2f);
        Assert.assertEquals(10.2f, bindVariableService.getFunction(0).getFloat(null), 0.00001);
        bindVariableService.setShort(0, (short) 5);
        Assert.assertEquals(5f, bindVariableService.getFunction(0).getFloat(null), 0.00001);
    }

    @Test
    public void testSetIntToByte() throws SqlException {
        bindVariableService.setInt("a", 10);
        Assert.assertEquals(10, bindVariableService.getFunction(":a").getInt(null));
        bindVariableService.setByte("a", (byte) 5);
        Assert.assertEquals(5, bindVariableService.getFunction(":a").getInt(null));
    }

    @Test
    public void testSetIntToStr() throws SqlException {
        bindVariableService.define(0, ColumnType.INT, 0);
        bindVariableService.setStr(0, "21");
        Assert.assertEquals(21, bindVariableService.getFunction(0).getInt(null));
        bindVariableService.setStr(0, null);
        Assert.assertEquals(Numbers.INT_NaN, bindVariableService.getFunction(0).getInt(null));
    }

    @Test
    public void testSetIntVarToShort() throws SqlException {
        bindVariableService.setInt(0, 10);
        Assert.assertEquals(10, bindVariableService.getFunction(0).getInt(null));
        bindVariableService.setShort(0, (short) 5);
        Assert.assertEquals(5, bindVariableService.getFunction(0).getLong(null));
    }

    @Test
    public void testSetLong256ToLong256() throws SqlException {
        bindVariableService.define(0, ColumnType.LONG256, 0);
        bindVariableService.setLong256(0, 888, 777, 6666, 5555);
        StringSink sink = new StringSink();
        bindVariableService.getFunction(0).getLong256(null, sink);
        TestUtils.assertEquals("0x15b30000000000001a0a00000000000003090000000000000378", sink);
    }

    @Test
    public void testSetLongToByte() throws SqlException {
        bindVariableService.setLong("a", 10);
        Assert.assertEquals(10, bindVariableService.getFunction(":a").getLong(null));
        bindVariableService.setByte("a", (byte) 5);
        Assert.assertEquals(5, bindVariableService.getFunction(":a").getLong(null));
    }

    @Test
    public void testSetLongToStr() throws SqlException {
        bindVariableService.define(0, ColumnType.LONG, 0);
        bindVariableService.setStr(0, "21");
        Assert.assertEquals(21, bindVariableService.getFunction(0).getLong(null));
        bindVariableService.setStr(0, null);
        Assert.assertEquals(Numbers.LONG_NaN, bindVariableService.getFunction(0).getLong(null));
    }

    @Test
    public void testSetLongVarToShort() throws SqlException {
        bindVariableService.setLong(0, 10);
        Assert.assertEquals(10, bindVariableService.getFunction(0).getLong(null));
        bindVariableService.setShort(0, (short) 5);
        Assert.assertEquals(5, bindVariableService.getFunction(0).getLong(null));
    }

    @Test
    public void testSetShortToByte() throws SqlException {
        bindVariableService.define(0, ColumnType.SHORT, 0);
        bindVariableService.setByte(0, (byte) 10);
        Assert.assertEquals(10, bindVariableService.getFunction(0).getShort(null));
        bindVariableService.setByte(0, (byte) 22);
        Assert.assertEquals(22, bindVariableService.getFunction(0).getShort(null));
    }

    @Test
    public void testSetShortToStr() throws SqlException {
        bindVariableService.define(0, ColumnType.SHORT, 0);
        bindVariableService.setStr(0, "21");
        Assert.assertEquals(21, bindVariableService.getFunction(0).getShort(null));
    }

    @Test
    public void testSetShortVarToShort() throws SqlException {
        bindVariableService.setShort(0);
        bindVariableService.setShort(0, (short) 99);
        Assert.assertEquals(99, bindVariableService.getFunction(0).getShort(null));
        bindVariableService.setShort(0, (short) 71);
        Assert.assertEquals(71, bindVariableService.getFunction(0).getShort(null));
    }

    @Test
    public void testSetStrToBoolean() throws SqlException {
        bindVariableService.define(0, ColumnType.STRING, 0);
        bindVariableService.setBoolean(0, true);
        TestUtils.assertEquals("true", bindVariableService.getFunction(0).getStr(null));
        bindVariableService.setBoolean(0, false);
        TestUtils.assertEquals("false", bindVariableService.getFunction(0).getStr(null));
    }

    @Test
    public void testSetStrToByte() throws SqlException {
        bindVariableService.define(0, ColumnType.STRING, 0);
        bindVariableService.setByte(0, (byte) 22);
        TestUtils.assertEquals("22", bindVariableService.getFunction(0).getStr(null));
    }

    @Test
    public void testSetStrToLong256() throws SqlException {
        bindVariableService.define(0, ColumnType.STRING, 0);
        bindVariableService.setLong256(0, 888, 777, 6666, 5555);
        TestUtils.assertEquals("0x15b30000000000001a0a00000000000003090000000000000378", bindVariableService.getFunction(0).getStr(null));
        bindVariableService.setLong256(0);
        TestUtils.assertEquals("", bindVariableService.getFunction(0).getStr(null));

    }

    @Test
    public void testSetStrVarToShort() throws SqlException {
        bindVariableService.setStr(0, "ello");
        TestUtils.assertEquals("ello", bindVariableService.getFunction(0).getStr(null));
        bindVariableService.setShort(0, (short) 5);
        TestUtils.assertEquals("5", bindVariableService.getFunction(0).getStr(null));
    }

    @Test
    public void testSetTimestampToByte() throws SqlException {
        bindVariableService.define(0, ColumnType.TIMESTAMP, 0);
        bindVariableService.setByte(0, (byte) 10);
        Assert.assertEquals(10, bindVariableService.getFunction(0).getTimestamp(null));
        bindVariableService.setByte(0, (byte) 22);
        Assert.assertEquals(22, bindVariableService.getFunction(0).getTimestamp(null));
    }

    @Test
    public void testSetTimestampToStr() throws SqlException {
        bindVariableService.define(0, ColumnType.TIMESTAMP, 0);
        try {
            bindVariableService.setStr(0, "21");
            Assert.fail();
        } catch (SqlException e) {
            // Number string is not allowed to be set as timestamp
            // ISO formatted timestamp string is OK, but not number
        }
        bindVariableService.setStr(0, null);
        Assert.assertEquals(Numbers.LONG_NaN, bindVariableService.getFunction(0).getTimestamp(null));
        bindVariableService.setStr(0, "2019-10-31 15:05:22+08:00");
        Assert.assertEquals(1572505522000L, bindVariableService.getFunction(0).getTimestamp(null));
    }

    @Test
    public void testSetTimestampVarToShort() throws SqlException {
        bindVariableService.setTimestamp(0, 10);
        Assert.assertEquals(10, bindVariableService.getFunction(0).getTimestamp(null));
        bindVariableService.setShort(0, (short) 5);
        Assert.assertEquals(5, bindVariableService.getFunction(0).getTimestamp(null));
    }

    @Test
    public void testStrIndexedOverride() throws SqlException {
        bindVariableService.setLong(0, 10);
        try {
            bindVariableService.setStr(0, "ok");
            Assert.fail();
        } catch (SqlException e) {
            TestUtils.assertContains(e.getFlyweightMessage(), "could not parse [value='ok', as=LONG, index=0]");
        }
    }

    @Test
    public void testStrOverride() throws SqlException {
        bindVariableService.setLong("a", 10);
        try {
            bindVariableService.setStr("a", "ok");
            Assert.fail();
        } catch (SqlException e) {
            TestUtils.assertContains(e.getFlyweightMessage(), "could not parse [value='ok', as=LONG, index=-1]");
        }
    }

    @Test
    public void testStrVarSetToChar() throws SqlException {
        bindVariableService.setStr(0);
        bindVariableService.setStr(0, "perfecto");
        TestUtils.assertEquals("perfecto", bindVariableService.getFunction(0).getStr(null));

        bindVariableService.setChar(0, 'R');
        TestUtils.assertEquals("R", bindVariableService.getFunction(0).getStr(null));
    }

    @Test
    public void testStrVarSetToFloat() throws SqlException {
        bindVariableService.setStr(0);
        bindVariableService.setStr(0, "1000.88");
        TestUtils.assertEquals("1000.88", bindVariableService.getFunction(0).getStr(null));

        bindVariableService.setFloat(0, 451f);
        TestUtils.assertEquals("451.0000", bindVariableService.getFunction(0).getStr(null));

        bindVariableService.setFloat(0, Float.NaN);
        Assert.assertNull(bindVariableService.getFunction(0).getStr(null));
    }

    @Test
    public void testStrVarSetToLong() throws SqlException {
        bindVariableService.setStr(0);
        bindVariableService.setStr(0, "perfecto");
        TestUtils.assertEquals("perfecto", bindVariableService.getFunction(0).getStr(null));

        bindVariableService.setLong(0, 450);
        TestUtils.assertEquals("450", bindVariableService.getFunction(0).getStr(null));

        bindVariableService.setLong(0, Numbers.LONG_NaN);
        Assert.assertNull(bindVariableService.getFunction(0).getStr(null));
    }

    @Test
    public void testStringVarSetToDouble() throws SqlException {
        bindVariableService.setStr(0);
        bindVariableService.setStr(0, "test1");
        TestUtils.assertEquals("test1", bindVariableService.getFunction(0).getStr(null));
        TestUtils.assertEquals("test1", bindVariableService.getFunction(0).getStrB(null));
        Assert.assertEquals(5, bindVariableService.getFunction(0).getStrLen(null));
        bindVariableService.setDouble(0, 123.456d);
        TestUtils.assertEquals("123.456", bindVariableService.getFunction(0).getStr(null));
        bindVariableService.setDouble(0, Double.NaN);
        Assert.assertNull(bindVariableService.getFunction(0).getStr(null));
    }

    @Test
    public void testStringVarSetToInt() throws SqlException {
        bindVariableService.setStr(0);
        bindVariableService.setStr(0, "test1");
        TestUtils.assertEquals("test1", bindVariableService.getFunction(0).getStr(null));

        bindVariableService.setInt(0, 450);
        TestUtils.assertEquals("450", bindVariableService.getFunction(0).getStr(null));

        bindVariableService.setInt(0, Numbers.INT_NaN);
        Assert.assertNull(bindVariableService.getFunction(0).getStr(null));
    }

    @Test
    public void testTimestampOverride() throws SqlException {
        bindVariableService.setLong("a", 10);
        Assert.assertEquals(10, bindVariableService.getFunction(":a").getLong(null));
        bindVariableService.setTimestamp("a", 5);
        Assert.assertEquals(5, bindVariableService.getFunction(":a").getLong(null));
    }

    @Test
    public void testTimestampVarSetToInt() throws SqlException {
        bindVariableService.setTimestamp(0);
        bindVariableService.setTimestamp(0, 99999001);
        Assert.assertEquals(99999001, bindVariableService.getFunction(0).getTimestamp(null));

        bindVariableService.setInt(0, 450);
        Assert.assertEquals(450, bindVariableService.getFunction(0).getTimestamp(null));

        bindVariableService.setInt(0, Numbers.INT_NaN);
        final long d = bindVariableService.getFunction(0).getTimestamp(null);
        Assert.assertEquals(Numbers.LONG_NaN, d);
    }
}