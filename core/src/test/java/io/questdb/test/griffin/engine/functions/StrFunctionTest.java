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

package io.questdb.test.griffin.engine.functions;

import io.questdb.cairo.ImplicitCastException;
import io.questdb.cairo.NanosTimestampDriver;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.engine.functions.StrFunction;
import io.questdb.griffin.engine.functions.constants.StrConstant;
import io.questdb.std.Numbers;
import io.questdb.std.NumericException;
import io.questdb.std.datetime.millitime.DateFormatUtils;
import io.questdb.std.str.Utf8Sequence;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class StrFunctionTest {
    // assert that all type casts that are not possible will throw exception

    private static final StrFunction function = new StrFunction() {
        @Override
        public CharSequence getStrA(Record rec) {
            return "a";
        }

        @Override
        public CharSequence getStrB(Record rec) {
            return "a";
        }
    };

    @Test
    public void testCastStrToChar() {
        Assert.assertEquals('A', new StrConstant("A").getChar(null));
    }

    @Test
    public void testCastToByte() {
        Assert.assertEquals(10, new StrConstant("10").getByte(null));
    }

    @Test
    public void testCastToByteEmpty() {
        try {
            new StrConstant("").getByte(null);
            Assert.fail();
        } catch (ImplicitCastException e) {
            TestUtils.assertContains(e.getFlyweightMessage(), "inconvertible value: `` [STRING -> BYTE]");
        }
    }

    @Test
    public void testCastToByteNeg() {
        Assert.assertEquals(-10, new StrConstant("-10").getByte(null));
    }

    @Test
    public void testCastToByteNull() {
        Assert.assertEquals(0, new StrConstant(null).getByte(null));
    }

    @Test
    public void testCastToByteTooWide1() {
        try {
            new StrConstant("129").getByte(null);
            Assert.fail();
        } catch (ImplicitCastException e) {
            TestUtils.assertContains(e.getFlyweightMessage(), "inconvertible value: `129` [STRING -> BYTE]");
        }
    }

    @Test
    public void testCastToByteTooWide2() {
        try {
            new StrConstant("-129").getByte(null);
            Assert.fail();
        } catch (ImplicitCastException e) {
            TestUtils.assertContains(e.getFlyweightMessage(), "inconvertible value: `-129` [STRING -> BYTE]");
        }
    }

    @Test
    public void testCastToDate() throws NumericException {
        Assert.assertEquals(DateFormatUtils.parseUTCDate("2021-09-10T10:12:33.887Z"), new StrConstant("2021-09-10T10:12:33.887Z").getDate(null));
    }

    @Test
    public void testCastToDateEmpty() {
        try {
            new StrConstant("").getDate(null);
            Assert.fail();
        } catch (ImplicitCastException e) {
            TestUtils.assertContains(e.getFlyweightMessage(), "inconvertible value: `` [STRING -> DATE]");
        }
    }

    @Test
    public void testCastToDateNull() {
        Assert.assertEquals(Numbers.LONG_NULL, new StrConstant(null).getDate(null));
    }

    @Test
    public void testCastToDouble() {
        Assert.assertEquals(1345.998, new StrConstant("1345.998").getDouble(null), 0.001);
    }

    @Test
    public void testCastToDoubleEmpty() {
        try {
            new StrConstant("").getDouble(null);
            Assert.fail();
        } catch (ImplicitCastException e) {
            TestUtils.assertContains(e.getFlyweightMessage(), "inconvertible value: `` [STRING -> DOUBLE]");
        }
    }

    @Test
    public void testCastToDoubleNeg() {
        Assert.assertEquals(-1990.997, new StrConstant("-1990.997").getDouble(null), 0.001);
    }

    @Test
    public void testCastToDoubleNegZero() {
        Assert.assertEquals(-0.0, new StrConstant("-0.000").getDouble(null), 0.001);
    }

    @Test
    public void testCastToDoubleNull() {
        Assert.assertEquals(Float.NaN, new StrConstant(null).getDouble(null), 0.001);
    }

    @Test
    public void testCastToDoublePosInf() {
        Assert.assertEquals(Float.POSITIVE_INFINITY, new StrConstant("Infinity").getDouble(null), 0.001);
    }

    @Test
    public void testCastToDoubleTooWide2() {
        try {
            new StrConstant("-9E-410").getDouble(null);
            Assert.fail();
        } catch (ImplicitCastException e) {
            TestUtils.assertContains(e.getFlyweightMessage(), "inconvertible value: `-9E-410` [STRING -> DOUBLE]");
        }
    }

    @Test
    public void testCastToDoubleZero() {
        Assert.assertEquals(0, new StrConstant("0.0000").getDouble(null), 0.001);
    }

    @Test
    public void testCastToDoubletNegInf() {
        Assert.assertEquals(Float.NEGATIVE_INFINITY, new StrConstant("-Infinity").getDouble(null), 0.001);
    }

    @Test
    public void testCastToDoubletTooWide1() {
        try {
            new StrConstant("1E350").getDouble(null);
            Assert.fail();
        } catch (ImplicitCastException e) {
            TestUtils.assertContains(e.getFlyweightMessage(), "inconvertible value: `1E350` [STRING -> DOUBLE]");
        }
    }

    @Test
    public void testCastToFloat() {
        Assert.assertEquals(1345.998f, new StrConstant("1345.998").getFloat(null), 0.001);
    }

    @Test
    public void testCastToFloatEmpty() {
        try {
            new StrConstant("").getFloat(null);
            Assert.fail();
        } catch (ImplicitCastException e) {
            TestUtils.assertContains(e.getFlyweightMessage(), "inconvertible value: `` [STRING -> FLOAT]");
        }
    }

    @Test
    public void testCastToFloatNeg() {
        Assert.assertEquals(-1990.997f, new StrConstant("-1990.997").getFloat(null), 0.001);
    }

    @Test
    public void testCastToFloatNegInf() {
        Assert.assertEquals(Float.NEGATIVE_INFINITY, new StrConstant("-Infinity").getFloat(null), 0.001);
    }

    @Test
    public void testCastToFloatNegZero() {
        Assert.assertEquals(-0.0f, new StrConstant("-0.000f").getFloat(null), 0.001);
    }

    @Test
    public void testCastToFloatNull() {
        Assert.assertEquals(Float.NaN, new StrConstant(null).getFloat(null), 0.001);
    }

    @Test
    public void testCastToFloatPosInf() {
        Assert.assertEquals(Float.POSITIVE_INFINITY, new StrConstant("Infinity").getFloat(null), 0.001);
    }

    @Test
    public void testCastToFloatTooWide1() {
        try {
            new StrConstant("1E250").getFloat(null);
            Assert.fail();
        } catch (ImplicitCastException e) {
            TestUtils.assertContains(e.getFlyweightMessage(), "inconvertible value: `1E250` [STRING -> FLOAT]");
        }
    }

    @Test
    public void testCastToFloatTooWide2() {
        try {
            new StrConstant("-9E-210").getFloat(null);
            Assert.fail();
        } catch (ImplicitCastException e) {
            TestUtils.assertContains(e.getFlyweightMessage(), "inconvertible value: `-9E-210` [STRING -> FLOAT]");
        }
    }

    @Test
    public void testCastToFloatZero() {
        Assert.assertEquals(0, new StrConstant("0.0000f").getFloat(null), 0.001);
    }

    @Test
    public void testCastToIPv4() {
        Assert.assertEquals("23.200.41.90", TestUtils.ipv4ToString(new StrConstant("23.200.41.90").getIPv4(null)));
    }

    @Test
    public void testCastToInt() {
        Assert.assertEquals(1345, new StrConstant("1345").getInt(null));
    }

    @Test
    public void testCastToIntEmpty() {
        try {
            new StrConstant("").getInt(null);
            Assert.fail();
        } catch (ImplicitCastException e) {
            TestUtils.assertContains(e.getFlyweightMessage(), "inconvertible value: `` [STRING -> INT]");
        }
    }

    @Test
    public void testCastToIntNeg() {
        Assert.assertEquals(-1990, new StrConstant("-1990").getInt(null));
    }

    @Test
    public void testCastToIntNull() {
        Assert.assertEquals(Numbers.INT_NULL, new StrConstant(null).getInt(null));
    }

    @Test
    public void testCastToIntTooWide1() {
        try {
            new StrConstant("2147483648").getInt(null);
            Assert.fail();
        } catch (ImplicitCastException e) {
            TestUtils.assertContains(e.getFlyweightMessage(), "inconvertible value: `2147483648` [STRING -> INT]");
        }
    }

    @Test
    public void testCastToIntTooWide2() {
        try {
            new StrConstant("-2147483649").getInt(null);
            Assert.fail();
        } catch (ImplicitCastException e) {
            TestUtils.assertContains(e.getFlyweightMessage(), "inconvertible value: `-2147483649` [STRING -> INT]");
        }
    }

    @Test
    public void testCastToLong() {
        Assert.assertEquals(1345, new StrConstant("1345").getLong(null));
    }

    @Test
    public void testCastToLongEmpty() {
        try {
            new StrConstant("").getLong(null);
            Assert.fail();
        } catch (ImplicitCastException e) {
            TestUtils.assertContains(e.getFlyweightMessage(), "inconvertible value: `` [STRING -> LONG]");
        }
    }

    @Test
    public void testCastToLongNeg() {
        Assert.assertEquals(-1990, new StrConstant("-1990").getLong(null));
    }

    @Test
    public void testCastToLongNull() {
        Assert.assertEquals(Numbers.LONG_NULL, new StrConstant(null).getLong(null));
    }

    @Test
    public void testCastToLongTooWide1() {
        try {
            new StrConstant("9223372036854775808").getLong(null);
            Assert.fail();
        } catch (ImplicitCastException e) {
            TestUtils.assertContains(e.getFlyweightMessage(), "inconvertible value: `9223372036854775808` [STRING -> LONG]");
        }
    }

    @Test
    public void testCastToLongTooWide2() {
        try {
            new StrConstant("-9223372036854775809").getLong(null);
            Assert.fail();
        } catch (ImplicitCastException e) {
            TestUtils.assertContains(e.getFlyweightMessage(), "inconvertible value: `-9223372036854775809` [STRING -> LONG]");
        }
    }

    @Test
    public void testCastToShort() {
        Assert.assertEquals(1345, new StrConstant("1345").getShort(null));
    }

    @Test
    public void testCastToShortEmpty() {
        try {
            new StrConstant("").getShort(null);
            Assert.fail();
        } catch (ImplicitCastException e) {
            TestUtils.assertContains(e.getFlyweightMessage(), "inconvertible value: `` [STRING -> SHORT]");
        }
    }

    @Test
    public void testCastToShortNeg() {
        Assert.assertEquals(-1990, new StrConstant("-1990").getShort(null));
    }

    @Test
    public void testCastToShortNull() {
        Assert.assertEquals(0, new StrConstant(null).getShort(null));
    }

    @Test
    public void testCastToShortTooWide1() {
        try {
            new StrConstant("32768").getShort(null);
            Assert.fail();
        } catch (ImplicitCastException e) {
            TestUtils.assertContains(e.getFlyweightMessage(), "inconvertible value: `32768` [STRING -> SHORT]");
        }
    }

    @Test
    public void testCastToShortTooWide2() {
        try {
            new StrConstant("-32769").getShort(null);
            Assert.fail();
        } catch (ImplicitCastException e) {
            TestUtils.assertContains(e.getFlyweightMessage(), "inconvertible value: `-32769` [STRING -> SHORT]");
        }
    }

    @Test
    public void testCastToTimestamp() throws NumericException {
        Assert.assertEquals(NanosTimestampDriver.INSTANCE.parseFloorLiteral("2021-09-10T10:12:33.887889Z"), new StrConstant("2021-09-10T10:12:33.887889").getTimestamp(null));
    }

    @Test
    public void testCastToTimestampEmpty() {
        try {
            new StrConstant("").getTimestamp(null);
            Assert.fail();
        } catch (ImplicitCastException e) {
            TestUtils.assertContains(e.getFlyweightMessage(), "inconvertible value: `` [STRING -> TIMESTAMP_NS]");
        }
    }

    @Test
    public void testCastToTimestampNull() {
        Assert.assertEquals(Numbers.LONG_NULL, new StrConstant(null).getTimestamp(null));
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetArray() {
        function.getArray(null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetBin() {
        function.getBin(null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetBinLen() {
        function.getBinLen(null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetBool() {
        function.getBool(null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetDecimal128() {
        function.getDecimal128(null, null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetDecimal16() {
        function.getDecimal16(null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetDecimal256() {
        function.getDecimal256(null, null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetDecimal32() {
        function.getDecimal32(null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetDecimal64() {
        function.getDecimal64(null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetDecimal8() {
        function.getDecimal8(null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetGeoByte() {
        function.getGeoByte(null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetGeoInt() {
        function.getGeoInt(null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetGeoLong() {
        function.getGeoLong(null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetGeoShort() {
        function.getGeoShort(null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetLong128Hi() {
        function.getLong128Hi(null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetLong128Lo() {
        function.getLong128Lo(null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetLong256() {
        function.getLong256(null, null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetLong256A() {
        function.getLong256A(null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetLong256B() {
        function.getLong256B(null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetRecordCursorFactory() {
        function.getRecordCursorFactory();
    }

    @Test
    public void testGetStrLen() {
        Assert.assertEquals(1, function.getStrLen(null));
    }

    @Test
    public void testGetSymbol() {
        TestUtils.assertEquals("a", function.getSymbol(null));
    }

    @Test
    public void testGetSymbolB() {
        TestUtils.assertEquals("a", function.getSymbolB(null));
    }

    @Test
    public void testGetVarcharA() {
        Utf8Sequence value = function.getVarcharA(null);
        Assert.assertNotNull(value);
        TestUtils.assertEquals("a", value.toString());
    }

    @Test
    public void testGetVarcharB() {
        Utf8Sequence value = function.getVarcharB(null);
        Assert.assertNotNull(value);
        TestUtils.assertEquals("a", value.toString());
    }
}
