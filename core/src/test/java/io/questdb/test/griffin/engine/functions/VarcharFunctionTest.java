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
import io.questdb.griffin.engine.functions.VarcharFunction;
import io.questdb.griffin.engine.functions.constants.VarcharConstant;
import io.questdb.std.Numbers;
import io.questdb.std.NumericException;
import io.questdb.std.str.Utf8Sequence;
import io.questdb.test.tools.TestUtils;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Test;

public class VarcharFunctionTest {
    // assert that all type casts that are not possible will throw exception

    private static final Utf8Sequence utf8Seq = new Utf8Sequence() {
        @Override
        public @NotNull CharSequence asAsciiCharSequence() {
            return "a";
        }

        @Override
        public byte byteAt(int index) {
            return (byte) (index == 0 ? 'a' : -1);
        }

        @Override
        public int size() {
            return 1;
        }
    };

    private static final VarcharFunction function = new VarcharFunction() {
        @Override
        public Utf8Sequence getVarcharA(Record rec) {
            return utf8Seq;
        }

        @Override
        public Utf8Sequence getVarcharB(Record rec) {
            return utf8Seq;
        }
    };

    @Test
    public void testCastStrToChar() {
        Assert.assertEquals('A', new VarcharConstant("A").getChar(null));
    }

    @Test
    public void testCastToByte() {
        Assert.assertEquals(10, new VarcharConstant("10").getByte(null));
    }

    @Test
    public void testCastToByteEmpty() {
        try {
            new VarcharConstant("").getByte(null);
            Assert.fail();
        } catch (ImplicitCastException e) {
            TestUtils.assertContains(e.getFlyweightMessage(), "inconvertible value: `` [VARCHAR -> BYTE]");
        }
    }

    @Test
    public void testCastToByteNeg() {
        Assert.assertEquals(-10, new VarcharConstant("-10").getByte(null));
    }

    @Test
    public void testCastToByteNull() {
        Assert.assertEquals(0, new VarcharConstant((Utf8Sequence) null).getByte(null));
    }

    @Test
    public void testCastToByteTooWide1() {
        try {
            new VarcharConstant("129").getByte(null);
            Assert.fail();
        } catch (ImplicitCastException e) {
            TestUtils.assertContains(e.getFlyweightMessage(), "inconvertible value: `129` [VARCHAR -> BYTE]");
        }
    }

    @Test
    public void testCastToByteTooWide2() {
        try {
            new VarcharConstant("-129").getByte(null);
            Assert.fail();
        } catch (ImplicitCastException e) {
            TestUtils.assertContains(e.getFlyweightMessage(), "inconvertible value: `-129` [VARCHAR -> BYTE]");
        }
    }

    @Test
    public void testCastToDate() {
        Assert.assertEquals(1631268753887L, new VarcharConstant("2021-09-10T10:12:33.887Z").getDate(null));
    }

    @Test
    public void testCastToDouble() {
        Assert.assertEquals(1345.998, new VarcharConstant("1345.998").getDouble(null), 0.001);
    }

    @Test
    public void testCastToDoubleEmpty() {
        try {
            new VarcharConstant("").getDouble(null);
            Assert.fail();
        } catch (ImplicitCastException e) {
            TestUtils.assertContains(e.getFlyweightMessage(), "inconvertible value: `` [VARCHAR -> DOUBLE]");
        }
    }

    @Test
    public void testCastToDoubleNeg() {
        Assert.assertEquals(-1990.997, new VarcharConstant("-1990.997").getDouble(null), 0.001);
    }

    @Test
    public void testCastToDoubleNegZero() {
        Assert.assertEquals(-0.0, new VarcharConstant("-0.000").getDouble(null), 0.001);
    }

    @Test
    public void testCastToDoubleNull() {
        Assert.assertEquals(Float.NaN, new VarcharConstant((Utf8Sequence) null).getDouble(null), 0.001);
    }

    @Test
    public void testCastToDoublePosInf() {
        Assert.assertEquals(Float.POSITIVE_INFINITY, new VarcharConstant("Infinity").getDouble(null), 0.001);
    }

    @Test
    public void testCastToDoubleTooWide2() {
        try {
            new VarcharConstant("-9E-410").getDouble(null);
            Assert.fail();
        } catch (ImplicitCastException e) {
            TestUtils.assertContains(e.getFlyweightMessage(), "inconvertible value: `-9E-410` [VARCHAR -> DOUBLE]");
        }
    }

    @Test
    public void testCastToDoubleZero() {
        Assert.assertEquals(0, new VarcharConstant("0.0000").getDouble(null), 0.001);
    }

    @Test
    public void testCastToDoubletNegInf() {
        Assert.assertEquals(Float.NEGATIVE_INFINITY, new VarcharConstant("-Infinity").getDouble(null), 0.001);
    }

    @Test
    public void testCastToDoubletTooWide1() {
        try {
            new VarcharConstant("1E350").getDouble(null);
            Assert.fail();
        } catch (ImplicitCastException e) {
            TestUtils.assertContains(e.getFlyweightMessage(), "inconvertible value: `1E350` [VARCHAR -> DOUBLE]");
        }
    }

    @Test
    public void testCastToFloat() {
        Assert.assertEquals(1345.998f, new VarcharConstant("1345.998").getFloat(null), 0.001);
    }

    @Test
    public void testCastToFloatEmpty() {
        try {
            new VarcharConstant("").getFloat(null);
            Assert.fail();
        } catch (ImplicitCastException e) {
            TestUtils.assertContains(e.getFlyweightMessage(), "inconvertible value: `` [VARCHAR -> FLOAT]");
        }
    }

    @Test
    public void testCastToFloatNeg() {
        Assert.assertEquals(-1990.997f, new VarcharConstant("-1990.997").getFloat(null), 0.001);
    }

    @Test
    public void testCastToFloatNegInf() {
        Assert.assertEquals(Float.NEGATIVE_INFINITY, new VarcharConstant("-Infinity").getFloat(null), 0.001);
    }

    @Test
    public void testCastToFloatNegZero() {
        Assert.assertEquals(-0.0f, new VarcharConstant("-0.000f").getFloat(null), 0.001);
    }

    @Test
    public void testCastToFloatNull() {
        Assert.assertEquals(Float.NaN, new VarcharConstant((Utf8Sequence) null).getFloat(null), 0.001);
    }

    @Test
    public void testCastToFloatPosInf() {
        Assert.assertEquals(Float.POSITIVE_INFINITY, new VarcharConstant("Infinity").getFloat(null), 0.001);
    }

    @Test
    public void testCastToFloatTooWide1() {
        try {
            new VarcharConstant("1E250").getFloat(null);
            Assert.fail();
        } catch (ImplicitCastException e) {
            TestUtils.assertContains(e.getFlyweightMessage(), "inconvertible value: `1E250` [VARCHAR -> FLOAT]");
        }
    }

    @Test
    public void testCastToFloatTooWide2() {
        try {
            new VarcharConstant("-9E-210").getFloat(null);
            Assert.fail();
        } catch (ImplicitCastException e) {
            TestUtils.assertContains(e.getFlyweightMessage(), "inconvertible value: `-9E-210` [VARCHAR -> FLOAT]");
        }
    }

    @Test
    public void testCastToFloatZero() {
        Assert.assertEquals(0, new VarcharConstant("0.0000f").getFloat(null), 0.001);
    }

    @Test
    public void testCastToIPv4() {
        Assert.assertEquals("23.200.41.90", TestUtils.ipv4ToString(new VarcharConstant("23.200.41.90").getIPv4(null)));
    }

    @Test
    public void testCastToInt() {
        Assert.assertEquals(1345, new VarcharConstant("1345").getInt(null));
    }

    @Test
    public void testCastToIntEmpty() {
        try {
            new VarcharConstant("").getInt(null);
            Assert.fail();
        } catch (ImplicitCastException e) {
            TestUtils.assertContains(e.getFlyweightMessage(), "inconvertible value: `` [VARCHAR -> INT]");
        }
    }

    @Test
    public void testCastToIntNeg() {
        Assert.assertEquals(-1990, new VarcharConstant("-1990").getInt(null));
    }

    @Test
    public void testCastToIntNull() {
        Assert.assertEquals(Numbers.INT_NULL, new VarcharConstant((Utf8Sequence) null).getInt(null));
    }

    @Test
    public void testCastToIntTooWide1() {
        try {
            new VarcharConstant("2147483648").getInt(null);
            Assert.fail();
        } catch (ImplicitCastException e) {
            TestUtils.assertContains(e.getFlyweightMessage(), "inconvertible value: `2147483648` [VARCHAR -> INT]");
        }
    }

    @Test
    public void testCastToIntTooWide2() {
        try {
            new VarcharConstant("-2147483649").getInt(null);
            Assert.fail();
        } catch (ImplicitCastException e) {
            TestUtils.assertContains(e.getFlyweightMessage(), "inconvertible value: `-2147483649` [VARCHAR -> INT]");
        }
    }

    @Test
    public void testCastToLong() {
        Assert.assertEquals(1345, new VarcharConstant("1345").getLong(null));
    }

    @Test
    public void testCastToLongEmpty() {
        try {
            new VarcharConstant("").getLong(null);
            Assert.fail();
        } catch (ImplicitCastException e) {
            TestUtils.assertContains(e.getFlyweightMessage(), "inconvertible value: `` [VARCHAR -> LONG]");
        }
    }

    @Test
    public void testCastToLongNeg() {
        Assert.assertEquals(-1990, new VarcharConstant("-1990").getLong(null));
    }

    @Test
    public void testCastToLongNull() {
        Assert.assertEquals(Numbers.LONG_NULL, new VarcharConstant((Utf8Sequence) null).getLong(null));
    }

    @Test
    public void testCastToLongTooWide1() {
        try {
            new VarcharConstant("9223372036854775808").getLong(null);
            Assert.fail();
        } catch (ImplicitCastException e) {
            TestUtils.assertContains(e.getFlyweightMessage(), "inconvertible value: `9223372036854775808` [VARCHAR -> LONG]");
        }
    }

    @Test
    public void testCastToLongTooWide2() {
        try {
            new VarcharConstant("-9223372036854775809").getLong(null);
            Assert.fail();
        } catch (ImplicitCastException e) {
            TestUtils.assertContains(e.getFlyweightMessage(), "inconvertible value: `-9223372036854775809` [VARCHAR -> LONG]");
        }
    }

    @Test
    public void testCastToShort() {
        Assert.assertEquals(1345, new VarcharConstant("1345").getShort(null));
    }

    @Test
    public void testCastToShortEmpty() {
        try {
            new VarcharConstant("").getShort(null);
            Assert.fail();
        } catch (ImplicitCastException e) {
            TestUtils.assertContains(e.getFlyweightMessage(), "inconvertible value: `` [VARCHAR -> SHORT]");
        }
    }

    @Test
    public void testCastToShortNeg() {
        Assert.assertEquals(-1990, new VarcharConstant("-1990").getShort(null));
    }

    @Test
    public void testCastToShortNull() {
        Assert.assertEquals(0, new VarcharConstant((Utf8Sequence) null).getShort(null));
    }

    @Test
    public void testCastToShortTooWide1() {
        try {
            new VarcharConstant("32768").getShort(null);
            Assert.fail();
        } catch (ImplicitCastException e) {
            TestUtils.assertContains(e.getFlyweightMessage(), "inconvertible value: `32768` [VARCHAR -> SHORT]");
        }
    }

    @Test
    public void testCastToShortTooWide2() {
        try {
            new VarcharConstant("-32769").getShort(null);
            Assert.fail();
        } catch (ImplicitCastException e) {
            TestUtils.assertContains(e.getFlyweightMessage(), "inconvertible value: `-32769` [VARCHAR -> SHORT]");
        }
    }

    @Test
    public void testCastToTimestamp() throws NumericException {
        Assert.assertEquals(NanosTimestampDriver.INSTANCE.parseFloorLiteral("2021-09-10T10:12:33.887889Z"), new VarcharConstant("2021-09-10T10:12:33.887889").getTimestamp(null));
    }

    @Test
    public void testCastToTimestampEmpty() {
        try {
            new VarcharConstant("").getTimestamp(null);
            Assert.fail();
        } catch (ImplicitCastException e) {
            TestUtils.assertContains(e.getFlyweightMessage(), "inconvertible value: `` [VARCHAR -> TIMESTAMP_NS]");
        }
    }

    @Test
    public void testCastToTimestampNull() {
        Assert.assertEquals(Numbers.LONG_NULL, new VarcharConstant((Utf8Sequence) null).getTimestamp(null));
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGeoByte() {
        function.getGeoByte(null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGeoInt() {
        function.getGeoInt(null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGeoLong() {
        function.getGeoLong(null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGeoShort() {
        function.getGeoShort(null);
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
}
