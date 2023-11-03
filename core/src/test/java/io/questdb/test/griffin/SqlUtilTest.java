/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 * <p>
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
 * <p>
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 * <p>
 *  http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 ******************************************************************************/

package io.questdb.test.griffin;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.GeoHashes;
import io.questdb.cairo.ImplicitCastException;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlUtil;
import io.questdb.griffin.engine.functions.Long256Function;
import io.questdb.griffin.engine.functions.constants.Constants;
import io.questdb.griffin.engine.functions.constants.Long256Constant;
import io.questdb.mp.SOCountDownLatch;
import io.questdb.std.Numbers;
import io.questdb.std.Rnd;
import io.questdb.std.datetime.microtime.TimestampFormatUtils;
import io.questdb.std.datetime.microtime.Timestamps;
import io.questdb.std.datetime.millitime.Dates;
import io.questdb.std.str.StringSink;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

public class SqlUtilTest {

    @Test
    public void testImplicitCastCharAsGeoHash() {
        int bits = 5;
        long hash = SqlUtil.implicitCastCharAsGeoHash('c', ColumnType.getGeoHashTypeWithBits(bits));
        StringSink sink = new StringSink();
        GeoHashes.appendChars(hash, bits / 5, sink);
        TestUtils.assertEquals("c", sink);
    }

    @Test
    public void testImplicitCastCharAsGeoHashInvalidChar() {
        testImplicitCastCharAsGeoHashInvalidChar0('o');
        testImplicitCastCharAsGeoHashInvalidChar0('O');
        testImplicitCastCharAsGeoHashInvalidChar0('l');
        testImplicitCastCharAsGeoHashInvalidChar0('L');
        testImplicitCastCharAsGeoHashInvalidChar0('i');
        testImplicitCastCharAsGeoHashInvalidChar0('I');
        testImplicitCastCharAsGeoHashInvalidChar0('-');
    }

    @Test
    public void testImplicitCastCharAsGeoHashNarrowing() {
        int bits = 6;
        try {
            SqlUtil.implicitCastCharAsGeoHash('c', ColumnType.getGeoHashTypeWithBits(bits));
            Assert.fail();
        } catch (ImplicitCastException e) {
            TestUtils.assertContains("inconvertible value: c [CHAR -> GEOHASH(6b)]", e.getFlyweightMessage());
        }
    }

    @Test
    public void testImplicitCastCharAsGeoHashWidening() {
        int bits = 4;
        long hash = SqlUtil.implicitCastCharAsGeoHash('c', ColumnType.getGeoHashTypeWithBits(bits));
        StringSink sink = new StringSink();
        GeoHashes.appendBinary(hash, bits, sink);
        TestUtils.assertEquals("0101", sink);
    }

    @Test
    public void testImplicitCastStrAsLong256() {
        Assert.assertEquals(Constants.getNullConstant(ColumnType.LONG256), SqlUtil.implicitCastStrAsLong256(null));
        Assert.assertEquals(Constants.getNullConstant(ColumnType.LONG256), SqlUtil.implicitCastStrAsLong256(""));
        int n = 5;
        SOCountDownLatch completed = new SOCountDownLatch(n);
        for (int t = 0; t < n; t++) {
            new Thread(() -> {
                Rnd rnd = new Rnd();
                StringSink sink0 = new StringSink();
                StringSink sink1 = new StringSink();
                for (int i = 0; i < 1000; i++) {
                    sink0.clear();
                    sink1.clear();
                    Long256Constant expected = new Long256Constant(rnd.nextLong(), rnd.nextLong(), rnd.nextLong(), rnd.nextLong());
                    expected.getLong256(null, sink0);
                    Long256Function function = SqlUtil.implicitCastStrAsLong256(sink0);
                    function.getLong256(null, sink1);
                    Assert.assertEquals(sink0.toString(), sink1.toString());
                    Assert.assertEquals(expected.getLong256A(null), function.getLong256A(null));
                    Assert.assertEquals(expected.getLong256B(null), function.getLong256B(null));
                }
                completed.countDown();
            }).start();
        }
        Assert.assertTrue(completed.await(TimeUnit.SECONDS.toNanos(2L)));
    }

    @Test
    public void testNaNCast() {
        Assert.assertEquals(0, SqlUtil.implicitCastIntAsByte(Numbers.INT_NaN));
        Assert.assertEquals(0, SqlUtil.implicitCastIntAsShort(Numbers.INT_NaN));
        Assert.assertEquals(0, SqlUtil.implicitCastLongAsByte(Numbers.LONG_NaN));
        Assert.assertEquals(Numbers.INT_NaN, SqlUtil.implicitCastLongAsInt(Numbers.LONG_NaN));
        Assert.assertEquals(0, SqlUtil.implicitCastLongAsShort(Numbers.LONG_NaN));
    }

    @Test
    public void testParseMicrosSansQualifier() {
        try {
            SqlUtil.expectMicros("125", 12);
            Assert.fail();
        } catch (SqlException e) {
            TestUtils.assertContains(e.getFlyweightMessage(), "expected interval qualifier");
        }
    }

    @Test
    public void testParseStrByte() {
        Assert.assertEquals(0, SqlUtil.implicitCastStrAsByte(null));
        Assert.assertEquals(89, SqlUtil.implicitCastStrAsByte("89"));
        Assert.assertEquals(-89, SqlUtil.implicitCastStrAsByte("-89"));

        // overflow
        try {
            SqlUtil.implicitCastStrAsByte("778");
            Assert.fail();
        } catch (ImplicitCastException e) {
            TestUtils.assertEquals("inconvertible value: `778` [STRING -> BYTE]", e.getFlyweightMessage());
        }

        // not a number
        try {
            SqlUtil.implicitCastStrAsByte("hello");
            Assert.fail();
        } catch (ImplicitCastException e) {
            TestUtils.assertEquals("inconvertible value: `hello` [STRING -> BYTE]", e.getFlyweightMessage());
        }
    }

    @Test
    public void testParseStrDate() {
        Assert.assertEquals(Numbers.LONG_NaN, SqlUtil.implicitCastStrAsDate(null));
        Assert.assertEquals("2022-11-20T10:30:55.123Z", Dates.toString(SqlUtil.implicitCastStrAsDate("2022-11-20T10:30:55.123Z")));
        Assert.assertEquals("2022-11-20T10:30:55.000Z", Dates.toString(SqlUtil.implicitCastStrAsDate("2022-11-20 10:30:55Z")));
        Assert.assertEquals("2022-11-20T00:00:00.000Z", Dates.toString(SqlUtil.implicitCastStrAsDate("2022-11-20 Z")));
        Assert.assertEquals("2022-11-20T00:00:00.000Z", Dates.toString(SqlUtil.implicitCastStrAsDate("2022-11-20")));
        Assert.assertEquals("2022-11-20T10:30:55.123Z", Dates.toString(SqlUtil.implicitCastStrAsDate("2022-11-20 10:30:55.123Z")));
        Assert.assertEquals("1970-01-01T00:00:00.200Z", Dates.toString(SqlUtil.implicitCastStrAsDate("200")));
        Assert.assertEquals("1969-12-31T23:59:59.100Z", Dates.toString(SqlUtil.implicitCastStrAsDate("-900")));

        // not a number
        try {
            SqlUtil.implicitCastStrAsDate("hello");
            Assert.fail();
        } catch (ImplicitCastException e) {
            TestUtils.assertEquals("inconvertible value: `hello` [STRING -> DATE]", e.getFlyweightMessage());
        }
    }

    @Test
    public void testParseStrDouble() {
        //noinspection SimplifiableAssertion
        Assert.assertFalse(SqlUtil.implicitCastStrAsDouble(null) == SqlUtil.implicitCastStrAsDouble(null));
        Assert.assertEquals(9.901E62, SqlUtil.implicitCastStrAsDouble("990.1e60"), 0.001);

        // overflow
        try {
            SqlUtil.implicitCastStrAsDouble("1e450");
            Assert.fail();
        } catch (ImplicitCastException e) {
            TestUtils.assertEquals("inconvertible value: `1e450` [STRING -> DOUBLE]", e.getFlyweightMessage());
        }

        // not a number
        try {
            SqlUtil.implicitCastStrAsDouble("hello");
            Assert.fail();
        } catch (ImplicitCastException e) {
            TestUtils.assertEquals("inconvertible value: `hello` [STRING -> DOUBLE]", e.getFlyweightMessage());
        }
    }

    @Test
    public void testParseStrFloat() {
        //noinspection SimplifiableAssertion
        Assert.assertFalse(SqlUtil.implicitCastStrAsFloat(null) == SqlUtil.implicitCastStrAsFloat(null));
        Assert.assertEquals(990.1, SqlUtil.implicitCastStrAsFloat("990.1"), 0.001);
        Assert.assertEquals(-899.23, SqlUtil.implicitCastStrAsFloat("-899.23"), 0.001);

        // overflow
        try {
            SqlUtil.implicitCastStrAsFloat("1e210");
            Assert.fail();
        } catch (ImplicitCastException e) {
            TestUtils.assertEquals("inconvertible value: `1e210` [STRING -> FLOAT]", e.getFlyweightMessage());
        }

        // not a number
        try {
            SqlUtil.implicitCastStrAsFloat("hello");
            Assert.fail();
        } catch (ImplicitCastException e) {
            TestUtils.assertEquals("inconvertible value: `hello` [STRING -> FLOAT]", e.getFlyweightMessage());
        }
    }

    @Test
    public void testParseStrInt() {
        Assert.assertEquals(Numbers.INT_NaN, SqlUtil.implicitCastStrAsInt(null));
        Assert.assertEquals(22222123, SqlUtil.implicitCastStrAsInt("22222123"));
        Assert.assertEquals(-2222232, SqlUtil.implicitCastStrAsInt("-2222232"));

        // overflow
        try {
            SqlUtil.implicitCastStrAsInt("77823232322323233");
            Assert.fail();
        } catch (ImplicitCastException e) {
            TestUtils.assertEquals("inconvertible value: `77823232322323233` [STRING -> INT]", e.getFlyweightMessage());
        }

        // not a number
        try {
            SqlUtil.implicitCastStrAsInt("hello");
            Assert.fail();
        } catch (ImplicitCastException e) {
            TestUtils.assertEquals("inconvertible value: `hello` [STRING -> INT]", e.getFlyweightMessage());
        }
    }

    @Test
    public void testImplicitCastStrAsIPv4() {
        Assert.assertEquals(0, SqlUtil.implicitCastStrAsIPv4(null));
        Assert.assertEquals(201741578, SqlUtil.implicitCastStrAsIPv4("12.6.85.10"));
        Assert.assertEquals(4738954, SqlUtil.implicitCastStrAsIPv4("0.72.79.138"));

        try {
            SqlUtil.implicitCastStrAsIPv4("77823.23232.23232.33");
            Assert.fail();
        } catch (ImplicitCastException e) {
            TestUtils.assertEquals("invalid ipv4 format: 77823.23232.23232.33", e.getFlyweightMessage());
        }

        try {
            SqlUtil.implicitCastStrAsIPv4("hello");
            Assert.fail();
        } catch (ImplicitCastException e) {
            TestUtils.assertEquals("invalid ipv4 format: hello", e.getFlyweightMessage());
        }
    }

    @Test
    public void testParseStrLong() {
        Assert.assertEquals(Numbers.LONG_NaN, SqlUtil.implicitCastStrAsLong(null));
        Assert.assertEquals(222221211212123L, SqlUtil.implicitCastStrAsLong("222221211212123"));
        Assert.assertEquals(222221211212123L, SqlUtil.implicitCastStrAsLong("222221211212123L"));
        Assert.assertEquals(-222221211212123L, SqlUtil.implicitCastStrAsLong("-222221211212123"));
        Assert.assertEquals(-222221211212123L, SqlUtil.implicitCastStrAsLong("-222221211212123L"));

        // overflow
        try {
            SqlUtil.implicitCastStrAsLong("778232323223232389080898083");
            Assert.fail();
        } catch (ImplicitCastException e) {
            TestUtils.assertEquals("inconvertible value: `778232323223232389080898083` [STRING -> LONG]", e.getFlyweightMessage());
        }

        // not a number
        try {
            SqlUtil.implicitCastStrAsLong("hello");
            Assert.fail();
        } catch (ImplicitCastException e) {
            TestUtils.assertEquals("inconvertible value: `hello` [STRING -> LONG]", e.getFlyweightMessage());
        }
    }

    @Test
    public void testParseStrShort() {
        Assert.assertEquals(0, SqlUtil.implicitCastStrAsShort(null));
        Assert.assertEquals(22222, SqlUtil.implicitCastStrAsShort("22222"));
        Assert.assertEquals(-22222, SqlUtil.implicitCastStrAsShort("-22222"));

        // overflow
        try {
            SqlUtil.implicitCastStrAsShort("77823232323");
            Assert.fail();
        } catch (ImplicitCastException e) {
            TestUtils.assertEquals("inconvertible value: `77823232323` [STRING -> SHORT]", e.getFlyweightMessage());
        }

        // not a number
        try {
            SqlUtil.implicitCastStrAsShort("hello");
            Assert.fail();
        } catch (ImplicitCastException e) {
            TestUtils.assertEquals("inconvertible value: `hello` [STRING -> SHORT]", e.getFlyweightMessage());
        }
    }

    @Test
    public void testParseStrTimestamp() {
        Assert.assertEquals(Numbers.LONG_NaN, SqlUtil.implicitCastStrAsTimestamp(null));
        Assert.assertEquals("2022-11-20T10:30:55.123999Z", Timestamps.toUSecString(SqlUtil.implicitCastStrAsTimestamp("2022-11-20T10:30:55.123999Z")));
        Assert.assertEquals("2022-11-20T10:30:55.000000Z", Timestamps.toUSecString(SqlUtil.implicitCastStrAsTimestamp("2022-11-20 10:30:55Z")));
        Assert.assertEquals("2022-11-20T00:00:00.000000Z", Timestamps.toUSecString(SqlUtil.implicitCastStrAsTimestamp("2022-11-20 Z")));
        Assert.assertEquals("2022-11-20T10:30:55.123000Z", Timestamps.toUSecString(SqlUtil.implicitCastStrAsTimestamp("2022-11-20 10:30:55.123Z")));
        Assert.assertEquals("1970-01-01T00:00:00.000200Z", Timestamps.toUSecString(SqlUtil.implicitCastStrAsTimestamp("200")));
        Assert.assertEquals("1969-12-31T23:59:59.999100Z", Timestamps.toUSecString(SqlUtil.implicitCastStrAsTimestamp("-900")));

        // not a number
        try {
            SqlUtil.implicitCastStrAsDate("hello");
            Assert.fail();
        } catch (ImplicitCastException e) {
            TestUtils.assertEquals("inconvertible value: `hello` [STRING -> DATE]", e.getFlyweightMessage());
        }
    }

    private void testImplicitCastCharAsGeoHashInvalidChar0(char c) {
        int bits = 5;
        try {
            SqlUtil.implicitCastCharAsGeoHash(c, ColumnType.getGeoHashTypeWithBits(bits));
            Assert.fail();
        } catch (ImplicitCastException e) {
            TestUtils.assertContains("inconvertible value: " + c + " [CHAR -> GEOHASH(1c)]", e.getFlyweightMessage());
        }
    }

    static {
        // this is required to initialize calendar indexes ahead of using it
        // otherwise sink can end up having odd characters
        TimestampFormatUtils.init();
    }


}