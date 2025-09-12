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
import io.questdb.cairo.GeoHashes;
import io.questdb.cairo.ImplicitCastException;
import io.questdb.cairo.TimestampDriver;
import io.questdb.griffin.CharacterStore;
import io.questdb.griffin.OperatorExpression;
import io.questdb.griffin.OperatorRegistry;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlUtil;
import io.questdb.griffin.engine.functions.Long256Function;
import io.questdb.griffin.engine.functions.constants.Constants;
import io.questdb.griffin.engine.functions.constants.Long256Constant;
import io.questdb.griffin.model.QueryColumn;
import io.questdb.mp.SOCountDownLatch;
import io.questdb.std.LowerCaseCharSequenceObjHashMap;
import io.questdb.std.Numbers;
import io.questdb.std.Rnd;
import io.questdb.std.datetime.microtime.Micros;
import io.questdb.std.datetime.microtime.MicrosFormatUtils;
import io.questdb.std.datetime.millitime.Dates;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf8Sequence;
import io.questdb.std.str.Utf8String;
import io.questdb.std.str.Utf8StringSink;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

public class SqlUtilTest {

    @Test
    public void testExprColumnAliasDisallowedAlias() {
        OperatorRegistry registry = OperatorExpression.getRegistry();
        CharacterStore store = new CharacterStore(32, 1);
        LowerCaseCharSequenceObjHashMap<QueryColumn> aliasMap = new LowerCaseCharSequenceObjHashMap<>(0);
        for (int i = 0, n = registry.operators.size(); i < n; i++) {
            String token = registry.operators.getQuick(i).getToken();
            Assert.assertEquals(
                    '"' + token + '"',
                    SqlUtil.createExprColumnAlias(store, token, aliasMap, 64, true).toString()
            );

            token = token.toUpperCase();
            Assert.assertEquals(
                    '"' + token + '"',
                    SqlUtil.createExprColumnAlias(store, token, aliasMap, 64, true).toString()
            );
        }
    }

    @Test
    public void testExprColumnAliasDuplicates() {
        CharacterStore store = new CharacterStore(32, 1);
        LowerCaseCharSequenceObjHashMap<QueryColumn> aliasMap = new LowerCaseCharSequenceObjHashMap<>(8);
        aliasMap.put("same", null);
        Assert.assertEquals(
                "same_2",
                SqlUtil.createExprColumnAlias(store, "same", aliasMap, 64).toString()
        );
        aliasMap.put("same_2", null);
        Assert.assertEquals(
                "same_3",
                SqlUtil.createExprColumnAlias(store, "same", aliasMap, 64).toString()
        );
    }

    @Test
    public void testExprColumnAliasSimpleCase() {
        CharacterStore store = new CharacterStore(32, 1);
        LowerCaseCharSequenceObjHashMap<QueryColumn> aliasMap = new LowerCaseCharSequenceObjHashMap<>(0);
        Assert.assertEquals(
                "basic",
                SqlUtil.createExprColumnAlias(store, "basic", aliasMap, 64).toString()
        );
    }

    @Test
    public void testExprColumnAliasTrimEnd() {
        CharacterStore store = new CharacterStore(32, 1);
        LowerCaseCharSequenceObjHashMap<QueryColumn> aliasMap = new LowerCaseCharSequenceObjHashMap<>(0);
        Assert.assertEquals(
                "  space",
                SqlUtil.createExprColumnAlias(store, "  space    ", aliasMap, 64).toString()
        );
    }

    @Test
    public void testExprColumnAliasTrimmed() {
        CharacterStore store = new CharacterStore(32, 1);
        LowerCaseCharSequenceObjHashMap<QueryColumn> aliasMap = new LowerCaseCharSequenceObjHashMap<>(0);
        Assert.assertEquals(
                "longstr",
                SqlUtil.createExprColumnAlias(store, "longstring", aliasMap, 7).toString()
        );
    }

    @Test
    public void testExprNonLiteral() {
        CharacterStore store = new CharacterStore(32, 1);
        LowerCaseCharSequenceObjHashMap<QueryColumn> aliasMap = new LowerCaseCharSequenceObjHashMap<>(0);
        Assert.assertEquals(
                "\"quoted\"",
                SqlUtil.createExprColumnAlias(store, "\"quoted\"", aliasMap, 64, true).toString()
        );
        Assert.assertEquals(
                "\"prefix.nonliteral\"",
                SqlUtil.createExprColumnAlias(store, "prefix.nonliteral", aliasMap, 64, true).toString()
        );
        Assert.assertEquals(
                "\"prefix.\"",
                SqlUtil.createExprColumnAlias(store, "prefix.", aliasMap, 64, true).toString()
        );
    }

    @Test
    public void testExprPrefixedColumn() {
        CharacterStore store = new CharacterStore(32, 1);
        LowerCaseCharSequenceObjHashMap<QueryColumn> aliasMap = new LowerCaseCharSequenceObjHashMap<>(0);
        Assert.assertEquals(
                "basic",
                SqlUtil.createExprColumnAlias(store, "table.basic", aliasMap, 64).toString()
        );
        aliasMap.put("basic", null);
        Assert.assertEquals(
                "\"between\"",
                SqlUtil.createExprColumnAlias(store, "table.between", aliasMap, 64).toString()
        );
        Assert.assertEquals(
                "\"quoted\"",
                SqlUtil.createExprColumnAlias(store, "\"table\".\"quoted\"", aliasMap, 64).toString()
        );
        Assert.assertEquals(
                "\"quoted.table\"",
                SqlUtil.createExprColumnAlias(store, "\"quoted.table\"", aliasMap, 64).toString()
        );
        Assert.assertEquals(
                "spaces",
                SqlUtil.createExprColumnAlias(store, "table.spaces   ", aliasMap, 64).toString()
        );
        Assert.assertEquals(
                "\"quoted spaces   \"",
                SqlUtil.createExprColumnAlias(store, "table.\"quoted spaces   \"", aliasMap, 64).toString()
        );
        Assert.assertEquals(
                "\"table.\"",
                SqlUtil.createExprColumnAlias(store, "table.", aliasMap, 64).toString()
        );

        for (int i = 0; i < 100; i++) {
            Assert.assertEquals(
                    "basic_" + (i + 2),
                    SqlUtil.createExprColumnAlias(store, "table.basic", aliasMap, 64).toString()
            );
            aliasMap.put("basic_" + (i + 2), null);
        }
    }

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
            TestUtils.assertContains(e.getFlyweightMessage(), "inconvertible value: c [CHAR -> GEOHASH(6b)]");
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
    public void testImplicitCastStrAsIPv4() {
        Assert.assertEquals(0, SqlUtil.implicitCastStrAsIPv4((CharSequence) null));
        Assert.assertEquals(201741578, SqlUtil.implicitCastStrAsIPv4("12.6.85.10"));
        Assert.assertEquals(4738954, SqlUtil.implicitCastStrAsIPv4("0.72.79.138"));

        try {
            SqlUtil.implicitCastStrAsIPv4("77823.23232.23232.33");
            Assert.fail();
        } catch (ImplicitCastException e) {
            TestUtils.assertEquals("invalid IPv4 format: 77823.23232.23232.33", e.getFlyweightMessage());
        }

        try {
            SqlUtil.implicitCastStrAsIPv4("hello");
            Assert.fail();
        } catch (ImplicitCastException e) {
            TestUtils.assertEquals("invalid IPv4 format: hello", e.getFlyweightMessage());
        }
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
    public void testImplicitCastUtf8StrAsIPv4() {
        Assert.assertEquals(0, SqlUtil.implicitCastStrAsIPv4((Utf8Sequence) null));
        Assert.assertEquals(201741578, SqlUtil.implicitCastStrAsIPv4(new Utf8String("12.6.85.10")));
        Assert.assertEquals(4738954, SqlUtil.implicitCastStrAsIPv4(new Utf8String("0.72.79.138")));

        try {
            SqlUtil.implicitCastStrAsIPv4(new Utf8String("77823.23232.23232.33"));
            Assert.fail();
        } catch (ImplicitCastException e) {
            TestUtils.assertEquals("invalid IPv4 format: 77823.23232.23232.33", e.getFlyweightMessage());
        }

        try {
            SqlUtil.implicitCastStrAsIPv4(new Utf8String("hello"));
            Assert.fail();
        } catch (ImplicitCastException e) {
            TestUtils.assertEquals("invalid IPv4 format: hello", e.getFlyweightMessage());
        }
    }

    @Test
    public void testNaNCast() {
        Assert.assertEquals(0, SqlUtil.implicitCastIntAsByte(Numbers.INT_NULL));
        Assert.assertEquals(0, SqlUtil.implicitCastIntAsShort(Numbers.INT_NULL));
        Assert.assertEquals(0, SqlUtil.implicitCastLongAsByte(Numbers.LONG_NULL));
        Assert.assertEquals(Numbers.INT_NULL, SqlUtil.implicitCastLongAsInt(Numbers.LONG_NULL));
        Assert.assertEquals(0, SqlUtil.implicitCastLongAsShort(Numbers.LONG_NULL));
    }

    @Test
    public void testParseMicros() throws SqlException {
        Assert.assertEquals(1, SqlUtil.expectMicros("1000ns", 12));
        Assert.assertEquals(1, SqlUtil.expectMicros("1us", 12));
        Assert.assertEquals(1000, SqlUtil.expectMicros("1ms", 12));
        Assert.assertEquals(2000000, SqlUtil.expectMicros("2s", 12));
        Assert.assertEquals(180000000, SqlUtil.expectMicros("3m", 12));
        Assert.assertEquals(14400000000L, SqlUtil.expectMicros("4h", 12));
        Assert.assertEquals(432000000000L, SqlUtil.expectMicros("5d", 12));
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
    public void testParseMicrosTooLongQualifier() {
        try {
            SqlUtil.expectMicros("125usu", 12);
            Assert.fail();
        } catch (SqlException e) {
            TestUtils.assertContains(e.getFlyweightMessage(), "expected 1/2 letter interval qualifier in 125us");
        }
    }

    @Test
    public void testParseSeconds() throws SqlException {
        Assert.assertEquals(1, SqlUtil.expectSeconds("1s", 12));
        Assert.assertEquals(120, SqlUtil.expectSeconds("2m", 12));
        Assert.assertEquals(10800, SqlUtil.expectSeconds("3h", 12));
        Assert.assertEquals(345600, SqlUtil.expectSeconds("4d", 12));
    }

    @Test
    public void testParseSecondsSansQualifier() {
        try {
            SqlUtil.expectSeconds("125", 12);
            Assert.fail();
        } catch (SqlException e) {
            TestUtils.assertContains(e.getFlyweightMessage(), "expected interval qualifier");
        }
    }

    @Test
    public void testParseSecondsTooLongQualifier() {
        try {
            SqlUtil.expectSeconds("125us", 12);
            Assert.fail();
        } catch (SqlException e) {
            TestUtils.assertContains(e.getFlyweightMessage(), "expected single letter interval qualifier in 125us");
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
    public void testParseStrChar() {
        Assert.assertEquals(0, SqlUtil.implicitCastStrAsChar(null));
        Assert.assertEquals(0, SqlUtil.implicitCastStrAsChar(""));
        Assert.assertEquals('a', SqlUtil.implicitCastStrAsChar("a"));
        // arabic
        Assert.assertEquals('ع', SqlUtil.implicitCastStrAsChar("ع"));

        try {
            SqlUtil.implicitCastStrAsChar("hello");
            Assert.fail();
        } catch (ImplicitCastException e) {
            TestUtils.assertEquals("inconvertible value: `hello` [STRING -> CHAR]", e.getFlyweightMessage());
        }
    }

    @Test
    public void testParseStrDate() {
        Assert.assertEquals(Numbers.LONG_NULL, SqlUtil.implicitCastStrAsDate(null));
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
        Assert.assertNotSame(SqlUtil.implicitCastStrAsDouble(null), SqlUtil.implicitCastStrAsDouble(null));
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
        Assert.assertNotSame(SqlUtil.implicitCastStrAsFloat(null), SqlUtil.implicitCastStrAsFloat(null));
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
        Assert.assertEquals(Numbers.INT_NULL, SqlUtil.implicitCastStrAsInt(null));
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
    public void testParseStrLong() {
        Assert.assertEquals(Numbers.LONG_NULL, SqlUtil.implicitCastStrAsLong(null));
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
        TimestampDriver timestampDriver = ColumnType.getTimestampDriver(ColumnType.TIMESTAMP);
        Assert.assertEquals(Numbers.LONG_NULL, timestampDriver.implicitCast(null));
        Assert.assertEquals("2022-11-20T10:30:55.123999Z", Micros.toUSecString(timestampDriver.implicitCast("2022-11-20T10:30:55.123999Z")));
        Assert.assertEquals("2022-11-20T10:30:55.000000Z", Micros.toUSecString(timestampDriver.implicitCast("2022-11-20 10:30:55Z")));
        Assert.assertEquals("2022-11-20T00:00:00.000000Z", Micros.toUSecString(timestampDriver.implicitCast("2022-11-20 Z")));
        Assert.assertEquals("2022-11-20T10:30:55.123000Z", Micros.toUSecString(timestampDriver.implicitCast("2022-11-20 10:30:55.123Z")));
        Assert.assertEquals("1970-01-01T00:00:00.000200Z", Micros.toUSecString(timestampDriver.implicitCast("200")));
        Assert.assertEquals("1969-12-31T23:59:59.999100Z", Micros.toUSecString(timestampDriver.implicitCast("-900")));

        // not a number
        try {
            timestampDriver.implicitCast("hello");
            Assert.fail();
        } catch (ImplicitCastException e) {
            TestUtils.assertEquals("inconvertible value: `hello` [STRING -> TIMESTAMP]", e.getFlyweightMessage());
        }
    }

    @Test
    public void testParseStrVarcharAsTimestamp0() {
        TimestampDriver timestampDriver = ColumnType.getTimestampDriver(ColumnType.TIMESTAMP);
        Assert.assertEquals(Numbers.LONG_NULL, timestampDriver.implicitCastVarchar(null));
        Assert.assertEquals("2022-11-20T10:30:55.123999Z", Micros.toUSecString(timestampDriver.implicitCastVarchar(new Utf8String("2022-11-20T10:30:55.123999Z"))));
        Assert.assertEquals("2022-11-20T10:30:55.000000Z", Micros.toUSecString(timestampDriver.implicitCastVarchar(new Utf8String("2022-11-20 10:30:55Z"))));
        Assert.assertEquals("2022-11-20T00:00:00.000000Z", Micros.toUSecString(timestampDriver.implicitCastVarchar(new Utf8String("2022-11-20 Z"))));
        Assert.assertEquals("2022-11-20T10:30:55.123000Z", Micros.toUSecString(timestampDriver.implicitCastVarchar(new Utf8String("2022-11-20 10:30:55.123Z"))));
        Assert.assertEquals("1970-01-01T00:00:00.000200Z", Micros.toUSecString(timestampDriver.implicitCastVarchar(new Utf8String("200"))));
        Assert.assertEquals("1969-12-31T23:59:59.999100Z", Micros.toUSecString(timestampDriver.implicitCastVarchar(new Utf8String("-900"))));

        // not a number
        try {
            timestampDriver.implicitCastVarchar(new Utf8String("hello"));
            Assert.fail();
        } catch (ImplicitCastException e) {
            TestUtils.assertEquals("inconvertible value: `hello` [VARCHAR -> TIMESTAMP]", e.getFlyweightMessage());
        }
    }

    @Test
    public void testParseVarcharByte() {
        Utf8StringSink sink = new Utf8StringSink();
        Assert.assertEquals(0, SqlUtil.implicitCastVarcharAsByte(null));

        sink.put("89");
        Assert.assertEquals(89, SqlUtil.implicitCastVarcharAsByte(sink));

        sink.clear();
        sink.put("-89");
        Assert.assertEquals(-89, SqlUtil.implicitCastVarcharAsByte(sink));

        // overflow
        try {
            sink.clear();
            sink.put("778");
            SqlUtil.implicitCastVarcharAsByte(sink);
            Assert.fail();
        } catch (ImplicitCastException e) {
            TestUtils.assertEquals("inconvertible value: `778` [VARCHAR -> BYTE]", e.getFlyweightMessage());
        }

        // not a number
        try {
            sink.clear();
            sink.put("hello");
            SqlUtil.implicitCastVarcharAsByte(sink);
            Assert.fail();
        } catch (ImplicitCastException e) {
            TestUtils.assertEquals("inconvertible value: `hello` [VARCHAR -> BYTE]", e.getFlyweightMessage());
        }
    }

    @Test
    public void testParseVarcharChar() {
        Utf8StringSink sink = new Utf8StringSink();
        Assert.assertEquals(0, SqlUtil.implicitCastVarcharAsChar(null));

        Assert.assertEquals(0, SqlUtil.implicitCastVarcharAsChar(sink));

        sink.clear();
        sink.put("a");
        Assert.assertEquals('a', SqlUtil.implicitCastVarcharAsChar(sink));
        // arabic
        sink.clear();
        sink.put("ع");
        Assert.assertEquals('ع', SqlUtil.implicitCastVarcharAsChar(sink));

        try {
            sink.clear();
            sink.put("hello");
            SqlUtil.implicitCastVarcharAsChar(sink);
            Assert.fail();
        } catch (ImplicitCastException e) {
            TestUtils.assertEquals("inconvertible value: `hello` [VARCHAR -> CHAR]", e.getFlyweightMessage());
        }
    }

    @Test
    public void testParseVarcharDate() {
        Assert.assertEquals(Numbers.LONG_NULL, SqlUtil.implicitCastVarcharAsDate(null));
        Assert.assertEquals("2022-11-20T10:30:55.123Z", Dates.toString(SqlUtil.implicitCastVarcharAsDate("2022-11-20T10:30:55.123Z")));
        Assert.assertEquals("2022-11-20T10:30:55.000Z", Dates.toString(SqlUtil.implicitCastVarcharAsDate("2022-11-20 10:30:55Z")));
        Assert.assertEquals("2022-11-20T00:00:00.000Z", Dates.toString(SqlUtil.implicitCastVarcharAsDate("2022-11-20 Z")));
        Assert.assertEquals("2022-11-20T00:00:00.000Z", Dates.toString(SqlUtil.implicitCastVarcharAsDate("2022-11-20")));
        Assert.assertEquals("2022-11-20T10:30:55.123Z", Dates.toString(SqlUtil.implicitCastVarcharAsDate("2022-11-20 10:30:55.123Z")));
        Assert.assertEquals("1970-01-01T00:00:00.200Z", Dates.toString(SqlUtil.implicitCastVarcharAsDate("200")));
        Assert.assertEquals("1969-12-31T23:59:59.100Z", Dates.toString(SqlUtil.implicitCastVarcharAsDate("-900")));

        // not a number
        try {
            SqlUtil.implicitCastVarcharAsDate("hello");
            Assert.fail();
        } catch (ImplicitCastException e) {
            TestUtils.assertEquals("inconvertible value: `hello` [VARCHAR -> DATE]", e.getFlyweightMessage());
        }
    }

    @Test
    public void testParseVarcharDouble() {
        Utf8StringSink sink = new Utf8StringSink();
        //noinspection SimplifiableAssertion
        Assert.assertFalse(SqlUtil.implicitCastVarcharAsDouble(null) == SqlUtil.implicitCastStrAsDouble(null));
        sink.put("990.1e60");
        Assert.assertEquals(9.901E62, SqlUtil.implicitCastVarcharAsDouble(sink), 0.001);

        // overflow
        try {
            sink.clear();
            sink.put("1e450");
            SqlUtil.implicitCastVarcharAsDouble(sink);
            Assert.fail();
        } catch (ImplicitCastException e) {
            TestUtils.assertEquals("inconvertible value: `1e450` [VARCHAR -> DOUBLE]", e.getFlyweightMessage());
        }

        // not a number
        try {
            sink.clear();
            sink.put("hello");
            SqlUtil.implicitCastVarcharAsDouble(sink);
            Assert.fail();
        } catch (ImplicitCastException e) {
            TestUtils.assertEquals("inconvertible value: `hello` [VARCHAR -> DOUBLE]", e.getFlyweightMessage());
        }
    }

    @Test
    public void testParseVarcharFloat() {
        Utf8StringSink sink = new Utf8StringSink();
        //noinspection SimplifiableAssertion
        Assert.assertFalse(SqlUtil.implicitCastVarcharAsFloat(null) == SqlUtil.implicitCastStrAsFloat(null));

        sink.put("990.1");
        Assert.assertEquals(990.1, SqlUtil.implicitCastVarcharAsFloat(sink), 0.001);

        sink.clear();
        sink.put("-899.23");
        Assert.assertEquals(-899.23, SqlUtil.implicitCastVarcharAsFloat(sink), 0.001);

        // overflow
        try {
            sink.clear();
            sink.put("1e210");
            SqlUtil.implicitCastVarcharAsFloat(sink);
            Assert.fail();
        } catch (ImplicitCastException e) {
            TestUtils.assertEquals("inconvertible value: `1e210` [VARCHAR -> FLOAT]", e.getFlyweightMessage());
        }

        // not a number
        try {
            sink.clear();
            sink.put("hello");
            SqlUtil.implicitCastVarcharAsFloat(sink);
            Assert.fail();
        } catch (ImplicitCastException e) {
            TestUtils.assertEquals("inconvertible value: `hello` [VARCHAR -> FLOAT]", e.getFlyweightMessage());
        }
    }

    @Test
    public void testParseVarcharInt() {
        Utf8StringSink sink = new Utf8StringSink();
        Assert.assertEquals(Numbers.INT_NULL, SqlUtil.implicitCastVarcharAsInt(null));

        sink.put("22222123");
        Assert.assertEquals(22222123, SqlUtil.implicitCastVarcharAsInt(sink));

        sink.clear();
        sink.put("-2222232");
        Assert.assertEquals(-2222232, SqlUtil.implicitCastVarcharAsInt(sink));

        // overflow
        try {
            sink.clear();
            sink.put("77823232322323233");
            SqlUtil.implicitCastVarcharAsInt(sink);
            Assert.fail();
        } catch (ImplicitCastException e) {
            TestUtils.assertEquals("inconvertible value: `77823232322323233` [VARCHAR -> INT]", e.getFlyweightMessage());
        }

        // not a number
        try {
            sink.clear();
            sink.put("hello");
            SqlUtil.implicitCastVarcharAsInt(sink);
            Assert.fail();
        } catch (ImplicitCastException e) {
            TestUtils.assertEquals("inconvertible value: `hello` [VARCHAR -> INT]", e.getFlyweightMessage());
        }
    }

    @Test
    public void testParseVarcharLong() {
        Assert.assertEquals(Numbers.LONG_NULL, SqlUtil.implicitCastStrAsLong(null));
        Utf8StringSink sink = new Utf8StringSink();
        sink.put("222221211212123");
        Assert.assertEquals(222221211212123L, SqlUtil.implicitCastVarcharAsLong(sink));

        sink.clear();
        sink.put("222221211212123L");
        Assert.assertEquals(222221211212123L, SqlUtil.implicitCastVarcharAsLong(sink));

        sink.clear();
        sink.put("-222221211212123");
        Assert.assertEquals(-222221211212123L, SqlUtil.implicitCastVarcharAsLong(sink));

        sink.clear();
        sink.put("-222221211212123L");
        Assert.assertEquals(-222221211212123L, SqlUtil.implicitCastVarcharAsLong(sink));

        // overflow
        try {
            sink.clear();
            sink.put("778232323223232389080898083");
            SqlUtil.implicitCastVarcharAsLong(sink);
            Assert.fail();
        } catch (ImplicitCastException e) {
            TestUtils.assertEquals("inconvertible value: `778232323223232389080898083` [VARCHAR -> LONG]", e.getFlyweightMessage());
        }

        // not a number
        try {
            sink.clear();
            sink.put("hello");
            SqlUtil.implicitCastVarcharAsLong(sink);
            Assert.fail();
        } catch (ImplicitCastException e) {
            TestUtils.assertEquals("inconvertible value: `hello` [VARCHAR -> LONG]", e.getFlyweightMessage());
        }
    }

    @Test
    public void testParseVarcharShort() {
        Assert.assertEquals(0, SqlUtil.implicitCastVarcharAsShort(null));
        Utf8StringSink sink = new Utf8StringSink();
        sink.put("22222");
        Assert.assertEquals(22222, SqlUtil.implicitCastVarcharAsShort(sink));
        sink.clear();
        sink.put("-22222");
        Assert.assertEquals(-22222, SqlUtil.implicitCastVarcharAsShort(sink));

        // overflow
        try {
            sink.clear();
            sink.put("77823232323");
            SqlUtil.implicitCastVarcharAsShort(sink);
            Assert.fail();
        } catch (ImplicitCastException e) {
            TestUtils.assertEquals("inconvertible value: `77823232323` [VARCHAR -> SHORT]", e.getFlyweightMessage());
        }

        // not a number
        try {
            sink.clear();
            sink.put("hello");
            SqlUtil.implicitCastVarcharAsShort(sink);
            Assert.fail();
        } catch (ImplicitCastException e) {
            TestUtils.assertEquals("inconvertible value: `hello` [VARCHAR -> SHORT]", e.getFlyweightMessage());
        }
    }

    private void testImplicitCastCharAsGeoHashInvalidChar0(char c) {
        int bits = 5;
        try {
            SqlUtil.implicitCastCharAsGeoHash(c, ColumnType.getGeoHashTypeWithBits(bits));
            Assert.fail();
        } catch (ImplicitCastException e) {
            TestUtils.assertContains(e.getFlyweightMessage(), "inconvertible value: " + c + " [CHAR -> GEOHASH(1c)]");
        }
    }

    static {
        // this is required to initialize calendar indexes ahead of using them
        // otherwise sink can end up having odd characters
        MicrosFormatUtils.init();
    }
}
