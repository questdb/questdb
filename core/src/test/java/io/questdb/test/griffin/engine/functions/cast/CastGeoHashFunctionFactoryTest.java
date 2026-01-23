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

package io.questdb.test.griffin.engine.functions.cast;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.GenericRecordMetadata;
import io.questdb.cairo.GeoHashes;
import io.questdb.cairo.sql.Function;
import io.questdb.griffin.FunctionParser;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.engine.functions.cast.CastGeoHashToGeoHashFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastStrToGeoHashFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastStrToVarcharFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastVarcharToGeoHashFunctionFactory;
import io.questdb.std.NumericException;
import io.questdb.test.griffin.BaseFunctionFactoryTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class CastGeoHashFunctionFactoryTest extends BaseFunctionFactoryTest {

    private FunctionParser functionParser;
    private GenericRecordMetadata metadata;

    @Before
    public void setUp5() {
        functions.add(new CastStrToGeoHashFunctionFactory());
        functions.add(new CastGeoHashToGeoHashFunctionFactory());
        functions.add(new CastVarcharToGeoHashFunctionFactory());
        functions.add(new CastStrToVarcharFunctionFactory());
        functionParser = createFunctionParser();
        metadata = new GenericRecordMetadata();
    }

    @Test
    public void testCastEmptyString() throws SqlException {
        String castExpr = "cast('' as geohash(1b))";
        Function function = parseFunction(castExpr, metadata, functionParser);

        Assert.assertTrue(function.isConstant());
        Assert.assertEquals(1, ColumnType.getGeoHashBits(function.getType()));
        Assert.assertEquals(GeoHashes.NULL, function.getGeoByte(null));
    }

    @Test
    public void testCastEmptyVarchar() throws SqlException {
        String castExpr = "cast(''::varchar as geohash(1b))";
        Function function = parseFunction(castExpr, metadata, functionParser);

        Assert.assertTrue(function.isConstant());
        Assert.assertEquals(1, ColumnType.getGeoHashBits(function.getType()));
        Assert.assertEquals(GeoHashes.NULL, function.getGeoByte(null));
    }

    @Test
    public void testCastEqNull() throws Exception {
        assertMemoryLeak(() -> assertSql(
                "column\n" +
                        "false\n",
                "select cast('x' as geohash(1c)) = null"
        ));
    }

    @Test
    public void testCastInvalidCharToGeoHash() {
        try {
            String castExpr = "cast('a' as geohash(1c))";
            parseFunction(castExpr, metadata, functionParser);
        } catch (SqlException e) {
            TestUtils.assertContains(e.getFlyweightMessage(), "invalid GEOHASH");
            Assert.assertEquals(5, e.getPosition());
        }
    }

    @Test
    public void testCastInvalidCharToGeoHash2() {
        try {
            String castExpr = "cast('^' as geohash(1c))";
            parseFunction(castExpr, metadata, functionParser);
        } catch (SqlException e) {
            TestUtils.assertContains(e.getFlyweightMessage(), "invalid GEOHASH");
            Assert.assertEquals(5, e.getPosition());
        }
    }

    @Test
    public void testCastMissingRightParens() {
        try {
            String castExpr = "cast('sp052w92' as geohash(2c)";
            parseFunction(castExpr, metadata, functionParser);
        } catch (SqlException e) {
            TestUtils.assertContains(e.getFlyweightMessage(), "unbalanced (");
            Assert.assertEquals(4, e.getPosition());
        }
    }

    @Test
    public void testCastMissingSize() {
        try {
            String castExpr = "cast('sp052w92' as geohash(c))";
            parseFunction(castExpr, metadata, functionParser);
        } catch (SqlException e) {
            TestUtils.assertContains(e.getFlyweightMessage(),
                    "invalid GEOHASH size, must be number followed by 'C' or 'B' character");
            Assert.assertEquals(27, e.getPosition());
        }
    }

    @Test
    public void testCastMissingSizeAndUnits() {
        try {
            String castExpr = "cast('sp052w92' as geohash())";
            parseFunction(castExpr, metadata, functionParser);
        } catch (SqlException e) {
            TestUtils.assertContains(e.getFlyweightMessage(),
                    "invalid GEOHASH, invalid type precision");
            Assert.assertEquals(27, e.getPosition());
        }
    }

    @Test
    public void testCastMissingUnits() {
        try {
            String castExpr = "cast('sp052w92' as geohash(1))";
            parseFunction(castExpr, metadata, functionParser);
        } catch (SqlException e) {
            TestUtils.assertContains(e.getFlyweightMessage(),
                    "invalid GEOHASH size, must be number followed by 'C' or 'B' character");
            Assert.assertEquals(27, e.getPosition());
        }
    }

    @Test
    public void testCastNullToGeoHash() throws SqlException {
        String castExpr = "cast(NULL as geohash(10b))";
        Function function = parseFunction(castExpr, metadata, functionParser);

        Assert.assertTrue(function.isConstant());
        Assert.assertEquals(10, ColumnType.getGeoHashBits(function.getType()));
        Assert.assertEquals(GeoHashes.NULL, function.getGeoShort(null));
    }

    @Test
    public void testCastStringToGeoHash() throws SqlException {
        String expectedGeoHash = "sp052w92";
        long expectedHash = 847187636514L;

        Function function = parseFunction(
                String.format("cast('%s' as GEOHASH(8c))", expectedGeoHash),
                metadata,
                functionParser
        );

        Assert.assertTrue(function.isConstant());
        Assert.assertEquals(ColumnType.GEOLONG, ColumnType.tagOf(function.getType()));
        Assert.assertEquals(ColumnType.getGeoHashTypeWithBits(expectedGeoHash.length() * 5), function.getType());
        Assert.assertEquals(expectedGeoHash.length() * 5, ColumnType.getGeoHashBits(function.getType()));
        Assert.assertEquals(expectedHash, function.getGeoLong(null));
        Assert.assertThrows(UnsupportedOperationException.class, () -> function.getLong(null));
        assertGeoHashLongStrEquals(expectedGeoHash, function);
    }

    @Test
    public void testCastStringToGeoHashSizesBinary() throws SqlException, NumericException {
        testCastToGeoHashSizesBinary(false);
    }

    @Test
    public void testCastStringToGeoHashSizesChar() throws SqlException {
        String longHash = "sp052w92bcde";

        for (int i = 0; i < longHash.length(); i++) {
            String expectedGeoHash = longHash.substring(0, i + 1);
            for (int j = 0; j <= i; j++) {
                int parsedGeoHashLen = j + 1;
                String castExpr = String.format("cast('%s' as geohash(%sc))", expectedGeoHash, parsedGeoHashLen);
                Function function = parseFunction(castExpr, metadata, functionParser);
                Assert.assertTrue(castExpr, function.isConstant());
                Assert.assertEquals(castExpr, parsedGeoHashLen * 5, ColumnType.getGeoHashBits(function.getType()));
            }
        }
    }

    @Test
    public void testCastStringTooLongForGeoHash() throws SqlException, NumericException {
        String castExpr = "cast('sp052w92bcde2569' as geohash(1c))";
        Function function = parseFunction(castExpr, metadata, functionParser);

        Assert.assertTrue(function.isConstant());
        Assert.assertEquals(5, ColumnType.getGeoHashBits(function.getType()));
        Assert.assertEquals(GeoHashes.fromString("s", 0, 1), function.getGeoByte(null));
    }

    @Test
    public void testCastVarcharEqNull() throws Exception {
        assertMemoryLeak(() -> assertSql(
                "column\n" +
                        "false\n",
                "select cast('x'::varchar as geohash(1c)) = null"
        ));
    }

    @Test
    public void testCastVarcharToGeoHash() throws SqlException {
        String expectedGeoHash = "sp052w92";
        long expectedHash = 847187636514L;

        Function function = parseFunction(
                String.format("cast('%s'::varchar as GEOHASH(8c))", expectedGeoHash),
                metadata,
                functionParser
        );

        Assert.assertTrue(function.isConstant());
        Assert.assertEquals(ColumnType.GEOLONG, ColumnType.tagOf(function.getType()));
        Assert.assertEquals(ColumnType.getGeoHashTypeWithBits(expectedGeoHash.length() * 5), function.getType());
        Assert.assertEquals(expectedGeoHash.length() * 5, ColumnType.getGeoHashBits(function.getType()));
        Assert.assertEquals(expectedHash, function.getGeoLong(null));
        Assert.assertThrows(UnsupportedOperationException.class, () -> function.getLong(null));
        assertGeoHashLongStrEquals(expectedGeoHash, function);
    }

    @Test
    public void testCastVarcharToGeoHashSizesBinary() throws SqlException, NumericException {
        testCastToGeoHashSizesBinary(true);
    }

    private void assertGeoHashLongStrEquals(String expectedGeoHash, Function function) {
        sink.clear();
        final int chars = ColumnType.getGeoHashBits(function.getType()) / 5;
        switch (ColumnType.tagOf(function.getType())) {
            case ColumnType.GEOBYTE:
                GeoHashes.appendChars(function.getGeoByte(null), chars, sink);
                break;
            case ColumnType.GEOSHORT:
                GeoHashes.appendChars(function.getGeoShort(null), chars, sink);
                break;
            case ColumnType.GEOINT:
                GeoHashes.appendChars(function.getGeoInt(null), chars, sink);
                break;
            default:
                GeoHashes.appendChars(function.getGeoLong(null), chars, sink);
                break;
        }
        TestUtils.assertEquals(expectedGeoHash, sink);
    }

    private void testCastToGeoHashSizesBinary(boolean varchar) throws SqlException, NumericException {
        String geoHash = "sp052w92p1p8ignore";
        int geoHashLen = 12;
        long fullGeoHash = GeoHashes.fromString(geoHash, 0, geoHashLen);
        Assert.assertEquals(888340623145993896L, fullGeoHash);
        for (int c = 1; c <= geoHashLen; c++) {
            String expectedGeoHash = geoHash.substring(0, c);
            Function function = null;
            for (int b = 1; b <= c * 5; b++) {
                String castExpr = String.format("cast('%s'" + (varchar ? "::varchar" : "") + " as geohash(%sb))", expectedGeoHash, b);
                function = parseFunction(castExpr, metadata, functionParser);
                Assert.assertTrue(castExpr, function.isConstant());
                Assert.assertEquals(castExpr, b, ColumnType.getGeoHashBits(function.getType()));
                switch (ColumnType.tagOf(function.getType())) {
                    case ColumnType.GEOBYTE:
                        Assert.assertEquals(castExpr, fullGeoHash >>> (geoHashLen * 5 - b), function.getGeoByte(null));
                        break;
                    case ColumnType.GEOSHORT:
                        Assert.assertEquals(castExpr, fullGeoHash >>> (geoHashLen * 5 - b), function.getGeoShort(null));
                        break;
                    case ColumnType.GEOINT:
                        Assert.assertEquals(castExpr, fullGeoHash >>> (geoHashLen * 5 - b), function.getGeoInt(null));
                        break;
                    default:
                        Assert.assertEquals(castExpr, fullGeoHash >>> (geoHashLen * 5 - b), function.getGeoLong(null));
                        break;
                }
            }
            if (function != null) { // just to remove the warning
                assertGeoHashLongStrEquals(expectedGeoHash, function);
            }
        }
    }
}