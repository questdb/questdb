/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
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

package io.questdb.griffin.engine.functions.cast;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.GenericRecordMetadata;
import io.questdb.cairo.GeoHashes;
import io.questdb.cairo.sql.Function;
import io.questdb.griffin.BaseFunctionFactoryTest;
import io.questdb.griffin.FunctionParser;
import io.questdb.griffin.SqlException;
import io.questdb.std.NumericException;
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
        functionParser = createFunctionParser();
        metadata = new GenericRecordMetadata();
    }

    @Test
    public void testCastStringToGeoHash() throws SqlException {
        String expectedGeohash = "sp052w92";
        long expectedHash = 847187636514L;

        Function function = parseFunction(
                String.format("cast('%s' as GEOHASH(8c))", expectedGeohash),
                metadata,
                functionParser);

        Assert.assertTrue(function.isConstant());
        Assert.assertNotEquals(ColumnType.GEOHASH, function.getType());
        Assert.assertEquals(ColumnType.geohashWithPrecision(expectedGeohash.length() * 5), function.getType());
        Assert.assertEquals(expectedGeohash.length() * 5, GeoHashes.getBitsPrecision(function.getType()));
        Assert.assertEquals(expectedHash, function.getGeoHashLong(null));
        Assert.assertThrows(UnsupportedOperationException.class, () -> function.getLong(null));
        Assert.assertEquals(0, GeoHashes.hashSize(function.getGeoHashLong(null)));
        assertGeoHashLongStrEquals(expectedGeohash, function);
    }

    @Test
    public void testCastStringToGeoHashSizesChar() throws SqlException {
        String longHash = "sp052w92bcde";

        for (int i = 0; i < longHash.length(); i++) {
            String expectedGeohash = longHash.substring(0, i + 1);
            for (int j = 0; j <= i; j++) {
                int parsedGeoHashLen = j + 1;
                String castExpr = String.format("cast('%s' as geohash(%sc))", expectedGeohash, parsedGeoHashLen);
                Function function = parseFunction(
                        castExpr,
                        metadata,
                        functionParser);
                Assert.assertTrue(castExpr, function.isConstant());
                Assert.assertEquals(castExpr, parsedGeoHashLen * 5, GeoHashes.getBitsPrecision(function.getType()));
            }
        }
    }

    @Test
    public void testCastStringToGeoHashSizesBinary() throws SqlException, NumericException {
        String geohash = "sp052w92p1p8ignore";
        int geohashLen = 12;
        long fullGeohash = GeoHashes.fromString0(geohash, 0, geohashLen);
        Assert.assertEquals(888340623145993896L, fullGeohash);
        for (int c = 1; c <= geohashLen; c++) {
            String expectedGeohash = geohash.substring(0, c);
            Function function = null;
            for (int b = 1; b <= c * 5; b++) {
                String castExpr = String.format("cast('%s' as geohash(%sb))", expectedGeohash, b);
                function = parseFunction(castExpr, metadata, functionParser);
                Assert.assertTrue(castExpr, function.isConstant());
                Assert.assertEquals(castExpr, b, GeoHashes.getBitsPrecision(function.getType()));
                Assert.assertEquals(castExpr, fullGeohash >>> (geohashLen * 5 - b), function.getGeoHashLong(null));
            }
            if (function != null) { // just to remove the warning
                assertGeoHashLongStrEquals(expectedGeohash, function);
            }
        }
    }

    @Test
    public void testCastEmptyString() throws SqlException {
        String castExpr = "cast('' as geohash(1b))";
        Function function = parseFunction(castExpr, metadata, functionParser);

        Assert.assertTrue(function.isConstant());
        Assert.assertEquals(1, GeoHashes.getBitsPrecision(function.getType()));
        Assert.assertEquals(GeoHashes.NULL, function.getGeoHashByte(null));
    }

    @Test
    public void testCastNullToGeohash() throws SqlException {
        String castExpr = "cast(NULL as geohash(10b))";
        Function function = parseFunction(castExpr, metadata, functionParser);

        Assert.assertTrue(function.isConstant());
        Assert.assertEquals(10, GeoHashes.getBitsPrecision(function.getType()));
        Assert.assertEquals(GeoHashes.NULL, function.getGeoHashShort(null));
    }

    @Test
    public void testCastInvalidCharToGeohash() {
        try {
            String castExpr = "cast('a' as geohash(1c))";
            parseFunction(castExpr, metadata, functionParser);
        } catch (SqlException e) {
            TestUtils.assertContains(e.getFlyweightMessage(), "invalid GEOHASH");
            Assert.assertEquals(5, e.getPosition());
        }
    }

    @Test
    public void testCastInvalidCharToGeohash2() {
        try {
            String castExpr = "cast('^' as geohash(1c))";
            parseFunction(castExpr, metadata, functionParser);
        } catch (SqlException e) {
            TestUtils.assertContains(e.getFlyweightMessage(), "invalid GEOHASH");
            Assert.assertEquals(5, e.getPosition());
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
            Assert.assertEquals(19, e.getPosition());
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
            Assert.assertEquals(19, e.getPosition());
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
    public void testCastMissingRightParens() {
        try {
            String castExpr = "cast('sp052w92' as geohash(2c)";
            parseFunction(castExpr, metadata, functionParser);
        } catch (SqlException e) {
            TestUtils.assertContains(e.getFlyweightMessage(),
                    "unbalanced (");
            Assert.assertEquals(4, e.getPosition());
        }
    }

    @Test
    public void testCastStringTooLongForGeohash() throws SqlException, NumericException {
        String castExpr = "cast('sp052w92bcde2569' as geohash(1c))";
        Function function = parseFunction(castExpr, metadata, functionParser);

        Assert.assertTrue(function.isConstant());
        Assert.assertEquals(5, GeoHashes.getBitsPrecision(function.getType()));
        Assert.assertEquals(GeoHashes.fromString0("s", 0, 1), function.getGeoHashByte(null));
    }

    private void assertGeoHashLongStrEquals(String expectedGeohash, Function function) {
        sink.clear();
        GeoHashes.toString(
                function.getGeoHashLong(null),
                GeoHashes.getBitsPrecision(function.getType()) / 5,
                sink);
        Assert.assertEquals(expectedGeohash, sink.toString());
    }
}