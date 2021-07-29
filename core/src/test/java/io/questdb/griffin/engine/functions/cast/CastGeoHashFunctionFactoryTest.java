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
import io.questdb.cairo.GeoHashExtra;
import io.questdb.cairo.sql.Function;
import io.questdb.griffin.BaseFunctionFactoryTest;
import io.questdb.griffin.FunctionParser;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.engine.functions.geohash.GeoHashNative;
import io.questdb.std.NumericException;
import io.questdb.std.str.StringSink;
import org.junit.Assert;
import org.junit.Test;

public class CastGeoHashFunctionFactoryTest extends BaseFunctionFactoryTest {

    @Test
    public void testCastStringToGeoHash() throws SqlException {
        String expectedGeohash = "sp052w92";
        long expectedHash = 847187636514L;

        functions.add(new CastStrToGeoHashFunctionFactory());
        FunctionParser functionParser = createFunctionParser();
        GenericRecordMetadata metadata = new GenericRecordMetadata();
        Function function = parseFunction(
                String.format("cast('%s' as GEOHASH(8c))", expectedGeohash),
                metadata,
                functionParser);

        Assert.assertTrue(function.isConstant());
        Assert.assertNotEquals(ColumnType.GEOHASH, function.getType());
        Assert.assertEquals(GeoHashExtra.setBitsPrecision(ColumnType.GEOHASH, expectedGeohash.length() * 5), function.getType());
        Assert.assertEquals(expectedGeohash.length() * 5, GeoHashExtra.getBitsPrecision(function.getType()));
        Assert.assertEquals(expectedHash, function.getGeoHash(null));
        Assert.assertEquals(expectedHash, function.getLong(null));
        Assert.assertEquals(0, GeoHashNative.hashSize(function.getLong(null)));

        StringSink sink = new StringSink();
        function.getStr(null, sink);
        Assert.assertEquals(expectedGeohash, sink.toString());
    }

    @Test
    public void testCastStringToGeoHashSizesChar() throws SqlException {
        String longHash = "sp052w92bcde";
        functions.add(new CastStrToGeoHashFunctionFactory());
        FunctionParser functionParser = createFunctionParser();
        GenericRecordMetadata metadata = new GenericRecordMetadata();

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
                Assert.assertEquals(castExpr, parsedGeoHashLen * 5, GeoHashExtra.getBitsPrecision(function.getType()));
            }
        }
    }

    @Test
    public void testCastStringToGeoHashSizesBinary() throws SqlException, NumericException {
        String geohash = "sp052w92p1p8ignore";
        int geohashLen = Math.min(geohash.length(), 12);
        functions.add(new CastStrToGeoHashFunctionFactory());
        FunctionParser functionParser = createFunctionParser();
        GenericRecordMetadata metadata = new GenericRecordMetadata();
        long fullGeohash = GeoHashNative.fromString(geohash);
        Assert.assertEquals(888340623145993896L, fullGeohash);
        for (int c = 1; c <= geohashLen; c++) {
            String expectedGeohash = geohash.substring(0, c);
            for (int b = 1; b <= c * 5 || c == 0; b++) {
                String castExpr = String.format("cast('%s' as geohash(%sb))", expectedGeohash, b);
                Function function = parseFunction(castExpr, metadata, functionParser);
                Assert.assertTrue(castExpr, function.isConstant());
                Assert.assertEquals(castExpr, b, GeoHashExtra.getBitsPrecision(function.getType()));
                Assert.assertEquals(castExpr, fullGeohash >>> (geohashLen * 5 - b), function.getLong(null));
            }
        }
    }
}