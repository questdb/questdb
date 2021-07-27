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
import io.questdb.std.str.StringSink;
import org.junit.Assert;
import org.junit.Test;

public class CastGeoHashFunctionFactoryTest extends BaseFunctionFactoryTest {

    // TODO: ExpressionParser to understand GEOHASH(8c), GEOHASH(40b)
    @Test
    public void testCastStringToGeoHash() throws SqlException {
        String expectedGeohash = "sp052w92";
        long expectedHash = 847187636514L;
        long expectedHashz = -9223371189667139294L;

        functions.add(new CastStrToGeoHashFunctionFactory());
        FunctionParser functionParser = createFunctionParser();
        GenericRecordMetadata metadata = new GenericRecordMetadata();
        Function function = parseFunction(
                String.format("cast('%s' as GEOHASH(8c))", expectedGeohash),
                metadata,
                functionParser);

        Assert.assertEquals(true, function.isConstant());
        Assert.assertNotEquals(ColumnType.GEOHASH, function.getType());
        Assert.assertEquals(GeoHashExtra.setBitsPrecision(ColumnType.GEOHASH, expectedGeohash.length() * 5), function.getType());
        Assert.assertEquals(expectedGeohash.length() * 5, GeoHashExtra.getBitsPrecision(function.getType()));

        Assert.assertEquals(expectedHashz, function.getGeoHash(null));
        Assert.assertEquals(expectedHashz, function.getLong(null));
        Assert.assertEquals(expectedHash, GeoHashNative.toHash(function.getLong(null)));
        Assert.assertEquals(expectedGeohash.length(), GeoHashNative.hashSize(function.getLong(null)));

        StringSink sink = new StringSink();
        GeoHashNative.toString(
                GeoHashNative.toHash(function.getLong(null)),
                GeoHashExtra.getBitsPrecision(function.getType()) / 5,
                sink);
        Assert.assertEquals(expectedGeohash, sink);
    }
}