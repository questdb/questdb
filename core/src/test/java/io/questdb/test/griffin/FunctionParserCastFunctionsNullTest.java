/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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
import io.questdb.cairo.GenericRecordMetadata;
import io.questdb.cairo.GeoHashes;
import io.questdb.cairo.sql.Function;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.FunctionParser;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.engine.functions.cast.*;
import io.questdb.std.CharSequenceIntHashMap;
import io.questdb.std.ObjList;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;

public class FunctionParserCastFunctionsNullTest extends BaseFunctionFactoryTest {

    private static final FunctionFactory[] CAST_FUNCS = {
            new CastBooleanToBooleanFunctionFactory(),
            new CastByteToByteFunctionFactory(),
            new CastShortToShortFunctionFactory(),
            new CastIntToIntFunctionFactory(),
            new CastLongToLongFunctionFactory(),
            new CastDateToDateFunctionFactory(),
            new CastTimestampToTimestampFunctionFactory(),
            new CastFloatToFloatFunctionFactory(),
            new CastDoubleToDoubleFunctionFactory(),
            new CastStrToStrFunctionFactory(),
            new CastStrToBooleanFunctionFactory(),
            new CastSymbolToSymbolFunctionFactory(),
            new CastLong256ToLong256FunctionFactory(),
            new CastStrToGeoHashFunctionFactory(),
            new CastGeoHashToGeoHashFunctionFactory(),
            new CastNullTypeFunctionFactory(),
    };
    private static final CharSequenceIntHashMap typeNameToId = new CharSequenceIntHashMap();
    private static final ObjList<CharSequence> typeNames = typeNameToId.keys();
    private FunctionParser functionParser;
    private GenericRecordMetadata metadata;

    @Before
    public void setUp5() {
        functions.addAll(Arrays.asList(CAST_FUNCS));
        Collections.shuffle(functions);
        functionParser = createFunctionParser();
        metadata = new GenericRecordMetadata();
    }

    @Test
    public void testCastFalseStringAsBoolean() throws SqlException {
        Function function = parseFunction("cast('FalSE' as BOOLEAN)", metadata, functionParser);
        Assert.assertEquals(ColumnType.BOOLEAN, function.getType());
        Assert.assertTrue(function.isConstant());
        Assert.assertFalse(function.getBool(null));
    }

    @Test
    public void testCastNull() throws SqlException {
        for (int i = 0; i < typeNames.size(); i++) {
            CharSequence type = typeNames.getQuick(i);
            Function function = parseFunction(String.format("cast(null as %s)", type), metadata, functionParser);
            Assert.assertEquals(typeNameToId.get(type), function.getType());
            Assert.assertTrue(function.isConstant());
        }
    }

    @Test
    public void testCastNullGeoByteBits() throws SqlException {
        Function function = parseFunction("cast(null as GeOhAsH(7b))", metadata, functionParser);
        Assert.assertTrue(function.isConstant());
        Assert.assertEquals(ColumnType.getGeoHashTypeWithBits(7), function.getType());
        Assert.assertEquals(GeoHashes.BYTE_NULL, function.getGeoByte(null));
    }

    @Test
    public void testCastNullGeoByteChars() throws SqlException {
        Function function = parseFunction("cast(null as GeOhAsH(1c))", metadata, functionParser);
        Assert.assertTrue(function.isConstant());
        Assert.assertEquals(ColumnType.getGeoHashTypeWithBits(5), function.getType());
        Assert.assertEquals(GeoHashes.BYTE_NULL, function.getGeoByte(null));
    }

    @Test
    public void testCastNullGeoHash3() throws SqlException {
        Function function = parseFunction("cast('' as GeOhAsH(60b))", metadata, functionParser);
        Assert.assertTrue(function.isConstant());
        Assert.assertEquals(ColumnType.getGeoHashTypeWithBits(60), function.getType());
        Assert.assertEquals(GeoHashes.NULL, function.getGeoLong(null));
    }

    @Test
    public void testCastNullGeoHashMissingSize1() throws Exception {
        assertFailure("cast(null as geohash())",
                null,
                21,
                "invalid GEOHASH, invalid type precision");
    }

    @Test
    public void testCastNullGeoHashMissingSize2() throws Exception {
        assertFailure("cast(null as GEOHASH)",
                null,
                13,
                "unsupported cast");
    }

    @Test
    public void testCastNullGeoHashMissingSize3() throws Exception {
        assertFailure("cast(null as GEOHASH(21b)",
                null,
                4,
                "unbalanced (");
    }

    @Test
    public void testCastNullGeoHashMissingSize4() throws Exception {
        assertFailure("cast(null as GEOHASH(21 b))",
                null,
                24,
                "invalid GEOHASH, missing ')'");
    }

    @Test
    public void testCastNullGeoHashMissingSize5() throws Exception {
        assertFailure("cast(null as GEOHASH(c))",
                null,
                13,
                "invalid GEOHASH size, must be number followed by 'C' or 'B' character");
    }

    @Test
    public void testCastNullGeoIntChars() throws SqlException {
        Function function = parseFunction("cast(null as GeOhAsH(6c))", metadata, functionParser);
        Assert.assertTrue(function.isConstant());
        Assert.assertEquals(ColumnType.getGeoHashTypeWithBits(30), function.getType());
        Assert.assertEquals(GeoHashes.INT_NULL, function.getGeoInt(null));
    }

    @Test
    public void testCastNullGeoLongBits() throws SqlException {
        Function function = parseFunction("cast(null as GeOhAsH(60b))", metadata, functionParser);
        Assert.assertTrue(function.isConstant());
        Assert.assertEquals(ColumnType.getGeoHashTypeWithBits(60), function.getType());
        Assert.assertEquals(GeoHashes.NULL, function.getGeoLong(null));
    }

    @Test
    public void testCastNullGeoLongChars() throws SqlException {
        Function function = parseFunction("cast(null as GeOhAsH(12c))", metadata, functionParser);
        Assert.assertTrue(function.isConstant());
        Assert.assertEquals(ColumnType.getGeoHashTypeWithBits(60), function.getType());
        Assert.assertEquals(GeoHashes.NULL, function.getGeoLong(null));
    }

    @Test
    public void testCastNullGeoShortBits() throws SqlException {
        Function function = parseFunction("cast(null as GeOhAsH(8b))", metadata, functionParser);
        Assert.assertTrue(function.isConstant());
        Assert.assertEquals(ColumnType.getGeoHashTypeWithBits(8), function.getType());
        Assert.assertEquals(GeoHashes.NULL, function.getGeoShort(null));
    }

    @Test
    public void testCastNullGeoShortChars() throws SqlException {
        Function function = parseFunction("cast(null as GeOhAsH(3c))", metadata, functionParser);
        Assert.assertTrue(function.isConstant());
        Assert.assertEquals(ColumnType.getGeoHashTypeWithBits(15), function.getType());
        Assert.assertEquals(GeoHashes.SHORT_NULL, function.getGeoShort(null));
    }

    @Test
    public void testCastNullStringAsBoolean() throws SqlException {
        Function function = parseFunction("cast('' as BOOLEAN)", metadata, functionParser);
        Assert.assertEquals(ColumnType.BOOLEAN, function.getType());
        Assert.assertTrue(function.isConstant());
        Assert.assertFalse(function.getBool(null));
    }

    @Test
    public void testCastStringAsBoolean() throws SqlException {
        Function function = parseFunction("cast('bongos' as BOOLEAN)", metadata, functionParser);
        Assert.assertEquals(ColumnType.BOOLEAN, function.getType());
        Assert.assertTrue(function.isConstant());
        Assert.assertFalse(function.getBool(null));
    }

    @Test
    public void testCastTrueStringAsBoolean() throws SqlException {
        Function function = parseFunction("cast('tRuE' as BOOLEAN)", metadata, functionParser);
        Assert.assertEquals(ColumnType.BOOLEAN, function.getType());
        Assert.assertTrue(function.isConstant());
        Assert.assertTrue(function.getBool(null));
    }

    static {
        typeNameToId.put("boolean", ColumnType.BOOLEAN);
        typeNameToId.put("byte", ColumnType.BYTE);
        typeNameToId.put("short", ColumnType.SHORT);
        typeNameToId.put("char", ColumnType.CHAR);
        typeNameToId.put("int", ColumnType.INT);
        typeNameToId.put("long", ColumnType.LONG);
        typeNameToId.put("date", ColumnType.DATE);
        typeNameToId.put("timestamp", ColumnType.TIMESTAMP);
        typeNameToId.put("float", ColumnType.FLOAT);
        typeNameToId.put("double", ColumnType.DOUBLE);
        typeNameToId.put("string", ColumnType.STRING);
        typeNameToId.put("symbol", ColumnType.SYMBOL);
        typeNameToId.put("long256", ColumnType.LONG256);
        typeNameToId.put("binary", ColumnType.BINARY);
    }
}
