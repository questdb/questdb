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

package io.questdb.test.std.json;

import io.questdb.std.json.*;
import io.questdb.std.str.DirectUtf8Sink;
import io.questdb.std.str.GcUtf8String;
import org.junit.*;

public class JsonTest {
    private static final String testUnicodeChars = "ðãµ¶Āڜ💩🦞";
    private static final String description = (
            "Hello, I'm John. I live in New York. I have a dog named Max and a cat named Whiskers. " +
            "This is a purposely long description so that it can stress the `maxLen` logic during string handling. " +
            "For good measure it also includes a few funky unicode characters: " +
            testUnicodeChars);
    public static final String jsonStr = "{\n" +
            "  \"name\": \"John\",\n" +
            "  \"age\": 30,\n" +
            "  \"city\": \"New York\",\n" +
            "  \"hasChildren\": false,\n" +
            "  \"height\": 5.6,\n" +
            "  \"nothing\": null,\n" +
            "  \"u64_val\": 18446744073709551615,\n" +
            "  \"bignum\": 12345678901234567890123456789012345678901234567890,\n" +
            "  \"pets\": [\n" +
            "    {\"name\": \"Max\", \"species\": \"Dog\"},\n" +
            "    {\"name\": \"Whiskers\", \"species\": \"Cat\", \"scratches\": true}\n" +
            "  ],\n" +
            "  \"description\": \"" + description + "\"\n" +
            "}";
    private static DirectUtf8Sink json;
    private static final JsonResult result = new JsonResult();

    @BeforeClass
    public static void setUp() {
        json = new DirectUtf8Sink(jsonStr.length() + Json.SIMDJSON_PADDING);
        json.put(jsonStr);
    }

    @AfterClass
    public static void tearDown() {
        json.close();
    }

    @Before
    public void before() {
        result.clear();
    }

    @Test
    public void testValidate() {
        Json.validate(json);
    }

    @Test
    public void testQueryPathString() {
        try (DirectUtf8Sink dest = new DirectUtf8Sink(100)) {
            Json.queryPathString(json, new GcUtf8String(".name"), result, dest, 100);
            Assert.assertEquals("John", dest.toString());

            GcUtf8String descriptionPath = new GcUtf8String(".description");
            dest.clear();
            Json.queryPathString(json, descriptionPath, result, dest, 100);
            Assert.assertEquals(description.substring(0, 100), dest.toString());

            // The maxLen == 272 chops one of the unicode characters and unless
            // the copy is handled with utf-8-aware logic it would produce a string
            // with an invalid utf-8 sequence.
            dest.clear();
            Json.queryPathString(json, descriptionPath, result, dest, 272);
            // The string is expected to be truncated at the last valid utf-8 sequence: 270 instead of 272.
            Assert.assertEquals(dest.size(), 270);

            // This ends up decoding just fine as UTF-8 and is shorter than the maxLen.
            Assert.assertEquals(description.substring(0, 262), dest.toString());
        }
    }

    @Test
    public void testQueryPathBoolean() {
        Assert.assertFalse(Json.queryPathBoolean(json, new GcUtf8String(".hasChildren"), result));
        Assert.assertTrue(Json.queryPathBoolean(json, new GcUtf8String(".pets[1].scratches"), result));
        Assert.assertEquals(result.getType(), JsonType.BOOLEAN);
    }

    @Test
    public void testQueryPathLong() {
        Assert.assertEquals(30, Json.queryPathLong(json, new GcUtf8String(".age"), result));
        Assert.assertEquals(result.getType(), JsonType.NUMBER);
        Assert.assertEquals(result.getNumType(), JsonNumType.SIGNED_INTEGER);
    }

    @Test
    public void testQueryPathDouble() {
        Assert.assertEquals(5.6, Json.queryPathDouble(json, new GcUtf8String(".height"), result), 0.0001);
        Assert.assertEquals(result.getType(), JsonType.NUMBER);
        Assert.assertEquals(result.getNumType(), JsonNumType.FLOATING_POINT_NUMBER);
    }

    @Test
    public void testInvalidPath() {
        try (DirectUtf8Sink dest = new DirectUtf8Sink(100)) {
            Json.queryPathString(json, new GcUtf8String("£$£%£%invalid path!!"), result, dest, 100);
            Assert.assertEquals(result.getError(), JsonError.INVALID_JSON_POINTER);
            Assert.assertEquals(result.getType(), JsonType.UNSET);
            Assert.assertEquals(result.getNumType(), JsonNumType.UNSET);
        }
    }

    @Test
    public void testStringAbsent() {
        try (DirectUtf8Sink dest = new DirectUtf8Sink(100)) {
            Json.queryPathString(json, new GcUtf8String(".nonexistent"), result, dest, 100);
            Assert.assertEquals("", dest.toString());
            Assert.assertEquals(result.getType(), JsonType.UNSET);
            Assert.assertEquals(result.getNumType(), JsonNumType.UNSET);
        }
    }

    @Test
    public void testLongAbsent() {
        final long res = Json.queryPathLong(json, new GcUtf8String(".nonexistent"), result);
        Assert.assertEquals(Long.MIN_VALUE, res);
        Assert.assertEquals(result.getType(), JsonType.UNSET);
        Assert.assertEquals(result.getNumType(), JsonNumType.UNSET);
    }

    @Test
    public void testBooleanAbsent() {
        final boolean res = Json.queryPathBoolean(json, new GcUtf8String(".nonexistent"), result);
        Assert.assertFalse(res);
        Assert.assertEquals(result.getType(), JsonType.UNSET);
        Assert.assertEquals(result.getNumType(), JsonNumType.UNSET);
    }

    @Test
    public void testDoubleAbsent() {
        final double res = Json.queryPathDouble(json, new GcUtf8String(".nonexistent"), result);
        Assert.assertTrue(Double.isNaN(res));
        Assert.assertEquals(result.getType(), JsonType.UNSET);
        Assert.assertEquals(result.getNumType(), JsonNumType.UNSET);
    }

    @Test
    public void testStringNull() {
        try (DirectUtf8Sink dest = new DirectUtf8Sink(100)) {
            Json.queryPathString(json, new GcUtf8String(".nothing"), result, dest, 100);
            Assert.assertEquals("", dest.toString());
            Assert.assertEquals(result.getType(), JsonType.NULL);
            Assert.assertEquals(result.getNumType(), JsonNumType.UNSET);
        }
    }

    @Test
    public void testLongNull() {
        final long res = Json.queryPathLong(json, new GcUtf8String(".nothing"), result);
        Assert.assertEquals(Long.MIN_VALUE, res);
        Assert.assertEquals(result.getType(), JsonType.NULL);
        Assert.assertEquals(result.getNumType(), JsonNumType.UNSET);
    }

    @Test
    public void testBooleanNull() {
        final boolean res = Json.queryPathBoolean(json, new GcUtf8String(".nothing"), result);
        Assert.assertFalse(res);
        Assert.assertEquals(result.getType(), JsonType.NULL);
        Assert.assertEquals(result.getNumType(), JsonNumType.UNSET);
    }

    @Test
    public void testDoubleNull() {
        final double res = Json.queryPathDouble(json, new GcUtf8String(".nothing"), result);
        Assert.assertTrue(Double.isNaN(res));
        Assert.assertEquals(result.getType(), JsonType.NULL);
        Assert.assertEquals(result.getNumType(), JsonNumType.UNSET);
    }

    @Test
    public void testQueryPathLongU64() {
        final long res = Json.queryPathLong(json, new GcUtf8String(".u64_val"), result);
        Assert.assertEquals(Long.MIN_VALUE, res);
        Assert.assertEquals(result.getError(), JsonError.INCORRECT_TYPE);
        Assert.assertEquals(result.getType(), JsonType.NUMBER);
        Assert.assertEquals(result.getNumType(), JsonNumType.UNSIGNED_INTEGER);
    }

    @Test
    public void testQueryPathLongBignum() {
        final long res = Json.queryPathLong(json, new GcUtf8String(".bignum"), result);
        Assert.assertEquals(Long.MIN_VALUE, res);
        Assert.assertEquals(result.getError(), JsonError.INCORRECT_TYPE);
        Assert.assertEquals(result.getType(), JsonType.NUMBER);
        Assert.assertEquals(result.getNumType(), JsonNumType.BIG_INTEGER);
    }
}
