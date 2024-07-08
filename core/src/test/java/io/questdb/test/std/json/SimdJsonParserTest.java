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

import io.questdb.log.LogFactory;
import io.questdb.std.json.*;
import io.questdb.std.str.DirectUtf8Sink;
import io.questdb.std.str.GcUtf8String;
import io.questdb.test.tools.TestUtils;
import org.junit.*;

import java.nio.charset.StandardCharsets;

public class SimdJsonParserTest {
    private static final SimdJsonResult result = new SimdJsonResult();
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
            "  \"hasPets\": true,\n" +
            "  \"height\": 5.6,\n" +
            "  \"nothing\": null,\n" +
            "  \"u64_val\": 18446744073709551614,\n" +
            "  \"bignum\": 12345678901234567890123456789012345678901234567890,\n" +
            "  \"pets\": [\n" +
            "    {\"name\": \"Max\", \"species\": \"Dog\"},\n" +
            "    {\"name\": \"Whiskers\", \"species\": \"Cat\", \"scratches\": true}\n" +
            "  ],\n" +
            "  \"description\": \"" + description + "\"\n" +
            "}";
    private static DirectUtf8Sink json;
    private static SimdJsonParser parser;

    @BeforeClass
    public static void setUp() {
        json = new DirectUtf8Sink(jsonStr.getBytes(StandardCharsets.UTF_8).length + SimdJsonParser.SIMDJSON_PADDING);
        json.put(jsonStr);
        parser = new SimdJsonParser();
    }

    @AfterClass
    public static void tearDown() {
        json.close();
        parser.close();
    }

    @Test
    public void testBooleanAbsent() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final boolean res = parser.queryPointerBoolean(json, path2Pointer(".nonexistent"), result);
            Assert.assertFalse(res);
            Assert.assertEquals(result.getType(), SimdJsonType.UNSET);
        });
    }

    @Test
    public void testBooleanNull() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final boolean res = parser.queryPointerBoolean(json, path2Pointer(".nothing"), result);
            Assert.assertFalse(res);
            Assert.assertEquals(result.getError(), SimdJsonError.INCORRECT_TYPE);
            Assert.assertEquals(result.getType(), SimdJsonType.NULL);
        });
    }

    @Test
    public void testConstructDestruct() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            SimdJsonParser parser = new SimdJsonParser();
            parser.close();
        });
    }

    @Test
    public void testConvertPathToPointer() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (DirectUtf8Sink dest = new DirectUtf8Sink(100)) {
                SimdJsonParser.convertJsonPathToPointer(new GcUtf8String(".name[0]"), dest);
                Assert.assertEquals("/name/0", dest.toString());
            }
        });
    }

    @Test
    public void testDoubleAbsent() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final double res = parser.queryPointerDouble(json, path2Pointer(".nonexistent"), result);
            Assert.assertTrue(Double.isNaN(res));
            Assert.assertEquals(result.getType(), SimdJsonType.UNSET);
        });
    }

    @Test
    public void testDoubleNull() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final double res = parser.queryPointerDouble(json, path2Pointer(".nothing"), result);
            Assert.assertTrue(Double.isNaN(res));
            Assert.assertEquals(result.getType(), SimdJsonType.NULL);
        });
    }

    @Test
    public void testInvalidPath() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (DirectUtf8Sink dest = new DirectUtf8Sink(100)) {
                parser.queryPointerUtf8(json, path2Pointer("£$£%£%invalid path!!"), result, dest, 100);
                Assert.assertEquals(result.getError(), SimdJsonError.INVALID_JSON_POINTER);
                Assert.assertEquals(result.getType(), SimdJsonType.UNSET);
            }
        });
    }

    @Test
    public void testLongAbsent() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final long res = parser.queryPointerLong(json, path2Pointer(".nonexistent"), result);
            Assert.assertEquals(Long.MIN_VALUE, res);
            Assert.assertEquals(result.getType(), SimdJsonType.UNSET);
        });
    }

    @Test
    public void testLongNull() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final long res = parser.queryPointerLong(json, path2Pointer(".nothing"), result);
            Assert.assertEquals(Long.MIN_VALUE, res);
            Assert.assertEquals(result.getType(), SimdJsonType.NULL);
        });
    }

    @Test
    public void testQueryPathBoolean() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            Assert.assertFalse(parser.queryPointerBoolean(json, path2Pointer(".hasChildren"), result));
            Assert.assertTrue(parser.queryPointerBoolean(json, path2Pointer(".pets[1].scratches"), result));
            Assert.assertEquals(result.getType(), SimdJsonType.BOOLEAN);
        });
    }

    @Test
    public void testQueryPathDouble() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            Assert.assertEquals(5.6, parser.queryPointerDouble(json, path2Pointer(".height"), result), 0.0001);
            Assert.assertEquals(result.getType(), SimdJsonType.NUMBER);
        });
    }

    @Test
    public void testQueryPathLong() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            Assert.assertEquals(30, parser.queryPointerLong(json, path2Pointer(".age"), result));
            Assert.assertEquals(result.getType(), SimdJsonType.NUMBER);
        });
    }

    @Test
    public void testQueryPathLongBignum() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final long res = parser.queryPointerLong(json, path2Pointer(".bignum"), result);
            Assert.assertEquals(Long.MIN_VALUE, res);
            Assert.assertEquals(result.getError(), SimdJsonError.NUMBER_OUT_OF_RANGE);
            Assert.assertEquals(result.getType(), SimdJsonType.NUMBER);
        });
    }

    @Test
    public void testQueryPathLongU64() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final long res = parser.queryPointerLong(json, path2Pointer(".u64_val"), result);
            Assert.assertEquals(Long.MIN_VALUE, res);
            Assert.assertEquals(result.getError(), SimdJsonError.NUMBER_OUT_OF_RANGE);
            Assert.assertEquals(result.getType(), SimdJsonType.NUMBER);
        });
    }

    @Test
    public void testQueryPathUtf8() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (DirectUtf8Sink dest = new DirectUtf8Sink(1000)) {
                parser.queryPointerUtf8(json, path2Pointer(".name"), result, dest, 100);
                Assert.assertEquals("John", dest.toString());

                GcUtf8String descriptionPath = path2Pointer(".description");
                dest.clear();
                parser.queryPointerUtf8(json, descriptionPath, result, dest, 100);
                Assert.assertEquals(description.substring(0, 100), dest.toString());

                // The maxLen == 272 chops one of the unicode characters and unless
                // the copy is handled with utf-8-aware logic it would produce a string
                // with an invalid utf-8 sequence.
                dest.clear();
                parser.queryPointerUtf8(json, descriptionPath, result, dest, 272);
                // The string is expected to be truncated at the last valid utf-8 sequence: 270 instead of 272.
                Assert.assertEquals(dest.size(), 270);

                // This ends up decoding just fine as UTF-8 and is shorter than the maxLen.
                Assert.assertEquals(description.substring(0, 262), dest.toString());
            }
        });
    }

    @Test
    public void testQueryPointerLong() throws Exception {
        TestUtils.assertMemoryLeak(() -> Assert.assertEquals(30, parser.queryPointerLong(json, new GcUtf8String("/age"), result)));
    }

    @Test
    public void testQueryValue() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final int cap = 1000;
            try (DirectUtf8Sink dest = new DirectUtf8Sink(cap)) {
                final long ret1 = parser.queryPointerValue(json, path2Pointer(".name"), result, dest, cap);
                Assert.assertEquals(0, ret1);
                Assert.assertEquals(result.getError(), SimdJsonError.SUCCESS);
                Assert.assertEquals(result.getType(), SimdJsonType.STRING);
                Assert.assertEquals(result.getNumberType(), SimdJsonNumberType.UNSET);
                Assert.assertEquals("John", dest.toString());

                dest.clear();

                final long ret2 = parser.queryPointerValue(json, path2Pointer(".age"), result, dest, cap);
                Assert.assertEquals(30, ret2);
                Assert.assertEquals(result.getError(), SimdJsonError.SUCCESS);
                Assert.assertEquals(result.getType(), SimdJsonType.NUMBER);
                Assert.assertEquals(result.getNumberType(), SimdJsonNumberType.SIGNED_INTEGER);
                Assert.assertEquals(0, dest.size());

                final long ret3 = parser.queryPointerValue(json, path2Pointer(".height"), result, dest, cap);
                final double ret3Double = Double.longBitsToDouble(ret3);
                Assert.assertEquals(5.6, ret3Double, 0.000001);
                Assert.assertEquals(result.getError(), SimdJsonError.SUCCESS);
                Assert.assertEquals(result.getType(), SimdJsonType.NUMBER);
                Assert.assertEquals(result.getNumberType(), SimdJsonNumberType.FLOATING_POINT_NUMBER);
                Assert.assertEquals(0, dest.size());

                final long ret4 = parser.queryPointerValue(json, path2Pointer(".hasChildren"), result, dest, cap);
                Assert.assertEquals(0, ret4);
                Assert.assertEquals(result.getError(), SimdJsonError.SUCCESS);
                Assert.assertEquals(result.getType(), SimdJsonType.BOOLEAN);
                Assert.assertEquals(result.getNumberType(), SimdJsonNumberType.UNSET);
                Assert.assertEquals(0, dest.size());

                final long ret5 = parser.queryPointerValue(json, path2Pointer(".hasPets"), result, dest, cap);
                Assert.assertEquals(1, ret5);
                Assert.assertEquals(result.getError(), SimdJsonError.SUCCESS);
                Assert.assertEquals(result.getType(), SimdJsonType.BOOLEAN);
                Assert.assertEquals(result.getNumberType(), SimdJsonNumberType.UNSET);
                Assert.assertEquals(0, dest.size());

                final long ret6 = parser.queryPointerValue(json, path2Pointer(".nothing"), result, dest, cap);
                Assert.assertEquals(0, ret6);
                Assert.assertEquals(result.getError(), SimdJsonError.SUCCESS);
                Assert.assertEquals(result.getType(), SimdJsonType.NULL);
                Assert.assertEquals(result.getNumberType(), SimdJsonNumberType.UNSET);
                Assert.assertEquals(0, dest.size());

                final long ret7 = parser.queryPointerValue(json, path2Pointer(".u64_val"), result, dest, cap);
                Assert.assertEquals(-2L, ret7);
                Assert.assertEquals(result.getError(), SimdJsonError.SUCCESS);
                Assert.assertEquals(result.getType(), SimdJsonType.NUMBER);
                Assert.assertEquals(result.getNumberType(), SimdJsonNumberType.UNSIGNED_INTEGER);
                Assert.assertEquals(0, dest.size());

                final long ret8 = parser.queryPointerValue(json, path2Pointer(".bignum"), result, dest, cap);
                Assert.assertEquals(0, ret8);
                Assert.assertEquals(result.getError(), SimdJsonError.SUCCESS);
                Assert.assertEquals(result.getType(), SimdJsonType.NUMBER);
                Assert.assertEquals(result.getNumberType(), SimdJsonNumberType.BIG_INTEGER);
                Assert.assertEquals("12345678901234567890123456789012345678901234567890", dest.toString());
                dest.clear();

                final long ret9 = parser.queryPointerValue(json, path2Pointer(".pets"), result, dest, cap);
                Assert.assertEquals(0, ret9);
                Assert.assertEquals(result.getError(), SimdJsonError.SUCCESS);
                Assert.assertEquals(result.getType(), SimdJsonType.ARRAY);
                Assert.assertEquals(result.getNumberType(), SimdJsonNumberType.UNSET);
                Assert.assertEquals("[\n" +
                        "    {\"name\": \"Max\", \"species\": \"Dog\"},\n" +
                        "    {\"name\": \"Whiskers\", \"species\": \"Cat\", \"scratches\": true}\n" +
                        "  ]", dest.toString());
                dest.clear();

                final long ret10 = parser.queryPointerValue(json, path2Pointer(".pets[0]"), result, dest, cap);
                Assert.assertEquals(0, ret10);
                Assert.assertEquals(result.getError(), SimdJsonError.SUCCESS);
                Assert.assertEquals(result.getType(), SimdJsonType.OBJECT);
                Assert.assertEquals(result.getNumberType(), SimdJsonNumberType.UNSET);
                Assert.assertEquals("{\"name\": \"Max\", \"species\": \"Dog\"}", dest.toString());
                dest.clear();

                final long ret11 = parser.queryPointerValue(json, path2Pointer(".nonexistent"), result, dest, cap);
                Assert.assertEquals(0, ret11);
                Assert.assertEquals(result.getError(), SimdJsonError.NO_SUCH_FIELD);
                Assert.assertEquals(result.getType(), SimdJsonType.UNSET);
                Assert.assertEquals(result.getNumberType(), SimdJsonNumberType.UNSET);
                Assert.assertEquals(0, dest.size());
            }
        });
    }

    @Test
    public void testStringAbsent() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (DirectUtf8Sink dest = new DirectUtf8Sink(100)) {
                parser.queryPointerUtf8(json, path2Pointer(".nonexistent"), result, dest, 100);
                Assert.assertEquals("", dest.toString());
                Assert.assertEquals(result.getType(), SimdJsonType.UNSET);
            }
        });
    }

    @Test
    public void testStringNull() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (DirectUtf8Sink dest = new DirectUtf8Sink(100)) {
                parser.queryPointerUtf8(json, path2Pointer(".nothing"), result, dest, 100);
                Assert.assertEquals("", dest.toString());
                Assert.assertEquals(result.getType(), SimdJsonType.NULL);
            }
        });
    }

    private static GcUtf8String path2Pointer(String path) {
        try (DirectUtf8Sink dest = new DirectUtf8Sink(100)) {
            SimdJsonParser.convertJsonPathToPointer(new GcUtf8String(path), dest);
            return new GcUtf8String(dest.toString());
        }
    }

    static {
        // log is needed to greedily allocate logger infra and
        // exclude it from leak detector
        LogFactory.getLog(SimdJsonParserTest.class);
    }
}
