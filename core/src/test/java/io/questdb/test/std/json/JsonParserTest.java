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

public class JsonParserTest {
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
    private static JsonParser parser;
    private static final JsonResult result = new JsonResult();

    @BeforeClass
    public static void setUp() {
        json = new DirectUtf8Sink(jsonStr.getBytes(StandardCharsets.UTF_8).length + JsonParser.SIMDJSON_PADDING);
        json.put(jsonStr);
        parser = new JsonParser();
    }

    @AfterClass
    public static void tearDown() {
        json.close();
        parser.close();
    }

    @Before
    public void before() {
        result.clear();
    }

    @Test
    public void testConstructDestruct() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            JsonParser parser = new JsonParser();
            parser.close();
        });
    }

    @Test
    public void testQueryPathString() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (DirectUtf8Sink dest = new DirectUtf8Sink(1000)) {
                parser.queryPathString(json, new GcUtf8String(".name"), result, dest, 100);
                Assert.assertEquals("John", dest.toString());

                GcUtf8String descriptionPath = new GcUtf8String(".description");
                dest.clear();
                parser.queryPathString(json, descriptionPath, result, dest, 100);
                Assert.assertEquals(description.substring(0, 100), dest.toString());

                // The maxLen == 272 chops one of the unicode characters and unless
                // the copy is handled with utf-8-aware logic it would produce a string
                // with an invalid utf-8 sequence.
                dest.clear();
                parser.queryPathString(json, descriptionPath, result, dest, 272);
                // The string is expected to be truncated at the last valid utf-8 sequence: 270 instead of 272.
                Assert.assertEquals(dest.size(), 270);

                // This ends up decoding just fine as UTF-8 and is shorter than the maxLen.
                Assert.assertEquals(description.substring(0, 262), dest.toString());
            }
        });
    }

    @Test
    public void testQueryPathBoolean() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            Assert.assertFalse(parser.queryPathBoolean(json, new GcUtf8String(".hasChildren"), result));
            Assert.assertTrue(parser.queryPathBoolean(json, new GcUtf8String(".pets[1].scratches"), result));
            Assert.assertEquals(result.getType(), JsonType.BOOLEAN);
        });
    }

    @Test
    public void testQueryPathLong() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            Assert.assertEquals(30, parser.queryPathLong(json, new GcUtf8String(".age"), result));
            Assert.assertEquals(result.getType(), JsonType.NUMBER);
        });
    }

    @Test
    public void testConvertPathToPointer() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (DirectUtf8Sink dest = new DirectUtf8Sink(100)) {
                JsonParser.convertJsonPathToPointer(new GcUtf8String(".name[0]"), dest);
                Assert.assertEquals("/name/0", dest.toString());
            }
        });
    }

    @Test
    public void testQueryPointerLong() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            Assert.assertEquals(30, parser.queryPointerLong(json, new GcUtf8String("/age"), result));
        });
    }

    @Test
    public void testQueryPathDouble() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            Assert.assertEquals(5.6, parser.queryPathDouble(json, new GcUtf8String(".height"), result), 0.0001);
            Assert.assertEquals(result.getType(), JsonType.NUMBER);
        });
    }

    @Test
    public void testInvalidPath() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (DirectUtf8Sink dest = new DirectUtf8Sink(100)) {
                parser.queryPathString(json, new GcUtf8String("£$£%£%invalid path!!"), result, dest, 100);
                Assert.assertEquals(result.getError(), JsonError.INVALID_JSON_POINTER);
                Assert.assertEquals(result.getType(), JsonType.UNSET);
            }
        });
    }

    @Test
    public void testStringAbsent() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (DirectUtf8Sink dest = new DirectUtf8Sink(100)) {
                parser.queryPathString(json, new GcUtf8String(".nonexistent"), result, dest, 100);
                Assert.assertEquals("", dest.toString());
                Assert.assertEquals(result.getType(), JsonType.UNSET);
            }
        });
    }

    @Test
    public void testLongAbsent() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final long res = parser.queryPathLong(json, new GcUtf8String(".nonexistent"), result);
            Assert.assertEquals(Long.MIN_VALUE, res);
            Assert.assertEquals(result.getType(), JsonType.UNSET);
        });
    }

    @Test
    public void testBooleanAbsent() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final boolean res = parser.queryPathBoolean(json, new GcUtf8String(".nonexistent"), result);
            Assert.assertFalse(res);
            Assert.assertEquals(result.getType(), JsonType.UNSET);
        });
    }

    @Test
    public void testDoubleAbsent() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final double res = parser.queryPathDouble(json, new GcUtf8String(".nonexistent"), result);
            Assert.assertTrue(Double.isNaN(res));
            Assert.assertEquals(result.getType(), JsonType.UNSET);
        });
    }

    @Test
    public void testStringNull() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (DirectUtf8Sink dest = new DirectUtf8Sink(100)) {
                parser.queryPathString(json, new GcUtf8String(".nothing"), result, dest, 100);
                Assert.assertEquals("", dest.toString());
                Assert.assertEquals(result.getType(), JsonType.NULL);
            }
        });
    }

    @Test
    public void testLongNull() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final long res = parser.queryPathLong(json, new GcUtf8String(".nothing"), result);
            Assert.assertEquals(Long.MIN_VALUE, res);
            Assert.assertEquals(result.getType(), JsonType.NULL);
        });
    }

    @Test
    public void testBooleanNull() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final boolean res = parser.queryPathBoolean(json, new GcUtf8String(".nothing"), result);
            Assert.assertFalse(res);
            Assert.assertEquals(result.getType(), JsonType.NULL);
        });
    }

    @Test
    public void testDoubleNull() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final double res = parser.queryPathDouble(json, new GcUtf8String(".nothing"), result);
            Assert.assertTrue(Double.isNaN(res));
            Assert.assertEquals(result.getType(), JsonType.NULL);
        });
    }

    @Test
    public void testQueryPathLongU64() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final long res = parser.queryPathLong(json, new GcUtf8String(".u64_val"), result);
            Assert.assertEquals(Long.MIN_VALUE, res);
            Assert.assertEquals(result.getError(), JsonError.INCORRECT_TYPE);
            Assert.assertEquals(result.getType(), JsonType.NUMBER);
        });
    }

    @Test
    public void testQueryPathLongBignum() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final long res = parser.queryPathLong(json, new GcUtf8String(".bignum"), result);
            Assert.assertEquals(Long.MIN_VALUE, res);
            Assert.assertEquals(result.getError(), JsonError.INCORRECT_TYPE);
            Assert.assertEquals(result.getType(), JsonType.NUMBER);
        });
    }

    static {
        // log is needed to greedily allocate logger infra and
        // exclude it from leak detector
        LogFactory.getLog(JsonParserTest.class);
    }
}
