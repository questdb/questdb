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

package io.questdb.test.std.json;

import io.questdb.log.LogFactory;
import io.questdb.std.json.SimdJsonError;
import io.questdb.std.json.SimdJsonNumberType;
import io.questdb.std.json.SimdJsonParser;
import io.questdb.std.json.SimdJsonResult;
import io.questdb.std.json.SimdJsonType;
import io.questdb.std.str.DirectUtf8Sequence;
import io.questdb.std.str.DirectUtf8Sink;
import io.questdb.std.str.GcUtf8String;
import io.questdb.test.tools.TestUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.Objects;

public class SimdJsonParserTest {
    private static final GcUtf8String ELEM_0_POINTER = new GcUtf8String("/0");
    private static final GcUtf8String ROOT_POINTER = new GcUtf8String("");
    // N.B.: Compare these scenarios with those from `JsonExtractCastScenariosTest`.
    // These tests are run twice:
    //   * (1) Once as-is (to check that scalars are extracted correctly).
    //       * E.g. Scenario "null" would call `parser.queryPointerLong("null", "", result)`.
    //       * Note that in this case, the extraction json path is to extract the root of the document.
    //   * (2) - Once with the `json` field string wrapped inside a list.
    //       * E.g. Scenario "null" would call `parser.queryPointerLong("[null]", "[0]", result)`.
    //       * Note that in this case, the extraction json path is to extract the first element of the list.
    private static final Scenario[] SCENARIOS = new Scenario[]{
            new Scenario(
                    "null",
                    new Value<>(
                            SimdJsonError.SUCCESS,
                            SimdJsonType.NULL,
                            SimdJsonNumberType.UNSET,
                            false
                    ),
                    new Value<>(
                            SimdJsonError.SUCCESS,
                            SimdJsonType.NULL,
                            SimdJsonNumberType.UNSET,
                            (short) 0
                    ),
                    new Value<>(
                            SimdJsonError.SUCCESS,
                            SimdJsonType.NULL,
                            SimdJsonNumberType.UNSET,
                            Integer.MIN_VALUE
                    ),
                    new Value<>(
                            SimdJsonError.SUCCESS,
                            SimdJsonType.NULL,
                            SimdJsonNumberType.UNSET,
                            Long.MIN_VALUE
                    ),
                    new Value<>(
                            SimdJsonError.SUCCESS,
                            SimdJsonType.NULL,
                            SimdJsonNumberType.UNSET,
                            Double.NaN
                    ),
                    new Value<>(
                            SimdJsonError.SUCCESS,
                            SimdJsonType.NULL,
                            SimdJsonNumberType.UNSET,
                            ""
                    ),
                    new Value<>(
                            SimdJsonError.SUCCESS,
                            SimdJsonType.NULL,
                            SimdJsonNumberType.UNSET,
                            0L,
                            ""
                    )
            ),
            new Scenario(
                    "true",
                    new Value<>(
                            SimdJsonError.SUCCESS,
                            SimdJsonType.BOOLEAN,
                            SimdJsonNumberType.UNSET,
                            true
                    ),
                    new Value<>(
                            SimdJsonError.SUCCESS,
                            SimdJsonType.BOOLEAN,
                            SimdJsonNumberType.UNSET,
                            (short) 1
                    ),
                    new Value<>(
                            SimdJsonError.SUCCESS,
                            SimdJsonType.BOOLEAN,
                            SimdJsonNumberType.UNSET,
                            1
                    ),
                    new Value<>(
                            SimdJsonError.SUCCESS,
                            SimdJsonType.BOOLEAN,
                            SimdJsonNumberType.UNSET,
                            1L
                    ),
                    new Value<>(
                            SimdJsonError.SUCCESS,
                            SimdJsonType.BOOLEAN,
                            SimdJsonNumberType.UNSET,
                            1.0
                    ),
                    new Value<>(
                            SimdJsonError.SUCCESS,
                            SimdJsonType.BOOLEAN,
                            SimdJsonNumberType.UNSET,
                            "true"
                    ),
                    new Value<>(
                            SimdJsonError.SUCCESS,
                            SimdJsonType.BOOLEAN,
                            SimdJsonNumberType.UNSET,
                            1L,
                            ""
                    )
            ),
            new Scenario(
                    "false",
                    new Value<>(
                            SimdJsonError.SUCCESS,
                            SimdJsonType.BOOLEAN,
                            SimdJsonNumberType.UNSET,
                            false
                    ),
                    new Value<>(
                            SimdJsonError.SUCCESS,
                            SimdJsonType.BOOLEAN,
                            SimdJsonNumberType.UNSET,
                            (short) 0
                    ),
                    new Value<>(
                            SimdJsonError.SUCCESS,
                            SimdJsonType.BOOLEAN,
                            SimdJsonNumberType.UNSET,
                            0
                    ),
                    new Value<>(
                            SimdJsonError.SUCCESS,
                            SimdJsonType.BOOLEAN,
                            SimdJsonNumberType.UNSET,
                            0L
                    ),
                    new Value<>(
                            SimdJsonError.SUCCESS,
                            SimdJsonType.BOOLEAN,
                            SimdJsonNumberType.UNSET,
                            0.0
                    ),
                    new Value<>(
                            SimdJsonError.SUCCESS,
                            SimdJsonType.BOOLEAN,
                            SimdJsonNumberType.UNSET,
                            "false"
                    ),
                    new Value<>(
                            SimdJsonError.SUCCESS,
                            SimdJsonType.BOOLEAN,
                            SimdJsonNumberType.UNSET,
                            0L,
                            ""
                    )
            ),
            new Scenario(
                    "1",
                    new Value<>(
                            SimdJsonError.SUCCESS,
                            SimdJsonType.NUMBER,
                            SimdJsonNumberType.SIGNED_INTEGER,
                            false
                    ),
                    new Value<>(
                            SimdJsonError.SUCCESS,
                            SimdJsonType.NUMBER,
                            SimdJsonNumberType.SIGNED_INTEGER,
                            (short) 1
                    ),
                    new Value<>(
                            SimdJsonError.SUCCESS,
                            SimdJsonType.NUMBER,
                            SimdJsonNumberType.SIGNED_INTEGER,
                            1
                    ),
                    new Value<>(
                            SimdJsonError.SUCCESS,
                            SimdJsonType.NUMBER,
                            SimdJsonNumberType.SIGNED_INTEGER,
                            1L
                    ),
                    new Value<>(
                            SimdJsonError.SUCCESS,
                            SimdJsonType.NUMBER,
                            SimdJsonNumberType.FLOATING_POINT_NUMBER,
                            1.0
                    ),
                    new Value<>(
                            SimdJsonError.SUCCESS,
                            SimdJsonType.NUMBER,
                            SimdJsonNumberType.SIGNED_INTEGER,
                            "1"
                    ),
                    new Value<>(
                            SimdJsonError.SUCCESS,
                            SimdJsonType.NUMBER,
                            SimdJsonNumberType.SIGNED_INTEGER,
                            1L,
                            ""
                    )
            ),
            new Scenario(
                    "0",
                    new Value<>(
                            SimdJsonError.SUCCESS,
                            SimdJsonType.NUMBER,
                            SimdJsonNumberType.SIGNED_INTEGER,
                            false
                    ),
                    new Value<>(
                            SimdJsonError.SUCCESS,
                            SimdJsonType.NUMBER,
                            SimdJsonNumberType.SIGNED_INTEGER,
                            (short) 0
                    ),
                    new Value<>(
                            SimdJsonError.SUCCESS,
                            SimdJsonType.NUMBER,
                            SimdJsonNumberType.SIGNED_INTEGER,
                            0
                    ),
                    new Value<>(
                            SimdJsonError.SUCCESS,
                            SimdJsonType.NUMBER,
                            SimdJsonNumberType.SIGNED_INTEGER,
                            0L
                    ),
                    new Value<>(
                            SimdJsonError.SUCCESS,
                            SimdJsonType.NUMBER,
                            SimdJsonNumberType.FLOATING_POINT_NUMBER,
                            0.0
                    ),
                    new Value<>(
                            SimdJsonError.SUCCESS,
                            SimdJsonType.NUMBER,
                            SimdJsonNumberType.SIGNED_INTEGER,
                            "0"
                    ),
                    new Value<>(
                            SimdJsonError.SUCCESS,
                            SimdJsonType.NUMBER,
                            SimdJsonNumberType.SIGNED_INTEGER,
                            0L,
                            ""
                    )
            ),
            new Scenario(
                    "-1",
                    new Value<>(
                            SimdJsonError.SUCCESS,
                            SimdJsonType.NUMBER,
                            SimdJsonNumberType.SIGNED_INTEGER,
                            false
                    ),
                    new Value<>(
                            SimdJsonError.SUCCESS,
                            SimdJsonType.NUMBER,
                            SimdJsonNumberType.SIGNED_INTEGER,
                            (short) -1
                    ),
                    new Value<>(
                            SimdJsonError.SUCCESS,
                            SimdJsonType.NUMBER,
                            SimdJsonNumberType.SIGNED_INTEGER,
                            -1
                    ),
                    new Value<>(
                            SimdJsonError.SUCCESS,
                            SimdJsonType.NUMBER,
                            SimdJsonNumberType.SIGNED_INTEGER,
                            -1L
                    ),
                    new Value<>(
                            SimdJsonError.SUCCESS,
                            SimdJsonType.NUMBER,
                            SimdJsonNumberType.FLOATING_POINT_NUMBER,
                            -1.0
                    ),
                    new Value<>(
                            SimdJsonError.SUCCESS,
                            SimdJsonType.NUMBER,
                            SimdJsonNumberType.SIGNED_INTEGER,
                            "-1"
                    ),
                    new Value<>(
                            SimdJsonError.SUCCESS,
                            SimdJsonType.NUMBER,
                            SimdJsonNumberType.SIGNED_INTEGER,
                            -1L,
                            ""
                    )
            ),
            new Scenario(
                    "\"  abc  \"",
                    new Value<>(
                            SimdJsonError.SUCCESS,
                            SimdJsonType.STRING,
                            SimdJsonNumberType.UNSET,
                            false
                    ),
                    new Value<>(
                            SimdJsonError.SUCCESS,
                            SimdJsonType.STRING,
                            SimdJsonNumberType.UNSET,
                            (short) 0
                    ),
                    new Value<>(
                            SimdJsonError.SUCCESS,
                            SimdJsonType.STRING,
                            SimdJsonNumberType.UNSET,
                            Integer.MIN_VALUE
                    ),
                    new Value<>(
                            SimdJsonError.SUCCESS,
                            SimdJsonType.STRING,
                            SimdJsonNumberType.UNSET,
                            Long.MIN_VALUE
                    ),
                    new Value<>(
                            SimdJsonError.SUCCESS,
                            SimdJsonType.STRING,
                            SimdJsonNumberType.UNSET,
                            Double.NaN
                    ),
                    new Value<>(
                            SimdJsonError.SUCCESS,
                            SimdJsonType.STRING,
                            SimdJsonNumberType.UNSET,
                            "  abc  "
                    ),
                    new Value<>(
                            SimdJsonError.SUCCESS,
                            SimdJsonType.STRING,
                            SimdJsonNumberType.UNSET,
                            0L,
                            "  abc  "
                    )
            ),
            new Scenario(  // N.B.: We report success even when rounding numbers.
                    "1.25",
                    new Value<>(
                            SimdJsonError.SUCCESS,
                            SimdJsonType.NUMBER,
                            SimdJsonNumberType.FLOATING_POINT_NUMBER,
                            false
                    ),
                    new Value<>(
                            SimdJsonError.SUCCESS,
                            SimdJsonType.NUMBER,
                            SimdJsonNumberType.FLOATING_POINT_NUMBER,
                            (short) 1
                    ),
                    new Value<>(
                            SimdJsonError.SUCCESS,
                            SimdJsonType.NUMBER,
                            SimdJsonNumberType.FLOATING_POINT_NUMBER,
                            1
                    ),
                    new Value<>(
                            SimdJsonError.SUCCESS,
                            SimdJsonType.NUMBER,
                            SimdJsonNumberType.FLOATING_POINT_NUMBER,
                            1L
                    ),
                    new Value<>(
                            SimdJsonError.SUCCESS,
                            SimdJsonType.NUMBER,
                            SimdJsonNumberType.FLOATING_POINT_NUMBER,
                            1.25
                    ),
                    new Value<>(
                            SimdJsonError.SUCCESS,
                            SimdJsonType.NUMBER,
                            SimdJsonNumberType.FLOATING_POINT_NUMBER,
                            "1.25"
                    ),
                    new Value<>(
                            SimdJsonError.SUCCESS,
                            SimdJsonType.NUMBER,
                            SimdJsonNumberType.FLOATING_POINT_NUMBER,
                            Double.doubleToLongBits(1.25),
                            ""
                    )
            ),
            new Scenario(  // One past the max value for `short`.
                    "32768",
                    new Value<>(
                            SimdJsonError.SUCCESS,
                            SimdJsonType.NUMBER,
                            SimdJsonNumberType.SIGNED_INTEGER,
                            false
                    ),
                    new Value<>(
                            SimdJsonError.NUMBER_OUT_OF_RANGE,
                            SimdJsonType.NUMBER,
                            SimdJsonNumberType.SIGNED_INTEGER,
                            (short) 0
                    ),
                    new Value<>(
                            SimdJsonError.SUCCESS,
                            SimdJsonType.NUMBER,
                            SimdJsonNumberType.SIGNED_INTEGER,
                            32768
                    ),
                    new Value<>(
                            SimdJsonError.SUCCESS,
                            SimdJsonType.NUMBER,
                            SimdJsonNumberType.SIGNED_INTEGER,
                            32768L
                    ),
                    new Value<>(
                            SimdJsonError.SUCCESS,
                            SimdJsonType.NUMBER,
                            SimdJsonNumberType.FLOATING_POINT_NUMBER,
                            32768.0
                    ),
                    new Value<>(
                            SimdJsonError.SUCCESS,
                            SimdJsonType.NUMBER,
                            SimdJsonNumberType.SIGNED_INTEGER,
                            "32768"
                    ),
                    new Value<>(
                            SimdJsonError.SUCCESS,
                            SimdJsonType.NUMBER,
                            SimdJsonNumberType.SIGNED_INTEGER,
                            32768L,
                            ""
                    )
            ),
            new Scenario(  // One past the numeric range of `int`
                    "2147483648",
                    new Value<>(
                            SimdJsonError.SUCCESS,
                            SimdJsonType.NUMBER,
                            SimdJsonNumberType.SIGNED_INTEGER,
                            false
                    ),
                    new Value<>(
                            SimdJsonError.NUMBER_OUT_OF_RANGE,
                            SimdJsonType.NUMBER,
                            SimdJsonNumberType.SIGNED_INTEGER,
                            (short) 0
                    ),
                    new Value<>(
                            SimdJsonError.NUMBER_OUT_OF_RANGE,
                            SimdJsonType.NUMBER,
                            SimdJsonNumberType.SIGNED_INTEGER,
                            Integer.MIN_VALUE
                    ),
                    new Value<>(
                            SimdJsonError.SUCCESS,
                            SimdJsonType.NUMBER,
                            SimdJsonNumberType.SIGNED_INTEGER,
                            2147483648L
                    ),
                    new Value<>(
                            SimdJsonError.SUCCESS,
                            SimdJsonType.NUMBER,
                            SimdJsonNumberType.FLOATING_POINT_NUMBER,
                            2147483648.0
                    ),
                    new Value<>(
                            SimdJsonError.SUCCESS,
                            SimdJsonType.NUMBER,
                            SimdJsonNumberType.SIGNED_INTEGER,
                            "2147483648"
                    ),
                    new Value<>(
                            SimdJsonError.SUCCESS,
                            SimdJsonType.NUMBER,
                            SimdJsonNumberType.SIGNED_INTEGER,
                            2147483648L,
                            ""
                    )
            ),
            new Scenario(  // Two past last the range of long
                    "9223372036854775809",
                    new Value<>(
                            SimdJsonError.SUCCESS,
                            SimdJsonType.NUMBER,
                            SimdJsonNumberType.SIGNED_INTEGER,
                            false
                    ),
                    new Value<>(
                            SimdJsonError.NUMBER_OUT_OF_RANGE,
                            SimdJsonType.NUMBER,
                            SimdJsonNumberType.SIGNED_INTEGER,
                            (short) 0
                    ),
                    new Value<>(
                            SimdJsonError.NUMBER_OUT_OF_RANGE,
                            SimdJsonType.NUMBER,
                            SimdJsonNumberType.SIGNED_INTEGER,
                            Integer.MIN_VALUE
                    ),
                    new Value<>(
                            SimdJsonError.NUMBER_OUT_OF_RANGE,
                            SimdJsonType.NUMBER,
                            SimdJsonNumberType.UNSIGNED_INTEGER,
                            Long.MIN_VALUE
                    ),
                    new Value<>(
                            SimdJsonError.SUCCESS,
                            SimdJsonType.NUMBER,
                            SimdJsonNumberType.FLOATING_POINT_NUMBER,
                            9223372036854775809.0
                    ),
                    new Value<>(
                            SimdJsonError.SUCCESS,
                            SimdJsonType.NUMBER,
                            SimdJsonNumberType.UNSIGNED_INTEGER,
                            "9223372036854775809"
                    ),
                    new Value<>(
                            SimdJsonError.SUCCESS,
                            SimdJsonType.NUMBER,
                            SimdJsonNumberType.UNSIGNED_INTEGER,
                            -9223372036854775807L,
                            ""
                    )
            ),
            new Scenario(  // Outside the u64 range, completely.
                    "10000000000000000000000000000000000000000",
                    new Value<>(
                            SimdJsonError.SUCCESS,
                            SimdJsonType.NUMBER,
                            SimdJsonNumberType.BIG_INTEGER,
                            false
                    ),
                    new Value<>(
                            SimdJsonError.NUMBER_OUT_OF_RANGE,
                            SimdJsonType.NUMBER,
                            SimdJsonNumberType.BIG_INTEGER,
                            (short) 0
                    ),
                    new Value<>(
                            SimdJsonError.NUMBER_OUT_OF_RANGE,
                            SimdJsonType.NUMBER,
                            SimdJsonNumberType.BIG_INTEGER,
                            Integer.MIN_VALUE
                    ),
                    new Value<>(
                            SimdJsonError.NUMBER_OUT_OF_RANGE,
                            SimdJsonType.NUMBER,
                            SimdJsonNumberType.BIG_INTEGER,
                            Long.MIN_VALUE
                    ),
                    new Value<>(
                            SimdJsonError.SUCCESS,
                            SimdJsonType.NUMBER,
                            SimdJsonNumberType.BIG_INTEGER,
                            1.0E40
                    ),
                    new Value<>(
                            SimdJsonError.SUCCESS,
                            SimdJsonType.NUMBER,
                            SimdJsonNumberType.BIG_INTEGER,
                            "10000000000000000000000000000000000000000"
                    ),
                    new Value<>(
                            SimdJsonError.SUCCESS,
                            SimdJsonType.NUMBER,
                            SimdJsonNumberType.BIG_INTEGER,
                            0L,
                            "10000000000000000000000000000000000000000"
                    )
            ),
            new Scenario(  // A floating point number that is too large for anything else.
                    "1e308",
                    new Value<>(
                            SimdJsonError.SUCCESS,
                            SimdJsonType.NUMBER,
                            SimdJsonNumberType.FLOATING_POINT_NUMBER,
                            false
                    ),
                    new Value<>(
                            SimdJsonError.NUMBER_OUT_OF_RANGE,
                            SimdJsonType.NUMBER,
                            SimdJsonNumberType.FLOATING_POINT_NUMBER,
                            (short) 0
                    ),
                    new Value<>(
                            SimdJsonError.NUMBER_OUT_OF_RANGE,
                            SimdJsonType.NUMBER,
                            SimdJsonNumberType.FLOATING_POINT_NUMBER,
                            Integer.MIN_VALUE
                    ),
                    new Value<>(
                            SimdJsonError.NUMBER_OUT_OF_RANGE,
                            SimdJsonType.NUMBER,
                            SimdJsonNumberType.FLOATING_POINT_NUMBER,
                            Long.MIN_VALUE
                    ),
                    new Value<>(
                            SimdJsonError.SUCCESS,
                            SimdJsonType.NUMBER,
                            SimdJsonNumberType.FLOATING_POINT_NUMBER,
                            1e308
                    ),
                    new Value<>(
                            SimdJsonError.SUCCESS,
                            SimdJsonType.NUMBER,
                            SimdJsonNumberType.FLOATING_POINT_NUMBER,
                            "1e308"
                    ),
                    new Value<>(
                            SimdJsonError.SUCCESS,
                            SimdJsonType.NUMBER,
                            SimdJsonNumberType.FLOATING_POINT_NUMBER,
                            Double.doubleToLongBits(1e308),
                            ""
                    )
            ),
            new Scenario(  // An object
                    "{\"a\": 1}",
                    new Value<>(
                            SimdJsonError.SUCCESS,
                            SimdJsonType.OBJECT,
                            SimdJsonNumberType.UNSET,
                            false
                    ),
                    new Value<>(
                            SimdJsonError.SUCCESS,
                            SimdJsonType.OBJECT,
                            SimdJsonNumberType.UNSET,
                            (short) 0
                    ),
                    new Value<>(
                            SimdJsonError.SUCCESS,
                            SimdJsonType.OBJECT,
                            SimdJsonNumberType.UNSET,
                            Integer.MIN_VALUE
                    ),
                    new Value<>(
                            SimdJsonError.SUCCESS,
                            SimdJsonType.OBJECT,
                            SimdJsonNumberType.UNSET,
                            Long.MIN_VALUE
                    ),
                    new Value<>(
                            SimdJsonError.SUCCESS,
                            SimdJsonType.OBJECT,
                            SimdJsonNumberType.UNSET,
                            Double.NaN
                    ),
                    new Value<>(
                            SimdJsonError.SUCCESS,
                            SimdJsonType.OBJECT,
                            SimdJsonNumberType.UNSET,
                            "{\"a\": 1}"
                    ),
                    new Value<>(
                            SimdJsonError.SUCCESS,
                            SimdJsonType.OBJECT,
                            SimdJsonNumberType.UNSET,
                            0L,
                            "{\"a\": 1}"
                    )
            )
    };
    private static final SimdJsonResult result = new SimdJsonResult();
    private static final String testUnicodeChars = "ðãµ¶Āڜ💩🦞";
    private static final String description = (
            "Hello, I'm John. I live in New York. I have a dog named Max and a cat named Whiskers. " +
                    "This is a purposely long description so that it can stress the `maxLen` logic during " +
                    "string handling. For good measure it also includes a few funky unicode characters: " +
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
            Assert.assertEquals(SimdJsonType.UNSET, result.getType());
        });
    }

    @Test
    public void testBooleanNull() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final boolean res = parser.queryPointerBoolean(json, path2Pointer(".nothing"), result);
            Assert.assertFalse(res);
            Assert.assertEquals(SimdJsonError.SUCCESS, result.getError());
            Assert.assertEquals(SimdJsonType.NULL, result.getType());
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
                SimdJsonParser.convertJsonPathToPointer(new GcUtf8String(""), dest);
                Assert.assertEquals("", dest.toString());
            }
        });

        TestUtils.assertMemoryLeak(() -> {
            try (DirectUtf8Sink dest = new DirectUtf8Sink(100)) {
                SimdJsonParser.convertJsonPathToPointer(new GcUtf8String("$"), dest);
                Assert.assertEquals("", dest.toString());
            }
        });

        TestUtils.assertMemoryLeak(() -> {
            try (DirectUtf8Sink dest = new DirectUtf8Sink(100)) {
                SimdJsonParser.convertJsonPathToPointer(new GcUtf8String(".name[0]"), dest);
                Assert.assertEquals("/name/0", dest.toString());
            }
        });

        TestUtils.assertMemoryLeak(() -> {
            try (DirectUtf8Sink dest = new DirectUtf8Sink(100)) {
                SimdJsonParser.convertJsonPathToPointer(new GcUtf8String("$.name[0]"), dest);
                Assert.assertEquals("/name/0", dest.toString());
            }
        });
    }

    @Test
    public void testDoubleAbsent() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final double res = parser.queryPointerDouble(json, path2Pointer(".nonexistent"), result);
            Assert.assertTrue(Double.isNaN(res));
            Assert.assertEquals(SimdJsonType.UNSET, result.getType());
        });
    }

    @Test
    public void testDoubleNull() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final double res = parser.queryPointerDouble(json, path2Pointer(".nothing"), result);
            Assert.assertTrue(Double.isNaN(res));
            Assert.assertEquals(SimdJsonType.NULL, result.getType());
        });
    }

    @Test
    public void testInvalidPath() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (DirectUtf8Sink dest = new DirectUtf8Sink(100)) {
                parser.queryPointerUtf8(json, path2Pointer("£$£%£%invalid path!!"), result, dest, 100);
                Assert.assertEquals(SimdJsonError.INVALID_JSON_POINTER, result.getError());
                Assert.assertEquals(SimdJsonType.UNSET, result.getType());
            }
        });
    }

    @Test
    public void testLongAbsent() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final long res = parser.queryPointerLong(json, path2Pointer(".nonexistent"), result);
            Assert.assertEquals(Long.MIN_VALUE, res);
            Assert.assertEquals(SimdJsonType.UNSET, result.getType());
        });
    }

    @Test
    public void testLongNull() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final long res = parser.queryPointerLong(json, path2Pointer(".nothing"), result);
            Assert.assertEquals(Long.MIN_VALUE, res);
            Assert.assertEquals(SimdJsonType.NULL, result.getType());
        });
    }

    @Test
    public void testQueryPathBoolean() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            Assert.assertFalse(parser.queryPointerBoolean(json, path2Pointer(".hasChildren"), result));
            Assert.assertTrue(parser.queryPointerBoolean(json, path2Pointer(".pets[1].scratches"), result));
            Assert.assertEquals(SimdJsonType.BOOLEAN, result.getType());
        });
    }

    @Test
    public void testQueryPathDouble() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            Assert.assertEquals(5.6, parser.queryPointerDouble(json, path2Pointer(".height"), result), 0.0001);
            Assert.assertEquals(SimdJsonType.NUMBER, result.getType());
        });
    }

    @Test
    public void testQueryPathLong() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            Assert.assertEquals(30, parser.queryPointerLong(json, path2Pointer(".age"), result));
            Assert.assertEquals(SimdJsonType.NUMBER, result.getType());
        });
    }

    @Test
    public void testQueryPathLongBignum() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final long res = parser.queryPointerLong(json, path2Pointer(".bignum"), result);
            Assert.assertEquals(Long.MIN_VALUE, res);
            Assert.assertEquals(SimdJsonError.NUMBER_OUT_OF_RANGE, result.getError());
            Assert.assertEquals(SimdJsonType.NUMBER, result.getType());
        });
    }

    @Test
    public void testQueryPathLongU64() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final long res = parser.queryPointerLong(json, path2Pointer(".u64_val"), result);
            Assert.assertEquals(Long.MIN_VALUE, res);
            Assert.assertEquals(SimdJsonError.NUMBER_OUT_OF_RANGE, result.getError());
            Assert.assertEquals(SimdJsonType.NUMBER, result.getType());
        });
    }

    @Test
    public void testQueryPathUtf8() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (DirectUtf8Sink dest = new DirectUtf8Sink(1000)) {
                parser.queryPointerUtf8(json, path2Pointer(".name"), result, dest, 100);
                Assert.assertEquals("John", dest.toString());
                Assert.assertTrue(dest.isAscii());

                GcUtf8String descriptionPath = path2Pointer(".description");
                dest.clear();
                parser.queryPointerUtf8(json, descriptionPath, result, dest, 100);
                Assert.assertEquals(description.substring(0, 100), dest.toString());
                Assert.assertTrue(dest.isAscii());

                // The maxLen == 272 chops one of the unicode characters and unless
                // the copy is handled with utf-8-aware logic it would produce a string
                // with an invalid utf-8 sequence.
                dest.clear();
                parser.queryPointerUtf8(json, descriptionPath, result, dest, 272);
                // The string is expected to be truncated at the last valid utf-8 sequence: 270 instead of 272.
                Assert.assertEquals(270, dest.size());
                Assert.assertFalse(dest.isAscii());

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
                Assert.assertEquals(SimdJsonError.SUCCESS, result.getError());
                Assert.assertEquals(SimdJsonType.STRING, result.getType());
                Assert.assertEquals(SimdJsonNumberType.UNSET, result.getNumberType());
                Assert.assertEquals("John", dest.toString());

                dest.clear();

                final long ret2 = parser.queryPointerValue(json, path2Pointer(".age"), result, dest, cap);
                Assert.assertEquals(30, ret2);
                Assert.assertEquals(SimdJsonError.SUCCESS, result.getError());
                Assert.assertEquals(SimdJsonType.NUMBER, result.getType());
                Assert.assertEquals(SimdJsonNumberType.SIGNED_INTEGER, result.getNumberType());
                Assert.assertEquals(0, dest.size());

                final long ret3 = parser.queryPointerValue(json, path2Pointer(".height"), result, dest, cap);
                final double ret3Double = Double.longBitsToDouble(ret3);
                Assert.assertEquals(5.6, ret3Double, 0.000001);
                Assert.assertEquals(SimdJsonError.SUCCESS, result.getError());
                Assert.assertEquals(SimdJsonType.NUMBER, result.getType());
                Assert.assertEquals(SimdJsonNumberType.FLOATING_POINT_NUMBER, result.getNumberType());
                Assert.assertEquals(0, dest.size());

                final long ret4 = parser.queryPointerValue(json, path2Pointer(".hasChildren"), result, dest, cap);
                Assert.assertEquals(0, ret4);
                Assert.assertEquals(SimdJsonError.SUCCESS, result.getError());
                Assert.assertEquals(SimdJsonType.BOOLEAN, result.getType());
                Assert.assertEquals(SimdJsonNumberType.UNSET, result.getNumberType());
                Assert.assertEquals(0, dest.size());

                final long ret5 = parser.queryPointerValue(json, path2Pointer(".hasPets"), result, dest, cap);
                Assert.assertEquals(1, ret5);
                Assert.assertEquals(SimdJsonError.SUCCESS, result.getError());
                Assert.assertEquals(SimdJsonType.BOOLEAN, result.getType());
                Assert.assertEquals(SimdJsonNumberType.UNSET, result.getNumberType());
                Assert.assertEquals(0, dest.size());

                final long ret6 = parser.queryPointerValue(json, path2Pointer(".nothing"), result, dest, cap);
                Assert.assertEquals(0, ret6);
                Assert.assertEquals(SimdJsonError.SUCCESS, result.getError());
                Assert.assertEquals(SimdJsonType.NULL, result.getType());
                Assert.assertEquals(SimdJsonNumberType.UNSET, result.getNumberType());
                Assert.assertEquals(0, dest.size());

                final long ret7 = parser.queryPointerValue(json, path2Pointer(".u64_val"), result, dest, cap);
                Assert.assertEquals(-2L, ret7);
                Assert.assertEquals(SimdJsonError.SUCCESS, result.getError());
                Assert.assertEquals(SimdJsonType.NUMBER, result.getType());
                Assert.assertEquals(SimdJsonNumberType.UNSIGNED_INTEGER, result.getNumberType());
                Assert.assertEquals(0, dest.size());

                final long ret8 = parser.queryPointerValue(json, path2Pointer(".bignum"), result, dest, cap);
                Assert.assertEquals(0, ret8);
                Assert.assertEquals(SimdJsonError.SUCCESS, result.getError());
                Assert.assertEquals(SimdJsonType.NUMBER, result.getType());
                Assert.assertEquals(SimdJsonNumberType.BIG_INTEGER, result.getNumberType());
                Assert.assertEquals("12345678901234567890123456789012345678901234567890", dest.toString());
                dest.clear();

                final long ret9 = parser.queryPointerValue(json, path2Pointer(".pets"), result, dest, cap);
                Assert.assertEquals(0, ret9);
                Assert.assertEquals(SimdJsonError.SUCCESS, result.getError());
                Assert.assertEquals(SimdJsonType.ARRAY, result.getType());
                Assert.assertEquals(SimdJsonNumberType.UNSET, result.getNumberType());
                Assert.assertEquals("[\n" +
                        "    {\"name\": \"Max\", \"species\": \"Dog\"},\n" +
                        "    {\"name\": \"Whiskers\", \"species\": \"Cat\", \"scratches\": true}\n" +
                        "  ]", dest.toString());
                dest.clear();

                final long ret10 = parser.queryPointerValue(json, path2Pointer(".pets[0]"), result, dest, cap);
                Assert.assertEquals(0, ret10);
                Assert.assertEquals(SimdJsonError.SUCCESS, result.getError());
                Assert.assertEquals(SimdJsonType.OBJECT, result.getType());
                Assert.assertEquals(SimdJsonNumberType.UNSET, result.getNumberType());
                Assert.assertEquals("{\"name\": \"Max\", \"species\": \"Dog\"}", dest.toString());
                dest.clear();

                final long ret11 = parser.queryPointerValue(json, path2Pointer(".nonexistent"), result, dest, cap);
                Assert.assertEquals(0, ret11);
                Assert.assertEquals(SimdJsonError.NO_SUCH_FIELD, result.getError());
                Assert.assertEquals(SimdJsonType.UNSET, result.getType());
                Assert.assertEquals(SimdJsonNumberType.UNSET, result.getNumberType());
                Assert.assertEquals(0, dest.size());
            }
        });
    }

    @Test
    public void testScenariosBoolean() throws Exception {
        testScenarios("queryPointerBoolean", (json, dest, scenario, path) -> {
            final boolean res = parser.queryPointerBoolean(json, path, result);
            Value<Boolean> actual = new Value<>(
                    result.getError(),
                    result.getType(),
                    result.getNumberType(),
                    res
            );
            Assert.assertEquals(scenario.expectedBoolean, actual);
        });
    }

    @Test
    public void testScenariosDouble() throws Exception {
        testScenarios("queryPointerDouble", (json, dest, scenario, path) -> {
            final double res = parser.queryPointerDouble(json, path, result);
            Value<Double> actual = new Value<>(
                    result.getError(),
                    result.getType(),
                    result.getNumberType(),
                    res
            );
            Assert.assertEquals(scenario.expectedDouble, actual);
        });
    }

    @Test
    public void testScenariosInt() throws Exception {
        testScenarios("queryPointerInt", (json, dest, scenario, path) -> {
            final int res = parser.queryPointerInt(json, path, result);
            Value<Integer> actual = new Value<>(
                    result.getError(),
                    result.getType(),
                    result.getNumberType(),
                    res
            );
            Assert.assertEquals(scenario.expectedInt, actual);
        });
    }

    @Test
    public void testScenariosLong() throws Exception {
        testScenarios("queryPointerLong", (json, dest, scenario, path) -> {
            final long res = parser.queryPointerLong(json, path, result);
            Value<Long> actual = new Value<>(
                    result.getError(),
                    result.getType(),
                    result.getNumberType(),
                    res
            );
            Assert.assertEquals(scenario.expectedLong, actual);
        });
    }

    @Test
    public void testScenariosShort() throws Exception {
        testScenarios("queryPointerShort", (json, dest, scenario, path) -> {
            final short res = parser.queryPointerShort(json, path, result);
            Value<Short> actual = new Value<>(
                    result.getError(),
                    result.getType(),
                    result.getNumberType(),
                    res
            );
            Assert.assertEquals(scenario.expectedShort, actual);
        });
    }

    @Test
    public void testScenariosUtf8() throws Exception {
        testScenarios("queryPointerUtf8", (json, dest, scenario, path) -> {
            parser.queryPointerUtf8(json, path, result, dest, 1000);
            Value<String> actual = new Value<>(
                    result.getError(),
                    result.getType(),
                    result.getNumberType(),
                    dest.toString()
            );
            Assert.assertEquals(scenario.expectedString, actual);
        });
    }

    @Test
    public void testScenariosValue() throws Exception {
        testScenarios("queryPointerValue", (json, dest, scenario, path) -> {
            final long res = parser.queryPointerValue(json, path, result, dest, 1000);
            Value<Long> actual = new Value<>(
                    result.getError(),
                    result.getType(),
                    result.getNumberType(),
                    res,
                    dest.toString()
            );
            Assert.assertEquals(scenario.expectedValue, actual);
        });
    }

    @Test
    public void testStringAbsent() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (DirectUtf8Sink dest = new DirectUtf8Sink(100)) {
                parser.queryPointerUtf8(json, path2Pointer(".nonexistent"), result, dest, 100);
                Assert.assertEquals("", dest.toString());
                Assert.assertEquals(SimdJsonType.UNSET, result.getType());
            }
        });
    }

    @Test
    public void testStringNull() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (DirectUtf8Sink dest = new DirectUtf8Sink(100)) {
                parser.queryPointerUtf8(json, path2Pointer(".nothing"), result, dest, 100);
                Assert.assertEquals("", dest.toString());
                Assert.assertEquals(SimdJsonType.NULL, result.getType());
            }
        });
    }

    private static String escape(String json) {
        return json.replace("\"", "\\\"");
    }

    private static GcUtf8String path2Pointer(String path) {
        try (DirectUtf8Sink dest = new DirectUtf8Sink(100)) {
            SimdJsonParser.convertJsonPathToPointer(new GcUtf8String(path), dest);
            return new GcUtf8String(dest.toString());
        }
    }

    private static void testListScenario(
            String method,
            ScenarioTestCode testCode,
            Scenario scenario,
            DirectUtf8Sink json,
            DirectUtf8Sink dest
    ) {
        json.clear();
        json.put('[');
        json.put(scenario.json);
        json.put(']');
        dest.clear();

        try {
            testCode.run(json, dest, scenario, ELEM_0_POINTER);
        } catch (AssertionError e) {
            final String message = "List scenario failed for call `parser." + method +
                    "(\"[" + escape(scenario.json) + "]\", \"" + ELEM_0_POINTER + "\", ..)`. Error: " + e.getMessage();
            throw new AssertionError(message, e);
        }
    }

    private static void testScalarScenario(
            String method,
            ScenarioTestCode testCode,
            Scenario scenario,
            DirectUtf8Sink json,
            DirectUtf8Sink dest
    ) {
        json.clear();
        json.put(scenario.json);
        dest.clear();

        try {
            testCode.run(json, dest, scenario, ROOT_POINTER);
        } catch (AssertionError e) {
            final String message = "Scalar scenario failed for call `parser." + method +
                    "(\"" + escape(scenario.json) + "\", \"" + ROOT_POINTER + "\", ..)`. Error: " + e.getMessage();
            throw new AssertionError(message, e);
        }
    }

    private void testScenarios(String method, ScenarioTestCode testCode) throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (
                    DirectUtf8Sink json = new DirectUtf8Sink(1000);
                    DirectUtf8Sink dest = new DirectUtf8Sink(1000)
            ) {
                for (Scenario scenario : SCENARIOS) {
                    testScalarScenario(method, testCode, scenario, json, dest);
                    testListScenario(method, testCode, scenario, json, dest);
                }
            }
        });
    }

    private interface ScenarioTestCode {
        void run(
                DirectUtf8Sequence json,
                DirectUtf8Sink dest,
                Scenario scenario,
                DirectUtf8Sequence path
        );
    }

    private static class Scenario {
        public final Value<Boolean> expectedBoolean;
        public final Value<Double> expectedDouble;
        public final Value<Integer> expectedInt;
        public final Value<Long> expectedLong;
        public final Value<Short> expectedShort;
        public final Value<String> expectedString;
        public final Value<Long> expectedValue;
        public final String json;

        public Scenario(
                String json,
                Value<Boolean> expectedBoolean,
                Value<Short> expectedShort,
                Value<Integer> expectedInt,
                Value<Long> expectedLong,
                Value<Double> expectedDouble,
                Value<String> expectedString,
                Value<Long> expectedValue
        ) {
            this.json = json;
            this.expectedBoolean = expectedBoolean;
            this.expectedShort = expectedShort;
            this.expectedInt = expectedInt;
            this.expectedLong = expectedLong;
            this.expectedDouble = expectedDouble;
            this.expectedString = expectedString;
            this.expectedValue = expectedValue;
        }
    }

    private static class Value<T> {
        String buffer = null;
        int error;
        int numberType;
        int type;
        T value;

        public Value(int error, int type, int numberType, T value) {
            this.error = error;
            this.type = type;
            this.value = value;
        }

        public Value(int error, int type, int numberType, T value, String buffer) {
            this.error = error;
            this.type = type;
            this.numberType = numberType;
            this.value = value;
            this.buffer = buffer;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj instanceof Value) {
                Value<?> other = (Value<?>) obj;
                return error == other.error &&
                        type == other.type &&
                        numberType == other.numberType &&
                        Objects.equals(value, other.value) &&
                        Objects.equals(buffer, other.buffer);
            }
            return false;
        }

        @Override
        public String toString() {
            final String errorStr = SimdJsonError.getMessage(error).split(":")[0];
            if (buffer != null) {
                return "error=" + errorStr +
                        ", type=" + SimdJsonType.nameOf(type) +
                        ", numberType=" + SimdJsonNumberType.nameOf(numberType) +
                        ", value=" + value +
                        ", buffer=" + buffer;
            } else {
                return "error=" + errorStr +
                        ", type=" + SimdJsonType.nameOf(type) +
                        ", numberType=" + SimdJsonNumberType.nameOf(numberType) +
                        ", value=" + value;
            }
        }
    }

    static {
        // log is needed to greedily allocate logger infra and
        // exclude it from leak detector
        LogFactory.getLog(SimdJsonParserTest.class);
    }
}
