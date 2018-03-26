/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2018 Appsicle
 *
 * This program is free software: you can redistribute it and/or  modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 ******************************************************************************/

package com.questdb.parser.json;

import com.questdb.std.*;
import com.questdb.std.str.Path;
import com.questdb.test.tools.TestUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class JsonLexerTest {

    private static final JsonLexer LEXER = new JsonLexer(1024);
    private static final JsonAssemblingParser listener = new JsonAssemblingParser();

    @AfterClass
    public static void tearDown() {
        LEXER.close();
    }

    @Before
    public void setUp() {
        LEXER.clear();
        listener.clear();
        listener.recordPositions = false;
    }

    @Test
    public void tesStringTooLong() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            String json = "{\"a\":1, \"b\": \"123456789012345678901234567890\"]}";
            int len = json.length() - 6;
            long address = TestUtils.toMemory(json);
            try (JsonLexer lexer = new JsonLexer(4)) {
                try {
                    lexer.parse(address, len, listener);
                    lexer.parseLast();
                    Assert.fail();
                } catch (JsonException e) {
                    Assert.assertEquals("String is too long", e.getMessage());
                    Assert.assertEquals(41, e.getPosition());
                }
            } finally {
                Unsafe.free(address, json.length());
            }
        });
    }

    @Test
    public void testArrayObjArray() throws Exception {
        assertThat("[{\"A\":[\"122\",\"133\"],\"x\":\"y\"},\"134\",\"abc\"]", "[\n" +
                "{\"A\":[122, 133], \"x\": \"y\"}, 134  , \"abc\"\n" +
                "]");
    }

    @Test
    public void testBreakOnValue() throws Exception {
        String in = "{\"x\": \"abcdefhijklmn\"}";
        int len = in.length();
        long address = TestUtils.toMemory(in);
        try {
            LEXER.parse(address, len - 7, listener);
            LEXER.parse(address + len - 7, 7, listener);
            LEXER.parseLast();
            TestUtils.assertEquals("{\"x\":\"abcdefhijklmn\"}", listener.value());
        } finally {
            Unsafe.free(address, len);
        }
    }

    @Test
    public void testDanglingArrayEnd() {
        assertError("Dangling ]", 8, "[1,2,3]]");
    }

    @Test
    public void testDanglingComma() {
        assertError("Attribute name expected", 12, "{\"x\": \"abc\",}");
    }

    @Test
    public void testDanglingObjectEnd() {
        assertError("Dangling }", 8, "[1,2,3]}");
    }

    @Test
    public void testEmptyArray() throws Exception {
        assertThat("[]", "[]");
    }

    @Test
    public void testEmptyObject() throws Exception {
        assertThat("{}", "{}");
    }

    @Test
    public void testExponent() throws Exception {
        assertThat("[\"-1.34E4\",\"3\"]", "[-1.34E4,3]");
    }

    @Test
    public void testIncorrectArrayStart() {
        assertError("[ is not expected here", 3, "[1[]]");
    }

    @Test
    public void testInvalidObjectNesting() {
        assertError("{ is not expected here", 11, "{\"a\":\"x\", {}}");
    }

    @Test
    public void testJsonSlicingAndPositions() throws Exception {
        String in = "[{\"name\": null, \"type\": true, \"formatPattern\":12E-2, \"locale\": \"en-GB\"}]";
        String expected = "<1>[<2>{<4>\"name\":<11>\"null\"<18>,\"type\":<25>\"true\"<32>,\"formatPattern\":<47>\"12E-2\"<55>,\"locale\":<65>\"en-GB\"<71>}<72>]";

        int len = in.length();
        long address = TestUtils.toMemory(in);
        try {
            listener.recordPositions = true;

            for (int i = 0; i < len; i++) {
                listener.clear();
                LEXER.clear();

                LEXER.parse(address, i, listener);
                LEXER.parse(address + i, len - i, listener);
                LEXER.parseLast();
                TestUtils.assertEquals(expected, listener.value());
            }
        } finally {
            Unsafe.free(address, len);
        }
    }

    @Test
    public void testMisplacedArrayEnd() {
        assertError("] is not expected here. You have non-terminated object", 18, "{\"a\":1, \"b\": 15.2]}");
    }

    @Test
    public void testMisplacedColon() {
        assertError("Misplaced ':'", 9, "{\"a\":\"x\":}");
    }

    @Test
    public void testMisplacedQuote() {
        assertError("Unexpected quote '\"'", 9, "{\"a\":\"1\"\", \"b\": 15.2}");
    }

    @Test
    public void testMisplacesObjectEnd() {
        assertError("} is not expected here. You have non-terminated array", 7, "[1,2,3}");
    }

    @Test
    public void testMissingArrayValue() {
        assertError("Unexpected comma", 2, "[,]");
    }

    @Test
    public void testMissingAttributeValue() {
        assertError("Attribute value expected", 6, "{\"x\": }");
    }

    @Test
    public void testNestedObjNestedArray() throws Exception {
        assertThat("{\"x\":{\"y\":[[\"1\",\"2\",\"3\"],[\"5\",\"2\",\"3\"],[\"0\",\"1\"]],\"a\":\"b\"}}", "{\"x\": { \"y\": [[1,2,3], [5,2,3], [0,1]], \"a\":\"b\"}}");
    }

    @Test
    public void testNestedObjects() throws Exception {
        assertThat("{\"abc\":{\"x\":\"123\"},\"val\":\"000\"}", "{\"abc\": {\"x\":\"123\"}, \"val\": \"000\"}");
    }

    @Test
    public void testParseLargeFile() throws Exception {
        String path = JsonLexerTest.class.getResource("/json/test.json").getPath();
        Path p = new Path();
        if (Os.type == Os.WINDOWS && path.startsWith("/")) {
            p.of(path.substring(1));
        } else {
            p.of(path);
        }

        try {
            long l = Files.length(p.$());
            long fd = Files.openRO(p);
            JsonParser listener = new NoOpParser();
            try {
                long buf = Unsafe.malloc(l);
                long bufA = Unsafe.malloc(l);
                long bufB = Unsafe.malloc(l);
                try {
                    Assert.assertEquals(l, Files.read(fd, buf, (int) l, 0));
                    JsonLexer lexer = new JsonLexer(1024);

                    long t = System.nanoTime();
                    for (int i = 0; i < l; i++) {
                        try {
                            lexer.clear();
                            Unsafe.getUnsafe().copyMemory(buf, bufA, i);
                            Unsafe.getUnsafe().copyMemory(buf + i, bufB, l - i);
                            lexer.parse(bufA, i, listener);
                            lexer.parse(bufB, l - i, listener);
                            lexer.parseLast();
                        } catch (JsonException e) {
                            System.out.println(i);
                            throw e;
                        }
                    }
                    System.out.println((System.nanoTime() - t) / l);
                } finally {
                    Unsafe.free(buf, l);
                    Unsafe.free(bufA, l);
                    Unsafe.free(bufB, l);
                }
            } finally {
                Files.close(fd);
            }
        } finally {
            p.close();
        }
    }

    @Test
    public void testQuoteEscape() throws Exception {
        assertThat("{\"x\":\"a\\\"bc\"}", "{\"x\": \"a\\\"bc\"}");
    }

    @Test
    public void testSimpleJson() throws Exception {
        assertThat("{\"abc\":\"123\"}", "{\"abc\": \"123\"}");
    }

    @Test
    public void testUnclosedQuote() {
        assertError("Unexpected symbol", 11, "{\"a\":\"1, \"b\": 15.2}");
    }

    @Test
    public void testUnquotedNumbers() throws Exception {
        assertThat("[{\"A\":\"122\"},\"134\",\"abc\"]", "[\n" +
                "{\"A\":122}, 134  , \"abc\"\n" +
                "]");
    }

    @Test
    public void testUnterminatedArray() {
        assertError("Unterminated array", 37, "{\"x\": { \"y\": [[1,2,3], [5,2,3], [0,1]");
    }

    @Test
    public void testUnterminatedObject() {
        assertError("Unterminated object", 38, "{\"x\": { \"y\": [[1,2,3], [5,2,3], [0,1]]");
    }

    @Test
    public void testUnterminatedString() {
        assertError("Unterminated string", 46, "{\"x\": { \"y\": [[1,2,3], [5,2,3], [0,1]], \"a\":\"b");
    }

    @Test
    public void testWrongQuote() {
        assertError("Unexpected symbol", 10, "{\"x\": \"a\"bc\",}");
    }

    private void assertError(String expected, int expectedPosition, String input) {
        int len = input.length();
        long address = TestUtils.toMemory(input);
        try {
            try {
                LEXER.parse(address, len, listener);
                LEXER.parseLast();
                Assert.fail();
            } catch (JsonException e) {
                Assert.assertEquals(expected, e.getMessage());
                Assert.assertEquals(expectedPosition, e.getPosition());
            }
        } finally {
            Unsafe.free(address, len);
        }
    }

    private void assertThat(String expected, String input) throws Exception {
        int len = input.length();
        long address = TestUtils.toMemory(input);
        try {
            LEXER.parse(address, len, listener);
            LEXER.parseLast();
            TestUtils.assertEquals(expected, listener.value());
        } finally {
            Unsafe.free(address, len);
        }
    }

    private static final class NoOpParser implements JsonParser {
        @Override
        public void onEvent(int code, CharSequence tag, int position) {
        }
    }

    private static class JsonAssemblingParser implements JsonParser, Mutable {
        private final StringBuffer buffer = new StringBuffer();
        private final IntStack itemCountStack = new IntStack();
        private int itemCount = 0;
        private boolean recordPositions = false;

        @Override
        public void clear() {
            buffer.setLength(0);
            itemCount = 0;
            itemCountStack.clear();
        }

        public CharSequence value() {
            return buffer;
        }

        @Override
        public void onEvent(int code, CharSequence tag, int position) {
            if (recordPositions) {
                buffer.append('<').append(position).append('>');
            }
            switch (code) {
                case JsonLexer.EVT_OBJ_START:
                    if (itemCount++ > 0) {
                        buffer.append(',');
                    }
                    buffer.append('{');
                    itemCountStack.push(itemCount);
                    itemCount = 0;
                    break;
                case JsonLexer.EVT_OBJ_END:
                    buffer.append('}');
                    itemCount = itemCountStack.pop();
                    break;
                case JsonLexer.EVT_ARRAY_START:
                    if (itemCount++ > 0) {
                        buffer.append(',');
                    }
                    buffer.append('[');
                    itemCountStack.push(itemCount);
                    itemCount = 0;
                    break;
                case JsonLexer.EVT_ARRAY_END:
                    itemCount = itemCountStack.pop();
                    buffer.append(']');
                    break;
                case JsonLexer.EVT_NAME:
                    if (itemCount > 0) {
                        buffer.append(',');
                    }
                    buffer.append('"');
                    buffer.append(tag);
                    buffer.append('"');
                    buffer.append(':');
                    break;
                case JsonLexer.EVT_VALUE:
                    buffer.append('"');
                    buffer.append(tag);
                    buffer.append('"');
                    itemCount++;
                    break;
                case JsonLexer.EVT_ARRAY_VALUE:
                    if (itemCount++ > 0) {
                        buffer.append(',');
                    }
                    buffer.append('"');
                    buffer.append(tag);
                    buffer.append('"');
                    break;
                default:
                    break;
            }
        }
    }
}