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

package io.questdb.test.cutlass.pgwire;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.arr.VarcharArrayParser;
import io.questdb.std.Files;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import io.questdb.test.AbstractTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class VarcharArrayParserTest extends AbstractTest {

    @Test
    public void testEmptyArrayWithExpectedDimCount() {
        try (VarcharArrayParser parser = new VarcharArrayParser()) {
            parser.of("{}", 1);
            Assert.assertEquals(1, parser.getDimCount());
            Assert.assertEquals(0, parser.getDimLen(0));
        }
    }

    @Test
    public void testExpectedDimCount() {
        try (VarcharArrayParser parser = new VarcharArrayParser()) {
            parser.of("{\"a\",\"b\"}", 1);
            Assert.assertEquals(1, parser.getDimCount());
            Assert.assertEquals(ColumnType.encodeArrayType(ColumnType.VARCHAR, 1, false), parser.getType());
        }
    }

    @Test
    public void testExpectedDimCountMismatch() {
        try (VarcharArrayParser parser = new VarcharArrayParser()) {
            try {
                parser.of("{\"a\",\"b\"}", 2);
                Assert.fail("Expected IllegalArgumentException");
            } catch (IllegalArgumentException e) {
                TestUtils.assertContains(e.getMessage(), "expected 2 dimensions, got 1");
            }
        }
    }

    @Test
    public void testMultiDimensionalArrayThrowsException() {
        try (VarcharArrayParser parser = new VarcharArrayParser()) {
            try {
                parser.of("{{\"a\",\"b\"},{\"c\",\"d\"}}");
                Assert.fail("Expected IllegalArgumentException");
            } catch (IllegalArgumentException e) {
                TestUtils.assertContains(e.getMessage(), "varchar arrays must be 1-dimensional");
            }
        }
    }

    @Test
    public void testParseArrayWithNewlines() {
        try (VarcharArrayParser parser = new VarcharArrayParser()) {
            parser.of("{\n\"hello\",\r\n\"world\"\n}");
            Assert.assertEquals(1, parser.getDimCount());
            Assert.assertEquals(2, parser.getDimLen(0));
            Assert.assertEquals("hello", parser.getVarcharAt(0).toString());
            Assert.assertEquals("world", parser.getVarcharAt(1).toString());
        }
    }

    @Test
    public void testParseArrayWithNulls() {
        try (VarcharArrayParser parser = new VarcharArrayParser()) {
            parser.of("{\"hello\",null,\"world\",NULL}");
            Assert.assertEquals(1, parser.getDimCount());
            Assert.assertEquals(4, parser.getDimLen(0));
            Assert.assertEquals(4, parser.getFlatViewLength());
            Assert.assertEquals("hello", parser.getVarcharAt(0).toString());
            Assert.assertNull(parser.getVarcharAt(1));
            Assert.assertEquals("world", parser.getVarcharAt(2).toString());
            Assert.assertNull(parser.getVarcharAt(3));
        }
    }

    @Test
    public void testParseArrayWithSquareBrackets() {
        try (VarcharArrayParser parser = new VarcharArrayParser()) {
            parser.of("[\"foo\",\"bar\"]");
            Assert.assertEquals(1, parser.getDimCount());
            Assert.assertEquals(2, parser.getDimLen(0));
            Assert.assertEquals("foo", parser.getVarcharAt(0).toString());
            Assert.assertEquals("bar", parser.getVarcharAt(1).toString());
        }
    }

    @Test
    public void testParseArrayWithWhitespace() {
        try (VarcharArrayParser parser = new VarcharArrayParser()) {
            parser.of("{ \"hello\" , \"world\" }");
            Assert.assertEquals(1, parser.getDimCount());
            Assert.assertEquals(2, parser.getDimLen(0));
            Assert.assertEquals("hello", parser.getVarcharAt(0).toString());
            Assert.assertEquals("world", parser.getVarcharAt(1).toString());
        }
    }

    @Test
    public void testParseArrayWithoutQuotes() {
        try (VarcharArrayParser parser = new VarcharArrayParser()) {
            parser.of("{hello, world, test}");
            Assert.assertEquals(1, parser.getDimCount());
            Assert.assertEquals(3, parser.getDimLen(0));
            Assert.assertEquals(3, parser.getFlatViewLength());
            Assert.assertEquals("hello", parser.getVarcharAt(0).toString());
            Assert.assertEquals("world", parser.getVarcharAt(1).toString());
            Assert.assertEquals("test", parser.getVarcharAt(2).toString());
        }
    }

    @Test
    public void testParseEmptyArray() {
        try (VarcharArrayParser parser = new VarcharArrayParser()) {
            parser.of("{}", 1);
            Assert.assertEquals(1, parser.getDimCount());
            Assert.assertEquals(0, parser.getDimLen(0));
            Assert.assertEquals(0, parser.length());
            Assert.assertEquals(0, parser.getFlatViewLength());
        }
    }

    @Test
    public void testParseEmptyStrings() {
        try (VarcharArrayParser parser = new VarcharArrayParser()) {
            parser.of("{\"\",\"hello\",\"\"}");
            Assert.assertEquals(1, parser.getDimCount());
            Assert.assertEquals(3, parser.getDimLen(0));
            Assert.assertEquals("", parser.getVarcharAt(0).toString());
            Assert.assertEquals("hello", parser.getVarcharAt(1).toString());
            Assert.assertEquals("", parser.getVarcharAt(2).toString());
        }
    }

    @Test
    public void testParseFromMemoryEmptyArray() {
        String input = "{}";
        byte[] bytes = input.getBytes(Files.UTF_8);
        long ptr = Unsafe.malloc(bytes.length, MemoryTag.NATIVE_DEFAULT);
        try {
            for (int i = 0; i < bytes.length; i++) {
                Unsafe.getUnsafe().putByte(ptr + i, bytes[i]);
            }
            try (VarcharArrayParser parser = new VarcharArrayParser()) {
                parser.of(ptr, ptr + bytes.length, 1);
                Assert.assertEquals(1, parser.getDimCount());
                Assert.assertEquals(0, parser.getDimLen(0));
                Assert.assertEquals(0, parser.getFlatViewLength());
            }
        } finally {
            Unsafe.free(ptr, bytes.length, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testParseFromMemoryEmptyInput() {
        try (VarcharArrayParser parser = new VarcharArrayParser()) {
            parser.of(0, 0);
            Assert.assertEquals(ColumnType.NULL, parser.getType());
            Assert.assertEquals(0, parser.getFlatViewLength());
        }
    }

    @Test
    public void testParseFromMemorySimpleArray() {
        String input = "{\"hello\",\"world\"}";
        byte[] bytes = input.getBytes(Files.UTF_8);
        long ptr = Unsafe.malloc(bytes.length, MemoryTag.NATIVE_DEFAULT);
        try {
            for (int i = 0; i < bytes.length; i++) {
                Unsafe.getUnsafe().putByte(ptr + i, bytes[i]);
            }
            try (VarcharArrayParser parser = new VarcharArrayParser()) {
                parser.of(ptr, ptr + bytes.length);
                Assert.assertEquals(1, parser.getDimCount());
                Assert.assertEquals(2, parser.getDimLen(0));
                Assert.assertEquals("hello", parser.getVarcharAt(0).toString());
                Assert.assertEquals("world", parser.getVarcharAt(1).toString());
            }
        } finally {
            Unsafe.free(ptr, bytes.length, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testParseFromMemorySingleElement() {
        String input = "hello";
        byte[] bytes = input.getBytes(Files.UTF_8);
        long ptr = Unsafe.malloc(bytes.length, MemoryTag.NATIVE_DEFAULT);
        try {
            for (int i = 0; i < bytes.length; i++) {
                Unsafe.getUnsafe().putByte(ptr + i, bytes[i]);
            }
            try (VarcharArrayParser parser = new VarcharArrayParser()) {
                parser.of(ptr, ptr + bytes.length);
                Assert.assertEquals(1, parser.getDimCount());
                Assert.assertEquals(1, parser.getDimLen(0));
                Assert.assertEquals("hello", parser.getVarcharAt(0).toString());
            }
        } finally {
            Unsafe.free(ptr, bytes.length, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testParseFromMemoryWithExpectedDimCount() {
        String input = "{\"a\",\"b\"}";
        byte[] bytes = input.getBytes(Files.UTF_8);
        long ptr = Unsafe.malloc(bytes.length, MemoryTag.NATIVE_DEFAULT);
        try {
            for (int i = 0; i < bytes.length; i++) {
                Unsafe.getUnsafe().putByte(ptr + i, bytes[i]);
            }
            try (VarcharArrayParser parser = new VarcharArrayParser()) {
                parser.of(ptr, ptr + bytes.length, 1);
                Assert.assertEquals(1, parser.getDimCount());
                Assert.assertEquals(ColumnType.encodeArrayType(ColumnType.VARCHAR, 1, false), parser.getType());
            }
        } finally {
            Unsafe.free(ptr, bytes.length, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testParseFromMemoryWithNulls() {
        String input = "{\"hello\",null,\"world\"}";
        byte[] bytes = input.getBytes(Files.UTF_8);
        long ptr = Unsafe.malloc(bytes.length, MemoryTag.NATIVE_DEFAULT);
        try {
            for (int i = 0; i < bytes.length; i++) {
                Unsafe.getUnsafe().putByte(ptr + i, bytes[i]);
            }
            try (VarcharArrayParser parser = new VarcharArrayParser()) {
                parser.of(ptr, ptr + bytes.length);
                Assert.assertEquals(1, parser.getDimCount());
                Assert.assertEquals(3, parser.getDimLen(0));
                Assert.assertEquals("hello", parser.getVarcharAt(0).toString());
                Assert.assertNull(parser.getVarcharAt(1));
                Assert.assertEquals("world", parser.getVarcharAt(2).toString());
            }
        } finally {
            Unsafe.free(ptr, bytes.length, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testParseFromMemoryWithSquareBrackets() {
        String input = "[\"foo\",\"bar\"]";
        byte[] bytes = input.getBytes(Files.UTF_8);
        long ptr = Unsafe.malloc(bytes.length, MemoryTag.NATIVE_DEFAULT);
        try {
            for (int i = 0; i < bytes.length; i++) {
                Unsafe.getUnsafe().putByte(ptr + i, bytes[i]);
            }
            try (VarcharArrayParser parser = new VarcharArrayParser()) {
                parser.of(ptr, ptr + bytes.length);
                Assert.assertEquals(1, parser.getDimCount());
                Assert.assertEquals(2, parser.getDimLen(0));
                Assert.assertEquals("foo", parser.getVarcharAt(0).toString());
                Assert.assertEquals("bar", parser.getVarcharAt(1).toString());
            }
        } finally {
            Unsafe.free(ptr, bytes.length, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testParseNull() {
        try (VarcharArrayParser parser = new VarcharArrayParser()) {
            parser.of(null);
            Assert.assertEquals(0, parser.getDimCount());
            Assert.assertEquals(0, parser.getFlatViewLength());
            Assert.assertEquals(ColumnType.NULL, parser.getType());
        }
    }

    @Test
    public void testParseSimpleArray() {
        try (VarcharArrayParser parser = new VarcharArrayParser()) {
            parser.of("{\"hello\",\"world\"}");
            Assert.assertEquals(1, parser.getDimCount());
            Assert.assertEquals(2, parser.getDimLen(0));
            Assert.assertEquals(2, parser.getFlatViewLength());
            Assert.assertEquals("hello", parser.getVarcharAt(0).toString());
            Assert.assertEquals("world", parser.getVarcharAt(1).toString());
        }
    }

    @Test
    public void testParseSingleElement() {
        try (VarcharArrayParser parser = new VarcharArrayParser()) {
            parser.of("hello");
            Assert.assertEquals(1, parser.getDimCount());
            Assert.assertEquals(1, parser.getDimLen(0));
            Assert.assertEquals(1, parser.getFlatViewLength());
            Assert.assertEquals("hello", parser.getVarcharAt(0).toString());
        }
    }

    @Test
    public void testParseSingleElementNull() {
        try (VarcharArrayParser parser = new VarcharArrayParser()) {
            parser.of("null");
            Assert.assertEquals(1, parser.getDimCount());
            Assert.assertEquals(1, parser.getDimLen(0));
            Assert.assertEquals(1, parser.getFlatViewLength());
            Assert.assertNull(parser.getVarcharAt(0));
        }
    }

    @Test
    public void testParseUtf8Strings() {
        try (VarcharArrayParser parser = new VarcharArrayParser()) {
            parser.of("{\"Привет\",\"你好\",\"🌍\"}");
            Assert.assertEquals(1, parser.getDimCount());
            Assert.assertEquals(3, parser.getDimLen(0));
            Assert.assertEquals("Привет", parser.getVarcharAt(0).toString());
            Assert.assertEquals("你好", parser.getVarcharAt(1).toString());
            Assert.assertEquals("🌍", parser.getVarcharAt(2).toString());
        }
    }

    @Test
    public void testRepeatedParseWithDifferentLengthArrays() {
        try (VarcharArrayParser parser = new VarcharArrayParser()) {
            parser.of("{\"a\", \"b\"}");
            Assert.assertEquals(2, parser.getDimLen(0));

            parser.of("{\"x\", \"y\", \"z\"}");
            Assert.assertEquals(3, parser.getDimLen(0));
        }
    }
}
