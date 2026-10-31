/*+*****************************************************************************
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

package io.questdb.test.std.str;

import io.questdb.std.ObjList;
import io.questdb.std.str.DirectString;
import io.questdb.std.str.DirectUtf16Sink;
import io.questdb.std.str.DirectUtf8Sink;
import io.questdb.std.str.GcUtf8String;
import io.questdb.std.str.Utf8Sequence;
import io.questdb.std.str.Utf8String;
import io.questdb.std.str.Utf8StringSink;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.nio.charset.StandardCharsets;

public class Utf8StringSinkTest {

    @Test
    public void testAsAsciiCharSequence() {
        Utf8StringSink sink = new Utf8StringSink();
        final String str = "foobar";
        sink.putAscii(str);
        TestUtils.assertEquals(str, sink.asAsciiCharSequence());
    }

    @Test
    public void testCharSequence() {
        Utf8StringSink sink = new Utf8StringSink(4);
        final String str = "Здравей свят";
        sink.put(str);
        byte[] expectedBytes = str.getBytes(StandardCharsets.UTF_8);
        TestUtils.assertEquals(expectedBytes, sink);
    }

    @Test
    public void testClear() {
        Utf8StringSink sink = new Utf8StringSink();
        final String str = "foobar";
        sink.putAscii(str);
        for (int i = str.length(); i > -1; i--) {
            sink.clear(i);
            TestUtils.assertEquals(str.substring(0, i), sink);
        }
    }

    @Test
    public void testDirectUtf8Sequence() {
        final String str = "Здравей свят";
        final GcUtf8String src = new GcUtf8String(str);
        final Utf8StringSink sink = new Utf8StringSink(4);
        sink.put(src);
        final byte[] expectedBytes = str.getBytes(StandardCharsets.UTF_8);
        TestUtils.assertEquals(expectedBytes, sink);
        TestUtils.assertEquals(src, sink);
    }

    @Test
    public void testEscapeJsonStrCharAtFailureKeepsPrefix() {
        Utf8StringSink sink = new Utf8StringSink(8);
        sink.putAscii("pre");
        Assert.assertThrows(IndexOutOfBoundsException.class, () -> sink.escapeJsonStr("abc", 0, 5));
        TestUtils.assertEquals("preabc", sink);
        Assert.assertTrue(sink.isAscii());
        Assert.assertThrows(IndexOutOfBoundsException.class, () -> sink.escapeJsonStr("abc", Integer.MIN_VALUE, Integer.MAX_VALUE));
        TestUtils.assertEquals("preabc", sink);
    }

    @Test
    public void testEscapeJsonStrFourCharWords() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (DirectUtf16Sink input = new DirectUtf16Sink(32);
                 DirectUtf8Sink expected = new DirectUtf8Sink(32)) {
                DirectString direct = new DirectString();
                Utf8StringSink actual = new Utf8StringSink(32);
                for (int lane = 0; lane < 4; lane++) {
                    for (int value = 0; value <= Character.MAX_VALUE; value++) {
                        input.clear();
                        for (int i = 0; i < 4; i++) {
                            input.put(i == lane ? (char) value : (char) ('a' + i));
                        }
                        direct.of(input.ptr(), input.length());
                        expected.clear();
                        expected.escapeJsonStr(input);
                        actual.clear();
                        Assert.assertSame(actual, actual.escapeJsonStr(direct));
                        TestUtils.assertEquals(expected, actual);
                        Assert.assertEquals(expected.isAscii(), actual.isAscii());
                    }
                }
            }
        });
    }

    @Test
    public void testEscapeJsonStrNegativePosition() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (DirectUtf16Sink input = new DirectUtf16Sink(32)) {
                input.put("abcd");
                DirectString direct = new DirectString().of(input.ptr(), input.length());
                Utf8StringSink sink = new Utf8StringSink(8);
                String prefix = "sentinel";
                sink.putAscii(prefix);
                sink.clear(-1);
                Assert.assertThrows(ArrayIndexOutOfBoundsException.class, () -> sink.escapeJsonStr(direct));
                Assert.assertEquals(0, sink.size());
                Assert.assertTrue(sink.isAscii());
                for (int i = 0; i < prefix.length(); i++) {
                    Assert.assertEquals((byte) prefix.charAt(i), sink.byteAt(i));
                }
                sink.putAscii("ok");
                TestUtils.assertEquals("ok", sink);
            }
        });
    }

    @Test
    public void testEscapeJsonStrRanges() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            ObjList<String> cases = new ObjList<>();
            cases.add("");
            cases.add("ASCII prefix ~\u007f");
            cases.add("a\"b\\c");
            cases.add("prefix-\u00e9\u4e2d\ud83d\ude00-tail");
            cases.add("prefix-\ud800x\udc00-tail");
            StringBuilder controls = new StringBuilder();
            for (char c = 0; c < 32; c++) {
                controls.append(c);
            }
            cases.add(controls.toString());
            try (DirectUtf8Sink expected = new DirectUtf8Sink(32);
                 DirectUtf16Sink nativeInput = new DirectUtf16Sink(64)) {
                for (int c = 0; c < cases.size(); c++) {
                    String input = cases.getQuick(c);
                    nativeInput.clear();
                    nativeInput.put(input);
                    DirectString direct = new DirectString().of(nativeInput.ptr(), input.length());
                    for (int lo = 0; lo <= input.length(); lo++) {
                        for (int hi = lo; hi <= input.length(); hi++) {
                            for (int prefixType = 0; prefixType < 2; prefixType++) {
                                String prefix = prefixType == 0 ? "pre:" : "\u00e9:";
                                expected.clear();
                                expected.put(prefix);
                                int prefixSize = expected.size();
                                expected.escapeJsonStr(input, lo, hi);
                                for (int capacity : new int[]{2, prefixSize + hi - lo, 256}) {
                                    Utf8StringSink actual = new Utf8StringSink(capacity);
                                    actual.put(prefix);
                                    Assert.assertSame(actual, actual.escapeJsonStr(input, lo, hi));
                                    TestUtils.assertEquals(expected, actual);
                                    Assert.assertEquals(expected.isAscii(), actual.isAscii());
                                    actual = new Utf8StringSink(capacity);
                                    actual.put(prefix);
                                    Assert.assertSame(actual, actual.escapeJsonStr(direct, lo, hi));
                                    TestUtils.assertEquals(expected, actual);
                                    Assert.assertEquals(expected.isAscii(), actual.isAscii());
                                }
                            }
                        }
                    }
                }
            }
            Utf8StringSink sink = new Utf8StringSink(3);
            sink.putAscii("pre");
            Assert.assertSame(sink, sink.escapeJsonStr("", 0, 0));
            Assert.assertSame(sink, sink.escapeJsonStr(null, 3, 1));
            TestUtils.assertEquals("pre", sink);
            Assert.assertTrue(sink.isAscii());
        });
    }

    @Test
    public void testEscapeJsonStrSubclassUsesCharAt() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            String input = "ab\"\\\u0000\u00e9\ud83d\ude00";
            try (DirectUtf16Sink nativeInput = new DirectUtf16Sink(32);
                 DirectUtf8Sink expected = new DirectUtf8Sink(32)) {
                nativeInput.put("abcdefgh");
                DirectString direct = new DirectString() {
                    @Override
                    public char charAt(int index) {
                        return input.charAt(index);
                    }
                }.of(nativeInput.ptr(), input.length());
                expected.escapeJsonStr(input);
                Utf8StringSink actual = new Utf8StringSink(32);
                actual.escapeJsonStr(direct);
                TestUtils.assertEquals(expected, actual);
                Assert.assertEquals(expected.isAscii(), actual.isAscii());
            }
        });
    }

    @Test
    public void testEscapeJsonStrUtf8Sequence() {
        // Every control character is escaped, NUL included, and a multibyte character is copied
        // byte for byte. The UTF-16 overload is the reference: both have to write the same JSON.
        final String str = "a\0b\"c\\d\ne\u001fé世";
        final Utf8StringSink sink = new Utf8StringSink();
        sink.escapeJsonStr(new Utf8String(str));
        TestUtils.assertEquals("a\\u0000b\\\"c\\\\d\\ne\\u001fé世", sink.toString());

        final Utf8StringSink utf16Sink = new Utf8StringSink();
        utf16Sink.escapeJsonStr(str);
        TestUtils.assertEquals(utf16Sink.toString(), sink.toString());
    }

    @Test
    public void testPutUtf8Sequence() {
        Utf8StringSink sink = new Utf8StringSink(1);
        final String str = "こんにちは世界";
        final Utf8String utf8Str = new Utf8String(str);
        sink.put(utf8Str);
        byte[] expectedBytes = str.getBytes(StandardCharsets.UTF_8);
        TestUtils.assertEquals(expectedBytes, sink);
    }

    @Test
    public void testRepeatChar() {
        int n = 10_000; // high enough to trigger multi-byte encoding
        for (int i = 0; i < n; i++) {
            Utf8StringSink sink = new Utf8StringSink();
            char c = (char) ('a' + i);
            int count = 10;
            sink.repeat(c, count);

            String expectedString = "";
            for (int j = 0; j < count; j++) {
                expectedString += c;
            }
            TestUtils.assertEquals(expectedString, sink);
        }
    }

    @Test
    public void testResize() {
        final String expected = "a\n" +
                "b\n" +
                "c\n" +
                "d\n" +
                "e\n" +
                "f\n" +
                "g\n" +
                "h\n" +
                "i\n" +
                "j\n" +
                "k\n" +
                "l\n" +
                "m\n" +
                "n\n" +
                "o\n" +
                "p\n" +
                "q\n" +
                "r\n" +
                "s\n" +
                "t\n" +
                "u\n" +
                "v\n" +
                "w\n" +
                "x\n" +
                "y\n" +
                "z\n" +
                "{\n" +
                "|\n" +
                "}\n" +
                "~\n";

        final int initialCapacity = 4;
        Utf8StringSink sink = new Utf8StringSink(initialCapacity);
        for (int i = 0; i < 30; i++) {
            sink.putAscii((char) ('a' + i)).putAscii('\n');
        }
        TestUtils.assertEquals(expected, sink.toString());
        sink.clear();
        for (int i = 0; i < 30; i++) {
            sink.putAscii((char) ('a' + i)).putAscii('\n');
        }
        TestUtils.assertEquals(expected, sink.toString());

        Assert.assertTrue(sink.size() > 0);
        Assert.assertTrue(sink.getCapacity() >= sink.size());
        sink.resetCapacity();
        Assert.assertEquals(0, sink.size());
        Assert.assertEquals(initialCapacity, sink.getCapacity());
    }

    @Test
    public void testUtf8Encoding() {
        final int initialCapacity = 4;
        Utf8StringSink sink = new Utf8StringSink(initialCapacity);
        assertUtf8Encoding(sink, "Hello world");
        assertUtf8Encoding(sink, "Привет мир"); // Russian
        assertUtf8Encoding(sink, "你好世界"); // Chinese
        assertUtf8Encoding(sink, "こんにちは世界"); // Japanese
        assertUtf8Encoding(sink, "안녕하세요 세계"); // Korean
        assertUtf8Encoding(sink, "สวัสดีชาวโลก"); // Thai
        assertUtf8Encoding(sink, "مرحبا بالعالم");  // Arabic
        assertUtf8Encoding(sink, "שלום עולם"); // Hebrew
        assertUtf8Encoding(sink, "Γειά σου Κόσμε"); // Greek
        assertUtf8Encoding(sink, "���"); // 4 bytes code points
    }

    @Test
    public void testUtf8Sequence() {
        // non-ascii
        Utf8StringSink sink = new Utf8StringSink(4);
        final String str = "Здравей свят";
        final Utf8Sequence utf8str = new Utf8String(str);

        sink.put(utf8str);
        byte[] expectedBytes = str.getBytes(StandardCharsets.UTF_8);
        TestUtils.assertEquals(expectedBytes, sink);

        sink.clear();
        sink.put(utf8str, 2, 10);
        byte[] expectedBytes2 = "драв".getBytes(StandardCharsets.UTF_8);
        TestUtils.assertEquals(expectedBytes2, sink);

        // ascii
        final String str2 = "abcdefgh";
        final Utf8Sequence utf8str2 = new Utf8String(str2);

        sink.clear();
        sink.put(utf8str2, 2, 6);
        byte[] expectedBytes3 = "cdef".getBytes(StandardCharsets.UTF_8);
        TestUtils.assertEquals(expectedBytes3, sink);
    }

    private static void assertUtf8Encoding(Utf8StringSink sink, String s) {
        sink.clear();
        sink.put(s);

        int len = sink.size();
        byte[] bytes = new byte[len];
        for (int i = 0; i < len; i++) {
            bytes[i] = sink.byteAt(i);
        }

        TestUtils.assertEquals(s, new String(bytes, StandardCharsets.UTF_8));
    }
}
