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

package io.questdb.test.std.str;

import io.questdb.std.str.GcUtf8String;
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
    public void testPutUtf8Sequence() {
        Utf8StringSink sink = new Utf8StringSink(1);
        final String str = "こんにちは世界";
        final Utf8String utf8Str = new Utf8String(str);
        sink.put(utf8Str);
        byte[] expectedBytes = str.getBytes(StandardCharsets.UTF_8);
        TestUtils.assertEquals(expectedBytes, sink);
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
            sink.put((byte) ('a' + i)).put((byte) '\n');
        }
        TestUtils.assertEquals(expected, sink.toString());
        sink.clear();
        for (int i = 0; i < 30; i++) {
            sink.put((byte) ('a' + i)).put((byte) '\n');
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
        Utf8StringSink sink = new Utf8StringSink(4);
        final String str = "Здравей свят";
        sink.put(str);
        byte[] expectedBytes = str.getBytes(StandardCharsets.UTF_8);
        TestUtils.assertEquals(expectedBytes, sink);
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
