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

import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import io.questdb.std.bytes.DirectByteSink;
import io.questdb.std.bytes.NativeByteSink;
import io.questdb.std.str.DirectUtf8Sink;
import io.questdb.std.str.Utf8String;
import io.questdb.std.str.Utf8StringSink;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.function.Supplier;

public class DirectUtf8SinkTest {

    @Test
    public void testAsAsciiCharSequence() {
        try (DirectUtf8Sink sink = new DirectUtf8Sink(4)) {
            final String str = "foobar";
            sink.putAscii(str);
            TestUtils.assertEquals(str, sink.asAsciiCharSequence());
        }
    }

    @Test
    public void testBorrowNativeByteSink() {
        try (DirectUtf8Sink sink = new DirectUtf8Sink(16)) {
            final long ptr = sink.ptr();
            Assert.assertNotEquals(0, ptr);
            Assert.assertEquals(32, sink.capacity());
            sink.put((byte) 'a');
            sink.put((byte) 'b');
            sink.put((byte) 'c');
            Assert.assertEquals(3, sink.size());
            Assert.assertEquals((byte) 'a', sink.byteAt(0));
            Assert.assertEquals((byte) 'b', sink.byteAt(1));
            Assert.assertEquals((byte) 'c', sink.byteAt(2));

            try (NativeByteSink directSink = sink.borrowDirectByteSink()) {
                final long impl = directSink.ptr();
                Assert.assertNotEquals(ptr, impl);
                final long implPtr = Unsafe.getUnsafe().getLong(impl);
                Unsafe.getUnsafe().putByte(implPtr, (byte) 'd');
                Unsafe.getUnsafe().putLong(impl, implPtr + 1);
                Assert.assertEquals(4, sink.size());
                Assert.assertEquals(32, sink.capacity());
                final long newImplPtr = DirectByteSink.implBook(impl, 400);
                Assert.assertEquals(newImplPtr, Unsafe.getUnsafe().getLong(impl));
                Assert.assertEquals(512, sink.capacity());
                final long implLo = Unsafe.getUnsafe().getLong(impl + 8);
                final long implHi = Unsafe.getUnsafe().getLong(impl + 16);
                Assert.assertEquals(512, implHi - implLo);
            }

            Assert.assertEquals(4, sink.size());
            Assert.assertEquals((byte) 'a', sink.byteAt(0));
            Assert.assertEquals((byte) 'b', sink.byteAt(1));
            Assert.assertEquals((byte) 'c', sink.byteAt(2));
            Assert.assertEquals((byte) 'd', sink.byteAt(3));
            Assert.assertEquals(512, sink.capacity());
        }
    }

    @Test
    public void testCreateEmpty() {
        final long mallocCount0 = Unsafe.getMallocCount();
        final long reallocCount0 = Unsafe.getReallocCount();
        final long freeCount0 = Unsafe.getFreeCount();
        final long memUsed0 = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_DIRECT_UTF8_SINK);
        final Supplier<Long> getMallocCount = () -> Unsafe.getMallocCount() - mallocCount0;
        final Supplier<Long> getReallocCount = () -> Unsafe.getReallocCount() - reallocCount0;
        final Supplier<Long> getFreeCount = () -> Unsafe.getFreeCount() - freeCount0;
        final Supplier<Long> getMemUsed = () -> Unsafe.getMemUsedByTag(MemoryTag.NATIVE_DIRECT_UTF8_SINK) - memUsed0;

        try (DirectUtf8Sink sink = new DirectUtf8Sink(0)) {
            Assert.assertEquals(0, sink.size());
            Assert.assertEquals(32, sink.capacity());
            final long ptr = sink.ptr();
            Assert.assertNotEquals(0, ptr);
            Assert.assertEquals(getMallocCount.get().longValue(), 1);
            Assert.assertEquals(getReallocCount.get().longValue(), 0);
            Assert.assertEquals(getFreeCount.get().longValue(), 0);
            Assert.assertEquals(getMemUsed.get().longValue(), 32);

            sink.put((byte) 'a');
            Assert.assertEquals(1, sink.size());
            Assert.assertEquals(32, sink.capacity());
            Assert.assertEquals(ptr, sink.ptr());
            Assert.assertEquals(getMallocCount.get().longValue(), 1);
            Assert.assertEquals(getReallocCount.get().longValue(), 0);
            Assert.assertEquals(getFreeCount.get().longValue(), 0);
            Assert.assertEquals(getMemUsed.get().longValue(), 32);

            sink.clear();
            Assert.assertEquals(0, sink.size());
            Assert.assertEquals(32, sink.capacity());
            Assert.assertEquals(ptr, sink.ptr());
            Assert.assertEquals(getMallocCount.get().longValue(), 1);
            Assert.assertEquals(getReallocCount.get().longValue(), 0);
            Assert.assertEquals(getFreeCount.get().longValue(), 0);
            Assert.assertEquals(getMemUsed.get().longValue(), 32);

            Utf8StringSink onHeapSink = new Utf8StringSink();
            onHeapSink.repeat("a", 40);

            sink.put(onHeapSink);
            Assert.assertEquals(40, sink.size());
            Assert.assertEquals(64, sink.capacity());
            Assert.assertEquals(getMallocCount.get().longValue(), 1);
            Assert.assertEquals(getReallocCount.get().longValue(), 1);
            Assert.assertEquals(getFreeCount.get().longValue(), 0);
            Assert.assertEquals(getMemUsed.get().longValue(), 64);
        }

        Assert.assertEquals(getMallocCount.get().longValue(), 1);
        Assert.assertEquals(getReallocCount.get().longValue(), 1);
        Assert.assertEquals(getFreeCount.get().longValue(), 1);
        Assert.assertEquals(getMemUsed.get().longValue(), 0);
    }

    @Test
    public void testDirectUtf8Sequence() {
        try (DirectUtf8Sink srcSink = new DirectUtf8Sink(4); DirectUtf8Sink destSink = new DirectUtf8Sink(4)) {
            final String str = "Здравей свят";
            srcSink.put(str);
            final byte[] expectedBytes = str.getBytes(StandardCharsets.UTF_8);
            TestUtils.assertEquals(expectedBytes, srcSink);
            destSink.put(srcSink);
            TestUtils.assertEquals(expectedBytes, destSink);
        }
    }

    @Test
    public void testPutUtf8Sequence() {
        try (DirectUtf8Sink sink = new DirectUtf8Sink(1)) {
            final String str = "こんにちは世界";
            final Utf8String utf8Str = new Utf8String(str);
            sink.put(utf8Str);
            byte[] expectedBytes = str.getBytes(StandardCharsets.UTF_8);
            TestUtils.assertEquals(expectedBytes, sink);
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
        try (DirectUtf8Sink sink = new DirectUtf8Sink(initialCapacity)) {
            for (int i = 0; i < 30; i++) {
                sink.putAscii((char) ('a' + i)).putAscii('\n');
            }
            TestUtils.assertEquals(expected, sink.toString());
            sink.clear();
            for (int i = 0; i < 30; i++) {
                sink.put((byte) ('a' + i)).put((byte) '\n');
            }
            TestUtils.assertEquals(expected, sink.toString());

            Assert.assertTrue(sink.size() > 0);
            Assert.assertTrue(sink.capacity() >= sink.size());
        }
    }

    @Test
    public void testUtf8Encoding() {
        final int initialCapacity = 4;
        try (DirectUtf8Sink sink = new DirectUtf8Sink(initialCapacity)) {
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
    }

    @Test
    public void testUtf8Sequence() {
        try (DirectUtf8Sink sink = new DirectUtf8Sink(4)) {
            final String str = "Здравей свят";
            sink.put(str);
            final byte[] expectedBytes = str.getBytes(StandardCharsets.UTF_8);
            TestUtils.assertEquals(expectedBytes, sink);
        }
    }

    private static void assertUtf8Encoding(DirectUtf8Sink sink, String s) {
        sink.clear();
        sink.put(s);

        long ptr = sink.ptr();
        int len = sink.size();
        byte[] bytes = new byte[len];
        for (int i = 0; i < len; i++) {
            bytes[i] = Unsafe.getUnsafe().getByte(ptr + i);
        }

        TestUtils.assertEquals(s, new String(bytes, StandardCharsets.UTF_8));
    }
}
