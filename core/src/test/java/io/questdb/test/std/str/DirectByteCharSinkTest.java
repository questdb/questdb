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
import io.questdb.std.str.ByteSequence;
import io.questdb.std.str.DirectByteCharSink;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.function.Supplier;

public class DirectByteCharSinkTest {
    @Test
    public void testBorrowNativeByteSink() {
        try (DirectByteCharSink sink = new DirectByteCharSink(16)) {
            final long ptr = sink.ptr();
            Assert.assertNotEquals(0, ptr);
            Assert.assertEquals(32, sink.capacity());
            sink.put((byte) 'a');
            sink.put((byte) 'b');
            sink.put((byte) 'c');
            Assert.assertEquals(3, sink.length());
            Assert.assertEquals((byte) 'a', sink.byteAt(0));
            Assert.assertEquals((byte) 'b', sink.byteAt(1));
            Assert.assertEquals((byte) 'c', sink.byteAt(2));

            try (NativeByteSink directSink = sink.borrowDirectByteSink()) {
                final long impl = directSink.ptr();
                Assert.assertNotEquals(ptr, impl);
                final long implPtr = Unsafe.getUnsafe().getLong(impl);
                Unsafe.getUnsafe().putByte(implPtr, (byte) 'd');
                Unsafe.getUnsafe().putLong(impl, implPtr + 1);
                Assert.assertEquals(4, sink.length());
                Assert.assertEquals(32, sink.capacity());
                final long newImplPtr = DirectByteSink.implBook(impl, 400);
                Assert.assertEquals(newImplPtr, Unsafe.getUnsafe().getLong(impl));
                Assert.assertEquals(512, sink.capacity());
                final long implLo = Unsafe.getUnsafe().getLong(impl + 8);
                final long implHi = Unsafe.getUnsafe().getLong(impl + 16);
                Assert.assertEquals(512, implHi - implLo);
            }

            Assert.assertEquals(4, sink.length());
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
        final long memUsed0 = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_DIRECT_CHAR_SINK);
        final Supplier<Long> getMallocCount = () -> Unsafe.getMallocCount() - mallocCount0;
        final Supplier<Long> getReallocCount = () -> Unsafe.getReallocCount() - reallocCount0;
        final Supplier<Long> getFreeCount = () -> Unsafe.getFreeCount() - freeCount0;
        final Supplier<Long> getMemUsed = () -> Unsafe.getMemUsedByTag(MemoryTag.NATIVE_DIRECT_CHAR_SINK) - memUsed0;

        try (DirectByteCharSink sink = new DirectByteCharSink(0)) {
            Assert.assertEquals(0, sink.length());
            Assert.assertEquals(32, sink.capacity());
            final long ptr = sink.ptr();
            Assert.assertNotEquals(0, ptr);
            Assert.assertEquals(getMallocCount.get().longValue(), 1);
            Assert.assertEquals(getReallocCount.get().longValue(), 0);
            Assert.assertEquals(getFreeCount.get().longValue(), 0);
            Assert.assertEquals(getMemUsed.get().longValue(), 32);

            sink.put((byte) 'a');
            Assert.assertEquals(1, sink.length());
            Assert.assertEquals(32, sink.capacity());
            Assert.assertEquals(ptr, sink.ptr());
            Assert.assertEquals(getMallocCount.get().longValue(), 1);
            Assert.assertEquals(getReallocCount.get().longValue(), 0);
            Assert.assertEquals(getFreeCount.get().longValue(), 0);
            Assert.assertEquals(getMemUsed.get().longValue(), 32);

            sink.clear();
            Assert.assertEquals(0, sink.length());
            Assert.assertEquals(32, sink.capacity());
            Assert.assertEquals(ptr, sink.ptr());
            Assert.assertEquals(getMallocCount.get().longValue(), 1);
            Assert.assertEquals(getReallocCount.get().longValue(), 0);
            Assert.assertEquals(getFreeCount.get().longValue(), 0);
            Assert.assertEquals(getMemUsed.get().longValue(), 32);

            final ByteSequence bs = new ByteSequence() {
                @Override
                public byte byteAt(int index) {
                    return (byte) 'a';
                }

                @Override
                public int length() {
                    return 40;
                }
            };

            sink.put(bs);
            Assert.assertEquals(40, sink.length());
            Assert.assertEquals(64, sink.capacity());
            Assert.assertNotEquals(ptr, sink.ptr());
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
        try (DirectByteCharSink sink = new DirectByteCharSink(initialCapacity)) {
            for (int i = 0; i < 30; i++) {
                sink.put((byte) ('a' + i)).put((byte) '\n');
            }
            TestUtils.assertEquals(expected, sink.toString());
            sink.clear();
            for (int i = 0; i < 30; i++) {
                sink.put((byte) ('a' + i)).put((byte) '\n');
            }
            TestUtils.assertEquals(expected, sink.toString());

            Assert.assertTrue(sink.length() > 0);
            Assert.assertTrue(sink.capacity() >= sink.length());
        }
    }

    @Test
    public void testUtf8Encoding() {
        final int initialCapacity = 4;
        try (DirectByteCharSink sink = new DirectByteCharSink(initialCapacity)) {
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

    private static void assertUtf8Encoding(DirectByteCharSink sink, String s) {
        sink.clear();
        sink.encodeUtf8(s);

        long ptr = sink.ptr();
        int len = sink.length();
        byte[] bytes = new byte[len];
        for (int i = 0; i < len; i++) {
            bytes[i] = Unsafe.getUnsafe().getByte(ptr + i);
        }

        TestUtils.assertEquals(s, new String(bytes, StandardCharsets.UTF_8));
    }
}
