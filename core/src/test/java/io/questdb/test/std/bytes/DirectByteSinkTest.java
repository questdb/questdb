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

package io.questdb.test.std.bytes;

import io.questdb.cairo.CairoException;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import io.questdb.std.bytes.ByteSequence;
import io.questdb.std.bytes.DirectByteSink;
import io.questdb.std.bytes.NativeByteSink;
import org.junit.Assert;
import org.junit.Test;

import java.util.function.Supplier;

public class DirectByteSinkTest {
    @Test
    public void testBasics() {
        final long mallocCount0 = Unsafe.getMallocCount();
        final long reallocCount0 = Unsafe.getReallocCount();
        final long freeCount0 = Unsafe.getFreeCount();
        final long memUsed0 = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_DIRECT_BYTE_SINK);
        final Supplier<Long> getMallocCount = () -> Unsafe.getMallocCount() - mallocCount0;
        final Supplier<Long> getReallocCount = () -> Unsafe.getReallocCount() - reallocCount0;
        final Supplier<Long> getFreeCount = () -> Unsafe.getFreeCount() - freeCount0;
        final Supplier<Long> getMemUsed = () -> Unsafe.getMemUsedByTag(MemoryTag.NATIVE_DIRECT_BYTE_SINK) - memUsed0;

        try (DirectByteSink sink = new DirectByteSink(0, MemoryTag.NATIVE_DIRECT_BYTE_SINK)) {
            Assert.assertEquals(0, sink.size());
            Assert.assertEquals(32, sink.allocatedCapacity());
            final long ptr = sink.ptr();
            Assert.assertNotEquals(0, ptr);
            Assert.assertEquals(ptr, sink.lo());
            Assert.assertEquals(1, getMallocCount.get().longValue());
            Assert.assertEquals(0, getReallocCount.get().longValue());
            Assert.assertEquals(0, getFreeCount.get().longValue());
            Assert.assertEquals(32, getMemUsed.get().longValue());

            sink.put((byte) 'a');
            Assert.assertEquals(1, sink.size());
            Assert.assertEquals(32, sink.allocatedCapacity());
            Assert.assertEquals(ptr, sink.ptr());
            Assert.assertEquals(ptr + sink.size(), sink.hi());
            Assert.assertEquals(1, getMallocCount.get().longValue());
            Assert.assertEquals(0, getReallocCount.get().longValue());
            Assert.assertEquals(0, getFreeCount.get().longValue());
            Assert.assertEquals(32, getMemUsed.get().longValue());

            sink.clear();
            Assert.assertEquals(0, sink.size());
            Assert.assertEquals(32, sink.allocatedCapacity());
            Assert.assertEquals(ptr, sink.ptr());
            Assert.assertEquals(1, getMallocCount.get().longValue());
            Assert.assertEquals(0, getReallocCount.get().longValue());
            Assert.assertEquals(0, getFreeCount.get().longValue());
            Assert.assertEquals(32, getMemUsed.get().longValue());

            final ByteSequence bs = new ByteSequence() {
                @Override
                public byte byteAt(int index) {
                    return (byte) 'a';
                }

                @Override
                public int size() {
                    return 40;
                }
            };

            sink.put(bs);
            Assert.assertEquals(40, sink.size());
            Assert.assertEquals(64, sink.allocatedCapacity());
            Assert.assertEquals(1, getMallocCount.get().longValue());
            Assert.assertEquals(1, getReallocCount.get().longValue());
            Assert.assertEquals(0, getFreeCount.get().longValue());
            Assert.assertEquals(64, getMemUsed.get().longValue());

            Assert.assertEquals(1, getMallocCount.get().longValue());
            Assert.assertEquals(1, getReallocCount.get().longValue());
            Assert.assertEquals(0, getFreeCount.get().longValue());
            Assert.assertEquals(64, getMemUsed.get().longValue());

            try (DirectByteSink other = new DirectByteSink(32, MemoryTag.NATIVE_DIRECT_BYTE_SINK)) {
                Assert.assertEquals(2, getMallocCount.get().longValue());
                other.put((byte) 'x');
                other.put((byte) 'y');
                other.put((byte) 'z');
                Assert.assertEquals(3, other.size());
                Assert.assertEquals(32, other.allocatedCapacity());
                sink.put(null).put(other);  // passed in as DirectByteSequence
                Assert.assertEquals(43, sink.size());
                Assert.assertEquals(64, sink.allocatedCapacity());
                Assert.assertEquals(96, getMemUsed.get().longValue());
            }

            Assert.assertEquals(2, getMallocCount.get().longValue());
            Assert.assertEquals(1, getReallocCount.get().longValue());
            Assert.assertEquals(1, getFreeCount.get().longValue());
            Assert.assertEquals(64, getMemUsed.get().longValue());
        }

        Assert.assertEquals(2, getMallocCount.get().longValue());
        Assert.assertEquals(1, getReallocCount.get().longValue());
        Assert.assertEquals(2, getFreeCount.get().longValue());
        Assert.assertEquals(0, getMemUsed.get().longValue());
    }

    @Test
    public void testBorrowNativeByteSink() {
        try (DirectByteSink sink = new DirectByteSink(16, MemoryTag.NATIVE_DIRECT_BYTE_SINK)) {
            final long ptr = sink.ptr();
            Assert.assertNotEquals(0, ptr);
            Assert.assertEquals(32, sink.allocatedCapacity());
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
                Assert.assertEquals(32, sink.allocatedCapacity());
                final long newImplPtr = DirectByteSink.implBook(impl, 400);
                final long newImplPtr2 = DirectByteSink.implBook(impl, 400);  // idempotent
                Assert.assertEquals(newImplPtr, newImplPtr2);
                Assert.assertEquals(newImplPtr, Unsafe.getUnsafe().getLong(impl));
                Assert.assertEquals(512, sink.allocatedCapacity());
                final long implLo = Unsafe.getUnsafe().getLong(impl + 8);
                final long implHi = Unsafe.getUnsafe().getLong(impl + 16);
                Assert.assertEquals(512, implHi - implLo);
            }

            Assert.assertEquals(4, sink.size());
            Assert.assertEquals((byte) 'a', sink.byteAt(0));
            Assert.assertEquals((byte) 'b', sink.byteAt(1));
            Assert.assertEquals((byte) 'c', sink.byteAt(2));
            Assert.assertEquals((byte) 'd', sink.byteAt(3));
            Assert.assertEquals(512, sink.allocatedCapacity());
        }

    }

    @Test
    public void testOver2GiB() {
        try (
                DirectByteSink oneMegChunk = new DirectByteSink(32, MemoryTag.NATIVE_DIRECT_BYTE_SINK);
                DirectByteSink dbs = new DirectByteSink(32, MemoryTag.NATIVE_DIRECT_BYTE_SINK)
        ) {

            final int oneMiB = 1024 * 1024;
            for (int i = 0; i < oneMiB; i++) {
                oneMegChunk.put((byte) '0');
            }
            Assert.assertEquals(oneMiB, oneMegChunk.size());

            dbs.ensureCapacity(Integer.MAX_VALUE);

            final int numChunks = Integer.MAX_VALUE / oneMiB;
            for (int i = 0; i < numChunks; i++) {
                dbs.put(oneMegChunk);
            }

            for (int i = 0; i < (oneMiB - 1); i++) {
                dbs.put((byte) '0');
            }

            Assert.assertEquals(Integer.MAX_VALUE, dbs.size());
            Assert.assertEquals(Integer.MAX_VALUE, dbs.allocatedCapacity());
            Assert.assertEquals(Integer.MAX_VALUE, dbs.hi() - dbs.lo());

            // The next byte will overflow.
            Assert.assertThrows(CairoException.class, () -> dbs.put((byte) '0'));
        }
    }
}
