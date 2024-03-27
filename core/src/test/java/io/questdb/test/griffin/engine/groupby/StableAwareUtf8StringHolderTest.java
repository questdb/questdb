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

package io.questdb.test.griffin.engine.groupby;

import io.questdb.cairo.VarcharTypeDriver;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryCARW;
import io.questdb.griffin.engine.groupby.GroupByAllocator;
import io.questdb.griffin.engine.groupby.GroupByAllocatorArena;
import io.questdb.griffin.engine.groupby.StableAwareUtf8StringHolder;
import io.questdb.std.Chars;
import io.questdb.std.MemoryTag;
import io.questdb.std.Numbers;
import io.questdb.std.Rnd;
import io.questdb.std.str.*;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class StableAwareUtf8StringHolderTest extends AbstractCairoTest {

    @Test
    public void testClearAndSet() throws Exception {
        assertMemoryLeak(() -> {
            try (GroupByAllocator allocator = new GroupByAllocatorArena(64, Numbers.SIZE_1GB)) {
                StableAwareUtf8StringHolder holder = new StableAwareUtf8StringHolder();
                holder.setAllocator(allocator);
                holder.clearAndSet(new Utf8String("foobar"));
                TestUtils.assertEquals("foobar", holder);

                holder.clearAndSet(new Utf8String("barbaz"));
                TestUtils.assertEquals("barbaz", holder);
            }
        });
    }

    @Test
    public void testClearAndSetDirect() throws Exception {
        assertMemoryLeak(() -> {
            try (
                    GroupByAllocator allocator = new GroupByAllocatorArena(64, Numbers.SIZE_1GB);
                    DirectUtf8Sink directSink = new DirectUtf8Sink(16)
            ) {
                directSink.put("barbaz");
                StableDirectUtf8String stableDirectString = new StableDirectUtf8String();
                stableDirectString.of(directSink.lo(), directSink.hi());
                StableAwareUtf8StringHolder holder = new StableAwareUtf8StringHolder();
                holder.setAllocator(allocator);

                // store a non-stable sequence
                holder.of(0).clearAndSet(new Utf8String("foobar"));
                TestUtils.assertEquals("foobar", holder);
                long foobarPtr = holder.ptr();

                // store a direct sequence into a new location
                holder.of(0).clearAndSet(stableDirectString);
                TestUtils.assertEquals("barbaz", holder);
                long barbazPtr = holder.ptr();

                // store a direct sequence into the original location of the non-direct string
                holder.of(foobarPtr).clearAndSet(stableDirectString);
                TestUtils.assertEquals("barbaz", holder);

                // now add a non-direct string without changing the address
                holder.clearAndSet(new Utf8String("something_else"));
                TestUtils.assertEquals("something_else", holder);

                // and re-add the direct string. again, without changing the address
                holder.clearAndSet(stableDirectString);
                TestUtils.assertEquals("barbaz", holder);

                // store a non-direct long char sequence into the original location of the direct string
                holder.of(barbazPtr).clearAndSet(new Utf8String("foobarfoobarfoobarfoobarfoobarfoobarfoobarfoobarfoobar"));
                TestUtils.assertEquals("foobarfoobarfoobarfoobarfoobarfoobarfoobarfoobarfoobar", holder);
            }
        });
    }

    @Test
    public void testClearAndSetDirect_fuzzed() throws Exception {
        assertMemoryLeak(() -> {
            try (
                    GroupByAllocator allocator = new GroupByAllocatorArena(64, Numbers.SIZE_1GB);
                    DirectUtf16Sink directCharSequence = new DirectUtf16Sink(16);
            ) {
                StableAwareUtf8StringHolder holder = new StableAwareUtf8StringHolder();
                holder.setAllocator(allocator);
                Rnd rnd = TestUtils.generateRandom(null);
                StableDirectUtf8String stableDirectString = new StableDirectUtf8String();
                Utf8StringSink sink = new Utf8StringSink();
                for (int i = 0; i < 1_000; i++) {
                    boolean useDirect = rnd.nextBoolean();
                    int size = rnd.nextPositiveInt() % 100;
                    if (size == 99) {
                        holder.clearAndSet(null);
                        Assert.assertEquals(0, holder.size());
                    } else if (useDirect) {
                        directCharSequence.clear();
                        rnd.nextChars(directCharSequence, size);
                        stableDirectString.of(directCharSequence.lo(), directCharSequence.hi());
                        holder.clearAndSet(stableDirectString);
                        TestUtils.assertEquals(stableDirectString, holder);
                    } else {
                        sink.clear();
                        rnd.nextUtf8Str(size, sink);
                        holder.clearAndSet(sink);
                        TestUtils.assertEquals(sink, holder);
                    }
                }
            }
        });
    }

    @Test
    public void testPutSplitString() throws Exception {
        assertMemoryLeak(() -> {
            try (
                    GroupByAllocator allocator = new GroupByAllocatorArena(64, Numbers.SIZE_1GB);
                    MemoryCARW auxMem = Vm.getCARWInstance(1024, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT);
                    MemoryCARW dataMem = Vm.getCARWInstance(1024, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT)
            ) {
                StableAwareUtf8StringHolder holder = new StableAwareUtf8StringHolder();
                holder.setAllocator(allocator);
                Assert.assertEquals(0, holder.size());

                VarcharTypeDriver.appendValue(dataMem, auxMem, new Utf8String("foobarbaz"));
                Utf8Sequence splitString = VarcharTypeDriver.getValue(0, dataMem, auxMem, 1);

                holder.clearAndSet(splitString);
                Assert.assertEquals(holder.size(), 9);
                TestUtils.assertEquals("foobarbaz", splitString);
            }
        });
    }

    @Test
    public void testPutUtf8Sequence() throws Exception {
        final int N = 1000;
        assertMemoryLeak(() -> {
            try (GroupByAllocator allocator = new GroupByAllocatorArena(64, Numbers.SIZE_1GB)) {
                StableAwareUtf8StringHolder holder = new StableAwareUtf8StringHolder();
                holder.setAllocator(allocator);
                Assert.assertEquals(0, holder.size());

                for (int i = 0; i < 3; i++) {
                    holder.clearAndSet(Utf8String.EMPTY);
                    Assert.assertEquals(0, holder.size());
                }

                Utf8StringSink sink = new Utf8StringSink();
                sink.put(Chars.repeat("a", N));
                holder.clearAndSet(sink);
                Assert.assertEquals(holder.size(), N);
                for (int i = 0; i < N; i++) {
                    Assert.assertEquals((byte) 'a', holder.byteAt(i));
                }
            }
        });
    }
}
