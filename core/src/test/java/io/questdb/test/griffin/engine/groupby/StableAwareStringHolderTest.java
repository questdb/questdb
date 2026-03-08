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

package io.questdb.test.griffin.engine.groupby;

import io.questdb.griffin.engine.groupby.FastGroupByAllocator;
import io.questdb.griffin.engine.groupby.GroupByAllocator;
import io.questdb.griffin.engine.groupby.StableAwareStringHolder;
import io.questdb.std.Chars;
import io.questdb.std.Numbers;
import io.questdb.std.Rnd;
import io.questdb.std.str.DirectString;
import io.questdb.std.str.DirectUtf16Sink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class StableAwareStringHolderTest extends AbstractCairoTest {

    @Test
    public void testClearAndSet() throws Exception {
        assertMemoryLeak(() -> {
            try (GroupByAllocator allocator = new FastGroupByAllocator(64, Numbers.SIZE_1GB)) {
                StableAwareStringHolder holder = new StableAwareStringHolder();
                holder.setAllocator(allocator);
                holder.clearAndSet("foobar");
                TestUtils.assertEquals("foobar", holder);

                holder.clearAndSet("barbaz");
                TestUtils.assertEquals("barbaz", holder);
            }
        });
    }

    @Test
    public void testClearAndSetDirect() throws Exception {
        assertMemoryLeak(() -> {
            try (GroupByAllocator allocator = new FastGroupByAllocator(64, Numbers.SIZE_1GB);
                 DirectUtf16Sink directCharSequence = new DirectUtf16Sink(16)
            ) {
                directCharSequence.put("barbaz");
                DirectString stableDirectString = new DirectString(() -> true);
                stableDirectString.of(directCharSequence.lo(), directCharSequence.hi());
                StableAwareStringHolder holder = new StableAwareStringHolder();
                holder.setAllocator(allocator);

                // store a non-stable char sequence
                holder.of(0).clearAndSet("foobar");
                TestUtils.assertEquals("foobar", holder);
                long foobarPtr = holder.colouredPtr();

                // store a direct char sequence into a new location
                holder.of(0).clearAndSet(stableDirectString);
                TestUtils.assertEquals("barbaz", holder);
                long barbazPtr = holder.colouredPtr();

                // store a direct char sequence into the original location of the non-direct string
                holder.of(foobarPtr).clearAndSet(stableDirectString);
                TestUtils.assertEquals("barbaz", holder);

                // now add a non-direct string without changing the address
                holder.clearAndSet("something_else");
                TestUtils.assertEquals("something_else", holder);

                // and re-add the direct string. again, without changing the address
                holder.clearAndSet(stableDirectString);
                TestUtils.assertEquals("barbaz", holder);

                // store a non-direct long char sequence into the original location of the direct string
                holder.of(barbazPtr).clearAndSet("foobarfoobarfoobarfoobarfoobarfoobarfoobarfoobarfoobar");
                TestUtils.assertEquals("foobarfoobarfoobarfoobarfoobarfoobarfoobarfoobarfoobar", holder);
            }
        });
    }

    @Test
    public void testClearAndSetDirect_fuzzed() throws Exception {
        assertMemoryLeak(() -> {
            try (GroupByAllocator allocator = new FastGroupByAllocator(64, Numbers.SIZE_1GB);
                 DirectUtf16Sink directCharSequence = new DirectUtf16Sink(16)
            ) {
                StableAwareStringHolder holder = new StableAwareStringHolder();
                holder.setAllocator(allocator);
                Rnd rnd = TestUtils.generateRandom(null);
                DirectString stableDirectString = new DirectString(() -> true);
                for (int i = 0; i < 1_000; i++) {
                    boolean useDirect = rnd.nextBoolean();
                    int len = rnd.nextPositiveInt() % 100;
                    if (len == 99) {
                        holder.clearAndSet(null);
                        Assert.assertEquals(0, holder.length());
                    } else if (useDirect) {
                        directCharSequence.clear();
                        rnd.nextChars(directCharSequence, len);
                        stableDirectString.of(directCharSequence.lo(), directCharSequence.hi());
                        holder.clearAndSet(stableDirectString);
                        TestUtils.assertEquals(stableDirectString, holder);
                    } else {
                        CharSequence cs = rnd.nextChars(len);
                        holder.clearAndSet(cs);
                        TestUtils.assertEquals(cs, holder);
                    }
                }
            }
        });
    }

    @Test
    public void testPutCharSequence() throws Exception {
        final int N = 1000;
        assertMemoryLeak(() -> {
            try (GroupByAllocator allocator = new FastGroupByAllocator(64, Numbers.SIZE_1GB)) {
                StableAwareStringHolder holder = new StableAwareStringHolder();
                holder.setAllocator(allocator);
                Assert.assertEquals(0, holder.length());

                for (int i = 0; i < 3; i++) {
                    holder.clearAndSet("");
                    Assert.assertEquals(0, holder.length());
                }

                holder.clearAndSet(Chars.repeat("a", N));
                Assert.assertEquals(holder.length(), N);
                for (int i = 0; i < N; i++) {
                    Assert.assertEquals('a', holder.charAt(i));
                }
            }
        });
    }
}
