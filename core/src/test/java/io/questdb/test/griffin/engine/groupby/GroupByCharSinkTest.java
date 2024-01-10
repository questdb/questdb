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

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.DefaultCairoConfiguration;
import io.questdb.griffin.engine.groupby.GroupByAllocator;
import io.questdb.griffin.engine.groupby.GroupByCharSink;
import io.questdb.std.Chars;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class GroupByCharSinkTest extends AbstractCairoTest {

    @Test
    public void testClear() throws Exception {
        final CairoConfiguration config = new DefaultCairoConfiguration(root) {
            @Override
            public long getGroupByAllocatorDefaultChunkSize() {
                return 64;
            }
        };
        assertMemoryLeak(() -> {
            try (GroupByAllocator allocator = new GroupByAllocator(config)) {
                GroupByCharSink sink = new GroupByCharSink();
                sink.setAllocator(allocator);
                sink.put("foobar");
                TestUtils.assertEquals("foobar", sink);

                sink.clear();
                Assert.assertEquals(0, sink.length());

                sink.put("barbaz");
                TestUtils.assertEquals("barbaz", sink);
            }
        });
    }

    @Test
    public void testPutChar() throws Exception {
        final int N = 1000;
        final CairoConfiguration config = new DefaultCairoConfiguration(root) {
            @Override
            public long getGroupByAllocatorDefaultChunkSize() {
                return 64;
            }
        };
        assertMemoryLeak(() -> {
            try (GroupByAllocator allocator = new GroupByAllocator(config)) {
                GroupByCharSink sink = new GroupByCharSink();
                sink.setAllocator(allocator);
                for (int i = 0; i < N; i++) {
                    sink.put('a');
                }
                Assert.assertEquals(N, sink.length());
                for (int i = 0; i < N; i++) {
                    Assert.assertEquals('a', sink.charAt(i));
                }
            }
        });
    }

    @Test
    public void testPutCharSequence() throws Exception {
        final int N = 1000;
        final CairoConfiguration config = new DefaultCairoConfiguration(root) {
            @Override
            public long getGroupByAllocatorDefaultChunkSize() {
                return 64;
            }
        };
        assertMemoryLeak(() -> {
            try (GroupByAllocator allocator = new GroupByAllocator(config)) {
                GroupByCharSink sink = new GroupByCharSink();
                sink.setAllocator(allocator);
                int len = 0;
                for (int i = 0; i < N; i++) {
                    sink.put(Chars.repeat("a", i));
                    len += i;
                }
                Assert.assertEquals(len, sink.length());
                for (int i = 0; i < len; i++) {
                    Assert.assertEquals('a', sink.charAt(i));
                }
            }
        });
    }
}
