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
import io.questdb.griffin.engine.groupby.GroupByUtf8Sink;
import io.questdb.std.Chars;
import io.questdb.std.Numbers;
import io.questdb.std.Rnd;
import io.questdb.std.str.Utf8StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class GroupByUtf8SinkTest extends AbstractCairoTest {

    @Test
    public void testClear() throws Exception {
        assertMemoryLeak(() -> {
            try (GroupByAllocator allocator = new FastGroupByAllocator(64, Numbers.SIZE_1MB)) {
                GroupByUtf8Sink sink = new GroupByUtf8Sink();
                sink.setAllocator(allocator);
                sink.put("foobar");
                TestUtils.assertEquals("foobar", sink);

                sink.clear();
                Assert.assertEquals(0, sink.size());

                sink.put("barbaz");
                TestUtils.assertEquals("barbaz", sink);
            }
        });
    }

    @Test
    public void testPutCharSequence() throws Exception {
        final int N = 1000;
        assertMemoryLeak(() -> {
            try (GroupByAllocator allocator = new FastGroupByAllocator(64, Numbers.SIZE_1MB)) {
                GroupByUtf8Sink sink = new GroupByUtf8Sink();
                sink.setAllocator(allocator);
                int len = 0;
                for (int i = 0; i < N; i++) {
                    sink.put(Chars.repeat("a", i));
                    len += i;
                }
                Assert.assertEquals(len, sink.size());
                for (int i = 0; i < len; i++) {
                    Assert.assertEquals('a', sink.byteAt(i));
                }
            }
        });
    }

    @Test
    public void testPutUtf8Sequence() throws Exception {
        final int N = 1000;
        assertMemoryLeak(() -> {
            try (GroupByAllocator allocator = new FastGroupByAllocator(64, Numbers.SIZE_1MB)) {
                GroupByUtf8Sink sink = new GroupByUtf8Sink();
                sink.setAllocator(allocator);
                Rnd rnd = new Rnd();
                Utf8StringSink expectedSink = new Utf8StringSink();
                for (int i = 0; i < N; i++) {
                    int len = rnd.nextPositiveInt() % 20;
                    rnd.nextUtf8Str(len, expectedSink);

                    sink.put(expectedSink);
                    TestUtils.assertEquals(expectedSink, sink);
                    Assert.assertEquals(expectedSink.size(), sink.size());
                    Assert.assertEquals(expectedSink.isAscii(), sink.isAscii());
                    sink.clear();
                }
            }
        });
    }

    @Test
    public void testPutUtf8SequenceAscii() throws Exception {
        final int N = 1000;
        assertMemoryLeak(() -> {
            try (GroupByAllocator allocator = new FastGroupByAllocator(64, Numbers.SIZE_1MB)) {
                GroupByUtf8Sink sink = new GroupByUtf8Sink();
                sink.setAllocator(allocator);
                Rnd rnd = new Rnd();
                Utf8StringSink expectedSink = new Utf8StringSink();
                TestUtils.assertEquals(expectedSink.asAsciiCharSequence(), sink.asAsciiCharSequence());
                Assert.assertEquals(expectedSink.size(), sink.size());
                Assert.assertEquals(expectedSink.isAscii(), sink.isAscii());
                for (int i = 0; i < N; i++) {
                    int len = rnd.nextPositiveInt() % 20;
                    rnd.nextUtf8AsciiStr(len, expectedSink);

                    sink.put(expectedSink);
                    TestUtils.assertEquals(expectedSink, sink);
                    TestUtils.assertEquals(expectedSink.asAsciiCharSequence(), sink.asAsciiCharSequence());
                    Assert.assertEquals(expectedSink.size(), sink.size());
                    Assert.assertEquals(expectedSink.isAscii(), sink.isAscii());
                    sink.clear();
                }
            }
        });
    }
}
