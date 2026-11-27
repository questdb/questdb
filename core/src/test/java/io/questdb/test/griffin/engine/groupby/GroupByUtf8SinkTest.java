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

package io.questdb.test.griffin.engine.groupby;

import io.questdb.griffin.engine.groupby.FastGroupByAllocator;
import io.questdb.griffin.engine.groupby.GroupByAllocator;
import io.questdb.griffin.engine.groupby.GroupByUtf8Sink;
import io.questdb.std.Chars;
import io.questdb.std.Numbers;
import io.questdb.std.Rnd;
import io.questdb.std.str.DirectUtf8String;
import io.questdb.std.str.Utf8String;
import io.questdb.std.str.Utf8StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.nio.charset.StandardCharsets;

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
    public void testOfPtrRoundTrip() throws Exception {
        assertMemoryLeak(() -> {
            // 64-byte chunks: the growth below moves the buffer to a chunk of its own
            try (GroupByAllocator allocator = new FastGroupByAllocator(64, Numbers.SIZE_1MB)) {
                GroupByUtf8Sink sink = new GroupByUtf8Sink();
                sink.setAllocator(allocator);
                sink.put(new Utf8String("foo"));

                // GROUP BY functions keep ptr() in a map value and come back to it through of()
                GroupByUtf8Sink restored = new GroupByUtf8Sink();
                restored.setAllocator(allocator);
                restored.of(sink.ptr());
                TestUtils.assertEquals("foo", restored);

                final String tail = "bar".repeat(100);
                restored.put(new Utf8String(tail));
                TestUtils.assertPtrCompliance(restored);
                sink.of(restored.ptr());
                TestUtils.assertEquals("foo" + tail, sink);
                Assert.assertEquals(3 + tail.length(), sink.size());
                Assert.assertTrue(sink.capacity() >= sink.size());

                sink.of(0);
                Assert.assertEquals(0, sink.ptr());
                Assert.assertEquals(0, sink.size());
            }
        });
    }

    @Test
    public void testPtrAddressesFirstByte() throws Exception {
        final int N = 300;
        assertMemoryLeak(() -> {
            // 64-byte chunks: the buffer grows in place, then moves to chunks of its own
            try (GroupByAllocator allocator = new FastGroupByAllocator(64, Numbers.SIZE_1MB)) {
                GroupByUtf8Sink sink = new GroupByUtf8Sink();
                sink.setAllocator(allocator);
                Rnd rnd = TestUtils.generateRandom(LOG);
                Utf8StringSink piece = new Utf8StringSink();
                Utf8StringSink expected = new Utf8StringSink();
                DirectUtf8String view = new DirectUtf8String();
                for (int i = 0; i < N; i++) {
                    piece.clear();
                    rnd.nextUtf8Str(rnd.nextInt(12), piece);
                    sink.put(piece);
                    expected.put(piece);

                    TestUtils.assertPtrCompliance(sink);
                    TestUtils.assertEquals(expected, view.of(sink.ptr(), sink.ptr() + sink.size()));
                }
            }
        });
    }

    @Test
    public void testPtrOfEmptySink() throws Exception {
        assertMemoryLeak(() -> {
            try (GroupByAllocator allocator = new FastGroupByAllocator(64, Numbers.SIZE_1MB)) {
                GroupByUtf8Sink sink = new GroupByUtf8Sink();
                sink.setAllocator(allocator);
                // no buffer yet
                Assert.assertEquals(0, sink.ptr());
                Assert.assertEquals(0, sink.size());
                Assert.assertEquals(0, sink.capacity());
                Assert.assertTrue(sink.isAscii());
                Assert.assertEquals(0, sink.zeroPaddedSixPrefix());

                // an empty value allocates the buffer
                sink.put(Utf8String.EMPTY);
                Assert.assertTrue(sink.ptr() > 0);
                Assert.assertEquals(0, sink.size());
                Assert.assertTrue(sink.isAscii());
                Assert.assertEquals(0, sink.zeroPaddedSixPrefix());
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
    public void testZeroPaddedSixPrefix() throws Exception {
        assertMemoryLeak(() -> {
            try (GroupByAllocator allocator = new FastGroupByAllocator(64, Numbers.SIZE_1MB)) {
                GroupByUtf8Sink sink = new GroupByUtf8Sink();
                sink.setAllocator(allocator);
                // a long value first, so that a shorter one must not see its bytes after clear()
                final String[] values = {"abcdefghijk", "", "a", "ab", "abcde", "abcdef", "abcdefg", "żółw", "x", "😀"};
                for (String value : values) {
                    sink.clear();
                    sink.put(new Utf8String(value));
                    final byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
                    long expected = 0;
                    for (int i = 0, n = Math.min(bytes.length, 6); i < n; i++) {
                        expected |= (bytes[i] & 0xffL) << (8 * i);
                    }
                    Assert.assertEquals(value, expected, sink.zeroPaddedSixPrefix());
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
