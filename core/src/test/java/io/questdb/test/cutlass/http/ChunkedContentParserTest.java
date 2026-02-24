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

package io.questdb.test.cutlass.http;

import io.questdb.cutlass.http.ChunkedContentParser;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class ChunkedContentParserTest {

    @Test
    public void testChunkSizeOverflow16HexDs() throws Exception {
        // 16 hex 'd' digits = 0xDDDDDDDDDDDDDDDD (negative as signed long).
        // This is the original PoC payload that caused SIGSEGV.
        assertProtocolViolation("dddddddddddddddd\r\nAAAA");
    }

    @Test
    public void testChunkSizeOverflow16HexFs() throws Exception {
        // 16 hex 'f' digits = 0xFFFFFFFFFFFFFFFF = -1 as signed long.
        assertProtocolViolation("ffffffffffffffff\r\nAAAA");
    }

    @Test
    public void testChunkSizeOverflow17Digits() throws Exception {
        // 17 hex digits always overflow (> 64 bits).
        assertProtocolViolation("10000000000000000\r\nAAAA");
    }

    @Test
    public void testChunkSizeOverflowAtBoundary() throws Exception {
        // After parsing 15 digits ('800000000000000'), chunkSize is
        // 0x0800_0000_0000_0000 = MAX_CHUNK_SIZE_BEFORE_SHIFT + 1,
        // so the 16th digit triggers the overflow guard.
        assertProtocolViolation("8000000000000000\r\nAAAA");
    }

    @Test
    public void testChunkSizeOverflowInSecondChunk() throws Exception {
        // Valid first chunk followed by an overflowing second chunk.
        // Exercises the skipEol=false path combined with the overflow guard.
        // Also verifies the first chunk's data was delivered before rejection.
        String input = "3\r\nabc\r\ndddddddddddddddd\r\nAAAA";
        TestUtils.assertMemoryLeak(() -> {
            long ptr = TestUtils.toMemory(input);
            try {
                ChunkedContentParser parser = new ChunkedContentParser();
                parser.clear();

                StringBuilder received = new StringBuilder();
                long result = parser.handleRecv(ptr, ptr + input.length(), (lo, hi) -> {
                    for (long p = lo; p < hi; p++) {
                        received.append((char) Unsafe.getUnsafe().getByte(p));
                    }
                });
                Assert.assertEquals(Long.MIN_VALUE, result);
                TestUtils.assertEquals("abc", received);
            } finally {
                Unsafe.free(ptr, input.length(), MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testMaxChunkSize15HexFs() throws Exception {
        // 15 hex 'f' digits = 0x0FFFFFFFFFFFFFFF (60 bits, positive).
        // Must be accepted.
        String input = "fffffffffffffff\r\nA";
        TestUtils.assertMemoryLeak(() -> {
            long ptr = TestUtils.toMemory(input);
            try {
                ChunkedContentParser parser = new ChunkedContentParser();
                parser.clear();

                long result = parser.handleRecv(ptr, ptr + input.length(), (lo, hi) -> {
                });
                Assert.assertEquals(ptr + input.length(), result);
            } finally {
                Unsafe.free(ptr, input.length(), MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testMaxChunkSizeLongMaxValue() throws Exception {
        // 7fffffffffffffff hex = Long.MAX_VALUE (16 hex digits).
        // Largest value that fits in a positive long. Must be accepted.
        String input = "7fffffffffffffff\r\nA";
        TestUtils.assertMemoryLeak(() -> {
            long ptr = TestUtils.toMemory(input);
            try {
                ChunkedContentParser parser = new ChunkedContentParser();
                parser.clear();

                long result = parser.handleRecv(ptr, ptr + input.length(), (lo, hi) -> {
                });
                // Parser accepts the chunk size and consumes the 1 available
                // byte, returning hi (end of buffer).
                Assert.assertEquals(ptr + input.length(), result);
            } finally {
                Unsafe.free(ptr, input.length(), MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testMultipleChunks() throws Exception {
        String input = "5\r\nhello\r\n6\r\n world\r\n0\r\n\r\n";
        TestUtils.assertMemoryLeak(() -> {
            long ptr = TestUtils.toMemory(input);
            try {
                ChunkedContentParser parser = new ChunkedContentParser();
                parser.clear();

                StringBuilder received = new StringBuilder();
                long result = parser.handleRecv(ptr, ptr + input.length(), (lo, hi) -> {
                    for (long p = lo; p < hi; p++) {
                        received.append((char) Unsafe.getUnsafe().getByte(p));
                    }
                });
                Assert.assertEquals(Long.MAX_VALUE, result);
                TestUtils.assertEquals("hello world", received);
            } finally {
                Unsafe.free(ptr, input.length(), MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testNormalChunkedRequest() throws Exception {
        String input = "5\r\nhello\r\n0\r\n\r\n";
        TestUtils.assertMemoryLeak(() -> {
            long ptr = TestUtils.toMemory(input);
            try {
                ChunkedContentParser parser = new ChunkedContentParser();
                parser.clear();

                StringBuilder received = new StringBuilder();
                long result = parser.handleRecv(ptr, ptr + input.length(), (lo, hi) -> {
                    for (long p = lo; p < hi; p++) {
                        received.append((char) Unsafe.getUnsafe().getByte(p));
                    }
                });
                Assert.assertEquals(Long.MAX_VALUE, result);
                TestUtils.assertEquals("hello", received);
            } finally {
                Unsafe.free(ptr, input.length(), MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    private static void assertProtocolViolation(String input) throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            long ptr = TestUtils.toMemory(input);
            try {
                ChunkedContentParser parser = new ChunkedContentParser();
                parser.clear();

                long result = parser.handleRecv(ptr, ptr + input.length(), (lo, hi) -> {
                });
                Assert.assertEquals(Long.MIN_VALUE, result);
            } finally {
                Unsafe.free(ptr, input.length(), MemoryTag.NATIVE_DEFAULT);
            }
        });
    }
}
