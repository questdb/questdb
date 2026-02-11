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

package io.questdb.test.cutlass.http.client;


import io.questdb.cutlass.http.client.AbstractChunkedResponse;
import io.questdb.cutlass.http.client.Fragment;
import io.questdb.std.MemoryTag;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.Os;
import io.questdb.std.Rnd;
import io.questdb.std.Unsafe;
import io.questdb.std.str.StringSink;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;

public class ChunkedResponseTest {

    @Test
    public void testChunkSizeSplit() {
        String[] fragments = {
                """
                10\r
                abcdefghjklzxnmd\r
                0a""",
                """
                \r
                0123456789\r
                00\r
                \r
                """
        };
        assertResponse("abcdefghjklzxnmd0123456789", fragments);
    }

    @Test
    public void testChunkSizeSplit2() {
        String[] fragments = {
                """
                10\r
                abcdefghjklzxnmd\r
                0a\r""",
                """
                
                0123456789\r
                00\r
                \r
                """
        };
        assertResponse("abcdefghjklzxnmd0123456789", fragments);
    }

    @Test
    public void testChunkSizeSplit3() {
        String[] fragments = {
                """
                10\r
                abcdefghjklzxnmd\r
                0a\r
                """,
                """
                0123456789\r
                00\r
                \r
                """
        };
        assertResponse("abcdefghjklzxnmd0123456789", fragments);
    }

    @Test
    public void testChunkSizeSplit4() {
        String[] fragments = {
                "10\r\n" +
                        "abcdefghjklzxnmd\r",
                """
                
                0a\r
                0123456789\r
                00\r
                \r
                """
        };
        assertResponse("abcdefghjklzxnmd0123456789", fragments);
    }

    @Test
    public void testChunkSizeSplit5() {
        String[] fragments = {
                "10\r\n" +
                        "abcdefghjklzxnmd",
                """
                \r
                0a\r
                0123456789\r
                00\r
                \r
                """
        };
        assertResponse("abcdefghjklzxnmd0123456789", fragments);
    }

    @Test
    public void testChunkSizeSplit6() {
        String[] fragments = {
                """
                10\r
                abcdefghjklzxnmd\r
                0""",
                """
                a\r
                0123456789\r
                00\r
                \r
                """
        };
        assertResponse("abcdefghjklzxnmd0123456789", fragments);
    }

    @Test
    public void testChunkSizeSplit7() {
        String[] fragments = {
                "10\r\n" +
                        "abcdefghjklzxnmd",
                "\r",
                """
                
                0a\r
                0123456789\r
                00\r
                \r
                """
        };
        assertResponse("abcdefghjklzxnmd0123456789", fragments);
    }

    @Test
    public void testChunkSplit() {
        String[] fragments = {
                """
                10\r
                abcdefghjklzxnmd\r
                """,
                """
                0a\r
                0123456789\r
                00\r
                \r
                """
        };
        assertResponse("abcdefghjklzxnmd0123456789", fragments);
    }

    @Test
    public void testFuzz() {
        Rnd rnd = TestUtils.generateRandom(null);
        int strLen = Math.max(1, rnd.nextInt(1024));
        int chunkCount = Math.max(1, rnd.nextInt(strLen));
        int fragCount = Math.max(1, rnd.nextInt(strLen));
        String input = rnd.nextString(strLen);
        String[] chunks = createChunks(rnd, input, chunkCount);

        // verify that we split chunks correctly
        StringSink actual = new StringSink();
        for (String c : chunks) {
            actual.put(c);
        }
        TestUtils.assertEquals(input, actual);

        StringSink encoded = new StringSink();
        for (String c : chunks) {
            int len = c.length();
            if (len > 0) {
                Numbers.appendHex(encoded, len, false);
                encoded.put("\r\n");
                encoded.put(c);
                encoded.put("\r\n");
            }
        }
        encoded.put("00\r\n");
        encoded.put("\r\n");

        assertResponse(
                input,
                createChunks(rnd, encoded.toString(), fragCount)
        );
    }

    @Test
    public void testSingleFragment() {
        String[] fragments = {
                """
                10\r
                abcdefghjklzxnmd\r
                0a\r
                0123456789\r
                00\r
                \r
                """
        };
        assertResponse("abcdefghjklzxnmd0123456789", fragments);
    }

    private static void assertResponse(CharSequence expected, String[] fragments) {
        long memSize = 4096;
        long mem = Unsafe.malloc(memSize, MemoryTag.NATIVE_DEFAULT);
        try {
            final AbstractChunkedResponse rsp = new AbstractChunkedResponse(mem, mem + memSize, -1) {
                int fragIndex = 0;
                int fragOffset = 0;

                @Override
                protected int recvOrDie(long bufLo, long bufHi, int timeout) {
                    String frag = fragments[fragIndex];
                    int fragLen = frag.length() - fragOffset;
                    int bufRemaining = (int) (bufHi - bufLo);

                    final int n;
                    final int o = fragOffset;
                    if (fragLen <= bufRemaining) {
                        fragIndex++;
                        fragOffset = 0;
                        n = fragLen;
                    } else {
                        fragOffset += bufRemaining;
                        n = bufRemaining;
                    }
                    for (int i = 0; i < n; i++) {
                        Unsafe.getUnsafe().putByte(bufLo + i, (byte) frag.charAt(o + i));
                    }
                    return n;
                }
            };

            StringSink sink = new StringSink();
            Fragment fragment;
            rsp.begin(mem, mem);
            while ((fragment = rsp.recv()) != null) {
                for (long p = fragment.lo(); p < fragment.hi(); p++) {
                    sink.put((char) Unsafe.getUnsafe().getByte(p));
                }
            }
            TestUtils.assertEquals(expected, sink);
        } finally {
            Unsafe.free(mem, memSize, MemoryTag.NATIVE_DEFAULT);
        }
    }

    private static String[] createChunks(Rnd rnd, String str, int chunkCount) {
        final ObjList<String> chunks = new ObjList<>();
        int len = str.length();
        int[] splits = new int[chunkCount + 1];
        splits[0] = 0;
        for (int i = 1; i < chunkCount; i++) {
            splits[i] = rnd.nextInt(len);
        }
        splits[chunkCount] = len;
        Arrays.sort(splits);
        for (int i = 0; i < chunkCount; i++) {
            int lo = splits[i];
            int hi = splits[i + 1];
            if (lo < hi) {
                chunks.add(str.substring(lo, hi));
            }
        }

        // copy non-zero len chunks
        String[] array = new String[chunks.size()];
        for (int i = 0, n = chunks.size(); i < n; i++) {
            array[i] = chunks.getQuick(i);
            Assert.assertNotEquals(0, array[i].length());
        }
        return array;
    }

    static {
        Os.init();
    }
}