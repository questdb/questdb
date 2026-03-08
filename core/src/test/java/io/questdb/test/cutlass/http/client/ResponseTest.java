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


import io.questdb.cutlass.http.client.AbstractResponse;
import io.questdb.cutlass.http.client.Fragment;
import io.questdb.std.MemoryTag;
import io.questdb.std.Os;
import io.questdb.std.Unsafe;
import io.questdb.std.str.StringSink;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class ResponseTest {

    @Test
    public void testNoSplit() {
        String[] expectedFragments = {
                "abcdefghjklzxnmd0123456789"
        };
        String[] actualFragments = {
                "abcdefghjklzxnmd0123456789"
        };
        assertResponse(expectedFragments, actualFragments);
    }

    @Test
    public void testSplit1() {
        String[] expectedFragments = {
                "abcdefghjklzxnmd",
                "0123456789"
        };
        String[] actualFragments = {
                "abcdefghjklzxnmd",
                "0123456789"
        };
        assertResponse(expectedFragments, actualFragments);
    }

    @Test
    public void testSplit2() {
        String[] expectedFragments = {
                "abcdefghjklzxnmd0123456789"
        };
        String[] actualFragments = {
                "",
                "",
                "",
                "",
                "abcdefghjklzxnmd0123456789"
        };
        assertResponse(expectedFragments, actualFragments);
    }

    @Test
    public void testSplit3() {
        String[] expectedFragments = {
                "abcdefg",
                "hjklzxnmd",
                "0123456789"
        };
        String[] actualFragments = {
                "abcdefg",
                "hjklzxnmd",
                "0123456789"
        };
        assertResponse(expectedFragments, actualFragments);
    }

    private static void assertResponse(String[] expectedFragments, String[] actualFragments) {
        long memSize = 4096;
        long mem = Unsafe.malloc(memSize, MemoryTag.NATIVE_DEFAULT);
        try {
            final AbstractResponse rsp = new AbstractResponse(mem, mem + memSize, -1) {
                int fragIndex = 0;
                int fragOffset = 0;

                @Override
                protected int recvOrDie(long bufLo, long bufHi, int timeout) {
                    String frag = actualFragments[fragIndex];
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
            for (String expectedFragment : expectedFragments) {
                rsp.begin(mem, mem, expectedFragment.length());
                Fragment fragment = rsp.recv();
                Assert.assertNotNull(fragment);
                sink.clear();
                for (long p = rsp.lo(); p < rsp.hi(); p++) {
                    sink.put((char) Unsafe.getUnsafe().getByte(p));
                }
                TestUtils.assertEquals(expectedFragment, sink);
            }
            Assert.assertNull(rsp.recv());
        } finally {
            Unsafe.free(mem, memSize, MemoryTag.NATIVE_DEFAULT);
        }
    }

    static {
        Os.init();
    }
}