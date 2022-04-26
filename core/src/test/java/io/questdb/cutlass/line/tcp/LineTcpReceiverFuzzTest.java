/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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

package io.questdb.cutlass.line.tcp;

import io.questdb.griffin.SqlException;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.SOCountDownLatch;
import io.questdb.network.Net;
import io.questdb.std.Chars;
import io.questdb.std.MemoryTag;
import io.questdb.std.Os;
import io.questdb.std.Unsafe;
import io.questdb.test.tools.TestUtils;
import org.junit.Ignore;
import org.junit.Test;

import java.util.concurrent.CyclicBarrier;

public class LineTcpReceiverFuzzTest extends AbstractLineTcpReceiverFuzzTest {

    private static final Log LOG = LogFactory.getLog(LineTcpReceiverFuzzTest.class);

    @Test
    public void testAddColumns() throws Exception {
        initLoadParameters(15, 2, 2, 5, 75);
        initFuzzParameters(-1, -1, -1, 4, -1, false, true, false, false);
        runTest();
    }

    @Test
    public void testAddColumnsNoTagsStringsAsSymbol() throws Exception {
        initLoadParameters(15, 2, 2, 5, 75);
        initFuzzParameters(-1, -1, -1, 4, -1, false, false, true, false);
        runTest();
    }

    @Test
    @Ignore
    public void testCrash() {
        int n = 1;
        int k = 1;
        CyclicBarrier barrier = new CyclicBarrier(n);
        SOCountDownLatch doneLatch = new SOCountDownLatch(n);

        for (int i = 0; i < n; i++) {
            int a = i;
            new Thread(() -> {
                TestUtils.await(barrier);
                try {
                    po(k, a);
                } catch (SqlException e) {
                    e.printStackTrace();
                } finally {
                    doneLatch.countDown();
                }

            }).start();
        }

        doneLatch.await();

    }

    @Test
    public void testDuplicatesReorderingColumns() throws Exception {
        initLoadParameters(100, Os.type == Os.WINDOWS ? 3 : 5, 5, 5, 50);
        initFuzzParameters(4, 4, -1, -1, -1, true, true, false, false);
        runTest();
    }

    @Test
    public void testDuplicatesReorderingColumnsNoTagsStringsAsSymbol() throws Exception {
        initLoadParameters(100, Os.type == Os.WINDOWS ? 3 : 5, 5, 5, 50);
        initFuzzParameters(4, 4, -1, -1, -1, true, false, true, false);
        runTest();
    }

    @Test
    public void testDuplicatesReorderingColumnsSendSymbolsWithSpace() throws Exception {
        initLoadParameters(100, Os.type == Os.WINDOWS ? 3 : 5, 5, 5, 50);
        initFuzzParameters(4, 4, -1, -1, -1, true, true, false, true);
        runTest();
    }

    @Test
    public void testLoad() throws Exception {
        initLoadParameters(100, Os.type == Os.WINDOWS ? 3 : 5, 7, 12, 20);
        runTest();
    }

    @Test
    public void testLoadNoTagsStringsAsSymbol() throws Exception {
        initLoadParameters(100, Os.type == Os.WINDOWS ? 3 : 5, 7, 12, 20);
        initFuzzParameters(-1, -1, -1, -1, -1, false, false, true, false);
        runTest();
    }

    @Test
    public void testLoadSendSymbolsWithSpace() throws Exception {
        initLoadParameters(100, Os.type == Os.WINDOWS ? 3 : 5, 4, 8, 20);
        initFuzzParameters(-1, -1, -1, -1, -1, false, true, false, true);
        runTest();
    }

    @Test
    public void testReorderingAddSkipDuplicateColumnsWithNonAscii() throws Exception {
        initLoadParameters(100, Os.type == Os.WINDOWS ? 3 : 5, 5, 5, 50);
        initFuzzParameters(4, 4, 4, -1, 4, true, true, false, false);
        runTest();
    }

    @Test
    public void testReorderingAddSkipDuplicateColumnsWithNonAsciiNoTagsStringsAsSymbol() throws Exception {
        initLoadParameters(100, Os.type == Os.WINDOWS ? 3 : 5, 5, 5, 50);
        initFuzzParameters(4, 4, 4, -1, 4, true, false, true, false);
        runTest();
    }

    @Test
    public void testReorderingColumns() throws Exception {
        initLoadParameters(100, Os.type == Os.WINDOWS ? 3 : 5, 5, 5, 50);
        initFuzzParameters(-1, 4, -1, -1, -1, false, true, false, false);
        runTest();
    }

    @Test
    public void testReorderingColumnsNoTagsStringsAsSymbol() throws Exception {
        initLoadParameters(100, Os.type == Os.WINDOWS ? 3 : 5, 5, 5, 50);
        initFuzzParameters(-1, 4, -1, -1, -1, false, false, true, false);
        runTest();
    }

    @Override
    protected Log getLog() {
        return LOG;
    }

    private void po(int k, int a) throws SqlException {
        final String ilp = "conn pcap_id=\"a\" 1627046637414969856\n" +
                "conn pcap_id=0.07 1627046637414969856\n";
        final int len = ilp.length();
        long mem = Unsafe.malloc(len * 2, MemoryTag.NATIVE_DEFAULT);
        try {
            long fd = Net.socketTcp(true);
            if (fd != -1) {
                int i = 0;
                do {
                    if (Net.connect(fd, Net.sockaddr("127.0.0.1", 9009)) == 0) {
                        try {
                            boolean repeat = false;
                            for (; i < k; i++) {
                                Chars.asciiStrCpy(ilp, len, mem);
                                int sent = Net.send(fd, mem, len);
                                if (sent < 0) {
                                    i++;
                                    repeat = true;
                                    break;
                                }
                            }
                            if (repeat) {
                                continue;
                            }
                            break;
                        } finally {
                            Net.close(fd);
                        }
                    } else {
                        System.out.println("could not connect");
                        break;
                    }
                } while (true);
            } else {
                System.out.println("Could not open socket [errno=" + Os.errno() + "]");
            }
        } finally {
            Unsafe.free(mem, len * 2, MemoryTag.NATIVE_DEFAULT);
        }
    }
}
