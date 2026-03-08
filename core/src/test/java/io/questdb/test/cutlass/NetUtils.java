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

package io.questdb.test.cutlass;

import io.questdb.network.Net;
import io.questdb.network.NetworkFacade;
import io.questdb.std.MemoryTag;
import io.questdb.std.Numbers;
import io.questdb.std.NumericException;
import io.questdb.std.Unsafe;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;

public class NetUtils {

    public static void playScript(
            NetworkFacade nf,
            String script,
            CharSequence ipv4Address,
            int port
    ) {
        long clientFd = nf.socketTcp(true);
        long sockAddress = nf.sockaddr(Net.parseIPv4(ipv4Address), port);
        TestUtils.assertConnect(clientFd, sockAddress);

        final int N = 1024 * 1024;
        final long sendBuf = Unsafe.malloc(N, MemoryTag.NATIVE_DEFAULT);
        final long recvBuf = Unsafe.malloc(N, MemoryTag.NATIVE_DEFAULT);

        try {
            long sendPtr = sendBuf;
            boolean expectDisconnect = false;

            int line = 0;
            int mode = 0;
            int n = script.length();
            int i = 0;
            while (i < n) {
                char c1 = script.charAt(i);
                switch (c1) {
                    case '<':
                    case '>':
                        int expectedLen = (int) (sendPtr - sendBuf);
                        if (mode == 0) {
                            // we were sending - lets wrap up and send
                            if (expectedLen > 0) {
                                int m = nf.sendRaw(clientFd, sendBuf, expectedLen);
                                // if we expect disconnect we might get it on either `send` or `recv`
                                // check if we expect disconnect on recv?
                                if (m == -2 && script.charAt(i + 1) == '!' && script.charAt(i + 2) == '!') {
                                    // force exit
                                    i = n;
                                } else {
                                    Assert.assertEquals("disc:" + expectDisconnect, expectedLen, m);
                                    sendPtr = sendBuf;
                                }
                            }
                        } else {
                            // we meant to receive; sendBuf will contain expected bytes we have to receive
                            // and this buffer will also drive the length of the message
                            if (expectedLen > 0) {
                                int actualLen = nf.recvRaw(clientFd, recvBuf, expectedLen);
                                if (expectDisconnect) {
                                    Assert.assertTrue(actualLen < 0);
                                    // force exit
                                    i = n;
                                } else {
                                    assertBuffers(line, sendBuf, expectedLen, recvBuf, actualLen);
                                    // clear sendBuf
                                    sendPtr = sendBuf;
                                }
                            }
                        }

                        if (c1 == '<') {
                            mode = 1;
                        } else {
                            mode = 0;
                        }
                        i++;
                        continue;
                    case '\n':
                        i++;
                        line++;
                        continue;
                    default:
                        char c2 = script.charAt(i + 1);
                        if (c1 == '!' && c2 == '!') {
                            expectDisconnect = true;
                        } else {
                            try {
                                byte b = (byte) ((Numbers.hexToDecimal(c1) << 4) | Numbers.hexToDecimal(c2));
                                Unsafe.getUnsafe().putByte(sendPtr++, b);
                            } catch (NumericException e) {
                                e.printStackTrace();
                            }
                        }
                        i += 2;
                        break;
                }
            }

            // here we do final receive (or send) when we don't have
            // any more script left to process
            int expectedLen = (int) (sendPtr - sendBuf);
            if (mode == 0) {
                // we were sending - lets wrap up and send
                if (expectedLen > 0) {
                    int m = nf.sendRaw(clientFd, sendBuf, expectedLen);
                    Assert.assertEquals(expectedLen, m);
                }
            } else {
                // we meant to receive; sendBuf will contain expected bytes we have to receive
                // and this buffer will also drive the length of the message
                if (expectedLen > 0 || expectDisconnect) {
                    if (expectDisconnect) {
                        Assert.assertTrue(Net.isDead(clientFd));
                    } else {
                        int actualLen = nf.recvRaw(clientFd, recvBuf, expectedLen);
                        assertBuffers(line, sendBuf, expectedLen, recvBuf, actualLen);
                    }
                }
            }
        } finally {
            Unsafe.free(sendBuf, N, MemoryTag.NATIVE_DEFAULT);
            Unsafe.free(recvBuf, N, MemoryTag.NATIVE_DEFAULT);
            nf.freeSockAddr(sockAddress);
            nf.close(clientFd);
        }
    }

    private static void assertBuffers(int line, long expectedBuf, int expectedLen, long actualBuf, int actualLen) {
        Assert.assertEquals(expectedLen, actualLen);
        for (int j = 0; j < expectedLen; j++) {
            if (Unsafe.getUnsafe().getByte(expectedBuf + j) != Unsafe.getUnsafe().getByte(actualBuf + j)) {
                Assert.fail("line = " + line + ", pos = " + j + ", expected: " + Unsafe.getUnsafe().getByte(expectedBuf + j) + ", actual: " + Unsafe.getUnsafe().getByte(actualBuf + j));
            }
        }
    }
}
