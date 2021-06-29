/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
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

package io.questdb.cutlass;

import io.questdb.network.Net;
import io.questdb.network.NetworkFacade;
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
            int port) {
        long clientFd = nf.socketTcp(true);
        long sockAddress = nf.sockaddr(Net.parseIPv4(ipv4Address), port);
        TestUtils.assertConnect(clientFd, sockAddress);

        final int N = 1024 * 1024;
        final long sendBuf = Unsafe.malloc(N);
        final long recvBuf = Unsafe.malloc(N);

        try {
            long sendPtr = sendBuf;
            boolean expectDisconnect = false;

            int mode = 0;
            int n = script.length();
            int i = 0;
            while (i < n) {
                char c1 = script.charAt(i);
                switch (c1) {
                    case '<':
                    case '>':
                        int len = (int) (sendPtr - sendBuf);
                        if (mode == 0) {
                            // we were sending - lets wrap up and send
                            if (len > 0) {
                                int m = nf.send(clientFd, sendBuf, len);
                                Assert.assertEquals("disc:" + expectDisconnect, len, m);
                                sendPtr = sendBuf;
                            }
                        } else {
                            // we meant to receive; sendBuf will contain expected bytes we have to receive
                            // and this buffer will also drive the length of the message
                            if (len > 0) {
                                int m = nf.recv(clientFd, recvBuf, len);
                                if (expectDisconnect) {
                                    Assert.assertTrue(m < 0);
                                    // force exit
                                    i = n;
                                } else {
//                                    Assert.assertEquals(len, m);
//                                    for (int j = 0; j < len; j++) {
//                                        Assert.assertEquals("at " + j,
//                                                Unsafe.getUnsafe().getByte(sendBuf + j),
//                                                Unsafe.getUnsafe().getByte(recvBuf + j)
//                                        );
//                                    }
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

            // this we do final receive (or send) when we don't have
            // any more script left to process
            int len = (int) (sendPtr - sendBuf);
            if (mode == 0) {
                // we were sending - lets wrap up and send
                if (len > 0) {
                    int m = nf.send(clientFd, sendBuf, len);
                    Assert.assertEquals(len, m);
                }
            } else {
                // we meant to receive; sendBuf will contain expected bytes we have to receive
                // and this buffer will also drive the length of the message
                if (len > 0 || expectDisconnect) {
                    if (expectDisconnect) {
                        Assert.assertTrue(Net.isDead(clientFd));
                    } else {
                        nf.recv(clientFd, recvBuf, len);
                    }
                }
            }
        } finally {
            Unsafe.free(sendBuf, N);
            Unsafe.free(recvBuf, N);
            nf.freeSockAddr(sockAddress);
            nf.close(clientFd);
        }
    }
}
