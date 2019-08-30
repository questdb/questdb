/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2019 Appsicle
 *
 * This program is free software: you can redistribute it and/or  modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 ******************************************************************************/

package io.questdb.cutlass;

import io.questdb.network.Net;
import io.questdb.network.NetworkFacade;
import io.questdb.std.Numbers;
import io.questdb.std.NumericException;
import io.questdb.std.Unsafe;
import org.junit.Assert;

public class NetUtils {
    public static void playScript(
            NetworkFacade nf,
            String script,
            CharSequence ipv4Address,
            int port) {
        long clientFd = nf.socketTcp(true);
        long sockAddress = nf.sockaddr(Net.parseIPv4(ipv4Address), port);
        Assert.assertEquals(0, nf.connect(clientFd, sockAddress));

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
                                    Assert.assertEquals(len, m);
                                    for (int j = 0; j < len; j++) {
                                        Assert.assertEquals("at " + j,
                                                Unsafe.getUnsafe().getByte(sendBuf + j),
                                                Unsafe.getUnsafe().getByte(recvBuf + j)
                                        );
                                    }
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
                        int m = nf.recv(clientFd, recvBuf, len);
                        Assert.assertEquals(len, m);
                        for (int j = 0; j < len; j++) {
                            Assert.assertEquals(
                                    Unsafe.getUnsafe().getByte(sendBuf + j),
                                    Unsafe.getUnsafe().getByte(recvBuf + j)
                            );
                        }
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
