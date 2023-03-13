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

package org.questdb;

import io.questdb.cutlass.line.LineTcpSender;
import io.questdb.mp.SOCountDownLatch;
import io.questdb.network.Net;
import io.questdb.std.Os;
import io.questdb.std.Rnd;
import io.questdb.std.datetime.microtime.MicrosecondClock;
import io.questdb.std.datetime.microtime.MicrosecondClockImpl;

// Sends data in slow constant rate. Test case that commits still happen in QuestDB regularly.
public class LineTCPSenderMainLowConstantRate {
    public static void main(String[] args) {
        int n = 1;
        final SOCountDownLatch haltLatch = new SOCountDownLatch(n);
        for (int i = 0; i < n; i++) {
            int k = i;
            new Thread(() -> doSend(k)).start();
        }
        haltLatch.await();
    }

    private static void doSend(int k) {
        String hostIPv4 = "127.0.0.1";
        int port = 9009; // 8089 influx
        int bufferCapacity = 4 * 1024;

        final Rnd rnd = new Rnd();
        MicrosecondClock clock = new MicrosecondClockImpl();
        String tab = "weather";
        try (LineTcpSender sender = LineTcpSender.newSender(Net.parseIPv4(hostIPv4), port, bufferCapacity)) {
            while (true) {
                sender.metric(tab);
                sender
                        .tag("location", "london")
                        .tag("by", "blah")
                        .field("wind", Long.toString(rnd.nextPositiveLong()))
                        .field("temp", rnd.nextPositiveLong())
                        .field("ok", rnd.nextPositiveInt());
                sender.$(clock.getTicks() * 1000L);
                Os.pause();
                Os.pause();

                if (rnd.nextLong() % ((k + 1) * 10L) == 0) {
                    Os.sleep(1);
                }

                if (rnd.nextLong(50_000) == 0) {
                    Os.sleep(20);
                }
            }
        }
    }
}
