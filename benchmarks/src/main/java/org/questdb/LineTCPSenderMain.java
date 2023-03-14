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
import io.questdb.network.NoSpaceLeftInResponseBufferException;
import io.questdb.std.NanosecondClock;
import io.questdb.std.NanosecondClockImpl;
import io.questdb.std.Os;
import io.questdb.std.Rnd;
import io.questdb.std.datetime.microtime.MicrosecondClock;
import io.questdb.std.datetime.microtime.MicrosecondClockImpl;

public class LineTCPSenderMain {
    public static void main(String[] args) {
        int n = 6;
        final SOCountDownLatch haltLatch = new SOCountDownLatch(n);
        for (int i = 0; i < n; i++) {
            int k = i;
            new Thread(() -> doSend(k, haltLatch)).start();
        }
        haltLatch.await();
    }

    private static void doSend(int k, SOCountDownLatch haltLatch) {
        final long count = 300_000_000L;
        String hostIPv4 = "127.0.0.1";
        int port = 9009; // 8089 influx
        int bufferCapacity = 4 * 1024;

        final Rnd rnd = new Rnd();
        long start = System.nanoTime();
        String tab = "weather";
        MicrosecondClock nanosecondClock = new MicrosecondClockImpl();

        long sinceClose = nanosecondClock.getTicks();
        LineTcpSender sender = LineTcpSender.newSender(Net.parseIPv4(hostIPv4), port, bufferCapacity);
        for (int i = 0; i < count; i++) {
            sender.metric(tab);
            sender
                    .tag("location", "london")
                    .tag("by", rnd.nextString(4))
                    .tag("by2", rnd.nextString(4))
                    .tag("by3", rnd.nextString(4))
                    .field("temp", rnd.nextPositiveLong())
                    .field("str", rnd.nextString(10))
                    .field("str2", rnd.nextString(10))
                    .field("str3", rnd.nextString(10))
                    .field("l", rnd.nextLong())
                    .field("l2", rnd.nextLong())
                    .field("l3", rnd.nextLong())
                    .field("l4", rnd.nextLong())
                    .field("d1", rnd.nextDouble())
                    .field("d2", rnd.nextDouble())
                    .field("d3", rnd.nextDouble())
                    .field("d4", rnd.nextDouble())
                    .field("ok", rnd.nextPositiveInt());
            sender.$(nanosecondClock.getTicks() * 1000L);
            Os.pause();
            if (rnd.nextLong(50) == 10) {
                Os.sleep(50);
            } else {
                Os.sleep(1);
            }
            if (nanosecondClock.getTicks() - sinceClose > 1_000_000L) {
                sender.close();
                sender = LineTcpSender.newSender(Net.parseIPv4(hostIPv4), port, bufferCapacity);
                sinceClose = nanosecondClock.getTicks();
            }
        }

        sender.close();
        System.out.println("Actual rate: " + (count * 1_000_000_000L / (System.nanoTime() - start)));
        haltLatch.countDown();
    }
}
