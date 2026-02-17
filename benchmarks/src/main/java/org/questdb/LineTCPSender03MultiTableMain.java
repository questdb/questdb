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

package org.questdb;

import io.questdb.cutlass.line.AbstractLineTcpSender;
import io.questdb.cutlass.line.LineTcpSenderV2;
import io.questdb.mp.SOCountDownLatch;
import io.questdb.network.Net;
import io.questdb.std.Rnd;
import io.questdb.std.datetime.Clock;
import io.questdb.std.datetime.microtime.MicrosecondClockImpl;

public class LineTCPSender03MultiTableMain {
    private static final long DAY_START_US = 1_770_854_400_000_000L; // 2026-02-12T00:00:00Z in micros
    private static final long MICROS_PER_DAY = 24 * 60 * 60 * 1_000_000L;

    public static void main(String[] args) {
        int[] tables = new int[]{3};
        final SOCountDownLatch haltLatch = new SOCountDownLatch(tables.length);
        for (int i = 0; i < tables.length; i++) {
            int k = tables[i];
            new Thread(() -> doSend(k, haltLatch)).start();
        }
        haltLatch.await();
    }

    private static void doSend(int k, SOCountDownLatch haltLatch) {
        String hostIPv4 = "127.0.0.1";
        int port = 9009; // 8089 influx
        int bufferCapacity = 4 * 1024;

        final Rnd rnd = new Rnd();
        Clock clock = new MicrosecondClockImpl();
        String tab = "weather" + k;
        try (AbstractLineTcpSender sender = LineTcpSenderV2.newSender(Net.parseIPv4(hostIPv4), port, bufferCapacity)) {
            while (true) {
                sender.metric(tab);
                sender
                        .tag("location", "london")
                        .tag("by", "blah")
                        .field("temp", rnd.nextPositiveLong())
                        .field("ok", rnd.nextPositiveInt());
                final long ticks = DAY_START_US + clock.getTicks() % MICROS_PER_DAY;
                final long ts = ticks * 1000L + rnd.nextLong(1_000_000_000) - 500_000_000;
                sender.$(ts);
            }
        } finally {
            haltLatch.countDown();
        }
    }
}
