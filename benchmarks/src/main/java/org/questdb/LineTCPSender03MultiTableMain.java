/*+*****************************************************************************
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

import io.questdb.client.cutlass.line.AbstractLineTcpSender;
import io.questdb.client.cutlass.line.LineTcpSenderV2;
import io.questdb.mp.SOCountDownLatch;
import io.questdb.network.Net;
import io.questdb.std.Rnd;
import io.questdb.std.datetime.Clock;
import io.questdb.std.datetime.microtime.MicrosecondClockImpl;

public class LineTCPSender03MultiTableMain {
    private static final long MAX_STREAM_OFFSET_MICROS = 60_000_000L; // 1 minute
    private static final int STREAM_COUNT = 3;
    private static final String TABLE_NAME = "weather3";

    public static void main(String[] args) {
        final SOCountDownLatch haltLatch = new SOCountDownLatch(STREAM_COUNT);
        for (int i = 0; i < STREAM_COUNT; i++) {
            // Evenly spread across the 1-minute window, so the oldest stream starts exactly
            // MAX_STREAM_OFFSET_MICROS behind the newest.
            final long streamOffsetMicros = i * (MAX_STREAM_OFFSET_MICROS / (STREAM_COUNT - 1));
            new Thread(() -> doSend(streamOffsetMicros, haltLatch)).start();
        }
        haltLatch.await();
    }

    /**
     * One stream into the shared table. Its own timestamps only ever move forward - unlike the old
     * per-row jitter, which could land anywhere within a window and so was out of order even within
     * one stream - so the out-of-order shape merge-append exists for comes only from interleaving
     * the three streams' otherwise-ordered data against each other, staggered by up to a minute.
     */
    private static void doSend(long streamOffsetMicros, SOCountDownLatch haltLatch) {
        String hostIPv4 = "127.0.0.1";
        int port = 9009; // 8089 influx
        int bufferCapacity = 4 * 1024;

        final Rnd rnd = new Rnd();
        Clock clock = new MicrosecondClockImpl();
        try (AbstractLineTcpSender sender = LineTcpSenderV2.newSender(Net.parseIPv4(hostIPv4), port, bufferCapacity)) {
            while (true) {
                long ts = clock.getTicks() * 1000L - streamOffsetMicros + rnd.nextLong(1_000_000);
                sender.metric(TABLE_NAME);
                sender
                        .tag("location", "london")
                        .tag("by", "blah")
                        .field("temp", rnd.nextPositiveLong())
                        .field("ok", rnd.nextPositiveInt());
                sender.$(ts);
            }
        } finally {
            haltLatch.countDown();
        }
    }
}
