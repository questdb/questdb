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

package org.questdb;

import io.questdb.cutlass.line.LineProtoSender;
import io.questdb.cutlass.line.tcp.LineTCPProtoSender;
import io.questdb.network.Net;
import io.questdb.std.Os;
import io.questdb.std.Rnd;

public class LineTCPSenderMain {
    public static void main(String[] args) {
        final long count = 50_000_000;
        String hostIPv4 = "127.0.0.1";
        int port = 9009; // 8089 influx
        int bufferCapacity = 256 * 1024;

        final Rnd rnd = new Rnd();
        long start = System.nanoTime();
        try (LineProtoSender sender = new LineTCPProtoSender(Net.parseIPv4(hostIPv4), port, bufferCapacity)) {
            for (int i = 0; i < count; i++) {
                // if ((i & 0x1) == 0) {
                    sender.metric("weather1");
                    // } else {
                    // sender.metric("weather2");
                    // }
                sender.tag("location", "l ondon").tag("by", "quest").field("temp", rnd.nextPositiveLong()).field("ok", rnd.nextPositiveInt()).$(Os.currentTimeMicros() * 1000);
            }
            sender.flush();
        }
        System.out.println("Actual rate: " + (count * 1_000_000_000L / (System.nanoTime() - start)));
    }
}
