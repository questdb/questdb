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

package org.questdb;

import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.network.Net;
import io.questdb.std.Chars;
import io.questdb.std.MemoryTag;
import io.questdb.std.Os;
import io.questdb.std.Unsafe;
import io.questdb.std.datetime.microtime.Timestamps;
import io.questdb.std.str.DirectByteCharSequence;

public class PingMain {
    private static final Log LOG = LogFactory.getLog(PingMain.class);
    public static final String PING = "PING";
    public static final String PONG = "PONG";

    public static void main(String[] args) {
        String host = "127.0.0.1";
        int port = 9001;
        long durationSec = 60;
        long delayMillis = 10;
        long bufSize = 1024;

        // blocking client for simplicity
        long fd = Net.socketTcp(true);
        // DNS resolution is provided by the OS
        long inf = Net.getAddrInfo(host, port);
        // attempt to connect
        int res = Net.connectAddrInfo(fd, inf);
        if (res != 0) {
            Net.freeAddrInfo(inf);
            LOG.error()
                    .$("could not connect [host=").$(host)
                    .$(", port=").$(port)
                    .$(", errno=").$(Os.errno())
                    .I$();
        } else {
            long buf = Unsafe.malloc(bufSize, MemoryTag.NATIVE_DEFAULT);
            DirectByteCharSequence flyweight = new DirectByteCharSequence();

            long durationUs = durationSec * Timestamps.SECOND_MICROS;
            long startUs = Os.currentTimeMicros();
            while (Os.currentTimeMicros() - durationUs < startUs) {
                Chars.asciiStrCpy(PING, buf);
                int n = Net.send(fd, buf, PING.length());
                assert n == PING.length();

                n = Net.recv(fd, buf, PONG.length());
                assert n == PONG.length();
                LOG.info().$(flyweight.of(buf, buf + PONG.length())).$();
                Os.sleep(delayMillis);
            }
        }
        LogFactory.INSTANCE.flushJobsAndClose();
    }
}
