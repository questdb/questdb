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

package io.questdb.cutlass.line.udp;

import io.questdb.network.Net;
import io.questdb.network.NetworkFacadeImpl;
import io.questdb.std.Os;
import org.junit.Ignore;
import org.junit.Test;

public class LineProtoSenderTest {

    @Test
    @Ignore
    public void testSimple() {
        try (LineProtoSender sender = new LineProtoSender(NetworkFacadeImpl.INSTANCE, 0, Net.parseIPv4("127.0.0.1"), 9009, 1024, 1)) {
            for (int i = 0; i < 50000000; i++) {
                sender.metric("weather").tag("location", "london").tag("by", "quest").field("temp", 3400).field("ok", 600000).$(Os.currentTimeMicros() * 1000);
            }
            sender.flush();
        }
    }
}