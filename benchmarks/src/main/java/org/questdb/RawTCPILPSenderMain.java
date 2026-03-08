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

import io.questdb.network.Net;
import io.questdb.std.MemoryTag;
import io.questdb.std.Os;
import io.questdb.std.Unsafe;
import io.questdb.std.str.Utf8s;

public class RawTCPILPSenderMain {
    public static void main(String[] args) {
        final String ilp = "vbw water_speed_longitudinal=0.07,water_speed_traversal=,water_speed_status=\"A\",ground_speed_longitudinal=0,ground_speed_traversal=0,ground_speed_status=\"A\",water_speed_stern_traversal=,water_speed_stern_traversal_status=\"V\",ground_speed_stern_traversal=0,ground_speed_stern_traversal_status=\"V\" 1627046637414969856\n";
        final int len = ilp.length();

        long mem = Unsafe.malloc(len, MemoryTag.NATIVE_DEFAULT);
        try {
            Utf8s.strCpyAscii(ilp, len, mem);
            long fd = Net.socketTcp(true);
            if (fd != -1) {
                if (Net.connect(fd, Net.sockaddr("127.0.0.1", 9009)) == 0) {
                    try {
                        int sent = Net.send(fd, mem, len);
                        System.out.println("Sent " + sent + " out of " + len);
                    } finally {
                        Net.close(fd);
                    }
                } else {
                    System.out.println("could not connect");
                }
            } else {
                System.out.println("Could not open socket [errno=" + Os.errno() + "]");
            }
        } finally {
            Unsafe.free(mem, len, MemoryTag.NATIVE_DEFAULT);
        }

    }
}
