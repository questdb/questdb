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

import io.questdb.network.Net;
import io.questdb.std.Chars;
import io.questdb.std.MemoryTag;
import io.questdb.std.Os;
import io.questdb.std.Unsafe;

public class RawTCPILPSenderMain {
    public static void main(String[] args) {
        final String ilp = "dhl_plug,building=ba1aa618-4de4-40be-a4f7-72e341274b91,floor=a3979437-5d95-4f4d-8ef2-ace635cffce4,label=144c8537-db16-11e9-a374-0e9c62fb84fe,room=d534235d-d253-4aaa-8e40-eb8dc07c0d2b,socket_id=affbe16e-6a9e-4a06-85fe-e24430a1d8ba,address_id=8ca1d63a-d119-45aa-9747-351eb40bedf9 mac_address=\"xxxx\",milliamps=4,millijoules=151200,millivolts=120609,milliwatts=2520,duration=60587 1635189276608000\n";
        final int len = ilp.length();

        long mem = Unsafe.malloc(len, MemoryTag.NATIVE_DEFAULT);
        try {
            Chars.asciiStrCpy(ilp, len, mem);
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
