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

package io.questdb.network;

import io.questdb.std.Unsafe;

public class NetworkUtils {

    private NetworkUtils() {
    }

    public static boolean testConnection(NetworkFacade nf, int fd, long buffer, int bufferSize) {
        if (fd == -1) {
            return true;
        }

        final int nRead = nf.peek(fd, buffer, bufferSize);

        if (nRead == 0) {
            return false;
        }

        if (nRead < 0) {
            return true;
        }

        // Read \r\n from the input stream and discard it since some HTTP clients
        // send these chars as a keep alive in between requests.
        int index = 0;
        while (index < nRead) {
            byte b = Unsafe.getUnsafe().getByte(buffer + index);
            if (b != (byte) '\r' && b != (byte) '\n') {
                break;
            }
            index++;
        }

        if (index > 0) {
            nf.recv(fd, buffer, index);
        }

        return false;
    }
}
