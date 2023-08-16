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

package io.questdb.network;

import io.questdb.std.QuietCloseable;

/**
 * Abstraction for plain and encrypted TCP sockets.
 */
public interface Socket extends QuietCloseable {
    int INIT_DONE = 0;
    int WANTS_READ = 2;
    int WANTS_WRITE = 1;

    int getFd();

    /**
     * Initialize socket for communication. The method has to be called (potentially,
     * multiple times) before the first call to {@link #recv(long, int)} and
     * {@link #send(long, int)} until {@link #INIT_DONE} is returned.
     *
     * @return one of the following values:
     * <ul>
     *     <li>{@link #INIT_DONE} - initialization is done</li>
     *     <li>{@link #WANTS_READ} - a read for the socket is required</li>
     *     <li>{@link #WANTS_WRITE} - a write to the socket is required</li>
     * </ul>
     */
    int init();

    int recv(long bufferPtr, int bufferLen);

    int send(long bufferPtr, int bufferLen);

    void shutdown(int how);
}
