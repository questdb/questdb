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
 * Abstraction for plain and encrypted TCP sockets. Encrypted sockets use additional buffer
 * to accumulate messages, so they require extra calls to convert encrypted data to raw data.
 */
public interface Socket extends QuietCloseable {

    /**
     * @return file descriptor associated with the socket.
     */
    int getFd();

    boolean isTlsSessionStarted();

    /**
     * Reads encrypted data from the socket into internal buffer.
     * No-op on plain socket.
     *
     * @return non-negative value if there were no errors.
     */
    int read();

    /**
     * Receives plain data into the given buffer from the socket. On encrypted
     * sockets this call includes {@link #read()}, so an extra read() call
     * is not required.
     *
     * @param bufferPtr pointer to the buffer
     * @param bufferLen buffer length
     * @return recv() result; non-negative if there were no errors.
     */
    int recv(long bufferPtr, int bufferLen);

    /**
     * Sends plain data from the given buffer to the socket. On encrypted
     * sockets this call includes {@link #write()}, so an extra write() call
     * is not required.
     *
     * @param bufferPtr pointer to the buffer
     * @param bufferLen buffer length
     * @return send() result; non-negative if there were no errors.
     */
    int send(long bufferPtr, int bufferLen);

    /**
     * Does a shutdown() call on the socket.
     *
     * @param how valid shutdown flag, e.g. {@link Net#SHUT_WR}.
     */
    void shutdown(int how);

    boolean startTlsSession();

    boolean supportsTls();

    /**
     * @return true if a {@link #read()} call should be made once
     * the socket becomes readable.
     */
    boolean wantsRead();

    /**
     * @return true if a {@link #write()} call should be made once
     * the socket becomes writable.
     */
    boolean wantsWrite();

    /**
     * Writes encrypted data from the internal buffer to the socket.
     * No-op on plain socket.
     *
     * @return non-negative value if there were no errors.
     */
    int write();
}
