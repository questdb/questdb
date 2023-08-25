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

    /**
     * @return true if TLS session was already started.
     */
    boolean isTlsSessionStarted();

    /**
     * Receives plain data into the given buffer from the socket. On encrypted
     * sockets this call includes {@link #readTls()}, so an extra readTls()
     * call is not required.
     *
     * @param bufferPtr pointer to the buffer
     * @param bufferLen buffer length
     * @return recv() result; non-negative if there were no errors.
     */
    int read(long bufferPtr, int bufferLen);

    /**
     * Reads encrypted data from the socket into internal buffer.
     * Should never be called on plain socket.
     *
     * @return non-negative value if there were no errors.
     */
    int readTls();

    /**
     * Does a shutdown() call on the socket.
     *
     * @param how valid shutdown flag, e.g. {@link Net#SHUT_WR}.
     * @return 0 if the call is successful; -1 if there was an error.
     */
    int shutdown(int how);

    /**
     * Starts a TLS session, if supported.
     *
     * @return 0 if the call is successful; -1 if there was an error.
     */
    int startTlsSession();

    /**
     * @return true if the socket support TLS encryption; false otherwise.
     */
    boolean supportsTls();

    /**
     * @return true if a {@link #readTls()} call should be made once
     * the socket becomes readable.
     */
    boolean wantsRead();

    /**
     * @return true if a {@link #writeTls()} call should be made once
     * the socket becomes writable.
     */
    boolean wantsWrite();

    /**
     * Sends plain data from the given buffer to the socket. On encrypted
     * sockets this call includes {@link #writeTls()}, so an extra writeTls()
     * call is not required.
     *
     * @param bufferPtr pointer to the buffer
     * @param bufferLen buffer length
     * @return send() result; non-negative if there were no errors.
     */
    int write(long bufferPtr, int bufferLen);

    /**
     * Writes encrypted data from the internal buffer to the socket.
     * Should never be called on plain socket.
     *
     * @return non-negative value if there were no errors.
     */
    int writeTls();
}
