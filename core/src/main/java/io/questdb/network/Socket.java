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
 * <p>
 * {@link #close()} implementations must be idempotent. Also, supports object reuse after
 * {@link #close()}: see {@link #of(int)}.
 */
public interface Socket extends QuietCloseable {
    int READ_FLAG = 1 << 1;
    int WRITE_FLAG = 1;

    /**
     * @return file descriptor associated with the socket.
     */
    int getFd();

    boolean isClosed();

    /**
     * @return true if TLS session was already started.
     */
    boolean isTlsSessionStarted();

    /**
     * Sets the file descriptor associated with the socket.
     * The socket owns the fd after this call.
     *
     * @param fd file descriptor
     */
    void of(int fd);

    /**
     * Receives plain data into the given buffer from the socket. On encrypted
     * sockets this call includes {@link #tlsIO(int)}, so an extra tlsIO()
     * call is not required.
     *
     * @param bufferPtr pointer to the buffer
     * @param bufferLen buffer length
     * @return recv() result; non-negative if there were no errors.
     */
    int recv(long bufferPtr, int bufferLen);

    /**
     * Sends plain data from the given buffer to the socket. On encrypted
     * sockets this call includes {@link #tlsIO(int)}, so an extra tlsIO()
     * call is not required.
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
     * Reads or writes encrypted data to/from the internal buffer from/to
     * the socket. Can be called safely even if the socket doesn't
     * support TLS.
     *
     * @param readinessFlags socket readiness flags (see {@link #READ_FLAG}
     *                       and {@link #WRITE_FLAG}).
     * @return 0 if the call is successful; -1 if there was an error.
     */
    int tlsIO(int readinessFlags);

    /**
     * @return true if a {@link #tlsIO(int)} call should be made once
     * the socket becomes readable.
     */
    boolean wantsTlsRead();

    /**
     * @return true if a {@link #tlsIO(int)} call should be made once
     * the socket becomes writable.
     */
    boolean wantsTlsWrite();
}
