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

package io.questdb.cutlass.auth;

import io.questdb.network.Socket;
import io.questdb.std.QuietCloseable;
import org.jetbrains.annotations.NotNull;

public interface Authenticator extends QuietCloseable {

    int NEEDS_DISCONNECT = 3;
    int NEEDS_READ = 0;
    int NEEDS_WRITE = 1;
    int OK = -1;
    int QUEUE_FULL = 2;

    default void clear() {
    }

    @Override
    default void close() {
    }

    default int denyAccess() throws AuthenticatorException {
        throw new UnsupportedOperationException();
    }

    CharSequence getPrincipal();

    long getRecvBufPos();

    long getRecvBufPseudoStart();

    int handleIO() throws AuthenticatorException;

    void init(@NotNull Socket socket, long recvBuffer, long recvBufferLimit, long sendBuffer, long sendBufferLimit);

    boolean isAuthenticated();

    default int loginOK() throws AuthenticatorException {
        throw new UnsupportedOperationException();
    }
}
