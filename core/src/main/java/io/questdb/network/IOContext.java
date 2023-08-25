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

import io.questdb.std.Misc;
import io.questdb.std.Mutable;
import io.questdb.std.QuietCloseable;
import org.jetbrains.annotations.Nullable;

public abstract class IOContext<T extends IOContext<T>> implements Mutable, QuietCloseable {
    protected IODispatcher<T> dispatcher;
    protected long heartbeatId = -1;
    protected Socket socket;

    @Override
    public void clear() {
        assert socket == null : "socket must be closed before clear call";
        heartbeatId = -1;
        dispatcher = null;
    }

    public void clearSuspendEvent() {
        // no-op
    }

    public void closeSocket() {
        socket = Misc.free(socket);
    }

    public long getAndResetHeartbeatId() {
        long id = heartbeatId;
        heartbeatId = -1;
        return id;
    }

    public IODispatcher<T> getDispatcher() {
        return dispatcher;
    }

    public int getFd() {
        return socket != null ? socket.getFd() : -1;
    }

    public Socket getSocket() {
        return socket;
    }

    public SuspendEvent getSuspendEvent() {
        return null;
    }

    public boolean invalid() {
        return socket == null;
    }

    @SuppressWarnings("unchecked")
    public T of(@Nullable Socket socket, @Nullable IODispatcher<T> dispatcher) {
        this.socket = socket;
        this.dispatcher = dispatcher;
        return (T) this;
    }

    public void setHeartbeatId(long heartbeatId) {
        this.heartbeatId = heartbeatId;
    }
}
