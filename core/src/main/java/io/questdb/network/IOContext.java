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

import io.questdb.std.Mutable;
import io.questdb.std.QuietCloseable;

public abstract class IOContext<T extends IOContext<T>> implements Mutable, QuietCloseable {
    protected IODispatcher<T> dispatcher;
    protected int fd = -1;
    protected long heartbeatId = -1;

    @Override
    public void clear() {
        heartbeatId = -1;
        fd = -1;
        dispatcher = null;
    }

    public void clearSuspendEvent() {
        // no-op
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
        return fd;
    }

    public SuspendEvent getSuspendEvent() {
        return null;
    }

    public boolean invalid() {
        return fd == -1;
    }

    @SuppressWarnings("unchecked")
    public T of(int fd, IODispatcher<T> dispatcher) {
        this.fd = fd;
        this.dispatcher = dispatcher;
        return (T) this;
    }

    public void setHeartbeatId(long heartbeatId) {
        this.heartbeatId = heartbeatId;
    }
}
