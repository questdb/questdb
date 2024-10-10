/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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

import io.questdb.log.Log;
import io.questdb.metrics.LongGauge;
import io.questdb.std.Mutable;
import io.questdb.std.QuietCloseable;
import org.jetbrains.annotations.NotNull;

public abstract class IOContext<T extends IOContext<T>> implements Mutable, QuietCloseable {
    protected final Socket socket;
    private final LongGauge connectionCountGauge;
    protected long heartbeatId = -1;
    private int disconnectReason;
    // keep dispatcher private to avoid context scheduling itself multiple times
    private IODispatcher<T> dispatcher;

    protected IOContext(SocketFactory socketFactory, NetworkFacade nf, Log log, LongGauge connectionCountGauge) {
        this.socket = socketFactory.newInstance(nf, log);
        this.connectionCountGauge = connectionCountGauge;
    }

    @Override
    public void clear() {
        _clear();
    }

    public void clearSuspendEvent() {
        // no-op
    }

    @Override
    public void close() {
        _clear();
    }

    public long getAndResetHeartbeatId() {
        long id = heartbeatId;
        heartbeatId = -1;
        return id;
    }

    public int getDisconnectReason() {
        return disconnectReason;
    }

    public long getFd() {
        return socket != null ? socket.getFd() : -1;
    }

    public Socket getSocket() {
        return socket;
    }

    public SuspendEvent getSuspendEvent() {
        return null;
    }

    /**
     * @throws io.questdb.cairo.CairoException if initialization fails
     */
    public void init() {
        // no-op
    }

    public boolean invalid() {
        return socket.isClosed();
    }

    @SuppressWarnings("unchecked")
    public T of(long fd, @NotNull IODispatcher<T> dispatcher) {
        if (fd != -1) {
            connectionCountGauge.inc();
        }
        socket.of(fd);
        this.dispatcher = dispatcher;
        return (T) this;
    }

    public ServerDisconnectException registerDispatcherDisconnect(int reason) {
        disconnectReason = reason;
        return ServerDisconnectException.INSTANCE;
    }

    public HeartBeatException registerDispatcherHeartBeat() {
        return HeartBeatException.INSTANCE;
    }

    public PeerIsSlowToWriteException registerDispatcherRead() {
        return PeerIsSlowToWriteException.INSTANCE;
    }

    public PeerIsSlowToReadException registerDispatcherWrite() {
        return PeerIsSlowToReadException.INSTANCE;
    }

    public void setHeartbeatId(long heartbeatId) {
        this.heartbeatId = heartbeatId;
    }

    private void _clear() {
        if (socket.getFd() != -1) {
            connectionCountGauge.dec();
        }
        heartbeatId = -1;
        socket.close();
        dispatcher = null;
        disconnectReason = -1;
        clearSuspendEvent();
    }
}
