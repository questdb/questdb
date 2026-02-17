/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2026 QuestDB
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
import io.questdb.std.Mutable;
import io.questdb.std.QuietCloseable;
import org.jetbrains.annotations.NotNull;

public abstract class IOContext<T extends IOContext<T>> implements Mutable, QuietCloseable {
    protected final Socket socket;
    protected long heartbeatId = -1;
    private int disconnectReason;
    private volatile boolean initialized = false;

    // IMPORTANT: Keep subclass constructors lightweight!
    // Under high load, new context objects are created for each accepted connection.
    // Since connection acceptance runs on a single thread, slow constructors can
    // significantly degrade performance and throttle incoming connections.
    // To avoid this, defer context initialization to the doInit() method.
    protected IOContext(@NotNull SocketFactory socketFactory, NetworkFacade nf, Log log) {
        this.socket = socketFactory.newInstance(nf, log);
    }

    @Override
    public void clear() {
        _clear();
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

    public final void init() throws TlsSessionInitFailedException {
        if (!initialized) {
            doInit();
            initialized = true;
        }
    }

    public boolean invalid() {
        return socket.isClosed();
    }

    @SuppressWarnings("unchecked")
    public final T of(long fd) {
        socket.of(fd);
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
        heartbeatId = -1;
        socket.close();
        disconnectReason = -1;
        initialized = false;
    }

    /**
     * Initializes the state of the context object.
     * <p>
     * Override this method to perform any setup required.
     * Note that this method is called once per connection, but not on the thread
     * that accepts connections. Instead, it is invoked when the first I/O event
     * is dispatched for this context.
     * <p>
     * Avoid placing initialization logic in the context's constructor, as it may
     * negatively impact the database's ability to accept new connections under high load.
     *
     * @throws io.questdb.cairo.CairoException if initialization fails
     */
    protected void doInit() throws TlsSessionInitFailedException {
    }
}
