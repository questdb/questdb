/*
 * Copyright (c) 2014. Vlad Ilyushchenko
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.nfsdb.ha;

import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.RingBuffer;

import java.net.SocketAddress;

public class ServerLogMsg {

    public static final EventFactory<ServerLogMsg> EVENT_FACTORY = new EventFactory<ServerLogMsg>() {
        @Override
        public ServerLogMsg newInstance() {
            return new ServerLogMsg();
        }
    };

    long sequence;
    RingBuffer<ServerLogMsg> ringBuffer;
    private Level level;
    private String message;
    private Throwable throwable;
    private SocketAddress socketAddress;

    public Level getLevel() {
        return level;
    }

    public ServerLogMsg setLevel(Level level) {
        this.level = level;
        return this;
    }

    public String getMessage() {
        return message;
    }

    public ServerLogMsg setMessage(String message) {
        this.message = message;
        return this;
    }

    public SocketAddress getSocketAddress() {
        return socketAddress;
    }

    public ServerLogMsg setSocketAddress(SocketAddress address) {
        this.socketAddress = address;
        return this;
    }

    public Throwable getThrowable() {
        return throwable;
    }

    public void send() {
        ringBuffer.publish(sequence);
    }

    public ServerLogMsg setException(Throwable throwable) {
        this.throwable = throwable;
        return this;
    }

    public static enum Level {
        TRACE, DEBUG, INFO, ERROR
    }
}
