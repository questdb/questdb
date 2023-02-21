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

package io.questdb.cutlass.line;

import io.questdb.client.Sender;
import io.questdb.cutlass.line.tcp.PlainTcpLineChannel;
import io.questdb.network.NetworkFacadeImpl;

/**
 * LineTcpSender is for testing purposes only. It has error-prone API and comes with no API guarantees
 * If you are looking for an ILP client for your application use {@link Sender} instead.
 */
public class LineTcpSender extends AbstractLineSender {

    /**
     * @param ip             IP address of a server
     * @param port           port where a server is listening
     * @param bufferCapacity capacity of an internal buffer in bytes
     * @deprecated use {@link #newSender(int, int, int)} instead.
     * <br>
     * IP address is encoded as <code>int</code> obtained via {@link io.questdb.network.Net#parseIPv4(CharSequence)}
     */
    @Deprecated
    public LineTcpSender(int ip, int port, int bufferCapacity) {
        super(new PlainTcpLineChannel(NetworkFacadeImpl.INSTANCE, ip, port, bufferCapacity * 2), bufferCapacity);
    }

    public LineTcpSender(LineChannel channel, int bufferCapacity) {
        super(channel, bufferCapacity);
    }

    /**
     * Create a new LineTcpSender.
     * <br>
     * IP address is encoded as <code>int</code> obtained via {@link io.questdb.network.Net#parseIPv4(CharSequence)}
     * <br>
     * This is meant to be used for testing only, it's not something most users want to use.
     * See {@link Sender} instead
     *
     * @param ip             IP address of a server
     * @param port           port where a server is listening
     * @param bufferCapacity capacity of an internal buffer in bytes
     * @return LineTcpSender instance of LineTcpSender
     */
    public static LineTcpSender newSender(int ip, int port, int bufferCapacity) {
        PlainTcpLineChannel channel = new PlainTcpLineChannel(NetworkFacadeImpl.INSTANCE, ip, port, bufferCapacity * 2);
        try {
            return new LineTcpSender(channel, bufferCapacity);
        } catch (Throwable t) {
            channel.close();
            throw t;
        }
    }

    @Override
    public void flush() {
        validateNotClosed();
        sendAll();
    }

    @Override
    protected void send00() {
        sendAll();
    }
}
