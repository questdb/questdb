/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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

import io.questdb.cutlass.line.tcp.DelegatingTlsChannel;
import io.questdb.cutlass.line.tcp.PlanTcpLineChannel;
import io.questdb.network.NetworkFacadeImpl;

import java.security.PrivateKey;

public class LineTcpSender extends AbstractLineSender {
    private static final int MIN_BUFFER_SIZE_FOR_AUTH = 512 + 1; // challenge size + 1;

    public LineTcpSender(int sendToIPv4Address, int sendToPort, int bufferCapacity) {
        super(new PlanTcpLineChannel(NetworkFacadeImpl.INSTANCE, sendToIPv4Address, sendToPort, bufferCapacity * 2), bufferCapacity);
    }

    public LineTcpSender(LineChannel channel, int bufferCapacity) {
        super(channel, bufferCapacity);
    }

    public static LineTcpSender authenticatedPlainTextSender(int sendToIPv4Address, int sendToPort, int bufferCapacity, String authKey, PrivateKey privateKey) {
        checkBufferCapacity(bufferCapacity);
        LineTcpSender sender = new LineTcpSender(sendToIPv4Address, sendToPort, bufferCapacity);
        sender.authenticate(authKey, privateKey);
        return sender;
    }

    public static LineTcpSender tlsSender(int sendToIPv4Address, int sendToPort, int bufferCapacity, String trustStorePath, char[] trustStorePassword) {
        LineChannel plainTcpChannel = new PlanTcpLineChannel(NetworkFacadeImpl.INSTANCE, sendToIPv4Address, sendToPort, bufferCapacity * 2);
        LineChannel tlsChannel = new DelegatingTlsChannel(plainTcpChannel, trustStorePath, trustStorePassword);
        return new LineTcpSender(tlsChannel, bufferCapacity);
    }

    public static LineTcpSender authenticatedTlsSender(int sendToIPv4Address, int sendToPort, int bufferCapacity, String username, PrivateKey token, String trustStorePath, char[] trustStorePassword) {
        checkBufferCapacity(bufferCapacity);
        LineTcpSender sender = tlsSender(sendToIPv4Address, sendToPort, bufferCapacity, trustStorePath, trustStorePassword);
        sender.authenticate(username, token);
        return sender;
    }

    private static void checkBufferCapacity(int capacity) {
        if (capacity < MIN_BUFFER_SIZE_FOR_AUTH) {
            throw new LineSenderException("Minimal buffer capacity is " + capacity + ". Requested buffer capacity: " + capacity);
        }
    }

    @Override
    public void flush() {
        sendAll();
    }

    @Override
    protected void send00() {
        sendAll();
    }
}
