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

import io.questdb.cutlass.line.tcp.PlanTcpLineChannel;
import io.questdb.cutlass.line.tcp.OldTlsChannel;
import io.questdb.cutlass.line.udp.UdpLineChannel;
import io.questdb.network.NetworkFacade;

import java.io.Closeable;

public interface LineChannel extends Closeable {
    void send(long ptr, int len);
    int receive(long ptr, int len);

    int errno();

    static LineChannel newTcpChannel(NetworkFacade nf, int address, int port, int sndBufferSize) {
        return new PlanTcpLineChannel(nf, address, port, sndBufferSize);
    }

    static LineChannel newTlsChannel(int address, int port, int sndBufferSize) {
        return new OldTlsChannel(address, port, sndBufferSize);
    }

    static LineChannel newUdpChannel(NetworkFacade nf, int interfaceIPv4Address, int sendToAddress, int port, int ttl) {
        return new UdpLineChannel(nf, interfaceIPv4Address, sendToAddress, port, ttl);
    }
}
