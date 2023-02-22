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

import io.questdb.cutlass.line.udp.UdpLineChannel;
import io.questdb.network.NetworkFacade;
import io.questdb.network.NetworkFacadeImpl;

public class LineUdpSender extends AbstractLineSender {

    public LineUdpSender(int interfaceIPv4Address, int sendToIPv4Address, int sendToPort, int bufferCapacity, int ttl) {
        this(NetworkFacadeImpl.INSTANCE, interfaceIPv4Address, sendToIPv4Address, sendToPort, bufferCapacity, ttl);
    }

    public LineUdpSender(NetworkFacade nf, int interfaceIPv4Address, int sendToIPv4Address, int sendToPort, int capacity, int ttl) {
        super(new UdpLineChannel(nf, interfaceIPv4Address, sendToIPv4Address, sendToPort, ttl), capacity);
    }
}
