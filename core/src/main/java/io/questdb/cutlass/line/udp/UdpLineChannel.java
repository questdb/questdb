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

package io.questdb.cutlass.line.udp;

import io.questdb.cutlass.line.LineChannel;
import io.questdb.cutlass.line.LineSenderException;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.network.NetworkFacade;

public final class UdpLineChannel implements LineChannel {
    private static final Log LOG = LogFactory.getLog(UdpLineChannel.class);
    private final long fd;
    private final NetworkFacade nf;
    private final long sockaddr;

    public UdpLineChannel(NetworkFacade nf, int interfaceIPv4Address, int sendToAddress, int port, int ttl) {
        this.nf = nf;
        this.fd = nf.socketUdp();
        if (fd == -1) {
            throw new LineSenderException("could not create UDP socket").errno(nf.errno());
        }
        this.sockaddr = nf.sockaddr(sendToAddress, port);
        if (nf.setMulticastInterface(fd, interfaceIPv4Address) != 0) {
            final int errno = nf.errno();
            close();
            throw new LineSenderException("could not bind ")
                    .put("[ip=").appendIPv4(interfaceIPv4Address).put("]")
                    .errno(errno);
        }

        if (nf.setMulticastTtl(fd, ttl) != 0) {
            final int errno = nf.errno();
            close();
            throw new LineSenderException("could not set ttl ")
                    .put("[fd=").put(fd)
                    .put(", ttl=").put(ttl).put("]")
                    .errno(errno);
        }
    }

    @Override
    public void close() {
        if (nf.close(fd) != 0) {
            LOG.error().$("could not close network socket [fd=").$(fd).$(", errno=").$(nf.errno()).$(']').$();
        }
        nf.freeSockAddr(sockaddr);
    }

    @Override
    public int errno() {
        return nf.errno();
    }

    @Override
    public int receive(long ptr, int len) {
        throw new UnsupportedOperationException("Udp channel does not support receive()");
    }

    @Override
    public void send(long ptr, int len) {
        if (nf.sendToRaw(fd, ptr, len, sockaddr) != len) {
            throw new LineSenderException("send error").errno(nf.errno());
        }
    }
}
