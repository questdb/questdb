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

package io.questdb.cutlass.line.tcp;

import io.questdb.cutlass.line.LineChannel;
import io.questdb.cutlass.line.LineSenderException;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.network.NetworkFacade;

public final class PlainTcpLineChannel implements LineChannel {
    private static final Log LOG = LogFactory.getLog(PlainTcpLineChannel.class);
    private final long fd;
    private final NetworkFacade nf;
    private final long sockaddr;

    public PlainTcpLineChannel(NetworkFacade nf, int address, int port, int sndBufferSize) {
        this.nf = nf;
        this.fd = nf.socketTcp(true);
        if (fd < 0) {
            throw new LineSenderException("could not allocate a file descriptor").errno(nf.errno());
        }
        nf.configureKeepAlive(fd);
        this.sockaddr = nf.sockaddr(address, port);
        if (nf.connect(fd, sockaddr) != 0) {
            int errno = nf.errno();
            nf.close(fd, LOG);
            nf.freeSockAddr(sockaddr);
            throw new LineSenderException("could not connect to host ")
                    .put("[ip=").appendIPv4(address).put("]")
                    .errno(errno);
        }
        configureBuffers(nf, sndBufferSize);
    }

    public PlainTcpLineChannel(NetworkFacade nf, CharSequence host, int port, int sndBufferSize) {
        this.nf = nf;
        this.sockaddr = -1;
        this.fd = nf.socketTcp(true);
        if (fd < 0) {
            throw new LineSenderException("could not allocate a file descriptor").errno(nf.errno());
        }
        nf.configureKeepAlive(fd);
        long addrInfo = nf.getAddrInfo(host, port);
        if (addrInfo == -1) {
            nf.close(fd, LOG);
            throw new LineSenderException("could not resolve host ")
                    .put("[host=").put(host).put("]");
        }
        if (nf.connectAddrInfo(fd, addrInfo) != 0) {
            int errno = nf.errno();
            nf.close(fd, LOG);
            nf.freeAddrInfo(addrInfo);
            throw new LineSenderException("could not connect to host ")
                    .put("[host=").put(host).put("]").errno(errno);
        }
        nf.freeAddrInfo(addrInfo);
        configureBuffers(nf, sndBufferSize);
    }

    @Override
    public void close() {
        nf.close(fd, LOG);
        if (sockaddr != -1) {
            nf.freeSockAddr(sockaddr);
        }
    }

    @Override
    public int errno() {
        return nf.errno();
    }

    @Override
    public int receive(long ptr, int len) {
        return nf.recvRaw(fd, ptr, len);
    }

    @Override
    public void send(long ptr, int len) {
        if (len > 0) {
            long o = 0;
            while (len > 0) {
                int n = nf.sendRaw(fd, ptr + o, len);
                if (n > 0) {
                    len -= n;
                    o += n;
                } else {
                    throw new LineSenderException("send error ").errno(nf.errno());
                }
            }
        }
    }

    private void configureBuffers(NetworkFacade nf, int sndBufferSize) {
        int orgSndBufSz = nf.getSndBuf(fd);
        nf.setSndBuf(fd, sndBufferSize);
        int newSndBufSz = nf.getSndBuf(fd);
        LOG.debug().$("Send buffer size change from ").$(orgSndBufSz).$(" to ").$(newSndBufSz).$();
    }
}
