/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
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

import io.questdb.cutlass.line.LineProtoSender;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.network.NetworkError;

public class LineTCPProtoSender extends LineProtoSender {
    private static final Log LOG = LogFactory.getLog(LineProtoSender.class);

    public LineTCPProtoSender(int sendToIPv4Address, int sendToPort, int bufferCapacity) {
        super(0, sendToIPv4Address, sendToPort, bufferCapacity, 0);
    }

    @Override
    protected long createSocket(int interfaceIPv4Address, int ttl, long sockaddr) throws NetworkError {
        long fd = nf.socketTcp(true);
        if (nf.connect(fd, sockaddr) != 0) {
            throw NetworkError.instance(nf.errno(), "could not connect to ").ip(interfaceIPv4Address);
        }
        int orgSndBufSz = nf.getSndBuf(fd);
        nf.setSndBuf(fd, 2 * capacity);
        int newSndBufSz = nf.getSndBuf(fd);
        LOG.info().$("Send buffer size change from ").$(orgSndBufSz).$(" to ").$(newSndBufSz).$();
        return fd;
    }

    @Override
    protected void sendToSocket(long fd, long lo, long sockaddr, int len) throws NetworkError {
        if (nf.send(fd, lo, len) != len) {
            throw NetworkError.instance(nf.errno()).put("send error");
        }
    }
}
