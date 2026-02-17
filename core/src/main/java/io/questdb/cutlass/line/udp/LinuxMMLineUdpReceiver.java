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

import io.questdb.cairo.CairoEngine;
import io.questdb.mp.WorkerPool;
import io.questdb.network.Net;

public class LinuxMMLineUdpReceiver extends AbstractLineProtoUdpReceiver {
    private final int msgCount;
    private long msgVec;

    public LinuxMMLineUdpReceiver(
            LineUdpReceiverConfiguration configuration,
            CairoEngine engine,
            WorkerPool workerPool
    ) {
        super(configuration, engine, workerPool);
        this.msgCount = configuration.getMsgCount();
        msgVec = nf.msgHeaders(configuration.getMsgBufferSize(), msgCount);
        start();
    }

    @Override
    public void close() {
        super.close();
        if (msgVec != 0) {
            nf.freeMsgHeaders(msgVec);
            msgVec = 0;
        }
    }

    @Override
    protected boolean runSerially() {
        boolean ran = false;
        int count;
        while ((count = nf.recvmmsgRaw(fd, msgVec, msgCount)) > 0) {
            long p = msgVec;
            for (int i = 0; i < count; i++) {
                long buf = nf.getMMsgBuf(p);
                lexer.parse(buf, buf + nf.getMMsgBufLen(p));
                lexer.parseLast();
                p += Net.MMSGHDR_SIZE;
            }

            totalCount += count;

            if (totalCount > commitRate) {
                totalCount = 0;
                parser.commitAll();
            }

            if (ran) {
                continue;
            }

            ran = true;
        }
        parser.commitAll();
        return ran;
    }
}
