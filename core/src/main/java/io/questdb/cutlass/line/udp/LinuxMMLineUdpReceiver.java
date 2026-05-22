/*+*****************************************************************************
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

import java.util.concurrent.atomic.AtomicBoolean;

public class LinuxMMLineUdpReceiver extends AbstractLineProtoUdpReceiver {
    private final int msgCount;
    private long msgVec;

    public LinuxMMLineUdpReceiver(
            LineUdpReceiverConfiguration configuration,
            CairoEngine engine,
            WorkerPool workerPool
    ) {
        this(configuration, engine, workerPool, new AtomicBoolean(true));
    }

    public LinuxMMLineUdpReceiver(
            LineUdpReceiverConfiguration configuration,
            CairoEngine engine,
            WorkerPool workerPool,
            AtomicBoolean acceptOpen
    ) {
        super(configuration, engine, workerPool, acceptOpen);
        this.msgCount = configuration.getMsgCount();
        // #051: wrap the post-super native allocation and start() in try/catch so a failure
        // (msgHeaders OOM, thread alloc, affinity bind) releases what the super already opened.
        // close() releases the fd and any allocated msgVec; matches the LineTcpReceiver pattern.
        try {
            msgVec = nf.msgHeaders(configuration.getMsgBufferSize(), msgCount);
            start();
        } catch (Throwable t) {
            try {
                close();
            } catch (Throwable s) {
                t.addSuppressed(s);
            }
            throw t;
        }
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
        if (!acceptOpen.get()) {
            // Mirror the worker-path acceptOpen gate (AbstractLineProtoUdpReceiver.run)
            // so the own-thread driver also quiesces after switchRole publishes
            // acceptOpen=false. close() does not depend on runSerially() flowing
            // post-close, so placement at the top is safe. (#036)
            return false;
        }
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
