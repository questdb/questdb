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

package io.questdb.cutlass.qwp.server;

import io.questdb.cairo.CairoEngine;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.WorkerPool;
import io.questdb.network.Net;
import org.jetbrains.annotations.Nullable;

public class LinuxMMQwpUdpReceiver extends QwpUdpReceiver {
    private static final Log LOG = LogFactory.getLog(LinuxMMQwpUdpReceiver.class);

    private final int msgCount;
    private long msgVec;

    public LinuxMMQwpUdpReceiver(QwpUdpReceiverConfiguration configuration, CairoEngine engine) {
        this(configuration, engine, null);
    }

    public LinuxMMQwpUdpReceiver(QwpUdpReceiverConfiguration configuration, CairoEngine engine, @Nullable WorkerPool workerPool) {
        super(configuration, engine, workerPool);
        this.msgCount = configuration.getMsgCount();
        try {
            this.msgVec = nf.msgHeaders(bufLen, msgCount);
        } catch (Throwable e) {
            close();
            throw e;
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
    public boolean runSerially() {
        if (checkClosed()) {
            return false;
        }
        boolean ran = false;
        boolean committed = false;
        int count;
        while ((count = nf.recvmmsgRaw(fd, msgVec, msgCount)) > 0) {
            long p = msgVec;
            for (int i = 0; i < count; i++) {
                processDatagram(nf.getMMsgBuf(p), (int) nf.getMMsgBufLen(p));
                processedCount++;
                p += Net.MMSGHDR_SIZE;
            }

            ran = true;
            totalCount += count;

            if (totalCount >= commitRate) {
                totalCount = 0;
                tudCache.commitAllBestEffort();
                committed = true;
                break;
            }
        }
        if (!committed && ran) {
            tudCache.commitAllBestEffort();
        }
        return ran;
    }
}
