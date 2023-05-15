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

package io.questdb.cutlass.line.udp;

import io.questdb.cairo.CairoEngine;
import io.questdb.mp.WorkerPool;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;

public class LineUdpReceiver extends AbstractLineProtoUdpReceiver {
    private final int bufLen;
    private long buf;

    public LineUdpReceiver(
            LineUdpReceiverConfiguration configuration,
            CairoEngine engine,
            WorkerPool workerPool
    ) {
        super(configuration, engine, workerPool);
        this.buf = Unsafe.malloc(this.bufLen = configuration.getMsgBufferSize(), MemoryTag.NATIVE_ILP_RSS);
        start();
    }

    @Override
    public void close() {
        super.close();
        if (buf != 0) {
            Unsafe.free(buf, bufLen, MemoryTag.NATIVE_ILP_RSS);
            buf = 0;
        }
    }

    @Override
    protected boolean runSerially() {
        boolean ran = false;
        int count;
        while ((count = nf.recv(fd, buf, bufLen)) > 0) {
            lexer.parse(buf, buf + count);
            lexer.parseLast();

            totalCount++;

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
