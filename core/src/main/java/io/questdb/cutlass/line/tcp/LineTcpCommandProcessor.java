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

package io.questdb.cutlass.line.tcp;

import io.questdb.cairo.CairoException;
import io.questdb.network.NetworkFacade;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;

import java.io.Closeable;

class LineTcpCommandProcessor implements Closeable {
    private final int bufferSize = 8192;
    private final long bufferStart;
    private final long bufferEnd;
    private long bufferPos;
    private NetworkFacade nf;
    private LineTcpConnectionContext context;
    private Command command;
    private final CommandHeader commandHeader;

    LineTcpCommandProcessor(LineTcpMeasurementScheduler scheduler) {
        commandHeader = new CommandHeader(scheduler);

        bufferStart = Unsafe.malloc(bufferSize, MemoryTag.NATIVE_ILP_RSS);
        bufferPos = bufferStart;
        bufferEnd = bufferStart + bufferSize;
    }

    @Override
    public void close() {
        of(null, null);
        bufferPos = Unsafe.free(bufferStart, bufferSize, MemoryTag.NATIVE_ILP_RSS);
    }

    void of(NetworkFacade nf, LineTcpConnectionContext context) {
        this.nf = nf;
        this.context = context;
    }

    LineTcpParser.ParseResult parseHeader(long bufferPos, long bufHi) {
        return commandHeader.read(bufferPos, bufHi);
    }

    long parseCommand(long bufferPos) {
        command = commandHeader.getCommand();
        return command.read(bufferPos);
    }

    void executeCommand(NetworkIOJob netIoJob) {
        bufferPos = bufferStart;
        final int msgSize = command.getAckSize();
        if (bufferPos + msgSize > bufferEnd) {
            throw CairoException.critical(CommandError.BUFFER_TOO_SMALL)
                    .put("Command processor buffer size is too small [msgSize=").put(msgSize).put(']');
        }
        command.execute(netIoJob, context);
        bufferPos = command.writeAck(bufferPos);
        flush();
    }

    void processError(CairoException e) {
        bufferPos = bufferStart;
        final int msgSize = CommandHeader.getAckSize();
        if (bufferPos + msgSize > bufferEnd) {
            throw CairoException.critical(CommandError.BUFFER_TOO_SMALL)
                    .put("Command processor buffer size is too small [msgSize=").put(msgSize).put(']');
        }
        commandHeader.ofError(e.getErrno());
        bufferPos = commandHeader.writeHeader(bufferPos);
        flush();
    }

    private void flush() {
        final int n = (int) (bufferPos - bufferStart);
        int sent = 0;
        while (sent != n) {
            final int rc = nf.send(context.getFd(), bufferStart + sent, n - sent);
            if (rc < 0) {
                throw new RuntimeException("Could not send data to client [rc=" + rc + "]");
            }
            sent += rc;
        }
    }
}
