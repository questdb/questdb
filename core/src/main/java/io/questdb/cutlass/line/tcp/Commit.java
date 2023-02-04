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
import io.questdb.cairo.CommitFailedException;
import io.questdb.std.Unsafe;
import io.questdb.std.str.DirectByteCharSequence;

import static io.questdb.cutlass.line.tcp.CommandError.*;

class Commit extends Command {
    private static final byte NO_REPLY = 0;
    private static final byte ONLY_ACK = 1;
//    private static final byte ACK_AND_APPLIED = 2;
//    private static final byte ONLY_APPLIED = 3;

    private byte commitType;
    private long seqTxn;
    private final DirectByteCharSequence tableName = new DirectByteCharSequence();
    private final LineTcpMeasurementScheduler scheduler;

    Commit(CommandHeader header, LineTcpMeasurementScheduler scheduler) {
        super(header);
        this.scheduler = scheduler;
    }

    @Override
    void execute(NetworkIOJob netIoJob, LineTcpConnectionContext context) {
        switch (commitType) {
            case NO_REPLY:
                return;
            case ONLY_ACK:
                final TableUpdateDetails tud = scheduler.lookupTUD(tableName, netIoJob, context.getFd());
                try {
                    if (tud.isWal()) {
                        seqTxn = tud.commit(true); //WAL - client can use this seqTxn to come back later for a WAL apply status of the txn
                    } else {
                        if (scheduler.dispatchCommitEvent(tud)) {
                            throw CairoException.critical(QUEUE_FULL).put("Queue is full, commit dropped [tableName=").put(tableName).put("]");
                        }
                        seqTxn = -1L; //non-WAL - no way for the client to get further update on the status
                    }
                } catch (CommitFailedException e) {
                    throw CairoException.critical(COMMIT_FAILED).put(e.getMessage());
                }
                break;
            default:
                throw CairoException.critical(INVALID_COMMIT_TYPE).put("Unsupported commit type [tableName=").put(tableName).put(", commitType=").put(commitType).put("]");
        }
    }

    @Override
    long read(long bufferPos) {
        commitType = Unsafe.getUnsafe().getByte(bufferPos);
        bufferPos += Byte.BYTES;
        final int length = Unsafe.getUnsafe().getInt(bufferPos);
        bufferPos += Integer.BYTES;
        tableName.of(bufferPos, bufferPos + length);
        bufferPos += length;
        return bufferPos;
    }

    @Override
    long writeAck(long bufferPos) {
        if (commitType == NO_REPLY) {
            return bufferPos;
        }

        header.of(Long.BYTES);
        bufferPos = header.writeHeader(bufferPos);
        Unsafe.getUnsafe().putLong(bufferPos, seqTxn);
        bufferPos += Long.BYTES;
        return bufferPos;
    }

    @Override
    int getAckSize() {
        return commitType == NO_REPLY
                ? 0
                : CommandHeader.getAckSize() + Long.BYTES;
    }
}
