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

import io.questdb.std.Unsafe;

class CommandHeader {
    private long msgId;
    private short cmdType;
    private int errorCode;
    private int bodyLength;

    private final Command[] commands;

    CommandHeader(LineTcpMeasurementScheduler scheduler) {
        commands = new Command[]{
                new HandShake(this),
                new Commit(this, scheduler)
        };
    }

    Command getCommand() {
        return commands[cmdType];
    }

    static int getSize() {
        // msgId, cmdType, bodyLength
        return Long.BYTES + Short.BYTES + Integer.BYTES;
    }

    static int getAckSize() {
        // msgId, cmdType, errorCode, bodyLength
        return Long.BYTES + Short.BYTES + Integer.BYTES + Integer.BYTES;
    }

    LineTcpParser.ParseResult read(long bufferPos, long bufHi) {
        if (bufferPos + CommandHeader.getSize() > bufHi) {
            return LineTcpParser.ParseResult.BUFFER_UNDERFLOW;
        }
        msgId = Unsafe.getUnsafe().getLong(bufferPos);
        bufferPos += Long.BYTES;
        cmdType = Unsafe.getUnsafe().getShort(bufferPos);
        bufferPos += Short.BYTES;
        bodyLength = Unsafe.getUnsafe().getInt(bufferPos);
        bufferPos += Integer.BYTES;
        if (bufferPos + bodyLength > bufHi) {
            return LineTcpParser.ParseResult.BUFFER_UNDERFLOW;
        }
        return LineTcpParser.ParseResult.OK;
    }

    void of(int bodyLength) {
        this.errorCode = CommandError.OK;
        this.bodyLength = bodyLength;
    }

    void ofError(int errorCode) {
        this.errorCode = errorCode;
        this.bodyLength = 0; // todo: add error msg in the body
    }

    long writeHeader(long bufferPos) {
        Unsafe.getUnsafe().putLong(bufferPos, msgId);       // msgId of command
        bufferPos += Long.BYTES;
        Unsafe.getUnsafe().putShort(bufferPos, cmdType);    // type of command
        bufferPos += Short.BYTES;
        Unsafe.getUnsafe().putInt(bufferPos, errorCode);    // errorCode, 0: OK, anything else is a failure
        bufferPos += Integer.BYTES;
        Unsafe.getUnsafe().putInt(bufferPos, bodyLength);    // bodyLength
        bufferPos += Integer.BYTES;
        return bufferPos;
    }
}
