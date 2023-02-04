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

abstract class Command {
    final CommandHeader header;

    Command(CommandHeader header) {
        this.header = header;
    }

    // deserialize the command from the buffer starting at `bufferPos`
    abstract long read(long bufferPos);

    // execute the command
    abstract void execute(NetworkIOJob netIoJob, LineTcpConnectionContext context);

    // serialize the ack into the buffer starting at `bufferPos`
    abstract long writeAck(long bufferPos);

    // the size of an ack message in bytes
    // should return 0 if command does not require an ack to the client
    abstract int getAckSize();
}
