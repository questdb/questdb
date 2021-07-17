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

package io.questdb.network;

import io.questdb.mp.Job;

import java.io.Closeable;

public interface IODispatcher<C extends IOContext> extends Closeable, Job {
    int DISCONNECT_REASON_UNKNOWN_OPERATION = 0;
    int DISCONNECT_REASON_KEEPALIVE_OFF = 1;
    int DISCONNECT_REASON_KICKED_OUT_AT_RERUN = 2;
    int DISCONNECT_REASON_KICKED_OUT_AT_SEND = 3;
    int DISCONNECT_REASON_KEEPALIVE_OFF_RECV = 4;
    int DISCONNECT_REASON_KICKED_OUT_AT_RECV = 5;
    int DISCONNECT_REASON_RETRY_FAILED = 6;
    int DISCONNECT_REASON_PROTOCOL_VIOLATION = 7;
    int DISCONNECT_REASON_PEER_DISCONNECT_AT_MULTIPART_RECV = 8;
    int DISCONNECT_REASON_MULTIPART_HEADER_TOO_BIG = 9;
    int DISCONNECT_REASON_PEER_DISCONNECT_AT_RERUN = 10;
    int DISCONNECT_REASON_PEER_DISCONNECT_AT_SEND = 11;
    int DISCONNECT_REASON_PEER_DISCONNECT_AT_HEADER_RECV = 12;
    int DISCONNECT_REASON_KICKED_OUT_AT_EXTRA_BYTES = 13;
    int DISCONNECT_REASON_KICKED_TXT_NOT_ENOUGH_LINES = 14;
    int DISCONNECT_REASON_PEER_DISCONNECT_AT_RECV = 15;
    int DISCONNECT_REASON_TEST = 16;

    void disconnect(C context, int reason);

    int getConnectionCount();

    boolean processIOQueue(IORequestProcessor<C> processor);

    public boolean isListening();

    void registerChannel(C context, int operation);
}
