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

package io.questdb.network;

import io.questdb.mp.Job;

import java.io.Closeable;

public interface IODispatcher<C extends IOContext<C>> extends Closeable, Job {
    int DISCONNECT_REASON_KEEPALIVE_OFF = 1;
    int DISCONNECT_REASON_KEEPALIVE_OFF_RECV = 4;
    int DISCONNECT_REASON_KICKED_OUT_AT_EXTRA_BYTES = 13;
    int DISCONNECT_REASON_KICKED_OUT_AT_RECV = 5;
    int DISCONNECT_REASON_KICKED_OUT_AT_RERUN = 2;
    int DISCONNECT_REASON_KICKED_OUT_AT_SEND = 3;
    int DISCONNECT_REASON_KICKED_TXT_NOT_ENOUGH_LINES = 14;
    int DISCONNECT_REASON_MULTIPART_HEADER_TOO_BIG = 9;
    int DISCONNECT_REASON_PEER_DISCONNECT_AT_HEADER_RECV = 12;
    int DISCONNECT_REASON_PEER_DISCONNECT_AT_MULTIPART_RECV = 8;
    int DISCONNECT_REASON_PEER_DISCONNECT_AT_RECV = 15;
    int DISCONNECT_REASON_PEER_DISCONNECT_AT_RERUN = 10;
    int DISCONNECT_REASON_PEER_DISCONNECT_AT_SEND = 11;
    int DISCONNECT_REASON_PROTOCOL_VIOLATION = 7;
    int DISCONNECT_REASON_RETRY_FAILED = 6;
    /**
     * Unexpected server error caused connection disconnect (to avoid client working with potentially corrupt server state).
     */
    int DISCONNECT_REASON_SERVER_ERROR = 17;
    int DISCONNECT_REASON_TEST = 16;
    int DISCONNECT_REASON_TLS_SESSION_INIT_FAILED = 18;
    int DISCONNECT_REASON_UNKNOWN_OPERATION = 0;

    void disconnect(C context, int reason);

    default void drainIOQueue(IORequestProcessor<C> processor) {
        //noinspection StatementWithEmptyBody
        while (processIOQueue(processor)) ;
    }

    int getConnectionCount();

    int getPort();

    boolean isListening();

    boolean processIOQueue(IORequestProcessor<C> processor);

    void registerChannel(C context, int operation);
}
