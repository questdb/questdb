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

package io.questdb.cutlass.qwp.websocket;

/**
 * WebSocket close status codes as defined in RFC 6455.
 */
public final class WebSocketCloseCode {
    /**
     * Message too big (1009).
     * The endpoint received a message that is too big to process.
     */
    public static final int MESSAGE_TOO_BIG = 1009;
    /**
     * Normal closure (1000).
     * The connection successfully completed whatever purpose for which it was created.
     */
    public static final int NORMAL_CLOSURE = 1000;
    /**
     * Protocol error (1002).
     * The endpoint is terminating the connection due to a protocol error.
     */
    public static final int PROTOCOL_ERROR = 1002;
    /**
     * Unsupported data (1003).
     * The endpoint received a type of data it cannot accept
     * (e.g., a binary-only endpoint received a text message).
     */
    public static final int UNSUPPORTED_DATA = 1003;

    private WebSocketCloseCode() {
        // Constants class
    }
}
