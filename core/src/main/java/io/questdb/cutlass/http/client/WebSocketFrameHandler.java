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

package io.questdb.cutlass.http.client;

/**
 * Callback interface for handling received WebSocket frames.
 * <p>
 * Implementations should process received data efficiently and avoid blocking,
 * as callbacks are invoked on the I/O thread.
 * <p>
 * Thread safety: Callbacks are invoked from the thread that called receiveFrame().
 * Implementations must handle their own synchronization if accessed from multiple threads.
 */
public interface WebSocketFrameHandler {

    /**
     * Called when a binary frame is received.
     *
     * @param payloadPtr pointer to the payload data in native memory
     * @param payloadLen length of the payload in bytes
     */
    void onBinaryMessage(long payloadPtr, int payloadLen);

    /**
     * Called when a text frame is received.
     * <p>
     * Default implementation does nothing. Override if text frames need handling.
     *
     * @param payloadPtr pointer to the UTF-8 encoded payload in native memory
     * @param payloadLen length of the payload in bytes
     */
    default void onTextMessage(long payloadPtr, int payloadLen) {
        // Default: ignore text frames
    }

    /**
     * Called when a close frame is received from the server.
     * <p>
     * After this callback, the connection will be closed. The handler should
     * perform any necessary cleanup.
     *
     * @param code   the close status code (e.g., 1000 for normal closure)
     * @param reason the close reason (may be null or empty)
     */
    void onClose(int code, String reason);

    /**
     * Called when a ping frame is received.
     * <p>
     * Default implementation does nothing. The WebSocketClient automatically
     * sends a pong response, so this callback is for informational purposes only.
     *
     * @param payloadPtr pointer to the ping payload in native memory
     * @param payloadLen length of the payload in bytes
     */
    default void onPing(long payloadPtr, int payloadLen) {
        // Default: handled automatically by client
    }

    /**
     * Called when a pong frame is received.
     * <p>
     * Default implementation does nothing.
     *
     * @param payloadPtr pointer to the pong payload in native memory
     * @param payloadLen length of the payload in bytes
     */
    default void onPong(long payloadPtr, int payloadLen) {
        // Default: ignore pong frames
    }
}
