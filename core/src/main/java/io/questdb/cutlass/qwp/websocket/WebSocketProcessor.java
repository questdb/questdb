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

package io.questdb.cutlass.qwp.websocket;

/**
 * Interface for processing WebSocket messages.
 * Implementations handle incoming data and control frames.
 *
 * <p>Thread safety: Implementations are NOT expected to be thread-safe.
 * Each connection should have its own processor instance.
 */
public interface WebSocketProcessor {

    /**
     * Called when a complete binary message is received.
     * For fragmented messages, this is called after all fragments are assembled.
     *
     * @param payload pointer to the payload data
     * @param length  the payload length
     */
    void onBinaryMessage(long payload, int length);

    /**
     * Called when a complete text message is received.
     * For fragmented messages, this is called after all fragments are assembled.
     *
     * @param payload pointer to the UTF-8 encoded payload data
     * @param length  the payload length
     */
    void onTextMessage(long payload, int length);

    /**
     * Called when a PING frame is received.
     * The connection context will automatically send a PONG response.
     *
     * @param payload pointer to the ping payload (may be empty)
     * @param length  the payload length (0-125)
     */
    void onPing(long payload, int length);

    /**
     * Called when a PONG frame is received.
     *
     * @param payload pointer to the pong payload
     * @param length  the payload length (0-125)
     */
    void onPong(long payload, int length);

    /**
     * Called when a CLOSE frame is received.
     *
     * @param code   the close status code, or -1 if no code was provided
     * @param reason pointer to the UTF-8 encoded close reason, or 0 if none
     * @param reasonLength the length of the reason string
     */
    void onClose(int code, long reason, int reasonLength);

    /**
     * Called when a protocol error occurs.
     *
     * @param errorCode the WebSocket close code that describes the error
     * @param message   human-readable error description
     */
    void onError(int errorCode, CharSequence message);
}
