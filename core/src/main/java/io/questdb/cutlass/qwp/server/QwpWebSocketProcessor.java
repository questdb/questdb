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

package io.questdb.cutlass.qwp.server;

import io.questdb.cutlass.qwp.protocol.*;
import io.questdb.cutlass.qwp.websocket.WebSocketProcessor;

/**
 * WebSocket processor for ILP v4 binary protocol.
 * <p>
 * This processor handles WebSocket messages containing ILP v4 binary data
 * and delegates processing to a callback.
 */
public class QwpWebSocketProcessor implements WebSocketProcessor {
    private Callback callback;

    /**
     * Sets the callback for processing events.
     *
     * @param callback the callback to receive events
     */
    public void setCallback(Callback callback) {
        this.callback = callback;
    }

    @Override
    public void onBinaryMessage(long payload, int length) {
        if (callback != null) {
            callback.onBinaryMessage(payload, length);
        }
    }

    @Override
    public void onTextMessage(long payload, int length) {
        // ILP v4 is binary only - text messages are ignored
        if (callback != null) {
            callback.onTextMessage(payload, length);
        }
    }

    @Override
    public void onPing(long payload, int length) {
        if (callback != null) {
            callback.onPing(payload, length);
        }
    }

    @Override
    public void onPong(long payload, int length) {
        if (callback != null) {
            callback.onPong(payload, length);
        }
    }

    @Override
    public void onClose(int code, long reason, int reasonLength) {
        if (callback != null) {
            callback.onClose(code, reason, reasonLength);
        }
    }

    @Override
    public void onError(int errorCode, CharSequence message) {
        if (callback != null) {
            callback.onError(errorCode, message);
        }
    }

    /**
     * Callback interface for ILP v4 WebSocket processing events.
     */
    public interface Callback {
        /**
         * Called when a binary message is received.
         */
        void onBinaryMessage(long payload, int length);

        /**
         * Called when a text message is received.
         * ILP v4 is binary-only, so this is typically ignored.
         */
        void onTextMessage(long payload, int length);

        /**
         * Called when a ping frame is received.
         */
        void onPing(long payload, int length);

        /**
         * Called when a pong frame is received.
         */
        void onPong(long payload, int length);

        /**
         * Called when a close frame is received.
         */
        void onClose(int code, long reason, int reasonLength);

        /**
         * Called when an error occurs.
         */
        void onError(int errorCode, CharSequence message);
    }
}
