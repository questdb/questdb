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
 * WebSocket frame opcodes as defined in RFC 6455.
 */
public final class WebSocketOpcode {
    /**
     * Continuation frame (0x0).
     * Used for fragmented messages after the initial frame.
     */
    public static final int CONTINUATION = 0x00;

    /**
     * Text frame (0x1).
     * Payload is UTF-8 encoded text.
     */
    public static final int TEXT = 0x01;

    /**
     * Binary frame (0x2).
     * Payload is arbitrary binary data.
     */
    public static final int BINARY = 0x02;

    // Reserved non-control frames: 0x3-0x7

    /**
     * Connection close frame (0x8).
     * Indicates that the endpoint wants to close the connection.
     */
    public static final int CLOSE = 0x08;

    /**
     * Ping frame (0x9).
     * Used for keep-alive and connection health checks.
     */
    public static final int PING = 0x09;

    /**
     * Pong frame (0xA).
     * Response to a ping frame.
     */
    public static final int PONG = 0x0A;

    // Reserved control frames: 0xB-0xF

    private WebSocketOpcode() {
        // Constants class
    }

    /**
     * Checks if the opcode is a control frame.
     * Control frames are CLOSE (0x8), PING (0x9), and PONG (0xA).
     *
     * @param opcode the opcode to check
     * @return true if the opcode is a control frame
     */
    public static boolean isControlFrame(int opcode) {
        return (opcode & 0x08) != 0;
    }

    /**
     * Checks if the opcode is a data frame.
     * Data frames are CONTINUATION (0x0), TEXT (0x1), and BINARY (0x2).
     *
     * @param opcode the opcode to check
     * @return true if the opcode is a data frame
     */
    public static boolean isDataFrame(int opcode) {
        return opcode <= 0x02;
    }

    /**
     * Checks if the opcode is valid according to RFC 6455.
     *
     * @param opcode the opcode to check
     * @return true if the opcode is valid
     */
    public static boolean isValid(int opcode) {
        return opcode == CONTINUATION
                || opcode == TEXT
                || opcode == BINARY
                || opcode == CLOSE
                || opcode == PING
                || opcode == PONG;
    }

    /**
     * Returns a human-readable name for the opcode.
     *
     * @param opcode the opcode
     * @return the opcode name
     */
    public static String name(int opcode) {
        switch (opcode) {
            case CONTINUATION:
                return "CONTINUATION";
            case TEXT:
                return "TEXT";
            case BINARY:
                return "BINARY";
            case CLOSE:
                return "CLOSE";
            case PING:
                return "PING";
            case PONG:
                return "PONG";
            default:
                return "UNKNOWN(" + opcode + ")";
        }
    }
}
