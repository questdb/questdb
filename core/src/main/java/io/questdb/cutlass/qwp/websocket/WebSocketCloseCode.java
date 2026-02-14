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
 * WebSocket close status codes as defined in RFC 6455.
 */
public final class WebSocketCloseCode {
    /**
     * Normal closure (1000).
     * The connection successfully completed whatever purpose for which it was created.
     */
    public static final int NORMAL_CLOSURE = 1000;

    /**
     * Going away (1001).
     * The endpoint is going away, e.g., server shutting down or browser navigating away.
     */
    public static final int GOING_AWAY = 1001;

    /**
     * Protocol error (1002).
     * The endpoint is terminating the connection due to a protocol error.
     */
    public static final int PROTOCOL_ERROR = 1002;

    /**
     * Unsupported data (1003).
     * The endpoint received a type of data it cannot accept.
     */
    public static final int UNSUPPORTED_DATA = 1003;

    /**
     * Reserved (1004).
     * Reserved for future use.
     */
    public static final int RESERVED = 1004;

    /**
     * No status received (1005).
     * Reserved value. MUST NOT be sent in a Close frame.
     */
    public static final int NO_STATUS_RECEIVED = 1005;

    /**
     * Abnormal closure (1006).
     * Reserved value. MUST NOT be sent in a Close frame.
     * Used to indicate that a connection was closed abnormally.
     */
    public static final int ABNORMAL_CLOSURE = 1006;

    /**
     * Invalid frame payload data (1007).
     * The endpoint received a message with invalid payload data.
     */
    public static final int INVALID_PAYLOAD_DATA = 1007;

    /**
     * Policy violation (1008).
     * The endpoint received a message that violates its policy.
     */
    public static final int POLICY_VIOLATION = 1008;

    /**
     * Message too big (1009).
     * The endpoint received a message that is too big to process.
     */
    public static final int MESSAGE_TOO_BIG = 1009;

    /**
     * Mandatory extension (1010).
     * The client expected the server to negotiate one or more extensions.
     */
    public static final int MANDATORY_EXTENSION = 1010;

    /**
     * Internal server error (1011).
     * The server encountered an unexpected condition that prevented it from fulfilling the request.
     */
    public static final int INTERNAL_ERROR = 1011;

    /**
     * TLS handshake (1015).
     * Reserved value. MUST NOT be sent in a Close frame.
     * Used to indicate that the connection was closed due to TLS handshake failure.
     */
    public static final int TLS_HANDSHAKE = 1015;

    private WebSocketCloseCode() {
        // Constants class
    }

    /**
     * Checks if a close code is valid for use in a Close frame.
     * Codes 1005 and 1006 are reserved and must not be sent.
     *
     * @param code the close code
     * @return true if the code can be sent in a Close frame
     */
    public static boolean isValidForSending(int code) {
        if (code < 1000) {
            return false;
        }
        if (code == NO_STATUS_RECEIVED || code == ABNORMAL_CLOSURE || code == TLS_HANDSHAKE) {
            return false;
        }
        // 1000-2999 are defined by RFC 6455
        // 3000-3999 are reserved for libraries/frameworks
        // 4000-4999 are reserved for applications
        return code < 5000;
    }

    /**
     * Returns a human-readable description of the close code.
     *
     * @param code the close code
     * @return the description
     */
    public static String describe(int code) {
        switch (code) {
            case NORMAL_CLOSURE:
                return "Normal Closure";
            case GOING_AWAY:
                return "Going Away";
            case PROTOCOL_ERROR:
                return "Protocol Error";
            case UNSUPPORTED_DATA:
                return "Unsupported Data";
            case RESERVED:
                return "Reserved";
            case NO_STATUS_RECEIVED:
                return "No Status Received";
            case ABNORMAL_CLOSURE:
                return "Abnormal Closure";
            case INVALID_PAYLOAD_DATA:
                return "Invalid Payload Data";
            case POLICY_VIOLATION:
                return "Policy Violation";
            case MESSAGE_TOO_BIG:
                return "Message Too Big";
            case MANDATORY_EXTENSION:
                return "Mandatory Extension";
            case INTERNAL_ERROR:
                return "Internal Error";
            case TLS_HANDSHAKE:
                return "TLS Handshake";
            default:
                if (code >= 3000 && code < 4000) {
                    return "Library/Framework Code (" + code + ")";
                } else if (code >= 4000 && code < 5000) {
                    return "Application Code (" + code + ")";
                }
                return "Unknown (" + code + ")";
        }
    }
}
