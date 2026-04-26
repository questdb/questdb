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

package io.questdb.cutlass.qwp.server;

import io.questdb.cairo.CairoEngine;
import io.questdb.cutlass.http.HttpFullFatServerConfiguration;
import io.questdb.cutlass.http.HttpRequestHandler;
import io.questdb.cutlass.http.HttpRequestHeader;
import io.questdb.cutlass.http.HttpRequestProcessor;
import io.questdb.std.Numbers;
import io.questdb.std.Unsafe;
import io.questdb.std.str.Utf8Sequence;
import io.questdb.std.str.Utf8String;
import io.questdb.std.str.Utf8s;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;

/**
 * HTTP request handler for QWP v1 WebSocket connections.
 * <p>
 * This handler detects WebSocket upgrade requests and returns an appropriate
 * processor to handle the WebSocket protocol upgrade and subsequent communication.
 */
public class QwpWebSocketHttpProcessor implements HttpRequestHandler {

    public static final Utf8String HEADER_CONNECTION = new Utf8String("Connection");
    public static final Utf8String HEADER_ORIGIN = new Utf8String("Origin");
    public static final Utf8String HEADER_SEC_WEBSOCKET_KEY = new Utf8String("Sec-WebSocket-Key");
    public static final Utf8String HEADER_SEC_WEBSOCKET_VERSION = new Utf8String("Sec-WebSocket-Version");
    // Header names (case-insensitive)
    public static final Utf8String HEADER_UPGRADE = new Utf8String("Upgrade");
    // QWP version negotiation headers
    public static final Utf8String HEADER_X_QWP_CLIENT_ID = new Utf8String("X-QWP-Client-Id");
    public static final Utf8String HEADER_X_QWP_MAX_VERSION = new Utf8String("X-QWP-Max-Version");
    // Client opt-in for STATUS_DURABLE_ACK frames. Value "true" (case-insensitive) enables.
    // Any other value, or header absent, leaves the feature disabled for this connection.
    public static final Utf8String HEADER_X_QWP_REQUEST_DURABLE_ACK = new Utf8String("X-QWP-Request-Durable-Ack");
    // Expected value for HEADER_X_QWP_REQUEST_DURABLE_ACK to enable durable-ack; compared case-insensitively.
    public static final Utf8String HEADER_VALUE_DURABLE_ACK_ENABLED = new Utf8String("true");
    // Header values
    public static final Utf8String VALUE_WEBSOCKET = new Utf8String("websocket");
    /**
     * The WebSocket magic GUID used in the Sec-WebSocket-Accept calculation.
     */
    public static final String WEBSOCKET_GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";
    /**
     * The required WebSocket version (RFC 6455).
     */
    public static final int WEBSOCKET_VERSION = 13;
    private static final String ERROR_CONNECTION_MUST_CONTAIN_UPGRADE = "Connection header must contain 'upgrade'";
    private static final String ERROR_INVALID_SEC_WEBSOCKET_KEY = "Invalid Sec-WebSocket-Key (must be 24-character base64 key)";
    private static final String ERROR_INVALID_UPGRADE_HEADER_VALUE = "Invalid Upgrade header value";
    private static final String ERROR_MISSING_CONNECTION_HEADER = "Missing Connection header";
    private static final String ERROR_MISSING_SEC_WEBSOCKET_KEY_HEADER = "Missing Sec-WebSocket-Key header";
    private static final String ERROR_MISSING_SEC_WEBSOCKET_VERSION_HEADER = "Missing Sec-WebSocket-Version header";
    private static final String ERROR_MISSING_UPGRADE_HEADER = "Missing Upgrade header";
    private static final String ERROR_ORIGIN_HEADER_NOT_ALLOWED = "Origin header not allowed on QWP WebSocket";
    private static final String ERROR_UNSUPPORTED_WEBSOCKET_VERSION = "Unsupported WebSocket version (must be 13)";
    private static final byte[] RESPONSE_AFTER_ACCEPT = "\r\nX-QWP-Version: ".getBytes(StandardCharsets.US_ASCII);
    // Response template
    private static final byte[] RESPONSE_PREFIX =
            "HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: ".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] RESPONSE_SUFFIX = "\r\n\r\n".getBytes(StandardCharsets.US_ASCII);
    // Thread-local SHA-1 digest for computing Sec-WebSocket-Accept
    private static final ThreadLocal<MessageDigest> SHA1_DIGEST = ThreadLocal.withInitial(() -> {
        try {
            return MessageDigest.getInstance("SHA-1");
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("SHA-1 not available", e);
        }
    });
    private static final byte[] WEBSOCKET_GUID_BYTES = WEBSOCKET_GUID.getBytes(StandardCharsets.US_ASCII);
    private final QwpWebSocketUpgradeProcessor processor;

    /**
     * Creates a new QWP v1 WebSocket HTTP processor.
     *
     * @param engine            the Cairo engine for database access
     * @param httpConfiguration the HTTP server configuration
     */
    public QwpWebSocketHttpProcessor(CairoEngine engine, HttpFullFatServerConfiguration httpConfiguration) {
        this.processor = new QwpWebSocketUpgradeProcessor(engine, httpConfiguration);
    }

    /**
     * Computes the Sec-WebSocket-Accept value for the given key.
     *
     * @param key the Sec-WebSocket-Key from the client
     * @return the base64-encoded SHA-1 hash to send in the response
     */
    public static String computeAcceptKey(Utf8Sequence key) {
        MessageDigest sha1 = SHA1_DIGEST.get();
        sha1.reset();

        // Concatenate key + GUID
        byte[] keyBytes = new byte[key.size()];
        for (int i = 0; i < key.size(); i++) {
            keyBytes[i] = key.byteAt(i);
        }
        sha1.update(keyBytes);
        sha1.update(WEBSOCKET_GUID_BYTES);

        // Compute SHA-1 hash and base64 encode
        byte[] hash = sha1.digest();
        return Base64.getEncoder().encodeToString(hash);
    }

    /**
     * Gets the WebSocket key from the request header.
     *
     * @param header the HTTP request header
     * @return the WebSocket key, or null if not present
     */
    public static Utf8Sequence getWebSocketKey(HttpRequestHeader header) {
        return header.getHeader(HEADER_SEC_WEBSOCKET_KEY);
    }

    /**
     * Checks if the Connection header contains "upgrade".
     *
     * @param connectionHeader the value of the Connection header
     * @return true if the connection should be upgraded
     */
    public static boolean isConnectionUpgrade(Utf8Sequence connectionHeader) {
        if (connectionHeader == null) {
            return false;
        }
        // Connection header may contain multiple values, e.g., "keep-alive, Upgrade"
        // Perform case-insensitive token match for "upgrade"
        return containsUpgrade(connectionHeader);
    }

    /**
     * Validates the Sec-WebSocket-Key header.
     * The key must be a base64-encoded 16-byte value.
     *
     * @param key the Sec-WebSocket-Key header value
     * @return true if the key is valid
     */
    public static boolean isValidKey(Utf8Sequence key) {
        if (key == null) {
            return false;
        }
        // Base64-encoded 16-byte value should be exactly 24 characters
        // (16 bytes = 128 bits = 22 base64 chars + 2 padding = 24)
        int size = key.size();
        if (size != 24) {
            return false;
        }
        // Basic validation: check that all characters are valid base64
        for (int i = 0; i < size; i++) {
            byte b = key.byteAt(i);
            boolean valid = (b >= 'A' && b <= 'Z') || (b >= 'a' && b <= 'z') ||
                    (b >= '0' && b <= '9') || b == '+' || b == '/' || b == '=';
            if (!valid) {
                return false;
            }
        }
        return true;
    }

    /**
     * Validates the WebSocket version.
     *
     * @param versionHeader the Sec-WebSocket-Version header value
     * @return true if the version is valid (13)
     */
    public static boolean isValidVersion(Utf8Sequence versionHeader) {
        return versionHeader != null && Numbers.parseNonNegativeIntQuiet(versionHeader) == WEBSOCKET_VERSION;
    }

    public static boolean isVersionValidationError(String validationError) {
        return ERROR_MISSING_SEC_WEBSOCKET_VERSION_HEADER.equals(validationError)
                || ERROR_UNSUPPORTED_WEBSOCKET_VERSION.equals(validationError);
    }

    /**
     * Checks if the given header indicates a WebSocket upgrade request.
     *
     * @param upgradeHeader the value of the Upgrade header
     * @return true if this is a WebSocket upgrade request
     */
    public static boolean isWebSocketUpgrade(Utf8Sequence upgradeHeader) {
        return upgradeHeader != null && Utf8s.equalsIgnoreCaseAscii(upgradeHeader, VALUE_WEBSOCKET);
    }

    /**
     * Returns the size of the handshake response for the given accept key and QWP version.
     *
     * @param acceptKey  the computed accept key
     * @param qwpVersion the negotiated QWP version number
     * @return the total response size in bytes
     */
    public static int responseSize(String acceptKey, int qwpVersion) {
        return RESPONSE_PREFIX.length + acceptKey.length()
                + RESPONSE_AFTER_ACCEPT.length + digitCount(qwpVersion)
                + RESPONSE_SUFFIX.length;
    }

    /**
     * Validates WebSocket handshake headers and returns an error message if invalid.
     *
     * @param header the HTTP request header
     * @return null if valid, error message otherwise
     */
    public static String validateHandshake(HttpRequestHeader header) {
        // Reject browser-originated requests. QWP is a machine-to-machine protocol;
        // browsers send Origin automatically, legitimate clients do not.
        // This prevents Cross-Site WebSocket Hijacking (CSWSH).
        if (header.getHeader(HEADER_ORIGIN) != null) {
            return ERROR_ORIGIN_HEADER_NOT_ALLOWED;
        }

        // Check Upgrade header
        Utf8Sequence upgradeHeader = header.getHeader(HEADER_UPGRADE);
        if (upgradeHeader == null) {
            return ERROR_MISSING_UPGRADE_HEADER;
        }
        if (!isWebSocketUpgrade(upgradeHeader)) {
            return ERROR_INVALID_UPGRADE_HEADER_VALUE;
        }

        // Check Connection header
        Utf8Sequence connectionHeader = header.getHeader(HEADER_CONNECTION);
        if (connectionHeader == null) {
            return ERROR_MISSING_CONNECTION_HEADER;
        }
        if (!isConnectionUpgrade(connectionHeader)) {
            return ERROR_CONNECTION_MUST_CONTAIN_UPGRADE;
        }

        // Check Sec-WebSocket-Key
        Utf8Sequence keyHeader = header.getHeader(HEADER_SEC_WEBSOCKET_KEY);
        if (keyHeader == null) {
            return ERROR_MISSING_SEC_WEBSOCKET_KEY_HEADER;
        }
        if (!isValidKey(keyHeader)) {
            return ERROR_INVALID_SEC_WEBSOCKET_KEY;
        }

        // Check Sec-WebSocket-Version
        Utf8Sequence versionHeader = header.getHeader(HEADER_SEC_WEBSOCKET_VERSION);
        if (versionHeader == null) {
            return ERROR_MISSING_SEC_WEBSOCKET_VERSION_HEADER;
        }
        if (!isValidVersion(versionHeader)) {
            return ERROR_UNSUPPORTED_WEBSOCKET_VERSION;
        }

        return null;
    }

    /**
     * Writes the WebSocket handshake response to the given buffer.
     *
     * @param buf        the buffer to write to
     * @param acceptKey  the computed Sec-WebSocket-Accept value
     * @param qwpVersion the negotiated QWP version number
     * @return the number of bytes written
     */
    public static int writeResponse(long buf, String acceptKey, int qwpVersion) {
        int offset = 0;

        // Write prefix
        for (byte b : RESPONSE_PREFIX) {
            Unsafe.putByte(buf + offset++, b);
        }

        // Write accept key
        byte[] acceptBytes = acceptKey.getBytes(StandardCharsets.US_ASCII);
        for (byte b : acceptBytes) {
            Unsafe.putByte(buf + offset++, b);
        }

        // Write X-QWP-Version header
        for (byte b : RESPONSE_AFTER_ACCEPT) {
            Unsafe.putByte(buf + offset++, b);
        }
        byte[] versionBytes = Integer.toString(qwpVersion).getBytes(StandardCharsets.US_ASCII);
        for (byte b : versionBytes) {
            Unsafe.putByte(buf + offset++, b);
        }

        // Write suffix
        for (byte b : RESPONSE_SUFFIX) {
            Unsafe.putByte(buf + offset++, b);
        }

        return offset;
    }

    @Override
    public HttpRequestProcessor getProcessor(HttpRequestHeader requestHeader) {
        // Always return the same processor instance. Per-connection state lives
        // in LocalValue, so the instance is safe to share. Returning unconditionally
        // is required because resolveProcessorById() calls this after headers are
        // cleared (post-protocol-switch).
        return processor;
    }

    private static boolean containsUpgrade(Utf8Sequence seq) {
        int seqLen = seq.size();
        // "upgrade" is 7 bytes; match it only at comma-separated token boundaries
        for (int i = 0, n = seqLen - 6; i < n; i++) {
            if ((seq.byteAt(i) | 32) == 'u'
                    && (seq.byteAt(i + 1) | 32) == 'p'
                    && (seq.byteAt(i + 2) | 32) == 'g'
                    && (seq.byteAt(i + 3) | 32) == 'r'
                    && (seq.byteAt(i + 4) | 32) == 'a'
                    && (seq.byteAt(i + 5) | 32) == 'd'
                    && (seq.byteAt(i + 6) | 32) == 'e') {
                boolean startOk = i == 0 || seq.byteAt(i - 1) == ',' || seq.byteAt(i - 1) == ' ';
                boolean endOk = i + 7 >= seqLen || seq.byteAt(i + 7) == ',' || seq.byteAt(i + 7) == ' ';
                if (startOk && endOk) {
                    return true;
                }
            }
        }
        return false;
    }

    private static int digitCount(int value) {
        if (value < 10) return 1;
        if (value < 100) return 2;
        return 3; // QWP version will not exceed 255
    }
}
