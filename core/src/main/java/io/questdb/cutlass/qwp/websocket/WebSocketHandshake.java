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

import io.questdb.std.Unsafe;
import io.questdb.std.str.Utf8Sequence;
import io.questdb.std.str.Utf8String;
import io.questdb.std.str.Utf8s;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;

/**
 * WebSocket handshake processing as defined in RFC 6455.
 * Provides utilities for validating WebSocket upgrade requests and
 * generating proper handshake responses.
 */
public final class WebSocketHandshake {
    /**
     * The WebSocket magic GUID used in the Sec-WebSocket-Accept calculation.
     */
    public static final String WEBSOCKET_GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";

    /**
     * The required WebSocket version (RFC 6455).
     */
    public static final int WEBSOCKET_VERSION = 13;

    // Header names (case-insensitive)
    public static final Utf8String HEADER_UPGRADE = new Utf8String("Upgrade");
    public static final Utf8String HEADER_CONNECTION = new Utf8String("Connection");
    public static final Utf8String HEADER_SEC_WEBSOCKET_KEY = new Utf8String("Sec-WebSocket-Key");
    public static final Utf8String HEADER_SEC_WEBSOCKET_VERSION = new Utf8String("Sec-WebSocket-Version");
    public static final Utf8String HEADER_SEC_WEBSOCKET_PROTOCOL = new Utf8String("Sec-WebSocket-Protocol");
    public static final Utf8String HEADER_SEC_WEBSOCKET_ACCEPT = new Utf8String("Sec-WebSocket-Accept");

    // Header values
    public static final Utf8String VALUE_WEBSOCKET = new Utf8String("websocket");
    public static final Utf8String VALUE_UPGRADE = new Utf8String("upgrade");

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

    private WebSocketHandshake() {
        // Static utility class
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
        // Perform case-insensitive substring search
        return containsIgnoreCaseAscii(connectionHeader, VALUE_UPGRADE);
    }

    /**
     * Checks if the sequence contains the given substring (case-insensitive).
     */
    private static boolean containsIgnoreCaseAscii(Utf8Sequence seq, Utf8Sequence substring) {
        int seqLen = seq.size();
        int subLen = substring.size();

        if (subLen > seqLen) {
            return false;
        }
        if (subLen == 0) {
            return true;
        }

        outer:
        for (int i = 0; i <= seqLen - subLen; i++) {
            for (int j = 0; j < subLen; j++) {
                byte a = seq.byteAt(i + j);
                byte b = substring.byteAt(j);
                // Convert to lowercase for comparison
                if (a >= 'A' && a <= 'Z') {
                    a = (byte) (a + 32);
                }
                if (b >= 'A' && b <= 'Z') {
                    b = (byte) (b + 32);
                }
                if (a != b) {
                    continue outer;
                }
            }
            return true;
        }
        return false;
    }

    /**
     * Validates the WebSocket version.
     *
     * @param versionHeader the Sec-WebSocket-Version header value
     * @return true if the version is valid (13)
     */
    public static boolean isValidVersion(Utf8Sequence versionHeader) {
        if (versionHeader == null || versionHeader.size() == 0) {
            return false;
        }
        // Parse the version number
        try {
            int version = 0;
            for (int i = 0; i < versionHeader.size(); i++) {
                byte b = versionHeader.byteAt(i);
                if (b < '0' || b > '9') {
                    return false;
                }
                version = version * 10 + (b - '0');
            }
            return version == WEBSOCKET_VERSION;
        } catch (Exception e) {
            return false;
        }
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
        sha1.update(WEBSOCKET_GUID.getBytes(StandardCharsets.US_ASCII));

        // Compute SHA-1 hash and base64 encode
        byte[] hash = sha1.digest();
        return Base64.getEncoder().encodeToString(hash);
    }

    /**
     * Computes the Sec-WebSocket-Accept value for the given key string.
     *
     * @param key the Sec-WebSocket-Key from the client
     * @return the base64-encoded SHA-1 hash to send in the response
     */
    public static String computeAcceptKey(String key) {
        MessageDigest sha1 = SHA1_DIGEST.get();
        sha1.reset();

        // Concatenate key + GUID
        sha1.update(key.getBytes(StandardCharsets.US_ASCII));
        sha1.update(WEBSOCKET_GUID.getBytes(StandardCharsets.US_ASCII));

        // Compute SHA-1 hash and base64 encode
        byte[] hash = sha1.digest();
        return Base64.getEncoder().encodeToString(hash);
    }

    /**
     * Writes the WebSocket handshake response to the given buffer.
     *
     * @param buf       the buffer to write to
     * @param acceptKey the computed Sec-WebSocket-Accept value
     * @return the number of bytes written
     */
    public static int writeResponse(long buf, String acceptKey) {
        int offset = 0;

        // Write prefix
        for (byte b : RESPONSE_PREFIX) {
            Unsafe.getUnsafe().putByte(buf + offset++, b);
        }

        // Write accept key
        byte[] acceptBytes = acceptKey.getBytes(StandardCharsets.US_ASCII);
        for (byte b : acceptBytes) {
            Unsafe.getUnsafe().putByte(buf + offset++, b);
        }

        // Write suffix
        for (byte b : RESPONSE_SUFFIX) {
            Unsafe.getUnsafe().putByte(buf + offset++, b);
        }

        return offset;
    }

    /**
     * Returns the size of the handshake response for the given accept key.
     *
     * @param acceptKey the computed accept key
     * @return the total response size in bytes
     */
    public static int responseSize(String acceptKey) {
        return RESPONSE_PREFIX.length + acceptKey.length() + RESPONSE_SUFFIX.length;
    }

    /**
     * Writes the WebSocket handshake response with an optional subprotocol.
     *
     * @param buf       the buffer to write to
     * @param acceptKey the computed Sec-WebSocket-Accept value
     * @param protocol  the negotiated subprotocol (may be null or empty)
     * @return the number of bytes written
     */
    public static int writeResponseWithProtocol(long buf, String acceptKey, String protocol) {
        int offset = 0;

        // Write prefix
        for (byte b : RESPONSE_PREFIX) {
            Unsafe.getUnsafe().putByte(buf + offset++, b);
        }

        // Write accept key
        byte[] acceptBytes = acceptKey.getBytes(StandardCharsets.US_ASCII);
        for (byte b : acceptBytes) {
            Unsafe.getUnsafe().putByte(buf + offset++, b);
        }

        // Write protocol header if present
        if (protocol != null && !protocol.isEmpty()) {
            byte[] protocolHeader = ("\r\nSec-WebSocket-Protocol: " + protocol).getBytes(StandardCharsets.US_ASCII);
            for (byte b : protocolHeader) {
                Unsafe.getUnsafe().putByte(buf + offset++, b);
            }
        }

        // Write suffix
        for (byte b : RESPONSE_SUFFIX) {
            Unsafe.getUnsafe().putByte(buf + offset++, b);
        }

        return offset;
    }

    /**
     * Returns the size of the handshake response with an optional subprotocol.
     *
     * @param acceptKey the computed accept key
     * @param protocol  the negotiated subprotocol (may be null or empty)
     * @return the total response size in bytes
     */
    public static int responseSizeWithProtocol(String acceptKey, String protocol) {
        int size = RESPONSE_PREFIX.length + acceptKey.length() + RESPONSE_SUFFIX.length;
        if (protocol != null && !protocol.isEmpty()) {
            size += "\r\nSec-WebSocket-Protocol: ".length() + protocol.length();
        }
        return size;
    }

    /**
     * Writes a 400 Bad Request response.
     *
     * @param buf    the buffer to write to
     * @param reason the reason for the bad request
     * @return the number of bytes written
     */
    public static int writeBadRequestResponse(long buf, String reason) {
        int offset = 0;

        byte[] statusLine = "HTTP/1.1 400 Bad Request\r\n".getBytes(StandardCharsets.US_ASCII);
        for (byte b : statusLine) {
            Unsafe.getUnsafe().putByte(buf + offset++, b);
        }

        byte[] contentType = "Content-Type: text/plain\r\n".getBytes(StandardCharsets.US_ASCII);
        for (byte b : contentType) {
            Unsafe.getUnsafe().putByte(buf + offset++, b);
        }

        byte[] reasonBytes = reason != null ? reason.getBytes(StandardCharsets.UTF_8) : new byte[0];
        byte[] contentLength = ("Content-Length: " + reasonBytes.length + "\r\n\r\n").getBytes(StandardCharsets.US_ASCII);
        for (byte b : contentLength) {
            Unsafe.getUnsafe().putByte(buf + offset++, b);
        }

        for (byte b : reasonBytes) {
            Unsafe.getUnsafe().putByte(buf + offset++, b);
        }

        return offset;
    }

    /**
     * Writes a 426 Upgrade Required response indicating unsupported WebSocket version.
     *
     * @param buf the buffer to write to
     * @return the number of bytes written
     */
    public static int writeVersionNotSupportedResponse(long buf) {
        int offset = 0;

        byte[] statusLine = "HTTP/1.1 426 Upgrade Required\r\n".getBytes(StandardCharsets.US_ASCII);
        for (byte b : statusLine) {
            Unsafe.getUnsafe().putByte(buf + offset++, b);
        }

        byte[] versionHeader = "Sec-WebSocket-Version: 13\r\n".getBytes(StandardCharsets.US_ASCII);
        for (byte b : versionHeader) {
            Unsafe.getUnsafe().putByte(buf + offset++, b);
        }

        byte[] contentLength = "Content-Length: 0\r\n\r\n".getBytes(StandardCharsets.US_ASCII);
        for (byte b : contentLength) {
            Unsafe.getUnsafe().putByte(buf + offset++, b);
        }

        return offset;
    }

    /**
     * Validates all required headers for a WebSocket upgrade request.
     *
     * @param upgradeHeader   the Upgrade header value
     * @param connectionHeader the Connection header value
     * @param keyHeader       the Sec-WebSocket-Key header value
     * @param versionHeader   the Sec-WebSocket-Version header value
     * @return null if valid, or an error message describing the problem
     */
    public static String validate(
            Utf8Sequence upgradeHeader,
            Utf8Sequence connectionHeader,
            Utf8Sequence keyHeader,
            Utf8Sequence versionHeader
    ) {
        if (!isWebSocketUpgrade(upgradeHeader)) {
            return "Missing or invalid Upgrade header";
        }
        if (!isConnectionUpgrade(connectionHeader)) {
            return "Missing or invalid Connection header";
        }
        if (!isValidKey(keyHeader)) {
            return "Missing or invalid Sec-WebSocket-Key header";
        }
        if (!isValidVersion(versionHeader)) {
            return "Unsupported WebSocket version";
        }
        return null;
    }
}
