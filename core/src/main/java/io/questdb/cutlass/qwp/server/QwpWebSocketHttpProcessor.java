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
import io.questdb.cutlass.qwp.protocol.QwpConstants;
import io.questdb.std.Numbers;
import io.questdb.std.Unsafe;
import io.questdb.std.str.Utf8Sequence;
import io.questdb.std.str.Utf8String;
import io.questdb.std.str.Utf8s;

import java.nio.charset.StandardCharsets;
import java.security.DigestException;
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
    // Expected value for HEADER_X_QWP_REQUEST_DURABLE_ACK to enable durable-ack; compared case-insensitively.
    public static final Utf8String HEADER_VALUE_DURABLE_ACK_ENABLED = new Utf8String("true");
    // QWP version negotiation headers
    public static final Utf8String HEADER_X_QWP_ACCEPT_ENCODING = new Utf8String("X-QWP-Accept-Encoding");
    public static final Utf8String HEADER_X_QWP_CLIENT_ID = new Utf8String("X-QWP-Client-Id");
    public static final Utf8String HEADER_X_QWP_MAX_BATCH_ROWS = new Utf8String("X-QWP-Max-Batch-Rows");
    public static final Utf8String HEADER_X_QWP_MAX_VERSION = new Utf8String("X-QWP-Max-Version");
    // Client opt-in for STATUS_DURABLE_ACK frames. Value "true" (case-insensitive) enables.
    // Any other value, or header absent, leaves the feature disabled for this connection.
    public static final Utf8String HEADER_X_QWP_REQUEST_DURABLE_ACK = new Utf8String("X-QWP-Request-Durable-Ack");
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
    // Package-private so QwpWebSocketUpgradeProcessor can precompute a complete
    // 400 Bad Request response per error constant at class-init time, sparing
    // the reject path the per-call reason.getBytes / Integer.toString /
    // contentLength.getBytes allocations.
    static final String ERROR_CONNECTION_MUST_CONTAIN_UPGRADE = "Connection header must contain 'upgrade'";
    static final String ERROR_INVALID_SEC_WEBSOCKET_KEY = "Invalid Sec-WebSocket-Key (must be 24-character base64 key)";
    static final String ERROR_INVALID_UPGRADE_HEADER_VALUE = "Invalid Upgrade header value";
    static final String ERROR_MISSING_CONNECTION_HEADER = "Missing Connection header";
    static final String ERROR_MISSING_SEC_WEBSOCKET_KEY_HEADER = "Missing Sec-WebSocket-Key header";
    static final String ERROR_MISSING_SEC_WEBSOCKET_VERSION_HEADER = "Missing Sec-WebSocket-Version header";
    static final String ERROR_MISSING_UPGRADE_HEADER = "Missing Upgrade header";
    static final String ERROR_ORIGIN_HEADER_NOT_ALLOWED = "Origin header not allowed on QWP WebSocket";
    static final String ERROR_UNSUPPORTED_WEBSOCKET_VERSION = "Unsupported WebSocket version (must be 13)";
    // Sec-WebSocket-Key is defined by RFC 6455 as a 16-byte base64 value --
    // exactly 24 ASCII bytes on the wire. 64 bytes leaves defensive headroom
    // for callers that bypass {@link #isValidKey}.
    private static final int KEY_SCRATCH_SIZE = 64;
    // Per-thread scratch so the accept-key computation runs with zero byte[] allocs
    // under sustained reconnect load.
    private static final ThreadLocal<byte[]> KEY_SCRATCH = ThreadLocal.withInitial(() -> new byte[KEY_SCRATCH_SIZE]);
    private static final byte[] MISDIRECTED_REQUEST_PREFIX =
            ("""
                    HTTP/1.1 421 Misdirected Request\r
                    Connection: close\r
                    Content-Length: 0\r
                    X-QuestDB-Role:\s""").getBytes(StandardCharsets.US_ASCII);
    private static final byte[] RESPONSE_AFTER_ACCEPT = "\r\nX-QWP-Version: ".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] RESPONSE_CONTENT_ENCODING_PREFIX =
            "\r\nX-QWP-Content-Encoding: ".getBytes(StandardCharsets.US_ASCII);
    // Echoed back to clients that opted in via X-QWP-Request-Durable-Ack and
    // landed on a server where the durable-ack registry is enabled. Absence
    // tells an opted-in client that this server will never emit STATUS_DURABLE_ACK
    // frames, so the client must fail at handshake rather than wait forever.
    private static final byte[] RESPONSE_DURABLE_ACK_ENABLED =
            "\r\nX-QWP-Durable-Ack: enabled".getBytes(StandardCharsets.US_ASCII);
    // Advertises the server's hard cap on QWP message payload bytes so the
    // ingest client can size its batches without trial-and-error. Without this
    // hint a wide-row sender would have to discover the cap by sending an
    // oversized batch and reacting to STATUS_PARSE_ERROR.
    private static final byte[] RESPONSE_MAX_BATCH_SIZE_PREFIX =
            "\r\nX-QWP-Max-Batch-Size: ".getBytes(StandardCharsets.US_ASCII);
    // Response template
    private static final byte[] RESPONSE_PREFIX =
            "HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: ".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] RESPONSE_ROLE_PREFIX = "\r\nX-QuestDB-Role: ".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] RESPONSE_SUFFIX = "\r\n\r\n".getBytes(StandardCharsets.US_ASCII);
    private static final int SHA1_BASE64_SIZE = 28;
    private static final ThreadLocal<byte[]> BASE64_SCRATCH = ThreadLocal.withInitial(() -> new byte[SHA1_BASE64_SIZE]);
    // Thread-local SHA-1 digest for computing Sec-WebSocket-Accept
    private static final ThreadLocal<MessageDigest> SHA1_DIGEST = ThreadLocal.withInitial(() -> {
        try {
            return MessageDigest.getInstance("SHA-1");
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("SHA-1 not available", e);
        }
    });
    // SHA-1 output is always 20 bytes; 28 is the base64 length of a 20-byte input
    // (ceil(20/3)*4 = 28, with no padding needed for inputs divisible by 3... but 20
    // is not, so one '=' padding byte lands in slot 27). The exact 28 matches both.
    private static final int SHA1_DIGEST_SIZE = 20;
    private static final ThreadLocal<byte[]> HASH_SCRATCH = ThreadLocal.withInitial(() -> new byte[SHA1_DIGEST_SIZE]);
    // Precomputed X-QWP-Version digit bytes indexed by version number. Lets the
    // handshake response writer skip per-call Integer.toString + getBytes
    // allocations since the negotiated version is always inside the closed set
    // [VERSION_1, MAX_SUPPORTED_VERSION] the server itself defines.
    private static final byte[][] VERSION_BYTES = buildVersionBytes();
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
     * <p>
     * Returns a reference to a thread-local scratch buffer of length
     * {@link #SHA1_BASE64_SIZE} containing the base64-encoded SHA-1 hash.
     * The caller must consume the bytes (e.g. copy them into the response
     * buffer) before invoking {@code computeAcceptKey} again on the same
     * thread, as the next call overwrites the scratch in place.
     *
     * @param key the Sec-WebSocket-Key from the client
     * @return the base64-encoded SHA-1 hash bytes (length {@link #SHA1_BASE64_SIZE})
     */
    public static byte[] computeAcceptKey(Utf8Sequence key) {
        MessageDigest sha1 = SHA1_DIGEST.get();
        sha1.reset();

        // Copy the base64-ASCII key bytes into a thread-local scratch so the
        // MessageDigest.update(byte[],int,int) overload can consume them without
        // a per-call allocation. Key length is always 24 for a valid handshake,
        // but we fall back to the caller-supplied size in case isValidKey was
        // skipped (e.g. in tests).
        int n = key.size();
        byte[] keyBytes = n <= KEY_SCRATCH_SIZE
                ? KEY_SCRATCH.get()
                // Defensive fallback: caller handed us an oversized key. Allocate
                // locally so we don't blow the scratch invariant for other calls
                // on this thread. Never fires in the validated path.
                : new byte[n];
        for (int i = 0; i < n; i++) {
            keyBytes[i] = key.byteAt(i);
        }
        sha1.update(keyBytes, 0, n);
        sha1.update(WEBSOCKET_GUID_BYTES);

        // digest(byte[],int,int) writes into the supplied buffer -- no per-call
        // byte[] from sha1.digest() and no per-call byte[] from the Base64
        // encoder (which also has a no-alloc encode(byte[],byte[]) overload).
        try {
            byte[] hash = HASH_SCRATCH.get();
            sha1.digest(hash, 0, SHA1_DIGEST_SIZE);
            byte[] b64 = BASE64_SCRATCH.get();
            Base64.getEncoder().encode(hash, b64);
            return b64;
        } catch (DigestException e) {
            throw new RuntimeException("SHA-1 digest failed", e);
        }
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

    public static int misdirectedRequestWithRoleSize(byte[] roleBytes) {
        return MISDIRECTED_REQUEST_PREFIX.length + roleBytes.length + RESPONSE_SUFFIX.length;
    }

    /**
     * Returns the size of the handshake response for the given accept key and QWP version.
     *
     * @param acceptKey  the computed accept key bytes
     * @param qwpVersion the negotiated QWP version number
     * @return the total response size in bytes
     */
    public static int responseSize(byte[] acceptKey, int qwpVersion) {
        return responseSize(acceptKey, qwpVersion, null, false, null, null);
    }

    public static int responseSize(byte[] acceptKey, int qwpVersion, byte[] contentEncodingBytes, boolean durableAckEnabled, byte[] roleBytes) {
        return responseSize(acceptKey, qwpVersion, contentEncodingBytes, durableAckEnabled, roleBytes, null);
    }

    /**
     * Same as {@link #responseSize(byte[], int)} but accounts for an optional
     * {@code X-QWP-Content-Encoding} header echoing the negotiated compression
     * codec, an optional {@code X-QWP-Durable-Ack: enabled} confirmation
     * header, an optional {@code X-QuestDB-Role} header advertising the
     * server role, and an optional {@code X-QWP-Max-Batch-Size} header
     * advertising the server's ingest payload cap in bytes. Pass {@code null}
     * / {@code false} to skip any of them. All byte[] arguments are written
     * verbatim, so callers are expected to cache them on the hot path rather
     * than allocating per handshake.
     */
    public static int responseSize(byte[] acceptKey, int qwpVersion, byte[] contentEncodingBytes, boolean durableAckEnabled, byte[] roleBytes, byte[] maxBatchSizeBytes) {
        int size = RESPONSE_PREFIX.length + acceptKey.length
                + RESPONSE_AFTER_ACCEPT.length + VERSION_BYTES[qwpVersion].length
                + RESPONSE_SUFFIX.length;
        if (contentEncodingBytes != null) {
            size += RESPONSE_CONTENT_ENCODING_PREFIX.length + contentEncodingBytes.length;
        }
        if (durableAckEnabled) {
            size += RESPONSE_DURABLE_ACK_ENABLED.length;
        }
        if (roleBytes != null) {
            size += RESPONSE_ROLE_PREFIX.length + roleBytes.length;
        }
        if (maxBatchSizeBytes != null) {
            size += RESPONSE_MAX_BATCH_SIZE_PREFIX.length + maxBatchSizeBytes.length;
        }
        return size;
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

    public static int writeMisdirectedRequestWithRole(long buf, int bufferSize, byte[] roleBytes) {
        int needed = misdirectedRequestWithRoleSize(roleBytes);
        if (needed > bufferSize) {
            return -1;
        }
        int offset = 0;
        for (byte b : MISDIRECTED_REQUEST_PREFIX) {
            Unsafe.putByte(buf + offset++, b);
        }
        for (byte b : roleBytes) {
            Unsafe.putByte(buf + offset++, b);
        }
        for (byte b : RESPONSE_SUFFIX) {
            Unsafe.putByte(buf + offset++, b);
        }
        return offset;
    }

    /**
     * Writes the WebSocket handshake response to the given buffer.
     *
     * @param buf        the buffer to write to
     * @param acceptKey  the computed Sec-WebSocket-Accept value bytes
     * @param qwpVersion the negotiated QWP version number
     * @return the number of bytes written
     */
    public static int writeResponse(long buf, byte[] acceptKey, int qwpVersion) {
        return writeResponse(buf, acceptKey, qwpVersion, null, false, null, null);
    }

    public static int writeResponse(long buf, byte[] acceptKey, int qwpVersion, byte[] contentEncodingBytes, boolean durableAckEnabled, byte[] roleBytes) {
        return writeResponse(buf, acceptKey, qwpVersion, contentEncodingBytes, durableAckEnabled, roleBytes, null);
    }

    /**
     * Same as {@link #writeResponse(long, byte[], int)} but appends an optional
     * {@code X-QWP-Content-Encoding} header echoing the negotiated compression
     * codec (e.g. {@code zstd;level=1}), an optional
     * {@code X-QWP-Durable-Ack: enabled} confirmation that this connection
     * will receive {@code STATUS_DURABLE_ACK} frames, an optional
     * {@code X-QuestDB-Role} header advertising the server role, and an
     * optional {@code X-QWP-Max-Batch-Size} header advertising the server's
     * ingest payload cap in bytes. Pass {@code null} / {@code false} to skip
     * any of them. All byte[] arguments are written verbatim, so callers are
     * expected to cache them on the hot path rather than allocating per
     * handshake.
     */
    public static int writeResponse(long buf, byte[] acceptKey, int qwpVersion, byte[] contentEncodingBytes, boolean durableAckEnabled, byte[] roleBytes, byte[] maxBatchSizeBytes) {
        int offset = 0;

        for (byte b : RESPONSE_PREFIX) {
            Unsafe.putByte(buf + offset++, b);
        }

        for (byte b : acceptKey) {
            Unsafe.putByte(buf + offset++, b);
        }

        for (byte b : RESPONSE_AFTER_ACCEPT) {
            Unsafe.putByte(buf + offset++, b);
        }
        for (byte b : VERSION_BYTES[qwpVersion]) {
            Unsafe.putByte(buf + offset++, b);
        }

        if (contentEncodingBytes != null) {
            for (byte b : RESPONSE_CONTENT_ENCODING_PREFIX) {
                Unsafe.putByte(buf + offset++, b);
            }
            for (byte b : contentEncodingBytes) {
                Unsafe.putByte(buf + offset++, b);
            }
        }

        // Optional X-QWP-Durable-Ack confirmation. Emitted only when the
        // client opted in AND this server has the durable-ack registry
        // enabled. Absence tells an opted-in client that this connection
        // will never receive durable acks, so its store-and-forward path
        // must not be allowed to start.
        if (durableAckEnabled) {
            for (byte b : RESPONSE_DURABLE_ACK_ENABLED) {
                Unsafe.putByte(buf + offset++, b);
            }
        }

        if (roleBytes != null) {
            for (byte b : RESPONSE_ROLE_PREFIX) {
                Unsafe.putByte(buf + offset++, b);
            }
            for (byte b : roleBytes) {
                Unsafe.putByte(buf + offset++, b);
            }
        }

        if (maxBatchSizeBytes != null) {
            for (byte b : RESPONSE_MAX_BATCH_SIZE_PREFIX) {
                Unsafe.putByte(buf + offset++, b);
            }
            for (byte b : maxBatchSizeBytes) {
                Unsafe.putByte(buf + offset++, b);
            }
        }

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

    private static byte[][] buildVersionBytes() {
        byte[][] table = new byte[QwpConstants.MAX_SUPPORTED_VERSION + 1][];
        for (int v = QwpConstants.VERSION_1; v <= QwpConstants.MAX_SUPPORTED_VERSION; v++) {
            table[v] = Integer.toString(v).getBytes(StandardCharsets.US_ASCII);
        }
        return table;
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
}
