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

package io.questdb.cutlass.ilpv4.server;

import io.questdb.cutlass.ilpv4.protocol.*;

import io.questdb.cutlass.ilpv4.websocket.*;

import io.questdb.cairo.CairoEngine;
import io.questdb.cutlass.http.HttpFullFatServerConfiguration;
import io.questdb.cutlass.http.HttpRequestHandler;
import io.questdb.cutlass.http.HttpRequestHeader;
import io.questdb.cutlass.http.HttpRequestProcessor;
import io.questdb.std.str.Utf8Sequence;

/**
 * HTTP request handler for ILP v4 WebSocket connections.
 * <p>
 * This handler detects WebSocket upgrade requests and returns an appropriate
 * processor to handle the WebSocket protocol upgrade and subsequent communication.
 */
public class IlpV4WebSocketHttpProcessor implements HttpRequestHandler {

    private final IlpV4WebSocketUpgradeProcessor processor;

    /**
     * Creates a new ILP v4 WebSocket HTTP processor.
     *
     * @param engine            the Cairo engine for database access
     * @param httpConfiguration the HTTP server configuration
     */
    public IlpV4WebSocketHttpProcessor(CairoEngine engine, HttpFullFatServerConfiguration httpConfiguration) {
        this.processor = new IlpV4WebSocketUpgradeProcessor(engine, httpConfiguration);
    }

    @Override
    public HttpRequestProcessor getProcessor(HttpRequestHeader requestHeader) {
        // Always return the same processor instance. Per-connection state lives
        // in LocalValue, so the instance is safe to share. Returning unconditionally
        // is required because resolveProcessorById() calls this after headers are
        // cleared (post-protocol-switch).
        return processor;
    }

    /**
     * Checks if the request is a valid WebSocket upgrade request.
     *
     * @param header the HTTP request header
     * @return true if this is a valid WebSocket upgrade request
     */
    public static boolean isWebSocketUpgradeRequest(HttpRequestHeader header) {
        // Check Upgrade header
        Utf8Sequence upgradeHeader = header.getHeader(WebSocketHandshake.HEADER_UPGRADE);
        if (!WebSocketHandshake.isWebSocketUpgrade(upgradeHeader)) {
            return false;
        }

        // Check Connection header
        Utf8Sequence connectionHeader = header.getHeader(WebSocketHandshake.HEADER_CONNECTION);
        if (!WebSocketHandshake.isConnectionUpgrade(connectionHeader)) {
            return false;
        }

        // Check Sec-WebSocket-Key
        Utf8Sequence keyHeader = header.getHeader(WebSocketHandshake.HEADER_SEC_WEBSOCKET_KEY);
        if (!WebSocketHandshake.isValidKey(keyHeader)) {
            return false;
        }

        // Check Sec-WebSocket-Version
        Utf8Sequence versionHeader = header.getHeader(WebSocketHandshake.HEADER_SEC_WEBSOCKET_VERSION);
        if (!WebSocketHandshake.isValidVersion(versionHeader)) {
            return false;
        }

        return true;
    }

    /**
     * Validates WebSocket handshake headers and returns an error message if invalid.
     *
     * @param header the HTTP request header
     * @return null if valid, error message otherwise
     */
    public static String validateHandshake(HttpRequestHeader header) {
        // Check Upgrade header
        Utf8Sequence upgradeHeader = header.getHeader(WebSocketHandshake.HEADER_UPGRADE);
        if (upgradeHeader == null) {
            return "Missing Upgrade header";
        }
        if (!WebSocketHandshake.isWebSocketUpgrade(upgradeHeader)) {
            return "Invalid Upgrade header value";
        }

        // Check Connection header
        Utf8Sequence connectionHeader = header.getHeader(WebSocketHandshake.HEADER_CONNECTION);
        if (connectionHeader == null) {
            return "Missing Connection header";
        }
        if (!WebSocketHandshake.isConnectionUpgrade(connectionHeader)) {
            return "Connection header must contain 'upgrade'";
        }

        // Check Sec-WebSocket-Key
        Utf8Sequence keyHeader = header.getHeader(WebSocketHandshake.HEADER_SEC_WEBSOCKET_KEY);
        if (keyHeader == null) {
            return "Missing Sec-WebSocket-Key header";
        }
        if (!WebSocketHandshake.isValidKey(keyHeader)) {
            return "Invalid Sec-WebSocket-Key (must be 24-character base64 key)";
        }

        // Check Sec-WebSocket-Version
        Utf8Sequence versionHeader = header.getHeader(WebSocketHandshake.HEADER_SEC_WEBSOCKET_VERSION);
        if (versionHeader == null) {
            return "Missing Sec-WebSocket-Version header";
        }
        if (!WebSocketHandshake.isValidVersion(versionHeader)) {
            return "Unsupported WebSocket version (must be 13)";
        }

        return null;
    }

    /**
     * Gets the WebSocket key from the request header.
     *
     * @param header the HTTP request header
     * @return the WebSocket key, or null if not present
     */
    public static Utf8Sequence getWebSocketKey(HttpRequestHeader header) {
        return header.getHeader(WebSocketHandshake.HEADER_SEC_WEBSOCKET_KEY);
    }
}
