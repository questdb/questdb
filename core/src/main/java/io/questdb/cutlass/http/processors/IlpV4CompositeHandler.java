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

package io.questdb.cutlass.http.processors;

import io.questdb.cairo.CairoEngine;
import io.questdb.cutlass.http.HttpFullFatServerConfiguration;
import io.questdb.cutlass.http.HttpRequestHandler;
import io.questdb.cutlass.http.HttpRequestHeader;
import io.questdb.cutlass.http.HttpRequestProcessor;
import io.questdb.cutlass.http.websocket.IlpV4WebSocketHttpProcessor;

/**
 * Composite HTTP request handler for ILP v4 endpoint that supports both
 * regular HTTP POST requests and WebSocket upgrade requests.
 * <p>
 * This handler routes requests based on their type:
 * <ul>
 *   <li>WebSocket upgrade requests → IlpV4WebSocketUpgradeProcessor</li>
 *   <li>HTTP POST requests → IlpV4HttpProcessor</li>
 * </ul>
 */
public class IlpV4CompositeHandler implements HttpRequestHandler {

    private final IlpV4HttpProcessor httpProcessor;
    private final IlpV4WebSocketHttpProcessor webSocketHandler;

    public IlpV4CompositeHandler(CairoEngine engine, HttpFullFatServerConfiguration httpConfiguration) {
        this.httpProcessor = new IlpV4HttpProcessor(engine, httpConfiguration);
        this.webSocketHandler = new IlpV4WebSocketHttpProcessor(engine, httpConfiguration);
    }

    @Override
    public HttpRequestProcessor getProcessor(HttpRequestHeader requestHeader) {
        // First, check if this is a WebSocket upgrade request
        if (IlpV4WebSocketHttpProcessor.isWebSocketUpgradeRequest(requestHeader)) {
            return webSocketHandler.getProcessor(requestHeader);
        }
        // Otherwise, use the standard HTTP processor
        return httpProcessor.getProcessor(requestHeader);
    }

    @Override
    public HttpRequestProcessor getDefaultProcessor() {
        return httpProcessor.getDefaultProcessor();
    }
}
