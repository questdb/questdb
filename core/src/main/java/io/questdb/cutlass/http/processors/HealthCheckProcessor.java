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

import io.questdb.cutlass.http.HttpChunkedResponse;
import io.questdb.cutlass.http.HttpConnectionContext;
import io.questdb.cutlass.http.HttpRequestHandler;
import io.questdb.cutlass.http.HttpRequestHeader;
import io.questdb.cutlass.http.HttpRequestProcessor;
import io.questdb.cutlass.http.HttpServerConfiguration;
import io.questdb.metrics.HealthMetricsImpl;
import io.questdb.network.PeerDisconnectedException;
import io.questdb.network.PeerIsSlowToReadException;

public class HealthCheckProcessor implements HttpRequestProcessor, HttpRequestHandler {

    private final boolean pessimisticMode;
    private final byte requiredAuthType;

    public HealthCheckProcessor(HttpServerConfiguration configuration) {
        this.pessimisticMode = configuration.isPessimisticHealthCheckEnabled();
        this.requiredAuthType = configuration.getRequiredAuthType();
    }

    @Override
    public HttpRequestProcessor getDefaultProcessor() {
        return this;
    }

    @Override
    public HttpRequestProcessor getProcessor(HttpRequestHeader requestHeader) {
        return this;
    }

    @Override
    public byte getRequiredAuthType() {
        return requiredAuthType;
    }

    @Override
    public void onRequestComplete(HttpConnectionContext context) throws PeerDisconnectedException, PeerIsSlowToReadException {
        HttpChunkedResponse response = context.getChunkedResponse();

        if (pessimisticMode) {
            final HealthMetricsImpl metrics = context.getMetrics().healthMetrics();
            final long unhandledErrors = metrics.unhandledErrorsCount();
            if (unhandledErrors > 0) {
                response.status(500, "text/plain");
                response.sendHeader();
                response.putAscii("Status: Unhealthy\nUnhandled errors: ");
                response.put(unhandledErrors);
                response.sendChunk(true);
                return;
            }
        }

        response.status(200, "text/plain");
        response.sendHeader();
        response.putAscii("Status: Healthy");
        response.sendChunk(true);
    }
}
