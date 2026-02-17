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

import io.questdb.cairo.SecurityContext;
import io.questdb.cutlass.http.ActiveConnectionTracker;
import io.questdb.cutlass.http.HttpConnectionContext;
import io.questdb.cutlass.http.HttpRequestHandler;
import io.questdb.cutlass.http.HttpRequestHeader;
import io.questdb.cutlass.http.HttpRequestProcessor;
import io.questdb.network.PeerDisconnectedException;
import io.questdb.network.PeerIsSlowToReadException;

// Inherits the same connection limit gauges as the ILP HTTP processor
// This processor handles compatibility with InfluxDB line drivers that ping the server.
public class LineHttpPingProcessor implements HttpRequestProcessor, HttpRequestHandler {
    private final String header;

    public LineHttpPingProcessor(CharSequence version) {
        this.header = "X-Influxdb-Version: " + version;
    }

    public String getName() {
        return ActiveConnectionTracker.PROCESSOR_ILP_HTTP;
    }

    @Override
    public HttpRequestProcessor getProcessor(HttpRequestHeader requestHeader) {
        return this;
    }

    @Override
    public byte getRequiredAuthType() {
        return SecurityContext.AUTH_TYPE_NONE;
    }

    @Override
    public void onRequestComplete(
            HttpConnectionContext context
    ) throws PeerDisconnectedException, PeerIsSlowToReadException {
        context.simpleResponse().sendStatusNoContent(204, header);
    }

    @Override
    public void resumeSend(HttpConnectionContext context) throws PeerIsSlowToReadException, PeerDisconnectedException {
        context.simpleResponse().sendStatusNoContent(204, header);
    }
}
