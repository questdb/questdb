/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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

import io.questdb.cutlass.http.HttpChunkedResponseSocket;
import io.questdb.cutlass.http.HttpConnectionContext;
import io.questdb.cutlass.http.HttpMinServerConfiguration;
import io.questdb.cutlass.http.HttpRequestProcessor;
import io.questdb.metrics.Scrapable;
import io.questdb.network.PeerDisconnectedException;
import io.questdb.network.PeerIsSlowToReadException;

public class PrometheusMetricsProcessor implements HttpRequestProcessor {
    private static final CharSequence CONTENT_TYPE_TEXT = "text/plain; version=0.0.4; charset=utf-8";
    private final Scrapable metrics;
    private final boolean requiresAuthentication;

    public PrometheusMetricsProcessor(Scrapable metrics, HttpMinServerConfiguration configuration) {
        this.metrics = metrics;
        this.requiresAuthentication = configuration.isHealthCheckAuthenticationRequired();
    }

    @Override
    public void onRequestComplete(HttpConnectionContext context) throws PeerDisconnectedException, PeerIsSlowToReadException {
        HttpChunkedResponseSocket r = context.getChunkedResponseSocket();
        r.status(200, CONTENT_TYPE_TEXT);
        r.sendHeader();

        metrics.scrapeIntoPrometheus(r);

        r.done();
    }

    @Override
    public boolean requiresAuthentication() {
        return requiresAuthentication;
    }
}
