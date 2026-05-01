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

package io.questdb.cutlass.qwp.server.egress;

import io.questdb.cairo.CairoEngine;
import io.questdb.cutlass.http.HttpFullFatServerConfiguration;
import io.questdb.cutlass.http.HttpRequestHandler;
import io.questdb.cutlass.http.HttpRequestHeader;
import io.questdb.cutlass.http.HttpRequestProcessor;
import io.questdb.std.Misc;
import io.questdb.std.QuietCloseable;

/**
 * HTTP request handler for QWP egress (query results) WebSocket connections at /read/v1.
 * <p>
 * Mirrors {@link io.questdb.cutlass.qwp.server.QwpWebSocketHttpProcessor} for the
 * server-to-client direction. The handshake validation and 101 response writing
 * are reused via static helpers; only the post-upgrade behavior differs.
 */
public class QwpEgressHttpProcessor implements HttpRequestHandler, QuietCloseable {

    private final QwpEgressUpgradeProcessor processor;

    public QwpEgressHttpProcessor(
            CairoEngine engine,
            HttpFullFatServerConfiguration httpConfiguration,
            int sharedWorkerCount
    ) {
        this.processor = new QwpEgressUpgradeProcessor(engine, httpConfiguration, sharedWorkerCount);
    }

    @Override
    public void close() {
        Misc.free(processor);
    }

    @Override
    public HttpRequestProcessor getProcessor(HttpRequestHeader requestHeader) {
        return processor;
    }
}
