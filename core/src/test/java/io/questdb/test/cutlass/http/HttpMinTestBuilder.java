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

package io.questdb.test.cutlass.http;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.security.AllowAllSecurityContext;
import io.questdb.cutlass.http.DefaultHttpServerConfiguration;
import io.questdb.cutlass.http.HttpRequestHandler;
import io.questdb.cutlass.http.HttpRequestHandlerFactory;
import io.questdb.cutlass.http.HttpServer;
import io.questdb.cutlass.http.processors.HealthCheckProcessor;
import io.questdb.cutlass.http.processors.PrometheusMetricsProcessor;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.metrics.Target;
import io.questdb.mp.WorkerPool;
import io.questdb.network.PlainSocketFactory;
import io.questdb.std.ObjHashSet;
import io.questdb.test.cairo.DefaultTestCairoConfiguration;
import io.questdb.test.mp.TestWorkerPool;
import org.junit.rules.TemporaryFolder;

import static io.questdb.test.tools.TestUtils.assertMemoryLeak;

public class HttpMinTestBuilder {

    private static final Log LOG = LogFactory.getLog(HttpMinTestBuilder.class);
    private PrometheusMetricsProcessor.RequestStatePool prometheusRequestStatePool;
    private int sendBufferSize;
    private Target target;
    private int tcpSndBufSize;
    private TemporaryFolder temp;
    private int workerCount;

    public void run(HttpQueryTestBuilder.HttpClientCode code) throws Exception {
        assertMemoryLeak(() -> {
            final String baseDir = temp.getRoot().getAbsolutePath();

            CairoConfiguration cairoConfiguration = new DefaultTestCairoConfiguration(baseDir);

            final DefaultHttpServerConfiguration httpConfiguration = new HttpServerConfigurationBuilder()
                    .withBaseDir(temp.getRoot().getAbsolutePath())
                    .withTcpSndBufSize(tcpSndBufSize)
                    .withSendBufferSize(sendBufferSize)
                    .withWorkerCount(workerCount)
                    .build(cairoConfiguration);

            final WorkerPool workerPool = new TestWorkerPool(httpConfiguration.getWorkerCount());

            try (
                    CairoEngine engine = new CairoEngine(cairoConfiguration);
                    HttpServer httpServer = new HttpServer(httpConfiguration, workerPool, PlainSocketFactory.INSTANCE);
                    SqlExecutionContext sqlExecutionContext = new SqlExecutionContextImpl(engine, 1).with(AllowAllSecurityContext.INSTANCE)
            ) {
                final PrometheusMetricsProcessor.RequestStatePool requestStatePool = prometheusRequestStatePool != null
                        ? prometheusRequestStatePool
                        : new PrometheusMetricsProcessor.RequestStatePool(httpConfiguration.getWorkerCount());
                httpServer.registerClosable(requestStatePool);
                httpServer.bind(new HttpRequestHandlerFactory() {
                    @Override
                    public ObjHashSet<String> getUrls() {
                        return httpConfiguration.getContextPathMetrics();
                    }

                    @Override
                    public HttpRequestHandler newInstance() {
                        return new PrometheusMetricsProcessor(target, httpConfiguration, requestStatePool);
                    }
                });

                // This `bind` for the default handler is only here to allow checking what the server behaviour is with
                // an external web browser that would issue additional requests to `/favicon.ico`.
                // It mirrors the setup of the min http server.
                httpServer.bind(new HttpRequestHandlerFactory() {
                    @Override
                    public ObjHashSet<String> getUrls() {
                        return httpConfiguration.getContextPathStatus();
                    }

                    @Override
                    public HttpRequestHandler newInstance() {
                        return new HealthCheckProcessor(httpConfiguration);
                    }
                }, true);

                workerPool.start(LOG);

                try {
                    code.run(engine, sqlExecutionContext);
                } finally {
                    workerPool.halt();
                }
            }
        });
    }

    public HttpMinTestBuilder withPrometheusPool(PrometheusMetricsProcessor.RequestStatePool pool) {
        this.prometheusRequestStatePool = pool;
        return this;
    }

    public HttpMinTestBuilder withScrappable(Target target) {
        this.target = target;
        return this;
    }

    public HttpMinTestBuilder withSendBufferSize(int sendBufferSize) {
        this.sendBufferSize = sendBufferSize;
        return this;
    }

    public HttpMinTestBuilder withTcpSndBufSize(int tcpSndBufSize) {
        this.tcpSndBufSize = tcpSndBufSize;
        return this;
    }

    public HttpMinTestBuilder withTempFolder(TemporaryFolder temp) {
        this.temp = temp;
        return this;
    }

    public HttpMinTestBuilder withWorkerCount(int workerCount) {
        this.workerCount = workerCount;
        return this;
    }
}
