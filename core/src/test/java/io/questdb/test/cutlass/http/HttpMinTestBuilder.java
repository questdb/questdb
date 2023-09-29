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

package io.questdb.test.cutlass.http;

import io.questdb.Metrics;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoEngine;
import io.questdb.cutlass.http.DefaultHttpServerConfiguration;
import io.questdb.cutlass.http.HttpRequestProcessor;
import io.questdb.cutlass.http.HttpRequestProcessorFactory;
import io.questdb.cutlass.http.HttpServer;
import io.questdb.cutlass.http.processors.PrometheusMetricsProcessor;
import io.questdb.cutlass.http.processors.QueryCache;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.metrics.Scrapable;
import io.questdb.mp.WorkerPool;
import io.questdb.test.cairo.DefaultTestCairoConfiguration;
import io.questdb.test.mp.TestWorkerPool;
import org.junit.rules.TemporaryFolder;

import static io.questdb.test.tools.TestUtils.assertMemoryLeak;

public class HttpMinTestBuilder {

    private static final Log LOG = LogFactory.getLog(HttpMinTestBuilder.class);
    private Scrapable scrapable;
    private TemporaryFolder temp;

    public void run(HttpQueryTestBuilder.HttpClientCode code) throws Exception {
        assertMemoryLeak(() -> {
            final String baseDir = temp.getRoot().getAbsolutePath();
            final DefaultHttpServerConfiguration httpConfiguration = new HttpServerConfigurationBuilder()
                    .withBaseDir(temp.getRoot().getAbsolutePath())
                    .build();

            final WorkerPool workerPool = new TestWorkerPool(1);

            CairoConfiguration cairoConfiguration = new DefaultTestCairoConfiguration(baseDir);

            try (
                    CairoEngine engine = new CairoEngine(cairoConfiguration, Metrics.disabled());
                    HttpServer httpServer = new HttpServer(httpConfiguration, engine.getMessageBus(), Metrics.disabled(), workerPool)
            ) {
                httpServer.bind(new HttpRequestProcessorFactory() {
                    @Override
                    public String getUrl() {
                        return "/metrics";
                    }

                    @Override
                    public HttpRequestProcessor newInstance() {
                        return new PrometheusMetricsProcessor(scrapable, httpConfiguration);
                    }
                });

                QueryCache.configure(httpConfiguration, Metrics.disabled());

                workerPool.start(LOG);

                try {
                    code.run(engine);
                } finally {
                    workerPool.halt();
                }
            }
        });
    }

    public HttpMinTestBuilder withScrapable(Scrapable scrapable) {
        this.scrapable = scrapable;
        return this;
    }

    public HttpMinTestBuilder withTempFolder(TemporaryFolder temp) {
        this.temp = temp;
        return this;
    }
}
