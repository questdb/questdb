/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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

package io.questdb.cutlass.http;

import io.questdb.Metrics;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.DefaultCairoConfiguration;
import io.questdb.cutlass.http.processors.PrometheusMetricsProcessor;
import io.questdb.cutlass.http.processors.QueryCache;
import io.questdb.metrics.Scrapable;
import io.questdb.mp.TestWorkerPoolConfiguration;
import io.questdb.mp.WorkerPool;
import io.questdb.mp.WorkerPoolManager;
import org.junit.rules.TemporaryFolder;

import static io.questdb.test.tools.TestUtils.assertMemoryLeak;

public class HttpMinTestBuilder {

    private TemporaryFolder temp;
    private Scrapable scrapable;

    private WorkerPoolManager workerPoolManager = new WorkerPoolManager();

    public HttpMinTestBuilder withTempFolder(TemporaryFolder temp) {
        this.temp = temp;
        return this;
    }

    public HttpMinTestBuilder withScrapable(Scrapable scrapable) {
        this.scrapable = scrapable;
        return this;
    }

    public void run(HttpQueryTestBuilder.HttpClientCode code) throws Exception {
        assertMemoryLeak(() -> {
            final String baseDir = temp.getRoot().getAbsolutePath();
            final DefaultHttpServerConfiguration httpConfiguration = new HttpServerConfigurationBuilder()
                    .withBaseDir(temp.getRoot().getAbsolutePath())
                    .build();

            DefaultCairoConfiguration cairoConfiguration = new DefaultCairoConfiguration(baseDir);

            try (
                    CairoEngine engine = new CairoEngine(cairoConfiguration, Metrics.disabled());
                    WorkerPool workerPool = workerPoolManager.getInstance(new TestWorkerPoolConfiguration(1), Metrics.disabled());
                    HttpServer httpServer = new HttpServer(httpConfiguration, engine.getMessageBus(), Metrics.disabled(), workerPool)
            ) {
                httpServer.bind(new HttpRequestProcessorFactory() {
                    @Override
                    public HttpRequestProcessor newInstance() {
                        return new PrometheusMetricsProcessor(scrapable);
                    }

                    @Override
                    public String getUrl() {
                        return "/metrics";
                    }
                });

                QueryCache.configure(httpConfiguration);

                workerPoolManager.startAll();
                code.run(engine);
            } finally {
                workerPoolManager.closeAll();
            }
        });
    }
}
