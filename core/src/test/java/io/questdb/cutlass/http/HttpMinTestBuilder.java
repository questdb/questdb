/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
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

import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.DefaultCairoConfiguration;
import io.questdb.cutlass.http.processors.PrometheusMetricsProcessor;
import io.questdb.cutlass.http.processors.QueryCache;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.metrics.Scrapable;
import io.questdb.mp.WorkerPool;
import io.questdb.mp.WorkerPoolConfiguration;
import org.junit.rules.TemporaryFolder;

import java.util.Arrays;

import static io.questdb.test.tools.TestUtils.assertMemoryLeak;

public class HttpMinTestBuilder {

    private static final Log LOG = LogFactory.getLog(HttpMinTestBuilder.class);
    private TemporaryFolder temp;
    private Scrapable metrics;

    public HttpMinTestBuilder withTempFolder(TemporaryFolder temp) {
        this.temp = temp;
        return this;
    }

    public HttpMinTestBuilder withMetrics(Scrapable metrics) {
        this.metrics = metrics;
        return this;
    }

    public void run(HttpQueryTestBuilder.HttpClientCode code) throws Exception {
        final int[] workerAffinity = new int[1];
        Arrays.fill(workerAffinity, -1);

        assertMemoryLeak(() -> {
            final String baseDir = temp.getRoot().getAbsolutePath();
            final DefaultHttpServerConfiguration httpConfiguration = new HttpServerConfigurationBuilder()
                    .withBaseDir(temp.getRoot().getAbsolutePath())
                    .build();

            final WorkerPool workerPool = new WorkerPool(new WorkerPoolConfiguration() {
                @Override
                public int[] getWorkerAffinity() {
                    return workerAffinity;
                }

                @Override
                public int getWorkerCount() {
                    return workerAffinity.length;
                }

                @Override
                public boolean haltOnError() {
                    return false;
                }
            });

            DefaultCairoConfiguration cairoConfiguration = new DefaultCairoConfiguration(baseDir);

            try (
                    CairoEngine engine = new CairoEngine(cairoConfiguration);
                    HttpServer httpServer = new HttpServer(httpConfiguration, workerPool, false)
            ) {
                httpServer.bind(new HttpRequestProcessorFactory() {
                    @Override
                    public HttpRequestProcessor newInstance() {
                        return new PrometheusMetricsProcessor(metrics);
                    }

                    @Override
                    public String getUrl() {
                        return "/metrics";
                    }
                });

                QueryCache.configure(httpConfiguration);

                workerPool.start(LOG);

                try {
                    code.run(engine);
                } finally {
                    workerPool.halt();
                }
            }
        });
    }
}
