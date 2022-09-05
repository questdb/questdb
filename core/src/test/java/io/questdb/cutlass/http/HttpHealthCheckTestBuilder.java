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
import io.questdb.cutlass.Services;
import io.questdb.cutlass.http.processors.QueryCache;
import io.questdb.griffin.SqlException;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.TestWorkerPoolConfiguration;
import io.questdb.mp.WorkerPool;
import io.questdb.mp.WorkerPoolManager;
import io.questdb.std.Os;
import org.junit.rules.TemporaryFolder;

import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.atomic.AtomicBoolean;

import static io.questdb.test.tools.TestUtils.assertMemoryLeak;

public class HttpHealthCheckTestBuilder {

    private static final Log LOG = LogFactory.getLog(HttpHealthCheckTestBuilder.class);

    private Metrics metrics;
    private TemporaryFolder temp;
    private boolean injectUnhandledError;

    private WorkerPoolManager workerPoolManager = new WorkerPoolManager();

    public void run(HttpClientCode code) throws Exception {
        assertMemoryLeak(() -> {
            final String baseDir = temp.getRoot().getAbsolutePath();
            final DefaultHttpServerConfiguration httpConfiguration = new HttpServerConfigurationBuilder()
                    .withBaseDir(baseDir)
                    .build();
            QueryCache.configure(httpConfiguration);

            if (metrics == null) {
                metrics = Metrics.enabled();
            }

            DefaultCairoConfiguration cairoConfiguration = new DefaultCairoConfiguration(baseDir);
            try (CairoEngine engine = new CairoEngine(cairoConfiguration, metrics)) {
                WorkerPool workerPool = workerPoolManager.getInstance(new TestWorkerPoolConfiguration(1), metrics);
                if (injectUnhandledError) {
                    final AtomicBoolean alreadyErrored = new AtomicBoolean();
                    workerPool.assign(workerId -> {
                        if (!alreadyErrored.getAndSet(true)) {
                            throw new NullPointerException("you'd better not handle me");
                        }
                        return false;
                    });
                }
                try (HttpServer ignored = Services.createMinHttpServer(httpConfiguration, workerPoolManager, engine, metrics)) {
                    workerPoolManager.startAll();
                    if (injectUnhandledError && metrics.isEnabled()) {
                        for (int i = 0; i < 40; i++) {
                            if (metrics.healthCheck().unhandledErrorsCount() > 0) {
                                break;
                            }
                            Os.sleep(50);
                        }
                    }

                    code.run(engine);
                } finally {
                    workerPoolManager.closeAll();
                }
            }
        });
    }

    public HttpHealthCheckTestBuilder withMetrics(Metrics metrics) {
        this.metrics = metrics;
        return this;
    }

    public HttpHealthCheckTestBuilder withInjectedUnhandledError() {
        this.injectUnhandledError = true;
        return this;
    }

    public HttpHealthCheckTestBuilder withTempFolder(TemporaryFolder temp) {
        this.temp = temp;
        return this;
    }

    @FunctionalInterface
    public interface HttpClientCode {
        void run(CairoEngine engine) throws InterruptedException, SqlException, BrokenBarrierException;
    }
}
