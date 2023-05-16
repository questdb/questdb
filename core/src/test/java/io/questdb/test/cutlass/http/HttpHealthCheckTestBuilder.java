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
import io.questdb.cairo.CairoEngine;
import io.questdb.cutlass.Services;
import io.questdb.cutlass.http.DefaultHttpServerConfiguration;
import io.questdb.cutlass.http.HttpServer;
import io.questdb.cutlass.http.processors.QueryCache;
import io.questdb.griffin.SqlException;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.WorkerPool;
import io.questdb.std.Os;
import io.questdb.test.cairo.DefaultTestCairoConfiguration;
import io.questdb.test.mp.TestWorkerPool;
import org.junit.rules.TemporaryFolder;

import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.atomic.AtomicBoolean;

import static io.questdb.test.tools.TestUtils.assertMemoryLeak;

public class HttpHealthCheckTestBuilder {

    private static final Log LOG = LogFactory.getLog(HttpHealthCheckTestBuilder.class);
    private boolean injectUnhandledError;
    private Metrics metrics;
    private boolean pessimisticHealthCheck = false;
    private TemporaryFolder temp;

    public void run(HttpClientCode code) throws Exception {
        assertMemoryLeak(() -> {
            final String baseDir = temp.getRoot().getAbsolutePath();
            final DefaultHttpServerConfiguration httpConfiguration = new HttpServerConfigurationBuilder()
                    .withBaseDir(baseDir)
                    .withPessimisticHealthCheck(pessimisticHealthCheck)
                    .build();
            if (metrics == null) {
                metrics = Metrics.enabled();
            }

            QueryCache.configure(httpConfiguration, metrics);

            WorkerPool workerPool = new TestWorkerPool(1, metrics);

            if (injectUnhandledError) {
                final AtomicBoolean alreadyErrored = new AtomicBoolean();
                workerPool.assign((workerId, runStatus) -> {
                    if (!alreadyErrored.getAndSet(true)) {
                        throw new NullPointerException("you'd better not handle me");
                    }
                    return false;
                });
            }

            DefaultTestCairoConfiguration cairoConfiguration = new DefaultTestCairoConfiguration(baseDir);
            try (
                    CairoEngine engine = new CairoEngine(cairoConfiguration, metrics);
                    HttpServer ignored = Services.createMinHttpServer(httpConfiguration, engine, workerPool, metrics)
            ) {
                workerPool.start(LOG);

                if (injectUnhandledError && metrics.isEnabled()) {
                    for (int i = 0; i < 40; i++) {
                        if (metrics.health().unhandledErrorsCount() > 0) {
                            break;
                        }
                        Os.sleep(50);
                    }
                }

                try {
                    code.run(engine);
                } finally {
                    workerPool.halt();
                }
            }
        });
    }

    public HttpHealthCheckTestBuilder withInjectedUnhandledError() {
        this.injectUnhandledError = true;
        return this;
    }

    public HttpHealthCheckTestBuilder withMetrics(Metrics metrics) {
        this.metrics = metrics;
        return this;
    }

    public HttpHealthCheckTestBuilder withPessimisticHealthCheck(boolean pessimisticHealthCheck) {
        this.pessimisticHealthCheck = pessimisticHealthCheck;
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
