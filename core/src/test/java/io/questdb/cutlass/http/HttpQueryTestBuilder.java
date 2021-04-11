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

import io.questdb.MessageBus;
import io.questdb.MessageBusImpl;
import io.questdb.TelemetryJob;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.DefaultCairoConfiguration;
import io.questdb.cutlass.http.processors.*;
import io.questdb.griffin.SqlException;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.WorkerPool;
import io.questdb.mp.WorkerPoolConfiguration;
import io.questdb.std.Misc;
import org.jetbrains.annotations.Nullable;
import org.junit.rules.TemporaryFolder;

import java.util.Arrays;

import static io.questdb.test.tools.TestUtils.assertMemoryLeak;

public class HttpQueryTestBuilder {

    @FunctionalInterface
    public interface HttpRequestProcessorBuilder {
        HttpRequestProcessor create(JsonQueryProcessorConfiguration configuration,
                                    CairoEngine engine,
                                    @Nullable MessageBus messageBus,
                                    int workerCount);
    }

    private static final Log LOG = LogFactory.getLog(HttpQueryTestBuilder.class);
    private boolean telemetry;
    private TemporaryFolder temp;
    private HttpServerConfigurationBuilder serverConfigBuilder;
    private HttpRequestProcessorBuilder textImportProcessor;

    @FunctionalInterface
    public interface HttpClientCode {
        void run(CairoEngine engine) throws InterruptedException, SqlException;
    }

    private int workerCount = 1;

    public HttpQueryTestBuilder withWorkerCount(int workerCount) {
        this.workerCount = workerCount;
        return this;
    }

    public int getWorkerCount() {
        return this.workerCount;
    }


    public HttpQueryTestBuilder withTelemetry(boolean telemetry) {
        this.telemetry = telemetry;
        return this;
    }

    public HttpQueryTestBuilder withTempFolder(TemporaryFolder temp) {
        this.temp = temp;
        return this;
    }

    public HttpQueryTestBuilder withHttpServerConfigBuilder(HttpServerConfigurationBuilder serverConfigBuilder) {
        this.serverConfigBuilder = serverConfigBuilder;
        return this;
    }


    public HttpQueryTestBuilder withCustomTextImportProcessor(HttpRequestProcessorBuilder textQueryProcessor) {
        this.textImportProcessor = textQueryProcessor;
        return this;
    }

    public void run(HttpClientCode code) throws Exception {
        final int[] workerAffinity = new int[workerCount];
        Arrays.fill(workerAffinity, -1);

        assertMemoryLeak(() -> {
            final String baseDir = temp.getRoot().getAbsolutePath();
            final DefaultHttpServerConfiguration httpConfiguration = serverConfigBuilder
                    .withBaseDir(temp.getRoot().getAbsolutePath())
                    .build();

            final WorkerPool workerPool = new WorkerPool(new WorkerPoolConfiguration() {
                @Override
                public int[] getWorkerAffinity() {
                    return workerAffinity;
                }

                @Override
                public int getWorkerCount() {
                    return workerCount;
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
                TelemetryJob telemetryJob = null;
                final MessageBus messageBus = new MessageBusImpl(cairoConfiguration);
                if (telemetry) {
                    telemetryJob = new TelemetryJob(engine);
                }
                httpServer.bind(new HttpRequestProcessorFactory() {
                    @Override
                    public HttpRequestProcessor newInstance() {
                        return new StaticContentProcessor(httpConfiguration);
                    }

                    @Override
                    public String getUrl() {
                        return HttpServerConfiguration.DEFAULT_PROCESSOR_URL;
                    }
                });

                httpServer.bind(new HttpRequestProcessorFactory() {
                    @Override
                    public HttpRequestProcessor newInstance() {
                        return textImportProcessor != null ? textImportProcessor.create(
                                httpConfiguration.getJsonQueryProcessorConfiguration(),
                                engine,
                                null,
                                workerPool.getWorkerCount()
                        ) : new TextImportProcessor(engine);
                    }

                    @Override
                    public String getUrl() {
                        return "/upload";
                    }
                });

                httpServer.bind(new HttpRequestProcessorFactory() {
                    @Override
                    public HttpRequestProcessor newInstance() {
                        return new JsonQueryProcessor(
                                httpConfiguration.getJsonQueryProcessorConfiguration(),
                                engine,
                                messageBus,
                                workerPool.getWorkerCount()
                        );
                    }

                    @Override
                    public String getUrl() {
                        return "/query";
                    }
                });

                httpServer.bind(new HttpRequestProcessorFactory() {
                    @Override
                    public HttpRequestProcessor newInstance() {
                        return new TextQueryProcessor(
                                httpConfiguration.getJsonQueryProcessorConfiguration(),
                                engine,
                                null,
                                workerPool.getWorkerCount()
                        );
                    }

                    @Override
                    public String getUrl() {
                        return "/exp";
                    }
                });


                httpServer.bind(new HttpRequestProcessorFactory() {
                    @Override
                    public HttpRequestProcessor newInstance() {
                        return new TableStatusCheckProcessor(engine, httpConfiguration.getJsonQueryProcessorConfiguration());
                    }

                    @Override
                    public String getUrl() {
                        return "/chk";
                    }
                });

                httpServer.bind(new HttpRequestProcessorFactory() {
                    @Override
                    public HttpRequestProcessor newInstance() {
                        return new JsonQueryProcessor(httpConfiguration.getJsonQueryProcessorConfiguration(), engine, engine.getMessageBus(), 1);
                    }

                    @Override
                    public String getUrl() {
                        return "/exec";
                    }
                });

                QueryCache.configure(httpConfiguration);

                workerPool.start(LOG);

                try {
                    code.run(engine);
                } finally {
                    workerPool.halt();

                    if (telemetryJob != null) {
                        Misc.free(telemetryJob);
                    }
                }
            }
        });
    }
}
