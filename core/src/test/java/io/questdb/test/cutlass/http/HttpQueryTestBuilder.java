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
import io.questdb.TelemetryJob;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.SqlJitMode;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.SqlExecutionCircuitBreakerConfiguration;
import io.questdb.cutlass.http.*;
import io.questdb.cutlass.http.processors.*;
import io.questdb.griffin.*;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.WorkerPool;
import io.questdb.std.FilesFacade;
import io.questdb.std.Misc;
import io.questdb.std.datetime.microtime.MicrosecondClock;
import io.questdb.test.cairo.DefaultTestCairoConfiguration;
import io.questdb.test.mp.TestWorkerPool;
import io.questdb.test.std.TestFilesFacadeImpl;

import java.util.concurrent.BrokenBarrierException;

import static io.questdb.test.tools.TestUtils.assertMemoryLeak;

public class HttpQueryTestBuilder {

    private static final Log LOG = LogFactory.getLog(HttpQueryTestBuilder.class);
    private String copyInputRoot;
    private FilesFacade filesFacade = new TestFilesFacadeImpl();
    private int jitMode = SqlJitMode.JIT_MODE_ENABLED;
    private long maxWriterWaitTimeout = 30_000L;
    private Metrics metrics;
    private MicrosecondClock microsecondClock;
    private QueryFutureUpdateListener queryFutureUpdateListener;
    private long queryTimeout = -1;
    private HttpServerConfigurationBuilder serverConfigBuilder;
    private SqlExecutionContextImpl sqlExecutionContext;
    private long startWriterWaitTimeout = 500;
    private boolean telemetry;
    private String temp;
    private HttpRequestProcessorBuilder textImportProcessor;
    private int workerCount = 1;

    public SqlExecutionContextImpl getSqlExecutionContext() {
        return sqlExecutionContext;
    }

    public int getWorkerCount() {
        return this.workerCount;
    }

    public void run(HttpClientCode code) throws Exception {
        run(null, code);
    }

    public void run(CairoConfiguration configuration, HttpClientCode code) throws Exception {
        assertMemoryLeak(() -> {
            final String baseDir = temp;
            final DefaultHttpServerConfiguration httpConfiguration = serverConfigBuilder
                    .withBaseDir(baseDir)
                    .build();
            if (metrics == null) {
                metrics = Metrics.enabled();
            }

            final WorkerPool workerPool = new TestWorkerPool(workerCount, metrics);

            CairoConfiguration cairoConfiguration = configuration;
            if (cairoConfiguration == null) {
                cairoConfiguration = new DefaultTestCairoConfiguration(baseDir) {
                    @Override
                    public SqlExecutionCircuitBreakerConfiguration getCircuitBreakerConfiguration() {
                        return new DefaultSqlExecutionCircuitBreakerConfiguration() {
                            @Override
                            public long getTimeout() {
                                return queryTimeout > 0 || queryTimeout == SqlExecutionCircuitBreaker.TIMEOUT_FAIL_ON_FIRST_CHECK
                                        ? queryTimeout
                                        : super.getTimeout();
                            }
                        };
                    }

                    public FilesFacade getFilesFacade() {
                        return filesFacade;
                    }

                    @Override
                    public MicrosecondClock getMicrosecondClock() {
                        return microsecondClock != null ? microsecondClock : super.getMicrosecondClock();
                    }

                    @Override
                    public boolean getSimulateCrashEnabled() {
                        return true;
                    }

                    @Override
                    public CharSequence getSqlCopyInputRoot() {
                        return copyInputRoot != null ? copyInputRoot : super.getSqlCopyInputRoot();
                    }

                    @Override
                    public int getSqlJitMode() {
                        return jitMode;
                    }

                    @Override
                    public long getWriterAsyncCommandBusyWaitTimeout() {
                        return startWriterWaitTimeout;
                    }

                    @Override
                    public long getWriterAsyncCommandMaxTimeout() {
                        return maxWriterWaitTimeout;
                    }

                    @Override
                    public boolean mangleTableDirNames() {
                        return false;
                    }
                };
            }
            try (
                    CairoEngine engine = new CairoEngine(cairoConfiguration, metrics);
                    HttpServer httpServer = new HttpServer(httpConfiguration, engine.getMessageBus(), metrics, workerPool)
            ) {
                TelemetryJob telemetryJob = null;
                if (telemetry) {
                    telemetryJob = new TelemetryJob(engine);
                }
                httpServer.bind(new HttpRequestProcessorFactory() {
                    @Override
                    public String getUrl() {
                        return HttpServerConfiguration.DEFAULT_PROCESSOR_URL;
                    }

                    @Override
                    public HttpRequestProcessor newInstance() {
                        return new StaticContentProcessor(httpConfiguration);
                    }
                });

                httpServer.bind(new HttpRequestProcessorFactory() {
                    @Override
                    public String getUrl() {
                        return "/upload";
                    }

                    @Override
                    public HttpRequestProcessor newInstance() {
                        return textImportProcessor != null ? textImportProcessor.create(
                                httpConfiguration.getJsonQueryProcessorConfiguration(),
                                engine,
                                workerPool.getWorkerCount()
                        ) : new TextImportProcessor(engine);
                    }
                });

                this.sqlExecutionContext = new SqlExecutionContextImpl(engine, workerCount) {
                    @Override
                    public QueryFutureUpdateListener getQueryFutureUpdateListener() {
                        return queryFutureUpdateListener != null ? queryFutureUpdateListener : QueryFutureUpdateListener.EMPTY;
                    }
                };

                httpServer.bind(new HttpRequestProcessorFactory() {
                    @Override
                    public String getUrl() {
                        return "/query";
                    }

                    @Override
                    public HttpRequestProcessor newInstance() {
                        return new JsonQueryProcessor(
                                httpConfiguration.getJsonQueryProcessorConfiguration(),
                                engine,
                                new SqlCompiler(engine),
                                sqlExecutionContext
                        );
                    }
                });

                httpServer.bind(new HttpRequestProcessorFactory() {
                    @Override
                    public String getUrl() {
                        return "/exp";
                    }

                    @Override
                    public HttpRequestProcessor newInstance() {
                        return new TextQueryProcessor(
                                httpConfiguration.getJsonQueryProcessorConfiguration(),
                                engine,
                                workerPool.getWorkerCount()
                        );
                    }
                });

                httpServer.bind(new HttpRequestProcessorFactory() {
                    @Override
                    public String getUrl() {
                        return "/chk";
                    }

                    @Override
                    public HttpRequestProcessor newInstance() {
                        return new TableStatusCheckProcessor(engine, httpConfiguration.getJsonQueryProcessorConfiguration());
                    }
                });

                httpServer.bind(new HttpRequestProcessorFactory() {
                    @Override
                    public String getUrl() {
                        return "/exec";
                    }

                    @Override
                    public HttpRequestProcessor newInstance() {
                        return new JsonQueryProcessor(httpConfiguration.getJsonQueryProcessorConfiguration(), engine, 1);
                    }
                });

                QueryCache.configure(httpConfiguration, metrics);

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

    public HttpQueryTestBuilder withAlterTableMaxWaitTimeout(long maxWriterWaitTimeout) {
        this.maxWriterWaitTimeout = maxWriterWaitTimeout;
        return this;
    }

    public HttpQueryTestBuilder withAlterTableStartWaitTimeout(long startWriterWaitTimeout) {
        this.startWriterWaitTimeout = startWriterWaitTimeout;
        return this;
    }

    public HttpQueryTestBuilder withCopyInputRoot(String copyInputRoot) {
        this.copyInputRoot = copyInputRoot;
        return this;
    }

    public HttpQueryTestBuilder withCustomTextImportProcessor(HttpRequestProcessorBuilder textQueryProcessor) {
        this.textImportProcessor = textQueryProcessor;
        return this;
    }

    public HttpQueryTestBuilder withFilesFacade(FilesFacade ff) {
        this.filesFacade = ff;
        return this;
    }

    public HttpQueryTestBuilder withHttpServerConfigBuilder(HttpServerConfigurationBuilder serverConfigBuilder) {
        this.serverConfigBuilder = serverConfigBuilder;
        return this;
    }

    public HttpQueryTestBuilder withJitMode(int jitMode) {
        this.jitMode = jitMode;
        return this;
    }

    public HttpQueryTestBuilder withMetrics(Metrics metrics) {
        this.metrics = metrics;
        return this;
    }

    public HttpQueryTestBuilder withMicrosecondClock(MicrosecondClock clock) {
        this.microsecondClock = clock;
        return this;
    }

    public HttpQueryTestBuilder withQueryFutureUpdateListener(QueryFutureUpdateListener queryFutureUpdateListener) {
        this.queryFutureUpdateListener = queryFutureUpdateListener;
        return this;
    }

    public HttpQueryTestBuilder withQueryTimeout(long queryTimeout) {
        this.queryTimeout = queryTimeout;
        return this;
    }

    public HttpQueryTestBuilder withTelemetry(boolean telemetry) {
        this.telemetry = telemetry;
        return this;
    }

    public HttpQueryTestBuilder withTempFolder(String temp) {
        this.temp = temp;
        return this;
    }

    public HttpQueryTestBuilder withWorkerCount(int workerCount) {
        this.workerCount = workerCount;
        return this;
    }

    @FunctionalInterface
    public interface HttpClientCode {
        void run(CairoEngine engine) throws InterruptedException, SqlException, BrokenBarrierException;
    }

    @FunctionalInterface
    public interface HttpRequestProcessorBuilder {
        HttpRequestProcessor create(
                JsonQueryProcessorConfiguration configuration,
                CairoEngine engine,
                int workerCount
        );
    }
}
