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

import io.questdb.DefaultFactoryProvider;
import io.questdb.FactoryProvider;
import io.questdb.TelemetryJob;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.SecurityContext;
import io.questdb.cairo.SqlJitMode;
import io.questdb.cairo.security.AllowAllSecurityContext;
import io.questdb.cairo.security.SecurityContextFactory;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.SqlExecutionCircuitBreakerConfiguration;
import io.questdb.cutlass.http.DefaultHttpServerConfiguration;
import io.questdb.cutlass.http.HttpRequestHandler;
import io.questdb.cutlass.http.HttpRequestHandlerFactory;
import io.questdb.cutlass.http.HttpServer;
import io.questdb.cutlass.http.processors.ExportQueryProcessor;
import io.questdb.cutlass.http.processors.HealthCheckProcessor;
import io.questdb.cutlass.http.processors.JsonQueryProcessor;
import io.questdb.cutlass.http.processors.SqlValidationProcessor;
import io.questdb.cutlass.http.processors.StaticContentProcessorFactory;
import io.questdb.cutlass.http.processors.TableStatusCheckProcessor;
import io.questdb.cutlass.http.processors.TextImportProcessor;
import io.questdb.cutlass.parquet.CopyExportRequestJob;
import io.questdb.cutlass.text.CopyImportRequestJob;
import io.questdb.griffin.DefaultSqlExecutionCircuitBreakerConfiguration;
import io.questdb.griffin.QueryFutureUpdateListener;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.WorkerPool;
import io.questdb.network.PlainSocketFactory;
import io.questdb.std.Chars;
import io.questdb.std.FilesFacade;
import io.questdb.std.Misc;
import io.questdb.std.ObjHashSet;
import io.questdb.std.datetime.MicrosecondClock;
import io.questdb.std.datetime.NanosecondClock;
import io.questdb.std.datetime.nanotime.NanosecondClockImpl;
import io.questdb.test.cairo.DefaultTestCairoConfiguration;
import io.questdb.test.mp.TestWorkerPool;
import io.questdb.test.std.TestFilesFacadeImpl;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;

import static io.questdb.test.tools.TestUtils.assertMemoryLeak;

public class HttpQueryTestBuilder {

    private static final Log LOG = LogFactory.getLog(HttpQueryTestBuilder.class);
    private String copyExportRoot;
    private String copyInputRoot;
    private FactoryProvider factoryProvider;
    private FilesFacade filesFacade = new TestFilesFacadeImpl();
    private int forceRecvFragmentationChunkSize = Integer.MAX_VALUE;
    private int forceSendFragmentationChunkSize = Integer.MAX_VALUE;
    private byte httpHealthCheckAuthType = SecurityContext.AUTH_TYPE_NONE;
    private byte httpStaticContentAuthType = SecurityContext.AUTH_TYPE_NONE;
    private int jitMode = SqlJitMode.JIT_MODE_ENABLED;
    private long maxWriterWaitTimeout = 30_000L;
    private MicrosecondClock microsecondClock;
    private NanosecondClock nanosecondClock = NanosecondClockImpl.INSTANCE;
    private QueryFutureUpdateListener queryFutureUpdateListener;
    private long queryTimeout = -1;
    private SecurityContext securityContext = null;
    private int sendBufferSize = -1;
    private HttpServerConfigurationBuilder serverConfigBuilder;
    private long startWriterWaitTimeout = 500;
    private boolean telemetry;
    private String temp;
    private int workerCount = 1;

    public int getWorkerCount() {
        return this.workerCount;
    }

    public void run(HttpClientCode code) throws Exception {
        run(null, code);
    }

    public void run(CairoConfiguration configuration, HttpClientCode code) throws Exception {
        assertMemoryLeak(() -> runNoLeakCheck(configuration, code));
    }

    public void runNoLeakCheck(CairoConfiguration configuration, HttpClientCode code) throws Exception {
        final String baseDir = temp;
        serverConfigBuilder
                .withBaseDir(baseDir)
                .withFactoryProvider(factoryProvider)
                .withStaticContentAuthRequired(httpStaticContentAuthType)
                .withHealthCheckAuthRequired(httpHealthCheckAuthType)
                .withNanosClock(nanosecondClock)
                .withForceSendFragmentationChunkSize(forceSendFragmentationChunkSize)
                .withForceRecvFragmentationChunkSize(forceRecvFragmentationChunkSize)
                .withQueryFutureUpdateListener(queryFutureUpdateListener);
        if (sendBufferSize != -1) {
            serverConfigBuilder.withSendBufferSize(sendBufferSize);
        }

        if (securityContext != null) {
            SecurityContextFactory securityContextFactory = (principalContext, interfaceId) -> securityContext;

            serverConfigBuilder.withFactoryProvider(new DefaultFactoryProvider() {
                @Override
                public @NotNull SecurityContextFactory getSecurityContextFactory() {
                    return securityContextFactory;
                }
            });
        }
        final DefaultHttpServerConfiguration httpConfiguration = serverConfigBuilder.build(configuration);
        final WorkerPool workerPool = new TestWorkerPool(workerCount, httpConfiguration.getMetrics());

        CairoConfiguration cairoConfiguration = configuration;
        if (cairoConfiguration == null) {
            cairoConfiguration = new DefaultTestCairoConfiguration(baseDir) {
                private final AtomicLong copyIdGenerator = new AtomicLong(0);

                @Override
                public @NotNull SqlExecutionCircuitBreakerConfiguration getCircuitBreakerConfiguration() {
                    return new DefaultSqlExecutionCircuitBreakerConfiguration() {
                        @Override
                        public long getQueryTimeout() {
                            return queryTimeout > 0 || queryTimeout == SqlExecutionCircuitBreaker.TIMEOUT_FAIL_ON_FIRST_CHECK
                                    ? queryTimeout
                                    : super.getQueryTimeout();
                        }
                    };
                }

                @Override
                public @NotNull LongSupplier getCopyIDSupplier() {
                    return copyIdGenerator::getAndIncrement;
                }

                public @NotNull FilesFacade getFilesFacade() {
                    return filesFacade;
                }

                @Override
                public @NotNull MicrosecondClock getMicrosecondClock() {
                    return microsecondClock != null ? microsecondClock : super.getMicrosecondClock();
                }

                @Override
                public @Nullable CharSequence getSqlCopyExportRoot() {
                    return copyExportRoot != null ? copyExportRoot : super.getSqlCopyExportRoot();
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
                CairoEngine engine = new CairoEngine(cairoConfiguration);
                HttpServer httpServer = new HttpServer(httpConfiguration, workerPool, PlainSocketFactory.INSTANCE);
                SqlExecutionContext sqlExecutionContext = new SqlExecutionContextImpl(engine, 1).with(AllowAllSecurityContext.INSTANCE)
        ) {
            TelemetryJob telemetryJob = null;
            if (telemetry) {
                telemetryJob = new TelemetryJob(engine);
            }

            if (!Chars.isBlank(cairoConfiguration.getSqlCopyInputRoot())) {
                CopyImportRequestJob copyImportRequestJob = new CopyImportRequestJob(engine, workerCount);
                workerPool.assign(copyImportRequestJob);
                workerPool.freeOnExit(copyImportRequestJob);
            }

            if (!Chars.isBlank(cairoConfiguration.getSqlCopyExportRoot())) {
                final CopyExportRequestJob copyExportRequestJob = new CopyExportRequestJob(engine);
                workerPool.assign(copyExportRequestJob);
                workerPool.freeOnExit(copyExportRequestJob);
            }

            httpServer.bind(new StaticContentProcessorFactory(engine, httpConfiguration));

            httpServer.bind(new HttpRequestHandlerFactory() {
                @Override
                public ObjHashSet<String> getUrls() {
                    return new ObjHashSet<>() {{
                        add("/upload");
                    }};
                }

                @Override
                public HttpRequestHandler newInstance() {
                    return new TextImportProcessor(engine, httpConfiguration.getJsonQueryProcessorConfiguration());
                }
            });

            httpServer.bind(new HttpRequestHandlerFactory() {
                @Override
                public ObjHashSet<String> getUrls() {
                    return new ObjHashSet<>() {{
                        add("/query");
                    }};
                }

                @Override
                public HttpRequestHandler newInstance() {
                    return new JsonQueryProcessor(
                            httpConfiguration.getJsonQueryProcessorConfiguration(),
                            engine,
                            workerCount
                    );
                }
            });

            httpServer.bind(new HttpRequestHandlerFactory() {
                @Override
                public ObjHashSet<String> getUrls() {
                    return httpConfiguration.getContextPathSqlValidation();
                }

                @Override
                public HttpRequestHandler newInstance() {
                    return new SqlValidationProcessor(httpConfiguration.getJsonQueryProcessorConfiguration(), engine, workerCount) {
                    };
                }
            });

            httpServer.bind(new HttpRequestHandlerFactory() {
                @Override
                public ObjHashSet<String> getUrls() {
                    return httpConfiguration.getContextPathExport();
                }

                @Override
                public HttpRequestHandler newInstance() {
                    return new ExportQueryProcessor(
                            httpConfiguration.getJsonQueryProcessorConfiguration(),
                            engine,
                            workerPool.getWorkerCount()
                    );
                }
            });

            httpServer.bind(new HttpRequestHandlerFactory() {
                @Override
                public ObjHashSet<String> getUrls() {
                    return httpConfiguration.getContextPathTableStatus();
                }

                @Override
                public HttpRequestHandler newInstance() {
                    return new TableStatusCheckProcessor(engine, httpConfiguration.getJsonQueryProcessorConfiguration());
                }
            });

            httpServer.bind(new HttpRequestHandlerFactory() {
                @Override
                public ObjHashSet<String> getUrls() {
                    return httpConfiguration.getContextPathExec();
                }

                @Override
                public HttpRequestHandler newInstance() {
                    return new JsonQueryProcessor(httpConfiguration.getJsonQueryProcessorConfiguration(), engine, 1);
                }
            });

            httpServer.bind(new HttpRequestHandlerFactory() {
                @Override
                public ObjHashSet<String> getUrls() {
                    return new ObjHashSet<>() {{
                        add("/status");
                    }};
                }

                @Override
                public HttpRequestHandler newInstance() {
                    return new HealthCheckProcessor(httpConfiguration);
                }
            });

            workerPool.start(LOG);

            try {
                code.run(engine, sqlExecutionContext);
            } finally {
                workerPool.halt();

                if (telemetryJob != null) {
                    Misc.free(telemetryJob);
                }
            }
        }
    }

    public HttpQueryTestBuilder withAlterTableMaxWaitTimeout(long maxWriterWaitTimeout) {
        this.maxWriterWaitTimeout = maxWriterWaitTimeout;
        return this;
    }

    public HttpQueryTestBuilder withAlterTableStartWaitTimeout(long startWriterWaitTimeout) {
        this.startWriterWaitTimeout = startWriterWaitTimeout;
        return this;
    }

    public HttpQueryTestBuilder withCopyExportRoot(String copyExportRoot) {
        this.copyExportRoot = copyExportRoot;
        return this;
    }

    public HttpQueryTestBuilder withCopyInputRoot(String copyInputRoot) {
        this.copyInputRoot = copyInputRoot;
        return this;
    }

    public HttpQueryTestBuilder withFactoryProvider(FactoryProvider factoryProvider) {
        this.factoryProvider = factoryProvider;
        return this;
    }

    public HttpQueryTestBuilder withFilesFacade(FilesFacade ff) {
        this.filesFacade = ff;
        return this;
    }

    public HttpQueryTestBuilder withForceRecvFragmentationChunkSize(int forceRecvFragmentationChunkSize) {
        this.forceRecvFragmentationChunkSize = forceRecvFragmentationChunkSize;
        return this;
    }

    public HttpQueryTestBuilder withForceSendFragmentationChunkSize(int forceSendFragmentationChunkSize) {
        this.forceSendFragmentationChunkSize = forceSendFragmentationChunkSize;
        return this;
    }

    public HttpQueryTestBuilder withHealthCheckAuthRequired(byte httpHealthCheckAuthType) {
        this.httpHealthCheckAuthType = httpHealthCheckAuthType;
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

    public HttpQueryTestBuilder withMicrosecondClock(MicrosecondClock clock) {
        this.microsecondClock = clock;
        return this;
    }

    public HttpQueryTestBuilder withNanosClock(NanosecondClock nanosecondClock) {
        this.nanosecondClock = nanosecondClock;
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

    public HttpQueryTestBuilder withSecurityContext(SecurityContext securityContext) {
        this.securityContext = securityContext;
        return this;
    }

    public HttpQueryTestBuilder withSendBufferSize(int sendBufferSize) {
        this.sendBufferSize = sendBufferSize;
        return this;
    }

    public HttpQueryTestBuilder withStaticContentAuthRequired(byte httpStaticContentAuthType) {
        this.httpStaticContentAuthType = httpStaticContentAuthType;
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
        void run(CairoEngine engine, SqlExecutionContext sqlExecutionContext) throws Exception;
    }
}
