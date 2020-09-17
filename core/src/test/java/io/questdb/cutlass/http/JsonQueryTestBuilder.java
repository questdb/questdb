package io.questdb.cutlass.http;

import io.questdb.MessageBus;
import io.questdb.MessageBusImpl;
import io.questdb.TelemetryJob;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.DefaultCairoConfiguration;
import io.questdb.cutlass.http.processors.*;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.WorkerPool;
import io.questdb.mp.WorkerPoolConfiguration;
import io.questdb.network.Net;
import io.questdb.network.NetworkFacade;
import io.questdb.std.Misc;
import org.junit.rules.TemporaryFolder;

import java.util.Arrays;

import static io.questdb.test.tools.TestUtils.assertMemoryLeak;

public class JsonQueryTestBuilder {
    private static final Log LOG = LogFactory.getLog(JsonQueryTestBuilder.class);
    private boolean telemetry;
    private TemporaryFolder temp;
    private HttpServerConfigurationBuilder serverConfigBuilder;

    @FunctionalInterface
    public interface HttpClientCode {
        void run(CairoEngine engine) throws InterruptedException;
    }

    private int workerCount ;

    public JsonQueryTestBuilder withWorkerCount(int workerCount) {
        this.workerCount = workerCount;
        return this;
    }

    public JsonQueryTestBuilder withTelemetry(boolean telemetry) {
        this.telemetry = telemetry;
        return this;
    }

    public JsonQueryTestBuilder withTempFolder(TemporaryFolder temp) {
        this.temp = temp;
        return this;
    }

    public JsonQueryTestBuilder withHttpServerConfigBuilder(HttpServerConfigurationBuilder serverConfigBuilder) {
        this.serverConfigBuilder = serverConfigBuilder;
        return this;
    }

    public void runJson(HttpClientCode code) throws Exception {
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
