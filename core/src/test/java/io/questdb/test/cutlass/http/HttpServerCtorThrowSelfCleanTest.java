/*+*****************************************************************************
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
import io.questdb.DefaultServerConfiguration;
import io.questdb.Metrics;
import io.questdb.cutlass.Services;
import io.questdb.cutlass.http.HttpFullFatServerConfiguration;
import io.questdb.cutlass.http.HttpRequestHandler;
import io.questdb.cutlass.http.HttpRequestHandlerFactory;
import io.questdb.cutlass.http.HttpRequestHeader;
import io.questdb.cutlass.http.HttpRequestProcessor;
import io.questdb.cutlass.http.HttpServer;
import io.questdb.cutlass.http.HttpServerConfiguration;
import io.questdb.cutlass.http.HttpServerConfigurationWrapper;
import io.questdb.mp.WorkerPool;
import io.questdb.mp.WorkerPoolMode;
import io.questdb.network.NetworkError;
import io.questdb.network.NetworkFacadeImpl;
import io.questdb.network.PlainSocketFactory;
import io.questdb.std.ObjHashSet;
import io.questdb.std.QuietCloseable;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.cairo.DefaultTestCairoConfiguration;
import io.questdb.test.mp.TestWorkerPool;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicInteger;

public class HttpServerCtorThrowSelfCleanTest extends AbstractCairoTest {

    @Test
    public void testBindFailureFreesPartialConstruction() throws Exception {
        assertMemoryLeak(() -> {
            HttpServerConfiguration configuration = new HttpServerConfigurationBuilder()
                    .withBaseDir(root)
                    .withFactoryProvider(DefaultFactoryProvider.INSTANCE)
                    .withFiberEnabled(true)
                    .withNetwork(new BindFailingNetworkFacade())
                    .build(new DefaultTestCairoConfiguration(root));
            for (WorkerPoolMode workerPoolMode : WorkerPoolMode.values()) {
                try (WorkerPool workerPool = new TestWorkerPool(1, workerPoolMode)) {
                    try {
                        new HttpServer(configuration, workerPool, PlainSocketFactory.INSTANCE);
                        Assert.fail();
                    } catch (NetworkError expected) {
                        Assert.assertTrue(expected.getMessage().contains("could not bind"));
                    }
                    if (workerPoolMode == WorkerPoolMode.FIBER_HOST) {
                        Assert.assertEquals(
                                0,
                                workerPool.getFiberRuntime().getConfigurationListenerCountForTesting()
                        );
                    }
                }
            }
        });
    }

    @Test
    public void testDefaultProcessorFailureClosesHandler() throws Exception {
        assertMemoryLeak(() -> {
            for (WorkerPoolMode workerPoolMode : WorkerPoolMode.values()) {
                final AtomicInteger closeCount = new AtomicInteger();
                final HttpServerConfiguration configuration = new HttpServerConfigurationBuilder()
                        .withBaseDir(root)
                        .withFiberEnabled(true)
                        .withPort(0)
                        .build(new DefaultTestCairoConfiguration(root));
                try (
                        WorkerPool workerPool = new TestWorkerPool(1, workerPoolMode);
                        HttpServer server = new HttpServer(configuration, workerPool, PlainSocketFactory.INSTANCE)
                ) {
                    try {
                        server.bind(new HttpRequestHandlerFactory() {
                            @Override
                            public ObjHashSet<String> getUrls() {
                                return urls(HttpFullFatServerConfiguration.DEFAULT_PROCESSOR_URL);
                            }

                            @Override
                            public HttpRequestHandler newInstance() {
                                return new CloseTrackingHandler(closeCount, true);
                            }
                        });
                        server.createSelectorForTesting();
                        Assert.fail();
                    } catch (RuntimeException e) {
                        Assert.assertEquals("default processor failure", e.getMessage());
                    }
                }
                Assert.assertEquals(1, closeCount.get());
            }
        });
    }

    @Test
    public void testEndpointFailureFreesServer() throws Exception {
        assertMemoryLeak(() -> {
            FailingEndpointServerConfiguration configuration = new FailingEndpointServerConfiguration(root);
            try (WorkerPool workerPool = new TestWorkerPool(1)) {
                try {
                    Services.INSTANCE.createHttpServer(configuration, engine, workerPool, 1);
                    Assert.fail();
                } catch (RuntimeException expected) {
                    Assert.assertEquals("endpoint failure", expected.getMessage());
                }
            }
        });
    }

    @Test
    public void testSelectorCreateFailureClosesPartialSelector() throws Exception {
        assertMemoryLeak(() -> {
            for (WorkerPoolMode workerPoolMode : WorkerPoolMode.values()) {
                final AtomicInteger closeCount = new AtomicInteger();
                final AtomicInteger failingFactoryCalls = new AtomicInteger();
                final HttpServerConfiguration configuration = new HttpServerConfigurationBuilder()
                        .withBaseDir(root)
                        .withFiberEnabled(true)
                        .withPort(0)
                        .build(new DefaultTestCairoConfiguration(root));
                try (
                        WorkerPool workerPool = new TestWorkerPool(1, workerPoolMode);
                        HttpServer server = new HttpServer(configuration, workerPool, PlainSocketFactory.INSTANCE)
                ) {
                    server.bind(new HttpRequestHandlerFactory() {
                        @Override
                        public ObjHashSet<String> getUrls() {
                            return urls("/close-tracking");
                        }

                        @Override
                        public HttpRequestHandler newInstance() {
                            return new CloseTrackingHandler(closeCount, false);
                        }
                    });
                    server.bind(new HttpRequestHandlerFactory() {
                        @Override
                        public ObjHashSet<String> getUrls() {
                            return urls("/failing");
                        }

                        @Override
                        public HttpRequestHandler newInstance() {
                            if (failingFactoryCalls.getAndIncrement() > 0) {
                                throw new RuntimeException("create failure");
                            }
                            return requestHeader -> null;
                        }
                    });

                    server.createSelectorForTesting();
                    final int closeCountBeforeFailure = closeCount.get();
                    try {
                        server.createSelectorForTesting();
                        Assert.fail();
                    } catch (RuntimeException e) {
                        Assert.assertEquals("create failure", e.getMessage());
                    }
                    Assert.assertEquals(closeCountBeforeFailure + 1, closeCount.get());
                }
                Assert.assertEquals(2, closeCount.get());
            }
        });
    }

    private static ObjHashSet<String> urls(String url) {
        final ObjHashSet<String> urls = new ObjHashSet<>();
        urls.add(url);
        return urls;
    }

    private static class BindFailingNetworkFacade extends NetworkFacadeImpl {
        @Override
        public boolean bindTcp(long fd, int address, int port) {
            return false;
        }

        @Override
        public int errno() {
            return -1;
        }
    }

    private static class CloseTrackingHandler implements HttpRequestHandler, QuietCloseable {
        private final AtomicInteger closeCount;
        private final boolean isDefaultProcessorFailure;

        private CloseTrackingHandler(AtomicInteger closeCount, boolean isDefaultProcessorFailure) {
            this.closeCount = closeCount;
            this.isDefaultProcessorFailure = isDefaultProcessorFailure;
        }

        @Override
        public void close() {
            closeCount.incrementAndGet();
        }

        @Override
        public HttpRequestProcessor getDefaultProcessor() {
            if (isDefaultProcessorFailure) {
                throw new RuntimeException("default processor failure");
            }
            return null;
        }

        @Override
        public HttpRequestProcessor getProcessor(HttpRequestHeader requestHeader) {
            return null;
        }
    }

    private static class FailingEndpointServerConfiguration extends DefaultServerConfiguration {
        private final HttpFullFatServerConfiguration httpServerConfiguration;

        private FailingEndpointServerConfiguration(CharSequence root) {
            super(root);
            HttpServerConfigurationWrapper wrapper = new HttpServerConfigurationWrapper(Metrics.ENABLED) {
                @Override
                public int getBindPort() {
                    return 0;
                }

                @Override
                public ObjHashSet<String> getContextPathSettings() {
                    throw new RuntimeException("endpoint failure");
                }
            };
            wrapper.setDelegate(super.getHttpServerConfiguration());
            httpServerConfiguration = wrapper;
        }

        @Override
        public HttpFullFatServerConfiguration getHttpServerConfiguration() {
            return httpServerConfiguration;
        }
    }
}
