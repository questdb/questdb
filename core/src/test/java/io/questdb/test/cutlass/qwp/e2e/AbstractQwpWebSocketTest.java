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

package io.questdb.test.cutlass.qwp.e2e;

import io.questdb.cutlass.http.DefaultHttpContextConfiguration;
import io.questdb.cutlass.http.DefaultHttpServerConfiguration;
import io.questdb.cutlass.http.HttpFullFatServerConfiguration;
import io.questdb.cutlass.http.HttpRequestHandlerFactory;
import io.questdb.cutlass.http.HttpServer;
import io.questdb.cutlass.http.processors.LineHttpProcessorConfiguration;
import io.questdb.cutlass.qwp.server.QwpWebSocketHttpProcessor;
import io.questdb.griffin.SqlException;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.WorkerPoolUtils;
import io.questdb.network.PlainSocketFactory;
import io.questdb.std.ObjHashSet;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.mp.TestWorkerPool;
import io.questdb.test.tools.TestUtils;

public class AbstractQwpWebSocketTest extends AbstractCairoTest {

    private static final Log LOG = LogFactory.getLog(AbstractQwpWebSocketTest.class);

    protected void assertSql(String sql, String expected) {
        try {
            TestUtils.assertSql(engine, sqlExecutionContext, sql, sink, expected);
        } catch (SqlException e) {
            throw new AssertionError(e);
        }
    }

    protected void runInContext(QwpTestContext r) throws Exception {
        runInContext(r, 65_536);
    }

    protected void runInContext(QwpTestContext r, int recvBufferSize) throws Exception {
        runInContext(r, recvBufferSize, Integer.MAX_VALUE);
    }

    protected void runInContext(QwpTestContext r, int recvBufferSize, int forceRecvFragmentationChunkSize) throws Exception {
        runInContext(r, recvBufferSize, forceRecvFragmentationChunkSize, true);
    }

    protected void runInContextNoAutoCreate(QwpTestContext r) throws Exception {
        runInContext(r, 65_536, Integer.MAX_VALUE, false);
    }

    private void runInContext(QwpTestContext r, int recvBufferSize, int forceRecvFragmentationChunkSize, boolean autoCreateNewColumns) throws Exception {
        final HttpFullFatServerConfiguration httpConfig = new DefaultHttpServerConfiguration(
                configuration,
                new DefaultHttpContextConfiguration() {
                    @Override
                    public int getForceRecvFragmentationChunkSize() {
                        return forceRecvFragmentationChunkSize;
                    }
                }
        ) {
            @Override
            public int getBindPort() {
                return 0;
            }

            @Override
            public LineHttpProcessorConfiguration getLineHttpProcessorConfiguration() {
                if (autoCreateNewColumns) {
                    return super.getLineHttpProcessorConfiguration();
                }
                return new DefaultLineHttpProcessorConfiguration(configuration) {
                    @Override
                    public boolean autoCreateNewColumns() {
                        return false;
                    }
                };
            }

            @Override
            public int getRecvBufferSize() {
                return recvBufferSize;
            }
        };

        assertMemoryLeak(() -> {
            try (
                    TestWorkerPool workerPool = new TestWorkerPool(1);
                    HttpServer server = new HttpServer(httpConfig, workerPool, PlainSocketFactory.INSTANCE)
            ) {
                server.bind(new HttpRequestHandlerFactory() {
                    @Override
                    public ObjHashSet<String> getUrls() {
                        return httpConfig.getContextPathQWP();
                    }

                    @Override
                    public QwpWebSocketHttpProcessor newInstance() {
                        return new QwpWebSocketHttpProcessor(engine, httpConfig);
                    }
                });
                WorkerPoolUtils.setupWriterJobs(workerPool, engine);
                workerPool.start(LOG);
                try {
                    r.run(server.getPort());
                } catch (Throwable err) {
                    LOG.error().$("Stopping QWP worker pool because of an error").$(err).$();
                    throw err;
                } finally {
                    workerPool.halt();
                    Path.clearThreadLocals();
                }
            }
        });
    }

    @FunctionalInterface
    public interface QwpTestContext {
        void run(int port) throws Exception;
    }
}
