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
import io.questdb.client.Sender;
import io.questdb.client.SenderErrorHandler;
import io.questdb.client.cutlass.qwp.client.QwpWebSocketSender;
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

    /**
     * Build a plain WS sender against {@code localhost:port} via
     * {@link Sender#fromConfig(CharSequence)}. Tests should construct senders
     * through this helper (or {@code Sender.fromConfig} directly) so that
     * refactoring of the {@link QwpWebSocketSender}'s programmatic
     * constructors / {@code connect(...)} overloads cannot break tests.
     */
    protected static QwpWebSocketSender connectWs(int port) {
        return (QwpWebSocketSender) Sender.fromConfig(
                "ws::addr=localhost:" + port + ";close_flush_timeout_millis=60000;");
    }

    /**
     * Plain WS sender with a registered async error handler. Use when a test
     * needs to observe server-side rejections deterministically — the handler
     * fires on the dispatcher daemon thread for every {@code SenderError}
     * (including {@link io.questdb.client.SenderError.Policy#DROP_AND_CONTINUE},
     * which never throws from {@code flush()}).
     */
    protected static QwpWebSocketSender connectWs(int port, SenderErrorHandler errorHandler) {
        return (QwpWebSocketSender) Sender.builder(Sender.Transport.WEBSOCKET)
                .address("localhost:" + port)
                .errorHandler(errorHandler)
                .closeFlushTimeoutMillis(60_000L)
                .build();
    }

    /**
     * Plain WS sender combining the legacy auto-flush tunables of
     * {@link #connectWs(int, int, int, long, int)} with a registered async
     * error handler. Used by tests that need to assert on both batching
     * behaviour and async server rejections.
     * <p>
     * Sentinel translation matches the all-tunables overload: 0 or
     * {@link Integer#MAX_VALUE} for rows/bytes maps to "off"; an interval
     * whose milliseconds exceed {@link Integer#MAX_VALUE} also maps to "off".
     */
    protected static QwpWebSocketSender connectWs(
            int port,
            int autoFlushRows,
            int autoFlushBytes,
            long autoFlushIntervalNanos,
            int inFlightWindowSize,
            SenderErrorHandler errorHandler
    ) {
        int rows = (autoFlushRows <= 0 || autoFlushRows == Integer.MAX_VALUE) ? 0 : autoFlushRows;
        int bytes = (autoFlushBytes <= 0 || autoFlushBytes == Integer.MAX_VALUE) ? 0 : autoFlushBytes;
        long millis = autoFlushIntervalNanos / 1_000_000L;
        int intervalMillis = (autoFlushIntervalNanos <= 0 || millis > Integer.MAX_VALUE) ? 0 : (int) millis;
        return (QwpWebSocketSender) Sender.builder(Sender.Transport.WEBSOCKET)
                .address("localhost:" + port)
                .errorHandler(errorHandler)
                .inFlightWindowSize(inFlightWindowSize)
                .autoFlushRows(rows)
                .autoFlushBytes(bytes)
                .autoFlushIntervalMillis(intervalMillis)
                .closeFlushTimeoutMillis(60_000L)
                .build();
    }

    /** TLS variant of {@link #connectWs(int)}; trusts everything (test only). */
    protected static QwpWebSocketSender connectWss(int port) {
        return (QwpWebSocketSender) Sender.fromConfig(
                "wss::addr=localhost:" + port + ";tls_verify=unsafe_off;");
    }

    /**
     * Plain WS sender with the legacy auto-flush / window tunables that
     * existed on {@code QwpWebSocketSender.connect(...)}. Maps to connect-string
     * keys: {@code auto_flush_rows}, {@code auto_flush_bytes},
     * {@code auto_flush_interval} (millis), {@code in_flight_window}.
     * Sentinel translation: {@code Integer.MAX_VALUE} or {@code 0} for
     * rows/bytes maps to {@code off}; an interval whose milliseconds
     * exceed {@link Integer#MAX_VALUE} also maps to {@code off}.
     */
    protected static QwpWebSocketSender connectWs(
            int port,
            int autoFlushRows,
            int autoFlushBytes,
            long autoFlushIntervalNanos,
            int inFlightWindowSize
    ) {
        StringBuilder cfg = new StringBuilder("ws::addr=localhost:").append(port).append(';');
        appendAutoFlushRows(cfg, autoFlushRows);
        appendAutoFlushBytes(cfg, autoFlushBytes);
        appendAutoFlushInterval(cfg, autoFlushIntervalNanos);
        cfg.append("in_flight_window=").append(inFlightWindowSize).append(';');
        // Default close drain timeout (5s) is too tight for fuzz tests that
        // push hundreds of batches against a single-worker test server with
        // concurrent ALTERs slowing down WAL apply. 60s is enough headroom
        // without masking real problems.
        cfg.append("close_flush_timeout_millis=60000;");
        return (QwpWebSocketSender) Sender.fromConfig(cfg.toString());
    }

    private static void appendAutoFlushBytes(StringBuilder cfg, int autoFlushBytes) {
        if (autoFlushBytes <= 0 || autoFlushBytes == Integer.MAX_VALUE) {
            cfg.append("auto_flush_bytes=off;");
        } else {
            cfg.append("auto_flush_bytes=").append(autoFlushBytes).append(';');
        }
    }

    private static void appendAutoFlushInterval(StringBuilder cfg, long autoFlushIntervalNanos) {
        long millis = autoFlushIntervalNanos / 1_000_000L;
        if (autoFlushIntervalNanos <= 0 || millis > Integer.MAX_VALUE) {
            cfg.append("auto_flush_interval=off;");
        } else {
            cfg.append("auto_flush_interval=").append(millis).append(';');
        }
    }

    private static void appendAutoFlushRows(StringBuilder cfg, int autoFlushRows) {
        if (autoFlushRows <= 0 || autoFlushRows == Integer.MAX_VALUE) {
            cfg.append("auto_flush_rows=off;");
        } else {
            cfg.append("auto_flush_rows=").append(autoFlushRows).append(';');
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
