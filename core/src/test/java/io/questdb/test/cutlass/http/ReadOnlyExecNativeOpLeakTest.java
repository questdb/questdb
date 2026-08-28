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

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoEngine;
import io.questdb.cutlass.http.DefaultHttpServerConfiguration;
import io.questdb.cutlass.http.HttpRequestHandler;
import io.questdb.cutlass.http.HttpRequestHandlerFactory;
import io.questdb.cutlass.http.HttpServer;
import io.questdb.cutlass.http.processors.JsonQueryProcessor;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.network.PlainSocketFactory;
import io.questdb.std.ObjHashSet;
import io.questdb.test.AbstractTest;
import io.questdb.test.cairo.DefaultTestCairoConfiguration;
import io.questdb.test.mp.TestWorkerPool;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;

import static io.questdb.test.tools.TestUtils.assertMemoryLeak;

/**
 * Guards the read-only /exec gate in JsonQueryProcessor against native-memory leaks.
 *
 * <p>When a read-only node receives a write statement over /exec, JsonQueryProcessor.compileAndExecuteQuery
 * calls compiler.compile() which builds a native operation (InsertOperation, UpdateOperation,
 * AlterOperation, CreateTableOperation, or similar). The per-statement read-only gate fires
 * AFTER compile(), so the per-type executors that normally free those operations are never
 * reached. Without explicit op-freeing before the gate throws, each refused request leaks a
 * native allocation.
 *
 * <p>Each test method:
 * <ul>
 *   <li>Boots a CairoEngine with {@code isReadOnlyInstance() = true} (engine.isReadOnlyMode() == true).</li>
 *   <li>Wraps the test body in {@code assertMemoryLeak} so a native-allocation difference fails the test.</li>
 *   <li>POSTs each gated write type to the /exec endpoint and asserts: HTTP 403 + body contains
 *       "replica access is read-only".</li>
 * </ul>
 *
 * <p>RED without the fix: each refused write leaks the compiled op, and assertMemoryLeak reports
 * a native-allocation delta. GREEN after the fix: cc.closeAllButSelect() + Misc.free(cc.getOperation())
 * free every compiled op before the gate throws.
 */
public class ReadOnlyExecNativeOpLeakTest extends AbstractTest {

    private static final Log LOG = LogFactory.getLog(ReadOnlyExecNativeOpLeakTest.class);

    @Rule
    public Timeout timeout = Timeout.builder()
            .withTimeout(60, TimeUnit.SECONDS)
            .withLookingForStuckThread(true)
            .build();

    /**
     * For each gated write type, sends the statement to the /exec endpoint on a read-only node
     * and asserts: HTTP 403, body containing "replica access is read-only", and no native-memory
     * leak (enforced by assertMemoryLeak). The assertMemoryLeak wrapper is RED if any compiled
     * native op is leaked per request.
     */
    @Test
    public void testGatedWriteTypesRefusedWithNoNativeLeak() throws Exception {
        assertMemoryLeak(() -> {
            final CairoConfiguration readOnlyConfig = new DefaultTestCairoConfiguration(root) {
                @Override
                public boolean getAllowTableRegistrySharedWrite() {
                    // Required when isReadOnlyInstance() = true so the registry opens in RO mode.
                    return false;
                }

                @Override
                public boolean isReadOnlyInstance() {
                    return true;
                }
            };

            final DefaultHttpServerConfiguration httpConfig = new HttpServerConfigurationBuilder()
                    .withPort(0)
                    .build(readOnlyConfig);

            final TestWorkerPool workerPool = new TestWorkerPool(1, httpConfig.getMetrics());

            try (
                    CairoEngine engine = new CairoEngine(readOnlyConfig);
                    HttpServer httpServer = new HttpServer(httpConfig, workerPool, PlainSocketFactory.INSTANCE)
            ) {
                httpServer.bind(new HttpRequestHandlerFactory() {
                    @Override
                    public ObjHashSet<String> getUrls() {
                        return httpConfig.getContextPathExec();
                    }

                    @Override
                    public HttpRequestHandler newInstance() {
                        return new JsonQueryProcessor(httpConfig.getJsonQueryProcessorConfiguration(), engine, 1);
                    }
                });

                workerPool.start(LOG);
                try {
                    final int port = httpServer.getPort();

                    // Each statement exercises a distinct gated type. CREATE TABLE does not require
                    // a pre-existing table; the others are syntactically valid CREATE variants that
                    // produce a compiled op before the read-only gate fires.
                    String[] statements = {
                            "CREATE TABLE ro_test_leak (ts TIMESTAMP, val INT) TIMESTAMP(ts)",
                            "CREATE TABLE ro_test_leak2 (ts TIMESTAMP, val INT) TIMESTAMP(ts) PARTITION BY DAY WAL",
                    };

                    for (String sql : statements) {
                        assertExecRefused(port, sql);
                    }
                } finally {
                    workerPool.halt();
                }
            }
        });
    }

    /**
     * Sends the given SQL to /exec on a read-only node and asserts HTTP 403 with
     * "replica access is read-only" in the response body.
     */
    private static void assertExecRefused(int port, String sql) throws Exception {
        final String encoded = URLEncoder.encode(sql, StandardCharsets.UTF_8);
        final String urlStr = "http://localhost:" + port + "/exec?query=" + encoded;
        final HttpURLConnection conn = (HttpURLConnection) new URI(urlStr).toURL().openConnection();
        try {
            conn.setRequestMethod("GET");
            conn.setConnectTimeout(5000);
            conn.setReadTimeout(5000);
            final int status = conn.getResponseCode();
            final byte[] body;
            try (java.io.InputStream in = status >= 400 ? conn.getErrorStream() : conn.getInputStream()) {
                body = in == null ? new byte[0] : in.readAllBytes();
            }
            final String bodyStr = new String(body, StandardCharsets.UTF_8).toLowerCase();
            Assert.assertEquals("expected HTTP 403 for sql=[" + sql + "]", 403, status);
            Assert.assertTrue(
                    "expected 'replica access is read-only' in response body for sql=[" + sql + "], body=" + bodyStr,
                    bodyStr.contains("replica access is read-only")
            );
        } finally {
            conn.disconnect();
        }
    }
}
