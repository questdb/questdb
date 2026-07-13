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

package io.questdb.test.cairo.lv;

import io.questdb.DefaultHttpClientConfiguration;
import io.questdb.PropertyKey;
import io.questdb.cutlass.http.client.Fragment;
import io.questdb.cutlass.http.client.HttpClient;
import io.questdb.cutlass.http.client.HttpClientFactory;
import io.questdb.cutlass.http.client.Response;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf8s;
import io.questdb.test.AbstractBootstrapTest;
import io.questdb.test.TestServerMain;
import io.questdb.test.tools.LogCapture;
import io.questdb.test.tools.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.net.Socket;
import java.nio.charset.StandardCharsets;

/**
 * A live view is stored WAL-backed derived state: it is {@code isWal()==true},
 * {@code isView()==false} and {@code isMatView()==false}, so the pre-existing
 * ILP ingestion guards that rejected only views and materialized views let it
 * through, handed the sender a {@code WalWriter} for the view's own WAL, and
 * interleaved foreign rows with the view's window output. These tests pin the
 * rejection on the ILP over HTTP and TCP paths; the QWP path is pinned by
 * {@code QwpIngressProcessorStateTest#testQwpCannotIngestIntoLiveView}.
 * <p>
 * The raw HTTP client and socket keep the tests independent of the native ILP
 * client binaries, which are not built for a core-only build.
 */
public class LiveViewIngestRejectTest extends AbstractBootstrapTest {

    private static final LogCapture capture = new LogCapture();

    @Before
    @Override
    public void setUp() {
        super.setUp();
        TestUtils.unchecked(() -> createDummyConfiguration());
        capture.start();
    }

    @After
    @Override
    public void tearDown() throws Exception {
        capture.stop();
        super.tearDown();
    }

    @Test
    public void testIlpCannotIngestIntoLiveView() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.DEV_MODE_ENABLED.getEnvVarName(), "true",
                    PropertyKey.HTTP_MIN_ENABLED.getEnvVarName(), "false",
                    PropertyKey.PG_ENABLED.getEnvVarName(), "false"
            )) {
                serverMain.execute("CREATE TABLE base (val INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY HOUR WAL");
                serverMain.execute("CREATE LIVE VIEW lv FLUSH EVERY 1s AS " +
                        "SELECT val, ts, row_number() OVER () AS rn FROM base");

                // ILP over HTTP surfaces the rejection in the response body.
                final StringSink sink = new StringSink();
                try (HttpClient httpClient = HttpClientFactory.newPlainTextInstance(new DefaultHttpClientConfiguration())) {
                    final HttpClient.Request request = httpClient.newRequest("localhost", serverMain.getHttpServerPort());
                    try (HttpClient.ResponseHeaders resp = request.POST()
                            .url("/write")
                            .withContent()
                            .putAscii("lv val=1i\n")
                            .send()) {
                        resp.await();
                        final Response response = resp.getResponse();
                        Fragment fragment;
                        while ((fragment = response.recv()) != null) {
                            Utf8s.utf8ToUtf16(fragment.lo(), fragment.hi(), sink);
                        }
                    }
                }
                TestUtils.assertContains(sink, "cannot modify live view: lv");

                // ILP over TCP disconnects the client on the error and logs it.
                try (Socket socket = new Socket("localhost", ILP_PORT)) {
                    socket.getOutputStream().write("lv val=1i\n".getBytes(StandardCharsets.UTF_8));
                    socket.getOutputStream().flush();
                    capture.waitFor("could not process line data 1 [table=lv, msg=cannot modify live view [view=lv], errno=-1]");
                }
            }
        });
    }
}
