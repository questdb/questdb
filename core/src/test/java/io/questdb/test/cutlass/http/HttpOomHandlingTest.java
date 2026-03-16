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

import io.questdb.ServerMain;
import io.questdb.std.Os;
import io.questdb.std.Unsafe;
import io.questdb.test.AbstractBootstrapTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Before;
import org.junit.Test;

import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.Socket;
import java.net.URL;

import static io.questdb.PropertyKey.HTTP_CONNECTION_POOL_INITIAL_CAPACITY;
import static io.questdb.PropertyKey.HTTP_WORKER_COUNT;
import static org.junit.Assert.assertEquals;

public class HttpOomHandlingTest extends AbstractBootstrapTest {

    @Override
    @Before
    public void setUp() {
        super.setUp();
    }

    // Not wrapped in assertMemoryLeak() because OOM during HttpConnectionContext construction
    // inherently leaks native memory from partially initialized fields (e.g., DirectUtf8Sink in
    // HttpHeaderParser). This is a pre-existing resource management limitation, not a regression.
    @Test
    public void testOomDuringAcceptReturns503() throws Exception {
        TestUtils.unchecked(() -> createDummyConfiguration(
                HTTP_WORKER_COUNT + "=1",
                HTTP_CONNECTION_POOL_INITIAL_CAPACITY + "=1"
        ));

        try (ServerMain serverMain = new ServerMain(getServerMainArgs())) {
            serverMain.start();
            int port = serverMain.getHttpServerPort();

            // verify the server is functional
            HttpURLConnection conn = (HttpURLConnection) new URL("http://localhost:" + port + "/ping").openConnection();
            conn.setRequestMethod("GET");
            assertEquals(204, conn.getResponseCode());
            conn.disconnect();

            // open a socket to hold the only pooled context
            try (Socket holder = new Socket("localhost", port)) {
                // give the dispatcher time to accept and assign the context
                Os.sleep(500);

                // set a tight RSS limit so new context allocation fails
                long currentUsage = Unsafe.getRssMemUsed();
                Unsafe.setRssMemLimit(currentUsage + 1024);
                try {
                    // open a second connection - pool is empty, context creation triggers OOM
                    // the dispatcher should send a pre-allocated HTTP 503 response
                    try (Socket probe = new Socket("localhost", port)) {
                        probe.setSoTimeout(5_000);
                        InputStream is = probe.getInputStream();
                        byte[] buf = new byte[256];
                        int totalRead = 0;
                        int n;
                        // the server sends 503 and closes the fd, so we read until EOF
                        while ((n = is.read(buf, totalRead, buf.length - totalRead)) > 0) {
                            totalRead += n;
                        }
                        String response = new String(buf, 0, totalRead);
                        TestUtils.assertContains(response, "503 Service Unavailable");
                    }
                } finally {
                    Unsafe.setRssMemLimit(0);
                }
            }

            // give the server time to clean up
            Os.sleep(200);

            // verify the server recovered and still works
            conn = (HttpURLConnection) new URL("http://localhost:" + port + "/ping").openConnection();
            conn.setRequestMethod("GET");
            assertEquals(204, conn.getResponseCode());
            conn.disconnect();
        }
    }
}
