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
import io.questdb.std.Unsafe;
import io.questdb.test.AbstractBootstrapTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.Socket;
import java.net.URL;

import static io.questdb.PropertyKey.HTTP_CONNECTION_POOL_INITIAL_CAPACITY;
import static io.questdb.PropertyKey.HTTP_WORKER_COUNT;
import static org.junit.Assert.assertEquals;

public class HttpOomHandlingTest extends AbstractBootstrapTest {

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
            assertPingSucceeds(port);

            // open a socket to hold the only pooled context
            try (Socket holder = new Socket("localhost", port)) {
                // drive a request to completion on the holder rather than sleeping: a served
                // response proves the dispatcher accepted the connection and handed it the one
                // pooled context, which is the precondition the probe below depends on. The
                // keep-alive connection then holds that context until the socket closes.
                holder.setSoTimeout(30_000);
                holder.getOutputStream().write("GET /ping HTTP/1.1\r\nHost: localhost\r\n\r\n".getBytes());
                holder.getOutputStream().flush();
                TestUtils.assertContains(readUntil(holder.getInputStream(), "\r\n\r\n"), "204");

                // set an RSS limit just above current usage so new context allocation fails.
                // 16 KiB of slack absorbs incidental allocations (JIT, logging, GC bookkeeping)
                // between the rss read and the probe connect; the per-context allocation is
                // far larger than the slack, so the OOM path still fires.
                long currentUsage = Unsafe.getRssMemUsed();
                Unsafe.setRssMemLimit(currentUsage + 16_384);
                try {
                    // open a second connection - pool is empty, context creation triggers OOM
                    // the dispatcher should send a pre-allocated HTTP 503 response
                    try (Socket probe = new Socket("localhost", port)) {
                        probe.setSoTimeout(30_000);
                        TestUtils.assertContains(readUntil(probe.getInputStream(), null), "503 Service Unavailable");
                    }
                } finally {
                    Unsafe.setRssMemLimit(0);
                }
            }

            // verify the server recovered and still works. The holder's context returns to the pool
            // asynchronously, so retry rather than sleep on a fixed guess.
            TestUtils.assertEventually(() -> assertPingSucceeds(port));
        }
    }

    /**
     * Reports a failed ping as an {@link AssertionError}, including the transport failures, so that
     * {@link TestUtils#assertEventually} retries them - it only retries assertion failures, and a
     * server that has not finished reclaiming the previous connection refuses rather than 500s.
     */
    private static void assertPingSucceeds(int port) {
        try {
            HttpURLConnection conn = (HttpURLConnection) new URL("http://localhost:" + port + "/ping").openConnection();
            try {
                conn.setRequestMethod("GET");
                assertEquals(204, conn.getResponseCode());
            } finally {
                conn.disconnect();
            }
        } catch (IOException e) {
            throw new AssertionError("ping failed: " + e, e);
        }
    }

    /**
     * Reads until {@code terminator} appears, or to EOF when it is null. Grows the sink instead of
     * reading into a fixed buffer: a bounded read stops silently once the buffer fills, which would
     * turn a longer-than-expected response into a passing assertion against a truncated string.
     */
    private static String readUntil(InputStream is, String terminator) throws Exception {
        ByteArrayOutputStream sink = new ByteArrayOutputStream();
        byte[] chunk = new byte[256];
        int n;
        while ((n = is.read(chunk)) > 0) {
            sink.write(chunk, 0, n);
            if (terminator != null && sink.toString().contains(terminator)) {
                break;
            }
        }
        return sink.toString();
    }
}
