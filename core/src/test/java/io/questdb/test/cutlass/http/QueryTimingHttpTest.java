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

import io.questdb.PropertyKey;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cutlass.parquet.ParquetExportMode;
import io.questdb.griffin.SqlCompiler;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.test.AbstractBootstrapTest;
import io.questdb.test.TestServerMain;
import org.junit.Assert;
import org.junit.Test;

import java.io.BufferedInputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;

public class QueryTimingHttpTest extends AbstractBootstrapTest {
    private static final Log LOG = LogFactory.getLog(QueryTimingHttpTest.class);
    private static final int CLIENT_RECEIVE_BUFFER_SIZE = 1_024;
    private static final int MIN_RESPONSE_SIZE = 4_000_000;
    private static final long READ_PAUSE_MILLIS = 750;
    private static final String TABLE_DDL = """
            CREATE TABLE timing_tab AS (
                SELECT x, rnd_str(128, 128, 0) AS s
                FROM long_sequence(100_000)
            )
            """;

    @Test
    public void testSlowCsvExportClientCountsAsWait() throws Exception {
        assertSlowClientTiming("/exp", "SELECT * FROM timing_tab");
    }

    @Test
    public void testSlowDirectPageFrameParquetExportClientCountsAsWait() throws Exception {
        assertSlowClientTiming("/exp?fmt=parquet", "SELECT * FROM timing_tab", ParquetExportMode.DIRECT_PAGE_FRAME);
    }

    @Test
    public void testSlowExecClientCountsAsWait() throws Exception {
        assertSlowClientTiming("/exec", "SELECT * FROM timing_tab");
    }

    @Test
    public void testSlowPageFrameBackedParquetExportClientCountsAsWait() throws Exception {
        assertSlowClientTiming("/exp?fmt=parquet", "SELECT x + 1 AS computed_x, s FROM timing_tab");
    }

    private static void assertEventually(TestServerMain serverMain, String query) throws Exception {
        int sleepMillis = 100;
        while (true) {
            Thread.sleep(sleepMillis);
            try {
                serverMain.assertSql(
                        "SELECT count() FROM _query_trace "
                                + "WHERE query_text = '" + query.replace("'", "''") + "' "
                                + "AND client_wait_micros > 0 "
                                + "AND client_wait_micros <= execution_micros "
                                + "AND first_row_micros IS NOT NULL "
                                + "AND first_row_micros >= 0 "
                                + "AND first_row_micros <= execution_micros",
                        "count\n1\n"
                );
                return;
            } catch (AssertionError e) {
                if (sleepMillis >= 6_400) {
                    throw e;
                }
                sleepMillis *= 2;
            }
        }
    }

    private static void assertParquetExportMode(TestServerMain serverMain, String query, ParquetExportMode expectedExportMode) throws Exception {
        try (
                SqlCompiler compiler = serverMain.getEngine().getSqlCompiler();
                RecordCursorFactory factory = compiler.compile(query, serverMain.getSqlExecutionContext()).getRecordCursorFactory()
        ) {
            Assert.assertEquals(
                    expectedExportMode,
                    ParquetExportMode.determineExportMode(
                            factory,
                            factory.getScanDirection() == RecordCursorFactory.SCAN_DIRECTION_BACKWARD,
                            serverMain.getSqlExecutionContext()
                    )
            );
        }
    }

    private static void assertSlowClientTiming(String path, String query) throws Exception {
        assertSlowClientTiming(path, query, null);
    }

    private static void assertSlowClientTiming(String path, String query, ParquetExportMode expectedExportMode) throws Exception {
        assertMemoryLeak(() -> {
            try (TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_BIND_TO.getEnvVarName(), "127.0.0.1:0",
                    PropertyKey.HTTP_MIN_ENABLED.getEnvVarName(), "false",
                    PropertyKey.LINE_TCP_ENABLED.getEnvVarName(), "false",
                    PropertyKey.PG_ENABLED.getEnvVarName(), "false",
                    PropertyKey.QUERY_TRACING_ENABLED.getEnvVarName(), "true",
                    PropertyKey.HTTP_SEND_BUFFER_SIZE.getEnvVarName(), "1024"
            )) {
                serverMain.execute(TABLE_DDL);
                if (expectedExportMode != null) {
                    assertParquetExportMode(serverMain, query, expectedExportMode);
                }
                int responseSize = executeSlowGet(serverMain.getHttpServerPort(), path, query);
                LOG.info().$("slow client response [path=").$(path).$(", bytes=").$(responseSize).I$();
                Assert.assertTrue("response must exceed kernel buffering [bytes=" + responseSize + ']', responseSize > MIN_RESPONSE_SIZE);
                assertEventually(serverMain, query);
            }
        });
    }

    private static int executeSlowGet(int port, String path, String query) throws Exception {
        try (Socket socket = new Socket()) {
            socket.setReceiveBufferSize(CLIENT_RECEIVE_BUFFER_SIZE);
            socket.setSoTimeout(300_000);
            socket.connect(new InetSocketAddress("127.0.0.1", port));

            String separator = path.indexOf('?') > -1 ? "&" : "?";
            String request = "GET " + path + separator + "query=" + URLEncoder.encode(query, StandardCharsets.UTF_8)
                    + " HTTP/1.1\r\n"
                    + "Host: localhost\r\n"
                    + "Accept: */*\r\n"
                    + "Accept-Encoding: identity\r\n"
                    + "Connection: close\r\n"
                    + "\r\n";
            OutputStream output = socket.getOutputStream();
            output.write(request.getBytes(StandardCharsets.US_ASCII));
            output.flush();

            Thread.sleep(READ_PAUSE_MILLIS);

            InputStream input = new BufferedInputStream(socket.getInputStream());
            String statusLine = readAsciiLine(input);
            Assert.assertTrue(statusLine, statusLine.startsWith("HTTP/1.1 200"));
            boolean isChunked = false;
            String header;
            while (!(header = readAsciiLine(input)).isEmpty()) {
                if (header.equalsIgnoreCase("Transfer-Encoding: chunked")) {
                    isChunked = true;
                }
            }
            Assert.assertTrue("expected a chunked response", isChunked);

            int responseSize = 0;
            byte[] buffer = new byte[8_192];
            while (true) {
                String chunkHeader = readAsciiLine(input);
                int extensionIndex = chunkHeader.indexOf(';');
                int chunkSize = Integer.parseInt(extensionIndex > -1 ? chunkHeader.substring(0, extensionIndex) : chunkHeader, 16);
                if (chunkSize == 0) {
                    while (!readAsciiLine(input).isEmpty()) {
                        // Consume trailers.
                    }
                    return responseSize;
                }
                int remaining = chunkSize;
                while (remaining > 0) {
                    int read = input.read(buffer, 0, Math.min(buffer.length, remaining));
                    Assert.assertTrue("response ended inside a chunk", read > 0);
                    responseSize += read;
                    remaining -= read;
                }
                Assert.assertEquals('\r', input.read());
                Assert.assertEquals('\n', input.read());
            }
        }
    }

    private static String readAsciiLine(InputStream input) throws Exception {
        StringBuilder line = new StringBuilder();
        while (true) {
            int b = input.read();
            Assert.assertTrue("response ended before the terminal chunk", b > -1);
            if (b == '\r') {
                Assert.assertEquals('\n', input.read());
                return line.toString();
            }
            line.append((char) b);
        }
    }
}
