/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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

package io.questdb.log;

import io.questdb.mp.SOCountDownLatch;
import io.questdb.std.str.StringSink;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;

import static io.questdb.log.HttpLogRecordSink.CRLF;
import static io.questdb.log.LogAlertSocket.filterHttpHeader;

public class LogAlertSocketTest {

    private static final Log LOG = LogFactory.getLog(LogAlertSocketTest.class);

    private StringSink sink;

    @Before
    public void setUp() {
        sink = new StringSink();
    }

    @Test
    public void testParseAlertTargetsEmpty() throws Exception {
        String[] expectedHosts = {LogAlertSocket.DEFAULT_HOST};
        int[] expectedPorts = {LogAlertSocket.DEFAULT_PORT};
        assertAlertTargets("", expectedHosts, expectedPorts);
        assertAlertTargets(":", expectedHosts, expectedPorts);
        assertAlertTargets(":    ", expectedHosts, expectedPorts);
        assertAlertTargets("  :  ", expectedHosts, expectedPorts);
        assertAlertTargets("    :", expectedHosts, expectedPorts);
        assertAlertTargets("     ", expectedHosts, expectedPorts);
        assertAlertTargets("\"\"", expectedHosts, expectedPorts);
        assertAlertTargets("\":\"", expectedHosts, expectedPorts);
        assertAlertTargets("\"     \"", expectedHosts, expectedPorts);
        assertAlertTargets("\":     \"", expectedHosts, expectedPorts);
        assertAlertTargets("\"  :  \"", expectedHosts, expectedPorts);
        assertAlertTargets("\"     :\"", expectedHosts, expectedPorts);
    }

    @Test
    public void testParseAlertTargetsMultipleEmpty0() throws Exception {
        String[] expectedHosts = {LogAlertSocket.DEFAULT_HOST, LogAlertSocket.DEFAULT_HOST};
        int[] expectedPorts = {LogAlertSocket.DEFAULT_PORT, LogAlertSocket.DEFAULT_PORT};
        assertAlertTargets(",    ", expectedHosts, expectedPorts);
        assertAlertTargets("  ,  ", expectedHosts, expectedPorts);
        assertAlertTargets("    ,", expectedHosts, expectedPorts);
        assertAlertTargets(":,    ", expectedHosts, expectedPorts);
        assertAlertTargets("  ,:  ", expectedHosts, expectedPorts);
        assertAlertTargets(",", expectedHosts, expectedPorts);
        assertAlertTargets("\",    \"", expectedHosts, expectedPorts);
        assertAlertTargets("\"  ,  \"", expectedHosts, expectedPorts);
        assertAlertTargets("\"    ,\"", expectedHosts, expectedPorts);
        assertAlertTargets("\",\"", expectedHosts, expectedPorts);
    }

    @Test
    public void testParseAlertTargetsMultipleEmpty1() throws Exception {
        String[] expectedHosts = new String[9];
        int[] expectedPorts = new int[9];
        Arrays.fill(expectedHosts, LogAlertSocket.DEFAULT_HOST);
        Arrays.fill(expectedPorts, LogAlertSocket.DEFAULT_PORT);
        assertAlertTargets(",,, :,:,,,    :,", expectedHosts, expectedPorts);
    }

    @Test
    public void testParseAlertTargets() throws Exception {
        String[] expectedHosts = new String[3];
        int[] expectedPorts = new int[3];
        Arrays.fill(expectedHosts, LogAlertSocket.DEFAULT_HOST);
        Arrays.fill(expectedPorts, LogAlertSocket.DEFAULT_PORT);
        expectedPorts[1] = 1234;
        assertAlertTargets("localhost,127.0.0.1:1234,localhost:", expectedHosts, expectedPorts);
    }

    @Test
    public void testParseAlertTargetsNull() throws Exception {
        String[] expectedHosts = {LogAlertSocket.DEFAULT_HOST};
        int[] expectedPorts = {LogAlertSocket.DEFAULT_PORT};
        assertAlertTargets(null, expectedHosts, expectedPorts);
    }

    @Test
    public void testParseAlertTargetsBad() throws Exception {
        assertLogError("::", "Unexpected ':' found at position 1: ::");
        assertLogError("does not exist", "Invalid host value [does not exist] at position 0 for alertTargets: does not exist");
        assertLogError("localhost:banana", "Invalid port value [banana] at position 10 for alertTargets: localhost:banana");
        assertLogError(",:si", "Invalid port value [si] at position 2 for alertTargets: ,:si");
        assertLogError("  :  ,", "Invalid port value [  ] at position 3 for alertTargets:   :  ,");
    }

    @Test
    public void testFailOver() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (LogAlertSocket alertSkt = new LogAlertSocket("localhost:1234,localhost:1242", LOG)) {
                final HttpLogRecordSink builder = new HttpLogRecordSink(alertSkt)
                        .putHeader("localhost")
                        .setMark();

                // start servers
                final int numHosts = alertSkt.getAlertHostsCount();
                Assert.assertEquals(2, numHosts);
                final SOCountDownLatch haltLatch = new SOCountDownLatch(numHosts);
                final SOCountDownLatch firstServerCompleted = new SOCountDownLatch(1);
                final MockAlertTarget[] servers = new MockAlertTarget[numHosts];
                for (int i = 0; i < numHosts; i++) {
                    final int portNumber = alertSkt.getAlertPorts()[i];
                    servers[i] = new MockAlertTarget(portNumber, () -> {
                        firstServerCompleted.countDown();
                        haltLatch.countDown();
                    });
                    servers[i].start();
                }

                // connect to a server and send something
                alertSkt.send(builder
                        .rewindToMark()
                        .put("Something")
                        .put(CRLF)
                        .put(MockAlertTarget.DEATH_PILL)
                        .put(CRLF)
                        .$());
                Assert.assertTrue(firstServerCompleted.await(20_000_000_000L));

                // by now there is only one server surviving, and we are connected to the other.
                // send a death pill and kill the surviving server.
                builder.clear();
                alertSkt.send(builder.put(MockAlertTarget.DEATH_PILL).put(CRLF).$());

                // wait for haltness, and then all servers should be done.
                Assert.assertTrue(haltLatch.await(20_000_000_000L));
                for (int i = 0; i < numHosts; i++) {
                    Assert.assertFalse(servers[i].isRunning());
                }
            }
        });
    }

    @Test
    public void testFailOverSingleHost() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final String host = "127.0.0.1";
            final int port = 1241;
            try (LogAlertSocket alertSkt = new LogAlertSocket(host + ":" + port, LOG)) {
                final HttpLogRecordSink builder = new HttpLogRecordSink(alertSkt)
                        .putHeader(host)
                        .setMark();

                // start server
                final SOCountDownLatch haltLatch = new SOCountDownLatch(1);
                final MockAlertTarget server = new MockAlertTarget(port, () -> {
                    haltLatch.countDown();
                });
                server.start();

                // connect to server
                alertSkt.connect();

                // send something
                alertSkt.send(builder
                                .rewindToMark()
                                .put("Something")
                                .put(CRLF)
                                .put(MockAlertTarget.DEATH_PILL)
                                .put(CRLF)
                                .$(),
                        ack -> Assert.assertEquals(MockAlertTarget.ACK, ack));

                // wait for haltness
                Assert.assertTrue(haltLatch.await(20_000_000_000L));
                Assert.assertFalse(server.isRunning());

                // send and fail after a reconnect delay
                final long start = System.nanoTime();
                alertSkt.send(builder.length(), ack -> Assert.assertEquals(LogAlertSocket.NACK, ack));
                Assert.assertTrue(System.nanoTime() - start >= 2 * LogAlertSocket.RECONNECT_DELAY_NANO);
            }
        });
    }

    @Test
    public void testFailOverNoServers() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (LogAlertSocket alertSkt = new LogAlertSocket("localhost:1234,localhost:1243", LOG)) {
                final HttpLogRecordSink builder = new HttpLogRecordSink(alertSkt)
                        .putHeader("localhost")
                        .setMark();

                // start servers
                final int numHosts = alertSkt.getAlertHostsCount();
                Assert.assertEquals(2, numHosts);

                // connect to a server and send something
                Assert.assertFalse(
                        alertSkt.send(builder
                                        .rewindToMark()
                                        .put("Something")
                                        .put(CRLF)
                                        .put(MockAlertTarget.DEATH_PILL)
                                        .put(CRLF)
                                        .$(),
                                ack -> Assert.assertEquals(ack, LogAlertSocket.NACK)
                        ));
            }
        });
    }

    @Test
    public void testParseStatusSuccessResponse() {
        sink.put("HTTP/1.1 200 OK\r\n" +
                "Access-Control-Allow-Headers: Accept, Authorization, Content-Type, Origin\r\n" +
                "Access-Control-Allow-Methods: GET, POST, DELETE, OPTIONS\r\n" +
                "Access-Control-Allow-Origin: *\r\n" +
                "Access-Control-Expose-Headers: Date\r\n" +
                "Cache-Control: no-cache, no-store, must-revalidate\r\n" +
                "Content-Type: application/json\r\n" +
                "Date: Thu, 09 Dec 2021 09:37:22 GMT\r\n" +
                "Content-Length: 20\r\n" +
                "\r\n" +
                "{\"status\":\"success\"}");
        TestUtils.assertEquals("{\"status\":\"success\"}", filterHttpHeader(sink));
    }

    @Test
    public void testParseStatusErrorResponse() {
        sink.put("HTTP/1.1 400 Bad Request\r\n" +
                "Access-Control-Allow-Headers: Accept, Authorization, Content-Type, Origin\r\n" +
                "Access-Control-Allow-Methods: GET, POST, DELETE, OPTIONS\r\n" +
                "Access-Control-Allow-Origin: *\r\n" +
                "Access-Control-Expose-Headers: Date\r\n" +
                "Cache-Control: no-cache, no-store, must-revalidate\r\n" +
                "Content-Type: application/json\r\n" +
                "Date: Thu, 09 Dec 2021 10:01:28 GMT\r\n" +
                "Content-Length: 66\r\n" +
                "\r\n" +
                "{\"status\":\"error\",\"errorType\":\"bad_data\",\"error\":\"unexpected EOF\"}");
        TestUtils.assertEquals(
                "{\"status\":\"error\",\"errorType\":\"bad_data\",\"error\":\"unexpected EOF\"}",
                filterHttpHeader(sink)
        );
    }

    @Test
    public void testParseBadResponse0() {
        String response = "HTTP/1.1 400 Bad Request\r\n" +
                "Access-Control-Allow-Headers: Accept, Authorization, Content-Type, Origin\r\n" +
                "Access-Control-Allow-Methods: GET, POST, DELETE, OPTIONS\r\n" +
                "Access-Control-Allow-Origin: *\r\n" +
                "Access-Control-Expose-Headers: Date\r\n" +
                "Cache-Control: no-cache, no-store, must-revalidate\r\n" +
                "Content-Type: application/json\r\n" +
                "Date: Thu, 09 Dec 2021 10:01:28 GMT\r\n" +
                "Content-Length: 6o\r\n" +
                "\r\n" +
                "{\"status\":\"error\",\"errorType\":\"bad_data\",\"error\":\"unexpected EOF\"}";
        sink.put(response);
        TestUtils.assertEquals(response, filterHttpHeader(sink));
    }

    @Test
    public void testParseBadResponse1() {
        String response = "HTTP/1.1 400 Bad Request\r\n" +
                "Access-Control-Allow-Headers: Accept, Authorization, Content-Type, Origin\r\n" +
                "Access-Control-Allow-Methods: GET, POST, DELETE, OPTIONS\r\n" +
                "Access-Control-Allow-Origin: *\r\n" +
                "Access-Control-Expose-Headers: Date\r\n" +
                "Cache-Control: no-cache, no-store, must-revalidate\r\n" +
                "Content-Type: application/json\r\n" +
                "Date: Thu, 09 Dec 2021 10:01:28 GMT\r\n" +
                "\r\n" +
                "{\"status\":\"error\",\"errorType\":\"bad_data\",\"error\":\"unexpected EOF\"}";
        sink.put(response);
        TestUtils.assertEquals(response, filterHttpHeader(sink));
    }

    @Test
    public void testParseBadResponse2() {
        String response = "HTTP/1.1 400 Bad Request\r\n" +
                "Access-Control-Allow-Headers: Accept, Authorization, Content-Type, Origin\r\n" +
                "Access-Control-Allow-Methods: GET, POST, DELETE, OPTIONS\r\n" +
                "Access-Control-Allow-Origin: *\r\n" +
                "Access-Control-Expose-Headers: Date\r\n" +
                "Cache-Control: no-cache, no-store, must-revalidate\r\n" +
                "Content-Type: application/json\r\n" +
                "Date: Thu, 09 Dec 2021 10:01:28 GMT\r\n" +
                "Content-Length: 6\r\n" +
                "\r\n" +
                "{\"status\":\"error\",\"errorType\":\"bad_data\",\"error\":\"unexpected EOF\"}";
        sink.put(response);
        TestUtils.assertEquals(response, filterHttpHeader(sink));
    }

    @Test
    public void testParseBadResponse3() {
        String response = "HTTP/1.1 400 Bad Request\r\n" +
                "Access-Control-Allow-Headers: Accept, Authorization, Content-Type, Origin\r\n" +
                "Access-Control-Allow-Methods: GET, POST, DELETE, OPTIONS\r\n" +
                "Access-Control-Allow-Origin: *\r\n" +
                "Access-Control-Expose-Headers: Date\r\n" +
                "Cache-Control: no-cache, no-store, must-revalidate\r\n" +
                "Content-Type: application/json\r\n" +
                "Date: Thu, 09 Dec 2021 10:01:28 GMT\r\n" +
                "Content-Length: 66\r\n" +
                "{\"status\":\"error\",\"errorType\":\"bad_data\",\"error\":\"unexpected EOF\"}";
        sink.put(response);
        TestUtils.assertEquals(response, filterHttpHeader(sink));
    }

    private void assertLogError(String socketAddress, String expected) throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (LogAlertSocket ignored = new LogAlertSocket(socketAddress, LOG)) {
                Assert.fail();
            } catch (LogError logError) {
                Assert.assertEquals(expected, logError.getMessage());
            }
        });
    }

    public void assertAlertTargets(
            String alertTargets,
            String[] expectedHosts,
            int[] expectedPorts
    ) throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (LogAlertSocket socket = new LogAlertSocket(alertTargets, LOG)) {
                Assert.assertEquals(expectedHosts.length, socket.getAlertHostsCount());
                Assert.assertEquals(expectedPorts.length, socket.getAlertHostsCount());
                for (int i = 0; i < expectedHosts.length; i++) {
                    Assert.assertEquals(expectedHosts[i], socket.getAlertHosts()[i]);
                    Assert.assertEquals(expectedPorts[i], socket.getAlertPorts()[i]);
                }
            }
        });
    }
}
