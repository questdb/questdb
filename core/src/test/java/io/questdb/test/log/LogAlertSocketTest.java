/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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

package io.questdb.test.log;

import io.questdb.cairo.TimestampDriver;
import io.questdb.log.HttpLogRecordUtf8Sink;
import io.questdb.log.Log;
import io.questdb.log.LogAlertSocket;
import io.questdb.log.LogError;
import io.questdb.log.LogFactory;
import io.questdb.log.LogRecord;
import io.questdb.log.LogRecordUtf8Sink;
import io.questdb.mp.SOCountDownLatch;
import io.questdb.network.NetworkFacade;
import io.questdb.network.NetworkFacadeImpl;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.str.DirectUtf8Sequence;
import io.questdb.std.str.Sinkable;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf8Sequence;
import io.questdb.std.str.Utf8Sink;
import io.questdb.test.tools.TestUtils;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import java.io.File;
import java.util.Arrays;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static io.questdb.log.HttpLogRecordUtf8Sink.CRLF;

public class LogAlertSocketTest {

    private static final Log LOG = LogFactory.getLog(LogAlertSocketTest.class);
    @Rule
    public Timeout timeout = Timeout.builder()
            .withTimeout(20 * 60 * 1000, TimeUnit.MILLISECONDS)
            .withLookingForStuckThread(true)
            .build();

    public void assertAlertTargets(
            String alertTargets,
            String[] expectedHosts,
            int[] expectedPorts
    ) throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            NetworkFacade nf = new NetworkFacadeImpl() {
                @Override
                public int connect(long fd, long pSockaddr) {
                    return -1;
                }
            };

            try (LogAlertSocket socket = new LogAlertSocket(nf, alertTargets, LOG)) {
                Assert.assertEquals(expectedHosts.length, socket.getAlertHostsCount());
                Assert.assertEquals(expectedPorts.length, socket.getAlertHostsCount());
                for (int i = 0; i < expectedHosts.length; i++) {
                    Assert.assertEquals(expectedHosts[i], socket.getAlertHosts()[i]);
                    Assert.assertEquals(expectedPorts[i], socket.getAlertPorts()[i]);
                }
            }
        });
    }

    @Test
    public void testFailOver() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (LogAlertSocket alertSkt = new LogAlertSocket(NetworkFacadeImpl.INSTANCE, "localhost:1234,localhost:1242", LOG)) {
                final HttpLogRecordUtf8Sink builder = new HttpLogRecordUtf8Sink(alertSkt)
                        .putHeader("localhost")
                        .setMark();

                // start servers
                final int numHosts = alertSkt.getAlertHostsCount();
                Assert.assertEquals(2, numHosts);
                final SOCountDownLatch haltLatch = new SOCountDownLatch(numHosts);
                final SOCountDownLatch firstServerCompleted = new SOCountDownLatch(1);
                final MockAlertTarget[] servers = new MockAlertTarget[numHosts];
                final CyclicBarrier startBarrier = new CyclicBarrier(numHosts + 1);
                for (int i = 0; i < numHosts; i++) {
                    final int portNumber = alertSkt.getAlertPorts()[i];
                    servers[i] = new MockAlertTarget(
                            portNumber,
                            () -> {
                                firstServerCompleted.countDown();
                                haltLatch.countDown();
                            },
                            () -> TestUtils.await(startBarrier)
                    );
                    servers[i].start();
                }

                startBarrier.await();

                // connect to a server and send something
                alertSkt.send(
                        builder
                                .rewindToMark()
                                .put("Something")
                                .put(CRLF)
                                .put(MockAlertTarget.DEATH_PILL)
                                .put(CRLF)
                                .$()
                );
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
    public void testFailOverNoServers() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final NetworkFacade nf = new NetworkFacadeImpl() {
                @Override
                public int connect(long fd, long pSockaddr) {
                    return -1;
                }
            };
            try (LogAlertSocket alertSkt = new LogAlertSocket(nf, "localhost:1234,localhost:1243", LOG)) {
                final HttpLogRecordUtf8Sink builder = new HttpLogRecordUtf8Sink(alertSkt)
                        .putHeader("localhost")
                        .setMark();
                final int numHosts = alertSkt.getAlertHostsCount();
                Assert.assertEquals(2, numHosts);
                Assert.assertFalse(
                        alertSkt.send(builder
                                .rewindToMark()
                                .put("Something")
                                .put(CRLF)
                                .put(MockAlertTarget.DEATH_PILL)
                                .put(CRLF)
                                .$()));
            }
        });
    }

    @Test
    public void testFailOverSingleHost() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final String host = "127.0.0.1";

            // start server
            final SOCountDownLatch haltLatch = new SOCountDownLatch(1);
            final CyclicBarrier startBarrier = new CyclicBarrier(2);
            final MockAlertTarget server = new MockAlertTarget(
                    9863,
                    haltLatch::countDown,
                    () -> TestUtils.await(startBarrier)
            );
            server.start();
            startBarrier.await();
            int port = server.getPortNumber();

            try (LogAlertSocket alertSkt = new LogAlertSocket(NetworkFacadeImpl.INSTANCE, host + ":" + port, LOG)) {
                final HttpLogRecordUtf8Sink builder = new HttpLogRecordUtf8Sink(alertSkt)
                        .putHeader(host)
                        .setMark();

                // connect to server
                alertSkt.connect();

                // send something
                Assert.assertTrue(
                        alertSkt.send(builder
                                .rewindToMark()
                                .put("Something")
                                .put(CRLF)
                                .put(MockAlertTarget.DEATH_PILL)
                                .put(CRLF)
                                .$()));

                // wait for haltness
                haltLatch.await();
                Assert.assertFalse(server.isRunning());

                // send and fail after a re-connect delay
                AtomicInteger reconnectCounter = new AtomicInteger();
                Assert.assertFalse(alertSkt.send(builder.size(), reconnectCounter::incrementAndGet));
                Assert.assertEquals(2, reconnectCounter.get());
            }
        });
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
    public void testParseAlertTargetsBad() throws Exception {
        assertLogError("::", "Unexpected ':' found at position 1: ::");
        assertLogError("does not exist", "Invalid host value [does not exist] at position 0 for alertTargets: does not exist");
        assertLogError("localhost:banana", "Invalid port value [banana] at position 10 for alertTargets: localhost:banana");
        assertLogError(",:si", "Invalid port value [si] at position 2 for alertTargets: ,:si");
        assertLogError("  :  ,", "Invalid port value [  ] at position 3 for alertTargets:   :  ,");
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
    public void testParseAlertTargetsNull() throws Exception {
        String[] expectedHosts = {LogAlertSocket.DEFAULT_HOST};
        int[] expectedPorts = {LogAlertSocket.DEFAULT_PORT};
        assertAlertTargets(null, expectedHosts, expectedPorts);
    }

    @Test
    public void testParseBadResponse0() throws Exception {
        testParseStatusResponse(
                "HTTP/1.1 400 Bad Request\r\n" +
                        "Access-Control-Allow-Headers: Accept, Authorization, Content-Type, Origin\r\n" +
                        "Access-Control-Allow-Methods: GET, POST, DELETE, OPTIONS\r\n" +
                        "Access-Control-Allow-Origin: *\r\n" +
                        "Access-Control-Expose-Headers: Date\r\n" +
                        "Cache-Control: no-cache, no-store, must-revalidate\r\n" +
                        "Content-Type: application/json\r\n" +
                        "Date: Thu, 09 Dec 2021 10:01:28 GMT\r\n" +
                        "Content-Length: 6o\r\n" +
                        "\r\n" +
                        "{\"status\":\"error\",\"errorType\":\"bad_data\",\"error\":\"unexpected EOF\"}",
                "Added default alert manager [0] 127.0.0.1:9093\r\n" +
                        "Received [0] 127.0.0.1:9093: HTTP/1.1 400 Bad Request\r\n" +
                        "Access-Control-Allow-Headers: Accept, Authorization, Content-Type, Origin\r\n" +
                        "Access-Control-Allow-Methods: GET, POST, DELETE, OPTIONS\r\n" +
                        "Access-Control-Allow-Origin: *\r\n" +
                        "Access-Control-Expose-Headers: Date\r\n" +
                        "Cache-Control: no-cache, no-store, must-revalidate\r\n" +
                        "Content-Type: application/json\r\n" +
                        "Date: Thu, 09 Dec 2021 10:01:28 GMT\r\n" +
                        "Content-Length: 6o\r\n" +
                        "\r\n" +
                        "{\"status\":\"error\",\"errorType\":\"bad_data\",\"error\":\"unexpected EOF\"}\r\n"
        );
    }

    @Test
    public void testParseBadResponse1() throws Exception {
        testParseStatusResponse(
                "HTTP/1.1 400 Bad Request\r\n" +
                        "Access-Control-Allow-Headers: Accept, Authorization, Content-Type, Origin\r\n" +
                        "Access-Control-Allow-Methods: GET, POST, DELETE, OPTIONS\r\n" +
                        "Access-Control-Allow-Origin: *\r\n" +
                        "Access-Control-Expose-Headers: Date\r\n" +
                        "Cache-Control: no-cache, no-store, must-revalidate\r\n" +
                        "Content-Type: application/json\r\n" +
                        "Date: Thu, 09 Dec 2021 10:01:28 GMT\r\n" +
                        "\r\n" +
                        "{\"status\":\"error\",\"errorType\":\"bad_data\",\"error\":\"unexpected EOF\"}",
                "Added default alert manager [0] 127.0.0.1:9093\r\n" +
                        "Received [0] 127.0.0.1:9093: HTTP/1.1 400 Bad Request\r\n" +
                        "Access-Control-Allow-Headers: Accept, Authorization, Content-Type, Origin\r\n" +
                        "Access-Control-Allow-Methods: GET, POST, DELETE, OPTIONS\r\n" +
                        "Access-Control-Allow-Origin: *\r\n" +
                        "Access-Control-Expose-Headers: Date\r\n" +
                        "Cache-Control: no-cache, no-store, must-revalidate\r\n" +
                        "Content-Type: application/json\r\n" +
                        "Date: Thu, 09 Dec 2021 10:01:28 GMT\r\n" +
                        "\r\n" +
                        "{\"status\":\"error\",\"errorType\":\"bad_data\",\"error\":\"unexpected EOF\"}\r\n"
        );
    }

    @Test
    public void testParseBadResponse2() throws Exception {
        testParseStatusResponse(
                "HTTP/1.1 400 Bad Request\r\n" +
                        "Access-Control-Allow-Headers: Accept, Authorization, Content-Type, Origin\r\n" +
                        "Access-Control-Allow-Methods: GET, POST, DELETE, OPTIONS\r\n" +
                        "Access-Control-Allow-Origin: *\r\n" +
                        "Access-Control-Expose-Headers: Date\r\n" +
                        "Cache-Control: no-cache, no-store, must-revalidate\r\n" +
                        "Content-Type: application/json\r\n" +
                        "Date: Thu, 09 Dec 2021 10:01:28 GMT\r\n" +
                        "Content-Length: 6\r\n" +
                        "\r\n" +
                        "{\"status\":\"error\",\"errorType\":\"bad_data\",\"error\":\"unexpected EOF\"}",
                "Added default alert manager [0] 127.0.0.1:9093\r\n" +
                        "Received [0] 127.0.0.1:9093: HTTP/1.1 400 Bad Request\r\n" +
                        "Access-Control-Allow-Headers: Accept, Authorization, Content-Type, Origin\r\n" +
                        "Access-Control-Allow-Methods: GET, POST, DELETE, OPTIONS\r\n" +
                        "Access-Control-Allow-Origin: *\r\n" +
                        "Access-Control-Expose-Headers: Date\r\n" +
                        "Cache-Control: no-cache, no-store, must-revalidate\r\n" +
                        "Content-Type: application/json\r\n" +
                        "Date: Thu, 09 Dec 2021 10:01:28 GMT\r\n" +
                        "Content-Length: 6\r\n" +
                        "\r\n" +
                        "{\"status\":\"error\",\"errorType\":\"bad_data\",\"error\":\"unexpected EOF\"}\r\n"
        );
    }

    @Test
    public void testParseBadResponse3() throws Exception {
        testParseStatusResponse(
                "HTTP/1.1 400 Bad Request\r\n" +
                        "Access-Control-Allow-Headers: Accept, Authorization, Content-Type, Origin\r\n" +
                        "Access-Control-Allow-Methods: GET, POST, DELETE, OPTIONS\r\n" +
                        "Access-Control-Allow-Origin: *\r\n" +
                        "Access-Control-Expose-Headers: Date\r\n" +
                        "Cache-Control: no-cache, no-store, must-revalidate\r\n" +
                        "Content-Type: application/json\r\n" +
                        "Date: Thu, 09 Dec 2021 10:01:28 GMT\r\n" +
                        "Content-Length: 66\r\n" +
                        "{\"status\":\"error\",\"errorType\":\"bad_data\",\"error\":\"unexpected EOF\"}",
                "Added default alert manager [0] 127.0.0.1:9093\r\n" +
                        "Received [0] 127.0.0.1:9093: HTTP/1.1 400 Bad Request\r\n" +
                        "Access-Control-Allow-Headers: Accept, Authorization, Content-Type, Origin\r\n" +
                        "Access-Control-Allow-Methods: GET, POST, DELETE, OPTIONS\r\n" +
                        "Access-Control-Allow-Origin: *\r\n" +
                        "Access-Control-Expose-Headers: Date\r\n" +
                        "Cache-Control: no-cache, no-store, must-revalidate\r\n" +
                        "Content-Type: application/json\r\n" +
                        "Date: Thu, 09 Dec 2021 10:01:28 GMT\r\n" +
                        "Content-Length: 66\r\n" +
                        "{\"status\":\"error\",\"errorType\":\"bad_data\",\"error\":\"unexpected EOF\"}\r\n"
        );
    }

    @Test
    public void testParseStatusErrorResponse() throws Exception {
        testParseStatusResponse(
                "HTTP/1.1 400 Bad Request\r\n" +
                        "Access-Control-Allow-Headers: Accept, Authorization, Content-Type, Origin\r\n" +
                        "Access-Control-Allow-Methods: GET, POST, DELETE, OPTIONS\r\n" +
                        "Access-Control-Allow-Origin: *\r\n" +
                        "Access-Control-Expose-Headers: Date\r\n" +
                        "Cache-Control: no-cache, no-store, must-revalidate\r\n" +
                        "Content-Type: application/json\r\n" +
                        "Date: Thu, 09 Dec 2021 10:01:28 GMT\r\n" +
                        "Content-Length: 66\r\n" +
                        "\r\n" +
                        "{\"status\":\"error\",\"errorType\":\"bad_data\",\"error\":\"unexpected EOF\"}",
                "Added default alert manager [0] 127.0.0.1:9093\r\n" +
                        "Received [0] 127.0.0.1:9093: {\"status\":\"error\",\"errorType\":\"bad_data\",\"error\":\"unexpected EOF\"}\r\n"
        );
    }

    @Test
    public void testParseStatusSuccessResponse() throws Exception {
        testParseStatusResponse(
                "HTTP/1.1 200 OK\r\n" +
                        "Access-Control-Allow-Headers: Accept, Authorization, Content-Type, Origin\r\n" +
                        "Access-Control-Allow-Methods: GET, POST, DELETE, OPTIONS\r\n" +
                        "Access-Control-Allow-Origin: *\r\n" +
                        "Access-Control-Expose-Headers: Date\r\n" +
                        "Cache-Control: no-cache, no-store, must-revalidate\r\n" +
                        "Content-Type: application/json\r\n" +
                        "Date: Thu, 09 Dec 2021 09:37:22 GMT\r\n" +
                        "Content-Length: 20\r\n" +
                        "\r\n" +
                        "{\"status\":\"success\"}",
                "Added default alert manager [0] 127.0.0.1:9093\r\n" +
                        "Received [0] 127.0.0.1:9093: {\"status\":\"success\"}\r\n"
        );
    }

    private void assertLogError(String socketAddress, String expected) throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            NetworkFacade nf = new NetworkFacadeImpl() {
                @Override
                public int connect(long fd, long pSockaddr) {
                    return -1;
                }
            };
            try (LogAlertSocket ignored = new LogAlertSocket(nf, socketAddress, LOG)) {
                Assert.fail();
            } catch (LogError logError) {
                Assert.assertEquals(expected, logError.getMessage());
            }
        });
    }

    private void testParseStatusResponse(CharSequence httpMessage, CharSequence expected) throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            NetworkFacade nf = new NetworkFacadeImpl() {
                @Override
                public int connect(long fd, long pSockaddr) {
                    return -1;
                }
            };
            MockLog log = new MockLog();
            try (LogAlertSocket alertSkt = new LogAlertSocket(nf, "", log)) {
                LogRecordUtf8Sink logRecord = new LogRecordUtf8Sink(alertSkt.getInBufferPtr(), alertSkt.getInBufferSize());
                logRecord.put(httpMessage);
                alertSkt.logResponse(logRecord.size());
                TestUtils.assertEquals(expected, log.logRecord.sink);
            }
        });
    }

    private static class MockLog implements Log {
        final MockLogRecord logRecord = new MockLogRecord();

        @Override
        public LogRecord advisory() {
            throw new UnsupportedOperationException();
        }

        @Override
        public LogRecord advisoryW() {
            throw new UnsupportedOperationException();
        }

        @Override
        public LogRecord critical() {
            throw new UnsupportedOperationException();
        }

        @Override
        public LogRecord debug() {
            throw new UnsupportedOperationException();
        }

        @Override
        public LogRecord debugW() {
            throw new UnsupportedOperationException();
        }

        @Override
        public LogRecord error() {
            throw new UnsupportedOperationException();
        }

        @Override
        public LogRecord errorW() {
            throw new UnsupportedOperationException();
        }

        @Override
        public LogRecord info() {
            return logRecord;
        }

        @Override
        public LogRecord infoW() {
            throw new UnsupportedOperationException();
        }

        @Override
        public LogRecord xDebugW() {
            throw new UnsupportedOperationException();
        }

        @Override
        public LogRecord xInfoW() {
            throw new UnsupportedOperationException();
        }

        @Override
        public LogRecord xadvisory() {
            throw new UnsupportedOperationException();
        }

        @Override
        public LogRecord xcritical() {
            throw new UnsupportedOperationException();
        }

        @Override
        public LogRecord xdebug() {
            throw new UnsupportedOperationException();
        }

        @Override
        public LogRecord xerror() {
            throw new UnsupportedOperationException();
        }

        @Override
        public LogRecord xinfo() {
            throw new UnsupportedOperationException();
        }
    }

    private static class MockLogRecord implements LogRecord {
        final StringSink sink = new StringSink();

        @Override
        public void $() {
            sink.put(Misc.EOL);
        }

        @Override
        public LogRecord $(@Nullable CharSequence sequence) {
            sink.put(sequence);
            return this;
        }

        @Override
        public LogRecord $(@Nullable Utf8Sequence sequence) {
            throw new UnsupportedOperationException();
        }

        @Override
        public LogRecord $(@Nullable DirectUtf8Sequence sequence) {
            throw new UnsupportedOperationException();
        }

        @Override
        public LogRecord $(int x) {
            sink.put(x);
            return this;
        }

        @Override
        public LogRecord $(char c) {
            sink.put(c);
            return this;
        }

        @Override
        public LogRecord $(double x) {
            throw new UnsupportedOperationException();
        }

        @Override
        public LogRecord $(long l) {
            throw new UnsupportedOperationException();
        }

        @Override
        public LogRecord $(boolean x) {
            throw new UnsupportedOperationException();
        }

        @Override
        public LogRecord $(@Nullable Throwable e) {
            throw new UnsupportedOperationException();
        }

        @Override
        public LogRecord $(@Nullable File x) {
            throw new UnsupportedOperationException();
        }

        @Override
        public LogRecord $(@Nullable Object x) {
            throw new UnsupportedOperationException();
        }

        @Override
        public LogRecord $(@Nullable Sinkable x) {
            throw new UnsupportedOperationException();
        }

        @Override
        public LogRecord $256(long a, long b, long c, long d) {
            throw new UnsupportedOperationException();
        }

        @Override
        public LogRecord $hex(long value) {
            Numbers.appendHex(sink, value, false);
            return this;
        }

        @Override
        public LogRecord $hexPadded(long value) {
            Numbers.appendHex(sink, value, true);
            return this;
        }

        @Override
        public LogRecord $ip(long ip) {
            throw new UnsupportedOperationException();
        }

        @Override
        public LogRecord $safe(@NotNull CharSequence sequence, int lo, int hi) {
            sink.put(sequence, lo, hi);
            return this;
        }

        @Override
        public LogRecord $safe(@Nullable DirectUtf8Sequence sequence) {
            return this;
        }

        @Override
        public LogRecord $safe(@Nullable Utf8Sequence sequence) {
            return this;
        }

        @Override
        public LogRecord $safe(long lo, long hi) {
            throw new UnsupportedOperationException();
        }

        @Override
        public LogRecord $safe(@Nullable CharSequence sequence) {
            throw new UnsupportedOperationException();
        }

        @Override
        public LogRecord $size(long memoryBytes) {
            sink.putSize(memoryBytes);
            return this;
        }

        @Override
        public LogRecord $substr(int from, @Nullable DirectUtf8Sequence sequence) {
            throw new UnsupportedOperationException();
        }

        @Override
        public LogRecord $ts(long x) {
            throw new UnsupportedOperationException();
        }

        @Override
        public LogRecord $ts(TimestampDriver driver, long x) {
            throw new UnsupportedOperationException();
        }

        @Override
        public LogRecord $uuid(long lo, long hi) {
            Numbers.appendUuid(lo, hi, sink);
            return this;
        }

        @Override
        public boolean isEnabled() {
            return true;
        }

        @Override
        public LogRecord microTime(long x) {
            throw new UnsupportedOperationException();
        }

        @Override
        public LogRecord put(@Nullable Utf8Sequence us) {
            sink.put(us);
            return this;
        }

        @Override
        public LogRecord put(byte b) {
            sink.put(b);
            return this;
        }

        @Override
        public LogRecord put(char c) {
            sink.put(c);
            return this;
        }

        @Override
        public Utf8Sink putNonAscii(long lo, long hi) {
            sink.put(lo, hi);
            return this;
        }

        @Override
        public LogRecord ts() {
            throw new UnsupportedOperationException();
        }
    }
}
