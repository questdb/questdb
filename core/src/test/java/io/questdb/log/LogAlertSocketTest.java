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

import io.questdb.std.str.StringSink;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static io.questdb.log.HttpLogAlertBuilder.CRLF;

public class LogAlertSocketTest {

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
        assertLogError("does not exist", "Invalid host value [does not exist] at position 0 for socketAddress: does not exist");
        assertLogError("localhost:banana", "Invalid port value [banana] at position 10 for socketAddress: localhost:banana");
        assertLogError(",:si", "Invalid port value [si] at position 2 for socketAddress: ,:si");
        assertLogError("  :  ,", "Invalid port value [  ] at position 3 for socketAddress:   :  ,");
    }

    @Test
    public void testFailOver() {
        try (LogAlertSocket alertSkt = new LogAlertSocket("localhost:1234,localhost:1243")) {
            final HttpLogAlertBuilder builder = new HttpLogAlertBuilder(alertSkt)
                    .putHeader("localhost")
                    .setMark();

            // start servers
            final int numHosts = alertSkt.getAlertHostsCount();
            Assert.assertEquals(2, numHosts);
            final CountDownLatch haltLatch = new CountDownLatch(numHosts);
            final CountDownLatch firstServerCompleted = new CountDownLatch(1);
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
            try {
                firstServerCompleted.await(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Assert.fail("timed-out");
            }

            // by now there is only one server surviving, and we are connected to the other.
            // send a death pill and kill the surviving server.
            builder.clear();
            builder.put(MockAlertTarget.DEATH_PILL).put(CRLF);
            Assert.assertTrue(
                    alertSkt.send(
                            builder.length(),
                            numHosts,
                            ack -> Assert.assertEquals(ack, MockAlertTarget.ACK)
                    ));

            // wait for haltness
            try {
                haltLatch.await(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Assert.fail("timed-out");
            }
            // all servers should be done.
            for (int i = 0; i < servers.length; i++) {
                Assert.assertEquals(Thread.State.TERMINATED, servers[i].getState());
            }
        }
    }

    @Test
    public void testFailOverNoServers() {
        try (LogAlertSocket alertSkt = new LogAlertSocket("localhost:1234,localhost:1243")) {
            final HttpLogAlertBuilder builder = new HttpLogAlertBuilder(alertSkt)
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
                            10,
                            ack -> Assert.assertEquals(ack, LogAlertSocket.NACK)
                    ));
        }
    }

    private void assertLogError(String socketAddress, String expected) throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (LogAlertSocket ignored = new LogAlertSocket(socketAddress, 1024)) {
                Assert.fail();
            } catch (LogError logError) {
                Assert.assertEquals(expected, logError.getMessage());
            }
        });
    }

    public void assertAlertTargets(
            String socketAddress,
            String[] expectedHosts,
            int[] expectedPorts
    ) throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (LogAlertSocket socket = new LogAlertSocket(socketAddress, 1024)) {
                Assert.assertEquals(expectedHosts.length, socket.getAlertHostsCount());
                Assert.assertEquals(expectedPorts.length, socket.getAlertHostsCount());
                for (int i = 0; i < expectedHosts.length; i++) {
                    Assert.assertEquals(expectedHosts[i], socket.getAlertHosts()[i]);
                    Assert.assertEquals(expectedPorts[i], socket.getAlertPorts()[i]);
                }
            }
        });
    }

    private static class MockAlertTarget extends Thread {
        static final String ACK = "Ack";
        static final String DEATH_PILL = ".ByE.";

        private final int portNumber;
        private final Runnable onTargetEnd;
        private final AtomicReference<State> endState;


        MockAlertTarget(int portNumber, Runnable onTargetEnd) {
            this.portNumber = portNumber;
            this.onTargetEnd = onTargetEnd;
            this.endState = new AtomicReference<>();
        }

        @Override
        public State getState() {
            return endState.get();
        }

        @Override
        public void run() {
            try (
                    ServerSocket serverSkt = new ServerSocket(portNumber);
                    Socket clientSkt = serverSkt.accept();
                    BufferedReader in = new BufferedReader(new InputStreamReader(clientSkt.getInputStream()));
                    PrintWriter out = new PrintWriter(clientSkt.getOutputStream(), true)
            ) {
                StringSink inputLine = new StringSink();
                while (endState.get() == null) {
                    String line = in.readLine();
                    if (line != null) {
                        inputLine.put(line).put(CRLF);
                        if (line.equals(DEATH_PILL)) {
                            break;
                        }
                    } else {
                        break;
                    }
                }
                out.print(ACK);
            } catch (IOException e) {
                Assert.fail(e.getMessage());
            } finally {
                endState.set(State.TERMINATED);
                if (onTargetEnd != null) {
                    onTargetEnd.run();
                }
            }
        }
    }
}
