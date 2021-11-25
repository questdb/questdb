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

import io.questdb.std.FilesFacade;
import io.questdb.std.FilesFacadeImpl;
import io.questdb.std.str.StringSink;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class LogAlertSocketTest {

    private static final FilesFacade ff = FilesFacadeImpl.INSTANCE;

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
    public void testFailOver() throws IOException {
        try (LogAlertSocket alertSkt = new LogAlertSocket(ff, "localhost:1234,localhost:1243")) {
            final String ACK = "Ack";
            final String DEATH_PILL = ".ByE.";
            final String logMessage = "Something";
            final HttpLogAlertBuilder builder = new HttpLogAlertBuilder(alertSkt)
                    .putHeader("localhost")
                    .setMark();

            // start servers
            final int numHosts = alertSkt.getNumberOfAlertHosts();
            Assert.assertEquals(2, numHosts);
            final CountDownLatch haltLatch = new CountDownLatch(numHosts);
            final CountDownLatch firstServerCompleted = new CountDownLatch(1);
            final Thread[] servers = new Thread[numHosts];
            for (int i = 0; i < numHosts; i++) {
                final int portNumber = alertSkt.getAlertPorts()[i];
                servers[i] = new Thread(() -> {
                    try (
                            ServerSocket serverSkt = new ServerSocket(portNumber);
                            Socket clientSkt = serverSkt.accept();
                            BufferedReader in = new BufferedReader(new InputStreamReader(clientSkt.getInputStream()));
                            PrintWriter out = new PrintWriter(clientSkt.getOutputStream(), true)
                    ) {
                        System.out.printf("Listening on port [%d]%n", portNumber);
                        StringSink inputLine = new StringSink();
                        while (true) {
                            String line = in.readLine();
                            if (line == null || line.equals(DEATH_PILL)) {
                                break;
                            }
                            inputLine.put(line).put('\n');
                        }
                        System.out.printf("Received [%d]:%n%s%n", portNumber, inputLine);
                        out.print(ACK);
                        System.out.printf("Sent [%d]: %s%n", portNumber, ACK);
                    } catch (IOException e) {
                        Assert.fail(e.getMessage());
                    } finally {
                        firstServerCompleted.countDown();
                        haltLatch.countDown();
                        System.out.printf("Bye [%d]%n", portNumber);
                    }
                });
                servers[i].start();
            }
            alertSkt.connect();
            alertSkt.send(builder
                            .rewindToMark()
                            .put(logMessage)
                            .put('\n')
                            .put(DEATH_PILL)
                            .put('\n')
                            .$(),
                    ack -> Assert.assertEquals(ack, ACK)
            );
            try {
                firstServerCompleted.await(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Assert.fail("timed-out");
            }

            builder.clear();
            builder.put(DEATH_PILL).put('\n');
            for (int i=0; i< numHosts; i++) {
                alertSkt.send(builder.length(), ack -> Assert.assertEquals(ack, ACK));
            }

            try {
                haltLatch.await(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Assert.fail("timed-out");
            }
            for (int i=0; i< servers.length; i++) {
                Assert.assertEquals(Thread.State.TERMINATED, servers[i].getState());
            }
        }
    }

    private void assertLogError(String socketAddress, String expected) throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (LogAlertSocket ignored = new LogAlertSocket(ff, socketAddress, 1024)) {
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
            try (LogAlertSocket socket = new LogAlertSocket(ff, socketAddress, 1024)) {
                Assert.assertEquals(expectedHosts.length, socket.getNumberOfAlertHosts());
                Assert.assertEquals(expectedPorts.length, socket.getNumberOfAlertHosts());
                for (int i = 0; i < expectedHosts.length; i++) {
                    Assert.assertEquals(expectedHosts[i], socket.getAlertHosts()[i]);
                    Assert.assertEquals(expectedPorts[i], socket.getAlertPorts()[i]);
                }
            }
        });
    }
}
