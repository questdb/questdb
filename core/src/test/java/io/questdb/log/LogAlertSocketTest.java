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
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;

import static io.questdb.log.HttpLogRecordSink.CRLF;

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
        try (LogAlertSocket alertSkt = new LogAlertSocket("localhost:1234,localhost:1242")) {
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
            Assert.assertTrue(firstServerCompleted.await(5_000_000_000L));

            // by now there is only one server surviving, and we are connected to the other.
            // send a death pill and kill the surviving server.
            builder.clear();
            alertSkt.send(builder.put(MockAlertTarget.DEATH_PILL).put(CRLF).$());

            // wait for haltness, and then all servers should be done.
            Assert.assertTrue(haltLatch.await(10_000_000_000L));
            for (int i = 0; i < numHosts; i++) {
                Assert.assertFalse(servers[i].isRunning());
            }
        }
    }

    @Test
    public void testFailOverNoServers() {
        try (LogAlertSocket alertSkt = new LogAlertSocket("localhost:1234,localhost:1243")) {
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
    }

    private void assertLogError(String socketAddress, String expected) throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (LogAlertSocket ignored = new LogAlertSocket(socketAddress, 1024, 50_000_000)) {
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
            try (LogAlertSocket socket = new LogAlertSocket(socketAddress, 1024, 50_000_000)) {
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
