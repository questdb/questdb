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
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class LogAlertSocketTest {

    private static final FilesFacade ff = FilesFacadeImpl.INSTANCE;


    @Test
    public void testParseSocketAddressEmpty() throws Exception {
        assertSocketAddress("", LogAlertSocket.DEFAULT_HOST, LogAlertSocket.DEFAULT_PORT);
        assertSocketAddress(":", LogAlertSocket.DEFAULT_HOST, LogAlertSocket.DEFAULT_PORT);
        assertSocketAddress(":    ", LogAlertSocket.DEFAULT_HOST, LogAlertSocket.DEFAULT_PORT);
        assertSocketAddress("  :  ", LogAlertSocket.DEFAULT_HOST, LogAlertSocket.DEFAULT_PORT);
        assertSocketAddress("    :", LogAlertSocket.DEFAULT_HOST, LogAlertSocket.DEFAULT_PORT);
        assertSocketAddress("     ", LogAlertSocket.DEFAULT_HOST, LogAlertSocket.DEFAULT_PORT);
        assertSocketAddress("\"\"", LogAlertSocket.DEFAULT_HOST, LogAlertSocket.DEFAULT_PORT);
        assertSocketAddress("\":\"", LogAlertSocket.DEFAULT_HOST, LogAlertSocket.DEFAULT_PORT);
        assertSocketAddress("\"     \"", LogAlertSocket.DEFAULT_HOST, LogAlertSocket.DEFAULT_PORT);
        assertSocketAddress("\":     \"", LogAlertSocket.DEFAULT_HOST, LogAlertSocket.DEFAULT_PORT);
        assertSocketAddress("\"  :  \"", LogAlertSocket.DEFAULT_HOST, LogAlertSocket.DEFAULT_PORT);
        assertSocketAddress("\"     :\"", LogAlertSocket.DEFAULT_HOST, LogAlertSocket.DEFAULT_PORT);
    }

    @Test
    public void testParseSocketAddressMultipleEmpty() throws Exception {
        String[] expectedHosts = {LogAlertSocket.DEFAULT_HOST, LogAlertSocket.DEFAULT_HOST};
        int[] expectedPorts = {LogAlertSocket.DEFAULT_PORT, LogAlertSocket.DEFAULT_PORT};
        assertSocketAddress(",    ", expectedHosts, expectedPorts);
        assertSocketAddress("  ,  ", expectedHosts, expectedPorts);
        assertSocketAddress("    ,", expectedHosts, expectedPorts);
        assertSocketAddress(":,    ", expectedHosts, expectedPorts);
        assertSocketAddress("  ,:  ", expectedHosts, expectedPorts);
        assertSocketAddress("  :  ,", expectedHosts, expectedPorts);
        assertSocketAddress(",", expectedHosts, expectedPorts);
        assertSocketAddress("\",    \"", expectedHosts, expectedPorts);
        assertSocketAddress("\"  ,  \"", expectedHosts, expectedPorts);
        assertSocketAddress("\"    ,\"", expectedHosts, expectedPorts);
        assertSocketAddress("\",\"", expectedHosts, expectedPorts);
    }

    public void assertSocketAddress(
            String socketAddress,
            String[] expectedHosts,
            int[] expectedPorts
    ) throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (LogAlertSocket socket = new LogAlertSocket(ff, socketAddress, 1024)) {
                Assert.assertEquals(expectedHosts.length, socket.getHostPortLimit());
                Assert.assertEquals(expectedPorts.length, socket.getHostPortLimit());
                for (int i = 0; i < expectedHosts.length; i++) {
                    Assert.assertEquals(expectedHosts[i], socket.getHosts()[i]);
                    Assert.assertEquals(expectedPorts[i], socket.getPorts()[i]);
                }
            }
        });
    }

    public void assertSocketAddress(
            String socketAddress,
            String expectedHost,
            int expectedPort
    ) throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (LogAlertSocket socket = new LogAlertSocket(ff, socketAddress, 1024)) {
                Assert.assertEquals(1, socket.getHostPortLimit());
                Assert.assertEquals(0, socket.getCurrentHostPortIdx());
                Assert.assertEquals(expectedHost, socket.getCurrentHost());
                Assert.assertEquals(expectedPort, socket.getCurrentPort());
            }
        });
    }
}
