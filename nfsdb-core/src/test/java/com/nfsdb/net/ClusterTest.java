/*
 * Copyright (c) 2014. Vlad Ilyushchenko
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.nfsdb.net;

import com.nfsdb.net.config.ClientConfig;
import com.nfsdb.net.config.ServerConfig;
import com.nfsdb.test.tools.AbstractTest;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

public class ClusterTest extends AbstractTest {
    @Test
    public void testBasicClusterWin() throws Exception {
        JournalServer server = new JournalServer(new ServerConfig() {{
            setHostname("localhost");
            setPort(7079);
            setHeartbeatFrequency(TimeUnit.MILLISECONDS.toMillis(100));
            setEnableMulticast(false);
        }}, factory);


        JournalClient client = new JournalClient(new ClientConfig() {{
            setHostname("localhost");
            setPort(7080);
        }}, factory);


        if (!client.pingServer()) {
            server.start();

            if (client.pingServer() && !client.sendClusterWin(server.getClusterInstance())) {
                server.halt();
            }
        }
        System.out.println(server.isRunning());

        JournalServer server2 = new JournalServer(new ServerConfig() {{
            setHostname("localhost");
            setPort(7080);
            setHeartbeatFrequency(TimeUnit.MILLISECONDS.toMillis(100));
            setEnableMulticast(false);
        }}, factory);


        JournalClient client2 = new JournalClient(new ClientConfig() {{
            setHostname("localhost");
            setPort(7079);
        }}, factory);

        if (!client2.pingServer()) {
            server2.start();

            if (client2.pingServer() && !client2.sendClusterWin(server.getClusterInstance())) {
                server.halt();
            }
        }

        client2.halt();

        System.out.println(server2.isRunning());

    }
}
