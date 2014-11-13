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

package com.nfsdb.journal.net;

import com.nfsdb.journal.exceptions.JournalNetworkException;
import com.nfsdb.journal.net.config.ServerConfig;
import com.nfsdb.journal.net.mcast.AbstractOnDemandSender;
import com.nfsdb.journal.net.mcast.OnDemandAddressPoller;
import com.nfsdb.journal.net.mcast.OnDemandAddressSender;
import org.junit.Assert;
import org.junit.Test;

import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;

public class MulticastTest {

    private void assertMulticast(ServerConfig serverConfig) throws JournalNetworkException {
        AbstractOnDemandSender sender = new OnDemandAddressSender(serverConfig, 120, 150);
        sender.start();

        OnDemandAddressPoller poller = new OnDemandAddressPoller(serverConfig, 150, 120);
        InetSocketAddress address = poller.poll(2, 500, TimeUnit.MILLISECONDS);
        Assert.assertNotNull(address);
        System.out.println(address);
        sender.halt();
    }

    @Test
    public void testLocalhostBehaviour() throws Exception {
        ServerConfig networkConfig = new ServerConfig();
        networkConfig.setHostname("localhost");
        assertMulticast(networkConfig);
    }

    @Test
    public void testDefaultNICBehaviour() throws Exception {
        assertMulticast(new ServerConfig());
    }

    @Test
    public void testIPV4Forced() throws Exception {
        System.setProperty("java.net.preferIPv4Stack", "true");
        ServerConfig networkConfig = new ServerConfig();
        networkConfig.setHostname("localhost");
        assertMulticast(networkConfig);
    }

    @Test
    public void testAllNics() throws Exception {
        ServerConfig networkConfig = new ServerConfig();
        networkConfig.setHostname(null);
        assertMulticast(networkConfig);
    }
}
