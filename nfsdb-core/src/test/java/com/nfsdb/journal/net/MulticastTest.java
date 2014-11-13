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

import com.nfsdb.journal.Journal;
import com.nfsdb.journal.JournalWriter;
import com.nfsdb.journal.exceptions.JournalNetworkException;
import com.nfsdb.journal.model.Quote;
import com.nfsdb.journal.net.config.ServerConfig;
import com.nfsdb.journal.net.mcast.AbstractOnDemandSender;
import com.nfsdb.journal.net.mcast.OnDemandAddressPoller;
import com.nfsdb.journal.net.mcast.OnDemandAddressSender;
import com.nfsdb.journal.test.tools.AbstractTest;
import com.nfsdb.journal.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.net.InetSocketAddress;
import java.net.SocketException;
import java.util.concurrent.TimeUnit;

public class MulticastTest extends AbstractTest {

    private void assertMulticast(ServerConfig serverConfig) throws JournalNetworkException {
        AbstractOnDemandSender sender = new OnDemandAddressSender(serverConfig, 120, 150);
        sender.start();

        OnDemandAddressPoller poller = new OnDemandAddressPoller(serverConfig, 150, 120);
        InetSocketAddress address = poller.poll(2, 500, TimeUnit.MILLISECONDS);
        Assert.assertNotNull(address);
        System.out.println(address);
        sender.halt();
    }

    private boolean isMulticastEnabled() throws JournalNetworkException, SocketException {
        return new ServerConfig().getNetworkInterface().supportsMulticast();
    }

    @Test
    public void testLocalhostBehaviour() throws Exception {

        if (!isMulticastEnabled()) {
            return;
        }

        ServerConfig networkConfig = new ServerConfig();
        networkConfig.setHostname("localhost");
        assertMulticast(networkConfig);
    }

    @Test
    public void testDefaultNICBehaviour() throws Exception {
        if (!isMulticastEnabled()) {
            return;
        }
        assertMulticast(new ServerConfig());
    }

    @Test
    public void testIPV4Forced() throws Exception {
        if (!isMulticastEnabled()) {
            return;
        }
        System.setProperty("java.net.preferIPv4Stack", "true");
        ServerConfig networkConfig = new ServerConfig();
        networkConfig.setHostname("localhost");
        assertMulticast(networkConfig);
    }

    @Test
    public void testAllNics() throws Exception {
        if (!isMulticastEnabled()) {
            return;
        }
        ServerConfig networkConfig = new ServerConfig();
        networkConfig.setHostname(null);
        assertMulticast(networkConfig);
    }

    @Test
    public void testIPv6() throws Exception {
        if (!isMulticastEnabled()) {
            return;
        }

        ServerConfig networkConfig = new ServerConfig() {{
            setHostname("0:0:0:0:0:0:0:0");
            setHeartbeatFrequency(100);
        }};

        JournalServer server = new JournalServer(networkConfig, factory);
        JournalClient client = new JournalClient(factory);


        JournalWriter<Quote> remote = factory.writer(Quote.class, "remote");
        server.start();
        client.start();

        client.halt();
        server.halt();
        Journal<Quote> local = factory.reader(Quote.class, "local");
        TestUtils.assertDataEquals(remote, local);
    }
}
